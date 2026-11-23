use super::super::*;
use super::helpers::*;
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use serial_test::serial;
use tempfile::TempDir;

#[test]
#[serial]
fn gh515_targeted_watch_once_finishes_admitted_batch_after_stop() -> Result<()> {
    use std::io::Write;
    use tracing::field::{Field, Visit};
    use tracing::{Event, Subscriber};
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    struct StopOnWatchScan {
        progress: Arc<IndexingProgress>,
        observed: Arc<AtomicBool>,
    }

    #[derive(Default)]
    struct WatchScanVisitor(bool);

    impl Visit for WatchScanVisitor {
        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "message" && value == "watch_once_scan" {
                self.0 = true;
            }
        }

        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.record_str(field, format!("{value:?}").trim_matches('"'));
        }
    }

    impl<S: Subscriber> Layer<S> for StopOnWatchScan {
        fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
            let mut visitor = WatchScanVisitor::default();
            event.record(&mut visitor);
            if visitor.0 && !self.observed.swap(true, Ordering::SeqCst) {
                // This event follows the real connector scan. Stop is
                // requested after work admission, before that selected
                // batch's canonical and lexical writes finish.
                self.progress.request_stop();
            }
        }
    }

    for watch in [false, true] {
        let tmp = tempfile::tempdir()?;
        let data_dir = tmp.path().join("cass-data");
        let session = tmp
            .path()
            .join(".codex/sessions/rollout-gh515-watch-stop.jsonl");
        write_semantic_watch_once_codex_session(&session, "gh515-watch-stop", "initial")?;
        let mut opts =
            semantic_watch_once_opts(&data_dir, &session, Arc::new(IndexingProgress::default()));
        opts.semantic = false;
        opts.watch = watch;
        run_index(opts.clone(), None)?;

        let marker = "gh515admittedtargetedbatch";
        // The admitted batch is a new turn appended to the session, as a
        // live rollout grows. Rewriting the existing turns would not do:
        // ingest keeps a message already stored at its index.
        fs::OpenOptions::new().append(true).open(&session)?.write_all(
            format!(
                r#"{{"timestamp":"2026-05-28T09:00:03.000Z","type":"response_item","payload":{{"type":"message","role":"user","content":[{{"type":"input_text","text":"{marker} user semantic"}}]}}}}
{{"timestamp":"2026-05-28T09:00:04.000Z","type":"response_item","payload":{{"type":"message","role":"assistant","content":[{{"type":"output_text","text":"{marker} assistant semantic"}}]}}}}
"#
            )
            .as_bytes(),
        )?;
        // Make this a changed source without depending on filesystem
        // timestamp resolution or a scheduling delay between runs.
        fs::File::options().write(true).open(&session)?.set_times(
            fs::FileTimes::new().set_modified(SystemTime::now() + Duration::from_secs(2)),
        )?;
        let progress = Arc::new(IndexingProgress::default());
        opts.progress = Some(Arc::clone(&progress));
        let observed = Arc::new(AtomicBool::new(false));
        let subscriber = Registry::default().with(StopOnWatchScan {
            progress,
            observed: Arc::clone(&observed),
        });
        let result =
            tracing::subscriber::with_default(subscriber, || run_index(opts.clone(), None));
        assert!(
            observed.load(Ordering::SeqCst),
            "targeted scan was not admitted"
        );
        let error = result.expect_err("the admitted stop must remain an interrupted result");
        assert!(
            error.downcast_ref::<IndexInterrupted>().is_some(),
            "{error:#}"
        );

        let storage = FrankenStorage::open(&opts.db_path)?;
        let committed = watch_lexical_canonical_rows(&storage);
        assert_eq!(committed.len(), 4, "{committed:?}");
        assert_eq!(
            committed
                .iter()
                .filter(|row| row.3.contains(marker))
                .map(|row| row.2)
                .collect::<Vec<_>>(),
            [2, 3],
            "the admitted turn must be committed whole: {committed:?}"
        );
        assert_eq!(
            storage.raw().query_row_map(
                "SELECT COUNT(*) FROM meta WHERE key LIKE 'watch_lexical_replay_v1:%'",
                &[],
                |row| row.get_typed::<i64>(0),
            )?,
            0,
            "the admitted batch must finish lexical publication and clear its recovery debt"
        );
        storage.close()?;
        let index_path = index_dir(&data_dir)?;
        let published = watch_lexical_search_ids(&index_path, marker);
        assert_eq!(published.len(), 2);
        assert!(fs::read(data_dir.join("index-run.lock"))?.is_empty());

        opts.progress = Some(Arc::new(IndexingProgress::default()));
        run_index(opts.clone(), None)?;
        let recovered = FrankenStorage::open(&opts.db_path)?;
        assert_eq!(watch_lexical_canonical_rows(&recovered), committed);
        assert_eq!(
            recovered
                .raw()
                .query_row_map("PRAGMA integrity_check", &[], |row| {
                    row.get_typed::<String>(0)
                })?,
            "ok"
        );
        assert!(
            recovered
                .raw()
                .query("PRAGMA foreign_key_check")?
                .is_empty()
        );
        recovered.close()?;
        assert_eq!(watch_lexical_search_ids(&index_path, marker), published);
        assert!(fs::read(data_dir.join("index-run.lock"))?.is_empty());
    }
    Ok(())
}

fn write_semantic_watch_once_codex_session(path: &Path, id: &str, marker: &str) -> Result<()> {
    let parent = path
        .parent()
        .context("semantic watch-once fixture path should have a parent")?;
    std::fs::create_dir_all(parent)?;
    std::fs::write(
        path,
        format!(
            r#"{{"timestamp":"2026-05-28T09:00:00.000Z","type":"session_meta","payload":{{"id":"{id}","cwd":"/data/projects/coding_agent_session_search"}}}}
{{"timestamp":"2026-05-28T09:00:01.000Z","type":"response_item","payload":{{"type":"message","role":"user","content":[{{"type":"input_text","text":"{marker} user semantic"}}]}}}}
{{"timestamp":"2026-05-28T09:00:02.000Z","type":"response_item","payload":{{"type":"message","role":"assistant","content":[{{"type":"output_text","text":"{marker} assistant semantic"}}]}}}}
"#,
        ),
    )?;
    Ok(())
}

fn semantic_watch_once_opts(
    data_dir: &Path,
    session: &Path,
    progress: Arc<IndexingProgress>,
) -> crate::indexer::IndexOptions {
    crate::indexer::IndexOptions {
        full: false,
        watch: false,
        force_rebuild: false,
        watch_once_paths: Some(vec![session.to_path_buf()]),
        db_path: data_dir.join("db.sqlite"),
        data_dir: data_dir.to_path_buf(),
        semantic: true,
        build_hnsw: false,
        embedder: "hash".to_string(),
        progress: Some(progress),
        watch_interval_secs: 30,
    }
}

fn semantic_watch_once_stats(progress: &Arc<IndexingProgress>) -> Result<SemanticWatchOnceStats> {
    let stats = progress
        .stats
        .lock()
        .map_err(|err| anyhow::anyhow!("semantic watch-once stats lock poisoned: {err}"))?;
    stats
        .semantic_watch_once
        .clone()
        .context("semantic watch-once proof")
}

#[test]
fn targeted_semantic_watch_once_bypasses_unrelated_lexical_optimization() {
    assert!(!should_optimize_tantivy_after_watch_once_ingest(0, false));
    assert!(!should_optimize_tantivy_after_watch_once_ingest(0, true));
    assert!(should_optimize_tantivy_after_watch_once_ingest(1, false));
    assert!(!should_optimize_tantivy_after_watch_once_ingest(1, true));
}

#[test]
fn issue_342_semantic_build_replaces_stale_lexical_progress_with_initialize_phase() {
    let progress = Arc::new(IndexingProgress::default());
    let progress_bump = Arc::new(AtomicI64::new(0));
    progress.phase.store(2, Ordering::Relaxed);
    progress.total.store(1, Ordering::Relaxed);
    progress.current.store(1, Ordering::Relaxed);
    progress.is_rebuilding.store(true, Ordering::Relaxed);
    progress
        .rebuild_pipeline_staged_shard_build_active_jobs
        .store(1, Ordering::Relaxed);

    prepare_progress_for_semantic_build(Some(&progress));
    assert_eq!(
        progress.phase.load(Ordering::Relaxed),
        INDEX_PHASE_PREPARING,
        "shared targeted semantic repair must remain in its opaque phase"
    );
    assert_eq!(progress.total.load(Ordering::Relaxed), 1);
    assert_eq!(progress.current.load(Ordering::Relaxed), 1);
    assert!(progress.rebuild_pipeline_is_quiescent());

    set_semantic_progress_phase(
        Some(&progress),
        &progress_bump,
        INDEX_PHASE_SEMANTIC_INITIALIZE,
        0,
        0,
    );

    assert_eq!(
        progress.phase.load(Ordering::Relaxed),
        INDEX_PHASE_SEMANTIC_INITIALIZE
    );
    assert_eq!(progress.total.load(Ordering::Relaxed), 0);
    assert_eq!(progress.current.load(Ordering::Relaxed), 0);
    assert!(!progress.is_rebuilding.load(Ordering::Relaxed));
    assert!(
        progress.rebuild_pipeline_is_quiescent(),
        "semantic initialization must not inherit lexical pipeline activity"
    );
}

#[test]
#[serial]
fn run_index_semantic_watch_once_publishes_targeted_manifest() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let data_dir = tmp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir)?;
    let session = tmp
        .path()
        .join(".codex")
        .join("sessions")
        .join("2026")
        .join("05")
        .join("28")
        .join("rollout-semantic-watch-once.jsonl");
    write_semantic_watch_once_codex_session(&session, "semantic-watch-once-fresh", "swonce-fresh")?;

    let progress = Arc::new(IndexingProgress::default());
    run_index(
        semantic_watch_once_opts(&data_dir, &session, Arc::clone(&progress)),
        None,
    )?;

    let stats = semantic_watch_once_stats(&progress)?;
    anyhow::ensure!(stats.published, "semantic watch-once did not publish");
    anyhow::ensure!(
        matches!(stats.reason.as_str(), "fresh_watch_once_db"),
        "wrong reason"
    );
    anyhow::ensure!(matches!(stats.tier.as_str(), "fast"), "wrong tier");
    anyhow::ensure!(matches!(stats.selected_docs, 2), "wrong selected doc count");
    anyhow::ensure!(matches!(stats.embedded_docs, 2), "wrong embedded doc count");
    anyhow::ensure!(
        matches!(
            stats.manifest_after_db_fingerprint.as_deref(),
            Some("content-v1:1:1:2")
        ),
        "wrong post-publish fingerprint"
    );

    let manifest = SemanticManifest::load_or_default(&data_dir)?;
    let artifact = manifest.fast_tier.as_ref().context("fast artifact")?;
    anyhow::ensure!(
        matches!(artifact.conversation_count, 1),
        "wrong conversation count"
    );
    anyhow::ensure!(matches!(artifact.doc_count, 2), "wrong doc count");
    anyhow::ensure!(
        matches!(artifact.db_fingerprint.as_str(), "content-v1:1:1:2"),
        "wrong artifact fingerprint"
    );
    let index = FsVectorIndex::open(&vector_index_path(&data_dir, "fnv1a-384"))?;
    anyhow::ensure!(
        matches!(index.record_count(), 2),
        "wrong vector record count"
    );
    Ok(())
}

#[test]
#[serial]
fn run_index_semantic_watch_once_catches_up_append_only_prefix_manifest() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let data_dir = tmp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir)?;
    let first = tmp
        .path()
        .join(".codex")
        .join("sessions")
        .join("2026")
        .join("05")
        .join("28")
        .join("rollout-semantic-watch-once-first.jsonl");
    let second = first.with_file_name("rollout-semantic-watch-once-second.jsonl");
    write_semantic_watch_once_codex_session(&first, "semantic-watch-once-first", "swonce-one")?;
    write_semantic_watch_once_codex_session(&second, "semantic-watch-once-second", "swonce-two")?;

    let first_progress = Arc::new(IndexingProgress::default());
    run_index(
        semantic_watch_once_opts(&data_dir, &first, first_progress),
        None,
    )?;

    let second_progress = Arc::new(IndexingProgress::default());
    run_index(
        semantic_watch_once_opts(&data_dir, &second, Arc::clone(&second_progress)),
        None,
    )?;

    let stats = semantic_watch_once_stats(&second_progress)?;
    anyhow::ensure!(stats.published, "semantic watch-once did not publish");
    anyhow::ensure!(
        matches!(
            stats.reason.as_str(),
            "semantic_artifact_is_append_only_prefix"
        ),
        "wrong reason"
    );
    anyhow::ensure!(
        matches!(
            stats.manifest_before_db_fingerprint.as_deref(),
            Some("content-v1:1:1:2")
        ),
        "wrong pre-publish fingerprint"
    );
    anyhow::ensure!(
        matches!(
            stats.manifest_after_db_fingerprint.as_deref(),
            Some("content-v1:2:2:4")
        ),
        "wrong post-publish fingerprint"
    );
    anyhow::ensure!(matches!(stats.selected_docs, 2), "wrong selected doc count");
    anyhow::ensure!(matches!(stats.embedded_docs, 2), "wrong embedded doc count");

    let manifest = SemanticManifest::load_or_default(&data_dir)?;
    let artifact = manifest.fast_tier.as_ref().context("fast artifact")?;
    anyhow::ensure!(
        matches!(artifact.conversation_count, 2),
        "wrong conversation count"
    );
    anyhow::ensure!(matches!(artifact.doc_count, 4), "wrong doc count");
    anyhow::ensure!(
        matches!(artifact.db_fingerprint.as_str(), "content-v1:2:2:4"),
        "wrong artifact fingerprint"
    );
    let index = FsVectorIndex::open(&vector_index_path(&data_dir, "fnv1a-384"))?;
    anyhow::ensure!(
        matches!(index.record_count(), 4),
        "wrong vector record count"
    );
    Ok(())
}

#[test]
#[serial]
fn run_index_semantic_watch_once_reconciles_deleted_prefix_conversation() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let data_dir = tmp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir)?;
    let first = tmp
        .path()
        .join(".codex")
        .join("sessions")
        .join("2026")
        .join("05")
        .join("28")
        .join("rollout-semantic-watch-once-reconcile-first.jsonl");
    let second = first.with_file_name("rollout-semantic-watch-once-reconcile-second.jsonl");
    let third = first.with_file_name("rollout-semantic-watch-once-reconcile-third.jsonl");
    write_semantic_watch_once_codex_session(
        &first,
        "semantic-watch-once-reconcile-first",
        "swonce-reconcile-one",
    )?;
    write_semantic_watch_once_codex_session(
        &second,
        "semantic-watch-once-reconcile-second",
        "swonce-reconcile-two",
    )?;
    write_semantic_watch_once_codex_session(
        &third,
        "semantic-watch-once-reconcile-third",
        "swonce-reconcile-three",
    )?;

    run_index(
        semantic_watch_once_opts(&data_dir, &first, Arc::new(IndexingProgress::default())),
        None,
    )?;
    run_index(
        semantic_watch_once_opts(&data_dir, &second, Arc::new(IndexingProgress::default())),
        None,
    )?;

    let index_path = vector_index_path(&data_dir, "fnv1a-384");
    let before_index = FsVectorIndex::open(&index_path)?;
    let mut before_vectors = HashMap::new();
    for record_index in 0..before_index.record_count() {
        let doc_id = before_index.doc_id_at(record_index)?.to_string();
        before_vectors.insert(doc_id, before_index.vector_at_f32(record_index)?);
    }
    drop(before_index);
    let mut manifest = SemanticManifest::load_or_default(&data_dir)?;
    manifest.publish_hnsw(crate::search::semantic_manifest::HnswRecord {
        base_tier: SemanticTierKind::Fast,
        embedder_id: "fnv1a-384".to_string(),
        ef_search: 64,
        index_path: "vector_index/hnsw-fnv1a-384.chsw".to_string(),
        size_bytes: 1,
        built_at_ms: 1,
        ready: true,
    });
    manifest.save(&data_dir)?;

    let storage = FrankenStorage::open(&data_dir.join("db.sqlite"))?;
    let removed_conversation_id: i64 = storage.raw().query_row_map(
        "SELECT id FROM conversations ORDER BY id ASC LIMIT 1",
        &[],
        |row| row.get_typed(0),
    )?;
    for statement in [
        "DELETE FROM conversation_external_lookup WHERE conversation_id = ?1",
        "DELETE FROM conversation_external_tail_lookup WHERE conversation_id = ?1",
        "DELETE FROM messages WHERE conversation_id = ?1",
        "DELETE FROM conversation_tail_state WHERE conversation_id = ?1",
        "DELETE FROM conversations WHERE id = ?1",
    ] {
        storage
            .raw()
            .execute_compat(statement, &[ParamValue::from(removed_conversation_id)])?;
    }
    drop(storage);

    let progress = Arc::new(IndexingProgress::default());
    run_index(
        semantic_watch_once_opts(&data_dir, &third, Arc::clone(&progress)),
        None,
    )?;

    let stats = semantic_watch_once_stats(&progress)?;
    anyhow::ensure!(stats.published, "semantic watch-once did not publish");
    anyhow::ensure!(
        matches!(
            stats.reason.as_str(),
            "semantic_artifact_reconciled_with_current_db"
        ),
        "wrong reason: {}",
        stats.reason
    );
    anyhow::ensure!(matches!(stats.selected_docs, 2), "wrong selected doc count");
    anyhow::ensure!(matches!(stats.embedded_docs, 2), "wrong embedded doc count");

    let storage = FrankenStorage::open(&data_dir.join("db.sqlite"))?;
    let current_message_ids: HashSet<u64> = storage
        .raw()
        .query_map_collect(
            "SELECT m.id
             FROM messages AS m
             INNER JOIN conversations AS c ON c.id = m.conversation_id",
            &[],
            |row| row.get_typed::<i64>(0),
        )?
        .into_iter()
        .filter_map(message_id_from_db)
        .collect();
    let manifest = SemanticManifest::load_or_default(&data_dir)?;
    let artifact = manifest.fast_tier.as_ref().context("fast artifact")?;
    anyhow::ensure!(
        manifest.hnsw.is_none(),
        "reconciliation must invalidate the now-stale HNSW manifest record"
    );
    anyhow::ensure!(matches!(artifact.conversation_count, 2));
    anyhow::ensure!(matches!(artifact.doc_count, 4));
    anyhow::ensure!(matches!(
        artifact.db_fingerprint.as_str(),
        "content-v1:2:3:6"
    ));

    let after_index = FsVectorIndex::open(&index_path)?;
    anyhow::ensure!(matches!(after_index.record_count(), 4));
    anyhow::ensure!(matches!(after_index.wal_record_count(), 0));
    let mut retained_vectors_checked = 0usize;
    for record_index in 0..after_index.record_count() {
        anyhow::ensure!(
            !after_index.is_deleted(record_index),
            "reconciled semantic index retained a tombstone"
        );
        let doc_id = after_index.doc_id_at(record_index)?;
        let parsed = crate::search::vector_index::parse_semantic_doc_id(doc_id)
            .with_context(|| format!("parse semantic doc id {doc_id}"))?;
        anyhow::ensure!(
            current_message_ids.contains(&parsed.message_id),
            "stale semantic message {} survived reconciliation",
            parsed.message_id
        );
        if let Some(before_vector) = before_vectors.get(doc_id) {
            anyhow::ensure!(
                after_index.vector_at_f32(record_index)?.eq(before_vector),
                "retained semantic vector changed for {doc_id}"
            );
            retained_vectors_checked = retained_vectors_checked.saturating_add(1);
        }
    }
    anyhow::ensure!(
        matches!(retained_vectors_checked, 2),
        "expected both unchanged messages to retain their vectors"
    );
    Ok(())
}

#[test]
#[serial]
fn run_index_semantic_watch_once_reconciles_same_id_content_replacement() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let data_dir = tmp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir)?;
    let session = tmp
        .path()
        .join(".codex")
        .join("sessions")
        .join("2026")
        .join("05")
        .join("28")
        .join("rollout-semantic-watch-once-content-replacement.jsonl");
    write_semantic_watch_once_codex_session(
        &session,
        "semantic-watch-once-content-replacement",
        "swonce-content-before",
    )?;
    run_index(
        semantic_watch_once_opts(&data_dir, &session, Arc::new(IndexingProgress::default())),
        None,
    )?;

    let index_path = vector_index_path(&data_dir, "fnv1a-384");
    let before_index = FsVectorIndex::open(&index_path)?;
    let mut before_vectors = HashMap::new();
    for record_index in 0..before_index.record_count() {
        let doc_id = before_index.doc_id_at(record_index)?.to_owned();
        before_vectors.insert(doc_id, before_index.vector_at_f32(record_index)?);
    }
    drop(before_index);

    let storage = FrankenStorage::open(&data_dir.join("db.sqlite"))?;
    let changed_message_id: i64 = storage.raw().query_row_map(
        "SELECT id FROM messages ORDER BY id ASC LIMIT 1",
        &[],
        |row| row.get_typed(0),
    )?;
    let stale_doc_id = before_vectors
        .keys()
        .find(|doc_id| {
            crate::search::vector_index::parse_semantic_doc_id(doc_id).is_some_and(|parsed| {
                i64::try_from(parsed.message_id).ok() == Some(changed_message_id)
            })
        })
        .cloned()
        .context("semantic document for changed message")?;
    storage.raw().execute_compat(
        "UPDATE messages SET content = ?1 WHERE id = ?2",
        &[
            ParamValue::from("swonce-content-after replacement semantic content"),
            ParamValue::from(changed_message_id),
        ],
    )?;
    let canonical_documents = CanonicalSemanticDocuments::from_storage(&storage)?;
    anyhow::ensure!(
        !canonical_documents.doc_ids.contains(&stale_doc_id),
        "same-ID replacement unexpectedly preserved the stale semantic identity"
    );
    let expected_doc_ids = canonical_documents.doc_ids.clone();
    let storage = Mutex::new(storage);

    let stats = run_targeted_semantic_watch_once_publish("hash", &data_dir, &storage, 1, 1)?;
    anyhow::ensure!(stats.published, "semantic watch-once did not publish");
    anyhow::ensure!(
        matches!(
            stats.reason.as_str(),
            "semantic_artifact_reconciled_with_current_db"
        ),
        "wrong reason: {}",
        stats.reason
    );
    anyhow::ensure!(matches!(stats.selected_docs, 1), "wrong selected doc count");
    anyhow::ensure!(matches!(stats.embedded_docs, 1), "wrong embedded doc count");
    anyhow::ensure!(
        stats
            .manifest_before_db_fingerprint
            .eq(&stats.manifest_after_db_fingerprint),
        "count/max fingerprint should remain unchanged for a same-ID replacement"
    );

    let after_index = FsVectorIndex::open(&index_path)?;
    anyhow::ensure!(matches!(after_index.record_count(), 2));
    anyhow::ensure!(matches!(after_index.wal_record_count(), 0));
    let mut after_doc_ids = HashSet::new();
    let mut retained_vectors_checked = 0usize;
    for record_index in 0..after_index.record_count() {
        anyhow::ensure!(
            !after_index.is_deleted(record_index),
            "reconciled semantic index retained a tombstone"
        );
        let doc_id = after_index.doc_id_at(record_index)?.to_owned();
        after_doc_ids.insert(doc_id.clone());
        if let Some(before_vector) = before_vectors.get(&doc_id) {
            anyhow::ensure!(
                after_index.vector_at_f32(record_index)?.eq(before_vector),
                "unchanged semantic vector changed for {doc_id}"
            );
            retained_vectors_checked = retained_vectors_checked.saturating_add(1);
        }
    }
    anyhow::ensure!(
        after_doc_ids.eq(&expected_doc_ids),
        "published semantic identities do not match the canonical DB"
    );
    anyhow::ensure!(
        !after_doc_ids.contains(&stale_doc_id),
        "stale same-ID content vector survived reconciliation"
    );
    anyhow::ensure!(
        matches!(retained_vectors_checked, 1),
        "expected the unchanged message vector to be copied exactly"
    );
    Ok(())
}

#[test]
#[serial]
fn run_index_semantic_watch_once_reports_already_covered_manifest() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let data_dir = tmp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir)?;
    let session = tmp
        .path()
        .join(".codex")
        .join("sessions")
        .join("2026")
        .join("05")
        .join("28")
        .join("rollout-semantic-watch-once-rerun.jsonl");
    write_semantic_watch_once_codex_session(&session, "semantic-watch-once-rerun", "swonce-rerun")?;

    let first_progress = Arc::new(IndexingProgress::default());
    run_index(
        semantic_watch_once_opts(&data_dir, &session, first_progress),
        None,
    )?;

    let second_progress = Arc::new(IndexingProgress::default());
    run_index(
        semantic_watch_once_opts(&data_dir, &session, Arc::clone(&second_progress)),
        None,
    )?;

    let stats = semantic_watch_once_stats(&second_progress)?;
    anyhow::ensure!(stats.published, "semantic watch-once did not publish");
    anyhow::ensure!(
        matches!(stats.reason.as_str(), "semantic_artifact_already_covers_db"),
        "wrong reason"
    );
    anyhow::ensure!(
        matches!(
            stats.manifest_before_db_fingerprint.as_deref(),
            Some("content-v1:1:1:2")
        ),
        "wrong pre-publish fingerprint"
    );
    anyhow::ensure!(
        matches!(
            stats.manifest_after_db_fingerprint.as_deref(),
            Some("content-v1:1:1:2")
        ),
        "wrong post-publish fingerprint"
    );
    anyhow::ensure!(matches!(stats.selected_docs, 0), "wrong selected doc count");
    anyhow::ensure!(matches!(stats.embedded_docs, 0), "wrong embedded doc count");

    let manifest = SemanticManifest::load_or_default(&data_dir)?;
    let artifact = manifest.fast_tier.as_ref().context("fast artifact")?;
    anyhow::ensure!(
        matches!(artifact.conversation_count, 1),
        "wrong conversation count"
    );
    anyhow::ensure!(matches!(artifact.doc_count, 2), "wrong doc count");
    anyhow::ensure!(
        matches!(artifact.db_fingerprint.as_str(), "content-v1:1:1:2"),
        "wrong artifact fingerprint"
    );
    let index = FsVectorIndex::open(&vector_index_path(&data_dir, "fnv1a-384"))?;
    anyhow::ensure!(
        matches!(index.record_count(), 2),
        "wrong vector record count"
    );
    Ok(())
}

#[test]
#[serial]
fn run_index_semantic_watch_once_fails_when_no_conversation_is_indexed() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let data_dir = tmp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir)?;
    let session = tmp.path().join("not-a-supported-watch-once-file.jsonl");
    std::fs::write(&session, "{}\n")?;

    let progress = Arc::new(IndexingProgress::default());
    let err = match run_index(
        semantic_watch_once_opts(&data_dir, &session, progress),
        None,
    ) {
        Ok(_) => anyhow::bail!("zero-conversation semantic watch-once unexpectedly succeeded"),
        Err(err) => err,
    };
    let rendered = err.to_string();
    anyhow::ensure!(
        rendered.contains("indexed zero conversations"),
        "unexpected error: {rendered}"
    );
    Ok(())
}

#[test]
fn watch_event_filter_ignores_read_access_noise() {
    let event = notify::Event::new(notify::event::EventKind::Access(AccessKind::Read))
        .add_path(PathBuf::from("/tmp/session.jsonl"));
    assert!(
        !watch_event_should_trigger_reindex(&event),
        "read-only access events should not retrigger watch indexing"
    );

    let event = notify::Event::new(notify::event::EventKind::Access(AccessKind::Close(
        AccessMode::Read,
    )))
    .add_path(PathBuf::from("/tmp/session.jsonl"));
    assert!(
        !watch_event_should_trigger_reindex(&event),
        "close-after-read events should not retrigger watch indexing"
    );
}

#[test]
fn watch_event_filter_keeps_mutating_events() {
    let event = notify::Event::new(notify::event::EventKind::Access(AccessKind::Close(
        AccessMode::Write,
    )))
    .add_path(PathBuf::from("/tmp/session.jsonl"));
    assert!(
        watch_event_should_trigger_reindex(&event),
        "close-after-write events should still retrigger indexing"
    );

    let event = notify::Event::new(notify::event::EventKind::Modify(ModifyKind::Metadata(
        MetadataKind::WriteTime,
    )))
    .add_path(PathBuf::from("/tmp/session.jsonl"));
    assert!(
        watch_event_should_trigger_reindex(&event),
        "write-time metadata changes should still retrigger indexing"
    );
}

#[test]
fn watch_event_filter_ignores_access_time_metadata() {
    let event = notify::Event::new(notify::event::EventKind::Modify(ModifyKind::Metadata(
        MetadataKind::AccessTime,
    )))
    .add_path(PathBuf::from("/tmp/session.jsonl"));
    assert!(
        !watch_event_should_trigger_reindex(&event),
        "access-time metadata changes are read noise and should be ignored"
    );
}

#[test]
fn watch_event_filter_ignores_remove_events_without_delete_support() {
    let event = notify::Event::new(notify::event::EventKind::Remove(
        notify::event::RemoveKind::File,
    ))
    .add_path(PathBuf::from("/tmp/session.jsonl"));
    assert!(
        !watch_event_should_trigger_reindex(&event),
        "remove events should be ignored until watch mode can remove stale indexed rows"
    );
}

#[test]
fn watch_notification_backlog_coalesces_and_overflow_retains_a_full_scan() {
    let backlog = WatchNotificationMailbox::default();
    let rescan = AtomicBool::new(false);
    let (wake, notifications) = bounded(1);
    for _ in 0..10_000 {
        enqueue_watch_notification(
            Ok(notify::Event::new(notify::EventKind::Any)
                .add_path(PathBuf::from("sessions/repeated.jsonl"))),
            &backlog,
            &rescan,
            &wake,
        );
    }
    assert_eq!(notifications.len(), 1, "wakeups must coalesce");
    assert_eq!(backlog.backlog.lock().unwrap().paths.len(), 1);
    assert!(!rescan.load(Ordering::Acquire));

    // Simulate a filesystem flood while indexing owns the consumer thread.
    for index in 0..WATCH_PENDING_MAX_PATHS {
        enqueue_watch_notification(
            Ok(notify::Event::new(notify::EventKind::Any)
                .add_path(PathBuf::from(format!("sessions/rollout-{index}.jsonl")))),
            &backlog,
            &rescan,
            &wake,
        );
    }
    assert_eq!(notifications.len(), 1);
    assert!(rescan.load(Ordering::Acquire));
    assert!(backlog.backlog.lock().unwrap().paths.len() <= WATCH_PENDING_MAX_PATHS);
    let mut pending = WatchEventBacklog::default();
    let mut pending_rescan = false;
    collect_watch_notifications(&backlog, &rescan, &mut pending, &mut pending_rescan);
    assert!(pending_rescan, "overflow must not silently drop sources");
    assert!(pending.paths.is_empty());

    let roots = [(
        ConnectorKind::Codex,
        ScanRoot::local(PathBuf::from("sessions")),
    )];
    let deferred = PathBuf::from("sessions/still-active.jsonl");
    dispatch_pending_watch_callback(
        &mut pending,
        &mut pending_rescan,
        &roots,
        &|paths, actual_roots, rebuilding| {
            assert!(rebuilding, "lost events require a watermark-free scan");
            assert!(paths.is_empty());
            assert_eq!(actual_roots[0].1.path, roots[0].1.path);
            Ok(BTreeSet::from([deferred.clone()]))
        },
    );
    assert!(!pending_rescan);
    assert_eq!(pending.paths, BTreeSet::from([deferred.clone()]));
    dispatch_pending_watch_callback(
        &mut pending,
        &mut pending_rescan,
        &roots,
        &|paths, _, rebuilding| {
            assert!(!rebuilding);
            assert_eq!(paths, vec![deferred.clone()]);
            Ok(BTreeSet::new())
        },
    );
    assert!(pending.paths.is_empty());
}

#[test]
fn watch_notification_backlog_bounds_retained_path_capacity_and_pending_paths() {
    let backlog = WatchNotificationMailbox::default();
    let rescan = AtomicBool::new(false);
    let (wake, notifications) = bounded(1);
    let mut overallocated = PathBuf::with_capacity(WATCH_PENDING_MAX_PATH_BYTES + 1);
    overallocated.push("sessions/small-name.jsonl");
    enqueue_watch_notification(
        Ok(notify::Event::new(notify::EventKind::Any).add_path(overallocated)),
        &backlog,
        &rescan,
        &wake,
    );
    assert!(rescan.load(Ordering::Acquire));
    assert_eq!(notifications.len(), 1, "an empty queue must still wake");
    assert_eq!(backlog.backlog.lock().unwrap().path_bytes, 0);

    let mut pending = WatchEventBacklog::default();
    assert!(pending.extend(
        (0..WATCH_PENDING_MAX_PATHS).map(|index| PathBuf::from(format!("session-{index}")))
    ));
    let mut pending_rescan = false;
    collect_watch_notifications(&backlog, &rescan, &mut pending, &mut pending_rescan);
    assert!(pending_rescan);
    assert!(pending.paths.is_empty());
    assert_eq!(pending.path_bytes, 0);

    // Separately exercise overflow while merging individually bounded
    // notifications into the consumer's debounce window.
    pending_rescan = false;
    assert!(pending.extend(
        (0..WATCH_PENDING_MAX_PATHS).map(|index| PathBuf::from(format!("session-{index}")))
    ));
    enqueue_watch_notification(
        Ok(notify::Event::new(notify::EventKind::Any).add_path(PathBuf::from("one-more-session"))),
        &backlog,
        &rescan,
        &wake,
    );
    collect_watch_notifications(&backlog, &rescan, &mut pending, &mut pending_rescan);
    assert!(pending_rescan);
    assert!(pending.paths.is_empty());
}

#[test]
fn watch_notification_backlog_never_waits_for_the_consumer_lock() {
    let backlog = Arc::new(WatchNotificationMailbox::default());
    let rescan = Arc::new(AtomicBool::new(false));
    let (wake, notifications) = bounded(1);
    let (done, completed) = bounded(1);
    let held = backlog.backlog.lock().unwrap();
    let producer_backlog = Arc::clone(&backlog);
    let producer_rescan = Arc::clone(&rescan);
    let producer = thread::spawn(move || {
        enqueue_watch_notification(
            Ok(notify::Event::new(notify::EventKind::Any)
                .add_path(PathBuf::from("sessions/changed-while-locked.jsonl"))),
            &producer_backlog,
            &producer_rescan,
            &wake,
        );
        done.send(()).unwrap();
    });
    let finished_without_lock = completed.recv_timeout(Duration::from_secs(2)).is_ok();
    drop(held);
    producer.join().unwrap();
    assert!(finished_without_lock, "notify must never wait for indexing");
    assert!(
        !rescan.load(Ordering::Acquire),
        "a retained event must not force an archive-wide census"
    );
    assert_eq!(notifications.len(), 1);
    let mut pending = WatchEventBacklog::default();
    let mut pending_rescan = false;
    collect_watch_notifications(&backlog, &rescan, &mut pending, &mut pending_rescan);
    assert!(!pending_rescan);
    assert_eq!(
        pending.paths,
        BTreeSet::from([PathBuf::from("sessions/changed-while-locked.jsonl")])
    );
    assert_eq!(backlog.retained.load(Ordering::Acquire), 0);
}

#[test]
fn watch_notification_backlog_contended_handoff_shares_the_path_limit() {
    let mailbox = WatchNotificationMailbox::default();
    let rescan = AtomicBool::new(false);
    let (wake, _notifications) = bounded(1);
    enqueue_watch_notification(
        Ok(notify::Event::new(notify::EventKind::Any)
            .add_path(PathBuf::from("sessions/coalesced.jsonl"))),
        &mailbox,
        &rescan,
        &wake,
    );
    let held = mailbox.backlog.lock().unwrap();
    for index in 1..WATCH_PENDING_MAX_PATHS {
        enqueue_watch_notification(
            Ok(notify::Event::new(notify::EventKind::Any)
                .add_path(PathBuf::from(format!("sessions/deferred-{index}.jsonl")))),
            &mailbox,
            &rescan,
            &wake,
        );
    }
    assert!(!rescan.load(Ordering::Acquire));
    assert_eq!(
        held.paths.len() + mailbox.deferred_rx.len(),
        WATCH_PENDING_MAX_PATHS
    );
    assert_eq!(
        mailbox.retained.load(Ordering::Acquire) >> 32,
        WATCH_PENDING_MAX_PATHS as u64
    );
    enqueue_watch_notification(
        Ok(notify::Event::new(notify::EventKind::Any)
            .add_path(PathBuf::from("sessions/one-too-many.jsonl"))),
        &mailbox,
        &rescan,
        &wake,
    );
    assert!(
        rescan.load(Ordering::Acquire),
        "actual loss still requires a full census"
    );
    assert_eq!(
        held.paths.len() + mailbox.deferred_rx.len(),
        WATCH_PENDING_MAX_PATHS
    );
    drop(held);
    let mut pending = WatchEventBacklog::default();
    let mut pending_rescan = false;
    collect_watch_notifications(&mailbox, &rescan, &mut pending, &mut pending_rescan);
    assert!(pending_rescan);
    assert!(pending.paths.is_empty());
    assert!(mailbox.deferred_rx.is_empty());
    assert_eq!(mailbox.retained.load(Ordering::Acquire), 0);

    // Completing that recovery releases all reservations for later events.
    dispatch_pending_watch_callback(
        &mut pending,
        &mut pending_rescan,
        &[],
        &|paths, _, rebuilding| {
            assert!(paths.is_empty() && rebuilding);
            Ok(BTreeSet::new())
        },
    );
    enqueue_watch_notification(
        Ok(notify::Event::new(notify::EventKind::Any)
            .add_path(PathBuf::from("sessions/after-recovery.jsonl"))),
        &mailbox,
        &rescan,
        &wake,
    );
    collect_watch_notifications(&mailbox, &rescan, &mut pending, &mut pending_rescan);
    assert!(!pending_rescan);
    assert_eq!(
        pending.paths,
        BTreeSet::from([PathBuf::from("sessions/after-recovery.jsonl")])
    );
    assert_eq!(mailbox.retained.load(Ordering::Acquire), 0);
}

#[test]
fn watch_notification_backlog_contended_handoff_shares_the_byte_limit() {
    let mailbox = WatchNotificationMailbox::default();
    let rescan = AtomicBool::new(false);
    let (wake, _notifications) = bounded(1);
    let mut coalesced = PathBuf::with_capacity(WATCH_PENDING_MAX_PATH_BYTES / 2);
    coalesced.push("sessions/coalesced.jsonl");
    let reserved_bytes = coalesced.capacity();
    assert!(reserved_bytes < WATCH_PENDING_MAX_PATH_BYTES);
    enqueue_watch_notification(
        Ok(notify::Event::new(notify::EventKind::Any).add_path(coalesced)),
        &mailbox,
        &rescan,
        &wake,
    );
    let held = mailbox.backlog.lock().unwrap();
    let mut too_large = PathBuf::with_capacity(WATCH_PENDING_MAX_PATH_BYTES - reserved_bytes + 1);
    too_large.push("sessions/deferred.jsonl");
    assert!(too_large.capacity() <= WATCH_PENDING_MAX_PATH_BYTES);
    enqueue_watch_notification(
        Ok(notify::Event::new(notify::EventKind::Any).add_path(too_large)),
        &mailbox,
        &rescan,
        &wake,
    );
    assert!(rescan.load(Ordering::Acquire));
    assert!(mailbox.deferred_rx.is_empty());
    assert_eq!(held.path_bytes, reserved_bytes);
    assert_eq!(
        mailbox.retained.load(Ordering::Acquire) & u64::from(u32::MAX),
        reserved_bytes as u64
    );
    drop(held);
    let mut pending = WatchEventBacklog::default();
    let mut pending_rescan = false;
    collect_watch_notifications(&mailbox, &rescan, &mut pending, &mut pending_rescan);
    assert!(pending_rescan);
    assert!(pending.paths.is_empty());
    assert_eq!(mailbox.retained.load(Ordering::Acquire), 0);
}

#[test]
fn watch_notification_backlog_concurrent_handoffs_retain_every_source() {
    let mailbox = WatchNotificationMailbox::default();
    let rescan = AtomicBool::new(false);
    let (wake, _notifications) = bounded(1);
    let barrier = std::sync::Barrier::new(3);
    let mut pending = WatchEventBacklog::default();
    let mut pending_rescan = false;
    thread::scope(|scope| {
        for producer in 0..2 {
            let mailbox = &mailbox;
            let rescan = &rescan;
            let wake = &wake;
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                for index in 0..512 {
                    enqueue_watch_notification(
                        Ok(
                            notify::Event::new(notify::EventKind::Any).add_path(PathBuf::from(
                                format!("sessions/producer-{producer}-{index}.jsonl"),
                            )),
                        ),
                        mailbox,
                        rescan,
                        wake,
                    );
                }
            });
        }
        barrier.wait();
        for _ in 0..1024 {
            collect_watch_notifications(&mailbox, &rescan, &mut pending, &mut pending_rescan);
            thread::yield_now();
        }
    });
    collect_watch_notifications(&mailbox, &rescan, &mut pending, &mut pending_rescan);
    assert!(!pending_rescan);
    assert!(!rescan.load(Ordering::Acquire));
    let expected: BTreeSet<_> = (0..2)
        .flat_map(|producer| {
            (0..512).map(move |index| {
                PathBuf::from(format!("sessions/producer-{producer}-{index}.jsonl"))
            })
        })
        .collect();
    assert_eq!(pending.paths, expected);
    assert_eq!(mailbox.retained.load(Ordering::Acquire), 0);
    assert!(mailbox.deferred_rx.is_empty());
}

#[test]
fn watch_notification_backlog_preserves_changes_during_a_failed_full_scan() {
    let backlog = WatchNotificationMailbox::default();
    let rescan = AtomicBool::new(false);
    let (wake, _notifications) = bounded(1);
    let roots = [(
        ConnectorKind::Codex,
        ScanRoot::local(PathBuf::from("sessions")),
    )];
    let mut pending = WatchEventBacklog::default();
    let mut pending_rescan = true;
    dispatch_pending_watch_callback(
        &mut pending,
        &mut pending_rescan,
        &roots,
        &|_, _, rebuilding| {
            assert!(rebuilding);
            enqueue_watch_notification(
                Ok(notify::Event::new(notify::EventKind::Any)
                    .add_path(PathBuf::from("sessions/changed-during-scan.jsonl"))),
                &backlog,
                &rescan,
                &wake,
            );
            anyhow::bail!("transient storage failure")
        },
    );
    assert!(
        pending_rescan,
        "failed full scan must retain full-scan debt"
    );
    collect_watch_notifications(&backlog, &rescan, &mut pending, &mut pending_rescan);
    dispatch_pending_watch_callback(
        &mut pending,
        &mut pending_rescan,
        &roots,
        &|_, _, rebuilding| {
            assert!(rebuilding, "a retry must not restore timestamp cutoffs");
            // An event after this scan starts is distinct work even if
            // this full-root scan eventually completes successfully.
            enqueue_watch_notification(
                Ok(notify::Event::new(notify::EventKind::Any)
                    .add_path(PathBuf::from("sessions/after-retry-start.jsonl"))),
                &backlog,
                &rescan,
                &wake,
            );
            Ok(BTreeSet::new())
        },
    );
    assert!(!pending_rescan);
    collect_watch_notifications(&backlog, &rescan, &mut pending, &mut pending_rescan);
    assert!(!pending_rescan);
    assert_eq!(
        pending.paths,
        BTreeSet::from([PathBuf::from("sessions/after-retry-start.jsonl")])
    );
}

#[test]
fn watch_notification_backlog_keeps_control_commands_ahead_of_wakeups() {
    let (commands, controls) = bounded(1);
    let (wake, notifications) = bounded(1);
    wake.send(()).unwrap();
    commands
        .send(IndexerEvent::Command(ReindexCommand::Full))
        .unwrap();
    assert!(matches!(
        next_watch_loop_event(&controls, &notifications, Duration::ZERO),
        WatchLoopEvent::Control(IndexerEvent::Command(ReindexCommand::Full))
    ));
    assert!(matches!(
        next_watch_loop_event(&controls, &notifications, Duration::ZERO),
        WatchLoopEvent::Filesystem
    ));
    assert!(matches!(
        next_watch_loop_event(&controls, &notifications, Duration::ZERO),
        WatchLoopEvent::Timeout
    ));
    drop(commands);
    assert!(matches!(
        next_watch_loop_event(&controls, &notifications, Duration::ZERO),
        WatchLoopEvent::Disconnected
    ));
}

#[test]
#[serial]
fn watch_state_round_trips_to_disk() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let mut state = HashMap::new();
    state.insert(ConnectorKind::Codex, 123);
    state.insert(ConnectorKind::Gemini, 456);
    state.insert(ConnectorKind::Cursor, 789);
    state.insert(ConnectorKind::Devin, 987);

    save_watch_state(&data_dir, &state).unwrap();

    let loaded = load_watch_state(&data_dir);
    assert_eq!(loaded.get(&ConnectorKind::Codex), Some(&123));
    assert_eq!(loaded.get(&ConnectorKind::Gemini), Some(&456));
    assert_eq!(loaded.get(&ConnectorKind::Cursor), Some(&789));
    assert_eq!(loaded.get(&ConnectorKind::Devin), Some(&987));
}

#[test]
fn devin_connector_kind_maps_runtime_slug_and_compact_wire_key() {
    assert_eq!(
        ConnectorKind::from_slug("devin"),
        Some(ConnectorKind::Devin)
    );
    assert_eq!(ConnectorKind::Devin.slug(), "devin");
    assert_eq!(
        serde_json::to_string(&ConnectorKind::Devin).unwrap(),
        "\"dv\""
    );
    assert_eq!(
        serde_json::from_str::<ConnectorKind>("\"Devin\"").unwrap(),
        ConnectorKind::Devin,
        "legacy human-readable watch-state keys remain accepted"
    );
}

#[test]
fn qoder_connector_kind_maps_runtime_slug_and_compact_wire_key() {
    assert_eq!(
        ConnectorKind::from_slug("qoder"),
        Some(ConnectorKind::Qoder)
    );
    assert_eq!(ConnectorKind::Qoder.slug(), "qoder");
    assert_eq!(
        serde_json::to_string(&ConnectorKind::Qoder).unwrap(),
        "\"qd\""
    );
    assert_eq!(
        serde_json::from_str::<ConnectorKind>("\"Qoder\"").unwrap(),
        ConnectorKind::Qoder,
        "legacy human-readable watch-state keys remain accepted"
    );
}

#[test]
#[serial]
fn watch_state_overwrites_existing_file() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let mut first = HashMap::new();
    first.insert(ConnectorKind::Codex, 111);
    save_watch_state(&data_dir, &first).unwrap();

    let mut second = HashMap::new();
    second.insert(ConnectorKind::Gemini, 222);
    save_watch_state(&data_dir, &second).unwrap();

    let loaded = load_watch_state(&data_dir);
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded.get(&ConnectorKind::Gemini), Some(&222));
    assert!(!loaded.contains_key(&ConnectorKind::Codex));
}

#[test]
fn watch_state_temp_paths_are_unique() {
    let final_path = Path::new("/tmp/watch_state.json");
    let first = unique_atomic_temp_path(final_path);
    let second = unique_atomic_temp_path(final_path);

    assert_ne!(first, second);
    assert_eq!(first.parent(), final_path.parent());
    assert_eq!(second.parent(), final_path.parent());
}

#[cfg(unix)]
#[test]
fn atomic_sidecar_file_creation_refuses_preexisting_symlink() {
    use std::os::unix::fs::symlink;

    let tmp = TempDir::new().unwrap();
    let protected = tmp.path().join("protected.json");
    let sidecar = tmp.path().join(".watch_state.json.tmp");
    std::fs::write(&protected, b"protected").unwrap();
    symlink(&protected, &sidecar).unwrap();

    let err = create_new_atomic_sidecar_file(&sidecar).expect_err("symlink collision should fail");
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(std::fs::read(&protected).unwrap(), b"protected");
}

#[test]
#[serial]
fn watch_state_loads_legacy_map_format() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let legacy = r#"{"Codex":123,"Gemini":456}"#;
    std::fs::write(data_dir.join("watch_state.json"), legacy).unwrap();

    let loaded = load_watch_state(&data_dir);
    assert_eq!(loaded.get(&ConnectorKind::Codex), Some(&123));
    assert_eq!(loaded.get(&ConnectorKind::Gemini), Some(&456));
}

#[test]
#[serial]
fn watch_state_saves_compact_keys() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let mut state = HashMap::new();
    state.insert(ConnectorKind::Codex, 123);

    save_watch_state(&data_dir, &state).unwrap();

    let raw = std::fs::read_to_string(data_dir.join("watch_state.json")).unwrap();
    assert!(raw.contains("\"m\""));
    assert!(raw.contains("\"cx\""));
    assert!(!raw.contains("Codex"));
}

#[test]
#[serial]
fn watch_state_updates_after_reindex_paths() {
    let tmp = TempDir::new().unwrap();
    // Use unique subdirectory to avoid conflicts with other tests
    let xdg = tmp.path().join("xdg_watch_state");
    std::fs::create_dir_all(&xdg).unwrap();
    let prev = dotenvy::var("XDG_DATA_HOME").ok();
    unsafe { std::env::set_var("XDG_DATA_HOME", &xdg) };

    // Use xdg directly (not dirs::data_dir() which doesn't respect XDG_DATA_HOME on macOS)
    let data_dir = xdg.join("codex");
    std::fs::create_dir_all(&data_dir).unwrap();

    // Prepare codex fixture under data dir so detection + scan succeed.
    let codex_dir = data_dir.join("codex/sessions");
    std::fs::create_dir_all(&codex_dir).unwrap();
    let codex_file = codex_dir.join("rollout-002.jsonl");
    write_watch_codex_messages(
        &codex_file,
        "thread-002",
        &[
            ("user", "hi", 1_700_000_000_100),
            ("assistant", "hello", 1_700_000_000_200),
        ],
    );

    let opts = crate::indexer::IndexOptions {
        full: false,
        watch: false,
        force_rebuild: false,
        db_path: data_dir.join("agent_search.db"),
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: None,
        watch_once_paths: None,
        watch_interval_secs: 30,
    };

    // Manually set up dependencies for reindex_paths
    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    let index_path = index_dir(&opts.data_dir).unwrap();
    let t_index = TantivyIndex::open_or_create(&index_path).unwrap();

    let state = std::sync::Mutex::new(std::collections::HashMap::new());
    let storage = std::sync::Mutex::new(storage);
    let t_index = std::sync::Mutex::new(Some(t_index));

    // Need roots for reindex_paths
    let roots = vec![(ConnectorKind::Codex, ScanRoot::local(codex_dir))];

    reindex_paths(
        &opts,
        vec![codex_file.clone()],
        &roots,
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
    )
    .unwrap();

    let loaded = load_watch_state(&data_dir);
    assert!(loaded.contains_key(&ConnectorKind::Codex));
    let ts = loaded.get(&ConnectorKind::Codex).copied().unwrap();
    assert!(ts > 0);

    if let Some(prev) = prev {
        unsafe { std::env::set_var("XDG_DATA_HOME", prev) };
    } else {
        unsafe { std::env::remove_var("XDG_DATA_HOME") };
    }
}
