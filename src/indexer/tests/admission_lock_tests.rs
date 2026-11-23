use super::super::*;
use super::helpers::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use crate::sources::provenance::SourceKind;
use serial_test::serial;
use tempfile::TempDir;

#[test]
fn raw_mirror_capture_reuses_preparse_manifest_without_a_prepersist_db_link() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("preparse-then-parsed.jsonl");
    let source_bytes = b"{\"type\":\"message\",\"role\":\"user\",\"content\":\"hello\"}\n";
    std::fs::write(&source_path, source_bytes).expect("write source");
    let root = ScanRoot::local(source_path.clone());

    let active_filter = ActiveSessionSourceFilter::default();
    capture_scan_root_file_before_parse(
        &data_dir,
        "codex",
        &root,
        &active_filter,
        &RawSourceExclusions::default(),
    );

    let mut conv = NormalizedConversation {
        agent_slug: "codex".to_string(),
        external_id: Some("raw-mirror-preparse-enrichment".to_string()),
        title: Some("Raw mirror preparse enrichment".to_string()),
        workspace: None,
        source_path: source_path.clone(),
        started_at: Some(1_733_000_000_000),
        ended_at: Some(1_733_000_000_100),
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".to_string(),
            author: None,
            created_at: Some(1_733_000_000_000),
            content: "hello".to_string(),
            extra: serde_json::json!({}),
            snippets: Vec::new(),
            invocations: Vec::new(),
        }],
    };
    inject_provenance(&mut conv, &Origin::local());
    attach_raw_mirror_capture(&data_dir, &mut conv, &RawSourceExclusions::default());

    let manifest_root = data_dir.join("raw-mirror/v1/manifests");
    let manifests = std::fs::read_dir(&manifest_root)
        .expect("manifest dir")
        .collect::<std::io::Result<Vec<_>>>()
        .expect("manifest entries");
    assert_eq!(manifests.len(), 1);
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(manifests[0].path()).expect("manifest bytes"))
            .expect("manifest json");
    assert_eq!(
        manifest["db_links"].as_array().map(Vec::len),
        Some(0),
        "successful parsing alone does not prove a canonical archive insertion"
    );
    assert_eq!(
        std::fs::read(&source_path).expect("source bytes"),
        source_bytes
    );
}

#[test]
fn ingest_batch_records_persisted_raw_mirror_conversation_id_link() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("data dir");
    let db_path = data_dir.join("raw-mirror-link.db");
    let storage = FrankenStorage::open(&db_path).expect("storage");
    ensure_fts_schema(&storage);

    let source_path = temp.path().join("persisted-link.jsonl");
    let source_bytes = b"{\"type\":\"message\",\"role\":\"user\",\"content\":\"hello\"}\n";
    std::fs::write(&source_path, source_bytes).expect("write source");
    let mut conv = NormalizedConversation {
        agent_slug: "codex".to_string(),
        external_id: Some("raw-mirror-persisted-link".to_string()),
        title: Some("Raw mirror persisted link".to_string()),
        workspace: None,
        source_path: source_path.clone(),
        started_at: Some(1_733_000_000_000),
        ended_at: Some(1_733_000_000_100),
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".to_string(),
            author: None,
            created_at: Some(1_733_000_000_000),
            content: "hello".to_string(),
            extra: serde_json::json!({}),
            snippets: Vec::new(),
            invocations: Vec::new(),
        }],
    };
    inject_provenance(&mut conv, &Origin::local());
    attach_raw_mirror_capture(&data_dir, &mut conv, &RawSourceExclusions::default());
    let manifest_relative = conv.metadata["cass"]["raw_mirror"]["manifest_relative_path"]
        .as_str()
        .expect("manifest relative path")
        .to_string();

    let mutations = ingest_batch(
        &storage,
        None,
        &data_dir,
        &[conv],
        &None,
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        false,
    )
    .expect("ingest batch");
    assert_eq!(mutations.inserted_conversations, 1);
    assert_eq!(mutations.inserted_messages, 1);

    let conversation_ids: Vec<i64> = storage
        .raw()
        .query_map_collect(
            "SELECT id FROM conversations WHERE external_id = ?1",
            &[ParamValue::from("raw-mirror-persisted-link")],
            |row| row.get_typed(0),
        )
        .expect("conversation id query");
    assert_eq!(conversation_ids.len(), 1);
    let conversation_id = conversation_ids[0];

    let manifest_path = data_dir.join("raw-mirror/v1").join(manifest_relative);
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(manifest_path).expect("manifest bytes"))
            .expect("manifest json");
    let db_links = manifest["db_links"].as_array().expect("db links");
    assert!(
        db_links.iter().any(|link| {
            link["conversation_id"].as_i64() == Some(conversation_id)
                && link["message_count"].as_u64() == Some(1)
                && link["started_at_ms"].as_i64() == Some(1_733_000_000_000)
        }),
        "manifest should include persisted conversation_id link: {manifest:#}"
    );
    assert_eq!(
        std::fs::read(&source_path).expect("source bytes"),
        source_bytes
    );
}

#[test]
#[serial]
fn batch_index_captures_explicit_file_root_before_failed_scan() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("data dir");
    let source_path = temp.path().join("parse-failure-candidate.jsonl");
    let source_bytes = b"{not valid connector json\n";
    std::fs::write(&source_path, source_bytes).expect("write source");
    *FAILING_EXPLICIT_FILE_ROOT
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(source_path.clone());

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).expect("storage");
    ensure_fts_schema(&storage);
    let opts = IndexOptions {
        full: true,
        force_rebuild: false,
        watch: false,
        watch_once_paths: None,
        db_path,
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: None,
        watch_interval_secs: 30,
    };

    let mutations = run_batch_index_with_connector_factories(
        &storage,
        None,
        &opts,
        None,
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        Vec::new(),
        None,
        vec![("codex", failing_explicit_file_root_connector_factory)],
        Some(FrankenStorage::now_millis()),
        None,
    )
    .expect("failed scan should not abort batch indexing");
    *FAILING_EXPLICIT_FILE_ROOT
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = None;

    assert_eq!(mutations, CanonicalMutationCounts::default());
    let manifest_root = data_dir.join("raw-mirror/v1/manifests");
    let manifests = std::fs::read_dir(&manifest_root)
        .expect("manifest dir")
        .collect::<std::io::Result<Vec<_>>>()
        .expect("manifest entries");
    assert_eq!(
        manifests.len(),
        1,
        "source should be mirrored before the connector parse failure"
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(manifests[0].path()).expect("manifest bytes"))
            .expect("manifest json");
    assert_eq!(manifest["provider"].as_str(), Some("codex"));
    assert_eq!(
        manifest["verification"]["status"].as_str(),
        Some("captured")
    );
    assert_eq!(
        std::fs::read(&source_path).expect("source bytes"),
        source_bytes
    );
}

/// Regression for issue #203: the direct `cass index --semantic`
/// path must classify embedder ids onto the right manifest tier so
/// the published artifact lands where `cass status` looks for it.
#[test]
fn semantic_tier_for_embedder_id_maps_known_ids() {
    assert_eq!(
        crate::indexer::semantic_tier_for_embedder_id("minilm-384"),
        Some(crate::indexer::SemanticTierKind::Quality)
    );
    assert_eq!(
        crate::indexer::semantic_tier_for_embedder_id("fnv1a-384"),
        Some(crate::indexer::SemanticTierKind::Fast)
    );
    assert_eq!(
        crate::indexer::semantic_tier_for_embedder_id("multilingual-minilm-384"),
        Some(crate::indexer::SemanticTierKind::Quality)
    );
    assert_eq!(
        crate::indexer::semantic_tier_for_embedder_id("unknown"),
        None
    );
}

/// Regression for issue #203: hash embedder must report a stable
/// "hash" model_revision so the manifest record matches what
/// `run_backfill_batch` writes for the same embedder.
#[test]
fn semantic_model_revision_for_embedder_id_known_ids() {
    assert_eq!(
        crate::indexer::semantic_model_revision_for_embedder_id("fnv1a-384"),
        "hash"
    );
    assert_eq!(
        crate::indexer::semantic_model_revision_for_embedder_id("minilm-384"),
        crate::search::model_download::ModelManifest::minilm_v2().revision
    );
    assert_eq!(
        crate::indexer::semantic_model_revision_for_embedder_id("multilingual-minilm-384"),
        crate::search::model_download::ModelManifest::multilingual_minilm_l12_v2().revision
    );
}

#[test]
fn gh470_native_watermark_admission_checks_artifacts_without_loading_models() -> Result<()> {
    for (requested, embedder_id, legacy_revision) in [
        (
            "all-MiniLM-L6-v2",
            "minilm-384",
            "native-minilm-v1:c9745ed1d9f207416be6d2e6f8de32d1f16199bf",
        ),
        (
            "multilingual",
            "multilingual-minilm-384",
            "native-multilingual-minilm-v1:59160d9e43d396d05b4139c99f9feb7922da14868587fca7e33d379821a41405",
        ),
    ] {
        let temp = tempfile::tempdir()?;
        let index_path = vector_index_path(temp.path(), embedder_id);
        fs::create_dir_all(index_path.parent().context("vector parent")?)?;
        let revision = crate::indexer::semantic::expected_vector_space_revision(embedder_id)
            .context("registered input contract")?;
        // Empty valid FSVIs exercise admission metadata only. No synthetic
        // model vectors or model files stand in for native inference.
        let write_header = |revision: &str| -> Result<()> {
            let writer = FsVectorIndex::create_with_revision(
                &index_path,
                embedder_id,
                revision,
                384,
                frankensearch::index::Quantization::F16,
            )?;
            writer.finish()?;
            Ok(())
        };
        write_header(revision)?;
        assert!(!semantic_index_has_current_contract(temp.path(), requested));
        let mut manifest = SemanticManifest::default();
        manifest.publish_artifact(ArtifactRecord {
            tier: SemanticTierKind::Quality,
            embedder_id: embedder_id.to_string(),
            model_revision: semantic_model_revision_for_embedder_id(embedder_id),
            schema_version: SEMANTIC_SCHEMA_VERSION,
            chunking_version: CHUNKING_STRATEGY_VERSION,
            dimension: 384,
            doc_count: 0,
            conversation_count: 0,
            db_fingerprint: "content-v1:0:0:0".to_string(),
            index_path: index_path
                .strip_prefix(temp.path())?
                .to_string_lossy()
                .into_owned(),
            size_bytes: fs::metadata(&index_path)?.len(),
            started_at_ms: 1,
            completed_at_ms: 2,
            ready: true,
        });
        manifest.save(temp.path())?;
        assert!(semantic_index_has_current_contract(temp.path(), requested));
        assert!(semantic_index_has_current_contract(
            temp.path(),
            embedder_id
        ));
        assert!(!semantic_index_has_current_contract(temp.path(), "hash"));
        write_header(legacy_revision)?;
        assert!(!semantic_index_has_current_contract(temp.path(), requested));
        write_header(revision)?;
        manifest
            .quality_tier
            .as_mut()
            .context("quality artifact")?
            .chunking_version = 1;
        manifest.save(temp.path())?;
        assert!(!semantic_index_has_current_contract(temp.path(), requested));
        assert!(!temp.path().join("models").exists());
    }
    Ok(())
}

#[test]
fn gh481_incremental_semantic_preflight_preserves_watermark_before_model_load() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Mutex::new(FrankenStorage::open(&temp.path().join("archive.db"))?);
    update_incremental_semantic_watermark(&storage, 7, "seed test watermark")?;
    let index_path = vector_index_path(temp.path(), "minilm-384");
    fs::create_dir_all(index_path.parent().context("vector parent")?)?;
    let mut manifest = SemanticManifest::default();
    manifest.publish_artifact(ArtifactRecord {
        tier: SemanticTierKind::Quality,
        embedder_id: "minilm-384".to_string(),
        model_revision: semantic_model_revision_for_embedder_id("minilm-384"),
        schema_version: SEMANTIC_SCHEMA_VERSION,
        chunking_version: CHUNKING_STRATEGY_VERSION,
        dimension: 384,
        doc_count: 0,
        conversation_count: 0,
        db_fingerprint: "content-v1:0:0:0".to_string(),
        index_path: index_path
            .strip_prefix(temp.path())?
            .to_string_lossy()
            .into_owned(),
        size_bytes: 0,
        started_at_ms: 1,
        completed_at_ms: 2,
        ready: true,
    });
    manifest.save(temp.path())?;
    for corrupt in [false, true] {
        if corrupt {
            fs::write(&index_path, b"not a vector index")?;
        }
        for inputs in [
            vec![],
            vec![EmbeddingInput::new(8, "Explain the parser bug")],
        ] {
            let error = embed_incremental_semantic_inputs(
                "minilm",
                temp.path(),
                &storage,
                inputs,
                8,
                "filtered test watermark",
                "appended test watermark",
            )
            .expect_err("unavailable base must refuse before loading the absent model");
            assert!(
                error
                    .to_string()
                    .contains("incremental semantic base is missing or incompatible"),
                "{error:#}"
            );
            assert_eq!(
                storage.lock().unwrap().get_last_embedded_message_id()?,
                Some(7)
            );
            assert!(!temp.path().join("models").exists());
        }
    }
    assert_eq!(fs::read(&index_path)?, b"not a vector index");
    Ok(())
}

#[test]
fn gh481_incremental_semantic_preflight_allows_valid_hash_append() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Mutex::new(FrankenStorage::open(&temp.path().join("archive.db"))?);
    let indexer = SemanticIndexer::new("hash", Some(temp.path()))?;
    let index_path = vector_index_path(temp.path(), indexer.embedder_id());
    fs::create_dir_all(index_path.parent().context("vector parent")?)?;
    let writer = FsVectorIndex::create_with_revision(
        &index_path,
        indexer.embedder_id(),
        crate::indexer::semantic::HASH_VECTOR_SPACE_REVISION,
        384,
        frankensearch::index::Quantization::F16,
    )?;
    writer.finish()?;
    publish_direct_semantic_artifact(
        &storage.lock().unwrap(),
        temp.path(),
        &index_path,
        indexer.embedder_id(),
        384,
        0,
        1,
    )?;
    assert_eq!(
        embed_incremental_semantic_inputs(
            "hash",
            temp.path(),
            &storage,
            vec![EmbeddingInput::new(8, "Explain the parser bug")],
            8,
            "filtered test watermark",
            "appended test watermark",
        )?,
        1
    );
    assert_eq!(
        inspect_semantic_artifact(&index_path)?.live_record_count(),
        1
    );
    assert_eq!(
        storage.lock().unwrap().get_last_embedded_message_id()?,
        Some(8)
    );
    assert_eq!(
        embed_incremental_semantic_inputs(
            "hash",
            temp.path(),
            &storage,
            vec![],
            9,
            "filtered test watermark",
            "appended test watermark",
        )?,
        0
    );
    assert_eq!(
        storage.lock().unwrap().get_last_embedded_message_id()?,
        Some(9)
    );
    assert_eq!(
        inspect_semantic_artifact(&index_path)?.live_record_count(),
        1
    );
    Ok(())
}

#[test]
fn continuous_watch_semantic_retains_cooldown_work_and_publishes_all_rows() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Mutex::new(FrankenStorage::open(&temp.path().join("archive.db"))?);
    seed_archive_conversation(&storage.lock().unwrap(), "watch-base");
    run_targeted_semantic_watch_once_publish("hash", temp.path(), &storage, 1, 0)?;
    let mut manifest = SemanticManifest::load_or_default(temp.path())?;
    let old_fingerprint = manifest
        .fast_tier
        .as_ref()
        .context("fast tier")?
        .db_fingerprint
        .clone();
    manifest.publish_hnsw(crate::search::semantic_manifest::HnswRecord {
        base_tier: SemanticTierKind::Fast,
        embedder_id: "fnv1a-384".to_string(),
        ef_search: 64,
        index_path: "vector_index/hnsw-fnv1a-384.chsw".to_string(),
        size_bytes: 1,
        built_at_ms: 1,
        ready: true,
    });
    manifest.save(temp.path())?;
    let mut maintenance = WatchSemanticMaintenance::new(1);
    let cooldown = Duration::from_secs(60);
    for name in ["watch-cooldown-first", "watch-cooldown-second"] {
        seed_archive_conversation(&storage.lock().unwrap(), name);
        maintenance.record_ingest(1, [temp.path().join(name)]);
        assert!(
            maintenance
                .reconcile_if_due("hash", temp.path(), &storage, cooldown, None)?
                .is_none()
        );
    }
    assert_eq!(maintenance.pending_paths.len(), 2);
    assert_eq!(
        SemanticManifest::load_or_default(temp.path())?
            .fast_tier
            .context("fast tier")?
            .db_fingerprint,
        old_fingerprint
    );
    // A retry with no new ingestion must still publish both skipped callbacks.
    maintenance.record_ingest(0, std::iter::empty());
    maintenance.last_attempt = Instant::now()
        .checked_sub(cooldown)
        .context("elapsed cooldown")?;
    let stats = maintenance
        .reconcile_if_due("hash", temp.path(), &storage, cooldown, None)?
        .context("publication")?;
    assert!(stats.published);
    assert_eq!(stats.embedded_docs, 2);
    assert!(maintenance.pending_paths.is_empty());
    assert_eq!(maintenance.pending_conversations, 0);
    assert_eq!(maintenance.pre_watch_conversations, 3);
    let guard = storage.lock().unwrap();
    let canonical = CanonicalSemanticDocuments::from_storage(&guard)?;
    let index = FsVectorIndex::open(&vector_index_path(temp.path(), "fnv1a-384"))?;
    let actual = (0..index.record_count())
        .map(|i| index.doc_id_at(i).map(str::to_owned))
        .collect::<Result<HashSet<_>, _>>()?;
    assert_eq!(actual, canonical.doc_ids);
    let manifest = SemanticManifest::load_or_default(temp.path())?;
    assert!(manifest.hnsw.is_none());
    assert_eq!(
        manifest.fast_tier.context("fast tier")?.db_fingerprint,
        lexical_rebuild_content_fingerprint(&guard, 3)?
    );
    assert_eq!(guard.get_last_embedded_message_id()?, Some(3));
    Ok(())
}

#[test]
fn continuous_watch_semantic_retries_failed_base_and_reconciles_edits() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let storage = Mutex::new(FrankenStorage::open(&temp.path().join("archive.db"))?);
    for name in ["watch-edited", "watch-deleted", "watch-preserved"] {
        seed_archive_conversation(&storage.lock().unwrap(), name);
    }
    run_targeted_semantic_watch_once_publish("hash", temp.path(), &storage, 3, 0)?;
    let index_path = vector_index_path(temp.path(), "fnv1a-384");
    let before = FsVectorIndex::open(&index_path)?;
    let before_vectors = (0..before.record_count())
        .map(|i| Ok((before.doc_id_at(i)?.to_owned(), before.vector_at_f32(i)?)))
        .collect::<Result<HashMap<_, _>>>()?;
    drop(before);
    {
        let guard = storage.lock().unwrap();
        guard.raw().execute_compat("UPDATE messages SET content = 'Explain the corrected watch reconciliation algorithm' WHERE id = 1", &[])?;
        guard
            .raw()
            .execute_compat("DELETE FROM messages WHERE id = 2", &[])?;
    }
    let mut maintenance = WatchSemanticMaintenance::new(3);
    let source = temp.path().join("watch-edited.jsonl");
    maintenance.record_ingest(1, [source.clone()]);
    let held_index = index_path.with_extension("held-for-retry");
    fs::rename(&index_path, &held_index)?;
    let error = maintenance
        .reconcile_if_due("hash", temp.path(), &storage, Duration::ZERO, None)
        .unwrap_err();
    assert!(error.to_string().contains("watch semantic base is missing"));
    assert!(maintenance.pending_paths.contains(&source));
    assert_eq!(maintenance.pending_conversations, 1);
    assert_eq!(
        storage.lock().unwrap().get_last_embedded_message_id()?,
        Some(3)
    );
    fs::rename(&held_index, &index_path)?;
    let stats = maintenance
        .reconcile_if_due("hash", temp.path(), &storage, Duration::ZERO, None)?
        .context("retry publication")?;
    assert!(stats.published);
    assert_eq!(stats.embedded_docs, 1);
    assert!(maintenance.pending_paths.is_empty());
    let canonical = CanonicalSemanticDocuments::from_storage(&storage.lock().unwrap())?;
    let after = FsVectorIndex::open(&index_path)?;
    let mut actual = HashSet::new();
    let mut preserved = 0;
    for i in 0..after.record_count() {
        let id = after.doc_id_at(i)?;
        actual.insert(id.to_owned());
        if let Some(vector) = before_vectors.get(id) {
            assert_eq!(&after.vector_at_f32(i)?, vector);
            preserved += 1;
        }
    }
    assert_eq!(actual, canonical.doc_ids);
    assert_eq!(preserved, 1);
    Ok(())
}

/// Regression test for issue #201: `staged_*` progress fields must
/// surface as JSON `null` when the lexical rebuild pipeline isn't
/// running, not as `0`. The post-init zeros from
/// `capture_lexical_rebuild_pipeline_runtime` look identical to
/// the #196-class "broken-to-zero workers_max" semaphore bug, and
/// triage was misled exactly that way.
#[test]
fn staged_field_or_null_emits_null_outside_rebuild() {
    // During an active rebuild the controller's actual workers_max
    // (always >= 1 by construction) flows through verbatim.
    assert_eq!(staged_field_or_null(true, 6), serde_json::json!(6));
    assert_eq!(staged_field_or_null(true, 0), serde_json::json!(0));

    // Outside the rebuild window the field carries no meaningful
    // signal — emit null so consumers (humans, dashboards, the
    // stall watchdog's diagnostic dump) don't read it as a 0-worker
    // configuration bug.
    assert_eq!(staged_field_or_null(false, 6), serde_json::Value::Null);
    assert_eq!(staged_field_or_null(false, 0), serde_json::Value::Null);
}

/// xicw9: an ingest OOM injected by one test must not reach an ingest
/// running on another test's thread. With the old process-env injection,
/// the spawned thread below saw the fault and a concurrent
/// `streaming_configured_scan_roots_ignore_global_watermark` inserted
/// nothing.
#[test]
fn ingest_test_oom_injection_is_scoped_to_the_injecting_thread() {
    let convs = vec![norm_conv(
        Some("oom-scope"),
        vec![norm_msg(0, 1_700_000_000_000)],
    )];
    let guard = inject_non_watch_ingest_test_oom(1);
    let lexical_guard = inject_incremental_lexical_update_test_oom();
    assert!(should_inject_non_watch_ingest_test_oom(&convs));
    assert!(INCREMENTAL_LEXICAL_UPDATE_TEST_OOM.with(std::cell::Cell::get));

    let other_convs = convs.clone();
    let (other_non_watch, other_lexical) = std::thread::spawn(move || {
        (
            should_inject_non_watch_ingest_test_oom(&other_convs),
            INCREMENTAL_LEXICAL_UPDATE_TEST_OOM.with(std::cell::Cell::get),
        )
    })
    .join()
    .expect("probe thread");
    assert!(!other_non_watch, "non-watch OOM leaked to another thread");
    assert!(!other_lexical, "lexical OOM leaked to another thread");

    assert!(
        !should_inject_non_watch_ingest_test_oom(&[]),
        "a batch smaller than the minimum is not failed"
    );
    drop(lexical_guard);
    assert!(!INCREMENTAL_LEXICAL_UPDATE_TEST_OOM.with(std::cell::Cell::get));
    assert!(should_inject_non_watch_ingest_test_oom(&convs));
    drop(guard);
    assert!(!should_inject_non_watch_ingest_test_oom(&convs));
}

#[test]
fn active_session_recent_write_detection_is_watch_opt_in() {
    let tmp = TempDir::new().expect("tempdir");
    let source_path = tmp.path().join("active-session.jsonl");
    std::fs::write(&source_path, b"{\"type\":\"session\"}\n").expect("write source");

    let inactive_filter = ActiveSessionSourceFilter::with_recent_write_window_for_test(None);
    assert_eq!(inactive_filter.active_writer_reason(&source_path), None);

    let active_filter = ActiveSessionSourceFilter::with_recent_write_window_for_test(Some(
        Duration::from_secs(3_600),
    ));
    assert_eq!(
        active_filter.active_writer_reason(&source_path),
        Some(ActiveSessionSourceReason::RecentlyModified)
    );
    assert!(should_skip_active_session_source(
        &active_filter,
        SourceKind::Local,
        &source_path
    ));
    assert!(
        !should_skip_active_session_source(&active_filter, SourceKind::Ssh, &source_path),
        "remote mirrors are immutable local copies, not live provider writers"
    );
}

#[test]
fn out_of_memory_classifier_rejects_contextual_substrings() {
    let typed: anyhow::Error = crate::franken_sync::FrankenError::OutOfMemory.into();
    assert!(error_is_out_of_memory(&typed));

    let exact = anyhow::anyhow!("out of memory");
    assert!(error_is_out_of_memory(&exact));

    let contextual = anyhow::anyhow!("connector parse failed: not enough memory in record");
    assert!(
        !error_is_out_of_memory(&contextual),
        "contextual text must not be promoted into permanent OOM quarantine"
    );
}

#[test]
fn poison_quarantine_record_preserves_full_error_chain() {
    use crate::connectors::{NormalizedConversation, NormalizedMessage};

    let tmp = TempDir::new().expect("tempdir");
    let data_dir = tmp.path().join("poison-chain");

    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: Some("chain-capture-conv".into()),
        title: Some("chain capture".into()),
        workspace: Some(std::path::PathBuf::from("/ws/chain")),
        source_path: std::path::PathBuf::from("/log/chain.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_010),
        metadata: serde_json::json!({}),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".into(),
            author: None,
            created_at: Some(1_700_000_000_005),
            content: "trigger".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };

    // Build a realistic wrapped chain: low-level FrankenError::OutOfMemory
    // bubbling up through two layers of context (mirrors how it actually
    // arrives at the quarantine path inside the watch ingest flow).
    let typed: anyhow::Error = crate::franken_sync::FrankenError::OutOfMemory.into();
    let with_inner_ctx = typed.context("vdbe register allocation");
    let error: anyhow::Error = with_inner_ctx.context("ingest_batch_with_semantic_delta");

    crate::indexer::record_poison_conversation(
        &data_dir,
        WATCH_INGEST_POISON_FILE,
        "watch-ingest-out-of-memory",
        &conv,
        &error,
        None,
    )
    .expect("record poison");

    let path = data_dir.join("quarantine").join(WATCH_INGEST_POISON_FILE);
    let contents = std::fs::read_to_string(&path).expect("read quarantine file");
    let line = contents
        .lines()
        .find(|line| !line.trim().is_empty())
        .expect("at least one record");
    let value: serde_json::Value = serde_json::from_str(line).expect("parse record");
    let last_error = value
        .get("last_error")
        .and_then(serde_json::Value::as_str)
        .expect("last_error string");
    assert!(
        last_error.contains("ingest_batch_with_semantic_delta"),
        "last_error must include outer context: {last_error}"
    );
    assert!(
        last_error.contains("vdbe register allocation"),
        "last_error must include intermediate context: {last_error}"
    );
    assert!(
        last_error.contains("out of memory"),
        "last_error must include innermost cause: {last_error}"
    );
    assert!(
        last_error.contains(" | "),
        "last_error must be ` | `-joined chain: {last_error}"
    );
}

/// #364: the poison record carries the memory-pressure attribution so
/// post-mortem triage can tell a genuine host-pressure OOM from a
/// precautionary large-conversation quarantine — and omits it entirely
/// (pre-#364 shape) when no attribution is supplied.
#[test]
fn poison_record_carries_memory_pressure_attribution() {
    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: Some("attrib-conv".into()),
        title: None,
        workspace: None,
        source_path: std::path::PathBuf::from("/log/attrib.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_010),
        metadata: serde_json::json!({}),
        messages: vec![],
    };
    let error: anyhow::Error = crate::franken_sync::FrankenError::OutOfMemory.into();
    let read_record = |data_dir: &std::path::Path| -> serde_json::Value {
        let path = data_dir.join("quarantine").join(INDEX_INGEST_POISON_FILE);
        let contents = std::fs::read_to_string(&path).expect("read quarantine file");
        let line = contents
            .lines()
            .find(|line| !line.trim().is_empty())
            .expect("at least one record");
        serde_json::from_str(line).expect("parse record")
    };

    // Genuine host memory pressure.
    let real = tempfile::tempdir().expect("tempdir");
    crate::indexer::record_poison_conversation(
        real.path(),
        INDEX_INGEST_POISON_FILE,
        "index-ingest-out-of-memory",
        &conv,
        &error,
        Some(crate::indexer::PoisonMemoryAttribution {
            real_memory_pressure: Some(true),
            indexed_text_bytes: 12_345,
        }),
    )
    .expect("record poison");
    let rec = read_record(real.path());
    assert_eq!(
        rec.get("memory_pressure")
            .and_then(serde_json::Value::as_str),
        Some("real")
    );
    assert_eq!(
        rec.get("quarantine_trigger")
            .and_then(serde_json::Value::as_str),
        Some("real_host_memory_pressure")
    );
    assert_eq!(
        rec.get("indexed_text_bytes")
            .and_then(serde_json::Value::as_u64),
        Some(12_345)
    );

    // Pressure could not be sampled -> precautionary large-conversation
    // quarantine trigger.
    let unknown = tempfile::tempdir().expect("tempdir");
    crate::indexer::record_poison_conversation(
        unknown.path(),
        INDEX_INGEST_POISON_FILE,
        "index-ingest-out-of-memory",
        &conv,
        &error,
        Some(crate::indexer::PoisonMemoryAttribution {
            real_memory_pressure: None,
            indexed_text_bytes: 999,
        }),
    )
    .expect("record poison");
    let rec = read_record(unknown.path());
    assert_eq!(
        rec.get("memory_pressure")
            .and_then(serde_json::Value::as_str),
        Some("unknown")
    );
    assert_eq!(
        rec.get("quarantine_trigger")
            .and_then(serde_json::Value::as_str),
        Some("unknown_pressure_large_conversation_precaution")
    );

    // No attribution -> fields omitted (pre-#364 record shape).
    let none = tempfile::tempdir().expect("tempdir");
    crate::indexer::record_poison_conversation(
        none.path(),
        INDEX_INGEST_POISON_FILE,
        "index-ingest-out-of-memory",
        &conv,
        &error,
        None,
    )
    .expect("record poison");
    let rec = read_record(none.path());
    assert!(
        rec.get("memory_pressure").is_none() && rec.get("quarantine_trigger").is_none(),
        "attribution fields must be omitted when no attribution is supplied"
    );
}

#[test]
fn heartbeat_index_run_lock_refreshes_updated_at_without_touching_identity_fields() {
    let tmp = TempDir::new().unwrap();
    let lock_path = tmp.path().join("index-run.lock");
    std::fs::write(
        &lock_path,
        "pid=123\nstarted_at_ms=111\nupdated_at_ms=222\ndb_path=/tmp/db.sqlite\nmode=index\njob_id=lexical_refresh-123\njob_kind=lexical_refresh\nphase=index\n",
    )
    .unwrap();

    heartbeat_index_run_lock(tmp.path()).unwrap();
    heartbeat_index_run_lock(tmp.path()).unwrap();

    let refreshed = read_index_run_lock_metadata_for_test(&lock_path).unwrap();
    assert!(refreshed.contains("pid=123"));
    assert!(refreshed.contains("started_at_ms=111"));
    assert!(refreshed.contains("db_path=/tmp/db.sqlite"));
    assert!(refreshed.contains("job_id=lexical_refresh-123"));
    let updated_at_raw_values: Vec<&str> = refreshed
        .lines()
        .filter_map(|line| line.strip_prefix("updated_at_ms="))
        .collect();
    assert_eq!(
        updated_at_raw_values.len(),
        1,
        "heartbeat refresh should replace the existing updated_at_ms line"
    );
    let updated_at_value = updated_at_raw_values
        .first()
        .and_then(|value| value.parse::<i64>().ok());
    assert!(
        updated_at_value.is_some_and(|value| value >= 222),
        "updated_at_ms should be parseable and should not move backwards: {updated_at_raw_values:?}"
    );

    let temp_artifacts: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(".index-run.lock.tmp."))
        .collect();
    assert!(
        temp_artifacts.is_empty(),
        "successful heartbeat refresh should not leave temp files: {temp_artifacts:?}"
    );
}

/// Regression for #258 (review follow-up — fix-and-test-the-writer-side).
///
/// The original #258 fix landed the READER side
/// (`asset_state::maintenance_stall_age_ms` consumes
/// `last_progress_at_ms`) and the HEARTBEAT-PRESERVATION test below,
/// but the WRITER side — `IndexRunLockGuard::write_metadata`
/// actually emitting a `last_progress_at_ms=...` line into the
/// lock file — was missing in the initial commit. Without this
/// test, every lock file in production had
/// `last_progress_at_ms=None`, so the stall detector never fired
/// and the wedged-indexer fix was inert.
///
/// This test exercises the REAL writer path (no string fixture):
/// it calls `acquire_index_run_lock` which calls `write_metadata`
/// on construction, then re-reads the lock file off disk and
/// asserts the field is populated. The same test passes through
/// `read_search_maintenance_snapshot` to verify the value flows
/// end-to-end into the same data the status JSON consumes.
#[test]
fn acquire_index_run_lock_writes_last_progress_at_ms_field() -> Result<()> {
    use crate::search::asset_state::read_search_maintenance_snapshot;
    let tmp = TempDir::new()?;
    let db_path = tmp.path().join("agent_search.db");
    std::fs::write(&db_path, b"placeholder")?;
    let before_ms = crate::storage::sqlite::FrankenStorage::now_millis();
    let guard = acquire_index_run_lock(tmp.path(), &db_path, SearchMaintenanceMode::Index)?;
    let after_ms = crate::storage::sqlite::FrankenStorage::now_millis();

    // The on-disk lock file must include the `last_progress_at_ms`
    // key with a fresh timestamp. Parsing it lets a future change
    // to the field's value type surface as a precise test failure.
    let lock_path = tmp.path().join("index-run.lock");
    let raw = read_index_run_lock_metadata_for_test(&lock_path)?;
    let last_progress_lines: Vec<&str> = raw
        .lines()
        .filter_map(|line| line.strip_prefix("last_progress_at_ms="))
        .collect();
    assert_eq!(
        last_progress_lines.len(),
        1,
        "lock file must contain exactly one last_progress_at_ms line; got {raw:?}",
    );
    let value: i64 = last_progress_lines
        .first()
        .context("last_progress_at_ms line missing")?
        .parse()
        .context("last_progress_at_ms must parse as i64")?;
    assert!(
        (before_ms..=after_ms).contains(&value),
        "last_progress_at_ms ({value}) must be within [before_ms={before_ms}, after_ms={after_ms}] of the acquire_index_run_lock call",
    );

    // Cross-check via the same snapshot reader the status JSON uses:
    // `last_progress_at_ms` must surface as Some(..) (NOT None,
    // which is what production lock files had until this fix).
    let snapshot = read_search_maintenance_snapshot(tmp.path());
    assert_eq!(
        snapshot.last_progress_at_ms,
        Some(value),
        "snapshot reader must surface last_progress_at_ms; #258 stall detector is inert otherwise",
    );

    drop(guard);
    Ok(())
}

#[test]
fn gh515_semantic_admission_preserves_owner_and_releases_heartbeat() -> Result<()> {
    use crate::search::asset_state::read_search_maintenance_snapshot;

    let tmp = TempDir::new()?;
    let db_path = tmp.path().join("agent_search.db");
    let lexical = acquire_index_run_lock(tmp.path(), &db_path, SearchMaintenanceMode::Index)?;
    let original = read_search_maintenance_snapshot(tmp.path());
    assert!(original.active);
    assert!(try_acquire_semantic_backfill_lock(tmp.path(), &db_path)?.is_none());
    let contended = read_search_maintenance_snapshot(tmp.path());
    assert_eq!(contended.job_id, original.job_id);
    assert_eq!(contended.last_progress_at_ms, original.last_progress_at_ms);
    drop(lexical);

    let semantic = try_acquire_semantic_backfill_lock(tmp.path(), &db_path)?
        .context("semantic job must be admitted after the lexical owner drains")?;
    assert!(acquire_index_run_lock(tmp.path(), &db_path, SearchMaintenanceMode::Index).is_err());
    let active = read_search_maintenance_snapshot(tmp.path());
    assert!(active.active);
    assert_eq!(
        active.job_kind,
        Some(SearchMaintenanceJobKind::SemanticRebuild)
    );
    drop(semantic);

    let after = read_search_maintenance_snapshot(tmp.path());
    assert!(!after.active);
    assert!(after.job_id.is_none());
    assert!(read_index_run_lock_metadata_for_test(&tmp.path().join("index-run.lock"))?.is_empty());
    let _next = acquire_index_run_lock(tmp.path(), &db_path, SearchMaintenanceMode::Index)?;

    // Permission/path errors must not be mistaken for a busy owner and
    // silently put accepted daemon work into an endless retry queue.
    let not_a_directory = tmp.path().join("not-a-directory");
    fs::write(&not_a_directory, b"preserve")?;
    assert!(try_acquire_semantic_backfill_lock(&not_a_directory, &db_path).is_err());
    assert_eq!(fs::read(&not_a_directory)?, b"preserve");
    Ok(())
}

#[test]
fn gh515_archive_admission_serializes_different_data_directories() -> Result<()> {
    use crate::search::asset_state::read_search_maintenance_snapshot;

    for archive_exists in [false, true] {
        let tmp = TempDir::new()?;
        let db_path = tmp.path().join("archive.db");
        if archive_exists {
            fs::write(&db_path, b"canonical bytes")?;
        }
        let first_data = tmp.path().join("first");
        let second_data = tmp.path().join("second");
        let owner = acquire_semantic_backfill_lock(&first_data, &db_path)?;
        let original = read_search_maintenance_snapshot(&first_data);
        let lease_path = archive_lease::ArchiveWriteLease::path(&db_path)?;
        assert_eq!(db_path.exists(), archive_exists);
        assert_eq!(fs::metadata(&lease_path)?.len(), 0);

        let error = acquire_index_run_lock(&second_data, &db_path, SearchMaintenanceMode::Index)
            .expect_err("a different data directory must not admit the same archive");
        assert_eq!(
            error.downcast_ref::<IndexRunBusy>().unwrap().lock_path,
            lease_path,
        );
        assert!(try_acquire_semantic_backfill_lock(&second_data, &db_path)?.is_none());
        assert!(
            read_index_run_lock_metadata_for_test(&second_data.join("index-run.lock"))?.is_empty()
        );
        assert!(!read_search_maintenance_snapshot(&second_data).active);
        assert_eq!(
            read_search_maintenance_snapshot(&first_data).job_id,
            original.job_id
        );

        // Creating the database while its first owner is admitted must not
        // switch the lease key from the missing-leaf path to a new inode.
        if !archive_exists {
            fs::write(&db_path, b"canonical bytes")?;
            assert!(try_acquire_semantic_backfill_lock(&second_data, &db_path)?.is_none());
        }
        drop(owner);
        let successor = try_acquire_semantic_backfill_lock(&second_data, &db_path)?
            .context("the next data directory must acquire the released archive")?;
        assert!(read_search_maintenance_snapshot(&second_data).active);
        assert!(!read_search_maintenance_snapshot(&first_data).active);
        assert_eq!(fs::metadata(&lease_path)?.len(), 0);
        assert_eq!(fs::read(&db_path)?, b"canonical bytes");
        drop(successor);
        assert!(!read_search_maintenance_snapshot(&second_data).active);
        assert!(
            read_index_run_lock_metadata_for_test(&second_data.join("index-run.lock"))?.is_empty()
        );
    }
    Ok(())
}

#[test]
fn gh515_archive_admission_releases_partial_admission_and_keeps_archives_independent() -> Result<()>
{
    let tmp = TempDir::new()?;
    let first_data = tmp.path().join("first");
    let second_data = tmp.path().join("second");
    let first_db = tmp.path().join("first.db");
    let second_db = tmp.path().join("second.db");
    let first = acquire_semantic_backfill_lock(&first_data, &first_db)?;
    assert!(try_acquire_semantic_backfill_lock(&second_data, &first_db)?.is_none());

    // The failed archive attempt must release its preliminary data-dir
    // lock; an unrelated archive can then use that data dir immediately.
    let second = acquire_semantic_backfill_lock(&second_data, &second_db)?;
    assert!(try_acquire_semantic_backfill_lock(&first_data, &second_db)?.is_none());
    drop(second);
    drop(first);

    let blocked_parent = tmp.path().join("not-a-directory");
    fs::write(&blocked_parent, b"preserve")?;
    let error =
        try_acquire_semantic_backfill_lock(&second_data, &blocked_parent.join("archive.db"))
            .err()
            .context("an archive path error must remain an error, not deferred work")?;
    assert!(!error.is::<IndexRunBusy>());
    assert_eq!(fs::read(blocked_parent)?, b"preserve");
    let _retry = acquire_semantic_backfill_lock(&second_data, &second_db)?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn gh515_archive_admission_unifies_symlink_spellings_before_and_after_creation() -> Result<()> {
    use std::os::unix::fs::{MetadataExt, symlink};

    for archive_exists in [false, true] {
        let tmp = TempDir::new()?;
        let archive_dir = tmp.path().join("archives");
        fs::create_dir(&archive_dir)?;
        let alias_dir = tmp.path().join("alias");
        symlink(&archive_dir, &alias_dir)?;
        let db_path = archive_dir.join("archive.db");
        if archive_exists {
            fs::write(&db_path, b"preserve")?;
        }
        let first_data = tmp.path().join("first");
        let second_data = tmp.path().join("second");
        let owner = acquire_semantic_backfill_lock(&first_data, &db_path)?;
        let lease_path = archive_lease::ArchiveWriteLease::path(&db_path)?;
        let before = fs::metadata(&lease_path)?;
        assert!(
            try_acquire_semantic_backfill_lock(&second_data, &alias_dir.join("archive.db"))?
                .is_none()
        );
        if archive_exists {
            let alias_file = tmp.path().join("archive-link.db");
            symlink(&db_path, &alias_file)?;
            assert!(try_acquire_semantic_backfill_lock(&second_data, &alias_file)?.is_none());
        }
        drop(owner);
        let retry = acquire_semantic_backfill_lock(&second_data, &alias_dir.join("archive.db"))?;
        let after = fs::metadata(&lease_path)?;
        assert_eq!((before.dev(), before.ino()), (after.dev(), after.ino()));
        assert_eq!(after.len(), 0);
        drop(retry);
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn gh515_archive_admission_resolves_symlinks_before_parent_components() -> Result<()> {
    use std::os::unix::fs::symlink;

    for archive_exists in [false, true] {
        let tmp = TempDir::new()?;
        let lexical_parent = tmp.path().join("lexical-parent");
        let archive_parent = tmp.path().join("actual-parent");
        fs::create_dir(&lexical_parent)?;
        fs::create_dir_all(archive_parent.join("child"))?;
        symlink(archive_parent.join("child"), lexical_parent.join("link"))?;
        let db_path = archive_parent.join("archive.db");
        let spelling = lexical_parent.join("link/../archive.db");
        if archive_exists {
            fs::write(&db_path, b"canonical bytes")?;
        }
        let first_data = tmp.path().join("first");
        let second_data = tmp.path().join("second");
        let owner = acquire_semantic_backfill_lock(&first_data, &db_path)?;
        assert!(try_acquire_semantic_backfill_lock(&second_data, &spelling)?.is_none());

        // Delayed writer admission from a readonly rebuild must preserve
        // exactly the same OS path semantics as immediate admission.
        let mut readonly = acquire_index_run_lock_with_archive_admission(
            &second_data,
            &spelling,
            SearchMaintenanceMode::Index,
            SearchMaintenanceJobKind::LexicalRefresh,
            ArchiveAdmission::DataDirOnly,
        )?;
        assert!(
            readonly
                .acquire_archive_write_lease()
                .expect_err("the readonly upgrade must respect the canonical owner")
                .is::<IndexRunBusy>()
        );
        if !archive_exists {
            fs::write(&spelling, b"canonical bytes")?;
        }
        assert_eq!(fs::canonicalize(&spelling)?, fs::canonicalize(&db_path)?);
        drop(owner);
        readonly.acquire_archive_write_lease()?;
        assert!(try_acquire_semantic_backfill_lock(&first_data, &db_path)?.is_none());
        assert!(
            !lexical_parent
                .join("archive.db.cass-index-run.lock")
                .exists()
        );
        drop(readonly);
        let _retry = acquire_semantic_backfill_lock(&first_data, &db_path)?;
        assert_eq!(fs::read(&db_path)?, b"canonical bytes");
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn gh515_archive_admission_unifies_dangling_leaf_targets_and_creates_missing_parents() -> Result<()>
{
    use std::os::unix::fs::symlink;

    let tmp = TempDir::new()?;
    let db_path = tmp.path().join("new/nested/archive.db");
    let spelling = tmp.path().join("archive-link.db");
    symlink("new/nested/archive.db", &spelling)?;
    let first_data = tmp.path().join("first");
    let second_data = tmp.path().join("second");
    let owner = acquire_semantic_backfill_lock(&first_data, &spelling)?;
    assert!(db_path.parent().unwrap().is_dir());
    assert!(!db_path.exists(), "admission must not create the archive");
    assert!(try_acquire_semantic_backfill_lock(&second_data, &db_path)?.is_none());
    fs::write(&spelling, b"created through the original dangling link")?;
    assert!(try_acquire_semantic_backfill_lock(&second_data, &db_path)?.is_none());
    assert_eq!(
        archive_lease::ArchiveWriteLease::path(&spelling)?,
        archive_lease::ArchiveWriteLease::path(&db_path)?,
    );
    drop(owner);
    let successor = acquire_semantic_backfill_lock(&second_data, &db_path)?;
    assert!(try_acquire_semantic_backfill_lock(&first_data, &spelling)?.is_none());
    drop(successor);

    let cycle_a = tmp.path().join("cycle-a.db");
    let cycle_b = tmp.path().join("cycle-b.db");
    symlink(&cycle_b, &cycle_a)?;
    symlink(&cycle_a, &cycle_b)?;
    let error = try_acquire_semantic_backfill_lock(&first_data, &cycle_a)
        .err()
        .context("unresolvable symlinks must fail without claiming a different archive")?;
    assert!(!error.is::<IndexRunBusy>());
    assert!(read_index_run_lock_metadata_for_test(&first_data.join("index-run.lock"))?.is_empty());
    let _retry = acquire_semantic_backfill_lock(&first_data, &spelling)?;
    assert_eq!(
        fs::read(&db_path)?,
        b"created through the original dangling link"
    );
    Ok(())
}

#[test]
fn gh515_writer_admission_precedes_archive_preparation() -> Result<()> {
    let tmp = TempDir::new()?;
    let db_path = tmp.path().join("agent_search.db");
    let original = b"an archive being initialized by its existing owner";
    fs::write(&db_path, original)?;
    let _owner = acquire_semantic_backfill_lock(tmp.path(), &db_path)?;
    let snapshot = crate::search::asset_state::read_search_maintenance_snapshot(tmp.path());
    let result = run_index_with_local_connector_roots(
        IndexOptions {
            full: false,
            force_rebuild: false,
            watch: false,
            watch_once_paths: None,
            db_path: db_path.clone(),
            data_dir: tmp.path().to_path_buf(),
            semantic: false,
            build_hnsw: false,
            embedder: "hash".to_string(),
            progress: Some(Arc::new(IndexingProgress::default())),
            watch_interval_secs: 30,
        },
        HashMap::new(),
        None,
    );
    assert!(
        result
            .expect_err("a contender must not inspect another owner's archive")
            .is::<IndexRunBusy>()
    );
    assert_eq!(fs::read(&db_path)?, original);
    assert!(!tmp.path().join("index").exists());
    assert_eq!(
        crate::search::asset_state::read_search_maintenance_snapshot(tmp.path()).job_id,
        snapshot.job_id,
    );
    Ok(())
}

#[test]
fn gh515_stop_before_preparation_preserves_archive_and_does_not_claim_writer() -> Result<()> {
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    fs::create_dir(&data_dir)?;
    let db_path = data_dir.join("agent_search.db");
    // An already-cancelled invocation must not even probe or migrate its
    // archive. A sentinel makes an accidental open fail unmistakably.
    let original = b"archive not to be opened by a cancelled invocation";
    fs::write(&db_path, original)?;
    let progress = Arc::new(IndexingProgress::default());
    progress.request_stop();
    let result = run_index_with_local_connector_roots(
        IndexOptions {
            full: false,
            force_rebuild: false,
            watch: false,
            watch_once_paths: None,
            db_path: db_path.clone(),
            data_dir: data_dir.clone(),
            semantic: false,
            build_hnsw: false,
            embedder: "hash".to_string(),
            progress: Some(progress),
            watch_interval_secs: 30,
        },
        HashMap::new(),
        None,
    );
    assert!(
        result
            .expect_err("cancelled index must stop")
            .is::<IndexInterrupted>()
    );
    assert_eq!(fs::read(db_path)?, original);
    assert!(!data_dir.join("index-run.lock").exists());
    assert!(!data_dir.join("index").exists());
    Ok(())
}

#[test]
fn issue_342_semantic_index_lock_identifies_rebuild_job_and_phase() -> Result<()> {
    use crate::search::asset_state::read_search_maintenance_snapshot;

    let tmp = TempDir::new()?;
    let db_path = tmp.path().join("agent_search.db");
    std::fs::write(&db_path, b"placeholder")?;
    let mut guard = acquire_index_run_lock_with_job_kind(
        tmp.path(),
        &db_path,
        SearchMaintenanceMode::Index,
        SearchMaintenanceJobKind::SemanticRebuild,
    )?;
    guard.set_phase(SearchMaintenanceMode::Index, "semantic:embedding")?;

    let snapshot = read_search_maintenance_snapshot(tmp.path());
    assert_eq!(
        snapshot.job_kind,
        Some(SearchMaintenanceJobKind::SemanticRebuild)
    );
    assert_eq!(snapshot.phase.as_deref(), Some("semantic:embedding"));
    assert!(
        snapshot
            .job_id
            .as_deref()
            .is_some_and(|job_id| job_id.starts_with("semantic_rebuild-"))
    );

    drop(guard);
    Ok(())
}

#[test]
fn gh424_analytics_maintenance_lock_reports_kind_and_excludes_other_writers() -> Result<()> {
    use crate::search::asset_state::read_search_maintenance_snapshot;
    let tmp = TempDir::new()?;
    let db_path = tmp.path().join("agent_search.db");
    let guard = acquire_search_maintenance_mutation_lock(
        tmp.path(),
        &db_path,
        SearchMaintenanceJobKind::AnalyticsRebuild,
    )?;
    let snapshot = read_search_maintenance_snapshot(tmp.path());
    assert!(snapshot.active);
    assert_eq!(
        snapshot.job_kind,
        Some(SearchMaintenanceJobKind::AnalyticsRebuild)
    );
    assert!(
        snapshot
            .job_id
            .as_deref()
            .is_some_and(|id| id.starts_with("analytics_rebuild-"))
    );
    let Err(conflict) = acquire_semantic_backfill_lock(tmp.path(), &db_path) else {
        anyhow::bail!("analytics did not exclude semantic/canonical maintenance");
    };
    assert!(format!("{conflict:#}").contains("already holds"));
    drop(guard);
    let retry = acquire_semantic_backfill_lock(tmp.path(), &db_path)?;
    assert_eq!(
        read_search_maintenance_snapshot(tmp.path()).job_kind,
        Some(SearchMaintenanceJobKind::SemanticRebuild)
    );
    drop(retry);
    Ok(())
}
