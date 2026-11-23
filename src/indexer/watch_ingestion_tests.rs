//! GH538: source receipts must bound watch retries without hiding unpublished
//! content. These tests use real connector parsing, canonical transactions and
//! Quill publication; the active-source control holds a real advisory lock.

use super::*;
use frankensearch::quill::query::{Query, QueryField};
use serial_test::serial;
use tempfile::TempDir;

mod receipt_recovery_tests;

struct Fixture {
    _temp: TempDir,
    kind: ConnectorKind,
    root: ScanRoot,
    options: IndexOptions,
    index_path: PathBuf,
    previous_active_skip: bool,
}

impl Fixture {
    fn new(kind: ConnectorKind) -> Result<Self> {
        let temp = TempDir::new()?;
        let data = temp.path().join("cass-data");
        fs::create_dir_all(&data)?;
        let root = ScanRoot::local(temp.path().join(match kind {
            ConnectorKind::Codex => ".codex/sessions",
            ConnectorKind::Claude => ".claude/projects/receipt-project",
            other => anyhow::bail!("unsupported watch fixture connector: {other:?}"),
        }));
        fs::create_dir_all(&root.path)?;
        let index_path = index_dir(&data)?;
        let options = IndexOptions {
            full: false,
            watch: true,
            force_rebuild: false,
            db_path: data.join("agent_search.db"),
            data_dir: data,
            semantic: false,
            build_hnsw: false,
            embedder: "hash".into(),
            progress: None,
            watch_once_paths: None,
            watch_interval_secs: 1,
        };
        Ok(Self {
            _temp: temp,
            kind,
            root,
            options,
            index_path,
            previous_active_skip: ACTIVE_SESSION_SOURCE_SKIP_OBSERVED
                .swap(false, Ordering::Relaxed),
        })
    }

    fn path(&self, id: &str) -> PathBuf {
        self.root.path.join(format!("rollout-{id}.jsonl"))
    }

    fn source(&self, id: &str, text: &str) -> Result<PathBuf> {
        let path = self.path(id);
        let mut file = File::create(&path)?;
        if self.kind == ConnectorKind::Codex {
            writeln!(
                file,
                "{}",
                serde_json::json!({
                    "timestamp": "2025-09-30T15:42:00.000Z", "type": "session_meta",
                    "payload": {"id": id, "cwd": "/receipt-workspace"}
                })
            )?;
        }
        drop(file);
        self.append(&path, id, text)?;
        age_source(&path)?;
        Ok(path)
    }

    fn append(&self, path: &Path, id: &str, text: &str) -> Result<()> {
        let record = if self.kind == ConnectorKind::Codex {
            serde_json::json!({
                "timestamp": "2025-09-30T15:43:00.000Z", "type": "response_item",
                "payload": {"type": "message", "role": "user",
                    "content": [{"type": "input_text", "text": text}]}
            })
        } else {
            serde_json::json!({
                "timestamp": "2025-09-30T15:43:00.000Z", "type": "user",
                "sessionId": id, "uuid": format!("{id}-{text}"), "cwd": "/receipt-workspace",
                "message": {"role": "user", "content": text}
            })
        };
        let mut file = OpenOptions::new().append(true).open(path)?;
        writeln!(file, "{record}")?;
        Ok(())
    }

    /// One merged Codex thread: an original rollout plus a continuation, both
    /// carrying the thread's paginated session_meta. Returns (original,
    /// continuation). Synthetic UUIDs and prompts; no personal data.
    fn paginated_thread(
        &self,
        thread: &str,
        segment: &str,
        original_text: &str,
        continuation_text: &str,
    ) -> Result<(PathBuf, PathBuf)> {
        let original = self
            .root
            .path
            .join(format!("rollout-2026-11-12T22-40-18-{thread}.jsonl"));
        let continuation = self.root.path.join(format!(
            "rollout-2026-11-12T22-47-12-{thread}_{segment}.jsonl"
        ));
        for (path, text, ts) in [
            (&original, original_text, "2026-11-12T21:40:20Z"),
            (&continuation, continuation_text, "2026-11-12T21:47:20Z"),
        ] {
            let mut file = File::create(path)?;
            writeln!(
                file,
                "{}",
                serde_json::json!({
                    "timestamp": "2026-11-12T21:40:18Z", "type": "session_meta",
                    "payload": {"id": thread, "session_id": thread, "cwd": "/receipt-workspace",
                        "history_mode": "paginated", "originator": "codex-tui", "source": "cli"}
                })
            )?;
            writeln!(
                file,
                "{}",
                serde_json::json!({
                    "timestamp": ts, "type": "response_item",
                    "payload": {"type": "message", "role": "user",
                        "content": [{"type": "input_text", "text": text}]}
                })
            )?;
            drop(file);
            age_source(path)?;
        }
        Ok((original, continuation))
    }

    fn roots(&self) -> [(ConnectorKind, ScanRoot); 1] {
        [(self.kind, self.root.clone())]
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        ACTIVE_SESSION_SOURCE_SKIP_OBSERVED.store(self.previous_active_skip, Ordering::Relaxed);
    }
}

struct Handles {
    storage: Mutex<FrankenStorage>,
    index: Mutex<Option<TantivyIndex>>,
    state: Mutex<HashMap<ConnectorKind, i64>>,
}

impl Handles {
    fn open(fixture: &Fixture) -> Result<Self> {
        Ok(Self {
            storage: Mutex::new(FrankenStorage::open(&fixture.options.db_path)?),
            index: Mutex::new(Some(TantivyIndex::open_or_create(&fixture.index_path)?)),
            state: Mutex::new(load_watch_state(&fixture.options.data_dir)),
        })
    }

    fn scan(
        &self,
        fixture: &Fixture,
        force_full: bool,
    ) -> Result<(WatchReindexOutcome, BTreeSet<PathBuf>)> {
        let filter = ActiveSessionSourceFilter::with_recent_write_window_for_test(None);
        let outcome = reindex_paths_with_semantic_delta(
            &fixture.options,
            vec![fixture.root.path.clone()],
            &fixture.roots(),
            &self.state,
            &self.storage,
            &self.index,
            &fixture.index_path,
            force_full,
            None,
            &filter,
        )?;
        Ok((outcome, filter.take_deferred_sources()))
    }

    fn close(self) -> Result<()> {
        let Self { storage, index, .. } = self;
        drop(index);
        storage.into_inner().unwrap().close()
    }
}

fn age_source(path: &Path) -> Result<()> {
    File::options()
        .write(true)
        .open(path)?
        .set_modified(UNIX_EPOCH + Duration::from_secs(1_600_000_000))?;
    Ok(())
}

fn canonical_rows(handles: &Handles) -> Result<Vec<(i64, i64, i64, String)>> {
    Ok(handles.storage.lock().unwrap().raw().query_map_collect(
        "SELECT conversation_id, id, idx, content FROM messages ORDER BY id",
        &[],
        |row| {
            Ok((
                row.get_typed(0)?,
                row.get_typed(1)?,
                row.get_typed(2)?,
                row.get_typed(3)?,
            ))
        },
    )?)
}

fn search_ids(fixture: &Fixture, term: &str) -> Result<BTreeSet<String>> {
    // These assertions identify message bodies. An unscoped query also
    // matches the conversation title, which every appended message inherits.
    // CASS's parser sanitizes ':' rather than implementing field syntax.
    assert!(
        !term.is_empty()
            && term
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit()),
        "body probes require one normalized term"
    );
    let fields = crate::search::quill_bridge::QuillCassFields::compiled();
    let query = Query::term(vec![QueryField::new(fields.content, 1.0)], term.to_owned());
    search_ids_for_query(fixture, &query)
}

fn search_ids_for_query(fixture: &Fixture, query: &Query) -> Result<BTreeSet<String>> {
    let reader = crate::search::quill_bridge::open_cass_reader(&fixture.index_path)?;
    // Task 5: persisted search-region documents share the index in the
    // `msg_idx >= REGION_DOC_BASE` space and their meta fragments match
    // body-probe terms (the session title contains the probe token).
    // These assertions identify canonical message bodies, so apply the
    // same canonical-message guard the chat-facing query path uses.
    let query = crate::search::quill_bridge::canonical_message_query(query.clone());
    let page = crate::search::quill_bridge::search_paginated(&reader, &query, 512, 0, true)?;
    let ids: BTreeSet<_> = page
        .hits
        .iter()
        .map(|hit| hit.document_id.clone())
        .collect();
    assert_eq!(ids.len(), page.hits.len(), "duplicate lexical identities");
    assert_eq!(page.total_count, Some(ids.len()), "truncated search proof");
    Ok(ids)
}

#[test]
#[serial]
fn gh538_body_probe_distinguishes_inherited_title_from_message_content() -> Result<()> {
    let fixture = Fixture::new(ConnectorKind::Codex)?;
    let path = fixture.source("oracle", "contentprobeoriginalgh538")?;
    let handles = Handles::open(&fixture)?;
    assert_eq!(handles.scan(&fixture, true)?.0.published_conversations, 1);
    let original = search_ids(&fixture, "contentprobeoriginalgh538")?;
    assert_eq!(original.len(), 1);

    fixture.append(&path, "oracle", "contentprobeappendgh538")?;
    assert_eq!(handles.scan(&fixture, false)?.0.published_conversations, 1);
    let appended = search_ids(&fixture, "contentprobeappendgh538")?;
    assert_eq!(appended.len(), 1);
    assert!(original.is_disjoint(&appended));
    let both: BTreeSet<_> = original.union(&appended).cloned().collect();
    let rows = canonical_rows(&handles)?;
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.0 == rows[0].0));
    let parser = frankensearch::quill::query::CassQueryParser::new(
        frankensearch::quill::schema::CASS_SEMANTIC_SCHEMA,
    )?;
    let query = parser.parse(
        "contentprobeoriginalgh538",
        &frankensearch::quill::query::CassQueryFilters::default(),
    );
    assert_eq!(
        search_ids_for_query(&fixture, &query.query)?,
        both,
        "the old all-fields oracle also selects the appended row through its title"
    );
    assert_eq!(search_ids(&fixture, "contentprobeoriginalgh538")?, original);
    assert!(search_ids(&fixture, "contentprobenotpresentgh538")?.is_empty());

    // Content is indexed but not stored in CASS_SEMANTIC_SCHEMA. Bind the
    // hit's stored message index to its canonical SQLite body instead; the
    // independently stored title still carries the original term.
    let reader = crate::search::quill_bridge::open_cass_reader(&fixture.index_path)?;
    let page = crate::search::quill_bridge::search_paginated(&reader, &query.query, 8, 0, true)?;
    let hit = page
        .hits
        .iter()
        .find(|hit| appended.contains(&hit.document_id))
        .context("all-fields query must include the appended message")?;
    let fields = crate::search::quill_bridge::QuillCassFields::compiled();
    assert_eq!(
        crate::search::quill_bridge::stored_u64(&reader, fields.conversation_id, hit.global_docid)?,
        Some(u64::try_from(rows[0].0)?)
    );
    assert_eq!(
        crate::search::quill_bridge::stored_u64(&reader, fields.msg_idx, hit.global_docid)?,
        Some(1)
    );
    assert_eq!(
        rows.iter()
            .filter(|row| row.2 == 1)
            .map(|row| row.3.as_str())
            .collect::<Vec<_>>(),
        vec!["contentprobeappendgh538"]
    );
    assert!(
        crate::search::quill_bridge::stored_text(&reader, fields.title, hit.global_docid)?
            .is_some_and(|title| title.contains("contentprobeoriginalgh538"))
    );
    drop(reader);
    handles.close()?;
    Ok(())
}

#[test]
#[serial]
fn gh538_watch_checkpoint_requires_published_coverage_and_no_replay_debt() -> Result<()> {
    let fixture = Fixture::new(ConnectorKind::Codex)?;
    let source = fixture.source("checkpoint", "checkpointbaselinegh538")?;
    let handles = Handles::open(&fixture)?;
    let refresh = || -> Result<bool> {
        let storage = handles.storage.lock().unwrap();
        let index = handles.index.lock().unwrap();
        refresh_completed_watch_lexical_checkpoint(
            &storage,
            index.as_ref().context("live lexical writer")?,
            &fixture.options.db_path,
            &fixture.index_path,
        )
    };
    assert_eq!(handles.scan(&fixture, true)?.0.published_conversations, 1);
    assert!(refresh()?);
    let baseline =
        load_lexical_rebuild_checkpoint(&fixture.index_path)?.context("baseline checkpoint")?;
    assert_eq!(baseline.indexed_docs - baseline.indexed_region_docs, 1);
    // Ordinary indexing may do the independent full verification; the live
    // callback must use that observation without cold-scanning old bodies.
    assert_eq!(
        expected_live_lexical_doc_count_cached(
            &handles.storage.lock().unwrap(),
            &fixture.index_path,
            &fixture.options.db_path.to_string_lossy(),
            1,
            None,
        )?,
        1
    );

    // Deliberately omit replay debt as an independent negative control:
    // canonical coverage alone must still reject the old lexical writer.
    let conversation_id = canonical_rows(&handles)?[0].0;
    handles.storage.lock().unwrap().raw().execute_compat(
        "INSERT INTO messages(conversation_id, idx, role, content)
         VALUES(?1, 1, 'user', 'checkpointappendgh538')",
        &[ParamValue::from(conversation_id)],
    )?;
    assert!(!refresh()?);
    assert_eq!(
        load_lexical_rebuild_checkpoint(&fixture.index_path)?.context("preserved checkpoint")?,
        baseline
    );
    // Even a checkpoint claiming this canonical fingerprint and the old
    // writer's count cannot substitute for independent coverage validation.
    let mut claimed = load_lexical_rebuild_state(&fixture.index_path)?
        .context("checkpoint state to challenge")?;
    claimed.db =
        lexical_rebuild_db_state(&handles.storage.lock().unwrap(), &fixture.options.db_path)?;
    persist_lexical_rebuild_state(&fixture.index_path, &claimed)?;
    let unverified =
        load_lexical_rebuild_checkpoint(&fixture.index_path)?.context("unverified checkpoint")?;
    assert!(!refresh()?);
    assert_eq!(
        load_lexical_rebuild_checkpoint(&fixture.index_path)?.context("refused certification")?,
        unverified
    );
    // Real watch ingestion records replay debt in the same canonical
    // transaction. Restore that obligation before exercising its recovery.
    persist_watch_lexical_replay_debt(
        &handles.storage.lock().unwrap(),
        &WatchLexicalReplayDebt::for_root(fixture.kind, &fixture.root),
    )?;
    fixture.append(&source, "checkpoint", "checkpointappendgh538")?;
    assert_eq!(handles.scan(&fixture, false)?.0.published_conversations, 1);
    assert_eq!(search_ids(&fixture, "checkpointappendgh538")?.len(), 1);
    assert!(refresh()?);
    let advanced =
        load_lexical_rebuild_checkpoint(&fixture.index_path)?.context("advanced checkpoint")?;
    assert_eq!(advanced.indexed_docs - advanced.indexed_region_docs, 2);

    // Canonical hard noise is intentionally absent from the lexical sink.
    // Preserve the existing independent noise-adjusted coverage criterion.
    handles.storage.lock().unwrap().raw().execute_compat(
        "INSERT INTO messages(conversation_id, idx, role, content)
         VALUES(?1, 2, 'tool', '')",
        &[ParamValue::from(conversation_id)],
    )?;
    assert_eq!(canonical_rows(&handles)?.len(), 3);
    assert!(refresh()?);
    let published = load_lexical_rebuild_checkpoint(&fixture.index_path)?
        .context("noise-adjusted checkpoint")?;
    assert_eq!(published.indexed_docs - published.indexed_region_docs, 2);
    persist_watch_lexical_replay_debt(
        &handles.storage.lock().unwrap(),
        &WatchLexicalReplayDebt::for_root(fixture.kind, &fixture.root),
    )?;
    assert!(!refresh()?);
    assert_eq!(
        load_lexical_rebuild_checkpoint(&fixture.index_path)?.context("debt checkpoint")?,
        published
    );
    handles.close()?;
    Ok(())
}

#[test]
#[serial]
fn gh538_bounded_watch_count_cache_defers_cold_and_expired_verification() -> Result<()> {
    let fixture = Fixture::new(ConnectorKind::Codex)?;
    let source = fixture.source("bounded", "boundedbaselinegh538")?;
    let handles = Handles::open(&fixture)?;
    assert_eq!(handles.scan(&fixture, true)?.0.published_conversations, 1);
    let count = |allow_full_scan| -> Result<Option<usize>> {
        let storage = handles.storage.lock().unwrap();
        expected_live_lexical_doc_count_cached_with_scan_policy(
            &storage,
            &fixture.index_path,
            &fixture.options.db_path.to_string_lossy(),
            count_total_messages_exact(&storage)?,
            None,
            allow_full_scan,
        )
    };
    let cache_path = expected_lexical_docs_cache_path(&fixture.index_path)
        .context("expected document cache path")?;
    assert!(!cache_path.exists());
    assert_eq!(count(false)?, None);
    assert!(
        !cache_path.exists(),
        "bounded lookup must not create a full-scan observation"
    );
    assert_eq!(count(true)?, Some(1));
    assert_eq!(count(false)?, Some(1));

    let mut cached: ExpectedLexicalDocsCache = serde_json::from_slice(&fs::read(&cache_path)?)?;
    cached.delta_generations = EXPECTED_LEXICAL_DOCS_MAX_DELTA_GENERATIONS;
    fs::write(&cache_path, serde_json::to_vec(&cached)?)?;
    let capped = fs::read(&cache_path)?;
    fixture.append(&source, "bounded", "boundedappendgh538")?;
    assert_eq!(handles.scan(&fixture, false)?.0.published_conversations, 1);
    assert_eq!(canonical_rows(&handles)?.len(), 2);
    assert_eq!(search_ids(&fixture, "boundedappendgh538")?.len(), 1);
    assert_eq!(count(false)?, None);
    assert_eq!(
        fs::read(&cache_path)?,
        capped,
        "no hidden full-scan cache refresh"
    );
    assert_eq!(count(true)?, Some(2));
    let verified: ExpectedLexicalDocsCache = serde_json::from_slice(&fs::read(&cache_path)?)?;
    assert_eq!(verified.delta_generations, 0);
    assert_eq!(count(false)?, Some(2));
    handles.close()?;
    Ok(())
}

#[test]
#[serial]
fn gh538_failed_full_watch_census_preserves_durable_watermarks_until_publication() -> Result<()> {
    let fixture = Fixture::new(ConnectorKind::Codex)?;
    fixture.source("watermark-base", "watermarkbaselinegh538")?;
    let handles = Handles::open(&fixture)?;
    assert_eq!(handles.scan(&fixture, true)?.0.published_conversations, 1);
    let baseline_ids = search_ids(&fixture, "watermarkbaselinegh538")?;
    fixture.source("watermark-arriving", "watermarkarrivalgh538")?;
    // Seed older durable progress, then observe the actual trigger and
    // payload timestamps. Opening a directory just to set its mtime requires
    // platform-specific handle flags on Windows.
    let root_timestamp = i64::try_from(
        fs::metadata(&fixture.root.path)?
            .modified()?
            .duration_since(UNIX_EPOCH)?
            .as_millis(),
    )?;
    let payload_timestamp =
        chrono::DateTime::parse_from_rfc3339("2025-09-30T15:43:00.000Z")?.timestamp_millis();
    let next_watermark = root_timestamp.max(payload_timestamp);
    {
        let mut state = handles.state.lock().unwrap();
        state.insert(fixture.kind, next_watermark.saturating_sub(60_000));
        save_watch_state(&fixture.options.data_dir, &state)?;
    }
    let watermark_before = fs::read(state_path(&fixture.options.data_dir))?;
    assert_eq!(
        load_watch_state(&fixture.options.data_dir).get(&fixture.kind),
        Some(&next_watermark.saturating_sub(60_000))
    );
    let census = || {
        reindex_full_watch_roots(
            &fixture.options,
            &fixture.roots(),
            &handles.state,
            &handles.storage,
            &handles.index,
            &fixture.index_path,
            &ActiveSessionSourceFilter::with_recent_write_window_for_test(None),
        )
    };
    {
        let _fault = FaultGuard::set(WatchLexicalTestFault::Commit);
        assert!(census().is_err());
    }
    assert_eq!(
        fs::read(state_path(&fixture.options.data_dir))?,
        watermark_before
    );
    assert_eq!(
        search_ids(&fixture, "watermarkbaselinegh538")?,
        baseline_ids
    );
    assert!(search_ids(&fixture, "watermarkarrivalgh538")?.is_empty());
    assert!(!load_watch_lexical_replay_debt(&handles.storage.lock().unwrap())?.is_empty());
    assert!(census()?.published_conversations > 0);
    assert_eq!(search_ids(&fixture, "watermarkarrivalgh538")?.len(), 1);
    assert_eq!(
        search_ids(&fixture, "watermarkbaselinegh538")?,
        baseline_ids
    );
    assert_eq!(canonical_rows(&handles)?.len(), 2);
    assert!(load_watch_lexical_replay_debt(&handles.storage.lock().unwrap())?.is_empty());
    assert_eq!(
        load_watch_state(&fixture.options.data_dir).get(&fixture.kind),
        Some(&next_watermark)
    );
    handles.close()?;
    Ok(())
}

fn assert_publication_receipts(handles: &Handles, expected: usize) -> Result<()> {
    let entries = handles
        .storage
        .lock()
        .unwrap()
        .source_ingest_ledger_entries()?;
    assert_eq!(entries.len(), expected, "durable source receipt count");
    for observation in entries.values() {
        let observation: serde_json::Value = serde_json::from_str(observation)?;
        assert_eq!(observation["watch_published"], true, "{observation}");
    }
    Ok(())
}

fn published_state(fixture: &Fixture, handles: &Handles) -> Result<(Option<i64>, Vec<u8>)> {
    Ok((
        handles.storage.lock().unwrap().get_last_indexed_at()?,
        fs::read(fixture.index_path.join("MANIFEST"))?,
    ))
}

struct FaultGuard(Option<WatchLexicalTestFault>);

impl FaultGuard {
    fn set(fault: WatchLexicalTestFault) -> Self {
        Self(WATCH_LEXICAL_TEST_FAULT.with(|slot| slot.replace(Some(fault))))
    }
}

impl Drop for FaultGuard {
    fn drop(&mut self) {
        WATCH_LEXICAL_TEST_FAULT.with(|slot| slot.set(self.0));
    }
}

fn dispatch_source_retry(
    fixture: &Fixture,
    handles: &Handles,
    pending: &mut BTreeSet<PathBuf>,
) -> Result<WatchReindexOutcome> {
    let observed = std::cell::RefCell::new(None);
    assert!(dispatch_watch_callback(
        pending,
        &fixture.roots(),
        false,
        &|paths, roots, force_full| {
            assert!(!force_full);
            let filter = ActiveSessionSourceFilter::with_recent_write_window_for_test(None);
            let outcome = reindex_paths_with_semantic_delta(
                &fixture.options,
                paths,
                roots,
                &handles.state,
                &handles.storage,
                &handles.index,
                &fixture.index_path,
                force_full,
                None,
                &filter,
            )?;
            assert!(filter.take_deferred_sources().is_empty());
            let retries = outcome.retry_sources.clone();
            observed.replace(Some(outcome));
            Ok(retries)
        },
    ));
    observed.into_inner().context("watch callback must run")
}

fn prepare_absent_root_publication(
    kind: ConnectorKind,
) -> Result<(Fixture, Handles, BTreeSet<PathBuf>)> {
    let mut fixture = Fixture::new(kind)?;
    fixture.options.progress = Some(Arc::new(IndexingProgress::default()));
    let missing = fixture.source("missing", "absentrootbaselinegh538")?;
    fixture.source("sibling", "absentrootretainedgh538")?;
    let handles = Handles::open(&fixture)?;
    assert_eq!(handles.scan(&fixture, true)?.0.published_conversations, 2);
    fixture.append(&missing, "missing", "absentrootpublicationgh538")?;
    let mut pending = BTreeSet::from([missing.clone()]);
    {
        let _fault = FaultGuard::set(WatchLexicalTestFault::Commit);
        assert!(!dispatch_watch_callback(
            &mut pending,
            &fixture.roots(),
            false,
            &|paths, roots, force_full| {
                assert!(!force_full);
                let filter = ActiveSessionSourceFilter::with_recent_write_window_for_test(None);
                reindex_paths_with_semantic_delta(
                    &fixture.options,
                    paths,
                    roots,
                    &handles.state,
                    &handles.storage,
                    &handles.index,
                    &fixture.index_path,
                    force_full,
                    None,
                    &filter,
                )
                .map(|outcome| outcome.retry_sources)
            },
        ));
    }
    assert_eq!(pending, BTreeSet::from([missing]));
    assert_eq!(canonical_rows(&handles)?.len(), 3);
    assert!(search_ids(&fixture, "absentrootpublicationgh538")?.is_empty());
    assert!(!load_watch_lexical_replay_debt(&handles.storage.lock().unwrap())?.is_empty());
    fs::rename(
        &fixture.root.path,
        fixture._temp.path().join("preserved-source-root"),
    )?;
    Ok((fixture, handles, pending))
}

#[test]
#[serial]
fn gh538_watch_publishes_canonical_debt_after_whole_current_root_disappears() -> Result<()> {
    for kind in [ConnectorKind::Codex, ConnectorKind::Claude] {
        for restart in [false, true] {
            let (fixture, handles, mut pending) = prepare_absent_root_publication(kind)?;
            let old_ids = search_ids(&fixture, "absentrootbaselinegh538")?;
            let retained_ids = search_ids(&fixture, "absentrootretainedgh538")?;
            let rows = canonical_rows(&handles)?;
            let published = published_state(&fixture, &handles)?;
            let watch_state = load_watch_state(&fixture.options.data_dir);
            assert_publication_receipts(&handles, 1)?;
            let handles = if restart {
                handles.close()?;
                let handles = Handles::open(&fixture)?;
                // Only an explicitly retained current root authorizes replay
                // after restart. The old debt cannot supply configuration.
                pending = watch_lexical_replay_sources(
                    &handles.storage.lock().unwrap(),
                    &fixture.roots(),
                )?;
                assert_eq!(pending, BTreeSet::from([fixture.root.path.clone()]));
                handles
            } else {
                handles
            };
            let recovered = dispatch_source_retry(&fixture, &handles, &mut pending)?;
            assert_eq!(recovered.indexed, 2);
            assert_eq!(recovered.published_conversations, 2);
            assert!(pending.is_empty());
            assert_eq!(search_ids(&fixture, "absentrootpublicationgh538")?.len(), 1);
            assert_eq!(search_ids(&fixture, "absentrootbaselinegh538")?, old_ids);
            assert_eq!(
                search_ids(&fixture, "absentrootretainedgh538")?,
                retained_ids
            );
            assert_eq!(canonical_rows(&handles)?, rows);
            assert_eq!(published_state(&fixture, &handles)?.0, published.0);
            assert_eq!(load_watch_state(&fixture.options.data_dir), watch_state);
            assert_eq!(*handles.state.lock().unwrap(), watch_state);
            assert_eq!(
                fixture
                    .options
                    .progress
                    .as_ref()
                    .unwrap()
                    .phase
                    .load(Ordering::Relaxed),
                0,
            );
            assert_publication_receipts(&handles, 1)?;
            assert!(load_watch_lexical_replay_debt(&handles.storage.lock().unwrap())?.is_empty());

            let completed = published_state(&fixture, &handles)?;
            pending.insert(fixture.root.path.clone());
            assert_eq!(
                dispatch_source_retry(&fixture, &handles, &mut pending)?.published_conversations,
                0,
            );
            assert!(pending.is_empty());
            assert_eq!(published_state(&fixture, &handles)?, completed);
            assert_eq!(canonical_rows(&handles)?, rows);
            handles.close()?;
        }
    }
    Ok(())
}

#[test]
#[serial]
fn gh538_watch_absent_root_recovery_requires_commit_before_debt_clearance() -> Result<()> {
    let (fixture, handles, mut pending) = prepare_absent_root_publication(ConnectorKind::Codex)?;
    let retained = pending.clone();
    let rows = canonical_rows(&handles)?;
    let published = published_state(&fixture, &handles)?;
    let watch_state = load_watch_state(&fixture.options.data_dir);
    {
        let _fault = FaultGuard::set(WatchLexicalTestFault::Commit);
        assert!(!dispatch_watch_callback(
            &mut pending,
            &fixture.roots(),
            false,
            &|paths, roots, force_full| {
                let filter = ActiveSessionSourceFilter::with_recent_write_window_for_test(None);
                reindex_paths_with_semantic_delta(
                    &fixture.options,
                    paths,
                    roots,
                    &handles.state,
                    &handles.storage,
                    &handles.index,
                    &fixture.index_path,
                    force_full,
                    None,
                    &filter,
                )
                .map(|outcome| outcome.retry_sources)
            },
        ));
    }
    assert_eq!(pending, retained);
    let failed_state = published_state(&fixture, &handles)?;
    assert_eq!(
        failed_state.0, published.0,
        "failed final commit is not completion"
    );
    assert_ne!(
        failed_state.1, published.1,
        "bounded Quill upsert already published"
    );
    assert_eq!(canonical_rows(&handles)?, rows);
    assert_eq!(load_watch_state(&fixture.options.data_dir), watch_state);
    assert_eq!(search_ids(&fixture, "absentrootpublicationgh538")?.len(), 1);
    assert!(!load_watch_lexical_replay_debt(&handles.storage.lock().unwrap())?.is_empty());
    assert_eq!(
        fixture
            .options
            .progress
            .as_ref()
            .unwrap()
            .phase
            .load(Ordering::Relaxed),
        0,
    );
    assert_eq!(
        dispatch_source_retry(&fixture, &handles, &mut pending)?.published_conversations,
        2,
    );
    assert!(pending.is_empty());
    assert_eq!(canonical_rows(&handles)?, rows);
    assert_eq!(search_ids(&fixture, "absentrootpublicationgh538")?.len(), 1);
    assert!(load_watch_lexical_replay_debt(&handles.storage.lock().unwrap())?.is_empty());
    handles.close()?;
    Ok(())
}

#[test]
#[serial]
fn gh538_watch_absent_root_recovery_requires_current_authorization() -> Result<()> {
    let (fixture, handles, pending) = prepare_absent_root_publication(ConnectorKind::Codex)?;
    let rows = canonical_rows(&handles)?;
    let published = published_state(&fixture, &handles)?;
    let mut another_source = fixture.root.clone();
    another_source.origin.source_id = "removed-source-control".into();
    for roots in [
        Vec::new(),
        vec![(ConnectorKind::Claude, fixture.root.clone())],
        vec![(fixture.kind, another_source)],
    ] {
        assert!(watch_lexical_replay_sources(&handles.storage.lock().unwrap(), &roots)?.is_empty());
        let filter = ActiveSessionSourceFilter::with_recent_write_window_for_test(None);
        let outcome = reindex_paths_with_semantic_delta(
            &fixture.options,
            pending.iter().cloned().collect(),
            &roots,
            &handles.state,
            &handles.storage,
            &handles.index,
            &fixture.index_path,
            false,
            None,
            &filter,
        )?;
        assert_eq!(outcome.published_conversations, 0);
        assert!(outcome.retry_sources.is_empty());
        assert_eq!(canonical_rows(&handles)?, rows);
        assert_eq!(published_state(&fixture, &handles)?, published);
        assert!(!load_watch_lexical_replay_debt(&handles.storage.lock().unwrap())?.is_empty());
    }
    let mut once = fixture.options.clone();
    once.watch_once_paths = Some(pending.iter().cloned().collect());
    let filter = ActiveSessionSourceFilter::with_recent_write_window_for_test(None);
    assert_eq!(
        reindex_paths_with_semantic_delta(
            &once,
            pending.into_iter().collect(),
            &fixture.roots(),
            &handles.state,
            &handles.storage,
            &handles.index,
            &fixture.index_path,
            false,
            None,
            &filter,
        )?
        .published_conversations,
        0,
        "explicit watch-once keeps its absent-path no-op semantics",
    );
    assert_eq!(published_state(&fixture, &handles)?, published);
    assert!(search_ids(&fixture, "absentrootpublicationgh538")?.is_empty());
    handles.close()?;
    Ok(())
}

#[test]
#[serial]
fn gh538_watch_absent_root_routing_prefers_extant_owner_and_coalesces_duplicates() -> Result<()> {
    let (fixture, handles, pending) = prepare_absent_root_publication(ConnectorKind::Codex)?;
    let nested = ScanRoot::local(fixture.root.path.join("nested"));
    let mut paths: Vec<_> = pending.iter().cloned().collect();
    paths.extend([fixture.root.path.clone(), nested.path.clone()]);
    let selected = promote_missing_watch_replay_paths(
        &mut paths,
        &[
            (fixture.kind, fixture.root.clone()),
            (fixture.kind, nested),
            (fixture.kind, fixture.root.clone()),
        ],
        &handles.storage,
        None,
    )?;
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].1.path, fixture.root.path);

    let parent = ScanRoot::local(fixture.root.path.parent().unwrap().to_path_buf());
    let roots = [
        (fixture.kind, fixture.root.clone()),
        (fixture.kind, parent.clone()),
    ];
    let mut paths: Vec<_> = pending.iter().cloned().collect();
    assert!(
        promote_missing_watch_replay_paths(&mut paths, &roots, &handles.storage, None,)?.is_empty()
    );
    assert!(paths.contains(&parent.path));
    let filter = ActiveSessionSourceFilter::with_recent_write_window_for_test(None);
    let outcome = reindex_paths_with_semantic_delta(
        &fixture.options,
        pending.into_iter().collect(),
        &roots,
        &handles.state,
        &handles.storage,
        &handles.index,
        &fixture.index_path,
        false,
        None,
        &filter,
    )?;
    assert_eq!(
        outcome.published_conversations, 2,
        "one canonical replay per identity"
    );
    assert_eq!(canonical_rows(&handles)?.len(), 3);
    assert_eq!(search_ids(&fixture, "absentrootpublicationgh538")?.len(), 1);
    handles.close()?;
    Ok(())
}

#[test]
fn gh538_watch_absent_root_selection_rejects_nonabsence_errors() -> Result<()> {
    for kind in [
        std::io::ErrorKind::PermissionDenied,
        std::io::ErrorKind::Other,
    ] {
        let error = watch_replay_root_status(Err(std::io::Error::new(kind, "unavailable root")))
            .expect_err("an unavailable root cannot be certified absent");
        assert_eq!(error.kind(), kind);
    }
    assert_eq!(
        watch_replay_root_status(Err(std::io::Error::from(std::io::ErrorKind::NotADirectory)))?,
        WatchReplayRootStatus::Other,
    );
    assert_eq!(
        watch_replay_root_status(Err(std::io::Error::from(std::io::ErrorKind::NotFound)))?,
        WatchReplayRootStatus::Absent,
    );
    Ok(())
}

#[cfg(unix)]
#[test]
#[serial]
fn gh538_watch_absent_root_lookup_loop_cannot_authorize_recovery() -> Result<()> {
    let (fixture, handles, mut pending) = prepare_absent_root_publication(ConnectorKind::Codex)?;
    let published = published_state(&fixture, &handles)?;
    let rows = canonical_rows(&handles)?;
    std::os::unix::fs::symlink(&fixture.root.path, &fixture.root.path)?;
    let error = fs::metadata(&fixture.root.path).expect_err("fixture has a real symlink loop");
    assert_ne!(error.kind(), std::io::ErrorKind::NotFound);
    assert_ne!(error.kind(), std::io::ErrorKind::NotADirectory);
    assert_eq!(
        dispatch_source_retry(&fixture, &handles, &mut pending)?.published_conversations,
        0
    );
    assert_eq!(canonical_rows(&handles)?, rows);
    assert_eq!(published_state(&fixture, &handles)?, published);
    assert!(!load_watch_lexical_replay_debt(&handles.storage.lock().unwrap())?.is_empty());
    assert!(search_ids(&fixture, "absentrootpublicationgh538")?.is_empty());
    handles.close()?;
    Ok(())
}

#[test]
#[serial]
fn gh538_watch_absent_root_preserves_nomem_retry_and_discovery_debt() -> Result<()> {
    let (fixture, handles, mut pending) = prepare_absent_root_publication(ConnectorKind::Codex)?;
    let published = published_state(&fixture, &handles)?;
    let now = FrankenStorage::now_millis();
    let mut deferrals = WatchNomemDeferrals::default();
    deferrals.entries.insert(
        "held-source".into(),
        WatchNomemDeferral {
            agent: fixture.kind.slug().into(),
            source_path: fixture.path("missing").display().to_string(),
            external_id: Some("missing".into()),
            deferrals: 1,
            first_deferred_at_ms: now,
            last_deferred_at_ms: now,
            next_retry_at_ms: now.saturating_add(60_000),
            indexed_text_bytes: 1,
        },
    );
    deferrals.save(&fixture.options.data_dir);
    persist_watch_lexical_replay_debt(
        &handles.storage.lock().unwrap(),
        &WatchLexicalReplayDebt::for_discovery(fixture.kind, &fixture.root),
    )?;
    let held = dispatch_source_retry(&fixture, &handles, &mut pending)?;
    assert_eq!(held.published_conversations, 0);
    assert_eq!(pending, BTreeSet::from([fixture.root.path.clone()]));
    assert_eq!(published_state(&fixture, &handles)?, published);
    assert_eq!(
        load_watch_lexical_replay_debt(&handles.storage.lock().unwrap())?.len(),
        2
    );
    deferrals
        .entries
        .get_mut("held-source")
        .unwrap()
        .next_retry_at_ms = now.saturating_sub(1);
    deferrals.save(&fixture.options.data_dir);
    let recovered = dispatch_source_retry(&fixture, &handles, &mut pending)?;
    assert_eq!(recovered.published_conversations, 2);
    assert!(pending.is_empty());
    let remaining = load_watch_lexical_replay_debt(&handles.storage.lock().unwrap())?;
    assert_eq!(remaining.len(), 1);
    assert!(
        remaining[0].discovery_only,
        "canonical publication is not a source census"
    );
    assert_eq!(published_state(&fixture, &handles)?.0, published.0);
    assert_publication_receipts(&handles, 1)?;
    handles.close()?;
    Ok(())
}

#[test]
#[serial]
fn gh538_watch_absent_root_empty_canonical_identity_retains_debt() -> Result<()> {
    let (fixture, handles, mut pending) = prepare_absent_root_publication(ConnectorKind::Codex)?;
    let published = published_state(&fixture, &handles)?;
    let watch_state = load_watch_state(&fixture.options.data_dir);
    let old_ids = search_ids(&fixture, "absentrootbaselinegh538")?;
    let missing_id = canonical_rows(&handles)?
        .into_iter()
        .find(|(_, _, _, text)| text == "absentrootbaselinegh538")
        .context("canonical baseline identity")?
        .0;
    {
        let storage = handles.storage.lock().unwrap();
        persist::with_ephemeral_writer(
            &storage,
            false,
            "prepare empty absent root identity",
            |writer| {
                writer.raw().execute_compat(
                    "DELETE FROM messages WHERE conversation_id = ?1",
                    &[ParamValue::from(missing_id)],
                )?;
                Ok(())
            },
        )?;
    }
    let outcome = dispatch_source_retry(&fixture, &handles, &mut pending)?;
    assert_eq!(outcome.published_conversations, 1);
    assert!(
        pending.is_empty(),
        "an unrepairable empty identity cannot request a retry loop"
    );
    assert_eq!(search_ids(&fixture, "absentrootbaselinegh538")?, old_ids);
    assert!(search_ids(&fixture, "absentrootpublicationgh538")?.is_empty());
    assert_eq!(canonical_rows(&handles)?.len(), 1);
    assert_eq!(published_state(&fixture, &handles)?.0, published.0);
    assert_eq!(load_watch_state(&fixture.options.data_dir), watch_state);
    assert_eq!(*handles.state.lock().unwrap(), watch_state);
    assert!(!load_watch_lexical_replay_debt(&handles.storage.lock().unwrap())?.is_empty());
    assert_publication_receipts(&handles, 1)?;
    handles.close()?;
    Ok(())
}

#[test]
#[serial]
fn gh538_watch_absent_root_honors_exclusion_skip_and_defer_policies() -> Result<()> {
    const CHILD_DATA: &str = "CASS_TEST_GH538_ABSENT_ROOT_POLICY_DATA";
    const CHILD_ROOT: &str = "CASS_TEST_GH538_ABSENT_ROOT_POLICY_ROOT";
    const POLICIES: [&str; 4] = [
        "CASS_EXCLUDE_PATHS",
        "CASS_SKIP_SUBAGENTS",
        "CASS_EXCLUDE_WORKSPACES",
        "CASS_DEFER_LEXICAL_UPDATES",
    ];
    if let Some(data) = std::env::var_os(CHILD_DATA) {
        let data = PathBuf::from(data);
        let root = ScanRoot::local(PathBuf::from(
            std::env::var_os(CHILD_ROOT).context("policy root")?,
        ));
        let index_path = index_dir(&data)?;
        let opts = IndexOptions {
            full: false,
            force_rebuild: false,
            watch: true,
            watch_once_paths: None,
            db_path: data.join("agent_search.db"),
            data_dir: data.clone(),
            semantic: false,
            build_hnsw: false,
            embedder: "hash".into(),
            progress: None,
            watch_interval_secs: 1,
        };
        let handles = Handles {
            storage: Mutex::new(FrankenStorage::open(&opts.db_path)?),
            index: Mutex::new(Some(TantivyIndex::open_or_create(&index_path)?)),
            state: Mutex::new(load_watch_state(&data)),
        };
        let rows = canonical_rows(&handles)?;
        let manifest = fs::read(index_path.join("MANIFEST"))?;
        let timestamp = handles.storage.lock().unwrap().get_last_indexed_at()?;
        let state = load_watch_state(&data);
        let filter = ActiveSessionSourceFilter::with_recent_write_window_for_test(None);
        let outcome = reindex_paths_with_semantic_delta(
            &opts,
            vec![root.path.clone()],
            &[(ConnectorKind::Codex, root)],
            &handles.state,
            &handles.storage,
            &handles.index,
            &index_path,
            false,
            None,
            &filter,
        )?;
        assert_eq!(outcome.published_conversations, 0);
        assert!(
            outcome.retry_sources.is_empty(),
            "intentional policy cannot cause a retry loop"
        );
        assert_eq!(canonical_rows(&handles)?, rows);
        assert_eq!(fs::read(index_path.join("MANIFEST"))?, manifest);
        assert_eq!(
            handles.storage.lock().unwrap().get_last_indexed_at()?,
            timestamp
        );
        assert_eq!(load_watch_state(&data), state);
        assert!(!load_watch_lexical_replay_debt(&handles.storage.lock().unwrap())?.is_empty());
        assert_publication_receipts(&handles, 1)?;
        handles.close()?;
        return Ok(());
    }

    let (fixture, handles, _) = prepare_absent_root_publication(ConnectorKind::Codex)?;
    handles.close()?;
    // Each child opens the same isolated archive after the original writer
    // closes. Environment policy changes never race other application tests.
    for (key, value) in [
        (POLICIES[0], fixture.root.path.display().to_string()),
        (POLICIES[1], "1".into()),
        (POLICIES[2], "/receipt-workspace".into()),
        (POLICIES[3], "1".into()),
    ] {
        let mut command = std::process::Command::new(std::env::current_exe()?);
        command.args([
            "--exact",
            "indexer::watch_ingestion_tests::gh538_watch_absent_root_honors_exclusion_skip_and_defer_policies",
            "--nocapture",
        ]);
        for policy in POLICIES {
            command.env_remove(policy);
        }
        let output = command
            .env(CHILD_DATA, &fixture.options.data_dir)
            .env(CHILD_ROOT, &fixture.root.path)
            .env(key, value)
            .output()?;
        anyhow::ensure!(
            output.status.success() && String::from_utf8_lossy(&output.stdout).contains("1 passed"),
            "absent-root policy {key} failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    let handles = Handles::open(&fixture)?;
    let mut pending =
        watch_lexical_replay_sources(&handles.storage.lock().unwrap(), &fixture.roots())?;
    assert_eq!(
        dispatch_source_retry(&fixture, &handles, &mut pending)?.published_conversations,
        2
    );
    assert!(pending.is_empty());
    assert_eq!(search_ids(&fixture, "absentrootpublicationgh538")?.len(), 1);
    handles.close()?;
    Ok(())
}
