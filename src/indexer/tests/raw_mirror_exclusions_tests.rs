use crate::dependency_pin_correlation::build_metadata::current_for_unit_test;
use super::super::*;
use super::helpers::*;
use crate::connectors::{DetectionResult, NormalizedConversation, NormalizedMessage, ScanContext};
use serial_test::serial;
use tempfile::TempDir;

#[test]
fn closed_world_connector_roots_skip_default_detection() {
    let closed_world = Some(Arc::new(HashMap::new()));
    let result = detect_for_local_scan(&closed_world, || {
        panic!("closed-world indexing must not run default detection")
    });
    assert!(result.is_none());

    let result = detect_for_local_scan(&None, DetectionResult::not_found);
    assert!(result.is_some_and(|detection| !detection.detected));
}

/// FAD 0.3.11 added `ScanContext::allow_local_default_stores` (upstream
/// issue #174): connectors may union their ambient local default stores
/// only while it is set. cass's production scans must keep reading the
/// same local default stores as before the FAD pin move, so every
/// constructor the src/indexer scan sites use yields `true`, the
/// with_scan_tick progress chain (src/indexer/mod.rs: `with_progress_tick`)
/// preserves it, and only the explicit hermeticity opt-out flips it.
#[test]
fn scan_context_production_constructors_keep_local_default_stores() {
    let with_roots_ctx = ScanContext::with_roots(
        std::path::PathBuf::from("cass-data"),
        vec![ScanRoot::local(std::path::PathBuf::from("selected-root"))],
        None,
    );
    assert!(
        with_roots_ctx.allow_local_default_stores,
        "production with_roots scans must keep reading local default stores"
    );

    let local_default_ctx = ScanContext::local_default(std::path::PathBuf::from("cass-data"), None);
    assert!(
        local_default_ctx.allow_local_default_stores,
        "production local_default scans must keep reading local default stores"
    );

    // with_scan_tick's Some(tick) arm must not silently flip the stance.
    let tick: Arc<dyn Fn() + Send + Sync> = Arc::new(|| {});
    let ticked = with_roots_ctx.with_progress_tick(Arc::clone(&tick));
    assert!(
        ticked.allow_local_default_stores,
        "attaching a progress tick must not flip the local-default stance"
    );

    // The hermeticity opt-out is the only sanctioned way to disable
    // local default store reads (test harnesses; never production scans).
    let hermetic = local_default_ctx.without_local_default_stores();
    assert!(!hermetic.allow_local_default_stores);
}

/// #366: waiting↔waiting park flaps are scheduler churn a livelocked
/// pipeline can sustain forever; they must not reset the stall clock.
/// Every transition through a working site is genuine progress.
#[test]
fn producer_state_waiting_flaps_are_not_work() {
    assert!(!producer_state_transition_is_work(
        "waiting_turn",
        "waiting_budget"
    ));
    assert!(!producer_state_transition_is_work(
        "waiting_budget",
        "waiting_result"
    ));
    assert!(!producer_state_transition_is_work("idle", "idle"));
    assert!(!producer_state_transition_is_work(
        "waiting_turn",
        "waiting_turn"
    ));
    assert!(producer_state_transition_is_work("idle", "listing_db"));
    assert!(producer_state_transition_is_work(
        "listing_db",
        "waiting_budget"
    ));
    assert!(producer_state_transition_is_work(
        "waiting_turn",
        "handoff_send"
    ));
    assert!(producer_state_transition_is_work("handoff_send", "idle"));
    assert!(producer_state_transition_is_work("waiting_result", "idle"));
}

#[test]
fn scan_path_exclusions_value_active_handles_commas_and_newlines() {
    assert!(!scan_path_exclusions_value_active(None));
    assert!(!scan_path_exclusions_value_active(Some("   \n , ")));
    assert!(scan_path_exclusions_value_active(Some(
        " /tmp/active.jsonl ,\n/var/log/cass "
    )));
}

fn gh537_isolated_workspace_policy_test(name: &str) -> Result<bool> {
    const CHILD: &str = "CASS_TEST_GH537_WORKSPACE_POLICY_CHILD";
    if dotenvy::var(CHILD).is_ok_and(|value| value == name) {
        return Ok(true);
    }
    // Other indexer tests run concurrently and read this policy. Keep
    // mutations in a fresh process executing only the requested test.
    let output = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            &format!("{}::{name}", module_path!().split_once("::").unwrap().1),
            "--nocapture",
        ])
        .env(CHILD, name)
        .env_remove("CASS_EXCLUDE_WORKSPACES")
        .env_remove("CASS_EXCLUDE_PATHS")
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "workspace policy test {name} failed: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    anyhow::ensure!(
        String::from_utf8_lossy(&output.stdout).contains("1 passed"),
        "workspace policy child did not execute its test"
    );
    Ok(false)
}

#[test]
fn gh537_workspace_exclusions_validate_and_match_known_workspace_globs() -> Result<()> {
    let policy = WorkspaceExclusions::parse(" /work/retired/*,\n/work/scratch-*,, ")?;
    assert!(policy.excludes(Some(Path::new("/work/retired/project"))));
    assert!(policy.excludes(Some(Path::new("/work/scratch-build"))));
    assert!(!policy.excludes(Some(Path::new("/work/active"))));
    assert!(!policy.excludes(None));
    assert!(!WorkspaceExclusions::parse(" ,\n ")?.excludes(Some(Path::new("/work/active"))));
    let error = WorkspaceExclusions::parse("/work/ok,/work/[broken")
        .expect_err("invalid workspace glob must not be ignored");
    assert!(error.to_string().contains("CASS_EXCLUDE_WORKSPACES"));
    assert!(error.to_string().contains("/work/[broken"));

    let reordered = WorkspaceExclusions::parse("/work/scratch-*, /work/retired/*,/work/scratch-*")?;
    assert_eq!(
        policy.bind_contract("producer"),
        reordered.bind_contract("producer")
    );
    assert_ne!(policy.bind_contract("producer"), "producer");
    assert_ne!(
        policy.bind_contract("producer"),
        WorkspaceExclusions::parse("/work/retired/*")?.bind_contract("producer")
    );
    assert_eq!(
        WorkspaceExclusions::default().bind_contract("producer"),
        "producer"
    );
    Ok(())
}

#[test]
#[serial]
fn gh537_workspace_exclusions_advance_watermarks_and_revisit_policy_changes() -> Result<()> {
    if !gh537_isolated_workspace_policy_test(
        "gh537_workspace_exclusions_advance_watermarks_and_revisit_policy_changes",
    )? {
        return Ok(());
    }
    let _paths = unset_env_var("CASS_EXCLUDE_PATHS");
    let tmp = TempDir::new()?;
    let storage = FrankenStorage::open(&tmp.path().join("workspace-policy.db"))?;
    let factories = get_connector_factories()
        .into_iter()
        .filter(|(name, _)| *name == "claude")
        .collect::<Vec<_>>();
    assert_eq!(factories.len(), 1);
    let _policy = set_env("CASS_EXCLUDE_WORKSPACES", "/work/retired/*");
    storage.set_connector_last_scan_ts("claude", 1000)?;
    let initial = connector_local_scan_since_ts_map(&storage, Some(999), &factories)?;
    assert_eq!(
        initial["claude"], None,
        "a changed policy revisits historical sources"
    );
    record_connector_scan_watermark(&storage, "claude", 2000)?;
    assert_eq!(storage.get_connector_last_scan_ts("claude")?, Some(2000));
    let unchanged = connector_local_scan_since_ts_map(&storage, Some(1999), &factories)?;
    assert_eq!(
        unchanged["claude"],
        Some(1999),
        "exclusions do not hold the watermark"
    );
    {
        let _removed = set_env("CASS_EXCLUDE_WORKSPACES", "");
        let removed = connector_local_scan_since_ts_map(&storage, Some(1999), &factories)?;
        assert_eq!(
            removed["claude"], None,
            "removed exclusions admit old sessions"
        );
        record_connector_scan_watermark(&storage, "claude", 3000)?;
        let resumed = connector_local_scan_since_ts_map(&storage, Some(2999), &factories)?;
        assert_eq!(resumed["claude"], Some(2999));
    }
    Ok(())
}

#[test]
#[serial]
fn gh537_workspace_excluded_source_keeps_reusable_completion_without_rows() -> Result<()> {
    if !gh537_isolated_workspace_policy_test(
        "gh537_workspace_excluded_source_keeps_reusable_completion_without_rows",
    )? {
        return Ok(());
    }
    let _policy = set_env("CASS_EXCLUDE_WORKSPACES", "/workspace/demo");
    let tmp = TempDir::new()?;
    let storage = FrankenStorage::open(&tmp.path().join("archive.db"))?;
    let source_path = tmp.path().join("session.jsonl");
    fs::write(&source_path, b"completed transcript")?;
    let mut conv = norm_conv(Some("excluded"), vec![norm_msg(0, 100)]);
    conv.agent_slug = "codex".into();
    conv.source_path = source_path.clone();
    let source = DiscoveredSourceFile::new(
        "codex",
        &ScanRoot::local(tmp.path().to_path_buf()),
        source_path.clone(),
        crate::connectors::DiscoveredSourceRole::PrimarySessionLog,
        true,
    )
    .with_fs_metadata();
    let contract = WorkspaceExclusions::from_env()?.bind_contract(
        current_for_unit_test().ingest_contract,
    );
    let completion = crate::storage::sqlite::SourceIngestLedgerEntry {
        key: "source_ingest_v1:workspace-excluded".into(),
        observation: serde_json::json!({
            "primary": source_file_observation(&source_path),
            "dependencies": [],
            "producer_contract": contract,
        })
        .to_string(),
    };
    let outcome = persist::persist_conversations_batched_inner(
        &storage,
        None,
        std::slice::from_ref(&conv),
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        false,
        false,
        None,
        persist::PersistHeartbeat::NONE,
        Some(&completion),
    )?;
    assert_eq!(outcome.inserted_conversations, 0);
    assert_eq!(count_total_conversations_exact(&storage)?, 0);
    let ledger = storage.source_ingest_ledger_entries()?;
    let saved = ledger
        .get(&completion.key)
        .context("excluded source completion")?;
    assert_eq!(saved, &completion.observation);
    assert!(source_ledger_matches_with_contract(
        saved, &source, &contract
    ));
    assert!(!source_ledger_matches_with_contract(
        saved,
        &source,
        current_for_unit_test().ingest_contract
    ));
    {
        let _removed = set_env("CASS_EXCLUDE_WORKSPACES", "");
        let admitted = persist::persist_conversations_batched(
            &storage,
            None,
            std::slice::from_ref(&conv),
            LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
            false,
        )?;
        assert_eq!(admitted.inserted_conversations, 1);
        assert_eq!(count_total_messages_exact(&storage)?, 1);
    }
    Ok(())
}

#[test]
#[serial]
fn gh537_shared_store_tombstones_filter_sessions_with_exact_provenance() -> Result<()> {
    if !gh537_isolated_workspace_policy_test(
        "gh537_shared_store_tombstones_filter_sessions_with_exact_provenance",
    )? {
        return Ok(());
    }
    let _policy = set_env("CASS_EXCLUDE_WORKSPACES", "");
    let tmp = TempDir::new()?;
    let storage = FrankenStorage::open(&tmp.path().join("archive.db"))?;
    let source_path = tmp.path().join("sessions.db");
    fs::write(&source_path, b"shared database before sibling update")?;
    let mut forgotten = norm_conv(Some("forgotten"), vec![norm_msg(0, 100)]);
    forgotten.agent_slug = "opencode".into();
    forgotten.workspace = Some(PathBuf::from("/workspace/retired"));
    forgotten.source_path = source_path.clone();
    let mut sibling = forgotten.clone();
    sibling.external_id = Some("kept-sibling".into());
    sibling.workspace = Some(PathBuf::from("/workspace/active"));
    let persist = |convs: &[NormalizedConversation]| {
        persist::persist_conversations_batched(
            &storage,
            None,
            convs,
            LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
            false,
        )
    };
    persist(&[forgotten.clone(), sibling.clone()])?;
    let report = storage.forget_conversations(
        &crate::storage::sqlite::ForgetConversationsFilter {
            workspace_glob: Some("/workspace/retired"),
            ..Default::default()
        },
        false,
    )?;
    assert_eq!(report.conversations_deleted, 1);
    assert!(storage.forgotten_source_stamps()?.is_empty());
    fs::write(
        &source_path,
        b"shared database after sibling append and checkpoint",
    )?;
    sibling.messages.push(norm_msg(1, 200));
    sibling.ended_at = Some(200);
    let outcome = persist(&[forgotten.clone(), sibling.clone()])?;
    assert_eq!(outcome.inserted_conversations, 0);
    assert_eq!(outcome.inserted_messages, 1);
    assert_eq!(count_total_conversations_exact(&storage)?, 1);
    assert_eq!(count_total_messages_exact(&storage)?, 2);

    // Session identities remain forgotten even if that session's own
    // messages change. Both public persistence paths honor the policy.
    forgotten.messages.push(norm_msg(1, 300));
    forgotten.ended_at = Some(300);
    let mut index = TantivyIndex::open_or_create(&tmp.path().join("direct-index"))?;
    persist::persist_conversation(&storage, &mut index, &forgotten)?;
    let replay = persist(std::slice::from_ref(&forgotten))?;
    assert_eq!(replay.inserted_conversations, 0);
    assert_eq!(replay.inserted_messages, 0);
    assert_eq!(count_total_conversations_exact(&storage)?, 1);
    assert_eq!(count_total_messages_exact(&storage)?, 2);

    let mut remote = forgotten.clone();
    inject_provenance(&mut remote, &Origin::remote("another-host"));
    let mut other_agent = forgotten;
    other_agent.agent_slug = "devin".into();
    let other_identities = persist(&[remote, other_agent])?;
    assert_eq!(other_identities.inserted_conversations, 2);
    assert_eq!(count_total_conversations_exact(&storage)?, 3);
    assert_eq!(storage.forgotten_conversation_keys()?.len(), 1);
    Ok(())
}

#[test]
#[serial]
fn gh537_ingest_tombstone_reads_are_batch_scoped() -> Result<()> {
    if !gh537_isolated_workspace_policy_test("gh537_ingest_tombstone_reads_are_batch_scoped")? {
        return Ok(());
    }
    let tmp = TempDir::new()?;
    let storage = FrankenStorage::open(&tmp.path().join("archive.db"))?;
    // A whole-table decode cannot pass these unrelated malformed records.
    // The real ingest entry points must query only the requested identities.
    storage.raw().execute_batch(
        "INSERT INTO forgotten_sources
            (source_path, size_bytes, mtime_ms, forgotten_at_ms)
         VALUES ('/unrelated/history.jsonl', X'FF', 1, 1);
         INSERT INTO forgotten_conversations
            (source_id, agent_slug, source_path, external_id, forgotten_at_ms)
         VALUES (X'FF', 'opencode', '/unrelated/store.db', 'unrelated', 1);",
    )?;
    assert!(storage.forgotten_source_stamps().is_err());
    assert!(storage.forgotten_conversation_keys().is_err());
    let mut conversation = norm_conv(Some("kept-batch"), vec![norm_msg(0, 100)]);
    conversation.source_path = tmp.path().join("kept.jsonl");
    let outcome = persist::persist_conversations_batched(
        &storage,
        None,
        std::slice::from_ref(&conversation),
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        false,
    )?;
    assert_eq!(outcome.inserted_conversations, 1);
    assert_eq!(outcome.inserted_messages, 1);
    conversation.external_id = Some("kept-single".into());
    let mut index = TantivyIndex::open_or_create(&tmp.path().join("direct-index"))?;
    persist::persist_conversation(&storage, &mut index, &conversation)?;
    assert_eq!(count_total_conversations_exact(&storage)?, 2);
    assert_eq!(count_total_messages_exact(&storage)?, 2);
    Ok(())
}

#[test]
#[serial]
fn gh537_workspace_exclusions_skip_per_conversation_raw_capture() -> Result<()> {
    if !gh537_isolated_workspace_policy_test(
        "gh537_workspace_exclusions_skip_per_conversation_raw_capture",
    )? {
        return Ok(());
    }
    let _policy = set_env("CASS_EXCLUDE_WORKSPACES", "/workspace/demo");
    let _capture = set_env("CASS_RAW_MIRROR", "1");
    let _free_space = set_env("CASS_RAW_MIRROR_MIN_FREE_BYTES", "0");
    let _budget = set_env("CASS_RAW_MIRROR_MAX_BYTES", "67108864");
    let tmp = TempDir::new()?;
    let data_dir = tmp.path().join("data");
    let source_path = tmp.path().join("session.jsonl");
    let source_bytes = b"{\"role\":\"user\",\"content\":\"keep source intact\"}\n";
    fs::write(&source_path, source_bytes)?;
    let root = ScanRoot::local(source_path.clone());
    let ctx = ScanContext::with_roots(data_dir.clone(), vec![root.clone()], None);
    let (_diagnostics, active_skipped) = capture_connector_sources_before_parse(
        &FailingDiscoveryConnector {
            sources: Vec::new(),
        },
        &ctx,
        &data_dir,
        "codex",
        &[root],
        None,
        &ActiveSessionSourceFilter::default(),
        &RawSourceExclusions::default(),
    );
    assert!(
        !active_skipped,
        "workspace deferral must not hold the scan watermark"
    );
    assert!(
        !data_dir.join("raw-mirror").exists(),
        "empty discovery must not bypass workspace policy"
    );
    let mut conv = norm_conv(Some("excluded-mirror"), vec![norm_msg(0, 100)]);
    conv.source_path = source_path.clone();
    prepare_conversation_for_ingest(
        &data_dir,
        "codex",
        &Origin::local(),
        None,
        &mut conv,
        &RawSourceExclusions::default(),
    );
    assert!(conv.metadata.pointer("/cass/raw_mirror").is_none());
    assert!(!data_dir.join("raw-mirror").exists());
    assert_eq!(fs::read(&source_path)?, source_bytes);
    conv.workspace = Some(PathBuf::from("/workspace/admitted"));
    prepare_conversation_for_ingest(
        &data_dir,
        "codex",
        &Origin::local(),
        None,
        &mut conv,
        &RawSourceExclusions::default(),
    );
    assert!(
        conv.metadata.pointer("/cass/raw_mirror").is_some(),
        "an admitted fallback transcript is captured after workspace discovery"
    );
    assert_eq!(fs::read(&source_path)?, source_bytes);
    Ok(())
}

#[test]
#[serial]
fn gh537_shared_raw_exclusions_preserve_sibling_ingest_and_completion() -> Result<()> {
    if !gh537_isolated_workspace_policy_test(
        "gh537_shared_raw_exclusions_preserve_sibling_ingest_and_completion",
    )? {
        return Ok(());
    }
    let _capture = set_env("CASS_RAW_MIRROR", "1");
    let _free_space = set_env("CASS_RAW_MIRROR_MIN_FREE_BYTES", "0");
    let _budget = set_env("CASS_RAW_MIRROR_MAX_BYTES", "67108864");
    let _policy = set_env("CASS_EXCLUDE_WORKSPACES", "");
    let temp = TempDir::new()?;
    let data_dir = temp.path().join("mirror-data");
    // The archive is intentionally not data_dir/agent_search.db: the
    // snapshot must use the storage selected by the indexing operation.
    let storage = FrankenStorage::open(&temp.path().join("custom-canonical.db"))?;
    let source_path = temp.path().join("shared.db");
    let wal = temp.path().join("shared.db-wal");
    fs::write(&source_path, b"shared source with a forgotten session")?;
    fs::write(&wal, b"shared source pending changes")?;
    let control_data_dir = temp.path().join("capture-control");
    capture_scan_root_file_before_parse(
        &control_data_dir,
        "opencode",
        &ScanRoot::local(source_path.clone()),
        &ActiveSessionSourceFilter::default(),
        &RawSourceExclusions::default(),
    );
    assert!(
        !raw_mirror_manifest_values(&control_data_dir).is_empty(),
        "the fixture must be capturable without a permanent exclusion"
    );
    let mut forgotten = norm_conv(Some("forgotten-raw"), vec![norm_msg(0, 100)]);
    forgotten.agent_slug = "opencode".into();
    forgotten.workspace = Some(PathBuf::from("/workspace/retired"));
    forgotten.source_path = source_path.clone();
    let mut sibling = forgotten.clone();
    sibling.external_id = Some("retained-raw".into());
    sibling.workspace = Some(PathBuf::from("/workspace/active"));
    persist::persist_conversations_batched(
        &storage,
        None,
        &[forgotten.clone(), sibling.clone()],
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        false,
    )?;
    storage.forget_conversations(
        &crate::storage::sqlite::ForgetConversationsFilter {
            workspace_glob: Some("/workspace/retired"),
            ..Default::default()
        },
        false,
    )?;
    let exclusions = RawSourceExclusions::load(&storage)?;
    assert!(exclusions.excludes(&source_path));
    assert!(storage.forgotten_source_stamps()?.is_empty());
    let root = ScanRoot::local(temp.path().to_path_buf());
    let source = discovered_test_source(
        &root,
        source_path.clone(),
        crate::connectors::DiscoveredSourceRole::SqliteDatabase,
    );
    let connector = FailingDiscoveryConnector {
        sources: vec![
            source,
            discovered_test_source(
                &root,
                wal.clone(),
                crate::connectors::DiscoveredSourceRole::MetadataSidecar,
            ),
        ],
    };
    let ctx = ScanContext::with_roots(data_dir.clone(), vec![root], None);
    let (diagnostics, active_skipped) = capture_connector_sources_before_parse(
        &connector,
        &ctx,
        &data_dir,
        "opencode",
        &[],
        None,
        &ActiveSessionSourceFilter::default(),
        &exclusions,
    );
    assert!(
        !active_skipped,
        "raw suppression must not defer source progress"
    );
    assert_eq!(diagnostics.finish().summary.discovered, 2);
    let fallback = ScanRoot::local(source_path.clone());
    assert!(!capture_scan_sources_before_parse(
        &data_dir,
        "opencode",
        &fallback,
        None,
        &ActiveSessionSourceFilter::default(),
        &exclusions,
    ));
    fs::write(
        &source_path,
        b"shared source after the retained sibling changes",
    )?;
    sibling.messages.push(norm_msg(1, 200));
    sibling.ended_at = Some(200);
    prepare_conversation_for_ingest(
        &data_dir,
        "opencode",
        &Origin::local(),
        None,
        &mut sibling,
        &exclusions,
    );
    assert!(sibling.metadata.pointer("/cass/raw_mirror").is_none());
    assert!(!data_dir.join("raw-mirror").exists());
    let completion = crate::storage::sqlite::SourceIngestLedgerEntry {
        key: "source_ingest_v1:shared-raw-excluded".into(),
        observation: serde_json::json!({
            "primary": source_file_observation(&source_path),
            "dependencies": [],
            "producer_contract": current_for_unit_test().ingest_contract,
        })
        .to_string(),
    };
    let outcome = persist::persist_conversations_batched_inner(
        &storage,
        None,
        &[forgotten, sibling],
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        false,
        false,
        None,
        persist::PersistHeartbeat::NONE,
        Some(&completion),
    )?;
    assert_eq!(outcome.inserted_messages, 1);
    assert_eq!(count_total_conversations_exact(&storage)?, 1);
    assert_eq!(count_total_messages_exact(&storage)?, 2);
    assert_eq!(
        storage.source_ingest_ledger_entries()?.get(&completion.key),
        Some(&completion.observation)
    );
    assert_eq!(
        fs::read(source_path)?,
        b"shared source after the retained sibling changes"
    );
    assert_eq!(fs::read(wal)?, b"shared source pending changes");
    Ok(())
}

#[test]
#[serial]
fn gh537_shared_raw_policy_preserves_changed_single_file_reingestion() -> Result<()> {
    if !gh537_isolated_workspace_policy_test(
        "gh537_shared_raw_policy_preserves_changed_single_file_reingestion",
    )? {
        return Ok(());
    }
    let _capture = set_env("CASS_RAW_MIRROR", "1");
    let _free_space = set_env("CASS_RAW_MIRROR_MIN_FREE_BYTES", "0");
    let _budget = set_env("CASS_RAW_MIRROR_MAX_BYTES", "67108864");
    let temp = TempDir::new()?;
    let data_dir = temp.path().join("data");
    let storage = FrankenStorage::open(&temp.path().join("archive.db"))?;
    let source_path = temp.path().join("ordinary.jsonl");
    fs::write(&source_path, b"original ordinary transcript")?;
    let mut conv = norm_conv(Some("ordinary-raw"), vec![norm_msg(0, 100)]);
    conv.source_path = source_path.clone();
    persist::persist_conversations_batched(
        &storage,
        None,
        std::slice::from_ref(&conv),
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        false,
    )?;
    storage.forget_conversations(
        &crate::storage::sqlite::ForgetConversationsFilter {
            source_glob: Some(source_path.to_str().context("source path")?),
            ..Default::default()
        },
        false,
    )?;
    assert_eq!(storage.forgotten_source_stamps()?.len(), 1);
    assert!(storage.forgotten_conversation_keys()?.is_empty());
    let exclusions = RawSourceExclusions::load(&storage)?;
    assert!(!exclusions.excludes(&source_path));
    fs::write(
        &source_path,
        b"ordinary transcript explicitly changed after forget",
    )?;
    conv.messages.push(norm_msg(1, 200));
    conv.ended_at = Some(200);
    prepare_conversation_for_ingest(
        &data_dir,
        "codex",
        &Origin::local(),
        None,
        &mut conv,
        &exclusions,
    );
    assert!(conv.metadata.pointer("/cass/raw_mirror").is_some());
    let outcome = persist::persist_conversations_batched(
        &storage,
        None,
        std::slice::from_ref(&conv),
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        false,
    )?;
    assert_eq!(outcome.inserted_conversations, 1);
    assert_eq!(count_total_messages_exact(&storage)?, 2);
    assert!(storage.forgotten_source_stamps()?.is_empty());
    assert_eq!(
        fs::read(source_path)?,
        b"ordinary transcript explicitly changed after forget"
    );
    Ok(())
}

// review 2026-11-02 I1: a forgotten source whose file becomes unreadable
// between parse and persist must stay forgotten.
#[test]
#[serial]
fn review_i1_unreadable_forgotten_source_keeps_its_tombstone() -> Result<()> {
    if !gh537_isolated_workspace_policy_test(
        "review_i1_unreadable_forgotten_source_keeps_its_tombstone",
    )? {
        return Ok(());
    }
    let temp = TempDir::new()?;
    let storage = FrankenStorage::open(&temp.path().join("archive.db"))?;
    let source_path = temp.path().join("ordinary.jsonl");
    fs::write(&source_path, b"original ordinary transcript")?;
    let mut conv = norm_conv(Some("review-i1"), vec![norm_msg(0, 100)]);
    conv.source_path = source_path.clone();
    persist::persist_conversations_batched(
        &storage,
        None,
        std::slice::from_ref(&conv),
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        false,
    )?;
    storage.forget_conversations(
        &crate::storage::sqlite::ForgetConversationsFilter {
            source_glob: Some(source_path.to_str().context("source path")?),
            ..Default::default()
        },
        false,
    )?;
    assert_eq!(storage.forgotten_source_stamps()?.len(), 1);
    assert_eq!(count_total_messages_exact(&storage)?, 0);

    // The conversation was already parsed into memory; now the file
    // disappears (moved, deleted, permission change) before persistence.
    fs::remove_file(&source_path)?;
    conv.messages.push(norm_msg(1, 200));
    persist::persist_conversations_batched(
        &storage,
        None,
        std::slice::from_ref(&conv),
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        false,
    )?;

    assert_eq!(
        count_total_messages_exact(&storage)?,
        0,
        "a forgotten conversation must not come back because its file is unreadable"
    );
    assert_eq!(storage.forgotten_source_stamps()?.len(), 1);
    Ok(())
}

#[test]
fn raw_mirror_capture_attaches_metadata_without_claiming_a_persisted_db_link() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("rollout-test.jsonl");
    let source_bytes = b"{\"type\":\"message\",\"role\":\"user\",\"content\":\"hello\"}\n";
    std::fs::write(&source_path, source_bytes).expect("write source");

    let mut conv = NormalizedConversation {
        agent_slug: "codex".to_string(),
        external_id: Some("raw-mirror-metadata".to_string()),
        title: Some("Raw mirror metadata".to_string()),
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

    let raw_mirror = &conv.metadata["cass"]["raw_mirror"];
    let manifest_id = raw_mirror["manifest_id"]
        .as_str()
        .expect("manifest id metadata");
    assert!(manifest_id.starts_with("doctor-raw-mirror-manifest-id-v1-"));
    assert_eq!(
        raw_mirror["blob_size_bytes"].as_u64(),
        Some(source_bytes.len() as u64)
    );
    let manifest_relative = raw_mirror["manifest_relative_path"]
        .as_str()
        .expect("manifest relative path");
    let blob_relative = raw_mirror["blob_relative_path"]
        .as_str()
        .expect("blob relative path");
    assert!(
        data_dir
            .join("raw-mirror/v1")
            .join(manifest_relative)
            .exists()
    );
    assert_eq!(
        std::fs::read(data_dir.join("raw-mirror/v1").join(blob_relative)).expect("raw mirror blob"),
        source_bytes
    );
    let manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(data_dir.join("raw-mirror/v1").join(manifest_relative))
            .expect("raw mirror manifest"),
    )
    .expect("manifest json");
    assert_eq!(
        manifest["db_links"].as_array().map(Vec::len),
        Some(0),
        "pre-persist capture must not manufacture a database link without a canonical conversation id"
    );
    assert_eq!(
        std::fs::read(&source_path).expect("source bytes"),
        source_bytes
    );
}

#[test]
fn raw_mirror_capture_handles_explicit_file_root_before_parse() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let source_path = temp.path().join("parse-failure-candidate.jsonl");
    std::fs::write(&source_path, b"{not valid connector json\n").expect("write source");
    let root = ScanRoot::local(source_path.clone());

    let active_filter = ActiveSessionSourceFilter::default();
    capture_scan_root_file_before_parse(
        &data_dir,
        "codex",
        &root,
        &active_filter,
        &RawSourceExclusions::default(),
    );

    let manifest_root = data_dir.join("raw-mirror/v1/manifests");
    let manifests = std::fs::read_dir(&manifest_root)
        .expect("manifest dir")
        .collect::<std::io::Result<Vec<_>>>()
        .expect("manifest entries");
    assert_eq!(manifests.len(), 1);
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(manifests[0].path()).expect("manifest bytes"))
            .expect("manifest json");
    assert_eq!(manifest["provider"].as_str(), Some("codex"));
    assert_eq!(manifest["source_id"].as_str(), Some("local"));
    assert_eq!(
        manifest["verification"]["status"].as_str(),
        Some("captured")
    );
    assert_eq!(
        std::fs::read(&source_path).expect("source remains untouched"),
        b"{not valid connector json\n"
    );
}

#[test]
fn raw_mirror_capture_expands_codex_directory_root_before_parse() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let codex_root = temp.path().join(".codex");
    let sessions = codex_root.join("sessions");
    std::fs::create_dir_all(&sessions).expect("sessions dir");
    let first_path = sessions.join("rollout-first.jsonl");
    let second_path = sessions.join("rollout-second.json");
    let ignored_path = sessions.join("notes.jsonl");
    let first_bytes =
        b"{\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"first\"}}\n";
    let second_bytes =
        b"{\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"second\"}}\n";
    std::fs::write(&first_path, first_bytes).expect("first source");
    std::fs::write(&second_path, second_bytes).expect("second source");
    std::fs::write(&ignored_path, b"{\"not\":\"a rollout file\"}\n").expect("ignored source");
    let root = ScanRoot::local(codex_root);

    let active_filter = ActiveSessionSourceFilter::default();
    capture_scan_sources_before_parse(
        &data_dir,
        "codex",
        &root,
        None,
        &active_filter,
        &RawSourceExclusions::default(),
    );

    let manifest_root = data_dir.join("raw-mirror/v1/manifests");
    let manifests = std::fs::read_dir(&manifest_root)
        .expect("manifest dir")
        .collect::<std::io::Result<Vec<_>>>()
        .expect("manifest entries");
    assert_eq!(
        manifests.len(),
        2,
        "codex directory preparse capture should mirror rollout files only"
    );
    let mut manifest_sources = manifests
        .iter()
        .map(|entry| {
            let manifest: serde_json::Value =
                serde_json::from_slice(&std::fs::read(entry.path()).expect("manifest bytes"))
                    .expect("manifest json");
            assert_eq!(manifest["provider"].as_str(), Some("codex"));
            assert_eq!(manifest["source_id"].as_str(), Some("local"));
            assert_eq!(
                manifest["verification"]["status"].as_str(),
                Some("captured")
            );
            manifest["original_path"]
                .as_str()
                .expect("original path")
                .to_string()
        })
        .collect::<Vec<_>>();
    manifest_sources.sort();
    assert_eq!(
        manifest_sources,
        vec![
            first_path.display().to_string(),
            second_path.display().to_string()
        ]
    );
    assert_eq!(
        std::fs::read(&first_path).expect("first bytes"),
        first_bytes
    );
    assert_eq!(
        std::fs::read(&second_path).expect("second bytes"),
        second_bytes
    );
    assert_eq!(
        std::fs::read(&ignored_path).expect("ignored bytes"),
        b"{\"not\":\"a rollout file\"}\n"
    );
}

#[test]
fn raw_mirror_capture_uses_discovered_sources_before_parser_failure() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let provider_root = temp.path().join("provider-root");
    std::fs::create_dir_all(&provider_root).expect("provider root");
    let source_path = provider_root.join("parse-fails.jsonl");
    let source_bytes = b"{this connector will fail after discovery\n";
    std::fs::write(&source_path, source_bytes).expect("write source");

    let root = ScanRoot::local(provider_root);
    let discovered = crate::connectors::DiscoveredSourceFile::new(
        "synthetic",
        &root,
        source_path.clone(),
        crate::connectors::DiscoveredSourceRole::PrimarySessionLog,
        true,
    )
    .with_fs_metadata();
    let connector = FailingDiscoveryConnector {
        sources: vec![discovered],
    };
    let ctx =
        crate::connectors::ScanContext::with_roots(temp.path().to_path_buf(), vec![root], None);

    let active_filter = ActiveSessionSourceFilter::default();
    capture_connector_sources_before_parse(
        &connector,
        &ctx,
        &data_dir,
        "synthetic",
        &[],
        None,
        &active_filter,
        &RawSourceExclusions::default(),
    );
    assert!(
        connector.scan(&ctx).is_err(),
        "test connector must fail after discovery to model parser failure"
    );

    let manifest_root = data_dir.join("raw-mirror/v1/manifests");
    let manifests = std::fs::read_dir(&manifest_root)
        .expect("manifest dir")
        .collect::<std::io::Result<Vec<_>>>()
        .expect("manifest entries");
    assert_eq!(
        manifests.len(),
        1,
        "discovered source should be captured even when later parsing fails"
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(manifests[0].path()).expect("manifest bytes"))
            .expect("manifest json");
    assert_eq!(manifest["provider"].as_str(), Some("synthetic"));
    assert_eq!(manifest["source_id"].as_str(), Some("local"));
    assert_eq!(manifest["db_links"].as_array().map(Vec::len), Some(0));
    assert_eq!(
        manifest["verification"]["status"].as_str(),
        Some("captured")
    );
    assert_eq!(
        std::fs::read(&source_path).expect("source remains untouched"),
        source_bytes
    );
}

#[test]
fn raw_mirror_capture_preserves_unreadable_copilot_state_before_parse() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let source_path = temp.path().join("state.vscdb");
    let source_bytes = b"unreadable legacy chat database evidence";
    std::fs::write(&source_path, source_bytes)?;
    for (kind, discovered) in [("discovered", true), ("explicit", false)] {
        let data_dir = temp.path().join(kind);
        if discovered {
            let root = ScanRoot::local(temp.path().to_path_buf());
            let source = DiscoveredSourceFile::new(
                "cursor",
                &root,
                source_path.clone(),
                crate::connectors::DiscoveredSourceRole::SqliteDatabase,
                true,
            );
            capture_discovered_source_file_before_parse(
                &data_dir,
                "cursor",
                &source,
                &RawSourceExclusions::default(),
            );
        } else {
            capture_scan_root_file_before_parse(
                &data_dir,
                "cursor",
                &ScanRoot::local(source_path.clone()),
                &ActiveSessionSourceFilter::default(),
                &RawSourceExclusions::default(),
            );
        }
        let manifests = raw_mirror_manifest_values(&data_dir);
        assert_eq!(
            manifests.len(),
            1,
            "ambiguous state must be preserved before parse"
        );
        let blob = manifests[0]["blob_relative_path"].as_str().unwrap();
        assert_eq!(
            std::fs::read(data_dir.join("raw-mirror/v1").join(blob))?,
            source_bytes
        );
    }
    let ctx = ScanContext::with_roots(
        temp.path().to_path_buf(),
        vec![ScanRoot::local(source_path.clone())],
        None,
    );
    let parsed = CursorConnector::new().scan(&ctx);
    assert!(parsed.is_err() || parsed.is_ok_and(|conversations| conversations.is_empty()));
    assert_eq!(std::fs::read(source_path)?, source_bytes);
    Ok(())
}

/// f3grf: the Claude connector registers as `claude` and its conversations
/// are `claude_code`. Pre-parse capture under the registry name filed a
/// second manifest for every Claude source, which no DB link reached, and
/// doctor reported a critical coverage finding on a fresh install.
#[test]
fn f3grf_claude_preparse_and_parsed_captures_share_one_manifest() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let provider_root = temp.path().join("projects");
    std::fs::create_dir_all(&provider_root).expect("provider root");
    let source_path = provider_root.join("session.jsonl");
    std::fs::write(&source_path, b"{\"type\":\"user\"}\n").expect("write source");
    let root = ScanRoot::local(provider_root);
    let connector = FailingDiscoveryConnector {
        sources: vec![discovered_test_source(
            &root,
            source_path.clone(),
            crate::connectors::DiscoveredSourceRole::PrimarySessionLog,
        )],
    };
    let ctx =
        crate::connectors::ScanContext::with_roots(temp.path().to_path_buf(), vec![root], None);
    capture_connector_sources_before_parse(
        &connector,
        &ctx,
        &data_dir,
        "claude",
        &[],
        None,
        &ActiveSessionSourceFilter::default(),
        &RawSourceExclusions::default(),
    );

    let mut conv = NormalizedConversation {
        agent_slug: "claude_code".to_string(),
        external_id: Some("f3grf-session".to_string()),
        title: None,
        workspace: None,
        source_path: source_path.clone(),
        started_at: Some(1_733_000_000_000),
        ended_at: Some(1_733_000_000_100),
        metadata: serde_json::json!({}),
        messages: Vec::new(),
    };
    inject_provenance(&mut conv, &Origin::local());
    attach_raw_mirror_capture(&data_dir, &mut conv, &RawSourceExclusions::default());

    let manifests = raw_mirror_manifest_values(&data_dir);
    let providers: Vec<_> = manifests
        .iter()
        .map(|manifest| manifest["provider"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(
        providers,
        vec!["claude_code"],
        "one Claude source must leave one manifest, under the archive slug"
    );
    assert_eq!(
        conv.metadata["cass"]["raw_mirror"]["manifest_id"].as_str(),
        manifests[0]["manifest_id"].as_str(),
        "the parsed conversation must reuse the pre-parse manifest that DB links will reach"
    );
}

#[test]
#[serial]
fn gh426_preparse_active_source_marks_connector_incomplete_without_a_conversation() {
    let _active_skip_reset = ActiveSessionSkipReset;
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let provider_root = temp.path().join("provider-root");
    std::fs::create_dir_all(&provider_root).expect("provider root");
    let source_path = provider_root.join("still-open.jsonl");
    std::fs::write(&source_path, b"partial source\n").expect("active source");
    let active_paths = std::env::join_paths([source_path.as_os_str()]).expect("active path");
    let _active_guard = set_env(
        "CASS_TEST_ACTIVE_SESSION_SOURCE_PATHS",
        active_paths.to_string_lossy().as_ref(),
    );

    let root = ScanRoot::local(provider_root);
    let connector = FailingDiscoveryConnector {
        sources: vec![discovered_test_source(
            &root,
            source_path,
            crate::connectors::DiscoveredSourceRole::PrimarySessionLog,
        )],
    };
    let ctx =
        crate::connectors::ScanContext::with_roots(temp.path().to_path_buf(), vec![root], None);
    let active_filter = ActiveSessionSourceFilter::default();

    let (_ingest_run, active_source_skipped) = capture_connector_sources_before_parse(
        &connector,
        &ctx,
        &data_dir,
        "synthetic",
        &[],
        None,
        &active_filter,
        &RawSourceExclusions::default(),
    );

    assert!(
        active_source_skipped,
        "preparse discovery must preserve this connector's watermark even when parsing yields no conversation"
    );
    assert!(
        raw_mirror_manifest_values(&data_dir).is_empty(),
        "an actively written source must not be captured as a stable raw artifact"
    );
}

#[test]
fn raw_mirror_capture_rejects_relative_discovered_source_paths() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let relative_root = ScanRoot::local(std::path::PathBuf::from("relative-provider-root"));
    let absolute_root = ScanRoot::local(temp.path().join("provider-root"));
    let connector = FailingDiscoveryConnector {
        sources: vec![
            discovered_test_source(
                &relative_root,
                std::path::PathBuf::from("relative-provider-root/session.jsonl"),
                crate::connectors::DiscoveredSourceRole::PrimarySessionLog,
            ),
            discovered_test_source(
                &absolute_root,
                std::path::PathBuf::from("relative-session.jsonl"),
                crate::connectors::DiscoveredSourceRole::PrimarySessionLog,
            ),
        ],
    };
    let ctx = crate::connectors::ScanContext::with_roots(
        temp.path().to_path_buf(),
        vec![relative_root, absolute_root],
        None,
    );

    let active_filter = ActiveSessionSourceFilter::default();
    capture_connector_sources_before_parse(
        &connector,
        &ctx,
        &data_dir,
        "synthetic",
        &[],
        None,
        &active_filter,
        &RawSourceExclusions::default(),
    );

    assert!(
        raw_mirror_manifest_values(&data_dir).is_empty(),
        "relative discovered roots or source paths must not publish raw-mirror manifests"
    );
}

#[test]
fn raw_mirror_capture_rejects_discovered_sources_that_escape_scan_root() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let provider_root = temp.path().join("provider-root");
    std::fs::create_dir_all(&provider_root).expect("provider root");
    let outside_dotdot = provider_root.join("../outside-dotdot.jsonl");
    let outside_absolute = temp.path().join("outside-absolute.jsonl");
    std::fs::write(&outside_dotdot, b"escaped through parent component\n").expect("dotdot source");
    std::fs::write(&outside_absolute, b"escaped through absolute path\n").expect("absolute source");

    let root = ScanRoot::local(provider_root);
    let connector = FailingDiscoveryConnector {
        sources: vec![
            discovered_test_source(
                &root,
                outside_dotdot.clone(),
                crate::connectors::DiscoveredSourceRole::PrimarySessionLog,
            ),
            discovered_test_source(
                &root,
                outside_absolute.clone(),
                crate::connectors::DiscoveredSourceRole::MetadataSidecar,
            ),
        ],
    };
    let ctx =
        crate::connectors::ScanContext::with_roots(temp.path().to_path_buf(), vec![root], None);

    let active_filter = ActiveSessionSourceFilter::default();
    capture_connector_sources_before_parse(
        &connector,
        &ctx,
        &data_dir,
        "synthetic",
        &[],
        None,
        &active_filter,
        &RawSourceExclusions::default(),
    );

    assert!(
        raw_mirror_manifest_values(&data_dir).is_empty(),
        "escaped discovered sources must not publish raw-mirror manifests"
    );
    assert_eq!(
        std::fs::read(&outside_dotdot).expect("dotdot source remains"),
        b"escaped through parent component\n"
    );
    assert_eq!(
        std::fs::read(&outside_absolute).expect("absolute source remains"),
        b"escaped through absolute path\n"
    );
}

#[test]
fn raw_mirror_capture_handles_deleted_after_discovery_source_without_manifest() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let provider_root = temp.path().join("provider-root");
    std::fs::create_dir_all(&provider_root).expect("provider root");
    let missing_source = provider_root.join("deleted-after-discovery.jsonl");

    let root = ScanRoot::local(provider_root);
    let connector = FailingDiscoveryConnector {
        sources: vec![discovered_test_source(
            &root,
            missing_source,
            crate::connectors::DiscoveredSourceRole::PrimarySessionLog,
        )],
    };
    let ctx =
        crate::connectors::ScanContext::with_roots(temp.path().to_path_buf(), vec![root], None);

    let active_filter = ActiveSessionSourceFilter::default();
    capture_connector_sources_before_parse(
        &connector,
        &ctx,
        &data_dir,
        "synthetic",
        &[],
        None,
        &active_filter,
        &RawSourceExclusions::default(),
    );

    assert!(
        raw_mirror_manifest_values(&data_dir).is_empty(),
        "missing discovered sources must not publish partial raw-mirror manifests"
    );
}

#[test]
fn raw_mirror_preparse_defers_large_primary_source_sets_but_keeps_sidecars() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let provider_root = temp.path().join("provider-root");
    std::fs::create_dir_all(&provider_root).expect("provider root");

    let root = ScanRoot::local(provider_root.clone());
    let mut sources = Vec::new();
    for i in 0..=PREPARSE_PRIMARY_SOURCE_CAPTURE_LIMIT {
        let source_path = provider_root.join(format!("session-{i}.jsonl"));
        std::fs::write(&source_path, format!("primary session bytes {i}\n"))
            .expect("primary source");
        sources.push(discovered_test_source(
            &root,
            source_path,
            crate::connectors::DiscoveredSourceRole::PrimarySessionLog,
        ));
    }

    let sidecar = provider_root.join("metadata.json");
    std::fs::write(&sidecar, b"{\"metadata\":true}\n").expect("sidecar source");
    sources.push(discovered_test_source(
        &root,
        sidecar.clone(),
        crate::connectors::DiscoveredSourceRole::MetadataSidecar,
    ));

    let connector = FailingDiscoveryConnector { sources };
    let ctx =
        crate::connectors::ScanContext::with_roots(temp.path().to_path_buf(), vec![root], None);

    let active_filter = ActiveSessionSourceFilter::default();
    capture_connector_sources_before_parse(
        &connector,
        &ctx,
        &data_dir,
        "synthetic",
        &[],
        None,
        &active_filter,
        &RawSourceExclusions::default(),
    );

    let manifests = raw_mirror_manifest_values(&data_dir);
    assert_eq!(
        manifests.len(),
        1,
        "large primary session-log sets should stream raw-mirror capture per parsed conversation instead of blocking preparse"
    );
    assert_eq!(manifests[0]["provider"].as_str(), Some("synthetic"));
    let sidecar_display = sidecar.display().to_string();
    assert_eq!(
        manifests[0]["original_path"].as_str(),
        Some(sidecar_display.as_str())
    );
    assert_eq!(
        manifests[0]["verification"]["status"].as_str(),
        Some("captured")
    );
}

#[test]
fn raw_mirror_capture_deduplicates_duplicate_discovered_sources_and_keeps_multi_file_set() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let provider_root = temp.path().join("provider-root");
    std::fs::create_dir_all(&provider_root).expect("provider root");
    let primary = provider_root.join("session.jsonl");
    let sidecar = provider_root.join("metadata.json");
    std::fs::write(&primary, b"primary session bytes\n").expect("primary source");
    std::fs::write(&sidecar, b"{\"metadata\":true}\n").expect("sidecar source");

    let root = ScanRoot::local(provider_root);
    let primary_source = discovered_test_source(
        &root,
        primary.clone(),
        crate::connectors::DiscoveredSourceRole::PrimarySessionLog,
    );
    let connector = FailingDiscoveryConnector {
        sources: vec![
            primary_source.clone(),
            primary_source,
            discovered_test_source(
                &root,
                sidecar.clone(),
                crate::connectors::DiscoveredSourceRole::MetadataSidecar,
            ),
        ],
    };
    let ctx =
        crate::connectors::ScanContext::with_roots(temp.path().to_path_buf(), vec![root], None);

    let active_filter = ActiveSessionSourceFilter::default();
    capture_connector_sources_before_parse(
        &connector,
        &ctx,
        &data_dir,
        "synthetic",
        &[],
        None,
        &active_filter,
        &RawSourceExclusions::default(),
    );

    let manifests = raw_mirror_manifest_values(&data_dir);
    assert_eq!(
        manifests.len(),
        2,
        "duplicate discovered sources should collapse to one manifest while multi-file sessions keep each source file"
    );
    let original_paths = manifests
        .iter()
        .map(|manifest| manifest["original_path"].as_str().expect("original path"))
        .collect::<Vec<_>>();
    assert_eq!(
        original_paths,
        vec![sidecar.display().to_string(), primary.display().to_string()]
    );
    for manifest in manifests {
        assert_eq!(manifest["provider"].as_str(), Some("synthetic"));
        assert_eq!(
            manifest["verification"]["status"].as_str(),
            Some("captured")
        );
        assert_eq!(manifest["db_links"].as_array().map(Vec::len), Some(0));
    }
    assert_eq!(
        std::fs::read(&primary).expect("primary remains"),
        b"primary session bytes\n"
    );
    assert_eq!(
        std::fs::read(&sidecar).expect("sidecar remains"),
        b"{\"metadata\":true}\n"
    );
}

#[test]
fn raw_mirror_capture_skips_logical_non_file_conversation_sources() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let db_path = temp.path().join("opencode.db");
    std::fs::write(&db_path, b"not a real sqlite fixture for this test")
        .expect("logical db source");

    let mut conv = NormalizedConversation {
        agent_slug: "opencode".to_string(),
        external_id: Some("ses_2b0314216ffeBBw5c7WZ41fFMl".to_string()),
        title: Some("OpenCode logical DB row".to_string()),
        workspace: None,
        source_path: db_path.join("ses_2b0314216ffeBBw5c7WZ41fFMl"),
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

    attach_raw_mirror_capture(&data_dir, &mut conv, &RawSourceExclusions::default());

    assert!(
        conv.metadata
            .get("cass")
            .and_then(|cass| cass.get("raw_mirror"))
            .is_none(),
        "logical DB-backed conversation paths must not receive file raw-mirror metadata"
    );
    assert!(
        raw_mirror_manifest_values(&data_dir).is_empty(),
        "logical non-file conversation paths must not publish failed raw-mirror manifests"
    );
    assert_eq!(
        std::fs::read(&db_path).expect("db source remains"),
        b"not a real sqlite fixture for this test"
    );
}

#[cfg(unix)]
#[test]
fn raw_mirror_logical_source_skip_preserves_symlink_validation_path() {
    let temp = TempDir::new().expect("tempdir");
    let real_source = temp.path().join("real.jsonl");
    let symlink_source = temp.path().join("link.jsonl");
    std::fs::write(&real_source, b"real source bytes\n").expect("real source");
    std::os::unix::fs::symlink(&real_source, &symlink_source).expect("source symlink");

    assert!(
        !should_skip_raw_mirror_capture_for_logical_source(&symlink_source),
        "symlink source paths should still reach raw-mirror's validator"
    );

    let db_path = temp.path().join("opencode.db");
    std::fs::write(&db_path, b"not a directory").expect("db source");
    assert!(
        should_skip_raw_mirror_capture_for_logical_source(&db_path.join("session-row-id")),
        "logical DB row paths should be treated as non-file source identifiers"
    );
}

#[cfg(unix)]
#[test]
fn raw_mirror_capture_rejects_discovered_symlink_source_without_manifest() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let provider_root = temp.path().join("provider-root");
    std::fs::create_dir_all(&provider_root).expect("provider root");
    let real_source = provider_root.join("real.jsonl");
    let symlink_source = provider_root.join("link.jsonl");
    std::fs::write(&real_source, b"real source bytes\n").expect("real source");
    std::os::unix::fs::symlink(&real_source, &symlink_source).expect("source symlink");

    let root = ScanRoot::local(provider_root);
    let connector = FailingDiscoveryConnector {
        sources: vec![discovered_test_source(
            &root,
            symlink_source,
            crate::connectors::DiscoveredSourceRole::PrimarySessionLog,
        )],
    };
    let ctx =
        crate::connectors::ScanContext::with_roots(temp.path().to_path_buf(), vec![root], None);

    let active_filter = ActiveSessionSourceFilter::default();
    capture_connector_sources_before_parse(
        &connector,
        &ctx,
        &data_dir,
        "synthetic",
        &[],
        None,
        &active_filter,
        &RawSourceExclusions::default(),
    );

    assert!(
        raw_mirror_manifest_values(&data_dir).is_empty(),
        "symlink discovered sources must not publish raw-mirror manifests"
    );
    assert_eq!(
        std::fs::read(&real_source).expect("real source remains"),
        b"real source bytes\n"
    );
}

#[cfg(unix)]
#[test]
fn raw_mirror_capture_rejects_discovered_source_under_symlink_parent() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    let provider_root = temp.path().join("provider-root");
    let outside_root = temp.path().join("outside-root");
    std::fs::create_dir_all(&provider_root).expect("provider root");
    std::fs::create_dir_all(&outside_root).expect("outside root");
    let real_source = outside_root.join("escaped.jsonl");
    let symlink_parent = provider_root.join("linkdir");
    let discovered_source = symlink_parent.join("escaped.jsonl");
    std::fs::write(&real_source, b"escaped through symlink parent\n").expect("real source");
    std::os::unix::fs::symlink(&outside_root, &symlink_parent).expect("parent symlink");

    let root = ScanRoot::local(provider_root);
    let connector = FailingDiscoveryConnector {
        sources: vec![discovered_test_source(
            &root,
            discovered_source,
            crate::connectors::DiscoveredSourceRole::PrimarySessionLog,
        )],
    };
    let ctx =
        crate::connectors::ScanContext::with_roots(temp.path().to_path_buf(), vec![root], None);

    let active_filter = ActiveSessionSourceFilter::default();
    capture_connector_sources_before_parse(
        &connector,
        &ctx,
        &data_dir,
        "synthetic",
        &[],
        None,
        &active_filter,
        &RawSourceExclusions::default(),
    );

    assert!(
        raw_mirror_manifest_values(&data_dir).is_empty(),
        "sources under symlink parents must not publish raw-mirror manifests"
    );
    assert_eq!(
        std::fs::read(&real_source).expect("real source remains"),
        b"escaped through symlink parent\n"
    );
}
