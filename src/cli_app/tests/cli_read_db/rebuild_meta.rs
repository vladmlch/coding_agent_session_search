#[allow(unused_imports)]
use super::*;

#[test]
#[serial]
fn unsupported_semantic_policy_does_not_route_to_unloadable_embedder() {
    let _embedder = set_env("CASS_SEMANTIC_EMBEDDER", "snowflake-arctic-s");

    assert_eq!(resolve_semantic_index_embedder(None), "fastembed");
    assert_eq!(
        resolve_semantic_index_embedder(Some("fastembed")),
        "fastembed"
    );
    assert_eq!(resolve_semantic_index_embedder(Some("minilm")), "minilm");
    assert_eq!(resolve_semantic_index_embedder(Some("hash")), "hash");
}

/// GH #381 (ntxs1): status shows where an interrupted expected-docs scan
/// stopped, from the position file such a run leaves beside the index,
/// and nothing for a position file that belongs to another archive.
#[test]
#[serial]
fn state_meta_json_reports_interrupted_expected_docs_scan_position() {
    let (temp, db_path) = seed_cli_db();
    let index_root = temp.path().join("index");
    std::fs::create_dir_all(&index_root).expect("index root");
    let partial_path = index_root.join(".expected-lexical-docs.partial.json");
    // The on-disk shape an interrupted scan saves (memo schema 2).
    let write_partial = |db: &Path| {
        let partial = serde_json::json!({
            "schema_version": 2,
            "db_path": db.display().to_string(),
            "identity": {
                "total_conversations": 9,
                "max_conversation_id": 9,
                "max_message_id": 40,
                "total_messages": 40,
                "content_cap_bytes": 8_388_608
            },
            "last_conversation_id": 4,
            "expected_docs": 17
        });
        std::fs::write(&partial_path, serde_json::to_vec(&partial).unwrap())
            .expect("write scan position");
    };

    write_partial(&db_path);
    let state = state_meta_json(temp.path(), &db_path, 60, true);
    let scan = &state["rebuild"]["expected_docs_scan"];
    assert_eq!(scan["last_conversation_id"].as_i64(), Some(4), "{scan}");
    assert_eq!(scan["max_conversation_id"].as_i64(), Some(9), "{scan}");
    assert_eq!(scan["expected_docs_so_far"].as_u64(), Some(17), "{scan}");
    assert!(scan["saved_at"].is_string(), "{scan}");

    write_partial(&temp.path().join("another-archive.db"));
    let state = state_meta_json(temp.path(), &db_path, 60, true);
    assert!(
        state["rebuild"]["expected_docs_scan"].is_null(),
        "{}",
        state["rebuild"]
    );
}

#[test]
#[serial]
fn state_meta_json_reports_lexical_rebuild_pipeline_settings() {
    let _responsiveness = set_env("CASS_RESPONSIVENESS_DISABLE", "1");
    let _workers = set_env("CASS_TANTIVY_REBUILD_WORKERS", "9");
    let _reserved_cores = set_env("CASS_TANTIVY_REBUILD_RESERVED_CORES", "4");
    let _controller_mode = set_env("CASS_TANTIVY_REBUILD_CONTROLLER_MODE", "steady");
    let _controller_clear_samples =
        set_env("CASS_TANTIVY_REBUILD_CONTROLLER_RESTORE_CLEAR_SAMPLES", "5");
    let _controller_hold_ms = set_env("CASS_TANTIVY_REBUILD_CONTROLLER_RESTORE_HOLD_MS", "2345");
    let _controller_loadavg_high = set_env(
        "CASS_TANTIVY_REBUILD_CONTROLLER_LOADAVG_HIGH_WATERMARK_1M",
        "7.5",
    );
    let _controller_loadavg_low = set_env(
        "CASS_TANTIVY_REBUILD_CONTROLLER_LOADAVG_LOW_WATERMARK_1M",
        "6.25",
    );
    let _steady_fetch = set_env("CASS_TANTIVY_REBUILD_BATCH_FETCH_CONVERSATIONS", "210");
    let _startup_fetch = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_BATCH_FETCH_CONVERSATIONS",
        "21",
    );
    let _steady_conversations = set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_CONVERSATIONS", "410");
    let _startup_conversations = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_CONVERSATIONS",
        "41",
    );
    let _steady_messages = set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGES", "510");
    let _startup_messages = set_env("CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_MESSAGES", "51");
    let _steady_message_bytes =
        set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGE_BYTES", "610000");
    let _startup_message_bytes = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_MESSAGE_BYTES",
        "61000",
    );
    let _pipeline_channel = set_env("CASS_TANTIVY_REBUILD_PIPELINE_CHANNEL_SIZE", "4");
    let _page_prep_workers = set_env("CASS_TANTIVY_REBUILD_PAGE_PREP_WORKERS", "6");
    let _pipeline_bytes = set_env(
        "CASS_TANTIVY_REBUILD_PIPELINE_MAX_MESSAGE_BYTES_IN_FLIGHT",
        "888888",
    );
    let _writer_threads = set_env("CASS_TANTIVY_MAX_WRITER_THREADS", "2");
    let _shard_builders = set_env("CASS_TANTIVY_REBUILD_STAGED_SHARD_BUILDERS", "4");
    let _merge_workers = set_env("CASS_TANTIVY_REBUILD_STAGED_MERGE_WORKERS", "2");

    let (temp, db_path) = seed_cli_db();
    let state = state_meta_json(temp.path(), &db_path, 60, true);
    let pipeline = &state["rebuild"]["pipeline"];

    assert_eq!(pipeline["workers"].as_u64(), Some(9));
    let available_parallelism = pipeline["available_parallelism"]
        .as_u64()
        .expect("available_parallelism is reported");
    assert_eq!(
        pipeline["reserved_cores"].as_u64(),
        Some(4_u64.min(available_parallelism.saturating_sub(1)))
    );
    assert_eq!(pipeline["controller_mode"].as_str(), Some("steady"));
    assert_eq!(
        pipeline["controller_restore_clear_samples"].as_u64(),
        Some(5)
    );
    assert_eq!(pipeline["controller_restore_hold_ms"].as_u64(), Some(2345));
    assert_eq!(
        pipeline["controller_loadavg_high_watermark_1m"].as_f64(),
        Some(7.5)
    );
    assert_eq!(
        pipeline["controller_loadavg_low_watermark_1m"].as_f64(),
        Some(6.25)
    );
    assert_eq!(
        pipeline["tantivy_writer_threads"].as_u64(),
        Some(available_parallelism.min(2))
    );
    assert_eq!(
        pipeline["staged_shard_builders"].as_u64(),
        Some(crate::indexer::responsiveness::effective_worker_count(4).max(1) as u64)
    );
    assert_eq!(
        pipeline["staged_merge_workers"].as_u64(),
        Some(crate::indexer::responsiveness::effective_worker_count(2).max(1) as u64)
    );
    assert_eq!(
        pipeline["page_size"].as_i64(),
        Some(crate::indexer::LEXICAL_REBUILD_PAGE_SIZE_PUBLIC)
    );
    assert_eq!(
        pipeline["steady_batch_fetch_conversations"].as_u64(),
        Some(210)
    );
    assert_eq!(
        pipeline["startup_batch_fetch_conversations"].as_u64(),
        Some(21)
    );
    assert_eq!(
        pipeline["steady_commit_every_conversations"].as_u64(),
        Some(410)
    );
    assert_eq!(
        pipeline["startup_commit_every_conversations"].as_u64(),
        Some(41)
    );
    assert_eq!(pipeline["steady_commit_every_messages"].as_u64(), Some(510));
    assert_eq!(pipeline["startup_commit_every_messages"].as_u64(), Some(51));
    assert_eq!(
        pipeline["steady_commit_every_message_bytes"].as_u64(),
        Some(610000)
    );
    assert_eq!(
        pipeline["startup_commit_every_message_bytes"].as_u64(),
        Some(61000)
    );
    assert_eq!(pipeline["pipeline_channel_size"].as_u64(), Some(4));
    assert_eq!(
        pipeline["page_prep_workers"].as_u64(),
        Some(crate::indexer::responsiveness::effective_worker_count(6).max(1) as u64)
    );
    assert_eq!(
        pipeline["pipeline_max_message_bytes_in_flight"].as_u64(),
        Some(888888)
    );
    // GH #496: a page holds at most an equal share of the in-flight
    // budget across the channel slots plus the sink (888888 / 5): the
    // steady 610000-byte commit interval is clamped, the 61000-byte
    // startup one is not.
    assert_eq!(
        pipeline["startup_page_fetch_message_bytes"].as_u64(),
        Some(61000)
    );
    assert_eq!(
        pipeline["steady_page_fetch_message_bytes"].as_u64(),
        Some(888888 / 5)
    );
    assert_eq!(pipeline["runtime"], serde_json::Value::Null);
}

#[test]
fn state_meta_json_reports_active_rebuild_pipeline_runtime() {
    let (temp, db_path) = seed_cli_db();
    let index_path = crate::search::tantivy::index_dir(temp.path()).expect("index dir");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    std::fs::write(
        index_path.join(".lexical-rebuild-state.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 3,
            "schema_hash": crate::search::tantivy::SCHEMA_HASH,
            "db": {
                "db_path": db_path.display().to_string(),
                "total_conversations": 10,
                "storage_fingerprint": "10:42:0:0"
            },
            "page_size": crate::indexer::LEXICAL_REBUILD_PAGE_SIZE_PUBLIC,
            "committed_offset": 4,
            "committed_conversation_id": 4,
            "processed_conversations": 4,
            "indexed_docs": 20,
            "committed_meta_fingerprint": null,
            "pending": null,
            "completed": false,
            "updated_at_ms": 1_733_000_123_000_i64,
            "runtime": {
                "queue_depth": 3,
                "inflight_message_bytes": 65_536,
                "max_message_bytes_in_flight": 131_072,
                "pending_batch_conversations": 9,
                "pending_batch_message_bytes": 131_072,
                "page_prep_workers": 6,
                "active_page_prep_jobs": 2,
                "ordered_buffered_pages": 4,
                "budget_generation": 1,
                "producer_budget_wait_count": 2,
                "producer_budget_wait_ms": 17,
                "producer_handoff_wait_count": 1,
                "producer_handoff_wait_ms": 9,
                "host_loadavg_1m_milli": 7_250,
                "controller_mode": "pressure_limited",
                "controller_reason": "queue_depth_3_reached_pipeline_capacity_3",
                "staged_merge_workers_max": 3,
                "staged_merge_allowed_jobs": 1,
                "staged_merge_active_jobs": 1,
                "staged_merge_ready_artifacts": 5,
                "staged_merge_ready_groups": 1,
                "staged_merge_controller_reason": "page_prep_workers_saturated_6_of_6",
                "staged_shard_build_workers_max": 6,
                "staged_shard_build_allowed_jobs": 5,
                "staged_shard_build_active_jobs": 4,
                "staged_shard_build_pending_jobs": 2,
                "staged_shard_build_controller_reason": "reserving_1_slots_for_staged_merge_active_jobs_1_ready_groups_1",
                "updated_at_ms": 1_733_000_124_000_i64
            }
        }))
        .expect("serialize rebuild state"),
    )
    .expect("write rebuild state");

    let lock_path = temp.path().join("index-run.lock");
    let mut lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open lock file");
    lock_file.try_lock_exclusive().expect("hold index lock");
    write_index_lock_metadata(
        &lock_path,
        &mut lock_file,
        &format!(
            "pid={}\nstarted_at_ms={}\ndb_path={}\nmode=index\n",
            std::process::id(),
            1_733_000_111_000_i64,
            db_path.display()
        ),
    );

    let state = state_meta_json(temp.path(), &db_path, 60, true);
    let runtime = &state["rebuild"]["pipeline"]["runtime"];
    let pipeline = &state["rebuild"]["pipeline"];

    assert_eq!(runtime["queue_depth"].as_u64(), Some(3));
    assert_eq!(
        runtime["queue_capacity"].as_u64(),
        pipeline["pipeline_channel_size"].as_u64()
    );
    assert_eq!(
        runtime["queue_headroom"].as_u64(),
        runtime["queue_capacity"]
            .as_u64()
            .map(|value| value.saturating_sub(3))
    );
    assert_eq!(runtime["inflight_message_bytes"].as_u64(), Some(65_536));
    assert_eq!(
        runtime["max_message_bytes_in_flight"].as_u64(),
        Some(131_072)
    );
    assert_eq!(
        runtime["inflight_message_bytes_headroom"].as_u64(),
        Some(65_536)
    );
    assert_eq!(runtime["pending_batch_conversations"].as_u64(), Some(9));
    assert_eq!(
        runtime["pending_batch_message_bytes"].as_u64(),
        Some(131_072)
    );
    assert_eq!(runtime["page_prep_workers"].as_u64(), Some(6));
    assert_eq!(runtime["active_page_prep_jobs"].as_u64(), Some(2));
    assert_eq!(runtime["ordered_buffered_pages"].as_u64(), Some(4));
    assert_eq!(runtime["budget_generation"].as_u64(), Some(1));
    assert_eq!(runtime["producer_budget_wait_count"].as_u64(), Some(2));
    assert_eq!(runtime["producer_budget_wait_ms"].as_u64(), Some(17));
    assert_eq!(runtime["producer_handoff_wait_count"].as_u64(), Some(1));
    assert_eq!(runtime["producer_handoff_wait_ms"].as_u64(), Some(9));
    assert_eq!(runtime["host_loadavg_1m"].as_f64(), Some(7.25));
    assert_eq!(
        runtime["controller_mode"].as_str(),
        Some("pressure_limited")
    );
    assert_eq!(
        runtime["controller_reason"].as_str(),
        Some("queue_depth_3_reached_pipeline_capacity_3")
    );
    assert_eq!(runtime["staged_merge_workers_max"].as_u64(), Some(3));
    assert_eq!(runtime["staged_merge_allowed_jobs"].as_u64(), Some(1));
    assert_eq!(runtime["staged_merge_active_jobs"].as_u64(), Some(1));
    assert_eq!(runtime["staged_merge_ready_artifacts"].as_u64(), Some(5));
    assert_eq!(runtime["staged_merge_ready_groups"].as_u64(), Some(1));
    assert_eq!(
        runtime["staged_merge_controller_reason"].as_str(),
        Some("page_prep_workers_saturated_6_of_6")
    );
    assert_eq!(runtime["staged_shard_build_workers_max"].as_u64(), Some(6));
    assert_eq!(runtime["staged_shard_build_allowed_jobs"].as_u64(), Some(5));
    assert_eq!(runtime["staged_shard_build_active_jobs"].as_u64(), Some(4));
    assert_eq!(runtime["staged_shard_build_pending_jobs"].as_u64(), Some(2));
    assert_eq!(
        runtime["staged_shard_build_controller_reason"].as_str(),
        Some("reserving_1_slots_for_staged_merge_active_jobs_1_ready_groups_1")
    );
    assert_eq!(
        runtime["updated_at"].as_str(),
        format_timestamp_millis_rfc3339(1_733_000_124_000_i64).as_deref()
    );
}

#[test]
fn state_meta_json_hides_empty_active_rebuild_pipeline_runtime_before_first_heartbeat() {
    let (temp, db_path) = seed_cli_db();
    let index_path = crate::search::tantivy::index_dir(temp.path()).expect("index dir");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    std::fs::write(
        index_path.join(".lexical-rebuild-state.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 3,
            "schema_hash": crate::search::tantivy::SCHEMA_HASH,
            "db": {
                "db_path": db_path.display().to_string(),
                "total_conversations": 10,
                "storage_fingerprint": "10:42:0:0"
            },
            "page_size": crate::indexer::LEXICAL_REBUILD_PAGE_SIZE_PUBLIC,
            "committed_offset": 4,
            "committed_conversation_id": 4,
            "processed_conversations": 4,
            "indexed_docs": 20,
            "committed_meta_fingerprint": null,
            "pending": null,
            "completed": false,
            "updated_at_ms": 1_733_000_123_000_i64
        }))
        .expect("serialize rebuild state"),
    )
    .expect("write rebuild state");

    let lock_path = temp.path().join("index-run.lock");
    let mut lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open lock file");
    lock_file.try_lock_exclusive().expect("hold index lock");
    write_index_lock_metadata(
        &lock_path,
        &mut lock_file,
        &format!(
            "pid={}\nstarted_at_ms={}\ndb_path={}\nmode=index\n",
            std::process::id(),
            1_733_000_111_000_i64,
            db_path.display()
        ),
    );

    let state = state_meta_json(temp.path(), &db_path, 60, true);
    assert_eq!(state["index"]["rebuilding"].as_bool(), Some(true));
    assert_eq!(state["rebuild"]["active"].as_bool(), Some(true));
    assert_eq!(
        state["rebuild"]["pipeline"]["runtime"],
        serde_json::Value::Null
    );
}

#[test]
fn state_meta_json_reports_active_rebuild() {
    let (temp, db_path) = seed_cli_db();
    let index_path = crate::search::tantivy::index_dir(temp.path()).expect("index dir");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    std::fs::write(
        index_path.join(".lexical-rebuild-state.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 3,
            "schema_hash": crate::search::tantivy::SCHEMA_HASH,
            "db": {
                "db_path": db_path.display().to_string(),
                "total_conversations": 10,
                "storage_fingerprint": "10:42:0:0"
            },
            "page_size": crate::indexer::LEXICAL_REBUILD_PAGE_SIZE_PUBLIC,
            "committed_offset": 4,
            "committed_conversation_id": 4,
            "processed_conversations": 4,
            "indexed_docs": 20,
            "committed_meta_fingerprint": null,
            "pending": null,
            "completed": false,
            "updated_at_ms": 1_733_000_123_000_i64
        }))
        .expect("serialize rebuild state"),
    )
    .expect("write rebuild state");

    let lock_path = temp.path().join("index-run.lock");
    let mut lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open lock file");
    lock_file.try_lock_exclusive().expect("hold index lock");
    write_index_lock_metadata(
        &lock_path,
        &mut lock_file,
        &format!(
            "pid={}\nstarted_at_ms={}\ndb_path={}\nmode=index\n",
            std::process::id(),
            1_733_000_111_000_i64,
            db_path.display()
        ),
    );

    let state = state_meta_json(temp.path(), &db_path, 60, true);
    assert_eq!(state["index"]["rebuilding"].as_bool(), Some(true));
    assert_eq!(state["rebuild"]["active"].as_bool(), Some(true));
    assert_eq!(state["pending"]["sessions"].as_u64(), Some(6));
    assert_eq!(
        state["rebuild"]["processed_conversations"].as_u64(),
        Some(4)
    );
    assert_eq!(state["rebuild"]["total_conversations"].as_u64(), Some(10));
}

#[test]
fn state_meta_json_prefers_pending_rebuild_progress_when_present() {
    let (temp, db_path) = seed_cli_db();
    let index_path = crate::search::tantivy::index_dir(temp.path()).expect("index dir");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    std::fs::write(
        index_path.join(".lexical-rebuild-state.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "version": 3,
            "schema_hash": crate::search::tantivy::SCHEMA_HASH,
            "db": {
                "db_path": db_path.display().to_string(),
                "total_conversations": 10,
                "storage_fingerprint": "10:42:0:0"
            },
            "page_size": crate::indexer::LEXICAL_REBUILD_PAGE_SIZE_PUBLIC,
            "committed_offset": 4,
            "committed_conversation_id": 4,
            "processed_conversations": 4,
            "indexed_docs": 20,
            "committed_meta_fingerprint": null,
            "pending": {
                "next_offset": 6,
                "next_conversation_id": 6,
                "processed_conversations": 6,
                "indexed_docs": 30,
                "base_meta_fingerprint": "stable-meta"
            },
            "completed": false,
            "updated_at_ms": 1_733_000_223_000_i64
        }))
        .expect("serialize rebuild state"),
    )
    .expect("write rebuild state");

    let lock_path = temp.path().join("index-run.lock");
    let mut lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open lock file");
    lock_file.try_lock_exclusive().expect("hold index lock");
    write_index_lock_metadata(
        &lock_path,
        &mut lock_file,
        &format!(
            "pid={}\nstarted_at_ms={}\ndb_path={}\nmode=index\n",
            std::process::id(),
            1_733_000_111_000_i64,
            db_path.display()
        ),
    );

    let state = state_meta_json(temp.path(), &db_path, 60, true);
    assert_eq!(state["rebuild"]["active"].as_bool(), Some(true));
    assert_eq!(state["pending"]["sessions"].as_u64(), Some(4));
    assert_eq!(
        state["rebuild"]["processed_conversations"].as_u64(),
        Some(6)
    );
    assert_eq!(state["rebuild"]["indexed_docs"].as_u64(), Some(30));
}

#[test]
fn state_meta_json_matches_active_rebuild_for_equivalent_db_path_spellings() {
    let (temp, db_path) = seed_cli_db();
    let db_path_variant = temp.path().join(".").join("agent_search.db");
    let index_path = crate::search::tantivy::index_dir(temp.path()).expect("index dir");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");

    let lock_path = temp.path().join("index-run.lock");
    let mut lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open lock file");
    lock_file.try_lock_exclusive().expect("hold index lock");
    write_index_lock_metadata(
        &lock_path,
        &mut lock_file,
        &format!(
            "pid={}\nstarted_at_ms={}\ndb_path={}\nmode=index\n",
            std::process::id(),
            1_733_000_556_000_i64,
            db_path_variant.display()
        ),
    );

    let state = state_meta_json(temp.path(), &db_path, 60, true);
    assert_eq!(state["index"]["rebuilding"].as_bool(), Some(true));
    assert_eq!(state["rebuild"]["active"].as_bool(), Some(true));
    assert_eq!(
        state["rebuild"]["pid"].as_u64(),
        Some(std::process::id() as u64)
    );
}

#[test]
fn state_meta_json_matches_active_rebuild_for_equivalent_missing_db_path_spellings() {
    let temp = tempfile::tempdir().expect("tempdir");
    let db_path = temp.path().join("agent_search.db");
    let db_path_variant = temp.path().join(".").join("agent_search.db");

    let lock_path = temp.path().join("index-run.lock");
    let mut lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open lock file");
    lock_file.try_lock_exclusive().expect("hold index lock");
    write_index_lock_metadata(
        &lock_path,
        &mut lock_file,
        &format!(
            "pid={}\nstarted_at_ms={}\ndb_path={}\nmode=index\n",
            std::process::id(),
            1_733_000_557_000_i64,
            db_path_variant.display()
        ),
    );

    let state = state_meta_json(temp.path(), &db_path, 60, true);
    assert_eq!(state["index"]["exists"].as_bool(), Some(false));
    assert_eq!(state["index"]["rebuilding"].as_bool(), Some(true));
    assert_eq!(state["rebuild"]["active"].as_bool(), Some(true));
    assert_eq!(
        state["rebuild"]["pid"].as_u64(),
        Some(std::process::id() as u64)
    );
}

#[test]
fn state_meta_json_reports_active_data_dir_lock_without_db_path_metadata() {
    let (temp, db_path) = seed_cli_db();
    let index_path = crate::search::tantivy::index_dir(temp.path()).expect("index dir");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");

    let lock_path = temp.path().join("index-run.lock");
    let mut lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open lock file");
    lock_file.try_lock_exclusive().expect("hold index lock");
    write_index_lock_metadata(
        &lock_path,
        &mut lock_file,
        &format!(
            "pid={}\nstarted_at_ms={}\nmode=index\n",
            std::process::id(),
            1_733_000_558_000_i64
        ),
    );

    let state = state_meta_json(temp.path(), &db_path, 60, true);
    assert_eq!(state["index"]["rebuilding"].as_bool(), Some(true));
    assert_eq!(state["rebuild"]["active"].as_bool(), Some(true));
    assert_eq!(
        state["rebuild"]["pid"].as_u64(),
        Some(std::process::id() as u64)
    );
    assert_eq!(
        state["rebuild"]["started_at"].as_str(),
        Some("2024-11-30T21:02:38+00:00")
    );
}

#[cfg(unix)]
#[test]
fn normalize_path_identity_resolves_existing_symlink_parent_for_missing_leaf() {
    let temp = tempfile::tempdir().expect("tempdir");
    let real_dir = temp.path().join("real");
    std::fs::create_dir_all(&real_dir).expect("real dir");
    let alias_dir = temp.path().join("alias");
    std::os::unix::fs::symlink(&real_dir, &alias_dir).expect("symlink");

    let via_symlink = alias_dir.join("agent_search.db");
    let via_real_dir = real_dir.join("agent_search.db");

    assert_eq!(
        normalize_path_identity(&via_symlink),
        normalize_path_identity(&via_real_dir)
    );
    assert!(path_identities_match(&via_symlink, &via_real_dir));
}

#[test]
fn state_meta_json_reports_active_rebuild_before_lexical_snapshot_exists() {
    let (temp, db_path) = seed_cli_db();
    let index_path = crate::search::tantivy::index_dir(temp.path()).expect("index dir");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");

    let lock_path = temp.path().join("index-run.lock");
    let mut lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open lock file");
    lock_file.try_lock_exclusive().expect("hold index lock");
    write_index_lock_metadata(
        &lock_path,
        &mut lock_file,
        &format!(
            "pid={}\nstarted_at_ms={}\ndb_path={}\nmode=index\n",
            std::process::id(),
            1_733_000_555_000_i64,
            db_path.display()
        ),
    );

    let state = state_meta_json(temp.path(), &db_path, 60, true);
    assert_eq!(state["index"]["rebuilding"].as_bool(), Some(true));
    assert_eq!(state["rebuild"]["active"].as_bool(), Some(true));
    assert_eq!(
        state["rebuild"]["pid"].as_u64(),
        Some(std::process::id() as u64)
    );
    assert_eq!(
        state["rebuild"]["processed_conversations"],
        serde_json::Value::Null
    );
    assert!(state["rebuild"]["updated_at"].as_str().is_some());
}

#[test]
fn state_meta_json_reports_watch_active_without_marking_rebuild() {
    let (temp, db_path) = seed_cli_db();
    let index_path = crate::search::tantivy::index_dir(temp.path()).expect("index dir");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");

    let lock_path = temp.path().join("index-run.lock");
    let mut lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open lock file");
    lock_file.try_lock_exclusive().expect("hold index lock");
    write_index_lock_metadata(
        &lock_path,
        &mut lock_file,
        &format!(
            "pid={}\nstarted_at_ms={}\ndb_path={}\nmode=watch\n",
            std::process::id(),
            1_733_000_777_000_i64,
            db_path.display()
        ),
    );

    let state = state_meta_json(temp.path(), &db_path, 60, true);
    assert_eq!(state["pending"]["watch_active"].as_bool(), Some(true));
    assert_eq!(state["pending"]["sessions"].as_u64(), Some(0));
    assert_eq!(state["index"]["rebuilding"].as_bool(), Some(false));
    assert_eq!(state["rebuild"]["active"].as_bool(), Some(false));
}

#[test]
fn gh422_state_meta_ignores_stale_lock_metadata_without_rewriting_it() {
    // gh#422 keeps SEARCH probes pure-read (pinned on
    // read_search_maintenance_snapshot in asset_state), while the
    // status/health observation surface exercised here follows the
    // #176 / bd-k9jb9 contract (pinned end-to-end in e2e_health):
    // stale metadata from a dead owner is hidden from the caller AND
    // reaped in place — truncated, never deleted — so later readers
    // observe a clean lock without re-doing the staleness dance.
    let (temp, db_path) = seed_cli_db();
    let index_path = crate::search::tantivy::index_dir(temp.path()).expect("index dir");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");

    let lock_path = temp.path().join("index-run.lock");
    std::fs::write(
        &lock_path,
        format!(
            concat!(
                "pid={}\n",
                "started_at_ms={}\n",
                "updated_at_ms={}\n",
                "db_path={}\n",
                "mode=index\n",
                "job_id={}\n",
                "job_kind=lexical_refresh\n",
                "phase=rebuilding\n"
            ),
            4242_u32,
            1_733_000_888_000_i64,
            1_733_000_999_000_i64,
            db_path.display(),
            "lexical_refresh-1733000888000-4242"
        ),
    )
    .expect("write orphaned lock metadata");

    let state = state_meta_json(temp.path(), &db_path, 60, true);
    assert_eq!(state["pending"]["orphaned"].as_bool(), Some(false));
    assert_eq!(state["rebuild"]["orphaned"].as_bool(), Some(false));
    assert_eq!(state["rebuild"]["active"].as_bool(), Some(false));
    assert_eq!(state["rebuild"]["job_kind"].as_str(), None);
    assert_eq!(state["rebuild"]["phase"].as_str(), None);
    assert_eq!(
        std::fs::metadata(&lock_path)
            .expect("stat lock after observation")
            .len(),
        0,
        "observation surfaces must reap stale lock metadata in place (#176 / bd-k9jb9)"
    );
    assert!(
        lock_path.exists(),
        "the lock file must be truncated, never deleted"
    );
}

#[test]
fn state_meta_json_uses_latest_lock_heartbeat_when_asset_inspection_fails() {
    let (temp, db_path) = seed_cli_db();
    let index_path = crate::search::tantivy::index_dir(temp.path()).expect("index dir");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    std::fs::create_dir_all(index_path.join(".lexical-rebuild-state.json"))
        .expect("create unreadable rebuild state path");

    let lock_path = temp.path().join("index-run.lock");
    let mut lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open lock file");
    lock_file.try_lock_exclusive().expect("hold index lock");
    write_index_lock_metadata(
        &lock_path,
        &mut lock_file,
        &format!(
            concat!(
                "pid={}\n",
                "started_at_ms={}\n",
                "updated_at_ms={}\n",
                "db_path={}\n",
                "mode=index\n"
            ),
            std::process::id(),
            1_733_000_555_000_i64,
            1_733_000_666_000_i64,
            db_path.display()
        ),
    );

    let state = state_meta_json(temp.path(), &db_path, 60, true);
    assert_eq!(state["index"]["status"].as_str(), Some("error"));
    assert_eq!(
        state["index"]["activity_at"].as_str(),
        Some("2024-11-30T21:04:26+00:00")
    );
    assert!(
        state["index"]["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("asset inspection failed"))
    );
}
