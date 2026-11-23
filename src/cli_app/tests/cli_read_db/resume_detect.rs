#[allow(unused_imports)]
use super::*;

#[test]
fn state_meta_json_does_not_infer_watch_activity_from_watch_state_file() {
    let (temp, db_path) = seed_cli_db();
    let index_path = crate::search::tantivy::index_dir(temp.path()).expect("index dir");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    std::fs::write(
        temp.path().join("watch_state.json"),
        br#"{"amp":1700000000000}"#,
    )
    .expect("write watch state");

    let state = state_meta_json(temp.path(), &db_path, 60, true);
    assert_eq!(state["pending"]["watch_active"].as_bool(), Some(false));
    assert_eq!(state["pending"]["sessions"].as_u64(), Some(0));
}

#[test]
fn state_meta_json_marks_lexical_fingerprint_mismatch_stale() {
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
                "storage_fingerprint": "stale-fingerprint"
            },
            "page_size": crate::indexer::LEXICAL_REBUILD_PAGE_SIZE_PUBLIC,
            "committed_offset": 10,
            "committed_conversation_id": 10,
            "processed_conversations": 10,
            "indexed_docs": 20,
            "committed_meta_fingerprint": null,
            "pending": null,
            "completed": true,
            "updated_at_ms": 1_733_000_123_000_i64
        }))
        .expect("serialize rebuild state"),
    )
    .expect("write rebuild state");

    let state = state_meta_json(temp.path(), &db_path, 60, true);
    assert_eq!(state["index"]["status"].as_str(), Some("stale"));
    assert_eq!(state["index"]["stale"].as_bool(), Some(true));
    assert_eq!(
        state["index"]["fingerprint"]["matches_current_db_fingerprint"].as_bool(),
        Some(false)
    );
    assert!(
        state["index"]["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("fingerprint"))
    );
    assert_eq!(state["pending"]["sessions"].as_u64(), Some(0));
    assert_eq!(
        state["rebuild"]["processed_conversations"],
        serde_json::Value::Null
    );
    assert_eq!(
        state["rebuild"]["total_conversations"],
        serde_json::Value::Null
    );
    assert_eq!(state["rebuild"]["indexed_docs"], serde_json::Value::Null);
    assert_eq!(state["semantic"]["fallback_mode"].as_str(), Some("lexical"));
}

#[test]
fn exact_artifact_contract_status_json_and_human_summary_share_owner_reason() {
    let (temp, db_path) = seed_cli_db();
    let index_path = crate::search::tantivy::index_dir(temp.path()).expect("index dir");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");

    let pointer_path =
        crate::search::semantic_manifest::SemanticCurrentPointerV1::path(temp.path());
    std::fs::create_dir_all(pointer_path.parent().expect("pointer parent"))
        .expect("create pointer parent");
    std::fs::write(
        &pointer_path,
        br#"{"schema_version":1,"generation_id":"selected","manifest_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","selection_epoch":1,"selected_at_ms":1733100000000}"#,
    )
    .expect("write semantic current pointer");

    let state = state_meta_json(temp.path(), &db_path, 60, true);
    let reason = crate::search::vector_index::SemanticProgressiveUnavailableReason::
        OwnerBackedReaderRequired
        .code();
    assert_eq!(
        state["semantic"]["progressive_reason_code"].as_str(),
        Some(reason)
    );
    assert_eq!(
        state["semantic"]["progressive_ready"].as_bool(),
        Some(false)
    );
    assert_eq!(
        state["semantic"]["availability"].as_str(),
        Some("load_failed")
    );
    assert!(
        state["semantic"]["summary"]
            .as_str()
            .is_some_and(|summary| summary.contains(reason)),
        "the human status summary and JSON reason code must use the same stable reason"
    );
}

#[test]
fn active_index_run_details_reads_matching_lock_metadata() {
    let (temp, db_path) = seed_cli_db();
    let lock_path = temp.path().join("index-run.lock");
    let pid = std::process::id();
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
            "pid={}\nstarted_at_ms={}\ndb_path={}\n",
            pid,
            1_733_001_111_000_i64,
            db_path.display()
        ),
    );

    let details =
        active_index_run_details(temp.path(), &db_path).expect("matching active index run");
    assert_eq!(details.pid, Some(pid));
    assert_eq!(details.started_at_ms, Some(1_733_001_111_000));
    assert_eq!(details.data_dir, temp.path());
    assert_eq!(details.db_path, db_path);
    assert_eq!(
        details.started_at_rfc3339().as_deref(),
        Some("2024-11-30T21:11:51+00:00")
    );
}

#[test]
fn active_index_run_details_reports_owner_for_other_db_in_same_data_dir() {
    let (temp, db_path) = seed_cli_db();
    let other_db_path = temp.path().join("other-agent-search.db");
    let lock_path = temp.path().join("index-run.lock");
    let pid = std::process::id();
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
            "pid={}\nstarted_at_ms={}\ndb_path={}\n",
            pid,
            1_733_001_222_000_i64,
            other_db_path.display()
        ),
    );

    let details = active_index_run_details(temp.path(), &db_path)
        .expect("active lock in same data dir should still be reported");
    assert_eq!(details.pid, Some(pid));
    assert_eq!(details.db_path, other_db_path);
}

#[test]
fn active_index_busy_cli_error_is_clean_and_structured() {
    let details = ActiveIndexRunDetails {
        pid: Some(777),
        started_at_ms: Some(1_733_001_333_000_i64),
        data_dir: PathBuf::from("/tmp/cass-data"),
        db_path: PathBuf::from("/tmp/cass-data/agent_search.db"),
        job_id: Some("lexical_refresh-1733001333000-777".to_string()),
        job_kind: Some(crate::search::asset_state::SearchMaintenanceJobKind::LexicalRefresh),
        phase: Some("rebuilding".to_string()),
    };

    let err = details.to_cli_error();
    assert_eq!(err.code, 7);
    assert_eq!(err.kind, "index-busy");
    assert!(err.retryable);
    assert!(
        err.message
            .contains("another 'cass index' run is already active"),
        "unexpected message: {}",
        err.message
    );
    assert!(
        err.message.contains("pid 777"),
        "pid should be surfaced cleanly: {}",
        err.message
    );
    assert!(
        err.message.contains("/tmp/cass-data"),
        "data dir/db should be surfaced cleanly: {}",
        err.message
    );
    assert!(
        err.hint
            .as_deref()
            .is_some_and(|hint| hint.contains("--data-dir/--db")),
        "unexpected hint: {:?}",
        err.hint
    );

    let payload = cli_error_json_payload(&err, 1234);
    assert_eq!(payload["success"].as_bool(), Some(false));
    assert_eq!(payload["elapsed_ms"].as_u64(), Some(1234));
    assert_eq!(payload["code"].as_i64(), Some(7));
    assert_eq!(payload["kind"].as_str(), Some("index-busy"));
    assert_eq!(payload["retryable"].as_bool(), Some(true));
}

#[test]
fn retryable_storage_contention_cli_error_is_lock_busy_shaped() {
    let chain = "canonical db is busy/locked during full rebuild open; refusing to replace it: database is locked";

    assert!(error_chain_indicates_retryable_storage_contention(chain));
    let err = index_storage_contention_cli_error(chain);

    assert_eq!(err.code, 7);
    assert_eq!(err.kind, "index-busy");
    assert!(err.retryable);
    assert!(
        err.message.contains("busy/locked"),
        "message should name the contention class: {}",
        err.message
    );
    assert!(
        err.message.contains("refusing to replace"),
        "message should preserve the no-replacement decision: {}",
        err.message
    );
    assert!(
        err.hint
            .as_deref()
            .is_some_and(|hint| hint.contains("Retry")),
        "retry hint should be actionable: {:?}",
        err.hint
    );

    let payload = cli_error_json_payload(&err, 99);
    assert_eq!(payload["code"].as_i64(), Some(7));
    assert_eq!(payload["kind"].as_str(), Some("index-busy"));
    assert_eq!(payload["retryable"].as_bool(), Some(true));
}

// =====================================================
// `cass resume` — issue #175
// =====================================================

#[test]
fn resume_detects_claude_code_from_path() {
    let path = PathBuf::from(
        "/Users/ellis/.claude/projects/-home-ellis-proj/abc12345-dead-beef-cafe-0123456789ab.jsonl",
    );
    let target = resolve_resume_target(&path, None).expect("resolve");
    assert_eq!(target.agent, "claude");
    assert_eq!(
        target.session_id.as_deref(),
        Some("abc12345-dead-beef-cafe-0123456789ab")
    );
    assert_eq!(
        target.argv,
        vec![
            "claude".to_string(),
            "--resume".to_string(),
            "abc12345-dead-beef-cafe-0123456789ab".to_string(),
        ]
    );
}

#[test]
fn resume_detects_codex_from_path() {
    let path =
        PathBuf::from("/Users/ellis/.codex/sessions/fedc1234-babe-cafe-beef-abcdef012345.jsonl");
    let target = resolve_resume_target(&path, None).expect("resolve");
    assert_eq!(target.agent, "codex");
    assert_eq!(
        target.session_id.as_deref(),
        Some("fedc1234-babe-cafe-beef-abcdef012345")
    );
    assert_eq!(target.argv[0], "codex");
    assert_eq!(target.argv[1], "resume");
}

#[test]
fn resume_agent_detection_accepts_native_windows_separators() {
    for (path, expected) in [
        (
            r"C:\Users\ellis\.claude\projects\-repo\session.jsonl",
            "claude",
        ),
        (r"C:\Users\ellis\.codex\sessions\session.jsonl", "codex"),
        (r"C:\Users\ellis\.gemini\sessions\session.jsonl", "gemini"),
    ] {
        let detected = detect_resume_agent(Path::new(path), None)
            .unwrap_or_else(|error| panic!("failed to detect {path}: {}", error.message));
        assert_eq!(detected.slug, expected, "Windows path {path}");
    }
}

#[test]
fn resume_detects_opencode_session_from_source_path() {
    let path = PathBuf::from("/Users/ellis/.local/share/opencode/opencode.db/sess%2D42");
    let target = resolve_resume_target(&path, None).expect("resolve");
    assert_eq!(target.agent, "opencode");
    assert_eq!(target.session_id.as_deref(), Some("sess-42"));
    // Current OpenCode continues a session via `--session <id>`, not the
    // removed `opencode resume <id>` subcommand (cass#316).
    assert_eq!(target.argv, vec!["opencode", "--session", "sess-42"]);
}

#[test]
fn resume_rejects_opencode_path_outside_opencode_db_parent() {
    // `~/.config/opencode/config.json` matches the opencode detection
    // heuristic but is NOT a session path. We must refuse to produce
    // a resume command rather than synthesize a plausible-but-wrong
    // `opencode --session config.json` invocation.
    let path = PathBuf::from("/Users/ellis/.config/opencode/config.json");
    let err = resolve_resume_target(&path, None).expect_err("must reject non-session path");
    assert_eq!(err.code, 5);
    assert_eq!(err.kind, "session-id-not-found");
    assert!(
        err.message.contains("opencode.db"),
        "error should explain the expected shape: {}",
        err.message
    );
    // The hint should point at the override escape hatch.
    assert!(
        err.hint
            .as_deref()
            .is_some_and(|h| h.contains("--agent opencode")),
        "hint should advertise the override: {:?}",
        err.hint
    );
}

#[test]
fn resume_allows_opencode_path_outside_parent_with_explicit_override() {
    // With `--agent opencode` the user takes responsibility for the
    // path shape. Cass uses the final path component as the id.
    let path = PathBuf::from("/Users/ellis/custom/opencode-export/sess-xyz");
    let target = resolve_resume_target(&path, Some("opencode"))
        .expect("explicit override must bypass parent-directory check");
    assert_eq!(target.agent, "opencode");
    assert_eq!(target.session_id.as_deref(), Some("sess-xyz"));
    assert_eq!(target.argv, vec!["opencode", "--session", "sess-xyz"]);
}

#[test]
fn resume_detects_gemini_from_path_and_uses_path_argv() {
    let temp = tempfile::tempdir().expect("tempdir");
    let gemini_sessions = temp.path().join(".gemini/sessions");
    std::fs::create_dir_all(&gemini_sessions).expect("create dir");
    let session_file = gemini_sessions.join("2026-04-09T00-00-00.jsonl");
    std::fs::write(&session_file, b"{}").expect("write session");

    let target = resolve_resume_target(&session_file, None).expect("resolve");
    assert_eq!(target.agent, "gemini");
    assert!(target.session_id.is_none());
    assert_eq!(target.argv[0], "gemini");
    assert_eq!(target.argv[1], "session");
    assert_eq!(target.argv[2], "restore");
    // The path is handed to an external tool: absolute, but never in
    // Windows' \\?\ verbatim form (txc9h; std canonicalize produced it).
    let expected = dunce::canonicalize(&session_file)
        .expect("canonical session path")
        .display()
        .to_string();
    assert_eq!(target.argv[3], expected);
    assert!(
        !target.argv[3].starts_with(r"\\?\"),
        "gemini restore argv must not carry the verbatim prefix: {}",
        target.argv[3]
    );
}

#[test]
fn resume_detects_antigravity_from_transcript_path() {
    // cass#314: an Antigravity transcript source path resolves to
    // `agy --conversation <uuid>`, where <uuid> is the `brain/<uuid>`
    // directory name. The path also contains `.gemini/`, so this proves
    // antigravity detection wins over the gemini arm.
    let uuid = "f1e2d3c4-b5a6-4789-9abc-def012345678";
    let path = PathBuf::from(format!(
        "/home/dev/.gemini/antigravity-cli/brain/{uuid}/.system_generated/logs/transcript.jsonl"
    ));
    let target = resolve_resume_target(&path, None).expect("resolve");
    assert_eq!(target.agent, "antigravity");
    assert_eq!(target.session_id.as_deref(), Some(uuid));
    assert_eq!(
        target.argv,
        vec!["agy", "--conversation", uuid],
        "antigravity must resume via `agy --conversation <uuid>`"
    );
}

#[test]
fn resume_detects_antigravity_ide_store_from_transcript_path() {
    // #454: the Antigravity IDE store is `~/.gemini/antigravity/` (no
    // `-cli` suffix) with the same brain/<uuid> layout. It must also win
    // over the gemini arm and resolve the uuid.
    let uuid = "00f0c687-1c8a-412b-a942-ec58f4b03c00";
    let path = PathBuf::from(format!(
        "/home/dev/.gemini/antigravity/brain/{uuid}/.system_generated/logs/transcript.jsonl"
    ));
    let target = resolve_resume_target(&path, None).expect("resolve");
    assert_eq!(target.agent, "antigravity");
    assert_eq!(target.session_id.as_deref(), Some(uuid));
    assert_eq!(target.argv, vec!["agy", "--conversation", uuid]);
    // The bare IDE base must not fall through to the gemini arm either.
    let detected = detect_resume_agent(
        Path::new("/home/dev/.gemini/antigravity/conversations/x.db"),
        None,
    )
    .expect("detect");
    assert_eq!(detected.slug, "antigravity");
}

#[test]
fn resume_antigravity_override_and_agy_alias() {
    // Both `--agent antigravity` and `--agent agy` map to the antigravity
    // slug; the UUID is recovered from the transcript path in either case.
    let uuid = "11111111-2222-3333-4444-555555555555";
    let path = PathBuf::from(format!(
        "/mnt/mirror/antigravity-cli/brain/{uuid}/.system_generated/logs/transcript_full.jsonl"
    ));
    for alias in ["antigravity", "agy"] {
        let target = resolve_resume_target(&path, Some(alias)).expect("resolve");
        assert_eq!(target.agent, "antigravity");
        assert_eq!(target.argv, vec!["agy", "--conversation", uuid]);
    }
}

#[test]
fn resume_antigravity_rejects_path_without_uuid() {
    // A path with no `brain/<uuid>` / `.system_generated` ancestry cannot
    // yield a conversation id — fail explicitly rather than fabricate one.
    let path = PathBuf::from("/home/dev/.gemini/antigravity-cli/notes.txt");
    let err = resolve_resume_target(&path, Some("agy")).expect_err("must reject");
    assert_eq!(err.code, 5);
    assert_eq!(err.kind, "session-id-not-found");
}

#[test]
fn resume_honors_explicit_agent_override() {
    // Claude path, but we force codex detection — legal alias handling.
    let path = PathBuf::from("/tmp/unknown/abc-123.jsonl");
    let target = resolve_resume_target(&path, Some("codex")).expect("resolve");
    assert_eq!(target.agent, "codex");
    assert_eq!(target.session_id.as_deref(), Some("abc-123"));
}

#[test]
fn resume_rejects_unknown_agent_override() {
    let path = PathBuf::from("/tmp/whatever/xyz.jsonl");
    let err = resolve_resume_target(&path, Some("bogus")).expect_err("must reject");
    assert_eq!(err.code, 2);
    assert_eq!(err.kind, "invalid-agent");
}

#[test]
fn resume_errors_when_no_agent_detectable() {
    let path = PathBuf::from("/tmp/somewhere/random/file.txt");
    let err = resolve_resume_target(&path, None).expect_err("must error");
    assert_eq!(err.code, 3);
    assert_eq!(err.kind, "unknown-agent");
}

#[test]
fn resume_does_not_false_positive_on_claude_code_substring() {
    // Paths like `~/projects/my_claude_code_extension/notes.jsonl`
    // must NOT be misdetected as Claude Code session files. The
    // canonical path pattern is `.claude/projects/...`.
    let path = PathBuf::from("/home/user/projects/my_claude_code_extension/notes.jsonl");
    let err = resolve_resume_target(&path, None).expect_err("must not false-match claude");
    assert_eq!(err.code, 3);
    assert_eq!(err.kind, "unknown-agent");
}

#[test]
fn looks_like_session_uuid_accepts_canonical_form() {
    // Canonical 8-4-4-4-12 hex UUIDs (mixed case allowed).
    assert!(looks_like_session_uuid(
        "abc12345-dead-beef-cafe-0123456789ab"
    ));
    assert!(looks_like_session_uuid(
        "ABC12345-DEAD-BEEF-CAFE-0123456789AB"
    ));
    assert!(looks_like_session_uuid(
        "fedc1234-babe-cafe-beef-abcdef012345"
    ));
    assert!(looks_like_session_uuid(
        "00000000-0000-0000-0000-000000000000"
    ));
    assert!(looks_like_session_uuid(
        "ffffffff-ffff-ffff-ffff-ffffffffffff"
    ));
}

#[test]
fn looks_like_session_uuid_rejects_bad_forms() {
    // Wrong length.
    assert!(!looks_like_session_uuid(""));
    assert!(!looks_like_session_uuid("abc"));
    assert!(!looks_like_session_uuid(
        "abc12345-dead-beef-cafe-0123456789a"
    )); // 35
    assert!(!looks_like_session_uuid(
        "abc12345-dead-beef-cafe-0123456789abc"
    )); // 37
    // Right length, wrong separator positions.
    assert!(!looks_like_session_uuid(
        "abc1234-5dead-beef-cafe-0123456789ab"
    ));
    // Non-hex characters.
    assert!(!looks_like_session_uuid(
        "xyz12345-dead-beef-cafe-0123456789ab"
    ));
    // Common wrong-path mistakes.
    assert!(!looks_like_session_uuid("2026-04-09T10-00-00_notes"));
    assert!(!looks_like_session_uuid("my_project_name"));
    assert!(!looks_like_session_uuid("notes"));
}

#[test]
fn resume_rejects_non_uuid_claude_path_without_override() {
    // `.claude/projects/foo/notes.jsonl` is under a claude projects
    // directory but the filename stem is not a UUID. Without an
    // explicit `--agent`, we block and suggest the user verify the
    // path.
    let path = PathBuf::from("/home/u/.claude/projects/-home-proj/notes.jsonl");
    let err = resolve_resume_target(&path, None)
        .expect_err("auto-detected non-UUID claude path must be rejected");
    assert_eq!(err.code, 5);
    assert_eq!(err.kind, "session-id-not-found");
    assert!(
        err.message.contains("notes"),
        "error should include the bad stem: {}",
        err.message
    );
    assert!(
        err.hint
            .as_deref()
            .is_some_and(|h| h.contains("--agent claude")),
        "hint should point at the override escape hatch: {:?}",
        err.hint
    );
}

#[test]
fn resume_allows_non_uuid_claude_path_with_explicit_override() {
    // With `--agent claude` the user is saying "trust me" — we
    // accept non-UUID stems as-is and let the downstream harness
    // decide.
    let path = PathBuf::from("/home/u/.claude/projects/-home-proj/legacy-name.jsonl");
    let target = resolve_resume_target(&path, Some("claude"))
        .expect("explicit override must bypass UUID validation");
    assert_eq!(target.agent, "claude");
    assert_eq!(target.session_id.as_deref(), Some("legacy-name"));
    assert_eq!(target.argv, vec!["claude", "--resume", "legacy-name"]);
}

#[test]
fn resume_rejects_non_uuid_codex_path_without_override() {
    let path = PathBuf::from("/home/u/.codex/sessions/2026/04/09/notes.jsonl");
    let err = resolve_resume_target(&path, None)
        .expect_err("auto-detected non-UUID codex path must be rejected");
    assert_eq!(err.code, 5);
    assert_eq!(err.kind, "session-id-not-found");
    assert!(
        err.message.contains("Codex"),
        "error should name the harness: {}",
        err.message
    );
}

#[test]
fn extract_uuid_from_stem_handles_bare_and_prefixed_forms() {
    // Bare UUID stem (Claude Code / legacy Codex layout).
    assert_eq!(
        extract_uuid_from_stem("fedc1234-babe-cafe-beef-abcdef012345").as_deref(),
        Some("fedc1234-babe-cafe-beef-abcdef012345")
    );
    // Codex rollout prefix: `rollout-<ISO timestamp>-<uuid>` — the
    // timestamp carries dashes but no 8-4-4-4-12 hex run, so only the
    // trailing UUID is recovered.
    assert_eq!(
        extract_uuid_from_stem("rollout-2026-06-03T17-39-34-019e8e23-d763-76a2-9273-9fa5b5fa1204")
            .as_deref(),
        Some("019e8e23-d763-76a2-9273-9fa5b5fa1204")
    );
    // No UUID present anywhere → None (caller falls back / errors).
    assert_eq!(extract_uuid_from_stem("notes"), None);
    assert_eq!(extract_uuid_from_stem("2026-04-09T10-00-00_notes"), None);
    // A UUID-shaped slice embedded in a longer hex run must be rejected by
    // the boundary checks (no false recovery from arbitrary hex blobs).
    assert_eq!(
        extract_uuid_from_stem("deadbeef12345678-dead-beef-cafe-0123456789abff"),
        None
    );
}

#[test]
fn resume_recovers_uuid_from_codex_rollout_prefix() {
    // Regression for cass#300: auto-detected Codex rollout files carry a
    // `rollout-<timestamp>-` prefix on the stem. Resume must recover the
    // canonical UUID and pass *that* (not the full stem) to `codex resume`.
    let path = PathBuf::from(
        "/Users/ellis/.codex/sessions/2026/06/03/rollout-2026-06-03T17-39-34-019e8e23-d763-76a2-9273-9fa5b5fa1204.jsonl",
    );
    let target =
        resolve_resume_target(&path, None).expect("prefixed Codex rollout filename must resolve");
    assert_eq!(target.agent, "codex");
    assert_eq!(
        target.session_id.as_deref(),
        Some("019e8e23-d763-76a2-9273-9fa5b5fa1204")
    );
    assert_eq!(
        target.argv,
        vec![
            "codex".to_string(),
            "resume".to_string(),
            "019e8e23-d763-76a2-9273-9fa5b5fa1204".to_string(),
        ]
    );
}

#[test]
fn resume_recovers_uuid_from_codex_rollout_prefix_with_override() {
    // `--agent codex` must also hand the bare UUID to `codex resume`, not
    // the rollout-prefixed stem (the documented workaround is now correct).
    let path = PathBuf::from(
        "/tmp/exported/rollout-2026-06-03T17-39-34-019e8e23-d763-76a2-9273-9fa5b5fa1204.jsonl",
    );
    let target = resolve_resume_target(&path, Some("codex"))
        .expect("explicit override on a prefixed rollout filename must resolve");
    assert_eq!(
        target.session_id.as_deref(),
        Some("019e8e23-d763-76a2-9273-9fa5b5fa1204")
    );
    assert_eq!(target.argv[2], "019e8e23-d763-76a2-9273-9fa5b5fa1204");
}

#[test]
fn run_resume_rejects_exec_with_structured_output() {
    // `--exec` + `--robot-format json` is a conflict clap cannot
    // see from the subcommand (robot_format is global). Runtime
    // rejects it with a clear usage error.
    let path = PathBuf::from("/home/user/.claude/projects/p/uuid.jsonl");
    let err = run_resume(&path, None, true, false, Some(RobotFormat::Json))
        .expect_err("must reject exec + structured output");
    assert_eq!(err.code, 2);
    assert_eq!(err.kind, "usage");
    assert!(
        err.message.contains("`--exec`"),
        "error should mention --exec: {}",
        err.message
    );
}

#[test]
fn run_resume_rejects_shell_with_structured_output() {
    let path = PathBuf::from("/home/user/.claude/projects/p/uuid.jsonl");
    let err = run_resume(&path, None, false, true, Some(RobotFormat::Json))
        .expect_err("must reject shell + structured output");
    assert_eq!(err.code, 2);
    assert_eq!(err.kind, "usage");
    assert!(
        err.message.contains("`--shell`"),
        "error should mention --shell: {}",
        err.message
    );
}

#[test]
fn shell_quote_escapes_special_characters() {
    assert_eq!(shell_quote("safe_value-1.0"), "safe_value-1.0");
    assert_eq!(shell_quote("has space"), "'has space'");
    assert_eq!(shell_quote("has'quote"), "'has'\\''quote'");
    assert_eq!(shell_quote(""), "''");
    assert_eq!(
        shell_quote("/abs/path/with-dashes"),
        "/abs/path/with-dashes"
    );
}
