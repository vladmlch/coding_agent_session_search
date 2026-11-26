use super::fts::fts_rebuild_dry_run_envelope;
use super::leaked_pages::{
    LeakedPagesVerdict, classify_leaked_pages, leaked_pages_bundle_components,
};
use super::*;
use crate::RobotFormat;
use crate::franken_sync::compat::{ConnectionExt as _, ParamValue, RowExt as _};
use crate::storage::sqlite::{
    FrankenStorage, FtsDryRunParity, FtsShadowParity, FtsShadowParityStatus,
    RecoveryConversationRow,
};

fn exact_dry_run_parity(parity: FtsShadowParity) -> FtsDryRunParity {
    FtsDryRunParity {
        exact_status: Some(parity.status),
        canonical_messages: parity.canonical_messages,
        indexable_messages: parity.indexable_messages,
        indexed_messages: parity.indexed_messages,
        inspection_complete: true,
        comparison_cap: FTS_DRY_RUN_ROWID_COMPARISON_CAP,
        canonical_ids_examined: usize::try_from(parity.indexable_messages.max(0))
            .unwrap_or(usize::MAX),
        indexed_ids_examined: parity
            .indexed_messages
            .and_then(|count| usize::try_from(count.max(0)).ok())
            .unwrap_or(0),
        observed_missing_canonical_rowids_at_least: 0,
        observed_excess_fts_rowids_at_least: 0,
        detail: parity.detail,
    }
}

fn write_message(storage: &FrankenStorage, conversation_id: i64, idx: i64, raw_line: &str) {
    // Store the verbatim line via the historical-raw-json sentinel wrapper
    // (the exact shape franken_message_insert_payload writes for raw lines).
    let wrapper = serde_json::json!({ "__cass_historical_raw_json__": raw_line });
    let extra = serde_json::to_string(&wrapper).unwrap();
    storage
        .raw()
        .execute_compat(
            "INSERT INTO messages(conversation_id, idx, role, author, created_at, content, extra_json, extra_bin) \
             VALUES(?1, ?2, 'user', NULL, ?3, ?4, ?5, NULL)",
            &[
                ParamValue::from(conversation_id),
                ParamValue::from(idx),
                ParamValue::from(1000_i64 + idx),
                ParamValue::from(format!("content {idx}")),
                ParamValue::from(extra),
            ] as &[ParamValue],
        )
        .expect("insert message");
}

fn seed_agent(storage: &FrankenStorage) -> i64 {
    // conversations.agent_id is NOT NULL REFERENCES agents(id) after
    // migrations, so a conversation row needs a real agent first.
    storage
        .raw()
        .execute_compat(
            "INSERT INTO agents(slug, name, version, kind, created_at, updated_at) \
             VALUES('claude', 'Claude Code', NULL, 'cli', 1000, 1000)",
            &[] as &[ParamValue],
        )
        .expect("insert agent");
    storage
        .raw()
        .query_row_map("SELECT last_insert_rowid()", &[] as &[ParamValue], |row| {
            row.get_typed::<i64>(0)
        })
        .expect("agent rowid")
}

fn seed_conversation(
    storage: &FrankenStorage,
    agent_id: i64,
    external_id: &str,
    source_path: &str,
) -> i64 {
    storage
        .raw()
        .execute_compat(
            "INSERT INTO conversations(agent_id, external_id, title, source_path, started_at) \
             VALUES(?1, ?2, ?3, ?4, 1000)",
            &[
                ParamValue::from(agent_id),
                ParamValue::from(external_id),
                ParamValue::from(format!("title {external_id}")),
                ParamValue::from(source_path),
            ] as &[ParamValue],
        )
        .expect("insert conversation");
    storage
        .raw()
        .query_row_map("SELECT last_insert_rowid()", &[] as &[ParamValue], |row| {
            row.get_typed::<i64>(0)
        })
        .expect("rowid")
}

#[test]
fn sanitize_path_component_strips_separators_and_traversal() {
    // Path separators collapse to '_', so the result is always a single
    // flat filename component (interior dots are harmless once no '/'
    // remains).
    assert_eq!(sanitize_path_component("a/b/../c"), "a_b_.._c");
    assert!(!sanitize_path_component("a/b/../c").contains('/'));
    assert_eq!(sanitize_path_component("normal-id_1.2"), "normal-id_1.2");
    assert_eq!(sanitize_path_component(""), "");
    // Leading/trailing dots are trimmed so we never emit "." or "..".
    assert_eq!(sanitize_path_component(".."), "");
    assert_eq!(sanitize_path_component("."), "");
}

#[test]
fn reconstruction_output_path_stays_inside_target_dir() {
    let target = Path::new("/tmp/recover");
    let out = reconstruction_output_path(
        target,
        7,
        Some("sess-abc"),
        Path::new("/home/u/.claude/projects/foo/bar.jsonl"),
    );
    assert!(out.starts_with(target));
    let name = out.file_name().unwrap().to_string_lossy();
    assert!(name.starts_with("sess-abc"));
    assert!(name.ends_with(".jsonl"));
    // A malicious external_id can never escape the recovery dir.
    let evil =
        reconstruction_output_path(target, 7, Some("../../etc/passwd"), Path::new("x.jsonl"));
    assert!(evil.starts_with(target));
    assert_eq!(evil.parent().unwrap(), target);
}

#[test]
fn recover_from_archive_reconstructs_verbatim_lines() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let db_path = tmp.path().join("agent_search.db");
    let target = tmp.path().join("recovered");
    {
        let storage = FrankenStorage::open(&db_path).expect("open db");
        let agent_id = seed_agent(&storage);
        let cid = seed_conversation(&storage, agent_id, "sess-1", "/orig/a.jsonl");
        write_message(
            &storage,
            cid,
            0,
            r#"{"type":"user","uuid":"u1","text":"hi"}"#,
        );
        write_message(
            &storage,
            cid,
            1,
            r#"{"type":"assistant","uuid":"a1","text":"yo"}"#,
        );
    }

    run_doctor_recover_from_archive(
        Some(tmp.path().to_path_buf()),
        Some(db_path.clone()),
        target.clone(),
        Some(RobotFormat::Json),
    )
    .expect("recover");

    // One .jsonl file with the two verbatim lines, in order.
    let out_file = std::fs::read_dir(&target)
        .expect("read recovered dir")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .find(|p| p.extension().and_then(|e| e.to_str()) == Some("jsonl"))
        .expect("a reconstructed jsonl file");
    let body = std::fs::read_to_string(&out_file).expect("read reconstructed file");
    let lines: Vec<&str> = body.lines().collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0], r#"{"type":"user","uuid":"u1","text":"hi"}"#);
    assert_eq!(lines[1], r#"{"type":"assistant","uuid":"a1","text":"yo"}"#);
}

#[test]
fn cleanup_interrupted_artifacts_quarantines_without_delete() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_dir = tmp.path().to_path_buf();
    let tmp_root = data_dir.join("raw-mirror").join("v1").join("tmp");
    std::fs::create_dir_all(tmp_root.join("capture.dead1")).expect("mk capture dir");
    std::fs::create_dir_all(tmp_root.join("capture.dead2")).expect("mk capture dir");

    // Without --yes the command refuses (and does not move anything).
    let refused = run_doctor_cleanup_interrupted_artifacts(
        Some(data_dir.clone()),
        false,
        Some(RobotFormat::Json),
    );
    assert!(refused.is_err());
    assert!(tmp_root.join("capture.dead1").exists());

    // With --yes the artifacts are quarantined (moved, not deleted).
    run_doctor_cleanup_interrupted_artifacts(Some(data_dir.clone()), true, Some(RobotFormat::Json))
        .expect("cleanup");
    assert!(!tmp_root.join("capture.dead1").exists());
    assert!(!tmp_root.join("capture.dead2").exists());
    let quarantine = data_dir
        .join("doctor")
        .join("quarantine")
        .join("interrupted-artifacts");
    assert!(quarantine.join("capture.dead1").exists());
    assert!(quarantine.join("capture.dead2").exists());
}

#[test]
fn rebuild_canonical_fts_refuses_without_yes() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let db_path = tmp.path().join("agent_search.db");
    {
        let _storage = FrankenStorage::open(&db_path).expect("open db");
    }
    let refused = run_doctor_rebuild_canonical_fts(
        Some(tmp.path().to_path_buf()),
        Some(db_path),
        false,
        false,
        Some(RobotFormat::Json),
    );
    assert!(refused.is_err());
}

#[test]
fn rebuild_canonical_fts_dry_run_with_yes_is_read_only() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let db_path = tmp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).expect("open db");
    let schema_rows_before: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'fts_messages'",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .expect("count FTS schema before dry-run");
    let marker_rows_before: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM meta WHERE key = 'fts_frankensqlite_rebuild_generation'",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .expect("count FTS generation markers before dry-run");
    drop(storage);
    let db_bytes_before = std::fs::read(&db_path).expect("snapshot database before dry-run");

    run_doctor_rebuild_canonical_fts(
        Some(tmp.path().to_path_buf()),
        Some(db_path.clone()),
        true,
        true,
        Some(RobotFormat::Json),
    )
    .expect("dry-run with --yes must remain read-only");
    let db_bytes_after = std::fs::read(&db_path).expect("snapshot database after dry-run");
    assert_eq!(
        db_bytes_after, db_bytes_before,
        "dry-run with --yes must not alter any canonical database bytes"
    );

    let storage = FrankenStorage::open_readonly(&db_path).expect("reopen read-only");
    let schema_rows_after: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'fts_messages'",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .expect("count FTS schema after dry-run");
    let marker_rows_after: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM meta WHERE key = 'fts_frankensqlite_rebuild_generation'",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .expect("count FTS generation markers after dry-run");
    assert_eq!(schema_rows_after, schema_rows_before);
    assert_eq!(marker_rows_after, marker_rows_before);
}

#[test]
fn rebuild_canonical_fts_repairs_absent_shadow_without_canonical_row_changes() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let db_path = tmp.path().join("agent_search.db");
    let conversation_id = {
        let storage = FrankenStorage::open(&db_path).expect("open db");
        let agent_id = seed_agent(&storage);
        let conversation_id =
            seed_conversation(&storage, agent_id, "fts-repair", "/orig/fts.jsonl");
        write_message(
            &storage,
            conversation_id,
            0,
            r#"{"type":"user","uuid":"fts-1","text":"canonical sentinel"}"#,
        );
        assert_eq!(
            storage
                .inspect_search_fallback_fts_parity()
                .expect("inspect absent FTS")
                .status,
            FtsShadowParityStatus::Absent
        );
        conversation_id
    };

    run_doctor_rebuild_canonical_fts(
        Some(tmp.path().to_path_buf()),
        Some(db_path.clone()),
        false,
        true,
        Some(RobotFormat::Json),
    )
    .expect("repair absent FTS through schema-only writer");

    let readonly = FrankenStorage::open_readonly(&db_path).expect("reopen read-only");
    let parity = readonly
        .inspect_search_fallback_fts_parity()
        .expect("inspect repaired FTS");
    assert_eq!(parity.status, FtsShadowParityStatus::Healthy);
    assert_eq!(parity.canonical_messages, 1);
    assert_eq!(parity.indexable_messages, 1);
    assert_eq!(parity.indexed_messages, Some(1));
    let canonical: (i64, i64, String, String) = readonly
        .raw()
        .query_row_map(
            "SELECT id, conversation_id, content, extra_json FROM messages",
            &[] as &[ParamValue],
            |row| {
                Ok((
                    row.get_typed(0)?,
                    row.get_typed(1)?,
                    row.get_typed(2)?,
                    row.get_typed(3)?,
                ))
            },
        )
        .expect("read canonical sentinel after repair");
    assert_eq!(canonical.0, 1);
    assert_eq!(canonical.1, conversation_id);
    assert_eq!(canonical.2, "content 0");
    assert!(canonical.3.contains("canonical sentinel"));
}

/// GH #438: an explicit repair of a parity-healthy shadow must rewrite its
/// segments (older validators rejected unmerged segments even though reads
/// work), not report "already healthy" and leave them. Three catch-up passes
/// leave several segments; the repair must merge them into one without
/// changing any search result.
#[test]
fn rebuild_canonical_fts_rewrites_segments_of_a_parity_healthy_shadow() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let db_path = tmp.path().join("agent_search.db");
    let segment_count = |storage: &FrankenStorage| -> i64 {
        storage
            .raw()
            .query_row_map(
                "SELECT COUNT(DISTINCT segid) FROM fts_messages_idx",
                &[] as &[ParamValue],
                |row| row.get_typed(0),
            )
            .expect("count FTS segments")
    };
    let matches = |storage: &FrankenStorage| -> i64 {
        storage
            .raw()
            .query_row_map(
                "SELECT COUNT(*) FROM fts_messages WHERE fts_messages MATCH 'content'",
                &[] as &[ParamValue],
                |row| row.get_typed(0),
            )
            .expect("match canonical content")
    };
    let before_matches = {
        let storage = FrankenStorage::open(&db_path).expect("open db");
        let agent_id = seed_agent(&storage);
        let conversation_id =
            seed_conversation(&storage, agent_id, "fts-segments", "/orig/segments.jsonl");
        for idx in 0..3 {
            write_message(&storage, conversation_id, idx, r#"{"type":"user"}"#);
            storage
                .ensure_search_fallback_fts_consistency()
                .expect("build or catch up the FTS shadow");
        }
        assert_eq!(
            storage
                .inspect_search_fallback_fts_parity()
                .expect("inspect FTS")
                .status,
            FtsShadowParityStatus::Healthy
        );
        assert!(
            segment_count(&storage) >= 2,
            "fixture must leave several segments to rewrite"
        );
        matches(&storage)
    };

    run_doctor_rebuild_canonical_fts(
        Some(tmp.path().to_path_buf()),
        Some(db_path.clone()),
        false,
        true,
        Some(RobotFormat::Json),
    )
    .expect("repair a parity-healthy shadow");

    let storage = FrankenStorage::open(&db_path).expect("reopen db");
    assert_eq!(
        segment_count(&storage),
        1,
        "segments must be rewritten into one"
    );
    assert_eq!(
        matches(&storage),
        before_matches,
        "search results must not change"
    );
    assert_eq!(before_matches, 3);
    storage
        .validate_fts_messages_integrity()
        .expect("rewritten segments pass the FTS5 integrity-check");
}

#[test]
fn divergent_fts_dry_run_contract_refuses_mutation() {
    let parity = FtsShadowParity {
        status: FtsShadowParityStatus::Divergent,
        canonical_messages: 2,
        indexable_messages: 2,
        indexed_messages: Some(2),
        detail: Some("equal counts conceal rowid divergence".to_string()),
    };
    let parity = exact_dry_run_parity(parity);
    let envelope = fts_rebuild_dry_run_envelope(Path::new("/tmp/divergent.db"), &parity, false);
    assert_eq!(
        envelope["planned_action"],
        "refuse_unsafe_destructive_rebuild"
    );
    assert_eq!(envelope["would_mutate"], false);
    assert_eq!(envelope["apply_command"], serde_json::Value::Null);
    assert_eq!(envelope["parity"]["status"], "divergent");
}

/// GH #497 follow-up: over the shadow bound, the dry-run plan must match
/// what the apply does. A residue shadow is dropped and retired, an absent
/// shadow without the retirement markers gets them, and an already
/// recorded retirement is settled: nothing to apply, nothing mutated.
#[test]
fn gh497_oversized_fts_dry_run_plans_match_the_retirement_state() {
    let Some(bound) = crate::storage::sqlite::fts_shadow_max_messages() else {
        // CASS_FTS_SHADOW_MAX_MESSAGES=0 in this environment disables the
        // bound, so no corpus is oversized and there is nothing to plan.
        return;
    };
    let over_bound = i64::try_from(bound.saturating_add(1)).unwrap_or(i64::MAX);
    let oversized = |status| {
        exact_dry_run_parity(FtsShadowParity {
            status,
            canonical_messages: over_bound,
            indexable_messages: over_bound,
            indexed_messages: None,
            detail: None,
        })
    };
    let db = Path::new("/tmp/oversized.db");

    let settled = fts_rebuild_dry_run_envelope(db, &oversized(FtsShadowParityStatus::Absent), true);
    assert_eq!(settled["planned_action"], "none_shadow_retired_not_viable");
    assert_eq!(settled["would_mutate"], false);
    assert_eq!(settled["shadow_retired_not_viable"], true);
    assert_eq!(settled["apply_command"], serde_json::Value::Null);

    let unrecorded =
        fts_rebuild_dry_run_envelope(db, &oversized(FtsShadowParityStatus::Absent), false);
    assert_eq!(unrecorded["planned_action"], "record_not_viable_retirement");
    assert_eq!(unrecorded["would_mutate"], true);
    assert_eq!(unrecorded["shadow_retired_not_viable"], false);

    let residue =
        fts_rebuild_dry_run_envelope(db, &oversized(FtsShadowParityStatus::Residue), false);
    assert_eq!(
        residue["planned_action"],
        "drop_residue_and_mark_not_viable"
    );
    assert_eq!(residue["would_mutate"], true);

    // Within the bound a recorded marker is obsolete: the apply clears it
    // and recreates the shadow, so the plan says so.
    let mut fits = oversized(FtsShadowParityStatus::Absent);
    fits.indexable_messages = 1;
    let fits = fts_rebuild_dry_run_envelope(db, &fits, true);
    assert_eq!(fits["planned_action"], "failure_atomic_recreate");
    assert_eq!(fits["would_mutate"], true);
}

#[test]
fn unqueryable_fts_dry_run_preserves_bundle_instead_of_advertising_apply() {
    let parity = FtsShadowParity {
        status: FtsShadowParityStatus::Unqueryable,
        canonical_messages: 2,
        indexable_messages: 2,
        indexed_messages: None,
        detail: Some("counting fts_messages_docsize failed".to_string()),
    };
    let parity = exact_dry_run_parity(parity);
    let envelope = fts_rebuild_dry_run_envelope(Path::new("/tmp/unqueryable.db"), &parity, false);
    assert_eq!(
        envelope["planned_action"],
        "refuse_unqueryable_preserve_bundle"
    );
    assert_eq!(envelope["would_mutate"], false);
    assert_eq!(envelope["apply_command"], serde_json::Value::Null);
}

/// #345: `CASS_FTS_DRYRUN_CAP` truthiness/validity contract (pure parse
/// half — the env wrapper only feeds it the raw string).
#[test]
fn gh345_dry_run_cap_env_parsing() {
    assert_eq!(
        parse_fts_dry_run_cap(None),
        FTS_DRY_RUN_ROWID_COMPARISON_CAP
    );
    assert_eq!(parse_fts_dry_run_cap(Some("512")), 512);
    assert_eq!(parse_fts_dry_run_cap(Some(" 8 ")), 8);
    // Zero and garbage fall back to the default (a zero cap would make
    // every dry-run indeterminate and trip the storage-layer ensure).
    assert_eq!(
        parse_fts_dry_run_cap(Some("0")),
        FTS_DRY_RUN_ROWID_COMPARISON_CAP
    );
    assert_eq!(
        parse_fts_dry_run_cap(Some("not-a-number")),
        FTS_DRY_RUN_ROWID_COMPARISON_CAP
    );
    assert_eq!(
        parse_fts_dry_run_cap(Some("")),
        FTS_DRY_RUN_ROWID_COMPARISON_CAP
    );
}

/// #345: a capped dry-run envelope must carry the ">= N divergent" floor
/// while staying indeterminate and deferring exact parity to the apply.
#[test]
fn gh345_capped_dry_run_envelope_reports_divergence_floor() {
    let parity = FtsDryRunParity {
        exact_status: None,
        canonical_messages: 2_000_000,
        indexable_messages: 2_000_000,
        indexed_messages: Some(1_395_000),
        inspection_complete: false,
        comparison_cap: 4_096,
        canonical_ids_examined: 4_096,
        indexed_ids_examined: 4_096,
        observed_missing_canonical_rowids_at_least: 7,
        observed_excess_fts_rowids_at_least: 2,
        detail: Some(
            "bounded dry-run stopped after at most 4096 row IDs per domain (>= 9 divergent row ID(s) observed within the cap: missing >= 7, excess >= 2); exact parity is deferred to --yes before any mutation".to_string(),
        ),
    };
    assert_eq!(parity.divergent_rowids_at_least(), 9);
    let envelope = fts_rebuild_dry_run_envelope(Path::new("/tmp/large.db"), &parity, false);
    assert_eq!(envelope["parity"]["status"], "indeterminate");
    assert_eq!(envelope["parity"]["inspection_complete"], false);
    assert_eq!(envelope["parity"]["divergent_rowids_at_least"], 9);
    assert_eq!(
        envelope["planned_action"],
        "exact_parity_inspection_deferred_to_apply"
    );
    assert_eq!(envelope["would_mutate"], serde_json::Value::Null);
}

#[test]
fn gh345_indeterminate_fts_dry_run_defers_exact_parity_without_claiming_mutation() {
    let parity = FtsDryRunParity {
        exact_status: None,
        canonical_messages: 2_000_000,
        indexable_messages: 2_000_000,
        indexed_messages: Some(39_000),
        inspection_complete: false,
        comparison_cap: FTS_DRY_RUN_ROWID_COMPARISON_CAP,
        canonical_ids_examined: FTS_DRY_RUN_ROWID_COMPARISON_CAP,
        indexed_ids_examined: FTS_DRY_RUN_ROWID_COMPARISON_CAP,
        observed_missing_canonical_rowids_at_least: 0,
        observed_excess_fts_rowids_at_least: 0,
        detail: Some("bounded comparison reached its cap".to_string()),
    };
    let envelope = fts_rebuild_dry_run_envelope(Path::new("/tmp/large.db"), &parity, false);
    assert_eq!(envelope["parity"]["status"], "indeterminate");
    assert_eq!(envelope["parity"]["inspection_complete"], false);
    assert_eq!(
        envelope["planned_action"],
        "exact_parity_inspection_deferred_to_apply"
    );
    assert_eq!(envelope["would_mutate"], serde_json::Value::Null);
    assert_eq!(
        envelope["apply_command"],
        "cass doctor --rebuild-canonical-fts --yes --json"
    );
}

/// GH #362: a single whitespace-delimited token beyond the FTS5 u16
/// leaf-offset space (91,548 bytes observed in a real Codex rollout) used
/// to fail every canonical FTS repair path with "fts5: corrupt %_data
/// record: segment leaf term offset exceeds u16" — including this exact
/// `--rebuild-canonical-fts` recovery. With C SQLite FTS5, the rebuild
/// completes natively and neighboring terms in the same message stay
/// indexed.
#[test]
fn rebuild_canonical_fts_survives_overlong_term_in_corpus() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let db_path = tmp.path().join("agent_search.db");
    let giant = "a".repeat(91_548);
    {
        let storage = FrankenStorage::open(&db_path).expect("open db");
        let agent_id = seed_agent(&storage);
        let conversation_id =
            seed_conversation(&storage, agent_id, "overlong", "/orig/overlong.jsonl");
        // The giant token must land in `messages.content` — that is the
        // column the FTS rebuild streams through the tokenizer. (The
        // `write_message` helper stores a placeholder content, which
        // would never exercise the overlong path.)
        for (idx, content) in [
            (0_i64, format!("before {giant} needle")),
            (1_i64, "ordinary reply".to_string()),
        ] {
            storage
                .raw()
                .execute_compat(
                    "INSERT INTO messages(conversation_id, idx, role, author, created_at, content, extra_json, extra_bin) \
                     VALUES(?1, ?2, 'user', NULL, ?3, ?4, NULL, NULL)",
                    &[
                        ParamValue::from(conversation_id),
                        ParamValue::from(idx),
                        ParamValue::from(1000_i64 + idx),
                        ParamValue::from(content),
                    ] as &[ParamValue],
                )
                .expect("insert message with overlong content");
        }
    }

    run_doctor_rebuild_canonical_fts(
        Some(tmp.path().to_path_buf()),
        Some(db_path.clone()),
        false,
        true,
        Some(RobotFormat::Json),
    )
    .expect("rebuild must survive an overlong term in the corpus (GH #362)");

    let readonly = FrankenStorage::open_readonly(&db_path).expect("reopen read-only");
    let parity = readonly
        .inspect_search_fallback_fts_parity()
        .expect("inspect rebuilt FTS");
    assert_eq!(parity.status, FtsShadowParityStatus::Healthy);
    assert_eq!(parity.canonical_messages, 2);
    assert_eq!(parity.indexed_messages, Some(2));
}

/// GH #368 (defect 3): when the FTS5 `%_data` *structure* record is corrupt,
/// querying `fts_messages` fails even though opening succeeds; `--rebuild-canonical-fts --yes`
/// must repair it by dropping + recreating + repopulating the shadow from canonical rows.
/// This drives the full doctor CLI fallback end-to-end (`is_fts5_shadow_open_corruption_error`
/// detection plus the corrupt-shadow branch of `run_doctor_rebuild_canonical_fts`,
/// including the read-only dry-run report) — coverage the storage-level test
/// `drop_recreate_repairs_corrupt_fts_shadow_structure` does not reach.
#[test]
fn rebuild_canonical_fts_repairs_structurally_corrupt_shadow_that_blocks_open() {
    use crate::franken_sync::Connection as FrankenConnection;

    let tmp = tempfile::tempdir().expect("tempdir");
    let db_path = tmp.path().join("agent_search.db");

    // Canonical data + a VALID FTS shadow (which we then corrupt).
    {
        let storage = FrankenStorage::open(&db_path).expect("open db");
        let agent_id = seed_agent(&storage);
        let conversation_id =
            seed_conversation(&storage, agent_id, "corrupt-shadow", "/orig/corrupt.jsonl");
        // The FTS rebuild streams `messages.content`, so the matchable token
        // must land there (the `write_message` helper stores a placeholder
        // content and would never be queryable for 'needle').
        storage
            .raw()
            .execute_compat(
                "INSERT INTO messages(conversation_id, idx, role, author, created_at, content, extra_json, extra_bin) \
                 VALUES(?1, 0, 'user', NULL, 1000, ?2, NULL, NULL)",
                &[
                    ParamValue::from(conversation_id),
                    ParamValue::from("authentication needle".to_string()),
                ] as &[ParamValue],
            )
            .expect("insert canonical message");
        storage.rebuild_fts().expect("build a valid FTS shadow");
    }

    // Corrupt the %_data structure record (rowid 10) through a fresh
    // connection (the FTS is still valid here, so the open succeeds).
    {
        let conn = FrankenConnection::open(db_path.to_string_lossy().into_owned())
            .expect("open raw connection");
        let garbage = [0xFFu8; 12];
        conn.execute_compat(
            "UPDATE fts_messages_data SET block = ?1 WHERE id = ?2",
            &[
                ParamValue::from(garbage.as_slice()),
                ParamValue::from(10_i64),
            ] as &[ParamValue],
        )
        .expect("corrupt the FTS5 structure record");
    }

    // Querying the corrupt %_data structure record must fail on both the
    // read-only open (dry-run path) and the schema-only repair open (apply
    // path), forcing the doctor command through the corrupt-shadow branch.
    let assert_match_fails = |storage: FrankenStorage, label: &str| {
        let err = storage
            .raw()
            .query("SELECT COUNT(*) FROM fts_messages WHERE fts_messages MATCH 'needle'")
            .expect_err(label);
        assert!(
            is_fts5_shadow_open_corruption_error(&anyhow::Error::new(err)),
            "{label}"
        );
    };
    assert_match_fails(
        FrankenStorage::open_readonly(&db_path).expect("open read-only"),
        "read-only MATCH must fail on a corrupt FTS5 structure record",
    );
    assert_match_fails(
        FrankenStorage::open_existing_schema_only_for_fts_repair(&db_path)
            .expect("open schema-only for repair"),
        "schema-only repair MATCH must fail on a corrupt FTS5 structure record",
    );

    // Dry-run (even with --yes) must report the planned repair while staying
    // strictly read-only: it must NOT open the archive writable, take the
    // doctor mutation lock, or repair anything.
    let bytes_before_dry_run = std::fs::read(&db_path).expect("snapshot db before dry-run");
    run_doctor_rebuild_canonical_fts(
        Some(tmp.path().to_path_buf()),
        Some(db_path.clone()),
        true,
        true,
        Some(RobotFormat::Json),
    )
    .expect("dry-run on a corrupt shadow must succeed read-only");
    let bytes_after_dry_run = std::fs::read(&db_path).expect("snapshot db after dry-run");
    assert_eq!(
        bytes_after_dry_run, bytes_before_dry_run,
        "corrupt-shadow dry-run must not alter any database bytes"
    );
    assert_match_fails(
        FrankenStorage::open_existing_schema_only_for_fts_repair(&db_path)
            .expect("reopen after dry-run"),
        "dry-run must not have repaired the corrupt shadow",
    );

    // Apply: the fallback drops + recreates + rebuilds the shadow from
    // canonical rows.
    run_doctor_rebuild_canonical_fts(
        Some(tmp.path().to_path_buf()),
        Some(db_path.clone()),
        false,
        true,
        Some(RobotFormat::Json),
    )
    .expect("rebuild-canonical-fts must repair a structurally-corrupt shadow (GH #368 defect 3)");

    // The archive now opens normally and the rebuilt shadow reaches exact
    // parity and is queryable for a token from canonical content.
    let readonly = FrankenStorage::open_readonly(&db_path).expect("reopen read-only");
    let parity = readonly
        .inspect_search_fallback_fts_parity()
        .expect("inspect rebuilt FTS");
    assert_eq!(parity.status, FtsShadowParityStatus::Healthy);
    assert_eq!(parity.canonical_messages, 1);
    assert_eq!(parity.indexed_messages, Some(1));
    let matches: i64 = readonly
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM fts_messages WHERE fts_messages MATCH 'needle'",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .expect("MATCH on rebuilt shadow");
    assert_eq!(
        matches, 1,
        "rebuilt FTS shadow must be queryable for 'needle'"
    );
}

/// GH #434 defect 2: the schema-level open refusal reported against real
/// archives (both the 0.6.26 read-path refusal and the doctor
/// `--rebuild-canonical-fts` inspection refusal carried the identical
/// engine string) must route into the deferred-FTS5 drop+recreate repair
/// branch instead of the generic "cannot open" wall, while failures naming
/// canonical (non-shadow) objects keep the generic routing.
#[test]
fn gh434_schema_level_shadow_open_failures_route_to_deferred_repair() {
    // Verbatim shapes from the #434 report (index refusal + doctor refusal).
    let index_refusal = anyhow::anyhow!(
        "opening raw sqlite db readonly at /home/claude/.local/share/coding-agent-search/agent_search.db: \
         database disk image is malformed: sqlite_master is missing implicit autoindex slot 1 for table `fts_messages_config`"
    );
    assert!(is_fts_shadow_schema_level_open_failure(&index_refusal));

    let doctor_refusal = anyhow::anyhow!(
        "opening sqlite db readonly at /tmp/cass-check/agent_search.db: \
         database disk image is malformed: sqlite_master is missing implicit autoindex slot 1 for table `fts_messages_config`"
    );
    assert!(is_fts_shadow_schema_level_open_failure(&doctor_refusal));

    // The same autoindex failure on a CANONICAL table is not repairable by
    // recreating the derived shadow — it must keep the generic routing.
    let canonical_refusal = anyhow::anyhow!(
        "database disk image is malformed: sqlite_master is missing implicit autoindex slot 1 for table `conversations`"
    );
    assert!(!is_fts_shadow_schema_level_open_failure(&canonical_refusal));

    // The %_data structure class keeps its own predicate; neither predicate
    // swallows the other.
    let data_corruption = anyhow::anyhow!(
        "fts5: corrupt %_data record: structure segment count exceeds FTS5 maximum"
    );
    assert!(!is_fts_shadow_schema_level_open_failure(&data_corruption));
    assert!(is_fts5_shadow_open_corruption_error(&data_corruption));

    let structure_corruption =
        anyhow::anyhow!("fts5: corrupt structure record for table \"fts_messages\"");
    assert!(!is_fts_shadow_schema_level_open_failure(
        &structure_corruption
    ));
    assert!(is_fts5_shadow_open_corruption_error(&structure_corruption));

    // The gh#369 oversized-leaf shape is a write-time engine limitation,
    // not an open-blocking schema failure.
    let oversized =
        anyhow::anyhow!("fts5: corrupt %_data record: segment leaf term offset exceeds u16");
    assert!(!is_fts_shadow_schema_level_open_failure(&oversized));
}

/// GH #503: pre-v1 archives without the schema-v1 stamp are refused with
/// `SchemaRebuildRequired`, while a schema-v1 archive opens under
/// `open_deferred_fts5_for_repair` matching `ENGINE_REPAIRS_MISSING_FTS5_SHADOW_AUTOINDEX`,
/// and the string predicates keep classifying missing-autoindex shadow failures.
#[test]
fn pinned_engine_deferred_open_matches_the_legacy_shadow_catalog_policy() {
    use crate::storage::sqlite::SchemaRebuildRequired;

    let dir = tempfile::TempDir::new().unwrap();
    let legacy_db = dir.path().join("legacy_agent_search.db");
    std::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/gh503/legacy_fts_config_shadow.db"),
        &legacy_db,
    )
    .unwrap();

    let ordinary = FrankenStorage::open_readonly(&legacy_db)
        .err()
        .expect("unstamped pre-v1 fixture must be refused by schema-v1 open");
    assert!(
        ordinary.downcast_ref::<SchemaRebuildRequired>().is_some(),
        "expected SchemaRebuildRequired on pre-v1 fixture: {ordinary:#}"
    );
    let legacy_repair = FrankenStorage::open_deferred_fts5_for_repair(&legacy_db)
        .err()
        .expect("unstamped pre-v1 fixture must also be refused by deferred repair open");
    assert!(
        legacy_repair
            .downcast_ref::<SchemaRebuildRequired>()
            .is_some(),
        "expected SchemaRebuildRequired on deferred repair open: {legacy_repair:#}"
    );

    let v1_db = dir.path().join("agent_search.db");
    drop(FrankenStorage::open(&v1_db).expect("bootstrap schema-v1 db"));
    let repair_open = FrankenStorage::open_deferred_fts5_for_repair(&v1_db);
    assert_eq!(
        repair_open.is_ok(),
        ENGINE_REPAIRS_MISSING_FTS5_SHADOW_AUTOINDEX,
        "schema-v1 deferred repair open disagrees with policy constant: {:?}",
        repair_open.as_ref().err()
    );

    let shadow_missing = anyhow::anyhow!(
        "database disk image is malformed: sqlite_master is missing implicit autoindex slot 1 for table `fts_messages_config`"
    );
    assert!(is_missing_fts5_shadow_autoindex_failure(&shadow_missing));

    // The predicate names only the shadow shape.
    let canonical = anyhow::anyhow!(
        "database disk image is malformed: sqlite_master is missing implicit autoindex slot 1 for table `conversations`"
    );
    assert!(!is_missing_fts5_shadow_autoindex_failure(&canonical));
}

/// GH #369: the cumulative oversized-leaf failure (many in-cap terms in one
/// batch, not a single overlong token) must be recognized so the operator
/// gets a reassuring "search still works via Tantivy" diagnostic rather than
/// the generic storage wall. This mirrors the exact wrapped chain the
/// failure-atomic rebuild produces (`sqlite.rs` `.context(...)`), with the
/// FTS5 root string preserved.
#[test]
fn oversized_leaf_error_is_classified_and_gets_reassuring_diagnostic() {
    let wrapped = anyhow::anyhow!(
        "inserting 4000 rows into fts_messages during streaming FTS maintenance: \
         fts5: corrupt %_data record: segment leaf term offset exceeds u16"
    )
    .context("failure-atomic FTS rebuild rolled back without publishing a partial shadow");
    assert!(
        is_fts5_oversized_leaf_error(&wrapped),
        "the real wrapped chain must be recognized as the GH #369 oversized-leaf case"
    );

    // Each sibling leaf/footer overflow signature is also covered.
    for signature in [
        "segment leaf rowid offset exceeds u16",
        "segment leaf footer offset exceeds u16",
        "segment footer offset exceeds u16",
    ] {
        assert!(
            is_fts5_oversized_leaf_error(&anyhow::anyhow!(signature.to_string())),
            "signature must be recognized: {signature}"
        );
    }

    // Unrelated storage failures must NOT be misclassified — they still get
    // the generic storage wall + bundle-preservation hint.
    for unrelated in [
        "database is locked",
        "no such table: fts_messages",
        "disk I/O error while reading page 42",
        "segment terms must be strictly increasing",
    ] {
        assert!(
            !is_fts5_oversized_leaf_error(&anyhow::anyhow!(unrelated.to_string())),
            "unrelated error must not be misclassified: {unrelated}"
        );
    }

    let diagnostic = fts5_oversized_leaf_shadow_error(Path::new("/tmp/agent_search.db"));
    assert_eq!(diagnostic.kind, "fts5-oversized-leaf-shadow-unbuildable");
    assert!(!diagnostic.retryable);
    assert!(
        diagnostic
            .message
            .contains("not corruption of your archive"),
        "message must reassure the operator their data is intact"
    );
    let hint = diagnostic
        .hint
        .expect("oversized-leaf diagnostic carries a hint");
    assert!(
        hint.contains("Tantivy") && hint.contains("No action is needed"),
        "hint must state search still works and no action is needed"
    );
}
#[test]
fn recovery_listing_tolerates_type_mismatched_rows() {
    // #391: a page-aliasing corruption leaves integers where TEXT is
    // declared. The strict lister failed the whole page with
    // `type mismatch: expected text, got integer`; the recovery lister
    // must coerce, report the coercion, and keep going.
    let tmp = tempfile::tempdir().expect("tempdir");
    let db_path = tmp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).expect("open db");
    let agent_id = seed_agent(&storage);
    let healthy = seed_conversation(&storage, agent_id, "sess-ok", "/orig/ok.jsonl");
    let damaged = seed_conversation(&storage, agent_id, "sess-bad", "/orig/bad.jsonl");
    // TEXT affinity rewrites an integer to text on INSERT/UPDATE, so the
    // mismatch (what stock SQLite's integrity_check reports as "NUMERIC
    // value in conversations.title") cannot be planted through SQL.
    // Exercise the mapper directly with a SELECT shaped exactly like the
    // listing projection, presenting the values a damaged page would.
    let rows: Vec<RecoveryConversationRow> = storage
        .raw()
        .query_map_collect(
            "SELECT ?1, 'claude', NULL, 'sess-bad', 7, ?2, 1000, 'not-a-timestamp', 1.5,
                    NULL, 'local', NULL, NULL",
            &[
                ParamValue::from(damaged),
                ParamValue::from("/orig/bad.jsonl"),
            ] as &[ParamValue],
            |row| Ok(crate::storage::sqlite::recovery_conversation_row_from_lenient_columns(row)),
        )
        .expect("query");
    assert_eq!(rows.len(), 1);
    match &rows[0] {
        RecoveryConversationRow::Readable {
            conversation,
            coercions,
        } => {
            assert_eq!(conversation.id, Some(damaged));
            assert_eq!(conversation.external_id.as_deref(), Some("sess-bad"));
            assert_eq!(conversation.title.as_deref(), Some("7"));
            assert_eq!(conversation.started_at, Some(1000));
            assert_eq!(conversation.ended_at, None, "unparseable text is dropped");
            assert_eq!(conversation.approx_tokens, Some(1), "real truncates");
            assert_eq!(coercions.len(), 3, "{coercions:?}");
            assert!(coercions[0].starts_with("title: integer 7"));
            assert!(coercions[1].contains("ended_at: text \"not-a-timestamp\""));
            assert!(coercions[1].ends_with("(dropped)"));
            assert!(coercions[2].starts_with("approx_tokens: real 1.5"));
        }
        other => panic!("expected a readable row, got {other:?}"),
    }

    // An id that is not an integer is reported, not fatal.
    let rows: Vec<RecoveryConversationRow> = storage
        .raw()
        .query_map_collect(
            "SELECT 'garbage', 'claude', NULL, NULL, NULL, '/p', NULL, NULL, NULL,
                    NULL, 'local', NULL, NULL",
            &[] as &[ParamValue],
            |row| Ok(crate::storage::sqlite::recovery_conversation_row_from_lenient_columns(row)),
        )
        .expect("query");
    match &rows[0] {
        RecoveryConversationRow::Unreadable { stored_id, reason } => {
            assert_eq!(stored_id, "text");
            assert!(reason.contains("not an integer"));
        }
        other => panic!("expected an unreadable row, got {other:?}"),
    }

    // The real listing pages by id and reads healthy rows unchanged.
    let page = storage
        .list_conversations_for_recovery(0, 1)
        .expect("first page");
    assert_eq!(page.len(), 1);
    let RecoveryConversationRow::Readable {
        conversation,
        coercions,
    } = &page[0]
    else {
        panic!("healthy row must be readable");
    };
    assert_eq!(conversation.id, Some(healthy));
    assert_eq!(conversation.external_id.as_deref(), Some("sess-ok"));
    assert!(coercions.is_empty());
    let page = storage
        .list_conversations_for_recovery(healthy, 10)
        .expect("second page");
    assert_eq!(page.len(), 1);
    let RecoveryConversationRow::Readable { conversation, .. } = &page[0] else {
        panic!("second row must be readable");
    };
    assert_eq!(conversation.id, Some(damaged));
    assert!(
        storage
            .list_conversations_for_recovery(damaged, 10)
            .expect("past the end")
            .is_empty()
    );
}

/// Plant a BLOB in a declared-TEXT column. TEXT affinity converts numerics
/// on write but stores a blob as-is, so this is the one mis-typed value
/// that can be planted through SQL (the shape stock SQLite reports as
/// "NUMERIC value in conversations.title" otherwise needs a damaged page).
fn plant_blob(storage: &FrankenStorage, conversation_id: i64, column: &str) {
    storage
        .raw()
        .execute_compat(
            &format!("UPDATE conversations SET {column} = X'DEADBEEF' WHERE id = ?1"),
            &[ParamValue::from(conversation_id)] as &[ParamValue],
        )
        .expect("plant blob");
}

#[test]
fn recovery_listing_quarantines_rows_with_mistyped_identity_columns() {
    // #391: an aliased page decodes a foreign cell through the
    // `conversations` schema, so an identity column (`source_path`,
    // `external_id`, ...) holds a non-text value. Such a row is not a
    // conversation with one damaged cell — its `id` would address some
    // other conversation's messages — so it is quarantined, while a row
    // whose only coercions are title/timestamps still exports.
    let tmp = tempfile::tempdir().expect("tempdir");
    let db_path = tmp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).expect("open db");
    let agent_id = seed_agent(&storage);
    let healthy = seed_conversation(&storage, agent_id, "sess-ok", "/orig/ok.jsonl");
    let coerced = seed_conversation(&storage, agent_id, "sess-title", "/orig/title.jsonl");
    let aliased = seed_conversation(&storage, agent_id, "sess-alias", "/orig/alias.jsonl");
    plant_blob(&storage, coerced, "title");
    plant_blob(&storage, aliased, "source_path");

    let page = storage
        .list_conversations_for_recovery(0, 10)
        .expect("page");
    assert_eq!(page.len(), 3, "{page:?}");
    let RecoveryConversationRow::Readable {
        conversation,
        coercions,
    } = &page[0]
    else {
        panic!("healthy row must be readable: {:?}", page[0]);
    };
    assert_eq!(conversation.id, Some(healthy));
    assert!(coercions.is_empty());
    let RecoveryConversationRow::Readable {
        conversation,
        coercions,
    } = &page[1]
    else {
        panic!(
            "a row with only a coerced title must stay readable: {:?}",
            page[1]
        );
    };
    assert_eq!(conversation.id, Some(coerced));
    assert_eq!(conversation.source_path, Path::new("/orig/title.jsonl"));
    assert_eq!(coercions.len(), 1, "{coercions:?}");
    assert!(coercions[0].starts_with("title: 4-byte blob"));
    let RecoveryConversationRow::Quarantined { id, coercions } = &page[2] else {
        panic!(
            "a row with a mis-typed source_path must be quarantined: {:?}",
            page[2]
        );
    };
    assert_eq!(*id, aliased);
    assert_eq!(coercions.len(), 1, "{coercions:?}");
    assert!(coercions[0].starts_with("source_path: 4-byte blob"));

    // Paging by id continues past a quarantined row.
    let rest = storage
        .list_conversations_for_recovery(aliased, 10)
        .expect("past the end");
    assert!(rest.is_empty(), "{rest:?}");

    // The other shapes an aliased page presents, exercised on the mapper
    // with the listing projection: an integer or NULL where `source_path`
    // is declared, an integer `external_id`, an integer `source_id`.
    let mapper = |row: &crate::franken_sync::Row| {
        Ok(crate::storage::sqlite::recovery_conversation_row_from_lenient_columns(row))
    };
    for (sql, expect) in [
        (
            "SELECT ?1, 'claude', NULL, 'sess-x', 7, 1700000000, 1000, NULL, NULL,
                    NULL, 'local', NULL, NULL",
            "source_path: integer 1700000000",
        ),
        (
            "SELECT ?1, 'claude', NULL, 'sess-x', NULL, NULL, 1000, NULL, NULL,
                    NULL, 'local', NULL, NULL",
            "source_path: NULL or empty",
        ),
        (
            "SELECT ?1, 'claude', NULL, 4242, NULL, '/p', 1000, NULL, NULL,
                    NULL, 'local', NULL, NULL",
            "external_id: integer 4242",
        ),
        (
            "SELECT ?1, 'claude', NULL, 'sess-x', NULL, '/p', 1000, NULL, NULL,
                    NULL, 99, NULL, NULL",
            "source_id: integer 99",
        ),
    ] {
        let rows: Vec<RecoveryConversationRow> = storage
            .raw()
            .query_map_collect(sql, &[ParamValue::from(aliased)] as &[ParamValue], mapper)
            .expect("query");
        match &rows[0] {
            RecoveryConversationRow::Quarantined { id, coercions } => {
                assert_eq!(*id, aliased);
                assert!(coercions[0].starts_with(expect), "{sql}: {coercions:?}");
            }
            other => panic!("{sql}: expected a quarantined row, got {other:?}"),
        }
    }
    // Identity coercions are listed before content coercions on the same row.
    let rows: Vec<RecoveryConversationRow> = storage
        .raw()
        .query_map_collect(
            "SELECT ?1, 'claude', NULL, 'sess-x', 7, 1700000000, 1000, NULL, NULL,
                    NULL, 'local', NULL, NULL",
            &[ParamValue::from(aliased)] as &[ParamValue],
            mapper,
        )
        .expect("query");
    let RecoveryConversationRow::Quarantined { coercions, .. } = &rows[0] else {
        panic!("expected a quarantined row, got {:?}", rows[0]);
    };
    assert_eq!(coercions.len(), 2, "{coercions:?}");
    assert!(coercions[0].starts_with("source_path: integer 1700000000"));
    assert!(coercions[1].starts_with("title: integer 7"));
}

#[test]
fn recover_from_archive_quarantines_mistyped_rows_and_exports_coerced_ones() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let db_path = tmp.path().join("agent_search.db");
    let target = tmp.path().join("recovered");
    {
        let storage = FrankenStorage::open(&db_path).expect("open db");
        let agent_id = seed_agent(&storage);
        let healthy = seed_conversation(&storage, agent_id, "sess-ok", "/orig/ok.jsonl");
        let coerced = seed_conversation(&storage, agent_id, "sess-title", "/orig/title.jsonl");
        let aliased = seed_conversation(&storage, agent_id, "sess-alias", "/orig/alias.jsonl");
        for cid in [healthy, coerced, aliased] {
            write_message(
                &storage,
                cid,
                0,
                &format!(r#"{{"type":"user","uuid":"u{cid}","text":"hi"}}"#),
            );
        }
        plant_blob(&storage, coerced, "title");
        plant_blob(&storage, aliased, "source_path");
    }

    run_doctor_recover_from_archive(
        Some(tmp.path().to_path_buf()),
        Some(db_path.clone()),
        target.clone(),
        Some(RobotFormat::Json),
    )
    .expect("recover");

    let mut written: Vec<String> = std::fs::read_dir(&target)
        .expect("read recovered dir")
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".jsonl"))
        .collect();
    written.sort();
    assert_eq!(written.len(), 2, "{written:?}");
    assert!(
        written.iter().any(|n| n.starts_with("sess-ok")),
        "{written:?}"
    );
    assert!(
        written.iter().any(|n| n.starts_with("sess-title")),
        "a row with only a coerced title still exports: {written:?}"
    );
    assert!(
        !written.iter().any(|n| n.starts_with("sess-alias")),
        "a quarantined row must not be exported: {written:?}"
    );
}

fn rows(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

#[test]
fn leaked_pages_classifier_accepts_only_a_lone_engine_leak_row() {
    assert_eq!(
        classify_leaked_pages(&rows(&["ok"]), &rows(&["ok"])),
        LeakedPagesVerdict::Clean
    );
    // The engine's informational notes are not failures.
    assert_eq!(
        classify_leaked_pages(
            &rows(&["ok"]),
            &rows(&[
                "ok",
                "note: orphaned FTS5 contentless content shadow table x"
            ])
        ),
        LeakedPagesVerdict::Clean
    );
    for leak in [
        "database disk image is malformed: page 8196 is never used",
        "page 8196 is never used",
        "*** in database main ***\nPage 8196: never used",
        "*** in database main ***\npage 8196: never used",
    ] {
        assert_eq!(
            classify_leaked_pages(&rows(&["ok"]), &rows(&[leak])),
            LeakedPagesVerdict::LeakedPagesOnly {
                first_leaked_page: 8196
            },
            "{leak}"
        );
    }
    // C SQLite reports leaked pages in both quick_check(1) and multi-line
    // integrity_check(8) output.
    assert_eq!(
        classify_leaked_pages(
            &rows(&["*** in database main ***\nPage 3: never used"]),
            &rows(&["*** in database main ***\nPage 3: never used\nPage 4: never used\nPage 5: never used"]),
        ),
        LeakedPagesVerdict::LeakedPagesOnly {
            first_leaked_page: 3
        }
    );
    // `integrity_is_leaked_pages_only` receives quick_check output with an
    // empty integrity_check list when doctor_taxonomy skips integrity_check.
    assert_eq!(
        classify_leaked_pages(&rows(&["*** in database main ***\nPage 7: never used"]), &[]),
        LeakedPagesVerdict::LeakedPagesOnly {
            first_leaked_page: 7
        }
    );
}

#[test]
fn leaked_pages_classifier_refuses_every_other_failure() {
    for (quick, full) in [
        // A doubled reference fails the ownership walk before any orphan
        // scan, so it never reads as a leak.
        (
            vec!["ok"],
            vec!["database disk image is malformed: page 7 is referenced multiple times"],
        ),
        // quick_check itself failing is structural damage.
        (
            vec!["database disk image is malformed: btree page 3 is malformed"],
            vec![],
        ),
        // A non-leak failure alongside leaked pages is other damage.
        (
            vec!["ok"],
            vec![
                "database disk image is malformed: page 9 is never used",
                "*** in database aux ***\nindex mismatch",
            ],
        ),
        (
            vec!["*** in database main ***\nPage 3: never used"],
            vec![
                "*** in database main ***\nPage 3: never used",
                "wrong # of entries in index idx_messages_conv_idx",
            ],
        ),
        // A bare database header without diagnostic lines is never Clean.
        (vec!["*** in database main ***"], vec!["ok"]),
        // Near misses of the leak text.
        (vec!["ok"], vec!["page 9 is never used again"]),
        (vec!["ok"], vec!["page -9 is never used"]),
        (vec!["ok"], vec!["page  is never used"]),
        (
            vec!["ok"],
            vec!["Page 9 is never used; page 10 is never used"],
        ),
        (
            vec!["ok"],
            vec!["row 4 missing from index idx_messages_created"],
        ),
    ] {
        let verdict = classify_leaked_pages(&rows(&quick), &rows(&full));
        assert!(
            matches!(verdict, LeakedPagesVerdict::OtherDamage { .. }),
            "quick={quick:?} full={full:?} => {verdict:?}"
        );
    }
}

#[test]
fn leaked_pages_backup_takes_shm_only_beside_a_wal() {
    let temp = tempfile::tempdir().expect("tempdir");
    let db = temp.path().join("agent_search.db");
    std::fs::write(&db, b"db").expect("db");
    let suffixes = || -> Vec<&'static str> {
        leaked_pages_bundle_components(&db)
            .into_iter()
            .map(|(suffix, _)| suffix)
            .collect()
    };
    assert_eq!(suffixes(), vec![""]);
    std::fs::write(temp.path().join("agent_search.db-shm"), b"shm").expect("shm");
    assert_eq!(
        suffixes(),
        vec![""],
        "an SHM without its WAL is not backed up"
    );
    std::fs::write(temp.path().join("agent_search.db-wal"), b"wal").expect("wal");
    assert_eq!(suffixes(), vec!["", "-wal", "-shm"]);
    std::fs::remove_file(temp.path().join("agent_search.db-shm")).expect("rm shm");
    assert_eq!(suffixes(), vec!["", "-wal"]);
}

#[test]
fn leaked_pages_guidance_predicates_match_the_classifier() {
    assert!(integrity_is_leaked_pages_only(
        "ok",
        &rows(&["database disk image is malformed: page 12 is never used"])
    ));
    assert!(integrity_is_leaked_pages_only(
        "*** in database main ***\nPage 12: never used",
        &[]
    ));
    assert!(!integrity_is_leaked_pages_only(
        "ok",
        &rows(&["database disk image is malformed: page 12 is referenced multiple times"])
    ));
    assert!(!integrity_is_leaked_pages_only("ok", &[]));
    assert!(attested_integrity_is_leaked_pages_only(
        "integrity_check",
        Some("database disk image is malformed: page 12 is never used")
    ));
    assert!(attested_integrity_is_leaked_pages_only(
        "integrity_check",
        Some("*** in database main ***\nPage 12: never used\nPage 13: never used")
    ));
    assert!(attested_integrity_is_leaked_pages_only(
        "quick_check",
        Some("*** in database main ***\nPage 12: never used")
    ));
    assert!(!attested_integrity_is_leaked_pages_only(
        "quick_check",
        Some("wrong # of entries in index idx_messages_conv_idx")
    ));
    assert!(!attested_integrity_is_leaked_pages_only(
        "other_check",
        Some("*** in database main ***\nPage 12: never used")
    ));
    assert!(!attested_integrity_is_leaked_pages_only(
        "integrity_check",
        None
    ));
}

#[test]
fn repair_leaked_pages_reclaims_detached_freelist_via_vacuum() {
    use std::io::{Read, Seek, SeekFrom, Write};

    let tmp = tempfile::tempdir().expect("tempdir");
    let data_dir = tmp.path().to_path_buf();
    let db_path = data_dir.join("agent_search.db");
    let page_count_before = {
        let storage = FrankenStorage::open(&db_path).expect("open db");
        let agent_id = seed_agent(&storage);
        let cid = seed_conversation(&storage, agent_id, "leak-1", "/orig/leak.jsonl");
        write_message(
            &storage,
            cid,
            0,
            r#"{"type":"user","uuid":"u1","text":"hi"}"#,
        );
        write_message(
            &storage,
            cid,
            1,
            r#"{"type":"assistant","uuid":"a1","text":"yo"}"#,
        );
        storage
            .raw()
            .execute("CREATE TABLE leak_probe(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
            .expect("create leak_probe");
        for id in 1_i64..=24 {
            let payload = format!("{id:04}{}", "leak probe payload ".repeat(180));
            storage
                .raw()
                .execute_compat(
                    "INSERT INTO leak_probe(id, payload) VALUES(?1, ?2)",
                    &[ParamValue::from(id), ParamValue::from(payload)] as &[ParamValue],
                )
                .expect("insert leak_probe");
        }
        storage
            .raw()
            .execute("DELETE FROM leak_probe")
            .expect("delete leak_probe");
        storage
            .raw()
            .query("PRAGMA wal_checkpoint(TRUNCATE);")
            .expect("checkpoint");
        storage
            .raw()
            .query_row_map("PRAGMA page_count;", &[] as &[ParamValue], |row| {
                row.get_typed::<i64>(0)
            })
            .expect("page_count")
    };

    // Detach the durable freelist trunk + count in the 100-byte SQLite header
    // (bytes 32..40) so those formerly free pages become unowned ("never used").
    {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .expect("open db bytes");
        let mut header = [0u8; 8];
        file.seek(SeekFrom::Start(32)).expect("seek freelist header");
        file.read_exact(&mut header).expect("read freelist header");
        let trunk = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        let count = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);
        assert!(
            trunk > 1 && count >= 8,
            "fixture needs durable freelist pages: trunk={trunk} count={count}"
        );
        file.seek(SeekFrom::Start(32)).expect("seek freelist header");
        file.write_all(&[0u8; 8]).expect("detach freelist");
        file.sync_all().expect("sync header");
    }

    // Dry-run classifies the leaked pages without altering the DB.
    run_doctor_repair_leaked_pages(
        Some(data_dir.clone()),
        Some(db_path.clone()),
        true,
        false,
        Some(RobotFormat::Json),
    )
    .expect("dry-run repair_leaked_pages");

    // Apply runs VACUUM; behind a backup and re-verifies integrity_check.
    run_doctor_repair_leaked_pages(
        Some(data_dir.clone()),
        Some(db_path.clone()),
        false,
        true,
        Some(RobotFormat::Json),
    )
    .expect("apply repair_leaked_pages via VACUUM");

    let readonly = FrankenStorage::open_readonly(&db_path).expect("reopen read-only");
    let integrity: Vec<String> = readonly
        .raw()
        .query_map_collect("PRAGMA integrity_check(8);", &[] as &[ParamValue], |row| {
            row.get_typed(0)
        })
        .expect("integrity_check after repair");
    assert_eq!(integrity, vec!["ok".to_string()]);
    let freelist_after: i64 = readonly
        .raw()
        .query_row_map("PRAGMA freelist_count;", &[] as &[ParamValue], |row| {
            row.get_typed(0)
        })
        .expect("freelist_count after repair");
    assert_eq!(freelist_after, 0);
    let page_count_after: i64 = readonly
        .raw()
        .query_row_map("PRAGMA page_count;", &[] as &[ParamValue], |row| {
            row.get_typed(0)
        })
        .expect("page_count after repair");
    assert!(
        page_count_after < page_count_before,
        "VACUUM must shrink page_count ({page_count_before} -> {page_count_after})"
    );
    let convs: i64 = readonly
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM conversations;",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .expect("conversations count");
    let msgs: i64 = readonly
        .raw()
        .query_row_map("SELECT COUNT(*) FROM messages;", &[] as &[ParamValue], |row| {
            row.get_typed(0)
        })
        .expect("messages count");
    assert_eq!(convs, 1);
    assert_eq!(msgs, 2);
}
