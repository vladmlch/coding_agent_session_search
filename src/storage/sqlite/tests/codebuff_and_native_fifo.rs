use super::*;
use crate::franken_sync::compat::{
    ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt,
};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use serial_test::serial;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// mt90z: a Codebuff replay carries the workspace from its run-state
/// sidecar. A changed one replaces the canonical workspace and moves the
/// chat's analytics with it; a replay without one keeps what is known.
#[test]
#[serial]
fn codebuff_run_state_workspace_replaces_the_canonical_workspace_on_replay() {
    let _lexical = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "0");
    let before = PathBuf::from("/synthetic/probe");
    let after = PathBuf::from("/metadata/watch-updated");
    let probe = serde_json::json!({});
    assert!(provider_workspace_is_authoritative(
        "codebuff",
        Some(&after),
        &probe
    ));
    assert!(!provider_workspace_is_authoritative(
        "codebuff", None, &probe
    ));
    assert!(!provider_workspace_is_authoritative(
        "codebuff",
        Some(Path::new("")),
        &probe
    ));
    assert!(!provider_workspace_is_authoritative(
        "claude_code",
        Some(&after),
        &probe
    ));
    for batched in [false, true] {
        let dir = TempDir::new().unwrap();
        let storage = FrankenStorage::open(&dir.path().join("codebuff-workspace.db")).unwrap();
        let agent = gh423_agent(&storage);
        let before_id = storage.ensure_workspace(&before, None).unwrap();
        let after_id = storage.ensure_workspace(&after, None).unwrap();
        let persist = |conv: &Conversation, workspace_id: Option<i64>| {
            if batched {
                storage
                    .insert_conversations_batched_with_analytics(
                        &[(agent, workspace_id, conv)],
                        false,
                    )
                    .unwrap()
                    .pop()
                    .unwrap()
            } else {
                storage
                    .insert_conversation_tree_with_analytics(agent, workspace_id, conv, false)
                    .unwrap()
            }
        };
        let attribution = || {
            let rows = storage.list_conversations(10, 0).unwrap();
            assert_eq!(rows.len(), 1);
            let analytics: Vec<i64> = storage
                .raw()
                .query_map_collect(
                    "SELECT workspace_id FROM message_metrics
                     UNION SELECT IFNULL(workspace_id, 0) FROM token_usage",
                    fparams![],
                    |row| row.get_typed(0),
                )
                .unwrap();
            (rows[0].workspace.clone(), analytics)
        };
        let mut conv = gh423_storage_snapshot();
        conv.workspace = Some(before.clone());
        let original = persist(&conv, Some(before_id));
        let messages =
            serde_json::to_value(storage.fetch_messages(original.conversation_id).unwrap())
                .unwrap();
        assert_eq!(attribution(), (Some(before.clone()), vec![before_id]));
        let (external_id, metadata) = (conv.external_id.clone(), conv.metadata_json.clone());
        let own = conv.source_path.clone();
        // The same chat reached through another store shares its lineage.
        let alias = PathBuf::from(
            "/profile/.config/freebuff/projects/project/chats/native-chat/chat-messages.json",
        );
        let repair_needed = |source_path: &Path, workspace: Option<&Path>| {
            storage
                .provider_workspace_repair_needed(
                    "codebuff",
                    "local",
                    external_id.as_deref(),
                    source_path,
                    workspace,
                    &metadata,
                )
                .unwrap()
        };
        assert!(
            !repair_needed(&own, Some(&before)),
            "an unchanged replay needs no repair"
        );
        assert!(repair_needed(&own, Some(&after)));
        assert!(
            !repair_needed(&own, None),
            "an absent workspace is not a change"
        );
        assert!(
            !repair_needed(&alias, Some(&after)),
            "an alias source does not own the workspace"
        );
        assert!(!persist(&conv, Some(before_id)).workspace_changed);
        let mut aliased = conv.clone();
        aliased.source_path = alias.clone();
        aliased.workspace = Some(after.clone());
        let alias_replay = persist(&aliased, Some(after_id));
        assert_eq!(alias_replay.conversation_id, original.conversation_id);
        assert!(!alias_replay.workspace_changed);
        assert_eq!(attribution(), (Some(before.clone()), vec![before_id]));
        let generation_before = storage
            .semantic_identity_rebuild_generation(SemanticIdentityTier::Fast)
            .unwrap();

        conv.workspace = Some(after.clone());
        let changed = persist(&conv, Some(after_id));
        assert_eq!(changed.conversation_id, original.conversation_id);
        assert!(changed.workspace_changed);
        assert!(!changed.conversation_inserted);
        assert!(changed.inserted_indices.is_empty());
        assert!(changed.updated_indices.is_empty());
        assert_eq!(attribution(), (Some(after.clone()), vec![after_id]));
        let generation_after = storage
            .semantic_identity_rebuild_generation(SemanticIdentityTier::Fast)
            .unwrap();
        assert!(generation_after.is_some());
        assert_ne!(
            generation_after, generation_before,
            "vector doc IDs embed the workspace"
        );
        assert!(!repair_needed(&own, Some(&after)));

        conv.workspace = None;
        let absent = persist(&conv, None);
        assert_eq!(absent.conversation_id, original.conversation_id);
        assert!(!absent.workspace_changed);
        assert_eq!(attribution(), (Some(after.clone()), vec![after_id]));
        assert_eq!(
            serde_json::to_value(storage.fetch_messages(original.conversation_id).unwrap())
                .unwrap(),
            messages
        );
    }
}

/// `--watch-once` scans the canonical path, so a chat first indexed
/// through a symlinked home (macOS `/var`) is replayed under another
/// spelling of the same file. That replay still owns the workspace; a
/// different file with the same layout does not.
#[cfg(unix)]
#[test]
fn codebuff_replay_owns_the_workspace_through_another_spelling_of_its_file() {
    let dir = TempDir::new().unwrap();
    let chat = |root: &Path| {
        root.join("projects")
            .join("project")
            .join("chats")
            .join("native-chat")
            .join("chat-messages.json")
    };
    let real = dir.path().join("real");
    let other = dir.path().join("other");
    for root in [&real, &other] {
        std::fs::create_dir_all(chat(root).parent().unwrap()).unwrap();
        std::fs::write(chat(root), b"[]").unwrap();
    }
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    let canonical = path_to_string(chat(&link));
    let owns = |path: &Path| replay_owns_provider_workspace("codebuff", Some(&canonical), path);

    assert!(owns(&chat(&link)));
    assert!(owns(&chat(&real)), "the same file through its real path");
    assert!(!owns(&chat(&other)), "another store's file is an alias");
    assert!(
        !owns(&chat(&dir.path().join("absent"))),
        "a path that does not exist compares by spelling only"
    );
    assert!(!replay_owns_provider_workspace(
        "codebuff",
        None,
        &chat(&real)
    ));
    assert!(replay_owns_provider_workspace(
        "cursor",
        Some(&canonical),
        &chat(&other)
    ));
}

#[test]
#[serial]
fn gh423_codebuff_conflicting_native_identity_rolls_back_every_projection() {
    let _lexical = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "0");
    for defect in 0..5 {
        let dir = TempDir::new().unwrap();
        let storage = FrankenStorage::open(&dir.path().join("conflict.db")).unwrap();
        storage.ensure_search_fallback_fts_consistency().unwrap();
        let agent = gh423_agent(&storage);
        let original = gh423_storage_snapshot();
        let inserted = gh423_persist(&storage, agent, &original, 0).unwrap();
        let before = storage.fetch_messages(inserted.conversation_id).unwrap();
        let analytics = gh423_analytics_snapshot(&storage);
        let mut revision = original.clone();
        gh423_revise(&mut revision);
        let mut invalid = revision.clone();
        match defect {
            0 => invalid.messages[1].role = MessageRole::User,
            1 => invalid.messages[1].created_at = Some(1),
            2 => invalid.messages[1].author = Some("different identity".into()),
            3 => invalid.messages.push(invalid.messages[1].clone()),
            _ => invalid.messages[1].extra_json["codebuff_message_id"] = serde_json::json!("wrong"),
        }
        assert!(
            storage
                .insert_conversations_batched_with_analytics(
                    &[(agent, None, &revision), (agent, None, &invalid)],
                    false
                )
                .is_err()
        );
        assert_eq!(
            serde_json::to_value(storage.fetch_messages(inserted.conversation_id).unwrap())
                .unwrap(),
            serde_json::to_value(before).unwrap()
        );
        assert_eq!(gh423_analytics_snapshot(&storage), analytics);
        assert!(
            !storage
                .semantic_identity_rebuild_required(SemanticIdentityTier::Fast)
                .unwrap()
        );
        let snippets: Vec<String> = storage
            .raw()
            .query_map_collect(
                "SELECT snippet_text FROM snippets ORDER BY id",
                fparams![],
                |row| row.get_typed(0),
            )
            .unwrap();
        assert_eq!(snippets, vec!["old snippet", "old snippet", "old snippet"]);
        let matches: Vec<i64> = storage
            .raw()
            .query_map_collect(
                "SELECT rowid FROM fts_messages WHERE fts_messages MATCH 'newneedle'",
                fparams![],
                |row| row.get_typed(0),
            )
            .unwrap();
        assert!(matches.is_empty());
    }
}

#[test]
#[serial]
fn gh423_codebuff_revision_preserves_canonical_fts_metadata() {
    let _lexical = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "0");
    let dir = TempDir::new().unwrap();
    let storage = FrankenStorage::open(&dir.path().join("canonical-fts-context.db")).unwrap();
    storage.ensure_search_fallback_fts_consistency().unwrap();
    let agent = gh423_agent(&storage);
    let mut conv = gh423_storage_snapshot();
    conv.title = Some("storedtitle".into());
    conv.source_path = "/storedsource/chat.json".into();
    conv.workspace = Some("/storedworkspace".into());
    let workspace = storage
        .ensure_workspace(conv.workspace.as_deref().unwrap(), None)
        .unwrap();
    let first = storage
        .insert_conversation_tree(agent, Some(workspace), &conv)
        .unwrap();
    gh423_revise(&mut conv);
    conv.title = Some("changedtitle".into());
    conv.source_path = "/aliassource/chat.json".into();
    conv.workspace = Some("/changedworkspace".into());
    let mut tail = conv.messages[0].clone();
    tail.content = "tailneedle".into();
    tail.extra_json["id"] = serde_json::json!("native-tail");
    tail.extra_json["codebuff_message_id"] = serde_json::json!("native-tail");
    conv.messages.push(tail);
    let alias_workspace = storage
        .ensure_workspace(conv.workspace.as_deref().unwrap(), None)
        .unwrap();
    storage
        .insert_conversation_tree(agent, Some(alias_workspace), &conv)
        .unwrap();
    let canonical: (String, String, i64) = storage
        .raw()
        .query_row_map(
            "SELECT source_path, title, workspace_id FROM conversations WHERE id = ?1",
            fparams![first.conversation_id],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
        )
        .unwrap();
    assert_eq!(
        canonical,
        (
            "/storedsource/chat.json".into(),
            "storedtitle".into(),
            workspace
        )
    );
    for table in ["message_metrics", "token_usage"] {
        let canonical_count: i64 = storage
            .raw()
            .query_row_map(
                &format!("SELECT COUNT(*) FROM {table} WHERE workspace_id = ?1"),
                fparams![workspace],
                |row| row.get_typed(0),
            )
            .unwrap();
        assert_eq!(
            canonical_count, 4,
            "new tails retain the canonical workspace in {table}"
        );
    }
    for (term, expected) in [
        ("title:storedtitle", 4),
        ("title:changedtitle", 0),
        ("oldneedle", 2),
        ("newneedle", 1),
        ("tailneedle", 1),
    ] {
        let count: i64 = storage
            .raw()
            .query_row_map(
                "SELECT COUNT(*) FROM fts_messages WHERE fts_messages MATCH ?1",
                fparams![term],
                |row| row.get_typed(0),
            )
            .unwrap();
        assert_eq!(count, expected, "{term}");
    }
}

#[test]
#[serial]
fn gh423_codebuff_partial_analytics_revisions_preserve_independent_projections() {
    let _lexical = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "0");
    for mode in 0..3 {
        let dir = TempDir::new().unwrap();
        let storage = FrankenStorage::open(&dir.path().join("partial-analytics.db")).unwrap();
        storage.ensure_search_fallback_fts_consistency().unwrap();
        let agent = gh423_agent(&storage);
        let mut conv = gh423_storage_snapshot();
        let first = storage
            .insert_conversations_batched_with_analytics(&[(agent, None, &conv)], mode == 0)
            .unwrap();
        let conversation_id = first[0].conversation_id;
        let ids: Vec<_> = storage
            .fetch_messages(conversation_id)
            .unwrap()
            .iter()
            .map(|message| (message.id, message.idx))
            .collect();
        if mode == 0 {
            // Real deferred ingest followed by the independently available
            // Track A rebuild must remain revisable without a token ledger.
            storage.rebuild_analytics().unwrap();
        } else {
            // Partial-materialization controls retain the real token
            // ledger while the other derived surfaces require backfill.
            for table in ["usage_hourly", "usage_daily", "usage_models_daily"] {
                storage
                    .raw()
                    .execute(&format!("DELETE FROM {table}"))
                    .unwrap();
            }
            if mode == 1 {
                storage
                    .raw()
                    .execute("DELETE FROM message_metrics")
                    .unwrap();
            } else {
                storage.raw().execute("DELETE FROM daily_stats").unwrap();
                storage
                    .raw()
                    .execute("DELETE FROM token_daily_stats")
                    .unwrap();
            }
        }
        gh423_revise(&mut conv);
        let revision = storage
            .insert_conversation_tree(agent, None, &conv)
            .unwrap();
        assert_eq!(revision.updated_indices, vec![1]);
        assert!(revision.inserted_indices.is_empty());
        let chars: i64 = conv
            .messages
            .iter()
            .map(|message| message.content.len() as i64)
            .sum();
        for (table, present) in [("message_metrics", mode != 1), ("token_usage", mode != 0)] {
            let totals: (i64, i64, i64) = storage.raw().query_row_map(
                &format!("SELECT COUNT(*), COALESCE(SUM(content_chars), 0), COALESCE(SUM(tool_call_count), 0) FROM {table}"),
                fparams![], |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
            ).unwrap();
            assert_eq!(
                totals,
                if present { (3, chars, 4) } else { (0, 0, 0) },
                "mode={mode}, {table}"
            );
        }
        for table in ["usage_hourly", "usage_daily", "usage_models_daily"] {
            let totals: (i64, i64) = storage.raw().query_row_map(
                &format!("SELECT COALESCE(SUM(message_count), 0), COALESCE(SUM(tool_call_count), 0) FROM {table}"),
                fparams![], |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
            ).unwrap();
            assert_eq!(totals, if mode == 0 { (3, 4) } else { (0, 0) });
        }
        for table in ["daily_stats", "token_daily_stats"] {
            let count: i64 = storage
                .raw()
                .query_row_map(
                    &format!("SELECT COUNT(*) FROM {table}"),
                    fparams![],
                    |row| row.get_typed(0),
                )
                .unwrap();
            if mode == 1 {
                assert!(count > 0);
            } else {
                assert_eq!(count, 0);
            }
        }
        if mode == 1 {
            let daily: (i64, i64, i64) = storage.raw().query_row_map(
                "SELECT session_count, message_count, total_chars FROM daily_stats WHERE agent_slug = 'codebuff' AND source_id = 'local'",
                fparams![], |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
            ).unwrap();
            assert_eq!(daily, (1, 3, chars));
            let tokens: (i64, i64) = storage.raw().query_row_map(
                "SELECT total_content_chars, total_tool_calls FROM token_daily_stats WHERE agent_slug = 'codebuff' AND source_id = 'local' AND model_family = 'unknown'",
                fparams![], |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
            ).unwrap();
            assert_eq!(tokens, (chars, 4));
        }
        for (term, expected) in [("oldneedle", 2), ("newneedle", 1)] {
            let count: i64 = storage
                .raw()
                .query_row_map(
                    "SELECT COUNT(*) FROM fts_messages WHERE fts_messages MATCH ?1",
                    fparams![term],
                    |row| row.get_typed(0),
                )
                .unwrap();
            assert_eq!(count, expected);
        }
        let snapshot = gh423_analytics_snapshot(&storage);
        let replay = storage
            .insert_conversation_tree(agent, None, &conv)
            .unwrap();
        assert!(replay.inserted_indices.is_empty() && replay.updated_indices.is_empty());
        assert_eq!(gh423_analytics_snapshot(&storage), snapshot);
        assert_eq!(
            storage
                .fetch_messages(conversation_id)
                .unwrap()
                .iter()
                .map(|message| (message.id, message.idx))
                .collect::<Vec<_>>(),
            ids
        );
    }
}

#[test]
#[serial]
fn gh423_codebuff_partial_daily_bucket_requires_bounded_canonical_rebuild() {
    let _lexical = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "0");
    let _conversations = set_env_var("CASS_DAILY_STATS_REBUILD_CONVERSATION_BATCH_SIZE", "1");
    let _messages = set_env_var("CASS_DAILY_STATS_REBUILD_MESSAGE_BATCH_SIZE", "2");
    let dir = TempDir::new().unwrap();
    let storage = FrankenStorage::open(&dir.path().join("partial-shared-day.db")).unwrap();
    let agent = gh423_agent(&storage);
    let mut conv = gh423_storage_snapshot();
    let mut sibling = conv.clone();
    sibling.external_id = Some("sibling-shared-day".into());
    sibling.source_path = "/sibling/chat.json".into();
    storage
        .insert_conversation_tree(agent, None, &sibling)
        .unwrap();
    storage
        .record_daily_stats_archive_fingerprint("original-bucket")
        .unwrap();
    storage
        .insert_conversations_batched_with_analytics(&[(agent, None, &conv)], true)
        .unwrap();
    storage.rebuild_analytics().unwrap();
    let daily = || -> (i64, i64, i64) {
        storage.raw().query_row_map(
            "SELECT session_count, message_count, total_chars FROM daily_stats WHERE agent_slug = 'codebuff' AND source_id = 'local'",
            fparams![], |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
        ).unwrap()
    };
    let sibling_chars: i64 = sibling
        .messages
        .iter()
        .map(|message| message.content.len() as i64)
        .sum();
    assert_eq!(daily(), (1, 3, sibling_chars));
    gh423_revise(&mut conv);
    storage
        .insert_conversation_tree(agent, None, &conv)
        .unwrap();
    assert_eq!(
        daily(),
        (1, 3, sibling_chars),
        "deferred messages never contributed a byte delta to this bucket"
    );
    assert!(storage.daily_stats_content_repair_required().unwrap());
    assert!(
        !storage
            .daily_stats_is_known_healthy_for_archive_fingerprint("original-bucket")
            .unwrap()
    );
    let rebuilt = storage.rebuild_daily_stats().unwrap();
    assert_eq!(rebuilt.total_sessions, 2);
    let revised_chars: i64 = conv
        .messages
        .iter()
        .map(|message| message.content.len() as i64)
        .sum();
    assert_eq!(daily(), (2, 6, sibling_chars + revised_chars));
    assert!(!storage.daily_stats_content_repair_required().unwrap());
    let replay = storage
        .insert_conversation_tree(agent, None, &conv)
        .unwrap();
    assert!(replay.updated_indices.is_empty() && replay.inserted_indices.is_empty());
    assert_eq!(daily(), (2, 6, sibling_chars + revised_chars));
    assert!(!storage.daily_stats_content_repair_required().unwrap());
}

#[test]
#[serial]
fn gh423_codebuff_deferred_fts_revision_requires_content_repair() {
    let _lexical = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "0");
    for suspended in [false, true] {
        let dir = TempDir::new().unwrap();
        let storage = FrankenStorage::open(&dir.path().join("revision-fts.db")).unwrap();
        storage.ensure_search_fallback_fts_consistency().unwrap();
        let agent = gh423_agent(&storage);
        let mut conv = gh423_storage_snapshot();
        storage
            .insert_conversation_tree(agent, None, &conv)
            .unwrap();
        storage.record_fts_franken_rebuild_generation().unwrap();
        storage
            .record_search_fallback_fts_archive_fingerprint("unchanged-rowids")
            .unwrap();
        gh423_revise(&mut conv);
        if suspended {
            assert!(storage.suspend_fts_inline_writes("bounded test budget exhausted".into()));
            storage
                .insert_conversation_tree(agent, None, &conv)
                .unwrap();
        } else {
            let _defer = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "1");
            storage
                .insert_conversation_tree(agent, None, &conv)
                .unwrap();
        }
        let term_count = |term: &str| -> i64 {
            storage
                .raw()
                .query_row_map(
                    "SELECT COUNT(*) FROM fts_messages WHERE fts_messages MATCH ?1",
                    fparams![term],
                    |row| row.get_typed(0),
                )
                .unwrap()
        };
        assert_eq!(term_count("oldneedle"), 3);
        assert_eq!(term_count("newneedle"), 0);
        assert_eq!(
            storage.inspect_search_fallback_fts_parity().unwrap().status,
            FtsShadowParityStatus::Healthy
        );
        assert!(
            !storage
                .fallback_fts_is_known_healthy_for_archive_fingerprint("unchanged-rowids")
                .unwrap()
        );
        arm_fts_rebuild_interruption(1);
        let error = storage
            .ensure_search_fallback_fts_consistency()
            .unwrap_err();
        assert!(format!("{error:#}").contains("injected FTS rebuild interruption"));
        assert_eq!(
            term_count("oldneedle"),
            3,
            "failed repair preserves prior publication"
        );
        assert_eq!(
            storage.read_fts_franken_rebuild_generation().unwrap(),
            Some(FTS_FRANKEN_CONTENT_REVISION_PENDING)
        );
        assert!(matches!(
            storage.ensure_search_fallback_fts_consistency().unwrap(),
            FtsConsistencyRepair::Rebuilt { inserted_rows: 3 }
        ));
        assert_eq!(term_count("oldneedle"), 2);
        assert_eq!(term_count("newneedle"), 1);
        assert_eq!(
            storage.read_fts_franken_rebuild_generation().unwrap(),
            Some(FTS_FRANKEN_REBUILD_GENERATION)
        );
        storage
            .insert_conversation_tree(agent, None, &conv)
            .unwrap();
        assert!(matches!(
            storage.ensure_search_fallback_fts_consistency().unwrap(),
            FtsConsistencyRepair::AlreadyHealthy { rows: 3 }
        ));
    }
}

#[test]
#[serial]
fn gh423_codebuff_writer_lifecycle_revisions_keep_absent_fts_optional() {
    let _lexical = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "0");
    for lane in 0..3 {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("writer-lifecycle.db");
        let owner = FrankenStorage::open(&path).unwrap();
        owner.raw().execute("DROP TABLE fts_messages").unwrap();
        let agent = gh423_agent(&owner);
        let mut conv = gh423_storage_snapshot();
        let first = if lane == 2 {
            // The initial batch learns that FTS is absent. Returning and
            // reacquiring this same connection must not manufacture a
            // positive presence result for the following revision.
            let (writer, reusable) = owner.acquire_cached_ephemeral_writer().unwrap();
            assert!(reusable);
            let first = gh423_persist(&writer, agent, &conv, 2).unwrap();
            owner.release_cached_ephemeral_writer(writer);
            first
        } else {
            gh423_persist(&owner, agent, &conv, 0).unwrap()
        };
        let before = owner.fetch_messages(first.conversation_id).unwrap();
        let writer = match lane {
            0 => FrankenStorage::open(&path).unwrap(),
            1 => FrankenStorage::open_writer(&path).unwrap(),
            _ => {
                let (writer, reusable) = owner.acquire_cached_ephemeral_writer().unwrap();
                assert!(reusable);
                writer
            }
        };
        gh423_revise(&mut conv);
        let revision = gh423_persist(&writer, agent, &conv, 2).unwrap();
        assert_eq!(revision.conversation_id, first.conversation_id);
        assert_eq!(revision.updated_indices, vec![1]);
        assert!(revision.inserted_indices.is_empty());
        let saved = writer.fetch_messages(first.conversation_id).unwrap();
        assert_eq!(saved.len(), 3);
        for (old, new) in before.iter().zip(&saved) {
            assert_eq!((old.id, old.idx), (new.id, new.idx));
        }
        assert_eq!(saved[1].content, conv.messages[1].content);
        assert_eq!(saved[1].extra_json, conv.messages[1].extra_json);
        assert_eq!(
            writer.inspect_search_fallback_fts_parity().unwrap().status,
            FtsShadowParityStatus::Absent
        );
        assert_eq!(
            writer.read_fts_franken_rebuild_generation().unwrap(),
            Some(FTS_FRANKEN_CONTENT_REVISION_PENDING)
        );
        gh423_assert_analytics(
            &writer,
            first.conversation_id,
            3,
            conv.messages
                .iter()
                .map(|message| message.content.len() as i64)
                .sum(),
            4,
        );
        let analytics = gh423_analytics_snapshot(&writer);
        let replay = gh423_persist(&writer, agent, &conv, 2).unwrap();
        assert!(replay.updated_indices.is_empty() && replay.inserted_indices.is_empty());
        assert_eq!(gh423_analytics_snapshot(&writer), analytics);
        if lane == 2 {
            owner.release_cached_ephemeral_writer(writer);
        }
    }
}

#[test]
#[serial]
fn gh423_codebuff_absent_fts_revision_reopens_and_recovers_failed_repair() {
    let _lexical = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "0");
    for route in 0..3 {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("absent-revision-fts.db");
        let storage = FrankenStorage::open(&path).unwrap();
        storage.raw().execute("DROP TABLE fts_messages").unwrap();
        assert_eq!(
            storage.inspect_search_fallback_fts_parity().unwrap().status,
            FtsShadowParityStatus::Absent
        );
        let agent = gh423_agent(&storage);
        let mut conv = gh423_storage_snapshot();
        let first = gh423_persist(&storage, agent, &conv, route).unwrap();
        let ids: Vec<_> = storage
            .fetch_messages(first.conversation_id)
            .unwrap()
            .iter()
            .map(|message| (message.id, message.idx))
            .collect();
        gh423_revise(&mut conv);
        let revision = gh423_persist(&storage, agent, &conv, route).unwrap();
        assert_eq!(revision.updated_indices, vec![1]);
        assert!(revision.inserted_indices.is_empty());
        assert_eq!(
            storage.inspect_search_fallback_fts_parity().unwrap().status,
            FtsShadowParityStatus::Absent,
            "native revisions must not require or eagerly create an FTS shadow"
        );
        assert_eq!(
            storage.read_fts_franken_rebuild_generation().unwrap(),
            Some(FTS_FRANKEN_CONTENT_REVISION_PENDING)
        );
        let canonical =
            serde_json::to_value(storage.fetch_messages(first.conversation_id).unwrap()).unwrap();
        let analytics = gh423_analytics_snapshot(&storage);
        drop(storage);

        let storage = FrankenStorage::open(&path).unwrap();
        let replay = gh423_persist(&storage, agent, &conv, route).unwrap();
        assert!(replay.inserted_indices.is_empty() && replay.updated_indices.is_empty());
        assert_eq!(
            storage.read_fts_franken_rebuild_generation().unwrap(),
            Some(FTS_FRANKEN_CONTENT_REVISION_PENDING),
            "unchanged replay must retain the unfulfilled repair obligation"
        );
        // Exercise real absent-table creation and row insertion, then
        // roll back before publication and reopen the database file.
        arm_fts_rebuild_interruption(2);
        let error = storage
            .ensure_search_fallback_fts_consistency()
            .unwrap_err();
        assert!(format!("{error:#}").contains("injected FTS rebuild interruption"));
        assert_eq!(
            storage.inspect_search_fallback_fts_parity().unwrap().status,
            FtsShadowParityStatus::Absent,
            "failed first publication must roll back the created shadow"
        );
        assert_eq!(
            storage.read_fts_franken_rebuild_generation().unwrap(),
            Some(FTS_FRANKEN_CONTENT_REVISION_PENDING)
        );
        assert_eq!(
            serde_json::to_value(storage.fetch_messages(first.conversation_id).unwrap()).unwrap(),
            canonical
        );
        assert_eq!(gh423_analytics_snapshot(&storage), analytics);
        drop(storage);

        let storage = FrankenStorage::open(&path).unwrap();
        assert!(matches!(
            storage.ensure_search_fallback_fts_consistency().unwrap(),
            FtsConsistencyRepair::Rebuilt { inserted_rows: 3 }
        ));
        for (term, expected) in [
            ("newneedle", vec![ids[1].0.unwrap()]),
            ("oldneedle", vec![ids[0].0.unwrap(), ids[2].0.unwrap()]),
        ] {
            let hits: Vec<i64> = storage
                .raw()
                .query_map_collect(
                    "SELECT rowid FROM fts_messages WHERE fts_messages MATCH ?1 ORDER BY rowid",
                    fparams![term],
                    |row| row.get_typed(0),
                )
                .unwrap();
            assert_eq!(hits, expected);
        }
        assert_eq!(
            serde_json::to_value(storage.fetch_messages(first.conversation_id).unwrap()).unwrap(),
            canonical
        );
        assert_eq!(gh423_analytics_snapshot(&storage), analytics);
        assert_eq!(
            storage.read_fts_franken_rebuild_generation().unwrap(),
            Some(FTS_FRANKEN_REBUILD_GENERATION)
        );
        assert!(matches!(
            storage.ensure_search_fallback_fts_consistency().unwrap(),
            FtsConsistencyRepair::AlreadyHealthy { rows: 3 }
        ));
    }
}

#[test]
#[serial]
fn gh423_codebuff_analytics_rebuild_revision_replay_conserves_tool_counts() {
    let _lexical = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "0");
    let dir = TempDir::new().unwrap();
    let storage = FrankenStorage::open(&dir.path().join("revision-analytics.db")).unwrap();
    let agent = gh423_agent(&storage);
    let mut conv = gh423_storage_snapshot();
    let first = storage
        .insert_conversation_tree(agent, None, &conv)
        .unwrap();
    let ids: Vec<_> = storage
        .fetch_messages(first.conversation_id)
        .unwrap()
        .iter()
        .map(|message| (message.id, message.idx))
        .collect();
    storage.rebuild_analytics().unwrap();
    gh423_assert_analytics(
        &storage,
        first.conversation_id,
        3,
        conv.messages
            .iter()
            .map(|message| message.content.len() as i64)
            .sum(),
        3,
    );
    gh423_revise(&mut conv);
    let revision = storage
        .insert_conversation_tree(agent, None, &conv)
        .unwrap();
    assert_eq!(revision.updated_indices, vec![1]);
    assert!(revision.inserted_indices.is_empty());
    storage.rebuild_analytics().unwrap();
    gh423_assert_analytics(
        &storage,
        first.conversation_id,
        3,
        conv.messages
            .iter()
            .map(|message| message.content.len() as i64)
            .sum(),
        4,
    );
    let analytics = gh423_analytics_snapshot(&storage);
    let replay = storage
        .insert_conversation_tree(agent, None, &conv)
        .unwrap();
    assert!(replay.updated_indices.is_empty());
    assert!(replay.inserted_indices.is_empty());
    assert_eq!(gh423_analytics_snapshot(&storage), analytics);
    assert_eq!(
        storage
            .fetch_messages(first.conversation_id)
            .unwrap()
            .iter()
            .map(|message| (message.id, message.idx))
            .collect::<Vec<_>>(),
        ids
    );
}

#[test]
#[serial]
fn gh423_codebuff_revision_invalidates_running_token_rebuild_and_resume() {
    let _lexical = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "0");
    let dir = TempDir::new().unwrap();
    let storage = FrankenStorage::open(&dir.path().join("revision-token-resume.db")).unwrap();
    let agent = gh423_agent(&storage);
    let mut conv = gh423_storage_snapshot();
    let first = storage
        .insert_conversation_tree(agent, None, &conv)
        .unwrap();
    let before = token_daily_stats_ledger_fingerprint(storage.raw()).unwrap();
    gh423_revise(&mut conv);
    let revised = std::cell::Cell::new(false);
    let heartbeat = || {
        if !revised.get()
            && read_token_daily_stats_rebuild_cursor(storage.raw())
                .unwrap()
                .is_some()
        {
            // This callback runs after the real staged batch committed.
            // Canonical ingest must invalidate that already-built stage.
            let outcome = storage
                .insert_conversation_tree(agent, None, &conv)
                .unwrap();
            assert_eq!(outcome.updated_indices, vec![1]);
            revised.set(true);
        }
    };
    let error = storage
        .rebuild_token_daily_stats_with_progress(Some(&heartbeat), None)
        .unwrap_err();
    assert!(error.to_string().contains("phase=pre_publish_consistency"));
    assert!(revised.get());
    let after = token_daily_stats_ledger_fingerprint(storage.raw()).unwrap();
    assert_eq!(
        (before.row_count, before.max_id),
        (after.row_count, after.max_id)
    );
    assert_ne!(before.fingerprint(), after.fingerprint());
    assert!(
        read_token_daily_stats_rebuild_cursor(storage.raw())
            .unwrap()
            .is_none()
    );
    assert!(historical_table_exists(storage.raw(), TOKEN_DAILY_STATS_REBUILD_STAGE_TABLE).unwrap());
    gh423_assert_analytics(
        &storage,
        first.conversation_id,
        3,
        conv.messages
            .iter()
            .map(|message| message.content.len() as i64)
            .sum(),
        4,
    );
    storage.rebuild_token_daily_stats().unwrap();
    gh423_assert_analytics(
        &storage,
        first.conversation_id,
        3,
        conv.messages
            .iter()
            .map(|message| message.content.len() as i64)
            .sum(),
        4,
    );
    let analytics = gh423_analytics_snapshot(&storage);
    let replay = storage
        .insert_conversation_tree(agent, None, &conv)
        .unwrap();
    assert!(replay.updated_indices.is_empty());
    assert_eq!(gh423_analytics_snapshot(&storage), analytics);
}

#[test]
#[serial]
fn gh423_codebuff_deferred_revision_keeps_published_analytics_consistent() {
    let _lexical = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "0");
    for initial_deferred in [false, true] {
        let dir = TempDir::new().unwrap();
        let storage = FrankenStorage::open(&dir.path().join("deferred.db")).unwrap();
        let agent = gh423_agent(&storage);
        let mut conv = gh423_storage_snapshot();
        let first = storage
            .insert_conversations_batched_with_analytics(&[(agent, None, &conv)], initial_deferred)
            .unwrap();
        gh423_revise(&mut conv);
        let revision = storage
            .insert_conversations_batched_with_analytics(&[(agent, None, &conv)], true)
            .unwrap();
        assert_eq!(revision[0].updated_indices, vec![1]);
        if initial_deferred {
            let count: i64 = storage
                .raw()
                .query_row_map("SELECT COUNT(*) FROM message_metrics", fparams![], |row| {
                    row.get_typed(0)
                })
                .unwrap();
            assert_eq!(
                count, 0,
                "deferred initial rows still require normal backfill"
            );
        } else {
            gh423_assert_analytics(
                &storage,
                first[0].conversation_id,
                3,
                conv.messages.iter().map(|m| m.content.len() as i64).sum(),
                4,
            );
        }
    }
}

#[test]
#[serial]
fn gh423_codebuff_native_revisions_are_source_and_store_scoped() {
    let _lexical = set_env_var("CASS_DEFER_LEXICAL_UPDATES", "0");
    let dir = TempDir::new().unwrap();
    let storage = FrankenStorage::open(&dir.path().join("scopes.db")).unwrap();
    let agent = gh423_agent(&storage);
    let first = gh423_storage_snapshot();
    let mut second_store = first.clone();
    second_store.external_id =
        Some(r#"["/second/.config/manicode","project","native-chat"]"#.into());
    second_store.source_path = PathBuf::from(
        "/second/.config/manicode/projects/project/chats/native-chat/chat-messages.json",
    );
    let mut remote = first.clone();
    remote.source_id = "remote-machine".into();
    remote.origin_host = Some("remote-host".into());
    let outcomes = storage
        .insert_conversations_batched_with_analytics(
            &[
                (agent, None, &first),
                (agent, None, &second_store),
                (agent, None, &remote),
            ],
            false,
        )
        .unwrap();
    let ids: HashSet<i64> = outcomes
        .iter()
        .map(|outcome| outcome.conversation_id)
        .collect();
    assert_eq!(ids.len(), 3);
    let before_store = storage.fetch_messages(outcomes[1].conversation_id).unwrap();
    let before_remote = storage.fetch_messages(outcomes[2].conversation_id).unwrap();
    let mut revised = first.clone();
    gh423_revise(&mut revised);
    let updated = gh423_persist(&storage, agent, &revised, 0).unwrap();
    assert_eq!(updated.conversation_id, outcomes[0].conversation_id);
    assert_eq!(updated.updated_indices, vec![1]);
    assert_eq!(
        serde_json::to_value(storage.fetch_messages(outcomes[1].conversation_id).unwrap()).unwrap(),
        serde_json::to_value(before_store).unwrap()
    );
    assert_eq!(
        serde_json::to_value(storage.fetch_messages(outcomes[2].conversation_id).unwrap()).unwrap(),
        serde_json::to_value(before_remote).unwrap()
    );
}

#[test]
fn gh447_native_fifo_batch_packets_and_scopes_are_independent() {
    let dir = TempDir::new().unwrap();
    let storage = FrankenStorage::open(&dir.path().join("scope.db")).unwrap();
    let agent = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "grok_bot".into(),
            name: "Grok Bot".into(),
            version: None,
            kind: AgentKind::Cli,
        })
        .unwrap();
    let first = gh447_native_window(1, 200);
    let second = gh447_native_window(2, 200);
    let third = gh447_native_window(201, 2);
    let mut other_account = first.clone();
    other_account.external_id = Some(
        first
            .external_id
            .as_ref()
            .unwrap()
            .replace("user_a", "user_b"),
    );
    let mut other_agent = first.clone();
    other_agent.external_id = Some(
        first
            .external_id
            .as_ref()
            .unwrap()
            .replace("agent-a", "agent-b"),
    );
    let mut remote = first.clone();
    remote.source_id = "remote-native".into();
    let outcomes = storage
        .insert_conversations_batched_with_analytics(
            &[
                (agent, None, &first),
                (agent, None, &second),
                (agent, None, &second),
                (agent, None, &third),
                (agent, None, &other_account),
                (agent, None, &other_agent),
                (agent, None, &remote),
            ],
            false,
        )
        .unwrap();
    assert_eq!(
        outcomes
            .iter()
            .map(|o| o.inserted_indices.len())
            .collect::<Vec<_>>(),
        vec![200, 1, 0, 1, 200, 200, 200]
    );
    let original = outcomes[0].conversation_id;
    assert!(
        outcomes[..4]
            .iter()
            .all(|outcome| outcome.conversation_id == original)
    );
    assert_eq!(outcomes[3].inserted_indices, vec![201]);
    let scopes = [
        original,
        outcomes[4].conversation_id,
        outcomes[5].conversation_id,
        outcomes[6].conversation_id,
    ];
    assert_eq!(scopes.into_iter().collect::<HashSet<_>>().len(), 4);
    assert_eq!(storage.fetch_messages(original).unwrap().len(), 202);
    for id in &scopes[1..] {
        assert_eq!(storage.fetch_messages(*id).unwrap().len(), 200);
    }
}

#[test]
fn gh447_native_fifo_conflicts_and_missing_ids_roll_back_the_transaction() {
    for batched in [false, true] {
        let dir = TempDir::new().unwrap();
        let storage = FrankenStorage::open(&dir.path().join("conflict.db")).unwrap();
        let agent = storage
            .ensure_agent(&Agent {
                id: None,
                slug: "grok_bot".into(),
                name: "Grok Bot".into(),
                version: None,
                kind: AgentKind::Cli,
            })
            .unwrap();
        let initial = gh447_native_window(1, 2);
        let id = gh447_persist(&storage, agent, &initial, batched)
            .unwrap()
            .conversation_id;
        // Exercise the historical JSON-column form alongside normal MessagePack rows.
        storage.raw().execute_compat(
            "UPDATE messages SET extra_json = ?1, extra_bin = NULL WHERE conversation_id = ?2 AND idx = 0",
            fparams![serde_json::to_string(&initial.messages[0].extra_json).unwrap(), id],
        ).unwrap();
        assert!(
            gh447_persist(&storage, agent, &initial, batched)
                .unwrap()
                .inserted_indices
                .is_empty()
        );
        let before = serde_json::to_value(storage.fetch_messages(id).unwrap()).unwrap();
        for defect in 0..5 {
            let mut invalid = gh447_native_window(3, 1);
            let mut conflicting = initial.messages[0].clone();
            match defect {
                0 => conflicting.content = "conflicting native identity".into(),
                1 => conflicting.extra_json = serde_json::json!({}),
                2 => conflicting.extra_json["grok_bot_entry_id"] = serde_json::json!(" "),
                3 => conflicting.created_at = Some(99),
                _ => conflicting.role = MessageRole::User,
            }
            invalid.messages.push(conflicting);
            let result = if batched {
                storage
                    .insert_conversations_batched_with_analytics(
                        &[
                            (agent, None, &gh447_native_window(9, 1)),
                            (agent, None, &invalid),
                        ],
                        false,
                    )
                    .map(|_| ())
            } else {
                gh447_persist(&storage, agent, &invalid, false).map(|_| ())
            };
            assert!(
                result.is_err(),
                "identity defect {defect} must refuse the complete transaction"
            );
            assert_eq!(
                serde_json::to_value(storage.fetch_messages(id).unwrap()).unwrap(),
                before
            );
        }
        let mut duplicate = gh447_native_window(3, 1);
        duplicate.messages.push(duplicate.messages[0].clone());
        assert_eq!(
            gh447_persist(&storage, agent, &duplicate, batched)
                .unwrap()
                .inserted_indices,
            vec![2]
        );
        let mut conflicting_duplicate = gh447_native_window(4, 1);
        let mut second = conflicting_duplicate.messages[0].clone();
        second.content = "changed under same ID".into();
        conflicting_duplicate.messages.push(second);
        assert!(gh447_persist(&storage, agent, &conflicting_duplicate, batched).is_err());
        assert_eq!(storage.fetch_messages(id).unwrap().len(), 3);
        let empty = gh447_native_window(1, 0);
        assert!(
            gh447_persist(&storage, agent, &empty, batched)
                .unwrap()
                .inserted_indices
                .is_empty()
        );
    }
}

#[test]
fn gh415_shelley_metadata_refresh_preserves_rows_and_invalidates_searchable_changes() {
    for batched in [false, true] {
        let dir = TempDir::new().unwrap();
        let storage = FrankenStorage::open(&dir.path().join("shelley.db")).unwrap();
        let agent = storage
            .ensure_agent(&Agent {
                id: None,
                slug: "shelley".into(),
                name: "Shelley".into(),
                version: None,
                kind: AgentKind::Cli,
            })
            .unwrap();
        let old_path = PathBuf::from("/old");
        let new_path = PathBuf::from("/new");
        let old = storage.ensure_workspace(&old_path, None).unwrap();
        let new = storage.ensure_workspace(&new_path, None).unwrap();
        let mut conv = Conversation {
            id: None,
            agent_slug: "shelley".into(),
            workspace: Some(old_path),
            external_id: Some("shelley:fixture:alpha".into()),
            title: Some("old title".into()),
            source_path: PathBuf::from("/source/shelley.db"),
            started_at: Some(100),
            ended_at: Some(100),
            approx_tokens: None,
            metadata_json: serde_json::json!({"source":"shelley","shelley":{"archived":false},"cass":{"retained":true}}),
            messages: vec![Message {
                id: None,
                idx: 7,
                role: MessageRole::User,
                author: None,
                created_at: Some(100),
                content: "retained Shelley content".into(),
                extra_json: serde_json::json!({"keep":true}),
                snippets: vec![],
            }],
            source_id: "local".into(),
            origin_host: None,
        };
        let original = storage
            .insert_conversation_tree(agent, Some(old), &conv)
            .unwrap();
        let messages =
            serde_json::to_value(storage.fetch_messages(original.conversation_id).unwrap())
                .unwrap();
        conv.messages.clear();
        conv.metadata_json =
            serde_json::json!({"source":"shelley","shelley":{"archived":true,"tags":["reviewed"]}});
        let mut generation = None;
        for step in 0..5 {
            if step == 1 {
                conv.title = Some("new title".into());
            }
            if step == 2 {
                conv.workspace = Some(new_path.clone());
            }
            let workspace = if step < 2 { old } else { new };
            let outcome = if batched {
                storage
                    .insert_conversations_batched(&[(agent, Some(workspace), &conv)])
                    .unwrap()
                    .pop()
                    .unwrap()
            } else {
                storage
                    .insert_conversation_tree(agent, Some(workspace), &conv)
                    .unwrap()
            };
            assert_eq!(outcome.conversation_id, original.conversation_id);
            assert!(!outcome.conversation_inserted);
            assert!(outcome.inserted_indices.is_empty());
            assert_eq!(outcome.workspace_changed, step == 1 || step == 2);
            assert_eq!(
                serde_json::to_value(storage.fetch_messages(original.conversation_id).unwrap())
                    .unwrap(),
                messages
            );
            let saved = storage.list_conversations(10, 0).unwrap().pop().unwrap();
            assert_eq!(saved.metadata_json["cass"]["retained"], true);
            assert_eq!(saved.metadata_json["shelley"]["archived"], true);
            assert_eq!(saved.title, conv.title);
            for tier in [SemanticIdentityTier::Fast, SemanticIdentityTier::Quality] {
                let current = storage.semantic_identity_rebuild_generation(tier).unwrap();
                if step < 2 {
                    assert!(current.is_none());
                } else if step == 2 {
                    assert!(current.is_some());
                    if generation.is_none() {
                        generation = current.clone();
                    }
                    assert_eq!(current, generation);
                } else {
                    assert_eq!(current, generation);
                }
            }
        }
    }
}

#[test]
fn gh459_cursor_workspace_repair_preserves_rows_and_requires_explicit_authority() {
    for batched in [false, true] {
        let dir = TempDir::new().unwrap();
        let storage = FrankenStorage::open(&dir.path().join("cursor.db")).unwrap();
        let agent = storage
            .ensure_agent(&Agent {
                id: None,
                slug: "cursor".into(),
                name: "Cursor".into(),
                version: None,
                kind: AgentKind::Cli,
            })
            .unwrap();
        let wrong = PathBuf::from("/parent/project/my/app");
        let correct = PathBuf::from("/parent-project/my-app");
        let wrong_id = storage.ensure_workspace(&wrong, None).unwrap();
        let correct_id = storage.ensure_workspace(&correct, None).unwrap();
        let mut conv = Conversation {
            id: None,
            agent_slug: "cursor".into(),
            workspace: Some(wrong),
            external_id: Some("cursor-agent-stable-id".into()),
            title: Some("unchanged title".into()),
            source_path: PathBuf::from("/cursor/transcript.jsonl"),
            started_at: Some(100),
            ended_at: Some(100),
            approx_tokens: None,
            metadata_json: serde_json::json!({"cursor_format":"agent", "keep":{"nested":7}, "cass":{"retained":true}}),
            messages: vec![Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: None,
                created_at: Some(100),
                content: "unchanged workspace message".into(),
                extra_json: serde_json::json!({"retained":true}),
                snippets: Vec::new(),
            }],
            source_id: "local".into(),
            origin_host: None,
        };
        let original = storage
            .insert_conversation_tree(agent, Some(wrong_id), &conv)
            .unwrap();
        let messages =
            serde_json::to_value(storage.fetch_messages(original.conversation_id).unwrap())
                .unwrap();
        storage
            .set_last_embedded_message_id(storage.max_message_id().unwrap().unwrap())
            .unwrap();
        let mut repair_generation = None;
        conv.workspace = Some(correct.clone());
        conv.metadata_json = serde_json::json!({"cursor_format":"agent", "cursor_workspace_attribution":"workspace_trusted", "cursor_project_dir":"parent-project-my-app", "cass":{"workspace_original":"/remote/my-app"}});
        for expected_change in [true, false] {
            let outcome = if batched {
                storage
                    .insert_conversations_batched(&[(agent, Some(correct_id), &conv)])
                    .unwrap()
                    .pop()
                    .unwrap()
            } else {
                storage
                    .insert_conversation_tree(agent, Some(correct_id), &conv)
                    .unwrap()
            };
            assert_eq!(outcome.conversation_id, original.conversation_id);
            assert!(!outcome.conversation_inserted);
            assert!(outcome.inserted_indices.is_empty());
            assert_eq!(outcome.workspace_changed, expected_change);
            assert_eq!(storage.get_last_embedded_message_id().unwrap(), None);
            let generation = storage
                .semantic_identity_rebuild_generation(SemanticIdentityTier::Fast)
                .unwrap()
                .expect("fast identity debt");
            assert_eq!(
                storage
                    .semantic_identity_rebuild_generation(SemanticIdentityTier::Quality)
                    .unwrap()
                    .as_ref(),
                Some(&generation)
            );
            if expected_change {
                repair_generation = Some(generation);
            } else {
                assert_eq!(
                    Some(generation),
                    repair_generation,
                    "idempotent replay must retain pending semantic generation"
                );
            }
            let rows = storage.list_conversations(10, 0).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].workspace.as_ref(), Some(&correct));
            assert_eq!(rows[0].metadata_json["keep"]["nested"], 7);
            assert_eq!(rows[0].metadata_json["cass"]["retained"], true);
            assert_eq!(
                rows[0].metadata_json["cass"]["workspace_original"],
                "/remote/my-app"
            );
            assert_eq!(
                serde_json::to_value(storage.fetch_messages(original.conversation_id).unwrap())
                    .unwrap(),
                messages
            );
        }
        storage.close().unwrap();
        let storage = FrankenStorage::open(&dir.path().join("cursor.db")).unwrap();
        for tier in [SemanticIdentityTier::Fast, SemanticIdentityTier::Quality] {
            assert_eq!(
                storage.semantic_identity_rebuild_generation(tier).unwrap(),
                repair_generation,
                "identity debt must survive reopen"
            );
        }
        conv.workspace = None;
        for metadata in [
            serde_json::json!({"cursor_format":"agent"}),
            serde_json::json!({"cursor_format":"agent", "cursor_workspace_attribution":"workspace_trusted"}),
            serde_json::json!({"cursor_format":"ide", "cursor_workspace_attribution":"unresolved"}),
        ] {
            conv.metadata_json = metadata;
            let outcome = storage
                .insert_conversation_tree(agent, None, &conv)
                .unwrap();
            assert!(!outcome.workspace_changed);
            assert_eq!(
                storage.list_conversations(10, 0).unwrap()[0]
                    .workspace
                    .as_ref(),
                Some(&correct)
            );
        }
        conv.metadata_json = serde_json::json!({"cursor_format":"agent", "cursor_workspace_attribution":"unresolved"});
        assert!(!provider_workspace_is_authoritative(
            "codex",
            None,
            &conv.metadata_json
        ));
        let cleared = storage
            .insert_conversations_batched(&[(agent, None, &conv)])
            .unwrap()
            .pop()
            .unwrap();
        assert!(cleared.workspace_changed);
        assert_eq!(cleared.conversation_id, original.conversation_id);
        assert!(cleared.inserted_indices.is_empty());
        assert_eq!(
            storage.list_conversations(10, 0).unwrap()[0].workspace,
            None
        );
        assert_eq!(
            serde_json::to_value(storage.fetch_messages(original.conversation_id).unwrap())
                .unwrap(),
            messages
        );
        let cleared_generation = storage
            .semantic_identity_rebuild_generation(SemanticIdentityTier::Fast)
            .unwrap();
        assert_ne!(
            cleared_generation, repair_generation,
            "a second change needs a new semantic generation"
        );
        conv.workspace = Some(correct.clone());
        conv.metadata_json["cursor_workspace_attribution"] = serde_json::json!("workspace_trusted");
        {
            let mut tx = storage
                .conn
                .transaction_with_behavior(crate::franken_sync::TransactionBehavior::Immediate)
                .unwrap();
            assert!(
                franken_reconcile_provider_workspace(
                    &tx,
                    agent,
                    original.conversation_id,
                    Some(correct_id),
                    &conv
                )
                .unwrap()
            );
            tx.rollback().unwrap();
        }
        assert_eq!(
            storage.list_conversations(10, 0).unwrap()[0].workspace,
            None
        );
        for tier in [SemanticIdentityTier::Fast, SemanticIdentityTier::Quality] {
            assert_eq!(
                storage.semantic_identity_rebuild_generation(tier).unwrap(),
                cleared_generation,
                "workspace and both debts must roll back together"
            );
        }
        let changed_back = storage
            .insert_conversation_tree(agent, Some(correct_id), &conv)
            .unwrap();
        assert!(changed_back.workspace_changed);
        let returned_generation = storage
            .semantic_identity_rebuild_generation(SemanticIdentityTier::Fast)
            .unwrap();
        assert_ne!(
            returned_generation, repair_generation,
            "returning to an earlier workspace must not revive an old checkpoint"
        );
        assert_ne!(returned_generation, cleared_generation);
        assert_eq!(
            serde_json::to_value(storage.fetch_messages(original.conversation_id).unwrap())
                .unwrap(),
            messages
        );
    }
}
