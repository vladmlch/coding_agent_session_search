#[allow(unused_imports)]
pub(super) use super::*;

#[cfg(test)]
mod resolve_data_dir_tests {
    use super::{default_data_dir, resolve_data_dir};
    use std::path::PathBuf;

    #[test]
    fn no_overrides_yields_default() {
        assert_eq!(resolve_data_dir(&None, None), default_data_dir());
    }

    #[test]
    fn explicit_data_dir_override_always_wins() {
        let dir = PathBuf::from("/explicit/data-dir");
        let db = PathBuf::from("/scratch/probe.db");
        assert_eq!(resolve_data_dir(&Some(dir.clone()), Some(&db)), dir);
    }

    #[test]
    fn db_inside_default_data_dir_keeps_default() {
        let db = default_data_dir().join("agent_search.db");
        assert_eq!(resolve_data_dir(&None, Some(&db)), default_data_dir());
    }

    #[test]
    fn scratch_db_isolates_data_dir_to_its_parent() {
        // GH #403: `--db /scratch/probe.db` must not leave derived assets
        // (lexical index, raw-mirror, locks) pointing at the live data dir.
        let tmp = tempfile::tempdir().expect("tempdir");
        let db = tmp.path().join("probe.db");
        assert_eq!(resolve_data_dir(&None, Some(&db)), tmp.path());
    }
}

#[cfg(all(test, not(windows)))]
mod clipboard_helper_tests {
    use super::copy_to_system_clipboard;

    /// On a host with **no** clipboard tool on PATH (PATH=""), the helper
    /// must return Err so the caller can fall back to stdout. The
    /// previous failure mode was a silent success that dropped the
    /// export on the floor.
    #[test]
    fn returns_err_when_no_tool_is_available() {
        // Save and clear PATH so spawn() can't find pbcopy/wl-copy/xclip
        // /xsel/clip. Restore on exit even if the assertion panics.
        struct PathGuard(Option<std::ffi::OsString>);
        impl Drop for PathGuard {
            fn drop(&mut self) {
                // Tests touch process env: required for an end-to-end
                // check of the spawn fallback chain.
                unsafe {
                    match self.0.take() {
                        Some(prev) => std::env::set_var("PATH", prev),
                        None => std::env::remove_var("PATH"),
                    }
                }
            }
        }
        let guard = PathGuard(std::env::var_os("PATH"));
        unsafe {
            std::env::set_var("PATH", "");
        }

        let err = copy_to_system_clipboard("hello")
            .expect_err("with PATH cleared, no clipboard tool should be findable");
        assert!(
            err.contains("no clipboard tool") || err.contains("tried"),
            "error should describe what was tried, got: {err}"
        );

        drop(guard);
    }
}

// ============================================================================
// Unit Tests for Message Grouping Algorithm
// ============================================================================

#[cfg(test)]
mod export_timestamp_tests {
    use super::{
        extract_message_timestamp, format_export_duration, publish_unique_export_output_file,
        run_export_html, write_unique_export_output_file,
    };
    use serde_json::json;
    use std::fs;
    use std::io::Write;
    use tempfile::TempDir;

    #[cfg(unix)]
    #[test]
    fn ztlan_clipboard_stdin_survives_default_sigpipe_and_delivers_exact_bytes() {
        use std::process::{Command, Stdio};
        use std::time::{Duration, Instant};

        if dotenvy::var("CASS_TEST_ZTLAN_SIGPIPE_CHILD")
            .ok()
            .as_deref()
            == Some("1")
        {
            // Only this fresh process changes the signal disposition.
            // Its parent test harness must never inherit SIG_DFL here.
            sigpipe::reset();
            let text = "clipboard 🦀\0\n".repeat(100_000);
            let mut exits_without_reading = Command::new("sh");
            exits_without_reading.args(["-c", "exit 23"]);
            match super::write_clipboard_command(exits_without_reading, &text) {
                Ok(status) => assert_eq!(status.code(), Some(23)),
                Err(error) => assert!(
                    matches!(
                        error.kind(),
                        std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
                    ),
                    "unexpected write error: {error}"
                ),
            }

            let temp = TempDir::new().expect("clipboard receiver fixture");
            let output_path = temp.path().join("received");
            let mut reads_until_eof = Command::new("sh");
            reads_until_eof
                .args(["-c", "cat > \"$1\"", "cass-clipboard-test"])
                .arg(&output_path);
            assert!(
                super::write_clipboard_command(reads_until_eof, &text)
                    .expect("deliver clipboard bytes")
                    .success()
            );
            assert_eq!(
                fs::read(output_path).expect("receiver bytes"),
                text.as_bytes()
            );
            assert!(
                super::write_clipboard_command(Command::new(temp.path().join("absent")), &text)
                    .is_err()
            );
            return;
        }

        let mut child = Command::new(std::env::current_exe().expect("test executable"))
            .args([
                "--exact",
                &format!(
                    "{}::ztlan_clipboard_stdin_survives_default_sigpipe_and_delivers_exact_bytes",
                    module_path!().split_once("::").unwrap().1
                ),
                "--nocapture",
            ])
            .env("CASS_TEST_ZTLAN_SIGPIPE_CHILD", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("fresh SIGPIPE test process");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if child.try_wait().expect("child status").is_some() {
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("clipboard stdin did not close or notice the exited reader");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let output = child.wait_with_output().expect("child output");
        assert!(
            output.status.success(),
            "clipboard child terminated: {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
    }

    #[test]
    fn extract_message_timestamp_parses_multiple_shapes() {
        let direct_i64 = json!({"timestamp": 1_733_000_000});
        assert_eq!(
            extract_message_timestamp(&direct_i64),
            Some(1_733_000_000_000)
        );

        let direct_iso = json!({"timestamp": "2025-12-01T10:00:00Z"});
        assert!(extract_message_timestamp(&direct_iso).is_some());

        let nested_message = json!({"message": {"timestamp": "1733000000"}});
        assert_eq!(
            extract_message_timestamp(&nested_message),
            Some(1_733_000_000_000)
        );

        let nested_payload = json!({"payload": {"timestamp": 1_733_000_123_000i64}});
        assert_eq!(
            extract_message_timestamp(&nested_payload),
            Some(1_733_000_123_000)
        );
    }

    #[test]
    fn export_duration_handles_boundaries_without_overflow() {
        assert_eq!(
            format_export_duration(Some(0), Some(59_999)).as_deref(),
            Some("< 1m")
        );
        assert_eq!(
            format_export_duration(Some(0), Some(60_000)).as_deref(),
            Some("1m")
        );
        assert_eq!(
            format_export_duration(Some(0), Some(3_660_000)).as_deref(),
            Some("1h 1m")
        );
        assert_eq!(format_export_duration(Some(1), Some(1)), None);
        assert_eq!(format_export_duration(Some(2), Some(1)), None);
        assert_eq!(format_export_duration(Some(i64::MIN), Some(i64::MAX)), None);
    }

    #[test]
    fn export_html_reports_not_indexed_without_reading_source_file() {
        // Index-first export: the source path is an opaque identity key and
        // is never opened. An unreadable file must not matter — the failure
        // is the missing indexed conversation, not a file-read error.
        let temp = TempDir::new().expect("temp dir");
        let db_path = temp.path().join("agent_search.db");
        drop(crate::storage::sqlite::FrankenStorage::open(&db_path).expect("open db"));

        let session_path = temp.path().join("broken-session.jsonl");
        fs::write(
            &session_path,
            b"{\"role\":\"user\",\"content\":\"hello\"}\n\xff\n",
        )
        .expect("write invalid session");

        let err = run_export_html(
            &session_path,
            Some(db_path),
            None,
            None,
            Some(temp.path()),
            Some("out.html"),
            true,
            true,
            false,
            false,
            "system",
            false,
            false,
            false,
            None,
        )
        .expect_err("unindexed path should fail explicitly");

        assert_eq!(err.kind, "conversation-not-indexed");
        assert_eq!(err.code, 3);
    }

    #[test]
    #[cfg(unix)]
    fn export_html_does_not_follow_existing_output_symlink() {
        use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
        use crate::storage::sqlite::FrankenStorage;

        let temp = TempDir::new().expect("temp dir");
        let db_path = temp.path().join("agent_search.db");
        let storage = FrankenStorage::open(&db_path).expect("open db");
        let agent_id = storage
            .ensure_agent(&Agent {
                id: None,
                slug: "claude_code".to_string(),
                name: "Claude Code".to_string(),
                version: None,
                kind: AgentKind::Cli,
            })
            .expect("ensure agent");
        let session_path = temp.path().join("session.jsonl");
        storage
            .insert_conversation_tree(
                agent_id,
                None,
                &Conversation {
                    id: None,
                    agent_slug: "claude_code".to_string(),
                    workspace: None,
                    external_id: Some("symlink-safe".to_string()),
                    title: Some("Symlink Safe Session".to_string()),
                    source_path: session_path.clone(),
                    started_at: Some(1_733_000_000_000),
                    ended_at: Some(1_733_000_010_000),
                    approx_tokens: None,
                    metadata_json: serde_json::json!({}),
                    messages: vec![Message {
                        id: None,
                        idx: 0,
                        role: MessageRole::Agent,
                        author: None,
                        created_at: Some(1_733_000_000_000),
                        content: "safe exported response".to_string(),
                        extra_json: serde_json::Value::Null,
                        snippets: Vec::new(),
                    }],
                    source_id: crate::sources::provenance::LOCAL_SOURCE_ID.to_string(),
                    origin_host: None,
                },
            )
            .expect("insert indexed conversation");
        drop(storage);

        let protected_target = temp.path().join("protected.html");
        fs::write(&protected_target, "do not overwrite").expect("write protected target");
        let requested_output = temp.path().join("out.html");
        std::os::unix::fs::symlink(&protected_target, &requested_output)
            .expect("create output symlink");

        run_export_html(
            &session_path,
            Some(db_path),
            None,
            None,
            Some(temp.path()),
            Some("out.html"),
            true,
            true,
            false,
            false,
            "system",
            false,
            false,
            false,
            None,
        )
        .expect("export should choose a fresh filename instead of following symlink");

        assert_eq!(
            fs::read_to_string(&protected_target).expect("read protected target"),
            "do not overwrite"
        );
        assert_eq!(
            fs::read_link(&requested_output).expect("requested output remains a symlink"),
            protected_target
        );
        let actual_output = temp.path().join("out_1.html");
        let html = fs::read_to_string(&actual_output).expect("read suffixed export");
        assert!(html.contains("safe exported response"));
    }

    #[test]
    fn export_output_file_retries_when_preselected_path_now_exists() {
        let temp = TempDir::new().expect("temp dir");
        let mut output_path = temp.path().join("out.html");

        fs::write(&output_path, "first writer").expect("create race winner");

        write_unique_export_output_file(
            temp.path(),
            "out.html",
            &mut output_path,
            b"second writer",
        )
        .expect("retry with suffixed filename");

        assert_eq!(
            fs::read_to_string(temp.path().join("out.html")).expect("read original"),
            "first writer"
        );
        assert_eq!(
            output_path.file_name().and_then(|name| name.to_str()),
            Some("out_1.html")
        );
        assert_eq!(
            fs::read_to_string(&output_path).expect("read retry output"),
            "second writer"
        );
    }

    #[test]
    fn export_output_publish_failure_never_exposes_partial_final_file() {
        let temp = TempDir::new().expect("temp dir");
        let mut output_path = temp.path().join("out.html");

        let err =
            publish_unique_export_output_file(temp.path(), "out.html", &mut output_path, |file| {
                file.write_all(b"partial bytes")?;
                Err(std::io::Error::other("injected staged-write failure"))
            })
            .expect_err("injected staged write should fail");

        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert!(
            !output_path.exists(),
            "a failed staged write must not expose a partial final export"
        );
        let retained_entries = fs::read_dir(temp.path())
            .expect("read output directory")
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(
            retained_entries.is_empty(),
            "failed staged export should clean up private temporary files: {retained_entries:?}"
        );
    }

    #[test]
    #[cfg(unix)]
    fn export_output_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().expect("temp dir");
        let mut output_path = temp.path().join("out.html");

        write_unique_export_output_file(
            temp.path(),
            "out.html",
            &mut output_path,
            b"private transcript",
        )
        .expect("publish private export");

        let mode = fs::metadata(&output_path)
            .expect("output metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "HTML exports must not be group/world-readable");
    }

    #[test]
    fn export_html_virtual_opencode_sqlite_path_reports_not_indexed() {
        // Index-first export: a virtual `<db>/<session-id>` path is only an
        // identity key. When no indexed conversation claims it, export fails
        // with the not-indexed diagnostic instead of parsing the provider
        // database itself.
        let temp = TempDir::new().expect("temp dir");
        let db_path = temp.path().join("agent_search.db");
        drop(crate::storage::sqlite::FrankenStorage::open(&db_path).expect("open db"));

        let opencode_db = temp.path().join("opencode.db");
        fs::write(&opencode_db, b"not a sqlite db").expect("write placeholder db file");
        let session_path = opencode_db.join("session-1");

        let err = run_export_html(
            &session_path,
            Some(db_path),
            None,
            None,
            Some(temp.path()),
            Some("out.html"),
            true,
            true,
            false,
            false,
            "system",
            false,
            false,
            false,
            None,
        )
        .expect_err("virtual session without an indexed row should fail explicitly");

        assert_eq!(err.kind, "conversation-not-indexed");
        assert_eq!(err.code, 3);
    }
}

#[cfg(test)]
mod legacy_source_filter_tests {
    use super::*;
    use crate::franken_sync::compat::{ConnectionExt, RowExt};
    use crate::sources::provenance::SourceFilter;
    use std::path::Path;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn make_legacy_local_db() -> (TempDir, PathBuf, PathBuf) {
        let tmp = TempDir::new().expect("temp dir");
        let db_path = tmp.path().join("agent_search.db");
        let shared_path = PathBuf::from("/legacy/shared/session.jsonl");

        with_sqlite_connection(&db_path, "legacy source test", |conn| {
            conn.execute("PRAGMA application_id = 0x43415353;")?;
            conn.execute("PRAGMA user_version = 1;")?;
            conn.execute("PRAGMA journal_mode = WAL;")?;
            conn.execute("CREATE TABLE agents (id INTEGER PRIMARY KEY, slug TEXT NOT NULL);")?;
            conn.execute(
                "CREATE TABLE workspaces (id INTEGER PRIMARY KEY, path TEXT, display_name TEXT);",
            )?;
            conn.execute(
                "CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT, host_label TEXT);",
            )?;
            conn.execute(
                "CREATE TABLE conversations (
                    id INTEGER PRIMARY KEY,
                    agent_id INTEGER NOT NULL,
                    workspace_id INTEGER,
                    source_id TEXT,
                    origin_host TEXT,
                    title TEXT,
                    source_path TEXT NOT NULL,
                    started_at INTEGER,
                    ended_at INTEGER,
                    approx_tokens INTEGER,
                    external_id TEXT,
                    metadata_json TEXT,
                    metadata_bin BLOB
                );",
            )?;
            conn.execute(
                "CREATE TABLE messages (
                    id INTEGER PRIMARY KEY,
                    conversation_id INTEGER NOT NULL,
                    idx INTEGER,
                    role TEXT,
                    created_at INTEGER,
                    content TEXT
                );",
            )?;
            conn.execute("INSERT INTO agents(id, slug) VALUES (1, 'codex')")?;
            conn.execute(
                "INSERT INTO sources(id, kind, host_label) VALUES ('laptop', 'ssh', 'user@laptop')",
            )?;
            conn.execute(
                "INSERT INTO conversations(
                    id, agent_id, workspace_id, source_id, origin_host, title,
                    source_path, started_at, ended_at, approx_tokens, external_id,
                    metadata_json, metadata_bin
                ) VALUES (
                    1, 1, NULL, NULL, NULL, 'Local Session',
                    '/legacy/shared/session.jsonl', 1700000000000, NULL, 10,
                    'legacy-local', '{}', NULL
                )",
            )?;
            conn.execute(
                "INSERT INTO conversations(
                    id, agent_id, workspace_id, source_id, origin_host, title,
                    source_path, started_at, ended_at, approx_tokens, external_id,
                    metadata_json, metadata_bin
                ) VALUES (
                    2, 1, NULL, 'laptop', 'user@laptop', 'Remote Session',
                    '/legacy/shared/session.jsonl', 1700000100000, NULL, 10,
                    'remote-shared', '{}', NULL
                )",
            )?;
            conn.execute(
                "INSERT INTO messages(id, conversation_id, idx, role, created_at, content)
                 VALUES (1, 1, 0, 'user', 1700000000100, 'local body')",
            )?;
            conn.execute(
                "INSERT INTO messages(id, conversation_id, idx, role, created_at, content)
                 VALUES (2, 2, 0, 'user', 1700000100100, 'remote body')",
            )?;
            Ok(())
        })
        .expect("create legacy db");

        (tmp, db_path, shared_path)
    }

    fn open_legacy_local_db_for_update(db_path: &Path) -> crate::franken_sync::Connection {
        crate::franken_sync::Connection::open(db_path.to_string_lossy().to_string())
            .expect("open writable legacy source test db")
    }

    #[test]
    fn source_filter_helpers_use_normalized_origin_kind_for_local_semantics() {
        let normalized_source_sql =
            normalized_source_identity_sql_expr("c.source_id", "c.origin_host");
        let normalized_where_origin_kind_sql = normalized_source_origin_kind_sql_expr(
            "c.source_id",
            "(SELECT s.kind FROM sources s WHERE s.id = c.source_id LIMIT 1)",
            "c.origin_host",
        );

        let (where_sql, param) = source_filter_where_clause(Some(&SourceFilter::Local));
        assert_eq!(
            where_sql,
            format!(
                " WHERE {normalized_where_origin_kind_sql} = '{local}'",
                local = crate::sources::provenance::LOCAL_SOURCE_ID,
            )
        );
        assert!(param.is_none());

        let (where_sql, param) =
            source_filter_where_clause(Some(&SourceFilter::SourceId("  LOCAL  ".to_string())));
        assert_eq!(where_sql, format!(" WHERE {normalized_source_sql} = ?"));
        assert_eq!(param.as_deref(), Some("local"));

        let mut sql = "SELECT 1 WHERE 1=1".to_string();
        let mut params = Vec::new();
        append_source_filter_condition(&mut sql, &mut params, &SourceFilter::Remote);
        let normalized_joined_origin_kind_sql =
            normalized_source_origin_kind_sql_expr("c.source_id", "s.kind", "c.origin_host");
        assert_eq!(
            sql,
            format!(
                "SELECT 1 WHERE 1=1 AND {normalized_joined_origin_kind_sql} != '{local}'",
                local = crate::sources::provenance::LOCAL_SOURCE_ID,
            )
        );
        assert!(params.is_empty());
    }

    #[test]
    fn stats_source_rows_match_the_messages_join_with_and_without_the_autoindex() {
        // (conversation id, source_id, origin_host, messages)
        let seed: [(i64, Option<&str>, Option<&str>, i64); 6] = [
            (1, None, None, 3),
            (2, Some("local"), None, 0),
            (3, Some("   "), None, 2),
            (4, Some("laptop"), Some("user@laptop"), 4),
            (5, Some(" laptop "), Some("user@laptop"), 1),
            (6, Some(""), Some("srv"), 0),
        ];
        // Modern archives carry UNIQUE(conversation_id, idx), whose autoindex
        // the streaming pass reads; historical ones fall back to a table scan.
        for unique_index in [true, false] {
            let tmp = TempDir::new().expect("temp dir");
            let db_path = tmp.path().join("agent_search.db");
            with_sqlite_connection(&db_path, "stats source rows test", |conn| {
                conn.execute("PRAGMA application_id = 0x43415353;")?;
                conn.execute("PRAGMA user_version = 1;")?;
                conn.execute(
                    "CREATE TABLE conversations (id INTEGER PRIMARY KEY, source_id TEXT, origin_host TEXT)",
                )?;
                conn.execute(if unique_index {
                    "CREATE TABLE messages (id INTEGER PRIMARY KEY, conversation_id INTEGER NOT NULL, \
                     idx INTEGER NOT NULL, UNIQUE(conversation_id, idx))"
                } else {
                    "CREATE TABLE messages (id INTEGER PRIMARY KEY, conversation_id INTEGER NOT NULL, idx INTEGER)"
                })?;
                for (id, source_id, origin_host, messages) in seed {
                    conn.execute_compat(
                        "INSERT INTO conversations(id, source_id, origin_host) VALUES (?1, ?2, ?3)",
                        &[id.into(), source_id.into(), origin_host.into()],
                    )?;
                    for idx in 0..messages {
                        conn.execute_compat(
                            "INSERT INTO messages(conversation_id, idx) VALUES (?1, ?2)",
                            &[id.into(), idx.into()],
                        )?;
                    }
                }
                Ok(())
            })
            .expect("seed stats source rows db");

            let conn = open_franken_analytics_db(&None, Some(&db_path)).expect("open db");
            let normalized = normalized_source_identity_sql_expr("c.source_id", "c.origin_host");
            let mut oracle: Vec<(String, i64, i64)> = conn
                .query_map_collect(
                    &format!(
                        "SELECT {normalized}, COUNT(DISTINCT c.id), COUNT(m.id) FROM conversations c \
                         LEFT JOIN messages m ON m.conversation_id = c.id GROUP BY {normalized}"
                    ),
                    &[],
                    |r: &crate::franken_sync::Row| {
                        Ok((r.get_typed(0)?, r.get_typed(1)?, r.get_typed(2)?))
                    },
                )
                .expect("old join query");
            oracle.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            let conversation_sources: Vec<(i64, String)> = conn
                .query_map_collect(
                    &format!("SELECT c.id, {normalized} FROM conversations c"),
                    &[],
                    |r: &crate::franken_sync::Row| Ok((r.get_typed(0)?, r.get_typed(1)?)),
                )
                .expect("conversation sources");
            let rows = stats_source_rows(&conn, conversation_sources).expect("source rows");

            assert_eq!(rows, oracle, "unique_index={unique_index}");
            assert_eq!(
                rows,
                [
                    ("local".to_string(), 3, 5),
                    ("laptop".to_string(), 2, 5),
                    ("srv".to_string(), 1, 0),
                ],
                "unique_index={unique_index}"
            );
            close_franken_cli_read_db(conn, &db_path, "stats source rows test").expect("close db");
        }
    }

    #[test]
    fn stats_message_count_sql_skips_join_when_unfiltered() {
        assert_eq!(
            stats_message_count_sql("", ""),
            "SELECT COUNT(*) FROM messages",
            "unfiltered stats can count messages directly without a conversation join"
        );
        assert_eq!(
            stats_workspace_count_sql("", ""),
            "SELECT c.workspace_id, COUNT(*) FROM conversations c WHERE c.workspace_id IS NOT NULL GROUP BY c.workspace_id",
            "unfiltered workspace stats can aggregate workspace IDs before path lookup"
        );

        let source_join = " LEFT JOIN sources s ON s.id = c.source_id";
        let (where_sql, _param) = source_filter_where_clause_with_columns(
            Some(&SourceFilter::Local),
            "c.source_id",
            "s.kind",
            "c.origin_host",
        );
        let sql = stats_message_count_sql(source_join, &where_sql);
        assert!(
            sql.contains("JOIN conversations c ON m.conversation_id = c.id"),
            "source-filtered stats must retain the conversation join; sql={sql}"
        );
        assert!(
            sql.contains(source_join),
            "kind-filtered stats must join registered source metadata; sql={sql}"
        );
        assert!(
            sql.ends_with(&where_sql),
            "source filter should be appended unchanged; sql={sql}"
        );

        let workspace_sql = stats_workspace_count_sql(source_join, &where_sql);
        assert!(
            workspace_sql.contains(" AND c.workspace_id IS NOT NULL"),
            "source-filtered workspace stats append the non-null guard after the source WHERE; sql={workspace_sql}"
        );
    }

    #[test]
    fn stats_grouping_preserves_sql_results_with_large_unused_payloads() {
        use crate::franken_sync::compat::{ConnectionExt as _, RowExt as _};

        fn assert_fold_matches_sql(conn: &crate::franken_sync::Connection) {
            let folded = unfiltered_conversation_stats(conn).expect("stream real statistics");
            let count: i64 = conn
                .query_row_map("SELECT COUNT(*) FROM conversations", &[], |row| {
                    row.get_typed(0)
                })
                .expect("SQL count oracle");
            assert_eq!(folded.count, count);
            let dates: (Option<i64>, Option<i64>) = conn
                .query_row_map(
                    "SELECT MIN(started_at), MAX(started_at) FROM conversations WHERE started_at IS NOT NULL",
                    &[],
                    |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
                )
                .expect("SQL date oracle");
            assert_eq!((folded.oldest, folded.newest), dates);
            let mut agents: Vec<(Option<i64>, i64)> = conn
                .query_map_collect(
                    "SELECT agent_id, COUNT(*) FROM conversations GROUP BY agent_id ORDER BY COUNT(*) DESC",
                    &[],
                    |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
                )
                .expect("SQL agent oracle");
            let mut folded_agents: Vec<_> = folded.agents.into_iter().collect();
            sort_stats_counts(&mut agents);
            sort_stats_counts(&mut folded_agents);
            assert_eq!(folded_agents, agents);
            let mut workspaces: Vec<(i64, i64)> = conn
                .query_map_collect(
                    "SELECT workspace_id, COUNT(*) FROM conversations WHERE workspace_id IS NOT NULL GROUP BY workspace_id ORDER BY COUNT(*) DESC",
                    &[],
                    |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
                )
                .expect("SQL workspace oracle");
            let mut folded_workspaces: Vec<_> = folded.workspaces.into_iter().collect();
            sort_stats_counts(&mut workspaces);
            sort_stats_counts(&mut folded_workspaces);
            assert_eq!(folded_workspaces, workspaces);
        }

        let tmp = tempfile::tempdir().expect("temporary archive");
        let path = tmp.path().join("stats.db");
        let writer = crate::franken_sync::Connection::open(path.to_string_lossy().into_owned())
            .expect("create archive");
        writer
            .execute("PRAGMA application_id = 0x43415353;")
            .expect("application_id");
        writer
            .execute("PRAGMA user_version = 1;")
            .expect("user_version");
        writer.execute("CREATE TABLE conversations (id INTEGER PRIMARY KEY, agent_id INTEGER, workspace_id INTEGER, source_id TEXT, origin_host TEXT, payload TEXT, started_at INTEGER)").expect("conversations");
        assert_fold_matches_sql(&writer);
        writer
            .execute("CREATE TABLE sources (id TEXT PRIMARY KEY, kind TEXT)")
            .expect("sources");
        writer.execute("INSERT INTO sources VALUES ('local', 'local'), ('remote-a', 'ssh'), ('registered-local', 'local')").expect("source kinds");
        let payload = "unused archive payload ".repeat(8192);
        for id in 1..=24_i64 {
            writer
                .execute_compat(
                    "INSERT INTO conversations VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    crate::franken_sync::params![
                        id,
                        (id % 4 != 0).then_some(id % 3),
                        (id % 5 != 0).then_some(id % 12),
                        match id % 3 {
                            0 => "local",
                            1 => "remote-a",
                            _ => "registered-local",
                        },
                        if id % 3 == 1 { Some("remote-a") } else { None },
                        payload.as_str(),
                        (id % 4 != 0).then_some((id - 12) * 1000)
                    ],
                )
                .expect("payload-heavy conversation");
        }
        assert_fold_matches_sql(&writer);
        writer
            .execute("UPDATE conversations SET started_at = NULL")
            .expect("plant all-NULL dates");
        assert_fold_matches_sql(&writer);
        writer
            .execute("UPDATE conversations SET started_at = CASE WHEN id % 4 != 0 THEN (id - 12) * 1000 ELSE NULL END")
            .expect("restore mixed pre-epoch and NULL dates");
        writer.close().expect("publish archive");
        let conn = open_franken_cli_read_db(path, "stats parity", Duration::from_secs(5))
            .expect("same reader as stats");
        assert_fold_matches_sql(&conn);
        for (label, sql, expect_order_by_temp_btree) in [
            (
                "old",
                format!("{} ORDER BY COUNT(*) DESC", stats_agent_count_sql("", "")),
                true,
            ),
            ("new", stats_agent_count_sql("", ""), false),
        ] {
            let plan_details = conn
                .query_map_collect(&format!("EXPLAIN QUERY PLAN {sql}"), &[], |row| {
                    row.get_typed::<String>(3)
                })
                .expect("explain grouped execution");
            let uses_order_by_temp_btree = plan_details
                .iter()
                .any(|detail| detail.contains("USE TEMP B-TREE FOR ORDER BY"));
            assert_eq!(
                uses_order_by_temp_btree, expect_order_by_temp_btree,
                "{label} plan details: {plan_details:?}"
            );
        }
        for filter in [
            None,
            Some(SourceFilter::Local),
            Some(SourceFilter::Remote),
            Some(SourceFilter::SourceId("remote-a".to_string())),
            Some(SourceFilter::SourceId("absent".to_string())),
        ] {
            let joined = matches!(filter, Some(SourceFilter::Local | SourceFilter::Remote));
            let join = if joined {
                " LEFT JOIN sources s ON s.id = c.source_id"
            } else {
                ""
            };
            let (condition, param) = source_filter_where_clause_with_columns(
                filter.as_ref(),
                "c.source_id",
                if joined { "s.kind" } else { "NULL" },
                "c.origin_host",
            );
            let params: Vec<crate::franken_sync::compat::ParamValue> =
                param.as_deref().map(Into::into).into_iter().collect();
            for sql in [
                stats_agent_count_sql(join, &condition),
                stats_workspace_count_sql(join, &condition),
            ] {
                let read = |query: &str| -> Vec<(Option<i64>, i64)> {
                    conn.query_map_collect(query, &params, |row| {
                        Ok((row.get_typed(0)?, row.get_typed(1)?))
                    })
                    .expect("execute real aggregate")
                };
                let mut old = read(&format!("{sql} ORDER BY COUNT(*) DESC"));
                let mut new = read(&sql);
                sort_stats_counts(&mut old);
                sort_stats_counts(&mut new);
                assert_eq!(new, old, "source-filtered aggregate parity: {sql}");
            }
        }
        let mut ties = vec![(Some(3), 2), (None, 2), (Some(1), 2), (Some(9), 3)];
        sort_stats_counts(&mut ties);
        assert_eq!(
            ties,
            vec![(Some(9), 3), (None, 2), (Some(1), 2), (Some(3), 2)]
        );
    }

    #[test]
    fn source_filter_helpers_match_blank_remote_source_id_via_origin_host() {
        let (_tmp, db_path, _shared_path) = make_legacy_local_db();
        let conn = open_legacy_local_db_for_update(&db_path);
        conn.execute("UPDATE conversations SET source_id = '   ' WHERE id = 2")
            .expect("blank remote source_id");

        let (where_sql, param) =
            source_filter_where_clause(Some(&SourceFilter::SourceId("user@laptop".to_string())));
        let sql = format!("SELECT COUNT(*) FROM conversations c{where_sql}");
        let params_vec = vec![param.expect("source id param").into()];
        let count: i64 = conn
            .query_row_map(&sql, &params_vec, |r: &crate::franken_sync::Row| {
                r.get_typed(0)
            })
            .expect("count blank remote source rows");
        assert_eq!(count, 1);

        let (where_sql, param) = source_filter_where_clause(Some(&SourceFilter::Remote));
        assert!(param.is_none());
        let sql = format!("SELECT COUNT(*) FROM conversations c{where_sql}");
        let count: i64 = conn
            .query_row_map(&sql, &[], |r: &crate::franken_sync::Row| r.get_typed(0))
            .expect("count remote source rows");
        assert_eq!(count, 1);

        conn.close().expect("close writable legacy source test db");
    }

    #[test]
    fn source_filter_helpers_classify_registered_named_local_source_by_kind() {
        let (_tmp, db_path, _shared_path) = make_legacy_local_db();
        let conn = open_legacy_local_db_for_update(&db_path);
        conn.execute(
            "INSERT INTO sources(id, kind, host_label) VALUES ('backup-local', 'local', NULL)",
        )
        .expect("insert named local source");
        conn.execute(
            "UPDATE conversations SET source_id = 'backup-local', origin_host = NULL WHERE id = 2",
        )
        .expect("move second conversation to named local source");

        let count_for = |filter: SourceFilter| {
            let (where_sql, param) = source_filter_where_clause_with_columns(
                Some(&filter),
                "c.source_id",
                "s.kind",
                "c.origin_host",
            );
            let sql = format!(
                "SELECT COUNT(*) FROM conversations c \
                 LEFT JOIN sources s ON s.id = c.source_id{where_sql}"
            );
            let params = param
                .as_deref()
                .map(crate::franken_sync::compat::ParamValue::from)
                .into_iter()
                .collect::<Vec<_>>();
            conn.query_row_map(&sql, &params, |row: &crate::franken_sync::Row| {
                row.get_typed::<i64>(0)
            })
            .expect("count source-filtered conversations")
        };

        assert_eq!(count_for(SourceFilter::Local), 2);
        assert_eq!(count_for(SourceFilter::Remote), 0);
        assert_eq!(
            count_for(SourceFilter::SourceId("backup-local".to_string())),
            1
        );

        conn.close().expect("close writable legacy source test db");
    }

    #[test]
    fn source_filter_helpers_match_trimmed_local_source_id_in_db() {
        let (_tmp, db_path, _shared_path) = make_legacy_local_db();
        let conn = open_legacy_local_db_for_update(&db_path);
        conn.execute("UPDATE conversations SET source_id = '  local  ' WHERE id = 1")
            .expect("update legacy local source_id");

        let (where_sql, param) =
            source_filter_where_clause(Some(&SourceFilter::SourceId("local".to_string())));
        let sql = format!("SELECT COUNT(*) FROM conversations c{where_sql}");
        let params_vec = vec![param.expect("source id param").into()];
        let count: i64 = conn
            .query_row_map(&sql, &params_vec, |r: &crate::franken_sync::Row| {
                r.get_typed(0)
            })
            .expect("count trimmed local source rows");
        assert_eq!(count, 1);

        let (where_sql, param) = source_filter_where_clause(Some(&SourceFilter::Local));
        assert!(param.is_none());
        let sql = format!("SELECT COUNT(*) FROM conversations c{where_sql}");
        let count: i64 = conn
            .query_row_map(&sql, &[], |r: &crate::franken_sync::Row| r.get_typed(0))
            .expect("count local source rows");
        assert_eq!(count, 1);

        conn.close().expect("close writable legacy source test db");
    }

    #[test]
    fn context_candidates_covering_plan_limits_and_legacy_nulls() {
        use crate::franken_sync::compat::ConnectionExt as _;

        let conn = crate::franken_sync::Connection::open(":memory:").unwrap();
        conn.execute_batch(
            "CREATE TABLE conversations (
                 id INTEGER PRIMARY KEY, workspace_id INTEGER, agent_id INTEGER,
                 started_at INTEGER, metadata_bin BLOB
             );
             INSERT INTO conversations(id, workspace_id, agent_id, started_at) VALUES
                 (1, 1, 1, 100), (2, 1, 2, 300), (3, 2, 1, 200),
                 (4, 1, 1, NULL), (5, 2, 2, 86400000), (6, NULL, NULL, 250),
                 (7, 1, 1, 300), (8, NULL, NULL, NULL);",
        )
        .unwrap();
        let source = ContextCandidate {
            id: 1,
            workspace_id: Some(1),
            agent_id: Some(1),
            started_at: Some(100),
        };
        let expected = [vec![2, 7, 4], vec![2, 7, 6, 3], vec![7, 3, 4]];
        assert_eq!(
            context_related_ids(&conn, &source, usize::MAX).unwrap(),
            expected
        );
        conn.execute(
            "CREATE INDEX idx_conversations_context
             ON conversations(started_at DESC, workspace_id, agent_id)",
        )
        .unwrap();
        conn.execute("PRAGMA query_only = 1").unwrap();
        assert_eq!(
            context_related_ids(&conn, &source, usize::MAX).unwrap(),
            expected
        );
        assert_eq!(
            context_related_ids(&conn, &source, 1).unwrap(),
            [vec![2], vec![2], vec![7]]
        );
        assert_eq!(
            context_related_ids(&conn, &source, 0).unwrap(),
            [Vec::<i64>::new(), vec![], vec![]]
        );
        let null_source = ContextCandidate {
            id: 8,
            workspace_id: None,
            agent_id: None,
            started_at: None,
        };
        assert_eq!(
            context_related_ids(&conn, &null_source, 10).unwrap(),
            [Vec::<i64>::new(), vec![], vec![]]
        );

        let index_root: i64 = conn
            .query_row_map(
                "SELECT rootpage FROM sqlite_master WHERE name = 'idx_conversations_context'",
                &[],
                |row| row.get_typed(0),
            )
            .unwrap();
        let explain = conn
            .query(&format!("EXPLAIN {CONTEXT_CANDIDATES_SQL}"))
            .unwrap();
        let mut read_roots = Vec::new();
        for row in explain {
            let opcode: String = row.get_typed(1).unwrap();
            assert!(
                !matches!(opcode.as_str(), "SeekRowid" | "SorterOpen"),
                "{opcode}"
            );
            if opcode == "OpenRead" {
                read_roots.push(row.get_typed::<i64>(3).unwrap());
            }
        }
        assert_eq!(
            read_roots,
            vec![index_root],
            "candidate scan must open only the covering index"
        );
        // Negative control: projecting the unindexed `metadata_bin` column (or
        // disabling the index) reads the conversation table, so the EXPLAIN
        // check genuinely detects non-covering table scans.
        for negative_sql in [
            "EXPLAIN SELECT id, workspace_id, agent_id, started_at, metadata_bin FROM conversations",
            "EXPLAIN SELECT id, workspace_id, agent_id, started_at FROM conversations NOT INDEXED",
        ] {
            let unhinted = conn.query(negative_sql).unwrap();
            assert!(
                unhinted.iter().any(|row| {
                    row.get_typed::<String>(1).unwrap() == "OpenRead"
                        && row.get_typed::<i64>(3).unwrap() != index_root
                }),
                "negative control did not open conversations table: {negative_sql}"
            );
        }
    }

    #[test]
    fn find_context_source_conversation_treats_null_source_as_local() {
        let (_tmp, db_path, shared_path) = make_legacy_local_db();
        let conn = open_franken_analytics_db(&None, Some(&db_path)).expect("open analytics db");

        let row =
            find_context_source_conversation(&conn, &shared_path.to_string_lossy(), Some("local"))
                .expect("legacy local row should resolve");
        assert_eq!(row.4, "Local Session");
        assert_eq!(row.6, "local");

        close_franken_cli_read_db(conn, &db_path, "legacy source test").expect("close db");
    }

    #[test]
    fn find_context_source_conversation_normalizes_trimmed_source_id() {
        let (_tmp, db_path, shared_path) = make_legacy_local_db();
        let conn = open_franken_analytics_db(&None, Some(&db_path)).expect("open analytics db");

        let row = find_context_source_conversation(
            &conn,
            &shared_path.to_string_lossy(),
            Some("  LOCAL  "),
        )
        .expect("trimmed local source should resolve");
        assert_eq!(row.4, "Local Session");
        assert_eq!(row.6, "local");

        close_franken_cli_read_db(conn, &db_path, "legacy source test").expect("close db");
    }

    #[test]
    fn find_context_source_conversation_matches_blank_remote_source_id_via_origin_host() {
        let (_tmp, db_path, shared_path) = make_legacy_local_db();
        let conn = open_legacy_local_db_for_update(&db_path);
        conn.execute("UPDATE conversations SET source_id = '   ' WHERE id = 2")
            .expect("blank remote source_id");

        let row = find_context_source_conversation(
            &conn,
            &shared_path.to_string_lossy(),
            Some("user@laptop"),
        )
        .expect("blank remote source should resolve via origin host");
        assert_eq!(row.4, "Remote Session");
        assert_eq!(row.6, "user@laptop");

        conn.close().expect("close writable legacy source test db");
    }

    #[test]
    fn find_context_source_conversation_matches_trimmed_local_source_id_in_db() {
        let (_tmp, db_path, shared_path) = make_legacy_local_db();
        let conn = open_legacy_local_db_for_update(&db_path);
        conn.execute("UPDATE conversations SET source_id = '  local  ' WHERE id = 1")
            .expect("update legacy local source_id");

        let row =
            find_context_source_conversation(&conn, &shared_path.to_string_lossy(), Some("local"))
                .expect("trimmed local source stored in db should resolve");
        assert_eq!(row.4, "Local Session");
        assert_eq!(row.6, "local");

        let preferred =
            find_context_source_conversation(&conn, &shared_path.to_string_lossy(), None)
                .expect("path-only lookup should still prefer normalized local row");
        assert_eq!(preferred.4, "Local Session");
        assert_eq!(preferred.6, "local");

        conn.close().expect("close writable legacy source test db");
    }

    #[test]
    fn context_and_stats_queries_surface_null_source_id_as_local() {
        let (_tmp, db_path, shared_path) = make_legacy_local_db();
        let conn = open_franken_analytics_db(&None, Some(&db_path)).expect("open analytics db");

        let selected =
            find_context_source_conversation(&conn, &shared_path.to_string_lossy(), None)
                .expect("shared path should resolve");
        assert_eq!(selected.4, "Local Session");
        assert_eq!(selected.6, "local");

        let normalized_source_sql = normalized_source_id_sql_expr("c.source_id");
        let source_sql = format!(
            "SELECT {normalized_source_sql} as source_id, COUNT(DISTINCT c.id)
             FROM conversations c
             GROUP BY {normalized_source_sql}
             ORDER BY source_id"
        );
        let source_rows: Vec<(String, i64)> = conn
            .query_map_collect(&source_sql, &[], |r: &crate::franken_sync::Row| {
                Ok((r.get_typed(0)?, r.get_typed(1)?))
            })
            .expect("query source rows");
        assert!(
            source_rows
                .iter()
                .any(|(source_id, count)| source_id == "local" && *count == 1)
        );
        assert!(
            source_rows
                .iter()
                .any(|(source_id, count)| source_id == "laptop" && *count == 1)
        );

        let timeline_rows: Vec<String> = conn
            .query_map_collect(
                "SELECT COALESCE(c.source_id, 'local')
                 FROM conversations c
                 JOIN agents a ON c.agent_id = a.id
                 LEFT JOIN messages m ON m.conversation_id = c.id
                 WHERE c.started_at >= ?1 AND c.started_at <= ?2
                 GROUP BY c.id
                 ORDER BY c.started_at DESC",
                &[1_699_999_999_000_i64.into(), 1_700_000_200_000_i64.into()],
                |r: &crate::franken_sync::Row| r.get_typed(0),
            )
            .expect("query timeline rows");
        assert!(timeline_rows.iter().any(|source_id| source_id == "local"));

        close_franken_cli_read_db(conn, &db_path, "legacy source test").expect("close db");
    }

    #[test]
    fn stats_source_grouping_uses_origin_host_when_remote_source_id_blank() {
        let (_tmp, db_path, _shared_path) = make_legacy_local_db();
        let conn = open_legacy_local_db_for_update(&db_path);
        conn.execute("UPDATE conversations SET source_id = '   ' WHERE id = 2")
            .expect("blank remote source_id");

        let normalized_source_sql =
            normalized_source_identity_sql_expr("c.source_id", "c.origin_host");
        let source_sql = format!(
            "SELECT {normalized_source_sql} as source_id, COUNT(DISTINCT c.id)
             FROM conversations c
             GROUP BY {normalized_source_sql}
             ORDER BY source_id"
        );
        let source_rows: Vec<(String, i64)> = conn
            .query_map_collect(&source_sql, &[], |r: &crate::franken_sync::Row| {
                Ok((r.get_typed(0)?, r.get_typed(1)?))
            })
            .expect("query normalized source rows");

        assert!(
            source_rows
                .iter()
                .any(|(source_id, count)| source_id == "user@laptop" && *count == 1),
            "expected blank remote source to group under origin host, got: {source_rows:?}"
        );

        conn.close().expect("close writable legacy source test db");
    }

    #[test]
    fn stats_source_grouping_normalizes_trimmed_local_source_id_in_db() {
        let (_tmp, db_path, _shared_path) = make_legacy_local_db();
        let conn = open_legacy_local_db_for_update(&db_path);
        conn.execute("UPDATE conversations SET source_id = '  local  ' WHERE id = 1")
            .expect("update legacy local source_id");

        let normalized_source_sql = normalized_source_id_sql_expr("c.source_id");
        let source_sql = format!(
            "SELECT {normalized_source_sql} as source_id, COUNT(DISTINCT c.id)
             FROM conversations c
             GROUP BY {normalized_source_sql}
             ORDER BY source_id"
        );
        let source_rows: Vec<(String, i64)> = conn
            .query_map_collect(&source_sql, &[], |r: &crate::franken_sync::Row| {
                Ok((r.get_typed(0)?, r.get_typed(1)?))
            })
            .expect("query normalized source rows");

        assert!(
            source_rows
                .iter()
                .any(|(source_id, count)| source_id == "local" && *count == 1),
            "expected trimmed local source to group under local, got: {source_rows:?}"
        );
        assert!(
            !source_rows
                .iter()
                .any(|(source_id, _)| source_id.trim() != source_id || source_id.is_empty()),
            "expected no raw trimmed/blank source ids, got: {source_rows:?}"
        );

        conn.close().expect("close writable legacy source test db");
    }
}
