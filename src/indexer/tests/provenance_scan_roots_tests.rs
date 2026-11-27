use super::super::*;
use super::helpers::*;
use crate::connectors::{Connector, NormalizedConversation, ScanContext};
use crate::sources::provenance::SourceKind;
use serial_test::serial;
use tempfile::TempDir;

#[test]
#[serial]
fn reindex_paths_watch_once_does_not_advance_persistent_watch_state() {
    let tmp = TempDir::new().unwrap();
    let xdg = tmp.path().join("xdg_watch_once_state_isolation");
    std::fs::create_dir_all(&xdg).unwrap();
    let prev = dotenvy::var("XDG_DATA_HOME").ok();
    unsafe { std::env::set_var("XDG_DATA_HOME", &xdg) };

    let data_dir = xdg.join("codex");
    std::fs::create_dir_all(&data_dir).unwrap();
    let codex_dir = data_dir.join("codex/sessions");
    std::fs::create_dir_all(&codex_dir).unwrap();
    let codex_file = codex_dir.join("rollout-watch-once.jsonl");
    write_watch_codex_messages(
        &codex_file,
        "watch-once",
        &[("user", "p", 1_700_000_000_100)],
    );

    let persisted_ts = 123_456_i64;
    let mut persisted_state = HashMap::new();
    persisted_state.insert(ConnectorKind::Codex, persisted_ts);
    save_watch_state(&data_dir, &persisted_state).unwrap();

    let opts = crate::indexer::IndexOptions {
        full: false,
        watch: false,
        force_rebuild: false,
        watch_once_paths: Some(vec![codex_file.clone()]),
        db_path: data_dir.join("db.sqlite"),
        data_dir: data_dir.clone(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: None,
        watch_interval_secs: 30,
    };

    let storage = FrankenStorage::open(&opts.db_path).unwrap();
    let index_path = index_dir(&opts.data_dir).unwrap();
    let t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let state = Mutex::new(persisted_state.clone());
    let storage = Mutex::new(storage);
    let t_index = Mutex::new(Some(t_index));

    let indexed = reindex_paths(
        &opts,
        vec![codex_file],
        &[(ConnectorKind::Codex, ScanRoot::local(codex_dir))],
        &state,
        &storage,
        &t_index,
        &index_path,
        false,
    )
    .unwrap();
    assert_eq!(indexed, 1);

    let in_memory = state.lock().unwrap();
    assert_eq!(
        in_memory.get(&ConnectorKind::Codex).copied(),
        Some(persisted_ts)
    );
    drop(in_memory);

    let loaded = load_watch_state(&data_dir);
    assert_eq!(
        loaded.get(&ConnectorKind::Codex).copied(),
        Some(persisted_ts)
    );

    if let Some(prev) = prev {
        unsafe { std::env::set_var("XDG_DATA_HOME", prev) };
    } else {
        unsafe { std::env::remove_var("XDG_DATA_HOME") };
    }
}

// P2.2 Tests: Multi-root orchestration and provenance injection

#[test]
fn inject_provenance_adds_cass_origin_to_metadata() {
    let mut conv = norm_conv(Some("test"), vec![norm_msg(0, 100)]);
    assert!(conv.metadata.get("cass").is_none());

    let origin = Origin::local();
    inject_provenance(&mut conv, &origin);

    let cass = conv.metadata.get("cass").expect("cass field should exist");
    let origin_obj = cass.get("origin").expect("origin should exist");
    assert_eq!(origin_obj.get("source_id").unwrap().as_str(), Some("local"));
    assert_eq!(origin_obj.get("kind").unwrap().as_str(), Some("local"));
}

#[test]
fn inject_provenance_handles_remote_origin() {
    let mut conv = norm_conv(Some("test"), vec![norm_msg(0, 100)]);

    let origin = Origin::remote_with_host("laptop", "user@laptop.local");
    inject_provenance(&mut conv, &origin);

    let cass = conv.metadata.get("cass").expect("cass field should exist");
    let origin_obj = cass.get("origin").expect("origin should exist");
    assert_eq!(
        origin_obj.get("source_id").unwrap().as_str(),
        Some("laptop")
    );
    assert_eq!(origin_obj.get("kind").unwrap().as_str(), Some("ssh"));
    assert_eq!(
        origin_obj.get("host").unwrap().as_str(),
        Some("user@laptop.local")
    );
}

#[test]
fn inject_provenance_repairs_non_object_cass_metadata() {
    let mut conv = norm_conv(Some("test"), vec![norm_msg(0, 100)]);
    conv.metadata = serde_json::json!({
        "cass": "connector supplied a malformed cass metadata value",
        "other": "preserve me"
    });

    let origin = Origin::remote_with_host("laptop", "user@laptop.local");
    inject_provenance(&mut conv, &origin);

    assert_eq!(
        conv.metadata.pointer("/other"),
        Some(&serde_json::json!("preserve me"))
    );
    assert_eq!(
        conv.metadata.pointer("/cass/origin/source_id"),
        Some(&serde_json::json!("laptop"))
    );
    assert_eq!(
        conv.metadata.pointer("/cass/origin/kind"),
        Some(&serde_json::json!("ssh"))
    );
    assert_eq!(
        conv.metadata.pointer("/cass/origin/host"),
        Some(&serde_json::json!("user@laptop.local"))
    );
}

#[test]
fn large_codex_session_keeps_full_extras_end_to_end_and_is_not_extra_compacted() {
    // Owner decision 2026-11-08: the 16 MiB codex extra-compaction layer
    // is gone. A session past the former threshold keeps full per-message
    // extras through ingest, and its viewer facts must not carry the
    // `extra_compacted` partial verdict.
    const FORMER_COMPACT_THRESHOLD_BYTES: usize = 16 * 1024 * 1024;

    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let sessions = temp.path().join("sessions/2026/11/08");
    std::fs::create_dir_all(&sessions).expect("create sessions dir");
    let source_path = sessions.join("rollout-large.jsonl");

    let records = concat!(
        "{\"timestamp\":\"2026-11-08T10:00:00.000Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"large-extra-id\",\"cwd\":\"/fixture/large\",\"cli_version\":\"0.49.0\"}}\n",
        "{\"timestamp\":\"2026-11-08T10:00:01.000Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"run the large sweep\"}}\n",
        "{\"timestamp\":\"2026-11-08T10:00:02.000Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"name\":\"exec_command\",\"call_id\":\"call_1\",\"arguments\":\"{\\\"command\\\":[\\\"ls\\\",\\\"-la\\\"]}\"}}\n",
        "{\"timestamp\":\"2026-11-08T10:00:03.000Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"function_call_output\",\"call_id\":\"call_1\",\"output\":\"file-a\\nfile-b\"}}\n",
        "{\"timestamp\":\"2026-11-08T10:00:04.000Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"sweep complete\"}]}}\n",
        "{\"timestamp\":\"2026-11-08T10:00:05.000Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"agent_reasoning\",\"text\":\"planning the sweep\"}}\n",
    );
    // Whitespace-only padding grows the source past the former threshold
    // without adding records the connector must parse.
    let padding = (FORMER_COMPACT_THRESHOLD_BYTES + 1).saturating_sub(records.len());
    let mut body = String::with_capacity(records.len() + padding + 1);
    body.push_str(records);
    body.push_str(&" ".repeat(padding));
    body.push('\n');
    std::fs::write(&source_path, body).expect("write large rollout");
    let source_len = std::fs::metadata(&source_path)
        .expect("stat large rollout")
        .len();
    assert!(
        source_len as usize > FORMER_COMPACT_THRESHOLD_BYTES,
        "fixture must exceed the former threshold (got {source_len})"
    );

    let connector = crate::connectors::codex::CodexConnector::new();
    let ctx = ScanContext::with_roots(
        temp.path().to_path_buf(),
        vec![crate::connectors::ScanRoot::local(
            temp.path().to_path_buf(),
        )],
        None,
    );
    let conversations = connector.scan(&ctx).expect("scan large rollout");
    assert_eq!(conversations.len(), 1);
    let mut conv = conversations
        .into_iter()
        .next()
        .expect("one conversation from the large rollout");

    prepare_conversation_for_ingest(
        &data_dir,
        "codex",
        &Origin::local(),
        None,
        &mut conv,
        &RawSourceExclusions::default(),
    );

    // End-to-end half: every message keeps the raw record (its `payload`)
    // instead of a `{"cass":{…}}` skeleton.
    assert!(!conv.messages.is_empty());
    for message in &conv.messages {
        assert!(
            message.extra.get("payload").is_some(),
            "message {} must keep its raw record payload after ingest; extra: {}",
            message.idx,
            message.extra
        );
    }

    // The raw mirror still sizes and captures the whole padded source.
    assert_eq!(
        conv.metadata.pointer("/cass/raw_mirror/source_size_bytes"),
        Some(&serde_json::json!(source_len))
    );

    // Viewer-facts half: the session must not be classified as
    // `extra_compacted`, the text inventory must be complete and the
    // tool call must pair with its recorded result.
    let packet = crate::connectors::packet_bridge::conversation_packet_from_normalized(
        &conv,
        crate::model::conversation_packet::ConversationPacketProvenance::local(),
    );
    let publication = crate::viewer_extensions::facts::extract_session_facts(&packet, "codex");
    assert!(
        !publication.coverage.iter().any(|entry| entry
            .reasons
            .iter()
            .any(|reason| reason == "extra_compacted")),
        "no scope may report the extra_compacted verdict: {:?}",
        publication.coverage
    );
    let text = publication
        .coverage
        .iter()
        .find(|entry| entry.field == "text_inventory")
        .expect("text_inventory coverage");
    assert_eq!(text.state, "complete", "text_inventory: {text:?}");
    let tools = publication
        .coverage
        .iter()
        .find(|entry| entry.field == "tools")
        .expect("tools coverage");
    assert_eq!(tools.state, "complete", "tools: {tools:?}");
    let exec = publication
        .tools
        .iter()
        .find(|fact| fact.raw_call_id == "call_1")
        .expect("exec_command tool fact");
    assert!(
        exec.has_result,
        "the tool call must pair with its recorded result: {exec:?}"
    );
}

#[test]
fn claude_external_id_canonicalized_so_full_and_watch_paths_dedupe() {
    // gh #302: the full-rebuild path roots Claude at `~/.claude/projects`
    // (bare relative id) while the live `--watch` daemon roots at
    // `~/.claude` (detect_installed_agents reports `~/.claude`), yielding a
    // `projects/`-prefixed id for the SAME session file. Canonicalization at
    // the ingest chokepoint must collapse both to one stable dedup key.
    let rel = "-Users-mrm-Developer-proj/7b85f067-dead-beef-cafe-000000000000.jsonl";

    // Full-rebuild form (bare).
    let mut bare = norm_conv(Some(rel), vec![]);
    bare.agent_slug = "claude".into();
    canonicalize_claude_external_id("claude", &mut bare);

    // Watch-daemon form (projects/-prefixed) for the identical session.
    let mut prefixed = norm_conv(Some(&format!("projects/{rel}")), vec![]);
    prefixed.agent_slug = "claude".into();
    canonicalize_claude_external_id("claude", &mut prefixed);

    assert_eq!(
        bare.external_id, prefixed.external_id,
        "both ingest paths must produce the same external_id for the same session"
    );
    assert_eq!(
        bare.external_id.as_deref(),
        Some(rel),
        "canonical form is the bare full-rebuild rooting"
    );
}

#[test]
fn claude_external_id_canonicalize_is_scoped_and_conservative() {
    // Only the `claude` connector is touched; other connectors that may
    // legitimately carry a leading `projects/` segment are left untouched.
    let mut codex = norm_conv(Some("projects/foo/bar.jsonl"), vec![]);
    canonicalize_claude_external_id("codex", &mut codex);
    assert_eq!(codex.external_id.as_deref(), Some("projects/foo/bar.jsonl"));

    // A degenerate id with no workspace segment after `projects/` is left
    // alone — stripping it would invent a bare uuid id that never collides
    // with a real `<workspace>/<uuid>.jsonl` key, so this can only be a
    // genuinely different id (defensive: never over-strip).
    let mut shallow = norm_conv(Some("projects/loose.jsonl"), vec![]);
    canonicalize_claude_external_id("claude", &mut shallow);
    assert_eq!(shallow.external_id.as_deref(), Some("projects/loose.jsonl"));

    // Already-bare ids are unchanged.
    let mut bare = norm_conv(Some("-Users-x-proj/abc.jsonl"), vec![]);
    canonicalize_claude_external_id("claude", &mut bare);
    assert_eq!(bare.external_id.as_deref(), Some("-Users-x-proj/abc.jsonl"));

    // None stays None.
    let mut empty = norm_conv(None, vec![]);
    canonicalize_claude_external_id("claude", &mut empty);
    assert_eq!(empty.external_id, None);
}

#[test]
fn extract_provenance_returns_local_for_empty_metadata() {
    let conv = persist::map_to_internal(&NormalizedConversation {
        agent_slug: "test".into(),
        external_id: None,
        title: None,
        workspace: None,
        source_path: PathBuf::from("/test"),
        started_at: None,
        ended_at: None,
        metadata: serde_json::json!({}),
        messages: vec![],
    });
    assert_eq!(conv.source_id, "local");
    assert!(conv.origin_host.is_none());
}

#[test]
fn extract_provenance_extracts_remote_origin() {
    let metadata = serde_json::json!({
        "cass": {
            "origin": {
                "source_id": "laptop",
                "kind": "ssh",
                "host": "user@laptop.local"
            }
        }
    });
    let conv = persist::map_to_internal(&NormalizedConversation {
        agent_slug: "test".into(),
        external_id: None,
        title: None,
        workspace: None,
        source_path: PathBuf::from("/test"),
        started_at: None,
        ended_at: None,
        metadata,
        messages: vec![],
    });
    assert_eq!(conv.source_id, "laptop");
    assert_eq!(conv.origin_host, Some("user@laptop.local".to_string()));
}

#[test]
fn extract_provenance_infers_remote_source_from_host_without_source_id() {
    let metadata = serde_json::json!({
        "cass": {
            "origin": {
                "source_id": "   ",
                "host": "user@laptop.local"
            }
        }
    });
    let conv = persist::map_to_internal(&NormalizedConversation {
        agent_slug: "test".into(),
        external_id: None,
        title: None,
        workspace: None,
        source_path: PathBuf::from("/test"),
        started_at: None,
        ended_at: None,
        metadata,
        messages: vec![],
    });
    assert_eq!(conv.source_id, "user@laptop.local");
    assert_eq!(conv.origin_host, Some("user@laptop.local".to_string()));
}

#[test]
#[serial]
fn build_scan_roots_creates_local_root() {
    let _guard = ignore_sources_config();
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();

    let roots = build_scan_roots(Some(&storage), &data_dir);

    // Should have at least the local root
    assert!(!roots.is_empty());
    assert_eq!(roots[0].origin.source_id, "local");
    assert!(!roots[0].origin.is_remote());
}

#[test]
#[serial]
fn build_scan_roots_includes_remote_mirror_if_exists() {
    let _guard = ignore_sources_config();
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    // Create a remote source in the database
    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();

    // Register a remote source
    storage
        .upsert_source(&crate::sources::provenance::Source {
            id: "laptop".to_string(),
            kind: SourceKind::Ssh,
            host_label: Some("user@laptop.local".to_string()),
            machine_id: None,
            platform: Some("linux".to_string()),
            config_json: None,
            created_at: None,
            updated_at: None,
        })
        .unwrap();

    // Create the mirror directory
    let mirror_dir = data_dir.join("remotes").join("laptop").join("mirror");
    std::fs::create_dir_all(&mirror_dir).unwrap();

    let roots = build_scan_roots(Some(&storage), &data_dir);

    // Should have local root + remote root
    assert_eq!(roots.len(), 2);

    // Find the remote root
    let remote_root = roots.iter().find(|r| r.origin.source_id == "laptop");
    assert!(remote_root.is_some());
    let remote_root = remote_root.unwrap();
    assert!(remote_root.origin.is_remote());
    assert_eq!(
        remote_root.origin.host,
        Some("user@laptop.local".to_string())
    );
    assert_eq!(remote_root.platform, Some(Platform::Linux));
}

#[test]
#[serial]
fn build_scan_roots_preserves_remote_mirror_path_keys() {
    let _guard = ignore_sources_config();
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let storage = FrankenStorage::open(&data_dir.join("db.sqlite")).unwrap();

    for path in [
        "~",
        "~/.codex/sessions",
        "/home/operator/.codex/sessions",
        "~/.factory/sessions",
        "~/.config/Cursor/User",
        "~/.gemini/tmp",
        "~/.vibe/logs",
        "~/.prime/agent/sessions",
    ] {
        storage
            .upsert_source(&crate::sources::provenance::Source {
                id: "laptop".to_string(),
                kind: SourceKind::Ssh,
                host_label: Some("operator@laptop.invalid".to_string()),
                machine_id: None,
                platform: Some("linux".to_string()),
                config_json: Some(serde_json::json!({"paths": [path]})),
                created_at: None,
                updated_at: None,
            })
            .unwrap();
        let mirror = data_dir
            .join("remotes/laptop/mirror")
            .join(path_to_safe_dirname(path));
        std::fs::create_dir_all(&mirror).unwrap();

        let roots = build_scan_roots(Some(&storage), &data_dir);
        assert_eq!(roots.len(), 2, "configured mirror missing for {path}");
        let remote = roots.iter().find(|root| root.origin.is_remote()).unwrap();
        assert_eq!(remote.path, mirror);
        assert_eq!(remote.origin.source_id, "laptop");
        assert_eq!(remote.mirrored_source_path.as_deref(), Some(path));
    }
}

#[test]
fn mirrored_roots_are_scanned_only_by_the_connectors_that_own_their_store() {
    let remote = |path: &str| {
        ScanRoot::remote(
            PathBuf::from("/cass/remotes/laptop/mirror/opaque_0123456789abcdef"),
            Origin::remote_with_host("laptop", "user@laptop"),
            None,
        )
        .with_mirrored_source_path(path)
    };
    let cases: [(&str, &[&str]); 13] = [
        ("~/.claude/projects", &["claude"]),
        ("~/.codex/sessions", &["codex"]),
        ("~/.pi/agent/sessions", &["pi_agent", "pi_durable"]),
        ("~/.prime/agent/sessions", &["prime_agent"]),
        ("~/.omp/agent/sessions", &["omp"]),
        ("~/.local/share/omp", &["omp"]),
        ("~/.factory/sessions", &["factory"]),
        ("~/.vibe/logs/session", &["vibe"]),
        ("~/.gemini/tmp", &["gemini", "antigravity"]),
        ("/home/u/.gemini/antigravity", &["antigravity"]),
        ("~/.config/Cursor/User", &["cursor", "cline"]),
        (
            "~/.config/Cursor/User/globalStorage/saoudrizwan.claude-dev",
            &["cline"],
        ),
        (r"C:\Users\u\.claude\projects", &["claude"]),
    ];
    for (path, owners) in cases {
        let root = remote(path);
        assert_eq!(mirrored_root_owners(&root), Some(owners), "{path}");
        for connector in [
            "claude",
            "pi_agent",
            "prime_agent",
            "factory",
            "gemini",
            "cline",
        ] {
            assert_eq!(
                mirrored_root_may_hold(connector, &root),
                owners.contains(&connector),
                "{connector} on {path}"
            );
        }
    }
    // The misattributions this prevents.
    assert!(!mirrored_root_may_hold(
        "claude",
        &remote("~/.pi/agent/sessions")
    ));
    assert!(!mirrored_root_may_hold(
        "claude",
        &remote("~/.factory/sessions")
    ));
    assert!(!mirrored_root_may_hold(
        "pi_agent",
        &remote("~/.prime/agent/sessions")
    ));
    // A path naming no known store, an unannotated remote root and a local
    // root keep every connector.
    for root in [
        remote("/backups/claude-sessions"),
        remote("~"),
        ScanRoot::remote(
            PathBuf::from("/cass/remotes/laptop/mirror"),
            Origin::remote("laptop"),
            None,
        ),
        ScanRoot::local(PathBuf::from("/home/u/.factory/sessions")),
    ] {
        assert_eq!(mirrored_root_owners(&root), None, "{root:?}");
        assert!(mirrored_root_may_hold("claude", &root));
        assert!(mirrored_root_may_hold("factory", &root));
    }
}

#[test]
#[serial]
fn build_scan_roots_discovers_gh520_mirrored_layouts() {
    use crate::connectors::Connector;
    use franken_agent_detection::connectors::{cursor::CursorConnector, gemini::GeminiConnector};
    let _guard = ignore_sources_config();
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let storage = FrankenStorage::open(&data_dir.join("db.sqlite")).unwrap();
    let cases: Vec<(&str, &str, Box<dyn Connector>)> = vec![
        (
            "~/.config/Cursor/User",
            "User/globalStorage/state.vscdb",
            Box::new(CursorConnector::new()),
        ),
        (
            "~/.gemini/tmp",
            "tmp/hash/chats/session-test.json",
            Box::new(GeminiConnector::new()),
        ),
    ];
    for (path, relative, connector) in cases {
        storage
            .upsert_source(&crate::sources::provenance::Source {
                id: "laptop".to_string(),
                kind: SourceKind::Ssh,
                host_label: Some("user@laptop".to_string()),
                machine_id: None,
                platform: Some("linux".to_string()),
                config_json: Some(serde_json::json!({"paths": [path]})),
                created_at: None,
                updated_at: None,
            })
            .unwrap();
        let file = data_dir
            .join("remotes/laptop/mirror")
            .join(path_to_safe_dirname(path))
            .join(relative);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        // Discovery is deliberately pre-parse, including SQLite sources.
        std::fs::write(&file, b"").unwrap();
        let ctx = ScanContext::with_roots(
            data_dir.clone(),
            build_scan_roots(Some(&storage), &data_dir),
            None,
        );
        let discovered = connector.discover_source_files(&ctx).unwrap();
        assert_eq!(discovered.len(), 1, "{path}");
        assert_eq!(discovered[0].source_path, file);
        assert_eq!(discovered[0].origin.source_id, "laptop");
        assert_eq!(discovered[0].origin.host.as_deref(), Some("user@laptop"));
    }
}

#[test]
#[serial]
fn build_scan_roots_skips_nonexistent_mirror() {
    let _guard = ignore_sources_config();
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    // Register a remote source but don't create mirror directory
    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();

    // Register a remote source
    storage
        .upsert_source(&crate::sources::provenance::Source {
            id: "nonexistent".to_string(),
            kind: SourceKind::Ssh,
            host_label: Some("user@host".to_string()),
            machine_id: None,
            platform: None,
            config_json: None,
            created_at: None,
            updated_at: None,
        })
        .unwrap();

    // Create the mirror directory but with a different name
    let mirror_dir = data_dir.join("remotes").join("laptop").join("mirror");
    std::fs::create_dir_all(&mirror_dir).unwrap();

    let roots = build_scan_roots(Some(&storage), &data_dir);

    // Should only have local root (remote skipped because mirror doesn't exist)
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].origin.source_id, "local");
}

#[test]
#[serial]
fn build_scan_roots_includes_configured_local_source_paths() {
    let _guard = ignore_sources_config();
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let backup_root = tmp.path().join("backup-root");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&backup_root).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();

    storage
        .upsert_source(&crate::sources::provenance::Source {
            id: "backup-local".to_string(),
            kind: SourceKind::Local,
            host_label: None,
            machine_id: None,
            platform: Some("linux".to_string()),
            config_json: Some(serde_json::json!({
                "paths": [backup_root.to_string_lossy().to_string()],
                "path_mappings": [],
                "sync_schedule": {
                    "enabled": false
                }
            })),
            created_at: None,
            updated_at: None,
        })
        .unwrap();

    let roots = build_scan_roots(Some(&storage), &data_dir);

    assert_eq!(roots.len(), 2);
    let backup_scan_root = roots
        .iter()
        .find(|root| root.origin.source_id == "backup-local")
        .expect("configured local backup root should be included");
    assert_eq!(backup_scan_root.path, backup_root);
    assert!(!backup_scan_root.origin.is_remote());
    assert_eq!(backup_scan_root.platform, Some(Platform::Linux));
}

#[test]
#[serial]
fn gh538_watch_roots_retain_missing_configured_paths_and_provenance() {
    let _guard = ignore_sources_config();
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let storage = FrankenStorage::open(&data_dir.join("archive.db")).unwrap();
    let local_path = temp.path().join("future-local/.codex/sessions");
    for (id, kind, path) in [
        (
            "future-local",
            SourceKind::Local,
            local_path.to_string_lossy().into_owned(),
        ),
        (
            "future-remote",
            SourceKind::Ssh,
            "~/.codex/sessions".to_string(),
        ),
    ] {
        storage
            .upsert_source(&Source {
                id: id.to_string(),
                kind,
                host_label: Some("source-host".to_string()),
                machine_id: None,
                platform: Some("linux".to_string()),
                config_json: Some(serde_json::json!({
                    "paths": [path],
                    "path_mappings": [{"from": "/source/work", "to": "/mapped/work"}]
                })),
                created_at: None,
                updated_at: None,
            })
            .unwrap();
    }
    assert_eq!(build_scan_roots(Some(&storage), &data_dir).len(), 1);
    let roots = build_scan_roots_with_missing(Some(&storage), &data_dir, true);
    assert_eq!(roots.len(), 3);
    for (source_id, expected_path) in [
        ("future-local", local_path),
        (
            "future-remote",
            data_dir
                .join("remotes/future-remote/mirror")
                .join(path_to_safe_dirname("~/.codex/sessions")),
        ),
    ] {
        let root = roots
            .iter()
            .find(|root| root.origin.source_id == source_id)
            .unwrap();
        assert_eq!(root.path, expected_path);
        assert!(!root.path.exists());
        assert_eq!(root.origin.host.as_deref(), Some("source-host"));
        assert_eq!(root.platform, Some(Platform::Linux));
        assert_eq!(root.workspace_rewrites.len(), 1);
        assert_eq!(root.workspace_rewrites[0].from, "/source/work");
        assert_eq!(root.workspace_rewrites[0].to, "/mapped/work");
    }
}

#[test]
fn apply_workspace_rewrite_no_rewrites() {
    let mut conv = norm_conv(None, vec![norm_msg(0, 1000)]);
    conv.workspace = Some(PathBuf::from("/home/user/projects/app"));

    let root = crate::connectors::ScanRoot::local(PathBuf::from("/"));
    apply_workspace_rewrite(&mut conv, &root);

    // Workspace unchanged when no rewrites
    assert_eq!(
        conv.workspace,
        Some(PathBuf::from("/home/user/projects/app"))
    );
    // No workspace_original in metadata
    assert!(
        conv.metadata
            .get("cass")
            .and_then(|c| c.get("workspace_original"))
            .is_none()
    );
}

#[test]
fn apply_workspace_rewrite_no_workspace() {
    let mut conv = norm_conv(None, vec![norm_msg(0, 1000)]);
    conv.workspace = None;

    let mappings = vec![crate::sources::config::PathMapping::new(
        "/home/user",
        "/Users/me",
    )];

    let mut root = crate::connectors::ScanRoot::local(PathBuf::from("/"));
    root.workspace_rewrites = mappings;
    apply_workspace_rewrite(&mut conv, &root);

    // Still None
    assert!(conv.workspace.is_none());
}

#[test]
fn apply_workspace_rewrite_applies_mapping() {
    let mut conv = norm_conv(None, vec![norm_msg(0, 1000)]);
    conv.workspace = Some(PathBuf::from("/home/user/projects/app"));

    let mappings = vec![crate::sources::config::PathMapping::new(
        "/home/user",
        "/Users/me",
    )];

    let mut root = crate::connectors::ScanRoot::local(PathBuf::from("/"));
    root.workspace_rewrites = mappings;
    apply_workspace_rewrite(&mut conv, &root);

    // Workspace rewritten
    assert_eq!(
        conv.workspace,
        Some(PathBuf::from("/Users/me/projects/app"))
    );

    // Original stored in metadata
    let workspace_original = conv
        .metadata
        .get("cass")
        .and_then(|c| c.get("workspace_original"))
        .and_then(|v| v.as_str());
    assert_eq!(workspace_original, Some("/home/user/projects/app"));
}

#[test]
fn apply_workspace_rewrite_longest_prefix_match() {
    let mut conv = norm_conv(None, vec![norm_msg(0, 1000)]);
    conv.workspace = Some(PathBuf::from("/home/user/projects/special/app"));

    let mappings = vec![crate::sources::config::PathMapping::new(
        "/home/user",
        "/Users/me",
    )];

    let mut root = crate::connectors::ScanRoot::local(PathBuf::from("/"));
    root.workspace_rewrites = mappings;
    apply_workspace_rewrite(&mut conv, &root);

    // Should use longest prefix match
    assert_eq!(
        conv.workspace,
        Some(PathBuf::from("/Users/me/projects/special/app"))
    );
}

#[test]
fn apply_workspace_rewrite_no_match() {
    let mut conv = norm_conv(None, vec![norm_msg(0, 1000)]);
    conv.workspace = Some(PathBuf::from("/opt/other/path"));

    let mappings = vec![crate::sources::config::PathMapping::new(
        "/home/user",
        "/Users/me",
    )];

    let mut root = crate::connectors::ScanRoot::local(PathBuf::from("/"));
    root.workspace_rewrites = mappings;
    apply_workspace_rewrite(&mut conv, &root);

    // Workspace unchanged - no matching prefix
    assert_eq!(conv.workspace, Some(PathBuf::from("/opt/other/path")));
    // No workspace_original since nothing was rewritten
    assert!(
        conv.metadata
            .get("cass")
            .and_then(|c| c.get("workspace_original"))
            .is_none()
    );
}

#[test]
fn apply_workspace_rewrite_with_agent_filter() {
    // Test with agent filter
    let mut conv = norm_conv(None, vec![norm_msg(0, 1000)]);
    conv.agent_slug = "claude-code".to_string();
    conv.workspace = Some(PathBuf::from("/home/user/projects/app"));

    let mappings = vec![crate::sources::config::PathMapping::with_agents(
        "/home/user/projects",
        "/Volumes/Work",
        vec!["cursor".to_string()], // Only for cursor, not claude-code
    )];

    let mut root = crate::connectors::ScanRoot::local(PathBuf::from("/"));
    root.workspace_rewrites = mappings;
    apply_workspace_rewrite(&mut conv, &root);

    // Should NOT use cursor-specific mapping, falls back to general
    assert_eq!(
        conv.workspace,
        Some(PathBuf::from("/home/user/projects/app"))
    );
}

#[test]
fn apply_workspace_rewrite_preserves_existing_metadata() {
    let mut conv = norm_conv(None, vec![norm_msg(0, 1000)]);
    conv.workspace = Some(PathBuf::from("/home/user/app"));
    conv.metadata = serde_json::json!({
        "cass": {
            "origin": {
                "source_id": "laptop",
                "kind": "ssh",
                "host": "user@laptop.local"
            }
        }
    });

    let mappings = vec![crate::sources::config::PathMapping::new(
        "/home/user",
        "/Users/me",
    )];

    let mut root = crate::connectors::ScanRoot::local(PathBuf::from("/"));
    root.workspace_rewrites = mappings;
    apply_workspace_rewrite(&mut conv, &root);

    // Origin preserved
    assert_eq!(
        conv.metadata["cass"]["origin"]["source_id"].as_str(),
        Some("laptop")
    );
    // workspace_original added
    assert_eq!(
        conv.metadata["cass"]["workspace_original"].as_str(),
        Some("/home/user/app")
    );
}

#[test]
fn apply_workspace_rewrite_repairs_non_object_cass_metadata() {
    let mut conv = norm_conv(None, vec![norm_msg(0, 1000)]);
    conv.workspace = Some(PathBuf::from("/home/user/app"));
    conv.metadata = serde_json::json!({
        "cass": ["malformed"],
        "other": "preserve me"
    });

    let mappings = vec![crate::sources::config::PathMapping::new(
        "/home/user",
        "/Users/me",
    )];

    let mut root = crate::connectors::ScanRoot::local(PathBuf::from("/"));
    root.workspace_rewrites = mappings;
    apply_workspace_rewrite(&mut conv, &root);

    assert_eq!(conv.workspace, Some(PathBuf::from("/Users/me/app")));
    assert_eq!(
        conv.metadata.pointer("/other"),
        Some(&serde_json::json!("preserve me"))
    );
    assert_eq!(
        conv.metadata.pointer("/cass/workspace_original"),
        Some(&serde_json::json!("/home/user/app"))
    );
}

// =========================================================================
// Stale Detection Tests
// =========================================================================

#[test]
fn stale_action_from_env_str_parses_correctly() {
    assert_eq!(StaleAction::from_env_str("warn"), StaleAction::Warn);
    assert_eq!(StaleAction::from_env_str("WARN"), StaleAction::Warn);
    assert_eq!(StaleAction::from_env_str("rebuild"), StaleAction::Rebuild);
    assert_eq!(StaleAction::from_env_str("auto"), StaleAction::Rebuild);
    assert_eq!(StaleAction::from_env_str("fix"), StaleAction::Rebuild);
    assert_eq!(StaleAction::from_env_str("none"), StaleAction::None);
    assert_eq!(StaleAction::from_env_str("off"), StaleAction::None);
    assert_eq!(StaleAction::from_env_str("0"), StaleAction::None);
    assert_eq!(StaleAction::from_env_str("false"), StaleAction::None);
    assert_eq!(StaleAction::from_env_str("unknown"), StaleAction::Warn);
}

#[test]
fn stale_config_default_values() {
    let cfg = StaleConfig::default();
    assert_eq!(cfg.threshold_hours, 24);
    assert_eq!(cfg.action, StaleAction::Warn);
    assert_eq!(cfg.check_interval_mins, 60);
    assert_eq!(cfg.min_zero_scans, 10);
    assert!(cfg.is_enabled());
}

#[test]
fn stale_config_none_action_disables_detection() {
    let cfg = StaleConfig {
        action: StaleAction::None,
        ..Default::default()
    };
    assert!(!cfg.is_enabled());
}

#[test]
fn stale_detector_records_successful_ingest() {
    let detector = StaleDetector::new(StaleConfig::default());
    assert_eq!(detector.stats().total_ingests, 0);
    assert_eq!(detector.stats().consecutive_zero_scans, 0);

    detector.record_scan(5);
    assert_eq!(detector.stats().total_ingests, 1);
    assert_eq!(detector.stats().consecutive_zero_scans, 0);
    assert!(detector.stats().seconds_since_last_ingest.is_some());
}

#[test]
fn stale_detector_tracks_zero_scans() {
    let detector = StaleDetector::new(StaleConfig::default());

    detector.record_scan(0);
    assert_eq!(detector.stats().consecutive_zero_scans, 1);

    detector.record_scan(0);
    assert_eq!(detector.stats().consecutive_zero_scans, 2);

    // Successful scan resets counter
    detector.record_scan(1);
    assert_eq!(detector.stats().consecutive_zero_scans, 0);
}

#[test]
fn stale_detector_reset_clears_state() {
    let detector = StaleDetector::new(StaleConfig::default());

    detector.record_scan(0);
    detector.record_scan(0);
    assert_eq!(detector.stats().consecutive_zero_scans, 2);

    detector.reset();
    assert_eq!(detector.stats().consecutive_zero_scans, 0);
    assert!(detector.stats().seconds_since_last_ingest.is_some());
}

#[test]
fn finalize_watch_reindex_result_records_error_and_resets_phase() {
    let detector = StaleDetector::new(StaleConfig::default());
    let progress = Arc::new(IndexingProgress::default());
    progress.phase.store(2, Ordering::Relaxed);

    let indexed = finalize_watch_reindex_result(
        Err(anyhow::anyhow!("boom")),
        &detector,
        Some(&progress),
        "watch incremental reindex",
    );

    assert_eq!(
        indexed
            .expect_err("failed watch reindex must remain retryable")
            .to_string(),
        "boom"
    );
    assert_eq!(
        detector.stats().consecutive_zero_scans,
        1,
        "failed watch reindex should still count as a zero-result scan for stale detection"
    );
    assert_eq!(
        progress.phase.load(Ordering::Relaxed),
        0,
        "failed watch reindex should reset progress phase back to idle"
    );
    assert_eq!(
        progress
            .last_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_deref(),
        Some("watch incremental reindex: boom"),
        "failed watch reindex should surface the real error"
    );
}

#[test]
fn watch_retains_pending_sources_when_reindex_finalization_fails() {
    let source = PathBuf::from("sessions/rollout-deferred.jsonl");
    let root = ScanRoot::local(PathBuf::from("sessions"));
    let roots = [(ConnectorKind::Codex, root)];
    let detector = StaleDetector::new(StaleConfig::default());

    for is_rebuild in [false, true] {
        let mut pending = BTreeSet::from([source.clone()]);
        dispatch_watch_callback(&mut pending, &roots, is_rebuild, &|paths, _, rebuilding| {
            assert_eq!(rebuilding, is_rebuild);
            if !rebuilding {
                assert_eq!(paths, vec![source.clone()]);
            }
            finalize_watch_reindex_result(
                Err(anyhow::anyhow!("retryable storage failure")),
                &detector,
                None,
                "watch reindex",
            )?;
            Ok(BTreeSet::new())
        });
        assert!(
            pending.contains(&source),
            "a failed scan must retain its source"
        );
        if is_rebuild {
            assert!(pending.contains(&roots[0].1.path));
        }
    }
    assert_eq!(detector.stats().consecutive_zero_scans, 2);
}

#[test]
fn gh479_write_byte_counter_parses_only_physical_writes() {
    assert_eq!(
        parse_process_index_write_bytes(
            "wchar: 90000\nread_bytes: 80000\nwrite_bytes: 4096\ncancelled_write_bytes: 1024\n"
        ),
        Some(4096)
    );
    assert_eq!(parse_process_index_write_bytes("write_bytes: 0\n"), Some(0));
    assert_eq!(
        parse_process_index_write_bytes("write_bytes: 18446744073709551615\n"),
        Some(u64::MAX)
    );
    for text in [
        "",
        "wchar: 42",
        "cancelled_write_bytes: 42",
        "write_bytes: -1",
        "write_bytes: bad",
        "write_bytes: 18446744073709551616",
    ] {
        assert_eq!(parse_process_index_write_bytes(text), None, "{text}");
    }
}

#[cfg(target_os = "linux")]
#[test]
fn gh479_write_byte_counter_includes_writes_before_guard_drop() {
    use std::io::Write;

    let progress = Arc::new(IndexingProgress::default());
    let mut file = tempfile::tempfile().unwrap();
    let guard = RunIndexProgressReset::new(Some(Arc::clone(&progress)));
    let before = guard.initial_write_bytes.unwrap();
    file.write_all(&[0x5a; 65536]).unwrap();
    file.sync_all().unwrap();
    let observed = process_index_write_bytes().unwrap();
    drop(guard);
    let stats = progress.stats.lock().unwrap();
    // A tmpfs may account zero physical writes. Concurrent process threads
    // may add writes, so use the observed kernel delta as a lower bound.
    assert!(stats.bytes_written.unwrap() >= observed - before);
    assert_eq!(
        serde_json::to_value(&*stats).unwrap()["bytes_written"].as_u64(),
        stats.bytes_written
    );
    assert!(
        serde_json::to_value(IndexingStats::default())
            .unwrap()
            .get("bytes_written")
            .is_none()
    );
}

#[test]
fn run_index_progress_reset_guard_resets_idle_state_without_clobbering_error() {
    let progress = Arc::new(IndexingProgress::default());
    progress.phase.store(2, Ordering::Relaxed);
    progress.is_rebuilding.store(true, Ordering::Relaxed);
    *progress
        .last_error
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some("boom".to_string());

    {
        let _guard = RunIndexProgressReset::new(Some(progress.clone()));
    }

    assert_eq!(progress.phase.load(Ordering::Relaxed), 0);
    assert!(
        !progress.is_rebuilding.load(Ordering::Relaxed),
        "drop guard should clear stale rebuild state after failures"
    );
    assert_eq!(
        progress
            .last_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_deref(),
        Some("boom"),
        "idle-state cleanup should not erase the real error"
    );
}

#[test]
fn reconcile_pending_lexical_commit_promotes_committed_offset_when_meta_changes() {
    let tmp = TempDir::new().unwrap();
    let index_path = tmp.path().join("index");
    fs::create_dir_all(&index_path).unwrap();
    fs::write(index_path.join("meta.json"), b"before").unwrap();

    let db_state = LexicalRebuildDbState {
        db_path: "/tmp/agent_search.db".to_string(),
        total_conversations: 400,
        total_messages: 800,
        storage_fingerprint: "seed:400".to_string(),
    };
    let mut state = LexicalRebuildState::new(db_state, LEXICAL_REBUILD_PAGE_SIZE);
    state.set_runtime(&LexicalRebuildPipelineRuntimeSnapshot {
        queue_depth: 3,
        inflight_message_bytes: 65_536,
        max_message_bytes_in_flight: 131_072,
        pending_batch_conversations: 9,
        pending_batch_message_bytes: 131_072,
        page_prep_workers: 6,
        active_page_prep_jobs: 2,
        ordered_buffered_pages: 4,
        budget_generation: 1,
        producer_budget_wait_count: 2,
        producer_budget_wait_ms: 15,
        producer_handoff_wait_count: 1,
        producer_handoff_wait_ms: 7,
        host_loadavg_1m_milli: None,
        controller_mode: "pressure_limited".to_string(),
        controller_reason: "queue_depth_3_reached_pipeline_capacity_3".to_string(),
        staged_merge_workers_max: 0,
        staged_merge_allowed_jobs: 0,
        staged_merge_active_jobs: 0,
        staged_merge_ready_artifacts: 0,
        staged_merge_ready_groups: 0,
        staged_merge_controller_reason: String::new(),
        staged_shard_build_workers_max: 0,
        staged_shard_build_allowed_jobs: 0,
        staged_shard_build_active_jobs: 0,
        staged_shard_build_pending_jobs: 0,
        staged_shard_build_controller_reason: String::new(),
        updated_at_ms: 1_733_000_124_000_i64,
        ..LexicalRebuildPipelineRuntimeSnapshot::default()
    });
    state.record_pending_commit(
        Some(200),
        200,
        600,
        index_meta_fingerprint(&index_path).unwrap(),
    );
    persist_lexical_rebuild_state(&index_path, &state).unwrap();

    fs::write(index_path.join("meta.json"), b"after").unwrap();

    let reconciled = reconcile_pending_lexical_commit(&index_path, state).unwrap();
    assert_eq!(reconciled.committed_offset, 200);
    assert_eq!(reconciled.committed_conversation_id, Some(200));
    assert_eq!(reconciled.processed_conversations, 200);
    assert_eq!(reconciled.indexed_docs, 600);
    assert!(reconciled.pending.is_none());
    assert_eq!(
        reconciled.runtime,
        LexicalRebuildPipelineRuntimeSnapshot::default()
    );
    let persisted = load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("reconciled checkpoint should persist");
    assert_eq!(
        persisted.runtime,
        LexicalRebuildPipelineRuntimeSnapshot::default()
    );
}
