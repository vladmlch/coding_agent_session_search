use crate::cli::args_sub::{ConvExportFormat, RobotFormat};
use crate::cli::commands::expand::{
    extract_message_timestamp, extract_role, extract_text_content, run_expand,
};
use crate::cli::commands::export::indexed_export_cli_error::run_export;
use crate::cli::commands::export_html::run_export_html;
use crate::cli::commands::view::role_to_export_string::{
    ViewWindow, canonical_followup_source_id, conversation_view_to_raw_messages,
    followup_source_is_local, prefers_direct_jsonl_file, prefers_direct_view_file, run_view,
    try_load_indexed_conversation_from_db, validate_followup_source_id,
};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use crate::sources::provenance::{Source, SourceKind};
use crate::storage::sqlite::SqliteStorage;
use std::path::PathBuf;
use tempfile::TempDir;

#[test]
pub(crate) fn prefer_direct_helpers_allow_explicit_local_source() {
    let tmp = TempDir::new().expect("temp dir");
    let jsonl = tmp.path().join("session.jsonl");
    let markdown = tmp.path().join("session.md");
    std::fs::write(&jsonl, "{\"role\":\"user\",\"content\":\"hello\"}\n").expect("write jsonl");
    std::fs::write(&markdown, "# heading\nbody\n").expect("write markdown");

    assert!(prefers_direct_view_file(
        &jsonl,
        Some(crate::sources::provenance::LOCAL_SOURCE_ID)
    ));
    assert!(prefers_direct_view_file(
        &markdown,
        Some(crate::sources::provenance::LOCAL_SOURCE_ID)
    ));
    assert!(prefers_direct_jsonl_file(
        &jsonl,
        Some(crate::sources::provenance::LOCAL_SOURCE_ID)
    ));
}

#[test]
pub(crate) fn prefer_direct_helpers_allow_trimmed_explicit_local_source() {
    let tmp = TempDir::new().expect("temp dir");
    let jsonl = tmp.path().join("session.jsonl");
    let markdown = tmp.path().join("session.md");
    std::fs::write(&jsonl, "{\"role\":\"user\",\"content\":\"hello\"}\n").expect("write jsonl");
    std::fs::write(&markdown, "# heading\nbody\n").expect("write markdown");

    assert!(prefers_direct_view_file(&jsonl, Some("  local  ")));
    assert!(prefers_direct_view_file(&markdown, Some("\tlocal\n")));
    assert!(prefers_direct_jsonl_file(&jsonl, Some("  LOCAL  ")));
    assert!(followup_source_is_local(Some("  local  ")));
    assert_eq!(
        canonical_followup_source_id(Some("\tLOCAL\n")).as_deref(),
        Some(crate::sources::provenance::LOCAL_SOURCE_ID)
    );
}

#[test]
pub(crate) fn validate_followup_source_id_rejects_trimmed_ambiguous_keywords() {
    for input in ["  ", "  all  ", "\tREMOTE\n"] {
        let err = validate_followup_source_id(input, "cass view")
            .expect_err("ambiguous keyword should be rejected");
        assert_eq!(err.kind, "ambiguous-source");
        assert!(err.message.contains(input));
    }
}

#[test]
pub(crate) fn prefer_direct_helpers_reject_non_local_sources() {
    let tmp = TempDir::new().expect("temp dir");
    let jsonl = tmp.path().join("session.jsonl");
    let markdown = tmp.path().join("session.md");
    std::fs::write(&jsonl, "{\"role\":\"user\",\"content\":\"hello\"}\n").expect("write jsonl");
    std::fs::write(&markdown, "# heading\nbody\n").expect("write markdown");

    assert!(!prefers_direct_view_file(&jsonl, Some("work-laptop")));
    assert!(!prefers_direct_view_file(&markdown, Some("work-laptop")));
    assert!(!prefers_direct_jsonl_file(&jsonl, Some("work-laptop")));
}

#[test]
pub(crate) fn db_fallback_loads_virtual_source_path_and_converts_messages() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).expect("open sqlite");

    let agent = Agent {
        id: None,
        slug: "cursor".to_string(),
        name: "Cursor".to_string(),
        version: None,
        kind: AgentKind::VsCode,
    };
    let agent_id = storage.ensure_agent(&agent).expect("ensure agent");

    let synthetic_path = tmp
        .path()
        .join("Cursor/globalStorage/state.vscdb/composer-123");
    let conversation = Conversation {
        id: None,
        agent_slug: "cursor".to_string(),
        workspace: Some(PathBuf::from("/tmp/ws")),
        external_id: Some("composer-123".to_string()),
        title: Some("Cursor synthetic path".to_string()),
        source_path: synthetic_path.clone(),
        started_at: Some(1_733_000_000_000),
        ended_at: Some(1_733_000_010_000),
        approx_tokens: None,
        metadata_json: serde_json::json!({}),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: Some("me".to_string()),
            created_at: Some(1_733_000_000_000),
            content: "hello from cursor".to_string(),
            extra_json: serde_json::json!({}),
            snippets: Vec::new(),
        }],
        source_id: "local".to_string(),
        origin_host: None,
    };
    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .expect("insert conversation");

    let loaded = try_load_indexed_conversation_from_db(&synthetic_path, &db_path)
        .expect("conversation should load via db fallback");
    assert_eq!(loaded.convo.external_id.as_deref(), Some("composer-123"));
    assert_eq!(loaded.messages.len(), 1);

    let raw_messages = conversation_view_to_raw_messages(&loaded);
    assert_eq!(raw_messages.len(), 1);
    assert_eq!(extract_role(&raw_messages[0]), "user");
    assert_eq!(extract_text_content(&raw_messages[0]), "hello from cursor");
    assert_eq!(
        extract_message_timestamp(&raw_messages[0]),
        Some(1_733_000_000_000)
    );
}

#[test]
pub(crate) fn try_load_indexed_conversation_from_db_prefers_local_source_for_shared_path() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use crate::storage::sqlite::FrankenStorage;
    use tempfile::tempdir;

    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("open db");
    let agent = Agent {
        id: None,
        slug: "codex".to_string(),
        name: "Codex".to_string(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).expect("ensure agent");
    storage
        .upsert_source(&Source {
            id: "work-laptop".to_string(),
            kind: SourceKind::Ssh,
            host_label: Some("work-laptop".to_string()),
            machine_id: None,
            platform: None,
            config_json: None,
            created_at: None,
            updated_at: None,
        })
        .expect("insert remote source");
    let synthetic_path = PathBuf::from("/same/path/session.jsonl");

    let remote = Conversation {
        id: None,
        agent_slug: "codex".to_string(),
        workspace: None,
        external_id: Some("remote-123".to_string()),
        title: Some("Remote Session".to_string()),
        source_path: synthetic_path.clone(),
        started_at: Some(1_700_000_000_000),
        ended_at: None,
        approx_tokens: Some(10),
        metadata_json: serde_json::json!({}),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: Some("remote".to_string()),
            created_at: Some(1_700_000_000_001),
            content: "remote body".to_string(),
            extra_json: serde_json::json!({}),
            snippets: Vec::new(),
        }],
        source_id: "work-laptop".to_string(),
        origin_host: Some("work-laptop".to_string()),
    };
    storage
        .insert_conversation_tree(agent_id, None, &remote)
        .expect("insert remote conversation");

    let local = Conversation {
        id: None,
        agent_slug: "codex".to_string(),
        workspace: None,
        external_id: Some("local-123".to_string()),
        title: Some("Local Session".to_string()),
        source_path: synthetic_path.clone(),
        started_at: Some(1_700_000_100_000),
        ended_at: None,
        approx_tokens: Some(10),
        metadata_json: serde_json::json!({}),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: Some("local".to_string()),
            created_at: Some(1_700_000_100_001),
            content: "local body".to_string(),
            extra_json: serde_json::json!({}),
            snippets: Vec::new(),
        }],
        source_id: crate::sources::provenance::LOCAL_SOURCE_ID.to_string(),
        origin_host: None,
    };
    storage
        .insert_conversation_tree(agent_id, None, &local)
        .expect("insert local conversation");

    let loaded = try_load_indexed_conversation_from_db(&synthetic_path, &db_path)
        .expect("conversation should load via db fallback");
    assert_eq!(
        loaded.convo.source_id,
        crate::sources::provenance::LOCAL_SOURCE_ID
    );
    assert_eq!(loaded.convo.external_id.as_deref(), Some("local-123"));
    assert_eq!(loaded.messages[0].content, "local body");
}

#[test]
pub(crate) fn try_load_indexed_conversation_from_db_falls_back_to_remote_when_local_missing() {
    use crate::storage::sqlite::FrankenStorage;
    use tempfile::tempdir;

    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("open db");
    let agent = Agent {
        id: None,
        slug: "codex".to_string(),
        name: "Codex".to_string(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).expect("ensure agent");
    storage
        .upsert_source(&Source {
            id: "work-laptop".to_string(),
            kind: SourceKind::Ssh,
            host_label: Some("work-laptop".to_string()),
            machine_id: None,
            platform: None,
            config_json: None,
            created_at: None,
            updated_at: None,
        })
        .expect("insert remote source");
    let synthetic_path = PathBuf::from("/same/path/remote-only.jsonl");

    let remote = Conversation {
        id: None,
        agent_slug: "codex".to_string(),
        workspace: None,
        external_id: Some("remote-only".to_string()),
        title: Some("Remote Only".to_string()),
        source_path: synthetic_path.clone(),
        started_at: Some(1_700_000_000_000),
        ended_at: None,
        approx_tokens: Some(10),
        metadata_json: serde_json::json!({}),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: Some("remote".to_string()),
            created_at: Some(1_700_000_000_001),
            content: "remote-only body".to_string(),
            extra_json: serde_json::json!({}),
            snippets: Vec::new(),
        }],
        source_id: "work-laptop".to_string(),
        origin_host: Some("work-laptop".to_string()),
    };
    storage
        .insert_conversation_tree(agent_id, None, &remote)
        .expect("insert remote conversation");

    let loaded = try_load_indexed_conversation_from_db(&synthetic_path, &db_path)
        .expect("conversation should load via db fallback");
    assert_eq!(loaded.convo.source_id, "work-laptop");
    assert_eq!(loaded.convo.external_id.as_deref(), Some("remote-only"));
    assert_eq!(loaded.messages[0].content, "remote-only body");
}

#[test]
pub(crate) fn run_export_uses_exact_source_id_for_shared_indexed_path() {
    use crate::storage::sqlite::FrankenStorage;
    use tempfile::tempdir;

    let tmp = tempdir().expect("tempdir");
    let db_path = tmp.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path).expect("open db");
    let agent = Agent {
        id: None,
        slug: "codex".to_string(),
        name: "Codex".to_string(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).expect("ensure agent");
    storage
        .upsert_source(&Source {
            id: "work-laptop".to_string(),
            kind: SourceKind::Ssh,
            host_label: Some("work-laptop".to_string()),
            machine_id: None,
            platform: None,
            config_json: None,
            created_at: None,
            updated_at: None,
        })
        .expect("insert remote source");
    let synthetic_path = PathBuf::from("/same/path/export.jsonl");

    let local = Conversation {
        id: None,
        agent_slug: "codex".to_string(),
        workspace: None,
        external_id: Some("local-export".to_string()),
        title: Some("Local Export".to_string()),
        source_path: synthetic_path.clone(),
        started_at: Some(1_700_000_100_000),
        ended_at: None,
        approx_tokens: Some(10),
        metadata_json: serde_json::json!({}),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: Some("local".to_string()),
            created_at: Some(1_700_000_100_001),
            content: "local export body".to_string(),
            extra_json: serde_json::json!({}),
            snippets: Vec::new(),
        }],
        source_id: crate::sources::provenance::LOCAL_SOURCE_ID.to_string(),
        origin_host: None,
    };
    storage
        .insert_conversation_tree(agent_id, None, &local)
        .expect("insert local conversation");

    let remote = Conversation {
        id: None,
        agent_slug: "codex".to_string(),
        workspace: None,
        external_id: Some("remote-export".to_string()),
        title: Some("Remote Export".to_string()),
        source_path: synthetic_path.clone(),
        started_at: Some(1_700_000_000_000),
        ended_at: None,
        approx_tokens: Some(10),
        metadata_json: serde_json::json!({}),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: Some("remote".to_string()),
            created_at: Some(1_700_000_000_001),
            content: "remote export body".to_string(),
            extra_json: serde_json::json!({}),
            snippets: Vec::new(),
        }],
        source_id: "work-laptop".to_string(),
        origin_host: Some("work-laptop".to_string()),
    };
    storage
        .insert_conversation_tree(agent_id, None, &remote)
        .expect("insert remote conversation");

    let output_path = tmp.path().join("export.md");
    run_export(
        &synthetic_path,
        Some(db_path.clone()),
        Some("work-laptop"),
        None,
        ConvExportFormat::Markdown,
        Some(output_path.as_path()),
        false, // clipboard
        false,
        true,
        false, // include_secrets
    )
    .expect("run export");

    let markdown = std::fs::read_to_string(&output_path).expect("read markdown");
    assert!(markdown.contains("remote export body"));
    assert!(!markdown.contains("local export body"));
}

#[test]
pub(crate) fn run_export_uses_indexed_snapshot_for_local_jsonl_file() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).expect("open sqlite");

    let agent = Agent {
        id: None,
        slug: "claude_code".to_string(),
        name: "Claude Code".to_string(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).expect("ensure agent");

    // The on-disk file is a live transcript with different content. The
    // index-first contract pins exports to the indexed snapshot: the file
    // must never be opened, so "local second" cannot leak into output.
    let session_path = tmp.path().join("session.jsonl");
    std::fs::write(
        &session_path,
        br#"{"role":"user","content":"local first"}
{"role":"assistant","content":"local second"}
"#,
    )
    .expect("write jsonl");

    let conversation = Conversation {
        id: None,
        agent_slug: "claude_code".to_string(),
        workspace: Some(PathBuf::from("/tmp/ws")),
        external_id: Some("stale-index".to_string()),
        title: Some("Indexed Session".to_string()),
        source_path: session_path.clone(),
        started_at: Some(1_733_000_000_000),
        ended_at: Some(1_733_000_010_000),
        approx_tokens: None,
        metadata_json: serde_json::json!({}),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: Some("me".to_string()),
            created_at: Some(1_733_000_000_000),
            content: "indexed only line".to_string(),
            extra_json: serde_json::json!({}),
            snippets: Vec::new(),
        }],
        source_id: "local".to_string(),
        origin_host: None,
    };
    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .expect("insert conversation");

    let output_path = tmp.path().join("export.md");
    run_export(
        &session_path,
        Some(db_path),
        None,
        None,
        ConvExportFormat::Markdown,
        Some(output_path.as_path()),
        false, // clipboard
        false,
        true,
        false, // include_secrets
    )
    .expect("export should use the indexed snapshot regardless of the local JSONL");

    let markdown = std::fs::read_to_string(&output_path).expect("read markdown");
    assert!(markdown.contains("indexed only line"));
    assert!(!markdown.contains("local second"));
}

#[test]
pub(crate) fn run_export_html_uses_indexed_snapshot_for_local_jsonl_file() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).expect("open sqlite");

    let agent = Agent {
        id: None,
        slug: "claude_code".to_string(),
        name: "Claude Code".to_string(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).expect("ensure agent");

    // The live file carries different content; the index-first contract
    // pins exports to the indexed snapshot and never opens the source.
    let session_path = tmp.path().join("session.jsonl");
    std::fs::write(
        &session_path,
        br#"{"role":"user","content":"local first"}
{"role":"assistant","content":"local second"}
"#,
    )
    .expect("write jsonl");

    let conversation = Conversation {
        id: None,
        agent_slug: "claude_code".to_string(),
        workspace: Some(PathBuf::from("/tmp/ws")),
        external_id: Some("stale-index".to_string()),
        title: Some("Indexed Session".to_string()),
        source_path: session_path.clone(),
        started_at: Some(1_733_000_000_000),
        ended_at: Some(1_733_000_010_000),
        approx_tokens: None,
        metadata_json: serde_json::json!({}),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: Some("me".to_string()),
            created_at: Some(1_733_000_000_000),
            content: "indexed only line".to_string(),
            extra_json: serde_json::json!({}),
            snippets: Vec::new(),
        }],
        source_id: "local".to_string(),
        origin_host: None,
    };
    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .expect("insert conversation");

    let output_dir = tmp.path().join("html-out");
    std::fs::create_dir_all(&output_dir).expect("mkdir html-out");
    run_export_html(
        &session_path,
        Some(db_path),
        None,
        None,
        Some(output_dir.as_path()),
        Some("export.html"),
        false,
        false,
        false,
        true,
        false,
        false,
        "system",
        false,
        false,
        false,
        Some(RobotFormat::Json),
    )
    .expect("html export should use the indexed snapshot regardless of the local JSONL");

    let html_path = output_dir.join("export.html");
    let html = std::fs::read_to_string(&html_path).expect("read html");
    assert!(html.contains("indexed only line"));
    assert!(!html.contains("local second"));
}

#[test]
pub(crate) fn run_export_html_infers_metadata_for_indexed_local_jsonl() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).expect("open sqlite");

    let agent = Agent {
        id: None,
        slug: "claude_code".to_string(),
        name: "Claude Code".to_string(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).expect("ensure agent");

    let session_path = tmp
        .path()
        .join(".claude")
        .join("projects")
        .join("demo")
        .join("session.jsonl");
    // `workspace` is left unset so the export falls back to path-derived
    // metadata for the project label; the conversation body itself comes
    // from the indexed snapshot.
    let conversation = Conversation {
        id: None,
        agent_slug: "claude_code".to_string(),
        workspace: None,
        external_id: Some("claude-demo".to_string()),
        title: Some("Claude Demo".to_string()),
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
            content: "indexed second".to_string(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: "local".to_string(),
        origin_host: None,
    };
    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .expect("insert conversation");

    let output_dir = tmp.path().join("html-out");
    std::fs::create_dir_all(&output_dir).expect("mkdir html-out");
    run_export_html(
        &session_path,
        Some(db_path),
        None,
        None,
        Some(output_dir.as_path()),
        Some("export.html"),
        false,
        false,
        false,
        true,
        false,
        false,
        "system",
        false,
        false,
        false,
        Some(RobotFormat::Json),
    )
    .expect("html export should render indexed content with inferred metadata");

    let html_path = output_dir.join("export.html");
    let html = std::fs::read_to_string(&html_path).expect("read html");
    assert!(html.contains("Claude"));
    assert!(html.contains("demo"));
    assert!(html.contains("indexed second"));
}

#[test]
pub(crate) fn run_export_html_infers_copilot_metadata_for_indexed_local_jsonl() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).expect("open sqlite");

    let agent = Agent {
        id: None,
        slug: "copilot".to_string(),
        name: "GitHub Copilot".to_string(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).expect("ensure agent");

    let session_path = tmp
        .path()
        .join(".config")
        .join("gh-copilot")
        .join("sessions")
        .join("session.jsonl");
    // `workspace` is left unset so the export falls back to path-derived
    // metadata for the project label.
    let conversation = Conversation {
        id: None,
        agent_slug: "copilot".to_string(),
        workspace: None,
        external_id: Some("copilot-demo".to_string()),
        title: Some("Copilot Session".to_string()),
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
            content: "copilot second".to_string(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: "local".to_string(),
        origin_host: None,
    };
    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .expect("insert conversation");

    let output_dir = tmp.path().join("html-out");
    std::fs::create_dir_all(&output_dir).expect("mkdir html-out");
    run_export_html(
        &session_path,
        Some(db_path),
        None,
        None,
        Some(output_dir.as_path()),
        Some("export.html"),
        false,
        false,
        false,
        true,
        false,
        false,
        "system",
        false,
        false,
        false,
        Some(RobotFormat::Json),
    )
    .expect("html export should render indexed content with inferred copilot metadata");

    let html_path = output_dir.join("export.html");
    let html = std::fs::read_to_string(&html_path).expect("read html");
    assert!(html.contains("GitHub Copilot"));
    assert!(html.contains("sessions"));
    assert!(html.contains("copilot second"));
}

#[test]
pub(crate) fn run_view_prefers_local_jsonl_file_when_no_source_is_given() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).expect("open sqlite");

    let agent = Agent {
        id: None,
        slug: "claude_code".to_string(),
        name: "Claude Code".to_string(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).expect("ensure agent");

    let session_path = tmp.path().join("session.jsonl");
    std::fs::write(
        &session_path,
        br#"{"role":"user","content":"local first"}
{"role":"assistant","content":"local second"}
"#,
    )
    .expect("write jsonl");

    let conversation = Conversation {
        id: None,
        agent_slug: "claude_code".to_string(),
        workspace: Some(PathBuf::from("/tmp/ws")),
        external_id: Some("stale-index".to_string()),
        title: Some("Indexed Session".to_string()),
        source_path: session_path.clone(),
        started_at: Some(1_733_000_000_000),
        ended_at: Some(1_733_000_010_000),
        approx_tokens: None,
        metadata_json: serde_json::json!({}),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: Some("me".to_string()),
            created_at: Some(1_733_000_000_000),
            content: "indexed only line".to_string(),
            extra_json: serde_json::json!({}),
            snippets: Vec::new(),
        }],
        source_id: "local".to_string(),
        origin_host: None,
    };
    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .expect("insert conversation");

    run_view(
        &session_path,
        Some(db_path),
        None,
        None,
        ViewWindow {
            line: Some(2),
            context: 0,
        },
        Some(RobotFormat::Json),
        None,
    )
    .expect("view should prefer the local JSONL file over stale indexed content");
}

#[test]
pub(crate) fn run_view_prefers_local_markdown_file_when_no_source_is_given() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).expect("open sqlite");

    let agent = Agent {
        id: None,
        slug: "aider".to_string(),
        name: "Aider".to_string(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).expect("ensure agent");

    let session_path = tmp.path().join("session.md");
    std::fs::write(
        &session_path,
        "# local heading
local second line
",
    )
    .expect("write markdown");

    let conversation = Conversation {
        id: None,
        agent_slug: "aider".to_string(),
        workspace: Some(PathBuf::from("/tmp/ws")),
        external_id: Some("stale-index".to_string()),
        title: Some("Indexed Session".to_string()),
        source_path: session_path.clone(),
        started_at: Some(1_733_000_000_000),
        ended_at: Some(1_733_000_010_000),
        approx_tokens: None,
        metadata_json: serde_json::json!({}),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: Some("me".to_string()),
            created_at: Some(1_733_000_000_000),
            content: "indexed only line".to_string(),
            extra_json: serde_json::json!({}),
            snippets: Vec::new(),
        }],
        source_id: "local".to_string(),
        origin_host: None,
    };
    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .expect("insert conversation");

    run_view(
        &session_path,
        Some(db_path),
        None,
        None,
        ViewWindow {
            line: Some(2),
            context: 0,
        },
        Some(RobotFormat::Json),
        None,
    )
    .expect("view should prefer the local markdown file over stale indexed content");
}

#[test]
pub(crate) fn run_expand_prefers_local_jsonl_file_when_no_source_is_given() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).expect("open sqlite");

    let agent = Agent {
        id: None,
        slug: "claude_code".to_string(),
        name: "Claude Code".to_string(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).expect("ensure agent");

    let session_path = tmp.path().join("session.jsonl");
    std::fs::write(
        &session_path,
        br#"{"role":"user","content":"local first"}
{"role":"assistant","content":"local second"}
"#,
    )
    .expect("write jsonl");

    let conversation = Conversation {
        id: None,
        agent_slug: "claude_code".to_string(),
        workspace: Some(PathBuf::from("/tmp/ws")),
        external_id: Some("stale-index".to_string()),
        title: Some("Indexed Session".to_string()),
        source_path: session_path.clone(),
        started_at: Some(1_733_000_000_000),
        ended_at: Some(1_733_000_010_000),
        approx_tokens: None,
        metadata_json: serde_json::json!({}),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: Some("me".to_string()),
            created_at: Some(1_733_000_000_000),
            content: "indexed only line".to_string(),
            extra_json: serde_json::json!({}),
            snippets: Vec::new(),
        }],
        source_id: "local".to_string(),
        origin_host: None,
    };
    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .expect("insert conversation");

    run_expand(
        &session_path,
        Some(db_path),
        None,
        2,
        0,
        Some(RobotFormat::Json),
    )
    .expect("expand should prefer the local JSONL file over stale indexed content");
}

#[test]
pub(crate) fn run_expand_prefers_indexed_conversation_for_local_markdown_file() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).expect("open sqlite");

    let agent = Agent {
        id: None,
        slug: "aider".to_string(),
        name: "Aider".to_string(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).expect("ensure agent");

    let session_path = tmp.path().join("session.md");
    std::fs::write(
        &session_path,
        "# raw aider markdown

This is not JSONL.
",
    )
    .expect("write markdown");

    let conversation = Conversation {
        id: None,
        agent_slug: "aider".to_string(),
        workspace: Some(PathBuf::from("/tmp/ws")),
        external_id: Some("indexed-md".to_string()),
        title: Some("Indexed Session".to_string()),
        source_path: session_path.clone(),
        started_at: Some(1_733_000_000_000),
        ended_at: Some(1_733_000_010_000),
        approx_tokens: None,
        metadata_json: serde_json::json!({}),
        messages: vec![
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: Some("me".to_string()),
                created_at: Some(1_733_000_000_000),
                content: "indexed first line".to_string(),
                extra_json: serde_json::json!({}),
                snippets: Vec::new(),
            },
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: Some("assistant".to_string()),
                created_at: Some(1_733_000_001_000),
                content: "indexed second line".to_string(),
                extra_json: serde_json::json!({}),
                snippets: Vec::new(),
            },
        ],
        source_id: "local".to_string(),
        origin_host: None,
    };
    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .expect("insert conversation");

    run_expand(
        &session_path,
        Some(db_path),
        None,
        2,
        0,
        Some(RobotFormat::Json),
    )
    .expect("expand should use indexed conversation data for local markdown-backed sessions");
}

#[test]
pub(crate) fn run_expand_requires_indexed_conversation_for_local_markdown_file() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("missing.db");
    let session_path = tmp.path().join("session.md");
    std::fs::write(
        &session_path,
        "# raw aider markdown

This is not JSONL.
",
    )
    .expect("write markdown");

    let err = run_expand(
        &session_path,
        Some(db_path),
        None,
        1,
        0,
        Some(RobotFormat::Json),
    )
    .expect_err(
        "expand should require the indexed conversation for local markdown-backed sessions",
    );

    assert_eq!(err.kind, "indexed-session-required");
}

#[test]
pub(crate) fn run_export_ignores_invalid_local_jsonl_for_indexed_export() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("agent_search.db");
    let storage = SqliteStorage::open(&db_path).expect("open sqlite");

    let agent = Agent {
        id: None,
        slug: "claude_code".to_string(),
        name: "Claude Code".to_string(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).expect("ensure agent");

    // The unreadable file is an opaque identity key: export resolves the
    // indexed snapshot and never parses the source.
    let session_path = tmp.path().join("broken-session.jsonl");
    std::fs::write(&session_path, b"this is not valid jsonl\n").expect("write invalid jsonl");

    let conversation = Conversation {
        id: None,
        agent_slug: "claude_code".to_string(),
        workspace: Some(PathBuf::from("/tmp/ws")),
        external_id: Some("indexed-fallback".to_string()),
        title: Some("Indexed Session".to_string()),
        source_path: session_path.clone(),
        started_at: Some(1_733_000_000_000),
        ended_at: Some(1_733_000_010_000),
        approx_tokens: None,
        metadata_json: serde_json::json!({}),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: Some("me".to_string()),
            created_at: Some(1_733_000_000_000),
            content: "indexed markdown fallback".to_string(),
            extra_json: serde_json::json!({}),
            snippets: Vec::new(),
        }],
        source_id: "local".to_string(),
        origin_host: None,
    };
    storage
        .insert_conversation_tree(agent_id, None, &conversation)
        .expect("insert conversation");

    let output_path = tmp.path().join("export.md");
    run_export(
        &session_path,
        Some(db_path),
        None,
        None,
        ConvExportFormat::Markdown,
        Some(output_path.as_path()),
        false, // clipboard
        false,
        true,
        false, // include_secrets
    )
    .expect("export should use indexed content without touching the invalid JSONL");

    let markdown = std::fs::read_to_string(&output_path).expect("read markdown");
    assert!(markdown.contains("indexed markdown fallback"));
}
