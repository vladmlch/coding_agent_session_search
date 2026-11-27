use coding_agent_search::connectors::{NormalizedConversation, NormalizedMessage};
use coding_agent_search::indexer::{IndexOptions, persist::persist_conversation};
use coding_agent_search::search::query::{FieldMask, SearchClient, SearchFilters};
use coding_agent_search::search::tantivy::{TantivyIndex, index_dir};
use coding_agent_search::storage::sqlite::SqliteStorage;
use tempfile::TempDir;

fn norm_msg(idx: i64) -> NormalizedMessage {
    NormalizedMessage {
        idx,
        role: "user".into(),
        author: None,
        created_at: Some(1_700_000_000_000 + idx),
        content: format!("hello-{idx}"),
        extra: serde_json::json!({}),
        snippets: Vec::new(),
        invocations: Vec::new(),
    }
}

#[test]
fn search_logs_backend_selection() {
    coding_agent_search::ensure_runtime_hooks_registered();
    let trace = TestTracing::new();
    let _guard = trace.install();

    let dir = TempDir::new().unwrap();
    let mut index = TantivyIndex::open_or_create(dir.path()).unwrap();
    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: None,
        title: Some("log test".into()),
        workspace: None,
        source_path: dir.path().join("rollout.jsonl"),
        started_at: Some(1),
        ended_at: Some(2),
        metadata: serde_json::json!({}),
        messages: vec![norm_msg(0)],
    };
    index.add_conversation(&conv).unwrap();
    index.commit().unwrap();

    let client = SearchClient::open(dir.path(), None)
        .unwrap()
        .expect("client");
    client
        .search("hello", SearchFilters::default(), 5, 0, FieldMask::FULL)
        .unwrap();

    let out = trace.output();
    eprintln!("logs: {out}");
    // Pin the exact `search_start` line shape: backend label AND the
    // query string we passed must both appear on the SAME line, not
    // separately across unrelated spans. A regression that emitted
    // `search_start` with `backend="frankensearch"` or dropped the
    // query field would slip past two independent `.contains(...)`
    // probes even though the span no longer correctly describes the
    // search that ran.
    let search_start_line = out
        .lines()
        .find(|line| line.contains("search_start"))
        .unwrap_or_else(|| panic!("trace output must contain a `search_start` event; got:\n{out}"));
    assert!(
        search_start_line.contains("backend=\"quill\""),
        "search_start span must name the quill backend; got line:\n{search_start_line}"
    );
    assert!(
        search_start_line.contains("query=") && search_start_line.contains("hello"),
        "search_start span must record the actual query string `hello`; got line:\n{search_start_line}"
    );
}

#[test]
fn persist_conversation_logs_counts() {
    coding_agent_search::ensure_runtime_hooks_registered();
    let trace = TestTracing::new();
    let _guard = trace.install();

    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("db.sqlite");
    let storage = SqliteStorage::open(&db_path).unwrap();
    let mut index = TantivyIndex::open_or_create(&index_dir(&data_dir).unwrap()).unwrap();

    let conv = NormalizedConversation {
        agent_slug: "tester".into(),
        external_id: Some("ext-log".into()),
        title: Some("persist".into()),
        workspace: None,
        source_path: data_dir.join("src.log"),
        started_at: Some(10),
        ended_at: Some(20),
        metadata: serde_json::json!({}),
        messages: vec![norm_msg(0), norm_msg(1)],
    };

    persist_conversation(&storage, &mut index, &conv).unwrap();

    let out = trace.output();
    assert!(out.contains("persist_conversation"));
    assert!(out.contains("messages=2"));
}

#[test]
fn run_index_does_not_drop_storage_without_explicit_close() {
    coding_agent_search::ensure_runtime_hooks_registered();
    let trace = TestTracing::new();
    let _guard = trace.install();

    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let home_dir = tmp.path().join("home");
    let xdg_dir = tmp.path().join("xdg");
    std::fs::create_dir_all(&home_dir).unwrap();
    std::fs::create_dir_all(&xdg_dir).unwrap();
    let codex_dir = data_dir.join("codex-sessions");
    std::fs::create_dir_all(&codex_dir).unwrap();
    std::fs::write(
        codex_dir.join("rollout-log.jsonl"),
        concat!(
            r#"{"timestamp":"2023-11-14T22:13:20.100Z","type":"session_meta","payload":{"id":"thread-log","cwd":"/tmp/log-test"}}"#,
            "\n",
            r#"{"timestamp":"2023-11-14T22:13:20.100Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}}"#,
            "\n",
            r#"{"timestamp":"2023-11-14T22:13:20.200Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello"}]}}"#,
            "\n",
        ),
    )
    .unwrap();

    // qu81y: env-free — the closed-world roots override below replaces
    // HOME/XDG redirection and CASS_IGNORE_SOURCES_CONFIG entirely (the
    // override suppresses both default detection and sources.toml loading).
    let _ = (&home_dir, &xdg_dir);

    let opts = IndexOptions {
        full: false,
        force_rebuild: false,
        watch: false,
        watch_once_paths: None,
        db_path: data_dir.join("agent_search.db"),
        data_dir,
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".to_string(),
        progress: None,
        watch_interval_secs: 30,
    };

    let mut local_roots = std::collections::HashMap::new();
    local_roots.insert("codex".to_string(), vec![codex_dir.clone()]);
    coding_agent_search::indexer::run_index_with_local_connector_roots(opts, local_roots, None)
        .unwrap();

    let out = trace.output();
    assert!(
        !out.contains("drop_close"),
        "run_index should explicitly close storage instead of relying on Drop: {out}"
    );
}

// Re-export util module so tests can find helpers without extra path noise.
mod util;
use util::TestTracing;
