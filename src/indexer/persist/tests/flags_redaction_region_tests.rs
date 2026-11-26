use super::super::*;
use super::helpers::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::franken_sync::FrankenError;
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use crate::indexer::LexicalPopulationStrategy;
use crate::model::types::{Agent, AgentKind};
use crate::search::tantivy::TantivyIndex;
use crate::storage::sqlite::FrankenStorage;
use serial_test::serial;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

/// gh373/oeu5a: mapping one giant conversation must tick the
/// work-liveness heartbeat per MAP_HEARTBEAT_MESSAGE_STRIDE messages
/// so the stall watchdog sees live work during the minutes-long
/// redaction window (previously zero ticks until the whole batch
/// persisted — the proven exit(70) trigger).
#[test]
fn gh474_singleton_redaction_preserves_output_heartbeat_and_thresholds() {
    const CHILD: &str = "CASS_TEST_GH474_CHILD";
    if dotenvy::var(CHILD).is_err() {
        let mod_rel = module_path!().split_once("::").unwrap().1;
        let exact_filter = format!(
            "{mod_rel}::gh474_singleton_redaction_preserves_output_heartbeat_and_thresholds"
        );
        for mode in ["full", "off"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", &exact_filter, "--nocapture"])
                .env(CHILD, "1")
                .env("CASS_INDEX_REDACTION", mode)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{mode}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        }
        return;
    }
    let enabled = crate::indexer::redact_secrets::redaction_enabled();
    let message_count = 769;
    let secret = "password=abcdefgh12345678"; // Synthetic redaction fixture.
    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: Some("gh474".into()),
        title: Some(secret.into()),
        workspace: Some(PathBuf::from("/workspace/gh474")),
        source_path: PathBuf::from("/logs/gh474.jsonl"),
        started_at: Some(1000),
        ended_at: Some(2000),
        metadata: serde_json::json!({"nested": [{"credential": secret}]}),
        messages: (0..message_count)
            .map(|index| NormalizedMessage {
                idx: (index * 2) as i64,
                role: ["user", "assistant", "tool", "system", "custom"][index % 5].into(),
                author: Some(format!("author-{index}")),
                created_at: Some(1000 + index as i64),
                content: format!("{index}: {} {secret}", "payload ".repeat(256)),
                extra: serde_json::json!({"nested": [{"credential": secret, "index": index}]}),
                snippets: vec![crate::connectors::NormalizedSnippet {
                    file_path: Some(PathBuf::from(format!("file-{index}.rs"))),
                    start_line: Some(1),
                    end_line: Some(2),
                    language: Some("rust".into()),
                    snippet_text: Some(secret.into()),
                }],
                invocations: Vec::new(),
            })
            .collect(),
    };
    let mut serial_redactor = crate::indexer::redact_secrets::MemoizingRedactor::new();
    let serial = map_to_internal_with_redactor(&conv, Some(&mut serial_redactor));
    let serialized = serde_json::to_value(&serial).unwrap();
    assert_eq!(serial.messages[0].content.contains(secret), !enabled);
    for threads in [1, 4] {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();
        pool.install(|| {
            assert_eq!(
                use_inner_message_redaction(std::slice::from_ref(&conv)),
                enabled && threads > 1
            );
            let progress = crate::indexer::IndexingProgress::default();
            let bump = Arc::new(AtomicI64::new(0));
            let result = map_batch_to_internal(
                std::slice::from_ref(&conv),
                PersistHeartbeat::new(Some(&progress), Some(&bump)),
            );
            assert_eq!(serde_json::to_value(&result[0]).unwrap(), serialized);
            assert_eq!(progress.activity.load(Ordering::Relaxed), 4);
            assert!(bump.load(Ordering::Relaxed) > 0);
            let multi = vec![conv.clone(), conv.clone()];
            assert!(!use_inner_message_redaction(&multi));
            let mapped = map_batch_to_internal(&multi, PersistHeartbeat::NONE);
            assert_eq!(
                serde_json::to_value(&mapped).unwrap(),
                serde_json::json!([serialized, serialized])
            );

            let mut boundary = conv.clone();
            boundary.messages.truncate(INNER_REDACTION_MIN_MESSAGES);
            for message in &mut boundary.messages {
                message.content.clear();
                message.snippets.clear();
            }
            boundary.messages[0].content = "x".repeat(INNER_REDACTION_MIN_TEXT_BYTES - 1);
            assert!(!use_inner_message_redaction(std::slice::from_ref(
                &boundary
            )));
            boundary.messages[0].snippets = vec![crate::connectors::NormalizedSnippet {
                file_path: None,
                start_line: None,
                end_line: None,
                language: None,
                snippet_text: Some("x".into()),
            }];
            assert_eq!(
                use_inner_message_redaction(std::slice::from_ref(&boundary)),
                enabled && threads > 1
            );
            boundary.messages.pop();
            assert!(!use_inner_message_redaction(std::slice::from_ref(
                &boundary
            )));
            assert!(!use_inner_message_redaction(&[]));
        });
    }
}

#[test]
fn map_to_internal_heartbeat_ticks_per_message_stride() {
    use crate::connectors::NormalizedConversation;
    use std::sync::atomic::Ordering;

    let message_count = MAP_HEARTBEAT_MESSAGE_STRIDE * 3 + 5;
    let conv = NormalizedConversation {
        agent_slug: "codex".to_string(),
        external_id: Some("giant".to_string()),
        title: Some("giant conversation".to_string()),
        workspace: None,
        source_path: std::path::PathBuf::from("/log/giant.jsonl"),
        started_at: Some(1000),
        ended_at: Some(2000),
        metadata: serde_json::json!({}),
        messages: (0..message_count)
            .map(|j| crate::connectors::NormalizedMessage {
                idx: j as i64,
                role: "user".to_string(),
                author: None,
                created_at: Some(1000 + j as i64),
                content: format!("msg {j}"),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            })
            .collect(),
    };

    let progress = crate::indexer::IndexingProgress::default();
    let heartbeat = PersistHeartbeat::new(Some(&progress), None);
    let mapped = map_to_internal_with_redactor_and_heartbeat(&conv, None, heartbeat);
    assert_eq!(mapped.messages.len(), message_count);
    let ticks = progress.activity.load(Ordering::Relaxed);
    // Ticks fire at message indices 0, STRIDE, 2*STRIDE, 3*STRIDE.
    assert_eq!(
        ticks, 4,
        "expected one tick per started stride (got {ticks} for {message_count} messages)"
    );

    // The heartbeat-free path stays a no-op (no progress handle).
    let unmapped = map_to_internal_with_redactor(&conv, None);
    assert_eq!(unmapped.messages.len(), message_count);
}

// --- Task 7: `NormalizedMessage.invocations` -> `extra.cass.invocations` ---
//
// The persisted shape must match FAD's Antigravity dual-write
// (`planner_calls::invocations_extra`) byte-for-key so the single
// `cass.invocations` extraction branch in export projection serves
// every provider.

fn invocation_fixture() -> Vec<franken_agent_detection::NormalizedInvocation> {
    vec![franken_agent_detection::NormalizedInvocation {
        kind: "tool".to_string(),
        name: "read_file".to_string(),
        raw_name: None,
        call_id: Some("call_1".to_string()),
        arguments: Some(serde_json::json!({"path": "/tmp/a.rs"})),
    }]
}

fn message_with_extra_and_invocations(
    extra: serde_json::Value,
    invocations: Vec<franken_agent_detection::NormalizedInvocation>,
) -> NormalizedMessage {
    NormalizedMessage {
        idx: 0,
        role: "assistant".to_string(),
        author: None,
        created_at: Some(1_700_000_000_000),
        content: "[Tool: read_file]".to_string(),
        extra,
        snippets: Vec::new(),
        invocations,
    }
}

#[test]
fn map_message_to_internal_persists_invocations_under_extra_cass() {
    // Devin-style synthetic extra: no `cass` key yet.
    let message = message_with_extra_and_invocations(
        serde_json::json!({"message_id": "m-7"}),
        invocation_fixture(),
    );
    let mapped = map_message_to_internal(&message, 0, false, None, PersistHeartbeat::NONE);
    assert_eq!(mapped.extra_json["message_id"], "m-7");
    let invocations = mapped.extra_json["cass"]["invocations"]
        .as_array()
        .expect("cass.invocations must be persisted");
    assert_eq!(invocations.len(), 1);
    assert_eq!(
        invocations[0],
        serde_json::json!({
            "kind": "tool",
            "name": "read_file",
            "call_id": "call_1",
            "arguments": {"path": "/tmp/a.rs"},
        }),
        "persisted invocation must keep the FAD NormalizedInvocation \
         field names verbatim (`raw_name` omitted when None)"
    );
}

#[test]
fn map_message_to_internal_merges_invocations_under_existing_cass_metadata() {
    // Compact-ingest shape (`{cass:{model, token_usage, ..}}` from
    // Claude >=32 MiB / Codex >=16 MiB): invocations join the existing
    // `cass` object instead of replacing it.
    let message = message_with_extra_and_invocations(
        serde_json::json!({"cass": {"model": "gpt-5", "token_usage": {"in": 1}}}),
        invocation_fixture(),
    );
    let mapped = map_message_to_internal(&message, 0, false, None, PersistHeartbeat::NONE);
    assert_eq!(mapped.extra_json["cass"]["model"], "gpt-5");
    assert_eq!(mapped.extra_json["cass"]["token_usage"]["in"], 1);
    assert_eq!(
        mapped.extra_json["cass"]["invocations"][0]["name"],
        "read_file"
    );
}

#[test]
fn map_message_to_internal_keeps_source_written_cass_invocations() {
    // Antigravity dual-writes planner invocations at normalization
    // time; the indexer must never clobber or duplicate them.
    let source_written = serde_json::json!({
        "cass": {"invocations": [{"kind": "tool", "name": "planner_call"}]},
        "thinking": "hmm",
    });
    let message = message_with_extra_and_invocations(source_written.clone(), invocation_fixture());
    let mapped = map_message_to_internal(&message, 0, false, None, PersistHeartbeat::NONE);
    assert_eq!(
        mapped.extra_json, source_written,
        "a source-written cass.invocations array stays authoritative"
    );
}

#[test]
fn map_message_to_internal_creates_cass_for_null_extra() {
    let message = message_with_extra_and_invocations(serde_json::Value::Null, invocation_fixture());
    let mapped = map_message_to_internal(&message, 0, false, None, PersistHeartbeat::NONE);
    assert_eq!(
        mapped.extra_json["cass"]["invocations"][0]["call_id"],
        "call_1"
    );
}

#[test]
fn map_message_to_internal_without_invocations_leaves_extra_untouched() {
    // Older archives / non-tool messages: no `cass` key is invented.
    for extra in [
        serde_json::json!({"message_id": "m-1"}),
        serde_json::Value::Null,
        serde_json::json!({"cass": {"model": "claude"}}),
    ] {
        let message = message_with_extra_and_invocations(extra.clone(), Vec::new());
        let mapped = map_message_to_internal(&message, 0, false, None, PersistHeartbeat::NONE);
        assert_eq!(mapped.extra_json, extra);
    }
    // A non-object extra cannot host `cass` and is left alone even
    // when invocations exist (no wrapper shape is invented).
    let scalar = message_with_extra_and_invocations(
        serde_json::Value::String("opaque".to_string()),
        invocation_fixture(),
    );
    let mapped = map_message_to_internal(&scalar, 0, false, None, PersistHeartbeat::NONE);
    assert_eq!(
        mapped.extra_json,
        serde_json::Value::String("opaque".to_string())
    );
}

#[test]
fn map_message_to_internal_invocations_flow_through_redaction() {
    // `should_redact=true` exercises the production redact branch
    // directly — `redact_json` itself is not env-gated.
    let secret = "password=abcdefgh12345678"; // Synthetic redaction fixture.
    let mut invocations = invocation_fixture();
    invocations[0].arguments = Some(serde_json::json!({"cmd": secret}));
    let message =
        message_with_extra_and_invocations(serde_json::json!({"message_id": "m-9"}), invocations);
    let mapped = map_message_to_internal(&message, 0, true, None, PersistHeartbeat::NONE);
    let arguments = mapped.extra_json["cass"]["invocations"][0]["arguments"]
        .as_object()
        .expect("arguments object");
    let cmd = arguments["cmd"].as_str().expect("cmd string");
    assert!(
        !cmd.contains(secret) && cmd.contains("[REDACTED]"),
        "invocation arguments must be secret-redacted like any other \
         extra_json string (got {cmd:?})"
    );
}

/// Reindex regression: the persisted `cass.invocations` must survive
/// the storage round-trip (msgpack `extra_bin`) and come back through
/// `message_extra_to_source_jsonl_line` — the exact decode path the
/// export projection's `cass` branch reads — for a freshly indexed
/// message. Older archives simply lack the key and export unchanged.
#[test]
fn persisted_invocations_survive_storage_round_trip_to_export_decode() {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    let agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "devin".into(),
            name: "Devin".into(),
            version: None,
            kind: AgentKind::Cli,
        })
        .unwrap();
    let conv = NormalizedConversation {
        agent_slug: "devin".into(),
        external_id: Some("task7-invocations".into()),
        title: Some("Task 7 fixture".into()),
        workspace: None,
        source_path: PathBuf::from("/virtual/sessions.db/sess-1"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        metadata: serde_json::json!({}),
        messages: vec![
            NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: None,
                created_at: Some(1_700_000_000_000),
                content: "read the file".into(),
                extra: serde_json::json!({"message_id": "u-1"}),
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
            {
                // `messages` is keyed on (conversation_id, idx) and FAD
                // `reindex_messages` always emits distinct idx values —
                // the fixture must too, or this row collides with the
                // user message's idx 0 and only one row persists.
                let mut assistant = message_with_extra_and_invocations(
                    serde_json::json!({"message_id": "a-1", "thinking": "hmm"}),
                    invocation_fixture(),
                );
                assistant.idx = 1;
                assistant
            },
        ],
    };
    let outcome = storage
        .insert_conversation_tree(agent_id, None, &map_to_internal(&conv))
        .unwrap();
    let fetched = storage.fetch_messages(outcome.conversation_id).unwrap();
    assert_eq!(fetched.len(), 2);
    assert_eq!(
        fetched[0].extra_json["cass"]["invocations"],
        serde_json::Value::Null,
        "user message without invocations gains no cass key"
    );
    let invocations = fetched[1].extra_json["cass"]["invocations"]
        .as_array()
        .expect("persisted cass.invocations must survive extra_bin round-trip");
    assert_eq!(invocations[0]["name"], "read_file");
    assert_eq!(invocations[0]["call_id"], "call_1");
    assert_eq!(invocations[0]["arguments"]["path"], "/tmp/a.rs");
    // The export projection decodes `extra_json` via
    // `message_extra_to_source_jsonl_line`; assert the persisted
    // record is consumable through that exact seam.
    let line = crate::storage::sqlite::message_extra_to_source_jsonl_line(&fetched[1].extra_json)
        .expect("source line");
    let event: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(event["cass"]["invocations"][0]["name"], "read_file");
    assert_eq!(event["thinking"], "hmm");
}

#[test]
#[serial]
fn conversation_metadata_redaction_is_structural_and_fail_closed() {
    let _redact_guard = set_env("CASS_INDEX_REDACTION", "full");
    let metadata = serde_json::json!({
        "api_key=abcdefgh12345678": "first", // ubs:ignore -- synthetic collision fixture, not a credential.
        "password=abcdefgh12345678": "second", // ubs:ignore -- synthetic collision fixture, not a credential.
        "private_material": "prefix\n-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAA",
    });
    let conv = crate::connectors::NormalizedConversation {
        agent_slug: "codex".to_string(),
        external_id: Some("metadata-redaction".to_string()),
        title: None,
        workspace: None,
        source_path: std::path::PathBuf::from("/log/metadata-redaction.jsonl"),
        started_at: None,
        ended_at: None,
        metadata,
        messages: Vec::new(),
    };

    let plain = map_to_internal_with_redactor(&conv, None).metadata_json;
    let mut redactor = crate::indexer::redact_secrets::MemoizingRedactor::with_capacity(8);
    let memoized = map_to_internal_with_redactor(&conv, Some(&mut redactor)).metadata_json;
    assert_eq!(
        plain, memoized,
        "memoized and plain metadata paths diverged"
    );

    let object = plain.as_object().expect("metadata must remain an object");
    assert_eq!(object.len(), 3, "redacted key collision discarded a value");
    let serialized = serde_json::to_string(&plain).expect("redacted metadata must serialize");
    assert!(!serialized.contains("abcdefgh12345678"));
    assert!(!serialized.contains("b3BlbnNzaC1rZXktdjE"));
    assert!(serialized.contains("prefix\\n[REDACTED]"));

    let mut preserved_values = object
        .values()
        .filter_map(serde_json::Value::as_str)
        .filter(|value| matches!(*value, "first" | "second"))
        .collect::<Vec<_>>();
    preserved_values.sort_unstable();
    assert_eq!(preserved_values, ["first", "second"]);
}

#[test]
#[serial]
fn writer_retry_limit_parsing_fallback_and_clamping() {
    {
        let _new_guard = set_env("CASS_INDEXER_WRITER_RETRIES", "11");
        let _legacy_guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT_RETRIES", "9");
        assert_eq!(writer_retry_limit(), 11);
    }
    {
        let _new_guard = set_env("CASS_INDEXER_WRITER_RETRIES", "0");
        let _legacy_guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT_RETRIES", "9");
        assert_eq!(writer_retry_limit(), 9);
    }
    {
        let _new_guard = set_env("CASS_INDEXER_WRITER_RETRIES", "1000000000");
        assert_eq!(writer_retry_limit(), WRITER_RETRY_MAX);
    }
    {
        let _new_guard = set_env("CASS_INDEXER_WRITER_RETRIES", "0");
        let _legacy_guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT_RETRIES", "0");
        assert_eq!(writer_retry_limit(), 6);
    }
}

#[test]
#[serial]
fn wal_autocheckpoint_defaults_follow_bulk_import_mode() {
    let _guard = set_env("CASS_INDEX_WRITER_WAL_AUTOCHECKPOINT_PAGES", "-1");
    // GH#320: bulk import is bounded (not 0/unbounded) so the WAL —
    // and the pager memory that scales with it — cannot grow with
    // the corpus during a full ingest.
    assert_eq!(
        index_writer_wal_autocheckpoint_pages(true),
        BULK_IMPORT_WAL_AUTOCHECKPOINT_PAGES
    );
    assert_eq!(index_writer_wal_autocheckpoint_pages(false), 1000);
}

#[test]
fn defer_lexical_updates_flag_parsing() {
    let _guard = set_env("CASS_DEFER_LEXICAL_UPDATES", "1");
    assert!(defer_lexical_updates_enabled());
}

#[test]
fn retryable_franken_errors_are_detected() {
    let retryable = anyhow::Error::new(FrankenError::BusySnapshot {
        conflicting_pages: "1,2".to_string(),
    });
    assert!(is_retryable_franken_error(&retryable));

    let not_retryable = anyhow::Error::new(FrankenError::ConcurrentUnavailable);
    assert!(!is_retryable_franken_error(&not_retryable));
}

/// Reproducer for the unchanged-session re-ingest cost: the same source
/// bytes persisted twice must keep the first run's `complete` receipt
/// (no republish, `attempts` unchanged).
#[test]
fn unchanged_session_reingest_keeps_generation_receipt() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("reingest.db");
    let storage = create_franken_db(&db_path);
    let conv = NormalizedConversation {
        agent_slug: "claude_code".into(),
        external_id: Some("unchanged-reingest".into()),
        title: Some("Unchanged re-ingest".into()),
        workspace: Some(dir.path().join("workspace")),
        source_path: dir.path().join("unchanged.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_050),
        metadata: serde_json::json!({
            "model": "fixture-model",
            "tags": ["alpha", "beta"],
            "nested": {"b": 2, "a": 1}
        }),
        messages: vec![
            NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: None,
                created_at: Some(1_700_000_000_000),
                content: "first question".into(),
                extra: serde_json::json!({"uuid": "u-0"}),
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
            NormalizedMessage {
                idx: 1,
                role: "assistant".into(),
                author: None,
                created_at: Some(1_700_000_000_050),
                content: "first answer".into(),
                extra: serde_json::json!({"uuid": "a-1"}),
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
        ],
    };
    let batch = std::slice::from_ref(&conv);
    persist_conversations_batched(
        &storage,
        None,
        batch,
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        false,
    )
    .unwrap();
    let first = crate::viewer_extensions::facts::ingest_receipt_for(storage.raw(), 1)
        .unwrap()
        .expect("first ingest writes a receipt");
    assert_eq!(first.state, "complete", "{first:?}");

    persist_conversations_batched(
        &storage,
        None,
        batch,
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        false,
    )
    .unwrap();
    let second = crate::viewer_extensions::facts::ingest_receipt_for(storage.raw(), 1)
        .unwrap()
        .expect("second ingest keeps a receipt");
    assert_eq!(
        second.attempts, first.attempts,
        "unchanged re-ingest must skip the generation: first={first:?} second={second:?}"
    );
}

/// A tiny two-message claude_code session unique per `ordinal` — the
/// `region_reingest_session` shape, parameterized so a grind fixture
/// can hold hundreds of distinct sessions.
fn grind_region_session(dir: &std::path::Path, ordinal: i64) -> NormalizedConversation {
    NormalizedConversation {
        agent_slug: "claude_code".into(),
        external_id: Some(format!("region-grind-{ordinal}")),
        title: Some(format!("Region grind {ordinal}")),
        workspace: Some(dir.join("workspace")),
        source_path: dir.join(format!("region-grind-{ordinal}.jsonl")),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_050),
        metadata: serde_json::json!({"model": "fixture-model"}),
        messages: vec![
            NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: None,
                created_at: Some(1_700_000_000_000),
                content: format!("question {ordinal}"),
                extra: serde_json::json!({"uuid": format!("u-{ordinal}-0")}),
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
            NormalizedMessage {
                idx: 1,
                role: "assistant".into(),
                author: None,
                created_at: Some(1_700_000_000_050),
                content: format!("answer {ordinal}"),
                extra: serde_json::json!({"uuid": format!("u-{ordinal}-1")}),
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
        ],
    }
}

/// Build a stable Cass lexical document identity for a conversation slot.
fn pag_doc_identity(source_path: &std::path::Path, conversation_id: i64, slot: u64) -> String {
    frankensearch::quill::cass::cass_document_identity(
        "local",
        frankensearch::quill::cass::CassConversationKey {
            source_path: &source_path.to_string_lossy(),
            id: Some(conversation_id),
        },
        slot,
    )
}

/// Live canonical+region document identities matching a term.
fn pag_live_doc_ids(index: &TantivyIndex, term: &str) -> std::collections::BTreeSet<String> {
    let reader = index.reader().unwrap();
    let parser = frankensearch::quill::query::CassQueryParser::new(
        frankensearch::quill::schema::CASS_SEMANTIC_SCHEMA,
    )
    .unwrap();
    let parsed = parser.parse(
        term,
        &frankensearch::quill::query::CassQueryFilters::default(),
    );
    let page = crate::search::quill_bridge::search_paginated(&reader, &parsed.query, 512, 0, true)
        .unwrap();
    page.hits
        .iter()
        .map(|hit| hit.document_id.clone())
        .collect()
}

fn pag_conversation_count(storage: &FrankenStorage) -> i64 {
    storage
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM conversations",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .unwrap()
}

fn pag_conversation_id(storage: &FrankenStorage, external_id: &str) -> Option<i64> {
    storage
        .raw()
        .query_map_collect(
            "SELECT id FROM conversations WHERE external_id = ?1",
            &[ParamValue::from(external_id)],
            |row| row.get_typed::<i64>(0),
        )
        .unwrap()
        .into_iter()
        .next()
}

/// FAD paginated-thread merge, section 7.3: an archive holding the old
/// per-segment Codex rows (one conversation per rollout segment) must
/// retire the continuation row when the merged thread is persisted,
/// drop its derived lexical documents, and the viewer bridge list must
/// then show one row. The cleanup is keyed on metadata.segments[].external_id.
/// This models the persist step of a rescan (the scan calls the same
/// batched persist path). Synthetic fixture only.
#[test]
#[serial]
fn codex_merged_thread_retires_stale_per_segment_rows() {
    let _defer = set_env("CASS_DEFER_LEXICAL_UPDATES", "0");
    let dir = tempfile::TempDir::new().unwrap();
    let storage = create_franken_db(&dir.path().join("paginated.db"));
    crate::viewer_extensions::schema::ensure_schema(storage.raw()).unwrap();
    let index_path = crate::search::tantivy::index_dir(dir.path()).unwrap();
    let mut index = TantivyIndex::open_or_create(&index_path).unwrap();

    let a = "01a1d1b4-5529-7cd3-97db-d52326514bc2";
    let b = "01a1d1ba-a4da-7b00-92b0-0368cfada0b2";
    let sessions = dir.path().join("sessions/2026/11/12");
    let original = sessions.join(format!("rollout-2026-11-12T22-40-18-{a}.jsonl"));
    let continuation = sessions.join(format!("rollout-2026-11-12T22-47-12-{a}_{b}.jsonl"));
    let orig_ext = format!("sessions/2026/11/12/rollout-2026-11-12T22-40-18-{a}");
    let cont_ext = format!("sessions/2026/11/12/rollout-2026-11-12T22-47-12-{a}_{b}");

    let msg = |idx: i64, role: &str, text: &str| NormalizedMessage {
        idx,
        role: role.into(),
        author: None,
        created_at: Some(1_762_980_000_000 + idx * 1_000),
        content: text.into(),
        extra: serde_json::json!({}),
        snippets: Vec::new(),
        invocations: Vec::new(),
    };
    let base = |path: std::path::PathBuf,
                ext: String,
                msgs: Vec<NormalizedMessage>,
                meta: serde_json::Value| NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: Some(ext),
        title: Some("paginated thread".into()),
        workspace: Some(dir.path().join("workspace")),
        source_path: path,
        started_at: Some(1_762_980_000_000),
        ended_at: Some(1_762_980_060_000),
        metadata: meta,
        messages: msgs,
    };
    let orig_msgs = vec![
        msg(0, "user", "sharedmarker first question"),
        msg(1, "assistant", "sharedmarker first answer"),
    ];
    let cont_msgs = vec![
        msg(0, "user", "sharedmarker second question"),
        msg(1, "assistant", "sharedmarker second answer"),
    ];

    // Old FAD shape: one conversation per rollout segment.
    let old_original = base(
        original.clone(),
        orig_ext.clone(),
        orig_msgs.clone(),
        serde_json::json!({"source":"rollout"}),
    );
    let old_continuation = base(
        continuation.clone(),
        cont_ext.clone(),
        cont_msgs.clone(),
        serde_json::json!({"source":"rollout"}),
    );
    persist_conversations_batched(
        &storage,
        Some(&mut index),
        &[old_original, old_continuation],
        LexicalPopulationStrategy::IncrementalInline,
        false,
    )
    .unwrap();
    index.commit_with_region_doc_stamps(storage.raw()).unwrap();
    assert_eq!(
        pag_conversation_count(&storage),
        2,
        "seeded archive holds one row per segment"
    );
    let orig_id = pag_conversation_id(&storage, &orig_ext).expect("original row");
    let cont_id = pag_conversation_id(&storage, &cont_ext).expect("continuation row");
    let retired_canonical: Vec<String> = (0..2)
        .map(|i| pag_doc_identity(&continuation, cont_id, i))
        .collect();
    let live_before = pag_live_doc_ids(&index, "sharedmarker");
    for id in &retired_canonical {
        assert!(
            live_before.contains(id),
            "seeded continuation doc must be live before retirement: {id}"
        );
    }

    // Merged thread (new FAD shape): identity is the original segment,
    // metadata.segments lists both segments' former external ids.
    let mut merged_msgs = orig_msgs.clone();
    merged_msgs.push(msg(2, "user", "sharedmarker second question"));
    merged_msgs.push(msg(3, "assistant", "sharedmarker second answer"));
    let merged = base(
        original.clone(),
        orig_ext.clone(),
        merged_msgs,
        serde_json::json!({
            "source":"rollout", "thread_id": a, "segment_count": 2,
            "segments": [
                {"path": original, "external_id": orig_ext, "kind":"original"},
                {"path": continuation, "external_id": cont_ext, "kind":"continuation"},
            ],
        }),
    );
    persist_conversations_batched(
        &storage,
        Some(&mut index),
        &[merged],
        LexicalPopulationStrategy::IncrementalInline,
        false,
    )
    .unwrap();
    index.commit_with_region_doc_stamps(storage.raw()).unwrap();

    // The continuation row is retired; the identity row survives with all four messages.
    assert_eq!(
        pag_conversation_count(&storage),
        1,
        "the stale per-segment row must be retired"
    );
    assert_eq!(
        pag_conversation_id(&storage, &cont_ext),
        None,
        "continuation row gone"
    );
    assert_eq!(
        pag_conversation_id(&storage, &orig_ext),
        Some(orig_id),
        "identity row survives"
    );
    let surviving_messages: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM messages WHERE conversation_id = ?1",
            &[ParamValue::from(orig_id)],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(
        surviving_messages, 4,
        "merged identity keeps every segment's messages"
    );
    let orphan_regions: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM viewer_search_regions WHERE conversation_id = ?1",
            &[ParamValue::from(cont_id)],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(orphan_regions, 0, "retired row's region rows cascade away");

    // The retired row's canonical documents are gone; the merged identity's remain.
    let live_after = pag_live_doc_ids(&index, "sharedmarker");
    for id in &retired_canonical {
        assert!(
            !live_after.contains(id),
            "retired continuation doc must be removed: {id}"
        );
    }
    for i in 0..4u64 {
        let id = pag_doc_identity(&original, orig_id, i);
        assert!(
            live_after.contains(id.as_str()),
            "merged identity doc must stay live: {id}"
        );
    }

    // The viewer bridge list lane is binary-only; its one-row assertion
    // for a retired thread lives in tests/viewer_bridge_extensions.rs
    // (codex_merged_thread_retirement_shows_one_list_row).
}

/// `(region_version, docs_state, count)` rows of the region marks
/// table, the same audit the A/B reproduction ran on the real archive.
fn region_mark_state_histogram(storage: &FrankenStorage) -> Vec<(i64, String, i64)> {
    storage
        .raw()
        .query_map_collect(
            "SELECT region_version, docs_state, COUNT(*) \
             FROM viewer_search_region_marks \
             GROUP BY region_version, docs_state \
             ORDER BY region_version, docs_state",
            &[] as &[ParamValue],
            |row| {
                Ok((
                    row.get_typed::<i64>(0)?,
                    row.get_typed::<String>(1)?,
                    row.get_typed::<i64>(2)?,
                ))
            },
        )
        .unwrap()
}

/// The incremental lane's whole-archive republish (a REGION_VERSION
/// bump leaves every marks row stale, so every receipt demands a
/// re-projection) must defer region-doc staging to ONE bounded drain
/// before the run's final commit. Staging per persist batch published
/// one successor segment per session on the real archive — the
/// streaming limits make every oversized session its own batch —
/// which measured ~3 s per session on a 6,107-session archive.
#[test]
#[serial]
fn republish_grind_defers_region_staging_to_one_final_drain() {
    let _defer = set_env("CASS_DEFER_LEXICAL_UPDATES", "0");
    let dir = tempfile::TempDir::new().unwrap();
    let storage = create_franken_db(&dir.path().join("grind.db"));
    let index_path = crate::search::tantivy::index_dir(dir.path()).unwrap();
    let mut index = TantivyIndex::open_or_create(&index_path).unwrap();
    let session_count = REGION_DOC_REPUBLISH_DEFER_MIN_STALE_SESSIONS + 1;
    let convs: Vec<_> = (0..session_count)
        .map(|ordinal| grind_region_session(dir.path(), ordinal))
        .collect();

    // Seed: one persist call = one batch (no oversized conversation
    // to slice). Its prelude sweep finds no pending marks on a
    // fresh archive; the per-outcome hook then stages each
    // session's region docs additively (a fresh identity publishes
    // nothing per upsert), and the final commit stamps every mark
    // published at the current REGION_VERSION — the pre-bump state.
    persist_conversations_batched(
        &storage,
        Some(&mut index),
        &convs,
        LexicalPopulationStrategy::IncrementalInline,
        false,
    )
    .unwrap();
    index.commit_with_region_doc_stamps(storage.raw()).unwrap();
    assert_eq!(
        region_mark_state_histogram(&storage),
        vec![(
            crate::viewer_extensions::search_regions::REGION_VERSION,
            crate::viewer_extensions::search_regions::DOCS_PUBLISHED.to_string(),
            session_count
        )],
        "the seeded archive must hold every mark published at the current version"
    );
    let seeded_segments = index.segment_count();

    // The durable state a REGION_VERSION bump creates: every marks row
    // stale for its old version, still reading `published` — invisible
    // to the pending sweep, and each session's receipt now demands a
    // re-projection on its next persist.
    storage
        .raw()
        .execute("UPDATE viewer_search_region_marks SET region_version = 5")
        .unwrap();
    assert_eq!(
        crate::viewer_extensions::search_regions::stale_region_version_session_count(storage.raw())
            .unwrap(),
        session_count
    );

    // The lane's latch: the scan entry calls this once, before the
    // first batch, and the stale count is what it reads.
    maybe_defer_region_doc_convergence_for_republish(storage.raw(), &mut index).unwrap();
    assert!(index.region_doc_convergence_deferred_to_final_drain());

    // The grind: one persist batch per conversation, no commits
    // between — the batch-of-one shape every oversized agent session
    // produces on the streaming lane. The latched prelude must stage
    // NOTHING: no upsert (so no publish), no queued stamp; the marks
    // accumulate honestly `pending`.
    for conv in &convs {
        persist_conversations_batched(
            &storage,
            Some(&mut index),
            std::slice::from_ref(conv),
            LexicalPopulationStrategy::IncrementalInline,
            false,
        )
        .unwrap();
    }
    assert_eq!(
        index.pending_region_doc_stamp_count(),
        0,
        "the deferred prelude must not queue any region-doc stamp during the grind"
    );
    assert_eq!(
        index.segment_count(),
        seeded_segments,
        "the deferred prelude must not publish during the grind: seeded={seeded_segments} after={}",
        index.segment_count()
    );
    assert_eq!(
        region_mark_state_histogram(&storage),
        vec![(
            crate::viewer_extensions::search_regions::REGION_VERSION,
            crate::viewer_extensions::search_regions::DOCS_PENDING.to_string(),
            session_count
        )],
        "every re-projected session must hold an honest pending mark at the current version"
    );

    // The final drain + commit — the production shape at the run's
    // end: one bounded pass stages the whole accumulated set (a
    // handful of upserts, not one per session) and the commit stamps
    // them all `published` together.
    index
        .converge_pending_search_region_docs(storage.raw(), i64::MAX)
        .unwrap();
    assert_eq!(
        index.pending_region_doc_stamp_count(),
        session_count as usize,
        "the drain queues one stamp per converged session"
    );
    index.commit_with_region_doc_stamps(storage.raw()).unwrap();
    // The drain's ONE bounded upsert publishes successors for the
    // live segments whose identities it replaces (a handful, bounded
    // by the segment count — 6 on this fixture), and the commit's
    // stale-identity delete publishes one more successor; the
    // failure this guards against is one publish per SESSION.
    assert!(
        index.segment_count() <= seeded_segments + 32,
        "the drain must publish a bounded handful of segments for the whole run: seeded={seeded_segments} after={}",
        index.segment_count()
    );
    assert_eq!(
        region_mark_state_histogram(&storage),
        vec![(
            crate::viewer_extensions::search_regions::REGION_VERSION,
            crate::viewer_extensions::search_regions::DOCS_PUBLISHED.to_string(),
            session_count
        )],
        "the final commit must stamp every converged mark published"
    );
    assert_eq!(
        crate::viewer_extensions::search_regions::stale_region_version_session_count(storage.raw())
            .unwrap(),
        0,
        "the drained archive must hold no stale-version marks"
    );
}

/// Below the deferral threshold the latch must NOT engage: ordinary
/// incremental runs, watch events and crash leftovers keep today's
/// converge-with-your-batch behavior.
#[test]
#[serial]
fn republish_deferral_engages_only_above_the_stale_threshold() {
    let _defer = set_env("CASS_DEFER_LEXICAL_UPDATES", "0");
    let dir = tempfile::TempDir::new().unwrap();
    let storage = create_franken_db(&dir.path().join("below.db"));
    let index_path = crate::search::tantivy::index_dir(dir.path()).unwrap();
    let mut index = TantivyIndex::open_or_create(&index_path).unwrap();
    let convs: Vec<_> = (0..3)
        .map(|ordinal| grind_region_session(dir.path(), ordinal))
        .collect();
    persist_conversations_batched(
        &storage,
        Some(&mut index),
        &convs,
        LexicalPopulationStrategy::IncrementalInline,
        false,
    )
    .unwrap();
    index.commit_with_region_doc_stamps(storage.raw()).unwrap();
    storage
        .raw()
        .execute("UPDATE viewer_search_region_marks SET region_version = 5")
        .unwrap();

    maybe_defer_region_doc_convergence_for_republish(storage.raw(), &mut index).unwrap();
    assert!(
        !index.region_doc_convergence_deferred_to_final_drain(),
        "three stale marks must not latch the deferral"
    );
}

/// Detection feeds the re-projection lane from the PERSISTED
/// extension tables alone: every staleness arm — stale marks, a
/// MISSING marks row, stale facts, and both projection versions —
/// is detected while a current session is not, and there is NO
/// source-side gating at all: a session without a source path
/// and a remote-mirror session stay detected, because the lane
/// replays the persisted archive, not the source.
#[test]
#[serial]
fn reproject_detection_covers_marks_facts_and_projection_staleness() {
    let _defer = set_env("CASS_DEFER_LEXICAL_UPDATES", "0");
    use crate::viewer_extensions::facts::FACTS_EXTRACTOR_VERSION;
    use crate::viewer_extensions::projection::PROJECTION_VERSION;
    use crate::viewer_extensions::search_regions::{
        REGION_VERSION, region_reproject_stale_session_ids,
    };

    let dir = tempfile::TempDir::new().unwrap();
    let storage = create_franken_db(&dir.path().join("detect.db"));
    let index_path = crate::search::tantivy::index_dir(dir.path()).unwrap();
    let mut index = TantivyIndex::open_or_create(&index_path).unwrap();
    let convs: Vec<_> = (0..6i64)
        .map(|ordinal| grind_region_session(dir.path(), ordinal))
        .collect();
    persist_conversations_batched(
        &storage,
        Some(&mut index),
        &convs,
        LexicalPopulationStrategy::IncrementalInline,
        false,
    )
    .unwrap();
    index.commit_with_region_doc_stamps(storage.raw()).unwrap();
    let db = storage.raw();

    // One staleness arm per conversation (ids are 1..=6 in insert
    // order on the fresh archive); conversation 6 stays current.
    let old_version = REGION_VERSION - 1;
    db.execute_compat(
        "UPDATE viewer_search_region_marks SET region_version = ?1 \
         WHERE conversation_id = 1",
        &[ParamValue::from(old_version)],
    )
    .unwrap();
    db.execute_compat(
        "UPDATE viewer_session_facts SET extractor_version = ?1 \
         WHERE conversation_id = 2",
        &[ParamValue::from(FACTS_EXTRACTOR_VERSION - 1)],
    )
    .unwrap();
    db.execute_compat(
        "UPDATE viewer_session_projection SET projection_version = ?1 \
         WHERE conversation_id = 3",
        &[ParamValue::from(PROJECTION_VERSION - 1)],
    )
    .unwrap();
    db.execute_compat(
        "UPDATE viewer_session_projection SET extractor_version = ?1 \
         WHERE conversation_id = 4",
        &[ParamValue::from(FACTS_EXTRACTOR_VERSION - 1)],
    )
    .unwrap();
    db.execute("DELETE FROM viewer_search_region_marks WHERE conversation_id = 5")
        .unwrap();

    let detected = region_reproject_stale_session_ids(db).unwrap();
    assert_eq!(detected, vec![1, 2, 3, 4, 5]);

    // No source-side gating at all: a session without a source
    // path stays detected — the lane replays the persisted
    // archive, not the source.
    db.execute("UPDATE conversations SET source_path = '' WHERE id = 5")
        .unwrap();
    assert_eq!(
        region_reproject_stale_session_ids(db).unwrap(),
        vec![1, 2, 3, 4, 5],
        "a session without a source path is still a re-projection target"
    );

    // Remote-mirror sessions converge through the same archive
    // replay — they are detected too. (`conversations.source_id`
    // carries a foreign key into the `sources` table, so the
    // fixture needs the row.)
    db.execute(
        "INSERT OR REPLACE INTO sources \
         (id, kind, host_label, created_at, updated_at) \
         VALUES ('remote', 'ssh', 'remote fixture', 0, 0)",
    )
    .unwrap();
    db.execute_compat(
        "UPDATE viewer_search_region_marks SET region_version = ?1 \
         WHERE conversation_id = 6",
        &[ParamValue::from(old_version)],
    )
    .unwrap();
    db.execute("UPDATE conversations SET source_id = 'remote' WHERE id = 6")
        .unwrap();
    assert_eq!(
        region_reproject_stale_session_ids(db).unwrap(),
        vec![1, 2, 3, 4, 5, 6],
        "a remote session is still a re-projection target"
    );
}

fn published_receipt(storage: &FrankenStorage) -> crate::viewer_extensions::facts::IngestReceipt {
    crate::viewer_extensions::facts::ingest_receipt_for(storage.raw(), 1)
        .unwrap()
        .expect("ingest receipt for conversation 1")
}

/// Unchanged session on the incremental lane: the generation skips
/// (`SkippedUnchanged`, receipt untouched) and the region documents are
/// neither re-upserted nor re-stamped.
#[test]
#[serial]
fn unchanged_session_skips_generation_and_region_upsert() {
    let _defer = set_env("CASS_DEFER_LEXICAL_UPDATES", "0");
    let dir = tempfile::TempDir::new().unwrap();
    let storage = create_franken_db(&dir.path().join("unchanged.db"));
    let index_path = crate::search::tantivy::index_dir(dir.path()).unwrap();
    let mut index = TantivyIndex::open_or_create(&index_path).unwrap();
    let conv = region_reingest_session(dir.path(), "first answer");

    incremental_lane_pass(&storage, &mut index, &conv);
    let first = published_receipt(&storage);
    assert_eq!(first.state, "complete", "{first:?}");
    let (_, _, _, docs_state) =
        crate::viewer_extensions::search_regions::region_mark(storage.raw(), 1)
            .unwrap()
            .expect("region marks row after the first pass");
    assert_eq!(
        docs_state,
        crate::viewer_extensions::search_regions::DOCS_PUBLISHED
    );

    incremental_lane_pass(&storage, &mut index, &conv);
    let outcome = crate::viewer_extensions::projection::publish_ingest_generation(
        storage.raw(),
        &map_to_internal(&conv),
        1,
    )
    .unwrap();
    assert_eq!(
        outcome,
        crate::viewer_extensions::projection::GenerationPublishOutcome::SkippedUnchanged
    );
    assert_eq!(published_receipt(&storage).attempts, first.attempts);

    assert_eq!(
        index
            .stage_search_region_docs_unless_published(storage.raw(), 1)
            .unwrap(),
        0,
        "a published, current session upserts no region documents"
    );
    assert_eq!(index.pending_region_doc_stamp_count(), 0);
}

/// Changed session: the generation republishes at the new revision and
/// the region documents restage and re-stamp `published` at that revision.
#[test]
#[serial]
fn changed_session_republishes_and_restages_region_docs() {
    let _defer = set_env("CASS_DEFER_LEXICAL_UPDATES", "0");
    let dir = tempfile::TempDir::new().unwrap();
    let storage = create_franken_db(&dir.path().join("changed.db"));
    let index_path = crate::search::tantivy::index_dir(dir.path()).unwrap();
    let mut index = TantivyIndex::open_or_create(&index_path).unwrap();

    incremental_lane_pass(
        &storage,
        &mut index,
        &region_reingest_session(dir.path(), "first answer"),
    );
    let first = published_receipt(&storage);

    incremental_lane_pass(
        &storage,
        &mut index,
        &region_reingest_session(dir.path(), "second answer"),
    );
    let after = published_receipt(&storage);
    assert_eq!(after.state, "complete", "{after:?}");
    assert_ne!(after.source_revision, first.source_revision);
    assert!(
        after.attempts > first.attempts,
        "a changed session republishes: first={first:?} after={after:?}"
    );
    let (_, mark_revision, _, docs_state) =
        crate::viewer_extensions::search_regions::region_mark(storage.raw(), 1)
            .unwrap()
            .expect("region marks row after the changed pass");
    assert_eq!(
        docs_state,
        crate::viewer_extensions::search_regions::DOCS_PUBLISHED
    );
    assert_eq!(mark_revision, after.source_revision);
}
