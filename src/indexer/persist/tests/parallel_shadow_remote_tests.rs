use super::super::*;
use super::helpers::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::indexer::LexicalPopulationStrategy;
use crate::model::conversation_packet::{ConversationPacket, ConversationPacketProvenance};
use crate::search::tantivy::TantivyIndex;
use crate::storage::sqlite::FrankenStorage;
use serial_test::serial;

#[test]
#[serial]
fn persist_conversations_batched_falls_back_for_duplicate_keys() {
    use crate::connectors::NormalizedConversation;
    use crate::franken_sync::compat::{ConnectionExt, RowExt};
    use crate::search::tantivy::TantivyIndex;
    use crate::sources::provenance::{Source, SourceKind};

    let _begin_guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT", "1");
    let _chunk_guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT_CHUNK_SIZE", "1");

    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let index_path = dir.path().join("tantivy");

    let storage = create_franken_db(&db_path);
    let mut t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    storage
        .upsert_source(&Source {
            id: "remote-source".into(),
            kind: SourceKind::Ssh,
            host_label: Some("example-host".into()),
            machine_id: None,
            platform: None,
            config_json: None,
            created_at: None,
            updated_at: None,
        })
        .unwrap();
    let metadata = serde_json::json!({
        "cass": {
            "origin": {
                "source_id": "remote-source",
                "host": "example-host"
            }
        }
    });

    let convs = vec![
        NormalizedConversation {
            agent_slug: "shared-agent".into(),
            external_id: Some("dup-session".into()),
            title: Some("Shared Session".into()),
            workspace: Some(std::path::PathBuf::from("/ws/shared")),
            source_path: std::path::PathBuf::from("/log/first.jsonl"),
            started_at: Some(1_000),
            ended_at: Some(1_010),
            metadata: metadata.clone(),
            messages: vec![NormalizedMessage {
                idx: 2,
                role: "user".into(),
                author: Some("tester".into()),
                created_at: Some(1_002),
                content: "third".into(),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            }],
        },
        NormalizedConversation {
            agent_slug: "shared-agent".into(),
            external_id: Some("dup-session".into()),
            title: Some("Shared Session".into()),
            workspace: Some(std::path::PathBuf::from("/ws/shared")),
            source_path: std::path::PathBuf::from("/log/second.jsonl"),
            started_at: Some(1_000),
            ended_at: Some(1_020),
            metadata,
            messages: vec![
                NormalizedMessage {
                    idx: 0,
                    role: "user".into(),
                    author: Some("tester".into()),
                    created_at: Some(1_000),
                    content: "first".into(),
                    extra: serde_json::json!({}),
                    snippets: vec![],
                    invocations: Vec::new(),
                },
                NormalizedMessage {
                    idx: 1,
                    role: "assistant".into(),
                    author: Some("tester".into()),
                    created_at: Some(1_001),
                    content: "second".into(),
                    extra: serde_json::json!({}),
                    snippets: vec![],
                    invocations: Vec::new(),
                },
            ],
        },
    ];

    persist_conversations_batched(
        &storage,
        Some(&mut t_index),
        &convs,
        LexicalPopulationStrategy::IncrementalInline,
        false,
    )
    .expect("duplicate-key batch should fall back to serial path");

    let reader = FrankenStorage::open(&db_path).unwrap();
    let conversation_count: i64 = reader
        .raw()
        .query_row_map("SELECT COUNT(*) FROM conversations", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(conversation_count, 1);

    let stored_indices: Vec<i64> = reader
        .raw()
        .query_map_collect("SELECT idx FROM messages ORDER BY idx", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(stored_indices, vec![0, 1, 2]);

    t_index.commit().unwrap();
}

#[test]
#[serial]
fn persist_conversations_batched_parallel_pre_map_preserves_content_and_order() {
    // Regression test for the perf refactor that hoists map_to_internal
    // out of the writer transaction and runs it on rayon. This test
    // asserts that the parallel pre-compute does NOT change the
    // persisted content, message ordering, or redaction behaviour.
    use crate::connectors::NormalizedConversation;
    use crate::franken_sync::compat::{ConnectionExt, RowExt};
    use crate::search::tantivy::TantivyIndex;

    let _begin_guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT", "0");
    // Force redaction on so we exercise the heavier allocation path
    // that is the real target of the parallel hoist.
    let _redact_guard = set_env("CASS_REDACT_SECRETS", "1");

    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("parallel-pre-map.db");
    let index_path = dir.path().join("tantivy");

    let storage = create_franken_db(&db_path);
    let mut t_index = TantivyIndex::open_or_create(&index_path).unwrap();

    // Enough conversations that the parallel hoist will actually fan
    // out across rayon workers on a multi-core host; small enough that
    // the serial writer loop still completes quickly.
    let convs: Vec<NormalizedConversation> = (0..32)
        .map(|i| NormalizedConversation {
            agent_slug: format!("agent-{}", i % 4),
            external_id: Some(format!("session-{i}")),
            // Secret format must match the Stripe redaction regex in
            // redact_secrets.rs: `\b[spr]k_live_[A-Za-z0-9]{20,}`.
            title: Some(format!(
                "Title {i} with sk_live_ABCdef0123456789AAAAbbbb{i:04}"
            )),
            workspace: Some(std::path::PathBuf::from(format!("/ws/proj-{}", i % 3))),
            source_path: std::path::PathBuf::from(format!("/log/s{i}.jsonl")),
            started_at: Some(1_000 + i as i64),
            ended_at: Some(1_010 + i as i64),
            metadata: serde_json::json!({}),
            messages: vec![
                NormalizedMessage {
                    idx: 0,
                    role: "user".into(),
                    author: Some("tester".into()),
                    created_at: Some(1_000 + i as i64),
                    content: format!("hello from conv {i}"),
                    extra: serde_json::json!({}),
                    snippets: vec![],
                    invocations: Vec::new(),
                },
                NormalizedMessage {
                    idx: 1,
                    role: "assistant".into(),
                    author: Some("tester".into()),
                    created_at: Some(1_001 + i as i64),
                    content: format!("reply {i}"),
                    extra: serde_json::json!({}),
                    snippets: vec![],
                    invocations: Vec::new(),
                },
            ],
        })
        .collect();

    persist_conversations_batched(
        &storage,
        Some(&mut t_index),
        &convs,
        LexicalPopulationStrategy::IncrementalInline,
        false,
    )
    .expect("parallel pre-map serial batched path should persist all conversations");

    let reader = FrankenStorage::open(&db_path).unwrap();
    let conversation_count: i64 = reader
        .raw()
        .query_row_map("SELECT COUNT(*) FROM conversations", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(conversation_count, convs.len() as i64);

    let message_count: i64 = reader
        .raw()
        .query_row_map("SELECT COUNT(*) FROM messages", &[], |row| row.get_typed(0))
        .unwrap();
    assert_eq!(message_count, (convs.len() * 2) as i64);

    // Titles must be persisted in their redacted form. If the hoist
    // accidentally dropped the redaction branch, raw "sk_live_..."
    // substrings would survive here.
    let titles: Vec<String> = reader
        .raw()
        .query_map_collect(
            "SELECT title FROM conversations WHERE title IS NOT NULL ORDER BY id",
            &[],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(titles.len(), convs.len());
    for title in &titles {
        assert!(
            !title.contains("sk_live_"),
            "title should have been redacted but contained a live secret marker: {title}"
        );
    }

    t_index.commit().unwrap();
}

#[test]
#[serial]
fn incremental_lexical_redaction_matches_canonical_rebuild() {
    use crate::search::query::{FieldMask, SearchClient, SearchFilters};

    let _begin_guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT", "0");
    let _defer_guard = set_env("CASS_DEFER_LEXICAL_UPDATES", "0");
    let _redact_guard = set_env("CASS_REDACT_SECRETS", "1");

    let dir = tempfile::TempDir::new().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
    let index_path = crate::search::tantivy::index_dir(&data_dir).unwrap();

    let storage = create_franken_db(&db_path);
    let mut t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    // Joined at compile time from two literals (no contiguous source literal)
    // so the value still exercises redaction without tripping secret scanners.
    let secret = concat!("sk_live_", "ABCdef0123456789AAAAbbbb0007");
    let secret_tail = secret.trim_start_matches("sk_live_");
    let safe_canary = "canary0007";
    let conv = NormalizedConversation {
        agent_slug: "codex".to_string(),
        external_id: Some("incremental-redaction".to_string()),
        title: Some(format!("Sensitive title {secret}")),
        workspace: Some(std::path::PathBuf::from("/work/private")),
        source_path: std::path::PathBuf::from("/logs/private-session.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_001_000),
        metadata: serde_json::json!({
            "cass": {
                "origin": {
                    "source_id": "work-laptop",
                    "kind": "ssh",
                    "host": "devbox"
                }
            }
        }),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "user".to_string(),
            author: Some("tester".to_string()),
            created_at: Some(1_700_000_000_000),
            content: format!("safe marker {safe_canary}; secret {secret}"),
            extra: serde_json::json!({"credential": secret}),
            snippets: Vec::new(),
            invocations: Vec::new(),
        }],
    };

    persist_conversations_batched(
        &storage,
        Some(&mut t_index),
        &[conv],
        LexicalPopulationStrategy::IncrementalInline,
        false,
    )
    .expect("incremental persistence should succeed");
    t_index.commit().unwrap();
    drop(t_index);

    let stable_hit = |client: &SearchClient| {
        let leaked = client
            .search(
                secret_tail,
                SearchFilters::default(),
                10,
                0,
                FieldMask::FULL,
            )
            .unwrap();
        assert!(
            leaked.is_empty(),
            "the raw secret tail must not be searchable: {leaked:?}"
        );

        let hits = client
            .search(
                safe_canary,
                SearchFilters::default(),
                10,
                0,
                FieldMask::FULL,
            )
            .unwrap();
        assert_eq!(hits.len(), 1, "safe canary should find one redacted hit");
        let hit = &hits[0];
        for (field, value) in [
            ("title", hit.title.as_str()),
            ("snippet", hit.snippet.as_str()),
            ("content", hit.content.as_str()),
        ] {
            assert!(
                !value.contains(secret) && !value.contains(secret_tail),
                "{field} leaked raw secret material: {value}"
            );
        }
        (
            hit.title.clone(),
            hit.snippet.clone(),
            hit.content.clone(),
            hit.source_path.clone(),
            hit.agent.clone(),
            hit.workspace.clone(),
            hit.workspace_original.clone(),
            hit.created_at,
            hit.line_number,
            hit.source_id.clone(),
            hit.origin_kind.clone(),
            hit.origin_host.clone(),
        )
    };

    let incremental_client = SearchClient::open(&index_path, None)
        .unwrap()
        .expect("incremental lexical reader");
    let incremental_hit = stable_hit(&incremental_client);
    drop(incremental_client);

    let rebuild = crate::indexer::rebuild_tantivy_from_db(&db_path, &data_dir, 1, None)
        .expect("canonical lexical rebuild should succeed");
    assert_eq!(rebuild.indexed_docs - rebuild.indexed_region_docs, 1);

    let rebuilt_client = SearchClient::open(&index_path, None)
        .unwrap()
        .expect("rebuilt lexical reader");
    let rebuilt_hit = stable_hit(&rebuilt_client);
    assert_eq!(
        incremental_hit, rebuilt_hit,
        "incremental and canonical-rebuild hits must agree on stable fields"
    );
}

#[test]
#[serial]
fn persist_conversations_batched_parallel_pre_map_preserves_content_in_begin_concurrent_path() {
    // Mirror of the serial-path regression test: prove that the
    // parallel pre-compute of internal_convs still feeds the
    // begin-concurrent writers the *same* redacted, ordered data the
    // old per-chunk map_to_internal loop produced.
    use crate::connectors::NormalizedConversation;
    use crate::franken_sync::compat::{ConnectionExt, RowExt};
    use crate::search::tantivy::TantivyIndex;

    let _begin_guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT", "1");
    // Use chunk_size 8 so the 32-conv batch splits across multiple
    // rayon chunks. That exercises the per-chunk slicing of
    // internal_convs.
    let _chunk_guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT_CHUNK_SIZE", "8");
    let _redact_guard = set_env("CASS_REDACT_SECRETS", "1");

    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("parallel-pre-map-concurrent.db");
    let index_path = dir.path().join("tantivy");

    let storage = create_franken_db(&db_path);
    let mut t_index = TantivyIndex::open_or_create(&index_path).unwrap();

    let convs: Vec<NormalizedConversation> = (0..32)
        .map(|i| NormalizedConversation {
            agent_slug: format!("agent-bc-{}", i % 4),
            external_id: Some(format!("bc-session-{i}")),
            // Stripe regex requires `[A-Za-z0-9]{20,}` after sk_live_.
            title: Some(format!(
                "Title bc-{i} sk_live_ABCdef0123456789AAAAbbbb{i:04}"
            )),
            workspace: Some(std::path::PathBuf::from(format!("/ws/bc-{}", i % 3))),
            source_path: std::path::PathBuf::from(format!("/log/bc{i}.jsonl")),
            started_at: Some(2_000 + i as i64),
            ended_at: Some(2_010 + i as i64),
            metadata: serde_json::json!({}),
            messages: vec![NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: Some("tester".into()),
                created_at: Some(2_000 + i as i64),
                content: format!("bc hello {i}"),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            }],
        })
        .collect();

    persist_conversations_batched(
        &storage,
        Some(&mut t_index),
        &convs,
        LexicalPopulationStrategy::IncrementalInline,
        false,
    )
    .expect("begin-concurrent path with parallel pre-map should persist all conversations");

    let reader = FrankenStorage::open(&db_path).unwrap();
    let conversation_count: i64 = reader
        .raw()
        .query_row_map("SELECT COUNT(*) FROM conversations", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(conversation_count, convs.len() as i64);

    let titles: Vec<String> = reader
        .raw()
        .query_map_collect(
            "SELECT title FROM conversations WHERE title IS NOT NULL ORDER BY id",
            &[],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(titles.len(), convs.len());
    for title in &titles {
        assert!(
            !title.contains("sk_live_"),
            "begin-concurrent hoist must preserve secret redaction; found raw token: {title}"
        );
    }

    t_index.commit().unwrap();
}

#[test]
#[serial]
fn parallel_wal_shadow_observer_does_not_change_persisted_state() {
    // C1.3 per `ALIEN-ARTIFACT-CARD1-SPEC.md`:
    // Toggling the shadow observer must NOT change what gets
    // committed. Run the same 16-conv workload through the
    // begin-concurrent path with CASS_INDEXER_PARALLEL_WAL unset
    // and with =shadow, diff the resulting DB state by
    // (external_id, started_at) tuples.
    use crate::connectors::NormalizedConversation;
    use crate::franken_sync::compat::{ConnectionExt, RowExt};
    use crate::search::tantivy::TantivyIndex;

    fn run_once(parallel_wal: Option<&str>) -> Vec<(String, i64)> {
        let _begin_guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT", "1");
        let _chunk_guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT_CHUNK_SIZE", "4");
        let _wal_guard = parallel_wal.map(|v| set_env("CASS_INDEXER_PARALLEL_WAL", v));
        let _ = _wal_guard;

        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("shadow-parity.db");
        let index_path = dir.path().join("tantivy");
        let storage = create_franken_db(&db_path);
        let mut t_index = TantivyIndex::open_or_create(&index_path).unwrap();

        let convs: Vec<NormalizedConversation> = (0..16)
            .map(|i| NormalizedConversation {
                agent_slug: format!("agent-{}", i % 3),
                external_id: Some(format!("shadow-parity-{i}")),
                title: Some(format!("Title {i}")),
                workspace: Some(std::path::PathBuf::from(format!("/ws/{}", i % 2))),
                source_path: std::path::PathBuf::from(format!("/log/{i}.jsonl")),
                started_at: Some(1_000 + i as i64),
                ended_at: Some(1_010 + i as i64),
                metadata: serde_json::json!({}),
                messages: vec![NormalizedMessage {
                    idx: 0,
                    role: "user".into(),
                    author: Some("tester".into()),
                    created_at: Some(1_000 + i as i64),
                    content: format!("body {i}"),
                    extra: serde_json::json!({}),
                    snippets: vec![],
                    invocations: Vec::new(),
                }],
            })
            .collect();

        persist_conversations_batched(
            &storage,
            Some(&mut t_index),
            &convs,
            LexicalPopulationStrategy::IncrementalInline,
            false,
        )
        .expect("begin-concurrent path should persist all conversations");

        let reader = FrankenStorage::open(&db_path).unwrap();
        reader
            .raw()
            .query_map_collect(
                "SELECT external_id, started_at FROM conversations ORDER BY id",
                &[],
                |row| {
                    let ext: Option<String> = row.get_typed(0)?;
                    let started: Option<i64> = row.get_typed(1)?;
                    Ok((ext.unwrap_or_default(), started.unwrap_or(0)))
                },
            )
            .unwrap()
    }

    // BEGIN CONCURRENT commits chunks in nondeterministic order
    // (par_chunks scheduling), so the committed-row order is NOT
    // identical run-to-run. The invariant we care about is "the
    // SET of persisted (external_id, started_at) tuples is
    // identical." Compare as sorted vectors to normalize.
    let mut off = run_once(None);
    let mut shadow = run_once(Some("shadow"));
    off.sort();
    shadow.sort();
    assert_eq!(off.len(), 16);
    assert_eq!(shadow.len(), 16);
    assert_eq!(
        off, shadow,
        "shadow-mode observer must NOT change the SET of persisted rows"
    );
}

#[test]
#[serial]
fn parallel_wal_shadow_observer_emits_chunk_telemetry() {
    // Positive control: with shadow mode ON, running a real
    // begin-concurrent batch should populate the shadow observer
    // ring buffer with at least one record.
    use crate::connectors::NormalizedConversation;
    use crate::search::tantivy::TantivyIndex;

    let _begin_guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT", "1");
    let _chunk_guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT_CHUNK_SIZE", "4");
    let _wal_guard = set_env("CASS_INDEXER_PARALLEL_WAL", "shadow");

    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("shadow-tele.db");
    let index_path = dir.path().join("tantivy");
    let storage = create_franken_db(&db_path);
    let mut t_index = TantivyIndex::open_or_create(&index_path).unwrap();

    let convs: Vec<NormalizedConversation> = (0..8)
        .map(|i| NormalizedConversation {
            agent_slug: "codex".into(),
            external_id: Some(format!("shadow-tele-{i}")),
            title: Some(format!("Title {i}")),
            workspace: None,
            source_path: std::path::PathBuf::from(format!("/log/{i}.jsonl")),
            started_at: Some(1_000 + i as i64),
            ended_at: Some(1_010 + i as i64),
            metadata: serde_json::json!({}),
            messages: vec![NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: Some("tester".into()),
                created_at: Some(1_000 + i as i64),
                content: format!("body {i}"),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            }],
        })
        .collect();

    let baseline = crate::indexer::parallel_wal_shadow::telemetry_snapshot();
    persist_conversations_batched(
        &storage,
        Some(&mut t_index),
        &convs,
        LexicalPopulationStrategy::IncrementalInline,
        false,
    )
    .expect("shadow-mode begin-concurrent should persist");
    let after = crate::indexer::parallel_wal_shadow::telemetry_snapshot();

    assert!(after.active, "active must be true under shadow");
    assert!(
        after.chunks_observed > baseline.chunks_observed,
        "shadow observer must record ≥1 chunk when begin-concurrent path runs"
    );
}

#[test]
#[serial]
fn persist_conversations_batched_registers_missing_remote_source_in_serial_path() {
    use crate::connectors::NormalizedConversation;
    use crate::franken_sync::compat::{ConnectionExt, RowExt};
    use crate::search::tantivy::TantivyIndex;

    let _begin_guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT", "0");

    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("serial-source.db");
    let index_path = dir.path().join("tantivy");

    let storage = create_franken_db(&db_path);
    let mut t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let convs = vec![NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: Some("remote-serial-session".into()),
        title: Some("Remote serial session".into()),
        workspace: Some(std::path::PathBuf::from("/ws/remote")),
        source_path: std::path::PathBuf::from("/log/remote-serial.jsonl"),
        started_at: Some(1_000),
        ended_at: Some(1_010),
        metadata: serde_json::json!({
            "cass": {
                "origin": {
                    "source_id": "remote-source",
                    "host": "builder-1"
                }
            }
        }),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "assistant".into(),
            author: Some("tester".into()),
            created_at: Some(1_005),
            content: "serial remote content".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    }];

    persist_conversations_batched(
        &storage,
        Some(&mut t_index),
        &convs,
        LexicalPopulationStrategy::IncrementalInline,
        false,
    )
    .expect("serial batched path should auto-register embedded remote sources");

    let reader = FrankenStorage::open(&db_path).unwrap();
    let source_ids = reader.get_source_ids().unwrap();
    assert_eq!(source_ids, vec!["remote-source".to_string()]);

    let provenance: Vec<(String, Option<String>)> = reader
        .raw()
        .query_map_collect(
            "SELECT source_id, origin_host FROM conversations",
            &[],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert_eq!(
        provenance,
        vec![("remote-source".to_string(), Some("builder-1".to_string()))]
    );
}

#[test]
#[serial]
fn persist_conversations_batched_registers_missing_remote_source_in_begin_concurrent_path() {
    use crate::connectors::NormalizedConversation;
    use crate::franken_sync::compat::{ConnectionExt, RowExt};
    use crate::search::tantivy::TantivyIndex;

    let _begin_guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT", "1");
    let _chunk_guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT_CHUNK_SIZE", "1");

    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("begin-concurrent-source.db");
    let index_path = dir.path().join("tantivy");

    let storage = create_franken_db(&db_path);
    let mut t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let convs = vec![NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: Some("remote-begin-session".into()),
        title: Some("Remote begin-concurrent session".into()),
        workspace: Some(std::path::PathBuf::from("/ws/remote")),
        source_path: std::path::PathBuf::from("/log/remote-begin.jsonl"),
        started_at: Some(2_000),
        ended_at: Some(2_010),
        metadata: serde_json::json!({
            "cass": {
                "origin": {
                    "source_id": "remote-begin-source",
                    "host": "builder-2"
                }
            }
        }),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "assistant".into(),
            author: Some("tester".into()),
            created_at: Some(2_005),
            content: "begin-concurrent remote content".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    }];

    persist_conversations_batched(
        &storage,
        Some(&mut t_index),
        &convs,
        LexicalPopulationStrategy::IncrementalInline,
        false,
    )
    .expect("begin-concurrent path should auto-register embedded remote sources");

    let reader = FrankenStorage::open(&db_path).unwrap();
    let source_ids = reader.get_source_ids().unwrap();
    assert_eq!(source_ids, vec!["remote-begin-source".to_string()]);

    let provenance: Vec<(String, Option<String>)> = reader
        .raw()
        .query_map_collect(
            "SELECT source_id, origin_host FROM conversations",
            &[],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert_eq!(
        provenance,
        vec![(
            "remote-begin-source".to_string(),
            Some("builder-2".to_string())
        )]
    );
}

#[test]
#[serial]
fn persist_conversations_batched_reuses_auto_registered_remote_source_across_serial_runs() {
    use crate::connectors::NormalizedConversation;
    use crate::franken_sync::compat::{ConnectionExt, RowExt};
    use crate::search::tantivy::TantivyIndex;

    let _begin_guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT", "0");

    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("serial-source-reuse.db");
    let index_path = dir.path().join("tantivy");

    let storage = create_franken_db(&db_path);
    let mut t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let metadata = serde_json::json!({
        "cass": {
            "origin": {
                "source_id": "remote-source-reused",
                "host": "builder-reuse-1"
            }
        }
    });

    for (external_id, started_at, content, source_path) in [
        (
            "remote-serial-session-1",
            10_000_i64,
            "serial remote content one",
            "/log/remote-serial-1.jsonl",
        ),
        (
            "remote-serial-session-2",
            20_000_i64,
            "serial remote content two",
            "/log/remote-serial-2.jsonl",
        ),
    ] {
        let convs = vec![NormalizedConversation {
            agent_slug: "codex".into(),
            external_id: Some(external_id.into()),
            title: Some(format!("Remote serial session {external_id}")),
            workspace: Some(std::path::PathBuf::from("/ws/remote")),
            source_path: std::path::PathBuf::from(source_path),
            started_at: Some(started_at),
            ended_at: Some(started_at + 10),
            metadata: metadata.clone(),
            messages: vec![NormalizedMessage {
                idx: 0,
                role: "assistant".into(),
                author: Some("tester".into()),
                created_at: Some(started_at + 5),
                content: content.into(),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            }],
        }];

        persist_conversations_batched(
            &storage,
            Some(&mut t_index),
            &convs,
            LexicalPopulationStrategy::IncrementalInline,
            false,
        )
        .expect("serial batched path should keep reusing the auto-registered source");
    }

    let reader = FrankenStorage::open(&db_path).unwrap();
    let source_rows: Vec<(String, Option<String>)> = reader
        .raw()
        .query_map_collect(
            "SELECT id, host_label FROM sources WHERE id <> 'local' ORDER BY id",
            &[],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert_eq!(
        source_rows,
        vec![(
            "remote-source-reused".to_string(),
            Some("builder-reuse-1".to_string())
        )],
        "serial path should upsert the missing remote source once and then reuse it"
    );

    let provenance: Vec<(String, Option<String>, String)> = reader
        .raw()
        .query_map_collect(
            "SELECT source_id, origin_host, external_id FROM conversations ORDER BY external_id",
            &[],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
        )
        .unwrap();
    assert_eq!(
        provenance,
        vec![
            (
                "remote-source-reused".to_string(),
                Some("builder-reuse-1".to_string()),
                "remote-serial-session-1".to_string()
            ),
            (
                "remote-source-reused".to_string(),
                Some("builder-reuse-1".to_string()),
                "remote-serial-session-2".to_string()
            )
        ],
        "every persisted conversation should retain the recovered source provenance"
    );

    let conversation_count: i64 = reader
        .raw()
        .query_row_map("SELECT COUNT(*) FROM conversations", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(conversation_count, 2);

    let fk_violations = reader.raw().query("PRAGMA foreign_key_check").unwrap();
    assert!(
        fk_violations.is_empty(),
        "serial path should not leave any foreign-key violations after source auto-registration"
    );
}

#[test]
#[serial]
fn persist_conversations_batched_reuses_auto_registered_remote_source_across_begin_concurrent_runs()
{
    use crate::connectors::NormalizedConversation;
    use crate::franken_sync::compat::{ConnectionExt, RowExt};
    use crate::search::tantivy::TantivyIndex;

    let _begin_guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT", "1");
    let _chunk_guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT_CHUNK_SIZE", "1");

    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("begin-source-reuse.db");
    let index_path = dir.path().join("tantivy");

    let storage = create_franken_db(&db_path);
    let mut t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let metadata = serde_json::json!({
        "cass": {
            "origin": {
                "source_id": "remote-begin-reused",
                "host": "builder-reuse-2"
            }
        }
    });

    for (external_id, started_at, content, source_path) in [
        (
            "remote-begin-session-1",
            30_000_i64,
            "begin-concurrent content one",
            "/log/remote-begin-1.jsonl",
        ),
        (
            "remote-begin-session-2",
            40_000_i64,
            "begin-concurrent content two",
            "/log/remote-begin-2.jsonl",
        ),
    ] {
        let convs = vec![NormalizedConversation {
            agent_slug: "codex".into(),
            external_id: Some(external_id.into()),
            title: Some(format!("Remote begin session {external_id}")),
            workspace: Some(std::path::PathBuf::from("/ws/remote")),
            source_path: std::path::PathBuf::from(source_path),
            started_at: Some(started_at),
            ended_at: Some(started_at + 10),
            metadata: metadata.clone(),
            messages: vec![NormalizedMessage {
                idx: 0,
                role: "assistant".into(),
                author: Some("tester".into()),
                created_at: Some(started_at + 5),
                content: content.into(),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            }],
        }];

        persist_conversations_batched(
            &storage,
            Some(&mut t_index),
            &convs,
            LexicalPopulationStrategy::IncrementalInline,
            false,
        )
        .expect("begin-concurrent path should keep reusing the auto-registered source");
    }

    let reader = FrankenStorage::open(&db_path).unwrap();
    let source_rows: Vec<(String, Option<String>)> = reader
        .raw()
        .query_map_collect(
            "SELECT id, host_label FROM sources WHERE id <> 'local' ORDER BY id",
            &[],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert_eq!(
        source_rows,
        vec![(
            "remote-begin-reused".to_string(),
            Some("builder-reuse-2".to_string())
        )],
        "begin-concurrent path should upsert the missing remote source once and then reuse it"
    );

    let provenance: Vec<(String, Option<String>, String)> = reader
        .raw()
        .query_map_collect(
            "SELECT source_id, origin_host, external_id FROM conversations ORDER BY external_id",
            &[],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?)),
        )
        .unwrap();
    assert_eq!(
        provenance,
        vec![
            (
                "remote-begin-reused".to_string(),
                Some("builder-reuse-2".to_string()),
                "remote-begin-session-1".to_string()
            ),
            (
                "remote-begin-reused".to_string(),
                Some("builder-reuse-2".to_string()),
                "remote-begin-session-2".to_string()
            )
        ],
        "every begin-concurrent persist should retain the recovered source provenance"
    );

    let conversation_count: i64 = reader
        .raw()
        .query_row_map("SELECT COUNT(*) FROM conversations", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(conversation_count, 2);

    let fk_violations = reader.raw().query("PRAGMA foreign_key_check").unwrap();
    assert!(
        fk_violations.is_empty(),
        "begin-concurrent path should not leave any foreign-key violations after source auto-registration"
    );
}

#[test]
fn persist_conversation_registers_missing_remote_source() {
    use crate::connectors::NormalizedConversation;
    use crate::franken_sync::compat::{ConnectionExt, RowExt};
    use crate::search::tantivy::TantivyIndex;

    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("single-remote-source.db");
    let index_path = dir.path().join("tantivy");

    let storage = create_franken_db(&db_path);
    let mut t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: Some("remote-single-session".into()),
        title: Some("Remote single session".into()),
        workspace: Some(std::path::PathBuf::from("/ws/remote")),
        source_path: std::path::PathBuf::from("/log/remote-single.jsonl"),
        started_at: Some(3_000),
        ended_at: Some(3_010),
        metadata: serde_json::json!({
            "cass": {
                "origin": {
                    "source_id": "remote-single-source",
                    "host": "builder-3"
                }
            }
        }),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "assistant".into(),
            author: Some("tester".into()),
            created_at: Some(3_005),
            content: "single remote content".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };

    persist_conversation(&storage, &mut t_index, &conv)
        .expect("single conversation path should auto-register embedded remote sources");

    let reader = FrankenStorage::open(&db_path).unwrap();
    let source_ids = reader.get_source_ids().unwrap();
    assert_eq!(source_ids, vec!["remote-single-source".to_string()]);

    let provenance: Vec<(String, Option<String>)> = reader
        .raw()
        .query_map_collect(
            "SELECT source_id, origin_host FROM conversations",
            &[],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert_eq!(
        provenance,
        vec![(
            "remote-single-source".to_string(),
            Some("builder-3".to_string())
        )]
    );
}

#[test]
fn persist_conversation_host_only_remote_source_infers_source_id_from_host() {
    use crate::connectors::NormalizedConversation;
    use crate::franken_sync::compat::{ConnectionExt, RowExt};
    use crate::search::tantivy::TantivyIndex;

    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("single-host-only-remote.db");
    let index_path = dir.path().join("tantivy");

    let storage = create_franken_db(&db_path);
    let mut t_index = TantivyIndex::open_or_create(&index_path).unwrap();
    let conv = NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: Some("host-only-remote-session".into()),
        title: Some("Host only remote session".into()),
        workspace: Some(std::path::PathBuf::from("/ws/remote")),
        source_path: std::path::PathBuf::from("/log/host-only-remote.jsonl"),
        started_at: Some(3_100),
        ended_at: Some(3_110),
        metadata: serde_json::json!({
            "cass": {
                "origin": {
                    "source_id": "   ",
                    "host": "builder-4"
                }
            }
        }),
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "assistant".into(),
            author: Some("tester".into()),
            created_at: Some(3_105),
            content: "host only remote content".into(),
            extra: serde_json::json!({}),
            snippets: vec![],
            invocations: Vec::new(),
        }],
    };

    persist_conversation(&storage, &mut t_index, &conv)
        .expect("host-only remote provenance should be auto-registered as remote");

    let reader = FrankenStorage::open(&db_path).unwrap();
    let source_ids = reader.get_source_ids().unwrap();
    assert_eq!(source_ids, vec!["builder-4".to_string()]);

    let provenance: Vec<(String, Option<String>)> = reader
        .raw()
        .query_map_collect(
            "SELECT source_id, origin_host FROM conversations",
            &[],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert_eq!(
        provenance,
        vec![("builder-4".to_string(), Some("builder-4".to_string()))]
    );
}

fn make_profiled_remote_conversation(external_id: i64, msg_count: usize) -> NormalizedConversation {
    NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: Some(format!("profiled-remote-{external_id}")),
        title: Some(format!("Profiled remote conversation {external_id}")),
        workspace: Some(std::path::PathBuf::from("/ws/profiled-remote")),
        source_path: std::path::PathBuf::from(format!("/log/profiled-remote-{external_id}.jsonl")),
        started_at: Some(10_000 + external_id * 100),
        ended_at: Some(10_000 + external_id * 100 + msg_count as i64),
        metadata: serde_json::json!({
            "cass": {
                "origin": {
                    "source_id": "profiled-remote-source",
                    "host": "builder-profile"
                }
            }
        }),
        messages: (0..msg_count)
            .map(|idx| NormalizedMessage {
                idx: idx as i64,
                role: if idx % 2 == 0 { "user" } else { "assistant" }.into(),
                author: Some("tester".into()),
                created_at: Some(20_000 + external_id * 100 + idx as i64),
                content: format!(
                    "profiled remote content ext={external_id} idx={idx} {}",
                    "x".repeat(64)
                ),
                extra: serde_json::json!({ "idx": idx }),
                snippets: vec![],
                invocations: Vec::new(),
            })
            .collect(),
    }
}

#[test]
fn persist_conversation_stage_profile_tracks_steady_state_remote_reuse() {
    use crate::search::tantivy::TantivyIndex;

    let _defer_guard = set_env("CASS_DEFER_LEXICAL_UPDATES", "0");
    let log_profile = std::env::var_os("CASS_PERSIST_STAGE_PROFILE").is_some();

    for &(msg_count, iterations) in &[(5usize, 80usize), (20, 50), (50, 24)] {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join(format!("profile-{msg_count}.db"));
        let index_path = dir.path().join(format!("tantivy-{msg_count}"));

        let storage = create_franken_db(&db_path);
        let mut t_index = TantivyIndex::open_or_create(&index_path).unwrap();
        persist_conversation(
            &storage,
            &mut t_index,
            &make_profiled_remote_conversation(0, msg_count),
        )
        .unwrap();

        let mut profile = PersistConversationPerfProfile::default();
        for external_id in 1..=iterations {
            persist_conversation_with_profile(
                &storage,
                &mut t_index,
                &make_profiled_remote_conversation(external_id as i64, msg_count),
                &mut profile,
            )
            .unwrap();
        }

        assert_eq!(profile.invocations, iterations);
        assert_eq!(profile.messages, iterations * msg_count);
        assert_eq!(profile.inserted_messages, iterations * msg_count);
        assert!(
            profile.total_duration >= profile.db_duration,
            "db stage cannot exceed total duration"
        );
        assert!(
            profile.total_duration
                >= profile.db_duration
                    + profile.packet_duration
                    + profile.positional_duration
                    + profile.tantivy_add_duration,
            "accounted stage durations cannot exceed total duration"
        );

        if log_profile {
            profile.log_summary(&format!("remote_reuse_{msg_count}_msgs"));
        }
    }
}

#[test]
fn duplicate_conversation_keys_present_for_shared_source_path_without_external_id() {
    use crate::connectors::NormalizedConversation;

    let convs = vec![
        NormalizedConversation {
            agent_slug: "shared-agent".into(),
            external_id: None,
            title: Some("Shared Session".into()),
            workspace: Some(std::path::PathBuf::from("/ws/shared")),
            source_path: std::path::PathBuf::from("/log/shared.jsonl"),
            started_at: Some(1_000),
            ended_at: Some(1_010),
            metadata: serde_json::json!({}),
            messages: vec![NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: Some("tester".into()),
                created_at: Some(1_000),
                content: "first".into(),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            }],
        },
        NormalizedConversation {
            agent_slug: "shared-agent".into(),
            external_id: None,
            title: Some("Shared Session".into()),
            workspace: Some(std::path::PathBuf::from("/ws/shared")),
            source_path: std::path::PathBuf::from("/log/shared.jsonl"),
            started_at: Some(9_999),
            ended_at: Some(10_010),
            metadata: serde_json::json!({}),
            messages: vec![NormalizedMessage {
                idx: 1,
                role: "assistant".into(),
                author: Some("tester".into()),
                created_at: Some(1_001),
                content: "second".into(),
                extra: serde_json::json!({}),
                snippets: vec![],
                invocations: Vec::new(),
            }],
        },
    ];

    assert!(duplicate_conversation_keys_present(&convs));
}

#[test]
fn begin_concurrent_disabled_falls_through_to_default() {
    let _guard = set_env("CASS_INDEXER_BEGIN_CONCURRENT", "0");
    assert!(!begin_concurrent_writes_enabled());

    let _guard2 = set_env("CASS_INDEXER_BEGIN_CONCURRENT", "false");
    assert!(!begin_concurrent_writes_enabled());
}

/// `coding_agent_session_search-5b9p0`: pin the new helper that
/// maps `outcome.inserted_indices` (message *idx* values) onto
/// positional indices into the packet's messages Vec. The
/// legacy filter walked `conv.messages` looking for `m.idx in
/// inserted_indices` — the new helper produces the same
/// positional set so `add_messages_from_packet` emits the
/// same messages the legacy `add_messages_with_conversation_id`
/// + filter loop did.
///
/// A regression that mis-mapped idx → position would silently
/// drop or duplicate documents in the lexical sink; this test
/// catches that.
#[test]
fn positional_indices_for_inserted_maps_idx_values_to_packet_positions() {
    use serde_json::Value;

    // Fixture: a NormalizedConversation with msg.idx values
    // that are NOT 0..N (gaps + non-monotonic). The packet
    // builder preserves source order, so positional indices
    // are 0..N regardless of idx values.
    let conv = NormalizedConversation {
        agent_slug: "codex".to_string(),
        external_id: Some("idx-mapping".to_string()),
        title: None,
        workspace: None,
        source_path: std::path::PathBuf::from("/tmp/idx-mapping.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_010_000),
        metadata: Value::Null,
        messages: vec![
            NormalizedMessage {
                idx: 0,
                role: "user".to_string(),
                author: None,
                created_at: Some(1_700_000_000_000),
                content: "first".to_string(),
                extra: Value::Null,
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
            // Idx jumps to 5: the legacy filter treated this
            // as the second message in the iteration order
            // because it walks .messages in source order.
            NormalizedMessage {
                idx: 5,
                role: "assistant".to_string(),
                author: None,
                created_at: Some(1_700_000_001_000),
                content: "second".to_string(),
                extra: Value::Null,
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
            NormalizedMessage {
                idx: 7,
                role: "tool".to_string(),
                author: None,
                created_at: Some(1_700_000_002_000),
                content: "third".to_string(),
                extra: Value::Null,
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
        ],
    };

    let packet = ConversationPacket::from_canonical_replay(
        &map_to_internal(&conv),
        ConversationPacketProvenance::local(),
    );
    assert_eq!(
        packet.payload.messages.len(),
        3,
        "packet must preserve all conversation messages"
    );

    // Empty inserted_indices ⇒ empty positional set (no work).
    let positional_empty = positional_indices_for_inserted(&packet, &[]);
    assert_eq!(positional_empty, Vec::<usize>::new());

    // Inserted only idx=5 ⇒ position 1.
    let positional_one = positional_indices_for_inserted(&packet, &[5]);
    assert_eq!(
        positional_one,
        vec![1],
        "idx=5 must map to packet position 1, got {positional_one:?}"
    );

    // Inserted idx=0 and idx=7 (non-contiguous) ⇒ positions 0 and 2,
    // ordered by packet iteration (source order), NOT by inserted_indices order.
    let positional_two_unordered = positional_indices_for_inserted(&packet, &[7, 0]);
    assert_eq!(
        positional_two_unordered,
        vec![0, 2],
        "result must be in source order (positions 0, 2) regardless of \
         inserted_indices ordering, got {positional_two_unordered:?}"
    );

    // Inserted_indices that don't match any message idx ⇒ empty.
    // (E.g. caller passed a stale/wrong list — silently emit nothing
    // rather than crash, matching the legacy filter behavior.)
    let positional_unmatched = positional_indices_for_inserted(&packet, &[99, 100]);
    assert_eq!(
        positional_unmatched,
        Vec::<usize>::new(),
        "unmatched idx values must produce an empty positional set"
    );

    // All three inserted ⇒ positions [0, 1, 2].
    let positional_all = positional_indices_for_inserted(&packet, &[0, 5, 7]);
    assert_eq!(positional_all, vec![0, 1, 2]);

    // Duplicate idx values in inserted_indices ⇒ deduplicated by
    // packet position (HashSet under the hood). Legacy filter
    // would also have emitted each message once because
    // `inserted_indices.contains(&m.idx)` is a contains check.
    let positional_dupe = positional_indices_for_inserted(&packet, &[5, 5, 5]);
    assert_eq!(
        positional_dupe,
        vec![1],
        "duplicate idx values must collapse to a single position"
    );
}
