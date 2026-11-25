use super::super::*;
use super::helpers::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use crate::indexer::LexicalPopulationStrategy;
use crate::model::types::{Agent, AgentKind, Conversation};
use crate::search::tantivy::TantivyIndex;
use crate::sources::provenance::Source;
use crate::storage::sqlite::FrankenStorage;
use crate::franken_sync::SqliteValue;
use serial_test::serial;
use std::path::Path;

// -------------------------------------------------------------------
// Publication durable-commit budget + crash-point protocol
// (scan-perf §4 fix 3, option (a): the PENDING receipt rides the
// generation transaction). The commit counter counts the
// publication lane's durable-commit calls: standalone receipt
// writes on the bare connection plus the generation transaction
// commit. The probe guards are thread-local, so parallel tests
// never inherit each other's injections.
// -------------------------------------------------------------------

use crate::viewer_extensions::projection::GenerationPublishOutcome;

struct CrashProbeGuard;
impl CrashProbeGuard {
    fn arm() -> Self {
        crate::viewer_extensions::projection::test_set_generation_crash_probe(true);
        CrashProbeGuard
    }
}
impl Drop for CrashProbeGuard {
    fn drop(&mut self) {
        crate::viewer_extensions::projection::test_set_generation_crash_probe(false);
    }
}

struct FailProbeGuard;
impl FailProbeGuard {
    fn arm() -> Self {
        crate::viewer_extensions::projection::test_set_generation_fail_probe(true);
        FailProbeGuard
    }
}
impl Drop for FailProbeGuard {
    fn drop(&mut self) {
        crate::viewer_extensions::projection::test_set_generation_fail_probe(false);
    }
}

/// Distinct-identity two-message session for the commit-budget and
/// crash-point tests (`region_reingest_session`'s shape, seeded
/// `external_id` so several conversations can coexist in one db).
fn budget_session(dir: &std::path::Path, id: i64, answer: &str) -> NormalizedConversation {
    let mut session = region_reingest_session(dir, answer);
    session.external_id = Some(format!("budget-{id}"));
    session
}

/// Ensure the session's agent and return its id (one upsert).
fn ensure_cli_agent(storage: &FrankenStorage, slug: &str) -> i64 {
    storage
        .ensure_agent(&crate::model::types::Agent {
            id: None,
            slug: slug.into(),
            name: slug.into(),
            version: None,
            kind: crate::model::types::AgentKind::Cli,
        })
        .unwrap()
}

/// Commit one canonical conversation tree and hand back the internal
/// model + canonical id the ingest hook would hold post-commit, for
/// direct `publish_ingest_generation` calls.
fn insert_for_publication(
    storage: &FrankenStorage,
    agent_id: i64,
    conv: &NormalizedConversation,
) -> (Conversation, i64) {
    let internal = map_to_internal(conv);
    let outcome = storage
        .insert_conversation_tree(agent_id, None, &internal)
        .unwrap();
    (internal, outcome.conversation_id)
}

fn publication_commit_calls() -> u64 {
    crate::viewer_extensions::projection::test_publication_commit_calls()
}

fn reset_publication_commit_calls() {
    crate::viewer_extensions::projection::test_reset_publication_commit_calls();
}

fn receipt_of(
    storage: &FrankenStorage,
    conversation_id: i64,
) -> Option<crate::viewer_extensions::facts::IngestReceipt> {
    crate::viewer_extensions::facts::ingest_receipt_for(storage.raw(), conversation_id).unwrap()
}

fn facts_rows_at(storage: &FrankenStorage, conversation_id: i64) -> (i64, Option<i64>) {
    // (row count, source_revision of the single row when present)
    let rows = storage
        .raw()
        .query(&format!(
            "SELECT COUNT(*), MAX(source_revision) FROM viewer_session_facts \
             WHERE conversation_id = {conversation_id}"
        ))
        .unwrap();
    (
        rows[0].get_typed::<i64>(0).unwrap(),
        rows[0].get_typed::<Option<i64>>(1).unwrap(),
    )
}

/// A fresh publication must cost exactly ONE durable commit — the
/// generation transaction, with the PENDING receipt folded into it —
/// and an unchanged re-publication must cost none (the skip path never
/// writes).
#[test]
fn fresh_publication_costs_one_durable_commit_and_skip_costs_none() {
    let dir = tempfile::TempDir::new().unwrap();
    let storage = create_franken_db(&dir.path().join("budget.db"));
    let agent_id = ensure_cli_agent(&storage, "claude_code");
    let (internal, conversation_id) = insert_for_publication(
        &storage,
        agent_id,
        &budget_session(dir.path(), 1, "budgeted answer"),
    );

    reset_publication_commit_calls();
    let outcome = crate::viewer_extensions::projection::publish_ingest_generation(
        storage.raw(),
        &internal,
        conversation_id,
    )
    .unwrap();
    assert_eq!(outcome, GenerationPublishOutcome::Published);
    assert_eq!(
        publication_commit_calls(),
        1,
        "a fresh publication must issue exactly one durable commit: \
         the generation transaction (the PENDING receipt rides it)"
    );

    reset_publication_commit_calls();
    let outcome = crate::viewer_extensions::projection::publish_ingest_generation(
        storage.raw(),
        &internal,
        conversation_id,
    )
    .unwrap();
    assert_eq!(outcome, GenerationPublishOutcome::SkippedUnchanged);
    assert_eq!(
        publication_commit_calls(),
        0,
        "an unchanged session must republish nothing — zero durable commits"
    );
}

/// Crash point: a kill before the generation transaction commits (the
/// only remaining pre-complete crash window after the PENDING fold)
/// leaves NO receipt and NO rows — never a false complete — and costs
/// zero durable commits. The resumable backfill's full conversation
/// scan (its repair authority for receipt-less sessions) converges it.
#[test]
fn crashed_first_publication_leaves_no_receipt_and_backfill_converges() {
    let dir = tempfile::TempDir::new().unwrap();
    let storage = create_franken_db(&dir.path().join("crash-first.db"));
    let agent_id = ensure_cli_agent(&storage, "claude_code");
    let (internal, conversation_id) = insert_for_publication(
        &storage,
        agent_id,
        &budget_session(dir.path(), 2, "crash before the generation commit"),
    );

    {
        let _probe = CrashProbeGuard::arm();
        reset_publication_commit_calls();
        let outcome = crate::viewer_extensions::projection::publish_ingest_generation(
            storage.raw(),
            &internal,
            conversation_id,
        )
        .unwrap();
        assert_eq!(outcome, GenerationPublishOutcome::Interrupted);
        assert_eq!(
            publication_commit_calls(),
            0,
            "a crash before the generation commit must not leave any \
             durable commit behind"
        );
    }

    // Durable state: no receipt, no rows — an interrupted first
    // publication is indistinguishable from "never attempted", which
    // is exactly the state the backfill scan reconciles.
    assert!(
        receipt_of(&storage, conversation_id).is_none(),
        "an interrupted first publication must leave no receipt"
    );
    let (facts_rows, _) = facts_rows_at(&storage, conversation_id);
    assert_eq!(facts_rows, 0, "no partial rows may survive the crash");

    // Repair: converge through the explicit resumable backfill.
    crate::viewer_extensions::schema::ensure_schema(storage.raw()).unwrap();
    let stats = crate::viewer_extensions::backfill::backfill_viewer_extensions(
        storage.raw(),
        &crate::viewer_extensions::backfill::BackfillOptions::default(),
        |_| {},
    )
    .unwrap();
    assert_eq!(stats.conversations_scanned, 1);
    assert_eq!(stats.facts_published, 1, "the backfill must publish it");
    assert_eq!(stats.failures, 0);
    let receipt = receipt_of(&storage, conversation_id).expect("receipt after backfill");
    assert_eq!(receipt.state, "complete", "{receipt:?}");
    let (facts_rows, revision) = facts_rows_at(&storage, conversation_id);
    assert_eq!(facts_rows, 1);
    assert_eq!(revision, Some(receipt.source_revision));
}

/// Crash point during a RE-publication: the canonical row already
/// moved to a new revision, and the crash leaves the prior
/// `complete(R1)` receipt in place. That state is honest (the receipt
/// certifies R1 and R1's rows exist) but must never mask the moved
/// canonical row: the resume path's skip check is revision-keyed, so
/// a plain re-publish repairs it — never a false skip.
#[test]
fn crashed_republish_keeps_prior_complete_honest_and_republish_repairs() {
    let dir = tempfile::TempDir::new().unwrap();
    let storage = create_franken_db(&dir.path().join("crash-republish.db"));
    let agent_id = ensure_cli_agent(&storage, "claude_code");
    let (mut internal, conversation_id) = insert_for_publication(
        &storage,
        agent_id,
        &budget_session(dir.path(), 3, "first answer"),
    );
    crate::viewer_extensions::projection::publish_ingest_generation(
        storage.raw(),
        &internal,
        conversation_id,
    )
    .unwrap();
    let first = receipt_of(&storage, conversation_id).expect("first receipt");
    assert_eq!(first.state, "complete");

    // The canonical row moves to a new revision (the real update
    // path: same conversation, changed message payload).
    storage
        .raw()
        .execute_with_params(
            "UPDATE messages SET content = ?1 \
             WHERE conversation_id = ?2 AND idx = 1",
            &[
                SqliteValue::Text("second answer".into()),
                SqliteValue::Integer(conversation_id),
            ],
        )
        .unwrap();
    internal.messages[1].content = "second answer".into();

    {
        let _probe = CrashProbeGuard::arm();
        let outcome = crate::viewer_extensions::projection::publish_ingest_generation(
            storage.raw(),
            &internal,
            conversation_id,
        )
        .unwrap();
        assert_eq!(outcome, GenerationPublishOutcome::Interrupted);
    }

    // The durable state is the prior generation's: complete at R1 with
    // its rows — honest for R1, claiming nothing about R2.
    let stale = receipt_of(&storage, conversation_id).expect("stale receipt");
    assert_eq!(stale.state, "complete", "{stale:?}");
    assert_eq!(stale.source_revision, first.source_revision);
    let (facts_rows, revision) = facts_rows_at(&storage, conversation_id);
    assert_eq!(facts_rows, 1);
    assert_eq!(revision, Some(first.source_revision));

    // Repair (resume): the revision-keyed skip check must NOT skip —
    // the stale complete cannot stand in for the moved canonical row.
    let outcome = crate::viewer_extensions::projection::publish_ingest_generation(
        storage.raw(),
        &internal,
        conversation_id,
    )
    .unwrap();
    assert_eq!(
        outcome,
        GenerationPublishOutcome::Published,
        "a stale complete at an older revision must never skip"
    );
    let after = receipt_of(&storage, conversation_id).expect("receipt after repair");
    assert_eq!(after.state, "complete");
    assert_ne!(after.source_revision, first.source_revision);
    let (facts_rows, revision) = facts_rows_at(&storage, conversation_id);
    assert_eq!(facts_rows, 1);
    assert_eq!(revision, Some(after.source_revision));
}

/// Crash point during a RE-publication, repaired through the
/// converge lane instead: the backfill's revision-keyed full scan
/// republishes the moved conversation even though its receipt says
/// `complete` (at the old revision) — a stale complete is not a skip
/// key for the scan either.
#[test]
fn crashed_republish_backfill_converges_stale_complete_to_new_revision() {
    let dir = tempfile::TempDir::new().unwrap();
    let storage = create_franken_db(&dir.path().join("crash-backfill.db"));
    let agent_id = ensure_cli_agent(&storage, "claude_code");
    let (mut internal, conversation_id) = insert_for_publication(
        &storage,
        agent_id,
        &budget_session(dir.path(), 4, "first answer"),
    );
    crate::viewer_extensions::projection::publish_ingest_generation(
        storage.raw(),
        &internal,
        conversation_id,
    )
    .unwrap();
    let first = receipt_of(&storage, conversation_id).expect("first receipt");
    assert_eq!(first.state, "complete");

    storage
        .raw()
        .execute_with_params(
            "UPDATE messages SET content = ?1 \
             WHERE conversation_id = ?2 AND idx = 1",
            &[
                SqliteValue::Text("second answer".into()),
                SqliteValue::Integer(conversation_id),
            ],
        )
        .unwrap();
    internal.messages[1].content = "second answer".into();

    {
        let _probe = CrashProbeGuard::arm();
        let outcome = crate::viewer_extensions::projection::publish_ingest_generation(
            storage.raw(),
            &internal,
            conversation_id,
        )
        .unwrap();
        assert_eq!(outcome, GenerationPublishOutcome::Interrupted);
    }

    // Repair (converge): the backfill republishes at the new revision.
    crate::viewer_extensions::schema::ensure_schema(storage.raw()).unwrap();
    let stats = crate::viewer_extensions::backfill::backfill_viewer_extensions(
        storage.raw(),
        &crate::viewer_extensions::backfill::BackfillOptions::default(),
        |_| {},
    )
    .unwrap();
    assert_eq!(
        stats.facts_published, 1,
        "the backfill must republish the moved conversation, not skip it"
    );
    assert_eq!(stats.failures, 0);
    let after = receipt_of(&storage, conversation_id).expect("receipt after backfill");
    assert_eq!(after.state, "complete");
    assert_ne!(after.source_revision, first.source_revision);
    let (facts_rows, revision) = facts_rows_at(&storage, conversation_id);
    assert_eq!(facts_rows, 1);
    assert_eq!(revision, Some(after.source_revision));
}

/// Crash point inside the generation transaction (an aborting error,
/// not a kill): the whole generation rolls back, the durable FAILED
/// receipt — carrying the exact reason — is the publication lane's
/// ONLY durable commit, and a plain re-publish (fault cleared)
/// converges without residue.
#[test]
fn failed_publication_costs_exactly_one_failed_receipt_commit() {
    let dir = tempfile::TempDir::new().unwrap();
    let storage = create_franken_db(&dir.path().join("fail-budget.db"));
    let agent_id = ensure_cli_agent(&storage, "claude_code");
    let (internal, conversation_id) = insert_for_publication(
        &storage,
        agent_id,
        &budget_session(dir.path(), 5, "fails mid transaction"),
    );

    {
        let _probe = FailProbeGuard::arm();
        reset_publication_commit_calls();
        let error = crate::viewer_extensions::projection::publish_ingest_generation(
            storage.raw(),
            &internal,
            conversation_id,
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("CASS_TEST_VIEWER_GENERATION_FAIL"),
            "{error:#}"
        );
        assert_eq!(
            publication_commit_calls(),
            1,
            "a failed publication's only durable commit must be the \
             failed receipt — the generation rolled back"
        );
    }

    let receipt = receipt_of(&storage, conversation_id).expect("failure receipt");
    assert_eq!(receipt.state, "failed", "{receipt:?}");
    assert!(
        receipt
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("CASS_TEST_VIEWER_GENERATION_FAIL")),
        "{receipt:?}"
    );
    let (facts_rows, _) = facts_rows_at(&storage, conversation_id);
    assert_eq!(facts_rows, 0, "the rolled-back generation leaves no rows");

    // Repair (resume): the failed receipt never masks a retry.
    let outcome = crate::viewer_extensions::projection::publish_ingest_generation(
        storage.raw(),
        &internal,
        conversation_id,
    )
    .unwrap();
    assert_eq!(outcome, GenerationPublishOutcome::Published);
    let after = receipt_of(&storage, conversation_id).expect("receipt after repair");
    assert_eq!(after.state, "complete");
    let (facts_rows, revision) = facts_rows_at(&storage, conversation_id);
    assert_eq!(facts_rows, 1);
    assert_eq!(revision, Some(after.source_revision));
}

/// Synthetic consumer-throughput bench for the ingest projection
/// lane (scan-perf §4 fix 3 A/B): publish N small conversations
/// through the same per-conversation `publish_ingest_generation`
/// the streaming consumer runs, on one long-lived writer under the
/// consumer's bulk checkpoint policy. Not part of the default suite —
/// run explicitly with
/// `cargo test --lib bench_publish_ingest_generation -- --ignored --nocapture`.
#[test]
#[ignore = "synthetic throughput bench; run with --ignored --nocapture"]
fn bench_publish_ingest_generation_throughput() {
    let conversations: usize = dotenvy::var("CASS_BENCH_PUBLISH_N")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(2000);
    let dir = tempfile::TempDir::new().unwrap();
    let storage = create_franken_db(&dir.path().join("bench.db"));
    apply_index_writer_checkpoint_policy(&storage, true);
    let agent_id = ensure_cli_agent(&storage, "claude_code");

    // Untimed setup: commit the canonical rows the consumer would
    // already have committed.
    let mut pairs = Vec::with_capacity(conversations);
    for id in 0..conversations as i64 {
        let (internal, conversation_id) = insert_for_publication(
            &storage,
            agent_id,
            &budget_session(dir.path(), id, "bench answer"),
        );
        pairs.push((internal, conversation_id));
    }

    let start = std::time::Instant::now();
    for (internal, conversation_id) in &pairs {
        let outcome = crate::viewer_extensions::projection::publish_ingest_generation(
            storage.raw(),
            internal,
            *conversation_id,
        )
        .unwrap();
        assert!(
            matches!(outcome, GenerationPublishOutcome::Published),
            "every bench conversation must publish: {outcome:?}"
        );
    }
    let elapsed = start.elapsed();
    let per_conv_us = elapsed.as_micros() as f64 / pairs.len() as f64;
    println!(
        "BENCH publish_ingest_generation N={} elapsed_ms={} conv_per_s={:.2} \
         per_conv_us={:.0}",
        pairs.len(),
        elapsed.as_millis(),
        pairs.len() as f64 / elapsed.as_secs_f64(),
        per_conv_us,
    );
}

/// The rebuild lanes populate a fresh snapshot without region documents,
/// so they must restage every marked session even though its marks are
/// `published`; the incremental helper must still skip it.
#[test]
#[serial]
fn rebuild_lanes_restage_published_sessions() {
    let _defer = set_env("CASS_DEFER_LEXICAL_UPDATES", "0");
    let dir = tempfile::TempDir::new().unwrap();
    let storage = create_franken_db(&dir.path().join("rebuild.db"));
    let index_path = crate::search::tantivy::index_dir(dir.path()).unwrap();
    let mut index = TantivyIndex::open_or_create(&index_path).unwrap();
    let conv = region_reingest_session(dir.path(), "first answer");
    incremental_lane_pass(&storage, &mut index, &conv);

    stage_persisted_region_docs(
        &mut index,
        storage.raw(),
        1,
        LexicalPopulationStrategy::IncrementalInline,
    )
    .unwrap();
    assert_eq!(index.pending_region_doc_stamp_count(), 0);

    stage_persisted_region_docs(
        &mut index,
        storage.raw(),
        1,
        LexicalPopulationStrategy::InlineRebuildFromScan,
    )
    .unwrap();
    assert_eq!(index.pending_region_doc_stamp_count(), 1);
    index.commit_with_region_doc_stamps(storage.raw()).unwrap();
    assert_eq!(index.pending_region_doc_stamp_count(), 0);

    assert_eq!(
        index
            .converge_pending_search_region_docs(storage.raw(), 4096)
            .unwrap(),
        0,
        "published marks are not pending"
    );
    let (sessions, _) = index
        .converge_all_search_region_docs(storage.raw(), i64::MAX)
        .unwrap();
    assert_eq!(sessions, 1);
    assert_eq!(index.pending_region_doc_stamp_count(), 1);
}

/// Both lanes must agree on the generation revision: a session the
/// ingest lane just published (snippets included) is skipped by the
/// backfill lane instead of being republished at a different revision.
#[test]
fn backfill_lane_agrees_with_ingest_revision() {
    let dir = tempfile::TempDir::new().unwrap();
    let storage = create_franken_db(&dir.path().join("lanes.db"));
    let mut conv = region_reingest_session(dir.path(), "first answer");
    conv.messages[1].snippets = vec![crate::connectors::NormalizedSnippet {
        file_path: Some(dir.path().join("src/lib.rs")),
        start_line: Some(1),
        end_line: Some(2),
        language: Some("rust".into()),
        snippet_text: Some("fn main() {}".into()),
    }];
    persist_conversations_batched(
        &storage,
        None,
        std::slice::from_ref(&conv),
        LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
        false,
    )
    .unwrap();

    let stats = crate::viewer_extensions::backfill::backfill_viewer_extensions(
        storage.raw(),
        &crate::viewer_extensions::backfill::BackfillOptions::default(),
        |_| {},
    )
    .unwrap();
    assert_eq!(stats.facts_published, 0, "{stats:?}");
    assert_eq!(stats.structure_published, 0, "{stats:?}");
    assert_eq!(stats.facts_skipped_unchanged, 1, "{stats:?}");
}

#[test]
fn lexical_rebuild_content_fingerprint_uses_table_max_ids() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("fingerprint.db");
    let storage = create_franken_db(&db_path);
    let agent_id = storage
        .ensure_agent(&Agent {
            id: None,
            slug: "codex".into(),
            name: "Codex".into(),
            version: None,
            kind: AgentKind::Cli,
        })
        .unwrap();
    let workspace_id = storage
        .ensure_workspace(std::path::Path::new("/tmp/fingerprint"), None)
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "INSERT INTO conversations(
                 id, agent_id, workspace_id, source_id, title, source_path, metadata_json
             ) VALUES
                 (4, ?1, ?2, 'local', 'older', '/tmp/fingerprint/older.jsonl', '{}'),
                 (9, ?1, ?2, 'local', 'newer', '/tmp/fingerprint/newer.jsonl', '{}')",
            &[ParamValue::from(agent_id), ParamValue::from(workspace_id)],
        )
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "INSERT INTO messages(id, conversation_id, idx, role, content, extra_json)
             VALUES
                 (10, 4, 0, 'user', 'older message', '{}'),
                 (11, 9, 0, 'assistant', 'newer message', '{}')",
            &[],
        )
        .unwrap();

    let fingerprint = crate::indexer::lexical_rebuild_content_fingerprint(&storage, 2).unwrap();

    assert_eq!(fingerprint, "content-v1:2:9:11");
}
fn assert_gh466_canonical_prefix_cap(agent_slug: &str, route: usize, expand_prefix: bool) {
    use crate::search::query::{FieldMask, SearchClient, SearchFilters};

    let cap = if expand_prefix { 8 * 1024 * 1024 } else { 128 };
    let _cap = set_env("CASS_LEXICAL_MAX_MESSAGE_CONTENT_BYTES", &cap.to_string());
    let _defer = set_env("CASS_DEFER_LEXICAL_UPDATES", "0");
    let _begin = set_env(
        "CASS_INDEXER_BEGIN_CONCURRENT",
        if route == 2 { "1" } else { "0" },
    );
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("agent_search.db");
    let storage = create_franken_db(&db_path);
    let index_path = crate::search::tantivy::index_dir(dir.path()).unwrap();
    let mut index = TantivyIndex::open_or_create(&index_path).unwrap();
    let make_message = |idx, content: String| NormalizedMessage {
        idx,
        role: "user".into(),
        author: None,
        created_at: Some(1_700_000_000_000 + idx),
        content,
        extra: serde_json::json!({"uuid": format!("stable-message-{idx}")}),
        snippets: Vec::new(),
        invocations: Vec::new(),
    };
    let mut conv = NormalizedConversation {
        agent_slug: agent_slug.into(),
        external_id: Some("canonical-prefix-cap".into()),
        title: Some("Canonical prefix cap".into()),
        workspace: Some(dir.path().join("workspace")),
        source_path: dir.path().join("source.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_001),
        metadata: serde_json::Value::Null,
        messages: vec![
            make_message(0, "original alpha".into()),
            make_message(1, "stable bravo".into()),
        ],
    };
    let prefix = conv.clone();
    let persist = |index: &mut TantivyIndex, conv: &NormalizedConversation| {
        if route == 0 {
            persist_conversation(&storage, index, conv).unwrap();
            None
        } else {
            Some(
                persist_conversations_batched(
                    &storage,
                    Some(index),
                    std::slice::from_ref(conv),
                    LexicalPopulationStrategy::IncrementalInline,
                    false,
                )
                .unwrap(),
            )
        }
    };
    persist(&mut index, &conv);
    // Leave the direct/serial/begin-concurrent incremental additions
    // pending, then enter rebuild twice without a caller commit or a
    // count helper that could conceal the production batch boundary.
    for _ in 0..2 {
        let replay = persist_conversations_batched(
            &storage,
            Some(&mut index),
            std::slice::from_ref(&conv),
            LexicalPopulationStrategy::InlineRebuildFromScan,
            false,
        )
        .unwrap();
        assert_eq!(replay.inserted_conversations, 0);
        assert_eq!(replay.inserted_messages, 0);
        assert_eq!(replay.updated_messages, 0);
        assert!(!replay.lexical_update_deferred);
    }
    assert_eq!(tantivy_doc_count(&mut index), 2);
    crate::indexer::refresh_completed_lexical_rebuild_checkpoint(&storage, &db_path, dir.path())
        .unwrap();
    let baseline_checkpoint = crate::indexer::load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("baseline checkpoint");
    assert!(baseline_checkpoint.completed);
    let conversation_id: i64 = storage
        .raw()
        .query_row_map("SELECT id FROM conversations", &[], |row| row.get_typed(0))
        .unwrap();
    let baseline_ids: Vec<_> = storage
        .fetch_messages(conversation_id)
        .unwrap()
        .into_iter()
        .map(|message| (message.id, message.idx))
        .collect();

    if expand_prefix {
        // Same identity and timestamp, but the newly parsed source
        // exceeds the real default cap. Persistence retains the short
        // canonical row, and the lexical projection indexes that row.
        let fresh_prefix = "oversized fresh prefix ";
        conv.messages[0].content = fresh_prefix.repeat(cap / fresh_prefix.len() + 1);
        assert!(conv.messages[0].content.len() > cap);
        conv.messages.push(make_message(2, "appended delta".into()));
        conv.messages.push(make_message(3, "quartzcharlie".into()));
    } else {
        // The first append exceeds the per-message cap: only its tail
        // is dropped. The following message is indexed in full, as it
        // is however much text the conversation already holds (bgn6s).
        conv.messages.push(make_message(
            2,
            format!(
                "quartzcharlie {} tailbeyondcap",
                "bounded text ".repeat(cap)
            ),
        ));
        conv.messages
            .push(make_message(3, "beyondcapneedle".into()));
    }
    conv.ended_at = Some(1_700_000_000_003);
    if let Some(outcome) = persist(&mut index, &conv) {
        assert_eq!(outcome.inserted_conversations, 0);
        assert_eq!(outcome.inserted_messages, 2);
        assert_eq!(outcome.updated_messages, 0);
        assert!(!outcome.lexical_update_deferred);
    }
    let expected_docs = 4;
    assert_eq!(tantivy_doc_count(&mut index), expected_docs);
    let stored = storage.fetch_messages(conversation_id).unwrap();
    assert_eq!(stored.len(), 4);
    assert_eq!(stored[0].content, "original alpha");
    assert_eq!(stored[1].content, "stable bravo");
    assert_eq!(
        stored[..2]
            .iter()
            .map(|message| (message.id, message.idx))
            .collect::<Vec<_>>(),
        baseline_ids,
        "retained canonical rows keep their identities"
    );
    assert_eq!(stored[2].content, conv.messages[2].content);
    assert_eq!(stored[3].content, conv.messages[3].content);
    let packet = lexical_packet_for_changed_messages(&storage, conversation_id, &[3, 2]).unwrap();
    assert_eq!(
        packet
            .payload
            .messages
            .iter()
            .map(|message| message.idx)
            .collect::<Vec<_>>(),
        vec![2, 3],
        "an append carries exactly its changed canonical rows, in idx order"
    );
    assert_eq!(
        positional_indices_for_inserted(&packet, &[2, 3]),
        vec![0, 1]
    );
    assert_eq!(
        packet.payload.metadata_json,
        serde_json::json!({
            "cass": {
                "origin": {"source_id": "local", "kind": "local", "host": null}
            }
        }),
        "only the canonical origin envelope is needed by the lexical sink"
    );
    assert!(
        packet
            .payload
            .messages
            .iter()
            .all(|message| message.extra_json.is_null())
    );
    let retained_bytes: usize = packet
        .payload
        .messages
        .iter()
        .map(|message| message.content.len())
        .sum();
    if expand_prefix {
        assert!(retained_bytes < cap);
        assert_eq!(packet.payload.messages[1].content, "quartzcharlie");
    } else {
        assert_eq!(packet.payload.messages[0].content.len(), cap);
        assert!(
            packet.payload.messages[0]
                .content
                .starts_with("quartzcharlie ")
        );
        assert_eq!(packet.payload.messages[1].content, "beyondcapneedle");
    }
    assert_eq!(
        crate::indexer::expected_live_lexical_doc_count(&storage).unwrap(),
        expected_docs as usize,
        "the unchanged checkpoint guard must agree with actual indexed documents"
    );
    crate::indexer::refresh_completed_lexical_rebuild_checkpoint(&storage, &db_path, dir.path())
        .unwrap();
    let checkpoint = crate::indexer::load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("appended checkpoint");
    assert!(checkpoint.completed);
    assert_ne!(
        checkpoint.db.storage_fingerprint,
        baseline_checkpoint.db.storage_fingerprint
    );
    assert_eq!(
        checkpoint.db.storage_fingerprint,
        crate::indexer::lexical_rebuild_content_fingerprint(&storage, 1).unwrap()
    );

    if let Some(outcome) = persist(&mut index, &conv) {
        assert_eq!(outcome.inserted_messages, 0);
        assert_eq!(outcome.updated_messages, 0);
    }
    assert_eq!(tantivy_doc_count(&mut index), expected_docs);
    drop(index);
    let marker_hit = |path: &Path| {
        let client = SearchClient::open(path, None).unwrap().unwrap();
        let hits = client
            .search(
                "quartzcharlie",
                SearchFilters::default(),
                10,
                0,
                FieldMask::FULL,
            )
            .unwrap();
        assert_eq!(
            hits.len(),
            1,
            "appended marker must be searchable exactly once"
        );
        for absent in ["tailbeyondcap", "oversized"] {
            assert!(
                client
                    .search(absent, SearchFilters::default(), 10, 0, FieldMask::FULL)
                    .unwrap()
                    .is_empty(),
                "{absent} lies past a message cap or in a replaced source variant"
            );
        }
        if !expand_prefix {
            assert_eq!(
                client
                    .search(
                        "beyondcapneedle",
                        SearchFilters::default(),
                        10,
                        0,
                        FieldMask::FULL
                    )
                    .unwrap()
                    .len(),
                1,
                "a message after a capped one stays searchable (bgn6s)"
            );
        }
        let hit = &hits[0];
        assert_eq!(hit.agent, agent_slug);
        (
            hit.content.clone(),
            hit.source_path.clone(),
            hit.line_number,
            hit.created_at,
        )
    };
    let incremental_hit = marker_hit(&index_path);
    let scan_index_path = dir.path().join("scan-index");
    let mut scan_index = TantivyIndex::open_or_create(&scan_index_path).unwrap();
    let overlap = persist_conversations_batched(
        &storage,
        Some(&mut scan_index),
        &[prefix, conv.clone()],
        LexicalPopulationStrategy::InlineRebuildFromScan,
        false,
    )
    .unwrap();
    assert_eq!(overlap.inserted_messages, 0);
    assert_eq!(overlap.updated_messages, 0);
    // These IDs were new to scan_index. Read the published snapshot
    // directly, before any caller commit, to prove batch-exit visibility.
    {
        let reader = scan_index.reader().unwrap();
        crate::search::quill_bridge::refresh_reader(&reader).unwrap();
        // Task 5: the raw live count binds canonical messages AND
        // materialized region documents — canonical expected count
        // plus the persisted doc-backed region bound (the batch's
        // stamp pass already drained the stale-delete queue).
        assert_eq!(
            reader.doc_count().unwrap(),
            expected_docs
                + crate::viewer_extensions::search_regions::live_region_doc_bound(storage.raw())
                    .unwrap()
        );
    }
    assert_eq!(tantivy_doc_count(&mut scan_index), expected_docs);
    persist_conversations_batched(
        &storage,
        Some(&mut scan_index),
        std::slice::from_ref(&conv),
        LexicalPopulationStrategy::InlineRebuildFromScan,
        false,
    )
    .unwrap();
    assert_eq!(tantivy_doc_count(&mut scan_index), expected_docs);
    drop(scan_index);
    assert_eq!(
        marker_hit(&scan_index_path),
        incremental_hit,
        "overlapping inline scan packets must converge on canonical document identities"
    );
    let rebuilt = crate::indexer::rebuild_tantivy_from_db(&db_path, dir.path(), 1, None).unwrap();
    // Task 5: `indexed_docs` binds every published document;
    // `indexed_docs - indexed_region_docs` is the canonical share.
    assert_eq!(
        rebuilt.indexed_docs - rebuilt.indexed_region_docs,
        expected_docs as usize
    );
    assert_eq!(
        marker_hit(&index_path),
        incremental_hit,
        "incremental and canonical rebuild projections agree"
    );
}

#[test]
#[serial]
fn gh466_incremental_lexical_cap_uses_retained_canonical_prefix_for_every_route() {
    for agent in ["claude_code", "codex"] {
        for route in 0..3 {
            assert_gh466_canonical_prefix_cap(agent, route, true);
        }
    }
}

#[test]
#[serial]
fn gh466_incremental_append_after_a_capped_message_stays_searchable() {
    for route in 0..3 {
        assert_gh466_canonical_prefix_cap("claude_code", route, false);
    }
}

/// lk524 (GH #524 follow-up): canonical replay labels a conversation
/// with its registered source kind, as a full rebuild's source map
/// does. A local source named anything but `local` replayed as remote.
#[test]
fn canonical_replay_uses_the_registered_source_kind() {
    let dir = tempfile::TempDir::new().unwrap();
    let storage = create_franken_db(&dir.path().join("agent_search.db"));
    let mut named_local = Source::local();
    named_local.id = "full-history-plan".into();
    storage.upsert_source(&named_local).unwrap();
    storage
        .upsert_source(&Source::remote("laptop", "laptop.example"))
        .unwrap();
    let mut index = TantivyIndex::open_or_create(&dir.path().join("index")).unwrap();
    let mut expected = Vec::new();
    // Origins as the connectors record them; the replay must not
    // depend on that stored metadata, which it does not read.
    for (source_id, origin, kind) in [
        (
            "full-history-plan",
            serde_json::json!({"source_id": "full-history-plan", "kind": "local"}),
            "local",
        ),
        (
            "laptop",
            serde_json::json!({"source_id": "laptop", "kind": "ssh", "host": "laptop.example"}),
            "remote",
        ),
        (
            "local",
            serde_json::json!({"source_id": "local", "kind": "local"}),
            "local",
        ),
    ] {
        let conv = NormalizedConversation {
            agent_slug: "codex".into(),
            external_id: Some(format!("{source_id}-session")),
            title: Some(source_id.into()),
            workspace: None,
            source_path: dir.path().join(format!("{source_id}.jsonl")),
            started_at: Some(1_700_000_000_000),
            ended_at: Some(1_700_000_000_001),
            metadata: serde_json::json!({"cass": {"origin": origin}}),
            messages: vec![NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: None,
                created_at: Some(1_700_000_000_000),
                content: format!("{source_id} provenance probe"),
                extra: serde_json::json!({}),
                snippets: Vec::new(),
                invocations: Vec::new(),
            }],
        };
        persist_conversation(&storage, &mut index, &conv).unwrap();
        expected.push((source_id.to_string(), kind.to_string()));
    }
    let ids: Vec<i64> = storage
        .raw()
        .query_map_collect("SELECT id FROM conversations ORDER BY id", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    let replayed: Vec<(String, String)> = ids
        .into_iter()
        .map(|id| {
            let conversation = canonical_lexical_conversation(&storage, id).unwrap();
            let envelope = conversation.metadata_json["cass"]["origin"]["kind"]
                .as_str()
                .unwrap()
                .to_string();
            let packet = canonical_lexical_packet(&conversation, Vec::new());
            assert_eq!(packet.payload.provenance.origin_kind, envelope);
            (conversation.source_id, envelope)
        })
        .collect();
    assert_eq!(replayed, expected);
}

/// bgn6s: an inline replay reads a conversation one bounded chunk at
/// a time and still indexes every message; a cumulative cap would
/// leave everything after the first message unindexed here.
#[test]
#[serial]
fn chunked_canonical_replay_indexes_every_message_of_a_long_conversation() {
    use crate::search::query::{FieldMask, SearchClient, SearchFilters};

    let _cap = set_env("CASS_LEXICAL_MAX_MESSAGE_CONTENT_BYTES", "40");
    let _defer = set_env("CASS_DEFER_LEXICAL_UPDATES", "1");
    let dir = tempfile::TempDir::new().unwrap();
    let storage = create_franken_db(&dir.path().join("agent_search.db"));
    let markers = [
        "alphamark",
        "bravomark",
        "charliemark",
        "deltamark",
        "foxtrotmark",
    ];
    let messages = markers
        .iter()
        .enumerate()
        .map(|(idx, marker)| NormalizedMessage {
            idx: idx as i64,
            role: "user".into(),
            author: None,
            created_at: Some(1_700_000_000_000 + idx as i64),
            content: if *marker == "charliemark" {
                format!("{marker} {} tailmark", "padding ".repeat(8))
            } else {
                format!("{marker} steady text")
            },
            extra: serde_json::json!({"uuid": format!("chunked-replay-{idx}")}),
            snippets: Vec::new(),
            invocations: Vec::new(),
        })
        .collect();
    let conv = NormalizedConversation {
        agent_slug: "claude_code".into(),
        external_id: Some("chunked-canonical-replay".into()),
        title: Some("Chunked canonical replay".into()),
        workspace: Some(dir.path().join("workspace")),
        source_path: dir.path().join("source.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_010),
        metadata: serde_json::Value::Null,
        messages,
    };
    let unused_index_path = dir.path().join("unused-index");
    let mut unused_index = TantivyIndex::open_or_create(&unused_index_path).unwrap();
    persist_conversation(&storage, &mut unused_index, &conv).unwrap();
    let conversation_id: i64 = storage
        .raw()
        .query_row_map("SELECT id FROM conversations", &[], |row| row.get_typed(0))
        .unwrap();
    let footprints = storage
        .lexical_rebuild_message_footprints(conversation_id)
        .unwrap();
    assert_eq!(footprints.len(), markers.len());
    assert_eq!(footprints[2].1, 40, "footprints report the capped length");
    assert_eq!(
        crate::indexer::lexical_rebuild_conversation_chunks(&footprints, 1).len(),
        markers.len(),
        "a one-byte chunk budget reads every message as its own chunk"
    );

    let index_path = dir.path().join("replay-index");
    let mut index = TantivyIndex::open_or_create(&index_path).unwrap();
    for _ in 0..2 {
        replay_canonical_conversation_in_chunks(&storage, &mut index, conversation_id, 1).unwrap();
        assert_eq!(
            tantivy_doc_count(&mut index),
            markers.len() as u64,
            "a repeated chunked replay reconciles the same identities"
        );
    }
    drop(index);
    let client = SearchClient::open(&index_path, None).unwrap().unwrap();
    let hits = |query: &str| {
        client
            .search(query, SearchFilters::default(), 10, 0, FieldMask::FULL)
            .unwrap()
            .len()
    };
    for marker in markers {
        assert_eq!(hits(marker), 1, "{marker} must be searchable exactly once");
    }
    assert_eq!(hits("tailmark"), 0, "text past one message's cap stays out");
}

/// m91r6: a giant conversation persisted in slices must store exactly
/// what one transaction stores, be searchable at both ends, resume
/// after a crash between slices, and replay as a no-op.
#[test]
#[serial]
fn giant_conversation_persists_in_slices_like_one_transaction() {
    use crate::search::query::{FieldMask, SearchClient, SearchFilters};

    let _defer = set_env("CASS_DEFER_LEXICAL_UPDATES", "0");
    let _begin = set_env("CASS_INDEXER_BEGIN_CONCURRENT", "0");
    let limits = PersistSliceLimits {
        messages: 7,
        content_bytes: usize::MAX,
    };
    let conversation = |dir: &std::path::Path, timestamps: bool| {
        let messages = (0..50)
            .map(|idx| NormalizedMessage {
                idx,
                role: if idx % 2 == 0 { "user" } else { "assistant" }.into(),
                author: None,
                created_at: timestamps.then_some(1_700_000_000_000 + idx * 1000),
                content: format!("slicemark{idx}z giant session text {idx}"),
                extra: serde_json::json!({"uuid": format!("slice-{idx}")}),
                snippets: Vec::new(),
                invocations: Vec::new(),
            })
            .collect();
        NormalizedConversation {
            agent_slug: "codex".into(),
            external_id: Some("giant-sliced".into()),
            title: Some("Giant sliced".into()),
            workspace: Some(dir.join("workspace")),
            source_path: dir.join("rollout-giant.jsonl"),
            started_at: timestamps.then_some(1_700_000_000_000),
            ended_at: timestamps.then_some(1_700_000_049_000),
            metadata: serde_json::Value::Null,
            messages,
        }
    };
    let stored = |storage: &FrankenStorage| {
        let rows: Vec<(i64, Option<i64>)> = storage
            .raw()
            .query_map_collect("SELECT id, ended_at FROM conversations", &[], |row| {
                Ok((row.get_typed(0)?, row.get_typed(1)?))
            })
            .unwrap();
        assert_eq!(rows.len(), 1, "exactly one conversation row");
        let messages = storage
            .fetch_messages(rows[0].0)
            .unwrap()
            .into_iter()
            .map(|message| (message.idx, message.created_at, message.content))
            .collect::<Vec<_>>();
        (rows[0].1, messages)
    };
    for strategy in [
        LexicalPopulationStrategy::IncrementalInline,
        LexicalPopulationStrategy::InlineRebuildFromScan,
    ] {
        for timestamps in [true, false] {
            let dir = tempfile::TempDir::new().unwrap();
            let conv = conversation(dir.path(), timestamps);
            assert_eq!(persist_slice_ranges(&conv, limits).len(), 8);
            // Untimed messages would make every later slice rescan
            // the stored conversation, so they persist whole.
            assert_eq!(should_slice_conversation(&conv, limits), timestamps);
            let persist = |name: &str, convs: &[NormalizedConversation], sliced: bool| {
                let root = dir.path().join(name);
                std::fs::create_dir_all(&root).unwrap();
                let storage = create_franken_db(&root.join("agent_search.db"));
                let index_path = root.join("index");
                let mut index = TantivyIndex::open_or_create(&index_path).unwrap();
                let mut outcomes = Vec::new();
                for conv in convs {
                    outcomes.push(
                        persist_conversations_batched_sliced(
                            &storage,
                            Some(&mut index),
                            std::slice::from_ref(conv),
                            strategy,
                            false,
                            false,
                            Some(&root),
                            PersistHeartbeat::NONE,
                            None,
                            if sliced {
                                limits
                            } else {
                                PersistSliceLimits::DEFAULT
                            },
                        )
                        .unwrap(),
                    );
                    index.commit().unwrap();
                }
                let docs = tantivy_doc_count(&mut index);
                drop(index);
                (storage, index_path, outcomes, docs)
            };
            let label = format!("{strategy:?} timestamps={timestamps}");

            let (whole, _, _, whole_docs) = persist("whole", std::slice::from_ref(&conv), false);
            let expected = stored(&whole);
            assert_eq!(expected.1.len(), 50, "{label}");

            let (sliced, index_path, outcomes, sliced_docs) =
                persist("sliced", &[conv.clone(), conv.clone()], true);
            assert_eq!(stored(&sliced), expected, "{label}");
            assert_eq!(sliced_docs, whole_docs, "{label}");
            assert_eq!(outcomes[0].inserted_conversations, 1, "{label}");
            assert_eq!(outcomes[0].inserted_messages, 50, "{label}");
            assert_eq!(outcomes[1].inserted_messages, 0, "{label}: replay");
            let client = SearchClient::open(&index_path, None).unwrap().unwrap();
            for marker in ["slicemark0z", "slicemark27z", "slicemark49z"] {
                let hits = client
                    .search(marker, SearchFilters::default(), 10, 0, FieldMask::FULL)
                    .unwrap();
                assert_eq!(hits.len(), 1, "{label}: {marker}");
            }

            // A crash after three slices leaves a stored prefix; the
            // next run completes it without duplicating a row.
            let prefix = persist_conversation_slice(&conv, 0..21, false);
            let (resumed, _, outcomes, resumed_docs) =
                persist("resumed", &[prefix, conv.clone()], true);
            assert_eq!(stored(&resumed), expected, "{label}: resumed");
            assert_eq!(resumed_docs, whole_docs, "{label}: resumed");
            assert_eq!(outcomes[1].inserted_messages, 29, "{label}: resumed");
        }
    }
}

/// An inline rebuild of one batch that holds a giant conversation must
/// index every conversation in the batch, not only those in its last
/// part: the ordinary runs around the giant one, and a giant
/// conversation whose final slice is not the batch's last part.
#[test]
#[serial]
fn sliced_inline_rebuild_batch_indexes_every_conversation() {
    use crate::search::query::{FieldMask, SearchClient, SearchFilters};

    let _defer = set_env("CASS_DEFER_LEXICAL_UPDATES", "0");
    let _begin = set_env("CASS_INDEXER_BEGIN_CONCURRENT", "0");
    let limits = PersistSliceLimits {
        messages: 7,
        content_bytes: usize::MAX,
    };
    let conversation = |dir: &std::path::Path, name: &str, count: i64| NormalizedConversation {
        agent_slug: "codex".into(),
        external_id: Some(format!("batch-{name}")),
        title: Some(format!("Batch {name}")),
        workspace: Some(dir.join("workspace")),
        source_path: dir.join(format!("rollout-{name}.jsonl")),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_000 + (count - 1) * 1000),
        metadata: serde_json::Value::Null,
        messages: (0..count)
            .map(|idx| NormalizedMessage {
                idx,
                role: if idx % 2 == 0 { "user" } else { "assistant" }.into(),
                author: None,
                created_at: Some(1_700_000_000_000 + idx * 1000),
                content: format!("{name}mark{idx}z batch text {idx}"),
                extra: serde_json::json!({"uuid": format!("{name}-{idx}")}),
                snippets: Vec::new(),
                invocations: Vec::new(),
            })
            .collect(),
    };
    let dir = tempfile::TempDir::new().unwrap();
    let before = conversation(dir.path(), "before", 3);
    let giant = conversation(dir.path(), "giant", 50);
    let after = conversation(dir.path(), "after", 3);
    assert!(!should_slice_conversation(&before, limits));
    assert!(should_slice_conversation(&giant, limits));
    for (label, batch) in [
        (
            "surrounded",
            vec![before.clone(), giant.clone(), after.clone()],
        ),
        ("giant-first", vec![giant.clone(), after.clone()]),
        ("giant-last", vec![before.clone(), giant.clone()]),
    ] {
        let root = dir.path().join(label);
        std::fs::create_dir_all(&root).unwrap();
        let storage = create_franken_db(&root.join("agent_search.db"));
        let index_path = root.join("index");
        let mut index = TantivyIndex::open_or_create(&index_path).unwrap();
        let outcome = persist_conversations_batched_sliced(
            &storage,
            Some(&mut index),
            &batch,
            LexicalPopulationStrategy::InlineRebuildFromScan,
            false,
            false,
            Some(&root),
            PersistHeartbeat::NONE,
            None,
            limits,
        )
        .unwrap();
        index.commit().unwrap();
        let messages: usize = batch.iter().map(|conv| conv.messages.len()).sum();
        assert_eq!(outcome.inserted_messages, messages, "{label}");
        assert_eq!(
            tantivy_doc_count(&mut index),
            messages as u64,
            "{label}: every stored message is indexed"
        );
        drop(index);
        let client = SearchClient::open(&index_path, None).unwrap().unwrap();
        for conv in &batch {
            for message in [&conv.messages[0], &conv.messages[conv.messages.len() - 1]] {
                let marker = message.content.split_whitespace().next().unwrap();
                let hits = client
                    .search(marker, SearchFilters::default(), 10, 0, FieldMask::FULL)
                    .unwrap();
                assert_eq!(hits.len(), 1, "{label}: {marker}");
            }
        }
    }
}
