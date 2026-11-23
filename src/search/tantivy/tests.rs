use super::federated::searchable_index_summary_from_tantivy_meta;
use super::ingest::{
    cass_doc_context, cass_doc_context_from_packet, cass_document_for_message,
    cass_document_for_packet_message,
};
use super::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use frankensearch::quill::cass::{
    CassDocument as FsCassDocument, CassDocumentRef as FsCassDocumentRef,
};
use serde_json::Value;
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

#[test]
fn open_or_create_roundtrip() {
    let dir = TempDir::new().expect("temp dir");
    let idx = TantivyIndex::open_or_create(dir.path()).expect("create index");
    let reader = idx.reader().expect("reader");
    assert_eq!(reader.doc_count().expect("doc count"), 0);
}

#[test]
fn schema_hash_matches_current_hash() {
    assert!(schema_hash_matches(SCHEMA_HASH));
    assert!(!schema_hash_matches("invalid"));
}

#[test]
fn default_tantivy_writer_cap_scales_with_memory() -> Result<(), &'static str> {
    const GIB: u64 = 1024 * 1024 * 1024;

    require_default_tantivy_writer_cap(None, 26, "unknown memory uses ceiling")?;
    require_default_tantivy_writer_cap(Some(8 * GIB), 1, "8 GiB caps at one thread")?;
    require_default_tantivy_writer_cap(Some(24 * GIB), 2, "24 GiB caps at two threads")?;
    require_default_tantivy_writer_cap(Some(64 * GIB), 6, "64 GiB allows six threads")?;
    require_default_tantivy_writer_cap(Some(512 * GIB), 26, "512 GiB reaches ceiling")?;
    Ok(())
}

#[test]
fn default_tantivy_writer_cap_accounts_for_actual_concurrent_writers() {
    const GIB: u64 = 1024 * 1024 * 1024;

    assert_eq!(
        default_tantivy_max_writer_threads_for_memory_bytes_and_concurrent_writers(
            Some(24 * GIB),
            8,
        ),
        2
    );
    assert_eq!(
        default_tantivy_max_writer_threads_for_memory_bytes_and_concurrent_writers(
            Some(24 * GIB),
            2,
        ),
        9
    );
    assert_eq!(
        default_tantivy_max_writer_threads_for_memory_bytes_and_concurrent_writers(
            Some(24 * GIB),
            0,
        ),
        19
    );
}

fn require_default_tantivy_writer_cap(
    memory_bytes: Option<u64>,
    expected: usize,
    label: &'static str,
) -> Result<(), &'static str> {
    if default_tantivy_max_writer_threads_for_memory_bytes(memory_bytes) == expected {
        return Ok(());
    }
    Err(label)
}

#[test]
fn generate_edge_ngrams_prefixes() {
    let out = frankensearch::lexical_tantivy::cass_generate_edge_ngrams("hello world");
    assert!(out.contains("he"));
    assert!(out.contains("world"));
}

#[test]
fn build_preview_truncates_with_ellipsis() {
    let preview =
        frankensearch::lexical_tantivy::cass_build_preview("abcdefghijklmnopqrstuvwxyz", 10);
    assert_eq!(preview, "abcdefghij…");
}

#[test]
fn merge_status_api_is_exposed() {
    let dir = TempDir::new().expect("temp dir");
    let index = TantivyIndex::open_or_create(dir.path()).expect("create");
    let status = index.merge_status();
    assert_eq!(status.merge_threshold, 4);
}

#[test]
fn searchable_index_summary_uses_meta_file_without_opening_index() {
    let dir = TempDir::new().expect("temp dir");
    fs::write(
        dir.path().join("meta.json"),
        serde_json::to_vec(&serde_json::json!({
            "segments": [
                {"segment_id": "a", "max_doc": 3, "deletes": null},
                {"segment_id": "b", "max_doc": 5}
            ]
        }))
        .expect("serialize meta"),
    )
    .expect("write meta");

    assert_eq!(
        searchable_index_summary(dir.path())
            .expect("summary")
            .expect("index exists"),
        SearchableIndexSummary {
            docs: 8,
            segments: 2
        }
    );
}

#[test]
fn searchable_index_summary_meta_fast_path_declines_deleted_segments() {
    let dir = TempDir::new().expect("temp dir");
    fs::write(
        dir.path().join("meta.json"),
        serde_json::to_vec(&serde_json::json!({
            "segments": [
                {"segment_id": "a", "max_doc": 3, "deletes": {"opstamp": 1}}
            ]
        }))
        .expect("serialize meta"),
    )
    .expect("write meta");

    assert_eq!(
        searchable_index_summary_from_tantivy_meta(dir.path()).expect("summary"),
        None
    );
}

#[test]
fn merge_status_should_merge_logic() {
    let status = MergeStatus {
        segment_count: 5,
        last_merge_ts: 0,
        ms_since_last_merge: -1,
        merge_threshold: 4,
        cooldown_ms: 300_000,
    };
    assert!(status.should_merge());
}

fn gh423_lexical_packet(count: usize) -> ConversationPacket {
    let conv = NormalizedConversation {
        agent_slug: "codebuff".into(),
        external_id: Some("shared-lineage-chat".into()),
        title: Some("Shared lineage".into()),
        workspace: Some(PathBuf::from("/work/project")),
        source_path: PathBuf::from("/shared/manicode/chat-messages.json"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_000),
        metadata: serde_json::json!({"cass":{"origin":{"source_id":"remote-source","kind":"ssh","host":"remote-host"}}}),
        messages: (0..count)
            .map(|position| NormalizedMessage {
                idx: i64::try_from(position).unwrap() * 3 + 5,
                role: "assistant".into(),
                author: None,
                created_at: Some(1_700_000_000_000),
                content: format!("oldrevision marker{position}"),
                extra: Value::Null,
                snippets: Vec::new(),
                invocations: Vec::new(),
            })
            .collect(),
    };
    ConversationPacket::from_normalized_conversation(&conv, ConversationPacketProvenance::local())
}

/// The indexer preflight asked for the summary, then validated the
/// contract: two opens, and each Quill open hashes every live segment.
#[test]
fn validated_summary_takes_its_counts_from_one_index_open() {
    let dir = TempDir::new().expect("temp dir");
    {
        let mut index = TantivyIndex::open_or_create(dir.path()).expect("create");
        index
            .add_messages_from_packet(&gh423_lexical_packet(3), None, Some(7), |_| Ok(()))
            .expect("add messages");
        index.commit().expect("commit");
    }
    let opens = crate::search::quill_bridge::reader_open_count();
    let summary = searchable_index_summary(dir.path())
        .expect("summary")
        .expect("summary present");
    validate_searchable_index_contract(dir.path()).expect("contract");
    assert_eq!(crate::search::quill_bridge::reader_open_count() - opens, 2);

    let opens = crate::search::quill_bridge::reader_open_count();
    let validated = validated_searchable_index_summary(dir.path()).expect("validated");
    assert_eq!(crate::search::quill_bridge::reader_open_count() - opens, 1);
    assert_eq!(
        (validated.docs, validated.segments),
        (summary.docs, summary.segments)
    );
    assert_eq!(validated.docs, 3);

    let empty = TempDir::new().expect("temp dir");
    assert!(
        validated_searchable_index_summary(empty.path()).is_err(),
        "a directory without an index fails validation"
    );
}

fn gh423_lexical_hits(index: &TantivyIndex, term: &str) -> BTreeSet<String> {
    let reader = index.reader().unwrap();
    let parser = frankensearch::quill::query::CassQueryParser::new(
        frankensearch::quill::schema::CASS_SEMANTIC_SCHEMA,
    )
    .unwrap();
    let query = parser.parse(
        term,
        &frankensearch::quill::query::CassQueryFilters::default(),
    );
    let page =
        crate::search::quill_bridge::search_paginated(&reader, &query.query, 128, 0, true).unwrap();
    let hits: BTreeSet<_> = page
        .hits
        .iter()
        .map(|hit| hit.document_id.clone())
        .collect();
    assert_eq!(
        hits.len(),
        page.hits.len(),
        "each stable identity is live exactly once"
    );
    assert_eq!(page.total_count, Some(hits.len()));
    hits
}

fn gh423_lexical_identity(
    packet: &ConversationPacket,
    position: usize,
    conversation_id: i64,
) -> String {
    let context = cass_doc_context_from_packet(packet);
    frankensearch::quill::cass::cass_document_identity(
        &context.source_id,
        frankensearch::quill::cass::CassConversationKey {
            source_path: &context.source_path,
            id: Some(conversation_id),
        },
        packet.payload.messages[position].idx.max(0) as u64,
    )
}

#[test]
fn gh423_lexical_revisions_replace_all_32_and_preserve_untouched_across_batches() {
    let dir = TempDir::new().unwrap();
    let mut index = TantivyIndex::open_or_create(dir.path()).unwrap();
    let mut packet = gh423_lexical_packet(33);
    index
        .add_messages_from_packet(&packet, None, Some(77), |_| Ok(()))
        .unwrap();
    index.commit().unwrap();
    assert_eq!(index.doc_count().unwrap(), 33);
    let indices: Vec<usize> = (0..32).collect();
    let expected: BTreeSet<String> = indices
        .iter()
        .map(|&idx| gh423_lexical_identity(&packet, idx, 77))
        .collect();
    let untouched = gh423_lexical_identity(&packet, 32, 77);
    for &position in &indices {
        packet.payload.messages[position].content =
            format!("newrevision completed response marker{position}");
    }
    // Run the production batching body with small limits to cross both
    // document and content thresholds without a giant fixture or env race.
    index
        .reconcile_packet_messages_with_limits(&packet, &indices, Some(77), 7, 80)
        .unwrap();
    index.commit().unwrap();
    assert_eq!(index.doc_count().unwrap(), 33);
    assert_eq!(gh423_lexical_hits(&index, "newrevision"), expected);
    assert_eq!(
        gh423_lexical_hits(&index, "oldrevision"),
        BTreeSet::from([untouched.clone()])
    );
    let mut repeated_selectors = indices.clone();
    repeated_selectors.push(0);
    index
        .reconcile_messages_from_packet(&packet, &repeated_selectors, Some(77))
        .unwrap();
    index.commit().unwrap();
    assert_eq!(index.doc_count().unwrap(), 33);
    assert_eq!(gh423_lexical_hits(&index, "newrevision"), expected);
    for &position in &indices {
        packet.payload.messages[position].content.clear();
    }
    index
        .reconcile_packet_messages_with_limits(&packet, &indices, Some(77), 7, 80)
        .unwrap();
    assert_eq!(index.doc_count().unwrap(), 1);
    assert_eq!(
        gh423_lexical_hits(&index, "oldrevision"),
        BTreeSet::from([untouched])
    );
    assert!(gh423_lexical_hits(&index, "newrevision").is_empty());
    index
        .reconcile_messages_from_packet(&packet, &indices, Some(77))
        .unwrap();
    assert_eq!(
        index.doc_count().unwrap(),
        1,
        "repeated removals are idempotent"
    );
}

#[test]
fn gh423_lexical_noise_removal_commits_pending_work_and_preserves_sibling_identity() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let dir = TempDir::new().unwrap();
    let mut index = TantivyIndex::open_or_create(dir.path()).unwrap();
    let ticks = Arc::new(AtomicUsize::new(0));
    let heartbeat = Arc::clone(&ticks);
    index.set_heartbeat(Some(Arc::new(move || {
        heartbeat.fetch_add(1, Ordering::Relaxed);
    })));
    let mut packet = gh423_lexical_packet(3);
    index
        .add_messages_from_packet(&packet, None, Some(77), |_| Ok(()))
        .unwrap();
    index
        .add_messages_from_packet(&packet, None, Some(88), |_| Ok(()))
        .unwrap();
    index.commit().unwrap();
    let mut pending = gh423_lexical_packet(1);
    pending.payload.identity.source_path = "/pending/chat-messages.json".into();
    pending.payload.messages[0].content = "pendingneedle survives removal".into();
    index
        .add_messages_from_packet(&pending, None, Some(99), |_| Ok(()))
        .unwrap();
    assert_eq!(
        index.doc_count().unwrap(),
        6,
        "plain addition is still pending"
    );
    let before_ticks = ticks.load(Ordering::Relaxed);
    packet.payload.messages[0].content = "   \n\t".into();
    packet.payload.messages[1].content = "OK".into();
    index
        .reconcile_messages_from_packet(&packet, &[0, 1], Some(77))
        .unwrap();
    assert!(
        ticks.load(Ordering::Relaxed) >= before_ticks + 2,
        "commit and removal both tick liveness"
    );
    assert_eq!(index.doc_count().unwrap(), 5);
    let mut survivors: BTreeSet<String> = (0..3)
        .map(|position| gh423_lexical_identity(&packet, position, 88))
        .collect();
    survivors.insert(gh423_lexical_identity(&packet, 2, 77));
    assert_eq!(gh423_lexical_hits(&index, "oldrevision"), survivors);
    assert_eq!(
        gh423_lexical_hits(&index, "pendingneedle"),
        BTreeSet::from([gh423_lexical_identity(&pending, 0, 99)])
    );
    index
        .reconcile_messages_from_packet(&packet, &[0, 1], Some(77))
        .unwrap();
    assert_eq!(index.doc_count().unwrap(), 5);
    index.set_heartbeat(None);
}

#[test]
fn gh423_lexical_out_of_range_selector_refuses_before_any_publication() {
    let dir = TempDir::new().unwrap();
    let mut index = TantivyIndex::open_or_create(dir.path()).unwrap();
    let mut packet = gh423_lexical_packet(2);
    index
        .add_messages_from_packet(&packet, None, Some(77), |_| Ok(()))
        .unwrap();
    index.commit().unwrap();
    let before = gh423_lexical_hits(&index, "oldrevision");
    packet.payload.messages[0].content = "newrevision must not publish".into();
    let error = index
        .reconcile_messages_from_packet(&packet, &[0, 2], Some(77))
        .unwrap_err();
    assert!(error.to_string().contains("out of range"));
    assert_eq!(index.doc_count().unwrap(), 2);
    assert_eq!(gh423_lexical_hits(&index, "oldrevision"), before);
    assert!(gh423_lexical_hits(&index, "newrevision").is_empty());
    index
        .reconcile_messages_from_packet(&packet, &[], Some(77))
        .unwrap();
    assert_eq!(index.doc_count().unwrap(), 2);
}

/// #440: an armed resume reconcile routes exactly the owed leading
/// documents through the upsert path and plain-adds the rest, so a replay
/// over a published prefix converges without a duplicate-identity refusal
/// and without paying identity probes for the whole remainder.
#[test]
fn armed_resume_reconcile_upserts_only_the_owed_prefix() {
    let dir = TempDir::new().expect("tempdir");
    let mut index = TantivyIndex::open_or_create(dir.path()).expect("open");
    let doc = |msg_idx: u64, content: &str| FsCassDocument {
        agent: "claude".to_owned(),
        workspace: Some("cass".to_owned()),
        workspace_original: Some("cass".to_owned()),
        source_path: "/transcripts/resume.jsonl".to_owned(),
        msg_idx,
        created_at: Some(1_700_000_000),
        title: Some("resume".to_owned()),
        content: content.to_owned(),
        source_id: "local".to_owned(),
        origin_kind: "local".to_owned(),
        origin_host: None,
        conversation_id: Some(7),
    };
    // The "published before the interruption" prefix.
    let published = [doc(0, "alpha published"), doc(1, "beta published")];
    let refs: Vec<FsCassDocumentRef<'_>> = published.iter().map(FsCassDocument::as_ref).collect();
    index
        .add_prebuilt_document_refs_slice(&refs)
        .expect("publish prefix");
    index.commit().expect("commit prefix");
    assert_eq!(index.doc_count().expect("doc count"), 2);

    // The replay from the stale cursor re-sends the prefix plus new docs.
    let replay = [
        doc(0, "alpha published"),
        doc(1, "beta published"),
        doc(2, "gamma new"),
        doc(3, "delta new"),
    ];
    let refs: Vec<FsCassDocumentRef<'_>> = replay.iter().map(FsCassDocument::as_ref).collect();
    index.arm_resume_reconcile(2);
    assert_eq!(index.resume_reconcile_docs_remaining(), 2);
    let written = index
        .add_prebuilt_document_refs_slice(&refs)
        .expect("replay converges through the reconcile window");
    assert_eq!(written, 4);
    assert_eq!(
        index.resume_reconcile_docs_remaining(),
        0,
        "the owed prefix is consumed exactly once"
    );
    index.commit().expect("commit replay");
    assert_eq!(index.doc_count().expect("doc count"), 4);

    // With the window consumed, a further duplicate is refused again —
    // the reconcile is a bounded window, not a permanent mode switch.
    let dup = [doc(2, "gamma new")];
    let refs: Vec<FsCassDocumentRef<'_>> = dup.iter().map(FsCassDocument::as_ref).collect();
    let refused = index.add_prebuilt_document_refs_slice(&refs);
    assert!(
        refused
            .as_ref()
            .is_err_and(|error| error.to_string().contains("duplicate live document id")),
        "expected the duplicate refusal after the window closed: {refused:?}"
    );
}

/// A published Quill index must produce a fingerprint the daemon can parse.
///
/// Regression: the fingerprint reader only knew Tantivy's `meta.json`, so a
/// live Quill index returned `None`. Both consumers treat `None` as "no
/// published index", which silently broke the daemon's served-generation
/// tracking and the rebuild checkpoint's meta fingerprint. Asserting only
/// "is Some" would be too weak — the daemon parses the first 16 characters
/// as hex, so a non-hex fingerprint would satisfy `is_some()` and still
/// break it.
#[test]
fn searchable_index_fingerprint_recognizes_a_published_quill_index() {
    let dir = TempDir::new().expect("temp dir");
    let mut index = TantivyIndex::open_or_create(dir.path()).expect("create index");
    index.commit().expect("commit");

    let fingerprint = searchable_index_fingerprint(dir.path())
        .expect("fingerprint read must not error")
        .expect("a published Quill index must have a fingerprint");

    assert!(
        fingerprint.len() >= 16,
        "fingerprint too short for the daemon's 16-char generation slice: {fingerprint:?}"
    );
    assert!(
        u64::from_str_radix(&fingerprint[..16], 16).is_ok(),
        "daemon parses the first 16 chars as hex; got {fingerprint:?}"
    );
}

/// Status falls back to the lexical publication timestamp when the
/// canonical DB has no recorded indexing time. After the Quill flip, a
/// standard published index has `MANIFEST` rather than Tantivy's
/// `meta.json`; overlooking that marker made the fallback falsely return
/// `None` for the current engine.
#[test]
fn searchable_index_modified_time_recognizes_a_published_quill_index() {
    let dir = TempDir::new().expect("temp dir");
    let mut index = TantivyIndex::open_or_create(dir.path()).expect("create index");
    index.commit().expect("commit");

    assert!(
        !dir.path().join("meta.json").exists(),
        "the regression requires a Quill-only publication"
    );
    assert!(
        searchable_index_modified_time(dir.path()).is_some(),
        "a published Quill MANIFEST must provide the status fallback timestamp"
    );
}

#[test]
fn index_dir_creates_versioned_path() {
    let dir = TempDir::new().expect("temp dir");
    let result = index_dir(dir.path()).expect("index dir");
    // The schema-generation sentinel now comes from Quill: the CASS->Quill
    // flip bumped it to v9-quill, a SIBLING of the Tantivy-era v8, so the
    // new index never reads foreign segments.
    assert!(result.ends_with("index/v9-quill"));
}

#[test]
fn tokenizer_registration_is_callable() {
    let dir = TempDir::new().expect("temp dir");
    let mut idx = Index::create_in_ram(build_schema());
    ensure_tokenizer(&mut idx);
    let _ = TantivyIndex::open_or_create(dir.path()).expect("open or create");
}

#[test]
fn add_messages_batches_large_payloads_without_dropping_docs() {
    let dir = TempDir::new().expect("temp dir");
    let mut idx = TantivyIndex::open_or_create(dir.path()).expect("create index");
    let content = "x".repeat(4096);
    let messages: Vec<_> = (0..1_200)
        .map(|i| NormalizedMessage {
            idx: i,
            role: "assistant".to_string(),
            author: None,
            created_at: Some(1_700_000_000_000 + i),
            content: format!("{i}-{content}"),
            extra: Value::Null,
            snippets: Vec::new(),
            invocations: Vec::new(),
        })
        .collect();
    let conv = NormalizedConversation {
        agent_slug: "codex".to_string(),
        external_id: Some("large-batch".to_string()),
        title: Some("Large Batch".to_string()),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        source_path: PathBuf::from("/tmp/rollout.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_999),
        metadata: Value::Null,
        messages,
    };

    idx.add_messages(&conv, &conv.messages)
        .expect("add messages");
    idx.commit().expect("commit");

    let reader = idx.reader().expect("reader");
    crate::search::quill_bridge::refresh_reader(&reader).expect("reload");
    assert_eq!(
        reader.doc_count().expect("doc count"),
        conv.messages.len() as u64
    );
}

#[test]
fn add_conversations_with_ids_streams_large_payloads_without_dropping_docs() {
    let dir = TempDir::new().expect("temp dir");
    let mut idx = TantivyIndex::open_or_create(dir.path()).expect("create index");
    let content = "y".repeat(2048);
    let conversations: Vec<_> = (0..24)
        .map(|conv_idx| {
            let messages = (0..256)
                .map(|msg_idx| NormalizedMessage {
                    idx: msg_idx,
                    role: "assistant".to_string(),
                    author: None,
                    created_at: Some(1_700_000_000_000 + (conv_idx * 1_000 + msg_idx)),
                    content: format!("conv-{conv_idx}-msg-{msg_idx}-{content}"),
                    extra: Value::Null,
                    snippets: Vec::new(),
                    invocations: Vec::new(),
                })
                .collect();
            NormalizedConversation {
                agent_slug: "codex".to_string(),
                external_id: Some(format!("conv-{conv_idx}")),
                title: Some(format!("Conversation {conv_idx}")),
                workspace: Some(PathBuf::from("/tmp/workspace")),
                source_path: PathBuf::from(format!("/tmp/rollout-{conv_idx}.jsonl")),
                started_at: Some(1_700_000_000_000 + conv_idx),
                ended_at: Some(1_700_000_000_999 + conv_idx),
                metadata: Value::Null,
                messages,
            }
        })
        .collect();
    let expected_docs: usize = conversations.iter().map(|conv| conv.messages.len()).sum();

    let indexed_docs = idx
        .add_conversations_with_ids(conversations.iter().map(|conv| (conv, Some(42))))
        .expect("add conversations");
    assert_eq!(indexed_docs, expected_docs);
    idx.commit().expect("commit");

    let reader = idx.reader().expect("reader");
    crate::search::quill_bridge::refresh_reader(&reader).expect("reload");
    assert_eq!(reader.doc_count().expect("doc count"), expected_docs as u64);
}

#[test]
fn normalized_index_source_id_infers_remote_from_origin_host_without_kind() {
    let source_id = normalized_index_source_id(Some("   "), None, Some("dev@laptop"));
    assert_eq!(source_id, "dev@laptop");
    assert_eq!(normalized_index_origin_kind(&source_id, None), "remote");
}

#[test]
fn add_prebuilt_documents_streams_large_payloads_without_dropping_docs() {
    let dir = TempDir::new().expect("temp dir");
    let mut idx = TantivyIndex::open_or_create(dir.path()).expect("create index");
    let content = "z".repeat(2048);
    let docs: Vec<_> = (0..6_144)
        .map(|msg_idx| FsCassDocument {
            agent: "codex".to_string(),
            workspace: Some("/tmp/workspace".to_string()),
            workspace_original: None,
            source_path: "/tmp/prebuilt-rollout.jsonl".to_string(),
            msg_idx: msg_idx as u64,
            created_at: Some(1_700_000_000_000 + msg_idx as i64),
            title: Some("Prebuilt Batch".to_string()),
            content: format!("prebuilt-msg-{msg_idx}-{content}"),
            conversation_id: Some(7),
            source_id: crate::sources::provenance::LOCAL_SOURCE_ID.to_string(),
            origin_kind: crate::sources::provenance::LOCAL_SOURCE_ID.to_string(),
            origin_host: None,
        })
        .collect();
    let expected_docs = docs.len();

    let indexed_docs = idx.add_prebuilt_documents(docs).expect("add prebuilt docs");
    assert_eq!(indexed_docs, expected_docs);
    idx.commit().expect("commit");

    let reader = idx.reader().expect("reader");
    crate::search::quill_bridge::refresh_reader(&reader).expect("reload");
    assert_eq!(reader.doc_count().expect("doc count"), expected_docs as u64);
}

/// Equivalence gate for `coding_agent_session_search-ibuuh.32`:
/// the packet-driven lexical pipeline (`add_messages_from_packet`)
/// must emit byte-identical CassDocuments to the legacy
/// `add_messages_with_conversation_id` path on the same input.
/// This proves the migration of `add_conversation*` is a true
/// no-op semantically while removing the duplicate normalization
/// pass, so future migration slices in indexer/mod.rs can adopt
/// `add_messages_from_packet` with confidence.
#[test]
fn packet_driven_lexical_pipeline_matches_legacy_for_normalized_conv() {
    use crate::model::conversation_packet::{ConversationPacket, ConversationPacketProvenance};

    fn make_conv() -> NormalizedConversation {
        NormalizedConversation {
            agent_slug: "codex".to_string(),
            external_id: Some("packet-equivalence".to_string()),
            title: Some("Packet Equivalence".to_string()),
            workspace: Some(PathBuf::from("/work/eq")),
            source_path: PathBuf::from("/work/eq/.codex/session.jsonl"),
            started_at: Some(1_700_000_000_000),
            ended_at: Some(1_700_000_010_000),
            metadata: serde_json::json!({
                "cass": {
                    "origin": {
                        "source_id": "remote-host",
                        "kind": "ssh",
                        "host": "ws-42.example",
                    },
                    "workspace_original": "/Users/dev/eq",
                },
                "model": "gpt-5",
            }),
            messages: vec![
                NormalizedMessage {
                    idx: 0,
                    role: "user".to_string(),
                    author: Some("human".to_string()),
                    created_at: Some(1_700_000_000_000),
                    content: "explain the packet pipeline".to_string(),
                    extra: serde_json::json!({"turn": 1}),
                    snippets: Vec::new(),
                    invocations: Vec::new(),
                },
                NormalizedMessage {
                    idx: 1,
                    role: "assistant".to_string(),
                    author: None,
                    created_at: Some(1_700_000_001_000),
                    content: "the pipeline normalizes once".to_string(),
                    extra: Value::Null,
                    snippets: Vec::new(),
                    invocations: Vec::new(),
                },
                NormalizedMessage {
                    idx: 2,
                    role: "tool".to_string(),
                    author: Some("ripgrep".to_string()),
                    created_at: Some(1_700_000_002_000),
                    content: "matches: 3".to_string(),
                    extra: Value::Null,
                    snippets: Vec::new(),
                    invocations: Vec::new(),
                },
            ],
        }
    }

    let legacy_dir = TempDir::new().expect("legacy temp dir");
    let mut legacy_idx = TantivyIndex::open_or_create(legacy_dir.path()).expect("legacy idx");
    let conv = make_conv();
    legacy_idx
        .add_messages_with_conversation_id(&conv, &conv.messages, Some(99))
        .expect("legacy add");
    legacy_idx.commit().expect("legacy commit");
    let legacy_reader = legacy_idx.reader().expect("legacy reader");
    crate::search::quill_bridge::refresh_reader(&legacy_reader).expect("legacy reload");
    let legacy_count = legacy_reader.doc_count().expect("doc count");

    let packet_dir = TempDir::new().expect("packet temp dir");
    let mut packet_idx = TantivyIndex::open_or_create(packet_dir.path()).expect("packet idx");
    let packet = ConversationPacket::from_normalized_conversation(
        &conv,
        ConversationPacketProvenance::local(),
    );
    packet_idx
        .add_messages_from_packet(&packet, None, Some(99), |_| Ok(()))
        .expect("packet add");
    packet_idx.commit().expect("packet commit");
    let packet_reader = packet_idx.reader().expect("packet reader");
    crate::search::quill_bridge::refresh_reader(&packet_reader).expect("packet reload");
    let packet_count = packet_reader.doc_count().expect("doc count");

    assert_eq!(
        legacy_count, packet_count,
        "packet pipeline must emit the same number of docs as legacy: legacy={legacy_count} packet={packet_count}"
    );
    assert_eq!(
        legacy_count,
        conv.messages.len() as u64,
        "all 3 fixture messages should land (none filter as hard noise)"
    );

    // Compare every stored field byte-for-byte by reconstructing the
    // CassDocument list both pipelines fed into Tantivy. This sidesteps
    // schema-coupled retrieval boilerplate and pins the property the
    // bead acceptance gate cares about: same projection, same docs.
    let legacy_context = cass_doc_context(&conv, Some(99));
    let legacy_docs: Vec<FsCassDocument> = conv
        .messages
        .iter()
        .filter_map(|m| cass_document_for_message(&legacy_context, m))
        .collect();
    let packet_context_owned = {
        let mut ctx = cass_doc_context_from_packet(&packet);
        ctx.conversation_id = Some(99);
        ctx
    };
    let packet_docs: Vec<FsCassDocument> = packet
        .payload
        .messages
        .iter()
        .filter_map(|m| cass_document_for_packet_message(&packet_context_owned, m))
        .collect();
    assert_eq!(
        legacy_docs.len(),
        packet_docs.len(),
        "packet doc list length should match legacy"
    );
    for (legacy_doc, packet_doc) in legacy_docs.iter().zip(packet_docs.iter()) {
        assert_eq!(legacy_doc.agent, packet_doc.agent);
        assert_eq!(legacy_doc.workspace, packet_doc.workspace);
        assert_eq!(legacy_doc.workspace_original, packet_doc.workspace_original);
        assert_eq!(legacy_doc.source_path, packet_doc.source_path);
        assert_eq!(legacy_doc.msg_idx, packet_doc.msg_idx);
        assert_eq!(legacy_doc.created_at, packet_doc.created_at);
        assert_eq!(legacy_doc.title, packet_doc.title);
        assert_eq!(legacy_doc.content, packet_doc.content);
        assert_eq!(legacy_doc.conversation_id, packet_doc.conversation_id);
        assert_eq!(
            legacy_doc.source_id, packet_doc.source_id,
            "source_id must match (remote-host normalization is the bead's tripwire)"
        );
        assert_eq!(legacy_doc.origin_kind, packet_doc.origin_kind);
        assert_eq!(legacy_doc.origin_host, packet_doc.origin_host);
    }
    // Sanity check the remote-host provenance actually round-tripped:
    // a regression in normalization on either side would silently
    // pass the per-doc compare unless we pin the expected value too.
    let first_packet_doc = packet_docs
        .first()
        .expect("packet fixture should emit at least one doc");
    assert_eq!(
        first_packet_doc.source_id, "remote-host",
        "metadata.cass.origin.source_id must be the canonical value"
    );
    assert_eq!(
        first_packet_doc.origin_host.as_deref(),
        Some("ws-42.example"),
        "metadata.cass.origin.host must surface as origin_host"
    );
}

/// Pins the `add_conversation_with_id` migration: the convenience
/// entrypoint now routes through the packet pipeline, but operators
/// see no behavioral change. The doc count and conversation_id
/// stamping must match the legacy `add_messages_with_conversation_id`
/// path on the same fixture.
#[test]
fn add_conversation_with_id_packet_path_emits_expected_doc_count() {
    fn fixture(id: i64) -> NormalizedConversation {
        NormalizedConversation {
            agent_slug: "codex".to_string(),
            external_id: Some(format!("conv-{id}")),
            title: Some(format!("Conv {id}")),
            workspace: None,
            source_path: PathBuf::from(format!("/tmp/conv-{id}.jsonl")),
            started_at: Some(1_700_000_000_000 + id),
            ended_at: Some(1_700_000_001_000 + id),
            metadata: Value::Null,
            messages: vec![
                NormalizedMessage {
                    idx: 0,
                    role: "user".to_string(),
                    author: None,
                    created_at: Some(1_700_000_000_000 + id),
                    content: format!("hello-{id}"),
                    extra: Value::Null,
                    snippets: Vec::new(),
                    invocations: Vec::new(),
                },
                NormalizedMessage {
                    idx: 1,
                    role: "assistant".to_string(),
                    author: None,
                    created_at: Some(1_700_000_000_500 + id),
                    content: format!("response-{id}"),
                    extra: Value::Null,
                    snippets: Vec::new(),
                    invocations: Vec::new(),
                },
            ],
        }
    }

    let dir = TempDir::new().expect("temp dir");
    let mut idx = TantivyIndex::open_or_create(dir.path()).expect("idx");
    idx.add_conversation_with_id(&fixture(1), Some(101))
        .expect("conv 1");
    idx.add_conversation_with_id(&fixture(2), Some(102))
        .expect("conv 2");
    idx.commit().expect("commit");

    let reader = idx.reader().expect("reader");
    crate::search::quill_bridge::refresh_reader(&reader).expect("reload");
    assert_eq!(
        reader.doc_count().expect("doc count"),
        4,
        "two conversations × two messages each ⇒ four lexical docs"
    );
}

/// GH #499 (2l1b0.49): a date-filtered search over a sealed segment that
/// holds a tombstone. Under frankensearch-quill 0.3.1 the numeric window
/// was scored on the segment's at-seal rows while the term leaf used its
/// live rows, and cass reported "posting cursor invariant failed: Boolean
/// children belong to different segment domains" (exit 9) for every
/// `--days`/`--since` search on a long-lived index. Quill 0.3.2 carries
/// the fix. The window deliberately excludes one row: a window over every
/// row lowers to a whole-segment match and would pass on the old engine.
#[test]
fn gh499_date_window_over_a_tombstoned_sealed_segment() {
    let message = |idx: i64, created_at: i64, content: &str| NormalizedMessage {
        idx,
        role: "user".to_string(),
        author: None,
        created_at: Some(created_at),
        content: content.to_string(),
        extra: Value::Null,
        snippets: Vec::new(),
        invocations: Vec::new(),
    };
    let dir = TempDir::new().expect("temp dir");
    let conversation = NormalizedConversation {
        agent_slug: "codex".to_string(),
        external_id: Some("gh499".to_string()),
        title: Some("window".to_string()),
        workspace: None,
        source_path: dir.path().join("gh499.jsonl"),
        started_at: Some(100),
        ended_at: None,
        metadata: Value::Null,
        messages: vec![
            message(0, 100, "window early"),
            message(1, 500, "window middle"),
            message(2, 900, "window late"),
        ],
    };
    let index_dir = dir.path().join("index");
    let mut idx = TantivyIndex::open_or_create(&index_dir).expect("idx");
    idx.add_conversation(&conversation).expect("seed");
    idx.commit().expect("seal the first segment");

    // Re-ingesting a message under its stable identity tombstones the
    // sealed row, as an incremental index run does.
    let edited = message(1, 500, "window middle, edited");
    let replacement = cass_document_for_message(&cass_doc_context(&conversation, None), &edited)
        .expect("replacement document");
    idx.upsert_prebuilt_documents_slice(&[replacement])
        .expect("upsert");
    idx.commit().expect("publish the replacement");

    let live = crate::search::quill_bridge::manifest_live_doc_count(&index_dir)
        .expect("published manifest");
    assert!(
        live.tombstones > 0 && live.segments >= 2,
        "the fixture needs a tombstone in a sealed segment: {live:?}"
    );

    let client = crate::search::query::SearchClient::open(&index_dir, None)
        .expect("open search client")
        .expect("index present");
    let filters = crate::search::query::SearchFilters {
        created_from: Some(400),
        created_to: Some(1_000),
        ..Default::default()
    };
    let hits = client
        .search(
            "window",
            filters,
            10,
            0,
            crate::search::query::FieldMask::FULL,
        )
        .expect("a date-filtered search over a tombstoned segment must succeed");
    let mut stamps: Vec<i64> = hits.iter().filter_map(|hit| hit.created_at).collect();
    stamps.sort_unstable();
    assert_eq!(stamps, vec![500, 900], "{hits:?}");
    assert!(
        hits.iter().any(|hit| hit.content.contains("edited")),
        "the live replacement is served, not the tombstoned row: {hits:?}"
    );
}
