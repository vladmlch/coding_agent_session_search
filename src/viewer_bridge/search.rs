//! Raw Quill message enumeration. No CASS noise reduction or session grouping.
use std::path::Path;

use anyhow::Result;
use coding_agent_search::search::quill_bridge::{
    QuillCassFields, QuillLexicalPage, open_cass_reader, search_paginated, stored_i64, stored_text,
    stored_u64,
};
use coding_agent_search::search::tantivy::{
    expected_index_dir, open_federated_search_readers, searchable_index_fingerprint,
    validate_searchable_index_contract,
};
use frankensearch::quill::QuillSearchIndex;
use frankensearch::quill::query::{
    CassQueryFilters, CassQueryParser, CassSourceFilter, Query, QueryNode,
};
use frankensearch::quill::schema::CASS_SEMANTIC_SCHEMA;
use serde::Serialize;

use super::protocol::{BridgeFilters, refuse};

pub struct ReaderSet {
    pub readers: Vec<QuillSearchIndex>,
    pub keys: Vec<String>,
    pub index_epoch: String,
}

impl ReaderSet {
    pub fn open(data_dir: &Path, worker_epoch: &str) -> Result<Self> {
        let path = expected_index_dir(data_dir);
        let before = searchable_index_fingerprint(&path)?.ok_or_else(|| {
            refuse(
                "index_unavailable",
                "No published lexical index at the selected data directory",
            )
        })?;
        let readers = if let Some(readers) = open_federated_search_readers(&path)? {
            readers
                .into_iter()
                .map(|(reader, _)| reader)
                .collect::<Vec<_>>()
        } else {
            validate_searchable_index_contract(&path).map_err(|e| {
                refuse(
                    "index_unavailable",
                    format!("Lexical index failed contract validation: {e}"),
                )
            })?;
            vec![open_cass_reader(&path)?]
        };
        if readers.is_empty() || readers.len() > 32 {
            return Err(refuse(
                "ranking_budget_exceeded",
                "Reader set is empty or exceeds the 32-shard profile",
            ));
        }
        if searchable_index_fingerprint(&path)?.as_ref() != Some(&before) {
            return Err(refuse(
                "stale_snapshot",
                "Index published while opening the reader set",
            ));
        }
        let index_epoch = format!("{worker_epoch}:{}:{before}", rand::random::<u64>());
        let keys = (0..readers.len())
            .map(|idx| format!("{index_epoch}:{idx:08}"))
            .collect();
        Ok(Self {
            readers,
            keys,
            index_epoch,
        })
    }
}

pub fn engine_filters(filters: &BridgeFilters) -> CassQueryFilters {
    CassQueryFilters {
        agents: filters.agents.to_vec(),
        workspaces: filters.workspaces.to_vec(),
        created_from: filters.created_from,
        created_to: filters.created_to,
        source_filter: filters
            .source_id
            .as_ref()
            .map_or(CassSourceFilter::All, |id| {
                // Match the reserved local spelling accepted by CASS lexical
                // filters; remote source identifiers retain their exact case.
                let local = coding_agent_search::sources::provenance::LOCAL_SOURCE_ID;
                CassSourceFilter::SourceId(if id.eq_ignore_ascii_case(local) {
                    local.to_owned()
                } else {
                    id.clone()
                })
            }),
    }
}

pub fn parse_lexical_query(text: &str, filters: &CassQueryFilters) -> Result<(Query, Vec<String>)> {
    let parser = CassQueryParser::new(CASS_SEMANTIC_SCHEMA)
        .map_err(|e| anyhow::anyhow!("Building CASS lexical query parser: {e}"))?;
    let unfiltered = parser.parse(text, &CassQueryFilters::default());
    if matches!(unfiltered.query.root(), QueryNode::All | QueryNode::Empty) {
        return Err(refuse(
            "invalid_request",
            "Lexical query contains no searchable terms or collapsed to an empty/match-all expression",
        ));
    }
    let parsed = parser.parse(text, filters);
    let issues = parsed
        .diagnostics
        .into_iter()
        .map(|diag| diag.message)
        .collect::<Vec<_>>();
    Ok((parsed.query, issues))
}

pub fn raw_page(
    reader: &QuillSearchIndex,
    text: &str,
    filters: &CassQueryFilters,
    offset: usize,
    limit: usize,
) -> Result<QuillLexicalPage> {
    let (query, _) = parse_lexical_query(text, filters)?;
    // Stage 1: the aggregated session×scope documents (msg_idx >=
    // SESSION_DOC_BASE) share the index but carry no region coordinates —
    // the per-region lane must never enumerate them as phantom hits.
    let query = coding_agent_search::search::quill_bridge::region_documents_query(query);
    search_paginated(reader, &query, limit, offset, true).map_err(|error| {
        if coding_agent_search::search::quill_bridge::is_query_fuel_exhausted(&error) {
            refuse(
                "ranking_budget_exceeded",
                "Quill query fuel exhausted; no complete ranking receipt",
            )
        } else {
            error
        }
    })
}

/// [`raw_page`] over the Stage 1 session×scope documents only — the fast
/// lane's membership posting set. Every hit decodes to the conversation
/// whose (scope, chunk) document carries the literal; region documents and
/// canonical messages stay out of the page and the exact total.
pub fn raw_page_session_docs(
    reader: &QuillSearchIndex,
    text: &str,
    filters: &CassQueryFilters,
    offset: usize,
    limit: usize,
) -> Result<QuillLexicalPage> {
    let (query, _) = parse_lexical_query(text, filters)?;
    let query = coding_agent_search::search::quill_bridge::session_scope_documents_query(query);
    search_paginated(reader, &query, limit, offset, true).map_err(|error| {
        if coding_agent_search::search::quill_bridge::is_query_fuel_exhausted(&error) {
            refuse(
                "ranking_budget_exceeded",
                "Quill query fuel exhausted; no complete ranking receipt",
            )
        } else {
            error
        }
    })
}

/// [`raw_page`] over canonical message documents only.
///
/// Raw rank cursors map every hit to a canonical
/// `(conversation_id, message_index)` chat coordinate. Persisted
/// search-region documents share the index in the synthetic
/// `msg_idx >= REGION_DOC_BASE` space (Task 5) and would surface here as
/// phantom chat hits, so the enumeration wraps the parsed query in the
/// canonical-message guard. `search_sessions` deliberately keeps region
/// documents in scope through [`raw_page`] (which itself excludes the
/// Stage 1 session×scope documents at `msg_idx >= SESSION_DOC_BASE`
/// through its own guard), which is why the exclusion lives in sibling
/// entry points rather than inside `raw_page` itself.
pub fn raw_page_canonical(
    reader: &QuillSearchIndex,
    text: &str,
    filters: &CassQueryFilters,
    offset: usize,
    limit: usize,
) -> Result<QuillLexicalPage> {
    let (query, _) = parse_lexical_query(text, filters)?;
    let query = coding_agent_search::search::quill_bridge::canonical_message_query(query);
    search_paginated(reader, &query, limit, offset, true).map_err(|error| {
        if coding_agent_search::search::quill_bridge::is_query_fuel_exhausted(&error) {
            refuse(
                "ranking_budget_exceeded",
                "Quill query fuel exhausted; no complete ranking receipt",
            )
        } else {
            error
        }
    })
}

#[derive(Debug, Clone, Serialize)]
pub struct RawRankHit {
    pub source_id: String,
    pub conversation_id: i64,
    pub message_index_zero_based: u64,
    pub score: f32,
    pub reader_key: String,
}

pub fn decode_hit(
    reader: &QuillSearchIndex,
    hit: &coding_agent_search::search::quill_bridge::QuillLexicalDocHit,
    reader_key: &str,
) -> Result<RawRankHit> {
    let fields = QuillCassFields::compiled();
    let missing = || {
        refuse(
            "unmappable_hit",
            "A raw hit has no unambiguous stored canonical coordinates",
        )
    };
    let source_id = stored_text(reader, fields.source_id, hit.global_docid)?.ok_or_else(missing)?;
    let conversation_id =
        stored_i64(reader, fields.conversation_id, hit.global_docid)?.ok_or_else(missing)?;
    let message_index_zero_based =
        stored_u64(reader, fields.msg_idx, hit.global_docid)?.ok_or_else(missing)?;
    if source_id.is_empty()
        || source_id.len() > 4096
        || conversation_id <= 0
        || message_index_zero_based > i64::MAX as u64
        || !hit.bm25_score.is_finite()
    {
        return Err(missing());
    }
    // `document_id` is deliberately opaque. Never split it to infer identity.
    Ok(RawRankHit {
        source_id,
        conversation_id,
        message_index_zero_based,
        score: hit.bm25_score,
        reader_key: reader_key.to_owned(),
    })
}
