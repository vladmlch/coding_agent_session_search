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

/// Map a lexical-page engine failure to the bridge's typed refusal kinds.
///
/// Two engine refusals mean "no complete ranking receipt" rather than a
/// partial answer: query-fuel exhaustion, and the per-leaf glob-expansion
/// cap (a prefix or wildcard literal matching more dictionary terms than
/// [`crate::search::quill_bridge::cass_quill_config`] allows). The glob
/// branch refuses typed — never a silently truncated match set — and its
/// message names the pattern the engine exploded (the engine diagnostic is
/// embedded verbatim, so the refusal carries the exact term) plus the
/// actionable remedy: a longer prefix before the wildcard.
fn lexical_page_refusal(error: anyhow::Error, query: &str) -> anyhow::Error {
    if coding_agent_search::search::quill_bridge::is_query_fuel_exhausted(&error) {
        refuse(
            "ranking_budget_exceeded",
            "Quill query fuel exhausted; no complete ranking receipt",
        )
    } else if coding_agent_search::search::quill_bridge::is_glob_expansion_limited(&error) {
        refuse(
            "ranking_budget_exceeded",
            format!(
                "Quill glob expansion exceeded the per-leaf dictionary cap for query \
                 {query:?} ({error}); no complete ranking receipt — lengthen the prefix \
                 before the wildcard and retry"
            ),
        )
    } else {
        error
    }
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
    search_paginated(reader, &query, limit, offset, true)
        .map_err(|error| lexical_page_refusal(error, text))
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
    search_paginated(reader, &query, limit, offset, true)
        .map_err(|error| lexical_page_refusal(error, text))
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
    search_paginated(reader, &query, limit, offset, true)
        .map_err(|error| lexical_page_refusal(error, text))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::viewer_bridge::protocol::Refusal;
    use coding_agent_search::search::quill_bridge::{
        QuillCassIndex, open_cass_reader_with_glob_expansion_limit,
    };
    use frankensearch::quill::cass::CassDocument;

    /// Build one published CASS index whose single canonical document
    /// carries nine distinct `docker`-prefixed tokens — enough to explode
    /// any glob cap below nine, and few enough that the default cap
    /// admits the same query.
    fn docker_prefixed_index(directory: &Path) -> anyhow::Result<()> {
        let content = (0..9_u32)
            .map(|ordinal| format!("docker{ordinal:02}"))
            .collect::<Vec<_>>()
            .join(" ");
        let mut writer = QuillCassIndex::open_or_create(directory)?;
        writer.add_cass_documents(&[CassDocument {
            agent: "claude_code".into(),
            workspace: Some("/deep-prefix".into()),
            workspace_original: None,
            source_path: "/sessions/deep-prefix.jsonl".into(),
            msg_idx: 0,
            created_at: Some(1_700_000_000_000),
            title: Some("docker prefix fixture".into()),
            content,
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: Some(1),
        }])?;
        writer.commit()?;
        Ok(())
    }

    /// An over-broad prefix is a TYPED refusal on every raw enumeration
    /// lane, never a silently truncated match set: the engine's
    /// glob-expansion cap surfaces as the bridge's
    /// `ranking_budget_exceeded` kind, the message names the exploding
    /// pattern (`docker*`) and prescribes the remedy (a longer prefix).
    /// The default-cap control proves the refusal is cap-driven, not
    /// data-driven: the same query over the same index answers normally.
    #[test]
    fn over_broad_prefix_refuses_typed_on_every_raw_lane() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        docker_prefixed_index(directory.path())?;
        let filters = CassQueryFilters::default();

        // Capped reader: every lane must refuse with the typed kind, the
        // exploded pattern, and the longer-prefix remedy.
        let capped = open_cass_reader_with_glob_expansion_limit(directory.path(), 8)?;
        let lanes = [
            ("region", raw_page(&capped, "docker", &filters, 0, 10)),
            (
                "session_docs",
                raw_page_session_docs(&capped, "docker", &filters, 0, 10),
            ),
            (
                "canonical",
                raw_page_canonical(&capped, "docker", &filters, 0, 10),
            ),
        ];
        for (lane, result) in lanes {
            let error = result.expect_err(&format!("{lane} must refuse the over-broad prefix"));
            let refusal = error
                .downcast_ref::<Refusal>()
                .unwrap_or_else(|| panic!("{lane} must refuse typed, got: {error:#}"));
            assert_eq!(
                refusal.kind, "ranking_budget_exceeded",
                "{lane}: the glob cap is a budget-shaped refusal"
            );
            let message = &refusal.message;
            assert!(
                message.contains("docker*"),
                "{lane}: the refusal must name the exploded pattern: {message}"
            );
            assert!(
                message.contains("lengthen the prefix"),
                "{lane}: the refusal must suggest a longer prefix: {message}"
            );
        }

        // Default-cap control: the same query, same index, default reader —
        // a complete receipt over the canonical lane, not a refusal.
        let fed = open_cass_reader(directory.path())?;
        let page = raw_page_canonical(&fed, "docker", &filters, 0, 10)?;
        assert_eq!(
            page.total_count,
            Some(1),
            "the default cap must afford the nine-term expansion"
        );
        Ok(())
    }
}
