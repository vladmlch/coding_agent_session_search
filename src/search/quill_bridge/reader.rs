use std::path::Path;

use anyhow::{Result, anyhow};
use frankensearch::quill::cass::CASS_MERGE_SEGMENT_THRESHOLD;
use frankensearch::quill::schema::CASS_SEMANTIC_SCHEMA;
use frankensearch::quill::{QuillConfig, QuillSearchIndex};

use super::{QUILL_INDEX_MARKER, cass_quill_config, drive};

/// Refresh a reader to the latest published snapshot.
///
/// The Tantivy incumbent called `IndexReader::reload`, which re-mmaps the
/// directory. A Quill reader is bound to the snapshot it opened, and `refresh`
/// rebinds it to the current publication; it reports whether the binding
/// actually moved.
///
/// # Errors
///
/// Returns an error when the current publication cannot be opened.
pub fn refresh_reader(reader: &QuillSearchIndex) -> Result<bool> {
    drive(|cx| async move { reader.refresh(&cx).await })
        .map_err(|error| anyhow!("refreshing the Quill CASS reader: {error}"))
}

/// Refresh a reader in place, or report that its directory was republished.
///
/// A lexical rebuild publishes by exchanging the whole index directory
/// (`publish_staged_lexical_index`: `cass index --full`, forget, dedup, agent
/// purge, the background repair). The path then holds a new index whose
/// MANIFEST generations restart, and read-only catch-up refuses it as a
/// generation regression, a reused generation, or a watermark rollback. A
/// long-lived reader would fail every later search. `Ok(None)` is an in-place
/// refresh; `Ok(Some(reason))` means the path must be opened again.
///
/// # Errors
///
/// Returns an error when the current publication cannot be read for any
/// other reason.
pub fn refresh_reader_or_detect_republish(reader: &QuillSearchIndex) -> Result<Option<String>> {
    use frankensearch::quill::{QuillIndexError, SnapshotError};
    match drive(|cx| async move { reader.refresh(&cx).await }) {
        Ok(_) => Ok(None),
        Err(QuillIndexError::Snapshot(
            error @ (SnapshotError::KeeperGenerationRegression { .. }
            | SnapshotError::KeeperGenerationCollision { .. }
            | SnapshotError::KeeperTransition { .. }),
        )) => Ok(Some(error.to_string())),
        Err(error) => Err(anyhow!("refreshing the Quill CASS reader: {error}")),
    }
}

/// One ranked hit from a Quill lexical search.
///
/// Replaces the incumbent's `LexicalDocHit`, whose `doc_address` was a Tantivy
/// `(segment_ord, segment_doc_id)` pair. Quill addresses a document by its
/// snapshot-global id, and additionally carries the external document id, so a
/// consumer that only needs identity never has to touch stored fields at all.
#[derive(Debug, Clone, PartialEq)]
pub struct QuillLexicalDocHit {
    /// BM25 relevance score.
    pub bm25_score: f32,
    /// Zero-based rank within the returned page.
    pub rank: usize,
    /// Snapshot-global document id, used to read stored columns back.
    pub global_docid: u32,
    /// External document identity, as minted by
    /// [`frankensearch::quill::cass::cass_document_identity`].
    ///
    /// Deliberately NOT documented as a literal format string: the shape is the
    /// engine's to define and it has already changed once (it was
    /// `"{source_id}#{msg_idx}"` until that proved non-unique — one `source_id`
    /// covers every locally discovered conversation, so message 0 of each
    /// collided). Treat it as an opaque identity; if you need the parts, read
    /// the stored columns rather than parsing this.
    pub document_id: String,
}

/// One page of Quill lexical results.
#[derive(Debug, Clone, PartialEq)]
pub struct QuillLexicalPage {
    /// Ranked hits for the requested page.
    pub hits: Vec<QuillLexicalDocHit>,
    /// Exact match count when it was requested and computed.
    pub total_count: Option<usize>,
    /// Live document count in the searched snapshot.
    pub doc_count: usize,
}

/// Execute one already-parsed query against `reader`.
///
/// `exact_count` is the caller's decision, not the engine's: computing an exact
/// total over a large index costs a full scan, so the caller decides when that
/// is worth paying for and this reports `None` otherwise.
///
/// # Errors
///
/// Returns an error when execution, scoring, or collection fails.
pub fn search_paginated(
    reader: &QuillSearchIndex,
    query: &frankensearch::quill::query::Query,
    limit: usize,
    offset: usize,
    exact_count: bool,
) -> Result<QuillLexicalPage> {
    let result = drive(|cx| async move {
        reader.search_preparsed_paginated(&cx, query, limit, offset, exact_count)
    })
    .map_err(|error| {
        // Preserve the engine type for recovery admission, while retaining the
        // full outer display for CLI callers that do not print the cause chain.
        let diagnostic = format!("executing a Quill lexical query: {error}");
        anyhow::Error::new(error).context(diagnostic)
    })?;
    Ok(QuillLexicalPage {
        hits: result
            .hits
            .iter()
            .enumerate()
            .map(|(rank, hit)| QuillLexicalDocHit {
                bm25_score: hit.score,
                rank,
                global_docid: hit.global_docid,
                document_id: hit.document_id.clone(),
            })
            .collect(),
        total_count: result
            .total_count
            .map(|count| usize::try_from(count).unwrap_or(usize::MAX)),
        doc_count: usize::try_from(result.doc_count).unwrap_or(usize::MAX),
    })
}

/// Canonical-message guard for persisted search-region documents.
///
/// Task 5 publishes `meta:*`/derived region documents into the SAME Quill
/// index as canonical message documents, in the synthetic `msg_idx` space
/// starting at `REGION_DOC_BASE`. They exist so session search can gate
/// region membership on index hits; chat-surface consumers (plain `cass
/// search`, viewer raw rank cursors) must never see them as ordinary
/// hits. A `MustNot` range over the indexed `msg_idx` field excludes them
/// inside the engine, so page boundaries and exact totals stay identical
/// to a region-free index — filtering a materialized page instead would
/// misreport `total_count` and could starve a page whose matches were
/// already charged to `limit` upstream.
#[must_use]
pub fn canonical_message_query(
    query: frankensearch::quill::query::Query,
) -> frankensearch::quill::query::Query {
    use frankensearch::quill::query::{BooleanClause, Occur, QueryValue};
    use std::ops::Bound;
    frankensearch::quill::query::Query::boolean(
        Vec::from([
            BooleanClause::new(Occur::Must, query),
            BooleanClause::new(
                Occur::MustNot,
                frankensearch::quill::query::Query::range(
                    frankensearch::quill::cass::field::MSG_IDX,
                    Bound::Included(QueryValue::U64(
                        crate::viewer_extensions::search_regions::REGION_DOC_BASE,
                    )),
                    Bound::Unbounded,
                ),
            ),
        ]),
        None,
    )
}

/// Region-document guard for the Stage 1 session×scope documents.
///
/// Stage 1 publishes one aggregated document per (conversation, scope)
/// chunk into the same synthetic `msg_idx` space, at slots ≥
/// [`SESSION_DOC_BASE`](crate::viewer_extensions::search_regions::SESSION_DOC_BASE) — strictly above every region-document slot. The
/// legacy per-region enumeration lane must never see them: they carry no
/// region coordinates, so they would surface as phantom hits that inflate
/// `total_count` toward the enumeration cap and crowd real region hits
/// out of a page. The same in-engine `MustNot` range shape as
/// [`canonical_message_query`] keeps page boundaries and exact totals
/// honest.
#[must_use]
pub fn region_documents_query(
    query: frankensearch::quill::query::Query,
) -> frankensearch::quill::query::Query {
    use frankensearch::quill::query::{BooleanClause, Occur, QueryValue};
    use std::ops::Bound;
    frankensearch::quill::query::Query::boolean(
        Vec::from([
            BooleanClause::new(Occur::Must, query),
            BooleanClause::new(
                Occur::MustNot,
                frankensearch::quill::query::Query::range(
                    frankensearch::quill::cass::field::MSG_IDX,
                    Bound::Included(QueryValue::U64(
                        crate::viewer_extensions::search_regions::SESSION_DOC_BASE,
                    )),
                    Bound::Unbounded,
                ),
            ),
        ]),
        None,
    )
}

/// Session×scope-document selector (Stage 1 fast lane): the mirror of
/// [`region_documents_query`], admitting ONLY the aggregated documents at
/// `msg_idx >= SESSION_DOC_BASE`. Membership on the fast lane is the set
/// of conversations whose session×scope chunks hit the literal; the
/// region documents and canonical messages stay out of the posting set.
#[must_use]
pub fn session_scope_documents_query(
    query: frankensearch::quill::query::Query,
) -> frankensearch::quill::query::Query {
    use frankensearch::quill::query::{BooleanClause, Occur, QueryValue};
    use std::ops::Bound;
    frankensearch::quill::query::Query::boolean(
        Vec::from([
            BooleanClause::new(Occur::Must, query),
            BooleanClause::new(
                Occur::Must,
                frankensearch::quill::query::Query::range(
                    frankensearch::quill::cass::field::MSG_IDX,
                    Bound::Included(QueryValue::U64(
                        crate::viewer_extensions::search_regions::SESSION_DOC_BASE,
                    )),
                    Bound::Unbounded,
                ),
            ),
        ]),
        None,
    )
}

/// Read one stored text column for a hit, if the column holds anything.
///
/// # Errors
///
/// Returns an error when the snapshot cannot be proven readable.
pub fn stored_text(
    reader: &QuillSearchIndex,
    field_ord: u16,
    global_docid: u32,
) -> Result<Option<String>> {
    let Some(bytes) = reader.stored_field_value(field_ord, global_docid)? else {
        return Ok(None);
    };
    // A stored text column holds its source UTF-8 bytes. Invalid UTF-8 here
    // would mean the column was written by something other than this schema's
    // ingest, so report it rather than lossily substituting replacement chars.
    Ok(Some(String::from_utf8(bytes).map_err(|error| {
        anyhow!("stored column {field_ord} for doc {global_docid} is not UTF-8: {error}")
    })?))
}

/// Read one stored numeric column as `i64`.
///
/// Scribe writes a stored numeric column as exactly eight little-endian bytes,
/// so anything else means the column was written by a different schema — that
/// is reported rather than silently truncated.
///
/// # Errors
///
/// Returns an error when the snapshot cannot be proven readable or the column
/// is not eight bytes wide.
pub fn stored_i64(
    reader: &QuillSearchIndex,
    field_ord: u16,
    global_docid: u32,
) -> Result<Option<i64>> {
    Ok(stored_numeric_bytes(reader, field_ord, global_docid)?.map(i64::from_le_bytes))
}

/// Read one stored numeric column as `u64`.
///
/// # Errors
///
/// Identical to [`stored_i64`].
pub fn stored_u64(
    reader: &QuillSearchIndex,
    field_ord: u16,
    global_docid: u32,
) -> Result<Option<u64>> {
    Ok(stored_numeric_bytes(reader, field_ord, global_docid)?.map(u64::from_le_bytes))
}

fn stored_numeric_bytes(
    reader: &QuillSearchIndex,
    field_ord: u16,
    global_docid: u32,
) -> Result<Option<[u8; 8]>> {
    let Some(bytes) = reader.stored_field_value(field_ord, global_docid)? else {
        return Ok(None);
    };
    let width = bytes.len();
    Ok(Some(<[u8; 8]>::try_from(bytes.as_slice()).map_err(
        |_| {
            anyhow!(
                "stored numeric column {field_ord} for doc {global_docid} is {width} bytes, not 8"
            )
        },
    )?))
}

/// Build a snippet generator for the content field from a query's terms.
///
/// The incumbent built its generator from a Tantivy query and rendered against
/// a retrieved `TantivyDocument`. Quill's generator is compiled from terms and
/// renders against source text, which suits this caller better: it already
/// hydrates content per hit, so no second document fetch is needed.
///
/// Term weights are derived from document frequency, and this supplies `1` for
/// every term rather than a real per-term count. Frequency only steers *which*
/// window is chosen among candidates — a uniform weight can pick a different
/// but still valid window, and never changes which terms are highlighted.
/// Passing `0` would silently drop the term entirely, so `1` is the correct
/// floor here.
#[must_use]
pub fn content_snippet_generator(
    terms: &[String],
    config: frankensearch::quill::SnippetConfig,
) -> frankensearch::quill::SnippetGenerator {
    use frankensearch::quill::{SnippetGenerator, SnippetTerm, schema::Analyzer};
    SnippetGenerator::new(
        Analyzer::CassHyphenNormalize,
        terms
            .iter()
            .filter(|term| !term.is_empty())
            .map(|term| SnippetTerm::new(term.clone(), 1)),
        config,
    )
}

/// Open a read-only CASS-schema reader on `path`.
///
/// The Tantivy incumbent took a `ReloadPolicy` here because a Tantivy reader
/// watches its directory and reloads. A Quill reader is bound to the published
/// snapshot it opened, so freshness is a matter of reopening rather than of
/// policy — there is no equivalent knob, and callers that want newer data open
/// again.
///
/// # Errors
///
/// Returns an error when the published snapshot cannot be opened.
pub fn open_cass_reader(path: &Path) -> Result<QuillSearchIndex> {
    #[cfg(test)]
    super::READER_OPEN_COUNT.with(|count| count.set(count.get() + 1));
    drive(|cx| {
        let path = path.to_path_buf();
        async move {
            QuillSearchIndex::open_with_schema(&cx, path, CASS_SEMANTIC_SCHEMA, cass_quill_config())
                .await
        }
    })
    .map_err(|error| anyhow!("opening Quill CASS reader at {}: {error}", path.display()))
}

/// [`open_cass_reader`] with its own query-fuel ceiling, for tests that need a
/// query to run out of fuel without a process-wide environment variable.
#[cfg(test)]
pub fn open_cass_reader_with_query_fuel_budget(
    path: &Path,
    query_fuel_budget: u64,
) -> Result<QuillSearchIndex> {
    drive(|cx| {
        let path = path.to_path_buf();
        async move {
            QuillSearchIndex::open_with_schema(
                &cx,
                path,
                CASS_SEMANTIC_SCHEMA,
                QuillConfig {
                    query_fuel_budget,
                    ..cass_quill_config()
                },
            )
            .await
        }
    })
    .map_err(|error| anyhow!("opening Quill CASS reader at {}: {error}", path.display()))
}

/// [`open_cass_reader`] with its own per-leaf glob-expansion cap, for
/// callers (the viewer bridge's tests, which live in the bin crate) that
/// need a prefix literal to explode against the dictionary without a
/// process-wide environment variable.
pub fn open_cass_reader_with_glob_expansion_limit(
    path: &Path,
    glob_expansion_limit: usize,
) -> Result<QuillSearchIndex> {
    drive(|cx| {
        let path = path.to_path_buf();
        async move {
            QuillSearchIndex::open_with_schema(
                &cx,
                path,
                CASS_SEMANTIC_SCHEMA,
                QuillConfig {
                    glob_expansion_limit,
                    ..cass_quill_config()
                },
            )
            .await
        }
    })
    .map_err(|error| anyhow!("opening Quill CASS reader at {}: {error}", path.display()))
}

/// File name pattern of one published Quill segment inside an index directory.
pub(super) const QUILL_SEGMENT_FILE_PREFIX: &str = "seg-";
pub(super) const QUILL_SEGMENT_FILE_SUFFIX: &str = ".fslx";

/// Live segment count of a published Quill index, from the engine's own
/// reader — the number a query actually pays for. Costs an engine open, so
/// it belongs on surfaces that may spend (`doctor`, tests), not on
/// `status`/`health`, which report [`segment_file_count`] instead. Folded
/// inputs stay on disk for a while after a merge, so the two can differ.
#[must_use]
pub fn live_segment_count(path: &Path) -> Option<usize> {
    if !path.join(QUILL_INDEX_MARKER).is_file() {
        return None;
    }
    open_cass_reader(path)
        .ok()
        .and_then(|reader| reader.segment_count().ok())
}

/// Segment-file count above which `doctor` reports segment pressure (#441).
/// Eight times the bounded-merge threshold: the post-run maintenance pass
/// folds runs above the threshold, so a generation this far past it either
/// predates the maintenance pass (a v0.7.1 archive) or is not being folded,
/// and one `cass index --full` consolidates it.
pub const CASS_SEGMENT_PRESSURE_FILES: usize = 8 * CASS_MERGE_SEGMENT_THRESHOLD;

/// Number of segment files in a Quill index directory, from directory
/// metadata alone — no engine open, no manifest parse.
///
/// This is the observation-surface cousin of [`super::QuillCassIndex::segment_count`]
/// (#441, WS-B.1a): `status`/`health` must never open the engine, but an
/// operator still needs to see an archive that has fragmented into hundreds
/// of segments. Files on disk can briefly exceed the manifest's live segment
/// count (a merge publishes before the folded inputs are removed), so this is
/// a truthful upper bound labeled as such, not the live count. Returns `None`
/// when the directory is not a Quill index.
#[must_use]
pub fn segment_file_count(path: &Path) -> Option<usize> {
    if !path.join(QUILL_INDEX_MARKER).is_file() {
        return None;
    }
    let entries = std::fs::read_dir(path).ok()?;
    Some(
        entries
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
            .filter(|entry| {
                entry.file_name().to_str().is_some_and(|name| {
                    name.starts_with(QUILL_SEGMENT_FILE_PREFIX)
                        && name.ends_with(QUILL_SEGMENT_FILE_SUFFIX)
                })
            })
            .count(),
    )
}

/// Live-document accounting for a published Quill generation, read from the
/// engine's MANIFEST alone (GH #457).
///
/// Every segment record in a Quill MANIFEST carries its live-at-seal
/// `doc_count` and the generation's tombstone set for that segment, so the
/// number of documents a query can actually return is
/// `Σ (doc_count − tombstones)`. No segment file is opened, mapped or hashed,
/// which keeps this cheap enough for `status`, `health` and
/// `search --robot-meta`, none of which may spend an engine open.
///
/// This is the count a hollow generation cannot hide behind: the cass
/// generation manifest and the rebuild checkpoint both record what a rebuild
/// *published*, while this reads what the engine currently *serves*.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuillManifestLiveDocs {
    /// Documents that survive the generation's tombstones.
    pub live_docs: u64,
    /// Documents sealed into the referenced segments before any tombstoning.
    pub sealed_docs: u64,
    /// Tombstoned documents across every referenced segment.
    pub tombstones: u64,
    /// Segments the MANIFEST references (the count a query pays for).
    pub segments: usize,
    /// Monotone publication generation of the MANIFEST that was read.
    pub generation: u64,
    /// Which durable slot the counts came from.
    pub source: QuillManifestSource,
}

/// The durable MANIFEST slot a read served: the engine's read-only two-slot
/// recovery (`frankensearch::quill::ManifestSource`) as the readiness surfaces
/// name it. Ordered by severity, so a federated bundle reports its worst shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum QuillManifestSource {
    /// `MANIFEST` is valid: the newest publish is served.
    Current,
    /// `MANIFEST` is absent, as in a publish's crash window between its two
    /// renames, so `MANIFEST.prev` is served.
    PreviousAfterMissingCurrent,
    /// `MANIFEST` exists but fails validation, so the one-publish-older
    /// generation in `MANIFEST.prev` is served. cass builds Quill without
    /// durability sidecars, so nothing on disk reconstructs the newest
    /// publish: a writer open only retires the corrupt slot, and a rebuild
    /// from the canonical database is the repair.
    PreviousAfterCorruptCurrent,
}

impl QuillManifestSource {
    /// Stable robot-surface spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::PreviousAfterMissingCurrent => "previous_after_missing_current",
            Self::PreviousAfterCorruptCurrent => "previous_after_corrupt_current",
        }
    }

    fn from_engine(source: frankensearch::quill::ManifestSource) -> Self {
        use frankensearch::quill::ManifestSource;
        match source {
            // `load_manifest_pair` reads a directory and never reports the
            // owned-buffer genesis snapshot, which is current by definition.
            ManifestSource::Current | ManifestSource::InMemory => Self::Current,
            ManifestSource::PreviousAfterMissingCurrent => Self::PreviousAfterMissingCurrent,
            ManifestSource::PreviousAfterCorruptCurrent => Self::PreviousAfterCorruptCurrent,
        }
    }
}

/// Read [`QuillManifestLiveDocs`] for the Quill index at `path`.
///
/// `None` when the directory holds no decodable MANIFEST slot (an absent or
/// corrupt generation is reported by the readiness surfaces on their own).
#[must_use]
pub fn manifest_live_doc_count(path: &Path) -> Option<QuillManifestLiveDocs> {
    let loaded = frankensearch::quill::load_manifest_pair(path).ok()?;
    let manifest = &loaded.manifest;
    let mut live_docs = 0_u64;
    let mut sealed_docs = 0_u64;
    let mut tombstones = 0_u64;
    for segment in &manifest.segments {
        let sealed = u64::from(segment.doc_count);
        let live = u64::from(segment.live_doc_count());
        sealed_docs = sealed_docs.saturating_add(sealed);
        live_docs = live_docs.saturating_add(live);
        tombstones = tombstones.saturating_add(sealed.saturating_sub(live));
    }
    Some(QuillManifestLiveDocs {
        live_docs,
        sealed_docs,
        tombstones,
        segments: manifest.segments.len(),
        generation: manifest.generation,
        source: QuillManifestSource::from_engine(loaded.source),
    })
}

/// On-disk footprint of one Quill index directory, split by what a full
/// rebuild has to reproduce versus what the engine reclaims on its own (#453).
///
/// A concat merge publishes its output and drops the folded inputs from the
/// MANIFEST, but the input files stay on disk until the engine's writer-open
/// garbage sweep has seen them unreferenced by both durable slots for a full
/// grace period (`frankensearch_quill::DEFAULT_GARBAGE_GRACE`). Those files
/// are not part of the index a rebuild rewrites, so the headroom preflight
/// must not double them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QuillDirectoryFootprint {
    /// Bytes of the segment files the current MANIFEST references, plus every
    /// other regular file in the directory (MANIFEST slots, lock record,
    /// repair sidecars, staged temporaries).
    pub live_bytes: u64,
    /// Bytes of `seg-*.fslx` files the current MANIFEST no longer references
    /// (merge-folded inputs awaiting the engine sweep) and of their
    /// `.retired` receipts.
    pub retired_bytes: u64,
    /// Number of unreferenced `seg-*.fslx` files behind `retired_bytes`.
    pub retired_segment_files: usize,
}

/// Canonical file name of a Quill segment, as the engine writes it.
fn canonical_segment_file_name(segment_id: u64) -> String {
    format!("{QUILL_SEGMENT_FILE_PREFIX}{segment_id:016x}{QUILL_SEGMENT_FILE_SUFFIX}")
}

/// Suffix of the engine's per-segment retirement receipt (`seg-<id>.fslx.retired`).
pub(super) const QUILL_RETIREMENT_RECEIPT_SUFFIX: &str = ".retired";

/// Split one Quill index directory's regular files into live and retired
/// bytes by reading the published MANIFEST (no engine open, no segment I/O).
///
/// Returns `None` when `path` is not a Quill index or its MANIFEST cannot be
/// read, so a caller sizing storage falls back to counting everything as
/// live: an unreadable manifest is a reason to be conservative, never to
/// call bytes reclaimable. Only direct children are inspected; a Quill index
/// directory is flat.
#[must_use]
pub fn quill_directory_footprint(path: &Path) -> Option<QuillDirectoryFootprint> {
    if !path.join(QUILL_INDEX_MARKER).is_file() {
        return None;
    }
    let loaded = frankensearch::quill::load_manifest_pair(path).ok()?;
    let live_segments: std::collections::HashSet<String> = loaded
        .manifest
        .segments
        .iter()
        .map(|segment| canonical_segment_file_name(segment.segment_id))
        .collect();
    let mut footprint = QuillDirectoryFootprint::default();
    for entry in std::fs::read_dir(path).ok()?.filter_map(Result::ok) {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if !metadata.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            footprint.live_bytes = footprint.live_bytes.saturating_add(metadata.len());
            continue;
        };
        let is_segment = name.starts_with(QUILL_SEGMENT_FILE_PREFIX)
            && name.ends_with(QUILL_SEGMENT_FILE_SUFFIX);
        let is_receipt = name
            .strip_suffix(QUILL_RETIREMENT_RECEIPT_SUFFIX)
            .is_some_and(|base| {
                base.starts_with(QUILL_SEGMENT_FILE_PREFIX)
                    && base.ends_with(QUILL_SEGMENT_FILE_SUFFIX)
            });
        if is_segment && !live_segments.contains(name) {
            footprint.retired_bytes = footprint.retired_bytes.saturating_add(metadata.len());
            footprint.retired_segment_files += 1;
        } else if is_receipt {
            footprint.retired_bytes = footprint.retired_bytes.saturating_add(metadata.len());
        } else {
            footprint.live_bytes = footprint.live_bytes.saturating_add(metadata.len());
        }
    }
    Some(footprint)
}
