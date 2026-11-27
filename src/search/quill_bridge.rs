//! Synchronous facade over the async Quill lexical engine.
//!
//! CASS's lexical layer is synchronous because Tantivy is synchronous. Quill is
//! `asupersync`-async. Rather than colour 54k lines of indexer call sites with
//! `async fn`, this module drives each Quill future to completion on the calling
//! thread — the same bridge pattern [`crate::franken_sync`] uses for its
//! synchronous SQLite facade, for the same reason.
//!
//! The runtime lives in a thread-local slot and is *taken out* while a future is
//! being driven, so a reentrant bridge call finds the slot empty and builds a
//! fresh runtime instead of re-entering `block_on` on the same instance.
//! `Runtime::block_on` has no `Send` bound and saves/restores the ambient
//! runtime handle, so nesting inside a consumer's own `block_on` is safe.
//!
//! Every future is created, polled, and dropped entirely within one bridge
//! call, so engine state never crosses a thread boundary between poll steps.

use std::cell::RefCell;
use std::future::Future;
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};
use asupersync::runtime::{Runtime, RuntimeBuilder};
use frankensearch::Cx;
use frankensearch::quill::cass::{
    CASS_MERGE_COOLDOWN_MS, CASS_MERGE_SEGMENT_THRESHOLD, CassDocument as QuillCassDocument,
    CassMergeStatus,
};
use frankensearch::quill::schema::CASS_SEMANTIC_SCHEMA;
use frankensearch::quill::{
    QuillConfig, QuillIndex, QuillIndexError, QuillSearchIndex, SchemaDocument, TermDictionaryError,
};

#[path = "quill_bridge/fuel_errors.rs"]
mod fuel_errors;
mod liveness;
mod merge_plan;
#[cfg(test)]
mod merge_tests;
mod reader;
#[cfg(test)]
mod tests;

use liveness::EngineLivenessProbe;
use merge_plan::{
    MAX_BOUNDED_MERGE_PASSES, SegmentFoldProfile, fresh_segment_id,
    plan_capped_balanced_merge_runs, plan_capped_merge_runs,
};

pub use liveness::EngineHeartbeat;
pub(crate) use liveness::run_with_liveness_ticks;
pub use merge_plan::{
    CASS_LEXICAL_MERGE_MAX_OUTPUT_BYTES_DEFAULT, CASS_LEXICAL_MERGE_MAX_OUTPUT_BYTES_ENV,
    lexical_merge_max_output_bytes,
};
#[cfg(test)]
pub(crate) use reader::open_cass_reader_with_query_fuel_budget;
pub use reader::{
    CASS_SEGMENT_PRESSURE_FILES, QuillDirectoryFootprint, QuillLexicalDocHit, QuillLexicalPage,
    QuillManifestLiveDocs, QuillManifestSource, canonical_message_query, content_snippet_generator,
    live_segment_count, manifest_live_doc_count, open_cass_reader,
    open_cass_reader_with_glob_expansion_limit, quill_directory_footprint, refresh_reader,
    refresh_reader_or_detect_republish, region_documents_query, search_paginated,
    segment_file_count, session_scope_documents_query, stored_i64, stored_text, stored_u64,
};

/// Filename that marks a directory as a published Quill index.
///
/// Verified empirically rather than assumed: a freshly created CASS index
/// contains exactly `MANIFEST` and `LOCK`. The manifest is the published
/// authority, so its presence is what makes a directory readable.
pub const QUILL_INDEX_MARKER: &str = "MANIFEST";

/// Operator override for Quill's deterministic per-query work budget
/// (`QuillConfig::query_fuel_budget`, default 10,000,000 units). The budget is
/// what turns a pathological query into a fast typed refusal instead of an
/// unbounded scan, so raising it is a diagnostic/escape hatch, not a tuning
/// knob; the durable fix for fuel exhaustion is a compacted index (#441).
pub const CASS_QUILL_QUERY_FUEL_BUDGET_ENV: &str = "CASS_QUILL_QUERY_FUEL_BUDGET";

/// Whether `error` is a Quill query-fuel refusal, anywhere in its cause chain.
/// Concrete engine errors take precedence over diagnostic text. Historical
/// flattened bridge errors are accepted only in the engine's diagnostic format,
/// not because a query, context or unrelated failure mentions fuel exhaustion.
#[must_use]
pub fn is_query_fuel_exhausted(error: &anyhow::Error) -> bool {
    fuel_errors::is_exhausted(error)
}

/// Whether `error` is Quill's posting-cursor invariant refusal
/// (`ArgusError::CursorInvariant`), anywhere in its context chain. Unlike a
/// busy or cancelled query, this failure belongs to the published
/// generation: the same query over the same segments fails the same way every
/// time (GH #499: a date-filtered Boolean over a sealed segment that holds
/// tombstones), so callers must not report it as retryable.
#[must_use]
pub fn is_engine_invariant_failure(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .to_string()
            .contains("posting cursor invariant failed")
    })
}

/// Whether `error` is Quill's typed per-leaf glob-expansion refusal
/// (`QuillIndexError::Dictionary(TermDictionaryError::GlobExpansionLimitExceeded)`),
/// anywhere in its cause chain. The engine expands a glob query against each
/// sealed leaf's own term dictionary under [`QuillConfig::glob_expansion_limit`]
/// (default 16,384 matching terms per leaf); a prefix that crosses the cap
/// refuses typed instead of silently under-matching, and the engine's own
/// diagnostic names the pattern that exploded. Callers surface it as a typed
/// refusal naming the term and suggesting a longer prefix — never a silent
/// drop and never a truncated match set.
#[must_use]
pub fn is_glob_expansion_limited(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let Some(engine) = cause.downcast_ref::<QuillIndexError>() else {
            return false;
        };
        matches!(
            engine,
            QuillIndexError::Dictionary(TermDictionaryError::GlobExpansionLimitExceeded { .. })
        )
    })
}

/// Engine configuration every CASS reader and writer opens with.
///
/// Two deliberate departures from `QuillConfig::default()`:
///
/// * `max_visibility_lag_ms` is effectively infinite. Quill's default (1 s)
///   is a cross-process visibility contract: once unpublished changes are a
///   second old, the *next ingest call* seals every shard and publishes a new
///   MANIFEST on its own. CASS never relies on that — every reader opens the
///   snapshot CASS itself publishes through an explicit `commit()`, and the
///   rebuild checkpoint (`.lexical-rebuild-state.json`) is written around that
///   same commit. Letting the engine publish underneath the checkpoint is
///   exactly what #440 reported: an interrupted `--force-rebuild` left a
///   staging MANIFEST several conversations ahead of the durable cursor, so
///   the resumed run re-inserted already-live identities and Quill refused
///   the duplicate. It is also why #441's archives grew hundreds of tiny
///   segments: a seal per shard per second, each in its own sparse docid
///   lease, which the hole-ratio-gated tier merge can never fold. With
///   explicit publication only, a segment is sealed on the accumulation
///   budget or at commit, and the MANIFEST moves only when CASS says so.
/// * `tier_fanout` is effectively infinite, which switches off the engine's
///   own tier merge. That merge runs inside every commit and folds any
///   `tier_fanout` adjacent same-tier segments, and its top tier has no upper
///   bound: eight "large" segments are each wider than the medium boundary
///   (524,288 docids), so their fold spans more than 4,194,304 documents —
///   the per-term posting limit every Quill reader and concat merge enforces
///   (`merge_plan::MAX_FOLD_OUTPUT_DOCS`). On a multi-million-message archive a common
///   token is in more documents than that, so the rebuild's final commit
///   failed with "posting validation limit 4194304 exceeded" (GH #498). The
///   same unbounded fold also bypassed the #456 merge-memory cap. CASS's own
///   planners (`plan_capped_merge_runs`, `plan_capped_balanced_merge_runs`)
///   already own merging after incremental runs and at the end of a rebuild,
///   under both the byte cap and the document bound.
/// * `query_fuel_budget` honours [`CASS_QUILL_QUERY_FUEL_BUDGET_ENV`].
///
/// Everything else keeps the engine default so the on-disk format and the
/// admission semantics stay exactly what the pinned engine documents.
#[must_use]
pub fn cass_quill_config() -> QuillConfig {
    let mut config = QuillConfig {
        max_visibility_lag_ms: u64::MAX,
        tier_fanout: usize::MAX,
        ..QuillConfig::default()
    };
    if let Some(budget) = query_fuel_budget_override(
        dotenvy::var(CASS_QUILL_QUERY_FUEL_BUDGET_ENV)
            .ok()
            .as_deref(),
    ) {
        config.query_fuel_budget = budget;
    }
    config
}

/// Parse the fuel-budget override. Zero, garbage, and absent all mean "keep
/// the engine default" — a zero budget would refuse every query, which is
/// never what an operator setting this variable wants.
fn query_fuel_budget_override(raw: Option<&str>) -> Option<u64> {
    raw.and_then(|value| value.trim().replace('_', "").parse::<u64>().ok())
        .filter(|budget| *budget > 0)
}

thread_local! {
    static DRIVER: RefCell<Option<Runtime>> = const { RefCell::new(None) };
}

#[cfg(test)]
thread_local! {
    static READER_OPEN_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reader_open_count() -> u64 {
    READER_OPEN_COUNT.with(std::cell::Cell::get)
}

pub(crate) fn shutdown_driver() -> bool {
    DRIVER
        .with(|slot| slot.borrow_mut().take())
        .is_none_or(|runtime| runtime.shutdown_timeout(std::time::Duration::from_secs(30)))
}

/// Drive one Quill future to completion on the calling thread.
///
/// The closure receives a per-call [`Cx`]. A fresh `Cx` per bridge call is
/// deliberate: a `Cx` carries cancellation and deadline state scoped to one
/// request, and reusing one across independent engine calls would let an
/// earlier cancellation silently poison a later unrelated call.
fn drive<T, F>(call: impl FnOnce(Cx) -> F) -> T
where
    F: Future<Output = T>,
{
    let runtime = DRIVER
        .with(|slot| slot.borrow_mut().take())
        .unwrap_or_else(|| {
            RuntimeBuilder::current_thread()
                .build()
                .expect("failed to build Quill sync-bridge runtime")
        });
    // Restore the runtime on the way out even if the driven future panics.
    //
    // The slot is TAKEN for the duration of the call so a reentrant bridge call
    // finds it empty and builds its own runtime instead of re-entering
    // `block_on` on this one. That take must be paired with a put-back on every
    // exit path: with a plain sequential restore, a panic escaping `block_on`
    // would unwind past it and leave the slot permanently empty, so every later
    // call on this thread would build a fresh runtime. Not a correctness bug —
    // a fresh runtime is always valid — but it silently converts a cached
    // runtime into a per-call allocation for the rest of the thread's life.
    struct RestoreOnDrop(Option<Runtime>);
    impl Drop for RestoreOnDrop {
        fn drop(&mut self) {
            if let Some(runtime) = self.0.take() {
                DRIVER.with(|slot| {
                    let mut slot = slot.borrow_mut();
                    // Only reclaim the slot if it is still empty: a reentrant
                    // call may have built and parked its own runtime while this
                    // one was driving, and clobbering it would drop a live
                    // runtime that an outer frame is still using.
                    if slot.is_none() {
                        *slot = Some(runtime);
                    }
                });
            }
        }
    }

    // The guard OWNS the runtime across the call, so an unwind runs its Drop
    // with the runtime still in hand. Setting the guard after `block_on`
    // returns would be pointless: a panic unwinds before that assignment and
    // drops the runtime instead of parking it.
    let guard = RestoreOnDrop(Some(runtime));
    let output = guard
        .0
        .as_ref()
        .expect("sync-bridge runtime is present for the duration of the call")
        .block_on(async {
            let cx = Cx::for_request();
            call(cx).await
        });
    drop(guard);
    output
}

/// Field handles for the compiled CASS schema.
///
/// The Tantivy incumbent resolved these from a runtime schema read, because a
/// Tantivy `Field` is an opaque handle minted when the schema is built. Quill
/// field ordinals are fixed by the compiled `CASS_SEMANTIC_SCHEMA`, so this is
/// a constant table rather than a lookup — there is no failure mode where a
/// field is missing, and `conversation_id` is no longer `Option` because the
/// compiled schema always carries it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuillCassFields {
    pub agent: u16,
    pub workspace: u16,
    pub workspace_original: u16,
    pub source_path: u16,
    pub msg_idx: u16,
    pub created_at: u16,
    pub title: u16,
    pub content: u16,
    pub title_prefix: u16,
    pub content_prefix: u16,
    pub preview: u16,
    pub source_id: u16,
    pub origin_kind: u16,
    pub origin_host: u16,
    pub conversation_id: u16,
}

impl QuillCassFields {
    /// The pinned ordinals of the compiled CASS schema.
    #[must_use]
    pub const fn compiled() -> Self {
        use frankensearch::quill::cass::field;
        Self {
            agent: field::AGENT,
            workspace: field::WORKSPACE,
            workspace_original: field::WORKSPACE_ORIGINAL,
            source_path: field::SOURCE_PATH,
            msg_idx: field::MSG_IDX,
            created_at: field::CREATED_AT,
            title: field::TITLE,
            content: field::CONTENT,
            title_prefix: field::TITLE_PREFIX,
            content_prefix: field::CONTENT_PREFIX,
            preview: field::PREVIEW,
            source_id: field::SOURCE_ID,
            origin_kind: field::ORIGIN_KIND,
            origin_host: field::ORIGIN_HOST,
            conversation_id: field::CONVERSATION_ID,
        }
    }
}

impl Default for QuillCassFields {
    fn default() -> Self {
        Self::compiled()
    }
}

/// A Quill-backed CASS lexical index with a synchronous API.
///
/// Mirrors the surface `CassTantivyIndex` exposed so the calling layer keeps its
/// existing shape across the engine swap.
pub struct QuillCassIndex {
    index: QuillIndex,
    directory: PathBuf,
    /// Epoch milliseconds of the last compaction, 0 when never compacted.
    last_merge_ts: i64,
    /// GH #446: liveness sink for the stall watchdog. `None` outside an
    /// indexing run (searches, tests, tools) — no thread, no ticks.
    liveness: Option<EngineLivenessProbe>,
}

impl QuillCassIndex {
    /// Open an existing CASS-schema index, creating it when absent.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be created or the index
    /// cannot be opened under the CASS schema.
    pub fn open_or_create(path: &Path) -> Result<Self> {
        Self::open_or_create_with_config(path, &cass_quill_config())
    }

    /// [`Self::open_or_create`] under an explicit engine configuration; tests
    /// use it to isolate one knob against the engine's own behaviour.
    fn open_or_create_with_config(path: &Path, config: &QuillConfig) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        let directory = path.to_path_buf();
        // Create ONLY when the directory holds no published manifest.
        //
        // The obvious spelling — try `open_with_schema`, fall back to
        // `create_with_schema` on any error — is wrong: it converts every open
        // failure into "make a new index", so a corrupt manifest, a permission
        // error, or a schema mismatch would all silently produce an empty index
        // in place of the real one. Deciding on the marker's presence keeps
        // "absent" separate from "broken", and lets a genuine open failure
        // propagate.
        //
        // `create_with_schema` is create-or-open-compatible, so two processes
        // racing on a fresh directory both end up with the same index rather
        // than one failing.
        let index_exists = path.join(QUILL_INDEX_MARKER).exists();
        let index = drive(|cx| {
            let directory = directory.clone();
            let config = config.clone();
            async move {
                if index_exists {
                    QuillIndex::open_with_schema(&cx, directory, CASS_SEMANTIC_SCHEMA, config).await
                } else {
                    QuillIndex::create_with_schema(&cx, directory, CASS_SEMANTIC_SCHEMA, config)
                        .await
                }
            }
        })
        .map_err(|error| {
            anyhow!(
                "{} Quill CASS index at {}: {error}",
                if index_exists { "opening" } else { "creating" },
                path.display()
            )
        })?;
        let mut index = Self {
            index,
            directory: path.to_path_buf(),
            last_merge_ts: 0,
            liveness: None,
        };
        // A freshly created index has a writer but no published manifest, so
        // nothing on disk yet announces it as an index and no reader can open
        // it. Publish the empty snapshot immediately: a caller that indexes
        // zero documents (an empty corpus, or a rebuild that finds nothing)
        // must still leave behind a readable, contract-valid index rather than
        // a directory that later reads as "no index here".
        if !index_exists {
            index.commit()?;
        }
        Ok(index)
    }

    /// GH #446: install (or, with `None`, remove) the liveness callback the
    /// sink invokes from real engine work — see `EngineLivenessProbe`.
    /// The indexer passes `IndexingProgress::tick_activity`.
    pub fn set_heartbeat(&mut self, heartbeat: Option<EngineHeartbeat>) {
        self.liveness = heartbeat
            .map(|heartbeat| EngineLivenessProbe::spawn(vec![self.directory.clone()], heartbeat));
    }

    fn tick_heartbeat(&self) {
        if let Some(liveness) = &self.liveness {
            (liveness.heartbeat)();
        }
    }

    /// Run one opaque engine call (commit / merge / compact / accumulate)
    /// with the liveness sampler armed for its duration.
    fn with_engine_liveness<T>(&self, op: impl FnOnce() -> T) -> T {
        match &self.liveness {
            Some(liveness) => liveness.during(op),
            None => op(),
        }
    }

    /// Index one batch of CASS documents.
    ///
    /// # Errors
    ///
    /// Returns an error when admission or accumulation refuses the batch.
    pub fn add_cass_documents(&mut self, documents: &[QuillCassDocument]) -> Result<()> {
        if documents.is_empty() {
            return Ok(());
        }
        let projected: Vec<SchemaDocument> = documents
            .iter()
            .map(QuillCassDocument::to_schema_document)
            .collect();
        // Accumulation can seal a segment on budget, so it is sampled like a
        // commit; the completed batch then ticks synchronously.
        self.with_engine_liveness(|| {
            drive(|cx| {
                let projected = &projected;
                let index = &self.index;
                async move { index.index_schema_documents(&cx, projected).await }
            })
        })
        .map_err(|error| {
            // Preserve the engine cause for retry classification without changing
            // the operator-facing diagnostic.
            let diagnostic = format!("indexing CASS documents into Quill: {error}");
            anyhow::Error::new(error).context(diagnostic)
        })?;
        self.tick_heartbeat();
        Ok(())
    }

    /// Upsert one batch of CASS documents under their stable identities
    /// (source id + source path + canonical conversation id + message idx).
    ///
    /// Unlike [`Self::add_cass_documents`], a document whose identity is
    /// already live REPLACES it instead of appending a duplicate — the
    /// primitive the qhiv2 / gh#382 targeted partial-prefix reconcile needs:
    /// retrying the same source set converges to exactly one live document
    /// per identity.
    ///
    /// # Errors
    ///
    /// Returns an error when admission, identity resolution, or accumulation
    /// refuses the batch.
    pub fn upsert_cass_documents(&mut self, documents: &[QuillCassDocument]) -> Result<()> {
        if documents.is_empty() {
            return Ok(());
        }
        let projected: Vec<SchemaDocument> = documents
            .iter()
            .map(QuillCassDocument::to_schema_document)
            .collect();
        self.with_engine_liveness(|| {
            drive(|cx| {
                let projected = &projected;
                let index = &self.index;
                async move { index.upsert_schema_documents(&cx, projected).await }
            })
        })
        .map_err(|error| {
            let diagnostic = format!("upserting CASS documents into Quill: {error}");
            anyhow::Error::new(error).context(diagnostic)
        })?;
        self.tick_heartbeat();
        Ok(())
    }

    /// Remove a bounded batch of stable CASS identities. Quill requires a
    /// committed writer for deletion, so publish any pending plain additions
    /// first. The removal publishes its own successor; these two publications
    /// are not one atomic whole-conversation replacement.
    pub fn delete_cass_document_ids(&mut self, document_ids: &[String]) -> Result<()> {
        if document_ids.is_empty() {
            return Ok(());
        }
        self.commit()?;
        let ids: Vec<&str> = document_ids.iter().map(String::as_str).collect();
        self.with_engine_liveness(|| {
            drive(|cx| {
                let index = &self.index;
                let ids = &ids;
                async move { index.delete_documents(&cx, ids).await }
            })
        })
        .map_err(|error| anyhow!("removing revised CASS documents from Quill: {error}"))?;
        self.tick_heartbeat();
        Ok(())
    }

    /// Publish everything staged since the last commit.
    ///
    /// # Errors
    ///
    /// Returns an error when publication fails.
    pub fn commit(&mut self) -> Result<()> {
        self.with_engine_liveness(|| {
            drive(|cx| {
                let index = &self.index;
                async move { index.commit(&cx).await }
            })
        })
        .map(|_| ())
        .map_err(|error| anyhow!("committing the Quill CASS index: {error}"))?;
        self.tick_heartbeat();
        Ok(())
    }

    /// Delete every live document and publish the empty successor.
    ///
    /// # Errors
    ///
    /// Returns an error when the successor cannot be published.
    pub fn delete_all(&mut self) -> Result<()> {
        drive(|cx| {
            let index = &self.index;
            async move { index.delete_all(&cx).await }
        })
        .map_err(|error| anyhow!("clearing the Quill CASS index: {error}"))
    }

    /// Open a read handle on the published snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the published snapshot cannot be opened.
    pub fn reader(&self) -> Result<QuillSearchIndex> {
        #[cfg(test)]
        READER_OPEN_COUNT.with(|count| count.set(count.get() + 1));
        let reader = self
            .with_engine_liveness(|| {
                drive(|cx| {
                    let directory = self.directory.clone();
                    async move {
                        QuillSearchIndex::open_with_schema(
                            &cx,
                            directory,
                            CASS_SEMANTIC_SCHEMA,
                            cass_quill_config(),
                        )
                        .await
                    }
                })
            })
            .map_err(|error| anyhow!("opening the Quill CASS reader: {error}"))?;
        self.tick_heartbeat();
        Ok(reader)
    }

    /// Live document count in the published snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when publication authority cannot prove the snapshot
    /// readable.
    pub fn doc_count(&self) -> Result<u64> {
        // The writer verified its published snapshot when it opened or
        // committed it. A new reader would hash every segment again, which
        // is a full read of the lexical index per call. Reopen only when the
        // engine says publication may have moved past this writer's view.
        match self.index.doc_count() {
            Ok(count) => Ok(count),
            Err(error) => {
                tracing::debug!(
                    error = %error,
                    path = %self.directory.display(),
                    "writer snapshot cannot vouch for the published count; reopening a reader"
                );
                Ok(self.reader()?.doc_count()?)
            }
        }
    }

    /// Durable directory backing this index.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.directory
    }

    /// Whether compaction is currently wanted, and why.
    #[must_use]
    pub fn merge_status(&self, segment_count: usize, now_ms: i64) -> CassMergeStatus {
        CassMergeStatus {
            segment_count,
            last_merge_ts: self.last_merge_ts,
            ms_since_last_merge: if self.last_merge_ts > 0 {
                now_ms - self.last_merge_ts
            } else {
                -1
            },
            merge_threshold: CASS_MERGE_SEGMENT_THRESHOLD,
            cooldown_ms: CASS_MERGE_COOLDOWN_MS,
        }
    }

    /// Record that a compaction completed at `now_ms`.
    pub fn note_merged(&mut self, now_ms: i64) {
        self.last_merge_ts = now_ms;
    }

    /// Index one batch of borrowed CASS documents.
    ///
    /// Quill's ingest owns its column values, so the borrowed form is
    /// materialized here. This keeps the caller's streaming shape — it never
    /// has to clone message bodies just to hold a batch — while paying the
    /// copy once, at the engine boundary.
    ///
    /// # Errors
    ///
    /// Returns an error when admission or accumulation refuses the batch.
    pub fn add_cass_document_refs(
        &mut self,
        documents: &[frankensearch::quill::cass::CassDocumentRef<'_>],
    ) -> Result<()> {
        if documents.is_empty() {
            return Ok(());
        }
        let owned: Vec<QuillCassDocument> = documents
            .iter()
            .map(|document| QuillCassDocument {
                agent: document.agent.to_owned(),
                workspace: document.workspace.map(str::to_owned),
                workspace_original: document.workspace_original.map(str::to_owned),
                source_path: document.source_path.to_owned(),
                msg_idx: document.msg_idx,
                created_at: document.created_at,
                title: document.title.map(str::to_owned),
                content: document.content.to_owned(),
                source_id: document.source_id.to_owned(),
                origin_kind: document.origin_kind.to_owned(),
                origin_host: document.origin_host.map(str::to_owned),
                conversation_id: document.conversation_id,
            })
            .collect();
        self.add_cass_documents(&owned)
    }

    /// Upsert one batch of borrowed CASS documents under their stable
    /// identities (see [`Self::upsert_cass_documents`]).
    ///
    /// # Errors
    ///
    /// Returns an error when admission, identity resolution, or accumulation
    /// refuses the batch.
    pub fn upsert_cass_document_refs(
        &mut self,
        documents: &[frankensearch::quill::cass::CassDocumentRef<'_>],
    ) -> Result<()> {
        if documents.is_empty() {
            return Ok(());
        }
        let owned: Vec<QuillCassDocument> = documents
            .iter()
            .map(|document| QuillCassDocument {
                agent: document.agent.to_owned(),
                workspace: document.workspace.map(str::to_owned),
                workspace_original: document.workspace_original.map(str::to_owned),
                source_path: document.source_path.to_owned(),
                msg_idx: document.msg_idx,
                created_at: document.created_at,
                title: document.title.map(str::to_owned),
                content: document.content.to_owned(),
                source_id: document.source_id.to_owned(),
                origin_kind: document.origin_kind.to_owned(),
                origin_host: document.origin_host.map(str::to_owned),
                conversation_id: document.conversation_id,
            })
            .collect();
        self.upsert_cass_documents(&owned)
    }

    /// Live segment count in the published snapshot.
    ///
    /// Returns 0 when nothing is published yet, which is what the caller's
    /// merge policy treats as "nothing to merge".
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.reader()
            .ok()
            .and_then(|reader| reader.segment_count().ok())
            .unwrap_or(0)
    }

    /// Compact when the segment count and cooldown say it is worth doing.
    ///
    /// Runs the bounded policy in `plan_capped_balanced_merge_runs`: a
    /// balanced run of adjacent *small* segments is folded into one, so an
    /// append-only archive keeps large segments plus geometric tail tiers
    /// instead of accumulating a segment per session (#441) or rewriting the
    /// same large tail after each tiny append (#479). Comparable adjacent
    /// pairs may also fold; every nonempty output grows its largest input by
    /// at least half its live-document count.
    /// Tombstone-density compaction runs afterwards so replaced/deleted rows
    /// are reclaimed on the same cadence.
    ///
    /// Returns whether any merge or compaction actually ran.
    ///
    /// # Errors
    ///
    /// Returns an error when a merge or compaction itself fails.
    pub fn optimize_if_idle(&mut self, now_ms: i64) -> Result<bool> {
        let segments = self.segment_count();
        if !self.merge_status(segments, now_ms).should_merge() {
            return Ok(false);
        }
        let merged = self.merge_small_segment_runs()?;
        let compacted = self.compact_tombstones()?;
        if merged || compacted {
            self.note_merged(now_ms);
        }
        Ok(merged || compacted)
    }

    /// At most one bounded merge, for use while a streaming ingest is running.
    ///
    /// Every concat merge is a publish, and every publish re-verifies each
    /// live segment's bytes, so [`Self::optimize_if_idle`] — which folds every
    /// planned run and then compacts — costs one full verification per run.
    /// Run mid-ingest on a multi-gigabyte generation, that loop held the
    /// consumer for more than ten minutes. This folds only the planned run
    /// that removes the most segments (normally the tail the run's own
    /// commits left), under the same threshold and cooldown, and leaves the
    /// rest to the post-run fold.
    ///
    /// Returns whether a merge ran.
    ///
    /// # Errors
    ///
    /// Returns an error when the merge itself fails.
    pub fn fold_largest_small_run(&mut self, now_ms: i64) -> Result<bool> {
        let segments = self.segment_count();
        if !self.merge_status(segments, now_ms).should_merge() {
            return Ok(false);
        }
        let profile = self.published_segment_profile()?;
        let fold_profile = self.published_segment_fold_profile()?;
        let Some(run) = plan_capped_balanced_merge_runs(
            &profile,
            &fold_profile,
            lexical_merge_max_output_bytes(),
        )
        .into_iter()
        .max_by_key(Vec::len) else {
            return Ok(false);
        };
        self.concat_merge_run(&run)?;
        self.note_merged(now_ms);
        Ok(true)
    }

    /// Fold the published segments into as few as the merge-output byte cap
    /// allows, regardless of the idle policy, then reclaim tombstones.
    ///
    /// This is the end-of-rebuild step: a from-scratch build seals one segment
    /// per ingest shard per accumulation budget, and the query planner's
    /// per-segment dictionary probes make segment count the dominant query
    /// cost (#441), so a freshly published generation should start small.
    ///
    /// It used to fold everything into ONE segment. The engine assembles a
    /// concat-merge output as a single in-memory buffer sized to the finished
    /// file and re-verifies it in place before writing, so that fold held the
    /// entire lexical generation in anonymous memory at once — the
    /// publish-tail RSS spike of GH #456 (36 GB on a 1.1M-message archive),
    /// and the 6.75 GB single segment of GH #453. The runs are now planned
    /// under [`lexical_merge_max_output_bytes`], which bounds the peak merge
    /// allocation by the cap and the generation by roughly
    /// `index_bytes / cap` segments — a handful, not the hundreds #441 was
    /// about. Each run also holds at most `merge_plan::MAX_FOLD_OUTPUT_DOCS` documents,
    /// so a multi-million-message archive folds into several segments rather
    /// than one whose common-token posting lists no reader could open (#498).
    ///
    /// # Errors
    ///
    /// Returns an error when the merge or compaction fails. Requires a fully
    /// committed index (call [`Self::commit`] first).
    pub fn force_merge(&mut self) -> Result<()> {
        self.force_merge_with_output_cap(lexical_merge_max_output_bytes())
    }

    /// [`Self::force_merge`] with an explicit merge-output byte cap.
    ///
    /// # Errors
    ///
    /// Returns an error when the merge or compaction fails.
    pub fn force_merge_with_output_cap(&mut self, max_output_bytes: u64) -> Result<()> {
        let runs =
            plan_capped_merge_runs(&self.published_segment_fold_profile()?, max_output_bytes);
        for run in runs {
            self.concat_merge_run(&run)?;
        }
        self.compact_tombstones()?;
        Ok(())
    }

    /// The fold profile of every published segment, in manifest (docid)
    /// order: what the merge-output cap plans against.
    fn published_segment_fold_profile(&self) -> Result<Vec<SegmentFoldProfile>> {
        let snapshot = self
            .index
            .snapshot()
            .map_err(|error| anyhow!("reading the Quill CASS manifest: {error}"))?;
        Ok(snapshot
            .segments()
            .iter()
            .map(|segment| {
                let manifest = segment.manifest();
                SegmentFoldProfile {
                    segment_id: manifest.segment_id,
                    file_len: manifest.file_len,
                    docid_lo: manifest.docid_lo,
                    docid_hi: manifest.docid_hi,
                    doc_count: u64::from(manifest.doc_count),
                }
            })
            .collect())
    }

    /// `(segment_id, live_doc_count)` for every published segment, in manifest
    /// (docid) order — the order [`QuillIndex::concat_merge`] requires a source
    /// run to be consecutive in.
    fn published_segment_profile(&self) -> Result<Vec<(u64, u64)>> {
        let snapshot = self
            .index
            .snapshot()
            .map_err(|error| anyhow!("reading the Quill CASS manifest: {error}"))?;
        Ok(snapshot
            .segments()
            .iter()
            .map(|segment| {
                (
                    segment.manifest().segment_id,
                    u64::from(segment.manifest().live_doc_count()),
                )
            })
            .collect())
    }

    /// Apply [`plan_capped_balanced_merge_runs`] until it has nothing left to fold.
    /// Returns whether at least one merge ran.
    ///
    /// GH #456: the document-tiered run is split under the merge-output byte
    /// cap before it is folded, for the same reason as [`Self::force_merge`]:
    /// a fragmented generation whose segments are all "small" by document
    /// share is one run covering the whole index, and folding it at once
    /// materialises the whole index in memory during an ordinary
    /// `cass index`. A pass that can fold nothing under the cap ends the loop.
    fn merge_small_segment_runs(&mut self) -> Result<bool> {
        let max_output_bytes = lexical_merge_max_output_bytes();
        let mut merged_any = false;
        // Each productive iteration strictly reduces the segment count, so
        // this loop is bounded by the initial count; the cap is a
        // belt-and-braces guard.
        for _ in 0..MAX_BOUNDED_MERGE_PASSES {
            let profile = self.published_segment_profile()?;
            let fold_profile = self.published_segment_fold_profile()?;
            let sub_runs =
                plan_capped_balanced_merge_runs(&profile, &fold_profile, max_output_bytes);
            if sub_runs.is_empty() {
                break;
            }
            for sub_run in sub_runs {
                self.concat_merge_run(&sub_run)?;
            }
            merged_any = true;
        }
        Ok(merged_any)
    }

    /// Q1-preserving concat merge of one consecutive manifest run into a
    /// fresh segment. The output id is random and collision-checked against
    /// the live manifest, matching the engine's own id discipline.
    fn concat_merge_run(&mut self, source_segment_ids: &[u64]) -> Result<()> {
        let output_segment_id = fresh_segment_id(&self.published_segment_profile()?);
        let created_unix_s = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX))
            .unwrap_or(0);
        self.with_engine_liveness(|| {
            drive(|cx| {
                let index = &self.index;
                async move {
                    index
                        .concat_merge(&cx, source_segment_ids, output_segment_id, created_unix_s)
                        .await
                }
            })
        })
        .map(|_| ())
        .map_err(|error| {
            anyhow!(
                "concat-merging {} Quill CASS segments: {error}",
                source_segment_ids.len()
            )
        })?;
        // One merged run is one unit of completed work.
        self.tick_heartbeat();
        Ok(())
    }

    /// Density-driven tombstone compaction; a no-op on an append-only index.
    fn compact_tombstones(&mut self) -> Result<bool> {
        let changed = self
            .with_engine_liveness(|| {
                drive(|cx| {
                    let index = &self.index;
                    async move {
                        index
                            .compact(&cx, frankensearch::quill::CompactionPolicy::default())
                            .await
                    }
                })
            })
            .map(|report| report.changed())
            .map_err(|error| anyhow!("compacting the Quill CASS index: {error}"))?;
        self.tick_heartbeat();
        Ok(changed)
    }

    /// Bulk-load merge policy hook.
    ///
    /// Tantivy needed its merge policy relaxed during a bulk load so the writer
    /// did not merge continuously while ingesting. Quill seals segments on
    /// budget and lease boundaries and compacts only when asked, so there is no
    /// equivalent knob and nothing to relax. Retained as a no-op so the
    /// caller's bulk-load sequence keeps its shape.
    pub const fn configure_bulk_load_merge_policy(&self) {}
}
