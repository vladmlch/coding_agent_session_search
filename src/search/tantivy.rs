use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::connectors::NormalizedConversation;
use crate::model::conversation_packet::{ConversationPacket, ConversationPacketProvenance};
use crate::sources::provenance::LOCAL_SOURCE_ID;
use anyhow::{Context, Result};
// CASS->Quill flip: index open/create, ingest, read, and compaction are all on
// Quill now. What remains from the incumbent is the schema-generation sentinel
// vocabulary plus the Tantivy-format helpers the federated *assembly* path
// still uses to read legacy shard metadata.
// The schema-generation sentinel now comes from Quill: it names `v9-quill`,
// which puts the new index in a SIBLING directory of the Tantivy-era `v8` and
// makes the rebuild automatic rather than a corrupt read of foreign segments.
use frankensearch::lexical_tantivy::{
    Index, Schema, cass_build_schema as fs_build_schema,
    cass_ensure_tokenizer as fs_ensure_tokenizer,
};
use frankensearch::quill::cass::{
    CASS_SCHEMA_HASH, CASS_SCHEMA_VERSION, cass_index_dir as fs_index_dir, cass_schema_hash_matches,
};

mod federated;
#[cfg(test)]
mod federated_tests;
mod ingest;
mod legacy_merge;
#[cfg(test)]
mod tests;

use federated::{
    materialize_federated_search_bundle_for_write, wipe_index_dir_on_schema_hash_mismatch,
    write_root_schema_hash_file,
};

pub(crate) use federated::{
    OpenedLexicalIndex, federated_search_manifest_path, open_validated_lexical_index,
};
pub use federated::{
    SearchableIndexSummary, SearchableLiveDocs, federated_search_evidence_bundle_manifest,
    lexical_search_evidence_bundle_manifest, open_federated_search_readers,
    publish_federated_searchable_index_directories,
    publish_federated_searchable_index_directories_with_summaries, searchable_index_exists,
    searchable_index_fingerprint, searchable_index_live_doc_count, searchable_index_live_docs,
    searchable_index_modified_time, searchable_index_summary, validate_searchable_index_contract,
    validated_searchable_index_summary, write_federated_search_evidence_bundle_manifest,
    write_lexical_search_evidence_bundle_manifest,
};

pub(crate) fn normalized_index_source_id(
    source_id: Option<&str>,
    origin_kind: Option<&str>,
    origin_host: Option<&str>,
) -> String {
    let trimmed_source_id = source_id.unwrap_or_default().trim();
    if !trimmed_source_id.is_empty() {
        if trimmed_source_id.eq_ignore_ascii_case(LOCAL_SOURCE_ID) {
            return LOCAL_SOURCE_ID.to_string();
        }
        return trimmed_source_id.to_string();
    }

    let trimmed_origin_host = origin_host.map(str::trim).filter(|value| !value.is_empty());
    let trimmed_origin_kind = origin_kind.unwrap_or_default().trim();
    if trimmed_origin_kind.eq_ignore_ascii_case("ssh")
        || trimmed_origin_kind.eq_ignore_ascii_case("remote")
    {
        return trimmed_origin_host.unwrap_or("remote").to_string();
    }
    if let Some(origin_host) = trimmed_origin_host {
        return origin_host.to_string();
    }

    LOCAL_SOURCE_ID.to_string()
}

pub(crate) fn normalized_index_origin_kind(source_id: &str, origin_kind: Option<&str>) -> String {
    if let Some(kind) = origin_kind.map(str::trim).filter(|value| !value.is_empty()) {
        if kind.eq_ignore_ascii_case("local") {
            return LOCAL_SOURCE_ID.to_string();
        }
        if kind.eq_ignore_ascii_case("ssh") || kind.eq_ignore_ascii_case("remote") {
            return "remote".to_string();
        }
        return kind.to_ascii_lowercase();
    }

    if source_id == LOCAL_SOURCE_ID {
        LOCAL_SOURCE_ID.to_string()
    } else {
        "remote".to_string()
    }
}

pub(crate) fn normalized_index_origin_host(origin_host: Option<&str>) -> Option<String> {
    origin_host
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub const SCHEMA_HASH: &str = CASS_SCHEMA_HASH;
const ENV_TANTIVY_ADD_BATCH_MAX_CHARS: &str = "CASS_TANTIVY_ADD_BATCH_MAX_CHARS";
const ENV_TANTIVY_ADD_BATCH_MAX_MESSAGES: &str = "CASS_TANTIVY_ADD_BATCH_MAX_MESSAGES";
const ENV_TANTIVY_MAX_WRITER_THREADS: &str = "CASS_TANTIVY_MAX_WRITER_THREADS";
const ENV_TANTIVY_REBUILD_STAGED_SHARD_BUILDERS: &str =
    "CASS_TANTIVY_REBUILD_STAGED_SHARD_BUILDERS";
const DEFAULT_TANTIVY_MAX_WRITER_THREADS_CEILING: usize = 26;
const DEFAULT_TANTIVY_ASSUMED_CONCURRENT_WRITERS: u64 = 8;
const DEFAULT_TANTIVY_WRITER_HEAP_PER_THREAD_BYTES: u64 = 128 * 1024 * 1024;
const DEFAULT_TANTIVY_WRITER_HEAP_RAM_FRACTION: u64 = 10;

fn positive_usize_env(name: &str) -> Option<usize> {
    dotenvy::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
}

#[cfg(target_os = "linux")]
fn host_memory_bytes_for_tantivy_default() -> Option<u64> {
    // GH #496: MemAvailable clamped to this process's cgroup headroom, so a
    // memory-capped unit does not size its writer heap from the whole host.
    crate::indexer::responsiveness::available_memory_bytes()
}

#[cfg(target_os = "macos")]
fn host_memory_bytes_for_tantivy_default() -> Option<u64> {
    let output = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    stdout.trim().parse::<u64>().ok()
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn host_memory_bytes_for_tantivy_default() -> Option<u64> {
    None
}

#[cfg(test)]
pub(crate) fn default_tantivy_max_writer_threads_for_memory_bytes(
    memory_bytes: Option<u64>,
) -> usize {
    default_tantivy_max_writer_threads_for_memory_bytes_and_concurrent_writers(
        memory_bytes,
        DEFAULT_TANTIVY_ASSUMED_CONCURRENT_WRITERS,
    )
}

pub(crate) fn default_tantivy_max_writer_threads_for_memory_bytes_and_concurrent_writers(
    memory_bytes: Option<u64>,
    concurrent_writers: u64,
) -> usize {
    let Some(memory_bytes) = memory_bytes else {
        return DEFAULT_TANTIVY_MAX_WRITER_THREADS_CEILING;
    };
    let per_thread_peak =
        DEFAULT_TANTIVY_WRITER_HEAP_PER_THREAD_BYTES.saturating_mul(concurrent_writers.max(1));
    if per_thread_peak == 0 {
        return DEFAULT_TANTIVY_MAX_WRITER_THREADS_CEILING;
    }
    let budget = memory_bytes / DEFAULT_TANTIVY_WRITER_HEAP_RAM_FRACTION;
    let threads = budget / per_thread_peak;
    usize::try_from(threads)
        .unwrap_or(usize::MAX)
        .clamp(1, DEFAULT_TANTIVY_MAX_WRITER_THREADS_CEILING)
}

fn default_tantivy_assumed_concurrent_writers() -> u64 {
    positive_usize_env(ENV_TANTIVY_REBUILD_STAGED_SHARD_BUILDERS)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(DEFAULT_TANTIVY_ASSUMED_CONCURRENT_WRITERS)
        .max(1)
}

pub fn default_tantivy_max_writer_threads() -> usize {
    default_tantivy_max_writer_threads_for_memory_bytes_and_concurrent_writers(
        host_memory_bytes_for_tantivy_default(),
        default_tantivy_assumed_concurrent_writers(),
    )
}

pub(crate) fn tantivy_writer_parallelism_hint_for_available(available_parallelism: usize) -> usize {
    let max_threads = positive_usize_env(ENV_TANTIVY_MAX_WRITER_THREADS)
        .unwrap_or_else(default_tantivy_max_writer_threads);

    available_parallelism.max(1).clamp(1, max_threads)
}

/// Governor-aware variant of `tantivy_writer_parallelism_hint_for_available`.
/// Returns the same value on an idle host but scales down when the machine
/// responsiveness governor has shrunk the global capacity. Call this from
/// production code paths; the ungoverned `_for_available` variant is kept so
/// formula-only unit tests stay deterministic.
pub(crate) fn tantivy_writer_parallelism_hint_for_available_governed(
    available_parallelism: usize,
) -> usize {
    let raw = tantivy_writer_parallelism_hint_for_available(available_parallelism);
    crate::indexer::responsiveness::effective_worker_count(raw).max(1)
}

pub(crate) fn tantivy_writer_parallelism_hint() -> usize {
    tantivy_writer_parallelism_hint_for_available_governed(
        std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1),
    )
}

fn tantivy_add_batch_max_messages() -> usize {
    positive_usize_env(ENV_TANTIVY_ADD_BATCH_MAX_MESSAGES)
        .unwrap_or_else(|| 4_096.max(tantivy_writer_parallelism_hint().saturating_mul(512)))
}

fn tantivy_add_batch_max_chars() -> usize {
    positive_usize_env(ENV_TANTIVY_ADD_BATCH_MAX_CHARS).unwrap_or_else(|| {
        (16 * 1024 * 1024).max(tantivy_writer_parallelism_hint().saturating_mul(2 * 1024 * 1024))
    })
}

fn tantivy_prebuilt_add_batch_max_messages() -> usize {
    positive_usize_env(ENV_TANTIVY_ADD_BATCH_MAX_MESSAGES)
        .unwrap_or_else(|| 16_384.max(tantivy_writer_parallelism_hint().saturating_mul(512)))
}

/// Returns true if the given stored hash matches the current schema hash.
pub fn schema_hash_matches(stored: &str) -> bool {
    cass_schema_hash_matches(stored)
}

pub type Fields = crate::search::quill_bridge::QuillCassFields;
pub type MergeStatus = frankensearch::quill::cass::CassMergeStatus;

/// Epoch milliseconds, saturating rather than panicking on a pre-epoch clock.
fn now_unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
        })
}

/// Field handles for the compiled CASS schema.
fn cass_field_handles() -> Fields {
    crate::search::quill_bridge::QuillCassFields::compiled()
}

pub struct TantivyIndex {
    /// Quill-backed lexical index, driven synchronously through
    /// [`crate::search::quill_bridge`].
    inner: crate::search::quill_bridge::QuillCassIndex,
    pub fields: Fields,
    /// #440: documents still owed to the identity-idempotent (upsert) path
    /// because a resumed rebuild found the published authority ahead of its
    /// durable cursor. `add_prebuilt_document_refs_slice` routes this many
    /// leading documents through upsert, then reverts to plain adds.
    resume_reconcile_docs_remaining: usize,
    /// Task 5 two-phase region-doc publication: marks-row snapshots taken
    /// by [`Self::stage_search_region_docs`] while the documents were
    /// staged on this uncommitted writer. [`Self::commit_region_doc_stamps`]
    /// replays them as freshness-guarded `published` stamps AFTER a
    /// successful commit — the marks row can never certify documents a
    /// failed commit or crash never shipped. [`Self::commit`] clears the
    /// queue on failure so a later commit cannot stamp marks for
    /// documents the lost commit may have dropped.
    pending_region_doc_stamps: Vec<crate::viewer_extensions::search_regions::RegionDocStamp>,
    /// Sessions whose region documents were already staged on this
    /// uncommitted writer. Their marks still read `pending` (the stamp
    /// lands only after commit), so the end-of-batch pending sweep would
    /// otherwise re-stage them inside the same generation — a second
    /// upsert of the same doc identity before publication is a duplicate
    /// live id Quill refuses. Cleared exactly when the stamp queue is.
    staged_region_doc_sessions: HashSet<i64>,
    /// Whole-archive republish deferral (a REGION_VERSION bump): the
    /// non-watch incremental lane latched this writer so its per-batch
    /// region-doc prelude stages NOTHING — every upsert a re-projected
    /// session issues against the committed snapshot replaces live
    /// identities, publishes a successor segment and re-validates every
    /// existing segment, so per-batch convergence is quadratic in the
    /// session count (hours on a 6k-session archive). The lane drains the
    /// accumulated pending set with ONE bounded
    /// [`Self::converge_pending_search_region_docs`] pass right before
    /// its final commit. Marks stay honestly `pending` until then; a
    /// crash mid-run leaves exactly the state the pending sweep
    /// re-converges on the next run. Never latched by watch or the
    /// rebuild lanes.
    region_doc_convergence_deferred: bool,
    /// Task 8 coverage floor: the `viewer_revisions` snapshot captured when
    /// this writer began staging the work its next commit will seal. The
    /// post-commit [`certify_lexical_publication`] receipt binds AT MOST
    /// these revisions — anything committed after the floor is
    /// not-in-this-generation work the next publication must pick up.
    /// Captured lazily by every db-carrying staging entry and by
    /// [`Self::commit_with_region_doc_stamps`] pre-commit; cleared when a
    /// commit certifies or fails.
    publication_floor: Option<crate::viewer_extensions::changes::JournalRevisions>,
}

impl TantivyIndex {
    pub(crate) fn path(&self) -> &Path {
        self.inner.path()
    }

    pub fn open_or_create(path: &Path) -> Result<Self> {
        materialize_federated_search_bundle_for_write(path)?;
        wipe_index_dir_on_schema_hash_mismatch(path)?;
        let inner = crate::search::quill_bridge::QuillCassIndex::open_or_create(path)?;
        // The rebuild sentinel is cass's, not the engine's: it is what makes a
        // schema-generation change trigger an automatic reindex instead of a
        // corrupt read. The Tantivy backend wrote it inside its own
        // open_or_create; with Quill owning the index, cass writes it here.
        write_root_schema_hash_file(path)?;
        // The compiled CASS schema is fixed, so field handles no longer come
        // from a runtime schema read; they are the pinned ordinals.
        let fields = cass_field_handles();
        Ok(Self {
            inner,
            fields,
            resume_reconcile_docs_remaining: 0,
            pending_region_doc_stamps: Vec::new(),
            staged_region_doc_sessions: HashSet::new(),
            region_doc_convergence_deferred: false,
            publication_floor: None,
        })
    }

    pub fn open_or_create_with_writer_parallelism(
        path: &Path,
        writer_parallelism: usize,
    ) -> Result<Self> {
        materialize_federated_search_bundle_for_write(path)?;
        // Same mismatch guard as `open_or_create`. Without it this constructor
        // would open a stale-schema index and then `write_root_schema_hash_file`
        // would STAMP it as current — turning a detectable mismatch into a
        // silent one, which is strictly worse than not checking at all.
        wipe_index_dir_on_schema_hash_mismatch(path)?;
        // Quill does not take a per-index writer-thread count: the CASS schema
        // is wider than the shipping five-field shape, so its ingest takes the
        // scalar route by construction and there are no writer threads to
        // size. The hint is accepted so callers keep their shape.
        let _ = writer_parallelism;
        let inner = crate::search::quill_bridge::QuillCassIndex::open_or_create(path)?;
        write_root_schema_hash_file(path)?;
        let fields = cass_field_handles();
        Ok(Self {
            inner,
            fields,
            resume_reconcile_docs_remaining: 0,
            pending_region_doc_stamps: Vec::new(),
            staged_region_doc_sessions: HashSet::new(),
            region_doc_convergence_deferred: false,
            publication_floor: None,
        })
    }

    /// Task 8: capture this writer's coverage floor — the committed
    /// `viewer_revisions` snapshot the next [`certify_lexical_publication`]
    /// receipt will bind as the revisions this generation covers. First
    /// capture wins: staging more work later does NOT move the floor
    /// forward, so a certification never claims revisions committed after
    /// this writer began the generation it is about to seal. Callers
    /// staging canonical documents through db-less lanes should invoke
    /// this before they begin for a tight floor; every db-carrying staging
    /// entry captures it lazily.
    pub fn capture_publication_floor(
        &mut self,
        db: &impl crate::viewer_extensions::facts::FactsDb,
    ) -> Result<()> {
        if self.publication_floor.is_none() {
            self.publication_floor =
                crate::viewer_extensions::changes::current_journal_revisions(db)?;
        }
        Ok(())
    }

    /// Arm the coverage floor with a snapshot the caller captured earlier.
    /// The authoritative rebuild reads its floor at BUILD START — before
    /// the streaming loop stages a single document and before this writer
    /// exists — so a canonical commit landing mid-build lands ABOVE the
    /// floor and the eventual publication receipt can never claim
    /// revisions the generation does not contain. First floor wins, like
    /// [`Self::capture_publication_floor`]: the lazy stage-time captures
    /// must not move an armed floor forward.
    pub fn arm_publication_floor(
        &mut self,
        floor: Option<crate::viewer_extensions::changes::JournalRevisions>,
    ) {
        if self.publication_floor.is_none() {
            self.publication_floor = floor;
        }
    }

    /// Disarm and return the captured coverage floor without certifying —
    /// for the authoritative rebuild, which drops this writer before the
    /// candidate is validated and (for a staged build) swapped live, then
    /// binds the floor to the PUBLISHED manifest via
    /// [`Self::certify_published_manifest`].
    pub fn take_publication_floor(
        &mut self,
    ) -> Option<crate::viewer_extensions::changes::JournalRevisions> {
        self.publication_floor.take()
    }

    pub fn add_conversation(&mut self, conv: &NormalizedConversation) -> Result<()> {
        // ibuuh.32 migration: route the in-tree convenience entrypoint
        // through the packet-driven pipeline so the lexical sink stops
        // re-deriving doc context from the raw NormalizedConversation
        // separately. The legacy `add_messages_with_conversation_id`
        // path remains for indexer/mod.rs callers until that file's
        // exclusive lock is released and they can migrate too.
        let provenance = ConversationPacketProvenance::local();
        let packet = ConversationPacket::from_normalized_conversation(conv, provenance);
        self.add_messages_from_packet(&packet, None, None, |_| Ok(()))
    }

    pub fn add_conversation_with_id(
        &mut self,
        conv: &NormalizedConversation,
        conversation_id: Option<i64>,
    ) -> Result<()> {
        let provenance = ConversationPacketProvenance::local();
        let mut packet = ConversationPacket::from_normalized_conversation(conv, provenance);
        // Stamp the canonical id onto the packet identity so the lexical
        // doc carries the same conversation_id the legacy path emitted.
        packet.payload.identity.conversation_id = conversation_id;
        self.add_messages_from_packet(&packet, None, conversation_id, |_| Ok(()))
    }

    pub fn delete_all(&mut self) -> Result<()> {
        self.inner.delete_all()
    }

    /// Delete specific canonical/region documents by their stable Cass
    /// identities, publishing a successor generation. Used by the indexer
    /// to drop the derived documents of retired conversations (e.g. the
    /// stale per-segment Codex rows a merged thread supersedes) without a
    /// whole-archive rebuild. Requires a committed writer, like the
    /// region-stamp stale-identity deletes.
    pub fn delete_cass_document_ids(&mut self, document_ids: &[String]) -> Result<()> {
        if document_ids.is_empty() {
            return Ok(());
        }
        self.inner.delete_cass_document_ids(document_ids)
    }

    pub fn commit(&mut self) -> Result<()> {
        match self.inner.commit() {
            Ok(()) => Ok(()),
            Err(error) => {
                // A failed publish can drop staged documents; queued
                // region-doc stamps must never certify a marks row for
                // documents a later commit did not carry. Clearing keeps
                // the sessions honestly pending — the next converge pass
                // re-materializes and re-stamps them. The Task-8 coverage
                // floor clears with them: it described the staging the
                // lost commit would have sealed, and must not certify a
                // later generation.
                self.pending_region_doc_stamps.clear();
                self.staged_region_doc_sessions.clear();
                self.publication_floor = None;
                Err(error)
            }
        }
    }

    /// Commit, then stamp the region documents this commit sealed
    /// `published` (`docs_state='published'` marks rows). A commit
    /// failure propagates with the stamps dropped — see [`Self::commit`].
    /// The stamp itself is a freshness-guarded compare-and-swap, so a
    /// marks row that changed since staging is refused rather than
    /// clobbered.
    ///
    /// Task 8: before the commit, capture this writer's coverage floor if
    /// staging never did (a db-less staging lane leaves it unset — the
    /// pre-commit revisions are the tightest honest bound still provable
    /// here). After the commit seals the manifest, the floor certifies
    /// the publication inside [`Self::commit_region_doc_stamps`].
    pub fn commit_with_region_doc_stamps(
        &mut self,
        db: &impl crate::viewer_extensions::facts::FactsDb,
    ) -> Result<()> {
        self.capture_publication_floor(db)?;
        self.commit()?;
        self.commit_region_doc_stamps(db)?;
        Ok(())
    }

    /// Stamp every session whose region documents were staged on this
    /// writer `published` — meaningful only after a successful commit.
    /// Each stamp replays its stage-time marks snapshot
    /// (`region_version`/`source_revision`/`docs_state`/
    /// `pending_doc_deletes`), so a concurrent republish or meta-edit is
    /// left pending instead of being overwritten. Returns the number of
    /// sessions stamped (fewer than staged when the guard lost a race).
    ///
    /// Queued stale region identities are retired here, after the commit
    /// that sealed their replacements: `delete_cass_document_ids` needs a
    /// committed writer (it publishes its own successor generation), so
    /// running it during staging would have committed the whole writer —
    /// canonical batch included — ahead of the real boundary. A delete
    /// error propagates WITHOUT stamping, leaving `docs_state`/`pending_
    /// doc_deletes` honestly pending for the next converge pass.
    ///
    /// Task 8: once the marks stamps land, the publication is certified
    /// against the durable MANIFEST this writer just sealed — the ONLY
    /// lane that ever mints `lexical_generation` or journals `index_ready`.
    /// A certification failure (a contended archive, a missing manifest
    /// slot) logs and keeps the captured coverage floor so the next
    /// stamp-aware commit certifies the still-wider coverage honestly —
    /// never a fabricated or dropped readiness claim.
    pub fn commit_region_doc_stamps(
        &mut self,
        db: &impl crate::viewer_extensions::facts::FactsDb,
    ) -> Result<usize> {
        // Task 8: a db-less staging lane reaches this commit with the
        // floor still unset — capture it at stamp time rather than let
        // `certify_lexical_publication` bind the revisions at certify
        // time, the broadest possible coverage claim.
        self.capture_publication_floor(db)?;
        let stamped = self.replay_region_doc_stamps(db)?;
        self.certify_published_generation(db);
        Ok(stamped)
    }

    /// The stamp half of [`Self::commit_region_doc_stamps`] WITHOUT the
    /// post-commit certification — for the authoritative lexical rebuild,
    /// whose candidate still folds and re-commits after the stamps land,
    /// so the generation sealed at stamp time is an intermediate manifest
    /// the terminal commit supersedes. The captured coverage floor stays
    /// armed for [`Self::certify_sealed_publication`] once that terminal
    /// commit lands. Every single-commit lane must keep calling
    /// [`Self::commit_region_doc_stamps`]: skipping certification entirely
    /// would publish a generation readiness can never explain.
    pub fn replay_region_doc_stamps(
        &mut self,
        db: &impl crate::viewer_extensions::facts::FactsDb,
    ) -> Result<usize> {
        let stamps = std::mem::take(&mut self.pending_region_doc_stamps);
        self.staged_region_doc_sessions.clear();
        let mut stale_ids: Vec<String> = Vec::new();
        for stamp in &stamps {
            let ids: Vec<String> =
                serde_json::from_str(&stamp.pending_doc_deletes).unwrap_or_default();
            if ids.is_empty() {
                continue;
            }
            // GH #457 (Layer 2): never delete an identity this staged
            // generation upserted — every keep id is live in the candidate
            // this commit just published, and removing one is the
            // fresh-reader doc-count deficit the certification refuses. A
            // crash-left queue (or one left by a receipt-skipped
            // conversation) can still name a LIVE chunk: its epoch
            // computed the entry when the conversation was smaller.
            let keep: std::collections::HashSet<&str> =
                stamp.keep_ids.iter().map(String::as_str).collect();
            stale_ids.extend(ids.into_iter().filter(|id| !keep.contains(id.as_str())));
        }
        stale_ids.sort();
        stale_ids.dedup();
        if !stale_ids.is_empty() {
            self.inner
                .delete_cass_document_ids(&stale_ids)
                .context("deleting staged search-region stale identities")?;
        }
        crate::viewer_extensions::search_regions::stamp_region_docs_published(db, &stamps)
    }

    /// Certify the generation this writer's last commit sealed — consumes
    /// the armed coverage floor against the durable MANIFEST at the
    /// writer's own path. Retained for lanes that publish in place:
    /// single-commit lanes certify inside
    /// [`Self::commit_region_doc_stamps`] instead, and the authoritative
    /// rebuild defers to [`Self::certify_published_manifest`] so the
    /// certification binds the manifest readers open at the PUBLISHED
    /// path — never a candidate a refusal kept off the live index.
    pub fn certify_sealed_publication(
        &mut self,
        db: &impl crate::viewer_extensions::facts::FactsDb,
    ) {
        self.certify_published_generation(db);
    }

    /// Task 8 post-commit certification: read the durable Quill MANIFEST
    /// this writer just sealed and bind its generation plus the captured
    /// coverage floor into `viewer_revisions.lexical_generation` +
    /// `viewer_lexical_publication` + the `index_ready` journal row — one
    /// atomic certification transaction. Best-effort by construction: a
    /// certification that cannot write leaves readiness honestly
    /// un-advanced (the floor is restored so the next publish certifies
    /// wider coverage instead of losing it).
    fn certify_published_generation(&mut self, db: &impl crate::viewer_extensions::facts::FactsDb) {
        let floor = self.publication_floor.take();
        if let Err(floor) = Self::certify_manifest_at(db, self.inner.path(), floor) {
            self.publication_floor = self.publication_floor.or(floor);
        }
    }

    /// Task 8 deferred certification for the authoritative rebuild: the
    /// sealing writer is dropped before the candidate is validated and
    /// (for a staged build) swapped live, so certification runs here —
    /// AFTER `validate_published_generation` proved the manifest readers
    /// actually open — against the PUBLISHED path with the floor the
    /// writer armed at build start. A validation/refusal before this
    /// point can never leave a durable certification for a manifest that
    /// never went live. Best-effort: a failure logs and leaves readiness
    /// honestly un-advanced (the disarmed floor is dropped with the
    /// writer either way).
    pub fn certify_published_manifest(
        db: &impl crate::viewer_extensions::facts::FactsDb,
        published_path: &Path,
        floor: Option<crate::viewer_extensions::changes::JournalRevisions>,
    ) {
        let _ = Self::certify_manifest_at(db, published_path, floor);
    }

    /// Bind the durable manifest at `index_path` plus `floor` into the
    /// atomic [`certify_lexical_publication`] transaction. Returns
    /// `Err(floor)` when the certification could not commit so the caller
    /// may re-arm it for a later publish; `Ok(())` covers both a real
    /// certification and a knowingly-skipped one (no decodable manifest,
    /// or an archive whose extension profile predates schema v8 — logged,
    /// never silent).
    fn certify_manifest_at(
        db: &impl crate::viewer_extensions::facts::FactsDb,
        index_path: &Path,
        floor: Option<crate::viewer_extensions::changes::JournalRevisions>,
    ) -> std::result::Result<(), Option<crate::viewer_extensions::changes::JournalRevisions>> {
        let Some(manifest) = crate::search::quill_bridge::manifest_live_doc_count(index_path)
        else {
            // No decodable durable manifest — the commit's generation is
            // not on disk, so there is nothing truthful to certify.
            return Ok(());
        };
        match crate::viewer_extensions::changes::certify_lexical_publication(
            db,
            manifest.generation.min(i64::MAX as u64) as i64,
            manifest.live_docs.min(i64::MAX as u64) as i64,
            floor,
        ) {
            Ok(Some(_)) => Ok(()),
            Ok(None) => {
                // The journal or the v8 receipt table is absent (the
                // extension profile was never installed or predates the
                // certification lane). Certification is a durable no-op
                // there forever — say so rather than silently claiming
                // `lexical_tracking: bound` on the read side.
                tracing::warn!(
                    index_path = %index_path.display(),
                    "lexical publication certification skipped: archive lacks \
                     viewer_lexical_publication (extension schema predates v8 \
                     or is uninstalled); readiness stays honestly un-advanced"
                );
                Ok(())
            }
            Err(error) => {
                tracing::warn!(
                    error = %format!("{error:#}"),
                    "lexical publication certification deferred; readiness stays honestly un-advanced"
                );
                Err(floor)
            }
        }
    }

    /// GH #446: route the stall watchdog's liveness signal into the Quill
    /// sink so accumulate / commit / merge work ticks `activity` — see
    /// `quill_bridge::EngineLivenessProbe`.
    pub fn set_heartbeat(
        &mut self,
        heartbeat: Option<crate::search::quill_bridge::EngineHeartbeat>,
    ) {
        self.inner.set_heartbeat(heartbeat);
    }

    pub fn configure_bulk_load_merge_policy(&mut self) {
        self.inner.configure_bulk_load_merge_policy();
    }

    pub fn reader(&self) -> Result<frankensearch::quill::QuillSearchIndex> {
        self.inner.reader()
    }

    pub fn segment_count(&self) -> usize {
        self.inner.segment_count()
    }

    pub fn merge_status(&self) -> MergeStatus {
        self.inner
            .merge_status(self.inner.segment_count(), now_unix_millis())
    }

    /// Attempt to merge segments if idle conditions are met.
    /// Returns Ok(true) if merge was triggered, Ok(false) if skipped.
    pub fn optimize_if_idle(&mut self) -> Result<bool> {
        self.inner.optimize_if_idle(now_unix_millis())
    }

    /// At most one bounded segment merge; see
    /// [`QuillCassIndex::fold_largest_small_run`].
    pub fn fold_largest_small_run(&mut self) -> Result<bool> {
        self.inner.fold_largest_small_run(now_unix_millis())
    }

    /// Force immediate segment merge and wait for completion.
    /// Use sparingly - blocks until merge finishes.
    pub fn force_merge(&mut self) -> Result<()> {
        self.inner.force_merge()?;
        self.inner.note_merged(now_unix_millis());
        Ok(())
    }
}

pub fn build_schema() -> Schema {
    fs_build_schema()
}

/// Field handles for the compiled CASS schema.
///
/// Quill field ordinals are pinned by `CASS_SEMANTIC_SCHEMA`, so this no longer
/// reads a runtime schema. The parameter is retained so callers keep their
/// shape across the engine swap.
pub fn fields_from_schema(schema: &Schema) -> Result<Fields> {
    let _ = schema;
    Ok(cass_field_handles())
}

pub fn expected_index_dir(base: &Path) -> std::path::PathBuf {
    base.join("index").join(CASS_SCHEMA_VERSION)
}

pub fn index_dir(base: &Path) -> Result<std::path::PathBuf> {
    Ok(fs_index_dir(base)?)
}

pub fn ensure_tokenizer(index: &mut Index) {
    fs_ensure_tokenizer(index);
}
