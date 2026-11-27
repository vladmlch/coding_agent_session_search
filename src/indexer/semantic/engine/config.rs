use super::scheduler::env_truthy;
pub use crate::search_defaults::{HASH_VECTOR_SPACE_REVISION, expected_vector_space_revision};

/// Default embedder batch size — the **maximum row count** per embed batch.
/// 128 is a sweet spot for short messages: big enough to amortize dispatch
/// overhead and keep the tensor kernels saturated. It is no longer the sole
/// bound on a batch: [`DEFAULT_SEMANTIC_EMBED_BATCH_CHAR_BUDGET`] additionally
/// caps `row_count × max_canonical_len` so a batch that happens to contain a
/// long message holds proportionally fewer rows (cass #309).
pub(in crate::indexer::semantic::engine) const DEFAULT_SEMANTIC_BATCH_SIZE: usize = 128;
/// Default per-batch canonical-character budget for length-aware embed
/// batching (cass #309). A fixed-size batch (128) padded to its longest member
/// makes the ONNX embedder allocate a `batch × max_seq²` attention tensor, so a
/// single long message in the batch blows RSS to multiple GB (#309 observed
/// 4.7–9.9 GB on a 67 MB corpus, flat RSS + 1000–1200% CPU = ort inference, not
/// FSVI staging). Capping `row_count × max_canonical_len ≤ this` bounds that
/// padded working set; with `MAX_EMBED_CHARS`-capped (~2 KB) messages the worst
/// case is ≈8 rows — the regime that embeds cleanly (<1 GB) in practice. Set
/// `CASS_SEMANTIC_EMBED_BATCH_CHAR_BUDGET=0` to disable (fixed `batch_size`
/// chunks, pre-#309 behavior).
pub(in crate::indexer::semantic::engine) const DEFAULT_SEMANTIC_EMBED_BATCH_CHAR_BUDGET: usize =
    16 * 1024;
pub(in crate::indexer::semantic::engine) const DEFAULT_SEMANTIC_PREP_MEMO_CAPACITY: usize = 4_096;
pub(in crate::indexer::semantic::engine) const DEFAULT_SEMANTIC_EMBED_BATCH_WARN_AFTER_MS: u64 =
    30_000;
pub(in crate::indexer::semantic::engine) const DEFAULT_SEMANTIC_EMBED_BATCH_FAIL_AFTER_MS: u64 =
    300_000;
pub(in crate::indexer::semantic::engine) const DEFAULT_SEMANTIC_MAX_MESSAGES_PER_CHECKPOINT: usize =
    10_000;
pub(in crate::indexer::semantic::engine) const DEFAULT_SEMANTIC_MAX_BYTES_PER_CHECKPOINT: u64 =
    8 * 1024 * 1024;
pub(in crate::indexer::semantic::engine) const DEFAULT_SEMANTIC_RECONCILIATION_SCAN_CONVERSATIONS: usize = 64;
pub(in crate::indexer::semantic::engine) const SEMANTIC_PREP_MEMO_ALGORITHM: &str =
    "semantic_prepare_window";
pub(in crate::indexer::semantic::engine) const SEMANTIC_PREP_MEMO_VERSION: &str =
    "canonicalize_for_embedding:v2:stable-content-hash";
/// A caller-owned cancellation check for backfill jobs. It must be cheap and
/// must not block. Backfill polls it at durable batch boundaries; an
/// interrupted batch is never committed.
pub type CancelCheck = std::sync::Arc<dyn Fn() -> bool + Send + Sync>;

pub(in crate::indexer::semantic::engine) fn resolved_env_usize(key: &str, default: usize) -> usize {
    dotenvy::var(key)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

pub(in crate::indexer::semantic::engine) fn resolved_env_u64(key: &str, default: u64) -> u64 {
    dotenvy::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

pub(in crate::indexer::semantic::engine) fn resolved_default_batch_size() -> usize {
    dotenvy::var("CASS_SEMANTIC_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_SEMANTIC_BATCH_SIZE)
}

pub(in crate::indexer::semantic::engine) fn resolved_semantic_prep_memo_capacity() -> usize {
    dotenvy::var("CASS_SEMANTIC_PREP_MEMO_CAPACITY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_SEMANTIC_PREP_MEMO_CAPACITY)
}

pub(in crate::indexer::semantic::engine) fn resolved_semantic_embed_batch_char_budget() -> usize {
    resolved_env_usize(
        "CASS_SEMANTIC_EMBED_BATCH_CHAR_BUDGET",
        DEFAULT_SEMANTIC_EMBED_BATCH_CHAR_BUDGET,
    )
}

pub(in crate::indexer::semantic::engine) fn resolved_semantic_embed_batch_warn_after_ms() -> u64 {
    resolved_env_u64(
        "CASS_SEMANTIC_EMBED_BATCH_WARN_AFTER_MS",
        DEFAULT_SEMANTIC_EMBED_BATCH_WARN_AFTER_MS,
    )
}

pub(in crate::indexer::semantic::engine) fn resolved_semantic_embed_batch_fail_after_ms() -> u64 {
    resolved_env_u64(
        "CASS_SEMANTIC_EMBED_BATCH_FAIL_AFTER_MS",
        DEFAULT_SEMANTIC_EMBED_BATCH_FAIL_AFTER_MS,
    )
}

/// Opt in to the rayon-parallel canonicalize+hash prep step. **Default: OFF.**
///
/// The parallel path is kept because canonicalize+hash CAN dominate the
/// embedding wall-clock on pathological inputs (very long messages, costly
/// Unicode normalization). But criterion baselines captured under
/// `tests/artifacts/perf/2026-04-21-profile-run/baselines.md` showed a
/// 1.2×–2.3× **regression** on the hash embedder across every batch size
/// tested (2 000 messages, mixed markdown/code/unicode): rayon's per-task
/// scheduling overhead is larger than the per-message canonicalize+hash cost
/// when the embedder itself is cheap. For the production ONNX (MiniLM)
/// embedder, per-batch inference already dwarfs prep, so parallel prep never
/// buys meaningful wall-clock — the prep step is ≤ 1% of total embed time.
///
/// Set `CASS_SEMANTIC_PREP_PARALLEL=1` / `true` / `yes` / `on` to opt in.
pub(in crate::indexer::semantic::engine) fn parallel_prep_enabled() -> bool {
    env_truthy("CASS_SEMANTIC_PREP_PARALLEL")
}

pub(in crate::indexer::semantic::engine) fn saturating_u64_from_usize(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(in crate::indexer::semantic::engine) fn saturating_u64_from_millis(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
