use std::collections::HashMap;

use anyhow::{Result, bail};
use indicatif::ProgressBar;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use super::config::{
    SEMANTIC_PREP_MEMO_ALGORITHM, SEMANTIC_PREP_MEMO_VERSION, saturating_u64_from_usize,
};
use super::types::{EmbeddedMessage, EmbeddingInput};
use crate::indexer::memoization::{
    ContentAddressedMemoCache, MemoCacheAuditRecord, MemoContentHash, MemoKey, MemoLookup,
};
use crate::search::canonicalize::{canonicalize_for_embedding, content_hash};
use crate::search::embedder::Embedder;

pub(in crate::indexer::semantic::engine) struct Prepared<'a> {
    pub(in crate::indexer::semantic::engine) msg: &'a EmbeddingInput,
    pub(in crate::indexer::semantic::engine) canonical: String,
    pub(in crate::indexer::semantic::engine) hash: [u8; 32],
}

/// An invocation owns its reuse table. Vectors stay in the required output
/// records; entries borrow the original input text and retain an output index.
/// A content hash selects a bucket, never establishes text equality.
#[derive(Default)]
pub(in crate::indexer::semantic::engine) struct ExactEmbeddingReuse<'a> {
    pub(in crate::indexer::semantic::engine) by_hash: HashMap<[u8; 32], Vec<(&'a str, usize)>>,
}

impl<'a> ExactEmbeddingReuse<'a> {
    pub(in crate::indexer::semantic::engine) fn lookup(
        &self,
        prepared: &Prepared<'_>,
    ) -> Option<usize> {
        self.by_hash
            .get(&prepared.hash)?
            .iter()
            .find_map(|(raw, output_index)| {
                // Recheck exact canonical equality only on a hash hit. Borrowing
                // the input avoids retaining a second corpus of canonical text
                // when a full index passes the entire archive in one invocation.
                (canonicalize_for_embedding(raw) == prepared.canonical).then_some(*output_index)
            })
    }

    pub(in crate::indexer::semantic::engine) fn insert(
        &mut self,
        prepared: &Prepared<'a>,
        output_index: usize,
    ) {
        self.by_hash
            .entry(prepared.hash)
            .or_default()
            .push((prepared.msg.content.as_str(), output_index));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::indexer::semantic::engine) struct MemoizedPreparedMessage {
    canonical: String,
    hash: [u8; 32],
}

pub(in crate::indexer::semantic::engine) fn semantic_prep_memo_key(content: &str) -> MemoKey {
    MemoKey::new(
        MemoContentHash::from_bytes(content_hash(content).to_vec()),
        SEMANTIC_PREP_MEMO_ALGORITHM,
        SEMANTIC_PREP_MEMO_VERSION,
    )
}

fn memo_counter_delta(after: u64, before: u64) -> u64 {
    after.saturating_sub(before)
}

pub(in crate::indexer::semantic::engine) fn trace_semantic_prep_memo_window(
    window_index: usize,
    window_len: usize,
    prepared_len: usize,
    entry_capacity: usize,
    before: &crate::indexer::memoization::MemoCacheStats,
    after: &crate::indexer::memoization::MemoCacheStats,
) {
    tracing::trace!(
        algorithm = SEMANTIC_PREP_MEMO_ALGORITHM,
        algorithm_version = SEMANTIC_PREP_MEMO_VERSION,
        window_index,
        window_len,
        prepared_messages = prepared_len,
        skipped_messages = window_len.saturating_sub(prepared_len),
        hit_delta = memo_counter_delta(after.hits, before.hits),
        miss_delta = memo_counter_delta(after.misses, before.misses),
        insert_delta = memo_counter_delta(after.inserts, before.inserts),
        evictions_capacity_delta =
            memo_counter_delta(after.evictions_capacity, before.evictions_capacity),
        quarantined_delta = memo_counter_delta(after.quarantined, before.quarantined),
        live_entries = after.live_entries,
        entry_capacity,
        "semantic prep memo cache window"
    );
}

fn trace_semantic_prep_memo_audit(audit: &MemoCacheAuditRecord) {
    tracing::trace!(?audit, "semantic prep memo cache audit");
}

pub(in crate::indexer::semantic::engine) fn prepare_window_with_memo<'a>(
    window: &'a [EmbeddingInput],
    cache: &mut ContentAddressedMemoCache<MemoizedPreparedMessage>,
) -> Vec<Prepared<'a>> {
    window
        .iter()
        .filter_map(|msg| {
            let key = semantic_prep_memo_key(&msg.content);
            let (lookup, lookup_audit) = cache.get_with_audit(&key);
            trace_semantic_prep_memo_audit(&lookup_audit);
            match lookup {
                MemoLookup::Hit { value } => Some(Prepared {
                    msg,
                    canonical: value.canonical,
                    hash: value.hash,
                }),
                MemoLookup::Miss | MemoLookup::Quarantined { .. } => {
                    let canonical = canonicalize_for_embedding(&msg.content);
                    if canonical.is_empty() {
                        return None;
                    }
                    let hash = content_hash(&canonical);
                    let insert_audit = cache.insert_with_audit(
                        key,
                        MemoizedPreparedMessage {
                            canonical: canonical.clone(),
                            hash,
                        },
                    );
                    trace_semantic_prep_memo_audit(&insert_audit);
                    Some(Prepared {
                        msg,
                        canonical,
                        hash,
                    })
                }
            }
        })
        .collect()
}

/// Canonicalize + hash a window of messages. Default is serial; opt in to
/// the rayon-parallel path via `CASS_SEMANTIC_PREP_PARALLEL=1` (see the
/// `parallel_prep_enabled` docstring for why it is not the default).
/// Parallel results preserve input order via `par_iter().filter_map().collect()`.
/// Messages whose canonical form is empty are filtered out so the embedder
/// batch is never polluted with useless inputs.
pub(in crate::indexer::semantic::engine) fn prepare_window<'a>(
    window: &'a [EmbeddingInput],
    serial: bool,
) -> Vec<Prepared<'a>> {
    let prep = |msg: &'a EmbeddingInput| -> Option<Prepared<'a>> {
        let canonical = canonicalize_for_embedding(&msg.content);
        if canonical.is_empty() {
            return None;
        }
        let hash = content_hash(&canonical);
        Some(Prepared {
            msg,
            canonical,
            hash,
        })
    };

    if serial {
        window.iter().filter_map(prep).collect()
    } else {
        window.par_iter().filter_map(prep).collect()
    }
}

/// Split a prepared window into contiguous, length-aware embed batches (cass
/// #309).
///
/// Each returned batch holds at most `max_count` rows AND keeps
/// `row_count × max_canonical_len ≤ char_budget`, so a batch that contains a
/// long message stays small and the embedder's padded `batch × max_seq²`
/// working set stays bounded regardless of corpus content. A single message
/// longer than `char_budget` becomes its own one-row batch (never dropped).
/// Order is preserved — callers still zip results back to inputs positionally.
/// `char_budget == 0` disables the byte bound (fixed `max_count` chunks,
/// pre-#309 behavior).
pub(in crate::indexer::semantic::engine) fn length_aware_batches<'p, 'a>(
    prepared: &'p [Prepared<'a>],
    max_count: usize,
    char_budget: usize,
) -> Vec<&'p [Prepared<'a>]> {
    let max_count = max_count.max(1);
    let mut batches: Vec<&'p [Prepared<'a>]> = Vec::new();
    if prepared.is_empty() {
        return batches;
    }
    let mut start = 0usize;
    let mut max_len = 0usize;
    for (i, item) in prepared.iter().enumerate() {
        let len = item.canonical.len();
        let count = i - start + 1;
        let prospective_max = max_len.max(len);
        // Keep at least one row per batch: only split on the budget once the
        // in-progress batch already holds a row (`count > 1`), so a lone
        // over-budget message still forms its own batch rather than looping.
        let over_budget =
            char_budget > 0 && count > 1 && count.saturating_mul(prospective_max) > char_budget;
        if count > max_count || over_budget {
            batches.push(&prepared[start..i]);
            start = i;
            max_len = len;
        } else {
            max_len = prospective_max;
        }
    }
    batches.push(&prepared[start..]);
    batches
}

pub(in crate::indexer::semantic::engine) fn flush_prepared_batch<'a>(
    batch: &[Prepared<'a>],
    embeddings: &mut Vec<EmbeddedMessage>,
    pb: &ProgressBar,
    embedder: &dyn Embedder,
    mut reuse: Option<&mut ExactEmbeddingReuse<'a>>,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    enum VectorSource {
        PriorOutput(usize),
        Fresh(usize),
    }

    let mut plan = Vec::with_capacity(batch.len());
    let mut fresh: Vec<&Prepared<'_>> = Vec::new();
    let mut fresh_by_text: HashMap<&str, usize> = HashMap::new();
    for prepared in batch {
        if let Some(output_index) = reuse.as_ref().and_then(|cache| cache.lookup(prepared)) {
            plan.push(VectorSource::PriorOutput(output_index));
        } else if let Some(fresh_index) = reuse
            .as_ref()
            .and_then(|_| fresh_by_text.get(prepared.canonical.as_str()))
        {
            plan.push(VectorSource::Fresh(*fresh_index));
        } else {
            let fresh_index = fresh.len();
            if reuse.is_some() {
                fresh_by_text.insert(prepared.canonical.as_str(), fresh_index);
            }
            fresh.push(prepared);
            plan.push(VectorSource::Fresh(fresh_index));
        }
    }
    let texts: Vec<&str> = fresh.iter().map(|p| p.canonical.as_str()).collect();
    let vectors = if texts.is_empty() {
        Vec::new()
    } else {
        embedder
            .embed_batch_sync(&texts)
            .map_err(|e| anyhow::anyhow!("embedding failed: {e}"))?
    };

    if vectors.len() != fresh.len() {
        bail!(
            "embedder returned {} embeddings for {} inputs",
            vectors.len(),
            fresh.len()
        );
    }

    // Admit every fresh vector before extending either the outputs or reuse
    // table. A malformed batch cannot supply a cached result to a later row.
    for (prepared, vector) in fresh.iter().zip(&vectors) {
        validate_embedding_vector(vector, embedder.dimension(), prepared.msg.message_id)?;
    }
    let mut vectors: Vec<_> = vectors.into_iter().map(Some).collect();
    let mut fresh_outputs = vec![0usize; fresh.len()];
    for (prepared, source) in batch.iter().zip(plan) {
        let (vector, first_occurrence) = match source {
            VectorSource::PriorOutput(output_index) => {
                (embeddings[output_index].embedding.clone(), false)
            }
            VectorSource::Fresh(fresh_index) => match vectors[fresh_index].take() {
                Some(vector) => {
                    fresh_outputs[fresh_index] = embeddings.len();
                    (vector, true)
                }
                None => (
                    embeddings[fresh_outputs[fresh_index]].embedding.clone(),
                    false,
                ),
            },
        };
        if let Some(cache) = reuse.as_mut()
            && first_occurrence
        {
            cache.insert(prepared, embeddings.len());
        }
        embeddings.push(EmbeddedMessage {
            message_id: prepared.msg.message_id,
            created_at_ms: prepared.msg.created_at_ms,
            agent_id: prepared.msg.agent_id,
            workspace_id: prepared.msg.workspace_id,
            source_id: prepared.msg.source_id,
            role: prepared.msg.role,
            chunk_idx: prepared.msg.chunk_idx,
            content_hash: prepared.hash,
            embedding: vector,
        });
    }

    pb.inc(saturating_u64_from_usize(batch.len()));
    Ok(())
}

pub(in crate::indexer::semantic::engine) fn validate_embedding_vector(
    vector: &[f32],
    expected_dimension: usize,
    message_id: u64,
) -> Result<()> {
    if vector.len() != expected_dimension {
        bail!(
            "embedding dimension mismatch: expected {}, got {}",
            expected_dimension,
            vector.len()
        );
    }
    if vector.iter().any(|value| !value.is_finite()) {
        bail!("embedding for message {message_id} contains a non-finite value");
    }
    Ok(())
}
