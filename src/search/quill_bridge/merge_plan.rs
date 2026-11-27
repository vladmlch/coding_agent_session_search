/// Upper bound on bounded-merge passes per `optimize_if_idle` call. Each pass
/// removes at least one segment, so this can only bind on a manifest with
/// thousands of segments — and then it merely defers the remainder to the next
/// maintenance window instead of holding the writer for the whole backlog.
pub(super) const MAX_BOUNDED_MERGE_PASSES: usize = 256;

/// Indexes at or below this many live documents have no protected big segments,
/// but still require balanced merge inputs. One Q1 lease is the boundary.
const BOUNDED_MERGE_SMALL_INDEX_DOCS: u64 = 1 << 16;

/// A segment holding at least `total / BOUNDED_MERGE_BIG_DIVISOR` live docs is
/// "big" and never rewritten by the bounded policy; only runs of small
/// segments are folded. There are at most eight protected big segments;
/// smaller segments accumulate in geometric size tiers between them.
const BOUNDED_MERGE_BIG_DIVISOR: u64 = 8;

/// Pick the next run of adjacent segments to concat-merge, or `None` when the
/// manifest is already converged.
///
/// `profile` is `(segment_id, live_doc_count)` in manifest order. The policy
/// is size-tiered by *document count*, deliberately not by docid width: the
/// engine's own tier merge classifies by docid-range width and refuses runs
/// whose hull is more than half holes, and a CASS archive built session by
/// session is nothing but holes (every writer session leases fresh 65,536-wide
/// docid blocks per shard and burns the unused tail). A Q1 concat merge
/// preserves ids and tolerates those gaps, so counting live rows is the
/// measure that actually predicts merge cost and query cost here.
///
/// Returns the longest balanced run of adjacent small segments. The largest
/// input must not exceed the other inputs combined: an existing tail cannot
/// be rewritten on every tiny append (GH #479). Ties go to the earliest run.
#[cfg(test)]
pub(super) fn plan_bounded_segment_merge(profile: &[(u64, u64)]) -> Option<Vec<u64>> {
    let mut best: Option<(usize, usize)> = None;
    for (start, end) in small_segment_ranges(profile) {
        if let Some((offset, len)) = balanced_segment_subrun(&profile[start..end])
            && best.is_none_or(|(_, best_len)| len > best_len)
        {
            best = Some((start + offset, len));
        }
    }
    best.map(|(start, len)| {
        profile[start..start + len]
            .iter()
            .map(|(id, _)| *id)
            .collect()
    })
}

fn small_segment_ranges(profile: &[(u64, u64)]) -> Vec<(usize, usize)> {
    let total: u64 = profile
        .iter()
        .map(|(_, live)| *live)
        .fold(0, u64::saturating_add);
    if total <= BOUNDED_MERGE_SMALL_INDEX_DOCS {
        return vec![(0, profile.len())];
    }
    let big_threshold = (total / BOUNDED_MERGE_BIG_DIVISOR).max(1);
    let mut runs = Vec::new();
    let mut run_start: Option<usize> = None;
    for (index, (_, live)) in profile.iter().enumerate() {
        let small = *live < big_threshold;
        match (small, run_start) {
            (true, None) => run_start = Some(index),
            (false, Some(start)) => {
                runs.push((start, index));
                run_start = None;
            }
            _ => {}
        }
    }
    if let Some(start) = run_start {
        runs.push((start, profile.len()));
    }
    runs
}

/// Find a contiguous run whose largest input is no larger than its peers
/// combined. An unbalanced run has one dominant input; no subrun containing
/// that input can qualify, so search independently on either side of it.
/// Every split removes more than half the run's live-document mass, bounding
/// depth by 128 for the u128 aggregate. Each level visits disjoint slices;
/// zero-mass slices are skipped. The explicit stack avoids recursion.
pub(super) fn balanced_segment_subrun(profile: &[(u64, u64)]) -> Option<(usize, usize)> {
    let mut pending = vec![(0, profile.len())];
    let mut best: Option<(usize, usize)> = None;
    while let Some((start, end)) = pending.pop() {
        let len = end - start;
        if len < 2 || best.is_some_and(|(_, best_len)| len < best_len) {
            continue;
        }
        let mut total = 0_u128;
        let mut largest = 0_u128;
        let mut largest_index = start;
        for (offset, (_, live)) in profile[start..end].iter().enumerate() {
            let weight = u128::from(*live);
            total += weight;
            if weight > largest {
                largest = weight;
                largest_index = start + offset;
            }
        }
        if largest == 0 {
            continue;
        }
        if largest <= total - largest {
            if best.is_none_or(|(best_start, best_len)| {
                len > best_len || (len == best_len && start < best_start)
            }) {
                best = Some((start, len));
            }
        } else {
            pending.push((largest_index + 1, end));
            pending.push((start, largest_index));
        }
    }
    best
}

/// A byte-cap split must retain geometric balance too: a balanced overall
/// run can otherwise become an expensive large-plus-tiny prefix after capping.
pub(super) fn plan_capped_balanced_merge_runs(
    profile: &[(u64, u64)],
    fold_profile: &[SegmentFoldProfile],
    max_output_bytes: u64,
) -> Vec<Vec<u64>> {
    let live_counts: std::collections::HashMap<_, _> = profile.iter().copied().collect();
    let folds: std::collections::HashMap<_, _> = fold_profile
        .iter()
        .map(|segment| (segment.segment_id, *segment))
        .collect();
    let mut planned = Vec::new();
    let mut used = std::collections::HashSet::new();
    for (start, end) in small_segment_ranges(profile) {
        let run = &profile[start..end];
        let Some(run_folds) = run
            .iter()
            .map(|(id, _)| folds.get(id).copied())
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        for capped in plan_capped_merge_runs(&run_folds, max_output_bytes) {
            let sub_profile: Vec<_> = capped
                .iter()
                .filter_map(|id| live_counts.get(id).map(|live| (*id, *live)))
                .collect();
            if let Some((offset, len)) = balanced_segment_subrun(&sub_profile) {
                let ids: Vec<_> = sub_profile[offset..offset + len]
                    .iter()
                    .map(|(id, _)| *id)
                    .collect();
                used.extend(ids.iter().copied());
                planned.push(ids);
            }
        }
        // Recover comparable siblings across artificial greedy-cap boundaries.
        // Do not revisit any input already selected above or by an earlier pair.
        for (pair, pair_folds) in run.windows(2).zip(run_folds.windows(2)) {
            let (left_id, left) = pair[0];
            let (right_id, right) = pair[1];
            if left == 0 || right == 0 || used.contains(&left_id) || used.contains(&right_id) {
                continue;
            }
            if u128::from(left.max(right)) <= 2 * u128::from(left.min(right))
                && fold_fits(pair_folds, max_output_bytes)
            {
                used.insert(left_id);
                used.insert(right_id);
                planned.push(vec![left_id, right_id]);
            }
        }
    }
    planned
}

/// GH #456: default upper bound on the estimated output of one concat merge
/// (1 GiB; see `estimated_fold_output_bytes`).
///
/// The engine builds a merge output as one in-memory buffer sized to the
/// finished segment file, then re-verifies it in place before writing, so the
/// peak anonymous allocation of a fold is about the output size plus the
/// inputs' term dictionaries. Without a cap the end-of-rebuild fold (and an
/// incremental fold over a fully fragmented generation) is sized to the whole
/// lexical index. With it, a generation converges to roughly
/// `index_bytes / cap` segments — a handful even at 10 GB — and the fold's
/// memory no longer grows with archive size.
pub const CASS_LEXICAL_MERGE_MAX_OUTPUT_BYTES_DEFAULT: u64 = 1 << 30;

/// Environment override for [`CASS_LEXICAL_MERGE_MAX_OUTPUT_BYTES_DEFAULT`]
/// (bytes; underscores allowed; `0`/unparsable keep the default).
pub const CASS_LEXICAL_MERGE_MAX_OUTPUT_BYTES_ENV: &str = "CASS_LEXICAL_MERGE_MAX_OUTPUT_BYTES";

/// The effective merge-output byte cap (GH #456).
#[must_use]
pub fn lexical_merge_max_output_bytes() -> u64 {
    lexical_merge_max_output_bytes_from(
        dotenvy::var(CASS_LEXICAL_MERGE_MAX_OUTPUT_BYTES_ENV)
            .ok()
            .as_deref(),
    )
}

pub(super) fn lexical_merge_max_output_bytes_from(raw: Option<&str>) -> u64 {
    raw.map(|value| value.trim().replace('_', ""))
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|&bytes| bytes > 0)
        .unwrap_or(CASS_LEXICAL_MERGE_MAX_OUTPUT_BYTES_DEFAULT)
}

/// What the merge-output cap plans against for one published segment
/// (GH #456): its file bytes and its Q1 docid range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SegmentFoldProfile {
    pub segment_id: u64,
    pub file_len: u64,
    pub docid_lo: u64,
    pub docid_hi: u64,
    /// Physical documents the segment holds, tombstoned ones included until
    /// compaction: everything its posting lists can cover (GH #498).
    pub doc_count: u64,
}

/// GH #498: the most physical documents one concat-merge output may hold.
///
/// Every Quill reader parses a term's posting list under the engine's
/// `DEFAULT_MAX_POSTINGS_PER_TERM` (2^22) and the concat merge refuses to
/// assemble a list it could not reopen, so an output where a common token is
/// in more documents than that fails with "posting validation limit 4194304
/// exceeded". A term's postings cover at most every document the sources
/// hold, so a fold within this bound cannot hit the limit whatever the text.
pub(super) const MAX_FOLD_OUTPUT_DOCS: u64 =
    frankensearch::quill::quiver::DEFAULT_MAX_POSTINGS_PER_TERM as u64;

/// Physical documents a concat merge of `run` would hold (GH #498).
pub(super) fn fold_doc_count(run: &[SegmentFoldProfile]) -> u64 {
    run.iter()
        .map(|segment| segment.doc_count)
        .fold(0_u64, u64::saturating_add)
}

/// Whether a concat merge of `run` stays under both the output byte cap
/// (GH #456) and the posting limit (GH #498).
fn fold_fits(run: &[SegmentFoldProfile], max_output_bytes: u64) -> bool {
    estimated_fold_output_bytes(run) <= max_output_bytes
        && fold_doc_count(run) <= MAX_FOLD_OUTPUT_DOCS
}

/// Conservative per-docid cost of the docid *hull* a concat-merge output
/// spans (GH #456). A Q1-preserving merge keeps global docids, so its output
/// carries per-docid structures for every id between the first input's
/// `docid_lo` and the last input's `docid_hi` — holes included. Two 3 KB
/// session segments one lease apart merge into a 4,624,714-byte file over a
/// 65,538-docid hull (measured on quill 0.2.3: ~70.5 bytes per hull docid);
/// an archive built session by session is nothing but such holes, which is
/// how #453's incremental fold of 1,605 session segments produced one
/// 6.75 GB file. 128 leaves headroom over the measurement so the estimate
/// stays an upper bound (`force_merge_under_a_byte_cap_…` asserts it).
pub(super) const FOLD_HULL_BYTES_PER_DOCID: u64 = 128;

/// Upper-bound estimate of the file a concat merge of `run` would assemble
/// (and therefore hold in memory): the inputs' bytes plus the hull cost.
pub(super) fn estimated_fold_output_bytes(run: &[SegmentFoldProfile]) -> u64 {
    let bytes = run
        .iter()
        .map(|segment| segment.file_len)
        .fold(0_u64, u64::saturating_add);
    let lo = run
        .iter()
        .map(|segment| segment.docid_lo)
        .min()
        .unwrap_or(0);
    let hi = run
        .iter()
        .map(|segment| segment.docid_hi)
        .max()
        .unwrap_or(0);
    bytes.saturating_add(
        hi.saturating_sub(lo)
            .saturating_mul(FOLD_HULL_BYTES_PER_DOCID),
    )
}

/// Split a manifest-ordered fold profile into consecutive runs whose
/// estimated merge output ([`estimated_fold_output_bytes`]) stays at or below
/// `max_output_bytes` and whose documents stay within
/// [`MAX_FOLD_OUTPUT_DOCS`], keeping only runs of two or more segments (a
/// single segment has nothing to fold into). A segment that exceeds either
/// bound on its own closes the run before it and is left as it is. Greedy and
/// deterministic in manifest order, so the runs stay consecutive — the shape
/// a Q1-preserving concat merge requires — and merging one run never breaks
/// the adjacency of another.
pub(super) fn plan_capped_merge_runs(
    profile: &[SegmentFoldProfile],
    max_output_bytes: u64,
) -> Vec<Vec<u64>> {
    let mut runs: Vec<Vec<u64>> = Vec::new();
    let mut current: Vec<SegmentFoldProfile> = Vec::new();
    let mut current_bytes = 0_u64;
    let mut current_docs = 0_u64;
    let mut current_lo = 0_u64;
    let mut current_hi = 0_u64;
    let close = |current: &mut Vec<SegmentFoldProfile>, runs: &mut Vec<Vec<u64>>| {
        if current.len() >= 2 {
            runs.push(current.iter().map(|segment| segment.segment_id).collect());
        }
        current.clear();
    };
    for &segment in profile {
        if !current.is_empty() {
            let next_bytes = current_bytes.saturating_add(segment.file_len);
            let next_docs = current_docs.saturating_add(segment.doc_count);
            let next_lo = current_lo.min(segment.docid_lo);
            let next_hi = current_hi.max(segment.docid_hi);
            let next_estimate = next_bytes.saturating_add(
                next_hi
                    .saturating_sub(next_lo)
                    .saturating_mul(FOLD_HULL_BYTES_PER_DOCID),
            );
            if next_estimate > max_output_bytes || next_docs > MAX_FOLD_OUTPUT_DOCS {
                close(&mut current, &mut runs);
            } else {
                current.push(segment);
                current_bytes = next_bytes;
                current_docs = next_docs;
                current_lo = next_lo;
                current_hi = next_hi;
                continue;
            }
        }
        if !fold_fits(std::slice::from_ref(&segment), max_output_bytes) {
            // Oversized on its own: never a merge input under these bounds.
            continue;
        }
        current.push(segment);
        current_bytes = segment.file_len;
        current_docs = segment.doc_count;
        current_lo = segment.docid_lo;
        current_hi = segment.docid_hi;
    }
    close(&mut current, &mut runs);
    runs
}

/// A random, nonzero segment id absent from `profile`.
pub(super) fn fresh_segment_id(profile: &[(u64, u64)]) -> u64 {
    loop {
        let candidate: u64 = rand::random();
        if candidate != 0 && profile.iter().all(|(id, _)| *id != candidate) {
            return candidate;
        }
    }
}
