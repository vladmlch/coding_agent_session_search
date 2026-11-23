use super::merge_plan::{
    FOLD_HULL_BYTES_PER_DOCID, MAX_FOLD_OUTPUT_DOCS, SegmentFoldProfile, balanced_segment_subrun,
    estimated_fold_output_bytes, fold_doc_count, fresh_segment_id,
    lexical_merge_max_output_bytes_from, plan_bounded_segment_merge,
    plan_capped_balanced_merge_runs, plan_capped_merge_runs,
};
use super::tests::{published_segment_count, sample};
use super::*;

/// GH #498 end to end on the real engine, along the reporter's path
/// (`phase=checkpoint_after_fold`): a rebuild's own fold leaves eight
/// "large" segments — each wider than the engine's 524,288-docid medium
/// tier — holding 4,240,000 documents that share a token, and the next
/// commit runs the engine tier merge over them. Under the engine's
/// default fanout that commit fails exactly as reported; under the CASS
/// config it publishes, and the CASS fold stays within the posting limit
/// and keeps the token searchable.
///
/// Ignored by default (4.24M documents); run with
/// `cargo test --lib gh498_multi_million -- --ignored --nocapture`.
#[test]
#[ignore = "ingests 4.24M documents; run explicitly for #498 evidence"]
fn gh498_multi_million_document_generation_commits_and_folds() {
    const LARGE_SEGMENT_DOCS: u64 = 530_000;
    const LARGE_SEGMENTS: u64 = 8;
    let config_with_fanout = |tier_fanout: usize| QuillConfig {
        deterministic_ingest: true,
        tier_fanout,
        // Seal at commit, not on the 64 MiB accumulation budget.
        scribe_shard_budget_bytes: 4 << 30,
        ..cass_quill_config()
    };
    let common = |source: &str, msg_idx: u64| sample(source, msg_idx, "common posting token");
    let directory = tempfile::tempdir().expect("bridge index directory");

    // Ingest without engine merges, then fold consecutive segments as a
    // rebuild's capped fold does, into eight large segments.
    let mut index = QuillCassIndex::open_or_create_with_config(
        directory.path(),
        &config_with_fanout(usize::MAX),
    )
    .expect("open");
    for commit in 0..LARGE_SEGMENTS {
        let batch: Vec<_> = (0..LARGE_SEGMENT_DOCS)
            .map(|offset| common(&format!("gh498-{commit}"), offset))
            .collect();
        index.add_cass_documents(&batch).expect("index batch");
        index.commit().expect("commit batch");
    }
    let mut run = Vec::new();
    let mut run_docs = 0_u64;
    for segment in index
        .published_segment_fold_profile()
        .expect("fold profile")
    {
        run.push(segment.segment_id);
        run_docs += segment.doc_count;
        if run_docs >= LARGE_SEGMENT_DOCS {
            if run.len() > 1 {
                index
                    .concat_merge_run(&run)
                    .expect("fold into one large segment");
            }
            run.clear();
            run_docs = 0;
        }
    }
    let large = index
        .published_segment_fold_profile()
        .expect("fold profile");
    eprintln!("gh498 large generation: {large:?}");
    assert_eq!(
        large.len(),
        usize::try_from(LARGE_SEGMENTS).expect("small count")
    );
    assert!(
        large
            .iter()
            .all(|segment| segment.docid_hi - segment.docid_lo
                > frankensearch::quill::config::DEFAULT_TIER_MEDIUM_MAX_DOCID_WIDTH),
        "every folded segment must be in the engine's large tier: {large:?}"
    );
    assert!(fold_doc_count(&large) > MAX_FOLD_OUTPUT_DOCS);
    drop(index);

    // Control: the next commit under the engine default tier-merges them.
    let mut control = QuillCassIndex::open_or_create_with_config(
        directory.path(),
        &config_with_fanout(frankensearch::quill::config::DEFAULT_TIER_FANOUT),
    )
    .expect("reopen under the engine default");
    control
        .add_cass_documents(&[common("gh498-control", 0)])
        .expect("index one more document");
    let error = format!(
        "{:#}",
        control
            .commit()
            .expect_err("control: the engine tier merge must refuse the oversized fold")
    );
    eprintln!("gh498 control error: {error}");
    assert!(
        error.contains("posting validation limit 4194304 exceeded"),
        "control must reproduce #498: {error}"
    );
    drop(control);

    // Fix: the same commit under the CASS config publishes, and the CASS
    // fold splits the generation within the posting limit.
    let mut index = QuillCassIndex::open_or_create_with_config(
        directory.path(),
        &config_with_fanout(cass_quill_config().tier_fanout),
    )
    .expect("reopen under the CASS config");
    index
        .add_cass_documents(&[common("gh498-fixed", 0)])
        .expect("index one more document");
    index
        .commit()
        .expect("a CASS commit never runs the engine tier merge");
    let total = index.doc_count().expect("doc count");
    assert!(total > MAX_FOLD_OUTPUT_DOCS);
    let before = index.segment_count();
    index
        .force_merge_with_output_cap(u64::MAX)
        .expect("the fold is planned within the posting limit");
    let folded = index
        .published_segment_fold_profile()
        .expect("fold profile");
    eprintln!("gh498 folded generation: {folded:?}");
    assert!(
        folded.len() >= 2 && folded.len() < before,
        "the fold must merge, but not into one segment: {folded:?}"
    );
    assert!(
        folded
            .iter()
            .all(|segment| segment.doc_count <= MAX_FOLD_OUTPUT_DOCS),
        "{folded:?}"
    );
    assert_eq!(index.doc_count().expect("doc count"), total);
    let reader = index.reader().expect("reader over the folded generation");
    let parser = frankensearch::quill::query::CassQueryParser::new(CASS_SEMANTIC_SCHEMA)
        .expect("CASS query parser");
    let parsed = parser.parse(
        "common",
        &frankensearch::quill::query::CassQueryFilters::default(),
    );
    let page = search_paginated(&reader, &parsed.query, 10, 0, false)
        .expect("the common token's posting lists reopen");
    assert_eq!(page.hits.len(), 10);
}

#[test]
fn bounded_merge_plan_folds_small_runs_and_leaves_big_segments_alone() {
    // Tiny index: everything merges.
    assert_eq!(
        plan_bounded_segment_merge(&[(1, 10), (2, 20), (3, 30)]),
        Some(vec![1, 2, 3])
    );
    // Nothing to do with fewer than two segments.
    assert_eq!(plan_bounded_segment_merge(&[(1, 10)]), None);
    assert_eq!(plan_bounded_segment_merge(&[]), None);

    // Large index (total 800k, big threshold 100k): one big head, then a
    // run of small tail segments -> the tail run is the plan and the big
    // segment is untouched.
    let profile = [(7, 700_000), (8, 40_000), (9, 30_000), (10, 30_000)];
    assert_eq!(plan_bounded_segment_merge(&profile), Some(vec![8, 9, 10]));

    // Two runs: the longest wins; ties go to the earliest.
    let profile = [
        (1, 500_000),
        (2, 10),
        (3, 10),
        (4, 500_000),
        (5, 10),
        (6, 10),
        (7, 10),
    ];
    assert_eq!(plan_bounded_segment_merge(&profile), Some(vec![5, 6, 7]));
    let profile = [
        (1, 500_000),
        (2, 10),
        (3, 10),
        (4, 500_000),
        (5, 10),
        (6, 10),
    ];
    assert_eq!(plan_bounded_segment_merge(&profile), Some(vec![2, 3]));

    // Converged: only big segments, or isolated smalls between bigs.
    let profile = [(1, 300_000), (2, 300_000), (3, 300_000)];
    assert_eq!(plan_bounded_segment_merge(&profile), None);
    let profile = [(1, 300_000), (2, 5), (3, 300_000), (4, 5)];
    assert_eq!(plan_bounded_segment_merge(&profile), None);
}

#[test]
fn bounded_merge_plan_defers_disproportionate_tail_rewrites() {
    let profile = [
        (1, 160_000),
        (2, 160_000),
        (3, 160_000),
        (4, 63_385),
        (5, 1_615),
    ];
    assert_eq!(plan_bounded_segment_merge(&profile), None);
    let with_new_sibling = [
        (1, 160_000),
        (2, 160_000),
        (3, 160_000),
        (4, 63_385),
        (5, 1_615),
        (6, 1_615),
    ];
    assert_eq!(
        plan_bounded_segment_merge(&with_new_sibling),
        Some(vec![5, 6])
    );
    assert_eq!(plan_bounded_segment_merge(&[(1, 60_000), (2, 1)]), None);
    assert_eq!(
        plan_bounded_segment_merge(&[(1, 60_000), (2, 1), (3, 1)]),
        Some(vec![2, 3])
    );
    assert_eq!(
        balanced_segment_subrun(&[(1, 1), (2, 1), (3, 100), (4, 1), (5, 1)]),
        Some((0, 2))
    );
    assert_eq!(balanced_segment_subrun(&[(1, 0), (2, 0)]), None);
    assert_eq!(
        balanced_segment_subrun(&[(1, u64::MAX), (2, u64::MAX)]),
        Some((0, 2))
    );
}

#[test]
fn bounded_merge_plan_repeated_appends_have_logarithmic_rewrite_work() {
    let mut profile = Vec::new();
    let mut rewritten_docs = 0_u64;
    let mut next_id = 1_u64;
    for _ in 0..256 {
        profile.push((next_id, 1));
        next_id += 1;
        loop {
            let folds: Vec<_> = profile
                .iter()
                .enumerate()
                .map(|(position, (id, docs))| SegmentFoldProfile {
                    segment_id: *id,
                    file_len: *docs,
                    docid_lo: position as u64,
                    docid_hi: position as u64 + 1,
                    doc_count: 1,
                })
                .collect();
            let runs = plan_capped_balanced_merge_runs(&profile, &folds, u64::MAX);
            if runs.is_empty() {
                break;
            }
            for run in runs {
                let start = profile.iter().position(|(id, _)| *id == run[0]).unwrap();
                let end = start + run.len();
                let docs: u64 = profile[start..end].iter().map(|(_, docs)| docs).sum();
                rewritten_docs += docs;
                profile.splice(start..end, [(next_id, docs)]);
                next_id += 1;
            }
        }
        assert!(profile.len() <= 16, "geometric tail stays bounded");
    }
    assert_eq!(profile.iter().map(|(_, docs)| docs).sum::<u64>(), 256);
    assert!(
        rewritten_docs <= 256 * 16,
        "geometric growth bounds rewritten documents, got {rewritten_docs}"
    );
}

#[test]
fn bounded_merge_plan_byte_cap_preserves_balance() {
    let profile = [(1, 100), (2, 1), (3, 1), (4, 100)];
    let fold_profile: Vec<_> = profile
        .iter()
        .map(|(id, _)| SegmentFoldProfile {
            segment_id: *id,
            file_len: 100,
            docid_lo: *id - 1,
            docid_hi: *id,
            doc_count: 1,
        })
        .collect();
    let cap = 3 * (100 + FOLD_HULL_BYTES_PER_DOCID);
    assert_eq!(
        plan_capped_merge_runs(&fold_profile, cap),
        vec![vec![1, 2, 3]]
    );
    assert_eq!(
        plan_capped_balanced_merge_runs(&profile, &fold_profile, cap),
        vec![vec![2, 3]],
        "cap must not force the large first segment to absorb two tiny inputs"
    );
    assert!(estimated_fold_output_bytes(&fold_profile[1..3]) <= cap);
}

#[test]
fn bounded_merge_plan_recovers_siblings_across_cap_boundaries() {
    let profile = [
        (1, 1_000_000),
        (2, 100),
        (3, 1),
        (4, 1),
        (5, 100),
        (6, 1_000_000),
        (7, 1),
        (8, 1),
    ];
    let folds: Vec<_> = profile
        .iter()
        .map(|(id, _)| SegmentFoldProfile {
            segment_id: *id,
            file_len: 100,
            docid_lo: *id - 1,
            docid_hi: *id,
            doc_count: 1,
        })
        .collect();
    let cap = 2 * (100 + FOLD_HULL_BYTES_PER_DOCID);
    assert_eq!(
        plan_capped_balanced_merge_runs(&profile, &folds, cap),
        vec![vec![3, 4], vec![7, 8]],
        "recover middle siblings and do not starve a later protected-small run"
    );
    assert_eq!(
        plan_capped_balanced_merge_runs(&[(1, 100), (2, 60)], &folds[..2], cap),
        vec![vec![1, 2]],
        "comparable unequal inputs still make geometric progress"
    );
    assert!(
        plan_capped_balanced_merge_runs(&[(1, 0), (2, 0)], &folds[..2], u64::MAX).is_empty(),
        "empty inputs must not repeatedly rewrite a large empty segment"
    );
    assert!(plan_capped_balanced_merge_runs(&[(1, 0), (2, 1)], &folds[..2], u64::MAX).is_empty());
}

#[test]
fn fresh_segment_id_avoids_live_ids_and_zero() {
    let profile = [(1, 1), (2, 1)];
    for _ in 0..64 {
        let id = fresh_segment_id(&profile);
        assert!(id != 0 && id != 1 && id != 2);
    }
}

/// GH #456: the merge planner keeps every run under the byte cap, skips
/// oversized segments, and never emits a single-segment run.
#[test]
fn capped_merge_runs_respect_the_output_byte_cap() {
    // Dense: every segment's hull is exactly its own docids, so the
    // estimate is bytes plus `FOLD_HULL_BYTES_PER_DOCID` per docid.
    let dense = |id: u64, bytes: u64, lo: u64, docs: u64| SegmentFoldProfile {
        segment_id: id,
        file_len: bytes,
        docid_lo: lo,
        docid_hi: lo + docs,
        doc_count: docs,
    };
    let profile = [
        dense(1, 300, 0, 1),
        dense(2, 300, 1, 1),
        dense(3, 300, 2, 1),
        dense(4, 300, 3, 1),
        dense(5, 2_000, 4, 1),
        dense(6, 100, 5, 1),
        dense(7, 100, 6, 1),
    ];
    let h = FOLD_HULL_BYTES_PER_DOCID;
    // 1..=3: 900 + 3h fits exactly; adding 4 -> 1200 + 4h does not.
    // 5 alone: 2000 + h is oversized. 6+7: 200 + 2h fits.
    assert_eq!(
        plan_capped_merge_runs(&profile, 900 + 3 * h),
        vec![vec![1, 2, 3], vec![6, 7]],
        "four 300-byte segments do not fit; segment 5 is oversized; 4 stands alone"
    );
    assert_eq!(
        plan_capped_merge_runs(&profile, u64::MAX),
        vec![vec![1, 2, 3, 4, 5, 6, 7]],
        "an unbounded cap folds everything into one run"
    );
    assert_eq!(
        plan_capped_merge_runs(&profile, 200 + 2 * h),
        vec![vec![6, 7]],
        "only the two 100-byte segments fit together"
    );
    assert!(
        plan_capped_merge_runs(&profile, 100 + h - 1).is_empty(),
        "a cap below every segment folds nothing"
    );
    assert!(plan_capped_merge_runs(&[dense(9, 10, 0, 1)], 1_000).is_empty());
    assert!(plan_capped_merge_runs(&[], 1_000).is_empty());

    // Sparse: two tiny segments a whole lease apart cost the hull, not
    // their bytes — the shape of a session-built archive (#453).
    let lease = 65_536_u64;
    let sparse = [
        dense(1, 3_000, 0, 2),
        dense(2, 3_000, lease, 2),
        dense(3, 3_000, 2 * lease, 2),
    ];
    let hull_two = 6_000 + (lease + 2) * FOLD_HULL_BYTES_PER_DOCID;
    assert!(
        plan_capped_merge_runs(&sparse, hull_two - 1).is_empty(),
        "under the two-lease hull cost nothing folds"
    );
    assert_eq!(
        plan_capped_merge_runs(&sparse, hull_two),
        vec![vec![1, 2]],
        "exactly the two-lease hull folds the first pair and leaves the third"
    );
    assert_eq!(
        plan_capped_merge_runs(&sparse, u64::MAX),
        vec![vec![1, 2, 3]]
    );
}

/// GH #498: no planned fold may hold more documents than a Quill posting
/// list can reopen, however generous the byte cap. The reporter's
/// generation was 7,113,816 documents; a single fold of it failed with
/// "posting validation limit 4194304 exceeded by declared doc_freq".
#[test]
fn capped_merge_runs_never_exceed_the_engine_posting_limit() {
    assert_eq!(
        MAX_FOLD_OUTPUT_DOCS, 4_194_304,
        "the bound must be the limit the engine reports in #498"
    );
    let dense = |id: u64, lo: u64, docs: u64| SegmentFoldProfile {
        segment_id: id,
        file_len: docs,
        docid_lo: lo,
        docid_hi: lo + docs,
        doc_count: docs,
    };
    let docs = 889_227_u64;
    let generation: Vec<_> = (0..8_u64)
        .map(|index| dense(index + 1, index * docs, docs))
        .collect();
    assert!(
        fold_doc_count(&generation) > MAX_FOLD_OUTPUT_DOCS,
        "the whole generation is one fold the engine would refuse"
    );
    let runs = plan_capped_merge_runs(&generation, u64::MAX);
    assert_eq!(runs, vec![vec![1, 2, 3, 4], vec![5, 6, 7, 8]]);
    for run in &runs {
        let folded: Vec<_> = generation
            .iter()
            .filter(|segment| run.contains(&segment.segment_id))
            .copied()
            .collect();
        assert!(fold_doc_count(&folded) <= MAX_FOLD_OUTPUT_DOCS, "{run:?}");
    }

    // The bound is inclusive: exactly the limit folds, one more does not.
    let half = MAX_FOLD_OUTPUT_DOCS / 2;
    assert_eq!(
        plan_capped_merge_runs(&[dense(1, 0, half), dense(2, half, half)], u64::MAX),
        vec![vec![1, 2]]
    );
    assert!(
        plan_capped_merge_runs(&[dense(1, 0, half), dense(2, half, half + 1)], u64::MAX).is_empty()
    );

    // Tombstoned documents keep their postings until compaction, so the
    // bound counts physical documents, not live ones: two segments whose
    // live rows are small but whose physical rows sum past the limit stay
    // apart, including through the comparable-sibling recovery pass.
    let big = 40_000_000_u64;
    let live = [(1, big), (2, 1_000), (3, 1_000), (4, big)];
    let physical = |second: u64| {
        [
            dense(1, 0, big),
            dense(2, big, second),
            dense(3, big + second, second),
            dense(4, big + 2 * second, big),
        ]
    };
    assert!(
        plan_capped_balanced_merge_runs(&live, &physical(half + 1), u64::MAX).is_empty(),
        "a pair past the posting limit must not fold"
    );
    assert_eq!(
        plan_capped_balanced_merge_runs(&live, &physical(half), u64::MAX),
        vec![vec![2, 3]],
        "the same pair within the limit folds"
    );
}

#[test]
fn merge_output_cap_env_parsing_keeps_the_default_on_junk() {
    assert_eq!(
        lexical_merge_max_output_bytes_from(None),
        CASS_LEXICAL_MERGE_MAX_OUTPUT_BYTES_DEFAULT
    );
    assert_eq!(
        lexical_merge_max_output_bytes_from(Some("0")),
        CASS_LEXICAL_MERGE_MAX_OUTPUT_BYTES_DEFAULT
    );
    assert_eq!(
        lexical_merge_max_output_bytes_from(Some("lots")),
        CASS_LEXICAL_MERGE_MAX_OUTPUT_BYTES_DEFAULT
    );
    assert_eq!(
        lexical_merge_max_output_bytes_from(Some(" 268_435_456 ")),
        268_435_456
    );
}

/// GH #456: under a cap that fits only some of the session segments, the
/// end-of-rebuild fold still folds what fits, keeps every document
/// searchable, and never assembles more than the cap in one output.
#[test]
fn force_merge_under_a_byte_cap_folds_partially_and_keeps_every_document() {
    let directory = tempfile::tempdir().expect("bridge index directory");
    let sessions = 6_u64;
    for session in 0..sessions {
        let mut index = QuillCassIndex::open_or_create(directory.path()).expect("open session");
        index
            .add_cass_documents(&[
                sample(&format!("cap{session}"), 0, "capped fold alpha message"),
                sample(&format!("cap{session}"), 1, "capped fold beta message"),
            ])
            .expect("index session documents");
        index.commit().expect("commit session");
    }
    let mut index = QuillCassIndex::open_or_create(directory.path()).expect("reopen");
    let before = index
        .published_segment_fold_profile()
        .expect("fold profile");
    assert!(before.len() >= usize::try_from(sessions).expect("small"));
    // Room for exactly the first two session segments (their hull plus
    // bytes), which is less than any three.
    let cap = estimated_fold_output_bytes(&before[..2]);
    assert!(estimated_fold_output_bytes(&before[..3]) > cap);
    let planned = plan_capped_merge_runs(&before, cap);
    assert!(
        !planned.is_empty() && planned.iter().all(|run| run.len() == 2),
        "the cap admits pairs only: {planned:?}"
    );

    index
        .force_merge_with_output_cap(cap)
        .expect("capped force merge");
    let after = index
        .published_segment_fold_profile()
        .expect("fold profile");
    assert_eq!(
        after.len(),
        before.len() - planned.len(),
        "every planned pair folded into one segment: {before:?} -> {after:?}"
    );
    assert!(
        after.len() > 1,
        "the cap must stop the fold short of a single segment: {after:?}"
    );
    // The estimate is an upper bound on what the engine actually wrote,
    // which is what makes the cap a memory bound: every merged output
    // stays under the cap it was planned for.
    let merged_ids: std::collections::BTreeSet<u64> = after
        .iter()
        .map(|segment| segment.segment_id)
        .filter(|id| !before.iter().any(|segment| segment.segment_id == *id))
        .collect();
    assert_eq!(merged_ids.len(), planned.len());
    assert!(
        after
            .iter()
            .filter(|segment| merged_ids.contains(&segment.segment_id))
            .all(|segment| segment.file_len <= cap),
        "a merged output exceeded the cap {cap} it was planned under: {after:?}"
    );
    assert_eq!(index.doc_count().expect("doc count"), sessions * 2);

    index
        .force_merge_with_output_cap(u64::MAX)
        .expect("uncapped force merge");
    assert_eq!(published_segment_count(&index), 1);
    assert_eq!(index.doc_count().expect("doc count"), sessions * 2);
}

/// The streaming-ingest fold merges one run per call and then honours the
/// cooldown, so a mid-run call costs at most one extra publish.
#[test]
fn fold_largest_small_run_merges_one_run_then_cools_down() {
    let directory = tempfile::tempdir().expect("bridge index directory");
    let sessions = 5_u64;
    for session in 0..sessions {
        let mut index = QuillCassIndex::open_or_create(directory.path()).expect("open session");
        index
            .add_cass_documents(&[sample(&format!("s{session}"), 0, "session alpha")])
            .expect("index session documents");
        index.commit().expect("commit session");
    }
    let mut index = QuillCassIndex::open_or_create(directory.path()).expect("reopen");
    let before = published_segment_count(&index);
    assert!(before >= 5, "each session leaves a segment, got {before}");

    assert!(
        index
            .fold_largest_small_run(1_700_000_000_000)
            .expect("fold")
    );
    let after = published_segment_count(&index);
    assert!(after < before, "{after} segments after folding {before}");
    assert_eq!(index.doc_count().expect("doc count"), sessions);

    // Enough later commits to clear the merge threshold, so only the
    // cooldown can hold the next fold back.
    for late in 0..CASS_MERGE_SEGMENT_THRESHOLD {
        index
            .add_cass_documents(&[sample(&format!("late{late}"), 0, "late session alpha")])
            .expect("index late documents");
        index.commit().expect("commit late");
    }
    let grown = published_segment_count(&index);
    assert!(
        grown >= CASS_MERGE_SEGMENT_THRESHOLD,
        "got {grown} segments"
    );
    assert!(
        !index
            .fold_largest_small_run(1_700_000_000_001)
            .expect("fold inside cooldown"),
        "a call inside the cooldown must not merge"
    );
    assert_eq!(published_segment_count(&index), grown);
}

/// #441: one writer session per open leases fresh docid blocks, so every
/// session leaves at least one segment the engine's width-tiered merge
/// will never fold. The bounded policy must fold them and keep the corpus
/// searchable.
#[test]
fn bounded_merge_folds_session_segments_into_one() {
    let directory = tempfile::tempdir().expect("bridge index directory");
    let sessions = 5_u64;
    for session in 0..sessions {
        let mut index = QuillCassIndex::open_or_create(directory.path()).expect("open session");
        index
            .add_cass_documents(&[
                sample(&format!("s{session}"), 0, "session zero message alpha"),
                sample(&format!("s{session}"), 1, "session one message beta"),
            ])
            .expect("index session documents");
        index.commit().expect("commit session");
    }
    let mut index = QuillCassIndex::open_or_create(directory.path()).expect("reopen");
    let before = published_segment_count(&index);
    assert!(
        before >= usize::try_from(sessions).expect("small count"),
        "each session must have left at least one segment, got {before}"
    );
    assert_eq!(index.doc_count().expect("doc count"), sessions * 2);

    // Cooldown never elapsed and nothing merged yet -> policy fires.
    let merged = index.optimize_if_idle(1_700_000_000_000).expect("optimize");
    assert!(merged, "five session segments exceed the merge threshold");
    assert_eq!(published_segment_count(&index), 1);
    assert_eq!(index.doc_count().expect("doc count"), sessions * 2);

    // A reader opened on the merged generation still finds every session.
    // The CASS-schema reader is preparsed-only, so go through the CASS
    // parser exactly as the search layer does.
    let reader = index.reader().expect("reader");
    let parser = frankensearch::quill::query::CassQueryParser::new(CASS_SEMANTIC_SCHEMA)
        .expect("CASS query parser");
    let parsed = parser.parse(
        "alpha",
        &frankensearch::quill::query::CassQueryFilters::default(),
    );
    let page =
        search_paginated(&reader, &parsed.query, 100, 0, false).expect("search merged generation");
    assert_eq!(
        page.hits.len(),
        usize::try_from(sessions).expect("small count")
    );

    // Converged: another pass inside the cooldown is a no-op.
    assert!(!index.optimize_if_idle(1_700_000_000_001).expect("optimize"));

    // A further session appends a segment; force_merge folds it back.
    index
        .add_cass_documents(&[sample("late", 0, "late session alpha")])
        .expect("index late session");
    index.commit().expect("commit late session");
    assert!(published_segment_count(&index) >= 2);
    index.force_merge().expect("force merge");
    assert_eq!(published_segment_count(&index), 1);
    assert_eq!(index.doc_count().expect("doc count"), sessions * 2 + 1);
}
