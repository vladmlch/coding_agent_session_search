use super::liveness::{
    ENGINE_LIVENESS_SAMPLE_INTERVAL, EngineLivenessProbe, EngineLivenessSample,
    EngineLivenessTracker, directory_footprint, watch_target_footprint,
};
use super::reader::{
    QUILL_RETIREMENT_RECEIPT_SUFFIX, QUILL_SEGMENT_FILE_PREFIX, QUILL_SEGMENT_FILE_SUFFIX,
};
use super::*;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// Segments referenced by the published MANIFEST, read through the same
/// reader the merge policy consults.
pub(super) fn published_segment_count(index: &QuillCassIndex) -> usize {
    index.segment_count()
}

#[test]
fn fuel_budget_override_parses_positive_integers_only() {
    assert_eq!(query_fuel_budget_override(None), None);
    assert_eq!(query_fuel_budget_override(Some("")), None);
    assert_eq!(query_fuel_budget_override(Some("0")), None);
    assert_eq!(query_fuel_budget_override(Some("lots")), None);
    assert_eq!(
        query_fuel_budget_override(Some(" 25000000 ")),
        Some(25_000_000)
    );
    assert_eq!(
        query_fuel_budget_override(Some("25_000_000")),
        Some(25_000_000)
    );
}

#[test]
fn cass_config_publishes_only_on_explicit_commit() {
    let config = cass_quill_config();
    assert_eq!(
        config.max_visibility_lag_ms,
        u64::MAX,
        "#440/#441: the engine must never publish a MANIFEST underneath the rebuild checkpoint"
    );
    assert!(
        config.validate().is_ok(),
        "the CASS config must pass engine validation"
    );
    let default = QuillConfig::default();
    assert_eq!(
        config.tier_fanout,
        usize::MAX,
        "#498: the engine's unbounded tier merge must never run under CASS"
    );
    assert_eq!(
        config.scribe_shard_budget_bytes,
        default.scribe_shard_budget_bytes
    );
    assert_eq!(config.query_fuel_budget, default.query_fuel_budget);
}

/// Segments and documents published after `commits` single-document
/// commits under `config`, pinned to one deterministic ingest shard so
/// every sealed segment is adjacent in one docid lease — the shape the
/// engine's tier merge folds.
fn segments_after_single_document_commits(commits: usize, tier_fanout: usize) -> (usize, u64) {
    let directory = tempfile::tempdir().expect("bridge index directory");
    let config = QuillConfig {
        deterministic_ingest: true,
        tier_fanout,
        ..cass_quill_config()
    };
    let mut index =
        QuillCassIndex::open_or_create_with_config(directory.path(), &config).expect("open");
    for commit in 0..commits {
        let msg_idx = u64::try_from(commit).expect("small count");
        index
            .add_cass_documents(&[sample("tier", msg_idx, "tier fold message")])
            .expect("index document");
        index.commit().expect("commit");
    }
    (
        published_segment_count(&index),
        index.doc_count().expect("doc count"),
    )
}

/// GH #498: CASS commits never run the engine's own tier merge, while the
/// engine default folds the same commits. That merge has no upper bound
/// on its top tier; CASS's capped planners are the only merge authority.
#[test]
fn cass_commits_never_run_the_engine_tier_merge() {
    let fanout = frankensearch::quill::config::DEFAULT_TIER_FANOUT;
    let commits = fanout + 1;
    let (engine_segments, engine_docs) = segments_after_single_document_commits(commits, fanout);
    assert!(
        engine_segments < commits,
        "control: the engine default must tier-merge {commits} adjacent segments, left {engine_segments}"
    );
    let (cass_segments, cass_docs) =
        segments_after_single_document_commits(commits, cass_quill_config().tier_fanout);
    assert_eq!(
        cass_segments, commits,
        "a CASS commit must publish its segment without an engine fold"
    );
    let expected_docs = u64::try_from(commits).expect("small count");
    assert_eq!(engine_docs, expected_docs);
    assert_eq!(cass_docs, expected_docs);
}

// GH #446: the sampler's decision core — first sample is a baseline, then
// footprint growth OR a real CPU advance counts as engine work; a frozen
// engine (nothing written, no CPU) never ticks.
#[test]
fn engine_liveness_tracker_ticks_on_footprint_or_cpu_advance_only() {
    let sample = |bytes: u64, files: u64, cpu_ms: u64| EngineLivenessSample {
        directory_footprint: (bytes, files),
        process_cpu_time: Some(Duration::from_millis(cpu_ms)),
    };
    let mut tracker = EngineLivenessTracker::default();
    assert!(!tracker.observe(sample(100, 2, 1_000)), "baseline");
    assert!(
        !tracker.observe(sample(100, 2, 1_010)),
        "wedged: 10ms CPU, no writes"
    );
    assert!(
        tracker.observe(sample(4_096, 3, 1_012)),
        "segment file written"
    );
    assert!(!tracker.observe(sample(4_096, 3, 1_020)), "quiet again");
    assert!(
        tracker.observe(sample(4_096, 3, 1_120)),
        "CPU-bound merge stretch"
    );
    assert!(
        tracker.observe(sample(2_048, 3, 1_121)),
        "size-neutral rewrite shrank"
    );
    assert!(tracker.observe(sample(2_048, 2, 1_122)), "file count moved");

    // Without CPU telemetry only the footprint can prove progress.
    let mut tracker = EngineLivenessTracker::default();
    let no_cpu = |bytes: u64| EngineLivenessSample {
        directory_footprint: (bytes, 1),
        process_cpu_time: None,
    };
    assert!(!tracker.observe(no_cpu(1)));
    assert!(!tracker.observe(no_cpu(1)));
    assert!(tracker.observe(no_cpu(2)));
}

/// GH #450: a long blocking call that keeps growing a watched file must
/// post liveness ticks so the #258 stall detector does not call it a stall.
#[test]
fn run_with_liveness_ticks_reports_progress_for_a_long_growing_op() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let watched = tmp.path().join("archive.db");
    std::fs::write(&watched, b"seed").expect("seed");
    let ticks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let heartbeat: EngineHeartbeat = {
        let ticks = Arc::clone(&ticks);
        Arc::new(move || {
            ticks.fetch_add(1, Ordering::Relaxed);
        })
    };

    let result = run_with_liveness_ticks(vec![watched.clone()], heartbeat, || {
        // Three sample intervals: one to take the baseline, two that can
        // observe growth.
        for step in 0..3 {
            std::fs::write(&watched, vec![b'x'; 1024 * (step + 1)]).expect("grow");
            std::thread::sleep(ENGINE_LIVENESS_SAMPLE_INTERVAL + Duration::from_millis(250));
        }
        "done"
    });

    assert_eq!(result, "done");
    assert!(
        ticks.load(Ordering::Relaxed) > 0,
        "a growing archive must post at least one liveness tick"
    );
}

/// Dropping a probe ends its sampler at once. It used to sleep out the
/// rest of a whole sample interval first, which put a one-second floor
/// on every storage open and Quill index drop: an empty `cass index`
/// took 2.4 s, all of it in two such waits.
#[test]
fn dropping_a_liveness_probe_does_not_wait_out_the_sample_interval() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let probe = EngineLivenessProbe::spawn(vec![tmp.path().to_path_buf()], Arc::new(|| {}));
    // Let the sampler enter its wait.
    std::thread::sleep(Duration::from_millis(50));
    let started = std::time::Instant::now();
    drop(probe);
    let elapsed = started.elapsed();
    assert!(
        elapsed < ENGINE_LIVENESS_SAMPLE_INTERVAL / 2,
        "dropping the probe took {elapsed:?}"
    );
}

#[test]
fn engine_liveness_sample_reads_real_footprint_and_cpu() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    std::fs::create_dir_all(tmp.path().join("nested")).expect("nested dir");
    std::fs::write(tmp.path().join("a.bin"), [0u8; 10]).expect("a");
    std::fs::write(tmp.path().join("nested").join("b.bin"), [0u8; 5]).expect("b");
    let sample = EngineLivenessSample::capture(&[tmp.path().to_path_buf()]);
    assert_eq!(sample.directory_footprint, (15, 2));
    // GH #450: a watch target may be a plain file (a database and its
    // sidecars) or missing entirely.
    assert_eq!(
        watch_target_footprint(&[
            tmp.path().join("a.bin"),
            tmp.path().join("nested").join("b.bin"),
            tmp.path().join("absent.bin"),
        ]),
        (15, 2),
        "file targets sum their own sizes; a missing target contributes nothing"
    );
    assert_eq!(
        directory_footprint(&tmp.path().join("does-not-exist")),
        (0, 0),
        "an unreadable directory is an empty footprint, never an error"
    );
    if cfg!(unix) {
        assert!(
            sample.process_cpu_time.is_some(),
            "getrusage must work on unix"
        );
    }
}

#[test]
fn quill_reader_validation_ticks_the_installed_heartbeat() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let mut index = QuillCassIndex::open_or_create(tmp.path()).expect("open");
    index
        .add_cass_documents(&[sample("s1", 0, "verified reader work")])
        .expect("accumulate");
    index.commit().expect("commit");
    let ticks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let sink = Arc::clone(&ticks);
    index.set_heartbeat(Some(Arc::new(move || {
        sink.fetch_add(1, Ordering::Relaxed);
    })));
    index.reader().expect("validated reader");
    assert!(ticks.load(Ordering::Relaxed) > 0);
    index.set_heartbeat(None);
    let before = ticks.load(Ordering::Relaxed);
    index.reader().expect("read without heartbeat");
    assert_eq!(ticks.load(Ordering::Relaxed), before);
}

/// Real sink work must reach the heartbeat: accumulate and commit each
/// tick synchronously, and clearing the heartbeat stops the ticks.
#[test]
fn quill_sink_work_ticks_the_installed_heartbeat() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let mut index = QuillCassIndex::open_or_create(tmp.path()).expect("open");
    let ticks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let sink = Arc::clone(&ticks);
    index.set_heartbeat(Some(Arc::new(move || {
        sink.fetch_add(1, Ordering::Relaxed);
    })));

    index
        .add_cass_documents(&[sample("s1", 0, "alpha beta"), sample("s1", 1, "gamma")])
        .expect("accumulate");
    let after_accumulate = ticks.load(Ordering::Relaxed);
    assert!(
        after_accumulate >= 1,
        "accumulate must tick: {after_accumulate}"
    );
    index.commit().expect("commit");
    let after_commit = ticks.load(Ordering::Relaxed);
    assert!(after_commit > after_accumulate, "commit must tick");
    index.force_merge().expect("merge");
    let after_merge = ticks.load(Ordering::Relaxed);
    assert!(after_merge > after_commit, "compaction must tick");

    index.set_heartbeat(None);
    index
        .add_cass_documents(&[sample("s2", 0, "delta")])
        .expect("accumulate without heartbeat");
    index.commit().expect("commit without heartbeat");
    assert_eq!(
        ticks.load(Ordering::Relaxed),
        after_merge,
        "a cleared heartbeat must never be invoked again"
    );
    assert_eq!(index.doc_count().expect("doc count"), 3);
}

pub(super) fn sample(source_id: &str, msg_idx: u64, content: &str) -> QuillCassDocument {
    QuillCassDocument {
        agent: "claude".to_owned(),
        workspace: Some("cass".to_owned()),
        workspace_original: Some("cass".to_owned()),
        source_path: format!("/transcripts/{source_id}.jsonl"),
        msg_idx,
        created_at: Some(1_700_000_000),
        title: Some("bridge session".to_owned()),
        content: content.to_owned(),
        source_id: source_id.to_owned(),
        origin_kind: "local".to_owned(),
        origin_host: None,
        conversation_id: Some(1),
    }
}

/// The bridge must drive a full write/commit/read cycle from sync code.
///
/// This is the claim the whole module exists to support: cass's
/// synchronous lexical layer can operate an async engine without any
/// caller becoming `async`.
#[test]
fn bridge_round_trips_documents_without_an_async_caller() {
    let directory = tempfile::tempdir().expect("bridge index directory");
    let mut index = QuillCassIndex::open_or_create(directory.path()).expect("open or create");
    index
        .add_cass_documents(&[
            sample("alpha", 0, "the borrow checker rejected this lifetime"),
            sample("beta", 1, "tokenizer throughput regressed"),
        ])
        .expect("index documents");
    index.commit().expect("commit");
    assert_eq!(index.doc_count().expect("doc count"), 2);
}

/// Counting documents on a writer reads its verified snapshot. A reader
/// open re-hashes every segment, so a no-op `cass index` that asked for
/// the count several times read the whole lexical index each time.
#[test]
fn writer_doc_count_tracks_commits_without_opening_a_reader() {
    let directory = tempfile::tempdir().expect("bridge index directory");
    let mut index = QuillCassIndex::open_or_create(directory.path()).expect("open or create");
    index
        .add_cass_documents(&[sample("alpha", 0, "first published document")])
        .expect("index one document");
    index.commit().expect("commit");
    let opens = reader_open_count();
    assert_eq!(index.doc_count().expect("doc count"), 1);
    index
        .add_cass_documents(&[sample("beta", 1, "second published document")])
        .expect("index another document");
    assert_eq!(
        index.doc_count().expect("doc count before commit"),
        1,
        "an uncommitted document is not published"
    );
    index.commit().expect("commit");
    assert_eq!(index.doc_count().expect("doc count"), 2);
    assert_eq!(reader_open_count(), opens, "doc_count opened a reader");
    // A fresh reader agrees with the writer's count.
    assert_eq!(
        index.reader().expect("reader").doc_count().expect("count"),
        2
    );
}

/// A reentrant bridge call must not re-enter `block_on` on one runtime.
///
/// The thread-local slot is emptied while a future is in flight precisely
/// so this nests instead of panicking; without that, any Quill call made
/// from inside another Quill call would abort.
#[test]
fn bridge_nests_without_reentering_one_runtime() {
    let directory = tempfile::tempdir().expect("bridge index directory");
    let index = QuillCassIndex::open_or_create(directory.path()).expect("open or create");
    let nested = drive(|_cx| async {
        // A second bridge call while the first future is being driven.
        index.doc_count()
    });
    assert_eq!(nested.expect("nested doc count"), 0);
}

#[test]
fn lexical_round_trip_restores_full_and_restricted_callers() {
    let runtime = RuntimeBuilder::current_thread()
        .build()
        .expect("outer runtime");
    runtime.block_on(async {
        let parent = Cx::current().expect("outer runtime context");
        for restricted in [false, true] {
            let restriction =
                restricted.then(|| Cx::push_restriction(asupersync::cx::CapMask::none()));
            let caller = Cx::current().expect("caller context");
            let assert_caller = || {
                let current = Cx::current().expect("restored caller context");
                assert_eq!(current.task_id(), caller.task_id());
                assert_eq!(current.region_id(), caller.region_id());
                assert_eq!(current.capabilities(), caller.capabilities());
                if restricted {
                    assert!(!current.capabilities().spawn);
                    assert!(!current.capabilities().io);
                }
            };
            let directory = tempfile::tempdir().expect("bridge index directory").keep();
            eprintln!(
                "retained lexical caller-restoration fixture: {}",
                directory.display()
            );
            let mut index = QuillCassIndex::open_or_create(&directory).expect("open index");
            assert_caller();
            let content = "migration preserves the caller and searchable content";
            index
                .add_cass_documents(&[sample("bridge", 0, content)])
                .expect("stage document");
            index.commit().expect("publish document");
            assert_caller();

            let nested = drive(|_cx| async {
                let mapping = Cx::current().expect("nested bridge context");
                let count = index.doc_count().expect("nested published count");
                let restored = Cx::current().expect("restored nested bridge context");
                assert_eq!(restored.task_id(), mapping.task_id());
                assert_eq!(restored.region_id(), mapping.region_id());
                assert_eq!(restored.capabilities(), mapping.capabilities());
                count
            });
            assert_eq!(nested, 1);
            assert_caller();
            let reader = index.reader().expect("published reader");
            let parser = frankensearch::quill::query::CassQueryParser::new(CASS_SEMANTIC_SCHEMA)
                .expect("CASS query parser");
            let parsed = parser.parse(
                "migration",
                &frankensearch::quill::query::CassQueryFilters::default(),
            );
            let page = search_paginated(&reader, &parsed.query, 10, 0, true)
                .expect("query published document");
            assert_eq!(page.hits.len(), 1);
            assert_eq!(page.total_count, Some(1));
            // Content is searchable but not stored in CASS_SEMANTIC_SCHEMA.
            // Exercise stored-field hydration with the fixture's title.
            assert_eq!(
                stored_text(
                    &reader,
                    QuillCassFields::compiled().title,
                    page.hits[0].global_docid
                )
                .expect("hydrate title"),
                Some("bridge session".to_owned())
            );
            assert_caller();
            drop(reader);
            drop(index);
            assert_caller();
            drop(restriction);
            let restored = Cx::current().expect("restored outer runtime context");
            assert_eq!(restored.task_id(), parent.task_id());
            assert_eq!(restored.region_id(), parent.region_id());
            assert_eq!(restored.capabilities(), parent.capabilities());
        }
    });
    assert!(shutdown_driver(), "Quill bridge runtime must drain");
    assert!(runtime.shutdown_timeout(Duration::from_secs(30)));
}

/// #440: the engine must not publish a MANIFEST on its own visibility
/// cadence. With the engine default (1 s) a second ingest call after a
/// one-second pause seals and publishes everything staged so far, which
/// is precisely how a staging index ran ahead of the rebuild checkpoint.
#[test]
fn engine_publishes_only_on_explicit_commit() {
    let directory = tempfile::tempdir().expect("bridge index directory");
    let mut index = QuillCassIndex::open_or_create(directory.path()).expect("open or create");
    index
        .add_cass_documents(&[sample("alpha", 0, "first staged batch")])
        .expect("stage first batch");
    std::thread::sleep(std::time::Duration::from_millis(1_200));
    index
        .add_cass_documents(&[sample("alpha", 1, "second staged batch")])
        .expect("stage second batch");
    assert_eq!(
        index.doc_count().expect("published doc count"),
        0,
        "nothing may be published before CASS commits"
    );
    index.commit().expect("commit");
    assert_eq!(index.doc_count().expect("published doc count"), 2);
}

/// #440: a resumed rebuild re-adds documents the published authority may
/// already hold. The upsert path must converge to one live document per
/// identity where a plain add is (correctly) refused as a duplicate.
#[test]
fn upsert_refs_converge_where_add_refuses_duplicates() {
    let directory = tempfile::tempdir().expect("bridge index directory");
    let mut index = QuillCassIndex::open_or_create(directory.path()).expect("open or create");
    let published = [
        sample("alpha", 0, "published before the interruption"),
        sample("alpha", 1, "also published before the interruption"),
    ];
    index
        .add_cass_documents(&published)
        .expect("index published documents");
    index.commit().expect("publish");

    let duplicate = index.add_cass_documents(&published[..1]);
    assert!(
        duplicate
            .as_ref()
            .is_err_and(|error| error.to_string().contains("duplicate live document id")),
        "a plain re-add of a live identity must be refused: {duplicate:?}"
    );
    assert!(
        duplicate.as_ref().is_err_and(|error| {
            error
                .downcast_ref::<frankensearch::quill::QuillIndexError>()
                .is_some()
        }),
        "ingest errors must retain the typed engine cause: {duplicate:?}"
    );

    assert_eq!(index.doc_count().expect("unchanged published count"), 2);

    let replay = [
        published[0].clone(),
        published[1].clone(),
        sample("alpha", 2, "new after the interruption"),
    ];
    let refs: Vec<_> = replay.iter().map(QuillCassDocument::as_ref).collect();
    index
        .upsert_cass_document_refs(&refs)
        .expect("upsert converges on the published identities");
    index.commit().expect("publish replay");
    assert_eq!(index.doc_count().expect("doc count"), 3);
}

/// GH #457: the MANIFEST-only live count must track what the engine
/// serves — every sealed document before any deletion, zero once
/// `delete_all` publishes the hollow successor — without a reader open.
#[test]
fn manifest_live_doc_count_tracks_served_documents_without_a_reader() {
    let directory = tempfile::tempdir().expect("bridge index directory");
    assert_eq!(
        manifest_live_doc_count(directory.path()),
        None,
        "no MANIFEST has been published yet"
    );
    let mut index = QuillCassIndex::open_or_create(directory.path()).expect("open");
    index
        .add_cass_documents(&[
            sample("hollow", 0, "alpha"),
            sample("hollow", 1, "beta"),
            sample("hollow", 2, "gamma"),
        ])
        .expect("index documents");
    index.commit().expect("commit");

    let live = manifest_live_doc_count(directory.path()).expect("published manifest");
    assert_eq!(live.live_docs, 3);
    assert_eq!(live.sealed_docs, 3);
    assert_eq!(live.tombstones, 0);
    assert!(live.segments >= 1, "a commit seals at least one segment");
    assert_eq!(live.source, QuillManifestSource::Current);
    assert_eq!(live.live_docs, index.doc_count().expect("engine count"));

    index.delete_all().expect("publish the hollow successor");
    let hollow = manifest_live_doc_count(directory.path()).expect("hollow manifest");
    assert_eq!(hollow.live_docs, 0);
    assert_eq!(hollow.segments, 0);
    assert!(
        hollow.generation > live.generation,
        "the hollow successor is a later generation"
    );
    assert_eq!(index.doc_count().expect("engine count"), 0);
}

/// mknmt: the live count names the slot it was read from. A corrupt
/// current MANIFEST is read through `MANIFEST.prev`, which lacks the
/// newest publish, and a writer open only retires the corrupt slot:
/// without durability sidecars nothing on disk restores that publish.
#[test]
fn manifest_source_names_the_slot_a_read_serves() {
    let directory = tempfile::tempdir().expect("bridge index directory");
    let mut index = QuillCassIndex::open_or_create(directory.path()).expect("open");
    index
        .add_cass_documents(&[sample("slot", 0, "alpha")])
        .expect("first batch");
    index.commit().expect("first publish");
    index
        .add_cass_documents(&[sample("slot", 1, "beta")])
        .expect("second batch");
    index.commit().expect("second publish");
    drop(index);
    let current = manifest_live_doc_count(directory.path()).expect("current slot");
    assert_eq!(current.source, QuillManifestSource::Current);
    assert_eq!(current.live_docs, 2);

    let manifest = directory.path().join(QUILL_INDEX_MARKER);
    let mut bytes = std::fs::read(&manifest).expect("read MANIFEST");
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(&manifest, &bytes).expect("corrupt MANIFEST");
    let corrupt = manifest_live_doc_count(directory.path()).expect("previous slot");
    assert_eq!(
        corrupt.source,
        QuillManifestSource::PreviousAfterCorruptCurrent
    );
    assert!(corrupt.generation < current.generation);
    assert_eq!(
        corrupt.live_docs, 1,
        "MANIFEST.prev lacks the newest publish"
    );

    drop(QuillCassIndex::open_or_create(directory.path()).expect("writer open"));
    let retired = manifest_live_doc_count(directory.path()).expect("previous slot");
    assert_eq!(
        retired.source,
        QuillManifestSource::PreviousAfterMissingCurrent,
        "a writer open retires the corrupt slot; it cannot restore the newest publish"
    );
    assert_eq!(retired.live_docs, 1);
}

/// GH #518 (frankensearch#41): a two-word query's dictionary work grows
/// with the segment count, not its square. Quill 0.3.4 (cass 0.10.0)
/// re-charged one dictionary unit per segment for every cached term
/// statistic, so each segment of each of the query's eight term streams
/// (two words x title, content and both prefix fields) paid for every
/// segment: about 8 x 120 x 120 = 115,200 units here, and the whole
/// 10,000,000 in the report. The budget below sits far under that and far
/// over a linear charge.
#[test]
fn gh518_two_word_query_fuel_is_linear_in_segment_count() {
    const SEGMENTS: usize = 120;
    const BUDGET: u64 = 40_000;
    let directory = tempfile::tempdir().expect("bridge index directory");
    let config = QuillConfig {
        deterministic_ingest: true,
        ..cass_quill_config()
    };
    let mut index =
        QuillCassIndex::open_or_create_with_config(directory.path(), &config).expect("open");
    for session in 0..SEGMENTS {
        index
            .add_cass_documents(&[sample(&format!("gh518-{session}"), 0, "git fixture setup")])
            .expect("index document");
        index.commit().expect("commit");
    }
    assert_eq!(published_segment_count(&index), SEGMENTS);

    let open_with_budget = |query_fuel_budget: u64| {
        let path = directory.path().to_path_buf();
        drive(|cx| async move {
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
        })
        .expect("open reader")
    };
    let parser = frankensearch::quill::query::CassQueryParser::new(CASS_SEMANTIC_SCHEMA)
        .expect("CASS query parser");
    let query = parser
        .parse(
            "git fixture",
            &frankensearch::quill::query::CassQueryFilters::default(),
        )
        .query;

    let page = search_paginated(&open_with_budget(BUDGET), &query, 4, 0, false)
        .expect("a two-word query over 120 segments fits a linear budget");
    assert_eq!(page.hits.len(), 4);

    // Control: this reader meters the query, so the pass above is the
    // budget holding rather than metering being off.
    let error = search_paginated(&open_with_budget(10), &query, 4, 0, false)
        .expect_err("ten units cannot cover 120 segments");
    assert!(is_query_fuel_exhausted(&error), "{error:#}");
}

/// Fuel exhaustion is recognised by the engine's own error text, anywhere
/// in the cause chain, and only that text (GH #441 degrade path).
#[test]
fn query_fuel_exhaustion_is_recognised_through_the_cause_chain() {
    let inner = anyhow!("scalar Quill query fuel exhausted after 10000000/10000000 units");
    let wrapped = inner.context("executing a Quill lexical query");
    assert!(is_query_fuel_exhausted(&wrapped));
    assert!(!is_query_fuel_exhausted(&anyhow!(
        "opening the Quill CASS reader: manifest missing"
    )));
}

/// GH #499: the invariant classifier is keyed to the engine's own Display
/// text, so an upstream rewording fails here instead of quietly turning a
/// deterministic failure back into a retryable one. Fuel exhaustion,
/// cancellation, and reader faults must stay outside the class.
#[test]
fn cursor_invariant_failure_is_recognised_from_the_engine_display() {
    use frankensearch::quill::QuillIndexError;
    use frankensearch::quill::argus::ArgusError;

    let engine_error = |error: ArgusError| {
        anyhow!(
            "executing a Quill lexical query: {}",
            QuillIndexError::from(error)
        )
    };
    let invariant = engine_error(ArgusError::CursorInvariant(
        "Boolean children belong to different segment domains",
    ));
    assert!(is_engine_invariant_failure(&invariant));
    assert!(!is_query_fuel_exhausted(&invariant));

    let fuel = engine_error(ArgusError::QueryFuelExhausted {
        budget: 10,
        consumed: 10,
        segments_touched: 1,
        dictionary_blocks: 0,
        posting_blocks: 10,
        position_docs: 0,
    });
    assert!(is_query_fuel_exhausted(&fuel));
    assert!(!is_engine_invariant_failure(&fuel));
    assert!(!is_engine_invariant_failure(&engine_error(
        ArgusError::QueryCancelled { phase: "collect" }
    )));
    assert!(!is_engine_invariant_failure(&anyhow!(
        "opening the Quill CASS reader: manifest missing"
    )));
}

/// GH #441: an archive only ever appends, so tombstone-driven compaction
/// never fires and every commit seals a segment the engine's own tier
/// policy does not fold (measured here: 40 commits → 40 segments with the
/// production configuration alone). What bounds growth in production is
/// the post-run maintenance step `cass index` calls after every
/// incremental run (`optimize_if_idle`) — so that is the invariant this
/// pins: after forty append-only commits, one maintenance pass folds the
/// generation well below the merge threshold and keeps every document.
/// No-claim: this proves consolidation happens, not its cost.
#[test]
fn post_run_maintenance_bounds_segment_growth_on_append_only_commits() {
    let directory = tempfile::tempdir().expect("bridge index directory");
    let mut index = QuillCassIndex::open_or_create(directory.path()).expect("open or create");
    let commits = 40_u64;
    for commit in 0..commits {
        let batch: Vec<QuillCassDocument> = (0..8)
            .map(|slot| {
                let source = format!("session-{commit}");
                sample(
                    &source,
                    slot,
                    &format!("commit {commit} message {slot} about borrow checker lifetimes"),
                )
            })
            .collect();
        index.add_cass_documents(&batch).expect("index batch");
        index.commit().expect("commit batch");
    }
    let before = published_segment_count(&index);
    assert!(
        before >= CASS_MERGE_SEGMENT_THRESHOLD,
        "precondition: append-only commits must leave unfolded segments (got {before})"
    );

    let merged = index.optimize_if_idle(1_700_000_000_000).expect("optimize");
    assert!(
        merged,
        "the post-run maintenance pass must fold {before} segments"
    );
    let after = published_segment_count(&index);
    assert!(
        after < CASS_MERGE_SEGMENT_THRESHOLD,
        "maintenance left {after} segments (threshold {CASS_MERGE_SEGMENT_THRESHOLD})"
    );
    assert_eq!(index.doc_count().expect("doc count"), commits * 8);
}

/// WS-B.1a: the metadata-only segment-file count that `status`/`doctor`
/// report must track the live segment count as an upper bound, without
/// opening the engine. Positive observable: after a run of append-only
/// commits the file count is at least the reader's live count and grows
/// with it; after a merge it is still at least the (now small) live
/// count. Planted negative: a directory without a MANIFEST is `None`,
/// never a fake zero. No-claim: this does not pin when the engine removes
/// folded segment files, only that the bound holds.
#[test]
fn segment_file_count_bounds_the_live_segment_count_without_opening_the_engine() {
    let directory = tempfile::tempdir().expect("bridge index directory");
    assert_eq!(
        segment_file_count(directory.path()),
        None,
        "a directory without a MANIFEST is not a Quill index"
    );

    let mut index = QuillCassIndex::open_or_create(directory.path()).expect("open or create");
    for commit in 0..6_u64 {
        index
            .add_cass_documents(&[sample(
                &format!("session-{commit}"),
                0,
                "segment file count fixture",
            )])
            .expect("index batch");
        index.commit().expect("commit batch");
    }
    let live = published_segment_count(&index);
    let files = segment_file_count(directory.path()).expect("published index");
    assert!(
        files >= live && live >= 2,
        "segment files ({files}) must bound the live segment count ({live})"
    );

    index.force_merge().expect("force merge");
    let live_after = published_segment_count(&index);
    let files_after = segment_file_count(directory.path()).expect("published index");
    assert_eq!(live_after, 1);
    assert!(
        files_after >= live_after,
        "segment files ({files_after}) must still bound the live count ({live_after})"
    );
}

/// #453: the byte split behind doctor's full-rebuild headroom. After a
/// merge the folded inputs are still on disk but no longer referenced by
/// the MANIFEST; they must land in `retired_bytes`, never in
/// `live_bytes`, and the two must add up to the directory's regular
/// files.
#[test]
fn quill_directory_footprint_splits_manifest_referenced_bytes_from_retired_inputs() {
    let directory = tempfile::tempdir().expect("bridge index directory");
    assert_eq!(
        quill_directory_footprint(directory.path()),
        None,
        "a directory without a MANIFEST is not a Quill index"
    );

    let mut index = QuillCassIndex::open_or_create(directory.path()).expect("open or create");
    for commit in 0..4_u64 {
        index
            .add_cass_documents(&[sample(
                &format!("session-{commit}"),
                0,
                "footprint fixture with a few distinct tokens",
            )])
            .expect("index batch");
        index.commit().expect("commit batch");
    }
    let before = quill_directory_footprint(directory.path()).expect("published index");
    assert_eq!(before.retired_bytes, 0, "nothing is folded yet: {before:?}");
    assert_eq!(before.retired_segment_files, 0);
    assert!(before.live_bytes > 0);

    index.force_merge().expect("force merge");
    assert_eq!(published_segment_count(&index), 1);
    let after = quill_directory_footprint(directory.path()).expect("published index");
    let files_after = segment_file_count(directory.path()).expect("published index");
    assert_eq!(
        after.retired_segment_files,
        files_after - 1,
        "every segment file but the merge output is retired: {after:?}"
    );
    assert!(after.retired_bytes > 0, "{after:?}");

    // The split is exhaustive over the directory's regular files.
    let regular_bytes: u64 = std::fs::read_dir(directory.path())
        .expect("read index directory")
        .filter_map(Result::ok)
        .filter_map(|entry| entry.metadata().ok())
        .filter(|metadata| metadata.is_file())
        .map(|metadata| metadata.len())
        .sum();
    assert_eq!(after.live_bytes + after.retired_bytes, regular_bytes);

    // The live figure is exactly what the MANIFEST references plus the
    // non-segment bookkeeping files.
    let manifest = frankensearch::quill::load_manifest_pair(directory.path())
        .expect("load manifest")
        .manifest;
    let referenced: u64 = manifest
        .segments
        .iter()
        .map(|segment| segment.file_len)
        .sum();
    let bookkeeping: u64 = std::fs::read_dir(directory.path())
        .expect("read index directory")
        .filter_map(Result::ok)
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            !(name.starts_with(QUILL_SEGMENT_FILE_PREFIX)
                && (name.ends_with(QUILL_SEGMENT_FILE_SUFFIX)
                    || name.ends_with(QUILL_RETIREMENT_RECEIPT_SUFFIX)))
        })
        .filter_map(|entry| entry.metadata().ok())
        .filter(|metadata| metadata.is_file())
        .map(|metadata| metadata.len())
        .sum();
    assert_eq!(after.live_bytes, referenced + bookkeeping);
}
