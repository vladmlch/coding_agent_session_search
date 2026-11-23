use super::super::*;
use super::helpers::*;
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use tempfile::TempDir;

#[test]
fn open_storage_retryable_classifier_reads_anyhow_error_chain() {
    let err = anyhow::anyhow!("database is locked by another indexer")
        .context("opening canonical archive read-only before index");
    assert!(
        anyhow_chain_indicates_retryable_storage_contention(&err),
        "retryable storage contention can be hidden below contextual anyhow wrappers: {err:#}"
    );
}

#[test]
fn open_storage_retryable_classifier_rejects_corruption() {
    let err = anyhow::anyhow!("database disk image is malformed")
        .context("opening canonical archive read-only before index");
    assert!(
        !anyhow_chain_indicates_retryable_storage_contention(&err),
        "corruption must stay on the fail-closed archive-health path: {err:#}"
    );
}

#[test]
fn headroom_probe_uses_existing_ancestor_for_missing_data_dir() {
    let tmp = TempDir::new().unwrap();
    let missing_data_dir = tmp.path().join("missing").join("cass-data");
    let missing_db_path = missing_data_dir.join("agent_search.db");

    assert_eq!(
        existing_headroom_probe_paths(&missing_data_dir, &missing_db_path),
        vec![tmp.path().to_path_buf()]
    );
}

#[test]
fn authoritative_rebuild_headroom_counts_existing_lexical_index() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let db_path = data_dir.join("agent_search.db");
    // Fixtures must clear INDEX_MIN_FREE_SPACE_BYTES (512 MiB), otherwise the
    // floor dominates both sides and the lexical contribution is invisible.
    // An unmigrated 640 MiB db is charged 640 + 40 MiB. Sparse `set_len`
    // keeps this cheap.
    std::fs::File::create(&db_path)
        .unwrap()
        .set_len(640 * 1024 * 1024)
        .unwrap();

    let db_bytes = 640 * 1024 * 1024;
    let required = || {
        required_index_headroom_bytes(
            &data_dir,
            &db_path,
            IndexStorageHeadroomRequirement::AuthoritativeLexicalRebuild,
        )
    };

    // No live generation yet: the rebuild is sized as the db bundle.
    assert_eq!(
        required(),
        expected_rebuild_requirement(db_bytes, db_bytes, 0, true)
    );

    // With a live index, the rebuild is sized by it, and a larger index
    // raises the requirement by 9/4 of the difference (nw61n).
    let index_dir = data_dir.join(LEXICAL_INDEX_ROOT_DIR).join("seg");
    std::fs::create_dir_all(&index_dir).unwrap();
    let store = std::fs::File::create(index_dir.join("0.store")).unwrap();
    store.set_len(200 * 1024 * 1024).unwrap();
    let with_index = required();
    assert_eq!(
        with_index,
        expected_rebuild_requirement(db_bytes, 200 * 1024 * 1024, 0, true)
    );
    store.set_len(400 * 1024 * 1024).unwrap();
    let with_larger_index = required();
    assert_eq!(
        with_larger_index - with_index,
        200 * 1024 * 1024 * 9 / 4,
        "lexical index size must raise the rebuild headroom requirement"
    );
}

#[test]
fn incremental_headroom_ignores_lexical_index_size() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let db_path = data_dir.join("agent_search.db");
    std::fs::write(&db_path, vec![0u8; 4096]).unwrap();
    let index_dir = data_dir.join(LEXICAL_INDEX_ROOT_DIR);
    std::fs::create_dir_all(&index_dir).unwrap();
    std::fs::write(index_dir.join("big.store"), vec![0u8; 1024 * 1024]).unwrap();

    // Incremental startup does not rewrite the lexical index, so its floor
    // stays the flat minimum.
    assert_eq!(
        required_index_headroom_bytes(
            &data_dir,
            &db_path,
            IndexStorageHeadroomRequirement::IncrementalStartup
        ),
        INDEX_MIN_FREE_SPACE_BYTES
    );
}

/// Build a real Quill index at the data dir's expected lexical path with
/// `rounds` single-document commits, then fold them into one segment so
/// the folded inputs sit on disk unreferenced by the MANIFEST (#453).
fn plant_merged_quill_index(data_dir: &Path, rounds: u64) -> PathBuf {
    let (index_path, mut index) = plant_unmerged_quill_index(data_dir, rounds);
    index.force_merge().expect("force merge");
    assert_eq!(
        index.segment_count(),
        1,
        "merge must leave one live segment"
    );
    index_path
}

/// Build a real Quill index at the data dir's expected lexical path with
/// `rounds` single-document commits, left unmerged.
fn plant_unmerged_quill_index(
    data_dir: &Path,
    rounds: u64,
) -> (PathBuf, crate::search::quill_bridge::QuillCassIndex) {
    use crate::search::quill_bridge::QuillCassIndex;
    use frankensearch::quill::cass::CassDocument;

    let index_path = crate::search::tantivy::expected_index_dir(data_dir);
    std::fs::create_dir_all(&index_path).unwrap();
    let mut index = QuillCassIndex::open_or_create(&index_path).expect("open or create");
    for round in 0..rounds {
        index
            .add_cass_documents(&[CassDocument {
                agent: "claude".to_owned(),
                workspace: Some("cass".to_owned()),
                workspace_original: Some("cass".to_owned()),
                source_path: format!("/transcripts/session-{round}.jsonl"),
                msg_idx: 0,
                created_at: Some(1_700_000_000),
                title: Some("footprint fixture".to_owned()),
                content: format!("footprint fixture round {round} with distinct tokens"),
                source_id: format!("session-{round}"),
                origin_kind: "local".to_owned(),
                origin_host: None,
                conversation_id: Some(1),
            }])
            .expect("index batch");
        index.commit().expect("commit batch");
    }
    (index_path, index)
}

#[test]
fn streaming_commit_interval_is_paced_by_commit_cost() {
    let base = Duration::from_secs(5);
    assert_eq!(
        paced_streaming_commit_interval(base, Duration::ZERO),
        base,
        "cheap commits keep the base cadence"
    );
    assert_eq!(
        paced_streaming_commit_interval(base, Duration::from_millis(900)),
        base
    );
    assert_eq!(
        paced_streaming_commit_interval(base, Duration::from_secs(10)),
        Duration::from_secs(40),
        "a 10 s publish is followed by at least 40 s of ingest"
    );
    assert_eq!(
        paced_streaming_commit_interval(base, Duration::from_secs(3600)),
        STREAMING_COMMIT_MAX_PACED_INTERVAL
    );
    assert_eq!(
        paced_streaming_commit_interval(Duration::from_secs(600), Duration::from_secs(10)),
        Duration::from_secs(600),
        "an operator's longer base interval still wins"
    );
}

/// A long streaming catch-up used to fold segments only after the run
/// ended, so every periodic publish re-verified an ever-growing pile. The
/// consumer's commit now folds the small tail too, at most once per
/// `STREAMING_FOLD_ATTEMPT_INTERVAL`.
#[test]
fn streaming_commit_folds_small_segments_at_most_once_per_interval() {
    let data_dir = tempfile::tempdir().expect("data dir");
    let (index_path, planted) = plant_unmerged_quill_index(data_dir.path(), 6);
    let planted_segments = planted.segment_count();
    drop(planted);
    assert!(
        planted_segments >= 6,
        "each committed round must leave a segment, got {planted_segments}"
    );
    let mut t_index = TantivyIndex::open_or_create(&index_path).expect("open lexical index");

    let mut pacer = StreamingCommitPacer::new();
    pacer.last_fold_attempt = Some(std::time::Instant::now());
    pacer.commit(Some(&mut t_index), None);
    assert_eq!(
        t_index.segment_count(),
        planted_segments,
        "a fold attempted inside the spacing interval must be skipped"
    );

    pacer.last_fold_attempt = None;
    pacer.commit(Some(&mut t_index), None);
    assert!(
        pacer.last_fold_attempt.is_some(),
        "the attempt must be recorded"
    );
    assert!(
        t_index.segment_count() < planted_segments,
        "the small segments must be folded, still {} of {planted_segments}",
        t_index.segment_count()
    );
    assert_eq!(t_index.doc_count().expect("doc count"), 6);
}

/// The full-rebuild rule written out independently of the implementation:
/// max(512 MiB, basis * 9/4 - staging (not below 0) + db / 16 + db while
/// the migration repair is pending), where the caller passes the live
/// lexical bytes as the basis, or the db bundle when there are none.
fn expected_rebuild_requirement(
    db_bundle_bytes: u64,
    basis_bytes: u64,
    staging_bytes: u64,
    migration_pending: bool,
) -> u64 {
    let lexical = (u128::from(basis_bytes) * 9 / 4).saturating_sub(u128::from(staging_bytes));
    let db = u128::from(db_bundle_bytes) / 16
        + if migration_pending {
            u128::from(db_bundle_bytes)
        } else {
            0
        };
    u64::try_from(lexical + db)
        .unwrap_or(u64::MAX)
        .max(INDEX_MIN_FREE_SPACE_BYTES)
}

/// #453: the headroom projection charges only the live lexical bytes.
/// Merge-retired segment files and retained publish backups are already
/// on disk and are not rewritten by a rebuild, so they are reported but
/// excluded from the requirement.
#[test]
fn full_rebuild_headroom_excludes_retired_segments_and_retained_backups() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let db_path = data_dir.join("agent_search.db");
    std::fs::File::create(&db_path)
        .unwrap()
        .set_len(640 * 1024 * 1024)
        .unwrap();
    let index_path = plant_merged_quill_index(&data_dir, 4);

    let footprint = lexical_index_footprint(&data_dir);
    let quill = crate::search::quill_bridge::quill_directory_footprint(&index_path)
        .expect("published Quill index");
    assert_eq!(footprint.live_bytes, quill.live_bytes);
    assert_eq!(footprint.retired_segment_bytes, quill.retired_bytes);
    assert_eq!(footprint.retired_segment_files, quill.retired_segment_files);
    assert_eq!(
        footprint.retired_segment_files, 4,
        "four folded inputs remain on disk"
    );
    assert!(footprint.retired_segment_bytes > 0);
    assert_eq!(footprint.retained_backup_bytes, 0);

    // A retained prior generation and a stray file next to the index.
    let backup_dir = data_dir
        .join(LEXICAL_INDEX_ROOT_DIR)
        .join(LEXICAL_PUBLISH_BACKUPS_DIR_NAME)
        .join("2026-09-05T00-00-00Z");
    std::fs::create_dir_all(&backup_dir).unwrap();
    std::fs::File::create(backup_dir.join("seg-000000000000abcd.fslx"))
        .unwrap()
        .set_len(64 * 1024 * 1024)
        .unwrap();
    std::fs::write(
        backup_dir.join("MANIFEST"),
        b"not parsed: backups are never split",
    )
    .unwrap();
    std::fs::write(
        data_dir
            .join(LEXICAL_INDEX_ROOT_DIR)
            .join("schema_hash.json"),
        vec![b'x'; 40],
    )
    .unwrap();

    let with_extras = lexical_index_footprint(&data_dir);
    assert_eq!(with_extras.live_bytes, footprint.live_bytes + 40);
    assert_eq!(
        with_extras.retired_segment_bytes,
        footprint.retired_segment_bytes
    );
    assert_eq!(
        with_extras.retained_backup_bytes,
        64 * 1024 * 1024 + "not parsed: backups are never split".len() as u64
    );
    assert_eq!(
        with_extras.total_bytes(),
        recursive_size_for_test(&data_dir.join(LEXICAL_INDEX_ROOT_DIR)),
        "the classification is exhaustive over the index tree"
    );

    let projection = full_rebuild_headroom_projection(&data_dir, &db_path);
    assert_eq!(projection.lexical_index_bytes, with_extras.live_bytes);
    assert_eq!(
        projection.retired_segment_bytes,
        with_extras.retired_segment_bytes
    );
    assert_eq!(projection.retired_segment_files, 4);
    assert_eq!(
        projection.retained_backup_bytes,
        with_extras.retained_backup_bytes
    );
    // The planted db file has no engine migration marker, so it is charged
    // one copy plus the growth allowance, which clears the floor: the
    // requirement is exactly the rule over live bytes, and the 64 MiB
    // backup and the retired inputs are not charged.
    assert!(projection.migration_repair_pending);
    assert_eq!(projection.migration_copy_bytes, projection.db_bundle_bytes);
    assert_eq!(
        projection.db_growth_allowance_bytes,
        projection.db_bundle_bytes / 16
    );
    assert_eq!(
        projection.required_bytes,
        expected_rebuild_requirement(projection.db_bundle_bytes, with_extras.live_bytes, 0, true)
    );
    assert!(
        projection.required_bytes
            < expected_rebuild_requirement(
                projection.db_bundle_bytes,
                with_extras.total_bytes(),
                0,
                true
            ),
        "the recursive size would have demanded more"
    );
}

/// GH #496 / #498: a failed or interrupted rebuild's staged generation
/// is reported and credited against the requirement. The next rebuild
/// resumes into it or clears it first, so its bytes come back; doubling
/// it locked a large archive out of the retry (13.4 GB archive: 67.8 GB
/// required, 64.7 GB free, 32 GB of it the #498 leftover).
#[test]
fn full_rebuild_headroom_credits_a_leftover_rebuild_staging_generation() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let db_path = data_dir.join("agent_search.db");
    // Unmigrated (no marker), so the db charge alone clears the floor and
    // the lexical credit stays visible in the requirement.
    std::fs::File::create(&db_path)
        .unwrap()
        .set_len(640 * 1024 * 1024)
        .unwrap();
    let index_path = plant_merged_quill_index(&data_dir, 2);
    let before = lexical_index_footprint(&data_dir);
    let projection_before = full_rebuild_headroom_projection(&data_dir, &db_path);
    assert_eq!(before.rebuild_staging_bytes, 0, "control: nothing staged");
    assert_eq!(projection_before.rebuild_staging_bytes, 0);

    // A leftover staged generation beside the live index, as a failed
    // `cass index --full` leaves it.
    let staging = staged_lexical_rebuild_scratch_path(&index_path);
    std::fs::create_dir_all(staging.join("merge-00000")).unwrap();
    std::fs::File::create(staging.join("seg-00000000000000ff.fslx"))
        .unwrap()
        .set_len(96 * 1024 * 1024)
        .unwrap();
    std::fs::write(staging.join("MANIFEST"), b"staging is counted whole").unwrap();
    std::fs::write(
        staging.join("merge-00000").join("scratch.bin"),
        vec![0u8; 1000],
    )
    .unwrap();
    let staged = 96 * 1024 * 1024 + "staging is counted whole".len() as u64 + 1000;

    let after = lexical_index_footprint(&data_dir);
    assert_eq!(after.rebuild_staging_bytes, staged);
    assert_eq!(after.live_bytes, before.live_bytes, "staging is not live");
    assert_eq!(
        after.total_bytes(),
        recursive_size_for_test(&data_dir.join(LEXICAL_INDEX_ROOT_DIR)),
        "the classification is exhaustive over the index tree"
    );

    let projection = full_rebuild_headroom_projection(&data_dir, &db_path);
    assert_eq!(projection.rebuild_staging_bytes, staged);
    assert_eq!(
        projection_before.required_bytes,
        expected_rebuild_requirement(projection.db_bundle_bytes, before.live_bytes, 0, true)
    );
    assert_eq!(
        projection.required_bytes,
        expected_rebuild_requirement(projection.db_bundle_bytes, after.live_bytes, staged, true)
    );
    let lexical_charge = after.live_bytes * 9 / 4;
    assert_eq!(
        projection_before.required_bytes - projection.required_bytes,
        staged.min(lexical_charge),
        "the staged bytes are credited one-for-one against the lexical charge, never beyond it"
    );

    // A staged generation larger than the lexical charge pays that charge
    // and nothing more: the db terms stay.
    let huge = staging.join("seg-00000000000001ff.fslx");
    std::fs::File::create(&huge)
        .unwrap()
        .set_len(4 * 1024 * 1024 * 1024)
        .unwrap();
    let capped = full_rebuild_headroom_projection(&data_dir, &db_path);
    assert!(capped.rebuild_staging_bytes > capped.lexical_index_bytes * 9 / 4);
    assert_eq!(
        capped.required_bytes,
        capped.db_growth_allowance_bytes + capped.migration_copy_bytes
    );
}

/// nw61n, on the measured fixture (256k messages: db bundle 3,161,862,224
/// bytes, live lexical 1,393,617,850 bytes; `--force-rebuild` wrote 2,821
/// MiB). The db is charged only its growth allowance once migrated, so the
/// requirement covers the measured rebuild with margin and falls well
/// below the old `2*db + 2*live` (9,110,960,148 bytes, which refused a
/// `--full` that wrote nothing). Negative: with the engine's migration
/// repair pending the whole bundle is charged on top.
#[test]
fn full_rebuild_headroom_charges_the_db_only_for_what_touches_it() {
    let footprint = LexicalIndexFootprint {
        live_bytes: 1_393_617_850,
        retired_segment_bytes: 1_563_884_292,
        retired_segment_files: 150,
        retained_backup_bytes: 2_937_502_205,
        rebuild_staging_bytes: 0,
        quarantine_bytes: 0,
    };
    let db = 3_161_862_224;
    let migrated = full_rebuild_headroom_projection_from(db, footprint, false);
    assert_eq!(
        migrated.required_bytes,
        expected_rebuild_requirement(db, footprint.live_bytes, 0, false)
    );
    assert!(!migrated.migration_repair_pending);
    assert_eq!(migrated.migration_copy_bytes, 0);
    let measured_force_rebuild_peak = 2_821 * 1024 * 1024;
    assert!(migrated.required_bytes >= measured_force_rebuild_peak);
    assert!(migrated.required_bytes < 9_110_960_148 / 2);

    let pending = full_rebuild_headroom_projection_from(db, footprint, true);
    assert!(pending.migration_repair_pending);
    assert_eq!(pending.migration_copy_bytes, db);
    assert_eq!(pending.required_bytes, migrated.required_bytes + db);

    // Small archives sit on the floor; a staged generation never takes
    // the requirement below it.
    let small = LexicalIndexFootprint {
        live_bytes: 8 * 1024 * 1024,
        rebuild_staging_bytes: 64 * 1024 * 1024,
        ..LexicalIndexFootprint::default()
    };
    assert_eq!(
        full_rebuild_headroom_projection_from(16 * 1024 * 1024, small, false).required_bytes,
        INDEX_MIN_FREE_SPACE_BYTES
    );

    // Quarantine bundles are evidence a rebuild never rewrites: ts1's
    // 34.6 GB of them change nothing in the requirement.
    let with_quarantine = LexicalIndexFootprint {
        quarantine_bytes: 34_600_000_000,
        ..footprint
    };
    let quarantined = full_rebuild_headroom_projection_from(db, with_quarantine, false);
    assert_eq!(quarantined.required_bytes, migrated.required_bytes);
    assert_eq!(quarantined.quarantine_bytes, 34_600_000_000);
    assert_eq!(
        quarantined.lexical_rebuild_basis_bytes,
        footprint.live_bytes
    );

    // With no live generation left (only backups, retired segments and
    // quarantine), the rebuild is sized as the db bundle, not the floor.
    let no_live = LexicalIndexFootprint {
        live_bytes: 0,
        ..with_quarantine
    };
    let repair = full_rebuild_headroom_projection_from(db, no_live, false);
    assert_eq!(repair.lexical_index_bytes, 0);
    assert_eq!(repair.lexical_rebuild_basis_bytes, db);
    assert_eq!(
        repair.required_bytes,
        expected_rebuild_requirement(db, db, 0, false)
    );
    assert!(repair.required_bytes > measured_force_rebuild_peak);
    // Negative: an empty data dir with no db stays on the floor.
    assert_eq!(
        full_rebuild_headroom_projection_from(0, LexicalIndexFootprint::default(), false)
            .required_bytes,
        INDEX_MIN_FREE_SPACE_BYTES
    );
}

/// The walker sets quarantine bundles (and interrupted reclaims of them)
/// apart from the live generation, by the inventory's own names.
#[test]
fn lexical_index_footprint_counts_quarantine_bundles_apart_from_live_bytes() {
    let tmp = TempDir::new().unwrap();
    let index = tmp.path().join(LEXICAL_INDEX_ROOT_DIR);
    let live = index.join("v9-quill");
    std::fs::create_dir_all(&live).unwrap();
    std::fs::write(live.join("schema.json"), vec![0_u8; 100]).unwrap();
    for (bundle, bytes) in [
        (".lexical-rebuild-quarantine-Ab12Cd", 1_000_usize),
        (".lexical-rebuild-reclaim-Ef34Gh", 300),
    ] {
        let generation = index.join(bundle).join("index");
        std::fs::create_dir_all(&generation).unwrap();
        std::fs::write(generation.join("seg-1.fslx"), vec![0_u8; bytes]).unwrap();
    }
    // Negative: a name the inventory would not match stays live.
    let lookalike = index.join(".lexical-rebuild-quarantine-toolong1");
    std::fs::create_dir_all(&lookalike).unwrap();
    std::fs::write(lookalike.join("data"), vec![0_u8; 20]).unwrap();

    let footprint = lexical_index_footprint(tmp.path());
    assert_eq!(footprint.quarantine_bytes, 1_300);
    assert_eq!(footprint.live_bytes, 120);
    assert_eq!(footprint.total_bytes(), 1_420);
}

/// The rebuild's runtime backstop: below the floor it stops with the marker
/// the CLI maps to exit 14, naming the resume; at or above it, it runs on.
/// The fold runs only with room for a second copy of the generation plus
/// the floor.
#[test]
fn lexical_rebuild_free_space_backstop_stops_below_the_floor() {
    let probe = Path::new("/data/cass/index");
    let floor = INDEX_MIN_FREE_SPACE_BYTES;
    assert!(lexical_rebuild_free_space_verdict(floor, floor, probe).is_ok());
    assert!(lexical_rebuild_free_space_verdict(u64::MAX, floor, probe).is_ok());
    let stopped = lexical_rebuild_free_space_verdict(floor - 1, floor, probe)
        .expect_err("one byte below the floor stops the rebuild");
    let message = format!("{stopped:#}");
    assert!(
        message.contains(LEXICAL_REBUILD_DISK_STOP_MARKER),
        "{message}"
    );
    assert!(message.contains("/data/cass/index"), "{message}");
    assert!(message.contains("resume"), "{message}");

    let generation = 3 * 1024 * 1024 * 1024;
    assert!(lexical_rebuild_fold_fits(
        generation + floor,
        generation,
        floor
    ));
    assert!(!lexical_rebuild_fold_fits(
        generation + floor - 1,
        generation,
        floor
    ));
    assert!(!lexical_rebuild_fold_fits(u64::MAX - 1, u64::MAX, floor));
}

/// An unreadable MANIFEST must make the walk conservative: every byte in
/// that directory counts as live.
#[test]
fn lexical_footprint_treats_an_unreadable_manifest_as_all_live() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let index_path = crate::search::tantivy::expected_index_dir(&data_dir);
    std::fs::create_dir_all(&index_path).unwrap();
    std::fs::write(index_path.join("MANIFEST"), b"garbage").unwrap();
    std::fs::write(
        index_path.join("seg-0000000000000001.fslx"),
        vec![0u8; 1000],
    )
    .unwrap();
    std::fs::write(
        index_path.join("seg-0000000000000001.fslx.retired"),
        vec![0u8; 40],
    )
    .unwrap();
    let footprint = lexical_index_footprint(&data_dir);
    assert_eq!(footprint.live_bytes, 1000 + 40 + "garbage".len() as u64);
    assert_eq!(footprint.retired_segment_bytes, 0);
    assert_eq!(footprint.retired_segment_files, 0);
}

/// `cass index --gc` plumbing (#453): the sweep opens the writer, reports
/// the footprint on both sides, and cannot reclaim inside the engine's
/// grace period (the folded inputs were retired seconds ago).
#[test]
fn lexical_segment_gc_reports_the_footprint_and_respects_the_grace_period() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().to_path_buf();
    assert!(
        run_lexical_segment_gc(&data_dir)
            .unwrap_err()
            .to_string()
            .contains("no published lexical index"),
        "a missing index is a typed refusal, not a create"
    );
    let index_path = plant_merged_quill_index(&data_dir, 3);
    let before = crate::search::quill_bridge::quill_directory_footprint(&index_path)
        .expect("published Quill index");

    let report = run_lexical_segment_gc(&data_dir).expect("sweep");
    assert_eq!(report.index_path, index_path);
    assert_eq!(report.live_segments, 1);
    assert_eq!(
        report.segment_files_before, 4,
        "three inputs plus the merge output"
    );
    assert_eq!(
        report.segment_files_after, 4,
        "inputs are inside the grace period"
    );
    assert_eq!(report.retired_bytes_before, before.retired_bytes);
    assert_eq!(report.retired_bytes_after, before.retired_bytes);
    assert_eq!(report.reclaimed_files, 0);
    assert_eq!(report.reclaimed_bytes, 0);
    assert_eq!(
        report.grace_secs,
        frankensearch::quill::DEFAULT_GARBAGE_GRACE.as_secs()
    );
    let json = serde_json::to_value(&report).expect("serialize");
    assert_eq!(json["live_segments"], 1);
    assert_eq!(json["reclaimed_files"], 0);
}

fn recursive_size_for_test(root: &Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(metadata) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if metadata.is_file() {
            total += metadata.len();
        } else if metadata.is_dir() {
            for entry in std::fs::read_dir(&path).unwrap().flatten() {
                stack.push(entry.path());
            }
        }
    }
    total
}

#[test]
fn headroom_probe_checks_data_dir_and_custom_db_parent_when_distinct() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("cass-data");
    let db_parent = tmp.path().join("custom-db");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&db_parent).unwrap();

    assert_eq!(
        existing_headroom_probe_paths(&data_dir, &db_parent.join("agent_search.db")),
        vec![data_dir, db_parent]
    );
}

#[test]
fn headroom_refusal_names_the_raw_mirror_only_on_the_data_dir_probe() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("cass-data");
    let db_parent = tmp.path().join("custom-db");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&db_parent).unwrap();
    assert_eq!(raw_mirror_headroom_note(&data_dir, &data_dir), "");

    let source = tmp.path().join("session.jsonl");
    std::fs::write(&source, b"{\"text\":\"mirrored\"}\n").unwrap();
    crate::raw_mirror::capture_source_file(crate::raw_mirror::RawMirrorCaptureInput {
        data_dir: &data_dir,
        provider: "codex",
        source_id: "local",
        origin_kind: "local",
        origin_host: None,
        source_path: &source,
        db_links: &[],
    })
    .unwrap();
    let mirror_bytes = crate::raw_mirror::physical_storage_bytes(&data_dir);
    assert!(mirror_bytes > 0);

    let note = raw_mirror_headroom_note(&data_dir, &data_dir);
    assert!(
        note.contains(&format!("holds {mirror_bytes} bytes")),
        "{note}"
    );
    assert!(
        note.contains("cass mirror prune --keep-latest 1 --safety-hold-down 6h --json"),
        "{note}"
    );
    // A shortfall at a database directory elsewhere is not the mirror's.
    assert_eq!(raw_mirror_headroom_note(&data_dir, &db_parent), "");
}

/// nw61n: a full-rebuild refusal names the rule and the terms it charged,
/// and on the data dir's filesystem what cass can reclaim without touching
/// the live index (none of which the requirement charges).
#[test]
fn full_rebuild_refusal_names_the_rule_terms_and_reclaimable_bytes() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("cass-data");
    let elsewhere = tmp.path().join("custom-db");
    std::fs::create_dir_all(data_dir.join(LEXICAL_INDEX_ROOT_DIR)).unwrap();
    std::fs::create_dir_all(&elsewhere).unwrap();
    let projection = FullRebuildHeadroomProjection {
        required_bytes: 9_000,
        floor_bytes: INDEX_MIN_FREE_SPACE_BYTES,
        db_bundle_bytes: 3_200,
        lexical_index_bytes: 1_600,
        lexical_rebuild_basis_bytes: 1_600,
        retired_segment_bytes: 4_096,
        retired_segment_files: 2,
        retained_backup_bytes: 8_192,
        rebuild_staging_bytes: 512,
        quarantine_bytes: 0,
        db_growth_allowance_bytes: 200,
        migration_repair_pending: true,
        migration_copy_bytes: 3_200,
    };
    let terms = full_rebuild_refusal_terms(&data_dir, &data_dir, &projection);
    // The formula itself names the fallback; the sentence below is the
    // refusal saying it applied.
    assert!(
        !terms.contains("There is no live lexical generation"),
        "{terms}"
    );
    let no_live = FullRebuildHeadroomProjection {
        lexical_index_bytes: 0,
        lexical_rebuild_basis_bytes: 3_200,
        ..projection
    };
    let no_live_terms = full_rebuild_refusal_terms(&data_dir, &data_dir, &no_live);
    assert!(
        no_live_terms.contains("There is no live lexical generation to size the rebuild by")
            && no_live_terms.contains("(3200 bytes)"),
        "{no_live_terms}"
    );
    for expected in [
        FULL_REBUILD_HEADROOM_FORMULA,
        "lexical index 1600 bytes",
        "staged generation credit 512 bytes",
        "db bundle 3200 bytes (growth allowance 200 bytes)",
        "It includes 3200 bytes for the engine's one-time migration repair",
        "4096 bytes of merge-retired lexical segments",
        "'cass index --gc'",
        "8192 bytes of prior lexical generations",
        "'cass doctor cleanup'",
    ] {
        assert!(terms.contains(expected), "missing {expected:?}: {terms}");
    }
    assert!(!terms.contains("quarantine"), "{terms}");

    // A failed rebuild's quarantine bundle is named with its size and the
    // read-only preview that would reclaim it.
    let evidence = b"failed rebuild evidence";
    let bundle = data_dir
        .join(LEXICAL_INDEX_ROOT_DIR)
        .join(".lexical-rebuild-quarantine-000001");
    std::fs::create_dir_all(bundle.join("index")).unwrap();
    std::fs::write(bundle.join("index/MANIFEST"), evidence).unwrap();
    let terms = full_rebuild_refusal_terms(&data_dir, &data_dir, &projection);
    assert!(
        terms.contains(&format!(
            "quarantine bundles hold {} bytes, 0 of them eligible",
            evidence.len()
        )),
        "{terms}"
    );
    assert!(
        terms.contains(&format!(
            "'cass lexical-quarantine --data-dir {} --plan --json'",
            data_dir.display()
        )),
        "{terms}"
    );

    // A probe elsewhere gets the rule but none of the data dir's
    // reclaimable space, and a migrated archive no migration copy.
    let migrated = FullRebuildHeadroomProjection {
        migration_repair_pending: false,
        migration_copy_bytes: 0,
        ..projection
    };
    let elsewhere_terms = full_rebuild_refusal_terms(&data_dir, &elsewhere, &migrated);
    assert!(
        elsewhere_terms.contains(FULL_REBUILD_HEADROOM_FORMULA),
        "{elsewhere_terms}"
    );
    for absent in [
        "migration repair",
        "merge-retired",
        "publish-backups",
        "quarantine",
    ] {
        assert!(
            !elsewhere_terms.contains(absent),
            "unexpected {absent:?}: {elsewhere_terms}"
        );
    }
}

/// nw61n: the production refusal carries those terms under the prefix the
/// CLI maps to exit 14 / storage. A sparse 2 TiB archive without an engine
/// migration marker needs more than any test host has free.
#[test]
fn full_rebuild_refusal_message_carries_the_rule_terms() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("cass-data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let db_path = data_dir.join("agent_search.db");
    let db = std::fs::File::create(&db_path).unwrap();
    db.set_len(2 * 1024 * 1024 * 1024 * 1024).unwrap();
    drop(db);

    let message = format!(
        "{:#}",
        ensure_index_storage_headroom(
            &data_dir,
            &db_path,
            IndexStorageHeadroomRequirement::AuthoritativeLexicalRebuild,
        )
        .expect_err("a 2 TiB unmigrated archive cannot fit")
    );
    assert!(
        message.starts_with("canonical archive disk headroom check failed for "),
        "{message}"
    );
    assert!(message.contains(FULL_REBUILD_HEADROOM_FORMULA), "{message}");
    assert!(
        message.contains("one-time migration repair, which is still pending"),
        "{message}"
    );
}

#[test]
fn database_sidecar_paths_preserve_filename_bytes_without_display_roundtrip() {
    let db_path = PathBuf::from("/tmp/cass.db");
    let [wal, shm] = database_sidecar_paths(&db_path);

    assert_eq!(wal, PathBuf::from("/tmp/cass.db-wal"));
    assert_eq!(shm, PathBuf::from("/tmp/cass.db-shm"));
}

/// GH #450: the storage-open liveness probe must watch the frankensqlite
/// migration-repair artifacts, not just the live archive — writing the
/// `.pre-migration-bak` copy is the longest, most opaque part of the open
/// and is the only footprint that grows while it runs.
#[test]
fn storage_open_liveness_targets_cover_the_migration_repair_artifacts() {
    let db_path = PathBuf::from("/tmp/cass.db");
    let targets = storage_open_liveness_targets(&db_path);

    for expected in [
        "/tmp/cass.db",
        "/tmp/cass.db-wal",
        "/tmp/cass.db-shm",
        "/tmp/cass.db.pre-migration-bak",
        "/tmp/cass.db.pre-migration-bak-wal",
        "/tmp/cass.db.pre-migration-bak-shm",
        "/tmp/cass.db.fsqlite-migration-state",
    ] {
        assert!(
            targets.contains(&PathBuf::from(expected)),
            "missing liveness watch target {expected}: {targets:?}"
        );
    }
}

#[test]
fn incremental_index_startup_headroom_uses_safety_floor_not_archive_clone_size() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");
    std::fs::File::create(&db_path)
        .unwrap()
        .set_len(300 * 1024 * 1024)
        .unwrap();
    std::fs::write(database_path_with_suffix(&db_path, "-wal"), b"wal").unwrap();
    std::fs::write(database_path_with_suffix(&db_path, "-shm"), b"shm").unwrap();

    assert_eq!(
        index_startup_storage_headroom_requirement(false, false),
        IndexStorageHeadroomRequirement::IncrementalStartup,
        "plain cass index must not demand full-rebuild scratch space before it knows a full rebuild is needed"
    );
    assert_eq!(
        required_index_headroom_bytes(
            tmp.path(),
            &db_path,
            IndexStorageHeadroomRequirement::IncrementalStartup,
        ),
        INDEX_MIN_FREE_SPACE_BYTES,
        "routine incremental startup should require only the safety floor"
    );
}

/// `--full` and `--force-rebuild` take the full-rebuild requirement. With no
/// lexical index the projection is the db charge: a whole copy of the
/// bundle while the engine's one-time migration repair is pending (it keeps
/// a `.pre-migration-bak`), plus the 1/16 growth allowance. Neither path
/// clones the canonical archive otherwise (a `--full` that would need to
/// replace it is refused), so once the migration marker is complete only
/// the allowance remains (nw61n).
#[test]
fn full_and_force_rebuild_startup_headroom_charge_the_archive_copy_only_while_migration_is_pending()
{
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");
    std::fs::File::create(&db_path)
        .unwrap()
        .set_len(640 * 1024 * 1024)
        .unwrap();
    std::fs::write(database_path_with_suffix(&db_path, "-wal"), vec![0_u8; 256]).unwrap();
    std::fs::write(database_path_with_suffix(&db_path, "-shm"), vec![0_u8; 512]).unwrap();
    let bundle = database_bundle_size_bytes(&db_path);
    // No lexical index here, so the rebuild itself is sized as the bundle.
    let pending_requirement = expected_rebuild_requirement(bundle, bundle, 0, true);
    let migrated_requirement = expected_rebuild_requirement(bundle, bundle, 0, false);
    assert_eq!(pending_requirement - migrated_requirement, bundle);

    assert_eq!(
        index_startup_storage_headroom_requirement(true, false),
        IndexStorageHeadroomRequirement::AuthoritativeLexicalRebuild
    );
    assert_eq!(
        index_startup_storage_headroom_requirement(false, true),
        IndexStorageHeadroomRequirement::AuthoritativeLexicalRebuild
    );
    assert_eq!(
        required_index_headroom_bytes(
            tmp.path(),
            &db_path,
            IndexStorageHeadroomRequirement::AuthoritativeLexicalRebuild,
        ),
        pending_requirement,
        "an unmigrated archive is charged the migration repair's whole copy"
    );

    // A completed engine migration marker removes the copy and nothing else.
    std::fs::write(
        database_path_with_suffix(&db_path, ".fsqlite-migration-state"),
        br#"{"last_upgrade_version":1,"last_run_at":0,"repairs_applied":[]}"#,
    )
    .unwrap();
    assert!(crate::storage::sqlite::index_engine_migration_is_complete(
        &db_path
    ));
    assert_eq!(
        required_index_headroom_bytes(
            tmp.path(),
            &db_path,
            IndexStorageHeadroomRequirement::AuthoritativeLexicalRebuild,
        ),
        migrated_requirement,
        "a migrated archive is not charged the migration repair's copy"
    );
}

#[test]
fn disk_headroom_skip_env_value_parser_is_truthy_not_presence_based() {
    for truthy in ["1", "true", "YES", "on"] {
        assert!(
            env_value_truthy(truthy),
            "expected {truthy:?} to disable the headroom check"
        );
    }

    for falsy in ["0", "false", "No", "off", "", "maybe"] {
        assert!(
            !env_value_truthy(falsy),
            "expected {falsy:?} to keep the headroom check enabled"
        );
    }
}

#[test]
fn full_rebuild_integrity_preflight_size_gate_is_bounded_and_fail_safe() {
    let gib = 1024_u64 * 1024 * 1024;

    assert!(should_run_engine_backed_archive_integrity_preflight(
        Some(2 * gib),
        2 * gib
    ));
    assert!(!should_run_engine_backed_archive_integrity_preflight(
        Some(2 * gib + 1),
        2 * gib
    ));
    assert!(should_run_engine_backed_archive_integrity_preflight(
        Some(64 * gib),
        0
    ));
    assert!(should_run_engine_backed_archive_integrity_preflight(
        None,
        2 * gib
    ));
}

#[test]
fn full_rebuild_integrity_preflight_large_archive_stops_before_fsqlite_open() {
    // bet45/qu81y: the 4096-byte cap is INJECTED instead of set via
    // process-global env — the env guard leaked into parallel sibling
    // preflight tests and made their engine-backed detection skip.
    let tmp = TempDir::new().unwrap();
    let seed_path = tmp.path().join("seed.db");
    let seed = FrankenStorage::open(&seed_path).unwrap();
    drop(seed);
    let seed_bytes = fs::read(&seed_path).unwrap();

    let db_path = tmp.path().join("large-agent-search.db");
    let mut large = File::create(&db_path).unwrap();
    large.write_all(&seed_bytes[..100]).unwrap();
    large.set_len(8192).unwrap();
    drop(large);

    assert_eq!(
        full_rebuild_existing_archive_integrity_preflight_with_max_bytes(&db_path, 4096).unwrap(),
        None,
        "a large archive must defer engine-backed integrity work instead of opening fsqlite"
    );
    for suffix in ["-fsqlite-ns-gate", "-fsqlite-ns-use"] {
        let sidecar = database_path_with_suffix(&db_path, suffix);
        assert!(
            !sidecar.exists(),
            "large-archive preflight opened fsqlite and created {}",
            sidecar.display()
        );
    }
}

/// GH #450: background runs defer the engine's one-time migration repair
/// only for a large archive whose migration marker is absent or invalid.
#[test]
fn background_migration_repair_deferral_needs_a_large_unmigrated_archive() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("agent_search.db");
    let pending =
        |max_bytes| background_migration_repair_pending_bytes_with_max_bytes(&db_path, max_bytes);
    assert_eq!(pending(4096), None, "no archive, nothing to defer");

    let archive = File::create(&db_path).unwrap();
    archive.set_len(8192).unwrap();
    drop(archive);
    assert_eq!(pending(4096), Some(8192), "large and never migrated");
    assert_eq!(
        pending(16384),
        None,
        "small archives migrate in the background"
    );
    assert_eq!(pending(0), None, "a zero cap disables the size guard");

    let marker = database_path_with_suffix(&db_path, ".fsqlite-migration-state");
    fs::write(&marker, b"{\"last_upgrade_version\":1}").unwrap();
    assert_eq!(
        pending(4096),
        Some(8192),
        "a truncated marker is not a migration"
    );
    fs::write(
        &marker,
        br#"{"last_upgrade_version":1,"last_run_at":0,"repairs_applied":[]}"#,
    )
    .unwrap();
    assert_eq!(pending(4096), None, "a completed migration never defers");
    assert_eq!(fs::metadata(&db_path).unwrap().len(), 8192);
}

#[test]
fn full_rebuild_integrity_preflight_accepts_only_empty_bootstrap_states() {
    for tracking_table in [false, true] {
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("interrupted-bootstrap.db");
        {
            let conn =
                crate::franken_sync::Connection::open(db_path.to_string_lossy().into_owned())
                    .unwrap();
            // Persist a valid SQLite header even in the zero-object case.
            conn.execute("PRAGMA user_version = 1").unwrap();
            if tracking_table {
                conn.execute(
                    "CREATE TABLE _schema_migrations (
                        version INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        applied_at TEXT NOT NULL DEFAULT 'now'
                    )",
                )
                .unwrap();
            }
        }
        let before = fs::read(&db_path).unwrap();
        assert!(!before.is_empty());
        assert_eq!(
            full_rebuild_existing_archive_integrity_preflight(&db_path).unwrap(),
            None,
            "an interrupted empty bootstrap must permit normal migrations"
        );
        assert_eq!(fs::read(&db_path).unwrap(), before);
        let storage = FrankenStorage::open(&db_path).unwrap();
        assert_eq!(
            storage.schema_version().unwrap(),
            crate::storage::sqlite::CURRENT_SCHEMA_VERSION
        );
    }
}

#[test]
fn full_rebuild_integrity_preflight_refuses_nonempty_bootstrap_catalogs() {
    for sql in [
        "CREATE TABLE _schema_migrations (version INTEGER PRIMARY KEY);
         INSERT INTO _schema_migrations VALUES (13)",
        "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
         INSERT INTO meta VALUES ('schema_version', '21')",
        "CREATE TABLE conversations (id INTEGER PRIMARY KEY)",
        "CREATE TABLE unrelated (id INTEGER PRIMARY KEY)",
        "CREATE VIEW _schema_migrations AS SELECT 13 AS version",
    ] {
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("incomplete-archive.db");
        {
            let conn =
                crate::franken_sync::Connection::open(db_path.to_string_lossy().into_owned())
                    .unwrap();
            conn.execute_batch(sql).unwrap();
        }
        let before = fs::read(&db_path).unwrap();
        let problem = full_rebuild_existing_archive_integrity_preflight(&db_path)
            .unwrap()
            .expect("schema objects or committed migrations must not bypass archive checks");
        assert!(
            problem.contains("canonical table canary failed"),
            "{problem}"
        );
        assert_eq!(fs::read(&db_path).unwrap(), before);
    }
}

#[test]
fn full_rebuild_integrity_preflight_accepts_healthy_current_schema() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("healthy-current-schema.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    storage
        .ensure_search_fallback_fts_consistency()
        .expect("materialize a healthy canonical FTS shadow");
    drop(storage);

    assert_eq!(
        full_rebuild_existing_archive_integrity_preflight(&db_path)
            .expect("strict read-only archive preflight"),
        None,
        "a healthy canonical FTS shadow must pass through the same strict read-only opener used before a production full rebuild"
    );

    let mut storage = FrankenStorage::open_readonly(&db_path).unwrap();

    assert_eq!(
        full_rebuild_existing_storage_integrity_problem(&storage)
            .unwrap()
            .as_deref(),
        None
    );
    storage.close_best_effort_in_place();
}

#[test]
fn full_rebuild_integrity_preflight_rejects_corrupt_fts_structure_record() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("corrupt-fts-segment.db");

    {
        let storage = FrankenStorage::open(&db_path).unwrap();
        storage
            .ensure_search_fallback_fts_consistency()
            .expect("materialize the canonical FTS shadow");
        storage
            .raw()
            .execute(
                // bet45: the contentless fts_messages shadow no longer
                // carries a message_id column; the rowid IS the message id.
                "INSERT INTO fts_messages(
                     rowid, content, title, agent, workspace, source_path, created_at
                 ) VALUES(
                     1, 'segment corruption sentinel', 'title', 'codex', '/workspace',
                     '/tmp/session.jsonl', 1
                 )",
            )
            .expect("persist one FTS segment");
    }

    {
        let conn =
            crate::franken_sync::Connection::open(db_path.to_string_lossy().into_owned()).unwrap();
        // bet45: target the newest structure record instead of the old
        // magic id=10 — the contentless shadow's row layout changed and
        // the fixture's single segment no longer lands on that id.
        conn.execute(
            "UPDATE fts_messages_data
             SET block = X'FFFFFFFFFFFFFFFFFFFFFFFF'
             WHERE id = (SELECT MAX(id) FROM fts_messages_data)",
        )
        .expect("corrupt the persisted FTS structure record");
    }

    let mut storage = FrankenStorage::open_readonly(&db_path).unwrap();
    let problem = full_rebuild_existing_storage_integrity_problem(&storage)
        .unwrap()
        .expect("a corrupt FTS structure must stop a full rebuild");
    assert!(
        problem.contains("FTS5 segment integrity-check failed"),
        "diagnostic must identify the FTS segment preflight: {problem}"
    );
    storage.close_best_effort_in_place();
}

#[test]
fn full_rebuild_integrity_preflight_refuses_legacy_fts_before_schema_repair() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("legacy-fts-before-mutation.db");
    let mut storage = FrankenStorage::open(&db_path).unwrap();
    storage
        .raw()
        .execute(
            "DROP TABLE _schema_migrations;
             CREATE TABLE fts_messages_config(k TEXT PRIMARY KEY, v);
             CREATE TABLE fts_messages_idx(segid INTEGER, term BLOB, pgno INTEGER);",
        )
        .unwrap();
    storage.close_best_effort_in_place();

    let problem = full_rebuild_existing_archive_integrity_preflight(&db_path)
        .unwrap()
        .expect("legacy rowid FTS shadows must stop a full rebuild");
    assert!(problem.contains("fts_messages_config"));
    assert!(problem.contains("WITHOUT ROWID"));

    let readonly = FrankenStorage::open_strict_readonly(&db_path).unwrap();
    assert!(
        readonly
            .raw()
            .query("SELECT version FROM _schema_migrations LIMIT 1")
            .is_err(),
        "the read-only refusal preflight must not recreate transition tables"
    );
    readonly.close_without_checkpoint().unwrap();
}

#[test]
fn full_rebuild_integrity_preflight_recognizes_legacy_fts_shadow_ddl() {
    let legacy_config = "CREATE TABLE fts_messages_config(k TEXT PRIMARY KEY, v)";
    let legacy_idx = "CREATE TABLE fts_messages_idx(segid INTEGER, term BLOB, pgno INTEGER)";
    for (table, ddl) in [
        ("fts_messages_config", legacy_config),
        ("fts_messages_idx", legacy_idx),
    ] {
        let problem = incompatible_legacy_fts_shadow_ddl(table, ddl)
            .expect("legacy rowid FTS shadow DDL must be rejected");
        assert!(problem.contains(table));
        assert!(problem.contains("WITHOUT ROWID"));
    }

    for (table, ddl) in [
        (
            "fts_messages_config",
            "CREATE TABLE 'fts_messages_config'(k PRIMARY KEY, v) WITHOUT ROWID",
        ),
        (
            "fts_messages_idx",
            "CREATE TABLE \"fts_messages_idx\"(segid, term, pgno, PRIMARY KEY(segid, term)) WITHOUT ROWID",
        ),
    ] {
        assert_eq!(
            incompatible_legacy_fts_shadow_ddl(table, ddl),
            None,
            "canonical FTS shadow DDL must remain accepted"
        );
    }
}

#[test]
fn full_rebuild_integrity_preflight_flags_missing_core_tables() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("broken-current-schema.db");
    let storage = FrankenStorage::open(&db_path).unwrap();
    storage
        .raw()
        .execute("DROP TABLE messages")
        .expect("drop messages to simulate current-schema archive damage");

    let problem = full_rebuild_existing_storage_integrity_problem(&storage)
        .unwrap()
        .expect("missing core table should trigger fresh full-rebuild archive");
    assert!(
        problem.contains("messages"),
        "diagnostic should identify the failing canary table: {problem}"
    );
}

#[test]
fn current_schema_fast_probe_accepts_current_schema() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("current-schema.db");

    let storage = FrankenStorage::open(&db_path).unwrap();
    assert_eq!(
        storage.schema_version().unwrap(),
        crate::storage::sqlite::CURRENT_SCHEMA_VERSION
    );
    drop(storage);

    assert!(current_schema_fast_probe(&db_path).unwrap());
}

#[test]
fn current_schema_fast_probe_rejects_future_schema_marker() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("future-marker.db");

    let storage = FrankenStorage::open(&db_path).unwrap();
    storage
        .raw()
        .execute_compat(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', ?1)",
            &[ParamValue::from(format!(
                "{}",
                crate::storage::sqlite::CURRENT_SCHEMA_VERSION + 1
            ))],
        )
        .unwrap();
    drop(storage);

    assert!(!current_schema_fast_probe(&db_path).unwrap());
}

#[test]
fn open_storage_for_index_fast_current_schema_path_preserves_transition_state() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("current-schema-transition.db");

    {
        let storage = FrankenStorage::open(&db_path).unwrap();
        storage
            .raw()
            .execute("DROP TABLE _schema_migrations")
            .unwrap();
    }

    assert!(current_schema_fast_probe(&db_path).unwrap());

    let (storage, rebuilt, opened_fresh_for_full) =
        open_storage_for_index(&db_path, false).unwrap();
    assert!(!rebuilt);
    assert!(!opened_fresh_for_full);
    assert_eq!(
        storage.schema_version().unwrap(),
        crate::storage::sqlite::CURRENT_SCHEMA_VERSION
    );
    assert!(
        storage
            .raw()
            .query("SELECT version FROM _schema_migrations LIMIT 1;")
            .is_ok()
    );
}

#[test]
fn reset_storage_clears_data_but_leaves_meta() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);

    let agent = crate::model::types::Agent {
        id: None,
        slug: "tester".into(),
        name: "Tester".into(),
        version: None,
        kind: crate::model::types::AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    let conv = norm_conv(Some("c1"), vec![norm_msg(0, 10)]);
    storage
        .insert_conversation_tree(
            agent_id,
            None,
            &crate::model::types::Conversation {
                id: None,
                agent_slug: conv.agent_slug.clone(),
                workspace: conv.workspace.clone(),
                external_id: conv.external_id.clone(),
                title: conv.title.clone(),
                source_path: conv.source_path.clone(),
                started_at: conv.started_at,
                ended_at: conv.ended_at,
                approx_tokens: None,
                metadata_json: conv.metadata.clone(),
                messages: conv
                    .messages
                    .iter()
                    .map(|m| crate::model::types::Message {
                        id: None,
                        idx: m.idx,
                        role: crate::model::types::MessageRole::User,
                        author: m.author.clone(),
                        created_at: m.created_at,
                        content: m.content.clone(),
                        extra_json: m.extra.clone(),
                        snippets: Vec::new(),
                    })
                    .collect(),
                source_id: "local".to_string(),
                origin_host: None,
            },
        )
        .unwrap();

    let msg_count: i64 = storage
        .raw()
        .query_row_map("SELECT COUNT(*) FROM messages", &[] as &[ParamValue], |r| {
            r.get_typed(0)
        })
        .unwrap();
    assert_eq!(msg_count, 1);

    storage
        .raw()
        .execute_compat(
            "INSERT INTO daily_stats(day_id, agent_slug, source_id, session_count, message_count, total_chars, last_updated)
             VALUES(?1, ?2, ?3, 1, 1, 10, ?4)",
            &[
                ParamValue::from(1_i64),
                ParamValue::from("tester"),
                ParamValue::from("local"),
                ParamValue::from(123_i64),
            ],
        )
        .unwrap();
    storage
        .raw()
        .execute_compat(
            "INSERT INTO usage_daily(day_id, agent_slug, workspace_id, source_id, message_count, last_updated)
             VALUES(?1, ?2, ?3, ?4, 1, ?5)",
            &[
                ParamValue::from(1_i64),
                ParamValue::from("tester"),
                ParamValue::from(0_i64),
                ParamValue::from("local"),
                ParamValue::from(123_i64),
            ],
        )
        .unwrap();

    reset_storage(&storage).unwrap();
    let reopened = FrankenStorage::open(&db_path).unwrap();

    let msg_count: i64 = reopened
        .raw()
        .query_row_map("SELECT COUNT(*) FROM messages", &[] as &[ParamValue], |r| {
            r.get_typed(0)
        })
        .unwrap();
    assert_eq!(msg_count, 0);
    let daily_count: i64 = reopened
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM daily_stats",
            &[] as &[ParamValue],
            |r| r.get_typed(0),
        )
        .unwrap();
    assert_eq!(daily_count, 0);
    let usage_daily_count: i64 = reopened
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM usage_daily",
            &[] as &[ParamValue],
            |r| r.get_typed(0),
        )
        .unwrap();
    assert_eq!(usage_daily_count, 0);
    let fts_count: i64 = reopened
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM fts_messages",
            &[] as &[ParamValue],
            |r| r.get_typed(0),
        )
        .unwrap();
    assert_eq!(fts_count, 0, "reset should recreate an empty FTS table");
    assert_eq!(
        reopened.schema_version().unwrap(),
        crate::storage::sqlite::CURRENT_SCHEMA_VERSION
    );
}
