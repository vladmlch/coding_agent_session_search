use super::super::*;
use super::*;

#[test]
fn lexical_storage_fingerprint_matching_handles_jitter_and_size_drift() {
    let cases = [
        (
            "small mtime settle jitter",
            "323584:1776310228000:329632:1776310227824",
            "323584:1776310227832:329632:1776310227824",
            true,
        ),
        (
            "wal size drift",
            "323584:1776310228000:329632:1776310227824",
            "323584:1776310227832:400000:1776310227824",
            false,
        ),
    ];

    for (label, current, saved, expected) in cases {
        assert_eq!(
            lexical_storage_fingerprints_match(current, saved),
            expected,
            "{label}"
        );
    }
}

fn hollow_probe_checkpoint(db_path: &Path, indexed_docs: usize) -> LexicalRebuildCheckpoint {
    LexicalRebuildCheckpoint {
        db_path: db_path.display().to_string(),
        total_conversations: 10,
        storage_fingerprint: "content-v1:10:10:100".to_string(),
        committed_offset: 10,
        committed_conversation_id: Some(10),
        processed_conversations: 10,
        indexed_docs,
        indexed_region_docs: 0,
        schema_hash: SCHEMA_HASH.to_string(),
        page_size: LEXICAL_REBUILD_PAGE_SIZE_PUBLIC,
        completed: true,
        updated_at_ms: 1_733_000_000_000,
    }
}

fn hollow_probe_state(
    index_path: &Path,
    db_path: &Path,
    checkpoint: Option<&LexicalRebuildCheckpoint>,
    live_docs: Option<u64>,
) -> LexicalAssetState {
    lexical_state_from_observations(LexicalObservationInput {
        index_path,
        db_path,
        stale_threshold: 3600,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms: 1_733_000_001_000,
        maintenance: SearchMaintenanceSnapshot::default(),
        checkpoint,
        current_db_fingerprint: Some("content-v1:10:10:100"),
        live: live_docs.map(|live_docs| crate::search::tantivy::SearchableLiveDocs {
            live_docs,
            manifest_source: crate::search::quill_bridge::QuillManifestSource::Current,
        }),
    })
}

/// GH #457: a completed, fingerprint-matching checkpoint that certified
/// 1,000 documents over a generation serving 3 is HOLLOW — its own
/// status, stale, not fresh, and the reason names the only remedy.
#[test]
fn lexical_state_reports_a_hollow_generation_as_its_own_status() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v9-quill");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    let db_path = temp.path().join("agent_search.db");
    std::fs::write(&db_path, b"db").expect("write db file");
    let checkpoint = hollow_probe_checkpoint(&db_path, 1_000);

    let state = hollow_probe_state(&index_path, &db_path, Some(&checkpoint), Some(3));
    assert!(state.hollow);
    assert_eq!(state.live_docs, Some(3));
    assert_eq!(state.status, "hollow");
    assert!(state.stale);
    assert!(!state.fresh);
    assert_eq!(
        state.fingerprint.matches_current_db_fingerprint,
        Some(true),
        "hollowness is orthogonal to the content fingerprint"
    );
    let reason = state.status_reason.as_deref().unwrap_or_default();
    assert!(reason.contains("HOLLOW"), "{reason}");
    assert!(reason.contains("3 live document"), "{reason}");
    assert!(reason.contains("1000 indexed document"), "{reason}");
    assert!(reason.contains("99% missing"), "{reason}");
    assert!(reason.contains("Run `cass index`"), "{reason}");

    // Exactly the floor is not hollow; one below it is.
    let at_floor = hollow_probe_state(&index_path, &db_path, Some(&checkpoint), Some(500));
    assert!(!at_floor.hollow, "50% of the certified count is the floor");
    assert_eq!(at_floor.status, "ready");
    let below_floor = hollow_probe_state(&index_path, &db_path, Some(&checkpoint), Some(499));
    assert!(below_floor.hollow);

    // Growth after the rebuild (incremental ingest) is never hollow.
    let grown = hollow_probe_state(&index_path, &db_path, Some(&checkpoint), Some(5_000));
    assert!(!grown.hollow);
    assert_eq!(grown.status, "ready");
}

/// mknmt: a corrupt current MANIFEST is read through `MANIFEST.prev`, so
/// the generation is stale (searchable, one publish behind) and the reason
/// names the slot and the repair. A missing current slot is reported as
/// missing, never as corruption.
#[test]
fn lexical_state_reports_a_corrupt_current_manifest_as_stale() {
    use crate::search::quill_bridge::QuillManifestSource;
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v9-quill");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    let marker = index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER);
    std::fs::write(&marker, b"{}").expect("write quill manifest");
    let db_path = temp.path().join("agent_search.db");
    std::fs::write(&db_path, b"db").expect("write db file");
    let checkpoint = hollow_probe_checkpoint(&db_path, 1_000);
    let probe = |manifest_source| {
        lexical_state_from_observations(LexicalObservationInput {
            index_path: &index_path,
            db_path: &db_path,
            stale_threshold: 3600,
            last_indexed_at_ms: Some(1_733_000_000_000),
            now_ms: 1_733_000_001_000,
            maintenance: SearchMaintenanceSnapshot::default(),
            checkpoint: Some(&checkpoint),
            current_db_fingerprint: Some("content-v1:10:10:100"),
            live: Some(crate::search::tantivy::SearchableLiveDocs {
                live_docs: 1_000,
                manifest_source,
            }),
        })
    };

    let current = probe(QuillManifestSource::Current);
    assert_eq!(current.status, "ready");
    assert!(current.fresh);
    assert_eq!(current.manifest_source, Some(QuillManifestSource::Current));

    let corrupt = probe(QuillManifestSource::PreviousAfterCorruptCurrent);
    assert_eq!(corrupt.status, "stale");
    assert!(corrupt.stale);
    assert!(!corrupt.fresh);
    assert!(
        !corrupt.hollow,
        "the served count still matches the checkpoint"
    );
    assert_eq!(
        corrupt.manifest_source,
        Some(QuillManifestSource::PreviousAfterCorruptCurrent)
    );
    assert_eq!(
        corrupt.status_reason.as_deref(),
        Some(CORRUPT_MANIFEST_STATUS_REASON)
    );

    let parked = index_path.join("MANIFEST.parked");
    std::fs::rename(&marker, &parked).expect("park MANIFEST");
    let missing = probe(QuillManifestSource::PreviousAfterMissingCurrent);
    assert_eq!(missing.status, "missing");
    assert_eq!(
        missing.manifest_source,
        Some(QuillManifestSource::PreviousAfterMissingCurrent)
    );
    let reason = missing.status_reason.as_deref().unwrap_or_default();
    assert!(!reason.contains("corrupt"), "{reason}");
}

/// GH #457: without a certifying checkpoint, or without a decodable
/// manifest, there is no verdict — an incomplete checkpoint keeps its
/// own `partial`/incomplete signal and never doubles as hollow.
#[test]
fn lexical_state_never_calls_an_uncertified_generation_hollow() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v9-quill");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    let db_path = temp.path().join("agent_search.db");
    std::fs::write(&db_path, b"db").expect("write db file");

    let no_checkpoint = hollow_probe_state(&index_path, &db_path, None, Some(0));
    assert!(!no_checkpoint.hollow);
    assert_ne!(no_checkpoint.status, "hollow");

    let mut incomplete = hollow_probe_checkpoint(&db_path, 1_000);
    incomplete.completed = false;
    let partial = hollow_probe_state(&index_path, &db_path, Some(&incomplete), Some(3));
    assert!(
        !partial.hollow,
        "an incomplete checkpoint is partial, not hollow"
    );
    assert_eq!(partial.status, "stale");
    assert!(
        partial
            .status_reason
            .as_deref()
            .unwrap_or_default()
            .contains("incomplete")
    );

    let mut foreign = hollow_probe_checkpoint(&db_path, 1_000);
    foreign.db_path = temp.path().join("other.db").display().to_string();
    let foreign_state = hollow_probe_state(&index_path, &db_path, Some(&foreign), Some(3));
    assert!(
        !foreign_state.hollow,
        "a foreign checkpoint certifies nothing here"
    );

    let unreadable = hollow_probe_state(
        &index_path,
        &db_path,
        Some(&hollow_probe_checkpoint(&db_path, 1_000)),
        None,
    );
    assert!(!unreadable.hollow, "no manifest means no verdict");
    assert_eq!(unreadable.status, "ready");

    let zero_certified = hollow_probe_state(
        &index_path,
        &db_path,
        Some(&hollow_probe_checkpoint(&db_path, 0)),
        Some(0),
    );
    assert!(!zero_certified.hollow, "an empty archive certifies nothing");
}

#[test]
fn hollow_verdict_math_is_exact_at_the_floor_and_saturates() {
    assert_eq!(lexical_generation_hollow_verdict(None, Some(0)), None);
    assert_eq!(lexical_generation_hollow_verdict(Some(0), Some(0)), None);
    assert_eq!(lexical_generation_hollow_verdict(Some(10), None), None);
    assert_eq!(lexical_generation_hollow_verdict(Some(10), Some(5)), None);
    let verdict = lexical_generation_hollow_verdict(Some(10), Some(4)).expect("hollow");
    assert_eq!(verdict.missing_percent(), 60);
    let huge = lexical_generation_hollow_verdict(Some(u64::MAX), Some(1)).expect("hollow");
    assert_eq!(huge.missing_percent(), 99);
    assert!(huge.reason().contains("Run `cass index`"));
}

/// b4uax (gh#382): a Tantivy-era `meta.json` without a Quill MANIFEST is
/// an existing-but-unreadable generation: not fresh, not "ready", and the
/// reason names the exact rebuild command.
#[test]
fn legacy_tantivy_generation_reports_engine_incompatible() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v9-quill");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(index_path.join("meta.json"), b"{}").expect("write legacy meta.json");
    let db_path = temp.path().join("agent_search.db");
    std::fs::write(&db_path, b"db").expect("write db file");

    let state = lexical_state_from_observations(LexicalObservationInput {
        index_path: &index_path,
        db_path: &db_path,
        stale_threshold: 3600,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms: 1_733_000_001_000,
        maintenance: SearchMaintenanceSnapshot::default(),
        checkpoint: None,
        current_db_fingerprint: None,
        live: None,
    });
    assert!(state.exists, "a legacy generation still counts as existing");
    assert!(state.engine_incompatible);
    assert!(!state.fresh);
    assert!(state.stale);
    assert_eq!(state.status, "legacy_engine");
    let reason = state.status_reason.as_deref().unwrap_or_default();
    assert!(reason.contains("cass index --full"));
    assert!(reason.contains("Tantivy"));
}

#[test]
fn lexical_state_marks_fingerprint_mismatch_stale() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v4");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    // Simulate an existing Quill generation (MANIFEST present) so the
    // "missing" branch in lexical_state_from_observations doesn't short
    // circuit before the fingerprint check we want to exercise.
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    let db_path = temp.path().join("agent_search.db");
    std::fs::write(&db_path, b"db").expect("write db file");

    let checkpoint = LexicalRebuildCheckpoint {
        db_path: db_path.display().to_string(),
        total_conversations: 10,
        storage_fingerprint: "before".to_string(),
        committed_offset: 10,
        committed_conversation_id: Some(10),
        processed_conversations: 10,
        indexed_docs: 100,
        indexed_region_docs: 0,
        schema_hash: SCHEMA_HASH.to_string(),
        page_size: LEXICAL_REBUILD_PAGE_SIZE_PUBLIC,
        completed: true,
        updated_at_ms: 1_733_000_000_000,
    };

    let state = lexical_state_from_observations(LexicalObservationInput {
        index_path: &index_path,
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms: 1_733_000_001_000,
        maintenance: SearchMaintenanceSnapshot::default(),
        checkpoint: Some(&checkpoint),
        current_db_fingerprint: Some("after"),
        live: None,
    });

    assert_eq!(state.status, "stale");
    assert_eq!(
        state.fingerprint.matches_current_db_fingerprint,
        Some(false)
    );
    assert!(
        state
            .status_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("fingerprint"))
    );
    assert_eq!(state.pending_sessions, 0);
    assert_eq!(state.processed_conversations, None);
    assert_eq!(state.total_conversations, None);
    assert_eq!(state.indexed_docs, None);
}

/// GH #452: an index whose checkpoint fingerprint still matches the live
/// database has nothing new to ingest, so an old `last_indexed_at` alone
/// must report `ready` — not `stale`, which sends every ordinary read into
/// an archive-scale refresh.
#[test]
fn lexical_state_treats_age_as_covered_by_a_matching_fingerprint() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v4");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    let db_path = temp.path().join("agent_search.db");
    std::fs::write(&db_path, b"db").expect("write db file");

    let checkpoint = LexicalRebuildCheckpoint {
        db_path: db_path.display().to_string(),
        total_conversations: 10,
        storage_fingerprint: "content-v1:10:10:100".to_string(),
        committed_offset: 10,
        committed_conversation_id: Some(10),
        processed_conversations: 10,
        indexed_docs: 100,
        indexed_region_docs: 0,
        schema_hash: SCHEMA_HASH.to_string(),
        page_size: LEXICAL_REBUILD_PAGE_SIZE_PUBLIC,
        completed: true,
        updated_at_ms: 1_733_000_000_000,
    };
    // Indexed long ago (well past the 60 s threshold) but the database has
    // not gained a conversation or a message since.
    let input = |current_db_fingerprint: Option<&'static str>| LexicalObservationInput {
        index_path: &index_path,
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms: 1_733_000_600_000,
        maintenance: SearchMaintenanceSnapshot::default(),
        checkpoint: Some(&checkpoint),
        current_db_fingerprint,
        live: None,
    };

    let covered = lexical_state_from_observations(input(Some("content-v1:10:10:100")));
    assert_eq!(
        covered.status, "ready",
        "a matching fingerprint covers pure age-staleness"
    );
    assert_eq!(covered.status_reason, None);

    // New content since the checkpoint still reports stale.
    let changed = lexical_state_from_observations(input(Some("content-v1:11:11:120")));
    assert_eq!(changed.status, "stale");

    // No fingerprint probe at all: age still decides, unchanged.
    let unprobed = lexical_state_from_observations(input(None));
    assert_eq!(unprobed.status, "stale");
    assert!(
        unprobed
            .status_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("older than the stale threshold"))
    );
}

/// An index with a matching fingerprint but NO recorded `last_indexed_at`
/// is unproven, not merely old: it stays stale.
#[test]
fn lexical_state_keeps_unknown_age_stale_even_with_a_matching_fingerprint() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v4");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    let db_path = temp.path().join("agent_search.db");
    std::fs::write(&db_path, b"db").expect("write db file");

    let checkpoint = LexicalRebuildCheckpoint {
        db_path: db_path.display().to_string(),
        total_conversations: 10,
        storage_fingerprint: "content-v1:10:10:100".to_string(),
        committed_offset: 10,
        committed_conversation_id: Some(10),
        processed_conversations: 10,
        indexed_docs: 100,
        indexed_region_docs: 0,
        schema_hash: SCHEMA_HASH.to_string(),
        page_size: LEXICAL_REBUILD_PAGE_SIZE_PUBLIC,
        completed: true,
        updated_at_ms: 1_733_000_000_000,
    };

    let state = lexical_state_from_observations(LexicalObservationInput {
        index_path: &index_path,
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: None,
        now_ms: 1_733_000_600_000,
        maintenance: SearchMaintenanceSnapshot::default(),
        checkpoint: Some(&checkpoint),
        current_db_fingerprint: Some("content-v1:10:10:100"),
        live: None,
    });

    assert_eq!(state.status, "stale");
}

#[test]
fn lexical_state_marks_checkpoint_db_mismatch_stale_without_fingerprint_probe() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v4");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    let db_path = temp.path().join("agent_search.db");
    let other_db_path = temp.path().join("other_agent_search.db");
    std::fs::write(&db_path, b"db").expect("write db file");
    std::fs::write(&other_db_path, b"other db").expect("write other db file");

    let checkpoint = LexicalRebuildCheckpoint {
        db_path: other_db_path.display().to_string(),
        total_conversations: 10,
        storage_fingerprint: "old-db-fingerprint".to_string(),
        committed_offset: 10,
        committed_conversation_id: Some(10),
        processed_conversations: 10,
        indexed_docs: 100,
        indexed_region_docs: 0,
        schema_hash: SCHEMA_HASH.to_string(),
        page_size: LEXICAL_REBUILD_PAGE_SIZE_PUBLIC,
        completed: true,
        updated_at_ms: 1_733_000_000_000,
    };

    let state = lexical_state_from_observations(LexicalObservationInput {
        index_path: &index_path,
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms: 1_733_000_001_000,
        maintenance: SearchMaintenanceSnapshot::default(),
        checkpoint: Some(&checkpoint),
        current_db_fingerprint: None,
        live: None,
    });

    assert_eq!(state.status, "stale");
    assert!(state.stale);
    assert!(!state.fresh);
    assert_eq!(state.checkpoint.db_matches, Some(false));
    assert_eq!(state.fingerprint.matches_current_db_fingerprint, None);
    assert_eq!(state.pending_sessions, 0);
    assert_eq!(state.processed_conversations, None);
    assert_eq!(state.total_conversations, None);
    assert_eq!(state.indexed_docs, None);
    assert!(
        state
            .status_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("different database"))
    );
}

#[test]
fn lexical_state_missing_index_is_not_marked_stale_until_initialized() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v4");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    let db_path = temp.path().join("agent_search.db");
    std::fs::write(&db_path, b"db").expect("write db file");

    let state = lexical_state_from_observations(LexicalObservationInput {
        index_path: &index_path,
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: None,
        now_ms: 1_733_000_001_000,
        maintenance: SearchMaintenanceSnapshot::default(),
        checkpoint: None,
        current_db_fingerprint: None,
        live: None,
    });

    assert_eq!(state.status, "missing");
    assert!(!state.exists);
    assert!(!state.stale);
    assert!(!state.fresh);
    assert_eq!(
        state.status_reason.as_deref(),
        Some("lexical index metadata missing")
    );
}

#[test]
fn lexical_state_keeps_progress_visible_during_active_rebuild_despite_fingerprint_drift() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v4");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    let db_path = temp.path().join("agent_search.db");
    std::fs::write(&db_path, b"db").expect("write db file");

    let checkpoint = LexicalRebuildCheckpoint {
        db_path: db_path.display().to_string(),
        total_conversations: 10,
        storage_fingerprint: "before".to_string(),
        committed_offset: 4,
        committed_conversation_id: Some(4),
        processed_conversations: 4,
        indexed_docs: 20,
        indexed_region_docs: 0,
        schema_hash: SCHEMA_HASH.to_string(),
        page_size: 200,
        completed: false,
        updated_at_ms: 1_733_000_123_000,
    };

    let state = lexical_state_from_observations(LexicalObservationInput {
        index_path: &index_path,
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms: 1_733_000_001_000,
        maintenance: SearchMaintenanceSnapshot {
            active: true,
            pid: Some(std::process::id()),
            started_at_ms: Some(1_733_000_111_000),
            db_path: Some(db_path.clone()),
            mode: Some(SearchMaintenanceMode::Index),
            job_id: None,
            job_kind: None,
            phase: None,
            updated_at_ms: None,
            last_progress_at_ms: None,
            orphaned: false,
        },
        checkpoint: Some(&checkpoint),
        current_db_fingerprint: Some("after"),
        live: None,
    });

    assert_eq!(state.status, "building");
    assert!(!state.stale);
    assert!(!state.fresh);
    assert_eq!(state.pending_sessions, 6);
    assert_eq!(state.processed_conversations, Some(4));
    assert_eq!(state.total_conversations, Some(10));
    assert_eq!(state.indexed_docs, Some(20));
    assert_eq!(state.checkpoint.page_size_matches, Some(false));
    assert_eq!(state.checkpoint.page_size_compatible, Some(true));
    assert_eq!(
        state.status_reason.as_deref(),
        Some("lexical rebuild is in progress")
    );
}

#[test]
fn lexical_state_hides_progress_for_incompatible_page_size_checkpoint() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v4");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    let db_path = temp.path().join("agent_search.db");
    std::fs::write(&db_path, b"db").expect("write db file");

    let checkpoint = LexicalRebuildCheckpoint {
        db_path: db_path.display().to_string(),
        total_conversations: 10,
        storage_fingerprint: "before".to_string(),
        committed_offset: 4,
        committed_conversation_id: Some(4),
        processed_conversations: 4,
        indexed_docs: 20,
        indexed_region_docs: 0,
        schema_hash: SCHEMA_HASH.to_string(),
        page_size: 13,
        completed: false,
        updated_at_ms: 1_733_000_123_000,
    };

    let state = lexical_state_from_observations(LexicalObservationInput {
        index_path: &index_path,
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms: 1_733_000_001_000,
        maintenance: SearchMaintenanceSnapshot::default(),
        checkpoint: Some(&checkpoint),
        current_db_fingerprint: Some("before"),
        live: None,
    });

    assert_eq!(state.status, "stale");
    assert!(state.stale);
    assert_eq!(state.pending_sessions, 0);
    assert_eq!(state.processed_conversations, None);
    assert_eq!(state.total_conversations, None);
    assert_eq!(state.indexed_docs, None);
    assert_eq!(state.checkpoint.page_size_matches, Some(false));
    assert_eq!(state.checkpoint.page_size_compatible, Some(false));
    assert!(
        state
            .status_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("contract"))
    );
}

#[test]
fn lexical_state_prefers_newer_maintenance_heartbeat_over_stale_checkpoint_timestamp() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v4");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    let db_path = temp.path().join("agent_search.db");
    std::fs::write(&db_path, b"db").expect("write db file");

    let checkpoint = LexicalRebuildCheckpoint {
        db_path: db_path.display().to_string(),
        total_conversations: 10,
        storage_fingerprint: "before".to_string(),
        committed_offset: 4,
        committed_conversation_id: Some(4),
        processed_conversations: 4,
        indexed_docs: 20,
        indexed_region_docs: 0,
        schema_hash: SCHEMA_HASH.to_string(),
        page_size: LEXICAL_REBUILD_PAGE_SIZE_PUBLIC,
        completed: false,
        updated_at_ms: 1_733_000_123_000,
    };

    let state = lexical_state_from_observations(LexicalObservationInput {
        index_path: &index_path,
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms: 1_733_000_001_000,
        maintenance: SearchMaintenanceSnapshot {
            active: true,
            pid: Some(std::process::id()),
            started_at_ms: Some(1_733_000_111_000),
            db_path: Some(db_path.clone()),
            mode: Some(SearchMaintenanceMode::Index),
            job_id: None,
            job_kind: None,
            phase: None,
            updated_at_ms: Some(1_733_000_456_000),
            last_progress_at_ms: None,
            orphaned: false,
        },
        checkpoint: Some(&checkpoint),
        current_db_fingerprint: Some("after"),
        live: None,
    });

    assert_eq!(state.status, "building");
    assert_eq!(state.activity_at_ms, Some(1_733_000_456_000));
}

#[test]
fn lexical_state_ignores_rebuild_lock_for_different_database() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v4");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    let db_path = temp.path().join("agent_search.db");
    std::fs::write(&db_path, b"db").expect("write db file");
    let other_db_path = temp.path().join("other.db");
    std::fs::write(&other_db_path, b"other").expect("write other db file");

    let checkpoint = LexicalRebuildCheckpoint {
        db_path: db_path.display().to_string(),
        total_conversations: 10,
        storage_fingerprint: "before".to_string(),
        committed_offset: 4,
        committed_conversation_id: Some(4),
        processed_conversations: 4,
        indexed_docs: 20,
        indexed_region_docs: 0,
        schema_hash: SCHEMA_HASH.to_string(),
        page_size: LEXICAL_REBUILD_PAGE_SIZE_PUBLIC,
        completed: false,
        updated_at_ms: 1_733_000_123_000,
    };

    let state = lexical_state_from_observations(LexicalObservationInput {
        index_path: &index_path,
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms: 1_733_000_001_000,
        maintenance: SearchMaintenanceSnapshot {
            active: true,
            pid: Some(std::process::id()),
            started_at_ms: Some(1_733_000_111_000),
            db_path: Some(other_db_path),
            mode: Some(SearchMaintenanceMode::Index),
            job_id: None,
            job_kind: None,
            phase: None,
            updated_at_ms: None,
            last_progress_at_ms: None,
            orphaned: false,
        },
        checkpoint: Some(&checkpoint),
        current_db_fingerprint: Some("after"),
        live: None,
    });

    assert_eq!(state.status, "stale");
    assert!(state.stale);
    assert!(!state.fresh);
    assert!(!state.rebuilding);
    assert!(!state.watch_active);
    assert_eq!(state.activity_at_ms, None);
    assert_eq!(state.pending_sessions, 0);
    assert_eq!(state.processed_conversations, None);
    assert_eq!(state.total_conversations, None);
    assert_eq!(state.indexed_docs, None);
    assert!(
        state
            .status_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("fingerprint"))
    );
}

#[test]
fn lexical_state_ignores_watch_lock_for_different_database() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v4");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    let db_path = temp.path().join("agent_search.db");
    std::fs::write(&db_path, b"db").expect("write db file");
    let other_db_path = temp.path().join("other.db");
    std::fs::write(&other_db_path, b"other").expect("write other db file");

    let state = lexical_state_from_observations(LexicalObservationInput {
        index_path: &index_path,
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms: 1_733_000_020_000,
        maintenance: SearchMaintenanceSnapshot {
            active: true,
            pid: Some(std::process::id()),
            started_at_ms: Some(1_733_000_111_000),
            db_path: Some(other_db_path),
            mode: Some(SearchMaintenanceMode::Watch),
            job_id: None,
            job_kind: None,
            phase: None,
            updated_at_ms: None,
            last_progress_at_ms: None,
            orphaned: false,
        },
        checkpoint: None,
        current_db_fingerprint: None,
        live: None,
    });

    assert_eq!(state.status, "ready");
    assert!(state.fresh);
    assert!(!state.stale);
    assert!(!state.rebuilding);
    assert!(!state.watch_active);
    assert_eq!(state.activity_at_ms, None);
}

// ---- Forward-progress liveness / stall detection (issue #258) ----

/// `last_progress_at_ms` is older than the default 120 s stall
/// threshold; the heartbeat `updated_at_ms` is fresh (the heartbeat
/// thread kept refreshing it independently). Status must flip to
/// `stalled`, NOT remain `building` — this is the regression #258
/// guards against.
#[test]
fn lexical_state_reports_stalled_when_progress_is_stale_despite_fresh_heartbeat() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v4");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    let db_path = temp.path().join("agent_search.db");
    std::fs::write(&db_path, b"db").expect("write db file");

    // now = 1_733_000_300 s (= 1_733_000_300_000 ms).
    // heartbeat updated 500 ms ago: fresh.
    // forward progress posted 300 s ago: well past the 120 s default stall threshold.
    // F4 (cass tech debt): the input now carries full-precision
    // `now_ms` end-to-end. Tests that previously needed
    // `now_secs as i64 * 1000` no longer have to round-trip.
    let now_ms: i64 = 1_733_000_300_000;

    let state = lexical_state_from_observations(LexicalObservationInput {
        index_path: &index_path,
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms,
        maintenance: SearchMaintenanceSnapshot {
            active: true,
            pid: Some(std::process::id()),
            started_at_ms: Some(now_ms - 600_000),
            db_path: Some(db_path.clone()),
            mode: Some(SearchMaintenanceMode::WatchStartup),
            job_id: Some("lexical_refresh-1-1".to_string()),
            job_kind: Some(SearchMaintenanceJobKind::LexicalRefresh),
            phase: Some("watch_startup".to_string()),
            updated_at_ms: Some(now_ms - 500),
            last_progress_at_ms: Some(now_ms - 300_000),
            orphaned: false,
        },
        checkpoint: None,
        current_db_fingerprint: None,
        live: None,
    });

    assert!(state.rebuilding, "active rebuild lock must still register");
    assert!(
        state.stalled,
        "stale forward-progress timestamp must flip stalled=true",
    );
    assert_eq!(state.status, "stalled");
    assert_eq!(
        state.last_progress_at_ms,
        Some(now_ms - 300_000),
        "last_progress_at_ms must be surfaced to status callers",
    );
    let age = state
        .last_progress_age_ms
        .expect("last_progress_age_ms must be computed");
    assert!(
        (299_900..=300_100).contains(&age),
        "computed last_progress_age_ms ({age}ms) should equal now - last_progress_at_ms (300_000ms)",
    );
    let reason = state
        .status_reason
        .as_deref()
        .expect("stalled state should populate status_reason");
    assert!(
        reason.contains("forward progress")
            && (reason.contains("#258") || reason.contains("issue #258")),
        "status_reason should mention forward progress and reference #258 ({reason})",
    );
}

/// Heartbeat is fresh AND forward-progress is fresh: no stall.
/// Status remains `building`. Ensures the new gate does not regress
/// the happy path.
#[test]
fn lexical_state_stays_building_when_progress_is_recent() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v4");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    let db_path = temp.path().join("agent_search.db");
    std::fs::write(&db_path, b"db").expect("write db file");

    // F4 (cass tech debt): the input now carries full-precision
    // `now_ms` end-to-end. Tests that previously needed
    // `now_secs as i64 * 1000` no longer have to round-trip.
    let now_ms: i64 = 1_733_000_300_000;

    let state = lexical_state_from_observations(LexicalObservationInput {
        index_path: &index_path,
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms,
        maintenance: SearchMaintenanceSnapshot {
            active: true,
            pid: Some(std::process::id()),
            started_at_ms: Some(now_ms - 30_000),
            db_path: Some(db_path.clone()),
            mode: Some(SearchMaintenanceMode::Index),
            job_id: Some("lexical_refresh-1-1".to_string()),
            job_kind: Some(SearchMaintenanceJobKind::LexicalRefresh),
            phase: Some("scanning".to_string()),
            updated_at_ms: Some(now_ms - 500),
            last_progress_at_ms: Some(now_ms - 1_000),
            orphaned: false,
        },
        checkpoint: None,
        current_db_fingerprint: None,
        live: None,
    });

    assert!(state.rebuilding);
    assert!(!state.stalled, "fresh progress must not flip stalled");
    assert_eq!(state.status, "building");
}

/// Legacy lock files (older cass that didn't write
/// `last_progress_at_ms`) must not be misreported as stalled. The
/// stall check only fires when an explicit `last_progress_at_ms`
/// is present; absent that, we fall back to the previous behavior.
#[test]
fn lexical_state_does_not_stall_when_legacy_lock_omits_progress_field() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v4");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    let db_path = temp.path().join("agent_search.db");
    std::fs::write(&db_path, b"db").expect("write db file");

    // F4 (cass tech debt): the input now carries full-precision
    // `now_ms` end-to-end. Tests that previously needed
    // `now_secs as i64 * 1000` no longer have to round-trip.
    let now_ms: i64 = 1_733_000_300_000;

    let state = lexical_state_from_observations(LexicalObservationInput {
        index_path: &index_path,
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms,
        maintenance: SearchMaintenanceSnapshot {
            active: true,
            pid: Some(std::process::id()),
            started_at_ms: Some(now_ms - 30_000),
            db_path: Some(db_path.clone()),
            mode: Some(SearchMaintenanceMode::Index),
            job_id: None,
            job_kind: None,
            phase: None,
            updated_at_ms: Some(now_ms - 500),
            last_progress_at_ms: None,
            orphaned: false,
        },
        checkpoint: None,
        current_db_fingerprint: None,
        live: None,
    });

    assert!(state.rebuilding);
    assert!(
        !state.stalled,
        "legacy lock without last_progress_at_ms must NOT be misreported as stalled",
    );
    assert_eq!(state.status, "building");
    assert!(state.last_progress_age_ms.is_none());
}

/// F4 (cass tech debt): the stall-age comparison must operate at
/// full millisecond precision. Pre-fix, `now_ms` was derived from
/// `now_secs * 1000` inside `lexical_state_from_observations`, which
/// quantised the comparison to second resolution and made a
/// 119_900 ms-old progress timestamp indistinguishable from a 119
/// 000 ms-old one. This test pins a 119_500 ms age so that the only
/// way it surfaces correctly as a `last_progress_age_ms` close to
/// 119_500 (and NOT 119_000 or 120_000) is full-ms plumbing.
#[test]
fn lexical_state_progress_age_is_ms_precision_not_seconds_quantised() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = temp.path().join("index").join("v4");
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");
    let db_path = temp.path().join("agent_search.db");
    std::fs::write(&db_path, b"db").expect("write db file");

    // `now_ms` deliberately lands at .700 of a second so any
    // second-quantisation upstream would shave 700 ms off the diff.
    let now_ms: i64 = 1_733_000_300_700;
    let last_progress_at_ms = now_ms - 119_500;

    let state = lexical_state_from_observations(LexicalObservationInput {
        index_path: &index_path,
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms,
        maintenance: SearchMaintenanceSnapshot {
            active: true,
            pid: Some(std::process::id()),
            started_at_ms: Some(now_ms - 600_000),
            db_path: Some(db_path.clone()),
            mode: Some(SearchMaintenanceMode::WatchStartup),
            job_id: Some("lexical_refresh-1-1".to_string()),
            job_kind: Some(SearchMaintenanceJobKind::LexicalRefresh),
            phase: Some("watch_startup".to_string()),
            updated_at_ms: Some(now_ms - 500),
            last_progress_at_ms: Some(last_progress_at_ms),
            orphaned: false,
        },
        checkpoint: None,
        current_db_fingerprint: None,
        live: None,
    });

    let age = state
        .last_progress_age_ms
        .expect("forward-progress age must be computed");
    assert_eq!(
        age, 119_500,
        "ms-precision plumbing must surface the exact diff (no second-quantisation)"
    );
    // 119_500 ms is still under the default 120 s stall threshold,
    // so we should be `building`, not `stalled`. Pre-F4, a
    // second-quantised clock could either floor the diff to 119_000
    // (still building, OK) OR — on different `.fff` ms suffixes —
    // round it to 120_000 (false-positive stall). Pinning the
    // expected status here protects both edges.
    assert!(
        !state.stalled,
        "119.5 s lag must remain `building`, not flip to `stalled`",
    );
    assert_eq!(state.status, "building");
}

#[test]
fn inspect_lexical_assets_primes_fingerprint_cache_for_skip_open_health() {
    let temp = tempfile::tempdir().expect("tempdir");
    let index_path = crate::search::tantivy::expected_index_dir(temp.path());
    std::fs::create_dir_all(&index_path).expect("create index dir");
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"{}",
    )
    .expect("write quill manifest");

    let db_path = temp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).expect("open canonical archive");
    let fingerprint =
        crate::storage::archive_fingerprint::lexical_storage_fingerprint_for_storage(&storage)
            .expect("compute storage fingerprint");
    storage.close().expect("close storage");

    let checkpoint_json = serde_json::json!({
        "version": 3,
        "schema_hash": SCHEMA_HASH,
        "page_size": LEXICAL_REBUILD_PAGE_SIZE_PUBLIC,
        "db": {
            "db_path": db_path.display().to_string(),
            "total_conversations": 0,
            "total_messages": 0,
            "storage_fingerprint": fingerprint,
        },
        "committed_offset": 0,
        "committed_conversation_id": null,
        "processed_conversations": 0,
        "indexed_docs": 0,
        "committed_meta_fingerprint": "test-meta",
        "pending": null,
        "completed": true,
        "updated_at_ms": 1_733_000_000_000_i64,
    });
    std::fs::write(
        index_path.join(".lexical-rebuild-state.json"),
        serde_json::to_vec(&checkpoint_json).expect("serialize checkpoint"),
    )
    .expect("write checkpoint");

    // Before priming the sidecar cache, skip-open health inspection cannot
    // observe the fingerprint and therefore marks an age-exceeded archive stale.
    let unprimed_health = inspect_lexical_assets(InspectLexicalAssetsInput {
        data_dir: temp.path(),
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms: 1_733_000_600_000,
        maintenance: SearchMaintenanceSnapshot::default(),
        db_available: true,
        compute_lexical_fingerprint: false,
    })
    .expect("unprimed skip-open inspection");
    assert_eq!(
        unprimed_health.fingerprint.current_db_fingerprint, None,
        "without sidecar cache, skip-open health has no fingerprint"
    );
    assert!(unprimed_health.stale);

    // Status-mode inspection computes the fingerprint and primes the sidecar cache.
    let status_view = inspect_lexical_assets(InspectLexicalAssetsInput {
        data_dir: temp.path(),
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms: 1_733_000_600_000,
        maintenance: SearchMaintenanceSnapshot::default(),
        db_available: true,
        compute_lexical_fingerprint: true,
    })
    .expect("status inspection");
    assert_eq!(status_view.status, "ready");
    assert!(!status_view.stale);
    assert_eq!(
        status_view.fingerprint.matches_current_db_fingerprint,
        Some(true)
    );

    // Subsequent skip-open health inspection now reads the primed sidecar cache
    // and agrees with status on fingerprint and freshness.
    let primed_health = inspect_lexical_assets(InspectLexicalAssetsInput {
        data_dir: temp.path(),
        db_path: &db_path,
        stale_threshold: 60,
        last_indexed_at_ms: Some(1_733_000_000_000),
        now_ms: 1_733_000_600_000,
        maintenance: SearchMaintenanceSnapshot::default(),
        db_available: true,
        compute_lexical_fingerprint: false,
    })
    .expect("primed skip-open inspection");
    assert_eq!(primed_health.status, "ready");
    assert!(!primed_health.stale);
    assert!(primed_health.fresh);
    assert_eq!(
        primed_health.fingerprint.current_db_fingerprint,
        status_view.fingerprint.current_db_fingerprint
    );
    assert_eq!(
        primed_health.fingerprint.matches_current_db_fingerprint,
        Some(true)
    );
}

#[test]
#[serial_test::serial]
fn legacy_omp_reclassification_preserves_one_canonical_conversation() -> anyhow::Result<()> {
    use crate::franken_sync::compat::{
        ConnectionExt as FrankenConnectionExt, ParamValue, RowExt as FrankenRowExt,
    };
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use crate::sources::provenance::LOCAL_SOURCE_ID;
    use crate::storage::sqlite::{
        LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY,
        LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY,
        LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY, LegacyOmpReclassificationResult,
        SqliteStorage, legacy_omp_analytics_pending, tests::set_env_var,
    };

    let dir = tempfile::TempDir::new()?;
    let _pi_sessions = set_env_var("PI_SESSIONS_DIR", "");
    let _omp_sessions = set_env_var("PI_CODING_AGENT_SESSION_DIR", "");
    let _shared_agent = set_env_var("PI_CODING_AGENT_DIR", "");
    let _omp_archive = set_env_var("CASS_OMP_DATA_ROOT", "");
    let _config_dir = set_env_var("PI_CONFIG_DIR", "");
    let _profile = set_env_var("OMP_PROFILE", "");
    let _legacy_profile = set_env_var("PI_PROFILE", "");
    let _xdg = set_env_var("XDG_DATA_HOME", "");
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path)?;
    let pi_agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "pi_agent".into(),
        name: "Pi Agent".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    let mut conversation = Conversation {
        id: None,
        agent_slug: "pi_agent".into(),
        workspace: None,
        external_id: Some("-projects-cass/legacy-omp".into()),
        title: Some("Legacy OMP".into()),
        source_path: dir
            .path()
            .join("home/.omp/profiles/work/agent/sessions/-projects-cass/legacy-omp.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_001_000),
        approx_tokens: None,
        metadata_json: serde_json::json!({"source":"pi_agent"}),
        messages: vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::Agent,
            author: Some("openrouter/stealth/ox-alpha".into()),
            created_at: Some(1_700_000_001_000),
            content: "legacy OMP answer".into(),
            extra_json: serde_json::json!({"message":{"model":"openrouter/stealth/ox-alpha"}}),
            snippets: Vec::new(),
        }],
        source_id: LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };
    storage.insert_conversations_batched(&[(pi_agent_id, None, &conversation)])?;

    let legacy_token_agent_id: i64 = storage.raw().query_row_map(
        "SELECT agent_id FROM token_usage LIMIT 1",
        &[] as &[ParamValue],
        |row| row.get_typed(0),
    )?;
    assert_eq!(legacy_token_agent_id, pi_agent_id);
    let legacy_metrics_slug: String = storage.raw().query_row_map(
        "SELECT agent_slug FROM message_metrics LIMIT 1",
        &[] as &[ParamValue],
        |row| row.get_typed(0),
    )?;
    assert_eq!(legacy_metrics_slug, "pi_agent");
    let legacy_message_id = storage
        .max_message_id()?
        .expect("legacy OMP fixture should contain one message");
    storage.set_last_embedded_message_id(legacy_message_id)?;
    for tier in [SemanticIdentityTier::Fast, SemanticIdentityTier::Quality] {
        assert!(!storage.semantic_identity_rebuild_required(tier)?);
    }

    assert_eq!(
        storage.reclassify_legacy_omp_conversations()?,
        LegacyOmpReclassificationResult {
            conversations_reclassified: 1,
            lexical_rebuild_required: true,
            analytics_rebuild_required: true,
        }
    );
    assert_eq!(
        storage.get_last_embedded_message_id()?,
        None,
        "agent/source identity changes must invalidate the rowid-only semantic watermark"
    );
    for tier in [SemanticIdentityTier::Fast, SemanticIdentityTier::Quality] {
        assert!(
            storage.semantic_identity_rebuild_required(tier)?,
            "each semantic tier must fail closed until its Pi-era doc ids are republished"
        );
    }
    assert_eq!(
        storage.reclassify_legacy_omp_conversations()?,
        LegacyOmpReclassificationResult {
            conversations_reclassified: 0,
            lexical_rebuild_required: true,
            analytics_rebuild_required: true,
        }
    );

    let conversations = storage.list_conversations(10, 0)?;
    assert_eq!(conversations.len(), 1);
    assert_eq!(conversations[0].agent_slug, "omp");
    assert_eq!(conversations[0].metadata_json["source"], "omp");
    assert_eq!(
        conversations[0].metadata_json["profile"], "work",
        "the structural OMP profile must survive legacy Pi ownership and remote-safe migration"
    );
    let omp_agent_id: i64 = storage.raw().query_row_map(
        "SELECT id FROM agents WHERE slug = 'omp'",
        &[] as &[ParamValue],
        |row| row.get_typed(0),
    )?;
    let token_agent_id: i64 = storage.raw().query_row_map(
        "SELECT agent_id FROM token_usage LIMIT 1",
        &[] as &[ParamValue],
        |row| row.get_typed(0),
    )?;
    assert_eq!(token_agent_id, omp_agent_id);
    let metrics_slug: String = storage.raw().query_row_map(
        "SELECT agent_slug FROM message_metrics LIMIT 1",
        &[] as &[ParamValue],
        |row| row.get_typed(0),
    )?;
    assert_eq!(
        metrics_slug, "pi_agent",
        "canonical identity migration must not block on the deferred analytics rebuild"
    );

    // The lexical publish can finish while an operator explicitly defers
    // analytics. A retry must retain only the analytics work; otherwise
    // every startup would repeat the expensive authoritative lexical
    // rebuild that already published successfully.
    let mut canonical = storage.list_conversations(10, 0)?.remove(0);
    canonical.messages = storage.fetch_messages(canonical.id.unwrap())?;
    let lexical_path = dir.path().join("omp-lexical");
    let mut lexical = crate::search::tantivy::TantivyIndex::open_or_create(&lexical_path)?;
    let packet = crate::model::conversation_packet::ConversationPacket::from_canonical_replay(
        &canonical,
        crate::model::conversation_packet::ConversationPacketProvenance::local(),
    );
    lexical.add_messages_from_packet(&packet, None, canonical.id, |_| Ok(()))?;
    lexical.commit()?;
    drop(lexical);
    let published_manifest = std::fs::read(lexical_path.join("MANIFEST"))?;
    storage.mark_legacy_omp_lexical_publish_complete()?;
    assert!(legacy_omp_analytics_pending(storage.raw())?);
    assert_eq!(
        storage.reclassify_legacy_omp_conversations()?,
        LegacyOmpReclassificationResult {
            conversations_reclassified: 0,
            lexical_rebuild_required: false,
            analytics_rebuild_required: true,
        }
    );
    let progress_events = std::cell::Cell::new(0_usize);
    let heartbeat_events = std::cell::Cell::new(0_usize);
    let record_progress = |_processed: i64, _total: i64| {
        progress_events.set(progress_events.get().saturating_add(1));
    };
    let record_heartbeat = || {
        heartbeat_events.set(heartbeat_events.get().saturating_add(1));
    };
    let stop_after_metrics = std::cell::Cell::new(false);
    let stop_progress = |processed: i64, total: i64| {
        if processed > 0 && processed == total {
            stop_after_metrics.set(true);
        }
    };
    let stop_control = || -> anyhow::Result<()> {
        anyhow::ensure!(
            !stop_after_metrics.get(),
            "planted stop before token rollups"
        );
        Ok(())
    };
    let cancelled = storage
        .rebuild_legacy_omp_analytics_with_progress(
            Some(&stop_progress),
            Some(&record_heartbeat),
            Some(&stop_control),
        )
        .expect_err("finishing message metrics must not certify remaining analytics phases");
    assert!(format!("{cancelled:#}").contains("planted stop before token rollups"));
    let pending = storage.reclassify_legacy_omp_conversations()?;
    assert!(!pending.lexical_rebuild_required);
    assert!(pending.analytics_rebuild_required);
    assert!(legacy_omp_analytics_pending(storage.raw())?);
    assert_eq!(
        std::fs::read(lexical_path.join("MANIFEST"))?,
        published_manifest
    );
    let unknown_phases = std::cell::Cell::new(0_usize);
    let resumed_progress = |processed: i64, total: i64| {
        record_progress(processed, total);
        if processed == 0 && total == 0 {
            unknown_phases.set(unknown_phases.get() + 1);
        }
    };
    let _ = storage.rebuild_legacy_omp_analytics_with_progress(
        Some(&resumed_progress),
        Some(&record_heartbeat),
        None,
    )?;
    assert_eq!(
        unknown_phases.get(),
        2,
        "non-message phases must revoke the completed message denominator"
    );
    assert!(
        progress_events.get() >= 2,
        "message-metrics rebuild must report start and committed progress"
    );
    assert!(
        heartbeat_events.get() >= 2,
        "token and daily rollup phases must keep the index watchdog alive"
    );
    let rebuilt_metrics_slug: String = storage.raw().query_row_map(
        "SELECT agent_slug FROM message_metrics LIMIT 1",
        &[] as &[ParamValue],
        |row| row.get_typed(0),
    )?;
    assert_eq!(rebuilt_metrics_slug, "omp");
    assert!(!legacy_omp_analytics_pending(storage.raw())?);
    assert_eq!(
        std::fs::read(lexical_path.join("MANIFEST"))?,
        published_manifest
    );
    for cursor_key in [
        LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY,
        LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY,
        LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY,
    ] {
        let cursor_rows: i64 = storage.raw().query_row_map(
            "SELECT COUNT(*) FROM meta WHERE key = ?1",
            &[ParamValue::from(cursor_key)],
            |row| row.get_typed(0),
        )?;
        assert_eq!(
            cursor_rows, 0,
            "completed analytics must atomically retire cursor {cursor_key}"
        );
    }
    assert!(storage.semantic_identity_rebuild_required(SemanticIdentityTier::Fast)?);
    assert!(storage.semantic_identity_rebuild_required(SemanticIdentityTier::Quality)?);
    storage.complete_semantic_identity_rebuild(SemanticIdentityTier::Fast)?;
    assert!(!storage.semantic_identity_rebuild_required(SemanticIdentityTier::Fast)?);
    assert!(
        storage.semantic_identity_rebuild_required(SemanticIdentityTier::Quality)?,
        "republishing the fast tier must not bless stale quality-tier doc ids"
    );
    storage.complete_semantic_identity_rebuild(SemanticIdentityTier::Quality)?;
    assert!(!storage.semantic_identity_rebuild_required(SemanticIdentityTier::Quality)?);
    assert_eq!(
        storage.reclassify_legacy_omp_conversations()?,
        LegacyOmpReclassificationResult::default()
    );

    // A historical import can add legacy rows after the marker was
    // completed. Routine startup keeps trusting the marker, while the
    // import-specific path must deliberately recheck the new archive data.
    conversation.external_id = Some("-projects-cass/imported-legacy-omp".into());
    conversation.title = Some("Imported legacy OMP".into());
    conversation.source_path = dir
        .path()
        .join("backup/.omp/agent/sessions/-projects-cass/imported-legacy-omp.jsonl");
    storage.insert_conversations_batched(&[(pi_agent_id, None, &conversation)])?;
    let imported_message_id = storage
        .max_message_id()?
        .expect("historical OMP fixture should contain messages");
    storage.set_last_embedded_message_id(imported_message_id)?;
    assert_eq!(
        storage.reclassify_legacy_omp_conversations()?,
        LegacyOmpReclassificationResult::default(),
        "the normal startup fast path should continue to trust a completed marker"
    );
    assert_eq!(
        storage.reclassify_legacy_omp_conversations_after_historical_import()?,
        LegacyOmpReclassificationResult {
            conversations_reclassified: 1,
            lexical_rebuild_required: true,
            analytics_rebuild_required: true,
        }
    );
    assert_eq!(storage.get_last_embedded_message_id()?, None);
    assert!(storage.semantic_identity_rebuild_required(SemanticIdentityTier::Fast)?);
    assert!(storage.semantic_identity_rebuild_required(SemanticIdentityTier::Quality)?);
    let conversations = storage.list_conversations(10, 0)?;
    assert_eq!(conversations.len(), 2);
    assert!(
        conversations
            .iter()
            .all(|conversation| conversation.agent_slug == "omp")
    );
    Ok(())
}
