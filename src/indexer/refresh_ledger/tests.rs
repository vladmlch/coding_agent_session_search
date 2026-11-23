use super::*;

#[test]
fn phase_model_covers_all_phases() {
    assert_eq!(RefreshPhase::ALL.len(), 7);
    assert_eq!(RefreshPhase::ALL[0], RefreshPhase::Scan);
    assert_eq!(RefreshPhase::ALL[6], RefreshPhase::Recovery);
}

#[test]
fn phase_as_str_round_trips() {
    for phase in RefreshPhase::ALL {
        let s = phase.as_str();
        assert!(!s.is_empty(), "phase {phase:?} has empty string");
    }
}

#[test]
fn ledger_builder_records_phases() {
    let mut builder = RefreshLedger::start("small", false);

    builder.begin_phase(RefreshPhase::Scan);
    builder.record_items(100, 5);
    builder.set_counter("connectors_scanned", 3);

    builder.begin_phase(RefreshPhase::Persist);
    builder.record_items(95, 0);
    builder.set_counter("bytes_written", 50_000);

    builder.begin_phase(RefreshPhase::LexicalRebuild);
    builder.record_items(450, 0);

    builder.begin_phase(RefreshPhase::Publish);
    builder.record_items(1, 0);

    let ledger = builder.finish();

    assert_eq!(ledger.phases.len(), 4);
    assert_eq!(ledger.corpus_family, "small");
    assert!(!ledger.full_rebuild);

    let scan = ledger.phase(RefreshPhase::Scan).unwrap();
    assert_eq!(scan.items_processed, 100);
    assert_eq!(scan.items_skipped, 5);
    assert_eq!(*scan.counters.get("connectors_scanned").unwrap(), 3);

    let persist = ledger.phase(RefreshPhase::Persist).unwrap();
    assert_eq!(persist.items_processed, 95);
    assert_eq!(*persist.counters.get("bytes_written").unwrap(), 50_000);

    assert!(ledger.all_phases_succeeded());
    assert_eq!(ledger.total_items_processed(), 100 + 95 + 450 + 1);
    assert!(ledger.completed_at_ms >= ledger.started_at_ms);
    let max_phase_duration = ledger
        .phases
        .iter()
        .map(|phase| phase.duration_ms)
        .max()
        .unwrap_or(0);
    assert!(ledger.total_duration_ms >= max_phase_duration);
}

#[test]
fn ledger_builder_saturates_counter_arithmetic() {
    let mut builder = RefreshLedger::start("pathological", true);

    builder.begin_phase(RefreshPhase::Scan);
    builder.record_items(u64::MAX, u64::MAX);
    builder.record_items(1, 1);
    builder.inc_counter("bytes_scanned", u64::MAX);
    builder.inc_counter("bytes_scanned", 1);

    let ledger = builder.finish();
    let scan = ledger.phase(RefreshPhase::Scan).unwrap();
    assert_eq!(scan.items_processed, u64::MAX);
    assert_eq!(scan.items_skipped, u64::MAX);
    assert_eq!(scan.counters.get("bytes_scanned"), Some(&u64::MAX));
    assert_eq!(ledger.total_items_processed(), u64::MAX);
}

#[test]
fn ledger_builder_records_failures() {
    let mut builder = RefreshLedger::start("small", false);

    builder.begin_phase(RefreshPhase::Scan);
    builder.record_items(50, 0);

    builder.begin_phase(RefreshPhase::Persist);
    builder.record_failure("database locked");

    let ledger = builder.finish();

    assert!(!ledger.all_phases_succeeded());
    assert_eq!(ledger.failed_phases().len(), 1);
    assert_eq!(ledger.failed_phases()[0].phase, RefreshPhase::Persist);
    assert_eq!(
        ledger.failed_phases()[0].error_message.as_deref(),
        Some("database locked")
    );
    assert_eq!(ledger.failed_phases()[0].errors, 1);
    assert_eq!(ledger.total_errors(), 1);
}

#[test]
fn ledger_builder_records_errors_without_failure() {
    let mut builder = RefreshLedger::start("medium", false);

    builder.begin_phase(RefreshPhase::Scan);
    builder.record_items(90, 0);
    builder.record_error("connector timeout");
    builder.record_error("permission denied");

    let ledger = builder.finish();

    let scan = ledger.phase(RefreshPhase::Scan).unwrap();
    assert!(scan.success); // phase still succeeded
    assert_eq!(scan.errors, 2);
    // Both error messages are preserved (joined with "; ").
    let msg = scan.error_message.as_deref().unwrap();
    assert!(
        msg.contains("connector timeout"),
        "missing first error: {msg}"
    );
    assert!(
        msg.contains("permission denied"),
        "missing second error: {msg}"
    );
}

#[test]
fn ledger_equivalence_artifacts() {
    let mut builder = RefreshLedger::start("small", true);

    builder.begin_phase(RefreshPhase::Scan);
    builder.record_items(10, 0);

    builder.set_equivalence(EquivalenceArtifacts {
        conversation_count: 10,
        message_count: 40,
        lexical_doc_count: 40,
        lexical_fingerprint: Some("fp-abc".to_owned()),
        semantic_manifest_fingerprint: None,
        search_hit_digest: Some("sha256-xyz".to_owned()),
        peak_rss_bytes: Some(100_000_000),
        db_size_bytes: Some(5_000_000),
        lexical_index_size_bytes: Some(2_000_000),
    });

    let ledger = builder.finish();

    assert_eq!(ledger.equivalence.conversation_count, 10);
    assert_eq!(ledger.equivalence.message_count, 40);
    assert_eq!(
        ledger.equivalence.lexical_fingerprint.as_deref(),
        Some("fp-abc")
    );
    assert!(ledger.full_rebuild);
}

#[test]
fn ledger_duration_breakdown() {
    let mut builder = RefreshLedger::start("small", false);

    builder.begin_phase(RefreshPhase::Scan);
    // Phases are very fast in tests — duration_ms may be 0.
    builder.begin_phase(RefreshPhase::LexicalRebuild);

    let ledger = builder.finish();

    let breakdown = ledger.duration_breakdown();
    assert!(breakdown.contains_key("scan"));
    assert!(breakdown.contains_key("lexical_rebuild"));
}

#[test]
fn readiness_milestones_measure_lexical_search_and_settled_times() {
    let ledger = RefreshLedger {
        total_duration_ms: 90,
        phases: vec![
            phase_record(RefreshPhase::Scan, 10, true),
            phase_record(RefreshPhase::Persist, 20, true),
            phase_record(RefreshPhase::LexicalRebuild, 30, true),
            phase_record(RefreshPhase::Publish, 5, true),
            phase_record(RefreshPhase::Analytics, 7, true),
            phase_record(RefreshPhase::Semantic, 8, true),
        ],
        ..Default::default()
    };

    let milestones = ledger.readiness_milestones();

    assert_eq!(milestones.time_to_lexical_ready_ms, Some(60));
    assert_eq!(milestones.time_to_search_ready_ms, Some(65));
    assert_eq!(milestones.time_to_full_settled_ms, Some(90));
    assert_eq!(milestones.failed_phase, None);
    assert_eq!(
        milestones.search_readiness_state,
        RefreshSearchReadinessState::Published
    );

    let json = serde_json::to_value(&milestones).unwrap();
    assert_eq!(json["time_to_lexical_ready_ms"], 60);
    assert_eq!(json["time_to_search_ready_ms"], 65);
    assert_eq!(json["time_to_full_settled_ms"], 90);
    assert_eq!(json["search_readiness_state"], "published");
}

#[test]
fn readiness_milestones_stop_at_first_failed_phase() {
    let ledger = RefreshLedger {
        total_duration_ms: 75,
        phases: vec![
            phase_record(RefreshPhase::Scan, 10, true),
            phase_record(RefreshPhase::Persist, 20, true),
            phase_record(RefreshPhase::LexicalRebuild, 30, false),
            phase_record(RefreshPhase::Publish, 5, true),
        ],
        ..Default::default()
    };

    let milestones = ledger.readiness_milestones();

    assert_eq!(milestones.time_to_lexical_ready_ms, None);
    assert_eq!(milestones.time_to_search_ready_ms, None);
    assert_eq!(milestones.time_to_full_settled_ms, None);
    assert_eq!(milestones.failed_phase.as_deref(), Some("lexical_rebuild"));
    assert_eq!(
        milestones.search_readiness_state,
        RefreshSearchReadinessState::BlockedBeforePublish
    );
}

#[test]
fn readiness_milestones_explain_unpublished_and_publish_failed_states() {
    let unpublished = RefreshLedger {
        phases: vec![
            phase_record(RefreshPhase::Scan, 10, true),
            phase_record(RefreshPhase::Persist, 20, true),
            phase_record(RefreshPhase::LexicalRebuild, 30, true),
        ],
        ..Default::default()
    };

    let unpublished_milestones = unpublished.readiness_milestones();

    assert_eq!(unpublished_milestones.time_to_lexical_ready_ms, Some(60));
    assert_eq!(unpublished_milestones.time_to_search_ready_ms, None);
    assert_eq!(unpublished_milestones.time_to_full_settled_ms, None);
    assert_eq!(unpublished_milestones.failed_phase, None);
    assert_eq!(
        unpublished_milestones.search_readiness_state,
        RefreshSearchReadinessState::WaitingForPublish
    );

    let publish_failed = RefreshLedger {
        phases: vec![
            phase_record(RefreshPhase::Scan, 10, true),
            phase_record(RefreshPhase::Persist, 20, true),
            phase_record(RefreshPhase::LexicalRebuild, 30, true),
            phase_record(RefreshPhase::Publish, 5, false),
        ],
        ..Default::default()
    };

    let publish_failed_milestones = publish_failed.readiness_milestones();

    assert_eq!(publish_failed_milestones.time_to_lexical_ready_ms, Some(60));
    assert_eq!(publish_failed_milestones.time_to_search_ready_ms, None);
    assert_eq!(publish_failed_milestones.time_to_full_settled_ms, None);
    assert_eq!(
        publish_failed_milestones.failed_phase.as_deref(),
        Some("publish")
    );
    assert_eq!(
        publish_failed_milestones.search_readiness_state,
        RefreshSearchReadinessState::PublishFailed
    );

    let post_publish_failure = RefreshLedger {
        phases: vec![
            phase_record(RefreshPhase::Scan, 10, true),
            phase_record(RefreshPhase::Persist, 20, true),
            phase_record(RefreshPhase::LexicalRebuild, 30, true),
            phase_record(RefreshPhase::Publish, 5, true),
            phase_record(RefreshPhase::Analytics, 7, false),
        ],
        ..Default::default()
    };

    let post_publish_failure_milestones = post_publish_failure.readiness_milestones();

    assert_eq!(
        post_publish_failure_milestones.time_to_lexical_ready_ms,
        Some(60)
    );
    assert_eq!(
        post_publish_failure_milestones.time_to_search_ready_ms,
        Some(65)
    );
    assert_eq!(
        post_publish_failure_milestones.time_to_full_settled_ms,
        None
    );
    assert_eq!(
        post_publish_failure_milestones.failed_phase.as_deref(),
        Some("analytics")
    );
    assert_eq!(
        post_publish_failure_milestones.search_readiness_state,
        RefreshSearchReadinessState::Published
    );
}

#[test]
fn readiness_milestones_do_not_report_full_settlement_before_publish() {
    let empty = RefreshLedger::default().readiness_milestones();

    assert_eq!(empty.time_to_lexical_ready_ms, None);
    assert_eq!(empty.time_to_search_ready_ms, None);
    assert_eq!(empty.time_to_full_settled_ms, None);
    assert_eq!(
        empty.search_readiness_state,
        RefreshSearchReadinessState::WaitingForPublish
    );

    let partial = RefreshLedger {
        total_duration_ms: 42,
        phases: vec![
            phase_record(RefreshPhase::Scan, 10, true),
            phase_record(RefreshPhase::Persist, 20, true),
        ],
        ..Default::default()
    }
    .readiness_milestones();

    assert_eq!(partial.time_to_lexical_ready_ms, None);
    assert_eq!(partial.time_to_search_ready_ms, None);
    assert_eq!(partial.time_to_full_settled_ms, None);
    assert_eq!(
        partial.search_readiness_state,
        RefreshSearchReadinessState::WaitingForPublish
    );
}

#[test]
fn ledger_tags() {
    let mut builder = RefreshLedger::start("medium", false);
    builder.tag("run_id", "bench-2026-04-01");
    builder.tag("machine", "csd");

    let ledger = builder.finish();

    assert_eq!(ledger.tags.get("run_id").unwrap(), "bench-2026-04-01");
    assert_eq!(ledger.tags.get("machine").unwrap(), "csd");
}

#[test]
fn ledger_json_round_trip() {
    let mut builder = RefreshLedger::start("duplicate_heavy", true);
    builder.begin_phase(RefreshPhase::Scan);
    builder.record_items(50, 10);
    builder.set_counter("duplicate_conversations", 25);
    builder.begin_phase(RefreshPhase::Persist);
    builder.record_items(40, 0);

    builder.set_equivalence(EquivalenceArtifacts {
        conversation_count: 40,
        message_count: 200,
        lexical_doc_count: 200,
        ..Default::default()
    });

    let ledger = builder.finish();
    let json = ledger.to_json();
    let deser: RefreshLedger = serde_json::from_str(&json).unwrap();

    assert_eq!(deser.corpus_family, "duplicate_heavy");
    assert!(deser.full_rebuild);
    assert_eq!(deser.phases.len(), 2);
    assert_eq!(deser.equivalence.conversation_count, 40);
    assert_eq!(
        *deser.phases[0]
            .counters
            .get("duplicate_conversations")
            .unwrap(),
        25
    );
}

#[test]
fn ledger_inc_counter() {
    let mut builder = RefreshLedger::start("small", false);
    builder.begin_phase(RefreshPhase::Scan);
    builder.inc_counter("files_scanned", 10);
    builder.inc_counter("files_scanned", 15);
    builder.inc_counter("files_scanned", 5);

    let ledger = builder.finish();
    let scan = ledger.phase(RefreshPhase::Scan).unwrap();
    assert_eq!(*scan.counters.get("files_scanned").unwrap(), 30);
}

#[test]
fn benchmark_corpus_configs_have_correct_families() {
    assert_eq!(BenchmarkCorpusConfig::small().family, "small");
    assert_eq!(BenchmarkCorpusConfig::medium().family, "medium");
    assert_eq!(BenchmarkCorpusConfig::large().family, "large");
    assert_eq!(
        BenchmarkCorpusConfig::duplicate_heavy().family,
        "duplicate_heavy"
    );
    assert_eq!(BenchmarkCorpusConfig::pathological().family, "pathological");
    assert_eq!(BenchmarkCorpusConfig::mixed_agent().family, "mixed_agent");
    assert_eq!(BenchmarkCorpusConfig::incremental().family, "incremental");
}

#[test]
fn benchmark_corpus_configs_have_reasonable_sizes() {
    let configs = [
        BenchmarkCorpusConfig::small(),
        BenchmarkCorpusConfig::medium(),
        BenchmarkCorpusConfig::large(),
        BenchmarkCorpusConfig::duplicate_heavy(),
        BenchmarkCorpusConfig::pathological(),
        BenchmarkCorpusConfig::mixed_agent(),
        BenchmarkCorpusConfig::incremental(),
    ];
    for cfg in &configs {
        assert!(
            cfg.num_conversations > 0,
            "{} has 0 conversations",
            cfg.family
        );
        assert!(
            cfg.messages_per_conversation > 0,
            "{} has 0 messages",
            cfg.family
        );
        assert!(cfg.agent_count > 0, "{} has 0 agents", cfg.family);
        assert!(
            cfg.duplicate_fraction >= 0.0 && cfg.duplicate_fraction <= 1.0,
            "{} has invalid duplicate fraction",
            cfg.family
        );
    }
}

fn phase_record(phase: RefreshPhase, duration_ms: u64, success: bool) -> PhaseRecord {
    PhaseRecord {
        phase,
        duration_ms,
        items_processed: 0,
        items_skipped: 0,
        errors: u64::from(!success),
        counters: BTreeMap::new(),
        success,
        error_message: (!success).then(|| format!("failed {}", phase.as_str())),
    }
}

fn phase_record_with_items(phase: RefreshPhase, duration_ms: u64, items: u64) -> PhaseRecord {
    PhaseRecord {
        phase,
        duration_ms,
        items_processed: items,
        items_skipped: 0,
        errors: 0,
        counters: BTreeMap::new(),
        success: true,
        error_message: None,
    }
}

fn ledger_with(phases: Vec<PhaseRecord>) -> RefreshLedger {
    let total_duration_ms = phases.iter().map(|p| p.duration_ms).sum();
    RefreshLedger {
        version: 1,
        started_at_ms: 1_700_000_000_000,
        completed_at_ms: 1_700_000_000_000 + i64::try_from(total_duration_ms).unwrap_or(0),
        total_duration_ms,
        full_rebuild: true,
        corpus_family: "evidence-test".to_owned(),
        phases,
        equivalence: EquivalenceArtifacts::default(),
        tags: BTreeMap::new(),
    }
}

/// `coding_agent_session_search-ibuuh.24` (evidence-ledger gate):
/// throughput math is correct + zero-duration / zero-items
/// degenerate cases yield None (NOT NaN). Pinning the math in a
/// golden test means a future tweak that introduced NaN
/// poisoning into benchmark JSON would trip immediately.
#[test]
fn evidence_summary_reports_per_phase_throughput_with_safe_zero_handling() {
    // Mixed corpus: Scan moved 1000 items in 500ms, Persist moved
    // 2000 items in 1000ms, LexicalRebuild moved 0 items in 100ms
    // (warmup-only phase), Recovery did 0 items in 0ms (no-op).
    let ledger = ledger_with(vec![
        phase_record_with_items(RefreshPhase::Scan, 500, 1000),
        phase_record_with_items(RefreshPhase::Persist, 1000, 2000),
        phase_record_with_items(RefreshPhase::LexicalRebuild, 100, 0),
        phase_record_with_items(RefreshPhase::Recovery, 0, 0),
    ]);

    let evidence = ledger.evidence_summary();

    // Throughput vector excludes zero-item phases (LexicalRebuild,
    // Recovery): nothing to extrapolate.
    assert_eq!(
        evidence.throughput.len(),
        2,
        "throughput must skip zero-item phases; got {:?}",
        evidence.throughput
    );

    // Scan: 1000 items / 0.5s = 2000.0 items/s.
    let scan = evidence
        .throughput
        .iter()
        .find(|t| t.phase == RefreshPhase::Scan)
        .expect("scan throughput present");
    assert_eq!(scan.items_per_second, Some(2000.0));
    assert_eq!(scan.duration_ms, 500);
    assert_eq!(scan.items_processed, 1000);

    // Persist: 2000 items / 1.0s = 2000.0 items/s.
    let persist = evidence
        .throughput
        .iter()
        .find(|t| t.phase == RefreshPhase::Persist)
        .expect("persist throughput present");
    assert_eq!(persist.items_per_second, Some(2000.0));

    // Aggregate: (1000+2000+0+0) / (500+1000+100+0)ms = 3000/1.6s = 1875.0
    assert_eq!(evidence.aggregate_items_processed, 3000);
    assert_eq!(evidence.aggregate_duration_ms, 1600);
    assert_eq!(evidence.aggregate_items_per_second, Some(1875.0));
}

/// Zero-duration ledger (empty or instantaneous) must NOT panic
/// and must NOT emit NaN. dominant_phase is None; aggregate
/// throughput is None.
#[test]
fn evidence_summary_handles_empty_and_zero_duration_ledgers() {
    // Truly empty.
    let empty = ledger_with(Vec::new());
    let empty_evidence = empty.evidence_summary();
    assert!(empty_evidence.throughput.is_empty());
    assert!(empty_evidence.phase_share.is_empty());
    assert_eq!(empty_evidence.dominant_phase, None);
    assert_eq!(empty_evidence.aggregate_items_per_second, None);
    assert_eq!(empty_evidence.aggregate_duration_ms, 0);

    // Phases ran but contributed 0ms each (instantaneous run).
    let instant = ledger_with(vec![
        phase_record_with_items(RefreshPhase::Scan, 0, 5),
        phase_record_with_items(RefreshPhase::Persist, 0, 5),
    ]);
    let instant_evidence = instant.evidence_summary();
    // Phases ran but with zero duration ⇒ throughput None for each.
    for t in &instant_evidence.throughput {
        assert_eq!(t.items_per_second, None, "zero duration must yield None");
    }
    // No phase was dominant (all zero) ⇒ dominant_phase None.
    assert_eq!(instant_evidence.dominant_phase, None);
    // Phase shares all 0.0 — no NaN poisoning.
    for share in &instant_evidence.phase_share {
        assert_eq!(share.share_pct, 0.0);
        assert!(!share.share_pct.is_nan(), "share_pct must never be NaN");
    }
}

/// Phase shares sum to ~100.0 across phases with non-zero
/// duration (sub-millisecond rounding can cause ±0.01 drift).
/// dominant_phase identifies the phase with the largest
/// duration_ms.
#[test]
fn evidence_summary_phase_share_sums_to_one_hundred_and_dominant_phase_picks_max() {
    let ledger = ledger_with(vec![
        phase_record_with_items(RefreshPhase::Scan, 200, 100),
        phase_record_with_items(RefreshPhase::Persist, 600, 1500), // dominant
        phase_record_with_items(RefreshPhase::LexicalRebuild, 200, 1500),
    ]);
    let evidence = ledger.evidence_summary();

    let total_share: f64 = evidence.phase_share.iter().map(|s| s.share_pct).sum();
    assert!(
        (total_share - 100.0).abs() <= 0.05,
        "phase shares must sum to ~100.0 (±0.05 for rounding); got {total_share}"
    );

    // Persist contributed 600ms / 1000ms = 60% of wall time.
    let persist_share = evidence
        .phase_share
        .iter()
        .find(|s| s.phase == RefreshPhase::Persist)
        .expect("persist share present");
    assert_eq!(persist_share.share_pct, 60.0);

    // Dominant phase must be Persist (largest duration).
    assert_eq!(evidence.dominant_phase, Some(RefreshPhase::Persist));
}

/// Tie-break for dominant phase: when two phases have IDENTICAL
/// duration_ms, the FIRST one (in pipeline order) wins —
/// matches Iterator::max_by_key semantics, so a future phase
/// reordering doesn't silently flip the dominant phase contract.
#[test]
fn evidence_summary_dominant_phase_tie_break_is_first_in_pipeline_order() {
    let ledger = ledger_with(vec![
        phase_record_with_items(RefreshPhase::Scan, 500, 1),
        phase_record_with_items(RefreshPhase::Persist, 500, 1),
        phase_record_with_items(RefreshPhase::LexicalRebuild, 500, 1),
    ]);
    let evidence = ledger.evidence_summary();
    // Iterator::max_by_key returns the LAST max element on ties,
    // so LexicalRebuild wins when all three are 500ms. Pin this
    // behavior so a future change to last-vs-first tie-break
    // semantics fails the test (operators reading benchmark JSON
    // for "dominant_phase" rely on stable ordering).
    assert_eq!(
        evidence.dominant_phase,
        Some(RefreshPhase::LexicalRebuild),
        "tie-break: max_by_key returns the LAST phase at max duration"
    );
}

/// Evidence summary serializes through serde so benchmark
/// gates / dashboards can store the JSON and diff across runs.
/// Pin the field set so a future struct-shape regression
/// (e.g. dropping aggregate_items_per_second) trips this.
#[test]
fn evidence_summary_serializes_to_stable_json_field_set() {
    let ledger = ledger_with(vec![phase_record_with_items(RefreshPhase::Scan, 100, 50)]);
    let evidence = ledger.evidence_summary();
    let json = serde_json::to_string(&evidence).expect("serialize");
    for required_field in [
        "\"throughput\"",
        "\"phase_share\"",
        "\"dominant_phase\"",
        "\"aggregate_items_processed\"",
        "\"aggregate_duration_ms\"",
        "\"aggregate_items_per_second\"",
    ] {
        assert!(
            json.contains(required_field),
            "evidence JSON missing field {required_field}; got: {json}"
        );
    }
    // Round-trip via serde_json::Value (the typed roundtrip is
    // not used by consumers; they parse into serde_json::Value
    // for diffing).
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("parse");
    assert_eq!(parsed["aggregate_items_processed"], 50);
    assert_eq!(parsed["aggregate_duration_ms"], 100);
    assert_eq!(parsed["aggregate_items_per_second"], 500.0);
    assert_eq!(parsed["dominant_phase"], "scan");
}

/// `coding_agent_session_search-ibuuh.24` cross-run comparator
/// gate: `compare_to` must surface real regressions and real
/// improvements with the conventional sign:
/// - duration_delta_pct > 0 ⇒ slower in `current`
/// - throughput_delta_pct > 0 ⇒ faster in `current`
///
/// A regression in either sign convention would cause benchmark
/// CI to misclassify slowdowns as wins (or vice versa).
#[test]
fn evidence_compare_to_reports_per_phase_regressions_and_improvements() {
    // Baseline: scan moved 100 items in 100ms (1000 items/s).
    let baseline =
        ledger_with(vec![phase_record_with_items(RefreshPhase::Scan, 100, 100)]).evidence_summary();
    // Current: scan moved 100 items in 200ms (500 items/s) —
    // slower wall clock, halved throughput. Pure regression.
    let current =
        ledger_with(vec![phase_record_with_items(RefreshPhase::Scan, 200, 100)]).evidence_summary();

    let cmp = current.compare_to(&baseline);

    assert_eq!(cmp.phase_deltas.len(), 1);
    let scan = &cmp.phase_deltas[0];
    assert_eq!(scan.phase, RefreshPhase::Scan);
    // duration: (200-100)/100 * 100 = +100% (twice as slow).
    assert_eq!(scan.duration_delta_pct, Some(100.0));
    // throughput: (500-1000)/1000 * 100 = -50% (half as fast).
    assert_eq!(scan.throughput_delta_pct, Some(-50.0));
    // Aggregate mirrors the single-phase signals.
    assert_eq!(cmp.aggregate_duration_delta_pct, Some(100.0));
    assert_eq!(cmp.aggregate_throughput_delta_pct, Some(-50.0));
    // Same phase dominant in both ⇒ no shift signal.
    assert_eq!(cmp.dominant_phase_shift, None);

    // Symmetric improvement case: swap baseline + current.
    let cmp_improved = baseline.compare_to(&current);
    let scan = &cmp_improved.phase_deltas[0];
    // duration: (100-200)/200 * 100 = -50% (half as long).
    assert_eq!(scan.duration_delta_pct, Some(-50.0));
    // throughput: (1000-500)/500 * 100 = +100% (twice as fast).
    assert_eq!(scan.throughput_delta_pct, Some(100.0));
}

/// Phase unique to ONE side must surface in the comparison
/// (not silently dropped). Pre-fix this is the failure mode where
/// a phase that ran in baseline but disappeared from current
/// (e.g. publish phase elided due to a dispatch-routing bug)
/// would not show up at all.
#[test]
fn evidence_compare_to_surfaces_phases_unique_to_one_side() {
    let baseline = ledger_with(vec![
        phase_record_with_items(RefreshPhase::Scan, 100, 100),
        phase_record_with_items(RefreshPhase::Persist, 50, 200),
    ])
    .evidence_summary();
    // Current: only Scan ran. Persist is "missing" — caller must
    // see this so they can investigate.
    let current =
        ledger_with(vec![phase_record_with_items(RefreshPhase::Scan, 100, 100)]).evidence_summary();

    let cmp = current.compare_to(&baseline);

    let phases: Vec<RefreshPhase> = cmp.phase_deltas.iter().map(|d| d.phase).collect();
    assert!(
        phases.contains(&RefreshPhase::Scan),
        "Scan ran in both sides; must appear in comparison; got phases {phases:?}"
    );
    assert!(
        phases.contains(&RefreshPhase::Persist),
        "Persist is missing from current but ran in baseline — comparison MUST \
             surface it so caller can investigate; got phases {phases:?}"
    );

    // The missing-from-current Persist entry should report
    // baseline_duration_ms=50 + current_duration_ms=0 + duration_delta_pct
    // is well-defined (it's -100%: phase went away).
    let persist = cmp
        .phase_deltas
        .iter()
        .find(|d| d.phase == RefreshPhase::Persist)
        .expect("Persist delta present");
    assert_eq!(persist.baseline_duration_ms, 50);
    assert_eq!(persist.current_duration_ms, 0);
    assert_eq!(
        persist.duration_delta_pct,
        Some(-100.0),
        "phase disappearing from current must surface as -100% duration delta; \
             got {persist:?}"
    );
}

/// Zero-item phases still consume wall-clock time and must remain
/// visible to benchmark comparisons. Throughput summaries omit
/// them by design, so `compare_to` must derive phase presence
/// from phase-share data instead.
#[test]
fn evidence_compare_to_retains_zero_item_phases_with_duration() {
    let baseline = ledger_with(vec![
        phase_record_with_items(RefreshPhase::Scan, 100, 100),
        phase_record_with_items(RefreshPhase::Publish, 40, 0),
    ])
    .evidence_summary();
    let current = ledger_with(vec![
        phase_record_with_items(RefreshPhase::Scan, 100, 100),
        phase_record_with_items(RefreshPhase::Publish, 80, 0),
    ])
    .evidence_summary();

    assert!(
        baseline
            .throughput
            .iter()
            .all(|entry| entry.phase != RefreshPhase::Publish),
        "zero-item Publish must stay out of throughput: {:?}",
        baseline.throughput
    );

    let cmp = current.compare_to(&baseline);
    let publish = cmp
        .phase_deltas
        .iter()
        .find(|delta| delta.phase == RefreshPhase::Publish)
        .expect("zero-item Publish phase must remain in comparison");

    assert_eq!(publish.baseline_duration_ms, 40);
    assert_eq!(publish.current_duration_ms, 80);
    assert_eq!(publish.duration_delta_pct, Some(100.0));
    assert_eq!(publish.baseline_items_processed, 0);
    assert_eq!(publish.current_items_processed, 0);
    assert_eq!(publish.baseline_items_per_second, None);
    assert_eq!(publish.current_items_per_second, None);
    assert_eq!(publish.throughput_delta_pct, None);
}

/// Dominant-phase shift signal: when the hot phase changes
/// between runs (even if absolute totals are similar), the
/// operator should look at why. Pinning the shift detection
/// directly catches a regression where the comparator silently
/// reports the same dominant phase for both sides.
#[test]
fn evidence_compare_to_reports_dominant_phase_shift() {
    // Baseline: Scan dominates wall time.
    let baseline = ledger_with(vec![
        phase_record_with_items(RefreshPhase::Scan, 800, 100),
        phase_record_with_items(RefreshPhase::Persist, 200, 100),
    ])
    .evidence_summary();
    // Current: total wall time similar but Persist now dominates.
    let current = ledger_with(vec![
        phase_record_with_items(RefreshPhase::Scan, 200, 100),
        phase_record_with_items(RefreshPhase::Persist, 800, 100),
    ])
    .evidence_summary();
    // Sanity: the two sides really did have different dominant
    // phases (would silently break this test if dominant_phase
    // tie-breaking changed).
    assert_eq!(baseline.dominant_phase, Some(RefreshPhase::Scan));
    assert_eq!(current.dominant_phase, Some(RefreshPhase::Persist));

    let cmp = current.compare_to(&baseline);

    assert_eq!(
        cmp.dominant_phase_shift,
        Some((RefreshPhase::Scan, RefreshPhase::Persist)),
        "dominant phase shifted Scan→Persist; comparison must surface this; got {cmp:?}"
    );

    // Negative case: same dominant phase in both ⇒ no shift.
    let same_dom =
        ledger_with(vec![phase_record_with_items(RefreshPhase::Scan, 100, 100)]).evidence_summary();
    let cmp_same = same_dom.compare_to(&same_dom);
    assert_eq!(cmp_same.dominant_phase_shift, None);
}

/// Empty / zero-baseline degenerate cases must NOT panic and
/// must NOT emit NaN — pre-fix `pct_delta` would have returned
/// Inf for `(x - 0) / 0`. The defensive None branch is the only
/// thing keeping benchmark JSON parseable when the baseline is
/// missing or empty.
#[test]
fn evidence_compare_to_safely_handles_zero_baseline_and_empty_evidence() {
    let empty = ledger_with(Vec::new()).evidence_summary();
    let normal =
        ledger_with(vec![phase_record_with_items(RefreshPhase::Scan, 100, 50)]).evidence_summary();

    // empty → normal: baseline has nothing, every delta is None
    // (no rate of change defined when baseline is zero).
    let against_empty = normal.compare_to(&empty);
    assert!(
        against_empty
            .phase_deltas
            .iter()
            .all(|d| d.duration_delta_pct.is_none() || d.baseline_duration_ms == 0),
        "phases with zero-baseline duration must report None for duration_delta_pct"
    );
    assert_eq!(against_empty.aggregate_duration_delta_pct, None);
    assert_eq!(against_empty.aggregate_throughput_delta_pct, None);

    // empty vs empty: zero comparison surface, no panic.
    let against_self = empty.compare_to(&empty);
    assert!(against_self.phase_deltas.is_empty());
    assert_eq!(against_self.aggregate_duration_delta_pct, None);

    // No NaN anywhere in the JSON serialization (pins that the
    // defensive branches actually emit serializable output).
    let json = serde_json::to_string(&against_empty).expect("serialize");
    assert!(
        !json.contains("NaN"),
        "comparison JSON must not contain NaN; got {json}"
    );
    assert!(
        !json.contains("Infinity"),
        "comparison JSON must not contain Infinity"
    );
}

/// `coding_agent_session_search-ibuuh.24` cross-run tracing
/// gate: emit_tracing_summary picks WARN for significant
/// slowdowns (>=+25%), INFO for notable improvements (<=-10%),
/// DEBUG for the steady-state range. Pre-fix this routing did
/// not exist; pinning the thresholds directly catches a
/// regression where a peer "tunes" the tier and accidentally
/// hides a slowdown signal in debug-level logs.
#[test]
fn evidence_comparison_emit_tracing_summary_uses_correct_severity_tier() {
    use std::sync::{Arc, Mutex};
    use tracing::field::{Field, Visit};
    use tracing::{Event, Subscriber};
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    #[derive(Debug, Clone, Default)]
    struct CapturedEvent {
        level: String,
        message: String,
    }

    #[derive(Clone, Default)]
    struct LevelCollector {
        events: Arc<Mutex<Vec<CapturedEvent>>>,
    }

    impl<S: Subscriber> Layer<S> for LevelCollector {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            if event.metadata().target() != "cass::indexer::lexical_refresh" {
                return;
            }
            let mut visitor = MessageVisitor::default();
            event.record(&mut visitor);
            self.events
                .lock()
                .expect("collector lock")
                .push(CapturedEvent {
                    level: event.metadata().level().to_string(),
                    message: visitor.message,
                });
        }
    }

    #[derive(Default)]
    struct MessageVisitor {
        message: String,
    }
    impl Visit for MessageVisitor {
        fn record_str(&mut self, _field: &Field, _value: &str) {}
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.message = format!("{:?}", value).trim_matches('"').to_string();
            }
        }
    }

    // Helper: build a comparison directly with a given duration
    // delta so we exercise the tier routing without setting up
    // full ledger fixtures.
    fn comparison_with_duration_pct(pct: f64) -> RefreshLedgerEvidenceComparison {
        RefreshLedgerEvidenceComparison {
            phase_deltas: Vec::new(),
            aggregate_duration_delta_pct: Some(pct),
            aggregate_throughput_delta_pct: None,
            dominant_phase_shift: None,
        }
    }

    // Tier 1: significant slowdown ⇒ warn.
    let collector = LevelCollector::default();
    let subscriber = Registry::default().with(collector.clone());
    tracing::subscriber::with_default(subscriber, || {
        comparison_with_duration_pct(50.0).emit_tracing_summary();
    });
    let evs = collector.events.lock().expect("lock").clone();
    assert_eq!(
        evs.len(),
        1,
        "exactly one event per emit_tracing_summary call"
    );
    assert_eq!(
        evs[0].level, "WARN",
        "+50% slowdown must be warn; got {evs:?}"
    );
    assert!(
        evs[0].message.contains("significant slowdown"),
        "warn message must name the slowdown; got {:?}",
        evs[0].message
    );

    // Tier 2: notable improvement ⇒ info.
    let collector = LevelCollector::default();
    let subscriber = Registry::default().with(collector.clone());
    tracing::subscriber::with_default(subscriber, || {
        comparison_with_duration_pct(-25.0).emit_tracing_summary();
    });
    let evs = collector.events.lock().expect("lock").clone();
    assert_eq!(
        evs[0].level, "INFO",
        "-25% improvement must be info; got {evs:?}"
    );
    assert!(
        evs[0].message.contains("notable improvement"),
        "info message must name the improvement; got {:?}",
        evs[0].message
    );

    // Tier 3: steady-state ⇒ debug.
    let collector = LevelCollector::default();
    let subscriber = Registry::default().with(collector.clone());
    tracing::subscriber::with_default(subscriber, || {
        comparison_with_duration_pct(5.0).emit_tracing_summary();
    });
    let evs = collector.events.lock().expect("lock").clone();
    assert_eq!(
        evs[0].level, "DEBUG",
        "+5% within steady-state must be debug; got {evs:?}"
    );
    assert!(
        evs[0].message.contains("cross-run comparison"),
        "debug message must use the steady-state phrasing; got {:?}",
        evs[0].message
    );

    // Boundary: exactly +25.0 ⇒ warn (>= threshold).
    let collector = LevelCollector::default();
    let subscriber = Registry::default().with(collector.clone());
    tracing::subscriber::with_default(subscriber, || {
        comparison_with_duration_pct(25.0).emit_tracing_summary();
    });
    let evs = collector.events.lock().expect("lock").clone();
    assert_eq!(
        evs[0].level, "WARN",
        "exactly +25% must be warn (inclusive threshold); got {evs:?}"
    );

    // Boundary: exactly -10.0 ⇒ info (<= threshold).
    let collector = LevelCollector::default();
    let subscriber = Registry::default().with(collector.clone());
    tracing::subscriber::with_default(subscriber, || {
        comparison_with_duration_pct(-10.0).emit_tracing_summary();
    });
    let evs = collector.events.lock().expect("lock").clone();
    assert_eq!(
        evs[0].level, "INFO",
        "exactly -10% must be info (inclusive threshold); got {evs:?}"
    );

    // None duration delta (e.g. baseline missing) ⇒ debug
    // (defaults to 0.0 which lands in steady-state).
    let collector = LevelCollector::default();
    let subscriber = Registry::default().with(collector.clone());
    tracing::subscriber::with_default(subscriber, || {
        RefreshLedgerEvidenceComparison {
            phase_deltas: Vec::new(),
            aggregate_duration_delta_pct: None,
            aggregate_throughput_delta_pct: None,
            dominant_phase_shift: None,
        }
        .emit_tracing_summary();
    });
    let evs = collector.events.lock().expect("lock").clone();
    assert_eq!(
        evs[0].level, "DEBUG",
        "None duration delta defaults to steady-state (debug); got {evs:?}"
    );
}

/// `coding_agent_session_search-ibuuh.24` CI hard-gate
/// regression: pin the regression_verdict tier semantics +
/// boundary cases + degenerate inputs. A regression in any
/// of the four classes (Clean / Warning / Failure /
/// degenerate-clean) would silently break either the
/// improvement signal (false-positive failure) or the
/// failure gate (silent passthrough on real regression).
#[test]
fn regression_verdict_categorizes_each_band_and_handles_degenerate_cases() {
    let thresholds = RegressionVerdictThresholds::defaults();
    assert_eq!(thresholds.warning_duration_pct, 15.0);
    assert_eq!(thresholds.failure_duration_pct, 30.0);

    // Helper: build a comparison with a given duration delta.
    fn comparison_with_pct(pct: Option<f64>) -> RefreshLedgerEvidenceComparison {
        RefreshLedgerEvidenceComparison {
            phase_deltas: Vec::new(),
            aggregate_duration_delta_pct: pct,
            aggregate_throughput_delta_pct: None,
            dominant_phase_shift: None,
        }
    }

    // ─── Clean band ────────────────────────────────────────
    // Below warning threshold ⇒ Clean.
    let clean = comparison_with_pct(Some(10.0)).regression_verdict(&thresholds);
    assert_eq!(clean, RegressionVerdict::Clean);
    assert!(!clean.should_fail_build());

    // ─── Warning band ──────────────────────────────────────
    // At threshold (inclusive) ⇒ Warning.
    let warn_at = comparison_with_pct(Some(15.0)).regression_verdict(&thresholds);
    assert!(
        matches!(
            warn_at,
            RegressionVerdict::Warning { duration_delta_pct, threshold_pct }
                if (duration_delta_pct - 15.0).abs() < 0.01 && threshold_pct == 15.0
        ),
        "+15% must trigger warn at the inclusive threshold; got {warn_at:?}"
    );
    assert!(!warn_at.should_fail_build());

    // Mid-band ⇒ Warning.
    let warn_mid = comparison_with_pct(Some(22.5)).regression_verdict(&thresholds);
    assert!(matches!(warn_mid, RegressionVerdict::Warning { .. }));
    assert!(!warn_mid.should_fail_build());

    // ─── Failure band ──────────────────────────────────────
    // At threshold (inclusive) ⇒ Failure.
    let fail_at = comparison_with_pct(Some(30.0)).regression_verdict(&thresholds);
    assert!(
        matches!(
            fail_at,
            RegressionVerdict::Failure { duration_delta_pct, threshold_pct }
                if (duration_delta_pct - 30.0).abs() < 0.01 && threshold_pct == 30.0
        ),
        "+30% must trigger failure at the inclusive threshold; got {fail_at:?}"
    );
    assert!(
        fail_at.should_fail_build(),
        "Failure verdict MUST cause CI to exit non-zero"
    );

    // Far past failure ⇒ still Failure (capping behavior).
    let fail_far = comparison_with_pct(Some(150.0)).regression_verdict(&thresholds);
    assert!(matches!(fail_far, RegressionVerdict::Failure { .. }));

    // ─── Improvements never trigger a regression verdict ───
    let improvement = comparison_with_pct(Some(-50.0)).regression_verdict(&thresholds);
    assert_eq!(
        improvement,
        RegressionVerdict::Clean,
        "improvements (negative duration delta) MUST NOT trigger regression verdicts; \
             got {improvement:?}"
    );

    // ─── None duration delta (no comparison data) ─────────
    let no_data = comparison_with_pct(None).regression_verdict(&thresholds);
    assert_eq!(
        no_data,
        RegressionVerdict::Clean,
        "missing comparison data MUST NOT cause a CI failure (no signal to gate on)"
    );

    let invalid_negative = RegressionVerdictThresholds {
        warning_duration_pct: -20.0,
        failure_duration_pct: -10.0,
    };
    let steady_state = comparison_with_pct(Some(0.0)).regression_verdict(&invalid_negative);
    assert_eq!(
        steady_state,
        RegressionVerdict::Clean,
        "invalid negative thresholds must fail open instead of turning a 0% \
             steady-state comparison into a CI failure"
    );
}

/// `coding_agent_session_search-ibuuh.24`: the threshold
/// constructor MUST refuse internally-inconsistent
/// configurations (warning >= failure would never raise a
/// warning before the failure trips). A project that misorders
/// its threshold values would otherwise get a hard CI failure
/// on every run.
#[test]
fn regression_verdict_thresholds_try_new_rejects_inconsistent_configurations() {
    // Happy path.
    assert!(RegressionVerdictThresholds::try_new(10.0, 20.0).is_ok());

    // warning >= failure ⇒ Err.
    let err = RegressionVerdictThresholds::try_new(20.0, 10.0)
        .expect_err("warning > failure must be rejected");
    assert!(
        err.contains("strictly less"),
        "rejection message must explain the constraint; got {err:?}"
    );

    // warning == failure ⇒ Err (warning would never trigger).
    let err_eq = RegressionVerdictThresholds::try_new(15.0, 15.0)
        .expect_err("warning == failure must be rejected");
    assert!(err_eq.contains("strictly less"));

    // Negative thresholds make steady-state (0%) compare greater
    // than the failure threshold, so reject them up front.
    let negative_warning = RegressionVerdictThresholds::try_new(-20.0, 10.0)
        .expect_err("negative warning threshold must be rejected");
    assert!(negative_warning.contains("non-negative"));
    let negative_failure = RegressionVerdictThresholds::try_new(10.0, -20.0)
        .expect_err("negative failure threshold must be rejected");
    assert!(negative_failure.contains("non-negative"));
    let invalid_json = r#"{"warning_duration_pct":-30.0,"failure_duration_pct":-10.0}"#;
    let deser = serde_json::from_str::<RegressionVerdictThresholds>(invalid_json)
        .expect_err("serde-loaded negative thresholds must be rejected too");
    assert!(
        deser.to_string().contains("non-negative"),
        "serde validation error must explain the threshold polarity; got {deser}"
    );

    // Non-finite values rejected explicitly (defensive — never
    // reachable from clean f64 arithmetic but pin the contract).
    assert!(RegressionVerdictThresholds::try_new(f64::NAN, 30.0).is_err());
    assert!(RegressionVerdictThresholds::try_new(15.0, f64::INFINITY).is_err());
}

/// `coding_agent_session_search-whnja`: the non-negative-thresholds
/// fix (commit 5cb0038f) pinned the try_new rejection path and the
/// fail-open behavior for struct-update bypass, but nothing
/// directly asserted that a 0% steady-state delta evaluates as
/// Clean under a *valid* non-default threshold pair — the common
/// case for bench harnesses that tune tolerance away from the
/// 15/30 defaults. Pin it here so a future refactor of the
/// `>= warning` / `>= failure` ordering can't silently flip a
/// no-op bench run into a Warning under a tighter profile.
#[test]
fn regression_verdict_zero_change_under_valid_custom_thresholds_is_clean() {
    fn zero_delta_comparison() -> RefreshLedgerEvidenceComparison {
        RefreshLedgerEvidenceComparison {
            phase_deltas: Vec::new(),
            aggregate_duration_delta_pct: Some(0.0),
            aggregate_throughput_delta_pct: None,
            dominant_phase_shift: None,
        }
    }

    // Strict CI profile — 5% warn / 20% fail. 0% change is a
    // steady-state bench run and must not trigger any band.
    let strict = RegressionVerdictThresholds::try_new(5.0, 20.0)
        .expect("valid strict thresholds must construct");
    let steady_state = zero_delta_comparison().regression_verdict(&strict);
    assert_eq!(
        steady_state,
        RegressionVerdict::Clean,
        "0% steady-state delta must be Clean under any valid \
             threshold pair — tight CI profiles must not flag no-op runs"
    );

    // Extra-loose profile — 50% warn / 200% fail. Same 0% delta
    // must still be Clean; tight vs loose is a policy knob on the
    // warning band, not the zero-crossing.
    let loose = RegressionVerdictThresholds::try_new(50.0, 200.0)
        .expect("valid loose thresholds must construct");
    let steady_state_loose = zero_delta_comparison().regression_verdict(&loose);
    assert_eq!(
        steady_state_loose,
        RegressionVerdict::Clean,
        "0% steady-state delta must be Clean under loose thresholds too"
    );
}

/// `coding_agent_session_search-ibuuh.24`: RegressionVerdict
/// serializes through serde (CI runners persist the verdict
/// JSON for PR comments + dashboards). Pin the tag/snake_case
/// shape so a future variant addition or rename trips a clear
/// deserialization break in downstream consumers.
#[test]
fn regression_verdict_serializes_with_snake_case_verdict_tag() {
    let clean_json = serde_json::to_string(&RegressionVerdict::Clean).expect("serialize");
    assert!(
        clean_json.contains("\"verdict\":\"clean\""),
        "Clean must serialize with snake_case `verdict` tag; got {clean_json}"
    );

    let warning_json = serde_json::to_string(&RegressionVerdict::Warning {
        duration_delta_pct: 18.5,
        threshold_pct: 15.0,
    })
    .expect("serialize");
    assert!(warning_json.contains("\"verdict\":\"warning\""));
    assert!(warning_json.contains("\"duration_delta_pct\":18.5"));
    assert!(warning_json.contains("\"threshold_pct\":15"));

    let failure_json = serde_json::to_string(&RegressionVerdict::Failure {
        duration_delta_pct: 42.0,
        threshold_pct: 30.0,
    })
    .expect("serialize");
    assert!(failure_json.contains("\"verdict\":\"failure\""));
}
