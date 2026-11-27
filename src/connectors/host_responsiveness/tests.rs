use super::calibration::*;
use super::memory::*;
use super::*;
use serial_test::serial;

fn cgroup_fixture(files: &[(&str, &str)]) -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    for (relative, contents) in files {
        let path = root.path().join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, contents).unwrap();
    }
    root
}

/// GH #496: a 10 GB systemd unit on a 64 GB host must size budgets from
/// the unit, not the host; the tightest ancestor wins; reclaimable page
/// cache counts as headroom like MemAvailable does.
#[test]
fn cgroup_v2_budget_takes_the_tightest_ancestor_and_credits_page_cache() {
    const GIB: u64 = 1 << 30;
    let root = cgroup_fixture(&[
        ("user.slice/memory.max", &format!("{}\n", 10 * GIB)),
        ("user.slice/memory.current", &format!("{}\n", 3 * GIB)),
        (
            "user.slice/memory.stat",
            &format!("anon 1\ninactive_file {}\n", GIB),
        ),
        ("user.slice/app.scope/memory.max", "max\n"),
        (
            "user.slice/app.scope/memory.current",
            &format!("{}\n", 2 * GIB),
        ),
    ]);
    let budget = cgroup_memory_budget("0::/user.slice/app.scope\n", root.path())
        .expect("a limited ancestor must produce a budget");
    assert_eq!(budget.limit, 10 * GIB);
    assert_eq!(budget.headroom, 8 * GIB, "10 - (3 - 1 reclaimable) GiB");
    assert_eq!(
        budget.limit_level, "cgroup_v2:/user.slice",
        "the unlimited leaf must not take the slice's attribution"
    );

    // A tighter leaf wins over the slice.
    let root = cgroup_fixture(&[
        ("user.slice/memory.max", &format!("{}\n", 10 * GIB)),
        ("user.slice/memory.current", "0\n"),
        ("user.slice/app.scope/memory.max", &format!("{}\n", 2 * GIB)),
        ("user.slice/app.scope/memory.current", &format!("{}\n", GIB)),
    ]);
    let budget = cgroup_memory_budget("0::/user.slice/app.scope\n", root.path()).unwrap();
    assert_eq!(
        budget,
        CgroupMemoryBudget {
            limit: 2 * GIB,
            headroom: GIB,
            limit_level: "cgroup_v2:/user.slice/app.scope".to_owned(),
        }
    );

    // The tightest headroom can sit at a different level from the
    // tightest limit; the attribution follows the limit.
    let root = cgroup_fixture(&[
        ("user.slice/memory.max", &format!("{}\n", 10 * GIB)),
        ("user.slice/memory.current", &format!("{}\n", 9 * GIB)),
        ("user.slice/app.scope/memory.max", &format!("{}\n", 4 * GIB)),
        ("user.slice/app.scope/memory.current", "0\n"),
    ]);
    let budget = cgroup_memory_budget("0::/user.slice/app.scope\n", root.path()).unwrap();
    assert_eq!(
        budget,
        CgroupMemoryBudget {
            limit: 4 * GIB,
            headroom: GIB,
            limit_level: "cgroup_v2:/user.slice/app.scope".to_owned(),
        }
    );
}

/// GH #496: `status` must say what set the memory total. A limit at or
/// above the host total changes nothing and is not named as the source.
#[test]
fn memory_total_names_the_cgroup_level_only_when_it_is_below_the_host() {
    const GIB: u64 = 1 << 30;
    let budget = |limit: u64| CgroupMemoryBudget {
        limit,
        headroom: limit,
        limit_level: "cgroup_v2:/user.slice/run-r1.scope".to_owned(),
    };
    assert_eq!(
        clamp_memory_total(64 * GIB, Some(budget(16 * GIB))),
        MemoryTotal {
            effective_bytes: 16 * GIB,
            host_bytes: 64 * GIB,
            limit_source: "cgroup_v2:/user.slice/run-r1.scope".to_owned(),
        }
    );
    for unconstrained in [Some(budget(64 * GIB)), Some(budget(128 * GIB)), None] {
        assert_eq!(
            clamp_memory_total(64 * GIB, unconstrained),
            MemoryTotal {
                effective_bytes: 64 * GIB,
                host_bytes: 64 * GIB,
                limit_source: "host".to_owned(),
            }
        );
    }
}

#[test]
fn cgroup_budget_is_none_without_a_limit_and_skips_malformed_lines() {
    let root = cgroup_fixture(&[
        ("user.slice/memory.max", "max\n"),
        ("user.slice/memory.current", "123\n"),
        (
            "memory/legacy/memory.limit_in_bytes",
            "9223372036854771712\n",
        ),
        ("memory/legacy/memory.usage_in_bytes", "123\n"),
    ]);
    assert_eq!(
        cgroup_memory_budget("garbage\n0::/user.slice\n", root.path()),
        None,
        "'max' (v2) means unlimited"
    );
    assert_eq!(
        cgroup_memory_budget("4:memory:/legacy\n", root.path()),
        None,
        "the v1 page-rounded i64::MAX sentinel means unlimited"
    );
    assert_eq!(cgroup_memory_budget("", root.path()), None);
}

#[test]
fn cgroup_v1_budget_reads_the_memory_controller_hierarchy() {
    const MIB: u64 = 1 << 20;
    let root = cgroup_fixture(&[
        (
            "memory/docker/abc/memory.limit_in_bytes",
            &format!("{}\n", 512 * MIB),
        ),
        (
            "memory/docker/abc/memory.usage_in_bytes",
            &format!("{}\n", 300 * MIB),
        ),
        (
            "memory/docker/abc/memory.stat",
            &format!("total_inactive_file {}\n", 100 * MIB),
        ),
    ]);
    let budget = cgroup_memory_budget(
        "7:cpu,cpuacct:/docker/abc\n5:memory:/docker/abc\n",
        root.path(),
    )
    .unwrap();
    assert_eq!(budget.limit, 512 * MIB);
    assert_eq!(budget.headroom, 312 * MIB);
    assert_eq!(budget.limit_level, "cgroup_v1:/docker/abc");
}

/// macOS load-average parser is pure so it runs on Linux CI too.
#[test]
fn macos_sysctl_loadavg_parser_reads_one_minute_field() {
    assert_eq!(
        macos_loadavg_1m_from_sysctl("{ 1.23 1.45 1.50 }\n"),
        Some(1.23)
    );
    assert_eq!(macos_loadavg_1m_from_sysctl("0.07 0.10 0.12"), Some(0.07));
    assert_eq!(macos_loadavg_1m_from_sysctl(""), None);
    assert_eq!(macos_loadavg_1m_from_sysctl("{ }"), None);
    assert_eq!(macos_loadavg_1m_from_sysctl("{ nan 1 1 }"), None);
    assert_eq!(macos_loadavg_1m_from_sysctl("{ -1 1 1 }"), None);
}

#[test]
fn macos_ioreg_hid_idle_parser_converts_nanoseconds_and_takes_min() {
    let sample = "+-o IOHIDSystem  <class IOHIDSystem>\n\
             |   {\n\
             |     \"HIDIdleTime\" = 125000000000\n\
             |     \"HIDParameters\" = {\"Foo\"=1}\n\
             |   }\n\
             +-o IOHIDSystem2\n\
             |     \"HIDIdleTime\" = 3000000000\n";
    assert_eq!(macos_hid_idle_seconds_from_ioreg(sample), Some(3));
    assert_eq!(macos_hid_idle_seconds_from_ioreg("no idle here"), None);
    assert_eq!(
        macos_hid_idle_seconds_from_ioreg("\"HIDIdleTime\" = notanumber"),
        None
    );
}

#[test]
fn user_idle_gate_is_disabled_at_zero_and_fails_open_without_signal() {
    let disabled = user_idle_gate_for(0, Some(1));
    assert!(disabled.satisfied);
    assert_eq!(disabled.required_secs, 0);

    let unknown = user_idle_gate_for(600, None);
    assert!(unknown.satisfied, "unknown idle must fail open");

    let active = user_idle_gate_for(600, Some(30));
    assert!(!active.satisfied);
    assert_eq!(active.observed_secs, Some(30));

    let away = user_idle_gate_for(600, Some(600));
    assert!(away.satisfied);
}

#[test]
fn machine_pressure_classifies_against_config_thresholds() {
    let c = cfg();
    let idle = HealthSnapshot {
        load_per_core: Some(0.2),
        psi_cpu_some_avg10: Some(1.0),
    };
    let p = machine_pressure_for(&idle, &c);
    assert!(!p.pressured);
    assert!(!p.severe);

    let pressured = HealthSnapshot {
        load_per_core: Some(1.5),
        psi_cpu_some_avg10: None,
    };
    let p = machine_pressure_for(&pressured, &c);
    assert!(p.pressured);
    assert!(!p.severe);

    let severe = HealthSnapshot {
        load_per_core: Some(3.0),
        psi_cpu_some_avg10: None,
    };
    let p = machine_pressure_for(&severe, &c);
    assert!(p.severe);

    // Unknown signals (non-Linux/macOS platforms) must fail open.
    let unknown = HealthSnapshot {
        load_per_core: None,
        psi_cpu_some_avg10: None,
    };
    let p = machine_pressure_for(&unknown, &c);
    assert!(!p.pressured);
    assert!(!p.severe);
}

#[test]
fn user_idle_seconds_probe_never_panics() {
    // Platform-dependent: Some(_) on macOS with a console session, None
    // elsewhere. Either is acceptable; the probe must simply not fail.
    let _ = user_idle_seconds();
}

/// #294: the macOS `vm_stat` parser turns reclaimable page counts into bytes
/// using the reported page size, so the host-memory-reserve throttle (which
/// was silently disabled on macOS because `/proc/meminfo` is absent) can
/// engage. Pure, so testable on Linux CI.
#[test]
fn macos_vm_stat_parser_sums_reclaimable_pages_at_page_size() {
    let sample = "Mach Virtual Memory Statistics: (page size of 16384 bytes)\n\
             Pages free:                               100.\n\
             Pages active:                            5000.\n\
             Pages inactive:                           200.\n\
             Pages speculative:                         50.\n\
             Pages wired down:                        3000.\n\
             Pages purgeable:                           10.\n";
    // (100 + 200 + 50 + 10) pages * 16384 bytes/page = 360 * 16384.
    let expected = 360u64 * 16384;
    assert_eq!(
        macos_available_memory_bytes_from_vm_stat(sample),
        Some(expected)
    );
}

#[test]
fn macos_vm_stat_parser_defaults_page_size_and_rejects_empty() {
    // No recognizable page lines -> None (caller falls back to no throttle,
    // exactly as before this fix on unknown platforms).
    assert_eq!(macos_available_memory_bytes_from_vm_stat("garbage\n"), None);
    // Missing the page-size header defaults to 4096.
    let sample = "Pages free: 10.\nPages inactive: 0.\n";
    assert_eq!(
        macos_available_memory_bytes_from_vm_stat(sample),
        Some(10 * 4096)
    );
}

#[test]
fn macos_rss_parser_converts_kib_to_bytes() {
    assert_eq!(macos_rss_kib_to_bytes("  204800\n"), Some(204800 * 1024));
    assert_eq!(macos_rss_kib_to_bytes("not-a-number"), None);
}

/// A telemetry command that never answers (the wedged `footprint`
/// service) must come back as `TimedOut` near the deadline, with the
/// child's whole process group killed, instead of blocking the caller.
#[cfg(unix)]
#[test]
fn bounded_telemetry_probe_kills_a_hung_command_at_its_deadline() {
    let marker = tempfile::tempdir().unwrap();
    let grandchild_pid = marker.path().join("grandchild.pid");
    let mut command = std::process::Command::new("sh");
    // The grandchild inherits the pipes; only a group kill ends it.
    command.args([
        "-c",
        &format!("sleep 60 & echo $! > '{}'; wait", grandchild_pid.display()),
    ]);
    let started = std::time::Instant::now();
    // Long enough for the shell to record the grandchild's pid first.
    let outcome = run_bounded_telemetry_probe(command, Duration::from_millis(1500));
    let elapsed = started.elapsed();
    assert!(
        matches!(outcome, TelemetryProbe::TimedOut),
        "hung probe must report TimedOut, got {outcome:?}"
    );
    assert!(
        elapsed < Duration::from_secs(10),
        "probe must return near its deadline, took {elapsed:?}"
    );
    let pid: i32 = std::fs::read_to_string(&grandchild_pid)
        .expect("grandchild pid recorded")
        .trim()
        .parse()
        .expect("numeric grandchild pid");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        // `kill -0` fails once the grandchild is gone (killed and reaped
        // by init after its parent group was SIGKILLed).
        let alive = std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .status()
            .is_ok_and(|status| status.success());
        if !alive {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "grandchild {pid} of the timed-out probe is still alive"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Positive control: a prompt command completes with its real output
/// and exit status, so the deadline does not swallow good telemetry.
#[cfg(unix)]
#[test]
fn bounded_telemetry_probe_returns_prompt_output() {
    let mut command = std::process::Command::new("sh");
    command.args(["-c", "printf 'phys_footprint: 4096 B\\n'; exit 0"]);
    let outcome = run_bounded_telemetry_probe(command, Duration::from_secs(10));
    assert!(
        matches!(&outcome, TelemetryProbe::Completed(output) if output.status.success()),
        "prompt probe must complete successfully, got {outcome:?}"
    );
    if let TelemetryProbe::Completed(output) = outcome {
        assert_eq!(
            macos_phys_footprint_bytes_from_output(&String::from_utf8_lossy(&output.stdout)),
            Some(4096)
        );
    }
    let missing = std::process::Command::new("/nonexistent/cass-telemetry-probe");
    assert!(matches!(
        run_bounded_telemetry_probe(missing, Duration::from_secs(10)),
        TelemetryProbe::Failed
    ));
}

/// After one timeout the footprint path is disabled for the process:
/// later samples must not spawn (and wait on) the wedged tool again.
#[test]
fn footprint_timeout_disables_later_probes() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let disabled = AtomicBool::new(false);
    let calls = AtomicUsize::new(0);
    let timed_out = || {
        calls.fetch_add(1, Ordering::SeqCst);
        TelemetryProbe::TimedOut
    };
    assert_eq!(phys_footprint_bytes_via_probe(&disabled, timed_out), None);
    assert!(
        disabled.load(Ordering::SeqCst),
        "a timeout trips the breaker"
    );
    assert_eq!(phys_footprint_bytes_via_probe(&disabled, timed_out), None);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a tripped breaker must not run the probe again"
    );

    // A failed (not hung) probe does not trip the breaker, and a healthy
    // probe still yields the parsed footprint.
    let healthy = AtomicBool::new(false);
    assert_eq!(
        phys_footprint_bytes_via_probe(&healthy, || TelemetryProbe::Failed),
        None
    );
    assert!(!healthy.load(Ordering::SeqCst));
    #[cfg(unix)]
    {
        let mut command = std::process::Command::new("sh");
        command.args(["-c", "printf 'phys_footprint: 8192 B\\n'"]);
        assert_eq!(
            phys_footprint_bytes_via_probe(&healthy, || run_bounded_telemetry_probe(
                command,
                Duration::from_secs(10)
            )),
            Some(8192)
        );
    }
}

#[test]
fn macos_phys_footprint_parser_selects_current_byte_value() {
    let output = "\
======================================================================\n\
cass [123]: 64-bit    Footprint: 9000000 B (16384 bytes per page)\n\
======================================================================\n\
\n\
Auxiliary data:\n\
    phys_footprint_peak: 14000000 B\n\
    phys_footprint: 12000000 B\n";
    assert_eq!(
        macos_phys_footprint_bytes_from_output(output),
        Some(12_000_000)
    );
    assert_eq!(
        macos_phys_footprint_bytes_from_output("phys_footprint_peak: 42 B\n"),
        None,
        "a peak-only report must not masquerade as the current footprint"
    );
    assert_eq!(
        macos_phys_footprint_bytes_from_output("phys_footprint: unknown\n"),
        None
    );
}

fn cfg() -> GovernorConfig {
    GovernorConfig {
        available_parallelism: 16,
        reserved_cores: 2,
        max_workers: 14,
        max_inflight_bytes: DEFAULT_MAX_INFLIGHT_BYTES,
        min_inflight_bytes: DEFAULT_MIN_INFLIGHT_BYTES,
        min_capacity_pct: DEFAULT_MIN_CAPACITY_PCT,
        max_load_per_core: DEFAULT_MAX_LOAD_PER_CORE,
        severe_load_per_core: DEFAULT_SEVERE_LOAD_PER_CORE,
        max_psi_avg10: DEFAULT_MAX_PSI_AVG10,
        severe_psi_avg10: DEFAULT_SEVERE_PSI_AVG10,
        growth_consecutive_healthy_ticks: DEFAULT_GROWTH_CONSECUTIVE_HEALTHY_TICKS,
        tick: Duration::from_millis(1),
        disabled: false,
        // Conformal knobs default to `Static` so the existing tests
        // continue to exercise the original threshold policy unchanged.
        calibration_mode: CalibrationMode::Static,
        conformal_k: DEFAULT_CONFORMAL_K,
        conformal_k_min: DEFAULT_CONFORMAL_K_MIN,
        conformal_alpha_pressured: DEFAULT_CONFORMAL_ALPHA_PRESSURED,
        conformal_alpha_severe: DEFAULT_CONFORMAL_ALPHA_SEVERE,
        drift_delta: DEFAULT_DRIFT_DELTA,
        drift_lambda: DEFAULT_DRIFT_LAMBDA,
    }
}

fn healthy() -> HealthSnapshot {
    HealthSnapshot {
        load_per_core: Some(0.1),
        psi_cpu_some_avg10: Some(0.0),
    }
}

fn pressured() -> HealthSnapshot {
    HealthSnapshot {
        load_per_core: Some(1.5),
        psi_cpu_some_avg10: Some(5.0),
    }
}

fn severe() -> HealthSnapshot {
    HealthSnapshot {
        load_per_core: Some(3.0),
        psi_cpu_some_avg10: Some(80.0),
    }
}

/// A test-only `HealthReader` that returns a scripted sequence of
/// snapshots. Once the script is exhausted, the last entry is
/// repeated so callers that run the sampler for extra ticks see a
/// stable tail.
struct ScriptedReader {
    snapshots: std::sync::Mutex<std::collections::VecDeque<HealthSnapshot>>,
    fallback: HealthSnapshot,
}

impl ScriptedReader {
    fn new(script: Vec<HealthSnapshot>) -> Self {
        let fallback = *script.last().unwrap_or(&HealthSnapshot {
            load_per_core: None,
            psi_cpu_some_avg10: None,
        });
        Self {
            snapshots: std::sync::Mutex::new(script.into()),
            fallback,
        }
    }
}

impl HealthReader for ScriptedReader {
    fn snapshot(&self) -> HealthSnapshot {
        let mut guard = self.snapshots.lock().expect("scripted reader mutex");
        guard.pop_front().unwrap_or(self.fallback)
    }
}

/// Build a test-only governor that never spawns a thread; caller drives
/// it via `step_once()`.
fn build_test_governor(cfg: GovernorConfig, script: Vec<HealthSnapshot>) -> Governor {
    Governor::new(cfg, Arc::new(ScriptedReader::new(script)))
}

#[test]
fn disabled_config_always_returns_full_capacity() {
    let mut c = cfg();
    c.disabled = true;
    let snap = HealthSnapshot {
        load_per_core: Some(10.0),
        psi_cpu_some_avg10: Some(90.0),
    };
    let (next, streak, reason) = next_capacity(50, 0, &snap, &c);
    assert_eq!(next, 100);
    assert_eq!(streak, 0);
    assert_eq!(reason, GovernorDecisionReason::Disabled);
}

#[test]
fn healthy_snapshot_does_not_grow_before_streak_threshold() {
    let c = cfg();
    let h = healthy();
    let (next, streak, reason) = next_capacity(50, 0, &h, &c);
    assert_eq!(next, 50);
    assert_eq!(streak, 1);
    assert_eq!(reason, GovernorDecisionReason::HealthyHold);

    let (next, streak, reason) = next_capacity(next, streak, &h, &c);
    assert_eq!(next, 50);
    assert_eq!(streak, 2);
    assert_eq!(reason, GovernorDecisionReason::HealthyHold);

    let (next, streak, reason) = next_capacity(next, streak, &h, &c);
    assert_eq!(next, 75);
    assert_eq!(streak, 0, "streak must reset after a growth step");
    assert_eq!(reason, GovernorDecisionReason::HealthyGrow);
}

#[test]
fn healthy_at_ceiling_is_classified_as_ceiling_hold() {
    let c = cfg();
    let h = healthy();
    let (next, streak, reason) = next_capacity(100, 2, &h, &c);
    // Third healthy tick fires, capacity is already 100 so we hold.
    assert_eq!(next, 100);
    assert_eq!(reason, GovernorDecisionReason::HealthyCeilingHold);
    assert_eq!(streak, 3, "ceiling hold keeps streak rather than resetting");
}

#[test]
fn moderate_pressure_shrinks_immediately() {
    let c = cfg();
    let p = pressured();
    let (next, streak, reason) = next_capacity(100, 2, &p, &c);
    assert_eq!(next, 75);
    assert_eq!(streak, 0);
    assert_eq!(reason, GovernorDecisionReason::Pressured);

    let (next, streak, reason) = next_capacity(next, streak, &p, &c);
    assert_eq!(next, 50);
    assert_eq!(streak, 0);
    assert_eq!(reason, GovernorDecisionReason::Pressured);

    // Floor holds even if pressure persists, and the hold is classified.
    let (next, _, reason) = next_capacity(DEFAULT_MIN_CAPACITY_PCT, 0, &p, &c);
    assert_eq!(next, DEFAULT_MIN_CAPACITY_PCT);
    assert_eq!(reason, GovernorDecisionReason::PressuredFloorHold);
}

#[test]
fn severe_pressure_drops_straight_to_floor() {
    let c = cfg();
    let s = severe();
    let (next, streak, reason) = next_capacity(100, 2, &s, &c);
    assert_eq!(next, DEFAULT_MIN_CAPACITY_PCT);
    assert_eq!(streak, 0);
    assert_eq!(reason, GovernorDecisionReason::Severe);
}

#[test]
fn scale_worker_count_never_below_one_and_never_above_desired() {
    assert_eq!(scale_worker_count(0, 100), 0);
    assert_eq!(scale_worker_count(16, 100), 16);
    assert_eq!(scale_worker_count(16, 50), 8);
    assert_eq!(scale_worker_count(16, 25), 4);
    assert_eq!(scale_worker_count(1, 1), 1);
    assert!(scale_worker_count(4, 100) <= 4);
}

#[test]
fn default_reserved_core_policy_preserves_interactive_headroom() {
    assert_eq!(default_reserved_cores_for_available(1), 0);
    assert_eq!(default_reserved_cores_for_available(2), 1);
    assert_eq!(default_reserved_cores_for_available(8), 2);
    assert_eq!(default_reserved_cores_for_available(64), 8);
}

#[test]
fn worker_policy_applies_reserved_cores_and_live_capacity() {
    let mut c = cfg();
    c.available_parallelism = 16;
    c.reserved_cores = 4;
    c.max_workers = 20;

    assert_eq!(scale_worker_count_with_policy(64, 100, &c), 12);
    assert_eq!(scale_worker_count_with_policy(64, 50, &c), 6);
    assert_eq!(scale_worker_count_with_policy(2, 50, &c), 1);
    assert_eq!(scale_worker_count_with_policy(0, 50, &c), 0);
}

#[test]
fn inflight_byte_policy_caps_and_scales_without_increasing_small_budgets() {
    let mut c = cfg();
    c.max_inflight_bytes = 128 * 1024 * 1024;
    c.min_inflight_bytes = 8 * 1024 * 1024;

    assert_eq!(
        scale_inflight_byte_limit(512 * 1024 * 1024, 100, &c),
        128 * 1024 * 1024
    );
    assert_eq!(
        scale_inflight_byte_limit(512 * 1024 * 1024, 25, &c),
        32 * 1024 * 1024
    );
    assert_eq!(
        scale_inflight_byte_limit(4 * 1024 * 1024, 25, &c),
        4 * 1024 * 1024
    );
    assert_eq!(scale_inflight_byte_limit(0, 25, &c), 0);
}

#[test]
fn default_inflight_byte_budget_scales_with_available_memory() {
    let gib = 1024_u64 * 1024 * 1024;

    assert_eq!(
        default_max_inflight_bytes_for_available(None),
        DEFAULT_MAX_INFLIGHT_BYTES
    );
    assert_eq!(
        default_max_inflight_bytes_for_available(Some(2 * gib)),
        DEFAULT_MAX_INFLIGHT_BYTES,
        "small hosts keep the old conservative floor"
    );
    assert_eq!(
        default_max_inflight_bytes_for_available(Some(256 * gib)),
        8 * 1024 * 1024 * 1024,
        "256 GiB hosts can keep materially more work in flight"
    );
    assert_eq!(
        default_max_inflight_bytes_for_available(Some(1024 * gib)),
        usize::try_from(DEFAULT_MAX_INFLIGHT_BYTES_CEILING).unwrap_or(usize::MAX),
        "very large hosts are still bounded"
    );
}

#[test]
fn env_disable_signal_is_truthy_aware() {
    let probe = "__CASS_RESP_DISABLE_PARSE_PROBE__";
    let prior = std::env::var(probe).ok();
    for truthy in ["1", "true", "True", "YES", "on"] {
        // SAFETY: test-scoped env mutation with a unique sentinel key.
        unsafe {
            std::env::set_var(probe, truthy);
        }
        assert!(
            env_bool_truthy(probe),
            "expected `{truthy}` to be recognized as truthy"
        );
    }
    for falsy in ["0", "false", "No", "off", ""] {
        // SAFETY: test-scoped env mutation with a unique sentinel key.
        unsafe {
            std::env::set_var(probe, falsy);
        }
        assert!(
            !env_bool_truthy(probe),
            "expected `{falsy}` to be recognized as falsy"
        );
    }
    // SAFETY: test-scoped env cleanup.
    unsafe {
        std::env::remove_var(probe);
    }
    assert!(!env_bool_truthy(probe), "absent env var must be falsy");
    if let Some(v) = prior {
        // SAFETY: test-scoped env restore.
        unsafe {
            std::env::set_var(probe, v);
        }
    }
}

#[test]
fn snapshot_classification_tolerates_missing_signals() {
    let c = cfg();
    let no_signals = HealthSnapshot {
        load_per_core: None,
        psi_cpu_some_avg10: None,
    };
    assert!(!no_signals.is_severe(&c));
    assert!(!no_signals.is_pressured(&c));
    let (next, streak, reason) = next_capacity(80, 0, &no_signals, &c);
    assert_eq!(next, 80);
    assert_eq!(streak, 1);
    assert_eq!(reason, GovernorDecisionReason::HealthyHold);
}

#[test]
fn telemetry_counts_shrink_and_grow_events() {
    // Script: 1 severe (shrink to floor), then enough healthies to grow
    // all the way back. Default floor is 25 → need (100-25)/25 = 3 grow
    // steps, each requiring 3 healthy ticks = 9 ticks. Plus the 1 severe.
    let mut script = vec![severe()];
    script.extend(std::iter::repeat_n(healthy(), 9));
    let gov = build_test_governor(cfg(), script);

    for _ in 0..10 {
        gov.step_once();
    }

    let tele = gov.telemetry();
    assert_eq!(
        tele.current_capacity_pct, 100,
        "should have recovered to ceiling after 9 healthy ticks"
    );
    assert_eq!(tele.shrink_count, 1, "one severe drop = one shrink");
    assert_eq!(
        tele.grow_count, 3,
        "recovery from 25 to 100 in 25pp steps = 3 grow events"
    );
    assert_eq!(tele.ticks_total, 10);

    // The ring buffer should contain the severe drop plus the three
    // grow events (healthy-hold ticks are deliberately not recorded).
    let reasons: Vec<GovernorDecisionReason> =
        tele.recent_decisions.iter().map(|d| d.reason).collect();
    assert_eq!(
        reasons,
        vec![
            GovernorDecisionReason::Severe,
            GovernorDecisionReason::HealthyGrow,
            GovernorDecisionReason::HealthyGrow,
            GovernorDecisionReason::HealthyGrow,
        ]
    );
}

#[test]
fn telemetry_ring_buffer_is_bounded() {
    // Feed more than TELEMETRY_DECISION_HISTORY pressured ticks so the
    // buffer wraps. All ticks are "pressured" (either real step-down or
    // floor-hold) so every tick is recorded.
    let count = TELEMETRY_DECISION_HISTORY + 50;
    let script = std::iter::repeat_n(pressured(), count).collect::<Vec<_>>();
    let gov = build_test_governor(cfg(), script);
    for _ in 0..count {
        gov.step_once();
    }

    let tele = gov.telemetry();
    assert_eq!(
        tele.recent_decisions.len(),
        TELEMETRY_DECISION_HISTORY,
        "ring buffer must saturate at its cap"
    );
    assert_eq!(tele.ticks_total, count as u64);
    // The newest entry should be the most-recent tick (elapsed_ms
    // monotonically increases).
    let last = tele.recent_decisions.last().unwrap();
    let first = tele.recent_decisions.first().unwrap();
    assert!(
        last.at_elapsed_ms >= first.at_elapsed_ms,
        "ring buffer must preserve chronological order"
    );
}

#[test]
fn telemetry_skips_healthy_hold_ticks() {
    // A long run of healthy-hold ticks below the growth threshold should
    // NOT accumulate buffer entries.
    let script = std::iter::repeat_n(healthy(), 2).collect::<Vec<_>>();
    let gov = build_test_governor(cfg(), script);
    for _ in 0..2 {
        gov.step_once();
    }
    let tele = gov.telemetry();
    assert_eq!(
        tele.recent_decisions.len(),
        0,
        "healthy-hold ticks should not pollute the ring buffer"
    );
    assert_eq!(tele.current_capacity_pct, 100);
}

#[test]
fn telemetry_survives_mutex_poison() {
    // Regression: an earlier version used `if let Ok(guard) = lock()` /
    // `match Ok/Err` to access the runtime state, which silently dropped
    // every telemetry update for the rest of the process if any thread
    // panicked while holding the mutex. Switching to
    // `unwrap_or_else(PoisonError::into_inner)` means a single panic
    // cannot mute the governor forever.
    let gov = Arc::new(build_test_governor(
        cfg(),
        std::iter::repeat_n(pressured(), 4).collect(),
    ));
    // Poison the runtime mutex deliberately by panicking inside a
    // closure that holds the lock.
    {
        let poison_gov = Arc::clone(&gov);
        let handle = std::thread::spawn(move || {
            let _held = poison_gov
                .runtime
                .lock()
                .expect("fresh mutex should not be poisoned");
            panic!("intentional poison for regression test");
        });
        let _ = handle.join();
    }
    assert!(
        gov.runtime.is_poisoned(),
        "mutex must be poisoned after the helper thread's panic"
    );

    // Now drive the sampler. If the old `if let Ok(...)` guard were
    // still in place, none of these ticks would be recorded.
    for _ in 0..4 {
        gov.step_once();
    }

    let tele = gov.telemetry();
    assert_eq!(
        tele.ticks_total, 4,
        "atomics update regardless of mutex state"
    );
    assert!(
        !tele.recent_decisions.is_empty(),
        "telemetry must continue to record after mutex poison, got: {tele:?}"
    );
    assert_eq!(
        tele.recent_decisions.first().unwrap().reason,
        GovernorDecisionReason::Pressured,
        "first recorded decision after poison should still classify correctly"
    );
}

#[test]
fn telemetry_serializes_to_json_with_expected_keys() {
    let gov = build_test_governor(cfg(), vec![severe(), pressured()]);
    gov.step_once();
    gov.step_once();
    let tele = gov.telemetry();
    let json = serde_json::to_string(&tele).expect("telemetry serializes");
    for key in [
        "current_capacity_pct",
        "resource_policy",
        "reserved_cores",
        "max_inflight_bytes",
        "shrink_count",
        "grow_count",
        "ticks_total",
        "disabled_via_env",
        "last_snapshot",
        "last_reason",
        "recent_decisions",
        "healthy_streak",
    ] {
        assert!(
            json.contains(key),
            "expected JSON to contain `{key}`, got: {json}"
        );
    }
    // Spot-check that reason serializes as a snake_case string.
    assert!(
        json.contains("\"severe\"") || json.contains("\"pressured\""),
        "expected snake_case reason tag in JSON: {json}"
    );
}

// -----------------------------------------------------------------
// Anti-oscillation stress tests (bead d2qix anti-flap hardening)
// -----------------------------------------------------------------

fn run_script_and_trace(cfg: GovernorConfig, script: Vec<HealthSnapshot>) -> (Governor, Vec<u32>) {
    let tick_count = script.len();
    let gov = build_test_governor(cfg, script);
    let mut capacities = Vec::with_capacity(tick_count);
    for _ in 0..tick_count {
        gov.step_once();
        capacities.push(gov.current_capacity.load(Ordering::Relaxed));
    }
    (gov, capacities)
}

fn transitions(capacities: &[u32]) -> usize {
    capacities
        .windows(2)
        .filter(|pair| pair[0] != pair[1])
        .count()
}

#[test]
fn anti_flap_alternating_pressured_healthy_never_grows() {
    // Alternate pressured/healthy for 100 ticks. Each healthy tick
    // must reset the growth streak (because it always follows a
    // pressured tick which reset it to 0), so capacity should never
    // grow back. The floor absorbs repeated pressure; only the first
    // pressured tick actually shrinks because we start at 100%.
    let mut script = Vec::with_capacity(100);
    for i in 0..100 {
        script.push(if i % 2 == 0 { pressured() } else { healthy() });
    }
    let (gov, capacities) = run_script_and_trace(cfg(), script);

    let tele = gov.telemetry();
    assert_eq!(
        tele.grow_count, 0,
        "alternating flap must never produce a grow event"
    );
    // Over 100 ticks, shrinks happen each pressured tick until we hit
    // the floor (100 → 75 → 50 → 25 = 3 shrinks). After that, pressure
    // samples hit the PressuredFloorHold branch with no shrink.
    assert_eq!(tele.shrink_count, 3, "flap shrinks until floor, then holds");
    let t = transitions(&capacities);
    assert!(
        t <= 3,
        "alternating flap must not oscillate capacity; saw {t} transitions over {} ticks",
        capacities.len()
    );
}

#[test]
fn anti_flap_near_threshold_jitter_does_not_oscillate() {
    // Jitter around the pressured threshold. With max_load_per_core=1.25,
    // load samples of 1.24 are healthy-hold, 1.26 are pressured.
    let mut script = Vec::with_capacity(60);
    for i in 0..60 {
        script.push(HealthSnapshot {
            load_per_core: Some(if i % 2 == 0 { 1.24 } else { 1.26 }),
            psi_cpu_some_avg10: Some(1.0),
        });
    }
    let (_gov, capacities) = run_script_and_trace(cfg(), script);
    let t = transitions(&capacities);
    // Shrink on each pressured tick up to the floor (3 shrinks), then
    // no growth because each healthy tick follows a pressured tick
    // which just reset the streak.
    assert!(
        t <= 3,
        "threshold jitter must not cause capacity oscillation; saw {t} transitions"
    );
}

#[test]
fn anti_flap_burst_recovery_respects_hysteresis() {
    // Alternate blocks of severe pressure and recovery windows. After
    // each severe burst, growth requires exactly growth_ticks healthy
    // samples per 25pp step.
    let mut script = Vec::new();
    for _ in 0..3 {
        for _ in 0..5 {
            script.push(severe());
        }
        for _ in 0..9 {
            script.push(healthy());
        }
    }
    let (gov, capacities) = run_script_and_trace(cfg(), script);

    let tele = gov.telemetry();
    // Three severe bursts each drop from wherever we are straight to
    // the floor; but the FIRST burst starts at 100% so it drops to 25%
    // (one shrink event). Subsequent bursts start at 100% too (after
    // recovery), so they each produce one shrink event. 3 shrinks.
    assert_eq!(tele.shrink_count, 3, "one shrink per severe burst");
    // Each recovery window has 9 healthy ticks = 3 growth steps = 3
    // grow events per burst, × 3 bursts = 9 grow events.
    assert_eq!(
        tele.grow_count, 9,
        "each 9-tick healthy window produces 3 grow steps"
    );
    // `transitions()` only counts pairs in `capacities`, i.e. it compares
    // post-tick values. The initial 100 → 25 shrink of the *first* burst
    // happens before any capacity has been sampled, so it doesn't appear
    // as a transition between adjacent elements. So:
    //   burst 1: 3 grow transitions (the initial shrink is invisible)
    //   burst 2: 1 shrink + 3 grow = 4 transitions
    //   burst 3: 1 shrink + 3 grow = 4 transitions
    // Total = 11. This is consistent with `shrink_count=3` and
    // `grow_count=9` (which the Governor tracks against its starting
    // capacity, not against the capacities vec).
    let t = transitions(&capacities);
    assert_eq!(t, 11);
    assert!(
        (t as f64) / (capacities.len() as f64) <= 1.0 / 3.0,
        "transition rate must respect the 3-tick hysteresis"
    );
}

#[test]
fn anti_flap_transition_rate_upper_bound() {
    // Property-style guard: for any interleaving, transitions per K
    // ticks must never exceed `ceil(K / growth_consecutive_healthy_ticks) + K/growth_ticks + shrink_budget`.
    // Concretely we pick a pathological worst-case where growth fires as
    // fast as possible (3 healthy, grow; 3 healthy, grow; ...). That's
    // one transition every 3 ticks for grow, plus shrink-on-every-
    // pressured. Even then the rate is bounded.
    let growth_ticks = DEFAULT_GROWTH_CONSECUTIVE_HEALTHY_TICKS as usize;
    // 120 ticks: alternate windows of 3 healthy + 1 severe.
    let mut script = Vec::with_capacity(120);
    while script.len() < 120 {
        for _ in 0..growth_ticks {
            script.push(healthy());
        }
        script.push(severe());
    }
    script.truncate(120);
    let tick_count = script.len();
    let (_gov, capacities) = run_script_and_trace(cfg(), script);
    let t = transitions(&capacities);
    // Per 4-tick window: one severe drop (100 → 25 if previously at 100,
    // else same) and one grow (25 → 50). That's at most 2 transitions
    // per 4 ticks = 0.5 per tick.
    let rate = t as f64 / tick_count as f64;
    assert!(
        rate <= 0.55,
        "worst-case transition rate must stay bounded; saw {rate} over {tick_count} ticks"
    );
}

// -----------------------------------------------------------------
// Conformal-calibration tests (Card 2). All of these exercise
// pure-function helpers (quantile, MAD, Page-Hinkley) plus the
// Governor::step_once integration; the 17 static-mode tests above
// must also stay green (PO-C2-2 bit-exact compatibility).
// -----------------------------------------------------------------

#[test]
fn conformal_quantile_index_matches_vovk_theorem_1() {
    // Theorem 1 formula: index = ⌈(K+1)(1-α)⌉ - 1, clamped to [0, K-1].
    let sorted: Vec<f32> = (0..256).map(|i| i as f32).collect();
    // K=256, α=0.05 → ⌈257·0.95⌉-1 = 245-1 = 244-1 = 243
    //   ceil(244.15) = 245, so index = 244... let's compute directly:
    //   (256+1)*(1-0.05) = 244.15 → ceil = 245 → -1 = 244
    // So the returned value should be sorted[244] = 244.0.
    let q = conformal_quantile_sorted(&sorted, 0.05).unwrap();
    assert!(
        (q - 244.0).abs() < 1e-6,
        "K=256, α=0.05 expected sorted[244]=244 but got {q}"
    );

    // Tighter α should yield a higher quantile.
    let q_tight = conformal_quantile_sorted(&sorted, 0.01).unwrap();
    assert!(q_tight > q, "α=0.01 must produce q̂ ≥ α=0.05 q̂");
}

#[test]
fn conformal_quantile_clamps_to_last_element_on_tiny_window() {
    let sorted = [0.0_f32, 1.0, 2.0, 3.0, 4.0];
    // K=5, α=0.01 → ⌈6·0.99⌉-1 = 6-1 = 5 → clamped to 4 (last idx).
    let q = conformal_quantile_sorted(&sorted, 0.01).unwrap();
    assert_eq!(q, 4.0);
}

#[test]
fn conformal_quantile_rejects_pathological_alpha() {
    let sorted = [1.0_f32, 2.0, 3.0];
    assert!(conformal_quantile_sorted(&sorted, 0.0).is_none());
    assert!(conformal_quantile_sorted(&sorted, 1.0).is_none());
    assert!(conformal_quantile_sorted(&sorted, f32::NAN).is_none());
}

#[test]
fn conformal_coverage_on_iid_uniform_meets_guarantee() {
    // Classical split-conformal validation: generate N iid samples,
    // calibrate q̂ on K of them, test on the remaining. Observed
    // coverage should be within sqrt(α(1-α)/N) of the guaranteed
    // 1-α floor.
    let mut stream = CalibrationStream::new(256);
    // Deterministic "uniform" via halton-like sequence so the test
    // is not dependent on a PRNG seed.
    for i in 0..256 {
        let v = (i as f32) * 0.0039; // 0 to ~1.0 uniformly
        stream.push(v);
    }
    let q = stream.quantile(0.05, 32).unwrap();
    // Empirical coverage on a fresh test set of 1024 identically
    // distributed values.
    let mut covered = 0usize;
    let test_n = 1024;
    for i in 0..test_n {
        let v = (i as f32) * (1.0 / test_n as f32);
        if v <= q {
            covered += 1;
        }
    }
    let coverage = covered as f32 / test_n as f32;
    // Target 1-α = 0.95, finite-sample slack ≈ 3σ ≈ 0.02 for N=1024.
    // We assert coverage within [0.90, 1.00] to stay robust to the
    // halton-sequence non-iid pattern while still catching a real
    // breakage of the quantile formula.
    assert!(
        (0.90..=1.00).contains(&coverage),
        "observed coverage {coverage} outside [0.90, 1.00] window"
    );
}

#[test]
fn mad_rejects_obvious_outlier_on_stationary_stream() {
    let mut stream = CalibrationStream::new(64);
    for _ in 0..32 {
        stream.push(1.0);
    }
    for _ in 0..32 {
        stream.push(1.2);
    }
    // median ≈ 1.1, MAD ≈ 0.1 → reject anything > 1.1 + 3.5·0.1 = 1.45
    assert!(stream.is_outlier(10.0));
    assert!(stream.is_outlier(1.5));
    assert!(!stream.is_outlier(1.15));
}

#[test]
fn mad_is_not_an_outlier_on_empty_window() {
    let stream = CalibrationStream::new(16);
    // Empty window has no median/MAD; nothing can be an outlier yet.
    assert!(!stream.is_outlier(100.0));
}

#[test]
fn page_hinkley_does_not_trip_on_stationary_stream() {
    let mut ph = PageHinkley::new(0.01, 0.5);
    // 10 000 samples drawn from roughly-stationary N(0, 0.01).
    // Deterministic pseudo-random via LCG so test is reproducible.
    let mut state: u32 = 12345;
    let mut trips = 0;
    for _ in 0..10_000 {
        state = state.wrapping_mul(1103515245).wrapping_add(12345);
        let r = (state as f32 / u32::MAX as f32) * 0.02 - 0.01; // [-0.01, 0.01]
        if ph.observe(r) {
            trips += 1;
            ph.reset();
        }
    }
    // With δ=0.01 the stream barely accumulates drift; stationary
    // should trip ≤ 100 times per 10 000 (< 1 %).
    assert!(
        trips < 100,
        "page-hinkley tripped {trips} times on stationary stream (expected < 100)"
    );
}

#[test]
fn page_hinkley_trips_on_clear_mean_shift() {
    let mut ph = PageHinkley::new(0.01, 0.5);
    // Phase 1: 500 zero-mean samples.
    for _ in 0..500 {
        ph.observe(0.0);
    }
    // Phase 2: mean shifts to +0.5 and stays there.
    let mut trips_in_phase_2 = 0;
    for _ in 0..500 {
        if ph.observe(0.5) {
            trips_in_phase_2 += 1;
            // reset so we can confirm detection lag is short
            ph.reset();
            break;
        }
    }
    assert!(
        trips_in_phase_2 >= 1,
        "page-hinkley missed a clear 0.5-magnitude mean shift"
    );
}

fn conformal_cfg() -> GovernorConfig {
    GovernorConfig {
        calibration_mode: CalibrationMode::Conformal,
        conformal_k: 64,
        conformal_k_min: 16,
        conformal_alpha_pressured: 0.05,
        conformal_alpha_severe: 0.01,
        drift_delta: 0.01,
        drift_lambda: 0.5,
        ..cfg()
    }
}

#[test]
fn conformal_mode_static_behavior_until_k_min_reached() {
    // Before the window is full enough, the governor should behave
    // exactly like static mode. Drive it through healthy ticks that
    // are under the STATIC pressured threshold (load=0.1 < 1.25),
    // so even though they're below threshold they populate the
    // window. We should see zero shrinks during warm-up.
    let script = std::iter::repeat_n(healthy(), 20).collect();
    let gov = build_test_governor(conformal_cfg(), script);
    for _ in 0..20 {
        gov.step_once();
    }
    let tele = gov.telemetry();
    assert_eq!(
        tele.shrink_count, 0,
        "no shrinks expected during healthy-only warm-up"
    );
    // Calibration telemetry should be present and should report the
    // window filling up.
    let cal = tele
        .calibration
        .expect("conformal mode must emit calibration telemetry");
    assert_eq!(cal.mode, CalibrationMode::Conformal);
    assert!(cal.load_window_len > 0);
}

#[test]
fn conformal_mode_never_inverts_severe_vs_pressured_invariant() {
    // PO-C2-3: when `observe_and_compute_thresholds` returns
    // `Some(effective)`, the severe thresholds must strictly exceed
    // the pressured thresholds. We exercise this contract directly
    // on `GovernorCalibration` because the governor-level shape of
    // the invariant depends on whether ANY samples reach the top
    // quantile bins, which is an artifact of sample count × α.
    let cfg_conf = conformal_cfg();
    let mut cal = GovernorCalibration::new(&cfg_conf);
    // Stationary pseudo-random LCG over [0.05, 1.05]: same mean and
    // variance throughout, so Page-Hinkley sees no drift. Enough
    // unique values that the α=0.01 and α=0.05 quantiles pick
    // different sorted positions.
    let mut state: u32 = 987654321;
    for _ in 0..96 {
        state = state.wrapping_mul(1103515245).wrapping_add(12345);
        let v = 0.05 + (state as f32 / u32::MAX as f32);
        let snap = HealthSnapshot {
            load_per_core: Some(v),
            psi_cpu_some_avg10: Some(v * 10.0),
        };
        let thresholds = cal.observe_and_compute_thresholds(&snap, true, &cfg_conf);
        // When thresholds are emitted, they must satisfy the
        // invariant; when they are None (still warming up or
        // degenerate), the governor falls back to static, which is
        // also safe.
        if let Some(t) = thresholds {
            assert!(
                t.severe_load > t.pressured_load,
                "PO-C2-3 violated for load: severe {} !> pressured {}",
                t.severe_load,
                t.pressured_load
            );
            assert!(
                t.severe_psi > t.pressured_psi,
                "PO-C2-3 violated for psi: severe {} !> pressured {}",
                t.severe_psi,
                t.pressured_psi
            );
        }
    }
    // At the end we should have emitted at least once — otherwise
    // the test isn't exercising the invariant.
    let tele = cal.telemetry(&cfg_conf);
    assert!(
        tele.load_pressured_q.is_some() && tele.load_severe_q.is_some(),
        "expected both load quantiles to be emitted by the end of the loop"
    );
}

#[test]
fn conformal_mode_falls_back_to_static_on_degenerate_window() {
    // When every sample is identical, both quantiles collide. PO-C2-3
    // requires the apply-calibration path to refuse rather than emit
    // an inverted pair — degenerate distributions fall back to the
    // static thresholds silently (safe behavior).
    let script = std::iter::repeat_n(healthy(), 80).collect();
    let gov = build_test_governor(conformal_cfg(), script);
    for _ in 0..80 {
        gov.step_once();
    }
    let tele = gov.telemetry();
    // Governor should have spent all 80 ticks on static thresholds —
    // no shrinks, no grows (except the built-in 3-tick healthy-streak
    // hold pattern which can trigger HealthyCeilingHold from 100).
    assert_eq!(tele.shrink_count, 0);
    // Calibration telemetry is present but the quantiles may or may
    // not be emitted; either way behaviour was static-safe.
    assert!(tele.calibration.is_some());
}

#[test]
fn conformal_telemetry_serializes_with_calibration_block() {
    let script = std::iter::repeat_n(healthy(), 40).collect();
    let gov = build_test_governor(conformal_cfg(), script);
    for _ in 0..40 {
        gov.step_once();
    }
    let tele = gov.telemetry();
    let json = serde_json::to_string(&tele).expect("telemetry serialization");
    for key in [
        "calibration",
        "\"mode\":\"conformal\"",
        "load_window_len",
        "psi_window_len",
        "conformal_k",
        "drift_reset_count",
        "outliers_rejected",
    ] {
        assert!(
            json.contains(key),
            "expected JSON to contain `{key}`; got: {json}"
        );
    }
}

// -----------------------------------------------------------------
// Decision-replay harness (P-M3): feed the SAME synthetic signal
// trace through both static and conformal governors, count the
// shrink/grow events each produces. This is the simplest way to
// answer "does conformal behave worse than static on a realistic
// idle trace?" without running a full cass index rebuild.
// -----------------------------------------------------------------

/// Generate a synthetic loadavg trace that mimics an idle dev box:
/// baseline `~0.3 / core` with small Poisson-like spikes.
fn idle_dev_box_trace(n: usize) -> Vec<HealthSnapshot> {
    let mut state: u32 = 42;
    (0..n)
        .map(|_| {
            // Deterministic PRNG; uniform [0, 1).
            state = state.wrapping_mul(1103515245).wrapping_add(12345);
            let u = (state as f32 / u32::MAX as f32).clamp(0.0, 0.9999);
            // Most ticks land around 0.3; rare spikes to ~1.0.
            let load = if u < 0.95 {
                0.2 + u * 0.3
            } else {
                0.9 + u * 0.2
            };
            HealthSnapshot {
                load_per_core: Some(load),
                psi_cpu_some_avg10: Some(load * 8.0),
            }
        })
        .collect()
}

fn run_replay(mut cfg: GovernorConfig, script: Vec<HealthSnapshot>) -> GovernorTelemetry {
    cfg.tick = Duration::from_millis(1);
    let gov = build_test_governor(cfg, script.clone());
    for _ in 0..script.len() {
        gov.step_once();
    }
    gov.telemetry()
}

#[test]
#[serial]
fn conformal_vs_static_idle_dev_trace_is_not_materially_worse() {
    // Feed both governors the same 2 048-tick idle-dev trace and
    // compare shrink counts. An idle trace (load stays under the
    // static 1.25 threshold almost everywhere) should produce
    // ZERO static-mode shrinks; a well-calibrated conformal
    // governor should produce a small, bounded number of shrinks
    // matching its 5% false-positive target (~100 over 2 048 ticks
    // under adversarial exchangeable samples; typically far fewer
    // on a stationary trace).
    let script = idle_dev_box_trace(2_048);
    let static_cfg = GovernorConfig {
        calibration_mode: CalibrationMode::Static,
        ..cfg()
    };
    let conf_cfg = GovernorConfig {
        calibration_mode: CalibrationMode::Conformal,
        conformal_k: 256,
        conformal_k_min: 32,
        conformal_alpha_pressured: 0.05,
        conformal_alpha_severe: 0.01,
        drift_delta: 0.01,
        drift_lambda: 0.5,
        ..cfg()
    };

    let static_tele = run_replay(static_cfg, script.clone());
    let conformal_tele = run_replay(conf_cfg, script);

    // Hard floor: conformal must not produce 10× more shrinks than
    // static. Our 5% target means conformal has a distribution-free
    // guarantee of ≤ ~102 spurious shrinks on a worst-case
    // exchangeable 2 048-tick trace. If it blows through 10× that,
    // something is broken (bad K, bad α, broken quantile).
    eprintln!(
        "replay trace: static shrinks={}, conformal shrinks={}, \
             static grows={}, conformal grows={}",
        static_tele.shrink_count,
        conformal_tele.shrink_count,
        static_tele.grow_count,
        conformal_tele.grow_count,
    );
    assert!(
        conformal_tele.shrink_count <= 1024,
        "conformal shrink_count={} is more than 10x the α=0.05 theoretical FP budget — conformal calibration is misbehaving",
        conformal_tele.shrink_count
    );
}

#[test]
#[serial]
fn conformal_vs_static_under_sustained_pressure_shrinks_similarly() {
    // Both policies should shrink aggressively once a severe-class
    // signal arrives. This catches the opposite failure: conformal
    // learning thresholds that are too permissive for actual load
    // spikes.
    let pressured_trace: Vec<HealthSnapshot> = std::iter::repeat_n(severe(), 128).collect();
    let static_cfg = GovernorConfig {
        calibration_mode: CalibrationMode::Static,
        ..cfg()
    };
    let conf_cfg = GovernorConfig {
        calibration_mode: CalibrationMode::Conformal,
        conformal_k: 256,
        conformal_k_min: 32,
        conformal_alpha_pressured: 0.05,
        conformal_alpha_severe: 0.01,
        drift_delta: 0.01,
        drift_lambda: 0.5,
        ..cfg()
    };

    let static_tele = run_replay(static_cfg, pressured_trace.clone());
    let conformal_tele = run_replay(conf_cfg, pressured_trace);

    // Both must aggressively drop capacity to the min_capacity floor.
    assert_eq!(static_tele.current_capacity_pct, cfg().min_capacity_pct);
    assert_eq!(
        conformal_tele.current_capacity_pct,
        cfg().min_capacity_pct,
        "conformal must not be more permissive than static under severe load"
    );
}
