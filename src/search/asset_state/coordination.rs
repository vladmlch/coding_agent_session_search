use std::fs::OpenOptions;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use super::maintenance::{
    SearchMaintenanceJobKind, SearchMaintenanceSnapshot, maintenance_stall_age_ms,
    read_search_maintenance_snapshot, rebuild_stall_detect_threshold_ms,
};

// ---------------------------------------------------------------------------
// Maintenance coordination: single-flight, attach-to-progress, fail-open
// ---------------------------------------------------------------------------

#[cfg_attr(not(test), allow(dead_code))]
const HEARTBEAT_STALE_THRESHOLD_MS: i64 = 30_000;
#[cfg_attr(not(test), allow(dead_code))]
const BOUNDED_WAIT_DEFAULT: Duration = Duration::from_secs(5);
#[cfg_attr(not(test), allow(dead_code))]
const POLL_INTERVAL_DEFAULT: Duration = Duration::from_millis(250);

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum MaintenanceCoordinationOutcome {
    Idle,
    Active {
        job_id: String,
        job_kind: SearchMaintenanceJobKind,
        phase: Option<String>,
        started_at_ms: i64,
        updated_at_ms: i64,
    },
    Stale {
        job_id: String,
        reason: String,
    },
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum MaintenanceDecision {
    Launch,
    AttachOrWait {
        job_id: String,
        job_kind: SearchMaintenanceJobKind,
        phase: Option<String>,
        elapsed_ms: u64,
    },
    FailOpen {
        reason: String,
    },
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn evaluate_maintenance_coordination(
    data_dir: &Path,
    now_ms: i64,
) -> MaintenanceCoordinationOutcome {
    evaluate_maintenance_coordination_from_snapshot(
        &read_search_maintenance_snapshot(data_dir),
        now_ms,
    )
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn evaluate_maintenance_coordination_from_snapshot(
    snapshot: &SearchMaintenanceSnapshot,
    now_ms: i64,
) -> MaintenanceCoordinationOutcome {
    if !snapshot.active {
        return MaintenanceCoordinationOutcome::Idle;
    }
    let job_id = maintenance_snapshot_job_id(snapshot);
    if let Some(updated_at_ms) = snapshot.updated_at_ms {
        let heartbeat_age_ms = now_ms.saturating_sub(updated_at_ms);
        if heartbeat_age_ms > HEARTBEAT_STALE_THRESHOLD_MS {
            return MaintenanceCoordinationOutcome::Stale {
                job_id,
                reason: format!(
                    "heartbeat is {heartbeat_age_ms}ms old (threshold {HEARTBEAT_STALE_THRESHOLD_MS}ms)"
                ),
            };
        }
    }
    // Forward-progress liveness check (issue #258). Even if the
    // heartbeat is fresh, treat the job as `Stale` when the indexing
    // thread itself has not posted progress for longer than the stall
    // threshold. Coordination consumers (search fail-open, attach-or-
    // wait) then route around the wedged worker instead of waiting on
    // it indefinitely.
    if let Some(stall_age_ms) = maintenance_stall_age_ms(snapshot, now_ms) {
        let threshold_ms = rebuild_stall_detect_threshold_ms().unwrap_or(0);
        return MaintenanceCoordinationOutcome::Stale {
            job_id,
            reason: format!(
                "indexing thread has not posted forward progress for {stall_age_ms}ms while the heartbeat keeps refreshing (stall threshold {threshold_ms}ms) — see issue #258"
            ),
        };
    }
    MaintenanceCoordinationOutcome::Active {
        job_id,
        job_kind: snapshot
            .job_kind
            .unwrap_or(SearchMaintenanceJobKind::LexicalRefresh),
        phase: snapshot.phase.clone(),
        started_at_ms: snapshot.started_at_ms.unwrap_or(0),
        updated_at_ms: snapshot.updated_at_ms.unwrap_or(now_ms),
    }
}

fn maintenance_snapshot_job_id(snapshot: &SearchMaintenanceSnapshot) -> String {
    snapshot
        .job_id
        .as_ref()
        .filter(|job_id| !job_id.is_empty())
        .cloned()
        .unwrap_or_else(|| {
            let mode = snapshot
                .mode
                .map(|mode| mode.as_lock_value())
                .unwrap_or("unknown");
            let owner = snapshot
                .pid
                .map(|pid| pid.to_string())
                .unwrap_or_else(|| "unknown-owner".to_string());
            format!("{mode}-active-lock-{owner}")
        })
}

fn maintenance_snapshot_job_kind(snapshot: &SearchMaintenanceSnapshot) -> SearchMaintenanceJobKind {
    snapshot
        .job_kind
        .unwrap_or(SearchMaintenanceJobKind::LexicalRefresh)
}

fn maintenance_elapsed_ms(snapshot: &SearchMaintenanceSnapshot, now_ms: i64) -> u64 {
    snapshot
        .started_at_ms
        .map(|started_at_ms| u64::try_from(now_ms.saturating_sub(started_at_ms)).unwrap_or(0))
        .unwrap_or(0)
}

fn stale_heartbeat_phase(snapshot: &SearchMaintenanceSnapshot) -> Option<String> {
    snapshot
        .phase
        .clone()
        .or_else(|| Some("stale-heartbeat".to_string()))
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn decide_maintenance_action(data_dir: &Path, now_ms: i64) -> MaintenanceDecision {
    decide_maintenance_action_from_snapshot(&read_search_maintenance_snapshot(data_dir), now_ms)
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn decide_maintenance_action_from_snapshot(
    snapshot: &SearchMaintenanceSnapshot,
    now_ms: i64,
) -> MaintenanceDecision {
    match evaluate_maintenance_coordination_from_snapshot(snapshot, now_ms) {
        MaintenanceCoordinationOutcome::Idle => MaintenanceDecision::Launch,
        MaintenanceCoordinationOutcome::Stale { job_id, .. } => MaintenanceDecision::AttachOrWait {
            job_id,
            job_kind: maintenance_snapshot_job_kind(snapshot),
            phase: stale_heartbeat_phase(snapshot),
            elapsed_ms: maintenance_elapsed_ms(snapshot, now_ms),
        },
        MaintenanceCoordinationOutcome::Active {
            job_id,
            job_kind,
            phase,
            started_at_ms,
            ..
        } => MaintenanceDecision::AttachOrWait {
            job_id,
            job_kind,
            phase,
            elapsed_ms: u64::try_from(now_ms.saturating_sub(started_at_ms)).unwrap_or(0),
        },
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn decide_search_failopen(
    data_dir: &Path,
    now_ms: i64,
    lexical_available: bool,
) -> MaintenanceDecision {
    let snapshot = read_search_maintenance_snapshot(data_dir);
    match evaluate_maintenance_coordination_from_snapshot(&snapshot, now_ms) {
        MaintenanceCoordinationOutcome::Idle => MaintenanceDecision::Launch,
        MaintenanceCoordinationOutcome::Stale { job_id, reason } => {
            if lexical_available {
                MaintenanceDecision::FailOpen {
                    reason: format!(
                        "maintenance job {job_id} has a stale heartbeat ({reason}); lexical index is available, failing open"
                    ),
                }
            } else {
                MaintenanceDecision::AttachOrWait {
                    job_id,
                    job_kind: maintenance_snapshot_job_kind(&snapshot),
                    phase: stale_heartbeat_phase(&snapshot),
                    elapsed_ms: maintenance_elapsed_ms(&snapshot, now_ms),
                }
            }
        }
        MaintenanceCoordinationOutcome::Active {
            job_id,
            job_kind,
            phase,
            started_at_ms,
            ..
        } => {
            if lexical_available {
                MaintenanceDecision::FailOpen {
                    reason: format!(
                        "maintenance job {job_id} is active; lexical index is available, failing open"
                    ),
                }
            } else {
                MaintenanceDecision::AttachOrWait {
                    job_id,
                    job_kind,
                    phase,
                    elapsed_ms: u64::try_from(now_ms.saturating_sub(started_at_ms)).unwrap_or(0),
                }
            }
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub struct PollResult {
    pub outcome: MaintenanceCoordinationOutcome,
    pub polls: u32,
    pub elapsed: Duration,
    pub timed_out: bool,
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn poll_maintenance_until_idle(
    data_dir: &Path,
    timeout: Option<Duration>,
    poll_interval: Option<Duration>,
) -> PollResult {
    let timeout = timeout.unwrap_or(BOUNDED_WAIT_DEFAULT);
    let interval = poll_interval.unwrap_or(POLL_INTERVAL_DEFAULT);
    let start = Instant::now();
    let deadline = start + timeout;
    let mut polls = 0u32;
    loop {
        let now_ms = crate::storage::sqlite::FrankenStorage::now_millis();
        let outcome = evaluate_maintenance_coordination(data_dir, now_ms);
        polls += 1;
        if matches!(outcome, MaintenanceCoordinationOutcome::Idle) {
            return PollResult {
                outcome,
                polls,
                elapsed: start.elapsed(),
                timed_out: false,
            };
        }

        let now = Instant::now();
        if now >= deadline {
            return PollResult {
                outcome,
                polls,
                elapsed: start.elapsed(),
                timed_out: true,
            };
        }
        let remaining = deadline - now;
        std::thread::sleep(interval.min(remaining));
    }
}

// ---------------------------------------------------------------------------
// Rich multi-actor event log, yield/pause signaling, unified view (ibuuh.22)
// ---------------------------------------------------------------------------

#[cfg_attr(not(test), allow(dead_code))]
const MAINTENANCE_EVENTS_FILE: &str = ".maintenance-events.jsonl";
#[cfg_attr(not(test), allow(dead_code))]
const YIELD_SIGNAL_FILE: &str = "maintenance-yield.signal";
#[cfg_attr(not(test), allow(dead_code))]
pub(super) const MAX_EVENT_LOG_ENTRIES: usize = 500;

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MaintenanceEvent {
    pub timestamp_ms: i64,
    pub job_id: String,
    pub actor_pid: u32,
    pub kind: MaintenanceEventKind,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MaintenanceEventKind {
    Started { job_kind: String, phase: String },
    PhaseChanged { from: String, to: String },
    Progress { processed: u64, total: u64 },
    YieldRequested { requester_pid: u32, reason: String },
    Paused { reason: String },
    Resumed,
    Completed { summary: String },
    Failed { error: String },
    Cancelled { reason: String },
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn append_maintenance_event(data_dir: &Path, event: &MaintenanceEvent) -> Result<()> {
    // ubs:ignore[rust.security.path-traversal] — joins compile-time MAINTENANCE_EVENTS_FILE constant
    let path = data_dir.join(MAINTENANCE_EVENTS_FILE);
    let line = serde_json::to_string(event).with_context(|| "serializing maintenance event")?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("opening maintenance event log at {}", path.display()))?;
    use std::io::Write;
    writeln!(file, "{line}")
        .with_context(|| format!("appending to maintenance event log at {}", path.display()))?;
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn read_maintenance_events(
    data_dir: &Path,
    after_ms: Option<i64>,
    limit: Option<usize>,
) -> Vec<MaintenanceEvent> {
    // ubs:ignore[rust.security.path-traversal] — joins compile-time MAINTENANCE_EVENTS_FILE constant
    let path = data_dir.join(MAINTENANCE_EVENTS_FILE);
    let contents = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    let cap = limit.unwrap_or(MAX_EVENT_LOG_ENTRIES);
    contents
        .lines()
        .filter_map(|line| serde_json::from_str::<MaintenanceEvent>(line).ok())
        .filter(|e| after_ms.is_none_or(|threshold| e.timestamp_ms > threshold))
        .rev()
        .take(cap)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn truncate_maintenance_event_log(data_dir: &Path) -> Result<()> {
    // ubs:ignore[rust.security.path-traversal] — joins compile-time MAINTENANCE_EVENTS_FILE constant
    let path = data_dir.join(MAINTENANCE_EVENTS_FILE);
    let contents = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(e).with_context(|| {
                format!("reading event log for truncation at {}", path.display())
            });
        }
    };
    let lines: Vec<&str> = contents.lines().collect();
    if lines.len() <= MAX_EVENT_LOG_ENTRIES {
        return Ok(());
    }
    let keep = &lines[lines.len() - MAX_EVENT_LOG_ENTRIES..];
    let mut output = keep.join("\n");
    output.push('\n');
    std::fs::write(&path, output)
        .with_context(|| format!("truncating event log at {}", path.display()))
}

// ---------------------------------------------------------------------------
// Yield/pause signaling
// ---------------------------------------------------------------------------

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct YieldRequest {
    pub requester_pid: u32,
    pub requested_at_ms: i64,
    pub reason: String,
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn request_yield(data_dir: &Path, reason: &str) -> Result<()> {
    // ubs:ignore[rust.security.path-traversal] — joins compile-time YIELD_SIGNAL_FILE constant
    let path = data_dir.join(YIELD_SIGNAL_FILE);
    let now_ms = crate::storage::sqlite::FrankenStorage::now_millis();
    let req = YieldRequest {
        requester_pid: std::process::id(),
        requested_at_ms: now_ms,
        reason: reason.to_string(),
    };
    let payload = serde_json::to_string(&req).with_context(|| "serializing yield request")?;
    std::fs::write(&path, payload)
        .with_context(|| format!("writing yield signal to {}", path.display()))
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn check_yield_requested(data_dir: &Path) -> Option<YieldRequest> {
    // ubs:ignore[rust.security.path-traversal] — joins compile-time YIELD_SIGNAL_FILE constant
    let path = data_dir.join(YIELD_SIGNAL_FILE);
    let contents = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str::<YieldRequest>(&contents).ok()
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn clear_yield_signal(data_dir: &Path) -> Result<()> {
    // ubs:ignore[rust.security.path-traversal] — joins compile-time YIELD_SIGNAL_FILE constant
    let path = data_dir.join(YIELD_SIGNAL_FILE);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("clearing yield signal at {}", path.display())),
    }
}

// ---------------------------------------------------------------------------
// Unified maintenance view
// ---------------------------------------------------------------------------

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, serde::Serialize)]
pub struct UnifiedMaintenanceView {
    pub coordination: MaintenanceCoordinationOutcome,
    pub snapshot: SearchMaintenanceSnapshot,
    pub yield_pending: Option<YieldRequest>,
    pub recent_events: Vec<MaintenanceEvent>,
    pub decision: MaintenanceDecision,
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn unified_maintenance_view(
    data_dir: &Path,
    lexical_available: bool,
) -> UnifiedMaintenanceView {
    let now_ms = crate::storage::sqlite::FrankenStorage::now_millis();
    let snapshot = read_search_maintenance_snapshot(data_dir);
    let coordination = evaluate_maintenance_coordination_from_snapshot(&snapshot, now_ms);
    let yield_pending = check_yield_requested(data_dir);
    let recent_events = read_maintenance_events(data_dir, None, Some(20));
    let decision = if lexical_available {
        match &coordination {
            MaintenanceCoordinationOutcome::Active {
                job_id,
                job_kind,
                phase,
                ..
            } => MaintenanceDecision::FailOpen {
                reason: format!(
                    "maintenance job {} ({:?}) is active (phase: {}); lexical available, failing open",
                    job_id,
                    job_kind,
                    phase.as_deref().unwrap_or("unknown")
                ),
            },
            MaintenanceCoordinationOutcome::Stale { job_id, reason } => {
                MaintenanceDecision::FailOpen {
                    reason: format!(
                        "maintenance job {job_id} has a stale heartbeat ({reason}); lexical available, failing open"
                    ),
                }
            }
            _ => decide_maintenance_action_from_snapshot(&snapshot, now_ms),
        }
    } else {
        decide_maintenance_action_from_snapshot(&snapshot, now_ms)
    };

    UnifiedMaintenanceView {
        coordination,
        snapshot,
        yield_pending,
        recent_events,
        decision,
    }
}
