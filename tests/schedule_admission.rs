//! Real CLI coverage for whole-job scheduler admission. No live scheduler is
//! installed, no network source is contacted, and refused runs must never
//! reach the archive. Legacy `sources.toml` knobs (`type = "ssh"`,
//! `sync_schedule`) and the sync ledger are not scheduler inputs: the
//! scheduled jobs index the local archive only, so none of them is read,
//! executed, or created by a run.

use serde_json::Value;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

struct Fixture {
    _temporary: tempfile::TempDir,
    home: PathBuf,
    data: PathBuf,
}

impl Fixture {
    /// A hermetic HOME/data dir with the given `sources.toml` body and an
    /// optional pre-existing `sync_status.json` ledger. `seed_session` plants
    /// one real Claude Code conversation so the nightly `index --full` has a
    /// non-empty corpus to publish (it refuses to publish an empty archive).
    fn new(config: &str, ledger: Option<&str>, seed_session: bool) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let home = temporary.path().join("home");
        let data = temporary.path().join("data");
        fs::create_dir_all(home.join(".config/cass")).unwrap();
        fs::create_dir_all(&data).unwrap();
        // Stop dotenvy's ancestor search without touching any real .env file.
        File::create_new(home.join(".env")).unwrap();
        fs::write(home.join(".config/cass/sources.toml"), config).unwrap();
        if let Some(ledger) = ledger {
            fs::write(data.join("sync_status.json"), ledger).unwrap();
        }
        if seed_session {
            let src = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests")
                .join("fixtures")
                .join("claude_code_real")
                .join("projects")
                .join("-test-project")
                .join("agent-test123.jsonl");
            let dst = home
                .join(".claude")
                .join("projects")
                .join("-test-project")
                .join("agent-test123.jsonl");
            fs::create_dir_all(dst.parent().unwrap()).unwrap();
            fs::copy(src, dst).unwrap();
        }
        Self {
            _temporary: temporary,
            home,
            data,
        }
    }

    fn run(&self, job: &str) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_cass"));
        command.env_clear();
        for key in ["PATH", "SystemRoot", "WINDIR"] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        command
            .current_dir(&self.home)
            .env("HOME", &self.home)
            .env("USERPROFILE", &self.home)
            .env("XDG_CONFIG_HOME", self.home.join(".config"))
            .env("XDG_DATA_HOME", self.home.join(".local/share"))
            .env("CLAUDE_CONFIG_DIR", self.home.join(".claude"))
            .env("CODEX_HOME", self.home.join(".codex"))
            .env("RUST_MIN_STACK", "134217728")
            .env("TUI_HEADLESS", "1")
            .env("CASS_RESPONSIVENESS_DISABLE", "1")
            .env("CASS_AUTO_REFRESH", "0")
            .args([
                "schedule",
                "run",
                "--job",
                job,
                "--force",
                "--json",
                "--data-dir",
            ])
            .arg(&self.data)
            .output()
            .expect("execute the actual scheduled-run admission path")
    }

    /// A refused run spawns no children at all: nothing may even create the
    /// archive or its journal sidecars.
    fn assert_no_archive_started(&self) {
        for suffix in ["", "-wal", "-shm", "-journal"] {
            assert!(
                !self.data.join(format!("agent_search.db{suffix}")).exists(),
                "refused run started work on the archive (agent_search.db{suffix})"
            );
        }
    }
}

fn report_of(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("exactly one JSON report")
}

fn step_names(report: &Value) -> Vec<&str> {
    report["steps"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|step| step["name"].as_str())
        .collect()
}

fn refused(output: &Output, job: &str, step: &str, reason: &str) -> Value {
    assert_eq!(
        output.status.code(),
        Some(9),
        "scheduler failure must reach the caller"
    );
    let report = report_of(output);
    assert_eq!(report["job"], job);
    assert_eq!(report["ok"], false);
    let steps = report["steps"].as_array().unwrap();
    assert_eq!(steps.len(), 1, "no dependent process may start");
    assert_eq!(steps[0]["name"], step);
    assert_eq!(steps[0]["ok"], false);
    assert!(
        steps[0]["exit_code"].is_null(),
        "there is no child exit status"
    );
    assert!(steps[0]["argv"].as_array().unwrap().is_empty());
    assert_eq!(steps[0]["result"]["reason"], reason);
    assert_eq!(steps[0]["result"]["dependent_work_started"], false);
    for bytes in [&output.stdout, &output.stderr] {
        assert!(!String::from_utf8_lossy(bytes).contains("private-"));
    }
    report
}

#[test]
fn legacy_sync_schedule_and_ledger_are_never_consulted() {
    // `type = "ssh"` + `sync_schedule = "hourly"` plus a corrupt sync ledger:
    // the scheduler performs no remote sync, so both forced jobs run only
    // their local index (nightly also its semantic backfill), spawn no
    // `sources-*` step, and neither read nor write `sync_status.json`.
    let config = "[[sources]]\nname = 'fixture'\ntype = 'ssh'\nhost = 'fixture.invalid'\npaths = ['~/.claude/projects']\nsync_schedule = 'hourly'\n";
    let ledger = "{\"private-ledger-sentinel\":";
    let fixture = Fixture::new(config, Some(ledger), true);
    for job in ["incremental", "nightly"] {
        let output = fixture.run(job);
        assert!(
            output.status.success(),
            "{job}: local indexing must run: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let report = report_of(&output);
        assert_eq!(report["ok"], true, "{job}: {report}");
        let names = step_names(&report);
        let index = if job == "incremental" {
            "index"
        } else {
            "index-full"
        };
        assert!(names.contains(&index), "{job} steps: {names:?}");
        assert!(
            !names.iter().any(|name| name.starts_with("sources-")),
            "{job} must not plan source work: {names:?}"
        );
        if job == "nightly" {
            assert!(
                names
                    .iter()
                    .any(|name| name.starts_with("semantic-backfill")),
                "nightly still runs its semantic backfill: {names:?}"
            );
        }
        // The ledger is not a scheduler input: bytes are byte-identical and
        // no sibling artifacts appeared.
        assert_eq!(
            fs::read_to_string(fixture.data.join("sync_status.json")).unwrap(),
            ledger
        );
        assert_eq!(
            fs::read_to_string(fixture.home.join(".config/cass/sources.toml")).unwrap(),
            config
        );
        for bytes in [&output.stdout, &output.stderr] {
            assert!(!String::from_utf8_lossy(bytes).contains("private-"));
        }
    }
    // A run over a fixture with no ledger at all never creates one either.
    let fixture = Fixture::new(config, None, true);
    let output = fixture.run("incremental");
    assert!(output.status.success());
    assert!(!fixture.data.join("sync_status.json").exists());
}

#[test]
fn malformed_configuration_is_not_a_schedule_input() {
    // The scheduler never reads sources.toml for planning: a malformed file
    // cannot refuse or delay local indexing, and its content never leaks.
    let fixture = Fixture::new(
        "private-configuration-sentinel = [unterminated",
        None,
        false,
    );
    for job in ["incremental", "nightly"] {
        let output = fixture.run(job);
        let report = report_of(&output);
        let names = step_names(&report);
        let index = if job == "incremental" {
            "index"
        } else {
            "index-full"
        };
        assert!(
            names.contains(&index),
            "{job} must reach indexing, not a planning refusal: {names:?}"
        );
        assert!(
            !names.iter().any(|name| name.starts_with("sources-")),
            "{job} must not plan source work: {names:?}"
        );
        assert!(!fixture.data.join("sync_status.json").exists());
        for bytes in [&output.stdout, &output.stderr] {
            assert!(!String::from_utf8_lossy(bytes).contains("private-"));
        }
        assert_eq!(
            fs::read_to_string(fixture.home.join(".config/cass/sources.toml")).unwrap(),
            "private-configuration-sentinel = [unterminated"
        );
    }
}

#[test]
fn both_forced_jobs_respect_a_cross_process_lease_and_preserve_owner_receipts() {
    let fixture = Fixture::new(
        "private-configuration-sentinel = [unterminated",
        None,
        false,
    );
    let schedule = fixture.data.join("schedule");
    fs::create_dir(&schedule).unwrap();
    let state = br#"{"last_incremental":null,"last_nightly":null}"#;
    let history = b"private-owner-history-sentinel\n";
    fs::write(schedule.join("state.json"), state).unwrap();
    fs::write(schedule.join("runs.jsonl"), history).unwrap();
    let lock = File::create_new(schedule.join("run.lock")).unwrap();
    lock.try_lock().unwrap();
    for job in ["incremental", "nightly"] {
        let report = refused(
            &fixture.run(job),
            job,
            "schedule-admission",
            "schedule_busy",
        );
        assert_eq!(report["steps"][0]["result"]["status"], "deferred");
        assert_eq!(report["steps"][0]["result"]["persisted"], false);
        fixture.assert_no_archive_started();
        assert_eq!(fs::read(schedule.join("state.json")).unwrap(), state);
        assert_eq!(fs::read(schedule.join("runs.jsonl")).unwrap(), history);
        assert!(!schedule.join(format!("{job}.log")).exists());
        assert!(!schedule.join("state.json.tmp").exists());
    }
    // A later invocation can acquire the same inode and reaches indexing; no
    // stale-lock cleanup or force override is needed.
    drop(lock);
    let output = fixture.run("incremental");
    let report = report_of(&output);
    assert!(
        step_names(&report).contains(&"index"),
        "released lease must admit the job and reach the index step: {report}"
    );
    assert!(schedule.join("run.lock").is_file());
}

#[test]
fn invalid_lock_control_is_a_visible_failure_not_an_unlocked_run() {
    let fixture = Fixture::new(
        "private-configuration-sentinel = [unterminated",
        None,
        false,
    );
    let schedule = fixture.data.join("schedule");
    fs::create_dir_all(schedule.join("run.lock")).unwrap();
    for job in ["incremental", "nightly"] {
        let report = refused(
            &fixture.run(job),
            job,
            "schedule-admission",
            "schedule_lock_unavailable",
        );
        assert_eq!(report["steps"][0]["result"]["persisted"], false);
        assert_eq!(report["steps"][0]["result"]["status"], "failed");
        fixture.assert_no_archive_started();
        assert!(schedule.join("run.lock").is_dir());
        assert!(!schedule.join("state.json").exists());
        assert!(!schedule.join("runs.jsonl").exists());
    }
}
