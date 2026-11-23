use std::path::PathBuf;

use crate::{CliError, CliResult, RobotFormat, default_data_dir};

use super::{io_error, now_unix_ms, print_json};

/// Quarantine interrupted `raw_mirror_capture` staging artifacts.
///
/// Empty/partial `raw-mirror/v1/tmp/capture.*` staging dirs from killed index
/// runs otherwise block doctor mutation behind "interrupted doctor artifact(s)
/// require inspection", forcing a manual `rm` inside cass's own data dir. This
/// moves them into `<data_dir>/doctor/quarantine/interrupted-artifacts/`
/// (renamed, never deleted — cass never deletes; the operator owns final
/// reclamation), clearing the gate.
pub fn run_doctor_cleanup_interrupted_artifacts(
    data_dir_override: Option<PathBuf>,
    yes: bool,
    structured_format: Option<RobotFormat>,
) -> CliResult<()> {
    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    let tmp_root = data_dir.join("raw-mirror").join("v1").join("tmp");

    let quarantine_root = data_dir
        .join("doctor")
        .join("quarantine")
        .join("interrupted-artifacts");

    // Enumerate the interrupted capture staging entries (top-level children of
    // the raw-mirror tmp dir). These are the `capture.*` dirs the doctor gate
    // flags as needs-inspection.
    let mut candidates: Vec<PathBuf> = Vec::new();
    if tmp_root.exists() {
        let entries = std::fs::read_dir(&tmp_root).map_err(|e| {
            io_error(
                format!(
                    "reading interrupted-capture staging dir {}: {e}",
                    tmp_root.display()
                ),
                None,
            )
        })?;
        for entry in entries {
            let entry = entry.map_err(|e| {
                io_error(format!("enumerating interrupted-capture entry: {e}"), None)
            })?;
            candidates.push(entry.path());
        }
    }
    candidates.sort();

    if candidates.is_empty() {
        let envelope = serde_json::json!({
            "schema_version": 1,
            "doctor_contract_version": 1,
            "kind": "cleanup_interrupted_artifacts",
            "data_dir": data_dir.display().to_string(),
            "tmp_root": tmp_root.display().to_string(),
            "quarantined_count": 0,
            "quarantined": [],
            "note": "No interrupted raw_mirror_capture artifacts found; doctor mutation is not blocked by this class.",
        });
        if structured_format.is_some() {
            print_json(&envelope)?;
        } else {
            println!("No interrupted raw_mirror_capture artifacts found.");
        }
        return Ok(());
    }

    if !yes {
        return Err(CliError {
            code: 4,
            kind: "refused-unsafe",
            message: format!(
                "found {} interrupted raw_mirror_capture artifact(s); `--cleanup-interrupted-artifacts` requires `--yes` to quarantine them",
                candidates.len()
            ),
            hint: Some(format!(
                "Inspect them under {} first, then re-run with `--cleanup-interrupted-artifacts --yes`. They are renamed into a quarantine dir, never deleted.",
                tmp_root.display()
            )),
            retryable: false,
        });
    }

    std::fs::create_dir_all(&quarantine_root).map_err(|e| {
        io_error(
            format!("creating quarantine dir {}: {e}", quarantine_root.display()),
            None,
        )
    })?;

    let mut quarantined: Vec<String> = Vec::new();
    for src in &candidates {
        let name = src
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| format!("artifact-{}", now_unix_ms()));
        let dst = quarantine_root.join(&name);
        let final_dst = if dst.exists() {
            quarantine_root.join(format!("{name}.{}", now_unix_ms()))
        } else {
            dst
        };
        std::fs::rename(src, &final_dst).map_err(|e| {
            io_error(
                format!(
                    "quarantining interrupted artifact {} → {}: {e}",
                    src.display(),
                    final_dst.display()
                ),
                Some("The cleanup halted at this artifact; inspect it manually."),
            )
        })?;
        quarantined.push(final_dst.display().to_string());
    }

    let envelope = serde_json::json!({
        "schema_version": 1,
        "doctor_contract_version": 1,
        "kind": "cleanup_interrupted_artifacts",
        "data_dir": data_dir.display().to_string(),
        "tmp_root": tmp_root.display().to_string(),
        "quarantine_root": quarantine_root.display().to_string(),
        "quarantined_count": quarantined.len(),
        "quarantined": quarantined,
        "note": "Interrupted raw_mirror_capture artifacts were renamed into quarantine; cass never deletes. This clears the 'interrupted doctor artifact(s) require inspection' mutation gate.",
    });

    if structured_format.is_some() {
        print_json(&envelope)?;
    } else {
        println!(
            "Quarantined {} interrupted raw_mirror_capture artifact(s) into {}",
            quarantined.len(),
            quarantine_root.display()
        );
    }
    Ok(())
}
