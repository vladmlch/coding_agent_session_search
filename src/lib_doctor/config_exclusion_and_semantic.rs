#[allow(unused_imports)]
use crate::franken_sync::compat::{
    ConnectionExt, OpenFlags as FrankenOpenFlags, RowExt,
    open_with_flags as open_franken_with_flags,
};
#[allow(unused_imports)]
use crate::indexer::IndexOptions;
#[allow(unused_imports)]
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
#[allow(unused_imports)]
use crate::robot_budget_envelope::BudgetSnapshot;
#[allow(unused_imports)]
use crate::{cli_app::*, cli_root::*, lib_doctor::*, *};
#[allow(unused_imports)]
use anyhow::Result;
#[allow(unused_imports)]
use base64::prelude::*;
#[allow(unused_imports)]
use chrono::Utc;
#[allow(unused_imports)]
use clap::{Arg, ArgAction, Command, CommandFactory, Parser, Subcommand, ValueEnum, ValueHint};
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
#[allow(unused_imports)]
use std::fs::{File, OpenOptions};
#[allow(unused_imports)]
use std::io::{self, BufRead, BufReader, IsTerminal, Read, Seek, SeekFrom, Write};
#[allow(unused_imports)]
use std::path::{Path, PathBuf};
#[allow(unused_imports)]
use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
use std::time::{Duration, Instant};
#[allow(unused_imports)]
use tracing::{info, warn};

pub(crate) fn doctor_config_exclusion_targets(
    data_dir: &Path,
    db_path: &Path,
) -> Vec<DoctorConfigExclusionTarget> {
    vec![
        DoctorConfigExclusionTarget {
            path: data_dir.to_path_buf(),
            asset_class: DoctorAssetClass::CanonicalArchiveDb,
            label: "cass data directory",
            tokens: &["cass", "cass-data", "data_dir", "data-dir"],
        },
        DoctorConfigExclusionTarget {
            path: db_path.to_path_buf(),
            asset_class: DoctorAssetClass::CanonicalArchiveDb,
            label: "canonical archive database",
            tokens: &["agent_search.db", "archive", "database"],
        },
        DoctorConfigExclusionTarget {
            path: doctor_raw_mirror_root(data_dir),
            asset_class: DoctorAssetClass::RawMirrorBlob,
            label: "raw mirror evidence",
            tokens: &["raw-mirror", "raw_mirror", "raw mirror"],
        },
        DoctorConfigExclusionTarget {
            path: data_dir.join("backups"),
            asset_class: DoctorAssetClass::BackupBundle,
            label: "doctor backups",
            tokens: &["backup", "backups"],
        },
        DoctorConfigExclusionTarget {
            path: data_dir.join("doctor").join("receipts"),
            asset_class: DoctorAssetClass::OperationReceipt,
            label: "doctor receipts",
            tokens: &["receipt", "receipts"],
        },
        DoctorConfigExclusionTarget {
            path: data_dir.join("doctor").join("forensic-bundles"),
            asset_class: DoctorAssetClass::ForensicBundle,
            label: "forensic bundles",
            tokens: &["forensic-bundle", "forensic_bundles", "forensic"],
        },
        DoctorConfigExclusionTarget {
            path: data_dir.join("doctor").join("support-bundles"),
            asset_class: DoctorAssetClass::SupportBundle,
            label: "support bundles",
            tokens: &["support-bundle", "support_bundles", "support"],
        },
    ]
}

/// `path` with `/` separators, `.` and `..` dropped. A Windows prefix is kept
/// (`C:`, `//server/share`; the verbatim forms render like the plain ones):
/// this string is the data dir in the archive-normalize fingerprint and in
/// the apply command doctor prints, and without the drive `C:\x` and `D:\x`
/// were one identity and `--data-dir /x` named the current drive (dddwv).
pub(crate) fn doctor_path_to_slash_string(path: &Path) -> String {
    use std::path::{Component, Prefix};

    let mut rendered = String::new();
    let mut needs_separator = false;
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => {
                match prefix.kind() {
                    Prefix::Disk(drive) | Prefix::VerbatimDisk(drive) => {
                        rendered.push(char::from(drive.to_ascii_uppercase()));
                        rendered.push(':');
                    }
                    Prefix::UNC(server, share) | Prefix::VerbatimUNC(server, share) => {
                        rendered.push_str("//");
                        rendered.push_str(&server.to_string_lossy());
                        rendered.push('/');
                        rendered.push_str(&share.to_string_lossy());
                    }
                    Prefix::Verbatim(name) => {
                        rendered.push_str("//?/");
                        rendered.push_str(&name.to_string_lossy());
                    }
                    Prefix::DeviceNS(name) => {
                        rendered.push_str("//./");
                        rendered.push_str(&name.to_string_lossy());
                    }
                }
                needs_separator = false;
            }
            Component::RootDir => {
                rendered.push('/');
                needs_separator = false;
            }
            Component::Normal(part) => {
                if needs_separator {
                    rendered.push('/');
                }
                rendered.push_str(&part.to_string_lossy());
                needs_separator = true;
            }
            Component::CurDir | Component::ParentDir => {}
        }
    }
    rendered
}

#[cfg(test)]
mod doctor_path_slash_string_tests {
    use super::{doctor_path_identity_for_fingerprint, doctor_path_to_slash_string};
    use std::path::Path;

    #[test]
    fn rooted_and_relative_paths_render_with_single_slashes() {
        assert_eq!(
            doctor_path_to_slash_string(Path::new("/home/x/data")),
            "/home/x/data"
        );
        assert_eq!(doctor_path_to_slash_string(Path::new("/")), "/");
        assert_eq!(
            doctor_path_to_slash_string(Path::new("database/candidate.db")),
            "database/candidate.db"
        );
        assert_eq!(doctor_path_to_slash_string(Path::new("./a/../b")), "a/b");
        assert_eq!(doctor_path_to_slash_string(Path::new("")), "");
    }

    /// The fingerprint identity is also the `--data-dir` of the apply command
    /// doctor prints, so it has to name the same directory again.
    #[test]
    fn fingerprint_identity_names_the_same_directory() {
        let temp = tempfile::tempdir().expect("tempdir");
        let identity = doctor_path_identity_for_fingerprint(temp.path());
        assert_eq!(
            std::fs::canonicalize(Path::new(&identity)).expect("identity resolves"),
            std::fs::canonicalize(temp.path()).expect("temp dir resolves"),
            "{identity}"
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_paths_keep_their_drive_or_share() {
        assert_eq!(
            doctor_path_to_slash_string(Path::new(r"C:\Users\x\data")),
            "C:/Users/x/data"
        );
        assert_eq!(
            doctor_path_to_slash_string(Path::new(r"\\?\c:\Users\x\data")),
            "C:/Users/x/data"
        );
        assert_ne!(
            doctor_path_to_slash_string(Path::new(r"C:\x")),
            doctor_path_to_slash_string(Path::new(r"D:\x"))
        );
        assert_eq!(
            doctor_path_to_slash_string(Path::new(r"\\server\share\dir")),
            "//server/share/dir"
        );
        assert_eq!(
            doctor_path_to_slash_string(Path::new(r"\\?\UNC\server\share\dir")),
            "//server/share/dir"
        );
        assert_eq!(
            doctor_path_to_slash_string(Path::new(r"C:rel\x")),
            "C:rel/x"
        );
    }
}

pub(crate) fn doctor_pattern_matches_relative_path(
    raw_pattern: &str,
    relative_path: &Path,
) -> bool {
    let mut pattern = raw_pattern.trim();
    if pattern.is_empty() || pattern.starts_with('#') {
        return false;
    }
    if let Some(stripped) = pattern.strip_prefix('!') {
        pattern = stripped.trim();
    }
    if let Some(stripped) = pattern.strip_prefix("- ") {
        pattern = stripped.trim();
    }
    if let Some(stripped) = pattern.strip_prefix("+ ") {
        pattern = stripped.trim();
    }
    if let Some(stripped) = pattern.strip_prefix("exclude ") {
        pattern = stripped.trim();
    }
    if let Some(stripped) = pattern.strip_prefix("--exclude=") {
        pattern = stripped.trim();
    }
    let pattern = pattern.trim_matches('"').trim_matches('\'').trim();
    if pattern.is_empty() {
        return false;
    }

    let anchored = pattern.starts_with('/');
    let directory_pattern = pattern.ends_with('/');
    let pattern = pattern.trim_start_matches('/').trim_end_matches('/');
    let relative = doctor_path_to_slash_string(relative_path);
    let relative = relative.trim_start_matches("./");
    if relative.is_empty() {
        return false;
    }
    if let Some(prefix) = pattern.strip_suffix("/**")
        && (relative == prefix || relative.starts_with(&format!("{prefix}/")))
    {
        return true;
    }
    if pattern == relative || relative.starts_with(&format!("{pattern}/")) {
        return true;
    }

    let mut candidates = vec![pattern.to_string()];
    if directory_pattern {
        candidates.push(format!("{pattern}/**"));
    }
    if !anchored && !pattern.contains('/') {
        candidates.push(format!("**/{pattern}"));
        candidates.push(format!("**/{pattern}/**"));
    }
    candidates.iter().any(|candidate| {
        glob::Pattern::new(candidate)
            .map(|compiled| compiled.matches(relative))
            .unwrap_or(false)
    })
}

pub(crate) fn doctor_parse_exclusion_pattern_line(line: &str) -> Option<(String, bool)> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }
    if let Some(rest) = trimmed.strip_prefix('!') {
        return Some((rest.trim().to_string(), true));
    }
    if let Some(rest) = trimmed.strip_prefix("+ ") {
        return Some((rest.trim().to_string(), true));
    }
    if let Some(rest) = trimmed.strip_prefix("- ") {
        return Some((rest.trim().to_string(), false));
    }
    if let Some(rest) = trimmed.strip_prefix("exclude ") {
        return Some((rest.trim().to_string(), false));
    }
    if let Some(rest) = trimmed.strip_prefix("--exclude=") {
        return Some((rest.trim().to_string(), false));
    }
    Some((trimmed.to_string(), false))
}

pub(crate) fn doctor_last_matching_exclusion_in_file(
    config_path: &Path,
    base_dir: &Path,
    target_path: &Path,
) -> Option<DoctorConfigPatternMatch> {
    let relative_path = target_path.strip_prefix(base_dir).ok()?;
    let content = std::fs::read_to_string(config_path).ok()?;
    let mut last_match = None;
    for (line_index, line) in content.lines().enumerate() {
        let Some((pattern, include)) = doctor_parse_exclusion_pattern_line(line) else {
            continue;
        };
        if doctor_pattern_matches_relative_path(&pattern, relative_path) {
            last_match = Some(DoctorConfigPatternMatch {
                config_path: config_path.to_path_buf(),
                line_number: line_index + 1,
                pattern,
                include,
            });
        }
    }
    last_match
}

pub(crate) fn doctor_existing_or_parent(path: &Path) -> PathBuf {
    for ancestor in path.ancestors() {
        if ancestor.exists() {
            return ancestor.to_path_buf();
        }
    }
    path.parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf()
}

pub(crate) fn doctor_repo_root_for_path(path: &Path) -> Option<PathBuf> {
    let start = doctor_existing_or_parent(path);
    let start = if start.is_file() {
        start.parent().unwrap_or(&start).to_path_buf()
    } else {
        start
    };
    start
        .ancestors()
        .find(|ancestor| ancestor.join(".git").exists())
        .map(Path::to_path_buf)
}

pub(crate) fn doctor_gitignore_files_between(repo_root: &Path, target_path: &Path) -> Vec<PathBuf> {
    let target_parent = target_path.parent().unwrap_or(target_path);
    let Ok(relative_parent) = target_parent.strip_prefix(repo_root) else {
        return Vec::new();
    };
    let mut dirs = vec![repo_root.to_path_buf()];
    let mut current = repo_root.to_path_buf();
    for component in relative_parent.components() {
        if let std::path::Component::Normal(part) = component {
            current.push(part);
            dirs.push(current.clone());
        }
    }
    dirs.into_iter()
        .map(|dir| dir.join(".gitignore"))
        .filter(|path| path.exists())
        .collect()
}

pub(crate) fn doctor_repo_gitignore_exclusion(
    target_path: &Path,
) -> Option<DoctorConfigPatternMatch> {
    let repo_root = doctor_repo_root_for_path(target_path)?;
    let mut last_match = None;
    for config_path in doctor_gitignore_files_between(&repo_root, target_path) {
        let base_dir = config_path.parent().unwrap_or(&repo_root);
        if let Some(pattern_match) =
            doctor_last_matching_exclusion_in_file(&config_path, base_dir, target_path)
        {
            last_match = Some(pattern_match);
        }
    }
    last_match.filter(|pattern_match| !pattern_match.include)
}

pub(crate) fn doctor_local_exclusion_config_files(data_dir: &Path) -> Vec<PathBuf> {
    [
        ".gitignore",
        ".ignore",
        ".backupignore",
        ".rsync-filter",
        "backup-excludes.txt",
        "restic-excludes.txt",
        "borg-excludes.txt",
        "rclone-excludes.txt",
    ]
    .into_iter()
    .map(|name| data_dir.join(name))
    .filter(|path| path.exists())
    .collect()
}

pub(crate) fn doctor_path_has_cache_component(path: &Path) -> bool {
    path.components().any(|component| {
        let std::path::Component::Normal(part) = component else {
            return false;
        };
        let part = part.to_string_lossy().to_ascii_lowercase();
        part == ".cache"
            || part == "cache"
            || part == "caches"
            || part == "temporaryitems"
            || part == "temporary items"
    })
}

pub(crate) fn doctor_config_target_recommended_action(
    target: &DoctorConfigExclusionTarget,
) -> String {
    format!(
        "Review backup, sync, and ignore configuration before trusting archival coverage for {}; remove broad exclusions or add a later include/allow rule. cass will not edit these files automatically.",
        target.label
    )
}

pub(crate) fn doctor_push_config_exclusion_risk(
    risks: &mut Vec<DoctorConfigExclusionRiskReport>,
    seen: &mut HashSet<(String, String, String)>,
    target: &DoctorConfigExclusionTarget,
    risk_kind: &str,
    evidence: Vec<String>,
    confidence: &str,
) {
    let path = target.path.display().to_string();
    let evidence_key = evidence.join("\n");
    let key = (path.clone(), risk_kind.to_string(), evidence_key);
    if !seen.insert(key) {
        return;
    }
    risks.push(DoctorConfigExclusionRiskReport {
        schema_version: 1,
        path,
        risk_kind: risk_kind.to_string(),
        evidence,
        confidence: confidence.to_string(),
        recommended_action: doctor_config_target_recommended_action(target),
        auto_fix_available: false,
    });
}

pub(crate) fn doctor_scan_text_config_for_exclusion_mentions(
    risks: &mut Vec<DoctorConfigExclusionRiskReport>,
    seen: &mut HashSet<(String, String, String)>,
    config_path: &Path,
    risk_kind: &str,
    targets: &[DoctorConfigExclusionTarget],
) {
    let Ok(content) = std::fs::read_to_string(config_path) else {
        return;
    };
    for (line_index, line) in content.lines().enumerate() {
        let lower = line.to_ascii_lowercase();
        if !(lower.contains("exclude")
            || lower.contains("ignore")
            || lower.contains("filter")
            || lower.contains("prune")
            || lower.contains("skip"))
        {
            continue;
        }
        for target in targets {
            let target_basename = target
                .path
                .file_name()
                .map(|name| name.to_string_lossy().to_ascii_lowercase())
                .unwrap_or_default();
            // Without its Windows prefix: a config line that names the path
            // with or without the drive still mentions it.
            let target_path = doctor_path_to_slash_string(
                &target
                    .path
                    .components()
                    .filter(|component| !matches!(component, std::path::Component::Prefix(_)))
                    .collect::<PathBuf>(),
            )
            .to_ascii_lowercase();
            let target_mentioned = target.tokens.iter().any(|token| lower.contains(token))
                || (!target_basename.is_empty() && lower.contains(&target_basename))
                || lower.contains(&target_path);
            if target_mentioned {
                doctor_push_config_exclusion_risk(
                    risks,
                    seen,
                    target,
                    risk_kind,
                    vec![format!(
                        "{}:{} mentions exclusion-like policy for {}: {}",
                        config_path.display(),
                        line_index + 1,
                        target.label,
                        line.trim()
                    )],
                    "medium",
                );
            }
        }
    }
}

pub(crate) fn collect_doctor_config_exclusion_risks(
    data_dir: &Path,
    db_path: &Path,
    config_path: &Path,
    sources_path: &Path,
) -> Vec<DoctorConfigExclusionRiskReport> {
    let targets = doctor_config_exclusion_targets(data_dir, db_path);
    let mut risks = Vec::new();
    let mut seen = HashSet::new();

    for target in &targets {
        if doctor_path_has_cache_component(&target.path) {
            doctor_push_config_exclusion_risk(
                &mut risks,
                &mut seen,
                target,
                "volatile-cache-location",
                vec![format!(
                    "{} is under a cache-like directory; cache cleaners and sync tools commonly exclude or prune this location",
                    target.path.display()
                )],
                "high",
            );
        }

        if let Some(pattern_match) = doctor_repo_gitignore_exclusion(&target.path) {
            doctor_push_config_exclusion_risk(
                &mut risks,
                &mut seen,
                target,
                "repo-ignore-excludes-cass-evidence",
                vec![format!(
                    "{}:{} pattern {:?} excludes {} ({})",
                    pattern_match.config_path.display(),
                    pattern_match.line_number,
                    pattern_match.pattern,
                    target.label,
                    doctor_asset_class_name(target.asset_class)
                )],
                "high",
            );
        }

        for config_file in doctor_local_exclusion_config_files(data_dir) {
            if let Some(pattern_match) =
                doctor_last_matching_exclusion_in_file(&config_file, data_dir, &target.path)
                && !pattern_match.include
            {
                doctor_push_config_exclusion_risk(
                    &mut risks,
                    &mut seen,
                    target,
                    "backup-filter-excludes-cass-evidence",
                    vec![format!(
                        "{}:{} pattern {:?} excludes {} ({})",
                        pattern_match.config_path.display(),
                        pattern_match.line_number,
                        pattern_match.pattern,
                        target.label,
                        doctor_asset_class_name(target.asset_class)
                    )],
                    "high",
                );
            }
        }
    }

    if config_path.exists() {
        doctor_scan_text_config_for_exclusion_mentions(
            &mut risks,
            &mut seen,
            config_path,
            "cass-config-excludes-cass-evidence",
            &targets,
        );
    }
    if sources_path.exists() {
        doctor_scan_text_config_for_exclusion_mentions(
            &mut risks,
            &mut seen,
            sources_path,
            "sources-config-excludes-cass-evidence",
            &targets,
        );
    }

    risks
}

pub(crate) fn doctor_fallback_mode_from_state(state: &serde_json::Value) -> String {
    state
        .pointer("/semantic/fallback_mode")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
            if state
                .pointer("/semantic/can_search")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
            {
                "none".to_string()
            } else {
                "unknown".to_string()
            }
        })
}

pub(crate) fn doctor_json_pointer_string(
    value: &serde_json::Value,
    pointer: &str,
    fallback: &str,
) -> String {
    value
        .pointer(pointer)
        .and_then(serde_json::Value::as_str)
        .unwrap_or(fallback)
        .to_string()
}

pub(crate) fn doctor_json_pointer_bool(value: &serde_json::Value, pointer: &str) -> Option<bool> {
    value.pointer(pointer).and_then(serde_json::Value::as_bool)
}

pub(crate) struct DoctorDerivedSemanticComponentInput {
    pub(crate) asset_class: DoctorAssetClass,
    pub(crate) status: String,
    pub(crate) path: Option<String>,
    pub(crate) current_db_matches: Option<bool>,
    pub(crate) ready: Option<bool>,
    pub(crate) present: Option<bool>,
    pub(crate) safe_to_rebuild: bool,
    pub(crate) safe_for_auto_repair: bool,
    pub(crate) note: String,
}

pub(crate) fn doctor_derived_semantic_component_report(
    data_dir: &Path,
    input: DoctorDerivedSemanticComponentInput,
) -> DoctorDerivedSemanticComponentReport {
    let DoctorDerivedSemanticComponentInput {
        asset_class,
        status,
        path,
        current_db_matches,
        ready,
        present,
        safe_to_rebuild,
        safe_for_auto_repair,
        note,
    } = input;
    let policy = doctor_asset_policy(asset_class);
    let redacted_path = path
        .as_deref()
        .map(|path| doctor_redacted_path(path, data_dir));
    DoctorDerivedSemanticComponentReport {
        asset_class,
        safety_classification: policy.safety_classification,
        status,
        safe_to_rebuild,
        safe_for_auto_repair,
        blocks_archive_recovery: false,
        network_allowed: false,
        auto_download_attempted: false,
        path,
        redacted_path,
        current_db_matches,
        ready,
        present,
        note,
    }
}

pub(crate) fn doctor_existing_quarantine_marker(path: Option<&str>) -> Option<String> {
    let path = path?;
    let marker = Path::new(path).join(".quarantined");
    marker.is_file().then(|| marker.display().to_string())
}

pub(crate) fn doctor_semantic_memo_quarantine_marker(data_dir: &Path) -> Option<String> {
    [
        "semantic-memo/.quarantined",
        "semantic/memo-cache/.quarantined",
        "memoization-cache/.quarantined",
        "vector_index/.memo-cache.quarantined",
    ]
    .into_iter()
    .map(|relative| data_dir.join(relative))
    .find(|path| path.is_file())
    .map(|path| path.display().to_string())
}

pub(crate) fn doctor_semantic_model_asset_class(availability: &str) -> DoctorAssetClass {
    match availability {
        "not_installed" | "disabled" | "model_missing" | "load_failed" => {
            DoctorAssetClass::ModelCache
        }
        _ => DoctorAssetClass::DerivedSemanticIndex,
    }
}

pub(crate) fn doctor_semantic_recommended_action(
    status: &str,
    availability: &str,
    model_quarantined: bool,
    memo_quarantined: bool,
    archive_db_usable: bool,
) -> String {
    if !archive_db_usable {
        return "repair canonical archive DB before rebuilding semantic derivatives".to_string();
    }
    if memo_quarantined {
        return "inspect memoization cache quarantine; rebuild derived memo cache from the canonical archive only after archive checks pass".to_string();
    }
    if model_quarantined || availability == "load_failed" {
        return "cass models verify --json; reinstall corrupt models with cass models install --from-file <dir>; lexical search remains usable".to_string();
    }
    match availability {
        "not_installed" | "model_missing" => {
            "cass models status --json; install explicitly with cass models install --from-file <dir>, then run cass index --semantic --json".to_string()
        }
        "index_missing" | "index_stale" | "index_building" => {
            "cass index --semantic --json, or keep using lexical search".to_string()
        }
        "disabled" => {
            "semantic search is disabled by policy; continue lexical search or re-enable semantic mode explicitly".to_string()
        }
        _ if status == "ready" || status == "hash_fallback" => "none".to_string(),
        _ => "cass models status --json; lexical search remains usable".to_string(),
    }
}

pub(crate) fn doctor_build_derived_semantic_asset_report(
    data_dir: &Path,
    state: &serde_json::Value,
    archive_db_usable: bool,
    probe_duration_ms: u64,
) -> DoctorDerivedSemanticAssetReport {
    let fallback_mode = doctor_fallback_mode_from_state(state);
    let semantic_status = doctor_json_pointer_string(state, "/semantic/status", "unknown");
    let semantic_availability =
        doctor_json_pointer_string(state, "/semantic/availability", "unknown");
    let can_search = doctor_json_pointer_bool(state, "/semantic/can_search").unwrap_or(false);
    let model_dir = state
        .pointer("/semantic/model_dir")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let vector_index_path = state
        .pointer("/semantic/vector_index_path")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let hnsw_path = state
        .pointer("/semantic/hnsw_path")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let hnsw_ready = doctor_json_pointer_bool(state, "/semantic/hnsw_ready").unwrap_or(false);
    let fast_present = doctor_json_pointer_bool(state, "/semantic/fast_tier/present");
    let fast_ready = doctor_json_pointer_bool(state, "/semantic/fast_tier/ready");
    let fast_matches = doctor_json_pointer_bool(state, "/semantic/fast_tier/current_db_matches");
    let quality_present = doctor_json_pointer_bool(state, "/semantic/quality_tier/present");
    let quality_ready = doctor_json_pointer_bool(state, "/semantic/quality_tier/ready");
    let quality_matches =
        doctor_json_pointer_bool(state, "/semantic/quality_tier/current_db_matches");
    let semantic_checkpoint_active =
        doctor_json_pointer_bool(state, "/semantic/checkpoint/active").unwrap_or(false);
    let model_quarantine_marker = doctor_existing_quarantine_marker(model_dir.as_deref());
    let memo_quarantine_marker = doctor_semantic_memo_quarantine_marker(data_dir);
    let model_quarantined = model_quarantine_marker.is_some();
    let memo_quarantined = memo_quarantine_marker.is_some();
    let policy_disabled = semantic_availability == "disabled" || semantic_status == "disabled";
    let not_initialized =
        semantic_availability == "not_initialized" || semantic_status == "not_initialized";
    let lexical_fallback_active = fallback_mode == "lexical" && !can_search;
    let semantic_ready = can_search && fallback_mode != "lexical";
    let safe_to_rebuild = archive_db_usable && !policy_disabled && !not_initialized;

    let mut asset_class = doctor_semantic_model_asset_class(&semantic_availability);
    if memo_quarantined {
        asset_class = DoctorAssetClass::MemoCache;
    } else if model_quarantined {
        asset_class = DoctorAssetClass::ModelCache;
    }
    let asset_policy = doctor_asset_policy(asset_class);

    let status = if !archive_db_usable {
        "skipped-archive-unavailable"
    } else if memo_quarantined {
        "memo-cache-quarantined"
    } else if model_quarantined {
        "model-cache-quarantined"
    } else if semantic_ready {
        "ready"
    } else if policy_disabled {
        "policy-disabled"
    } else if not_initialized {
        "not-initialized"
    } else if lexical_fallback_active {
        "lexical-fallback"
    } else {
        "semantic-assets-unavailable"
    }
    .to_string();
    let doctor_check_status = match status.as_str() {
        "ready" | "policy-disabled" | "not-initialized" | "skipped-archive-unavailable" => "pass",
        _ => "warn",
    }
    .to_string();
    let recommended_action = doctor_semantic_recommended_action(
        &semantic_status,
        &semantic_availability,
        model_quarantined,
        memo_quarantined,
        archive_db_usable,
    );
    let model_policy = if policy_disabled {
        "disabled-by-policy"
    } else if matches!(
        semantic_availability.as_str(),
        "not_installed" | "model_missing"
    ) {
        "explicit-install-required-no-auto-download"
    } else if model_quarantined || semantic_availability == "load_failed" {
        "explicit-verify-or-reinstall-required-no-auto-download"
    } else {
        "no-auto-download"
    }
    .to_string();
    let selected_repair_action = if status == "ready" || policy_disabled || not_initialized {
        "none"
    } else if memo_quarantined {
        "manual-memo-cache-rebuild-after-archive-check"
    } else if model_quarantined || semantic_availability == "load_failed" {
        "manual-model-cache-verify-or-reinstall"
    } else if safe_to_rebuild && matches!(asset_class, DoctorAssetClass::DerivedSemanticIndex) {
        "manual-cass-index-semantic"
    } else {
        "none-no-auto-download"
    }
    .to_string();
    let skipped_auto_download_reason = (!semantic_ready).then(|| {
        "doctor never acquires semantic models; model installation requires explicit cass models install --from-file".to_string()
    });

    let model_status = if !archive_db_usable {
        "skipped-archive-unavailable"
    } else if model_quarantined {
        "quarantined"
    } else {
        match semantic_availability.as_str() {
            "ready" | "hash_fallback" | "index_missing" | "index_building" | "index_stale" => {
                "available-or-not-required"
            }
            "disabled" => "policy-disabled",
            "not_installed" | "model_missing" => "missing-explicit-install-required",
            "load_failed" => "corrupt-or-unreadable",
            _ => "unknown",
        }
    };
    let vector_present = match (fast_present, quality_present) {
        (Some(fast), Some(quality)) => Some(fast || quality),
        (Some(fast), None) => Some(fast),
        (None, Some(quality)) => Some(quality),
        (None, None) => None,
    };
    let vector_ready = match (fast_ready, quality_ready) {
        (Some(fast), Some(quality)) => Some(fast || quality),
        (Some(fast), None) => Some(fast),
        (None, Some(quality)) => Some(quality),
        (None, None) => None,
    };
    let vector_matches = match (fast_matches, quality_matches) {
        (Some(fast), Some(quality)) => Some(fast || quality),
        (Some(fast), None) => Some(fast),
        (None, Some(quality)) => Some(quality),
        (None, None) => None,
    };
    let vector_status = if !archive_db_usable {
        "skipped-archive-unavailable"
    } else if semantic_ready {
        "ready"
    } else if semantic_checkpoint_active || semantic_availability == "index_building" {
        "building"
    } else if semantic_status == "stale" || semantic_availability == "index_stale" {
        "stale"
    } else if semantic_availability == "index_missing" {
        "missing"
    } else if lexical_fallback_active {
        "unavailable-lexical-fallback"
    } else {
        "not-ready"
    };
    let hnsw_status = if !archive_db_usable {
        "skipped-archive-unavailable"
    } else if hnsw_ready {
        "ready"
    } else {
        match state
            .pointer("/semantic/hnsw_state")
            .and_then(serde_json::Value::as_str)
        {
            Some("present_unverified") => "present-unverified",
            Some("absent") => "not-present",
            Some("inspection_failed") => "inspection-failed",
            Some("not_inspected") => "not-inspected",
            _ if hnsw_path.is_some() => "missing-or-unreadable",
            _ => "not-present",
        }
    };
    let memo_status = if !archive_db_usable {
        "skipped-archive-unavailable"
    } else if memo_quarantined {
        "quarantined"
    } else {
        "not-present-or-not-required"
    };

    let model_cache = doctor_derived_semantic_component_report(
        data_dir,
        DoctorDerivedSemanticComponentInput {
            asset_class: DoctorAssetClass::ModelCache,
            status: model_status.to_string(),
            path: model_quarantine_marker.or(model_dir),
            current_db_matches: None,
            ready: Some(matches!(
                semantic_availability.as_str(),
                "ready"
                    | "hash_fallback"
                    | "index_missing"
                    | "index_building"
                    | "index_stale"
            )),
            present: None,
            safe_to_rebuild: false,
            safe_for_auto_repair: false,
            note: "model cache is opt-in; doctor reports state without downloading or deleting model files".to_string(),
        },
    );
    let vector_index = doctor_derived_semantic_component_report(
        data_dir,
        DoctorDerivedSemanticComponentInput {
            asset_class: DoctorAssetClass::DerivedSemanticIndex,
            status: vector_status.to_string(),
            path: vector_index_path,
            current_db_matches: vector_matches,
            ready: vector_ready,
            present: vector_present,
            safe_to_rebuild,
            safe_for_auto_repair: false,
            note: "semantic/vector indexes are derived from the canonical archive and explicit model policy".to_string(),
        },
    );
    let hnsw_index = doctor_derived_semantic_component_report(
        data_dir,
        DoctorDerivedSemanticComponentInput {
            asset_class: DoctorAssetClass::DerivedSemanticIndex,
            status: hnsw_status.to_string(),
            path: hnsw_path,
            current_db_matches: None,
            ready: Some(hnsw_ready),
            present: doctor_json_pointer_bool(state, "/semantic/hnsw_present"),
            safe_to_rebuild,
            safe_for_auto_repair: false,
            note: "HNSW/mmap accelerators are optional derived assets and must not block lexical search".to_string(),
        },
    );
    let memoization_cache = doctor_derived_semantic_component_report(
        data_dir,
        DoctorDerivedSemanticComponentInput {
            asset_class: DoctorAssetClass::MemoCache,
            status: memo_status.to_string(),
            path: memo_quarantine_marker,
            current_db_matches: None,
            ready: None,
            present: None,
            safe_to_rebuild: archive_db_usable,
            safe_for_auto_repair: false,
            note: "memoization caches are derived and can be rebuilt or reclaimed only through explicit safe plans".to_string(),
        },
    );
    let mut notes = vec![
        "semantic/model/vector/memo assets are derived or optional quality layers".to_string(),
        "lexical search and archive recovery remain the authoritative recovery path".to_string(),
    ];
    if lexical_fallback_active {
        notes.push(
            "default hybrid search will fail open to lexical until semantic assets are ready"
                .to_string(),
        );
    }

    DoctorDerivedSemanticAssetReport {
        schema_version: 1,
        status,
        doctor_check_status,
        asset_class,
        safety_classification: asset_policy.safety_classification,
        fallback_mode,
        semantic_status,
        semantic_availability,
        can_search,
        lexical_search_unblocked: true,
        blocks_archive_recovery: false,
        safe_to_rebuild,
        safe_for_auto_repair: false,
        network_allowed: false,
        auto_download_allowed: false,
        auto_download_attempted: false,
        skipped_auto_download_reason,
        model_policy,
        selected_repair_action,
        recommended_action,
        probe_duration_ms,
        redaction_status: "redacted".to_string(),
        receipt_path: None,
        model_cache,
        vector_index,
        hnsw_index,
        memoization_cache,
        notes,
    }
}

#[cfg(test)]
mod doctor_derived_semantic_asset_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ann_presence_is_not_readiness_and_does_not_probe_diagnostic_paths() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut state = semantic_state(temp.path(), "ready", "ready", true, None);
        for (present, ann_state, expected_status) in [
            (true, "present_unverified", "present-unverified"),
            (false, "absent", "not-present"),
            (false, "not_inspected", "not-inspected"),
            (false, "inspection_failed", "inspection-failed"),
        ] {
            state["semantic"]["hnsw_present"] = json!(present);
            state["semantic"]["hnsw_state"] = json!(ann_state);
            let report = doctor_build_derived_semantic_asset_report(temp.path(), &state, true, 0);
            assert_eq!(report.hnsw_index.status, expected_status);
            assert_eq!(report.hnsw_index.present, Some(present));
            assert_eq!(report.hnsw_index.ready, Some(false));
            assert!(!report.blocks_archive_recovery);
        }
    }

    fn semantic_state(
        data_dir: &Path,
        status: &str,
        availability: &str,
        can_search: bool,
        fallback_mode: Option<&str>,
    ) -> serde_json::Value {
        json!({
            "semantic": {
                "status": status,
                "availability": availability,
                "can_search": can_search,
                "fallback_mode": fallback_mode,
                "model_dir": data_dir.join("models/all-MiniLM-L6-v2").display().to_string(),
                "vector_index_path": data_dir.join("vector_index/vector.fast.idx").display().to_string(),
                "hnsw_path": data_dir.join("vector_index/hnsw.fast.idx").display().to_string(),
                "hnsw_ready": false,
                "fast_tier": {
                    "present": false,
                    "ready": false,
                    "current_db_matches": false
                },
                "quality_tier": {
                    "present": false,
                    "ready": false,
                    "current_db_matches": false
                },
                "checkpoint": {
                    "active": false
                }
            }
        })
    }

    #[test]
    fn absent_model_reports_lexical_fallback_without_network_or_archive_block() {
        let temp = tempfile::tempdir().expect("tempdir");
        let state = semantic_state(
            temp.path(),
            "missing",
            "model_missing",
            false,
            Some("lexical"),
        );

        let report = doctor_build_derived_semantic_asset_report(temp.path(), &state, true, 7);

        assert_eq!(report.status, "lexical-fallback");
        assert_eq!(report.doctor_check_status, "warn");
        assert_eq!(report.asset_class, DoctorAssetClass::ModelCache);
        assert_eq!(report.fallback_mode, "lexical");
        assert!(report.safe_to_rebuild);
        assert!(!report.safe_for_auto_repair);
        assert!(!report.blocks_archive_recovery);
        assert!(!report.network_allowed);
        assert!(!report.auto_download_allowed);
        assert!(!report.auto_download_attempted);
        assert!(!report.model_cache.safe_to_rebuild);
        assert!(report.recommended_action.contains("cass models install"));
    }

    #[test]
    fn semantic_fallback_human_copy_is_explicitly_non_archive_damage() {
        let temp = tempfile::tempdir().expect("tempdir");
        let state = semantic_state(
            temp.path(),
            "missing",
            "model_missing",
            false,
            Some("lexical"),
        );

        let report = doctor_build_derived_semantic_asset_report(temp.path(), &state, true, 7);
        let summary =
            doctor_human_semantic_fallback_summary(&report).expect("semantic fallback copy");

        assert!(summary.contains("not archive damage"));
        assert!(summary.contains("cass never acquires models during doctor"));
        assert!(summary.contains("cass models install"));
        assert!(!summary.contains("cass models install --json"));
        assert!(summary.contains("cass index --semantic --json"));
    }

    #[test]
    fn corrupt_model_cache_quarantine_is_explicit_manual_repair() {
        let temp = tempfile::tempdir().expect("tempdir");
        let model_dir = temp.path().join("models/all-MiniLM-L6-v2");
        std::fs::create_dir_all(&model_dir).expect("create model dir");
        std::fs::write(model_dir.join(".quarantined"), b"bad checksum")
            .expect("write quarantine marker");
        let state = semantic_state(temp.path(), "error", "load_failed", false, Some("lexical"));

        let report = doctor_build_derived_semantic_asset_report(temp.path(), &state, true, 11);

        assert_eq!(report.status, "model-cache-quarantined");
        assert_eq!(report.asset_class, DoctorAssetClass::ModelCache);
        assert_eq!(
            report.selected_repair_action,
            "manual-model-cache-verify-or-reinstall"
        );
        assert_eq!(report.model_cache.status, "quarantined");
        assert!(
            report
                .model_cache
                .redacted_path
                .as_deref()
                .is_some_and(|path| path.starts_with("[cass-data]/"))
        );
        assert!(!report.model_cache.auto_download_attempted);
    }

    #[test]
    fn stale_vectors_are_rebuildable_derived_semantic_assets() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut state = semantic_state(temp.path(), "stale", "index_stale", false, Some("lexical"));
        state["semantic"]["fast_tier"]["present"] = json!(true);
        state["semantic"]["fast_tier"]["ready"] = json!(true);
        state["semantic"]["fast_tier"]["current_db_matches"] = json!(false);

        let report = doctor_build_derived_semantic_asset_report(temp.path(), &state, true, 13);

        assert_eq!(report.asset_class, DoctorAssetClass::DerivedSemanticIndex);
        assert_eq!(report.vector_index.status, "stale");
        assert_eq!(report.vector_index.present, Some(true));
        assert_eq!(report.vector_index.current_db_matches, Some(false));
        assert!(report.vector_index.safe_to_rebuild);
        assert!(!report.vector_index.blocks_archive_recovery);
        assert_eq!(report.selected_repair_action, "manual-cass-index-semantic");
    }

    #[test]
    fn failed_hnsw_inspection_is_optional_and_does_not_block_lexical() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mut state = semantic_state(
            temp.path(),
            "missing",
            "index_missing",
            false,
            Some("lexical"),
        );
        state["semantic"]["hnsw_present"] = json!(true);
        state["semantic"]["hnsw_state"] = json!("inspection_failed");

        let report = doctor_build_derived_semantic_asset_report(temp.path(), &state, true, 17);

        assert_eq!(report.hnsw_index.status, "inspection-failed");
        assert_eq!(report.hnsw_index.present, Some(true));
        assert_eq!(report.hnsw_index.ready, Some(false));
        assert!(report.lexical_search_unblocked);
        assert!(!report.hnsw_index.blocks_archive_recovery);
    }

    #[test]
    fn memo_quarantine_is_reported_as_rebuildable_cache_not_archive_damage() {
        let temp = tempfile::tempdir().expect("tempdir");
        let memo_dir = temp.path().join("semantic/memo-cache");
        std::fs::create_dir_all(&memo_dir).expect("create memo dir");
        std::fs::write(memo_dir.join(".quarantined"), b"schema drift")
            .expect("write memo quarantine");
        let state = semantic_state(temp.path(), "ready", "ready", true, None);

        let report = doctor_build_derived_semantic_asset_report(temp.path(), &state, true, 19);

        assert_eq!(report.status, "memo-cache-quarantined");
        assert_eq!(report.asset_class, DoctorAssetClass::MemoCache);
        assert_eq!(report.doctor_check_status, "warn");
        assert_eq!(report.memoization_cache.status, "quarantined");
        assert!(report.memoization_cache.safe_to_rebuild);
        assert!(!report.memoization_cache.blocks_archive_recovery);
    }

    #[test]
    fn disabled_semantic_policy_is_not_a_repair_or_archive_problem() {
        let temp = tempfile::tempdir().expect("tempdir");
        let state = semantic_state(temp.path(), "disabled", "disabled", false, Some("lexical"));

        let report = doctor_build_derived_semantic_asset_report(temp.path(), &state, true, 23);

        assert_eq!(report.status, "policy-disabled");
        assert_eq!(report.doctor_check_status, "pass");
        assert_eq!(report.model_policy, "disabled-by-policy");
        assert!(!report.safe_to_rebuild);
        assert_eq!(report.selected_repair_action, "none");
        assert!(!report.network_allowed);
        assert!(report.recommended_action.contains("disabled by policy"));
    }
}

pub(crate) fn doctor_risk_level_for_reports(
    coverage_risk: &DoctorCoverageRiskSummary,
    checks: &[DoctorCheckReport],
) -> &'static str {
    if coverage_risk.status == "sole_copy_risk"
        || checks
            .iter()
            .any(|check| check.data_loss_risk == DoctorDataLossRisk::High)
    {
        return "high";
    }
    if coverage_risk.missing_current_source_count > 0
        || doctor_unmirrored_rows_are_a_gap(coverage_risk)
        || checks.iter().any(|check| {
            matches!(
                check.data_loss_risk,
                DoctorDataLossRisk::Medium | DoctorDataLossRisk::Unknown
            )
        })
    {
        return "medium";
    }
    if checks
        .iter()
        .any(|check| check.status == "warn" || check.data_loss_risk == DoctorDataLossRisk::Low)
    {
        return "low";
    }
    "none"
}

pub(crate) fn doctor_recommended_action_for_reports(
    initialized_action: Option<String>,
    operation_state: &DoctorOperationStateReport,
    coverage_risk: &DoctorCoverageRiskSummary,
    checks: &[DoctorCheckReport],
) -> Option<String> {
    if initialized_action.is_some() {
        return initialized_action;
    }
    // Active index maintenance and another process's doctor repair both set
    // `mutation_blocked_reason`. An active repair with no blocker is this
    // `doctor --fix` run holding its own lock, whose `next_action` only says
    // mutation is allowed; it must not mask the step after this run (zp0fp).
    if operation_state.mutation_blocked_reason.is_some() {
        return Some(operation_state.next_action.clone());
    }
    if coverage_risk.recommended_action != "none" {
        return Some(coverage_risk.recommended_action.clone());
    }
    checks
        .iter()
        .find(|check| check.status != "pass" && check.recommended_action != "none")
        .map(|check| check.recommended_action.to_string())
}

#[cfg(test)]
#[test]
pub(crate) fn doctor_recommended_action_defers_to_operation_state_only_when_mutation_is_blocked() {
    let state = |blocked: Option<&str>| DoctorOperationStateReport {
        schema_version: 1,
        read_only_check_allowed: true,
        mutating_doctor_allowed: blocked.is_none(),
        active_index_maintenance: false,
        active_rebuild: false,
        active_watch: false,
        active_doctor_repair: true,
        owner_count: 1,
        stale_owner_count: 0,
        interrupted_state_count: 0,
        mutation_blocked_reason: blocked.map(str::to_string),
        owners: Vec::new(),
        interrupted_states: Vec::new(),
        next_action: "operation-state next action".to_string(),
        notes: Vec::new(),
    };
    let risk = DoctorCoverageRiskSummary {
        recommended_action: "coverage next step".to_string(),
        ..DoctorCoverageRiskSummary::default()
    };
    // This `doctor --fix` run holds its own lock: the step after it wins.
    assert_eq!(
        doctor_recommended_action_for_reports(None, &state(None), &risk, &[]).as_deref(),
        Some("coverage next step")
    );
    // Another doctor holds the lock: wait for it first.
    assert_eq!(
        doctor_recommended_action_for_reports(
            None,
            &state(Some(
                "another cass doctor --fix owner holds the mutation lock"
            )),
            &risk,
            &[],
        )
        .as_deref(),
        Some("operation-state next action")
    );
}

pub(crate) fn doctor_active_repair_report(
    operation_state: &DoctorOperationStateReport,
) -> serde_json::Value {
    serde_json::json!({
        "active": operation_state.active_doctor_repair,
        "active_index_maintenance": operation_state.active_index_maintenance,
        "active_rebuild": operation_state.active_rebuild,
        "active_watch": operation_state.active_watch,
        "owner_count": operation_state.owner_count,
        "stale_owner_count": operation_state.stale_owner_count,
        "interrupted_state_count": operation_state.interrupted_state_count,
        "mutation_blocked_reason": operation_state.mutation_blocked_reason.clone(),
        "next_action": operation_state.next_action.clone(),
    })
}

pub(crate) fn doctor_check_scope_report(
    command_surface: doctor::DoctorCommandSurface,
    execution_mode: doctor::DoctorExecutionMode,
) -> serde_json::Value {
    let mut skipped_expensive_collectors = vec![
        serde_json::json!({
            "name": "full_raw_log_reparse",
            "status": "not_checked",
            "next_action": "Use a future deep archive verification command when raw parser loss must be audited."
        }),
        serde_json::json!({
            "name": "semantic_embedding_deep_integrity",
            "status": "not_checked",
            "next_action": "Doctor checks bounded semantic readiness only; use `cass models verify --json` and explicit semantic backfill commands for deep model/vector integrity."
        }),
        serde_json::json!({
            "name": "network_source_sync",
            "status": "removed",
            "next_action": "Remote transports were removed by the local-only conversion; configured remote sources are diagnosed locally and never probed."
        }),
    ];
    if command_surface == doctor::DoctorCommandSurface::Check {
        skipped_expensive_collectors.push(serde_json::json!({
            "name": "cleanup_planning",
            "status": "not_checked",
            "next_action": "Use explicit repair/cleanup planning once read-only check output has been inspected."
        }));
    }

    serde_json::json!({
        "schema_version": 1,
        "surface": command_surface.stable_name(),
        "execution_mode": execution_mode.stable_name(),
        "read_only": !execution_mode.permits_mutation(),
        "mutation_allowed": execution_mode.permits_mutation(),
        "bounded": command_surface == doctor::DoctorCommandSurface::Check,
        "cleanup_planning": if command_surface == doctor::DoctorCommandSurface::Check {
            serde_json::json!({
                "status": "not_checked",
                "note": "bounded doctor check does not run cleanup dry-run planning"
            })
        } else if execution_mode.permits_mutation() {
            serde_json::json!({
                "status": "checked_and_apply_gated",
                "note": "mutating doctor execution runs cleanup planning and applies only gated derived-asset cleanup"
            })
        } else {
            serde_json::json!({
                "status": "checked",
                "note": "legacy read-only doctor output includes cleanup dry-run planning in quarantine metadata"
            })
        },
        "skipped_expensive_collectors": skipped_expensive_collectors,
    })
}

pub(crate) fn doctor_normalized_provider_slug(provider: &str) -> String {
    let normalized = provider.trim().to_ascii_lowercase().replace('-', "_");
    if normalized.is_empty() {
        "unknown".to_string()
    } else {
        public_connector_slug(&normalized).to_string()
    }
}

pub(crate) fn doctor_normalized_source_path(path: Option<&str>) -> Option<String> {
    path.map(str::trim)
        .filter(|path| !path.is_empty())
        .map(ToOwned::to_owned)
}

pub(crate) fn doctor_provider_prune_risk(provider: &str) -> DoctorProviderPruneRisk {
    match provider {
        "claude_code" => DoctorProviderPruneRisk {
            level: "high",
            note: "Claude Code may prune local harness logs; cass archive rows can be the durable copy.",
        },
        "codex" => DoctorProviderPruneRisk {
            level: "high",
            note: "Codex may prune local harness logs; cass archive rows can be the durable copy.",
        },
        "cursor" | "chatgpt" | "gemini" | "opencode" => DoctorProviderPruneRisk {
            level: "medium",
            note: "Provider history can move, rotate, or live behind app-managed storage; treat missing upstream files as a coverage risk.",
        },
        "unknown" => DoctorProviderPruneRisk {
            level: "unknown",
            note: "Provider is unknown, so doctor cannot infer retention behavior.",
        },
        _ => DoctorProviderPruneRisk {
            level: "normal",
            note: "No provider-specific pruning behavior is encoded yet; monitor source coverage over time.",
        },
    }
}
