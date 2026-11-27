// Index walks over parallel slices by a single counter; an iterator rewrite would
// obscure the arithmetic without changing behaviour.
#![allow(clippy::needless_range_loop)]
#![recursion_limit = "512"]
// `unsafe` is denied crate-wide outside tests; the only sanctioned sites carry
// `#[allow(unsafe_code)]` + a SAFETY comment (startup env writes, unavoidable FFI per AGENTS.md).
#![cfg_attr(not(test), deny(unsafe_code))]

// Modules shared verbatim (via `#[path]`) between this crate and binaries
// spell crate-internal paths as `coding_agent_search::…`; the alias makes
// that spelling resolve inside the library too.
extern crate self as coding_agent_search;

pub mod bookmarks;
pub mod connector_ingest_diagnostics;
pub mod connectors;
pub mod conversation_view;
pub mod crash_replay;
#[cfg(unix)]
pub mod daemon;
pub mod daemon_runtime_state;
pub mod dependency_pin_correlation;
pub mod doctor;
#[cfg(test)]
mod doctor_candidate_wal_tests;
pub(crate) mod doctor_chokepoint;
pub mod doctor_recover;
pub(crate) mod doctor_robot_docs;
pub(crate) mod doctor_runs;
pub(crate) mod doctor_undo;
pub mod evidence_bundle;
pub mod explainability;
pub mod export;
mod followup_coordinates;
pub mod franken_sync;
pub mod html_export;
mod index_result;
pub mod indexer;
pub mod model;
pub mod pages;
pub mod perf_evidence;
pub mod policy_registry;
pub mod privacy_exposure;
pub mod query_cost_planner;
pub mod raw_mirror;
pub(crate) mod recipes_robot_docs;
pub(crate) mod recovery_support_bundle;
pub mod resource_plan;
pub mod robot_budget_envelope;
pub mod root_cause_projection;
pub mod root_cause_taxonomy;
pub mod search;
pub mod search_defaults;
pub mod search_quality_eval;
pub mod source_onboarding;
pub mod sources;
pub mod storage;
pub mod subsystem_coverage_matrix;
pub mod time_parser;
mod timeline_json;
pub mod topology_budget;
pub mod viewer_extensions;

/// Shut down synchronous bridge runtimes cached on the calling thread.
///
/// The CLI calls this after its command future completes so Windows tears down
/// the runtimes before the CRT starts destroying thread-local storage.
#[doc(hidden)]
pub use search::quill_bridge::shutdown_thread_local_bridge_runtimes;

pub fn ensure_runtime_hooks_registered() {
    dependency_pin_correlation::build_metadata::BuildMetadata::register_idempotent(
        dependency_pin_correlation::build_metadata::BuildMetadata {
            ingest_contract: env!("CASS_SOURCE_INGEST_CONTRACT"),
            ingest_reuse: env!("CASS_SOURCE_INGEST_REUSE") == "true",
            ingest_reuse_blocker: env!("CASS_SOURCE_INGEST_REUSE_BLOCKER"),
            build_commit: option_env!("CASS_BUILD_COMMIT"),
            build_commit_date: option_env!("CASS_BUILD_COMMIT_DATE"),
            git_sha: option_env!("VERGEN_GIT_SHA"),
        },
    );
    search::archive_rebuild::register_archive_lexical_repair_hook(|db_path, data_dir| {
        indexer::repair_lexical_index_from_canonical_db_for_search(db_path, data_dir, None)
            .map(|outcome| outcome.indexed_docs)
    });
}

pub(crate) mod cli_app;
pub(crate) mod cli_root;
pub(crate) mod lib_doctor;

#[allow(unused_imports)]
use crate::franken_sync::compat::{
    ConnectionExt, OpenFlags as FrankenOpenFlags, RowExt,
    open_with_flags as open_franken_with_flags,
};
#[allow(unused_imports)]
use anyhow::Result;
#[allow(unused_imports)]
use base64::prelude::*;
#[allow(unused_imports)]
use chrono::Utc;
#[allow(unused_imports)]
use clap::{Arg, ArgAction, Command, CommandFactory, Parser, Subcommand, ValueEnum, ValueHint};
#[allow(unused_imports)]
use indexer::IndexOptions;
#[allow(unused_imports)]
pub(crate) use model::cli_error_kind::ErrorKind as CliErrorKind;
#[allow(unused_imports)]
use robot_budget_envelope::BudgetSnapshot;
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
#[allow(unused_imports)]
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub use cli_root::cli_types::{
    AgentsAction, AggregateField, AggregationBucket, Aggregations, ColorPref, ConvExportFormat,
    DisplayFormat, FieldAggregation, ImportCommand, MappingsAction, MirrorCommand, ModelsCommand,
    PagesDeployTarget, ProgressMode, ProgressResolved, QuarantineCommand, RebuildProfileArg,
    RobotTopic, SourcesCommand, TimelineGrouping,
};
pub use model::cli_error::{CliError, CliResult, RobotFormat};
pub use cli_root::commands::{Cli, Commands, InvocationInterpretation};
pub use cli_root::entry::{ParsedCli, parse_cli, run, run_with_parsed, try_run_with_parsed_fast};
pub use cli_root::search_filters::{SemanticSearchOptions, TimeFilter};

pub use cli_app::introspect::capabilities::{
    ArgumentSchema, CapabilitiesLimits, CapabilitiesResponse, CommandSchema, EnvVarCapability,
    ExitCodeCapability, IntrospectResponse, MistakeRecoveryCapability, SourceIngestReuseCapability,
    WorkflowCapability,
};
pub use cli_app::introspect::clap_schema::register_external_subcommands;
pub use model::paths::{default_data_dir, default_db_path, resolve_data_dir};
pub use cli_app::view_export::message_grouping::{
    AgentFormat, MessageClassification, classify_message, detect_agent_format,
    extract_correlation_id, group_messages_for_export,
};

#[allow(unused_imports)]
pub(crate) use cli_app::{
    ViewWindow, cached_index_payload, canonical_followup_source_id, diagnostics_connector_paths,
    doctor_archive_wide_collectors_deferred_reason,
    doctor_open_error_message_is_affirmative_corruption, extract_message_timestamp,
    followup_source_is_local, is_fsqlite_runtime_lock_sidecar, maybe_test_view_delay,
    normalized_provenance_origin_host, normalized_provenance_origin_kind,
    normalized_provenance_source_id, normalized_source_identity_sql_expr,
    output_bounded_view_partial, run_doctor_impl, run_expand, run_view,
    validate_followup_source_id,
};
#[allow(unused_imports)]
pub(crate) use export::message_parse::{
    extract_role, extract_text_content, extract_text_content_without_tool_blocks,
    extract_tool_calls, format_as_markdown, smart_truncate, strip_flattened_tool_call_lines,
    strip_tool_marker,
};
#[allow(unused_imports)]
pub(crate) use cli_root::{
    DEFAULT_STALE_THRESHOLD_SECS, DoctorCandidateLiveInventory, DoctorCandidatePromotionReport,
    auto_refresh_is_scratch_data_dir, build_truth_table_for_data_dir,
    build_truth_table_from_state, cass_lexical_index_initialized, cass_not_initialized,
    cass_not_initialized_recommended_action, cli_error_json_payload,
    doctor_forensic_create_private_dir_all, doctor_now_ms, doctor_redacted_path,
    doctor_sqlite_sidecar_path, encode_structured_value, lexical_rebuild_progress_note,
    maybe_test_search_worker_delay, robot_format_from_env, run_read_only_search_worker,
    state_meta_json_for_health,
};
#[allow(unused_imports)]
pub(crate) use model::cli_error::WrapConfig;
#[allow(unused_imports)]
pub(crate) use model::paths::{
    default_data_dir_with_source, normalize_path_identity, path_identities_match,
    stored_path_identity_matches,
};
#[allow(unused_imports)]
pub(crate) use storage::sqlite::open_and_locks::bounded_canonical_db_corruption_probe;
#[allow(unused_imports)]
pub(crate) use lib_doctor::{
    DOCTOR_LEAKED_PAGES_REPAIR_BACKUP_MANIFEST_KIND, DoctorRuntimeSummaryInput,
    build_doctor_runtime_summary, doctor_candidate_id, doctor_candidate_live_inventory,
    doctor_candidate_promotion_derived_status, doctor_candidate_promotion_root,
    doctor_candidate_root, doctor_copy_regular_file_to_private_target,
    doctor_fast_coverage_risk_unchecked, doctor_write_private_json_artifact, file_blake3_hex,
    promote_doctor_reconstruct_candidate_bundle, run_doctor_archive_normalize_impl,
    run_doctor_archive_scan_impl, run_doctor_backups_impl, write_doctor_restore_candidate_manifest,
};

#[cfg(test)]
fn lib_split_is_src_test_only(rel_path: &str) -> bool {
    rel_path.ends_with("_tests.rs")
        || rel_path.ends_with(".tests.rs")
        || rel_path.contains("/tests/")
        || rel_path.ends_with("/tests.rs")
}

#[cfg(test)]
fn lib_split_collect_rs_files(dir: &Path, manifest_dir: &Path, out: &mut Vec<(String, String)>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut paths: Vec<PathBuf> = entries.flatten().map(|entry| entry.path()).collect();
    paths.sort();
    for path in paths {
        if path.is_dir() {
            lib_split_collect_rs_files(&path, manifest_dir, out);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            let rel = path
                .strip_prefix(manifest_dir)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            let content = std::fs::read_to_string(&path)
                .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
            out.push((rel, content));
        }
    }
}

#[cfg(test)]
fn lib_split_strip_cfg_test_items(source: &str) -> String {
    let bytes = source.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len);
    let mut i = 0usize;
    let mut brace_depth = 0usize;
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut item_attr_start: Option<usize> = None;
    let mut item_has_cfg_test = false;
    let mut skipping_until_brace_depth: Option<usize> = None;
    let mut skipping_brace_opened = false;
    let mut skipping_stmt_semicolon_depth: Option<usize> = None;

    let push_slice = |out: &mut String, start: usize, end: usize, active_skip: bool| {
        if !active_skip {
            out.push_str(&source[start..end]);
        }
    };

    while i < len {
        let is_skipping =
            skipping_until_brace_depth.is_some() || skipping_stmt_semicolon_depth.is_some();

        // Comments & strings are scanned atomically so braces/quotes inside them never confuse depth.
        if i + 1 < len && bytes[i] == b'/' && bytes[i + 1] == b'/' {
            let start = i;
            i += 2;
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            push_slice(&mut out, start, i, is_skipping);
            continue;
        }
        if i + 1 < len && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            let start = i;
            i += 2;
            let mut bc = 1usize;
            while i < len && bc > 0 {
                if i + 1 < len && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                    bc += 1;
                    i += 2;
                } else if i + 1 < len && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                    bc -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            push_slice(&mut out, start, i, is_skipping);
            continue;
        }
        if bytes[i] == b'r' {
            let mut j = i + 1;
            while j < len && bytes[j] == b'#' {
                j += 1;
            }
            if j < len && bytes[j] == b'"' {
                let hashes = j - (i + 1);
                let start = i;
                i = j + 1;
                while i < len {
                    if bytes[i] == b'"'
                        && i + 1 + hashes <= len
                        && bytes[i + 1..i + 1 + hashes].iter().all(|&b| b == b'#')
                    {
                        i += 1 + hashes;
                        break;
                    }
                    i += 1;
                }
                push_slice(&mut out, start, i, is_skipping);
                continue;
            }
        }
        if bytes[i] == b'"' {
            let start = i;
            i += 1;
            while i < len {
                if bytes[i] == b'\\' {
                    i = (i + 2).min(len);
                    continue;
                }
                if bytes[i] == b'"' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            push_slice(&mut out, start, i, is_skipping);
            continue;
        }
        if bytes[i] == b'\'' {
            if i + 2 < len && bytes[i + 1] != b'\\' && bytes[i + 2] == b'\'' {
                push_slice(&mut out, i, i + 3, is_skipping);
                i += 3;
                continue;
            }
            if i + 3 < len && bytes[i + 1] == b'\\' && bytes[i + 3] == b'\'' {
                push_slice(&mut out, i, i + 4, is_skipping);
                i += 4;
                continue;
            }
        }

        // At top of an attribute/item at brace_depth == 0
        if !is_skipping
            && brace_depth == 0
            && paren_depth == 0
            && bracket_depth == 0
            && (i == 0 || bytes[i - 1] == b'\n')
        {
            let line_end = source[i..].find('\n').map_or(len, |off| i + off);
            let trimmed = source[i..line_end].trim();
            if trimmed.starts_with("#[") {
                if item_attr_start.is_none() {
                    item_attr_start = Some(out.len());
                }
                if trimmed == "#[cfg(test)]"
                    || trimmed.starts_with("#[cfg(all(test,")
                    || trimmed.starts_with("#[cfg(all(test ")
                    || trimmed == "#[test]"
                {
                    item_has_cfg_test = true;
                }
            } else if trimmed.starts_with("//") || trimmed.is_empty() {
                if trimmed.is_empty() && !item_has_cfg_test {
                    item_attr_start = None;
                }
            } else if item_has_cfg_test {
                if let Some(cut) = item_attr_start.take() {
                    out.truncate(cut);
                }
                item_has_cfg_test = false;
                let rest = trimmed;
                let is_stmt = rest.starts_with("use ")
                    || rest.starts_with("pub use ")
                    || rest.starts_with("pub(crate) use ")
                    || rest.starts_with("const ")
                    || rest.starts_with("pub const ")
                    || rest.starts_with("pub(crate) const ")
                    || rest.starts_with("static ")
                    || rest.starts_with("pub static ")
                    || rest.starts_with("pub(crate) static ")
                    || rest.starts_with("type ")
                    || rest.starts_with("pub type ")
                    || rest.starts_with("pub(crate) type ")
                    || (rest.starts_with("mod ") && rest.ends_with(';'))
                    || (rest.starts_with("pub mod ") && rest.ends_with(';'))
                    || (rest.starts_with("pub(crate) mod ") && rest.ends_with(';'));
                if is_stmt && !rest.contains("const fn ") {
                    skipping_stmt_semicolon_depth = Some(brace_depth);
                } else {
                    skipping_until_brace_depth = Some(brace_depth);
                    skipping_brace_opened = false;
                }
            } else {
                item_attr_start = None;
            }
        }

        let ch = bytes[i];
        let mut ended_skip = false;
        if ch == b'{' {
            brace_depth += 1;
            if skipping_until_brace_depth.is_some() {
                skipping_brace_opened = true;
            }
        } else if ch == b'}' {
            brace_depth = brace_depth.saturating_sub(1);
            if let Some(target) = skipping_until_brace_depth
                && skipping_brace_opened
                && brace_depth == target
            {
                skipping_until_brace_depth = None;
                skipping_brace_opened = false;
                ended_skip = true;
            }
        } else if ch == b'(' {
            paren_depth += 1;
        } else if ch == b')' {
            paren_depth = paren_depth.saturating_sub(1);
        } else if ch == b'[' {
            bracket_depth += 1;
        } else if ch == b']' {
            bracket_depth = bracket_depth.saturating_sub(1);
        } else if ch == b';' && paren_depth == 0 && bracket_depth == 0 {
            if let Some(target) = skipping_stmt_semicolon_depth
                && brace_depth == target
            {
                skipping_stmt_semicolon_depth = None;
                ended_skip = true;
            } else if let Some(target) = skipping_until_brace_depth
                && !skipping_brace_opened
                && brace_depth == target
            {
                skipping_until_brace_depth = None;
                ended_skip = true;
            }
        }

        if !is_skipping && !ended_skip {
            let next_i = i + source[i..].chars().next().map_or(1, char::len_utf8);
            out.push_str(&source[i..next_i]);
            i = next_i;
        } else {
            i += source[i..].chars().next().map_or(1, char::len_utf8);
        }
    }

    out
}

#[cfg(test)]
pub(crate) fn lib_split_all_sources() -> Vec<(String, String)> {
    let manifest_dir = crate::subsystem_coverage_matrix::workspace_paths::workspace_root();
    let mut files = Vec::new();
    let lib_path = crate::subsystem_coverage_matrix::workspace_paths::resolve_workspace_source_file(
        &manifest_dir,
        "src/lib.rs",
    );
    let lib_src = std::fs::read_to_string(&lib_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", lib_path.display()));
    files.push(("src/lib.rs".to_string(), lib_src));
    let mut subdirs = Vec::new();
    for src_dir in
        crate::subsystem_coverage_matrix::workspace_paths::workspace_source_dirs(&manifest_dir)
    {
        for dir_name in ["cli_root", "lib_doctor", "cli_app"] {
            let dir = src_dir.join(dir_name);
            if dir.is_dir() {
                lib_split_collect_rs_files(&dir, &manifest_dir, &mut subdirs);
            }
        }
    }
    subdirs.sort_by(|a, b| a.0.cmp(&b.0));
    files.extend(subdirs);
    files
}

#[cfg(test)]
pub(crate) fn lib_split_production_sources() -> Vec<(String, String)> {
    lib_split_all_sources()
        .into_iter()
        .filter(|(rel_path, _)| !lib_split_is_src_test_only(rel_path))
        .map(|(rel_path, content)| (rel_path, lib_split_strip_cfg_test_items(&content)))
        .collect()
}

#[cfg(test)]
pub(crate) fn lib_split_production_concat_source() -> String {
    lib_split_production_sources()
        .into_iter()
        .map(|(_, content)| content)
        .collect::<Vec<_>>()
        .join("\n")
}
