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

pub mod analytics;
pub mod bakeoff;
pub mod bookmarks;
pub mod connector_ingest_diagnostics;
pub mod connectors;
pub mod crash_replay;
#[cfg(unix)]
pub mod daemon;
pub mod daemon_runtime_state;
pub mod dependency_pin_correlation;
pub mod doctor;

pub(crate) mod doctor_chokepoint;
pub mod doctor_recover;
pub(crate) mod doctor_robot_docs;
pub(crate) mod doctor_runs;
pub(crate) mod doctor_undo;
pub mod e2e_runner;
pub mod encryption;
pub mod evidence_bundle;
pub mod explainability;
pub mod export;
mod followup_coordinates;
pub mod franken_sync;
pub mod ftui_harness;
pub mod guide_planner;
pub mod guide_runner;
pub mod html_export;
pub mod incident_discovery;
mod index_result;
pub mod indexer;
pub mod metric_integrity;
pub mod model;
pub mod pages;
pub mod perf_evidence;
pub mod policy_registry;
pub mod privacy_exposure;
pub mod proof_artifact;
pub mod query_cost_planner;
pub mod raw_mirror;
pub(crate) mod recipes_robot_docs;
pub(crate) mod recovery_support_bundle;
pub mod resource_plan;
pub mod robot_budget_envelope;
pub mod root_cause_projection;
pub mod root_cause_taxonomy;
pub mod schedule;
pub mod search;
pub mod search_defaults;
pub mod search_quality_eval;
pub mod source_onboarding;
pub mod sources;
pub mod storage;
pub mod subsystem_coverage_matrix;
mod timeline_json;
pub mod top_session_summary;
pub mod topology_budget;
pub mod tui_asciicast;
pub mod ui;
pub mod viewer_extensions;
pub mod workflow_macros;

/// Shut down synchronous bridge runtimes cached on the calling thread.
///
/// The CLI calls this after its command future completes so Windows tears down
/// the runtimes before the CRT starts destroying thread-local storage.
#[doc(hidden)]
pub fn shutdown_thread_local_bridge_runtimes() -> bool {
    let fsqlite_shutdown = franken_sync::shutdown_driver();
    let quill_shutdown = search::quill_bridge::shutdown_driver();
    // SQLite connectors cache a separate runtime inside FAD. Drop it before
    // Windows process teardown terminates its worker threads (GH #519).
    // Evaluate every shutdown before combining results: a timeout in one
    // bridge must not leave another runtime cached until process exit.
    let fad_shutdown = franken_agent_detection::connectors::sqlite_sync::shutdown_driver();
    fsqlite_shutdown && quill_shutdown && fad_shutdown
}

use crate::franken_sync::compat::{ConnectionExt, RowExt};
use anyhow::Result;
use base64::prelude::*;
use chrono::Utc;
use clap::{Arg, ArgAction, Command, CommandFactory, Parser, Subcommand, ValueEnum, ValueHint};
use indexer::IndexOptions;
use model::cli_error_kind::ErrorKind as CliErrorKind;
use robot_budget_envelope::BudgetSnapshot;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, IsTerminal, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

// -- generated lib-split bridges --
pub(crate) mod robot_schema;
#[allow(unused_imports)]
pub(crate) use crate::robot_schema::build_response_schemas::*;
#[allow(unused_imports)]
pub(crate) use crate::robot_schema::response_schema_doctor_repair_failure_marker::*;
#[allow(unused_imports)]
pub(crate) use crate::robot_schema::response_schema_index_state::*;
#[allow(unused_imports)]
pub(crate) use crate::robot_schema::response_schema_pack::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::robot_schema::response_schema_tests_part_1::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::robot_schema::response_schema_tests_part_2::*;
// -- generated lib-split bridges --
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::indexer::stall_diagnostics_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::indexer::stall_watchdog::*;
// -- generated lib-split bridges --
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::export::message_grouping_tests::*;
#[allow(unused_imports)]
pub use crate::export::messages::AgentFormat;
#[allow(unused_imports)]
pub use crate::export::messages::MessageClassification;
#[allow(unused_imports)]
pub use crate::export::messages::classify_message;
#[allow(unused_imports)]
pub use crate::export::messages::detect_agent_format;
#[allow(unused_imports)]
pub use crate::export::messages::extract_correlation_id;
#[allow(unused_imports)]
pub use crate::export::messages::group_messages_for_export;
#[allow(unused_imports)]
pub(crate) use crate::export::messages::*;
// -- generated lib-split bridges --
pub mod cli;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::pack::pack_budget_ms::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::pack::pack_field_mask_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::agent_filter_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::aggregate_field_validation_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::ann_fallback_human_warning_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::compute_aggregations::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::effective_db_path_source::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::execute_search_operation::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::field_mask_resolution_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::fieldbudgets::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::lexical_engine_invariant_hint_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::output_display_results::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::resolve_search_defaults::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::run_cli_search::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::safe_robot_score_value::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::search_dry_run_meta::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::search_dry_run_meta_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::search_lexical_self_heal_gate_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::search_lexical_self_heal_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::search_time_filter_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::searchlexicalselfhealdiagnosis::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::sparse_threshold_for_visible_limit::*;
#[allow(unused_imports)]
pub use crate::cli::commands::search::timefilter::SemanticSearchOptions;
#[allow(unused_imports)]
pub use crate::cli::commands::search::timefilter::TimeFilter;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::timefilter::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::toon_encode_options_from_env::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::search::truncate_content_tests::*;
// -- generated lib-split bridges --
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::analytics::analytics_filter_validation_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::analytics::analytics_query_cli_error_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::analytics::analyticsvalidationsummary::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::analytics::franken_query_retry_callback_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::analytics::fresh_franken_count_retry::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::analytics::run_analytics::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::chatgpt_import_file_stem_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::chatgpt_import_scan_root_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::chatgpt_import_source_name::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::forget::command::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::forget::forget_selector_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::forget::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::import_chatgpt_export::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::quarantine_mirror_import::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::run_mirror_prune::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::stats_diag_storage::run_diag::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::stats_diag_storage::run_stats::*;
// -- generated lib-split bridges --
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::run_onboarding_interactive_wizard::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::status_health::cli_read_db_tests_part_1::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::status_health::cli_read_db_tests_part_2::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::status_health::diagnostics::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::status_health::doctor_fts_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::status_health::probe_daemon_runtime_for_diagnostics::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::status_health::root_cause_signal_gather_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::status_health::run_health::*;
// -- generated lib-split bridges --
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::path_spelling_tests::*;
#[allow(unused_imports)]
pub use crate::cli::commands::sessions_context::SourceIngestReuseCapability;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::sessions_context::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::source_ingest_reuse_capability_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::view::role_to_export_string::*;
// -- generated lib-split bridges --
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::ArgumentSchema;
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::CapabilitiesLimits;
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::CapabilitiesResponse;
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::CommandSchema;
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::EnvVarCapability;
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::ExitCodeCapability;
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::IntrospectResponse;
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::MistakeRecoveryCapability;
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::WorkflowCapability;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::capabilities_introspect::capabilitiesresponse::*;
#[allow(unused_imports)]
pub use crate::cli::commands::capabilities_introspect::run_api_version::register_external_subcommands;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::capabilities_introspect::run_api_version::*;
// -- generated lib-split bridges --
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::expand::*;
#[cfg(all(test, not(windows)))]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export::clipboard_helper_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export::export_html_password_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export::export_timestamp_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export::format_as_markdown::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export::indexed_conversation_fallback_tests_part_1::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export::indexed_conversation_fallback_tests_part_2::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export::indexed_export_cli_error::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export::legacy_source_filter_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export::robot_output_score_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::export_html::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::index::run_lexical_reconcile_cli::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::resume::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::timeline::*;
#[allow(unused_imports)]
pub use crate::cli::paths::default_data_dir;
#[allow(unused_imports)]
pub use crate::cli::paths::default_db_path;
#[allow(unused_imports)]
pub use crate::cli::paths::resolve_data_dir;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::paths::resolve_data_dir_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::paths::session_paths::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::paths::*;
// -- generated lib-split bridges --
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::cli_models_resolution_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::daemon::*;
#[cfg(all(test, unix))]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::daemon_cli_config_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::mappings::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::models::run_models_command::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::models::run_models_remove::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::models::semantic_identity_backfill_checkpoint_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::run_on_large_stack::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::schedule::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::sources::run_sources_command::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::sources::source_doctor_local_storage_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::state_db_integrity_probe_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::subcommand_robot_output_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::commands::timeline_datetime_parse_tests::*;
// -- generated lib-split bridges --
#[allow(unused_imports)]
pub(crate) use crate::doctor::forensic::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::policy::assets::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::policy::types::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::policy::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::repair_state::types::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::checks::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::config::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::derived_semantic::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::doctor_derived_semantic_asset_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::doctor_path_slash_string_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::doctor_pattern_matches_relative_path::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::human::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::inventory::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::recommendations::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::storage::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::types::*;
// -- generated lib-split bridges --
#[cfg(all(test, unix))]
#[allow(unused_imports)]
pub(crate) use crate::doctor::cleanup_target_safety_tests::*;
#[cfg(all(test, unix))]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_archive_export_path_safety_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_asset_taxonomy_tests_part_1::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_asset_taxonomy_tests_part_2::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_asset_taxonomy_tests_part_3::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_asset_taxonomy_tests_part_4::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_asset_taxonomy_tests_part_5::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_asset_taxonomy_tests_part_6::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_asset_taxonomy_tests_part_7::*;
#[cfg(all(test, unix))]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_baseline_path_safety_tests::*;
#[cfg(all(test, unix))]
#[allow(unused_imports)]
pub(crate) use crate::doctor::doctor_support_bundle_path_safety_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::fs_safety::cleanup_apply::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::fs_safety::doctor_copy_source_path_is_safe::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::fs_safety::mutation_executor::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::lexical_cleanup_disposition_label::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::repair_state::cliheartbeat::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::repair_state::collect_doctor_interrupted_operation_states::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::repair_state::doctor_heartbeat_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::repair_state::doctor_sorted_cleanup_actions::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::repair_state::finalize_cleanup_apply_contract::*;
// -- generated lib-split bridges --
#[allow(unused_imports)]
pub(crate) use crate::doctor::archive::doctor_archive_coverage_snapshot::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::archive::export::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::backup_restore::kinds::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::backup_restore::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::baseline::doctor_baseline_path_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::baseline::document::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::baseline::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::candidate::promote_doctor_reconstruct_candidate_bundle::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::candidate::promotion::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::raw_mirror::collect_doctor_raw_mirror_report_with_thresholds_and::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::raw_mirror::doctor_bound_healthy_listing::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::raw_mirror::doctor_raw_mirror_listing_bound_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::raw_mirror::doctor_raw_mirror_root_dir::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::raw_mirror::doctorrawmirrorverificationmode::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::raw_mirror::query_doctor_raw_mirror_backfill_candidates::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::repair_state::doctor_acquire_mutation_lock::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::repair_state::doctor_lock_probe_error_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::repair_state::locks::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::report::coverage::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::run_doctor_ls::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::sources::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::support_bundle::capture::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::support_bundle::doctor_support_bundle_redaction_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::support_bundle::*;
// -- generated lib-split bridges --
#[allow(unused_imports)]
pub(crate) use crate::doctor::candidate::build_doctor_reconstruct_candidate::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::candidate::doctor_candidate_root::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::doctor::probes::doctor_probe_timeout_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::probes::integrity::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::probes::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::run::*;
#[allow(unused_imports)]
pub(crate) use crate::doctor::sessionsummaryrecord::*;
// -- generated lib-split bridges --
#[allow(unused_imports)]
pub use crate::cli::args::{Cli};
#[allow(unused_imports)]
pub use crate::cli::args::{InvocationInterpretation};
#[allow(unused_imports)]
pub use crate::cli::args::{Commands};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{ScheduleCommand};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{QuarantineCommand};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{MirrorCommand};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{ImportCommand};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{SourcesCommand};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{ModelsCommand};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{MappingsAction};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{AgentsAction};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{RebuildProfileArg};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{AnalyticsBucketing};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{AnalyticsTrack};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{AnalyticsCommand};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{AnalyticsCommon};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{ColorPref};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{ProgressMode};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{RobotTopic};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{RobotFormat};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{DisplayFormat};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{ConvExportFormat};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{TimelineGrouping};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{PagesSubcommand};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{PagesDeployTarget};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{AggregateField};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{AggregationBucket};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{FieldAggregation};
#[allow(unused_imports)]
pub use crate::cli::args_sub::{Aggregations};
#[allow(unused_imports)]
pub use crate::cli::error::{CliError};
#[allow(unused_imports)]
pub use crate::cli::error::{CliResult};
#[allow(unused_imports)]
pub use crate::cli::error::{ProgressResolved};
#[allow(unused_imports)]
pub use crate::cli::entry::{ParsedCli};
#[allow(unused_imports)]
pub use crate::cli::entry::{parse_cli};
#[allow(unused_imports)]
pub use crate::cli::entry::{run};
#[allow(unused_imports)]
pub use crate::cli::entry::{run_with_parsed};
#[allow(unused_imports)]
pub use crate::cli::entry::{try_run_with_parsed_fast};
#[allow(unused_imports)]
pub(crate) use crate::cli::contract_version::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::args::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::args_sub::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::error::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::argv::root_structured_triage_rewrite::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::argv::robot_docs_shorthand_regression_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::argv::structured_output_request_span_at::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::argv::normalize::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::argv::canonical_top_level_command_tests::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::argv::removed_invocation_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::argv::heuristic_parse_recovery::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::entry::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::dispatch::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::state::state_db_open_timeout::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::state::health_watermark_probe_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::state::probe_state_db_strict_worker::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::state::index_error_mapping_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::output::cli_error_json_payload::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::state::state_meta_json::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::state::semantic_readiness_from_state::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::state::readiness_projection_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::output::warn_tui_terminal_profile::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::output::headless_asciicast_sentinel_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::output::configure_color::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::output::watch_once_resolution_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::output::describe_command::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::output::default_dep_log_suppression::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::output::log_hygiene_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::output::emit_correction_notes::*;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use crate::cli::output::highlight_matches_tests::*;
#[allow(unused_imports)]
pub(crate) use crate::cli::output::extract_search_terms::*;
