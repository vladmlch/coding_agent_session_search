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
pub mod cli;
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
pub(crate) mod robot_schema;
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

pub use crate::cli::args::Cli;
pub use crate::cli::args::Commands;
pub use crate::cli::args::InvocationInterpretation;
pub use crate::cli::args_sub::AgentsAction;
pub use crate::cli::args_sub::AggregateField;
pub use crate::cli::args_sub::AggregationBucket;
pub use crate::cli::args_sub::Aggregations;
pub use crate::cli::args_sub::AnalyticsBucketing;
pub use crate::cli::args_sub::AnalyticsCommand;
pub use crate::cli::args_sub::AnalyticsCommon;
pub use crate::cli::args_sub::AnalyticsTrack;
pub use crate::cli::args_sub::ColorPref;
pub use crate::cli::args_sub::ConvExportFormat;
pub use crate::cli::args_sub::DisplayFormat;
pub use crate::cli::args_sub::FieldAggregation;
pub use crate::cli::args_sub::ImportCommand;
pub use crate::cli::args_sub::MappingsAction;
pub use crate::cli::args_sub::MirrorCommand;
pub use crate::cli::args_sub::ModelsCommand;
pub use crate::cli::args_sub::PagesDeployTarget;
pub use crate::cli::args_sub::PagesSubcommand;
pub use crate::cli::args_sub::ProgressMode;
pub use crate::cli::args_sub::QuarantineCommand;
pub use crate::cli::args_sub::RebuildProfileArg;
pub use crate::cli::args_sub::RobotFormat;
pub use crate::cli::args_sub::RobotTopic;
pub use crate::cli::args_sub::ScheduleCommand;
pub use crate::cli::args_sub::SourcesCommand;
pub use crate::cli::args_sub::TimelineGrouping;
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::ArgumentSchema;
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::CapabilitiesLimits;
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::CapabilitiesResponse;
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::CommandSchema;
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::EnvVarCapability;
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::ExitCodeCapability;
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::IntrospectResponse;
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::MistakeRecoveryCapability;
pub use crate::cli::commands::capabilities_introspect::capabilitiesresponse::WorkflowCapability;
pub use crate::cli::commands::capabilities_introspect::run_api_version::register_external_subcommands;
pub use crate::cli::commands::search::timefilter::SemanticSearchOptions;
pub use crate::cli::commands::search::timefilter::TimeFilter;
pub use crate::cli::commands::sessions_context::SourceIngestReuseCapability;
pub use crate::cli::entry::ParsedCli;
pub use crate::cli::entry::parse_cli;
pub use crate::cli::entry::run;
pub use crate::cli::entry::run_with_parsed;
pub use crate::cli::entry::try_run_with_parsed_fast;
pub use crate::cli::error::CliError;
pub use crate::cli::error::CliResult;
pub use crate::cli::error::ProgressResolved;
pub use crate::cli::paths::default_data_dir;
pub use crate::cli::paths::default_db_path;
pub use crate::cli::paths::resolve_data_dir;
pub use crate::export::messages::AgentFormat;
pub use crate::export::messages::MessageClassification;
pub use crate::export::messages::classify_message;
pub use crate::export::messages::detect_agent_format;
pub use crate::export::messages::extract_correlation_id;
pub use crate::export::messages::group_messages_for_export;
