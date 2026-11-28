#![recursion_limit = "512"]
#![cfg_attr(not(test), deny(unsafe_code))]

pub mod daemon_runtime_state;
pub mod dependency_pin_correlation;
pub mod evidence_bundle;
pub mod model;
pub mod perf_evidence;
pub mod resource_plan;
pub mod robot_budget_envelope;
pub mod root_cause_taxonomy;
pub mod search_defaults;
pub mod subsystem_coverage_matrix;
pub mod time_parser;
