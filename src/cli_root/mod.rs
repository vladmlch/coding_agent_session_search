pub(crate) mod analytics_db;
pub(crate) mod chatgpt_import;
pub(crate) mod cli_types;
pub(crate) mod commands;
pub(crate) mod dispatch;
pub(crate) mod dispatch_pages_doctor;
pub(crate) mod doctor_contracts;
pub(crate) mod doctor_forensics;
pub(crate) mod doctor_inventory_types;
pub(crate) mod doctor_policy;
pub(crate) mod doctor_storage_pressure;
pub(crate) mod doctor_taxonomy;
pub(crate) mod entry;
pub(crate) mod maintenance_cmds;
pub(crate) mod normalize_args;
pub(crate) mod normalize_prepasses;
pub(crate) mod pack_cmd;
pub(crate) mod pack_fields;
pub(crate) mod parse_recovery;
pub(crate) mod readiness;
pub(crate) mod robot_docs;
pub(crate) mod search_exec;
pub(crate) mod search_exec_present;
pub(crate) mod search_filters;
pub(crate) mod search_format;
pub(crate) mod search_robot_output;
pub(crate) mod search_self_heal;
pub(crate) mod search_setup;
pub(crate) mod state_meta;
pub(crate) mod state_probe;
pub(crate) mod stats_diag_cmds;
pub(crate) mod tracing_setup;

#[cfg(test)]
mod log_hygiene_tests;
#[cfg(test)]
mod normalize_tests;
#[cfg(test)]
mod search_self_heal_tests;

#[allow(unused_imports)]
pub(crate) use self::{
    analytics_db::*, chatgpt_import::*, cli_types::*, commands::*, dispatch::*,
    dispatch_pages_doctor::*, doctor_contracts::*, doctor_forensics::*, doctor_inventory_types::*,
    doctor_policy::*, doctor_storage_pressure::*, doctor_taxonomy::*, entry::*,
    maintenance_cmds::*, normalize_args::*, normalize_prepasses::*, pack_cmd::*, pack_fields::*,
    parse_recovery::*, readiness::*, robot_docs::*, search_exec::*, search_exec_present::*,
    search_filters::*, search_format::*, search_robot_output::*, search_self_heal::*,
    search_setup::*, state_meta::*, state_probe::*, stats_diag_cmds::*, tracing_setup::*,
};
