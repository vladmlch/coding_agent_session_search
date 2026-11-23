pub(crate) mod archive_export;
pub(crate) mod archive_scan_normalize;
pub(crate) mod backups;
pub(crate) mod baseline;
pub(crate) mod candidate_promotion_apply;
pub(crate) mod candidate_promotion_prep;
pub(crate) mod candidate_reconstruct;
pub(crate) mod candidate_staging_core;
pub(crate) mod config_exclusion_and_semantic;
pub(crate) mod coverage_and_authority;
pub(crate) mod diag_and_onboarding_helpers;
pub(crate) mod fs_mutation_executor;
pub(crate) mod fs_mutation_guards;
pub(crate) mod locks_and_heartbeat;
pub(crate) mod plan_and_events;
pub(crate) mod post_repair_probes;
pub(crate) mod quarantine_and_cleanup;
pub(crate) mod raw_mirror_amplification;
pub(crate) mod raw_mirror_backfill;
pub(crate) mod raw_mirror_verify;
pub(crate) mod repair_plan_and_failure_markers;
pub(crate) mod runs_and_subcommands;
pub(crate) mod source_inventory;
pub(crate) mod support_bundle;

#[allow(unused_imports)]
pub(crate) use {
    archive_export::*, archive_scan_normalize::*, backups::*, baseline::*,
    candidate_promotion_apply::*, candidate_promotion_prep::*, candidate_reconstruct::*,
    candidate_staging_core::*, config_exclusion_and_semantic::*, coverage_and_authority::*,
    diag_and_onboarding_helpers::*, fs_mutation_executor::*, fs_mutation_guards::*,
    locks_and_heartbeat::*, plan_and_events::*, post_repair_probes::*, quarantine_and_cleanup::*,
    raw_mirror_amplification::*, raw_mirror_backfill::*, raw_mirror_verify::*,
    repair_plan_and_failure_markers::*, runs_and_subcommands::*, source_inventory::*,
    support_bundle::*,
};

#[cfg(test)]
#[path = "tests/doctor_asset_taxonomy/mod.rs"]
mod doctor_asset_taxonomy_tests;

#[cfg(all(test, unix))]
mod path_safety_tests;

#[cfg(all(test, unix))]
mod cleanup_target_safety_tests;
