use super::{Check, DoctorRun, add_check};
use crate::cli::contract_version::DEFAULT_STALE_THRESHOLD_SECS;
use crate::cli::state::state_meta_json::state_meta_json_inner;
use crate::doctor;
use crate::doctor::policy::DOCTOR_REPAIR_ACTION_PROMOTE_RECONSTRUCT_CANDIDATE;
use crate::doctor::repair_state::cliheartbeat::{
    build_doctor_repair_plan_preview, doctor_push_timing_span, doctor_repair_plan_has_action,
    doctor_repair_push_blocker, doctor_serde_label,
};
use crate::doctor::repair_state::collect_doctor_interrupted_operation_states::build_doctor_operation_state_report;
use crate::doctor::repair_state::doctor_acquire_mutation_lock::doctor_acquire_mutation_lock;
use crate::doctor::repair_state::types::{
    DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS, DoctorMutationLockObservation,
};
use crate::doctor::report::derived_semantic::{
    doctor_build_derived_semantic_asset_report, doctor_fallback_mode_from_state,
};
use crate::doctor::report::inventory::DoctorSourceAuthorityReport;
use crate::doctor::report::types::DoctorDerivedSemanticAssetReport;
use std::time::Instant;

#[allow(clippy::collapsible_if, clippy::collapsible_else_if)]
impl DoctorRun {
    /// Repair-plan phase: the read-only semantic readiness probe and derived
    /// semantic asset report, the repair-plan preview, fingerprint-apply
    /// admission (including the late mutation-lock reacquisition and
    /// `fix_can_mutate` transitions), and the promotion plan/live blocker
    /// gates. Returns the derived-semantic-asset report `report` consumes.
    /// Original lines run verbatim in order.
    pub(crate) fn repair_plan(
        &mut self,
        source_authority: &DoctorSourceAuthorityReport,
    ) -> DoctorDerivedSemanticAssetReport {
        let checks = &mut self.checks;
        let semantic_probe_started = Instant::now();
        self.readiness_snapshot = state_meta_json_inner(
            &self.data_dir,
            &self.db_path,
            DEFAULT_STALE_THRESHOLD_SECS,
            false,
            Some(false),
            true,
            true,
            // passive_db_probe: doctor's readiness probe must stay read-only.
            true,
        );
        doctor_push_timing_span(
            &mut self.timing_spans,
            "semantic_probe",
            "semantic_model",
            "semantic_probe",
            semantic_probe_started,
            DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
            vec!["semantic readiness was inspected without downloading models".to_string()],
        );
        let semantic_probe_duration_ms = semantic_probe_started.elapsed().as_millis() as u64;
        self.fallback_mode = doctor_fallback_mode_from_state(&self.readiness_snapshot);
        let derived_semantic_assets = doctor_build_derived_semantic_asset_report(
            &self.data_dir,
            &self.readiness_snapshot,
            self.archive_queryable_for_non_destructive_derived_rebuild,
            semantic_probe_duration_ms,
        );
        let semantic_status = self
            .readiness_snapshot
            .pointer("/semantic/status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let semantic_availability = self
            .readiness_snapshot
            .pointer("/semantic/availability")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let fallback_mode = &self.fallback_mode;
        add_check!(
            checks,
            "semantic_model",
            derived_semantic_assets.doctor_check_status.clone(),
            format!(
                "Semantic/model readiness status={semantic_status}, availability={semantic_availability}, fallback_mode={fallback_mode}, asset_class={}, safe_to_rebuild={}, network_allowed=false, blocks_archive_recovery=false",
                doctor_serde_label(derived_semantic_assets.asset_class),
                derived_semantic_assets.safe_to_rebuild
            ),
            derived_semantic_assets.safe_to_rebuild
        );

        self.repair_plan = if self.command_surface == doctor::DoctorCommandSurface::Repair {
            Some(build_doctor_repair_plan_preview(
                &self.data_dir,
                &self.db_path,
                &self.index_path,
                self.output_format,
                self.force_rebuild,
                self.allow_repeated_repair,
                self.execution_mode == doctor::DoctorExecutionMode::RepairDryRun,
                self.execution_mode == doctor::DoctorExecutionMode::FingerprintApply,
                self.requested_plan_fingerprint.clone(),
                &self.operation_state,
                &self.initial_failure_marker,
                self.needs_rebuild,
                self.db_ok,
                self.archive_queryable_for_non_destructive_derived_rebuild,
                self.canonical_candidate_replacement_authorized,
                self.archive_wide_collectors_complete,
                self.candidate_repair_authority_complete,
                self.db_messages,
                &self.raw_mirror_backfill,
                &self.coverage_summary,
                &self.coverage_risk,
                source_authority,
                &self.candidate_staging,
            ))
        } else {
            None
        };
        if let Some(plan) = self.repair_plan.as_mut() {
            if self.execution_mode == doctor::DoctorExecutionMode::RepairDryRun {
                checks.push(Check {
                    name: "repair_plan_approval".to_string(),
                    status: "pass".to_string(),
                    message: format!(
                        "Repair dry-run plan ready; apply with plan fingerprint {}",
                        plan.plan_fingerprint
                    ),
                    fix_available: plan.planned_action_count > 0,
                    fix_applied: false,
                });
            } else if self.execution_mode == doctor::DoctorExecutionMode::FingerprintApply {
                if plan.approval_status == "matched" {
                    if plan.planned_action_count == 0 {
                        plan.apply_authorized = true;
                        plan.will_mutate = false;
                        checks.push(Check {
                            name: "repair_plan_approval".to_string(),
                            status: "pass".to_string(),
                            message: format!(
                                "Repair plan fingerprint matched; no repair actions were selected: {}",
                                plan.plan_fingerprint
                            ),
                            fix_available: false,
                            fix_applied: false,
                        });
                    } else if self.mutating_lock_acquired
                        && self.operation_state.mutating_doctor_allowed
                        && !self.repeat_repair_refused
                    {
                        self.fix_can_mutate = true;
                        plan.apply_authorized = true;
                        plan.will_mutate = plan.planned_action_count > 0;
                        plan.approval_status = "matched".to_string();
                        checks.push(Check {
                            name: "repair_plan_approval".to_string(),
                            status: "pass".to_string(),
                            message: format!(
                                "Repair plan fingerprint matched and mutation lock is held: {}",
                                plan.plan_fingerprint
                            ),
                            fix_available: plan.planned_action_count > 0,
                            fix_applied: false,
                        });
                    } else {
                        match doctor_acquire_mutation_lock(&self.data_dir, &self.db_path) {
                            Ok((guard, observation)) => {
                                self._doctor_lock_guard = Some(guard);
                                self._doctor_db_open_bypass_guard = Some(
                                    crate::storage::sqlite::enter_doctor_mutation_db_open_bypass(),
                                );
                                self.operation_state = build_doctor_operation_state_report(
                                    &self.data_dir,
                                    &self.db_path,
                                    &self.index_path,
                                    &self.maintenance_snapshot,
                                    &observation,
                                );
                                let lock_acquired = matches!(
                                    observation,
                                    DoctorMutationLockObservation::Acquired { .. }
                                );
                                if self.operation_state.mutating_doctor_allowed
                                    && lock_acquired
                                    && !self.repeat_repair_refused
                                {
                                    self.fix_can_mutate = true;
                                    plan.apply_authorized = true;
                                    plan.will_mutate = plan.planned_action_count > 0;
                                    plan.approval_status = "matched".to_string();
                                    checks.push(Check {
                                        name: "repair_plan_approval".to_string(),
                                        status: "pass".to_string(),
                                        message: format!(
                                            "Repair plan fingerprint matched and mutation lock was acquired: {}",
                                            plan.plan_fingerprint
                                        ),
                                        fix_available: plan.planned_action_count > 0,
                                        fix_applied: false,
                                    });
                                } else {
                                    let reason = self
                                        .operation_state
                                        .mutation_blocked_reason
                                        .clone()
                                        .unwrap_or_else(|| {
                                            "doctor mutation lock was not acquired after plan revalidation"
                                                .to_string()
                                        });
                                    plan.apply_authorized = false;
                                    plan.will_mutate = false;
                                    plan.approval_status = "lock_uncertain".to_string();
                                    if !plan
                                        .branchable_blocker_codes
                                        .iter()
                                        .any(|code| code == "lock-uncertain")
                                    {
                                        plan.branchable_blocker_codes
                                            .push("lock-uncertain".to_string());
                                    }
                                    plan.blocked_reasons
                                        .push(format!("lock-uncertain: {reason}"));
                                    checks.push(Check {
                                        name: "repair_plan_approval".to_string(),
                                        status: "fail".to_string(),
                                        message: format!(
                                            "Repair plan fingerprint matched, but apply was blocked before mutation: {reason}"
                                        ),
                                        fix_available: false,
                                        fix_applied: false,
                                    });
                                }
                            }
                            Err(observation) => {
                                self.operation_state = build_doctor_operation_state_report(
                                    &self.data_dir,
                                    &self.db_path,
                                    &self.index_path,
                                    &self.maintenance_snapshot,
                                    &observation,
                                );
                                let reason = self
                                    .operation_state
                                    .mutation_blocked_reason
                                    .clone()
                                    .unwrap_or_else(|| {
                                        "doctor mutation lock could not be acquired after plan revalidation"
                                            .to_string()
                                    });
                                plan.apply_authorized = false;
                                plan.will_mutate = false;
                                plan.approval_status = "lock_uncertain".to_string();
                                if !plan
                                    .branchable_blocker_codes
                                    .iter()
                                    .any(|code| code == "lock-uncertain")
                                {
                                    plan.branchable_blocker_codes
                                        .push("lock-uncertain".to_string());
                                }
                                plan.blocked_reasons
                                    .push(format!("lock-uncertain: {reason}"));
                                checks.push(Check {
                                    name: "repair_plan_approval".to_string(),
                                    status: "fail".to_string(),
                                    message: format!(
                                        "Repair plan fingerprint matched, but apply was blocked before mutation: {reason}"
                                    ),
                                    fix_available: false,
                                    fix_applied: false,
                                });
                            }
                        }
                    }
                } else {
                    let reason = if plan.provided_plan_fingerprint.is_none() {
                        "approval-fingerprint-missing"
                    } else if plan.approval_status == "blocked" {
                        "plan-blocked"
                    } else {
                        "approval-fingerprint-mismatched"
                    };
                    if !plan
                        .branchable_blocker_codes
                        .iter()
                        .any(|code| code == reason)
                    {
                        plan.branchable_blocker_codes.push(reason.to_string());
                    }
                    checks.push(Check {
                        name: "repair_plan_approval".to_string(),
                        status: "fail".to_string(),
                        message: format!(
                            "Repair apply refused before mutation: {reason}; rerun dry-run and apply the exact current fingerprint"
                        ),
                        fix_available: false,
                        fix_applied: false,
                    });
                }
            }
        }

        self.candidate_promotion_plan_requested = self.repair_plan.as_ref().is_some_and(|plan| {
            plan.apply_authorized
                && doctor_repair_plan_has_action(
                    plan,
                    DOCTOR_REPAIR_ACTION_PROMOTE_RECONSTRUCT_CANDIDATE,
                )
        });
        // Defense in depth: even a malformed/stale plan object must satisfy all
        // live replacement-authority gates again immediately before mutation.
        let candidate_promotion_live_blocker = if self
            .archive_queryable_for_non_destructive_derived_rebuild
        {
            Some((
                "canonical-archive-queryable",
                "candidate promotion refused because bounded canonical archive reads succeeded; deferred deep integrity is not replacement authority",
            ))
        } else if !self.canonical_candidate_replacement_authorized {
            Some((
                "canonical-archive-failure-unproven",
                "candidate promotion refused because no affirmative current integrity failure or absent canonical DB authorizes replacement",
            ))
        } else if !self.candidate_repair_authority_complete {
            Some((
                "archive-authority-collectors-unchecked",
                "candidate promotion refused because archive-wide coverage and source-authority collectors are unchecked",
            ))
        } else {
            None
        };
        let candidate_promotion_blocked_live =
            self.candidate_promotion_plan_requested && candidate_promotion_live_blocker.is_some();
        if candidate_promotion_blocked_live {
            self.fix_can_mutate = false;
            if let Some(plan) = self.repair_plan.as_mut() {
                plan.apply_authorized = false;
                plan.will_mutate = false;
                plan.approval_status = "blocked".to_string();
                let (code, reason) = candidate_promotion_live_blocker
                    .expect("live blocker must exist when candidate promotion is blocked");
                doctor_repair_push_blocker(
                    &mut plan.blocked_reasons,
                    &mut plan.branchable_blocker_codes,
                    code,
                    reason.to_string(),
                );
            }
            checks.push(Check {
                name: "candidate_promotion".to_string(),
                status: "fail".to_string(),
                message: candidate_promotion_live_blocker
                    .map(|(_, reason)| reason)
                    .unwrap_or("candidate promotion refused by live safety gates")
                    .to_string(),
                fix_available: false,
                fix_applied: false,
            });
        }
        self.candidate_promotion_apply_requested =
            self.candidate_promotion_plan_requested && !candidate_promotion_blocked_live;
        derived_semantic_assets
    }
}
