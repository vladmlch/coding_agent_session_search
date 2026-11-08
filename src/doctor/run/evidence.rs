use super::{Check, DoctorRun, add_check};
use crate::doctor::policy::DoctorArtifactChecksumStatus;
use crate::doctor::raw_mirror::doctorrawmirrorverificationmode::DoctorRawMirrorReport;
use crate::doctor::repair_state::cliheartbeat::doctor_push_timing_span;
use crate::doctor::repair_state::types::DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS;
use crate::doctor::report::coverage::{
    build_doctor_source_authority_report, doctor_refuse_source_authority_when_collectors_deferred,
};
use crate::doctor::report::inventory::{
    DoctorSourceAuthorityDecision, DoctorSourceAuthorityKind, DoctorSourceAuthorityReport,
};
use std::time::Instant;

#[allow(clippy::collapsible_if, clippy::collapsible_else_if)]
impl DoctorRun {
    /// Source-authority evidence: select (or refuse, when collectors were
    /// deferred) the repair authority matrix, derive
    /// `candidate_repair_authority_complete`, and emit the source_authority
    /// check. Returns the authority report the `candidate`/`repair_plan`/
    /// `report` phases consume. Original lines run verbatim in order.
    pub(crate) fn evidence(
        &mut self,
        raw_mirror: &DoctorRawMirrorReport,
    ) -> DoctorSourceAuthorityReport {
        let checks = &mut self.checks;
        let source_authority_started = Instant::now();
        let source_authority = {
            let report = build_doctor_source_authority_report(
                &self.db_path,
                &self.source_inventory,
                raw_mirror,
            );
            if let Some(reason) = self.recovery_evidence_deferred_reason.as_deref() {
                doctor_refuse_source_authority_when_collectors_deferred(
                    report,
                    reason,
                    self.storage_integrity_failed,
                )
            } else {
                report
            }
        };
        self.candidate_repair_authority_complete = self.recovery_evidence_collectors_complete
            || (self.storage_integrity_failed
                && source_authority
                    .selected_authorities
                    .iter()
                    .any(|authority| {
                        matches!(
                            authority.authority,
                            DoctorSourceAuthorityKind::VerifiedRawMirror
                        ) && matches!(
                            authority.decision,
                            DoctorSourceAuthorityDecision::CandidateOnly
                        ) && matches!(
                            authority.checksum_status,
                            DoctorArtifactChecksumStatus::Matched
                        )
                    }));
        doctor_push_timing_span(
            &mut self.timing_spans,
            "source_authority",
            "source_authority",
            "source_authority",
            source_authority_started,
            DOCTOR_SLOW_OPERATION_DEFAULT_THRESHOLD_MS,
            vec!["source-authority matrix selected or rejected repair authorities".to_string()],
        );
        if let Some(reason) = self.recovery_evidence_deferred_reason.as_deref() {
            if self.candidate_repair_authority_complete {
                add_check!(
                    checks,
                    "source_authority",
                    "warn",
                    format!(
                        "Archive-wide coverage authority is deferred, but checksum-verified raw mirror evidence may seed an isolated candidate after affirmative canonical corruption; promotion remains independently gated; {reason}"
                    ),
                    true
                );
            } else {
                add_check!(
                    checks,
                    "source_authority",
                    "warn",
                    format!("No archive repair authority was selected; {reason}"),
                    false
                );
            }
        }
        source_authority
    }
}
