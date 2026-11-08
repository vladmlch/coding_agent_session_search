use crate::doctor::forensic::doctor_canonical_blake3;
use crate::doctor::policy::assets::doctor_asset_policy;
use crate::doctor::policy::types::{
    DoctorAnomaly, DoctorAnomalyPolicy, DoctorAnomalyTaxonomyEntry, DoctorAssetClass,
    DoctorDataLossRisk, DoctorHealth, DoctorSeverity,
};
use crate::doctor::policy::{
    DoctorOperationOutcomeKind, DoctorOperationOutcomeReport, DoctorPostRepairProbeSuiteReport,
    DoctorRepairOutcomeKind, doctor_operation_outcome_with_details,
};
use crate::doctor::repair_state::cliheartbeat::doctor_serde_label;
use crate::doctor::repair_state::finalize_cleanup_apply_contract::DiagCleanupApplyResult;
use crate::doctor::repair_state::types::DoctorOperationStateReport;
use crate::doctor::report::inventory::{
    DoctorCandidatePromotionReport, DoctorCandidateStagingReport, DoctorCoverageRiskSummary,
};
use crate::doctor::report::types::{
    DoctorCheckReport, DoctorIncidentRootCauseKind, DoctorRootCauseIncident,
    DoctorSafeAutoRunBuildInput, DoctorSafeAutoRunDecisionKind, DoctorSafeAutoRunFinding,
    DoctorSafeAutoRunReceiptReference, DoctorSafeAutoRunReport,
};
use crate::doctor_recover;
use crate::franken_sync::compat::ConnectionExt;
use anyhow::Result;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub(crate) struct DoctorDatabaseIntegrityProbe {
    pub(crate) quick_check_status: String,
    pub(crate) integrity_check_diagnostics: Vec<String>,
}

pub(crate) const DOCTOR_DATABASE_INTEGRITY_DIAGNOSTIC_LIMIT: usize = 20;

pub(crate) const DOCTOR_ANOMALY_POLICY_TABLE: &[DoctorAnomalyPolicy] = &[
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::Healthy,
        health_class: DoctorHealth::Healthy,
        severity: DoctorSeverity::Info,
        affected_asset_class: DoctorAssetClass::Unknown,
        data_loss_risk: DoctorDataLossRisk::None,
        default_outcome_kind: DoctorRepairOutcomeKind::NoOp,
        safe_for_auto_repair: false,
        recommended_action: "none",
    },
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::DegradedDerivedAssets,
        health_class: DoctorHealth::DegradedDerivedAssets,
        severity: DoctorSeverity::Warn,
        affected_asset_class: DoctorAssetClass::ReclaimableDerivedCache,
        data_loss_risk: DoctorDataLossRisk::None,
        default_outcome_kind: DoctorRepairOutcomeKind::Planned,
        safe_for_auto_repair: true,
        recommended_action: "rebuild-or-clean-derived-assets",
    },
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::DegradedArchiveRisk,
        health_class: DoctorHealth::DegradedArchiveRisk,
        severity: DoctorSeverity::Error,
        affected_asset_class: DoctorAssetClass::CanonicalArchiveDb,
        data_loss_risk: DoctorDataLossRisk::High,
        default_outcome_kind: DoctorRepairOutcomeKind::Blocked,
        safe_for_auto_repair: false,
        recommended_action: "preserve-evidence-and-plan-archive-recovery",
    },
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::RepairBlocked,
        health_class: DoctorHealth::RepairBlocked,
        severity: DoctorSeverity::Warn,
        affected_asset_class: DoctorAssetClass::OperationReceipt,
        data_loss_risk: DoctorDataLossRisk::Unknown,
        default_outcome_kind: DoctorRepairOutcomeKind::Blocked,
        safe_for_auto_repair: false,
        recommended_action: "inspect-blocker-before-retrying",
    },
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::RepairPreviouslyFailed,
        health_class: DoctorHealth::RepairPreviouslyFailed,
        severity: DoctorSeverity::Error,
        affected_asset_class: DoctorAssetClass::OperationReceipt,
        data_loss_risk: DoctorDataLossRisk::Medium,
        default_outcome_kind: DoctorRepairOutcomeKind::Failed,
        safe_for_auto_repair: false,
        recommended_action: "inspect-failure-marker-before-repair",
    },
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::SourceAuthorityUnsafe,
        health_class: DoctorHealth::SourceAuthorityUnsafe,
        severity: DoctorSeverity::Warn,
        affected_asset_class: DoctorAssetClass::ExternalUpstreamSource,
        data_loss_risk: DoctorDataLossRisk::Medium,
        default_outcome_kind: DoctorRepairOutcomeKind::Blocked,
        safe_for_auto_repair: false,
        recommended_action: "verify-source-authority-before-rebuild",
    },
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::ArchiveDbCorrupt,
        health_class: DoctorHealth::DegradedArchiveRisk,
        severity: DoctorSeverity::Error,
        affected_asset_class: DoctorAssetClass::CanonicalArchiveDb,
        data_loss_risk: DoctorDataLossRisk::High,
        default_outcome_kind: DoctorRepairOutcomeKind::Blocked,
        safe_for_auto_repair: false,
        recommended_action: "capture-backup-and-reconstruct-from-verified-authority",
    },
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::ArchiveDbUnreadable,
        health_class: DoctorHealth::DegradedArchiveRisk,
        severity: DoctorSeverity::Error,
        affected_asset_class: DoctorAssetClass::CanonicalArchiveDb,
        data_loss_risk: DoctorDataLossRisk::High,
        default_outcome_kind: DoctorRepairOutcomeKind::Blocked,
        safe_for_auto_repair: false,
        recommended_action: "inspect-archive-db-and-preserve-sidecars",
    },
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::RawMirrorMissing,
        health_class: DoctorHealth::DegradedArchiveRisk,
        severity: DoctorSeverity::Warn,
        affected_asset_class: DoctorAssetClass::RawMirrorBlob,
        data_loss_risk: DoctorDataLossRisk::Medium,
        default_outcome_kind: DoctorRepairOutcomeKind::Blocked,
        safe_for_auto_repair: false,
        recommended_action: "capture-or-backfill-raw-mirror-before-risky-repair",
    },
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::RawMirrorBehindSource,
        health_class: DoctorHealth::DegradedArchiveRisk,
        severity: DoctorSeverity::Warn,
        affected_asset_class: DoctorAssetClass::RawMirrorBlob,
        data_loss_risk: DoctorDataLossRisk::Medium,
        default_outcome_kind: DoctorRepairOutcomeKind::Planned,
        safe_for_auto_repair: false,
        recommended_action: "refresh-raw-mirror-before-using-source-authority",
    },
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::UpstreamSourcePruned,
        health_class: DoctorHealth::DegradedArchiveRisk,
        severity: DoctorSeverity::Warn,
        affected_asset_class: DoctorAssetClass::ExternalUpstreamSource,
        data_loss_risk: DoctorDataLossRisk::High,
        default_outcome_kind: DoctorRepairOutcomeKind::Blocked,
        safe_for_auto_repair: false,
        recommended_action: "use-cass-archive-or-raw-mirror-as-authority",
    },
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::DerivedLexicalStale,
        health_class: DoctorHealth::DegradedDerivedAssets,
        severity: DoctorSeverity::Warn,
        affected_asset_class: DoctorAssetClass::DerivedLexicalIndex,
        data_loss_risk: DoctorDataLossRisk::None,
        default_outcome_kind: DoctorRepairOutcomeKind::Planned,
        safe_for_auto_repair: true,
        recommended_action: "rebuild-derived-lexical-index",
    },
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::DerivedSemanticStale,
        health_class: DoctorHealth::DegradedDerivedAssets,
        severity: DoctorSeverity::Warn,
        affected_asset_class: DoctorAssetClass::DerivedSemanticIndex,
        data_loss_risk: DoctorDataLossRisk::None,
        default_outcome_kind: DoctorRepairOutcomeKind::Planned,
        safe_for_auto_repair: false,
        recommended_action: "refresh-semantic-assets-when-model-is-available",
    },
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::InterruptedRepair,
        health_class: DoctorHealth::RepairBlocked,
        severity: DoctorSeverity::Warn,
        affected_asset_class: DoctorAssetClass::OperationReceipt,
        data_loss_risk: DoctorDataLossRisk::Medium,
        default_outcome_kind: DoctorRepairOutcomeKind::Blocked,
        safe_for_auto_repair: false,
        recommended_action: "inspect-interrupted-repair-artifacts",
    },
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::LockContention,
        health_class: DoctorHealth::RepairBlocked,
        severity: DoctorSeverity::Warn,
        affected_asset_class: DoctorAssetClass::OperationReceipt,
        data_loss_risk: DoctorDataLossRisk::Low,
        default_outcome_kind: DoctorRepairOutcomeKind::Blocked,
        safe_for_auto_repair: false,
        recommended_action: "wait-or-inspect-active-owner-before-repair",
    },
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::StoragePressure,
        health_class: DoctorHealth::RepairBlocked,
        severity: DoctorSeverity::Warn,
        affected_asset_class: DoctorAssetClass::Unknown,
        data_loss_risk: DoctorDataLossRisk::Medium,
        default_outcome_kind: DoctorRepairOutcomeKind::Blocked,
        safe_for_auto_repair: false,
        recommended_action: "free-space-without-deleting-archive-evidence",
    },
    DoctorAnomalyPolicy {
        // A blocked full rebuild is a capacity fact, not a health defect:
        // incremental indexing and search keep working, nothing is lost, and
        // the indexer already refuses before writing. Health stays `healthy`
        // (risk `low` via the warn), and the numbers live in
        // `storage_pressure.full_rebuild_readiness`.
        anomaly_class: DoctorAnomaly::FullRebuildHeadroom,
        health_class: DoctorHealth::Healthy,
        severity: DoctorSeverity::Warn,
        affected_asset_class: DoctorAssetClass::DerivedLexicalIndex,
        data_loss_risk: DoctorDataLossRisk::None,
        default_outcome_kind: DoctorRepairOutcomeKind::Blocked,
        safe_for_auto_repair: false,
        recommended_action: "free-disk-headroom-before-full-rebuild",
    },
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::ConfigExclusionRisk,
        health_class: DoctorHealth::SourceAuthorityUnsafe,
        severity: DoctorSeverity::Warn,
        affected_asset_class: DoctorAssetClass::UserConfig,
        data_loss_risk: DoctorDataLossRisk::Medium,
        default_outcome_kind: DoctorRepairOutcomeKind::Blocked,
        safe_for_auto_repair: false,
        recommended_action: "inspect-config-before-trusting-source-coverage",
    },
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::BackupUnverified,
        health_class: DoctorHealth::DegradedArchiveRisk,
        severity: DoctorSeverity::Warn,
        affected_asset_class: DoctorAssetClass::BackupBundle,
        data_loss_risk: DoctorDataLossRisk::High,
        default_outcome_kind: DoctorRepairOutcomeKind::Blocked,
        safe_for_auto_repair: false,
        recommended_action: "verify-backup-before-restore-or-cleanup",
    },
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::BackupStale,
        health_class: DoctorHealth::DegradedArchiveRisk,
        severity: DoctorSeverity::Warn,
        affected_asset_class: DoctorAssetClass::BackupBundle,
        data_loss_risk: DoctorDataLossRisk::Medium,
        default_outcome_kind: DoctorRepairOutcomeKind::Planned,
        safe_for_auto_repair: false,
        recommended_action: "create-or-verify-current-backup-before-repair",
    },
    DoctorAnomalyPolicy {
        anomaly_class: DoctorAnomaly::PrivacyRedactionRequired,
        health_class: DoctorHealth::DegradedArchiveRisk,
        severity: DoctorSeverity::Error,
        affected_asset_class: DoctorAssetClass::SupportBundle,
        data_loss_risk: DoctorDataLossRisk::High,
        default_outcome_kind: DoctorRepairOutcomeKind::Blocked,
        safe_for_auto_repair: false,
        recommended_action: "redact-sensitive-output-before-sharing-artifacts",
    },
];

pub(crate) fn doctor_anomaly_policy(anomaly_class: DoctorAnomaly) -> &'static DoctorAnomalyPolicy {
    DOCTOR_ANOMALY_POLICY_TABLE
        .iter()
        .find(|policy| policy.anomaly_class == anomaly_class)
        .expect("doctor anomaly policy table must cover every anomaly")
}

pub(crate) fn doctor_anomaly_taxonomy_report() -> Vec<DoctorAnomalyTaxonomyEntry> {
    DOCTOR_ANOMALY_POLICY_TABLE
        .iter()
        .map(|policy| DoctorAnomalyTaxonomyEntry {
            anomaly_class: policy.anomaly_class,
            health_class: policy.health_class,
            severity: policy.severity,
            affected_asset_class: policy.affected_asset_class,
            data_loss_risk: policy.data_loss_risk,
            default_outcome_kind: policy.default_outcome_kind,
            safe_for_auto_repair: policy.safe_for_auto_repair,
            recommended_action: policy.recommended_action,
        })
        .collect()
}

/// Minimal synchronous read surface shared by same-thread raw test fixtures
/// and production dedicated-owner handles. The production hard-timeout path
/// implements this only through `FrankenOwnerConnection`, so no raw engine
/// value becomes reachable across workers.
pub(crate) trait DoctorArchiveReadConnection {
    fn doctor_query(
        &self,
        sql: &str,
    ) -> Result<Vec<crate::franken_sync::Row>, crate::franken_sync::FrankenError>;

    fn doctor_query_row_map<T, F>(
        &self,
        sql: &str,
        params: &[crate::franken_sync::compat::ParamValue],
        map: F,
    ) -> Result<T, crate::franken_sync::FrankenError>
    where
        F: FnOnce(&crate::franken_sync::Row) -> Result<T, crate::franken_sync::FrankenError>;
}

impl DoctorArchiveReadConnection for crate::franken_sync::Connection {
    fn doctor_query(
        &self,
        sql: &str,
    ) -> Result<Vec<crate::franken_sync::Row>, crate::franken_sync::FrankenError> {
        self.query(sql)
    }

    fn doctor_query_row_map<T, F>(
        &self,
        sql: &str,
        params: &[crate::franken_sync::compat::ParamValue],
        map: F,
    ) -> Result<T, crate::franken_sync::FrankenError>
    where
        F: FnOnce(&crate::franken_sync::Row) -> Result<T, crate::franken_sync::FrankenError>,
    {
        self.query_row_map(sql, params, map)
    }
}

impl DoctorArchiveReadConnection for crate::storage::sqlite::FrankenOwnerConnection {
    fn doctor_query(
        &self,
        sql: &str,
    ) -> Result<Vec<crate::franken_sync::Row>, crate::franken_sync::FrankenError> {
        self.query(sql)
    }

    fn doctor_query_row_map<T, F>(
        &self,
        sql: &str,
        params: &[crate::franken_sync::compat::ParamValue],
        map: F,
    ) -> Result<T, crate::franken_sync::FrankenError>
    where
        F: FnOnce(&crate::franken_sync::Row) -> Result<T, crate::franken_sync::FrankenError>,
    {
        self.query_row_map(sql, params, map)
    }
}

pub(crate) fn doctor_quick_check_status(
    rows: Vec<crate::franken_sync::Row>,
) -> Result<String, String> {
    use crate::franken_sync::compat::RowExt as _;

    if rows.is_empty() {
        return Err("PRAGMA quick_check(1) returned no diagnostic rows".to_string());
    }
    let mut diagnostics = Vec::new();
    let mut omitted = 0;
    for row in rows {
        let detail: String = row
            .get_typed(0)
            .map_err(|err| format!("reading PRAGMA quick_check output: {err}"))?;
        let detail = detail.trim();
        if detail.is_empty() {
            return Err("PRAGMA quick_check(1) returned an empty diagnostic".to_string());
        }
        if detail.eq_ignore_ascii_case("ok") {
            continue;
        }
        if diagnostics.len() < DOCTOR_DATABASE_INTEGRITY_DIAGNOSTIC_LIMIT {
            diagnostics.push(detail.to_string());
        } else {
            omitted += 1;
        }
    }
    if diagnostics.is_empty() {
        return Ok("ok".to_string());
    }
    if omitted > 0 {
        diagnostics.push(format!("{omitted} additional diagnostic row(s) omitted"));
    }
    Ok(diagnostics.join("; "))
}

pub(crate) fn doctor_database_quick_check<C: DoctorArchiveReadConnection>(
    conn: &C,
) -> Result<String, String> {
    // Some engine versions return multiple findings despite the requested
    // limit. Preserve those findings instead of replacing them with a row-count
    // error. Inspect every row before declaring health; bound only the output.
    let rows = conn
        .doctor_query("PRAGMA quick_check(1)")
        .map_err(|err| format!("running PRAGMA quick_check(1): {err}"))?;
    doctor_quick_check_status(rows)
}

#[test]
pub(crate) fn doctor_quick_check_preserves_multiple_findings_and_bounds_output() {
    let conn = crate::franken_sync::Connection::open(":memory:").unwrap();
    assert_eq!(doctor_database_quick_check(&conn).unwrap(), "ok");
    // Actual engine rows exercise the diagnostic decoder; these SELECTs model
    // the response protocol, not a claim that this database is corrupt.
    let rows = conn
        .query(
            "SELECT 'ok' UNION ALL SELECT 'page 7 never used' UNION ALL SELECT 'page 9 never used'",
        )
        .unwrap();
    assert_eq!(
        doctor_quick_check_status(rows).unwrap(),
        "page 7 never used; page 9 never used"
    );
    let mut terms = vec!["SELECT 'ok'"; DOCTOR_DATABASE_INTEGRITY_DIAGNOSTIC_LIMIT + 1];
    terms.push("SELECT 'late failure'");
    assert_eq!(
        doctor_quick_check_status(conn.query(&terms.join(" UNION ALL ")).unwrap()).unwrap(),
        "late failure"
    );
    let terms = vec!["SELECT 'page failure'"; DOCTOR_DATABASE_INTEGRITY_DIAGNOSTIC_LIMIT + 2];
    let status =
        doctor_quick_check_status(conn.query(&terms.join(" UNION ALL ")).unwrap()).unwrap();
    assert_eq!(
        status.matches("page failure").count(),
        DOCTOR_DATABASE_INTEGRITY_DIAGNOSTIC_LIMIT
    );
    assert!(status.ends_with("2 additional diagnostic row(s) omitted"));
    assert!(doctor_quick_check_status(conn.query("SELECT 'ok' WHERE 0").unwrap()).is_err());
    assert!(doctor_quick_check_status(conn.query("SELECT ''").unwrap()).is_err());
}

pub(crate) fn doctor_database_integrity_probe<C: DoctorArchiveReadConnection>(
    conn: &C,
    set_phase: impl Fn(&'static str),
) -> Result<DoctorDatabaseIntegrityProbe, String> {
    use crate::franken_sync::compat::RowExt as _;

    set_phase("quick_check");
    let quick_check_status = doctor_database_quick_check(conn)?;

    let quick_check_ok = quick_check_status.trim().eq_ignore_ascii_case("ok");
    let integrity_check_diagnostics = if quick_check_ok {
        set_phase("integrity_check");
        let integrity_sql =
            format!("PRAGMA integrity_check({DOCTOR_DATABASE_INTEGRITY_DIAGNOSTIC_LIMIT});");
        let rows = conn
            .doctor_query(&integrity_sql)
            .map_err(|err| format!("running PRAGMA integrity_check: {err}"))?;
        let mut diagnostics = Vec::new();
        for row in rows {
            let detail: String = row
                .get_typed(0)
                .map_err(|err| format!("reading PRAGMA integrity_check output: {err}"))?;
            let detail = detail.trim();
            if detail.eq_ignore_ascii_case("ok") || detail.is_empty() {
                continue;
            }
            diagnostics.push(detail.to_string());
            if diagnostics.len() >= DOCTOR_DATABASE_INTEGRITY_DIAGNOSTIC_LIMIT {
                break;
            }
        }
        diagnostics
    } else {
        Vec::new()
    };

    Ok(DoctorDatabaseIntegrityProbe {
        quick_check_status,
        integrity_check_diagnostics,
    })
}

impl DoctorDatabaseIntegrityProbe {
    pub(crate) fn is_ok(&self) -> bool {
        self.quick_check_status.trim().eq_ignore_ascii_case("ok")
            && self.integrity_check_diagnostics.is_empty()
    }

    pub(crate) fn failed_pragma_name(&self) -> &'static str {
        if self.quick_check_status.trim().eq_ignore_ascii_case("ok") {
            "integrity_check"
        } else {
            "quick_check"
        }
    }

    pub(crate) fn diagnostic_summary(&self) -> String {
        if !self.quick_check_status.trim().eq_ignore_ascii_case("ok") {
            return self.quick_check_status.trim().to_string();
        }
        let mut summary = self
            .integrity_check_diagnostics
            .iter()
            .take(3)
            .cloned()
            .collect::<Vec<_>>()
            .join("; ");
        let omitted = self.integrity_check_diagnostics.len().saturating_sub(3);
        if omitted > 0 {
            if !summary.is_empty() {
                summary.push_str("; ");
            }
            summary.push_str(&format!("{omitted} additional diagnostic row(s) omitted"));
        }
        if summary.is_empty() {
            "integrity diagnostics were empty".to_string()
        } else {
            summary
        }
    }
}

pub(crate) fn doctor_anomaly_for_check(name: &str, status: &str, message: &str) -> DoctorAnomaly {
    if status == "pass" {
        return DoctorAnomaly::Healthy;
    }

    if status == "timeout" {
        return DoctorAnomaly::RepairBlocked;
    }

    // An affirmative integrity verdict outranks any accompanying deferral
    // context (for example, a current cached FAIL used when a live deep probe
    // is too large).  This is evidence of corruption, not uncertainty.
    let affirmative_database_integrity_failure = name == "database"
        && status == "fail"
        && (message.starts_with("Database failed frankensqlite ")
            || message.contains("attestation records structural failure"));
    if affirmative_database_integrity_failure {
        return DoctorAnomaly::ArchiveDbCorrupt;
    }

    // A deliberately deferred, known-unbounded integrity/FTS probe is not
    // evidence that the archive is corrupt or unreadable.  Keep the robot
    // taxonomy honest: this is a blocked diagnostic, not an archive verdict.
    if matches!(status, "warn" | "fail")
        && (message.contains("frankensqlite_full_page_integrity_probe_deferred")
            || message.contains("archive_wide_collectors_deferred")
            || message.contains("busy or locked")
            || message.contains("health is unverified")
            || message.contains("structural integrity is unchecked")
            || message.contains("health probe failed via frankensqlite")
            || message.contains("no integrity verdict was returned")
            || message == "Database query failed")
    {
        return DoctorAnomaly::RepairBlocked;
    }

    match name {
        "data_directory" => DoctorAnomaly::StoragePressure,
        "storage_pressure" => DoctorAnomaly::StoragePressure,
        "full_rebuild_readiness" => DoctorAnomaly::FullRebuildHeadroom,
        "lock_file" => DoctorAnomaly::LockContention,
        "operation_state" => {
            if message.contains("interrupted") {
                DoctorAnomaly::InterruptedRepair
            } else {
                DoctorAnomaly::LockContention
            }
        }
        "database" => {
            if affirmative_database_integrity_failure {
                DoctorAnomaly::ArchiveDbCorrupt
            } else {
                DoctorAnomaly::ArchiveDbUnreadable
            }
        }
        "database_backup" => DoctorAnomaly::BackupUnverified,
        "safe_auto_archive_rebuild" => DoctorAnomaly::DegradedArchiveRisk,
        "fts_table" | "index" | "index_sync" => DoctorAnomaly::DerivedLexicalStale,
        "semantic_model" => DoctorAnomaly::DerivedSemanticStale,
        "rebuild" => DoctorAnomaly::RepairPreviouslyFailed,
        "repair_failure_marker" => DoctorAnomaly::RepairPreviouslyFailed,
        "post_repair_probes" => DoctorAnomaly::RepairPreviouslyFailed,
        "derivative_cleanup" => DoctorAnomaly::DegradedDerivedAssets,
        "legacy_omp_analytics" => DoctorAnomaly::DegradedDerivedAssets,
        "config" | "sources_config" | "config_exclusion_risks" => {
            DoctorAnomaly::ConfigExclusionRisk
        }
        "sessions" => DoctorAnomaly::SourceAuthorityUnsafe,
        "source_inventory" => {
            if message.contains("no longer have a visible upstream file")
                || message.contains("Source coverage risk")
            {
                DoctorAnomaly::UpstreamSourcePruned
            } else if message.contains("could not query archive coverage") {
                DoctorAnomaly::ArchiveDbUnreadable
            } else {
                DoctorAnomaly::SourceAuthorityUnsafe
            }
        }
        "raw_mirror" => {
            if message.contains("interrupted capture") {
                DoctorAnomaly::InterruptedRepair
            } else {
                DoctorAnomaly::RawMirrorMissing
            }
        }
        "raw_mirror_backfill" => {
            if message.contains("DB-only") || message.contains("sole remaining copy") {
                DoctorAnomaly::UpstreamSourcePruned
            } else {
                DoctorAnomaly::RawMirrorMissing
            }
        }
        "candidate_staging" => {
            if message.contains("orphaned") || message.contains("interrupted") {
                DoctorAnomaly::InterruptedRepair
            } else {
                DoctorAnomaly::DegradedArchiveRisk
            }
        }
        "coverage_comparison_gate" => DoctorAnomaly::DegradedArchiveRisk,
        "source_coverage" => {
            if message.contains("sole-copy") {
                DoctorAnomaly::UpstreamSourcePruned
            } else if message.contains("CASS_RAW_MIRROR") {
                // GH #506: an operator setting reduces coverage, like
                // CASS_EXCLUDE_PATHS; nothing about the archive is unsafe.
                DoctorAnomaly::ConfigExclusionRisk
            } else {
                DoctorAnomaly::SourceAuthorityUnsafe
            }
        }
        _ => DoctorAnomaly::RepairBlocked,
    }
}

/// #438: queryability does not establish FTS segment integrity. The historical
/// disagreement was a real writer-format defect (frankensqlite#404), even
/// though some MATCH queries succeeded. A newer engine does not establish
/// that existing derived segments have been rewritten into a valid format.
pub(crate) const DOCTOR_FTS_TABLE_QUERYABLE_MESSAGE: &str = concat!(
    "FTS search table (fts_messages) is queryable via frankensqlite. ",
    "This bounded check does not validate FTS segment structure or every MATCH result. ",
    "Stock SQLite integrity errors must be investigated; successful reads do not rule out ",
    "the legacy FTS writer-format defect (cass#438, frankensqlite#404). ",
    "Preserve the archive and validate canonical rows before repair. The FTS shadow is derived: ",
    "`cass doctor --rebuild-canonical-fts --yes` explicitly regenerates it from canonical rows ",
    "and may require substantial memory on large archives."
);

pub(crate) fn doctor_check_report(
    name: &str,
    status: &str,
    message: &str,
    fix_available: bool,
    fix_applied: bool,
) -> DoctorCheckReport {
    let anomaly_class = doctor_anomaly_for_check(name, status, message);
    let policy = doctor_anomaly_policy(anomaly_class);
    DoctorCheckReport {
        name: name.to_string(),
        status: status.to_string(),
        message: message.to_string(),
        anomaly_class,
        health_class: policy.health_class,
        severity: policy.severity,
        affected_asset_class: policy.affected_asset_class,
        data_loss_risk: policy.data_loss_risk,
        recommended_action: policy.recommended_action,
        safe_for_auto_repair: policy.safe_for_auto_repair && fix_available && status != "pass",
        default_outcome_kind: policy.default_outcome_kind,
        fix_available,
        fix_applied,
    }
}

pub(crate) fn doctor_safe_auto_action_for_check(check: &DoctorCheckReport) -> &'static str {
    match check.name.as_str() {
        "data_directory" => "create_missing_cass_data_dir",
        "lock_file" => "remove_stale_legacy_index_lock",
        "index" | "index_sync" | "fts_table" | "rebuild" => {
            "rebuild_derived_lexical_index_from_archive_db"
        }
        "semantic_model" => "report_lexical_fallback_without_model_download",
        "raw_mirror_backfill" => "backfill_additive_raw_mirror_metadata",
        "candidate_staging" => "build_isolated_reconstruct_candidate",
        "safe_auto_archive_rebuild" => "archive_rebuild_from_sources",
        "database" => "archive_database_repair",
        "database_backup" => "archive_database_bundle_move_or_backup",
        "source_coverage" => "source_coverage_repair",
        "source_inventory" => "source_authority_repair",
        "repair_failure_marker" => "repeat_repair",
        "operation_state" => "mutating_doctor_repair",
        "derivative_cleanup" => "derived_cleanup",
        _ => "manual_doctor_review",
    }
}

pub(crate) fn doctor_safe_auto_manual_next_command(check: &DoctorCheckReport) -> &'static str {
    match check.name.as_str() {
        "safe_auto_archive_rebuild" | "database" | "database_backup" => {
            "cass doctor repair --dry-run --json"
        }
        "repair_failure_marker" => "cass doctor --json",
        "operation_state" | "lock_file" => "cass doctor --json",
        "source_coverage" | "source_inventory" | "raw_mirror" | "raw_mirror_backfill" => {
            "cass doctor archive-scan --json"
        }
        "candidate_staging" | "coverage_comparison_gate" => "cass doctor repair --dry-run --json",
        "storage_pressure" | "full_rebuild_readiness" => "cass doctor cleanup --json",
        "legacy_omp_analytics" => "cass analytics rebuild --track all --json",
        _ => "cass doctor check --json",
    }
}

/// Pick the next command to recommend from a read-only `doctor check`.
///
/// #374 Defect 2B: the read-only surface used to hardcode `cass doctor --fix
/// --json`, but `--fix` maps to the SafeAutoRun repair mode whose allowed
/// mutation classes are derived-cleanup only (RetainedPublishBackup /
/// ReclaimableDerivedCache / MemoCache). It can never repair a failing
/// canonical-archive or source-authority check, so recommending it there is a
/// dead end. Route those failures to the command that can actually address them
/// (a dry-run reconstruct/restore, or an archive scan), prioritizing the most
/// fundamental fault; fall back to `--fix` only when the failures are things the
/// safe auto-fix path genuinely handles (derived cleanup, staging, locks).
/// Suffix for the database check message when the engine's only integrity
/// failure is leaked pages; it names the in-place repair (2l1b0.73).
pub(crate) fn doctor_leaked_pages_remedy_suffix(leaked_pages_only: bool) -> String {
    if leaked_pages_only {
        format!(
            "; the only damage is leaked pages (no table, index or freelist owns them), which `{}` inspects and `--yes` frees in place",
            doctor_recover::LEAKED_PAGES_DRY_RUN_COMMAND
        )
    } else {
        String::new()
    }
}

pub(crate) fn doctor_read_only_next_command(check_reports: &[DoctorCheckReport]) -> String {
    let failing = |name: &str| {
        check_reports
            .iter()
            .any(|check| check.name == name && (check.status == "fail" || check.status == "error"))
    };
    // A leak-only integrity failure has an exact in-place repair; the generic
    // repair planner offers nothing for it (2l1b0.73).
    if check_reports.iter().any(|check| {
        check.name == "database"
            && check.status == "fail"
            && check
                .message
                .contains(doctor_recover::LEAKED_PAGES_DRY_RUN_COMMAND)
    }) {
        return doctor_recover::LEAKED_PAGES_DRY_RUN_COMMAND.to_string();
    }
    if failing("database")
        || failing("database_backup")
        || failing("safe_auto_archive_rebuild")
        || failing("candidate_staging")
        || failing("coverage_comparison_gate")
    {
        return "cass doctor repair --dry-run --json".to_string();
    }
    if failing("source_coverage")
        || failing("source_inventory")
        || failing("raw_mirror")
        || failing("raw_mirror_backfill")
    {
        return "cass doctor archive-scan --json".to_string();
    }
    "cass doctor --fix --json".to_string()
}

pub(crate) fn build_doctor_safe_auto_run_report(
    input: DoctorSafeAutoRunBuildInput<'_>,
) -> DoctorSafeAutoRunReport {
    let mut report = DoctorSafeAutoRunReport {
        schema_version: 1,
        enabled: input.enabled,
        mode: if input.enabled {
            "safe_auto_fix".to_string()
        } else {
            "read_only_or_explicit_mode".to_string()
        },
        evaluated_findings: Vec::new(),
        auto_apply_eligibility: false,
        why_safe: Vec::new(),
        why_blocked: Vec::new(),
        why_manual_approval_required: Vec::new(),
        skipped_due_to_lock_or_unknown: false,
        next_exact_command: None,
        applied_actions: input.auto_fix_actions.to_vec(),
        blocked_actions: Vec::new(),
        manual_approval_required: Vec::new(),
        receipt_action_ids: input
            .fs_mutation_receipts
            .iter()
            .map(|receipt| receipt.action_id.clone())
            .collect(),
        receipts: input
            .fs_mutation_receipts
            .iter()
            .map(|receipt| DoctorSafeAutoRunReceiptReference {
                receipt_kind: "fs_mutation".to_string(),
                action_id: receipt.action_id.clone(),
                status: receipt.status,
            })
            .collect(),
        event_log_reference: input.event_log_reference,
    };

    if !input.enabled {
        report
            .why_blocked
            .push("safe auto-run was not requested for this command surface".to_string());
        report.next_exact_command = Some(doctor_read_only_next_command(input.check_reports));
        return report;
    }

    if let Some(reason) = input.operation_state.mutation_blocked_reason.as_deref() {
        report.skipped_due_to_lock_or_unknown = true;
        report.why_blocked.push(format!(
            "mutating doctor lock or operation state blocks safe auto-run: {reason}"
        ));
    }

    for action in input.auto_fix_actions {
        report.evaluated_findings.push(DoctorSafeAutoRunFinding {
            finding: "applied_auto_fix_action".to_string(),
            action: action.clone(),
            decision: DoctorSafeAutoRunDecisionKind::Applied,
            asset_class: DoctorAssetClass::ReclaimableDerivedCache,
            risk_class: DoctorDataLossRisk::Low,
            why_safe: Some(
                "action already passed doctor mutation gates and emitted a receipt or event-log entry"
                    .to_string(),
            ),
            why_blocked: None,
            why_manual_approval_required: None,
            next_exact_command: Some("cass doctor check --json".to_string()),
        });
    }

    for check in input.check_reports {
        if check.status == "pass" && !check.fix_applied {
            continue;
        }
        let action = doctor_safe_auto_action_for_check(check).to_string();
        if check.fix_applied {
            report.evaluated_findings.push(DoctorSafeAutoRunFinding {
                finding: check.name.clone(),
                action: action.clone(),
                decision: DoctorSafeAutoRunDecisionKind::Applied,
                asset_class: check.affected_asset_class,
                risk_class: check.data_loss_risk,
                why_safe: Some(
                    "doctor applied this low-risk action under the safe auto-run gates".to_string(),
                ),
                why_blocked: None,
                why_manual_approval_required: None,
                next_exact_command: Some("cass doctor check --json".to_string()),
            });
            continue;
        }

        if check.status == "pass" {
            continue;
        }

        let data_risk = doctor_data_loss_risk_rank(check.data_loss_risk);
        let decision = if check.safe_for_auto_repair
            && input.operation_state.mutating_doctor_allowed
            && input.archive_queryable_for_non_destructive_derived_rebuild
        {
            DoctorSafeAutoRunDecisionKind::Eligible
        } else if data_risk >= doctor_data_loss_risk_rank(DoctorDataLossRisk::Medium)
            || matches!(
                check.affected_asset_class,
                DoctorAssetClass::CanonicalArchiveDb
                    | DoctorAssetClass::ArchiveDbSidecar
                    | DoctorAssetClass::SourceSessionLog
                    | DoctorAssetClass::RawMirrorBlob
                    | DoctorAssetClass::BackupBundle
                    | DoctorAssetClass::ExternalUpstreamSource
            )
        {
            DoctorSafeAutoRunDecisionKind::ManualApprovalRequired
        } else if input.operation_state.mutation_blocked_reason.is_some() {
            DoctorSafeAutoRunDecisionKind::Skipped
        } else {
            DoctorSafeAutoRunDecisionKind::Blocked
        };

        let next_command = doctor_safe_auto_manual_next_command(check).to_string();
        match decision {
            DoctorSafeAutoRunDecisionKind::Eligible => {
                report.auto_apply_eligibility = true;
                report.why_safe.push(format!(
                    "{} is a derived/additive finding that can be repaired from the current readable archive",
                    check.name
                ));
            }
            DoctorSafeAutoRunDecisionKind::ManualApprovalRequired => {
                report.manual_approval_required.push(action.clone());
                report.why_manual_approval_required.push(format!(
                    "{} affects {:?} with {:?} data-loss risk; safe auto-run will not replace, move, restore, or rebuild archive evidence without an explicit plan fingerprint",
                    check.name, check.affected_asset_class, check.data_loss_risk
                ));
                if report.next_exact_command.is_none() {
                    report.next_exact_command = Some(next_command.clone());
                }
            }
            DoctorSafeAutoRunDecisionKind::Skipped => {
                report.skipped_due_to_lock_or_unknown = true;
                report.blocked_actions.push(action.clone());
                report.why_blocked.push(format!(
                    "{} was skipped because mutation state is locked, active, stale, or unknown",
                    check.name
                ));
                if report.next_exact_command.is_none() {
                    report.next_exact_command = Some(next_command.clone());
                }
            }
            DoctorSafeAutoRunDecisionKind::Blocked => {
                report.blocked_actions.push(action.clone());
                report.why_blocked.push(format!(
                    "{} is not in the safe auto-run allowlist or lacks sufficient authority",
                    check.name
                ));
                if report.next_exact_command.is_none() {
                    report.next_exact_command = Some(next_command.clone());
                }
            }
            DoctorSafeAutoRunDecisionKind::Applied => {}
        }

        report.evaluated_findings.push(DoctorSafeAutoRunFinding {
            finding: check.name.clone(),
            action,
            decision,
            asset_class: check.affected_asset_class,
            risk_class: check.data_loss_risk,
            why_safe: (decision == DoctorSafeAutoRunDecisionKind::Eligible).then(|| {
                if input.db_messages.unwrap_or(0) > 0 {
                    "readable archive DB has messages; derived rebuild can use archive rows without touching source logs".to_string()
                } else {
                    "safe auto-run finding is low risk but no archive messages were available for an immediate rebuild".to_string()
                }
            }),
            why_blocked: matches!(
                decision,
                DoctorSafeAutoRunDecisionKind::Blocked | DoctorSafeAutoRunDecisionKind::Skipped
            )
            .then(|| {
                "not applied because the lock/authority/risk gates did not all pass".to_string()
            }),
            why_manual_approval_required: (decision
                == DoctorSafeAutoRunDecisionKind::ManualApprovalRequired)
                .then(|| {
                    "archive-risk repair requires a dry-run plan, operator review, and exact fingerprint apply"
                        .to_string()
                }),
            next_exact_command: Some(next_command),
        });
    }

    if input.needs_rebuild
        && input.archive_queryable_for_non_destructive_derived_rebuild
        && input.db_messages.unwrap_or(0) > 0
        && report.next_exact_command.is_none()
    {
        report.next_exact_command = Some("cass doctor --fix --json".to_string());
    }
    if report.next_exact_command.is_none() {
        report.next_exact_command = Some("cass doctor check --json".to_string());
    }
    report.auto_apply_eligibility = report.auto_apply_eligibility
        && !report.skipped_due_to_lock_or_unknown
        && report.manual_approval_required.is_empty();
    report
}

pub(crate) fn doctor_severity_rank(severity: DoctorSeverity) -> u8 {
    match severity {
        DoctorSeverity::Info => 0,
        DoctorSeverity::Warn => 1,
        DoctorSeverity::Error => 2,
    }
}

pub(crate) fn doctor_max_severity(left: DoctorSeverity, right: DoctorSeverity) -> DoctorSeverity {
    if doctor_severity_rank(right) > doctor_severity_rank(left) {
        right
    } else {
        left
    }
}

pub(crate) fn doctor_max_data_loss_risk(
    left: DoctorDataLossRisk,
    right: DoctorDataLossRisk,
) -> DoctorDataLossRisk {
    if doctor_data_loss_risk_rank(right) > doctor_data_loss_risk_rank(left) {
        right
    } else {
        left
    }
}

pub(crate) fn doctor_add_unique_string(values: &mut Vec<String>, value: impl Into<String>) {
    let value = value.into();
    if !value.is_empty() && !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

pub(crate) fn doctor_add_unique_asset_class(
    values: &mut Vec<DoctorAssetClass>,
    value: DoctorAssetClass,
) {
    if !values.contains(&value) {
        values.push(value);
    }
}

pub(crate) fn doctor_incident_kind_for_check(
    check: &DoctorCheckReport,
    coverage_risk: &DoctorCoverageRiskSummary,
    candidate_staging: &DoctorCandidateStagingReport,
    fallback_mode: &str,
    candidate_promotion_report: Option<&DoctorCandidatePromotionReport>,
) -> DoctorIncidentRootCauseKind {
    match check.anomaly_class {
        DoctorAnomaly::Healthy => DoctorIncidentRootCauseKind::Unknown,
        DoctorAnomaly::DerivedLexicalStale | DoctorAnomaly::DegradedDerivedAssets => {
            if candidate_promotion_report.is_some_and(|report| {
                report.status == "applied"
                    && (report.derived_lexical_rebuild_required
                        || report.derived_semantic_rebuild_required)
            }) {
                DoctorIncidentRootCauseKind::DerivedIndexStaleAfterDbPromotion
            } else {
                DoctorIncidentRootCauseKind::DerivedIndexStale
            }
        }
        DoctorAnomaly::DerivedSemanticStale => {
            if fallback_mode == "lexical" {
                DoctorIncidentRootCauseKind::SemanticModelMissingLexicalFallback
            } else {
                DoctorIncidentRootCauseKind::DerivedIndexStale
            }
        }
        DoctorAnomaly::UpstreamSourcePruned => {
            if coverage_risk.raw_mirror_db_link_count > 0 {
                DoctorIncidentRootCauseKind::SourcePrunedWithMirrorIntact
            } else {
                DoctorIncidentRootCauseKind::MirrorMissingWithDbSoleCopy
            }
        }
        DoctorAnomaly::RawMirrorMissing | DoctorAnomaly::RawMirrorBehindSource => {
            DoctorIncidentRootCauseKind::MirrorMissingWithDbSoleCopy
        }
        DoctorAnomaly::ArchiveDbCorrupt
        | DoctorAnomaly::ArchiveDbUnreadable
        | DoctorAnomaly::DegradedArchiveRisk => {
            if candidate_staging.completed_candidate_count > 0 {
                DoctorIncidentRootCauseKind::ArchiveDbUnreadableWithValidCandidate
            } else {
                DoctorIncidentRootCauseKind::ArchiveDbUnreadable
            }
        }
        DoctorAnomaly::LockContention => DoctorIncidentRootCauseKind::ActiveLockBlockingRepair,
        DoctorAnomaly::InterruptedRepair => DoctorIncidentRootCauseKind::InterruptedRepairState,
        DoctorAnomaly::StoragePressure | DoctorAnomaly::FullRebuildHeadroom => {
            DoctorIncidentRootCauseKind::StoragePressureDerivedCleanupAvailable
        }
        DoctorAnomaly::ConfigExclusionRisk => DoctorIncidentRootCauseKind::BackupExclusionRisk,
        DoctorAnomaly::BackupUnverified | DoctorAnomaly::BackupStale => {
            DoctorIncidentRootCauseKind::BackupUnverified
        }
        DoctorAnomaly::RepairPreviouslyFailed => DoctorIncidentRootCauseKind::PreviousRepairFailed,
        DoctorAnomaly::SourceAuthorityUnsafe | DoctorAnomaly::RepairBlocked => {
            DoctorIncidentRootCauseKind::Unknown
        }
        DoctorAnomaly::PrivacyRedactionRequired => DoctorIncidentRootCauseKind::Unknown,
    }
}

pub(crate) fn doctor_incident_summary(kind: DoctorIncidentRootCauseKind) -> &'static str {
    match kind {
        DoctorIncidentRootCauseKind::DerivedIndexStale => {
            "Derived search assets need rebuild or cleanup; archive evidence is not the failing asset."
        }
        DoctorIncidentRootCauseKind::DerivedIndexStaleAfterDbPromotion => {
            "A promoted archive generation needs derived search assets rebuilt or marked as lexical fallback."
        }
        DoctorIncidentRootCauseKind::SemanticModelMissingLexicalFallback => {
            "Semantic assets are unavailable, but lexical fallback remains the safe search mode."
        }
        DoctorIncidentRootCauseKind::SourcePrunedWithMirrorIntact => {
            "Upstream source logs are missing, but cass has verified raw mirror evidence."
        }
        DoctorIncidentRootCauseKind::MirrorMissingWithDbSoleCopy => {
            "Some archive rows lack visible upstream or raw-mirror evidence; preserve the cass archive before repair."
        }
        DoctorIncidentRootCauseKind::ArchiveDbUnreadableWithValidCandidate => {
            "The archive DB needs candidate-based recovery; a verified candidate is available or being staged."
        }
        DoctorIncidentRootCauseKind::ArchiveDbUnreadable => {
            "The archive DB is unreadable or corrupt; preserve sidecars and plan recovery before mutation."
        }
        DoctorIncidentRootCauseKind::ActiveLockBlockingRepair => {
            "An active or uncertain lock is blocking mutating doctor work."
        }
        DoctorIncidentRootCauseKind::InterruptedRepairState => {
            "An interrupted doctor operation left artifacts that need inspection before retry."
        }
        DoctorIncidentRootCauseKind::StoragePressureDerivedCleanupAvailable => {
            "Low free space can block safe doctor work; reclaim only approved derived bytes or relocate the archive."
        }
        DoctorIncidentRootCauseKind::BackupExclusionRisk => {
            "Backup or config exclusions may leave archive evidence unprotected."
        }
        DoctorIncidentRootCauseKind::BackupUnverified => {
            "Backups need verification before restore, repair, or cleanup decisions."
        }
        DoctorIncidentRootCauseKind::PreviousRepairFailed => {
            "A previous repair failed verification and must be inspected before repeating repair."
        }
        DoctorIncidentRootCauseKind::Unknown => {
            "Doctor found related symptoms but cannot prove one root cause yet."
        }
    }
}

pub(crate) fn doctor_incident_confidence(
    kind: DoctorIncidentRootCauseKind,
    coverage_risk: &DoctorCoverageRiskSummary,
) -> &'static str {
    if kind == DoctorIncidentRootCauseKind::Unknown
        || coverage_risk.confidence_tier == "unchecked"
        || coverage_risk.status == "not_initialized"
    {
        "unknown"
    } else if coverage_risk.confidence_tier.contains("verified")
        || matches!(
            kind,
            DoctorIncidentRootCauseKind::SourcePrunedWithMirrorIntact
                | DoctorIncidentRootCauseKind::ArchiveDbUnreadableWithValidCandidate
        )
    {
        "high"
    } else {
        "medium"
    }
}

pub(crate) fn doctor_incident_blocked_actions(
    check: &DoctorCheckReport,
    kind: DoctorIncidentRootCauseKind,
    blocked_actions: &mut Vec<String>,
) {
    if !check.safe_for_auto_repair && check.status != "pass" {
        doctor_add_unique_string(blocked_actions, "auto-repair");
    }
    if matches!(
        check.data_loss_risk,
        DoctorDataLossRisk::Medium | DoctorDataLossRisk::High | DoctorDataLossRisk::Unknown
    ) {
        doctor_add_unique_string(blocked_actions, "source-only-rebuild");
    }
    match kind {
        DoctorIncidentRootCauseKind::ActiveLockBlockingRepair => {
            doctor_add_unique_string(blocked_actions, "mutating-doctor-repair");
        }
        DoctorIncidentRootCauseKind::StoragePressureDerivedCleanupAvailable => {
            doctor_add_unique_string(blocked_actions, "archive-evidence-cleanup");
        }
        DoctorIncidentRootCauseKind::PreviousRepairFailed => {
            doctor_add_unique_string(blocked_actions, "repeat-repair-without-marker-inspection");
        }
        DoctorIncidentRootCauseKind::ArchiveDbUnreadable
        | DoctorIncidentRootCauseKind::ArchiveDbUnreadableWithValidCandidate
        | DoctorIncidentRootCauseKind::MirrorMissingWithDbSoleCopy => {
            doctor_add_unique_string(blocked_actions, "coverage-reducing-repair");
        }
        _ => {}
    }
}

pub(crate) fn doctor_incident_redacted_paths(
    kind: DoctorIncidentRootCauseKind,
    operation_state: &DoctorOperationStateReport,
    candidate_staging: &DoctorCandidateStagingReport,
) -> Vec<String> {
    let mut paths = Vec::new();
    if matches!(
        kind,
        DoctorIncidentRootCauseKind::ActiveLockBlockingRepair
            | DoctorIncidentRootCauseKind::InterruptedRepairState
    ) {
        for owner in &operation_state.owners {
            doctor_add_unique_string(&mut paths, owner.redacted_lock_path.clone());
        }
        for interrupted in &operation_state.interrupted_states {
            doctor_add_unique_string(&mut paths, interrupted.redacted_path.clone());
        }
    }
    if matches!(
        kind,
        DoctorIncidentRootCauseKind::ArchiveDbUnreadableWithValidCandidate
            | DoctorIncidentRootCauseKind::InterruptedRepairState
    ) {
        doctor_add_unique_string(&mut paths, candidate_staging.redacted_root_path.clone());
        if let Some(build) = candidate_staging.latest_build.as_ref() {
            if let Some(path) = build.redacted_path.as_ref() {
                doctor_add_unique_string(&mut paths, path.clone());
            }
            if let Some(path) = build.redacted_manifest_path.as_ref() {
                doctor_add_unique_string(&mut paths, path.clone());
            }
        }
        for candidate in &candidate_staging.candidates {
            doctor_add_unique_string(&mut paths, candidate.redacted_path.clone());
            if let Some(path) = candidate.redacted_manifest_path.as_ref() {
                doctor_add_unique_string(&mut paths, path.clone());
            }
        }
    }
    paths.sort();
    paths
}

pub(crate) fn build_doctor_root_cause_incidents(
    checks: &[DoctorCheckReport],
    coverage_risk: &DoctorCoverageRiskSummary,
    operation_state: &DoctorOperationStateReport,
    candidate_staging: &DoctorCandidateStagingReport,
    fallback_mode: &str,
    candidate_promotion_report: Option<&DoctorCandidatePromotionReport>,
) -> Vec<DoctorRootCauseIncident> {
    struct IncidentAccumulator {
        kind: DoctorIncidentRootCauseKind,
        severity: DoctorSeverity,
        affected_asset_classes: Vec<DoctorAssetClass>,
        archive_risk_level: DoctorDataLossRisk,
        derived_risk_level: DoctorDataLossRisk,
        evidence_check_ids: Vec<String>,
        blocked_actions: Vec<String>,
        safe_next_actions: Vec<String>,
        stale_or_unknown_fields: Vec<String>,
    }

    let mut groups: BTreeMap<String, IncidentAccumulator> = BTreeMap::new();
    for check in checks.iter().filter(|check| check.status != "pass") {
        let kind = doctor_incident_kind_for_check(
            check,
            coverage_risk,
            candidate_staging,
            fallback_mode,
            candidate_promotion_report,
        );
        let key = doctor_serde_label(kind);
        let entry = groups.entry(key).or_insert_with(|| IncidentAccumulator {
            kind,
            severity: DoctorSeverity::Info,
            affected_asset_classes: Vec::new(),
            archive_risk_level: DoctorDataLossRisk::None,
            derived_risk_level: DoctorDataLossRisk::None,
            evidence_check_ids: Vec::new(),
            blocked_actions: Vec::new(),
            safe_next_actions: Vec::new(),
            stale_or_unknown_fields: Vec::new(),
        });

        entry.severity = doctor_max_severity(entry.severity, check.severity);
        doctor_add_unique_asset_class(
            &mut entry.affected_asset_classes,
            check.affected_asset_class,
        );
        doctor_add_unique_string(&mut entry.evidence_check_ids, check.name.clone());
        if check.recommended_action != "none" {
            doctor_add_unique_string(&mut entry.safe_next_actions, check.recommended_action);
        }
        doctor_incident_blocked_actions(check, kind, &mut entry.blocked_actions);

        let asset_policy = doctor_asset_policy(check.affected_asset_class);
        let derived_risk = if asset_policy.derived
            && check.data_loss_risk == DoctorDataLossRisk::None
            && check.status != "pass"
        {
            DoctorDataLossRisk::Low
        } else {
            check.data_loss_risk
        };
        if asset_policy.derived {
            entry.derived_risk_level =
                doctor_max_data_loss_risk(entry.derived_risk_level, derived_risk);
        } else {
            entry.archive_risk_level =
                doctor_max_data_loss_risk(entry.archive_risk_level, check.data_loss_risk);
        }
        if check.data_loss_risk == DoctorDataLossRisk::Unknown
            || kind == DoctorIncidentRootCauseKind::Unknown
        {
            doctor_add_unique_string(&mut entry.stale_or_unknown_fields, "root_cause_kind");
        }
    }

    if coverage_risk.confidence_tier == "unchecked" {
        for entry in groups.values_mut() {
            doctor_add_unique_string(&mut entry.stale_or_unknown_fields, "coverage_risk");
        }
    }
    if operation_state.mutation_blocked_reason.is_some() {
        for entry in groups.values_mut().filter(|entry| {
            entry.kind == DoctorIncidentRootCauseKind::ActiveLockBlockingRepair
                || entry.kind == DoctorIncidentRootCauseKind::InterruptedRepairState
        }) {
            doctor_add_unique_string(
                &mut entry.safe_next_actions,
                operation_state.next_action.clone(),
            );
        }
    }

    let mut incidents = groups
        .into_values()
        .map(|mut entry| {
            entry
                .affected_asset_classes
                .sort_by_key(|class| doctor_serde_label(*class));
            entry.evidence_check_ids.sort();
            entry.blocked_actions.sort();
            entry.safe_next_actions.sort();
            entry.stale_or_unknown_fields.sort();
            let redacted_evidence_paths =
                doctor_incident_redacted_paths(entry.kind, operation_state, candidate_staging);
            let root_cause_label = doctor_serde_label(entry.kind);
            let incident_id = doctor_canonical_blake3(
                "doctor-incident-v1",
                serde_json::json!({
                    "kind": root_cause_label,
                    "evidence": entry.evidence_check_ids.clone(),
                    "assets": entry
                        .affected_asset_classes
                        .iter()
                        .map(|class| doctor_serde_label(*class))
                        .collect::<Vec<_>>(),
                }),
            );
            DoctorRootCauseIncident {
                schema_version: 1,
                incident_id,
                root_cause_kind: entry.kind,
                severity: entry.severity,
                affected_asset_classes: entry.affected_asset_classes,
                archive_risk_level: entry.archive_risk_level,
                derived_risk_level: entry.derived_risk_level,
                confidence: doctor_incident_confidence(entry.kind, coverage_risk).to_string(),
                evidence_check_ids: entry.evidence_check_ids,
                blocked_actions: entry.blocked_actions,
                safe_next_actions: entry.safe_next_actions,
                stale_or_unknown_fields: entry.stale_or_unknown_fields,
                redacted_evidence_paths,
                summary: doctor_incident_summary(entry.kind).to_string(),
            }
        })
        .collect::<Vec<_>>();

    incidents.sort_by(|left, right| {
        doctor_severity_rank(right.severity)
            .cmp(&doctor_severity_rank(left.severity))
            .then_with(|| {
                doctor_data_loss_risk_rank(right.archive_risk_level)
                    .cmp(&doctor_data_loss_risk_rank(left.archive_risk_level))
            })
            .then_with(|| {
                doctor_data_loss_risk_rank(right.derived_risk_level)
                    .cmp(&doctor_data_loss_risk_rank(left.derived_risk_level))
            })
            .then_with(|| {
                doctor_serde_label(left.root_cause_kind)
                    .cmp(&doctor_serde_label(right.root_cause_kind))
            })
            .then_with(|| left.incident_id.cmp(&right.incident_id))
    });
    incidents
}

pub(crate) fn doctor_health_class_for_checks(checks: &[DoctorCheckReport]) -> DoctorHealth {
    if checks
        .iter()
        .any(|check| check.health_class == DoctorHealth::RepairPreviouslyFailed)
    {
        return DoctorHealth::RepairPreviouslyFailed;
    }
    if checks
        .iter()
        .any(|check| check.health_class == DoctorHealth::DegradedArchiveRisk)
    {
        return DoctorHealth::DegradedArchiveRisk;
    }
    if checks
        .iter()
        .any(|check| check.health_class == DoctorHealth::SourceAuthorityUnsafe)
    {
        return DoctorHealth::SourceAuthorityUnsafe;
    }
    if checks
        .iter()
        .any(|check| check.health_class == DoctorHealth::RepairBlocked)
    {
        return DoctorHealth::RepairBlocked;
    }
    if checks
        .iter()
        .any(|check| check.health_class == DoctorHealth::DegradedDerivedAssets)
    {
        return DoctorHealth::DegradedDerivedAssets;
    }
    DoctorHealth::Healthy
}

pub(crate) fn doctor_data_loss_risk_rank(risk: DoctorDataLossRisk) -> u8 {
    match risk {
        DoctorDataLossRisk::None => 0,
        DoctorDataLossRisk::Low => 1,
        DoctorDataLossRisk::Medium => 2,
        DoctorDataLossRisk::High => 3,
        DoctorDataLossRisk::Unknown => 4,
    }
}

pub(crate) fn doctor_highest_data_loss_risk(checks: &[DoctorCheckReport]) -> DoctorDataLossRisk {
    checks
        .iter()
        .map(|check| check.data_loss_risk)
        .max_by_key(|risk| doctor_data_loss_risk_rank(*risk))
        .unwrap_or(DoctorDataLossRisk::None)
}

pub(crate) fn doctor_top_level_operation_outcome(
    checks: &[DoctorCheckReport],
    fix_requested: bool,
    issues_found: usize,
    issues_fixed: usize,
    not_initialized: bool,
    post_repair_probes: &DoctorPostRepairProbeSuiteReport,
    cleanup_apply_result: Option<&DiagCleanupApplyResult>,
) -> DoctorOperationOutcomeReport {
    if fix_requested && post_repair_probes.blocks_success {
        let failed = post_repair_probes
            .probes
            .iter()
            .filter(|probe| probe.status == "fail")
            .map(|probe| {
                format!(
                    "{}: {}",
                    probe.probe_id,
                    probe
                        .failure_reason
                        .as_deref()
                        .unwrap_or("post-repair probe failed")
                )
            })
            .collect::<Vec<_>>();
        return doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::VerificationFailed,
            format!(
                "post-repair verification failed after mutation: {}",
                failed.join("; ")
            ),
            "preserved repair artifacts and recorded post-repair probe failure context".to_string(),
            "doctor did not report repair success because the repaired state was not proven usable"
                .to_string(),
            DoctorDataLossRisk::Medium,
            Some("cass doctor --json".to_string()),
            post_repair_probes.redacted_manifest_path.clone(),
        );
    }

    if let Some(result) = cleanup_apply_result {
        return result.operation_outcome.clone();
    }

    let data_loss_risk = doctor_highest_data_loss_risk(checks);
    if fix_requested
        && checks
            .iter()
            .any(|check| check.anomaly_class == DoctorAnomaly::LockContention)
    {
        return doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::RepairBlocked,
            "doctor repair was blocked by an active or unverifiable operation lock".to_string(),
            "recorded lock contention without attempting mutation".to_string(),
            "no repair, rebuild, promotion, restore, or cleanup action was attempted".to_string(),
            data_loss_risk,
            Some("cass doctor --json".to_string()),
            None,
        );
    }

    if fix_requested
        && checks
            .iter()
            .any(|check| check.anomaly_class == DoctorAnomaly::InterruptedRepair)
    {
        return doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::RepairBlocked,
            "doctor repair was blocked by interrupted operation artifacts that require inspection"
                .to_string(),
            "reported interrupted operation state without attempting mutation".to_string(),
            "no repair, rebuild, promotion, restore, or cleanup action was attempted".to_string(),
            data_loss_risk,
            Some("cass doctor --json".to_string()),
            None,
        );
    }

    if fix_requested
        && checks
            .iter()
            .any(|check| check.name == "repair_failure_marker" && check.status == "fail")
    {
        return doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::RepairRefused,
            "previous doctor repair failure marker blocks repeated mutation".to_string(),
            "reported the durable failure marker and preserved prior evidence".to_string(),
            "no repair, rebuild, promotion, restore, or cleanup action was attempted".to_string(),
            data_loss_risk,
            Some("cass doctor --json".to_string()),
            None,
        );
    }

    if fix_requested
        && checks
            .iter()
            .any(|check| check.name == "repair_plan_approval" && check.status == "fail")
    {
        return doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::RepairRefused,
            "repair plan approval failed or drifted before mutation".to_string(),
            "reported the refused fingerprint approval without mutating archive or derived assets"
                .to_string(),
            "no repair, rebuild, promotion, restore, or cleanup action was attempted".to_string(),
            data_loss_risk,
            Some("cass doctor repair --dry-run --json".to_string()),
            None,
        );
    }

    if fix_requested && issues_fixed > 0 {
        let (kind, reason, action_taken, action_not_taken) = if issues_found == 0 {
            (
                DoctorOperationOutcomeKind::Fixed,
                format!("doctor fixed {issues_fixed} issue(s)"),
                "applied every safe repair that was selected".to_string(),
                "no selected safe repair remained blocked".to_string(),
            )
        } else {
            (
                DoctorOperationOutcomeKind::PartiallyFixed,
                format!(
                    "doctor fixed {issues_fixed} issue(s) and left {issues_found} issue(s) for review"
                ),
                "applied the safe repairs that passed all gates".to_string(),
                "left remaining blocked or unsafe issues for operator review".to_string(),
            )
        };
        return doctor_operation_outcome_with_details(
            kind,
            reason,
            action_taken,
            action_not_taken,
            data_loss_risk,
            Some("cass doctor --json".to_string()),
            None,
        );
    }

    if issues_found == 0 {
        return doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::OkNoActionNeeded,
            "all doctor checks passed".to_string(),
            "completed diagnostic checks".to_string(),
            "no repair or cleanup was needed".to_string(),
            DoctorDataLossRisk::None,
            None,
            None,
        );
    }

    if !fix_requested {
        // Same routing as the safe-auto-run report's next_exact_command
        // (#374): `--fix` only runs derived cleanup, so a failing archive or
        // source-authority check must not be told to run it. The owner's
        // leaked-page archive got `cass doctor --fix` here while its own
        // check said "reconstruct from verified authority" (2l1b0.73).
        let next_command = if not_initialized {
            Some("cass index --full".to_string())
        } else {
            Some(doctor_read_only_next_command(checks))
        };
        return doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::OkReadOnlyDiagnosed,
            format!("doctor diagnosed {issues_found} issue(s) in read-only mode"),
            "completed diagnostic checks without mutation".to_string(),
            "no repair was attempted because --fix was not requested".to_string(),
            data_loss_risk,
            next_command,
            None,
        );
    }

    if checks.iter().any(|check| {
        check.anomaly_class == DoctorAnomaly::RepairPreviouslyFailed && check.status == "fail"
    }) {
        return doctor_operation_outcome_with_details(
            DoctorOperationOutcomeKind::VerificationFailed,
            "previous repair verification failed and must be inspected first".to_string(),
            "refused automatic repeat repair".to_string(),
            "no repair was applied without reviewing the prior failure marker".to_string(),
            data_loss_risk,
            Some("cass doctor --json".to_string()),
            None,
        );
    }

    doctor_operation_outcome_with_details(
        DoctorOperationOutcomeKind::AutoRunSkipped,
        format!("doctor found {issues_found} issue(s), but none were eligible for safe auto-run"),
        "evaluated automatic repair eligibility".to_string(),
        "no repair was applied automatically".to_string(),
        data_loss_risk,
        Some("cass doctor --json".to_string()),
        None,
    )
}
