use crate::cli::args_sub::RobotFormat;
use crate::doctor::forensic::doctor_redacted_text;
use crate::doctor::policy::types::DoctorAssetClass;
use serde::Serialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DoctorSupportBundleCommandMode {
    Create,
    Verify,
}

impl DoctorSupportBundleCommandMode {
    pub(crate) fn stable_name(self) -> &'static str {
        match self {
            Self::Create => "support-bundle",
            Self::Verify => "support-bundle-verify",
        }
    }
}

pub(crate) struct DoctorSupportBundleRequest {
    pub(crate) data_dir_override: Option<PathBuf>,
    pub(crate) db_override: Option<PathBuf>,
    pub(crate) output_format: Option<RobotFormat>,
    pub(crate) mode: DoctorSupportBundleCommandMode,
    pub(crate) baseline_id: Option<String>,
    pub(crate) baseline_path: Option<PathBuf>,
    pub(crate) support_bundle_path: Option<PathBuf>,
    pub(crate) include_sensitive_attachments: bool,
    pub(crate) sensitive_attachments: Vec<PathBuf>,
    pub(crate) sensitive_attachment_max_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DoctorSupportBundleArtifact {
    pub(crate) artifact_id: String,
    pub(crate) artifact_kind: String,
    pub(crate) asset_class: DoctorAssetClass,
    pub(crate) relative_path: String,
    pub(crate) size_bytes: u64,
    pub(crate) blake3: String,
    pub(crate) sensitive: bool,
    pub(crate) opt_in_required: bool,
    pub(crate) included_by_default: bool,
    pub(crate) redaction_status: String,
    pub(crate) risk_copy: Option<String>,
}

pub(crate) struct DoctorSupportBundleArtifactSpec<'a> {
    pub(crate) relative_path: &'a str,
    pub(crate) artifact_kind: &'a str,
    pub(crate) asset_class: DoctorAssetClass,
    pub(crate) sensitive: bool,
    pub(crate) opt_in_required: bool,
    pub(crate) included_by_default: bool,
    pub(crate) risk_copy: Option<String>,
}

pub(crate) fn doctor_support_bundle_root(data_dir: &Path) -> PathBuf {
    data_dir.join("doctor").join("support-bundles")
}

pub(crate) fn doctor_support_bundle_redact_text(text: &str, data_dir: &Path) -> String {
    let locally_redacted = doctor_support_bundle_redact_local_text(text, data_dir);
    crate::pages::redact::redact_swarm_text(&locally_redacted)
}

pub(crate) fn doctor_support_bundle_redact_local_text(text: &str, data_dir: &Path) -> String {
    let mut redacted = doctor_redacted_text(text, data_dir);
    if let Ok(home) = dotenvy::var("HOME")
        && !home.is_empty()
    {
        redacted = redacted.replace(&home, "[home]");
    }
    redacted = redacted.replace(
        "CASS_DOCTOR_PRIVACY_SENTINEL",
        "[redacted-sensitive-sentinel]",
    );
    redacted = redacted.replace(
        "PRIVATE_SOURCE_SNIPPET",
        "[redacted-private-source-snippet]",
    );
    redacted = redacted.replace("raw mirror session bytes", "[redacted-raw-mirror-bytes]");
    redacted = redacted.replace("raw session text", "[redacted-raw-session-text]");
    redacted
}

pub(crate) fn doctor_support_bundle_redact_value(
    value: serde_json::Value,
    data_dir: &Path,
) -> serde_json::Value {
    let locally_redacted = doctor_support_bundle_redact_local_value(value, data_dir);
    crate::pages::redact::redact_swarm_json_value(&locally_redacted)
}

/// Apply support-bundle-specific path and sentinel substitutions before the
/// canonical structured secret policy. Keys are serialized output too, and
/// two distinct source keys can collapse to one redacted spelling, so preserve
/// both entries with the shared collision-safe insertion helper.
pub(crate) fn doctor_support_bundle_redact_local_value(
    value: serde_json::Value,
    data_dir: &Path,
) -> serde_json::Value {
    match value {
        serde_json::Value::String(text) => {
            serde_json::Value::String(doctor_support_bundle_redact_text(&text, data_dir))
        }
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .into_iter()
                .map(|item| doctor_support_bundle_redact_local_value(item, data_dir))
                .collect(),
        ),
        serde_json::Value::Object(map) => {
            let mut redacted = serde_json::Map::with_capacity(map.len());
            let mut next_suffixes = HashMap::new();
            for (key, value) in map {
                crate::indexer::redact_secrets::insert_redacted_json_entry(
                    &mut redacted,
                    &mut next_suffixes,
                    doctor_support_bundle_redact_local_text(&key, data_dir),
                    doctor_support_bundle_redact_local_value(value, data_dir),
                );
            }
            serde_json::Value::Object(redacted)
        }
        other => other,
    }
}

pub(crate) mod capture;
#[cfg(test)]
pub(crate) mod doctor_support_bundle_redaction_tests;
