use super::checks::validate_payload_path;
use super::types::{CheckResult, ShareProfileEvidence};
use crate::franken_sync::compat::RowExt;
use crate::pages::bundle::UnencryptedConfig;
use crate::pages::profiles::ShareProfile;
use crate::pages::redact::RedactionEngine;
use serde_json::Value;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::Path;

/// Text surfaces a Pages export redacts, as (label, table, column, holds JSON).
/// Both FTS tables are rescanned too: a value left in the search index is as
/// public as one left in the message it indexes.
const SHARE_PROFILE_SURFACES: &[(&str, &str, &str, bool)] = &[
    ("conversations.title", "conversations", "title", false),
    (
        "conversations.workspace",
        "conversations",
        "workspace",
        false,
    ),
    (
        "conversations.source_path",
        "conversations",
        "source_path",
        false,
    ),
    (
        "conversations.metadata_json",
        "conversations",
        "metadata_json",
        true,
    ),
    ("messages.content", "messages", "content", false),
    (
        "messages.attachment_refs",
        "messages",
        "attachment_refs",
        true,
    ),
    ("snippets.file_path", "snippets", "file_path", false),
    ("snippets.snippet_text", "snippets", "snippet_text", false),
    ("messages_fts.content", "messages_fts", "content", false),
    (
        "messages_code_fts.content",
        "messages_code_fts",
        "content",
        false,
    ),
];

/// Rescan a plaintext archive with the share profile it declares (2l1b0.60).
///
/// Fails when the archive declares a profile verification cannot check, or
/// when that profile's rules would still rewrite an exported value; the
/// residual is reported as counts per surface, never the values. Workspaces
/// are rescanned as plain text because project-name anonymization is a
/// mapping, not a pattern. The home-path and username rules use the verifying
/// account's home directory, so they catch the exporting account's paths when
/// the exporting account verifies, as every export does before it reports
/// success. What this check cannot read is a warning; the payload checks own
/// structural failures.
pub(super) fn check_share_profile(
    site_dir: &Path,
    warnings: &mut Vec<String>,
) -> (CheckResult, ShareProfileEvidence) {
    let mut evidence = ShareProfileEvidence::default();
    let config = File::open(site_dir.join("config.json"))
        .map_err(anyhow::Error::from)
        .and_then(|file| {
            Ok(serde_json::from_reader::<_, UnencryptedConfig>(
                BufReader::new(file),
            )?)
        });
    let Ok(config) = config else {
        return (CheckResult::pass(), evidence);
    };
    let relative = Path::new(&config.payload.path);
    let payload = site_dir.join(relative);
    if !validate_payload_path(&mut Vec::new(), "payload.path", relative)
        || !fs::symlink_metadata(&payload).is_ok_and(|meta| meta.file_type().is_file())
    {
        return (CheckResult::pass(), evidence);
    }
    let conn = match crate::pages::open_existing_sqlite_db(&payload) {
        Ok(conn) => conn,
        Err(error) => {
            warnings.push(format!(
                "share profile not checked: the plaintext payload could not be opened ({error:#})"
            ));
            return (CheckResult::pass(), evidence);
        }
    };
    let declared = conn
        .query_row("SELECT value FROM export_meta WHERE key = 'share_profile'")
        .ok()
        .and_then(|row| row.get_typed::<String>(0).ok());
    let Some(declared) = declared else {
        warnings.push(
            "the plaintext archive records no share profile, so it was exported without \
             share-profile redaction of usernames, hostnames and home paths"
                .to_string(),
        );
        return (CheckResult::pass(), evidence);
    };
    evidence.declared = Some(declared.clone());
    let Some(profile) = [
        ShareProfile::Public,
        ShareProfile::Team,
        ShareProfile::Personal,
    ]
    .into_iter()
    .find(|profile| profile.label() == declared) else {
        return (
            CheckResult::fail(format!(
                "archive declares share profile '{declared}', which verification cannot check"
            )),
            evidence,
        );
    };
    let engine = RedactionEngine::new(profile.export_redaction_config());
    for &(label, table, column, json) in SHARE_PROFILE_SURFACES {
        let sql = format!("SELECT \"{column}\" FROM \"{table}\" WHERE \"{column}\" IS NOT NULL");
        let mut residual = 0_u64;
        let scanned = conn.query_with_params_for_each(&sql, &[], |row| {
            residual += residual_values(&engine, &row.get_typed::<String>(0)?, json);
            Ok(())
        });
        if let Err(error) = scanned {
            return (
                CheckResult::fail(format!(
                    "could not rescan {label} for the '{declared}' share profile: {error}"
                )),
                evidence,
            );
        }
        if residual > 0 {
            evidence.residual.insert(label.to_string(), residual);
        }
    }
    evidence.rescanned = true;
    if evidence.residual.is_empty() {
        return (CheckResult::pass(), evidence);
    }
    let surfaces = evidence
        .residual
        .iter()
        .map(|(surface, count)| format!("{surface} ({count})"))
        .collect::<Vec<_>>()
        .join(", ");
    (
        CheckResult::fail(format!(
            "values the '{declared}' share profile removes remain in {surfaces}"
        )),
        evidence,
    )
}

/// How many values in `text` the engine would still rewrite. JSON is rescanned
/// by string value, as the export redacted it; text that does not parse is
/// rescanned whole.
fn residual_values(engine: &RedactionEngine, text: &str, json: bool) -> u64 {
    fn walk(engine: &RedactionEngine, node: &Value) -> u64 {
        match node {
            Value::String(text) => u64::from(!engine.redact_text(text).changes.is_empty()),
            Value::Array(items) => items.iter().map(|item| walk(engine, item)).sum(),
            Value::Object(fields) => fields.values().map(|item| walk(engine, item)).sum(),
            _ => 0,
        }
    }
    if json && let Ok(value) = serde_json::from_str::<Value>(text) {
        return walk(engine, &value);
    }
    u64::from(!engine.redact_text(text).changes.is_empty())
}
