use crate::cli::commands::analytics::run_analytics::{
    close_franken_cli_read_db, open_franken_cli_read_db,
};
use crate::cli::commands::view::role_to_export_string::{
    normalized_provenance_origin_host, normalized_provenance_origin_kind,
    normalized_provenance_source_id,
};
use crate::cli::error::CliError;
use crate::doctor::forensic::{
    DoctorForensicBundleCaptureInput, capture_doctor_forensic_bundle, doctor_canonical_blake3,
    doctor_forensic_bundle_uncaptured, doctor_redacted_path,
};
use crate::doctor::lexical_cleanup_disposition_label::system_time_to_unix_ms;
use crate::doctor::policy::DoctorArtifactChecksumStatus;
use crate::doctor::raw_mirror::doctor_raw_mirror_root_dir::DOCTOR_RAW_MIRROR_BACKFILL_GENERATION;
use crate::doctor::raw_mirror::doctorrawmirrorverificationmode::{
    DoctorRawMirrorManifestReport, DoctorRawMirrorReport, doctor_file_blake3,
    doctor_raw_mirror_manifest_relative_path,
};
use crate::doctor::report::inventory::{
    DoctorRawMirrorBackfillCandidate, DoctorRawMirrorBackfillReceipt,
    DoctorRawMirrorBackfillReport, DoctorRawMirrorBackfillSourceStatSnapshot,
    DoctorRawMirrorExistingEvidence,
};
use crate::doctor::report::recommendations::{
    doctor_normalized_provider_slug, doctor_normalized_source_path,
};
use crate::doctor::sources::doctor_table_columns;
use crate::franken_sync::compat::{ConnectionExt, RowExt};
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub(crate) fn query_doctor_raw_mirror_backfill_candidates(
    conn: &crate::franken_sync::Connection,
) -> std::result::Result<Vec<DoctorRawMirrorBackfillCandidate>, crate::franken_sync::FrankenError> {
    use crate::franken_sync::compat::ConnectionExt as _;

    let conversation_columns = doctor_table_columns(conn, "conversations");
    if !conversation_columns.contains("id") {
        return Ok(Vec::new());
    }
    let agent_columns = doctor_table_columns(conn, "agents");
    let source_columns = doctor_table_columns(conn, "sources");
    let message_columns = doctor_table_columns(conn, "messages");

    let can_join_agents = conversation_columns.contains("agent_id")
        && agent_columns.contains("id")
        && agent_columns.contains("slug");
    let can_join_sources = conversation_columns.contains("source_id")
        && source_columns.contains("id")
        && source_columns.contains("kind");
    let can_count_messages =
        message_columns.contains("conversation_id") && message_columns.contains("id");

    let provider_expr = if can_join_agents {
        "COALESCE(a.slug, 'unknown')"
    } else {
        "'unknown'"
    };
    let source_path_expr = if conversation_columns.contains("source_path") {
        "c.source_path"
    } else {
        "NULL"
    };
    let source_id_expr = if conversation_columns.contains("source_id") {
        "COALESCE(c.source_id, 'local')"
    } else {
        "'local'"
    };
    let origin_host_expr = if conversation_columns.contains("origin_host") {
        "c.origin_host"
    } else {
        "NULL"
    };
    let origin_kind_expr = if can_join_sources { "s.kind" } else { "NULL" };
    let started_at_expr = if conversation_columns.contains("started_at") {
        "c.started_at"
    } else {
        "NULL"
    };
    let message_count_expr = if can_count_messages {
        // Count messages in one narrow streaming index pass after the bounded
        // conversation metadata query.  A correlated COUNT here executes one
        // messages scan per conversation under FrankenSQLite.
        "0"
    } else if conversation_columns.contains("message_count") {
        "COALESCE(c.message_count, 0)"
    } else {
        "0"
    };
    let agent_join = if can_join_agents {
        " LEFT JOIN agents a ON c.agent_id = a.id"
    } else {
        ""
    };
    let source_join = if can_join_sources {
        " LEFT JOIN sources s ON c.source_id = s.id"
    } else {
        ""
    };
    let sql = format!(
        "SELECT c.id, {provider_expr}, {source_path_expr}, {source_id_expr}, \
         {origin_host_expr}, {origin_kind_expr}, {started_at_expr}, {message_count_expr} \
         FROM conversations c{agent_join}{source_join} \
         ORDER BY c.id"
    );

    let mut candidates = conn.query_map_collect(&sql, &[], |row: &crate::franken_sync::Row| {
        let message_count: i64 = row.get_typed(7)?;
        Ok(DoctorRawMirrorBackfillCandidate {
            conversation_id: row.get_typed(0)?,
            provider: row.get_typed(1)?,
            source_path: row.get_typed(2)?,
            source_id: row.get_typed(3)?,
            origin_host: row.get_typed(4)?,
            origin_kind: row.get_typed(5)?,
            started_at_ms: row.get_typed(6)?,
            message_count: message_count.max(0) as usize,
        })
    })?;

    if can_count_messages && !candidates.is_empty() {
        let candidate_positions: HashMap<i64, usize> = candidates
            .iter()
            .enumerate()
            .map(|(position, candidate)| (candidate.conversation_id, position))
            .collect();
        for_each_message_conversation_id(conn, |conversation_id| {
            if let Some(position) = candidate_positions.get(&conversation_id) {
                candidates[*position].message_count =
                    candidates[*position].message_count.saturating_add(1);
            }
        })?;
    }

    Ok(candidates)
}

/// Visit every message's conversation id in one streaming pass, for callers
/// that need per-conversation message counts across many conversations.
/// FrankenSQLite runs a correlated or grouped COUNT over messages as a scan
/// per conversation or a sorter over every joined row; this pass reads the
/// table once and allocates nothing per message.
pub(crate) fn for_each_message_conversation_id(
    conn: &crate::franken_sync::Connection,
    mut visit: impl FnMut(i64),
) -> Result<(), crate::franken_sync::FrankenError> {
    let message_indexes: HashSet<String> = conn
        .query_map_collect(
            "PRAGMA index_list(messages)",
            &[],
            |row: &crate::franken_sync::Row| row.get_typed::<String>(1),
        )
        .unwrap_or_default()
        .into_iter()
        .collect();
    let message_scan_sql = if message_indexes.contains("sqlite_autoindex_messages_1") {
        // The UNIQUE(conversation_id, idx) autoindex is narrow: scanning it
        // avoids loading the potentially enormous content column.
        "SELECT conversation_id \
         FROM messages INDEXED BY sqlite_autoindex_messages_1"
    } else {
        // Historical schemas may lack the modern uniqueness constraint.
        // A plain table scan remains exact and streaming.
        "SELECT conversation_id FROM messages"
    };
    conn.query_with_params_for_each(
        message_scan_sql,
        &[] as &[crate::franken_sync::SqliteValue],
        |row| {
            visit(row.get_typed(0)?);
            Ok(())
        },
    )
}

pub(crate) fn doctor_raw_mirror_backfill_source_path_blake3(path: &str) -> String {
    doctor_canonical_blake3(
        "doctor-raw-mirror-backfill-source-path-v1",
        serde_json::json!({ "source_path": path }),
    )
}

pub(crate) fn doctor_raw_mirror_backfill_source_key(
    source_id: &str,
    origin_kind: &str,
    origin_host: Option<&str>,
    source_path: &str,
) -> String {
    doctor_canonical_blake3(
        "doctor-raw-mirror-backfill-source-key-v2",
        serde_json::json!({
            "source_id": source_id,
            "origin_kind": origin_kind,
            "origin_host": origin_host,
            "source_path": source_path,
        }),
    )
}

pub(crate) fn doctor_raw_mirror_backfill_record_id(
    candidate: &DoctorRawMirrorBackfillCandidate,
) -> String {
    doctor_canonical_blake3(
        "doctor-raw-mirror-backfill-record-v1",
        serde_json::json!({
            "conversation_id": candidate.conversation_id,
            "provider": candidate.provider,
            "source_id": candidate.source_id,
            "origin_host": candidate.origin_host,
            "origin_kind": candidate.origin_kind,
            "source_path": candidate.source_path,
        }),
    )
}

pub(crate) fn doctor_raw_mirror_backfill_source_stat(
    path: &Path,
) -> DoctorRawMirrorBackfillSourceStatSnapshot {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            let file_type = if metadata.file_type().is_symlink() {
                "symlink"
            } else if metadata.is_file() {
                "file"
            } else if metadata.is_dir() {
                "directory"
            } else {
                "other"
            }
            .to_string();
            let (content_blake3, stat_error) =
                if metadata.is_file() && !metadata.file_type().is_symlink() {
                    match doctor_file_blake3(path) {
                        Ok(hash) => (Some(hash), None),
                        Err(err) => (None, Some(format!("failed to hash source contents: {err}"))),
                    }
                } else {
                    (None, None)
                };
            DoctorRawMirrorBackfillSourceStatSnapshot {
                exists: true,
                file_type,
                size_bytes: metadata.is_file().then_some(metadata.len()),
                modified_at_ms: metadata.modified().ok().and_then(system_time_to_unix_ms),
                content_blake3,
                stat_error,
            }
        }
        Err(err) => DoctorRawMirrorBackfillSourceStatSnapshot {
            exists: false,
            file_type: "missing".to_string(),
            stat_error: Some(err.to_string()),
            ..DoctorRawMirrorBackfillSourceStatSnapshot::default()
        },
    }
}

pub(crate) fn doctor_raw_mirror_manifest_is_verified(
    manifest: &DoctorRawMirrorManifestReport,
) -> bool {
    manifest.status == "verified"
        && manifest.blob_checksum_status == DoctorArtifactChecksumStatus::Matched
        && manifest.manifest_checksum_status == DoctorArtifactChecksumStatus::Matched
}

pub(crate) fn doctor_raw_mirror_existing_evidence(
    manifest: &DoctorRawMirrorManifestReport,
    db_linked: bool,
) -> DoctorRawMirrorExistingEvidence {
    DoctorRawMirrorExistingEvidence {
        manifest_id: manifest.manifest_id.clone(),
        manifest_relative_path: doctor_raw_mirror_manifest_relative_path(&manifest.manifest_id),
        blob_blake3: manifest.blob_blake3.clone(),
        blob_size_bytes: manifest.blob_size_bytes,
        source_content_blake3: manifest.source_content_blake3.clone(),
        source_size_bytes: manifest.source_size_bytes,
        storage_kind: manifest.storage_kind.clone(),
        chunk_count: manifest.chunk_count,
        captured_at_ms: manifest.captured_at_ms,
        source_mtime_ms: manifest.source_mtime_ms,
        db_linked,
    }
}

/// Raw-mirror evidence for archive conversations, keyed by `(conversation_id,
/// source path)`, and evidence by source key. A db link names a conversation
/// id in the archive that wrote it; after that archive is replaced (doctor's
/// reconstruct promotion numbers conversations anew), the same id can name a
/// different source. So a link is evidence only for the conversation at the
/// link's own source path, and a stale link falls through to the source-key
/// lookup instead of comparing a source with another source's digest (zp0fp).
pub(crate) fn doctor_raw_mirror_existing_evidence_maps(
    raw_mirror: &DoctorRawMirrorReport,
) -> (
    HashMap<(i64, String), DoctorRawMirrorExistingEvidence>,
    HashMap<String, DoctorRawMirrorExistingEvidence>,
) {
    let mut by_conversation_id: HashMap<(i64, String), DoctorRawMirrorExistingEvidence> =
        HashMap::new();
    let mut by_source_key: HashMap<String, DoctorRawMirrorExistingEvidence> = HashMap::new();

    for manifest in raw_mirror
        .manifests
        .iter()
        .filter(|manifest| doctor_raw_mirror_manifest_is_verified(manifest))
    {
        let linked_evidence = doctor_raw_mirror_existing_evidence(manifest, true);
        for link in &manifest.db_links {
            if let Some(conversation_id) = link.conversation_id {
                let linked_path = link
                    .source_path
                    .as_deref()
                    .map(str::trim)
                    .filter(|path| !path.is_empty())
                    .unwrap_or_else(|| manifest.original_path.trim())
                    .to_string();
                by_conversation_id
                    .entry((conversation_id, linked_path))
                    .and_modify(|existing| {
                        if doctor_raw_mirror_evidence_is_newer(&linked_evidence, existing) {
                            *existing = linked_evidence.clone();
                        }
                    })
                    .or_insert_with(|| linked_evidence.clone());
            }
        }

        if !manifest.original_path.trim().is_empty() {
            let has_db_link = !manifest.db_links.is_empty();
            let source_key = doctor_raw_mirror_backfill_source_key(
                &manifest.source_id,
                &manifest.origin_kind,
                manifest.origin_host.as_deref(),
                &manifest.original_path,
            );
            let evidence = doctor_raw_mirror_existing_evidence(manifest, has_db_link);
            by_source_key
                .entry(source_key)
                .and_modify(|existing| {
                    if doctor_raw_mirror_evidence_is_newer(&evidence, existing) {
                        *existing = evidence.clone();
                    }
                })
                .or_insert(evidence);
        }
    }

    (by_conversation_id, by_source_key)
}

pub(crate) fn doctor_raw_mirror_evidence_is_newer(
    candidate: &DoctorRawMirrorExistingEvidence,
    current: &DoctorRawMirrorExistingEvidence,
) -> bool {
    (
        candidate.captured_at_ms,
        candidate.source_mtime_ms.unwrap_or(i64::MIN),
        candidate.manifest_id.as_str(),
    ) > (
        current.captured_at_ms,
        current.source_mtime_ms.unwrap_or(i64::MIN),
        current.manifest_id.as_str(),
    )
}

pub(crate) fn doctor_raw_mirror_backfill_db_link(
    candidate: &DoctorRawMirrorBackfillCandidate,
    source_path: Option<&str>,
) -> crate::raw_mirror::RawMirrorDbLink {
    crate::raw_mirror::RawMirrorDbLink {
        conversation_id: Some(candidate.conversation_id),
        message_count: Some(candidate.message_count),
        source_path: source_path.map(ToOwned::to_owned),
        started_at_ms: candidate.started_at_ms,
    }
}

pub(crate) fn doctor_raw_mirror_backfill_receipt_base(
    data_dir: &Path,
    candidate: &DoctorRawMirrorBackfillCandidate,
    provider: String,
    source_id: String,
    origin_kind: String,
    origin_host: Option<String>,
    source_path: Option<&str>,
) -> DoctorRawMirrorBackfillReceipt {
    DoctorRawMirrorBackfillReceipt {
        stable_record_id: doctor_raw_mirror_backfill_record_id(candidate),
        conversation_id: Some(candidate.conversation_id),
        provider,
        source_id,
        origin_kind,
        origin_host,
        message_count: candidate.message_count,
        started_at_ms: candidate.started_at_ms,
        redacted_source_path: source_path.map(|path| doctor_redacted_path(path, data_dir)),
        source_path_blake3: source_path.map(doctor_raw_mirror_backfill_source_path_blake3),
        parse_loss_unknown: true,
        backfill_generation: DOCTOR_RAW_MIRROR_BACKFILL_GENERATION,
        forensic_bundle: doctor_forensic_bundle_uncaptured("not_required_read_only_or_no_mutation"),
        ..DoctorRawMirrorBackfillReceipt::default()
    }
}

pub(crate) fn doctor_raw_mirror_backfill_mark_existing_evidence(
    receipt: &mut DoctorRawMirrorBackfillReceipt,
    evidence: &DoctorRawMirrorExistingEvidence,
) {
    receipt.raw_source_captured = true;
    receipt.captured_at_ms = Some(evidence.captured_at_ms);
    receipt.raw_mirror_manifest_id = Some(evidence.manifest_id.clone());
    receipt.raw_mirror_manifest_relative_path = Some(evidence.manifest_relative_path.clone());
    receipt.raw_mirror_blob_blake3 = Some(evidence.blob_blake3.clone());
    receipt.raw_mirror_blob_size_bytes = Some(evidence.blob_size_bytes);
    receipt.raw_mirror_source_content_blake3 = Some(evidence.source_content_blake3.clone());
    receipt.raw_mirror_source_size_bytes = evidence.source_size_bytes;
    receipt.raw_mirror_storage_kind = Some(evidence.storage_kind.clone());
    receipt.raw_mirror_chunk_count = Some(evidence.chunk_count);
    if evidence.source_mtime_ms.is_none() {
        receipt
            .warnings
            .push("existing raw mirror manifest has no source mtime snapshot".to_string());
    }
}

pub(crate) fn doctor_raw_mirror_backfill_live_source_changed(
    receipt: &mut DoctorRawMirrorBackfillReceipt,
    evidence: &DoctorRawMirrorExistingEvidence,
) -> bool {
    if let Some(snapshot) = receipt.source_stat_snapshot.as_ref()
        && snapshot.file_type == "file"
        && snapshot.content_blake3.is_none()
    {
        receipt.warnings.push(format!(
            "current upstream file could not be checksum-verified against the raw mirror source digest: {}",
            snapshot
                .stat_error
                .as_deref()
                .unwrap_or("source content hash unavailable")
        ));
        return false;
    }
    let changed = receipt
        .source_stat_snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.content_blake3.as_deref())
        .is_some_and(|current_hash| current_hash != evidence.source_content_blake3);
    if changed {
        receipt.warnings.push(
            "current upstream file hash differs from the verified raw mirror source digest; backfill keeps the existing raw mirror evidence rather than recapturing changed live bytes".to_string(),
        );
    }
    changed
}

pub(crate) fn doctor_raw_mirror_backfill_candidate_receipt(
    data_dir: &Path,
    candidate: &DoctorRawMirrorBackfillCandidate,
    by_conversation_id: &HashMap<(i64, String), DoctorRawMirrorExistingEvidence>,
    by_source_key: &HashMap<String, DoctorRawMirrorExistingEvidence>,
    source_stat_cache: &mut HashMap<PathBuf, DoctorRawMirrorBackfillSourceStatSnapshot>,
    apply: bool,
) -> DoctorRawMirrorBackfillReceipt {
    let provider = doctor_normalized_provider_slug(&candidate.provider);
    let origin_host = normalized_provenance_origin_host(candidate.origin_host.as_deref());
    let source_id = normalized_provenance_source_id(
        candidate.source_id.as_str(),
        candidate.origin_kind.as_deref(),
        origin_host.as_deref(),
    );
    let origin_kind =
        normalized_provenance_origin_kind(source_id.as_str(), candidate.origin_kind.as_deref());
    let is_remote = origin_kind != crate::sources::provenance::LOCAL_SOURCE_ID;
    let source_path = doctor_normalized_source_path(candidate.source_path.as_deref());
    let mut receipt = doctor_raw_mirror_backfill_receipt_base(
        data_dir,
        candidate,
        provider.clone(),
        source_id.clone(),
        origin_kind.clone(),
        origin_host.clone(),
        source_path.as_deref(),
    );

    if matches!(provider.as_str(), "shelley" | "grok_bot" | "pi_durable") {
        receipt.action = "disabled_sensitive_container".to_string();
        receipt.warnings.push(
            match provider.as_str() {
                "shelley" => "Shelley stores credentials and application settings alongside conversations. Raw mirror capture and linking are disabled; index the original local database directly with CASS_SHELLEY_DB or a local source path.",
                "grok_bot" => "Grok Bot replicas mix chat with secret and approval payloads. Raw mirror capture and linking are disabled; index the original local replica directly with CASS_GROK_BOT_DATA_ROOT.",
                _ => "Pi durable stores keep system prompts, agent instructions, task checkpoints and application documents alongside conversations. Raw mirror capture and linking are disabled; index the original store directly (default <agent dir>/experimental/durable-sessions, agent dir from PI_CODING_AGENT_DIR) or a local source path.",
            }
            .to_string(),
        );
        return receipt;
    }

    if provider == "unknown" || source_path.is_none() {
        receipt.action = "unknown_mapping_db_projection_only".to_string();
        receipt.db_projection_only = true;
        receipt.warnings.push(
            "archive row lacks enough provider/source_path metadata for safe raw backfill"
                .to_string(),
        );
        return receipt;
    }

    let source_path = source_path.expect("checked source_path is_some");
    if is_remote {
        receipt.action = "remote_source_db_projection_only".to_string();
        receipt.db_projection_only = true;
        receipt.warnings.push(
            "remote source rows are not read from the local filesystem during doctor backfill"
                .to_string(),
        );
        return receipt;
    }

    let path = Path::new(&source_path);
    if !path.is_absolute() {
        receipt.action = "relative_source_path_db_projection_only".to_string();
        receipt.db_projection_only = true;
        receipt.warnings.push(
            "archive row source_path is relative; doctor refuses to resolve it against the current working directory".to_string(),
        );
        return receipt;
    }

    let source_key = doctor_raw_mirror_backfill_source_key(
        &source_id,
        &origin_kind,
        origin_host.as_deref(),
        &source_path,
    );
    let source_stat = source_stat_cache
        .entry(path.to_path_buf())
        .or_insert_with(|| doctor_raw_mirror_backfill_source_stat(path))
        .clone();
    receipt.source_stat_snapshot = Some(source_stat.clone());
    receipt.source_missing = !source_stat.exists;

    if let Some(evidence) =
        by_conversation_id.get(&(candidate.conversation_id, source_path.clone()))
    {
        doctor_raw_mirror_backfill_mark_existing_evidence(&mut receipt, evidence);
        receipt.raw_mirror_db_linked = true;
        receipt.parse_loss_unknown = false;
        receipt.action = if doctor_raw_mirror_backfill_live_source_changed(&mut receipt, evidence) {
            "already_raw_source_captured_live_source_changed".to_string()
        } else {
            "already_raw_source_captured".to_string()
        };
        return receipt;
    }

    if let Some(evidence) = by_source_key.get(&source_key) {
        doctor_raw_mirror_backfill_mark_existing_evidence(&mut receipt, evidence);
        let changed = doctor_raw_mirror_backfill_live_source_changed(&mut receipt, evidence);
        if evidence.db_linked {
            receipt.action = if changed {
                "existing_raw_manifest_live_source_changed_needs_db_link".to_string()
            } else {
                "existing_raw_manifest_needs_db_link".to_string()
            };
        } else {
            receipt.action = if changed {
                "unlinked_raw_manifest_live_source_changed_needs_db_link".to_string()
            } else {
                "unlinked_raw_manifest_needs_db_link".to_string()
            };
        }
        if apply {
            let link = doctor_raw_mirror_backfill_db_link(candidate, Some(&source_path));
            match crate::raw_mirror::merge_manifest_db_links(
                data_dir,
                &evidence.manifest_relative_path,
                &[link],
            ) {
                Ok(()) => {
                    receipt.raw_mirror_db_linked = true;
                    receipt.action = if changed {
                        "linked_existing_raw_manifest_live_source_changed".to_string()
                    } else {
                        "linked_existing_raw_manifest".to_string()
                    };
                }
                Err(err) => {
                    receipt.action = "existing_raw_manifest_link_failed".to_string();
                    receipt.warnings.push(format!(
                        "failed to link existing raw mirror manifest to archive row: {err}"
                    ));
                }
            }
        }
        return receipt;
    }

    if !source_stat.exists {
        receipt.action = "source_missing_db_projection_only".to_string();
        receipt.source_missing = true;
        receipt.db_projection_only = true;
        receipt.warnings.push(
            "upstream source file is missing; cass archive DB rows may be the only remaining copy and no raw upstream evidence is being fabricated".to_string(),
        );
        return receipt;
    }
    if source_stat.file_type != "file" {
        receipt.action = "non_file_source_db_projection_only".to_string();
        receipt.db_projection_only = true;
        receipt.warnings.push(format!(
            "source_path currently resolves to {}; doctor raw backfill only captures regular files",
            source_stat.file_type
        ));
        return receipt;
    }
    if source_stat.content_blake3.is_none() {
        receipt.action = "source_content_unverified_db_projection_only".to_string();
        receipt.db_projection_only = true;
        receipt.warnings.push(format!(
            "source file metadata was readable but its contents could not be checksum-verified; doctor refuses raw-mirror capture: {}",
            source_stat
                .stat_error
                .as_deref()
                .unwrap_or("source content hash unavailable")
        ));
        return receipt;
    }

    if !crate::raw_mirror::capture_enabled() {
        // GH #506: the operator turned capture off. The source is capturable,
        // so this is neither a projection-only row nor a failure.
        receipt.action = "capture_disabled".to_string();
        receipt.warnings.push(
            "raw mirror capture is off (CASS_RAW_MIRROR); doctor does not copy the live source file"
                .to_string(),
        );
        return receipt;
    }

    if !apply {
        receipt.action = "would_capture_live_source".to_string();
        return receipt;
    }

    let link = doctor_raw_mirror_backfill_db_link(candidate, Some(&source_path));
    match crate::raw_mirror::capture_source_file(crate::raw_mirror::RawMirrorCaptureInput {
        data_dir,
        provider: &provider,
        source_id: &source_id,
        origin_kind: &origin_kind,
        origin_host: origin_host.as_deref(),
        source_path: path,
        db_links: &[link],
    }) {
        Ok(record) => {
            receipt.action = if record.already_present {
                "captured_live_source_already_present".to_string()
            } else {
                "captured_live_source".to_string()
            };
            receipt.raw_source_captured = true;
            receipt.raw_mirror_db_linked = true;
            if let Some(snapshot) = receipt.source_stat_snapshot.as_mut() {
                snapshot.size_bytes = Some(record.source_size_bytes);
                snapshot.modified_at_ms = record.source_mtime_ms;
                snapshot.content_blake3 = Some(record.source_content_blake3.clone());
            }
            receipt.captured_at_ms = Some(record.captured_at_ms);
            receipt.raw_mirror_manifest_id = Some(record.manifest_id);
            receipt.raw_mirror_manifest_relative_path = Some(record.manifest_relative_path);
            receipt.raw_mirror_blob_blake3 = Some(record.blob_blake3);
            receipt.raw_mirror_blob_size_bytes = Some(record.blob_size_bytes);
            receipt.raw_mirror_source_content_blake3 = Some(record.source_content_blake3);
            receipt.raw_mirror_source_size_bytes = Some(record.source_size_bytes);
            receipt.raw_mirror_storage_kind = Some(record.storage_kind);
            receipt.raw_mirror_chunk_count = Some(record.chunk_count);
        }
        Err(err) => {
            receipt.action = "capture_live_source_failed".to_string();
            receipt.warnings.push(format!(
                "failed to capture live source into raw mirror; provider file was not modified: {err}"
            ));
        }
    }

    receipt
}

pub(crate) fn doctor_raw_mirror_backfill_receipt_would_mutate(
    receipt: &DoctorRawMirrorBackfillReceipt,
) -> bool {
    receipt.action == "would_capture_live_source" || receipt.action.contains("_needs_db_link")
}

pub(crate) fn accumulate_doctor_raw_mirror_backfill_receipt(
    report: &mut DoctorRawMirrorBackfillReport,
    receipt: DoctorRawMirrorBackfillReceipt,
) {
    if receipt.action.starts_with("already_raw_source_captured") {
        report.already_raw_source_captured_count += 1;
    }
    if receipt.action == "would_capture_live_source"
        || receipt.action == "captured_live_source"
        || receipt.action == "captured_live_source_already_present"
        || receipt.action == "blocked_forensic_bundle_capture_failed"
    {
        report.eligible_live_source_count += 1;
    }
    if receipt.action.starts_with("linked_existing_raw_manifest")
        || receipt.action.contains("_needs_db_link")
    {
        report.existing_raw_manifest_link_count += 1;
    }
    if receipt.action == "captured_live_source"
        || receipt.action == "captured_live_source_already_present"
    {
        report.captured_live_source_count += 1;
    }
    if receipt.action == "captured_live_source_already_present" {
        report.already_present_capture_count += 1;
    }
    if receipt.source_missing {
        report.source_missing_count += 1;
    }
    if receipt.db_projection_only {
        report.db_projection_only_count += 1;
    }
    if receipt.action == "remote_source_db_projection_only"
        || receipt.action == "unknown_mapping_db_projection_only"
        || receipt.action == "relative_source_path_db_projection_only"
    {
        report.remote_or_unknown_mapping_count += 1;
    }
    if receipt.action.contains("live_source_changed") {
        report.changed_source_hash_count += 1;
    }
    if receipt.action.ends_with("_failed") {
        report.capture_failure_count += 1;
    }
    report.receipts.push(receipt);
}

pub(crate) fn collect_doctor_raw_mirror_backfill_report(
    data_dir: &Path,
    db_path: &Path,
    raw_mirror: &DoctorRawMirrorReport,
    apply: bool,
) -> DoctorRawMirrorBackfillReport {
    let mut report = DoctorRawMirrorBackfillReport {
        schema_version: 1,
        backfill_generation: DOCTOR_RAW_MIRROR_BACKFILL_GENERATION,
        mode: if apply {
            "safe_auto_run".to_string()
        } else {
            "check".to_string()
        },
        status: "skipped".to_string(),
        read_only_external_source_dirs: true,
        notes: vec![
            "Backfill only writes cass raw-mirror manifests/blobs and db_links; it never creates, rewrites, moves, or deletes provider session logs.".to_string(),
            "DB-only projections are explicitly lower-authority evidence and are never marked as raw upstream captures.".to_string(),
            "parse_loss_unknown=true means doctor captured current live bytes after the archive row already existed, so it cannot prove those bytes are identical to the original parse input.".to_string(),
        ],
        forensic_bundle: doctor_forensic_bundle_uncaptured("not_required_read_only_or_no_mutation"),
        ..DoctorRawMirrorBackfillReport::default()
    };

    if !db_path.exists() {
        report.status = "skipped".to_string();
        report
            .notes
            .push("Backfill skipped until the cass archive database exists.".to_string());
        return report;
    }

    let query_result = open_franken_cli_read_db(
        db_path.to_path_buf(),
        "doctor raw mirror backfill",
        Duration::from_secs(1),
    )
    .and_then(|conn| {
        let query_result =
            query_doctor_raw_mirror_backfill_candidates(&conn).map_err(|e| CliError {
                code: 9,
                kind: CliErrorKind::DbQuery.kind_str(),
                message: format!("Failed to query doctor raw mirror backfill candidates: {e}"),
                hint: None,
                retryable: false,
            });
        let close_result = close_franken_cli_read_db(conn, db_path, "doctor raw mirror backfill");
        match (query_result, close_result) {
            (Ok(rows), Ok(())) => Ok(rows),
            (Err(err), _) | (_, Err(err)) => Err(err),
        }
    });

    let candidates = match query_result {
        Ok(candidates) => {
            report.db_available = true;
            candidates
        }
        Err(err) => {
            report.db_available = false;
            report.status = "warn".to_string();
            report.db_query_error = Some(err.message.clone());
            report
                .warnings
                .push("Could not query archive rows for raw mirror backfill.".to_string());
            return report;
        }
    };

    let (by_conversation_id, by_source_key) = doctor_raw_mirror_existing_evidence_maps(raw_mirror);
    let mut source_stat_cache: HashMap<PathBuf, DoctorRawMirrorBackfillSourceStatSnapshot> =
        HashMap::new();
    if apply {
        let dry_run_receipts = candidates
            .iter()
            .map(|candidate| {
                doctor_raw_mirror_backfill_candidate_receipt(
                    data_dir,
                    candidate,
                    &by_conversation_id,
                    &by_source_key,
                    &mut source_stat_cache,
                    false,
                )
            })
            .collect::<Vec<_>>();
        if dry_run_receipts
            .iter()
            .any(doctor_raw_mirror_backfill_receipt_would_mutate)
        {
            report.forensic_bundle =
                capture_doctor_forensic_bundle(DoctorForensicBundleCaptureInput {
                    operation_id: "doctor-raw-mirror-backfill",
                    data_dir,
                    db_path,
                    index_path: &crate::search::tantivy::expected_index_dir(data_dir),
                    plan: None,
                    quarantine_report: None,
                    extra_file_artifacts: &[],
                });
            if report.forensic_bundle.status != "captured" {
                let reason = report
                    .forensic_bundle
                    .blocked_reason
                    .clone()
                    .unwrap_or_else(|| "forensic bundle capture did not complete".to_string());
                let blocked_reason =
                    format!("forensic bundle capture failed before raw mirror backfill: {reason}");
                report.status = "blocked".to_string();
                report.warnings.push(blocked_reason.clone());
                for mut receipt in dry_run_receipts {
                    if doctor_raw_mirror_backfill_receipt_would_mutate(&receipt) {
                        receipt.action = "blocked_forensic_bundle_capture_failed".to_string();
                        receipt.warnings.push(blocked_reason.clone());
                        receipt.forensic_bundle = report.forensic_bundle.clone();
                    }
                    accumulate_doctor_raw_mirror_backfill_receipt(&mut report, receipt);
                }
                report.total_candidate_count = report.receipts.len();
                return report;
            }
        }
    }

    for candidate in &candidates {
        let mut receipt = doctor_raw_mirror_backfill_candidate_receipt(
            data_dir,
            candidate,
            &by_conversation_id,
            &by_source_key,
            &mut source_stat_cache,
            apply,
        );
        let mutating_receipt = receipt.raw_source_captured
            || receipt.raw_mirror_db_linked
            || receipt.action == "capture_live_source_failed"
            || receipt.action == "existing_raw_manifest_link_failed";
        if report.forensic_bundle.status == "captured" && mutating_receipt {
            receipt.forensic_bundle = report.forensic_bundle.clone();
        }
        accumulate_doctor_raw_mirror_backfill_receipt(&mut report, receipt);
    }
    report.total_candidate_count = report.receipts.len();

    if report.capture_failure_count > 0 {
        report.status = "partial".to_string();
        report.warnings.push(
            "Some raw mirror backfill actions failed; no provider source files were modified."
                .to_string(),
        );
    } else if apply
        && (report.captured_live_source_count > 0 || report.existing_raw_manifest_link_count > 0)
    {
        report.status = "applied".to_string();
    } else if !apply
        && (report.eligible_live_source_count > 0 || report.existing_raw_manifest_link_count > 0)
    {
        report.status = "planned".to_string();
    } else if report.db_projection_only_count > 0 || report.changed_source_hash_count > 0 {
        report.status = "warn".to_string();
    } else {
        report.status = "clean".to_string();
    }

    if report.source_missing_count > 0 {
        report.warnings.push(format!(
            "{} archive conversation(s) have missing upstream files and remain DB-only unless a prior raw mirror manifest can be linked",
            report.source_missing_count
        ));
    }
    if report.changed_source_hash_count > 0 {
        report.warnings.push(format!(
            "{} archive conversation(s) have live source bytes that differ from already preserved raw mirror evidence",
            report.changed_source_hash_count
        ));
    }

    report
}

pub(crate) fn doctor_raw_mirror_backfill_deferred(
    apply: bool,
    reason: &str,
    forensic_reason: &'static str,
) -> DoctorRawMirrorBackfillReport {
    DoctorRawMirrorBackfillReport {
        schema_version: 1,
        backfill_generation: DOCTOR_RAW_MIRROR_BACKFILL_GENERATION,
        mode: if apply {
            "safe_auto_run".to_string()
        } else {
            "check".to_string()
        },
        status: "deferred".to_string(),
        db_available: false,
        db_query_error: Some(reason.to_string()),
        read_only_external_source_dirs: true,
        forensic_bundle: doctor_forensic_bundle_uncaptured(forensic_reason),
        warnings: vec![reason.to_string()],
        notes: vec![
            "Archive-wide raw-mirror backfill candidates were not scanned; zero-valued candidate and coverage fields are unknown, not evidence of absence."
                .to_string(),
            "No provider source, raw-mirror, archive, or candidate bytes were mutated."
                .to_string(),
        ],
        ..DoctorRawMirrorBackfillReport::default()
    }
}

pub(crate) fn collect_doctor_raw_mirror_backfill_report_for_doctor(
    data_dir: &Path,
    db_path: &Path,
    raw_mirror: &DoctorRawMirrorReport,
    apply: bool,
    archive_wide_collectors_deferred_reason: Option<&str>,
) -> DoctorRawMirrorBackfillReport {
    if let Some(reason) = archive_wide_collectors_deferred_reason {
        return doctor_raw_mirror_backfill_deferred(
            apply,
            reason,
            "archive_wide_collectors_deferred",
        );
    }
    if raw_mirror.status == "verification_deferred" {
        return doctor_raw_mirror_backfill_deferred(
            apply,
            "raw mirror verification was deferred by the bounded read-only doctor limits; backfill is also deferred because unverified manifests must not be treated as absent evidence",
            "raw_mirror_verification_deferred",
        );
    }
    collect_doctor_raw_mirror_backfill_report(data_dir, db_path, raw_mirror, apply)
}

pub(crate) fn doctor_coverage_confidence_tier(
    archive_conversation_count: usize,
    db_projection_only_count: usize,
    db_without_raw_mirror_count: usize,
    missing_current_source_count: usize,
    raw_mirror_db_link_count: usize,
    mirror_without_db_link_count: usize,
    current_source_newer_than_archive_count: usize,
) -> String {
    if db_projection_only_count > 0 && missing_current_source_count > 0 {
        "sole_copy_db_projection".to_string()
    } else if missing_current_source_count > 0 && raw_mirror_db_link_count > 0 {
        "sole_copy_verified_raw_mirror".to_string()
    } else if db_without_raw_mirror_count > 0 {
        "archive_db_without_raw_mirror".to_string()
    } else if mirror_without_db_link_count > 0 {
        "raw_mirror_unlinked".to_string()
    } else if archive_conversation_count == 0 {
        "no_archive_rows".to_string()
    } else if current_source_newer_than_archive_count > 0 {
        "current_source_newer_than_archive".to_string()
    } else if raw_mirror_db_link_count >= archive_conversation_count {
        "verified_raw_mirror_coverage".to_string()
    } else {
        "archive_db_coverage".to_string()
    }
}
