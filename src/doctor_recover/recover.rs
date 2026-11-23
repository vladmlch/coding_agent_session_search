use std::path::PathBuf;

use crate::storage::sqlite::{FrankenStorage, RecoveryConversationRow};
use crate::{CliResult, RobotFormat, default_data_dir};

use super::{
    RECOVER_CONVERSATION_PAGE, ReconstructedSession, io_error, print_json,
    reconstruction_output_path, resolve_db_path, storage_error,
};

/// Rebuild the source JSONL tree from the canonical archive's preserved events.
///
/// `target_dir` receives one `.jsonl` file per reconstructable conversation.
/// The canonical archive is opened read-only and never mutated. After this
/// completes the operator can `cass index --full` over `target_dir` to produce
/// a fresh frankensqlite-native archive.
pub fn run_doctor_recover_from_archive(
    data_dir_override: Option<PathBuf>,
    db_override: Option<PathBuf>,
    target_dir: PathBuf,
    structured_format: Option<RobotFormat>,
) -> CliResult<()> {
    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    let db_path = resolve_db_path(&data_dir, db_override.as_deref());

    if !db_path.exists() {
        return Err(storage_error(
            format!(
                "canonical archive {} does not exist; nothing to recover from",
                db_path.display()
            ),
            Some(
                "Point --db at the archive, or restore a backup with 'cass doctor backups restore'.",
            ),
        ));
    }

    // Read-only open: recovery must never widen the corruption or take a write
    // lock on a fragile archive.
    let storage = FrankenStorage::open_readonly(&db_path).map_err(|e| {
        storage_error(
            format!(
                "could not open canonical archive {} read-only for recovery: {e:#}",
                db_path.display()
            ),
            Some(
                "If even read-only open fails, the page store itself is unreadable; restore from a \
                 backup ('cass doctor backups list') or a remote mirror.",
            ),
        )
    })?;

    // The count is reporting only; on a damaged tree it may itself fail, and
    // that must not stop the row-by-row export below (#391).
    let (total, total_count_error) = match storage.total_conversation_count() {
        Ok(total) => (Some(total), None),
        Err(e) => (None, Some(format!("{e:#}"))),
    };

    std::fs::create_dir_all(&target_dir).map_err(|e| {
        io_error(
            format!(
                "could not create recovery target dir {}: {e}",
                target_dir.display()
            ),
            None,
        )
    })?;

    let mut results: Vec<ReconstructedSession> = Vec::new();
    let mut written = 0usize;
    let mut skipped = 0usize;
    let mut unreadable_rows = 0usize;
    let mut quarantined_rows = 0usize;
    let mut coerced_rows = 0usize;
    let mut total_lines = 0usize;

    // Keyset pagination by `id`: no `ORDER BY started_at` sort over a possibly
    // damaged tree, and a page is never re-read after a partial failure.
    let mut after_id: i64 = 0;
    loop {
        let rows = storage
            .list_conversations_for_recovery(after_id, RECOVER_CONVERSATION_PAGE)
            .map_err(|e| {
                storage_error(
                    format!("listing conversations after id {after_id}: {e:#}"),
                    Some(
                        "The page walk itself failed inside the engine, so the rows after this id cannot be reached read-only. Sessions already written are complete; recover the rest from a backup ('cass doctor backups list') or a remote mirror.",
                    ),
                )
            })?;
        if rows.is_empty() {
            break;
        }
        let page_len = rows.len() as i64;
        let page_start_id = after_id;
        let mut page_saw_unreadable = false;

        for row in rows {
            let (conversation, coercions) = match row {
                RecoveryConversationRow::Readable {
                    conversation,
                    coercions,
                } => (*conversation, coercions),
                RecoveryConversationRow::Quarantined { id, coercions } => {
                    // #391: identity types are inconsistent with CASS's
                    // contract, so messages cannot safely be attributed to
                    // this row. The types alone do not diagnose page aliasing.
                    // Count it, keep paging past its id, export nothing.
                    after_id = after_id.max(id);
                    skipped += 1;
                    quarantined_rows += 1;
                    results.push(ReconstructedSession {
                        conversation_id: Some(id),
                        external_id: None,
                        relative_or_source_path: format!("<quarantined row: id {id}>"),
                        written_path: None,
                        line_count: 0,
                        skipped_reason: Some(
                            "quarantined canonical row: identity columns violate the text/path \
                             contract; message ownership cannot be verified"
                                .to_string(),
                        ),
                        coercions,
                    });
                    continue;
                }
                RecoveryConversationRow::Unreadable { stored_id, reason } => {
                    // #391: a row whose id is not an integer cannot be addressed
                    // for message reconstruction. Record it and keep exporting
                    // the rest instead of aborting with nothing written.
                    skipped += 1;
                    unreadable_rows += 1;
                    page_saw_unreadable = true;
                    results.push(ReconstructedSession {
                        conversation_id: None,
                        external_id: None,
                        relative_or_source_path: format!("<unreadable row: id {stored_id}>"),
                        written_path: None,
                        line_count: 0,
                        skipped_reason: Some(format!("unreadable canonical row: {reason}")),
                        coercions: Vec::new(),
                    });
                    continue;
                }
            };
            let Some(conversation_id) = conversation.id else {
                continue;
            };
            after_id = after_id.max(conversation_id);
            if !coercions.is_empty() {
                coerced_rows += 1;
            }
            let source_path_display = conversation.source_path.display().to_string();

            let lines = match storage.reconstruct_source_jsonl_for_conversation(conversation_id) {
                Ok(lines) => lines,
                Err(e) => {
                    skipped += 1;
                    results.push(ReconstructedSession {
                        conversation_id: Some(conversation_id),
                        external_id: conversation.external_id.clone(),
                        relative_or_source_path: source_path_display,
                        written_path: None,
                        line_count: 0,
                        skipped_reason: Some(format!("reconstruct failed: {e:#}")),
                        coercions,
                    });
                    continue;
                }
            };

            if lines.is_empty() {
                skipped += 1;
                results.push(ReconstructedSession {
                    conversation_id: Some(conversation_id),
                    external_id: conversation.external_id.clone(),
                    relative_or_source_path: source_path_display,
                    written_path: None,
                    line_count: 0,
                    skipped_reason: Some(
                        "no preserved source events (extra_json/extra_bin) to reconstruct"
                            .to_string(),
                    ),
                    coercions,
                });
                continue;
            }

            let out_path = reconstruction_output_path(
                &target_dir,
                conversation_id,
                conversation.external_id.as_deref(),
                &conversation.source_path,
            );

            let mut body = lines.join("\n");
            body.push('\n');
            std::fs::write(&out_path, body.as_bytes()).map_err(|e| {
                io_error(
                    format!(
                        "writing reconstructed session to {}: {e}",
                        out_path.display()
                    ),
                    None,
                )
            })?;

            written += 1;
            total_lines += lines.len();
            results.push(ReconstructedSession {
                conversation_id: Some(conversation_id),
                external_id: conversation.external_id.clone(),
                relative_or_source_path: source_path_display,
                written_path: Some(out_path),
                line_count: lines.len(),
                skipped_reason: None,
                coercions,
            });
        }

        if page_len < RECOVER_CONVERSATION_PAGE {
            break;
        }
        // `ORDER BY c.id` sorts every non-integer id after all integer ids,
        // so a page that carried an unreadable row has already reached the
        // end of the addressable rows; and a page that advanced nothing would
        // be re-read forever. Stop rather than loop or double-report.
        if page_saw_unreadable || after_id == page_start_id {
            break;
        }
    }

    let envelope = serde_json::json!({
        "schema_version": 1,
        "doctor_contract_version": 1,
        "kind": "recover_from_archive",
        "db_path": db_path.display().to_string(),
        "target_dir": target_dir.display().to_string(),
        "conversations_total": total,
        "conversations_total_error": total_count_error,
        "sessions_written": written,
        "sessions_skipped": skipped,
        // #391: rows the strict reader would have aborted the export on.
        "rows_unreadable": unreadable_rows,
        "rows_quarantined": quarantined_rows,
        "rows_coerced": coerced_rows,
        "lines_written": total_lines,
        "sessions": results
            .iter()
            .map(|r| serde_json::json!({
                "conversation_id": r.conversation_id,
                "external_id": r.external_id,
                "source_path": r.relative_or_source_path,
                "written_path": r.written_path.as_ref().map(|p| p.display().to_string()),
                "line_count": r.line_count,
                "skipped_reason": r.skipped_reason,
                "coercions": r.coercions,
            }))
            .collect::<Vec<_>>(),
        "next_action": format!(
            "Re-ingest the recovered tree with: cass index --full --data-dir <fresh-data-dir> (point the source scan at {})",
            target_dir.display()
        ),
        "note": "Reconstructed verbatim from the canonical archive's preserved extra_json/extra_bin envelopes. The corrupt archive was opened read-only and never mutated; no stock-sqlite .recover was required.",
    });

    if structured_format.is_some() {
        print_json(&envelope)?;
    } else {
        println!(
            "Recovered {written} session(s) ({total_lines} lines) into {}",
            target_dir.display()
        );
        if skipped > 0 {
            println!(
                "  {skipped} conversation(s) were skipped (no preserved events, unreadable, or quarantined)."
            );
        }
        if unreadable_rows > 0 || quarantined_rows > 0 || coerced_rows > 0 {
            println!(
                "  Damaged canonical rows tolerated: {unreadable_rows} unreadable, {quarantined_rows} quarantined (identity columns mis-typed), {coerced_rows} with coerced column types (see --json for details)."
            );
        }
        println!(
            "Next: re-ingest with 'cass index --full' over {} into a fresh data dir.",
            target_dir.display()
        );
    }
    Ok(())
}
