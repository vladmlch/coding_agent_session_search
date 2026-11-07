use crate::cli::args::Cli;
use crate::cli::args_sub::{ImportCommand, MirrorCommand, QuarantineCommand, RobotFormat};
use crate::cli::commands::import_chatgpt_export::import_chatgpt_export;
use crate::cli::commands::run_mirror_prune::run_mirror_prune;
use crate::cli::commands::search::fieldbudgets::robot_format_from_env;
use crate::cli::commands::search::toon_encode_options_from_env::output_structured_value;
use crate::cli::error::{CliError, CliResult};
use crate::cli::output::describe_command::resolve_subcommand_structured_format;
use crate::cli::paths::default_data_dir;
use crate::cli::state::probe_state_db_strict_worker::error_chain_indicates_active_cass_index;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

pub(crate) async fn handle_import(cmd: ImportCommand, cli: &Cli) -> CliResult<()> {
    match cmd {
        ImportCommand::Chatgpt {
            path,
            output_dir,
            json,
        } => {
            let structured_format = resolve_subcommand_structured_format(cli, json);
            import_chatgpt_export(&path, output_dir.as_deref(), structured_format).await
        }
    }
}

pub(crate) fn run_mirror_command(cmd: MirrorCommand, cli: &Cli) -> CliResult<()> {
    let MirrorCommand::Prune { json, .. } = &cmd;
    let structured_format = resolve_subcommand_structured_format(cli, *json);
    run_mirror_prune(cmd, structured_format)
}

/// `cass quarantine` (#292 ask #3): inspect and manage the
/// conversation-ingest quarantine without hand-editing
/// `quarantine_state.json`. `list` is read-only; `clear` is dry-run by
/// default and only mutates with `--apply`.
pub(crate) fn run_quarantine_command(cmd: QuarantineCommand, cli: &Cli) -> CliResult<()> {
    match cmd {
        QuarantineCommand::List { data_dir, json } => {
            let structured_format = resolve_subcommand_structured_format(cli, json);
            run_quarantine_list(data_dir, structured_format)
        }
        QuarantineCommand::Clear {
            data_dir,
            filter,
            apply,
            json,
        } => {
            let structured_format = resolve_subcommand_structured_format(cli, json);
            run_quarantine_clear(data_dir, filter, apply, structured_format)
        }
        QuarantineCommand::Retry {
            data_dir,
            max_attempts,
            force_irreducible,
            apply,
            json,
        } => {
            let structured_format = resolve_subcommand_structured_format(cli, json);
            run_quarantine_retry_command(
                data_dir,
                max_attempts,
                force_irreducible,
                apply,
                structured_format,
            )
        }
    }
}

pub(crate) fn quarantine_apply_cli_error(operation: &str, err: anyhow::Error) -> CliError {
    let rendered = format!("{err:#}");
    if error_chain_indicates_active_cass_index(&rendered) {
        return CliError {
            code: 7,
            kind: "lock-busy",
            message: format!(
                "quarantine {operation} could not start because another index/watch run is active: {rendered}"
            ),
            hint: Some(
                "Wait for the active index/watch process to finish, then retry the quarantine command."
                    .to_string(),
            ),
            retryable: true,
        };
    }

    CliError {
        code: 9,
        kind: "quarantine",
        message: format!("quarantine {operation} failed: {rendered}"),
        hint: Some("Run `cass quarantine list --json` to inspect current entries.".to_string()),
        retryable: false,
    }
}

/// `cass quarantine retry`: a bounded, resumable retry of retry-eligible
/// quarantined conversations (#292 ask #3). Dry-run emits the exact plan;
/// `--apply` reparses and persists each planned conversation by quarantine key.
pub(crate) fn run_quarantine_retry_command(
    data_dir_override: Option<PathBuf>,
    max_attempts: Option<usize>,
    force_irreducible: bool,
    apply: bool,
    output_format: Option<RobotFormat>,
) -> CliResult<()> {
    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    let config = crate::indexer::quarantine_retry::RetryConfig {
        max_attempts,
        eligible_only: !force_irreducible,
    };

    if !apply {
        let plan = crate::indexer::plan_quarantine_retry(&data_dir, &config)
            .map_err(|err| quarantine_apply_cli_error("retry plan", err))?;
        let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
            if matches!(fmt, RobotFormat::Sessions) {
                RobotFormat::Compact
            } else {
                fmt
            }
        });
        if let Some(fmt) = structured_format {
            let mut payload = serde_json::to_value(&plan).unwrap_or_else(|_| serde_json::json!({}));
            if let Some(obj) = payload.as_object_mut() {
                obj.insert("schema_version".to_string(), serde_json::json!(1));
                obj.insert("applied".to_string(), serde_json::json!(false));
                obj.insert("dry_run".to_string(), serde_json::json!(true));
                obj.insert(
                    "data_dir".to_string(),
                    serde_json::json!(data_dir.display().to_string()),
                );
            }
            return output_structured_value(payload, fmt);
        }

        println!("CASS Quarantine Retry");
        println!("=====================");
        println!();
        println!("Mode: dry-run (read-only plan)");
        println!();
        println!("{}", plan.summary);
        println!(
            "  planned attempts: {}, irreducible: {}, source-missing: {}",
            plan.planned_attempts, plan.skip_irreducible, plan.skip_source_missing
        );
        if plan.resume_recommended {
            println!("  (budget reached — re-run to plan the remaining eligible entries)");
        }
        println!();
        println!("Next: {}", plan.next_safe_command);
        return Ok(());
    }

    let report = crate::indexer::run_quarantine_retry(&data_dir, &config, true)
        .map_err(|err| quarantine_apply_cli_error("retry", err))?;

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(fmt) = structured_format {
        let mut payload = serde_json::to_value(&report).unwrap_or_else(|_| serde_json::json!({}));
        if let Some(obj) = payload.as_object_mut() {
            obj.insert("schema_version".to_string(), serde_json::json!(1));
            obj.insert("applied".to_string(), serde_json::json!(true));
            obj.insert("dry_run".to_string(), serde_json::json!(false));
            obj.insert(
                "data_dir".to_string(),
                serde_json::json!(data_dir.display().to_string()),
            );
        }
        return output_structured_value(payload, fmt);
    }

    println!("CASS Quarantine Retry");
    println!("=====================");
    println!();
    println!("Mode: APPLY (targeted re-ingest by quarantine key)");
    println!();
    println!("{}", report.summary);
    println!(
        "  re-ingested and cleared: {}, remaining quarantined: {}",
        report.cleared, report.remaining_quarantined
    );
    if report.resume_recommended {
        println!("  (budget reached — re-run to resume the remaining eligible entries)");
    }
    println!();
    println!("Next: {}", report.next_safe_command);
    Ok(())
}

/// Build the deterministic `cass quarantine list` rows: the structured state
/// merged with the poison ledgers. Each entry reports its conversation_id,
/// schema version, attempt count, last reason, the wall-clock timestamps, and
/// whether it is retry-eligible under the current binary (version-stale/legacy).
pub(crate) fn quarantine_list_entries(
    data_dir: &Path,
) -> anyhow::Result<Vec<crate::search::quarantine_status::QuarantineListEntry>> {
    use crate::indexer::quarantine::QuarantineState;
    use crate::search::quarantine_status::QuarantineListEntry;
    let state = QuarantineState::load_for_operator(data_dir)
        .map_err(|error| anyhow::anyhow!("loading quarantine state for operator list: {error}"))?;
    let current_version = env!("CARGO_PKG_VERSION");
    let mut entries = state
        .iter()
        .map(|(key, record)| {
            let identity = (key.conversation_id.clone(), i64::from(key.schema_version));
            let entry = QuarantineListEntry {
                conversation_id: key.conversation_id,
                schema_version: i64::from(key.schema_version),
                attempt_count: record.attempt_count,
                last_reason: record.last_reason.clone(),
                first_attempt_at: Some(record.first_attempt_at.to_rfc3339()),
                last_attempt_at: Some(record.last_attempt_at.to_rfc3339()),
                cass_version_at_quarantine: record.cass_version_at_quarantine.clone(),
                retry_eligible: record.is_version_stale_for_retry(current_version),
            };
            (identity, entry)
        })
        .collect::<BTreeMap<_, _>>();

    for (identity, record) in crate::indexer::operator_quarantine_poison_records(data_dir)? {
        let first_attempt_at = record
            .get("first_quarantined_at_ms")
            .and_then(serde_json::Value::as_i64)
            .or_else(|| {
                record
                    .get("recorded_at_ms")
                    .and_then(serde_json::Value::as_i64)
            })
            .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis)
            .map(|timestamp| timestamp.to_rfc3339());
        let last_attempt_at = record
            .get("last_attempt_at_ms")
            .and_then(serde_json::Value::as_i64)
            .or_else(|| {
                record
                    .get("recorded_at_ms")
                    .and_then(serde_json::Value::as_i64)
            })
            .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis)
            .map(|timestamp| timestamp.to_rfc3339());
        let cass_version = record
            .get("cass_version_at_quarantine")
            .and_then(serde_json::Value::as_str);
        let attempt_count = record
            .get("attempt_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(1);
        let retry_eligible = cass_version != Some(current_version);
        let poison_entry = QuarantineListEntry {
            conversation_id: identity.0.clone(),
            schema_version: identity.1,
            attempt_count,
            last_reason: record
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("ingest-out-of-memory")
                .to_string(),
            first_attempt_at,
            last_attempt_at,
            cass_version_at_quarantine: cass_version.map(str::to_string),
            retry_eligible,
        };

        match entries.entry(identity) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(poison_entry);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let existing = entry.get_mut();
                existing.attempt_count = existing.attempt_count.max(attempt_count);
                if !retry_eligible {
                    // Cross-surface saves are best-effort. If either surface
                    // records a retry under this binary, do not falsely offer
                    // the key as retry-eligible because the other is stale.
                    existing.cass_version_at_quarantine = Some(current_version.to_string());
                    existing.retry_eligible = false;
                }
            }
        }
    }

    Ok(entries.into_values().collect())
}

pub(crate) fn run_quarantine_list(
    data_dir_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
) -> CliResult<()> {
    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    let entries = quarantine_list_entries(&data_dir).map_err(|err| CliError {
        code: 9,
        kind: "quarantine",
        message: format!("quarantine list failed: {err:#}"),
        hint: Some("Check permissions on the quarantine files, then retry.".to_string()),
        retryable: false,
    })?;
    let summary = crate::indexer::conversation_ingest_quarantine_summary(&data_dir);
    let status = crate::search::quarantine_status::quarantine_status(
        &entries,
        env!("CARGO_PKG_VERSION"),
        &crate::indexer::quarantine_source_missing_ids(&data_dir),
    );
    // GH#510: conversations the watcher keeps deferring after a bounded-guard
    // NoMem are not quarantined, but they are missing from search until one
    // ingests.
    let deferred: Vec<_> = crate::indexer::WatchNomemDeferrals::load(&data_dir)
        .entries
        .into_values()
        .collect();
    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(fmt) = structured_format {
        let payload = serde_json::json!({
            "schema_version": 1,
            "data_dir": data_dir.display().to_string(),
            "quarantined_conversations": entries.len(),
            "circuit_breaker_active": summary.circuit_breaker_active,
            "recommended_action": summary.recommended_action,
            "status": status,
            "entries": entries,
            "deferred": deferred,
        });
        return output_structured_value(payload, fmt);
    }

    println!("CASS Conversation Quarantine");
    println!("============================");
    println!();
    if !deferred.is_empty() {
        println!(
            "Deferred by the watcher after repeated NoMem (not quarantined): {}",
            deferred.len()
        );
        for entry in &deferred {
            println!(
                "  {} ({}, deferrals={})",
                entry.source_path, entry.agent, entry.deferrals
            );
        }
        println!(
            "  An FSQLITE_PAGE_BUFFER_MAX above the database size removes the common trigger."
        );
        println!();
    }
    if entries.is_empty() {
        println!("No quarantined conversations.");
        return Ok(());
    }
    println!("Quarantined conversations: {}", entries.len());
    if summary.circuit_breaker_active {
        println!("Circuit breaker: ACTIVE (a recent burst of quarantines tripped the breaker)");
    }
    println!("{}", status.eligibility_reason);
    for (cause, count) in &status.by_cause {
        println!("  {count:>6}  {cause}");
    }
    println!();
    for entry in &entries {
        println!(
            "  {} (schema v{}, attempts={}, retry_eligible={})",
            entry.conversation_id, entry.schema_version, entry.attempt_count, entry.retry_eligible,
        );
        println!("      reason: {}", entry.last_reason);
    }
    if let Some(next) = &status.next_safe_command {
        println!();
        println!("Next: {next}");
    }
    Ok(())
}

pub(crate) fn run_quarantine_clear(
    data_dir_override: Option<PathBuf>,
    filter: Option<String>,
    apply: bool,
    output_format: Option<RobotFormat>,
) -> CliResult<()> {
    use crate::indexer::quarantine::{QuarantineKey, QuarantineState};

    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    // Clearing quarantine changes only the ledgers under the data directory;
    // unlike retrying a quarantined source, it never writes the archive.
    let _mutation_guard = apply
        .then(|| {
            crate::indexer::acquire_search_asset_mutation_lock(
                &data_dir,
                &data_dir.join("agent_search.db"),
                crate::search::asset_state::SearchMaintenanceJobKind::LexicalRefresh,
            )
        })
        .transpose()
        .map_err(|err| quarantine_apply_cli_error("clear", err))?;
    let mut state = QuarantineState::load_for_operator(&data_dir)
        .map_err(anyhow::Error::from)
        .map_err(|err| quarantine_apply_cli_error("clear", err))?;

    // Health treats the structured checkpoint and both poison ledgers as one
    // quarantine set. Clear must use the same union so a prior best-effort
    // structured-save failure cannot leave an operator-invisible poison key.
    let mut available_keys = state
        .iter()
        .map(|(key, _)| (key.conversation_id, i64::from(key.schema_version)))
        .collect::<BTreeSet<_>>();
    available_keys.extend(
        crate::indexer::operator_quarantine_poison_records(&data_dir)
            .map_err(|err| quarantine_apply_cli_error("clear", err))?
            .into_keys(),
    );

    // Collect the keys that match the optional conversation-id substring
    // filter. With no filter, every entry is targeted.
    let matched = available_keys
        .into_iter()
        .filter(|(key, _)| filter.as_deref().is_none_or(|needle| key.contains(needle)))
        .collect::<Vec<_>>();

    let mut cleared = 0usize;
    if apply && !matched.is_empty() {
        let original_state = state.clone();
        for (conversation_id, schema_version) in &matched {
            if let Ok(schema_version) = u32::try_from(*schema_version) {
                state.clear(&QuarantineKey::new(conversation_id.clone(), schema_version));
            }
        }
        state
            .save(&data_dir)
            .map_err(anyhow::Error::from)
            .map_err(|err| quarantine_apply_cli_error("clear", err))?;

        let quarantine_keys = matched.iter().cloned().collect::<BTreeSet<_>>();
        if let Err(ledger_error) =
            crate::indexer::clear_operator_quarantine_poison_records(&data_dir, &quarantine_keys)
        {
            let error = match original_state.save(&data_dir) {
                Ok(()) => anyhow::anyhow!(
                    "clearing mirrored poison quarantine records failed; restored the structured checkpoint: {ledger_error:#}"
                ),
                Err(restore_error) => anyhow::anyhow!(
                    "clearing mirrored poison quarantine records failed: {ledger_error:#}; restoring the structured checkpoint also failed: {restore_error}"
                ),
            };
            return Err(quarantine_apply_cli_error("clear", error));
        }
        cleared = matched.len();
    }

    let matched_json: Vec<serde_json::Value> = matched
        .iter()
        .map(|(conversation_id, schema_version)| {
            serde_json::json!({
                "conversation_id": conversation_id,
                "schema_version": schema_version,
            })
        })
        .collect();

    let recommended_action = if matched.is_empty() {
        "No quarantine entries match. Nothing to clear.".to_string()
    } else if apply {
        format!(
            "Cleared {cleared} quarantine entr(ies). They will be re-attempted on the next index/watch pass."
        )
    } else {
        format!(
            "{} quarantine entr(ies) match. Re-run with --apply to clear them.",
            matched.len()
        )
    };

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(fmt) = structured_format {
        let payload = serde_json::json!({
            "schema_version": 1,
            "applied": apply,
            "dry_run": !apply,
            "data_dir": data_dir.display().to_string(),
            "filter": filter,
            "matched": matched.len(),
            "cleared": cleared,
            "persist_error": null,
            "entries": matched_json,
            "recommended_action": recommended_action,
        });
        return output_structured_value(payload, fmt);
    }

    println!("CASS Quarantine Clear");
    println!("=====================");
    println!();
    println!(
        "Mode: {}",
        if apply {
            "APPLY (mutating)"
        } else {
            "dry-run (inspect only)"
        }
    );
    if let Some(needle) = &filter {
        println!("Filter: conversation_id contains {needle:?}");
    }
    println!();
    if matched.is_empty() {
        println!("No matching quarantine entries.");
        return Ok(());
    }
    println!("Matching entries: {}", matched.len());
    for (conversation_id, schema_version) in matched.iter().take(20) {
        println!("  {conversation_id} (schema v{schema_version})");
    }
    if matched.len() > 20 {
        println!("  ... and {} more", matched.len() - 20);
    }
    println!();
    println!("{recommended_action}");
    Ok(())
}
