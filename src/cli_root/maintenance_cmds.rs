#[allow(unused_imports)]
use crate::franken_sync::compat::{
    ConnectionExt, OpenFlags as FrankenOpenFlags, RowExt,
    open_with_flags as open_franken_with_flags,
};
#[allow(unused_imports)]
use crate::indexer::IndexOptions;
#[allow(unused_imports)]
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
#[allow(unused_imports)]
use crate::robot_budget_envelope::BudgetSnapshot;
#[allow(unused_imports)]
use crate::{cli_app::*, cli_root::*, lib_doctor::*, *};
#[allow(unused_imports)]
use anyhow::Result;
#[allow(unused_imports)]
use base64::prelude::*;
#[allow(unused_imports)]
use chrono::Utc;
#[allow(unused_imports)]
use clap::{Arg, ArgAction, Command, CommandFactory, Parser, Subcommand, ValueEnum, ValueHint};
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
#[allow(unused_imports)]
use std::fs::{File, OpenOptions};
#[allow(unused_imports)]
use std::io::{self, BufRead, BufReader, IsTerminal, Read, Seek, SeekFrom, Write};
#[allow(unused_imports)]
use std::path::{Path, PathBuf};
#[allow(unused_imports)]
use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
use std::time::{Duration, Instant};
#[allow(unused_imports)]
use tracing::{info, warn};
#[allow(unused_imports)]
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

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

/// Resolve the entire selection before opening the archive. Invalid input
/// must never turn a selective forget into a broader deletion or trigger a
/// storage migration. Resolve relative age exactly once for this invocation.
pub(crate) fn forget_filter<'a>(
    source_glob: Option<&'a str>,
    workspace_glob: Option<&'a str>,
    older_than: Option<&str>,
    now_ms: i64,
) -> CliResult<crate::storage::sqlite::ForgetConversationsFilter<'a>> {
    if source_glob.is_none() && workspace_glob.is_none() && older_than.is_none() {
        return Err(CliError::usage(
            "cass forget requires at least one selector",
            Some("Pass --source-glob, --workspace-glob, or --older-than. Supplied selectors are ANDed; preview with --json before adding --apply.".to_string()),
        ));
    }
    let source_glob = source_glob.map(str::trim);
    let workspace_glob = workspace_glob.map(str::trim);
    for (name, pattern) in [
        ("source-glob", source_glob),
        ("workspace-glob", workspace_glob),
    ] {
        if let Some(pattern) = pattern {
            if pattern.is_empty() {
                return Err(CliError::usage(
                    format!("forget --{name} must not be empty"),
                    None,
                ));
            }
            glob::Pattern::new(pattern).map_err(|error| {
                CliError::usage(
                    format!("invalid forget --{name} glob: {error}"),
                    Some("Quote the glob so your shell passes it to cass unchanged.".to_string()),
                )
            })?;
        }
    }
    let before_ms = older_than
        .map(|duration| {
            let age_ms = parse_duration_millis(duration)?;
            now_ms
                .checked_sub(age_ms)
                .ok_or_else(|| CliError::usage("forget --older-than is too large", None))
        })
        .transpose()?;
    Ok(crate::storage::sqlite::ForgetConversationsFilter {
        source_glob,
        workspace_glob,
        before_ms,
    })
}

#[cfg(test)]
mod forget_selector_tests {
    use super::*;

    #[test]
    fn forget_requires_an_explicit_selector_even_with_apply() {
        for args in [vec!["cass", "forget"], vec!["cass", "forget", "--apply"]] {
            assert!(Cli::try_parse_from(args).is_err());
        }
        assert!(forget_filter(None, None, None, 0).is_err());
    }

    #[test]
    fn forget_accepts_each_selector_and_intersections() {
        for selectors in [
            vec!["--source-glob", "**/rollout-*.jsonl"],
            vec!["--workspace-glob", "/work/scratch/**"],
            vec!["--older-than", "90d"],
            vec![
                "--source-glob",
                "**/rollout-*.jsonl",
                "--workspace-glob",
                "/work/scratch/**",
                "--older-than",
                "90d",
            ],
        ] {
            let mut args = vec!["cass", "forget"];
            args.extend(selectors);
            args.push("--json");
            let cli = Cli::try_parse_from(args).expect("valid forget selectors");
            assert!(matches!(
                cli.command,
                Some(Commands::Forget { apply: false, .. })
            ));
        }
        let filter = forget_filter(
            Some(" **/*.jsonl "),
            Some("/work/*"),
            Some("2d"),
            200_000_000,
        )
        .expect("valid selection");
        assert_eq!(filter.source_glob, Some("**/*.jsonl"));
        assert_eq!(filter.workspace_glob, Some("/work/*"));
        assert_eq!(filter.before_ms, Some(27_200_000));
    }

    #[test]
    fn forget_rejects_invalid_selectors_before_storage() {
        for pattern in ["", "   ", "["] {
            assert!(forget_filter(Some(pattern), None, None, 0).is_err());
            assert!(forget_filter(None, Some(pattern), None, 0).is_err());
        }
        for duration in [
            "",
            "yesterday",
            "-1d",
            "1.5d",
            "90fortnights",
            "9223372036854775807d",
        ] {
            assert!(forget_filter(None, None, Some(duration), 0).is_err());
        }
        assert!(forget_filter(None, None, Some("1s"), i64::MIN).is_err());
        assert_eq!(
            forget_filter(None, None, Some("0s"), 42)
                .expect("explicit zero age")
                .before_ms,
            Some(42)
        );
    }
}

#[derive(Default)]
pub(crate) struct ForgetCommandOptions {
    pub(crate) source_glob: Option<String>,
    pub(crate) workspace_glob: Option<String>,
    pub(crate) older_than: Option<String>,
    pub(crate) list: bool,
    pub(crate) reinclude: bool,
    pub(crate) source_path: Option<String>,
    pub(crate) source_id: Option<String>,
    pub(crate) agent: Option<String>,
    pub(crate) external_id: Option<String>,
    pub(crate) db: Option<PathBuf>,
    pub(crate) apply: bool,
}

impl ForgetCommandOptions {
    pub(crate) fn exclusion_selector(&self) -> crate::storage::sqlite::ForgetExclusionSelector<'_> {
        crate::storage::sqlite::ForgetExclusionSelector {
            source_glob: self.source_glob.as_deref(),
            source_path: self.source_path.as_deref(),
            source_id: self.source_id.as_deref(),
            agent_slug: self.agent.as_deref(),
            external_id: self.external_id.as_deref(),
        }
    }
}

/// Prune an explicitly selected subset of archived conversations. Preview
/// opens strictly read-only; apply holds maintenance ownership through both
/// canonical deletion and publication of the replacement search generation.
pub(crate) fn run_forget_command(
    options: ForgetCommandOptions,
    cli: &Cli,
    output_format: Option<RobotFormat>,
) -> CliResult<()> {
    use crate::storage::sqlite::FrankenStorage;

    let apply = options.apply;
    let older_than = &options.older_than;
    let exclusion_action = options.list || options.reinclude;
    let filter = if exclusion_action {
        if (options.list && (options.reinclude || apply))
            || options.workspace_glob.is_some()
            || older_than.is_some()
        {
            return Err(CliError::usage(
                "--list/--reinclude cannot use workspace or age selectors, and --list cannot apply changes",
                Some("Historical exclusions record source/session identity, not workspace or message age.".to_string()),
            ));
        }
        options
            .exclusion_selector()
            .validate(options.reinclude)
            .map_err(|error| CliError::usage(error.to_string(), None))?;
        None
    } else {
        if options.source_path.is_some()
            || options.source_id.is_some()
            || options.agent.is_some()
            || options.external_id.is_some()
        {
            return Err(CliError::usage(
                "exact exclusion identity selectors require --list or --reinclude",
                None,
            ));
        }
        Some(forget_filter(
            options.source_glob.as_deref(),
            options.workspace_glob.as_deref(),
            older_than.as_deref(),
            chrono::Utc::now().timestamp_millis(),
        )?)
    };
    let db_path = options
        .db
        .clone()
        .or_else(|| cli.db.clone())
        .unwrap_or_else(default_db_path);
    if !db_path.is_file() {
        return Err(CliError {
            code: 5,
            kind: "forget",
            message: format!("no canonical database at {}", db_path.display()),
            hint: Some("Run `cass index` first, or pass --db <path>.".to_string()),
            retryable: false,
        });
    }

    // The lexical index lives next to the canonical DB (the agent-purge path
    // resolves it the same way).
    let data_dir = db_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(default_data_dir);
    if apply && let Some(active_index) = active_index_run_details(&data_dir, &db_path) {
        return Err(CliError {
            code: 7,
            kind: "lock-busy",
            message: format!(
                "refusing to apply forget while an index run is active in {}",
                active_index.data_dir.display()
            ),
            hint: Some(
                "Wait for indexing/watch work to finish, then rerun `cass forget --apply`."
                    .to_string(),
            ),
            retryable: true,
        });
    }

    // An active-owner probe alone races a new index/daemon job. Acquire the
    // same data-directory and archive writer leases as indexing, and keep
    // them until all derived assets have stopped serving the deleted rows.
    let _mutation_guard = apply
        .then(|| {
            crate::indexer::acquire_search_maintenance_mutation_lock(
                &data_dir,
                &db_path,
                crate::search::asset_state::SearchMaintenanceJobKind::LexicalRefresh,
            )
            .map_err(|error| {
                let detail = format!("{error:#}");
                if error_chain_indicates_active_cass_index(&detail) {
                    return active_index_run_details(&data_dir, &db_path)
                        .map(|details| details.to_cli_error())
                        .unwrap_or_else(|| index_storage_contention_cli_error(&detail));
                }
                CliError {
                    code: 5,
                    kind: "forget",
                    message: format!("failed to acquire forget maintenance lock: {detail}"),
                    hint: Some(format!("Check permissions under {}.", data_dir.display())),
                    retryable: true,
                }
            })
        })
        .transpose()?;
    let storage = if apply {
        FrankenStorage::open(&db_path)
    } else {
        FrankenStorage::open_strict_readonly(&db_path)
    }
    .map_err(|e| CliError {
        code: 5,
        kind: "forget",
        message: format!("failed to open canonical database: {e}"),
        hint: None,
        retryable: false,
    })?;

    if exclusion_action {
        return run_forget_exclusion_action(storage, &options, &db_path, output_format);
    }

    let filter = filter.ok_or_else(|| CliError::usage("missing forget selectors", None))?;

    let report = storage
        .forget_conversations(&filter, !apply)
        .map_err(|e| CliError {
            code: 5,
            kind: "forget",
            message: format!("forget failed: {e}"),
            hint: Some(
                "Preview the same selectors with `cass forget ... --json` before adding --apply."
                    .to_string(),
            ),
            retryable: false,
        })?;

    // A previous attempt may have committed deletion before rebuilding failed.
    // Its durable marker must also trigger repair when this retry matches no
    // canonical rows. Preview must not turn into a repair operation.
    let rebuild_pending = apply
        && storage
            .forget_derived_rebuild_pending()
            .map_err(|error| CliError {
                code: 5,
                kind: "forget",
                message: format!("failed to read forget rebuild state: {error}"),
                hint: Some("Retry the same forget command with --apply.".to_string()),
                retryable: true,
            })?;

    // After an actual deletion every derived surface that stores message text
    // or counts must stop serving the forgotten conversations, exactly as on
    // the agent-purge path. The Quill documents carry stored content, so
    // until the lexical generation is rebuilt a plain search kept returning
    // the forgotten text (2l1b0.50). Failures are typed errors: a warning
    // with exit 0 would report success while the text stays searchable.
    let remaining_conversations = if apply && (report.conversations_deleted > 0 || rebuild_pending)
    {
        let derived_error = |kind: CliErrorKind, surface: &str, error: anyhow::Error| {
            CliError {
            code: 5,
            kind: kind.kind_str(),
            message: format!(
                "forgot {} conversation(s) but failed to rebuild {surface}: {error}",
                report.conversations_deleted
            ),
            hint: Some(
                "The canonical rows are already deleted; retry the same forget command with --apply to finish rebuilding derived search data, or run 'cass index --full'."
                    .to_string(),
            ),
            retryable: true,
        }
        };
        storage
            .rebuild_fts()
            .map_err(|e| derived_error(CliErrorKind::ArchiveFtsRebuild, "the FTS fallback", e))?;
        storage.rebuild_analytics().map_err(|e| {
            derived_error(
                CliErrorKind::ArchiveAnalyticsRebuild,
                "analytics rollups",
                e,
            )
        })?;
        storage
            .rebuild_daily_stats()
            .map_err(|e| derived_error(CliErrorKind::ArchiveDailyStatsRebuild, "daily stats", e))?;
        storage.rebuild_token_daily_stats().map_err(|e| {
            derived_error(
                CliErrorKind::ArchiveTokenDailyStatsRebuild,
                "token daily stats",
                e,
            )
        })?;
        Some(
            storage.total_conversation_count().map_err(|e| {
                derived_error(CliErrorKind::ArchiveCount, "the conversation count", e)
            })?,
        )
    } else {
        None
    };

    // WS-B.5 (z2uon): an applied forget deletes rows and rewrites FTS,
    // analytics and daily-stats tables; close through the checkpointing path
    // so the next opener does not replay all of that from the WAL. A dry run
    // wrote nothing and must not checkpoint anything.
    if apply {
        if let Err(err) =
            crate::indexer::close_storage_with_wal_checkpoint(storage, &db_path, "forget")
        {
            tracing::warn!(
                error = %format!("{err:#}"),
                db_path = %db_path.display(),
                "forget: final WAL checkpoint did not complete"
            );
            eprintln!("Warning: final WAL checkpoint after forget did not complete: {err:#}");
        }
    } else {
        storage
            .close_without_checkpoint()
            .map_err(|error| CliError {
                code: 5,
                kind: "forget",
                message: format!("failed to close forget preview: {error}"),
                hint: None,
                retryable: false,
            })?;
    }

    if let Some(remaining_conversations) = remaining_conversations {
        // The marker stays set across this failure, including a retry whose
        // selector no longer matches any canonical rows.
        let rebuilt = if dotenvy::var("CASS_TEST_FORGET_LEXICAL_REBUILD_FAILURE")
            .is_ok_and(|value| value == "1")
        {
            Err(anyhow::anyhow!(
                "injected by CASS_TEST_FORGET_LEXICAL_REBUILD_FAILURE"
            ))
        } else {
            crate::indexer::rebuild_tantivy_from_db(
                &db_path,
                &data_dir,
                remaining_conversations,
                None,
            )
        };
        rebuilt.map_err(|e| CliError {
            code: 5,
            kind: CliErrorKind::LexicalRebuild.kind_str(),
            message: format!(
                "forgot {} conversation(s) but failed to rebuild the lexical search index, \
                 which may still return their text: {e}",
                report.conversations_deleted
            ),
            hint: Some(
                "Retry the same forget command with --apply, or run 'cass index --full', \
                 to rebuild the lexical index from the canonical database."
                    .to_string(),
            ),
            retryable: true,
        })?;

        // Only a successfully published lexical generation completes the
        // operation. Keep maintenance ownership while clearing the marker.
        let storage = FrankenStorage::open(&db_path).map_err(|error| CliError {
            code: 5,
            kind: "forget",
            message: format!("rebuilt derived assets but failed to reopen forget state: {error}"),
            hint: Some("Retry the same forget command with --apply.".to_string()),
            retryable: true,
        })?;
        storage
            .clear_forget_derived_rebuild_pending()
            .map_err(|error| CliError {
                code: 5,
                kind: "forget",
                message: format!(
                    "rebuilt derived assets but failed to finish forget state: {error}"
                ),
                hint: Some("Retry the same forget command with --apply.".to_string()),
                retryable: true,
            })?;
        if let Err(error) =
            crate::indexer::close_storage_with_wal_checkpoint(storage, &db_path, "forget-complete")
        {
            tracing::warn!(error = %format!("{error:#}"), "forget completion checkpoint did not finish");
        }
    }

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
            obj.insert("applied".to_string(), serde_json::json!(apply));
            obj.insert("older_than".to_string(), serde_json::json!(older_than));
            obj.insert(
                "db_path".to_string(),
                serde_json::json!(db_path.display().to_string()),
            );
        }
        return output_structured_value(payload, fmt);
    }

    println!("CASS Forget (prune selected conversations)");
    println!("================================================");
    println!();
    println!(
        "Mode: {}",
        if apply {
            "APPLY (mutating)"
        } else {
            "dry-run (inspect only)"
        }
    );
    if let Some(pattern) = &report.source_glob {
        println!("Source glob: {pattern}");
    }
    if let Some(pattern) = &report.workspace_glob {
        println!("Workspace glob: {pattern}");
    }
    if let Some(cutoff) = report.before_ms {
        let cutoff = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(cutoff)
            .map(|time| time.to_rfc3339())
            .unwrap_or_else(|| format!("{cutoff} ms since the Unix epoch"));
        println!("Last message before: {cutoff} (unknown message times are kept)");
    }
    println!("All supplied selectors must match.");
    println!();
    println!(
        "Matched conversations: {} ({} messages)",
        report.conversations_matched, report.messages_matched
    );
    if !report.sample_sessions.is_empty() {
        println!("Sample matching sessions:");
        for session in &report.sample_sessions {
            let last_message_at = session
                .last_message_at
                .map(|timestamp| {
                    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(timestamp)
                        .map(|time| time.to_rfc3339())
                        .unwrap_or_else(|| format!("{timestamp} ms since the Unix epoch"))
                })
                .unwrap_or_else(|| "<unknown>".to_string());
            println!(
                "  Source: {}",
                session.source_path.as_deref().unwrap_or("<unknown>")
            );
            println!(
                "    Session: {}",
                session.external_id.as_deref().unwrap_or("<unknown>")
            );
            println!(
                "    Workspace: {}",
                session.workspace.as_deref().unwrap_or("<unknown>")
            );
            println!("    Last known message: {last_message_at}");
        }
        if report.conversations_matched > report.sample_sessions.len() {
            println!(
                "  ... and {} more",
                report.conversations_matched - report.sample_sessions.len()
            );
        }
    }
    println!();
    if apply {
        println!(
            "Deleted {} conversation(s) from the canonical DB and rebuilt derived assets.",
            report.conversations_deleted
        );
    } else if report.conversations_matched > 0 {
        println!("Re-run with --apply to delete these conversations.");
    } else {
        println!("No conversations match. Nothing to forget.");
    }
    Ok(())
}

pub(crate) fn run_forget_exclusion_action(
    storage: crate::storage::sqlite::FrankenStorage,
    options: &ForgetCommandOptions,
    db_path: &Path,
    output_format: Option<RobotFormat>,
) -> CliResult<()> {
    let failure = |error: anyhow::Error| {
        CliError {
        code: 5,
        kind: "forget-exclusions",
        message: format!("exclusion operation failed: {error:#}"),
        hint: Some(
            "Inspect matching exclusions with --list. If a session overlaps a legacy source exclusion, preview a source-only --reinclude before applying."
                .to_string(),
        ),
        retryable: false,
    }
    };
    let selector = options.exclusion_selector();
    let (mut payload, mut lines) = if options.list {
        let report = storage.list_forget_exclusions(&selector).map_err(failure)?;
        let mut lines = vec![
            "CASS Exclusions (read-only)".to_string(),
            format!("Session exclusions: {}", report.exclusions.len()),
            format!("Legacy source exclusions: {}", report.legacy_sources.len()),
        ];
        for exclusion in &report.exclusions {
            lines.push(format!(
                "  source={} agent={} path={} session={} forgotten_at_ms={}{}",
                exclusion.key.source_id,
                exclusion.key.agent_slug,
                exclusion.key.source_path,
                exclusion.key.external_id,
                exclusion.forgotten_at_ms,
                if exclusion.blocked_by_source_stamp {
                    " (also covered by a source-wide exclusion)"
                } else {
                    ""
                }
            ));
        }
        for exclusion in &report.legacy_sources {
            lines.push(format!(
                "  legacy source={} size_bytes={:?} mtime_ms={:?} forgotten_at_ms={}",
                exclusion.source_path,
                exclusion.size_bytes,
                exclusion.mtime_ms,
                exclusion.forgotten_at_ms
            ));
        }
        let mut payload = serde_json::to_value(&report).map_err(|error| failure(error.into()))?;
        if let Some(object) = payload.as_object_mut() {
            object.insert(
                "exclusions_count".to_string(),
                report.exclusions.len().into(),
            );
            object.insert(
                "legacy_sources_count".to_string(),
                report.legacy_sources.len().into(),
            );
        }
        (payload, lines)
    } else {
        let report = storage
            .reinclude_forget_exclusions(&selector, !options.apply)
            .map_err(failure)?;
        let reindex_required =
            options.apply && (report.exclusions_removed > 0 || report.legacy_sources_removed > 0);
        let mut lines = vec![
            format!(
                "CASS Re-inclusion ({})",
                if options.apply { "applied" } else { "dry-run" }
            ),
            format!("Matched session exclusions: {}", report.exclusions_matched),
            format!(
                "Matched legacy source exclusions: {}",
                report.legacy_sources_matched
            ),
            format!(
                "Source observations to invalidate: {}",
                report.source_observations_matched
            ),
        ];
        for exclusion in &report.sample_sessions {
            lines.push(format!(
                "  source={} agent={} path={} session={}",
                exclusion.key.source_id,
                exclusion.key.agent_slug,
                exclusion.key.source_path,
                exclusion.key.external_id,
            ));
        }
        for path in &report.sample_source_paths {
            lines.push(format!("  legacy source={path}"));
        }
        for path in &report.blocked_by_source_stamps {
            lines.push(format!(
                "  BLOCKED by a source-wide exclusion: {path}; preview a source-only release."
            ));
        }
        if options.apply {
            lines.push(format!(
                "Released {} session and {} legacy source exclusions; invalidated {} source observations.",
                report.exclusions_removed,
                report.legacy_sources_removed,
                report.source_observations_removed,
            ));
            if reindex_required {
                lines.push(
                    "Run `cass index` to restore eligible sessions. Current workspace/path exclusions still apply."
                        .to_string(),
                );
            }
        } else {
            lines.push("Preview only. Add --apply to release the reviewed exclusions.".to_string());
        }
        let mut payload = serde_json::to_value(&report).map_err(|error| failure(error.into()))?;
        if let Some(object) = payload.as_object_mut() {
            object.insert("recommended_next_command".to_string(), "cass index".into());
            object.insert("reindex_required".to_string(), reindex_required.into());
            object.insert("conversations_restored".to_string(), 0.into());
            let data_dir = db_path
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(default_data_dir);
            object.insert(
                "next_command_argv".to_string(),
                if reindex_required {
                    serde_json::json!([
                        "cass",
                        "index",
                        "--db",
                        db_path,
                        "--data-dir",
                        data_dir,
                        "--json"
                    ])
                } else {
                    serde_json::Value::Null
                },
            );
        }
        (payload, lines)
    };

    // Re-inclusion changes only exclusions and ingest replay state. It must
    // leave outstanding forget repair markers and derived assets alone.
    if options.apply {
        if let Err(error) =
            crate::indexer::close_storage_with_wal_checkpoint(storage, db_path, "forget-reinclude")
        {
            tracing::warn!(error = %format!("{error:#}"), "re-inclusion checkpoint did not finish");
        }
    } else {
        storage.close_without_checkpoint().map_err(failure)?;
    }

    if let Some(object) = payload.as_object_mut() {
        object.insert("schema_version".to_string(), 1.into());
        object.insert(
            "mode".to_string(),
            if options.list { "list" } else { "reinclude" }.into(),
        );
        object.insert("applied".to_string(), options.apply.into());
        object.insert("db_path".to_string(), db_path.display().to_string().into());
        object.insert(
            "selector".to_string(),
            serde_json::json!({
                "source_glob": options.source_glob,
                "source_path": options.source_path,
                "source_id": options.source_id,
                "agent_slug": options.agent,
                "external_id": options.external_id,
            }),
        );
    }
    if let Some(format) = output_format.or_else(robot_format_from_env) {
        return output_structured_value(
            payload,
            if matches!(format, RobotFormat::Sessions) {
                RobotFormat::Compact
            } else {
                format
            },
        );
    }
    lines
        .push("Historical exclusions do not record workspace or message-age metadata.".to_string());
    for line in lines {
        println!("{line}");
    }
    Ok(())
}

pub(crate) fn run_mirror_prune(
    command: MirrorCommand,
    output_format: Option<RobotFormat>,
) -> CliResult<()> {
    let MirrorCommand::Prune {
        data_dir: data_dir_override,
        older_than,
        max_size,
        providers,
        source_path,
        keep_tag: keep_tags,
        keep_latest,
        safety_hold_down,
        apply,
        ..
    } = command;
    if older_than.is_none() && max_size.is_none() && keep_latest.is_none() {
        return Err(CliError::usage(
            "cass mirror prune needs at least one retention predicate",
            Some("Pass --older-than 90d, --max-size 100GB and/or --keep-latest 1. Dry-run is the default; add --apply to prune.".to_string()),
        ));
    }
    if providers.iter().any(|provider| provider.trim().is_empty()) {
        return Err(CliError::usage(
            "mirror prune --provider must not be empty",
            Some("Pass an agent slug such as opencode or codex.".to_string()),
        ));
    }
    if let Some(pattern) = &source_path {
        if pattern.trim().is_empty() {
            return Err(CliError::usage(
                "mirror prune --source-path must not be empty",
                None,
            ));
        }
        glob::Pattern::new(pattern).map_err(|error| {
            CliError::usage(
                format!("invalid mirror prune --source-path glob: {error}"),
                Some("Quote the glob so your shell passes it to cass unchanged.".to_string()),
            )
        })?;
    }

    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    let older_than_ms = older_than
        .as_deref()
        .map(parse_duration_millis)
        .transpose()?;
    let max_size_bytes = max_size.as_deref().map(parse_size_bytes).transpose()?;
    let safety_hold_down_ms = parse_duration_millis(&safety_hold_down)?;

    if apply
        && let Some(active_index) =
            active_index_run_details(&data_dir, &data_dir.join("agent_search.db"))
    {
        return Err(CliError {
            code: 7,
            kind: "lock-busy",
            message: format!(
                "refusing to apply raw-mirror prune while an index run is active in {}",
                active_index.data_dir.display()
            ),
            hint: Some(
                "Wait for indexing/watch work to finish, then rerun `cass mirror prune --apply`."
                    .to_string(),
            ),
            retryable: true,
        });
    }

    let report = crate::raw_mirror::prune(
        &data_dir,
        crate::raw_mirror::RawMirrorPruneOptions {
            older_than_ms,
            max_size_bytes,
            providers,
            source_path,
            keep_tags,
            keep_latest: keep_latest.map(|count| usize::try_from(count).unwrap_or(usize::MAX)),
            safety_hold_down_ms,
            apply,
        },
    )
    .map_err(|err| {
        let detail = err.to_string();
        let index_lock_busy = detail.contains("because an index run acquired");
        CliError {
            code: if index_lock_busy { 7 } else { 9 },
            kind: if index_lock_busy {
                "lock-busy"
            } else {
                "raw-mirror"
            },
            message: format!("raw-mirror prune failed: {detail}"),
            hint: Some(if index_lock_busy {
                "Wait for indexing/watch work to finish, then rerun `cass mirror prune --apply`."
                    .to_string()
            } else {
                "Run `cass mirror prune --dry-run --json` first and inspect the plan before applying."
                    .to_string()
            }),
            retryable: index_lock_busy,
        }
    })?;

    if let Some(fmt) = output_format {
        return output_structured_value(
            serde_json::json!({
                "success": true,
                "data_dir": data_dir.display().to_string(),
                "prune": report,
            }),
            fmt,
        );
    }

    println!("Raw-mirror prune ({})", report.mode);
    println!("  Root: {}", report.root_path);
    println!("  Manifests: {}", report.manifest_count);
    println!("  Unique blobs: {}", report.unique_blob_count);
    println!("  Current blob bytes: {}", report.current_blob_bytes);
    if let Some(bytes) = report.scope_blob_bytes {
        println!("  Selected scope blob bytes: {bytes}");
        println!("  Providers: {:?}", report.providers);
        if let Some(pattern) = &report.source_path {
            println!("  Source path glob: {pattern}");
        }
    }
    println!("  Orphan blobs: {}", report.orphan_blob_count);
    println!("  Orphan blob bytes: {}", report.orphan_blob_bytes);
    if let Some(count) = report.keep_latest {
        println!("  Kept per source file: newest {count}");
    }
    println!("  Pinned manifests: {}", report.pinned_manifest_count);
    println!("  Pinned blobs: {}", report.pinned_blob_count);
    println!("  Planned manifests: {}", report.planned_manifest_count);
    println!("  Planned blobs: {}", report.planned_blob_count);
    println!(
        "  Planned abandoned capture staging dirs: {}",
        report.planned_staging_count
    );
    println!("  Planned reclaim bytes: {}", report.planned_reclaim_bytes);
    if apply {
        println!("  Applied manifests: {}", report.applied_manifest_count);
        println!("  Applied blobs: {}", report.applied_blob_count);
        println!(
            "  Applied abandoned capture staging dirs: {}",
            report.applied_staging_count
        );
        println!("  Applied reclaim bytes: {}", report.applied_reclaim_bytes);
    }
    if let Some(path) = report.audit_log_path {
        println!("  Audit log: {path}");
    }
    Ok(())
}

pub(crate) fn parse_duration_millis(raw: &str) -> std::result::Result<i64, CliError> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(CliError::usage(
            "empty duration",
            Some("Use a duration like 90d, 24h, 30m, or 3600s.".to_string()),
        ));
    }
    let split_at = value
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(value.len());
    let (digits, suffix) = value.split_at(split_at);
    let amount = digits.parse::<i64>().map_err(|_| {
        CliError::usage(
            format!("invalid duration `{value}`"),
            Some("Use an integer duration like 90d, 24h, 30m, or 3600s.".to_string()),
        )
    })?;
    let unit_ms = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "s" | "sec" | "secs" | "second" | "seconds" => 1_000,
        "m" | "min" | "mins" | "minute" | "minutes" => 60_000,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3_600_000,
        "d" | "day" | "days" => 86_400_000,
        other => {
            return Err(CliError::usage(
                format!("unknown duration unit `{other}` in `{value}`"),
                Some("Supported units: s, m, h, d.".to_string()),
            ));
        }
    };
    amount.checked_mul(unit_ms).ok_or_else(|| {
        CliError::usage(
            format!("duration `{value}` is too large"),
            Some("Use a smaller retention duration.".to_string()),
        )
    })
}

pub(crate) fn parse_size_bytes(raw: &str) -> std::result::Result<u64, CliError> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(CliError::usage(
            "empty size",
            Some("Use a size like 100GB, 512MiB, or 1048576.".to_string()),
        ));
    }
    let split_at = value
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(value.len());
    let (digits, suffix) = value.split_at(split_at);
    let amount = digits.parse::<u64>().map_err(|_| {
        CliError::usage(
            format!("invalid size `{value}`"),
            Some("Use an integer size like 100GB, 512MiB, or 1048576.".to_string()),
        )
    })?;
    let multiplier = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "b" | "byte" | "bytes" => 1,
        "k" | "kb" => 1_000,
        "m" | "mb" => 1_000_000,
        "g" | "gb" => 1_000_000_000,
        "t" | "tb" => 1_000_000_000_000,
        "kib" => 1024,
        "mib" => 1024 * 1024,
        "gib" => 1024 * 1024 * 1024,
        "tib" => 1024_u64.pow(4),
        other => {
            return Err(CliError::usage(
                format!("unknown size unit `{other}` in `{value}`"),
                Some("Supported units: B, KB, MB, GB, TB, KiB, MiB, GiB, TiB.".to_string()),
            ));
        }
    };
    amount.checked_mul(multiplier).ok_or_else(|| {
        CliError::usage(
            format!("size `{value}` is too large"),
            Some("Use a smaller size limit.".to_string()),
        )
    })
}
