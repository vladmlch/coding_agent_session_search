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

/// Human-readable onboarding summary (never a bare TUI).
pub(crate) fn print_onboarding_human(
    report: &crate::source_onboarding::OnboardingReport,
    observation: &crate::source_onboarding::OnboardingObservation,
) {
    use colored::Colorize;
    println!("{}", "CASS onboarding".bold());
    if report.providers.is_empty() {
        println!("  No agent session providers detected yet.");
    } else {
        let lines: Vec<String> = report
            .providers
            .iter()
            .map(|p| {
                format!(
                    "    - {} [{}] ~{} sessions",
                    p.name,
                    p.readiness.as_str(),
                    p.estimated_sessions
                )
            })
            .collect();
        println!("  Detected providers:");
        println!("{}", lines.join("\n"));
    }
    if !report.excluded_providers.is_empty() {
        println!(
            "  Excluded providers: {}",
            report.excluded_providers.join(", ")
        );
    }
    println!(
        "  Estimated sessions to index: {}",
        report.estimated_index_sessions
    );
    if observation.existing_indexed_db {
        println!(
            "  Already indexed conversations: {}",
            observation.indexed_conversation_count
        );
    }
    println!("  {}", report.semantic_note());
    if let Some(hint) = &report.remote_hint {
        println!("  {hint}");
    }
    println!();
    println!("  {} {}", "Recommended:".bold(), report.recommended_command);
    println!("  Undo: {}", report.rollback_note);
}

/// Run the TTY-only guided review shell over an already-gathered onboarding
/// report. The shell is deliberately advisory: choosing an item only explains
/// the observation or prints the next command. It never executes that command,
/// indexes data, installs a model, or changes source configuration.
pub(crate) fn run_onboarding_interactive_wizard(
    report: &crate::source_onboarding::OnboardingReport,
    observation: &crate::source_onboarding::OnboardingObservation,
) -> CliResult<()> {
    use colored::Colorize;
    use dialoguer::{Confirm, Select, theme::ColorfulTheme};

    const MENU_ITEMS: [&str; 4] = [
        "Show the safest next command",
        "Review providers and indexing scope",
        "Review privacy, search mode, and undo",
        "Exit without changes",
    ];

    println!();
    println!("{}", "Read-only guided review".bold().cyan());
    println!("  This wizard explains the plan. It will not run commands or change CASS state.");

    loop {
        let selection = Select::with_theme(&ColorfulTheme::default())
            .with_prompt("What would you like to review?")
            .items(MENU_ITEMS)
            .default(0)
            .interact_opt()
            .map_err(|err| CliError::unknown(format!("onboarding wizard input failed: {err}")))?;

        let Some(selection) = selection else {
            println!("  Input cancelled; no changes were made.");
            return Ok(());
        };

        match selection {
            0 => {
                println!();
                println!("{}", "Recommended next command".bold());
                println!("  {}", report.recommended_command.cyan());
                println!("  This wizard did not run that command.");
            }
            1 => {
                println!();
                println!("{}", "Provider and indexing scope".bold());
                if report.providers.is_empty() {
                    println!("  No provider roots were detected.");
                } else {
                    for provider in &report.providers {
                        println!(
                            "  - {}: {} (~{} sessions)",
                            provider.name,
                            provider.readiness.as_str(),
                            provider.estimated_sessions
                        );
                    }
                }
                println!(
                    "  Estimated new sessions in scope: {}",
                    report.estimated_index_sessions
                );
                println!(
                    "  Conversations already indexed: {}",
                    observation.indexed_conversation_count
                );
            }
            2 => {
                println!();
                println!("{}", "Privacy, search mode, and undo".bold());
                println!(
                    "  Provider discovery reads directory metadata; this review does not index session content."
                );
                println!("  {}", report.semantic_note());
                println!("  Undo: {}", report.rollback_note);
                if let Some(hint) = &report.remote_hint {
                    println!("  Remote sources: {hint}");
                }
            }
            _ => {
                println!("  No changes were made.");
                return Ok(());
            }
        }

        let review_another = Confirm::with_theme(&ColorfulTheme::default())
            .with_prompt("Review another section?")
            .default(false)
            .interact()
            .map_err(|err| CliError::unknown(format!("onboarding wizard input failed: {err}")))?;
        if !review_another {
            println!("  No changes were made.");
            return Ok(());
        }
    }
}

/// `cass onboarding` — read-only first-run onboarding/readiness surface. Gathers
/// a live observation, maps it through the pure `source_onboarding::recommend`
/// core, and emits the deterministic report (`--json` for agents, otherwise a
/// human summary). When both standard streams are terminals, the summary is
/// followed by a read-only guided review shell. Mutation-free (beads 5u82n.6
/// and 5u82n.16).
pub(crate) fn run_onboarding(
    data_dir_override: &Option<PathBuf>,
    db_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
) -> CliResult<()> {
    let data_dir = resolve_data_dir(data_dir_override, db_override.as_ref());
    let db_path = db_override.unwrap_or_else(|| data_dir.join("agent_search.db"));

    let observation = gather_onboarding_observation(&data_dir, &db_path);
    let report = crate::source_onboarding::recommend(&observation);

    if let Some(format) = output_format {
        let mut payload = serde_json::to_value(&report).unwrap_or(serde_json::Value::Null);
        if let serde_json::Value::Object(map) = &mut payload {
            map.insert(
                "data_dir".to_string(),
                serde_json::Value::String(data_dir.display().to_string()),
            );
            map.insert(
                "indexed_conversation_count".to_string(),
                serde_json::json!(observation.indexed_conversation_count),
            );
        }
        return output_structured_value(payload, format);
    }

    print_onboarding_human(&report, &observation);
    if io::stdin().is_terminal() && io::stdout().is_terminal() {
        run_onboarding_interactive_wizard(&report, &observation)?;
    }
    Ok(())
}
