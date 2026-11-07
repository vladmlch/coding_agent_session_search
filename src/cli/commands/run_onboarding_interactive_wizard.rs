use crate::cli::args_sub::RobotFormat;
use crate::cli::commands::search::toon_encode_options_from_env::output_structured_value;
use crate::cli::error::{CliError, CliResult};
use crate::cli::paths::{default_data_dir, resolve_data_dir};
use crate::cli::state::state_meta_json::state_meta_json;
use crate::doctor::lexical_cleanup_disposition_label::{
    gather_onboarding_observation, print_onboarding_human,
};
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
use std::io::{self, IsTerminal};
use std::path::{Path, PathBuf};

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

/// `cass guide [INTENT]` — intent-to-command planner plus gated runner (beads
/// `…5u82n.1` and `…5u82n.17`). Dry-run is the default. Apply mode automatically
/// evaluates only allowlisted read-only proof adapters and requires an explicit
/// confirmation for each exact mutating step; workflow macro strings are never
/// interpreted by a shell.
///
/// Live mode derives the determinable preflight facts (db present, lexical index
/// present, search assets ready) from a single `state_meta_json` snapshot; facts
/// it cannot determine are left for the operator to confirm. `--fixture <file>`
/// supplies `{ "facts": {…}, "intent"?: "…" }` for deterministic scenarios.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_guide(
    intent_words: &[String],
    fixture: Option<&Path>,
    data_dir_override: &Option<PathBuf>,
    db_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
    apply: bool,
    confirmed_steps: &[usize],
    confirmed_facts: &[String],
    accepted_privacy_tier: Option<&str>,
    accepted_cost_risk: Option<&str>,
    stop_conditions_clear: bool,
) -> CliResult<()> {
    let mut intent = intent_words.join(" ");
    let data_dir = data_dir_override.clone().unwrap_or_else(default_data_dir);
    let db_path = db_override.unwrap_or_else(|| data_dir.join("agent_search.db"));

    // Fixture mode: facts (and optionally a default intent) from a checked-in file.
    let (mut facts, source_kind, fixture_id, fixture_context): (
        Option<serde_json::Value>,
        &str,
        String,
        Option<serde_json::Value>,
    ) = if let Some(path) = fixture {
        let raw = std::fs::read_to_string(path).map_err(|err| CliError {
                code: 10,
                kind: CliErrorKind::Config.kind_str(),
                message: format!("could not read guide fixture {}: {err}", path.display()),
                hint: Some(
                    "Pass --fixture <file> with a JSON object like {\"facts\": {\"db_present\": true}}."
                        .to_string(),
                ),
                retryable: false,
            })?;
        let parsed: serde_json::Value = serde_json::from_str(&raw).map_err(|err| CliError {
            code: 10,
            kind: CliErrorKind::Config.kind_str(),
            message: format!("invalid guide fixture {}: {err}", path.display()),
            hint: Some("The fixture must be a JSON object with a `facts` map.".to_string()),
            retryable: false,
        })?;
        if intent.trim().is_empty()
            && let Some(fixture_intent) = parsed.get("intent").and_then(serde_json::Value::as_str)
        {
            intent = fixture_intent.to_string();
        }
        let facts = parsed.get("facts").cloned();
        let id = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("fixture")
            .to_string();
        (facts, "fixture", id, Some(parsed))
    } else {
        // Live mode: derive the determinable facts from one state snapshot.
        let state = state_meta_json(&data_dir, &db_path, 300, true);
        let db_present = state
            .pointer("/database/exists")
            .and_then(serde_json::Value::as_bool);
        let index_present = state
            .pointer("/index/exists")
            .and_then(serde_json::Value::as_bool);
        let rebuild_active = state
            .pointer("/rebuild/active")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let search_assets_ready = match (index_present, db_present) {
            (Some(idx), Some(db)) => Some(idx && db && !rebuild_active),
            _ => None,
        };
        let facts = crate::guide_planner::preflight_facts(
            index_present,
            db_present,
            search_assets_ready,
            None,
            None,
        );
        (Some(facts), "live", "live".to_string(), None)
    };

    // Operator-context facts are explicit grants, never inferred. They are
    // overlaid before readiness classification and echoed in the transcript.
    if !confirmed_facts.is_empty() {
        let facts = facts.get_or_insert_with(|| serde_json::json!({}));
        let map = facts.as_object_mut().ok_or_else(|| CliError {
            code: 10,
            kind: CliErrorKind::Config.kind_str(),
            message: "guide facts must be a JSON object".to_string(),
            hint: Some(
                "Use a fixture shaped like {\"facts\": {\"db_present\": true}}.".to_string(),
            ),
            retryable: false,
        })?;
        for fact in confirmed_facts {
            map.insert(fact.clone(), serde_json::Value::Bool(true));
        }
    }

    let plan =
        crate::guide_planner::render_guide_plan(&intent, facts.as_ref(), source_kind, &fixture_id);
    let payload = crate::guide_runner::render_execution(
        plan,
        &crate::guide_runner::GuideRunRequest {
            apply,
            confirmed_steps,
            confirmed_facts,
            accepted_privacy_tier,
            accepted_cost_risk,
            stop_conditions_clear,
            source_kind,
            fixture_context: fixture_context.as_ref(),
            data_dir: &data_dir,
            db_path: Some(&db_path),
        },
    );

    if let Some(format) = output_format {
        return output_structured_value(payload, format);
    }

    print_guide_human(&payload);
    Ok(())
}

/// Human-readable guide summary (never a bare TUI). The richer interactive
/// shell is a later dependent surface; this stays terse and copy-safe.
pub(crate) fn print_guide_human(payload: &serde_json::Value) {
    use colored::Colorize;
    let intent = payload
        .pointer("/intent/raw")
        .and_then(serde_json::Value::as_str);
    match intent {
        None => {
            // Catalog mode.
            println!("{}", "CASS guide — known intents".bold());
            if let Some(intents) = payload
                .get("known_intents")
                .and_then(serde_json::Value::as_array)
            {
                for entry in intents {
                    let name = entry
                        .get("intent")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("");
                    let summary = entry
                        .get("summary")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("");
                    println!("  - {} — {summary}", name.cyan());
                }
            }
            println!();
            println!("  Plan one: cass guide <intent> --json");
        }
        Some(raw) => {
            let recognized = payload
                .pointer("/intent/recognized")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            if !recognized {
                println!("{} {}", "Unknown intent:".bold(), raw);
                println!("  Try one of the known intents:");
                if let Some(intents) = payload
                    .get("known_intents")
                    .and_then(serde_json::Value::as_array)
                {
                    for entry in intents {
                        if let Some(name) = entry.get("intent").and_then(serde_json::Value::as_str)
                        {
                            println!("    - {}", name.cyan());
                        }
                    }
                }
                println!("  Docs: cass robot-docs guide");
                return;
            }
            let title = payload
                .pointer("/plan/title")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let readiness = payload
                .get("readiness")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            println!("{} {}", "Guide:".bold(), title);
            println!("  Intent: {raw}  |  Readiness: {readiness}");
            if let Some(steps) = payload
                .pointer("/plan/steps")
                .and_then(serde_json::Value::as_array)
            {
                println!("  Steps:");
                for step in steps {
                    let order = step
                        .get("order")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0);
                    let intent = step
                        .get("intent")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("");
                    let gate = step
                        .get("proof_gate")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("");
                    println!("    {order}. {intent}  (proof: {gate})");
                }
            }
            if let Some(action) = payload
                .get("recommended_action")
                .and_then(serde_json::Value::as_str)
            {
                println!("  Next: {action}");
            }
            let execution_mode = payload
                .pointer("/execution/mode")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("dry-run");
            let execution_status = payload
                .pointer("/execution/overall_status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("dry-run");
            println!("  Mode: {execution_mode}  |  Execution: {execution_status}");
            println!("  (pass --json for the argv, proof-gate, and confirmation transcript)");
        }
    }
}
