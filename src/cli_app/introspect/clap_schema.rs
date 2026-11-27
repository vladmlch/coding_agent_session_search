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

pub(crate) fn build_introspect_subsystem_coverage() -> crate::subsystem_coverage_matrix::MatrixReport
{
    crate::subsystem_coverage_matrix::matrix_report()
}

pub(crate) fn build_introspect_response() -> IntrospectResponse {
    IntrospectResponse {
        api_version: 1,
        contract_version: CONTRACT_VERSION.to_string(),
        subsystem_coverage: build_introspect_subsystem_coverage(),
        global_flags: build_global_flag_schemas(),
        commands: build_command_schemas(),
        response_schemas: build_response_schemas(),
    }
}

/// Full API schema introspection - commands, arguments, and response schemas.
pub(crate) fn run_introspect(output_format: Option<RobotFormat>) -> CliResult<()> {
    let response = build_introspect_response();

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(fmt) = structured_format {
        let output_error = |action: &str, error: &dyn std::fmt::Display| CliError {
            code: 9,
            kind: CliErrorKind::EncodeJson.kind_str(),
            message: format!("failed to {action} introspection response: {error}"),
            hint: None,
            retryable: false,
        };

        if matches!(fmt, RobotFormat::Toon) {
            let payload =
                serde_json::to_value(&response).map_err(|error| output_error("encode", &error))?;
            return output_structured_value(payload, fmt);
        }

        // Serialize the response struct directly so the frozen top-level field
        // order remains unchanged; converting through serde_json::Value would
        // reorder object keys when preserve_order is disabled.
        let stdout = std::io::stdout();
        let mut out = std::io::BufWriter::new(stdout.lock());
        if matches!(fmt, RobotFormat::Json) {
            serde_json::to_writer_pretty(&mut out, &response)
                .map_err(|error| output_error("encode JSON", &error))?;
        } else {
            serde_json::to_writer(&mut out, &response)
                .map_err(|error| output_error("encode compact JSON", &error))?;
        }
        writeln!(&mut out).map_err(|error| output_error("write newline for", &error))?;
        out.flush().map_err(|error| output_error("flush", &error))?;
        return Ok(());
    }

    // Human-readable output
    println!("CASS API Introspection");
    println!("======================");
    println!();
    println!("API Version: {}", response.api_version);
    println!("Contract Version: {}", response.contract_version);
    println!(
        "Subsystem Coverage: {} subsystems (complete: {})",
        response.subsystem_coverage.subsystem_count, response.subsystem_coverage.complete
    );
    println!();
    println!("Global Flags:");
    println!("-------------");
    for flag in &response.global_flags {
        let required = if flag.required { " (required)" } else { "" };
        let default = flag
            .default
            .as_ref()
            .map(|d| format!(" [default: {d}]"))
            .unwrap_or_default();
        let enum_values = flag
            .enum_values
            .as_ref()
            .map(|vals| format!(" [values: {}]", vals.join(",")))
            .unwrap_or_default();
        let short = flag.short.map(|s| format!("-{s}, ")).unwrap_or_default();
        let prefix = if flag.arg_type == "positional" {
            String::new()
        } else {
            format!("{short}--")
        };
        println!(
            "  {}{}: {}{}{}{}",
            prefix, flag.name, flag.description, required, default, enum_values
        );
    }
    println!();
    println!("Commands:");
    println!("---------");
    for cmd in &response.commands {
        println!();
        println!("  {} - {}", cmd.name, cmd.description);
        if cmd.has_json_output {
            println!("    [supports --json output]");
        }
        if !cmd.arguments.is_empty() {
            println!("    Arguments:");
            for arg in &cmd.arguments {
                let required = if arg.required { " (required)" } else { "" };
                let default = arg
                    .default
                    .as_ref()
                    .map(|d| format!(" [default: {d}]"))
                    .unwrap_or_default();
                let short = arg.short.map(|s| format!("-{s}, ")).unwrap_or_default();
                let prefix = if arg.arg_type == "positional" {
                    String::new()
                } else {
                    format!("{short}--")
                };
                println!(
                    "      {}{}: {}{}{}",
                    prefix, arg.name, arg.description, required, default
                );
            }
        }
    }
    println!();
    println!(
        "Response Schemas: {} defined",
        response.response_schemas.len()
    );
    for name in response.response_schemas.keys() {
        println!("  - {name}");
    }

    Ok(())
}

pub(crate) fn staged_secret_scan_json(
    scan: &crate::pages::secret_scan::StagedSecretScan,
    approval_state: &str,
) -> serde_json::Value {
    serde_json::json!({
        "performed": true,
        "complete": true,
        "findings": scan.report.summary.total,
        "has_critical": scan.report.summary.has_critical,
        "approval_state": approval_state,
        "artifact_sha256": scan.artifact_sha256.as_str(),
    })
}

/// Run export based on JSON config file.
pub(crate) struct ConfigExportSecretInputs<'a> {
    pub(crate) allow: &'a [String],
    pub(crate) deny: &'a [String],
}

pub(crate) fn run_config_based_export(
    config: &crate::pages::config_input::PagesConfig,
    wizard_state: &crate::pages::wizard::WizardState,
    db_path: &std::path::Path,
    dry_run: bool,
    output_format: Option<RobotFormat>,
    verbose: bool,
    secret_inputs: ConfigExportSecretInputs<'_>,
) -> anyhow::Result<()> {
    use chrono::DateTime;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    if dry_run {
        if let Some(fmt) = output_format {
            let result = serde_json::json!({
                "status": "dry_run",
                "output_dir": wizard_state.output_dir,
                "config_valid": true,
                "secret_scan": {
                    "performed": false,
                    "complete": false,
                    "approval_state": "not_performed_dry_run",
                    "artifact_sha256": null,
                },
            });
            output_structured_value(result, fmt)?;
        } else {
            println!("Dry run: would export to {:?}", wizard_state.output_dir);
            println!("Secret scan: not performed during dry run");
        }
        return Ok(());
    }

    // Output directory is the bundle root (contains site/ and private/)
    let output_dir = &wizard_state.output_dir;
    std::fs::create_dir_all(output_dir)?;

    // Create temp directory for intermediate export and encryption output
    let temp_dir = tempfile::tempdir()?;
    let export_db_path = temp_dir.path().join("export.db");
    let encrypted_dir = temp_dir.path().join("encrypted");
    std::fs::create_dir_all(&encrypted_dir)?;

    // Parse time filters to DateTime<Utc>
    let since_dt = config.since_ts().and_then(DateTime::from_timestamp_millis);
    let until_dt = config.until_ts().and_then(DateTime::from_timestamp_millis);

    // Build export filter
    let filter = crate::pages::export::ExportFilter {
        agents: if wizard_state.agents.is_empty() {
            None
        } else {
            Some(wizard_state.agents.clone())
        },
        workspaces: wizard_state.workspaces.clone(),
        since: since_dt,
        until: until_dt,
        path_mode: config.path_mode(),
    };

    // Run export and scan the exact committed staging database before it can
    // be encrypted, bundled, or deployed. Non-interactive config exports are
    // fail-closed: callers may suppress reviewed false positives with the
    // existing allowlist inputs, but cannot silently approve live findings.
    let export_engine = crate::pages::export::ExportEngine::new(db_path, &export_db_path, filter)
        .with_share_profile(wizard_state.effective_share_profile());

    let running = Arc::new(AtomicBool::new(true));
    let secret_scan_config = crate::pages::secret_scan::SecretScanConfig::from_inputs(
        secret_inputs.allow,
        secret_inputs.deny,
    )?;
    let (stats, staged_secret_scan) = export_engine.execute_verified(
        |_current, _total| {},
        Some(running),
        |staged_db_path| {
            let scan = crate::pages::secret_scan::scan_staged_export_database(
                staged_db_path,
                &secret_scan_config,
            )?;
            if scan.report.summary.total > 0 {
                anyhow::bail!(
                    "Staged export contains {} potential secret(s); config-driven exports fail closed. Review with `cass pages --scan-secrets` and use --secrets-allow only for confirmed false positives.",
                    scan.report.summary.total
                );
            }
            Ok(scan)
        },
    )?;

    let payload_dir = encrypted_dir.join("payload");
    std::fs::create_dir_all(&payload_dir)?;
    let dest_db = payload_dir.join("data.db");
    std::fs::copy(&export_db_path, &dest_db)?;

    let db_size = std::fs::metadata(&dest_db).map(|m| m.len()).unwrap_or(0);

    let unencrypted_config = crate::pages::bundle::UnencryptedConfig {
        encrypted: false,
        version: "1.0.0".to_string(),
        payload: crate::pages::bundle::UnencryptedPayload {
            path: "payload/data.db".to_string(),
            format: "sqlite".to_string(),
            size_bytes: Some(db_size),
        },
        warning: Some("UNENCRYPTED - All content is publicly readable".to_string()),
    };

    let config_path = encrypted_dir.join("config.json");
    std::fs::write(
        &config_path,
        serde_json::to_string_pretty(&unencrypted_config)?,
    )?;

    // Build bundle
    let bundle_config = crate::pages::bundle::BundleConfig {
        title: wizard_state.title.clone(),
        description: wizard_state.description.clone(),
        hide_metadata: wizard_state.hide_metadata,
        generated_docs: vec![],
        analytics_status: None,
    };

    let bundle_builder = crate::pages::bundle::BundleBuilder::with_config(bundle_config);
    let bundle_result = bundle_builder.build(&encrypted_dir, output_dir, |_phase, _msg| {})?;
    crate::pages::verify::ensure_valid_bundle(&bundle_result.site_dir, false).map_err(|error| {
        error.context("Completed config-driven Pages bundle failed full verification")
    })?;

    // No deployment: exports stay local archives. Hosted publishing was
    // removed; the field stays in the robot payload as `null` so consumers
    // see an explicit "nothing was published" rather than a missing key.
    let deploy_result = serde_json::Value::Null;

    // Output results
    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(fmt) = structured_format {
        let result = serde_json::json!({
            "status": "success",
            "output_dir": output_dir,
            "bundle_dir": output_dir,
            "site_dir": bundle_result.site_dir,
            "private_dir": bundle_result.private_dir,
            "stats": {
                "conversations": stats.conversations_processed,
                "messages": stats.messages_processed,
            },
            "secret_scan": staged_secret_scan_json(&staged_secret_scan, "clean"),
            "bundle": {
                "total_files": bundle_result.total_files,
                "fingerprint": bundle_result.fingerprint,
            },
            "deployment": deploy_result,
        });
        output_structured_value(result, fmt)?;
    } else {
        println!("Export complete:");
        println!("  Output: {}", output_dir.display());
        println!("  Site: {}", bundle_result.site_dir.display());
        println!("  Private: {}", bundle_result.private_dir.display());
        println!("  Conversations: {}", stats.conversations_processed);
        println!("  Messages: {}", stats.messages_processed);
        println!("  Secret scan: complete (clean)");
        if verbose {
            println!(
                "  Scanned artifact SHA-256: {}",
                staged_secret_scan.artifact_sha256
            );
        }
        println!("  Encryption: DISABLED (local archive)");
        println!(
            "  Fingerprint: {}",
            bundle_result
                .fingerprint
                .get(..8)
                .unwrap_or(&bundle_result.fingerprint)
        );
        println!(
            "  Preview: cass pages --preview {}",
            bundle_result.site_dir.display()
        );
    }

    Ok(())
}

/// Show API and contract versions (robot-friendly)
pub(crate) fn run_api_version(output_format: Option<RobotFormat>) -> CliResult<()> {
    let meta = crate::dependency_pin_correlation::build_metadata::BuildMetadata::current();
    let payload = serde_json::json!({
        "crate_version": env!("CARGO_PKG_VERSION"),
        "build_commit": meta.build_commit.unwrap_or("unknown"),
        "api_version": 1,
        "contract_version": CONTRACT_VERSION,
    });

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(fmt) = structured_format {
        return output_structured_value(payload, fmt);
    }

    println!("CASS API Version");
    println!("================");
    println!("crate: {}", env!("CARGO_PKG_VERSION"));
    println!("api:   v{}", 1);
    println!("contract: v{CONTRACT_VERSION}");

    Ok(())
}

/// Builds the clap trees of the subcommands the `cass` binary dispatches
/// before [`parse_cli`] (`serve`, `archive`; see src/main.rs).
pub(crate) static EXTERNAL_SUBCOMMANDS: std::sync::OnceLock<fn() -> Vec<Command>> =
    std::sync::OnceLock::new();

/// Called by the `cass` binary so help, completions, the man page,
/// `capabilities` and `introspect` describe the subcommands it parses itself.
/// Parsing is unchanged: the binary still dispatches them first.
pub fn register_external_subcommands(build: fn() -> Vec<Command>) {
    let _ = EXTERNAL_SUBCOMMANDS.set(build);
}

pub(crate) fn external_subcommands() -> Vec<Command> {
    EXTERNAL_SUBCOMMANDS
        .get()
        .map(|build| build())
        .unwrap_or_default()
}

/// The whole `cass` command tree: this crate's CLI plus the registered
/// binary-dispatched subcommands.
pub(crate) fn cli_command() -> Command {
    Cli::command().subcommands(external_subcommands())
}

/// Build command schemas for all CLI commands
pub(crate) fn build_command_schemas() -> Vec<CommandSchema> {
    let root = Cli::command();
    let global_robot_format = root
        .get_arguments()
        .find(|arg| arg.get_id().as_str() == "robot_format");
    // The binary parses its registered subcommands with their own parsers,
    // which do not accept this CLI's global --robot-format: list only their
    // own arguments.
    let external = external_subcommands();
    root.get_subcommands()
        .map(|cmd| command_schema_from_clap(cmd, global_robot_format))
        .chain(
            external
                .iter()
                .map(|cmd| command_schema_from_clap(cmd, None)),
        )
        .collect()
}

pub(crate) fn command_schema_from_clap(
    cmd: &Command,
    global_robot_format: Option<&Arg>,
) -> CommandSchema {
    let mut arguments: Vec<ArgumentSchema> = cmd
        .get_arguments()
        .filter(|arg| !should_skip_arg(arg))
        .map(argument_schema_from_clap)
        .collect();
    // WS-F.4: a command whose JSON flag lives on its subcommands
    // (`bookmarks list --json`, `pages key list --json`, `sources list --json`)
    // does emit JSON; reporting `false` here told agents to avoid a surface
    // they could use. Look through the subcommand tree, not only the top level.
    let has_json_output = command_tree_has_json_flag(cmd);
    if has_json_output
        && let Some(robot_format) = global_robot_format
        && !arguments.iter().any(|arg| arg.name == "robot-format")
    {
        arguments.push(argument_schema_from_clap(robot_format));
    }

    CommandSchema {
        name: cmd.get_name().to_string(),
        description: cmd
            .get_about()
            .or_else(|| cmd.get_long_about())
            .map(std::string::ToString::to_string)
            .unwrap_or_default(),
        arguments,
        has_json_output,
    }
}

/// Whether `cmd` or any subcommand beneath it accepts a `--json` flag.
pub(crate) fn command_tree_has_json_flag(cmd: &Command) -> bool {
    cmd.get_arguments()
        .any(|arg| arg.get_id().as_str() == "json")
        || cmd.get_subcommands().any(command_tree_has_json_flag)
}

pub(crate) fn argument_schema_from_clap(arg: &Arg) -> ArgumentSchema {
    let num_args = arg.get_num_args().unwrap_or_default();
    let takes_values = arg.get_action().takes_values() && num_args.takes_values();

    let arg_type = if !takes_values {
        "flag".to_string()
    } else if arg.is_positional() {
        "positional".to_string()
    } else {
        "option".to_string()
    };

    let value_type = if takes_values {
        infer_value_type(arg)
    } else {
        None
    };

    let default = {
        let defaults = arg.get_default_values();
        if defaults.is_empty() {
            None
        } else {
            Some(
                defaults
                    .iter()
                    .map(|v| v.to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
                    .join(","),
            )
        }
    };

    ArgumentSchema {
        name: arg.get_long().map_or_else(
            || arg.get_id().as_str().to_string(),
            std::string::ToString::to_string,
        ),
        short: arg.get_short(),
        description: arg
            .get_help()
            .or_else(|| arg.get_long_help())
            .map(std::string::ToString::to_string)
            .unwrap_or_default(),
        arg_type,
        value_type,
        required: arg.is_required_set(),
        default,
        enum_values: extract_enum_values(arg),
        repeatable: infer_repeatable(arg, num_args),
    }
}

pub(crate) const INTEGER_ARG_NAMES: &[&str] = &[
    "limit",
    "offset",
    "max-content-length",
    "max-tokens",
    "max-sessions",
    "max-evidence",
    "conversation-id",
    "context-lines",
    "max-excerpt-chars",
    "freshness-window-seconds",
    "days",
    "line",
    "message-index",
    "context",
    "stale-threshold",
    "timeout",
];

pub(crate) fn infer_value_type(arg: &Arg) -> Option<String> {
    let name = arg.get_long().map_or_else(
        || arg.get_id().as_str().to_string(),
        std::string::ToString::to_string,
    );

    if !arg.get_possible_values().is_empty() {
        return Some("enum".to_string());
    }

    if matches!(
        arg.get_value_hint(),
        ValueHint::AnyPath | ValueHint::DirPath | ValueHint::FilePath | ValueHint::ExecutablePath
    ) {
        return Some("path".to_string());
    }

    if INTEGER_ARG_NAMES.contains(&name.as_str()) {
        return Some("integer".to_string());
    }

    Some("string".to_string())
}

pub(crate) fn extract_enum_values(arg: &Arg) -> Option<Vec<String>> {
    let values = arg.get_possible_values();
    if values.is_empty() {
        None
    } else {
        Some(values.iter().map(|v| v.get_name().to_string()).collect())
    }
}

pub(crate) fn infer_repeatable(arg: &Arg, num_args: clap::builder::ValueRange) -> Option<bool> {
    let multi_values = num_args.max_values() > 1;
    let append_action = matches!(arg.get_action(), ArgAction::Append | ArgAction::Count);

    if multi_values || append_action {
        Some(true)
    } else {
        None
    }
}

pub(crate) fn should_skip_arg(arg: &Arg) -> bool {
    arg.is_hide_set() || matches!(arg.get_id().as_str(), "help" | "version")
}
