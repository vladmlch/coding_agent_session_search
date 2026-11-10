use crate::cli::args::Cli;
use crate::cli::args_sub::{RobotFormat, SourcesCommand};
use crate::cli::commands::mappings::run_mappings_command;
use crate::cli::commands::models::run_models_remove::run_agents_command;
use crate::cli::commands::search::fieldbudgets::robot_format_from_env;
use crate::cli::commands::search::toon_encode_options_from_env::output_structured_value;
use crate::cli::error::{CliError, CliResult};
use crate::cli::output::describe_command::resolve_subcommand_structured_format;
use crate::cli::paths::default_data_dir;
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
use anyhow::Result;
use std::path::{Path, PathBuf};

/// Handle sources subcommands.
pub(crate) fn run_sources_command(cmd: SourcesCommand, cli: &Cli) -> CliResult<()> {
    match cmd {
        SourcesCommand::List { verbose, json } => {
            let structured_format = resolve_subcommand_structured_format(cli, json);
            run_sources_list(verbose, structured_format)
        }
        SourcesCommand::Add { name, paths, json } => {
            let structured_format = resolve_subcommand_structured_format(cli, json);
            run_sources_add(&name, paths, structured_format)
        }
        SourcesCommand::Remove { name, purge, yes } => run_sources_remove(&name, purge, yes),
        SourcesCommand::Doctor {
            source,
            json,
            budget_ms,
            per_host_budget_ms,
        } => {
            let structured_format = resolve_subcommand_structured_format(cli, json);
            run_sources_doctor(
                source.as_deref(),
                structured_format,
                budget_ms,
                per_host_budget_ms,
            )
        }
        SourcesCommand::Mappings(action) => run_mappings_command(action, cli),
        SourcesCommand::Agents(action) => run_agents_command(action, cli),
    }
}

/// List configured sources.
///
/// `type = "ssh"` entries are legacy remote archive records: they are
/// reported with `kind = "legacy-remote"`, `supported = false`, and the
/// number of archived conversations still attributed to them.
pub(crate) fn run_sources_list(verbose: bool, output_format: Option<RobotFormat>) -> CliResult<()> {
    use crate::sources::config::SourcesConfig;

    let config = SourcesConfig::load().map_err(|e| CliError {
        code: 9,
        kind: CliErrorKind::Config.kind_str(),
        message: format!("Failed to load sources config: {e}"),
        hint: Some("Run 'cass sources add' to configure a source".into()),
        retryable: false,
    })?;

    // Get config path for display
    let config_path = SourcesConfig::config_path()
        .ok()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "unknown".into());

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });
    let disabled_agents = config.configured_disabled_agents();
    let data_dir = default_data_dir();
    let db_path = data_dir.join("agent_search.db");
    // Archived conversation counts are best-effort: a missing or locked
    // archive still yields a correct list, just with zero counts.
    let archived_counts = crate::storage::sqlite::FrankenStorage::open_strict_readonly(&db_path)
        .and_then(|storage| storage.conversation_counts_by_source())
        .unwrap_or_default();

    if let Some(fmt) = structured_format {
        let sources_json: Vec<serde_json::Value> = config
            .sources
            .iter()
            .map(|s| {
                if s.is_remote() {
                    serde_json::json!({
                        "name": s.name,
                        "kind": "legacy-remote",
                        "supported": false,
                        "type": s.source_type.as_str(),
                        "host": s.host,
                        "paths": s.paths,
                        "platform": s.platform.map(|p| p.to_string()),
                        "sync_schedule": s.sync_schedule.to_string(),
                        "archived_conversations": archived_counts.get(&s.name).copied().unwrap_or(0),
                    })
                } else {
                    serde_json::json!({
                        "name": s.name,
                        "kind": "local",
                        "supported": true,
                        "type": s.source_type.as_str(),
                        "paths": s.paths,
                    })
                }
            })
            .collect();

        let output = serde_json::json!({
            "config_path": config_path,
            "sources": sources_json,
            "disabled_agents": disabled_agents,
            "total": config.sources.len(),
        });
        return output_structured_value(output, fmt);
    }

    println!("CASS Sources Configuration");
    println!("===========================");
    println!("Config: {config_path}");
    if !disabled_agents.is_empty() {
        println!("Excluded agents: {}", disabled_agents.join(", "));
    }
    println!();

    if config.sources.is_empty() {
        println!("No sources configured.");
        if !disabled_agents.is_empty() {
            println!();
            println!("Indexing exclusions are still active for future runs.");
        }
        println!();
        println!("To add a local source, run:");
        println!("  cass sources add <name> --path <dir>");
        return Ok(());
    }

    if verbose {
        // Verbose output with full details
        for source in &config.sources {
            println!("Source: {}", source.name);
            println!("  Type: {}", source.source_type);
            if source.is_remote() {
                let archived = archived_counts.get(&source.name).copied().unwrap_or(0);
                println!("  Kind: legacy-remote (archive; remote sync removed)");
                println!("  Archived conversations: {archived}");
            } else {
                println!("  Kind: local");
            }
            if let Some(ref host) = source.host {
                println!("  Host: {host}");
            }
            if source.is_remote() {
                println!("  Legacy schedule: {}", source.sync_schedule);
            }
            if let Some(platform) = source.platform {
                println!("  Platform: {platform}");
            }
            if !source.paths.is_empty() {
                println!("  Paths:");
                for path in &source.paths {
                    println!("    - {path}");
                }
            }
            if !source.path_mappings.is_empty() {
                println!("  Path Mappings:");
                for mapping in &source.path_mappings {
                    if let Some(agents) = &mapping.agents {
                        println!(
                            "    {} -> {} (agents: {})",
                            mapping.from,
                            mapping.to,
                            agents.join(", ")
                        );
                    } else {
                        println!("    {} -> {}", mapping.from, mapping.to);
                    }
                }
            }
            println!();
        }
    } else {
        // Table output
        println!("  {:15} {:13} {:30} {:>5}", "NAME", "KIND", "HOST", "PATHS");
        println!("  {}", "-".repeat(67));
        for source in &config.sources {
            let host = source.host.as_deref().unwrap_or("-");
            let host_truncated = if host.len() > 30 {
                format!("{}...", &host[..host.floor_char_boundary(27)])
            } else {
                host.to_string()
            };
            let kind = if source.is_remote() {
                "legacy-remote"
            } else {
                "local"
            };
            println!(
                "  {:15} {:13} {:30} {:>5}",
                source.name,
                kind,
                host_truncated,
                source.paths.len()
            );
        }
        println!();
    }

    println!("Total: {} source(s)", config.sources.len());

    Ok(())
}

/// Add a new local source: extra directories indexed in place under a name.
pub(crate) fn run_sources_add(
    name: &str,
    paths: Vec<String>,
    output_format: Option<RobotFormat>,
) -> CliResult<()> {
    use crate::sources::config::{SourceDefinition, SourcesConfig};
    use crate::sources::provenance::SourceKind;

    let structured_format = output_format.map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    // The remote `sources add user@host` form was removed with remote sync;
    // report it through the same removed-command envelope as deleted verbs.
    if name.contains('@') {
        return Err(CliError {
            code: 2,
            kind: CliErrorKind::RemovedCommand.kind_str(),
            message: format!("`cass sources add {name}` was removed: cass runs fully local-only"),
            hint: Some(
                "local equivalent: cass sources add NAME --path PATH adds a local source".into(),
            ),
            retryable: false,
        });
    }

    if paths.is_empty() {
        return Err(CliError {
            code: 10,
            kind: CliErrorKind::Config.kind_str(),
            message: "No paths given: a local source needs at least one --path".to_string(),
            hint: Some("cass sources add NAME --path <dir> [--path <dir> ...]".into()),
            retryable: false,
        });
    }

    // Load existing config
    let mut config = SourcesConfig::load().map_err(|e| CliError {
        code: 9,
        kind: CliErrorKind::Config.kind_str(),
        message: format!("Failed to load sources config: {e}"),
        hint: None,
        retryable: false,
    })?;

    // Check for duplicate
    if config.find_source(name).is_some() {
        return Err(CliError {
            code: 10,
            kind: CliErrorKind::Config.kind_str(),
            message: format!("Source '{name}' already exists"),
            hint: Some("Use a different name or remove the existing source first".into()),
            retryable: false,
        });
    }

    // Create source definition
    let source = SourceDefinition {
        name: name.to_string(),
        source_type: SourceKind::Local,
        paths: paths.clone(),
        ..Default::default()
    };

    // Add and save
    config.add_source(source).map_err(|e| CliError {
        code: 10,
        kind: CliErrorKind::Config.kind_str(),
        message: format!("Failed to add source: {e}"),
        hint: None,
        retryable: false,
    })?;

    config.save().map_err(|e| CliError {
        code: 11,
        kind: CliErrorKind::Config.kind_str(),
        message: format!("Failed to save config: {e}"),
        hint: Some("Check file permissions on config directory".into()),
        retryable: false,
    })?;

    // Success output
    let config_path = SourcesConfig::config_path()
        .ok()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "~/.config/cass/sources.toml".into());

    if let Some(fmt) = structured_format {
        let output = serde_json::json!({
            "status": "added",
            "source": {
                "name": name,
                "kind": "local",
                "type": SourceKind::Local.as_str(),
                "paths": paths,
            },
            "config_path": config_path,
            "next_steps": [
                "cass index".to_string(),
                "cass sources list".to_string(),
            ],
        });
        return output_structured_value(output, fmt);
    }

    println!();
    println!("Added local source '{name}'");
    println!("  Paths: {} path(s)", paths.len());
    println!("  Config: {config_path}");
    println!();
    println!("Next steps:");
    println!("  cass index          # Index sessions under this source");
    println!("  cass sources list   # View all configured sources");

    Ok(())
}

/// Remove a configured source (P5.7)
pub(crate) fn run_sources_remove(name: &str, purge: bool, skip_confirm: bool) -> CliResult<()> {
    use crate::sources::config::SourcesConfig;

    // Load existing config
    let mut config = SourcesConfig::load().map_err(|e| CliError {
        code: 9,
        kind: CliErrorKind::Config.kind_str(),
        message: format!("Failed to load sources config: {e}"),
        hint: None,
        retryable: false,
    })?;

    // Check source exists
    let stored_source_name = config.find_source(name).map(|source| source.name.clone());
    let Some(stored_source_name) = stored_source_name else {
        return Err(CliError {
            code: 13,
            kind: CliErrorKind::NotFound.kind_str(),
            message: format!("Source '{name}' not found"),
            hint: Some("Run 'cass sources list' to see configured sources".into()),
            retryable: false,
        });
    };

    // Confirmation prompt
    if !skip_confirm {
        let display_name = &stored_source_name;
        let msg = if purge {
            format!(
                "Remove source '{display_name}' and delete indexed data? This cannot be undone. [y/N]: "
            )
        } else {
            format!("Remove source '{display_name}' from configuration? [y/N]: ")
        };
        print!("{msg}");
        std::io::Write::flush(&mut std::io::stdout()).ok();

        let mut input = String::new();
        std::io::stdin()
            .read_line(&mut input)
            .map_err(|e| CliError {
                code: 14,
                kind: CliErrorKind::Io.kind_str(),
                message: format!("Failed to read input: {e}"),
                hint: None,
                retryable: false,
            })?;

        let input = input.trim().to_lowercase();
        if input != "y" && input != "yes" {
            println!("Cancelled.");
            return Ok(());
        }
    }

    // Remove from config
    config.remove_source(&stored_source_name);
    config.save().map_err(|e| CliError {
        code: 11,
        kind: CliErrorKind::Config.kind_str(),
        message: format!("Failed to save config: {e}"),
        hint: Some("Check file permissions on config directory".into()),
        retryable: false,
    })?;

    println!("Removed '{}' from configuration.", stored_source_name);

    // Handle purge: drop the archived mirror directory for this source.
    if purge {
        let data_dir = default_data_dir();
        let source_dir = data_dir.join("remotes").join(&stored_source_name);
        if source_dir.exists() {
            std::fs::remove_dir_all(&source_dir).map_err(|e| CliError {
                code: 15,
                kind: CliErrorKind::Io.kind_str(),
                message: format!("Failed to delete archived mirror data: {e}"),
                hint: None,
                retryable: false,
            })?;
            println!("Deleted archived mirror data at {}", source_dir.display());
        }
        println!("Note: Run 'cass index --full' to remove entries from the search index.");
    }

    Ok(())
}

/// Diagnostic check result for sources doctor command (P5.6)
#[derive(serde::Serialize)]
pub(crate) struct DiagnosticCheck {
    pub(crate) name: String,
    pub(crate) status: String, // "pass", "warn", "fail"
    pub(crate) message: String,
    pub(crate) remediation: Option<String>,
}

/// Aggregated diagnostics for a single source (P5.6)
#[derive(serde::Serialize)]
pub(crate) struct SourceDiagnostics {
    pub(crate) source_id: String,
    pub(crate) checks: Vec<DiagnosticCheck>,
    pub(crate) passed: usize,
    pub(crate) warnings: usize,
    pub(crate) failed: usize,
}

/// The `sources doctor --json` envelope: the detailed raw per-source checks
/// under `diagnostics`, the robot budget block, and `sources.toml` validation
/// findings. Local-only: remote host probes were removed with the fleet
/// surface, so every check is derived from local evidence.
#[derive(serde::Serialize)]
pub(crate) struct SourcesDoctorOutput<'a> {
    pub(crate) diagnostics: &'a [SourceDiagnostics],
    pub(crate) budget: &'a crate::robot_budget_envelope::BudgetBlock,
    /// Problems in `sources.toml` itself that no per-source check can see.
    pub(crate) config_validation: &'a crate::sources::config_validation::SourceConfigValidation,
}

/// Validate the parsed source configuration against this host. The validator
/// is pure; this gathers its environment signals: `.sources.toml.tmp.*` files
/// an interrupted config write left beside the config. `disabled_agents`
/// names are not checked: they accept aliases (`claude_code`) that raw
/// connector names would misreport as unknown. Remote transports are never
/// probed: legacy `type = "ssh"` entries surface as `legacy_remote_source`
/// warnings.
pub(crate) fn sources_doctor_config_validation(
    config: &crate::sources::config::SourcesConfig,
) -> crate::sources::config_validation::SourceConfigValidation {
    use crate::sources::config_validation::{ConfigValidationContext, validate_sources_config};
    let orphaned_temp_files = crate::sources::config::SourcesConfig::config_path()
        .ok()
        .and_then(|path| path.parent().map(Path::to_path_buf))
        .and_then(|dir| std::fs::read_dir(dir).ok())
        .map_or(0, |entries| {
            entries
                .filter_map(Result::ok)
                .filter(|entry| {
                    entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".sources.toml.tmp")
                })
                .count()
        });
    let ctx = ConfigValidationContext {
        orphaned_temp_files,
        known_agents: Vec::new(),
    };
    validate_sources_config(config, &ctx)
}

/// Default wall-clock budget for `sources doctor` when `--budget-ms` is not
/// given. All remaining checks are local metadata reads, so this is generous.
pub(crate) const SOURCES_DOCTOR_DEFAULT_BUDGET_MS: u64 = 5_000;

/// Diagnose source configuration and local storage issues (P5.6).
///
/// Local-only: the remote SSH probes, fixture-probe seam, and fleet-doctor
/// health projection were removed with the fleet surface, so every check is
/// derived from cass-owned local evidence — `sources.toml` validation, source
/// path syntax, local path readability for local sources, and local mirror
/// storage writability. A configured remote host is reported as a warning:
/// nothing on the network is contacted.
pub(crate) fn run_sources_doctor(
    source_filter: Option<&str>,
    output_format: Option<RobotFormat>,
    total_budget_override_ms: Option<u64>,
    per_host_budget_override_ms: Option<u64>,
) -> CliResult<()> {
    use crate::sources::config::{SourcesConfig, source_names_equal, source_path_entry_error};
    use colored::Colorize;

    // `--per-host-budget-ms` bounded the removed per-remote-host probes. The
    // flag stays parseable for CLI compatibility but no longer scopes
    // anything; likewise CASS_FLEET_BUDGET_MS / CASS_FLEET_PER_HOST_BUDGET_MS
    // are tolerated and ignored (reported as deprecated by `cass diag`).
    let _ = per_host_budget_override_ms;
    let total_budget = crate::robot_budget_envelope::RobotBudget::new(
        total_budget_override_ms.unwrap_or(SOURCES_DOCTOR_DEFAULT_BUDGET_MS),
    );
    let config = SourcesConfig::load().map_err(|e| CliError {
        code: 9,
        kind: CliErrorKind::Config.kind_str(),
        message: format!("Failed to load sources config: {e}"),
        hint: Some("Run 'cass sources add' to configure a source".into()),
        retryable: false,
    })?;

    if config.sources.is_empty() {
        let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
            if matches!(fmt, RobotFormat::Sessions) {
                RobotFormat::Compact
            } else {
                fmt
            }
        });

        if let Some(_fmt) = structured_format {
            let budget = crate::robot_budget_envelope::BudgetBlock::from_budget(
                &total_budget,
                Vec::new(),
                None,
            );
            println!(
                "{}",
                serde_json::json!({
                    "error": "No sources configured",
                    "sources": [],
                    "budget": budget,
                })
            );
        } else {
            println!("No sources configured.");
            println!("Run 'cass sources add <name> --path <dir>' to add a local source.");
        }
        return Ok(());
    }

    let config_validation = sources_doctor_config_validation(&config);

    // Filter sources if specified
    let sources_to_check: Vec<_> = config
        .sources
        .iter()
        .filter(|s| source_filter.is_none_or(|filter| source_names_equal(filter, &s.name)))
        .collect();

    if sources_to_check.is_empty() {
        return Err(CliError {
            code: 13,
            kind: CliErrorKind::NotFound.kind_str(),
            message: format!("Source '{}' not found", source_filter.unwrap_or("unknown")),
            hint: Some("Run 'cass sources list' to see configured sources".into()),
            retryable: false,
        });
    }

    let mut all_diagnostics = Vec::new();

    for source in sources_to_check {
        let mut checks = Vec::new();

        // A configured remote host cannot be probed in a local-only build:
        // surface that explicitly instead of silently skipping the source.
        if let Some(host) = source.host.as_deref() {
            checks.push(DiagnosticCheck {
                name: "Remote Host".into(),
                status: "warn".into(),
                message: format!(
                    "Remote host {host} cannot be probed: remote connectivity was removed from cass"
                ),
                remediation: Some(
                    "Remove this source or copy its sessions onto this machine".into(),
                ),
            });
        }

        // Per-path syntax validation; local (non-remote) sources additionally
        // get a metadata-only existence check.
        for (idx, path) in source.paths.iter().enumerate() {
            if let Some(message) = source_path_entry_error(idx, path) {
                checks.push(DiagnosticCheck {
                    name: format!("Source Path: paths[{idx}]"),
                    status: "fail".into(),
                    message: format!("Invalid source path: {message}"),
                    remediation: Some("Fix or remove this path in sources.toml".into()),
                });
            } else if source.host.is_none() {
                checks.push(check_local_source_path(path));
            }
        }

        // Local mirror storage writable
        let storage_check = check_local_storage(&source.name);
        checks.push(storage_check);

        // Compute summary
        let passed = checks.iter().filter(|c| c.status == "pass").count();
        let warnings = checks.iter().filter(|c| c.status == "warn").count();
        let failed = checks.iter().filter(|c| c.status == "fail").count();

        all_diagnostics.push(SourceDiagnostics {
            source_id: source.name.clone(),
            checks,
            passed,
            warnings,
            failed,
        });
    }

    let budget =
        crate::robot_budget_envelope::BudgetBlock::from_budget(&total_budget, Vec::new(), None);

    // Output results
    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(_fmt) = structured_format {
        let output = SourcesDoctorOutput {
            diagnostics: &all_diagnostics,
            budget: &budget,
            config_validation: &config_validation,
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&output).unwrap_or_default()
        );
    } else {
        for diag in &all_diagnostics {
            println!();
            println!("{}", format!("Checking source: {}", diag.source_id).bold());
            println!();

            for check in &diag.checks {
                let icon = match check.status.as_str() {
                    "pass" => "✓".green(),
                    "warn" => "⚠".yellow(),
                    "fail" => "✗".red(),
                    _ => "?".normal(),
                };
                let name_styled = match check.status.as_str() {
                    "pass" => check.name.green(),
                    "warn" => check.name.yellow(),
                    "fail" => check.name.red(),
                    _ => check.name.normal(),
                };
                println!("  {} {}", icon, name_styled);
                println!("    {}", check.message.dimmed());
                if let Some(ref hint) = check.remediation {
                    println!("    {}: {}", "Hint".cyan(), hint);
                }
            }

            println!();
            println!(
                "Summary: {} passed, {} warnings, {} failed",
                diag.passed.to_string().green(),
                diag.warnings.to_string().yellow(),
                diag.failed.to_string().red()
            );
        }
        if !config_validation.issues.is_empty() {
            use crate::sources::config_validation::ConfigIssueSeverity;
            println!();
            println!("{}", "Configuration".bold());
            for issue in &config_validation.issues {
                let icon = match issue.severity {
                    ConfigIssueSeverity::Error => "✗".red(),
                    ConfigIssueSeverity::Warning => "⚠".yellow(),
                    ConfigIssueSeverity::Info => "ℹ".cyan(),
                };
                println!("  {icon} {}", issue.detail);
                if let Some(command) = issue.safe_next_command.as_deref() {
                    println!("    {}: {command}", "Next".cyan());
                }
            }
        }
    }

    // Set exit code based on results
    let total_failed: usize = all_diagnostics.iter().map(|d| d.failed).sum();
    if total_failed > 0 || config_validation.has_errors() {
        std::process::exit(1);
    }

    Ok(())
}

/// Metadata-only existence check for a local (non-remote) source path.
pub(crate) fn check_local_source_path(path: &str) -> DiagnosticCheck {
    let expanded = if let Some(rest) = path.strip_prefix("~/") {
        dirs::home_dir()
            .map(|home| home.join(rest))
            .unwrap_or_else(|| PathBuf::from(path))
    } else {
        PathBuf::from(path)
    };
    match std::fs::metadata(&expanded) {
        Ok(metadata) if metadata.is_dir() => DiagnosticCheck {
            name: format!("Source Path: {path}"),
            status: "pass".into(),
            message: "Path exists (read-only check; contents not scanned)".into(),
            remediation: None,
        },
        Ok(_) => DiagnosticCheck {
            name: format!("Source Path: {path}"),
            status: "fail".into(),
            message: "Path exists but is not a directory".into(),
            remediation: Some("Point this source at a directory".into()),
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => DiagnosticCheck {
            name: format!("Source Path: {path}"),
            status: "fail".into(),
            message: "Path does not exist".into(),
            remediation: Some("Remove this path or create it locally".into()),
        },
        Err(error) => DiagnosticCheck {
            name: format!("Source Path: {path}"),
            status: "warn".into(),
            message: format!("Could not inspect path: {error}"),
            remediation: Some("Check permissions on this path".into()),
        },
    }
}

/// Conservatively assess local storage from metadata without mutating it.
pub(crate) fn check_local_storage(source_name: &str) -> DiagnosticCheck {
    let data_dir = default_data_dir();
    let source_dir = data_dir.join("remotes").join(source_name);
    check_local_storage_path(&source_dir)
}

pub(crate) fn check_local_storage_path(source_dir: &Path) -> DiagnosticCheck {
    let mut inspected = None;
    for candidate in source_dir.ancestors() {
        match std::fs::metadata(candidate) {
            Ok(metadata) => {
                inspected = Some((candidate, metadata));
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return DiagnosticCheck {
                    name: "Local Storage".into(),
                    status: "fail".into(),
                    message: format!(
                        "Cannot inspect {} without writing: {error}",
                        candidate.display()
                    ),
                    remediation: Some("Check file permissions on data directory".into()),
                };
            }
        }
    }

    let Some((inspected_path, metadata)) = inspected else {
        return DiagnosticCheck {
            name: "Local Storage".into(),
            status: "fail".into(),
            message: format!(
                "No existing ancestor of {} could be inspected without writing",
                source_dir.display()
            ),
            remediation: Some("Check the configured data directory path".into()),
        };
    };

    if !metadata.is_dir() {
        return DiagnosticCheck {
            name: "Local Storage".into(),
            status: "fail".into(),
            message: format!(
                "{} is not a directory (read-only check; no write attempted)",
                inspected_path.display()
            ),
            remediation: Some("Replace the path with a writable directory".into()),
        };
    }

    let target_context = if inspected_path == source_dir {
        String::new()
    } else {
        format!(" for target {}", source_dir.display())
    };
    if metadata.permissions().readonly() {
        DiagnosticCheck {
            name: "Local Storage".into(),
            status: "fail".into(),
            message: format!(
                "{} appears read-only from permission metadata{} (read-only check; no write \
                 attempted; effective ACL access was not tested)",
                inspected_path.display(),
                target_context
            ),
            remediation: Some("Check file permissions on data directory".into()),
        }
    } else {
        DiagnosticCheck {
            name: "Local Storage".into(),
            status: "pass".into(),
            message: format!(
                "{} appears writable from permission metadata{} (read-only check; no write \
                 attempted; effective ACL access was not tested)",
                inspected_path.display(),
                target_context
            ),
            remediation: None,
        }
    }
}
