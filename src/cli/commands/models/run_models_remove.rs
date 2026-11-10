use crate::cli::args::Cli;
use crate::cli::args_sub::{AgentsAction, RobotFormat};
use crate::cli::commands::models::run_models_command::{
    resolve_cli_model_name, resolve_model_install_dir,
};
use crate::cli::commands::search::fieldbudgets::robot_format_from_env;
use crate::cli::commands::search::toon_encode_options_from_env::output_structured_value;
use crate::cli::error::{CliError, CliResult};
use crate::cli::output::describe_command::resolve_subcommand_structured_format;
use crate::cli::paths::{default_data_dir, default_db_path};
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
use std::path::{Path, PathBuf};

/// Remove model files
pub(crate) fn run_models_remove(
    model_name: &str,
    skip_confirm: bool,
    data_dir_override: Option<PathBuf>,
) -> CliResult<()> {
    use colored::Colorize;

    let registry_name = resolve_cli_model_name(model_name)?;
    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    let (model_dir, _manifest) = resolve_model_install_dir(&data_dir, registry_name)?;

    if !model_dir.is_dir() {
        println!("{} Model is not installed.", "✗".yellow());
        println!("  Expected location: {}", model_dir.display());
        return Ok(());
    }

    // Calculate size
    let mut total_size: u64 = 0;
    if let Ok(entries) = std::fs::read_dir(&model_dir) {
        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                total_size += metadata.len();
            }
        }
    }
    let size_mb = total_size as f64 / 1_048_576.0;

    if !skip_confirm {
        println!("Remove Semantic Search Model");
        println!("============================");
        println!();
        println!("Model:    {}", model_name);
        println!("Location: {}", model_dir.display());
        println!("Size:     {:.1} MB", size_mb);
        println!();
        println!("This will remove all model files. Semantic search will be unavailable");
        println!("until the model is reinstalled.");
        print!("Continue? [y/N] ");
        std::io::Write::flush(&mut std::io::stdout()).ok();

        let mut input = String::new();
        std::io::stdin().read_line(&mut input).ok();
        if !input.trim().eq_ignore_ascii_case("y") && !input.trim().eq_ignore_ascii_case("yes") {
            println!("Cancelled.");
            return Ok(());
        }
    }

    // Remove the directory
    std::fs::remove_dir_all(&model_dir).map_err(|e| CliError {
        code: 24,
        kind: CliErrorKind::Io.kind_str(),
        message: format!("Failed to remove model directory: {}", e),
        hint: Some("Check file permissions".into()),
        retryable: false,
    })?;

    println!();
    println!("{} Model removed successfully.", "✓".green());
    println!("  Freed {:.1} MB", size_mb);
    println!();
    println!("To reinstall, run:");
    println!("  cass models install --from-file <DIR>");

    Ok(())
}

pub(crate) fn run_agents_command(action: AgentsAction, cli: &Cli) -> CliResult<()> {
    match action {
        AgentsAction::List { json } => {
            let structured_format = resolve_subcommand_structured_format(cli, json);
            run_agents_list(structured_format)
        }
        AgentsAction::Exclude {
            agent,
            keep_indexed_data,
        } => run_agents_exclude(&agent, keep_indexed_data, cli),
        AgentsAction::Include { agent } => run_agents_include(&agent),
    }
}

pub(crate) fn archive_agent_slug_for_exclusion(agent: &str) -> String {
    match agent.trim().to_ascii_lowercase().as_str() {
        "claude" | "claude-code" | "claude_code" => "claude_code".to_string(),
        "oh-my-pi" | "oh_my_pi" | "ohmypi" => "omp".to_string(),
        other => other.to_string(),
    }
}

pub(crate) fn archive_data_dir_for_agents_command(cli: &Cli) -> PathBuf {
    cli.db
        .as_ref()
        .and_then(|db_path| db_path.parent().map(Path::to_path_buf))
        .unwrap_or_else(default_data_dir)
}

/// What `sources agents exclude` did with rows already archived for the agent.
/// "Nothing to purge" and "could not look" must not print the same sentence.
pub(crate) enum ExcludedAgentPurge {
    /// The archive opened; the result may report zero rows.
    Purged(crate::storage::sqlite::AgentArchivePurgeResult),
    /// No archive file exists at the resolved path.
    NoArchive(PathBuf),
    /// An archive exists but could not be opened, so nothing was purged.
    ArchiveUnavailable { db_path: PathBuf, error: String },
}

pub(crate) fn purge_excluded_agent_archive_data(
    agent: &str,
    cli: &Cli,
) -> CliResult<ExcludedAgentPurge> {
    use crate::storage::sqlite::FrankenStorage;

    let db_path = cli.db.clone().unwrap_or_else(default_db_path);
    if !db_path.is_file() {
        return Ok(ExcludedAgentPurge::NoArchive(db_path));
    }

    let data_dir = archive_data_dir_for_agents_command(cli);
    let archive_agent_slug = archive_agent_slug_for_exclusion(agent);
    let storage = match FrankenStorage::open(&db_path) {
        Ok(storage) => storage,
        Err(err) => {
            tracing::warn!(
                db_path = %db_path.display(),
                error = %err,
                "skipping excluded-agent archive purge because the local archive could not be opened"
            );
            return Ok(ExcludedAgentPurge::ArchiveUnavailable {
                db_path,
                error: format!("{err:#}"),
            });
        }
    };

    let purge = storage
        .purge_agent_archive_data(&archive_agent_slug)
        .map_err(|e| CliError {
            code: 5,
            kind: CliErrorKind::ArchivePurge.kind_str(),
            message: format!("Failed to purge indexed data for '{archive_agent_slug}': {e}"),
            hint: Some("The exclusion was still saved; run 'cass index --full' after fixing the archive if needed".into()),
            retryable: false,
        })?;
    if purge.conversations_deleted == 0 {
        return Ok(ExcludedAgentPurge::Purged(purge));
    }

    storage.rebuild_fts().map_err(|e| CliError {
        code: 5,
        kind: CliErrorKind::ArchiveFtsRebuild.kind_str(),
        message: format!("Purged '{archive_agent_slug}' but failed to rebuild FTS: {e}"),
        hint: Some("Run 'cass index --full' to refresh derived search data".into()),
        retryable: false,
    })?;
    storage.rebuild_analytics().map_err(|e| CliError {
        code: 5,
        kind: CliErrorKind::ArchiveAnalyticsRebuild.kind_str(),
        message: format!(
            "Purged '{archive_agent_slug}' but failed to rebuild analytics rollups: {e}"
        ),
        hint: Some("Run 'cass index --full' to refresh derived analytics data".into()),
        retryable: false,
    })?;
    storage.rebuild_daily_stats().map_err(|e| CliError {
        code: 5,
        kind: CliErrorKind::ArchiveDailyStatsRebuild.kind_str(),
        message: format!("Purged '{archive_agent_slug}' but failed to rebuild daily stats: {e}"),
        hint: Some("Run 'cass index --full' to refresh derived daily stats".into()),
        retryable: false,
    })?;
    storage.rebuild_token_daily_stats().map_err(|e| CliError {
        code: 5,
        kind: CliErrorKind::ArchiveTokenDailyStatsRebuild.kind_str(),
        message: format!(
            "Purged '{archive_agent_slug}' but failed to rebuild token_daily_stats: {e}"
        ),
        hint: Some("Run 'cass index --full' to refresh token analytics".into()),
        retryable: false,
    })?;
    let remaining_conversations = storage.total_conversation_count().map_err(|e| CliError {
        code: 5,
        kind: CliErrorKind::ArchiveCount.kind_str(),
        message: format!(
            "Purged '{archive_agent_slug}' but failed to count remaining conversations: {e}"
        ),
        hint: Some("Run 'cass index --full' to refresh the archive".into()),
        retryable: false,
    })?;
    drop(storage);

    crate::indexer::rebuild_tantivy_from_db(&db_path, &data_dir, remaining_conversations, None)
        .map_err(|e| CliError {
            code: 5,
            kind: CliErrorKind::LexicalRebuild.kind_str(),
            message: format!(
                "Purged '{archive_agent_slug}' but failed to rebuild the lexical search index: {e}"
            ),
            hint: Some("Run 'cass index --full' to refresh lexical search data".into()),
            retryable: false,
        })?;

    Ok(ExcludedAgentPurge::Purged(purge))
}

pub(crate) fn run_agents_list(output_format: Option<RobotFormat>) -> CliResult<()> {
    use crate::sources::config::SourcesConfig;

    let config = SourcesConfig::load().map_err(|e| CliError {
        code: 9,
        kind: CliErrorKind::Config.kind_str(),
        message: format!("Failed to load sources config: {e}"),
        hint: None,
        retryable: false,
    })?;
    let disabled_agents = config.configured_disabled_agents();

    if let Some(fmt) = output_format.or_else(robot_format_from_env) {
        return output_structured_value(
            serde_json::json!({
                "disabled_agents": disabled_agents,
                "total": disabled_agents.len(),
            }),
            if matches!(fmt, RobotFormat::Sessions) {
                RobotFormat::Compact
            } else {
                fmt
            },
        );
    }

    if disabled_agents.is_empty() {
        println!("No agents are excluded from indexing.");
        println!();
        println!("Exclude one with:");
        println!("  cass sources agents exclude openclaw");
        return Ok(());
    }

    println!("Excluded agents:");
    for agent in disabled_agents {
        println!("  - {agent}");
    }
    Ok(())
}

pub(crate) fn run_agents_exclude(agent: &str, keep_indexed_data: bool, cli: &Cli) -> CliResult<()> {
    use crate::sources::config::SourcesConfig;

    let mut config = SourcesConfig::load().map_err(|e| CliError {
        code: 9,
        kind: CliErrorKind::Config.kind_str(),
        message: format!("Failed to load sources config: {e}"),
        hint: None,
        retryable: false,
    })?;

    let changed = config
        .exclude_agent_from_indexing(agent)
        .map_err(|e| CliError {
            code: 11,
            kind: CliErrorKind::Config.kind_str(),
            message: format!("Failed to update excluded agents: {e}"),
            hint: Some("Provide a non-empty agent slug such as 'openclaw'".into()),
            retryable: false,
        })?;

    config.save().map_err(|e| CliError {
        code: 11,
        kind: CliErrorKind::Config.kind_str(),
        message: format!("Failed to save config: {e}"),
        hint: Some("Check file permissions on config directory".into()),
        retryable: false,
    })?;

    let purge = if keep_indexed_data {
        None
    } else {
        Some(purge_excluded_agent_archive_data(agent, cli)?)
    };

    let agent_slug = agent.trim().to_ascii_lowercase();
    if changed {
        println!("Excluded '{agent_slug}' from future indexing runs.");
        println!("This applies to local scans, remote mirror scans, and watch mode.");
    } else {
        println!("'{agent_slug}' was already excluded from indexing.");
    }
    match purge {
        None => println!("Existing indexed archive data was left untouched."),
        Some(ExcludedAgentPurge::Purged(purge)) if purge.conversations_deleted > 0 => {
            println!(
                "Purged {} conversations and {} messages already archived for that agent.",
                purge.conversations_deleted, purge.messages_deleted
            );
            println!("Lexical search data was rebuilt from the remaining archive.");
            println!(
                "If you use semantic search assets, rerun `cass index --semantic` once to refresh them."
            );
        }
        Some(ExcludedAgentPurge::Purged(_)) => {
            println!("No already archived data for that agent was present in the local archive.");
        }
        Some(ExcludedAgentPurge::NoArchive(db_path)) => {
            println!(
                "No local archive exists at {}; there was nothing to purge.",
                db_path.display()
            );
        }
        Some(ExcludedAgentPurge::ArchiveUnavailable { db_path, error }) => {
            println!(
                "The local archive at {} could not be opened ({error}).",
                db_path.display()
            );
            println!("Data already archived for that agent was NOT purged.");
            println!(
                "Inspect it with `cass doctor --json`, then rerun `cass sources agents exclude {agent_slug}` to purge."
            );
        }
    }
    println!();
    println!("View exclusions with:");
    println!("  cass sources agents list");
    Ok(())
}

pub(crate) fn run_agents_include(agent: &str) -> CliResult<()> {
    use crate::sources::config::SourcesConfig;

    let mut config = SourcesConfig::load().map_err(|e| CliError {
        code: 9,
        kind: CliErrorKind::Config.kind_str(),
        message: format!("Failed to load sources config: {e}"),
        hint: None,
        retryable: false,
    })?;

    let changed = config
        .include_agent_in_indexing(agent)
        .map_err(|e| CliError {
            code: 11,
            kind: CliErrorKind::Config.kind_str(),
            message: format!("Failed to update excluded agents: {e}"),
            hint: Some("Provide a non-empty agent slug such as 'openclaw'".into()),
            retryable: false,
        })?;

    config.save().map_err(|e| CliError {
        code: 11,
        kind: CliErrorKind::Config.kind_str(),
        message: format!("Failed to save config: {e}"),
        hint: Some("Check file permissions on config directory".into()),
        retryable: false,
    })?;

    if changed {
        println!(
            "Re-enabled '{}' for future indexing runs.",
            agent.trim().to_ascii_lowercase()
        );
    } else {
        println!(
            "'{}' was not excluded from indexing.",
            agent.trim().to_ascii_lowercase()
        );
    }
    println!();
    println!("View exclusions with:");
    println!("  cass sources agents list");
    Ok(())
}
