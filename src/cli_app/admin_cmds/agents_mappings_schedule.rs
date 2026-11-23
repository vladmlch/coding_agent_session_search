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

/// Handle mappings subcommands (P6.3)
pub(crate) fn run_mappings_command(action: MappingsAction, cli: &Cli) -> CliResult<()> {
    match action {
        MappingsAction::List { source, json } => {
            let structured_format = resolve_subcommand_structured_format(cli, json);
            run_mappings_list(source.as_deref(), structured_format)?;
        }
        MappingsAction::Add {
            source,
            from,
            to,
            agents,
        } => {
            run_mappings_add(&source, &from, &to, agents)?;
        }
        MappingsAction::Remove { source, index } => {
            run_mappings_remove(&source, index)?;
        }
        MappingsAction::Test {
            source,
            path,
            agent,
        } => {
            run_mappings_test(&source, &path, agent.as_deref())?;
        }
    }
    Ok(())
}

/// List path mappings for a source (P6.3)
pub(crate) fn run_mappings_list(
    source_name: Option<&str>,
    output_format: Option<RobotFormat>,
) -> CliResult<()> {
    use crate::sources::config::SourcesConfig;

    let config = SourcesConfig::load().map_err(|e| CliError {
        code: 9,
        kind: CliErrorKind::Config.kind_str(),
        message: format!("Failed to load sources config: {e}"),
        hint: None,
        retryable: false,
    })?;

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    let sources = if let Some(source_name) = source_name {
        let source = config.find_source(source_name).ok_or_else(|| CliError {
            code: 12,
            kind: CliErrorKind::Source.kind_str(),
            message: format!("Source '{}' not found", source_name),
            hint: Some("Use 'cass sources list' to see available sources".into()),
            retryable: false,
        })?;
        vec![source]
    } else {
        config.sources.iter().collect()
    };

    if let Some(_fmt) = structured_format {
        if let Some(source_name) = source_name {
            let Some(source) = sources.first() else {
                return Err(CliError {
                    code: 12,
                    kind: CliErrorKind::Source.kind_str(),
                    message: format!("Source '{}' not found", source_name),
                    hint: Some("Use 'cass sources list' to see available sources".into()),
                    retryable: false,
                });
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "source": source_name,
                    "mappings": &source.path_mappings,
                }))
                .unwrap_or_default()
            );
        } else {
            let source_mappings: Vec<_> = sources
                .iter()
                .map(|source| {
                    serde_json::json!({
                        "source": source.name,
                        "mappings": &source.path_mappings,
                    })
                })
                .collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "sources": source_mappings,
                }))
                .unwrap_or_default()
            );
        }
    } else if sources.is_empty() {
        println!("No sources configured.");
    } else {
        for source in sources {
            println!("Path mappings for source '{}':", source.name);
            println!();

            if source.path_mappings.is_empty() {
                println!("  No path mappings configured.");
                println!();
                println!("Add mappings with:");
                println!(
                    "  cass sources mappings add {} --from /remote/path --to /local/path",
                    source.name
                );
            } else {
                for (idx, mapping) in source.path_mappings.iter().enumerate() {
                    println!("  [{}] {} -> {}", idx, mapping.from, mapping.to);
                    if let Some(ref agents) = mapping.agents {
                        println!("      agents: {}", agents.join(", "));
                    }
                }
            }
            println!();
        }
    }

    Ok(())
}

/// Add a path mapping to a source (P6.3)
pub(crate) fn run_mappings_add(
    source_name: &str,
    from: &str,
    to: &str,
    agents: Option<Vec<String>>,
) -> CliResult<()> {
    use crate::sources::config::{PathMapping, SourcesConfig};

    let mut config = SourcesConfig::load().map_err(|e| CliError {
        code: 9,
        kind: CliErrorKind::Config.kind_str(),
        message: format!("Failed to load sources config: {e}"),
        hint: None,
        retryable: false,
    })?;

    let source = config
        .find_source_mut(source_name)
        .ok_or_else(|| CliError {
            code: 12,
            kind: CliErrorKind::Source.kind_str(),
            message: format!("Source '{}' not found", source_name),
            hint: Some("Use 'cass sources list' to see available sources".into()),
            retryable: false,
        })?;

    // Create the mapping
    let mapping = if let Some(agent_list) = agents {
        PathMapping::with_agents(from, to, agent_list)
    } else {
        PathMapping::new(from, to)
    };

    // Check for duplicates
    let already_exists = source
        .path_mappings
        .iter()
        .any(|m| m.from == mapping.from && m.to == mapping.to && m.agents == mapping.agents);

    if already_exists {
        return Err(CliError {
            code: 13,
            kind: CliErrorKind::Mapping.kind_str(),
            message: "This mapping already exists".into(),
            hint: None,
            retryable: false,
        });
    }

    source.path_mappings.push(mapping.clone());

    config.save().map_err(|e| CliError {
        code: 11,
        kind: CliErrorKind::Config.kind_str(),
        message: format!("Failed to save config: {e}"),
        hint: Some("Check file permissions on config directory".into()),
        retryable: false,
    })?;

    println!("Added mapping to source '{}':", source_name);
    println!("  {} → {}", mapping.from, mapping.to);
    if let Some(agents) = &mapping.agents {
        println!("  agents: {}", agents.join(", "));
    }
    println!();
    println!("Test with:");
    println!("  cass sources mappings test {} {}", source_name, from);

    Ok(())
}

/// Remove a path mapping from a source (P6.3)
pub(crate) fn run_mappings_remove(source_name: &str, index: usize) -> CliResult<()> {
    use crate::sources::config::SourcesConfig;

    let mut config = SourcesConfig::load().map_err(|e| CliError {
        code: 9,
        kind: CliErrorKind::Config.kind_str(),
        message: format!("Failed to load sources config: {e}"),
        hint: None,
        retryable: false,
    })?;

    let source = config
        .find_source_mut(source_name)
        .ok_or_else(|| CliError {
            code: 12,
            kind: CliErrorKind::Source.kind_str(),
            message: format!("Source '{}' not found", source_name),
            hint: Some("Use 'cass sources list' to see available sources".into()),
            retryable: false,
        })?;

    if index >= source.path_mappings.len() {
        return Err(CliError {
            code: 14,
            kind: CliErrorKind::Mapping.kind_str(),
            message: format!(
                "Invalid index {}. Source has {} mapping(s).",
                index,
                source.path_mappings.len()
            ),
            hint: Some("Use 'cass sources mappings list' to see valid indices".into()),
            retryable: false,
        });
    }

    let removed = source.path_mappings.remove(index);

    config.save().map_err(|e| CliError {
        code: 11,
        kind: CliErrorKind::Config.kind_str(),
        message: format!("Failed to save config: {e}"),
        hint: Some("Check file permissions on config directory".into()),
        retryable: false,
    })?;

    println!("Removed mapping from source '{}':", source_name);
    println!("  {} → {}", removed.from, removed.to);

    Ok(())
}

/// Test how a path would be rewritten for a source (P6.3)
pub(crate) fn run_mappings_test(
    source_name: &str,
    path: &str,
    agent: Option<&str>,
) -> CliResult<()> {
    use crate::sources::config::{PathMapping, SourcesConfig};
    use colored::Colorize;

    let config = SourcesConfig::load().map_err(|e| CliError {
        code: 9,
        kind: CliErrorKind::Config.kind_str(),
        message: format!("Failed to load sources config: {e}"),
        hint: None,
        retryable: false,
    })?;

    let source = config.find_source(source_name).ok_or_else(|| CliError {
        code: 12,
        kind: CliErrorKind::Source.kind_str(),
        message: format!("Source '{}' not found", source_name),
        hint: Some("Use 'cass sources list' to see available sources".into()),
        retryable: false,
    })?;

    // Find the matching mapping
    let mut matching_mapping = None;
    let rewritten = source.rewrite_path_for_agent(path, agent);

    // Find which rule matched (if any)
    if rewritten != path {
        // Find the longest matching prefix
        let mut best_match: Option<&PathMapping> = None;
        for mapping in &source.path_mappings {
            if !mapping.applies_to_agent(agent) {
                continue;
            }
            if path.starts_with(&mapping.from)
                && best_match.is_none_or(|best| mapping.from.len() > best.from.len())
            {
                best_match = Some(mapping);
            }
        }
        matching_mapping = best_match;
    }

    println!();
    println!("Input:  {}", path);
    println!("Output: {}", rewritten);

    if let Some(mapping) = matching_mapping {
        println!("Rule:   {} → {}", mapping.from, mapping.to);
        if let Some(ref agents) = mapping.agents {
            println!("        agents: {}", agents.join(", "));
        }
        println!("Status: {} mapped", "✓".green());
    } else if rewritten == path {
        println!("Status: {} no matching rule", "✗".yellow());

        if !source.path_mappings.is_empty() {
            println!();
            println!("Available rules:");
            for mapping in &source.path_mappings {
                println!("  {} → {}", mapping.from, mapping.to);
                if let Some(ref agents) = mapping.agents {
                    println!("    agents: {}", agents.join(", "));
                }
            }
        }
    }

    if let Some(a) = agent {
        println!();
        println!("(Tested with agent: {})", a);
    }
    println!();

    Ok(())
}

/// Run the semantic model daemon (Unix only)
#[cfg(unix)]
pub(crate) fn resolved_daemon_config(
    socket: Option<PathBuf>,
    idle_timeout: Option<u64>,
    max_connections: Option<usize>,
) -> crate::daemon::DaemonConfig {
    use std::time::Duration;

    let mut config = crate::daemon::DaemonConfig::from_env();
    if idle_timeout.is_none() && dotenvy::var("CASS_DAEMON_IDLE_TIMEOUT_SECS").is_err() {
        config.idle_timeout = Duration::from_secs(3600);
    }

    if let Some(socket_path) = socket {
        config.socket_path = socket_path;
    }
    if let Some(idle_timeout_secs) = idle_timeout {
        config.idle_timeout = Duration::from_secs(idle_timeout_secs);
    }
    if let Some(max_connections) = max_connections {
        config.max_connections = max_connections;
    }

    config
}

/// Run the semantic model daemon (Unix only)
#[cfg(unix)]
pub(crate) fn run_daemon(
    socket: Option<PathBuf>,
    idle_timeout: Option<u64>,
    max_connections: Option<usize>,
    data_dir: Option<PathBuf>,
) -> CliResult<()> {
    use crate::daemon::{ModelDaemon, ModelManager};

    let data_dir = data_dir.unwrap_or_else(default_data_dir);
    let socket_was_explicit = socket.is_some() || dotenvy::var("CASS_DAEMON_SOCKET").is_ok();
    let mut config = resolved_daemon_config(socket, idle_timeout, max_connections);
    if !socket_was_explicit {
        config.socket_path = crate::daemon::daemon_socket_path_for_data_dir(&data_dir);
    }
    config.served_generation = crate::daemon::published_lexical_generation(&data_dir);
    config.data_dir = Some(data_dir.clone());

    let models = ModelManager::new(&data_dir);
    let daemon = ModelDaemon::new(config, models);

    daemon.run().map_err(|e| CliError {
        code: 9,
        kind: CliErrorKind::Daemon.kind_str(),
        message: format!("Daemon failed: {e}"),
        hint: None,
        retryable: false,
    })
}
