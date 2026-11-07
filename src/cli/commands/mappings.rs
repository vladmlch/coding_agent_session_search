use crate::cli::args::Cli;
use crate::cli::args_sub::{MappingsAction, RobotFormat};
use crate::cli::commands::search::fieldbudgets::robot_format_from_env;
use crate::cli::error::{CliError, CliResult};
use crate::cli::output::describe_command::resolve_subcommand_structured_format;
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;

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
