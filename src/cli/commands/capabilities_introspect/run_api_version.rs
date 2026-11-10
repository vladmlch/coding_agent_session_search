use crate::cli::args::Cli;
use crate::cli::args_sub::RobotFormat;
use crate::cli::commands::capabilities_introspect::capabilitiesresponse::ArgumentSchema;
use crate::cli::commands::capabilities_introspect::capabilitiesresponse::CommandSchema;
use crate::cli::commands::search::fieldbudgets::robot_format_from_env;
use crate::cli::commands::search::toon_encode_options_from_env::output_structured_value;
use crate::cli::contract_version::CONTRACT_VERSION;
use crate::cli::error::CliResult;
use clap::{Arg, ArgAction, Command, CommandFactory, ValueHint};

/// Show API and contract versions (robot-friendly)
pub(crate) fn run_api_version(output_format: Option<RobotFormat>) -> CliResult<()> {
    let payload = serde_json::json!({
        "crate_version": env!("CARGO_PKG_VERSION"),
        "build_commit": option_env!("CASS_BUILD_COMMIT").unwrap_or("unknown"),
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
