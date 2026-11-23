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

pub(crate) fn root_structured_triage_rewrite(args: &[String]) -> Option<Vec<String>> {
    let mut rewritten = vec!["triage".to_string()];
    let mut saw_structured_request = false;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--json" | "--robot" => {
                saw_structured_request = true;
                i += 1;
            }
            "--data-dir" | "--stale-threshold" | "--timeout" => {
                let value = args.get(i + 1)?;
                if value.starts_with('-') {
                    return None;
                }
                rewritten.push(args[i].clone());
                rewritten.push(value.clone());
                i += 2;
            }
            arg if arg.starts_with("--data-dir=")
                || arg.starts_with("--stale-threshold=")
                || arg.starts_with("--timeout=") =>
            {
                rewritten.push(arg.to_string());
                i += 1;
            }
            _ => return None,
        }
    }

    if saw_structured_request {
        rewritten.push("--json".to_string());
        Some(rewritten)
    } else {
        None
    }
}

pub(crate) fn command_accepts_leading_structured_flag(command: &str) -> bool {
    matches!(
        command,
        "api-version"
            | "capabilities"
            | "context"
            | "dedup"
            | "diag"
            | "doctor"
            | "expand"
            | "export-html"
            | "health"
            | "selftest"
            | "import"
            | "index"
            | "introspect"
            | "models"
            | "onboarding"
            | "pack"
            | "pages"
            | "resume"
            | "robot-docs"
            | "search"
            | "sessions"
            | "sources"
            | "state"
            | "stats"
            | "status"
            | "storage"
            | "support-bundle"
            | "timeline"
            | "triage"
            | "view"
    )
}

pub(crate) fn data_dir_insertion_index(rest: &[String], command_index: usize) -> Option<usize> {
    let command = rest.get(command_index)?.to_ascii_lowercase();
    match command.as_str() {
        "index" | "search" | "pack" | "stats" | "diag" | "storage" | "dedup" | "status"
        | "triage" | "support-bundle" | "state" | "health" | "onboarding" | "doctor"
        | "context" | "sessions" | "timeline" | "daemon" => Some(command_index + 1),
        "mirror" => rest
            .get(command_index + 1)
            .is_some_and(|action| action.eq_ignore_ascii_case("prune"))
            .then_some(command_index + 2),
        "sources" => rest
            .get(command_index + 1)
            .is_some_and(|action| action.eq_ignore_ascii_case("artifact-manifest"))
            .then_some(command_index + 2),
        "models" => rest
            .get(command_index + 1)
            .is_some_and(|action| {
                ["install", "verify", "backfill", "remove"]
                    .iter()
                    .any(|candidate| action.eq_ignore_ascii_case(candidate))
            })
            .then_some(command_index + 2),
        // `fleet`, `release-verify`, and `upgrade` are removed commands: they
        // never reach this rewrite because `removed_invocation_error` rejects
        // them in `parse_cli` first, so they have no arm here.
        _ => None,
    }
}

pub(crate) fn recover_leading_data_dir_to_subcommand(
    rest: &mut Vec<String>,
    corrections: &mut Vec<String>,
) {
    let mut index = 0;
    let mut leading = Vec::new();
    let mut data_dir_args = Vec::new();

    while index < rest.len() {
        let arg = &rest[index];
        if arg == "--data-dir" {
            let Some(value) = rest.get(index + 1) else {
                return;
            };
            if value.starts_with('-') {
                return;
            }
            data_dir_args.push(arg.clone());
            data_dir_args.push(value.clone());
            index += 2;
            continue;
        }
        if arg.starts_with("--data-dir=") {
            data_dir_args.push(arg.clone());
            index += 1;
            continue;
        }
        if let Some(span) = structured_output_request_span_at(rest, index) {
            leading.extend(rest[index..index + span].iter().cloned());
            index += span;
            continue;
        }
        if arg.starts_with('-') {
            return;
        }

        let Some(insertion_index) = data_dir_insertion_index(rest, index) else {
            return;
        };
        if data_dir_args.is_empty() {
            return;
        }

        let mut normalized = Vec::with_capacity(rest.len());
        normalized.extend(leading);
        normalized.extend(rest[index..insertion_index].iter().cloned());
        normalized.extend(data_dir_args);
        normalized.extend(rest[insertion_index..].iter().cloned());
        *rest = normalized;
        corrections.push("Leading --data-dir moved onto the data-dir-aware subcommand".to_string());
        return;
    }
}

pub(crate) fn move_leading_structured_flag_to_subcommand(rest: &mut Vec<String>) -> bool {
    let mut leading_count = 0;
    while rest
        .get(leading_count)
        .is_some_and(|arg| arg == "--json" || arg == "--robot")
    {
        leading_count += 1;
    }
    let Some(command) = rest.get(leading_count) else {
        return false;
    };
    if leading_count == 0
        || command.starts_with('-')
        || !command_accepts_leading_structured_flag(command)
    {
        return false;
    }

    rest.drain(0..leading_count);
    if !rest.iter().any(|arg| arg == "--json" || arg == "--robot") {
        rest.push("--json".to_string());
    }
    true
}

pub(crate) fn robot_docs_topic_shorthand(arg: &str) -> Option<&'static str> {
    match arg.to_ascii_lowercase().as_str() {
        "commands" | "command" | "cmds" => Some("commands"),
        "env" | "environment" | "vars" | "variables" => Some("env"),
        "paths" | "path" | "dirs" | "directories" => Some("paths"),
        "schemas" | "schema" => Some("schemas"),
        "examples" | "example" => Some("examples"),
        "contracts" | "contract" | "api-contract" | "api_contract" => Some("contracts"),
        "wrap" | "wrapping" | "line-wrap" | "line_wrap" => Some("wrap"),
        "sources" | "source" | "remotes" | "remote" => Some("sources"),
        "doctor" | "diagnostics" | "diag" => Some("doctor"),
        "exit-codes" | "exit_codes" | "exitcodes" | "codes" => Some("exit-codes"),
        "quickstart" | "quick-start" => Some("guide"),
        _ => None,
    }
}

pub(crate) fn recover_robot_docs_topic_shorthands(
    rest: &mut Vec<String>,
    corrections: &mut Vec<String>,
) {
    let leading_structured_count = rest
        .iter()
        .take_while(|arg| matches!(arg.as_str(), "--json" | "--robot"))
        .count();
    let Some(arg) = rest.get(leading_structured_count) else {
        return;
    };
    // Topic aliases that are also real commands must stay commands. Structured
    // command help is handled by `recover_structured_help_requests` instead.
    if is_canonical_top_level_command_arg(arg) {
        return;
    }
    let Some(topic) = robot_docs_topic_shorthand(arg) else {
        return;
    };

    let alias = rest[leading_structured_count].clone();
    rest[leading_structured_count] = "robot-docs".to_string();
    rest.insert(leading_structured_count + 1, topic.to_string());
    corrections.push(format!(
        "'{alias}' → 'robot-docs {topic}' (robot-docs topic shorthand)"
    ));
}

pub(crate) fn structured_output_request_span_at(rest: &[String], index: usize) -> Option<usize> {
    let arg = rest.get(index)?;
    if matches!(arg.as_str(), "--json" | "--robot") {
        return Some(1);
    }
    if arg == "--robot-format" {
        return rest
            .get(index + 1)
            .is_some_and(|value| is_robot_format_alias_value(value))
            .then_some(2);
    }
    if let Some(value) = arg.strip_prefix("--robot-format=")
        && is_robot_format_alias_value(value)
    {
        return Some(1);
    }
    parse_format_alias_at(rest, index).map(|(consumed, _, _)| consumed)
}

pub(crate) fn is_canonical_top_level_command_arg(arg: &str) -> bool {
    CANONICAL_TOP_LEVEL_COMMANDS
        .iter()
        .any(|command| arg.eq_ignore_ascii_case(command))
}

pub(crate) fn contains_structured_output_request(rest: &[String]) -> bool {
    (0..rest.len()).any(|index| structured_output_request_span_at(rest, index).is_some())
}

pub(crate) fn prefix_is_structured_output_requests(rest: &[String], end: usize) -> bool {
    let mut index = 0;
    while index < end {
        let Some(span) = structured_output_request_span_at(rest, index) else {
            return false;
        };
        index += span;
    }
    true
}

pub(crate) fn structured_help_request_index(rest: &[String]) -> Option<usize> {
    rest.iter()
        .enumerate()
        .find_map(|(index, arg)| match arg.as_str() {
            "--help" | "-h" => Some(index),
            "help" if prefix_is_structured_output_requests(rest, index) => Some(index),
            _ => None,
        })
}

pub(crate) fn first_command_arg_after_structured_prefix(rest: &[String]) -> Option<&str> {
    let mut index = 0;
    while index < rest.len() {
        if let Some(span) = structured_output_request_span_at(rest, index) {
            index += span;
            continue;
        }
        let arg = &rest[index];
        if !arg.starts_with('-') {
            return Some(arg);
        }
        index += 1;
    }
    None
}

pub(crate) fn help_request_target(rest: &[String], help_index: usize) -> Option<&str> {
    if rest.get(help_index).is_some_and(|arg| arg == "help") {
        let mut index = help_index + 1;
        while index < rest.len() {
            if let Some(span) = structured_output_request_span_at(rest, index) {
                index += span;
                continue;
            }
            let arg = &rest[index];
            if !arg.starts_with('-') {
                return Some(arg);
            }
            index += 1;
        }
        return None;
    }

    if help_index > 0 {
        return first_command_arg_after_structured_prefix(rest);
    }

    None
}

pub(crate) fn recover_structured_help_requests(
    rest: &mut Vec<String>,
    corrections: &mut Vec<String>,
) {
    let Some(help_index) = structured_help_request_index(rest) else {
        return;
    };
    if !contains_structured_output_request(rest) {
        return;
    }

    let topic = help_request_target(rest, help_index)
        .and_then(robot_docs_topic_shorthand)
        .unwrap_or_else(|| {
            if help_request_target(rest, help_index).is_some() {
                "commands"
            } else {
                "guide"
            }
        });
    let original = rest.join(" ");
    *rest = vec!["robot-docs".to_string(), topic.to_string()];
    corrections.push(format!(
        "'{original}' → 'robot-docs {topic}' (structured help request)"
    ));
}

pub(crate) fn is_current_session_shorthand(arg: &str) -> bool {
    matches!(
        arg.to_ascii_lowercase().as_str(),
        "current" | "current-session" | "current_session" | "session-current" | "session_current"
    )
}

pub(crate) fn has_current_session_flag(rest: &[String]) -> bool {
    rest.iter()
        .any(|arg| arg == "--current" || arg.starts_with("--current="))
}

pub(crate) fn recover_current_session_shorthands(
    rest: &mut Vec<String>,
    corrections: &mut Vec<String>,
) {
    let leading_structured_count = rest
        .iter()
        .take_while(|arg| matches!(arg.as_str(), "--json" | "--robot"))
        .count();

    if rest
        .get(leading_structured_count)
        .is_some_and(|arg| is_current_session_shorthand(arg))
    {
        let alias = rest[leading_structured_count].clone();
        rest[leading_structured_count] = "sessions".to_string();
        if !has_current_session_flag(rest) {
            rest.insert(leading_structured_count + 1, "--current".to_string());
        }
        corrections.push(format!(
            "'{alias}' → 'sessions --current' (current-session shorthand)"
        ));
        return;
    }

    if !matches!(rest.first().map(String::as_str), Some("sessions")) {
        return;
    }
    if !rest
        .get(1)
        .is_some_and(|arg| is_current_session_shorthand(arg))
    {
        return;
    }

    let alias = rest.remove(1);
    if !has_current_session_flag(rest) {
        rest.insert(1, "--current".to_string());
    }
    corrections.push(format!(
        "'sessions {alias}' → 'sessions --current' (current-session shorthand)"
    ));
}

pub(crate) fn take_named_value(rest: &mut Vec<String>, names: &[&str]) -> Option<(String, String)> {
    for index in 1..rest.len() {
        let arg = rest[index].clone();
        for name in names {
            if arg == *name {
                let value = rest.get(index + 1)?;
                if value.starts_with('-') {
                    return None;
                }
                let value = value.clone();
                rest.drain(index..=index + 1);
                return Some(((*name).to_string(), value));
            }

            let prefix = format!("{name}=");
            if let Some(value) = arg.strip_prefix(&prefix)
                && !value.is_empty()
            {
                rest.remove(index);
                return Some(((*name).to_string(), value.to_string()));
            }
        }
    }
    None
}

pub(crate) fn take_assignment_value(
    rest: &mut Vec<String>,
    names: &[&str],
) -> Option<(String, String)> {
    for index in 1..rest.len() {
        let arg = rest[index].clone();
        let Some((key, value)) = arg.split_once('=') else {
            continue;
        };
        if value.is_empty() || !names.contains(&key) {
            continue;
        }

        rest.remove(index);
        return Some((key.to_string(), value.to_string()));
    }
    None
}

pub(crate) fn rewrite_named_assignment_positional(
    rest: &mut [String],
    names: &[&str],
) -> Option<(String, String)> {
    for slot in rest.iter_mut().skip(1) {
        let arg = slot.clone();
        for name in names {
            let prefix = format!("{name}=");
            if let Some(value) = arg.strip_prefix(&prefix)
                && !value.is_empty()
            {
                *slot = value.to_string();
                return Some(((*name).to_string(), value.to_string()));
            }
        }
    }
    None
}

pub(crate) fn split_path_line_assignment(arg: &str) -> Option<(String, usize)> {
    let (path, line) = arg.rsplit_once(':')?;
    if path.is_empty() {
        return None;
    }
    let line = line.parse::<usize>().ok().filter(|line| *line > 0)?;
    Some((path.to_string(), line))
}

pub(crate) fn has_line_arg(rest: &[String]) -> bool {
    // An explicit canonical selector must also suppress path:line recovery.
    has_option_alias(
        rest,
        &[
            "--line",
            "-n",
            "--line-number",
            "--line_number",
            "--message-index",
            "--message_index",
        ],
    )
}

pub(crate) fn has_option_alias(rest: &[String], aliases: &[&str]) -> bool {
    rest.iter().any(|arg| {
        aliases.iter().any(|alias| {
            arg == alias
                || arg
                    .strip_prefix(alias)
                    .is_some_and(|suffix| suffix.starts_with('='))
        })
    })
}

pub(crate) struct AssignmentOption {
    pub(crate) flag: &'static str,
    pub(crate) aliases: &'static [&'static str],
    pub(crate) repeatable: bool,
}

pub(crate) fn assignment_option_for_command(command: &str, key: &str) -> Option<AssignmentOption> {
    match command {
        "search" => match key {
            "agent" | "provider" | "tool" | "connector" | "agent-type" | "agent_type" => {
                Some(AssignmentOption {
                    flag: "--agent",
                    aliases: &["--agent"],
                    repeatable: true,
                })
            }
            "workspace" => Some(AssignmentOption {
                flag: "--workspace",
                aliases: &["--workspace"],
                repeatable: true,
            }),
            "fields" => Some(AssignmentOption {
                flag: "--fields",
                aliases: &["--fields"],
                repeatable: false,
            }),
            "max-content-length" | "max_content_length" => Some(AssignmentOption {
                flag: "--max-content-length",
                aliases: &["--max-content-length"],
                repeatable: false,
            }),
            "max-tokens" | "max_tokens" => Some(AssignmentOption {
                flag: "--max-tokens",
                aliases: &["--max-tokens"],
                repeatable: false,
            }),
            "request-id" | "request_id" => Some(AssignmentOption {
                flag: "--request-id",
                aliases: &["--request-id"],
                repeatable: false,
            }),
            "cursor" => Some(AssignmentOption {
                flag: "--cursor",
                aliases: &["--cursor"],
                repeatable: false,
            }),
            "data-dir" | "data_dir" => Some(AssignmentOption {
                flag: "--data-dir",
                aliases: &["--data-dir"],
                repeatable: false,
            }),
            "offset" => Some(AssignmentOption {
                flag: "--offset",
                aliases: &["--offset"],
                repeatable: false,
            }),
            "days" => Some(AssignmentOption {
                flag: "--days",
                aliases: &["--days"],
                repeatable: false,
            }),
            "since" => Some(AssignmentOption {
                flag: "--since",
                aliases: &["--since"],
                repeatable: false,
            }),
            "until" => Some(AssignmentOption {
                flag: "--until",
                aliases: &["--until"],
                repeatable: false,
            }),
            "display" => Some(AssignmentOption {
                flag: "--display",
                aliases: &["--display"],
                repeatable: false,
            }),
            "mode" => Some(AssignmentOption {
                flag: "--mode",
                aliases: &["--mode"],
                repeatable: false,
            }),
            _ => None,
        },
        "pack" => match key {
            "agent" | "provider" | "tool" | "connector" | "agent-type" | "agent_type" => {
                Some(AssignmentOption {
                    flag: "--agent",
                    aliases: &["--agent"],
                    repeatable: true,
                })
            }
            "workspace" => Some(AssignmentOption {
                flag: "--workspace",
                aliases: &["--workspace"],
                repeatable: true,
            }),
            "fields" | "field-mask" | "field_mask" => Some(AssignmentOption {
                flag: "--fields",
                aliases: &["--fields", "--field-mask"],
                repeatable: false,
            }),
            "max-tokens" | "max_tokens" => Some(AssignmentOption {
                flag: "--max-tokens",
                aliases: &["--max-tokens"],
                repeatable: false,
            }),
            "max-sessions" | "max_sessions" => Some(AssignmentOption {
                flag: "--max-sessions",
                aliases: &["--max-sessions"],
                repeatable: false,
            }),
            "max-evidence" | "max_evidence" => Some(AssignmentOption {
                flag: "--max-evidence",
                aliases: &["--max-evidence"],
                repeatable: false,
            }),
            "context-lines" | "context_lines" => Some(AssignmentOption {
                flag: "--context-lines",
                aliases: &["--context-lines"],
                repeatable: false,
            }),
            "max-excerpt-chars" | "max_excerpt_chars" => Some(AssignmentOption {
                flag: "--max-excerpt-chars",
                aliases: &["--max-excerpt-chars"],
                repeatable: false,
            }),
            "request-id" | "request_id" => Some(AssignmentOption {
                flag: "--request-id",
                aliases: &["--request-id"],
                repeatable: false,
            }),
            "data-dir" | "data_dir" => Some(AssignmentOption {
                flag: "--data-dir",
                aliases: &["--data-dir"],
                repeatable: false,
            }),
            "days" => Some(AssignmentOption {
                flag: "--days",
                aliases: &["--days"],
                repeatable: false,
            }),
            "since" => Some(AssignmentOption {
                flag: "--since",
                aliases: &["--since"],
                repeatable: false,
            }),
            "until" => Some(AssignmentOption {
                flag: "--until",
                aliases: &["--until"],
                repeatable: false,
            }),
            "source" => Some(AssignmentOption {
                flag: "--source",
                aliases: &["--source"],
                repeatable: false,
            }),
            "sessions-from" | "sessions_from" => Some(AssignmentOption {
                flag: "--sessions-from",
                aliases: &["--sessions-from"],
                repeatable: false,
            }),
            "mode" => Some(AssignmentOption {
                flag: "--mode",
                aliases: &["--mode"],
                repeatable: false,
            }),
            "freshness-policy" | "freshness_policy" => Some(AssignmentOption {
                flag: "--freshness-policy",
                aliases: &["--freshness-policy"],
                repeatable: false,
            }),
            "freshness-window-seconds" | "freshness_window_seconds" => Some(AssignmentOption {
                flag: "--freshness-window-seconds",
                aliases: &["--freshness-window-seconds"],
                repeatable: false,
            }),
            "timeout" => Some(AssignmentOption {
                flag: "--timeout",
                aliases: &["--timeout"],
                repeatable: false,
            }),
            _ => None,
        },
        "sessions" => match key {
            "workspace" => Some(AssignmentOption {
                flag: "--workspace",
                aliases: &["--workspace"],
                repeatable: false,
            }),
            "data-dir" | "data_dir" => Some(AssignmentOption {
                flag: "--data-dir",
                aliases: &["--data-dir"],
                repeatable: false,
            }),
            _ => None,
        },
        "context" | "expand" | "export" | "export-html" | "view" => match key {
            "source" | "source-id" | "source_id" => Some(AssignmentOption {
                flag: "--source",
                aliases: &["--source", "--source-id"],
                repeatable: false,
            }),
            _ => None,
        },
        "timeline" => match key {
            "agent" | "provider" | "tool" | "connector" | "agent-type" | "agent_type" => {
                Some(AssignmentOption {
                    flag: "--agent",
                    aliases: &["--agent"],
                    repeatable: true,
                })
            }
            "data-dir" | "data_dir" => Some(AssignmentOption {
                flag: "--data-dir",
                aliases: &["--data-dir"],
                repeatable: false,
            }),
            "since" => Some(AssignmentOption {
                flag: "--since",
                aliases: &["--since"],
                repeatable: false,
            }),
            "until" => Some(AssignmentOption {
                flag: "--until",
                aliases: &["--until"],
                repeatable: false,
            }),
            "group-by" | "group_by" => Some(AssignmentOption {
                flag: "--group-by",
                aliases: &["--group-by"],
                repeatable: false,
            }),
            "source" => Some(AssignmentOption {
                flag: "--source",
                aliases: &["--source"],
                repeatable: false,
            }),
            _ => None,
        },
        _ => None,
    }
}

pub(crate) fn command_accepts_time_assignment(command: &str) -> bool {
    matches!(command, "search" | "pack" | "timeline")
}

pub(crate) fn compact_relative_time_value(value: &str) -> bool {
    let digits_len = value
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .map(char::len_utf8)
        .sum::<usize>();
    if digits_len == 0 {
        return false;
    }

    matches!(
        value[digits_len..].trim(),
        "m" | "min"
            | "mins"
            | "minute"
            | "minutes"
            | "h"
            | "hr"
            | "hrs"
            | "hour"
            | "hours"
            | "d"
            | "day"
            | "days"
            | "w"
            | "week"
            | "weeks"
    )
}

pub(crate) fn normalize_past_time_window_value(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.starts_with('-')
        || trimmed.ends_with("ago")
        || matches!(trimmed, "now" | "today" | "yesterday")
    {
        return trimmed.to_string();
    }
    if trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return format!("-{trimmed}d");
    }
    if compact_relative_time_value(trimmed) {
        return format!("-{trimmed}");
    }
    trimmed.to_string()
}

pub(crate) fn time_assignment_option_for_command(
    command: &str,
    key: &str,
    value: &str,
) -> Option<(&'static str, &'static [&'static str], String)> {
    if !command_accepts_time_assignment(command) {
        return None;
    }

    match key {
        "last" | "recent" | "past" | "within" => Some((
            "--since",
            &["--since"],
            normalize_past_time_window_value(value),
        )),
        "from" | "after" | "start" | "start-date" | "start_date" => {
            Some(("--since", &["--since"], value.trim().to_string()))
        }
        "to" | "before" | "end" | "end-date" | "end_date" => {
            Some(("--until", &["--until"], value.trim().to_string()))
        }
        _ => None,
    }
}

pub(crate) fn take_time_alias_flag(
    rest: &mut Vec<String>,
) -> Option<(String, &'static str, String)> {
    for index in 1..rest.len() {
        let arg = rest[index].clone();
        if !arg.starts_with("--") {
            continue;
        }

        let (name, value, consumed) = if let Some((name, value)) = arg.split_once('=') {
            let Some(name) = name.strip_prefix("--") else {
                continue;
            };
            if value.is_empty() {
                continue;
            }
            (name, value.to_string(), 1)
        } else {
            let Some(name) = arg.strip_prefix("--") else {
                continue;
            };
            let Some(value) = rest.get(index + 1).cloned() else {
                continue;
            };
            if value.starts_with("--") {
                continue;
            }
            (name, value, 2)
        };

        let command = rest.first().map(String::as_str).unwrap_or_default();
        let Some((flag, _, rewritten_value)) =
            time_assignment_option_for_command(command, name, &value)
        else {
            continue;
        };
        rest.splice(
            index..index + consumed,
            [flag.to_string(), rewritten_value.clone()],
        );
        return Some((format!("--{name}"), flag, rewritten_value));
    }
    None
}

pub(crate) fn recover_time_alias_flags(rest: &mut Vec<String>, corrections: &mut Vec<String>) {
    while let Some((alias, flag, value)) = take_time_alias_flag(rest) {
        let command = rest
            .first()
            .cloned()
            .unwrap_or_else(|| "command".to_string());
        corrections.push(format!(
            "'{command} {alias} <value>' → '{command} {flag} {value}' (time-window alias)"
        ));
    }
}

pub(crate) fn command_accepts_agent_filter_alias(command: &str) -> bool {
    matches!(command, "search" | "pack" | "timeline")
}

pub(crate) fn take_agent_filter_alias_flag(rest: &mut Vec<String>) -> Option<(String, String)> {
    let command = rest.first().map(String::as_str).unwrap_or_default();
    if !command_accepts_agent_filter_alias(command) {
        return None;
    }

    for index in 1..rest.len() {
        let arg = rest[index].clone();
        if !arg.starts_with("--") {
            continue;
        }

        let (name, value, consumed) = if let Some((name, value)) = arg.split_once('=') {
            let Some(name) = name.strip_prefix("--") else {
                continue;
            };
            if value.is_empty() {
                continue;
            }
            (name, value.to_string(), 1)
        } else {
            let Some(name) = arg.strip_prefix("--") else {
                continue;
            };
            let Some(value) = rest.get(index + 1).cloned() else {
                continue;
            };
            if value.starts_with("--") {
                continue;
            }
            (name, value, 2)
        };

        if !matches!(
            name,
            "provider" | "tool" | "connector" | "agent-type" | "agent_type"
        ) {
            continue;
        }

        rest.splice(
            index..index + consumed,
            ["--agent".to_string(), value.clone()],
        );
        return Some((format!("--{name}"), value));
    }
    None
}

pub(crate) fn recover_agent_filter_alias_flags(
    rest: &mut Vec<String>,
    corrections: &mut Vec<String>,
) {
    while let Some((alias, value)) = take_agent_filter_alias_flag(rest) {
        let command = rest
            .first()
            .cloned()
            .unwrap_or_else(|| "command".to_string());
        corrections.push(format!(
            "'{command} {alias} <value>' → '{command} --agent {value}' (provider alias)"
        ));
    }
}

pub(crate) fn recover_key_value_option_assignments(
    rest: &mut [String],
    corrections: &mut Vec<String>,
) {
    let Some(command) = rest.first().cloned() else {
        return;
    };

    for index in 1..rest.len() {
        let arg = rest[index].clone();
        let Some((key, value)) = arg.split_once('=') else {
            continue;
        };
        if value.is_empty() {
            continue;
        }
        if let Some((flag, aliases, rewritten_value)) =
            time_assignment_option_for_command(&command, key, value)
        {
            if !has_option_alias(rest, aliases) {
                rest[index] = format!("{flag}={rewritten_value}");
                corrections.push(format!(
                    "'{command} {key}=<value>' → '{command} {flag} <value>' (time-window assignment)"
                ));
            }
            continue;
        }
        let Some(option) = assignment_option_for_command(&command, key) else {
            continue;
        };
        if !option.repeatable && has_option_alias(rest, option.aliases) {
            continue;
        }

        rest[index] = format!("{}={value}", option.flag);
        corrections.push(format!(
            "'{command} {key}=<value>' → '{command} {} <value>' (known option assignment)",
            option.flag
        ));
    }
}
