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

pub(crate) fn bare_option_pair_start_index(command: &str) -> Option<usize> {
    match command {
        // Require at least one query token before interpreting bare words as
        // filters. Otherwise `cass search provider codex --json` would lose a
        // plausible query instead of producing a useful usage error.
        "search" | "pack" => Some(2),
        "sessions" | "timeline" => Some(1),
        _ => None,
    }
}

pub(crate) fn recover_bare_option_value_pairs(
    rest: &mut Vec<String>,
    corrections: &mut Vec<String>,
) {
    let Some(command) = rest.first().cloned() else {
        return;
    };
    let Some(start_index) = bare_option_pair_start_index(&command) else {
        return;
    };

    let mut index = start_index;
    while index + 1 < rest.len() {
        let key = rest[index].clone();
        if key.starts_with('-') || key.contains('=') {
            index += 1;
            continue;
        }
        let value = rest[index + 1].clone();
        if value.starts_with("--") {
            index += 1;
            continue;
        }

        let key_lower = key.to_ascii_lowercase();
        if let Some((flag, aliases, rewritten_value)) =
            time_assignment_option_for_command(&command, &key_lower, &value)
        {
            if !has_option_alias(rest, aliases) {
                rest.splice(
                    index..index + 2,
                    [flag.to_string(), rewritten_value.clone()],
                );
                corrections.push(format!(
                    "'{command} {key} <value>' → '{command} {flag} {rewritten_value}' (bare time-window filter)"
                ));
                index += 2;
                continue;
            }
        } else if command_accepts_limit_alias(rest) && is_result_count_assignment_key(&key_lower) {
            if !has_canonical_limit(rest) {
                rest.splice(index..index + 2, ["--limit".to_string(), value.clone()]);
                corrections.push(format!(
                    "'{command} {key} <value>' → '{command} --limit {value}' (bare result-count filter)"
                ));
                index += 2;
                continue;
            }
        } else if let Some(option) = assignment_option_for_command(&command, &key_lower)
            && (option.repeatable || !has_option_alias(rest, option.aliases))
        {
            rest.splice(index..index + 2, [option.flag.to_string(), value.clone()]);
            corrections.push(format!(
                "'{command} {key} <value>' → '{command} {} {value}' (bare option filter)",
                option.flag
            ));
            index += 2;
            continue;
        }

        index += 1;
    }
}

pub(crate) fn search_like_option_value_count(command: &str, arg: &str) -> Option<usize> {
    let name = arg.strip_prefix("--")?;
    let (name, inline_value) = name
        .split_once('=')
        .map_or((name, false), |(name, _)| (name, true));
    let name = name.to_ascii_lowercase();

    let takes_value = matches!(
        name.as_str(),
        "agent"
            | "workspace"
            | "limit"
            | "offset"
            | "fields"
            | "max-content-length"
            | "max_content_length"
            | "max-tokens"
            | "max_tokens"
            | "request-id"
            | "request_id"
            | "cursor"
            | "display"
            | "data-dir"
            | "data_dir"
            | "days"
            | "since"
            | "until"
            | "aggregate"
            | "timeout"
            | "source"
            | "sessions-from"
            | "sessions_from"
            | "mode"
            | "model"
            | "reranker"
            | "robot-format"
            | "robot_format"
    ) || matches!(
        (command, name.as_str()),
        (
            "pack",
            "field-mask"
                | "field_mask"
                | "max-sessions"
                | "max_sessions"
                | "max-evidence"
                | "max_evidence"
                | "context-lines"
                | "context_lines"
                | "max-excerpt-chars"
                | "max_excerpt_chars"
                | "freshness-policy"
                | "freshness_policy"
                | "freshness-window-seconds"
                | "freshness_window_seconds"
        )
    );

    if takes_value {
        return Some(if inline_value { 1 } else { 2 });
    }

    let is_bool = matches!(
        name.as_str(),
        "json"
            | "robot"
            | "robot-meta"
            | "robot_meta"
            | "today"
            | "yesterday"
            | "week"
            | "explain"
            | "dry-run"
            | "dry_run"
            | "highlight"
            | "approximate"
            | "rerank"
            | "daemon"
            | "no-daemon"
            | "no_daemon"
            | "two-tier"
            | "two_tier"
            | "fast-only"
            | "fast_only"
            | "quality-only"
            | "quality_only"
            | "refresh"
            | "catch-up"
            | "catch_up"
    ) || matches!(
        (command, name.as_str()),
        (
            "pack",
            "require-evidence"
                | "require_evidence"
                | "explain-selection"
                | "explain_selection"
                | "include-skill-content"
                | "include_skill_content"
        )
    );

    is_bool.then_some(1)
}

pub(crate) fn recover_query_after_leading_search_options(
    rest: &mut Vec<String>,
    corrections: &mut Vec<String>,
) {
    let Some(command) = rest.first().cloned() else {
        return;
    };
    if !matches!(command.as_str(), "search" | "pack") || rest.len() < 3 {
        return;
    }

    let mut index = 1;
    let mut saw_leading_option = false;
    while index < rest.len() {
        let Some(arg) = rest.get(index) else {
            return;
        };
        if !arg.starts_with('-') {
            break;
        }
        let Some(width) = search_like_option_value_count(&command, arg) else {
            return;
        };
        if width == 2
            && rest
                .get(index + 1)
                .is_none_or(|value| value.starts_with("--"))
        {
            return;
        }
        saw_leading_option = true;
        index += width;
    }

    if !saw_leading_option || index >= rest.len() {
        return;
    }

    let query_start = index;
    let mut query_end = query_start;
    while let Some(arg) = rest.get(query_end) {
        if arg.starts_with('-') || is_query_boundary_assignment(&command, arg) {
            break;
        }
        query_end += 1;
    }
    if query_end <= query_start {
        return;
    }

    let query = rest[query_start..query_end].join(" ");
    rest.drain(query_start..query_end);
    rest.insert(1, query.clone());
    corrections.push(format!(
        "'{command} <filters> {query}' → '{command} \"{query}\" <filters>' (query moved before leading filters)"
    ));
}

pub(crate) fn recover_path_line_positionals(rest: &mut Vec<String>, corrections: &mut Vec<String>) {
    let Some(command) = rest.first().cloned() else {
        return;
    };
    if !matches!(command.as_str(), "expand" | "view") || has_line_arg(rest) {
        return;
    }
    let Some(raw_path) = rest.get(1).cloned() else {
        return;
    };
    let Some((path, line)) = split_path_line_assignment(&raw_path) else {
        return;
    };

    rest[1] = path;
    rest.push("--line".to_string());
    rest.push(line.to_string());
    corrections.push(format!(
        "'{command} {raw_path}' → '{command} <path> --line {line}' (path:line shorthand)"
    ));
}

pub(crate) fn recover_named_required_positionals(
    rest: &mut Vec<String>,
    corrections: &mut Vec<String>,
) {
    let Some(command) = rest.first().cloned() else {
        return;
    };

    match command.as_str() {
        "search" | "pack" => {
            if let Some((flag, value)) =
                take_named_value(rest, &["--query", "--q", "--text", "--pattern"])
            {
                rest.insert(1, value);
                corrections.push(format!(
                    "'{command} {flag} <value>' → '{command} <query>' (query is positional)"
                ));
            } else if let Some((name, _value)) =
                rewrite_named_assignment_positional(rest, &["query", "q", "text", "pattern"])
            {
                corrections.push(format!(
                    "'{command} {name}=<value>' → '{command} <value>' (query is positional)"
                ));
            }
        }
        "context" | "expand" | "export" | "export-html" | "resume" | "view" => {
            if let Some((flag, value)) =
                take_named_value(rest, &["--path", "--source-path", "--file", "--session"])
            {
                rest.insert(1, value);
                corrections.push(format!(
                    "'{command} {flag} <value>' → '{command} <path>' (path is positional)"
                ));
            } else if let Some((name, _value)) = rewrite_named_assignment_positional(
                rest,
                &["path", "source-path", "source_path", "file", "session"],
            ) {
                corrections.push(format!(
                    "'{command} {name}=<value>' → '{command} <value>' (path is positional)"
                ));
            }
        }
        _ => {}
    }
}

pub(crate) fn drilldown_assignment_flag(
    command: &str,
    key: &str,
) -> Option<(&'static str, &'static [&'static str])> {
    if !matches!(command, "expand" | "view") {
        return None;
    }

    match key {
        "line" | "n" => Some((
            "--line",
            &["--line", "--line-number", "--line_number", "-n"],
        )),
        // Pasted search fields are canonical ordinals, never raw file lines.
        "line-number" | "line_number" | "message-index" | "message_index" => {
            Some(("--message-index", &["--message-index", "--message_index"]))
        }
        "conversation-id" | "conversation_id" => Some((
            "--conversation-id",
            &["--conversation-id", "--conversation_id"],
        )),
        "context" | "c" => Some(("--context", &["--context", "-C"])),
        _ => None,
    }
}

pub(crate) fn recover_drilldown_assignment_flags(
    rest: &mut Vec<String>,
    corrections: &mut Vec<String>,
) {
    let Some(command) = rest.first().cloned() else {
        return;
    };

    let mut index = 1;
    while index < rest.len() {
        let arg = rest[index].clone();
        let Some((key, value)) = arg.split_once('=') else {
            index += 1;
            continue;
        };
        if value.is_empty() {
            index += 1;
            continue;
        }
        let Some((flag, aliases)) = drilldown_assignment_flag(&command, key) else {
            index += 1;
            continue;
        };
        if has_option_alias(rest, aliases) {
            index += 1;
            continue;
        }

        rest.splice(index..=index, [flag.to_string(), value.to_string()]);
        corrections.push(format!(
            "'{command} {key}=<value>' → '{command} {flag} <value>' (drill-down option)"
        ));
        index += 2;
    }
}

pub(crate) fn is_robot_format_alias_value(value: &str) -> bool {
    matches!(value, "json" | "jsonl" | "compact" | "sessions" | "toon")
}

pub(crate) fn can_recover_format_alias_for_command(command: &str) -> bool {
    command != "export" && command_accepts_leading_structured_flag(command)
}

pub(crate) fn parse_format_alias_at(
    rest: &[String],
    index: usize,
) -> Option<(usize, String, &'static str)> {
    let arg = rest.get(index)?;
    for alias in ["--format", "--output", "--output-format"] {
        if arg == alias {
            let value = rest.get(index + 1)?;
            let value = value.to_ascii_lowercase();
            if is_robot_format_alias_value(&value) {
                return Some((2, value, alias));
            }
        }
        if let Some(value) = arg
            .strip_prefix(alias)
            .and_then(|rest| rest.strip_prefix('='))
        {
            let value = value.to_ascii_lowercase();
            if is_robot_format_alias_value(&value) {
                return Some((1, value, alias));
            }
        }
    }
    None
}

pub(crate) fn recover_structured_format_aliases(
    rest: &mut Vec<String>,
    corrections: &mut Vec<String>,
) {
    if let Some((consumed, format, alias)) = parse_format_alias_at(rest, 0) {
        match rest.get(consumed).cloned() {
            Some(command) if can_recover_format_alias_for_command(&command) => {
                rest.drain(0..consumed);
                rest.push("--robot-format".to_string());
                rest.push(format.clone());
                corrections.push(format!(
                    "Leading '{alias} {format}' moved to '{command} --robot-format {format}' (structured output format)"
                ));
                return;
            }
            Some(command) if command.starts_with('-') => {
                let mut triage_tail = vec!["--json".to_string()];
                triage_tail.extend(rest[consumed..].iter().cloned());
                if let Some(mut rewritten) = root_structured_triage_rewrite(&triage_tail) {
                    if rewritten.last().is_some_and(|arg| arg == "--json") {
                        rewritten.pop();
                    }
                    rewritten.push("--robot-format".to_string());
                    rewritten.push(format.clone());
                    *rest = rewritten;
                    corrections.push(format!(
                        "Top-level '{alias} {format}' → 'triage --robot-format {format}' (read-only agent preflight)"
                    ));
                    return;
                }
            }
            None => {
                rest.drain(0..consumed);
                rest.push("triage".to_string());
                rest.push("--robot-format".to_string());
                rest.push(format.clone());
                corrections.push(format!(
                    "Top-level '{alias} {format}' → 'triage --robot-format {format}' (read-only agent preflight)"
                ));
                return;
            }
            _ => {}
        }
    }

    let Some(command) = rest.first().cloned() else {
        return;
    };
    if !can_recover_format_alias_for_command(&command) {
        return;
    }

    for index in 1..rest.len() {
        if let Some((consumed, format, alias)) = parse_format_alias_at(rest, index) {
            rest.splice(
                index..index + consumed,
                ["--robot-format".to_string(), format.clone()],
            );
            corrections.push(format!(
                "'{command} {alias} {format}' → '{command} --robot-format {format}' (structured output format)"
            ));
            return;
        }
    }
}

pub(crate) fn command_accepts_limit_alias(rest: &[String]) -> bool {
    matches!(
        rest.first().map(String::as_str),
        Some("context" | "pack" | "search" | "sessions")
    )
}

pub(crate) fn has_canonical_limit(rest: &[String]) -> bool {
    rest.iter()
        .any(|arg| arg == "--limit" || arg.starts_with("--limit="))
}

pub(crate) fn recover_limit_aliases(rest: &mut Vec<String>, corrections: &mut Vec<String>) {
    if !command_accepts_limit_alias(rest) || has_canonical_limit(rest) {
        return;
    }

    let named_limit = take_named_value(
        rest,
        &[
            "--max-results",
            "--num-results",
            "--results",
            "--count",
            "--top-k",
            "--topk",
            "--top_k",
            "--n",
            "-n",
        ],
    );
    let assignment_limit = || {
        take_assignment_value(
            rest,
            &[
                "limit",
                "max-results",
                "max_results",
                "num-results",
                "num_results",
                "results",
                "count",
                "top-k",
                "topk",
                "top_k",
                "n",
            ],
        )
    };

    let Some((flag, value)) = named_limit.or_else(assignment_limit) else {
        return;
    };

    let command = rest
        .first()
        .cloned()
        .unwrap_or_else(|| "command".to_string());
    rest.push("--limit".to_string());
    rest.push(value.clone());
    corrections.push(format!(
        "'{command} {flag} <value>' → '{command} --limit {value}' (result-count alias)"
    ));
}

pub(crate) fn has_explicit_structured_output_request(rest: &[String]) -> bool {
    rest.iter().enumerate().any(|(index, arg)| {
        matches!(arg.as_str(), "--json" | "--robot")
            || arg == "--robot-format"
            || arg.starts_with("--robot-format=")
            || parse_format_alias_at(rest, index).is_some()
    })
}

pub(crate) fn is_query_assignment(arg: &str) -> bool {
    arg.split_once('=')
        .is_some_and(|(key, value)| !value.is_empty() && matches!(key, "query" | "q"))
}

pub(crate) fn is_result_count_assignment_key(key: &str) -> bool {
    matches!(
        key,
        "limit"
            | "max-results"
            | "max_results"
            | "num-results"
            | "num_results"
            | "results"
            | "count"
            | "top-k"
            | "topk"
            | "top_k"
            | "n"
    )
}

pub(crate) fn is_query_boundary_assignment(command: &str, arg: &str) -> bool {
    let Some((key, value)) = arg.split_once('=') else {
        return false;
    };
    if value.is_empty() {
        return false;
    }

    let key = key.to_ascii_lowercase();
    assignment_option_for_command(command, &key).is_some()
        || time_assignment_option_for_command(command, &key, value).is_some()
        || is_result_count_assignment_key(&key)
}

pub(crate) fn looks_like_top_level_command_or_typo(arg: &str) -> bool {
    let lower = arg.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "help" | "--help" | "-h" | "--version" | "-v"
    ) || CANONICAL_TOP_LEVEL_COMMANDS.contains(&lower.as_str())
        || closest_top_level_command(&lower).is_some()
}

pub(crate) fn arg_marks_implicit_pack_intent(arg: &str) -> bool {
    if let Some(flag) = arg.strip_prefix("--") {
        let name = flag.split_once('=').map_or(flag, |(name, _)| name);
        return matches!(
            name,
            "field-mask"
                | "max-sessions"
                | "max-evidence"
                | "max-excerpt-chars"
                | "freshness-policy"
                | "freshness-window-seconds"
                | "include-skill-content"
                | "require-evidence"
                | "explain-selection"
        );
    }

    let Some((key, value)) = arg.split_once('=') else {
        return false;
    };
    if value.is_empty() {
        return false;
    }
    matches!(
        key.to_ascii_lowercase().as_str(),
        "field-mask"
            | "field_mask"
            | "max-sessions"
            | "max_sessions"
            | "max-evidence"
            | "max_evidence"
            | "max-excerpt-chars"
            | "max_excerpt_chars"
            | "freshness-policy"
            | "freshness_policy"
            | "freshness-window-seconds"
            | "freshness_window_seconds"
    )
}

pub(crate) fn implicit_robot_query_command(rest: &[String]) -> &'static str {
    if rest.iter().any(|arg| arg_marks_implicit_pack_intent(arg)) {
        "pack"
    } else {
        "search"
    }
}

pub(crate) fn recover_explicit_search_pack_intent(
    rest: &mut [String],
    corrections: &mut Vec<String>,
) {
    if !matches!(rest.first().map(String::as_str), Some("search"))
        || !has_explicit_structured_output_request(rest)
        || !rest
            .iter()
            .skip(1)
            .any(|arg| arg_marks_implicit_pack_intent(arg))
    {
        return;
    }

    rest[0] = "pack".to_string();
    corrections.push(
        "'search ... <pack-only flags>' → 'pack ... <pack-only flags>' (pack-only flags imply answer-pack intent)"
            .to_string(),
    );
}

pub(crate) fn recover_implicit_robot_search_query(
    rest: &mut Vec<String>,
    corrections: &mut Vec<String>,
) {
    let Some(first) = rest.first().cloned() else {
        return;
    };
    if first.starts_with('-')
        || !has_explicit_structured_output_request(rest)
        || looks_like_top_level_command_or_typo(&first)
    {
        return;
    }

    let command = implicit_robot_query_command(rest);
    if is_query_assignment(&first) {
        rest.insert(0, command.to_string());
        corrections.push(format!(
            "'query=<value> --json' → '{command} <value> --json' (implicit robot query)"
        ));
        return;
    }

    let mut query_token_count = 0;
    for arg in rest.iter() {
        if arg.starts_with('-') || is_query_boundary_assignment(command, arg) {
            break;
        }
        query_token_count += 1;
    }
    if query_token_count == 0 {
        return;
    }

    let query = rest[..query_token_count].join(" ");
    rest.splice(0..query_token_count, [command.to_string(), query.clone()]);
    corrections.push(format!(
        "'{query}' → '{command} \"{query}\"' (implicit robot query)"
    ));
}

pub(crate) fn recover_multiword_query_positionals(
    rest: &mut Vec<String>,
    corrections: &mut Vec<String>,
) {
    let Some(command) = rest.first().cloned() else {
        return;
    };
    if !matches!(command.as_str(), "search" | "pack") {
        return;
    }
    if rest.get(1).is_none_or(|arg| arg.starts_with('-')) {
        return;
    }

    let mut query_end = 1;
    while let Some(arg) = rest.get(query_end) {
        if arg.starts_with('-') || is_query_boundary_assignment(&command, arg) {
            break;
        }
        query_end += 1;
    }
    if query_end <= 2 {
        return;
    }

    let query = rest[1..query_end].join(" ");
    rest.splice(1..query_end, [query.clone()]);
    corrections.push(format!(
        "'{command} {query}' → '{command} \"{query}\"' (multi-word query positional)"
    ));
}

/// `cass robot-docs doctor` documents the doctor run-history surface as verbs
/// (`cass doctor ls`, `undo <run-id>`, `diff [<ref>]`, `gc --before <ts>`,
/// `capabilities`), but `doctor` takes them as flags, so every documented
/// spelling failed with "unexpected argument". Map `rest[1]` onto its flag and
/// return the correction note, or `None` when `rest[1]` is not such a verb.
/// `gc` without `--before` becomes a bare `--gc-before`, so clap reports the
/// missing value instead of running a plain doctor check.
pub(crate) fn rewrite_doctor_history_verb(rest: &mut Vec<String>) -> Option<String> {
    let verb = rest.get(1)?.to_ascii_lowercase();
    let flag = match verb.as_str() {
        "ls" => "--ls",
        "undo" => "--undo",
        "diff" => "--diff",
        "explain" => "--explain",
        "watch" => "--watch",
        "capabilities" => "--emit-capabilities",
        "gc" => {
            rest.remove(1);
            if let Some(before) = rest.iter_mut().find(|arg| *arg == "--before") {
                *before = "--gc-before".to_string();
            } else if let Some(before) = rest.iter_mut().find(|arg| arg.starts_with("--before=")) {
                *before = before.replacen("--before=", "--gc-before=", 1);
            } else {
                rest.insert(1, "--gc-before".to_string());
            }
            return Some(
                "'doctor gc --before <ts>' → 'doctor --gc-before <ts>' (doctor run-history GC)"
                    .to_string(),
            );
        }
        _ => return None,
    };
    rest[1] = flag.to_string();
    Some(format!(
        "'doctor {verb}' → 'doctor {flag}' (doctor run-history surface)"
    ))
}

/// Normalize common robot-mode invocation mistakes to make the CLI more forgiving for AI agents.
///
/// This function applies multiple layers of normalization to maximize acceptance of
/// commands where intent is clear, even if syntax is imperfect:
///
/// 1. **Single-dash long flags**: `-robot` → `--robot`, `-limit` → `--limit`
/// 2. **Case normalization**: `--Robot`, `--LIMIT` → `--robot`, `--limit`
/// 3. **Snake-case flag recovery**: `--max_results` → `--max-results`
/// 4. **Subcommand aliases**: `find`/`query`/`q` → `search`, `ls`/`list` → `stats`, etc.
/// 5. **Flag-as-subcommand**: `--robot-docs` → `robot-docs` subcommand
/// 6. **Root structured-output default**: `--json`/`--robot` with no command → `triage --json`
/// 7. **Leading structured-output recovery**: `--json search` → `search --json`
/// 8. **Named positional recovery**: `search --query foo` → `search foo`
/// 9. **Multi-word query recovery**: `search foo bar --json` → `search "foo bar" --json`
/// 10. **Structured format alias recovery**: `search foo --format json`/`--output json` → `search foo --robot-format json`
/// 11. **Result-count alias recovery**: `search foo --max-results 5` → `search foo --limit 5`
/// 12. **Time-window alias recovery**: `search foo --last 7` → `search foo --since -7d`
/// 13. **Provider alias recovery**: `search foo --provider codex` → `search foo --agent codex`
/// 14. **Bare option-pair recovery**: `search foo provider codex` → `search foo --agent codex`
/// 15. **Pack-intent recovery**: `search foo --json --max-evidence 3` → `pack foo --json --max-evidence 3`
/// 16. **Leading-filter query recovery**: `search --agent codex foo bar` → `search "foo bar" --agent codex`
/// 17. **Implicit robot search recovery**: `foo bar --json` → `search "foo bar" --json`
/// 18. **Drill-down option recovery**: `view file line=42` → `view file --line 42`
/// 19. **Search-result field assignments**: `view file line_number=42` → `view file --message-index 42`
/// 20. **Search-result source aliases**: `view source_path=file source_id=local` → `view file --source local`
/// 21. **Robot-docs topic shorthand**: `commands --json` → `robot-docs commands`
/// 22. **Current-session shorthand**: `current --json` → `sessions --current --json`
/// 23. **Structured help recovery**: `help search --json` → `robot-docs commands`
/// 24. **Global flag hoisting**: Moves global flags to front regardless of position
/// 25. **Doctor run-history verbs**: `doctor ls|undo|diff|gc --before|capabilities` → flags
///
/// Returns normalized argv plus an optional correction note teaching proper syntax.
pub(crate) fn normalize_args(raw: Vec<String>) -> (Vec<String>, Option<String>) {
    if raw.is_empty() {
        return (raw, None);
    }
    let prog = &raw[0];
    let mut globals: Vec<String> = Vec::new();
    let mut rest: Vec<String> = Vec::new();
    let mut sub_seen = false;
    let mut corrections: Vec<String> = Vec::new();

    // Known long flags (without --) for single-dash and case normalization
    const KNOWN_LONG_FLAGS: &[&str] = &[
        "robot",
        "json",
        "limit",
        "max-results",
        "num-results",
        "results",
        "count",
        "top-k",
        "topk",
        "top_k",
        "n",
        "offset",
        "agent",
        "provider",
        "tool",
        "connector",
        "agent-type",
        "agent_type",
        "workspace",
        "fields",
        "max-tokens",
        "request-id",
        "cursor",
        "since",
        "until",
        "last",
        "recent",
        "past",
        "within",
        "from",
        "after",
        "start",
        "start-date",
        "start_date",
        "to",
        "before",
        "end",
        "end-date",
        "end_date",
        "days",
        "today",
        "yesterday",
        "week",
        "full",
        "watch",
        "data-dir",
        "verbose",
        "quiet",
        "query",
        "q",
        "text",
        "pattern",
        "color",
        "progress",
        "wrap",
        "nowrap",
        "db",
        "trace-file",
        "robot-help",
        "robot-docs",
        "help",
        "version",
        "force",
        "dry-run",
        "no-cache",
        "source",
        "sessions-from",
        "mode",
        "highlight",
        "timeout",
        "explain",
        "aggregate",
        "display",
        // Subcommand-specific flags
        "path",
        "source-path",
        "source-id",
        "line-number",
        "message-index",
        "conversation-id",
        "file",
        "session",
        "line",
        "context",
        "output",
        "output-format",
        "format",
        "encrypt",
        "theme",
        // Added flags
        "watch-once",
        "watch-interval",
        "background",
        "semantic",
        "embedder",
        "idempotency-key",
        "model",
        "rerank",
        "reranker",
        "daemon",
        "no-daemon",
        "preview",
        "port",
        "config",
        "validate-config",
        "example-config",
        "skip-install",
        "skip-index",
        "skip-sync",
        "resume",
        "non-interactive",
        // Missing flags added
        "approximate",
        "build-hnsw",
        "export-only",
        "verify",
        "scan-secrets",
        "fail-on-secrets",
        "secrets-allow",
        "secrets-deny",
        "no-encryption",
        "i-understand-unencrypted-risks",
        "no-open",
        "stale-threshold",
        "by-source",
        "robot-meta",
        "robot-format",
        "max-content-length",
        "fast-only",
        "quality-only",
        "once",
        "reset-state",
        "asciicast",
        "inline",
        "ui-height",
        "anchor",
        "record-macro",
        "play-macro",
        "group-by",
        "from-file",
        "fixture",
        "repair",
        "cleanup",
        "recover-from-archive",
        "rebuild-canonical-fts",
        "repair-leaked-pages",
        "cleanup-interrupted-artifacts",
        "archive-scan",
        "archive-normalize",
        "check",
        "yes",
        "plan-fingerprint",
        "purge",
        "preset",
        "no-test",
        "no-index",
        "skip-existing",
        "hosts",
    ];

    // Subcommand aliases for common mistakes
    const SUBCOMMAND_ALIASES: &[(&str, &str)] = &[
        // Search aliases
        ("find", "search"),
        ("query", "search"),
        ("q", "search"),
        ("lookup", "search"),
        ("grep", "search"),
        ("session", "sessions"),
        // Stats aliases
        ("ls", "stats"),
        ("list", "stats"),
        ("info", "stats"),
        ("summary", "stats"),
        // Status aliases
        ("st", "status"),
        ("state", "status"),
        // Index aliases
        ("reindex", "index"),
        ("idx", "index"),
        ("rebuild", "index"),
        // View aliases
        ("show", "view"),
        ("get", "view"),
        ("read", "view"),
        // Diag aliases
        ("diagnose", "diag"),
        ("debug", "diag"),
        ("check", "diag"),
        // Capabilities aliases
        ("caps", "capabilities"),
        ("cap", "capabilities"),
        // Agent triage aliases
        ("ready", "triage"),
        ("preflight", "triage"),
        // Answer-pack aliases
        ("answer", "pack"),
        ("evidence", "pack"),
        ("bundle", "pack"),
        ("handoff", "pack"),
        ("why", "pack"),
        ("explain", "pack"),
        ("rca", "pack"),
        ("root-cause", "pack"),
        ("rootcause", "pack"),
        ("summarize", "pack"),
        ("summarise", "pack"),
        // HTML export aliases
        ("html-export", "export-html"),
        ("html_export", "export-html"),
        ("exporthtml", "export-html"),
        // Introspect aliases
        ("inspect", "introspect"),
        ("intro", "introspect"),
        // Robot-docs aliases
        ("docs", "robot-docs"),
        ("help-robot", "robot-docs"),
        ("robotdocs", "robot-docs"),
    ];

    // Short flags that should remain as single-dash
    const VALID_SHORT_FLAGS: &[&str] = &["-q", "-v", "-h", "-V"];

    // Global flags that take a value via separate argument (--flag VALUE)
    // Note: --data-dir is NOT a global flag - it's per-subcommand
    let global_with_value = |s: &str| {
        matches!(
            s,
            "--color" | "--progress" | "--wrap" | "--db" | "--trace-file"
        )
    };

    // Global flags that take a value via `=` syntax or are standalone
    // Note: --data-dir is NOT a global flag - it's per-subcommand
    let is_global = |s: &str| {
        s == "--color"
            || s.starts_with("--color=")
            || s == "--progress"
            || s.starts_with("--progress=")
            || s == "--wrap"
            || s.starts_with("--wrap=")
            || s == "--nowrap"
            || s == "--db"
            || s.starts_with("--db=")
            || s == "--quiet"
            || s == "-q"
            || s == "--verbose"
            || s == "-v"
            || s == "--trace-file"
            || s.starts_with("--trace-file=")
            || s == "--robot-help"
    };

    fn canonical_known_long_flag(flag: &str) -> Option<String> {
        if flag.contains('_') {
            let kebab = flag.replace('_', "-");
            if KNOWN_LONG_FLAGS.contains(&kebab.as_str()) {
                return Some(kebab);
            }
        }

        KNOWN_LONG_FLAGS.contains(&flag).then(|| flag.to_string())
    }

    /// Normalize a single argument: single-dash → double-dash, case → lowercase, snake_case → kebab-case
    fn normalize_single_arg(arg: &str, corrections: &mut Vec<String>) -> String {
        // Skip if already valid short flag
        if VALID_SHORT_FLAGS.contains(&arg) {
            return arg.to_string();
        }

        // Handle single-dash long flags: -robot → --robot, -limit=5 → --limit=5
        if arg.starts_with('-') && !arg.starts_with("--") && arg.len() > 2 {
            let (flag_part, value_part) = if let Some(idx) = arg.find('=') {
                (&arg[1..idx], Some(&arg[idx..]))
            } else {
                (&arg[1..], None)
            };
            let flag_lower = flag_part.to_lowercase();
            if let Some(flag_canonical) = canonical_known_long_flag(&flag_lower) {
                let corrected = if let Some(val) = value_part {
                    format!("--{flag_canonical}{val}")
                } else {
                    format!("--{flag_canonical}")
                };
                let reason = if flag_lower == flag_canonical {
                    "use double-dash for long flags"
                } else {
                    "use double-dash and kebab-case for long flags"
                };
                corrections.push(format!("'{arg}' → '{corrected}' ({reason})"));
                return corrected;
            }
        }

        // Handle case/snake_case normalization for double-dash flags:
        // --Robot → --robot, --max_results → --max-results.
        if let Some(stripped) = arg.strip_prefix("--") {
            let (flag_part, value_part) = if let Some(idx) = stripped.find('=') {
                (&stripped[..idx], Some(&stripped[idx..]))
            } else {
                (stripped, None)
            };
            let flag_lower = flag_part.to_lowercase();
            if let Some(flag_canonical) = canonical_known_long_flag(&flag_lower)
                && flag_part != flag_canonical
            {
                let corrected = if let Some(val) = value_part {
                    format!("--{flag_canonical}{val}")
                } else {
                    format!("--{flag_canonical}")
                };
                let reason = if flag_lower == flag_canonical {
                    "flags are lowercase"
                } else {
                    "long flags use kebab-case"
                };
                corrections.push(format!("'{arg}' → '{corrected}' ({reason})"));
                return corrected;
            }
        }

        arg.to_string()
    }

    let args: Vec<_> = raw.iter().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let arg = args[i];

        // First, normalize the argument (single-dash, case)
        let normalized_arg = normalize_single_arg(arg, &mut corrections);

        // Handle --robot-docs and --robot-docs=topic (flag used as subcommand)
        if normalized_arg == "--robot-docs" {
            rest.push("robot-docs".into());
            corrections
                .push("'--robot-docs' → 'robot-docs' (it's a subcommand, not a flag)".into());
            i += 1;
            continue;
        }
        if let Some(topic) = normalized_arg.strip_prefix("--robot-docs=") {
            rest.push("robot-docs".into());
            if !topic.is_empty() {
                rest.push(topic.to_string());
            }
            corrections.push(format!(
                "'{}' → 'robot-docs {topic}' (robot-docs is a subcommand)",
                arg
            ));
            i += 1;
            continue;
        }

        // Check for subcommand aliases (only before first subcommand seen)
        if !sub_seen && !normalized_arg.starts_with('-') {
            let lower = normalized_arg.to_lowercase();
            if let Some(&(alias, canonical)) = SUBCOMMAND_ALIASES
                .iter()
                .find(|(a, _)| a.eq_ignore_ascii_case(&lower))
            {
                rest.push(canonical.to_string());
                corrections.push(format!(
                    "'{alias}' → '{canonical}' (canonical subcommand name)"
                ));
                sub_seen = true;
                i += 1;
                continue;
            }
        }

        // Handle global flags
        if is_global(&normalized_arg) {
            globals.push(normalized_arg.clone());
            // Only note if globals appear after subcommand (moved to front)
            if sub_seen && !corrections.iter().any(|c| c.contains("moved to front")) {
                corrections.push("Global flags moved to front of command".into());
            }
            // If this global takes a value and doesn't use `=` syntax, consume the next arg
            if global_with_value(&normalized_arg)
                && !normalized_arg.contains('=')
                && i + 1 < args.len()
                && !args[i + 1].starts_with('-')
            {
                globals.push(args[i + 1].to_string());
                i += 1;
            }
            i += 1;
            continue;
        }

        if !sub_seen && !normalized_arg.starts_with('-') {
            sub_seen = true;
        }
        rest.push(normalized_arg);
        i += 1;
    }

    let mut normalized = Vec::with_capacity(1 + globals.len() + rest.len());
    recover_structured_help_requests(&mut rest, &mut corrections);
    recover_robot_docs_topic_shorthands(&mut rest, &mut corrections);
    recover_current_session_shorthands(&mut rest, &mut corrections);
    recover_leading_data_dir_to_subcommand(&mut rest, &mut corrections);
    if move_leading_structured_flag_to_subcommand(&mut rest) {
        corrections.push(
            "Leading --json/--robot moved after the subcommand (structured output flag)".into(),
        );
    }
    recover_implicit_robot_search_query(&mut rest, &mut corrections);
    recover_explicit_search_pack_intent(&mut rest, &mut corrections);
    recover_structured_format_aliases(&mut rest, &mut corrections);
    recover_limit_aliases(&mut rest, &mut corrections);
    recover_time_alias_flags(&mut rest, &mut corrections);
    recover_agent_filter_alias_flags(&mut rest, &mut corrections);
    recover_named_required_positionals(&mut rest, &mut corrections);
    recover_query_after_leading_search_options(&mut rest, &mut corrections);
    recover_bare_option_value_pairs(&mut rest, &mut corrections);
    recover_multiword_query_positionals(&mut rest, &mut corrections);
    recover_drilldown_assignment_flags(&mut rest, &mut corrections);
    recover_key_value_option_assignments(&mut rest, &mut corrections);
    recover_path_line_positionals(&mut rest, &mut corrections);
    if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest
            .get(1)
            .is_some_and(|arg| arg.eq_ignore_ascii_case("backups"))
        && let Some(action) = rest.get(2).map(|arg| arg.to_ascii_lowercase())
    {
        match action.as_str() {
            "list" => {
                rest.remove(2);
                rest.remove(1);
                rest.insert(1, "--backups-list".to_string());
                corrections.push(
                    "'doctor backups list' → 'doctor --backups-list' (backup inventory surface)"
                        .into(),
                );
            }
            "verify" | "restore" => {
                let flag = if action == "verify" {
                    "--backups-verify"
                } else {
                    "--backups-restore"
                };
                let backup_id = rest.get(3).filter(|arg| !arg.starts_with('-')).cloned();
                if backup_id.is_some() {
                    rest.remove(3);
                }
                rest.remove(2);
                rest.remove(1);
                rest.insert(1, flag.to_string());
                if let Some(backup_id) = backup_id {
                    rest.insert(2, backup_id);
                    rest.insert(2, "--backup-id".to_string());
                }
                corrections.push(format!(
                    "'doctor backups {action}' → 'doctor {flag}' (backup {action} surface)"
                ));
            }
            _ => {}
        }
    }
    if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest
            .get(1)
            .is_some_and(|arg| arg.eq_ignore_ascii_case("baseline"))
        && let Some(action) = rest.get(2).map(|arg| arg.to_ascii_lowercase())
    {
        let flag = match action.as_str() {
            "save" => Some("--baseline-save"),
            "diff" => Some("--baseline-diff"),
            "update" => Some("--baseline-update"),
            _ => None,
        };
        if let Some(flag) = flag {
            let baseline_id = rest.get(3).filter(|arg| !arg.starts_with('-')).cloned();
            if baseline_id.is_some() {
                rest.remove(3);
            }
            rest.remove(2);
            rest.remove(1);
            rest.insert(1, flag.to_string());
            if let Some(baseline_id) = baseline_id {
                rest.insert(2, baseline_id);
                rest.insert(2, "--baseline-id".to_string());
            }
            corrections.push(format!(
                "'doctor baseline {action}' → 'doctor {flag}' (diagnostic baseline {action})"
            ));
        }
    }
    if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest.get(1).is_some_and(|arg| {
            arg.eq_ignore_ascii_case("support-bundle") || arg.eq_ignore_ascii_case("support_bundle")
        })
    {
        if rest.get(2).is_some_and(|arg| {
            arg.eq_ignore_ascii_case("verify") || arg.eq_ignore_ascii_case("check")
        }) {
            let support_bundle_path = rest.get(3).filter(|arg| !arg.starts_with('-')).cloned();
            if support_bundle_path.is_some() {
                rest.remove(3);
            }
            rest.remove(2);
            rest.remove(1);
            rest.insert(1, "--support-bundle-verify".to_string());
            if let Some(support_bundle_path) = support_bundle_path {
                rest.insert(2, support_bundle_path);
                rest.insert(2, "--support-bundle-path".to_string());
            }
            corrections.push(
                "'doctor support-bundle verify' → 'doctor --support-bundle-verify' (scrubbed support bundle manifest verification)"
                    .into(),
            );
        } else {
            rest.remove(1);
            rest.insert(1, "--support-bundle".to_string());
            corrections.push(
                "'doctor support-bundle' → 'doctor --support-bundle' (scrubbed diagnostic support bundle)"
                    .into(),
            );
        }
    }
    if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest
            .get(1)
            .is_some_and(|arg| arg.eq_ignore_ascii_case("archive"))
        && let Some(action) = rest.get(2).map(|arg| arg.to_ascii_lowercase())
    {
        let flag = match action.as_str() {
            "export" => Some("--archive-export"),
            "relocate" | "relocation" => Some("--archive-relocate"),
            _ => None,
        };
        if let Some(mut flag) = flag {
            let mut target_index = 3;
            let verify_requested = rest.get(3).is_some_and(|arg| {
                arg.eq_ignore_ascii_case("verify") || arg.eq_ignore_ascii_case("check")
            });
            if verify_requested {
                flag = "--archive-export-verify";
                target_index = 4;
            }
            let archive_target = rest
                .get(target_index)
                .filter(|arg| !arg.starts_with('-'))
                .cloned();
            if archive_target.is_some() {
                rest.remove(target_index);
            }
            if verify_requested {
                rest.remove(3);
            }
            rest.remove(2);
            rest.remove(1);
            rest.insert(1, flag.to_string());
            if let Some(archive_target) = archive_target {
                rest.insert(2, archive_target);
                rest.insert(2, "--archive-target".to_string());
            }
            corrections.push(format!(
                "'doctor archive {action}' → 'doctor {flag}' (archive preservation workflow)"
            ));
        }
    }
    if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest
            .get(1)
            .is_some_and(|arg| arg.eq_ignore_ascii_case("check"))
    {
        rest.remove(1);
        rest.insert(1, "--check".to_string());
        corrections
            .push("'doctor check' → 'doctor --check' (typed read-only doctor surface)".into());
    } else if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest
            .get(1)
            .is_some_and(|arg| arg.eq_ignore_ascii_case("repair"))
    {
        rest.remove(1);
        rest.insert(1, "--repair".to_string());
        corrections.push(
            "'doctor repair' → 'doctor --repair' (fingerprinted doctor repair surface)".into(),
        );
    } else if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest
            .get(1)
            .is_some_and(|arg| arg.eq_ignore_ascii_case("cleanup"))
    {
        rest.remove(1);
        rest.insert(1, "--cleanup".to_string());
        corrections.push(
            "'doctor cleanup' → 'doctor --cleanup' (fingerprinted doctor cleanup surface)".into(),
        );
    } else if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest.get(1).is_some_and(|arg| {
            arg.eq_ignore_ascii_case("recover-from-archive")
                || arg.eq_ignore_ascii_case("recover_from_archive")
                || arg.eq_ignore_ascii_case("recover-from-canonical-events")
        })
    {
        // #285: 'doctor recover-from-archive <DIR>' → '--recover-from-archive <DIR>'.
        // The next bare positional (if any) is the recovery target dir.
        let target = rest.get(2).filter(|arg| !arg.starts_with('-')).cloned();
        if target.is_some() {
            rest.remove(2);
        }
        rest.remove(1);
        rest.insert(1, "--recover-from-archive".to_string());
        if let Some(target) = target {
            rest.insert(2, target);
        }
        corrections.push(
            "'doctor recover-from-archive <DIR>' → 'doctor --recover-from-archive <DIR>' (rebuild source JSONL from preserved archive events)"
                .into(),
        );
    } else if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest.get(1).is_some_and(|arg| {
            arg.eq_ignore_ascii_case("rebuild-canonical-fts")
                || arg.eq_ignore_ascii_case("rebuild_canonical_fts")
        })
    {
        rest.remove(1);
        rest.insert(1, "--rebuild-canonical-fts".to_string());
        corrections.push(
            "'doctor rebuild-canonical-fts' → 'doctor --rebuild-canonical-fts' (safe canonical FTS5 parity repair)"
                .into(),
        );
    } else if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest.get(1).is_some_and(|arg| {
            arg.eq_ignore_ascii_case("repair-leaked-pages")
                || arg.eq_ignore_ascii_case("repair_leaked_pages")
        })
    {
        rest.remove(1);
        rest.insert(1, "--repair-leaked-pages".to_string());
        corrections.push(
            "'doctor repair-leaked-pages' → 'doctor --repair-leaked-pages' (in-place leaked-page repair)"
                .into(),
        );
    } else if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest.get(1).is_some_and(|arg| {
            arg.eq_ignore_ascii_case("archive-scan") || arg.eq_ignore_ascii_case("archive_scan")
        })
    {
        rest.remove(1);
        rest.insert(1, "--archive-scan".to_string());
        corrections.push(
            "'doctor archive-scan' → 'doctor --archive-scan' (read-only archive hygiene scan)"
                .into(),
        );
    } else if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && rest.get(1).is_some_and(|arg| {
            arg.eq_ignore_ascii_case("archive-normalize")
                || arg.eq_ignore_ascii_case("archive_normalize")
        })
    {
        rest.remove(1);
        rest.insert(1, "--archive-normalize".to_string());
        corrections.push(
            "'doctor archive-normalize' → 'doctor --archive-normalize' (fingerprinted additive archive metadata normalization)"
                .into(),
        );
    }
    if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("doctor"))
        && let Some(correction) = rewrite_doctor_history_verb(&mut rest)
    {
        corrections.push(correction);
    }
    if rest
        .first()
        .is_some_and(|arg| arg.eq_ignore_ascii_case("robot-docs"))
    {
        let before_len = rest.len();
        rest.retain(|arg| arg != "--robot" && arg != "--json");
        if rest.len() != before_len {
            corrections.push(
                "Removed redundant --robot/--json from robot-docs (docs output is already machine-readable)"
                    .into(),
            );
        }
        if rest.len() == 1 {
            rest.push("guide".to_string());
            corrections.push("'robot-docs' → 'robot-docs guide' (default agent guide)".into());
        }
    }
    if rest.first().is_none_or(|arg| arg.starts_with('-'))
        && let Some(rewritten) = root_structured_triage_rewrite(&rest)
    {
        rest = rewritten;
        corrections
            .push("Top-level --json/--robot → 'triage --json' (read-only agent preflight)".into());
    }
    normalized.push(prog.clone());
    normalized.extend(globals);
    normalized.extend(rest);

    let note = if corrections.is_empty() {
        None
    } else {
        Some(format!(
            "Auto-corrected: {}. Canonical form: {}",
            corrections.join("; "),
            if normalized.len() > 1 {
                normalized[1..].join(" ")
            } else {
                String::new()
            }
        ))
    };
    (normalized, note)
}
