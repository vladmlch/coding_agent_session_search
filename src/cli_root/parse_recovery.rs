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

/// Build a helpful error message when a command group (e.g. `analytics`, `sources`,
/// `models`, `import`) is invoked without a required subcommand.
pub(crate) fn format_missing_subcommand_error(args: &[String]) -> String {
    // Find the bare command group name from the args (skip program name and flags).
    let group = args
        .iter()
        .skip(1)
        .find(|a| !a.starts_with('-'))
        .map(|s| s.as_str())
        .unwrap_or("unknown");

    let (subcommands, examples): (&[&str], &[&str]) = match group {
        "sources" => (
            &["list", "add", "remove", "doctor", "mappings", "agents"],
            &[
                "cass sources list --json",
                "cass sources add myserver --path /data/sessions",
                "cass sources doctor --json",
            ],
        ),
        "models" => (
            &[
                "status",
                "install",
                "verify",
                "backfill",
                "build-hnsw",
                "remove",
            ],
            &[
                "cass models status --json",
                "cass models install --model all-minilm-l6-v2 --from-file <DIR>",
                "cass models verify --json",
            ],
        ),
        "import" => (
            &["chatgpt"],
            &["cass import chatgpt /path/to/conversations.json"],
        ),
        _ => (&[], &["cass --help"]),
    };

    let mut msg = format!(
        "Missing required subcommand. Run `cass {} --help` for usage.",
        group
    );

    if !subcommands.is_empty() {
        msg.push_str(&format!(
            "\n\nAvailable subcommands for `cass {}`:\n",
            group
        ));
        for sc in subcommands {
            msg.push_str(&format!("  {}\n", sc));
        }
    }

    if !examples.is_empty() {
        msg.push_str("\nExamples:\n");
        for ex in examples {
            msg.push_str(&format!("  {}\n", ex));
        }
    }

    msg
}

/// Build a friendly parse error with actionable, context-aware examples for AI agents.
///
/// This function analyzes what the agent was likely trying to do and provides
/// targeted examples that match their apparent intent.
pub(crate) fn format_friendly_parse_error(
    err: clap::Error,
    raw: &[String],
    normalized: &[String],
) -> String {
    let is_robot = raw
        .iter()
        .any(|s| s == "--json" || s == "--robot" || s == "-robot" || s == "-json");

    // Detect what the agent was probably trying to do
    let raw_str = raw
        .iter()
        .skip(1)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase();
    let intent = detect_command_intent(&raw_str);

    if is_robot {
        let mut err_map = serde_json::Map::new();
        err_map.insert("status".into(), "error".into());
        err_map.insert("error".into(), err.to_string().into());
        err_map.insert("kind".into(), "argument_parsing".into());

        if raw != normalized && normalized.len() > 1 {
            err_map.insert(
                "normalized_attempt".into(),
                normalized[1..].join(" ").into(),
            );
        }

        // Context-aware examples based on detected intent
        let examples = get_contextual_examples(&intent);
        err_map.insert("examples".into(), serde_json::json!(examples));

        // Context-aware hints
        let hints = get_contextual_hints(&intent, &raw_str);
        err_map.insert("hints".into(), serde_json::json!(hints));

        // Common mistakes for this intent
        if let Some(common_mistakes) = get_common_mistakes(&intent) {
            err_map.insert("common_mistakes".into(), serde_json::json!(common_mistakes));
        }

        // Quick reference for flags
        err_map.insert(
            "flag_syntax".into(),
            serde_json::json!({
                "correct": ["--limit 5", "--robot", "--json"],
                "incorrect": ["--limt 5", "-limit 5", "--Limit"]
            }),
        );

        return serde_json::to_string_pretty(&err_map).unwrap_or_else(|_| err.to_string());
    }

    // Human-readable format
    let mut parts = Vec::new();
    parts.push("Argument parsing failed; command intent unclear.".to_string());
    parts.push(format!("Error: {err}"));
    if raw != normalized && normalized.len() > 1 {
        parts.push(format!(
            "Attempted normalization: {}",
            normalized[1..].join(" ")
        ));
    }
    parts.push(String::new());
    parts.push(format!(
        "Based on your command, you may be trying to: {intent}"
    ));
    parts.push(String::new());
    parts.push("Correct examples:".to_string());
    for ex in get_contextual_examples(&intent) {
        parts.push(format!("  {ex}"));
    }
    parts.push(String::new());
    parts.push("Quick syntax reference:".to_string());
    parts.push("  - Long flags use double-dash: --robot, --limit 5".to_string());
    parts.push("  - Flag values use space or equals: --limit 5 or --limit=5".to_string());
    parts.push("  - Subcommands come first: cass search \"query\"".to_string());
    parts.join("\n")
}

/// Detect the likely command intent from the raw argument string.
pub(crate) fn detect_command_intent(raw_str: &str) -> String {
    if matches!(
        raw_str.split_whitespace().next(),
        Some("index" | "rebuild" | "reindex")
    ) {
        "rebuild or manage the search index".to_string()
    } else if raw_str.contains("search")
        || raw_str.contains("find")
        || raw_str.contains("query")
        || raw_str.contains("grep")
    {
        "search for sessions or messages".to_string()
    } else if raw_str.contains("session") || raw_str.contains("current") {
        "discover or list session files".to_string()
    } else if raw_str.contains("doc") || raw_str.contains("help") || raw_str.contains("robot") {
        "get robot-mode documentation".to_string()
    } else if raw_str.contains("stats") || raw_str.contains("ls") || raw_str.contains("list") {
        "view statistics or list sessions".to_string()
    } else if raw_str.contains("index")
        || raw_str.contains("rebuild")
        || raw_str.contains("reindex")
    {
        "rebuild or manage the search index".to_string()
    } else if raw_str.contains("view") || raw_str.contains("show") || raw_str.contains("get") {
        "view a specific session".to_string()
    } else if raw_str.contains("cap") || raw_str.contains("introspect") {
        "discover tool capabilities".to_string()
    } else if raw_str.contains("diag") || raw_str.contains("debug") || raw_str.contains("check") {
        "run diagnostics".to_string()
    } else if raw_str.contains("status") {
        "check status".to_string()
    } else if raw_str.contains("health") {
        "run health check".to_string()
    } else {
        "run a cass command".to_string()
    }
}

/// Get context-aware examples based on detected intent.
pub(crate) fn get_contextual_examples(intent: &str) -> Vec<&'static str> {
    if intent.contains("index") {
        vec![
            "cass index --robot",
            "cass index --full --robot",
            "cass index --semantic --build-hnsw --robot",
            "cass index --robot --data-dir /custom/path",
        ]
    } else if intent.contains("search") {
        vec![
            "cass search \"error handling\" --robot --limit 10",
            "cass search \"authentication\" --robot --agent claude",
            "cass search \"database\" --robot --since 2024-01-01",
            "cass search \"TODO\" --robot --workspace /path/to/project",
        ]
    } else if intent.contains("view") {
        vec![
            "cass view <session-path> --robot",
            "cass view <session-path> -n 42 --json",
        ]
    } else if intent.contains("session") {
        vec![
            "cass sessions --current --json",
            "cass sessions --workspace /path/to/project --json --limit 5",
            "cass sessions --json --limit 10",
        ]
    } else if intent.contains("documentation") {
        vec![
            "cass robot-docs commands",
            "cass robot-docs schemas",
            "cass robot-docs examples",
            "cass --robot-help",
        ]
    } else if intent.contains("statistics") || intent.contains("list") {
        vec![
            "cass stats --robot",
            "cass stats --robot --source local",
            "cass stats --robot --by-source",
        ]
    } else if intent.contains("capabilities") {
        vec!["cass capabilities --json", "cass introspect --json"]
    } else if intent.contains("diagnostics") {
        vec!["cass diag --robot", "cass diag --robot --verbose"]
    } else if intent.contains("status") {
        vec!["cass status --robot"]
    } else if intent.contains("health") {
        vec!["cass health --json"]
    } else {
        vec![
            "cass --robot-help                    # Get robot-mode documentation",
            "cass search \"query\" --robot         # Search sessions",
            "cass capabilities --json             # Discover workflows, mistake recoveries, commands",
            "cass stats --robot                   # View statistics",
        ]
    }
}

/// Get context-aware hints based on detected intent and raw command.
pub(crate) fn get_contextual_hints(intent: &str, raw_str: &str) -> Vec<String> {
    let mut hints = Vec::new();

    // Check for common syntax mistakes
    if raw_str.contains("-robot") && !raw_str.contains("--robot") {
        hints.push("Use '--robot' (double-dash), not '-robot'".to_string());
    }
    if raw_str.contains("-json") && !raw_str.contains("--json") {
        hints.push("Use '--json' (double-dash), not '-json'".to_string());
    }
    // Only flag bare `limit=` without leading dash as problematic
    if (raw_str.contains(" limit=") || raw_str.starts_with("limit="))
        && !raw_str.contains("--limit=")
        && !raw_str.contains("-limit=")
    {
        hints.push("Use '--limit 5' or '--limit=5', not 'limit=5'".to_string());
    }
    if raw_str.contains("--robot-docs") {
        hints.push(
            "'robot-docs' is a subcommand: use 'cass robot-docs' not 'cass --robot-docs'"
                .to_string(),
        );
    }

    // Intent-specific hints
    if intent.contains("index") {
        hints.push("For indexing options, run: cass index --help".to_string());
        if raw_str.split_whitespace().any(|arg| arg == "--approximate") {
            hints.push(
                "Build ANN with: cass index --semantic --build-hnsw; use --approximate only when searching"
                    .to_string(),
            );
        }
    } else if intent.contains("search") && !raw_str.contains("search") {
        hints.push(
            "Use the 'search' subcommand explicitly: cass search \"your query\" --robot"
                .to_string(),
        );
    }

    if hints.is_empty() {
        hints.push(format!("For {intent}, try: cass --robot-help"));
    }

    hints
}

/// Get common mistakes for a given intent.
///
/// Note: Only include mistakes that would actually fail after normalization.
/// Commands that get auto-corrected and succeed (like `cass ls --robot` → `cass stats --robot`)
/// should NOT be listed here since the user would never see this error message.
pub(crate) fn get_common_mistakes(intent: &str) -> Option<serde_json::Value> {
    let mistakes = if intent.contains("index") {
        vec![(
            "cass index --semantic --approximate",
            "cass index --semantic --build-hnsw",
        )]
    } else if intent.contains("search") {
        vec![
            // query="foo" without subcommand - normalization adds "search" but the syntax is wrong
            ("cass query=\"foo\" --robot", "cass search \"foo\" --robot"),
            // Missing query entirely
            (
                "cass search --robot --limit 5",
                "cass search \"your query\" --robot --limit 5",
            ),
        ]
    } else if intent.contains("documentation") {
        vec![
            // Flag syntax for subcommand (--robot-docs gets normalized but shown for education)
            ("cass --robot-docs", "cass robot-docs"),
            ("cass --robot-docs=commands", "cass robot-docs commands"),
            // Adding --robot to robot-docs (which doesn't accept it)
            ("cass robot-docs --robot", "cass robot-docs"),
        ]
    } else if intent.contains("statistics") {
        // Note: `cass ls --robot` actually works (normalizes to `cass stats --robot`)
        // so we show mistakes that would actually fail
        vec![
            // Missing required output flag for piping
            ("cass stats | jq .", "cass stats --json | jq ."),
        ]
    } else {
        return None;
    };

    Some(serde_json::json!(
        mistakes
            .iter()
            .map(|(wrong, right)| { serde_json::json!({"wrong": wrong, "correct": right}) })
            .collect::<Vec<_>>()
    ))
}

pub(crate) const CANONICAL_TOP_LEVEL_COMMANDS: &[&str] = &[
    "search",
    "pack",
    "stats",
    "status",
    "state",
    "health",
    "selftest",
    "diag",
    "storage",
    "dedup",
    "doctor",
    "sessions",
    "view",
    "expand",
    "resume",
    "index",
    "capabilities",
    "triage",
    "support-bundle",
    "onboarding",
    "introspect",
    "api-version",
    "robot-docs",
    "models",
    "quarantine",
    "sources",
    "context",
    "timeline",
    "mirror",
    "forget",
    "export",
    "export-html",
    "pages",
    // Landed 2026-09-01 (bookmarks CLI); without this entry robot mode
    // rewrote `cass bookmarks … --robot` into `search bookmarks …` (GH #367
    // class), which the full lib suite caught on 2026-09-02.
    "bookmarks",
    "import",
    "daemon",
    "completions",
    "man",
];

/// Commands that mutate state or reach the network must never be entered by
/// fuzzy typo attraction — only by exact spelling. `forget` deletes indexed
/// data (its reparse failure also used to swallow the implicit-search
/// fallback: `cass format` → "forget" → hard exit-2 instead of searching)
/// and `upgrade` used to perform a release check / interactive install
/// (`cass parade` executed it) before the local-only port removed it.
/// Adversarial-review finding F1 on GH #367.
pub(crate) const SIDE_EFFECT_EXACT_ONLY_COMMANDS: &[&str] = &["forget"];

/// A command invocation removed by the local-only port (cass-local plan,
/// tasks 7–14).
///
/// `path` holds the invocation words: one for a top-level command
/// (`&["swarm"]` rejects `cass swarm <anything>`), two for a
/// `command subcommand` pair (`&["sources", "sync"]` rejects only that
/// pair — `cass sources list` stays live). Matching runs case-insensitively
/// on raw argv inside `parse_cli`, before `normalize_args` rewrites
/// anything and before `heuristic_parse_recovery` tries typo correction,
/// so a removed name errors out instead of being corrected into a
/// different live command or folded into an implicit search query.
///
/// `hint` names a local equivalent for the same intent when one exists
/// (for example `cass models install --from-file DIR`); it is reported to
/// the caller, never executed.
pub(crate) struct RemovedCommand {
    pub(crate) path: &'static [&'static str],
    pub(crate) hint: Option<&'static str>,
}

/// Commands removed by the local-only port. Tasks 7–14 append entries as
/// each surface is deleted. A one-word entry must drop the name from
/// `CANONICAL_TOP_LEVEL_COMMANDS` and `SUBCOMMAND_ALIASES` in the same
/// commit — `removed_invocation_tests` pins that a top-level name may
/// never sit in the live and the removed list at once. A pair entry
/// (`["sources","sync"]`) removes that pair alone and keeps its parent
/// live, so it never touches the top-level lists.
pub(crate) const REMOVED_COMMANDS: &[RemovedCommand] = &[
    RemovedCommand {
        path: &["swarm"],
        hint: Some("cass search and cass pack cover local-only handoffs"),
    },
    RemovedCommand {
        path: &["lessons"],
        hint: Some("cass search covers local session evidence"),
    },
    RemovedCommand {
        path: &["fleet"],
        hint: Some("cass sources doctor covers local source diagnostics"),
    },
    RemovedCommand {
        path: &["release-verify"],
        hint: Some("cass runs fully local-only; no release channels are verified"),
    },
    RemovedCommand {
        path: &["upgrade"],
        hint: Some("reinstall with `cargo install --path . --locked`"),
    },
    RemovedCommand {
        path: &["sources", "sync"],
        hint: Some(
            "remote sync was removed; existing archive mirrors stay indexed via `cass index`",
        ),
    },
    RemovedCommand {
        path: &["sources", "setup"],
        hint: Some("cass sources add NAME --path PATH adds a local source"),
    },
    RemovedCommand {
        path: &["sources", "discover"],
        hint: Some(
            "remote discovery was removed; cass sources add NAME --path PATH adds a local source",
        ),
    },
    RemovedCommand {
        path: &["sources", "reingest"],
        hint: Some("cass index scans existing archive mirrors directly"),
    },
    RemovedCommand {
        path: &["sources", "artifact-manifest"],
        hint: Some("remote artifact exchange was removed; the index is built and used locally"),
    },
    RemovedCommand {
        path: &["sources", "schedule"],
        hint: Some(
            "sync schedules were removed with remote sync; the field stays readable in old configs",
        ),
    },
    RemovedCommand {
        path: &["models", "check-update"],
        hint: Some(
            "model update checks were removed with network access; pinned manifest revisions are the only supported installs",
        ),
    },
];

/// A flag (or specific flag values) removed from a command that otherwise
/// stays live.
///
/// `path` is the command path the flag is removed from (`&["pages"]`,
/// `&["models", "install"]`); an empty `path` removes the flag under every
/// command (task 13's `--embedder external`). `flag` is the long flag name
/// without dashes — both `--mirror` and the single-dash `-mirror` spellings
/// match, as does `--mirror=VALUE`. `values: None` rejects the flag
/// outright; `values: Some(..)` rejects only the listed values
/// (case-insensitive), so a surviving flag loses only its network-facing
/// choices (`pages --target github` while `--target local` parses on).
/// `hint` behaves like `RemovedCommand::hint`.
pub(crate) struct RemovedFlag {
    pub(crate) path: &'static [&'static str],
    pub(crate) flag: &'static str,
    pub(crate) values: Option<&'static [&'static str]>,
    pub(crate) hint: Option<&'static str>,
}

/// Flags removed by the local-only port; see `RemovedFlag`.
pub(crate) const REMOVED_FLAGS: &[RemovedFlag] = &[
    RemovedFlag {
        path: &["models", "install"],
        flag: "mirror",
        values: None,
        hint: Some("`cass models install --from-file DIR`"),
    },
    RemovedFlag {
        path: &["models", "install"],
        flag: "yes",
        values: None,
        hint: Some("local installs need no confirmation: `cass models install --from-file DIR`"),
    },
    RemovedFlag {
        path: &["models", "verify"],
        flag: "repair",
        values: None,
        hint: Some(
            "repair-by-download was removed; reinstall with `cass models install --from-file DIR`",
        ),
    },
    // Task 13: the external HTTP embedding provider is gone. The flag itself
    // survives for local providers; only the `external` value is removed,
    // under every command that accepts `--embedder`.
    RemovedFlag {
        path: &[],
        flag: "embedder",
        values: Some(&["external"]),
        hint: Some(
            "select a local embedder (`--embedder minilm`, `multilingual-minilm` or `hash`); legacy CASS_EXTERNAL_* settings are ignored",
        ),
    },
    // Task 14: hosted Pages targets are gone. `--target` survives for its
    // sole remaining value `local`; the hosted values are rejected.
    RemovedFlag {
        path: &["pages"],
        flag: "target",
        values: Some(&["github", "cloudflare"]),
        hint: Some("`cass pages --target local` (the default) writes a local archive"),
    },
    // Task 14: hosted deployment flags were removed outright.
    RemovedFlag {
        path: &["pages"],
        flag: "project",
        values: None,
        hint: Some("hosted publishing is gone; exports stay in deployment.output_dir"),
    },
    RemovedFlag {
        path: &["pages"],
        flag: "repo",
        values: None,
        hint: Some("hosted publishing is gone; exports stay in deployment.output_dir"),
    },
    RemovedFlag {
        path: &["pages"],
        flag: "branch",
        values: None,
        hint: Some("hosted publishing is gone; exports stay in deployment.output_dir"),
    },
    RemovedFlag {
        path: &["pages"],
        flag: "account-id",
        values: None,
        hint: Some("Cloudflare credentials are gone; exports stay in deployment.output_dir"),
    },
    RemovedFlag {
        path: &["pages"],
        flag: "api-token",
        values: None,
        hint: Some("Cloudflare credentials are gone; exports stay in deployment.output_dir"),
    },
    // Task 14: exports are always self-contained; there is no CDN mode to
    // disable, so `--no-cdns` is retired rather than kept as a no-op.
    RemovedFlag {
        path: &["export-html"],
        flag: "no-cdns",
        values: None,
        hint: Some("exports are always fully self-contained (no CDN references)"),
    },
];

/// Whether `name` is a removed *top-level* command: a one-word path in
/// `removed_commands`. Removed names are excluded from fuzzy typo
/// attraction even while a stale `CANONICAL_TOP_LEVEL_COMMANDS` entry
/// still lists them (the disjointness test makes that state a hard
/// failure, the filter makes it harmless in the meantime). A pair entry
/// (`["sources","sync"]`) removes that pair alone — its parent stays a
/// live command, so it must keep attracting corrections (`cass sourcess`
/// → `sources`) instead of falling into the implicit-search fold.
pub(crate) fn is_removed_command_name(name: &str, removed_commands: &[RemovedCommand]) -> bool {
    removed_commands
        .iter()
        .any(|entry| entry.path.len() == 1 && entry.path[0].eq_ignore_ascii_case(name))
}

/// Positional argv words in order, program name excluded. Flag tokens are
/// skipped, and so is the separate value of the leading flags that take
/// one (`--db PATH`, `--data-dir DIR`, `--format FMT`, …); `--` ends the
/// scan because nothing after it can be a subcommand. Kept deliberately
/// small: a subcommand-level option with a separate value sitting between
/// the command word and the subcommand can still hide a removed pair
/// (`cass sources --agent x sync`), but clap rejects that invocation once
/// the pair is gone — the mechanism only has to catch canonical spellings
/// before correction does.
pub(crate) fn removed_invocation_positionals(raw_args: &[String]) -> Vec<&str> {
    const VALUE_TAKING_FLAGS: &[&str] = &[
        "--color",
        "--progress",
        "--wrap",
        "--db",
        "--trace-file",
        "--data-dir",
        "--stale-threshold",
        "--robot-format",
        "--format",
        "--output",
        "--output-format",
        "--output_format",
        "--agent",
        "--workspace",
        "--model",
        "--source",
        "--fields",
        "--file",
    ];
    let mut words = Vec::new();
    let mut index = 1;
    while index < raw_args.len() {
        let arg = raw_args[index].as_str();
        if arg == "--" {
            break;
        }
        if arg.starts_with('-') {
            if !arg.contains('=') && VALUE_TAKING_FLAGS.contains(&arg) {
                index += 1;
            }
            index += 1;
            continue;
        }
        words.push(arg);
        index += 1;
    }
    words
}

/// The structured error for a rejected invocation: exit 2 (usage), kind
/// `removed-command`, never retryable. `typed` is set when the caller's
/// word only fuzzy-matched the removed name — the message then names both
/// so `cass swarn` still reports `cass swarm` as removed.
pub(crate) fn removed_command_error(
    path: &[&str],
    typed: Option<&str>,
    hint: Option<&'static str>,
) -> CliError {
    let canonical = path.join(" ");
    let message = match typed {
        Some(arg) => format!(
            "`cass {arg}` resolves to `cass {canonical}`, which was removed: cass runs fully local-only"
        ),
        None => format!("`cass {canonical}` was removed: cass runs fully local-only"),
    };
    CliError {
        code: 2,
        kind: CliErrorKind::RemovedCommand.kind_str(),
        message,
        hint: hint.map(|equivalent| format!("local equivalent: {equivalent}")),
        retryable: false,
    }
}

/// Whether `command`/`sub` name the command path an entry applies to. An
/// empty entry path matches every command (flag removed suite-wide).
pub(crate) fn removed_path_matches(path: &[&str], command: &str, sub: &str) -> bool {
    match path {
        [] => true,
        [top] => command == *top,
        [top, second] => command == *top && sub == *second,
        _ => false,
    }
}

/// Rejection for `raw_args` under `removed_commands`/`removed_flags`, or
/// `None` when the invocation is live. Taken as parameters (not the
/// module consts) so unit tests exercise the matcher with synthetic
/// tables while the real lists are still empty.
pub(crate) fn removed_invocation_error(
    raw_args: &[String],
    removed_commands: &[RemovedCommand],
    removed_flags: &[RemovedFlag],
) -> Option<CliError> {
    if removed_commands.is_empty() && removed_flags.is_empty() {
        return None;
    }
    let words = removed_invocation_positionals(raw_args);
    let command = words.first().copied().unwrap_or_default();
    if command.is_empty() {
        return None;
    }
    let command_lc = command.to_ascii_lowercase();
    let sub = words.get(1).copied().unwrap_or_default();
    let sub_lc = sub.to_ascii_lowercase();

    // 1. Exact (case-insensitive) command path: `cass swarm status` hits
    //    `["swarm"]`, `cass sources sync` hits `["sources","sync"]`.
    for entry in removed_commands {
        if removed_path_matches(entry.path, &command_lc, &sub_lc) {
            return Some(removed_command_error(entry.path, None, entry.hint));
        }
    }

    // 2. Typo of a removed top-level command: `cass swarn` must report the
    //    removed `swarm`, not correct into a live command. A live
    //    candidate wins only when it is strictly closer — on ties the
    //    invocation still fails instead of redirecting intent. An exact live
    //    command (e.g. `sessions`, edit-distance 2 from `lessons`) is never a
    //    typo: it must reach normal dispatch untouched.
    if !CANONICAL_TOP_LEVEL_COMMANDS.contains(&command_lc.as_str()) {
        let closest_removed = removed_commands
            .iter()
            .filter(|entry| entry.path.len() == 1)
            .map(|entry| (entry, strsim::levenshtein(&command_lc, entry.path[0])))
            .filter(|(_, distance)| *distance <= 2)
            .min_by_key(|(_, distance)| *distance);
        if let Some((entry, removed_distance)) = closest_removed {
            let live_distance = closest_top_level_command_in(&command_lc, removed_commands)
                .map(|name| strsim::levenshtein(&command_lc, name));
            if live_distance.is_none_or(|distance| removed_distance <= distance) {
                return Some(removed_command_error(entry.path, Some(command), entry.hint));
            }
        }
    }

    // 3. Removed flags on live command paths. Tokens are scanned raw
    //    (normalize_args has not run): `--flag`, `--flag=value` and the
    //    single-dash `-flag` spelling all match.
    for entry in removed_flags {
        if !entry.path.is_empty() && !removed_path_matches(entry.path, &command_lc, &sub_lc) {
            continue;
        }
        let mut index = 1;
        while index < raw_args.len() {
            let token = &raw_args[index];
            index += 1;
            if token == "--" {
                break;
            }
            let Some(flag_body) = token.strip_prefix('-').filter(|_| token.len() > 1) else {
                continue;
            };
            let flag_body = flag_body.strip_prefix('-').unwrap_or(flag_body);
            let (name, inline_value) = match flag_body.split_once('=') {
                Some((name, value)) => (name, Some(value)),
                None => (flag_body, None),
            };
            if !name.eq_ignore_ascii_case(entry.flag) {
                continue;
            }
            let Some(removed_values) = entry.values else {
                return Some(removed_flag_error(entry, None));
            };
            let value = inline_value.map(str::to_string).or_else(|| {
                raw_args
                    .get(index)
                    .filter(|next| !next.starts_with('-'))
                    .cloned()
            });
            if let Some(value) = value.filter(|value| {
                removed_values
                    .iter()
                    .any(|removed| removed.eq_ignore_ascii_case(value))
            }) {
                return Some(removed_flag_error(entry, Some(value.as_str())));
            }
        }
    }

    None
}

/// The structured error for a removed flag: same envelope contract as a
/// removed command (exit 2, kind `removed-command`). `matched_value` names a
/// removed value when only that selection was dropped (`--embedder external`),
/// so the message does not claim the whole surviving flag is gone.
pub(crate) fn removed_flag_error(entry: &RemovedFlag, matched_value: Option<&str>) -> CliError {
    let on = if entry.path.is_empty() {
        String::new()
    } else {
        format!(" on `cass {}`", entry.path.join(" "))
    };
    let flag = match matched_value {
        Some(value) => format!("--{} {}", entry.flag, value),
        None => format!("--{}", entry.flag),
    };
    CliError {
        code: 2,
        kind: CliErrorKind::RemovedCommand.kind_str(),
        message: format!("`{flag}`{on} was removed: cass runs fully local-only"),
        hint: entry
            .hint
            .map(|equivalent| format!("local equivalent: {equivalent}")),
        retryable: false,
    }
}

/// Nearest canonical subcommand for a mistyped first argument.
///
/// An argument that already names a canonical subcommand is never a typo:
/// when clap rejects `cass status --jsn` for its flag, the subcommand must
/// stay `status`. Returning the nearest *other* command here made recovery
/// run `stats` (and `import` for `export`) with exit 0 and only a stderr
/// note (2l1b0.51).
pub(crate) fn closest_top_level_command(arg: &str) -> Option<&'static str> {
    closest_top_level_command_in(arg, REMOVED_COMMANDS)
}

/// `closest_top_level_command` with the removed-command table explicit —
/// the same parameterized-table pattern as `removed_invocation_error`, so
/// unit tests can prove a removed pair under a live parent never hides
/// that parent while the real lists are still empty.
pub(crate) fn closest_top_level_command_in(
    arg: &str,
    removed_commands: &[RemovedCommand],
) -> Option<&'static str> {
    let lower = arg.to_ascii_lowercase();
    if lower.len() < 3 || CANONICAL_TOP_LEVEL_COMMANDS.contains(&lower.as_str()) {
        return None;
    }
    CANONICAL_TOP_LEVEL_COMMANDS
        .iter()
        .copied()
        .filter(|candidate| !SIDE_EFFECT_EXACT_ONLY_COMMANDS.contains(candidate))
        // A removed name must never be a correction candidate either: the
        // disjointness invariant in `removed_invocation_tests` keeps this a
        // no-op while REMOVED_COMMANDS stays disjoint from CANONICAL_*.
        .filter(|candidate| !is_removed_command_name(candidate, removed_commands))
        .map(|candidate| (candidate, strsim::levenshtein(&lower, candidate)))
        .filter(|(_, distance)| *distance <= 2)
        .min_by_key(|(_, distance)| *distance)
        .map(|(candidate, _)| candidate)
}

/// Heuristic recovery for command-line errors to help agents.
/// Returns `(corrected_args, correction_note)` if a likely intent is found.
pub(crate) fn heuristic_parse_recovery(
    err: &clap::Error,
    raw_args: &[String],
) -> Option<(Vec<String>, String)> {
    // Only attempt recovery for "unknown argument" or "unrecognized subcommand" errors
    let is_unknown = err.kind() == clap::error::ErrorKind::UnknownArgument
        || err.kind() == clap::error::ErrorKind::InvalidSubcommand;

    if !is_unknown || raw_args.len() < 2 {
        return None;
    }

    let prog = &raw_args[0];
    let args = &raw_args[1..];
    let mut corrected = Vec::new();
    corrected.push(prog.clone());

    let mut made_correction = false;
    let mut notes = Vec::new();

    // 1. Detect implicit "search" subcommand
    // If the first arg isn't a known subcommand or flag, and looks like a query, assume "search".
    if !args.is_empty()
        && !args[0].starts_with('-')
        && let Some(command) = closest_top_level_command(&args[0])
    {
        corrected.push(command.to_string());
        notes.push(format!(
            "Corrected subcommand typo '{}' to '{}'",
            args[0], command
        ));
        made_correction = true;
        corrected.extend_from_slice(&args[1..]);
    } else if !args.is_empty()
        && !args[0].starts_with('-')
        && !CANONICAL_TOP_LEVEL_COMMANDS.contains(&args[0].as_str())
        && !matches!(
            args[0].as_str(),
            "help" | "--help" | "-h" | "--version" | "-V"
        )
    {
        corrected.push("search".to_string());
        // If the arg looks like `query="foo"`, strip the key
        if args[0].starts_with("query=") || args[0].starts_with("q=") {
            let val = args[0].split_once('=').map(|(_, v)| v).unwrap_or(&args[0]);
            corrected.push(val.to_string());
            notes.push(format!(
                "Assumed 'search' subcommand and stripped query key from '{}'",
                args[0]
            ));
        } else {
            corrected.push(args[0].clone());
            notes.push(format!(
                "Assumed 'search' subcommand for positional argument '{}'",
                args[0]
            ));
        }
        made_correction = true;
        corrected.extend_from_slice(&args[1..]);
    } else {
        // Just copy original structure to start
        corrected.extend_from_slice(args);
    }

    // 2. Fuzzy match flags and fix key=value syntax
    let mut final_args = Vec::new();
    final_args.push(corrected[0].clone()); // prog

    for arg in corrected.iter().skip(1) {
        if arg.starts_with("--") {
            // Split --flag=value or --flag
            let (flag, value) = if let Some((f, v)) = arg.split_once('=') {
                (f, Some(v))
            } else {
                (arg.as_str(), None)
            };

            // Known flags for fuzzy matching
            let known_flags = [
                "--robot",
                "--json",
                "--limit",
                "--offset",
                "--agent",
                "--workspace",
                "--fields",
                "--max-tokens",
                "--max-sessions",
                "--max-evidence",
                "--context-lines",
                "--max-excerpt-chars",
                "--sessions-from",
                "--freshness-policy",
                "--freshness-window-seconds",
                "--require-evidence",
                "--explain-selection",
                "--request-id",
                "--cursor",
                "--since",
                "--until",
                "--days",
                "--today",
                "--week",
                "--full",
                "--watch",
                "--data-dir",
                "--verbose",
                "--quiet",
            ];

            // Check for exact match
            if known_flags.contains(&flag) {
                final_args.push(arg.clone());
                continue;
            }

            // Check for typos (levenshtein distance <= 2)
            let best_match = known_flags
                .iter()
                .min_by_key(|k| strsim::levenshtein(flag, k))
                .filter(|k| strsim::levenshtein(flag, k) <= 2);

            if let Some(&correction) = best_match {
                if let Some(v) = value {
                    final_args.push(format!("{correction}={v}"));
                } else {
                    final_args.push(correction.to_string());
                }
                notes.push(format!("Corrected typo '{flag}' to '{correction}'"));
                made_correction = true;
            } else {
                // Keep as is if no good guess
                final_args.push(arg.clone());
            }
        } else if arg.contains('=') && !arg.starts_with('-') {
            // 3. Handle `limit=5` (missing --)
            let Some((key, val)) = arg.split_once('=') else {
                final_args.push(arg.clone());
                continue;
            };
            let flag_candidate = format!("--{key}");
            // Quick check if adding -- makes it a valid flag
            let known_flags = [
                "--limit",
                "--offset",
                "--agent",
                "--workspace",
                "--days",
                "--since",
                "--until",
                "--source",
                "--output",
                "--format",
                "--model",
                "--reranker",
                "--embedder",
                "--timeout",
                "--fields",
                "--max-tokens",
                "--max-sessions",
                "--max-evidence",
                "--context-lines",
                "--max-excerpt-chars",
                "--sessions-from",
                "--freshness-policy",
                "--freshness-window-seconds",
                "--request-id",
            ];
            if known_flags.contains(&flag_candidate.as_str()) {
                final_args.push(flag_candidate);
                final_args.push(val.to_string());
                notes.push(format!(
                    "Interpreted '{arg}' as flag '{key}' with value '{val}'"
                ));
                made_correction = true;
            } else {
                final_args.push(arg.clone());
            }
        } else {
            final_args.push(arg.clone());
        }
    }

    if made_correction {
        Some((final_args, notes.join("; ")))
    } else {
        None
    }
}
