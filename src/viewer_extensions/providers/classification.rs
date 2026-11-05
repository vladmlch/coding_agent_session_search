//! Turn-level classification ported from the Native oracle
//! (`viewer/classifier/{patterns,tools}.py`, `viewer/domain/tools.py`,
//! `viewer/storage/index_pipeline/metrics.py`): per-turn task categories via
//! intent patterns + tool capability inspection, and session rollups
//! (`turn_count`, `first_prompt`, `primary category`).
//!
//! The category axis lives in `session_turn_stats`; the remaining
//! `SessionMetricsCalculator` axes (retries, corrections, one-shot rate,
//! TTFE, category distribution, tool frequency, reworked files) live in
//! [`session_quality_metrics`] and persist through `viewer_session_metrics`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

use regex::Regex;
use serde_json::Value;

// Canonical tool-name normalization (oracle `canonical_tool_name`).
fn canonical_tool_name(tool_name: &str) -> &str {
    let mut cleaned = tool_name.trim();
    if let Some(rest) = cleaned.strip_prefix("default_api:") {
        cleaned = rest;
    }
    if let Some(rest) = cleaned.strip_prefix("mcp__") {
        cleaned = rest;
    }
    if let Some((_, tail)) = cleaned.rsplit_once(':') {
        cleaned = tail.trim();
    }
    cleaned
}

/// Base lowercased identifier without namespace (oracle `clean_tool_name`).
fn clean_tool_name(tool_name: &str) -> String {
    canonical_tool_name(tool_name)
        .rsplit('.')
        .next()
        .unwrap_or("")
        .trim()
        .to_lowercase()
        .replace('-', "_")
}

const EDIT_TOOLS: &[&str] = &[
    "replace_file_content",
    "write_to_file",
    "edit",
    "write",
    "fileedittool",
    "filewritetool",
    "notebookedit",
    "cursor:edit",
    "apply_diff",
    "apply_patch",
    "str_replace_editor",
    "insert_content",
    "edit_file_v2",
    "fileedit",
];
const READ_TOOLS: &[&str] = &[
    "view_file",
    "read_file",
    "cat",
    "list_dir",
    "grep_search",
    "find_by_name",
    "read_url_content",
    "read",
    "view",
    "grep",
    "glob",
    "filereadtool",
    "greptool",
    "globtool",
    "read_file_v2",
    "ripgrep_raw_search",
    "glob_file_search",
    "filelist",
    "fileread",
];
const COMMAND_TOOLS: &[&str] = &[
    "run_command",
    "exec_command",
    "bash",
    "bashtool",
    "powershelltool",
    "exec",
    "execute_command",
    "run_terminal_cmd",
    "run_terminal_command_v2",
    "terminal",
];
const TASK_PLAN_TOOLS: &[&str] = &[
    "taskcreate",
    "taskupdate",
    "taskget",
    "tasklist",
    "todowrite",
    "enterplanmode",
    "exitplanmode",
    "manage_task",
    "todo_write",
    "plan",
];
const DELEGATION_TOOLS: &[&str] = &[
    "invoke_subagent",
    "agent",
    "task",
    "subagent",
    "send_message",
    "task_v2",
];
const BROWSER_SEARCH_TOOLS: &[&str] = &[
    "search_web",
    "websearch",
    "webfetch",
    "toolsearch",
    "browser",
];

/// Oracle `ToolCategory` reduced to the variants classification consumes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolCategory {
    Edit,
    Read,
    Command,
    Browser,
    Subagent,
    Other,
}

fn classify_tool(tool_name: &str) -> ToolCategory {
    let name = clean_tool_name(tool_name);
    if name.is_empty() {
        return ToolCategory::Other;
    }
    if EDIT_TOOLS.contains(&name.as_str())
        || ["edit", "write", "replace", "patch"]
            .iter()
            .any(|key| name.contains(key))
    {
        return ToolCategory::Edit;
    }
    if READ_TOOLS.contains(&name.as_str())
        || ["read", "view", "grep", "glob", "list", "fetch"]
            .iter()
            .any(|key| name.contains(key))
    {
        return ToolCategory::Read;
    }
    if COMMAND_TOOLS.contains(&name.as_str())
        || ["bash", "command", "exec", "terminal", "shell", "run"]
            .iter()
            .any(|key| name.contains(key))
    {
        return ToolCategory::Command;
    }
    if BROWSER_SEARCH_TOOLS.contains(&name.as_str())
        || name.contains("browser")
        || name.contains("web")
    {
        return ToolCategory::Browser;
    }
    if DELEGATION_TOOLS.contains(&name.as_str())
        || name.contains("subagent")
        || name.contains("agent")
    {
        return ToolCategory::Subagent;
    }
    ToolCategory::Other
}

/// Exact or boundary-separated suffix match (oracle `_matches_tool_suffix`).
fn matches_tool_suffix(tool_name: &str, target: &str) -> bool {
    tool_name == target
        || tool_name.ends_with(&format!("_{target}"))
        || tool_name.ends_with(&format!(":{target}"))
        || tool_name.ends_with(&format!(".{target}"))
        || tool_name.ends_with(&format!("-{target}"))
}

fn any_suffix_or_set(tool_name: &str, set: &[&str], category: ToolCategory) -> bool {
    set.contains(&tool_name)
        || set.iter().any(|tool| matches_tool_suffix(tool_name, tool))
        || classify_tool(tool_name) == category
}

const COMMAND_KEYS: &[&str] = &["command", "CommandLine", "cmd", "command_line", "script"];

/// Command string from call arguments: direct keys, then a JSON-encoded
/// arguments string (oracle `extract_command` over `input`/`arguments`/`args`/
/// `parameters` — our call model carries the decoded args directly).
fn extract_command(arguments: Option<&Value>) -> String {
    let Some(arguments) = arguments else {
        return String::new();
    };
    let lookup = |map: &serde_json::Map<String, Value>| -> Option<String> {
        for key in COMMAND_KEYS {
            if let Some(value) = map.get(*key).and_then(Value::as_str)
                && !value.trim().is_empty()
            {
                return Some(value.trim().to_string());
            }
        }
        None
    };
    if let Some(map) = arguments.as_object() {
        if let Some(command) = lookup(map) {
            return command;
        }
    } else if let Some(text) = arguments.as_str()
        && let Ok(parsed) = serde_json::from_str::<Value>(text)
        && let Some(map) = parsed.as_object()
        && let Some(command) = lookup(map)
    {
        return command;
    }
    String::new()
}

fn pattern(source: &str) -> Regex {
    Regex::new(&format!("(?i){source}")).expect("classification pattern compiles")
}

static TEST_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    pattern(
        r"\b(tests?|pytest|vitest|jest|mocha|coverage|npm\s+test|cargo\s+test|go\s+test|unittest|e2e)\b",
    )
});
static GIT_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    pattern(
        r"\bgit\s+(push|pull|commit|merge|rebase|checkout|branch|stash|diff|status|add|reset|log|fetch|clone|tag|init)\b",
    )
});
static BUILD_DEPLOY_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    pattern(
        r"\b(npm\s+run\s+build|docker|deploy|deployment|make\s+build|cargo\s+build|pip\s+install|npm\s+install|yarn\s+build|pnpm\s+build|docker-compose|docker\s+build|kubectl|helm)\b",
    )
});
static DEBUG_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    pattern(
        r"\b(fix|bug|bugs|error|errors|broken|failing|failed|crash|crashes|crashed|issue|issues|traceback|exception|not\s+working|panic)\b",
    )
});
static FEATURE_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    pattern(
        r"\b(add|create|creating|implement|implementing|new|build|building|feature|features|set\s+up|scaffold|scaffolding|generate|generating)\b",
    )
});
static REFACTOR_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    pattern(
        r"\b(refactor|refactoring|clean\s+up|cleaning\s+up|cleanup|rename|renaming|reorganize|reorganizing|simplify|simplifying|extract|extracting|restructure|restructuring)\b",
    )
});
static BRAINSTORM_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    pattern(
        r"\b(brainstorm|brainstorming|ideas?|what\s+if|explore|think\s+about|strategy|design)\b",
    )
});
static RESEARCH_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    pattern(
        r"\b(research|investigate|investigating|look\s+into|find\s+out|check|analyze|analyzing|how\s+does|what\s+is|how\s+do|why\s+is|explain)\b",
    )
});
static PLANNING_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    pattern(
        r"\b(plan|planning|roadmap|specs?|specification|architecture\s+plan|tasks|milestone|todo)\b",
    )
});
static DELEGATION_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    pattern(r"\b(subagent|sub-agent|delegate|delegation|invoke\s+agent|spawn\s+agent)\b")
});
static CONVERSATION_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        r"^(hello|hi|hey|greetings|howdy|good\s+(morning|afternoon|evening|day)|how\s+are\s+you|how\s+is\s+it\s+going|what'?s\s+up)\b",
        r"\b(thanks|thank\s+you|thx|appreciate\s+it|that'?s\s+all(\s+for\s+now)?|have\s+a\s+(good|great|nice)\s+day|bye|goodbye|see\s+you)\b",
        r"^(ok|okay|got\s+it|understood|sounds\s+good|sure|great|cool|awesome|perfect|nice|will\s+do|yes|no|yep|nope|[,.!?;:\s])+$",
        r"^(who\s+are\s+you|what\s+can\s+you\s+do|tell\s+me\s+about\s+yourself)\b",
    ]
    .iter()
    .map(|source| pattern(source))
    .collect()
});

/// A tool invocation attributed to its recording message (canonical `idx`).
pub(crate) struct ClassifyCall {
    pub event_idx: i64,
    pub name: String,
    pub arguments: Option<Value>,
}

/// Oracle `_inspect_turn_tools` + `_resolve_task_category`.
fn classify_turn_category(user_message: &str, calls: &[&ClassifyCall]) -> &'static str {
    let names: Vec<String> = calls
        .iter()
        .map(|call| clean_tool_name(&call.name))
        .collect();
    let has_edits = names
        .iter()
        .any(|name| any_suffix_or_set(name, EDIT_TOOLS, ToolCategory::Edit));
    let has_reads = names
        .iter()
        .any(|name| any_suffix_or_set(name, READ_TOOLS, ToolCategory::Read));
    let has_bash = names
        .iter()
        .any(|name| any_suffix_or_set(name, COMMAND_TOOLS, ToolCategory::Command));
    let has_tasks = names
        .iter()
        .any(|name| any_suffix_or_set(name, TASK_PLAN_TOOLS, ToolCategory::Other));
    let has_delegation = names
        .iter()
        .any(|name| any_suffix_or_set(name, DELEGATION_TOOLS, ToolCategory::Subagent));
    let has_search = names
        .iter()
        .any(|name| any_suffix_or_set(name, BROWSER_SEARCH_TOOLS, ToolCategory::Browser));
    let bash_test = calls
        .iter()
        .any(|call| TEST_PATTERN.is_match(&extract_command(call.arguments.as_ref())));
    let bash_git = calls
        .iter()
        .any(|call| GIT_PATTERN.is_match(&extract_command(call.arguments.as_ref())));
    let bash_build = calls
        .iter()
        .any(|call| BUILD_DEPLOY_PATTERN.is_match(&extract_command(call.arguments.as_ref())));

    let msg = user_message.trim();
    if has_delegation || DELEGATION_PATTERN.is_match(msg) {
        return "delegation";
    }
    if has_tasks || PLANNING_PATTERN.is_match(msg) {
        return "planning";
    }
    if bash_git || GIT_PATTERN.is_match(msg) {
        return "git";
    }
    if bash_build || BUILD_DEPLOY_PATTERN.is_match(msg) {
        return "build_deploy";
    }
    if bash_test || TEST_PATTERN.is_match(msg) {
        return "testing";
    }
    if DEBUG_PATTERN.is_match(msg) {
        return "debugging";
    }
    if REFACTOR_PATTERN.is_match(msg) {
        return "refactoring";
    }
    if FEATURE_PATTERN.is_match(msg) {
        return "feature";
    }
    if BRAINSTORM_PATTERN.is_match(msg) && !has_edits {
        return "brainstorming";
    }
    if RESEARCH_PATTERN.is_match(msg) && !has_edits {
        return "exploration";
    }
    if has_edits || has_bash {
        return "coding";
    }
    if has_search || has_reads {
        return "exploration";
    }
    if msg.is_empty() || CONVERSATION_PATTERNS.iter().any(|p| p.is_match(msg)) {
        return "conversation";
    }
    "general"
}

// ---------------------------------------------------------------------------
// Quality metrics (Task 10): retries, corrections, one-shot rate, TTFE,
// category distribution, tool frequency and reworked files — the second
// half of the `SessionMetricsCalculator.calculate` oracle port. The
// category/turn rollups stay in `session_turn_stats`; this sibling walks
// the same deduplicated events and calls once for the metric axes that
// feed `viewer_session_metrics`.
// ---------------------------------------------------------------------------

/// Read-shaped shell commands that do NOT count as an edit verification
/// step (oracle `_READ_COMMAND_TOKENS`).
const READ_COMMAND_TOKENS: &[&str] = &[
    "cat", "head", "tail", "grep", "rg", "ls", "find", "echo", "which", "pwd", "less", "more",
    "bat", "wc", "file",
];
/// `git` read subcommands (oracle `_GIT_READ_PREFIXES`).
const GIT_READ_PREFIXES: &[&str] = &[
    "git log",
    "git status",
    "git diff",
    "git show",
    "git branch",
    "git remote",
];

/// Oracle `is_read_command`: read/lookup commands are not verifications;
/// redirection writes and any other command count as verification.
fn is_read_command(command: &str) -> bool {
    let clean = command.trim();
    if clean.is_empty() {
        return true;
    }
    let lower = clean.to_lowercase();
    if GIT_READ_PREFIXES.iter().any(|p| lower.starts_with(p)) {
        return true;
    }
    if clean.contains('>') {
        return false;
    }
    let first = lower.split_whitespace().next().unwrap_or("");
    let basename = first.rsplit('/').next().unwrap_or(first);
    READ_COMMAND_TOKENS.contains(&basename)
}

/// Oracle `extract_target_file_path` over the decoded argument object (the
/// same key list `viewer/domain/tools.py` uses), tolerating an arguments
/// string that is itself JSON.
fn extract_target_file_path(arguments: Option<&Value>) -> Option<String> {
    const KEYS: &[&str] = &[
        "TargetFile",
        "targetFile",
        "target_file",
        "file_path",
        "filePath",
        "path",
        "file",
        "filename",
        "AbsolutePath",
        "absolute_path",
    ];
    let lookup = |map: &serde_json::Map<String, Value>| -> Option<String> {
        for key in KEYS {
            if let Some(value) = map.get(*key)
                && let Some(text) = value.as_str()
                && !text.is_empty()
            {
                return Some(text.to_string());
            }
        }
        None
    };
    match arguments? {
        Value::Object(map) => lookup(map),
        Value::String(text) => match serde_json::from_str::<Value>(text) {
            Ok(Value::Object(map)) => lookup(&map),
            _ => None,
        },
        _ => None,
    }
}

/// Oracle `detect_file_retries`: an edit to a file that was already edited
/// and then "verified" by a non-read command counts as one retry.
fn detect_file_retries(calls: &[&ClassifyCall]) -> i64 {
    let mut edited: BTreeSet<String> = BTreeSet::new();
    let mut verified: BTreeSet<String> = BTreeSet::new();
    let mut retries = 0_i64;
    for call in calls {
        let name = clean_tool_name(&call.name);
        let is_edit = EDIT_TOOLS.contains(&name.as_str())
            || EDIT_TOOLS.iter().any(|t| matches_tool_suffix(&name, t))
            || classify_tool(&name) == ToolCategory::Edit;
        if is_edit {
            let path = extract_target_file_path(call.arguments.as_ref())
                .unwrap_or_else(|| "__unknown_file__".into());
            if verified.contains(&path) {
                retries += 1;
                verified.remove(&path);
            }
            edited.insert(path);
            continue;
        }
        let is_bash = COMMAND_TOOLS.contains(&name.as_str())
            || COMMAND_TOOLS.iter().any(|t| matches_tool_suffix(&name, t))
            || classify_tool(&name) == ToolCategory::Command;
        if is_bash && !is_read_command(&extract_command(call.arguments.as_ref())) {
            verified.extend(edited.iter().cloned());
        }
    }
    retries
}

static SELF_CORRECTION_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        r"\b(i\s+apologize|my\s+apologies|sorry\s+about\s+that|sorry\s+for\s+the|pardon\s+me)\b",
        r"\b(i\s+made\s+a\s+mistake|i\s+made\s+an\s+error|my\s+mistake|my\s+bad|that\s+was\s+my\s+mistake)\b",
        r"\b(let\s+me\s+correct|correcting\s+(this|that|my)|i\s+was\s+mistaken|i\s+incorrectly)\b",
        r"\b(i\s+misunderstood|i\s+realize\s+(now\s+)?that\s+i|i\s+overlooked)\b",
        r"\b(actually,?\s+(that\s+is|that's|this\s+is|it\s+is)\s+(not|incorrect|wrong))\b",
        r"\b(that\s+was\s+(incorrect|wrong|a\s+mistake))\b",
        r"\b(my\s+previous\s+(code|fix|answer|response|attempt)\s+(was|had|introduced))\b",
    ]
    .iter()
    .map(|source| pattern(source))
    .collect()
});

static USER_CORRECTION_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        r"\b(not\s+what\s+i\s+(asked|wanted|meant|requested|said))\b",
        r"\b(that'?s\s+not\s+(right|correct|what))\b",
        r"\b(please\s+)?(revert|undo|rollback|roll\s+back)\s+(that|this|the\s+change|changes)\b",
        r"\b(that'?s\s+wrong|that\s+is\s+wrong|that'?s\s+incorrect|that\s+is\s+incorrect)\b",
        r"\b(you\s+broke\s+(it|the|something)|you\s+messed\s+up)\b",
        r"\b(i\s+didn'?t\s+ask\s+(you\s+)?to|i\s+told\s+you\s+to|i\s+specifically\s+asked)\b",
        r"\b(still\s+(broken|failing|not\s+working|gives?\s+an?\s+error|failing\s+tests?|errors?))\b",
        r"\b(that\s+didn'?t\s+work|that\s+doesn'?t\s+work|that\s+failed)\b",
        r"^\s*no[,.!]\s+",
        r"\b(no,?\s+instead|no,?\s+use|no,?\s+do\s+not|no,?\s+don'?t)\b",
        r"\b(wrong\s+(file|function|variable|path|directory|folder|class|method|approach))\b",
    ]
    .iter()
    .map(|source| pattern(source))
    .collect()
});

/// One deduplicated fact event for the quality pass: canonical `idx`, the
/// recorded timestamp (ms, for TTFE), role and content.
pub(crate) struct QualityEvent<'a> {
    pub idx: i64,
    pub created_at: Option<i64>,
    pub role: &'a str,
    pub content: &'a str,
}

/// The metric half of the Native `tools_data` bundle. Every count is
/// measured evidence; `one_shot_rate`/`ttfe_seconds` stay `None` when the
/// oracle defines them as unmeasured (no user turn / no edit timestamp).
#[derive(Debug, Clone)]
pub struct SessionQualityMetrics {
    pub user_turn_count: i64,
    pub retries_count: i64,
    pub self_corrections_count: i64,
    pub user_corrections_count: i64,
    /// `metrics_data["one_shot_rate"]` — rounded to one decimal like Native.
    pub one_shot_rate: Option<f64>,
    /// `metrics_data["ttfe_seconds"]` — rounded to two decimals like Native.
    pub ttfe_seconds: Option<f64>,
    /// `category_distribution` — turn counts per category.
    pub category_distribution: BTreeMap<String, i64>,
    /// `tools_frequency` (invocation counts) + `tools_used` key order.
    pub tools_frequency: Vec<(String, i64)>,
    /// `reworked_files` — edit counts per file path.
    pub reworked_files: BTreeMap<String, i64>,
}

/// Oracle `SessionMetricsCalculator.calculate`, second half: walk the same
/// deduplicated events/calls as `session_turn_stats` and fold the quality
/// axes. Calls are re-ordered by `event_idx` because the collector groups
/// them by call id, not chronologically — retry detection is order-bound.
///
/// Parity note: the oracle drops every step up to and including the first
/// `FORK_BOUNDARY` before computing metrics. No connector records a fork
/// boundary in canonical extras today, so the unsliced walk is exact for
/// every provider CASS ingests.
pub(crate) fn session_quality_metrics(
    events: &[QualityEvent<'_>],
    calls: &[ClassifyCall],
) -> SessionQualityMetrics {
    let mut ordered_calls: Vec<&ClassifyCall> = calls.iter().collect();
    ordered_calls.sort_by_key(|call| call.event_idx);

    // tools_frequency + reworked_files fold over ALL calls (the oracle's
    // second pass iterates every step, not only classified turns).
    let mut tools_frequency: Vec<(String, i64)> = Vec::new();
    let mut reworked_files: BTreeMap<String, i64> = BTreeMap::new();
    for call in calls {
        let name = canonical_tool_name(&call.name).to_string();
        if name.is_empty() {
            continue;
        }
        match tools_frequency.iter_mut().find(|(n, _)| *n == name) {
            Some((_, count)) => *count += 1,
            None => tools_frequency.push((name, 1)),
        }
        if classify_tool(&call.name) == ToolCategory::Edit
            && let Some(path) = extract_target_file_path(call.arguments.as_ref())
            && !path.is_empty()
        {
            *reworked_files.entry(path).or_insert(0) += 1;
        }
    }

    let mut user_turn_count = 0_i64;
    let mut retries = 0_i64;
    let mut self_corrections = 0_i64;
    let mut user_corrections = 0_i64;
    let mut category_distribution: BTreeMap<String, i64> = BTreeMap::new();
    let mut turn_timestamps: Vec<f64> = Vec::new();
    let mut edit_timestamps: Vec<f64> = Vec::new();

    let mut current_prompt = String::new();
    let mut current_calls: Vec<&ClassifyCall> = Vec::new();
    let mut current_assistant_text = String::new();
    let flush = |prompt: &str,
                 turn_calls: &mut Vec<&ClassifyCall>,
                 assistant_text: &str,
                 counts: &mut BTreeMap<String, i64>,
                 retries_acc: &mut i64,
                 self_acc: &mut i64,
                 user_acc: &mut i64| {
        if !prompt.is_empty() || !turn_calls.is_empty() {
            let category = classify_turn_category(prompt, turn_calls);
            *counts.entry(category.to_string()).or_insert(0) += 1;
            *retries_acc += detect_file_retries(turn_calls);
            if SELF_CORRECTION_PATTERNS
                .iter()
                .any(|p| p.is_match(assistant_text))
            {
                *self_acc += 1;
            }
            if USER_CORRECTION_PATTERNS.iter().any(|p| p.is_match(prompt)) {
                *user_acc += 1;
            }
        }
    };

    for event in events {
        if let Some(created_at) = event.created_at {
            turn_timestamps.push(created_at as f64 / 1000.0);
        }
        let has_edit = ordered_calls.iter().any(|call| {
            call.event_idx == event.idx && classify_tool(&call.name) == ToolCategory::Edit
        });
        if has_edit && let Some(created_at) = event.created_at {
            edit_timestamps.push(created_at as f64 / 1000.0);
        }
        if event.role == "user" {
            flush(
                &current_prompt,
                &mut current_calls,
                &current_assistant_text,
                &mut category_distribution,
                &mut retries,
                &mut self_corrections,
                &mut user_corrections,
            );
            user_turn_count += 1;
            current_prompt = event.content.to_string();
            current_calls.clear();
            current_calls.extend(
                ordered_calls
                    .iter()
                    .filter(|call| call.event_idx == event.idx)
                    .copied(),
            );
            current_assistant_text.clear();
        } else {
            current_calls.extend(
                ordered_calls
                    .iter()
                    .filter(|call| call.event_idx == event.idx)
                    .copied(),
            );
            current_assistant_text.push(' ');
            current_assistant_text.push_str(event.content);
        }
    }
    flush(
        &current_prompt,
        &mut current_calls,
        &current_assistant_text,
        &mut category_distribution,
        &mut retries,
        &mut self_corrections,
        &mut user_corrections,
    );

    let one_shot_rate = if user_turn_count == 0 {
        None
    } else if user_turn_count == 1 && retries == 0 {
        Some(100.0)
    } else {
        let rate = (1.0 / (user_turn_count as f64 + retries as f64 * 0.2)) * 100.0;
        Some((rate.clamp(0.0, 100.0) * 10.0).round() / 10.0)
    };

    let ttfe_seconds = if turn_timestamps.is_empty() || edit_timestamps.is_empty() {
        None
    } else {
        let first_turn = turn_timestamps
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        let first_edit = edit_timestamps
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        Some(((first_edit - first_turn).max(0.0) * 100.0).round() / 100.0)
    };

    SessionQualityMetrics {
        user_turn_count,
        retries_count: retries,
        self_corrections_count: self_corrections,
        user_corrections_count: user_corrections,
        one_shot_rate,
        ttfe_seconds,
        category_distribution,
        tools_frequency,
        reworked_files,
    }
}

/// Session rollups over deduplicated events:
/// `(turn_count, step_count, first_prompt, task_category)`.
///
/// Oracle `SessionMetricsCalculator.calculate`: every user message closes
/// the previous turn; tool calls and assistant text accumulate onto the open
/// turn; the session category is the most common turn category (first-seen
/// tie-break, matching `Counter.most_common`).
pub(crate) fn session_turn_stats(
    events: &[(i64, &str, &str)],
    calls: &[ClassifyCall],
) -> (Option<i64>, Option<i64>, Option<String>, Option<String>) {
    let mut turn_count = 0_i64;
    let mut step_count = 0_i64;
    let mut first_prompt: Option<String> = None;
    let mut category_counts: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut category_order: Vec<&'static str> = Vec::new();

    let mut current_prompt = String::new();
    let mut current_calls: Vec<&ClassifyCall> = Vec::new();
    let flush = |prompt: &str,
                 calls: &mut Vec<&ClassifyCall>,
                 counts: &mut BTreeMap<&'static str, usize>,
                 order: &mut Vec<&'static str>| {
        if !prompt.is_empty() || !calls.is_empty() {
            let category = classify_turn_category(prompt, calls);
            if *counts.entry(category).or_insert(0) == 0 {
                order.push(category);
            }
            *counts.entry(category).or_insert(0) += 1;
        }
    };

    for &(idx, role, content) in events {
        if role == "user" {
            flush(
                &current_prompt,
                &mut current_calls,
                &mut category_counts,
                &mut category_order,
            );
            turn_count += 1;
            current_prompt = content.to_string();
            current_calls.clear();
            if first_prompt.is_none() && !content.trim().is_empty() {
                first_prompt = Some(content.to_string());
            }
            // Calls recorded on the user message itself belong to the new turn.
            current_calls.extend(calls.iter().filter(|call| call.event_idx == idx));
        } else {
            step_count += 1;
            current_calls.extend(calls.iter().filter(|call| call.event_idx == idx));
        }
    }
    flush(
        &current_prompt,
        &mut current_calls,
        &mut category_counts,
        &mut category_order,
    );

    let task_category = category_order
        .iter()
        .max_by_key(|category| category_counts[*category])
        .copied()
        .map(str::to_string);
    (
        Some(turn_count),
        Some(step_count),
        first_prompt,
        task_category,
    )
}
