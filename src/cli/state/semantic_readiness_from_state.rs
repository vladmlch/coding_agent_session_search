use super::state_meta_json::readiness_snapshot_from_state;
use std::path::Path;

pub(crate) fn semantic_readiness_from_state(
    state: &serde_json::Value,
) -> crate::search::readiness::SemanticReadinessState {
    use crate::search::readiness::SemanticReadinessState;

    let semantic = state.get("semantic");
    let availability = semantic
        .and_then(|sem| sem.get("availability"))
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    if availability == "disabled" {
        return SemanticReadinessState::PolicyDisabled;
    }

    let can_search = semantic
        .and_then(|sem| sem.get("can_search"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let fast_ready = semantic
        .and_then(|sem| sem.get("fast_tier"))
        .and_then(|tier| tier.get("ready"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
        && semantic
            .and_then(|sem| sem.get("fast_tier"))
            .and_then(|tier| tier.get("current_db_matches"))
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
    let quality_ready = semantic
        .and_then(|sem| sem.get("quality_tier"))
        .and_then(|tier| tier.get("ready"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
        && semantic
            .and_then(|sem| sem.get("quality_tier"))
            .and_then(|tier| tier.get("current_db_matches"))
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
    let checkpoint_active = semantic
        .and_then(|sem| sem.get("checkpoint"))
        .and_then(|checkpoint| checkpoint.get("active"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let backlog_pending = semantic
        .and_then(|sem| sem.get("backlog"))
        .and_then(|backlog| backlog.get("pending_work"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false);

    if quality_ready && (fast_ready || can_search) {
        return SemanticReadinessState::HybridReady;
    }
    if fast_ready || can_search {
        return SemanticReadinessState::FastTierReady;
    }
    if checkpoint_active || backlog_pending || availability == "index_building" {
        return SemanticReadinessState::Backfilling;
    }

    SemanticReadinessState::Absent
}

pub(crate) fn semantic_recommended_action(
    state: &serde_json::Value,
    not_initialized: bool,
) -> Option<String> {
    use crate::search::readiness::RecommendedAction;

    match readiness_snapshot_from_state(state, not_initialized).recommended_action() {
        RecommendedAction::WaitForSemanticCatchUp => Some(
            "Lexical search is ready; semantic assets are still catching up. Continue with lexical results or wait for hybrid refinement to finish."
                .to_string(),
        ),
        RecommendedAction::SemanticDisabledByPolicy => Some(
            "Semantic refinement is disabled by policy; continue with lexical search or re-enable semantic mode for hybrid results."
                .to_string(),
        ),
        _ => None,
    }
}

pub(crate) fn shell_quote_arg(arg: &str) -> String {
    if !arg.is_empty()
        && arg
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ':' | '='))
    {
        arg.to_string()
    } else {
        format!("'{}'", arg.replace('\'', "'\"'\"'"))
    }
}

pub(crate) fn cass_dataset_command(
    data_dir: &Path,
    db_path: &Path,
    subcommand_args: &[&str],
) -> String {
    let mut parts = vec!["cass".to_string()];
    let default_db_path = data_dir.join("agent_search.db");
    if db_path != default_db_path {
        parts.push("--db".to_string());
        parts.push(shell_quote_arg(&db_path.display().to_string()));
    }
    parts.extend(subcommand_args.iter().map(|arg| (*arg).to_string()));
    parts.push("--data-dir".to_string());
    parts.push(shell_quote_arg(&data_dir.display().to_string()));
    parts.join(" ")
}

pub(crate) struct ReadinessCommandRecommendation<'a> {
    pub(crate) id: &'a str,
    pub(crate) command: String,
    pub(crate) purpose: &'a str,
    pub(crate) safety: &'a str,
    pub(crate) run_when: &'a str,
    pub(crate) success_signal: &'a str,
    pub(crate) parse_fields: &'a [&'a str],
    pub(crate) retry_after_ms: Option<u64>,
}

pub(crate) fn readiness_command_recommendation(
    recommendation: ReadinessCommandRecommendation<'_>,
) -> serde_json::Value {
    serde_json::json!({
        "id": recommendation.id,
        "command": recommendation.command,
        "purpose": recommendation.purpose,
        "safety": recommendation.safety,
        "run_when": recommendation.run_when,
        "success_signal": recommendation.success_signal,
        "parse_fields": recommendation.parse_fields,
        "retry_after_ms": recommendation.retry_after_ms,
    })
}

pub(crate) fn readiness_recommended_commands(
    data_dir: &Path,
    db_path: &Path,
    state: &serde_json::Value,
    status: &str,
    healthy: bool,
    not_initialized: bool,
    recommended_action: Option<&str>,
) -> Vec<serde_json::Value> {
    let index_exists = state
        .get("index")
        .and_then(|index| index.get("exists"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let index_fresh = state
        .get("index")
        .and_then(|index| index.get("fresh"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let index_empty_with_messages = state
        .get("index")
        .and_then(|index| index.get("empty_with_messages"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let db_exists = state
        .get("database")
        .and_then(|database| database.get("exists"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let db_opened = state
        .get("database")
        .and_then(|database| database.get("opened"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let db_open_retryable = state
        .get("database")
        .and_then(|database| database.get("open_retryable"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let rebuild_active = state
        .get("rebuild")
        .and_then(|rebuild| rebuild.get("active"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let pending_sessions = state
        .get("pending")
        .and_then(|pending| pending.get("sessions"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);

    // b4uax: legacy Tantivy generation — the only fix is a full rebuild.
    let index_engine_incompatible = state
        .get("index")
        .and_then(|index| index.get("engine_incompatible"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);

    let mut commands = Vec::new();
    if rebuild_active {
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "poll-active-rebuild",
                command: cass_dataset_command(data_dir, db_path, &["status", "--json"]),
                purpose: "Poll the active rebuild without starting a second writer.",
                safety: "read-only",
                run_when: "Run after a short delay while rebuild_progress.active is true.",
                success_signal: "rebuild_progress.active=false",
                parse_fields: &[
                    "status",
                    "rebuild.active",
                    "rebuild_progress",
                    "recommended_commands",
                ],
                retry_after_ms: Some(30_000),
            },
        ));
        return commands;
    }

    if not_initialized || (!db_exists && !index_exists) {
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "initialize-archive",
                command: cass_dataset_command(
                    data_dir,
                    db_path,
                    &["index", "--full", "--json", "--no-progress-events"],
                ),
                purpose: "Discover local sessions and build the initial cass archive plus lexical index.",
                safety: "writes-cass-archive-and-derived-index",
                run_when: "Run when status is not_initialized for this data_dir.",
                success_signal: "success=true, then cass health reports initialized=true",
                parse_fields: &["success", "conversations", "messages", "error"],
                retry_after_ms: None,
            },
        ));
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "verify-initialization",
                command: cass_dataset_command(data_dir, db_path, &["health", "--json"]),
                purpose: "Verify the same data_dir is ready after the initial index run.",
                safety: "read-only",
                run_when: "Run after initialize-archive finishes.",
                success_signal: "healthy=true or recommended_commands changes to the next blocker",
                parse_fields: &[
                    "status",
                    "healthy",
                    "initialized",
                    "errors",
                    "recommended_commands",
                ],
                retry_after_ms: None,
            },
        ));
        return commands;
    }

    if index_engine_incompatible && !not_initialized && db_exists {
        // Deterministic, known fix: no doctor inspection needed first, and an
        // incremental `cass index` cannot help (it would refresh an index the
        // engine cannot open).
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "rebuild-lexical-index-for-current-engine",
                command: cass_dataset_command(
                    data_dir,
                    db_path,
                    &["index", "--full", "--json", "--no-progress-events"],
                ),
                purpose: "Rebuild the lexical index for the current search engine; the existing generation was written by the legacy Tantivy engine and cannot be read.",
                safety: "writes-derived-index",
                run_when: "Run once after upgrading past the Tantivy->Quill engine flip; expect a full rebuild whose cost scales with message count.",
                success_signal: "success=true, then status reports index.status=ready and index.engine_incompatible=false",
                parse_fields: &["success", "indexing_stats", "error"],
                retry_after_ms: None,
            },
        ));
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "verify-lexical-index",
                command: cass_dataset_command(data_dir, db_path, &["status", "--json"]),
                purpose: "Verify lexical readiness after the engine-flip rebuild.",
                safety: "read-only",
                run_when: "Run after rebuild-lexical-index-for-current-engine finishes.",
                success_signal: "healthy=true or status changes to the next blocker",
                parse_fields: &["status", "healthy", "index", "recommended_commands"],
                retry_after_ms: None,
            },
        ));
        return commands;
    }

    if status == "degraded" || (db_exists && !db_opened && !db_open_retryable) {
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "inspect-archive-health",
                command: cass_dataset_command(data_dir, db_path, &["doctor", "--json"]),
                purpose: "Inspect database/index health before choosing a repair path.",
                safety: "read-only",
                run_when: "Run when the database exists but health/status cannot open it.",
                success_signal: "doctor JSON identifies repair-safe next actions",
                parse_fields: &[
                    "status",
                    "health_class",
                    "recommended_action",
                    "checks",
                    "operation_state",
                ],
                retry_after_ms: None,
            },
        ));
        return commands;
    }

    if !index_exists || index_empty_with_messages {
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "rebuild-lexical-index",
                command: cass_dataset_command(
                    data_dir,
                    db_path,
                    &["index", "--full", "--json", "--no-progress-events"],
                ),
                purpose: "Rebuild the lexical search index from the cass archive.",
                safety: "writes-derived-index",
                run_when: "Run when the archive exists but the lexical index is missing or empty.",
                success_signal: "success=true, then health/status report index.exists=true",
                parse_fields: &["success", "indexing_stats", "error"],
                retry_after_ms: None,
            },
        ));
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "verify-lexical-index",
                command: cass_dataset_command(data_dir, db_path, &["status", "--json"]),
                purpose: "Verify lexical readiness after rebuilding derived index assets.",
                safety: "read-only",
                run_when: "Run after rebuild-lexical-index finishes.",
                success_signal: "healthy=true or status changes to the next blocker",
                parse_fields: &["status", "healthy", "index", "recommended_commands"],
                retry_after_ms: None,
            },
        ));
        return commands;
    }

    if !index_fresh || pending_sessions > 0 {
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "refresh-lexical-index",
                command: cass_dataset_command(
                    data_dir,
                    db_path,
                    &["index", "--json", "--no-progress-events"],
                ),
                purpose: "Index newly discovered sessions without a full rebuild.",
                safety: "writes-cass-archive-and-derived-index",
                run_when: "Run when the index is stale or pending.sessions is nonzero.",
                success_signal: "success=true, then pending.sessions=0 and index.fresh=true",
                parse_fields: &["success", "conversations", "messages", "error"],
                retry_after_ms: None,
            },
        ));
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "verify-refresh",
                command: cass_dataset_command(data_dir, db_path, &["health", "--json"]),
                purpose: "Confirm the refresh cleared the readiness blocker.",
                safety: "read-only",
                run_when: "Run after refresh-lexical-index finishes.",
                success_signal: "healthy=true or recommended_commands changes to the next blocker",
                parse_fields: &["status", "healthy", "state.index", "recommended_commands"],
                retry_after_ms: None,
            },
        ));
        return commands;
    }

    if recommended_action.is_some_and(|action| action.contains("semantic assets")) {
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "search-lexical-while-semantic-catches-up",
                command: cass_dataset_command(
                    data_dir,
                    db_path,
                    &[
                        "search",
                        "'<query>'",
                        "--robot",
                        "--mode",
                        "lexical",
                        "--limit",
                        "10",
                        "--fields",
                        "summary",
                        "--robot-meta",
                    ],
                ),
                purpose: "Continue with lexical results while optional semantic assets catch up.",
                safety: "read-only",
                run_when: "Run when lexical search is ready but semantic refinement is still backfilling.",
                success_signal: "results[] plus realized.fallback_mode=lexical",
                parse_fields: &["results", "realized", "_meta"],
                retry_after_ms: None,
            },
        ));
        return commands;
    }

    if healthy {
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "search-now",
                command: cass_dataset_command(
                    data_dir,
                    db_path,
                    &[
                        "search",
                        "'<query>'",
                        "--robot",
                        "--limit",
                        "10",
                        "--fields",
                        "summary",
                        "--robot-meta",
                    ],
                ),
                purpose: "Run a bounded machine-readable search against the ready archive.",
                safety: "read-only",
                run_when: "Run when healthy=true.",
                success_signal: "results[] plus realized.search_mode",
                parse_fields: &["results", "realized", "_meta"],
                retry_after_ms: None,
            },
        ));
        commands.push(readiness_command_recommendation(
            ReadinessCommandRecommendation {
                id: "build-answer-pack",
                command: cass_dataset_command(
                    data_dir,
                    db_path,
                    &[
                        "pack",
                        "'<query>'",
                        "--robot",
                        "--fields",
                        "summary,health,freshness,privacy,warnings",
                        "--max-tokens",
                        "4000",
                    ],
                ),
                purpose: "Build a compact evidence pack for handoff to another agent.",
                safety: "read-only",
                run_when: "Run when healthy=true and you need source-backed context.",
                success_signal: "pack summary plus health/freshness/privacy/warnings",
                parse_fields: &["pack", "health", "freshness", "privacy", "warnings"],
                retry_after_ms: None,
            },
        ));
    }

    commands
}
