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

pub(crate) fn response_schema_search_hit() -> serde_json::Value {
    response_schema_object([
        ("source_path", serde_json::json!({ "type": "string" })),
        (
            "conversation_id",
            serde_json::json!({
                "type": ["integer", "null"],
                "minimum": 1,
                "description": "Canonical conversation identity in the archive used by search. Pass with source_id, source_path and line_number to view/expand --message-index. Null means no canonical identity is available."
            }),
        ),
        (
            "line_number",
            serde_json::json!({
                "type": ["integer", "null"],
                "minimum": 1,
                "description": "Canonical message index (messages.idx + 1), not a physical file line. Pass unchanged to view/expand --message-index with source_id and conversation_id."
            }),
        ),
        ("agent", serde_json::json!({ "type": "string" })),
        (
            "workspace",
            serde_json::json!({ "type": ["string", "null"] }),
        ),
        (
            "workspace_original",
            serde_json::json!({
                "type": ["string", "null"],
                "description": "Original workspace path before remote path mapping"
            }),
        ),
        ("title", serde_json::json!({ "type": ["string", "null"] })),
        ("content", serde_json::json!({ "type": ["string", "null"] })),
        ("snippet", serde_json::json!({ "type": ["string", "null"] })),
        ("score", serde_json::json!({ "type": ["number", "null"] })),
        (
            "created_at",
            serde_json::json!({ "type": ["integer", "string", "null"] }),
        ),
        (
            "match_type",
            serde_json::json!({ "type": ["string", "null"] }),
        ),
        (
            "source_id",
            serde_json::json!({
                "type": "string",
                "description": "Source identifier (e.g., 'local', 'work-laptop')"
            }),
        ),
        (
            "origin_kind",
            serde_json::json!({
                "type": "string",
                "description": "Origin kind ('local' or 'ssh')"
            }),
        ),
        (
            "origin_host",
            serde_json::json!({
                "type": ["string", "null"],
                "description": "Host label for remote sources"
            }),
        ),
    ])
}

pub(crate) fn response_schema_search_cache_stats() -> serde_json::Value {
    response_schema_object([
        ("hits", serde_json::json!({ "type": "integer" })),
        ("misses", serde_json::json!({ "type": "integer" })),
        ("shortfall", serde_json::json!({ "type": "integer" })),
        (
            "outcomes",
            response_schema_object([
                ("hit", serde_json::json!({ "type": "integer" })),
                ("cold_miss", serde_json::json!({ "type": "integer" })),
                (
                    "stale_generation_miss",
                    serde_json::json!({ "type": "integer" }),
                ),
                ("forced_reload", serde_json::json!({ "type": "integer" })),
                ("reload_failure", serde_json::json!({ "type": "integer" })),
                ("fallback", serde_json::json!({ "type": "integer" })),
            ]),
        ),
        (
            "prewarm_scheduled",
            serde_json::json!({ "type": "integer" }),
        ),
        (
            "prewarm_skipped_pressure",
            serde_json::json!({ "type": "integer" }),
        ),
    ])
}

pub(crate) fn response_schema_search_timing() -> serde_json::Value {
    response_schema_object([
        ("search_ms", serde_json::json!({ "type": "integer" })),
        ("rerank_ms", serde_json::json!({ "type": "integer" })),
        ("other_ms", serde_json::json!({ "type": "integer" })),
    ])
}

pub(crate) fn response_schema_query_plan() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Budgeted query cost plan showing planned vs realized phases, budget exhaustion, cache behavior, and cursor identity continuity.",
        "properties": {
            "schema_version": { "type": "string" },
            "planner_id": { "type": "string" },
            "phases": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "phase": { "type": "string" },
                        "planned": { "type": "boolean" },
                        "realized": { "type": "boolean" },
                        "budget": {
                            "type": "object",
                            "properties": {
                                "limit": { "type": ["integer", "null"] },
                                "offset": { "type": ["integer", "null"] },
                                "max_tokens": { "type": ["integer", "null"] },
                                "timeout_ms": { "type": ["integer", "null"] }
                            }
                        },
                        "reason": { "type": "string" }
                    }
                }
            },
            "budget_exhaustion": {
                "type": ["object", "null"],
                "properties": {
                    "kind": { "type": "string" },
                    "reason": { "type": "string" }
                }
            },
            "result_identity": {
                "type": "object",
                "properties": {
                    "input_cursor_present": { "type": "boolean" },
                    "next_cursor_present": { "type": "boolean" },
                    "cursor_continuation": { "type": "boolean" },
                    "offset": { "type": "integer" },
                    "limit": { "type": "integer" },
                    "returned_count": { "type": "integer" },
                    "total_matches": { "type": "integer" },
                    "continuity_key": { "type": "string" },
                    "reason": { "type": "string" }
                }
            },
            "cache": {
                "type": "object",
                "properties": {
                    "eligible": { "type": "boolean" },
                    "hits": { "type": "integer" },
                    "misses": { "type": "integer" },
                    "shortfall": { "type": "integer" },
                    "reason": { "type": "string" }
                }
            },
            "summary": { "type": "string" }
        },
        "required": [
            "schema_version",
            "planner_id",
            "phases",
            "budget_exhaustion",
            "result_identity",
            "cache",
            "summary"
        ]
    })
}

pub(crate) fn response_schema_cursor_manifest() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Durable cursor contract for large robot search pages, including count precision, field projection, generation hints, and continuation safety.",
        "properties": {
            "schema_version": { "type": "integer" },
            "has_more": { "type": "boolean" },
            "count_precision": { "type": "string" },
            "count_reason": { "type": "string" },
            "continuation_safe": { "type": "boolean" },
            "continuation_reason": { "type": "string" },
            "input_cursor_present": { "type": "boolean" },
            "next_cursor_present": { "type": "boolean" },
            "offset": { "type": "integer" },
            "next_offset": { "type": ["integer", "null"] },
            "requested_limit": { "type": "integer" },
            "realized_limit": { "type": "integer" },
            "returned_count": { "type": "integer" },
            "search_page_count": { "type": "integer" },
            "total_matches": { "type": "integer", "description": "Same value as the top-level total_matches; exact only when count_precision is `exact`, otherwise the page-window lower bound `offset + returned + (1 if has_more)`." },
            "field_mask": {
                "type": "object",
                "properties": {
                    "requested": { "type": ["array", "null"], "items": { "type": "string" } },
                    "resolved": {},
                    "projection": { "type": "string" }
                }
            },
            "token_budget": {
                "type": "object",
                "properties": {
                    "max_tokens": { "type": ["integer", "null"] },
                    "tokens_estimated": { "type": ["integer", "null"] },
                    "hits_clamped": { "type": "boolean" }
                }
            },
            "cache_generation": {
                "type": "object",
                "properties": {
                    "reader_generation": { "type": ["integer", "null"] },
                    "reloads": { "type": "integer" },
                    "reload_ms_total": { "type": "integer" },
                    "policy": { "type": "string" },
                    "ghost_entries": { "type": "integer" },
                    "admission_rejects": { "type": "integer" }
                }
            },
            "index_generation": {
                "type": "object",
                "properties": {
                    "lexical_shard_generation": { "type": ["string", "null"] },
                    "freshness": {},
                    "stale": { "type": ["boolean", "null"] },
                    "rebuilding": { "type": "boolean" },
                    "pending_sessions": { "type": ["integer", "null"] }
                }
            },
            "semantic_fallback": {
                "type": "object",
                "properties": {
                    "requested_search_mode": { "type": "string" },
                    "realized_search_mode": { "type": "string" },
                    "fallback_tier": { "type": ["string", "null"] },
                    "fallback_reason": { "type": ["string", "null"] },
                    "semantic_refinement": { "type": "boolean" }
                }
            }
        },
        "required": [
            "schema_version",
            "has_more",
            "count_precision",
            "continuation_safe",
            "field_mask",
            "cache_generation",
            "index_generation",
            "semantic_fallback"
        ]
    })
}

pub(crate) fn response_schema_explanation_cards() -> serde_json::Value {
    serde_json::json!({
        "type": "array",
        "description": "Compact robot-visible decision cards explaining search, fallback, cache, rebuild, and source-sync controller behavior.",
        "items": {
            "type": "object",
            "properties": {
                "schema_version": { "type": "integer" },
                "card_id": { "type": "string" },
                "surface": { "type": "string" },
                "decision": { "type": "string" },
                "level": { "type": "integer" },
                "summary": { "type": "string" },
                "inputs": {
                    "type": "object",
                    "additionalProperties": true
                },
                "evidence": {
                    "type": "object",
                    "additionalProperties": true
                },
                "fallback_contract": {
                    "type": ["object", "null"],
                    "additionalProperties": true
                }
            },
            "required": [
                "schema_version",
                "card_id",
                "surface",
                "decision",
                "level",
                "summary",
                "inputs",
                "evidence"
            ]
        }
    })
}

/// JSON Schema for the uojcg.3.3 search-completeness verdict carried by the
/// readiness surfaces (`health`/`status`/`triage`) and search `--robot-meta`.
pub(crate) fn response_schema_search_completeness() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Whether quarantined conversations make search incomplete. quarantine_status=degraded means known conversations are excluded from results (distinct from a merely stale index). Ordinary search still runs (can_search=true); coverage_suspect=true when the ingest circuit breaker tripped on a recent quarantine burst. next_command is always a safe inspect/retry pointer.",
        "properties": {
            "quarantine_status": { "type": "string", "enum": ["ok", "degraded"] },
            "quarantined_conversations": { "type": "integer" },
            "complete": { "type": "boolean" },
            "can_search": { "type": "boolean" },
            "coverage_suspect": { "type": "boolean" },
            "impact": { "type": "string" },
            "next_command": { "type": "string" }
        },
        "required": ["quarantine_status", "quarantined_conversations", "complete", "can_search", "coverage_suspect", "impact", "next_command"]
    })
}

/// JSON Schema for the uojcg.9.2/9.5 root-cause attribution emitted by
/// status/triage/doctor `--json`.
pub(crate) fn response_schema_root_cause_attribution() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Likely root-cause family attributed from observed diagnostic signals. family/locus/confidence are kebab-case taxonomy values (see introspect root_cause_taxonomy); evidence_refs carry the structured signals behind the call; recommended_next_probe is the cheapest bounded confirm. family=unknown + confidence=unknown means no signal implicated a family (not a failure). Complements doctor incidents[]; branch on family/confidence, not summary prose.",
        "properties": {
            "schema_version": { "type": "integer" },
            "family": { "type": "string" },
            "locus": { "type": "string" },
            "confidence": { "type": "string" },
            "evidence_refs": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "kind": { "type": "string" },
                        "locator": { "type": "string" },
                        "detail": { "type": "string" }
                    },
                    "required": ["kind", "locator"]
                }
            },
            "summary": { "type": "string" },
            "recommended_next_probe": { "type": ["string", "null"] }
        },
        "required": ["schema_version", "family", "locus", "confidence", "evidence_refs", "summary"]
    })
}

/// Triage alone can leave a readiness component uninspected when its budget
/// expires. Keep the other surfaces' observed-value contracts unchanged.
pub(crate) fn response_schema_triage_inspection(
    mut schema: serde_json::Value,
) -> serde_json::Value {
    schema["properties"]["inspected"] = serde_json::json!({ "type": "boolean" });
    schema
}

pub(crate) fn response_schema_triage_search_completeness() -> serde_json::Value {
    let mut schema = response_schema_triage_inspection(response_schema_search_completeness());
    schema["properties"]["quarantine_status"]["enum"] =
        serde_json::json!(["ok", "degraded", "not_inspected"]);
    schema["properties"]["quarantined_conversations"]["type"] =
        serde_json::json!(["integer", "null"]);
    for field in ["complete", "can_search", "coverage_suspect"] {
        schema["properties"][field]["type"] = serde_json::json!(["boolean", "null"]);
    }
    schema
}

pub(crate) fn response_schema_triage_readiness() -> serde_json::Value {
    let mut index = response_schema_triage_inspection(response_schema_index_state());
    index["properties"]["exists"]["type"] = serde_json::json!(["boolean", "null"]);
    let mut database = response_schema_triage_inspection(response_schema_state_database());
    database["properties"]["exists"]["type"] = serde_json::json!(["boolean", "null"]);
    response_schema_object([
        ("index", index),
        ("database", database),
        (
            "pending",
            response_schema_triage_inspection(response_schema_pending_state()),
        ),
        (
            "rebuild",
            response_schema_triage_inspection(response_schema_rebuild_state()),
        ),
        (
            "rebuild_progress",
            response_schema_triage_inspection(response_schema_rebuild_progress()),
        ),
        (
            "semantic",
            response_schema_triage_inspection(response_schema_semantic_state()),
        ),
        (
            "ingest_quarantine",
            response_schema_triage_inspection(response_schema_ingest_quarantine()),
        ),
    ])
}

pub(crate) fn response_schema_budget_block() -> serde_json::Value {
    response_schema_object([
        ("elapsed_ms", serde_json::json!({ "type": "integer" })),
        ("budget_ms", serde_json::json!({ "type": "integer" })),
        ("timed_out", serde_json::json!({ "type": "boolean" })),
        (
            "skipped_sections",
            serde_json::json!({
                "type": "array",
                "items": { "type": "string" }
            }),
        ),
        (
            "recommended_next_probe",
            serde_json::json!({ "type": ["string", "null"] }),
        ),
    ])
}

pub(crate) fn response_schema_search_effective() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "What the search (or the search behind a pack) actually ran: command, database and path source, resolved time window with the flag behind each bound, parsed filters, argv auto-corrections, how the lexical engine groups the query, and, for search only, the daemon policy.",
        "properties": {
            "command": { "type": "string" },
            "query": { "type": "string" },
            "query_structure": {
                "type": ["string", "null"],
                "description": "Operand grouping the lexical engine applies, every compound group parenthesized (e.g. `a OR (b AND c)`); null for a query without operands."
            },
            "query_recoveries": {
                "type": "array",
                "items": { "type": "string" },
                "description": "Parentheses recovered instead of rejected (an unclosed `(` closes at the end of the query; an empty `()` is skipped)."
            },
            "db_path": { "type": "string" },
            "db_path_source": {
                "type": "string",
                "enum": ["--db", "env:CASS_DB_PATH", "--data-dir", "env:CASS_DATA_DIR", "env:XDG_DATA_HOME", "default"]
            },
            "time_window": {
                "type": "object",
                "properties": {
                    "since_ms": { "type": ["integer", "null"] },
                    "since_from": { "type": ["string", "null"] },
                    "until_ms": { "type": ["integer", "null"] },
                    "until_from": { "type": ["string", "null"] }
                }
            },
            "filters": {
                "type": "object",
                "properties": {
                    "agents": { "type": "array", "items": { "type": "string" } },
                    "workspaces": { "type": "array", "items": { "type": "string" } },
                    "source": { "type": "string" },
                    "sessions_from_paths": { "type": ["integer", "null"] }
                }
            },
            "daemon": {
                "type": "object",
                "description": "Warm-model daemon policy: robot searches use an already-running daemon unless --no-daemon, and never spawn one even with --daemon.",
                "properties": {
                    "use_existing": { "type": "boolean" },
                    "auto_spawn_requested": { "type": "boolean" },
                    "auto_spawn": { "type": "boolean" }
                }
            },
            "auto_corrections": { "type": "array", "items": { "type": "string" } }
        }
    })
}

pub(crate) fn response_schema_search_meta() -> serde_json::Value {
    response_schema_object([
        ("elapsed_ms", serde_json::json!({ "type": "integer" })),
        (
            "search_mode",
            serde_json::json!({ "type": ["string", "null"] }),
        ),
        (
            "requested_search_mode",
            serde_json::json!({ "type": ["string", "null"] }),
        ),
        (
            "mode_defaulted",
            serde_json::json!({ "type": ["boolean", "null"] }),
        ),
        (
            "fallback_tier",
            serde_json::json!({ "type": ["string", "null"] }),
        ),
        (
            "fallback_reason",
            serde_json::json!({ "type": ["string", "null"] }),
        ),
        (
            "semantic_refinement",
            serde_json::json!({ "type": ["boolean", "null"] }),
        ),
        // Truthful refinement level + typed fallback reason (bead .5.4).
        ("refinement_level", serde_json::json!({ "type": "string" })),
        (
            "semantic_fallback_reason",
            serde_json::json!({ "type": ["string", "null"] }),
        ),
        // GH #441: set when hybrid answered from the semantic leg alone
        // because the lexical leg exhausted its Quill query fuel.
        (
            "lexical_degrade_reason",
            serde_json::json!({ "type": ["string", "null"], "enum": ["query_fuel_exhausted", null] }),
        ),
        (
            "wildcard_fallback",
            serde_json::json!({ "type": "boolean" }),
        ),
        // 2l1b0.68: why a sparse lexical result did not get the automatic
        // wildcard retry, which used to turn off without a trace.
        (
            "wildcard_fallback_skipped",
            serde_json::json!({
                "type": ["string", "null"],
                "enum": [
                    "index_over_automatic_limit",
                    "automatic_retry_disabled",
                    "long_query_term",
                    "query_fuel_exhausted",
                    null
                ],
            }),
        ),
        ("cache_stats", response_schema_search_cache_stats()),
        ("query_plan", response_schema_query_plan()),
        ("cursor_manifest", response_schema_cursor_manifest()),
        ("explanation_cards", response_schema_explanation_cards()),
        ("effective", response_schema_search_effective()),
        ("timing", response_schema_search_timing()),
        (
            "tokens_estimated",
            serde_json::json!({ "type": ["integer", "null"] }),
        ),
        (
            "max_tokens",
            serde_json::json!({ "type": ["integer", "null"] }),
        ),
        (
            "request_id",
            serde_json::json!({ "type": ["string", "null"] }),
        ),
        (
            "next_cursor",
            serde_json::json!({ "type": ["string", "null"] }),
        ),
        ("hits_clamped", serde_json::json!({ "type": "boolean" })),
        ("state", response_schema_state_meta()),
        ("search_completeness", response_schema_search_completeness()),
        ("index_freshness", response_schema_index_freshness()),
        // qfswx: same storage-integrity vocabulary doctor/status project,
        // present in --robot-meta so every truth surface agrees.
        ("storage_integrity", response_schema_storage_integrity()),
        (
            "timeout_ms",
            serde_json::json!({ "type": ["integer", "null"] }),
        ),
        (
            "timed_out",
            serde_json::json!({ "type": ["boolean", "null"] }),
        ),
        (
            "partial_results",
            serde_json::json!({ "type": ["boolean", "null"] }),
        ),
        (
            "ann_stats",
            serde_json::json!({
                "type": ["object", "null"],
                "additionalProperties": true
            }),
        ),
    ])
}

pub(crate) fn response_schema_search() -> serde_json::Value {
    response_schema_object([
        ("query", serde_json::json!({ "type": "string" })),
        ("limit", serde_json::json!({ "type": "integer" })),
        ("offset", serde_json::json!({ "type": "integer" })),
        ("count", serde_json::json!({ "type": "integer" })),
        (
            "total_matches",
            serde_json::json!({
                "type": "integer",
                "description": "Matches known to exist for this query. Exact only when `_meta.pagination.count_precision` is `exact` (lexical paths that return a backend count); otherwise a lower bound derived from the current page window — `offset + returned + (1 if has_more)` — so it tracks the requested limit rather than counting the corpus (GH #404). Branch on `has_more`, not on this number, to decide whether to page."
            }),
        ),
        (
            "max_tokens",
            serde_json::json!({ "type": ["integer", "null"] }),
        ),
        (
            "request_id",
            serde_json::json!({ "type": ["string", "null"] }),
        ),
        ("cursor", serde_json::json!({ "type": ["string", "null"] })),
        ("hits_clamped", serde_json::json!({ "type": "boolean" })),
        ("budget", response_schema_budget_block()),
        (
            "hits",
            serde_json::json!({
                "type": "array",
                "items": response_schema_search_hit()
            }),
        ),
        (
            "aggregations",
            serde_json::json!({
                "type": ["object", "null"],
                "additionalProperties": {
                    "type": "array",
                    "items": response_schema_object([
                        ("key", serde_json::json!({ "type": "string" })),
                        ("count", serde_json::json!({ "type": "integer" })),
                    ])
                }
            }),
        ),
        (
            "_warning",
            serde_json::json!({ "type": ["string", "null"] }),
        ),
        ("_meta", response_schema_search_meta()),
        ("suggestions", response_schema_opaque_object_array()),
        (
            "zero_result_diagnosis",
            serde_json::json!({
                "type": ["object", "null"],
                "additionalProperties": true
            }),
        ),
        (
            "explanation",
            serde_json::json!({
                "type": ["object", "null"],
                "additionalProperties": true
            }),
        ),
        (
            "_timeout",
            serde_json::json!({
                "type": ["object", "null"],
                "additionalProperties": true
            }),
        ),
    ])
}

pub(crate) fn response_schema_pack() -> serde_json::Value {
    response_schema_object([
        ("schema_version", serde_json::json!({ "type": "string" })),
        ("budget", response_schema_budget_block()),
        (
            "query",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "text": { "type": "string" },
                    "normalized": { "type": "string" },
                    "filters": {
                        "type": "object",
                        "additionalProperties": { "type": "string" }
                    }
                }
            }),
        ),
        (
            "_meta",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "request_id": { "type": ["string", "null"] },
                    "generated_at_ms": { "type": "integer" },
                    "elapsed_ms": { "type": "integer" },
                    "partial": { "type": "boolean" },
                    "format": { "type": "string" },
                    "warnings": { "type": "array", "items": { "type": "string" } },
                    "effective": response_schema_search_effective()
                }
            }),
        ),
        (
            "limits",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "max_tokens": { "type": "integer" },
                    "estimated_tokens": { "type": "integer" },
                    "max_sessions": { "type": "integer" },
                    "max_evidence": { "type": "integer" },
                    "context_lines": { "type": "integer" },
                    "max_excerpt_chars": { "type": "integer" },
                    "field_mask": { "type": "string" }
                }
            }),
        ),
        (
            "realized",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "search_mode": { "type": "string" },
                    "fallback_mode": { "type": ["string", "null"] },
                    "fallback_reason": {
                        "type": ["string", "null"],
                        "description": "Why the pack's search fell back to fallback_mode, as the executor recorded it (the typed codes of search's _meta.semantic_fallback_reason; pack_enrichment_unavailable means pack itself selects evidence lexically). Null when there was no fallback, or when none was supplied, which also adds the warning semantic_fallback_reason_unreported."
                    },
                    "semantic_joined": { "type": "boolean" },
                    "candidate_count": { "type": "integer" },
                    "selected_evidence_count": { "type": "integer" },
                    "selected_session_count": { "type": "integer" }
                }
            }),
        ),
        (
            "health",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "healthy": { "type": "boolean" },
                    "recommended_action": { "type": ["string", "null"] },
                    "index_state": { "type": "string" },
                    "index_generation": { "type": ["string", "null"] },
                    "lexical_readiness": { "type": "string" },
                    "semantic_state": { "type": "string" },
                    "active_rebuild": { "type": "boolean" },
                    "lock_state": { "type": ["string", "null"] },
                    "missing_database": { "type": "boolean" },
                    "source_sync_gaps": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "source_id": { "type": "string" },
                                "origin_kind": { "type": "string" },
                                "kind": { "type": "string" },
                                "lag_seconds": { "type": ["integer", "null"] },
                                "last_synced_at_ms": { "type": ["integer", "null"] },
                                "recommended_action": { "type": ["string", "null"] }
                            }
                        }
                    },
                    "source_readiness": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "source_id": { "type": "string" },
                                "origin_kind": { "type": "string" },
                                "readiness": { "type": "string" },
                                "healthy": { "type": "boolean" },
                                "evidence_count": { "type": "integer" }
                            }
                        }
                    }
                }
            }),
        ),
        (
            "freshness",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "policy": { "type": "string" },
                    "window_seconds": { "type": "integer" },
                    "newest_evidence_at_ms": { "type": ["integer", "null"] },
                    "oldest_evidence_at_ms": { "type": ["integer", "null"] },
                    "stale_evidence_count": { "type": "integer" }
                }
            }),
        ),
        (
            "pack",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "title": { "type": "string" },
                    "answer_outline": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "rank": { "type": "integer" },
                                "heading": { "type": "string" },
                                "evidence_ids": { "type": "array", "items": { "type": "string" } }
                            }
                        }
                    },
                    "source_summary": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "source_id": { "type": "string" },
                                "origin_kind": { "type": "string" },
                                "session_count": { "type": "integer" },
                                "evidence_count": { "type": "integer" },
                                "newest_evidence_at_ms": { "type": ["integer", "null"] },
                                "healthy": { "type": "boolean" }
                            }
                        }
                    },
                    "handoff": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "rank": { "type": "integer" },
                                "kind": { "type": "string" },
                                "text": { "type": "string" },
                                "evidence_ids": { "type": "array", "items": { "type": "string" } }
                            }
                        }
                    }
                }
            }),
        ),
        (
            "evidence",
            serde_json::json!({
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": true
                }
            }),
        ),
        (
            "omitted",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "count": { "type": "integer" },
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": true
                        }
                    }
                }
            }),
        ),
        (
            "privacy",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "redaction_policy": { "type": "string" },
                    "redaction_applied": { "type": "boolean" },
                    "sensitive_output": { "type": "boolean" },
                    "skill_content_included": { "type": "boolean" },
                    "redaction_counts": {
                        "type": "object",
                        "additionalProperties": { "type": "integer" }
                    }
                }
            }),
        ),
        (
            "warnings",
            serde_json::json!({
                "type": "array",
                "items": { "type": "string" }
            }),
        ),
    ])
}

pub(crate) fn response_schema_subsystem_coverage_matrix() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "schema_version": { "type": "integer" },
            "subsystem_count": { "type": "integer" },
            "complete": { "type": "boolean" },
            "subsystems": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "subsystem": { "type": "string" },
                        "owning_beads": {
                            "type": "array",
                            "items": { "type": "string" }
                        },
                        "failure_modes": {
                            "type": "array",
                            "items": { "type": "string" }
                        },
                        "mandatory_proofs": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "enum": ["unit", "integration", "golden", "e2e", "logs"]
                            }
                        },
                        "proof_artifacts": {
                            "type": "array",
                            "items": { "type": "string" }
                        },
                        "optional_diagnostics": {
                            "type": "array",
                            "items": { "type": "string" }
                        },
                        "fixture_provenance": { "type": "string" },
                        "log_expectation": { "type": "string" },
                        "redaction": {
                            "type": "string",
                            "enum": ["no-user-content", "local-only", "redact-required"]
                        },
                        "closure_evidence": { "type": "string" }
                    },
                    "required": [
                        "subsystem",
                        "owning_beads",
                        "failure_modes",
                        "mandatory_proofs",
                        "proof_artifacts",
                        "optional_diagnostics",
                        "fixture_provenance",
                        "log_expectation",
                        "redaction",
                        "closure_evidence"
                    ]
                }
            }
        },
        "required": ["schema_version", "subsystem_count", "complete", "subsystems"]
    })
}
