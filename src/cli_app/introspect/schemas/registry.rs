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

/// Build response schemas for commands that support JSON output.
///
/// Returns a `BTreeMap` so the serialized JSON object has deterministic
/// key order (alphabetical) — see `IntrospectResponse::response_schemas`
/// and bead coding_agent_session_search-8sl73 for why this matters.
pub(crate) fn build_response_schemas() -> std::collections::BTreeMap<String, serde_json::Value> {
    use serde_json::json;
    let mut schemas = std::collections::BTreeMap::new();

    schemas.insert("search".to_string(), response_schema_search());
    schemas.insert("pack".to_string(), response_schema_pack());

    schemas.insert(
        "status".to_string(),
        json!({
            "type": "object",
            "properties": {
                "status": { "type": "string" },
                "healthy": { "type": "boolean" },
                "health_level": { "type": "string" },
                "initialized": { "type": "boolean" },
                "explanation": { "type": ["string", "null"] },
                "warnings": { "type": "array", "items": { "type": "string" } },
                "data_dir": { "type": "string" },
                "recommended_action": { "type": ["string", "null"] },
                "recommended_commands": response_schema_recommended_commands(),
                "index": response_schema_index_state(),
                "database": response_schema_status_database(),
                "pending": response_schema_pending_state(),
                "rebuild": response_schema_rebuild_state(),
                "rebuild_progress": response_schema_rebuild_progress(),
                "semantic": response_schema_semantic_state(),
                "ingest_quarantine": response_schema_ingest_quarantine(),
                "policy_registry": response_schema_policy_registry(),
                "topology_budget": response_schema_topology_budget(),
                "daemon_runtime": response_schema_opaque_object(),
                "doctor_summary": response_schema_doctor_v2_summary("status-summary"),
                "coverage_risk": response_schema_doctor_coverage_risk(),
                "quarantine": response_schema_opaque_object(),
                "search_completeness": response_schema_search_completeness(),
                "root_cause": response_schema_root_cause_attribution(),
                "storage_integrity": response_schema_storage_integrity(),
                "budget": response_schema_budget_block(),
                "_meta": {
                    "type": "object",
                    "properties": {
                        "timestamp": { "type": "string" },
                        "data_dir": { "type": "string" },
                        "db_path": { "type": "string" }
                    }
                }
            }
        }),
    );
    schemas.insert(
        "triage".to_string(),
        json!({
            "type": "object",
            "description": "One-shot read-only first stop for automation agents. Summarizes readiness and returns exact next commands plus discovery pointers.",
            "properties": {
                "surface": { "type": "string" },
                "schema_version": { "type": "integer" },
                "status": { "type": "string" },
                "healthy": { "type": "boolean" },
                "initialized": { "type": "boolean" },
                "explanation": { "type": ["string", "null"] },
                "data_dir": { "type": "string" },
                "recommended_action": { "type": ["string", "null"] },
                "recommended_commands": response_schema_recommended_commands(),
                "next_command": { "type": ["string", "null"] },
                "search_completeness": response_schema_triage_search_completeness(),
                "root_cause": response_schema_triage_inspection(response_schema_root_cause_attribution()),
                "readiness": response_schema_triage_readiness(),
                "discovery": {
                    "type": "object",
                    "properties": {
                        "capabilities_command": { "type": "string" },
                        "schemas_command": { "type": "string" },
                        "docs_command": { "type": "string" },
                        "api_version_command": { "type": "string" }
                    }
                },
                "starter_workflows": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": { "type": "string" },
                            "intent": { "type": "string" },
                            "first_command": { "type": "string" },
                            "follow_up_commands": { "type": "array", "items": { "type": "string" } },
                            "parse_contract": { "type": "string" },
                            "note": { "type": "string" }
                        }
                    }
                },
                "mistake_recoveries": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "wrong": { "type": "string" },
                            "canonical": { "type": "string" },
                            "accepted": { "type": "boolean" },
                            "behavior": { "type": "string" }
                        }
                    }
                },
                "budget": response_schema_budget_block(),
                "_meta": {
                    "type": "object",
                    "properties": {
                        "timestamp": { "type": "string" },
                        "data_dir": { "type": "string" },
                        "db_path": { "type": "string" }
                    }
                }
            },
            "required": [
                "surface",
                "schema_version",
                "status",
                "healthy",
                "initialized",
                "recommended_commands",
                "next_command",
                "search_completeness",
                "root_cause",
                "readiness",
                "discovery",
                "starter_workflows",
                "mistake_recoveries",
                "_meta"
            ]
        }),
    );
    schemas.insert(
        "sessions".to_string(),
        json!({
            "type": "object",
            "properties": {
                "sessions": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "path": { "type": "string" },
                            "workspace": { "type": ["string", "null"] },
                            "agent": { "type": "string" },
                            "title": { "type": ["string", "null"] },
                            "source_id": { "type": "string" },
                            "origin_host": { "type": ["string", "null"] },
                            "modified": { "type": ["string", "null"] },
                            "size_bytes": { "type": ["integer", "null"] },
                            "message_count": { "type": "integer" },
                            "human_turns": { "type": "integer" }
                        },
                        "required": ["path", "agent", "source_id", "message_count", "human_turns"]
                    }
                }
            },
            "required": ["sessions"]
        }),
    );
    schemas.insert(
        "state".to_string(),
        json!({
            "type": "object",
            "properties": {
                "status": { "type": "string" },
                "healthy": { "type": "boolean" },
                "health_level": { "type": "string" },
                "initialized": { "type": "boolean" },
                "explanation": { "type": ["string", "null"] },
                "warnings": { "type": "array", "items": { "type": "string" } },
                "data_dir": { "type": "string" },
                "recommended_action": { "type": ["string", "null"] },
                "recommended_commands": response_schema_recommended_commands(),
                "index": response_schema_index_state(),
                "database": response_schema_status_database(),
                "pending": response_schema_pending_state(),
                "rebuild": response_schema_rebuild_state(),
                "rebuild_progress": response_schema_rebuild_progress(),
                "semantic": response_schema_semantic_state(),
                "ingest_quarantine": response_schema_ingest_quarantine(),
                "policy_registry": response_schema_policy_registry(),
                "topology_budget": response_schema_topology_budget(),
                "doctor_summary": response_schema_doctor_v2_summary("status-summary"),
                "coverage_risk": response_schema_doctor_coverage_risk(),
                "quarantine": response_schema_opaque_object(),
                "daemon_runtime": response_schema_opaque_object(),
                "budget": response_schema_budget_block(),
                "search_completeness": response_schema_search_completeness(),
                "root_cause": response_schema_root_cause_attribution(),
                "storage_integrity": response_schema_storage_integrity(),
                "_meta": {
                    "type": "object",
                    "properties": {
                        "timestamp": { "type": "string" },
                        "data_dir": { "type": "string" },
                        "db_path": { "type": "string" }
                    }
                }
            }
        }),
    );

    schemas.insert(
        "capabilities".to_string(),
        json!({
            "type": "object",
            "properties": {
                "version": { "type": "string" },
                "crate_version": { "type": "string" },
                "build_commit": { "type": "string" },
                "build_commit_date": { "type": "string" },
                "source_ingest_reuse": {
                    "type": "object",
                    "properties": {
                        "enabled": { "type": "boolean" },
                        "disabled_by": { "type": ["string", "null"] }
                    }
                },
                "api_version": { "type": "integer" },
                "contract_version": { "type": "string" },
                "features": { "type": "array", "items": { "type": "string" } },
                "connectors": { "type": "array", "items": { "type": "string" } },
                "global_flags": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": { "type": "string" },
                            "short": { "type": ["string", "null"] },
                            "description": { "type": "string" },
                            "arg_type": { "type": "string" },
                            "value_type": { "type": ["string", "null"] },
                            "required": { "type": "boolean" },
                            "default": { "type": ["string", "null"] },
                            "enum_values": { "type": ["array", "null"], "items": { "type": "string" } },
                            "repeatable": { "type": ["boolean", "null"] }
                        }
                    }
                },
                "commands": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": { "type": "string" },
                            "description": { "type": "string" },
                            "has_json_output": { "type": "boolean" },
                            "arguments": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "name": { "type": "string" },
                                        "short": { "type": ["string", "null"] },
                                        "description": { "type": "string" },
                                        "arg_type": { "type": "string" },
                                        "value_type": { "type": ["string", "null"] },
                                        "required": { "type": "boolean" },
                                        "default": { "type": ["string", "null"] },
                                        "enum_values": { "type": ["array", "null"], "items": { "type": "string" } },
                                        "repeatable": { "type": ["boolean", "null"] }
                                    }
                                }
                            }
                        }
                    }
                },
                "exit_codes": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "code": { "type": "string" },
                            "meaning": { "type": "string" },
                            "retryable": { "type": "string" },
                            "agent_action": { "type": "string" }
                        }
                    }
                },
                "env_vars": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": { "type": "string" },
                            "default": { "type": ["string", "null"] },
                            "description": { "type": "string" }
                        }
                    }
                },
                "workflows": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": { "type": "string" },
                            "intent": { "type": "string" },
                            "first_command": { "type": "string" },
                            "follow_up_commands": { "type": "array", "items": { "type": "string" } },
                            "parse_contract": { "type": "string" },
                            "note": { "type": "string" }
                        }
                    }
                },
                "mistake_recoveries": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "wrong": { "type": "string" },
                            "canonical": { "type": "string" },
                            "accepted": { "type": "boolean" },
                            "behavior": { "type": "string" }
                        }
                    }
                },
                "limits": {
                    "type": "object",
                    "properties": {
                        "max_limit": { "type": "integer" },
                        "max_content_length": { "type": "integer" },
                        "max_fields": { "type": "integer" },
                        "max_agg_buckets": { "type": "integer" }
                    }
                }
            }
        }),
    );

    schemas.insert(
        "api-version".to_string(),
        json!({
            "type": "object",
            "properties": {
                "crate_version": { "type": "string" },
                "build_commit": { "type": "string" },
                "api_version": { "type": "integer" },
                "contract_version": { "type": "string" }
            }
        }),
    );

    schemas.insert(
        "introspect".to_string(),
        json!({
            "type": "object",
            "properties": {
                "api_version": { "type": "integer" },
                "contract_version": { "type": "string" },
                "subsystem_coverage": response_schema_subsystem_coverage_matrix(),
                "global_flags": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": { "type": "string" },
                            "short": { "type": ["string", "null"] },
                            "description": { "type": "string" },
                            "arg_type": { "type": "string" },
                            "value_type": { "type": ["string", "null"] },
                            "required": { "type": "boolean" },
                            "default": { "type": ["string", "null"] },
                            "enum_values": { "type": ["array", "null"], "items": { "type": "string" } },
                            "repeatable": { "type": ["boolean", "null"] }
                        }
                    }
                },
                "commands": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": { "type": "string" },
                            "description": { "type": "string" },
                            "has_json_output": { "type": "boolean" },
                            "arguments": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "name": { "type": "string" },
                                        "short": { "type": ["string", "null"] },
                                        "description": { "type": "string" },
                                        "arg_type": { "type": "string" },
                                        "value_type": { "type": ["string", "null"] },
                                        "required": { "type": "boolean" },
                                        "default": { "type": ["string", "null"] },
                                        "enum_values": { "type": ["array", "null"], "items": { "type": "string" } },
                                        "repeatable": { "type": ["boolean", "null"] }
                                    }
                                }
                            }
                        }
                    }
                },
                "response_schemas": {
                    "type": "object",
                    "additionalProperties": { "type": "object" }
                }
            }
        }),
    );

    schemas.insert(
        "index".to_string(),
        json!({
            "type": "object",
            "properties": {
                "success": { "type": "boolean" },
                "elapsed_ms": { "type": "integer" },
                "full": { "type": ["boolean", "null"] },
                "force_rebuild": { "type": ["boolean", "null"] },
                "entrypoint": response_schema_opaque_object(),
                "data_dir": { "type": ["string", "null"] },
                "db_path": { "type": ["string", "null"] },
                "conversations": { "type": ["integer", "null"] },
                "messages": { "type": ["integer", "null"] },
                "quarantined_conversations": { "type": "integer" },
                "lexical_update_deferred": { "type": "boolean" },
                "indexing_stats": response_schema_opaque_object(),
                "rebuild_profile": {
                    "type": "object",
                    "description": "--rebuild-profile echo (GH #483): the selected preset, where each of its knobs came from, and the lexical-rebuild settings this process would use after the responsiveness governor at report time",
                    "properties": {
                        "name": { "type": "string", "description": "default | bounded" },
                        "settings": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "env": { "type": "string" },
                                    "value": { "type": "integer" },
                                    "source": { "type": "string", "description": "profile, or env when an explicit environment value overrides the preset" }
                                }
                            }
                        },
                        "effective": {
                            "type": "object",
                            "properties": {
                                "workers": { "type": "integer" },
                                "page_prep_workers": { "type": "integer" },
                                "staged_shard_builders": { "type": "integer" },
                                "staged_merge_workers": { "type": "integer" },
                                "tantivy_writer_threads": { "type": "integer" },
                                "pipeline_channel_size": { "type": "integer" },
                                "steady_batch_fetch_conversations": { "type": "integer" },
                                "steady_commit_every_message_bytes": { "type": "integer" },
                                "pipeline_max_message_bytes_in_flight": { "type": "integer" }
                            }
                        }
                    }
                },
                "error": { "type": ["string", "null"] }
            }
        }),
    );

    schemas.insert(
        "diag".to_string(),
        json!({
            "type": "object",
            "properties": {
                "version": { "type": "string" },
                "platform": {
                    "type": "object",
                    "properties": {
                        "os": { "type": "string" },
                        "arch": { "type": "string" }
                    }
                },
                "paths": {
                    "type": "object",
                    "properties": {
                        "data_dir": { "type": "string" },
                        "db_path": { "type": "string" },
                        "index_path": { "type": "string" }
                    }
                },
                "database": {
                    "type": "object",
                    "properties": {
                        "exists": { "type": "boolean" },
                        "size_bytes": { "type": "integer" },
                        "conversations": { "type": "integer" },
                        "messages": { "type": "integer" }
                    }
                },
                "index": {
                    "type": "object",
                    "properties": {
                        "exists": { "type": "boolean" },
                        "size_bytes": { "type": "integer" }
                    }
                },
                "connectors": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": { "type": "string" },
                            "path": { "type": "string" },
                            "found": { "type": "boolean" }
                        }
                    }
                }
            }
        }),
    );

    schemas.insert(
        "view".to_string(),
        json!({
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "start_line": { "type": "integer" },
                "end_line": { "type": "integer" },
                "highlight_line": { "type": ["integer", "null"] },
                "target_line": { "type": ["integer", "null"] },
                "context": { "type": "integer" },
                "total_lines": { "type": "integer" },
                // Provenance flags (uojcg.2.3): archive_only=true means the
                // source file is gone/stale and content came from the archive.
                "source_exists": { "type": "boolean" },
                "archive_only": { "type": "boolean" },
                // #493 (5173f7db, aa5d5068): every view names the coordinate
                // system it was addressed in and where its text came from.
                // `--line` reads the physical file; `--message-index` reads
                // the canonical archive message ordinal.
                "coordinate_space": { "type": "string", "enum": ["file_line", "message_index"] },
                "content_source": { "type": "string", "enum": ["file", "archive"] },
                "source_id": { "type": "string" },
                "conversation_id": { "type": "integer" },
                "target_message_index": { "type": "integer" },
                "total_messages": { "type": "integer" },
                "budget": response_schema_budget_block(),
                "lines": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "line": { "type": "integer" },
                            "number": { "type": "integer" },
                            "file_line": { "type": "integer" },
                            "message_index": { "type": "integer" },
                            "coordinate_space": { "type": "string", "enum": ["file_line", "message_index"] },
                            "content_source": { "type": "string", "enum": ["file", "archive"] },
                            "message_id": { "type": ["integer", "null"] },
                            "conversation_id": { "type": "integer" },
                            "source_id": { "type": "string" },
                            "role": { "type": "string" },
                            "content": { "type": "string" },
                            "is_target": { "type": "boolean" },
                            "highlighted": { "type": "boolean" }
                        }
                    }
                }
            }
        }),
    );

    schemas.insert(
        "stats".to_string(),
        json!({
            "type": "object",
            "properties": {
                "conversations": { "type": "integer" },
                "messages": { "type": "integer" },
                "by_agent": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "agent": { "type": "string" },
                            "count": { "type": "integer" }
                        }
                    }
                },
                "top_workspaces": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "workspace": { "type": "string" },
                            "count": { "type": "integer" }
                        }
                    }
                },
                "date_range": {
                    "type": "object",
                    "properties": {
                        "oldest": { "type": ["string", "null"] },
                        "newest": { "type": ["string", "null"] }
                    }
                },
                "raw_mirror": {
                    "type": "object",
                    "properties": {
                        "initialized": { "type": "boolean" },
                        "root_path": { "type": "string" },
                        "total_storage_bytes": { "type": "integer" },
                        "manifest_count": { "type": "integer" },
                        "manifest_bytes": { "type": "integer" },
                        "unique_blob_count": { "type": "integer" },
                        "total_blob_bytes": { "type": "integer" },
                        "largest_blob_bytes": { "type": "integer" },
                        "orphan_blob_count": { "type": "integer" },
                        "orphan_blob_bytes": { "type": "integer" },
                        "missing_blob_count": { "type": "integer" },
                        "invalid_manifest_count": { "type": "integer" },
                        "oldest_capture_at_ms": { "type": ["integer", "null"] },
                        "newest_capture_at_ms": { "type": ["integer", "null"] },
                        "oldest_source_mtime_ms": { "type": ["integer", "null"] },
                        "newest_source_mtime_ms": { "type": ["integer", "null"] }
                    }
                },
                "db_path": { "type": "string" }
            }
        }),
    );

    schemas.insert(
        "selftest".to_string(),
        json!({
            "type": "object",
            "description": "Archive-independent executable probe. This command never resolves, reads, or writes the configured cass archive.",
            "properties": {
                "schema_version": { "type": "integer", "const": 1 },
                "status": { "type": "string", "enum": ["ok", "error"] },
                "functional": { "type": "boolean" },
                "archive_accessed": { "type": "boolean", "const": false },
                "version": { "type": "string" },
                "contract_version": { "type": "string" },
                "latency_ms": { "type": "integer" },
                "checks": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": { "type": "string" },
                            "status": { "type": "string", "enum": ["pass", "fail"] },
                            "detail": { "type": "string" }
                        },
                        "required": ["name", "status", "detail"]
                    }
                }
            },
            "required": [
                "schema_version",
                "status",
                "functional",
                "archive_accessed",
                "version",
                "contract_version",
                "latency_ms",
                "checks"
            ]
        }),
    );

    schemas.insert(
        "health".to_string(),
        json!({
            "type": "object",
            "properties": {
                "status": { "type": "string" },
                "healthy": { "type": "boolean" },
                "exit_policy": { "type": "string" },
                "health_level": { "type": "string" },
                "initialized": { "type": "boolean" },
                "explanation": { "type": ["string", "null"] },
                "warnings": { "type": "array", "items": { "type": "string" } },
                "data_dir": { "type": "string" },
                "recommended_action": { "type": ["string", "null"] },
                "recommended_commands": response_schema_recommended_commands(),
                "errors": {
                    "type": "array",
                    "items": { "type": "string" }
                },
                "latency_ms": { "type": "integer" },
                "rebuild_progress": response_schema_rebuild_progress(),
                "db": response_schema_health_db(),
                "doctor_summary": response_schema_doctor_v2_summary("health-summary"),
                "coverage_risk": response_schema_doctor_coverage_risk(),
                "policy_registry": response_schema_policy_registry(),
                "ingest_quarantine": response_schema_ingest_quarantine(),
                "search_completeness": response_schema_search_completeness(),
                "responsiveness": {
                    "type": "object",
                    "description": "Machine-responsiveness governor telemetry. Explains why the indexer is running at reduced fan-out and what pressure triggered any recent shrinkage.",
                    "properties": {
                        "current_capacity_pct": { "type": "integer", "description": "Published capacity scalar in [min_capacity_pct, 100]. Fan-out knobs multiply their caller-requested values by this percentage." },
                        "resource_policy": response_schema_opaque_object(),
                        "healthy_streak": { "type": "integer", "description": "Consecutive healthy ticks seen by the sampler; growth_ticks consecutive healthy ticks trigger a 25pp grow step." },
                        "shrink_count": { "type": "integer", "description": "Cumulative shrink events since governor startup." },
                        "grow_count": { "type": "integer", "description": "Cumulative grow events since governor startup." },
                        "ticks_total": { "type": "integer", "description": "Total sampler ticks observed since startup." },
                        "disabled_via_env": { "type": "boolean", "description": "True when CASS_RESPONSIVENESS_DISABLE pins capacity at 100%." },
                        "last_snapshot": {
                            "type": ["object", "null"],
                            "properties": {
                                "load_per_core": { "type": ["number", "null"] },
                                "psi_cpu_some_avg10": { "type": ["number", "null"] }
                            }
                        },
                        "last_reason": { "type": ["string", "null"], "description": "One of: disabled, severe, pressured, pressured_floor_hold, healthy_hold, healthy_grow, healthy_ceiling_hold." },
                        "calibration": response_schema_opaque_object(),
                        "recent_decisions": {
                            "type": "array",
                            "description": "Ring buffer of capacity changes and pressure events, oldest → newest.",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "at_elapsed_ms": { "type": "integer" },
                                    "prev_capacity_pct": { "type": "integer" },
                                    "next_capacity_pct": { "type": "integer" },
                                    "reason": { "type": "string" },
                                    "snapshot": {
                                        "type": "object",
                                        "properties": {
                                            "load_per_core": { "type": ["number", "null"] },
                                            "psi_cpu_some_avg10": { "type": ["number", "null"] }
                                        }
                                    }
                                }
                            }
                        },
                        "memory": {
                            "type": "object",
                            "description": "Live resident-memory observation, absent before the sampler has run.",
                            "properties": {
                                "capacity_pct": { "type": "integer" },
                                "inflight_byte_limit": { "type": "integer" },
                                "target_resident_bytes": { "type": ["integer", "null"] },
                                "resident_bytes": { "type": ["integer", "null"] },
                                "available_bytes": { "type": ["integer", "null"] },
                                "effective_total_bytes": { "type": ["integer", "null"] },
                                "healthy_streak": { "type": "integer" },
                                "reason": { "type": "string" }
                            }
                        }
                    }
                },
                "state": response_schema_state_meta(),
                "runtime_optimizations": {
                    "type": "object",
                    "properties": {
                        "simd_dot": { "type": "boolean" },
                        "parallel_search": { "type": "boolean" },
                        "preconvert_f16": { "type": "boolean" },
                        "config_source": { "type": "string" }
                    }
                },
                "parallel_wal_shadow": {
                    "type": "object",
                    "description": "Parallel-WAL shadow observer (Card 1, shadow-only phase). Activates under CASS_INDEXER_PARALLEL_WAL=shadow. Records per-chunk wall-clock on serialized chunk writes so operators can assess what an epoch-ordered group-commit coordinator would have decided. NEVER changes commit semantics.",
                    "properties": {
                        "active": { "type": "boolean", "description": "True when shadow mode is enabled." },
                        "chunks_observed": { "type": "integer", "description": "Cumulative shadow-observed chunks since startup." },
                        "cumulative_wall_micros": { "type": "integer", "description": "Total wall-clock across observed chunks (µs)." },
                        "chunk_errors": { "type": "integer", "description": "Count of observed chunks that returned an error." },
                        "recent_chunks": {
                            "type": "array",
                            "description": "Bounded ring buffer of the most-recent chunk records, oldest → newest.",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "chunk_idx": { "type": "integer" },
                                    "worker_slot": { "type": ["integer", "null"], "description": "Rayon worker slot observed for this chunk when available." },
                                    "base_conv_idx": { "type": "integer" },
                                    "convs_in_chunk": { "type": "integer" },
                                    "start_elapsed_micros": { "type": "integer" },
                                    "finish_elapsed_micros": { "type": "integer" },
                                    "wall_micros": { "type": "integer" },
                                    "succeeded": { "type": "boolean" }
                                }
                            }
                        },
                        "epoch_plan_manifest": {
                            "type": "object",
                            "description": "Shadow-only Silo/Aether epoch-plan manifest. Groups observed chunks into hypothetical group-commit epochs and exposes row counts, worker slots, logical digest, fallback decision, and proof gates. commit_mode_allowed is false until equivalence and crash-replay evidence pass.",
                            "properties": {
                                "schema_version": { "type": "integer" },
                                "mode": { "type": "string" },
                                "epoch_micros": { "type": "integer" },
                                "commit_mode_allowed": { "type": "boolean" },
                                "fallback_decision": { "type": "string" },
                                "fallback_reason": { "type": "string" },
                                "logical_digest": { "type": "string" },
                                "window_chunks": { "type": "integer" },
                                "total_chunks_observed": { "type": "integer" },
                                "successful_chunks": { "type": "integer" },
                                "failed_chunks": { "type": "integer" },
                                "total_conversations": { "type": "integer" },
                                "estimated_fsyncs_saved_vs_per_chunk": { "type": "integer" },
                                "planned_epochs": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "epoch_id": { "type": "integer" },
                                            "chunk_count": { "type": "integer" },
                                            "worker_slots": { "type": "array", "items": { "type": "integer" } },
                                            "conversation_count": { "type": "integer" },
                                            "first_chunk_idx": { "type": "integer" },
                                            "last_chunk_idx": { "type": "integer" },
                                            "first_start_elapsed_micros": { "type": "integer" },
                                            "last_finish_elapsed_micros": { "type": "integer" },
                                            "max_chunk_wall_micros": { "type": "integer" },
                                            "failed_chunks": { "type": "integer" },
                                            "would_have_group_fsyncs": { "type": "integer" },
                                            "fsyncs_saved_vs_per_chunk": { "type": "integer" }
                                        }
                                    }
                                },
                                "proof_obligations": { "type": "array", "items": { "type": "string" } }
                            }
                        }
                    }
                }
            }
        }),
    );

    super::registry_doctor::insert_doctor_response_schemas(&mut schemas);
    schemas
}
