use super::response_schema_doctor_repair_failure_marker::{
    response_schema_budget_block, response_schema_doctor_candidate_staging,
    response_schema_doctor_cleanup_apply, response_schema_doctor_config_exclusion_risk,
    response_schema_doctor_coverage_risk, response_schema_doctor_coverage_summary,
    response_schema_doctor_detail_output, response_schema_doctor_locks,
    response_schema_doctor_operation_state, response_schema_doctor_raw_mirror,
    response_schema_doctor_raw_mirror_backfill, response_schema_doctor_repair_failure_marker,
    response_schema_doctor_retry_recommendation, response_schema_doctor_slow_operations,
    response_schema_doctor_source_authority, response_schema_doctor_source_inventory,
    response_schema_doctor_timing_summary, response_schema_root_cause_attribution,
    response_schema_search, response_schema_search_completeness, response_schema_triage_inspection,
    response_schema_triage_readiness, response_schema_triage_search_completeness,
};
use super::response_schema_index_state::{
    DoctorV2SurfaceSchemaSpec, response_schema_doctor_action,
    response_schema_doctor_artifact_manifest, response_schema_doctor_derived_semantic_assets,
    response_schema_doctor_event_log_metadata, response_schema_doctor_failure_context,
    response_schema_doctor_forensic_bundle_metadata, response_schema_doctor_operation_outcome,
    response_schema_doctor_receipt, response_schema_doctor_repair_plan_preview,
    response_schema_doctor_safety_gate, response_schema_doctor_v2_contract_summary,
    response_schema_doctor_v2_error_envelope, response_schema_doctor_v2_summary,
    response_schema_doctor_v2_surface, response_schema_health_db, response_schema_index_state,
    response_schema_ingest_quarantine, response_schema_opaque_object,
    response_schema_opaque_object_array, response_schema_pending_state,
    response_schema_policy_registry, response_schema_rebuild_progress,
    response_schema_rebuild_state, response_schema_recommended_commands,
    response_schema_semantic_state, response_schema_state_meta, response_schema_status_database,
    response_schema_storage_integrity, response_schema_topology_budget,
};
use super::response_schema_pack::{
    response_schema_pack, response_schema_subsystem_coverage_matrix,
};

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
        "analytics-incidents".to_string(),
        json!({
            "type": "object",
            "description": "cass analytics incidents --json: bounded read-only incident mining over canonical archive conversations/messages.",
            "properties": {
                "command": { "type": "string", "const": "analytics/incidents" },
                "data": {
                    "type": "object",
                    "properties": {
                        "schema_version": { "type": "integer", "const": 2 },
                        "count_scope": { "type": "string", "enum": ["all_matching_candidates", "scanned_candidates_partial", "no_verified_results_hard_timeout"] },
                        "scan_units": {
                            "type": "object",
                            "properties": {
                                "files": { "type": "string" },
                                "lines": { "type": "string" },
                                "bytes": { "type": "string" },
                                "candidate_order": { "type": "string" },
                                "message_fragment_chars": { "type": "integer" }
                            },
                            "required": ["files", "lines", "bytes", "candidate_order", "message_fragment_chars"]
                        },
                        "discovery": {
                            "type": "object",
                            "properties": {
                                "schema_version": { "type": "integer", "const": 1 },
                                "caps": {
                                    "type": "object",
                                    "properties": {
                                        "max_files": { "type": "integer" },
                                        "max_lines": { "type": "integer" },
                                        "max_bytes": { "type": "integer" },
                                        "budget_ms": { "type": "integer" },
                                        "max_evidence": { "type": "integer" }
                                    },
                                    "required": ["max_files", "max_lines", "max_bytes", "budget_ms", "max_evidence"]
                                },
                                "files_considered": { "type": "integer" },
                                "files_scanned": { "type": "integer" },
                                "lines_scanned": { "type": "integer" },
                                "bytes_scanned": { "type": "integer" },
                                "elapsed_ms": { "type": "integer" },
                                "stop_reason": { "type": "string", "enum": ["completed", "files-capped", "lines-capped", "bytes-capped", "message-fragment-capped", "timed-out"] },
                                "timed_out": { "type": "boolean" },
                                "partial": { "type": "boolean" },
                                "evidence_truncated": { "type": "boolean" },
                                "evidence": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "file": { "type": "string" },
                                            "line": { "type": "integer" },
                                            "category": { "type": "string" },
                                            "reason": { "type": "string" }
                                        },
                                        "required": ["file", "line"]
                                    }
                                }
                            },
                            "required": ["caps", "files_considered", "files_scanned", "lines_scanned", "bytes_scanned", "elapsed_ms", "stop_reason", "timed_out", "partial", "evidence_truncated", "evidence"]
                        },
                        "top_sessions": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "conversation_id": { "type": "integer" },
                                    "session_id": { "type": "string" },
                                    "agent": { "type": "string" },
                                    "host": { "type": "string" },
                                    "source_id": { "type": "string" },
                                    "origin_host": { "type": ["string", "null"] },
                                    "source_path": { "type": "string" },
                                    "exists_state": { "type": "string", "enum": ["exists", "archive_only", "unknown"] },
                                    "hit_count": { "type": "integer" },
                                    "category_breadth": { "type": "integer" },
                                    "dominant_categories": { "type": "array", "items": { "type": "string" } },
                                    "evidence_summaries": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "category": { "type": "string" },
                                                "hit_count": { "type": "integer" },
                                                "content_fingerprints": { "type": "array", "items": { "type": "string" } },
                                                "evidence_paths": { "type": "array", "items": { "type": "string" } }
                                            },
                                            "required": ["category", "hit_count", "content_fingerprints", "evidence_paths"]
                                        }
                                    },
                                    "redaction_status": { "type": "string", "enum": ["redacted", "not_applicable"] },
                                    "suggested_command": {
                                        "type": "object",
                                        "properties": {
                                            "kind": { "type": "string", "const": "view" },
                                            "argv": {
                                                "type": "array",
                                                "description": "Executable argv preserving the effective --db and exact --conversation-id archive identity.",
                                                "items": { "type": "string" }
                                            },
                                            "display": { "type": "string" }
                                        },
                                        "required": ["kind", "argv", "display"]
                                    }
                                },
                                "required": ["conversation_id", "session_id", "agent", "host", "source_id", "source_path", "exists_state", "hit_count", "category_breadth", "dominant_categories", "evidence_summaries", "redaction_status", "suggested_command"]
                            }
                        },
                        "total_sessions": { "type": "integer" },
                        "total_hits": { "type": "integer" },
                        "top_sessions_truncated": { "type": "boolean" },
                        "redaction": {
                            "type": "object",
                            "properties": {
                                "private_text_policy": { "type": "string", "const": "suppress_all" },
                                "hash_strategy": { "type": "string", "const": "blake3_256_v1" },
                                "fields_emitted": { "type": "array", "items": { "type": "string" } },
                                "fields_suppressed": { "type": "array", "items": { "type": "string" } },
                                "opt_in_flags": { "type": "array", "items": { "type": "string" } }
                            },
                            "required": ["private_text_policy", "hash_strategy", "fields_emitted", "fields_suppressed", "opt_in_flags"]
                        }
                    },
                    "required": ["schema_version", "count_scope", "scan_units", "discovery", "top_sessions", "total_sessions", "total_hits", "top_sessions_truncated", "redaction"]
                },
                "_meta": response_schema_opaque_object()
            },
            "required": ["command", "data", "_meta"]
        }),
    );

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
                    "description": "Parallel-WAL shadow observer (Card 1, shadow-only phase). Activates under CASS_INDEXER_PARALLEL_WAL=shadow. Records per-chunk wall-clock on begin-concurrent writes so operators can assess what an epoch-ordered group-commit coordinator would have decided. NEVER changes commit semantics.",
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

    // `coding_agent_session_search-0h4cx`: extend response_schemas to
    // cover every advertised JSON surface from README line 103. The
    // surfaces below — doctor / models-status / models-verify — were
    // absent pre-fix even though their goldens live under
    // tests/golden/robot/. An agent reading response_schemas to drive
    // schema-aware parsing now finds a pinned schema for every surface
    // cass commits to.
    let mut doctor_schema = json!({
        "type": "object",
        "description": "cass doctor --json: diagnostic checks + optional auto-fix audit.",
        "properties": {
            "status": { "type": "string" },
            "health_class": { "type": "string", "description": "Stable kebab-case DoctorHealth value such as healthy, degraded-derived-assets, degraded-archive-risk, repair-blocked, repair-previously-failed, or source-authority-unsafe." },
            "risk_level": { "type": "string", "description": "none | low | medium | high archive/user-data risk summary for first-pass automation." },
            "healthy": { "type": "boolean" },
            "initialized": { "type": "boolean" },
            "explanation": { "type": ["string", "null"] },
            "recommended_action": { "type": ["string", "null"] },
            "fallback_mode": { "type": "string", "description": "Realized fallback tier for default hybrid search, usually lexical when semantic assets are unavailable." },
            "issues_found": { "type": "integer" },
            "issues_fixed": { "type": "integer" },
            "failures": { "type": "integer" },
            "warnings": { "type": "integer" },
            "needs_rebuild": { "type": "boolean" },
            "auto_fix_applied": { "type": "boolean" },
            "auto_fix_actions": { "type": "array", "items": { "type": "string" } },
            "doctor_command": response_schema_opaque_object(),
            "doctor_v2_contract": response_schema_doctor_v2_contract_summary(),
            "check_scope": response_schema_opaque_object(),
            "repair_previously_failed": { "type": "boolean" },
            "failure_marker_path": { "type": ["string", "null"] },
            "repeat_refusal_reason": { "type": ["string", "null"] },
            "override_available": { "type": "boolean" },
            "override_used": { "type": "boolean" },
            "active_repair": response_schema_opaque_object(),
            "post_repair_probes": response_schema_opaque_object(),
            "repair_failure_marker": response_schema_doctor_repair_failure_marker(),
            "failure_marker_write_error": { "type": ["string", "null"] },
            "failure_context": response_schema_opaque_object(),
            "operation_outcome": response_schema_doctor_operation_outcome(),
            "operation_state": response_schema_doctor_operation_state(),
            "locks": response_schema_doctor_locks(),
            "slow_operations": response_schema_doctor_slow_operations(),
            "timing_summary": response_schema_doctor_timing_summary(),
            "retry_recommendation": response_schema_doctor_retry_recommendation(),
            "safe_auto_eligibility": response_schema_opaque_object(),
            "root_cause": response_schema_root_cause_attribution(),
            "primary_incident_id": { "type": ["string", "null"], "description": "incident_id for the highest-priority root-cause incident, or null when no incident was found." },
            "incidents": {
                "type": "array",
                "description": "Root-cause incident groups derived from checks, coverage, lock, and candidate state. Robots should use root_cause_kind and evidence_check_ids instead of scraping check prose.",
                "items": {
                    "type": "object",
                    "properties": {
                        "schema_version": { "type": "integer" },
                        "incident_id": { "type": "string" },
                        "root_cause_kind": { "type": "string" },
                        "severity": { "type": "string" },
                        "affected_asset_classes": { "type": "array", "items": { "type": "string" } },
                        "archive_risk_level": { "type": "string" },
                        "derived_risk_level": { "type": "string" },
                        "confidence": { "type": "string" },
                        "evidence_check_ids": { "type": "array", "items": { "type": "string" } },
                        "blocked_actions": { "type": "array", "items": { "type": "string" } },
                        "safe_next_actions": { "type": "array", "items": { "type": "string" } },
                        "stale_or_unknown_fields": { "type": "array", "items": { "type": "string" } },
                        "redacted_evidence_paths": { "type": "array", "items": { "type": "string" } },
                        "summary": { "type": "string" }
                    },
                    "required": ["schema_version", "incident_id", "root_cause_kind", "severity", "affected_asset_classes", "archive_risk_level", "derived_risk_level", "confidence", "evidence_check_ids", "blocked_actions", "safe_next_actions", "stale_or_unknown_fields", "redacted_evidence_paths", "summary"]
                }
            },
            "event_log": response_schema_doctor_event_log_metadata(),
            "lexical": response_schema_index_state(),
            "semantic": response_schema_semantic_state(),
            "derived_semantic_assets": response_schema_doctor_derived_semantic_assets(),
            "storage_pressure": response_schema_opaque_object(),
            "asset_taxonomy": response_schema_opaque_object_array(),
            "anomaly_taxonomy": response_schema_opaque_object_array(),
            "repair_contract": response_schema_opaque_object(),
            "config_exclusion_risks": {
                "type": "array",
                "items": response_schema_doctor_config_exclusion_risk()
            },
            "source_inventory": response_schema_doctor_source_inventory(),
            "raw_mirror": response_schema_doctor_raw_mirror(),
            "raw_mirror_backfill": response_schema_doctor_raw_mirror_backfill(),
            "coverage_summary": response_schema_doctor_coverage_summary(),
            "sole_copy_warnings": { "type": "array", "items": response_schema_opaque_object() },
            "detail_output": response_schema_doctor_detail_output(),
            "coverage_risk": response_schema_doctor_coverage_risk(),
            "source_authority": response_schema_doctor_source_authority(),
            "candidate_staging": response_schema_doctor_candidate_staging(),
            "quarantine": response_schema_opaque_object(),
            "repair_plan": response_schema_doctor_repair_plan_preview(),
            "cleanup_apply": response_schema_doctor_cleanup_apply(),
            "_meta": response_schema_opaque_object(),
            "checks": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                        "status": { "type": "string", "description": "pass | warn | fail" },
                        "message": { "type": "string" },
                        "anomaly_class": { "type": "string", "description": "Stable kebab-case DoctorAnomaly value; robots should branch on this instead of message text." },
                        "health_class": { "type": "string", "description": "Stable kebab-case DoctorHealth value derived from anomaly_class." },
                        "severity": { "type": "string", "description": "info | warn | error" },
                        "affected_asset_class": { "type": "string", "description": "Stable DoctorAssetClass value naming the asset class at risk." },
                        "data_loss_risk": { "type": "string", "description": "none | low | medium | high | unknown" },
                        "recommended_action": { "type": "string" },
                        "safe_for_auto_repair": { "type": "boolean" },
                        "default_outcome_kind": { "type": "string" },
                        "fix_available": { "type": "boolean" },
                        "fix_applied": { "type": "boolean" }
                    },
                    "required": ["name", "status", "message", "anomaly_class", "health_class", "severity", "affected_asset_class", "data_loss_risk", "recommended_action", "safe_for_auto_repair", "default_outcome_kind", "fix_available", "fix_applied"]
                }
            }
        },
        "required": ["status", "health_class", "risk_level", "healthy", "initialized", "recommended_action", "fallback_mode", "doctor_command", "check_scope", "repair_previously_failed", "failure_marker_path", "repeat_refusal_reason", "override_available", "override_used", "active_repair", "post_repair_probes", "repair_failure_marker", "operation_outcome", "operation_state", "locks", "slow_operations", "timing_summary", "retry_recommendation", "safe_auto_eligibility", "primary_incident_id", "incidents", "event_log", "lexical", "semantic", "derived_semantic_assets", "storage_pressure", "config_exclusion_risks", "raw_mirror_backfill", "coverage_summary", "sole_copy_warnings", "detail_output", "coverage_risk", "source_authority", "candidate_staging", "checks"]
    });
    let doctor_properties = doctor_schema
        .get_mut("properties")
        .and_then(|value| value.as_object_mut())
        .expect("doctor schema properties object");
    doctor_properties.insert("schema_version".to_string(), json!({ "type": "integer" }));
    doctor_properties.insert(
        "doctor_contract_version".to_string(),
        json!({ "type": "integer" }),
    );
    doctor_properties.insert("capabilities_url".to_string(), json!({ "type": "string" }));
    // `.14.1` storage-integrity block (bead vl1cj). Inserted post-construction
    // (not inside the json! literal) so the giant doctor_schema macro stays
    // under the serde_json recursion limit.
    doctor_properties.insert(
        "storage_integrity".to_string(),
        response_schema_storage_integrity(),
    );
    doctor_properties.insert(
        "daemon_runtime".to_string(),
        response_schema_opaque_object(),
    );
    // #287 degradation taxonomy: `reason_code` is the primary entry of
    // `degraded_reason_codes`; both null/empty on healthy runs. The `budget`
    // envelope mirrors the search/pack budget block (gh352 family).
    doctor_properties.insert(
        "reason_code".to_string(),
        json!({ "type": ["string", "null"] }),
    );
    doctor_properties.insert(
        "degraded_reason_codes".to_string(),
        json!({ "type": "array", "items": { "type": "string" } }),
    );
    doctor_properties.insert("budget".to_string(), response_schema_budget_block());
    doctor_schema
        .get_mut("required")
        .and_then(|value| value.as_array_mut())
        .expect("doctor schema required array")
        .push(json!("daemon_runtime"));
    schemas.insert("doctor".to_string(), doctor_schema);

    schemas.insert(
        "doctor-archive-normalize".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Schema-first contract for archive normalization. Dry-run/apply implementations must report coverage deltas, redaction status, and receipt artifacts before any canonical archive mutation.",
                surface: "archive-normalize",
                mode: "repair-dry-run",
                status: "planned",
                outcome_kind: "planned",
                asset_class: "canonical_archive_db",
                risk_level: "medium",
                fallback_mode: "read-only",
                authority_status: "selected",
                recommended_action: "cass doctor archive-normalize --dry-run --json",
            },
            [
                ("normalization_candidates", response_schema_opaque_object_array()),
                ("repair_plan", response_schema_doctor_repair_plan_preview()),
                ("safety_gates", serde_json::json!({ "type": "array", "items": response_schema_doctor_safety_gate() })),
                ("forensic_bundle", response_schema_doctor_forensic_bundle_metadata()),
                ("detail_output", response_schema_doctor_detail_output()),
            ],
        ),
    );
    schemas.insert(
        "doctor-archive-scan".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Read-only archive scan contract covering canonical DB, raw mirror, current source visibility, sole-copy warnings, and source-authority evidence.",
                surface: "archive-scan",
                mode: "read-only-check",
                status: "ok",
                outcome_kind: "no_op",
                asset_class: "canonical_archive_db",
                risk_level: "low",
                fallback_mode: "read-only",
                authority_status: "read_only",
                recommended_action: "cass doctor check --json",
            },
            [
                ("source_inventory", response_schema_doctor_source_inventory()),
                ("config_exclusion_risks", serde_json::json!({
                    "type": "array",
                    "items": response_schema_doctor_config_exclusion_risk()
                })),
                ("raw_mirror", response_schema_doctor_raw_mirror()),
                ("coverage_summary", response_schema_doctor_coverage_summary()),
                ("sole_copy_warnings", serde_json::json!({ "type": "array", "items": response_schema_opaque_object() })),
                ("detail_output", response_schema_doctor_detail_output()),
                ("source_authority", response_schema_doctor_source_authority()),
            ],
        ),
    );
    schemas.insert(
        "doctor-backups-list".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Read-only backup inventory contract. Backup metadata and redacted paths are data; restore decisions require a separate rehearsal receipt.",
                surface: "backups",
                mode: "read-only-check",
                status: "ok",
                outcome_kind: "no_op",
                asset_class: "backup_bundle",
                risk_level: "low",
                fallback_mode: "read-only",
                authority_status: "read_only",
                recommended_action: "cass doctor backups list --json",
            },
            [
                ("backups", serde_json::json!({ "type": "array", "items": response_schema_opaque_object() })),
                ("retention_policy", response_schema_opaque_object()),
                ("verify_command", serde_json::json!({ "type": "string" })),
                ("restore_rehearsal_command", serde_json::json!({ "type": "string" })),
            ],
        ),
    );
    schemas.insert(
        "doctor-backups-verify".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Backup verification contract with checksum and manifest status for every candidate backup before restore rehearsal.",
                surface: "backups",
                mode: "read-only-check",
                status: "ok",
                outcome_kind: "no_op",
                asset_class: "backup_bundle",
                risk_level: "low",
                fallback_mode: "read-only",
                authority_status: "selected",
                recommended_action: "cass doctor backups verify --json",
            },
            [
                ("backup_verification", response_schema_opaque_object()),
                ("artifact_manifest", response_schema_doctor_artifact_manifest()),
                ("checksum_status_counts", serde_json::json!({ "type": "object", "additionalProperties": { "type": "integer" } })),
                ("restore_rehearsal_allowed", serde_json::json!({ "type": "boolean" })),
            ],
        ),
    );
    schemas.insert(
        "doctor-baseline-diff".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Read-only diagnostic baseline diff. Intended for before/after doctor comparisons with no source or archive mutation.",
                surface: "baseline-diff",
                mode: "baseline-diff",
                status: "unchanged",
                outcome_kind: "no_op",
                asset_class: "operation_receipt",
                risk_level: "low",
                fallback_mode: "not-applicable",
                authority_status: "read_only",
                recommended_action: "cass doctor baseline-diff --json",
            },
            [
                ("baseline_diff", response_schema_opaque_object()),
                ("baseline_id", serde_json::json!({ "type": "string" })),
                ("baseline_path", serde_json::json!({ "type": "string" })),
                ("compared_at", serde_json::json!({ "type": "integer" })),
                ("added_anomalies", serde_json::json!({ "type": "array", "items": { "type": "string" } })),
                ("removed_anomalies", serde_json::json!({ "type": "array", "items": { "type": "string" } })),
                ("changed_assets", serde_json::json!({ "type": "array", "items": response_schema_opaque_object() })),
                ("changed_recommendations", serde_json::json!({ "type": "array", "items": response_schema_opaque_object() })),
                ("perf_delta", response_schema_opaque_object()),
                ("privacy_redaction_status", response_schema_opaque_object()),
                ("event_log_path", serde_json::json!({ "type": ["string", "null"] })),
                ("artifact_manifest_path", serde_json::json!({ "type": "string" })),
                ("baseline_mutated", serde_json::json!({ "type": "boolean" })),
            ],
        ),
    );
    schemas.insert(
        "doctor-baseline-save".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Explicit diagnostic baseline save/update contract. The saved baseline is redacted and is not a backup or restore point.",
                surface: "baseline",
                mode: "baseline-save",
                status: "saved",
                outcome_kind: "applied",
                asset_class: "operation_receipt",
                risk_level: "low",
                fallback_mode: "not-applicable",
                authority_status: "selected",
                recommended_action: "cass doctor baseline diff <id> --json",
            },
            [
                ("baseline_id", serde_json::json!({ "type": "string" })),
                ("baseline_path", serde_json::json!({ "type": "string" })),
                ("redacted_baseline_path", serde_json::json!({ "type": "string" })),
                ("baseline_checksum", serde_json::json!({ "type": ["string", "null"] })),
                ("baseline_mutated", serde_json::json!({ "type": "boolean" })),
                ("baseline_document", response_schema_opaque_object()),
                ("write_error", serde_json::json!({ "type": ["string", "null"] })),
                ("diagnostic_only", serde_json::json!({ "type": "boolean" })),
                ("backup_semantics", serde_json::json!({ "type": "string" })),
            ],
        ),
    );
    schemas.insert(
        "doctor-baseline-update".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Explicit diagnostic baseline update contract. Replacing a baseline is an operator-selected diagnostic snapshot refresh, not a backup or restore point.",
                surface: "baseline",
                mode: "baseline-update",
                status: "updated",
                outcome_kind: "applied",
                asset_class: "operation_receipt",
                risk_level: "low",
                fallback_mode: "not-applicable",
                authority_status: "selected",
                recommended_action: "cass doctor baseline diff <id> --json",
            },
            [
                ("baseline_id", serde_json::json!({ "type": "string" })),
                ("baseline_path", serde_json::json!({ "type": "string" })),
                ("redacted_baseline_path", serde_json::json!({ "type": "string" })),
                ("baseline_checksum", serde_json::json!({ "type": ["string", "null"] })),
                ("baseline_mutated", serde_json::json!({ "type": "boolean" })),
                ("baseline_document", response_schema_opaque_object()),
                ("write_error", serde_json::json!({ "type": ["string", "null"] })),
                ("diagnostic_only", serde_json::json!({ "type": "boolean" })),
                ("backup_semantics", serde_json::json!({ "type": "string" })),
            ],
        ),
    );
    schemas.insert(
        "doctor-check".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Explicit read-only doctor check contract. It is the preferred first call before any repair, cleanup, restore, or reconstruct command.",
                surface: "check",
                mode: "read-only-check",
                status: "ok",
                outcome_kind: "no_op",
                asset_class: "canonical_archive_db",
                risk_level: "low",
                fallback_mode: "lexical",
                authority_status: "read_only",
                recommended_action: "no action needed; inspect checks before repair",
            },
            [
                ("checks", serde_json::json!({ "type": "array", "items": response_schema_opaque_object() })),
                ("doctor_command", response_schema_opaque_object()),
                ("operation_state", response_schema_doctor_operation_state()),
                ("repair_readiness", response_schema_opaque_object()),
                ("safe_auto_eligibility", response_schema_opaque_object()),
            ],
        ),
    );
    schemas.insert(
        "doctor-cleanup-apply".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Fingerprint-approved cleanup apply receipt. It may prune only derived reclaimable assets after safety gates pass and must never delete source session logs or raw mirror evidence.",
                surface: "cleanup",
                mode: "cleanup-apply",
                status: "applied",
                outcome_kind: "applied",
                asset_class: "reclaimable_derived_cache",
                risk_level: "low",
                fallback_mode: "not-applicable",
                authority_status: "selected",
                recommended_action: "cass doctor check --json",
            },
            [
                ("cleanup_apply", response_schema_doctor_cleanup_apply()),
                ("receipt", response_schema_doctor_receipt()),
                ("post_repair_probes", response_schema_opaque_object()),
                ("validation_summary", response_schema_opaque_object()),
            ],
        ),
    );
    schemas.insert(
        "doctor-cleanup-dry-run".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Cleanup dry-run inventory. It reports reclaim candidates, safe_to_gc advice, and a plan fingerprint without mutating the filesystem.",
                surface: "cleanup",
                mode: "cleanup-dry-run",
                status: "planned",
                outcome_kind: "planned",
                asset_class: "reclaimable_derived_cache",
                risk_level: "low",
                fallback_mode: "not-applicable",
                authority_status: "read_only",
                recommended_action: "cass doctor cleanup --yes --plan-fingerprint <fingerprint> --json",
            },
            [
                ("cleanup_plan", response_schema_doctor_repair_plan_preview()),
                ("reclaim_candidates", serde_json::json!({ "type": "array", "items": response_schema_opaque_object() })),
                ("safe_to_gc_count", serde_json::json!({ "type": "integer" })),
                ("planned_reclaim_bytes", serde_json::json!({ "type": "integer" })),
            ],
        ),
    );
    schemas.insert(
        "doctor-error-envelope".to_string(),
        response_schema_doctor_v2_error_envelope(),
    );
    schemas.insert(
        "doctor-failure-context".to_string(),
        response_schema_doctor_failure_context(),
    );
    schemas.insert(
        "doctor-health-summary".to_string(),
        response_schema_doctor_v2_summary("health-summary"),
    );
    schemas.insert(
        "doctor-reconstruct-dry-run".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Reconstruct dry-run contract. Candidate generation writes only isolated doctor/candidates artifacts and reports promotion gates before any canonical archive replacement.",
                surface: "reconstruct",
                mode: "reconstruct-dry-run",
                status: "planned",
                outcome_kind: "planned",
                asset_class: "raw_mirror_blob",
                risk_level: "medium",
                fallback_mode: "read-only",
                authority_status: "candidate_only",
                recommended_action: "inspect candidate_staging before any promote command",
            },
            [
                ("candidate_staging", response_schema_doctor_candidate_staging()),
                ("source_authority", response_schema_doctor_source_authority()),
                ("forensic_bundle", response_schema_doctor_forensic_bundle_metadata()),
                ("promotion_allowed", serde_json::json!({ "type": "boolean" })),
                ("promotion_blockers", serde_json::json!({ "type": "array", "items": { "type": "string" } })),
            ],
        ),
    );
    schemas.insert(
        "doctor-repair-dry-run".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Explicit repair dry-run contract. It returns a canonical plan_fingerprint and exact apply argv while remaining read-only.",
                surface: "repair",
                mode: "repair-dry-run",
                status: "planned",
                outcome_kind: "planned",
                asset_class: "derived_lexical_index",
                risk_level: "low",
                fallback_mode: "lexical",
                authority_status: "selected",
                recommended_action: "cass doctor repair --yes --plan-fingerprint <fingerprint> --json",
            },
            [
                ("repair_plan", response_schema_doctor_repair_plan_preview()),
                ("exact_apply_command", serde_json::json!({ "type": "string" })),
                ("apply_argv", serde_json::json!({ "type": "array", "items": { "type": "string" } })),
                ("safe_auto_eligible", serde_json::json!({ "type": "boolean" })),
                ("post_repair_probes_required", serde_json::json!({ "type": "boolean" })),
            ],
        ),
    );
    schemas.insert(
        "doctor-repair-receipt".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Fingerprint-approved repair receipt contract with event log, artifact manifest, post-repair probe summary, and remaining risk fields.",
                surface: "repair",
                mode: "fingerprint-apply",
                status: "applied",
                outcome_kind: "applied",
                asset_class: "derived_lexical_index",
                risk_level: "low",
                fallback_mode: "lexical",
                authority_status: "selected",
                recommended_action: "cass doctor check --json",
            },
            [
                ("receipt", response_schema_doctor_receipt()),
                ("post_repair_probes", response_schema_opaque_object()),
                ("verification_summary", response_schema_opaque_object()),
                ("remaining_risk", serde_json::json!({ "type": "array", "items": { "type": "string" } })),
            ],
        ),
    );
    schemas.insert(
        "doctor-restore-rehearsal".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Restore rehearsal contract. It validates backup bundles and produces a rehearsal receipt before any restore apply mode can touch canonical archive assets.",
                surface: "restore",
                mode: "restore-rehearsal",
                status: "planned",
                outcome_kind: "planned",
                asset_class: "backup_bundle",
                risk_level: "medium",
                fallback_mode: "read-only",
                authority_status: "selected",
                recommended_action: "inspect rehearsal receipt before restore apply",
            },
            [
                ("restore_rehearsal", response_schema_opaque_object()),
                ("backup_verification", response_schema_opaque_object()),
                ("rehearsal_receipt", response_schema_doctor_receipt()),
                ("restore_apply_allowed", serde_json::json!({ "type": "boolean" })),
            ],
        ),
    );
    schemas.insert(
        "doctor-safe-auto-run".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Safe auto-run contract for legacy cass doctor --fix. It may apply only predeclared safe actions and must emit receipts for every mutation.",
                surface: "legacy-doctor",
                mode: "safe-auto-fix",
                status: "partial",
                outcome_kind: "partial",
                asset_class: "reclaimable_derived_cache",
                risk_level: "low",
                fallback_mode: "lexical",
                authority_status: "selected",
                recommended_action: "cass doctor check --json",
            },
            [
                ("safe_auto_eligibility", response_schema_opaque_object()),
                ("actions", serde_json::json!({ "type": "array", "items": response_schema_doctor_action() })),
                ("receipts", serde_json::json!({ "type": "array", "items": response_schema_doctor_receipt() })),
                ("skipped_actions", serde_json::json!({ "type": "array", "items": response_schema_opaque_object() })),
            ],
        ),
    );
    schemas.insert(
        "doctor-semantic-model-fallback".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Semantic/model fallback contract. Missing model files or vector backlog degrade to lexical and should not block lexical search or archive repair.",
                surface: "semantic-model-fallback",
                mode: "read-only-check",
                status: "warn",
                outcome_kind: "no_op",
                asset_class: "model_cache",
                risk_level: "low",
                fallback_mode: "lexical",
                authority_status: "read_only",
                recommended_action: "cass models status --json",
            },
            [
                ("semantic", response_schema_semantic_state()),
                (
                    "derived_semantic_assets",
                    response_schema_doctor_derived_semantic_assets(),
                ),
                ("model_status", response_schema_opaque_object()),
                ("fallback_reason", serde_json::json!({ "type": ["string", "null"] })),
                ("model_install_command", serde_json::json!({ "type": ["string", "null"] })),
            ],
        ),
    );
    schemas.insert(
        "doctor-status-summary".to_string(),
        response_schema_doctor_v2_summary("status-summary"),
    );
    schemas.insert(
        "doctor-support-bundle".to_string(),
        response_schema_doctor_v2_surface(
            DoctorV2SurfaceSchemaSpec {
                description: "Support bundle contract. Bundles are scrubbed by default, include manifests/checksums, and require explicit opt-in for sensitive raw attachments.",
                surface: "support-bundle",
                mode: "support-bundle",
                status: "ok",
                outcome_kind: "no_op",
                asset_class: "support_bundle",
                risk_level: "low",
                fallback_mode: "not-applicable",
                authority_status: "read_only",
                recommended_action: "share artifact_manifest_path only after reviewing redaction_status",
            },
            [
                ("support_bundle", response_schema_doctor_forensic_bundle_metadata()),
                ("artifact_manifest", response_schema_doctor_artifact_manifest()),
                ("redaction_report", response_schema_opaque_object()),
                ("sensitive_attachment_policy", response_schema_opaque_object()),
                ("verification_summary", response_schema_opaque_object()),
            ],
        ),
    );

    schemas.insert(
        "models-status".to_string(),
        json!({
            "type": "object",
            "description": "cass models status --json: semantic-model acquisition + cache state.",
            "properties": {
                "policy_quality_tier_embedder": { "type": "string" },
                "active_registry_name": { "type": ["string", "null"] },
                "lexical_fail_open": { "type": "boolean" },
                "models": response_schema_opaque_object_array(),
                "model_id": { "type": "string" },
                "model_dir": { "type": "string" },
                "installed": { "type": "boolean" },
                "state": { "type": "string", "description": "not_acquired | acquired | checksum_mismatch | incompatible_version | disabled_by_policy | budget_blocked | quarantined_corrupt | preseeded_local" },
                "state_detail": { "type": "string" },
                "next_step": { "type": "string" },
                "revision": { "type": "string" },
                "license": { "type": "string" },
                "total_size_bytes": { "type": "integer" },
                "installed_size_bytes": { "type": "integer" },
                "observed_file_bytes": { "type": "integer" },
                "policy_source": { "type": "string" },
                "cache_lifecycle": {
                    "type": "object",
                    "additionalProperties": true,
                    "description": "Opaque lifecycle block describing cache state and missing files."
                },
                "model_acquisition": {
                    "type": "object",
                    "additionalProperties": true,
                    "description": "Hardened acquisition contract: state (absent|partial_install|ready|checksum_mismatch|quarantined_corrupt|budget_blocked|disabled_by_policy|baseline_no_semantic|incompatible_runtime|runtime_load_failed), runtime, source, cost_class, expected_install_bytes, fallback_mode, next_step, next_command, state_detail, rollback_guidance, fingerprint{revision,content_digest,marker_token}, and skipped_network_reason (the typed proof cass never acquires models outside `cass models install --from-file`)."
                },
                "files": response_schema_opaque_object_array()
            },
            "required": ["policy_quality_tier_embedder", "lexical_fail_open", "models"]
        }),
    );

    schemas.insert(
        "models-verify".to_string(),
        json!({
            "type": "object",
            "description": "cass models verify --json: per-file SHA-256 verification of the installed semantic model.",
            "properties": {
                "status": { "type": "string" },
                "state_detail": { "type": "string" },
                "next_step": { "type": "string" },
                "lexical_fail_open": { "type": "boolean" },
                "model_dir": { "type": "string" },
                "all_valid": { "type": "boolean" },
                "cache_lifecycle": response_schema_opaque_object(),
                "model_acquisition": {
                    "type": "object",
                    "additionalProperties": true,
                    "description": "Hardened acquisition contract (same shape as models-status.model_acquisition): state, runtime, source, cost_class, fingerprint, and skipped_network_reason proving verify never auto-downloads."
                },
                "error": { "type": ["string", "null"] }
            },
            "required": ["status", "all_valid", "lexical_fail_open"]
        }),
    );

    schemas.insert(
        "models-build-hnsw".to_string(),
        json!({
            "type": "object",
            "description": "cass models build-hnsw --json (gh#408): builds or verifies (--check) the HNSW accelerator for an already-published semantic artifact without opening the archive or embedding; state_* use the runtime loader's admissibility vocabulary.",
            "properties": {
                "tier": { "type": "string", "description": "fast | quality" },
                "embedder_id": { "type": "string" },
                "vector_count": { "type": "integer" },
                "artifact_path": { "type": "string", "description": "Published .fsvi path relative to data_dir" },
                "hnsw_path": { "type": "string", "description": "Accelerator metadata path relative to data_dir" },
                "state_before": { "type": "string", "description": "missing | invalid_entry | native_valid | stale_or_legacy | unreadable" },
                "state_after": { "type": "string", "description": "Same vocabulary as state_before; equals state_before for check_only" },
                "action": { "type": "string", "description": "check_only | unchanged | rebuilt" },
                "manifest_published": { "type": "boolean", "description": "True when an HnswRecord was written to the semantic manifest in this run" },
                "manifest_recorded": { "type": "boolean", "description": "True when the semantic manifest now carries a ready HnswRecord for this tier/embedder (written this run or already present)" },
                "current": { "type": "boolean", "description": "True when state_after is native_valid (approximate search usable)" },
                "next_command": { "type": ["string", "null"], "description": "cass models build-hnsw when the accelerator is not current" }
            },
            "required": ["tier", "embedder_id", "vector_count", "hnsw_path", "state_before", "state_after", "action", "manifest_published", "current"]
        }),
    );

    schemas
}
