pub(crate) fn response_schema_index_state() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "exists": { "type": "boolean" },
            "status": { "type": "string" },
            "reason": { "type": ["string", "null"] },
            "fresh": { "type": "boolean" },
            "partial": { "type": "boolean" },
            "partial_reason": { "type": ["string", "null"] },
            "last_indexed_at": { "type": ["string", "null"] },
            "age_seconds": { "type": ["integer", "null"] },
            "stale": { "type": "boolean" },
            "stale_threshold_seconds": { "type": "integer" },
            "engine_incompatible": { "type": "boolean" },
            "rebuilding": { "type": "boolean" },
            "stalled": { "type": "boolean" },
            "activity_at": { "type": ["string", "null"] },
            "documents": { "type": ["integer", "null"] },
            "live_documents": { "type": ["integer", "null"] },
            "hollow": { "type": "boolean" },
            "manifest_source": {
                "type": ["string", "null"],
                "enum": ["current", "previous_after_missing_current", "previous_after_corrupt_current", null]
            },
            "segment_files": { "type": ["integer", "null"] },
            "empty_with_messages": { "type": "boolean" },
            "quarantined_conversations": { "type": "integer" },
            "fingerprint": {
                "type": "object",
                "properties": {
                    "current_db_fingerprint": { "type": ["string", "null"] },
                    "checkpoint_fingerprint": { "type": ["string", "null"] },
                    "matches_current_db_fingerprint": { "type": ["boolean", "null"] }
                }
            },
            "checkpoint": {
                "type": "object",
                "properties": {
                    "present": { "type": "boolean" },
                    "completed": { "type": ["boolean", "null"] },
                    "db_matches": { "type": ["boolean", "null"] },
                    "schema_matches": { "type": ["boolean", "null"] },
                    "page_size_matches": { "type": ["boolean", "null"] },
                    "page_size_compatible": { "type": ["boolean", "null"] }
                }
            }
        }
    })
}

pub(crate) fn response_schema_ingest_quarantine() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "schema_version": { "type": "integer" },
            "status": { "type": "string" },
            "quarantined_conversations": { "type": "integer" },
            "deferred_conversations": { "type": "integer" },
            "recent_quarantined_conversations": { "type": "integer" },
            "recent_window_seconds": { "type": "integer" },
            "circuit_breaker_limit": { "type": "integer" },
            "circuit_breaker_active": { "type": "boolean" },
            "quarantine_files": { "type": "array", "items": { "type": "string" } },
            "newest_last_attempt_at_ms": { "type": ["integer", "null"] },
            "recommended_action": { "type": ["string", "null"] }
        }
    })
}

pub(crate) fn response_schema_state_database() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "exists": { "type": "boolean" },
            "db_bytes": { "type": "integer" },
            "wal_bytes": { "type": "integer" },
            "shm_present": { "type": "boolean" },
            "opened": { "type": "boolean" },
            "conversations": { "type": ["integer", "null"] },
            "messages": { "type": ["integer", "null"] },
            "open_error": { "type": ["string", "null"] },
            "open_retryable": { "type": "boolean" },
            "counts_skipped": { "type": "boolean" },
            "open_skipped": { "type": "boolean" },
            "integrity": { "type": "string" },
            "integrity_detail": { "type": ["string", "null"] },
            "integrity_hint": { "type": ["string", "null"] }
        }
    })
}

pub(crate) fn response_schema_status_database() -> serde_json::Value {
    let mut schema = response_schema_state_database();
    let props = schema
        .get_mut("properties")
        .and_then(serde_json::Value::as_object_mut)
        .expect("status database schema should expose properties");
    props.insert("path".to_string(), serde_json::json!({ "type": "string" }));
    schema
}

pub(crate) fn response_schema_health_db() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "exists": { "type": "boolean" },
            "opened": { "type": "boolean" },
            "conversations": { "type": ["integer", "null"] },
            "messages": { "type": ["integer", "null"] },
            "open_error": { "type": ["string", "null"] },
            "counts_skipped": { "type": "boolean" },
            "open_skipped": { "type": "boolean" }
        }
    })
}

pub(crate) fn response_schema_recommended_commands() -> serde_json::Value {
    serde_json::json!({
        "type": "array",
        "items": {
            "type": "object",
            "description": "Exact follow-up command a robot can run for the same cass dataset.",
            "properties": {
                "id": { "type": "string" },
                "command": { "type": "string" },
                "purpose": { "type": "string" },
                "safety": { "type": "string" },
                "run_when": { "type": "string" },
                "success_signal": { "type": "string" },
                "parse_fields": {
                    "type": "array",
                    "items": { "type": "string" }
                },
                "retry_after_ms": { "type": ["integer", "null"] }
            },
            "required": [
                "id",
                "command",
                "purpose",
                "safety",
                "run_when",
                "success_signal",
                "parse_fields",
                "retry_after_ms"
            ]
        }
    })
}

pub(crate) fn response_schema_pending_state() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "sessions": { "type": "integer" },
            "lexical_replay": { "type": ["boolean", "null"] },
            "watch_active": { "type": "boolean" },
            "orphaned": { "type": "boolean" }
        }
    })
}

pub(crate) fn response_schema_rebuild_state() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "active": { "type": "boolean" },
            "orphaned": { "type": "boolean" },
            "engine_incompatible": { "type": "boolean" },
            "pid": { "type": ["integer", "null"] },
            "mode": { "type": ["string", "null"] },
            "job_id": { "type": ["string", "null"] },
            "job_kind": { "type": ["string", "null"] },
            "phase": { "type": ["string", "null"] },
            "stalled": { "type": "boolean" },
            "last_progress_at": { "type": ["string", "null"] },
            "last_progress_age_ms": { "type": ["integer", "null"] },
            "started_at": { "type": ["string", "null"] },
            "updated_at": { "type": ["string", "null"] },
            "processed_conversations": { "type": ["integer", "null"] },
            "total_conversations": { "type": ["integer", "null"] },
            "indexed_docs": { "type": ["integer", "null"] },
            "pipeline": {
                "type": "object",
                "description": "Lexical rebuild pipeline settings plus optional runtime telemetry.",
                "additionalProperties": true
            },
            "expected_docs_scan": {
                "type": ["object", "null"],
                "description": "Saved position of a running or interrupted full expected-docs scan; the next index run resumes it when the archive is unchanged or only grew.",
                "properties": {
                    "last_conversation_id": { "type": "integer" },
                    "max_conversation_id": { "type": "integer" },
                    "expected_docs_so_far": { "type": "integer" },
                    "saved_at": { "type": ["string", "null"] }
                }
            }
        }
    })
}

pub(crate) fn response_schema_rebuild_progress() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "active": { "type": "boolean" },
            // `stalled=true` means the rebuild is nominally active but
            // the indexing thread has not posted forward progress for
            // longer than CASS_REBUILD_STALL_DETECT_SECS (default 120s)
            // — even though the lock heartbeat is still being
            // refreshed. Distinguishes a wedged indexer from a
            // slow-but-progressing one. Regression #258.
            "stalled": { "type": "boolean" },
            // RFC3339 timestamp of the last forward-progress event
            // posted by the indexing thread itself.
            "last_progress_at": { "type": ["string", "null"] },
            "last_progress_age_ms": { "type": ["integer", "null"] },
            "mode": { "type": ["string", "null"] },
            "phase": { "type": ["string", "null"] },
            "processed_conversations": { "type": ["integer", "null"] },
            "total_conversations": { "type": ["integer", "null"] },
            "remaining_conversations": { "type": ["integer", "null"] },
            "completion_ratio": { "type": ["number", "null"] },
            "indexed_docs": { "type": ["integer", "null"] },
            "runtime_available": { "type": "boolean" },
            "queue_depth": { "type": ["integer", "null"] },
            "queue_capacity": { "type": ["integer", "null"] },
            "queue_headroom": { "type": ["integer", "null"] },
            "pending_batch_conversations": { "type": ["integer", "null"] },
            "pending_batch_message_bytes": { "type": ["integer", "null"] },
            "inflight_message_bytes": { "type": ["integer", "null"] },
            "max_message_bytes_in_flight": { "type": ["integer", "null"] },
            "inflight_message_bytes_headroom": { "type": ["integer", "null"] },
            "controller_mode": { "type": ["string", "null"] },
            "controller_reason": { "type": ["string", "null"] },
            "updated_at": { "type": ["string", "null"] }
        }
    })
}

pub(crate) fn response_schema_semantic_state() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "status": { "type": "string" },
            "availability": { "type": "string" },
            "summary": { "type": "string" },
            "available": { "type": "boolean" },
            "can_search": { "type": "boolean" },
            "fallback_mode": { "type": ["string", "null"] },
            "preferred_backend": { "type": "string" },
            "embedder_id": { "type": ["string", "null"] },
            "vector_index_path": { "type": ["string", "null"] },
            "model_dir": { "type": ["string", "null"] },
            "hnsw_path": { "type": ["string", "null"] },
            "hnsw_present": { "type": "boolean" },
            "hnsw_state": { "type": "string" },
            "hnsw_ready": { "type": "boolean" },
            "progressive_ready": { "type": "boolean" },
            "progressive_reason_code": { "type": ["string", "null"] },
            "feature_compiled_in": { "type": "boolean" },
            "quality_tier_published": { "type": "boolean" },
            "semantic_only_search_available": { "type": "boolean" },
            "hint": { "type": ["string", "null"] },
            "fast_tier": {
                "type": "object",
                "properties": {
                    "present": { "type": "boolean" },
                    "ready": { "type": "boolean" },
                    "current_db_matches": { "type": ["boolean", "null"] },
                    "conversation_count": { "type": ["integer", "null"] },
                    "doc_count": { "type": ["integer", "null"] },
                    "embedder_id": { "type": ["string", "null"] },
                    "model_revision": { "type": ["string", "null"] },
                    "completed_at": { "type": ["string", "null"] },
                    "size_bytes": { "type": ["integer", "null"] },
                    "shard_count": { "type": ["integer", "null"] },
                    "legacy_external_embedding_space": { "type": "boolean" }
                }
            },
            "quality_tier": {
                "type": "object",
                "properties": {
                    "present": { "type": "boolean" },
                    "ready": { "type": "boolean" },
                    "current_db_matches": { "type": ["boolean", "null"] },
                    "conversation_count": { "type": ["integer", "null"] },
                    "doc_count": { "type": ["integer", "null"] },
                    "embedder_id": { "type": ["string", "null"] },
                    "model_revision": { "type": ["string", "null"] },
                    "completed_at": { "type": ["string", "null"] },
                    "size_bytes": { "type": ["integer", "null"] },
                    "shard_count": { "type": ["integer", "null"] },
                    "legacy_external_embedding_space": { "type": "boolean" }
                }
            },
            "backlog": {
                "type": "object",
                "properties": {
                    "total_conversations": { "type": "integer" },
                    "fast_tier_processed": { "type": "integer" },
                    "fast_tier_remaining": { "type": "integer" },
                    "quality_tier_processed": { "type": "integer" },
                    "quality_tier_remaining": { "type": "integer" },
                    "pending_work": { "type": "boolean" },
                    "current_db_matches": { "type": ["boolean", "null"] },
                    "computed_at": { "type": ["string", "null"] }
                }
            },
            "checkpoint": {
                "type": "object",
                "properties": {
                    "active": { "type": "boolean" },
                    "tier": { "type": ["string", "null"] },
                    "current_db_matches": { "type": ["boolean", "null"] },
                    "completed": { "type": ["boolean", "null"] },
                    "conversations_processed": { "type": ["integer", "null"] },
                    "total_conversations": { "type": ["integer", "null"] },
                    "progress_pct": { "type": ["integer", "null"] },
                    "docs_embedded": { "type": ["integer", "null"] },
                    "last_offset": { "type": ["integer", "null"] },
                    "saved_at": { "type": ["string", "null"] }
                }
            }
        }
    })
}

pub(crate) fn response_schema_doctor_derived_semantic_component() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "asset_class": { "type": "string" },
            "safety_classification": { "type": "string" },
            "status": { "type": "string" },
            "safe_to_rebuild": { "type": "boolean" },
            "safe_for_auto_repair": { "type": "boolean" },
            "blocks_archive_recovery": { "type": "boolean" },
            "network_allowed": { "type": "boolean" },
            "auto_download_attempted": { "type": "boolean" },
            "path": { "type": ["string", "null"] },
            "redacted_path": { "type": ["string", "null"] },
            "current_db_matches": { "type": ["boolean", "null"] },
            "ready": { "type": ["boolean", "null"] },
            "present": { "type": ["boolean", "null"] },
            "note": { "type": "string" }
        }
    })
}

pub(crate) fn response_schema_doctor_derived_semantic_assets() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Read-only semantic/model/vector/memo derivative report. These assets are optional quality layers and never archive authority.",
        "properties": {
            "schema_version": { "type": "integer" },
            "status": { "type": "string" },
            "doctor_check_status": { "type": "string" },
            "asset_class": { "type": "string" },
            "safety_classification": { "type": "string" },
            "fallback_mode": { "type": "string" },
            "semantic_status": { "type": "string" },
            "semantic_availability": { "type": "string" },
            "can_search": { "type": "boolean" },
            "lexical_search_unblocked": { "type": "boolean" },
            "blocks_archive_recovery": { "type": "boolean" },
            "safe_to_rebuild": { "type": "boolean" },
            "safe_for_auto_repair": { "type": "boolean" },
            "network_allowed": { "type": "boolean" },
            "auto_download_allowed": { "type": "boolean" },
            "auto_download_attempted": { "type": "boolean" },
            "skipped_auto_download_reason": { "type": ["string", "null"] },
            "model_policy": { "type": "string" },
            "selected_repair_action": { "type": "string" },
            "recommended_action": { "type": "string" },
            "probe_duration_ms": { "type": "integer" },
            "redaction_status": { "type": "string" },
            "receipt_path": { "type": ["string", "null"] },
            "model_cache": response_schema_doctor_derived_semantic_component(),
            "vector_index": response_schema_doctor_derived_semantic_component(),
            "hnsw_index": response_schema_doctor_derived_semantic_component(),
            "memoization_cache": response_schema_doctor_derived_semantic_component(),
            "notes": { "type": "array", "items": { "type": "string" } }
        }
    })
}

pub(crate) fn response_schema_policy_registry() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Data-only registry of active runtime controller policies, their deterministic inputs, and fallback state.",
        "properties": {
            "schema_version": { "type": "string" },
            "controllers": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "controller_id": { "type": "string" },
                        "policy_id": { "type": "string" },
                        "policy_version": { "type": "string" },
                        "status": { "type": "string", "description": "active | disabled | fallback" },
                        "fallback_state": { "type": "string", "description": "not_needed | conservative | disabled" },
                        "conservative_fallback": { "type": "boolean" },
                        "decision_reason": { "type": "string" },
                        "inputs": {
                            "type": "object",
                            "additionalProperties": { "type": "string" }
                        }
                    },
                    "required": [
                        "controller_id",
                        "policy_id",
                        "policy_version",
                        "status",
                        "fallback_state",
                        "conservative_fallback",
                        "decision_reason",
                        "inputs"
                    ]
                }
            }
        },
        "required": ["schema_version", "controllers"]
    })
}

pub(crate) fn response_schema_topology_budget() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Advisory CPU/RAM budget plan derived from Linux /sys topology; fallback mode preserves current defaults.",
        "properties": {
            "schema_version": { "type": "string" },
            "topology": {
                "type": "object",
                "properties": {
                    "source": { "type": "string", "description": "linux_sysfs | fallback" },
                    "topology_class": { "type": "string", "description": "unknown | single_socket | single_socket_smt | many_core_single_socket | multi_socket_numa" },
                    "logical_cpus": { "type": "integer" },
                    "physical_cores": { "type": "integer" },
                    "sockets": { "type": "integer" },
                    "numa_nodes": { "type": "integer" },
                    "llc_groups": { "type": "integer" },
                    "smt_threads_per_core": { "type": "integer" },
                    "memory_total_bytes": { "type": ["integer", "null"], "description": "Memory budgets are sized from: the host total clamped to the tightest cgroup limit on this process's chain" },
                    "memory_available_bytes": { "type": ["integer", "null"] },
                    "host_memory_total_bytes": { "type": ["integer", "null"], "description": "The host's own total before any cgroup limit" },
                    "memory_limit_source": { "type": ["string", "null"], "description": "What set memory_total_bytes: host, or cgroup_v2:/<path> / cgroup_v1:/<path> for the level whose limit is below the host total" }
                }
            },
            "reserved_core_policy": {
                "type": "object",
                "properties": {
                    "reserved_cores": { "type": "integer" },
                    "policy": { "type": "string" },
                    "reason": { "type": "string" }
                }
            },
            "advisory_budgets": {
                "type": "object",
                "properties": {
                    "shard_builders": { "type": "integer" },
                    "merge_workers": { "type": "integer" },
                    "page_prep_workers": { "type": "integer" },
                    "semantic_batchers": { "type": "integer" },
                    "cache_cap_bytes": { "type": "integer" },
                    "max_inflight_bytes": { "type": "integer" }
                }
            },
            "current_defaults": {
                "type": "object",
                "properties": {
                    "available_parallelism": { "type": "integer" },
                    "reserved_cores": { "type": "integer" },
                    "shard_builders": { "type": "integer" },
                    "merge_workers": { "type": "integer" },
                    "page_prep_workers": { "type": "integer" },
                    "cache_cap_bytes": { "type": "integer" },
                    "max_inflight_bytes": { "type": "integer" }
                }
            },
            "fallback_active": { "type": "boolean" },
            "decision_reason": { "type": "string" },
            "proof_notes": { "type": "array", "items": { "type": "string" } }
        },
        "required": [
            "schema_version",
            "topology",
            "reserved_core_policy",
            "advisory_budgets",
            "current_defaults",
            "fallback_active",
            "decision_reason",
            "proof_notes"
        ]
    })
}

pub(crate) fn response_schema_state_meta() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "index": response_schema_index_state(),
            "database": response_schema_state_database(),
            "pending": response_schema_pending_state(),
            "rebuild": response_schema_rebuild_state(),
            "semantic": response_schema_semantic_state(),
            "ingest_quarantine": response_schema_ingest_quarantine(),
            "policy_registry": response_schema_policy_registry(),
            "_meta": {
                "type": "object",
                "properties": {
                    "timestamp": { "type": "string" },
                    "data_dir": { "type": "string" },
                    "db_path": { "type": "string" }
                }
            }
        }
    })
}

pub(crate) fn response_schema_index_freshness() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "exists": { "type": "boolean" },
            "status": { "type": "string" },
            "reason": { "type": ["string", "null"] },
            "fresh": { "type": "boolean" },
            "partial": { "type": ["boolean", "null"] },
            "partial_reason": { "type": ["string", "null"] },
            "last_indexed_at": { "type": ["string", "null"] },
            "age_seconds": { "type": ["integer", "null"] },
            "stale": { "type": "boolean" },
            "stale_threshold_seconds": { "type": "integer" },
            "engine_incompatible": { "type": "boolean" },
            "manifest_source": {
                "type": ["string", "null"],
                "enum": ["current", "previous_after_missing_current", "previous_after_corrupt_current", null]
            },
            "rebuilding": { "type": "boolean" },
            "pending_sessions": { "type": "integer" },
            "auto_refresh": {
                "type": ["object", "null"],
                "description": "Present when a stale/partial/behind index triggered stale-on-read catch-up evaluation (indexer::background_refresh). `outcome` is one of spawned | disabled | index_run_active | cooldown | guard_busy | spawn_failed.",
                "properties": {
                    "outcome": { "type": "string" },
                    "trigger": { "type": "string" },
                    "pid": { "type": ["integer", "null"] },
                    "reason": { "type": ["string", "null"] },
                    "remaining_secs": { "type": ["integer", "null"] },
                    "error": { "type": ["string", "null"] },
                    "consecutive_failures": { "type": ["integer", "null"] },
                    "detail": { "type": ["string", "null"] }
                }
            }
        }
    })
}

pub(crate) fn response_schema_object(
    properties: impl IntoIterator<Item = (&'static str, serde_json::Value)>,
) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (key, value) in properties {
        map.insert(key.to_string(), value);
    }
    serde_json::Value::Object(
        [
            ("type".to_string(), serde_json::json!("object")),
            ("properties".to_string(), serde_json::Value::Object(map)),
        ]
        .into_iter()
        .collect(),
    )
}

pub(crate) fn response_schema_opaque_object() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "additionalProperties": true
    })
}

/// Schema for the `.14.1` storage-integrity block projected by every truth
/// surface that speaks the canonical storage vocabulary: `cass doctor --check
/// --json` (bead vl1cj), `cass status --json`, and `cass search --robot-meta`
/// (bead qfswx). Describes the `StorageState` / `SourceOfTruthRisk` /
/// `ArchiveReadability` vocabulary plus the read-only checks attempted. Built
/// in its own `json!` so the giant `doctor_schema` literal does not deepen past
/// the macro recursion limit.
pub(crate) fn response_schema_storage_integrity() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Storage-integrity taxonomy (.14.1): canonical archive state derived from the read-only db-open / integrity / fts / index signals.",
        "properties": {
            "storage_state": { "type": "string" },
            "source_of_truth_risk": { "type": "string" },
            "archive_readability": { "type": "string" },
            "checks_attempted": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                        "elapsed_ms": { "type": "integer" },
                        "timed_out": { "type": "boolean" },
                        "skipped_reason": { "type": "string" },
                        "read_only": { "type": "boolean" }
                    },
                    "required": ["name", "elapsed_ms", "timed_out", "read_only"]
                }
            },
            "checks_not_attempted": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                        "reason": { "type": "string" }
                    },
                    "required": ["name", "reason"]
                }
            },
            "attestation_source": { "type": "string" },
            "attested_at_ms": { "type": "integer" },
            "attestation_check_depth": { "type": "string" },
            "attested_db_fingerprint": { "type": "string" }
        },
        "required": ["storage_state", "source_of_truth_risk", "archive_readability"]
    })
}

pub(crate) fn response_schema_opaque_object_array() -> serde_json::Value {
    serde_json::json!({
        "type": "array",
        "items": {
            "type": "object",
            "additionalProperties": true
        }
    })
}

pub(crate) const DOCTOR_V2_COMMON_BRANCH_FIELDS: &[&str] = &[
    "schema_version",
    "surface",
    "mode",
    "status",
    "outcome_kind",
    "risk_level",
    "asset_class",
    "fallback_mode",
    "authority_status",
    "coverage_delta",
    "blocked_reasons",
    "plan_fingerprint",
    "receipt_path",
    "event_log_path",
    "artifact_manifest_path",
    "recommended_action",
    "redaction_status",
    "contract_provenance",
];

pub(crate) const DOCTOR_V2_RESPONSE_SCHEMA_KEYS: &[&str] = &[
    "doctor-archive-normalize",
    "doctor-archive-scan",
    "doctor-backups-list",
    "doctor-backups-verify",
    "doctor-baseline-diff",
    "doctor-baseline-save",
    "doctor-baseline-update",
    "doctor-check",
    "doctor-cleanup-apply",
    "doctor-cleanup-dry-run",
    "doctor-error-envelope",
    "doctor-failure-context",
    "doctor-health-summary",
    "doctor-reconstruct-dry-run",
    "doctor-repair-dry-run",
    "doctor-repair-receipt",
    "doctor-restore-rehearsal",
    "doctor-safe-auto-run",
    "doctor-semantic-model-fallback",
    "doctor-status-summary",
    "doctor-support-bundle",
];

pub(crate) fn response_schema_string_enum(values: &[&str]) -> serde_json::Value {
    serde_json::json!({
        "type": "string",
        "enum": values
    })
}

pub(crate) fn response_schema_doctor_v2_coverage_delta() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Counts-only coverage delta. It never contains raw session text.",
        "properties": {
            "status": response_schema_string_enum(&["unchanged", "improved", "reduced", "unknown"]),
            "archive_conversation_count": { "type": ["integer", "null"] },
            "visible_source_conversation_count": { "type": ["integer", "null"] },
            "raw_mirror_manifest_count": { "type": ["integer", "null"] },
            "db_projection_only_count": { "type": ["integer", "null"] },
            "missing_current_source_count": { "type": ["integer", "null"] },
            "conversation_delta": { "type": ["integer", "null"] },
            "message_delta": { "type": ["integer", "null"] },
            "semantic_vector_delta": { "type": ["integer", "null"] },
            "derived_asset_delta": { "type": ["integer", "null"] }
        },
        "required": [
            "status",
            "archive_conversation_count",
            "visible_source_conversation_count",
            "raw_mirror_manifest_count",
            "missing_current_source_count",
            "conversation_delta",
            "message_delta"
        ]
    })
}

pub(crate) fn response_schema_doctor_v2_common_properties()
-> serde_json::Map<String, serde_json::Value> {
    use serde_json::json;

    [
        (
            "schema_version",
            json!({
                "type": "integer",
                "description": "Doctor v2 response schema version. Version 2 is the first schema-first contract for typed agents."
            }),
        ),
        (
            "surface",
            response_schema_string_enum(&[
                "legacy-doctor",
                "check",
                "repair",
                "cleanup",
                "reconstruct",
                "restore",
                "baseline",
                "baseline-diff",
                "support-bundle",
                "health-summary",
                "status-summary",
                "backups",
                "archive-scan",
                "archive-normalize",
                "semantic-model-fallback",
                "failure-context",
                "error-envelope",
            ]),
        ),
        (
            "mode",
            response_schema_string_enum(&[
                "read-only-check",
                "repair-dry-run",
                "fingerprint-apply",
                "safe-auto-fix",
                "cleanup-dry-run",
                "cleanup-apply",
                "reconstruct-dry-run",
                "restore-rehearsal",
                "support-bundle",
                "baseline-save",
                "baseline-update",
                "baseline-diff",
                "not-applicable",
            ]),
        ),
        (
            "status",
            response_schema_string_enum(&[
                "ok",
                "warn",
                "planned",
                "applied",
                "partial",
                "blocked",
                "failed",
                "skipped",
                "saved",
                "updated",
                "changed",
                "unchanged",
            ]),
        ),
        (
            "outcome_kind",
            response_schema_string_enum(&[
                "no_op", "planned", "applied", "partial", "blocked", "failed",
            ]),
        ),
        (
            "risk_level",
            response_schema_string_enum(&["none", "low", "medium", "high", "unknown"]),
        ),
        (
            "asset_class",
            json!({
                "type": "string",
                "description": "Stable snake_case DoctorAssetClass such as canonical_archive_db, raw_mirror_blob, derived_lexical_index, support_bundle, or external_upstream_source."
            }),
        ),
        (
            "fallback_mode",
            response_schema_string_enum(&[
                "none",
                "lexical",
                "semantic",
                "hybrid",
                "read-only",
                "unavailable",
                "not-applicable",
            ]),
        ),
        (
            "authority_status",
            response_schema_string_enum(&[
                "read_only",
                "selected",
                "candidate_only",
                "promotable",
                "refused",
                "unavailable",
                "not_applicable",
            ]),
        ),
        ("coverage_delta", response_schema_doctor_v2_coverage_delta()),
        (
            "blocked_reasons",
            json!({
                "type": "array",
                "items": { "type": "string" },
                "description": "Branchable machine codes; empty when the operation can proceed."
            }),
        ),
        (
            "plan_fingerprint",
            json!({
                "type": ["string", "null"],
                "description": "Fingerprint that must be echoed back for mutating apply; null for read-only checks and summaries."
            }),
        ),
        (
            "receipt_path",
            json!({
                "type": ["string", "null"],
                "description": "Path to the durable receipt when one is produced. Use redacted paths for export/support bundles."
            }),
        ),
        (
            "event_log_path",
            json!({
                "type": ["string", "null"],
                "description": "Path to the operation event log. Progress/log details stay out of stdout and in this artifact."
            }),
        ),
        (
            "artifact_manifest_path",
            json!({
                "type": ["string", "null"],
                "description": "Path to the manifest covering receipts, bundles, backups, or validation artifacts."
            }),
        ),
        (
            "recommended_action",
            json!({
                "type": "string",
                "description": "Single next command or operator action. Robots should not parse prose fields to infer this."
            }),
        ),
        (
            "redaction_status",
            response_schema_string_enum(&[
                "redacted",
                "not_applicable",
                "contains_sensitive_paths",
                "raw_content_included",
            ]),
        ),
        (
            "contract_provenance",
            response_schema_string_enum(&["runtime", "contract-example", "golden-fixture"]),
        ),
        ("operation_outcome", response_schema_doctor_operation_outcome()),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_string(), value))
    .collect()
}

#[derive(Clone, Copy)]
pub(crate) struct DoctorV2SurfaceSchemaSpec {
    pub(crate) description: &'static str,
    pub(crate) surface: &'static str,
    pub(crate) mode: &'static str,
    pub(crate) status: &'static str,
    pub(crate) outcome_kind: &'static str,
    pub(crate) asset_class: &'static str,
    pub(crate) risk_level: &'static str,
    pub(crate) fallback_mode: &'static str,
    pub(crate) authority_status: &'static str,
    pub(crate) recommended_action: &'static str,
}

pub(crate) fn response_schema_doctor_v2_example(
    spec: DoctorV2SurfaceSchemaSpec,
) -> serde_json::Value {
    let (operation_kind, exit_code_kind, safe_to_retry, action_taken, action_not_taken) =
        match spec.outcome_kind {
            "applied" => (
                "fixed",
                "success",
                true,
                "example receipt records applied work",
                "no planned safe action was skipped",
            ),
            "partial" => (
                "partially-fixed",
                "repair-failure",
                true,
                "example receipt records partial work",
                "some work remains blocked",
            ),
            "blocked" => (
                "repair-blocked",
                "lock-busy",
                false,
                "example records a fail-closed refusal to mutate",
                "repair waits until blockers clear",
            ),
            "failed" => (
                "repair-incomplete",
                "repair-failure",
                false,
                "example preserves failure context artifacts",
                "doctor does not claim success without verification",
            ),
            "planned" => (
                "ok-read-only-diagnosed",
                "success",
                true,
                "example dry-run produced a plan only",
                "no filesystem mutation",
            ),
            _ => (
                "ok-no-action-needed",
                "success",
                true,
                "example check completed",
                "no repair or cleanup was needed",
            ),
        };

    serde_json::json!({
        "schema_version": 2,
        "surface": spec.surface,
        "mode": spec.mode,
        "status": spec.status,
        "outcome_kind": spec.outcome_kind,
        "risk_level": spec.risk_level,
        "asset_class": spec.asset_class,
        "fallback_mode": spec.fallback_mode,
        "authority_status": spec.authority_status,
        "coverage_delta": {
            "status": "unchanged",
            "archive_conversation_count": 42,
            "visible_source_conversation_count": 41,
            "raw_mirror_manifest_count": 42,
            "db_projection_only_count": 0,
            "missing_current_source_count": 1,
            "conversation_delta": 0,
            "message_delta": 0,
            "semantic_vector_delta": null,
            "derived_asset_delta": 0
        },
        "blocked_reasons": [],
        "plan_fingerprint": null,
        "receipt_path": null,
        "event_log_path": "[DATA_DIR]/doctor/events/example.jsonl",
        "artifact_manifest_path": "[DATA_DIR]/doctor/artifacts/example-manifest.json",
        "recommended_action": spec.recommended_action,
        "redaction_status": "redacted",
        "contract_provenance": "contract-example",
        "operation_outcome": {
            "kind": operation_kind,
            "reason": "example contract payload",
            "action_taken": action_taken,
            "action_not_taken": action_not_taken,
            "safe_to_retry": safe_to_retry,
            "requires_override": false,
            "data_loss_risk": spec.risk_level,
            "next_command": spec.recommended_action,
            "artifact_manifest_path": "[DATA_DIR]/doctor/artifacts/example-manifest.json",
            "exit_code_kind": exit_code_kind
        }
    })
}

pub(crate) fn response_schema_doctor_v2_surface(
    spec: DoctorV2SurfaceSchemaSpec,
    extra_properties: impl IntoIterator<Item = (&'static str, serde_json::Value)>,
) -> serde_json::Value {
    let mut properties = response_schema_doctor_v2_common_properties();
    for (key, value) in extra_properties {
        properties.insert(key.to_string(), value);
    }

    serde_json::json!({
        "type": "object",
        "description": spec.description,
        "properties": properties,
        "required": DOCTOR_V2_COMMON_BRANCH_FIELDS,
        "examples": [
            response_schema_doctor_v2_example(spec)
        ]
    })
}

pub(crate) fn response_schema_doctor_v2_summary(surface: &'static str) -> serde_json::Value {
    response_schema_doctor_v2_surface(
        DoctorV2SurfaceSchemaSpec {
            description: "Doctor summary block embedded by health/status so agents can decide whether doctor inspection is needed without running repair.",
            surface,
            mode: "read-only-check",
            status: "ok",
            outcome_kind: "no_op",
            asset_class: "canonical_archive_db",
            risk_level: "low",
            fallback_mode: "lexical",
            authority_status: "read_only",
            recommended_action: "cass doctor check --json",
        },
        [
            ("health_class", serde_json::json!({ "type": "string" })),
            ("doctor_available", serde_json::json!({ "type": "boolean" })),
            (
                "safe_auto_run_eligible",
                serde_json::json!({ "type": "boolean" }),
            ),
            (
                "last_receipt_path",
                serde_json::json!({ "type": ["string", "null"] }),
            ),
            (
                "failure_marker_path",
                serde_json::json!({ "type": ["string", "null"] }),
            ),
            (
                "repair_previously_failed",
                serde_json::json!({ "type": "boolean" }),
            ),
            (
                "active_repair",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "active": { "type": "boolean" },
                        "active_index_maintenance": { "type": "boolean" },
                        "repair_blocked_reason": { "type": ["string", "null"] }
                    }
                }),
            ),
            (
                "repair_recommended",
                serde_json::json!({ "type": "boolean" }),
            ),
            (
                "repair_blocked_reason",
                serde_json::json!({ "type": ["string", "null"] }),
            ),
            (
                "doctor_check_recommended",
                serde_json::json!({ "type": "boolean" }),
            ),
            (
                "archive_coverage_state",
                serde_json::json!({
                    "type": "string",
                    "description": "Compact coverage state: ok, not_checked, not_initialized, sole_copy_risk, raw_mirror_backfill_available, raw_mirror_capture_disabled (CASS_RAW_MIRROR is off), raw_mirror_unlinked, current_sources_newer_than_archive, or unknown."
                }),
            ),
            (
                "source_mirror_state",
                serde_json::json!({
                    "type": "string",
                    "description": "Compact raw-mirror/source state derived from bounded evidence."
                }),
            ),
            (
                "sole_copy_conversation_count",
                serde_json::json!({ "type": "integer" }),
            ),
            (
                "cleanup_reclaimable_bytes",
                serde_json::json!({ "type": ["integer", "null"] }),
            ),
            (
                "quarantine_summary",
                serde_json::json!({
                    "type": ["object", "null"],
                    "additionalProperties": true
                }),
            ),
            (
                "coverage_source",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "status": { "type": "string" },
                        "source": { "type": "string" },
                        "confidence_tier": { "type": "string" },
                        "generated_at": { "type": ["string", "null"] },
                        "stale_after_seconds": { "type": "integer" },
                        "source_report_id": { "type": ["string", "null"] },
                        "recommended_action": { "type": "string" }
                    }
                }),
            ),
            ("health_status", serde_json::json!({ "type": "string" })),
            ("db_exists", serde_json::json!({ "type": "boolean" })),
        ],
    )
}

pub(crate) fn response_schema_doctor_v2_contract_summary() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Schema-first doctor v2 contract inventory. Runtime commands may be implemented incrementally, but these response schema keys and branch fields are stable.",
        "properties": {
            "schema_version": { "type": "integer" },
            "response_schema_keys": {
                "type": "array",
                "items": { "type": "string" },
                "examples": [DOCTOR_V2_RESPONSE_SCHEMA_KEYS]
            },
            "common_branch_fields": {
                "type": "array",
                "items": { "type": "string" },
                "examples": [DOCTOR_V2_COMMON_BRANCH_FIELDS]
            },
            "branch_order": {
                "type": "array",
                "items": { "type": "string" },
                "examples": [[
                    "err.kind",
                    "status",
                    "operation_outcome.kind",
                    "outcome_kind",
                    "asset_class",
                    "risk_level",
                    "fallback_mode"
                ]]
            },
            "stdout_contract": { "type": "string" },
            "stderr_contract": { "type": "string" },
            "mutation_contract": { "type": "string" },
            "example_provenance": { "type": "string" }
        },
        "required": [
            "schema_version",
            "response_schema_keys",
            "common_branch_fields",
            "branch_order",
            "stdout_contract",
            "stderr_contract",
            "mutation_contract",
            "example_provenance"
        ],
        "examples": [{
            "schema_version": 2,
            "response_schema_keys": DOCTOR_V2_RESPONSE_SCHEMA_KEYS,
            "common_branch_fields": DOCTOR_V2_COMMON_BRANCH_FIELDS,
            "branch_order": [
                "err.kind",
                "status",
                "operation_outcome.kind",
                "outcome_kind",
                "asset_class",
                "risk_level",
                "fallback_mode"
            ],
            "stdout_contract": "robot stdout is parseable JSON only",
            "stderr_contract": "diagnostics, progress, and detailed logs use stderr or artifact paths",
            "mutation_contract": "mutating commands require a dry-run fingerprint or safe auto eligibility and never delete source evidence",
            "example_provenance": "contract-example"
        }]
    })
}

pub(crate) fn response_schema_doctor_v2_error_envelope() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Doctor v2 error envelope. Robots branch on err.kind before numeric exit codes.",
        "properties": {
            "err": {
                "type": "object",
                "properties": {
                    "kind": {
                        "type": "string",
                        "description": "Stable kebab-case error kind, for example usage, lock-busy, repair-blocked, verification-failed, config, io, semantic-unavailable, model."
                    },
                    "code": { "type": "integer" },
                    "message": { "type": "string" },
                    "hint": { "type": ["string", "null"] },
                    "retryable": { "type": "boolean" },
                    "surface": { "type": "string" },
                    "mode": { "type": "string" },
                    "risk_level": { "type": "string" },
                    "asset_class": { "type": "string" },
                    "blocked_reasons": { "type": "array", "items": { "type": "string" } },
                    "failure_context_path": { "type": ["string", "null"] },
                    "event_log_path": { "type": ["string", "null"] },
                    "artifact_manifest_path": { "type": ["string", "null"] },
                    "recommended_action": { "type": "string" },
                    "redaction_status": { "type": "string" }
                },
                "required": [
                    "kind",
                    "code",
                    "message",
                    "retryable",
                    "surface",
                    "mode",
                    "risk_level",
                    "asset_class",
                    "blocked_reasons",
                    "recommended_action",
                    "redaction_status"
                ]
            }
        },
        "required": ["err"],
        "examples": [{
            "err": {
                "kind": "repair-blocked",
                "code": 7,
                "message": "doctor repair blocked by active index maintenance",
                "hint": "Run cass doctor check --json after the active operation finishes.",
                "retryable": true,
                "surface": "repair",
                "mode": "fingerprint-apply",
                "risk_level": "medium",
                "asset_class": "canonical_archive_db",
                "blocked_reasons": ["active_index_maintenance"],
                "failure_context_path": "[DATA_DIR]/doctor/failures/example.json",
                "event_log_path": "[DATA_DIR]/doctor/events/example.jsonl",
                "artifact_manifest_path": "[DATA_DIR]/doctor/artifacts/example-manifest.json",
                "recommended_action": "cass doctor check --json",
                "redaction_status": "redacted"
            }
        }]
    })
}

pub(crate) fn response_schema_doctor_failure_context_artifacts() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Redacted paths to nearby debugging artifacts. These are manifest and log references, not raw session logs.",
        "additionalProperties": true,
        "properties": {
            "artifact_manifest_path": { "type": ["string", "null"] },
            "commands_path": { "type": ["string", "null"] },
            "doctor_events_path": { "type": ["string", "null"] },
            "execution_flow_path": { "type": ["string", "null"] },
            "receipts_path": { "type": ["string", "null"] },
            "checksums_path": { "type": ["string", "null"] },
            "stdout_path": { "type": ["string", "null"] },
            "stderr_path": { "type": ["string", "null"] },
            "failure_context_path": { "type": ["string", "null"] },
            "failure_summary_path": { "type": ["string", "null"] },
            "parsed_json_path": { "type": ["string", "null"] },
            "event_log_path": { "type": ["string", "null"] }
        }
    })
}

pub(crate) fn response_schema_doctor_failure_context_repro() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Safe repro template. It must default to copied fixtures or redacted placeholders, not mutation of a live archive.",
        "properties": {
            "safety": { "type": "string" },
            "mutates_live_archive": { "type": "boolean" },
            "requires_explicit_live_archive": { "type": "boolean" },
            "target": { "type": "string" },
            "working_directory": { "type": ["string", "null"] },
            "command_json": {
                "type": "array",
                "items": { "type": "string" }
            },
            "shell_command": { "type": "string" },
            "notes": {
                "type": "array",
                "items": { "type": "string" }
            }
        },
        "required": [
            "safety",
            "mutates_live_archive",
            "requires_explicit_live_archive",
            "target",
            "command_json",
            "shell_command"
        ]
    })
}

pub(crate) fn response_schema_doctor_failure_context() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Failure context artifact contract for blocked, refused, incomplete, or verification-failed doctor operations. This artifact is redacted by default and is safe to attach to support reports alongside manifest/commands/events artifacts.",
        "properties": {
            "schema_version": { "type": "integer" },
            "context_kind": { "type": ["string", "null"] },
            "scenario_id": { "type": ["string", "null"] },
            "probe_run_id": { "type": ["string", "null"] },
            "operation_id": { "type": ["string", "null"] },
            "failed_phase": { "type": "string" },
            "failed_check": { "type": "string" },
            "reasons": {
                "type": "array",
                "items": { "type": "string" }
            },
            "failure_reason": { "type": ["string", "null"] },
            "command": response_schema_opaque_object(),
            "command_history": response_schema_opaque_object_array(),
            "platform": response_schema_opaque_object(),
            "fixture": response_schema_opaque_object(),
            "artifacts": response_schema_doctor_failure_context_artifacts(),
            "repro": response_schema_doctor_failure_context_repro(),
            "recent_events": response_schema_opaque_object_array(),
            "captured_logs": response_schema_opaque_object_array(),
            "probe": response_schema_opaque_object(),
            "asset_class": { "type": ["string", "null"] },
            "authority_status": { "type": ["string", "null"] },
            "contract_provenance": { "type": ["string", "null"] },
            "blocked_reasons": {
                "type": "array",
                "items": { "type": "string" }
            },
            "command_id": { "type": ["string", "null"] },
            "exit_code": { "type": ["integer", "null"] },
            "plan_fingerprint": { "type": ["string", "null"] },
            "selected_authority": { "type": ["object", "array", "null"], "additionalProperties": true },
            "rejected_authorities": { "type": ["object", "array", "null"], "additionalProperties": true },
            "active_locks": { "type": ["object", "array", "null"], "additionalProperties": true },
            "coverage_summary": { "type": ["object", "array", "null"], "additionalProperties": true },
            "stdout_tail": { "type": ["string", "null"] },
            "stderr_tail": { "type": ["string", "null"] },
            "redaction_status": { "type": ["string", "null"] }
        },
        "required": [
            "schema_version",
            "failed_phase",
            "failed_check",
            "artifacts",
            "repro"
        ],
        "examples": [{
            "schema_version": 1,
            "context_kind": "cass_doctor_post_repair_probe_failure_context",
            "operation_id": "doctor-post-repair-probe-example",
            "failed_phase": "post_repair_probe",
            "failed_check": "rollback-confirmed",
            "failure_reason": "post-repair verification failed",
            "asset_class": "derived_lexical_index",
            "authority_status": "post_repair_verification_failed",
            "contract_provenance": "contract-example",
            "blocked_reasons": ["blocks_promotion", "blocks_cleanup"],
            "artifacts": {
                "artifact_manifest_path": "[cass-data]/doctor/post-repair-probes/example/manifest.json",
                "failure_context_path": "[cass-data]/doctor/post-repair-probes/example/failure_context.json"
            },
            "captured_logs": [{
                "kind": "failure_reason",
                "entries": ["post-repair verification failed"]
            }],
            "repro": {
                "safety": "read-only-redacted-template",
                "mutates_live_archive": false,
                "requires_explicit_live_archive": true,
                "target": "[cass-data]",
                "command_json": ["cass", "doctor", "check", "--json", "--data-dir", "[cass-data]"],
                "shell_command": "cass doctor check --json --data-dir '[cass-data]'",
                "notes": ["Replace [cass-data] with a copied fixture or explicit test data directory."]
            },
            "recent_events": [],
            "redaction_status": "redacted"
        }]
    })
}

pub(crate) fn response_schema_doctor_coverage_snapshot() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "generation_count": { "type": "integer" },
            "reclaim_candidate_count": { "type": "integer" },
            "reclaimable_bytes": { "type": "integer" },
            "retained_bytes": { "type": "integer" },
            "artifact_count": { "type": "integer" },
            "covered_asset_classes": { "type": "array", "items": { "type": "string" } }
        }
    })
}

pub(crate) fn response_schema_doctor_artifact() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "artifact_id": { "type": "string" },
            "artifact_kind": { "type": "string" },
            "asset_class": { "type": "string" },
            "path": { "type": "string" },
            "redacted_path": { "type": "string" },
            "exists": { "type": "boolean" },
            "size_bytes": { "type": ["integer", "null"] },
            "descriptor_blake3": { "type": "string" },
            "expected_content_blake3": { "type": ["string", "null"] },
            "actual_content_blake3": { "type": ["string", "null"] },
            "checksum_status": {
                "type": "string",
                "description": "not_recorded | matched | mismatched | missing"
            }
        },
        "required": ["artifact_id", "artifact_kind", "asset_class", "path", "redacted_path", "exists", "descriptor_blake3", "checksum_status"]
    })
}

pub(crate) fn response_schema_doctor_artifact_manifest() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "schema_version": { "type": "integer" },
            "artifact_count": { "type": "integer" },
            "manifest_blake3": { "type": "string" },
            "drift_detection_status": {
                "type": "string",
                "description": "not_checked | verified | checksum_mismatch | missing_artifact | manifest_mismatch"
            },
            "artifacts": {
                "type": "array",
                "items": response_schema_doctor_artifact()
            }
        }
    })
}

pub(crate) fn response_schema_doctor_safety_gate() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "mode": { "type": "string" },
            "asset_class": { "type": "string" },
            "allowed_by_mode": { "type": "boolean" },
            "allowed_by_taxonomy": { "type": "boolean" },
            "path_safe": { "type": "boolean" },
            "approval_requirement": { "type": "string" },
            "approval_fingerprint": { "type": "string" },
            "passed": { "type": "boolean" },
            "blocked_reasons": { "type": "array", "items": { "type": "string" } }
        }
    })
}

pub(crate) fn response_schema_doctor_action() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "action_id": { "type": "string" },
            "action_kind": { "type": "string" },
            "status": {
                "type": "string",
                "description": "planned | applied | skipped | blocked | failed | refused"
            },
            "mode": { "type": "string" },
            "asset_class": { "type": "string" },
            "target_path": { "type": "string" },
            "redacted_target_path": { "type": "string" },
            "reason": { "type": "string" },
            "authority_decision": { "type": "string" },
            "selected_authorities": { "type": "array", "items": { "type": "string" } },
            "rejected_authorities": { "type": "array", "items": { "type": "string" } },
            "safety_gate": response_schema_doctor_safety_gate(),
            "planned_bytes": { "type": "integer" },
            "bytes_copied": { "type": "integer" },
            "bytes_moved": { "type": "integer" },
            "bytes_pruned": { "type": "integer" },
            "backup_paths": { "type": "array", "items": { "type": "string" } },
            "verification_outcome": { "type": "string" },
            "remaining_risk": { "type": "array", "items": { "type": "string" } },
            "artifacts": { "type": "array", "items": response_schema_doctor_artifact() }
        }
    })
}

pub(crate) fn response_schema_doctor_event_log_metadata() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "path": { "type": ["string", "null"] },
            "checksum_blake3": { "type": ["string", "null"] },
            "hash_chain_tip": { "type": ["string", "null"] },
            "status": { "type": "string" },
            "event_count": { "type": "integer" },
            "events": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "schema_version": { "type": "integer" },
                        "event_id": { "type": "string" },
                        "previous_event_hash": { "type": ["string", "null"] },
                        "operation_id": { "type": "string" },
                        "action_id": { "type": ["string", "null"] },
                        "phase": { "type": "string" },
                        "mode": { "type": "string" },
                        "asset_class": { "type": ["string", "null"] },
                        "redacted_target_path": { "type": ["string", "null"] },
                        "elapsed_ms": { "type": ["integer", "null"] },
                        "progress_label": { "type": "string" },
                        "safety_gate_passed": { "type": ["boolean", "null"] },
                        "blocked_reasons": { "type": "array", "items": { "type": "string" } },
                        "receipt_correlation_id": { "type": ["string", "null"] },
                        "artifact_ids": { "type": "array", "items": { "type": "string" } }
                    }
                }
            }
        }
    })
}

pub(crate) fn response_schema_doctor_forensic_bundle_metadata() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "bundle_id": { "type": ["string", "null"] },
            "path": { "type": ["string", "null"] },
            "redacted_path": { "type": ["string", "null"] },
            "manifest_path": { "type": ["string", "null"] },
            "redacted_manifest_path": { "type": ["string", "null"] },
            "checksum_blake3": { "type": ["string", "null"] },
            "status": { "type": "string" },
            "created_at_ms": { "type": ["integer", "null"] },
            "artifact_count": { "type": "integer" },
            "copied_artifact_count": { "type": "integer" },
            "missing_artifact_count": { "type": "integer" },
            "failed_artifact_count": { "type": "integer" },
            "sidecar_complete": { "type": "boolean" },
            "redaction_status": { "type": "string" },
            "blocked_reason": { "type": ["string", "null"] },
            "artifacts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "artifact_kind": { "type": "string" },
                        "source_path": { "type": "string" },
                        "redacted_source_path": { "type": "string" },
                        "bundle_relative_path": { "type": ["string", "null"] },
                        "exists": { "type": "boolean" },
                        "copied": { "type": "boolean" },
                        "required": { "type": "boolean" },
                        "sidecar_group": { "type": ["string", "null"] },
                        "size_bytes": { "type": ["integer", "null"] },
                        "checksum_blake3": { "type": ["string", "null"] },
                        "error": { "type": ["string", "null"] }
                    }
                }
            }
        }
    })
}

pub(crate) fn response_schema_doctor_plan() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "plan_kind": { "type": "string" },
            "schema_version": { "type": "integer" },
            "mode": { "type": "string" },
            "approval_requirement": { "type": "string" },
            "approval_fingerprint": { "type": "string" },
            "plan_fingerprint": { "type": "string" },
            "fingerprint_algorithm": { "type": "string" },
            "outcome_contract": { "type": "string" },
            "coverage_before": response_schema_doctor_coverage_snapshot(),
            "safety_gates": { "type": "array", "items": response_schema_doctor_safety_gate() },
            "actions": { "type": "array", "items": response_schema_doctor_action() },
            "artifact_manifest": response_schema_doctor_artifact_manifest(),
            "event_log": response_schema_doctor_event_log_metadata(),
            "forensic_bundle": response_schema_doctor_forensic_bundle_metadata(),
            "selected_authorities": { "type": "array", "items": { "type": "string" } },
            "rejected_authorities": { "type": "array", "items": { "type": "string" } },
            "blocked_reasons": { "type": "array", "items": { "type": "string" } },
            "remaining_risk": { "type": "array", "items": { "type": "string" } }
        }
    })
}

pub(crate) fn response_schema_doctor_repair_plan_preview() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "cass doctor repair dry-run/apply approval plan. plan_fingerprint is a canonical hash of fingerprint_inputs and must match before apply mutates anything.",
        "properties": {
            "schema_version": { "type": "integer" },
            "plan_kind": { "type": "string" },
            "mode": { "type": "string" },
            "dry_run": { "type": "boolean" },
            "apply_requested": { "type": "boolean" },
            "approval_required": { "type": "boolean" },
            "approval_status": { "type": "string", "description": "dry_run_only | missing_plan_fingerprint | matched | mismatched | blocked | lock_uncertain" },
            "provided_plan_fingerprint": { "type": ["string", "null"] },
            "plan_fingerprint": { "type": "string" },
            "fingerprint_algorithm": { "type": "string" },
            "fingerprint_inputs": response_schema_opaque_object(),
            "exact_apply_command": { "type": "string" },
            "apply_argv": { "type": "array", "items": { "type": "string" } },
            "will_mutate": { "type": "boolean" },
            "apply_authorized": { "type": "boolean" },
            "planned_action_count": { "type": "integer" },
            "planned_actions": { "type": "array", "items": response_schema_opaque_object() },
            "selected_authorities": { "type": "array", "items": { "type": "string" } },
            "rejected_authorities": { "type": "array", "items": { "type": "string" } },
            "blocked_reasons": { "type": "array", "items": { "type": "string" } },
            "branchable_blocker_codes": { "type": "array", "items": { "type": "string" } },
            "warnings": { "type": "array", "items": { "type": "string" } },
            "freshness_policy": { "type": "array", "items": { "type": "string" } },
            "forensic_bundle_required": { "type": "boolean" },
            "post_repair_probes_required": { "type": "boolean" },
            "never_prunes_source_evidence": { "type": "boolean" }
        }
    })
}

pub(crate) fn response_schema_doctor_receipt() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "receipt_kind": { "type": "string" },
            "schema_version": { "type": "integer" },
            "mode": { "type": "string" },
            "outcome_kind": { "type": "string" },
            "approval_fingerprint": { "type": "string" },
            "plan_fingerprint": { "type": "string" },
            "started_at_ms": { "type": ["integer", "null"] },
            "finished_at_ms": { "type": ["integer", "null"] },
            "duration_ms": { "type": ["integer", "null"] },
            "planned_action_count": { "type": "integer" },
            "applied_action_count": { "type": "integer" },
            "skipped_action_count": { "type": "integer" },
            "failed_action_count": { "type": "integer" },
            "bytes_planned": { "type": "integer" },
            "bytes_copied": { "type": "integer" },
            "bytes_moved": { "type": "integer" },
            "bytes_pruned": { "type": "integer" },
            "reclaimed_bytes": { "type": "integer" },
            "backup_paths": { "type": "array", "items": { "type": "string" } },
            "selected_authorities": { "type": "array", "items": { "type": "string" } },
            "rejected_authorities": { "type": "array", "items": { "type": "string" } },
            "verification_outcomes": { "type": "array", "items": { "type": "string" } },
            "remaining_risk": { "type": "array", "items": { "type": "string" } },
            "event_log": response_schema_doctor_event_log_metadata(),
            "forensic_bundle": response_schema_doctor_forensic_bundle_metadata(),
            "artifact_manifest": response_schema_doctor_artifact_manifest(),
            "artifact_checksums": { "type": "array", "items": response_schema_doctor_artifact() },
            "drift_detection_status": { "type": "string" },
            "coverage_before": response_schema_doctor_coverage_snapshot(),
            "coverage_after": response_schema_doctor_coverage_snapshot(),
            "actions": { "type": "array", "items": response_schema_doctor_action() },
            "action_status_counts": { "type": "object", "additionalProperties": { "type": "integer" } },
            "blocked_reasons": { "type": "array", "items": { "type": "string" } }
        }
    })
}

pub(crate) fn response_schema_doctor_operation_outcome() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Stable doctor operation outcome. Robots should branch on kind before consulting prose fields.",
        "properties": {
            "kind": { "type": "string", "description": "Stable kebab-case outcome kind such as ok-no-action-needed, ok-read-only-diagnosed, fixed, partially-fixed, repair-blocked, repair-refused, repair-incomplete, verification-failed, cleanup-dry-run-only, cleanup-refused, auto-run-skipped, support-bundle-only, baseline-diff-only, or requires-manual-review." },
            "reason": { "type": "string" },
            "action_taken": { "type": "string" },
            "action_not_taken": { "type": "string" },
            "safe_to_retry": { "type": "boolean" },
            "requires_override": { "type": "boolean" },
            "data_loss_risk": { "type": "string", "description": "none | low | medium | high | unknown" },
            "next_command": { "type": ["string", "null"] },
            "artifact_manifest_path": { "type": ["string", "null"] },
            "exit_code_kind": { "type": "string", "description": "success | health-failure | usage-error | lock-busy | repair-failure" }
        },
        "required": ["kind", "reason", "action_taken", "action_not_taken", "safe_to_retry", "requires_override", "data_loss_risk", "next_command", "artifact_manifest_path", "exit_code_kind"]
    })
}
