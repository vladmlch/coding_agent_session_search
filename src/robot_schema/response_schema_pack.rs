use super::response_schema_doctor_repair_failure_marker::{
    response_schema_budget_block, response_schema_search_effective,
};
use super::response_schema_index_state::response_schema_object;

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
