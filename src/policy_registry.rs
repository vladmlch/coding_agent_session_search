//! Data-only registry for runtime controller policies.
//!
//! Controllers keep making their existing decisions in their home modules. This
//! registry only snapshots the resolved policy identity, deterministic inputs,
//! and fallback state so status/health surfaces can explain which control
//! policy is active without re-running hidden logic.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

use crate::search::policy::{SemanticMode, SemanticPolicy};

pub const POLICY_REGISTRY_SCHEMA_VERSION: &str = "1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyRegistrySnapshot {
    pub schema_version: String,
    pub controllers: Vec<PolicyControllerSnapshot>,
}

impl PolicyRegistrySnapshot {
    pub fn new(mut controllers: Vec<PolicyControllerSnapshot>) -> Self {
        controllers.sort_by(|left, right| left.controller_id.cmp(&right.controller_id));
        Self {
            schema_version: POLICY_REGISTRY_SCHEMA_VERSION.to_string(),
            controllers,
        }
    }

    pub fn controller_ids(&self) -> BTreeSet<&str> {
        self.controllers
            .iter()
            .map(|controller| controller.controller_id.as_str())
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyControllerSnapshot {
    pub controller_id: String,
    pub policy_id: String,
    pub policy_version: String,
    pub status: PolicyControllerStatus,
    pub fallback_state: PolicyFallbackState,
    pub conservative_fallback: bool,
    pub decision_reason: String,
    #[serde(default)]
    pub inputs: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyControllerStatus {
    Active,
    Disabled,
    Fallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyFallbackState {
    NotNeeded,
    Conservative,
    Disabled,
}

pub fn policy_registry_snapshot(
    semantic_policy: &SemanticPolicy,
    semantic_available: bool,
    semantic_fallback_mode: Option<&str>,
    lexical_rebuild_pipeline: &Value,
) -> PolicyRegistrySnapshot {
    PolicyRegistrySnapshot::new(vec![
        semantic_policy_controller_snapshot(
            semantic_policy,
            semantic_available,
            semantic_fallback_mode,
        ),
        lexical_rebuild_policy_controller_snapshot(lexical_rebuild_pipeline),
    ])
}

pub fn semantic_policy_controller_snapshot(
    policy: &SemanticPolicy,
    semantic_available: bool,
    semantic_fallback_mode: Option<&str>,
) -> PolicyControllerSnapshot {
    let mut inputs = BTreeMap::new();
    inputs.insert("mode".to_string(), policy.mode.as_str().to_string());
    inputs.insert(
        "fast_tier_embedder".to_string(),
        policy.fast_tier_embedder.clone(),
    );
    inputs.insert(
        "quality_tier_embedder".to_string(),
        policy.quality_tier_embedder.clone(),
    );
    inputs.insert("reranker".to_string(), policy.reranker.clone());
    inputs.insert(
        "fast_dimension".to_string(),
        policy.fast_dimension.to_string(),
    );
    inputs.insert(
        "quality_dimension".to_string(),
        policy.quality_dimension.to_string(),
    );
    inputs.insert(
        "quality_weight".to_string(),
        policy.quality_weight.to_string(),
    );
    inputs.insert(
        "max_refinement_docs".to_string(),
        policy.max_refinement_docs.to_string(),
    );
    inputs.insert(
        "semantic_budget_mb".to_string(),
        policy.semantic_budget_mb.to_string(),
    );
    inputs.insert(
        "min_free_disk_mb".to_string(),
        policy.min_free_disk_mb.to_string(),
    );
    inputs.insert(
        "max_model_size_mb".to_string(),
        policy.max_model_size_mb.to_string(),
    );
    inputs.insert(
        "max_backfill_threads".to_string(),
        policy.max_backfill_threads.to_string(),
    );
    inputs.insert(
        "max_backfill_rss_mb".to_string(),
        policy.max_backfill_rss_mb.to_string(),
    );
    inputs.insert(
        "idle_delay_seconds".to_string(),
        policy.idle_delay_seconds.to_string(),
    );
    inputs.insert(
        "chunk_timeout_seconds".to_string(),
        policy.chunk_timeout_seconds.to_string(),
    );
    inputs.insert(
        "semantic_schema_version".to_string(),
        policy.semantic_schema_version.to_string(),
    );
    inputs.insert(
        "chunking_strategy_version".to_string(),
        policy.chunking_strategy_version.to_string(),
    );
    inputs.insert(
        "semantic_available".to_string(),
        semantic_available.to_string(),
    );
    inputs.insert(
        "semantic_fallback_mode".to_string(),
        semantic_fallback_mode.unwrap_or("none").to_string(),
    );

    let fallback_state = if !policy.mode.should_build_semantic() {
        PolicyFallbackState::Disabled
    } else if semantic_fallback_mode.is_some() || !semantic_available {
        PolicyFallbackState::Conservative
    } else {
        PolicyFallbackState::NotNeeded
    };
    let status = match fallback_state {
        PolicyFallbackState::Disabled => PolicyControllerStatus::Disabled,
        PolicyFallbackState::Conservative => PolicyControllerStatus::Fallback,
        _ => PolicyControllerStatus::Active,
    };
    let decision_reason = match (policy.mode, semantic_available, semantic_fallback_mode) {
        (SemanticMode::LexicalOnly, _, _) => "semantic disabled by lexical_only policy",
        (mode, _, Some("lexical")) if mode.requires_semantic() => {
            "strict semantic policy observed lexical fallback; semantic is unavailable"
        }
        (_, _, Some(mode)) => {
            if mode == "lexical" {
                "semantic unavailable; lexical fallback remains active"
            } else {
                "semantic fallback mode reported by asset inspection"
            }
        }
        (_, false, _) => "semantic assets unavailable; conservative lexical floor remains active",
        _ => "semantic policy active",
    };

    PolicyControllerSnapshot {
        controller_id: "semantic_search".to_string(),
        policy_id: format!("semantic.{}.v1", policy.mode.as_str()),
        policy_version: format!(
            "semantic_schema_{}+chunking_{}",
            policy.semantic_schema_version, policy.chunking_strategy_version
        ),
        status,
        fallback_state,
        conservative_fallback: fallback_state == PolicyFallbackState::Conservative,
        decision_reason: decision_reason.to_string(),
        inputs,
    }
}

pub fn lexical_rebuild_policy_controller_snapshot(pipeline: &Value) -> PolicyControllerSnapshot {
    let mut inputs = BTreeMap::new();
    for key in [
        "controller_mode",
        "controller_restore_clear_samples",
        "controller_restore_hold_ms",
        "pipeline_channel_size",
        "pipeline_max_message_bytes_in_flight",
        "page_prep_workers",
        "staged_merge_workers",
        "staged_shard_builders",
        "controller_loadavg_high_watermark_1m",
        "controller_loadavg_low_watermark_1m",
    ] {
        insert_json_input(&mut inputs, key, pipeline.get(key));
    }

    let runtime = pipeline.get("runtime");
    let controller_mode = runtime
        .and_then(|value| value.get("controller_mode"))
        .and_then(Value::as_str);
    let controller_reason = runtime
        .and_then(|value| value.get("controller_reason"))
        .and_then(Value::as_str)
        .unwrap_or("pipeline settings active");
    let fallback_state = match controller_mode {
        Some("disabled") => PolicyFallbackState::Disabled,
        Some("conservative") | Some("throttled") | Some("reduced") => {
            PolicyFallbackState::Conservative
        }
        _ => PolicyFallbackState::NotNeeded,
    };
    let status = match fallback_state {
        PolicyFallbackState::Disabled => PolicyControllerStatus::Disabled,
        PolicyFallbackState::Conservative => PolicyControllerStatus::Fallback,
        PolicyFallbackState::NotNeeded => PolicyControllerStatus::Active,
    };

    PolicyControllerSnapshot {
        controller_id: "lexical_rebuild_pipeline".to_string(),
        policy_id: "lexical_rebuild.pipeline.v1".to_string(),
        policy_version: "pipeline_settings_v1".to_string(),
        status,
        fallback_state,
        conservative_fallback: fallback_state == PolicyFallbackState::Conservative,
        decision_reason: controller_reason.to_string(),
        inputs,
    }
}

fn insert_json_input(inputs: &mut BTreeMap<String, String>, key: &str, value: Option<&Value>) {
    inputs.insert(
        key.to_string(),
        value
            .map(json_input_string)
            .unwrap_or_else(|| "null".to_string()),
    );
}

fn json_input_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::policy::{CliSemanticOverrides, SemanticMode};
    use serde_json::json;

    #[test]
    fn registry_snapshot_is_deterministic_and_sorted() {
        let policy = SemanticPolicy::compiled_defaults();
        let pipeline = pipeline_fixture();

        let first = policy_registry_snapshot(&policy, false, Some("lexical"), &pipeline);
        let second = policy_registry_snapshot(&policy, false, Some("lexical"), &pipeline);

        assert_eq!(first, second);
        assert_eq!(
            first.controller_ids(),
            BTreeSet::from(["lexical_rebuild_pipeline", "semantic_search"])
        );
        assert_eq!(
            first.controllers[0].controller_id,
            "lexical_rebuild_pipeline"
        );
        assert_eq!(first.controllers[1].controller_id, "semantic_search");
    }

    #[test]
    fn semantic_policy_snapshot_reports_lexical_fallback_without_changing_policy() {
        let policy = SemanticPolicy::compiled_defaults();

        let snapshot = semantic_policy_controller_snapshot(&policy, false, Some("lexical"));

        assert_eq!(snapshot.status, PolicyControllerStatus::Fallback);
        assert_eq!(snapshot.fallback_state, PolicyFallbackState::Conservative);
        assert!(snapshot.conservative_fallback);
        assert_eq!(snapshot.inputs["mode"], "hybrid_preferred");
        assert_eq!(policy.mode, SemanticMode::HybridPreferred);
    }

    #[test]
    fn semantic_policy_snapshot_reports_disabled_lexical_only_policy() {
        let policy =
            SemanticPolicy::compiled_defaults().with_cli_overrides(&CliSemanticOverrides {
                mode: Some(SemanticMode::LexicalOnly),
                ..CliSemanticOverrides::default()
            });

        let snapshot = semantic_policy_controller_snapshot(&policy, true, None);

        assert_eq!(snapshot.status, PolicyControllerStatus::Disabled);
        assert_eq!(snapshot.fallback_state, PolicyFallbackState::Disabled);
        assert!(!snapshot.conservative_fallback);
        assert_eq!(snapshot.policy_id, "semantic.lexical_only.v1");
    }

    #[test]
    fn lexical_rebuild_snapshot_uses_only_supplied_pipeline_json() {
        let pipeline = pipeline_fixture();

        let first = lexical_rebuild_policy_controller_snapshot(&pipeline);
        let second = lexical_rebuild_policy_controller_snapshot(&pipeline);

        assert_eq!(first, second);
        assert_eq!(first.status, PolicyControllerStatus::Fallback);
        assert_eq!(first.fallback_state, PolicyFallbackState::Conservative);
        assert_eq!(first.inputs["pipeline_channel_size"], "128");
        assert_eq!(first.decision_reason, "load pressure reduced workers");
    }

    fn pipeline_fixture() -> Value {
        json!({
            "pipeline_channel_size": 128,
            "pipeline_max_message_bytes_in_flight": 1048576,
            "page_prep_workers": 12,
            "staged_merge_workers": 4,
            "staged_shard_builders": 8,
            "controller_mode": "auto",
            "controller_restore_clear_samples": 3,
            "controller_restore_hold_ms": 5000,
            "controller_loadavg_high_watermark_1m": 1.75,
            "controller_loadavg_low_watermark_1m": 0.75,
            "runtime": {
                "controller_mode": "throttled",
                "controller_reason": "load pressure reduced workers"
            }
        })
    }
}
