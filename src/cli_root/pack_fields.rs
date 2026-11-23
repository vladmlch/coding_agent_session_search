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

pub(crate) fn filter_pack_fields(
    value: serde_json::Value,
    fields: Option<&Vec<String>>,
) -> CliResult<serde_json::Value> {
    let Some(fields) = fields else {
        return Ok(value);
    };
    let Some(field_mask) = expand_pack_field_mask(fields)? else {
        return Ok(value);
    };
    let mut matched_fields = Vec::new();
    let mut ignored_fields = field_mask.ignored_fields;
    for field in field_mask.fields {
        if pack_field_projects(&value, &field) {
            matched_fields.push(field);
        } else {
            ignored_fields.push(field);
        }
    }
    if matched_fields.is_empty() {
        return Err(pack_invalid_field_error(
            field_mask
                .requested_fields
                .first()
                .map(String::as_str)
                .unwrap_or(""),
        ));
    }

    let mut value = value;
    append_pack_field_mask_warnings(&mut value, &ignored_fields);

    let mut filtered = serde_json::Map::new();
    for field in matched_fields {
        let _ = merge_projected_pack_field(&mut filtered, &value, &field);
    }
    if !ignored_fields.is_empty() && !filtered.contains_key("_meta") {
        let _ = merge_projected_pack_field(&mut filtered, &value, "_meta.warnings");
    }
    Ok(serde_json::Value::Object(filtered))
}

pub(crate) fn validate_pack_fields(fields: Option<&Vec<String>>) -> CliResult<()> {
    if let Some(fields) = fields {
        let _ = expand_pack_field_mask(fields)?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PackFieldMask {
    pub(crate) fields: BTreeSet<String>,
    pub(crate) ignored_fields: Vec<String>,
    pub(crate) requested_fields: Vec<String>,
}

pub(crate) fn expand_pack_field_mask(fields: &[String]) -> CliResult<Option<PackFieldMask>> {
    let mut set = BTreeSet::new();
    let mut ignored_fields = Vec::new();
    let mut requested_fields = Vec::new();
    for field in fields
        .iter()
        .map(|field| field.trim())
        .filter(|field| !field.is_empty())
    {
        requested_fields.push(field.to_string());
        match field {
            "*" | "all" => return Ok(None),
            "standard" | "full" => {
                set.extend(PACK_TOP_LEVEL_FIELDS.iter().copied().map(str::to_string));
            }
            "minimal" => {
                set.extend([
                    "schema_version".to_string(),
                    "query.text".to_string(),
                    "realized.search_mode".to_string(),
                    "pack.answer_outline".to_string(),
                    "evidence[].citation".to_string(),
                    "evidence[].excerpt".to_string(),
                    "omitted.count".to_string(),
                    "privacy.redaction_applied".to_string(),
                ]);
            }
            "summary" => {
                set.extend([
                    "schema_version".to_string(),
                    "_meta".to_string(),
                    "limits".to_string(),
                    "realized".to_string(),
                    "health".to_string(),
                    "freshness".to_string(),
                    "pack".to_string(),
                    "privacy".to_string(),
                    "warnings".to_string(),
                ]);
            }
            "meta" => {
                set.insert("_meta".to_string());
            }
            field @ ("schema_version" | "_meta" | "query" | "limits" | "realized" | "health"
            | "freshness" | "pack" | "evidence" | "omitted" | "privacy" | "warnings") => {
                set.insert(field.to_string());
            }
            field
                if pack_field_root(field)
                    .is_some_and(|root| PACK_TOP_LEVEL_FIELDS.contains(&root)) =>
            {
                set.insert(field.to_string());
            }
            other => {
                ignored_fields.push(other.to_string());
            }
        }
    }
    if set.is_empty() {
        if let Some(field) = ignored_fields.first() {
            Err(pack_invalid_field_error(field))
        } else {
            Ok(None)
        }
    } else {
        Ok(Some(PackFieldMask {
            fields: set,
            ignored_fields,
            requested_fields,
        }))
    }
}

pub(crate) const PACK_TOP_LEVEL_FIELDS: &[&str] = &[
    "schema_version",
    "_meta",
    "query",
    "limits",
    "realized",
    "health",
    "freshness",
    "pack",
    "evidence",
    "omitted",
    "privacy",
    "warnings",
];

pub(crate) fn pack_invalid_field_error(field: &str) -> CliError {
    CliError {
        code: 2,
        kind: CliErrorKind::PackInvalidField.kind_str(),
        message: format!("unknown pack field mask: {field}"),
        hint: Some(
            "Use minimal, standard, full, summary, all, or top-level fields such as evidence, health, freshness, omitted, privacy."
                .to_string(),
        ),
        retryable: false,
    }
}

pub(crate) fn pack_field_root(field: &str) -> Option<&str> {
    let raw_root = field.split('.').next()?;
    let root = raw_root.strip_suffix("[]").unwrap_or(raw_root);
    (!root.is_empty()).then_some(root)
}

pub(crate) fn append_pack_field_mask_warnings(
    value: &mut serde_json::Value,
    ignored_fields: &[String],
) {
    if ignored_fields.is_empty() {
        return;
    }

    let warnings = ignored_fields
        .iter()
        .map(|field| format!("unknown_pack_field_mask_ignored:{field}"))
        .collect::<Vec<_>>();
    for path in ["_meta", ""] {
        let Some(target) = pack_warning_array_mut(value, path) else {
            continue;
        };
        target.extend(warnings.iter().cloned().map(serde_json::Value::String));
    }
}

pub(crate) fn pack_warning_array_mut<'a>(
    value: &'a mut serde_json::Value,
    object_path: &str,
) -> Option<&'a mut Vec<serde_json::Value>> {
    let object = if object_path.is_empty() {
        value.as_object_mut()?
    } else {
        value.get_mut(object_path)?.as_object_mut()?
    };
    object
        .entry("warnings")
        .or_insert_with(|| serde_json::Value::Array(Vec::new()))
        .as_array_mut()
}

pub(crate) fn merge_projected_pack_field(
    target: &mut serde_json::Map<String, serde_json::Value>,
    source: &serde_json::Value,
    field: &str,
) -> bool {
    let Some(projected) = projected_pack_field(source, field) else {
        return false;
    };
    merge_json_object(target, projected);
    true
}

pub(crate) fn pack_field_projects(source: &serde_json::Value, field: &str) -> bool {
    projected_pack_field(source, field).is_some()
}

pub(crate) fn projected_pack_field(
    source: &serde_json::Value,
    field: &str,
) -> Option<serde_json::Value> {
    let segments = field.split('.').collect::<Vec<_>>();
    project_pack_json_path(source, &segments)
}

pub(crate) fn project_pack_json_path(
    source: &serde_json::Value,
    segments: &[&str],
) -> Option<serde_json::Value> {
    let (segment, rest) = segments.split_first()?;
    if let Some(key) = segment.strip_suffix("[]") {
        let array = source.get(key)?.as_array()?;
        let projected_array = if rest.is_empty() {
            array.clone()
        } else if array.is_empty() {
            Vec::new()
        } else {
            let mut any_projected = false;
            let projected = array
                .iter()
                .map(|item| {
                    let item_projected = project_pack_json_path(item, rest);
                    any_projected |= item_projected.is_some();
                    item_projected.unwrap_or_else(|| serde_json::Value::Object(Default::default()))
                })
                .collect::<Vec<_>>();
            if !any_projected {
                return None;
            }
            projected
        };
        return Some(pack_singleton_json_object(
            key,
            serde_json::Value::Array(projected_array),
        ));
    }

    let child = source.get(*segment)?;
    let projected_child = if rest.is_empty() {
        child.clone()
    } else {
        project_pack_json_path(child, rest)?
    };
    Some(pack_singleton_json_object(segment, projected_child))
}

pub(crate) fn pack_singleton_json_object(key: &str, value: serde_json::Value) -> serde_json::Value {
    let mut object = serde_json::Map::new();
    object.insert(key.to_string(), value);
    serde_json::Value::Object(object)
}

pub(crate) fn merge_json_object(
    target: &mut serde_json::Map<String, serde_json::Value>,
    projected: serde_json::Value,
) {
    let serde_json::Value::Object(projected) = projected else {
        return;
    };
    for (key, value) in projected {
        match target.get_mut(&key) {
            Some(existing) => merge_json_value(existing, value),
            None => {
                target.insert(key, value);
            }
        }
    }
}

pub(crate) fn merge_json_value(target: &mut serde_json::Value, incoming: serde_json::Value) {
    match (target, incoming) {
        (serde_json::Value::Object(target), serde_json::Value::Object(incoming)) => {
            for (key, value) in incoming {
                match target.get_mut(&key) {
                    Some(existing) => merge_json_value(existing, value),
                    None => {
                        target.insert(key, value);
                    }
                }
            }
        }
        (serde_json::Value::Array(target), serde_json::Value::Array(incoming)) => {
            for (idx, value) in incoming.into_iter().enumerate() {
                if let Some(existing) = target.get_mut(idx) {
                    merge_json_value(existing, value);
                } else {
                    target.push(value);
                }
            }
        }
        (target, incoming) => {
            *target = incoming;
        }
    }
}

#[cfg(test)]
mod pack_field_mask_tests {
    use super::*;

    #[test]
    fn pack_output_budget_measures_final_encoding_and_newline_at_the_boundary() {
        use crate::search::pack_planner::{PackPlanRequest, PackRenderFormat, PackRenderRequest};
        let correlation = crate::search::trust_correlation::CorrelationIndex::default();
        for format in [
            PackRenderFormat::Json,
            PackRenderFormat::CompactJson,
            PackRenderFormat::Jsonl,
            PackRenderFormat::Toon,
            PackRenderFormat::Markdown,
        ] {
            let plan =
                crate::search::pack_planner::plan_answer_pack(PackPlanRequest::default()).unwrap();
            let mut request = PackRenderRequest {
                format,
                query_text: "quote \" slash \\ control \u{1} Unicode 界😀".to_string(),
                ..PackRenderRequest::default()
            };
            // Fixed elapsed metadata makes the exact boundary reproducible.
            request.budget.timed_out = true;
            let output = render_cli_pack_with_output_budget(
                plan.clone(),
                request.clone(),
                None,
                false,
                Instant::now(),
                &correlation,
            )
            .unwrap();
            let tokens = output.chars().count().div_ceil(4);
            let mut boundary = plan;
            boundary.diagnostics.budget.max_output_tokens_with_overflow = tokens;
            let exact = render_cli_pack_with_output_budget(
                boundary.clone(),
                request.clone(),
                None,
                false,
                Instant::now(),
                &correlation,
            )
            .unwrap();
            assert_eq!(exact, output);
            boundary.diagnostics.budget.max_output_tokens_with_overflow = tokens - 1;
            let error = render_cli_pack_with_output_budget(
                boundary,
                request,
                None,
                false,
                Instant::now(),
                &correlation,
            )
            .unwrap_err();
            assert_eq!(error.kind, "pack-budget-too-small");
            assert_eq!(error.code, 2);
            assert!(!error.retryable);
        }
    }

    #[test]
    fn pack_output_budget_measures_the_field_projection_not_hidden_metadata() {
        use crate::search::pack_planner::{PackPlanRequest, PackRenderRequest};
        let plan =
            crate::search::pack_planner::plan_answer_pack(PackPlanRequest::default()).unwrap();
        let request = PackRenderRequest {
            request_id: Some("large hidden request metadata ".repeat(3_000)),
            ..PackRenderRequest::default()
        };
        let correlation = crate::search::trust_correlation::CorrelationIndex::default();
        let error = render_cli_pack_with_output_budget(
            plan.clone(),
            request.clone(),
            None,
            false,
            Instant::now(),
            &correlation,
        )
        .unwrap_err();
        assert_eq!(error.kind, "pack-budget-too-small");
        let fields = vec!["limits".to_string()];
        let output = render_cli_pack_with_output_budget(
            plan,
            request,
            Some(&fields),
            false,
            Instant::now(),
            &correlation,
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(value.as_object().unwrap().len(), 1);
        assert!(value["limits"].is_object());
        assert!(!output.contains("large hidden"));
    }

    fn sample_pack_value() -> serde_json::Value {
        serde_json::json!({
            "schema_version": "cass.pack.v1",
            "query": {
                "text": "checkout",
                "normalized": "checkout",
                "filters": {}
            },
            "_meta": {
                "request_id": "req-1",
                "generated_at_ms": 1,
                "elapsed_ms": 2,
                "partial": false,
                "format": "json",
                "warnings": []
            },
            "limits": {
                "max_tokens": 12000,
                "estimated_tokens": 10,
                "field_mask": "standard"
            },
            "realized": {
                "search_mode": "lexical",
                "fallback_mode": null,
                "semantic_joined": false
            },
            "health": {
                "healthy": true,
                "lexical_readiness": "ready",
                "semantic_state": "unavailable"
            },
            "freshness": {
                "policy": "prefer_recent",
                "window_seconds": 86400,
                "stale_evidence_count": 0
            },
            "pack": {
                "title": "checkout",
                "answer_outline": [{"rank": 1, "heading": "checkout", "evidence_ids": ["ev_1"]}],
                "handoff": [{"rank": 1, "text": "handoff"}]
            },
            "evidence": [{
                "id": "ev_1",
                "excerpt": "retry guard was missing",
                "citation": {"source_path": "/tmp/session.jsonl", "line_start": 4},
                "selection": {"score": 1.0}
            }],
            "omitted": {
                "count": 3,
                "items": [{"reason": "duplicate_content"}]
            },
            "privacy": {
                "redaction_applied": false,
                "redaction_policy": "strict"
            },
            "warnings": []
        })
    }

    fn fields(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn pack_standard_and_full_masks_preserve_the_complete_public_envelope() {
        for preset in ["standard", "full"] {
            let input = sample_pack_value();
            let filtered = filter_pack_fields(input.clone(), Some(&fields(&[preset]))).unwrap();
            assert_eq!(filtered, input, "preset {preset}");

            let filtered = filter_pack_fields(
                sample_pack_value(),
                Some(&fields(&[preset, "no_such_field"])),
            )
            .unwrap();
            assert_eq!(filtered["evidence"], input["evidence"]);
            assert_eq!(
                filtered["_meta"]["warnings"],
                serde_json::json!(["unknown_pack_field_mask_ignored:no_such_field"])
            );
        }
    }

    #[test]
    fn pack_minimal_field_mask_projects_documented_dotted_fields() {
        let fields = fields(&["minimal"]);
        let filtered = filter_pack_fields(sample_pack_value(), Some(&fields)).unwrap();

        assert_eq!(filtered["schema_version"], "cass.pack.v1");
        assert_eq!(filtered["query"], serde_json::json!({ "text": "checkout" }));
        assert_eq!(
            filtered["realized"],
            serde_json::json!({ "search_mode": "lexical" })
        );
        assert_eq!(
            filtered["pack"],
            serde_json::json!({
                "answer_outline": [{"rank": 1, "heading": "checkout", "evidence_ids": ["ev_1"]}]
            })
        );
        assert_eq!(
            filtered["evidence"],
            serde_json::json!([{
                "citation": {"source_path": "/tmp/session.jsonl", "line_start": 4},
                "excerpt": "retry guard was missing"
            }])
        );
        assert_eq!(filtered["omitted"], serde_json::json!({ "count": 3 }));
        assert_eq!(
            filtered["privacy"],
            serde_json::json!({ "redaction_applied": false })
        );
        assert!(filtered.get("_meta").is_none());
        assert!(filtered["evidence"][0].get("selection").is_none());
    }

    #[test]
    fn pack_explicit_dotted_mask_merges_array_item_fields() {
        let fields = fields(&["query.text", "evidence[].citation", "evidence[].excerpt"]);
        let filtered = filter_pack_fields(sample_pack_value(), Some(&fields)).unwrap();

        assert_eq!(filtered["query"], serde_json::json!({ "text": "checkout" }));
        assert_eq!(
            filtered["evidence"],
            serde_json::json!([{
                "citation": {"source_path": "/tmp/session.jsonl", "line_start": 4},
                "excerpt": "retry guard was missing"
            }])
        );
    }

    #[test]
    fn pack_field_mask_projection_is_order_and_duplicate_invariant() {
        let canonical = fields(&[
            "query.text",
            "realized.search_mode",
            "pack.answer_outline",
            "evidence[].citation",
            "evidence[].excerpt",
            "omitted.count",
            "privacy.redaction_applied",
        ]);
        let permuted = fields(&[
            "privacy.redaction_applied",
            "evidence[].excerpt",
            "query.text",
            "omitted.count",
            "pack.answer_outline",
            "evidence[].citation",
            "realized.search_mode",
        ]);
        let duplicated = fields(&[
            "query.text",
            "evidence[].citation",
            "query.text",
            "realized.search_mode",
            "pack.answer_outline",
            "evidence[].excerpt",
            "omitted.count",
            "privacy.redaction_applied",
            "evidence[].citation",
        ]);

        let expected = filter_pack_fields(sample_pack_value(), Some(&canonical)).unwrap();

        assert_eq!(
            filter_pack_fields(sample_pack_value(), Some(&permuted)).unwrap(),
            expected
        );
        assert_eq!(
            filter_pack_fields(sample_pack_value(), Some(&duplicated)).unwrap(),
            expected
        );
    }

    #[test]
    fn pack_mixed_valid_and_unknown_masks_warn_without_failing() {
        let fields = fields(&["query.text", "no_such_field"]);
        let filtered = filter_pack_fields(sample_pack_value(), Some(&fields)).unwrap();

        assert_eq!(filtered["query"], serde_json::json!({ "text": "checkout" }));
        assert_eq!(
            filtered["_meta"]["warnings"],
            serde_json::json!(["unknown_pack_field_mask_ignored:no_such_field"])
        );
    }

    #[test]
    fn pack_mixed_valid_and_unknown_dotted_masks_warn_without_failing() {
        let fields = fields(&["query.text", "query.nope"]);
        let filtered = filter_pack_fields(sample_pack_value(), Some(&fields)).unwrap();

        assert_eq!(filtered["query"], serde_json::json!({ "text": "checkout" }));
        assert_eq!(
            filtered["_meta"]["warnings"],
            serde_json::json!(["unknown_pack_field_mask_ignored:query.nope"])
        );
    }

    #[test]
    fn pack_all_invalid_field_masks_still_fail() {
        let fields = fields(&["no_such_field"]);
        let err = filter_pack_fields(sample_pack_value(), Some(&fields)).unwrap_err();

        assert_eq!(err.kind, "pack-invalid-field");
    }

    #[test]
    fn pack_all_invalid_dotted_field_masks_still_fail() {
        let fields = fields(&["query.nope"]);
        let err = filter_pack_fields(sample_pack_value(), Some(&fields)).unwrap_err();

        assert_eq!(err.kind, "pack-invalid-field");
    }
}
