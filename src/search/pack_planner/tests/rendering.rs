use super::super::*;
use super::*;

#[test]
fn render_compact_json_base_pack_matches_golden_shape() {
    let mut item = candidate("base", "local", "/s/base.jsonl", 10.0);
    item.excerpt = "Planner output cites existing evidence.".to_string();
    item.match_type = "implicit_wildcard".to_string();
    let plan = plan_answer_pack(request(vec![item])).unwrap();
    let req = render_request(PackRenderFormat::CompactJson);

    let rendered = render_answer_pack(&plan, &req).unwrap();
    let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();

    assert!(!rendered.contains('\n'));
    assert_eq!(value, render_answer_pack_value(&plan, &req).unwrap());
    assert_eq!(value["schema_version"], "cass.pack.v2");
    assert_eq!(value["_meta"]["format"], "compact");
    assert_eq!(value["_meta"]["partial"], false);
    assert_eq!(
        value["budget"],
        serde_json::json!({
            "elapsed_ms": 7,
            "budget_ms": 8_000,
            "timed_out": false,
            "skipped_sections": [],
            "recommended_next_probe": null,
        })
    );
    assert_eq!(value["query"]["text"], "pack handoff");
    assert_eq!(value["realized"]["fallback_mode"], "lexical");
    assert_eq!(
        value["evidence"][0]["citation"]["source_path"],
        "/s/base.jsonl"
    );
    assert_eq!(
        value["evidence"][0]["citation"]["match_type"],
        "implicit_wildcard"
    );
    assert_eq!(
        value["pack"]["handoff"][0]["evidence_ids"][0],
        value["evidence"][0]["id"]
    );
}

#[test]
fn render_jsonl_empty_pack_matches_golden_line_order() {
    let plan = plan_answer_pack(request(Vec::new())).unwrap();
    let req = render_request(PackRenderFormat::Jsonl);

    let rendered = render_answer_pack(&plan, &req).unwrap();
    let lines: Vec<_> = rendered.lines().collect();

    assert_eq!(lines.len(), 4);
    assert!(lines[0].starts_with("{\"_meta\":"));
    assert!(lines[1].starts_with("{\"pack\":"));
    assert!(lines[2].starts_with("{\"omitted\":"));
    assert!(lines[3].starts_with("{\"privacy\":"));
    let meta: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(meta["schema_version"], "cass.pack.v2");
    let omitted: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
    assert_eq!(
        meta["_meta"]["warnings"],
        serde_json::json!(["no_evidence_found", "semantic_fallback_lexical"])
    );
    assert_eq!(meta["_meta"]["partial"], false);
    assert_eq!(meta["budget"]["budget_ms"], 8_000);
    assert_eq!(meta["budget"]["skipped_sections"], serde_json::json!([]));
    assert_eq!(omitted["omitted"]["count"], 0);
}

#[test]
fn render_budget_timeout_is_partial_across_structured_formats() -> Result<(), String> {
    let plan = plan_answer_pack(request(vec![candidate(
        "budget-timeout",
        "local",
        "/s/budget-timeout.jsonl",
        10.0,
    )]))
    .map_err(|err| format!("{err:?}"))?;
    let expected_budget = serde_json::json!({
        "elapsed_ms": 125,
        "budget_ms": 100,
        "timed_out": true,
        "skipped_sections": ["semantic_refinement", "source_health"],
        "recommended_next_probe": "cass health --json",
    });

    for format in [PackRenderFormat::Json, PackRenderFormat::CompactJson] {
        let mut req = render_request(format);
        req.budget = timed_out_budget();

        let rendered = render_answer_pack(&plan, &req)
            .map_err(|err| format!("{} render failed: {}", err.format, err.message))?;
        let value: serde_json::Value =
            serde_json::from_str(&rendered).map_err(|err| err.to_string())?;

        assert_eq!(value["_meta"]["partial"], true);
        assert_eq!(value["budget"], expected_budget);
        let evidence = value["evidence"]
            .as_array()
            .ok_or_else(|| "structured pack evidence must be an array".to_string())?;
        assert_eq!(evidence.len(), 1);
    }

    let mut jsonl_req = render_request(PackRenderFormat::Jsonl);
    jsonl_req.budget = timed_out_budget();
    let jsonl = render_answer_pack(&plan, &jsonl_req)
        .map_err(|err| format!("{} render failed: {}", err.format, err.message))?;
    let header_line = jsonl
        .lines()
        .next()
        .ok_or_else(|| "JSONL pack must include a header line".to_string())?;
    let header: serde_json::Value =
        serde_json::from_str(header_line).map_err(|err| err.to_string())?;
    assert_eq!(header["_meta"]["partial"], true);
    assert_eq!(header["budget"], expected_budget);

    let mut toon_req = render_request(PackRenderFormat::Toon);
    toon_req.budget = timed_out_budget();
    let value = render_answer_pack_value(&plan, &toon_req)
        .map_err(|err| format!("{} render failed: {}", err.format, err.message))?;
    let toon = render_answer_pack(&plan, &toon_req)
        .map_err(|err| format!("{} render failed: {}", err.format, err.message))?;
    assert_eq!(value["_meta"]["partial"], true);
    assert_eq!(value["budget"], expected_budget);
    assert_eq!(toon, toon::encode(value, Some(pack_toon_encode_options())));
    Ok(())
}

#[test]
fn render_budget_skips_are_partial_without_timeout() -> Result<(), String> {
    let plan = plan_answer_pack(request(Vec::new())).map_err(|err| format!("{err:?}"))?;
    let mut req = render_request(PackRenderFormat::Json);
    req.budget = BudgetBlock {
        elapsed_ms: 75,
        budget_ms: 100,
        timed_out: false,
        skipped_sections: vec!["selection_explanations".to_string()],
        recommended_next_probe: Some("cass pack --help".to_string()),
    };

    let value = render_answer_pack_value(&plan, &req)
        .map_err(|err| format!("{} render failed: {}", err.format, err.message))?;

    assert_eq!(value["_meta"]["partial"], true);
    assert_eq!(value["budget"]["timed_out"], false);
    assert_eq!(
        value["budget"]["skipped_sections"],
        serde_json::json!(["selection_explanations"])
    );
    Ok(())
}

#[test]
fn render_markdown_duplicate_omission_matches_golden_text() {
    let first = candidate("a", "local", "/s/a.jsonl", 10.0);
    let mut duplicate = candidate("b", "local", "/s/b.jsonl", 9.0);
    duplicate.content_hash = first.content_hash.clone();
    let plan = plan_answer_pack(request(vec![first, duplicate])).unwrap();
    let req = render_request(PackRenderFormat::Markdown);
    let evidence_id = &plan.evidence[0].id;

    let rendered = render_answer_pack(&plan, &req).unwrap();

    assert_eq!(
        rendered,
        format!(
            "# pack handoff\n\n\
             ## Warnings\n\
             - semantic\\_fallback\\_lexical\n\n\
             ## Handoff\n\
             - 0123456789abcdef [{evidence_id}]\n\n\
             ## Evidence\n\
             [{evidence_id}] codex local /s/a.jsonl:10-12\n\n\
             ```text\n0123456789abcdef\n```\n\n\n\
             ## Omitted\n\
             - duplicate_content: /s/b.jsonl:10\n"
        )
    );
}

#[test]
fn render_markdown_preserves_complete_redacted_excerpts_as_literal_code() {
    use pulldown_cmark::{Event, Parser, Tag, TagEnd};

    let mut first = candidate("literal", "local", "/s/[literal].jsonl", 10.0);
    first.excerpt = format!(
        "\n\n<script>alert(1)</script> [link](https://example.invalid)\n{}\n\n\
         ```rust\nfn main() {{}}\n```\n~~~\n# source heading\n\
         Authorization: Bearer abcdefghijklmnopqrst\nfinal preserved context",
        "Unicode αβ 🚀 context. ".repeat(25)
    );
    let mut second = candidate("second", "local", "/s/second.jsonl", 9.0);
    second.excerpt =
        "Second record\n    original indentation\n\ttab and trailing spaces  \n\n".into();
    let mut plan_request = request(vec![first, second]);
    plan_request.limits.max_tokens = 12_000;
    plan_request.limits.max_excerpt_chars = 1_600;
    let plan = plan_answer_pack(plan_request).expect("plan literal excerpts");
    assert_eq!(plan.evidence.len(), 2);
    assert!(plan.evidence[0].excerpt.chars().count() > 220);
    assert!(plan.evidence[0].excerpt.starts_with("\n\n"));
    assert!(
        plan.evidence[0]
            .excerpt
            .ends_with("final preserved context")
    );
    assert!(plan.evidence[1].excerpt.ends_with("\n\n"));
    let mut req = render_request(PackRenderFormat::Markdown);
    req.limits.max_tokens = 12_000;
    req.limits.max_excerpt_chars = 1_600;
    req.query_text = "handoff <img src=x> [query](https://example.invalid)".into();

    for rendered in [
        render_answer_pack(&plan, &req).expect("render Markdown"),
        render_answer_pack_without_trust_correlation(&plan, &req)
            .expect("render Markdown without advisory correlation"),
    ] {
        assert!(!rendered.contains("abcdefghijklmnopqrst"));
        let mut excerpts = Vec::new();
        let mut current_excerpt = None::<String>;
        for event in Parser::new(&rendered) {
            match event {
                Event::Start(Tag::CodeBlock(_)) => {
                    current_excerpt = Some(String::new());
                }
                Event::Text(text) => {
                    if let Some(excerpt) = current_excerpt.as_mut() {
                        excerpt.push_str(&text);
                    }
                }
                Event::End(TagEnd::CodeBlock) => {
                    excerpts.push(current_excerpt.take().expect("open evidence block"));
                }
                Event::Html(_)
                | Event::InlineHtml(_)
                | Event::Start(Tag::Link { .. } | Tag::Image { .. }) => {
                    panic!("source markup must remain literal text");
                }
                _ => {}
            }
        }
        let expected = plan
            .evidence
            .iter()
            .map(|item| {
                if item.excerpt.ends_with('\n') {
                    item.excerpt.clone()
                } else {
                    format!("{}\n", item.excerpt)
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(
            excerpts, expected,
            "each complete excerpt must survive Markdown parsing"
        );
    }
}

#[test]
fn render_markdown_empty_pack_does_not_invent_evidence_blocks() {
    let plan = plan_answer_pack(request(Vec::new())).expect("empty plan");
    let rendered = render_answer_pack(&plan, &render_request(PackRenderFormat::Markdown))
        .expect("empty Markdown pack");
    assert!(rendered.contains("No evidence selected."));
    assert!(rendered.contains("No cited evidence."));
    assert!(
        !pulldown_cmark::Parser::new(&rendered).any(|event| matches!(
            event,
            pulldown_cmark::Event::Start(pulldown_cmark::Tag::CodeBlock(_))
        ))
    );
}

#[test]
fn render_stale_source_pack_marks_health_and_freshness() {
    let mut stale = candidate("stale", "remote", "/s/stale.jsonl", 10.0);
    stale.source_readiness = PackSourceReadiness::StaleReadable;
    let plan = plan_answer_pack(request(vec![stale])).unwrap();
    let req = render_request(PackRenderFormat::Json);

    let value = render_answer_pack_value(&plan, &req).unwrap();

    assert_eq!(value["health"]["healthy"], false);
    assert_eq!(
        value["health"]["recommended_action"],
        "inspect cass health --json and source status"
    );
    assert_eq!(
        value["health"]["source_readiness"][0]["readiness"],
        "stale_readable"
    );
    assert_eq!(value["freshness"]["stale_evidence_count"], 0);
    assert_eq!(value["pack"]["source_summary"][0]["healthy"], false);
}

#[test]
fn render_stale_evidence_count_uses_age_not_source_readiness() {
    let mut old_healthy = candidate("old-healthy", "local", "/s/old.jsonl", 10.0);
    old_healthy.created_at_ms = Some(999_999);
    let mut recent_stale_source =
        candidate("recent-stale-source", "remote", "/s/recent.jsonl", 9.0);
    recent_stale_source.created_at_ms = Some(1_060_000);
    recent_stale_source.source_readiness = PackSourceReadiness::StaleReadable;

    let plan = plan_answer_pack(request(vec![old_healthy, recent_stale_source])).unwrap();
    let req = render_request(PackRenderFormat::Json);

    let value = render_answer_pack_value(&plan, &req).unwrap();

    assert_eq!(value["freshness"]["window_seconds"], 60);
    assert_eq!(value["freshness"]["stale_evidence_count"], 1);
    assert_eq!(value["health"]["healthy"], false);
    assert!(
        value["health"]["source_readiness"]
            .as_array()
            .unwrap()
            .iter()
            // ubs:ignore — compares a public readiness wire label, not a secret.
            .any(|source| source["readiness"] == "stale_readable")
    );
}

#[test]
fn render_healthy_readiness_reports_generation_and_ready_states() {
    let plan = plan_answer_pack(request(vec![candidate(
        "healthy",
        "local",
        "/s/healthy.jsonl",
        10.0,
    )]))
    .unwrap();
    let mut req = render_request(PackRenderFormat::Json);
    req.fallback_mode = None;
    req.semantic_joined = true;
    req.readiness.index_generation = Some("lexical-generation-42".to_string());
    req.readiness.semantic_readiness = PackSemanticReadiness::Joined;

    let value = render_answer_pack_value(&plan, &req).unwrap();

    assert_eq!(value["health"]["healthy"], true);
    assert_eq!(value["health"]["index_state"], "ready");
    assert_eq!(value["health"]["index_generation"], "lexical-generation-42");
    assert_eq!(value["health"]["lexical_readiness"], "ready");
    assert_eq!(value["health"]["semantic_state"], "joined");
    assert_eq!(
        value["health"]["recommended_action"],
        serde_json::Value::Null
    );
    assert_eq!(value["warnings"], serde_json::json!([]));
}

#[test]
fn render_stale_lexical_readiness_reports_action() {
    let plan = plan_answer_pack(request(vec![candidate(
        "stale-index",
        "local",
        "/s/stale-index.jsonl",
        10.0,
    )]))
    .unwrap();
    let mut req = render_request(PackRenderFormat::Json);
    req.readiness.lexical_readiness = PackLexicalReadiness::Stale;

    let value = render_answer_pack_value(&plan, &req).unwrap();

    assert_eq!(value["health"]["healthy"], false);
    assert_eq!(value["health"]["index_state"], "stale");
    assert_eq!(value["health"]["lexical_readiness"], "stale");
    assert_eq!(
        value["health"]["recommended_action"],
        "refresh lexical index with cass index --full"
    );
    assert_eq!(value["warnings"][0], "lexical_index_stale");
}

#[test]
fn render_semantic_unavailable_keeps_lexical_health_truthful() {
    let plan = plan_answer_pack(request(vec![candidate(
        "semantic",
        "local",
        "/s/semantic.jsonl",
        10.0,
    )]))
    .unwrap();
    let mut req = render_request(PackRenderFormat::Json);
    req.readiness.semantic_readiness = PackSemanticReadiness::Unavailable;

    let value = render_answer_pack_value(&plan, &req).unwrap();

    assert_eq!(value["health"]["healthy"], true);
    assert_eq!(value["health"]["semantic_state"], "unavailable");
    assert_eq!(
        value["health"]["recommended_action"],
        "continue with lexical evidence or install semantic model explicitly"
    );
    assert_eq!(
        value["warnings"],
        serde_json::json!(["semantic_unavailable_lexical_fallback"])
    );
}

fn fallback_plan() -> PlannedAnswerPack {
    plan_answer_pack(request(vec![candidate(
        "fallback",
        "local",
        "/s/fallback.jsonl",
        10.0,
    )]))
    .unwrap()
}

fn warning_list(value: &serde_json::Value) -> Vec<String> {
    value["warnings"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|warning| warning.as_str().map(str::to_string))
        .collect()
}

/// GH #507: a fallback the executor explains carries that cause.
#[test]
fn render_fallback_reports_the_executor_cause() {
    let mut req = render_request(PackRenderFormat::Json);
    req.fallback_reason = Some("semantic_budget_limited".to_string());
    let value = render_answer_pack_value(&fallback_plan(), &req).unwrap();

    assert_eq!(value["realized"]["fallback_mode"], "lexical");
    assert_eq!(
        value["realized"]["fallback_reason"],
        "semantic_budget_limited"
    );
    assert_eq!(value["health"]["semantic_state"], "fallback_lexical");
    let warnings = warning_list(&value);
    assert!(warnings.contains(&"semantic_fallback_lexical".to_string()));
    assert!(!warnings.contains(&"semantic_fallback_reason_unreported".to_string()));
}

/// A fallback without a supplied cause is flagged, never given one.
#[test]
fn render_unexplained_fallback_is_flagged_not_filled_in() {
    for reason in [None, Some("   ".to_string())] {
        let mut req = render_request(PackRenderFormat::Json);
        req.fallback_reason = reason;
        let value = render_answer_pack_value(&fallback_plan(), &req).unwrap();

        assert!(value["realized"]["fallback_reason"].is_null());
        let warnings = warning_list(&value);
        assert!(warnings.contains(&"semantic_fallback_lexical".to_string()));
        assert!(warnings.contains(&"semantic_fallback_reason_unreported".to_string()));
    }
}

/// An explicit lexical request is not a fallback, and a stray cause
/// without a fallback is not reported.
#[test]
fn render_lexical_request_reports_no_fallback_or_cause() {
    let mut req = render_request(PackRenderFormat::Json);
    req.search_mode = "lexical".to_string();
    req.fallback_mode = None;
    req.fallback_reason = Some("semantic_absent".to_string());
    req.readiness.semantic_readiness = PackSemanticReadiness::Disabled;
    let value = render_answer_pack_value(&fallback_plan(), &req).unwrap();

    assert!(value["realized"]["fallback_mode"].is_null());
    assert!(value["realized"]["fallback_reason"].is_null());
    let warnings = warning_list(&value);
    assert!(warnings.contains(&"semantic_disabled".to_string()));
    assert!(
        !warnings
            .iter()
            .any(|warning| warning.starts_with("semantic_fallback"))
    );
}

/// Pack's own lexical selection is not an asset problem, so it must not
/// send the operator to install a model; an absent model still does.
#[test]
fn render_pack_policy_fallback_does_not_recommend_a_model_install() {
    let mut req = render_request(PackRenderFormat::Json);
    req.fallback_reason = Some("pack_enrichment_unavailable".to_string());
    let value = render_answer_pack_value(&fallback_plan(), &req).unwrap();
    assert_eq!(value["health"]["healthy"], true);
    let action = value["health"]["recommended_action"].as_str().unwrap();
    assert!(action.contains("cass search --mode hybrid"), "{action}");
    assert!(!action.contains("install semantic model"), "{action}");

    req.fallback_reason = Some("semantic_absent".to_string());
    let value = render_answer_pack_value(&fallback_plan(), &req).unwrap();
    assert_eq!(
        value["health"]["recommended_action"],
        "continue with lexical evidence or install semantic model explicitly"
    );
}

/// The cause passes through the envelope redaction and is counted.
#[test]
fn render_fallback_reason_is_redacted_and_counted() {
    let mut req = render_request(PackRenderFormat::Json);
    req.fallback_reason = Some("semantic_absent".to_string());
    let value = render_answer_pack_value(&fallback_plan(), &req).unwrap();
    assert_eq!(value["privacy"]["redaction_applied"], false);

    req.fallback_reason = Some("model at /home/alice/.cache/cass/model.bin unreadable".to_string());
    let value = render_answer_pack_value(&fallback_plan(), &req).unwrap();

    let reason = value["realized"]["fallback_reason"].as_str().unwrap();
    assert!(!reason.contains("/home/alice"), "{reason}");
    assert_eq!(value["privacy"]["redaction_applied"], true);
}

#[test]
fn render_active_rebuild_marks_pack_not_fresh() {
    let plan = plan_answer_pack(request(vec![candidate(
        "rebuild",
        "local",
        "/s/rebuild.jsonl",
        10.0,
    )]))
    .unwrap();
    let mut req = render_request(PackRenderFormat::Json);
    req.readiness.active_rebuild = true;
    req.readiness.lock_state = Some("writer-lock".to_string());

    let value = render_answer_pack_value(&plan, &req).unwrap();

    assert_eq!(value["health"]["healthy"], false);
    assert_eq!(value["health"]["active_rebuild"], true);
    assert_eq!(value["health"]["lock_state"], "writer-lock");
    assert_eq!(
        value["health"]["recommended_action"],
        "wait for active rebuild or inspect cass status --json"
    );
    assert!(
        value["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|warning| warning == "active_rebuild")
    );
}

#[test]
fn render_missing_db_reports_reindex_action() {
    let plan = plan_answer_pack(request(Vec::new())).unwrap();
    let mut req = render_request(PackRenderFormat::Json);
    req.readiness.missing_database = true;
    req.readiness.lexical_readiness = PackLexicalReadiness::Missing;

    let value = render_answer_pack_value(&plan, &req).unwrap();

    assert_eq!(value["health"]["healthy"], false);
    assert_eq!(value["health"]["missing_database"], true);
    assert_eq!(value["health"]["index_state"], "missing");
    assert_eq!(
        value["health"]["recommended_action"],
        "run cass index --full"
    );
    assert!(
        value["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|warning| warning == "missing_database")
    );
}

#[test]
fn render_source_sync_gap_and_pruned_source_metadata() {
    let plan = plan_answer_pack(request(vec![candidate(
        "remote-gap",
        "remote",
        "/s/remote-gap.jsonl",
        10.0,
    )]))
    .unwrap();
    let mut req = render_request(PackRenderFormat::Json);
    req.readiness.source_sync_gaps = vec![
        PackSourceSyncGap {
            source_id: "remote".to_string(),
            origin_kind: "ssh".to_string(),
            kind: PackSourceSyncGapKind::RemoteStale,
            lag_seconds: Some(3_600),
            last_synced_at_ms: Some(1_000_000),
            recommended_action: Some("run cass index --json".to_string()),
        },
        PackSourceSyncGap {
            source_id: "old-laptop".to_string(),
            origin_kind: "ssh".to_string(),
            kind: PackSourceSyncGapKind::SourcePruned,
            lag_seconds: None,
            last_synced_at_ms: None,
            recommended_action: Some("remove or refresh pruned source".to_string()),
        },
    ];

    let value = render_answer_pack_value(&plan, &req).unwrap();

    assert_eq!(value["health"]["healthy"], false);
    assert_eq!(
        value["health"]["source_sync_gaps"][0]["kind"],
        "remote_stale"
    );
    assert_eq!(
        value["health"]["source_sync_gaps"][1]["kind"],
        "source_pruned"
    );
    assert_eq!(
        value["health"]["recommended_action"],
        "inspect cass sources doctor --json and source status"
    );
    assert!(
        value["warnings"]
            .as_array()
            .unwrap()
            .iter()
            // ubs:ignore — compares a public diagnostic fixture string, not a secret.
            .any(|warning| warning == "source_sync_gap:[REDACTED_SOURCE]:source_pruned")
    );
    assert_eq!(
        value["health"]["source_sync_gaps"][1]["source_id"],
        REDACTED_SOURCE_MARKER
    );
}

#[test]
fn render_toon_matches_existing_toon_encoder() {
    let plan = plan_answer_pack(request(vec![candidate(
        "toon",
        "local",
        "/s/toon.jsonl",
        10.0,
    )]))
    .unwrap();
    let req = render_request(PackRenderFormat::Toon);
    let value = render_answer_pack_value(&plan, &req).unwrap();

    let rendered = render_answer_pack(&plan, &req).unwrap();

    assert_eq!(
        rendered,
        toon::encode(value, Some(pack_toon_encode_options()))
    );
}
