use super::super::*;
use super::*;

#[test]
fn pack_omits_skill_injections_before_truncation_and_preserves_safe_evidence() {
    let mut safe = candidate("safe", "local", "/work/safe.jsonl", 1.0);
    safe.excerpt = "Ordinary skill design discussion stays useful.".to_string();
    let mut candidates = vec![safe.clone()];
    for (index, marker) in [
        "Base directory for this skill:",
        "<system-reminder>",
        "The following skills are available for use with the Skill tool:",
        "skillInjection: matchedSkills",
        "<!-- skillInjection:",
    ]
    .into_iter()
    .enumerate()
    {
        let mut injected = candidate(
            &format!("skill-{index}"),
            "local",
            &format!("/work/skill-{index}.jsonl"),
            100.0,
        );
        injected.excerpt = format!("PROPRIETARY PLAYBOOK {} {marker}", "界".repeat(120));
        candidates.push(injected);
    }
    let plan = plan_answer_pack(request(candidates.clone())).unwrap();
    assert_eq!(plan.candidate_count, 6);
    assert_eq!(plan.selected_evidence_count, 1);
    assert_eq!(plan.evidence[0].candidate, safe);
    assert_eq!(plan.evidence[0].excerpt, safe.excerpt);
    assert_eq!(plan.omitted.len(), 5);
    let mut omitted_ids = HashSet::new();
    for omitted in &plan.omitted {
        assert_eq!(omitted.reason, PackOmittedReason::RedactedToEmpty);
        assert_eq!(omitted.estimated_tokens, 0);
        assert!(omitted_ids.insert(&omitted.candidate_id));
    }
    let value = render_answer_pack_value_without_trust_correlation(
        &plan,
        &render_request(PackRenderFormat::Json),
    )
    .unwrap();
    assert_eq!(value["privacy"]["redaction_applied"], true);
    assert_eq!(value["privacy"]["skill_content_included"], false);
    assert_eq!(value["privacy"]["redaction_counts"]["redacted_to_empty"], 5);
    assert!(!value.to_string().contains("PROPRIETARY PLAYBOOK"));

    candidates.remove(0);
    let excluded = plan_answer_pack(request(candidates)).unwrap();
    assert!(excluded.evidence.is_empty());
    assert_eq!(excluded.omitted.len(), 5);
    assert_eq!(excluded.estimated_tokens, 0);
}

#[test]
fn pack_skill_opt_in_keeps_credentials_redacted_and_reports_retained_evidence() {
    let credential = "abcdefghijklmnopqrst";
    let mut injected = candidate("skill", "local", "/work/skill.jsonl", 1.0);
    injected.excerpt = format!(
        "Base directory for this skill: /work/playbook\nUseful skill instructions.\nAuthorization: Bearer {credential}"
    );
    let mut plan_request = request(vec![injected]);
    plan_request.limits.max_excerpt_chars = 800;
    let excluded = plan_answer_pack(plan_request.clone()).unwrap();
    assert!(excluded.evidence.is_empty());
    assert_eq!(
        excluded.omitted[0].reason,
        PackOmittedReason::RedactedToEmpty
    );

    plan_request.include_skill_content = true;
    let plan = plan_answer_pack(plan_request).unwrap();
    assert_eq!(plan.selected_evidence_count, 1);
    assert!(plan.omitted.is_empty());
    assert!(
        plan.evidence[0]
            .excerpt
            .contains("Useful skill instructions.")
    );
    assert!(!plan.evidence[0].excerpt.contains(credential));
    assert!(plan.evidence[0].excerpt.contains(REDACTED_VALUE_MARKER));

    let render_request = render_request(PackRenderFormat::Json);
    let value = render_answer_pack_value_without_trust_correlation(&plan, &render_request).unwrap();
    assert_eq!(value["privacy"]["skill_content_included"], true);
    assert_eq!(value["privacy"]["redaction_applied"], true);
    assert!(!value.to_string().contains(credential));

    // Rendering is repeated after output-budget trimming. An opt-in alone
    // cannot claim that a fallback or reduced pack still contains skills.
    let empty = budget_fallback_answer_pack(&render_request.limits, 1).unwrap();
    let value =
        render_answer_pack_value_without_trust_correlation(&empty, &render_request).unwrap();
    assert_eq!(value["privacy"]["skill_content_included"], false);
    let ordinary = plan_answer_pack(request(vec![candidate(
        "ordinary",
        "local",
        "/work/ordinary.jsonl",
        1.0,
    )]))
    .unwrap();
    let value =
        render_answer_pack_value_without_trust_correlation(&ordinary, &render_request).unwrap();
    assert_eq!(value["privacy"]["skill_content_included"], false);
}

#[test]
fn from_search_hit_uses_robot_match_type_spelling() {
    let hit = SearchHit {
        title: "session".to_string(),
        snippet: "fallback".to_string(),
        content: "fallback content".to_string(),
        content_hash: 42,
        conversation_id: Some(7),
        score: 3.5,
        source_path: "/s/fallback.jsonl".to_string(),
        agent: "codex".to_string(),
        workspace: "/work".to_string(),
        workspace_original: None,
        created_at: Some(1_000_000),
        line_number: Some(12),
        match_type: MatchType::ImplicitWildcard,
        source_id: "local".to_string(),
        origin_kind: "local".to_string(),
        origin_host: None,
    };

    let candidate = PackCandidate::from_search_hit(&hit, 1, 0);

    assert_eq!(candidate.match_type, "implicit_wildcard");
    assert_eq!(candidate.message_index, Some(12));
    assert_eq!(candidate.line_start, None);
    assert_eq!(candidate.line_end, None);
    assert!(!candidate.citation_verified);
}

#[test]
fn source_citations_require_a_unique_matching_record_and_preserve_archived_evidence() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("rollout-citation.jsonl");
    let raw = serde_json::json!({
        "type": "response_item",
        "payload": {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "verifiedneedle"}]
        }
    })
    .to_string();
    let source = format!("{{\"type\":\"session_meta\"}}\n\ninvalid json\n{raw}\n");
    let mut item = candidate("verify", "local", path.to_str().unwrap(), 10.0);
    item.created_at_ms = None;
    item.excerpt = "verifiedneedle".to_string();
    item.message_index = Some(1);
    let base = plan_answer_pack(request(vec![item])).unwrap();

    for (contents, expected_line) in [
        (source.clone(), Some(4)),
        (source.replace('\n', "\r\n"), Some(4)),
        (format!("{raw}\n{raw}\n"), None),
        (source.replace("verifiedneedle", "changed source"), None),
        (String::new(), None),
    ] {
        std::fs::write(&path, &contents).unwrap();
        let mut plan = base.clone();
        verify_pack_source_citations(
            &mut plan,
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        );
        assert_eq!(plan.evidence.len(), 1);
        assert_eq!(plan.evidence[0].excerpt, "verifiedneedle");
        let citation = &plan.evidence[0].candidate;
        assert_eq!(citation.line_start, expected_line);
        assert_eq!(citation.line_end, expected_line);
        assert_eq!(citation.citation_verified, expected_line.is_some());
        assert_eq!(citation.message_index, Some(1));
        if expected_line.is_some() {
            assert_eq!(
                citation.span_hash,
                blake3::hash(raw.as_bytes()).to_hex().to_string()
            );
        }
        assert_eq!(std::fs::read_to_string(&path).unwrap(), contents);
    }

    std::fs::write(&path, source).unwrap();
    let future = std::time::Instant::now() + std::time::Duration::from_secs(1);
    for (agent, origin, deadline) in [
        ("claude_code", "local", future),
        ("codex", "remote", future),
        ("codex", "local", std::time::Instant::now()),
    ] {
        let mut plan = base.clone();
        plan.evidence[0].candidate.agent = agent.to_string();
        plan.evidence[0].candidate.origin_kind = origin.to_string();
        verify_pack_source_citations(&mut plan, deadline);
        assert!(!plan.evidence[0].candidate.citation_verified);
        assert_eq!(plan.evidence[0].candidate.line_start, None);
        assert_eq!(plan.evidence[0].excerpt, "verifiedneedle");
    }
    std::fs::rename(&path, temp.path().join("retained-source.jsonl")).unwrap();
    let mut missing = base.clone();
    verify_pack_source_citations(
        &mut missing,
        std::time::Instant::now() + std::time::Duration::from_secs(1),
    );
    assert!(!missing.evidence[0].candidate.citation_verified);
    assert_eq!(missing.evidence[0].candidate.line_start, None);
    assert_eq!(missing.evidence[0].excerpt, "verifiedneedle");

    std::fs::File::create_new(&path)
        .unwrap()
        .set_len(8 * 1024 * 1024 + 1)
        .unwrap();
    let mut oversized = base;
    verify_pack_source_citations(
        &mut oversized,
        std::time::Instant::now() + std::time::Duration::from_secs(1),
    );
    assert!(!oversized.evidence[0].candidate.citation_verified);
    assert_eq!(oversized.evidence[0].candidate.line_start, None);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 8 * 1024 * 1024 + 1);
}

#[test]
fn source_citations_normalize_only_message_boundary_whitespace() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("rollout-whitespace.jsonl");
    let excerpt = "\n\nverified needle \t\n";
    let record = |text: &str| {
        serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": text}]
            }
        })
        .to_string()
    };
    let mut item = candidate("whitespace", "local", path.to_str().unwrap(), 10.0);
    item.created_at_ms = None;
    item.excerpt = excerpt.to_string();
    let base = plan_answer_pack(request(vec![item])).unwrap();
    let raw = record(excerpt);
    for (source, verified) in [
        (raw.clone(), true),
        (record("verified needle"), true),
        (record("verified  needle"), false),
        (format!("{raw}\n{}", record("verified needle")), false),
    ] {
        std::fs::write(&path, &source).unwrap();
        let mut plan = base.clone();
        verify_pack_source_citations(
            &mut plan,
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        );
        let evidence = &plan.evidence[0];
        assert_eq!(evidence.excerpt, excerpt);
        assert_eq!(evidence.candidate.excerpt, excerpt);
        assert_eq!(evidence.candidate.citation_verified, verified);
        assert_eq!(evidence.candidate.line_start, verified.then_some(1));
        assert_eq!(evidence.candidate.line_end, verified.then_some(1));
        if verified {
            assert_eq!(
                evidence.candidate.span_hash,
                blake3::hash(source.as_bytes()).to_hex().to_string()
            );
        }
        assert_eq!(std::fs::read_to_string(&path).unwrap(), source);
    }
}

#[test]
fn source_citations_match_ingestion_redaction_without_ignoring_ambiguity() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("rollout-redacted.jsonl");
    let first = format!("verifiedneedle sk-{}", "A".repeat(40));
    let second = format!("verifiedneedle sk-{}", "B".repeat(40));
    let archived = "verifiedneedle [REDACTED]";
    let record = |text: &str| {
        serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": text}]
            }
        })
        .to_string()
    };
    let raw = record(&first);
    for (excerpt, source, timestamp, verified) in [
        (archived, raw.clone(), None, true),
        (first.as_str(), raw.clone(), None, true),
        (first.as_str(), record(&second), None, false),
        (archived, format!("{raw}\n{}", record(&second)), None, false),
        (archived, record("different content"), None, false),
        (archived, raw.clone(), Some(1), false),
    ] {
        std::fs::write(&path, &source).unwrap();
        let mut item = candidate("redacted", "local", path.to_str().unwrap(), 10.0);
        item.excerpt = excerpt.to_string();
        item.created_at_ms = timestamp;
        let mut plan = plan_answer_pack(request(vec![item])).unwrap();
        let prepared = plan.evidence[0].excerpt.clone();
        verify_pack_source_citations(
            &mut plan,
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        );
        let evidence = &plan.evidence[0];
        assert_eq!(evidence.candidate.citation_verified, verified);
        assert_eq!(evidence.candidate.line_start, verified.then_some(1));
        assert_eq!(evidence.candidate.line_end, verified.then_some(1));
        assert_eq!(evidence.candidate.excerpt, excerpt);
        assert_eq!(evidence.excerpt, prepared);
        assert_eq!(evidence.id, evidence_id(&evidence.candidate));
        if verified {
            assert_eq!(
                evidence.candidate.span_hash,
                blake3::hash(raw.as_bytes()).to_hex().to_string()
            );
        }
        assert_eq!(std::fs::read_to_string(&path).unwrap(), source);
    }
}

#[test]
fn evidence_ids_encode_the_full_digest_as_base32() {
    // Independent known answers from Python's base64.b32encode, with
    // padding removed because these IDs always carry a 32-byte digest.
    for (hash, expected) in [
        (
            [0u8; 32],
            "ev_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        ),
        (
            [255u8; 32],
            "ev_777777777777777777777777777777777777777777777777777Q",
        ),
        (
            std::array::from_fn(|index| index as u8),
            "ev_AAAQEAYEAUDAOCAJBIFQYDIOB4IBCEQTCQKRMFYYDENBWHA5DYPQ",
        ),
        (
            std::array::from_fn(|index| (31 - index) as u8),
            "ev_D4PB2HA3DIMRQFYWCUKBGEQRCAHQ4DIMBMFASCAHAYCQIAYCAEAA",
        ),
    ] {
        assert_eq!(encoded_evidence_id(&hash), expected);
    }
    let first = [0u8; 32];
    let mut last_bit_changed = first;
    last_bit_changed[31] = 1;
    assert_ne!(
        encoded_evidence_id(&first),
        encoded_evidence_id(&last_bit_changed),
        "IDs must distinguish hashes sharing their first 255 bits"
    );
}

#[test]
fn source_citation_ids_follow_verified_spans_and_unverified_exits() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("rollout-identity.jsonl");
    let raw = serde_json::json!({
        "type": "response_item",
        "payload": {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "identity needle"}]
        }
    })
    .to_string();
    let mut item = candidate("identity", "local", path.to_str().unwrap(), 10.0);
    item.created_at_ms = None;
    item.excerpt = "identity needle".to_string();
    let base = plan_answer_pack(request(vec![item])).unwrap();
    let mut verified_ids = Vec::new();
    for prefix in ["", "\n"] {
        let source = format!("{prefix}{raw}\n");
        std::fs::write(&path, &source).unwrap();
        let mut plan = base.clone();
        let expected_line = prefix.len() + 1;
        for _ in 0..2 {
            verify_pack_source_citations(
                &mut plan,
                std::time::Instant::now() + std::time::Duration::from_secs(1),
            );
            let evidence = &plan.evidence[0];
            assert!(evidence.candidate.citation_verified);
            assert_eq!(evidence.candidate.line_start, Some(expected_line));
            assert_eq!(evidence.id, evidence_id(&evidence.candidate));
            assert_ne!(evidence.id, base.evidence[0].id);
            assert_eq!(evidence.excerpt, base.evidence[0].excerpt);
            assert_eq!(std::fs::read_to_string(&path).unwrap(), source);
            verified_ids.push(evidence.id.clone());
        }

        let verified = plan.clone();
        for unavailable in [false, true] {
            let mut plan = verified.clone();
            if unavailable {
                plan.evidence[0].candidate.agent = "unsupported".to_string();
            }
            verify_pack_source_citations(&mut plan, std::time::Instant::now());
            let evidence = &plan.evidence[0];
            assert!(!evidence.candidate.citation_verified);
            assert_eq!(evidence.candidate.line_start, None);
            assert_eq!(evidence.id, evidence_id(&evidence.candidate));
            assert_ne!(evidence.id, verified.evidence[0].id);
            assert_eq!(evidence.excerpt, verified.evidence[0].excerpt);
        }
    }
    assert_eq!(verified_ids[0], verified_ids[1]);
    assert_eq!(verified_ids[2], verified_ids[3]);
    assert_ne!(verified_ids[0], verified_ids[2]);
}

#[cfg(unix)]
#[test]
fn source_citations_do_not_follow_symlinks_or_wait_for_fifo_writers() {
    let temp = tempfile::TempDir::new().unwrap();
    let source = temp.path().join("source.jsonl");
    std::fs::write(
        &source,
        r#"{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"verifiedneedle"}]}}"#,
    )
    .unwrap();
    let link = temp.path().join("link.jsonl");
    std::os::unix::fs::symlink(&source, &link).unwrap();
    let fifo = temp.path().join("fifo.jsonl");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    for path in [link, fifo] {
        let mut item = candidate("special", "local", path.to_str().unwrap(), 10.0);
        item.created_at_ms = None;
        item.excerpt = "verifiedneedle".to_string();
        let mut plan = plan_answer_pack(request(vec![item])).unwrap();
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        std::thread::spawn(move || {
            verify_pack_source_citations(
                &mut plan,
                std::time::Instant::now() + std::time::Duration::from_millis(100),
            );
            let _ = sender.send(plan);
        });
        let plan = receiver
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("citation verification must not wait for a FIFO writer");
        assert!(!plan.evidence[0].candidate.citation_verified);
        assert_eq!(plan.evidence[0].candidate.line_start, None);
        assert_eq!(plan.evidence[0].excerpt, "verifiedneedle");
    }
}

#[test]
fn render_pack_redacts_freeform_query_and_health_strings() {
    let home_path = "/home/alice/projects/private";
    let host = "alice-workstation.internal";
    let token = format!("sk-{}", "abcdefghijklmnopqrstuv");
    let plan = plan_answer_pack(request(vec![candidate(
        "health-redaction",
        "local",
        "/s/health.jsonl",
        10.0,
    )]))
    .unwrap();
    let mut req = render_request(PackRenderFormat::Json);
    req.query_text = format!("investigate {token} at {home_path}");
    req.normalized_query = req.query_text.clone();
    req.readiness.lexical_readiness = PackLexicalReadiness::Stale;
    req.readiness.index_generation = Some(format!("generation from {home_path}"));
    req.readiness.lock_state = Some(format!("writer lock held by {host}"));
    req.readiness.recommended_action = Some(format!("inspect {home_path} on {host}"));
    req.readiness.source_sync_gaps = vec![PackSourceSyncGap {
        source_id: host.to_string(),
        origin_kind: "ssh".to_string(),
        kind: PackSourceSyncGapKind::RemoteStale,
        lag_seconds: Some(60),
        last_synced_at_ms: Some(1_000_000),
        recommended_action: Some(format!("sync {home_path} from {host}")),
    }];
    req.effective = Some(serde_json::json!({
        "command": "pack",
        "query": req.query_text,
        "query_structure": format!("investigate AND {token}"),
        "db_path": format!("{home_path}/agent_search.db"),
        "filters": {"workspaces": [home_path]},
    }));

    let rendered = render_answer_pack(&plan, &req).unwrap();
    let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();

    for raw in [&token, home_path, host] {
        assert!(!rendered.contains(raw));
    }
    let effective = &value["_meta"]["effective"];
    assert_eq!(effective["command"], "pack");
    assert_eq!(effective["query"], value["query"]["text"]);
    assert_eq!(effective["db_path"], "[REDACTED_PATH]/agent_search.db");
    assert_eq!(
        effective["filters"]["workspaces"],
        serde_json::json!(["[REDACTED_PATH]/private"])
    );
    assert_eq!(
        value["query"]["text"],
        "investigate [REDACTED] at [REDACTED_PATH]/private"
    );
    assert_eq!(value["pack"]["title"], value["query"]["normalized"]);
    assert_eq!(
        value["health"]["source_sync_gaps"][0]["source_id"],
        REDACTED_SOURCE_MARKER
    );
    assert!(
        value["health"]["recommended_action"]
            .as_str()
            .unwrap()
            .contains("[REDACTED_PATH]/private")
    );
    assert!(
        value["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|warning| warning == "privacy_redactions_applied")
    );
    assert!(
        value["privacy"]["redaction_counts"]["private_path"]
            .as_u64()
            .unwrap()
            >= 6
    );
}

#[test]
fn render_redacted_empty_pack_reports_privacy_counts() {
    let mut redacted = candidate("redacted", "local", "/s/redacted.jsonl", 9.0);
    redacted.excerpt = " \n\t ".to_string();
    let plan = plan_answer_pack(request(vec![redacted])).unwrap();
    let req = render_request(PackRenderFormat::Json);

    let value = render_answer_pack_value(&plan, &req).unwrap();

    assert_eq!(value["privacy"]["redaction_applied"], true);
    assert_eq!(value["privacy"]["redaction_counts"]["redacted_to_empty"], 1);
    assert_eq!(value["omitted"]["items"][0]["reason"], "redacted_to_empty");
    assert_eq!(
        value["warnings"],
        serde_json::json!([
            "no_evidence_found",
            "semantic_fallback_lexical",
            "privacy_redactions_applied"
        ])
    );
}

#[test]
fn render_pack_redacts_api_keys_and_bearer_tokens_in_json_and_markdown() {
    let api_key = format!("sk-{}", "12345678901234567890");
    let bearer = "Bearer abcdefghijklmnopqrst";
    let mut secret = candidate("secret", "local", "/s/secret.jsonl", 10.0);
    secret.excerpt = format!("Use {api_key} and Authorization: {bearer}.");
    let plan = plan_answer_pack(request(vec![secret])).unwrap();
    let json_req = render_request(PackRenderFormat::Json);
    let markdown_req = render_request(PackRenderFormat::Markdown);

    let json_rendered = render_answer_pack(&plan, &json_req).unwrap();
    let markdown_rendered = render_answer_pack(&plan, &markdown_req).unwrap();
    let value: serde_json::Value = serde_json::from_str(&json_rendered).unwrap();

    assert!(!json_rendered.contains(&api_key));
    assert!(!json_rendered.contains(bearer));
    assert!(!markdown_rendered.contains(&api_key));
    assert!(!markdown_rendered.contains(bearer));
    assert!(json_rendered.contains(REDACTED_VALUE_MARKER));
    assert!(markdown_rendered.contains(REDACTED_VALUE_MARKER));
    assert_eq!(value["privacy"]["redaction_applied"], true);
    assert_eq!(value["privacy"]["redaction_counts"]["secret"], 1);
    assert_eq!(value["evidence"][0]["redactions"][0]["kind"], "secret");
    let emitted_excerpt = value["evidence"][0]["excerpt"].as_str().unwrap();
    assert_ne!(emitted_excerpt, plan.evidence[0].candidate.excerpt);
    assert_eq!(emitted_excerpt, plan.evidence[0].excerpt);
    let emitted_digest = <sha2::Sha256 as sha2::Digest>::digest(emitted_excerpt.as_bytes());
    assert_eq!(
        hex::decode(
            value["evidence"][0]["citation"]["excerpt_sha256"]
                .as_str()
                .unwrap()
        )
        .unwrap(),
        emitted_digest.to_vec(),
        "the citation hash must verify the bytes the consumer actually receives"
    );
}

#[test]
fn pack_redacts_credentials_before_excerpt_truncation() {
    let mut secret = candidate("cut-secret", "local", "/s/cut-secret.jsonl", 10.0);
    secret.excerpt = format!(
        "{} sk-12345678901234567890 trailing context",
        "x".repeat(68)
    );
    let mut request = request(vec![secret]);
    request.limits.max_excerpt_chars = 80;
    let plan = plan_answer_pack(request).unwrap();
    assert_eq!(plan.evidence.len(), 1);
    for format in [PackRenderFormat::Json, PackRenderFormat::Markdown] {
        let mut render_request = render_request(format);
        render_request.limits.max_excerpt_chars = 80;
        let output = render_answer_pack(&plan, &render_request).unwrap();
        assert!(!output.contains("sk-12345"), "partial credential leaked");
    }
    let excerpt = &plan.evidence[0].excerpt;
    assert!(!excerpt.contains("sk-"));
    assert!(excerpt.chars().count() <= 80);
    assert!(plan.evidence[0].excerpt_truncated);
    let value = render_answer_pack_value(&plan, &render_request(PackRenderFormat::Json)).unwrap();
    assert_eq!(value["privacy"]["redaction_counts"]["secret"], 1);
}

#[test]
fn pack_budgets_emitted_text_after_redaction_expansion_and_unicode() {
    for max_excerpt_chars in [80, 8_000] {
        let candidates = (0..8)
            .map(|index| {
                let mut item = candidate(
                    &format!("expanded-{index}"),
                    "local",
                    &format!("/s/expanded-{index}.jsonl"),
                    10.0,
                );
                item.excerpt = format!("{}界e\u{301}", "~/x ".repeat(100));
                item
            })
            .collect();
        let mut request = request(candidates);
        request.limits.max_tokens = 1_024;
        request.limits.max_excerpt_chars = max_excerpt_chars;
        let plan = plan_answer_pack(request).unwrap();
        assert!(!plan.evidence.is_empty());
        let value =
            render_answer_pack_value(&plan, &render_request(PackRenderFormat::Json)).unwrap();
        let mut emitted_total = 0;
        for item in value["evidence"].as_array().unwrap() {
            let excerpt = item["excerpt"].as_str().unwrap();
            assert!(!excerpt.contains("~/x"));
            assert!(excerpt.chars().count() <= max_excerpt_chars);
            let estimated = excerpt.chars().count().div_ceil(4);
            assert_eq!(item["estimated_tokens"], estimated);
            assert_eq!(item["selection"]["token_cost"], estimated);
            emitted_total += estimated;
        }
        assert_eq!(value["limits"]["estimated_tokens"], emitted_total);
        assert_eq!(plan.estimated_tokens, emitted_total);
        assert!(emitted_total <= plan.diagnostics.budget.evidence_tokens);
    }
}

#[test]
fn render_pack_redacts_home_directory_paths_in_evidence_and_omitted_output() {
    use pulldown_cmark::{Event, Parser};

    let source_path = "/home/alice/projects/private/session.jsonl";
    let duplicate_path = "/Users/alice/projects/private/duplicate.jsonl";
    let mut first = candidate("private-path", "local", source_path, 10.0);
    first.workspace = "/home/alice/projects/private".to_string();
    first.workspace_original = Some("/home/alice/projects/private".to_string());
    first.excerpt = format!("Open {source_path} before reading ~/notes/private.md");
    let mut duplicate = candidate("duplicate-private-path", "local", duplicate_path, 9.0);
    duplicate.candidate_id = format!("old-laptop:{duplicate_path}:10");
    duplicate.content_hash = first.content_hash.clone();
    let plan = plan_answer_pack(request(vec![first, duplicate])).unwrap();
    let json_req = render_request(PackRenderFormat::Json);
    let markdown_req = render_request(PackRenderFormat::Markdown);

    let json_rendered = render_answer_pack(&plan, &json_req).unwrap();
    let markdown_rendered = render_answer_pack(&plan, &markdown_req).unwrap();
    let value: serde_json::Value = serde_json::from_str(&json_rendered).unwrap();
    let markdown_text = Parser::new(&markdown_rendered)
        .filter_map(|event| match event {
            Event::Text(text) | Event::Code(text) => Some(text),
            _ => None,
        })
        .fold(String::new(), |mut output, text| {
            output.push_str(&text);
            output
        });

    for raw in ["/home/alice", "/Users/alice", "~/notes", "old-laptop"] {
        assert!(!json_rendered.contains(raw));
        assert!(!markdown_rendered.contains(raw));
        assert!(!markdown_text.contains(raw));
    }
    assert_eq!(
        value["evidence"][0]["citation"]["source_path"],
        "[REDACTED_PATH]/session.jsonl"
    );
    assert_eq!(
        value["omitted"]["items"][0]["source_path"],
        "[REDACTED_PATH]/duplicate.jsonl"
    );
    assert!(
        value["omitted"]["items"][0]["candidate_id"]
            .as_str()
            .unwrap()
            .starts_with("omitted_")
    );
    assert!(markdown_text.contains("[REDACTED_PATH]/session.jsonl"));
    assert!(markdown_text.contains("[REDACTED_PATH]/duplicate.jsonl"));
    assert_eq!(value["privacy"]["redaction_applied"], true);
    assert!(
        value["privacy"]["redaction_counts"]["private_path"]
            .as_u64()
            .unwrap()
            >= 2
    );
    assert_eq!(value["privacy"]["redaction_counts"]["candidate_id"], 1);
}

#[test]
fn render_pack_redacts_remote_host_details_from_citation_contract() {
    let mut remote = candidate(
        "remote-host",
        "alice-workstation.internal",
        "/s/remote.jsonl",
        10.0,
    );
    remote.origin_kind = "ssh".to_string();
    remote.origin_host = Some("alice@workstation.internal".to_string());
    let plan = plan_answer_pack(request(vec![remote])).unwrap();
    let req = render_request(PackRenderFormat::Json);

    let rendered = render_answer_pack(&plan, &req).unwrap();
    let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();

    assert!(!rendered.contains("alice-workstation.internal"));
    assert!(!rendered.contains("alice@workstation.internal"));
    assert_eq!(
        value["evidence"][0]["citation"]["source_id"],
        REDACTED_SOURCE_MARKER
    );
    assert_eq!(
        value["evidence"][0]["citation"]["origin_host"],
        REDACTED_REMOTE_HOST_MARKER
    );
    assert_eq!(value["privacy"]["redaction_counts"]["remote_host"], 2);
}

#[test]
fn render_pack_redacts_encrypted_payload_material() {
    let payload = "encrypted_payload_material=abcdef0123456789abcdef0123456789";
    let mut encrypted = candidate("encrypted", "local", "/s/chatgpt.jsonl", 10.0);
    encrypted.agent = "chatgpt".to_string();
    encrypted.excerpt = format!("Skipped encrypted ChatGPT block: {payload}.");
    let plan = plan_answer_pack(request(vec![encrypted])).unwrap();
    let json_req = render_request(PackRenderFormat::Json);
    let markdown_req = render_request(PackRenderFormat::Markdown);

    let json_rendered = render_answer_pack(&plan, &json_req).unwrap();
    let markdown_rendered = render_answer_pack(&plan, &markdown_req).unwrap();
    let value: serde_json::Value = serde_json::from_str(&json_rendered).unwrap();

    assert!(!json_rendered.contains(payload));
    assert!(!markdown_rendered.contains(payload));
    assert!(json_rendered.contains(REDACTED_ENCRYPTED_PAYLOAD_MARKER));
    assert!(markdown_rendered.contains(REDACTED_ENCRYPTED_PAYLOAD_MARKER));
    assert_eq!(value["privacy"]["redaction_counts"]["encrypted_payload"], 1);
    assert_eq!(
        value["evidence"][0]["redactions"][0]["replacement"],
        REDACTED_ENCRYPTED_PAYLOAD_MARKER
    );
}
