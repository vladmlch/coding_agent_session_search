use super::*;
use franken_agent_detection::{Origin, ScanRoot};
use serde_json::json;

fn fixture() -> Result<(tempfile::TempDir, ScanContext, Vec<PathBuf>)> {
    let root = tempfile::tempdir()?;
    let paths: Vec<_> = ["a", "b", "c"]
        .into_iter()
        .map(|name| root.path().join(format!("rollout-{name}.jsonl")))
        .collect();
    for path in &paths {
        std::fs::write(
            path,
            format!(
                "{}\n",
                json!({"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"source-budget-fixture"}]}})
            ),
        )?;
    }
    let ctx = ScanContext::with_roots(
        root.path().join("archive"),
        paths.iter().cloned().map(ScanRoot::local).collect(),
        None,
    );
    Ok((root, ctx, paths))
}

#[test]
fn late_size_rejection_continues_without_forwarding_completion() -> Result<()> {
    let (_root, ctx, paths) = fixture()?;
    let mut completed = Vec::new();
    let mut complete = |done: &SourceCompletion| {
        completed.push(done.source.source_path.clone());
        Ok(())
    };
    let mut hooks = SourceScanHooks {
        should_scan_source: None,
        on_source_complete: Some(&mut complete),
    };
    let mut delivered = Vec::new();
    let error = scan(
        &franken_agent_detection::CodexConnector::new(),
        &ctx,
        &mut hooks,
        &mut |conversation| {
            delivered.push(conversation.source_path);
            Ok(())
        },
        |conversation| {
            // A typed enrichment failure after the primary parser delivered B.
            // The file is unchanged, so FAD really calls its completion hook.
            if conversation.source_path == paths[1] {
                return Err(EnrichmentBudgetExceeded {
                    observed_bytes: MAX_AUGMENT_ROLLOUT_BYTES + 1,
                }
                .into());
            }
            super::super::augment_modern_codex_messages(conversation, None)
        },
    )
    .unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<IncompleteScan>()
            .unwrap()
            .rejected_source_count,
        1
    );
    assert_eq!(delivered, [paths[0].clone(), paths[2].clone()]);
    assert_eq!(completed, delivered);
    Ok(())
}

#[test]
fn actual_file_growth_at_enrichment_is_a_contained_size_failure() -> Result<()> {
    let (_root, ctx, paths) = fixture()?;
    let mut delivered = Vec::new();
    let error = scan(
        &franken_agent_detection::CodexConnector::new(),
        &ctx,
        &mut SourceScanHooks::default(),
        &mut |conversation| {
            delivered.push(conversation.source_path);
            Ok(())
        },
        |conversation| {
            if conversation.source_path == paths[1] {
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(&paths[1])?
                    .set_len(MAX_AUGMENT_ROLLOUT_BYTES + 1)?;
            }
            super::super::augment_modern_codex_messages(conversation, None)
        },
    )
    .unwrap_err();
    assert!(error.downcast_ref::<IncompleteScan>().is_some());
    assert_eq!(delivered, [paths[0].clone(), paths[2].clone()]);
    Ok(())
}

#[test]
fn generic_enrichment_failures_still_abort_at_the_failed_source() -> Result<()> {
    for kind in [
        std::io::ErrorKind::Interrupted,
        std::io::ErrorKind::InvalidData,
        std::io::ErrorKind::UnexpectedEof,
        std::io::ErrorKind::PermissionDenied,
    ] {
        let (_root, ctx, paths) = fixture()?;
        let mut delivered = Vec::new();
        let mut completed = Vec::new();
        let mut complete = |done: &SourceCompletion| {
            completed.push(done.source.source_path.clone());
            Ok(())
        };
        let mut hooks = SourceScanHooks {
            should_scan_source: None,
            on_source_complete: Some(&mut complete),
        };
        let error = scan(
            &franken_agent_detection::CodexConnector::new(),
            &ctx,
            &mut hooks,
            &mut |conversation| {
                delivered.push(conversation.source_path);
                Ok(())
            },
            |conversation| {
                if conversation.source_path == paths[1] {
                    return Err(std::io::Error::new(kind, "injected non-budget error").into());
                }
                Ok(())
            },
        )
        .unwrap_err();
        assert_eq!(error.downcast_ref::<std::io::Error>().unwrap().kind(), kind);
        assert_eq!(delivered, [paths[0].clone()]);
        assert_eq!(completed, delivered);
    }
    Ok(())
}

#[test]
fn sink_errors_even_of_the_budget_type_must_abort() -> Result<()> {
    let (_root, ctx, paths) = fixture()?;
    let mut called = Vec::new();
    let mut completed = 0;
    let mut complete = |_: &SourceCompletion| {
        completed += 1;
        Ok(())
    };
    let mut hooks = SourceScanHooks {
        should_scan_source: None,
        on_source_complete: Some(&mut complete),
    };
    let error = scan(
        &franken_agent_detection::CodexConnector::new(),
        &ctx,
        &mut hooks,
        &mut |conversation| {
            called.push(conversation.source_path);
            Err(EnrichmentBudgetExceeded {
                observed_bytes: MAX_AUGMENT_ROLLOUT_BYTES + 1,
            }
            .into())
        },
        |_| Ok(()),
    )
    .unwrap_err();
    assert!(error.downcast_ref::<EnrichmentBudgetExceeded>().is_some());
    assert_eq!(called, [paths[0].clone()]);
    assert_eq!(completed, 0);
    Ok(())
}

#[test]
fn completion_sink_failure_is_not_hidden() -> Result<()> {
    let (_root, ctx, paths) = fixture()?;
    let mut delivered = Vec::new();
    let mut complete = |_: &SourceCompletion| anyhow::bail!("completion sink failed");
    let mut hooks = SourceScanHooks {
        should_scan_source: None,
        on_source_complete: Some(&mut complete),
    };
    let error = scan(
        &franken_agent_detection::CodexConnector::new(),
        &ctx,
        &mut hooks,
        &mut |conversation| {
            delivered.push(conversation.source_path);
            Ok(())
        },
        |_| Ok(()),
    )
    .unwrap_err();
    assert!(error.to_string().contains("completion sink failed"));
    assert_eq!(delivered, [paths[0].clone()]);
    Ok(())
}

#[test]
fn rejection_reporting_is_bounded_and_escapes_unusual_paths() {
    let mut state = ScanState::default();
    for _ in 0..(MAX_REJECTION_SAMPLES + 8) {
        state.reject(
            Path::new("private\n\"rollout.jsonl"),
            MAX_AUGMENT_ROLLOUT_BYTES + 1,
        );
    }
    assert_eq!(
        state.incomplete.rejected_sources.len(),
        MAX_REJECTION_SAMPLES
    );
    assert_eq!(state.incomplete.omitted_source_count, 8);
    assert_eq!(
        state.incomplete.rejected_source_count,
        MAX_REJECTION_SAMPLES + 8
    );
    let text = state.incomplete.to_string();
    assert!(!text.contains('\n'));
    let report: serde_json::Value =
        serde_json::from_str(text.strip_prefix("Codex scan incomplete: ").unwrap()).unwrap();
    assert_eq!(report["reason"], "enrichment_read_budget_exceeded");
    assert_eq!(
        report["rejected_sources"][0]["source_path"],
        "private\n\"rollout.jsonl"
    );
}

#[test]
fn preflight_rechecks_current_size_and_only_primary_jsonl_sources() -> Result<()> {
    let (_root, _ctx, paths) = fixture()?;
    let mut source = DiscoveredSourceFile::new(
        "codex",
        &ScanRoot::local(paths[1].clone()),
        paths[1].clone(),
        DiscoveredSourceRole::PrimarySessionLog,
        true,
    )
    .with_fs_metadata();
    assert!(source.origin == Origin::local());
    source.size_bytes = Some(MAX_AUGMENT_ROLLOUT_BYTES + 1);
    assert_eq!(
        observed_over_limit(&source),
        None,
        "stale discovery size is not authority"
    );
    let file = std::fs::OpenOptions::new().write(true).open(&paths[1])?;
    file.set_len(MAX_AUGMENT_ROLLOUT_BYTES)?;
    assert_eq!(
        observed_over_limit(&source),
        None,
        "the exact cap is accepted"
    );
    file.set_len(MAX_AUGMENT_ROLLOUT_BYTES + 1)?;
    assert_eq!(
        observed_over_limit(&source),
        Some(MAX_AUGMENT_ROLLOUT_BYTES + 1)
    );
    source.role = DiscoveredSourceRole::MetadataSidecar;
    assert_eq!(observed_over_limit(&source), None);
    source.role = DiscoveredSourceRole::PrimarySessionLog;
    source.provider_slug = "other".into();
    assert_eq!(observed_over_limit(&source), None);
    Ok(())
}

/// A late enrichment budget refusal of ONE segment of a merged thread withdraws
/// the WHOLE thread: no fragment is delivered and no segment completion is
/// certified. The continuation grows past the enrichment cap after FAD's primary
/// parse read its opened prefix, during CASS enrichment (a concurrent writer).
#[test]
fn late_size_rejection_of_one_merged_segment_withdraws_the_whole_thread() -> Result<()> {
    use franken_agent_detection::DetectionResult;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    // Arms the growth only after the primary parse, so it lands in enrichment.
    struct ArmAfterPrimaryParse {
        armed: Arc<AtomicBool>,
    }
    impl Connector for ArmAfterPrimaryParse {
        fn detect(&self) -> DetectionResult {
            DetectionResult::not_found()
        }
        fn scan(&self, _ctx: &ScanContext) -> Result<Vec<NormalizedConversation>> {
            anyhow::bail!("this fault-injection adapter requires source boundaries")
        }
        fn supports_source_boundaries(&self) -> bool {
            true
        }
        fn scan_with_source_boundaries(
            &self,
            ctx: &ScanContext,
            hooks: &mut SourceScanHooks<'_>,
            on_conversation: &mut dyn FnMut(NormalizedConversation) -> Result<()>,
        ) -> Result<()> {
            let armed = self.armed.clone();
            let mut arm_then_forward = |conversation: NormalizedConversation| {
                armed.store(true, Ordering::SeqCst);
                on_conversation(conversation)
            };
            franken_agent_detection::CodexConnector::new().scan_with_source_boundaries(
                ctx,
                hooks,
                &mut arm_then_forward,
            )
        }
    }

    let dir = tempfile::tempdir()?;
    let sessions = dir.path().join("sessions");
    std::fs::create_dir_all(&sessions)?;
    let thread = "01a1d1b4-5529-7cd3-97db-d52326514bc2";
    let segment = "01a1d1ba-a4da-7b00-92b0-0368cfada0b2";
    let original = sessions.join(format!("rollout-2026-11-12T22-40-18-{thread}.jsonl"));
    let continuation = sessions.join(format!(
        "rollout-2026-11-12T22-47-12-{thread}_{segment}.jsonl"
    ));
    for (path, text, ts) in [
        (&original, "merged first question", "2026-11-12T21:40:20Z"),
        (&continuation, "merged second question", "2026-11-12T21:47:20Z"),
    ] {
        std::fs::write(
            path,
            format!(
                "{}\n{}\n",
                json!({"type":"session_meta","timestamp":"2026-11-12T21:40:18Z",
                    "payload":{"id":thread,"session_id":thread,"cwd":"/merged-workspace",
                        "history_mode":"paginated","originator":"codex-tui","source":"cli"}}),
                json!({"type":"response_item","timestamp":ts,
                    "payload":{"type":"message","role":"user",
                        "content":[{"type":"input_text","text":text}]}})
            ),
        )?;
    }

    let armed = Arc::new(AtomicBool::new(false));
    let armed_tick = armed.clone();
    let target = continuation.clone();
    let tick = move || {
        if armed_tick.load(Ordering::SeqCst) {
            let _ = std::fs::OpenOptions::new()
                .write(true)
                .open(&target)
                .and_then(|file| file.set_len(MAX_AUGMENT_ROLLOUT_BYTES + 1));
        }
    };
    let ctx = ScanContext::with_roots(
        dir.path().to_path_buf(),
        vec![ScanRoot::local(sessions.clone())],
        None,
    )
    .with_progress_tick(Arc::new(tick));

    let mut delivered = Vec::new();
    let mut completed = Vec::new();
    let mut complete = |done: &SourceCompletion| {
        completed.push(done.source.source_path.clone());
        Ok(())
    };
    let mut hooks = SourceScanHooks {
        should_scan_source: None,
        on_source_complete: Some(&mut complete),
    };
    let error = scan(
        &ArmAfterPrimaryParse {
            armed: armed.clone(),
        },
        &ctx,
        &mut hooks,
        &mut |conversation| {
            delivered.push(conversation.source_path);
            Ok(())
        },
        |conversation| super::super::augment_modern_codex_messages(conversation, None),
    )
    .unwrap_err();
    let incomplete = error.downcast_ref::<IncompleteScan>().unwrap();
    assert_eq!(incomplete.rejected_source_count, 1);
    assert_eq!(
        incomplete.rejected_sources[0].source_path,
        continuation.to_string_lossy(),
        "the refusal names the segment whose bytes were refused"
    );
    assert!(
        delivered.is_empty(),
        "a refused merged thread must never deliver a fragment: {delivered:?}"
    );
    assert!(
        completed.is_empty(),
        "a refused merged thread must never certify a segment completion: {completed:?}"
    );
    Ok(())
}
