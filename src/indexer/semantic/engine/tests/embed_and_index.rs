use super::super::*;
use super::helpers::*;
use tempfile::tempdir;

#[test]
fn hnsw_maintenance_refuses_an_incompatible_vector_contract() -> Result<()> {
    let temp = tempdir()?;
    let indexer = SemanticIndexer::new("hash", None)?;
    for (name, embedder_id, revision, dimension) in [
        ("old-input", "fnv1a-384", "hash-fnv1a-modular-v1", 384),
        ("other-model", "minilm-384", HASH_VECTOR_SPACE_REVISION, 384),
        (
            "other-dimension",
            "fnv1a-384",
            HASH_VECTOR_SPACE_REVISION,
            16,
        ),
    ] {
        let path = temp.path().join(format!("{name}.fsvi"));
        let mut writer = FsVectorIndex::create_with_revision(
            &path,
            embedder_id,
            revision,
            dimension,
            FsQuantization::F16,
        )?;
        let mut vector = vec![0.0; dimension];
        vector[0] = 1.0;
        writer.write_record("example", &vector)?;
        writer.finish()?;
        let index = FsVectorIndex::open_read_only(&path)?;
        let error = indexer
            .build_hnsw_index(&index, temp.path(), None, None)
            .expect_err("foreign vector contracts must be refused before graph publication");
        assert!(
            error
                .to_string()
                .contains("incompatible semantic vector space")
        );
        assert!(!hnsw_index_path(temp.path(), indexer.embedder_id()).exists());
    }
    Ok(())
}

#[test]
fn exact_embedding_reuse_preserves_rows_across_batches_windows_and_invocations() -> Result<()> {
    let messages: Vec<_> = [
        "**repeat alpha**",
        "repeat alpha",
        "repeat beta",
        "repeat alpha",
        "",
        "caf\u{e9} evidence",
        "cafe\u{301} evidence",
        "repeat beta",
        "repeat alpha",
        "repeat beta",
        "caf\u{e9} evidence",
        "repeat alpha",
    ]
    .into_iter()
    .enumerate()
    .map(|(i, content)| EmbeddingInput {
        message_id: i as u64 + 1,
        created_at_ms: -(i as i64),
        agent_id: i as u32 + 10,
        workspace_id: i as u32 + 20,
        source_id: i as u32 + 30,
        role: (i % 3) as u8,
        chunk_idx: i as u8,
        content: content.to_owned(),
    })
    .collect();
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut indexer = SemanticIndexer::new("hash", None)?.with_batch_size(2)?;
    assert!(
        indexer.exact_reuse,
        "the concrete hash factory admits reuse"
    );
    indexer.embedder = Box::new(MeasuredHashEmbedder {
        inner: HashEmbedder::default(),
        calls: calls.clone(),
        fault: None,
    });
    let mut progress = Vec::new();
    let actual = indexer.embed_messages_with_progress(&messages, |done, total| {
        progress.push((done, total));
    })?;
    let first_calls = calls.lock().unwrap().clone();
    assert_eq!(
        first_calls.concat(),
        ["repeat alpha", "repeat beta", "caf\u{e9} evidence"]
    );
    assert!(
        first_calls
            .iter()
            .all(|batch| !batch.is_empty() && batch.len() <= 2)
    );
    assert_eq!(actual.len(), 11, "only the empty canonical row is omitted");
    assert_eq!(progress.last(), Some(&(messages.len(), messages.len())));
    assert!(progress.windows(2).all(|pair| pair[0].0 <= pair[1].0));

    let again = indexer.embed_messages(&messages)?;
    assert_embedding_outputs_bitwise_equal(&again, &actual);
    assert_eq!(calls.lock().unwrap().concat().len(), 6);

    // The same concrete producer without admission takes the original
    // per-row inference path, even though its ID still says fnv1a-384.
    indexer.exact_reuse = false;
    calls.lock().unwrap().clear();
    let incumbent = indexer.embed_messages(&messages)?;
    assert_eq!(calls.lock().unwrap().concat().len(), 11);
    assert_embedding_outputs_bitwise_equal(&actual, &incumbent);
    let tmp = tempdir()?;
    let index = indexer.build_and_save_index(actual, tmp.path())?;
    assert_eq!(
        index.record_count(),
        11,
        "reuse must not deduplicate messages"
    );
    Ok(())
}

#[test]
fn exact_embedding_reuse_checks_full_text_in_colliding_hash_buckets() -> Result<()> {
    let messages = [
        EmbeddingInput::new(1, "alpha distinctive evidence"),
        EmbeddingInput::new(2, "beta unrelated evidence"),
        EmbeddingInput::new(3, "alpha distinctive evidence"),
    ];
    let mut prepared = prepare_window(&messages, true);
    for item in &mut prepared {
        item.hash = [7; 32];
    }
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let embedder = MeasuredHashEmbedder {
        inner: HashEmbedder::default(),
        calls: calls.clone(),
        fault: None,
    };
    let mut outputs = Vec::new();
    let mut reuse = ExactEmbeddingReuse::default();
    for item in &prepared {
        flush_prepared_batch(
            std::slice::from_ref(item),
            &mut outputs,
            &ProgressBar::hidden(),
            &embedder,
            Some(&mut reuse),
        )?;
    }
    assert_eq!(calls.lock().unwrap().concat().len(), 2);
    assert_eq!(reuse.by_hash.len(), 1);
    assert_eq!(reuse.by_hash[&[7; 32]].len(), 2);
    for (output, item) in outputs.iter().zip(&prepared) {
        assert_eq!(output.message_id, item.msg.message_id);
        assert_eq!(
            output.embedding,
            embedder.inner.embed_sync(&item.canonical)?
        );
    }
    assert_ne!(outputs[0].embedding, outputs[1].embedding);
    assert_eq!(outputs[0].embedding, outputs[2].embedding);
    Ok(())
}

#[test]
fn exact_embedding_reuse_failed_batch_preserves_state_and_can_retry() -> Result<()> {
    let messages = [
        EmbeddingInput::new(1, "existing canonical evidence"),
        EmbeddingInput::new(2, "existing canonical evidence"),
        EmbeddingInput::new(3, "new beta evidence"),
        EmbeddingInput::new(4, "new gamma evidence"),
    ];
    let prepared = prepare_window(&messages, true);
    for fault in [
        EmbeddingFault::Inference,
        EmbeddingFault::Count,
        EmbeddingFault::Dimension,
        EmbeddingFault::NonFinite,
    ] {
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut embedder = MeasuredHashEmbedder {
            inner: HashEmbedder::default(),
            calls: calls.clone(),
            fault: None,
        };
        let mut outputs = Vec::new();
        let mut reuse = ExactEmbeddingReuse::default();
        let progress = ProgressBar::hidden();
        flush_prepared_batch(
            &prepared[..1],
            &mut outputs,
            &progress,
            &embedder,
            Some(&mut reuse),
        )?;
        let prior_outputs = outputs.clone();
        let prior_cache = reuse.by_hash.clone();
        let prior_progress = progress.position();
        embedder.fault = Some(fault);
        let error = flush_prepared_batch(
            &prepared[1..],
            &mut outputs,
            &progress,
            &embedder,
            Some(&mut reuse),
        )
        .expect_err("an invalid producer batch must fail before output or cache mutation");
        let expected_error = match fault {
            EmbeddingFault::Inference => "injected inference failure",
            EmbeddingFault::Count => "embedder returned 1 embeddings for 2 inputs",
            EmbeddingFault::Dimension => "embedding dimension mismatch",
            EmbeddingFault::NonFinite => "contains a non-finite value",
        };
        assert!(error.to_string().contains(expected_error), "{error:#}");
        assert_embedding_outputs_bitwise_equal(&outputs, &prior_outputs);
        assert_eq!(reuse.by_hash, prior_cache, "{fault:?}");
        assert_eq!(progress.position(), prior_progress, "{fault:?}");

        embedder.fault = None;
        flush_prepared_batch(
            &prepared[1..],
            &mut outputs,
            &progress,
            &embedder,
            Some(&mut reuse),
        )?;
        assert_eq!(outputs.len(), 4);
        assert_eq!(progress.position(), 4);
        assert_eq!(reuse.by_hash.len(), 3);
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            &[
                vec!["existing canonical evidence".to_owned()],
                vec![
                    "new beta evidence".to_owned(),
                    "new gamma evidence".to_owned()
                ],
                vec![
                    "new beta evidence".to_owned(),
                    "new gamma evidence".to_owned()
                ],
            ]
        );
        for (output, input) in outputs.iter().zip(&prepared) {
            assert_eq!(output.message_id, input.msg.message_id);
            assert_eq!(
                output.embedding,
                embedder.inner.embed_sync(&input.canonical)?
            );
        }
    }
    Ok(())
}

#[test]
#[ignore = "requires an attested native MiniLM bundle in CASS_NATIVE_REUSE_MODEL_DIR; never downloads"]
fn exact_embedding_reuse_native_model_preserves_batch_composition_bits() -> Result<()> {
    let model_dir = PathBuf::from(
        dotenvy::var("CASS_NATIVE_REUSE_MODEL_DIR")
            .context("set CASS_NATIVE_REUSE_MODEL_DIR to an existing attested model bundle")?,
    );
    assert!(
        crate::search::fastembed_embedder::model_dir_override().is_none(),
        "the factory proof must use its supplied managed model directory"
    );
    let data_dir = tempfile::tempdir()?;
    let managed_model_dir = FastEmbedder::default_model_dir(data_dir.path());
    fs::create_dir_all(&managed_model_dir)?;
    let manifest = crate::search::model_download::ModelManifest::minilm_v2();
    for file in &manifest.files {
        let source = crate::search::model_download::model_file_path(&model_dir, file)
            .with_context(|| format!("missing supplied model file: {}", file.name))?;
        assert_eq!(
            fs::copy(source, managed_model_dir.join(file.local_name()))?,
            file.size
        );
    }
    let mut indexer = SemanticIndexer::new("minilm", Some(data_dir.path()))?.with_batch_size(1)?;
    assert!(
        indexer.exact_reuse,
        "the real English MiniLM factory admits exact reuse"
    );
    indexer.exact_reuse = false;
    assert!(indexer.embedder.is_semantic());
    assert_eq!(indexer.embedder_dimension(), 384);
    let short = "The database transaction recovered from its durable checkpoint.";
    let long = "A compiler investigation compares generated machine instructions. ".repeat(28);
    let unicode = "Unicode normalization preserves caf\u{e9} and \u{6771}\u{4eac} evidence.";
    let messages: Vec<_> = [
        short,
        long.as_str(),
        short,
        unicode,
        long.as_str(),
        short,
        unicode,
        "A separate source message retains its own workspace provenance.",
        short,
    ]
    .into_iter()
    .enumerate()
    .map(|(i, content)| EmbeddingInput {
        message_id: i as u64 + 1,
        created_at_ms: i as i64,
        agent_id: i as u32,
        workspace_id: i as u32 + 1,
        source_id: i as u32 + 2,
        role: ROLE_USER,
        chunk_idx: 0,
        content: content.to_owned(),
    })
    .collect();
    let singleton_incumbent = indexer.embed_messages(&messages)?;
    assert_eq!(singleton_incumbent.len(), messages.len());
    for batch_size in [1, 2, 8] {
        indexer.batch_size = batch_size;
        indexer.exact_reuse = false;
        let incumbent = indexer.embed_messages(&messages)?;
        assert_embedding_outputs_bitwise_equal(&incumbent, &singleton_incumbent);
        indexer.exact_reuse = true;
        let reused = indexer.embed_messages(&messages)?;
        assert_embedding_outputs_bitwise_equal(&reused, &incumbent);
    }
    Ok(())
}

#[test]
fn multilingual_model_has_a_distinct_registered_vector_space_revision() {
    let baseline = expected_vector_space_revision("minilm-384").expect("MiniLM revision");
    let multilingual = expected_vector_space_revision("multilingual-minilm-384")
        .expect("multilingual MiniLM revision");
    assert_ne!(baseline, multilingual);
    assert!(
        multilingual.contains("59160d9e43d396d05b4139c99f9feb7922da14868587fca7e33d379821a41405")
    );
}

/// cass #309: length-aware embed batching must cap both row count and
/// `row_count × max_canonical_len` per batch, never drop or reorder rows,
/// and keep a single over-budget message as its own one-row batch.
#[test]
fn length_aware_batches_bounds_count_and_chars() {
    fn rows<'a>(inputs: &'a [EmbeddingInput], lens: &[usize]) -> Vec<Prepared<'a>> {
        lens.iter()
            .enumerate()
            .map(|(i, &len)| Prepared {
                msg: &inputs[i],
                canonical: "a".repeat(len),
                hash: [0u8; 32],
            })
            .collect()
    }
    let inputs: Vec<EmbeddingInput> = (0..6).map(|i| EmbeddingInput::new(i as u64, "")).collect();

    // 1) Count cap only (budget disabled): fixed chunks of max_count.
    let r = rows(&inputs, &[10, 10, 10, 10, 10]);
    assert_eq!(
        length_aware_batches(&r, 2, 0)
            .iter()
            .map(|b| b.len())
            .collect::<Vec<_>>(),
        vec![2, 2, 1]
    );

    // 2) Char budget caps batches that hold longer rows (50 each, budget
    //    100 -> at most 2 rows/batch); order is preserved.
    let r = rows(&inputs, &[50, 50, 50, 50, 50]);
    let b = length_aware_batches(&r, 128, 100);
    assert!(b.iter().all(|s| s.len() <= 2));
    let flat: Vec<u64> = b
        .iter()
        .flat_map(|s| s.iter().map(|p| p.msg.message_id))
        .collect();
    assert_eq!(flat, vec![0, 1, 2, 3, 4]);

    // 3) A single over-budget row forms its own batch; nothing is dropped.
    let r = rows(&inputs, &[10, 500, 10]);
    let b = length_aware_batches(&r, 128, 100);
    assert_eq!(b.iter().map(|s| s.len()).sum::<usize>(), 3);
    assert!(
        b.iter()
            .any(|s| s.len() == 1 && s[0].canonical.len() == 500)
    );

    // 4) Empty input -> no batches.
    assert!(length_aware_batches(&[], 8, 100).is_empty());
}

#[test]
fn semantic_backfill_scheduler_waits_for_console_idle_unless_forced() {
    let policy = SemanticPolicy::compiled_defaults();
    let active = SemanticBackfillSchedulerSignals {
        user_active: true,
        ..default_scheduler_signals()
    };
    let decision = semantic_backfill_scheduler_decision_for_capacity(&policy, 64, &active, 100);
    assert!(!decision.should_run());
    assert_eq!(decision.state, SemanticBackfillSchedulerState::Paused);
    assert_eq!(decision.reason, SemanticBackfillSchedulerReason::UserActive);

    let forced = SemanticBackfillSchedulerSignals {
        user_active: true,
        force: true,
        ..default_scheduler_signals()
    };
    let decision = semantic_backfill_scheduler_decision_for_capacity(&policy, 64, &forced, 100);
    assert!(decision.should_run());
}

#[test]
fn semantic_backfill_scheduler_runs_and_scales_batch_under_idle_budget() {
    let policy = SemanticPolicy::compiled_defaults();
    let decision = semantic_backfill_scheduler_decision_for_capacity(
        &policy,
        64,
        &default_scheduler_signals(),
        80,
    );

    assert!(decision.should_run());
    assert_eq!(decision.state, SemanticBackfillSchedulerState::Running);
    assert_eq!(
        decision.reason,
        SemanticBackfillSchedulerReason::IdleBudgetAvailable
    );
    assert_eq!(decision.scheduled_batch_conversations, 51);
    assert_eq!(decision.current_capacity_pct, 80);
    assert_eq!(decision.next_eligible_after_ms, 0);
}

#[test]
fn semantic_backfill_scheduler_reason_next_steps_are_stable() {
    for (reason, expected) in [
        (
            SemanticBackfillSchedulerReason::IdleBudgetAvailable,
            "background semantic backfill is within idle budgets",
        ),
        (
            SemanticBackfillSchedulerReason::OperatorDisabled,
            "background semantic backfill is disabled by CASS_SEMANTIC_BACKFILL_DISABLE",
        ),
        (
            SemanticBackfillSchedulerReason::PolicyDisabled,
            "semantic policy disables background semantic backfill",
        ),
        (
            SemanticBackfillSchedulerReason::ForegroundPressure,
            "foreground pressure is present; retry after the idle delay",
        ),
        (
            SemanticBackfillSchedulerReason::LexicalRepairActive,
            "lexical repair is active; semantic backfill is yielding",
        ),
        (
            SemanticBackfillSchedulerReason::CapacityBelowFloor,
            "machine responsiveness capacity is below the semantic backfill floor",
        ),
        (
            SemanticBackfillSchedulerReason::ThreadBudgetZero,
            "semantic backfill thread budget is zero",
        ),
        (
            SemanticBackfillSchedulerReason::BatchBudgetZero,
            "semantic backfill batch budget is zero",
        ),
    ] {
        assert_eq!(reason.next_step(), expected, "{reason:?}");
    }
}

#[test]
fn semantic_backfill_scheduler_yields_to_foreground_and_lexical_pressure() {
    let policy = SemanticPolicy::compiled_defaults();
    let foreground = SemanticBackfillSchedulerSignals {
        foreground_pressure: true,
        ..default_scheduler_signals()
    };
    let foreground_decision =
        semantic_backfill_scheduler_decision_for_capacity(&policy, 64, &foreground, 100);
    assert!(!foreground_decision.should_run());
    assert_eq!(
        foreground_decision.state,
        SemanticBackfillSchedulerState::Paused
    );
    assert_eq!(
        foreground_decision.reason,
        SemanticBackfillSchedulerReason::ForegroundPressure
    );
    assert_eq!(
        foreground_decision.next_eligible_after_ms,
        policy.idle_delay_seconds * 1000
    );

    let lexical_repair = SemanticBackfillSchedulerSignals {
        lexical_repair_active: true,
        ..default_scheduler_signals()
    };
    let lexical_decision =
        semantic_backfill_scheduler_decision_for_capacity(&policy, 64, &lexical_repair, 100);
    assert!(!lexical_decision.should_run());
    assert_eq!(
        lexical_decision.state,
        SemanticBackfillSchedulerState::Paused
    );
    assert_eq!(
        lexical_decision.reason,
        SemanticBackfillSchedulerReason::LexicalRepairActive
    );
}

#[test]
fn semantic_backfill_scheduler_honors_policy_disable_and_force_override() {
    let mut policy = SemanticPolicy::compiled_defaults();
    policy.mode = crate::search::policy::SemanticMode::LexicalOnly;

    let disabled = semantic_backfill_scheduler_decision_for_capacity(
        &policy,
        64,
        &default_scheduler_signals(),
        100,
    );
    assert!(!disabled.should_run());
    assert_eq!(disabled.state, SemanticBackfillSchedulerState::Disabled);
    assert_eq!(
        disabled.reason,
        SemanticBackfillSchedulerReason::PolicyDisabled
    );

    let forced = SemanticBackfillSchedulerSignals {
        force: true,
        ..default_scheduler_signals()
    };
    let forced_decision =
        semantic_backfill_scheduler_decision_for_capacity(&policy, 64, &forced, 100);
    assert!(forced_decision.should_run());
    assert_eq!(
        forced_decision.reason,
        SemanticBackfillSchedulerReason::IdleBudgetAvailable
    );
    assert!(forced_decision.forced);
}

#[test]
fn test_batch_embedding() {
    let indexer = SemanticIndexer::new("hash", None).unwrap();
    let messages = vec![
        EmbeddingInput::new(1, "Hello world"),
        EmbeddingInput::new(2, "Goodbye world"),
    ];

    let embeddings = indexer.embed_messages(&messages).unwrap();

    assert_eq!(embeddings.len(), 2);
    assert_eq!(embeddings[0].message_id, 1);
    assert_eq!(embeddings[1].message_id, 2);
    assert_eq!(embeddings[0].embedding.len(), indexer.embedder_dimension());
}

#[test]
fn embedding_validation_rejects_non_finite_and_wrong_dimension() -> Result<()> {
    for (vector, expected_message) in [
        (vec![f32::NAN; 384], "non-finite"),
        (vec![f32::INFINITY; 384], "non-finite"),
        (vec![0.0; 383], "dimension mismatch"),
    ] {
        let Err(error) = validate_embedding_vector(&vector, 384, 7) else {
            bail!("invalid vector unexpectedly passed validation");
        };
        if !error.to_string().contains(expected_message) {
            bail!("unexpected validation error: {error:#}");
        }
    }
    Ok(())
}

#[test]
fn append_rejects_legacy_revision_and_invalid_new_vectors_without_mutation() -> Result<()> {
    let indexer = SemanticIndexer::new("hash", None)?;
    let tmp = tempdir()?;
    let index_path = vector_index_path(tmp.path(), indexer.embedder_id());
    let Some(index_parent) = index_path.parent() else {
        bail!("vector index path has no parent");
    };
    fs::create_dir_all(index_parent)?;

    let mut base_vectors = indexer.embed_messages(&[EmbeddingInput::new(1, "legacy vector")])?;
    let Some(base) = base_vectors.pop() else {
        bail!("embedding produced no vector");
    };
    let mut writer = FsVectorIndex::create_with_revision(
        &index_path,
        indexer.embedder_id(),
        "1.0",
        indexer.embedder_dimension(),
        FsQuantization::F16,
    )?;
    writer
        .write_record(&semantic_doc_id_for_embedded(&base), &base.embedding)
        .map_err(|error| anyhow::anyhow!(error))?;
    writer.finish().map_err(|error| anyhow::anyhow!(error))?;

    let candidate = indexer.embed_messages(&[EmbeddingInput::new(2, "current vector")])?;
    let Err(error) = indexer.append_to_index(candidate, tmp.path()) else {
        bail!("legacy revision was accepted");
    };
    if !error.to_string().contains("revision 1.0") {
        bail!("unexpected legacy-revision error: {error:#}");
    }
    let unchanged = FsVectorIndex::open(&index_path).map_err(|error| anyhow::anyhow!(error))?;
    if unchanged.record_count() != 1 || unchanged.wal_record_count() != 0 {
        bail!("legacy append mutated the index");
    }
    drop(unchanged);

    let mut current_writer = FsVectorIndex::create_with_revision(
        &index_path,
        indexer.embedder_id(),
        indexer.vector_space_revision()?,
        indexer.embedder_dimension(),
        FsQuantization::F16,
    )?;
    current_writer
        .write_record(&semantic_doc_id_for_embedded(&base), &base.embedding)
        .map_err(|error| anyhow::anyhow!(error))?;
    current_writer
        .finish()
        .map_err(|error| anyhow::anyhow!(error))?;

    let mut non_finite_vectors =
        indexer.embed_messages(&[EmbeddingInput::new(3, "invalid append")])?;
    let Some(mut non_finite) = non_finite_vectors.pop() else {
        bail!("embedding produced no vector");
    };
    non_finite.embedding.fill(f32::NAN);
    let Err(error) = indexer.append_to_index([non_finite], tmp.path()) else {
        bail!("non-finite append was accepted");
    };
    if !error.to_string().contains("non-finite") {
        bail!("unexpected non-finite error: {error:#}");
    }

    let mut wrong_dimension_vectors =
        indexer.embed_messages(&[EmbeddingInput::new(4, "wrong dimension")])?;
    let Some(mut wrong_dimension) = wrong_dimension_vectors.pop() else {
        bail!("embedding produced no vector");
    };
    wrong_dimension.embedding.pop();
    let Err(error) = indexer.append_to_index([wrong_dimension], tmp.path()) else {
        bail!("wrong-dimension append was accepted");
    };
    if !error.to_string().contains("dimension mismatch") {
        bail!("unexpected dimension error: {error:#}");
    }

    let unchanged = FsVectorIndex::open(&index_path).map_err(|error| anyhow::anyhow!(error))?;
    if unchanged.record_count() != 1 || unchanged.wal_record_count() != 0 {
        bail!("invalid append mutated the current index");
    }
    Ok(())
}

#[test]
fn issue_342_direct_semantic_progress_callbacks_reach_exact_totals() {
    let indexer = SemanticIndexer::new("hash", None).unwrap();
    let messages = vec![
        EmbeddingInput::new(1, "Hello world"),
        EmbeddingInput::new(2, "Goodbye world"),
    ];
    let mut embedding_progress = Vec::new();
    let embeddings = indexer
        .embed_messages_with_progress(&messages, |current, total| {
            embedding_progress.push((current, total));
        })
        .unwrap();
    assert_eq!(embedding_progress.last(), Some(&(2, 2)));

    let tmp = tempdir().unwrap();
    let mut vector_progress = Vec::new();
    let index = indexer
        .build_and_save_index_with_progress(embeddings, tmp.path(), |current| {
            vector_progress.push(current);
        })
        .unwrap();
    assert_eq!(vector_progress, vec![1, 2]);
    assert_eq!(index.record_count(), 2);
}

#[test]
fn test_progress_indicator() {
    let indexer = SemanticIndexer::new("hash", None).unwrap();
    let messages: Vec<_> = (0..1000)
        .map(|i| EmbeddingInput::new(i as u64, format!("Message {}", i)))
        .collect();

    let embeddings = indexer.embed_messages(&messages).unwrap();
    assert_eq!(embeddings.len(), messages.len());
}

#[test]
fn test_build_and_save_index() {
    let indexer = SemanticIndexer::new("hash", None).unwrap();
    let messages = vec![
        EmbeddingInput::new(1, "Hello world"),
        EmbeddingInput::new(2, "Goodbye world"),
    ];

    let embeddings = indexer.embed_messages(&messages).unwrap();
    let tmp = tempdir().unwrap();
    let index = indexer
        .build_and_save_index(embeddings, tmp.path())
        .unwrap();
    assert_eq!(index.embedder_id(), indexer.embedder_id());
    assert_eq!(index.dimension(), indexer.embedder_dimension());
    assert_eq!(index.record_count(), 2);
}

#[test]
fn semantic_reconciliation_rejects_invalid_replacement_without_mutating_live_fsvi_or_wal() {
    let indexer = SemanticIndexer::new("hash", None).unwrap();
    let tmp = tempdir().unwrap();
    let base_inputs: Vec<_> = (1..=20)
        .map(|message_id| {
            EmbeddingInput::new(
                message_id,
                format!("semantic reconciliation base message {message_id}"),
            )
        })
        .collect();
    let base_embeddings = indexer.embed_messages(&base_inputs).unwrap();
    drop(
        indexer
            .build_and_save_index(base_embeddings, tmp.path())
            .unwrap(),
    );

    let wal_input = EmbeddingInput::new(21, "semantic reconciliation pending WAL message");
    let wal_embeddings = indexer
        .embed_messages(std::slice::from_ref(&wal_input))
        .unwrap();
    assert_eq!(
        indexer.append_to_index(wal_embeddings, tmp.path()).unwrap(),
        1
    );

    let index_path = vector_index_path(tmp.path(), indexer.embedder_id());
    let wal_path = fsvi_wal_path_for(&index_path);
    let live = FsVectorIndex::open(&index_path).unwrap();
    assert_eq!(live.record_count(), 20);
    assert_eq!(
        live.wal_record_count(),
        1,
        "the fixture must exercise a real uncompacted WAL"
    );
    drop(live);
    let main_before = fs::read(&index_path).unwrap();
    let wal_before = fs::read(&wal_path).unwrap();

    let replacement_input = EmbeddingInput::new(22, "semantic reconciliation invalid replacement");
    let mut invalid_replacement = indexer
        .embed_messages(std::slice::from_ref(&replacement_input))
        .unwrap()
        .pop()
        .unwrap();
    invalid_replacement.embedding.pop();
    let current_doc_ids: HashSet<_> = base_inputs
        .iter()
        .chain([&wal_input, &replacement_input])
        .filter_map(semantic_doc_id_for_input)
        .collect();

    let err = indexer
        .reconcile_index_with_canonical_documents(
            vec![invalid_replacement],
            tmp.path(),
            TierKind::Fast,
            "content-v1:22:22:22",
            &current_doc_ids,
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("embedding dimension mismatch"),
        "unexpected reconciliation error: {err:#}"
    );
    assert_eq!(
        fs::read(&index_path).unwrap(),
        main_before,
        "invalid replacements must not rewrite the published FSVI"
    );
    assert_eq!(
        fs::read(&wal_path).unwrap(),
        wal_before,
        "invalid replacements must not compact or rewrite the published WAL"
    );
    let live_after = FsVectorIndex::open(&index_path).unwrap();
    assert_eq!(live_after.record_count(), 20);
    assert_eq!(live_after.wal_record_count(), 1);
}

#[test]
fn semantic_reconciliation_publishes_wal_entries_without_replaying_stale_sidecar() {
    let indexer = SemanticIndexer::new("hash", None).unwrap();
    let tmp = tempdir().unwrap();
    let base_inputs: Vec<_> = (1..=20)
        .map(|message_id| {
            EmbeddingInput::new(
                message_id,
                format!("semantic reconciliation base message {message_id}"),
            )
        })
        .collect();
    let base_embeddings = indexer.embed_messages(&base_inputs).unwrap();
    drop(
        indexer
            .build_and_save_index(base_embeddings, tmp.path())
            .unwrap(),
    );

    let wal_input = EmbeddingInput::new(21, "semantic reconciliation pending WAL message");
    let wal_embeddings = indexer
        .embed_messages(std::slice::from_ref(&wal_input))
        .unwrap();
    assert_eq!(
        indexer
            .append_to_index(wal_embeddings.clone(), tmp.path())
            .unwrap(),
        1
    );

    let index_path = vector_index_path(tmp.path(), indexer.embedder_id());
    let wal_path = fsvi_wal_path_for(&index_path);
    let before = FsVectorIndex::open(&index_path).unwrap();
    assert_eq!(before.metadata().compaction_gen, 1);
    assert_eq!(before.wal_record_count(), 1);
    assert!(wal_path.exists());
    drop(before);

    let current_doc_ids: HashSet<_> = base_inputs
        .iter()
        .chain([&wal_input])
        .filter_map(semantic_doc_id_for_input)
        .collect();
    let published = indexer
        .reconcile_index_with_canonical_documents(
            wal_embeddings,
            tmp.path(),
            TierKind::Fast,
            "content-v1:21:21:21",
            &current_doc_ids,
        )
        .unwrap();

    assert_eq!(published.record_count(), 21);
    assert_eq!(published.wal_record_count(), 0);
    assert_eq!(published.tombstone_count(), 0);
    assert!(
        !wal_path.exists(),
        "opening the published generation must discard the old live WAL"
    );
    let published_doc_ids: HashSet<_> = (0..published.record_count())
        .map(|record_index| published.doc_id_at(record_index).unwrap().to_owned())
        .collect();
    assert_eq!(published_doc_ids, current_doc_ids);
}

#[test]
fn semantic_reconciliation_replaces_duplicates_and_tombstones_in_private_candidate() {
    let indexer = SemanticIndexer::new("hash", None).unwrap();
    let tmp = tempdir().unwrap();
    let inputs = [
        EmbeddingInput::new(1, "semantic reconciliation duplicated message"),
        EmbeddingInput::new(2, "semantic reconciliation tombstoned message"),
    ];
    let embeddings = indexer.embed_messages(&inputs).unwrap();
    let doc_ids = inputs
        .iter()
        .filter_map(semantic_doc_id_for_input)
        .collect::<Vec<_>>();
    let index_path = vector_index_path(tmp.path(), indexer.embedder_id());
    fs::create_dir_all(index_path.parent().unwrap()).unwrap();
    let mut writer = FsVectorIndex::create_with_revision(
        &index_path,
        indexer.embedder_id(),
        HASH_VECTOR_SPACE_REVISION,
        indexer.embedder_dimension(),
        FsQuantization::F16,
    )
    .unwrap();
    writer
        .write_record(&doc_ids[0], &embeddings[0].embedding)
        .unwrap();
    writer
        .write_record(&doc_ids[0], &embeddings[0].embedding)
        .unwrap();
    writer
        .write_record(&doc_ids[1], &embeddings[1].embedding)
        .unwrap();
    writer.finish().unwrap();
    let mut live = FsVectorIndex::open(&index_path).unwrap();
    assert!(live.soft_delete(&doc_ids[1]).unwrap());
    assert_eq!(live.record_count(), 3);
    assert_eq!(live.tombstone_count(), 1);
    drop(live);

    let current_doc_ids = doc_ids.into_iter().collect::<HashSet<_>>();
    let published = indexer
        .reconcile_index_with_canonical_documents(
            embeddings,
            tmp.path(),
            TierKind::Fast,
            "content-v1:2:2:2",
            &current_doc_ids,
        )
        .unwrap();

    assert_eq!(published.record_count(), 2);
    assert_eq!(published.wal_record_count(), 0);
    assert_eq!(published.tombstone_count(), 0);
    let published_doc_ids = (0..published.record_count())
        .map(|record_index| published.doc_id_at(record_index).unwrap().to_owned())
        .collect::<HashSet<_>>();
    assert_eq!(published_doc_ids, current_doc_ids);
}

#[test]
fn sharded_index_build_writes_sidecar_without_runtime_publish() {
    let indexer = SemanticIndexer::new("hash", None).unwrap();
    let messages: Vec<_> = (0..5)
        .map(|idx| EmbeddingInput::new(idx, format!("semantic shard message {idx}")))
        .collect();
    let embeddings = indexer.embed_messages(&messages).unwrap();
    let tmp = tempdir().unwrap();

    let outcome = indexer
        .build_and_save_index_shards(
            embeddings,
            tmp.path(),
            SemanticShardBuildPlan {
                tier: TierKind::Fast,
                db_fingerprint: "db-fp-sharded-build".to_string(),
                model_revision: "hash".to_string(),
                total_conversations: 5,
                max_records_per_shard: 2,
                build_ann: false,
            },
        )
        .unwrap();

    assert_eq!(outcome.shard_count, 3);
    assert_eq!(outcome.doc_count, 5);
    assert_eq!(outcome.total_conversations, 5);
    assert!(outcome.complete);
    assert_eq!(outcome.index_paths.len(), 3);
    for path in &outcome.index_paths {
        let shard = FsVectorIndex::open(path).unwrap();
        assert_eq!(shard.embedder_id(), indexer.embedder_id());
        assert!(shard.record_count() > 0);
    }

    let shards = SemanticShardManifest::load(tmp.path()).unwrap().unwrap();
    let summary = shards.summary(TierKind::Fast, indexer.embedder_id(), "db-fp-sharded-build");
    assert!(summary.complete);
    assert_eq!(summary.ready_shards, 3);
    assert_eq!(summary.ann_ready_shards, 0);
    assert_eq!(summary.doc_count, 5);
    assert_eq!(summary.total_conversations, 5);

    assert!(
        SemanticManifest::load(tmp.path()).unwrap().is_none(),
        "sidecar shards must not publish the main runtime manifest"
    );
    assert!(!vector_index_path(tmp.path(), indexer.embedder_id()).exists());
}

#[test]
fn sharded_index_build_rejects_zero_sized_shards() {
    let indexer = SemanticIndexer::new("hash", None).unwrap();
    let err = indexer
        .build_and_save_index_shards(
            std::iter::empty(),
            tempdir().unwrap().path(),
            SemanticShardBuildPlan {
                tier: TierKind::Fast,
                db_fingerprint: "db-fp-sharded-build".to_string(),
                model_revision: "hash".to_string(),
                total_conversations: 0,
                max_records_per_shard: 0,
                build_ann: false,
            },
        )
        .unwrap_err();

    assert!(err.to_string().contains("max_records_per_shard > 0"));
}

#[test]
fn sharded_ann_build_records_per_shard_accelerators() {
    let indexer = SemanticIndexer::new("hash", None).unwrap();
    let messages: Vec<_> = (0..8)
        .map(|idx| EmbeddingInput::new(idx, format!("semantic ann shard message {idx}")))
        .collect();
    let embeddings = indexer.embed_messages(&messages).unwrap();
    let tmp = tempdir().unwrap();

    let outcome = indexer
        .build_and_save_index_shards(
            embeddings,
            tmp.path(),
            SemanticShardBuildPlan {
                tier: TierKind::Fast,
                db_fingerprint: "db-fp-sharded-ann-build".to_string(),
                model_revision: "hash".to_string(),
                total_conversations: 8,
                max_records_per_shard: 4,
                build_ann: true,
            },
        )
        .unwrap();

    assert_eq!(outcome.shard_count, 2);
    assert_eq!(outcome.ann_index_paths.len(), 2);
    for path in &outcome.ann_index_paths {
        assert!(path.exists(), "ANN shard missing at {}", path.display());
    }

    let shards = SemanticShardManifest::load(tmp.path()).unwrap().unwrap();
    let summary = shards.summary(
        TierKind::Fast,
        indexer.embedder_id(),
        "db-fp-sharded-ann-build",
    );
    assert!(summary.complete);
    assert_eq!(summary.ann_ready_shards, 2);
    assert!(summary.ann_size_bytes > 0);
    assert!(
        shards
            .shards
            .iter()
            .all(|record| record.ann_index_path.is_some() && record.ann_ready)
    );
}

/// Golden-output regression: any change to the embedding prep pipeline,
/// the canonicalizer, the hash embedder's deterministic projection, or
/// the ordering semantics of `embed_messages` must not silently mutate
/// the bytes we write to the vector index. This digest is derived from a
/// frozen 64-message corpus processed through the hash embedder; a
/// mismatch means one of those contracts moved.
#[test]
fn embed_messages_golden_digest_hash_embedder() {
    use ring::digest::{Context, SHA256};

    let corpus: Vec<EmbeddingInput> = (0..64)
        .map(|i| {
            let body = match i % 5 {
                0 => format!("plain text message number {i}"),
                1 => format!("**bold** line {i} with _emphasis_"),
                2 => format!("```rust\nfn f_{i}() {{ println!(\"{i}\"); }}\n```"),
                3 => format!("   whitespace {i}   "),
                _ => format!("unicode \u{00E9}\u{0301} + emoji \u{1F600} {i}"),
            };
            EmbeddingInput::new(i as u64, body)
        })
        .collect();

    let indexer = SemanticIndexer::new("hash", None)
        .unwrap()
        .with_batch_size(16)
        .unwrap();
    let embeddings = indexer.embed_messages(&corpus).unwrap();

    // Digest over (message_id, content_hash, embedding f32 bytes) for every
    // embedded message, in the order emitted. Preserves order + content +
    // numeric equality without having to compare raw floats directly.
    let mut ctx = Context::new(&SHA256);
    for em in &embeddings {
        ctx.update(&em.message_id.to_le_bytes());
        ctx.update(&em.content_hash);
        for v in &em.embedding {
            ctx.update(&v.to_le_bytes());
        }
    }
    let digest = hex::encode(ctx.finish().as_ref());

    // Captured 2026-04-21 against a freshly built hash embedder, batch
    // size 16, the frozen 64-message corpus above. Stable so long as
    // the prep pipeline, canonicalizer, and HashEmbedder::embed
    // implementation are all byte-preserving. If you intentionally
    // changed any of those, update this value AND record the reason
    // in the commit message.
    const EXPECTED: &str = "22d9ae7076925a4b70a194b0f519dfb1d465cc757368c296ef24055a02038c2c";
    assert_eq!(
        digest, EXPECTED,
        "embed_messages golden digest drifted; if this was intentional, \
         update EXPECTED in this test and record the reason in the commit message"
    );
}
