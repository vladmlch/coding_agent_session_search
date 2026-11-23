use super::super::*;
use super::helpers::*;
use crate::model::types::{Agent, AgentKind};
use crate::storage::sqlite::FrankenStorage;
use tempfile::tempdir;

#[test]
fn gh458_backfill_reuses_vectors_across_ingest_and_prior_conversation_append() -> Result<()> {
    let temp = tempdir()?;
    let storage = FrankenStorage::open(&temp.path().join("agent_search.db"))?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    let mut first_conversation = test_conversation("first", "retained original turn");
    for conversation in [
        first_conversation.clone(),
        test_conversation("second", "second unprocessed conversation"),
        test_conversation("third", "third unprocessed conversation"),
    ] {
        storage.insert_conversation_tree(agent_id, None, &conversation)?;
    }
    let indexer = SemanticIndexer::new("hash", None)?;
    let mut manifest = SemanticManifest::default();
    let run = |manifest: &mut SemanticManifest| {
        indexer.run_backfill_from_storage_with_caps_and_sink(
            &storage,
            temp.path(),
            manifest,
            SemanticBackfillStoragePlan {
                tier: TierKind::Fast,
                db_fingerprint: crate::indexer::lexical_storage_fingerprint_for_storage(&storage)?,
                model_revision: "hash".into(),
                max_conversations: 1,
            },
            SemanticCheckpointCaps {
                max_messages: 1,
                max_bytes: 256,
            },
            &SemanticProgressSink::disabled(),
        )
    };
    let first = run(&mut manifest)?;
    assert!(first.checkpoint_saved);
    assert_eq!(first.embedded_docs, 1);
    let first_index = FsVectorIndex::open(&first.index_path)?;
    let retained_id = first_index.doc_id_at(0)?.to_owned();
    let retained_vector = first_index.vector_at_f32(0)?;
    drop(first_index);

    let mut appended = first_conversation.messages[0].clone();
    appended.idx = 1;
    appended.content = "new tail in already processed conversation".into();
    first_conversation.messages.push(appended);
    storage.insert_conversation_tree(agent_id, None, &first_conversation)?;
    storage.insert_conversation_tree(
        agent_id,
        None,
        &test_conversation("fourth", "newly ingested conversation"),
    )?;
    let second = run(&mut manifest)?;
    assert!(!second.published);
    assert_eq!(
        second.embedded_docs, 1,
        "covered prefix must not spend the cap"
    );
    assert_eq!(manifest.checkpoint.as_ref().unwrap().docs_embedded, 2);
    assert_ne!(first.index_path, second.index_path);
    let third = run(&mut manifest)?;
    let fourth = run(&mut manifest)?;
    let fifth = run(&mut manifest)?;
    assert_eq!(
        [
            third.embedded_docs,
            fourth.embedded_docs,
            fifth.embedded_docs
        ],
        [1, 1, 1]
    );
    assert!(!third.published && !fourth.published && fifth.published);
    let published = FsVectorIndex::open(&fifth.index_path)?;
    let expected: HashSet<String> = packet_embedding_inputs_from_storage(&storage)?
        .iter()
        .filter_map(semantic_doc_id_for_input)
        .collect();
    let mut actual = HashSet::new();
    for record in 0..published.record_count() {
        let id = published.doc_id_at(record)?;
        actual.insert(id.to_owned());
        if id == retained_id {
            assert_eq!(published.vector_at_f32(record)?, retained_vector);
        }
    }
    assert!(actual.contains(&retained_id));
    assert_eq!(actual, expected);
    assert_eq!(actual.len(), 5);
    drop(published);
    let unchanged = run(&mut manifest)?;
    assert!(unchanged.published);
    assert_eq!(unchanged.embedded_docs, 0);
    Ok(())
}

#[test]
fn gh458_contract_replacement_checkpoints_without_revoking_published_generation() -> Result<()> {
    let temp = tempdir()?;
    let storage = FrankenStorage::open(&temp.path().join("agent_search.db"))?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    for (name, text) in [
        ("first", "first retained-contract message"),
        ("second", "second retained-contract message"),
    ] {
        storage.insert_conversation_tree(agent_id, None, &test_conversation(name, text))?;
    }

    let indexer = SemanticIndexer::new("hash", None)?;
    let db_fingerprint = crate::indexer::lexical_storage_fingerprint_for_storage(&storage)?;
    let final_path = vector_index_path(temp.path(), indexer.embedder_id());
    fs::create_dir_all(
        final_path
            .parent()
            .context("semantic index has no parent")?,
    )?;
    let published_bytes = b"retained published generation sentinel";
    fs::write(&final_path, published_bytes)?;

    let mut manifest = SemanticManifest::default();
    manifest.publish_artifact(ArtifactRecord {
        tier: TierKind::Fast,
        embedder_id: indexer.embedder_id().to_owned(),
        model_revision: "hash".into(),
        schema_version: SEMANTIC_SCHEMA_VERSION,
        chunking_version: CHUNKING_STRATEGY_VERSION.wrapping_add(1),
        dimension: indexer.embedder_dimension(),
        doc_count: 2,
        conversation_count: 2,
        db_fingerprint: db_fingerprint.clone(),
        index_path: final_path
            .strip_prefix(temp.path())?
            .to_string_lossy()
            .into_owned(),
        size_bytes: u64::try_from(published_bytes.len())?,
        started_at_ms: 10,
        completed_at_ms: 20,
        ready: true,
    });
    manifest.publish_hnsw(crate::search::semantic_manifest::HnswRecord {
        base_tier: TierKind::Fast,
        embedder_id: indexer.embedder_id().to_owned(),
        ef_search: 64,
        index_path: "vector_index/retained-hnsw.chsw".into(),
        size_bytes: 17,
        built_at_ms: 30,
        ready: true,
    });
    manifest.save(temp.path())?;
    let published = manifest.fast_tier.clone();
    let ann = manifest.hnsw.clone();

    let run = |manifest: &mut SemanticManifest, max_conversations| {
        indexer.run_backfill_from_storage(
            &storage,
            temp.path(),
            manifest,
            SemanticBackfillStoragePlan {
                tier: TierKind::Fast,
                db_fingerprint: db_fingerprint.clone(),
                model_revision: "hash".into(),
                max_conversations,
            },
        )
    };

    let checkpointed = run(&mut manifest, 1)?;
    assert!(!checkpointed.published);
    assert!(checkpointed.checkpoint_saved);
    assert_eq!(manifest.fast_tier, published);
    assert_eq!(manifest.hnsw, ann);
    assert_eq!(fs::read(&final_path)?, published_bytes);
    let checkpoint = manifest
        .checkpoint
        .as_ref()
        .context("replacement checkpoint missing")?;
    assert_eq!(checkpoint.chunking_version, CHUNKING_STRATEGY_VERSION);

    let loaded = SemanticManifest::load(temp.path())?.context("saved manifest missing")?;
    assert_eq!(loaded.fast_tier, published);
    assert_eq!(loaded.hnsw, ann);
    assert!(loaded.checkpoint.is_some());

    let completed = run(&mut manifest, 16)?;
    assert!(completed.published);
    assert!(!completed.checkpoint_saved);
    assert!(manifest.checkpoint.is_none());
    let replacement = manifest
        .fast_tier
        .as_ref()
        .context("replacement not published")?;
    assert!(replacement.ready);
    assert_eq!(replacement.chunking_version, CHUNKING_STRATEGY_VERSION);
    assert_ne!(manifest.fast_tier, published);
    assert!(manifest.hnsw.as_ref().is_some_and(|record| !record.ready));
    assert_ne!(fs::read(&final_path)?, published_bytes);
    Ok(())
}

#[test]
fn gh458_backfill_reconciles_same_id_edits_deletions_and_filter_identity() -> Result<()> {
    let temp = tempdir()?;
    let storage = FrankenStorage::open(&temp.path().join("agent_search.db"))?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    for (name, text) in [
        ("edited", "old edited text"),
        ("deleted", "removed text"),
        ("identity", "same text with changed identity"),
        ("retained", "unchanged vector"),
    ] {
        storage.insert_conversation_tree(agent_id, None, &test_conversation(name, text))?;
    }
    let indexer = SemanticIndexer::new("hash", None)?;
    let mut manifest = SemanticManifest::default();
    let run = |manifest: &mut SemanticManifest, max_conversations| {
        indexer.run_backfill_from_storage_with_caps_and_sink(
            &storage,
            temp.path(),
            manifest,
            SemanticBackfillStoragePlan {
                tier: TierKind::Fast,
                db_fingerprint: crate::indexer::lexical_storage_fingerprint_for_storage(&storage)?,
                model_revision: "hash".into(),
                max_conversations,
            },
            SemanticCheckpointCaps {
                max_messages: 1,
                max_bytes: 256,
            },
            &SemanticProgressSink::disabled(),
        )
    };
    for _ in 0..4 {
        run(&mut manifest, 1)?;
    }
    assert!(manifest.checkpoint.is_none());
    let before_fingerprint = manifest.fast_tier.as_ref().unwrap().db_fingerprint.clone();
    let before_ids: HashSet<String> = packet_embedding_inputs_from_storage(&storage)?
        .iter()
        .filter_map(semantic_doc_id_for_input)
        .collect();
    let other_agent = storage.ensure_agent(&Agent {
        id: None,
        slug: "claude_code".into(),
        name: "Claude Code".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    storage.raw().execute(
        "UPDATE messages SET content = 'replacement text at same rowid' WHERE conversation_id = 1",
    )?;
    storage
        .raw()
        .execute("DELETE FROM messages WHERE conversation_id = 2")?;
    storage.raw().execute_compat(
        "UPDATE conversations SET agent_id = ?1 WHERE id = 3",
        &[ParamValue::from(other_agent)],
    )?;
    assert_eq!(
        crate::indexer::lexical_storage_fingerprint_for_storage(&storage)?,
        before_fingerprint,
        "the negative must evade count/max-ID fingerprint detection"
    );
    let changed = run(&mut manifest, 1)?;
    assert!(!changed.published);
    assert_eq!(changed.embedded_docs, 1);
    let done = run(&mut manifest, 1)?;
    assert!(done.published);
    assert_eq!(done.embedded_docs, 1);
    let mut expected = HashSet::new();
    // The deleted conversation deliberately retains stale append metadata.
    for id in [1, 3, 4] {
        let (inputs, _) =
            packet_embedding_inputs_from_selected_canonical_messages(&storage, &[id], |_| true)?;
        expected.extend(inputs.iter().filter_map(semantic_doc_id_for_input));
    }
    let published = FsVectorIndex::open(&done.index_path)?;
    let actual: HashSet<String> = (0..published.record_count())
        .map(|record| published.doc_id_at(record).map(str::to_owned))
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(actual, expected);
    assert_eq!(actual.len(), 3);
    assert_eq!(
        actual.intersection(&before_ids).count(),
        1,
        "only the unchanged content and filter identity may survive"
    );
    Ok(())
}

#[test]
fn gh458_reconcile_revokes_live_and_shard_readiness_until_complete() -> Result<()> {
    use crate::search::asset_state::{SemanticPreference, semantic_state_from_availability};
    use crate::search::model_manager::SemanticAvailability;

    let temp = tempdir()?;
    let storage = FrankenStorage::open(&temp.path().join("agent_search.db"))?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    for name in ["first", "second", "third", "retained"] {
        storage.insert_conversation_tree(agent_id, None, &test_conversation(name, name))?;
    }
    let fingerprint = crate::indexer::lexical_storage_fingerprint_for_storage(&storage)?;
    let indexer = SemanticIndexer::new("hash", None)?;
    let mut manifest = SemanticManifest::default();
    let mut plan = SemanticBackfillStoragePlan {
        tier: TierKind::Fast,
        db_fingerprint: fingerprint.clone(),
        model_revision: "hash".into(),
        max_conversations: 4,
    };
    let initial =
        indexer.run_backfill_from_storage(&storage, temp.path(), &mut manifest, plan.clone())?;
    assert!(initial.published);
    indexer.build_and_save_index_shards(
        indexer.embed_messages(&packet_embedding_inputs_from_storage(&storage)?)?,
        temp.path(),
        SemanticShardBuildPlan {
            tier: TierKind::Fast,
            db_fingerprint: fingerprint.clone(),
            model_revision: "hash".into(),
            total_conversations: 4,
            max_records_per_shard: 4,
            build_ann: false,
        },
    )?;
    let state = || {
        semantic_state_from_availability(
            temp.path(),
            &SemanticAvailability::HashFallback,
            SemanticPreference::HashFallback,
            Some(&fingerprint),
        )
    };
    assert!(state().can_search);
    // Clearing only the monolithic flag is insufficient: the complete
    // shard generation really can promote readiness on this fixture.
    manifest.fast_tier.as_mut().unwrap().ready = false;
    manifest.save(temp.path())?;
    assert!(state().can_search);
    assert!(
        indexer
            .reusable_backfill_candidate(temp.path(), &manifest, &plan)
            .is_some(),
        "unready vectors remain useful construction inputs after a crash"
    );
    manifest.fast_tier.as_mut().unwrap().ready = true;
    manifest.save(temp.path())?;
    let live_before = fs::read(&initial.index_path)?;
    storage.raw().execute(
        "UPDATE messages SET content = 'changed content under existing rowid' WHERE conversation_id <= 3",
    )?;
    assert_eq!(
        crate::indexer::lexical_storage_fingerprint_for_storage(&storage)?,
        fingerprint
    );
    plan.max_conversations = 1;
    for _ in 0..2 {
        let partial = indexer.run_backfill_from_storage(
            &storage,
            temp.path(),
            &mut manifest,
            plan.clone(),
        )?;
        assert!(!partial.published);
        assert_eq!(partial.embedded_docs, 1);
        assert_eq!(fs::read(&initial.index_path)?, live_before);
        let observed = state();
        assert_eq!(observed.fast_tier.current_db_matches, Some(true));
        assert!(!observed.fast_tier.ready);
        assert!(
            !observed.can_search,
            "matching fingerprints cannot certify stale live IDs"
        );
        assert_eq!(observed.fallback_mode, Some("lexical"));
        assert!(
            SemanticShardManifest::load(temp.path())?
                .unwrap()
                .shards
                .iter()
                .all(|record| !record.ready && !record.ann_ready)
        );
    }
    let complete = indexer.run_backfill_from_storage(&storage, temp.path(), &mut manifest, plan)?;
    assert!(complete.published);
    assert_eq!(complete.embedded_docs, 1);
    assert!(state().can_search);
    Ok(())
}

#[test]
fn gh458_missing_checkpoint_vectors_restart_full_coverage() -> Result<()> {
    let temp = tempdir()?;
    let storage = FrankenStorage::open(&temp.path().join("agent_search.db"))?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    for name in ["first", "second", "third"] {
        storage.insert_conversation_tree(agent_id, None, &test_conversation(name, name))?;
    }
    let indexer = SemanticIndexer::new("hash", None)?;
    let mut manifest = SemanticManifest::default();
    let mut plan = SemanticBackfillStoragePlan {
        tier: TierKind::Fast,
        db_fingerprint: crate::indexer::lexical_storage_fingerprint_for_storage(&storage)?,
        model_revision: "hash".into(),
        max_conversations: 2,
    };
    let first =
        indexer.run_backfill_from_storage(&storage, temp.path(), &mut manifest, plan.clone())?;
    assert!(first.checkpoint_saved);
    assert_eq!(first.embedded_docs, 2);
    let parked = first.index_path.with_extension("parked.fsvi");
    fs::rename(&first.index_path, &parked)?;
    let parked_bytes = fs::read(&parked)?;
    plan.max_conversations = 1;
    let restarted =
        indexer.run_backfill_from_storage(&storage, temp.path(), &mut manifest, plan.clone())?;
    assert!(!restarted.published);
    assert_eq!(restarted.conversations_processed, 1);
    assert_eq!(manifest.checkpoint.as_ref().unwrap().docs_embedded, 1);
    let second =
        indexer.run_backfill_from_storage(&storage, temp.path(), &mut manifest, plan.clone())?;
    assert!(!second.published);
    let done = indexer.run_backfill_from_storage(&storage, temp.path(), &mut manifest, plan)?;
    assert!(done.published);
    let expected = packet_embedding_inputs_from_storage(&storage)?
        .iter()
        .filter_map(semantic_doc_id_for_input)
        .collect::<HashSet<_>>();
    assert_eq!(
        validated_semantic_index_ids(&FsVectorIndex::open(&done.index_path)?)?,
        expected
    );
    assert_eq!(fs::read(&parked)?, parked_bytes);
    Ok(())
}

#[test]
fn gh458_publication_retains_and_excludes_nonempty_destination_wal() -> Result<()> {
    let temp = tempdir()?;
    let storage = FrankenStorage::open(&temp.path().join("agent_search.db"))?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    for name in ["first", "second", "third"] {
        storage.insert_conversation_tree(agent_id, None, &test_conversation(name, name))?;
    }
    let indexer = SemanticIndexer::new("hash", None)?;
    let mut manifest = SemanticManifest::default();
    let mut plan = SemanticBackfillStoragePlan {
        tier: TierKind::Fast,
        db_fingerprint: crate::indexer::lexical_storage_fingerprint_for_storage(&storage)?,
        model_revision: "hash".into(),
        max_conversations: 2,
    };
    let partial =
        indexer.run_backfill_from_storage(&storage, temp.path(), &mut manifest, plan.clone())?;
    assert!(partial.checkpoint_saved);
    let live_path = vector_index_path(temp.path(), indexer.embedder_id());
    fs::copy(&partial.index_path, &live_path)?;
    let poison_input =
        EmbeddingInput::new(9999, "prior live WAL document absent from canonical DB");
    let poison_id = semantic_doc_id_for_input(&poison_input).unwrap();
    let poison = indexer.embed_messages(&[poison_input])?.pop().unwrap();
    let mut live = FsVectorIndex::open(&live_path)?;
    live.append_batch(&[(poison_id.clone(), poison.embedding)])?;
    assert_eq!(live.wal_record_count(), 1);
    drop(live);
    let destination_wal = fsvi_wal_path_for(&live_path);
    let wal_bytes = fs::read(&destination_wal)?;
    assert!(!wal_bytes.is_empty());
    plan.max_conversations = 1;
    let done = indexer.run_backfill_from_storage(&storage, temp.path(), &mut manifest, plan)?;
    assert!(done.published);
    let expected = packet_embedding_inputs_from_storage(&storage)?
        .iter()
        .filter_map(semantic_doc_id_for_input)
        .collect::<HashSet<_>>();
    let actual = validated_semantic_index_ids(&FsVectorIndex::open(&done.index_path)?)?;
    assert_eq!(actual, expected);
    assert!(!actual.contains(&poison_id));
    let mut retained = Vec::new();
    for entry in fs::read_dir(temp.path().join(VECTOR_INDEX_DIR))? {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(".quarantined-destination-wal-")
        {
            retained.push(fs::read(entry.path().join("destination.wal"))?);
        }
    }
    assert!(
        retained.contains(&wal_bytes),
        "prior destination WAL must survive byte-for-byte"
    );
    Ok(())
}

#[test]
fn gh458_reused_backfill_obeys_message_and_byte_caps() -> Result<()> {
    for caps in [
        SemanticCheckpointCaps {
            max_messages: 1,
            max_bytes: 0,
        },
        SemanticCheckpointCaps {
            max_messages: 0,
            max_bytes: 50,
        },
    ] {
        let temp = tempdir()?;
        let storage = FrankenStorage::open(&temp.path().join("agent_search.db"))?;
        let agent_id = storage.ensure_agent(&Agent {
            id: None,
            slug: "codex".into(),
            name: "Codex".into(),
            version: None,
            kind: AgentKind::Cli,
        })?;
        for name in ["first", "second", "third", "fourth"] {
            storage.insert_conversation_tree(
                agent_id,
                None,
                &test_conversation(name, "a canonical message longer than twenty five bytes"),
            )?;
        }
        let indexer = SemanticIndexer::new("hash", None)?;
        let mut manifest = SemanticManifest::default();
        let mut plan = SemanticBackfillStoragePlan {
            tier: TierKind::Fast,
            db_fingerprint: crate::indexer::lexical_storage_fingerprint_for_storage(&storage)?,
            model_revision: "hash".into(),
            max_conversations: 2,
        };
        let first = indexer.run_backfill_from_storage(
            &storage,
            temp.path(),
            &mut manifest,
            plan.clone(),
        )?;
        assert_eq!(first.embedded_docs, 2);
        plan.max_conversations = 8;
        let capped = indexer.run_backfill_from_storage_with_caps_and_sink(
            &storage,
            temp.path(),
            &mut manifest,
            plan.clone(),
            caps,
            &SemanticProgressSink::disabled(),
        )?;
        assert_eq!(capped.embedded_docs, 1);
        assert!(!capped.published);
        assert_eq!(manifest.checkpoint.as_ref().unwrap().docs_embedded, 3);
        let done = indexer.run_backfill_from_storage_with_caps_and_sink(
            &storage,
            temp.path(),
            &mut manifest,
            plan,
            caps,
            &SemanticProgressSink::disabled(),
        )?;
        assert_eq!(done.embedded_docs, 1);
        assert!(done.published);
        assert_eq!(FsVectorIndex::open(&done.index_path)?.record_count(), 4);
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn gh458_backfill_cache_binds_real_wal_and_rejects_pinned_read_snapshots() -> Result<()> {
    let temp = tempdir()?;
    let path = temp.path().join("archive.db");
    let alias = temp.path().join("alias.db");
    let writer = FrankenStorage::open(&path)?;
    writer
        .raw()
        .execute("CREATE TABLE cache_probe(id INTEGER PRIMARY KEY, value TEXT)")?;
    writer
        .raw()
        .execute("INSERT INTO cache_probe VALUES(1, 'old')")?;
    std::os::unix::fs::symlink(&path, &alias)?;
    let reader = FrankenStorage::open_readonly(&alias)?;
    let before = BackfillFilePair::archive(&reader).expect("fresh alias handle");
    assert_eq!(before.path, path.canonicalize()?);
    assert_eq!(
        before.wal,
        backfill_file_stamp(&crate::storage::sqlite::database_sidecar_path(
            &path, "-wal"
        ))
        .unwrap()
    );
    assert!(!crate::storage::sqlite::database_sidecar_path(&alias, "-wal").exists());
    reader.raw().execute("BEGIN")?;
    assert_eq!(
        reader
            .raw()
            .query("SELECT value FROM cache_probe WHERE id=1")?[0]
            .get_typed::<String>(0)?,
        "old"
    );
    writer
        .raw()
        .execute("UPDATE cache_probe SET value='new' WHERE id=1")?;
    assert_eq!(
        reader
            .raw()
            .query("SELECT value FROM cache_probe WHERE id=1")?[0]
            .get_typed::<String>(0)?,
        "old"
    );
    assert!(
        BackfillFilePair::archive(&reader).is_none(),
        "old snapshot must not authorize a cache under new file stamps"
    );
    reader.raw().execute("ROLLBACK")?;
    let after = BackfillFilePair::archive(&reader).expect("autocommit alias handle");
    assert_ne!(
        after, before,
        "the real WAL changed despite stable alias and rowid"
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn gh458_backfill_cache_rejects_replaced_archive_path() -> Result<()> {
    let temp = tempdir()?;
    let path = temp.path().join("archive.db");
    let replacement = temp.path().join("replacement.db");
    drop(FrankenStorage::open(&path)?);
    drop(FrankenStorage::open(&replacement)?);
    let reader = FrankenStorage::open_readonly(&path)?;
    assert!(BackfillFilePair::archive(&reader).is_some());
    fs::rename(&path, temp.path().join("retained-original.db"))?;
    fs::rename(&replacement, &path)?;
    assert!(
        BackfillFilePair::archive(&reader).is_none(),
        "a descriptor on the old inode must not certify the replacement"
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn gh458_unchanged_backfill_preserves_completed_generation_without_replay() -> Result<()> {
    let temp = tempdir()?;
    let storage = FrankenStorage::open(&temp.path().join("agent_search.db"))?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    for name in ["first", "second"] {
        storage.insert_conversation_tree(agent_id, None, &test_conversation(name, name))?;
    }
    let indexer = SemanticIndexer::new("hash", None)?;
    let mut manifest = SemanticManifest::default();
    let run = |manifest: &mut SemanticManifest| {
        indexer.run_backfill_from_storage(
            &storage,
            temp.path(),
            manifest,
            SemanticBackfillStoragePlan {
                tier: TierKind::Fast,
                db_fingerprint: crate::indexer::lexical_storage_fingerprint_for_storage(&storage)?,
                model_revision: "hash".into(),
                max_conversations: 2,
            },
        )
    };
    let first = run(&mut manifest)?;
    assert!(first.published && !first.unchanged);
    let fingerprint = manifest.fast_tier.as_ref().unwrap().db_fingerprint.clone();
    assert_eq!(
        indexer.completed_backfill_fingerprint(
            &storage,
            temp.path(),
            &manifest,
            TierKind::Fast,
            "hash"
        )?,
        Some(fingerprint.clone())
    );
    assert_eq!(
        indexer.completed_backfill_fingerprint(
            &storage,
            temp.path(),
            &manifest,
            TierKind::Fast,
            "foreign-model"
        )?,
        None
    );
    let vectors_before = fs::read(&first.index_path)?;
    let manifest_before = fs::read(SemanticManifest::path(temp.path()))?;
    let vector_stamp =
        BackfillFilePair::capture(&first.index_path, &fsvi_wal_path_for(&first.index_path));
    let replay = run(&mut manifest)?;
    assert!(replay.unchanged && replay.published);
    assert_eq!(replay.embedded_docs, 0);
    assert_eq!(replay.last_offset, first.last_offset);
    assert_eq!(
        fs::read(SemanticManifest::path(temp.path()))?,
        manifest_before
    );
    assert_eq!(fs::read(&first.index_path)?, vectors_before);
    assert_eq!(
        BackfillFilePair::capture(&first.index_path, &fsvi_wal_path_for(&first.index_path)),
        vector_stamp
    );

    // Same row count and tail fingerprint; an old row's content changed.
    storage.raw().execute(
        "UPDATE messages SET content = 'changed earlier content' WHERE conversation_id = 1",
    )?;
    assert_eq!(
        indexer.completed_backfill_fingerprint(
            &storage,
            temp.path(),
            &manifest,
            TierKind::Fast,
            "hash"
        )?,
        None
    );
    assert_eq!(
        crate::indexer::lexical_storage_fingerprint_for_storage(&storage)?,
        fingerprint
    );
    let changed = run(&mut manifest)?;
    assert!(!changed.unchanged);
    assert_eq!(changed.embedded_docs, 1);
    assert_ne!(fs::read(&changed.index_path)?, vectors_before);

    // Missing/corrupt cache is merely a full-reconciliation miss.
    let cache_path = temp
        .path()
        .join(VECTOR_INDEX_DIR)
        .join(".completed-backfill-fast-fnv1a-384.json");
    fs::write(&cache_path, b"incomplete cache")?;
    assert_eq!(
        indexer.completed_backfill_fingerprint(
            &storage,
            temp.path(),
            &manifest,
            TierKind::Fast,
            "hash"
        )?,
        None
    );
    let recovered = run(&mut manifest)?;
    assert!(!recovered.unchanged && recovered.published);
    assert_eq!(recovered.embedded_docs, 0);
    assert!(run(&mut manifest)?.unchanged);
    Ok(())
}

#[test]
fn gh470_prefix_checkpoint_restarts_complete_passage_coverage() -> Result<()> {
    let temp = tempdir()?;
    let storage = FrankenStorage::open(&temp.path().join("agent_search.db"))?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    let long = format!(
        "{} final antimeridian resolution",
        "deployment log ".repeat(400)
    );
    for (name, body) in [
        ("long", long.as_str()),
        ("short", "certificate renewal"),
        ("last", "database locks"),
    ] {
        storage.insert_conversation_tree(agent_id, None, &test_conversation(name, body))?;
    }
    let indexer = SemanticIndexer::new("hash", None)?;
    let mut manifest = SemanticManifest::default();
    let fingerprint = crate::indexer::lexical_storage_fingerprint_for_storage(&storage)?;
    let plan = |max_conversations| SemanticBackfillStoragePlan {
        tier: TierKind::Fast,
        db_fingerprint: fingerprint.clone(),
        model_revision: "hash".into(),
        max_conversations,
    };
    let initial =
        indexer.run_backfill_from_storage(&storage, temp.path(), &mut manifest, plan(3))?;
    assert!(initial.published);
    let current_inputs = packet_embedding_inputs_from_storage(&storage)?;
    let long_id = current_inputs[0].message_id;
    let expected = current_inputs
        .iter()
        .filter_map(semantic_doc_id_for_input)
        .collect::<HashSet<_>>();
    let mut legacy_inputs = Vec::new();
    for input in &current_inputs {
        if input.chunk_idx == 0 {
            let mut old = input.clone();
            if input.message_id == long_id {
                old.content = long.clone();
            }
            legacy_inputs.push(old);
        }
    }
    // Produce actual old-prefix vectors with the unchanged hash embedder.
    // The old header and cursor are historical input-contract metadata.
    let legacy_vectors = indexer.embed_messages(&legacy_inputs)?;
    let staging = semantic_staging_index_path(
        temp.path(),
        TierKind::Fast,
        indexer.embedder_id(),
        &fingerprint,
    );
    for (path, records) in [
        (&initial.index_path, legacy_vectors.as_slice()),
        (&staging, &legacy_vectors[..1]),
    ] {
        fs::create_dir_all(path.parent().context("index parent")?)?;
        let mut writer = FsVectorIndex::create_with_revision(
            path,
            indexer.embedder_id(),
            "hash-fnv1a-modular-v1",
            indexer.embedder_dimension(),
            FsQuantization::F16,
        )?;
        for record in records {
            writer.write_record(&semantic_doc_id_for_embedded(record), &record.embedding)?;
        }
        writer.finish()?;
    }
    let prior_live = fs::read(&initial.index_path)?;
    manifest
        .fast_tier
        .as_mut()
        .context("initial artifact")?
        .chunking_version = 1;
    manifest.checkpoint = Some(BuildCheckpoint {
        tier: TierKind::Fast,
        embedder_id: indexer.embedder_id().into(),
        last_offset: 1,
        docs_embedded: 1,
        conversations_processed: 1,
        total_conversations: 3,
        db_fingerprint: fingerprint.clone(),
        schema_version: SEMANTIC_SCHEMA_VERSION,
        chunking_version: 1,
        saved_at_ms: now_ms(),
        last_message_id: Some(i64::try_from(long_id)?),
        cursor_exhausted: false,
    });
    manifest.save(temp.path())?;
    // A chunking-contract replacement on an unchanged archive keeps the
    // complete old generation published until the replacement publishes
    // (5e96ebc6, GH #458); only that boundary retires it.
    let retained = manifest.fast_tier.clone();
    assert!(retained.as_ref().is_some_and(|artifact| artifact.ready));
    let first = indexer.run_backfill_from_storage(&storage, temp.path(), &mut manifest, plan(1))?;
    assert!(!first.unchanged && !first.published && first.checkpoint_saved);
    let checkpoint = manifest.checkpoint.as_ref().context("new checkpoint")?;
    assert_eq!(checkpoint.chunking_version, CHUNKING_STRATEGY_VERSION);
    assert_eq!(checkpoint.conversations_processed, 1);
    assert_eq!(checkpoint.last_message_id, Some(i64::try_from(long_id)?));
    assert_eq!(checkpoint.docs_embedded, 8);
    assert_eq!(manifest.fast_tier, retained);
    assert_eq!(fs::read(&initial.index_path)?, prior_live);
    let staged = FsVectorIndex::open(&staging)?;
    assert_eq!(staged.embedder_revision(), HASH_VECTOR_SPACE_REVISION);
    assert_eq!(staged.record_count(), 8);
    drop(staged);
    let mut resumed = SemanticManifest::load(temp.path())?.context("durable checkpoint")?;
    let second = indexer.run_backfill_from_storage(&storage, temp.path(), &mut resumed, plan(1))?;
    assert!(!second.published);
    assert_eq!(resumed.fast_tier, retained);
    let done = indexer.run_backfill_from_storage(&storage, temp.path(), &mut resumed, plan(1))?;
    assert!(done.published && !done.unchanged);
    let replacement = resumed.fast_tier.as_ref().context("replacement artifact")?;
    assert!(replacement.ready);
    assert_eq!(replacement.chunking_version, CHUNKING_STRATEGY_VERSION);
    let actual = FsVectorIndex::open(&done.index_path)?;
    assert_eq!(actual.embedder_revision(), HASH_VECTOR_SPACE_REVISION);
    assert_eq!(validated_semantic_index_ids(&actual)?, expected);
    assert_eq!(actual.record_count(), 10);
    assert_eq!(actual.tombstone_count(), 0);
    assert_eq!(actual.wal_record_count(), 0);
    Ok(())
}

#[test]
fn gh458_backfill_upgrades_legacy_hash_space_without_reusing_old_vectors() -> Result<()> {
    let temp = tempdir()?;
    let storage = FrankenStorage::open(&temp.path().join("agent_search.db"))?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    for name in ["first", "second"] {
        storage.insert_conversation_tree(agent_id, None, &test_conversation(name, name))?;
    }
    let indexer = SemanticIndexer::new("hash", None)?;
    let mut manifest = SemanticManifest::default();
    let fingerprint = crate::indexer::lexical_storage_fingerprint_for_storage(&storage)?;
    let run = |manifest: &mut SemanticManifest, max_conversations| {
        indexer.run_backfill_from_storage(
            &storage,
            temp.path(),
            manifest,
            SemanticBackfillStoragePlan {
                tier: TierKind::Fast,
                db_fingerprint: fingerprint.clone(),
                model_revision: "hash".into(),
                max_conversations,
            },
        )
    };
    let published = run(&mut manifest, 2)?;
    assert!(published.published);
    let index = FsVectorIndex::open(&published.index_path)?;
    let expected: Vec<_> = (0..index.record_count())
        .map(|row| Ok((index.doc_id_at(row)?.to_owned(), index.vector_at_f32(row)?)))
        .collect::<Result<_>>()?;
    drop(index);
    let mut legacy = FsVectorIndex::create_with_revision(
        &published.index_path,
        indexer.embedder_id(),
        "1.0",
        indexer.embedder_dimension(),
        FsQuantization::F16,
    )?;
    // Same canonical IDs with deliberately wrong old-space values catch
    // relabeling or reuse without recomputing the embeddings.
    let mut old_vector = vec![0.0; indexer.embedder_dimension()];
    old_vector[0] = 1.0;
    for (id, expected_vector) in &expected {
        assert_ne!(&old_vector, expected_vector);
        legacy.write_record(id, &old_vector)?;
    }
    legacy.finish()?;
    let prior_bytes = fs::read(&published.index_path)?;
    let first = run(&mut manifest, 1)?;
    assert_eq!(first.embedded_docs, 1);
    assert!(first.checkpoint_saved);
    assert!(!first.published);
    assert!(!manifest.fast_tier.as_ref().unwrap().ready);
    assert_eq!(fs::read(&published.index_path)?, prior_bytes);
    let second = run(&mut manifest, 1)?;
    assert_eq!(second.embedded_docs, 1);
    assert!(second.published);
    assert!(manifest.fast_tier.as_ref().unwrap().ready);
    let rebuilt = FsVectorIndex::open(&second.index_path)?;
    assert_eq!(rebuilt.embedder_revision(), HASH_VECTOR_SPACE_REVISION);
    assert_eq!(rebuilt.record_count(), expected.len());
    let actual: HashMap<_, _> = (0..rebuilt.record_count())
        .map(|row| {
            Ok((
                rebuilt.doc_id_at(row)?.to_owned(),
                rebuilt.vector_at_f32(row)?,
            ))
        })
        .collect::<Result<_>>()?;
    assert_eq!(actual, expected.into_iter().collect());
    Ok(())
}

#[test]
fn gh458_backfill_keeps_identity_generation_and_vector_space_fences() -> Result<()> {
    let temp = tempdir()?;
    let storage = FrankenStorage::open(&temp.path().join("agent_search.db"))?;
    let agent_id = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    for name in ["first", "second", "third"] {
        storage.insert_conversation_tree(agent_id, None, &test_conversation(name, name))?;
    }
    let indexer = SemanticIndexer::new("hash", None)?;
    let mut manifest = SemanticManifest::default();
    let fingerprint = crate::indexer::lexical_storage_fingerprint_for_storage(&storage)?;
    let first = indexer.run_backfill_from_storage(
        &storage,
        temp.path(),
        &mut manifest,
        SemanticBackfillStoragePlan {
            tier: TierKind::Fast,
            db_fingerprint: fingerprint.clone(),
            model_revision: "hash".into(),
            max_conversations: 2,
        },
    )?;
    assert_eq!(first.embedded_docs, 2);
    assert!(first.checkpoint_saved);
    let prior_bytes = fs::read(&first.index_path)?;
    let migrated_plan = SemanticBackfillStoragePlan {
        tier: TierKind::Fast,
        db_fingerprint: format!("identity-v1:changed-generation:{fingerprint}"),
        model_revision: "hash".into(),
        max_conversations: 1,
    };
    assert!(
        indexer
            .reusable_backfill_candidate(temp.path(), &manifest, &migrated_plan)
            .is_none()
    );
    let migrated = indexer.run_backfill_from_storage(
        &storage,
        temp.path(),
        &mut manifest,
        migrated_plan.clone(),
    )?;
    assert_eq!(migrated.embedded_docs, 1);
    assert_eq!(manifest.checkpoint.as_ref().unwrap().docs_embedded, 1);
    assert_eq!(fs::read(&first.index_path)?, prior_bytes);
    let manifest_before = fs::read(SemanticManifest::path(temp.path()))?;
    let index = FsVectorIndex::open(&migrated.index_path)?;
    let id = index.doc_id_at(0)?.to_owned();
    let vector = index.vector_at_f32(0)?;
    drop(index);
    // A real, readable FSVI with an incompatible vector space must not be
    // mistaken for reusable progress merely because the manifest matches.
    let mut writer = FsVectorIndex::create_with_revision(
        &migrated.index_path,
        indexer.embedder_id(),
        "incompatible-model-revision",
        indexer.embedder_dimension(),
        FsQuantization::F16,
    )?;
    writer.write_record(&id, &vector)?;
    writer.finish()?;
    let incompatible_bytes = fs::read(&migrated.index_path)?;
    let error = indexer
        .run_backfill_from_storage(&storage, temp.path(), &mut manifest, migrated_plan)
        .expect_err("an incompatible model must not reuse a checkpoint");
    assert!(error.to_string().contains("incompatible vector space"));
    assert_eq!(fs::read(&migrated.index_path)?, incompatible_bytes);
    assert_eq!(
        fs::read(SemanticManifest::path(temp.path()))?,
        manifest_before
    );
    assert!(manifest.fast_tier.is_none());
    Ok(())
}
