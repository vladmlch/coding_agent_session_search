// Included inside indexer::tests: use the real persistence path and fixtures.
mod gh473_preflight {
    use super::*;
    use std::process::{Command, Stdio};

    /// Exact canonical share: the committed index, served through the real
    /// Quill reader under the production canonical-message guard. Region
    /// documents (`msg_idx >= REGION_DOC_BASE`) cannot enter this list, and
    /// neither can the `live_region_doc_bound` bookkeeping — persisted
    /// doc-backed region rows and queued stale-delete identities are not
    /// live canonical documents. Returns the stored `msg_idx` of every live
    /// canonical document as a sorted multiset — a distinct document with a
    /// colliding `msg_idx` (e.g. another conversation's msg-0) keeps its own
    /// slot, so an extra canonical hit cannot silently collapse the way a
    /// set would, and a missing document is named, not just counted.
    fn canonical_msg_idx_vec(index: &TantivyIndex) -> Vec<u64> {
        let reader = index.reader().expect("canonical reader");
        crate::search::quill_bridge::refresh_reader(&reader).expect("canonical reader reload");
        let query = crate::search::quill_bridge::canonical_message_query(
            frankensearch::quill::query::Query::all(),
        );
        // The limit covers every fixture document; the exact total must
        // equal the returned page so no canonical hit is truncated away.
        let page = crate::search::quill_bridge::search_paginated(&reader, &query, 1024, 0, true)
            .expect("canonical document query");
        assert_eq!(
            page.total_count.map(|count| count as u64),
            Some(page.hits.len() as u64),
            "the canonical page must cover the exact total"
        );
        let fields = crate::search::quill_bridge::QuillCassFields::compiled();
        let mut msg_idxs: Vec<u64> = page
            .hits
            .iter()
            .map(|hit| {
                crate::search::quill_bridge::stored_u64(&reader, fields.msg_idx, hit.global_docid)
                    .expect("read stored msg_idx")
                    .expect("canonical documents always carry msg_idx")
            })
            .collect();
        msg_idxs.sort_unstable();
        msg_idxs
    }

    /// Packet mirroring `canonical_lexical_packet`: source identity material
    /// read from the stored conversation row, so the reconcile path names the
    /// exact document identities ingest published for this conversation.
    fn canonical_replay_packet(
        storage: &FrankenStorage,
        conversation: &NormalizedConversation,
        conversation_id: i64,
    ) -> ConversationPacket {
        let canonical_row = storage
            .raw()
            .query(&format!(
                "SELECT source_id, origin_host FROM conversations WHERE id = {conversation_id}"
            ))
            .unwrap();
        let raw_source_id: Option<String> = canonical_row[0].get_typed(0).unwrap();
        let raw_origin_host: Option<String> = canonical_row[0].get_typed(1).unwrap();
        let source_id = crate::search::tantivy::normalized_index_source_id(
            raw_source_id.as_deref(),
            None,
            raw_origin_host.as_deref(),
        );
        let mut packet = ConversationPacket::from_normalized_conversation(
            conversation,
            ConversationPacketProvenance {
                source_id: source_id.clone(),
                origin_kind: crate::search::tantivy::normalized_index_origin_kind(&source_id, None),
                origin_host: None,
            },
        );
        packet.payload.identity.conversation_id = Some(conversation_id);
        packet.payload.metadata_json = serde_json::json!({
            "cass": {"origin": {"source_id": source_id}}
        });
        packet
    }

    fn scalar(storage: &FrankenStorage, sql: &str) -> i64 {
        storage.raw().query(sql).unwrap()[0]
            .get_typed::<i64>(0)
            .unwrap()
    }

    fn prime_cold_writer(storage: &FrankenStorage) {
        assert!(!storage.ephemeral_writer_preflight_verified());
        let (writer, reusable) = storage.acquire_cached_ephemeral_writer().unwrap();
        assert!(
            reusable,
            "the regression must exercise the cached writer path"
        );
        // Force the actual connection and its cached policy to a stale long wait.
        writer.raw().execute("PRAGMA busy_timeout = 60000").unwrap();
        writer.mark_index_writer_busy_timeout_ms(60_000);
        storage.release_cached_ephemeral_writer(writer);
    }

    // Release a real writer lock only after the production retry loop has
    // observed a failure. No sleep-based race or synthetic Busy is involved.
    struct RetryObserver {
        retries: Arc<AtomicUsize>,
        release: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for RetryObserver {
        fn enabled(
            &self,
            metadata: &tracing::Metadata<'_>,
            _context: tracing_subscriber::layer::Context<'_, S>,
        ) -> bool {
            metadata.target().ends_with("indexer::persist")
        }

        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _context: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Message(bool);
            impl tracing::field::Visit for Message {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        self.0 = format!("{value:?}") == "begin_concurrent_retry";
                    }
                }
            }
            let mut message = Message(false);
            event.record(&mut message);
            if !message.0 {
                return;
            }
            self.retries.fetch_add(1, Ordering::Relaxed);
            if let Some((release, released)) = self.release.lock().unwrap().take() {
                release.send(()).unwrap();
                released.recv_timeout(Duration::from_secs(10)).unwrap();
            }
        }
    }

    fn with_held_writer<T>(
        path: &Path,
        release_on_retry: bool,
        operation: impl FnOnce() -> T,
    ) -> (T, usize) {
        use std::sync::mpsc;
        use tracing_subscriber::prelude::*;

        // Release even when an assertion in operation unwinds. Scoped joining
        // ensures the holder is gone before the temporary archive is removed.
        struct ReleaseOnDrop(mpsc::Sender<()>);
        impl Drop for ReleaseOnDrop {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }
        let path = path.to_path_buf();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (released_tx, released_rx) = mpsc::channel();
        let retries = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|scope| {
            let holder = scope.spawn(move || {
                let writer = crate::franken_sync::Connection::open_existing_schema_only(
                    path.to_string_lossy().into_owned(),
                )
                .unwrap();
                writer.execute("BEGIN IMMEDIATE").unwrap();
                ready_tx.send(()).unwrap();
                let released = release_rx.recv_timeout(Duration::from_secs(30));
                writer.execute("ROLLBACK").unwrap();
                writer.close_without_checkpoint().unwrap();
                let _ = released_tx.send(());
                assert!(released.is_ok(), "test never released the real writer");
            });
            let release_guard = ReleaseOnDrop(release_tx.clone());
            ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            let observer = RetryObserver {
                retries: Arc::clone(&retries),
                release: Mutex::new(release_on_retry.then_some((release_tx, released_rx))),
            };
            let result = tracing::subscriber::with_default(
                tracing_subscriber::registry().with(observer),
                operation,
            );
            drop(release_guard);
            holder.join().unwrap();
            (result, retries.load(Ordering::Relaxed))
        })
    }

    fn preflight_contention(expected: u64, release_after_failure: bool, primary: bool) {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("preflight.db");
        let storage = FrankenStorage::open(&path).unwrap();
        assert!(!storage.bulk_single_connection_enabled());
        let diagnostic = if primary {
            storage.enable_bulk_single_connection();
            storage
                .raw()
                .execute("PRAGMA busy_timeout = 60000")
                .unwrap();
            storage.mark_index_writer_busy_timeout_ms(60_000);
            "primary writer preflight failed"
        } else {
            prime_cold_writer(&storage);
            "ephemeral writer preflight write failed"
        };
        let mut bodies = 0;
        let started = Instant::now();
        // Deliberately do NOT add a test-only with_concurrent_retry around the
        // writer. The actual production setup must handle the first conflict.
        let (result, retries) = with_held_writer(&path, release_after_failure, || {
            persist::with_ephemeral_writer(&storage, false, "GH473 preflight", |writer| {
                bodies += 1;
                assert_eq!(scalar(writer, "PRAGMA busy_timeout"), expected as i64);
                Ok(())
            })
        });
        if release_after_failure {
            result.unwrap();
            assert_eq!((retries, bodies), (1, 1));
            assert!(storage.ephemeral_writer_preflight_verified());
        } else {
            let err = result.expect_err("exhaustion must not turn contention into success");
            assert!(
                err.to_string().contains(diagnostic),
                "wrong boundary: {err:#}"
            );
            assert!(anyhow_chain_indicates_retryable_storage_contention(&err));
            assert_eq!(retries, persist::SERIAL_CHUNK_CONTENTION_RETRIES);
            assert_eq!(bodies, 0);
            assert!(!storage.ephemeral_writer_preflight_verified());
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "preflight retained a long inner wait"
        );
        assert_eq!(scalar(&storage, "SELECT COUNT(*) FROM conversations"), 0);
        assert_eq!(scalar(&storage, "SELECT COUNT(*) FROM messages"), 0);
        assert!(storage.source_ingest_ledger_entries().unwrap().is_empty());
        storage.close().unwrap();
    }

    fn permanent_error_is_not_retried() {
        let tmp = TempDir::new().unwrap();
        let storage = FrankenStorage::open(&tmp.path().join("permanent.db")).unwrap();
        let mut attempts = 0;
        let result = persist::with_concurrent_retry(2, || {
            attempts += 1;
            persist::with_ephemeral_writer(&storage, false, "GH473 permanent error", |writer| {
                writer
                    .raw()
                    .execute("INSERT INTO gh473_table_that_does_not_exist VALUES (1)")
                    .map(|_| ())
                    .map_err(anyhow::Error::new)
            })
        });
        let err = result.expect_err("a permanent SQL failure must propagate");
        assert!(!anyhow_chain_indicates_retryable_storage_contention(&err));
        assert_eq!(attempts, 1);
        assert!(storage.source_ingest_ledger_entries().unwrap().is_empty());
        storage.close().unwrap();
    }

    fn canonical_write_and_replay_with_pinned_reader(expected: u64) {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("canonical.db");
        let storage = FrankenStorage::open(&path).unwrap();
        prime_cold_writer(&storage);
        let reader = FrankenStorage::open_readonly(&path).unwrap();
        reader.raw().execute("BEGIN").unwrap();
        assert_eq!(scalar(&reader, "SELECT COUNT(*) FROM messages"), 0);
        let conversation = norm_conv(Some("gh473-pinned-reader"), vec![norm_msg(0, 100)]);
        let completion = crate::storage::sqlite::SourceIngestLedgerEntry {
            key: "source_ingest_v1:gh473-pinned-reader".into(),
            observation: "{\"complete\":true,\"generation\":1}".into(),
        };
        for pass in 0..3 {
            let started = Instant::now();
            persist::persist_conversations_batched_inner(
                &storage,
                None,
                std::slice::from_ref(&conversation),
                LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
                false,
                false,
                None,
                persist::PersistHeartbeat::NONE,
                Some(&completion),
            )
            .unwrap();
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "canonical pass {pass} stalled"
            );
            assert_eq!(scalar(&storage, "SELECT COUNT(*) FROM conversations"), 1);
            assert_eq!(scalar(&storage, "SELECT COUNT(*) FROM messages"), 1);
            let ledger = storage.source_ingest_ledger_entries().unwrap();
            assert_eq!(ledger.len(), 1);
            assert_eq!(ledger.get(&completion.key), Some(&completion.observation));
            persist::with_ephemeral_writer(&storage, false, "GH473 reused policy", |writer| {
                assert_eq!(scalar(writer, "PRAGMA busy_timeout"), expected as i64);
                Ok(())
            })
            .unwrap();
        }
        assert_eq!(
            scalar(&reader, "SELECT COUNT(*) FROM messages"),
            0,
            "the snapshot stays pinned"
        );
        reader.raw().execute("ROLLBACK").unwrap();
        reader.close_without_checkpoint().unwrap();
        storage.close().unwrap();
        let reopened = FrankenStorage::open_readonly(&path).unwrap();
        assert_eq!(scalar(&reopened, "SELECT COUNT(*) FROM conversations"), 1);
        assert_eq!(scalar(&reopened, "SELECT COUNT(*) FROM messages"), 1);
        let ledger = reopened.source_ingest_ledger_entries().unwrap();
        assert_eq!(ledger.len(), 1);
        assert_eq!(ledger.get(&completion.key), Some(&completion.observation));
        reopened.close_without_checkpoint().unwrap();
    }

    fn readonly_preflight_is_permanent(primary: bool) {
        let tmp = TempDir::new().unwrap();
        let storage = FrankenStorage::open(&tmp.path().join("busy-readonly.db")).unwrap();
        if primary {
            storage.enable_bulk_single_connection();
            storage.raw().execute("PRAGMA query_only = ON").unwrap();
        } else {
            let (writer, reusable) = storage.acquire_cached_ephemeral_writer().unwrap();
            assert!(reusable);
            writer.raw().execute("PRAGMA query_only = ON").unwrap();
            storage.release_cached_ephemeral_writer(writer);
        }
        let mut attempts = 0;
        let mut bodies = 0;
        let result = persist::with_concurrent_retry(2, || {
            attempts += 1;
            persist::with_ephemeral_writer(&storage, false, "GH473 read-only preflight", |_| {
                bodies += 1;
                Ok(())
            })
        });
        let err = result.expect_err("query-only writers must reject preflight");
        assert!(
            err.downcast_ref::<crate::franken_sync::FrankenError>()
                .is_some()
        );
        assert!(
            !anyhow_chain_indicates_retryable_storage_contention(&err),
            "permanent error misclassified: {err:#}"
        );
        assert_eq!((attempts, bodies), (1, 0));
        assert!(!storage.ephemeral_writer_preflight_verified());
        assert_eq!(scalar(&storage, "SELECT COUNT(*) FROM messages"), 0);
        assert!(storage.source_ingest_ledger_entries().unwrap().is_empty());
        if primary {
            storage.raw().execute("PRAGMA query_only = OFF").unwrap();
        }
        storage.close().unwrap();
    }

    fn canonical_contention_preserves_completion_and_lexical_state() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("atomic.db");
        let storage = FrankenStorage::open(&path).unwrap();
        let index_path = tmp.path().join("index");
        let mut index = TantivyIndex::open_or_create(&index_path).unwrap();
        let mut conversation = norm_conv(Some("gh473-atomic"), vec![norm_msg(0, 100)]);
        let mut completion = crate::storage::sqlite::SourceIngestLedgerEntry {
            key: "source_ingest_v1:gh473-atomic".into(),
            observation: "generation-1".into(),
        };
        prime_cold_writer(&storage);
        let (first, retries) = with_held_writer(&path, true, || {
            persist::persist_conversations_batched_inner(
                &storage,
                Some(&mut index),
                std::slice::from_ref(&conversation),
                LexicalPopulationStrategy::IncrementalInline,
                false,
                false,
                None,
                persist::PersistHeartbeat::NONE,
                Some(&completion),
            )
        });
        let first = first.unwrap();
        assert_eq!(
            retries, 1,
            "cold preflight must really retry inside ingestion"
        );
        assert_eq!(
            (first.inserted_conversations, first.inserted_messages),
            (1, 1)
        );
        index.commit().unwrap();
        // Task 5: the index also holds this session's region documents;
        // the canonical share is the exact multiset the committed index serves.
        assert_eq!(
            canonical_msg_idx_vec(&index),
            vec![0_u64],
            "canonical message share (Task 5: region docs share the index)"
        );

        // With a warm writer, the existing canonical transaction retry budget
        // must fail without committing either the new message or the new ledger.
        conversation.messages.push(norm_msg(1, 101));
        completion.observation = "generation-2".into();
        let (blocked, retries) = with_held_writer(&path, false, || {
            persist::persist_conversations_batched_inner(
                &storage,
                Some(&mut index),
                std::slice::from_ref(&conversation),
                LexicalPopulationStrategy::IncrementalInline,
                false,
                false,
                None,
                persist::PersistHeartbeat::NONE,
                Some(&completion),
            )
        });
        let err = blocked.expect_err("a genuine holder must exhaust the transaction retry budget");
        assert!(anyhow_chain_indicates_retryable_storage_contention(&err));
        assert_eq!(retries, persist::SERIAL_CHUNK_CONTENTION_RETRIES);
        assert_eq!(scalar(&storage, "SELECT COUNT(*) FROM messages"), 1);
        let ledger = storage.source_ingest_ledger_entries().unwrap();
        assert_eq!(ledger.len(), 1);
        assert_eq!(
            ledger.get(&completion.key).map(String::as_str),
            Some("generation-1")
        );
        index.commit().unwrap();
        assert_eq!(
            canonical_msg_idx_vec(&index),
            vec![0_u64],
            "canonical message share (Task 5: region docs share the index)"
        );

        // Resume the exact source, then replay it unchanged. Both sinks and
        // the source-completion observation must converge without duplicates.
        for expected_insertions in [1, 0] {
            let outcome = persist::persist_conversations_batched_inner(
                &storage,
                Some(&mut index),
                std::slice::from_ref(&conversation),
                LexicalPopulationStrategy::IncrementalInline,
                false,
                false,
                None,
                persist::PersistHeartbeat::NONE,
                Some(&completion),
            )
            .unwrap();
            assert_eq!(outcome.inserted_messages, expected_insertions);
            assert_eq!(outcome.inserted_conversations, 0);
            index.commit().unwrap();
            assert_eq!(
                canonical_msg_idx_vec(&index),
                vec![0_u64, 1],
                "canonical message share (Task 5: region docs share the index)"
            );
            assert_eq!(scalar(&storage, "SELECT COUNT(*) FROM conversations"), 1);
            assert_eq!(scalar(&storage, "SELECT COUNT(*) FROM messages"), 2);
            let ledger = storage.source_ingest_ledger_entries().unwrap();
            assert_eq!(ledger.len(), 1);
            assert_eq!(ledger.get(&completion.key), Some(&completion.observation));
        }

        // Missing-canonical negative control: drive the real reconcile
        // removal path (`reconcile_messages_from_packet` — what
        // `publish_changed_packet_messages` runs when a revision lands) with
        // a revision whose content is hard noise. The packet reproduces the
        // canonical replay context — source identity material read from the
        // stored row — so the removal names the exact document ingest
        // published. Region marks stay `pending` with a queued stale-delete
        // identity, the bookkeeping the subtracted bound conflated, while
        // one expected canonical document is truly absent.
        let conversation_id = scalar(&storage, "SELECT MIN(id) FROM conversations");
        let mut noise_packet = canonical_replay_packet(&storage, &conversation, conversation_id);
        noise_packet.payload.messages[1].content = "   ".to_owned();
        index
            .reconcile_messages_from_packet(&noise_packet, &[1], Some(conversation_id))
            .unwrap();
        index.commit().unwrap();
        let observed = canonical_msg_idx_vec(&index);
        assert_eq!(
            observed,
            vec![0_u64],
            "the noise revision must really remove msg-1's canonical document"
        );
        let missing: Vec<u64> = [0_u64, 1]
            .into_iter()
            .filter(|idx| !observed.contains(idx))
            .collect();
        assert_eq!(
            missing,
            vec![1_u64],
            "the exact oracle must name the missing canonical message"
        );
        assert_eq!(
            scalar(&storage, "SELECT COUNT(*) FROM viewer_search_region_marks"),
            1,
            "region bookkeeping stays present while a canonical doc is absent"
        );
        assert_ne!(
            storage
                .raw()
                .query("SELECT pending_doc_deletes FROM viewer_search_region_marks")
                .unwrap()[0]
                .get_typed::<String>(0)
                .unwrap(),
            "[]",
            "queued stale region deletes survive alongside the missing canonical doc"
        );

        // Restoring the message republishes its document through the same
        // real reconcile path.
        noise_packet.payload.messages[1].content = "msg-1".to_owned();
        index
            .reconcile_messages_from_packet(&noise_packet, &[1], Some(conversation_id))
            .unwrap();
        index.commit().unwrap();
        assert_eq!(
            canonical_msg_idx_vec(&index),
            vec![0_u64, 1],
            "the reconcile republish must restore the removed canonical document"
        );

        // Admission: a real third-message ingest shifts this session's
        // region slots again and queues the retired slot's identity for
        // deletion — an identity this index never materialized, because the
        // writer's staged-session dedup kept only the first generation's
        // staging. Both canonical documents stay live; the bound still
        // counts the queued-absent identity, so only the exact canonical
        // query admits the healthy share.
        conversation.messages.push(norm_msg(2, 102));
        persist::persist_conversations_batched_inner(
            &storage,
            Some(&mut index),
            std::slice::from_ref(&conversation),
            LexicalPopulationStrategy::IncrementalInline,
            false,
            false,
            None,
            persist::PersistHeartbeat::NONE,
            None,
        )
        .unwrap();
        index.commit().unwrap();
        let observed = canonical_msg_idx_vec(&index);
        assert_eq!(
            observed,
            vec![0_u64, 1, 2],
            "queued pending-delete identities absent from the index must not \
             hide live canonical documents"
        );
        let live_noncanonical = index.doc_count().unwrap() - observed.len() as u64;
        let bound =
            crate::viewer_extensions::search_regions::live_region_doc_bound(storage.raw()).unwrap();
        assert!(
            bound > live_noncanonical,
            "the persisted bound must out-count the live non-canonical docs: \
             queued deletes name identities absent from the index \
             (bound {bound}, live non-canonical {live_noncanonical})"
        );

        // Extra-canonical negative control: a second conversation's canonical
        // document is real index content this fixture never expects. Its
        // msg-0 shares conv-1's `msg_idx`, so a plain set of idx values would
        // silently collapse it; the multiset must refuse the phantom share by
        // keeping both zeroes.
        let extra = norm_conv(Some("gh473-extra"), vec![norm_msg(0, 200)]);
        persist::persist_conversations_batched_inner(
            &storage,
            Some(&mut index),
            std::slice::from_ref(&extra),
            LexicalPopulationStrategy::IncrementalInline,
            false,
            false,
            None,
            persist::PersistHeartbeat::NONE,
            None,
        )
        .unwrap();
        index.commit().unwrap();
        assert_eq!(
            canonical_msg_idx_vec(&index),
            vec![0_u64, 0, 1, 2],
            "an extra canonical document must keep its own hit — the oracle \
             cannot deduplicate duplicate msg_idx values away"
        );

        // Removing the extra document through the same real reconcile path
        // restores the healthy share.
        let extra_id = scalar(&storage, "SELECT MAX(id) FROM conversations");
        let mut extra_packet = canonical_replay_packet(&storage, &extra, extra_id);
        extra_packet.payload.messages[0].content = "   ".to_owned();
        index
            .reconcile_messages_from_packet(&extra_packet, &[0], Some(extra_id))
            .unwrap();
        index.commit().unwrap();
        assert_eq!(
            canonical_msg_idx_vec(&index),
            vec![0_u64, 1, 2],
            "the extra conversation's canonical document must really leave \
             the share"
        );
        drop(index);
        storage.close().unwrap();
        let reopened = FrankenStorage::open_readonly(&path).unwrap();
        assert_eq!(
            scalar(
                &reopened,
                &format!("SELECT COUNT(*) FROM messages WHERE conversation_id = {conversation_id}")
            ),
            3
        );
        assert_eq!(
            reopened
                .source_ingest_ledger_entries()
                .unwrap()
                .get(&completion.key),
            Some(&completion.observation)
        );
        let index = TantivyIndex::open_or_create(&index_path).unwrap();
        // The index also carries region documents; compare the exact
        // canonical share, not the raw live total.
        assert_eq!(
            canonical_msg_idx_vec(&index),
            vec![0_u64, 1, 2],
            "canonical message share (Task 5: region docs share the index)"
        );
        reopened.close_without_checkpoint().unwrap();
    }

    fn writer_body_errors_are_never_replayed() {
        let tmp = TempDir::new().unwrap();
        let storage = FrankenStorage::open(&tmp.path().join("body.db")).unwrap();
        let mut calls = 0;
        let error =
            persist::with_ephemeral_writer(&storage, false, "GH473 once-only body", |writer| {
                calls += 1;
                writer
                    .raw()
                    .execute("INSERT INTO meta (key, value) VALUES ('gh473-side-effect', '1')")?;
                Err::<(), _>(anyhow::Error::new(crate::franken_sync::FrankenError::Busy))
            })
            .unwrap_err();
        assert!(anyhow_chain_indicates_retryable_storage_contention(&error));
        assert_eq!(
            calls, 1,
            "setup retries must never replay an arbitrary writer body"
        );
        assert_eq!(
            scalar(
                &storage,
                "SELECT COUNT(*) FROM meta WHERE key = 'gh473-side-effect'"
            ),
            1
        );
        storage.close().unwrap();
    }

    fn warm_writer_parent_registration_retries(workspace_only: bool) {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("parent-retry.db");
        let storage = FrankenStorage::open(&path).unwrap();
        let mut conversation = norm_conv(Some("gh473-parent"), vec![norm_msg(0, 100)]);
        conversation.workspace = Some(tmp.path().join("workspace"));
        let completion = crate::storage::sqlite::SourceIngestLedgerEntry {
            key: "source_ingest_v1:gh473-parent".into(),
            observation: "complete".into(),
        };
        persist::with_ephemeral_writer(&storage, false, "GH473 warm setup", |writer| {
            if workspace_only {
                writer.ensure_agent(&crate::model::types::Agent {
                    id: None,
                    slug: conversation.agent_slug.clone(),
                    name: conversation.agent_slug.clone(),
                    version: None,
                    kind: crate::model::types::AgentKind::Cli,
                })?;
            }
            Ok(())
        })
        .unwrap();
        assert!(storage.ephemeral_writer_preflight_verified());
        let (result, retries) = with_held_writer(&path, true, || {
            persist::persist_conversations_batched_inner(
                &storage,
                None,
                std::slice::from_ref(&conversation),
                LexicalPopulationStrategy::DeferredAuthoritativeDbRebuild,
                false,
                false,
                None,
                persist::PersistHeartbeat::NONE,
                Some(&completion),
            )
        });
        let result = result.unwrap();
        assert_eq!(
            retries, 1,
            "warm setup must retry the uncached parent write"
        );
        assert_eq!(
            (result.inserted_conversations, result.inserted_messages),
            (1, 1)
        );
        assert_eq!(scalar(&storage, "SELECT COUNT(*) FROM workspaces"), 1);
        assert_eq!(scalar(&storage, "SELECT COUNT(*) FROM conversations"), 1);
        assert_eq!(scalar(&storage, "SELECT COUNT(*) FROM messages"), 1);
        assert_eq!(
            storage
                .source_ingest_ledger_entries()
                .unwrap()
                .get(&completion.key),
            Some(&completion.observation)
        );
        storage.close().unwrap();
    }

    #[test]
    fn writer_preflight_contention_regression() {
        const CHILD: &str = "CASS_TEST_GH473_PREFLIGHT_CHILD";
        if let Ok(expected) = dotenvy::var(CHILD) {
            let expected = expected.parse::<u64>().unwrap();
            for primary in [false, true] {
                preflight_contention(expected, true, primary);
                preflight_contention(expected, false, primary);
                readonly_preflight_is_permanent(primary);
            }
            permanent_error_is_not_retried();
            writer_body_errors_are_never_replayed();
            warm_writer_parent_registration_retries(false);
            warm_writer_parent_registration_retries(true);
            canonical_contention_preserves_completion_and_lexical_state();
            canonical_write_and_replay_with_pinned_reader(expected);
            return;
        }
        // No unsafe global environment mutation; isolate ambient .env files too.
        for (value, expected) in [
            (None, "10"),
            (Some("0"), "10"),
            (Some("invalid"), "10"),
            (Some("-1"), "10"),
            (Some("18446744073709551616"), "10"),
            (Some("37"), "37"),
        ] {
            let dir = TempDir::new().unwrap();
            let stdout_path = dir.path().join("stdout.log");
            let stderr_path = dir.path().join("stderr.log");
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "indexer::tests::gh473_preflight::writer_preflight_contention_regression",
                    "--nocapture",
                ])
                .current_dir(dir.path())
                .env(CHILD, expected)
                .env_remove("CASS_INDEX_WRITER_BUSY_TIMEOUT_MS")
                .stdout(Stdio::from(std::fs::File::create(&stdout_path).unwrap()))
                .stderr(Stdio::from(std::fs::File::create(&stderr_path).unwrap()));
            if let Some(value) = value {
                command.env("CASS_INDEX_WRITER_BUSY_TIMEOUT_MS", value);
            }
            let mut child = command.spawn().unwrap();
            let started = Instant::now();
            let status = loop {
                if let Some(status) = child.try_wait().unwrap() {
                    break status;
                }
                if started.elapsed() > Duration::from_secs(40) {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("GH473 child policy {value:?} exceeded 40s (possible long inner wait)");
                }
                std::thread::sleep(Duration::from_millis(20));
            };
            let stdout = std::fs::read_to_string(stdout_path).unwrap();
            let stderr = std::fs::read_to_string(stderr_path).unwrap();
            assert!(status.success(), "policy {value:?}: {stdout}\n{stderr}");
            assert!(
                stdout.contains("1 passed; 0 failed"),
                "child did not run: {stdout}"
            );
        }
    }
}
