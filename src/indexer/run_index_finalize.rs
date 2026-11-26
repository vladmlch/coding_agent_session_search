use super::*;
use std::ops::ControlFlow;

pub(in crate::indexer) fn run_index_with_storage_ownership(
    opts: IndexOptions,
    event_channel: Option<(Sender<IndexerEvent>, Receiver<IndexerEvent>)>,
    local_connector_roots: LocalConnectorRootsOverride,
    maintain_raw_mirror: bool,
) -> Result<()> {
    let ControlFlow::Continue(preflight) = run_index_preflight_and_open(opts, maintain_raw_mirror)?
    else {
        return Ok(());
    };
    let (preflight, lexical) = run_index_lexical_and_scan(preflight, local_connector_roots)?;
    let (preflight, lexical) = run_index_post_scan_and_semantic(preflight, lexical)?;
    run_index_watch_or_close(preflight, lexical, event_channel)
}

fn run_index_post_scan_and_semantic(
    preflight: RunIndexPreflightOutcome,
    lexical: RunIndexLexicalOutcome,
) -> Result<(RunIndexPreflightOutcome, RunIndexLexicalOutcome)> {
    let RunIndexPreflightOutcome {
        mut ctx,
        mut storage,
        index_path,
        defer_checkpoints,
        canonical_storage_rebuilt,
        opened_fresh_for_full,
        reopened_after_writable_preflight,
        legacy_omp_upgrade,
        initial_canonical_sessions_before_salvage,
        canonical_only_full_rebuild,
        has_explicit_watch_once_paths,
        targeted_semantic_watch_once,
        resume_lexical_rebuild,
        restart_pending_lexical_rebuild_from_zero,
        initial_matching_lexical_checkpoint,
        reconcile_nightly_lexical,
        checked_daily_stats_pre_scan,
        tantivy_requires_rebuild,
        observed_tantivy_docs,
        needs_rebuild,
        initial_needs_rebuild,
        large_incremental_authoritative_lexical_repair_probe_deferred,
        deferred_incremental_authoritative_lexical_repair,
        empty_archive_lexical_rebuild,
        scan_start_ts,
        keep_tantivy_open_after_rebuild,
    } = preflight;
    let RunIndexLexicalOutcome {
        t_index,
        performed_scan,
        scan_canonical_mutations,
        scan_lexical_update_deferred,
        scanned_connectors,
        scan_had_errors,
        stale_index_ingest_quarantine_retry_attempted,
        deferred_watch_sources,
        mut exact_completed_lexical_checkpoint,
        exact_completed_lexical_checkpoint_predates_scan,
        skipped_noop_full_scan_authoritative_rebuild,
        targeted_watch_once_only_run,
    } = lexical;
    // GH #457: whatever this run did (incremental ingest, no-op scan, or a
    // rebuild that already proved itself), the generation it leaves behind
    // must serve what its completed checkpoint certified.
    verify_lexical_generation_not_hollow_after_run(&index_path, &ctx.opts.db_path)?;

    if legacy_omp_upgrade.lexical_rebuild_required {
        if !exact_completed_lexical_checkpoint {
            anyhow::bail!(
                "legacy OMP identity upgrade cannot mark lexical publication complete without an exact completed lexical checkpoint"
            );
        }
        storage
            .mark_legacy_omp_lexical_publish_complete()
            .with_context(|| "recording legacy OMP lexical publication")?;
    }
    if legacy_omp_upgrade.analytics_rebuild_required {
        if crate::storage::sqlite::explicit_analytics_rebuild_deferred() {
            tracing::warn!(
                db_path = %ctx.opts.db_path.display(),
                "legacy OMP analytics rebuild remains pending because CASS_DEFER_ANALYTICS_UPDATES is enabled; the completed lexical publication will not be repeated"
            );
        } else {
            check_legacy_omp_analytics_stop(ctx.opts.progress.as_ref())?;
            let prior_progress = ctx.opts.progress.as_ref().map(|progress| {
                (
                    progress.phase.load(Ordering::Relaxed),
                    progress.current.load(Ordering::Relaxed),
                    progress.total.load(Ordering::Relaxed),
                    progress.total_is_final.load(Ordering::Relaxed),
                )
            });
            ctx.index_run_lock
                .set_phase(ctx.initial_lock_mode, "analytics:legacy_omp")
                .with_context(|| "publishing legacy OMP analytics phase in index-run status")?;
            if let Some(progress) = ctx.opts.progress.as_ref() {
                progress.set_phase_progress(INDEX_PHASE_ANALYTICS_REBUILD, 0, 0);
                // The storage callback publishes the exact message count
                // after opening the repair; zero here is not a known total.
                progress.total_is_final.store(false, Ordering::Relaxed);
            }
            let report_analytics_heartbeat = || {
                if let Some(progress) = ctx.opts.progress.as_ref() {
                    progress.tick_activity();
                }
                bump_index_run_lock_progress_atomic(&ctx.progress_bump);
            };
            let report_analytics_progress = |processed: i64, total: i64| {
                report_legacy_omp_analytics_progress(ctx.opts.progress.as_ref(), processed, total);
                report_analytics_heartbeat();
            };
            let check_analytics_stop =
                || check_legacy_omp_analytics_stop(ctx.opts.progress.as_ref());
            // 2l1b0.75: the phase gets its own writer connection. Ingest and
            // repair commit through ephemeral writers, so opening a fresh
            // connection here picks up the latest database state cleanly.
            let analytics_writer = FrankenStorage::open_writer(&ctx.opts.db_path)
                .with_context(|| "opening a writer for the legacy OMP analytics rebuild")?;
            let rebuilt = analytics_writer
                .rebuild_legacy_omp_analytics_with_progress(
                    Some(&report_analytics_progress),
                    Some(&report_analytics_heartbeat),
                    Some(&check_analytics_stop),
                )
                .with_context(|| {
                    "rebuilding legacy OMP analytics after lexical publication with resumable progress"
                });
            let closed = analytics_writer
                .close()
                .with_context(|| "closing the legacy OMP analytics writer");
            let _ = rebuilt?;
            closed?;
            if let (Some(progress), Some((phase, current, total, total_is_final))) =
                (ctx.opts.progress.as_ref(), prior_progress)
            {
                // Terminal indexing summaries count conversations. Analytics
                // message counts must not replace the completed lexical count.
                progress.set_phase_progress(phase, current, total);
                progress
                    .total_is_final
                    .store(total_is_final, Ordering::Relaxed);
            }
        }
    }

    if stale_index_ingest_quarantine_retry_attempted && scan_watermark_preservation_active() {
        tracing::info!(
            data_dir = %ctx.opts.data_dir.display(),
            "leaving stale index-ingest quarantine retry records unchanged because this scan preserved the source watermark"
        );
    } else if stale_index_ingest_quarantine_retry_attempted {
        match mark_stale_index_ingest_quarantine_retry_attempted(&ctx.opts.data_dir) {
            Ok(marked) if marked > 0 => tracing::info!(
                data_dir = %ctx.opts.data_dir.display(),
                marked,
                current_version = current_cass_version(),
                "marked stale index-ingest quarantine records as retried for current cass version"
            ),
            Ok(_) => {}
            Err(err) => tracing::warn!(
                data_dir = %ctx.opts.data_dir.display(),
                error = %err,
                "failed to mark stale index-ingest quarantine records as retried"
            ),
        }
    }

    // Semantic indexing (if enabled)
    if ctx.opts.semantic && targeted_semantic_watch_once {
        tracing::info!(
            embedder = %ctx.opts.embedder,
            "deferring broad semantic indexing until targeted watch-once ingest completes"
        );
    } else if ctx.opts.semantic {
        let semantic_identity_tier =
            semantic_identity_tier_for_requested_embedder(&ctx.opts.embedder);
        let semantic_identity_rebuild_required = storage
            .semantic_identity_rebuild_required(semantic_identity_tier)
            .with_context(|| "checking whether canonical semantic filter identity changed")?;
        // A watermark from a prior input contract cannot certify the current
        // passage coverage. Only the requested, current artifact may use the
        // unchanged or incremental fast paths, including watch startup.
        let vi_dir = ctx
            .opts
            .data_dir
            .join(crate::search::vector_index::VECTOR_INDEX_DIR);
        let has_existing_index =
            semantic_index_has_current_contract(&ctx.opts.data_dir, &ctx.opts.embedder);
        let last_embedded = storage.get_last_embedded_message_id()?;
        let has_watermark = last_embedded.is_some();
        // #394: a one-shot `index --semantic` used to ignore the watermark
        // entirely and re-embed the whole corpus every run. When the existing
        // index + watermark already cover the newest message, there is nothing
        // to embed — skip the bulk pass on one-shot runs too.
        // An explicit full semantic rebuild must not be certified by an old
        // watermark, including when the canonical archive is now empty.
        let watermark_covers_corpus = !ctx.opts.full
            && !semantic_identity_rebuild_required
            && has_existing_index
            && match (last_embedded, storage.max_message_id()?) {
                (Some(watermark), Some(max_id)) => watermark >= max_id,
                (Some(_), None) => true,
                (None, _) => false,
            };

        // tpndx (#394 remainder): a one-shot `index --semantic` whose
        // watermark merely TRAILS the corpus used to fall through to the
        // full re-embed. Reuse the watch callback's incremental path (fetch
        // canonical packets since the watermark, embed, WAL-append to the
        // existing .fsvi, advance the watermark) so a routine refresh costs
        // the delta, not the corpus. Identity changes still force the bulk
        // rebuild, and any delta failure falls back to the bulk pass so the
        // artifact never ends up less complete than before.
        let one_shot_delta_eligible = !ctx.opts.full
            && !ctx.opts.watch
            && !semantic_identity_rebuild_required
            && has_existing_index
            && has_watermark
            && !watermark_covers_corpus;
        let mut one_shot_delta_embedded: Option<usize> = None;
        if one_shot_delta_eligible {
            prepare_progress_for_semantic_build(ctx.opts.progress.as_ref());
            if let Err(err) = ctx
                .index_run_lock
                .set_phase(ctx.initial_lock_mode, "semantic:incremental")
            {
                tracing::debug!(
                    error = %err,
                    "semantic incremental phase breadcrumb write failed (continuing)"
                );
            }
            set_semantic_progress_phase(
                ctx.opts.progress.as_ref(),
                &ctx.progress_bump,
                INDEX_PHASE_SEMANTIC_INITIALIZE,
                0,
                0,
            );
            let delta_started_at_ms = semantic_indexing_now_ms();
            let storage_cell = Mutex::new(storage);
            let outcome =
                incremental_semantic_embed(&ctx.opts.embedder, &ctx.opts.data_dir, &storage_cell);
            storage = storage_cell
                .into_inner()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match outcome {
                Ok(count) => {
                    tracing::info!(
                        dir = %vi_dir.display(),
                        watermark = last_embedded.unwrap_or(0),
                        embedded = count,
                        "one-shot semantic delta embed: appended messages past the \
                         watermark instead of re-embedding the corpus (tpndx / #394)"
                    );
                    // The appended artifact is searchable only once the manifest
                    // agrees with the canonical DB again (otherwise readiness
                    // reports "backfill in progress" and search falls back to
                    // lexical). Mirror the watch-once tail: compact the WAL,
                    // count live records, republish the manifest.
                    match finalize_one_shot_semantic_delta(
                        &ctx.opts.embedder,
                        &ctx.opts.data_dir,
                        &storage,
                        delta_started_at_ms,
                    ) {
                        Ok(doc_count) => {
                            tracing::info!(
                                doc_count,
                                "one-shot semantic delta: manifest republished"
                            );
                        }
                        Err(err) => {
                            tracing::warn!(
                                error = %err,
                                "one-shot semantic delta appended to disk but manifest \
                                 update failed; cass status may report backfill pending \
                                 until the next semantic run"
                            );
                        }
                    }
                    one_shot_delta_embedded = Some(count);
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "one-shot semantic delta embed failed; falling back to the \
                         bulk semantic pass"
                    );
                }
            }
        }

        if ctx.opts.watch
            && !ctx.opts.full
            && !semantic_identity_rebuild_required
            && has_existing_index
            && has_watermark
        {
            tracing::info!(
                dir = %vi_dir.display(),
                "skipping bulk semantic re-embed (existing index + watermark found); \
                 incremental watch callback will handle new messages"
            );
        } else if watermark_covers_corpus {
            tracing::info!(
                dir = %vi_dir.display(),
                watermark = last_embedded.unwrap_or(0),
                "skipping bulk semantic re-embed: watermark already covers the \
                 newest message and a vector index exists (issue #394)"
            );
        } else if let Some(count) = one_shot_delta_embedded {
            tracing::info!(
                embedded = count,
                "skipping bulk semantic re-embed: one-shot delta embed covered the \
                 trailing watermark (tpndx)"
            );
        } else {
            // The lexical pass may leave the shared progress atomics in phase
            // 2 even though its last batch is complete. A semantic-aware stall
            // watchdog still treats phase 2 as a genuine lexical wedge and can
            // otherwise abort a healthy multi-minute embed with exit 70. Park
            // the completed lexical pipeline in the quiescent phase-0 state
            // before semantic work starts (the same invariant used by targeted
            // semantic watch-once).
            prepare_progress_for_semantic_build(ctx.opts.progress.as_ref());
            let mut set_semantic_phase = |phase: &'static str| {
                if let Err(err) = ctx.index_run_lock.set_phase(ctx.initial_lock_mode, phase) {
                    tracing::debug!(
                        sub_phase = phase,
                        error = %err,
                        "semantic index phase breadcrumb write failed (continuing)"
                    );
                }
            };
            set_semantic_phase("semantic:initialize");
            set_semantic_progress_phase(
                ctx.opts.progress.as_ref(),
                &ctx.progress_bump,
                INDEX_PHASE_SEMANTIC_INITIALIZE,
                0,
                0,
            );
            tracing::info!(embedder = %ctx.opts.embedder, "starting semantic indexing");

            let semantic_indexer =
                SemanticIndexer::new(&ctx.opts.embedder, Some(&ctx.opts.data_dir))?;
            let mut semantic_manifest_published = false;
            set_semantic_phase("semantic:replay");
            set_semantic_progress_phase(
                ctx.opts.progress.as_ref(),
                &ctx.progress_bump,
                INDEX_PHASE_SEMANTIC_REPLAY,
                0,
                0,
            );
            let mut semantic_read_storage = FrankenStorage::open_readonly(&ctx.opts.db_path)
                .with_context(|| {
                    format!(
                        "opening fresh readonly canonical storage for semantic indexing: {}",
                        ctx.opts.db_path.display()
                    )
                })?;

            let build_started_at_ms = semantic_indexing_now_ms();
            let mut active_phase = semantic::SemanticStorageBuildPhase::Replay;
            let semantic_build = semantic_indexer.build_and_save_index_from_storage_with_progress(
                &semantic_read_storage,
                &ctx.opts.data_dir,
                |phase, current, total| {
                    let (name, code) = match phase {
                        semantic::SemanticStorageBuildPhase::Replay => {
                            ("semantic:replay", INDEX_PHASE_SEMANTIC_REPLAY)
                        }
                        semantic::SemanticStorageBuildPhase::Embedding => {
                            ("semantic:embedding", INDEX_PHASE_SEMANTIC_EMBEDDING)
                        }
                        semantic::SemanticStorageBuildPhase::VectorPublish => (
                            "semantic:vector_publish",
                            INDEX_PHASE_SEMANTIC_VECTOR_PUBLISH,
                        ),
                    };
                    if phase != active_phase {
                        set_semantic_phase(name);
                        set_semantic_progress_phase(
                            ctx.opts.progress.as_ref(),
                            &ctx.progress_bump,
                            code,
                            current,
                            total,
                        );
                        active_phase = phase;
                    } else {
                        update_semantic_progress(
                            ctx.opts.progress.as_ref(),
                            &ctx.progress_bump,
                            current,
                            total,
                        );
                    }
                },
            )?;
            tracing::info!(
                passage_count = semantic_build.input_passages,
                embedded_count = semantic_build.embedded_docs,
                packet_driven = true,
                "published semantic vector index from bounded canonical replay and embedding"
            );
            let index_path = crate::search::vector_index::vector_index_path(
                &ctx.opts.data_dir,
                semantic_indexer.embedder_id(),
            );

            // An empty completed replay is a real replacement too: leaving the
            // previous nonempty artifact installed would retain deleted evidence.
            set_semantic_phase("semantic:manifest");
            set_semantic_progress_phase(
                ctx.opts.progress.as_ref(),
                &ctx.progress_bump,
                INDEX_PHASE_SEMANTIC_MANIFEST,
                0,
                1,
            );
            if let Err(err) = publish_direct_semantic_artifact(
                &semantic_read_storage,
                &ctx.opts.data_dir,
                &index_path,
                semantic_indexer.embedder_id(),
                semantic_indexer.embedder_dimension(),
                u64::try_from(semantic_build.embedded_docs).unwrap_or(u64::MAX),
                build_started_at_ms,
            ) {
                tracing::warn!(
                    embedder = semantic_indexer.embedder_id(),
                    error = %err,
                    "direct semantic artifact published to disk but \
                     manifest update failed; cass status may report \
                    stale/unavailable until next backfill cycle"
                );
            } else {
                semantic_manifest_published = true;
                update_semantic_progress(ctx.opts.progress.as_ref(), &ctx.progress_bump, 1, 1);
            }

            // Set watermark so incremental watch-mode embedding only sees new messages
            set_semantic_phase("semantic:finalize");
            set_semantic_progress_phase(
                ctx.opts.progress.as_ref(),
                &ctx.progress_bump,
                INDEX_PHASE_SEMANTIC_FINALIZE,
                0,
                1,
            );
            semantic_read_storage.close_best_effort_in_place();
            persist::with_ephemeral_writer(
                &storage,
                false,
                "updating semantic indexing watermark",
                |writer| {
                    writer.set_last_embedded_message_id(semantic_build.last_message_id.unwrap_or(0))
                },
            )?;
            if semantic_identity_rebuild_required && semantic_manifest_published {
                crate::indexer::semantic::invalidate_identity_stale_semantic_shards(
                    &ctx.opts.data_dir,
                    semantic_indexer.embedder_id(),
                )
                .with_context(
                    || "revoking identity-stale semantic shard generations after direct publish",
                )?;
                persist::with_ephemeral_writer(
                    &storage,
                    false,
                    "completing semantic identity rebuild",
                    |writer| writer.complete_semantic_identity_rebuild(semantic_identity_tier),
                )?;
            }
            update_semantic_progress(ctx.opts.progress.as_ref(), &ctx.progress_bump, 1, 1);
        }
        // The embedding watermark can skip the bulk pass, or the delta path
        // can publish a replacement generation. Both still owe explicitly
        // requested graph maintenance, after the exact artifact is published.
        if ctx.opts.build_hnsw {
            prepare_progress_for_semantic_build(ctx.opts.progress.as_ref());
            ctx.index_run_lock
                .set_phase(ctx.initial_lock_mode, "semantic:hnsw")?;
            set_semantic_progress_phase(
                ctx.opts.progress.as_ref(),
                &ctx.progress_bump,
                INDEX_PHASE_SEMANTIC_HNSW,
                0,
                0,
            );
            let vector_count = maintain_requested_hnsw(&ctx.opts.data_dir, &ctx.opts.embedder)?;
            update_semantic_progress(
                ctx.opts.progress.as_ref(),
                &ctx.progress_bump,
                vector_count,
                vector_count,
            );
        }
    }

    if targeted_watch_once_only_run {
        tracing::info!(
            db_path = %ctx.opts.db_path.display(),
            "deferring final index-run metadata update until targeted watch-once paths are evaluated"
        );
    } else {
        let now_ms = FrankenStorage::now_millis();
        let preserve_scan_watermark = scan_watermark_preservation_active();
        let performed_scan_for_global_watermark =
            performed_scan && !preserve_scan_watermark && !scan_had_errors;
        // `scanned_connectors` already excludes the exact connector that
        // skipped an active source (and excludes every connector when path
        // exclusions are configured). Persist the remaining safe connector
        // watermarks even when one unrelated connector must stay behind.
        let performed_scan_for_connector_watermarks = performed_scan;
        if performed_scan && preserve_scan_watermark {
            tracing::info!(
                db_path = %ctx.opts.db_path.display(),
                "preserving final last_scan_ts because scan exclusions or active source skips are active"
            );
        } else if performed_scan && scan_had_errors {
            tracing::warn!(
                db_path = %ctx.opts.db_path.display(),
                "preserving final global last_scan_ts because one or more connector scans failed; successful connector-specific watermarks will still be updated"
            );
        }
        persist_final_index_run_metadata(
            &storage,
            &ctx.opts.db_path,
            performed_scan_for_global_watermark,
            scan_start_ts,
            now_ms,
        )?;
        // GH #413 follow-up: when this run's inline `fts_messages` writes blew
        // their budget the shadow was left behind on purpose (canonical rows
        // and the Quill index landed; only the SQL fallback lags). Persist the
        // reason where `doctor` already reads it. Best-effort observability.
        if let Some(detail) = storage.fts_inline_suspension() {
            tracing::warn!(
                db_path = %ctx.opts.db_path.display(),
                detail = %detail,
                "inline fallback-FTS shadow writes were suspended for this run; the shadow is behind \
                 (Quill lexical search is unaffected) and `cass doctor` reports it"
            );
            if let Err(err) = storage.record_fallback_fts_repair_pending(Some(&detail)) {
                tracing::warn!(
                    error = %err,
                    "recording the suspended inline FTS shadow writes failed (non-fatal)"
                );
            }
        }
        // GH #413 follow-up (iify0): a run that grew the corpus past the shadow
        // bound drops the shadow now, on this connection (the vtab's destructor
        // never reads it), so the next writable open pays nothing for it.
        if storage.fts_shadow_drop_pending() {
            let detail = storage
                .fts_inline_suspension()
                .unwrap_or_else(|| crate::storage::sqlite::fts_shadow_not_viable_detail(0, 0));
            match storage.drop_fts_shadow_as_not_viable(&detail) {
                Ok(()) => tracing::warn!(
                    db_path = %ctx.opts.db_path.display(),
                    "dropped the derived fallback FTS shadow: the corpus crossed the shadow bound \
                     during this run (GH #413); Quill lexical search is unaffected"
                ),
                Err(err) => tracing::warn!(
                    error = %format!("{err:#}"),
                    "dropping the oversized fallback FTS shadow failed (non-fatal); the next run \
                     retries at preflight"
                ),
            }
        }
        // zn1xn F4: track the persistent lexical-repair deferral streak so
        // `status`/`index --json` expose the recurring full-rebuild
        // amplification (previously only a stderr warn). Increment when this
        // run deferred inline lexical updates; clear the streak when a scan
        // completed without deferring. Best-effort observability — a marker
        // write must never fail a run whose indexing work already succeeded.
        if scan_lexical_update_deferred {
            if let Err(err) = storage.record_lexical_repair_deferred(
                "inline lexical publication deferred during non-watch scan; \
                 authoritative canonical lexical rebuild performed",
            ) {
                tracing::debug!(
                    error = %format!("{err:#}"),
                    "recording lexical-repair deferral marker failed (non-fatal)"
                );
            }
        } else if performed_scan {
            let _ = storage.clear_lexical_repair_deferred();
        }
        if performed_scan_for_connector_watermarks {
            persist_connector_scan_watermarks(
                &storage,
                &ctx.opts.db_path,
                &scanned_connectors,
                scan_start_ts,
            )?;
        }
    }
    // GH #457 follow-on: the pre-scan authoritative repair rebuilt the lexical
    // index from SQLite and persisted an exact completed checkpoint, and this
    // run then continued into the source scan. If that scan ingested
    // anything, both the checkpoint's storage fingerprint and the
    // exact row counts the rebuild recorded in progress describe the PRE-scan
    // database. Leaving them alone is the bug this branch exists to prevent:
    // the skip below would keep the stale fingerprint on disk, so search's
    // fingerprint check ("storage fingerprint no longer matches") and
    // `cass status` keep reporting the assets stale until some LATER run
    // rewrites the checkpoint.
    //
    // Re-deriving, rather than merely forcing the refresh to run, is the other
    // half of the fix. `total_counts_exact` is sticky once the rebuild sets
    // it, but the scan that follows overwrites the counts beside it with what
    // it discovered — so at this point `exact_total_counts` is neither the
    // rebuild's totals nor the database's, even if every source was skipped:
    // such a scan can replace nonzero archive totals with zero. On the append
    // fixture it reads (1, 2), the one new session, against a database holding
    // (3, 6). The final refresh consumes those numbers verbatim to build the
    // `COUNT`/`MAX(id)` fingerprint, so running it on them would replace a
    // stale fingerprint with a wrong one.
    if should_redrive_final_lexical_checkpoint_refresh_after_pre_scan_repair(
        exact_completed_lexical_checkpoint_predates_scan,
        performed_scan,
    ) {
        let post_scan_conversations = count_total_conversations_exact(&storage)?;
        let post_scan_messages = count_total_messages_exact(&storage)?;
        tracing::info!(
            db_path = %ctx.opts.db_path.display(),
            inserted_conversations = scan_canonical_mutations.inserted_conversations,
            inserted_messages = scan_canonical_mutations.inserted_messages,
            post_scan_conversations,
            post_scan_messages,
            "re-deriving exact canonical totals after the pre-scan authoritative lexical repair because the follow-up source scan replaced its progress counts"
        );
        record_exact_total_counts_in_progress(
            ctx.opts.progress.as_ref(),
            post_scan_conversations,
            post_scan_messages,
        );
        // Narrow, deliberate: the rebuild really did persist exact completed
        // state, it is just no longer current. The skip immediately below is
        // this flag's only remaining reader (the legacy-OMP publication gate
        // has already run), so clearing it here means exactly "do not take the
        // already-exact shortcut". A new reader added after this point must not
        // assume the flag still describes what the rebuild did.
        exact_completed_lexical_checkpoint = false;
    }
    let exact_total_counts = exact_total_counts_from_progress(ctx.opts.progress.as_ref());
    if exact_completed_lexical_checkpoint && exact_total_counts.is_some() {
        tracing::info!(
            db_path = %ctx.opts.db_path.display(),
            "skipping final lexical checkpoint refresh because the authoritative rebuild already persisted exact completed state"
        );
    } else if skipped_noop_full_scan_authoritative_rebuild {
        tracing::info!(
            db_path = %ctx.opts.db_path.display(),
            inserted_conversations = scan_canonical_mutations.inserted_conversations,
            inserted_messages = scan_canonical_mutations.inserted_messages,
            "skipping final lexical checkpoint refresh because the full scan preserved an already-matching completed checkpoint"
        );
    } else if targeted_watch_once_only_run {
        tracing::info!(
            db_path = %ctx.opts.db_path.display(),
            "skipping final lexical checkpoint refresh because targeted watch-once startup does not need broad checkpoint maintenance"
        );
    } else if let Some(deferred) = &large_incremental_authoritative_lexical_repair_probe_deferred {
        // GH #353: a large incremental run defers the *expensive* authoritative
        // rebuild — but it must NOT preserve a stale lexical checkpoint
        // fingerprint forever. Otherwise search's fingerprint-mismatch defer
        // ("storage fingerprint no longer matches") and this run's deferral form
        // a loop that never repairs (`cass index` no-ops above the size cap and
        // re-preserves the very fingerprint that keeps triggering the defer).
        //
        // Break the loop with the CHEAP, doc-count-gated metadata-only refresh:
        // `refresh_completed_lexical_rebuild_checkpoint` rewrites the checkpoint
        // fingerprint ONLY when the live Tantivy index already matches the
        // noise-adjusted expected document count (GH #435; i.e. the drift was
        // absorbed inline), and
        // safely skips otherwise — so it never masks genuine staleness (which
        // would silently drop the newly-ingested sessions from search). When
        // Tantivy is really behind, the refresh no-ops and search keeps
        // deferring; the real escalation there is a full rebuild, named below.
        grant_final_checkpoint_refresh_grace(ctx.opts.progress.as_ref());
        if let Err(err) = refresh_completed_lexical_rebuild_checkpoint(
            &storage,
            &ctx.opts.db_path,
            &ctx.opts.data_dir,
        ) {
            tracing::warn!(
                db_path = %ctx.opts.db_path.display(),
                error = %format!("{err:#}"),
                "gh353: bounded lexical checkpoint reconciliation failed; leaving the checkpoint unchanged"
            );
        }
        tracing::warn!(
            db_path = %ctx.opts.db_path.display(),
            canonical_conversations = deferred.canonical_conversations,
            db_size_bytes = deferred.db_size_bytes,
            max_automatic_repair_db_size_bytes =
                deferred.max_automatic_repair_db_size_bytes,
            reason = deferred.reason,
            inserted_conversations = scan_canonical_mutations.inserted_conversations,
            inserted_messages = scan_canonical_mutations.inserted_messages,
            "deferred the expensive authoritative lexical repair on this large incremental run and reconciled the lexical checkpoint against the live Tantivy index where equivalent (GH #353). If search still reports the storage fingerprint no longer matches, the derived index is genuinely behind — run 'cass index --full --force-rebuild' to rebuild the lexical index authoritatively"
        );
    } else if should_skip_noop_final_lexical_checkpoint_refresh(
        ctx.opts.full,
        initial_needs_rebuild,
        &initial_matching_lexical_checkpoint,
        exact_total_counts,
        scan_canonical_mutations,
        lexical_rebuild_state_path(&index_path).is_file(),
    ) {
        tracing::info!(
            db_path = %ctx.opts.db_path.display(),
            inserted_conversations = scan_canonical_mutations.inserted_conversations,
            inserted_messages = scan_canonical_mutations.inserted_messages,
            "skipping final lexical checkpoint refresh because this incremental run made no canonical changes and started from a matching completed checkpoint"
        );
    } else {
        grant_final_checkpoint_refresh_grace(ctx.opts.progress.as_ref());
        refresh_completed_lexical_rebuild_checkpoint_for_final_state(
            &mut storage,
            &ctx.opts.db_path,
            &ctx.opts.data_dir,
            ctx.opts.watch || ctx.opts.watch_once_paths.is_some(),
            exact_total_counts,
        )
        .with_context(|| {
            format!(
                "refreshing completed lexical checkpoint after index run for {}",
                ctx.opts.db_path.display()
            )
        })?;
    }

    let fallback_fts_archive_fingerprint = skipped_noop_full_scan_authoritative_rebuild
        .then_some(
            initial_matching_lexical_checkpoint
                .completed_storage_fingerprint
                .as_deref(),
        )
        .flatten();
    // #439: the fallback-FTS shadow maintenance below is the last heavy step
    // of a `--full` run and it runs AFTER the lexical generation is published
    // with every progress counter parked at phase 0 / `current == total`. To
    // the stall watchdog that is the #297 finalize-wedge shape, and on a
    // large corpus (the report: 640k messages) the shadow rebuild alone runs
    // past the base 300 s abort — killing a run whose index is already
    // complete and healthy. Two signals cover it: the per-page heartbeat
    // ticks `activity` while rows stream (so an active rebuild never even
    // reports), and `finalizing` grants the bounded finalize-class grace for
    // the parity probes that cannot tick (a COUNT over the FTS shadow).
    if should_repair_fallback_fts_after_full_index_run(ctx.opts.full, canonical_only_full_rebuild) {
        if let Some(progress) = ctx.opts.progress.as_ref() {
            progress.finalizing.store(true, Ordering::Relaxed);
            progress.tick_activity();
        }
        if let Err(err) = ctx
            .index_run_lock
            .set_phase(ctx.initial_lock_mode, "fts:fallback_repair")
        {
            tracing::debug!(
                error = %err,
                "fallback FTS repair phase breadcrumb write failed (continuing)"
            );
        }
    }
    let fallback_fts_heartbeat: Option<Box<dyn Fn() + Send + Sync>> = {
        let progress = ctx.opts.progress.clone();
        let progress_bump = Arc::clone(&ctx.progress_bump);
        Some(Box::new(move || {
            if let Some(progress) = progress.as_ref() {
                progress.tick_activity();
            }
            bump_index_run_lock_progress_atomic(&progress_bump);
        }))
    };
    let fallback_fts_repair = repair_fallback_fts_after_full_index_run(
        &storage,
        &ctx.opts.db_path,
        ctx.opts.full,
        canonical_only_full_rebuild,
        fallback_fts_archive_fingerprint,
        fallback_fts_heartbeat,
    );
    if let Some(progress) = ctx.opts.progress.as_ref() {
        progress.tick_activity();
        // The final WAL checkpoint below re-arms `finalizing` for its own
        // window; keep the flag scoped to the work it describes.
        progress.finalizing.store(false, Ordering::Relaxed);
    }
    if let Some(repair) = fallback_fts_repair.with_context(|| {
        format!(
            "repairing SQLite-owned fallback FTS after full index run for {}",
            ctx.opts.db_path.display()
        )
    })? {
        // #434 defect 3: track consecutive IDENTICAL derived-FTS corruption
        // failures across runs. `Some(detail)` marks this run's repair attempt
        // as the corruption class; any other attempted outcome resets the
        // chain below.
        let mut fts_repair_corruption_failure: Option<String> = None;
        match repair {
            FallbackFtsRepairOutcome::SkippedKnownHealthyForFingerprint {
                archive_fingerprint,
            } => {
                tracing::info!(
                    db_path = %ctx.opts.db_path.display(),
                    archive_fingerprint,
                    "skipping fallback FTS consistency repair because this no-op full run preserved an archive fingerprint already known to be healthy"
                );
            }
            FallbackFtsRepairOutcome::SkippedUnsupportedPopulatedShadowReload { detail } => {
                tracing::warn!(
                    db_path = %ctx.opts.db_path.display(),
                    error = %detail,
                    "skipping derived fallback FTS repair because populated FTS shadow WITHOUT ROWID reload is unsupported; canonical SQLite rows and Tantivy remain authoritative"
                );
            }
            FallbackFtsRepairOutcome::SkippedCorruptDerivedIndex { detail } => {
                tracing::warn!(
                    db_path = %ctx.opts.db_path.display(),
                    error = %detail,
                    "derived fallback FTS remains corrupt after its best-effort repair; preserving the successful canonical SQLite and Tantivy build. Run 'cass doctor check --json' before an explicit fallback-FTS rebuild"
                );
                fts_repair_corruption_failure = Some(detail);
            }
            FallbackFtsRepairOutcome::SkippedRepairFailed { detail } => {
                tracing::warn!(
                    db_path = %ctx.opts.db_path.display(),
                    error = %detail,
                    "optional derived fallback FTS repair failed; preserving the successful canonical SQLite and Tantivy build (#329). Lexical search is served by Tantivy; run 'cass doctor check --json' to inspect the shadow, and 'cass doctor --rebuild-canonical-fts --yes' for an explicit repair"
                );
                fts_repair_corruption_failure = Some(detail);
            }
            FallbackFtsRepairOutcome::SkippedNotViable { detail } => {
                tracing::warn!(
                    db_path = %ctx.opts.db_path.display(),
                    detail = %detail,
                    "derived fallback FTS shadow stays dropped: the corpus exceeds the shadow bound \
                     (GH #413); Quill lexical search is unaffected"
                );
            }
            FallbackFtsRepairOutcome::Repaired(FtsConsistencyRepair::AlreadyHealthy { rows }) => {
                tracing::info!(
                    db_path = %ctx.opts.db_path.display(),
                    rows,
                    "fallback FTS was already healthy after full index run; skipped rebuild"
                );
            }
            FallbackFtsRepairOutcome::Repaired(FtsConsistencyRepair::IncrementalCatchUp {
                inserted_rows,
                total_rows,
            }) => {
                tracing::info!(
                    db_path = %ctx.opts.db_path.display(),
                    inserted_rows,
                    total_rows,
                    "incrementally repaired fallback FTS after full index run"
                );
            }
            FallbackFtsRepairOutcome::Repaired(FtsConsistencyRepair::Rebuilt { inserted_rows }) => {
                tracing::info!(
                    db_path = %ctx.opts.db_path.display(),
                    inserted_rows,
                    "rebuilt fallback FTS after full index run"
                );
            }
        }
        // #434 defect 3: a derived FTS repair that fails IDENTICALLY on run
        // after run must not stay warn-and-exit-0 forever. Persist the streak
        // in the index state dir; once it reaches
        // FTS_REPAIR_FAILURE_ESCALATION_RUNS, a non-watch run exits non-zero
        // (the canonical + Tantivy work is already published and durable — the
        // exit code is the escalation signal for cron/automation). A watch
        // daemon logs the escalation instead of crash-looping.
        match fts_repair_corruption_failure {
            Some(detail) => {
                let streak = record_fts_repair_failure_streak(&index_path, &detail);
                let watch_mode = ctx.opts.watch || ctx.opts.watch_once_paths.is_some();
                if streak >= FTS_REPAIR_FAILURE_ESCALATION_RUNS {
                    if watch_mode {
                        tracing::warn!(
                            db_path = %ctx.opts.db_path.display(),
                            consecutive_identical_failures = streak,
                            escalation_threshold_runs = FTS_REPAIR_FAILURE_ESCALATION_RUNS,
                            "the optional derived fallback FTS repair keeps failing identically; a non-watch 'cass index' would now exit non-zero (#434). Run 'cass doctor --rebuild-canonical-fts --yes --json' for an explicit repair"
                        );
                    } else {
                        reset_progress_to_idle(ctx.opts.progress.as_ref());
                        anyhow::bail!(
                            "the optional derived fallback FTS repair has failed identically on {streak} consecutive index runs (escalation threshold: {FTS_REPAIR_FAILURE_ESCALATION_RUNS}; #434). \
                             This run's canonical rows and Tantivy lexical index completed and search still works — the non-zero exit exists so automation notices the persistent shadow-FTS corruption instead of an identical warning every run. \
                             Run 'cass doctor check --json' to inspect the shadow and 'cass doctor --rebuild-canonical-fts --yes --json' for an explicit repair; any run whose repair succeeds (or fails differently) resets the counter in {}. \
                             Last failure: {detail}",
                            fts_repair_failure_streak_path(&index_path).display()
                        );
                    }
                } else if streak > 1 {
                    tracing::warn!(
                        db_path = %ctx.opts.db_path.display(),
                        consecutive_identical_failures = streak,
                        escalation_threshold_runs = FTS_REPAIR_FAILURE_ESCALATION_RUNS,
                        "the optional derived fallback FTS repair has now failed identically on consecutive runs; 'cass index' escalates to a non-zero exit at the threshold (#434)"
                    );
                }
            }
            None => clear_fts_repair_failure_streak(&index_path),
        }
    } else if ctx.opts.full {
        tracing::info!(
            db_path = %ctx.opts.db_path.display(),
            canonical_only_full_rebuild,
            "skipping SQLite-owned fallback FTS rebuild because this full run only rebuilt Tantivy from the existing canonical database"
        );
    }

    reset_progress_to_idle(ctx.opts.progress.as_ref());

    let preflight = RunIndexPreflightOutcome {
        storage,
        ctx,
        index_path,
        defer_checkpoints,
        canonical_storage_rebuilt,
        opened_fresh_for_full,
        reopened_after_writable_preflight,
        legacy_omp_upgrade,
        initial_canonical_sessions_before_salvage,
        canonical_only_full_rebuild,
        has_explicit_watch_once_paths,
        targeted_semantic_watch_once,
        resume_lexical_rebuild,
        restart_pending_lexical_rebuild_from_zero,
        initial_matching_lexical_checkpoint,
        reconcile_nightly_lexical,
        checked_daily_stats_pre_scan,
        tantivy_requires_rebuild,
        observed_tantivy_docs,
        needs_rebuild,
        initial_needs_rebuild,
        large_incremental_authoritative_lexical_repair_probe_deferred,
        deferred_incremental_authoritative_lexical_repair,
        empty_archive_lexical_rebuild,
        scan_start_ts,
        keep_tantivy_open_after_rebuild,
    };
    let lexical = RunIndexLexicalOutcome {
        t_index,
        performed_scan,
        scan_canonical_mutations,
        scan_lexical_update_deferred,
        scanned_connectors,
        scan_had_errors,
        stale_index_ingest_quarantine_retry_attempted,
        deferred_watch_sources,
        exact_completed_lexical_checkpoint,
        exact_completed_lexical_checkpoint_predates_scan,
        skipped_noop_full_scan_authoritative_rebuild,
        targeted_watch_once_only_run,
    };
    Ok((preflight, lexical))
}

fn run_index_watch_or_close(
    preflight: RunIndexPreflightOutcome,
    lexical: RunIndexLexicalOutcome,
    event_channel: Option<(Sender<IndexerEvent>, Receiver<IndexerEvent>)>,
) -> Result<()> {
    let RunIndexPreflightOutcome {
        mut ctx,
        storage,
        index_path,
        targeted_semantic_watch_once,
        ..
    } = preflight;
    let RunIndexLexicalOutcome {
        t_index,
        mut deferred_watch_sources,
        targeted_watch_once_only_run,
        ..
    } = lexical;
    if ctx.opts.watch || ctx.opts.watch_once_paths.is_some() {
        let watch_once_mode = ctx
            .opts
            .watch_once_paths
            .as_ref()
            .is_some_and(|paths| !paths.is_empty());
        let additional_scan_roots = additional_scan_roots_for_scan_or_watch(
            &storage,
            &ctx.opts.data_dir,
            ctx.opts.watch && !watch_once_mode,
        );
        let watch_roots = build_watch_roots(additional_scan_roots.clone());
        if ctx.opts.watch && !watch_once_mode {
            // A stopped watch callback can leave canonical rows ahead of the
            // published index without another filesystem event to wake it.
            // Seed recovery once; steady-state callbacks return only work
            // that is actually retryable, not permanent exclusion debt.
            deferred_watch_sources.extend(watch_lexical_replay_sources(&storage, &watch_roots)?);
        }

        if targeted_watch_once_only_run
            && t_index.is_none()
            && watch_once_mode
            && should_skip_unchanged_explicit_watch_once_paths(&ctx.opts, &storage, &watch_roots)?
        {
            let path_count = ctx
                .opts
                .watch_once_paths
                .as_ref()
                .map(std::vec::Vec::len)
                .unwrap_or_default();
            record_lexical_population_strategy_if_unset(
                ctx.opts.progress.as_ref(),
                LexicalPopulationStrategy::IncrementalInline,
                "watch_once_targeted_reindex_applies_inline_lexical_updates_for_changed_paths",
            );
            tracing::info!(
                db_path = %ctx.opts.db_path.display(),
                path_count,
                "skipping unchanged explicit watch-once paths before opening Tantivy"
            );
            close_storage_after_index(storage, &ctx.opts.db_path, "watch-once no-op index run")?;
            prime_archive_fingerprint_cache_from_completed_checkpoint(
                &ctx.opts.db_path,
                &index_path,
            );
            return Ok(());
        }

        // Startup watch ingest defers WAL auto-checkpoints for bulk import.
        // Before entering the long-lived watch loop, restore the steady-state
        // policy so idle watch sessions do not leave auto-checkpointing
        // disabled indefinitely.
        restore_watch_steady_state_checkpoint_policy(&storage, ctx.opts.watch);
        if ctx.opts.watch {
            ctx.index_run_lock.set_mode(SearchMaintenanceMode::Watch)?;
        }

        let opts_clone = ctx.opts.clone();
        let state = Mutex::new(load_watch_state(&ctx.opts.data_dir));
        let storage = Rc::new(Mutex::new(storage));
        let storage_for_watch = Rc::clone(&storage);
        let should_preopen_tantivy_for_watch = ctx.opts.watch;
        let watch_once_defers_tantivy_open =
            watch_once_mode && !should_preopen_tantivy_for_watch && t_index.is_none();
        let t_index = Mutex::new(if should_preopen_tantivy_for_watch {
            Some(match t_index {
                Some(t_index) => t_index,
                None => open_lexical_index_with_heartbeat(&index_path, ctx.opts.progress.as_ref())
                    .with_context(|| {
                        format!(
                            "opening Tantivy index before entering watch mode for {}",
                            index_path.display()
                        )
                    })?,
            })
        } else {
            t_index
        });
        if watch_once_defers_tantivy_open {
            tracing::info!(
                index_path = %index_path.display(),
                "deferring Tantivy open until one-shot watch-once ingest has conversations"
            );
        }
        let index_path_for_watch = index_path.clone();

        // CASS #163 item 3: Periodically close and reopen the long-lived
        // watch storage handle to release accumulated connection/cache state.
        let watch_recycle_counter: std::cell::Cell<u32> = std::cell::Cell::new(0);
        let watch_recycle_interval: u32 = dotenvy::var("CASS_WATCH_RECYCLE_INTERVAL")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&v| v > 0) // guard against division by zero
            .unwrap_or(50); // recycle every 50 watch callbacks
        let watch_storage_reopen_pending = std::cell::Cell::new(false);
        let watch_lexical_checkpoint_pending = std::cell::Cell::new(false);

        // Semantic embedding cooldown state for watch mode.
        // The initial pass already embedded everything, so we start the clock
        // from now — the cooldown must elapse before the first incremental pass.
        let semantic_enabled = ctx.opts.semantic;
        let targeted_semantic_watch_once = targeted_semantic_watch_once && watch_once_mode;
        let embedder_id = ctx.opts.embedder.clone();
        let data_dir_for_semantic = ctx.opts.data_dir.clone();
        let pre_watch_semantic_conversations = if semantic_enabled {
            let storage = storage.lock().map_err(|err| {
                anyhow::anyhow!("storage lock poisoned before semantic watch-once: {err}")
            })?;
            count_total_conversations_exact(&storage)?
        } else {
            0
        };
        let semantic_cooldown = Duration::from_secs(
            dotenvy::var("CASS_WATCH_SEMANTIC_COOLDOWN_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(60),
        );
        let semantic_maintenance = std::cell::RefCell::new(WatchSemanticMaintenance::new(
            pre_watch_semantic_conversations,
        ));

        // Initialize stale detector for watch mode
        let stale_detector = Arc::new(StaleDetector::from_env());
        let stale_config = StaleConfig::from_env();
        if stale_config.is_enabled() {
            tracing::info!(
                action = ?stale_config.action,
                threshold_hours = stale_config.threshold_hours,
                check_interval_mins = stale_config.check_interval_mins,
                "stale detection enabled"
            );
        }

        // Clone detector for the callback
        let detector_clone = stale_detector.clone();

        if watch_once_mode {
            if ctx.opts.watch {
                // Explicit paths still run once when combined with --watch.
                // Preserve the already-published Watch mode while admitting
                // the callback under its finish-current-batch contract.
                drop(ctx.storage_cancellation_scope.take());
                if let Some(progress) = ctx.opts.progress.as_ref() {
                    progress.check_stop()?;
                }
            } else {
                ctx.enter_work_phase("index:scan")?;
            }
        }
        let watch_result = watch_sources(
            ctx.opts.watch_once_paths.clone(),
            watch_roots.clone(),
            event_channel,
            stale_detector,
            ctx.opts.progress.as_ref(),
            ctx.opts.watch_interval_secs,
            deferred_watch_sources,
            move |paths, roots, is_rebuild| {
                // A failed recycle leaves the old handle closed. Repair that
                // state before any callback can read or persist through it;
                // failure keeps this callback's sources queued for retry.
                reopen_watch_storage_if_pending(
                    &storage_for_watch,
                    &opts_clone.db_path,
                    &watch_storage_reopen_pending,
                    opts_clone.watch,
                    opts_clone.progress.as_ref(),
                )?;
                let semantic_retry_paths = if semantic_enabled && !targeted_semantic_watch_once {
                    if is_rebuild {
                        roots.iter().map(|(_, root)| root.path.clone()).collect()
                    } else {
                        paths.clone()
                    }
                } else {
                    Vec::new()
                };
                let active_source_filter = ActiveSessionSourceFilter::new(!watch_once_mode);
                let mut retry_sources = BTreeSet::new();
                let indexed = if is_rebuild {
                    // Reset stale detector on rebuild
                    detector_clone.reset();
                    // For rebuild, trigger reindex on all active roots
                    let indexed = reindex_full_watch_roots(
                        &opts_clone,
                        roots,
                        &state,
                        &storage_for_watch,
                        &t_index,
                        &index_path_for_watch,
                        &active_source_filter,
                    )
                    .map(|outcome| {
                        retry_sources.extend(outcome.retry_sources);
                        outcome.published_conversations
                    });
                    if !watch_once_mode
                        && (indexed.is_err() || indexed.as_ref().is_ok_and(|count| *count > 0))
                    {
                        watch_lexical_checkpoint_pending.set(true);
                    }
                    finalize_watch_reindex_result(
                        indexed,
                        &detector_clone,
                        opts_clone.progress.as_ref(),
                        "watch rebuild reindex",
                    )?
                } else if watch_once_mode {
                    let indexed = finalize_watch_once_reindex_result(
                        reindex_paths_with_semantic_delta(
                            &opts_clone,
                            paths,
                            roots,
                            &state,
                            &storage_for_watch,
                            &t_index,
                            &index_path_for_watch,
                            false,
                            None,
                            &active_source_filter,
                        )
                        .map(|outcome| {
                            retry_sources.extend(outcome.retry_sources);
                            outcome.indexed
                        }),
                        &detector_clone,
                        opts_clone.progress.as_ref(),
                        "watch incremental reindex",
                    )?;

                    // Only attempt segment merge when the scan actually
                    // ingested something. Empty watch scans must not wake the
                    // optimizer (#194), and targeted semantic watch-once must
                    // proceed directly to reconciliation rather than spending
                    // hours compacting an unrelated large lexical index (#336).
                    if should_optimize_tantivy_after_watch_once_ingest(
                        indexed,
                        targeted_semantic_watch_once,
                    ) && let Ok(mut guard) = t_index.lock()
                        && let Some(t_index) = guard.as_mut()
                        && let Err(e) = t_index.optimize_if_idle()
                    {
                        tracing::warn!(error = %e, "segment merge failed during watch");
                    }
                    if targeted_semantic_watch_once {
                        // The lexical ingest is complete. Park its progress
                        // atomics in the semantic-aware phase-0 state before
                        // scanning/copying a potentially million-record FSVI.
                        // Otherwise the generic phase-2 stall watchdog can
                        // abort a healthy targeted semantic repair after 300 s.
                        prepare_progress_for_semantic_build(opts_clone.progress.as_ref());
                        let stats = run_targeted_semantic_watch_once_publish(
                            &embedder_id,
                            &data_dir_for_semantic,
                            &storage_for_watch,
                            indexed,
                            pre_watch_semantic_conversations,
                        )?;
                        if opts_clone.build_hnsw {
                            if let Some(progress) = opts_clone.progress.as_ref() {
                                progress.set_phase_progress(INDEX_PHASE_SEMANTIC_HNSW, 0, 0);
                            }
                            maintain_requested_hnsw(&data_dir_for_semantic, &embedder_id)?;
                        }
                        record_semantic_watch_once_stats(opts_clone.progress.as_ref(), stats);
                    }
                    indexed
                } else {
                    let indexed = reindex_paths_with_semantic_delta(
                        &opts_clone,
                        paths,
                        roots,
                        &state,
                        &storage_for_watch,
                        &t_index,
                        &index_path_for_watch,
                        false,
                        None,
                        &active_source_filter,
                    )
                    .map(|outcome| {
                        retry_sources.extend(outcome.retry_sources);
                        outcome.published_conversations
                    });
                    if indexed.is_err() || indexed.as_ref().is_ok_and(|count| *count > 0) {
                        watch_lexical_checkpoint_pending.set(true);
                    }
                    let indexed = finalize_watch_reindex_result(
                        indexed,
                        &detector_clone,
                        opts_clone.progress.as_ref(),
                        "watch incremental reindex",
                    )?;

                    // Merge Tantivy segments if idle conditions are met.
                    // Without this, each reindex_paths() commit creates a new
                    // segment, leading to unbounded accumulation over weeks of
                    // continuous watch mode operation. The cooldown logic inside
                    // optimize_if_idle() (300s, 4-segment threshold) prevents
                    // over-merging. See issue #87.
                    // Also gated on `indexed > 0` so empty watch scans don't
                    // wake the optimizer. See issue #194.
                    if indexed > 0
                        && let Ok(mut guard) = t_index.lock()
                        && let Some(t_index) = guard.as_mut()
                        && let Err(e) = t_index.optimize_if_idle()
                    {
                        tracing::warn!(error = %e, "segment merge failed during watch");
                    }
                    indexed
                };

                if !watch_once_mode && watch_lexical_checkpoint_pending.get() {
                    let refreshed = (|| -> Result<bool> {
                        let storage = storage_for_watch
                            .lock()
                            .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?;
                        let index = t_index
                            .lock()
                            .map_err(|_| anyhow::anyhow!("lexical index lock poisoned"))?;
                        let Some(index) = index.as_ref() else {
                            return Ok(false);
                        };
                        refresh_completed_watch_lexical_checkpoint(
                            &storage,
                            index,
                            &opts_clone.db_path,
                            &index_path_for_watch,
                        )
                    })();
                    match refreshed {
                        Ok(true) => watch_lexical_checkpoint_pending.set(false),
                        Ok(false) => {}
                        Err(error) => tracing::warn!(
                            %error,
                            "watch lexical checkpoint refresh failed; retaining pending refresh"
                        ),
                    }
                }

                // Reconcile every pending canonical change, including earlier
                // cooldown-skipped callbacks, before publishing current coverage.
                if semantic_enabled && !targeted_semantic_watch_once {
                    let mut maintenance = semantic_maintenance.borrow_mut();
                    maintenance.record_ingest(indexed, semantic_retry_paths);
                    match maintenance.reconcile_if_due(
                        &embedder_id,
                        &data_dir_for_semantic,
                        &storage_for_watch,
                        semantic_cooldown,
                        opts_clone.progress.as_ref(),
                    ) {
                        Ok(Some(stats)) => tracing::info!(
                            selected_docs = stats.selected_docs,
                            embedded_docs = stats.embedded_docs,
                            reason = %stats.reason,
                            "watch semantic coverage reconciled and published"
                        ),
                        Ok(None) => {}
                        Err(error) => tracing::warn!(
                            %error,
                            "watch semantic maintenance failed; retaining sources for retry"
                        ),
                    }
                }

                // CASS #163 item 3: Periodically recycle the long-lived watch
                // storage handle to release retained connection/cache state.
                let count = watch_recycle_counter.get().wrapping_add(1);
                watch_recycle_counter.set(count);
                if count.is_multiple_of(watch_recycle_interval) {
                    close_watch_storage_for_recycle(
                        &storage_for_watch,
                        &watch_storage_reopen_pending,
                    )?;
                    reopen_watch_storage_if_pending(
                        &storage_for_watch,
                        &opts_clone.db_path,
                        &watch_storage_reopen_pending,
                        opts_clone.watch,
                        opts_clone.progress.as_ref(),
                    )?;
                    tracing::debug!(
                        cycle = count,
                        "recycled long-lived storage handle"
                    );
                }

                let mut deferred = active_source_filter.take_deferred_sources();
                // A long-lived watch retries these itself; a single
                // `--watch-once` cycle ends here, so its report must name them.
                if watch_once_mode {
                    record_deferred_active_sources(opts_clone.progress.as_ref(), &deferred);
                }
                deferred.extend(retry_sources);
                deferred.extend(semantic_maintenance.borrow().pending_paths.iter().cloned());
                Ok(deferred)
            },
        );

        let close_result =
            release_watch_storage_after_index(storage, &ctx.opts.db_path, "watch indexing session");
        if let Err(err) = watch_result {
            if let Err(close_err) = close_result {
                tracing::warn!(
                    error = %close_err,
                    db_path = %ctx.opts.db_path.display(),
                    "failed to close canonical db cleanly after watch indexing error"
                );
            }
            return Err(err);
        }
        close_result?;
        prime_archive_fingerprint_cache_from_completed_checkpoint(&ctx.opts.db_path, &index_path);
        return Ok(());
    }

    // #319/#321: `close_storage_after_index` runs the final WAL checkpoint of
    // the deferred bulk-ingest WAL — a synchronous, `!Send` SQLite
    // `conn.close()` + `wal_checkpoint(TRUNCATE)` that executes on THIS thread
    // and cannot report progress. On a large corpus (the report: ~1.1 GB /
    // ~290k-frame WAL) it legitimately takes minutes, especially on macOS.
    // Signal the stall watchdog that we have entered that finalize window so it
    // does not misread the quiescent, phase-0, current==total state as a #297
    // finalize wedge and kill the process (exit 70) mid-checkpoint — which would
    // strand the un-truncated WAL and leave the DB malformed (#296/#321). The
    // watchdog still bounds this window (see `index_finalize_abort_threshold`),
    // so a genuinely stuck finalize is still aborted.
    if let Some(progress) = ctx.opts.progress.as_ref() {
        progress.finalizing.store(true, Ordering::Relaxed);
    }
    close_storage_after_index(storage, &ctx.opts.db_path, "index run")?;
    prime_archive_fingerprint_cache_from_completed_checkpoint(&ctx.opts.db_path, &index_path);
    Ok(())
}
