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

pub(crate) fn parse_models_backfill_tier(
    raw: &str,
) -> CliResult<crate::search::semantic_manifest::TierKind> {
    use crate::search::semantic_manifest::TierKind;

    match raw.trim().to_ascii_lowercase().as_str() {
        "fast" => Ok(TierKind::Fast),
        "quality" => Ok(TierKind::Quality),
        other => Err(CliError {
            code: 2,
            kind: CliErrorKind::Usage.kind_str(),
            message: format!("Unknown semantic tier '{other}'."),
            hint: Some("Use --tier fast or --tier quality".into()),
            retryable: false,
        }),
    }
}

pub(crate) fn resolve_semantic_index_embedder(raw: Option<&str>) -> String {
    if let Some(requested) = raw {
        return requested.trim().to_string();
    }
    let requested = "fastembed";

    let policy = crate::search::policy::SemanticPolicy::resolve(
        &crate::search::policy::CliSemanticOverrides::default(),
    );
    if policy
        .quality_tier_embedder
        .trim()
        .eq_ignore_ascii_case("hash")
    {
        return "hash".to_string();
    }
    // A leftover legacy `external` policy value is unrecognized here and
    // falls through to the local default like any other unknown name.
    let Some(policy_embedder) = crate::search::fastembed_embedder::FastEmbedder::canonical_name(
        &policy.quality_tier_embedder,
    ) else {
        return requested.to_string();
    };
    if policy_embedder == "minilm" {
        requested.to_string()
    } else {
        policy_embedder.to_string()
    }
}

pub(crate) fn semantic_identity_backfill_fingerprint(
    canonical_db_fingerprint: &str,
    identity_generation: Option<&str>,
) -> String {
    identity_generation.map_or_else(
        || canonical_db_fingerprint.to_owned(),
        |generation| {
            let generation_hash = blake3::hash(generation.as_bytes()).to_hex();
            format!("identity-v1:{generation_hash}:{canonical_db_fingerprint}")
        },
    )
}

pub(crate) struct ModelsBackfillRunOptions {
    pub(crate) scheduled: bool,
    pub(crate) max_batches: u32,
}

pub(crate) fn run_models_backfill(
    tier_raw: &str,
    embedder_override: Option<&str>,
    batch_conversations: usize,
    options: ModelsBackfillRunOptions,
    data_dir_override: Option<PathBuf>,
    db_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
) -> CliResult<()> {
    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });
    let shutdown = IndexShutdownSignals::new(true).map_err(|error| CliError {
        code: 5,
        kind: CliErrorKind::SemanticBackfill.kind_str(),
        message: format!("Failed to install backfill shutdown signals: {error}"),
        hint: None,
        retryable: true,
    })?;
    // The provider polls the same signal streams between durable batches.
    // There is no detached listener or inference worker retaining the archive lock.
    let shutdown = std::sync::Arc::new(std::sync::Mutex::new(shutdown));
    let exit_code = || {
        shutdown
            .lock()
            .map_or(Some(130), |mut signals| signals.poll_exit_code())
    };
    let cancelled: crate::indexer::semantic::CancelCheck = {
        let shutdown = std::sync::Arc::clone(&shutdown);
        std::sync::Arc::new(move || {
            shutdown
                .lock()
                .map_or(true, |mut signals| signals.poll_exit_code().is_some())
        })
    };
    let mut retained = RetainedBackfillModel {
        cancelled: Some(cancelled),
        ..RetainedBackfillModel::default()
    };
    let mut attempted = 0u32;
    let mut completed = 0u32;
    let mut last_report = None;
    let mut last_completed_batch: Option<serde_json::Value> = None;
    let mut failure = None;

    loop {
        // A received signal stops at a durable batch boundary. Each completed
        // helper call has already dropped its archive, manifest and lock.
        // The streams register 130 (SIGINT) and 143 (SIGTERM); the literal
        // per-arm `code:` fields are load-bearing for the emitted-exit-code
        // scanners (spec_exit_codes_documented, golden_error_envelope), which
        // only see `code: <digits>` directly above `kind:`.
        if let Some(code) = exit_code() {
            failure = Some(match code {
                143 => CliError {
                    code: 143,
                    kind: CliErrorKind::SemanticBackfill.kind_str(),
                    message: "Semantic backfill cancelled at a batch boundary".to_string(),
                    hint: Some("Rerun the command to resume from the saved checkpoint".into()),
                    retryable: true,
                },
                _ => CliError {
                    code: 130,
                    kind: CliErrorKind::SemanticBackfill.kind_str(),
                    message: "Semantic backfill cancelled at a batch boundary".to_string(),
                    hint: Some("Rerun the command to resume from the saved checkpoint".into()),
                    retryable: true,
                },
            });
            break;
        }
        if attempted >= options.max_batches {
            break;
        }
        attempted += 1;
        let result = run_models_backfill_batch(
            tier_raw,
            embedder_override,
            batch_conversations,
            options.scheduled,
            data_dir_override.clone(),
            db_override.clone(),
            &mut retained,
        );
        match result {
            Ok(report) => {
                let made_progress = last_completed_batch.as_ref().is_none_or(|previous| {
                    previous.get("last_offset") != report.get("last_offset")
                        || previous.get("conversations_processed")
                            != report.get("conversations_processed")
                });
                // Paused/disabled scheduler reports have no admitted batch.
                if report.get("conversations_processed").is_some() {
                    completed += 1;
                    last_completed_batch = Some(report.clone());
                }
                let continue_backfill = report.get("checkpoint_saved")
                    == Some(&serde_json::Value::Bool(true))
                    && report.get("published") != Some(&serde_json::Value::Bool(true))
                    && report.get("unchanged") != Some(&serde_json::Value::Bool(true))
                    && made_progress;
                last_report = Some(report);
                // Still honor a signal delivered during the final batch.
                if !continue_backfill && exit_code().is_none() {
                    break;
                }
            }
            Err(error) => {
                // Same literal-code contract as the boundary-cancel arm above:
                // the signal-derived code is spelled per arm so the
                // emitted-exit-code scanners see both 130 and 143.
                let error = match exit_code() {
                    Some(143) => CliError {
                        code: 143,
                        kind: CliErrorKind::SemanticBackfill.kind_str(),
                        message:
                            "Semantic backfill cancelled; unfinished batch was not checkpointed"
                                .into(),
                        hint: Some(
                            "Rerun the command to resume from the last durable checkpoint".into(),
                        ),
                        retryable: true,
                    },
                    Some(_) => CliError {
                        code: 130,
                        kind: CliErrorKind::SemanticBackfill.kind_str(),
                        message:
                            "Semantic backfill cancelled; unfinished batch was not checkpointed"
                                .into(),
                        hint: Some(
                            "Rerun the command to resume from the last durable checkpoint".into(),
                        ),
                        retryable: true,
                    },
                    None => error,
                };
                if last_report.is_none() && !matches!(error.code, 130 | 143) {
                    return Err(error);
                }
                failure = Some(error);
                break;
            }
        }
    }

    let mut report = if let Some(error) = failure.as_ref() {
        serde_json::json!({
            "status": if matches!(error.code, 130 | 143) { "cancelled" } else { "failed" },
            "tier": tier_raw,
            "last_completed_batch": last_completed_batch,
            "error": {
                "code": error.code,
                "kind": error.kind,
                "message": error.message,
                "hint": error.hint,
                "retryable": error.retryable,
            },
        })
    } else {
        let mut report =
            last_report.unwrap_or_else(|| serde_json::json!({"status": "idle", "tier": tier_raw}));
        if report.get("conversations_processed").is_none()
            && let Some(completed_batch) = last_completed_batch
        {
            report["last_completed_batch"] = completed_batch;
        }
        report
    };
    report["batches_attempted"] = attempted.into();
    report["batches_completed"] = completed.into();
    report["model_initializations"] = retained.initializations.into();

    if let Some(fmt) = structured_format {
        output_structured_value(report, fmt)?;
    } else {
        use colored::Colorize;
        println!("{}", "Semantic backfill".bold());
        println!(
            "  Status: {}",
            report["status"].as_str().unwrap_or("unknown")
        );
        println!("  Tier: {}", tier_raw);
        println!("  Batches: {completed} completed / {attempted} attempted");
        let batch = report.get("last_completed_batch").unwrap_or(&report);
        for (label, key) in [
            ("Embedder", "embedder_id"),
            ("Embedded docs", "embedded_docs"),
            ("Conversations processed", "conversations_processed"),
            ("Total conversations", "total_conversations"),
            ("Last offset", "last_offset"),
            ("Index", "index_path"),
            ("Manifest", "manifest_path"),
        ] {
            if let Some(value) = batch.get(key) {
                if let Some(text) = value.as_str() {
                    println!("  {label}: {text}");
                } else {
                    println!("  {label}: {value}");
                }
            }
        }
        if let Some(next_step) = report.get("next_step").and_then(serde_json::Value::as_str) {
            println!("  {next_step}");
        }
    }
    match failure {
        Some(error) if structured_format.is_some() => Err(CliError::already_reported_from(&error)),
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// The one semantic model a bounded backfill keeps across its batches, and how
/// many times it was actually constructed (GH #471: the receipt must show a
/// per-batch reload, not merely that a model exists).
#[derive(Default)]
pub(crate) struct RetainedBackfillModel {
    pub(crate) indexer: Option<crate::indexer::semantic::SemanticIndexer>,
    pub(crate) initializations: u32,
    pub(crate) cancelled: Option<crate::indexer::semantic::CancelCheck>,
}

pub(crate) fn run_models_backfill_batch(
    tier_raw: &str,
    embedder_override: Option<&str>,
    batch_conversations: usize,
    scheduled: bool,
    data_dir_override: Option<PathBuf>,
    db_override: Option<PathBuf>,
    retained: &mut RetainedBackfillModel,
) -> CliResult<serde_json::Value> {
    use crate::indexer::semantic::{
        SemanticBackfillSchedulerSignals, SemanticBackfillStoragePlan, SemanticIndexer,
        semantic_backfill_scheduler_decision,
    };
    use crate::search::model_download::ModelManifest;
    use crate::search::policy::{CliSemanticOverrides, SemanticPolicy};
    use crate::search::semantic_manifest::{SemanticManifest, TierKind};
    use crate::storage::sqlite::{FrankenStorage, SemanticIdentityTier};

    if batch_conversations == 0 {
        return Err(CliError {
            code: 2,
            kind: CliErrorKind::Usage.kind_str(),
            message: "--batch-conversations must be greater than zero".to_string(),
            hint: Some("Use a small positive batch such as --batch-conversations 64".into()),
            retryable: false,
        });
    }

    let tier = parse_models_backfill_tier(tier_raw)?;
    let data_dir = resolve_data_dir(&data_dir_override, db_override.as_ref());
    let db_path = db_override.unwrap_or_else(|| data_dir.join("agent_search.db"));
    if !db_path.is_file() {
        return Err(CliError {
            code: 3,
            kind: CliErrorKind::MissingIndex.kind_str(),
            message: format!("cass database not found: {}", db_path.display()),
            hint: Some("Run 'cass index --full' before semantic backfill".into()),
            retryable: true,
        });
    }

    let embedder_type = embedder_override
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| match tier {
            TierKind::Fast => "hash".to_string(),
            TierKind::Quality => resolve_semantic_index_embedder(None),
        });
    // `--embedder external` never reaches this point: the removed-flag table
    // rejects that value before dispatch. Any other unknown name fails here.
    let embedder_valid = embedder_type == "hash"
        || crate::search::fastembed_embedder::FastEmbedder::canonical_name(&embedder_type)
            .is_some();
    if !embedder_valid {
        return Err(CliError {
            code: 20,
            kind: CliErrorKind::Model.kind_str(),
            message: format!("Unknown embedder '{}'.", embedder_type),
            hint: Some(
                "Use --embedder hash, --embedder minilm (alias fastembed), or --embedder multilingual-minilm"
                    .into(),
            ),
            retryable: false,
        });
    }

    let scheduler_decision = if scheduled {
        let policy = SemanticPolicy::resolve(&CliSemanticOverrides::default());
        let signals = SemanticBackfillSchedulerSignals::from_env();
        Some(semantic_backfill_scheduler_decision(
            &policy,
            batch_conversations,
            &signals,
        ))
    } else {
        None
    };

    if let Some(decision) = scheduler_decision.as_ref()
        && !decision.should_run()
    {
        let status = decision.state.as_str();
        let next_step = decision.reason.next_step();
        return Ok(serde_json::json!({
            "status": status,
            "next_step": next_step,
            "tier": tier.as_str(),
            "embedder_id": embedder_type,
            "data_dir": data_dir.display().to_string(),
            "db_path": db_path.display().to_string(),
            "batch_conversations_limit": batch_conversations,
            "scheduler": decision,
        }));
    }

    let effective_batch_conversations = scheduler_decision
        .as_ref()
        .map_or(batch_conversations, |decision| {
            decision.scheduled_batch_conversations
        });

    // GH458: own the canonical maintenance lock before any writable open,
    // fingerprint or manifest read, and retain it through publish and the
    // semantic identity marker completion below.
    let maintenance_guard = crate::indexer::acquire_semantic_backfill_lock(&data_dir, &db_path)
        .map_err(|error| {
            let rendered = format!("{error:#}");
            if error_chain_indicates_active_cass_index(&rendered) {
                return active_index_run_details(&data_dir, &db_path)
                    .map(|details| details.to_cli_error())
                    .unwrap_or_else(|| index_storage_contention_cli_error(&rendered));
            }
            CliError {
                code: 5,
                kind: CliErrorKind::Storage.kind_str(),
                message: format!(
                    "Failed to acquire semantic backfill maintenance lock: {rendered}"
                ),
                hint: Some("Check permissions under the cass data directory".into()),
                retryable: true,
            }
        })?;

    // Direct --scheduled commands must preserve ownership too, not just workers
    // launched by the nightly parent. Read strictly under the maintenance lock
    // before provider preflight or a storage open can have side effects. Reuse
    // the admitted snapshot below so a concurrent change cannot become a newly
    // authorized checkpoint after preflight; the artifact lease revalidates it.
    let scheduled_manifest = if scheduled
        || matches!(
            std::env::var(crate::indexer::semantic::SCHEDULE_PRESERVE_CHECKPOINT).as_deref(),
            Ok("1")
        ) {
        Some(
            crate::indexer::semantic::load_scheduled_checkpoint(
                &data_dir,
                tier,
                &embedder_type,
                retained.indexer.as_ref().map(|indexer| indexer.embedder_id()),
            )
            .map_err(|error| CliError {
                code: 5,
                kind: CliErrorKind::SemanticBackfill.kind_str(),
                message: format!("Scheduled semantic checkpoint admission failed: {error:#}"),
                hint: Some(
                    "Resume the checkpoint's original tier/provider, or explicitly rebuild in the foreground; scheduled work never replaces a foreign checkpoint".into(),
                ),
                retryable: false,
            })?,
        )
    } else {
        None
    };

    // Refuse unavailable models before opening the archive: even a current-
    // schema storage open can change its shared-memory sidecar. Keep model
    // admission inside the maintenance lock so index-busy retains precedence.
    let RetainedBackfillModel {
        indexer: retained_indexer,
        initializations: model_initializations,
        cancelled,
    } = retained;
    let indexer = match retained_indexer {
        Some(indexer) => indexer,
        vacant => vacant.insert({
            let indexer = SemanticIndexer::new_with_cancel(
                &embedder_type,
                Some(&data_dir),
                cancelled
                    .clone()
                    .unwrap_or_else(|| std::sync::Arc::new(|| false)),
            )
            .map_err(|e| CliError {
                code: 20,
                kind: CliErrorKind::Model.kind_str(),
                message: format!("Failed to initialize semantic embedder '{embedder_type}': {e}"),
                hint: Some(if embedder_type == "fastembed" {
                    "Run 'cass models install --from-file <DIR>' or retry with --embedder hash"
                        .into()
                } else {
                    "Use --embedder hash or install the selected embedder model".into()
                }),
                retryable: embedder_type != "hash",
            })?;
            // GH #471: count real constructions, so a regression that reloads
            // the model per batch reports it instead of a presence flag.
            *model_initializations = model_initializations.saturating_add(1);
            indexer
        }),
    };

    let storage = crate::storage::sqlite::open_current_schema_storage_with_timeout(
        &db_path,
        std::time::Duration::from_secs(10),
    )
    .and_then(|current| match current {
        Some(storage) => Ok(storage),
        None => FrankenStorage::open(&db_path),
    })
    .map_err(|e| CliError {
        code: 5,
        kind: CliErrorKind::Storage.kind_str(),
        message: format!("Failed to open cass database {}: {e}", db_path.display()),
        hint: Some("Run 'cass health --json' to inspect the archive database".into()),
        retryable: true,
    })?;
    // A corrupt ledger is not permission to discard durable progress.
    let mut manifest = if let Some(manifest) = scheduled_manifest {
        Ok(manifest)
    } else {
        SemanticManifest::load_or_default(&data_dir)
    }
    .map_err(|e| CliError {
        code: 5,
        kind: CliErrorKind::SemanticManifest.kind_str(),
        message: format!("Failed to load semantic manifest: {e}"),
        hint: Some("Check permissions under the cass data directory".into()),
        retryable: true,
    })?;
    let model_manifest =
        crate::search::fastembed_embedder::FastEmbedder::canonical_name(&embedder_type)
            .and_then(ModelManifest::for_embedder)
            .unwrap_or_else(ModelManifest::minilm_v2);
    let model_revision = if embedder_type == "hash" {
        "hash".to_string()
    } else {
        model_manifest.revision.clone()
    };
    // GH458: a fully unchanged archive must not pay a canonical count scan
    // just to discover that no backfill work is needed. The completed cache
    // binds the open descriptor, real WAL, artifact and producer; any mismatch
    // falls back to a strict fingerprint on this already-open handle.
    let canonical_db_fingerprint = indexer
        .completed_backfill_fingerprint(&storage, &data_dir, &manifest, tier, &model_revision)
        .and_then(|cached| match cached {
            Some(fingerprint) => Ok(fingerprint),
            None => crate::indexer::lexical_storage_fingerprint_for_storage(&storage),
        })
        .map_err(|e| CliError {
            code: 5,
            kind: CliErrorKind::StorageFingerprint.kind_str(),
            message: format!("Failed to fingerprint cass database {}: {e:#}", db_path.display()),
            hint: Some(
                "Run 'cass doctor check --json' if the archive is corrupt; index --force-rebuild only rebuilds derived assets from a healthy canonical archive."
                    .into(),
            ),
            retryable: true,
        })?;
    // Identity invalidation follows the physical vector space that query
    // serving selects, not the user-facing manifest tier. Tests and operators
    // may deliberately backfill the quality tier with the hash embedder.
    let identity_tier = if indexer.embedder_id() == "fnv1a-384" {
        SemanticIdentityTier::Fast
    } else {
        SemanticIdentityTier::Quality
    };
    let identity_rebuild_generation = storage
        .semantic_identity_rebuild_generation(identity_tier)
        .map_err(|e| CliError {
            code: 5,
            kind: CliErrorKind::Storage.kind_str(),
            message: format!("Failed to inspect semantic identity rebuild state: {e}"),
            hint: Some(
                "Retry the semantic backfill; stale semantic serving remains fail-closed".into(),
            ),
            retryable: true,
        })?;
    // The ordinary content fingerprint intentionally contains only
    // counts/max rowids, so a Pi -> OMP identity rewrite does not invalidate
    // an in-progress checkpoint. Namespace checkpoint and staging identity by
    // the durable migration generation; a stale Pi-era cursor can then never
    // resume into the rebuilt artifact. The published manifest is rebound to
    // the canonical content fingerprint below before serving is re-enabled.
    let backfill_db_fingerprint = semantic_identity_backfill_fingerprint(
        &canonical_db_fingerprint,
        identity_rebuild_generation.as_deref(),
    );

    // Sub-fix 1 for cass#257: open a JSONL progress sink whose
    // destination is taken from `CASS_SEMANTIC_PROGRESS_JSONL`. The
    // sink is silent when the env var is unset, so behaviour for
    // existing operators is unchanged. The sink threads through to
    // selection / packet replay / embed / staging / checkpoint /
    // publish events.
    let progress_sink = crate::indexer::semantic_progress::SemanticProgressSink::open(
        tier.as_str(),
        indexer.embedder_id(),
    )
    .with_progress_atomic(maintenance_guard.progress_atomic());
    let outcome = indexer
        .run_capped_backfill_from_storage_with_sink(
            &storage,
            &data_dir,
            &mut manifest,
            SemanticBackfillStoragePlan {
                tier,
                db_fingerprint: backfill_db_fingerprint.clone(),
                model_revision,
                max_conversations: effective_batch_conversations,
            },
            &progress_sink,
        )
        .map_err(|e| CliError {
            code: 5,
            kind: CliErrorKind::SemanticBackfill.kind_str(),
            message: format!("Semantic backfill failed: {e}"),
            hint: Some(
                "Retry the command; resumable checkpoints are kept in the semantic manifest".into(),
            ),
            retryable: true,
        })?;

    if outcome.published && identity_rebuild_generation.is_some() {
        let published_artifact = match outcome.tier {
            TierKind::Fast => manifest.fast_tier.as_mut(),
            TierKind::Quality => manifest.quality_tier.as_mut(),
        }
        .ok_or_else(|| CliError {
            code: 5,
            kind: CliErrorKind::SemanticManifest.kind_str(),
            message: "Semantic identity rebuild published without a tier artifact".to_string(),
            hint: Some(
                "Retry the semantic backfill; stale semantic serving remains fail-closed".into(),
            ),
            retryable: true,
        })?;
        if published_artifact.db_fingerprint != backfill_db_fingerprint {
            return Err(CliError {
                code: 5,
                kind: CliErrorKind::SemanticManifest.kind_str(),
                message: "Semantic identity rebuild published an unexpected generation".to_string(),
                hint: Some(
                    "Retry the semantic backfill; stale semantic serving remains fail-closed"
                        .into(),
                ),
                retryable: true,
            });
        }
        published_artifact.db_fingerprint = canonical_db_fingerprint.clone();
        manifest.refresh_backlog(outcome.total_conversations, &canonical_db_fingerprint);
        manifest.save(&data_dir).map_err(|e| CliError {
            code: 5,
            kind: CliErrorKind::SemanticManifest.kind_str(),
            message: format!("Failed to finalize semantic identity manifest: {e}"),
            hint: Some(
                "Retry the semantic backfill; stale semantic serving remains fail-closed".into(),
            ),
            retryable: true,
        })?;
        crate::indexer::semantic::invalidate_identity_stale_semantic_shards(
            &data_dir,
            indexer.embedder_id(),
        )
        .map_err(|e| CliError {
            code: 5,
            kind: CliErrorKind::SemanticManifest.kind_str(),
            message: format!("Failed to revoke stale semantic shard generations: {e}"),
            hint: Some(
                "Retry the semantic backfill; stale semantic serving remains fail-closed".into(),
            ),
            retryable: true,
        })?;
        storage
            .complete_semantic_identity_rebuild(identity_tier)
            .map_err(|e| CliError {
                code: 5,
                kind: CliErrorKind::Storage.kind_str(),
                message: format!("Failed to complete semantic identity rebuild: {e}"),
                hint: Some(
                    "Retry the semantic backfill; stale semantic serving remains fail-closed"
                        .into(),
                ),
                retryable: true,
            })?;
    }

    let progress_pct = outcome.progress_pct();
    let status = if outcome.unchanged {
        "unchanged"
    } else if outcome.published {
        "published"
    } else if outcome.checkpoint_saved {
        "checkpointed"
    } else {
        "idle"
    };
    let next_step = if outcome.published {
        "semantic tier is ready"
    } else if outcome.checkpoint_saved {
        "rerun the same command to continue the resumable backfill"
    } else {
        "no pending canonical conversations for this tier"
    };
    let backlog = serde_json::json!({
        "total_conversations": manifest.backlog.total_conversations,
        "fast_tier_processed": manifest.backlog.fast_tier_processed,
        "quality_tier_processed": manifest.backlog.quality_tier_processed,
        "computed_at_ms": manifest.backlog.computed_at_ms,
    });
    Ok(serde_json::json!({
        "status": status,
        "next_step": next_step,
        "tier": outcome.tier.as_str(),
        "embedder_id": outcome.embedder_id,
        "data_dir": data_dir.display().to_string(),
        "db_path": db_path.display().to_string(),
        "batch_conversations_limit": effective_batch_conversations,
        "requested_batch_conversations_limit": batch_conversations,
        "scheduler": scheduler_decision,
        "embedded_docs": outcome.embedded_docs,
        "conversations_processed": outcome.conversations_processed,
        "total_conversations": outcome.total_conversations,
        "progress_pct": progress_pct,
        "last_offset": outcome.last_offset,
        "checkpoint_saved": outcome.checkpoint_saved,
        "published": outcome.published,
        "unchanged": outcome.unchanged,
        "index_path": outcome.index_path.display().to_string(),
        "manifest_path": outcome.manifest_path.display().to_string(),
        "backlog": backlog,
    }))
}
