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

/// Handle models subcommands
pub(crate) fn run_models_command(cmd: ModelsCommand, cli: &Cli) -> CliResult<()> {
    match cmd {
        ModelsCommand::Status { json } => {
            let structured_format = resolve_subcommand_structured_format(cli, json);
            run_models_status(structured_format)
        }
        ModelsCommand::Install {
            model,
            from_file,
            data_dir,
        } => run_models_install(&model, &from_file, data_dir),
        ModelsCommand::Verify {
            model,
            data_dir,
            json,
        } => {
            let structured_format = resolve_subcommand_structured_format(cli, json);
            run_models_verify(&model, data_dir, structured_format)
        }
        ModelsCommand::Backfill {
            tier,
            embedder,
            batch_conversations,
            max_batches,
            scheduled,
            data_dir,
            db,
            json,
        } => {
            let structured_format = resolve_subcommand_structured_format(cli, json);
            run_models_backfill(
                &tier,
                embedder.as_deref(),
                batch_conversations,
                ModelsBackfillRunOptions {
                    scheduled,
                    max_batches,
                },
                data_dir,
                db.or_else(|| cli.db.clone()),
                structured_format,
            )
        }
        ModelsCommand::BuildHnsw {
            tier,
            check,
            force,
            data_dir,
            json,
        } => {
            let structured_format = resolve_subcommand_structured_format(cli, json);
            run_models_build_hnsw(tier.as_deref(), check, force, data_dir, structured_format)
        }
        ModelsCommand::Remove {
            model,
            yes,
            data_dir,
        } => run_models_remove(&model, yes, data_dir),
    }
}

/// Show semantic model installation status.
///
/// Reports the default MiniLM and explicit multilingual MiniLM embedders
/// implemented by the native backend.
/// The "active" model is the one resolved from policy
/// (`quality_tier_embedder`) — that is what `cass index` and `cass search`
/// will actually use.
pub(crate) fn run_models_status(output_format: Option<RobotFormat>) -> CliResult<()> {
    use crate::search::fastembed_embedder::FastEmbedder;
    use crate::search::model_download::{
        ModelAcquisitionPolicy, ModelManifest, classify_model_cache, model_file_path,
    };
    use crate::search::policy::{CliSemanticOverrides, SemanticPolicy};

    let data_dir = default_data_dir();
    let policy = SemanticPolicy::resolve(&CliSemanticOverrides::default());
    let acquisition_policy = ModelAcquisitionPolicy::from_semantic_policy(&policy);

    // Hardened model-acquisition contract (bead 0qxwg): cheap host loadability
    // (never loads ONNX — safe on any host) folded with the on-disk policy
    // inputs the filesystem-only probe needs. The same inputs drive every
    // model's `model_acquisition` block so the report cannot drift per model.
    let acq_runtime = crate::search::model_acquisition::RuntimeLoadability::probe_cheap_host();
    let acq_policy_inputs = crate::search::model_acquisition::OnDiskPolicyInputs {
        policy_enabled: acquisition_policy.install_enabled,
        budget_max_bytes: acquisition_policy.max_model_bytes,
    };

    // Canonicalize the policy's quality_tier_embedder to a registry name
    // implemented by the native backend. Unsupported historical aliases remain
    // visible in `policy_quality_tier_embedder`, but never become active.
    let policy_embedder = policy.quality_tier_embedder.as_str();
    let active_registry_name = FastEmbedder::canonical_name(policy_embedder);

    // Per-model status snapshot.
    struct ModelStatus {
        registry_name: &'static str,
        manifest: ModelManifest,
        model_dir: PathBuf,
        cache_report: crate::search::model_download::ModelCacheReport,
        total_size: u64,
        installed_size: u64,
        files: Vec<serde_json::Value>,
        /// Hardened model-acquisition projection (state, runtime, source,
        /// cost_class, skipped_network_reason, next_command, fingerprint, …).
        acquisition: serde_json::Value,
    }

    let known: &[&str] = &["minilm", "multilingual-minilm"];
    let mut statuses: Vec<ModelStatus> = Vec::with_capacity(known.len());
    for name in known {
        let Some(manifest) = ModelManifest::for_embedder(name) else {
            continue;
        };
        let model_dir = if active_registry_name == Some(*name) {
            FastEmbedder::runtime_model_dir_for(&data_dir, name)
        } else {
            FastEmbedder::model_dir_for(&data_dir, name)
        };
        let Some(model_dir) = model_dir else {
            continue;
        };
        let cache_report = classify_model_cache(&model_dir, &manifest, &acquisition_policy);
        let total_size = manifest.total_size();
        let mut installed_size: u64 = 0;
        let mut file_info: Vec<serde_json::Value> = Vec::with_capacity(manifest.files.len());
        for mfile in &manifest.files {
            let file_path = model_file_path(&model_dir, mfile);
            let exists = file_path.is_some();
            let size = if let Some(path) = file_path.as_ref() {
                path.metadata().map(|m| m.len()).unwrap_or(0)
            } else {
                0
            };
            if exists {
                installed_size += size;
            }
            file_info.push(serde_json::json!({
                "name": mfile.name,
                "local_name": mfile.local_name(),
                "actual_path": file_path.as_ref().map(|path| path.display().to_string()),
                "expected_size": mfile.size,
                "actual_size": size,
                "exists": exists,
                "size_match": exists && size == mfile.size,
            }));
        }
        let acquisition = crate::search::model_acquisition::live_acquisition_block(
            &model_dir,
            &manifest,
            acq_runtime,
            acq_policy_inputs,
            name,
        );
        statuses.push(ModelStatus {
            registry_name: name,
            manifest,
            model_dir,
            cache_report,
            total_size,
            installed_size,
            files: file_info,
            acquisition,
        });
    }

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    let active_status =
        active_registry_name.and_then(|name| statuses.iter().find(|s| s.registry_name == name));

    if let Some(_fmt) = structured_format {
        let models_json: Vec<serde_json::Value> = statuses
            .iter()
            .map(|s| {
                serde_json::json!({
                    "registry_name": s.registry_name,
                    "model_id": s.manifest.id,
                    "model_dir": s.model_dir.display().to_string(),
                    "active": active_registry_name == Some(s.registry_name),
                    "installed": s.cache_report.is_usable(),
                    "state": s.cache_report.state_code(),
                    "state_detail": s.cache_report.state.summary(),
                    "next_step": s.cache_report.state.next_step(s.registry_name),
                    "revision": s.manifest.revision,
                    "license": s.manifest.license,
                    "total_size_bytes": s.total_size,
                    "installed_size_bytes": s.cache_report.installed_size_bytes,
                    "observed_file_bytes": s.installed_size,
                    "policy_source": s.cache_report.policy_source.as_str(),
                    "cache_lifecycle": &s.cache_report,
                    "model_acquisition": &s.acquisition,
                    "files": s.files,
                })
            })
            .collect();

        // Top-level fields describe the *active* model — preserves the
        // original single-model JSON shape for any consumer that reads
        // `model_id` etc. directly. When the policy selects "hash" (or
        // an unrecognized name) there is no active model, so these
        // fields are omitted from the top level — consumers should
        // treat their absence as "no embedder model in use" and fall
        // back to `policy_quality_tier_embedder` + `models[*]` to see
        // the full picture.
        let active_json = active_status.map(|s| {
            serde_json::json!({
                "model_id": s.manifest.id,
                "model_dir": s.model_dir.display().to_string(),
                "installed": s.cache_report.is_usable(),
                "state": s.cache_report.state_code(),
                "state_detail": s.cache_report.state.summary(),
                "next_step": s.cache_report.state.next_step(s.registry_name),
                "revision": s.manifest.revision,
                "license": s.manifest.license,
                "total_size_bytes": s.total_size,
                "installed_size_bytes": s.cache_report.installed_size_bytes,
                "observed_file_bytes": s.installed_size,
                "policy_source": s.cache_report.policy_source.as_str(),
                "cache_lifecycle": &s.cache_report,
                "model_acquisition": &s.acquisition,
                "files": &s.files,
            })
        });

        let mut output = serde_json::json!({
            "policy_quality_tier_embedder": policy_embedder,
            "active_registry_name": active_registry_name,
            "lexical_fail_open": true,
            "models": models_json,
        });
        if let (Some(active), Some(obj)) = (active_json, output.as_object_mut())
            && let Some(active_obj) = active.as_object()
        {
            for (k, v) in active_obj {
                obj.insert(k.clone(), v.clone());
            }
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&output).unwrap_or_default()
        );
    } else {
        use colored::Colorize;

        println!("Semantic Search Model Status");
        println!("============================");
        println!();
        println!("Active embedder (quality tier): {}", policy_embedder);
        if active_registry_name.is_none() && policy_embedder != "hash" {
            println!(
                "  {} unsupported quality embedder; hybrid search will fail open to lexical.",
                "⚠".yellow()
            );
        }
        if policy_embedder == "hash" {
            println!("  The explicit hash vector tier is active; no native model is in use.");
        }
        println!("Override with: CASS_SEMANTIC_EMBEDDER={{minilm|multilingual-minilm|hash}}");
        println!("Fail-open: lexical search remains available.");
        println!();

        for s in &statuses {
            let is_active = active_registry_name == Some(s.registry_name);
            let header = format!(
                "{} {} ({})",
                if is_active { "▶" } else { " " },
                s.manifest.id,
                s.manifest.license
            );
            println!(
                "{}",
                if is_active {
                    header.bold().to_string()
                } else {
                    header
                }
            );
            println!(
                "  Revision: {}",
                s.manifest
                    .revision
                    .get(..12)
                    .unwrap_or(&s.manifest.revision)
            );
            println!("  Location: {}", s.model_dir.display());
            let total_size_mb = s.total_size as f64 / 1_048_576.0;
            println!("  Size:     {:.1} MB", total_size_mb);
            let status_str = match s.cache_report.state_code() {
                "acquired" | "preseeded_local" => {
                    s.cache_report.state.summary().green().to_string()
                }
                "not_acquired" | "budget_blocked" | "disabled_by_policy" => {
                    s.cache_report.state.summary().yellow().to_string()
                }
                _ => s.cache_report.state.summary().red().to_string(),
            };
            println!("  Status:   {}", status_str);

            print!("  Files:    ");
            let installed = s
                .files
                .iter()
                .filter(|f| f["size_match"].as_bool().unwrap_or(false))
                .count();
            let partial = s
                .files
                .iter()
                .filter(|f| {
                    f["exists"].as_bool().unwrap_or(false)
                        && !f["size_match"].as_bool().unwrap_or(false)
                })
                .count();
            let missing = s.files.len() - installed - partial;
            println!(
                "{} ok / {} partial / {} missing",
                installed, partial, missing
            );
            if let Some(next_step) = s.cache_report.state.next_step(s.registry_name) {
                println!("  Next:     {}", next_step);
            }
            println!();
        }

        if active_status.is_none() && policy_embedder != "hash" {
            println!(
                "{}: active embedder '{}' has no manifest registered. \
                 Use 'cass models install --model minilm --from-file <DIR>' or '--model multilingual-minilm --from-file <DIR>' to install a supported embedder.",
                "⚠".yellow(),
                policy_embedder
            );
        }
    }

    Ok(())
}

/// Resolve a CLI-supplied semantic model name (or alias) to the canonical
/// registry name used by model installation and removal. Embedder aliases match
/// the manifest-attested daemon worker; retired ONNX-era aliases are rejected before
/// they can route to an unloadable manifest.
/// Models the pure-Rust native inference backend can actually load (cass
/// #308/#410): the native embedder admits the registered MiniLM L6 and
/// multilingual MiniLM L12 topologies, while the
/// native reranker is architecture-verified for ms-marco only. Anything else
/// would copy and sha-verify fine, report installed/Ready, and then fail
/// every load with `EmbedderUnavailable` — so `models install` refuses it up
/// front instead of leaving a 100+ MB permanently-unloadable install behind.
pub(crate) fn native_backend_supports_model(registry_name: &str) -> bool {
    matches!(registry_name, "minilm" | "multilingual-minilm" | "ms-marco")
}

pub(crate) fn resolve_cli_model_name(model_name: &str) -> CliResult<&'static str> {
    match model_name.to_ascii_lowercase().as_str() {
        "fastembed" | "minilm" | "minilm-384" | "all-minilm-l6-v2" => Ok("minilm"),
        "multilingual"
        | "multilingual-minilm"
        | "multilingual-minilm-384"
        | "paraphrase-multilingual-minilm-l12-v2" => Ok("multilingual-minilm"),
        "snowflake"
        | "snowflake-arctic-s"
        | "snowflake-arctic-s-384"
        | "snowflake-arctic-embed-s"
        | "nomic"
        | "nomic-embed"
        | "nomic-embed-768"
        | "nomic-embed-text-v1.5" => Err(CliError {
            code: 20,
            kind: CliErrorKind::Model.kind_str(),
            message: format!(
                "Unsupported embedder '{}': the pure-Rust native backend supports all-MiniLM-L6-v2 (alias minilm) and paraphrase-multilingual-MiniLM-L12-v2 (alias multilingual-minilm).",
                model_name
            ),
            hint: Some(
                "Use --model minilm, --model multilingual-minilm, or keep using lexical search"
                    .into(),
            ),
            retryable: false,
        }),
        "ms-marco" | "ms-marco-minilm" | "ms-marco-minilm-l-6-v2" | "ms-marco-minilm-l6-v2" => {
            Ok("ms-marco")
        }
        "jina-reranker-turbo" | "jina-reranker-v1-turbo" | "jina-reranker-v1-turbo-en" => {
            Err(CliError {
                code: 20,
                kind: CliErrorKind::Model.kind_str(),
                message: format!(
                    "Unsupported reranker '{}': the pure-Rust native backend currently supports only ms-marco-MiniLM-L-6-v2 (alias ms-marco).",
                    model_name
                ),
                hint: Some("Use --model ms-marco, or omit reranking".into()),
                retryable: false,
            })
        }
        _ => Err(CliError {
            code: 20,
            kind: CliErrorKind::Model.kind_str(),
            message: format!(
                "Unknown model '{}'. Embedders: all-minilm-l6-v2 (alias minilm), \
                 paraphrase-multilingual-minilm-l12-v2 (alias multilingual-minilm). \
                 Reranker: ms-marco.",
                model_name
            ),
            hint: Some("Use 'cass models status' to see available models".into()),
            retryable: false,
        }),
    }
}

/// Returns the on-disk model directory for either an embedder or a reranker.
///
/// `registry_name` must be a canonical name returned by `resolve_cli_model_name`.
/// Embedders are routed through `FastEmbedder::model_dir_for`; rerankers use
/// `<data_dir>/models/<manifest.id>` so install layout matches the directory
/// `FastEmbedReranker` reads at startup.
pub(crate) fn resolve_model_install_dir(
    data_dir: &Path,
    registry_name: &str,
) -> CliResult<(PathBuf, crate::search::model_download::ModelManifest)> {
    use crate::search::fastembed_embedder::FastEmbedder;
    use crate::search::model_download::ModelManifest;

    if let Some(manifest) = ModelManifest::for_embedder(registry_name) {
        let model_dir =
            FastEmbedder::model_dir_for(data_dir, registry_name).ok_or_else(|| CliError {
                code: 20,
                kind: CliErrorKind::Model.kind_str(),
                message: format!(
                    "no model directory mapping for registered embedder '{}'",
                    registry_name
                ),
                hint: None,
                retryable: false,
            })?;
        return Ok((model_dir, manifest));
    }

    if let Some(manifest) = ModelManifest::for_reranker(registry_name) {
        let model_dir = data_dir.join("models").join(&manifest.id);
        return Ok((model_dir, manifest));
    }

    Err(CliError {
        code: 20,
        kind: CliErrorKind::Model.kind_str(),
        message: format!(
            "no manifest registered for model '{}' (neither embedder nor reranker)",
            registry_name
        ),
        hint: None,
        retryable: false,
    })
}

/// Install the semantic search model from a local directory.
///
/// cass is local-only: there is no downloader, mirror, or update path.
/// `ModelInstaller::install_from_directory` stages every manifest-required
/// file from `source_dir`, verifies each SHA256, writes the provenance
/// marker, and atomically promotes the staged files — a failed install
/// leaves any previous installation untouched.
pub(crate) fn run_models_install(
    model_name: &str,
    source_path: &Path,
    data_dir_override: Option<PathBuf>,
) -> CliResult<()> {
    use crate::search::model_download::ModelInstaller;
    use colored::Colorize;

    let registry_name = resolve_cli_model_name(model_name)?;
    if !native_backend_supports_model(registry_name) {
        return Err(CliError {
            code: 20,
            kind: CliErrorKind::Model.kind_str(),
            message: format!(
                "Model '{registry_name}' cannot be loaded by the pure-Rust native inference \
                 backend yet: only the registered MiniLM embedders and ms-marco reranker are \
                 architecture-verified (cass #308). Installing it would fetch files that \
                 can never be used."
            ),
            hint: Some(
                "Install the supported default embedder instead: \
                 cass models install --model all-minilm-l6-v2 --from-file <DIR>"
                    .into(),
            ),
            retryable: false,
        });
    }
    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    let (model_dir, manifest) = resolve_model_install_dir(&data_dir, registry_name)?;

    println!(
        "Installing model from local directory: {}",
        source_path.display()
    );

    let installer = ModelInstaller::new(model_dir.clone());
    installer
        .install_from_directory(source_path, &manifest)
        .map_err(|error| {
            use crate::search::model_download::InstallError;
            let rendered = error.to_string();
            let message = format!("Model install failed: {rendered}");
            let hint = Some(format!(
                "Provide a directory containing the manifest's files \
                 ({}); each file is SHA256-verified before anything is published",
                manifest
                    .files
                    .iter()
                    .map(|f| f.local_name().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            // Validation failures (not a directory, missing file, checksum
            // mismatch, source==target, unverified manifest) are operator
            // errors: rerunning the same command cannot succeed, so they are
            // reported non-retryable. Only genuine I/O failures qualify as
            // retryable and keep the Io kind/code. The literal `code:` field
            // values are load-bearing: the exit-code audit scanners
            // (spec_exit_codes_documented, golden_error_envelope) only see
            // `code: <digits>` directly above `kind:` in a CliError literal,
            // not destructured tuple variables.
            match &error {
                InstallError::IoError(io_error)
                    if io_error.kind() != std::io::ErrorKind::NotADirectory =>
                {
                    CliError {
                        code: 22,
                        kind: CliErrorKind::Io.kind_str(),
                        message,
                        hint,
                        retryable: true,
                    }
                }
                _ => CliError {
                    code: 21,
                    kind: CliErrorKind::Model.kind_str(),
                    message,
                    hint,
                    retryable: false,
                },
            }
        })?;

    println!();
    println!(
        "{} Model installed successfully from local files!",
        "✓".green()
    );
    println!("  Location: {}", model_dir.display());
    println!(
        "Model files are verified. Semantic search also requires a working embedder and a completed backfill."
    );
    Ok(())
}

/// Verify model file integrity
pub(crate) fn run_models_verify(
    model_name: &str,
    data_dir_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
) -> CliResult<()> {
    use crate::search::model_download::{
        ModelAcquisitionPolicy, classify_model_cache, compute_sha256, model_file_path,
    };
    use crate::search::policy::{CliSemanticOverrides, SemanticPolicy};
    use colored::Colorize;

    let data_dir = data_dir_override.clone().unwrap_or_else(default_data_dir);
    let registry_name = resolve_cli_model_name(model_name)?;
    let (model_dir, manifest) = resolve_model_install_dir(&data_dir, registry_name)?;
    let policy = SemanticPolicy::resolve(&CliSemanticOverrides::default());
    let acquisition_policy = ModelAcquisitionPolicy::from_semantic_policy(&policy);
    let cache_report = classify_model_cache(&model_dir, &manifest, &acquisition_policy);

    // Hardened model-acquisition projection (bead 0qxwg): the same probe-based
    // block `cass models status` emits, so verify and status agree. The probe
    // is filesystem-only — `skipped_network_reason` proves verify never
    // touches the network. `probe_cheap_host()` never loads ONNX, so it is
    // safe even on a corrupt/incompatible cache.
    let acquisition_block = crate::search::model_acquisition::live_acquisition_block(
        &model_dir,
        &manifest,
        crate::search::model_acquisition::RuntimeLoadability::probe_cheap_host(),
        crate::search::model_acquisition::OnDiskPolicyInputs {
            policy_enabled: acquisition_policy.install_enabled,
            budget_max_bytes: acquisition_policy.max_model_bytes,
        },
        registry_name,
    );

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if !model_dir.is_dir() {
        if let Some(_fmt) = structured_format {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "status": cache_report.state_code(),
                    "state_detail": cache_report.state.summary(),
                    "next_step": cache_report.state.next_step(registry_name),
                    "lexical_fail_open": true,
                    "model_dir": model_dir.display().to_string(),
                    "all_valid": false,
                    "cache_lifecycle": &cache_report,
                    "model_acquisition": &acquisition_block,
                    "error": "model directory does not exist",
                }))
                .unwrap_or_default()
            );
        } else {
            println!("{} {}", "✗".red(), cache_report.state.summary());
            println!("  Expected location: {}", model_dir.display());
            println!();
            if let Some(next_step) = cache_report.state.next_step(registry_name) {
                println!("{}", next_step);
            }
        }
        return Ok(());
    }

    let mut results: Vec<serde_json::Value> = Vec::new();
    let mut all_valid = true;
    let mut failed_files: Vec<String> = Vec::new();

    let is_robot = structured_format.is_some();
    if !is_robot {
        println!("Verifying model files...");
        println!();
    }

    for mfile in &manifest.files {
        let file_path = model_file_path(&model_dir, mfile);
        let exists = file_path.is_some();

        let (valid, actual_hash, error) = if let Some(path) = file_path.as_ref() {
            match compute_sha256(path) {
                Ok(hash) => {
                    let matches = hash == mfile.sha256;
                    (matches, Some(hash), None)
                }
                Err(e) => (false, None, Some(std::borrow::Cow::Owned(e.to_string()))),
            }
        } else {
            (false, None, Some(std::borrow::Cow::Borrowed("File not found")))
        };

        if !valid {
            all_valid = false;
            failed_files.push(mfile.local_name().to_string());
        }

        results.push(serde_json::json!({
            "file": mfile.name,
            "local_name": mfile.local_name(),
            "actual_path": file_path.as_ref().map(|path| path.display().to_string()),
            "exists": exists,
            "valid": valid,
            "expected_sha256": mfile.sha256,
            "actual_sha256": actual_hash,
            "error": error,
        }));

        if !is_robot {
            let status = if valid {
                "✓".green().to_string()
            } else {
                "✗".red().to_string()
            };
            println!("  {} {}", status, mfile.name);
            if let Some(ref err) = error {
                println!("      Error: {}", err);
            } else if !valid {
                println!(
                    "      Expected: {}",
                    mfile.sha256.get(..16).unwrap_or(&mfile.sha256)
                );
                if let Some(ref actual) = actual_hash {
                    println!("      Got:      {}", actual.get(..16).unwrap_or(actual));
                }
            }
        }
    }

    if let Some(_fmt) = structured_format {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "model_dir": model_dir.display().to_string(),
                "status": cache_report.state_code(),
                "state_detail": cache_report.state.summary(),
                "next_step": cache_report.state.next_step(registry_name),
                "lexical_fail_open": true,
                "all_valid": all_valid,
                "cache_lifecycle": &cache_report,
                "model_acquisition": &acquisition_block,
                "files": results,
            }))
            .unwrap_or_default()
        );
    } else {
        println!();
        if all_valid {
            println!("{} All model files verified successfully.", "✓".green());
        } else {
            println!(
                "{} {} file(s) failed verification.",
                "✗".red(),
                failed_files.len()
            );
            println!();
            println!("cass is local-only: repair means reinstalling from a known-good");
            println!("directory of model files. Reinstall with:");
            println!("  cass models install --model {registry_name} --from-file <DIR>");
        }
    }

    Ok(())
}

/// `cass models build-hnsw` (bead uaulb, gh#408): build or verify the HNSW
/// accelerator for an ALREADY-PUBLISHED semantic vector artifact.
///
/// Reads only the semantic manifest and the published `.fsvi`; never opens
/// the canonical archive and never embeds. The graph is written through
/// frankensearch's generation-atomic saver (metadata is the commit point),
/// proven loadable as the exact native artifact, and only then recorded in
/// the semantic manifest. `--check` classifies and writes nothing.
pub(crate) fn run_models_build_hnsw(
    tier_raw: Option<&str>,
    check: bool,
    force: bool,
    data_dir_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
) -> CliResult<()> {
    use crate::search::ann_index::{
        HnswAcceleratorState, hnsw_index_path, inspect_hnsw_accelerator, maintain_hnsw_accelerator,
    };
    use crate::search::semantic_manifest::{HnswRecord, SemanticManifest, TierKind};
    use crate::search::vector_index::VectorIndex as FsVectorIndex;
    use colored::Colorize;

    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    // Keep the selected vector/manifest generation stable through native
    // admission and manifest publication. The lock does not open the archive;
    // --check remains a strictly read-only inspection.
    let _maintenance_guard = if check {
        None
    } else {
        let db_path = data_dir.join("agent_search.db");
        Some(
            crate::indexer::acquire_search_asset_mutation_lock(
                &data_dir,
                &db_path,
                crate::search::asset_state::SearchMaintenanceJobKind::SemanticRebuild,
            )
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
                    message: format!("Failed to acquire HNSW maintenance lock: {rendered}"),
                    hint: Some("Check permissions under the cass data directory".into()),
                    retryable: true,
                }
            })?,
        )
    };
    let mut manifest = SemanticManifest::load(&data_dir)
        .map_err(|err| CliError {
            code: 5,
            kind: CliErrorKind::Storage.kind_str(),
            message: format!("semantic manifest unreadable: {err}"),
            hint: Some("Run 'cass doctor check --json' to diagnose the semantic assets".into()),
            retryable: false,
        })?
        .ok_or_else(|| CliError {
            code: 3,
            kind: CliErrorKind::MissingIndex.kind_str(),
            message: format!(
                "no semantic manifest under {}; nothing is published to accelerate",
                data_dir.display()
            ),
            hint: Some("Run 'cass index --semantic' first".into()),
            retryable: true,
        })?;

    let tier = match tier_raw {
        Some(raw) => parse_models_backfill_tier(raw)?,
        None if manifest
            .quality_tier
            .as_ref()
            .is_some_and(|record| record.ready) =>
        {
            TierKind::Quality
        }
        None => TierKind::Fast,
    };
    let record = match tier {
        TierKind::Fast => manifest.fast_tier.clone(),
        TierKind::Quality => manifest.quality_tier.clone(),
    }
    .filter(|record| record.ready)
    .ok_or_else(|| CliError {
        code: 3,
        kind: CliErrorKind::MissingIndex.kind_str(),
        message: format!(
            "no published {} semantic artifact in the manifest",
            tier.as_str()
        ),
        hint: Some(format!(
            "Run 'cass index --semantic' (or 'cass models backfill --tier {}') first",
            tier.as_str()
        )),
        retryable: true,
    })?;
    if !crate::search::semantic_manifest::semantic_shard_artifact_path_is_safe(&record.index_path) {
        return Err(CliError {
            code: 5,
            kind: CliErrorKind::Storage.kind_str(),
            message: format!(
                "manifest {} artifact path is not a safe relative path: {:?}",
                tier.as_str(),
                record.index_path
            ),
            hint: Some(
                "Run 'cass index --semantic --force-rebuild' to republish the artifact".into(),
            ),
            retryable: false,
        });
    }
    let index_path = data_dir.join(&record.index_path);
    let fs_index = FsVectorIndex::open_read_only(&index_path).map_err(|err| CliError {
        code: 5,
        kind: CliErrorKind::Storage.kind_str(),
        message: format!(
            "published {} artifact {} cannot be opened: {err}",
            tier.as_str(),
            index_path.display()
        ),
        hint: Some("Run 'cass index --semantic --force-rebuild' to republish the artifact".into()),
        retryable: false,
    })?;
    if fs_index
        .embedder_id()
        .cmp(record.embedder_id.as_str())
        .is_ne()
    {
        return Err(CliError {
            code: 5,
            kind: CliErrorKind::Storage.kind_str(),
            message: format!(
                "published {} artifact reports embedder '{}' but the manifest records '{}'",
                tier.as_str(),
                fs_index.embedder_id(),
                record.embedder_id
            ),
            hint: Some(
                "Run 'cass index --semantic --force-rebuild' to republish the artifact".into(),
            ),
            retryable: false,
        });
    }
    let vector_count = fs_index.record_count();
    let ann_path = hnsw_index_path(&data_dir, &record.embedder_id);
    let relative_ann_path = ann_path
        .strip_prefix(&data_dir)
        .unwrap_or(ann_path.as_path())
        .to_string_lossy()
        .to_string();
    let manifest_records_current = manifest.hnsw.as_ref().is_some_and(|hnsw| {
        hnsw.ready
            && hnsw.base_tier.as_str().cmp(tier.as_str()).is_eq()
            && hnsw.embedder_id.cmp(&record.embedder_id).is_eq()
            && hnsw.index_path == relative_ann_path
    });

    let (before, action, after, manifest_published) = if check {
        let before = inspect_hnsw_accelerator(&ann_path, &fs_index);
        (before, "check_only", before, false)
    } else {
        let maintenance = maintain_hnsw_accelerator(
            &ann_path,
            &fs_index,
            frankensearch::index::HnswConfig::default(),
            force,
        )
        .map_err(|err| CliError {
            code: 5,
            kind: CliErrorKind::Storage.kind_str(),
            message: format!("HNSW accelerator maintenance failed: {err:#}"),
            hint: Some("Retry; if it persists run 'cass doctor check --json'".into()),
            retryable: true,
        })?;
        // Re-record a verified graph that the manifest forgot, without
        // rebuilding it or changing its existing native generation.
        (
            maintenance.before,
            maintenance.action(),
            HnswAcceleratorState::NativeValid,
            maintenance.rebuilt || !manifest_records_current,
        )
    };

    if manifest_published {
        let size_bytes = std::fs::metadata(&ann_path).map(|m| m.len()).unwrap_or(0);
        manifest.publish_hnsw(HnswRecord {
            base_tier: tier,
            embedder_id: record.embedder_id.clone(),
            ef_search: frankensearch::index::HNSW_DEFAULT_EF_SEARCH,
            index_path: relative_ann_path.clone(),
            size_bytes,
            built_at_ms: chrono::Utc::now().timestamp_millis(),
            ready: true,
        });
        manifest.save(&data_dir).map_err(|err| CliError {
            code: 5,
            kind: CliErrorKind::Storage.kind_str(),
            message: format!(
                "HNSW accelerator is on disk but recording it in the semantic manifest failed: {err}"
            ),
            hint: Some("Re-run 'cass models build-hnsw' to record it".into()),
            retryable: true,
        })?;
    }

    let structured_format = output_format.or_else(robot_format_from_env);
    if structured_format.is_some() {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "tier": tier.as_str(),
                "embedder_id": record.embedder_id,
                "vector_count": vector_count,
                "artifact_path": record.index_path,
                "hnsw_path": relative_ann_path,
                "state_before": before.as_str(),
                "state_after": after.as_str(),
                "action": action,
                "manifest_published": manifest_published,
                "manifest_recorded": manifest_published || manifest_records_current,
                "current": after.is_current(),
                "next_command": if after.is_current() {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String("cass models build-hnsw".to_string())
                },
            }))
            .unwrap_or_default()
        );
        return Ok(());
    }

    println!(
        "{} tier: {} ({} vectors, embedder {})",
        "Semantic artifact".bold(),
        tier.as_str(),
        vector_count,
        record.embedder_id
    );
    println!("  Accelerator: {relative_ann_path}");
    match action {
        "check_only" => println!(
            "  State: {}{}",
            before.as_str(),
            if before.is_current() && !manifest_records_current {
                " (native graph present but not recorded in the semantic manifest; run without --check to record it)"
            } else {
                ""
            }
        ),
        "unchanged" => println!(
            "  State: {} (already current{})",
            after.as_str(),
            if manifest_published {
                "; recorded in the semantic manifest"
            } else {
                ""
            }
        ),
        _ => println!(
            "  {} rebuilt: {} -> {} (recorded in the semantic manifest)",
            "✓".green(),
            before.as_str(),
            after.as_str()
        ),
    }
    if !after.is_current() {
        println!(
            "  {} approximate search is unavailable; run 'cass models build-hnsw' to build it",
            "!".yellow()
        );
    }
    Ok(())
}

/// Remove model files
pub(crate) fn run_models_remove(
    model_name: &str,
    skip_confirm: bool,
    data_dir_override: Option<PathBuf>,
) -> CliResult<()> {
    use colored::Colorize;

    let registry_name = resolve_cli_model_name(model_name)?;
    let data_dir = data_dir_override.unwrap_or_else(default_data_dir);
    let (model_dir, _manifest) = resolve_model_install_dir(&data_dir, registry_name)?;

    if !model_dir.is_dir() {
        println!("{} Model is not installed.", "✗".yellow());
        println!("  Expected location: {}", model_dir.display());
        return Ok(());
    }

    // Calculate size
    let mut total_size: u64 = 0;
    if let Ok(entries) = std::fs::read_dir(&model_dir) {
        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                total_size += metadata.len();
            }
        }
    }
    let size_mb = total_size as f64 / 1_048_576.0;

    if !skip_confirm {
        println!("Remove Semantic Search Model");
        println!("============================");
        println!();
        println!("Model:    {}", model_name);
        println!("Location: {}", model_dir.display());
        println!("Size:     {:.1} MB", size_mb);
        println!();
        println!("This will remove all model files. Semantic search will be unavailable");
        println!("until the model is reinstalled.");
        print!("Continue? [y/N] ");
        std::io::Write::flush(&mut std::io::stdout()).ok();

        let mut input = String::new();
        std::io::stdin().read_line(&mut input).ok();
        if !input.trim().eq_ignore_ascii_case("y") && !input.trim().eq_ignore_ascii_case("yes") {
            println!("Cancelled.");
            return Ok(());
        }
    }

    // Remove the directory
    std::fs::remove_dir_all(&model_dir).map_err(|e| CliError {
        code: 24,
        kind: CliErrorKind::Io.kind_str(),
        message: format!("Failed to remove model directory: {}", e),
        hint: Some("Check file permissions".into()),
        retryable: false,
    })?;

    println!();
    println!("{} Model removed successfully.", "✓".green());
    println!("  Freed {:.1} MB", size_mb);
    println!();
    println!("To reinstall, run:");
    println!("  cass models install --from-file <DIR>");

    Ok(())
}
