use crate::cli::args::Cli;
use crate::cli::args_sub::{ModelsCommand, RobotFormat};
use crate::cli::commands::models::run_models_remove::run_models_remove;
use crate::cli::commands::search::fieldbudgets::robot_format_from_env;
use crate::cli::commands::search::toon_encode_options_from_env::output_structured_value;
use crate::cli::error::CliError;
use crate::cli::error::CliResult;
use crate::cli::output::describe_command::resolve_subcommand_structured_format;
use crate::cli::paths::default_data_dir;
use crate::cli::paths::resolve_data_dir;
use crate::cli::state::probe_state_db_strict_worker::{
    active_index_run_details, error_chain_indicates_active_cass_index,
    index_storage_contention_cli_error,
};
use crate::indexer::stall_watchdog::IndexShutdownSignals;
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
use std::path::{Path, PathBuf};

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
                Err(e) => (false, None, Some(e.to_string())),
            }
        } else {
            (false, None, Some("File not found".to_string()))
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
