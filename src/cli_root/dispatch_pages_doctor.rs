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

pub(crate) async fn execute_pages_command(
    command: Commands,
    cli: &Cli,
    robot_mode: bool,
) -> CliResult<()> {
    let Commands::Pages {
        export_only,
        verify,
        agents,
        workspaces,
        since,
        until,
        path_mode,
        share_profile,
        target,
        dry_run,
        scan_secrets,
        fail_on_secrets,
        secrets_allow,
        secrets_deny,
        verbose,
        preview,
        port,
        no_open,
        config,
        validate_config,
        example_config,
        json,
    } = command
    else {
        unreachable!();
    };
    let structured_format = resolve_subcommand_structured_format(cli, json);
    let robot_mode_here = structured_format.is_some() || robot_mode;
    // Handle --example-config (show example config and exit)
    if example_config {
        println!("{}", crate::pages::config_input::example_config());
        return Ok(());
    }

    // Handle --config based export
    if let Some(ref config_path) = config {
        let mut pages_config =
            crate::pages::config_input::PagesConfig::load(config_path).map_err(|e| CliError {
                code: 2,
                kind: CliErrorKind::Pages.kind_str(),
                message: format!("Failed to load config: {e}"),
                hint: Some("Check config file syntax with --example-config".to_string()),
                retryable: false,
            })?;

        // Only `local` reaches this point: the removed-flag
        // table rejects `--target github|cloudflare` before
        // clap parses, and the hosted flags are gone entirely.
        if let Some(target) = target {
            pages_config.deployment.target = target.as_config_value().to_string();
        }
        if let Some(profile) = share_profile {
            pages_config.bundle.share_profile = Some(profile);
        }

        // Resolve environment variables
        pages_config.resolve_env_vars().map_err(|e| CliError {
            code: 2,
            kind: CliErrorKind::Pages.kind_str(),
            message: format!("Failed to resolve env vars: {e}"),
            hint: Some("Ensure referenced environment variables are set".to_string()),
            retryable: false,
        })?;

        // Validate configuration
        let validation = pages_config.validate();

        if validate_config {
            // Just validate and output result
            println!(
                "{}",
                serde_json::to_string_pretty(&validation).unwrap_or_default()
            );
            if !validation.valid {
                return Err(CliError {
                    code: 2,
                    kind: CliErrorKind::Pages.kind_str(),
                    message: "Configuration validation failed".to_string(),
                    hint: Some("Review errors in JSON output".to_string()),
                    retryable: false,
                });
            }
            return Ok(());
        }

        if !validation.valid {
            if let Some(fmt) = structured_format {
                output_structured_value(
                    serde_json::to_value(&validation).unwrap_or_default(),
                    fmt,
                )?;
            } else if robot_mode_here {
                eprintln!("Configuration errors:");
                for err in &validation.errors {
                    eprintln!("  - {}", err);
                }
            }
            return Err(CliError {
                code: 2,
                kind: CliErrorKind::Pages.kind_str(),
                message: "Configuration validation failed".to_string(),
                hint: Some("Fix errors listed above".to_string()),
                retryable: false,
            });
        }

        // Print warnings
        if !validation.warnings.is_empty() && structured_format.is_none() && !robot_mode_here {
            eprintln!("Warnings:");
            for warn in &validation.warnings {
                eprintln!("  - {}", warn);
            }
            eprintln!();
        }

        // Get database path
        let db_path = cli.db.clone().unwrap_or_else(default_db_path);

        // Convert config to WizardState and run export
        let wizard_state = pages_config
            .to_wizard_state(db_path.clone())
            .map_err(|e| CliError {
                code: 9,
                kind: CliErrorKind::Pages.kind_str(),
                message: format!("Failed to create wizard state: {e}"),
                hint: None,
                retryable: false,
            })?;

        // Run the export using the config
        run_config_based_export(
            &pages_config,
            &wizard_state,
            &db_path,
            dry_run,
            structured_format,
            verbose,
            ConfigExportSecretInputs {
                allow: &secrets_allow,
                deny: &secrets_deny,
            },
        )
        .map_err(|e| CliError {
            code: 9,
            kind: CliErrorKind::Pages.kind_str(),
            message: format!("Export failed: {e:#}"),
            hint: None,
            retryable: false,
        })?;

        return Ok(());
    }

    // Handle --validate-config without --config
    if validate_config {
        return Err(CliError {
            code: 2,
            kind: CliErrorKind::Pages.kind_str(),
            message: "--validate-config requires --config".to_string(),
            hint: Some("Use --config <path> --validate-config".to_string()),
            retryable: false,
        });
    }

    // Handle --preview first (starts local preview server)
    if let Some(preview_dir) = preview {
        let config = crate::pages::preview::PreviewConfig {
            site_dir: preview_dir,
            port,
            open_browser: !no_open,
        };
        crate::pages::preview::start_preview_server(config)
            .await
            .map_err(|e| CliError {
                code: 9,
                kind: CliErrorKind::Pages.kind_str(),
                message: format!("Preview server failed: {e}"),
                hint: Some("Check that the directory exists and port is available".to_string()),
                retryable: false,
            })?;
        return Ok(());
    }

    // Handle --verify first
    if let Some(verify_path) = verify {
        let result =
            crate::pages::verify::verify_bundle(&verify_path, verbose).map_err(|e| CliError {
                code: 9,
                kind: CliErrorKind::Pages.kind_str(),
                message: format!("Verification failed: {e}"),
                hint: None,
                retryable: false,
            })?;

        if let Some(fmt) = structured_format {
            output_structured_value(serde_json::to_value(&result).unwrap_or_default(), fmt)?;
        } else {
            crate::pages::verify::print_result(&result, verbose);
        }

        // Exit with non-zero if invalid
        if result.status != "valid" {
            if structured_format.is_some() {
                return Err(CliError::already_reported(
                    1,
                    CliErrorKind::Pages.kind_str(),
                    false,
                ));
            }
            return Err(CliError {
                code: 1,
                kind: CliErrorKind::Pages.kind_str(),
                message: "Bundle verification failed".to_string(),
                hint: Some("Run with --verbose for detailed error information".to_string()),
                retryable: false,
            });
        }
    } else if scan_secrets {
        let db_path = cli.db.clone().unwrap_or_else(default_db_path);

        let workspaces_path = workspaces
            .clone()
            .map(|ws| ws.into_iter().map(PathBuf::from).collect());

        let filters = crate::pages::secret_scan::SecretScanFilters {
            agents: agents.clone(),
            workspaces: workspaces_path,
            since_ts: since
                .as_deref()
                .and_then(crate::time_parser::parse_time_input),
            until_ts: until
                .as_deref()
                .and_then(crate::time_parser::parse_time_input),
        };

        let config =
            crate::pages::secret_scan::SecretScanConfig::from_inputs(&secrets_allow, &secrets_deny)
                .map_err(|e| CliError {
                    code: 9,
                    kind: CliErrorKind::Pages.kind_str(),
                    message: format!("Secret scan config error: {e}"),
                    hint: None,
                    retryable: false,
                })?;

        crate::pages::secret_scan::run_secret_scan_cli(
            &db_path,
            &filters,
            &config,
            structured_format.is_some(),
            fail_on_secrets,
        )
        .map_err(|e| CliError {
            code: 9,
            kind: CliErrorKind::Pages.kind_str(),
            message: format!("Secret scan failed: {e}"),
            hint: None,
            retryable: false,
        })?;
    } else if let Some(output_path) = export_only {
        if dry_run {
            if let Some(fmt) = structured_format {
                output_structured_value(
                    serde_json::json!({
                        "status": "dry_run",
                        "output_path": output_path,
                        "secret_scan": {
                            "performed": false,
                            "complete": false,
                            "approval_state": "not_performed_dry_run",
                            "artifact_sha256": null,
                        },
                    }),
                    fmt,
                )?;
            } else {
                println!("Dry run: would export to {:?}", output_path);
                println!("Secret scan: not performed during dry run");
            }
            return Ok(());
        }

        let scan_config =
            crate::pages::secret_scan::SecretScanConfig::from_inputs(&secrets_allow, &secrets_deny)
                .map_err(|e| CliError {
                    code: 9,
                    kind: CliErrorKind::Pages.kind_str(),
                    message: format!("Secret scan config error: {e}"),
                    hint: None,
                    retryable: false,
                })?;
        let structured_export = structured_format.is_some() || robot_mode_here;
        let (stats, (staged_secret_scan, approval_state)) =
            crate::pages::export::export_pages_database_verified(
                cli.db.clone(),
                output_path.clone(),
                agents.clone(),
                workspaces.clone(),
                since.clone(),
                until.clone(),
                path_mode,
                // --export-only writes a plaintext database.
                share_profile.unwrap_or(
                    crate::pages::profiles::ShareProfile::default_for(false),
                ),
                |_current, _total| {},
                |staged_db_path| {
                    let scan = crate::pages::secret_scan::scan_staged_export_database(
                        staged_db_path,
                        &scan_config,
                    )?;
                    if scan.report.summary.total == 0 {
                        return Ok((scan, "clean"));
                    }
                    if structured_export || fail_on_secrets {
                        anyhow::bail!(
                            "Staged export contains {} potential secret(s); robot/CI export-only runs fail closed",
                            scan.report.summary.total
                        );
                    }

                    let mut term = console::Term::stderr();
                    crate::pages::secret_scan::print_human_report(
                        &mut term,
                        &scan.report,
                        3,
                    )?;
                    eprintln!(
                        "The report above describes the exact staged export database."
                    );
                    eprint!(
                        "Type exactly \"I understand the risks\" to publish it: "
                    );
                    use std::io::Write;
                    std::io::stderr().flush()?;
                    let mut acknowledgment = String::new();
                    std::io::stdin().read_line(&mut acknowledgment)?;
                    if acknowledgment.trim() != "I understand the risks" {
                        anyhow::bail!(
                            "Staged export secret findings were not acknowledged"
                        );
                    }
                    Ok((scan, "explicit_interactive_acknowledgment"))
                },
            )
            .map_err(|e| CliError {
                code: 9,
                kind: CliErrorKind::Pages.kind_str(),
                message: format!("Export failed: {e:#}"),
                hint: Some(
                    "Review findings with `cass pages --scan-secrets`; use --secrets-allow only for confirmed false positives."
                        .to_string(),
                ),
                retryable: false,
            })?;

        if let Some(fmt) = structured_format {
            output_structured_value(
                serde_json::json!({
                    "status": "success",
                    "output_path": output_path,
                    "stats": {
                        "conversations": stats.conversations_processed,
                        "messages": stats.messages_processed,
                    },
                    "secret_scan": staged_secret_scan_json(
                        &staged_secret_scan,
                        approval_state,
                    ),
                }),
                fmt,
            )?;
        } else {
            println!(
                "Export complete! Processed {} conversations, {} messages.",
                stats.conversations_processed, stats.messages_processed
            );
            println!(
                "Secret scan: complete ({approval_state}); artifact SHA-256 {}",
                staged_secret_scan.artifact_sha256
            );
        }
    } else {
        // `--target` may only be `local` here (hosted values are
        // rejected by the removed-flag table); it needs no wiring.
        let _ = target;

        // Wizard mode: pass flags
        let mut wizard = crate::pages::wizard::PagesWizard::new();
        if let Some(db_path) = cli.db.clone() {
            wizard.set_db_path(db_path);
        }
        if let Some(profile) = share_profile {
            wizard.set_share_profile(profile);
        }
        wizard.run().map_err(|e| CliError {
            code: 9,
            kind: CliErrorKind::Pages.kind_str(),
            message: format!("Wizard failed: {e:#}"),
            hint: None,
            retryable: false,
        })?;
    }
    Ok(())
}

pub(crate) fn execute_doctor_command(
    command: Commands,
    cli: &Cli,
    wrap: WrapConfig,
) -> CliResult<()> {
    let Commands::Doctor {
        data_dir,
        json,
        check,
        fix,
        repair,
        cleanup,
        archive_scan,
        archive_normalize,
        backups_list,
        backups_verify,
        backups_restore,
        baseline_save,
        baseline_diff,
        baseline_update,
        support_bundle,
        support_bundle_verify,
        archive_export,
        archive_relocate,
        archive_export_verify,
        archive_target,
        backup_id,
        baseline_id,
        baseline_path,
        support_bundle_path,
        include_sensitive_attachments,
        sensitive_attachment,
        sensitive_attachment_max_bytes,
        dry_run,
        yes,
        plan_fingerprint,
        verbose,
        force_rebuild,
        allow_repeated_repair,
        ls,
        undo,
        robot_triage,
        diff,
        gc_before,
        watch,
        watch_interval_ms,
        watch_iterations,
        explain,
        emit_capabilities,
        recover_from_archive,
        rebuild_canonical_fts,
        repair_leaked_pages,
        cleanup_interrupted_artifacts,
    } = command
    else {
        unreachable!();
    };
    let structured_format = resolve_subcommand_structured_format(cli, json);
    // Pass-12 fix (P2): the new pass-1+ flags have no clap-level
    // `conflicts_with` wiring, so passing multiple at once
    // would silently dispatch only the first match. That made
    // `cass doctor --fix --ls` silently drop `--fix`. Detect
    // and refuse multiple new flags at the dispatch boundary
    // so agents get a clean usage error rather than silent
    // precedence behavior.
    let pass1plus_flag_count = [
        explain.is_some(),
        emit_capabilities,
        ls,
        undo.is_some(),
        robot_triage,
        diff.is_some(),
        gc_before.is_some(),
        watch,
    ]
    .iter()
    .filter(|x| **x)
    .count();
    if pass1plus_flag_count > 1 {
        return Err(CliError {
            code: 2,
            kind: "usage",
            message:
                "multiple world-class-doctor flags specified; only one allowed per invocation"
                    .to_string(),
            hint: Some(
                "Pass exactly one of --ls, --undo, --robot-triage, --diff, --gc-before, --watch, --explain, --emit-capabilities at a time."
                    .to_string(),
            ),
            retryable: false,
        });
    }
    // World-class-doctor pass-8: `cass doctor --explain <run-id>`.
    if let Some(run_id_arg) = explain {
        run_doctor_explain(data_dir, &run_id_arg, structured_format)?;
        return Ok(());
    }
    // World-class-doctor pass-8: `cass doctor --emit-capabilities`.
    if emit_capabilities {
        run_doctor_emit_capabilities(structured_format)?;
        return Ok(());
    }
    // #285: `cass doctor --recover-from-archive <DIR>` — rebuild
    // the source JSONL tree from the canonical archive's
    // preserved events so a corrupt archive can be re-ingested.
    if let Some(target_dir) = recover_from_archive {
        doctor_recover::run_doctor_recover_from_archive(
            data_dir,
            cli.db.clone(),
            target_dir,
            structured_format,
        )?;
        return Ok(());
    }
    // #285: `cass doctor --rebuild-canonical-fts --yes` — drop
    // and rebuild the canonical FTS5 shadow tables in place.
    if rebuild_canonical_fts {
        doctor_recover::run_doctor_rebuild_canonical_fts(
            data_dir,
            cli.db.clone(),
            dry_run,
            yes,
            structured_format,
        )?;
        return Ok(());
    }
    // 2l1b0.73: `cass doctor --repair-leaked-pages --yes` —
    // free pages no tree or freelist owns, in place.
    if repair_leaked_pages {
        doctor_recover::run_doctor_repair_leaked_pages(
            data_dir,
            cli.db.clone(),
            dry_run,
            yes,
            structured_format,
        )?;
        return Ok(());
    }
    // #285: `cass doctor --cleanup-interrupted-artifacts --yes`
    // — quarantine interrupted raw_mirror_capture artifacts.
    if cleanup_interrupted_artifacts {
        doctor_recover::run_doctor_cleanup_interrupted_artifacts(data_dir, yes, structured_format)?;
        return Ok(());
    }
    // World-class-doctor pass-2: read-only `cass doctor --ls`.
    if ls {
        run_doctor_ls(data_dir, structured_format)?;
        return Ok(());
    }
    // World-class-doctor pass-2: `cass doctor --undo <run-id>`.
    if let Some(run_id_arg) = undo {
        run_doctor_undo(data_dir, &run_id_arg, structured_format)?;
        return Ok(());
    }
    // World-class-doctor pass-3: `cass doctor --robot-triage`.
    if robot_triage {
        run_doctor_robot_triage(data_dir, structured_format)?;
        return Ok(());
    }
    // World-class-doctor pass-3: `cass doctor --diff [<REF>]`.
    if let Some(diff_ref) = diff {
        run_doctor_diff(data_dir, &diff_ref, structured_format)?;
        return Ok(());
    }
    // World-class-doctor pass-3: `cass doctor --gc-before <ISO> --yes`.
    if let Some(before_iso) = gc_before {
        run_doctor_gc(data_dir, &before_iso, yes, structured_format)?;
        return Ok(());
    }
    // World-class-doctor pass-6: `cass doctor --watch`.
    if watch {
        run_doctor_watch(
            data_dir,
            watch_interval_ms,
            watch_iterations,
            structured_format,
        )?;
        return Ok(());
    }
    if archive_export || archive_relocate || archive_export_verify {
        run_doctor_archive_export_impl(
            DoctorArchiveExportRequest {
                data_dir_override: data_dir,
                db_override: cli.db.clone(),
                output_format: structured_format,
                workflow: if archive_relocate {
                    DoctorArchiveExportWorkflow::Relocate
                } else {
                    DoctorArchiveExportWorkflow::Export
                },
                command_mode: if archive_export_verify {
                    DoctorArchiveExportCommandMode::Verify
                } else if yes {
                    DoctorArchiveExportCommandMode::Apply
                } else {
                    DoctorArchiveExportCommandMode::Plan
                },
                target_root: archive_target,
                dry_run,
                yes,
                plan_fingerprint,
                backup_id,
                force_rebuild,
                allow_repeated_repair,
            },
            wrap,
        )?;
    } else if support_bundle || support_bundle_verify {
        validate_doctor_diagnostic_read_only_controls(
            "support-bundle",
            &backup_id,
            dry_run,
            yes,
            plan_fingerprint.as_deref(),
            force_rebuild,
            allow_repeated_repair,
        )?;
        let mode = if support_bundle_verify {
            DoctorSupportBundleCommandMode::Verify
        } else {
            DoctorSupportBundleCommandMode::Create
        };
        run_doctor_support_bundle_impl(
            DoctorSupportBundleRequest {
                data_dir_override: data_dir,
                db_override: cli.db.clone(),
                output_format: structured_format,
                mode,
                baseline_id,
                baseline_path,
                support_bundle_path,
                include_sensitive_attachments,
                sensitive_attachments: sensitive_attachment,
                sensitive_attachment_max_bytes,
            },
            wrap,
        )?;
    } else if baseline_save || baseline_diff || baseline_update {
        validate_doctor_diagnostic_read_only_controls(
            "baseline",
            &backup_id,
            dry_run,
            yes,
            plan_fingerprint.as_deref(),
            force_rebuild,
            allow_repeated_repair,
        )?;
        let mode = if baseline_diff {
            DoctorBaselineCommandMode::Diff
        } else if baseline_update {
            DoctorBaselineCommandMode::Update
        } else {
            DoctorBaselineCommandMode::Save
        };
        run_doctor_baseline_impl(
            data_dir,
            cli.db.clone(),
            structured_format,
            mode,
            baseline_id,
            baseline_path,
            wrap,
        )?;
    } else {
        let request = doctor::DoctorCommandRequest::from_cli_flags_with_backups(
            data_dir,
            cli.db.clone(),
            structured_format,
            check,
            fix,
            repair,
            cleanup,
            archive_scan,
            archive_normalize,
            backups_list,
            backups_verify,
            backups_restore,
            backup_id,
            dry_run,
            yes,
            plan_fingerprint,
            verbose,
            force_rebuild,
            allow_repeated_repair,
        )?;
        doctor::execute_doctor_command_with_wrap(request, wrap)?;
    }
    Ok(())
}
