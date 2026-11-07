use crate::cli::args_sub::RobotFormat;
use crate::cli::commands::analytics::fresh_franken_count_retry::fresh_franken_count_retry;
use crate::cli::commands::analytics::run_analytics::{
    franken_query_row_map_retry, open_franken_cli_read_db,
};
use crate::cli::commands::search::fieldbudgets::robot_format_from_env;
use crate::cli::commands::search::toon_encode_options_from_env::output_structured_value;
use crate::cli::commands::status_health::diagnostics::{diagnostics_connector_paths, format_bytes};
use crate::cli::error::CliResult;
use crate::cli::paths::resolve_data_dir;
use crate::cli::state::state_db_open_timeout::CLI_DIAG_DB_OPEN_TIMEOUT;
use crate::doctor::fs_safety::cleanup_apply::collect_diag_quarantine_report;
use crate::doctor::lexical_cleanup_disposition_label::{
    capabilities_connector_names, format_optional_i64, format_optional_u64, fs_dir_size,
    public_connector_slug,
};
use std::collections::HashMap;
use std::path::PathBuf;

pub(crate) fn run_diag(
    data_dir_override: &Option<PathBuf>,
    db_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
    quarantine: bool,
    verbose: bool,
) -> CliResult<()> {
    use crate::franken_sync::compat::RowExt;
    use crate::franken_sync::params;
    use std::fs;

    let version = env!("CARGO_PKG_VERSION");
    let data_dir = resolve_data_dir(data_dir_override, db_override.as_ref());
    let db_path = db_override.unwrap_or_else(|| data_dir.join("agent_search.db"));
    // Use the actual versioned lexical index path without creating it during diagnostics.
    let index_path = crate::search::tantivy::expected_index_dir(&data_dir);

    // Check database existence and get stats
    let (db_exists, db_size, conversation_count, message_count) = if db_path.exists() {
        let size = fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);
        let (convs, msgs) = match open_franken_cli_read_db(
            db_path.clone(),
            "diagnostics",
            CLI_DIAG_DB_OPEN_TIMEOUT,
        ) {
            Ok(conn) => {
                let mut convs: i64 = franken_query_row_map_retry(
                    &conn,
                    "SELECT COUNT(*) FROM conversations",
                    params![],
                    |r| r.get_typed(0),
                )
                .unwrap_or(0);
                let mut msgs: i64 = franken_query_row_map_retry(
                    &conn,
                    "SELECT COUNT(*) FROM messages",
                    params![],
                    |r| r.get_typed(0),
                )
                .unwrap_or(0);
                if convs == 0 {
                    convs = fresh_franken_count_retry(
                        &db_path,
                        "diagnostics",
                        CLI_DIAG_DB_OPEN_TIMEOUT,
                        "SELECT COUNT(*) FROM conversations",
                        params![],
                    )
                    .unwrap_or(0);
                }
                if msgs == 0 {
                    msgs = fresh_franken_count_retry(
                        &db_path,
                        "diagnostics",
                        CLI_DIAG_DB_OPEN_TIMEOUT,
                        "SELECT COUNT(*) FROM messages",
                        params![],
                    )
                    .unwrap_or(0);
                }
                (convs, msgs)
            }
            Err(e) => {
                tracing::warn!("failed to open database for diagnostics: {e}");
                (-1, -1)
            }
        };
        (true, size, convs, msgs)
    } else {
        (false, 0, 0, 0)
    };

    // Check index existence
    let (index_exists, index_size) = if crate::search::tantivy::searchable_index_exists(&index_path)
    {
        let size = fs_dir_size(&index_path);
        (true, size)
    } else {
        (false, 0)
    };

    // Agent search paths - compute path once, then check existence
    let home = dirs::home_dir().unwrap_or_default();
    let config_dir = dirs::config_dir().unwrap_or_default();

    let agent_paths: Vec<(String, PathBuf, bool)> = diagnostics_connector_paths(&home, &config_dir)
        .into_iter()
        .map(|(name, path)| {
            let exists = path.exists();
            (name, path, exists)
        })
        .collect();
    let mut connector_paths_by_name: HashMap<String, Vec<PathBuf>> = HashMap::new();
    for (name, path, exists) in &agent_paths {
        if *exists {
            connector_paths_by_name
                .entry(public_connector_slug(name).to_string())
                .or_default()
                .push(path.clone());
        }
    }
    let connector_rows: Vec<(String, String, bool)> = capabilities_connector_names()
        .into_iter()
        .map(|name| {
            let path = connector_paths_by_name
                .get(&name)
                .and_then(|paths| paths.first())
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "(not detected)".to_string());
            let found = path != "(not detected)";
            (name, path, found)
        })
        .collect();

    let platform = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let quarantine_report = quarantine.then(|| {
        let mut report = collect_diag_quarantine_report(&data_dir, &index_path);
        report.lexical_rebuild_quarantine = Some(
            crate::search::lexical_quarantine_diagnostics::collect(&data_dir),
        );
        report
    });

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(fmt) = structured_format {
        let mut payload = serde_json::json!({
            "version": version,
            "platform": { "os": platform, "arch": arch },
            "paths": {
                "data_dir": data_dir.display().to_string(),
                "db_path": db_path.display().to_string(),
                "index_path": index_path.display().to_string(),
            },
            "database": {
                "exists": db_exists,
                "size_bytes": db_size,
                "conversations": conversation_count,
                "messages": message_count,
            },
            "index": {
                "exists": index_exists,
                "size_bytes": index_size,
            },
            "connectors": connector_rows.iter().map(|(name, path, found)| {
                serde_json::json!({
                    "name": name,
                    "path": path,
                    "found": found,
                })
            }).collect::<Vec<_>>(),
        });
        if let Some(report) = quarantine_report.as_ref() {
            payload["quarantine"] = serde_json::json!(report);
        }
        return output_structured_value(payload, fmt);
    }

    println!("CASS Diagnostic Report");
    println!("======================");
    println!();
    println!("Version: {version}");
    println!("Platform: {platform} ({arch})");
    println!();
    println!("Paths:");
    println!("  Data directory: {}", data_dir.display());
    println!("  Database: {}", db_path.display());
    println!("  Lexical index: {}", index_path.display());
    println!();
    println!("Database Status:");
    if db_exists {
        println!("  Status: OK");
        if verbose {
            println!("  Size: {}", format_bytes(db_size));
        }
        println!("  Conversations: {conversation_count}");
        println!("  Messages: {message_count}");
    } else {
        println!("  Status: NOT FOUND");
        println!("  Hint: Run 'cass index --full' to create the database");
    }
    println!();
    println!("Index Status:");
    if index_exists {
        println!("  Status: OK");
        if verbose {
            println!("  Size: {}", format_bytes(index_size));
        }
    } else {
        println!("  Status: NOT FOUND");
        println!("  Hint: Run 'cass index --full' to create the index");
    }
    println!();
    println!("Connector Search Paths:");
    for (name, path, found) in &connector_rows {
        let status = if *found { "✓" } else { "✗" };
        println!("  {} {}: {}", status, name, path);
    }
    if let Some(report) = quarantine_report.as_ref() {
        println!();
        println!("Quarantine:");
        if let Some(rebuilds) = &report.lexical_rebuild_quarantine {
            for line in rebuilds.human_lines() {
                println!("  {line}");
            }
        }
        println!(
            "  Failed seed bundles: {}",
            report.summary.failed_seed_bundle_count
        );
        println!(
            "  Retained publish backups: {}",
            report.summary.retained_publish_backup_count
        );
        println!(
            "  Quarantined lexical generations: {}",
            report.summary.lexical_quarantined_generation_count
        );
        println!(
            "  Quarantined lexical shards: {}",
            report.summary.lexical_quarantined_shard_count
        );
        println!(
            "  Total retained bytes: {}",
            report.summary.total_retained_bytes
        );
        println!(
            "  Retained publish backup cap: {}",
            report.summary.retained_publish_backup_retention_limit
        );
        println!(
            "  GC-eligible assets: {}",
            report.summary.gc_eligible_asset_count
        );
        println!("  GC-eligible bytes: {}", report.summary.gc_eligible_bytes);
        println!(
            "  Inspection-required assets: {}",
            report.summary.inspection_required_asset_count
        );
        println!(
            "  Inspection-required bytes: {}",
            report.summary.inspection_required_bytes
        );
        if !report.failed_seed_bundle_files.is_empty() {
            println!("  Failed seed bundle files:");
            for artifact in &report.failed_seed_bundle_files {
                println!(
                    "    - {} ({} bytes, age_s={}, last_read_ms={}, gc={}, reason={})",
                    artifact.path,
                    artifact.size_bytes,
                    format_optional_u64(artifact.age_seconds),
                    format_optional_i64(artifact.last_read_at_ms),
                    artifact.safe_to_gc,
                    artifact.gc_reason
                );
            }
        }
        if !report.retained_publish_backups.is_empty() {
            println!("  Retained publish backups:");
            for artifact in &report.retained_publish_backups {
                println!(
                    "    - {} ({} bytes, age_s={}, last_read_ms={}, gc={}, reason={})",
                    artifact.path,
                    artifact.size_bytes,
                    format_optional_u64(artifact.age_seconds),
                    format_optional_i64(artifact.last_read_at_ms),
                    artifact.safe_to_gc,
                    artifact.gc_reason
                );
            }
        }
        if !report.lexical_generations.is_empty() {
            println!("  Quarantined lexical generations:");
            for generation in &report.lexical_generations {
                println!(
                    "    - {} [{}] shards={}/{} bytes={} age_s={} last_read_ms={} reclaimable_bytes={} inspection_required={} gc={} reason={}",
                    generation.path,
                    generation.publish_state,
                    generation.quarantined_shards,
                    generation.total_shards,
                    generation.artifact_bytes,
                    format_optional_u64(generation.age_seconds),
                    format_optional_i64(generation.last_read_at_ms),
                    generation.reclaimable_bytes,
                    generation.inspection_required,
                    generation.safe_to_gc,
                    generation.gc_reason
                );
            }
        }
        if verbose && !report.warnings.is_empty() {
            println!("  Scan warnings:");
            for warning in &report.warnings {
                println!("    - {warning}");
            }
        }
    }

    Ok(())
}
