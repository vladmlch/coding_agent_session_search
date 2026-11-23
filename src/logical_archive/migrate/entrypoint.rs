use std::fs;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::Path;

use anyhow::{Context, Result, bail, ensure};
use coding_agent_search::franken_sync::{Connection, compat::RowExt};
use coding_agent_search::storage::sqlite::SqliteStorage;

use super::projection::{
    identity, materialize_candidate, require_new_destination, sync_candidate,
    verify_persisted_projection,
};
use super::replay::restore_reviewed;
use super::{
    Input, Inspected, MigrationOutcome, SchemaMigrationReceipt, inspect, parse_source_version,
    require_reviewed_transition, target_version,
};
use crate::logical_archive::export::{self, DestinationLock};
use crate::logical_archive::{database_failure, import};

fn migration_receipt(inspected: &Inspected) -> Result<SchemaMigrationReceipt> {
    let source = parse_source_version(&inspected.header)?;
    require_reviewed_transition(source, target_version()?)?;
    Ok(SchemaMigrationReceipt {
        mode: match source {
            20 => "reviewed_v20_to_v23",
            21 => "reviewed_v21_to_v23",
            22 => "reviewed_v22_to_v23",
            _ => bail!("no reviewed migration mode for storage schema {source}"),
        },
        from_storage_schema_version: inspected.header.storage_schema_version.clone(),
        to_storage_schema_version: target_version()?.to_string(),
        schema_authority: "current_binary_initializer",
        source_rows_verified: true,
    })
}

pub fn import_compatible(
    input_path: &Path,
    destination: &Path,
    expected_archive_id: &str,
    if_identical: bool,
) -> Result<MigrationOutcome> {
    let mut file = import::open_input(input_path)?;
    let inspected = inspect(&mut file, expected_archive_id)?;
    let source_version = parse_source_version(&inspected.header)?;
    let target = target_version()?;

    if source_version == target {
        let (header, completion, created) = import::import_inspected_file(
            file,
            destination,
            &inspected.header,
            &inspected.completion,
            if_identical,
        )?;
        return Ok(MigrationOutcome {
            header,
            completion,
            created,
            migration: None,
        });
    }
    require_reviewed_transition(source_version, target)?;

    let _lock = DestinationLock::acquire(destination)?;
    if if_identical && fs::symlink_metadata(destination).is_ok() {
        let admitted = identity(destination)?;
        verify_persisted_projection(
            &mut file,
            destination,
            &inspected,
            Some(admitted),
            false,
        )
        .map_err(|error| {
            let message = format!(
                "cannot verify the existing destination as a reviewed migration; nothing was replaced: {error}"
            );
            error.context(message)
        })?;
        return Ok(MigrationOutcome {
            header: inspected.header.clone(),
            completion: inspected.completion.clone(),
            created: false,
            migration: Some(migration_receipt(&inspected)?),
        });
    }

    require_new_destination(destination)?;
    let staging = tempfile::Builder::new()
        .prefix(".cass-migrate-")
        .tempdir_in(export::parent(destination)?)?;
    let replay_path = staging.path().join("agent_search.db");
    let candidate = staging.path().join("publication.db");

    let storage = SqliteStorage::open(&replay_path)
        .context("cannot initialize reviewed migration candidate")?;
    drop(storage);
    let connection = Connection::open(export::path_text(&replay_path)?)
        .map_err(|error| database_failure("cannot open the private migration database", error))?;
    connection
        .execute("PRAGMA busy_timeout = 5000")
        .map_err(|error| {
            database_failure("cannot set the private migration busy timeout", error)
        })?;
    let mode = connection
        .query_row("PRAGMA journal_mode = WAL")
        .map_err(|error| {
            database_failure(
                "cannot enable WAL for private reviewed migration replay",
                error,
            )
        })?
        .get_typed::<String>(0)
        .map_err(|error| {
            database_failure("cannot decode the private migration journal mode", error)
        })?;
    ensure!(
        mode.eq_ignore_ascii_case("wal"),
        "cannot enable WAL for private reviewed migration replay"
    );
    connection
        .execute("PRAGMA synchronous = FULL")
        .map_err(|error| {
            database_failure("cannot require durable private migration writes", error)
        })?;

    file.seek(SeekFrom::Start(0))?;
    let mut bounded = Input::new(BufReader::new(&mut file));
    restore_reviewed(&connection, &mut bounded, &inspected)?;
    materialize_candidate(&connection, &candidate)?;
    connection.close().map_err(|error| {
        database_failure(
            "cannot close the materialized private migration database",
            error,
        )
    })?;

    verify_persisted_projection(&mut file, &candidate, &inspected, None, true)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&candidate, fs::Permissions::from_mode(0o600))?;
    }
    sync_candidate(&candidate)?;
    require_new_destination(destination)?;
    fs::hard_link(&candidate, destination).context(
        "cannot publish migrated archive without replacing existing data; destination must support hard links",
    )?;
    export::sync_parent(destination)?;

    Ok(MigrationOutcome {
        header: inspected.header.clone(),
        completion: inspected.completion.clone(),
        created: true,
        migration: Some(migration_receipt(&inspected)?),
    })
}
