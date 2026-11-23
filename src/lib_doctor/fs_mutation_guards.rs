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

#[cfg(test)]
thread_local! {
    static DOCTOR_TEST_FAIL_NEXT_FILE_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static DOCTOR_TEST_FAIL_NEXT_DIRECTORY_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static DOCTOR_TEST_FAIL_NEXT_RENAME: std::cell::Cell<Option<DoctorTestRenameFailure>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum DoctorTestRenameFailure {
    CrossDevice,
    PermissionDenied,
}

#[cfg(test)]
pub(crate) fn doctor_test_inject_next_file_sync_failure() {
    DOCTOR_TEST_FAIL_NEXT_FILE_SYNC.with(|fail_next| fail_next.set(true));
}

#[cfg(test)]
pub(crate) fn doctor_test_inject_next_directory_sync_failure() {
    DOCTOR_TEST_FAIL_NEXT_DIRECTORY_SYNC.with(|fail_next| fail_next.set(true));
}

#[cfg(test)]
pub(crate) fn doctor_test_inject_next_cross_device_rename_failure() {
    DOCTOR_TEST_FAIL_NEXT_RENAME.with(|fail_next| {
        fail_next.set(Some(DoctorTestRenameFailure::CrossDevice));
    });
}

#[cfg(test)]
pub(crate) fn doctor_test_inject_next_permission_denied_rename_failure() {
    DOCTOR_TEST_FAIL_NEXT_RENAME.with(|fail_next| {
        fail_next.set(Some(DoctorTestRenameFailure::PermissionDenied));
    });
}

#[cfg(test)]
pub(crate) fn doctor_test_take_next_file_sync_failure() -> bool {
    DOCTOR_TEST_FAIL_NEXT_FILE_SYNC.with(|fail_next| fail_next.replace(false))
}

#[cfg(not(test))]
pub(crate) fn doctor_test_take_next_file_sync_failure() -> bool {
    false
}

#[cfg(test)]
pub(crate) fn doctor_test_take_next_directory_sync_failure() -> bool {
    DOCTOR_TEST_FAIL_NEXT_DIRECTORY_SYNC.with(|fail_next| fail_next.replace(false))
}

#[cfg(test)]
pub(crate) fn doctor_test_take_next_rename_failure() -> Option<DoctorTestRenameFailure> {
    DOCTOR_TEST_FAIL_NEXT_RENAME.with(|fail_next| fail_next.replace(None))
}

#[cfg(not(test))]
pub(crate) fn doctor_test_take_next_directory_sync_failure() -> bool {
    false
}

#[cfg(not(test))]
pub(crate) fn doctor_test_take_next_rename_failure() -> Option<()> {
    None
}

pub(crate) fn sync_file(path: &Path, label: &str) -> Result<(), String> {
    if doctor_test_take_next_file_sync_failure() {
        return Err(format!(
            "injected test failure syncing {label} {}",
            path.display()
        ));
    }

    #[cfg(windows)]
    {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .and_then(|file| file.sync_all())
            .map_err(|err| format!("failed to sync {label} {}: {err}", path.display()))
    }
    #[cfg(not(windows))]
    {
        std::fs::File::open(path)
            .and_then(|file| file.sync_all())
            .map_err(|err| format!("failed to sync {label} {}: {err}", path.display()))
    }
}

pub(crate) fn sync_directory(path: &Path) -> Result<(), String> {
    if doctor_test_take_next_directory_sync_failure() {
        return Err(format!(
            "injected test failure syncing directory {}",
            path.display()
        ));
    }

    #[cfg(windows)]
    {
        Ok(())
    }
    #[cfg(not(windows))]
    {
        std::fs::File::open(path)
            .and_then(|file| file.sync_all())
            .map_err(|err| format!("failed to sync directory {}: {err}", path.display()))
    }
}

pub(crate) fn doctor_rename_file(source_path: &Path, target_path: &Path) -> io::Result<()> {
    #[cfg(test)]
    if let Some(failure) = doctor_test_take_next_rename_failure() {
        return match failure {
            DoctorTestRenameFailure::CrossDevice => Err(io::Error::new(
                io::ErrorKind::CrossesDevices,
                "injected test cross-device rename failure",
            )),
            DoctorTestRenameFailure::PermissionDenied => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected test permission denied rename failure",
            )),
        };
    }

    #[cfg(not(test))]
    let _ = doctor_test_take_next_rename_failure();

    if let Some(err) = doctor_env_rename_failure(source_path, target_path) {
        return Err(err);
    }

    std::fs::rename(source_path, target_path)
}

pub(crate) fn doctor_env_rename_failure(
    source_path: &Path,
    target_path: &Path,
) -> Option<io::Error> {
    let raw = dotenvy::var(CASS_TEST_DOCTOR_RENAME_FAILURE).ok()?;
    let mode = raw.trim().to_ascii_lowercase();
    if mode.is_empty() || matches!(mode.as_str(), "0" | "false" | "off") {
        return None;
    }
    let source_name = source_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let target_name = target_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    if source_name.contains(".cross-device-replace.tmp")
        || target_name.contains(".cross-device-replace.tmp")
    {
        return None;
    }
    match mode.as_str() {
        "cross-device" | "cross_device" | "exdev" => Some(io::Error::new(
            io::ErrorKind::CrossesDevices,
            "injected test cross-device rename failure from CASS_TEST_DOCTOR_RENAME_FAILURE",
        )),
        "permission-denied" | "permission_denied" | "eperm" => Some(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "injected test permission denied rename failure from CASS_TEST_DOCTOR_RENAME_FAILURE",
        )),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DoctorFsMoveMethod {
    AtomicRename,
    CrossDeviceCopyRemoveSource,
}

impl DoctorFsMoveMethod {
    pub(crate) fn receipt_fallback_kind(self) -> &'static str {
        match self {
            Self::AtomicRename => "none",
            Self::CrossDeviceCopyRemoveSource => "cross_device_copy_remove_source",
        }
    }

    pub(crate) fn receipt_precondition_check(self) -> &'static str {
        match self {
            Self::AtomicRename => "filesystem_rename_completed",
            Self::CrossDeviceCopyRemoveSource => "filesystem_cross_device_copy_remove_completed",
        }
    }
}

pub(crate) fn doctor_is_cross_device_rename_error(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::CrossesDevices
}

pub(crate) fn doctor_remove_partial_fallback_target(target_path: &Path) -> Result<(), String> {
    match std::fs::remove_file(target_path) {
        Ok(()) => {
            if let Some(parent) = target_path.parent() {
                sync_directory(parent).map_err(|err| {
                    format!(
                        "removed partial cross-device fallback target {} but failed to sync rollback parent: {err}",
                        target_path.display()
                    )
                })?;
            }
            Ok(())
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!(
            "failed to roll back partial cross-device fallback target {}: {err}",
            target_path.display()
        )),
    }
}

pub(crate) fn doctor_rollback_fallback_target_error(target_path: &Path, failure: String) -> String {
    match doctor_remove_partial_fallback_target(target_path) {
        Ok(()) => format!(
            "{failure}; rolled back copied target {}",
            target_path.display()
        ),
        Err(rollback_err) => format!("{failure}; {rollback_err}"),
    }
}

pub(crate) fn doctor_move_file_with_cross_device_fallback(
    source_path: &Path,
    target_path: &Path,
    expected_blake3: &str,
    expected_bytes: u64,
    operation_label: &str,
    target_sync_label: &str,
) -> Result<DoctorFsMoveMethod, String> {
    match doctor_rename_file(source_path, target_path) {
        Ok(()) => return Ok(DoctorFsMoveMethod::AtomicRename),
        Err(err) if !doctor_is_cross_device_rename_error(&err) => {
            return Err(format!(
                "failed to atomically {operation_label} {} to {}: {err}",
                source_path.display(),
                target_path.display()
            ));
        }
        Err(_) => {}
    }

    match std::fs::symlink_metadata(target_path) {
        Ok(_) => {
            return Err(format!(
                "refusing cross-device {operation_label} fallback over existing target {}",
                target_path.display()
            ));
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(format!(
                "refusing cross-device {operation_label} fallback to {}: could not verify target absence: {err}",
                target_path.display()
            ));
        }
    }

    let copied_bytes = (|| -> io::Result<u64> {
        let mut source = std::fs::File::open(source_path)?;
        let mut target = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(target_path)?;
        let copied = io::copy(&mut source, &mut target)?;
        use std::io::Write as _;
        target.flush()?;
        Ok(copied)
    })()
    .map_err(|err| {
        doctor_rollback_fallback_target_error(
            target_path,
            format!(
                "failed cross-device {operation_label} fallback copy {} to {}: {err}",
                source_path.display(),
                target_path.display()
            ),
        )
    })?;

    if copied_bytes != expected_bytes {
        return Err(doctor_rollback_fallback_target_error(
            target_path,
            format!(
                "cross-device {operation_label} fallback copied {copied_bytes} byte(s), expected {expected_bytes}"
            ),
        ));
    }

    let target_blake3 = file_blake3_hex(target_path).map_err(|err| {
        doctor_rollback_fallback_target_error(
            target_path,
            format!("failed to verify cross-device {operation_label} fallback target: {err}"),
        )
    })?;
    if target_blake3 != expected_blake3 {
        return Err(doctor_rollback_fallback_target_error(
            target_path,
            format!(
                "cross-device {operation_label} fallback target {} hash mismatch: expected source blake3 {expected_blake3}, observed target blake3 {target_blake3}",
                target_path.display()
            ),
        ));
    }

    sync_file(target_path, target_sync_label).map_err(|err| {
        doctor_rollback_fallback_target_error(
            target_path,
            format!("failed cross-device {operation_label} fallback target sync: {err}"),
        )
    })?;

    match std::fs::remove_file(source_path) {
        Ok(()) => Ok(DoctorFsMoveMethod::CrossDeviceCopyRemoveSource),
        Err(err) => Err(doctor_rollback_fallback_target_error(
            target_path,
            format!(
                "failed to remove cross-device {operation_label} fallback source {} after verified copy: {err}",
                source_path.display()
            ),
        )),
    }
}

pub(crate) fn cleanup_target_path_is_safe(
    path: &Path,
    data_dir: &Path,
    db_path: &Path,
    index_path: &Path,
) -> bool {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return false;
    };
    if metadata.file_type().is_symlink() {
        return false;
    }
    if !path.starts_with(data_dir) || path == data_dir || path == db_path || path == index_path {
        return false;
    }
    if path_has_symlink_below_root(path, data_dir) {
        return false;
    }
    let Ok(canonical_path) = std::fs::canonicalize(path) else {
        return false;
    };
    let Ok(canonical_data_dir) = std::fs::canonicalize(data_dir) else {
        return false;
    };
    if !canonical_path.starts_with(&canonical_data_dir) || canonical_path == canonical_data_dir {
        return false;
    }
    if std::fs::canonicalize(db_path)
        .ok()
        .is_some_and(|canonical_db_path| canonical_path == canonical_db_path)
    {
        return false;
    }
    if let Ok(canonical_index_path) = std::fs::canonicalize(index_path)
        && (canonical_path == canonical_index_path
            || canonical_path.starts_with(&canonical_index_path))
    {
        return false;
    }
    if path.starts_with(index_path) {
        return false;
    }
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return false;
    }
    let publish_backup_parent = index_path
        .parent()
        .unwrap_or(index_path)
        .join(".lexical-publish-backups");
    let is_retained_publish_backup = path
        .parent()
        .map(|parent| parent == publish_backup_parent)
        .unwrap_or(false);
    let is_manifest_backed_generation = path
        .join(crate::indexer::lexical_generation::LEXICAL_GENERATION_MANIFEST_FILE)
        .is_file();
    is_retained_publish_backup || is_manifest_backed_generation
}

pub(crate) fn legacy_index_lock_path_is_safe(path: &Path, data_dir: &Path) -> bool {
    let expected = data_dir.join(".index.lock");
    if path != expected {
        return false;
    }
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return false;
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return false;
    }
    if path_has_symlink_below_root(path, data_dir) {
        return false;
    }
    let Ok(canonical_parent) =
        std::fs::canonicalize(path.parent().unwrap_or_else(|| Path::new(".")))
    else {
        return false;
    };
    let Ok(canonical_data_dir) = std::fs::canonicalize(data_dir) else {
        return false;
    };
    canonical_parent == canonical_data_dir
}

pub(crate) fn doctor_copy_source_path_is_safe(path: &Path, data_dir: &Path) -> bool {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return false;
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return false;
    }
    if !path.starts_with(data_dir) || path_has_symlink_below_root(path, data_dir) {
        return false;
    }
    let Ok(canonical_path) = std::fs::canonicalize(path) else {
        return false;
    };
    let Ok(canonical_data_dir) = std::fs::canonicalize(data_dir) else {
        return false;
    };
    canonical_path.starts_with(canonical_data_dir)
}

pub(crate) fn doctor_staging_target_path_is_safe(
    path: &Path,
    staging_root: &Path,
    data_dir: &Path,
) -> bool {
    if path == staging_root
        || !path.starts_with(staging_root)
        || !staging_root.starts_with(data_dir)
    {
        return false;
    }
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return false;
    }
    let Ok(staging_metadata) = std::fs::symlink_metadata(staging_root) else {
        return false;
    };
    if !staging_metadata.is_dir() || staging_metadata.file_type().is_symlink() {
        return false;
    }
    if path_has_symlink_below_root(staging_root, data_dir) {
        return false;
    }
    let parent = path.parent().unwrap_or(staging_root);
    if !parent.exists() || existing_path_has_symlink_below_root(parent, staging_root) {
        return false;
    }
    let Ok(canonical_data_dir) = std::fs::canonicalize(data_dir) else {
        return false;
    };
    let Ok(canonical_staging_root) = std::fs::canonicalize(staging_root) else {
        return false;
    };
    if !canonical_staging_root.starts_with(&canonical_data_dir) {
        return false;
    }
    match std::fs::canonicalize(parent) {
        Ok(canonical_parent) => canonical_parent.starts_with(canonical_staging_root),
        Err(_) => false,
    }
}

pub(crate) fn doctor_staged_source_path_is_safe(
    path: &Path,
    staging_root: &Path,
    data_dir: &Path,
) -> bool {
    if path == staging_root || !path.starts_with(staging_root) {
        return false;
    }
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return false;
    }
    if !doctor_copy_source_path_is_safe(path, data_dir) {
        return false;
    }
    let Ok(staging_metadata) = std::fs::symlink_metadata(staging_root) else {
        return false;
    };
    if !staging_metadata.is_dir() || staging_metadata.file_type().is_symlink() {
        return false;
    }
    if path_has_symlink_below_root(staging_root, data_dir)
        || existing_path_has_symlink_below_root(path, staging_root)
    {
        return false;
    }
    let Ok(canonical_path) = std::fs::canonicalize(path) else {
        return false;
    };
    let Ok(canonical_staging_root) = std::fs::canonicalize(staging_root) else {
        return false;
    };
    canonical_path.starts_with(canonical_staging_root)
}

pub(crate) fn doctor_staged_rename_target_path_is_safe(
    path: &Path,
    staging_root: &Path,
    data_dir: &Path,
) -> bool {
    if path == data_dir || !path.starts_with(data_dir) || path.starts_with(staging_root) {
        return false;
    }
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return false;
    }
    let Some(parent) = path.parent() else {
        return false;
    };
    if !parent.is_dir() || path_has_symlink_below_root(parent, data_dir) {
        return false;
    }
    let Ok(canonical_parent) = std::fs::canonicalize(parent) else {
        return false;
    };
    let Ok(canonical_data_dir) = std::fs::canonicalize(data_dir) else {
        return false;
    };
    if !canonical_parent.starts_with(&canonical_data_dir) {
        return false;
    }
    if let Ok(canonical_staging_root) = std::fs::canonicalize(staging_root)
        && canonical_parent.starts_with(canonical_staging_root)
    {
        return false;
    }
    true
}

pub(crate) fn doctor_quarantine_source_path_is_safe(
    path: &Path,
    data_dir: &Path,
    db_path: &Path,
    index_path: &Path,
    asset_class: DoctorAssetClass,
) -> bool {
    if path == data_dir || path == db_path || path == index_path || !path.starts_with(data_dir) {
        return false;
    }
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return false;
    }
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return false;
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return false;
    }
    if path_has_symlink_below_root(path, data_dir) {
        return false;
    }
    let Ok(canonical_path) = std::fs::canonicalize(path) else {
        return false;
    };
    let Ok(canonical_data_dir) = std::fs::canonicalize(data_dir) else {
        return false;
    };
    if !canonical_path.starts_with(&canonical_data_dir) {
        return false;
    }
    if std::fs::canonicalize(db_path)
        .ok()
        .is_some_and(|canonical_db_path| canonical_path == canonical_db_path)
    {
        return false;
    }
    if let Ok(canonical_index_path) = std::fs::canonicalize(index_path)
        && canonical_path == canonical_index_path
    {
        return false;
    }
    let index_root = index_path.parent().unwrap_or(index_path);
    match asset_class {
        DoctorAssetClass::DerivedLexicalIndex => path.starts_with(index_path),
        DoctorAssetClass::RetainedPublishBackup => {
            path.starts_with(index_root.join(".lexical-publish-backups"))
        }
        DoctorAssetClass::DerivedSemanticIndex => path.starts_with(data_dir.join("semantic")),
        DoctorAssetClass::MemoCache | DoctorAssetClass::ReclaimableDerivedCache => {
            path.starts_with(data_dir.join("cache"))
        }
        _ => false,
    }
}

pub(crate) fn path_age_seconds(path: &Path) -> Option<u64> {
    std::fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| modified.elapsed().ok())
        .map(|elapsed| elapsed.as_secs())
}

pub(crate) fn path_has_symlink_below_root(path: &Path, root: &Path) -> bool {
    let mut current = path;
    loop {
        if current == root {
            return false;
        }
        let Ok(metadata) = std::fs::symlink_metadata(current) else {
            return true;
        };
        if metadata.file_type().is_symlink() {
            return true;
        }
        let Some(parent) = current.parent() else {
            return true;
        };
        if parent == current {
            return true;
        }
        current = parent;
    }
}

pub(crate) fn existing_path_has_symlink_below_root(path: &Path, root: &Path) -> bool {
    let mut current = path;
    loop {
        if current == root {
            return false;
        }
        match std::fs::symlink_metadata(current) {
            Ok(metadata) if metadata.file_type().is_symlink() => return true,
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return true,
        }
        let Some(parent) = current.parent() else {
            return true;
        };
        if parent == current {
            return true;
        }
        current = parent;
    }
}
