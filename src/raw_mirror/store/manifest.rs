use super::io::{
    RAW_MIRROR_BLOB_EXTENSION, RAW_MIRROR_FIXED_CHUNKS_KIND, RAW_MIRROR_HASH_ALGORITHM,
    RAW_MIRROR_MANIFEST_KIND, RAW_MIRROR_MANIFEST_MAX_BYTES, RAW_MIRROR_SCHEMA_VERSION,
    acquire_raw_mirror_mutation_lock, ensure_private_dir_descendant, file_blake3,
    private_create_new_file, raw_mirror_checkpoint, raw_mirror_io,
    raw_mirror_path_has_symlink_below_root, raw_mirror_record_progress, raw_mirror_root,
    raw_mirror_symlink_metadata, raw_mirror_tracked_io, remove_empty_temp_dir_best_effort,
    remove_temp_best_effort, sync_file, sync_open_file_if_required, sync_parent,
    unique_capture_temp_dir, unique_temp_path,
};
#[cfg(test)]
use super::io::{RAW_MIRROR_CHUNK_SIZE_BYTES, RAW_MIRROR_CHUNK_THRESHOLD_BYTES};
use super::policy;
#[cfg(test)]
use super::types::{PreparedRawMirrorContent, RawMirrorCaptureInput, RawMirrorCaptureRecord};
use super::types::{
    RawMirrorChunkRef, RawMirrorContentStorage, RawMirrorDbLink, RawMirrorManifestFile,
    RawMirrorVerifiedCapture,
};
use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

// Store codec tests route through `policy::capture` so test and production share
// a single capture and admission implementation.
#[cfg(test)]
pub fn capture_source_file(input: RawMirrorCaptureInput<'_>) -> Result<RawMirrorCaptureRecord> {
    capture_source_file_with_chunk_policy(
        input,
        RAW_MIRROR_CHUNK_THRESHOLD_BYTES,
        RAW_MIRROR_CHUNK_SIZE_BYTES,
    )
}

#[cfg(test)]
pub fn capture_source_file_with_chunk_policy(
    input: RawMirrorCaptureInput<'_>,
    chunk_threshold_bytes: u64,
    chunk_size_bytes: usize,
) -> Result<RawMirrorCaptureRecord> {
    policy::capture(
        input,
        chunk_threshold_bytes,
        chunk_size_bytes,
        policy::RawMirrorRetentionPolicy::from_env()?,
    )
}

pub fn merge_manifest_db_links(
    data_dir: &Path,
    manifest_relative_path: &str,
    links: &[RawMirrorDbLink],
) -> Result<()> {
    if links.is_empty() {
        return Ok(());
    }
    let root = raw_mirror_root(data_dir);
    let _mutation_lock = acquire_raw_mirror_mutation_lock(&root)?;
    let manifest_path = raw_mirror_manifest_path_from_relative(&root, manifest_relative_path)?;
    merge_raw_mirror_manifest_db_links(&root, &manifest_path, links, None)
}

#[cfg(test)]
pub(in crate::raw_mirror) fn prepare_source_content(
    root: &Path,
    source_path: &Path,
    _temp_dir: &Path,
    source_metadata: &fs::Metadata,
    chunk_threshold_bytes: u64,
    chunk_size_bytes: usize,
) -> Result<PreparedRawMirrorContent> {
    policy::prepare_for_test(
        root,
        source_path,
        source_metadata,
        chunk_threshold_bytes,
        chunk_size_bytes,
    )
}

pub(in crate::raw_mirror) fn content_addressed_blob_is_verified(
    root: &Path,
    reference: &RawMirrorChunkRef,
) -> Result<bool> {
    let path = root.join(&reference.blob_relative_path);
    if let Some(parent) = path.parent()
        && raw_mirror_path_has_symlink_below_root(root, parent)
    {
        return Err(anyhow!(
            "refusing to use symlink raw mirror dir {}; raw mirror blob path {} contains a symlink",
            parent.display(),
            path.display()
        ));
    }
    if raw_mirror_path_has_symlink_below_root(root, &path) {
        return Err(anyhow!(
            "raw mirror blob path {} contains a symlink",
            path.display()
        ));
    }
    match raw_mirror_symlink_metadata(&path) {
        Ok(_) => {
            verify_existing_blob_reference(root, reference)?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => {
            Err(error).with_context(|| format!("stat raw mirror blob {}", path.display()))
        }
    }
}

pub(in crate::raw_mirror) fn open_stable_source_file(
    source_path: &Path,
    expected_metadata: &fs::Metadata,
) -> Result<File> {
    let source = raw_mirror_io("open_source", source_path, || File::open(source_path))
        .with_context(|| format!("open raw mirror source {}", source_path.display()))?;
    let opened_metadata = raw_mirror_tracked_io("fstat_source", source_path, || source.metadata())
        .with_context(|| format!("stat opened raw mirror source {}", source_path.display()))?;
    if !same_source_identity(expected_metadata, &opened_metadata) {
        return Err(anyhow!(
            "raw mirror source {} changed identity before capture",
            source_path.display()
        ));
    }
    let current_path_metadata = raw_mirror_symlink_metadata(source_path)
        .with_context(|| format!("restat raw mirror source {}", source_path.display()))?;
    if current_path_metadata.file_type().is_symlink() {
        return Err(anyhow!(
            "refusing to raw-mirror symlink source {}",
            source_path.display()
        ));
    }
    if !same_source_identity(expected_metadata, &current_path_metadata) {
        return Err(anyhow!(
            "raw mirror source {} changed identity before capture",
            source_path.display()
        ));
    }
    Ok(source)
}

#[cfg(unix)]
pub(in crate::raw_mirror) fn same_source_identity(
    expected: &fs::Metadata,
    actual: &fs::Metadata,
) -> bool {
    use std::os::unix::fs::MetadataExt;
    actual.is_file() && expected.dev() == actual.dev() && expected.ino() == actual.ino()
}

#[cfg(not(unix))]
pub(in crate::raw_mirror) fn same_source_identity(
    _expected: &fs::Metadata,
    actual: &fs::Metadata,
) -> bool {
    actual.is_file()
}

#[cfg(unix)]
pub(in crate::raw_mirror) fn source_identity_token(metadata: &fs::Metadata) -> Option<String> {
    use std::os::unix::fs::MetadataExt;
    Some(format!("{}:{}", metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
pub(in crate::raw_mirror) fn source_identity_token(_metadata: &fs::Metadata) -> Option<String> {
    None
}

#[cfg(unix)]
pub(in crate::raw_mirror) fn source_change_time_ns(metadata: &fs::Metadata) -> Option<u128> {
    use std::os::unix::fs::MetadataExt;

    let seconds = u128::try_from(metadata.ctime()).ok()?;
    let nanoseconds = u128::try_from(metadata.ctime_nsec()).ok()?;
    Some(
        seconds
            .saturating_mul(1_000_000_000)
            .saturating_add(nanoseconds),
    )
}

#[cfg(not(unix))]
pub(in crate::raw_mirror) fn source_change_time_ns(_metadata: &fs::Metadata) -> Option<u128> {
    None
}

pub(in crate::raw_mirror) fn source_file_changed_during_capture(
    initial: &fs::Metadata,
    final_metadata: &fs::Metadata,
) -> bool {
    if !same_source_identity(initial, final_metadata) {
        return true;
    }
    if initial.len() != final_metadata.len() {
        return true;
    }
    if let (Some(initial_change), Some(final_change)) = (
        source_change_time_ns(initial),
        source_change_time_ns(final_metadata),
    ) && initial_change != final_change
    {
        return true;
    }
    match (initial.modified().ok(), final_metadata.modified().ok()) {
        (Some(initial_mtime), Some(final_mtime)) => initial_mtime != final_mtime,
        _ => false,
    }
}

pub(in crate::raw_mirror) fn source_path_changed_identity_during_capture(
    source_path: &Path,
    initial: &fs::Metadata,
) -> bool {
    match raw_mirror_symlink_metadata(source_path) {
        Ok(current) => current.file_type().is_symlink() || !same_source_identity(initial, &current),
        Err(_) => true,
    }
}

pub(in crate::raw_mirror) fn publish_content_addressed_temp(
    root: &Path,
    temp_path: &Path,
    final_path: &Path,
    expected_blake3: &str,
) -> Result<bool> {
    let expected_relative_path = raw_mirror_blob_relative_path(expected_blake3)
        .ok_or_else(|| anyhow!("computed invalid raw mirror blake3 digest"))?;
    if final_path != root.join(&expected_relative_path) {
        return Err(anyhow!(
            "raw mirror blob publish target {} is not the canonical path for {}",
            final_path.display(),
            expected_blake3
        ));
    }
    let temp_size_bytes = raw_mirror_symlink_metadata(temp_path)
        .with_context(|| format!("stat raw mirror temp {}", temp_path.display()))?
        .len();
    let reference = RawMirrorChunkRef {
        blob_relative_path: expected_relative_path,
        blob_blake3: expected_blake3.to_string(),
        blob_size_bytes: temp_size_bytes,
    };
    ensure_private_dir_descendant(
        root,
        final_path
            .parent()
            .ok_or_else(|| anyhow!("raw mirror blob path has no parent"))?,
    )?;
    match raw_mirror_symlink_metadata(final_path) {
        Ok(_) => {
            verify_existing_blob_reference(root, &reference)?;
            remove_temp_best_effort(temp_path);
            return Ok(true);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("stat raw mirror blob {}", final_path.display()));
        }
    }

    match raw_mirror_tracked_io("publish_blob", final_path, || {
        fs::hard_link(temp_path, final_path)
    }) {
        Ok(()) => {
            sync_file(final_path)?;
            sync_parent(final_path)?;
            remove_temp_best_effort(temp_path);
            Ok(false)
        }
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            verify_existing_blob_reference(root, &reference)?;
            remove_temp_best_effort(temp_path);
            Ok(true)
        }
        Err(err) => Err(anyhow!(
            "publish raw mirror blob {} from {}: {err}",
            final_path.display(),
            temp_path.display()
        )),
    }
}

pub(in crate::raw_mirror) fn publish_manifest_bytes_create_new(
    root: &Path,
    manifest_path: &Path,
    manifest_bytes: &[u8],
    blob_blake3: &str,
) -> Result<bool> {
    ensure_private_dir_descendant(
        root,
        manifest_path
            .parent()
            .ok_or_else(|| anyhow!("raw mirror manifest path has no parent"))?,
    )?;
    match raw_mirror_symlink_metadata(manifest_path) {
        Ok(_) => {
            verify_existing_manifest(root, manifest_path, blob_blake3)?;
            return Ok(true);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "inspect raw mirror manifest {} before publication",
                    manifest_path.display()
                )
            });
        }
    }

    let temp_dir = unique_capture_temp_dir(root);
    ensure_private_dir_descendant(root, &temp_dir)?;
    let temp_path = unique_temp_path(&temp_dir, "manifest");
    let mut temp = private_create_new_file(&temp_path)?;
    raw_mirror_tracked_io("write_manifest", &temp_path, || {
        temp.write_all(manifest_bytes)
    })
    .with_context(|| format!("write raw mirror manifest temp {}", temp_path.display()))?;
    sync_open_file_if_required(&temp, &temp_path, || {
        format!("sync raw mirror manifest temp {}", temp_path.display())
    })?;

    match raw_mirror_tracked_io("publish_manifest", manifest_path, || {
        fs::hard_link(&temp_path, manifest_path)
    }) {
        Ok(()) => {
            sync_file(manifest_path)?;
            sync_parent(manifest_path)?;
            remove_temp_best_effort(&temp_path);
            remove_empty_temp_dir_best_effort(&temp_dir);
            Ok(false)
        }
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            verify_existing_manifest(root, manifest_path, blob_blake3)?;
            remove_temp_best_effort(&temp_path);
            remove_empty_temp_dir_best_effort(&temp_dir);
            Ok(true)
        }
        Err(err) => Err(anyhow!(
            "publish raw mirror manifest {} from {}: {err}",
            manifest_path.display(),
            temp_path.display()
        )),
    }
}

pub(in crate::raw_mirror) fn merge_raw_mirror_manifest_db_links(
    root: &Path,
    manifest_path: &Path,
    links: &[RawMirrorDbLink],
    expected_blob_blake3: Option<&str>,
) -> Result<()> {
    if links.is_empty() {
        return Ok(());
    }

    // All callers hold this root's mutation file lock. A second process-wide
    // mutex held across the read/write used to stall unrelated mirrors behind
    // a slow filesystem without exposing the waiter or observing cancellation.
    if raw_mirror_path_has_symlink_below_root(root, manifest_path) {
        return Err(anyhow!("raw mirror manifest path contains a symlink"));
    }
    let mut manifest = read_raw_mirror_manifest(manifest_path)?;
    let expected_manifest_path =
        root.join(raw_mirror_manifest_relative_path(&manifest.manifest_id));
    if manifest_path != expected_manifest_path {
        return Err(anyhow!(
            "raw mirror manifest {} is not at its canonical path {}",
            manifest_path.display(),
            expected_manifest_path.display()
        ));
    }
    // Validate the snapshot already read. A second read both repeats metadata
    // work for every conversation and needlessly separates identity validation
    // from the descriptor whose links are being changed.
    validate_raw_mirror_manifest_contents(&manifest, &manifest.manifest_id)?;
    if let Some(expected_blob_blake3) = expected_blob_blake3
        && manifest.blob_blake3 != expected_blob_blake3
    {
        return Err(anyhow!(
            "existing raw mirror manifest {} points at blob {}, expected {}",
            manifest_path.display(),
            manifest.blob_blake3,
            expected_blob_blake3
        ));
    }

    let mut merged_links = manifest.db_links.clone();
    merged_links.extend_from_slice(links);
    let merged_links = unique_db_links(&merged_links);
    if merged_links == manifest.db_links {
        return Ok(());
    }

    manifest.db_links = merged_links;
    manifest.manifest_blake3 = Some(raw_mirror_manifest_blake3(&manifest));
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    if manifest_bytes.len() as u64 > RAW_MIRROR_MANIFEST_MAX_BYTES {
        return Err(anyhow!(
            "refusing to replace raw mirror manifest {} with {} bytes because the validation limit is {} bytes",
            manifest_path.display(),
            manifest_bytes.len(),
            RAW_MIRROR_MANIFEST_MAX_BYTES
        ));
    }
    // An already-linked conversation is a read-only no-op, including the
    // cross-process usage fence. Fence only an actual replacement.
    policy::note_mutation(root)?;
    replace_raw_mirror_bytes(root, manifest_path, &manifest_bytes)
}

pub(in crate::raw_mirror) fn replace_raw_mirror_bytes(
    root: &Path,
    manifest_path: &Path,
    manifest_bytes: &[u8],
) -> Result<()> {
    let previous_size = match raw_mirror_symlink_metadata(manifest_path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error).context("inspect raw mirror replacement size"),
    };
    let result = (|| -> Result<()> {
        ensure_private_dir_descendant(
            root,
            manifest_path
                .parent()
                .ok_or_else(|| anyhow!("raw mirror manifest path has no parent"))?,
        )?;
        let temp_dir = unique_capture_temp_dir(root);
        ensure_private_dir_descendant(root, &temp_dir)?;
        let temp_path = unique_temp_path(&temp_dir, "manifest-update");
        let mut temp = private_create_new_file(&temp_path)?;
        raw_mirror_tracked_io("write_manifest", &temp_path, || {
            temp.write_all(manifest_bytes)
        })
        .with_context(|| {
            format!(
                "write raw mirror manifest update temp {}",
                temp_path.display()
            )
        })?;
        sync_open_file_if_required(&temp, &temp_path, || {
            format!(
                "sync raw mirror manifest update temp {}",
                temp_path.display()
            )
        })?;
        drop(temp);

        raw_mirror_tracked_io("replace_manifest", manifest_path, || {
            fs::rename(&temp_path, manifest_path)
        })
        .with_context(|| {
            format!(
                "replace raw mirror manifest {} from {}",
                manifest_path.display(),
                temp_path.display()
            )
        })?;
        sync_parent(manifest_path)?;
        remove_empty_temp_dir_best_effort(&temp_dir);
        Ok(())
    })();
    if result.is_ok() {
        policy::record_file_size_change(root, previous_size, manifest_bytes.len() as u64);
    } else {
        // Failed replacement may leave retained staging bytes. Never reuse a
        // total that cannot account for the actual error outcome.
        policy::forget_measured_usage(root);
    }
    result
}

fn raw_mirror_manifest_path_from_relative(root: &Path, relative_path: &str) -> Result<PathBuf> {
    let relative = Path::new(relative_path);
    if relative.is_absolute() {
        return Err(anyhow!(
            "raw mirror manifest path must be relative: {relative_path}"
        ));
    }

    let mut normal_components = Vec::new();
    for component in relative.components() {
        match component {
            std::path::Component::Normal(part) => normal_components.push(part),
            _ => {
                return Err(anyhow!(
                    "raw mirror manifest path must use only normal relative components: {relative_path}"
                ));
            }
        }
    }

    if normal_components.len() != 2
        || normal_components[0] != std::ffi::OsStr::new("manifests")
        || Path::new(normal_components[1])
            .extension()
            .and_then(|ext| ext.to_str())
            != Some("json")
    {
        return Err(anyhow!(
            "raw mirror manifest path must match manifests/<id>.json: {relative_path}"
        ));
    }

    Ok(root.join(relative))
}

fn verify_existing_file(path: &Path, expected_blake3: &str) -> Result<()> {
    let metadata = raw_mirror_symlink_metadata(path)
        .with_context(|| format!("stat raw mirror blob {}", path.display()))?;
    if metadata.file_type().is_symlink() {
        return Err(anyhow!(
            "refusing to read symlink raw mirror blob {}",
            path.display()
        ));
    }
    if !metadata.is_file() {
        return Err(anyhow!(
            "refusing to read non-file raw mirror blob {}",
            path.display()
        ));
    }
    let actual = file_blake3(path)?;
    if actual == expected_blake3 {
        Ok(())
    } else {
        Err(anyhow!(
            "raw mirror blob checksum mismatch for {}: observed blake3 {}, expected {}",
            path.display(),
            actual,
            expected_blake3
        ))
    }
}

fn verify_existing_manifest(root: &Path, path: &Path, expected_blob_blake3: &str) -> Result<()> {
    validated_existing_manifest(root, path, expected_blob_blake3).map(|_| ())
}

/// Read exactly one descriptor snapshot, then bind its canonical path,
/// self-checksum, derived identity, and source-content identity together.
/// Callers authorizing deletion must not validate one read and compare fields
/// from a later read of the same path.
pub(in crate::raw_mirror) fn validated_existing_manifest(
    root: &Path,
    path: &Path,
    expected_blob_blake3: &str,
) -> Result<RawMirrorManifestFile> {
    let parsed_manifest = read_raw_mirror_manifest(path)?;
    let expected_manifest_path = root.join(raw_mirror_manifest_relative_path(
        &parsed_manifest.manifest_id,
    ));
    if path != expected_manifest_path {
        return Err(anyhow!(
            "existing raw mirror manifest {} is not at its canonical path {}",
            path.display(),
            expected_manifest_path.display()
        ));
    }
    validate_raw_mirror_manifest_contents(&parsed_manifest, &parsed_manifest.manifest_id)?;
    if parsed_manifest.blob_blake3 != expected_blob_blake3 {
        return Err(anyhow!(
            "existing raw mirror manifest {} points at blob {}, expected {}",
            path.display(),
            parsed_manifest.blob_blake3,
            expected_blob_blake3
        ));
    }
    Ok(parsed_manifest)
}

pub(in crate::raw_mirror) fn read_raw_mirror_manifest(
    path: &Path,
) -> Result<RawMirrorManifestFile> {
    let metadata = raw_mirror_symlink_metadata(path)
        .with_context(|| format!("stat raw mirror manifest {}", path.display()))?;
    if metadata.file_type().is_symlink() {
        return Err(anyhow!(
            "refusing to read symlink raw mirror manifest {}",
            path.display()
        ));
    }
    if !metadata.is_file() {
        return Err(anyhow!(
            "refusing to read non-file raw mirror manifest {}",
            path.display()
        ));
    }
    if metadata.len() > RAW_MIRROR_MANIFEST_MAX_BYTES {
        return Err(anyhow!(
            "refusing to read raw mirror manifest {} larger than {} bytes",
            path.display(),
            RAW_MIRROR_MANIFEST_MAX_BYTES
        ));
    }
    let mut file = open_stable_source_file(path, &metadata)?;
    read_raw_mirror_manifest_from_open_file(path, &mut file)
}

pub(in crate::raw_mirror) fn read_raw_mirror_manifest_from_open_file(
    path: &Path,
    file: &mut File,
) -> Result<RawMirrorManifestFile> {
    // A metadata check alone does not bound a file that grows before/during
    // reading. Read one sentinel byte beyond the limit, then reject before JSON
    // parsing rather than accepting a truncated prefix.
    let mut bytes = Vec::new();
    raw_mirror_io("read_manifest", path, || {
        file.take(RAW_MIRROR_MANIFEST_MAX_BYTES + 1)
            .read_to_end(&mut bytes)
    })
    .with_context(|| format!("read raw mirror manifest {}", path.display()))?;
    if bytes.len() as u64 > RAW_MIRROR_MANIFEST_MAX_BYTES {
        anyhow::bail!(
            "refusing to read raw mirror manifest {} larger than {} bytes",
            path.display(),
            RAW_MIRROR_MANIFEST_MAX_BYTES
        );
    }
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parse raw mirror manifest {}", path.display()))
}

pub(in crate::raw_mirror) fn raw_mirror_blob_relative_path(blob_blake3: &str) -> Option<String> {
    if blob_blake3.len() != 64 || !blob_blake3.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let lower = blob_blake3.to_ascii_lowercase();
    Some(format!(
        "blobs/{}/{}/{}.{}",
        RAW_MIRROR_HASH_ALGORITHM,
        &lower[..2],
        lower,
        RAW_MIRROR_BLOB_EXTENSION
    ))
}

pub(in crate::raw_mirror) fn raw_mirror_manifest_blob_references(
    manifest: &RawMirrorManifestFile,
) -> Result<Vec<RawMirrorChunkRef>> {
    let expected_descriptor_path = raw_mirror_blob_relative_path(&manifest.blob_blake3)
        .ok_or_else(|| anyhow!("raw mirror manifest has an invalid blob digest"))?;
    if manifest.blob_relative_path != expected_descriptor_path {
        return Err(anyhow!(
            "raw mirror manifest blob path does not match its digest"
        ));
    }

    let descriptor = RawMirrorChunkRef {
        blob_relative_path: manifest.blob_relative_path.clone(),
        blob_blake3: manifest.blob_blake3.clone(),
        blob_size_bytes: manifest.blob_size_bytes,
    };
    let Some(storage) = manifest.content_storage.as_ref() else {
        if manifest.source_size_bytes != manifest.blob_size_bytes {
            return Err(anyhow!(
                "whole-blob raw mirror source size does not match blob size"
            ));
        }
        if manifest
            .verification
            .content_blake3
            .as_deref()
            .is_some_and(|digest| digest != manifest.blob_blake3)
        {
            return Err(anyhow!(
                "whole-blob raw mirror content digest does not match blob digest"
            ));
        }
        return Ok(vec![descriptor]);
    };

    if storage.kind != RAW_MIRROR_FIXED_CHUNKS_KIND
        || storage.content_hash_algorithm != RAW_MIRROR_HASH_ALGORITHM
        || storage.chunk_size_bytes == 0
        || storage.chunks.is_empty()
        || storage.content_size_bytes != manifest.source_size_bytes
        || manifest.verification.content_blake3.as_deref() != Some(storage.content_blake3.as_str())
        || raw_mirror_blob_relative_path(&storage.content_blake3).is_none()
    {
        return Err(anyhow!(
            "raw mirror fixed-chunk storage metadata is invalid"
        ));
    }
    let descriptor_bytes = serde_json::to_vec(storage)?;
    if descriptor_bytes.len() as u64 != manifest.blob_size_bytes
        || blake3::hash(&descriptor_bytes).to_hex().as_str() != manifest.blob_blake3.as_str()
    {
        return Err(anyhow!(
            "raw mirror fixed-chunk descriptor does not match the manifest blob"
        ));
    }

    let mut content_size_bytes = 0_u64;
    for (index, chunk) in storage.chunks.iter().enumerate() {
        let expected_chunk_path = raw_mirror_blob_relative_path(&chunk.blob_blake3)
            .ok_or_else(|| anyhow!("raw mirror chunk has an invalid digest"))?;
        let is_last = index + 1 == storage.chunks.len();
        if chunk.blob_relative_path != expected_chunk_path
            || chunk.blob_size_bytes == 0
            || chunk.blob_size_bytes > storage.chunk_size_bytes
            || (!is_last && chunk.blob_size_bytes != storage.chunk_size_bytes)
        {
            return Err(anyhow!("raw mirror fixed-chunk entry is invalid"));
        }
        content_size_bytes = content_size_bytes
            .checked_add(chunk.blob_size_bytes)
            .ok_or_else(|| anyhow!("raw mirror fixed-chunk sizes overflow the source size"))?;
    }
    if content_size_bytes != storage.content_size_bytes {
        return Err(anyhow!(
            "raw mirror fixed-chunk sizes do not reconstruct the source size"
        ));
    }

    let mut references = Vec::with_capacity(storage.chunks.len() + 1);
    references.push(descriptor);
    references.extend(storage.chunks.iter().cloned());
    Ok(references)
}

pub fn read_source_bytes(data_dir: &Path, manifest_id: &str) -> Result<Vec<u8>> {
    let root = raw_mirror_root(data_dir);
    let manifest = read_validated_raw_mirror_manifest(&root, manifest_id)?;
    let references = raw_mirror_manifest_blob_references(&manifest)?;

    let Some(storage) = manifest.content_storage.as_ref() else {
        validate_existing_blob_metadata(&root, &references[0])?;
        let bytes = fs::read(root.join(&manifest.blob_relative_path)).with_context(|| {
            format!(
                "read raw mirror source blob for manifest {}",
                manifest.manifest_id
            )
        })?;
        if bytes.len() as u64 != manifest.source_size_bytes
            || blake3::hash(&bytes).to_hex().as_str() != manifest.blob_blake3.as_str()
        {
            return Err(anyhow!(
                "raw mirror source blob has the wrong size or digest for manifest {}",
                manifest.manifest_id
            ));
        }
        return Ok(bytes);
    };

    validate_existing_blob_metadata(&root, &references[0])?;
    let descriptor_bytes =
        fs::read(root.join(&manifest.blob_relative_path)).with_context(|| {
            format!(
                "read raw mirror chunk descriptor for manifest {}",
                manifest.manifest_id
            )
        })?;
    if blake3::hash(&descriptor_bytes).to_hex().as_str() != manifest.blob_blake3.as_str() {
        return Err(anyhow!(
            "raw mirror chunk descriptor failed content verification for manifest {}",
            manifest.manifest_id
        ));
    }
    let descriptor: RawMirrorContentStorage = serde_json::from_slice(&descriptor_bytes)
        .with_context(|| {
            format!(
                "parse raw mirror chunk descriptor for manifest {}",
                manifest.manifest_id
            )
        })?;
    if &descriptor != storage {
        return Err(anyhow!(
            "raw mirror chunk descriptor disagrees with manifest {}",
            manifest.manifest_id
        ));
    }

    let content_capacity = usize::try_from(storage.content_size_bytes)
        .map_err(|_| anyhow!("raw mirror source is too large to reconstruct in memory"))?;
    let mut content = Vec::with_capacity(content_capacity);
    for chunk in &storage.chunks {
        let chunk_path = root.join(&chunk.blob_relative_path);
        validate_existing_blob_metadata(&root, chunk)?;
        let bytes = fs::read(&chunk_path)
            .with_context(|| format!("read raw mirror chunk {}", chunk_path.display()))?;
        if bytes.len() as u64 != chunk.blob_size_bytes
            || blake3::hash(&bytes).to_hex().as_str() != chunk.blob_blake3.as_str()
        {
            return Err(anyhow!(
                "raw mirror chunk {} has the wrong size or digest",
                chunk_path.display()
            ));
        }
        content.extend_from_slice(&bytes);
    }
    if content.len() as u64 != storage.content_size_bytes
        || blake3::hash(&content).to_hex().as_str() != storage.content_blake3.as_str()
    {
        return Err(anyhow!(
            "raw mirror chunks do not reconstruct the recorded source content"
        ));
    }
    Ok(content)
}

pub fn verify_source_capture(
    data_dir: &Path,
    manifest_id: &str,
) -> Result<RawMirrorVerifiedCapture> {
    verify_source_capture_observed(data_dir, manifest_id, &mut |_| {})
}

/// Verifies a capture like `verify_source_capture`, reporting every bounded
/// read's byte count to `on_bytes`. Each read is a cancellation point and an
/// activity tick, so a long verification stays observable and stoppable.
pub fn verify_source_capture_observed(
    data_dir: &Path,
    manifest_id: &str,
    on_bytes: &mut dyn FnMut(u64),
) -> Result<RawMirrorVerifiedCapture> {
    let root = raw_mirror_root(data_dir);
    let manifest = read_validated_raw_mirror_manifest(&root, manifest_id)?;
    let references = raw_mirror_manifest_blob_references(&manifest)?;
    let descriptor_path = root.join(&references[0].blob_relative_path);
    verify_existing_blob_reference(&root, &references[0])?;

    let Some(storage) = manifest.content_storage.as_ref() else {
        return Ok(RawMirrorVerifiedCapture {
            storage_kind: "whole_blob_v1".to_string(),
            source_content_blake3: manifest.blob_blake3.clone(),
            source_size_bytes: manifest.source_size_bytes,
            stored_blob_count: 1,
            stored_bytes: manifest.blob_size_bytes,
            stored_blobs: vec![(manifest.blob_blake3, manifest.blob_size_bytes)],
            chunk_count: 1,
        });
    };

    let descriptor_bytes = fs::read(&descriptor_path).with_context(|| {
        format!(
            "read raw mirror chunk descriptor for manifest {}",
            manifest.manifest_id
        )
    })?;
    let descriptor: RawMirrorContentStorage = serde_json::from_slice(&descriptor_bytes)
        .with_context(|| {
            format!(
                "parse raw mirror chunk descriptor for manifest {}",
                manifest.manifest_id
            )
        })?;
    if &descriptor != storage {
        return Err(anyhow!(
            "raw mirror chunk descriptor disagrees with manifest {}",
            manifest.manifest_id
        ));
    }

    let mut content_hasher = blake3::Hasher::new();
    let mut content_size_bytes = 0_u64;
    let mut stored_paths = HashSet::new();
    stored_paths.insert(manifest.blob_relative_path.clone());
    let mut stored_bytes = manifest.blob_size_bytes;
    let mut stored_blobs = vec![(manifest.blob_blake3.clone(), manifest.blob_size_bytes)];
    let mut buffer = [0_u8; 64 * 1024];
    for chunk in &storage.chunks {
        let chunk_path = root.join(&chunk.blob_relative_path);
        validate_existing_blob_metadata(&root, chunk)?;
        let mut file = File::open(&chunk_path)
            .with_context(|| format!("open raw mirror chunk {}", chunk_path.display()))?;
        let mut chunk_hasher = blake3::Hasher::new();
        let mut observed_chunk_bytes = 0_u64;
        loop {
            raw_mirror_checkpoint()?;
            let read = file
                .read(&mut buffer)
                .with_context(|| format!("read raw mirror chunk {}", chunk_path.display()))?;
            if read == 0 {
                break;
            }
            chunk_hasher.update(&buffer[..read]);
            content_hasher.update(&buffer[..read]);
            observed_chunk_bytes = observed_chunk_bytes.saturating_add(read as u64);
            on_bytes(read as u64);
            raw_mirror_record_progress();
        }
        let observed_chunk_blake3 = chunk_hasher.finalize().to_hex().to_string();
        if observed_chunk_bytes != chunk.blob_size_bytes
            || observed_chunk_blake3 != chunk.blob_blake3
        {
            return Err(anyhow!(
                "raw mirror chunk {} failed content verification",
                chunk_path.display()
            ));
        }
        content_size_bytes = content_size_bytes.saturating_add(observed_chunk_bytes);
        if stored_paths.insert(chunk.blob_relative_path.clone()) {
            stored_bytes = stored_bytes.saturating_add(observed_chunk_bytes);
            stored_blobs.push((chunk.blob_blake3.clone(), observed_chunk_bytes));
        }
    }
    let observed_content_blake3 = content_hasher.finalize().to_hex().to_string();
    if content_size_bytes != storage.content_size_bytes
        || observed_content_blake3 != storage.content_blake3
    {
        return Err(anyhow!(
            "raw mirror chunks do not reconstruct the recorded source content"
        ));
    }
    Ok(RawMirrorVerifiedCapture {
        storage_kind: storage.kind.clone(),
        source_content_blake3: storage.content_blake3.clone(),
        source_size_bytes: storage.content_size_bytes,
        stored_blob_count: stored_paths.len(),
        stored_bytes,
        stored_blobs,
        chunk_count: storage.chunks.len(),
    })
}

pub(in crate::raw_mirror) fn read_validated_raw_mirror_manifest(
    root: &Path,
    manifest_id: &str,
) -> Result<RawMirrorManifestFile> {
    let manifest_relative_path = raw_mirror_manifest_relative_path(manifest_id);
    let manifest_path = raw_mirror_manifest_path_from_relative(root, &manifest_relative_path)?;
    if raw_mirror_path_has_symlink_below_root(root, &manifest_path) {
        return Err(anyhow!("raw mirror manifest path contains a symlink"));
    }
    let manifest = read_raw_mirror_manifest(&manifest_path)?;
    validate_raw_mirror_manifest_contents(&manifest, manifest_id)?;
    Ok(manifest)
}

pub(in crate::raw_mirror) fn validate_raw_mirror_manifest_contents(
    manifest: &RawMirrorManifestFile,
    expected_manifest_id: &str,
) -> Result<Vec<RawMirrorChunkRef>> {
    let expected_manifest_checksum = raw_mirror_manifest_blake3(manifest);
    let expected_original_path_blake3 = raw_mirror_original_path_blake3(&manifest.original_path);
    let derived_manifest_id = raw_mirror_manifest_id(
        &manifest.provider,
        &manifest.source_id,
        &manifest.origin_kind,
        manifest.origin_host.as_deref(),
        &manifest.original_path_blake3,
        &manifest.blob_blake3,
    );
    if manifest.manifest_id != expected_manifest_id
        || manifest.manifest_id != derived_manifest_id
        || manifest.original_path_blake3 != expected_original_path_blake3
        || manifest.manifest_kind != RAW_MIRROR_MANIFEST_KIND
        || manifest.schema_version != RAW_MIRROR_SCHEMA_VERSION
        || manifest.blob_hash_algorithm != RAW_MIRROR_HASH_ALGORITHM
        || manifest.manifest_blake3.as_deref() != Some(expected_manifest_checksum.as_str())
    {
        return Err(anyhow!(
            "raw mirror manifest identity or checksum is invalid"
        ));
    }
    raw_mirror_manifest_blob_references(manifest)
        .context("raw mirror manifest storage metadata is invalid")
}

pub(in crate::raw_mirror) fn verify_existing_blob_reference(
    root: &Path,
    reference: &RawMirrorChunkRef,
) -> Result<()> {
    validate_existing_blob_metadata(root, reference)?;
    verify_existing_file(
        &root.join(&reference.blob_relative_path),
        &reference.blob_blake3,
    )
}

fn validate_existing_blob_metadata(root: &Path, reference: &RawMirrorChunkRef) -> Result<()> {
    let expected_relative_path = raw_mirror_blob_relative_path(&reference.blob_blake3)
        .ok_or_else(|| anyhow!("raw mirror blob reference has an invalid digest"))?;
    if reference.blob_relative_path != expected_relative_path {
        return Err(anyhow!(
            "raw mirror blob reference path does not match its digest"
        ));
    }
    let path = root.join(&reference.blob_relative_path);
    if raw_mirror_path_has_symlink_below_root(root, &path) {
        return Err(anyhow!(
            "raw mirror blob path {} contains a symlink",
            path.display()
        ));
    }
    let metadata = raw_mirror_symlink_metadata(&path)
        .with_context(|| format!("stat raw mirror blob {}", path.display()))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() != reference.blob_size_bytes
    {
        return Err(anyhow!(
            "raw mirror blob {} is missing, unsafe, or has the wrong size",
            path.display()
        ));
    }
    Ok(())
}

pub(in crate::raw_mirror) fn raw_mirror_manifest_relative_path(manifest_id: &str) -> String {
    format!("manifests/{manifest_id}.json")
}

pub(in crate::raw_mirror) fn raw_mirror_original_path_blake3(original_path: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"doctor-raw-mirror-original-path-v1");
    hasher.update(&[0]);
    hasher.update(original_path.as_bytes());
    hasher.finalize().to_hex().to_string()
}

pub(in crate::raw_mirror) fn raw_mirror_manifest_id(
    provider: &str,
    source_id: &str,
    origin_kind: &str,
    origin_host: Option<&str>,
    original_path_blake3: &str,
    blob_blake3: &str,
) -> String {
    canonical_blake3(
        "doctor-raw-mirror-manifest-id-v1",
        json!({
            "provider": provider,
            "source_id": source_id,
            "origin_kind": origin_kind,
            "origin_host": origin_host,
            "original_path_blake3": original_path_blake3,
            "blob_blake3": blob_blake3,
        }),
    )
}

pub(in crate::raw_mirror) fn raw_mirror_manifest_blake3(
    manifest: &RawMirrorManifestFile,
) -> String {
    let mut value = serde_json::to_value(manifest).unwrap_or_default();
    if let Value::Object(map) = &mut value {
        map.remove("manifest_blake3");
    }
    canonical_blake3("doctor-raw-mirror-manifest-v1", value)
}

fn canonical_blake3(prefix: &str, value: Value) -> String {
    let encoded = serde_json::to_vec(&canonical_json_value(value)).unwrap_or_default();
    let mut hasher = blake3::Hasher::new();
    hasher.update(prefix.as_bytes());
    hasher.update(&[0]);
    hasher.update(&encoded);
    format!("{prefix}-{}", hasher.finalize().to_hex())
}

fn canonical_json_value(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.into_iter().map(canonical_json_value).collect()),
        Value::Object(map) => {
            let mut entries: Vec<_> = map.into_iter().collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let mut canonical = serde_json::Map::new();
            for (key, value) in entries {
                canonical.insert(key, canonical_json_value(value));
            }
            Value::Object(canonical)
        }
        other => other,
    }
}

pub(in crate::raw_mirror) fn unique_db_links(links: &[RawMirrorDbLink]) -> Vec<RawMirrorDbLink> {
    let mut dedup = links.to_vec();
    dedup.sort_by(|left, right| {
        (
            left.conversation_id,
            left.message_count,
            left.started_at_ms,
            left.source_path.as_deref().unwrap_or(""),
        )
            .cmp(&(
                right.conversation_id,
                right.message_count,
                right.started_at_ms,
                right.source_path.as_deref().unwrap_or(""),
            ))
    });
    dedup.dedup();
    dedup
}
