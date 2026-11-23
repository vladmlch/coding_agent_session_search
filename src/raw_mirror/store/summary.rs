use super::io::{
    RAW_MIRROR_MANIFEST_KIND, raw_mirror_path_has_symlink_below_root, raw_mirror_root,
};
use super::manifest::{
    raw_mirror_manifest_relative_path, read_raw_mirror_manifest,
    validate_raw_mirror_manifest_contents,
};
use super::prune::collect_raw_mirror_physical_blobs;
use super::types::RawMirrorStorageSummary;
use std::collections::HashSet;
use std::fs;
use std::path::Path;

pub fn storage_summary(data_dir: &Path) -> RawMirrorStorageSummary {
    let root = raw_mirror_root(data_dir);
    let mut summary = RawMirrorStorageSummary {
        root_path: root.display().to_string(),
        ..RawMirrorStorageSummary::default()
    };
    let root_metadata = match fs::symlink_metadata(&root) {
        Ok(metadata) => metadata,
        Err(_) => return summary,
    };
    summary.initialized = true;
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        summary.invalid_manifest_count = 1;
        return summary;
    }

    summary.total_storage_bytes = raw_mirror_dir_file_bytes(&root);

    let manifests_dir = root.join("manifests");
    let Ok(manifests_metadata) = fs::symlink_metadata(&manifests_dir) else {
        populate_raw_mirror_orphan_summary(&root, &HashSet::new(), &mut summary);
        return summary;
    };
    if manifests_metadata.file_type().is_symlink() || !manifests_metadata.is_dir() {
        summary.invalid_manifest_count = summary.invalid_manifest_count.saturating_add(1);
        return summary;
    }
    let entries = match fs::read_dir(&manifests_dir) {
        Ok(entries) => entries,
        Err(_) => return summary,
    };
    let mut seen_blobs = HashSet::new();
    for entry in entries {
        let Ok(entry) = entry else {
            summary.invalid_manifest_count = summary.invalid_manifest_count.saturating_add(1);
            continue;
        };
        let path = entry.path();
        let manifest_metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => metadata,
            _ => {
                summary.invalid_manifest_count = summary.invalid_manifest_count.saturating_add(1);
                continue;
            }
        };
        summary.manifest_bytes = summary
            .manifest_bytes
            .saturating_add(manifest_metadata.len());
        let manifest = match read_raw_mirror_manifest(&path) {
            Ok(manifest) if manifest.manifest_kind == RAW_MIRROR_MANIFEST_KIND => manifest,
            _ => {
                summary.invalid_manifest_count = summary.invalid_manifest_count.saturating_add(1);
                continue;
            }
        };
        summary.manifest_count = summary.manifest_count.saturating_add(1);
        let expected_path = root.join(raw_mirror_manifest_relative_path(&manifest.manifest_id));
        let blob_references = if path == expected_path {
            validate_raw_mirror_manifest_contents(&manifest, &manifest.manifest_id).ok()
        } else {
            None
        };
        let Some(blob_references) = blob_references else {
            summary.invalid_manifest_count = summary.invalid_manifest_count.saturating_add(1);
            continue;
        };
        merge_min_max(
            &mut summary.oldest_capture_at_ms,
            &mut summary.newest_capture_at_ms,
            Some(manifest.captured_at_ms),
        );
        merge_min_max(
            &mut summary.oldest_source_mtime_ms,
            &mut summary.newest_source_mtime_ms,
            manifest.source_mtime_ms,
        );

        for blob_reference in blob_references {
            if !seen_blobs.insert(blob_reference.blob_relative_path.clone()) {
                continue;
            }
            let blob_path = root.join(&blob_reference.blob_relative_path);
            if raw_mirror_path_has_symlink_below_root(&root, &blob_path) {
                summary.missing_blob_count = summary.missing_blob_count.saturating_add(1);
                continue;
            }
            match fs::symlink_metadata(&blob_path) {
                Ok(metadata)
                    if metadata.is_file()
                        && !metadata.file_type().is_symlink()
                        && metadata.len() == blob_reference.blob_size_bytes =>
                {
                    let size = metadata.len();
                    summary.unique_blob_count = summary.unique_blob_count.saturating_add(1);
                    summary.total_blob_bytes = summary.total_blob_bytes.saturating_add(size);
                    summary.largest_blob_bytes = summary.largest_blob_bytes.max(size);
                }
                _ => {
                    summary.missing_blob_count = summary.missing_blob_count.saturating_add(1);
                }
            }
        }
    }

    if summary.invalid_manifest_count == 0 {
        populate_raw_mirror_orphan_summary(&root, &seen_blobs, &mut summary);
    }

    summary
}

fn populate_raw_mirror_orphan_summary(
    root: &Path,
    referenced_blob_paths: &HashSet<String>,
    summary: &mut RawMirrorStorageSummary,
) {
    let Ok(physical_blobs) = collect_raw_mirror_physical_blobs(root) else {
        return;
    };
    for blob in physical_blobs {
        if !referenced_blob_paths.contains(&blob.relative_path) {
            summary.orphan_blob_count = summary.orphan_blob_count.saturating_add(1);
            summary.orphan_blob_bytes = summary.orphan_blob_bytes.saturating_add(blob.size_bytes);
        }
    }
}

pub fn physical_storage_bytes(data_dir: &Path) -> u64 {
    raw_mirror_dir_file_bytes(&raw_mirror_root(data_dir))
}

fn merge_min_max(min: &mut Option<i64>, max: &mut Option<i64>, value: Option<i64>) {
    let Some(value) = value else {
        return;
    };
    *min = Some(min.map_or(value, |current| current.min(value)));
    *max = Some(max.map_or(value, |current| current.max(value)));
}

fn raw_mirror_dir_file_bytes(root: &Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(metadata) = fs::symlink_metadata(&path) else {
            continue;
        };
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        } else if metadata.is_dir() {
            let Ok(entries) = fs::read_dir(&path) else {
                continue;
            };
            for entry in entries.flatten() {
                stack.push(entry.path());
            }
        }
    }
    total
}
