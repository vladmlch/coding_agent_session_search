use super::io::{
    RAW_MIRROR_BLOB_EXTENSION, RAW_MIRROR_HASH_ALGORITHM, acquire_raw_mirror_mutation_lock,
    configure_lock_open_options, ensure_private_dir, now_ms, raw_mirror_io,
    raw_mirror_path_has_symlink_below_root, raw_mirror_root, raw_mirror_symlink_metadata,
    raw_mirror_tracked_io, set_private_open_file_permissions, sync_parent_unconditionally,
    system_time_to_ms, try_acquire_index_run_lock_for_prune,
};
use super::manifest::{
    raw_mirror_blob_relative_path, raw_mirror_manifest_blob_references,
    raw_mirror_manifest_relative_path, read_raw_mirror_manifest,
    read_validated_raw_mirror_manifest, same_source_identity,
    validate_raw_mirror_manifest_contents, validated_existing_manifest,
    verify_existing_blob_reference,
};
use super::policy;
use super::types::{
    PRUNE_PREVIEW_ENTRY_LIMIT, RawMirrorChunkRef, RawMirrorPhysicalBlob, RawMirrorPruneEntry,
    RawMirrorPruneManifest, RawMirrorPruneOptions, RawMirrorPruneReport,
};
use anyhow::{Context, Result, anyhow};
use serde_json::json;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub fn prune(data_dir: &Path, options: RawMirrorPruneOptions) -> Result<RawMirrorPruneReport> {
    let providers = options
        .providers
        .iter()
        .map(|provider| provider.trim().to_string())
        .collect::<HashSet<_>>();
    if providers.contains("") {
        anyhow::bail!("raw-mirror prune provider selector must not be empty");
    }
    let source_pattern = options
        .source_path
        .as_deref()
        .map(|pattern| {
            if pattern.trim().is_empty() {
                anyhow::bail!("raw-mirror prune source-path selector must not be empty");
            }
            glob::Pattern::new(pattern).context("invalid raw-mirror prune source-path glob")
        })
        .transpose()?;
    if options.keep_latest == Some(0) {
        anyhow::bail!("raw-mirror prune keep-latest must keep at least one capture per source");
    }
    let scoped = !providers.is_empty() || source_pattern.is_some();
    let matches_scope = |manifest: &RawMirrorPruneManifest| {
        (providers.is_empty() || providers.contains(&manifest.provider))
            && source_pattern
                .as_ref()
                .is_none_or(|pattern| pattern.matches(&manifest.original_path))
    };
    let mut reported_providers = providers.iter().cloned().collect::<Vec<_>>();
    reported_providers.sort();
    let root = raw_mirror_root(data_dir);
    let mut report = RawMirrorPruneReport {
        initialized: false,
        root_path: root.display().to_string(),
        mode: if options.apply {
            "apply".to_string()
        } else {
            "dry-run".to_string()
        },
        manifest_count: 0,
        unique_blob_count: 0,
        current_blob_bytes: 0,
        providers: reported_providers,
        source_path: options.source_path.clone(),
        scope_blob_bytes: scoped.then_some(0),
        orphan_blob_count: 0,
        orphan_blob_bytes: 0,
        safety_hold_down_ms: options.safety_hold_down_ms,
        keep_tags: options.keep_tags.clone(),
        keep_latest: options.keep_latest,
        pinned_manifest_count: 0,
        pinned_blob_count: 0,
        planned_manifest_count: 0,
        planned_blob_count: 0,
        planned_reclaim_bytes: 0,
        planned_staging_count: 0,
        applied_manifest_count: 0,
        applied_blob_count: 0,
        applied_staging_count: 0,
        applied_reclaim_bytes: 0,
        audit_log_path: None,
        omitted_entry_count: 0,
        entries: Vec::new(),
    };

    let metadata = match fs::symlink_metadata(&root) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(report),
        Err(err) => {
            return Err(err).with_context(|| format!("stat raw mirror root {}", root.display()));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        anyhow::bail!(
            "refusing to prune invalid raw mirror root {}",
            root.display()
        );
    }
    report.initialized = true;
    // Both modes need one stable manifest/blob view. Applied prunes also
    // serialize their durable audit writes under this lock.
    let _mutation_lock = acquire_raw_mirror_mutation_lock(&root)?;
    let _index_run_lock = options
        .apply
        .then(|| try_acquire_index_run_lock_for_prune(data_dir))
        .transpose()?;

    let manifests = collect_prune_manifests(&root)?;
    report.manifest_count = manifests.len() as u64;
    let physical_blobs = collect_raw_mirror_physical_blobs(&root)?;
    let physical_blob_by_relative = physical_blobs
        .iter()
        .map(|blob| (blob.relative_path.clone(), blob))
        .collect::<BTreeMap<_, _>>();

    let mut blob_to_manifests: HashMap<String, Vec<String>> = HashMap::new();
    let mut manifest_by_id: HashMap<String, &RawMirrorPruneManifest> = HashMap::new();
    let mut blob_size_by_relative: HashMap<String, u64> = HashMap::new();
    for manifest in &manifests {
        manifest_by_id.insert(manifest.manifest_id.clone(), manifest);
        let mut seen_manifest_blobs = HashSet::new();
        for blob_reference in &manifest.blob_references {
            if !seen_manifest_blobs.insert(blob_reference.blob_relative_path.clone()) {
                continue;
            }
            blob_to_manifests
                .entry(blob_reference.blob_relative_path.clone())
                .or_default()
                .push(manifest.manifest_id.clone());
            let physical = physical_blob_by_relative
                .get(&blob_reference.blob_relative_path)
                .ok_or_else(|| {
                    anyhow!(
                        "refusing to prune because manifest {} references missing blob {}",
                        manifest.manifest_id,
                        blob_reference.blob_relative_path
                    )
                })?;
            if physical.size_bytes != blob_reference.blob_size_bytes
                || physical.blob_blake3 != blob_reference.blob_blake3
            {
                return Err(anyhow!(
                    "refusing to prune because manifest {} disagrees with physical blob {}",
                    manifest.manifest_id,
                    blob_reference.blob_relative_path
                ));
            }
            blob_size_by_relative
                .entry(blob_reference.blob_relative_path.clone())
                .or_insert(physical.size_bytes);
        }
    }
    let orphan_blobs = physical_blobs
        .iter()
        .filter(|blob| !blob_to_manifests.contains_key(&blob.relative_path))
        .collect::<Vec<_>>();
    report.unique_blob_count = physical_blobs.len() as u64;
    report.current_blob_bytes = physical_blobs
        .iter()
        .map(|blob| blob.size_bytes)
        .fold(0u64, u64::saturating_add);
    report.orphan_blob_count = orphan_blobs.len() as u64;
    report.orphan_blob_bytes = orphan_blobs
        .iter()
        .map(|blob| blob.size_bytes)
        .fold(0u64, u64::saturating_add);

    let now = now_ms();
    let mut pinned_manifests = pinned_prune_manifest_ids(
        data_dir,
        &manifests,
        &options.keep_tags,
        options.safety_hold_down_ms,
        now,
    )?;
    // Scope is an additional preservation boundary, including for blobs
    // shared with a capture outside the requested provider/path selection.
    pinned_manifests.extend(
        manifests
            .iter()
            .filter(|manifest| !matches_scope(manifest))
            .map(|manifest| manifest.manifest_id.clone()),
    );
    // The newest N captures of each source file are kept whatever the age
    // and size rules select, so a timer-driven prune can never retire the
    // only copy of a session whose provider has since deleted it (#522). A
    // source file is the same exact identity automatic retention uses:
    // provider, source, origin kind and host, and original path. `manifests`
    // is sorted oldest first, so walking it backwards meets each source's
    // newest captures first.
    let mut beyond_latest = Vec::new();
    if let Some(keep_latest) = options.keep_latest {
        type SourceIdentity<'a> = (&'a str, &'a str, &'a str, Option<&'a str>, &'a str);
        let mut kept_per_source: HashMap<SourceIdentity<'_>, usize> = HashMap::new();
        for manifest in manifests.iter().rev() {
            let kept = kept_per_source
                .entry((
                    manifest.provider.as_str(),
                    manifest.source_id.as_str(),
                    manifest.origin_kind.as_str(),
                    manifest.origin_host.as_deref(),
                    manifest.original_path.as_str(),
                ))
                .or_default();
            if *kept < keep_latest {
                *kept += 1;
                pinned_manifests.insert(manifest.manifest_id.clone());
            } else {
                beyond_latest.push(manifest.manifest_id.clone());
            }
        }
    }
    let budget_bytes = if scoped {
        let scope_blobs = manifests
            .iter()
            .filter(|manifest| matches_scope(manifest))
            .flat_map(|manifest| &manifest.blob_references)
            .map(|reference| &reference.blob_relative_path)
            .collect::<HashSet<_>>();
        let bytes = scope_blobs
            .iter()
            .filter_map(|path| blob_size_by_relative.get(*path))
            .copied()
            .fold(0u64, u64::saturating_add);
        report.scope_blob_bytes = Some(bytes);
        bytes
    } else {
        report.current_blob_bytes
    };
    report.pinned_manifest_count = pinned_manifests.len() as u64;
    let mut pinned_blobs: HashSet<String> = blob_to_manifests
        .iter()
        .filter(|(_, manifest_ids)| manifest_ids.iter().any(|id| pinned_manifests.contains(id)))
        .map(|(blob_relative_path, _)| blob_relative_path.clone())
        .collect();
    if scoped {
        // Orphans have no trustworthy provider/path provenance. A scoped
        // request cannot authorize reclaiming them.
        pinned_blobs.extend(orphan_blobs.iter().map(|blob| blob.relative_path.clone()));
    }
    if options.safety_hold_down_ms > 0 {
        let hold_down_cutoff = now.saturating_sub(options.safety_hold_down_ms);
        pinned_blobs.extend(
            orphan_blobs
                .iter()
                .filter(|blob| {
                    blob.modified_at_ms
                        .is_none_or(|modified_at_ms| modified_at_ms > hold_down_cutoff)
                })
                .map(|blob| blob.relative_path.clone()),
        );
    }
    report.pinned_blob_count = pinned_blobs.len() as u64;

    let mut selected_manifests: HashSet<String> = HashSet::new();
    let mut manifest_reasons: HashMap<String, String> = HashMap::new();
    let mut selected_orphan_blobs: HashSet<String> = HashSet::new();
    let mut orphan_blob_reasons: HashMap<String, String> = HashMap::new();

    if let Some(older_than_ms) = options.older_than_ms {
        let cutoff_ms = now.saturating_sub(older_than_ms.max(0));
        for manifest in &manifests {
            if manifest.captured_at_ms <= cutoff_ms
                && !pinned_manifests.contains(&manifest.manifest_id)
            {
                selected_manifests.insert(manifest.manifest_id.clone());
                manifest_reasons
                    .entry(manifest.manifest_id.clone())
                    .or_insert_with(|| format!("captured_at_ms <= {cutoff_ms}"));
            }
        }
        for blob in &orphan_blobs {
            if !pinned_blobs.contains(&blob.relative_path)
                && blob
                    .modified_at_ms
                    .is_some_and(|modified_at_ms| modified_at_ms <= cutoff_ms)
            {
                selected_orphan_blobs.insert(blob.relative_path.clone());
                orphan_blob_reasons.insert(
                    blob.relative_path.clone(),
                    format!("unreferenced blob modified_at_ms <= {cutoff_ms}"),
                );
            }
        }
    }

    if let Some(keep_latest) = options.keep_latest {
        for manifest_id in beyond_latest {
            if pinned_manifests.contains(&manifest_id) {
                continue;
            }
            manifest_reasons
                .entry(manifest_id.clone())
                .or_insert_with(|| {
                    format!("superseded by the newest {keep_latest} captures of its source file")
                });
            selected_manifests.insert(manifest_id);
        }
    }

    if let Some(max_size_bytes) = options.max_size_bytes
        && budget_bytes > max_size_bytes
    {
        let mut projected_bytes = budget_bytes;
        for (blob_relative_path, manifest_ids) in &blob_to_manifests {
            if manifest_ids
                .iter()
                .all(|id| selected_manifests.contains(id))
            {
                projected_bytes = projected_bytes.saturating_sub(
                    blob_size_by_relative
                        .get(blob_relative_path)
                        .copied()
                        .unwrap_or(0),
                );
            }
        }
        for blob in &orphan_blobs {
            if selected_orphan_blobs.contains(&blob.relative_path) {
                projected_bytes = projected_bytes.saturating_sub(blob.size_bytes);
            }
        }

        for blob in &orphan_blobs {
            if projected_bytes <= max_size_bytes {
                break;
            }
            if pinned_blobs.contains(&blob.relative_path)
                || selected_orphan_blobs.contains(&blob.relative_path)
            {
                continue;
            }
            selected_orphan_blobs.insert(blob.relative_path.clone());
            orphan_blob_reasons
                .entry(blob.relative_path.clone())
                .or_insert_with(|| {
                    "max-size over budget; reclaiming oldest unreferenced blob".to_string()
                });
            projected_bytes = projected_bytes.saturating_sub(blob.size_bytes);
        }

        for manifest in &manifests {
            if projected_bytes <= max_size_bytes {
                break;
            }
            if pinned_manifests.contains(&manifest.manifest_id)
                || selected_manifests.contains(&manifest.manifest_id)
            {
                continue;
            }
            selected_manifests.insert(manifest.manifest_id.clone());
            manifest_reasons
                .entry(manifest.manifest_id.clone())
                .or_insert_with(|| {
                    "max-size over budget; retiring oldest unpinned capture".to_string()
                });

            let mut seen_manifest_blobs = HashSet::new();
            for blob_reference in &manifest.blob_references {
                if !seen_manifest_blobs.insert(blob_reference.blob_relative_path.clone()) {
                    continue;
                }
                let becomes_unreferenced = blob_to_manifests
                    .get(&blob_reference.blob_relative_path)
                    .is_some_and(|manifest_ids| {
                        manifest_ids
                            .iter()
                            .all(|id| selected_manifests.contains(id))
                    });
                if becomes_unreferenced {
                    projected_bytes = projected_bytes.saturating_sub(
                        blob_size_by_relative
                            .get(&blob_reference.blob_relative_path)
                            .copied()
                            .unwrap_or(0),
                    );
                }
            }
        }
    }

    let mut selected_blobs: HashSet<String> = blob_to_manifests
        .iter()
        .filter(|(_, manifest_ids)| {
            manifest_ids
                .iter()
                .all(|id| selected_manifests.contains(id))
        })
        .map(|(blob_relative_path, _)| blob_relative_path.clone())
        .collect();
    selected_blobs.extend(selected_orphan_blobs);

    let mut entries = Vec::new();
    let mut selected_manifest_ids = selected_manifests.into_iter().collect::<Vec<_>>();
    selected_manifest_ids.sort();
    for manifest_id in &selected_manifest_ids {
        let Some(manifest) = manifest_by_id.get(manifest_id) else {
            continue;
        };
        report.planned_manifest_count += 1;
        report.planned_reclaim_bytes = report
            .planned_reclaim_bytes
            .saturating_add(manifest.size_bytes);
        if !options.apply && entries.len() == PRUNE_PREVIEW_ENTRY_LIMIT {
            report.omitted_entry_count += 1;
            continue;
        }
        let reason = manifest_reasons
            .remove(manifest_id)
            .unwrap_or_else(|| "selected by retention policy".to_string());
        entries.push(RawMirrorPruneEntry {
            kind: "manifest".to_string(),
            path: manifest.relative_path.clone(),
            blob_blake3: Some(manifest.blob_blake3.clone()),
            manifest_blake3: Some(manifest.manifest_blake3.clone()),
            size_bytes: manifest.size_bytes,
            reason,
            applied: false,
        });
    }

    let mut selected_blob_paths = selected_blobs.into_iter().collect::<Vec<_>>();
    selected_blob_paths.sort();
    for blob_relative_path in selected_blob_paths {
        let size = physical_blob_by_relative
            .get(&blob_relative_path)
            .map(|blob| blob.size_bytes)
            .unwrap_or(0);
        report.planned_blob_count += 1;
        report.planned_reclaim_bytes = report.planned_reclaim_bytes.saturating_add(size);
        if !options.apply && entries.len() == PRUNE_PREVIEW_ENTRY_LIMIT {
            report.omitted_entry_count += 1;
            continue;
        }
        let blob_blake3 = blob_relative_path
            .rsplit('/')
            .next()
            .and_then(|name| name.strip_suffix(".raw"))
            .map(ToOwned::to_owned);
        let reason = orphan_blob_reasons
            .remove(&blob_relative_path)
            .unwrap_or_else(|| {
                "no retained manifest references this blob after prune plan".to_string()
            });
        entries.push(RawMirrorPruneEntry {
            kind: "blob".to_string(),
            path: blob_relative_path,
            blob_blake3,
            manifest_blake3: None,
            size_bytes: size,
            reason,
            applied: false,
        });
    }

    // Every capture creates its staging dir after taking the mutation lock this
    // prune holds, and keeps the lock until it publishes or fails, so a staging
    // dir seen here belongs to a capture that was killed (GH #523). A staging
    // dir cannot be attributed to a provider or source path, so only an
    // unscoped prune reclaims it.
    if !scoped {
        let staging_cutoff_ms = now.saturating_sub(ABANDONED_STAGING_MIN_AGE_MS);
        for staging in collect_capture_staging_dirs(&root)? {
            if staging.contains_symlink || staging.newest_modified_ms > staging_cutoff_ms {
                continue;
            }
            report.planned_staging_count += 1;
            report.planned_reclaim_bytes = report
                .planned_reclaim_bytes
                .saturating_add(staging.size_bytes);
            if !options.apply && entries.len() == PRUNE_PREVIEW_ENTRY_LIMIT {
                report.omitted_entry_count += 1;
                continue;
            }
            entries.push(RawMirrorPruneEntry {
                kind: "staging".to_string(),
                path: staging.relative_path,
                blob_blake3: None,
                manifest_blake3: None,
                size_bytes: staging.size_bytes,
                reason: "capture staging abandoned by a killed capture (no capture holds the raw-mirror lock; untouched for 1 h)".to_string(),
                applied: false,
            });
        }
    }

    report.entries = entries;
    if options.apply {
        // A destructive apply is authority-bearing. Re-read every selected
        // manifest and hash every blob it depends on before removing even the
        // first file, including shared chunks that the plan retains. This
        // prevents a prune from erasing the last useful pointer after latent
        // content corruption or manifest drift.
        let mut verified_blob_paths = HashSet::new();
        for manifest_id in &selected_manifest_ids {
            let manifest = read_validated_raw_mirror_manifest(&root, manifest_id)
                .with_context(|| format!("preflight selected raw mirror manifest {manifest_id}"))?;
            for reference in raw_mirror_manifest_blob_references(&manifest)? {
                if verified_blob_paths.insert(reference.blob_relative_path.clone()) {
                    verify_existing_blob_reference(&root, &reference).with_context(|| {
                        format!("preflight content checksum for selected manifest {manifest_id}")
                    })?;
                }
            }
        }
        for entry in report.entries.iter().filter(|entry| entry.kind == "blob") {
            if !verified_blob_paths.insert(entry.path.clone()) {
                continue;
            }
            let physical = physical_blob_by_relative.get(&entry.path).ok_or_else(|| {
                anyhow!(
                    "preflight selected raw mirror blob disappeared: {}",
                    entry.path
                )
            })?;
            verify_existing_blob_reference(
                &root,
                &RawMirrorChunkRef {
                    blob_relative_path: physical.relative_path.clone(),
                    blob_blake3: physical.blob_blake3.clone(),
                    blob_size_bytes: physical.size_bytes,
                },
            )
            .with_context(|| format!("preflight selected raw mirror blob {}", entry.path))?;
        }
        for entry in &report.entries {
            let path = root.join(&entry.path);
            if entry.kind == "staging" {
                validate_prune_target_staging(&root, &path)
            } else {
                validate_prune_target_file(&root, &path)
            }
            .with_context(|| format!("preflight raw mirror prune target {}", path.display()))?;
        }

        // Record and fsync the exact authority-bearing plan before removing
        // the first file. If the process crashes or a later result append
        // fails, the audit still contains a durable intent record for every
        // target that may have been touched.
        if !report.entries.is_empty() {
            policy::note_mutation(&root)?;
            let (audit_path, mut audit_file) = open_prune_audit_log(&root)?;
            report.audit_log_path = Some(audit_path.display().to_string());
            apply_prune_entries(&root, &mut report, &audit_path, &mut audit_file)?;
        }
    }
    // A preview returns its plan to the caller without writing one audit row
    // per candidate. Only applied operations need durable intent/result records;
    // repeated previews must not grow the archive they are meant to inspect.
    Ok(report)
}

pub(in crate::raw_mirror) fn apply_prune_entries(
    root: &Path,
    report: &mut RawMirrorPruneReport,
    audit_path: &Path,
    audit_file: &mut File,
) -> Result<()> {
    policy::forget_measured_usage(root);
    append_prune_audit_records(audit_file, audit_path, &report.entries, "intent")?;
    for entry in &mut report.entries {
        let path = root.join(&entry.path);
        let removed = remove_prune_entry_file(root, entry)
            .with_context(|| format!("applying raw mirror prune for {}", path.display()))?;
        entry.applied = removed;
        if removed {
            if entry.kind == "manifest" {
                report.applied_manifest_count = report.applied_manifest_count.saturating_add(1);
            } else if entry.kind == "blob" {
                report.applied_blob_count = report.applied_blob_count.saturating_add(1);
            } else if entry.kind == "staging" {
                report.applied_staging_count = report.applied_staging_count.saturating_add(1);
            }
            report.applied_reclaim_bytes = report
                .applied_reclaim_bytes
                .saturating_add(entry.size_bytes);
        }
        // Persist the completed prefix before advancing. A later validation or
        // removal error must not erase evidence of earlier successful removals.
        append_prune_audit_records(
            audit_file,
            audit_path,
            std::slice::from_ref(entry),
            "result",
        )?;
    }
    Ok(())
}

pub(in crate::raw_mirror) fn collect_prune_manifests(
    root: &Path,
) -> Result<Vec<RawMirrorPruneManifest>> {
    let manifests_dir = root.join("manifests");
    let metadata = match raw_mirror_symlink_metadata(&manifests_dir) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err).with_context(|| format!("stat {}", manifests_dir.display())),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        anyhow::bail!(
            "refusing to prune invalid raw mirror manifests directory {}",
            manifests_dir.display()
        );
    }

    let mut manifests = Vec::new();
    let mut entries = raw_mirror_io("read_dir", &manifests_dir, || fs::read_dir(&manifests_dir))
        .with_context(|| format!("read {}", manifests_dir.display()))?;
    while let Some(entry) = raw_mirror_io("read_dir_next", &manifests_dir, || {
        entries.next().transpose()
    })? {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let manifest_metadata = raw_mirror_symlink_metadata(&path)
            .with_context(|| format!("stat raw mirror manifest {}", path.display()))?;
        if manifest_metadata.file_type().is_symlink() || !manifest_metadata.is_file() {
            anyhow::bail!(
                "refusing to prune with non-regular raw mirror manifest {}",
                path.display()
            );
        }
        let parsed_manifest = read_raw_mirror_manifest(&path)?;
        let expected_path = root.join(raw_mirror_manifest_relative_path(
            &parsed_manifest.manifest_id,
        ));
        if path != expected_path {
            anyhow::bail!(
                "refusing to prune non-canonical raw mirror manifest path {}; expected {}",
                path.display(),
                expected_path.display()
            );
        }
        let blob_references =
            validate_raw_mirror_manifest_contents(&parsed_manifest, &parsed_manifest.manifest_id)
                .with_context(|| {
                format!(
                    "refusing to prune raw mirror manifest without valid identity and checksum {}",
                    path.display()
                )
            })?;
        let manifest_blake3 = parsed_manifest.manifest_blake3.clone().ok_or_else(|| {
            anyhow!(
                "validated raw mirror manifest {} is missing its descriptor checksum",
                path.display()
            )
        })?;
        // The path was just proven canonical, so report the canonical
        // `manifests/<id>.json` form capture reports use. `strip_prefix`
        // would render `manifests\<id>.json` on Windows (2l1b0.62).
        let relative_path = raw_mirror_manifest_relative_path(&parsed_manifest.manifest_id);
        manifests.push(RawMirrorPruneManifest {
            manifest_id: parsed_manifest.manifest_id,
            manifest_blake3,
            blob_blake3: parsed_manifest.blob_blake3,
            relative_path,
            size_bytes: manifest_metadata.len(),
            blob_references,
            captured_at_ms: parsed_manifest.captured_at_ms,
            provider: parsed_manifest.provider,
            source_id: parsed_manifest.source_id,
            origin_kind: parsed_manifest.origin_kind,
            origin_host: parsed_manifest.origin_host,
            original_path: parsed_manifest.original_path,
            db_links: parsed_manifest.db_links,
        });
    }
    manifests.sort_by(|left, right| {
        left.captured_at_ms
            .cmp(&right.captured_at_ms)
            .then_with(|| left.provider.cmp(&right.provider))
            .then_with(|| left.original_path.cmp(&right.original_path))
            .then_with(|| left.manifest_id.cmp(&right.manifest_id))
    });
    Ok(manifests)
}

pub(in crate::raw_mirror) fn pinned_prune_manifest_ids(
    data_dir: &Path,
    manifests: &[RawMirrorPruneManifest],
    keep_tags: &[String],
    safety_hold_down_ms: i64,
    now_ms: i64,
) -> Result<HashSet<String>> {
    let mut pinned = HashSet::new();
    if safety_hold_down_ms > 0 {
        let cutoff_ms = now_ms.saturating_sub(safety_hold_down_ms);
        for manifest in manifests {
            if manifest.captured_at_ms > cutoff_ms {
                pinned.insert(manifest.manifest_id.clone());
            }
        }
    }

    let normalized_keep_tags = keep_tags
        .iter()
        .map(|tag| tag.trim())
        .filter(|tag| !tag.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if normalized_keep_tags.is_empty() {
        return Ok(pinned);
    }

    let keep_tag_conversation_ids =
        load_keep_tag_conversation_ids(data_dir, manifests, &normalized_keep_tags)?;
    for manifest in manifests {
        if manifest.db_links.iter().any(|link| {
            link.conversation_id
                .is_some_and(|id| keep_tag_conversation_ids.contains(&id))
        }) {
            pinned.insert(manifest.manifest_id.clone());
        }
    }
    Ok(pinned)
}

fn load_keep_tag_conversation_ids(
    data_dir: &Path,
    manifests: &[RawMirrorPruneManifest],
    keep_tags: &[String],
) -> Result<HashSet<i64>> {
    use crate::franken_sync::compat::{ConnectionExt as _, ParamValue, RowExt as _};

    let mut conversation_ids = manifests
        .iter()
        .flat_map(|manifest| manifest.db_links.iter())
        .filter_map(|link| link.conversation_id)
        .collect::<Vec<_>>();
    conversation_ids.sort_unstable();
    conversation_ids.dedup();
    if conversation_ids.is_empty() {
        return Ok(HashSet::new());
    }

    let db_path = data_dir.join("agent_search.db");
    let conn = crate::storage::sqlite::open_franken_raw_readonly_connection_with_timeout(
        &db_path,
        Duration::from_secs(30),
    )
    .with_context(|| {
        format!(
            "open {} to honor raw-mirror prune --keep-tag",
            db_path.display()
        )
    })?;
    let _ = conn.execute("PRAGMA query_only = 1;");

    let mut pinned = HashSet::new();
    for id_chunk in conversation_ids.chunks(400) {
        let tag_placeholders = (0..keep_tags.len())
            .map(|idx| format!("?{}", idx + 1))
            .collect::<Vec<_>>()
            .join(", ");
        let id_offset = keep_tags.len();
        let id_placeholders = (0..id_chunk.len())
            .map(|idx| format!("?{}", id_offset + idx + 1))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT DISTINCT ct.conversation_id \
             FROM conversation_tags ct \
             JOIN tags t ON t.id = ct.tag_id \
             WHERE t.name IN ({tag_placeholders}) \
               AND ct.conversation_id IN ({id_placeholders})"
        );
        let mut params = keep_tags
            .iter()
            .map(|tag| ParamValue::from(tag.as_str()))
            .collect::<Vec<_>>();
        params.extend(id_chunk.iter().copied().map(ParamValue::from));
        let rows: Vec<i64> = conn
            .query_map_collect(&sql, &params, |row: &crate::franken_sync::Row| {
                row.get_typed(0)
            })
            .with_context(|| "query raw-mirror prune keep-tag conversation pins")?;
        pinned.extend(rows);
    }

    Ok(pinned)
}

pub(in crate::raw_mirror) fn collect_raw_mirror_physical_blobs(
    root: &Path,
) -> Result<Vec<RawMirrorPhysicalBlob>> {
    let blobs_root = root.join("blobs").join(RAW_MIRROR_HASH_ALGORITHM);
    let metadata = match raw_mirror_symlink_metadata(&blobs_root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("stat raw mirror blob root {}", blobs_root.display()));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(anyhow!(
            "refusing to inventory invalid raw mirror blob root {}",
            blobs_root.display()
        ));
    }

    let mut stack = vec![blobs_root];
    let mut blobs = Vec::new();
    while let Some(directory) = stack.pop() {
        let mut entries = raw_mirror_io("read_dir", &directory, || fs::read_dir(&directory))
            .with_context(|| format!("read raw mirror blob directory {}", directory.display()))?;
        while let Some(entry) =
            raw_mirror_io("read_dir_next", &directory, || entries.next().transpose())?
        {
            let path = entry.path();
            let metadata = raw_mirror_symlink_metadata(&path).with_context(|| {
                format!("stat raw mirror blob inventory path {}", path.display())
            })?;
            if metadata.file_type().is_symlink() {
                return Err(anyhow!(
                    "refusing to inventory symlinked raw mirror blob path {}",
                    path.display()
                ));
            }
            if metadata.is_dir() {
                stack.push(path);
                continue;
            }
            if !metadata.is_file() {
                return Err(anyhow!(
                    "refusing to inventory non-file raw mirror blob path {}",
                    path.display()
                ));
            }
            if path.extension().and_then(|extension| extension.to_str())
                != Some(RAW_MIRROR_BLOB_EXTENSION)
            {
                continue;
            }

            let blob_blake3 = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .ok_or_else(|| anyhow!("raw mirror blob filename is not valid UTF-8"))?
                .to_string();
            let expected_relative = raw_mirror_blob_relative_path(&blob_blake3)
                .ok_or_else(|| anyhow!("raw mirror blob filename has an invalid digest"))?;
            let actual_relative = path.strip_prefix(root).with_context(|| {
                format!("raw mirror blob escaped inventory root {}", path.display())
            })?;
            if actual_relative != Path::new(&expected_relative) {
                return Err(anyhow!(
                    "refusing non-canonical raw mirror blob path {}; expected {}",
                    path.display(),
                    root.join(&expected_relative).display()
                ));
            }
            blobs.push(RawMirrorPhysicalBlob {
                relative_path: expected_relative,
                blob_blake3,
                size_bytes: metadata.len(),
                modified_at_ms: metadata.modified().ok().and_then(system_time_to_ms),
            });
        }
    }
    blobs.sort_by(|left, right| {
        left.modified_at_ms
            .unwrap_or(i64::MAX)
            .cmp(&right.modified_at_ms.unwrap_or(i64::MAX))
            .then_with(|| left.relative_path.cmp(&right.relative_path))
    });
    Ok(blobs)
}

pub(in crate::raw_mirror) fn validate_prune_target_file(root: &Path, path: &Path) -> Result<bool> {
    if raw_mirror_path_has_symlink_below_root(root, path) {
        anyhow::bail!(
            "refusing to prune raw mirror path with a symlinked ancestor {}",
            path.display()
        );
    }
    let metadata = match raw_mirror_symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err).with_context(|| format!("stat {}", path.display())),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        anyhow::bail!(
            "refusing to prune non-regular raw mirror file {}",
            path.display()
        );
    }
    Ok(true)
}

pub(in crate::raw_mirror) fn remove_prune_entry_file(
    root: &Path,
    entry: &RawMirrorPruneEntry,
) -> Result<bool> {
    let path = root.join(&entry.path);
    if entry.kind == "staging" {
        return remove_abandoned_capture_staging(root, &path, entry.size_bytes);
    }
    if !validate_prune_target_file(root, &path)? {
        return Ok(false);
    }
    let metadata = raw_mirror_symlink_metadata(&path)
        .with_context(|| format!("restat raw mirror prune target {}", path.display()))?;
    if metadata.len() != entry.size_bytes {
        return Err(anyhow!(
            "raw mirror prune target {} changed size after preflight: observed {}, expected {}",
            path.display(),
            metadata.len(),
            entry.size_bytes
        ));
    }

    match entry.kind.as_str() {
        "blob" => {
            let expected_blake3 = entry.blob_blake3.as_deref().ok_or_else(|| {
                anyhow!(
                    "raw mirror prune blob target {} is missing its expected checksum",
                    path.display()
                )
            })?;
            verify_existing_blob_reference(
                root,
                &RawMirrorChunkRef {
                    blob_relative_path: entry.path.clone(),
                    blob_blake3: expected_blake3.to_string(),
                    blob_size_bytes: entry.size_bytes,
                },
            )
            .with_context(|| {
                format!(
                    "revalidating raw mirror blob immediately before deletion {}",
                    path.display()
                )
            })?;
        }
        "manifest" => {
            let expected_blob_blake3 = entry.blob_blake3.as_deref().ok_or_else(|| {
                anyhow!(
                    "raw mirror prune manifest target {} is missing its expected content identity",
                    path.display()
                )
            })?;
            let expected_manifest_blake3 = entry.manifest_blake3.as_deref().ok_or_else(|| {
                anyhow!(
                    "raw mirror prune manifest target {} is missing its expected descriptor checksum",
                    path.display()
                )
            })?;
            let observed_manifest = validated_existing_manifest(root, &path, expected_blob_blake3)
                .with_context(|| {
                    format!(
                        "revalidating raw mirror manifest immediately before deletion {}",
                        path.display()
                    )
                })?;
            if observed_manifest.manifest_blake3.as_deref() != Some(expected_manifest_blake3) {
                return Err(anyhow!(
                    "raw mirror prune manifest checksum changed after preflight for {}: observed {:?}, expected {}",
                    path.display(),
                    observed_manifest.manifest_blake3,
                    expected_manifest_blake3
                ));
            }
        }
        kind => {
            return Err(anyhow!(
                "raw mirror prune target {} has unsupported kind {kind}",
                path.display()
            ));
        }
    }

    raw_mirror_tracked_io("remove_pruned_file", &path, || fs::remove_file(&path))
        .with_context(|| format!("remove raw mirror file {}", path.display()))?;
    sync_parent_unconditionally(&path)?;
    Ok(true)
}

/// Capture staging untouched for this long is reclaimed by an unscoped prune.
/// The mutation lock already proves no capture owns it; the age is a margin.
const ABANDONED_STAGING_MIN_AGE_MS: i64 = 60 * 60 * 1000;

struct CaptureStagingDir {
    relative_path: String,
    size_bytes: u64,
    newest_modified_ms: i64,
    contains_symlink: bool,
}

/// `capture.<pid>.<nanos>.<nonce>`, the name `unique_capture_temp_dir` gives.
fn is_capture_staging_name(name: &str) -> bool {
    let parts = name.split('.').collect::<Vec<_>>();
    parts.len() == 4
        && parts[0] == "capture"
        && parts[1..]
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
}

fn collect_capture_staging_dirs(root: &Path) -> Result<Vec<CaptureStagingDir>> {
    let tmp = root.join("tmp");
    let metadata = match fs::symlink_metadata(&tmp) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err).with_context(|| format!("stat {}", tmp.display())),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        anyhow::bail!(
            "refusing to prune invalid raw mirror staging directory {}",
            tmp.display()
        );
    }
    let mut staging = Vec::new();
    for entry in fs::read_dir(&tmp).with_context(|| format!("read {}", tmp.display()))? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        if !is_capture_staging_name(&name) {
            continue;
        }
        let path = entry.path();
        let metadata =
            fs::symlink_metadata(&path).with_context(|| format!("stat {}", path.display()))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            continue;
        }
        let (size_bytes, newest_modified_ms, contains_symlink) = measure_capture_staging(&path)?;
        staging.push(CaptureStagingDir {
            relative_path: format!("tmp/{name}"),
            size_bytes,
            newest_modified_ms,
            contains_symlink,
        });
    }
    staging.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(staging)
}

/// File bytes below `path`, the newest mtime of it or anything in it (an
/// unknown mtime counts as now), and whether a symlink appears below it.
fn measure_capture_staging(path: &Path) -> Result<(u64, i64, bool)> {
    let mut size_bytes = 0_u64;
    let mut newest_modified_ms = i64::MIN;
    let mut contains_symlink = false;
    let mut pending = vec![path.to_path_buf()];
    while let Some(current) = pending.pop() {
        let metadata = fs::symlink_metadata(&current)
            .with_context(|| format!("stat {}", current.display()))?;
        let modified_ms = metadata
            .modified()
            .ok()
            .and_then(system_time_to_ms)
            .unwrap_or(i64::MAX);
        newest_modified_ms = newest_modified_ms.max(modified_ms);
        if metadata.file_type().is_symlink() {
            contains_symlink = true;
        } else if metadata.is_dir() {
            for entry in
                fs::read_dir(&current).with_context(|| format!("read {}", current.display()))?
            {
                pending.push(entry?.path());
            }
        } else {
            size_bytes = size_bytes.saturating_add(metadata.len());
        }
    }
    Ok((size_bytes, newest_modified_ms, contains_symlink))
}

fn validate_prune_target_staging(root: &Path, path: &Path) -> Result<bool> {
    if raw_mirror_path_has_symlink_below_root(root, path) {
        anyhow::bail!(
            "refusing to prune raw mirror path with a symlinked ancestor {}",
            path.display()
        );
    }
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err).with_context(|| format!("stat {}", path.display())),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        anyhow::bail!(
            "refusing to prune non-directory raw mirror capture staging {}",
            path.display()
        );
    }
    Ok(true)
}

fn remove_abandoned_capture_staging(
    root: &Path,
    path: &Path,
    expected_size_bytes: u64,
) -> Result<bool> {
    if !validate_prune_target_staging(root, path)? {
        return Ok(false);
    }
    let (size_bytes, _, contains_symlink) = measure_capture_staging(path)?;
    if contains_symlink || size_bytes != expected_size_bytes {
        return Err(anyhow!(
            "raw mirror capture staging {} changed after preflight: observed {size_bytes} bytes, expected {expected_size_bytes}",
            path.display()
        ));
    }
    fs::remove_dir_all(path)
        .with_context(|| format!("remove raw mirror capture staging {}", path.display()))?;
    sync_parent_unconditionally(path)?;
    Ok(true)
}

pub(in crate::raw_mirror) fn open_prune_audit_log(root: &Path) -> Result<(PathBuf, File)> {
    ensure_private_dir(root)?;
    let audit_path = root.join("pruned.jsonl");
    ensure_prune_audit_log_appendable(&audit_path)?;
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    configure_lock_open_options(&mut options);
    let file = raw_mirror_io("open_prune_audit", &audit_path, || {
        options.open(&audit_path)
    })
    .with_context(|| format!("open raw mirror prune audit {}", audit_path.display()))?;
    let opened_metadata =
        raw_mirror_tracked_io("fstat_prune_audit", &audit_path, || file.metadata()).with_context(
            || {
                format!(
                    "stat opened raw mirror prune audit {}",
                    audit_path.display()
                )
            },
        )?;
    let path_metadata = raw_mirror_symlink_metadata(&audit_path)
        .with_context(|| format!("restat raw mirror prune audit {}", audit_path.display()))?;
    if path_metadata.file_type().is_symlink()
        || !path_metadata.is_file()
        || !same_source_identity(&opened_metadata, &path_metadata)
    {
        return Err(anyhow!(
            "raw mirror prune audit {} changed identity while being opened",
            audit_path.display()
        ));
    }
    set_private_open_file_permissions(&file, &audit_path)?;
    Ok((audit_path, file))
}

fn append_prune_audit_records(
    file: &mut File,
    audit_path: &Path,
    entries: &[RawMirrorPruneEntry],
    phase: &str,
) -> Result<()> {
    let now = now_ms();
    for entry in entries {
        let record = json!({
            "schema_version": 1,
            "recorded_at_ms": now,
            "mode": "apply",
            "phase": phase,
            "kind": entry.kind,
            "path": entry.path,
            "blob_blake3": entry.blob_blake3,
            "manifest_blake3": entry.manifest_blake3,
            "size_bytes": entry.size_bytes,
            "reason": entry.reason,
            "applied": entry.applied,
        });
        raw_mirror_tracked_io("write_prune_audit", audit_path, || {
            writeln!(file, "{record}")
        })
        .with_context(|| format!("write raw mirror prune audit {}", audit_path.display()))?;
    }
    // Unlike optional capture durability, this journal authorizes deletion.
    // Never advance past an intent/result whose sync failed or was disabled.
    raw_mirror_tracked_io("sync_prune_audit", audit_path, || file.sync_all())
        .with_context(|| format!("sync raw mirror prune audit {}", audit_path.display()))?;
    sync_parent_unconditionally(audit_path)?;
    Ok(())
}

fn ensure_prune_audit_log_appendable(path: &Path) -> Result<()> {
    match raw_mirror_symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            anyhow::bail!(
                "refusing to append raw mirror prune audit through symlink {}",
                path.display()
            );
        }
        Ok(metadata) if !metadata.is_file() => {
            anyhow::bail!(
                "refusing to append raw mirror prune audit to non-file {}",
                path.display()
            );
        }
        Ok(_) => Ok(()),
        Err(err) if matches!(err.kind(), std::io::ErrorKind::NotFound) => Ok(()),
        Err(err) => Err(err).with_context(|| {
            format!(
                "inspect raw mirror prune audit before append {}",
                path.display()
            )
        }),
    }
}
