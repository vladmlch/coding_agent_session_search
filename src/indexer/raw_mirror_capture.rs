use super::*;

/// When no storage is supplied, discover configured paths without opening the
/// archive. This permits mirror previews before acquiring the indexing lock.
pub fn build_scan_roots(storage: Option<&FrankenStorage>, data_dir: &Path) -> Vec<ScanRoot> {
    build_scan_roots_with_missing(storage, data_dir, false)
}

/// A resident watcher must retain explicitly configured roots while they are
/// absent, so registration can start when they appear. Ordinary source scans
/// and previews retain their existing present-path-only behavior.
pub(in crate::indexer) fn build_scan_roots_with_missing(
    storage: Option<&FrankenStorage>,
    data_dir: &Path,
    retain_missing: bool,
) -> Vec<ScanRoot> {
    let mut roots = Vec::new();

    // Add local default root with local provenance
    // We create a single "local" root that encompasses all local paths.
    // Connectors will use their own default detection logic when given an empty scan_roots.
    // For explicit multi-root support, we add the local root.
    roots.push(ScanRoot::local(data_dir.to_path_buf()));

    if dotenvy::var("CASS_IGNORE_SOURCES_CONFIG").is_err()
        && let Ok(config) = SourcesConfig::load()
        && !config.sources.is_empty()
    {
        for source in &config.sources {
            let origin = Origin {
                source_id: source.name.clone(),
                kind: source.source_type,
                host: source.host.clone(),
            };
            let platform = source.platform;
            let workspace_rewrites = source.path_mappings.clone();

            for path in &source.paths {
                if source.is_remote() {
                    // Sync keys mirror directories by the exact configured
                    // path, independent of remote home expansion.
                    let safe_name = path_to_safe_dirname(path);
                    let mirror_base = data_dir.join("remotes").join(&source.name).join("mirror");
                    let mirror_path = mirror_base.join(&safe_name);

                    if mirror_path.exists() {
                        let mut scan_root = ScanRoot::remote(mirror_path, origin.clone(), platform)
                            .with_mirrored_source_path(path);
                        scan_root.workspace_rewrites = workspace_rewrites.clone();
                        roots.push(scan_root);
                        continue;
                    }

                    let roots_before_legacy_lookup = roots.len();
                    if path.starts_with("~/") {
                        let suffix = path.trim_start_matches("~/");
                        let safe_suffix = path_to_safe_dirname(suffix);
                        if let Ok(entries) = std::fs::read_dir(&mirror_base) {
                            for entry in entries.flatten() {
                                let name = entry.file_name();
                                let name_str = name.to_string_lossy();
                                if name_str.ends_with(&safe_suffix) && entry.path().is_dir() {
                                    let mut scan_root =
                                        ScanRoot::remote(entry.path(), origin.clone(), platform)
                                            .with_mirrored_source_path(path);
                                    scan_root.workspace_rewrites = workspace_rewrites.clone();
                                    roots.push(scan_root);
                                    break;
                                }
                            }
                        }
                    }
                    if retain_missing && roots.len() == roots_before_legacy_lookup {
                        let mut scan_root = ScanRoot::remote(mirror_path, origin.clone(), platform)
                            .with_mirrored_source_path(path);
                        scan_root.workspace_rewrites = workspace_rewrites.clone();
                        roots.push(scan_root);
                    }
                } else {
                    let local_path = expand_local_scan_root_path(path);
                    if !retain_missing && !local_path.exists() {
                        continue;
                    }
                    let mut scan_root = ScanRoot::local(local_path);
                    scan_root.origin = origin.clone();
                    scan_root.platform = platform;
                    scan_root.workspace_rewrites = workspace_rewrites.clone();
                    roots.push(scan_root);
                }
            }
        }
        return roots;
    }

    // Fallback: remote mirror roots from registered sources
    if let Some(storage) = storage
        && let Ok(sources) = storage.list_sources()
    {
        for source in sources {
            // Parse platform from source
            let platform =
                source
                    .platform
                    .as_deref()
                    .and_then(|p| match p.to_lowercase().as_str() {
                        "macos" => Some(Platform::Macos),
                        "linux" => Some(Platform::Linux),
                        "windows" => Some(Platform::Windows),
                        _ => None,
                    });

            // Parse workspace rewrites from config_json
            // Format: array of {from, to, agents?} objects
            let workspace_rewrites = source
                .config_json
                .as_ref()
                .and_then(|cfg| cfg.get("path_mappings"))
                .and_then(|arr| arr.as_array())
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| {
                            let from = item.get("from")?.as_str()?.to_string();
                            let to = item.get("to")?.as_str()?.to_string();
                            let agents = item.get("agents").and_then(|a| {
                                a.as_array().map(|arr| {
                                    arr.iter()
                                        .filter_map(|v| v.as_str().map(String::from))
                                        .collect()
                                })
                            });
                            Some(crate::sources::config::PathMapping { from, to, agents })
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            if let Some(paths) = source
                .config_json
                .as_ref()
                .and_then(|cfg| cfg.get("paths"))
                .and_then(|arr| arr.as_array())
            {
                for path_val in paths {
                    let Some(path) = path_val.as_str() else {
                        continue;
                    };
                    if source.kind.is_remote() {
                        let safe_name = path_to_safe_dirname(path);
                        let mirror_path = data_dir
                            .join("remotes")
                            .join(&source.id)
                            .join("mirror")
                            .join(&safe_name);
                        if !retain_missing && !mirror_path.exists() {
                            continue;
                        }

                        let origin = Origin {
                            source_id: source.id.clone(),
                            kind: source.kind,
                            host: source.host_label.clone(),
                        };
                        let mut scan_root = ScanRoot::remote(mirror_path, origin, platform)
                            .with_mirrored_source_path(path);
                        scan_root.workspace_rewrites = workspace_rewrites.clone();
                        roots.push(scan_root);
                    } else {
                        let local_path = expand_local_scan_root_path(path);
                        if !retain_missing && !local_path.exists() {
                            continue;
                        }

                        let origin = Origin {
                            source_id: source.id.clone(),
                            kind: source.kind,
                            host: source.host_label.clone(),
                        };
                        let mut scan_root = ScanRoot::local(local_path);
                        scan_root.origin = origin;
                        scan_root.platform = platform;
                        scan_root.workspace_rewrites = workspace_rewrites.clone();
                        roots.push(scan_root);
                    }
                }
                continue;
            }

            // Remote mirror directory: data_dir/remotes/<source_id>/mirror
            let mirror_path = data_dir.join("remotes").join(&source.id).join("mirror");

            if source.kind.is_remote() && (retain_missing || mirror_path.exists()) {
                let origin = Origin {
                    source_id: source.id.clone(),
                    kind: source.kind,
                    host: source.host_label.clone(),
                };
                let mut scan_root = ScanRoot::remote(mirror_path, origin, platform);
                scan_root.workspace_rewrites = workspace_rewrites;

                roots.push(scan_root);
            }
        }
    }

    roots
}

/// Inject provenance metadata into a conversation from a scan root's origin.
///
/// This adds the `cass.origin` field to the conversation's metadata JSON
/// so that persistence can extract and store the source_id.
///
/// Part of P2.2 - provenance injection. Doctor's raw-mirror reconstruct
/// stamps re-parsed sources the same way (zp0fp).
pub(crate) fn inject_provenance(conv: &mut NormalizedConversation, origin: &Origin) {
    // Ensure metadata is an object
    if !conv.metadata.is_object() {
        conv.metadata = serde_json::json!({});
    }

    // Add cass.origin provenance
    if let Some(obj) = conv.metadata.as_object_mut() {
        let cass = obj
            .entry("cass".to_string())
            .or_insert_with(|| serde_json::json!({}));
        if !cass.is_object() {
            *cass = serde_json::json!({});
        }
        if let Some(cass_obj) = cass.as_object_mut() {
            cass_obj.insert(
                "origin".to_string(),
                serde_json::json!({
                    "source_id": origin.source_id,
                    "kind": origin.kind.as_str(),
                    "host": origin.host
                }),
            );
        }
    }
}

/// Collapse a Claude Code `external_id` to a single canonical rooting so that
/// the full-rebuild and incremental/watch ingest paths agree on the dedup key
/// `(source_id, agent_id, external_id)` for the *same* session file (gh #302).
///
/// The Claude connector derives `external_id` by stripping the scan root from
/// the session path. The two ingest paths use *different* scan roots for the
/// same corpus:
///
/// - Full rebuild (`ScanContext::local_default` → `source_roots` →
///   `projects_root_candidates()`) roots at `~/.claude/projects`, yielding a
///   bare relative id: `-Users-…-proj/<uuid>.jsonl`.
/// - The live `--watch` daemon roots at the connector's detected root
///   (`detect_installed_agents` reports `~/.claude` for `claude`), yielding a
///   `projects/`-prefixed id: `projects/-Users-…-proj/<uuid>.jsonl`.
///
/// Because the two forms differ only by the leading `projects/` segment, the
/// path that runs second fails to match the existing row and inserts a
/// duplicate. Canonicalizing to the bare form (the full-rebuild rooting) at this
/// single ingest chokepoint makes every path produce one stable key, so dedup
/// matches an existing row instead of accumulating duplicates as the watcher
/// runs.
pub(in crate::indexer) fn canonicalize_claude_external_id(
    connector_name: &str,
    conv: &mut NormalizedConversation,
) {
    if connector_name != "claude" {
        return;
    }
    let Some(external_id) = conv.external_id.as_ref() else {
        return;
    };
    // The connector emits OS-native separators from `Path::strip_prefix`, so
    // strip a single leading `projects/` (or `projects\` on Windows) segment.
    let canonical = external_id
        .strip_prefix("projects/")
        .or_else(|| external_id.strip_prefix("projects\\"));
    if let Some(canonical) = canonical {
        // Never canonicalize a bare `projects/<uuid>.jsonl` (no intervening
        // workspace segment) away into a workspace-less id; the real Claude
        // layout always has a `<workspace-slug>/<uuid>.jsonl` tail, so a stripped
        // form that still contains a separator is the genuine bare relative id.
        if canonical.contains('/') || canonical.contains('\\') {
            conv.external_id = Some(canonical.to_string());
        }
    }
}

pub(in crate::indexer) fn prepare_conversation_for_ingest(
    data_dir: &Path,
    connector_name: &str,
    origin: &Origin,
    workspace_rewrite_root: Option<&ScanRoot>,
    conv: &mut NormalizedConversation,
    raw_source_exclusions: &RawSourceExclusions,
) {
    prepare_conversation_metadata_for_ingest(connector_name, origin, workspace_rewrite_root, conv);
    capture_prepared_conversation(data_dir, conv, raw_source_exclusions);
}

pub(in crate::indexer) fn prepare_conversation_metadata_for_ingest(
    connector_name: &str,
    origin: &Origin,
    workspace_rewrite_root: Option<&ScanRoot>,
    conv: &mut NormalizedConversation,
) {
    inject_provenance(conv, origin);
    canonicalize_claude_external_id(connector_name, conv);
    if let Some(root) = workspace_rewrite_root {
        apply_workspace_rewrite(conv, root);
    }
}

pub(in crate::indexer) fn capture_prepared_conversation(
    data_dir: &Path,
    conv: &mut NormalizedConversation,
    raw_source_exclusions: &RawSourceExclusions,
) {
    if WorkspaceExclusions::from_env()
        .is_ok_and(|policy| !policy.excludes(conv.workspace.as_deref()))
    {
        attach_raw_mirror_capture(data_dir, conv, raw_source_exclusions);
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn capture_connector_sources_before_parse(
    connector: &(dyn crate::connectors::Connector + Send),
    ctx: &crate::connectors::ScanContext,
    data_dir: &Path,
    provider: &str,
    fallback_roots: &[ScanRoot],
    since_ts: Option<i64>,
    active_source_filter: &ActiveSessionSourceFilter,
    raw_source_exclusions: &RawSourceExclusions,
) -> (ConnectorIngestRun, bool) {
    // f3grf: raw-mirror captures use the slug the parsed conversations carry
    // (the manifest id hashes the provider). Under the registry name, `claude`
    // sources got a second manifest that no DB link ever reached. Diagnostics
    // keep the registry name.
    let mirror_provider = crate::connectors::archive_provider_slug(provider);
    let mut active_source_skipped = false;
    let (observed_sources, discovery_error) = match connector.discover_source_files(ctx) {
        Ok(sources) if !sources.is_empty() => {
            let primary_source_count = sources
                .iter()
                .filter(|source| {
                    source.role == crate::connectors::DiscoveredSourceRole::PrimarySessionLog
                })
                .count();
            // A per-session transcript's workspace is known only after parse.
            // Defer its capture to prepare_conversation_for_ingest so excluded
            // transcripts do not create new mirror blobs. Shared databases and
            // reconstruction sidecars remain recovery evidence unless a
            // permanent session exclusion suppresses their physical container.
            let workspace_exclusions_active = WorkspaceExclusions::from_env()
                .map(|policy| !policy.patterns.is_empty())
                .unwrap_or(true);
            let defer_primary_sources = primary_source_count
                > PREPARSE_PRIMARY_SOURCE_CAPTURE_LIMIT
                || workspace_exclusions_active;
            if defer_primary_sources {
                tracing::info!(
                    provider,
                    primary_source_count,
                    limit = PREPARSE_PRIMARY_SOURCE_CAPTURE_LIMIT,
                    workspace_exclusions_active,
                    "deferring primary source raw-mirror capture to per-conversation streaming path"
                );
            }
            let mut observed_sources = Vec::with_capacity(sources.len());
            for source in sources {
                if should_skip_active_session_source(
                    active_source_filter,
                    source.origin.kind,
                    &source.source_path,
                ) {
                    active_source_skipped = true;
                    continue;
                }
                if defer_primary_sources
                    && source.role == crate::connectors::DiscoveredSourceRole::PrimarySessionLog
                {
                    observed_sources.push(source);
                    continue;
                }
                capture_discovered_source_file_before_parse(
                    data_dir,
                    mirror_provider,
                    &source,
                    raw_source_exclusions,
                );
                observed_sources.push(source);
            }
            (observed_sources, None)
        }
        Ok(_) => {
            for root in fallback_roots {
                active_source_skipped |= capture_scan_sources_before_parse(
                    data_dir,
                    mirror_provider,
                    root,
                    since_ts,
                    active_source_filter,
                    raw_source_exclusions,
                );
            }
            (Vec::new(), None)
        }
        Err(error) => {
            tracing::warn!(
                provider,
                error = %format_args!("{error:#}"),
                "provider source discovery failed; falling back to legacy explicit-root preparse capture"
            );
            for root in fallback_roots {
                active_source_skipped |= capture_scan_sources_before_parse(
                    data_dir,
                    mirror_provider,
                    root,
                    since_ts,
                    active_source_filter,
                    raw_source_exclusions,
                );
            }
            (Vec::new(), Some(error))
        }
    };
    let mut run = ConnectorIngestRun::begin(provider, data_dir, ctx, &observed_sources);
    if let Some(error) = discovery_error {
        // GH #511: discovery can fail before any transcript is returned. Keep
        // its I/O cause and the failed directory instead of blaming CASS's own
        // output directory. WalkDir names the precise operation when possible.
        let source_path = error
            .chain()
            .find_map(|cause| {
                cause
                    .downcast_ref::<walkdir::Error>()
                    .and_then(walkdir::Error::path)
            })
            .or_else(|| {
                fallback_roots
                    .first()
                    .or_else(|| ctx.scan_roots.first())
                    .map(|root| root.path.as_path())
            })
            .unwrap_or_else(|| Path::new("(source root unavailable)"));
        run.observe_connector_scan_error(source_path, &error);
    }
    (run, active_source_skipped)
}

pub(in crate::indexer) fn should_skip_raw_mirror_capture_for_logical_source(path: &Path) -> bool {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            !file_type.is_file() && !file_type.is_symlink()
        }
        Err(error) => matches!(
            error.kind(),
            std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
        ),
    }
}

pub(in crate::indexer) fn capture_discovered_source_file_before_parse(
    data_dir: &Path,
    provider: &str,
    source: &crate::connectors::DiscoveredSourceFile,
    raw_source_exclusions: &RawSourceExclusions,
) {
    if !crate::raw_mirror::capture_enabled() {
        return;
    }
    if let Err(reason) = validate_discovered_source_path(source) {
        tracing::warn!(
            provider,
            discovered_provider = %source.provider_slug,
            role = source.role.as_str(),
            source_id = %source.origin.source_id,
            scan_root = %source.scan_root.display(),
            path = %source.source_path.display(),
            reason,
            "refusing to raw-mirror discovered provider source outside its scan root"
        );
        return;
    }
    if raw_source_exclusions.excludes(&source.source_path) {
        return;
    }

    match crate::raw_mirror::capture_source_file(crate::raw_mirror::RawMirrorCaptureInput {
        data_dir,
        provider,
        source_id: &source.origin.source_id,
        origin_kind: source.origin.kind.as_str(),
        origin_host: source.origin.host.as_deref(),
        source_path: &source.source_path,
        db_links: &[],
    }) {
        Ok(record) => {
            tracing::debug!(
                provider,
                discovered_provider = %source.provider_slug,
                role = source.role.as_str(),
                source_id = %source.origin.source_id,
                manifest_id = %record.manifest_id,
                blob_blake3 = %record.blob_blake3,
                already_present = record.already_present,
                required_for_reconstruction = source.required_for_reconstruction,
                "captured discovered provider source into raw mirror before connector parse"
            );
        }
        Err(error) => {
            tracing::warn!(
                provider,
                discovered_provider = %source.provider_slug,
                role = source.role.as_str(),
                source_id = %source.origin.source_id,
                path = %source.source_path.display(),
                error = %error,
                "failed to capture discovered provider source into raw mirror before connector parse"
            );
        }
    }
}

pub(in crate::indexer) fn validate_discovered_source_path(
    source: &crate::connectors::DiscoveredSourceFile,
) -> Result<(), &'static str> {
    if source.scan_root.as_os_str().is_empty() {
        return Err("empty scan root");
    }
    if !source.scan_root.is_absolute() {
        return Err("scan root is not absolute");
    }
    if !source.source_path.is_absolute() {
        return Err("source path is not absolute");
    }
    if !path_has_no_relative_escape(&source.scan_root) {
        return Err("scan root contains relative components");
    }
    if !path_has_no_relative_escape(&source.source_path) {
        return Err("source path contains relative components");
    }
    if source.source_path == source.scan_root || source.source_path.starts_with(&source.scan_root) {
        if discovered_source_path_has_symlink_component(&source.scan_root, &source.source_path) {
            return Err("source path contains symlink component");
        }
        return Ok(());
    }
    Err("source path is not contained by scan root")
}

pub(in crate::indexer) fn discovered_source_path_has_symlink_component(
    scan_root: &Path,
    source_path: &Path,
) -> bool {
    if path_component_is_symlink(scan_root) {
        return true;
    }

    let Ok(relative) = source_path.strip_prefix(scan_root) else {
        return false;
    };
    let mut current = scan_root.to_path_buf();
    for component in relative.components() {
        if let std::path::Component::Normal(part) = component {
            current.push(part);
            if path_component_is_symlink(&current) {
                return true;
            }
        }
    }
    false
}

pub(in crate::indexer) fn path_component_is_symlink(path: &Path) -> bool {
    std::fs::symlink_metadata(path)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
}

pub(in crate::indexer) fn path_has_no_relative_escape(path: &Path) -> bool {
    path.components().all(|component| {
        matches!(
            component,
            std::path::Component::Prefix(_)
                | std::path::Component::RootDir
                | std::path::Component::CurDir
                | std::path::Component::Normal(_)
        )
    })
}

pub(in crate::indexer) fn capture_scan_sources_before_parse(
    data_dir: &Path,
    provider: &str,
    root: &ScanRoot,
    since_ts: Option<i64>,
    active_source_filter: &ActiveSessionSourceFilter,
    raw_source_exclusions: &RawSourceExclusions,
) -> bool {
    let mut active_source_skipped = false;
    for capture_root in preparse_capture_roots(provider, root, since_ts) {
        active_source_skipped |= capture_scan_root_file_before_parse(
            data_dir,
            provider,
            &capture_root,
            active_source_filter,
            raw_source_exclusions,
        );
    }
    active_source_skipped
}

pub(in crate::indexer) fn preparse_capture_roots(
    provider: &str,
    root: &ScanRoot,
    since_ts: Option<i64>,
) -> Vec<ScanRoot> {
    if root.path.is_file() {
        return vec![root.clone()];
    }

    if provider == "codex" {
        let preflight = crate::connectors::preflight_codex_explicit_file_roots(
            std::slice::from_ref(root),
            since_ts,
        );
        let capture_roots: Vec<ScanRoot> = preflight
            .scan_roots
            .into_iter()
            .filter(|scan_root| scan_root.path.is_file())
            .collect();
        if !capture_roots.is_empty() {
            tracing::debug!(
                provider,
                scan_root = %root.path.display(),
                capture_files = capture_roots.len(),
                fallback_roots = preflight.fallback_roots,
                "expanded codex directory root into explicit raw-mirror preparse capture files"
            );
        }
        return capture_roots;
    }

    Vec::new()
}

pub(in crate::indexer) fn capture_scan_root_file_before_parse(
    data_dir: &Path,
    provider: &str,
    root: &ScanRoot,
    active_source_filter: &ActiveSessionSourceFilter,
    raw_source_exclusions: &RawSourceExclusions,
) -> bool {
    if !root.path.is_file() {
        return false;
    }
    if should_skip_active_session_source(active_source_filter, root.origin.kind, &root.path) {
        return true;
    }
    if !crate::raw_mirror::capture_enabled() || raw_source_exclusions.excludes(&root.path) {
        return false;
    }
    // Failed/empty discovery cannot prove that this fallback file contains
    // only admitted workspaces. Defer capture until a parsed conversation
    // reaches the normal workspace gate, just as discovered transcripts do.
    if WorkspaceExclusions::from_env()
        .map(|policy| !policy.patterns.is_empty())
        .unwrap_or(true)
    {
        return false;
    }
    match crate::raw_mirror::capture_source_file(crate::raw_mirror::RawMirrorCaptureInput {
        data_dir,
        provider,
        source_id: &root.origin.source_id,
        origin_kind: root.origin.kind.as_str(),
        origin_host: root.origin.host.as_deref(),
        source_path: &root.path,
        db_links: &[],
    }) {
        Ok(record) => {
            tracing::debug!(
                provider,
                source_id = %root.origin.source_id,
                manifest_id = %record.manifest_id,
                blob_blake3 = %record.blob_blake3,
                already_present = record.already_present,
            "captured explicit scan-root source into raw mirror before connector parse"
            );
        }
        Err(error) => {
            tracing::warn!(
                provider,
                source_id = %root.origin.source_id,
                path = %root.path.display(),
                error = %error,
                "failed to capture explicit scan-root source into raw mirror before connector parse"
            );
        }
    }
    false
}

pub(in crate::indexer) fn attach_raw_mirror_capture(
    data_dir: &Path,
    conv: &mut NormalizedConversation,
    raw_source_exclusions: &RawSourceExclusions,
) {
    // GH #506: CASS_RAW_MIRROR=off leaves the conversation without raw-mirror
    // metadata; the archive rows are written as usual.
    if !crate::raw_mirror::capture_enabled() {
        return;
    }
    if should_skip_raw_mirror_capture_for_logical_source(&conv.source_path) {
        tracing::debug!(
            agent = %conv.agent_slug,
            source_path = %conv.source_path.display(),
            "skipping raw-mirror capture for logical non-file parsed conversation source"
        );
        return;
    }
    if raw_source_exclusions.excludes(&conv.source_path) {
        return;
    }

    let (source_id, origin_kind, origin_host) = raw_mirror_origin_from_metadata(&conv.metadata);
    match crate::raw_mirror::capture_source_file(crate::raw_mirror::RawMirrorCaptureInput {
        data_dir,
        provider: &conv.agent_slug,
        source_id: &source_id,
        origin_kind: &origin_kind,
        origin_host: origin_host.as_deref(),
        source_path: &conv.source_path,
        // A conversation is not linked to the canonical archive until its
        // transaction returns an actual conversation id. Persist batches add
        // those authoritative links in one manifest update per source.
        db_links: &[],
    }) {
        Ok(record) => {
            attach_raw_mirror_metadata(conv, &record);
            tracing::debug!(
                agent = %conv.agent_slug,
                source_id = %source_id,
                manifest_id = %record.manifest_id,
                blob_blake3 = %record.blob_blake3,
                already_present = record.already_present,
                "captured parsed conversation source into raw mirror before archive upsert"
            );
        }
        Err(error) => {
            tracing::warn!(
                agent = %conv.agent_slug,
                source_id = %source_id,
                source_path = %conv.source_path.display(),
                error = %error,
                "failed to capture parsed conversation source into raw mirror before archive upsert"
            );
        }
    }
}

pub(in crate::indexer) fn raw_mirror_origin_from_metadata(
    metadata: &serde_json::Value,
) -> (String, String, Option<String>) {
    let cass_origin = metadata.get("cass").and_then(|cass| cass.get("origin"));
    let source_id = cass_origin
        .and_then(|origin| origin.get("source_id"))
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(LOCAL_SOURCE_ID)
        .to_string();
    let origin_kind = cass_origin
        .and_then(|origin| origin.get("kind"))
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(if source_id == LOCAL_SOURCE_ID {
            "local"
        } else {
            "ssh"
        })
        .to_string();
    let origin_host = cass_origin
        .and_then(|origin| origin.get("host"))
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    (source_id, origin_kind, origin_host)
}

pub(in crate::indexer) fn attach_raw_mirror_metadata(
    conv: &mut NormalizedConversation,
    record: &crate::raw_mirror::RawMirrorCaptureRecord,
) {
    if !conv.metadata.is_object() {
        conv.metadata = serde_json::json!({});
    }
    let Some(metadata) = conv.metadata.as_object_mut() else {
        return;
    };
    let cass = metadata
        .entry("cass".to_string())
        .or_insert_with(|| serde_json::json!({}));
    if !cass.is_object() {
        *cass = serde_json::json!({});
    }
    let Some(cass_obj) = cass.as_object_mut() else {
        return;
    };
    cass_obj.insert(
        "raw_mirror".to_string(),
        serde_json::json!({
            "schema_version": 1,
            "manifest_id": record.manifest_id,
            "manifest_relative_path": record.manifest_relative_path,
            "blob_relative_path": record.blob_relative_path,
            "blob_blake3": record.blob_blake3,
            "blob_size_bytes": record.blob_size_bytes,
            "source_content_blake3": record.source_content_blake3,
            "source_size_bytes": record.source_size_bytes,
            "storage_kind": record.storage_kind,
            "chunk_count": record.chunk_count,
            "captured_at_ms": record.captured_at_ms,
            "source_mtime_ms": record.source_mtime_ms,
        }),
    );
}

/// Apply workspace path rewriting to a conversation.
///
/// This rewrites workspace paths from remote formats to local equivalents
/// at ingest time, ensuring that workspace filters work consistently
/// across local and remote sources.
///
/// The original workspace path is preserved in metadata.cass.workspace_original
/// for display/audit purposes.
///
/// Part of P6.2 - Apply path mappings at ingest time.
pub fn apply_workspace_rewrite(conv: &mut NormalizedConversation, root: &ScanRoot) {
    // Only apply if we have a workspace and rewrites
    if root.workspace_rewrites.is_empty() {
        return;
    }

    // Clone workspace upfront to avoid borrow issues
    let original_workspace = match &conv.workspace {
        Some(ws) => ws.to_string_lossy().to_string(),
        None => return,
    };

    // Use optimized trie lookup via ScanRoot
    let rewritten = root.rewrite_workspace(&original_workspace, Some(&conv.agent_slug));

    // Only proceed if the rewrite actually changed something
    if rewritten != original_workspace {
        // Store original in metadata
        if !conv.metadata.is_object() {
            conv.metadata = serde_json::json!({});
        }

        if let Some(obj) = conv.metadata.as_object_mut() {
            // Get or create cass object
            let cass = obj
                .entry("cass".to_string())
                .or_insert_with(|| serde_json::json!({}));
            if !cass.is_object() {
                *cass = serde_json::json!({});
            }
            if let Some(cass_obj) = cass.as_object_mut() {
                cass_obj.insert(
                    "workspace_original".to_string(),
                    serde_json::Value::String(original_workspace.clone()),
                );
            }
        }

        // Update workspace to rewritten path
        conv.workspace = Some(std::path::PathBuf::from(&rewritten));

        tracing::debug!(
            original = %original_workspace,
            rewritten = %rewritten,
            agent = %conv.agent_slug,
            "workspace_rewritten"
        );
    }
}
