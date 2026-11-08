use crate::doctor::fs_safety::cleanup_apply::{
    collect_diag_quarantine_report, diag_lexical_manifest_scan_root,
};
use crate::doctor::fs_safety::doctor_copy_source_path_is_safe::{
    existing_path_has_symlink_below_root, path_has_symlink_below_root,
};
use crate::doctor::fs_safety::mutation_executor::{file_blake3_hex, sync_directory};
use crate::doctor::lexical_cleanup_disposition_label::system_time_to_unix_ms;
use crate::doctor::policy::{
    DoctorForensicBundleArtifact, DoctorForensicBundleMetadata, DoctorPlan,
};
use crate::doctor::raw_mirror::doctorrawmirrorverificationmode::{
    collect_doctor_raw_mirror_report, doctor_raw_mirror_root,
};
use crate::doctor::repair_state::finalize_cleanup_apply_contract::DiagQuarantineReport;
use crate::doctor::sources::collect_doctor_source_inventory;
use anyhow::Result;
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

pub(crate) fn doctor_now_ms() -> i64 {
    system_time_to_unix_ms(std::time::SystemTime::now()).unwrap_or(0)
}

pub(crate) fn doctor_canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(doctor_canonical_json_value).collect())
        }
        serde_json::Value::Object(map) => {
            let mut entries: Vec<_> = map.into_iter().collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let mut canonical = serde_json::Map::new();
            for (key, value) in entries {
                canonical.insert(key, doctor_canonical_json_value(value));
            }
            serde_json::Value::Object(canonical)
        }
        other => other,
    }
}

pub(crate) fn doctor_canonical_blake3(prefix: &str, value: serde_json::Value) -> String {
    let canonical = doctor_canonical_json_value(value);
    let encoded = serde_json::to_vec(&canonical).unwrap_or_default();
    let mut hasher = blake3::Hasher::new();
    hasher.update(prefix.as_bytes());
    hasher.update(&[0]);
    hasher.update(&encoded);
    format!("{prefix}-{}", hasher.finalize().to_hex())
}

pub(crate) fn doctor_redacted_path(path: &str, data_dir: &Path) -> String {
    fn portable_display(path: &Path) -> String {
        path.display().to_string().replace('\\', "/")
    }

    let path_ref = Path::new(path);
    if let Ok(relative) = path_ref.strip_prefix(data_dir) {
        if relative.as_os_str().is_empty() {
            return "[cass-data]".to_string();
        }
        return format!("[cass-data]/{}", portable_display(relative));
    }
    path_ref
        .file_name()
        .map(|name| format!("[external]/{}", name.to_string_lossy()))
        .unwrap_or_else(|| "[external]".to_string())
}

pub(crate) fn doctor_redacted_text(text: &str, data_dir: &Path) -> String {
    let data_dir_text = data_dir.display().to_string();
    text.replace(&data_dir_text, "[cass-data]")
}

pub(crate) fn doctor_forensic_bundle_uncaptured(status: &str) -> DoctorForensicBundleMetadata {
    DoctorForensicBundleMetadata {
        status: status.to_string(),
        sidecar_complete: true,
        redaction_status: "not_applicable".to_string(),
        ..DoctorForensicBundleMetadata::default()
    }
}

pub(crate) fn doctor_forensic_bundle_failed(
    reason: impl Into<String>,
) -> DoctorForensicBundleMetadata {
    DoctorForensicBundleMetadata {
        status: "failed".to_string(),
        sidecar_complete: false,
        redaction_status: "not_applicable".to_string(),
        blocked_reason: Some(reason.into()),
        ..DoctorForensicBundleMetadata::default()
    }
}

pub(crate) fn doctor_forensic_bundle_root(data_dir: &Path) -> PathBuf {
    data_dir.join("doctor").join("forensic-bundles")
}

pub(crate) fn doctor_forensic_create_private_dir(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;

        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        builder.create(path)?;
        let meta = std::fs::symlink_metadata(path)?;
        if meta.file_type().is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "refusing to use a symlink as a forensic directory",
            ));
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        std::fs::create_dir(path)
    }
}

pub(crate) fn doctor_forensic_create_private_dir_all(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        builder.create(path)?;
        let meta = std::fs::symlink_metadata(path)?;
        if meta.file_type().is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "refusing to use a symlink as a forensic directory",
            ));
        }
        if meta.permissions().mode() & 0o777 != 0o700 {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        std::fs::create_dir_all(path)
    }
}

pub(crate) fn doctor_forensic_create_private_file(path: &Path) -> io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.mode(0o600);
        let file = options.open(path)?;
        Ok(file)
    }

    #[cfg(not(unix))]
    {
        options.open(path)
    }
}

pub(crate) fn doctor_forensic_bundle_id(operation_id: &str, created_at_ms: i64) -> String {
    let mut safe_operation: String = operation_id
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .take(24)
        .collect();
    if safe_operation.is_empty() {
        safe_operation.push_str("operation");
    }
    format!("{created_at_ms}-{safe_operation}")
}

pub(crate) fn doctor_forensic_relative_path_is_safe(relative_path: &Path) -> bool {
    let raw = relative_path.as_os_str().to_string_lossy();
    !raw.is_empty()
        && !raw.contains('\\')
        && !raw.contains(':')
        && !relative_path.is_absolute()
        && relative_path.components().all(|component| match component {
            std::path::Component::Normal(name) => doctor_portable_relative_component_is_safe(name),
            std::path::Component::CurDir => true,
            _ => false,
        })
}

pub(crate) fn doctor_forensic_portable_child_path(prefix: &str, child: &Path) -> Option<PathBuf> {
    if prefix.is_empty() || prefix.contains('\\') || prefix.contains(':') {
        return None;
    }
    let mut relative = String::with_capacity(prefix.len() + 64);
    relative.push_str(prefix);
    for component in child.components() {
        match component {
            std::path::Component::Normal(name)
                if doctor_portable_relative_component_is_safe(name) =>
            {
                relative.push('/');
                relative.push_str(name.to_str()?);
            }
            std::path::Component::CurDir => {}
            _ => return None,
        }
    }
    let relative = PathBuf::from(relative);
    doctor_forensic_relative_path_is_safe(&relative).then_some(relative)
}

pub(crate) fn doctor_portable_relative_component_is_safe(name: &std::ffi::OsStr) -> bool {
    let text = name.to_string_lossy();
    if text.is_empty()
        || text.contains('\\')
        || text.contains(':')
        || text.ends_with(' ')
        || text.ends_with('.')
        || text.chars().any(char::is_control)
    {
        return false;
    }
    let stem = text
        .split('.')
        .next()
        .unwrap_or(text.as_ref())
        .trim_end_matches(' ');
    let upper = stem.to_ascii_uppercase();
    !matches!(
        upper.as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "CONIN$"
            | "CONOUT$"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}

pub(crate) fn doctor_forensic_bundle_root_is_safe(
    data_dir: &Path,
    root: &Path,
) -> Result<(), String> {
    let data_dir_metadata = std::fs::symlink_metadata(data_dir).map_err(|err| {
        format!(
            "cannot inspect data directory {} before forensic bundle capture: {err}",
            data_dir.display()
        )
    })?;
    if !data_dir_metadata.is_dir() || data_dir_metadata.file_type().is_symlink() {
        return Err(format!(
            "refusing forensic bundle capture through unsafe data directory {}",
            data_dir.display()
        ));
    }
    if !root.starts_with(data_dir) {
        return Err(format!(
            "refusing forensic bundle root outside data directory: {}",
            root.display()
        ));
    }
    if let Ok(root_metadata) = std::fs::symlink_metadata(root)
        && (!root_metadata.is_dir() || root_metadata.file_type().is_symlink())
    {
        return Err(format!(
            "refusing unsafe forensic bundle root {}",
            root.display()
        ));
    }
    if let Some(parent) = root.parent()
        && existing_path_has_symlink_below_root(parent, data_dir)
    {
        return Err(format!(
            "refusing forensic bundle root with symlinked ancestor {}",
            root.display()
        ));
    }
    Ok(())
}

pub(crate) fn doctor_sqlite_sidecar_path(db_path: &Path, suffix: &str) -> Option<PathBuf> {
    let mut file_name = db_path.file_name()?.to_os_string();
    file_name.push(suffix);
    Some(db_path.with_file_name(file_name))
}

pub(crate) struct DoctorForensicCopySpec<'a> {
    pub(crate) artifact_kind: &'a str,
    pub(crate) source_path: &'a Path,
    pub(crate) relative_path: &'a Path,
    pub(crate) required: bool,
    pub(crate) sidecar_group: Option<&'a str>,
}

pub(crate) fn doctor_forensic_source_artifact(
    data_dir: &Path,
    spec: &DoctorForensicCopySpec<'_>,
) -> DoctorForensicBundleArtifact {
    let source_path_text = spec.source_path.display().to_string();
    DoctorForensicBundleArtifact {
        artifact_kind: spec.artifact_kind.to_string(),
        source_path: source_path_text.clone(),
        redacted_source_path: doctor_redacted_path(&source_path_text, data_dir),
        bundle_relative_path: Some(spec.relative_path.display().to_string()),
        exists: false,
        copied: false,
        required: spec.required,
        sidecar_group: spec.sidecar_group.map(str::to_string),
        size_bytes: None,
        checksum_blake3: None,
        error: None,
    }
}

pub(crate) fn doctor_forensic_generated_artifact(
    artifact_kind: &str,
    relative_path: &Path,
    size_bytes: u64,
    checksum_blake3: String,
) -> DoctorForensicBundleArtifact {
    DoctorForensicBundleArtifact {
        artifact_kind: artifact_kind.to_string(),
        source_path: format!("generated:{artifact_kind}"),
        redacted_source_path: format!("generated:{artifact_kind}"),
        bundle_relative_path: Some(relative_path.display().to_string()),
        exists: true,
        copied: true,
        required: true,
        sidecar_group: None,
        size_bytes: Some(size_bytes),
        checksum_blake3: Some(checksum_blake3),
        error: None,
    }
}

pub(crate) fn doctor_forensic_record_missing_artifact(
    artifacts: &mut Vec<DoctorForensicBundleArtifact>,
    data_dir: &Path,
    artifact_kind: &str,
    source_path: &Path,
    relative_path: &Path,
    required: bool,
    sidecar_group: Option<&str>,
) {
    artifacts.push(doctor_forensic_source_artifact(
        data_dir,
        &DoctorForensicCopySpec {
            artifact_kind,
            source_path,
            relative_path,
            required,
            sidecar_group,
        },
    ));
}

pub(crate) fn doctor_forensic_copy_file_artifact(
    artifacts: &mut Vec<DoctorForensicBundleArtifact>,
    bundle_dir: &Path,
    data_dir: &Path,
    spec: DoctorForensicCopySpec<'_>,
) -> Result<(), String> {
    if !doctor_forensic_relative_path_is_safe(spec.relative_path) {
        let mut artifact = doctor_forensic_source_artifact(data_dir, &spec);
        artifact.error = Some("unsafe bundle relative path".to_string());
        artifacts.push(artifact);
        return Err(format!(
            "refusing unsafe forensic bundle relative path {}",
            spec.relative_path.display()
        ));
    }

    let mut artifact = doctor_forensic_source_artifact(data_dir, &spec);
    let source_metadata = match std::fs::symlink_metadata(spec.source_path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            if spec.required {
                artifact.error = Some("required source artifact is missing".to_string());
                artifacts.push(artifact);
                return Err(format!(
                    "required forensic source artifact is missing: {}",
                    spec.source_path.display()
                ));
            }
            artifacts.push(artifact);
            return Ok(());
        }
        Err(err) => {
            artifact.error = Some(format!("failed to inspect source artifact: {err}"));
            artifacts.push(artifact);
            return Err(format!(
                "failed to inspect forensic source artifact {}: {err}",
                spec.source_path.display()
            ));
        }
    };

    artifact.exists = true;
    artifact.size_bytes = Some(source_metadata.len());
    if source_metadata.file_type().is_symlink() || !source_metadata.is_file() {
        artifact.error = Some("source artifact is not a regular non-symlink file".to_string());
        artifacts.push(artifact);
        return Err(format!(
            "refusing to copy unsafe forensic source artifact {}",
            spec.source_path.display()
        ));
    }
    if spec.source_path.starts_with(data_dir)
        && path_has_symlink_below_root(spec.source_path, data_dir)
    {
        artifact.error = Some("source artifact has a symlinked ancestor".to_string());
        artifacts.push(artifact);
        return Err(format!(
            "refusing to copy forensic source artifact with symlinked ancestor {}",
            spec.source_path.display()
        ));
    }

    let source_hash = match file_blake3_hex(spec.source_path) {
        Ok(hash) => hash,
        Err(err) => {
            artifact.error = Some(err.clone());
            artifacts.push(artifact);
            return Err(err);
        }
    };

    let target_path = bundle_dir.join(spec.relative_path);
    let Some(target_parent) = target_path.parent() else {
        artifact.error = Some("bundle target has no parent".to_string());
        artifacts.push(artifact);
        return Err(format!(
            "forensic bundle target has no parent: {}",
            target_path.display()
        ));
    };
    doctor_forensic_create_private_dir_all(target_parent).map_err(|err| {
        format!(
            "failed to create forensic bundle parent {}: {err}",
            target_parent.display()
        )
    })?;
    if existing_path_has_symlink_below_root(target_parent, bundle_dir) {
        artifact.error = Some("bundle target parent has symlinked ancestor".to_string());
        artifacts.push(artifact);
        return Err(format!(
            "refusing forensic bundle target with symlinked parent {}",
            target_parent.display()
        ));
    }

    let copy_result = (|| -> io::Result<u64> {
        let mut source = std::fs::File::open(spec.source_path)?;
        let mut target = doctor_forensic_create_private_file(&target_path)?;
        let copied = io::copy(&mut source, &mut target)?;
        use std::io::Write as _;
        target.flush()?;
        target.sync_all()?;
        Ok(copied)
    })();
    let copied_bytes = match copy_result {
        Ok(bytes) => bytes,
        Err(err) => {
            artifact.error = Some(format!("copy failed: {err}"));
            artifacts.push(artifact);
            return Err(format!(
                "failed to copy forensic artifact {} to {}: {err}",
                spec.source_path.display(),
                target_path.display()
            ));
        }
    };
    if copied_bytes != source_metadata.len() {
        artifact.error = Some(format!(
            "copy byte count mismatch: copied {copied_bytes}, expected {}",
            source_metadata.len()
        ));
        artifacts.push(artifact);
        return Err(format!(
            "forensic artifact copy byte count mismatch for {}",
            spec.source_path.display()
        ));
    }
    let target_hash = match file_blake3_hex(&target_path) {
        Ok(hash) => hash,
        Err(err) => {
            artifact.error = Some(err.clone());
            artifacts.push(artifact);
            return Err(err);
        }
    };
    if target_hash != source_hash {
        artifact.error = Some(format!(
            "checksum mismatch after copy: source {source_hash}, target {target_hash}"
        ));
        artifacts.push(artifact);
        return Err(format!(
            "forensic artifact checksum mismatch after copying {}",
            spec.source_path.display()
        ));
    }
    artifact.copied = true;
    artifact.checksum_blake3 = Some(source_hash);
    artifacts.push(artifact);
    Ok(())
}

pub(crate) fn doctor_forensic_write_json_artifact(
    artifacts: &mut Vec<DoctorForensicBundleArtifact>,
    bundle_dir: &Path,
    artifact_kind: &str,
    relative_path: &Path,
    value: &serde_json::Value,
) -> Result<(), String> {
    if !doctor_forensic_relative_path_is_safe(relative_path) {
        return Err(format!(
            "refusing unsafe generated forensic bundle relative path {}",
            relative_path.display()
        ));
    }
    let target_path = bundle_dir.join(relative_path);
    let Some(target_parent) = target_path.parent() else {
        return Err(format!(
            "generated forensic artifact target has no parent: {}",
            target_path.display()
        ));
    };
    doctor_forensic_create_private_dir_all(target_parent).map_err(|err| {
        format!(
            "failed to create generated forensic artifact parent {}: {err}",
            target_parent.display()
        )
    })?;
    if existing_path_has_symlink_below_root(target_parent, bundle_dir) {
        return Err(format!(
            "refusing generated forensic artifact with symlinked parent {}",
            target_parent.display()
        ));
    }
    let encoded = serde_json::to_vec_pretty(value)
        .map_err(|err| format!("failed to encode forensic {artifact_kind}: {err}"))?;
    let mut target = doctor_forensic_create_private_file(&target_path).map_err(|err| {
        format!(
            "failed to create generated forensic artifact {}: {err}",
            target_path.display()
        )
    })?;
    use std::io::Write as _;
    target.write_all(&encoded).map_err(|err| {
        format!(
            "failed to write generated forensic artifact {}: {err}",
            target_path.display()
        )
    })?;
    target.flush().map_err(|err| {
        format!(
            "failed to flush generated forensic artifact {}: {err}",
            target_path.display()
        )
    })?;
    target.sync_all().map_err(|err| {
        format!(
            "failed to sync generated forensic artifact {}: {err}",
            target_path.display()
        )
    })?;
    drop(target);
    let checksum = file_blake3_hex(&target_path)?;
    artifacts.push(doctor_forensic_generated_artifact(
        artifact_kind,
        relative_path,
        encoded.len() as u64,
        checksum,
    ));
    Ok(())
}

pub(crate) fn doctor_forensic_safe_command_args() -> Vec<String> {
    let mut redact_next = false;
    let sensitive_terms = ["password", "passwd", "secret", "token", "apikey", "api-key"];
    std::env::args()
        .map(|arg| {
            if redact_next {
                redact_next = false;
                return "[redacted]".to_string();
            }
            let lower = arg.to_ascii_lowercase();
            let sensitive = sensitive_terms.iter().any(|term| lower.contains(term));
            if sensitive {
                if lower.starts_with("--") && !lower.contains('=') {
                    redact_next = true;
                }
                "[redacted-arg]".to_string()
            } else {
                arg
            }
        })
        .collect()
}

pub(crate) fn doctor_forensic_safe_env_metadata() -> serde_json::Value {
    let safe_env_keys = [
        "CASS_OUTPUT_FORMAT",
        "CASS_LEXICAL_PUBLISH_BACKUP_RETENTION",
        "CASS_NO_COLOR",
        "CASS_RESPECT_NO_COLOR",
        "NO_COLOR",
        "TERM",
        "COLORTERM",
    ];
    let mut captured = BTreeMap::new();
    for key in safe_env_keys {
        if let Ok(value) = dotenvy::var(key) {
            captured.insert(key.to_string(), value);
        }
    }
    serde_json::json!({
        "schema_version": 1,
        "captured_env": captured,
        "command_args": doctor_forensic_safe_command_args(),
        "redaction_policy": "allowlist-only; secret/token/password/key-shaped environment variables are not captured",
        "cass_version": env!("CARGO_PKG_VERSION"),
        "platform": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
        }
    })
}

pub(crate) struct DoctorForensicBundleCaptureInput<'a> {
    pub(crate) operation_id: &'a str,
    pub(crate) data_dir: &'a Path,
    pub(crate) db_path: &'a Path,
    pub(crate) index_path: &'a Path,
    pub(crate) plan: Option<&'a DoctorPlan>,
    pub(crate) quarantine_report: Option<&'a DiagQuarantineReport>,
    pub(crate) extra_file_artifacts: &'a [DoctorForensicCopySpec<'a>],
}

pub(crate) fn capture_doctor_forensic_bundle(
    input: DoctorForensicBundleCaptureInput<'_>,
) -> DoctorForensicBundleMetadata {
    let created_at_ms = doctor_now_ms();
    let root = doctor_forensic_bundle_root(input.data_dir);
    if let Err(err) = doctor_forensic_bundle_root_is_safe(input.data_dir, &root) {
        return doctor_forensic_bundle_failed(err);
    }
    if let Err(err) = doctor_forensic_create_private_dir_all(&root) {
        return doctor_forensic_bundle_failed(format!(
            "failed to create forensic bundle root {}: {err}",
            root.display()
        ));
    }
    if let Err(err) = doctor_forensic_bundle_root_is_safe(input.data_dir, &root) {
        return doctor_forensic_bundle_failed(err);
    }
    if let Err(err) = sync_directory(input.data_dir) {
        return doctor_forensic_bundle_failed(err);
    }

    let base_bundle_id = doctor_forensic_bundle_id(input.operation_id, created_at_ms);
    let mut bundle_dir = root.join(&base_bundle_id);
    let mut bundle_id = base_bundle_id.clone();
    let mut bundle_dir_allocated = false;
    for suffix in 0..100 {
        let candidate_id = if suffix == 0 {
            base_bundle_id.clone()
        } else {
            format!("{base_bundle_id}-{suffix}")
        };
        let candidate_dir = root.join(&candidate_id);
        match doctor_forensic_create_private_dir(&candidate_dir) {
            Ok(()) => {
                bundle_id = candidate_id;
                bundle_dir = candidate_dir;
                bundle_dir_allocated = true;
                break;
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return doctor_forensic_bundle_failed(format!(
                    "failed to create forensic bundle directory {}: {err}",
                    candidate_dir.display()
                ));
            }
        }
    }
    if !bundle_dir_allocated {
        return doctor_forensic_bundle_failed(format!(
            "failed to allocate unique forensic bundle under {}",
            root.display()
        ));
    }

    let mut artifacts = Vec::new();
    let mut first_error: Option<String> = None;
    macro_rules! copy_artifact {
        ($artifact_kind:expr, $source_path:expr, $relative_path:expr, $required:expr, $sidecar_group:expr $(,)?) => {
            if let Err(err) = doctor_forensic_copy_file_artifact(
                &mut artifacts,
                &bundle_dir,
                input.data_dir,
                DoctorForensicCopySpec {
                    artifact_kind: $artifact_kind,
                    source_path: $source_path,
                    relative_path: $relative_path,
                    required: $required,
                    sidecar_group: $sidecar_group,
                },
            ) && first_error.is_none()
            {
                first_error = Some(err);
            }
        };
    }

    copy_artifact!(
        "canonical_archive_db",
        input.db_path,
        Path::new("database/agent_search.db"),
        false,
        Some("sqlite_db"),
    );
    if let Some(wal_path) = doctor_sqlite_sidecar_path(input.db_path, "-wal") {
        copy_artifact!(
            "archive_db_wal_sidecar",
            &wal_path,
            Path::new("database/agent_search.db-wal"),
            false,
            Some("sqlite_db"),
        );
    }
    if let Some(shm_path) = doctor_sqlite_sidecar_path(input.db_path, "-shm") {
        copy_artifact!(
            "archive_db_shm_sidecar",
            &shm_path,
            Path::new("database/agent_search.db-shm"),
            false,
            Some("sqlite_db"),
        );
    }
    for artifact in input.extra_file_artifacts {
        copy_artifact!(
            artifact.artifact_kind,
            artifact.source_path,
            artifact.relative_path,
            artifact.required,
            artifact.sidecar_group,
        );
    }

    for (artifact_kind, path, relative) in [
        (
            "cass_data_config",
            input.data_dir.join("config.toml"),
            PathBuf::from("config/cass-data-config.toml"),
        ),
        (
            "bookmarks_json",
            input.data_dir.join("bookmarks.json"),
            PathBuf::from("bookmarks/bookmarks.json"),
        ),
        (
            "bookmarks_db",
            input.data_dir.join("bookmarks.db"),
            PathBuf::from("bookmarks/bookmarks.db"),
        ),
    ] {
        copy_artifact!(artifact_kind, &path, &relative, false, None);
    }

    let raw_manifest_root = doctor_raw_mirror_root(input.data_dir).join("manifests");
    if raw_manifest_root.exists() {
        for entry in walkdir::WalkDir::new(&raw_manifest_root)
            .follow_links(false)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_file() || entry.file_type().is_symlink())
        {
            let path = entry.path();
            let Ok(relative_to_root) = path.strip_prefix(&raw_manifest_root) else {
                continue;
            };
            let Some(relative) =
                doctor_forensic_portable_child_path("raw-mirror-manifests", relative_to_root)
            else {
                continue;
            };
            copy_artifact!("raw_mirror_manifest", path, &relative, false, None);
        }
    } else {
        doctor_forensic_record_missing_artifact(
            &mut artifacts,
            input.data_dir,
            "raw_mirror_manifest_root",
            &raw_manifest_root,
            Path::new("raw-mirror-manifests"),
            false,
            None,
        );
    }

    let lexical_manifest_root = diag_lexical_manifest_scan_root(input.index_path);
    if lexical_manifest_root.exists() {
        for entry in walkdir::WalkDir::new(&lexical_manifest_root)
            .follow_links(false)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.file_type().is_file()
                    && entry.file_name().to_str()
                        == Some(
                            crate::indexer::lexical_generation::LEXICAL_GENERATION_MANIFEST_FILE,
                        )
            })
        {
            let path = entry.path();
            let Ok(relative_to_root) = path.strip_prefix(&lexical_manifest_root) else {
                continue;
            };
            let Some(relative) =
                doctor_forensic_portable_child_path("index-manifests", relative_to_root)
            else {
                continue;
            };
            copy_artifact!("lexical_generation_manifest", path, &relative, false, None);
        }
    } else {
        doctor_forensic_record_missing_artifact(
            &mut artifacts,
            input.data_dir,
            "lexical_manifest_root",
            &lexical_manifest_root,
            Path::new("index-manifests"),
            false,
            None,
        );
    }

    let source_inventory = collect_doctor_source_inventory(input.data_dir, input.db_path);
    let raw_mirror_report = collect_doctor_raw_mirror_report(input.data_dir);
    let quarantine_report = input
        .quarantine_report
        .cloned()
        .unwrap_or_else(|| collect_diag_quarantine_report(input.data_dir, input.index_path));

    for (artifact_kind, relative_path, value) in [
        (
            "env_metadata",
            PathBuf::from("metadata/env-metadata.json"),
            doctor_forensic_safe_env_metadata(),
        ),
        (
            "source_inventory",
            PathBuf::from("metadata/source-inventory.json"),
            serde_json::to_value(&source_inventory).unwrap_or_else(|_| serde_json::json!({})),
        ),
        (
            "raw_mirror_report",
            PathBuf::from("metadata/raw-mirror-report.json"),
            serde_json::to_value(&raw_mirror_report).unwrap_or_else(|_| serde_json::json!({})),
        ),
        (
            "quarantine_report",
            PathBuf::from("metadata/quarantine-report.json"),
            serde_json::to_value(&quarantine_report).unwrap_or_else(|_| serde_json::json!({})),
        ),
    ] {
        if let Err(err) = doctor_forensic_write_json_artifact(
            &mut artifacts,
            &bundle_dir,
            artifact_kind,
            &relative_path,
            &value,
        ) && first_error.is_none()
        {
            first_error = Some(err);
        }
    }
    if let Some(plan) = input.plan
        && let Err(err) = doctor_forensic_write_json_artifact(
            &mut artifacts,
            &bundle_dir,
            "doctor_plan",
            Path::new("metadata/doctor-plan.json"),
            &serde_json::to_value(plan).unwrap_or_else(|_| serde_json::json!({})),
        )
        && first_error.is_none()
    {
        first_error = Some(err);
    }

    let manifest_relative_path = Path::new("manifest.json");
    let manifest_path = bundle_dir.join(manifest_relative_path);
    let manifest_value = serde_json::json!({
        "schema_version": 1,
        "bundle_kind": "cass_doctor_forensic_bundle_v1",
        "bundle_id": bundle_id,
        "operation_id": input.operation_id,
        "created_at_ms": created_at_ms,
        "cass_version": env!("CARGO_PKG_VERSION"),
        "data_dir": input.data_dir.display().to_string(),
        "redacted_data_dir": doctor_redacted_path(&input.data_dir.display().to_string(), input.data_dir),
        "db_path": input.db_path.display().to_string(),
        "redacted_db_path": doctor_redacted_path(&input.db_path.display().to_string(), input.data_dir),
        "index_path": input.index_path.display().to_string(),
        "redacted_index_path": doctor_redacted_path(&input.index_path.display().to_string(), input.data_dir),
        "artifacts": artifacts.clone(),
        "redaction_status": "local_forensic_bundle_contains_exact_paths; robot metadata includes redacted path companions",
    });
    if let Err(err) = doctor_forensic_write_json_artifact(
        &mut artifacts,
        &bundle_dir,
        "forensic_bundle_manifest",
        manifest_relative_path,
        &manifest_value,
    ) && first_error.is_none()
    {
        first_error = Some(err);
    }
    if let Err(err) = sync_directory(&bundle_dir)
        && first_error.is_none()
    {
        first_error = Some(err);
    }
    if let Err(err) = sync_directory(&root)
        && first_error.is_none()
    {
        first_error = Some(err);
    }

    let manifest_checksum = file_blake3_hex(&manifest_path).ok();
    let artifact_count = artifacts.len();
    let copied_artifact_count = artifacts.iter().filter(|artifact| artifact.copied).count();
    let missing_artifact_count = artifacts.iter().filter(|artifact| !artifact.exists).count();
    let failed_artifact_count = artifacts
        .iter()
        .filter(|artifact| artifact.error.is_some())
        .count();
    let sidecar_complete = artifacts
        .iter()
        .filter(|artifact| artifact.sidecar_group.is_some() && artifact.exists)
        .all(|artifact| artifact.copied && artifact.error.is_none());
    let blocked_reason = first_error.or_else(|| {
        (failed_artifact_count > 0)
            .then(|| "one or more forensic bundle artifacts failed to copy or verify".to_string())
    });
    DoctorForensicBundleMetadata {
        bundle_id: Some(bundle_id),
        path: Some(bundle_dir.display().to_string()),
        redacted_path: Some(doctor_redacted_path(
            &bundle_dir.display().to_string(),
            input.data_dir,
        )),
        manifest_path: Some(manifest_path.display().to_string()),
        redacted_manifest_path: Some(doctor_redacted_path(
            &manifest_path.display().to_string(),
            input.data_dir,
        )),
        checksum_blake3: manifest_checksum,
        status: if blocked_reason.is_some() {
            "failed".to_string()
        } else {
            "captured".to_string()
        },
        created_at_ms: Some(created_at_ms),
        artifact_count,
        copied_artifact_count,
        missing_artifact_count,
        failed_artifact_count,
        sidecar_complete,
        redaction_status:
            "local_forensic_bundle_contains_exact_paths; robot metadata includes redacted path companions"
                .to_string(),
        blocked_reason,
        artifacts,
    }
}
