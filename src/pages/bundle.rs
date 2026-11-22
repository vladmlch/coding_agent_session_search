//! Bundle builder for pages export.
//!
//! Creates the local static site bundle (site/) and private offline artifacts (private/)
//! from an export. Output is a self-contained archive opened locally or via
//! `cass pages --preview` (loopback only).

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
#[cfg(test)]
use std::io::BufWriter;
use std::io::{BufReader, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use super::docs::{DocLocation, GeneratedDoc};

/// Unencrypted bundle configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnencryptedConfig {
    /// Whether the bundle is encrypted (must be false).
    #[serde(
        serialize_with = "serialize_unencrypted_false",
        deserialize_with = "deserialize_unencrypted_false"
    )]
    pub encrypted: bool,
    /// Config version.
    pub version: String,
    /// Payload descriptor.
    pub payload: UnencryptedPayload,
    /// Optional warning message for viewers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

/// Unencrypted payload descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnencryptedPayload {
    /// Relative path to the SQLite database payload.
    pub path: String,
    /// Payload format (e.g., "sqlite").
    pub format: String,
    /// Optional byte size of the payload.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}

fn serialize_unencrypted_false<S>(encrypted: &bool, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if *encrypted {
        return Err(serde::ser::Error::custom(
            "unencrypted config must set encrypted=false",
        ));
    }
    serializer.serialize_bool(false)
}

fn deserialize_unencrypted_false<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let encrypted = bool::deserialize(deserializer)?;
    if encrypted {
        return Err(serde::de::Error::custom(
            "unencrypted config must set encrypted=false",
        ));
    }
    Ok(false)
}

/// Files embedded from pages_assets at compile time
const PAGES_ASSETS: &[(&str, &[u8])] = &[
    ("index.html", include_bytes!("../pages_assets/index.html")),
    ("styles.css", include_bytes!("../pages_assets/styles.css")),
    ("auth.js", include_bytes!("../pages_assets/auth.js")),
    ("viewer.js", include_bytes!("../pages_assets/viewer.js")),
    ("router.js", include_bytes!("../pages_assets/router.js")),
    ("share.js", include_bytes!("../pages_assets/share.js")),
    ("stats.js", include_bytes!("../pages_assets/stats.js")),
    ("storage.js", include_bytes!("../pages_assets/storage.js")),
    ("search.js", include_bytes!("../pages_assets/search.js")),
    (
        "conversation.js",
        include_bytes!("../pages_assets/conversation.js"),
    ),
    ("database.js", include_bytes!("../pages_assets/database.js")),
    ("session.js", include_bytes!("../pages_assets/session.js")),
    ("sw.js", include_bytes!("../pages_assets/sw.js")),
    (
        "sw-register.js",
        include_bytes!("../pages_assets/sw-register.js"),
    ),
    (
        "virtual-list.js",
        include_bytes!("../pages_assets/virtual-list.js"),
    ),
    (
        "coi-detector.js",
        include_bytes!("../pages_assets/coi-detector.js"),
    ),
    (
        "attachments.js",
        include_bytes!("../pages_assets/attachments.js"),
    ),
    ("settings.js", include_bytes!("../pages_assets/settings.js")),
];

/// Immutable third-party runtime files embedded in every Pages bundle.
///
/// These are exact bytes from the upstream releases recorded in
/// `pages_assets/vendor/manifest.json`. Keep the byte include, size, SHA-256,
/// manifest record, and license in lockstep when updating a dependency.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PinnedVendorAsset {
    pub path: &'static str,
    pub bytes: &'static [u8],
    pub size: u64,
    pub sha256: &'static str,
}

macro_rules! pinned_vendor_asset {
    ($path:literal, $size:literal, $sha256:literal) => {
        PinnedVendorAsset {
            path: concat!("vendor/", $path),
            bytes: include_bytes!(concat!("../pages_assets/vendor/", $path)),
            size: $size,
            sha256: $sha256,
        }
    };
}

pub(crate) const PAGES_VENDOR_ASSETS: &[PinnedVendorAsset] = &[
    pinned_vendor_asset!(
        "sqlite3.mjs",
        578_559,
        "f80870f0fa03a39a3338d17ed3fbea04808d344c88e724d90d5f37b9b7b83154"
    ),
    pinned_vendor_asset!(
        "sqlite3.wasm",
        864_752,
        "02d7e48164395fa68f81c6ec33e9da5461be397dc57602ac0cd89b4bbba1d312"
    ),
    pinned_vendor_asset!(
        "sqlite3-opfs-async-proxy.js",
        32_289,
        "502762db7bd24130f345df6a42883166ec0a91815207940328c9739bb0f5ec2f"
    ),
    pinned_vendor_asset!(
        "LICENSE-sqlite-wasm.txt",
        11_358,
        "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30"
    ),
    pinned_vendor_asset!(
        "manifest.json",
        1_776,
        "09f68c8e09427ecb21b23032aaa744aca0286aed67d2981b45a66aee8e49561a"
    ),
];

const BUNDLE_PUBLISH_JOURNAL_FORMAT: &str = "cass-pages-bundle-publish-v1";
const BUNDLE_PUBLISH_JOURNAL_MAX_BYTES: u64 = 16 * 1024;
const BUNDLE_PUBLISH_BACKUP_SCAN_LIMIT: usize = 65_536;
const BUNDLE_TREE_EVIDENCE_ENTRY_LIMIT: usize = 1_000_000;
const BUNDLE_TREE_EVIDENCE_DOMAIN: &[u8] = b"cass-pages-bundle-tree-v1\0";

#[derive(Debug, Clone, PartialEq, Eq)]
struct BundleTreeEvidence {
    file_count: u64,
    total_size_bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundlePublishJournal {
    format: String,
    backup_file_name: String,
    prior_file_count: u64,
    prior_size_bytes: u64,
    prior_sha256: String,
    candidate_file_count: u64,
    candidate_size_bytes: u64,
    candidate_sha256: String,
}

#[derive(Debug)]
struct BundlePublishGuard {
    final_dir: PathBuf,
    lock_path: PathBuf,
    lock_identity: crate::franken_sync::FileIdentity,
    lock_file: File,
}

/// Integrity entry for a single file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityEntry {
    /// SHA256 hash as hex string
    pub sha256: String,
    /// File size in bytes
    pub size: u64,
}

/// Full integrity manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityManifest {
    /// Schema version for integrity format
    pub version: u8,
    /// Generated timestamp
    pub generated_at: String,
    /// Map of relative path -> integrity entry
    pub files: BTreeMap<String, IntegrityEntry>,
}

/// Site metadata for public config
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteMetadata {
    pub title: String,
    pub description: String,
    pub generated_at: String,
    pub generator: String,
    pub generator_version: String,
}

/// Bundle configuration
#[derive(Debug, Clone)]
pub struct BundleConfig {
    /// Archive title
    pub title: String,
    /// Archive description
    pub description: String,
    /// Whether to obfuscate metadata (workspace paths etc)
    pub hide_metadata: bool,
    /// Additional generated documentation files to include
    pub generated_docs: Vec<GeneratedDoc>,
    /// Exact `cass analytics status --json` data projection for the source
    /// archive. Pages consumes this instead of inventing separate readiness.
    pub analytics_status: Option<serde_json::Value>,
}

impl Default for BundleConfig {
    fn default() -> Self {
        Self {
            title: "cass Archive".to_string(),
            description: "Searchable archive of AI coding agent conversations".to_string(),
            hide_metadata: false,
            generated_docs: Vec::new(),
            analytics_status: None,
        }
    }
}

/// Bundle builder for creating static site exports
#[derive(Default)]
pub struct BundleBuilder {
    config: BundleConfig,
}

impl BundleBuilder {
    /// Create a new bundle builder with default config
    pub fn new() -> Self {
        Self {
            config: BundleConfig::default(),
        }
    }

    /// Create a bundle builder with specific config
    pub fn with_config(config: BundleConfig) -> Self {
        Self { config }
    }

    /// Set the archive title
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.config.title = title.into();
        self
    }

    /// Set the archive description
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.config.description = description.into();
        self
    }

    /// Set metadata hiding option
    pub fn hide_metadata(mut self, hide: bool) -> Self {
        self.config.hide_metadata = hide;
        self
    }

    /// Add generated documentation files to include in the bundle
    pub fn with_docs(mut self, docs: Vec<GeneratedDoc>) -> Self {
        self.config.generated_docs = docs;
        self
    }

    /// Build the bundle from staged export output
    ///
    /// # Arguments
    /// * `source_dir` - Directory containing export output (`config.json`, `payload/`)
    /// * `output_dir` - Directory to write the bundle (will create `site/` and `private/` subdirs)
    /// * `progress` - Progress callback (phase, message)
    pub fn build<P: AsRef<Path>>(
        &self,
        source_dir: P,
        output_dir: P,
        progress: impl Fn(&str, &str),
    ) -> Result<BundleResult> {
        let source_dir = source_dir.as_ref();
        let output_dir = output_dir.as_ref();

        // Heal an interrupted fallback publish before doing potentially long
        // archive validation/copying work, so a parked prior generation is
        // returned to the live handle as soon as the next build starts.
        // 70o8f: recovery renames/removes entries at the output pathname, so
        // it must hold the same per-output publish lock the final
        // replace_dir_from_temp takes — otherwise two concurrent builders
        // could both run recovery against one output. The guard is taken
        // only when there is recovery EVIDENCE (a live output, a publish
        // journal, or a parked backup): acquiring it unconditionally would
        // leave the lock file behind on rejected builds that never touch a
        // pristine output location. A publisher racing in between is safe —
        // replace_dir_from_temp re-runs recovery under its own guard.
        let recovery_evidence = bundle_path_entry_exists(output_dir)?
            || bundle_path_entry_exists(&bundle_publish_recovery_journal_path(output_dir))?
            || first_unowned_bundle_publish_backup(output_dir, None)?.is_some();
        if recovery_evidence {
            let early_guard = acquire_bundle_publish_guard(output_dir)?;
            require_bundle_publish_guard(output_dir, &early_guard)?;
            recover_interrupted_bundle_publish(output_dir)?;
            ensure_replaceable_bundle_output_dir(output_dir)?;
        }

        // Validate source_dir has required files
        let config_path = source_dir.join("config.json");
        let payload_dir = source_dir.join("payload");

        if !config_path.exists() {
            bail!("Missing config.json in export directory");
        }
        if !payload_dir.exists() {
            bail!("Missing payload/ directory in export directory");
        }

        // Load unencrypted archive config
        let unenc_config: UnencryptedConfig = {
            let file = File::open(&config_path).context("Failed to open config.json")?;
            serde_json::from_reader(BufReader::new(file))
                .context("Unsupported or invalid archive config.json (only unencrypted archives are supported)")?
        };

        let temp_output_dir = unique_bundle_dir(output_dir, "tmp")?;
        let final_site_dir = output_dir.join("site");
        let final_private_dir = output_dir.join("private");
        let mut retain_temp_on_replace_error = false;
        let result = (|| -> Result<BundleResult> {
            progress("setup", "Creating directory structure...");

            // Stage the bundle under a unique temp root so reruns do not retain stale files.
            create_bundle_staging_root(&temp_output_dir)?;
            let site_dir = temp_output_dir.join("site");
            let private_dir = temp_output_dir.join("private");

            fs::create_dir_all(&site_dir).context("Failed to create site/ directory")?;
            ensure_private_artifact_dir(&private_dir)?;

            // Create site subdirectories
            let site_payload_dir = site_dir.join("payload");
            fs::create_dir_all(&site_payload_dir).context("Failed to create site/payload/")?;

            progress("assets", "Copying web assets...");

            // Copy embedded assets to site/
            for (name, content) in PAGES_ASSETS {
                let dest_path = site_dir.join(name);
                fs::write(&dest_path, content)
                    .with_context(|| format!("Failed to write {}", name))?;
            }
            write_pinned_vendor_assets(&site_dir)?;

            if let Some(status) = &self.config.analytics_status {
                let status_json = serde_json::to_vec_pretty(status)
                    .context("Failed to serialize analytics_status.json")?;
                crate::pages::write_file_durably(
                    &site_dir.join("analytics_status.json"),
                    &status_json,
                )
                .context("Failed to write analytics_status.json")?;
            }

            // Copy payload into site/payload/
            progress("payload", "Copying unencrypted payload...");
            let chunk_count = copy_payload_file(source_dir, &site_dir, &unenc_config)?;

            // Attachment bundling is not supported
            let attachment_count = 0;

            progress("config", "Writing configuration files...");

            // Write config.json to site/
            let site_config_path = site_dir.join("config.json");
            let config_json = serde_json::to_vec_pretty(&unenc_config)
                .context("Failed to serialize public Pages config")?;
            crate::pages::write_file_durably(&site_config_path, &config_json)
                .context("Failed to write public Pages config")?;

            // Write site metadata
            let site_metadata = SiteMetadata {
                title: self.config.title.clone(),
                description: self.config.description.clone(),
                generated_at: Utc::now().to_rfc3339(),
                generator: "cass".to_string(),
                generator_version: env!("CARGO_PKG_VERSION").to_string(),
            };
            let site_json_path = site_dir.join("site.json");
            let site_json = serde_json::to_vec_pretty(&site_metadata)
                .context("Failed to serialize Pages site metadata")?;
            crate::pages::write_file_durably(&site_json_path, &site_json)
                .context("Failed to write Pages site metadata")?;

            progress("static", "Writing static files...");

            // Write robots.txt
            let robots_content = "User-agent: *\nDisallow: /\n";
            fs::write(site_dir.join("robots.txt"), robots_content)?;

            // Write .nojekyll (empty file to disable Jekyll processing)
            fs::write(site_dir.join(".nojekyll"), "")?;

            // Write generated documentation if provided, otherwise fallback to basic readme
            if !self.config.generated_docs.is_empty() {
                progress("docs", "Writing generated documentation...");
                for doc in &self.config.generated_docs {
                    let dest_path = resolve_generated_doc_path(&site_dir, doc)?;
                    fs::write(&dest_path, &doc.content)
                        .with_context(|| format!("Failed to write {}", doc.filename))?;
                }
            } else {
                // Fallback to basic README.md
                let public_readme =
                    generate_public_readme(&self.config.title, &self.config.description);
                fs::write(site_dir.join("README.md"), public_readme)?;
            }

            validate_pinned_vendor_assets(&site_dir)
                .context("Materialized Pages vendor runtime failed exact-byte validation")?;

            progress("integrity", "Generating integrity manifest...");

            // Generate integrity.json for all files in site/
            let integrity_manifest = generate_integrity_manifest(&site_dir)?;
            let integrity_path = site_dir.join("integrity.json");
            let integrity_json = serde_json::to_vec_pretty(&integrity_manifest)
                .context("Failed to serialize Pages integrity manifest")?;
            crate::pages::write_file_durably(&integrity_path, &integrity_json)
                .context("Failed to write Pages integrity manifest")?;

            // Compute integrity fingerprint (short hash for visual verification)
            let fingerprint = compute_fingerprint(&integrity_manifest);

            progress("private", "Writing private artifacts...");

            // Write private artifacts
            write_private_fingerprint(&private_dir, &fingerprint)?;
            write_private_unencrypted_notice(&private_dir)?;

            prepare_bundle_root_for_publish(&temp_output_dir)?;
            sync_tree(&temp_output_dir)?;
            replace_dir_from_temp(
                &temp_output_dir,
                output_dir,
                &mut retain_temp_on_replace_error,
            )
            .context("Failed to install completed bundle")?;

            progress("complete", "Bundle complete!");

            Ok(BundleResult {
                site_dir: final_site_dir,
                private_dir: final_private_dir,
                chunk_count,
                attachment_count,
                fingerprint,
                total_files: integrity_manifest.files.len(),
            })
        })();

        match result {
            Err(build_error) if !retain_temp_on_replace_error => {
                match cleanup_rejected_bundle_temp(&temp_output_dir) {
                    Ok(()) => Err(build_error),
                    Err(cleanup_error) => Err(build_error.context(format!(
                        "failed to remove rejected staged bundle: {cleanup_error:#}"
                    ))),
                }
            }
            other => other,
        }
    }
}

fn create_bundle_staging_root(path: &Path) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed creating bundle staging parent {}", parent.display())
        })?;
    }

    #[cfg(unix)]
    {
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700);
        builder.create(path).with_context(|| {
            format!(
                "failed creating private bundle staging root {}",
                path.display()
            )
        })?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("failed securing bundle staging root {}", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        fs::create_dir(path)
            .with_context(|| format!("failed creating bundle staging root {}", path.display()))?;
    }
    Ok(())
}

fn prepare_bundle_root_for_publish(path: &Path) -> Result<()> {
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).with_context(|| {
        format!(
            "failed setting bundle root permissions on {}",
            path.display()
        )
    })?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn cleanup_rejected_bundle_temp(path: &Path) -> Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("failed removing staged bundle {}", path.display()))
        }
    }
}

fn unique_bundle_dir(path: &Path, suffix: &str) -> Result<PathBuf> {
    unique_bundle_sidecar_path(path, suffix, "pages_bundle")
}

fn legacy_bundle_publish_in_progress_backup_path(path: &Path) -> PathBuf {
    let mut backup_name = std::ffi::OsString::from(".");
    backup_name.push(
        path.file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("pages_bundle")),
    );
    backup_name.push(".publish-in-progress.bak");
    path.with_file_name(backup_name)
}

fn bundle_publish_recovery_journal_path(path: &Path) -> PathBuf {
    let mut journal_name = std::ffi::OsString::from(".");
    journal_name.push(
        path.file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("pages_bundle")),
    );
    journal_name.push(".pages-bundle-publish-in-progress.json");
    path.with_file_name(journal_name)
}

fn bundle_publish_lock_path(path: &Path) -> PathBuf {
    let mut lock_name = std::ffi::OsString::from(".");
    lock_name.push(
        path.file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("pages_bundle")),
    );
    lock_name.push(".pages-bundle-publish.lock");
    path.with_file_name(lock_name)
}

fn acquire_bundle_publish_guard(final_dir: &Path) -> Result<BundlePublishGuard> {
    ensure_existing_parent_ancestors_are_real_dirs(final_dir, "bundle publish lock")?;
    if let Some(parent) = final_dir
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed creating Pages bundle publish-lock parent {}",
                parent.display()
            )
        })?;
    }
    let lock_path = bundle_publish_lock_path(final_dir);
    let lock_exists = match fs::symlink_metadata(&lock_path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            #[cfg(unix)]
            if metadata.nlink() != 1 {
                bail!(
                    "Pages bundle publish lock {} has {} hard links; exclusive pathname ownership is not provable",
                    lock_path.display(),
                    metadata.nlink()
                );
            }
            true
        }
        Ok(_) => bail!(
            "Pages bundle publish lock is not a regular file: {}",
            lock_path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed inspecting Pages bundle publish lock {}",
                    lock_path.display()
                )
            });
        }
    };
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    if lock_exists {
        options.create(false);
    } else {
        options.create_new(true);
    }
    #[cfg(unix)]
    options.mode(0o600);
    let lock_file = options.open(&lock_path).with_context(|| {
        format!(
            "failed opening Pages bundle publish lock {}",
            lock_path.display()
        )
    })?;
    let lock_identity = crate::franken_sync::FileIdentity::from_file(&lock_file)
        .with_context(|| {
            format!(
                "failed identifying Pages bundle publish lock {}",
                lock_path.display()
            )
        })?
        .ok_or_else(|| {
            anyhow!(
                "filesystem does not expose a stable identity for Pages bundle publish lock {}",
                lock_path.display()
            )
        })?;
    // std's try_lock reports contention as WouldBlock on every platform; fs2
    // surfaced Windows contention as raw ERROR_LOCK_VIOLATION (2l1b0.74).
    match lock_file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => bail!(
            "another Pages bundle publication is already active for {}; lock contention at {}",
            final_dir.display(),
            lock_path.display()
        ),
        Err(std::fs::TryLockError::Error(error)) => {
            return Err(error).with_context(|| {
                format!(
                    "failed acquiring Pages bundle publish lock {}",
                    lock_path.display()
                )
            });
        }
    }
    let guard = BundlePublishGuard {
        final_dir: final_dir.to_path_buf(),
        lock_path,
        lock_identity,
        lock_file,
    };
    require_bundle_publish_guard(final_dir, &guard)?;
    Ok(guard)
}

fn require_bundle_publish_guard(final_dir: &Path, guard: &BundlePublishGuard) -> Result<()> {
    if guard.final_dir != final_dir {
        bail!(
            "Pages bundle publish guard for {} cannot authorize mutation of {}",
            guard.final_dir.display(),
            final_dir.display()
        );
    }
    let held_identity = crate::franken_sync::FileIdentity::from_file(&guard.lock_file)
        .context("failed re-identifying held Pages bundle publish lock")?
        .ok_or_else(|| {
            anyhow!(
                "filesystem stopped exposing a stable identity for held Pages bundle publish lock {}",
                guard.lock_path.display()
            )
        })?;
    if held_identity != guard.lock_identity {
        bail!(
            "held Pages bundle publish lock {} changed identity",
            guard.lock_path.display()
        );
    }
    let metadata = fs::symlink_metadata(&guard.lock_path).with_context(|| {
        format!(
            "failed re-inspecting Pages bundle publish lock {}",
            guard.lock_path.display()
        )
    })?;
    if !metadata.file_type().is_file() {
        bail!(
            "Pages bundle publish lock {} is no longer a regular file",
            guard.lock_path.display()
        );
    }
    #[cfg(unix)]
    if metadata.nlink() != 1 {
        bail!(
            "Pages bundle publish lock {} has {} hard links; refused publication",
            guard.lock_path.display(),
            metadata.nlink()
        );
    }
    let probe = File::open(&guard.lock_path).with_context(|| {
        format!(
            "failed re-opening Pages bundle publish lock {}",
            guard.lock_path.display()
        )
    })?;
    let path_identity = crate::franken_sync::FileIdentity::from_file(&probe)
        .context("failed re-identifying Pages bundle publish lock pathname")?
        .ok_or_else(|| {
            anyhow!(
                "filesystem stopped exposing a stable pathname identity for Pages bundle publish lock {}",
                guard.lock_path.display()
            )
        })?;
    if path_identity != guard.lock_identity {
        bail!(
            "Pages bundle publish lock pathname changed identity: {}",
            guard.lock_path.display()
        );
    }
    Ok(())
}

fn bundle_publish_backup_prefix(path: &Path) -> Result<String> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            anyhow!(
                "Pages bundle output has no UTF-8 file name: {}",
                path.display()
            )
        })?;
    Ok(format!(".{file_name}.publish-backup."))
}

fn unpredictable_bundle_publish_backup_path(path: &Path) -> Result<PathBuf> {
    let prefix = bundle_publish_backup_prefix(path)?;
    let nonce = bundle_sidecar_random_nonce()?;
    Ok(path.with_file_name(format!("{prefix}{nonce:032x}")))
}

fn bundle_path_entry_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error)
            .with_context(|| format!("failed inspecting bundle path entry {}", path.display())),
    }
}

fn journal_bundle_backup_path(final_dir: &Path, journal: &BundlePublishJournal) -> Result<PathBuf> {
    let backup_name_path = Path::new(&journal.backup_file_name);
    if backup_name_path.file_name() != Some(backup_name_path.as_os_str())
        || backup_name_path.components().count() != 1
    {
        bail!(
            "Pages bundle publish journal for {} contains a non-local backup name",
            final_dir.display()
        );
    }
    let prefix = bundle_publish_backup_prefix(final_dir)?;
    let Some(nonce) = journal.backup_file_name.strip_prefix(&prefix) else {
        bail!(
            "Pages bundle publish journal for {} contains an unexpected backup name {}",
            final_dir.display(),
            journal.backup_file_name
        );
    };
    if nonce.len() != 32
        || !nonce
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!(
            "Pages bundle publish journal for {} contains an invalid backup nonce",
            final_dir.display()
        );
    }
    Ok(final_dir.with_file_name(&journal.backup_file_name))
}

fn validate_bundle_publish_journal(
    final_dir: &Path,
    journal: &BundlePublishJournal,
) -> Result<PathBuf> {
    if journal.format != BUNDLE_PUBLISH_JOURNAL_FORMAT {
        bail!(
            "Pages bundle publish journal for {} has unsupported format {}",
            final_dir.display(),
            journal.format
        );
    }
    for (label, digest) in [
        ("prior", journal.prior_sha256.as_str()),
        ("candidate", journal.candidate_sha256.as_str()),
    ] {
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            bail!(
                "Pages bundle publish journal for {} has an invalid {label} SHA-256 digest",
                final_dir.display()
            );
        }
    }
    journal_bundle_backup_path(final_dir, journal)
}

fn read_bundle_publish_journal(final_dir: &Path) -> Result<Option<BundlePublishJournal>> {
    let journal_path = bundle_publish_recovery_journal_path(final_dir);
    let metadata = match fs::symlink_metadata(&journal_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed inspecting Pages bundle publish journal {}",
                    journal_path.display()
                )
            });
        }
    };
    if !metadata.file_type().is_file() {
        bail!(
            "Pages bundle publish journal is not a regular file; preserved it without mutation: {}",
            journal_path.display()
        );
    }
    #[cfg(unix)]
    if metadata.nlink() != 1 {
        bail!(
            "Pages bundle publish journal {} has {} hard links; preserved it because exclusive pathname ownership is not provable",
            journal_path.display(),
            metadata.nlink()
        );
    }
    if metadata.len() > BUNDLE_PUBLISH_JOURNAL_MAX_BYTES {
        bail!(
            "Pages bundle publish journal exceeds the {}-byte bound; preserved it without mutation: {}",
            BUNDLE_PUBLISH_JOURNAL_MAX_BYTES,
            journal_path.display()
        );
    }

    let mut file = File::open(&journal_path).with_context(|| {
        format!(
            "failed opening Pages bundle publish journal {}",
            journal_path.display()
        )
    })?;
    let opened_identity = crate::franken_sync::FileIdentity::from_file(&file)
        .with_context(|| {
            format!(
                "failed identifying Pages bundle publish journal {}",
                journal_path.display()
            )
        })?
        .ok_or_else(|| {
            anyhow!(
                "filesystem does not expose a stable identity for Pages bundle publish journal {}",
                journal_path.display()
            )
        })?;
    let mut bytes = Vec::new();
    (&mut file)
        .take(BUNDLE_PUBLISH_JOURNAL_MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .with_context(|| {
            format!(
                "failed reading Pages bundle publish journal {}",
                journal_path.display()
            )
        })?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > BUNDLE_PUBLISH_JOURNAL_MAX_BYTES
        || u64::try_from(bytes.len()).unwrap_or(u64::MAX) != metadata.len()
    {
        bail!(
            "Pages bundle publish journal changed size while read; preserved it without mutation: {}",
            journal_path.display()
        );
    }
    let path_probe = File::open(&journal_path).with_context(|| {
        format!(
            "failed re-opening Pages bundle publish journal {}",
            journal_path.display()
        )
    })?;
    let path_identity = crate::franken_sync::FileIdentity::from_file(&path_probe)
        .with_context(|| {
            format!(
                "failed re-identifying Pages bundle publish journal {}",
                journal_path.display()
            )
        })?
        .ok_or_else(|| {
            anyhow!(
                "filesystem stopped exposing a stable identity for Pages bundle publish journal {}",
                journal_path.display()
            )
        })?;
    if path_identity != opened_identity {
        bail!(
            "Pages bundle publish journal changed identity while read; preserved it without mutation: {}",
            journal_path.display()
        );
    }
    let observed_metadata = fs::symlink_metadata(&journal_path).with_context(|| {
        format!(
            "failed re-inspecting Pages bundle publish journal {}",
            journal_path.display()
        )
    })?;
    if !observed_metadata.file_type().is_file() {
        bail!(
            "Pages bundle publish journal changed file type while read; preserved it without mutation: {}",
            journal_path.display()
        );
    }
    #[cfg(unix)]
    if observed_metadata.nlink() != 1 {
        bail!(
            "Pages bundle publish journal {} gained additional hard links while read; preserved it without mutation",
            journal_path.display()
        );
    }

    let journal: BundlePublishJournal = serde_json::from_slice(&bytes).with_context(|| {
        format!(
            "Pages bundle publish journal is invalid and was preserved without mutation: {}",
            journal_path.display()
        )
    })?;
    validate_bundle_publish_journal(final_dir, &journal)?;
    Ok(Some(journal))
}

fn write_bundle_publish_journal(
    final_dir: &Path,
    backup_dir: &Path,
    prior: &BundleTreeEvidence,
    candidate: &BundleTreeEvidence,
) -> Result<BundlePublishJournal> {
    let backup_file_name = backup_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            anyhow!(
                "Pages bundle publish backup has no UTF-8 file name: {}",
                backup_dir.display()
            )
        })?
        .to_string();
    let journal = BundlePublishJournal {
        format: BUNDLE_PUBLISH_JOURNAL_FORMAT.to_string(),
        backup_file_name,
        prior_file_count: prior.file_count,
        prior_size_bytes: prior.total_size_bytes,
        prior_sha256: prior.sha256.clone(),
        candidate_file_count: candidate.file_count,
        candidate_size_bytes: candidate.total_size_bytes,
        candidate_sha256: candidate.sha256.clone(),
    };
    validate_bundle_publish_journal(final_dir, &journal)?;
    let bytes =
        serde_json::to_vec(&journal).context("failed serializing Pages bundle publish journal")?;
    let journal_path = bundle_publish_recovery_journal_path(final_dir);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(&journal_path).with_context(|| {
        format!(
            "failed exclusively creating Pages bundle publish journal {}; preserved the live and staged generations",
            journal_path.display()
        )
    })?;
    let created_identity = crate::franken_sync::FileIdentity::from_file(&file)
        .with_context(|| {
            format!(
                "failed identifying newly created Pages bundle publish journal {}",
                journal_path.display()
            )
        })?
        .ok_or_else(|| {
            anyhow!(
                "filesystem does not expose a stable identity for newly created Pages bundle publish journal {}",
                journal_path.display()
            )
        })?;
    file.write_all(&bytes).with_context(|| {
        format!(
            "failed writing Pages bundle publish journal {}; preserved it for inspection",
            journal_path.display()
        )
    })?;
    file.sync_all().with_context(|| {
        format!(
            "failed syncing Pages bundle publish journal {}; preserved it for inspection",
            journal_path.display()
        )
    })?;
    sync_parent_directory(&journal_path).with_context(|| {
        format!(
            "failed durably publishing Pages bundle journal {}; preserved it for recovery",
            journal_path.display()
        )
    })?;
    let path_probe = File::open(&journal_path).with_context(|| {
        format!(
            "failed re-opening newly created Pages bundle publish journal {}",
            journal_path.display()
        )
    })?;
    let path_identity = crate::franken_sync::FileIdentity::from_file(&path_probe)
        .context("failed re-identifying newly created Pages bundle publish journal")?
        .ok_or_else(|| {
            anyhow!(
                "filesystem stopped exposing a stable identity for newly created Pages bundle publish journal {}",
                journal_path.display()
            )
        })?;
    if path_identity != created_identity {
        bail!(
            "Pages bundle publish journal {} changed identity while it was created; live and staged generations were preserved",
            journal_path.display()
        );
    }
    let observed = read_bundle_publish_journal(final_dir)?.ok_or_else(|| {
        anyhow!(
            "newly created Pages bundle publish journal disappeared before publication: {}",
            journal_path.display()
        )
    })?;
    if observed != journal {
        bail!(
            "Pages bundle publish journal {} changed content before publication; live and staged generations were preserved",
            journal_path.display()
        );
    }
    Ok(journal)
}

fn remove_bundle_publish_journal(final_dir: &Path, expected: &BundlePublishJournal) -> Result<()> {
    let journal_path = bundle_publish_recovery_journal_path(final_dir);
    let observed = read_bundle_publish_journal(final_dir)?.ok_or_else(|| {
        anyhow!(
            "completed Pages bundle publish journal disappeared before validated cleanup: {}",
            journal_path.display()
        )
    })?;
    if observed != *expected {
        bail!(
            "Pages bundle publish journal changed before cleanup; preserved the current entry without mutation: {}",
            journal_path.display()
        );
    }
    fs::remove_file(&journal_path).with_context(|| {
        format!(
            "failed removing completed Pages bundle publish journal {}",
            journal_path.display()
        )
    })?;
    sync_parent_directory(&journal_path)
}

fn first_unowned_bundle_publish_backup(
    final_dir: &Path,
    owned_backup: Option<&Path>,
) -> Result<Option<PathBuf>> {
    let legacy_path = legacy_bundle_publish_in_progress_backup_path(final_dir);
    if owned_backup != Some(legacy_path.as_path()) && bundle_path_entry_exists(&legacy_path)? {
        return Ok(Some(legacy_path));
    }

    let Some(parent) = final_dir.parent() else {
        return Ok(None);
    };
    let entries = match fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed scanning Pages bundle publish namespace {}",
                    parent.display()
                )
            });
        }
    };
    let prefix = bundle_publish_backup_prefix(final_dir)?;
    for (index, entry) in entries.enumerate() {
        if index >= BUNDLE_PUBLISH_BACKUP_SCAN_LIMIT {
            bail!(
                "Pages bundle publish namespace {} exceeds the {}-entry recovery scan bound; preserved every entry",
                parent.display(),
                BUNDLE_PUBLISH_BACKUP_SCAN_LIMIT
            );
        }
        let entry = entry.with_context(|| {
            format!(
                "failed reading Pages bundle publish namespace {}",
                parent.display()
            )
        })?;
        let path = entry.path();
        if owned_backup == Some(path.as_path()) {
            continue;
        }
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(&prefix))
        {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

fn update_bundle_evidence_path(hasher: &mut Sha256, kind: u8, path: &str) -> Result<()> {
    let path_len = u64::try_from(path.len())
        .map_err(|_| anyhow!("Pages bundle relative path length exceeds u64"))?;
    hasher.update([kind]);
    hasher.update(path_len.to_le_bytes());
    hasher.update(path.as_bytes());
    Ok(())
}

fn inspect_bundle_tree(path: &Path, label: &str) -> Result<BundleTreeEvidence> {
    inspect_bundle_tree_with_entry_limit(path, label, BUNDLE_TREE_EVIDENCE_ENTRY_LIMIT)
}

fn inspect_bundle_tree_with_entry_limit(
    path: &Path,
    label: &str,
    entry_limit: usize,
) -> Result<BundleTreeEvidence> {
    if !ensure_bundle_directory_entry(path, label)? {
        bail!("{label} does not exist: {}", path.display());
    }

    let mut hasher = Sha256::new();
    hasher.update(BUNDLE_TREE_EVIDENCE_DOMAIN);
    let mut file_count = 0_u64;
    let mut total_size_bytes = 0_u64;
    let mut entry_count = 0_usize;
    let mut pending_directories = vec![path.to_path_buf()];
    let mut buffer = [0_u8; 64 * 1024];

    while let Some(directory) = pending_directories.pop() {
        let read_dir = fs::read_dir(&directory)
            .with_context(|| format!("failed reading {label} directory {}", directory.display()))?;
        let mut entries = Vec::new();
        for entry in read_dir {
            if entry_count >= entry_limit {
                bail!(
                    "{label} exceeds the {entry_limit}-entry recovery-evidence bound; preserved the tree without mutation: {}",
                    path.display()
                );
            }
            entries.push(entry.with_context(|| {
                format!("failed walking {label} directory {}", directory.display())
            })?);
            entry_count += 1;
        }
        entries.sort_by_key(|entry| entry.file_name());
        let mut child_directories = Vec::new();

        for entry in entries {
            let entry_path = entry.path();
            let relative = entry_path.strip_prefix(path).with_context(|| {
                format!(
                    "{label} entry escaped its root {}: {}",
                    path.display(),
                    entry_path.display()
                )
            })?;
            let relative = relative.to_str().ok_or_else(|| {
                anyhow!(
                    "{label} contains a non-UTF-8 relative path, which cannot be recovery-bound: {}",
                    entry_path.display()
                )
            })?;
            let metadata = fs::symlink_metadata(&entry_path).with_context(|| {
                format!("failed inspecting {label} entry {}", entry_path.display())
            })?;
            let file_type = metadata.file_type();
            if file_type.is_symlink() {
                bail!(
                    "{label} contains a symlink, which cannot be safely published or recursively cleaned: {}",
                    entry_path.display()
                );
            }
            if file_type.is_dir() {
                update_bundle_evidence_path(&mut hasher, b'D', relative)?;
                child_directories.push(entry_path);
                continue;
            }
            if !file_type.is_file() {
                bail!(
                    "{label} contains an unsupported filesystem entry: {}",
                    entry_path.display()
                );
            }

            let mut file = File::open(&entry_path)
                .with_context(|| format!("failed opening {label} file {}", entry_path.display()))?;
            let opened_metadata = file.metadata().with_context(|| {
                format!(
                    "failed inspecting opened {label} file {}",
                    entry_path.display()
                )
            })?;
            if !opened_metadata.file_type().is_file() {
                bail!(
                    "{label} entry changed file type while opening: {}",
                    entry_path.display()
                );
            }
            #[cfg(unix)]
            if opened_metadata.nlink() != 1 {
                bail!(
                    "{label} file {} has {} hard links; exclusive tree ownership is not provable",
                    entry_path.display(),
                    opened_metadata.nlink()
                );
            }
            let opened_identity = crate::franken_sync::FileIdentity::from_file(&file)
                .with_context(|| {
                    format!(
                        "failed identifying opened {label} file {}",
                        entry_path.display()
                    )
                })?
                .ok_or_else(|| {
                    anyhow!(
                        "filesystem does not expose a stable identity for {label} file {}",
                        entry_path.display()
                    )
                })?;
            update_bundle_evidence_path(&mut hasher, b'F', relative)?;
            hasher.update(opened_metadata.len().to_le_bytes());
            let mut observed_size = 0_u64;
            loop {
                let read = file.read(&mut buffer).with_context(|| {
                    format!("failed hashing {label} file {}", entry_path.display())
                })?;
                if read == 0 {
                    break;
                }
                observed_size = observed_size
                    .checked_add(u64::try_from(read).unwrap_or(u64::MAX))
                    .ok_or_else(|| anyhow!("{label} size overflow while hashing"))?;
                hasher.update(&buffer[..read]);
            }
            if observed_size != opened_metadata.len() {
                bail!(
                    "{label} file changed size while hashing: {}",
                    entry_path.display()
                );
            }
            let path_probe = File::open(&entry_path).with_context(|| {
                format!("failed re-opening {label} file {}", entry_path.display())
            })?;
            let path_identity = crate::franken_sync::FileIdentity::from_file(&path_probe)
                .with_context(|| {
                    format!(
                        "failed re-identifying {label} file {}",
                        entry_path.display()
                    )
                })?
                .ok_or_else(|| {
                    anyhow!(
                        "filesystem stopped exposing a stable identity for {label} file {}",
                        entry_path.display()
                    )
                })?;
            if path_identity != opened_identity {
                bail!(
                    "{label} file changed identity while hashing: {}",
                    entry_path.display()
                );
            }
            let observed_metadata = fs::symlink_metadata(&entry_path).with_context(|| {
                format!("failed re-inspecting {label} file {}", entry_path.display())
            })?;
            if !observed_metadata.file_type().is_file()
                || observed_metadata.len() != opened_metadata.len()
            {
                bail!(
                    "{label} file changed metadata while hashing: {}",
                    entry_path.display()
                );
            }
            #[cfg(unix)]
            if observed_metadata.nlink() != 1 {
                bail!(
                    "{label} file {} gained additional hard links while hashing",
                    entry_path.display()
                );
            }
            file_count = file_count
                .checked_add(1)
                .ok_or_else(|| anyhow!("{label} file count overflow"))?;
            total_size_bytes = total_size_bytes
                .checked_add(opened_metadata.len())
                .ok_or_else(|| anyhow!("{label} total size overflow"))?;
        }

        for child in child_directories.into_iter().rev() {
            pending_directories.push(child);
        }
    }

    Ok(BundleTreeEvidence {
        file_count,
        total_size_bytes,
        sha256: hex::encode(hasher.finalize()),
    })
}

fn journal_prior_evidence(journal: &BundlePublishJournal) -> BundleTreeEvidence {
    BundleTreeEvidence {
        file_count: journal.prior_file_count,
        total_size_bytes: journal.prior_size_bytes,
        sha256: journal.prior_sha256.clone(),
    }
}

fn journal_candidate_evidence(journal: &BundlePublishJournal) -> BundleTreeEvidence {
    BundleTreeEvidence {
        file_count: journal.candidate_file_count,
        total_size_bytes: journal.candidate_size_bytes,
        sha256: journal.candidate_sha256.clone(),
    }
}

fn ensure_bundle_tree_matches(
    path: &Path,
    label: &str,
    expected: &BundleTreeEvidence,
) -> Result<BundleTreeEvidence> {
    let observed = inspect_bundle_tree(path, label)?;
    if observed != *expected {
        bail!(
            "{label} changed identity or content; preserved it without mutation: {}",
            path.display()
        );
    }
    Ok(observed)
}

fn unique_bundle_sidecar_path(path: &Path, suffix: &str, fallback_name: &str) -> Result<PathBuf> {
    static NEXT_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    let random_nonce = bundle_sidecar_random_nonce()?;
    let nonce = NEXT_NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(fallback_name);

    Ok(path.with_file_name(format!(".{file_name}.{suffix}.{random_nonce:032x}.{nonce}")))
}

fn bundle_sidecar_random_nonce() -> Result<u128> {
    let mut bytes = [0u8; 16];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| anyhow!("failed to generate random bundle sidecar nonce"))?;
    Ok(u128::from_le_bytes(bytes))
}

fn ensure_replaceable_bundle_output_dir(path: &Path) -> Result<bool> {
    ensure_bundle_directory_entry(path, "bundle output path")
}

fn ensure_bundle_directory_entry(path: &Path, label: &str) -> Result<bool> {
    ensure_existing_parent_ancestors_are_real_dirs(path, label)?;

    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            if file_type.is_symlink() {
                bail!("{label} must not be a symlink: {}", path.display());
            }
            if !file_type.is_dir() {
                bail!(
                    "{label} points to a file, expected a directory: {}",
                    path.display()
                );
            }
            Ok(true)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => {
            Err(err).with_context(|| format!("failed inspecting {label} {}", path.display()))
        }
    }
}

fn ensure_existing_parent_ancestors_are_real_dirs(path: &Path, label: &str) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };

    let mut ancestors = Vec::new();
    let mut current = Some(parent);
    while let Some(ancestor) = current {
        if ancestor.as_os_str().is_empty() {
            break;
        }
        ancestors.push(ancestor.to_path_buf());
        current = ancestor.parent();
    }
    ancestors.reverse();

    for ancestor in ancestors {
        match fs::symlink_metadata(&ancestor) {
            Ok(metadata) => {
                let file_type = metadata.file_type();
                if file_type.is_symlink() {
                    if is_allowed_system_symlink_ancestor(&ancestor) {
                        continue;
                    }
                    bail!(
                        "{label} parent must not contain symlinks: {}",
                        ancestor.display()
                    );
                }
                if !file_type.is_dir() {
                    bail!("{label} parent must be a directory: {}", ancestor.display());
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed inspecting {label} parent {}", ancestor.display())
                });
            }
        }
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn is_allowed_system_symlink_ancestor(path: &Path) -> bool {
    path == Path::new("/var") || path == Path::new("/tmp")
}

#[cfg(not(target_os = "macos"))]
fn is_allowed_system_symlink_ancestor(_path: &Path) -> bool {
    false
}

fn replace_dir_from_temp(
    temp_dir: &Path,
    final_dir: &Path,
    retain_temp_on_error: &mut bool,
) -> Result<()> {
    *retain_temp_on_error = false;
    if !ensure_bundle_directory_entry(temp_dir, "staged bundle path")? {
        bail!("staged bundle path does not exist: {}", temp_dir.display());
    }

    let publish_guard = acquire_bundle_publish_guard(final_dir)?;
    require_bundle_publish_guard(final_dir, &publish_guard)?;
    recover_interrupted_bundle_publish(final_dir)?;
    require_bundle_publish_guard(final_dir, &publish_guard)?;
    let candidate = inspect_bundle_tree(temp_dir, "staged bundle path")?;
    if !ensure_replaceable_bundle_output_dir(final_dir)? {
        require_bundle_publish_guard(final_dir, &publish_guard)?;
        fs::rename(temp_dir, final_dir).with_context(|| {
            format!(
                "failed renaming completed bundle {} into place at {}",
                temp_dir.display(),
                final_dir.display()
            )
        })?;
        ensure_bundle_tree_matches(final_dir, "first published bundle", &candidate)?;
        sync_parent_directory(final_dir).with_context(|| {
            format!(
                "completed bundle is live at {}, but its first publication could not be durably synced",
                final_dir.display()
            )
        })?;
        return Ok(());
    }

    let prior = inspect_bundle_tree(final_dir, "prior live bundle")?;
    let backup_dir = unpredictable_bundle_publish_backup_path(final_dir)?;
    if bundle_path_entry_exists(&backup_dir)? {
        bail!(
            "unpredictable Pages bundle backup path unexpectedly exists; preserved every generation: {}",
            backup_dir.display()
        );
    }
    require_bundle_publish_guard(final_dir, &publish_guard)?;
    let journal = write_bundle_publish_journal(final_dir, &backup_dir, &prior, &candidate)?;
    if bundle_path_entry_exists(&backup_dir)? {
        bail!(
            "journaled Pages bundle backup path {} appeared before publication; preserved every generation and the journal",
            backup_dir.display()
        );
    }
    ensure_bundle_tree_matches(final_dir, "prior live bundle", &prior)?;
    ensure_bundle_tree_matches(temp_dir, "staged bundle path", &candidate)?;
    require_bundle_publish_guard(final_dir, &publish_guard)?;

    #[cfg(target_os = "linux")]
    {
        if try_publish_linux_bundle_via_atomic_exchange(
            temp_dir,
            final_dir,
            &backup_dir,
            &journal,
            &prior,
            &candidate,
            retain_temp_on_error,
        )? {
            return Ok(());
        }
    }

    replace_dir_from_temp_via_recoverable_rename_pair(
        temp_dir,
        final_dir,
        &backup_dir,
        &journal,
        &prior,
        &candidate,
        retain_temp_on_error,
    )
}

fn recover_interrupted_bundle_publish(final_dir: &Path) -> Result<()> {
    let Some(journal) = read_bundle_publish_journal(final_dir)? else {
        if let Some(unowned_backup) = first_unowned_bundle_publish_backup(final_dir, None)? {
            bail!(
                "unowned Pages bundle recovery artifact {} was preserved; refusing to infer ownership or modify live path {}",
                unowned_backup.display(),
                final_dir.display()
            );
        }
        return Ok(());
    };
    let backup_dir = validate_bundle_publish_journal(final_dir, &journal)?;
    if let Some(unowned_backup) = first_unowned_bundle_publish_backup(final_dir, Some(&backup_dir))?
    {
        bail!(
            "Pages bundle publish journal identifies {}, but additional unowned recovery artifact {} also exists; preserved every generation",
            backup_dir.display(),
            unowned_backup.display()
        );
    }
    let prior = journal_prior_evidence(&journal);
    let candidate = journal_candidate_evidence(&journal);
    let final_evidence = if ensure_replaceable_bundle_output_dir(final_dir)? {
        Some(inspect_bundle_tree(
            final_dir,
            "interrupted bundle publish destination",
        )?)
    } else {
        None
    };
    let backup_evidence = if bundle_path_entry_exists(&backup_dir)? {
        Some(inspect_bundle_tree(
            &backup_dir,
            "journaled bundle publish backup",
        )?)
    } else {
        None
    };

    match (final_evidence, backup_evidence) {
        (None, Some(backup)) if backup == prior => {
            if bundle_path_entry_exists(final_dir)? {
                bail!(
                    "journaled prior Pages bundle {} was ready to restore, but live path {} was concurrently repopulated; preserved every entry and the journal",
                    backup_dir.display(),
                    final_dir.display()
                );
            }
            ensure_bundle_tree_matches(&backup_dir, "journaled prior Pages bundle", &prior)?;
            fs::rename(&backup_dir, final_dir).with_context(|| {
                format!(
                    "failed restoring journaled prior Pages bundle {} to missing live path {}; preserved the backup and journal",
                    backup_dir.display(),
                    final_dir.display()
                )
            })?;
            ensure_bundle_tree_matches(final_dir, "restored prior Pages bundle", &prior)?;
            sync_parent_directory(final_dir).with_context(|| {
                format!(
                    "restored the prior Pages bundle at {}, but could not durably sync the recovery; journal retained at {}",
                    final_dir.display(),
                    bundle_publish_recovery_journal_path(final_dir).display()
                )
            })?;
            remove_bundle_publish_journal(final_dir, &journal)
        }
        (None, Some(_)) => bail!(
            "journaled Pages bundle backup {} does not match the prior generation while live path {} is missing; preserved the backup and journal",
            backup_dir.display(),
            final_dir.display()
        ),
        (Some(live), Some(backup)) if live == candidate && backup == prior => {
            ensure_bundle_tree_matches(
                final_dir,
                "journaled live candidate Pages bundle",
                &candidate,
            )?;
            cleanup_journaled_bundle_tree_after_publish(
                &backup_dir,
                final_dir,
                "journaled prior Pages bundle",
                &prior,
            )?;
            remove_bundle_publish_journal(final_dir, &journal)
        }
        (Some(live), Some(backup)) if live == prior && backup == candidate => {
            ensure_bundle_tree_matches(final_dir, "journaled prior live Pages bundle", &prior)?;
            cleanup_journaled_bundle_tree_after_publish(
                &backup_dir,
                final_dir,
                "journaled rejected candidate Pages bundle",
                &candidate,
            )?;
            remove_bundle_publish_journal(final_dir, &journal)
        }
        (Some(_), Some(_)) => bail!(
            "journaled Pages bundle recovery found live {} and backup {}, but their exact tree evidence does not match a valid publish state; preserved both and the journal",
            final_dir.display(),
            backup_dir.display()
        ),
        (Some(live), None) if live == prior || live == candidate => {
            ensure_bundle_tree_matches(final_dir, "journaled surviving Pages bundle", &live)?;
            remove_bundle_publish_journal(final_dir, &journal)
        }
        (Some(_), None) => bail!(
            "Pages bundle publish journal remains at {}, but live tree {} matches neither journaled generation; preserved both",
            bundle_publish_recovery_journal_path(final_dir).display(),
            final_dir.display()
        ),
        (None, None) => bail!(
            "Pages bundle publish journal remains at {}, but both live {} and journaled backup {} are missing; preserved the journal",
            bundle_publish_recovery_journal_path(final_dir).display(),
            final_dir.display(),
            backup_dir.display()
        ),
    }
}

fn cleanup_journaled_bundle_tree_after_publish(
    tree_dir: &Path,
    final_dir: &Path,
    label: &str,
    expected: &BundleTreeEvidence,
) -> Result<()> {
    cleanup_journaled_bundle_tree_after_publish_with(tree_dir, final_dir, label, expected, |path| {
        fs::remove_dir_all(path)
    })
}

fn cleanup_journaled_bundle_tree_after_publish_with<F>(
    tree_dir: &Path,
    final_dir: &Path,
    label: &str,
    expected: &BundleTreeEvidence,
    remove_dir: F,
) -> Result<()>
where
    F: FnOnce(&Path) -> std::io::Result<()>,
{
    ensure_bundle_tree_matches(tree_dir, label, expected)?;
    remove_dir(tree_dir).with_context(|| {
        format!(
            "new bundle is live at {}, but failed to remove {label} containing private artifacts retained at {}",
            final_dir.display(),
            tree_dir.display()
        )
    })?;
    sync_parent_directory(final_dir).with_context(|| {
        format!(
            "removed {label} after publishing {}, but could not durably sync its parent directory",
            final_dir.display()
        )
    })
}

#[cfg(target_os = "linux")]
fn try_publish_linux_bundle_via_atomic_exchange(
    temp_dir: &Path,
    final_dir: &Path,
    backup_dir: &Path,
    journal: &BundlePublishJournal,
    prior: &BundleTreeEvidence,
    candidate: &BundleTreeEvidence,
    retain_temp_on_error: &mut bool,
) -> Result<bool> {
    fs::rename(temp_dir, backup_dir).with_context(|| {
        format!(
            "failed moving staged bundle {} to journaled atomic-exchange path {}",
            temp_dir.display(),
            backup_dir.display()
        )
    })?;
    if let Err(error) =
        ensure_bundle_tree_matches(backup_dir, "journaled atomic-exchange candidate", candidate)
    {
        *retain_temp_on_error = true;
        return Err(error);
    }
    if let Err(sync_error) = sync_parent_directory(backup_dir) {
        return match restore_linux_atomic_staged_candidate(
            backup_dir,
            temp_dir,
            candidate,
            retain_temp_on_error,
        ) {
            Ok(()) => Err(sync_error).with_context(|| {
                format!(
                    "failed durably staging bundle at journaled atomic-exchange path {}; restored candidate at {}",
                    backup_dir.display(),
                    temp_dir.display()
                )
            }),
            Err(restore_error) => Err(anyhow!(
                "failed durably staging bundle at journaled atomic-exchange path {}: {sync_error:#}; failed restoring staged candidate to {}: {restore_error:#}",
                backup_dir.display(),
                temp_dir.display()
            )),
        };
    }

    match crate::indexer::atomic_exchange_paths(final_dir, backup_dir) {
        Ok(()) => {
            if let Err(sync_error) = sync_parent_directory(final_dir) {
                *retain_temp_on_error = true;
                bail!(
                    "new bundle is live at {} after atomic exchange, but the parent directory sync failed: {}; prior bundle containing private artifacts retained at {}",
                    final_dir.display(),
                    sync_error,
                    backup_dir.display()
                );
            }
            if let Err(error) =
                ensure_bundle_tree_matches(final_dir, "published Pages bundle candidate", candidate)
            {
                *retain_temp_on_error = true;
                return Err(error);
            }
            if let Err(cleanup_error) = cleanup_journaled_bundle_tree_after_publish(
                backup_dir,
                final_dir,
                "journaled prior Pages bundle",
                prior,
            ) {
                *retain_temp_on_error = true;
                return Err(cleanup_error);
            }
            remove_bundle_publish_journal(final_dir, journal)?;
            Ok(true)
        }
        Err(exchange_error)
            if crate::indexer::linux_atomic_exchange_is_unsupported(&exchange_error) =>
        {
            restore_linux_atomic_staged_candidate(
                backup_dir,
                temp_dir,
                candidate,
                retain_temp_on_error,
            )
            .context(
                "atomic bundle exchange is unsupported and the staged candidate could not be restored for rename-pair publication",
            )?;
            tracing::info!(
                live_bundle_path = %final_dir.display(),
                staged_bundle_path = %temp_dir.display(),
                "renameat2(RENAME_EXCHANGE) is unsupported for the Pages bundle output; using recoverable rename-pair publication"
            );
            Ok(false)
        }
        Err(exchange_error) => {
            match restore_linux_atomic_staged_candidate(
                backup_dir,
                temp_dir,
                candidate,
                retain_temp_on_error,
            ) {
                Ok(()) => Err(exchange_error).with_context(|| {
                    format!(
                        "failed atomically exchanging staged bundle {} with live bundle {}",
                        temp_dir.display(),
                        final_dir.display()
                    )
                }),
                Err(restore_error) => Err(anyhow!(
                    "failed atomically exchanging staged bundle {} with live bundle {}: {exchange_error:#}; failed restoring staged candidate from {}: {restore_error:#}",
                    temp_dir.display(),
                    final_dir.display(),
                    backup_dir.display()
                )),
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn restore_linux_atomic_staged_candidate(
    backup_dir: &Path,
    temp_dir: &Path,
    candidate: &BundleTreeEvidence,
    retain_temp_on_error: &mut bool,
) -> Result<()> {
    if let Err(restore_error) = fs::rename(backup_dir, temp_dir) {
        *retain_temp_on_error = true;
        bail!(
            "failed restoring staged bundle from journaled atomic-exchange path {} to {}; staged bundle containing private artifacts retained at {}: {}",
            backup_dir.display(),
            temp_dir.display(),
            backup_dir.display(),
            restore_error
        );
    }
    if let Err(error) = ensure_bundle_tree_matches(
        temp_dir,
        "restored staged Pages bundle candidate",
        candidate,
    ) {
        *retain_temp_on_error = true;
        return Err(error);
    }
    sync_parent_directory(temp_dir).with_context(|| {
        format!(
            "restored staged bundle to {}, but could not durably sync the rename from {}",
            temp_dir.display(),
            backup_dir.display()
        )
    })
}

fn replace_dir_from_temp_via_recoverable_rename_pair(
    temp_dir: &Path,
    final_dir: &Path,
    backup_dir: &Path,
    journal: &BundlePublishJournal,
    prior: &BundleTreeEvidence,
    candidate: &BundleTreeEvidence,
    retain_temp_on_error: &mut bool,
) -> Result<()> {
    ensure_bundle_tree_matches(final_dir, "prior live Pages bundle", prior)?;
    ensure_bundle_tree_matches(temp_dir, "staged Pages bundle candidate", candidate)?;
    if bundle_path_entry_exists(backup_dir)? {
        bail!(
            "journaled Pages bundle backup path appeared before rename-pair publication; preserved every generation: {}",
            backup_dir.display()
        );
    }
    fs::rename(final_dir, backup_dir).with_context(|| {
        format!(
            "failed parking live bundle {} at journaled recovery path {}",
            final_dir.display(),
            backup_dir.display()
        )
    })?;
    if let Err(error) =
        ensure_bundle_tree_matches(backup_dir, "journaled prior Pages bundle", prior)
    {
        *retain_temp_on_error = true;
        return Err(error);
    }

    if let Err(park_sync_error) = sync_parent_directory(final_dir) {
        return match fs::rename(backup_dir, final_dir) {
            Ok(()) => {
                ensure_bundle_tree_matches(final_dir, "restored prior Pages bundle", prior)?;
                sync_parent_directory(final_dir).with_context(|| {
                    format!(
                        "failed syncing recovery after restoring prior live bundle at {}",
                        final_dir.display()
                    )
                })?;
                Err(park_sync_error).with_context(|| {
                    format!(
                        "failed durably parking the prior live bundle at {}; restored it at {}",
                        backup_dir.display(),
                        final_dir.display()
                    )
                })
            }
            Err(restore_error) => {
                *retain_temp_on_error = true;
                bail!(
                    "failed durably parking the prior live bundle at {}: {}; restore to {} also failed: {}; prior bundle retained at {} and staged bundle retained at {}",
                    backup_dir.display(),
                    park_sync_error,
                    final_dir.display(),
                    restore_error,
                    backup_dir.display(),
                    temp_dir.display()
                )
            }
        };
    }

    match fs::rename(temp_dir, final_dir) {
        Ok(()) => {
            if let Err(error) =
                ensure_bundle_tree_matches(final_dir, "published Pages bundle candidate", candidate)
            {
                *retain_temp_on_error = true;
                return Err(error);
            }
            if let Err(sync_error) = sync_parent_directory(final_dir) {
                *retain_temp_on_error = true;
                bail!(
                    "new bundle was moved into place at {}, but the publish could not be durably synced: {}; prior bundle containing private artifacts retained at {}",
                    final_dir.display(),
                    sync_error,
                    backup_dir.display()
                );
            }
            if let Err(cleanup_error) = cleanup_journaled_bundle_tree_after_publish(
                backup_dir,
                final_dir,
                "journaled prior Pages bundle",
                prior,
            ) {
                *retain_temp_on_error = true;
                return Err(cleanup_error);
            }
            remove_bundle_publish_journal(final_dir, journal)
        }
        Err(publish_error) => match fs::rename(backup_dir, final_dir) {
            Ok(()) => {
                ensure_bundle_tree_matches(final_dir, "restored prior Pages bundle", prior)?;
                sync_parent_directory(final_dir)?;
                Err(publish_error).with_context(|| {
                    format!(
                        "failed publishing staged bundle {} at {}; restored the prior live bundle",
                        temp_dir.display(),
                        final_dir.display()
                    )
                })
            }
            Err(restore_error) => {
                *retain_temp_on_error = true;
                bail!(
                    "failed publishing staged bundle {} at {}: {}; restore also failed: {}; prior bundle retained at {} and staged bundle retained at {}",
                    temp_dir.display(),
                    final_dir.display(),
                    publish_error,
                    restore_error,
                    backup_dir.display(),
                    temp_dir.display()
                );
            }
        },
    }
}

#[cfg(not(windows))]
fn sync_tree(path: &Path) -> Result<()> {
    sync_tree_inner(path)?;
    sync_parent_directory(path)
}

#[cfg(windows)]
fn sync_tree(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(not(windows))]
fn sync_tree_inner(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed reading metadata for {}", path.display()))?;
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        return Ok(());
    }
    if file_type.is_file() {
        File::open(path)
            .with_context(|| format!("failed opening {} for sync", path.display()))?
            .sync_all()
            .with_context(|| format!("failed syncing {}", path.display()))?;
        return Ok(());
    }
    if file_type.is_dir() {
        for entry in
            fs::read_dir(path).with_context(|| format!("failed reading {}", path.display()))?
        {
            let entry = entry.with_context(|| format!("failed walking {}", path.display()))?;
            sync_tree_inner(&entry.path())?;
        }
        File::open(path)
            .with_context(|| format!("failed opening directory {} for sync", path.display()))?
            .sync_all()
            .with_context(|| format!("failed syncing directory {}", path.display()))?;
    }
    Ok(())
}

#[cfg(not(windows))]
fn sync_parent_directory(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    File::open(parent)
        .with_context(|| format!("failed opening parent directory {}", parent.display()))?
        .sync_all()
        .with_context(|| format!("failed syncing parent directory {}", parent.display()))
}

/// 70o8f durability contract, Windows: NTFS exposes no supported way to
/// fsync a DIRECTORY handle's entry table from stable Rust (directory
/// handles need FILE_FLAG_BACKUP_SEMANTICS and FlushFileBuffers on them is
/// not documented to durably order sibling renames), so this is an explicit
/// no-op. The crash-recovery model therefore does NOT rely on rename
/// ordering being durable on Windows: every rename-pair phase is journaled
/// first (BundlePublishJournal, written and fsynced as a FILE), and
/// `recover_interrupted_bundle_publish` re-derives the phase from the
/// journal plus on-disk evidence after restart — the strongest contract
/// available without platform-specific unsafe. On Unix the parent directory
/// is fsynced for real (above).
#[cfg(windows)]
fn sync_parent_directory(_path: &Path) -> Result<()> {
    Ok(())
}

/// Result from bundle building
#[derive(Debug, Clone)]
pub struct BundleResult {
    /// Path to site/ directory (serve or preview this locally)
    pub site_dir: PathBuf,
    /// Path to private/ directory
    pub private_dir: PathBuf,
    /// Number of payload files
    pub chunk_count: usize,
    /// Number of attachment blobs
    pub attachment_count: usize,
    /// Integrity fingerprint (for visual verification)
    pub fingerprint: String,
    /// Total number of files in site/
    pub total_files: usize,
}

/// Copy a single unencrypted payload file into the site directory.
fn copy_payload_file(
    src_root: &Path,
    site_dir: &Path,
    config: &UnencryptedConfig,
) -> Result<usize> {
    let rel_path = Path::new(&config.payload.path);
    if rel_path.is_absolute() {
        bail!("Unencrypted payload path must be relative");
    }
    if rel_path
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        bail!("Unencrypted payload path must not contain '..'");
    }
    if !rel_path.starts_with("payload") {
        bail!("Unencrypted payload path must reside under payload/");
    }

    let src_path = src_root.join(rel_path);
    ensure_regular_copy_source_under_root(src_root, &src_path, "Unencrypted payload file")?;

    let dest_path = site_dir.join(rel_path);
    if let Some(parent) = dest_path.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::copy(&src_path, &dest_path)?;
    Ok(1)
}

fn resolve_generated_doc_path(site_dir: &Path, doc: &GeneratedDoc) -> Result<PathBuf> {
    if doc.filename.contains(['/', '\\']) {
        bail!(
            "Generated documentation filename must not contain path separators: {}",
            doc.filename
        );
    }

    let rel_path = Path::new(&doc.filename);
    let mut components = rel_path.components();
    let Some(std::path::Component::Normal(file_name)) = components.next() else {
        bail!(
            "Generated documentation filename must be a plain relative file name: {}",
            doc.filename
        );
    };
    if components.next().is_some() {
        bail!(
            "Generated documentation filename must not contain path separators: {}",
            doc.filename
        );
    }

    Ok(match doc.location {
        DocLocation::RepoRoot | DocLocation::WebRoot => site_dir.join(file_name),
    })
}

fn ensure_regular_copy_source_under_root(
    src_root: &Path,
    src_path: &Path,
    label: &str,
) -> Result<()> {
    let metadata = fs::symlink_metadata(src_path)
        .with_context(|| format!("{label} not found: {}", src_path.display()))?;
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        bail!("{label} must not be a symlink: {}", src_path.display());
    }
    if !file_type.is_file() {
        bail!("{label} must be a regular file: {}", src_path.display());
    }

    let canonical_root = src_root.canonicalize().with_context(|| {
        format!(
            "Failed to resolve bundle source directory {}",
            src_root.display()
        )
    })?;
    let canonical_source = src_path.canonicalize().with_context(|| {
        format!(
            "Failed to resolve {label} source path {}",
            src_path.display()
        )
    })?;
    if !canonical_source.starts_with(&canonical_root) {
        bail!(
            "{label} resolves outside bundle source directory: {}",
            src_path.display()
        );
    }

    Ok(())
}

fn validate_embedded_vendor_asset(asset: &PinnedVendorAsset) -> Result<()> {
    let actual_size = u64::try_from(asset.bytes.len()).context("Vendor asset size overflow")?;
    if actual_size != asset.size {
        bail!(
            "Embedded Pages vendor asset {} has size {}, expected {}",
            asset.path,
            actual_size,
            asset.size
        );
    }

    let actual_sha256 = hex::encode(Sha256::digest(asset.bytes));
    if actual_sha256 != asset.sha256 {
        bail!(
            "Embedded Pages vendor asset {} has SHA-256 {}, expected {}",
            asset.path,
            actual_sha256,
            asset.sha256
        );
    }

    Ok(())
}

/// Materialize the exact self-hosted runtime dependency set into `site/vendor`.
///
/// The embedded bytes are checked against independent hard-coded pins before
/// any file is written. This turns accidental vendor replacement into a build
/// failure instead of silently publishing unreviewed executable code.
pub(crate) fn write_pinned_vendor_assets(site_dir: &Path) -> Result<()> {
    for asset in PAGES_VENDOR_ASSETS {
        validate_embedded_vendor_asset(asset)?;
    }

    fs::create_dir_all(site_dir.join("vendor"))
        .context("Failed to create site/vendor directory")?;
    for asset in PAGES_VENDOR_ASSETS {
        fs::write(site_dir.join(asset.path), asset.bytes)
            .with_context(|| format!("Failed to write Pages vendor asset {}", asset.path))?;
    }

    Ok(())
}

/// Verify that a materialized bundle contains every pinned vendor file as an
/// exact regular-file byte match.
pub(crate) fn validate_pinned_vendor_assets(site_dir: &Path) -> Result<()> {
    for asset in PAGES_VENDOR_ASSETS {
        let path = site_dir.join(asset.path);
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("Missing Pages vendor asset {}", asset.path))?;
        if !metadata.file_type().is_file() {
            bail!("Pages vendor asset {} is not a regular file", asset.path);
        }
        if metadata.len() != asset.size {
            bail!(
                "Pages vendor asset {} has size {}, expected {}",
                asset.path,
                metadata.len(),
                asset.size
            );
        }

        let entry = build_integrity_entry(&path)
            .with_context(|| format!("Failed to hash Pages vendor asset {}", asset.path))?;
        if entry.sha256 != asset.sha256 {
            bail!(
                "Pages vendor asset {} has SHA-256 {}, expected {}",
                asset.path,
                entry.sha256,
                asset.sha256
            );
        }
    }

    Ok(())
}

/// Generate integrity manifest for all files in a directory
pub(crate) fn generate_integrity_manifest(dir: &Path) -> Result<IntegrityManifest> {
    let mut files = BTreeMap::new();

    collect_file_hashes(dir, dir, &mut files)?;

    Ok(IntegrityManifest {
        version: 1,
        generated_at: Utc::now().to_rfc3339(),
        files,
    })
}

/// Recursively collect SHA256 hashes of all files
fn collect_file_hashes(
    base_dir: &Path,
    current_dir: &Path,
    files: &mut BTreeMap<String, IntegrityEntry>,
) -> Result<()> {
    let canonical_base_dir = base_dir.canonicalize().with_context(|| {
        format!(
            "Failed to resolve site directory {} while generating integrity manifest",
            base_dir.display()
        )
    })?;
    collect_file_hashes_recursive(base_dir, current_dir, &canonical_base_dir, files)
}

fn collect_file_hashes_recursive(
    base_dir: &Path,
    current_dir: &Path,
    canonical_base_dir: &Path,
    files: &mut BTreeMap<String, IntegrityEntry>,
) -> Result<()> {
    for entry in fs::read_dir(current_dir)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        let file_type = metadata.file_type();
        let rel_path = path.strip_prefix(base_dir)?;
        let rel_str = rel_path.to_string_lossy().replace('\\', "/");

        // Skip integrity.json itself (chicken/egg)
        if rel_str == "integrity.json" {
            continue;
        }

        if file_type.is_dir() {
            collect_file_hashes_recursive(base_dir, &path, canonical_base_dir, files)?;
        } else if file_type.is_symlink() {
            let canonical_target = path.canonicalize().with_context(|| {
                format!(
                    "Failed to resolve symlink {} while generating integrity manifest",
                    rel_str
                )
            })?;
            if !canonical_target.starts_with(canonical_base_dir) {
                bail!(
                    "Refusing to include symlink outside site directory in integrity manifest: {}",
                    rel_str
                );
            }

            let target_meta = fs::metadata(&path).with_context(|| {
                format!(
                    "Failed to read symlink target metadata for {} while generating integrity manifest",
                    rel_str
                )
            })?;
            if !target_meta.is_file() {
                bail!(
                    "Refusing to include symlink that does not point to a regular file in integrity manifest: {}",
                    rel_str
                );
            }

            files.insert(rel_str, build_integrity_entry(&path)?);
        } else if file_type.is_file() {
            files.insert(rel_str, build_integrity_entry(&path)?);
        }
    }

    Ok(())
}

fn build_integrity_entry(path: &Path) -> Result<IntegrityEntry> {
    let file = File::open(path)?;
    let metadata = file.metadata()?;
    let size = metadata.len();

    let mut hasher = Sha256::new();
    let mut reader = BufReader::new(file);
    let mut buffer = [0u8; 8192];

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    Ok(IntegrityEntry {
        // sha2 ≥ 0.11 dropped `LowerHex` for the `Output` GenericArray;
        // route through `hex::encode` for the same lowercase-hex wire
        // format.
        sha256: hex::encode(hasher.finalize()),
        size,
    })
}

/// Compute a short fingerprint from the integrity manifest
pub(crate) fn compute_fingerprint(manifest: &IntegrityManifest) -> String {
    // Compute a fingerprint by hashing the sorted list of file hashes
    let mut hasher = Sha256::new();

    for (path, entry) in &manifest.files {
        hasher.update(path.as_bytes());
        hasher.update(entry.sha256.as_bytes());
    }

    let hash = hasher.finalize();

    // Return first 16 hex chars as fingerprint. `hex::encode` replaces the
    // pre-sha2-0.11 `format!("{:x}", hash)` path (Output no longer
    // implements `LowerHex`).
    hex::encode(hash)[..16].to_string()
}

/// Write private artifacts that should never be deployed
pub(crate) fn write_private_fingerprint(private_dir: &Path, fingerprint: &str) -> Result<()> {
    let fingerprint_content = format!(
        "Integrity Fingerprint: {}\n\n\
        Generated: {}\n\n\
        Verify this fingerprint matches the one displayed in the web viewer\n\
        before proceeding. If it doesn't match, the archive may have been\n\
        tampered with.\n",
        fingerprint,
        Utc::now().to_rfc3339()
    );
    write_private_artifact_file(
        private_dir,
        "integrity-fingerprint.txt",
        fingerprint_content.as_bytes(),
    )?;
    Ok(())
}

fn ensure_private_artifact_dir(private_dir: &Path) -> Result<()> {
    ensure_existing_parent_ancestors_are_real_dirs(private_dir, "private artifact directory")?;

    match fs::symlink_metadata(private_dir) {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            if file_type.is_symlink() {
                bail!(
                    "private artifact directory must not be a symlink: {}",
                    private_dir.display()
                );
            }
            if !file_type.is_dir() {
                bail!(
                    "private artifact path must be a directory: {}",
                    private_dir.display()
                );
            }
            secure_private_artifact_dir(private_dir)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            create_private_artifact_dir(private_dir)?;
            ensure_private_artifact_dir(private_dir)
        }
        Err(err) => Err(err).with_context(|| {
            format!(
                "Failed to inspect private artifact directory {}",
                private_dir.display()
            )
        }),
    }
}

fn create_private_artifact_dir(private_dir: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        builder.create(private_dir).with_context(|| {
            format!(
                "Failed to create owner-only private artifact directory {}",
                private_dir.display()
            )
        })?;
    }
    #[cfg(not(unix))]
    fs::create_dir_all(private_dir).with_context(|| {
        format!(
            "Failed to create private artifact directory {}",
            private_dir.display()
        )
    })?;
    Ok(())
}

fn secure_private_artifact_dir(private_dir: &Path) -> Result<()> {
    #[cfg(unix)]
    fs::set_permissions(private_dir, fs::Permissions::from_mode(0o700)).with_context(|| {
        format!(
            "Failed to set owner-only permissions on private artifact directory {}",
            private_dir.display()
        )
    })?;
    #[cfg(not(unix))]
    let _ = private_dir;
    Ok(())
}

fn reject_symlinked_private_artifact(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            if file_type.is_symlink() {
                bail!(
                    "private artifact file must not be a symlink: {}",
                    path.display()
                );
            }
            if file_type.is_dir() {
                bail!(
                    "private artifact path must be a regular file, not a directory: {}",
                    path.display()
                );
            }
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err)
            .with_context(|| format!("Failed to inspect private artifact {}", path.display())),
    }
}

fn write_private_artifact_file(private_dir: &Path, filename: &str, contents: &[u8]) -> Result<()> {
    if filename.contains(['/', '\\']) {
        bail!("private artifact filename must not contain path separators: {filename}");
    }

    ensure_private_artifact_dir(private_dir)?;
    let final_path = private_dir.join(filename);
    reject_symlinked_private_artifact(&final_path)?;
    let temp_path = unique_bundle_sidecar_path(&final_path, "tmp", "private_artifact")?;

    let write_result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&temp_path).with_context(|| {
            format!(
                "Failed to create temporary private artifact {}",
                temp_path.display()
            )
        })?;
        file.write_all(contents).with_context(|| {
            format!(
                "Failed to write temporary private artifact {}",
                temp_path.display()
            )
        })?;
        file.sync_all().with_context(|| {
            format!(
                "Failed to sync temporary private artifact {}",
                temp_path.display()
            )
        })?;
        Ok(())
    })();

    if let Err(err) = write_result {
        let _ = fs::remove_file(&temp_path);
        return Err(err);
    }

    let mut retain_temp_on_replace_error = false;
    if let Err(err) = replace_private_artifact_from_temp(
        &temp_path,
        &final_path,
        &mut retain_temp_on_replace_error,
    ) {
        if !retain_temp_on_replace_error {
            let _ = fs::remove_file(&temp_path);
        }
        return Err(err);
    }
    Ok(())
}

#[cfg(windows)]
fn private_artifact_path_entry_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err)
            .with_context(|| format!("Failed to inspect private artifact {}", path.display())),
    }
}

#[cfg(any(windows, test))]
fn replace_private_artifact_from_temp_via_backup(
    temp_path: &Path,
    final_path: &Path,
    first_error: &std::io::Error,
    retain_temp_on_error: &mut bool,
) -> Result<()> {
    *retain_temp_on_error = false;
    reject_symlinked_private_artifact(final_path)?;
    let backup_path = unique_bundle_sidecar_path(final_path, "bak", "private_artifact")?;
    fs::rename(final_path, &backup_path).with_context(|| {
        format!(
            "Failed to preserve existing private artifact {} at {} after initial replacement error: {}",
            final_path.display(),
            backup_path.display(),
            first_error
        )
    })?;

    match fs::rename(temp_path, final_path) {
        Ok(()) => {
            fs::remove_file(&backup_path).with_context(|| {
                format!(
                    "Installed private artifact {} but failed to remove prior generation {}",
                    final_path.display(),
                    backup_path.display()
                )
            })?;
            sync_parent_directory(final_path)
        }
        Err(replace_error) => match fs::rename(&backup_path, final_path) {
            Ok(()) => {
                sync_parent_directory(final_path)?;
                Err(replace_error).with_context(|| {
                    format!(
                        "Failed to replace private artifact {} after initial error {}; restored its prior generation",
                        final_path.display(),
                        first_error
                    )
                })
            }
            Err(restore_error) => {
                *retain_temp_on_error = true;
                bail!(
                    "Failed to replace private artifact {} after initial error {}; replacement error: {}; restore error: {}; prior generation retained at {} and new generation retained at {}",
                    final_path.display(),
                    first_error,
                    replace_error,
                    restore_error,
                    backup_path.display(),
                    temp_path.display()
                )
            }
        },
    }
}

fn replace_private_artifact_from_temp(
    temp_path: &Path,
    final_path: &Path,
    retain_temp_on_error: &mut bool,
) -> Result<()> {
    *retain_temp_on_error = false;
    #[cfg(windows)]
    {
        match fs::rename(temp_path, final_path) {
            Ok(()) => sync_parent_directory(final_path),
            Err(first_error)
                if matches!(
                    first_error.kind(),
                    std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::PermissionDenied
                ) && private_artifact_path_entry_exists(final_path)? =>
            {
                replace_private_artifact_from_temp_via_backup(
                    temp_path,
                    final_path,
                    &first_error,
                    retain_temp_on_error,
                )
            }
            Err(error) => Err(error).with_context(|| {
                format!(
                    "Failed to install private artifact {} from {}",
                    final_path.display(),
                    temp_path.display()
                )
            }),
        }
    }

    #[cfg(not(windows))]
    {
        fs::rename(temp_path, final_path).with_context(|| {
            format!(
                "Failed to install private artifact {} from {}",
                final_path.display(),
                temp_path.display()
            )
        })?;
        sync_parent_directory(final_path)
    }
}

fn write_private_unencrypted_notice(private_dir: &Path) -> Result<()> {
    let content = format!(
        "UNENCRYPTED ARCHIVE WARNING\n\
        ============================\n\n\
        This bundle was generated WITHOUT encryption.\n\
        Anyone with access to the site can read its contents.\n\n\
        Generated: {}\n",
        Utc::now().to_rfc3339()
    );
    write_private_artifact_file(private_dir, "unencrypted-warning.txt", content.as_bytes())?;
    Ok(())
}

/// Generate public README for the site directory
fn generate_public_readme(title: &str, description: &str) -> String {
    let about_line =
        "This is a searchable archive of AI coding agent conversations (not encrypted)";
    let security_section = r#"## Security

⚠️ This archive is **NOT encrypted**.
Anyone with access to the site can read its contents.
Host it only on a trusted, private location."#;
    let open_section = r#"## How to Open

1. Host these files on any static web server
2. Open index.html in a modern browser
3. Verify the fingerprint matches your records
4. The archive loads immediately (no password required)"#;
    let technical_section = r#"## Technical Details

- Encryption: none (unencrypted archive)
- Search: SQLite with FTS5 (runs in browser via sql.js)
- Requires: SharedArrayBuffer (COOP/COEP headers)"#;
    let files_section = r#"- `index.html` - Entry point
- `config.json` - Plaintext archive metadata and payload location
- `integrity.json` - SHA256 hashes for all files
- `payload/data.db` - Unencrypted SQLite database (publicly readable)
- `*.js` - Application code
- `styles.css` - Styling"#;

    format!(
        r#"# {}

{}

## About This Archive

{}
generated by [cass](https://github.com/Dicklesworthstone/coding_agent_session_search).

{}

{}

{}

## Files

{}

## Viewing Locally

Serve this directory with `cass pages --preview <dir>` — it binds to
loopback only (127.0.0.1) — and open the printed `http://127.0.0.1`
address. `cass pages --preview` already sends the COOP/COEP headers the
viewer requires:

```
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

---

Generated by cass v{}
"#,
        title,
        description,
        about_line,
        security_section,
        open_section,
        technical_section,
        files_section,
        env!("CARGO_PKG_VERSION")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_unencrypted_source(root: &Path, payload_name: &str, body: &str) {
        let payload_dir = root.join("payload");
        fs::create_dir_all(&payload_dir).unwrap();
        let payload_path = payload_dir.join(payload_name);
        fs::write(&payload_path, body).unwrap();

        let config = UnencryptedConfig {
            encrypted: false,
            version: "1.0.0".to_string(),
            payload: UnencryptedPayload {
                path: format!("payload/{payload_name}"),
                format: "sqlite".to_string(),
                size_bytes: Some(body.len() as u64),
            },
            warning: Some("UNENCRYPTED".to_string()),
        };

        let file = File::create(root.join("config.json")).unwrap();
        serde_json::to_writer_pretty(BufWriter::new(file), &config).unwrap();
    }

    #[test]
    fn test_bundle_builder_default() {
        let builder = BundleBuilder::new();
        assert_eq!(builder.config.title, "cass Archive");
        assert!(!builder.config.hide_metadata);
    }

    #[test]
    fn test_bundle_builder_fluent() {
        let builder = BundleBuilder::new()
            .title("My Archive")
            .description("Test description")
            .hide_metadata(true);

        assert_eq!(builder.config.title, "My Archive");
        assert_eq!(builder.config.description, "Test description");
        assert!(builder.config.hide_metadata);
    }

    #[test]
    fn test_compute_fingerprint() {
        let mut files = BTreeMap::new();
        files.insert(
            "test.txt".to_string(),
            IntegrityEntry {
                sha256: "abc123".to_string(),
                size: 100,
            },
        );

        let manifest = IntegrityManifest {
            version: 1,
            generated_at: "2024-01-01T00:00:00Z".to_string(),
            files,
        };

        let fingerprint = compute_fingerprint(&manifest);
        assert_eq!(fingerprint.len(), 16);

        // Same manifest should produce same fingerprint
        let fingerprint2 = compute_fingerprint(&manifest);
        assert_eq!(fingerprint, fingerprint2);
    }

    #[test]
    #[cfg(unix)]
    fn private_artifacts_are_owner_only_even_under_permissive_ambient_modes() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new()?;
        let private_dir = temp.path().join("private");
        fs::create_dir_all(&private_dir)?;
        fs::set_permissions(&private_dir, fs::Permissions::from_mode(0o777))?;

        write_private_artifact_file(&private_dir, "fingerprint.txt", b"abcdef123456")?;

        let dir_mode = fs::metadata(&private_dir)?.permissions().mode();
        let file_mode = fs::metadata(private_dir.join("fingerprint.txt"))?
            .permissions()
            .mode();
        if dir_mode & 0o077 != 0 {
            return Err(anyhow!("private directory mode was {dir_mode:o}"));
        }
        if file_mode & 0o077 != 0 {
            return Err(anyhow!("private file mode was {file_mode:o}"));
        }
        Ok(())
    }

    #[test]
    fn private_artifact_writer_replaces_an_existing_generation() -> Result<()> {
        let temp = TempDir::new()?;
        let private_dir = temp.path().join("private");

        write_private_artifact_file(&private_dir, "fingerprint.txt", b"old generation")?;
        write_private_artifact_file(&private_dir, "fingerprint.txt", b"new generation")?;

        let installed = fs::read(private_dir.join("fingerprint.txt"))?;
        if installed != b"new generation" {
            return Err(anyhow!(
                "private artifact replacement published stale bytes"
            ));
        }
        Ok(())
    }

    #[test]
    fn private_artifact_backup_fallback_preserves_then_replaces() -> Result<()> {
        let temp = TempDir::new()?;
        let final_path = temp.path().join("master-key.json");
        let staged_path = temp.path().join("master-key.tmp.json");
        fs::write(&final_path, b"old generation")?;
        fs::write(&staged_path, b"new generation")?;
        let first_error = std::io::Error::from(std::io::ErrorKind::AlreadyExists);
        let mut retain_temp_on_error = false;

        replace_private_artifact_from_temp_via_backup(
            &staged_path,
            &final_path,
            &first_error,
            &mut retain_temp_on_error,
        )?;

        if retain_temp_on_error {
            return Err(anyhow!(
                "successful private artifact fallback requested temp retention"
            ));
        }
        if fs::read(&final_path)? != b"new generation" {
            return Err(anyhow!(
                "private artifact fallback did not publish staged bytes"
            ));
        }
        if staged_path.exists() {
            return Err(anyhow!(
                "private artifact fallback left the staged path behind"
            ));
        }
        let remaining_entries = fs::read_dir(temp.path())?.collect::<std::io::Result<Vec<_>>>()?;
        if remaining_entries.len() != 1 {
            return Err(anyhow!(
                "private artifact fallback left an unexpected backup sidecar"
            ));
        }
        Ok(())
    }

    #[test]
    #[cfg(unix)]
    fn bundle_staging_root_is_private_until_publish() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new()?;
        let staging_root = temp.path().join("bundle.staged");
        create_bundle_staging_root(&staging_root)?;

        let staged_mode = fs::metadata(&staging_root)?.permissions().mode();
        if staged_mode & 0o077 != 0 {
            return Err(anyhow!("bundle staging root mode was {staged_mode:o}"));
        }

        prepare_bundle_root_for_publish(&staging_root)?;
        let published_mode = fs::metadata(&staging_root)?.permissions().mode();
        if published_mode & 0o777 != 0o755 {
            return Err(anyhow!("published bundle root mode was {published_mode:o}"));
        }
        Ok(())
    }

    #[test]
    fn test_generate_public_readme() {
        let unencrypted = generate_public_readme("Test Archive", "A test archive");
        assert!(unencrypted.contains("Test Archive"));
        assert!(unencrypted.contains("A test archive"));
        assert!(unencrypted.contains("NOT encrypted"));
        assert!(unencrypted.contains("no password required"));
        assert!(unencrypted.contains("Plaintext archive metadata"));
        assert!(unencrypted.contains("Unencrypted SQLite database"));
        assert!(!unencrypted.contains("Public encryption parameters"));
        assert!(!unencrypted.contains("Encrypted database chunks"));
    }

    #[test]
    fn test_integrity_manifest_excludes_itself() {
        let temp = TempDir::new().unwrap();
        let temp_path = temp.path();

        // Create some test files
        fs::write(temp_path.join("test.txt"), "hello").unwrap();
        fs::write(temp_path.join("integrity.json"), "{}").unwrap();

        let manifest = generate_integrity_manifest(temp_path).unwrap();

        // Should include test.txt but not integrity.json
        assert!(manifest.files.contains_key("test.txt"));
        assert!(!manifest.files.contains_key("integrity.json"));
    }

    #[test]
    fn pinned_vendor_contract_matches_every_embedded_byte() {
        for asset in PAGES_VENDOR_ASSETS {
            validate_embedded_vendor_asset(asset)
                .unwrap_or_else(|error| panic!("invalid vendor pin for {}: {error:#}", asset.path));
        }

        let manifest: serde_json::Value = serde_json::from_slice(
            PAGES_VENDOR_ASSETS
                .iter()
                .find(|asset| asset.path == "vendor/manifest.json")
                .expect("vendor manifest pin")
                .bytes,
        )
        .expect("valid vendor manifest JSON");
        let package_versions = manifest["packages"]
            .as_array()
            .expect("vendor package list")
            .iter()
            .map(|package| {
                (
                    package["name"].as_str().expect("package name"),
                    package["version"].as_str().expect("package version"),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            package_versions.get("@sqlite.org/sqlite-wasm"),
            Some(&"3.53.0-build1")
        );
        assert_eq!(package_versions.len(), 1);
    }

    #[test]
    fn pinned_vendor_contract_rejects_hash_mutations() {
        let mutated = PinnedVendorAsset {
            path: "vendor/mutated.js",
            bytes: b"mutated",
            size: 7,
            sha256: "0000000000000000000000000000000000000000000000000000000000000000",
        };
        let error = validate_embedded_vendor_asset(&mutated).unwrap_err();
        assert!(error.to_string().contains("SHA-256"));
    }

    #[test]
    fn bundle_build_validates_materialized_vendor_before_integrity_publish() {
        let source = include_str!("bundle.rs");
        let validation = source
            .find("validate_pinned_vendor_assets(&site_dir)")
            .expect("materialized vendor validation in build path");
        let integrity = source
            .find("progress(\"integrity\", \"Generating integrity manifest...\")")
            .expect("integrity generation step");
        let publish = source
            .find("replace_dir_from_temp(")
            .expect("bundle publication step");
        assert!(validation < integrity && integrity < publish);
    }

    #[test]
    fn materialized_vendor_validation_rejects_same_size_tampering() {
        let temp = TempDir::new().unwrap();
        write_pinned_vendor_assets(temp.path()).unwrap();
        validate_pinned_vendor_assets(temp.path()).unwrap();

        let fflate_path = temp.path().join("vendor/fflate.js");
        let mut mutated = fs::read(&fflate_path).unwrap();
        mutated[0] ^= 0x01;
        fs::write(fflate_path, mutated).unwrap();

        let error = validate_pinned_vendor_assets(temp.path()).unwrap_err();
        assert!(error.to_string().contains("SHA-256"));
    }

    #[test]
    fn test_collect_file_hashes() {
        let temp = TempDir::new().unwrap();
        let temp_path = temp.path();

        // Create nested structure
        fs::create_dir_all(temp_path.join("subdir")).unwrap();
        fs::write(temp_path.join("root.txt"), "root").unwrap();
        fs::write(temp_path.join("subdir/nested.txt"), "nested").unwrap();

        let mut files = BTreeMap::new();
        collect_file_hashes(temp_path, temp_path, &mut files).unwrap();

        assert_eq!(files.len(), 2);
        assert!(files.contains_key("root.txt"));
        assert!(files.contains_key("subdir/nested.txt"));

        // Verify hash is SHA256 hex (64 chars)
        for entry in files.values() {
            assert_eq!(entry.sha256.len(), 64);
        }
    }

    #[test]
    #[cfg(unix)]
    fn test_collect_file_hashes_includes_symlinked_files_within_site() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();
        let temp_path = temp.path();

        fs::write(temp_path.join("real.txt"), "real").unwrap();
        symlink("real.txt", temp_path.join("linked-file.txt")).unwrap();

        let mut files = BTreeMap::new();
        collect_file_hashes(temp_path, temp_path, &mut files).unwrap();

        assert_eq!(files.len(), 2);
        assert!(files.contains_key("real.txt"));
        assert!(files.contains_key("linked-file.txt"));
        assert_eq!(files["real.txt"].sha256, files["linked-file.txt"].sha256);
        assert_eq!(files["real.txt"].size, files["linked-file.txt"].size);
    }

    #[test]
    #[cfg(unix)]
    fn test_collect_file_hashes_rejects_symlinks_outside_site() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();
        let temp_path = temp.path();
        let outside = TempDir::new().unwrap();

        fs::write(temp_path.join("root.txt"), "root").unwrap();
        fs::write(outside.path().join("secret.txt"), "secret").unwrap();
        fs::create_dir_all(outside.path().join("nested")).unwrap();
        fs::write(outside.path().join("nested/hidden.txt"), "hidden").unwrap();
        symlink(
            outside.path().join("secret.txt"),
            temp_path.join("linked-file.txt"),
        )
        .unwrap();
        symlink(outside.path().join("nested"), temp_path.join("linked-dir")).unwrap();

        let mut files = BTreeMap::new();
        let err = collect_file_hashes(temp_path, temp_path, &mut files).unwrap_err();
        assert!(
            err.to_string().contains("outside site directory"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    #[cfg(unix)]
    fn test_copy_unencrypted_payload_rejects_final_symlink() {
        use std::os::unix::fs::symlink;

        let source = TempDir::new().unwrap();
        let site = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();

        fs::create_dir_all(source.path().join("payload")).unwrap();
        fs::write(outside.path().join("secret.db"), "outside secret").unwrap();
        symlink(
            outside.path().join("secret.db"),
            source.path().join("payload/data.db"),
        )
        .unwrap();

        let config = UnencryptedConfig {
            encrypted: false,
            version: "1.0.0".to_string(),
            payload: UnencryptedPayload {
                path: "payload/data.db".to_string(),
                format: "sqlite".to_string(),
                size_bytes: None,
            },
            warning: None,
        };

        let err = copy_payload_file(source.path(), site.path(), &config).unwrap_err();
        assert!(
            err.to_string().contains("must not be a symlink"),
            "unexpected error: {err:#}"
        );
        assert!(!site.path().join("payload/data.db").exists());
    }

    #[test]
    #[cfg(unix)]
    fn test_copy_unencrypted_payload_rejects_symlinked_parent_escape() {
        use std::os::unix::fs::symlink;

        let source = TempDir::new().unwrap();
        let site = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();

        fs::write(outside.path().join("data.db"), "outside secret").unwrap();
        symlink(outside.path(), source.path().join("payload")).unwrap();

        let config = UnencryptedConfig {
            encrypted: false,
            version: "1.0.0".to_string(),
            payload: UnencryptedPayload {
                path: "payload/data.db".to_string(),
                format: "sqlite".to_string(),
                size_bytes: None,
            },
            warning: None,
        };

        let err = copy_payload_file(source.path(), site.path(), &config).unwrap_err();
        assert!(
            err.to_string().contains("outside bundle source directory"),
            "unexpected error: {err:#}"
        );
        assert!(!site.path().join("payload/data.db").exists());
    }

    #[test]
    fn test_generated_docs_reject_path_traversal_filename() {
        let source = TempDir::new().unwrap();
        let output_parent = TempDir::new().unwrap();
        let output_dir = output_parent.path().join("bundle");

        write_unencrypted_source(source.path(), "data.db", "payload");

        let config = BundleConfig {
            generated_docs: vec![GeneratedDoc {
                filename: "../escaped.md".to_string(),
                content: "escaped".to_string(),
                location: DocLocation::WebRoot,
            }],
            ..BundleConfig::default()
        };

        let err = BundleBuilder::with_config(config)
            .build(source.path(), output_dir.as_path(), |_, _| {})
            .unwrap_err();
        assert!(
            err.to_string().contains("must not contain path separators"),
            "unexpected error: {err:#}"
        );
        assert!(!output_parent.path().join("escaped.md").exists());
        let leftovers = fs::read_dir(output_parent.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(
            leftovers.is_empty(),
            "rejected bundle staging directory leaked: {leftovers:?}"
        );
    }

    #[test]
    fn test_generated_docs_reject_backslash_separator_filename() {
        let doc = GeneratedDoc {
            filename: r"nested\escaped.md".to_string(),
            content: "escaped".to_string(),
            location: DocLocation::WebRoot,
        };

        let err = resolve_generated_doc_path(Path::new("site"), &doc).unwrap_err();
        assert!(
            err.to_string().contains("must not contain path separators"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn test_build_replaces_existing_bundle_without_stale_files() {
        let source = TempDir::new().unwrap();
        let output_parent = TempDir::new().unwrap();
        let output_dir = output_parent.path().join("bundle");

        write_unencrypted_source(source.path(), "data.db", "fresh payload");

        let builder = BundleBuilder::new();
        builder
            .build(source.path(), output_dir.as_path(), |_, _| {})
            .expect("initial build");

        fs::write(output_dir.join("site/stale.txt"), "stale").unwrap();
        fs::write(output_dir.join("private/old-secret.txt"), "secret").unwrap();
        fs::write(output_dir.join("site/payload/old.bin"), "old").unwrap();

        builder
            .build(source.path(), output_dir.as_path(), |_, _| {})
            .expect("rebuild");

        assert!(output_dir.join("site/config.json").exists());
        assert!(
            output_dir
                .join("private/integrity-fingerprint.txt")
                .exists()
        );
        assert!(!output_dir.join("site/stale.txt").exists());
        assert!(!output_dir.join("private/old-secret.txt").exists());
        assert!(!output_dir.join("site/payload/old.bin").exists());
        assert!(output_dir.join("site/payload/data.db").exists());
    }

    #[test]
    fn test_build_failure_preserves_existing_bundle() {
        let source = TempDir::new().unwrap();
        let output_parent = TempDir::new().unwrap();
        let output_dir = output_parent.path().join("bundle");
        let broken_source = TempDir::new().unwrap();

        write_unencrypted_source(source.path(), "data.db", "fresh payload");

        let builder = BundleBuilder::new();
        builder
            .build(source.path(), output_dir.as_path(), |_, _| {})
            .expect("initial build");

        fs::write(output_dir.join("site/marker.txt"), "keep me").unwrap();

        let result = builder.build(broken_source.path(), output_dir.as_path(), |_, _| {});
        assert!(result.is_err(), "broken rebuild should fail");

        assert!(output_dir.join("site/marker.txt").exists());
        assert!(output_dir.join("site/config.json").exists());
        assert!(
            output_dir
                .join("private/integrity-fingerprint.txt")
                .exists()
        );
    }

    #[test]
    #[cfg(unix)]
    fn test_build_rejects_symlinked_output_directory() {
        use std::os::unix::fs::symlink;

        let source = TempDir::new().unwrap();
        let output_parent = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        let output_dir = output_parent.path().join("bundle-link");

        write_unencrypted_source(source.path(), "data.db", "payload");
        symlink(outside.path(), &output_dir).unwrap();

        let err = BundleBuilder::new()
            .build(source.path(), output_dir.as_path(), |_, _| {})
            .unwrap_err();

        assert!(
            err.to_string().contains("must not be a symlink"),
            "unexpected error: {err:#}"
        );
        assert!(
            fs::symlink_metadata(&output_dir)
                .unwrap()
                .file_type()
                .is_symlink(),
            "rejected symlink output path must be preserved for operator inspection"
        );
        assert!(
            !outside.path().join("site").exists(),
            "build must not write through a symlinked output directory"
        );
    }

    #[test]
    #[cfg(unix)]
    fn test_build_rejects_symlinked_output_parent_before_staging() {
        use std::os::unix::fs::symlink;

        let source = TempDir::new().unwrap();
        let output_parent = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        let linked_parent = output_parent.path().join("linked-parent");
        let output_dir = linked_parent.join("bundle");

        write_unencrypted_source(source.path(), "data.db", "payload");
        symlink(outside.path(), &linked_parent).unwrap();

        let err = BundleBuilder::new()
            .build(source.path(), output_dir.as_path(), |_, _| {})
            .unwrap_err();

        assert!(
            err.to_string().contains("parent must not contain symlinks"),
            "unexpected error: {err:#}"
        );
        assert!(
            fs::read_dir(outside.path()).unwrap().next().is_none(),
            "bundle builder must not stage output through a symlinked parent"
        );
    }

    #[test]
    fn test_replace_dir_from_temp_overwrites_existing_bundle() {
        let temp = TempDir::new().unwrap();
        let final_dir = temp.path().join("bundle");
        let staged_dir = temp.path().join("bundle.staged");

        fs::create_dir_all(final_dir.join("site")).unwrap();
        fs::write(final_dir.join("site/old.txt"), "old").unwrap();

        fs::create_dir_all(staged_dir.join("site")).unwrap();
        fs::write(staged_dir.join("site/new.txt"), "new").unwrap();

        let mut retain_temp_on_error = false;
        replace_dir_from_temp(&staged_dir, &final_dir, &mut retain_temp_on_error).unwrap();

        assert!(!staged_dir.exists());
        assert!(final_dir.join("site/new.txt").exists());
        assert!(!final_dir.join("site/old.txt").exists());
        assert!(
            !bundle_publish_recovery_journal_path(&final_dir).exists(),
            "completed publication journal should be cleaned up"
        );
        assert!(
            first_unowned_bundle_publish_backup(&final_dir, None)
                .unwrap()
                .is_none(),
            "completed publication should not leave a recovery backup"
        );
    }

    #[test]
    fn test_concurrent_publisher_is_refused_while_lock_is_held() {
        // 70o8f: per-output single-publisher arbitration — while one
        // publisher holds the advisory lock, a second publication attempt
        // for the SAME output fails fast with the contention error instead
        // of racing the rename pair.
        let temp = TempDir::new().unwrap();
        let final_dir = temp.path().join("bundle");
        let staged_dir = temp.path().join("bundle.staged");
        fs::create_dir_all(staged_dir.join("site")).unwrap();
        fs::write(staged_dir.join("site/new.txt"), "new").unwrap();

        let held = acquire_bundle_publish_guard(&final_dir).unwrap();
        let mut retain_temp_on_error = false;
        let error = replace_dir_from_temp(&staged_dir, &final_dir, &mut retain_temp_on_error)
            .expect_err("second publisher must be refused while the lock is held");
        assert!(
            error.to_string().contains("already active"),
            "expected lock-contention arbitration error, got: {error:#}"
        );
        assert!(
            staged_dir.join("site/new.txt").exists(),
            "refused publisher must leave its staged bundle untouched"
        );
        assert!(
            !final_dir.exists(),
            "refused publisher must not create the output"
        );
        drop(held);

        // Once the first publisher releases, the same publication succeeds.
        let mut retain_temp_on_error = false;
        replace_dir_from_temp(&staged_dir, &final_dir, &mut retain_temp_on_error).unwrap();
        assert!(final_dir.join("site/new.txt").exists());
    }

    #[test]
    fn test_swapped_lock_entry_is_refused_by_guard_revalidation() {
        // 70o8f: an attacker (or a confused cleaner) replacing the lock
        // pathname after acquisition must be detected — the guard re-checks
        // the pathname's file identity before authorizing mutation.
        let temp = TempDir::new().unwrap();
        let final_dir = temp.path().join("bundle");
        let guard = acquire_bundle_publish_guard(&final_dir).unwrap();
        let lock_path = bundle_publish_lock_path(&final_dir);

        // Swap the lock entry: remove and recreate at the same pathname.
        fs::remove_file(&lock_path).unwrap();
        fs::write(&lock_path, b"impostor").unwrap();

        let error = require_bundle_publish_guard(&final_dir, &guard)
            .expect_err("guard revalidation must refuse a swapped lock entry");
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("identity"),
            "expected an identity-mismatch refusal, got: {rendered}"
        );
    }

    #[test]
    fn test_non_regular_lock_entry_is_refused_fail_closed() {
        // 70o8f: a pre-existing non-regular-file entry at the lock pathname
        // (e.g. a directory planted by an attacker) must fail closed rather
        // than being followed or replaced.
        let temp = TempDir::new().unwrap();
        let final_dir = temp.path().join("bundle");
        let lock_path = bundle_publish_lock_path(&final_dir);
        fs::create_dir_all(&lock_path).unwrap();

        let error = acquire_bundle_publish_guard(&final_dir)
            .expect_err("a directory at the lock pathname must be refused");
        assert!(
            error.to_string().contains("not a regular file"),
            "expected fail-closed non-regular-file refusal, got: {error:#}"
        );
    }

    #[test]
    fn test_replace_dir_from_temp_preserves_first_publish_behavior() {
        let temp = TempDir::new().unwrap();
        let final_dir = temp.path().join("bundle");
        let staged_dir = temp.path().join("bundle.staged");

        fs::create_dir_all(staged_dir.join("site")).unwrap();
        fs::write(staged_dir.join("site/new.txt"), "new").unwrap();

        let mut retain_temp_on_error = false;
        replace_dir_from_temp(&staged_dir, &final_dir, &mut retain_temp_on_error).unwrap();

        assert!(!retain_temp_on_error);
        assert!(!staged_dir.exists());
        assert_eq!(
            fs::read_to_string(final_dir.join("site/new.txt")).unwrap(),
            "new"
        );
        assert!(!bundle_publish_recovery_journal_path(&final_dir).exists());
        assert!(
            first_unowned_bundle_publish_backup(&final_dir, None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_recover_interrupted_bundle_publish_restores_parked_only_live_bundle() {
        let temp = TempDir::new().unwrap();
        let final_dir = temp.path().join("bundle");
        let staged_dir = temp.path().join("bundle.staged");

        fs::create_dir_all(final_dir.join("private")).unwrap();
        fs::write(final_dir.join("private/old-private.txt"), "old").unwrap();
        fs::create_dir_all(staged_dir.join("private")).unwrap();
        fs::write(staged_dir.join("private/new-private.txt"), "new").unwrap();
        let prior = inspect_bundle_tree(&final_dir, "prior").unwrap();
        let candidate = inspect_bundle_tree(&staged_dir, "candidate").unwrap();
        let backup_dir = unpredictable_bundle_publish_backup_path(&final_dir).unwrap();
        write_bundle_publish_journal(&final_dir, &backup_dir, &prior, &candidate).unwrap();

        // Failpoint state: the fallback publisher durably parked OLD, then
        // the process died before installing NEW at the live handle.
        fs::rename(&final_dir, &backup_dir).unwrap();
        assert!(!final_dir.exists());

        recover_interrupted_bundle_publish(&final_dir).unwrap();

        assert_eq!(
            fs::read_to_string(final_dir.join("private/old-private.txt")).unwrap(),
            "old"
        );
        assert!(!backup_dir.exists());
        assert_eq!(
            fs::read_to_string(staged_dir.join("private/new-private.txt")).unwrap(),
            "new",
            "recovery must not consume the next staged candidate"
        );
        assert!(!bundle_publish_recovery_journal_path(&final_dir).exists());
    }

    #[test]
    fn test_recover_interrupted_fallback_publish_keeps_new_live_and_cleans_parked_old() {
        let temp = TempDir::new().unwrap();
        let final_dir = temp.path().join("bundle");
        let staged_dir = temp.path().join("bundle.staged");

        fs::create_dir_all(final_dir.join("private")).unwrap();
        fs::write(final_dir.join("private/old-private.txt"), "old").unwrap();
        fs::create_dir_all(staged_dir.join("private")).unwrap();
        fs::write(staged_dir.join("private/new-private.txt"), "new").unwrap();
        let prior = inspect_bundle_tree(&final_dir, "prior").unwrap();
        let candidate = inspect_bundle_tree(&staged_dir, "candidate").unwrap();
        let backup_dir = unpredictable_bundle_publish_backup_path(&final_dir).unwrap();
        write_bundle_publish_journal(&final_dir, &backup_dir, &prior, &candidate).unwrap();

        // Failpoint state: OLD was parked and NEW reached the live handle,
        // then the process died before removing OLD.
        fs::rename(&final_dir, &backup_dir).unwrap();
        fs::rename(&staged_dir, &final_dir).unwrap();

        recover_interrupted_bundle_publish(&final_dir).unwrap();

        assert_eq!(
            fs::read_to_string(final_dir.join("private/new-private.txt")).unwrap(),
            "new"
        );
        assert!(!final_dir.join("private/old-private.txt").exists());
        assert!(!backup_dir.exists());
        assert!(!bundle_publish_recovery_journal_path(&final_dir).exists());
    }

    #[test]
    fn test_recover_interrupted_publish_preserves_unowned_legacy_sidecar() {
        let temp = TempDir::new().unwrap();
        let final_dir = temp.path().join("bundle");
        let backup_dir = legacy_bundle_publish_in_progress_backup_path(&final_dir);

        fs::create_dir_all(final_dir.join("site")).unwrap();
        fs::write(final_dir.join("site/live.txt"), "live").unwrap();
        fs::create_dir_all(backup_dir.join("unrelated")).unwrap();
        fs::write(backup_dir.join("unrelated/sentinel.txt"), "preserve").unwrap();

        let error = recover_interrupted_bundle_publish(&final_dir)
            .expect_err("an unowned legacy sidecar must be preserved");
        let message = format!("{error:#}");

        assert!(
            message.contains("unowned Pages bundle recovery artifact"),
            "unexpected error: {message}"
        );
        assert_eq!(
            fs::read_to_string(final_dir.join("site/live.txt")).unwrap(),
            "live"
        );
        assert_eq!(
            fs::read_to_string(backup_dir.join("unrelated/sentinel.txt")).unwrap(),
            "preserve"
        );
    }

    #[test]
    fn test_recover_missing_live_never_adopts_unowned_legacy_sidecar() {
        let temp = TempDir::new().unwrap();
        let final_dir = temp.path().join("bundle");
        let backup_dir = legacy_bundle_publish_in_progress_backup_path(&final_dir);
        fs::create_dir_all(backup_dir.join("unrelated")).unwrap();
        fs::write(backup_dir.join("unrelated/sentinel.txt"), "preserve").unwrap();

        let error = recover_interrupted_bundle_publish(&final_dir)
            .expect_err("an unjournaled directory must never become the live bundle");

        assert!(error.to_string().contains("refusing to infer ownership"));
        assert!(!final_dir.exists());
        assert_eq!(
            fs::read_to_string(backup_dir.join("unrelated/sentinel.txt")).unwrap(),
            "preserve"
        );
    }

    #[test]
    fn test_legacy_constant_marker_cannot_authorize_recursive_cleanup() {
        let temp = TempDir::new().unwrap();
        let final_dir = temp.path().join("bundle");
        let backup_dir = legacy_bundle_publish_in_progress_backup_path(&final_dir);
        fs::create_dir_all(final_dir.join("site")).unwrap();
        fs::write(
            final_dir.join(".cass-pages-publish-in-progress-v1"),
            b"cass-pages-publish-in-progress-v1\n",
        )
        .unwrap();
        fs::create_dir_all(backup_dir.join("unrelated")).unwrap();
        fs::write(backup_dir.join("unrelated/sentinel.txt"), "preserve").unwrap();

        recover_interrupted_bundle_publish(&final_dir)
            .expect_err("a public legacy marker must not confer ownership");

        assert!(final_dir.exists());
        assert_eq!(
            fs::read_to_string(backup_dir.join("unrelated/sentinel.txt")).unwrap(),
            "preserve"
        );
    }

    #[test]
    fn test_recover_interrupted_atomic_publish_before_exchange_keeps_prior_live() {
        let temp = TempDir::new().unwrap();
        let final_dir = temp.path().join("bundle");
        let staged_dir = temp.path().join("bundle.staged");

        fs::create_dir_all(final_dir.join("private")).unwrap();
        fs::write(final_dir.join("private/old-private.txt"), "old").unwrap();
        fs::create_dir_all(staged_dir.join("private")).unwrap();
        fs::write(staged_dir.join("private/new-private.txt"), "new").unwrap();
        let prior = inspect_bundle_tree(&final_dir, "prior").unwrap();
        let candidate = inspect_bundle_tree(&staged_dir, "candidate").unwrap();
        let exchange_dir = unpredictable_bundle_publish_backup_path(&final_dir).unwrap();
        write_bundle_publish_journal(&final_dir, &exchange_dir, &prior, &candidate).unwrap();

        // Failpoint state: NEW reached the journaled exchange handle,
        // but the process died before the atomic exchange committed.
        fs::rename(&staged_dir, &exchange_dir).unwrap();

        recover_interrupted_bundle_publish(&final_dir).unwrap();

        assert_eq!(
            fs::read_to_string(final_dir.join("private/old-private.txt")).unwrap(),
            "old"
        );
        assert!(!final_dir.join("private/new-private.txt").exists());
        assert!(!exchange_dir.exists());
        assert!(!bundle_publish_recovery_journal_path(&final_dir).exists());
    }

    #[test]
    fn test_recover_interrupted_atomic_publish_immediately_after_exchange_keeps_new_live() {
        let temp = TempDir::new().unwrap();
        let final_dir = temp.path().join("bundle");
        let staged_dir = temp.path().join("bundle.staged");
        let parked_dir = temp.path().join("bundle.parked-for-test");
        fs::create_dir_all(final_dir.join("private")).unwrap();
        fs::write(final_dir.join("private/old-private.txt"), "old").unwrap();
        fs::create_dir_all(staged_dir.join("private")).unwrap();
        fs::write(staged_dir.join("private/new-private.txt"), "new").unwrap();
        let prior = inspect_bundle_tree(&final_dir, "prior").unwrap();
        let candidate = inspect_bundle_tree(&staged_dir, "candidate").unwrap();
        let exchange_dir = unpredictable_bundle_publish_backup_path(&final_dir).unwrap();
        write_bundle_publish_journal(&final_dir, &exchange_dir, &prior, &candidate).unwrap();

        // Failpoint state immediately after exchange: NEW is live and OLD is
        // at the exact random path authenticated by the durable journal.
        fs::rename(&final_dir, &parked_dir).unwrap();
        fs::rename(&staged_dir, &final_dir).unwrap();
        fs::rename(&parked_dir, &exchange_dir).unwrap();

        recover_interrupted_bundle_publish(&final_dir).unwrap();

        assert_eq!(
            fs::read_to_string(final_dir.join("private/new-private.txt")).unwrap(),
            "new"
        );
        assert!(!final_dir.join("private/old-private.txt").exists());
        assert!(!exchange_dir.exists());
        assert!(!bundle_publish_recovery_journal_path(&final_dir).exists());
    }

    #[test]
    fn test_recovery_rejects_journaled_backup_after_content_mutation() {
        let temp = TempDir::new().unwrap();
        let final_dir = temp.path().join("bundle");
        let staged_dir = temp.path().join("bundle.staged");
        fs::create_dir_all(final_dir.join("private")).unwrap();
        fs::write(final_dir.join("private/old-private.txt"), "old").unwrap();
        fs::create_dir_all(staged_dir.join("private")).unwrap();
        fs::write(staged_dir.join("private/new-private.txt"), "new").unwrap();
        let prior = inspect_bundle_tree(&final_dir, "prior").unwrap();
        let candidate = inspect_bundle_tree(&staged_dir, "candidate").unwrap();
        let backup_dir = unpredictable_bundle_publish_backup_path(&final_dir).unwrap();
        write_bundle_publish_journal(&final_dir, &backup_dir, &prior, &candidate).unwrap();
        fs::rename(&final_dir, &backup_dir).unwrap();
        fs::write(backup_dir.join("private/old-private.txt"), "tampered").unwrap();

        let error = recover_interrupted_bundle_publish(&final_dir)
            .expect_err("mutated journaled bytes must not be restored");

        assert!(
            error
                .to_string()
                .contains("does not match the prior generation")
        );
        assert!(!final_dir.exists());
        assert_eq!(
            fs::read_to_string(backup_dir.join("private/old-private.txt")).unwrap(),
            "tampered"
        );
        assert!(bundle_publish_recovery_journal_path(&final_dir).exists());
    }

    #[test]
    fn test_recoverable_rename_pair_replaces_and_cleans_prior_bundle() {
        let temp = TempDir::new().unwrap();
        let final_dir = temp.path().join("bundle");
        let staged_dir = temp.path().join("bundle.staged");

        fs::create_dir_all(final_dir.join("site")).unwrap();
        fs::write(final_dir.join("site/old.txt"), "old").unwrap();
        fs::create_dir_all(staged_dir.join("site")).unwrap();
        fs::write(staged_dir.join("site/new.txt"), "new").unwrap();
        let prior = inspect_bundle_tree(&final_dir, "prior").unwrap();
        let candidate = inspect_bundle_tree(&staged_dir, "candidate").unwrap();
        let backup_dir = unpredictable_bundle_publish_backup_path(&final_dir).unwrap();
        let journal =
            write_bundle_publish_journal(&final_dir, &backup_dir, &prior, &candidate).unwrap();

        let mut retain_temp_on_error = false;
        replace_dir_from_temp_via_recoverable_rename_pair(
            &staged_dir,
            &final_dir,
            &backup_dir,
            &journal,
            &prior,
            &candidate,
            &mut retain_temp_on_error,
        )
        .unwrap();

        assert!(!retain_temp_on_error);
        assert_eq!(
            fs::read_to_string(final_dir.join("site/new.txt")).unwrap(),
            "new"
        );
        assert!(!backup_dir.exists());
        assert!(!bundle_publish_recovery_journal_path(&final_dir).exists());
    }

    #[test]
    fn test_prior_private_bundle_cleanup_failure_names_every_retained_path() {
        let temp = TempDir::new().unwrap();
        let final_dir = temp.path().join("bundle");
        let backup_dir = temp.path().join("journaled-backup");
        fs::create_dir_all(final_dir.join("site")).unwrap();
        fs::create_dir_all(backup_dir.join("private")).unwrap();
        fs::write(backup_dir.join("private/recovery-material.txt"), "private").unwrap();
        let expected = inspect_bundle_tree(&backup_dir, "prior").unwrap();

        let error = cleanup_journaled_bundle_tree_after_publish_with(
            &backup_dir,
            &final_dir,
            "journaled prior Pages bundle",
            &expected,
            |_| {
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "injected cleanup denial",
                ))
            },
        )
        .expect_err("cleanup failure must fail the publication result");
        let message = format!("{error:#}");

        assert!(message.contains(&final_dir.display().to_string()));
        assert!(message.contains(&backup_dir.display().to_string()));
        assert!(message.contains("injected cleanup denial"));
        assert!(message.contains("private artifacts retained"));
        assert!(backup_dir.join("private/recovery-material.txt").exists());
    }

    #[test]
    fn test_cleanup_refuses_mutated_tree_before_recursive_removal() {
        let temp = TempDir::new().unwrap();
        let final_dir = temp.path().join("bundle");
        let backup_dir = temp.path().join("journaled-backup");
        fs::create_dir_all(final_dir.join("site")).unwrap();
        fs::create_dir_all(backup_dir.join("private")).unwrap();
        let private_file = backup_dir.join("private/recovery-material.txt");
        fs::write(&private_file, "private").unwrap();
        let expected = inspect_bundle_tree(&backup_dir, "prior").unwrap();
        fs::write(&private_file, "changed after journal").unwrap();
        let remove_called = std::cell::Cell::new(false);

        cleanup_journaled_bundle_tree_after_publish_with(
            &backup_dir,
            &final_dir,
            "journaled prior Pages bundle",
            &expected,
            |_| {
                remove_called.set(true);
                Ok(())
            },
        )
        .expect_err("tree mutation must stop before recursive cleanup");

        assert!(!remove_called.get());
        assert_eq!(
            fs::read_to_string(private_file).unwrap(),
            "changed after journal"
        );
    }

    #[test]
    fn test_bundle_tree_evidence_entry_bound_fails_closed() {
        let temp = TempDir::new().unwrap();
        let tree = temp.path().join("bundle");
        fs::create_dir_all(&tree).unwrap();
        for (name, body) in [("a.txt", "a"), ("b.txt", "b"), ("c.txt", "c")] {
            fs::write(tree.join(name), body).unwrap();
        }

        let error = inspect_bundle_tree_with_entry_limit(&tree, "bounded bundle", 2)
            .expect_err("a tree beyond the evidence bound must fail closed");

        assert!(
            error
                .to_string()
                .contains("2-entry recovery-evidence bound")
        );
        assert_eq!(fs::read_to_string(tree.join("a.txt")).unwrap(), "a");
        assert_eq!(fs::read_to_string(tree.join("b.txt")).unwrap(), "b");
        assert_eq!(fs::read_to_string(tree.join("c.txt")).unwrap(), "c");
    }

    #[test]
    #[cfg(unix)]
    fn test_recover_interrupted_bundle_publish_rejects_symlinked_backup() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        let final_dir = temp.path().join("bundle");
        let staged_dir = temp.path().join("bundle.staged");
        fs::create_dir_all(final_dir.join("site")).unwrap();
        fs::write(final_dir.join("site/old.txt"), "old").unwrap();
        fs::create_dir_all(staged_dir.join("site")).unwrap();
        fs::write(staged_dir.join("site/new.txt"), "new").unwrap();
        let prior = inspect_bundle_tree(&final_dir, "prior").unwrap();
        let candidate = inspect_bundle_tree(&staged_dir, "candidate").unwrap();
        let backup_dir = unpredictable_bundle_publish_backup_path(&final_dir).unwrap();
        write_bundle_publish_journal(&final_dir, &backup_dir, &prior, &candidate).unwrap();
        fs::create_dir_all(outside.path().join("private")).unwrap();
        fs::write(outside.path().join("private/material.txt"), "private").unwrap();
        symlink(outside.path(), &backup_dir).unwrap();

        let error = recover_interrupted_bundle_publish(&final_dir)
            .expect_err("a symlinked recovery backup must fail closed");

        assert!(error.to_string().contains("must not be a symlink"));
        assert!(
            error
                .to_string()
                .contains(&backup_dir.display().to_string())
        );
        assert_eq!(
            fs::read_to_string(outside.path().join("private/material.txt")).unwrap(),
            "private"
        );
        assert_eq!(
            fs::read_to_string(final_dir.join("site/old.txt")).unwrap(),
            "old"
        );
    }

    #[test]
    #[cfg(unix)]
    fn test_replace_dir_from_temp_rejects_symlinked_staging_tree() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        let final_dir = temp.path().join("bundle");
        let staged_dir = temp.path().join("bundle.staged");
        fs::create_dir_all(final_dir.join("site")).unwrap();
        fs::write(final_dir.join("site/old.txt"), "old").unwrap();
        fs::create_dir_all(outside.path().join("site")).unwrap();
        fs::write(outside.path().join("site/new.txt"), "new").unwrap();
        symlink(outside.path(), &staged_dir).unwrap();

        let mut retain_temp_on_error = false;
        let error = replace_dir_from_temp(&staged_dir, &final_dir, &mut retain_temp_on_error)
            .expect_err("a symlinked staged bundle must fail closed");

        assert!(error.to_string().contains("must not be a symlink"));
        assert!(
            error
                .to_string()
                .contains(&staged_dir.display().to_string())
        );
        assert_eq!(
            fs::read_to_string(final_dir.join("site/old.txt")).unwrap(),
            "old"
        );
        assert_eq!(
            fs::read_to_string(outside.path().join("site/new.txt")).unwrap(),
            "new"
        );
        assert!(!retain_temp_on_error);
    }

    #[test]
    #[cfg(unix)]
    fn test_replace_dir_from_temp_rejects_dangling_symlink_target() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();
        let final_dir = temp.path().join("bundle");
        let staged_dir = temp.path().join("bundle.staged");

        fs::create_dir_all(staged_dir.join("site")).unwrap();
        fs::write(staged_dir.join("site/new.txt"), "new").unwrap();
        symlink(temp.path().join("missing-target"), &final_dir).unwrap();

        let mut retain_temp_on_error = false;
        let err =
            replace_dir_from_temp(&staged_dir, &final_dir, &mut retain_temp_on_error).unwrap_err();
        assert!(
            err.to_string().contains("must not be a symlink"),
            "unexpected error: {err:#}"
        );
        assert!(staged_dir.join("site/new.txt").exists());
        assert!(
            !retain_temp_on_error,
            "ordinary validation failures do not require recovery retention"
        );
        assert!(
            fs::symlink_metadata(&final_dir)
                .unwrap()
                .file_type()
                .is_symlink(),
            "dangling symlink target must not be silently replaced"
        );
    }
}
