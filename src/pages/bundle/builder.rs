use anyhow::{Context, Result, bail};
use chrono::Utc;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fs::{self, File};
use std::io::BufReader;
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};

use crate::pages::docs::{DocLocation, GeneratedDoc};

use super::assets::{PAGES_ASSETS, validate_pinned_vendor_assets, write_pinned_vendor_assets};
use super::integrity::{compute_fingerprint, generate_integrity_manifest};
use super::private_artifacts::{
    ensure_private_artifact_dir, write_private_fingerprint, write_private_unencrypted_notice,
};
use super::publish::{recover_interrupted_bundle_publish, replace_dir_from_temp, sync_tree};
use super::publish_journal::{
    acquire_bundle_publish_guard, bundle_path_entry_exists, bundle_publish_recovery_journal_path,
    ensure_replaceable_bundle_output_dir, first_unowned_bundle_publish_backup,
    require_bundle_publish_guard, unique_bundle_sidecar_path,
};

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
    pub(super) config: BundleConfig,
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
                let dest_path = site_dir.join(name); // ubs:ignore[rust.security.path-traversal] — compile-time PAGES_ASSETS constant flat asset filenames
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

pub(super) fn create_bundle_staging_root(path: &Path) -> Result<()> {
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

pub(super) fn prepare_bundle_root_for_publish(path: &Path) -> Result<()> {
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
pub(super) fn copy_payload_file(
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

    let src_path = src_root.join(rel_path); // ubs:ignore[rust.security.path-traversal] — rel_path validated relative without '..' under payload/ at lines 449-460
    ensure_regular_copy_source_under_root(src_root, &src_path, "Unencrypted payload file")?;

    let dest_path = site_dir.join(rel_path); // ubs:ignore[rust.security.path-traversal] — rel_path validated relative without '..' under payload/ at lines 449-460
    if let Some(parent) = dest_path.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::copy(&src_path, &dest_path)?;
    Ok(1)
}

pub(super) fn resolve_generated_doc_path(site_dir: &Path, doc: &GeneratedDoc) -> Result<PathBuf> {
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
        DocLocation::RepoRoot | DocLocation::WebRoot => site_dir.join(file_name), // ubs:ignore[rust.security.path-traversal] — single Component::Normal filename validated at lines 475-495
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

/// Generate public README for the site directory
pub(super) fn generate_public_readme(title: &str, description: &str) -> String {
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
