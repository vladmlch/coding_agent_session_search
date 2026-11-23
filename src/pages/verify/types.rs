use super::checks::{
    check_config_schema, check_payload_manifest, check_required_files, check_size_limits,
};
use super::integrity::{check_integrity, check_no_secrets};
use super::paths::calculate_dir_size;
use super::share_profile::check_share_profile;
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

/// Verification result for a single check
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    /// Whether the check passed
    pub passed: bool,
    /// Details about the check (empty if passed, error message if failed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

impl CheckResult {
    pub(super) fn pass() -> Self {
        Self {
            passed: true,
            details: None,
        }
    }

    pub(super) fn fail(details: impl Into<String>) -> Self {
        Self {
            passed: false,
            details: Some(details.into()),
        }
    }
}

/// Summary of all verification checks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyChecks {
    pub required_files: CheckResult,
    pub config_schema: CheckResult,
    pub payload_manifest: CheckResult,
    pub size_limits: CheckResult,
    pub integrity: CheckResult,
    pub no_secrets_in_site: CheckResult,
    /// A plaintext archive holds no value its declared share profile removes.
    pub share_profile: CheckResult,
}

impl VerifyChecks {
    /// Returns true if all checks passed
    pub fn all_passed(&self) -> bool {
        self.required_files.passed
            && self.config_schema.passed
            && self.payload_manifest.passed
            && self.size_limits.passed
            && self.integrity.passed
            && self.no_secrets_in_site.passed
            && self.share_profile.passed
    }
}

/// What verification established about a bundle's share profile (2l1b0.60).
///
/// An encrypted bundle records its profile inside the payload, which
/// verification cannot open, so only plaintext archives are rescanned.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ShareProfileEvidence {
    /// Profile recorded in the plaintext archive's `export_meta`.
    pub declared: Option<String>,
    /// Whether every exported text surface was rescanned with that profile's rules.
    pub rescanned: bool,
    /// Values those rules would still rewrite, counted per surface; never the values.
    pub residual: BTreeMap<String, u64>,
}

/// Complete verification result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyResult {
    /// Overall status: "valid" or "invalid"
    pub status: String,
    /// Individual check results
    pub checks: VerifyChecks,
    /// Share profile declared by, and rescanned in, a plaintext archive
    pub share_profile: ShareProfileEvidence,
    /// Warning messages (non-fatal issues)
    pub warnings: Vec<String>,
    /// Total site size in bytes
    pub site_size_bytes: u64,
}

/// Verify a bundle export
///
/// # Arguments
/// * `path` - Path to the export root (containing site/) or site/ directory itself
/// * `verbose` - Whether to print detailed progress
///
/// # Returns
/// `VerifyResult` with all check outcomes
pub fn verify_bundle(path: &Path, verbose: bool) -> Result<VerifyResult> {
    // Resolve to site/ directory
    let site_dir = crate::pages::resolve_site_dir(path)?;

    if verbose {
        println!("Verifying bundle at: {}", site_dir.display());
    }

    let mut warnings = Vec::new();

    // Check 1: Required files
    if verbose {
        println!("  Checking required files...");
    }
    let required_files = check_required_files(&site_dir);

    // Check 2: Config schema (only if config.json exists)
    if verbose {
        println!("  Checking config.json schema...");
    }
    let config_schema = if site_dir.join("config.json").exists() {
        check_config_schema(&site_dir)
    } else {
        CheckResult::fail("config.json not found")
    };

    // Check 3: Payload manifest
    if verbose {
        println!("  Checking payload manifest...");
    }
    let payload_manifest = check_payload_manifest(&site_dir);

    // Check 4: Size limits
    if verbose {
        println!("  Checking size limits...");
    }
    let size_limits = check_size_limits(&site_dir);

    // Check 5: Integrity (if integrity.json exists)
    if verbose {
        println!("  Checking integrity...");
    }
    let integrity = if site_dir.join("integrity.json").exists() {
        check_integrity(&site_dir, verbose)
    } else {
        CheckResult::fail("integrity.json missing — bundle integrity cannot be verified")
    };

    // Check 6: No secrets in site/
    if verbose {
        println!("  Checking for secret leakage...");
    }
    let no_secrets_in_site = check_no_secrets(&site_dir);

    // Check 7: Share profile (plaintext archives only)
    if verbose {
        println!("  Checking share-profile redaction...");
    }
    let (share_profile_check, share_profile) = check_share_profile(&site_dir, &mut warnings);

    // Calculate total site size
    let site_size_bytes = calculate_dir_size(&site_dir)?;

    let checks = VerifyChecks {
        required_files,
        config_schema,
        payload_manifest,
        size_limits,
        integrity,
        no_secrets_in_site,
        share_profile: share_profile_check,
    };

    let status = if checks.all_passed() {
        "valid".to_string()
    } else {
        "invalid".to_string()
    };

    Ok(VerifyResult {
        status,
        checks,
        share_profile,
        warnings,
        site_size_bytes,
    })
}

fn failed_check_details(verification: &VerifyResult) -> String {
    [
        ("required_files", &verification.checks.required_files),
        ("config_schema", &verification.checks.config_schema),
        ("payload_manifest", &verification.checks.payload_manifest),
        ("size_limits", &verification.checks.size_limits),
        ("integrity", &verification.checks.integrity),
        (
            "no_secrets_in_site",
            &verification.checks.no_secrets_in_site,
        ),
        ("share_profile", &verification.checks.share_profile),
    ]
    .into_iter()
    .filter(|(_, check)| !check.passed)
    .map(|(name, check)| match check.details.as_deref() {
        Some(details) => format!("{name}: {details}"),
        None => name.to_string(),
    })
    .collect::<Vec<_>>()
    .join("; ")
}

/// Require every full-bundle verification check to pass.
///
/// This is the fail-closed gate for completed local/config exports. It returns
/// the detailed verification result only when the bundle is safe to report as
/// successfully built or ready for manual deployment.
pub fn ensure_valid_bundle(path: &Path, verbose: bool) -> Result<VerifyResult> {
    let site_dir = crate::pages::resolve_site_dir(path)?;
    let verification = verify_bundle(&site_dir, verbose)?;
    if verification.status != "valid" {
        bail!(
            "Pages bundle at {} failed full verification: {}",
            site_dir.display(),
            failed_check_details(&verification)
        );
    }
    Ok(verification)
}

/// Run a deployment action only after the exact site path it receives passes
/// every Pages bundle verification check.
///
/// Keeping path resolution, verification, and action dispatch in one helper
/// prevents callers from validating one directory and accidentally deploying a
/// different one. The action is never invoked for an invalid bundle.
pub fn with_verified_bundle_for_deployment<T>(
    path: &Path,
    verbose: bool,
    action: impl FnOnce(&Path) -> Result<T>,
) -> Result<T> {
    let site_dir = crate::pages::resolve_site_dir(path)?;
    let verification = verify_bundle(&site_dir, verbose)?;
    if verification.status != "valid" {
        bail!(
            "Refusing to deploy invalid Pages bundle at {}: {}",
            site_dir.display(),
            failed_check_details(&verification)
        );
    }

    action(&site_dir)
}

/// Print verification result in human-readable format
pub fn print_result(result: &VerifyResult, verbose: bool) {
    let status_icon = if result.status == "valid" {
        "✓"
    } else {
        "✗"
    };
    println!(
        "\n{} Bundle status: {}",
        status_icon,
        result.status.to_uppercase()
    );

    println!("\nChecks:");
    print_check("  Required files", &result.checks.required_files, verbose);
    print_check("  Config schema", &result.checks.config_schema, verbose);
    print_check(
        "  Payload manifest",
        &result.checks.payload_manifest,
        verbose,
    );
    print_check("  Size limits", &result.checks.size_limits, verbose);
    print_check("  Integrity", &result.checks.integrity, verbose);
    print_check("  No secrets", &result.checks.no_secrets_in_site, verbose);
    print_check("  Share profile", &result.checks.share_profile, verbose);
    if let Some(profile) = &result.share_profile.declared {
        let scope = if result.share_profile.rescanned {
            "rescanned"
        } else {
            "not rescanned"
        };
        println!("\nShare profile: {profile} ({scope})");
    }

    if !result.warnings.is_empty() {
        println!("\nWarnings:");
        for warning in &result.warnings {
            println!("  ⚠ {}", warning);
        }
    }

    println!(
        "\nTotal site size: {} bytes ({:.2} MB)",
        result.site_size_bytes,
        result.site_size_bytes as f64 / (1024.0 * 1024.0)
    );
}

fn print_check(name: &str, result: &CheckResult, verbose: bool) {
    let icon = if result.passed { "✓" } else { "✗" };
    print!("{}: {} ", name, icon);

    if result.passed {
        println!("OK");
    } else if let Some(details) = &result.details {
        if verbose {
            println!("FAILED");
            println!("      {}", details);
        } else {
            // Truncate long error messages (char-safe slicing)
            let display = if details.chars().count() > 60 {
                let truncated: String = details.chars().take(60).collect();
                format!("{truncated}...")
            } else {
                details.clone()
            };
            println!("FAILED: {}", display);
        }
    } else {
        println!("FAILED");
    }
}
