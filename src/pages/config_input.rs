//! Non-interactive configuration input for `cass pages` command.
//!
//! This module provides a JSON-based configuration schema for running the pages
//! export workflow in robot/CI mode without interactive wizard prompts.
//!
//! # Example Configuration
//!
//! ```json
//! {
//!   "filters": {
//!     "agents": ["claude-code", "codex"],
//!     "since": "30 days ago",
//!     "until": "2025-01-06",
//!     "workspaces": ["/path/one", "/path/two"],
//!     "path_mode": "relative"
//!   },
//!   "bundle": {
//!     "title": "Team Archive",
//!     "description": "Searchable cass export",
//!     "hide_metadata": false,
//!     "share_profile": "public"
//!   },
//!   "deployment": {
//!     "target": "local",
//!     "output_dir": "./dist"
//!   }
//! }
//! ```

use serde::{Deserialize, Serialize};
use std::io::Read;
use std::path::PathBuf;
use thiserror::Error;

use super::export::PathMode;
use super::profiles::ShareProfile;
use super::wizard::WizardState;
use crate::time_parser::parse_time_input;

/// Errors that can occur when loading or validating pages configuration.
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    ReadFile(#[from] std::io::Error),

    #[error("Failed to parse config JSON: {0}")]
    ParseJson(#[from] serde_json::Error),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Environment variable not found: {0}")]
    EnvVarNotFound(String),

    #[error("Invalid time format: {0}")]
    InvalidTime(String),
}

/// Configuration result for JSON output.
#[derive(Debug, Serialize)]
pub struct ConfigValidationResult {
    /// Whether the configuration is valid.
    pub valid: bool,
    /// Validation errors, if any.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
    /// Warnings that don't prevent export but should be reviewed.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    /// Resolved configuration (with env vars expanded).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved: Option<ResolvedConfig>,
}

/// Resolved configuration with defaults applied.
#[derive(Debug, Serialize)]
pub struct ResolvedConfig {
    pub filters: ResolvedFilters,
    pub bundle: ResolvedBundle,
    pub deployment: ResolvedDeployment,
}

/// Resolved filter configuration.
#[derive(Debug, Serialize)]
pub struct ResolvedFilters {
    pub agents: Vec<String>,
    pub workspaces: Vec<PathBuf>,
    pub since_ts: Option<i64>,
    pub until_ts: Option<i64>,
    pub path_mode: String,
}

/// Resolved bundle configuration.
#[derive(Debug, Serialize)]
pub struct ResolvedBundle {
    pub title: String,
    pub description: String,
    pub hide_metadata: bool,
}

/// Resolved deployment configuration.
#[derive(Debug, Serialize)]
pub struct ResolvedDeployment {
    pub target: String,
    pub output_dir: PathBuf,
    pub repo: Option<String>,
    pub branch: Option<String>,
    pub account_id: Option<String>,
    pub api_token_set: bool,
}

/// Root pages configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PagesConfig {
    /// Filter configuration for content selection.
    #[serde(default)]
    pub filters: FilterConfig,

    /// Bundle/site configuration.
    #[serde(default)]
    pub bundle: BundleConfig,

    /// Deployment configuration.
    #[serde(default)]
    pub deployment: DeploymentConfig,
}

/// Filter configuration for content selection.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FilterConfig {
    /// Filter by agent slugs (e.g., "claude-code", "codex").
    #[serde(default)]
    pub agents: Vec<String>,

    /// Filter entries since this time (ISO date or relative like "30 days ago").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,

    /// Filter entries until this time (ISO date or relative).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<String>,

    /// Filter by workspace paths.
    #[serde(default)]
    pub workspaces: Vec<String>,

    /// Path mode: relative (default), basename, full, hash.
    #[serde(default)]
    pub path_mode: Option<String>,
}

/// Bundle/site configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleConfig {
    /// Site title.
    #[serde(default = "default_title")]
    pub title: String,

    /// Site description.
    #[serde(default = "default_description")]
    pub description: String,

    /// Hide workspace/agent metadata in UI.
    #[serde(default)]
    pub hide_metadata: bool,

    /// Share profile applied to every exported text: public, team or personal.
    /// Defaults to `public` when absent.
    #[serde(default)]
    pub share_profile: Option<ShareProfile>,
}

impl Default for BundleConfig {
    fn default() -> Self {
        Self {
            title: default_title(),
            description: default_description(),
            hide_metadata: false,
            share_profile: None,
        }
    }
}

/// Deployment configuration.
///
/// Only `target = "local"` (the default) is supported: cass runs fully
/// local-only. The hosted-publishing keys below are still deserialized so a
/// legacy config gets an explicit diagnostic instead of being silently
/// ignored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentConfig {
    /// Deployment target: local only (github/cloudflare are rejected).
    #[serde(default = "default_target")]
    pub target: String,

    /// Output directory for local exports.
    #[serde(default = "default_output_dir")]
    pub output_dir: String,

    /// REMOVED: repository/project name for hosted publishing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,

    /// REMOVED: hosted-publishing branch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,

    /// REMOVED: hosted-publishing account ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,

    /// REMOVED: hosted-publishing API token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_token: Option<String>,
}

impl Default for DeploymentConfig {
    fn default() -> Self {
        Self {
            target: default_target(),
            output_dir: default_output_dir(),
            repo: None,
            branch: None,
            account_id: None,
            api_token: None,
        }
    }
}

// Default value functions
fn default_title() -> String {
    "cass Archive".to_string()
}
fn default_description() -> String {
    "Searchable archive of AI coding agent conversations".to_string()
}
fn default_target() -> String {
    "local".to_string()
}
fn default_output_dir() -> String {
    "cass-export".to_string()
}

const DEFAULT_PATH_MODE: &str = "relative";

fn resolve_env_var(env_var: &str) -> Result<String, ConfigError> {
    dotenvy::var(env_var).map_err(|_| ConfigError::EnvVarNotFound(env_var.to_string()))
}

fn option_has_non_empty_value(value: &Option<String>) -> bool {
    value
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
}

impl PagesConfig {
    fn normalized_path_mode(&self) -> Option<String> {
        self.filters
            .path_mode
            .as_deref()
            .map(str::trim)
            .filter(|mode| !mode.is_empty())
            .map(str::to_ascii_lowercase)
    }

    fn normalized_target(&self) -> String {
        self.deployment.target.trim().to_ascii_lowercase()
    }

    /// 019i2: `bundle.hide_metadata` promises hidden workspace paths and
    /// file names, so it forces the obfuscating `hash` policy on the
    /// non-wizard path too (the wizard already maps it to `PathMode::Hash`).
    /// With `hide_metadata` off, the configured `path_mode` stays explicit.
    fn resolved_path_mode(&self) -> String {
        if self.bundle.hide_metadata {
            return "hash".to_string();
        }
        self.normalized_path_mode()
            .unwrap_or_else(|| DEFAULT_PATH_MODE.to_string())
    }

    fn resolved_time_range(&self) -> Option<String> {
        match (&self.filters.since, &self.filters.until) {
            (Some(since), Some(until)) => Some(format!("{} to {}", since, until)),
            (Some(since), None) => Some(format!("since {}", since)),
            (None, Some(until)) => Some(format!("until {}", until)),
            (None, None) => None,
        }
    }

    /// Load configuration from a file path.
    ///
    /// If path is "-", reads from stdin.
    pub fn load(path: &str) -> Result<Self, ConfigError> {
        let content = if path == "-" {
            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf)?;
            buf
        } else {
            std::fs::read_to_string(path)?
        };

        let config: PagesConfig = serde_json::from_str(&content)?;
        Ok(config)
    }

    /// Load configuration from a reader.
    pub fn from_reader<R: Read>(reader: R) -> Result<Self, ConfigError> {
        let config: PagesConfig = serde_json::from_reader(reader)?;
        Ok(config)
    }

    /// Resolve environment variables in configuration values.
    ///
    /// Values starting with "env:" are resolved to the corresponding
    /// environment variable value.
    pub fn resolve_env_vars(&mut self) -> Result<(), ConfigError> {
        // Resolve env vars in output_dir if prefixed
        if let Some(env_var) = self.deployment.output_dir.strip_prefix("env:") {
            self.deployment.output_dir = resolve_env_var(env_var)?;
        }

        // deployment.account_id/api_token are intentionally not resolved:
        // hosted publishing was removed, so their values are only diagnosed
        // (validation warns they are ignored) and never dereferenced.

        Ok(())
    }

    /// Validate the configuration and return any errors/warnings.
    pub fn validate(&self) -> ConfigValidationResult {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        // Validate path_mode if specified
        if let Some(mode) = self.normalized_path_mode() {
            match mode.as_str() {
                "relative" | "basename" | "full" | "hash" => {}
                _ => {
                    errors.push(format!(
                        "Invalid filters.path_mode: '{}'. Must be one of: relative, basename, full, hash",
                        self.filters.path_mode.as_deref().unwrap_or_default()
                    ));
                }
            }
        }

        if self.bundle.share_profile == Some(ShareProfile::Custom) {
            errors.push(
                "bundle.share_profile 'custom' takes per-rule options that config input does not \
                 accept; choose public, team or personal."
                    .to_string(),
            );
        }

        // Validate deployment target
        let target = self.normalized_target();
        if self.deployment.output_dir.trim().is_empty() {
            errors.push("deployment.output_dir must not be empty.".to_string());
        }
        match target.as_str() {
            "local" => {}
            "github" | "cloudflare" => {
                errors.push(format!(
                    "unsupported_deployment_target: deployment.target '{}' is no longer supported; \
                     cass runs fully local-only. Remove the deployment block or set \"local\".",
                    self.deployment.target
                ));
            }
            _ => {
                errors.push(format!(
                    "Invalid deployment.target: '{}'. Only \"local\" is supported (cass runs fully local-only).",
                    self.deployment.target
                ));
            }
        }

        // Legacy hosted-publishing keys are diagnosed, never dereferenced.
        if option_has_non_empty_value(&self.deployment.repo)
            || option_has_non_empty_value(&self.deployment.branch)
            || option_has_non_empty_value(&self.deployment.account_id)
            || option_has_non_empty_value(&self.deployment.api_token)
        {
            warnings.push(
                "deployment.repo/branch/account_id/api_token are ignored: hosted publishing was \
                 removed; cass exports local archives only."
                    .to_string(),
            );
        }

        // Validate time formats
        if let Some(ref since) = self.filters.since
            && parse_time_input(since).is_none()
        {
            errors.push(format!(
                "Invalid filters.since time format: '{}'. \
                 Use ISO 8601 (2025-01-06), relative (30 days ago), or keywords (today, yesterday).",
                since
            ));
        }

        if let Some(ref until) = self.filters.until
            && parse_time_input(until).is_none()
        {
            errors.push(format!(
                "Invalid filters.until time format: '{}'. \
                 Use ISO 8601 (2025-01-06), relative (30 days ago), or keywords (today, yesterday).",
                until
            ));
        }

        let valid = errors.is_empty();
        let resolved = if valid {
            Some(self.to_resolved())
        } else {
            None
        };

        ConfigValidationResult {
            valid,
            errors,
            warnings,
            resolved,
        }
    }

    /// Convert to resolved config (with defaults applied).
    fn to_resolved(&self) -> ResolvedConfig {
        ResolvedConfig {
            filters: ResolvedFilters {
                agents: self.filters.agents.clone(),
                workspaces: self.filters.workspaces.iter().map(PathBuf::from).collect(),
                since_ts: self.filters.since.as_deref().and_then(parse_time_input),
                until_ts: self.filters.until.as_deref().and_then(parse_time_input),
                path_mode: self.resolved_path_mode(),
            },
            bundle: ResolvedBundle {
                title: self.bundle.title.clone(),
                description: self.bundle.description.clone(),
                hide_metadata: self.bundle.hide_metadata,
            },
            deployment: ResolvedDeployment {
                target: self.normalized_target(),
                output_dir: PathBuf::from(&self.deployment.output_dir),
                repo: self.deployment.repo.clone(),
                branch: self.deployment.branch.clone(),
                account_id: self.deployment.account_id.clone(),
                api_token_set: self.deployment.api_token.is_some(),
            },
        }
    }

    /// Convert to WizardState for execution.
    pub fn to_wizard_state(&self, db_path: PathBuf) -> Result<WizardState, ConfigError> {
        if self.normalized_target().as_str() != "local" {
            return Err(ConfigError::Validation(format!(
                "unsupported_deployment_target: deployment.target '{}' is no longer supported; \
                 cass runs fully local-only.",
                self.deployment.target
            )));
        }

        // Convert workspaces
        let workspaces = if self.filters.workspaces.is_empty() {
            None
        } else {
            Some(self.filters.workspaces.iter().map(PathBuf::from).collect())
        };

        Ok(WizardState {
            agents: self.filters.agents.clone(),
            time_range: self.resolved_time_range(),
            workspaces,
            title: self.bundle.title.clone(),
            description: self.bundle.description.clone(),
            hide_metadata: self.bundle.hide_metadata,
            share_profile: self.bundle.share_profile,
            output_dir: PathBuf::from(&self.deployment.output_dir),
            db_path,
            exclusions: Default::default(),
            last_summary: None,
            secret_scan_has_findings: false,
            secret_scan_has_critical: false,
            secret_scan_count: 0,
            final_site_dir: None,
        })
    }

    /// Parse path mode from config.
    pub fn path_mode(&self) -> PathMode {
        match self.resolved_path_mode().as_str() {
            "basename" => PathMode::Basename,
            "full" => PathMode::Full,
            "hash" => PathMode::Hash,
            _ => PathMode::Relative,
        }
    }

    /// Get since timestamp.
    pub fn since_ts(&self) -> Option<i64> {
        self.filters.since.as_deref().and_then(parse_time_input)
    }

    /// Get until timestamp.
    pub fn until_ts(&self) -> Option<i64> {
        self.filters.until.as_deref().and_then(parse_time_input)
    }
}

/// Generate example configuration JSON.
pub fn example_config() -> &'static str {
    r#"{
  "filters": {
    "agents": ["claude-code", "codex"],
    "since": "30 days ago",
    "until": null,
    "workspaces": [],
    "path_mode": "relative"
  },
  "bundle": {
    "title": "My Archive",
    "description": "Searchable cass export",
    "hide_metadata": false,
    "share_profile": "public"
  },
  "deployment": {
    "target": "local",
    "output_dir": "./cass-export"
  }
}"#
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_test_config() -> PagesConfig {
        PagesConfig::default()
    }

    #[test]
    fn test_parse_minimal_config() {
        let json = r#"{"bundle": {"title": "My Archive"}}"#;
        let config: PagesConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.bundle.title, "My Archive");
    }

    #[test]
    fn test_parse_full_config() {
        let json = example_config();
        let config: PagesConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.filters.agents, vec!["claude-code", "codex"]);
        assert_eq!(config.bundle.title, "My Archive");
        assert_eq!(config.deployment.target, "local");
        assert_eq!(config.bundle.share_profile, Some(ShareProfile::Public));
    }

    /// 2l1b0.60: bundle.share_profile reaches the export state; absent, the
    /// export falls back to public; custom and unknown names are refused
    /// rather than silently ignored.
    #[test]
    fn share_profile_config_reaches_the_export_and_refuses_unusable_values() {
        let mut config = default_test_config();
        let state = config.to_wizard_state(PathBuf::from("/tmp/a.db")).unwrap();
        assert_eq!(state.share_profile, None);
        assert_eq!(state.effective_share_profile(), ShareProfile::Public);

        config.bundle.share_profile = Some(ShareProfile::Personal);
        let state = config.to_wizard_state(PathBuf::from("/tmp/a.db")).unwrap();
        assert_eq!(state.effective_share_profile(), ShareProfile::Personal);
        assert!(config.validate().valid, "{:?}", config.validate().errors);

        config.bundle.share_profile = Some(ShareProfile::Custom);
        let result = config.validate();
        assert!(!result.valid);
        assert!(
            result
                .errors
                .iter()
                .any(|error| error.contains("bundle.share_profile 'custom'")),
            "{:?}",
            result.errors
        );

        let unknown = r#"{"bundle": {"share_profile": "everyone"}}"#;
        assert!(serde_json::from_str::<PagesConfig>(unknown).is_err());
    }

    #[test]
    fn test_validate_github_target_is_unsupported() {
        let mut config = default_test_config();
        config.deployment.target = "github".to_string();
        let result = config.validate();
        assert!(!result.valid);
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.contains("unsupported_deployment_target"))
        );
        // The config itself is diagnosed, not rewritten.
        assert_eq!(config.deployment.target, "github");
    }

    #[test]
    fn test_validate_cloudflare_target_is_unsupported() {
        let mut config = default_test_config();
        config.deployment.target = "cloudflare".to_string();

        let result = config.validate();
        assert!(!result.valid);
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.contains("unsupported_deployment_target"))
        );
    }

    #[test]
    fn test_validate_legacy_hosting_keys_warn_but_do_not_fail() {
        let mut config = default_test_config();
        config.deployment.repo = Some("my-archive".to_string());
        config.deployment.branch = Some("gh-pages".to_string());
        config.deployment.account_id = Some("acc".to_string());
        config.deployment.api_token = Some("token".to_string());

        let result = config.validate();
        assert!(result.valid, "{:?}", result.errors);
        assert!(
            result
                .warnings
                .iter()
                .any(|w| w.contains("ignored") && w.contains("hosted publishing"))
        );
    }

    #[test]
    fn test_validate_blank_output_dir() {
        let mut config = default_test_config();
        config.deployment.output_dir = "   ".to_string();

        let result = config.validate();
        assert!(!result.valid);
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.contains("output_dir") && e.contains("empty"))
        );
    }

    #[test]
    fn test_env_var_resolution() {
        // SAFETY: This test runs in isolation and the env var is cleaned up after use
        unsafe { std::env::set_var("TEST_OUTPUT_DIR_VAR", "/tmp/cass-out") };
        let mut config = PagesConfig::default();
        config.deployment.output_dir = "env:TEST_OUTPUT_DIR_VAR".to_string();
        config.resolve_env_vars().unwrap();
        assert_eq!(config.deployment.output_dir, "/tmp/cass-out");
        // SAFETY: Cleanup of test env var
        unsafe { std::env::remove_var("TEST_OUTPUT_DIR_VAR") };
    }

    #[test]
    fn test_env_var_resolution_ignores_removed_hosting_credentials() {
        // Hosted-publishing credentials are diagnosed, never dereferenced:
        // `env:`-prefixed values must stay literal so a missing variable
        // cannot break an otherwise local export.
        let mut config = PagesConfig::default();
        config.deployment.account_id = Some("env:TEST_CF_ACCOUNT_ID_MISSING".to_string());
        config.deployment.api_token = Some("env:TEST_CF_API_TOKEN_MISSING".to_string());
        config.resolve_env_vars().unwrap();

        assert_eq!(
            config.deployment.account_id,
            Some("env:TEST_CF_ACCOUNT_ID_MISSING".to_string())
        );
        assert_eq!(
            config.deployment.api_token,
            Some("env:TEST_CF_API_TOKEN_MISSING".to_string())
        );
    }

    #[test]
    fn test_env_var_not_found() {
        let mut config = PagesConfig::default();
        config.deployment.output_dir = "env:NONEXISTENT_VAR_12345".to_string();
        let result = config.resolve_env_vars();
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_path_mode() {
        let mut config = default_test_config();
        config.filters.path_mode = Some("invalid".to_string());
        let result = config.validate();
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.contains("path_mode")));
    }

    #[test]
    fn test_invalid_deploy_target() {
        let mut config = default_test_config();
        config.deployment.target = "invalid".to_string();
        let result = config.validate();
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.contains("target")));
    }

    #[test]
    fn test_validate_partial_cloudflare_credentials() {
        let mut config = default_test_config();
        config.deployment.target = "cloudflare".to_string();
        config.deployment.account_id = Some("acc-only".to_string());

        let result = config.validate();
        assert!(!result.valid);
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.contains("unsupported_deployment_target"))
        );
    }

    /// 019i2: hide_metadata forces the obfuscating hash policy regardless of
    /// the configured path_mode; with it off, path_mode stays explicit.
    #[test]
    fn hide_metadata_forces_hash_path_mode() {
        let mut config = PagesConfig::default();
        config.filters.path_mode = Some("full".to_string());
        config.bundle.hide_metadata = false;
        assert!(matches!(config.path_mode(), PathMode::Full));
        config.bundle.hide_metadata = true;
        assert!(matches!(config.path_mode(), PathMode::Hash));
        config.filters.path_mode = None;
        assert!(matches!(config.path_mode(), PathMode::Hash));
    }

    #[test]
    fn test_path_mode_parsing() {
        let mut config = PagesConfig::default();

        config.filters.path_mode = None;
        assert!(matches!(config.path_mode(), PathMode::Relative));

        config.filters.path_mode = Some("basename".to_string());
        assert!(matches!(config.path_mode(), PathMode::Basename));

        config.filters.path_mode = Some("full".to_string());
        assert!(matches!(config.path_mode(), PathMode::Full));

        config.filters.path_mode = Some("hash".to_string());
        assert!(matches!(config.path_mode(), PathMode::Hash));

        // Parsing should be case-insensitive and whitespace-tolerant, matching validation.
        config.filters.path_mode = Some("Basename".to_string());
        assert!(matches!(config.path_mode(), PathMode::Basename));

        config.filters.path_mode = Some(" FULL ".to_string());
        assert!(matches!(config.path_mode(), PathMode::Full));

        config.filters.path_mode = Some("   ".to_string());
        assert!(matches!(config.path_mode(), PathMode::Relative));
    }

    #[test]
    fn test_validate_path_mode_trims_whitespace() {
        let mut config = default_test_config();
        config.filters.path_mode = Some(" FULL ".to_string());

        let result = config.validate();
        assert!(result.valid, "{:?}", result.errors);

        let resolved = result.resolved.expect("resolved config should exist");
        assert_eq!(resolved.filters.path_mode, "full");
    }

    #[test]
    fn test_resolved_config_applies_export_defaults() {
        let config = default_test_config();

        let result = config.validate();
        assert!(result.valid, "{:?}", result.errors);

        let resolved = result.resolved.expect("resolved config should exist");
        assert_eq!(resolved.filters.path_mode, DEFAULT_PATH_MODE);
    }

    #[test]
    fn test_validate_target_trims_whitespace() {
        let mut config = default_test_config();
        config.deployment.target = " Local ".to_string();

        let result = config.validate();
        assert!(result.valid, "{:?}", result.errors);

        let resolved = result.resolved.expect("resolved config should exist");
        assert_eq!(resolved.deployment.target, "local");
    }

    #[test]
    fn test_to_wizard_state_rejects_hosted_target() {
        let mut config = default_test_config();
        config.deployment.target = " cloudflare ".to_string();

        let err = config
            .to_wizard_state(PathBuf::from("/tmp/test.db"))
            .expect_err("hosted target must be refused");
        assert!(err.to_string().contains("unsupported_deployment_target"));
    }

    #[test]
    fn test_resolved_time_range_priority() {
        let mut config = PagesConfig::default();

        assert_eq!(config.resolved_time_range(), None);

        config.filters.since = Some("30 days ago".to_string());
        assert_eq!(
            config.resolved_time_range(),
            Some("since 30 days ago".to_string())
        );

        config.filters.until = Some("today".to_string());
        assert_eq!(
            config.resolved_time_range(),
            Some("30 days ago to today".to_string())
        );

        config.filters.since = None;
        assert_eq!(
            config.resolved_time_range(),
            Some("until today".to_string())
        );
    }
}
