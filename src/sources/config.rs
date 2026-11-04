//! Configuration types for sources.
//!
//! This module defines the data structures for source configuration, stored
//! in TOML format at `~/.config/cass/sources.toml` (or XDG equivalent). In the
//! local-only build, `type = "local"` entries name extra local directories to
//! index. Historical `type = "ssh"` entries deserialize unchanged so their
//! archived mirror directories and indexed rows stay searchable; they are
//! inert records — cass never opens a remote connection for them.
//!
//! # Example Configuration
//!
//! ```toml
//! [[sources]]
//! name = "archive-box"
//! type = "local"
//! paths = ["/Volumes/old-sessions"]
//!
//! # A legacy remote entry: kept as an inert archive record. Its `host`,
//! # `sync_schedule` and `platform` fields are preserved on rewrites.
//! [[sources]]
//! name = "laptop"
//! type = "ssh"
//! host = "user@laptop.local"
//! paths = ["~/.claude/projects"]
//! sync_schedule = "manual"
//!
//! # Path mappings rewrite archived remote workspace paths to local
//! # equivalents when opening hits in an editor.
//! [[sources.path_mappings]]
//! from = "/home/user/projects"
//! to = "/Users/me/projects"
//!
//! # Agent-specific mappings only apply when viewing specific agent sessions
//! [[sources.path_mappings]]
//! from = "/opt/work"
//! to = "/Volumes/Work"
//! agents = ["claude-code"]
//!
//! # Disable noisy connectors globally, including the built-in local source.
//! disabled_agents = ["openclaw"]
//! ```

use serde::{Deserialize, Serialize};
use std::path::{Component, Path, PathBuf};
use thiserror::Error;

use super::provenance::SourceKind;

// Re-export types from franken_agent_detection.
pub use franken_agent_detection::{PathMapping, Platform};

const BUILT_IN_LOCAL_SOURCE_NAME: &str = "local";

pub(crate) fn source_name_key(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

pub(crate) fn source_names_equal(lhs: &str, rhs: &str) -> bool {
    source_name_key(lhs) == source_name_key(rhs)
}

pub(crate) fn agent_name_key(name: &str) -> String {
    name.trim().to_ascii_lowercase().replace('-', "_")
}

fn normalize_agent_config_name(name: &str) -> Option<String> {
    let normalized = match agent_name_key(name).as_str() {
        "claude_code" => "claude".to_string(),
        "oh_my_pi" | "ohmypi" => "omp".to_string(),
        "open_claw" => "openclaw".to_string(),
        other => other.to_string(),
    };
    (!normalized.is_empty()).then_some(normalized)
}

fn agent_config_names_equal(lhs: &str, rhs: &str) -> bool {
    match (
        normalize_agent_config_name(lhs),
        normalize_agent_config_name(rhs),
    ) {
        (Some(lhs), Some(rhs)) => lhs == rhs,
        _ => false,
    }
}

fn path_mapping_applies_to_agent(mapping: &PathMapping, agent: Option<&str>) -> bool {
    match (
        mapping.agents.as_ref(),
        agent.and_then(|value| {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then_some(trimmed)
        }),
    ) {
        (Some(agents), _) if agents.is_empty() => false,
        (None, _) | (Some(_), None) => true,
        (Some(agents), Some(actual)) => agents
            .iter()
            .any(|allowed| agent_config_names_equal(allowed, actual)),
    }
}

/// Errors that can occur when loading or saving source configuration.
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    Read(#[from] std::io::Error),

    #[error("Failed to parse config file: {0}")]
    Parse(#[from] toml::de::Error),

    #[error("Failed to serialize config: {0}")]
    Serialize(#[from] toml::ser::Error),

    #[error("Could not determine config directory")]
    NoConfigDir,

    #[error("Validation error: {0}")]
    Validation(String),
}

/// Root configuration containing all source definitions.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SourcesConfig {
    /// List of configured sources.
    #[serde(default)]
    pub sources: Vec<SourceDefinition>,

    /// Connectors to skip during indexing even if their files exist locally or
    /// in configured remote mirrors.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disabled_agents: Vec<String>,
}

/// Definition of a single source (local or remote).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceDefinition {
    /// Friendly name for this source (e.g., "laptop", "workstation").
    /// This becomes the `source_id` used throughout the system.
    pub name: String,

    /// Source kind. `local` entries add extra directories to index;
    /// `ssh` entries are inert legacy archive records.
    #[serde(rename = "type", default)]
    pub source_type: SourceKind,

    /// Legacy remote host (e.g., "user@laptop.local"). Inert: kept so old
    /// entries round-trip and archived rows keep their provenance.
    #[serde(default)]
    pub host: Option<String>,

    /// Paths associated with this source.
    /// For local sources these are extra directories to index.
    /// For legacy remote entries they identify the original remote paths.
    /// Supports ~ expansion.
    #[serde(default)]
    pub paths: Vec<String>,

    /// Legacy field: retained for config compatibility only, never executed.
    #[serde(default)]
    pub sync_schedule: SyncSchedule,

    /// Path mappings for workspace rewriting.
    /// Maps remote paths to local equivalents.
    /// Example: "/home/user/projects" -> "/Users/me/projects"
    #[serde(default)]
    pub path_mappings: Vec<PathMapping>,

    /// Platform hint for default paths (macos, linux).
    #[serde(default)]
    pub platform: Option<Platform>,
}

impl SourceDefinition {
    /// Create a new local source definition.
    pub fn local(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            source_type: SourceKind::Local,
            ..Default::default()
        }
    }

    /// Check if this is a legacy remote record (`type = "ssh"`).
    ///
    /// Remote synchronization was removed: such entries are inert archive
    /// records whose previously mirrored conversations stay searchable, and
    /// whose fields are preserved verbatim on config rewrites.
    pub fn is_remote(&self) -> bool {
        matches!(self.source_type, SourceKind::Ssh)
    }

    /// Validate the source definition.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validate_structure()?;
        self.validate_paths()
    }

    pub(crate) fn validate_name(&self) -> Result<(), ConfigError> {
        validate_source_name(&self.name)
    }

    pub(crate) fn validate_structure(&self) -> Result<(), ConfigError> {
        self.validate_name()?;

        for (idx, mapping) in self.path_mappings.iter().enumerate() {
            if mapping.from.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "path_mappings[{idx}].from cannot be empty"
                )));
            }

            if mapping.to.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "path_mappings[{idx}].to cannot be empty"
                )));
            }

            if let Some(agents) = mapping.agents.as_ref() {
                if agents.is_empty() {
                    return Err(ConfigError::Validation(format!(
                        "path_mappings[{idx}].agents cannot be empty"
                    )));
                }

                if agents.iter().any(|agent| agent.trim().is_empty()) {
                    return Err(ConfigError::Validation(format!(
                        "path_mappings[{idx}].agents cannot contain empty agent names"
                    )));
                }
            }
        }

        Ok(())
    }

    fn validate_paths(&self) -> Result<(), ConfigError> {
        for (idx, path) in self.paths.iter().enumerate() {
            validate_source_path_entry(idx, path)?;
        }

        Ok(())
    }

    /// Apply path mapping to rewrite a workspace path.
    ///
    /// Uses longest-prefix matching. If an agent is specified,
    /// only mappings that apply to that agent are considered.
    pub fn rewrite_path(&self, path: &str) -> String {
        self.rewrite_path_for_agent(path, None)
    }

    /// Apply path mapping for a specific agent.
    ///
    /// Uses longest-prefix matching, filtering by agent.
    pub fn rewrite_path_for_agent(&self, path: &str, agent: Option<&str>) -> String {
        // Sort by prefix length descending for longest-prefix match
        let mut mappings: Vec<_> = self
            .path_mappings
            .iter()
            .filter(|m| path_mapping_applies_to_agent(m, agent))
            .collect();
        mappings.sort_by_key(|m| std::cmp::Reverse(m.from.len()));

        for mapping in mappings {
            if let Some(rewritten) = mapping.apply(path) {
                return rewritten;
            }
        }

        path.to_string()
    }
}

fn has_dot_components(path: &Path) -> bool {
    path.components()
        .any(|c| matches!(c, Component::CurDir | Component::ParentDir))
}

fn validate_source_name(name: &str) -> Result<(), ConfigError> {
    if name.trim().is_empty() {
        return Err(ConfigError::Validation(
            "Source name cannot be empty".into(),
        ));
    }

    if name.trim() != name {
        return Err(ConfigError::Validation(
            "Source name cannot have leading or trailing whitespace".into(),
        ));
    }

    if source_names_equal(name, BUILT_IN_LOCAL_SOURCE_NAME) {
        return Err(ConfigError::Validation(format!(
            "Source name '{}' is reserved for the built-in local source",
            BUILT_IN_LOCAL_SOURCE_NAME
        )));
    }

    if name.contains('/') || name.contains('\\') {
        return Err(ConfigError::Validation(
            "Source name cannot contain path separators".into(),
        ));
    }

    // Source names become mirror-directory components on every platform.
    // Windows drive-relative prefixes such as C: discard the base on join.
    if name.contains(':') {
        return Err(ConfigError::Validation(
            "Source name cannot contain ':'".into(),
        ));
    }

    if has_dot_components(Path::new(name)) {
        return Err(ConfigError::Validation(
            "Source name cannot be '.' or '..'".into(),
        ));
    }

    Ok(())
}

pub(crate) fn source_path_entry_error(index: usize, path: &str) -> Option<String> {
    if path.trim().is_empty() {
        return Some(format!("paths[{index}] cannot be empty"));
    }

    if path.trim() != path {
        return Some(format!(
            "paths[{index}] cannot have leading or trailing whitespace"
        ));
    }

    if path.chars().any(char::is_control) {
        return Some(format!("paths[{index}] cannot contain control characters"));
    }

    None
}

fn validate_source_path_entry(index: usize, path: &str) -> Result<(), ConfigError> {
    match source_path_entry_error(index, path) {
        Some(message) => Err(ConfigError::Validation(message)),
        None => Ok(()),
    }
}

/// Legacy sync schedule retained only so old `sources.toml` files still
/// deserialize. It has no effect: remote synchronization was removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SyncSchedule {
    /// Only sync when explicitly requested.
    #[default]
    Manual,
    /// Sync every hour.
    Hourly,
    /// Sync once per day.
    Daily,
}

const SYNC_SCHEDULE_MANUAL: &str = "manual";
const SYNC_SCHEDULE_HOURLY: &str = "hourly";
const SYNC_SCHEDULE_DAILY: &str = "daily";

impl std::fmt::Display for SyncSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Manual => SYNC_SCHEDULE_MANUAL,
            Self::Hourly => SYNC_SCHEDULE_HOURLY,
            Self::Daily => SYNC_SCHEDULE_DAILY,
        })
    }
}

impl SourcesConfig {
    /// Load configuration from the default location.
    ///
    /// Returns an empty config if the file doesn't exist.
    pub fn load() -> Result<Self, ConfigError> {
        let config_path = Self::config_path()?;

        if !config_path.exists() {
            return Ok(Self::default());
        }

        let content = std::fs::read_to_string(&config_path)?;
        let config: Self = toml::from_str(&content)?;

        config.validate_for_load()?;

        Ok(config)
    }

    /// Load configuration from a specific path.
    pub fn load_from(path: &PathBuf) -> Result<Self, ConfigError> {
        if !path.exists() {
            return Ok(Self::default());
        }

        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        config.validate_for_load()?;

        Ok(config)
    }

    /// Save configuration to the default location.
    pub fn save(&self) -> Result<(), ConfigError> {
        let config_path = Self::config_path()?;

        // Create parent directories if needed
        if let Some(parent) = config_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        self.validate()?;
        let content = toml::to_string_pretty(self)?;
        let _: SourcesConfig = toml::from_str(&content)?;
        let temp_path = write_sources_config_temp_file(&config_path, content.as_bytes())?;
        replace_file_from_temp(&temp_path, &config_path)?;

        Ok(())
    }

    /// Save configuration to a specific path.
    pub fn save_to(&self, path: &Path) -> Result<(), ConfigError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        self.validate()?;
        let content = toml::to_string_pretty(self)?;
        let _: SourcesConfig = toml::from_str(&content)?;
        let temp_path = write_sources_config_temp_file(path, content.as_bytes())?;
        replace_file_from_temp(&temp_path, path)?;

        Ok(())
    }

    /// Get the default configuration file path.
    ///
    /// Uses XDG conventions:
    /// - Primary: `$XDG_CONFIG_HOME/cass/sources.toml`
    /// - Fallback: platform-specific config dir (e.g., `~/.config/cass/sources.toml` on Linux)
    pub fn config_path() -> Result<PathBuf, ConfigError> {
        config_path_from_parts(
            dotenvy::var("XDG_CONFIG_HOME").ok().map(PathBuf::from),
            dirs::config_dir(),
            dirs::home_dir(),
        )
    }

    /// Validate all sources in the configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validate_with_path_entries(true)
    }

    fn validate_for_load(&self) -> Result<(), ConfigError> {
        self.validate_with_path_entries(false)
    }

    fn validate_with_path_entries(&self, validate_paths: bool) -> Result<(), ConfigError> {
        // Check for duplicate names
        let mut seen_names = std::collections::HashSet::new();
        for source in &self.sources {
            if validate_paths {
                source.validate()?;
            } else {
                source.validate_structure()?;
            }

            if !seen_names.insert(source_name_key(&source.name)) {
                return Err(ConfigError::Validation(format!(
                    "Duplicate source name: {}",
                    source.name
                )));
            }
        }

        for (idx, agent) in self.disabled_agents.iter().enumerate() {
            if normalize_agent_config_name(agent).is_none() {
                return Err(ConfigError::Validation(format!(
                    "disabled_agents[{idx}] cannot be empty"
                )));
            }
        }

        Ok(())
    }

    /// Find a source by name.
    pub fn find_source(&self, name: &str) -> Option<&SourceDefinition> {
        self.sources
            .iter()
            .find(|s| source_names_equal(&s.name, name))
    }

    /// Find a source by name (mutable).
    pub fn find_source_mut(&mut self, name: &str) -> Option<&mut SourceDefinition> {
        self.sources
            .iter_mut()
            .find(|s| source_names_equal(&s.name, name))
    }

    /// Add a new source. Returns error if name already exists.
    pub fn add_source(&mut self, source: SourceDefinition) -> Result<(), ConfigError> {
        source.validate()?;

        if self
            .sources
            .iter()
            .any(|s| source_names_equal(&s.name, &source.name))
        {
            return Err(ConfigError::Validation(format!(
                "Source '{}' already exists",
                source.name
            )));
        }

        self.sources.push(source);
        Ok(())
    }

    /// Remove a source by name. Returns true if found and removed.
    pub fn remove_source(&mut self, name: &str) -> bool {
        let initial_len = self.sources.len();
        self.sources.retain(|s| !source_names_equal(&s.name, name));
        self.sources.len() < initial_len
    }

    /// Iterate legacy remote entries (`type = "ssh"`).
    ///
    /// These are inert archive records kept for provenance and source
    /// filtering — no connection is ever opened for them.
    pub fn remote_sources(&self) -> impl Iterator<Item = &SourceDefinition> {
        self.sources.iter().filter(|s| s.is_remote())
    }

    pub fn configured_disabled_agents(&self) -> Vec<String> {
        let mut disabled = self
            .disabled_agents
            .iter()
            .filter_map(|agent| normalize_agent_config_name(agent))
            .collect::<Vec<_>>();
        disabled.sort();
        disabled.dedup();
        disabled
    }

    pub fn is_agent_disabled(&self, agent: &str) -> bool {
        let Some(normalized) = normalize_agent_config_name(agent) else {
            return false;
        };
        self.disabled_agents
            .iter()
            .filter_map(|candidate| normalize_agent_config_name(candidate))
            .any(|candidate| candidate == normalized)
    }

    pub fn exclude_agent_from_indexing(&mut self, agent: &str) -> Result<bool, ConfigError> {
        let normalized = normalize_agent_config_name(agent)
            .ok_or_else(|| ConfigError::Validation("agent name cannot be empty".into()))?;
        if self.is_agent_disabled(&normalized) {
            return Ok(false);
        }
        self.disabled_agents.push(normalized);
        Ok(true)
    }

    pub fn include_agent_in_indexing(&mut self, agent: &str) -> Result<bool, ConfigError> {
        let normalized = normalize_agent_config_name(agent)
            .ok_or_else(|| ConfigError::Validation("agent name cannot be empty".into()))?;
        let initial_len = self.disabled_agents.len();
        self.disabled_agents.retain(|existing| {
            normalize_agent_config_name(existing).as_deref() != Some(&normalized)
        });
        Ok(self.disabled_agents.len() != initial_len)
    }
}

fn config_path_from_parts(
    xdg_config_home: Option<PathBuf>,
    platform_config_dir: Option<PathBuf>,
    home_dir: Option<PathBuf>,
) -> Result<PathBuf, ConfigError> {
    // Respect XDG_CONFIG_HOME first (important for testing and Linux users).
    if let Some(xdg_config) = xdg_config_home {
        return Ok(xdg_config.join("cass").join("sources.toml"));
    }

    // Check the platform-specific config dir (e.g. ~/Library/Application Support/ on macOS).
    let platform_path = platform_config_dir.map(|p| p.join("cass").join("sources.toml"));
    if let Some(ref path) = platform_path
        && path.exists()
    {
        return Ok(path.clone());
    }

    // Fallback: check ~/.config/cass/sources.toml for users who follow XDG
    // conventions without setting XDG_CONFIG_HOME.
    if let Some(home) = home_dir {
        let dot_config_path = home.join(".config").join("cass").join("sources.toml");
        if dot_config_path.exists() {
            return Ok(dot_config_path);
        }
    }

    // Neither exists: return the platform path for creation (original behavior).
    platform_path.ok_or(ConfigError::NoConfigDir)
}

fn replace_file_from_temp(temp_path: &Path, final_path: &Path) -> Result<(), std::io::Error> {
    #[cfg(windows)]
    {
        match std::fs::rename(temp_path, final_path) {
            Ok(()) => sync_parent_directory(final_path),
            Err(first_err)
                if path_entry_exists(final_path)
                    && matches!(
                        first_err.kind(),
                        std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::PermissionDenied
                    ) =>
            {
                let backup_path = unique_replace_backup_path(final_path);
                std::fs::rename(final_path, &backup_path).map_err(|backup_err| {
                    std::io::Error::other(format!(
                        "failed preparing backup {} before replacing {}: first error: {}; backup error: {}",
                        backup_path.display(),
                        final_path.display(),
                        first_err,
                        backup_err
                    ))
                })?;
                match std::fs::rename(temp_path, final_path) {
                    Ok(()) => sync_parent_directory(final_path),
                    Err(second_err) => {
                        let restore_result = std::fs::rename(&backup_path, final_path);
                        match restore_result {
                            Ok(()) => {
                                sync_parent_directory(final_path).map_err(|sync_err| {
                                    std::io::Error::other(format!(
                                        "failed replacing {} with {}: first error: {}; second error: {}; restored original file but failed syncing parent directory: {}",
                                        final_path.display(),
                                        temp_path.display(),
                                        first_err,
                                        second_err,
                                        sync_err
                                    ))
                                })?;
                                Err(std::io::Error::new(
                                    second_err.kind(),
                                    format!(
                                        "failed replacing {} with {}: first error: {}; second error: {}; restored original file",
                                        final_path.display(),
                                        temp_path.display(),
                                        first_err,
                                        second_err
                                    ),
                                ))
                            }
                            Err(restore_err) => Err(std::io::Error::other(format!(
                                "failed replacing {} with {}: first error: {}; second error: {}; restore error: {}; temp file retained at {}",
                                final_path.display(),
                                temp_path.display(),
                                first_err,
                                second_err,
                                restore_err,
                                temp_path.display()
                            ))),
                        }
                    }
                }
            }
            Err(rename_err) => Err(rename_err),
        }
    }

    #[cfg(not(windows))]
    {
        std::fs::rename(temp_path, final_path)?;
        sync_parent_directory(final_path)
    }
}

#[cfg(any(windows, test))]
fn path_entry_exists(path: &Path) -> bool {
    match std::fs::symlink_metadata(path) {
        Ok(_) => true,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => true,
    }
}

#[cfg(not(windows))]
fn sync_parent_directory(path: &Path) -> Result<(), std::io::Error> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    std::fs::File::open(parent)?.sync_all()
}

#[cfg(windows)]
fn sync_parent_directory(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

fn unique_atomic_temp_path(path: &Path) -> PathBuf {
    unique_atomic_sidecar_path(path, "tmp", "sources.toml")
}

fn write_sources_config_temp_file(path: &Path, contents: &[u8]) -> Result<PathBuf, std::io::Error> {
    for _ in 0..100 {
        let temp_path = unique_atomic_temp_path(path);
        match write_sources_config_temp_file_at(&temp_path, contents) {
            Ok(()) => return Ok(temp_path),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err),
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        format!(
            "failed to allocate unique sources config temp path for {}",
            path.display()
        ),
    ))
}

fn write_sources_config_temp_file_at(path: &Path, contents: &[u8]) -> Result<(), std::io::Error> {
    use std::io::Write;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(contents)?;
    file.sync_all()
}

#[cfg(windows)]
fn unique_replace_backup_path(path: &Path) -> PathBuf {
    unique_atomic_sidecar_path(path, "bak", "sources.toml")
}

fn unique_atomic_sidecar_path(path: &Path, suffix: &str, fallback_name: &str) -> PathBuf {
    static NEXT_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let nonce = NEXT_NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(fallback_name);

    path.with_file_name(format!(
        ".{file_name}.{suffix}.{}.{}.{}",
        std::process::id(),
        timestamp,
        nonce
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a legacy remote (`type = "ssh"`) record the way old sources.toml
    /// files deserialize it.
    fn ssh_source(name: &str, host: &str) -> SourceDefinition {
        SourceDefinition {
            name: name.into(),
            source_type: SourceKind::Ssh,
            host: Some(host.into()),
            ..Default::default()
        }
    }

    #[test]
    fn test_empty_config_default() {
        let config = SourcesConfig::default();
        assert!(config.sources.is_empty());
    }

    #[test]
    fn test_replace_file_from_temp_overwrites_existing_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let final_path = temp.path().join("sources.toml");
        let first_tmp = temp.path().join("first.tmp");
        let second_tmp = temp.path().join("second.tmp");

        std::fs::write(&first_tmp, "first = true\n").expect("write first temp");
        replace_file_from_temp(&first_tmp, &final_path).expect("initial replace");
        assert_eq!(
            std::fs::read_to_string(&final_path).expect("read first final"),
            "first = true\n"
        );

        std::fs::write(&second_tmp, "second = true\n").expect("write second temp");
        replace_file_from_temp(&second_tmp, &final_path).expect("overwrite replace");
        assert_eq!(
            std::fs::read_to_string(&final_path).expect("read second final"),
            "second = true\n"
        );
    }

    #[test]
    fn test_unique_atomic_temp_path_changes_each_call() {
        let final_path = Path::new("/tmp/sources.toml");
        let first = unique_atomic_temp_path(final_path);
        let second = unique_atomic_temp_path(final_path);

        assert_ne!(first, second);
        assert_eq!(first.parent(), final_path.parent());
        assert_eq!(second.parent(), final_path.parent());
    }

    #[cfg(unix)]
    #[test]
    fn test_sources_config_temp_write_refuses_existing_symlink() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let protected = temp.path().join("protected.toml");
        let temp_path = temp.path().join(".sources.toml.tmp");

        std::fs::write(&protected, b"protected = true\n").expect("write protected target");
        symlink(&protected, &temp_path).expect("create temp symlink");

        let err = write_sources_config_temp_file_at(&temp_path, b"[[sources]]\n")
            .expect_err("existing temp symlink must be rejected");

        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(
            std::fs::read(&protected).expect("read protected target"),
            b"protected = true\n"
        );
        assert!(
            std::fs::symlink_metadata(&temp_path)
                .expect("temp path metadata")
                .file_type()
                .is_symlink(),
            "failed temp write should leave the existing symlink untouched"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_replace_file_from_temp_replaces_symlink_without_following() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let final_path = temp.path().join("sources.toml");
        let protected = temp.path().join("protected.toml");
        let temp_path = temp.path().join("sources.tmp");

        std::fs::write(&protected, b"protected = true\n").expect("write protected target");
        symlink(&protected, &final_path).expect("create final symlink");
        std::fs::write(&temp_path, b"new_config = true\n").expect("write replacement temp");

        replace_file_from_temp(&temp_path, &final_path).expect("replace symlink path");

        assert_eq!(
            std::fs::read(&protected).expect("read protected target"),
            b"protected = true\n"
        );
        assert!(
            !std::fs::symlink_metadata(&final_path)
                .expect("final path metadata")
                .file_type()
                .is_symlink(),
            "replace should publish at the symlink path instead of following it"
        );
        assert_eq!(
            std::fs::read(&final_path).expect("read replacement config"),
            b"new_config = true\n"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_path_entry_exists_detects_dangling_symlink() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("sources.toml");
        let missing_target = temp.path().join("missing.toml");

        symlink(&missing_target, &path).expect("create dangling symlink");

        assert!(!path.exists(), "Path::exists follows the missing target");
        assert!(
            path_entry_exists(&path),
            "replacement fallback must detect the symlink path entry itself"
        );
    }

    #[test]
    fn test_config_path_from_parts_prefers_xdg_config_home() {
        let temp = tempfile::tempdir().expect("tempdir");
        let xdg_config_home = temp.path().join("xdg-config");
        let platform_config_dir = temp.path().join("platform-config");
        let home_dir = temp.path().join("home");

        assert_eq!(
            config_path_from_parts(
                Some(xdg_config_home.clone()),
                Some(platform_config_dir),
                Some(home_dir)
            )
            .expect("path from xdg config home"),
            xdg_config_home.join("cass").join("sources.toml")
        );
    }

    #[test]
    fn test_config_path_from_parts_prefers_existing_platform_path_before_dot_config() {
        let temp = tempfile::tempdir().expect("tempdir");
        let platform_config_dir = temp.path().join("platform-config");
        let platform_path = platform_config_dir.join("cass").join("sources.toml");
        let home_dir = temp.path().join("home");
        let dot_config_path = home_dir.join(".config").join("cass").join("sources.toml");
        std::fs::create_dir_all(platform_path.parent().expect("platform parent")).unwrap();
        std::fs::create_dir_all(dot_config_path.parent().expect("dot-config parent")).unwrap();
        std::fs::write(&platform_path, "").unwrap();
        std::fs::write(&dot_config_path, "").unwrap();

        assert_eq!(
            config_path_from_parts(None, Some(platform_config_dir), Some(home_dir))
                .expect("existing platform path"),
            platform_path
        );
    }

    #[test]
    fn test_config_path_from_parts_uses_existing_dot_config_before_new_platform_path() {
        let temp = tempfile::tempdir().expect("tempdir");
        let platform_config_dir = temp.path().join("platform-config");
        let home_dir = temp.path().join("home");
        let dot_config_path = home_dir.join(".config").join("cass").join("sources.toml");
        std::fs::create_dir_all(dot_config_path.parent().expect("dot-config parent")).unwrap();
        std::fs::write(&dot_config_path, "").unwrap();

        assert_eq!(
            config_path_from_parts(None, Some(platform_config_dir), Some(home_dir))
                .expect("existing dot-config path"),
            dot_config_path
        );
    }

    #[test]
    fn test_source_definition_local() {
        let source = SourceDefinition::local("test");
        assert_eq!(source.name, "test");
        assert_eq!(source.source_type, SourceKind::Local);
        assert!(!source.is_remote());
    }

    #[test]
    fn test_legacy_remote_source_deserializes_as_inert_record() {
        let source = ssh_source("laptop", "user@laptop.local");
        assert_eq!(source.name, "laptop");
        assert_eq!(source.source_type, SourceKind::Ssh);
        assert_eq!(source.host, Some("user@laptop.local".into()));
        assert!(source.is_remote());
    }

    #[test]
    fn test_source_validation_empty_name() {
        let source = SourceDefinition::default();
        assert!(source.validate().is_err());

        let source = SourceDefinition::local("   ");
        assert!(source.validate().is_err());
    }

    #[test]
    fn test_source_validation_rejects_padded_names() {
        let source = SourceDefinition::local(" laptop");
        assert!(source.validate().is_err());

        let source = SourceDefinition::local("laptop ");
        assert!(source.validate().is_err());
    }

    #[test]
    fn test_source_validation_dot_names() {
        let source = SourceDefinition::local(".");
        assert!(source.validate().is_err());

        let source = SourceDefinition::local("..");
        assert!(source.validate().is_err());
    }

    #[test]
    fn test_source_validation_portable_names_reject_drive_prefixes() {
        let temp = tempfile::tempdir().expect("source-name fixture directory");
        let path = temp.path().join("sources.toml");
        for name in ["C:", "C:outside", "c:outside", "host:stream"] {
            let source = ssh_source(name, "user@host");
            assert!(source.validate().is_err(), "accepted source name {name}");
            let mut config = SourcesConfig::default();
            assert!(config.add_source(source.clone()).is_err());
            config.sources.push(source);
            assert!(config.validate().is_err());
            // Bypass save validation to exercise the actual TOML load path.
            std::fs::write(&path, toml::to_string(&config).expect("encode fixture"))
                .expect("write source-name fixture");
            assert!(SourcesConfig::load_from(&path).is_err(), "loaded {name}");
        }
        for name in ["laptop", "host-name.example", "build_host-1"] {
            let source = ssh_source(name, "user@host");
            assert!(source.validate().is_ok(), "rejected source name {name}");
            let mut config = SourcesConfig::default();
            config.add_source(source).expect("add portable source");
            config.save_to(&path).expect("save portable source");
            let loaded = SourcesConfig::load_from(&path).expect("load portable source");
            assert_eq!(loaded.sources.len(), 1);
            assert_eq!(loaded.sources[0].name, name);
        }
    }

    #[test]
    fn test_source_validation_reserved_local_name() {
        let source = ssh_source("local", "user@host");
        assert!(source.validate().is_err());

        let source = ssh_source("LOCAL", "user@host");
        assert!(source.validate().is_err());
    }

    #[test]
    fn test_legacy_remote_source_without_host_is_accepted() {
        // Old entries are inert records: a missing host is fine because no
        // connection is ever opened for them.
        let mut source = ssh_source("test", "host");
        source.host = None;
        assert!(source.validate().is_ok());
    }

    #[test]
    fn test_source_validation_rejects_invalid_paths() {
        for path in [
            "",
            "   ",
            " ~/.claude/projects",
            "~/.claude/projects ",
            "~/.claude\nprojects",
        ] {
            let mut source = ssh_source("test", "user@host");
            source.paths = vec![path.to_string()];
            assert!(
                source.validate().is_err(),
                "path should be rejected: {path:?}"
            );
        }

        let mut source = ssh_source("test", "user@host");
        source.paths = vec!["~/Library/Application Support/Cursor/User/globalStorage".to_string()];
        assert!(source.validate().is_ok());
    }

    #[test]
    fn test_load_from_preserves_invalid_paths_for_operation_level_reporting() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config_path = temp.path().join("sources.toml");
        std::fs::write(
            &config_path,
            r#"
[[sources]]
name = "laptop"
type = "ssh"
host = "user@host"
paths = [" ~/.claude/projects", "~/.codex/sessions"]
"#,
        )
        .expect("write config");

        let loaded = SourcesConfig::load_from(&config_path).expect("lenient load");
        assert_eq!(loaded.sources.len(), 1);
        assert_eq!(loaded.sources[0].paths[0], " ~/.claude/projects");
        assert_eq!(loaded.sources[0].paths[1], "~/.codex/sessions");
        assert!(
            loaded.validate().is_err(),
            "strict validation should still reject writing the malformed path"
        );
    }

    #[test]
    fn test_load_from_still_rejects_invalid_source_structure() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config_path = temp.path().join("sources.toml");
        std::fs::write(
            &config_path,
            r#"
[[sources]]
name = "bad:name"
type = "ssh"
host = "user@host"
paths = ["~/.claude/projects"]
"#,
        )
        .expect("write config");

        assert!(
            SourcesConfig::load_from(&config_path).is_err(),
            "lenient load is only for per-path validation, not invalid structure"
        );
    }

    #[test]
    fn test_source_validation_path_mapping_empty_from() {
        let mut source = SourceDefinition::local("test");
        source.path_mappings.push(PathMapping::new("", "/Users/me"));
        assert!(source.validate().is_err());

        source.path_mappings.clear();
        source
            .path_mappings
            .push(PathMapping::new("   ", "/Users/me"));
        assert!(source.validate().is_err());
    }

    #[test]
    fn test_source_validation_path_mapping_empty_to() {
        let mut source = SourceDefinition::local("test");
        source
            .path_mappings
            .push(PathMapping::new("/home/user", ""));
        assert!(source.validate().is_err());

        source.path_mappings.clear();
        source
            .path_mappings
            .push(PathMapping::new("/home/user", "   "));
        assert!(source.validate().is_err());
    }

    #[test]
    fn test_source_validation_path_mapping_empty_agent_names() {
        let mut source = SourceDefinition::local("test");
        source.path_mappings.push(PathMapping::with_agents(
            "/home/user",
            "/Users/me",
            vec!["claude-code".into(), "   ".into()],
        ));
        assert!(source.validate().is_err());
    }

    #[test]
    fn test_source_validation_path_mapping_empty_agents_list() {
        let mut source = SourceDefinition::local("test");
        source.path_mappings.push(PathMapping::with_agents(
            "/home/user",
            "/Users/me",
            Vec::new(),
        ));
        assert!(source.validate().is_err());
    }

    #[test]
    fn test_path_mapping_new() {
        let mapping = PathMapping::new("/home/user", "/Users/me");
        assert_eq!(mapping.from, "/home/user");
        assert_eq!(mapping.to, "/Users/me");
        assert!(mapping.agents.is_none());
    }

    #[test]
    fn test_path_mapping_with_agents() {
        let mapping = PathMapping::with_agents(
            "/home/user",
            "/Users/me",
            vec!["claude-code".into(), "cursor".into()],
        );
        assert_eq!(mapping.from, "/home/user");
        assert_eq!(mapping.to, "/Users/me");
        assert_eq!(
            mapping.agents,
            Some(vec!["claude-code".into(), "cursor".into()])
        );
    }

    #[test]
    fn test_path_mapping_apply() {
        let mapping = PathMapping::new("/home/user/projects", "/Users/me/projects");

        // Matching prefix
        assert_eq!(
            mapping.apply("/home/user/projects/myapp"),
            Some("/Users/me/projects/myapp".into())
        );

        // Non-matching prefix
        assert_eq!(mapping.apply("/opt/data"), None);

        // Partial match (not at start)
        assert_eq!(mapping.apply("/data/home/user/projects"), None);
    }

    #[test]
    fn test_path_mapping_applies_to_agent() {
        // This test pins the semantics of the *cass wrapper*
        // (`path_mapping_applies_to_agent`) rather than the upstream
        // `PathMapping::applies_to_agent` method. Cass intentionally uses a
        // permissive wrapper: when the caller doesn't specify an agent, even
        // mappings that are scoped to a specific agent still apply. Upstream
        // (`franken_agent_detection`) uses a stricter default (`(Some, None)
        // => false`) because its scan-time usage wants to skip
        // agent-specific mappings when the agent is unknown. Both semantics
        // are correct in their own context; cass's tests must exercise the
        // cass wrapper to avoid coupling to whichever default franken picks.

        // Mapping with no agent filter — applies in every case.
        let global = PathMapping::new("/home", "/Users");
        assert!(path_mapping_applies_to_agent(&global, None));
        assert!(path_mapping_applies_to_agent(&global, Some("claude-code")));
        assert!(path_mapping_applies_to_agent(&global, Some("any-agent")));

        // Mapping with agent filter.
        let filtered = PathMapping::with_agents("/home", "/Users", vec!["claude-code".into()]);
        // No agent specified → cass wrapper matches (permissive default).
        assert!(path_mapping_applies_to_agent(&filtered, None));
        // An explicitly empty allow-list is invalid config and should not match
        // defensively if one is constructed in code.
        let empty_filter = PathMapping::with_agents("/home", "/Users", Vec::new());
        assert!(!path_mapping_applies_to_agent(&empty_filter, None));
        // Agent matches the allow-list.
        assert!(path_mapping_applies_to_agent(
            &filtered,
            Some("claude-code")
        ));
        // Agent not in the allow-list.
        assert!(!path_mapping_applies_to_agent(&filtered, Some("cursor")));
        // Hyphen/underscore normalization: `claude_code` must match the
        // allow-list entry `claude-code` because cass normalizes agent slugs
        // before comparison.
        assert!(path_mapping_applies_to_agent(
            &filtered,
            Some("claude_code")
        ));
        assert!(path_mapping_applies_to_agent(&filtered, Some("claude")));

        let openclaw_filtered =
            PathMapping::with_agents("/home", "/Users", vec!["openclaw".into()]);
        assert!(path_mapping_applies_to_agent(
            &openclaw_filtered,
            Some("open-claw")
        ));
    }

    #[test]
    fn test_path_rewriting() {
        let mut source = SourceDefinition::local("test");
        source.path_mappings.push(PathMapping::new(
            "/home/user/projects",
            "/Users/me/projects",
        ));
        source
            .path_mappings
            .push(PathMapping::new("/home/user", "/Users/me"));

        // Longest prefix should match
        assert_eq!(
            source.rewrite_path("/home/user/projects/myapp"),
            "/Users/me/projects/myapp"
        );

        // Shorter prefix
        assert_eq!(source.rewrite_path("/home/user/other"), "/Users/me/other");

        // No match
        assert_eq!(source.rewrite_path("/opt/data"), "/opt/data");
    }

    #[test]
    fn test_path_rewriting_with_agent_filter() {
        let mut source = SourceDefinition::local("test");
        // Global mapping
        source
            .path_mappings
            .push(PathMapping::new("/home/user", "/Users/me"));
        // Agent-specific mapping
        source.path_mappings.push(PathMapping::with_agents(
            "/home/user/projects",
            "/Volumes/Work/projects",
            vec!["claude-code".into()],
        ));

        // Without agent filter, both mappings apply (longest match wins)
        assert_eq!(
            source.rewrite_path_for_agent("/home/user/projects/app", None),
            "/Volumes/Work/projects/app"
        );

        // With claude-code agent, use specific mapping
        assert_eq!(
            source.rewrite_path_for_agent("/home/user/projects/app", Some("claude-code")),
            "/Volumes/Work/projects/app"
        );
        assert_eq!(
            source.rewrite_path_for_agent("/home/user/projects/app", Some("claude")),
            "/Volumes/Work/projects/app"
        );

        // With cursor agent, falls back to global mapping
        assert_eq!(
            source.rewrite_path_for_agent("/home/user/projects/app", Some("cursor")),
            "/Users/me/projects/app"
        );

        // Non-matching path
        assert_eq!(
            source.rewrite_path_for_agent("/opt/data", Some("claude-code")),
            "/opt/data"
        );
    }

    #[test]
    fn test_config_duplicate_names() {
        let mut config = SourcesConfig::default();
        config.sources.push(SourceDefinition::local("test"));
        config.sources.push(SourceDefinition::local("test"));

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_duplicate_names_case_insensitive() {
        let mut config = SourcesConfig::default();
        config.sources.push(ssh_source("Laptop", "user@laptop"));
        config.sources.push(ssh_source("laptop", "user@other-host"));

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_source_name_keys_trim_and_ignore_case() {
        assert_eq!(source_name_key(" Laptop "), "laptop");
        assert!(source_names_equal(" Laptop ", "laptop"));
    }

    #[test]
    fn test_config_add_source() {
        let mut config = SourcesConfig::default();
        config.add_source(SourceDefinition::local("test")).unwrap();

        assert_eq!(config.sources.len(), 1);

        // Adding duplicate should fail
        assert!(config.add_source(SourceDefinition::local("test")).is_err());
    }

    #[test]
    fn test_config_add_source_case_insensitive_duplicate() {
        let mut config = SourcesConfig::default();
        config
            .add_source(ssh_source("Laptop", "user@laptop"))
            .unwrap();

        assert!(
            config
                .add_source(ssh_source("laptop", "user@other-host"))
                .is_err()
        );
    }

    #[test]
    fn test_config_remove_source() {
        let mut config = SourcesConfig::default();
        config.sources.push(SourceDefinition::local("test"));

        assert!(config.remove_source("test"));
        assert!(!config.remove_source("nonexistent"));
        assert!(config.sources.is_empty());
    }

    #[test]
    fn test_config_remove_source_case_insensitive() {
        let mut config = SourcesConfig::default();
        config.sources.push(ssh_source("Laptop", "user@laptop"));

        assert!(config.remove_source("laptop"));
        assert!(config.sources.is_empty());
    }

    #[test]
    fn test_find_source_case_insensitive() {
        let mut config = SourcesConfig::default();
        config.sources.push(ssh_source("Laptop", "user@laptop"));

        assert!(config.find_source("laptop").is_some());
        assert!(config.find_source("LAPTOP").is_some());
        assert!(config.find_source_mut("laptop").is_some());
    }

    #[test]
    fn test_config_serialization_roundtrip() {
        let mut config = SourcesConfig::default();
        config.sources.push(SourceDefinition {
            name: "laptop".into(),
            source_type: SourceKind::Ssh,
            host: Some("user@laptop.local".into()),
            paths: vec!["~/.claude/projects".into()],
            sync_schedule: SyncSchedule::Daily,
            path_mappings: vec![PathMapping::new("/home/user", "/Users/me")],
            platform: Some(Platform::Linux),
        });

        let serialized = toml::to_string_pretty(&config).unwrap();
        let deserialized: SourcesConfig = toml::from_str(&serialized).unwrap();

        assert_eq!(deserialized.sources.len(), 1);
        assert_eq!(deserialized.sources[0].name, "laptop");
        assert_eq!(deserialized.sources[0].sync_schedule, SyncSchedule::Daily);
        assert_eq!(deserialized.sources[0].path_mappings.len(), 1);
        assert_eq!(deserialized.sources[0].path_mappings[0].from, "/home/user");
        assert_eq!(deserialized.sources[0].path_mappings[0].to, "/Users/me");
    }

    #[test]
    fn test_path_mapping_serialization_with_agents() {
        let mut config = SourcesConfig::default();
        config.sources.push(SourceDefinition {
            name: "remote".into(),
            source_type: SourceKind::Ssh,
            host: Some("user@server".into()),
            paths: vec![],
            sync_schedule: SyncSchedule::Manual,
            path_mappings: vec![
                PathMapping::new("/home/user", "/Users/me"),
                PathMapping::with_agents("/opt/work", "/Volumes/Work", vec!["claude-code".into()]),
            ],
            platform: None,
        });

        let serialized = toml::to_string_pretty(&config).unwrap();
        let deserialized: SourcesConfig = toml::from_str(&serialized).unwrap();

        assert_eq!(deserialized.sources[0].path_mappings.len(), 2);
        // First mapping has no agents filter
        assert!(deserialized.sources[0].path_mappings[0].agents.is_none());
        // Second mapping has agents filter
        assert_eq!(
            deserialized.sources[0].path_mappings[1].agents,
            Some(vec!["claude-code".into()])
        );
    }

    #[test]
    fn test_sync_schedule_display() {
        assert_eq!(SyncSchedule::Manual.to_string(), SYNC_SCHEDULE_MANUAL);
        assert_eq!(SyncSchedule::Hourly.to_string(), SYNC_SCHEDULE_HOURLY);
        assert_eq!(SyncSchedule::Daily.to_string(), SYNC_SCHEDULE_DAILY);
    }

    #[test]
    fn test_exclude_and_include_agents_normalize_and_dedup() {
        let mut config = SourcesConfig::default();

        assert!(config.exclude_agent_from_indexing(" OpenClaw ").unwrap());
        assert!(!config.exclude_agent_from_indexing("open-claw").unwrap());
        assert!(config.is_agent_disabled("openclaw"));
        assert_eq!(config.configured_disabled_agents(), vec!["openclaw"]);

        assert!(config.include_agent_in_indexing("open_claw").unwrap());
        assert!(!config.is_agent_disabled("openclaw"));
        assert!(config.configured_disabled_agents().is_empty());
    }

    #[test]
    fn test_exclude_agent_aliases_collapse_to_internal_connector_slug() {
        let mut config = SourcesConfig::default();

        assert!(config.exclude_agent_from_indexing("claude-code").unwrap());
        assert!(config.is_agent_disabled("claude"));
        assert!(config.is_agent_disabled("claude_code"));
        assert!(config.exclude_agent_from_indexing("oh-my-pi").unwrap());
        assert!(!config.exclude_agent_from_indexing("oh_my_pi").unwrap());
        assert!(config.is_agent_disabled("omp"));
        assert!(config.is_agent_disabled("ohmypi"));
        assert_eq!(config.configured_disabled_agents(), vec!["claude", "omp"]);
    }

    #[test]
    fn test_validate_rejects_empty_disabled_agent_entry() {
        let mut config = SourcesConfig::default();
        config.disabled_agents.push("   ".into());
        let err = config
            .validate()
            .expect_err("disabled_agents entry should fail");
        assert!(matches!(err, ConfigError::Validation(_)));
    }

    #[test]
    fn test_sources_config_roundtrip_preserves_disabled_agents() {
        let mut config = SourcesConfig::default();
        config.exclude_agent_from_indexing("openclaw").unwrap();
        config.exclude_agent_from_indexing("claude-code").unwrap();

        let serialized = toml::to_string_pretty(&config).unwrap();
        let deserialized: SourcesConfig = toml::from_str(&serialized).unwrap();

        assert_eq!(
            deserialized.configured_disabled_agents(),
            vec!["claude", "openclaw"]
        );
    }

    #[test]
    fn test_save_to_rejects_invalid_config() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("sources.toml");

        let mut config = SourcesConfig::default();
        config.sources.push(ssh_source("local", "user@host"));

        let err = config
            .save_to(&path)
            .expect_err("save_to should reject invalid config");
        assert!(matches!(err, ConfigError::Validation(_)));
        assert!(!path.exists(), "invalid config should not be written");
    }
}
