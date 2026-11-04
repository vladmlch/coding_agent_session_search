// Dead-code tolerated module-wide: `validate_sources_config` feeds
// `cass sources doctor` (its `config_validation` field), but the parse-failure
// half (`validate_malformed`) and the change manifest (`diff_configs`) have no
// production caller yet. This module is config-time validation.
#![allow(dead_code)]

//! Source configuration validation for the local-only build.
//!
//! `sources.toml` is validated so a malformed config or an interrupted write
//! is reported *as such* — never silently dropped, overwritten, or mistaken
//! for something else. This module classifies the config-time findings: no
//! sources configured, malformed config (with the parser's line/field
//! detail), a duplicate name, a source listing no paths, a missing path
//! mapping on a legacy remote entry, an unknown disabled agent, an
//! interrupted config write, and — as a warning rather than an error — the
//! presence of legacy `type = "ssh"` entries whose remote synchronization was
//! removed.
//!
//! It also provides a before/after [`ConfigChangeManifest`] for dry-run config
//! changes so a removal is always explicit and confirmable: cass never drops
//! an existing source entry to "fix" a malformed file. The pure
//! [`validate_sources_config`] takes a parsed [`SourcesConfig`] plus a
//! [`ConfigValidationContext`] of environment signals, so the classifier is
//! fully unit-testable; [`validate_malformed`] handles the parse-failure path.
//! All enums serialize as snake_case.

use serde::{Deserialize, Serialize};

use super::config::{SourceDefinition, SourcesConfig};
use super::provenance::SourceKind;

/// Schema version for the validation + manifest JSON contracts.
pub(crate) const SOURCE_CONFIG_VALIDATION_SCHEMA_VERSION: u32 = 1;

const VALIDATION_REPORT_KIND: &str = "source_config_validation";
const CHANGE_MANIFEST_KIND: &str = "source_config_change_manifest";

/// A distinct source-configuration problem class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ConfigIssueKind {
    /// No sources are configured — search is local-only (not an error).
    NoSourcesConfigured,
    /// The config file could not be parsed; `detail` carries the parser's
    /// line/column/field message.
    MalformedConfig,
    /// Two source entries share a name (the name is the `source_id`).
    DuplicateSourceName,
    /// A `type = "ssh"` entry is present. Remote synchronization was removed;
    /// the entry is an inert archive record kept for provenance.
    LegacyRemoteSource,
    /// A source lists no paths.
    SourceMissingPaths,
    /// A legacy remote source has no path mapping, so its archived workspace
    /// paths will not rewrite to local equivalents when opening hits.
    PathMappingMissing,
    /// A `disabled_agents` entry is not a known connector name.
    DisabledAgentUnknown,
    /// Orphaned `.sources.toml.tmp.*` temp files indicate an interrupted
    /// atomic config write.
    InterruptedConfigWrite,
}

impl ConfigIssueKind {
    pub(crate) fn stable_name(self) -> &'static str {
        match self {
            Self::NoSourcesConfigured => "no_sources_configured",
            Self::MalformedConfig => "malformed_config",
            Self::DuplicateSourceName => "duplicate_source_name",
            Self::LegacyRemoteSource => "legacy_remote_source",
            Self::SourceMissingPaths => "source_missing_paths",
            Self::PathMappingMissing => "path_mapping_missing",
            Self::DisabledAgentUnknown => "disabled_agent_unknown",
            Self::InterruptedConfigWrite => "interrupted_config_write",
        }
    }

    /// The default severity for this kind. A malformed or ambiguous config is
    /// an error; degraded-but-usable states warn; local-only is informational.
    pub(crate) fn default_severity(self) -> ConfigIssueSeverity {
        match self {
            Self::NoSourcesConfigured => ConfigIssueSeverity::Info,
            Self::MalformedConfig | Self::DuplicateSourceName => ConfigIssueSeverity::Error,
            Self::LegacyRemoteSource
            | Self::SourceMissingPaths
            | Self::PathMappingMissing
            | Self::DisabledAgentUnknown
            | Self::InterruptedConfigWrite => ConfigIssueSeverity::Warning,
        }
    }

    /// A concrete, non-destructive `cass` command (or `None` when the fix is a
    /// manual edit / host-level action). Never a bare or destructive command.
    pub(crate) fn safe_next_command(self) -> Option<&'static str> {
        match self {
            Self::NoSourcesConfigured | Self::SourceMissingPaths => {
                Some("cass sources add <name> --path <path>")
            }
            // A malformed file or duplicate name needs a manual edit; the
            // detail names the exact line/field. We never auto-rewrite the file.
            // A legacy remote entry is inert archive data — nothing to run.
            Self::MalformedConfig
            | Self::DuplicateSourceName
            | Self::LegacyRemoteSource
            | Self::InterruptedConfigWrite => None,
            Self::PathMappingMissing => {
                Some("cass sources mappings add <name> --from <remote> --to <local>")
            }
            Self::DisabledAgentUnknown => Some("cass sources agents list --json"),
        }
    }
}

/// How serious a config issue is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ConfigIssueSeverity {
    /// The configuration itself is malformed or ambiguous.
    Error,
    /// Degraded — links may not resolve or writes may have been interrupted.
    Warning,
    /// Informational (e.g. no extra sources configured).
    Info,
}

/// One classified configuration issue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ConfigIssue {
    pub kind: ConfigIssueKind,
    pub severity: ConfigIssueSeverity,
    /// The source entry this issue concerns, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_name: Option<String>,
    /// The config field this issue concerns, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// Human-readable detail; for `MalformedConfig` this carries the parser's
    /// line/column/field message verbatim.
    pub detail: String,
    /// A concrete, non-destructive next command, when one applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub safe_next_command: Option<String>,
}

impl ConfigIssue {
    fn new(kind: ConfigIssueKind, source_name: Option<String>, detail: impl Into<String>) -> Self {
        Self {
            kind,
            severity: kind.default_severity(),
            source_name,
            field: None,
            detail: detail.into(),
            safe_next_command: kind.safe_next_command().map(str::to_string),
        }
    }

    fn with_field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }
}

/// What search the current configuration supports right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SourceSearchMode {
    /// Search runs over local sessions and local archive mirrors only.
    LocalOnly,
    /// The config could not be parsed or has blocking errors and must not be
    /// reported as healthy.
    ConfigInvalid,
}

/// Environment signals the pure validator needs beyond the config itself.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct ConfigValidationContext {
    /// Count of orphaned `.sources.toml.tmp.*` temp files observed beside the
    /// config (evidence of an interrupted atomic write).
    pub orphaned_temp_files: usize,
    /// Known connector/agent names, to validate `disabled_agents` entries.
    /// Empty means "skip that check" (no known set available).
    pub known_agents: Vec<String>,
}

/// The source-configuration validation verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SourceConfigValidation {
    pub schema_version: u32,
    pub report_kind: String,
    /// True when there are no `Error`-severity issues.
    pub valid: bool,
    pub search_mode: SourceSearchMode,
    pub source_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub issues: Vec<ConfigIssue>,
    /// Always true: validation never drops or overwrites a source entry.
    pub never_drops_entries: bool,
}

impl SourceConfigValidation {
    /// Whether any issue of `Error` severity is present.
    pub(crate) fn has_errors(&self) -> bool {
        self.issues
            .iter()
            .any(|i| i.severity == ConfigIssueSeverity::Error)
    }

    /// Issues of a given kind (for assertions / projection).
    pub(crate) fn issues_of(&self, kind: ConfigIssueKind) -> impl Iterator<Item = &ConfigIssue> {
        self.issues.iter().filter(move |i| i.kind == kind)
    }
}

/// Validate a parsed source configuration against environment signals. Pure:
/// every input is explicit, so the classification is fully unit-testable.
pub(crate) fn validate_sources_config(
    config: &SourcesConfig,
    ctx: &ConfigValidationContext,
) -> SourceConfigValidation {
    let mut issues: Vec<ConfigIssue> = Vec::new();
    let source_count = config.sources.len();

    if config.sources.is_empty() {
        issues.push(ConfigIssue::new(
            ConfigIssueKind::NoSourcesConfigured,
            None,
            "no extra sources configured; search runs over local sessions and archives only",
        ));
    }

    // Duplicate names (the name is the source_id). Report each later
    // duplicate once.
    let mut seen_names: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for src in &config.sources {
        if !seen_names.insert(src.name.as_str()) {
            issues.push(
                ConfigIssue::new(
                    ConfigIssueKind::DuplicateSourceName,
                    Some(src.name.clone()),
                    format!(
                        "duplicate source name '{}' (names must be unique)",
                        src.name
                    ),
                )
                .with_field("name"),
            );
        }
    }

    for src in &config.sources {
        let is_ssh = src.source_type == SourceKind::Ssh;
        if is_ssh {
            issues.push(
                ConfigIssue::new(
                    ConfigIssueKind::LegacyRemoteSource,
                    Some(src.name.clone()),
                    format!(
                        "source '{}' is a legacy remote entry; remote synchronization was removed — \
                         its archived conversations stay searchable and the entry is kept for provenance",
                        src.name
                    ),
                )
                .with_field("type"),
            );
            if src.path_mappings.is_empty() {
                issues.push(
                    ConfigIssue::new(
                        ConfigIssueKind::PathMappingMissing,
                        Some(src.name.clone()),
                        format!(
                            "legacy remote source '{}' has no path mapping; archived workspace paths will not rewrite to local",
                            src.name
                        ),
                    )
                    .with_field("path_mappings"),
                );
            }
        }
        if src.paths.is_empty() {
            issues.push(
                ConfigIssue::new(
                    ConfigIssueKind::SourceMissingPaths,
                    Some(src.name.clone()),
                    format!("source '{}' lists no paths", src.name),
                )
                .with_field("paths"),
            );
        }
    }

    // Unknown disabled-agent names (only when a known set is supplied).
    if !ctx.known_agents.is_empty() {
        for agent in &config.disabled_agents {
            if !ctx.known_agents.iter().any(|k| k == agent) {
                issues.push(
                    ConfigIssue::new(
                        ConfigIssueKind::DisabledAgentUnknown,
                        None,
                        format!("disabled_agents entry '{agent}' is not a known connector"),
                    )
                    .with_field("disabled_agents"),
                );
            }
        }
    }

    if ctx.orphaned_temp_files > 0 {
        issues.push(ConfigIssue::new(
            ConfigIssueKind::InterruptedConfigWrite,
            None,
            format!(
                "an interrupted config write left {} orphaned temp file(s) beside sources.toml; \
                 the next successful sources command rewrites it atomically",
                ctx.orphaned_temp_files
            ),
        ));
    }

    let has_errors = issues
        .iter()
        .any(|i| i.severity == ConfigIssueSeverity::Error);
    let search_mode = if has_errors {
        SourceSearchMode::ConfigInvalid
    } else {
        SourceSearchMode::LocalOnly
    };

    SourceConfigValidation {
        schema_version: SOURCE_CONFIG_VALIDATION_SCHEMA_VERSION,
        report_kind: VALIDATION_REPORT_KIND.to_string(),
        valid: !has_errors,
        search_mode,
        source_count,
        issues,
        never_drops_entries: true,
    }
}

/// Build the validation verdict for a config that failed to parse. `detail`
/// should be the parser's full error string (which carries line/column/field
/// information); the config is reported `ConfigInvalid`, never healthy.
pub(crate) fn validate_malformed(detail: impl Into<String>) -> SourceConfigValidation {
    let issue = ConfigIssue::new(
        ConfigIssueKind::MalformedConfig,
        None,
        format!("sources config could not be parsed: {}", detail.into()),
    );
    SourceConfigValidation {
        schema_version: SOURCE_CONFIG_VALIDATION_SCHEMA_VERSION,
        report_kind: VALIDATION_REPORT_KIND.to_string(),
        valid: false,
        search_mode: SourceSearchMode::ConfigInvalid,
        source_count: 0,
        issues: vec![issue],
        never_drops_entries: true,
    }
}

// ---------------------------------------------------------------------------
// Dry-run before/after change manifest — proves no source is silently dropped.
// ---------------------------------------------------------------------------

/// What happened to a source entry between two config generations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SourceChangeKind {
    Added,
    Removed,
    Modified,
    Unchanged,
}

/// A redaction-safe summary of a source entry (no secrets — host is the only
/// connection detail, paths/mappings are counted not listed).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SourceSummary {
    pub name: String,
    pub source_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    pub path_count: usize,
    pub mapping_count: usize,
}

impl SourceSummary {
    fn of(src: &SourceDefinition) -> Self {
        let source_type = match src.source_type {
            SourceKind::Local => "local",
            SourceKind::Ssh => "ssh",
        }
        .to_string();
        Self {
            name: src.name.clone(),
            source_type,
            host: src.host.clone(),
            path_count: src.paths.len(),
            mapping_count: src.path_mappings.len(),
        }
    }

    /// Whether two summaries describe the same effective source definition.
    fn same_as(&self, other: &SourceSummary) -> bool {
        self == other
    }
}

/// One entry in a before/after change manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SourceChange {
    pub name: String,
    pub change: SourceChangeKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<SourceSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<SourceSummary>,
}

/// A dry-run manifest of how a config change affects each source entry, so a
/// removal can never happen silently.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ConfigChangeManifest {
    pub schema_version: u32,
    pub manifest_kind: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changes: Vec<SourceChange>,
    pub added: usize,
    pub removed: usize,
    pub modified: usize,
    /// True whenever the change removes an existing source — such a change
    /// must be explicitly confirmed, never applied silently.
    pub removals_require_confirmation: bool,
    /// Always true: cass never silently drops a source entry.
    pub never_silent_drop: bool,
}

/// Compute the before/after manifest for a proposed config change. Entries are
/// matched by name; a name present only in `before` is `Removed` (and flagged
/// as needing confirmation), only in `after` is `Added`, in both with a
/// different summary is `Modified`, otherwise `Unchanged`.
pub(crate) fn diff_configs(before: &SourcesConfig, after: &SourcesConfig) -> ConfigChangeManifest {
    use std::collections::BTreeMap;
    let before_map: BTreeMap<&str, &SourceDefinition> = before
        .sources
        .iter()
        .map(|s| (s.name.as_str(), s))
        .collect();
    let after_map: BTreeMap<&str, &SourceDefinition> =
        after.sources.iter().map(|s| (s.name.as_str(), s)).collect();

    let mut names: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    names.extend(before_map.keys().copied());
    names.extend(after_map.keys().copied());

    let mut changes = Vec::new();
    let (mut added, mut removed, mut modified) = (0usize, 0usize, 0usize);
    for name in names {
        let before_src = before_map.get(name).map(|s| SourceSummary::of(s));
        let after_src = after_map.get(name).map(|s| SourceSummary::of(s));
        let change = match (&before_src, &after_src) {
            (Some(_), None) => {
                removed += 1;
                SourceChangeKind::Removed
            }
            (None, Some(_)) => {
                added += 1;
                SourceChangeKind::Added
            }
            (Some(b), Some(a)) => {
                if b.same_as(a) {
                    SourceChangeKind::Unchanged
                } else {
                    modified += 1;
                    SourceChangeKind::Modified
                }
            }
            (None, None) => SourceChangeKind::Unchanged,
        };
        changes.push(SourceChange {
            name: name.to_string(),
            change,
            before: before_src,
            after: after_src,
        });
    }

    ConfigChangeManifest {
        schema_version: SOURCE_CONFIG_VALIDATION_SCHEMA_VERSION,
        manifest_kind: CHANGE_MANIFEST_KIND.to_string(),
        changes,
        added,
        removed,
        modified,
        removals_require_confirmation: removed > 0,
        never_silent_drop: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::config::SourceDefinition;

    fn local(name: &str, paths: &[&str]) -> SourceDefinition {
        let mut s = SourceDefinition::local(name);
        s.paths = paths.iter().map(|p| p.to_string()).collect();
        s
    }

    fn ssh(name: &str, host: Option<&str>, paths: &[&str], mappings: usize) -> SourceDefinition {
        SourceDefinition {
            name: name.to_string(),
            source_type: SourceKind::Ssh,
            host: host.map(str::to_string),
            paths: paths.iter().map(|p| p.to_string()).collect(),
            path_mappings: (0..mappings)
                .map(|i| crate::sources::config::PathMapping {
                    from: format!("/remote/{i}"),
                    to: format!("/local/{i}"),
                    agents: None,
                })
                .collect(),
            ..Default::default()
        }
    }

    fn ctx() -> ConfigValidationContext {
        ConfigValidationContext::default()
    }

    const ALL_KINDS: &[ConfigIssueKind] = &[
        ConfigIssueKind::NoSourcesConfigured,
        ConfigIssueKind::MalformedConfig,
        ConfigIssueKind::DuplicateSourceName,
        ConfigIssueKind::LegacyRemoteSource,
        ConfigIssueKind::SourceMissingPaths,
        ConfigIssueKind::PathMappingMissing,
        ConfigIssueKind::DisabledAgentUnknown,
        ConfigIssueKind::InterruptedConfigWrite,
    ];

    fn assert_no_destructive_commands(v: &SourceConfigValidation) {
        for issue in &v.issues {
            if let Some(cmd) = &issue.safe_next_command {
                assert!(cmd.starts_with("cass "), "must be concrete cass: {cmd}");
                assert_ne!(cmd.trim(), "cass");
                for bad in [
                    "rm ",
                    "rm -",
                    "delete ",
                    "DROP ",
                    "--purge",
                    "--force-clean",
                ] {
                    assert!(!cmd.contains(bad), "destructive token in {cmd}");
                }
            }
        }
    }

    #[test]
    fn issue_kinds_serialize_snake_case_and_are_stable() {
        let pairs: &[(ConfigIssueKind, &str)] = &[
            (
                ConfigIssueKind::NoSourcesConfigured,
                "no_sources_configured",
            ),
            (ConfigIssueKind::MalformedConfig, "malformed_config"),
            (
                ConfigIssueKind::DuplicateSourceName,
                "duplicate_source_name",
            ),
            (ConfigIssueKind::LegacyRemoteSource, "legacy_remote_source"),
            (ConfigIssueKind::SourceMissingPaths, "source_missing_paths"),
            (ConfigIssueKind::PathMappingMissing, "path_mapping_missing"),
            (
                ConfigIssueKind::DisabledAgentUnknown,
                "disabled_agent_unknown",
            ),
            (
                ConfigIssueKind::InterruptedConfigWrite,
                "interrupted_config_write",
            ),
        ];
        for (kind, want) in pairs {
            assert_eq!(
                serde_json::to_string(kind).expect("serialize kind"),
                format!("\"{want}\"")
            );
            assert_eq!(kind.stable_name(), *want);
        }
        assert_eq!(pairs.len(), ALL_KINDS.len());
    }

    #[test]
    fn empty_config_is_local_only_not_an_error() {
        let cfg = SourcesConfig::default();
        let v = validate_sources_config(&cfg, &ctx());
        assert!(v.valid, "no sources is not an error");
        assert_eq!(v.search_mode, SourceSearchMode::LocalOnly);
        assert_eq!(v.source_count, 0);
        assert_eq!(v.issues_of(ConfigIssueKind::NoSourcesConfigured).count(), 1);
        assert_eq!(
            v.issues[0].severity,
            ConfigIssueSeverity::Info,
            "local-only is informational"
        );
        assert!(v.never_drops_entries);
        assert_no_destructive_commands(&v);
    }

    #[test]
    fn valid_local_sources_are_valid_and_local_only() {
        let cfg = SourcesConfig {
            sources: vec![local("a", &["~/.claude/projects"])],
            disabled_agents: vec![],
        };
        let v = validate_sources_config(&cfg, &ctx());
        assert!(v.valid);
        assert_eq!(v.search_mode, SourceSearchMode::LocalOnly);
        assert!(v.issues.is_empty(), "{:?}", v.issues);
    }

    #[test]
    fn legacy_remote_source_warns_but_stays_valid() {
        let cfg = SourcesConfig {
            sources: vec![ssh("laptop", Some("me@laptop"), &["~/.claude/projects"], 1)],
            disabled_agents: vec![],
        };
        let v = validate_sources_config(&cfg, &ctx());
        assert!(v.valid, "{:?}", v.issues);
        assert_eq!(v.search_mode, SourceSearchMode::LocalOnly);
        let issue = v
            .issues_of(ConfigIssueKind::LegacyRemoteSource)
            .next()
            .expect("legacy-remote issue");
        assert_eq!(issue.severity, ConfigIssueSeverity::Warning);
        assert_eq!(issue.source_name.as_deref(), Some("laptop"));
    }

    #[test]
    fn legacy_remote_source_without_host_still_warns_not_errors() {
        let cfg = SourcesConfig {
            sources: vec![ssh("laptop", None, &["~/x"], 1)],
            disabled_agents: vec![],
        };
        let v = validate_sources_config(&cfg, &ctx());
        // A legacy entry never blocks: remote fields are inert data now.
        assert!(v.valid);
        assert!(!v.has_errors());
        assert_eq!(v.search_mode, SourceSearchMode::LocalOnly);
        let issue = v
            .issues_of(ConfigIssueKind::LegacyRemoteSource)
            .next()
            .expect("legacy-remote issue");
        assert_eq!(issue.severity, ConfigIssueSeverity::Warning);
        assert_eq!(issue.source_name.as_deref(), Some("laptop"));
    }

    #[test]
    fn legacy_remote_source_without_mapping_warns_but_stays_valid() {
        let cfg = SourcesConfig {
            sources: vec![ssh("laptop", Some("me@laptop"), &["~/x"], 0)],
            disabled_agents: vec![],
        };
        let v = validate_sources_config(&cfg, &ctx());
        let issue = v
            .issues_of(ConfigIssueKind::PathMappingMissing)
            .next()
            .expect("path-mapping-missing issue");
        assert_eq!(issue.severity, ConfigIssueSeverity::Warning);
        // A warning does not invalidate the config.
        assert!(v.valid);
        assert_eq!(v.search_mode, SourceSearchMode::LocalOnly);
        assert!(
            issue
                .safe_next_command
                .as_deref()
                .unwrap()
                .contains("mappings add")
        );
    }

    #[test]
    fn duplicate_names_are_reported_once_each_extra() {
        let cfg = SourcesConfig {
            sources: vec![
                local("dup", &["~/a"]),
                local("dup", &["~/b"]),
                local("dup", &["~/c"]),
            ],
            disabled_agents: vec![],
        };
        let v = validate_sources_config(&cfg, &ctx());
        // Two extra duplicates beyond the first.
        assert_eq!(v.issues_of(ConfigIssueKind::DuplicateSourceName).count(), 2);
        assert!(!v.valid, "duplicate names block");
    }

    #[test]
    fn source_missing_paths_warns() {
        let cfg = SourcesConfig {
            sources: vec![local("a", &[])],
            disabled_agents: vec![],
        };
        let v = validate_sources_config(&cfg, &ctx());
        let issue = v
            .issues_of(ConfigIssueKind::SourceMissingPaths)
            .next()
            .expect("missing-paths issue");
        assert_eq!(issue.severity, ConfigIssueSeverity::Warning);
        assert_eq!(issue.field.as_deref(), Some("paths"));
    }

    #[test]
    fn unknown_disabled_agent_warns_only_when_known_set_supplied() {
        let cfg = SourcesConfig {
            sources: vec![local("a", &["~/x"])],
            disabled_agents: vec!["claude".to_string(), "not_a_real_agent".to_string()],
        };
        // No known set: skip the check.
        let v = validate_sources_config(&cfg, &ctx());
        assert_eq!(
            v.issues_of(ConfigIssueKind::DisabledAgentUnknown).count(),
            0
        );
        // With a known set: flag the unknown one.
        let ctx = ConfigValidationContext {
            known_agents: vec!["claude".to_string(), "codex".to_string()],
            ..ctx()
        };
        let v = validate_sources_config(&cfg, &ctx);
        let issue = v
            .issues_of(ConfigIssueKind::DisabledAgentUnknown)
            .next()
            .expect("unknown-agent issue");
        assert!(issue.detail.contains("not_a_real_agent"));
    }

    #[test]
    fn interrupted_config_write_is_detected_from_orphaned_temps() {
        let cfg = SourcesConfig {
            sources: vec![local("a", &["~/x"])],
            disabled_agents: vec![],
        };
        let ctx = ConfigValidationContext {
            orphaned_temp_files: 2,
            ..ctx()
        };
        let v = validate_sources_config(&cfg, &ctx);
        let issue = v
            .issues_of(ConfigIssueKind::InterruptedConfigWrite)
            .next()
            .expect("interrupted-write issue");
        assert_eq!(issue.severity, ConfigIssueSeverity::Warning);
        // An interrupted write alone (no errors) does not flip config invalid.
        assert!(v.valid);
        assert_no_destructive_commands(&v);
    }

    #[test]
    fn malformed_config_is_invalid_and_carries_parser_detail() {
        let v = validate_malformed("TOML parse error at line 3, column 1\nexpected `=`");
        assert!(!v.valid);
        assert_eq!(v.search_mode, SourceSearchMode::ConfigInvalid);
        let issue = &v.issues[0];
        assert_eq!(issue.kind, ConfigIssueKind::MalformedConfig);
        assert_eq!(issue.severity, ConfigIssueSeverity::Error);
        assert!(issue.detail.contains("line 3"), "carries line/field detail");
        // Malformed config never auto-rewrites the file.
        assert!(issue.safe_next_command.is_none());
    }

    #[test]
    fn validation_round_trips_through_json() {
        let cfg = SourcesConfig {
            sources: vec![ssh("laptop", None, &["~/x"], 0)],
            disabled_agents: vec![],
        };
        let v = validate_sources_config(&cfg, &ConfigValidationContext::default());
        let json = serde_json::to_string(&v).expect("serialize");
        assert!(json.contains("\"report_kind\":\"source_config_validation\""));
        assert!(json.contains("\"never_drops_entries\":true"));
        let parsed: SourceConfigValidation = serde_json::from_str(&json).expect("parse");
        assert_eq!(parsed, v);
    }

    // -- before/after change manifest (never-silent-drop) -------------------

    #[test]
    fn diff_classifies_added_removed_modified_unchanged() {
        let before = SourcesConfig {
            sources: vec![
                local("keep", &["~/k"]),
                local("drop", &["~/d"]),
                ssh("edit", Some("me@h"), &["~/e"], 1),
            ],
            disabled_agents: vec![],
        };
        let after = SourcesConfig {
            sources: vec![
                local("keep", &["~/k"]),
                ssh("edit", Some("me@h"), &["~/e", "~/e2"], 1), // path added → modified
                local("new", &["~/n"]),
            ],
            disabled_agents: vec![],
        };
        let m = diff_configs(&before, &after);
        assert_eq!(m.added, 1);
        assert_eq!(m.removed, 1);
        assert_eq!(m.modified, 1);
        assert!(
            m.removals_require_confirmation,
            "a removal needs confirmation"
        );
        assert!(m.never_silent_drop);

        let kind_of = |name: &str| {
            m.changes
                .iter()
                .find(|c| c.name == name)
                .map(|c| c.change)
                .unwrap()
        };
        assert_eq!(kind_of("keep"), SourceChangeKind::Unchanged);
        assert_eq!(kind_of("drop"), SourceChangeKind::Removed);
        assert_eq!(kind_of("edit"), SourceChangeKind::Modified);
        assert_eq!(kind_of("new"), SourceChangeKind::Added);
    }

    #[test]
    fn diff_with_no_removals_does_not_require_confirmation() {
        let before = SourcesConfig {
            sources: vec![local("a", &["~/a"])],
            disabled_agents: vec![],
        };
        let after = SourcesConfig {
            sources: vec![local("a", &["~/a"]), local("b", &["~/b"])],
            disabled_agents: vec![],
        };
        let m = diff_configs(&before, &after);
        assert_eq!(m.added, 1);
        assert_eq!(m.removed, 0);
        assert!(!m.removals_require_confirmation);
        assert!(m.never_silent_drop, "invariant holds even with no removals");
    }

    #[test]
    fn manifest_round_trips_through_json() {
        let before = SourcesConfig {
            sources: vec![local("a", &["~/a"])],
            disabled_agents: vec![],
        };
        let after = SourcesConfig::default();
        let m = diff_configs(&before, &after);
        let json = serde_json::to_string(&m).expect("serialize manifest");
        assert!(json.contains("\"manifest_kind\":\"source_config_change_manifest\""));
        assert!(json.contains("\"never_silent_drop\":true"));
        let parsed: ConfigChangeManifest = serde_json::from_str(&json).expect("parse manifest");
        assert_eq!(parsed, m);
    }

    /// Integration-style coverage: write fixture `sources.toml` files to a
    /// tempdir, load through the real `SourcesConfig::load_from`, and validate
    /// — exercising the malformed-parse path and a structurally-bad config end
    /// to end.
    #[test]
    fn fixture_configs_validate_through_real_loader() {
        use std::io::Write;
        let dir = tempfile::TempDir::new().expect("temp dir");

        // 1) A malformed TOML file → load fails → validate_malformed carries
        //    the parser's line/field detail.
        let bad_path = dir.path().join("bad.toml");
        let mut f = std::fs::File::create(&bad_path).expect("create bad");
        writeln!(f, "[[sources]]\nname = \"laptop\"\ntype = ").expect("write bad");
        drop(f);
        let load_err = SourcesConfig::load_from(&bad_path).expect_err("malformed must fail load");
        let v = validate_malformed(load_err.to_string());
        assert_eq!(v.search_mode, SourceSearchMode::ConfigInvalid);
        assert_eq!(v.issues[0].kind, ConfigIssueKind::MalformedConfig);

        // 2) A well-formed legacy SSH source (host + paths, no mapping) → loads,
        //    then validates: the legacy remote entry is a warning, the absent
        //    path mapping is a second warning, and the config stays valid.
        let ok_path = dir.path().join("ok.toml");
        let mut f = std::fs::File::create(&ok_path).expect("create ok");
        writeln!(
            f,
            "[[sources]]\nname = \"laptop\"\ntype = \"ssh\"\nhost = \"me@laptop\"\npaths = [\"~/.claude/projects\"]"
        )
        .expect("write ok");
        drop(f);
        let cfg = SourcesConfig::load_from(&ok_path).expect("well-formed config loads");
        let v = validate_sources_config(&cfg, &ConfigValidationContext::default());
        assert!(v.valid, "a legacy remote entry warns but stays valid");
        assert_eq!(v.issues_of(ConfigIssueKind::LegacyRemoteSource).count(), 1);
        assert_eq!(v.issues_of(ConfigIssueKind::PathMappingMissing).count(), 1);
        assert_no_destructive_commands(&v);
    }
}
