use super::detectors::{compile_regexes, parse_env_regex_list};
use crate::resource_plan::redact_secrets::{
    ANTHROPIC_API_KEY_PATTERN, AWS_ACCESS_KEY_PATTERN, AWS_SECRET_KEY_PATTERN,
    AWS_SESSION_TOKEN_PATTERN, BEARER_TOKEN_PATTERN, DATABASE_URL_PATTERN,
    GENERIC_SECRET_ASSIGNMENT_PATTERN, GITHUB_TOKEN_PATTERN, JWT_PATTERN, OPENAI_API_KEY_PATTERN,
    PRIVATE_KEY_BLOCK_PATTERN, SLACK_TOKEN_PATTERN, STRIPE_KEY_PATTERN,
};
use anyhow::Result;
use console::style;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::PathBuf;

pub(super) const DEFAULT_ENTROPY_THRESHOLD: f64 = 4.0;
const DEFAULT_ENTROPY_MIN_LEN: usize = 20;
const DEFAULT_CONTEXT_BYTES: usize = 120;
pub(super) const DEFAULT_MAX_FINDINGS: usize = 500;
pub(super) const SCAN_PAGE_ROWS: usize = 128;
pub(super) const REDACTED_CONTEXT: &str = "[redacted]";
pub(super) const REDACTED_METADATA_CONTEXT: &str = "structured metadata: [redacted]";
pub(super) const CUSTOM_DENYLIST_PATTERN_ID: &str = "custom_denylist";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretSeverity {
    Critical,
    High,
    Medium,
    Low,
}

impl SecretSeverity {
    pub(super) fn rank(self) -> u8 {
        match self {
            SecretSeverity::Critical => 0,
            SecretSeverity::High => 1,
            SecretSeverity::Medium => 2,
            SecretSeverity::Low => 3,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            SecretSeverity::Critical => "critical",
            SecretSeverity::High => "high",
            SecretSeverity::Medium => "medium",
            SecretSeverity::Low => "low",
        }
    }

    pub(super) fn styled(self, text: &str) -> String {
        match self {
            SecretSeverity::Critical => style(text).red().bold().to_string(),
            SecretSeverity::High => style(text).red().to_string(),
            SecretSeverity::Medium => style(text).yellow().to_string(),
            SecretSeverity::Low => style(text).blue().to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretLocation {
    ConversationTitle,
    ConversationMetadata,
    MessageContent,
    MessageMetadata,
    MessageSnippet,
}

impl SecretLocation {
    pub(super) fn label(&self) -> &'static str {
        match self {
            SecretLocation::ConversationTitle => "conversation.title",
            SecretLocation::ConversationMetadata => "conversation.metadata",
            SecretLocation::MessageContent => "message.content",
            SecretLocation::MessageMetadata => "message.metadata",
            SecretLocation::MessageSnippet => "message.snippet",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SecretFinding {
    pub severity: SecretSeverity,
    pub kind: String,
    pub pattern: String,
    pub match_redacted: String,
    pub context: String,
    pub location: SecretLocation,
    pub agent: Option<String>,
    pub workspace: Option<String>,
    pub source_path: Option<String>,
    pub conversation_id: Option<i64>,
    pub message_id: Option<i64>,
    pub message_idx: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SecretScanSummary {
    pub total: usize,
    pub by_severity: BTreeMap<SecretSeverity, usize>,
    pub has_critical: bool,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SecretScanReport {
    pub summary: SecretScanSummary,
    pub findings: Vec<SecretFinding>,
}

/// Proof that a complete scan observed one immutable staged export artifact.
#[derive(Debug, Clone)]
pub struct StagedSecretScan {
    pub report: SecretScanReport,
    pub artifact_sha256: String,
}

#[derive(Debug, Clone)]
pub struct SecretScanFilters {
    pub agents: Option<Vec<String>>,
    pub workspaces: Option<Vec<PathBuf>>,
    pub since_ts: Option<i64>,
    pub until_ts: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct SecretScanConfig {
    pub allowlist: Vec<Regex>,
    pub denylist: Vec<Regex>,
    pub allowlist_raw: Vec<String>,
    pub denylist_raw: Vec<String>,
    pub entropy_threshold: f64,
    pub entropy_min_len: usize,
    pub context_bytes: usize,
    pub max_findings: usize,
}

impl SecretScanConfig {
    pub fn from_inputs(allowlist: &[String], denylist: &[String]) -> Result<Self> {
        Self::from_inputs_with_env(allowlist, denylist, true)
    }

    pub fn from_inputs_with_env(
        allowlist: &[String],
        denylist: &[String],
        use_env: bool,
    ) -> Result<Self> {
        let allowlist_raw = if allowlist.is_empty() && use_env {
            parse_env_regex_list("CASS_SECRETS_ALLOWLIST")?
        } else {
            allowlist.to_vec()
        };
        let denylist_raw = if denylist.is_empty() && use_env {
            parse_env_regex_list("CASS_SECRETS_DENYLIST")?
        } else {
            denylist.to_vec()
        };

        Ok(Self {
            allowlist: compile_regexes(&allowlist_raw, "allowlist")?,
            denylist: compile_regexes(&denylist_raw, "denylist")?,
            allowlist_raw,
            denylist_raw,
            entropy_threshold: DEFAULT_ENTROPY_THRESHOLD,
            entropy_min_len: DEFAULT_ENTROPY_MIN_LEN,
            context_bytes: DEFAULT_CONTEXT_BYTES,
            max_findings: DEFAULT_MAX_FINDINGS,
        })
    }
}

pub(super) struct SecretPattern {
    pub(super) id: &'static str,
    pub(super) severity: SecretSeverity,
    pub(super) regex: Regex,
}

pub(super) static BUILTIN_PATTERNS: Lazy<Vec<SecretPattern>> = Lazy::new(|| {
    vec![
        SecretPattern {
            id: "aws_access_key_id",
            severity: SecretSeverity::High,
            regex: Regex::new(AWS_ACCESS_KEY_PATTERN).expect("aws access key regex"),
        },
        SecretPattern {
            id: "aws_secret_key",
            severity: SecretSeverity::Critical,
            regex: Regex::new(AWS_SECRET_KEY_PATTERN).expect("aws secret regex"),
        },
        SecretPattern {
            id: "github_pat",
            severity: SecretSeverity::High,
            regex: Regex::new(GITHUB_TOKEN_PATTERN).expect("github token regex"),
        },
        SecretPattern {
            id: "openai_key",
            severity: SecretSeverity::High,
            regex: Regex::new(OPENAI_API_KEY_PATTERN).expect("openai key regex"),
        },
        SecretPattern {
            id: "anthropic_key",
            severity: SecretSeverity::High,
            regex: Regex::new(ANTHROPIC_API_KEY_PATTERN).expect("anthropic key regex"),
        },
        SecretPattern {
            id: "jwt",
            severity: SecretSeverity::Medium,
            regex: Regex::new(JWT_PATTERN).expect("jwt regex"),
        },
        SecretPattern {
            id: "private_key",
            severity: SecretSeverity::Critical,
            regex: Regex::new(PRIVATE_KEY_BLOCK_PATTERN).expect("private key regex"),
        },
        SecretPattern {
            id: "database_url",
            severity: SecretSeverity::Medium,
            regex: Regex::new(DATABASE_URL_PATTERN).expect("database URL regex"),
        },
        SecretPattern {
            id: "generic_api_key",
            severity: SecretSeverity::Low,
            regex: Regex::new(GENERIC_SECRET_ASSIGNMENT_PATTERN)
                .expect("generic secret assignment regex"),
        },
        SecretPattern {
            id: "aws_session_token",
            severity: SecretSeverity::Critical,
            regex: Regex::new(AWS_SESSION_TOKEN_PATTERN).expect("aws session token regex"),
        },
        SecretPattern {
            id: "bearer_token",
            severity: SecretSeverity::High,
            regex: Regex::new(BEARER_TOKEN_PATTERN).expect("bearer token regex"),
        },
        SecretPattern {
            id: "slack_token",
            severity: SecretSeverity::High,
            regex: Regex::new(SLACK_TOKEN_PATTERN).expect("slack token regex"),
        },
        SecretPattern {
            id: "stripe_key",
            severity: SecretSeverity::High,
            regex: Regex::new(STRIPE_KEY_PATTERN).expect("stripe key regex"),
        },
    ]
});

// `=` is only valid as base64 *padding*, so keep it terminal. Putting it in
// the repeated class made one candidate span an entire `key=value` assignment,
// and the Medium entropy finding then shadowed the assignment-pattern kind
// (e.g. `generic_api_key`) in overlap dedup.
pub(super) static ENTROPY_BASE64_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"[A-Za-z0-9+/_-]{20,}={0,2}").expect("entropy base64 regex"));
pub(super) static ENTROPY_HEX_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\b[A-Fa-f0-9]{32,}\b").expect("entropy hex regex"));

#[derive(Debug, Clone)]
pub(super) struct ScanContext {
    pub(super) agent: Option<String>,
    pub(super) workspace: Option<String>,
    pub(super) source_path: Option<String>,
    pub(super) conversation_id: Option<i64>,
    pub(super) message_id: Option<i64>,
    pub(super) message_idx: Option<i64>,
}
