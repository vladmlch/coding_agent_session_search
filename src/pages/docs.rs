//! Documentation Generation for pages export.
//!
//! Automatically generates comprehensive documentation that is included with
//! each exported archive site (opened locally or via `cass pages --preview`).
//!
//! # Overview
//!
//! Generated documentation includes:
//! - **README.md**: Main archive description for repository root
//! - **SECURITY.md**: Detailed security model and threat analysis
//! - **help.html**: In-app help accessible from web viewer
//! - **recovery.html**: Password recovery instructions
//! - **about.txt**: Simple text explanation for non-technical users

use crate::pages::summary::PrePublishSummary;
use chrono::{DateTime, Utc};

const CASS_VERSION: &str = env!("CARGO_PKG_VERSION");
const DOC_DATE_FORMAT: &str = "%Y-%m-%d";

fn format_optional_doc_date(value: Option<DateTime<Utc>>, fallback: &str) -> String {
    value
        .map(|dt| dt.format(DOC_DATE_FORMAT).to_string())
        .unwrap_or_else(|| fallback.to_string())
}

/// Location where a generated document should be placed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocLocation {
    /// Repository root (README.md, SECURITY.md)
    RepoRoot,
    /// Web root alongside index.html (help.html, about.txt)
    WebRoot,
}

/// A generated documentation file.
#[derive(Debug, Clone)]
pub struct GeneratedDoc {
    /// Filename for the document.
    pub filename: String,
    /// Content of the document.
    pub content: String,
    /// Where to place the document.
    pub location: DocLocation,
}

/// Configuration for documentation generation.
#[derive(Debug, Clone, Default)]
pub struct DocConfig {
    /// Whether the archived payload requires decryption before it can be read.
    pub archive_mode: ArchiveMode,
    /// Repository URL for CASS source.
    pub cass_repo_url: String,
    /// Argon2 memory parameter in KB.
    pub argon_memory_kb: u32,
    /// Argon2 time iterations.
    pub argon_iterations: u32,
    /// Argon2 parallelism.
    pub argon_parallelism: u32,
}

/// Security mode of the generated public archive documentation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ArchiveMode {
    /// The SQLite payload is stored as plaintext.
    #[default]
    Unencrypted,
}

impl DocConfig {
    /// Create a new DocConfig with default CASS repo URL.
    pub fn new() -> Self {
        Self {
            archive_mode: ArchiveMode::Unencrypted,
            cass_repo_url: "https://github.com/Dicklesworthstone/coding_agent_session_search"
                .to_string(),
            argon_memory_kb: 0,
            argon_iterations: 0,
            argon_parallelism: 0,
        }
    }

    /// Describe the security mode of the payload these documents accompany.
    pub fn with_archive_mode(mut self, archive_mode: ArchiveMode) -> Self {
        self.archive_mode = archive_mode;
        self
    }
}

/// Generator for export documentation.
pub struct DocumentationGenerator {
    config: DocConfig,
    summary: PrePublishSummary,
}

impl DocumentationGenerator {
    /// Create a new documentation generator.
    pub fn new(config: DocConfig, summary: PrePublishSummary) -> Self {
        Self { config, summary }
    }

    /// Generate all documentation files.
    pub fn generate_all(&self) -> Vec<GeneratedDoc> {
        vec![
            self.generate_readme(),
            self.generate_security_doc(),
            self.generate_help_html(),
            self.generate_recovery_html(),
            self.generate_about_txt(),
        ]
    }

    /// Generate README.md for repository root.
    pub fn generate_readme(&self) -> GeneratedDoc {
        let agent_list = self
            .summary
            .agents
            .iter()
            .map(|a| format!("- {} ({} conversations)", a.name, a.conversation_count))
            .collect::<Vec<_>>()
            .join("\n");

        let start_date = format_optional_doc_date(self.summary.earliest_timestamp, "Unknown");
        let end_date = format_optional_doc_date(self.summary.latest_timestamp, "Unknown");
        let date = Utc::now().format(DOC_DATE_FORMAT);

        let content = UNENCRYPTED_README_TEMPLATE
            .replace(
                "{conversation_count}",
                &self.summary.total_conversations.to_string(),
            )
            .replace("{agent_list}", &agent_list)
            .replace("{start_date}", &start_date)
            .replace("{end_date}", &end_date)
            .replace("{version}", CASS_VERSION)
            .replace("{date}", &date.to_string());

        GeneratedDoc {
            filename: "README.md".to_string(),
            content,
            location: DocLocation::RepoRoot,
        }
    }

    /// Generate SECURITY.md with threat model documentation.
    pub fn generate_security_doc(&self) -> GeneratedDoc {
        GeneratedDoc {
            filename: "SECURITY.md".to_string(),
            content: UNENCRYPTED_SECURITY_TEMPLATE
                .replace("{repo_url}", &self.config.cass_repo_url)
                .replace("{version}", CASS_VERSION),
            location: DocLocation::RepoRoot,
        }
    }

    /// Generate help.html for in-app help.
    pub fn generate_help_html(&self) -> GeneratedDoc {
        GeneratedDoc {
            filename: "help.html".to_string(),
            content: UNENCRYPTED_HELP_HTML_TEMPLATE.to_string(),
            location: DocLocation::WebRoot,
        }
    }

    /// Generate recovery.html with plaintext archive disclosure.
    pub fn generate_recovery_html(&self) -> GeneratedDoc {
        GeneratedDoc {
            filename: "recovery.html".to_string(),
            content: UNENCRYPTED_RECOVERY_HTML_TEMPLATE.to_string(),
            location: DocLocation::WebRoot,
        }
    }

    /// Generate about.txt for non-technical users.
    pub fn generate_about_txt(&self) -> GeneratedDoc {
        let conversation_count = self.summary.total_conversations.to_string();
        let date = Utc::now().format(DOC_DATE_FORMAT);

        let content = UNENCRYPTED_ABOUT_TXT_TEMPLATE
            .replace("{conversation_count}", &conversation_count)
            .replace("{date}", &date.to_string())
            .replace("{version}", CASS_VERSION);

        GeneratedDoc {
            filename: "about.txt".to_string(),
            content,
            location: DocLocation::WebRoot,
        }
    }
}

// =============================================================================
// Template Constants
// =============================================================================

const UNENCRYPTED_README_TEMPLATE: &str = r#"# Unencrypted Coding Session Archive

> **Warning:** This archive is not encrypted. Anyone who can access these
> files can read and copy every included conversation without a password.

Serve this directory with `cass pages --preview <dir>` (loopback only) and
open the printed `http://127.0.0.1` address, or open `index.html` directly.

## What This Contains

This archive includes {conversation_count} conversations from the following sources:
{agent_list}

Date range: {start_date} to {end_date}

## Access

No password, recovery key, or decryption step is required. Share this archive
only where plaintext access is intentional.

For the security implications, see [SECURITY.md](SECURITY.md).

---
Generated by CASS v{version} on {date}
"#;

const UNENCRYPTED_SECURITY_TEMPLATE: &str = r#"# Security Model: Unencrypted Archive

## Warning

This archive is **not encrypted**. Its SQLite payload is stored as plaintext,
so anyone who obtains these files can read all included conversation content.
There is no password or recovery-key access control.

Do not treat filesystem or transport security as payload encryption. Delete
these files if this exposure was not intended, then create a new encrypted
export.

For security issues with CASS, see {repo_url}/security.

---
Generated by CASS v{version}
"#;

const UNENCRYPTED_HELP_HTML_TEMPLATE: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Help - Unencrypted CASS Archive</title>
</head>
<body>
    <h1>Unencrypted Archive Help</h1>
    <p><strong>Warning:</strong> this archive is not encrypted. No password is required, and anyone with access to these files can read the included conversations.</p>
    <p>Use the search box to find conversations, select a result to open it, and use the browser's navigation controls to return to the archive.</p>
    <p>Share these files only where plaintext access is intentional.</p>
    <a href="./">Back to Archive</a>
</body>
</html>
"#;

const UNENCRYPTED_RECOVERY_HTML_TEMPLATE: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Access - Unencrypted CASS Archive</title>
</head>
<body>
    <h1>No Recovery Key Is Needed</h1>
    <p>This archive is not encrypted, so it has no archive password or recovery key. Anyone who can access these files can read the included conversations.</p>
    <a href="./">Back to Archive</a>
</body>
</html>
"#;

const UNENCRYPTED_ABOUT_TXT_TEMPLATE: &str = r#"UNENCRYPTED CODING SESSION ARCHIVE
==================================

WARNING: This archive is not encrypted. Anyone who can access these files
can read and copy all included conversation content without a password.

This archive contains {conversation_count} conversations.
Serve this directory with `cass pages --preview <dir>` and open the printed
http://127.0.0.1 address, or open index.html directly.

Share these files only where plaintext access is intentional.

---
Created: {date}
Version: CASS v{version}
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pages::summary::{
        AgentSummaryItem, DateRange, EncryptionSummary, KeySlotSummary, PrePublishSummary,
        ScanReportSummary, WorkspaceSummaryItem,
    };

    macro_rules! assert_doc_contains {
        ($doc:expr, $needle:literal) => {
            assert!($doc.content.contains($needle));
        };
    }

    fn create_test_summary() -> PrePublishSummary {
        PrePublishSummary {
            total_conversations: 42,
            total_messages: 1000,
            total_characters: 500_000,
            estimated_size_bytes: 200_000,
            earliest_timestamp: Some(Utc::now() - chrono::Duration::days(30)),
            latest_timestamp: Some(Utc::now()),
            date_histogram: vec![],
            workspaces: vec![WorkspaceSummaryItem {
                path: "/home/user/project".to_string(),
                display_name: "project".to_string(),
                conversation_count: 20,
                message_count: 500,
                date_range: DateRange {
                    earliest: None,
                    latest: None,
                },
                sample_titles: vec!["Fix bug".to_string()],
                included: true,
            }],
            agents: vec![
                AgentSummaryItem {
                    name: "claude-code".to_string(),
                    conversation_count: 30,
                    message_count: 700,
                    percentage: 71.4,
                    included: true,
                },
                AgentSummaryItem {
                    name: "aider".to_string(),
                    conversation_count: 12,
                    message_count: 300,
                    percentage: 28.6,
                    included: true,
                },
            ],
            secret_scan: ScanReportSummary::default(),
            encryption_config: None,
            key_slots: Vec::new(),
            generated_at: Utc::now(),
        }
    }

    #[test]
    fn test_generate_readme() {
        let config = DocConfig::new();
        let summary = create_test_summary();
        let generator = DocumentationGenerator::new(config, summary);

        let doc = generator.generate_readme();

        assert_eq!(doc.filename, "README.md");
        assert_eq!(doc.location, DocLocation::RepoRoot);
        assert_doc_contains!(doc, "Unencrypted Coding Session Archive");
        assert_doc_contains!(doc, "42 conversations");
        assert_doc_contains!(doc, "claude-code");
        assert_doc_contains!(doc, "aider");
        assert_doc_contains!(doc, "cass pages --preview");
    }

    #[test]
    fn test_generate_security_doc() {
        let config = DocConfig::new();
        let summary = create_test_summary();
        let generator = DocumentationGenerator::new(config, summary);

        let doc = generator.generate_security_doc();

        assert_eq!(doc.filename, "SECURITY.md");
        assert_eq!(doc.location, DocLocation::RepoRoot);
        assert_doc_contains!(doc, "Security Model");
        assert_doc_contains!(doc, "not encrypted");
    }

    #[test]
    fn test_generate_help_html() {
        let config = DocConfig::new();
        let summary = create_test_summary();
        let generator = DocumentationGenerator::new(config, summary);

        let doc = generator.generate_help_html();

        assert_eq!(doc.filename, "help.html");
        assert_eq!(doc.location, DocLocation::WebRoot);
        assert_doc_contains!(doc, "<!DOCTYPE html>");
        assert_doc_contains!(doc, "Unencrypted Archive Help");
    }

    #[test]
    fn test_generate_recovery_html() {
        let config = DocConfig::new();
        let summary = create_test_summary();
        let generator = DocumentationGenerator::new(config, summary);

        let doc = generator.generate_recovery_html();

        assert_eq!(doc.filename, "recovery.html");
        assert_eq!(doc.location, DocLocation::WebRoot);
        assert_doc_contains!(doc, "No Recovery Key Is Needed");
    }

    #[test]
    fn test_generate_about_txt() {
        let config = DocConfig::new();
        let summary = create_test_summary();
        let generator = DocumentationGenerator::new(config, summary);

        let doc = generator.generate_about_txt();

        assert_eq!(doc.filename, "about.txt");
        assert_eq!(doc.location, DocLocation::WebRoot);
        assert_doc_contains!(doc, "UNENCRYPTED CODING SESSION ARCHIVE");
        assert_doc_contains!(doc, "42 conversations");
        assert_doc_contains!(doc, "cass pages --preview");
    }

    #[test]
    fn test_generate_all() {
        let config = DocConfig::new();
        let summary = create_test_summary();
        let generator = DocumentationGenerator::new(config, summary);

        let docs = generator.generate_all();

        assert_eq!(docs.len(), 5);

        let filenames: Vec<_> = docs.iter().map(|d| d.filename.as_str()).collect();
        assert!(filenames.contains(&"README.md"));
        assert!(filenames.contains(&"SECURITY.md"));
        assert!(filenames.contains(&"help.html"));
        assert!(filenames.contains(&"recovery.html"));
        assert!(filenames.contains(&"about.txt"));

        // Check locations
        let repo_root_count = docs
            .iter()
            .filter(|d| d.location == DocLocation::RepoRoot)
            .count();
        let web_root_count = docs
            .iter()
            .filter(|d| d.location == DocLocation::WebRoot)
            .count();
        assert_eq!(repo_root_count, 2); // README.md, SECURITY.md
        assert_eq!(web_root_count, 3); // help.html, recovery.html, about.txt
    }

    #[test]
    fn test_doc_config_builder() {
        let config = DocConfig::new().with_archive_mode(ArchiveMode::Unencrypted);
        assert_eq!(config.archive_mode, ArchiveMode::Unencrypted);
    }

    #[test]
    fn unencrypted_docs_never_claim_encrypted_or_password_gated_access() {
        let config = DocConfig::new().with_archive_mode(ArchiveMode::Unencrypted);
        let generator = DocumentationGenerator::new(config, create_test_summary());

        for doc in generator.generate_all() {
            let lower = doc.content.to_ascii_lowercase();
            assert!(
                lower.contains("not encrypted"),
                "{} must explicitly disclose plaintext access",
                doc.filename
            );
            for false_claim in [
                "AES-256-GCM",
                "Argon2id",
                "Enter the password",
                "correct password",
                "protected with",
            ] {
                assert!(
                    !doc.content.contains(false_claim),
                    "{} retained encrypted-mode claim {false_claim:?}",
                    doc.filename
                );
            }
        }
    }

    #[test]
    fn test_readme_points_at_local_preview() {
        let config = DocConfig::new();
        let summary = create_test_summary();
        let generator = DocumentationGenerator::new(config, summary);

        let doc = generator.generate_readme();

        assert_doc_contains!(doc, "cass pages --preview");
        assert_doc_contains!(doc, "http://127.0.0.1");
        // The only URL left in the README is the CASS source-code link.
        assert!(!doc.content.contains("github.io"));
        assert!(!doc.content.contains("pages.dev"));
    }

    #[test]
    fn test_about_points_at_local_preview() {
        let config = DocConfig::new();
        let summary = create_test_summary();
        let generator = DocumentationGenerator::new(config, summary);

        let doc = generator.generate_about_txt();

        assert_doc_contains!(doc, "cass pages --preview");
    }

    #[test]
    fn test_no_placeholders_remain() {
        let config = DocConfig::new();
        let summary = create_test_summary();
        let generator = DocumentationGenerator::new(config, summary);

        let docs = generator.generate_all();

        for doc in docs {
            // Check that common placeholders are filled
            assert!(
                !doc.content.contains("{url}"),
                "Unfilled {{url}} in {}",
                doc.filename
            );
            assert!(
                !doc.content.contains("{conversation_count}"),
                "Unfilled {{conversation_count}} in {}",
                doc.filename
            );
            assert!(
                !doc.content.contains("{version}"),
                "Unfilled {{version}} in {}",
                doc.filename
            );
        }
    }
}
