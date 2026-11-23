use crate::pages::secret_scan::{SecretScanReport, SecretScanSummary};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Pre-publish summary containing all information about content to be exported.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrePublishSummary {
    // Quantitative metrics
    /// Total number of conversations to be exported.
    pub total_conversations: usize,
    /// Total number of messages across all conversations.
    pub total_messages: usize,
    /// Total character count of all message content.
    pub total_characters: usize,
    /// Estimated size in bytes after compression and encryption.
    pub estimated_size_bytes: usize,

    // Temporal scope
    /// Earliest timestamp in the export set.
    pub earliest_timestamp: Option<DateTime<Utc>>,
    /// Latest timestamp in the export set.
    pub latest_timestamp: Option<DateTime<Utc>>,
    /// Histogram of messages per day.
    pub date_histogram: Vec<DateHistogramEntry>,

    // Content categorization
    /// Per-workspace breakdown.
    pub workspaces: Vec<WorkspaceSummaryItem>,
    /// Per-agent breakdown.
    pub agents: Vec<AgentSummaryItem>,

    // Security status
    /// Summary of secret scan results.
    pub secret_scan: ScanReportSummary,
    /// Encryption configuration summary.
    pub encryption_config: Option<EncryptionSummary>,
    /// Key slots configured for the export.
    pub key_slots: Vec<KeySlotSummary>,

    /// When this summary was generated.
    pub generated_at: DateTime<Utc>,
}

/// Entry in the date histogram.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DateHistogramEntry {
    /// Date in YYYY-MM-DD format.
    pub date: String,
    /// Number of messages on this date.
    pub message_count: usize,
    /// Number of unique conversations active on this date.
    pub conversation_count: usize,
}

/// Summary of a workspace's content in the export.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceSummaryItem {
    /// Full path of the workspace.
    pub path: String,
    /// Display name (last path component).
    pub display_name: String,
    /// Number of conversations in this workspace.
    pub conversation_count: usize,
    /// Number of messages in this workspace.
    pub message_count: usize,
    /// Date range of conversations in this workspace.
    pub date_range: DateRange,
    /// Sample of conversation titles (first 5).
    pub sample_titles: Vec<String>,
    /// Whether this workspace is included in export.
    pub included: bool,
}

/// Summary of an agent's content in the export.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSummaryItem {
    /// Agent identifier (e.g., "claude-code", "aider").
    pub name: String,
    /// Number of conversations from this agent.
    pub conversation_count: usize,
    /// Number of messages from this agent.
    pub message_count: usize,
    /// Percentage of total conversations.
    pub percentage: f64,
    /// Whether this agent is included in export.
    pub included: bool,
}

/// Date range with optional bounds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DateRange {
    /// Earliest timestamp (RFC3339).
    pub earliest: Option<String>,
    /// Latest timestamp (RFC3339).
    pub latest: Option<String>,
}

impl DateRange {
    /// Create a new date range from optional timestamps.
    pub fn from_timestamps(earliest: Option<i64>, latest: Option<i64>) -> Self {
        Self {
            earliest: earliest
                .and_then(DateTime::from_timestamp_millis)
                .map(|dt| dt.to_rfc3339()),
            latest: latest
                .and_then(DateTime::from_timestamp_millis)
                .map(|dt| dt.to_rfc3339()),
        }
    }

    /// Get the span in days, if both bounds are present.
    pub fn span_days(&self) -> Option<i64> {
        let earliest = self.earliest.as_ref()?.parse::<DateTime<Utc>>().ok()?;
        let latest = self.latest.as_ref()?.parse::<DateTime<Utc>>().ok()?;
        Some((latest - earliest).num_days())
    }
}

/// Summary of secret scan results.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScanReportSummary {
    /// Total number of findings.
    pub total_findings: usize,
    /// Breakdown by severity.
    pub by_severity: HashMap<String, usize>,
    /// Whether any critical secrets were found.
    pub has_critical: bool,
    /// Whether findings were truncated due to max limit.
    pub truncated: bool,
    /// Status message for display.
    pub status_message: String,
}

impl ScanReportSummary {
    fn status_message(total: usize, has_critical: bool, truncated: bool) -> String {
        if truncated {
            if total == 0 {
                return "Scan incomplete: results truncated before findings were recorded"
                    .to_string();
            }
            let critical = if has_critical {
                " (including CRITICAL)"
            } else {
                ""
            };
            return format!("At least {total} issues found{critical}; results truncated");
        }
        if total == 0 {
            "No secrets detected".to_string()
        } else if has_critical {
            format!("{total} issues found (including CRITICAL)")
        } else {
            format!("{total} issues found")
        }
    }

    /// Create from a full secret scan report.
    pub fn from_report(report: &SecretScanReport) -> Self {
        let by_severity: HashMap<String, usize> = report
            .summary
            .by_severity
            .iter()
            .map(|(k, v)| (k.label().to_string(), *v))
            .collect();

        let status_message = Self::status_message(
            report.summary.total,
            report.summary.has_critical,
            report.summary.truncated,
        );

        Self {
            total_findings: report.summary.total,
            by_severity,
            has_critical: report.summary.has_critical,
            truncated: report.summary.truncated,
            status_message,
        }
    }

    /// Create from a summary only.
    pub fn from_summary(summary: &SecretScanSummary) -> Self {
        let by_severity: HashMap<String, usize> = summary
            .by_severity
            .iter()
            .map(|(k, v)| (k.label().to_string(), *v))
            .collect();

        let status_message =
            Self::status_message(summary.total, summary.has_critical, summary.truncated);

        Self {
            total_findings: summary.total,
            by_severity,
            has_critical: summary.has_critical,
            truncated: summary.truncated,
            status_message,
        }
    }
}

/// Summary of encryption configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionSummary {
    /// Encryption algorithm used.
    pub algorithm: String,
    /// Key derivation function.
    pub key_derivation: String,
    /// Number of key slots configured.
    pub key_slot_count: usize,
    /// Estimated decryption time (for display).
    pub estimated_decrypt_time_secs: u64,
}

impl Default for EncryptionSummary {
    fn default() -> Self {
        Self {
            algorithm: "none".to_string(),
            key_derivation: "none".to_string(),
            key_slot_count: 0,
            estimated_decrypt_time_secs: 0,
        }
    }
}

/// Type of key slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeySlotType {
    Password,
    QrCode,
    Recovery,
}

impl KeySlotType {
    /// Display label for the slot type.
    pub fn label(self) -> &'static str {
        match self {
            KeySlotType::Password => "Password",
            KeySlotType::QrCode => "QR Code",
            KeySlotType::Recovery => "Recovery Key",
        }
    }
}

/// Summary of a key slot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeySlotSummary {
    /// Slot index (0-based).
    pub slot_index: usize,
    /// Type of the slot.
    pub slot_type: KeySlotType,
    /// Optional hint for the slot.
    pub hint: Option<String>,
    /// When the slot was created.
    pub created_at: Option<DateTime<Utc>>,
}

/// Filters for summary generation.
#[derive(Debug, Clone, Default)]
pub struct SummaryFilters {
    /// Filter to specific agents.
    pub agents: Option<Vec<String>>,
    /// Filter to specific workspaces.
    pub workspaces: Option<Vec<String>>,
    /// Filter to conversations after this timestamp (millis).
    pub since_ts: Option<i64>,
    /// Filter to conversations before this timestamp (millis).
    pub until_ts: Option<i64>,
}

/// Estimate compressed size from character count.
/// Uses rough heuristic: ~40% of original after compression + encryption overhead.
pub fn estimate_compressed_size(char_count: usize) -> usize {
    let base_size = (char_count as f64 * 0.4) as usize;
    // Add ~5% for encryption overhead (nonces, auth tags, etc.)
    (base_size as f64 * 1.05) as usize
}

/// Format a byte size for display.
pub fn format_size(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = KB * 1024;
    const GB: usize = MB * 1024;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

impl PrePublishSummary {
    /// Render a text overview of the summary.
    pub fn render_overview(&self) -> String {
        let mut output = String::new();

        output.push_str("CONTENT OVERVIEW\n");
        output.push_str("----------------\n");
        output.push_str(&format!("Conversations: {}\n", self.total_conversations));
        output.push_str(&format!("Messages:      {}\n", self.total_messages));
        output.push_str(&format!(
            "Characters:    {} (~{})\n",
            self.total_characters,
            format_size(self.total_characters)
        ));
        output.push_str(&format!(
            "Archive Size:  ~{} (estimated, compressed + encrypted)\n",
            format_size(self.estimated_size_bytes)
        ));
        output.push('\n');

        output.push_str("DATE RANGE\n");
        output.push_str("----------\n");
        if let (Some(earliest), Some(latest)) = (&self.earliest_timestamp, &self.latest_timestamp) {
            let days = (*latest - *earliest).num_days();
            output.push_str(&format!(
                "From: {}  To: {}  ({} days)\n",
                earliest.format("%Y-%m-%d"),
                latest.format("%Y-%m-%d"),
                days
            ));
        } else {
            output.push_str("No date information available\n");
        }
        output.push('\n');

        output.push_str(&format!("WORKSPACES ({})\n", self.workspaces.len()));
        output.push_str("--------------\n");
        for ws in self.workspaces.iter().take(5) {
            let included_marker = if ws.included { " " } else { "x" };
            output.push_str(&format!(
                "[{}] {} ({} conversations)\n",
                included_marker, ws.display_name, ws.conversation_count
            ));
            if !ws.sample_titles.is_empty() {
                let titles: Vec<_> = ws.sample_titles.iter().take(3).cloned().collect();
                output.push_str(&format!("    \"{}\"...\n", titles.join("\", \"")));
            }
        }
        if self.workspaces.len() > 5 {
            output.push_str(&format!("... and {} more\n", self.workspaces.len() - 5));
        }
        output.push('\n');

        output.push_str("AGENTS\n");
        output.push_str("------\n");
        for agent in &self.agents {
            output.push_str(&format!(
                "  {}: {} conversations ({:.0}%)\n",
                agent.name, agent.conversation_count, agent.percentage
            ));
        }
        output.push('\n');

        output.push_str("SECURITY\n");
        output.push_str("--------\n");
        if let Some(enc) = &self.encryption_config {
            output.push_str(&format!("Encryption: {}\n", enc.algorithm));
            output.push_str(&format!("Key Derivation: {}\n", enc.key_derivation));
            output.push_str(&format!("Key Slots: {}\n", enc.key_slot_count));
        }
        output.push_str(&format!(
            "Secret Scan: {}\n",
            self.secret_scan.status_message
        ));

        output
    }

    /// Get count of included workspaces.
    pub fn included_workspace_count(&self) -> usize {
        self.workspaces.iter().filter(|w| w.included).count()
    }

    /// Get count of included agents.
    pub fn included_agent_count(&self) -> usize {
        self.agents.iter().filter(|a| a.included).count()
    }

    /// Update with secret scan results.
    pub fn set_secret_scan(&mut self, report: &SecretScanReport) {
        self.secret_scan = ScanReportSummary::from_report(report);
    }
}
