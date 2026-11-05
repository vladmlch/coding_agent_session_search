use anyhow::{Context, Result, bail};
use console::{Term, style};
use dialoguer::{Confirm, Input, MultiSelect, Password, Select, theme::ColorfulTheme};
use indicatif::{ProgressBar, ProgressStyle};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use crate::franken_sync::Connection;
use crate::pages::bundle::{BundleBuilder, BundleConfig};
use crate::pages::confirmation::{
    ConfirmationConfig, ConfirmationFlow, ConfirmationStep, PasswordStrengthAction, StepValidation,
    UNENCRYPTED_ACK_PHRASE, unencrypted_warning_lines, validate_unencrypted_ack,
};
use crate::pages::docs::{ArchiveMode, DocConfig, DocumentationGenerator};
use crate::pages::encrypt::{EncryptionConfig, EncryptionEngine};
use crate::pages::export::{ExportEngine, ExportFilter, PathMode};
use crate::pages::password::{PasswordStrength, format_strength_inline, validate_password};
use crate::pages::profiles::ShareProfile;
use crate::pages::secret_scan::{
    SecretScanConfig, SecretScanFilters, print_human_report, scan_staged_export_database,
    wizard_secret_scan,
};
use crate::pages::size::SizeEstimate;
use crate::pages::summary::{
    ExclusionSet, PrePublishSummary, SummaryFilters, SummaryGenerator, format_size,
};
use crate::storage::sqlite::FrankenStorage;

/// Wizard state tracking all configuration
#[derive(Clone)]
pub struct WizardState {
    // Content selection
    pub agents: Vec<String>,
    pub time_range: Option<String>,
    pub workspaces: Option<Vec<PathBuf>>,

    // Security configuration
    pub password: Option<String>,
    pub recovery_secret: Option<Vec<u8>>,
    pub generate_recovery: bool,
    pub generate_qr: bool,

    // Site configuration
    pub title: String,
    pub description: String,
    pub hide_metadata: bool,
    /// Share profile chosen by flag, config or prompt; `None` means the
    /// default for the encryption mode (see `effective_share_profile`).
    pub share_profile: Option<ShareProfile>,

    // Local output
    pub output_dir: PathBuf,

    // Database path
    pub db_path: PathBuf,

    // Pre-publish summary and exclusions
    pub exclusions: ExclusionSet,
    pub last_summary: Option<PrePublishSummary>,

    // Secret scan results
    pub secret_scan_has_findings: bool,
    pub secret_scan_has_critical: bool,
    pub secret_scan_count: usize,

    // Password entropy
    pub password_entropy_bits: f64,

    // Unencrypted export mode (DANGEROUS)
    pub no_encryption: bool,
    pub unencrypted_confirmed: bool,

    // Final output location (set after export)
    pub final_site_dir: Option<PathBuf>,
}

impl std::fmt::Debug for WizardState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WizardState")
            .field("agents", &self.agents)
            .field("time_range", &self.time_range)
            .field("workspaces", &self.workspaces)
            .field("password", &self.password.as_ref().map(|_| "[REDACTED]"))
            .field(
                "recovery_secret",
                &self.recovery_secret.as_ref().map(|_| "[REDACTED]"),
            )
            .field("generate_recovery", &self.generate_recovery)
            .field("generate_qr", &self.generate_qr)
            .field("title", &self.title)
            .field("description", &self.description)
            .field("hide_metadata", &self.hide_metadata)
            .field("share_profile", &self.share_profile)
            .field("output_dir", &self.output_dir)
            .field("db_path", &self.db_path)
            .field("exclusions", &self.exclusions)
            .field("last_summary", &self.last_summary)
            .field("secret_scan_has_findings", &self.secret_scan_has_findings)
            .field("secret_scan_has_critical", &self.secret_scan_has_critical)
            .field("secret_scan_count", &self.secret_scan_count)
            .field("password_entropy_bits", &self.password_entropy_bits)
            .field("no_encryption", &self.no_encryption)
            .field("unencrypted_confirmed", &self.unencrypted_confirmed)
            .field("final_site_dir", &self.final_site_dir)
            .finish()
    }
}

impl Default for WizardState {
    fn default() -> Self {
        let db_path = crate::default_db_path();

        Self {
            agents: Vec::new(),
            time_range: None,
            workspaces: None,
            password: None,
            recovery_secret: None,
            generate_recovery: true,
            generate_qr: false,
            title: "cass Archive".to_string(),
            description: "Encrypted archive of AI coding agent conversations".to_string(),
            hide_metadata: false,
            share_profile: None,
            output_dir: PathBuf::from("cass-export"),
            db_path,
            exclusions: ExclusionSet::new(),
            last_summary: None,
            secret_scan_has_findings: false,
            secret_scan_has_critical: false,
            secret_scan_count: 0,
            password_entropy_bits: 0.0,
            no_encryption: false,
            unencrypted_confirmed: false,
            final_site_dir: None,
        }
    }
}

impl WizardState {
    /// The share profile this export applies (2l1b0.60): the one chosen,
    /// else the strictest preset for a plaintext export and `team` for an
    /// encrypted one.
    pub fn effective_share_profile(&self) -> ShareProfile {
        self.share_profile
            .unwrap_or_else(|| ShareProfile::default_for(!self.no_encryption))
    }
}

fn truncate_sample_title(title: &str) -> String {
    if title.len() > 30 {
        format!("{}...", &title[..title.floor_char_boundary(27)])
    } else {
        title.to_string()
    }
}

fn documentation_summary(
    summary: &PrePublishSummary,
    encryption_config: Option<&EncryptionConfig>,
) -> (PrePublishSummary, ArchiveMode) {
    let mut summary = summary.clone();
    match encryption_config {
        Some(config) => {
            summary.set_encryption_config(&config.key_slots);
            (summary, ArchiveMode::Encrypted)
        }
        None => {
            summary.encryption_config = None;
            summary.key_slots.clear();
            (summary, ArchiveMode::Unencrypted)
        }
    }
}

fn wizard_archive_description(no_encryption: bool) -> &'static str {
    if no_encryption {
        "Create a searchable web archive without encryption. Published content will be plaintext."
    } else {
        "Create an encrypted, searchable web archive of your AI coding agent conversations."
    }
}

fn archive_size_summary_line(no_encryption: bool, estimated_size_bytes: usize) -> String {
    if no_encryption {
        "  Archive Size:  calculated from the final unencrypted SQLite file after export"
            .to_string()
    } else {
        format!(
            "  Archive Size:  ~{} (estimated, compressed + encrypted)",
            format_size(estimated_size_bytes)
        )
    }
}

fn summary_security_lines(no_encryption: bool, planned_key_slots: usize) -> [String; 3] {
    if no_encryption {
        [
            "Encryption: Disabled (public plaintext)".to_string(),
            "Key Derivation: Not applicable".to_string(),
            "Key Slots: 0".to_string(),
        ]
    } else {
        [
            "Encryption: AES-256-GCM".to_string(),
            "Key Derivation: Argon2id".to_string(),
            format!("Planned Key Slots: {planned_key_slots}"),
        ]
    }
}

pub struct PagesWizard {
    state: WizardState,
    no_encryption_mode: bool,
}

impl Default for PagesWizard {
    fn default() -> Self {
        Self::new()
    }
}

impl PagesWizard {
    pub fn new() -> Self {
        Self {
            state: WizardState::default(),
            no_encryption_mode: false,
        }
    }

    /// Override the database path used for agent/workspace discovery and export.
    pub fn set_db_path(&mut self, db_path: PathBuf) {
        self.state.db_path = db_path;
    }

    /// Set whether to skip encryption (DANGEROUS - requires explicit confirmation).
    pub fn set_no_encryption(&mut self, no_encryption: bool) {
        self.no_encryption_mode = no_encryption;
        self.state.no_encryption = no_encryption;
    }

    /// Choose the share profile up front (skips the wizard's prompt).
    pub fn set_share_profile(&mut self, profile: ShareProfile) {
        self.state.share_profile = Some(profile);
    }

    pub fn run(&mut self) -> Result<()> {
        let mut term = Term::stdout();
        let theme = ColorfulTheme::default();

        term.clear_screen()?;
        self.print_header(&mut term)?;

        if self.no_encryption_mode && !self.step_unencrypted_warning(&mut term, &theme)? {
            writeln!(term, "{}", style("Export cancelled.").yellow())?;
            return Ok(());
        }

        // Step 1: Content Selection
        self.step_content_selection(&mut term, &theme)?;

        // Step 2: Secret Scan
        self.step_secret_scan(&mut term, &theme)?;

        // Step 3: Security Configuration
        if !self.no_encryption_mode {
            self.step_security_config(&mut term, &theme)?;
        } else {
            self.state.generate_recovery = false;
            self.state.generate_qr = false;
        }

        // Step 4: Site Configuration
        self.step_site_config(&mut term, &theme)?;

        // Step 5: Output Location
        self.step_output_location(&mut term, &theme)?;

        // Step 6: Pre-Publish Summary
        if !self.step_summary(&mut term, &theme)? {
            writeln!(term, "{}", style("Export cancelled.").yellow())?;
            return Ok(());
        }

        // Step 7: Safety Confirmation
        if !self.no_encryption_mode && !self.step_confirmation(&mut term, &theme)? {
            writeln!(term, "{}", style("Export cancelled.").yellow())?;
            return Ok(());
        }

        // Step 8: Export Progress
        self.step_export(&mut term)?;

        // Step 9: Finish (local preview hint)
        self.step_finish(&mut term)?;

        Ok(())
    }

    /// Step for unencrypted export warning and confirmation.
    fn step_unencrypted_warning(&mut self, term: &mut Term, theme: &ColorfulTheme) -> Result<bool> {
        writeln!(term)?;
        writeln!(term, "{}", style("⚠️  SECURITY WARNING").red().bold())?;
        writeln!(term, "{}", style("━".repeat(60)).red())?;
        writeln!(term)?;

        for line in unencrypted_warning_lines() {
            if line.is_empty() {
                writeln!(term)?;
            } else {
                writeln!(term, "  {}", line)?;
            }
        }

        writeln!(term)?;
        writeln!(term, "{}", style("━".repeat(60)).red())?;
        writeln!(term)?;
        writeln!(term, "To proceed with unencrypted export, type exactly:")?;
        writeln!(term)?;
        writeln!(term, "  {}", style(UNENCRYPTED_ACK_PHRASE).cyan().bold())?;
        writeln!(term)?;

        loop {
            let input: String = Input::with_theme(theme)
                .with_prompt("Your input (or \"cancel\" to abort)")
                .interact_text()?;

            if input.trim().to_lowercase() == "cancel" {
                return Ok(false);
            }

            match validate_unencrypted_ack(&input) {
                StepValidation::Passed => {
                    // Additional y/N confirmation
                    writeln!(term)?;
                    let confirmed = Confirm::with_theme(theme)
                        .with_prompt(
                            "Are you ABSOLUTELY SURE you want to export WITHOUT encryption?",
                        )
                        .default(false)
                        .interact()?;

                    if !confirmed {
                        writeln!(term)?;
                        writeln!(
                            term,
                            "  {}",
                            style("Good choice. Export cancelled.").green()
                        )?;
                        writeln!(
                            term,
                            "  Remove --no-encryption to export with encryption (recommended)."
                        )?;
                        return Ok(false);
                    }

                    self.state.unencrypted_confirmed = true;
                    writeln!(term)?;
                    writeln!(
                        term,
                        "  {} Unencrypted export acknowledged",
                        style("⚠").yellow()
                    )?;
                    writeln!(
                        term,
                        "  {}",
                        style("Proceeding without encryption...").dim()
                    )?;
                    return Ok(true);
                }
                StepValidation::Failed(msg) => {
                    writeln!(term, "  {} {}", style("✗").red(), msg)?;
                }
            }
        }
    }

    fn print_header(&self, term: &mut Term) -> Result<()> {
        writeln!(
            term,
            "{}",
            style("🔐 cass Pages Export Wizard").bold().cyan()
        )?;
        writeln!(
            term,
            "{}",
            wizard_archive_description(self.no_encryption_mode)
        )?;
        writeln!(term)?;
        Ok(())
    }

    fn step_content_selection(&mut self, term: &mut Term, theme: &ColorfulTheme) -> Result<()> {
        writeln!(term, "\n{}", style("Step 1 of 9: Content Selection").bold())?;
        writeln!(term, "{}", style("─".repeat(40)).dim())?;

        // Load agents dynamically from database
        let storage = FrankenStorage::open_readonly(&self.state.db_path)
            .context("Failed to open database. Run 'cass index' first.")?;
        let db_agents = storage.list_agents()?;
        let db_workspaces = storage.list_workspaces()?;
        drop(storage);

        if db_agents.is_empty() {
            writeln!(
                term,
                "{}",
                style("⚠ No agents found in database. Run 'cass index' first.").red()
            )?;
            bail!("No agents found in database");
        }

        // Build agent display list with conversation counts
        let agent_items: Vec<String> = db_agents
            .iter()
            .map(|a| format!("{} ({})", a.name, a.slug))
            .collect();

        let selected_agents = MultiSelect::with_theme(theme)
            .with_prompt("Which agents would you like to include?")
            .items(&agent_items)
            .defaults(&vec![true; agent_items.len()])
            .interact()?;

        self.state.agents = selected_agents
            .iter()
            .map(|&i| db_agents[i].slug.clone())
            .collect();

        if self.state.agents.is_empty() {
            bail!("No agents selected. Export cancelled.");
        }

        writeln!(
            term,
            "  {} {} agents selected",
            style("✓").green(),
            self.state.agents.len()
        )?;

        // Workspace selection (optional)
        self.state.workspaces = None;
        if !db_workspaces.is_empty() {
            let include_all = Confirm::with_theme(theme)
                .with_prompt("Include all workspaces?")
                .default(true)
                .interact()?;

            if !include_all {
                let workspace_items: Vec<String> = db_workspaces
                    .iter()
                    .map(|w| {
                        w.display_name
                            .clone()
                            .unwrap_or_else(|| w.path.to_string_lossy().to_string())
                    })
                    .collect();

                let selected_ws = MultiSelect::with_theme(theme)
                    .with_prompt("Select workspaces to include:")
                    .items(&workspace_items)
                    .interact()?;

                self.state.workspaces = Some(
                    selected_ws
                        .iter()
                        .map(|&i| db_workspaces[i].path.clone())
                        .collect(),
                );
                writeln!(
                    term,
                    "  {} {} workspaces selected",
                    style("✓").green(),
                    selected_ws.len()
                )?;
                if selected_ws.is_empty() {
                    writeln!(
                        term,
                        "  {} No workspaces selected. The export will contain no conversations.",
                        style("ℹ").yellow()
                    )?;
                }
            }
        }

        // Time Range
        let time_options = vec![
            "All time",
            "Last 7 days",
            "Last 30 days",
            "Last 90 days",
            "Last year",
        ];
        let time_selection = Select::with_theme(theme)
            .with_prompt("Time range")
            .default(0)
            .items(&time_options)
            .interact()?;

        self.state.time_range = match time_selection {
            1 => Some("-7d".to_string()),
            2 => Some("-30d".to_string()),
            3 => Some("-90d".to_string()),
            4 => Some("-365d".to_string()),
            _ => None,
        };

        writeln!(
            term,
            "  {} Time range: {}",
            style("✓").green(),
            time_options[time_selection]
        )?;

        Ok(())
    }

    fn step_secret_scan(&mut self, term: &mut Term, _theme: &ColorfulTheme) -> Result<()> {
        writeln!(term, "\n{}", style("Step 2 of 9: Secret Scan").bold())?;
        writeln!(term, "{}", style("─".repeat(40)).dim())?;
        writeln!(
            term,
            "  {} Preliminary source review; the exact staged export will be scanned again before encryption.",
            style("ℹ").blue()
        )?;

        let since_ts = self
            .state
            .time_range
            .as_deref()
            .and_then(crate::ui::time_parser::parse_time_input);

        let filters = SecretScanFilters {
            agents: if self.state.agents.is_empty() {
                None
            } else {
                Some(self.state.agents.clone())
            },
            workspaces: self.state.workspaces.clone(),
            since_ts,
            until_ts: None,
        };

        let config = SecretScanConfig::from_inputs(&[], &[])?;
        if !config.allowlist_raw.is_empty() || !config.denylist_raw.is_empty() {
            writeln!(
                term,
                "  {} Allowlist patterns: {} | Denylist patterns: {}",
                style("ℹ").blue(),
                config.allowlist_raw.len(),
                config.denylist_raw.len()
            )?;
        }

        let report = wizard_secret_scan(&self.state.db_path, &filters, &config)?;
        print_human_report(term, &report, 3)?;

        // Save secret scan results to state for confirmation flow
        self.state.secret_scan_has_findings = report.summary.total > 0;
        self.state.secret_scan_has_critical = report.summary.has_critical;
        self.state.secret_scan_count = report.summary.total;

        if report.summary.has_critical {
            writeln!(
                term,
                "  {} Critical findings are present in this preliminary review.",
                style("⚠").yellow()
            )?;
            writeln!(
                term,
                "  Configuration may continue, but only the later exact staged-artifact scan can authorize encryption or publication."
            )?;
        }

        Ok(())
    }

    fn step_security_config(&mut self, term: &mut Term, theme: &ColorfulTheme) -> Result<()> {
        writeln!(
            term,
            "\n{}",
            style("Step 3 of 9: Security Configuration").bold()
        )?;
        writeln!(term, "{}", style("─".repeat(40)).dim())?;

        // Password
        let password = Password::with_theme(theme)
            .with_prompt("Archive password (min 8 characters)")
            .with_confirmation("Confirm password", "Passwords don't match")
            .validate_with(|input: &String| -> Result<(), &str> {
                if input.chars().count() >= 8 {
                    Ok(())
                } else {
                    Err("Password must be at least 8 characters")
                }
            })
            .interact()?;

        self.state.password = Some(password.clone());
        writeln!(term, "  {} Password set", style("✓").green())?;

        // Validate password using new password strength module
        let validation = validate_password(&password);

        // Calculate and save password entropy for confirmation flow
        self.state.password_entropy_bits = validation.entropy_bits;

        // Show password strength indicator with visual bar
        writeln!(
            term,
            "    Password strength: {}",
            format_strength_inline(&validation)
        )?;
        writeln!(term, "    Entropy: {:.0} bits", validation.entropy_bits)?;

        // Show improvement suggestions if not strong
        if validation.strength != PasswordStrength::Strong && !validation.suggestions.is_empty() {
            writeln!(term, "    {}", style("Suggestions:").dim())?;
            for suggestion in &validation.suggestions {
                writeln!(
                    term,
                    "      {} {}",
                    style("•").dim(),
                    style(suggestion).dim()
                )?;
            }
        }

        // Recovery secret
        self.state.generate_recovery = Confirm::with_theme(theme)
            .with_prompt("Generate recovery secret? (recommended)")
            .default(true)
            .interact()?;

        if self.state.generate_recovery {
            writeln!(
                term,
                "  {} Recovery secret will be generated",
                style("✓").green()
            )?;
        }

        // QR code
        self.state.generate_qr = Confirm::with_theme(theme)
            .with_prompt("Generate QR code for recovery? (for mobile access)")
            .default(false)
            .interact()?;

        if self.state.generate_qr {
            writeln!(term, "  {} QR code will be generated", style("✓").green())?;
        }

        Ok(())
    }

    fn step_site_config(&mut self, term: &mut Term, theme: &ColorfulTheme) -> Result<()> {
        writeln!(
            term,
            "\n{}",
            style("Step 4 of 9: Site Configuration").bold()
        )?;
        writeln!(term, "{}", style("─".repeat(40)).dim())?;

        // Title
        self.state.title = Input::with_theme(theme)
            .with_prompt("Archive title")
            .default(self.state.title.clone())
            .interact_text()?;

        writeln!(term, "  {} Title: {}", style("✓").green(), self.state.title)?;

        // Description
        self.state.description = Input::with_theme(theme)
            .with_prompt("Description (shown on unlock page)")
            .default(self.state.description.clone())
            .interact_text()?;

        writeln!(term, "  {} Description set", style("✓").green())?;

        // Metadata privacy
        self.state.hide_metadata = Confirm::with_theme(theme)
            .with_prompt("Hide workspace paths and file names? (for privacy)")
            .default(false)
            .interact()?;

        if self.state.hide_metadata {
            writeln!(term, "  {} Metadata will be obfuscated", style("✓").green())?;
        }

        // Share profile (2l1b0.60): a --share-profile flag skips the prompt.
        if self.state.share_profile.is_none() {
            let choices = [
                ShareProfile::Public,
                ShareProfile::Team,
                ShareProfile::Personal,
            ];
            let default_profile = self.state.effective_share_profile();
            let items: Vec<String> = choices
                .iter()
                .map(|profile| format!("{profile}: {}", profile.description()))
                .collect();
            let selection = Select::with_theme(theme)
                .with_prompt("Share profile (what to redact from every exported text)")
                .default(
                    choices
                        .iter()
                        .position(|profile| *profile == default_profile)
                        .unwrap_or(0),
                )
                .items(&items)
                .interact()?;
            self.state.share_profile = Some(choices[selection]);
        }
        writeln!(
            term,
            "  {} Share profile: {}",
            style("✓").green(),
            self.state.effective_share_profile()
        )?;

        Ok(())
    }

    fn step_output_location(&mut self, term: &mut Term, theme: &ColorfulTheme) -> Result<()> {
        writeln!(term, "\n{}", style("Step 5 of 9: Output Location").bold())?;
        writeln!(term, "{}", style("─".repeat(40)).dim())?;

        // Output directory
        self.state.output_dir = PathBuf::from(
            Input::<String>::with_theme(theme)
                .with_prompt("Output directory")
                .default("cass-export".to_string())
                .interact_text()?,
        );

        writeln!(
            term,
            "  {} Output: {}",
            style("✓").green(),
            self.state.output_dir.display()
        )?;

        Ok(())
    }

    fn step_summary(&mut self, term: &mut Term, theme: &ColorfulTheme) -> Result<bool> {
        writeln!(
            term,
            "\n{}",
            style("Step 6 of 9: Pre-Publish Summary").bold()
        )?;
        writeln!(term, "{}", style("─".repeat(40)).dim())?;

        // Generate comprehensive summary from database
        writeln!(term, "\n  Generating summary...")?;
        let mut summary = self.generate_prepublish_summary()?;
        if self.no_encryption_mode {
            summary.encryption_config = None;
            summary.key_slots.clear();
        }
        self.state.last_summary = Some(summary.clone());

        // Display content overview
        writeln!(term, "\n{}", style("📊 CONTENT OVERVIEW").bold().cyan())?;
        writeln!(term, "{}", style("─".repeat(40)).dim())?;
        writeln!(
            term,
            "  Conversations: {}",
            style(summary.total_conversations).green()
        )?;
        writeln!(
            term,
            "  Messages:      {}",
            style(summary.total_messages).green()
        )?;
        writeln!(
            term,
            "  Characters:    {} (~{})",
            summary.total_characters,
            format_size(summary.total_characters)
        )?;
        writeln!(
            term,
            "{}",
            archive_size_summary_line(self.no_encryption_mode, summary.estimated_size_bytes)
        )?;

        // Display date range
        writeln!(term, "\n{}", style("📅 DATE RANGE").bold().cyan())?;
        writeln!(term, "{}", style("─".repeat(40)).dim())?;
        if let (Some(earliest), Some(latest)) =
            (&summary.earliest_timestamp, &summary.latest_timestamp)
        {
            let days = (*latest - *earliest).num_days();
            writeln!(
                term,
                "  From: {}  To: {}  ({} days)",
                style(earliest.format("%Y-%m-%d")).white(),
                style(latest.format("%Y-%m-%d")).white(),
                days
            )?;

            // Show activity histogram (simplified sparkline)
            if !summary.date_histogram.is_empty() {
                let bars = ["▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"];

                // Group by month for display
                let mut months: std::collections::BTreeMap<String, usize> =
                    std::collections::BTreeMap::new();
                for entry in &summary.date_histogram {
                    if entry.date.len() >= 7 {
                        let month = &entry.date[0..7];
                        *months.entry(month.to_string()).or_insert(0) += entry.message_count;
                    }
                }

                if !months.is_empty() {
                    let month_max = months.values().max().copied().unwrap_or(1).max(1);
                    let sparkline: String = months
                        .values()
                        .map(|&count| {
                            let idx = (count * 7 / month_max).min(7);
                            bars[idx]
                        })
                        .collect();
                    writeln!(term, "  Activity: {}", style(sparkline).cyan())?;
                }
            }
        } else {
            writeln!(term, "  No date information available")?;
        }

        // Display workspaces
        writeln!(
            term,
            "\n{} ({})",
            style("📁 WORKSPACES").bold().cyan(),
            summary.workspaces.len()
        )?;
        writeln!(term, "{}", style("─".repeat(40)).dim())?;
        for (_idx, ws) in summary.workspaces.iter().enumerate().take(5) {
            let included_marker =
                if ws.included && !self.state.exclusions.is_workspace_excluded(&ws.path) {
                    style("✓").green()
                } else {
                    style("✗").red()
                };
            writeln!(
                term,
                "  {} {} ({} conversations)",
                included_marker,
                style(&ws.display_name).white(),
                ws.conversation_count
            )?;
            if !ws.sample_titles.is_empty() {
                let titles: Vec<_> = ws
                    .sample_titles
                    .iter()
                    .take(2)
                    .map(|t| truncate_sample_title(t))
                    .collect();
                writeln!(
                    term,
                    "      {}",
                    style(format!("\"{}\"", titles.join("\", \""))).dim()
                )?;
            }
        }
        if summary.workspaces.len() > 5 {
            writeln!(
                term,
                "  {} and {} more...",
                style("...").dim(),
                summary.workspaces.len() - 5
            )?;
        }

        // Display agents
        writeln!(term, "\n{}", style("🤖 AGENTS").bold().cyan())?;
        writeln!(term, "{}", style("─".repeat(40)).dim())?;
        for agent in &summary.agents {
            writeln!(
                term,
                "  • {}: {} conversations ({:.0}%)",
                style(&agent.name).white(),
                agent.conversation_count,
                agent.percentage
            )?;
        }

        // Display security status
        writeln!(term, "\n{}", style("🔒 SECURITY").bold().cyan())?;
        writeln!(term, "{}", style("─".repeat(40)).dim())?;
        let planned_key_slots =
            usize::from(self.state.password.is_some()) + usize::from(self.state.generate_recovery);
        for line in summary_security_lines(self.no_encryption_mode, planned_key_slots) {
            writeln!(term, "  {line}")?;
        }

        // This display reflects the preliminary source scan from Step 2. The
        // exact staged-artifact scan remains the publication authority later.
        let secret_status = if self.state.secret_scan_count == 0 {
            style("✓ No potential secrets detected".to_string()).green()
        } else if self.state.secret_scan_has_critical {
            style(format!(
                "⚠️  {} issues (CRITICAL)",
                self.state.secret_scan_count
            ))
            .red()
        } else {
            style(format!("⚠️  {} issues found", self.state.secret_scan_count)).yellow()
        };
        writeln!(term, "  Preliminary Secret Scan: {}", secret_status)?;

        // Configuration summary
        writeln!(term, "\n{}", style("⚙️  CONFIGURATION").bold().cyan())?;
        writeln!(term, "{}", style("─".repeat(40)).dim())?;
        writeln!(term, "  Title: {}", self.state.title)?;
        writeln!(
            term,
            "  Encryption: {}",
            if self.state.no_encryption {
                "Disabled (plaintext)"
            } else {
                "Enabled"
            }
        )?;
        writeln!(term, "  Output: {}", self.state.output_dir.display())?;
        writeln!(
            term,
            "  Recovery Key: {}",
            if self.state.generate_recovery {
                "Yes"
            } else {
                "No"
            }
        )?;
        writeln!(
            term,
            "  QR Code: {}",
            if self.state.generate_qr { "Yes" } else { "No" }
        )?;

        // Exclusion summary
        let (ws_excluded, conv_excluded, pattern_excluded) =
            self.state.exclusions.exclusion_counts();
        if ws_excluded > 0 || conv_excluded > 0 || pattern_excluded > 0 {
            writeln!(term, "\n{}", style("🚫 EXCLUSIONS").bold().yellow())?;
            writeln!(term, "{}", style("─".repeat(40)).dim())?;
            if ws_excluded > 0 {
                writeln!(term, "  {} workspace(s) excluded", ws_excluded)?;
            }
            if conv_excluded > 0 {
                writeln!(term, "  {} conversation(s) excluded", conv_excluded)?;
            }
            if pattern_excluded > 0 {
                writeln!(term, "  {} pattern(s) active", pattern_excluded)?;
            }
        }

        writeln!(term)?;

        // Options menu
        loop {
            let options = vec![
                "✓ Proceed with export",
                "📁 View/Edit workspace exclusions",
                "✗ Cancel export",
            ];

            let selection = Select::with_theme(theme)
                .with_prompt("What would you like to do?")
                .items(&options)
                .default(0)
                .interact()?;

            match selection {
                0 => return Ok(true), // Proceed
                1 => {
                    // Edit workspace exclusions
                    self.edit_workspace_exclusions(term, theme, &summary)?;
                }
                2 => return Ok(false), // Cancel
                _ => unreachable!(),
            }
        }
    }

    /// Generate the pre-publish summary from the database.
    fn generate_prepublish_summary(&self) -> Result<PrePublishSummary> {
        let conn = Connection::open(self.state.db_path.to_string_lossy().as_ref())
            .context("Failed to open database for summary generation")?;

        conn.execute_batch(
            "PRAGMA busy_timeout = 5000;
             PRAGMA journal_mode = WAL;",
        )
        .context("Failed to set PRAGMAs for summary generation")?;

        let since_ts = self
            .state
            .time_range
            .as_deref()
            .and_then(crate::ui::time_parser::parse_time_input);

        let filters = SummaryFilters {
            agents: if self.state.agents.is_empty() {
                None
            } else {
                Some(self.state.agents.clone())
            },
            workspaces: self
                .state
                .workspaces
                .as_ref()
                .map(|ws| ws.iter().map(|p| p.to_string_lossy().to_string()).collect()),
            since_ts,
            until_ts: None,
        };

        let generator = SummaryGenerator::new(&conn);
        let summary = generator.generate_with_exclusions(Some(&filters), &self.state.exclusions)?;

        Ok(summary)
    }

    /// Interactive workspace exclusion editing.
    fn edit_workspace_exclusions(
        &mut self,
        term: &mut Term,
        theme: &ColorfulTheme,
        summary: &PrePublishSummary,
    ) -> Result<()> {
        writeln!(term, "\n{}", style("Workspace Exclusions").bold())?;
        writeln!(term, "{}", style("─".repeat(40)).dim())?;

        if summary.workspaces.is_empty() {
            writeln!(term, "  No workspaces to configure.")?;
            return Ok(());
        }

        // Build list of workspaces with current inclusion status
        let items: Vec<String> = summary
            .workspaces
            .iter()
            .map(|ws| {
                format!(
                    "{} ({} conversations)",
                    ws.display_name, ws.conversation_count
                )
            })
            .collect();

        // Determine which are currently selected (included)
        let defaults: Vec<bool> = summary
            .workspaces
            .iter()
            .map(|ws| !self.state.exclusions.is_workspace_excluded(&ws.path))
            .collect();

        let selections = MultiSelect::with_theme(theme)
            .with_prompt("Select workspaces to INCLUDE (unselected will be excluded)")
            .items(&items)
            .defaults(&defaults)
            .interact()?;

        // Update exclusions based on selections
        for (idx, ws) in summary.workspaces.iter().enumerate() {
            if selections.contains(&idx) {
                // Include this workspace (remove from exclusions)
                self.state.exclusions.include_workspace(&ws.path);
            } else {
                // Exclude this workspace
                self.state.exclusions.exclude_workspace(&ws.path);
            }
        }

        let (ws_excluded, _, _) = self.state.exclusions.exclusion_counts();
        writeln!(
            term,
            "  {} {} workspace(s) now excluded",
            style("✓").green(),
            ws_excluded
        )?;

        Ok(())
    }

    /// Multi-step safety confirmation flow.
    fn step_confirmation(&mut self, term: &mut Term, theme: &ColorfulTheme) -> Result<bool> {
        writeln!(
            term,
            "\n{}",
            style("Step 7 of 9: Safety Confirmation").bold()
        )?;
        writeln!(term, "{}", style("─".repeat(40)).dim())?;

        // Build confirmation configuration
        let summary = if let Some(summary) = self.state.last_summary.clone() {
            summary
        } else {
            let generated = self
                .generate_prepublish_summary()
                .context("Failed to generate pre-publish summary for confirmation")?;
            self.state.last_summary = Some(generated.clone());
            generated
        };

        let config = ConfirmationConfig {
            // Step 2 inspected a mutable live source and is informational only.
            // Secret-risk approval is intentionally absent here: execute_export
            // scans and gates the exact staged artifact after filtering/export.
            has_secrets: false,
            has_critical_secrets: false,
            secret_count: 0,
            password_entropy_bits: self.state.password_entropy_bits,
            has_recovery_key: self.state.generate_recovery,
            recovery_key_phrase: None, // Will be set after generation
            summary,
        };

        let mut flow = ConfirmationFlow::new(config);

        // Process each confirmation step
        loop {
            match flow.current_step() {
                ConfirmationStep::SecretScanAcknowledgment => {
                    if !self.confirm_secret_ack(term, theme, &flow)? {
                        return Ok(false);
                    }
                    flow.complete_current_step();
                }
                ConfirmationStep::ContentReview => {
                    if !self.confirm_content_review(term, theme, &flow)? {
                        return Ok(false);
                    }
                    flow.complete_current_step();
                }
                ConfirmationStep::PasswordStrengthWarning => {
                    match self.confirm_password_strength(term, theme, &mut flow)? {
                        PasswordStrengthAction::SetStronger => {
                            // User wants to set a stronger password - go back
                            writeln!(
                                term,
                                "\n  {} Returning to security configuration...",
                                style("←").cyan()
                            )?;
                            return Ok(false);
                        }
                        PasswordStrengthAction::ProceedAnyway => {
                            flow.complete_current_step();
                        }
                        PasswordStrengthAction::Abort => {
                            return Ok(false);
                        }
                    }
                }
                ConfirmationStep::RecoveryKeyBackup => {
                    if !self.confirm_recovery_key(term, theme, &flow)? {
                        return Ok(false);
                    }
                    flow.complete_current_step();
                }
                ConfirmationStep::FinalConfirmation => {
                    if !self.confirm_final(term, theme, &mut flow)? {
                        return Ok(false);
                    }
                    flow.complete_current_step();
                    break;
                }
            }
        }

        writeln!(
            term,
            "\n  {} Pre-export confirmations completed; exact staged-artifact scan remains",
            style("✓").green()
        )?;
        Ok(true)
    }

    /// Confirm acknowledgment of detected secrets.
    fn confirm_secret_ack(
        &self,
        term: &mut Term,
        theme: &ColorfulTheme,
        flow: &ConfirmationFlow,
    ) -> Result<bool> {
        writeln!(
            term,
            "\n  {}",
            style("⚠️  SECRETS DETECTED").yellow().bold()
        )?;
        writeln!(term)?;
        writeln!(
            term,
            "  The secret scan found {} potential sensitive data item(s).",
            flow.config().secret_count
        )?;
        writeln!(term)?;
        writeln!(
            term,
            "  Even though the export will be encrypted, publishing content"
        )?;
        writeln!(term, "  containing secrets carries additional risk:")?;
        writeln!(term)?;
        writeln!(
            term,
            "  {} If your password is weak, secrets could be exposed",
            style("⚠").yellow()
        )?;
        writeln!(
            term,
            "  {} Secrets may remain valid if encryption is ever compromised",
            style("⚠").yellow()
        )?;
        writeln!(term)?;

        loop {
            let input: String = Input::with_theme(theme)
                .with_prompt("Type \"I understand the risks\" to proceed (or \"abort\" to cancel)")
                .interact_text()?;

            if input.trim().to_lowercase() == "abort" {
                return Ok(false);
            }

            match flow.validate_secret_ack(&input) {
                StepValidation::Passed => {
                    writeln!(term, "  {} Secrets acknowledged", style("✓").green())?;
                    return Ok(true);
                }
                StepValidation::Failed(msg) => {
                    writeln!(term, "  {} {}", style("✗").red(), msg)?;
                }
            }
        }
    }

    /// Confirm review of content summary.
    fn confirm_content_review(
        &self,
        term: &mut Term,
        theme: &ColorfulTheme,
        flow: &ConfirmationFlow,
    ) -> Result<bool> {
        writeln!(term, "\n  {}", style("📋 CONTENT REVIEW").cyan().bold())?;
        writeln!(term)?;
        writeln!(term, "  You are about to export:")?;
        writeln!(term)?;
        writeln!(
            term,
            "  • {} conversations from {} workspaces",
            flow.config().summary.total_conversations,
            flow.config().summary.workspaces.len()
        )?;
        writeln!(
            term,
            "  • {} messages",
            flow.config().summary.total_messages
        )?;
        writeln!(
            term,
            "  • Content from: {}",
            flow.config()
                .summary
                .agents
                .iter()
                .map(|a| a.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )?;
        writeln!(term)?;

        let confirmed = Confirm::with_theme(theme)
            .with_prompt("Have you reviewed the content summary?")
            .default(false)
            .interact()?;

        if confirmed {
            writeln!(term, "  {} Content reviewed", style("✓").green())?;
        }
        Ok(confirmed)
    }

    /// Handle password strength warning.
    fn confirm_password_strength(
        &self,
        term: &mut Term,
        theme: &ColorfulTheme,
        flow: &mut ConfirmationFlow,
    ) -> Result<PasswordStrengthAction> {
        writeln!(
            term,
            "\n  {}",
            style("🔐 PASSWORD STRENGTH WARNING").yellow().bold()
        )?;
        writeln!(term)?;
        writeln!(
            term,
            "  Your password has estimated entropy of {:.0} bits.",
            self.state.password_entropy_bits
        )?;
        writeln!(term)?;
        writeln!(term, "  Recommended minimum: 60 bits")?;
        writeln!(term)?;
        writeln!(
            term,
            "  A password with low entropy could potentially be cracked"
        )?;
        writeln!(
            term,
            "  by a determined attacker with sufficient resources."
        )?;
        writeln!(term)?;
        writeln!(term, "  For long-term security, consider:")?;
        writeln!(term, "  • Using a longer password (16+ characters)")?;
        writeln!(term, "  • Including numbers, symbols, and mixed case")?;
        writeln!(term, "  • Using a passphrase of 5+ random words")?;
        writeln!(term)?;

        let options = vec![
            "[S] Set a stronger password",
            "[P] Proceed with current password (not recommended)",
            "[A] Abort export",
        ];

        let selection = Select::with_theme(theme)
            .with_prompt("What would you like to do?")
            .items(&options)
            .default(0)
            .interact()?;

        let action = match selection {
            0 => PasswordStrengthAction::SetStronger,
            1 => {
                writeln!(
                    term,
                    "  {} Password warning acknowledged",
                    style("⚠").yellow()
                )?;
                PasswordStrengthAction::ProceedAnyway
            }
            _ => PasswordStrengthAction::Abort,
        };

        flow.set_password_action(action);
        Ok(action)
    }

    /// Confirm recovery key backup.
    fn confirm_recovery_key(
        &self,
        term: &mut Term,
        theme: &ColorfulTheme,
        _flow: &ConfirmationFlow,
    ) -> Result<bool> {
        writeln!(
            term,
            "\n  {}",
            style("💾 RECOVERY KEY BACKUP").cyan().bold()
        )?;
        writeln!(term)?;
        writeln!(
            term,
            "  A recovery key will be generated. This is the ONLY way"
        )?;
        writeln!(term, "  to recover your data if you forget your password.")?;
        writeln!(term)?;
        writeln!(
            term,
            "  {} If you lose both your password AND the recovery key,",
            style("⚠").yellow()
        )?;
        writeln!(term, "     your data will be permanently inaccessible.")?;
        writeln!(term)?;

        let confirmed = Confirm::with_theme(theme)
            .with_prompt("I understand that I must save the recovery key securely")
            .default(false)
            .interact()?;

        if confirmed {
            writeln!(
                term,
                "  {} Recovery key backup confirmed",
                style("✓").green()
            )?;
        }
        Ok(confirmed)
    }

    /// Final double-enter confirmation.
    fn confirm_final(
        &self,
        term: &mut Term,
        theme: &ColorfulTheme,
        flow: &mut ConfirmationFlow,
    ) -> Result<bool> {
        writeln!(term, "\n  {}", style("✓ FINAL CONFIRMATION").green().bold())?;
        writeln!(term)?;
        writeln!(term, "  Ready to publish:")?;
        writeln!(term)?;

        // Show completed steps
        for (_, label) in flow.completed_steps_summary() {
            writeln!(term, "  {} {}", style("✓").green(), label)?;
        }
        writeln!(term)?;

        // Show output location
        writeln!(
            term,
            "  Output: {} (local)",
            self.state.output_dir.display()
        )?;

        if let Some(summary) = &self.state.last_summary {
            writeln!(
                term,
                "  Size: ~{}",
                format_size(summary.estimated_size_bytes)
            )?;
        }
        writeln!(term)?;

        writeln!(
            term,
            "  {}",
            style("Press Enter TWICE to confirm and begin export").dim()
        )?;
        writeln!(term)?;

        // First Enter
        let _: String = Input::with_theme(theme)
            .with_prompt("[First confirmation - press Enter]")
            .allow_empty(true)
            .interact_text()?;

        flow.process_final_enter();
        writeln!(term, "  {} First confirmation received", style("•").cyan())?;

        // Second Enter
        let _: String = Input::with_theme(theme)
            .with_prompt("[Second confirmation - press Enter to proceed]")
            .allow_empty(true)
            .interact_text()?;

        flow.process_final_enter();
        writeln!(
            term,
            "  {} Second confirmation received",
            style("✓").green()
        )?;

        Ok(true)
    }

    fn step_export(&mut self, term: &mut Term) -> Result<()> {
        writeln!(term, "\n{}", style("Step 8 of 9: Export Progress").bold())?;
        writeln!(term, "{}", style("─".repeat(40)).dim())?;

        // Phase 0: Size estimation and limit checking
        writeln!(term, "\n  Estimating export size...")?;

        let since_ts = self
            .state
            .time_range
            .as_deref()
            .and_then(crate::ui::time_parser::parse_time_input);

        let agents: Vec<String> = self.state.agents.to_vec();
        let estimate = SizeEstimate::from_database(
            &self.state.db_path,
            if agents.is_empty() {
                None
            } else {
                Some(&agents)
            },
            since_ts,
            None,
        )?;

        // Display estimate
        writeln!(term)?;
        for line in estimate.format_display().lines() {
            writeln!(term, "  {}", line)?;
        }
        writeln!(term)?;

        // Stage export/encryption artifacts in a temp directory so the final
        // bundle root only contains publishable output (site/ + private/).
        let staging_dir = tempfile::tempdir()?;
        let export_db_path = staging_dir.path().join("export.db");
        let encrypted_dir = staging_dir.path().join("encrypted");
        std::fs::create_dir_all(&encrypted_dir)?;

        // Phase 1: Database Export with progress
        let pb = ProgressBar::new_spinner();
        let spinner_style = ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .context("build progress spinner style for export phase")?;
        pb.set_style(spinner_style);
        pb.enable_steady_tick(Duration::from_millis(100));
        pb.set_message("Filtering and exporting conversations...");

        // Build export filter with workspaces
        let workspaces = self.state.workspaces.clone();
        let since_dt = self.state.time_range.as_deref().and_then(|s| {
            crate::ui::time_parser::parse_time_input(s)
                .and_then(chrono::DateTime::from_timestamp_millis)
        });

        let filter = ExportFilter {
            agents: Some(self.state.agents.clone()),
            workspaces,
            since: since_dt,
            until: None,
            path_mode: if self.state.hide_metadata {
                PathMode::Hash
            } else {
                PathMode::Relative
            },
        };

        let engine = ExportEngine::new(&self.state.db_path, &export_db_path, filter)
            .with_exclusions(self.state.exclusions.clone())
            .with_share_profile(self.state.effective_share_profile());
        let running = Arc::new(AtomicBool::new(true));

        let staged_scan_config = SecretScanConfig::from_inputs(&[], &[])?;
        let (stats, staged_secret_scan) = engine.execute_verified(
            |current, total| {
                if total > 0 {
                    pb.set_message(format!("Exporting... {}/{} conversations", current, total));
                }
            },
            Some(running),
            |staged_db_path| scan_staged_export_database(staged_db_path, &staged_scan_config),
        )?;

        pb.finish_with_message(format!(
            "✓ Exported {} conversations, {} messages",
            stats.conversations_processed, stats.messages_processed
        ));

        writeln!(term)?;
        writeln!(
            term,
            "  {} Exact staged-export secret scan",
            style("🔒").cyan()
        )?;
        print_human_report(term, &staged_secret_scan.report, 3)?;
        self.state.secret_scan_has_findings = staged_secret_scan.report.summary.total > 0;
        self.state.secret_scan_has_critical = staged_secret_scan.report.summary.has_critical;
        self.state.secret_scan_count = staged_secret_scan.report.summary.total;

        if staged_secret_scan.report.summary.total > 0 {
            writeln!(
                term,
                "  Artifact SHA-256: {}",
                style(&staged_secret_scan.artifact_sha256).dim()
            )?;
            let theme = ColorfulTheme::default();
            let acknowledgment: String = Input::with_theme(&theme)
                .with_prompt(
                    "Type exactly \"I understand the risks\" to encrypt this staged artifact",
                )
                .interact_text()?;
            if acknowledgment.trim() != "I understand the risks" {
                bail!("Export cancelled: exact staged secret findings were not acknowledged");
            }
            writeln!(
                term,
                "  {} Exact staged artifact acknowledged",
                style("✓").green()
            )?;
        }

        // Phase 2: Encryption (skip if no_encryption mode)
        let encryption_config = if self.no_encryption_mode {
            writeln!(term)?;
            writeln!(
                term,
                "  {} Skipping encryption (unencrypted mode)",
                style("⚠").yellow()
            )?;
            writeln!(
                term,
                "  {}",
                style("WARNING: All content will be publicly readable!").red()
            )?;

            // For unencrypted mode, just copy the export.db to payload directory
            let payload_dir = encrypted_dir.join("payload");
            std::fs::create_dir_all(&payload_dir)?;
            let dest_db = payload_dir.join("data.db");
            std::fs::copy(&export_db_path, &dest_db)?;

            // Write minimal config.json for unencrypted bundle
            let db_size = std::fs::metadata(&dest_db).map(|m| m.len()).unwrap_or(0);
            let config = unencrypted_bundle_config(db_size);
            let config_path = encrypted_dir.join("config.json");
            crate::pages::write_file_durably(
                &config_path,
                serde_json::to_string_pretty(&config)?.as_bytes(),
            )?;
            None
        } else {
            let pb2 = ProgressBar::new_spinner();
            let spinner_style = ProgressStyle::default_spinner()
                .template("{spinner:.cyan} {msg}")
                .context("build progress spinner style for encryption phase")?;
            pb2.set_style(spinner_style);
            pb2.enable_steady_tick(Duration::from_millis(100));
            pb2.set_message("Encrypting archive...");

            // Initialize encryption engine
            let mut enc_engine = EncryptionEngine::default();

            // Add password slot
            if let Some(password) = &self.state.password {
                enc_engine.add_password_slot(password)?;
            }

            // Generate and add recovery secret if requested
            if self.state.generate_recovery {
                let mut recovery_bytes = [0u8; 32];
                use rand::Rng;
                let mut rng = rand::rng();
                rng.fill_bytes(&mut recovery_bytes);
                enc_engine.add_recovery_slot(&recovery_bytes)?;
                self.state.recovery_secret = Some(recovery_bytes.to_vec());
            }

            // Guard: refuse to produce an archive with zero key slots
            if enc_engine.key_slot_count() == 0 {
                bail!(
                    "No encryption key slots configured — archive would be permanently undecryptable"
                );
            }

            // Encrypt the database
            let encryption_config =
                enc_engine.encrypt_file(&export_db_path, &encrypted_dir, |_, _| {})?;

            pb2.finish_with_message("✓ Encryption complete");
            Some(encryption_config)
        };

        // Phase 3: Build static site bundle
        let pb3 = ProgressBar::new_spinner();
        let spinner_style = ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .context("build progress spinner style for bundle phase")?;
        pb3.set_style(spinner_style);
        pb3.enable_steady_tick(Duration::from_millis(100));
        pb3.set_message("Building static site bundle...");

        // Generate documentation
        let generated_docs = if let Some(ref summary) = self.state.last_summary {
            let (documentation_summary, archive_mode) =
                documentation_summary(summary, encryption_config.as_ref());
            let mut doc_config = DocConfig::new().with_archive_mode(archive_mode);
            if let Some(config) = encryption_config.as_ref() {
                doc_config = doc_config.with_argon_params(
                    config.kdf_defaults.memory_kb,
                    config.kdf_defaults.iterations,
                    config.kdf_defaults.parallelism,
                );
            }

            let doc_generator = DocumentationGenerator::new(doc_config, documentation_summary);
            doc_generator.generate_all()
        } else {
            Vec::new()
        };

        // Create bundle configuration
        let bundle_config = BundleConfig {
            title: self.state.title.clone(),
            description: self.state.description.clone(),
            hide_metadata: self.state.hide_metadata,
            recovery_secret: self.state.recovery_secret.clone(),
            generate_qr: self.state.generate_qr,
            generated_docs,
            analytics_status: Some(crate::pages::analytics::robot_status_projection(
                &self.state.db_path,
            )?),
        };

        let builder = BundleBuilder::with_config(bundle_config);
        let bundle_result =
            builder.build(&encrypted_dir, &self.state.output_dir, |phase, msg| {
                pb3.set_message(format!("{}: {}", phase, msg));
            })?;
        crate::pages::verify::ensure_valid_bundle(&bundle_result.site_dir, false)
            .context("Completed Pages bundle failed full verification")?;
        self.state.final_site_dir = Some(bundle_result.site_dir.clone());

        pb3.finish_with_message(format!(
            "✓ Bundle complete: {} files, fingerprint {}",
            bundle_result.total_files,
            bundle_result
                .fingerprint
                .get(..8)
                .unwrap_or(&bundle_result.fingerprint)
        ));

        writeln!(term)?;
        writeln!(
            term,
            "  {} Site directory (preview or open locally): {}",
            style("✓").green(),
            style(bundle_result.site_dir.display()).cyan()
        )?;
        writeln!(
            term,
            "  {} Private directory (keep secure): {}",
            style("✓").green(),
            style(bundle_result.private_dir.display()).cyan()
        )?;
        writeln!(
            term,
            "  {} Integrity fingerprint: {}",
            style("✓").green(),
            style(&bundle_result.fingerprint).cyan()
        )?;

        // Display recovery secret location if generated
        if self.state.recovery_secret.is_some() {
            writeln!(term)?;
            writeln!(
                term,
                "  {} Recovery secret saved to: {}",
                style("⚠").yellow().bold(),
                style(
                    bundle_result
                        .private_dir
                        .join("recovery-secret.txt")
                        .display()
                )
                .cyan()
            )?;
            writeln!(
                term,
                "  {}",
                style("Store this file securely - it can unlock your archive if you forget the password.").dim()
            )?;
        }

        if self.state.generate_qr {
            writeln!(
                term,
                "  {} QR codes saved to private directory",
                style("✓").green()
            )?;
        }

        Ok(())
    }

    fn site_dir(&self) -> PathBuf {
        self.state
            .final_site_dir
            .as_ref()
            .cloned()
            .unwrap_or_else(|| self.state.output_dir.join("site"))
    }

    fn step_finish(&self, term: &mut Term) -> Result<()> {
        writeln!(term, "\n{}", style("Step 9 of 9: Done").bold())?;
        writeln!(term, "{}", style("─".repeat(40)).dim())?;

        {
            let site_dir = self.site_dir();
            writeln!(term)?;
            writeln!(term, "{}", style("✓ Export complete!").green().bold())?;
            writeln!(term)?;
            writeln!(
                term,
                "Your archive bundle has been exported to: {}",
                style(self.state.output_dir.display()).cyan()
            )?;
            writeln!(term)?;
            writeln!(
                term,
                "Exported site directory: {}",
                style(site_dir.display()).cyan()
            )?;
            writeln!(term)?;
            writeln!(term, "To preview locally, run:")?;
            writeln!(
                term,
                "  {}",
                style(format!(
                    "cass pages --preview {} --no-open",
                    site_dir.display()
                ))
                .dim()
            )?;
            writeln!(term)?;
            writeln!(
                term,
                "Then open {} in your browser.",
                style("http://localhost:8080").cyan()
            )?;
        }

        writeln!(term)?;
        Ok(())
    }
}

fn unencrypted_bundle_config(db_size: u64) -> serde_json::Value {
    serde_json::json!({
        "encrypted": false,
        "version": "1.0.0",
        "payload": {
            "path": "payload/data.db",
            "format": "sqlite",
            "size_bytes": db_size
        },
        "warning": "UNENCRYPTED - All content is publicly readable"
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unencrypted_bundle_config_shape() {
        let config = unencrypted_bundle_config(1234);

        assert_eq!(
            config,
            serde_json::json!({
                "encrypted": false,
                "version": "1.0.0",
                "payload": {
                    "path": "payload/data.db",
                    "format": "sqlite",
                    "size_bytes": 1234
                },
                "warning": "UNENCRYPTED - All content is publicly readable"
            })
        );
    }

    #[test]
    fn bundle_verification_rechecks_mutated_bundle() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let archive_dir = temp.path().join("archive");
        let payload_dir = archive_dir.join("payload");
        std::fs::create_dir_all(&payload_dir)?;
        let payload = b"SQLite format 3\0test archive bytes";
        std::fs::write(payload_dir.join("data.db"), payload)?;
        std::fs::write(
            archive_dir.join("config.json"),
            serde_json::to_vec_pretty(&unencrypted_bundle_config(payload.len() as u64))?,
        )?;

        let output_dir = temp.path().join("bundle");
        let bundle = BundleBuilder::new().build(&archive_dir, &output_dir, |_, _| {})?;
        crate::pages::verify::ensure_valid_bundle(&bundle.site_dir, false)?;

        std::fs::write(
            bundle.site_dir.join("recovery-secret.txt"),
            b"post-build private material",
        )?;
        let error = crate::pages::verify::ensure_valid_bundle(&bundle.site_dir, false)
            .expect_err("mutated bundle must fail re-verification");
        let message = format!("{error:#}");

        assert!(message.contains("no_secrets_in_site"));
        Ok(())
    }

    #[test]
    fn documentation_uses_key_slots_from_the_emitted_encryption_config() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let input = temp.path().join("export.db");
        let encrypted_dir = temp.path().join("encrypted");
        std::fs::write(&input, b"committed export bytes")?;

        let mut engine = EncryptionEngine::new(1024)?;
        engine.add_password_slot("correct horse battery staple")?;
        engine.add_recovery_slot(&[7_u8; 32])?;
        let emitted = engine.encrypt_file(&input, &encrypted_dir, |_, _| {})?;

        let summary = PrePublishSummary {
            total_conversations: 0,
            total_messages: 0,
            total_characters: 0,
            estimated_size_bytes: 0,
            earliest_timestamp: None,
            latest_timestamp: None,
            date_histogram: Vec::new(),
            workspaces: Vec::new(),
            agents: Vec::new(),
            secret_scan: Default::default(),
            encryption_config: None,
            key_slots: Vec::new(),
            generated_at: chrono::Utc::now(),
        };

        let (documented, mode) = documentation_summary(&summary, Some(&emitted));
        assert_eq!(mode, ArchiveMode::Encrypted);
        assert_eq!(documented.key_slots.len(), emitted.key_slots.len());
        assert_eq!(documented.key_slots.len(), 2);
        assert_eq!(
            documented.key_slots[0].slot_type,
            crate::pages::summary::KeySlotType::Password
        );
        assert_eq!(
            documented.key_slots[1].slot_type,
            crate::pages::summary::KeySlotType::Recovery
        );
        assert_eq!(
            documented
                .encryption_config
                .as_ref()
                .map(|config| config.key_slot_count),
            Some(2)
        );

        let security_doc = DocumentationGenerator::new(
            DocConfig::new().with_archive_mode(mode).with_argon_params(
                emitted.kdf_defaults.memory_kb,
                emitted.kdf_defaults.iterations,
                emitted.kdf_defaults.parallelism,
            ),
            documented.clone(),
        )
        .generate_security_doc();
        assert!(security_doc.content.contains(&format!(
            "m={}KB, t={}, p={}",
            emitted.kdf_defaults.memory_kb,
            emitted.kdf_defaults.iterations,
            emitted.kdf_defaults.parallelism
        )));

        let (unencrypted, mode) = documentation_summary(&documented, None);
        assert_eq!(mode, ArchiveMode::Unencrypted);
        assert!(unencrypted.key_slots.is_empty());
        assert!(unencrypted.encryption_config.is_none());
        Ok(())
    }

    #[test]
    fn unencrypted_summary_copy_never_claims_encrypted_output() {
        assert!(wizard_archive_description(true).contains("plaintext"));

        let size_line = archive_size_summary_line(true, 1_048_576);
        assert!(size_line.contains("final unencrypted SQLite file"));
        assert!(!size_line.contains("compressed + encrypted"));

        let security_lines = summary_security_lines(true, 2).join("\n");
        assert!(security_lines.contains("Encryption: Disabled (public plaintext)"));
        assert!(security_lines.contains("Key Derivation: Not applicable"));
        assert!(security_lines.contains("Key Slots: 0"));
        assert!(!security_lines.contains("AES-256-GCM"));
        assert!(!security_lines.contains("Argon2id"));
    }

    #[test]
    fn encrypted_summary_copy_reports_planned_slots() {
        let security_lines = summary_security_lines(false, 2).join("\n");
        assert!(security_lines.contains("Encryption: AES-256-GCM"));
        assert!(security_lines.contains("Key Derivation: Argon2id"));
        assert!(security_lines.contains("Planned Key Slots: 2"));
    }

    // =========================
    // WizardState Tests
    // =========================

    #[test]
    fn wizard_state_default_values() {
        let state = WizardState::default();

        // Content selection defaults
        assert!(state.agents.is_empty());
        assert!(state.time_range.is_none());
        assert!(state.workspaces.is_none());

        // Security defaults
        assert!(state.password.is_none());
        assert!(state.recovery_secret.is_none());
        assert!(state.generate_recovery); // Should default to true
        assert!(!state.generate_qr); // Should default to false

        // Site configuration defaults
        assert_eq!(state.title, "cass Archive");
        assert_eq!(
            state.description,
            "Encrypted archive of AI coding agent conversations"
        );
        assert!(!state.hide_metadata);

        // Local output defaults
        assert_eq!(state.output_dir, PathBuf::from("cass-export"));

        // Exclusions default
        assert_eq!(state.exclusions.exclusion_counts(), (0, 0, 0));

        // Summary default
        assert!(state.last_summary.is_none());

        // Secret scan defaults
        assert!(!state.secret_scan_has_findings);
        assert!(!state.secret_scan_has_critical);
        assert_eq!(state.secret_scan_count, 0);

        // Password entropy default
        assert_eq!(state.password_entropy_bits, 0.0);

        // Unencrypted mode defaults
        assert!(!state.no_encryption);
        assert!(!state.unencrypted_confirmed);
    }

    #[test]
    fn wizard_state_db_path_is_set() {
        let state = WizardState::default();
        // db_path should be set to a valid path containing the expected filename
        assert!(state.db_path.to_string_lossy().contains("agent_search.db"));
    }

    #[test]
    fn wizard_state_clone() {
        let state = WizardState {
            title: "Custom Title".to_string(),
            agents: vec!["claude".to_string(), "codex".to_string()],
            no_encryption: true,
            ..Default::default()
        };

        let cloned = state.clone();
        assert_eq!(cloned.title, "Custom Title");
        assert_eq!(
            cloned.agents,
            vec!["claude".to_string(), "codex".to_string()]
        );
        assert!(cloned.no_encryption);
    }

    // =========================
    // PagesWizard Tests
    // =========================

    #[test]
    fn pages_wizard_new_initializes_default_state() {
        let wizard = PagesWizard::new();
        // Access state through the no_encryption_mode field which is false by default
        assert!(!wizard.no_encryption_mode);
    }

    #[test]
    fn pages_wizard_default_impl() {
        let wizard1 = PagesWizard::new();
        let wizard2 = PagesWizard::default();
        // Both should have same default state
        assert_eq!(wizard1.no_encryption_mode, wizard2.no_encryption_mode);
    }

    #[test]
    fn pages_wizard_set_no_encryption() {
        let mut wizard = PagesWizard::new();
        assert!(!wizard.no_encryption_mode);
        assert!(!wizard.state.no_encryption);

        wizard.set_no_encryption(true);
        assert!(wizard.no_encryption_mode);
        assert!(wizard.state.no_encryption);

        wizard.set_no_encryption(false);
        assert!(!wizard.no_encryption_mode);
        assert!(!wizard.state.no_encryption);
    }

    // Test for pages_wizard_set_include_attachments removed: flag removed
    // from WizardState per bead adyyt.

    // =========================
    // Time Range Mapping Tests
    // =========================

    #[test]
    fn time_range_selection_mapping() {
        // Test the time range mapping logic from step_content_selection
        // This is the mapping: 1 => -7d, 2 => -30d, 3 => -90d, 4 => -365d, 0/_ => None

        fn map_time_selection(selection: usize) -> Option<String> {
            match selection {
                1 => Some("-7d".to_string()),
                2 => Some("-30d".to_string()),
                3 => Some("-90d".to_string()),
                4 => Some("-365d".to_string()),
                _ => None,
            }
        }

        assert_eq!(map_time_selection(0), None);
        assert_eq!(map_time_selection(1), Some("-7d".to_string()));
        assert_eq!(map_time_selection(2), Some("-30d".to_string()));
        assert_eq!(map_time_selection(3), Some("-90d".to_string()));
        assert_eq!(map_time_selection(4), Some("-365d".to_string()));
        assert_eq!(map_time_selection(5), None);
    }

    // =========================
    // State Modification Tests
    // =========================

    #[test]
    fn wizard_state_agents_modification() {
        let mut state = WizardState::default();
        assert!(state.agents.is_empty());

        state.agents = vec!["claude".to_string()];
        assert_eq!(state.agents.len(), 1);

        state.agents.push("codex".to_string());
        assert_eq!(state.agents.len(), 2);
        assert_eq!(
            state.agents,
            vec!["claude".to_string(), "codex".to_string()]
        );
    }

    #[test]
    fn wizard_state_workspaces_modification() {
        let mut state = WizardState::default();
        assert!(state.workspaces.is_none());

        state.workspaces = Some(vec![PathBuf::from("/project1")]);
        assert_eq!(state.workspaces.as_ref().unwrap().len(), 1);

        state
            .workspaces
            .as_mut()
            .unwrap()
            .push(PathBuf::from("/project2"));
        assert_eq!(state.workspaces.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn wizard_state_security_configuration() {
        let state = WizardState {
            password: Some("test_password".to_string()),
            recovery_secret: Some(vec![1, 2, 3, 4]),
            generate_recovery: false,
            generate_qr: true,
            ..Default::default()
        };

        assert_eq!(state.password, Some("test_password".to_string()));
        assert_eq!(state.recovery_secret, Some(vec![1, 2, 3, 4]));
        assert!(!state.generate_recovery);
        assert!(state.generate_qr);
    }

    #[test]
    fn wizard_state_debug_redacts_sensitive_fields() {
        let state = WizardState {
            password: Some("test_password".to_string()),
            recovery_secret: Some(vec![1, 2, 3, 4]),
            ..Default::default()
        };

        let debug = format!("{state:?}");
        assert!(debug.contains("password"));
        assert!(debug.contains("recovery_secret"));
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("test_password"));
        assert!(!debug.contains("[1, 2, 3, 4]"));
    }

    #[test]
    fn sample_title_truncation_is_utf8_boundary_safe() {
        let ascii = "abcdefghijklmnopqrstuvwxyz0123456789";
        assert_eq!(
            truncate_sample_title(ascii),
            "abcdefghijklmnopqrstuvwxyz0..."
        );

        let unicode = format!("{}{}", "日本語".repeat(12), "suffix");
        let truncated = truncate_sample_title(&unicode);
        assert!(truncated.ends_with("..."));
        assert!(truncated.is_char_boundary(truncated.len()));
        assert!(truncated.len() <= 30);
    }

    #[test]
    fn wizard_state_password_entropy() {
        let mut state = WizardState::default();
        assert_eq!(state.password_entropy_bits, 0.0);

        state.password_entropy_bits = 64.5;
        assert!((state.password_entropy_bits - 64.5).abs() < f64::EPSILON);
    }

    #[test]
    fn wizard_state_secret_scan_results() {
        let state = WizardState {
            secret_scan_has_findings: true,
            secret_scan_has_critical: true,
            secret_scan_count: 5,
            ..Default::default()
        };

        assert!(state.secret_scan_has_findings);
        assert!(state.secret_scan_has_critical);
        assert_eq!(state.secret_scan_count, 5);
    }

    #[test]
    fn wizard_state_output_configuration() {
        let state = WizardState {
            output_dir: PathBuf::from("/custom/output"),
            ..Default::default()
        };

        assert_eq!(state.output_dir, PathBuf::from("/custom/output"));
    }

    // =========================
    // Edge Cases
    // =========================

    #[test]
    fn wizard_state_with_unicode_values() {
        let state = WizardState {
            title: "日本語タイトル".to_string(),
            description: "説明文 with émojis 🎉".to_string(),
            agents: vec!["クローード".to_string()],
            ..Default::default()
        };

        assert_eq!(state.title, "日本語タイトル");
        assert_eq!(state.description, "説明文 with émojis 🎉");
        assert_eq!(state.agents[0], "クローード");
    }

    #[test]
    fn wizard_state_empty_strings() {
        let state = WizardState {
            title: "".to_string(),
            description: "".to_string(),
            ..Default::default()
        };

        assert!(state.title.is_empty());
        assert!(state.description.is_empty());
    }
}
