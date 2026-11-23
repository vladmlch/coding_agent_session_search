use super::scanner::scan_database;
use super::types::{SecretScanConfig, SecretScanFilters, SecretScanReport, SecretSeverity};
use anyhow::{Result, bail};
use console::{Term, style};
use indicatif::{ProgressBar, ProgressStyle};
use std::io::Write;
use std::path::Path;
use std::time::Duration;

pub fn print_human_report(
    term: &mut Term,
    report: &SecretScanReport,
    max_examples: usize,
) -> Result<()> {
    write_human_report(term, report, max_examples)
}

pub(super) fn write_human_report(
    writer: &mut impl Write,
    report: &SecretScanReport,
    max_examples: usize,
) -> Result<()> {
    let total = report.summary.total;
    if total == 0 && !report.summary.truncated {
        writeln!(writer, "  {} No secrets detected", style("✓").green())?;
        return Ok(());
    }

    if total > 0 {
        writeln!(
            writer,
            "  {} {} potential secret(s) detected",
            style("⚠").yellow(),
            total
        )?;
    }

    let mut severities = vec![
        SecretSeverity::Critical,
        SecretSeverity::High,
        SecretSeverity::Medium,
        SecretSeverity::Low,
    ];

    severities.sort_by_key(|s| s.rank());

    for severity in severities {
        let count = report
            .summary
            .by_severity
            .get(&severity)
            .copied()
            .unwrap_or(0);
        if count == 0 {
            continue;
        }
        let label = severity.styled(severity.label());
        writeln!(writer, "  {}: {}", label, count)?;

        for finding in report
            .findings
            .iter()
            .filter(|f| f.severity == severity)
            .take(max_examples)
        {
            writeln!(
                writer,
                "    - {} in {} ({})",
                finding.kind,
                finding.location.label(),
                finding.match_redacted
            )?;
            if !finding.context.is_empty() {
                writeln!(writer, "      {}", style(&finding.context).dim())?;
            }
        }
        if count > max_examples {
            writeln!(
                writer,
                "      {}",
                style("…additional findings hidden").dim()
            )?;
        }
    }

    if report.summary.truncated {
        writeln!(
            writer,
            "  {} Results truncated (max findings reached)",
            style("⚠").yellow()
        )?;
    }

    Ok(())
}

pub fn print_cli_report(report: &SecretScanReport, json: bool) -> Result<()> {
    if json {
        let payload = serde_json::to_string_pretty(report)?;
        println!("{payload}");
        return Ok(());
    }

    let mut term = Term::stdout();
    print_human_report(&mut term, report, 3)
}

pub fn run_secret_scan_cli<P: AsRef<Path>>(
    db_path: P,
    filters: &SecretScanFilters,
    config: &SecretScanConfig,
    json: bool,
    fail_on_secrets: bool,
) -> Result<()> {
    let progress = ProgressBar::new_spinner();
    progress.set_style(
        ProgressStyle::with_template("{spinner} {msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );
    progress.set_message("Scanning for secrets...");
    progress.enable_steady_tick(Duration::from_millis(120));

    let report = scan_database(db_path, filters, config, None, Some(&progress))?;
    progress.finish_and_clear();

    print_cli_report(&report, json)?;

    if fail_on_secrets && report.summary.total > 0 {
        bail!("Secrets detected ({} finding(s))", report.summary.total);
    }

    Ok(())
}

pub fn wizard_secret_scan<P: AsRef<Path>>(
    db_path: P,
    filters: &SecretScanFilters,
    config: &SecretScanConfig,
) -> Result<SecretScanReport> {
    let progress = ProgressBar::new_spinner();
    progress.set_style(
        ProgressStyle::with_template("{spinner} {msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );
    progress.set_message("Scanning for secrets...");
    progress.enable_steady_tick(Duration::from_millis(120));

    let report = scan_database(db_path, filters, config, None, Some(&progress))?;
    progress.finish_and_clear();
    Ok(report)
}
