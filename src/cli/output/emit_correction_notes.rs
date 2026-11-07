use super::describe_command::{
    analytics_requests_structured_output, resolve_subcommand_structured_format,
};
use super::extract_search_terms::extract_search_terms;
use crate::cli::args::{Cli, Commands};
use crate::cli::args_sub::{
    AgentsAction, ImportCommand, MirrorCommand, ModelsCommand, PagesSubcommand, QuarantineCommand,
    ScheduleCommand, SourcesCommand,
};
use crate::cli::commands::search::fieldbudgets::robot_format_from_env;
use crate::cli::error::WrapConfig;

/// Print the auto-correction teaching note on stderr.
///
/// Humans get the multi-line bulleted form. Robots get one `note:` line per
/// correction so a JSON caller that captures stderr sees a stable, greppable
/// prefix (`note: auto-corrected:`) and never a decorative bullet or the
/// `cass --help` tip. stdout is untouched on both paths, so the data
/// contract (`stdout = data only, stderr = diagnostics`) holds.
pub(crate) fn emit_correction_notes(notes: &[&str], robot_mode: bool) {
    if robot_mode {
        for note in notes {
            eprintln!("note: auto-corrected: {note}");
        }
        return;
    }
    eprintln!("Note: Your command was auto-corrected:");
    for note in notes {
        eprintln!("  • {note}");
    }
    eprintln!("Tip: Run 'cass --help' for proper syntax.");
}

/// Returns true if the command is using robot/JSON output mode.
/// Used to auto-suppress INFO logs for clean machine-parseable output.
pub(crate) fn is_robot_mode(command: &Commands, cli: &Cli) -> bool {
    // Env-driven output formats should behave like robot mode (suppresses INFO logs).
    let env_robot_mode = robot_format_from_env().is_some();

    match command {
        Commands::Search {
            json, robot_meta, ..
        } => resolve_subcommand_structured_format(cli, *json).is_some() || *robot_meta,
        Commands::Pack { json, .. } => resolve_subcommand_structured_format(cli, *json).is_some(),
        Commands::Index { json, .. } => resolve_subcommand_structured_format(cli, *json).is_some(),
        Commands::Schedule(
            ScheduleCommand::Install { json, .. }
            | ScheduleCommand::Uninstall { json, .. }
            | ScheduleCommand::Status { json, .. }
            | ScheduleCommand::Run { json, .. },
        ) => resolve_subcommand_structured_format(cli, *json).is_some(),
        Commands::Health { json, .. } | Commands::Selftest { json } => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Onboarding { json, .. } => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Guide { json, .. } => resolve_subcommand_structured_format(cli, *json).is_some(),
        Commands::Pages {
            subcommand, json, ..
        } => {
            resolve_subcommand_structured_format(cli, *json).is_some()
                || subcommand.as_ref().is_some_and(|sub| match sub {
                    PagesSubcommand::Key(args) => args.command.json(),
                })
        }
        Commands::Sessions { json, .. } => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Resume { json, .. } => resolve_subcommand_structured_format(cli, *json).is_some(),
        Commands::ExportHtml { json, .. } => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Stats { json, .. }
        | Commands::Diag { json, .. }
        | Commands::Storage { json, .. }
        | Commands::Dedup { json, .. }
        | Commands::Status { json, .. }
        | Commands::Triage { json, .. }
        | Commands::SupportBundle { json, .. }
        | Commands::ApiVersion { json }
        | Commands::State { json, .. }
        | Commands::View { json, .. }
        | Commands::Capabilities { json }
        | Commands::Introspect { json }
        | Commands::Context { json, .. }
        | Commands::Expand { json, .. } => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Doctor { json, .. } => resolve_subcommand_structured_format(cli, *json).is_some(),
        Commands::Bookmarks(args) => args.command.json() || env_robot_mode,
        Commands::Timeline { json, .. } => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Mirror(MirrorCommand::Prune { json, .. }) => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Forget { json, .. } => resolve_subcommand_structured_format(cli, *json).is_some(),
        Commands::Sources(SourcesCommand::List { json, .. }) => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Sources(SourcesCommand::Add { json, .. }) => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Sources(SourcesCommand::Doctor { json, .. }) => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Sources(SourcesCommand::Agents(AgentsAction::List { json })) => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Models(ModelsCommand::Status { json }) => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Models(ModelsCommand::Verify { json, .. }) => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Models(ModelsCommand::Backfill { json, .. }) => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Models(ModelsCommand::BuildHnsw { json, .. }) => {
            resolve_subcommand_structured_format(cli, *json).is_some()
        }
        Commands::Sources(_) => false,
        Commands::Quarantine(
            QuarantineCommand::List { json, .. } | QuarantineCommand::Clear { json, .. },
        ) => resolve_subcommand_structured_format(cli, *json).is_some(),
        Commands::Import(cmd) => match cmd {
            ImportCommand::Chatgpt { json, .. } => {
                resolve_subcommand_structured_format(cli, *json).is_some()
            }
        },
        Commands::Models(_) => cli.robot_format.is_some() || env_robot_mode,
        Commands::Analytics(cmd) => analytics_requests_structured_output(cmd, cli),
        _ => false,
    }
}

pub(crate) fn apply_wrap(line: &str, wrap: WrapConfig) -> String {
    let width = wrap.effective_width();
    if line.trim().is_empty() || width.is_none() {
        return line.trim_end().to_string();
    }
    let width = width.unwrap_or(usize::MAX);
    if line.len() <= width {
        return line.trim_end().to_string();
    }

    let mut out = String::new();
    let mut current = String::new();
    for word in line.split_whitespace() {
        if current.len() + word.len() + 1 > width && !current.is_empty() {
            out.push_str(current.trim_end());
            out.push('\n');
            current.clear();
        }
        current.push_str(word);
        current.push(' ');
    }
    if !current.is_empty() {
        out.push_str(current.trim_end());
    }
    out
}

pub(crate) fn lowercase_with_map(text: &str) -> (String, Vec<usize>, Vec<(usize, usize)>) {
    let mut lower = String::with_capacity(text.len());
    let mut lower_starts = Vec::new();
    let mut orig_ranges = Vec::new();

    for (orig_idx, ch) in text.char_indices() {
        let orig_end = orig_idx + ch.len_utf8();
        for lower_ch in ch.to_lowercase() {
            lower_starts.push(lower.len());
            lower.push(lower_ch);
            orig_ranges.push((orig_idx, orig_end));
        }
    }

    (lower, lower_starts, orig_ranges)
}

pub(crate) fn lower_char_index(
    lower_starts: &[usize],
    lower_len: usize,
    byte_idx: usize,
) -> Option<usize> {
    if byte_idx == lower_len {
        return Some(lower_starts.len());
    }
    let idx = lower_starts.partition_point(|&b| b < byte_idx);
    if idx < lower_starts.len() && lower_starts[idx] == byte_idx {
        Some(idx)
    } else {
        None
    }
}

pub(crate) fn map_lower_range(
    lower_starts: &[usize],
    lower_len: usize,
    orig_ranges: &[(usize, usize)],
    start: usize,
    end: usize,
) -> Option<(usize, usize)> {
    let start_idx = lower_char_index(lower_starts, lower_len, start)?;
    let end_idx = lower_char_index(lower_starts, lower_len, end)?;
    if start_idx >= end_idx {
        return None;
    }
    let orig_start = orig_ranges.get(start_idx)?.0;
    let orig_end = orig_ranges.get(end_idx - 1)?.1;
    Some((orig_start, orig_end))
}

/// Highlight matching search terms in text
///
/// Extracts query terms and wraps matches with the specified markers.
/// Uses case-insensitive matching. Handles quoted phrases and individual terms.
///
/// # Arguments
/// * `text` - The text to highlight matches in
/// * `query` - The search query to extract terms from
/// * `start_mark` - Opening marker (e.g., "**" for markdown bold, "<mark>" for HTML)
/// * `end_mark` - Closing marker (e.g., "**" for markdown bold, "</mark>" for HTML)
pub(crate) fn highlight_matches(
    text: &str,
    query: &str,
    start_mark: &str,
    end_mark: &str,
) -> String {
    // Extract search terms from query (handles quoted phrases and individual words)
    let terms = extract_search_terms(query);
    if terms.is_empty() {
        return text.to_string();
    }

    // Sort terms by length (longest first) to avoid partial matches
    let mut terms: Vec<_> = terms.into_iter().collect();
    terms.sort_by_key(|s| std::cmp::Reverse(s.len()));

    let mut result = text.to_string();
    for term in &terms {
        if term.is_empty() {
            continue;
        }
        // Case-insensitive replacement
        // Build a lowercase copy with a byte-level map back to original indices to
        // avoid slicing bugs when Unicode case-folding changes string length.
        let (lower_result, lower_starts, orig_ranges) = lowercase_with_map(&result);
        let lower_term = term.to_lowercase();
        let marked = marked_spans(&result, start_mark, end_mark);
        let mut new_result = String::new();
        let mut last_end = 0;

        for (idx, matched_str) in lower_result.match_indices(&lower_term) {
            let end = idx + matched_str.len();
            let Some((orig_start, orig_end)) =
                map_lower_range(&lower_starts, lower_result.len(), &orig_ranges, idx, end)
            else {
                continue;
            };

            // Skip if this overlaps with a previous highlight (from a longer term)
            if orig_start < last_end {
                continue;
            }
            // Skip text that is already marked: the search engine wraps the
            // terms it matched in `**`, and wrapping them again printed
            // `****term****` (2l1b0.68).
            if marked
                .iter()
                .any(|&(start, end)| orig_start < end && orig_end > start)
            {
                continue;
            }
            // Append text before this match
            new_result.push_str(&result[last_end..orig_start]);
            // Append highlighted match (preserve original case)
            new_result.push_str(start_mark);
            new_result.push_str(&result[orig_start..orig_end]);
            new_result.push_str(end_mark);
            last_end = orig_end;
        }
        // Append remaining text
        new_result.push_str(&result[last_end..]);
        result = new_result;
    }

    result
}

/// Byte ranges of `text` already enclosed by `start_mark ... end_mark`,
/// markers included, pairing each opening mark with the next closing one.
pub(crate) fn marked_spans(text: &str, start_mark: &str, end_mark: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut cursor = 0;
    while let Some(open) = text[cursor..].find(start_mark) {
        let open = cursor + open;
        let body = open + start_mark.len();
        let Some(close) = text[body..].find(end_mark) else {
            break;
        };
        let end = body + close + end_mark.len();
        spans.push((open, end));
        cursor = end;
    }
    spans
}
