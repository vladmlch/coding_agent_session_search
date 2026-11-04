//! Project-scoped, identifier-anchored trust correlation (local-only form).
//!
//! Bead: coding_agent_session_search-q4pau (follow-on to
//! coding_agent_session_search-guided-ops-repro-trust-5u82n.3).
//!
//! ## Why
//!
//! [`crate::search::trust_scoring`] reduces metadata-only signals to a verdict,
//! but its strongest input — a linked commit — defaulted to "no signal" live
//! because nothing populated it. The recency / workspace / source / mode
//! portion of the verdict worked, but the "landed vs. unverified" distinction
//! did not. This module fills that gap with a cheap, conservative correlation
//! that lights the commit field up for results that belong to the project the
//! agent is working in right now.
//!
//! ## Local-only scope
//!
//! The coordination facts that used to feed this layer (Beads issue state,
//! Agent Mail provenance, durable lessons, and release-tag containment) were
//! removed with the local-only port: they required shared-state services cass
//! no longer consults. What remains is deliberately narrow and fully local:
//! whether a result explicitly cites a commit that exists in the current
//! project's own git history. `git` subprocesses stay because explicit commit
//! references in a local repository are still a meaningful, checkable signal
//! and there is no cheaper truthful way to resolve them.
//!
//! ## How (and why it does not fabricate trust)
//!
//! 1. Build once per query a [`CorrelationIndex`] for the CURRENT project (the
//!    git repo containing the working directory) and pin the observed HEAD.
//!    Recent commit ids are loaded lazily, only when an explicit commit
//!    reference needs corroboration.
//! 2. For each on-project result, [`correlate`] scans the result's own indexed
//!    text for an EXPLICIT commit id reference and links it. A temporal or
//!    workspace coincidence is never enough — we require an explicit
//!    identifier match — so an unrelated conversation never inherits a
//!    "trusted" verdict from work it merely happened near.
//!
//! ## Pure vs. live
//!
//! Identifier matching and [`proof_for`] are deterministic for an injected
//! [`CorrelationIndex`] + text. Live indexes defer history loading until an
//! explicit reference needs it; history is pinned to construction's HEAD. The
//! builder and lazy resolver are fail-open: a missing repo or a git error
//! yields no facts, and the verdict falls back to the recency / workspace /
//! source / mode portion. Correlation is advisory metadata only — like the
//! rest of the trust layer, it never changes result ordering and never emits
//! raw session text (only sanitized identifiers flow downstream).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use crate::search::trust_scoring::{OutcomeMarker, ProofStatus};

/// How many recent commits to index for commit-reference resolution when
/// building the live index. Bounded so the build stays one cheap `git log`
/// invocation.
const GIT_LOG_LIMIT: usize = 5000;

/// Max characters of a result's text scanned for identifier references. Commit
/// ids appear in titles/early lines in practice, so a bounded prefix keeps
/// per-result work cheap without missing the common case.
const MAX_SCAN_CHARS: usize = 8192;

/// Length of the commit-id prefix used to index/match known commits. Long
/// enough to be collision-free in any realistic project history, short enough
/// to match the abbreviated ids agents quote.
const COMMIT_PREFIX_LEN: usize = 12;

/// The strongest commit link found for one result. Final proof status is
/// resolved by the caller, so this pure result carries only what an explicit
/// text match can establish on its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrelationLink {
    /// Linked commit id (full 40-char), if a directly referenced known commit
    /// matched project history.
    pub linked_commit: Option<String>,
    /// Outcome marker implied by the match (`Landed` for a real commit,
    /// `Unknown` for no match).
    pub outcome: OutcomeMarker,
}

impl CorrelationLink {
    /// The empty link (no correlation found).
    pub fn none() -> Self {
        CorrelationLink {
            linked_commit: None,
            outcome: OutcomeMarker::Unknown,
        }
    }

    /// Whether any correlation signal was found.
    pub fn is_empty(&self) -> bool {
        self.linked_commit.is_none() && matches!(self.outcome, OutcomeMarker::Unknown)
    }
}

/// A cheap, once-per-query correlation index for the current project.
#[derive(Debug, Default)]
pub struct CorrelationIndex {
    /// Recent history, loaded once when an explicit commit reference needs it.
    /// Ordinary prose and off-project results need no history traversal.
    git_links: OnceLock<GitLinks>,
    /// HEAD at construction, so a concurrent commit cannot change the history
    /// observed by the deferred lookup. None also preserves an unborn history.
    git_revision: Option<String>,
    /// The project's git root, exposed as the cwd workspace anchor. `None`
    /// disables live history lookup.
    repo_root: Option<PathBuf>,
}

#[derive(Debug, Default)]
struct GitLinks {
    /// Known full commit ids, indexed by their [`COMMIT_PREFIX_LEN`]-char prefix.
    commit_by_prefix: HashMap<String, String>,
}

impl CorrelationIndex {
    fn git_links(&self) -> &GitLinks {
        self.git_links.get_or_init(|| {
            self.repo_root
                .as_deref()
                .zip(self.git_revision.as_deref())
                .map(|(root, revision)| read_git_links(root, revision))
                .unwrap_or_default()
        })
    }

    /// Whether the index carries no correlatable facts (so [`correlate`] can
    /// return no link). Resolves deferred history when called. An index with a
    /// repo root but no commits is still empty.
    pub fn is_empty(&self) -> bool {
        self.git_links().commit_by_prefix.is_empty()
    }

    /// The current project's workspace anchor (git root as a string), used as
    /// the `query_workspace` for cwd-relative workspace-match scoring.
    pub fn project_workspace(&self) -> Option<String> {
        self.repo_root
            .as_ref()
            .map(|root| root.to_string_lossy().into_owned())
    }
}

/// Map correlated provenance to the proof status it can actually establish.
/// A landed commit carries `ProofDebt` until claim-specific proof is
/// available. This correlation layer does not produce `Proven`.
pub fn proof_for(outcome: OutcomeMarker, has_commit: bool) -> ProofStatus {
    match outcome {
        OutcomeMarker::Landed if has_commit => ProofStatus::ProofDebt,
        _ => ProofStatus::Unknown,
    }
}

/// Whether a result's workspace is the project the agent is in now (cwd-relative
/// workspace match, q4pau). Component-aware path containment: either path may be
/// the other's ancestor (a session run in a subdirectory still belongs to the
/// project), compared case-insensitively with a trailing slash trimmed. Pure.
pub fn workspace_matches(query_workspace: &str, result_workspace: &str) -> bool {
    let q = query_workspace.trim().trim_end_matches('/');
    let w = result_workspace.trim().trim_end_matches('/');
    if q.is_empty() || w.is_empty() {
        return false;
    }
    if q.eq_ignore_ascii_case(w) {
        return true;
    }
    // Ancestor containment on a path boundary (avoid "/proj/a" matching
    // "/proj/ab"): the longer must start with the shorter followed by '/'.
    let (shorter, longer) = if q.len() <= w.len() { (q, w) } else { (w, q) };
    longer.len() > shorter.len()
        && longer.as_bytes()[shorter.len()] == b'/'
        && longer[..shorter.len()].eq_ignore_ascii_case(shorter)
}

/// Join a result's text fields into a single bounded scan string for
/// [`correlate`], capped at [`MAX_SCAN_CHARS`] characters (char-safe, one
/// allocation). Refs appear early in practice, so a bounded prefix suffices.
pub fn scan_text(parts: &[&str]) -> String {
    let mut out = String::new();
    let mut budget = MAX_SCAN_CHARS;
    for part in parts {
        if budget == 0 {
            break;
        }
        if !out.is_empty() {
            out.push(' ');
            budget -= 1;
        }
        for c in part.chars() {
            if budget == 0 {
                break;
            }
            out.push(c);
            budget -= 1;
        }
    }
    out
}

/// Whether a char can appear inside an abbreviated commit id.
fn is_id_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_')
}

/// Whether `word` is a plausible commit id reference (hex, long enough to be
/// collision-free, not so short it matches noise).
fn is_commit_word(word: &str) -> bool {
    word.len() >= COMMIT_PREFIX_LEN
        && word.len() <= 40
        && word.chars().all(|c| c.is_ascii_hexdigit())
}

/// Walk `text` (bounded) splitting it into maximal identifier words. Each word is
/// a run of [`is_id_char`] characters. Stops after [`MAX_SCAN_CHARS`] characters.
fn id_words(text: &str) -> Vec<&str> {
    let mut words = Vec::new();
    let mut start: Option<usize> = None;
    for (idx, c) in text.char_indices().take(MAX_SCAN_CHARS) {
        if is_id_char(c) {
            start.get_or_insert(idx);
        } else if let Some(s) = start.take() {
            words.push(&text[s..idx]);
        }
    }
    // A trailing word still open at the scan boundary.
    if let Some(s) = start {
        // Re-derive the end from the bounded scan to keep the slice in range.
        let end = text
            .char_indices()
            .take(MAX_SCAN_CHARS)
            .last()
            .map(|(i, c)| i + c.len_utf8())
            .unwrap_or(text.len());
        if s < end {
            words.push(&text[s..end]);
        }
    }
    words
}

/// Scan `text` for an explicit reference to a known commit id and return the
/// strongest link found. Live indexes load their pinned Git history on the
/// first plausible commit reference.
///
/// Deterministic: the first match in sorted-id order wins.
pub fn correlate(index: &CorrelationIndex, text: &str) -> CorrelationLink {
    let mut commits: Vec<&str> = Vec::new();

    for word in id_words(text) {
        if is_commit_word(word) {
            commits.push(word);
        }
    }

    // No explicit identifier can match: avoid reading thousands of Git objects
    // for a result that only needs workspace/recency trust signals.
    if commits.is_empty() {
        return CorrelationLink::none();
    }
    let git_links = index.git_links();
    commits.retain(|word| {
        // is_commit_word guarantees ASCII hex and at least COMMIT_PREFIX_LEN.
        git_links
            .commit_by_prefix
            .contains_key(&word[..COMMIT_PREFIX_LEN].to_ascii_lowercase())
    });

    commits.sort_unstable();
    commits.dedup();

    if let Some(commit) = commits.first().and_then(|word| {
        git_links
            .commit_by_prefix
            .get(&word[..COMMIT_PREFIX_LEN].to_ascii_lowercase())
            .cloned()
    }) {
        // A real commit in project history is landed work.
        return CorrelationLink {
            linked_commit: Some(commit),
            outcome: OutcomeMarker::Landed,
        };
    }
    CorrelationLink::none()
}

// ---------------------------------------------------------------------------
// Live index construction (fail-open I/O).
// ---------------------------------------------------------------------------

/// Build a correlation index for the project containing the working directory.
/// Always succeeds: returns an empty index when there is no repo or git is
/// unavailable.
pub fn build_for_cwd() -> CorrelationIndex {
    std::env::current_dir()
        .ok()
        .and_then(|cwd| build_for_repo(&cwd))
        .unwrap_or_default()
}

/// Build a correlation index for the git repo containing `start`. `None` when
/// `start` is not inside a git repo (so the caller falls back to an empty
/// index).
fn build_for_repo(start: &Path) -> Option<CorrelationIndex> {
    let root = git_output(start, &["rev-parse", "--show-toplevel"])?;
    let root = PathBuf::from(root);

    let git_revision = git_output(&root, &["rev-parse", "--verify", "HEAD"]);

    Some(CorrelationIndex {
        git_links: OnceLock::new(),
        git_revision,
        repo_root: Some(root),
    })
}

/// Read known commit prefixes from recent git history. Fail-open: no git or no
/// repo yields an empty map.
fn read_git_links(root: &Path, revision: &str) -> GitLinks {
    let mut commit_by_prefix = HashMap::new();
    let log = git_output(
        root,
        &[
            "log",
            &format!("-n{GIT_LOG_LIMIT}"),
            "--no-color",
            "--format=%H",
            revision,
        ],
    );
    let Some(log) = log else {
        return GitLinks::default();
    };
    for line in log.lines() {
        let sha = line.trim();
        if sha.len() >= COMMIT_PREFIX_LEN && sha.chars().all(|c| c.is_ascii_hexdigit()) {
            commit_by_prefix
                .entry(sha[..COMMIT_PREFIX_LEN].to_ascii_lowercase())
                .or_insert_with(|| sha.to_ascii_lowercase());
        }
    }
    GitLinks { commit_by_prefix }
}

/// Run a read-only git command in `repo_path`, returning trimmed stdout on
/// success. Fail-open: any spawn error or non-zero exit yields `None`.
fn git_output(repo_path: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index_with(commits: &[&str]) -> CorrelationIndex {
        let commit_by_prefix = commits
            .iter()
            .map(|sha| {
                (
                    sha[..COMMIT_PREFIX_LEN].to_ascii_lowercase(),
                    sha.to_string(),
                )
            })
            .collect();
        CorrelationIndex {
            git_links: OnceLock::from(GitLinks { commit_by_prefix }),
            git_revision: None,
            repo_root: None,
        }
    }

    const SHA_A: &str = "ab0d12ef90abcdef1234567890abcdef12345678";
    const SHA_B: &str = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

    #[test]
    fn live_history_is_deferred_and_pinned_to_the_constructed_revision() {
        let repo = tempfile::tempdir().expect("temporary repository");
        let root = repo.path();
        git_output(root, &["init", "--initial-branch=main"]).expect("initialize repository");
        let commit = |subject| {
            git_output(
                root,
                &[
                    "-c",
                    "user.name=CASS Test",
                    "-c",
                    "user.email=cass-test@example.invalid",
                    "-c",
                    "commit.gpgsign=false",
                    "commit",
                    "--allow-empty",
                    "-m",
                    subject,
                ],
            )
            .expect("create real history");
            git_output(root, &["rev-parse", "HEAD"]).expect("read commit id")
        };
        let original = commit("fix the thing");
        let index = build_for_repo(root).expect("build correlation index");
        assert_eq!(index.git_revision.as_deref(), Some(original.as_str()));
        assert!(index.git_links.get().is_none());
        assert!(correlate(&index, "ordinary text with no id").is_empty());
        assert!(index.git_links.get().is_none());

        // Publishing another commit after construction must not change which
        // commit corroborates this query's reference.
        let successor = commit("follow-up work");
        let linked = correlate(&index, &format!("landed as {original}"));
        assert_eq!(linked.linked_commit.as_deref(), Some(original.as_str()));
        assert_eq!(linked.outcome, OutcomeMarker::Landed);
        assert!(index.git_links.get().is_some());
        assert_eq!(
            correlate(&index, &original[..COMMIT_PREFIX_LEN]).linked_commit,
            Some(original)
        );
        assert!(correlate(&index, &successor).is_empty());
    }

    #[test]
    fn empty_index_correlates_to_nothing() {
        let idx = CorrelationIndex::default();
        assert_eq!(
            correlate(&idx, "mentions some text"),
            CorrelationLink::none()
        );
        assert!(idx.is_empty());
    }

    #[test]
    fn bare_commit_reference_is_landed() {
        let idx = index_with(&[SHA_B]);
        let text = format!("landed as {SHA_B} last week");
        let link = correlate(&idx, &text);
        assert_eq!(link.outcome, OutcomeMarker::Landed);
        assert_eq!(link.linked_commit.as_deref(), Some(SHA_B));
    }

    #[test]
    fn abbreviated_commit_reference_matches_by_prefix() {
        let idx = index_with(&[SHA_A]);
        // 12-char abbreviation of SHA_A.
        let link = correlate(&idx, "see commit ab0d12ef90ab for the fix");
        assert_eq!(link.linked_commit.as_deref(), Some(SHA_A));
    }

    #[test]
    fn short_hex_below_prefix_len_is_ignored() {
        let idx = index_with(&[SHA_A]);
        // 7-char abbreviation is below COMMIT_PREFIX_LEN; not matched.
        let link = correlate(&idx, "see commit ab0d12e for the fix");
        assert!(link.is_empty());
    }

    #[test]
    fn unrelated_text_does_not_fabricate_a_link() {
        let idx = index_with(&[SHA_A]);
        let link = correlate(&idx, "a totally unrelated conversation about cooking pasta");
        assert!(link.is_empty(), "no explicit id => no link");
    }

    #[test]
    fn proof_for_landed_commit_is_proof_debt() {
        assert_eq!(
            proof_for(OutcomeMarker::Landed, true),
            ProofStatus::ProofDebt
        );
    }

    #[test]
    fn proof_for_no_commit_or_other_outcome_is_unknown() {
        assert_eq!(
            proof_for(OutcomeMarker::Landed, false),
            ProofStatus::Unknown
        );
        for outcome in [
            OutcomeMarker::Open,
            OutcomeMarker::Unknown,
            OutcomeMarker::Failed,
            OutcomeMarker::Superseded,
        ] {
            for has_commit in [false, true] {
                assert_eq!(
                    proof_for(outcome, has_commit),
                    ProofStatus::Unknown,
                    "{outcome:?}, commit={has_commit}"
                );
            }
        }
    }

    #[test]
    fn id_words_is_bounded_and_char_safe() {
        // Non-ASCII before/after an id must not panic or split a char.
        let text = "héllo ab0d12ef90ab wörld";
        let words = id_words(text);
        assert!(words.contains(&"ab0d12ef90ab"));
    }

    #[test]
    fn workspace_matches_handles_equality_containment_and_boundaries() {
        // Exact (case + trailing slash insensitive).
        assert!(workspace_matches("/proj/a", "/proj/a/"));
        assert!(workspace_matches("/Proj/A", "/proj/a"));
        // Ancestor containment either direction (session run in a subdir).
        assert!(workspace_matches("/proj/a", "/proj/a/src/search"));
        assert!(workspace_matches("/proj/a/src", "/proj/a"));
        // Sibling and prefix-not-on-boundary must NOT match.
        assert!(!workspace_matches("/proj/a", "/proj/b"));
        assert!(!workspace_matches("/proj/a", "/proj/ab"));
        // Empty never matches.
        assert!(!workspace_matches("", "/proj/a"));
        assert!(!workspace_matches("/proj/a", "   "));
    }

    #[test]
    fn scan_text_joins_and_bounds() {
        let joined = scan_text(&["title", "snippet", "body"]);
        assert_eq!(joined, "title snippet body");
        // Bounded to MAX_SCAN_CHARS.
        let big = "x".repeat(MAX_SCAN_CHARS * 2);
        assert_eq!(scan_text(&[&big]).chars().count(), MAX_SCAN_CHARS);
    }

    #[test]
    fn correlate_is_deterministic() {
        let idx = index_with(&[SHA_A, SHA_B]);
        let text = format!("{SHA_B} and {SHA_A} both referenced");
        assert_eq!(correlate(&idx, &text), correlate(&idx, &text));
    }
}
