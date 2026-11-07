use crate::cli::commands::search::safe_robot_score_value::truncate_content;

/// `coding_agent_session_search-nj5eh` regression: pin behavioral
/// equivalence with the prior `(String, bool)` implementation
/// across the boundary cases that determine when truncation
/// fires.
#[test]
pub(crate) fn truncate_content_returns_none_when_chars_within_limit() {
    // Empty
    assert_eq!(truncate_content("", 10), None);
    // Strictly less than max_len
    assert_eq!(truncate_content("abc", 10), None);
    // Exactly max_len — boundary: pre-fix `char_count <= max_len`
    // returned (s.to_string(), false); post-fix returns None.
    assert_eq!(truncate_content("abcdefghij", 10), None);
}

#[test]
pub(crate) fn truncate_content_returns_some_when_chars_exceed_limit() {
    // max_len + 1 — first char count beyond limit
    assert_eq!(
        truncate_content("abcdefghijk", 10).as_deref(),
        Some("abcdefg...")
    );
    // Significantly larger than max_len
    assert_eq!(
        truncate_content("abcdefghijklmnopqrstuvwxyz", 10).as_deref(),
        Some("abcdefg...")
    );
}

#[test]
pub(crate) fn truncate_content_handles_max_len_below_ellipsis_budget() {
    // max_len < 3 — saturating_sub(3) yields 0; truncated prefix
    // is empty so output is just "...".
    assert_eq!(truncate_content("abcdef", 0).as_deref(), Some("..."));
    assert_eq!(truncate_content("abcdef", 1).as_deref(), Some("..."));
    assert_eq!(truncate_content("abcdef", 2).as_deref(), Some("..."));
}

#[test]
pub(crate) fn truncate_content_is_utf8_safe_on_multibyte_boundaries() {
    // 4 emojis (each 4 bytes in UTF-8) + ASCII tail; max_len=4
    // means "exactly fits" and returns None.
    let s = "😀😀😀😀x";
    assert_eq!(truncate_content(s, 5), None);
    // max_len=4: 5 chars > 4 → truncate. saturating_sub(3)=1, so
    // cut at the first multibyte boundary (byte 4) and append "..."
    let truncated = truncate_content(s, 4).expect("must truncate when chars > max_len");
    assert_eq!(truncated, "😀...");
}

#[test]
pub(crate) fn truncate_content_short_circuits_on_huge_input() {
    // 100KB ASCII string with max_len=10 — pre-fix walked all
    // 100K chars to count, then 7 more to take. Post-fix walks
    // exactly 11 chars (max_len + 1) before deciding. This test
    // doesn't measure perf but verifies CORRECTNESS on a large
    // input that exposed the original O(N) walk.
    let s = "a".repeat(100_000);
    assert_eq!(truncate_content(&s, 10).as_deref(), Some("aaaaaaa..."));
}

/// Pin the post-fix contract that `apply_content_truncation` relies
/// on: `None` ↔ caller skips clone+insert. Pre-fix, the function
/// always returned an owned String even in the no-truncation case,
/// causing wasted allocations on the robot-mode hot path.
#[test]
pub(crate) fn truncate_content_avoids_allocation_on_no_truncation_path() {
    // The contract is: return None when no truncation happened.
    // Callers (apply_content_truncation) MUST be able to
    // distinguish via `Option::is_none` without inspecting the
    // string contents.
    assert!(truncate_content("short", 1024).is_none());
}
