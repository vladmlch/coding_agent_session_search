use super::super::*;
use super::helpers::*;
use crate::search::tantivy::TantivyIndex;
use tempfile::TempDir;

// =============================================================================
// RRF (Reciprocal Rank Fusion) Tests
// =============================================================================

#[test]
fn test_rrf_fusion_ordering() {
    // Test that RRF correctly combines rankings from both lists
    // Higher ranks in both lists should result in higher final ranking
    let lexical = vec![
        make_test_hit("A", 10.0),
        make_test_hit("B", 8.0),
        make_test_hit("C", 6.0),
    ];
    let semantic = vec![
        make_test_hit("A", 0.9),
        make_test_hit("B", 0.7),
        make_test_hit("D", 0.5),
    ];

    let fused = rrf_fuse_hits(&lexical, &semantic, "", 10, 0);

    // A and B should be top (in both lists), A first (rank 0 in both)
    assert_eq!(fused.len(), 4);
    assert_eq!(fused[0].title, "A"); // Rank 0 in both
    assert_eq!(fused[1].title, "B"); // Rank 1 in both
    // C and D are in only one list each, order depends on their ranks
}

#[test]
fn test_rrf_handles_disjoint_sets() {
    // Test with no overlap between lexical and semantic results
    let lexical = vec![make_test_hit("A", 10.0), make_test_hit("B", 8.0)];
    let semantic = vec![make_test_hit("C", 0.9), make_test_hit("D", 0.7)];

    let fused = rrf_fuse_hits(&lexical, &semantic, "", 10, 0);

    // All 4 items should be present
    assert_eq!(fused.len(), 4);
    let titles: Vec<&str> = fused.iter().map(|h| h.title.as_str()).collect();
    assert!(titles.contains(&"A"));
    assert!(titles.contains(&"B"));
    assert!(titles.contains(&"C"));
    assert!(titles.contains(&"D"));
}

#[test]
fn test_rrf_tie_breaking_deterministic() {
    // Test that results are deterministic - same input always produces same output
    let lexical = vec![
        make_test_hit("X", 5.0),
        make_test_hit("Y", 5.0),
        make_test_hit("Z", 5.0),
    ];
    let semantic = vec![]; // Empty semantic list

    // Run multiple times and verify same order
    let fused1 = rrf_fuse_hits(&lexical, &semantic, "", 10, 0);
    let fused2 = rrf_fuse_hits(&lexical, &semantic, "", 10, 0);
    let fused3 = rrf_fuse_hits(&lexical, &semantic, "", 10, 0);

    // Order should be deterministic based on key comparison
    assert_eq!(fused1.len(), fused2.len());
    assert_eq!(fused2.len(), fused3.len());

    for i in 0..fused1.len() {
        assert_eq!(fused1[i].title, fused2[i].title, "Mismatch at index {}", i);
        assert_eq!(fused2[i].title, fused3[i].title, "Mismatch at index {}", i);
    }
}

#[test]
fn test_rrf_both_lists_bonus() {
    // Documents appearing in both lists should rank higher than those in only one
    // Even if their individual ranks are lower
    let lexical = vec![
        make_test_hit("solo_lex", 10.0), // Rank 0 lexical only
        make_test_hit("both", 5.0),      // Rank 1 lexical
    ];
    let semantic = vec![
        make_test_hit("solo_sem", 0.9), // Rank 0 semantic only
        make_test_hit("both", 0.5),     // Rank 1 semantic
    ];

    let fused = rrf_fuse_hits(&lexical, &semantic, "", 10, 0);

    // "both" should be first due to appearing in both lists
    // It gets RRF score from rank 1 in both lists = 1/(60+2) * 2 = 0.0322
    // vs solo items get 1/(60+1) = 0.0164 each
    assert_eq!(
        fused[0].title, "both",
        "Doc in both lists should rank first"
    );
}

#[test]
fn test_rrf_respects_limit_and_offset() {
    let lexical = vec![
        make_test_hit("A", 10.0),
        make_test_hit("B", 8.0),
        make_test_hit("C", 6.0),
    ];
    let semantic = vec![];

    // Test limit
    let fused = rrf_fuse_hits(&lexical, &semantic, "", 2, 0);
    assert_eq!(fused.len(), 2);

    // Test offset
    let fused_offset = rrf_fuse_hits(&lexical, &semantic, "", 10, 1);
    assert_eq!(fused_offset.len(), 2); // Skipped first one

    // Test limit 0
    let fused_empty = rrf_fuse_hits(&lexical, &semantic, "", 0, 0);
    assert!(fused_empty.is_empty());
}

#[test]
fn test_rrf_empty_inputs() {
    let empty: Vec<SearchHit> = vec![];
    let non_empty = vec![make_test_hit("A", 10.0)];

    // Both empty
    assert!(rrf_fuse_hits(&empty, &empty, "", 10, 0).is_empty());

    // Lexical empty
    let fused = rrf_fuse_hits(&empty, &non_empty, "", 10, 0);
    assert_eq!(fused.len(), 1);
    assert_eq!(fused[0].title, "A");

    // Semantic empty
    let fused = rrf_fuse_hits(&non_empty, &empty, "", 10, 0);
    assert_eq!(fused.len(), 1);
    assert_eq!(fused[0].title, "A");
}

#[test]
fn test_rrf_coalesces_empty_title_hits_across_search_modes() {
    let mut lexical = make_test_hit("shared", 10.0);
    lexical.title.clear();
    lexical.source_path = "/shared/untitled.jsonl".into();
    lexical.content = "same untitled body".into();
    lexical.content_hash = stable_content_hash("same untitled body");

    let mut semantic = lexical.clone();
    semantic.score = 0.9;

    let fused = rrf_fuse_hits(&[lexical], &[semantic], "", 10, 0);
    assert_eq!(fused.len(), 1);
    assert_eq!(fused[0].title, "");
}

#[test]
fn test_rrf_coalesces_blank_local_source_id_hits_across_search_modes() {
    let mut lexical = make_test_hit("shared-local", 10.0);
    lexical.source_path = "/shared/local.jsonl".into();
    lexical.content = "same local body".into();
    lexical.content_hash = stable_content_hash("same local body");
    lexical.source_id = "local".into();
    lexical.origin_kind = "local".into();

    let mut semantic = lexical.clone();
    semantic.source_id = "   ".into();
    semantic.origin_kind = "local".into();
    semantic.score = 0.9;

    let fused = rrf_fuse_hits(&[lexical], &[semantic], "", 10, 0);
    assert_eq!(fused.len(), 1);
    assert_eq!(fused[0].source_id, "local");
}

#[test]
fn test_rrf_keeps_repeated_same_content_at_different_lines() {
    let mut first = make_test_hit("same", 10.0);
    first.title = "Shared Session".into();
    first.source_path = "/shared/session.jsonl".into();
    first.content = "repeat me".into();
    first.content_hash = stable_content_hash("repeat me");
    first.line_number = Some(1);
    first.created_at = Some(100);

    let mut second = first.clone();
    second.line_number = Some(2);
    second.created_at = Some(200);
    second.score = 0.9;

    let fused = rrf_fuse_hits(&[first], &[second], "", 10, 0);
    assert_eq!(fused.len(), 2);
    assert_eq!(fused[0].line_number, Some(1));
    assert_eq!(fused[1].line_number, Some(2));
}

#[test]
fn test_rrf_coalesces_present_and_missing_conversation_id_for_same_message() {
    let mut lexical = make_test_hit("same", 10.0);
    lexical.title = "Shared Session".into();
    lexical.source_path = "/shared/session.jsonl".into();
    lexical.content = "identical body".into();
    lexical.content_hash = stable_content_hash("identical body");
    lexical.created_at = Some(100);
    lexical.line_number = Some(1);
    lexical.conversation_id = None;

    let mut semantic = lexical.clone();
    semantic.conversation_id = Some(42);
    semantic.score = 0.9;

    let fused = rrf_fuse_hits(&[lexical], &[semantic], "", 10, 0);
    assert_eq!(fused.len(), 1);
    assert_eq!(fused[0].conversation_id, Some(42));
}

#[test]
fn test_rrf_coalesces_present_and_missing_conversation_id_despite_blank_local_source_id() {
    let mut lexical = make_test_hit("same", 10.0);
    lexical.title = "Shared Session".into();
    lexical.source_path = "/shared/session.jsonl".into();
    lexical.content = "identical body".into();
    lexical.content_hash = stable_content_hash("identical body");
    lexical.created_at = Some(100);
    lexical.line_number = Some(1);
    lexical.conversation_id = None;
    lexical.source_id = "local".into();
    lexical.origin_kind = "local".into();

    let mut semantic = lexical.clone();
    semantic.conversation_id = Some(42);
    semantic.source_id = "   ".into();
    semantic.origin_kind = "local".into();
    semantic.score = 0.9;

    let fused = rrf_fuse_hits(&[lexical], &[semantic], "", 10, 0);
    assert_eq!(fused.len(), 1);
    assert_eq!(fused[0].conversation_id, Some(42));
}

#[test]
fn test_rrf_keeps_distinct_conversation_ids_for_shared_path_and_content() {
    let mut first = make_test_hit("same", 10.0);
    first.title = "Shared Session".into();
    first.source_path = "/shared/session.jsonl".into();
    first.content = "identical body".into();
    first.content_hash = stable_content_hash("identical body");
    first.conversation_id = Some(1);

    let mut second = first.clone();
    second.conversation_id = Some(2);
    second.score = 0.9;

    let fused = rrf_fuse_hits(&[first], &[second], "", 10, 0);
    assert_eq!(fused.len(), 2);
    assert!(fused.iter().any(|hit| hit.conversation_id == Some(1)));
    assert!(fused.iter().any(|hit| hit.conversation_id == Some(2)));
}

#[test]
fn test_rrf_coalesces_same_conversation_id_despite_title_drift() {
    let mut lexical = make_test_hit("same", 10.0);
    lexical.title = "Morning Session".into();
    lexical.source_path = "/shared/session.jsonl".into();
    lexical.content = "identical body".into();
    lexical.content_hash = stable_content_hash("identical body");
    lexical.conversation_id = Some(9);

    let mut semantic = lexical.clone();
    semantic.title = "Evening Session".into();
    semantic.score = 0.9;

    let fused = rrf_fuse_hits(&[lexical], &[semantic], "", 10, 0);
    assert_eq!(fused.len(), 1);
    assert_eq!(fused[0].conversation_id, Some(9));
}

#[test]
fn test_rrf_keeps_distinct_titles_for_shared_path_and_content() {
    let mut morning = make_test_hit("same", 10.0);
    morning.title = "Morning Session".into();
    morning.source_path = "/shared/session.jsonl".into();
    morning.content = "identical body".into();
    morning.content_hash = stable_content_hash("identical body");
    morning.created_at = None;

    let mut evening = morning.clone();
    evening.title = "Evening Session".into();
    evening.score = 0.9;

    let fused = rrf_fuse_hits(&[morning], &[evening], "", 10, 0);
    assert_eq!(fused.len(), 2);
    assert!(fused.iter().any(|hit| hit.title == "Morning Session"));
    assert!(fused.iter().any(|hit| hit.title == "Evening Session"));
}

#[test]
fn test_rrf_candidate_depth() {
    // Test with many candidates to ensure proper fusion
    let lexical: Vec<_> = (0..50)
        .map(|i| make_test_hit(&format!("L{}", i), 100.0 - i as f32))
        .collect();
    let semantic: Vec<_> = (0..50)
        .map(|i| make_test_hit(&format!("S{}", i), 1.0 - 0.01 * i as f32))
        .collect();

    let fused = rrf_fuse_hits(&lexical, &semantic, "", 20, 0);

    // Should return 20 items
    assert_eq!(fused.len(), 20);

    // All items should be unique
    let mut seen = std::collections::HashSet::new();
    for hit in &fused {
        assert!(seen.insert(&hit.title), "Duplicate hit: {}", hit.title);
    }
}

// ==========================================================================
// QueryTokenList Behavior Tests (Opt 4.4)
// ==========================================================================

#[test]
fn query_token_list_parses_small_queries() {
    let cases = [
        ("hello", 1),
        ("hello world", 2),
        ("hello AND world", 3),
        ("hello world foo bar", 4),
    ];

    for (query, expected_len) in cases {
        let tokens = parse_boolean_query(query);
        assert_eq!(tokens.len(), expected_len, "{query}");
    }
}

#[test]
fn query_token_list_parses_large_queries() {
    let tokens = parse_boolean_query("a b c d e f g h i");
    assert_eq!(tokens.len(), 9);
}

#[test]
fn query_token_list_handles_quoted_phrases() {
    let tokens = parse_boolean_query("\"hello world\" test");
    assert_eq!(tokens.len(), 2);

    // Verify the phrase is correctly parsed
    assert!(
        matches!(&tokens[0], QueryToken::Phrase(phrase) if phrase == "hello world"),
        "Expected Phrase token"
    );
}

#[test]
fn query_token_list_handles_operators() {
    let tokens = parse_boolean_query("foo AND bar OR baz");
    assert_eq!(tokens.len(), 5);
    assert_eq!(tokens[1], QueryToken::And);
    assert_eq!(tokens[3], QueryToken::Or);
}

#[test]
fn query_token_list_empty_query() {
    let tokens = parse_boolean_query("");
    assert!(tokens.is_empty());
}

#[test]
fn query_token_list_iteration_works() {
    let tokens = parse_boolean_query("a b c");
    let terms: Vec<_> = tokens
        .iter()
        .filter_map(|t| match t {
            QueryToken::Term(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(terms, vec!["a", "b", "c"]);
}

// ==========================================================================
// Unicode Query Parsing Tests (br-327c)
// Comprehensive Unicode handling tests covering emoji, CJK, RTL, mixed
// scripts, zero-width characters, combining characters, normalization,
// supplementary plane characters, and bidirectional text.
// ==========================================================================

// --- Emoji queries ---

#[test]
fn unicode_emoji_treated_as_separator() {
    // Emoji are not alphanumeric per Unicode, so sanitize_query replaces them with spaces
    let sanitized = sanitize_query("🚀 launch");
    assert_eq!(sanitized, "  launch", "Emoji should become space");
}

#[test]
fn unicode_emoji_splits_terms() {
    // Emoji between words acts as a separator
    let sanitized = sanitize_query("hot🔥code");
    assert_eq!(sanitized, "hot code", "Emoji between words splits them");
}

#[test]
fn unicode_multiple_emoji_become_spaces() {
    let sanitized = sanitize_query("🚀🔥💻");
    assert_eq!(
        sanitized.trim(),
        "",
        "All-emoji query sanitizes to whitespace"
    );
}

#[test]
fn unicode_emoji_query_parses_without_panic() {
    let tokens = parse_boolean_query("🚀 launch code 🔥");
    let terms: Vec<_> = tokens
        .iter()
        .filter_map(|t| match t {
            QueryToken::Term(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    // Emoji removed by sanitization in normalize_term_parts, only words remain
    assert!(
        terms
            .iter()
            .any(|t| t.contains("launch") || t.contains("code"))
    );
}

// --- CJK character queries ---

#[test]
fn unicode_cjk_chinese_preserved() {
    assert_eq!(sanitize_query("测试代码"), "测试代码");
    assert_eq!(sanitize_query("测试 代码"), "测试 代码");
}

#[test]
fn unicode_cjk_japanese_preserved() {
    assert_eq!(sanitize_query("テスト"), "テスト");
    // Hiragana and Katakana are alphanumeric
    assert_eq!(sanitize_query("こんにちは世界"), "こんにちは世界");
}

#[test]
fn unicode_cjk_korean_preserved() {
    assert_eq!(sanitize_query("테스트"), "테스트");
    assert_eq!(sanitize_query("안녕하세요"), "안녕하세요");
}

#[test]
fn unicode_cjk_parsed_as_terms() {
    let tokens = parse_boolean_query("测试 代码 search");
    let terms: Vec<_> = tokens
        .iter()
        .filter_map(|t| match t {
            QueryToken::Term(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(terms, vec!["测试", "代码", "search"]);
}

// --- RTL text queries ---

#[test]
fn unicode_hebrew_preserved() {
    assert_eq!(sanitize_query("שלום עולם"), "שלום עולם");
}

#[test]
fn unicode_arabic_preserved() {
    assert_eq!(sanitize_query("مرحبا"), "مرحبا");
}

#[test]
fn unicode_hebrew_parsed_as_terms() {
    let tokens = parse_boolean_query("שלום עולם");
    let terms: Vec<_> = tokens
        .iter()
        .filter_map(|t| match t {
            QueryToken::Term(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(terms, vec!["שלום", "עולם"]);
}

// --- Mixed script queries ---

#[test]
fn unicode_mixed_scripts_preserved() {
    let sanitized = sanitize_query("Hello 世界 мир");
    assert_eq!(sanitized, "Hello 世界 мир");
}

#[test]
fn unicode_mixed_scripts_parsed() {
    let tokens = parse_boolean_query("Hello 世界 мир");
    let terms: Vec<_> = tokens
        .iter()
        .filter_map(|t| match t {
            QueryToken::Term(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(terms, vec!["Hello", "世界", "мир"]);
}

#[test]
fn unicode_mixed_scripts_with_emoji() {
    // Emoji stripped, scripts preserved
    let sanitized = sanitize_query("Hello 🌍 世界");
    assert_eq!(sanitized, "Hello   世界");
}

// --- Zero-width characters ---

#[test]
fn unicode_zero_width_joiner_removed() {
    // Zero-width joiner (U+200D) is not alphanumeric → becomes space
    let sanitized = sanitize_query("test\u{200D}query");
    assert_eq!(sanitized, "test query");
}

#[test]
fn unicode_zero_width_non_joiner_removed() {
    // Zero-width non-joiner (U+200C) is not alphanumeric → becomes space
    let sanitized = sanitize_query("test\u{200C}query");
    assert_eq!(sanitized, "test query");
}

#[test]
fn unicode_zero_width_space_removed() {
    // Zero-width space (U+200B) is not alphanumeric → becomes space
    let sanitized = sanitize_query("test\u{200B}query");
    assert_eq!(sanitized, "test query");
}

#[test]
fn unicode_bom_removed() {
    // Byte-order mark (U+FEFF) should not appear in search terms
    let sanitized = sanitize_query("\u{FEFF}test");
    assert_eq!(sanitized, " test");
}

// --- Combining characters ---

#[test]
fn unicode_precomposed_accent_preserved() {
    // Precomposed é (U+00E9) is a single letter → alphanumeric
    let sanitized = sanitize_query("café");
    assert_eq!(sanitized, "café");
}

#[test]
fn unicode_combining_accent_becomes_separator() {
    // Decomposed: 'e' + combining acute accent (U+0301)
    // nfc_sanitize_query first normalizes to NFC, composing e + U+0301
    // into precomposed é (U+00E9), which is alphanumeric and preserved.
    let input = "cafe\u{0301}";
    let sanitized = sanitize_query(input);
    assert_eq!(sanitized, "caf\u{00e9}");
}

#[test]
fn unicode_nfc_and_nfd_produce_same_sanitized_query() {
    // NFC (precomposed): é = U+00E9 (single char, alphanumeric)
    let nfc = "caf\u{00E9}";
    // NFD (decomposed): e + ◌́ = U+0065 U+0301 (two chars, accent not alphanumeric)
    let nfd = "cafe\u{0301}";

    let san_nfc = sanitize_query(nfc);
    let san_nfd = sanitize_query(nfd);

    // Both produce "café" because nfc_sanitize_query normalizes to NFC
    // before sanitization, matching the NFC-indexed content from
    // DefaultCanonicalizer.
    assert_eq!(san_nfc, "café");
    assert_eq!(san_nfd, "café");
    assert_eq!(san_nfc, san_nfd);
}

#[test]
fn unicode_combining_marks_do_not_panic() {
    // Multiple combining marks stacked (e.g., Zalgo text)
    let zalgo = "t\u{0301}\u{0302}\u{0303}e\u{0304}\u{0305}st";
    let sanitized = sanitize_query(zalgo);
    // Should not panic; combining marks become spaces
    assert!(sanitized.contains('t'));
    assert!(sanitized.contains('s'));
}

// --- Supplementary plane characters (outside BMP) ---

#[test]
fn unicode_mathematical_bold_letters_preserved() {
    // Mathematical Bold Capital A (U+1D400) — classified as Letter
    let input = "\u{1D400}\u{1D401}\u{1D402}";
    let sanitized = sanitize_query(input);
    assert_eq!(
        sanitized, input,
        "Mathematical bold letters are alphanumeric"
    );
}

#[test]
fn unicode_supplementary_ideograph_preserved() {
    // CJK Unified Ideographs Extension B character (U+20000)
    let input = "\u{20000}";
    let sanitized = sanitize_query(input);
    assert_eq!(
        sanitized, input,
        "Supplementary CJK ideographs are alphanumeric"
    );
}

#[test]
fn unicode_supplementary_emoji_removed() {
    // Grinning face (U+1F600) — Symbol, not alphanumeric
    let input = "test\u{1F600}query";
    let sanitized = sanitize_query(input);
    assert_eq!(sanitized, "test query");
}

// --- Bidirectional text ---

#[test]
fn unicode_bidi_mixed_ltr_rtl_no_panic() {
    let input = "hello שלום world עולם";
    let tokens = parse_boolean_query(input);
    let terms: Vec<_> = tokens
        .iter()
        .filter_map(|t| match t {
            QueryToken::Term(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(terms.len(), 4);
    assert!(terms.contains(&"hello"));
    assert!(terms.contains(&"שלום"));
    assert!(terms.contains(&"world"));
    assert!(terms.contains(&"עולם"));
}

#[test]
fn unicode_bidi_override_chars_removed() {
    // Left-to-right override (U+202D) and pop directional (U+202C)
    // These are format characters, not alphanumeric
    let input = "test\u{202D}content\u{202C}end";
    let sanitized = sanitize_query(input);
    assert_eq!(sanitized, "test content end");
}

#[test]
fn unicode_bidi_rtl_mark_removed() {
    // Right-to-left mark (U+200F) is not alphanumeric
    let input = "test\u{200F}content";
    let sanitized = sanitize_query(input);
    assert_eq!(sanitized, "test content");
}

// --- Full pipeline integration tests ---

#[test]
fn unicode_full_pipeline_cjk_query() {
    let explanation = QueryExplanation::analyze("测试 代码", &SearchFilters::default());
    assert_eq!(explanation.parsed.terms.len(), 2);
    assert!(!explanation.parsed.terms[0].text.is_empty());
    assert!(!explanation.parsed.terms[1].text.is_empty());
}

#[test]
fn unicode_full_pipeline_mixed_script_boolean() {
    let explanation =
        QueryExplanation::analyze("Hello AND 世界 OR مرحبا", &SearchFilters::default());
    // Should parse operators correctly even with mixed scripts
    assert!(
        explanation.parsed.operators.iter().any(|op| op == "AND"),
        "AND operator should be recognized in mixed-script query"
    );
}

#[test]
fn unicode_full_pipeline_emoji_query_type() {
    // An all-emoji query sanitizes to empty — should handle gracefully
    let explanation = QueryExplanation::analyze("🚀🔥💻", &SearchFilters::default());
    // Should not panic; terms may be empty after sanitization
    assert!(
        explanation.parsed.terms.is_empty()
            || explanation
                .parsed
                .terms
                .iter()
                .all(|t| t.subterms.is_empty()),
        "All-emoji query should produce no meaningful terms"
    );
}

#[test]
fn unicode_full_pipeline_phrase_with_cjk() {
    let explanation = QueryExplanation::analyze("\"测试代码\"", &SearchFilters::default());
    assert!(
        !explanation.parsed.phrases.is_empty(),
        "CJK phrase should be recognized"
    );
}

#[test]
fn unicode_full_pipeline_wildcard_with_unicode() {
    let explanation = QueryExplanation::analyze("*测试*", &SearchFilters::default());
    assert!(
        !explanation.parsed.terms.is_empty(),
        "Wildcard with CJK should produce terms"
    );
    // Check that the term has a substring/wildcard pattern
    if let Some(term) = explanation.parsed.terms.first() {
        assert!(
            term.subterms
                .iter()
                .any(|s| s.pattern.contains("*") || s.pattern == "exact"),
            "CJK wildcard should produce wildcard or exact pattern"
        );
    }
}

#[test]
fn unicode_normalize_term_parts_cjk() {
    let parts = normalize_term_parts("测试 代码");
    assert_eq!(parts, vec!["测试", "代码"]);
}

#[test]
fn unicode_normalize_term_parts_strips_emoji() {
    let parts = normalize_term_parts("🚀launch🔥code");
    // Emoji replaced with space, splitting into two terms
    assert!(parts.contains(&"launch".to_string()));
    assert!(parts.contains(&"code".to_string()));
}

// ── Special character query tests (br-g650) ────────────────────────────

// Category 1: Unbalanced quotes

#[test]
fn special_char_unbalanced_quote_no_panic() {
    let tokens = parse_boolean_query("\"hello world");
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, QueryToken::Phrase(p) if p.contains("hello"))),
        "Unbalanced quote should still produce a phrase: {tokens:?}"
    );
}

#[test]
fn special_char_unbalanced_trailing_quote() {
    let tokens = parse_boolean_query("test\"");
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, QueryToken::Term(w) if w == "test")),
        "Text before trailing quote should parse as term: {tokens:?}"
    );
}

#[test]
fn special_char_multiple_unbalanced_quotes() {
    let tokens = parse_boolean_query("\"foo \"bar");
    assert!(
        !tokens.is_empty(),
        "Should parse despite odd quotes: {tokens:?}"
    );
}

#[test]
fn special_char_empty_quotes() {
    let tokens = parse_boolean_query("\"\" test");
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, QueryToken::Term(w) if w == "test")),
        "Empty quotes should be skipped: {tokens:?}"
    );
}

#[test]
fn special_char_unbalanced_via_sanitize() {
    let sanitized = sanitize_query("\"hello world");
    assert!(
        sanitized.contains('"'),
        "Quotes preserved by sanitize_query"
    );
}

// Category 2: Escaped quotes

#[test]
fn special_char_backslash_quote_sanitize() {
    let sanitized = sanitize_query("\\\"test\\\"");
    assert!(sanitized.contains('"'));
    assert!(!sanitized.contains('\\'), "Backslash should be stripped");
}

#[test]
fn special_char_backslash_quote_parse() {
    let tokens = parse_boolean_query("\\\"test\\\"");
    assert!(!tokens.is_empty(), "Should parse without panic: {tokens:?}");
}

#[test]
fn special_char_inner_escaped_quotes() {
    let tokens = parse_boolean_query("\"test \\\"inner\\\" test\"");
    assert!(
        !tokens.is_empty(),
        "Nested escaped quotes should not panic: {tokens:?}"
    );
}

// Category 3: Backslash sequences

#[test]
fn special_char_windows_path_sanitize() {
    let sanitized = sanitize_query("C:\\Users\\test");
    assert_eq!(sanitized, "C  Users test");
}

#[test]
fn special_char_unc_path_sanitize() {
    let sanitized = sanitize_query("\\\\server\\share");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert!(parts.contains(&"server"));
    assert!(parts.contains(&"share"));
}

#[test]
fn special_char_windows_path_terms() {
    let parts = normalize_term_parts("C:\\Users\\test\\file.rs");
    assert!(parts.contains(&"C".to_string()));
    assert!(parts.contains(&"Users".to_string()));
    assert!(parts.contains(&"test".to_string()));
    assert!(parts.contains(&"file".to_string()));
    assert!(parts.contains(&"rs".to_string()));
}

// Category 4: Regex metacharacters

#[test]
fn special_char_regex_dot_star() {
    let sanitized = sanitize_query("foo.*bar");
    assert_eq!(sanitized, "foo *bar");
}

#[test]
fn special_char_regex_char_class() {
    let sanitized = sanitize_query("[a-z]+");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert_eq!(parts, vec!["a-z"]);
    assert_eq!(normalize_term_parts("[a-z]+"), vec!["a", "z"]);
}

#[test]
fn special_char_regex_anchors() {
    let sanitized = sanitize_query("^start$");
    assert_eq!(sanitized.trim(), "start");
}

#[test]
fn special_char_regex_pipe_groups() {
    let sanitized = sanitize_query("(foo|bar)");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert_eq!(parts, vec!["foo", "bar"]);
}

// Category 5: SQL injection patterns

#[test]
fn special_char_sql_injection_or() {
    let sanitized = sanitize_query("'OR 1=1--");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert!(parts.contains(&"OR"));
    assert!(parts.contains(&"1"));
    assert!(!sanitized.contains('\''));
    assert!(!sanitized.contains('='));
}

#[test]
fn special_char_sql_injection_drop() {
    let sanitized = sanitize_query("; DROP TABLE users;--");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert!(parts.contains(&"DROP"));
    assert!(parts.contains(&"TABLE"));
    assert!(parts.contains(&"users"));
    assert!(!sanitized.contains(';'));
}

#[test]
fn special_char_sql_injection_union() {
    let sanitized = sanitize_query("' UNION SELECT * FROM passwords --");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert!(parts.contains(&"UNION"));
    assert!(parts.contains(&"SELECT"));
    assert!(parts.contains(&"*"));
    assert!(parts.contains(&"FROM"));
    assert!(parts.contains(&"passwords"));
}

#[test]
fn special_char_sql_parse_as_literal() {
    let tokens = parse_boolean_query("OR 1=1");
    assert!(
        tokens.iter().any(|t| matches!(t, QueryToken::Or)),
        "OR should be parsed as Or operator: {tokens:?}"
    );
}

// Category 6: Shell injection patterns

#[test]
fn special_char_shell_subshell() {
    let sanitized = sanitize_query("$(cmd)");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert_eq!(parts, vec!["cmd"]);
}

#[test]
fn special_char_shell_backticks() {
    let sanitized = sanitize_query("`cmd`");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert_eq!(parts, vec!["cmd"]);
}

#[test]
fn special_char_shell_pipe_rm() {
    let sanitized = sanitize_query("| rm -rf /");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert!(parts.contains(&"rm"));
    assert!(parts.contains(&"-rf"));
    assert_eq!(normalize_term_parts("| rm -rf /"), vec!["rm", "rf"]);
    assert!(!sanitized.contains('|'));
    assert!(!sanitized.contains('/'));
}

#[test]
fn special_char_shell_semicolon_chain() {
    let sanitized = sanitize_query("test; echo pwned; cat /etc/passwd");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert!(parts.contains(&"test"));
    assert!(parts.contains(&"echo"));
    assert!(parts.contains(&"pwned"));
    assert!(!sanitized.contains(';'));
}

// Category 7: Null bytes

#[test]
fn special_char_null_byte_mid_string() {
    let sanitized = sanitize_query("test\x00hidden");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert_eq!(parts, vec!["test", "hidden"]);
}

#[test]
fn special_char_null_byte_leading() {
    let sanitized = sanitize_query("\x00\x00attack");
    assert_eq!(sanitized.trim(), "attack");
}

#[test]
fn special_char_null_byte_trailing() {
    let sanitized = sanitize_query("query\x00\x00\x00");
    assert_eq!(sanitized.trim(), "query");
}

#[test]
fn special_char_null_byte_parse() {
    let tokens = parse_boolean_query("test\x00hidden");
    assert!(
        !tokens.is_empty(),
        "Null bytes should not prevent parsing: {tokens:?}"
    );
}

// Category 8: Control characters

#[test]
fn special_char_control_newline() {
    let sanitized = sanitize_query("line1\nline2");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert_eq!(parts, vec!["line1", "line2"]);
}

#[test]
fn special_char_control_tab_cr() {
    let sanitized = sanitize_query("tab\there\r\nend");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert_eq!(parts, vec!["tab", "here", "end"]);
}

#[test]
fn special_char_control_parse_whitespace() {
    let tokens = parse_boolean_query("hello\tworld\ntest");
    let terms: Vec<&str> = tokens
        .iter()
        .filter_map(|t| match t {
            QueryToken::Term(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(terms, vec!["hello", "world", "test"]);
}

#[test]
fn special_char_control_bell_escape() {
    let sanitized = sanitize_query("test\x07\x1b[31mred");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert!(parts.contains(&"test"));
    assert!(parts.contains(&"31mred"));
}

// Category 9: HTML/XML entities

#[test]
fn special_char_html_entity_lt() {
    let sanitized = sanitize_query("&lt;script&gt;");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert_eq!(parts, vec!["lt", "script", "gt"]);
}

#[test]
fn special_char_html_numeric_entity() {
    let sanitized = sanitize_query("&#x3C;script&#x3E;");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert!(parts.contains(&"x3C"));
    assert!(parts.contains(&"script"));
    assert!(parts.contains(&"x3E"));
}

#[test]
fn special_char_html_tags_stripped() {
    let sanitized = sanitize_query("<script>alert('xss')</script>");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert!(parts.contains(&"script"));
    assert!(parts.contains(&"alert"));
    assert!(parts.contains(&"xss"));
}

#[test]
fn special_char_html_attribute() {
    let sanitized = sanitize_query("<img src=\"evil.js\" onerror=\"alert(1)\">");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert!(parts.contains(&"img"));
    assert!(parts.contains(&"src"));
    assert!(parts.contains(&"onerror"));
}

// Category 10: URL encoding

#[test]
fn special_char_url_percent_encoding() {
    let sanitized = sanitize_query("%20space%2Fslash");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert_eq!(parts, vec!["20space", "2Fslash"]);
}

#[test]
fn special_char_url_null_byte_encoded() {
    let sanitized = sanitize_query("test%00hidden");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert_eq!(parts, vec!["test", "00hidden"]);
}

#[test]
fn special_char_url_full_query_string() {
    let sanitized = sanitize_query("search?q=hello&lang=en");
    let parts: Vec<&str> = sanitized.split_whitespace().collect();
    assert_eq!(parts, vec!["search", "q", "hello", "lang", "en"]);
}

// Cross-cutting: full pipeline integration

#[test]
fn special_char_explain_sql_injection() {
    let filters = SearchFilters::default();
    let explanation = QueryExplanation::analyze("'OR 1=1--", &filters);
    assert!(
        !explanation.parsed.terms.is_empty() || !explanation.parsed.phrases.is_empty(),
        "SQL injection should produce parseable terms"
    );
}

#[test]
fn special_char_explain_shell_injection() {
    let filters = SearchFilters::default();
    let explanation = QueryExplanation::analyze("$(rm -rf /)", &filters);
    assert!(
        !explanation.parsed.terms.is_empty(),
        "Shell injection should produce parseable terms"
    );
}

#[test]
fn special_char_explain_html_xss() {
    let filters = SearchFilters::default();
    let explanation = QueryExplanation::analyze("<script>alert('xss')</script>", &filters);
    assert!(
        !explanation.parsed.terms.is_empty(),
        "XSS payload should produce parseable terms"
    );
}

/// frankensearch 0.7's lexer reports grouping parentheses as tokens. The
/// fallback lanes split them off first, so a raw paren token (which would
/// stall `CassBoolParser`) never reaches the parser, and every lane still
/// terminates on unbalanced or code-shaped input.
#[test]
fn cass_bool_tokens_never_pass_a_raw_paren_token() {
    for query in [
        "foo(bar)",
        "a) (b",
        "x -(y)",
        "\"p\"(q)",
        "(a (b) c",
        "((a))",
        "a&&(b||c)",
        "NOT(a)",
        ")(",
        "f(x) OR (g(y) h)",
        "$(rm -rf /)",
    ] {
        let tokens = cass_bool_tokens(query);
        assert!(
            !tokens.iter().any(|token| matches!(
                token,
                CassBoolToken::Token(FsCassQueryToken::LParen | FsCassQueryToken::RParen)
            )),
            "{query:?}: {tokens:?}"
        );
        let _ = read_query(query);
        let _ = transpile_to_fts5(query);
    }
    // Mid-word parentheses stay part of the term.
    assert_eq!(
        cass_bool_tokens("foo(bar)"),
        vec![CassBoolToken::Token(FsCassQueryToken::Term(
            "foo(bar)".to_string()
        ))]
    );
}

/// The Colab fuzz crash input (cargo fuzz, `fuzz_query_transpiler`,
/// ASan; artifact in
/// `.scratch/glm/2026-11-12-query-nesting-depth/crash-input.bin`):
/// 3,934 `(` then `oT`, then bytes that are not valid UTF-8. The fuzz
/// harness's `&str` input goes through `arbitrary`, which truncates at
/// the first invalid byte, so the exact string the target saw is this
/// valid-UTF-8 prefix. The recursive-descent parser pays one
/// four-frame cycle (`parse_or`/`parse_and`/`parse_unary`/
/// `parse_primary`) per `(`, so the unfixed parse drove ~3,900 cycles
/// of native recursion into the guard page. This regression runs the
/// same input on a deliberately small thread stack (256 KiB) so the
/// overflow reproduces here instead of hiding behind the 128 MiB
/// `RUST_MIN_STACK` test-thread default: every recursive stage a
/// search can reach must survive the input, and no later boundary
/// (search entry, lane, renderer, AST drop) may turn it into a crash.
#[test]
fn fuzz_crash_input_never_overflows_a_small_stack() {
    let crash_input = format!("{}oT", "(".repeat(3_934));
    let worker_input = crash_input.clone();
    let worker = std::thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(move || {
            // The exact lane the fuzzer hit...
            let transpiled = transpile_to_fts5(&worker_input);
            // ...plus every other recursive stage on the search path:
            // the `_meta.effective` reading, and the `--explain`
            // analysis (`QueryExplanation::analyze` also parses through
            // `read_query`).
            let reading = read_query(&worker_input);
            let explanation = QueryExplanation::analyze(&worker_input, &SearchFilters::default());
            (transpiled, reading, explanation)
        })
        .expect("spawning the small-stack parser thread");
    let (transpiled, reading, explanation) = worker
        .join()
        .expect("a deeply parenthesised query must be declined, never a crash");
    assert_eq!(
        transpiled, None,
        "the FTS5 transpiler must decline the too-deep query"
    );
    assert!(
        reading.structure.is_none(),
        "the query reading must report no structure for a declined query"
    );
    assert_eq!(
        explanation.original_query, crash_input,
        "the explanation must still round-trip the original query"
    );

    // The typed gate the user-facing boundaries share: the same input
    // must be refused with the measured depth and the limit, never a
    // silent rewrite or a lane-specific degradation.
    let refusal = ensure_query_nesting_depth(&crash_input)
        .expect_err("the crash input must exceed the nesting bound");
    assert_eq!(refusal.limit, MAX_QUERY_NESTING_DEPTH);
    assert!(
        refusal.depth >= 3_934,
        "the crash input's 3,934 groups must all be read: {refusal}"
    );
    let message = refusal.to_string();
    assert!(
        message.contains(&MAX_QUERY_NESTING_DEPTH.to_string()),
        "the typed error must name the limit: {message}"
    );
}

/// Exactly at the bound the deepest admitted query must still work
/// end to end — parse, structure reading, FTS5 transpile — on a
/// small stack; one past it is refused everywhere, with the measured
/// depth and the limit in the typed error.
#[test]
fn nesting_depth_boundary_is_inclusive_then_typed_refusal() {
    let at_limit = format!(
        "{}term{}",
        "(".repeat(MAX_QUERY_NESTING_DEPTH),
        ")".repeat(MAX_QUERY_NESTING_DEPTH)
    );
    let past_limit = format!("{}term", "(".repeat(MAX_QUERY_NESTING_DEPTH + 1));

    // At the limit: admitted, and every lane still answers on a 256 KiB
    // stack — the deepest admitted parse costs ~4 small frames per
    // nesting level, tens of KB in total.
    let worker_input = at_limit.clone();
    let worker = std::thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(move || {
            let reading = read_query(&worker_input);
            let transpiled = transpile_to_fts5(&worker_input);
            (reading, transpiled)
        })
        .expect("spawning the small-stack boundary thread");
    let (reading, transpiled) = worker
        .join()
        .expect("a query at the nesting limit must parse on a small stack");
    ensure_query_nesting_depth(&at_limit).expect("a query at exactly the limit is admitted");
    // 128 nested single-operand groups collapse to the operand itself:
    // the parser wraps only compound groups.
    assert_eq!(reading.structure.as_deref(), Some("term"));
    assert_eq!(transpiled.as_deref(), Some("term"));

    // One past the limit: the gate reports the exact numbers, and every
    // lane declines instead of parsing.
    let refusal = ensure_query_nesting_depth(&past_limit)
        .expect_err("one level past the limit must be refused");
    assert_eq!(refusal.depth, MAX_QUERY_NESTING_DEPTH + 1);
    assert_eq!(refusal.limit, MAX_QUERY_NESTING_DEPTH);
    assert!(
        refusal
            .to_string()
            .contains(&MAX_QUERY_NESTING_DEPTH.to_string()),
        "the typed error must name the limit: {refusal}"
    );
    assert_eq!(
        transpile_to_fts5(&past_limit),
        None,
        "the FTS5 transpiler must decline a past-the-limit query"
    );
    assert!(
        read_query(&past_limit).structure.is_none(),
        "the query reading must report no structure for a refused query"
    );
}

/// The primary lexical engine's own lowering is ANOTHER recursive
/// descent (`parse_or`/`parse_and`/`parse_unary`/`parse_primary` in
/// frankensearch-quill, one cycle per `(`) — this campaign's
/// small-stack probe caught the fuzz crash input overflowing inside
/// the ENGINE, so cass's nesting gate is load-bearing for this lane,
/// not contract sugar: every cass entry into that parser
/// (`SearchClient::search` → `search_tantivy`, the viewer bridge's
/// `parse_lexical_query`, the CLI handlers) sits behind
/// `ensure_query_nesting_depth`. This pin proves the admitted range
/// fits the engine's stack envelope — a query at exactly
/// [`MAX_QUERY_NESTING_DEPTH`] parses on a 4 MiB stack (half the
/// 8 MiB default main-thread stack the production lanes run on). If
/// a future engine change grows the per-level cost, or a cass change
/// lets deeper queries through, this fails instead of shipping a
/// latent overflow.
#[test]
fn quill_lowering_handles_the_gate_admitted_depth_on_a_small_stack() {
    let deep = format!(
        "{}term{}",
        "(".repeat(MAX_QUERY_NESTING_DEPTH),
        ")".repeat(MAX_QUERY_NESTING_DEPTH)
    );
    let worker = std::thread::Builder::new()
        .stack_size(4 * 1024 * 1024)
        .spawn(move || {
            frankensearch::quill::query::CassQueryParser::new(
                frankensearch::quill::schema::CASS_SEMANTIC_SCHEMA,
            )
            .expect("building the CASS query parser")
            .parse(&deep, &FsCassQueryFilters::default())
            .query
        })
        .expect("spawning the small-stack quill thread");
    let _ = worker
        .join()
        .expect("the gate-admitted depth must fit the engine's stack envelope");
}

/// Every `SearchClient` entry a caller can reach refuses a too-deep
/// query with the typed error before any archive work: the refusal
/// carries the measured depth and the limit, so the CLI, TUI, serve
/// and pack callers can surface it without re-lexing the query.
#[test]
fn search_entries_refuse_deep_nesting_before_any_archive_work() -> Result<()> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    index.commit()?;
    let client = SearchClient::open(dir.path(), None)?.expect("index present");
    let deep = format!("{}needle", "(".repeat(MAX_QUERY_NESTING_DEPTH + 1));

    macro_rules! refuse_typed {
        ($call:expr) => {
            match $call {
                Ok(value) => panic!("a too-deep query must be refused, got {value:?}"),
                Err(error) => {
                    let exceeded =
                        error
                            .downcast_ref::<QueryDepthExceeded>()
                            .unwrap_or_else(|| {
                                panic!("expected a typed depth refusal, got: {error:#}")
                            });
                    assert_eq!(exceeded.depth, MAX_QUERY_NESTING_DEPTH + 1);
                    assert_eq!(exceeded.limit, MAX_QUERY_NESTING_DEPTH);
                }
            }
        };
    }
    refuse_typed!(client.search(&deep, SearchFilters::default(), 10, 0, FieldMask::FULL));
    refuse_typed!(client.search_semantic_with_tier(
        &deep,
        SearchFilters::default(),
        10,
        0,
        FieldMask::FULL,
        false,
        SemanticTierMode::Single
    ));
    refuse_typed!(client.search_hybrid_with_tier(
        &deep,
        &deep,
        SearchFilters::default(),
        10,
        0,
        3,
        FieldMask::FULL,
        false,
        SemanticTierMode::Single
    ));
    Ok(())
}

/// `--explain` lexes the raw query, where grouping parentheses now arrive
/// as tokens: they add no term, keep implicit-AND adjacency, and the
/// grouping shows in `structure`.
#[test]
fn explain_reports_grouped_operands_as_bare_terms() {
    let filters = SearchFilters::default();
    let explanation = QueryExplanation::analyze("(alpha OR beta) gamma", &filters);
    let terms: Vec<&str> = explanation
        .parsed
        .terms
        .iter()
        .map(|term| term.text.as_str())
        .collect();
    assert_eq!(terms, vec!["alpha", "beta", "gamma"]);
    assert!(explanation.parsed.implicit_and);
    assert!(
        explanation
            .parsed
            .structure
            .as_deref()
            .is_some_and(|structure| structure.contains("(alpha OR beta)")),
        "{:?}",
        explanation.parsed.structure
    );

    // An explicit OR before the group is not an implicit AND.
    let explanation = QueryExplanation::analyze("alpha OR (beta)", &filters);
    assert!(!explanation.parsed.implicit_and);

    // A parenthesis inside a word is not a group.
    let explanation = QueryExplanation::analyze("foo(bar)", &filters);
    let terms: Vec<&str> = explanation
        .parsed
        .terms
        .iter()
        .map(|term| term.text.as_str())
        .collect();
    assert_eq!(terms, vec!["foo(bar)"]);
}

#[test]
fn special_char_boolean_with_injection() {
    let tokens = parse_boolean_query("search AND 'OR 1=1-- NOT drop");
    assert!(
        tokens.iter().any(|t| matches!(t, QueryToken::And)),
        "Boolean AND should still be recognized: {tokens:?}"
    );
    assert!(
        tokens.iter().any(|t| matches!(t, QueryToken::Not)),
        "Boolean NOT should still be recognized: {tokens:?}"
    );
}
