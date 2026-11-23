use super::super::*;
use super::helpers::*;
use crate::search::vector_index::VectorIndex;
use tempfile::TempDir;

// ==========================================================================
// Query Length Stress Tests (coding_agent_session_search-z1bk)
// Tests for extreme input sizes to ensure parser robustness.
// ==========================================================================

#[test]
fn stress_query_100k_chars_completes_quickly() {
    // 100k character query - must complete in <1 second
    let long_query = "a ".repeat(50000);
    assert_eq!(long_query.len(), 100000);

    let start = std::time::Instant::now();
    let sanitized = sanitize_query(&long_query);
    let elapsed_sanitize = start.elapsed();

    let start = std::time::Instant::now();
    let tokens = parse_boolean_query(&sanitized);
    let elapsed_parse = start.elapsed();

    assert!(
        elapsed_sanitize < std::time::Duration::from_secs(1),
        "sanitize_query with 100k chars took {:?} (>1s)",
        elapsed_sanitize
    );
    assert!(
        elapsed_parse < std::time::Duration::from_secs(1),
        "parse_boolean_query with 100k chars took {:?} (>1s)",
        elapsed_parse
    );
    assert!(!tokens.is_empty(), "100k char query should produce tokens");
}

#[test]
fn stress_query_1000_terms() {
    // 1000 space-separated words
    let words: Vec<String> = (0..1000).map(|i| format!("word{}", i)).collect();
    let query = words.join(" ");

    let start = std::time::Instant::now();
    let sanitized = sanitize_query(&query);
    let tokens = parse_boolean_query(&sanitized);
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "1000 terms query took {:?} (>1s)",
        elapsed
    );
    // Should have roughly 1000 Term tokens
    let term_count = tokens
        .iter()
        .filter(|t| matches!(t, QueryToken::Term(_)))
        .count();
    assert!(
        term_count >= 900,
        "Expected ~1000 terms, got {} terms",
        term_count
    );
}

#[test]
fn stress_query_1000_identical_terms() {
    // Same word repeated 1000 times
    let query = "test ".repeat(1000);

    let start = std::time::Instant::now();
    let sanitized = sanitize_query(&query);
    let tokens = parse_boolean_query(&sanitized);
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "1000 identical terms query took {:?} (>1s)",
        elapsed
    );

    // Verify parse_boolean_query produced expected tokens
    let parsed_term_count = tokens
        .iter()
        .filter(|t| matches!(t, QueryToken::Term(_)))
        .count();
    assert_eq!(parsed_term_count, 1000, "Parser should produce 1000 terms");
}

#[test]
fn stress_query_10k_char_single_term() {
    // 10k character single continuous string (no spaces)
    let long_term = "a".repeat(10000);

    let start = std::time::Instant::now();
    let sanitized = sanitize_query(&long_term);
    let tokens = parse_boolean_query(&sanitized);
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "10k char single term took {:?} (>1s)",
        elapsed
    );
    assert_eq!(tokens.len(), 1, "Should produce exactly one token");
    assert!(
        matches!(&tokens[0], QueryToken::Term(t) if t.len() == 10000),
        "Expected Term token"
    );
}

#[test]
fn stress_deeply_nested_parentheses() {
    // 100+ levels of nested parentheses (though parser doesn't use them,
    // they become spaces and shouldn't cause issues)
    let open_parens = "(".repeat(100);
    let close_parens = ")".repeat(100);
    let query = format!("{}test{}", open_parens, close_parens);

    let start = std::time::Instant::now();
    let sanitized = sanitize_query(&query);
    let tokens = parse_boolean_query(&sanitized);
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_millis(100),
        "Deeply nested parens took {:?} (>100ms)",
        elapsed
    );
    // Parentheses become spaces, leaving just "test"
    let term_count = tokens
        .iter()
        .filter(|t| matches!(t, QueryToken::Term(_)))
        .count();
    assert_eq!(term_count, 1, "Should have 1 term after sanitizing parens");
}

#[test]
fn stress_many_boolean_operators() {
    // 100+ boolean operators: "a AND b AND c AND ..."
    let terms: Vec<String> = (0..101).map(|i| format!("term{}", i)).collect();
    let query = terms.join(" AND ");

    let start = std::time::Instant::now();
    let tokens = parse_boolean_query(&query);
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "100+ boolean ops took {:?} (>1s)",
        elapsed
    );

    let and_count = tokens
        .iter()
        .filter(|t| matches!(t, QueryToken::And))
        .count();
    let term_count = tokens
        .iter()
        .filter(|t| matches!(t, QueryToken::Term(_)))
        .count();

    assert_eq!(and_count, 100, "Should have 100 AND operators");
    assert_eq!(term_count, 101, "Should have 101 terms");
}

#[test]
fn stress_many_or_operators() {
    // 100+ OR operators: "a OR b OR c OR ..."
    let terms: Vec<String> = (0..101).map(|i| format!("opt{}", i)).collect();
    let query = terms.join(" OR ");

    let start = std::time::Instant::now();
    let tokens = parse_boolean_query(&query);
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "100+ OR ops took {:?} (>1s)",
        elapsed
    );

    let or_count = tokens
        .iter()
        .filter(|t| matches!(t, QueryToken::Or))
        .count();
    assert_eq!(or_count, 100, "Should have 100 OR operators");
}

#[test]
fn stress_mixed_boolean_operators() {
    // Complex query with many mixed operators
    let query = "a AND b OR c NOT d AND e OR f NOT g ".repeat(50);

    let start = std::time::Instant::now();
    let tokens = parse_boolean_query(&query);
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "Mixed boolean ops took {:?} (>1s)",
        elapsed
    );
    assert!(
        !tokens.is_empty(),
        "Complex boolean query should produce tokens"
    );
}

#[test]
fn stress_memory_bounds_large_query() {
    // Verify no excessive memory allocation with large input
    // We can't easily measure memory in a unit test, but we can verify
    // the output size is reasonable relative to input.
    let large_query = "x".repeat(100000);

    let sanitized = sanitize_query(&large_query);
    let tokens = parse_boolean_query(&sanitized);

    // Sanitized output shouldn't be larger than input
    assert!(
        sanitized.len() <= large_query.len(),
        "Sanitized output should not exceed input size"
    );

    // Should produce exactly 1 token
    assert_eq!(tokens.len(), 1);
}

#[test]
fn stress_concurrent_queries() {
    use std::thread;

    let queries: Vec<String> = (0..100)
        .map(|i| format!("concurrent_query_{} test search", i))
        .collect();

    let handles: Vec<_> = queries
        .into_iter()
        .map(|query| {
            thread::spawn(move || {
                let sanitized = sanitize_query(&query);
                let tokens = parse_boolean_query(&sanitized);
                tokens.len()
            })
        })
        .collect();

    for (i, handle) in handles.into_iter().enumerate() {
        let token_len = handle.join().expect("Thread panicked");
        assert!(token_len > 0, "Query {} should produce tokens", i);
    }
}

#[test]
fn stress_many_quoted_phrases() {
    // 50 quoted phrases
    let phrases: Vec<String> = (0..50)
        .map(|i| format!("\"phrase number {}\"", i))
        .collect();
    let query = phrases.join(" AND ");

    let start = std::time::Instant::now();
    let tokens = parse_boolean_query(&query);
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "50 quoted phrases took {:?} (>1s)",
        elapsed
    );

    let phrase_count = tokens
        .iter()
        .filter(|t| matches!(t, QueryToken::Phrase(_)))
        .count();
    assert_eq!(phrase_count, 50, "Should have 50 phrases");
}

#[test]
fn stress_alternating_quotes() {
    // Alternating quoted and unquoted: "a" b "c" d "e" ...
    let parts: Vec<String> = (0..100)
        .map(|i| {
            if i % 2 == 0 {
                format!("\"word{}\"", i)
            } else {
                format!("word{}", i)
            }
        })
        .collect();
    let query = parts.join(" ");

    let start = std::time::Instant::now();
    let tokens = parse_boolean_query(&query);
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "100 alternating quotes took {:?} (>1s)",
        elapsed
    );

    let phrase_count = tokens
        .iter()
        .filter(|t| matches!(t, QueryToken::Phrase(_)))
        .count();
    let term_count = tokens
        .iter()
        .filter(|t| matches!(t, QueryToken::Term(_)))
        .count();

    assert_eq!(phrase_count, 50, "Should have 50 phrases");
    assert_eq!(term_count, 50, "Should have 50 terms");
}

#[test]
fn stress_many_wildcards() {
    // Many wildcard patterns
    let patterns: Vec<&str> = vec!["pre*", "*suf", "*sub*", "a*b", "test*", "*ing", "*tion*"];
    let query = patterns
        .iter()
        .cycle()
        .take(100)
        .cloned()
        .collect::<Vec<_>>()
        .join(" ");

    let start = std::time::Instant::now();
    let sanitized = sanitize_query(&query);
    let tokens = parse_boolean_query(&sanitized);
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "100 wildcards took {:?} (>1s)",
        elapsed
    );
    assert!(!tokens.is_empty());
}

#[test]
fn stress_query_explanation_large_query() {
    // Test QueryExplanation with a large query
    let words: Vec<String> = (0..100).map(|i| format!("term{}", i)).collect();
    let query = words.join(" ");
    let filters = SearchFilters::default();

    let start = std::time::Instant::now();
    let explanation = QueryExplanation::analyze(&query, &filters);
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "QueryExplanation for 100 terms took {:?} (>2s)",
        elapsed
    );
    assert!(
        !explanation.parsed.terms.is_empty(),
        "Should parse terms successfully"
    );
}

#[test]
fn stress_very_long_single_quoted_phrase() {
    // Single quoted phrase with many words
    let words: Vec<String> = (0..500).map(|i| format!("word{}", i)).collect();
    let phrase = format!("\"{}\"", words.join(" "));

    let start = std::time::Instant::now();
    let tokens = parse_boolean_query(&phrase);
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "500-word phrase took {:?} (>1s)",
        elapsed
    );

    let phrase_count = tokens
        .iter()
        .filter(|t| matches!(t, QueryToken::Phrase(_)))
        .count();
    assert_eq!(phrase_count, 1, "Should have exactly 1 phrase");
}

#[test]
fn stress_not_prefix_many() {
    // Many NOT prefixes: -a -b -c -d ...
    let terms: Vec<String> = (0..100).map(|i| format!("-term{}", i)).collect();
    let query = terms.join(" ");

    let start = std::time::Instant::now();
    let tokens = parse_boolean_query(&query);
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "100 NOT prefixes took {:?} (>1s)",
        elapsed
    );

    let not_count = tokens
        .iter()
        .filter(|t| matches!(t, QueryToken::Not))
        .count();
    assert_eq!(not_count, 100, "Should have 100 NOT operators");
}

#[test]
fn stress_unicode_large_cjk_query() {
    // Large CJK query (each char is alphanumeric)
    let cjk_chars = "中文日本語한국어".repeat(1000);

    let start = std::time::Instant::now();
    let sanitized = sanitize_query(&cjk_chars);
    let tokens = parse_boolean_query(&sanitized);
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "Large CJK query took {:?} (>1s)",
        elapsed
    );
    assert!(!tokens.is_empty(), "CJK query should produce tokens");
}

#[test]
fn stress_unicode_many_emoji() {
    // Query with many emoji (non-alphanumeric, become spaces)
    let emoji_query = "🚀 🔍 📝 💻 🎯 ".repeat(500);

    let start = std::time::Instant::now();
    let sanitized = sanitize_query(&emoji_query);
    let tokens = parse_boolean_query(&sanitized);
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "Emoji query took {:?} (>1s)",
        elapsed
    );
    // Emoji are stripped, leaving empty
    assert!(
        tokens.is_empty(),
        "Emoji-only query should produce no tokens"
    );
}

#[test]
fn stress_mixed_content_large() {
    // Mixed content: code, prose, symbols, unicode
    let mixed = r#"
        function test() { return x + y; }
        SELECT * FROM users WHERE id = 1;
        The quick brown fox 狐狸 jumps over lazy dog
        Error: "undefined is not a function" at line 42
        https://example.com/path?query=value&other=123
    "#
    .repeat(100);

    let start = std::time::Instant::now();
    let sanitized = sanitize_query(&mixed);
    let tokens = parse_boolean_query(&sanitized);
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "Mixed content query took {:?} (>2s)",
        elapsed
    );
    assert!(!tokens.is_empty());
}

// ==========================================================================
// Query Parser Unit Tests (br-335y) - Unicode, Special Chars, Edge Cases
// ==========================================================================

// --- Unicode queries with emoji in terms ---

#[test]
fn unicode_emoji_mixed_with_alphanumeric() {
    // Emoji surrounded by alphanumeric text
    let tokens = parse_boolean_query("rocket🚀launch");
    assert_eq!(tokens.len(), 1);
    // sanitize_query strips emoji (non-alphanumeric), so this becomes "rocket launch"
    let sanitized = sanitize_query("rocket🚀launch");
    assert_eq!(sanitized, "rocket launch");

    // Multiple emoji between words
    let sanitized2 = sanitize_query("test🔥🎯code");
    assert_eq!(sanitized2, "test  code");
}

#[test]
fn unicode_emoji_with_boolean_operators() {
    // AND/OR/NOT with queries containing emoji
    let tokens = parse_boolean_query("🚀code AND test");
    // After parsing, we should have 3 tokens (emoji becomes space/empty)
    let term_count = tokens
        .iter()
        .filter(|t| matches!(t, QueryToken::Term(_)))
        .count();
    assert!(term_count >= 1, "Should have at least one term");

    // OR with emoji
    let tokens_or = parse_boolean_query("deploy OR 🎯target");
    let has_or = tokens_or.iter().any(|t| matches!(t, QueryToken::Or));
    assert!(has_or, "Should detect OR operator");
}

#[test]
fn unicode_emoji_at_word_boundaries() {
    // Emoji at start of query
    let sanitized_start = sanitize_query("🔍search");
    assert_eq!(sanitized_start, " search");

    // Emoji at end of query
    let sanitized_end = sanitize_query("complete✅");
    assert_eq!(sanitized_end, "complete ");

    // Only emoji - becomes empty
    let sanitized_only = sanitize_query("🎉🎊🎁");
    assert!(
        sanitized_only.trim().is_empty(),
        "Emoji-only should be empty after trimming"
    );
}

// --- RTL (Right-to-Left) text: Arabic and Hebrew ---

#[test]
fn unicode_arabic_text_preserved() {
    // Arabic text should be preserved as alphanumeric
    let arabic = "مرحبا بالعالم"; // "Hello World" in Arabic
    let sanitized = sanitize_query(arabic);
    assert_eq!(
        sanitized, arabic,
        "Arabic alphanumeric chars should be preserved"
    );

    let tokens = parse_boolean_query(arabic);
    assert!(!tokens.is_empty(), "Arabic query should produce tokens");
}

#[test]
fn unicode_hebrew_text_preserved() {
    // Hebrew text should be preserved
    let hebrew = "שלום עולם"; // "Hello World" in Hebrew
    let sanitized = sanitize_query(hebrew);
    assert_eq!(
        sanitized, hebrew,
        "Hebrew alphanumeric chars should be preserved"
    );

    let tokens = parse_boolean_query(hebrew);
    assert!(!tokens.is_empty(), "Hebrew query should produce tokens");
}

#[test]
fn unicode_mixed_rtl_and_ltr() {
    // Mixed RTL (Arabic) and LTR (English) text
    let mixed = "hello مرحبا world";
    let sanitized = sanitize_query(mixed);
    assert_eq!(sanitized, mixed, "Mixed RTL/LTR should be preserved");

    let tokens = parse_boolean_query(mixed);
    let term_count = tokens
        .iter()
        .filter(|t| matches!(t, QueryToken::Term(_)))
        .count();
    assert_eq!(term_count, 3, "Should have 3 terms");
}

#[test]
fn unicode_rtl_with_boolean_operators() {
    // Hebrew with AND operator
    let hebrew_and = "שלום AND עולם";
    let tokens = parse_boolean_query(hebrew_and);
    let has_and = tokens.iter().any(|t| matches!(t, QueryToken::And));
    assert!(has_and, "Should detect AND operator in Hebrew query");

    // Arabic with NOT operator
    let arabic_not = "مرحبا NOT بالعالم";
    let tokens_not = parse_boolean_query(arabic_not);
    let has_not = tokens_not.iter().any(|t| matches!(t, QueryToken::Not));
    assert!(has_not, "Should detect NOT operator in Arabic query");
}

// --- Backslash handling ---

#[test]
fn special_chars_backslash_stripped() {
    // Backslash is not alphanumeric, so it becomes space
    let query = r"path\to\file";
    let sanitized = sanitize_query(query);
    assert_eq!(sanitized, "path to file");
}

#[test]
fn special_chars_escaped_quotes_handling() {
    // Backslash before quote - backslash stripped, quote preserved
    let query = r#"say \"hello\""#;
    let sanitized = sanitize_query(query);
    // Backslash becomes space, quotes preserved
    assert!(sanitized.contains('"'), "Quotes should be preserved");
}

#[test]
fn special_chars_windows_paths() {
    // Windows-style paths with backslashes
    let path = r"C:\Users\test\Documents";
    let sanitized = sanitize_query(path);
    assert_eq!(sanitized, "C  Users test Documents");
}

// --- Nested/Complex boolean operators ---

#[test]
fn boolean_deeply_nested_operators() {
    // Complex nested expression (parser treats this as linear)
    let query = "a AND b OR c NOT d AND e";
    let tokens = parse_boolean_query(query);

    let mut and_count = 0;
    let mut or_count = 0;
    let mut not_count = 0;
    for token in &tokens {
        match token {
            QueryToken::And => and_count += 1,
            QueryToken::Or => or_count += 1,
            QueryToken::Not => not_count += 1,
            _ => {}
        }
    }

    assert_eq!(and_count, 2, "Should have 2 AND operators");
    assert_eq!(or_count, 1, "Should have 1 OR operator");
    assert_eq!(not_count, 1, "Should have 1 NOT operator");
}

#[test]
fn boolean_consecutive_operators_degenerate() {
    // Consecutive operators: "AND AND" - second AND becomes a term
    let tokens = parse_boolean_query("foo AND AND bar");
    // "AND" as the final part of "AND AND" is treated as operator, then next "bar" is term
    let term_count = tokens
        .iter()
        .filter(|t| matches!(t, QueryToken::Term(_)))
        .count();
    assert!(
        term_count >= 2,
        "Should have at least 2 terms (foo and bar)"
    );
}

#[test]
fn boolean_operator_at_start() {
    // Operator at start of query
    let tokens = parse_boolean_query("AND foo");
    let has_and = tokens.iter().any(|t| matches!(t, QueryToken::And));
    assert!(has_and, "Leading AND should be detected");

    let tokens_or = parse_boolean_query("OR test");
    let has_or = tokens_or.iter().any(|t| matches!(t, QueryToken::Or));
    assert!(has_or, "Leading OR should be detected");
}

#[test]
fn boolean_operator_at_end() {
    // Operator at end of query
    let tokens = parse_boolean_query("foo AND");
    let has_and = tokens.iter().any(|t| matches!(t, QueryToken::And));
    assert!(has_and, "Trailing AND should be detected");
}

// --- Numeric-only queries ---

#[test]
fn numeric_query_digits_only() {
    // Query with only digits
    let tokens = parse_boolean_query("12345");
    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0], QueryToken::Term("12345".to_string()));

    let sanitized = sanitize_query("12345");
    assert_eq!(sanitized, "12345");
}

#[test]
fn numeric_query_with_text() {
    // Mixed numeric and text
    let tokens = parse_boolean_query("error 404 not found");
    let term_count = tokens
        .iter()
        .filter(|t| matches!(t, QueryToken::Term(_)))
        .count();
    // "404", "error", "found" are terms, "not" is NOT operator
    assert!(term_count >= 3, "Should have at least 3 terms");
}

#[test]
fn numeric_versions_with_dots() {
    // Version numbers like "1.2.3"
    let sanitized = sanitize_query("version 1.2.3");
    assert_eq!(sanitized, "version 1 2 3"); // dots become spaces
}

// --- Tab and newline handling ---

#[test]
fn whitespace_tabs_treated_as_separators() {
    let tokens = parse_boolean_query("foo\tbar\tbaz");
    let term_count = tokens
        .iter()
        .filter(|t| matches!(t, QueryToken::Term(_)))
        .count();
    assert_eq!(term_count, 3, "Tabs should separate terms");
}

#[test]
fn whitespace_newlines_treated_as_separators() {
    let tokens = parse_boolean_query("foo\nbar\nbaz");
    let term_count = tokens
        .iter()
        .filter(|t| matches!(t, QueryToken::Term(_)))
        .count();
    assert_eq!(term_count, 3, "Newlines should separate terms");
}

#[test]
fn whitespace_mixed_types() {
    let tokens = parse_boolean_query("a \t b \n c   d");
    let term_count = tokens
        .iter()
        .filter(|t| matches!(t, QueryToken::Term(_)))
        .count();
    assert_eq!(term_count, 4, "Mixed whitespace should separate properly");
}

// --- Very long single terms (no spaces) ---

#[test]
fn stress_very_long_single_term() {
    // Single term with 10K characters (no spaces)
    let long_term = "a".repeat(10_000);

    let start = std::time::Instant::now();
    let tokens = parse_boolean_query(&long_term);
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "10K char term took {:?} (>1s)",
        elapsed
    );
    assert_eq!(tokens.len(), 1);
    assert!(
        matches!(tokens.first(), Some(QueryToken::Term(t)) if t.len() == 10_000),
        "Expected 10K Term token, got {tokens:?}"
    );
}

#[test]
fn stress_very_long_term_with_wildcard() {
    // Long term with wildcard suffix
    let long_pattern = format!("{}*", "prefix".repeat(1000));

    let start = std::time::Instant::now();
    let sanitized = sanitize_query(&long_pattern);
    let pattern = WildcardPattern::parse(&sanitized);
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "Long wildcard pattern took {:?} (>1s)",
        elapsed
    );
    assert!(
        matches!(pattern, WildcardPattern::Prefix(_)),
        "Should parse as prefix pattern"
    );
}

// --- QueryExplanation edge cases ---

#[test]
fn query_explanation_empty_query() {
    let explanation = QueryExplanation::analyze("", &SearchFilters::default());
    assert_eq!(explanation.query_type, QueryType::Empty);
}

#[test]
fn search_mode_default_is_hybrid_preferred() {
    assert_eq!(SearchMode::default(), SearchMode::Hybrid);
}

#[test]
fn query_explanation_whitespace_only_query() {
    let explanation = QueryExplanation::analyze("   \t\n  ", &SearchFilters::default());
    assert_eq!(explanation.query_type, QueryType::Empty);
}

#[test]
fn query_explanation_unicode_query() {
    let explanation = QueryExplanation::analyze("日本語 search", &SearchFilters::default());
    // Should classify as Simple (no operators, multiple terms = implicit AND)
    assert!(!explanation.parsed.terms.is_empty());
}

// --- WildcardPattern edge cases ---

#[test]
fn wildcard_pattern_internal_asterisk() {
    // Internal wildcard: f*o
    let pattern = WildcardPattern::parse("f*o");
    assert!(
        matches!(pattern, WildcardPattern::Complex(_)),
        "Internal asterisk should be Complex"
    );
}

#[test]
fn wildcard_pattern_multiple_internal_asterisks() {
    // Multiple internal wildcards: a*b*c
    let pattern = WildcardPattern::parse("a*b*c");
    assert!(
        matches!(pattern, WildcardPattern::Complex(_)),
        "Multiple internal asterisks should be Complex"
    );
}

#[test]
fn wildcard_pattern_regex_escapes_special_chars() {
    // Pattern with regex-special characters
    let pattern = WildcardPattern::parse("*foo.bar*");
    if let Some(regex) = pattern.to_regex() {
        assert!(
            regex.contains("\\."),
            "Dot should be escaped in regex: {}",
            regex
        );
    }
}

#[test]
fn wildcard_pattern_complex_regex_generation() {
    let pattern = WildcardPattern::parse("f*o*o");
    if let Some(regex) = pattern.to_regex() {
        // Should handle internal wildcards
        assert!(
            regex.contains(".*"),
            "Should have .* for internal wildcards: {}",
            regex
        );
    }
}

#[test]
fn test_transpile_to_fts5() {
    // Simple terms
    assert_eq!(
        transpile_to_fts5("foo bar"),
        Some("foo AND bar".to_string())
    );

    // Boolean operators
    assert_eq!(
        transpile_to_fts5("foo AND bar"),
        Some("foo AND bar".to_string())
    );
    assert_eq!(
        transpile_to_fts5("foo OR bar"),
        Some("foo OR bar".to_string())
    );
    assert_eq!(transpile_to_fts5("OR foo"), Some("foo".to_string()));
    assert_eq!(transpile_to_fts5("NOT foo"), None);

    // Standard precedence (2l1b0.52): AND binds tighter than OR, and the
    // result is an OR of explicitly parenthesized AND-groups. The legacy
    // grammar read "A AND B OR C" as "A AND (B OR C)".
    assert_eq!(
        transpile_to_fts5("A AND B OR C"),
        Some("(A AND B) OR C".to_string())
    );
    assert_eq!(
        transpile_to_fts5("A OR B AND C"),
        Some("A OR (B AND C)".to_string())
    );
    assert_eq!(
        transpile_to_fts5("A OR B OR C"),
        Some("A OR B OR C".to_string())
    );

    // An implicit conjunction binds like an explicit AND.
    assert_eq!(
        transpile_to_fts5("A OR B C"),
        Some("A OR (B AND C)".to_string())
    );
    assert_eq!(
        transpile_to_fts5("A OR B C OR D"),
        Some("A OR (B AND C) OR D".to_string())
    );
    // Negation is parity-based.
    assert_eq!(
        transpile_to_fts5("A NOT NOT B"),
        Some("A AND B".to_string())
    );

    // Phrases
    assert_eq!(
        transpile_to_fts5("\"foo bar\""),
        Some("\"foo bar\"".to_string())
    );

    // Wildcards (allowed trailing)
    assert_eq!(transpile_to_fts5("foo*"), Some("foo*".to_string()));

    // Unsupported wildcards (leading/internal)
    assert_eq!(transpile_to_fts5("*foo"), None);
    assert_eq!(transpile_to_fts5("f*o"), None);

    // SQLite FTS5's porter tokenizer splits punctuation into separate
    // fragments, so fallback queries must do the same.
    assert_eq!(
        transpile_to_fts5("foo-bar"),
        Some("(foo AND bar)".to_string())
    );
    assert_eq!(
        transpile_to_fts5("foo-bar*"),
        Some("(foo AND bar*)".to_string())
    );
    assert_eq!(
        transpile_to_fts5("br-123.jsonl"),
        Some("(br AND 123 AND jsonl)".to_string())
    );
    assert_eq!(
        transpile_to_fts5("br-123.json*"),
        Some("(br AND 123 AND json*)".to_string())
    );

    // Leading unary-NOT forms are not valid FTS5 queries.
    assert_eq!(transpile_to_fts5("NOT A OR B"), None);
}

#[test]
fn semantic_doc_id_roundtrip_from_query() {
    let hash_hex = "00".repeat(32);
    let doc_id = format!("m|42|2|3|7|11|1|1700000000000|{hash_hex}");
    let parsed = parse_semantic_doc_id(&doc_id).expect("roundtrip parse");
    assert_eq!(parsed.message_id, 42);
    assert_eq!(parsed.chunk_idx, 2);
    assert_eq!(parsed.agent_id, 3);
    assert_eq!(parsed.workspace_id, 7);
    assert_eq!(parsed.source_id, 11);
    assert_eq!(parsed.role, 1);
    assert_eq!(parsed.created_at_ms, 1_700_000_000_000);
}

#[test]
fn semantic_filter_applies_all_constraints() {
    use frankensearch::core::filter::SearchFilter;

    let filter = SemanticFilter {
        agents: Some(HashSet::from([3])),
        workspaces: Some(HashSet::from([7])),
        sources: Some(HashSet::from([11])),
        roles: Some(HashSet::from([1])),
        created_from: Some(1_700_000_000_000),
        created_to: Some(1_700_000_000_100),
    };

    assert!(filter.matches("m|42|2|3|7|11|1|1700000000001", None));
    assert!(!filter.matches("m|42|2|99|7|11|1|1700000000001", None));
    assert!(!filter.matches("m|42|2|3|7|11|1|1699999999999", None));
    assert!(!filter.matches("not-a-doc-id", None));
}

#[test]
fn fs_semantic_index_runs_filtered_search() -> Result<()> {
    let temp = TempDir::new()?;
    let index_path = crate::search::vector_index::vector_index_path(temp.path(), "embed-fast");
    if let Some(parent) = index_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let hash_a = "00".repeat(32);
    let hash_b = "11".repeat(32);
    let doc_a = format!("m|101|0|1|10|100|1|1700000000001|{hash_a}");
    let doc_b = format!("m|202|0|2|20|200|1|1700000000002|{hash_b}");

    let mut writer = VectorIndex::create_with_revision(
        &index_path,
        "embed-fast",
        "rev-1",
        2,
        frankensearch::index::Quantization::F16,
    )
    .map_err(|err| anyhow!("create fsvi index failed: {err}"))?;
    writer
        .write_record(&doc_a, &[1.0, 0.0])
        .map_err(|err| anyhow!("write_record failed: {err}"))?;
    writer
        .write_record(&doc_b, &[0.0, 1.0])
        .map_err(|err| anyhow!("write_record failed: {err}"))?;
    writer
        .finish()
        .map_err(|err| anyhow!("finish fsvi index failed: {err}"))?;

    let fs_index =
        VectorIndex::open(&index_path).map_err(|err| anyhow!("open fsvi failed: {err}"))?;
    let filter = SemanticFilter {
        agents: Some(HashSet::from([1])),
        workspaces: None,
        sources: None,
        roles: None,
        created_from: None,
        created_to: None,
    };
    let fs_filter = semantic_filter_as_search_filter(&filter).expect("expected active filter");
    let hits = fs_index
        .search_top_k(&[1.0, 0.0], 5, Some(fs_filter))
        .map_err(|err| anyhow!("frankensearch search failed: {err}"))?;
    assert_eq!(hits.len(), 1);
    let parsed = parse_semantic_doc_id(&hits[0].doc_id).expect("parse bridged doc_id");
    assert_eq!(parsed.message_id, 101);
    assert_eq!(parsed.agent_id, 1);
    Ok(())
}

// Regression guard for bead coding_agent_session_search-q6xf9
// (`cass search --fields minimal` silently returned zero hits even when
// matches existed). Root cause: the dedup pass called `hit_is_noise`,
// which fell through to `is_search_noise_text("")` when both `content`
// and `snippet` were stripped by the field_mask — treating every
// projection-only hit as tool/acknowledgement noise and dropping it.
//
// Fix: when both fields are empty because the caller explicitly
// requested a minimal projection, we cannot classify noise from text
// alone. Default to "not noise" and let the hit through so downstream
// field filtering emits the requested subset.
#[test]
fn hit_is_noise_returns_false_when_content_and_snippet_both_empty() {
    let hit = SearchHit {
        title: String::new(),
        snippet: String::new(),
        content: String::new(),
        content_hash: 0,
        conversation_id: Some(1),
        score: 1.0,
        source_path: "/tmp/session.jsonl".to_string(),
        agent: "codex".to_string(),
        workspace: String::new(),
        workspace_original: None,
        created_at: Some(1700000000000),
        line_number: Some(1),
        match_type: MatchType::Exact,
        source_id: "local".to_string(),
        origin_kind: "local".to_string(),
        origin_host: None,
    };

    // Query text doesn't matter — the point is that a hit stripped of
    // content+snippet by --fields minimal must survive the noise filter
    // so `cass search --fields minimal` returns the projection.
    assert!(
        !hit_is_noise(&hit, "anything"),
        "hit with empty content AND snippet (projection-only) must NOT be classified as noise"
    );
    assert!(
        !hit_is_noise(&hit, ""),
        "noise classifier must not treat an empty-query projection-only hit as noise"
    );
}

// Complementary guard: make sure the noise filter still flags legitimate
// empty rows (no content_hash, etc.) when the content is actually empty
// because the underlying message was empty — we don't want this fix to
// re-introduce tool-ack noise into projection-full outputs.
#[test]
fn hit_is_noise_still_drops_tool_acknowledgement_when_content_present() {
    let hit = SearchHit {
        title: String::new(),
        snippet: String::new(),
        content: "ok".to_string(),
        content_hash: 0,
        conversation_id: Some(1),
        score: 1.0,
        source_path: "/tmp/session.jsonl".to_string(),
        agent: "codex".to_string(),
        workspace: String::new(),
        workspace_original: None,
        created_at: Some(1700000000000),
        line_number: Some(1),
        match_type: MatchType::Exact,
        source_id: "local".to_string(),
        origin_kind: "local".to_string(),
        origin_host: None,
    };

    assert!(
        hit_is_noise(&hit, ""),
        "bare tool-ack 'ok' with content present should still be dropped as noise"
    );
}
