use pulldown_cmark::CowStr;

use super::render_group::{
    MAX_VISIBLE_BADGES, get_tool_lucide_icon, render_message_group, render_single_tool_badge,
    render_tool_badges_with_overflow,
};
use super::render_message::{
    format_json_or_raw, format_role_display, format_timestamp, render_tool_badge,
    sanitize_markdown_dest_url, truncate_to_char_boundary,
};
use super::*;

#[test]
fn test_render_error_display_strings() {
    assert_eq!(
        RenderError::InvalidMessage("missing role".to_string()).to_string(),
        "invalid message: missing role"
    );
    assert_eq!(
        RenderError::ParseError("bad markdown".to_string()).to_string(),
        "parse error: bad markdown"
    );
}

fn test_message(role: &str, content: &str) -> Message {
    Message {
        role: role.to_string(),
        content: content.to_string(),
        timestamp: None,
        tool_call: None,
        index: None,
        author: None,
    }
}

#[test]
fn test_render_message_user() {
    let msg = test_message("user", "Hello, world!");
    let opts = RenderOptions::default();
    let html = render_message(&msg, &opts).unwrap();

    assert!(html.contains("message-user"));
    assert!(html.contains("Hello, world!"));
    assert!(html.contains("lucide-icon")); // SVG Lucide icon
    assert!(html.contains("M19 21v-2")); // User icon path
}

#[test]
fn test_render_message_with_code() {
    let msg = test_message("assistant", "Here's code:\n```rust\nfn main() {}\n```");
    let opts = RenderOptions {
        syntax_highlighting: true,
        ..Default::default()
    };
    let html = render_message(&msg, &opts).unwrap();

    assert!(html.contains("<pre>"));
    assert!(html.contains("language-rust"));
    assert!(html.contains("fn main()"));
    assert!(html.contains("lucide-icon")); // SVG Lucide icon (bot)
}

#[test]
fn test_url_with_query_params_not_double_escaped() {
    // Test that URLs with & in query params are correctly escaped once, not twice.
    // The render_content function HTML-escapes first, then render_links processes.
    // If render_links re-escapes, &amp; becomes &amp;amp; (broken).
    let msg = test_message("user", "Visit https://example.com?a=1&b=2 for info");
    let html = render_message(&msg, &RenderOptions::default()).unwrap();

    // Should contain &amp; (single escape), NOT &amp;amp; (double escape)
    assert!(
        html.contains("https://example.com?a=1&amp;b=2"),
        "URL should have single-escaped ampersand. HTML: {}",
        html
    );
    assert!(
        !html.contains("&amp;amp;"),
        "URL should NOT be double-escaped. HTML: {}",
        html
    );
}

#[test]
fn test_html_escape_in_content() {
    let msg = test_message("user", "<script>alert('xss')</script>");
    let html = render_message(&msg, &RenderOptions::default()).unwrap();
    assert!(!html.contains("<script>"));
    assert!(html.contains("&lt;script&gt;"));
}

#[test]
fn test_javascript_url_sanitized_in_markdown_links() {
    let msg = test_message("user", "[click](javascript:alert(1))");
    let html = render_message(&msg, &RenderOptions::default()).unwrap();
    assert!(
        !html.contains("javascript:"),
        "javascript: URL should be sanitized, got: {}",
        html
    );
    assert!(html.contains("click")); // link text preserved
}

#[test]
fn test_vbscript_and_data_urls_sanitized() {
    let msg = test_message("user", "[a](vbscript:foo) [b](data:text/html,<script>)");
    let html = render_message(&msg, &RenderOptions::default()).unwrap();
    assert!(
        !html.contains("vbscript:"),
        "vbscript: URL should be sanitized, got: {}",
        html
    );
    assert!(
        !html.contains("data:text"),
        "data: URL should be sanitized, got: {}",
        html
    );
}

#[test]
fn test_unsafe_markdown_image_urls_sanitized() {
    let msg = test_message(
        "user",
        "![a](javascript:alert(1)) ![b](data:image/svg+xml,<svg/onload=alert(1)>)",
    );
    let html = render_message(&msg, &RenderOptions::default()).unwrap();
    assert!(
        !html.contains("javascript:"),
        "unsafe image URL should be sanitized, got: {}",
        html
    );
    assert!(
        !html.contains("data:image"),
        "data: image URL should be sanitized, got: {}",
        html
    );
    assert!(
        html.contains("<img"),
        "image markup should still render, got: {}",
        html
    );
    assert!(
        html.contains("src=\"#\""),
        "unsafe image src should be rewritten, got: {}",
        html
    );
}

#[test]
fn test_normal_markdown_image_urls_not_affected() {
    let msg = test_message("user", "![logo](https://example.com/logo.png)");
    let html = render_message(&msg, &RenderOptions::default()).unwrap();
    assert!(
        html.contains("https://example.com/logo.png"),
        "normal image URLs should be preserved, got: {}",
        html
    );
}

#[test]
fn test_javascript_url_case_insensitive() {
    let msg = test_message("user", "[x](JaVaScRiPt:alert(1))");
    let html = render_message(&msg, &RenderOptions::default()).unwrap();
    assert!(
        !html.contains("javascript:"),
        "case-variant javascript: should be sanitized, got: {}",
        html
    );
    assert!(
        !html.contains("JaVaScRiPt:"),
        "case-variant javascript: should be sanitized, got: {}",
        html
    );
}

#[test]
fn test_sanitize_markdown_dest_url_blocks_control_character_variants() {
    assert!(
        sanitize_markdown_dest_url("java\tscript:alert(1)".into()) == CowStr::from("#"),
        "tab-obfuscated javascript: URL should be rejected"
    );
    assert!(
        sanitize_markdown_dest_url("\u{0000}data:image/svg+xml,<svg/onload=1>".into())
            == CowStr::from("#"),
        "control-character data: URL should be rejected"
    );
}

#[test]
fn test_normal_urls_not_affected() {
    let msg = test_message("user", "[link](https://example.com)");
    let html = render_message(&msg, &RenderOptions::default()).unwrap();
    assert!(
        html.contains("https://example.com"),
        "normal URLs should be preserved, got: {}",
        html
    );
}

#[test]
fn test_format_role_display_escapes_unknown_roles() {
    let display = format_role_display("<img src=x onerror=alert(1)>");
    assert!(
        !display.contains("<img"),
        "unknown role should be HTML-escaped, got: {}",
        display
    );
    assert!(display.contains("&lt;img"));
}

#[test]
fn test_agent_css_class() {
    assert_eq!(agent_css_class("claude_code"), "agent-claude");
    assert_eq!(agent_css_class("codex"), "agent-codex");
    assert_eq!(agent_css_class("cursor"), "agent-cursor");
    assert_eq!(agent_css_class("gemini"), "agent-gemini");
    assert_eq!(agent_css_class("opencode"), "agent-codex");
    assert_eq!(agent_css_class("copilot-cli"), "agent-copilot");
    assert_eq!(agent_css_class("qwen"), "agent-codex");
    for alias in ["omp", "Oh My Pi", "oh-my-pi", "oh_my_pi", "ohmypi"] {
        assert_eq!(agent_css_class(alias), "agent-aider", "OMP alias {alias:?}");
    }
    assert_eq!(agent_css_class("oh_my_pipeline"), "agent-default");
    assert_eq!(agent_css_class("hermes"), "agent-hermes");
    assert_eq!(agent_css_class("goose"), "agent-goose");
    assert_eq!(agent_css_class("shelley"), "agent-aider");
    assert_eq!(agent_css_class("pi_durable"), "agent-aider");
    for alias in ["grok", "grok_bot", "grok-bot"] {
        assert_eq!(agent_css_class(alias), "agent-grok", "Grok alias {alias:?}");
    }
    assert_eq!(agent_css_class("unknown"), "agent-default");
}

#[test]
fn test_agent_display_name() {
    assert_eq!(agent_display_name("claude_code"), "Claude");
    assert_eq!(agent_display_name("codex"), "Codex");
    assert_eq!(agent_display_name("github_copilot"), "GitHub Copilot");
    assert_eq!(agent_display_name("copilot-cli"), "GitHub Copilot CLI");
    assert_eq!(agent_display_name("opencode"), "OpenCode");
    assert_eq!(agent_display_name("pi_agent"), "Pi Agent");
    assert_eq!(agent_display_name("pi_durable"), "Pi Durable");
    for alias in ["omp", "Oh My Pi", "oh-my-pi", "oh_my_pi", "ohmypi"] {
        assert_eq!(agent_display_name(alias), "Oh My Pi", "OMP alias {alias:?}");
    }
    assert_eq!(agent_display_name("oh_my_pipeline"), "AI Assistant");
    assert_eq!(agent_display_name("factory"), "Factory");
    assert_eq!(agent_display_name("openclaw"), "OpenClaw");
    assert_eq!(agent_display_name("clawdbot"), "ClawdBot");
    assert_eq!(agent_display_name("vibe"), "Vibe");
    assert_eq!(agent_display_name("crush"), "Crush");
    assert_eq!(agent_display_name("kimi"), "Kimi");
    assert_eq!(agent_display_name("qwen"), "Qwen");
    assert_eq!(agent_display_name("unknown"), "AI Assistant");
    assert_eq!(agent_display_name("shelley"), "Shelley");
    assert_eq!(agent_display_name("grok_bot"), "Grok Bot");
    assert_eq!(agent_display_name("grok-bot"), "Grok Bot");
}

#[test]
fn pi_agent_slug_normalization_composes_with_renderer_identity() {
    for alias in ["pi_agent", "pi-agent", "piagent", "pi"] {
        let slug = crate::html_export::agent_slug(alias);
        assert_eq!(slug, "pi_agent", "Pi Agent alias {alias:?}");
        assert_eq!(agent_css_class(&slug), "agent-aider");
        assert_eq!(agent_display_name(&slug), "Pi Agent");
    }
}

#[test]
fn connector_registry_slugs_have_specific_html_identity() {
    for (slug, _) in crate::connectors::get_connector_factories() {
        assert_ne!(
            agent_css_class(slug),
            "agent-default",
            "registered connector {slug} should not use default HTML export styling"
        );
        assert_ne!(
            agent_display_name(slug),
            "AI Assistant",
            "registered connector {slug} should have a specific HTML export display name"
        );
    }
}

#[test]
fn test_tool_status_rendering() {
    let msg = Message {
        role: "tool".to_string(),
        content: "Tool executed".to_string(),
        timestamp: None,
        tool_call: Some(ToolCall {
            name: "Bash".to_string(),
            input: r#"{"command": "ls -la"}"#.to_string(),
            output: Some("file1.txt\nfile2.txt".to_string()),
            status: Some(ToolStatus::Success),
            correlation_id: None,
        }),
        index: None,
        author: None,
    };

    let html = render_message(&msg, &RenderOptions::default()).unwrap();
    assert!(html.contains("tool-status-success"));
    assert!(html.contains("lucide-icon")); // SVG icon
    assert!(html.contains("M20 6 9 17l-5-5")); // Check icon path (success)
    assert!(html.contains("polyline points=\"4 17 10 11 4 5\"")); // Terminal icon path (bash)
}

#[test]
fn test_message_with_index() {
    let msg = Message {
        role: "user".to_string(),
        content: "Test message".to_string(),
        timestamp: None,
        tool_call: None,
        index: Some(42),
        author: None,
    };

    let html = render_message(&msg, &RenderOptions::default()).unwrap();
    assert!(html.contains(r#"id="msg-42""#));
}

#[test]
fn test_message_with_author() {
    let msg = Message {
        role: "user".to_string(),
        content: "Test message".to_string(),
        timestamp: None,
        tool_call: None,
        index: None,
        author: Some("Alice".to_string()),
    };

    let html = render_message(&msg, &RenderOptions::default()).unwrap();
    assert!(html.contains("Alice"));
}

#[test]
fn test_format_json_or_raw() {
    // Valid JSON gets pretty printed
    let json_input = r#"{"key":"value"}"#;
    let formatted = format_json_or_raw(json_input);
    assert!(formatted.contains('\n')); // Pretty printed has newlines

    // Invalid JSON passes through unchanged
    let raw_input = "not json at all";
    let formatted = format_json_or_raw(raw_input);
    assert_eq!(formatted, raw_input);
}

#[test]
fn test_long_message_collapse() {
    let long_content = "x".repeat(2000);
    let msg = test_message("user", &long_content);
    let opts = RenderOptions {
        collapse_threshold: 1000,
        ..Default::default()
    };

    let html = render_message(&msg, &opts).unwrap();
    assert!(html.contains("<details"));
    assert!(html.contains("Click to expand"));
}

#[test]
fn test_tool_icons_for_different_tools() {
    // Check that different tools get appropriate Lucide SVG icons
    let tools_and_svg_markers = vec![
        ("Read", "M15 2H6a2 2 0 0 0-2 2v16"), // FileText icon path
        ("Write", "M21.174 6.812"),           // Pencil icon path
        ("Bash", "polyline points=\"4 17 10 11 4 5\""), // Terminal icon
        ("Grep", "circle cx=\"11\" cy=\"11\" r=\"8\""), // Search icon
        ("WebFetch", "circle cx=\"12\" cy=\"12\" r=\"10\""), // Globe icon
    ];

    for (tool_name, svg_marker) in tools_and_svg_markers {
        let tc = ToolCall {
            name: tool_name.to_string(),
            input: "{}".to_string(),
            output: None,
            status: None,
            correlation_id: None,
        };
        let html = render_tool_badge(&tc, &RenderOptions::default());
        assert!(
            html.contains("lucide-icon"),
            "Tool {} should have lucide-icon class",
            tool_name
        );
        assert!(
            html.contains(svg_marker),
            "Tool {} should have SVG marker '{}', got: {}",
            tool_name,
            svg_marker,
            html
        );
    }
}

// ========================================================================
// UTF-8 boundary safety tests
// ========================================================================

#[test]
fn test_truncate_to_char_boundary() {
    // ASCII string
    assert_eq!(truncate_to_char_boundary("hello", 3), 3);
    assert_eq!(truncate_to_char_boundary("hello", 10), 5);

    // UTF-8 multi-byte characters
    // "日本語" = 3 chars, 9 bytes (each char is 3 bytes)
    let japanese = "日本語";
    assert_eq!(japanese.len(), 9);
    // Truncating at byte 4 should back up to byte 3 (end of first char)
    assert_eq!(truncate_to_char_boundary(japanese, 4), 3);
    // Truncating at byte 6 should stay at 6 (end of second char)
    assert_eq!(truncate_to_char_boundary(japanese, 6), 6);
}

#[test]
fn test_long_message_collapse_utf8_safe() {
    // Create a message with multi-byte UTF-8 content that would panic if sliced incorrectly
    let content_with_emoji = "This is a message with emoji 🎉🎊🎈 ".repeat(50);
    let msg = test_message("user", &content_with_emoji);
    let opts = RenderOptions {
        collapse_threshold: 100,
        ..Default::default()
    };

    // Should not panic even though the emoji may be at the slice boundary
    let html = render_message(&msg, &opts).unwrap();
    assert!(html.contains("<details"));
    // The preview should be valid UTF-8 (this would fail if we sliced incorrectly)
    assert!(!html.is_empty());
}

#[test]
fn test_collapse_threshold_uses_character_count() {
    // "é" is 2 bytes in UTF-8, so this string has 60 chars but 120 bytes.
    let msg = test_message("user", &"é".repeat(60));
    let opts = RenderOptions {
        collapse_threshold: 100,
        ..Default::default()
    };

    // Should NOT collapse because threshold is in characters, not bytes.
    let html = render_message(&msg, &opts).unwrap();
    assert!(
        !html.contains("<details"),
        "message should not collapse when char count is below threshold"
    );
}

#[test]
fn test_tool_output_with_unicode_renders_safely() {
    // Create a very long tool output with multi-byte chars
    let long_output_with_unicode = "结果: ".repeat(5000); // Chinese characters

    let msg = Message {
        role: "tool".to_string(),
        content: "Tool result".to_string(),
        timestamp: None,
        tool_call: Some(ToolCall {
            name: "Test".to_string(),
            input: "{}".to_string(),
            output: Some(long_output_with_unicode),
            status: Some(ToolStatus::Success),
            correlation_id: None,
        }),
        index: None,
        author: None,
    };

    // Should not panic with long multi-byte output
    let html = render_message(&msg, &RenderOptions::default()).unwrap();
    // Verify we have a tool badge with full content in popover
    assert!(html.contains("tool-badge"));
    assert!(html.contains("tool-popover-section"));
    // Full content is preserved (no truncation) — popovers scroll
    assert!(html.contains("结果"));
}

#[test]
fn test_format_timestamp_utf8_safe() {
    // Malformed timestamp with multi-byte chars (edge case)
    let weird_ts = "2026-01-25T12:30:00日本語";
    let formatted = format_timestamp(weird_ts);
    // Should not panic and should produce valid output
    assert!(!formatted.is_empty());
}

// ========================================================================
// MessageGroup Rendering Tests
// ========================================================================

fn test_tool_call(name: &str) -> ToolCall {
    ToolCall {
        name: name.to_string(),
        input: r#"{"test": "input"}"#.to_string(),
        output: Some("test output".to_string()),
        status: Some(ToolStatus::Success),
        correlation_id: None,
    }
}

fn test_tool_call_with_result(name: &str, status: ToolStatus) -> ToolCallWithResult {
    let call = test_tool_call(name);
    let result = ToolResult::new(name, "test output", status);
    ToolCallWithResult::new(call).with_result(result)
}

#[test]
fn test_render_message_group_user() {
    let msg = test_message("user", "Hello, assistant!");
    let group = MessageGroup::user(msg);
    let opts = RenderOptions::default();
    let html = render_message_group(&group, 0, &opts).unwrap();

    assert!(html.contains("message-user"));
    assert!(html.contains("Hello, assistant!"));
    assert!(html.contains(r#"role="article""#));
    assert!(html.contains("lucide-icon")); // Has role icon
}

#[test]
fn test_render_message_group_assistant_with_tools() {
    let msg = test_message("assistant", "Let me read that file.");
    let mut group = MessageGroup::assistant(msg);

    // Add tool calls
    group.add_tool_call(test_tool_call("Read"), Some("toolu_abc123".to_string()));
    assert!(
        group.add_tool_result(
            ToolResult::new("Read", "file contents here", ToolStatus::Success)
                .with_correlation_id("toolu_abc123"),
        )
    );

    let opts = RenderOptions::default();
    let html = render_message_group(&group, 0, &opts).unwrap();

    assert!(html.contains("message-assistant"));
    assert!(html.contains("Let me read that file."));
    assert!(html.contains("tool-badge")); // Has tool badge
    assert!(html.contains("Read")); // Tool name in badge
    assert!(html.contains(r#"role="group""#)); // Accessibility for tool container
    assert!(html.contains("aria-label")); // Accessible
}

#[test]
fn test_tool_result_uses_exact_correlation_before_name_fallback() {
    let msg = test_message("assistant", "Reading two files.");
    let mut group = MessageGroup::assistant(msg);
    group.add_tool_call(test_tool_call("Read"), Some("toolu_first".to_string()));
    group.add_tool_call(test_tool_call("Read"), Some("toolu_second".to_string()));

    assert!(
        group.add_tool_result(
            ToolResult::new("Read", "second file contents", ToolStatus::Success)
                .with_correlation_id("toolu_second"),
        )
    );

    assert!(
        group.tool_calls[0].result.is_none(),
        "correlated result must not attach to the first same-name tool call"
    );
    assert_eq!(
        group.tool_calls[1]
            .result
            .as_ref()
            .map(|result| result.content.as_str()),
        Some("second file contents")
    );
}

#[test]
fn test_mismatched_correlated_tool_result_does_not_fall_back_by_name() {
    let msg = test_message("assistant", "Reading a file.");
    let mut group = MessageGroup::assistant(msg);
    group.add_tool_call(test_tool_call("Read"), Some("toolu_expected".to_string()));

    assert!(
        !group.add_tool_result(
            ToolResult::new("Read", "wrong file contents", ToolStatus::Success)
                .with_correlation_id("toolu_other"),
        )
    );

    assert!(
        group.tool_calls[0].result.is_none(),
        "a result with an explicit mismatched provider ID must not attach by name"
    );
}

#[test]
fn test_tool_call_with_result_preserves_call_correlation_id() {
    let mut call = test_tool_call("Read");
    call.correlation_id = Some("toolu_from_call".to_string());

    let tool = ToolCallWithResult::new(call);

    assert_eq!(tool.correlation_id.as_deref(), Some("toolu_from_call"));
}

#[test]
fn test_render_message_group_multiple_tools() {
    let msg = test_message("assistant", "I'll run several commands.");
    let mut group = MessageGroup::assistant(msg);

    // Add multiple tool calls
    let tools = ["Bash", "Read", "Write"];
    for (i, name) in tools.iter().enumerate() {
        group.add_tool_call(test_tool_call(name), Some(format!("toolu_{}", i)));
    }

    let opts = RenderOptions::default();
    let html = render_message_group(&group, 0, &opts).unwrap();

    // Should have all tool badges
    for tool_name in tools {
        assert!(
            html.contains(tool_name),
            "Should contain badge for {}",
            tool_name
        );
    }
    assert!(html.contains("with 3 tool calls")); // Aria label mentions count
}

#[test]
fn test_render_tool_badges_overflow() {
    // Create more tools than MAX_VISIBLE_BADGES
    let tool_names = [
        "Read", "Write", "Bash", "Glob", "Grep", "WebFetch", "Task", "Search",
    ];
    let tools: Vec<ToolCallWithResult> = tool_names
        .iter()
        .map(|name| test_tool_call_with_result(name, ToolStatus::Success))
        .collect();

    let opts = RenderOptions::default();
    let (html, overflow) = render_tool_badges_with_overflow(&tools, &opts);

    // Should render all badges and hide extras until the overflow control expands them.
    assert!(overflow > 0, "Should have overflow");
    assert_eq!(overflow, tools.len() - MAX_VISIBLE_BADGES);
    for name in tool_names {
        assert!(html.contains(name), "overflow HTML should retain {name}");
    }
    assert_eq!(html.matches("tool-overflow-extra").count(), overflow);

    // Should have overflow badge
    assert!(html.contains("tool-overflow"));
    assert!(html.contains(&format!("+{}", overflow)));
}

#[test]
fn test_render_tool_badges_no_overflow() {
    let tools: Vec<ToolCallWithResult> = ["Read", "Write", "Bash"]
        .iter()
        .map(|name| test_tool_call_with_result(name, ToolStatus::Success))
        .collect();

    let opts = RenderOptions::default();
    let (html, overflow) = render_tool_badges_with_overflow(&tools, &opts);

    assert_eq!(overflow, 0);
    assert!(!html.contains("tool-overflow"));
    assert!(html.contains("Read"));
    assert!(html.contains("Write"));
    assert!(html.contains("Bash"));
}

#[test]
fn test_render_single_tool_badge_success() {
    let tool = test_tool_call_with_result("Bash", ToolStatus::Success);
    let html = render_single_tool_badge(&tool, false);

    assert!(html.contains("tool-badge"));
    assert!(html.contains("tool-status-success"));
    assert!(html.contains("Bash"));
    assert!(html.contains(r#"aria-label="Bash: success""#));
    assert!(html.contains("lucide-icon")); // Has SVG icon
}

#[test]
fn test_render_single_tool_badge_error() {
    let tool = test_tool_call_with_result("Bash", ToolStatus::Error);
    let html = render_single_tool_badge(&tool, false);

    assert!(html.contains("tool-status-error"));
    assert!(html.contains(r#"aria-label="Bash: error""#));
}

#[test]
fn test_render_single_tool_badge_with_inline_popover() {
    let tool = test_tool_call_with_result("Read", ToolStatus::Success);
    let html = render_single_tool_badge(&tool, false);

    assert!(html.contains(r#"data-tool-name="Read""#));
    assert!(html.contains("tool-popover"));
    assert!(html.contains("tool-popover-label"));
}

#[test]
fn test_render_single_tool_badge_can_mark_overflow_extra() {
    let tool = test_tool_call_with_result("Search", ToolStatus::Success);
    let html = render_single_tool_badge(&tool, true);

    assert!(html.contains(r#"class="tool-badge tool-status-success tool-overflow-extra""#));
    assert!(html.contains(r#"data-tool-name="Search""#));
}

#[test]
fn test_get_tool_lucide_icon() {
    // Check icon mappings
    assert!(get_tool_lucide_icon("Bash").contains("polyline")); // Terminal
    assert!(get_tool_lucide_icon("Read").contains("M15 2H6")); // FileText
    let pencil = get_tool_lucide_icon("Write");
    assert!(pencil.contains("M21.174"));
    assert!(pencil.contains("a2 2 0 0 0 .83-.497z"));
    assert!(pencil.contains(r#"<path d="m15 5 4 4"/>"#));
    assert!(!pencil.contains("a2 2 0 0 0 2 0l.43.25"));
    assert!(get_tool_lucide_icon("Glob").contains("circle cx=\"11\"")); // Search
    assert!(get_tool_lucide_icon("WebFetch").contains("circle cx=\"12\" cy=\"12\" r=\"10\"")); // Globe
    assert!(get_tool_lucide_icon("mcp__mcp-agent-mail__send").contains("rect width=\"20\"")); // Mail
    assert!(get_tool_lucide_icon("unknown_tool").contains("path d=\"M14.7 6.3")); // Wrench fallback
}

#[test]
fn test_render_message_groups_empty() {
    let groups: Vec<MessageGroup> = vec![];
    let opts = RenderOptions::default();
    let html = render_message_groups(&groups, &opts).unwrap();

    // Should just have the wrapper if agent class is set
    assert!(html.is_empty() || !html.contains("conversation-messages"));
}

#[test]
fn test_render_message_groups_with_agent_class() {
    let groups = vec![
        MessageGroup::user(test_message("user", "Hello")),
        MessageGroup::assistant(test_message("assistant", "Hi there")),
    ];
    let opts = RenderOptions {
        agent_slug: Some("claude_code".to_string()),
        ..Default::default()
    };
    let html = render_message_groups(&groups, &opts).unwrap();

    assert!(html.contains("agent-claude"));
    assert!(html.contains("conversation-messages"));
    assert!(html.contains("message-user"));
    assert!(html.contains("message-assistant"));
}

#[test]
fn test_render_message_group_system() {
    let msg = test_message("system", "You are a helpful assistant.");
    let group = MessageGroup::system(msg);
    let opts = RenderOptions::default();
    let html = render_message_group(&group, 0, &opts).unwrap();

    assert!(html.contains("message-system"));
    assert!(html.contains("System")); // Author display
    assert!(html.contains("You are a helpful assistant."));
    assert!(html.contains("M9.671 4.136"));
    assert!(html.contains("3.319-1.915"));
    assert!(!html.contains("a2 2 0 0 0-2.73.73"));
}

#[test]
fn test_render_message_group_tool_only() {
    let msg = test_message("tool", "Tool result content");
    let group = MessageGroup::tool_only(msg);
    let opts = RenderOptions::default();
    let html = render_message_group(&group, 0, &opts).unwrap();

    assert!(html.contains("message-tool"));
}

#[test]
fn test_render_message_group_with_timestamp() {
    let mut msg = test_message("user", "Test message");
    msg.timestamp = Some("2026-01-25T14:30:00Z".to_string());
    let group = MessageGroup::user(msg);

    let opts = RenderOptions {
        show_timestamps: true,
        ..Default::default()
    };
    let html = render_message_group(&group, 0, &opts).unwrap();

    assert!(html.contains("<time"));
    assert!(html.contains("datetime="));
    assert!(html.contains("2026-01-25"));
}

#[test]
fn test_render_message_group_without_timestamps() {
    let mut msg = test_message("user", "Test message");
    msg.timestamp = Some("2026-01-25T14:30:00Z".to_string());
    let group = MessageGroup::user(msg);

    let opts = RenderOptions {
        show_timestamps: false,
        ..Default::default()
    };
    let html = render_message_group(&group, 0, &opts).unwrap();

    assert!(!html.contains("<time"));
}

#[test]
fn test_render_message_group_tool_badges_hidden_when_disabled() {
    let msg = test_message("assistant", "Let me check that file.");
    let mut group = MessageGroup::assistant(msg);
    group.add_tool_call(test_tool_call("Read"), None);

    let opts = RenderOptions {
        show_tool_calls: false,
        ..Default::default()
    };
    let html = render_message_group(&group, 0, &opts).unwrap();

    assert!(!html.contains("tool-badge"));
}

#[test]
fn test_render_message_group_with_collapse() {
    let long_content = "x".repeat(2000);
    let msg = test_message("user", &long_content);
    let group = MessageGroup::user(msg);

    let opts = RenderOptions {
        collapse_threshold: 1000,
        ..Default::default()
    };
    let html = render_message_group(&group, 0, &opts).unwrap();

    assert!(html.contains("<details"));
    assert!(html.contains("message-collapse"));
    assert!(html.contains("Click to expand"));
}

#[test]
fn test_render_message_group_anchors() {
    let mut msg = test_message("user", "Test message");
    msg.index = Some(42);
    let group = MessageGroup::user(msg);
    let opts = RenderOptions::default();
    let html = render_message_group(&group, 0, &opts).unwrap();

    assert!(html.contains(r#"id="msg-42""#));
}

#[test]
fn test_render_message_group_uses_fallback_index() {
    // No message index, should use the group index
    let msg = test_message("user", "Test message");
    let group = MessageGroup::user(msg);
    let opts = RenderOptions::default();
    let html = render_message_group(&group, 5, &opts).unwrap();

    assert!(html.contains(r#"id="msg-5""#));
}

#[test]
fn test_tool_badge_preserves_full_input_in_popover() {
    let long_input = r#"{"command": ""#.to_owned() + &"x".repeat(500) + r#""}"#;
    let mut call = test_tool_call("Bash");
    call.input = long_input;
    let tool = ToolCallWithResult::new(call);
    let html = render_single_tool_badge(&tool, false);

    // Inline popovers preserve full content (scrollable), no truncation
    assert!(html.contains("tool-popover-section"));
    assert!(html.contains(&"x".repeat(100))); // Full content present
}

#[test]
fn test_tool_badge_accessibility() {
    let tool = test_tool_call_with_result("Read", ToolStatus::Success);
    let html = render_single_tool_badge(&tool, false);

    // Must be a button (keyboard accessible)
    assert!(html.contains("<button"));
    assert!(html.contains("</button>"));
    // Must have aria-label
    assert!(html.contains("aria-label="));
    // Must have aria-expanded for popover
    assert!(html.contains("aria-expanded="));
}

#[test]
fn test_render_message_groups_all_roles() {
    let groups = vec![
        MessageGroup::user(test_message("user", "User message")),
        MessageGroup::assistant(test_message("assistant", "Assistant response")),
        MessageGroup::system(test_message("system", "System context")),
        MessageGroup::tool_only(test_message("tool", "Tool result")),
    ];
    let opts = RenderOptions::default();
    let html = render_message_groups(&groups, &opts).unwrap();

    assert!(html.contains("message-user"));
    assert!(html.contains("message-assistant"));
    assert!(html.contains("message-system"));
    assert!(html.contains("message-tool"));
}
