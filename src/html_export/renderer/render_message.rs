use std::time::Instant;

use pulldown_cmark::{CowStr, Options, Parser, html};
use tracing::{debug, trace};

use crate::html_export::template::html_escape;

use super::agent_identity::{
    ICON_BOT, ICON_DATABASE, ICON_FILE_TEXT, ICON_GLOBE, ICON_MAIL, ICON_MESSAGE, ICON_PENCIL,
    ICON_SEARCH, ICON_SETTINGS, ICON_SPARKLES, ICON_TERMINAL, ICON_USER, ICON_WRENCH,
};
use super::types::{Message, RenderError, RenderOptions, ToolCall};

/// Render a single message to HTML.
pub fn render_message(message: &Message, options: &RenderOptions) -> Result<String, RenderError> {
    let started = Instant::now();
    trace!(
        component = "renderer",
        operation = "render_message",
        message_index = message.index.unwrap_or(0),
        has_index = message.index.is_some(),
        role = message.role.as_str(),
        content_len = message.content.len(),
        "Rendering message"
    );

    // Role class for semantic styling (matches styles.rs)
    let role_class = match message.role.as_str() {
        "user" => "message-user",
        "assistant" | "agent" => "message-assistant",
        "tool" => "message-tool",
        "system" => "message-system",
        _ => "",
    };

    // Message anchor for deep linking
    let anchor_id = message
        .index
        .map(|idx| format!(r#" id="msg-{}""#, idx))
        .unwrap_or_default();

    // Author display (falls back to role)
    let author_display = message
        .author
        .as_ref()
        .map(|a| html_escape(a))
        .unwrap_or_else(|| format_role_display(&message.role));

    let timestamp_html = if options.show_timestamps {
        if let Some(ts) = &message.timestamp {
            format!(
                r#"<time class="message-time" datetime="{}">{}</time>"#,
                html_escape(ts),
                html_escape(&format_timestamp(ts))
            )
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    let content_html = render_content(&message.content, options);

    // Check if message should be collapsed
    let content_bytes = message.content.len();
    let mut content_chars = 0; // Calculated lazily
    let should_collapse =
        options.collapse_threshold > 0 && content_bytes > options.collapse_threshold && {
            let mut chars = message.content.chars();
            let mut count = 0;
            while count <= options.collapse_threshold && chars.next().is_some() {
                count += 1;
            }
            content_chars = if count > options.collapse_threshold {
                // We know it exceeds, but we need the full count for display
                count + chars.count()
            } else {
                count
            };
            content_chars > options.collapse_threshold
        };

    let (content_wrapper_start, content_wrapper_end) = if should_collapse {
        debug!(
            component = "renderer",
            operation = "collapse_message",
            message_index = message.index.unwrap_or(0),
            content_len = content_chars,
            collapse_threshold = options.collapse_threshold,
            "Collapsing long message"
        );
        let preview_chars = options.collapse_threshold.min(500);
        // Safe truncation at char boundary to avoid panic on multi-byte UTF-8.
        let safe_len = byte_index_for_char_count(&message.content, preview_chars);
        let preview = message.content.get(..safe_len).unwrap_or("");
        (
            format!(
                r#"<details class="message-collapse">
                    <summary>
                        <span class="message-preview">{}</span>
                        <span class="message-expand-hint">Click to expand ({} chars)</span>
                    </summary>
                    <div class="message-expanded">"#,
                html_escape(preview),
                content_chars
            ),
            "</div></details>".to_string(),
        )
    } else {
        (String::new(), String::new())
    };

    // Tool badges rendered as compact icons in header (upper-right)
    let tool_badges_html = if options.show_tool_calls {
        if let Some(tc) = &message.tool_call {
            render_tool_badge(tc, options)
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    // Role icon for visual differentiation - using Lucide SVG icons
    let role_icon = match message.role.as_str() {
        "user" => ICON_USER,
        "assistant" | "agent" => ICON_BOT,
        "tool" => ICON_WRENCH,
        "system" => ICON_SETTINGS,
        _ => ICON_MESSAGE,
    };

    // Only render content div if there's actual content
    let content_section = if content_html.trim().is_empty() {
        String::new()
    } else {
        format!(
            r#"
                <div class="message-content">
                    {wrapper_start}{content}{wrapper_end}
                </div>"#,
            wrapper_start = content_wrapper_start,
            content = content_html,
            wrapper_end = content_wrapper_end,
        )
    };

    let rendered = format!(
        r#"            <article class="message {role_class}"{anchor} role="article" aria-label="{role} message">
                <header class="message-header">
                    <div class="message-header-left">
                        <span class="message-icon" aria-hidden="true">{role_icon}</span>
                        <span class="message-author">{author}</span>
                        {timestamp}
                    </div>
                    <div class="message-header-right">
                        {tool_badges}
                    </div>
                </header>{content_section}
            </article>"#,
        role_class = role_class,
        anchor = anchor_id,
        role = html_escape(&message.role),
        role_icon = role_icon,
        author = author_display,
        timestamp = timestamp_html,
        content_section = content_section,
        tool_badges = tool_badges_html,
    );

    debug!(
        component = "renderer",
        operation = "render_message_complete",
        message_index = message.index.unwrap_or(0),
        duration_ms = started.elapsed().as_millis(),
        bytes = rendered.len(),
        "Message rendered"
    );

    Ok(rendered)
}

/// Format role for display.
pub(super) fn format_role_display(role: &str) -> String {
    match role {
        "user" => "You".to_string(),
        "assistant" | "agent" => "Assistant".to_string(),
        "tool" => "Tool".to_string(),
        "system" => "System".to_string(),
        other => html_escape(other),
    }
}

/// Render message content, converting markdown to HTML using pulldown-cmark.
/// Raw HTML in the input is escaped for security (XSS prevention).
pub(super) fn render_content(content: &str, _options: &RenderOptions) -> String {
    use pulldown_cmark::{Event, Tag};

    // Configure pulldown-cmark with all common extensions
    let mut opts = Options::empty();
    opts.insert(Options::ENABLE_STRIKETHROUGH);
    opts.insert(Options::ENABLE_TABLES);
    opts.insert(Options::ENABLE_FOOTNOTES);
    opts.insert(Options::ENABLE_TASKLISTS);
    opts.insert(Options::ENABLE_SMART_PUNCTUATION);

    // Parse markdown and filter out raw HTML for security
    let parser = Parser::new_ext(content, opts).map(|event| match event {
        // Convert raw HTML to escaped text (XSS prevention)
        Event::Html(html) => Event::Text(html),
        Event::InlineHtml(html) => Event::Text(html),
        // Sanitize link destinations to prevent javascript:/vbscript:/data: XSS
        Event::Start(Tag::Link {
            link_type,
            dest_url,
            title,
            id,
        }) => Event::Start(Tag::Link {
            link_type,
            dest_url: sanitize_markdown_dest_url(dest_url),
            title,
            id,
        }),
        Event::Start(Tag::Image {
            link_type,
            dest_url,
            title,
            id,
        }) => Event::Start(Tag::Image {
            link_type,
            dest_url: sanitize_markdown_dest_url(dest_url),
            title,
            id,
        }),
        // Pass through all other events
        other => other,
    });

    let mut html_output = String::new();
    html::push_html(&mut html_output, parser);

    html_output
}

pub(super) fn sanitize_markdown_dest_url(dest_url: CowStr<'_>) -> CowStr<'_> {
    let trimmed = dest_url.trim();
    // Quick check: if it doesn't contain ':', it can't be a dangerous scheme.
    // This avoids allocation for most common URLs (http://, https://, or relative paths).
    if !trimmed.contains(':') {
        return dest_url;
    }

    // Schemes like javascript: are short. We only need to check the beginning.
    let mut normalized = String::with_capacity(16);
    for ch in trimmed
        .chars()
        .filter(|c| !c.is_ascii_whitespace() && !c.is_ascii_control())
    {
        normalized.push(ch.to_ascii_lowercase());
        if normalized.len() >= 15 {
            break;
        }
    }

    if normalized.starts_with("javascript:")
        || normalized.starts_with("vbscript:")
        || normalized.starts_with("data:")
    {
        "#".into()
    } else {
        dest_url
    }
}

/// Render a compact tool badge with hover popover for the message header.
pub(super) fn render_tool_badge(tool_call: &ToolCall, options: &RenderOptions) -> String {
    let started = Instant::now();
    trace!(
        component = "renderer",
        operation = "render_tool_badge",
        tool = tool_call.name.as_str(),
        input_len = tool_call.input.len(),
        output_len = tool_call.output.as_ref().map(|s| s.len()).unwrap_or(0),
        "Rendering tool badge"
    );

    // Status indicator - get CSS class and SVG icon
    let (status_class, status_icon_svg, status_label) = tool_call
        .status
        .as_ref()
        .map(|s| (s.css_class(), s.icon_svg(), s.label()))
        .unwrap_or(("", "", ""));

    // Format input as pretty JSON if possible
    let formatted_input = format_json_or_raw(&tool_call.input);

    // Tool icon based on name - using Lucide SVG icons
    let tool_icon = match tool_call.name.to_lowercase().as_str() {
        "bash" | "shell" => ICON_TERMINAL,
        "read" | "read_file" => ICON_FILE_TEXT,
        "write" | "write_file" | "edit" => ICON_PENCIL,
        "glob" | "find" | "grep" | "search" | "websearch" => ICON_SEARCH,
        "webfetch" | "fetch" | "http" => ICON_GLOBE,
        "task" => ICON_SPARKLES,
        n if n.starts_with("mcp__mcp-agent-mail") => ICON_MAIL,
        n if n.contains("sql") || n.contains("db") => ICON_DATABASE,
        _ => ICON_WRENCH,
    };

    // Suppress unused warning for options - may be used for future customization
    let _ = options;

    // Preserve full input/output for popover display (no truncation)
    let input_preview = formatted_input.clone();

    let output_preview = if let Some(output) = &tool_call.output {
        format_json_or_raw(output)
    } else {
        String::new()
    };

    // Build popover content
    let popover_input = if !input_preview.trim().is_empty() {
        format!(
            r#"<div class="tool-popover-section"><span class="tool-popover-label">Input</span><pre><code>{}</code></pre></div>"#,
            html_escape(&input_preview)
        )
    } else {
        String::new()
    };

    let popover_output = if !output_preview.is_empty() {
        format!(
            r#"<div class="tool-popover-section"><span class="tool-popover-label">Output</span><pre><code>{}</code></pre></div>"#,
            html_escape(&output_preview)
        )
    } else {
        String::new()
    };

    // Compact badge with hover popover - using SVG icons
    let rendered = format!(
        r#"<span class="tool-badge {status_class}" tabindex="0" role="button" aria-label="{name} tool call">
            <span class="tool-badge-icon">{icon}</span>
            {status_badge}
            <div class="tool-popover" role="tooltip">
                <div class="tool-popover-header">{icon} <span>{name}</span> {status_badge}</div>
                {input}{output}
            </div>
        </span>"#,
        icon = tool_icon,
        name = html_escape(&tool_call.name),
        status_class = status_class,
        status_badge = if !status_label.is_empty() {
            format!(
                r#"<span class="tool-badge-status {}">{}</span>"#,
                status_label, status_icon_svg
            )
        } else {
            String::new()
        },
        input = popover_input,
        output = popover_output,
    );

    debug!(
        component = "renderer",
        operation = "render_tool_badge_complete",
        tool = tool_call.name.as_str(),
        duration_ms = started.elapsed().as_millis(),
        bytes = rendered.len(),
        "Tool call rendered"
    );

    rendered
}

/// Try to format as pretty JSON, otherwise return raw.
pub(super) fn format_json_or_raw(s: &str) -> String {
    // Try to parse as JSON and pretty print
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(s)
        && let Ok(pretty) = serde_json::to_string_pretty(&value)
    {
        return pretty;
    }
    s.to_string()
}

/// Format a timestamp for display.
pub(super) fn format_timestamp(ts: &str) -> String {
    // Simple formatting: "2024-01-15T10:30:00Z" -> "2024-01-15 10:30:00"
    if ts.len() >= 19
        && ts.is_char_boundary(10)
        && ts.is_char_boundary(11)
        && ts.is_char_boundary(19)
        && let (Some(date_part), Some(time_part)) = (ts.get(..10), ts.get(11..19))
    {
        format!("{} {}", date_part, time_part)
    } else {
        ts.to_string()
    }
}

/// Find the largest byte index <= `max_bytes` that is on a UTF-8 char boundary.
#[cfg(test)]
pub(super) fn truncate_to_char_boundary(s: &str, max_bytes: usize) -> usize {
    if max_bytes >= s.len() {
        return s.len();
    }
    // Walk backwards from max_bytes to find a char boundary
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    end
}

/// Convert a maximum character count to a safe byte index in `s`.
pub(super) fn byte_index_for_char_count(s: &str, max_chars: usize) -> usize {
    if max_chars == 0 {
        return 0;
    }
    s.char_indices()
        .nth(max_chars)
        .map(|(idx, _)| idx)
        .unwrap_or(s.len())
}
