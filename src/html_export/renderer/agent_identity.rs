// ============================================
// Lucide SVG Icons (16x16, stroke-width: 2)
// ============================================

/// User icon - for user messages
pub(super) const ICON_USER: &str = r#"<svg class="lucide-icon" xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>"#;

/// Bot icon - for assistant messages
pub(super) const ICON_BOT: &str = r#"<svg class="lucide-icon" xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 8V4H8"/><rect width="16" height="12" x="4" y="8" rx="2"/><path d="M2 14h2"/><path d="M20 14h2"/><path d="M15 13v2"/><path d="M9 13v2"/></svg>"#;

/// Wrench icon - for tool messages
pub(super) const ICON_WRENCH: &str = r#"<svg class="lucide-icon" xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94l-3.76 3.76z"/></svg>"#;

/// Settings icon - for system messages
pub(super) const ICON_SETTINGS: &str = r#"<svg class="lucide-icon" xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9.671 4.136a2.34 2.34 0 0 1 4.659 0 2.34 2.34 0 0 0 3.319 1.915 2.34 2.34 0 0 1 2.33 4.033 2.34 2.34 0 0 0 0 3.831 2.34 2.34 0 0 1-2.33 4.033 2.34 2.34 0 0 0-3.319 1.915 2.34 2.34 0 0 1-4.659 0 2.34 2.34 0 0 0-3.32-1.915 2.34 2.34 0 0 1-2.33-4.033 2.34 2.34 0 0 0 0-3.831A2.34 2.34 0 0 1 6.35 6.051a2.34 2.34 0 0 0 3.319-1.915"/><circle cx="12" cy="12" r="3"/></svg>"#;

/// Message square icon - fallback
pub(super) const ICON_MESSAGE: &str = r#"<svg class="lucide-icon" xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>"#;

/// Terminal icon - for bash/shell
pub(super) const ICON_TERMINAL: &str = r#"<svg class="lucide-icon" xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="4 17 10 11 4 5"/><line x1="12" x2="20" y1="19" y2="19"/></svg>"#;

/// File text icon - for read
pub(super) const ICON_FILE_TEXT: &str = r#"<svg class="lucide-icon" xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z"/><path d="M14 2v4a2 2 0 0 0 2 2h4"/><path d="M10 9H8"/><path d="M16 13H8"/><path d="M16 17H8"/></svg>"#;

/// Pencil icon - for write/edit
pub(super) const ICON_PENCIL: &str = r#"<svg class="lucide-icon" xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21.174 6.812a1 1 0 0 0-3.986-3.987L3.842 16.174a2 2 0 0 0-.5.83l-1.321 4.352a.5.5 0 0 0 .623.622l4.353-1.32a2 2 0 0 0 .83-.497z"/><path d="m15 5 4 4"/></svg>"#;

/// Search icon - for glob/grep/search
pub(super) const ICON_SEARCH: &str = r#"<svg class="lucide-icon" xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/></svg>"#;

/// Globe icon - for web fetch
pub(super) const ICON_GLOBE: &str = r#"<svg class="lucide-icon" xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M12 2a14.5 14.5 0 0 0 0 20 14.5 14.5 0 0 0 0-20"/><path d="M2 12h20"/></svg>"#;

/// Check icon - for success status
pub(super) const ICON_CHECK: &str = r#"<svg class="lucide-icon" xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6 9 17l-5-5"/></svg>"#;

/// X icon - for error status
pub(super) const ICON_X: &str = r#"<svg class="lucide-icon" xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M18 6 6 18"/><path d="m6 6 12 12"/></svg>"#;

/// Loader icon - for pending status
pub(super) const ICON_LOADER: &str = r#"<svg class="lucide-icon lucide-spin" xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2v4"/><path d="m16.2 7.8 2.9-2.9"/><path d="M18 12h4"/><path d="m16.2 16.2 2.9 2.9"/><path d="M12 18v4"/><path d="m4.9 19.1 2.9-2.9"/><path d="M2 12h4"/><path d="m4.9 4.9 2.9 2.9"/></svg>"#;

/// Mail icon - for MCP agent mail
pub(super) const ICON_MAIL: &str = r#"<svg class="lucide-icon" xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="20" height="16" x="2" y="4" rx="2"/><path d="m22 7-8.97 5.7a1.94 1.94 0 0 1-2.06 0L2 7"/></svg>"#;

/// Database icon - for data operations
pub(super) const ICON_DATABASE: &str = r#"<svg class="lucide-icon" xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5V19A9 3 0 0 0 21 19V5"/><path d="M3 12A9 3 0 0 0 21 12"/></svg>"#;

/// Sparkles icon - for AI/task operations
pub(super) const ICON_SPARKLES: &str = r#"<svg class="lucide-icon" xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9.937 15.5A2 2 0 0 0 8.5 14.063l-6.135-1.582a.5.5 0 0 1 0-.962L8.5 9.936A2 2 0 0 0 9.937 8.5l1.582-6.135a.5.5 0 0 1 .963 0L14.063 8.5A2 2 0 0 0 15.5 9.937l6.135 1.581a.5.5 0 0 1 0 .964L15.5 14.063a2 2 0 0 0-1.437 1.437l-1.582 6.135a.5.5 0 0 1-.963 0z"/><path d="M20 3v4"/><path d="M22 5h-4"/><path d="M4 17v2"/><path d="M5 18H3"/></svg>"#;

/// Get the CSS class for an agent slug.
///
/// Maps agent identifiers to their visual styling class.
pub fn agent_css_class(slug: &str) -> &'static str {
    if crate::html_export::filename::is_omp_agent_alias(slug) {
        return "agent-aider";
    }

    let slug = slug.trim().to_ascii_lowercase().replace('-', "_");
    match slug.as_str() {
        "claude_code" | "claude" => "agent-claude",
        "codex" | "codex_cli" => "agent-codex",
        "cursor" | "cursor_ai" => "agent-cursor",
        "chatgpt" | "openai" => "agent-chatgpt",
        "gemini" | "gemini_cli" | "google" => "agent-gemini",
        "antigravity" | "agy" => "agent-antigravity",
        "aider" => "agent-aider",
        "copilot" | "copilot_cli" | "github_copilot" | "github_copilot_cli" => "agent-copilot",
        "cody" | "sourcegraph" | "continue" | "continue_dev" => "agent-cody",
        "windsurf" => "agent-windsurf",
        "amp" => "agent-amp",
        "grok" | "grok_bot" => "agent-grok",
        "cline" | "clawdbot" | "kimi" => "agent-gemini",
        "opencode" | "qwen" => "agent-codex",
        "pi_agent" | "pi_durable" | "omp" | "factory" | "droid" => "agent-aider",
        "openclaw" => "agent-copilot",
        "vibe" | "mistral" => "agent-chatgpt",
        "crush" => "agent-amp",
        "hermes" => "agent-hermes",
        "goose" => "agent-goose",
        "openhands" | "open_hands" | "shelley" => "agent-aider",
        "muse" | "muse_code" => "agent-amp",
        "prime_agent" => "agent-codex",
        "kiro" => "agent-gemini",
        "devin" => "agent-cursor",
        "codebuff" | "freebuff" | "manicode" => "agent-codex",
        "qoder" => "agent-codex",
        _ => "agent-default",
    }
}

/// Get human-readable agent name.
pub fn agent_display_name(slug: &str) -> &'static str {
    if crate::html_export::filename::is_omp_agent_alias(slug) {
        return "Oh My Pi";
    }

    let slug = slug.trim().to_ascii_lowercase().replace('-', "_");
    match slug.as_str() {
        "claude_code" | "claude" => "Claude",
        "codex" | "codex_cli" => "Codex",
        "cursor" | "cursor_ai" => "Cursor",
        "chatgpt" | "openai" => "ChatGPT",
        "gemini" | "gemini_cli" | "google" => "Gemini",
        "antigravity" | "agy" => "Antigravity",
        "aider" => "Aider",
        "copilot" | "github_copilot" => "GitHub Copilot",
        "copilot_cli" | "github_copilot_cli" => "GitHub Copilot CLI",
        "cody" | "sourcegraph" => "Cody",
        "continue" | "continue_dev" => "Continue",
        "windsurf" => "Windsurf",
        "amp" => "Amp",
        "grok" => "Grok",
        "cline" => "Cline",
        "opencode" => "OpenCode",
        "pi_agent" => "Pi Agent",
        "pi_durable" => "Pi Durable",
        "omp" => "Oh My Pi",
        "factory" | "droid" => "Factory",
        "openclaw" => "OpenClaw",
        "clawdbot" => "ClawdBot",
        "vibe" => "Vibe",
        "mistral" => "Mistral",
        "crush" => "Crush",
        "hermes" => "Hermes",
        "muse" | "muse_code" => "Muse Code",
        "goose" => "Goose",
        "kimi" => "Kimi",
        "qwen" => "Qwen",
        "openhands" | "open_hands" => "OpenHands",
        "prime_agent" => "Prime Agent",
        "kiro" => "Kiro",
        "devin" => "Devin",
        "qoder" => "Qoder",
        "shelley" => "Shelley",
        "grok_bot" => "Grok Bot",
        // One shared Manicode store; no per-chat writer marker exists (GH #423).
        "codebuff" | "freebuff" | "manicode" => "Codebuff / Freebuff",
        _ => "AI Assistant",
    }
}
