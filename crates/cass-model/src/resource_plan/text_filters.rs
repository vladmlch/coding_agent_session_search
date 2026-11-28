//! Pure text classification helpers shared across export, canonicalization,
//! search-region derivation, and answer-pack planning.

/// Whole-message skill and hook injections excluded from handoffs and exports
/// unless the caller explicitly opts in. Inspect the full message before any
/// excerpt truncation so a retained suffix cannot hide its injection marker.
pub fn is_skill_injection(content: &str) -> bool {
    content.contains("Base directory for this skill:")
        || content.contains("<system-reminder>")
        || content.contains("The following skills are available for use with the Skill tool:")
        || (content.contains("skillInjection:") && content.contains("matchedSkills"))
        || content.contains("<!-- skillInjection:")
}

fn role_is(role: Option<&str>, expected: &str) -> bool {
    role.is_some_and(|role| role.trim().eq_ignore_ascii_case(expected))
}

fn is_short_acknowledgement(lower: &str) -> bool {
    matches!(
        lower,
        "ok" | "ok."
            | "okay"
            | "okay."
            | "done"
            | "done."
            | "done!"
            | "got it"
            | "got it."
            | "got it!"
            | "ack"
            | "ack."
            | "acknowledged"
            | "acknowledged."
            | "confirmed"
            | "confirmed."
            | "completed"
            | "completed."
            | "complete"
            | "complete."
    )
}

/// Return true when text is a low-value acknowledgement/tool confirmation.
///
/// These messages add little search value and tend to dominate result sets with
/// repeated "done/acknowledged/wrote file" noise.
pub fn is_tool_acknowledgement(role: Option<&str>, text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return false;
    }

    if trimmed.len() > 200 {
        return false;
    }

    let lower = trimmed.to_ascii_lowercase();
    if is_short_acknowledgement(&lower) {
        return true;
    }

    let toolish = role_is(role, "tool");
    let short_tool_ack = lower == "no matches found"
        || lower == "no changes made"
        || lower == "no changes"
        || lower == "already up to date"
        || lower == "up to date"
        || lower == "file written";
    if short_tool_ack && (toolish || lower.contains("file") || lower.contains("match")) {
        return true;
    }

    let prefixed_tool_ack = lower.starts_with("successfully wrote to ")
        || lower.starts_with("successfully updated ")
        || lower.starts_with("successfully created ")
        || lower.starts_with("successfully deleted ")
        || lower.starts_with("successfully saved ")
        || lower.starts_with("successfully applied ")
        || lower.starts_with("applied patch")
        || lower.starts_with("patch applied");
    prefixed_tool_ack && (toolish || lower.contains('/') || lower.contains("file"))
}

/// Return true when content looks like an injected prompt/instructions block.
///
/// Noise we can safely skip during indexing.
pub fn is_hard_message_noise(role: Option<&str>, text: &str) -> bool {
    text.trim().is_empty() || is_tool_acknowledgement(role, text)
}
