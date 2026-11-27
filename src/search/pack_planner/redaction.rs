use std::borrow::Cow;

use once_cell::sync::Lazy;
use regex::{Captures, Regex};

use crate::resource_plan::redact_secrets::redact_text;

use super::types::RenderedRedaction;

pub(super) const REDACTED_VALUE_MARKER: &str = "[REDACTED]";
const REDACTED_PATH_PREFIX: &str = "[REDACTED_PATH]";
pub(super) const REDACTED_REMOTE_HOST_MARKER: &str = "[REDACTED_REMOTE_HOST]";
pub(super) const REDACTED_SOURCE_MARKER: &str = "[REDACTED_SOURCE]";
pub(super) const REDACTED_ENCRYPTED_PAYLOAD_MARKER: &str = "[REDACTED_ENCRYPTED_PAYLOAD]";

static PRIVATE_PATH_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?x)
        (?:
            (?:/home/[^/\s"'`<>\[\](){}:,;]+|/Users/[^/\s"'`<>\[\](){}:,;]+|~)
            (?:/[^\s"'`<>\[\](){}:,;]+)*
          |
            [A-Za-z]:\\Users\\[^\\\s"'`<>\[\](){}:,;]+
            (?:\\[^\s"'`<>\[\](){}:,;]+)*
        )
        "#,
    )
    .expect("private path redaction regex")
});

static ENCRYPTED_PAYLOAD_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)\b(?:encrypted_(?:payload(?:_material)?|content|blob|material)|ciphertext)\b\s*[:=]\s*[A-Za-z0-9+/=_:.-]{16,}",
    )
    .expect("encrypted payload redaction regex")
});

static PRIVATE_HOST_LABEL_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)\b(?:[A-Za-z0-9._%+-]+@)?[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*(?:\.internal|\.local)\b",
    )
    .expect("private host label redaction regex")
});

pub(super) fn redact_pack_output_text(
    input: &str,
    redactions: &mut Vec<RenderedRedaction>,
) -> String {
    let mut output = input.to_string();

    let encrypted_redacted = ENCRYPTED_PAYLOAD_RE.replace_all(&output, |_: &Captures<'_>| {
        REDACTED_ENCRYPTED_PAYLOAD_MARKER
    });
    if let Cow::Owned(redacted) = encrypted_redacted {
        push_full_redaction(
            redactions,
            "encrypted_payload",
            &output,
            REDACTED_ENCRYPTED_PAYLOAD_MARKER,
        );
        output = redacted;
    }

    if let Cow::Owned(redacted) = redact_text(&output) {
        push_full_redaction(redactions, "secret", &output, REDACTED_VALUE_MARKER);
        output = redacted;
    }

    let host_redacted =
        PRIVATE_HOST_LABEL_RE.replace_all(&output, |_: &Captures<'_>| REDACTED_REMOTE_HOST_MARKER);
    if let Cow::Owned(redacted) = host_redacted {
        push_full_redaction(
            redactions,
            "remote_host",
            &output,
            REDACTED_REMOTE_HOST_MARKER,
        );
        output = redacted;
    }

    let path_redacted = PRIVATE_PATH_RE.replace_all(&output, |captures: &Captures<'_>| {
        redacted_private_path_marker(&captures[0])
    });
    if let Cow::Owned(redacted) = path_redacted {
        push_full_redaction(
            redactions,
            "private_path",
            &output,
            &format!("{REDACTED_PATH_PREFIX}/<name>"),
        );
        output = redacted;
    }

    output
}

/// Redacts every string in a JSON value; keys are schema names and are kept.
pub(super) fn redact_pack_output_json(
    value: &serde_json::Value,
    redactions: &mut Vec<RenderedRedaction>,
) -> serde_json::Value {
    match value {
        serde_json::Value::String(text) => {
            serde_json::Value::String(redact_pack_output_text(text, redactions))
        }
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(|item| redact_pack_output_json(item, redactions))
                .collect(),
        ),
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(key, item)| (key.clone(), redact_pack_output_json(item, redactions)))
                .collect(),
        ),
        other => other.clone(),
    }
}

pub(super) fn redacted_source_label(
    source_id: &str,
    origin_kind: &str,
    redactions: &mut Vec<RenderedRedaction>,
) -> String {
    if is_remote_origin(origin_kind) && !source_id.trim().is_empty() {
        push_full_redaction(redactions, "remote_host", source_id, REDACTED_SOURCE_MARKER);
        REDACTED_SOURCE_MARKER.to_string()
    } else {
        redact_pack_output_text(source_id, redactions)
    }
}

pub(super) fn rendered_origin_host(
    origin_host: Option<&str>,
    redactions: &mut Vec<RenderedRedaction>,
) -> Option<String> {
    let host = origin_host?;
    let trimmed = host.trim();
    if trimmed.is_empty() {
        return Some(String::new());
    }
    push_full_redaction(redactions, "remote_host", host, REDACTED_REMOTE_HOST_MARKER);
    Some(REDACTED_REMOTE_HOST_MARKER.to_string())
}

fn is_remote_origin(origin_kind: &str) -> bool {
    !origin_kind.trim().eq_ignore_ascii_case("local")
}

fn redacted_private_path_marker(path: &str) -> String {
    let trimmed = path.trim_end_matches(['/', '\\']);
    let components = trimmed
        .split(['/', '\\'])
        .filter(|component| !component.is_empty())
        .collect::<Vec<_>>();
    let basename = match components.as_slice() {
        ["home", _] | ["Users", _] | [_, "Users", _] | ["~"] => "home",
        _ => components
            .last()
            .copied()
            .filter(|component| *component != "~")
            .unwrap_or("path"),
    };
    format!("{REDACTED_PATH_PREFIX}/{basename}")
}

pub(super) fn push_full_redaction(
    redactions: &mut Vec<RenderedRedaction>,
    kind: &str,
    original: &str,
    replacement: &str,
) {
    if original.is_empty() {
        return;
    }
    redactions.push(RenderedRedaction {
        kind: kind.to_string(),
        start_char: 0,
        end_char: original.chars().count(),
        replacement: replacement.to_string(),
    });
}
