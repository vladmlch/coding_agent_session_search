use serde_json::{Map, Value};

use super::MAX_REGION_TEXT;

// ---------------------------------------------------------------------------
// Derivation helpers (ported from the query-time lane, unchanged semantics)
// ---------------------------------------------------------------------------

pub(super) fn extra_string<'a>(map: &'a Map<String, Value>, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| map.get(*key).and_then(Value::as_str))
        .filter(|text| !text.is_empty())
}

pub(super) fn tool_records(extra: &Value) -> (Vec<Value>, Vec<Value>) {
    let mut calls = Vec::new();
    let mut results = Vec::new();
    let kind = extra.get("type").and_then(Value::as_str).unwrap_or("");
    if let Some(content) = extra.pointer("/message/content").and_then(Value::as_array) {
        for part in content {
            let part_kind = part.get("type").and_then(Value::as_str).unwrap_or("");
            if matches!(part_kind, "tool_use" | "tool_call") {
                calls.push(part.clone());
            } else if matches!(part_kind, "tool_result") {
                results.push(part.clone());
            }
        }
    }
    match kind {
        "tool_use" | "tool_call" | "function_call" => calls.push(extra.clone()),
        "tool_result" | "tool_call_output" | "function_call_output" => results.push(extra.clone()),
        _ => {}
    }
    if let Some(item) = extra.get("payload") {
        match item.get("type").and_then(Value::as_str).unwrap_or("") {
            "function_call" | "tool_use" | "tool_call" => calls.push(item.clone()),
            "function_call_output" | "tool_result" | "tool_call_output" => {
                results.push(item.clone())
            }
            _ => {}
        }
    }
    (calls, results)
}

pub(super) fn call_text(call: &Value) -> String {
    let mut parts = Vec::new();
    if let Some(input) = call.get("input") {
        match input {
            Value::Object(map) => {
                for key in ["command", "file_path", "path", "query", "pattern", "cmd"] {
                    if let Some(text) = map.get(key).and_then(Value::as_str) {
                        parts.push(text.to_owned());
                    }
                }
                if parts.is_empty() {
                    let text = input.to_string();
                    if text.len() <= 8_192 {
                        parts.push(text);
                    }
                }
            }
            Value::String(text) => parts.push(text.clone()),
            _ => {}
        }
    }
    if let Some(args) = call.get("arguments").and_then(Value::as_str) {
        parts.push(args.to_owned());
    }
    parts.join(" ")
}

/// Antigravity planner-invocation arguments as searchable text: named
/// command/path keys first, the canonical JSON otherwise (bounded). Not a
/// `call_text` extension on purpose — its `arguments` arm reads strings
/// only, and widening the shared helper would change other providers'
/// region text. The antigravity connector emits argument OBJECTS.
pub(super) fn invocation_arguments_text(value: Option<&Value>) -> String {
    let Some(value) = value else {
        return String::new();
    };
    match value {
        Value::Object(map) => {
            let mut parts = Vec::new();
            for key in [
                "command",
                "CommandLine",
                "file_path",
                "TargetFile",
                "path",
                "query",
                "pattern",
                "cmd",
            ] {
                if let Some(text) = map.get(key).and_then(Value::as_str) {
                    parts.push(text.to_owned());
                }
            }
            if parts.is_empty() {
                let text = value.to_string();
                if text.len() <= 8_192 {
                    parts.push(text);
                }
            }
            parts.join(" ")
        }
        Value::String(text) => text.clone(),
        _ => String::new(),
    }
}

pub(super) fn result_text(result: &Value) -> String {
    for key in ["content", "output", "text", "stdout"] {
        if let Some(value) = result.get(key) {
            match value {
                Value::String(text) => return text.clone(),
                Value::Array(parts) => {
                    return parts
                        .iter()
                        .filter_map(|part| {
                            part.as_str().map(str::to_owned).or_else(|| {
                                part.get("text").and_then(Value::as_str).map(str::to_owned)
                            })
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                }
                _ => {}
            }
        }
    }
    String::new()
}

pub(super) fn numeric_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| {
        value
            .as_f64()
            .filter(|v| v.fract() == 0.0)
            .map(|v| v as i64)
    })
}

pub(super) fn thinking_text(extra: &Value) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(content) = extra.pointer("/message/content").and_then(Value::as_array) {
        for part in content {
            if matches!(
                part.get("type").and_then(Value::as_str),
                Some("thinking" | "reasoning")
            ) && let Some(text) = part
                .get("thinking")
                .or_else(|| part.get("text"))
                .and_then(Value::as_str)
            {
                parts.push(text.to_owned());
            }
        }
    }
    for key in ["thinking", "reasoning", "reasoning_content"] {
        if let Some(text) = extra.get(key).and_then(Value::as_str) {
            parts.push(text.to_owned());
        }
    }
    if matches!(
        extra.get("type").and_then(Value::as_str),
        Some("thinking" | "reasoning" | "agent_reasoning")
    ) {
        for key in ["text", "content", "summary"] {
            if let Some(text) = extra.get(key).and_then(Value::as_str) {
                parts.push(text.to_owned());
            }
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
}

pub(super) fn attachment_paths(extra: &Value) -> Vec<String> {
    let mut paths = Vec::new();
    let kind = extra.get("type").and_then(Value::as_str).unwrap_or("");
    if matches!(kind, "attachment" | "artifact" | "file")
        && let Some(path) = extra_string(
            extra.as_object().expect("checked"),
            &["file_path", "path", "name"],
        )
    {
        paths.push(path.to_owned());
    }
    if let Some(list) = extra.get("attachments").and_then(Value::as_array) {
        for element in list {
            if let Some(path) = element.as_str().filter(|p| !p.is_empty()) {
                paths.push(path.to_owned());
            } else if let Some(object) = element.as_object()
                && let Some(path) = extra_string(object, &["file_path", "path", "name"])
            {
                paths.push(path.to_owned());
            }
        }
    }
    paths
}

pub(super) fn bounded(text: &str) -> String {
    // Byte budget with a UTF-8-safe edge: the bound may split a multibyte
    // scalar — back off to the previous char boundary instead of panicking
    // on the raw byte slice.
    let mut end = text.len().min(MAX_REGION_TEXT);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}
