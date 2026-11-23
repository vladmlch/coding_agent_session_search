#[allow(unused_imports)]
use crate::franken_sync::compat::{
    ConnectionExt, OpenFlags as FrankenOpenFlags, RowExt,
    open_with_flags as open_franken_with_flags,
};
#[allow(unused_imports)]
use crate::indexer::IndexOptions;
#[allow(unused_imports)]
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
#[allow(unused_imports)]
use crate::robot_budget_envelope::BudgetSnapshot;
#[allow(unused_imports)]
use crate::{cli_app::*, cli_root::*, lib_doctor::*, *};
#[allow(unused_imports)]
use anyhow::Result;
#[allow(unused_imports)]
use base64::prelude::*;
#[allow(unused_imports)]
use chrono::Utc;
#[allow(unused_imports)]
use clap::{Arg, ArgAction, Command, CommandFactory, Parser, Subcommand, ValueEnum, ValueHint};
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
#[allow(unused_imports)]
use std::fs::{File, OpenOptions};
#[allow(unused_imports)]
use std::io::{self, BufRead, BufReader, IsTerminal, Read, Seek, SeekFrom, Write};
#[allow(unused_imports)]
use std::path::{Path, PathBuf};
#[allow(unused_imports)]
use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
use std::time::{Duration, Instant};
#[allow(unused_imports)]
use tracing::{info, warn};
#[allow(unused_imports)]
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub(crate) fn stringify_tool_value(value: &serde_json::Value) -> String {
    if value.is_object() || value.is_array() {
        serde_json::to_string_pretty(value).unwrap_or_default()
    } else if let Some(s) = value.as_str() {
        s.to_string()
    } else {
        value.to_string()
    }
}

/// Extract all tool call/result blocks from a message for HTML export.
///
/// Supports multiple formats:
/// 1. Claude/Anthropic format: `content` array with `type: "tool_use"` or `type: "tool_result"` blocks
/// 2. Cursor/generic format: `type: "tool"` at top level with `message.tool_name`, `message.tool_input`, `message.tool_output`
/// 3. Pi-family format: camelCase `toolCall` blocks and `toolResult` messages
pub(crate) fn extract_tool_calls(msg: &serde_json::Value) -> Vec<html_export::ToolCall> {
    let inner = msg.get("message").unwrap_or(msg);
    if inner.get("role").and_then(serde_json::Value::as_str) == Some("toolResult") {
        let output = extract_text_content(msg);
        let output = if output.is_empty() {
            inner.get("content").map(stringify_tool_value)
        } else {
            Some(output)
        };
        let status = if inner
            .get("isError")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            html_export::ToolStatus::Error
        } else {
            html_export::ToolStatus::Success
        };
        return vec![html_export::ToolCall {
            name: inner
                .get("toolName")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("tool_result")
                .to_string(),
            input: String::new(),
            output,
            status: Some(status),
            correlation_id: extract_tool_correlation_id(inner)
                .or_else(|| extract_tool_correlation_id(msg)),
        }];
    }

    // Format 2: Cursor/generic format - check for top-level type: "tool"
    if let Some(msg_type) = msg.get("type").and_then(|t| t.as_str())
        && msg_type == "tool"
    {
        // Look for tool info in the message object
        let inner = msg.get("message").unwrap_or(msg);

        let tool_name = inner
            .get("tool_name")
            .and_then(|n| n.as_str())
            .unwrap_or("tool");

        let tool_input = inner.get("tool_input").map(stringify_tool_value);

        let tool_output = inner.get("tool_output").map(stringify_tool_value);

        // Determine status from explicit status field or presence of output
        let status_str = inner
            .get("status")
            .and_then(|s| s.as_str())
            .or_else(|| msg.get("status").and_then(|s| s.as_str()));

        let status = match status_str {
            Some("success") => Some(html_export::ToolStatus::Success),
            Some("error") => Some(html_export::ToolStatus::Error),
            Some("pending") => Some(html_export::ToolStatus::Pending),
            _ if tool_output.is_some() => Some(html_export::ToolStatus::Success),
            // For exported conversations, don't show "pending" - just hide the status badge
            _ => None,
        };

        return vec![html_export::ToolCall {
            name: tool_name.to_string(),
            input: tool_input.unwrap_or_default(),
            output: tool_output,
            status,
            correlation_id: extract_tool_correlation_id(inner)
                .or_else(|| extract_tool_correlation_id(msg)),
        }];
    }

    // Format 1: Claude/Anthropic format - content array with tool_use/tool_result blocks
    let content = msg
        .get("message")
        .and_then(|m| m.get("content"))
        .or_else(|| msg.get("content"));

    let mut tool_calls = Vec::new();
    if let Some(arr) = content.and_then(|c| c.as_array()) {
        for block in arr {
            if let Some(block_type) = block.get("type").and_then(|t| t.as_str()) {
                match block_type {
                    "tool_use" | "toolCall" => {
                        let name = block.get("name").and_then(|n| n.as_str()).unwrap_or("tool");
                        let input = block
                            .get("input")
                            .or_else(|| block.get("arguments"))
                            .map(stringify_tool_value);
                        tool_calls.push(html_export::ToolCall {
                            name: name.to_string(),
                            input: input.unwrap_or_default(),
                            output: None,
                            // For exported conversations, don't show "pending" for tool invocations
                            // without explicit status - the output may be in a separate message
                            status: None,
                            correlation_id: extract_tool_correlation_id(block),
                        });
                    }
                    "tool_result" => {
                        let content = block.get("content").map(stringify_tool_value);
                        tool_calls.push(html_export::ToolCall {
                            name: "tool_result".to_string(),
                            input: String::new(),
                            output: content,
                            status: Some(html_export::ToolStatus::Success),
                            correlation_id: extract_tool_correlation_id(block),
                        });
                    }
                    _ => {}
                }
            }
        }
    }

    tool_calls
}

pub(crate) fn extract_tool_correlation_id(value: &serde_json::Value) -> Option<String> {
    value
        .get("tool_use_id")
        .or_else(|| value.get("tool_call_id"))
        .or_else(|| value.get("call_id"))
        .or_else(|| value.get("toolCallId"))
        .or_else(|| value.get("callId"))
        .or_else(|| value.get("id"))
        .and_then(|id| id.as_str())
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
}

// ============================================================================
// Message Grouping Algorithm for Consolidated HTML Export
// ============================================================================

/// Agent format for message structure detection.
///
/// Different coding agents use different message formats. This enum helps
/// the grouping algorithm understand how to parse and correlate messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentFormat {
    /// Claude Code: content array with tool_use/tool_result blocks, correlation via tool_use_id
    ClaudeCode,
    /// Codex CLI: function_call and function role messages, correlation via function name
    Codex,
    /// Cursor: type: "tool" at top level with tool_name/tool_input/tool_output
    Cursor,
    /// OpenCode: special handling already exists
    OpenCode,
    /// Generic/unknown format
    Generic,
}

/// Message classification for grouping decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageClassification {
    /// User message with actual text content
    UserContent,
    /// Assistant message with text (may also have embedded tools)
    AssistantContent,
    /// Assistant message with only tool calls, no text
    AssistantToolOnly,
    /// Response to a tool call (tool_result, function response)
    ToolResult,
    /// System message
    System,
    /// Empty or skip-worthy message
    Empty,
}

/// Detect the agent format from a set of messages.
///
/// Examines message structure to determine which agent produced them.
/// This enables format-specific correlation logic.
pub fn detect_agent_format(messages: &[html_export::Message]) -> AgentFormat {
    use tracing::trace;

    // Check first few messages for format indicators
    for msg in messages.iter().take(10) {
        let role = msg.role.as_str();

        // Codex uses "function" role for tool results
        if role == "function" {
            trace!(
                agent_format = "codex",
                "Detected Codex format from function role"
            );
            return AgentFormat::Codex;
        }

        // Check for tool_call presence and structure
        if let Some(ref tc) = msg.tool_call {
            // Cursor format has specific tool names like "tool"
            if tc.name == "tool" || tc.name.starts_with("tool_") {
                trace!(
                    agent_format = "cursor",
                    "Detected Cursor format from tool name pattern"
                );
                return AgentFormat::Cursor;
            }
            // Claude Code uses standard tool names (Bash, Read, Write, etc.)
            if matches!(
                tc.name.as_str(),
                "Bash" | "Read" | "Write" | "Edit" | "Glob" | "Grep" | "Task" | "WebFetch"
            ) {
                trace!(
                    agent_format = "claude_code",
                    "Detected Claude Code format from tool name"
                );
                return AgentFormat::ClaudeCode;
            }
        }
    }

    trace!(
        agent_format = "generic",
        "Using generic format (no specific pattern detected)"
    );
    AgentFormat::Generic
}

/// Classify a message for grouping purposes.
///
/// Determines how a message should be handled in the grouping algorithm:
/// - Starting a new group
/// - Attaching to current group
/// - Being a tool result
/// - Being skipped
pub fn classify_message(msg: &html_export::Message, _format: AgentFormat) -> MessageClassification {
    use tracing::trace;

    let role = msg.role.as_str();
    let has_content = !msg.content.trim().is_empty();
    let has_tool = msg.tool_call.is_some();

    trace!(
        role = role,
        has_content = has_content,
        has_tool = has_tool,
        "Classifying message"
    );

    match role {
        "user" => {
            if msg
                .tool_call
                .as_ref()
                .is_some_and(|tc| tc.name == "tool_result" && tc.output.is_some())
            {
                return MessageClassification::ToolResult;
            }
            if has_content {
                MessageClassification::UserContent
            } else {
                MessageClassification::Empty
            }
        }
        "assistant" | "agent" => {
            if has_content {
                MessageClassification::AssistantContent
            } else if has_tool {
                MessageClassification::AssistantToolOnly
            } else {
                MessageClassification::Empty
            }
        }
        "tool" | "function" => {
            // Tool result messages
            MessageClassification::ToolResult
        }
        "system" => MessageClassification::System,
        _ => {
            // Unknown role - check if it has meaningful content
            if has_content || has_tool {
                MessageClassification::AssistantContent
            } else {
                MessageClassification::Empty
            }
        }
    }
}

/// Extract provider correlation ID from a message for tool call/result matching.
pub fn extract_correlation_id(msg: &html_export::Message, format: AgentFormat) -> Option<String> {
    use tracing::trace;

    if let Some(ref tc) = msg.tool_call
        && let Some(corr_id) = tc.correlation_id.clone()
    {
        trace!(
            correlation_id = %corr_id,
            tool_name = %tc.name,
            "Extracted provider tool correlation ID"
        );
        return Some(corr_id);
    }

    trace!(
        format = ?format,
        has_tool_call = msg.tool_call.is_some(),
        "No provider tool correlation ID present"
    );
    None
}

/// Flush the current group into the groups vector if it exists.
pub(crate) fn flush_group(
    groups: &mut Vec<html_export::MessageGroup>,
    current_group: &mut Option<html_export::MessageGroup>,
) {
    if let Some(group) = current_group.take() {
        tracing::trace!(
            group_type = ?group.group_type,
            tool_count = group.tool_count(),
            "Flushing message group"
        );
        groups.push(group);
    }
}

pub(crate) fn tool_result_content(msg: &html_export::Message) -> String {
    let Some(output) = msg
        .tool_call
        .as_ref()
        .and_then(|tool_call| tool_call.output.as_deref())
        .filter(|output| !output.is_empty())
    else {
        return msg.content.clone();
    };

    if msg.content.trim().is_empty() {
        output.to_string()
    } else if msg.content == output {
        msg.content.clone()
    } else {
        format!("{}\n\n{output}", msg.content)
    }
}

pub(crate) fn standalone_tool_result_group(
    msg: &html_export::Message,
) -> html_export::MessageGroup {
    let mut primary = msg.clone();
    primary.content = tool_result_content(msg);

    if let Some(tool_call) = primary.tool_call.as_ref() {
        primary.author = Some(match tool_call.status {
            Some(html_export::ToolStatus::Error) => format!("{} error", tool_call.name),
            Some(html_export::ToolStatus::Pending) => {
                format!("{} pending result", tool_call.name)
            }
            _ => format!("{} result", tool_call.name),
        });
    }
    primary.role = "tool".to_string();
    primary.tool_call = None;
    html_export::MessageGroup::tool_only(primary)
}

/// Groups flat messages into MessageGroups with tool correlation.
///
/// # Algorithm
/// 1. Detect agent format from message structure
/// 2. Classify each message
/// 3. User/Assistant content messages start new groups
/// 4. Tool-only messages attach to current assistant group
/// 5. Tool results correlate by ID to matching tool call
/// 6. System messages are standalone groups
/// 7. Track timestamps for group range
///
/// # Logging
/// - INFO: Group formation summary
/// - DEBUG: Each message classification
/// - TRACE: Correlation matching details
///
/// # Example
/// ```ignore
/// let messages: Vec<Message> = load_messages();
/// let groups = group_messages_for_export(messages);
/// for group in groups {
///     render_message_group(&group);
/// }
/// ```
pub fn group_messages_for_export(
    messages: Vec<html_export::Message>,
) -> Vec<html_export::MessageGroup> {
    use tracing::{debug, info, trace};

    info!(message_count = messages.len(), "Starting message grouping");

    let format = detect_agent_format(&messages);
    debug!(?format, "Detected agent format");

    let mut groups: Vec<html_export::MessageGroup> = Vec::new();
    let mut current_group: Option<html_export::MessageGroup> = None;

    for (idx, msg) in messages.iter().enumerate() {
        let classification = classify_message(msg, format);
        debug!(
            idx = idx,
            classification = ?classification,
            role = %msg.role,
            content_preview = %msg.content.chars().take(50).collect::<String>(),
            "Classified message"
        );

        match classification {
            MessageClassification::UserContent => {
                // User messages start a new group
                flush_group(&mut groups, &mut current_group);
                let group = html_export::MessageGroup::user(msg.clone());
                current_group = Some(group);
            }

            MessageClassification::AssistantContent => {
                // Assistant content starts a new group
                flush_group(&mut groups, &mut current_group);
                let mut group = html_export::MessageGroup::assistant(msg.clone());

                // If assistant has embedded tool calls, add them
                if let Some(ref tc) = msg.tool_call {
                    let corr_id = extract_correlation_id(msg, format);
                    group.add_tool_call(tc.clone(), corr_id);
                    trace!(tool_name = %tc.name, "Added embedded tool call to assistant group");
                }
                current_group = Some(group);
            }

            MessageClassification::AssistantToolOnly => {
                // A tool-only assistant message may extend an assistant group
                // or an existing tool-call-only group. It must never be folded
                // into a user prompt or a standalone orphan-result group.
                let can_attach = current_group.as_ref().is_some_and(|group| {
                    matches!(group.group_type, html_export::MessageGroupType::Assistant)
                        || (matches!(group.group_type, html_export::MessageGroupType::ToolOnly)
                            && !group.tool_calls.is_empty())
                });
                if !can_attach {
                    flush_group(&mut groups, &mut current_group);
                    let mut group = html_export::MessageGroup::tool_only(msg.clone());
                    if let Some(ref tc) = msg.tool_call {
                        let corr_id = extract_correlation_id(msg, format);
                        group.add_tool_call(tc.clone(), corr_id);
                    }
                    current_group = Some(group);
                    trace!("Created new tool-only group");
                } else if let Some(ref mut group) = current_group
                    && let Some(ref tc) = msg.tool_call
                {
                    let corr_id = extract_correlation_id(msg, format);
                    group.add_tool_call(tc.clone(), corr_id);
                    group.update_end_timestamp(msg.timestamp.clone());
                    trace!(tool_name = %tc.name, "Attached tool call to current assistant group");
                }
            }

            MessageClassification::ToolResult => {
                // Tool results attach to the current group when correlation is
                // unambiguous. Preserve unmatched/orphaned results as their own
                // transcript group rather than silently dropping source bytes.
                let mut attached = false;
                if let Some(ref mut g) = current_group {
                    // Create a ToolResult from the message
                    let tool_name = msg
                        .tool_call
                        .as_ref()
                        .map(|tc| tc.name.clone())
                        .unwrap_or_else(|| "tool_result".to_string());

                    let content = tool_result_content(msg);

                    let status = msg
                        .tool_call
                        .as_ref()
                        .and_then(|tc| tc.status)
                        .unwrap_or(html_export::ToolStatus::Success);

                    let mut result =
                        html_export::ToolResult::new(tool_name.clone(), content, status);

                    // Try to get correlation ID
                    if let Some(corr_id) = extract_correlation_id(msg, format) {
                        result = result.with_correlation_id(corr_id);
                    }

                    attached = g.add_tool_result(result);
                    if attached {
                        g.update_end_timestamp(msg.timestamp.clone());
                        trace!(tool_name = %tool_name, "Added tool result to group");
                    }
                }

                if !attached {
                    debug!(
                        idx = idx,
                        "Preserving unmatched tool result as a standalone group"
                    );
                    flush_group(&mut groups, &mut current_group);
                    current_group = Some(standalone_tool_result_group(msg));
                }
            }

            MessageClassification::System => {
                // System messages are standalone groups
                flush_group(&mut groups, &mut current_group);
                let group = html_export::MessageGroup::system(msg.clone());
                groups.push(group);
                trace!("Added standalone system group");
            }

            MessageClassification::Empty => {
                trace!(idx = idx, "Skipping empty message");
            }
        }
    }

    // Flush any remaining group
    flush_group(&mut groups, &mut current_group);

    info!(
        group_count = groups.len(),
        original_messages = messages.len(),
        "Message grouping complete"
    );

    groups
}
