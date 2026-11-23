use super::agent_identity::{ICON_CHECK, ICON_LOADER, ICON_X};

/// Errors that can occur during rendering.
#[derive(Debug, thiserror::Error)]
pub enum RenderError {
    /// Invalid message data
    #[error("invalid message: {0}")]
    InvalidMessage(String),
    /// Content parsing failed
    #[error("parse error: {0}")]
    ParseError(String),
}

/// Options for rendering conversations.
#[derive(Debug, Clone)]
pub struct RenderOptions {
    /// Show message timestamps
    pub show_timestamps: bool,

    /// Show tool call details
    pub show_tool_calls: bool,

    /// Emit `language-*` class markers on code blocks
    pub syntax_highlighting: bool,

    /// Wrap long lines in code blocks
    pub wrap_code: bool,

    /// Collapse messages longer than this threshold (characters)
    /// Set to 0 to disable collapsing
    pub collapse_threshold: usize,

    /// Maximum lines to show in collapsed code blocks preview
    pub code_preview_lines: usize,

    /// Agent slug for agent-specific styling
    pub agent_slug: Option<String>,
}

impl Default for RenderOptions {
    fn default() -> Self {
        Self {
            show_timestamps: true,
            show_tool_calls: true,
            syntax_highlighting: true,
            wrap_code: false,
            collapse_threshold: 0, // Disabled by default
            code_preview_lines: 20,
            agent_slug: None,
        }
    }
}

/// A message to render.
#[derive(Debug, Clone)]
pub struct Message {
    /// Role: user, assistant, tool, system
    pub role: String,

    /// Message content (may contain markdown)
    pub content: String,

    /// Optional timestamp (ISO 8601)
    pub timestamp: Option<String>,

    /// Optional tool call information
    pub tool_call: Option<ToolCall>,

    /// Optional message index for anchoring
    pub index: Option<usize>,

    /// Optional author name (for multi-participant sessions)
    pub author: Option<String>,
}

/// Tool call information.
#[derive(Debug, Clone)]
pub struct ToolCall {
    /// Tool name (e.g., "Bash", "Read", "Write")
    pub name: String,

    /// Tool input/arguments (usually JSON)
    pub input: String,

    /// Tool output/result
    pub output: Option<String>,

    /// Execution status (success, error, etc.)
    pub status: Option<ToolStatus>,

    /// Provider correlation ID linking a tool call to its later result.
    pub correlation_id: Option<String>,
}

/// Status of a tool execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolStatus {
    Success,
    Error,
    Pending,
}

impl ToolStatus {
    pub(super) fn css_class(&self) -> &'static str {
        match self {
            ToolStatus::Success => "tool-status-success",
            ToolStatus::Error => "tool-status-error",
            ToolStatus::Pending => "tool-status-pending",
        }
    }

    pub(super) fn icon_svg(&self) -> &'static str {
        match self {
            ToolStatus::Success => ICON_CHECK,
            ToolStatus::Error => ICON_X,
            ToolStatus::Pending => ICON_LOADER,
        }
    }

    pub(super) fn label(&self) -> &'static str {
        match self {
            ToolStatus::Success => "success",
            ToolStatus::Error => "error",
            ToolStatus::Pending => "pending",
        }
    }
}

// ============================================
// Message Grouping Types for Consolidated Rendering
// ============================================
/// Type of message group for rendering decisions.
///
/// Determines how a group of related messages should be styled and displayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageGroupType {
    /// User-initiated message (question, instruction, etc.)
    User,
    /// Assistant/agent response with potential tool calls
    Assistant,
    /// System message (context, instructions)
    System,
    /// Orphan tool calls without a parent assistant message
    ToolOnly,
}

impl MessageGroupType {
    /// Get the role icon for this group type.
    pub fn role_icon(&self) -> &'static str {
        match self {
            MessageGroupType::User => "user",
            MessageGroupType::Assistant => "assistant",
            MessageGroupType::System => "system",
            MessageGroupType::ToolOnly => "tool",
        }
    }
}

/// Extended tool result with status and content.
///
/// Represents the output from a tool execution, paired with metadata
/// for correlation and status tracking.
#[derive(Debug, Clone)]
pub struct ToolResult {
    /// Tool name this result responds to.
    pub tool_name: String,
    /// Result content (may be truncated for display).
    pub content: String,
    /// Execution status.
    pub status: ToolStatus,
    /// Correlation ID to match with the originating call (e.g., tool_use_id).
    pub correlation_id: Option<String>,
}

impl ToolResult {
    /// Create a new tool result.
    pub fn new(
        tool_name: impl Into<String>,
        content: impl Into<String>,
        status: ToolStatus,
    ) -> Self {
        Self {
            tool_name: tool_name.into(),
            content: content.into(),
            status,
            correlation_id: None,
        }
    }

    /// Set the correlation ID for matching with tool calls.
    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Check if this result indicates an error.
    pub fn is_error(&self) -> bool {
        self.status == ToolStatus::Error
    }
}

/// Tool call paired with its result for correlation.
///
/// Keeps a tool invocation together with its response, enabling
/// consolidated rendering of the complete tool interaction.
#[derive(Debug, Clone)]
pub struct ToolCallWithResult {
    /// The original tool call.
    pub call: ToolCall,
    /// The result (if received).
    pub result: Option<ToolResult>,
    /// Correlation ID (tool_use_id in Claude format).
    pub correlation_id: Option<String>,
}

impl ToolCallWithResult {
    /// Create a new tool call without a result yet.
    pub fn new(call: ToolCall) -> Self {
        let correlation_id = call.correlation_id.clone();
        Self {
            call,
            result: None,
            correlation_id,
        }
    }

    /// Set the correlation ID.
    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Attach a result to this tool call.
    pub fn with_result(mut self, result: ToolResult) -> Self {
        self.result = Some(result);
        self
    }

    /// Check if this tool call has a result.
    pub fn has_result(&self) -> bool {
        self.result.is_some()
    }

    /// Check if the tool call resulted in an error.
    pub fn is_error(&self) -> bool {
        self.result.as_ref().is_some_and(|r| r.is_error())
    }

    /// Get the effective status (from result or call).
    pub fn effective_status(&self) -> ToolStatus {
        self.result
            .as_ref()
            .map(|r| r.status)
            .or(self.call.status)
            .unwrap_or(ToolStatus::Pending)
    }
}

/// A group of related messages for consolidated rendering.
///
/// Represents a logical unit of conversation: a primary message (user question
/// or assistant response) along with all associated tool calls and their results.
/// This enables rendering an entire interaction as a cohesive block rather than
/// separate messages.
#[derive(Debug, Clone)]
pub struct MessageGroup {
    /// Group type for rendering decisions.
    pub group_type: MessageGroupType,
    /// The primary message (user or assistant text).
    pub primary: Message,
    /// Tool calls paired with their results.
    pub tool_calls: Vec<ToolCallWithResult>,
    /// Timestamp of the first message/action in this group.
    pub start_timestamp: Option<String>,
    /// Timestamp of the last message/action in this group.
    pub end_timestamp: Option<String>,
}

impl MessageGroup {
    /// Create a new message group with a primary message.
    pub fn new(primary: Message, group_type: MessageGroupType) -> Self {
        let end_timestamp = primary.timestamp.clone();
        let start_timestamp = primary.timestamp.clone();
        Self {
            group_type,
            primary,
            tool_calls: Vec::new(),
            start_timestamp,
            end_timestamp,
        }
    }

    /// Create a user message group.
    pub fn user(primary: Message) -> Self {
        Self::new(primary, MessageGroupType::User)
    }

    /// Create an assistant message group.
    pub fn assistant(primary: Message) -> Self {
        Self::new(primary, MessageGroupType::Assistant)
    }

    /// Create a system message group.
    pub fn system(primary: Message) -> Self {
        Self::new(primary, MessageGroupType::System)
    }

    /// Create a tool-only group (orphan tool calls).
    pub fn tool_only(primary: Message) -> Self {
        Self::new(primary, MessageGroupType::ToolOnly)
    }

    /// Add a tool call to this group.
    pub fn add_tool_call(&mut self, call: ToolCall, correlation_id: Option<String>) {
        tracing::trace!(
            tool_name = %call.name,
            correlation_id = ?correlation_id,
            "Adding tool call to message group"
        );
        let mut tc = ToolCallWithResult::new(call);
        if let Some(id) = correlation_id {
            tc = tc.with_correlation_id(id);
        }
        self.tool_calls.push(tc);
    }

    /// Add a tool result, matching it with an existing call by correlation ID.
    ///
    /// If a matching call is found, the result is attached to it.
    /// If no match is found, returns `false` so the caller can preserve the
    /// result as a standalone transcript entry instead of silently losing it.
    pub fn add_tool_result(&mut self, result: ToolResult) -> bool {
        // Try to match by correlation ID first
        if let Some(ref corr_id) = result.correlation_id {
            for tc in &mut self.tool_calls {
                if tc.correlation_id.as_ref() == Some(corr_id) {
                    tracing::trace!(
                        tool_name = %result.tool_name,
                        correlation_id = %corr_id,
                        "Matched tool result to call"
                    );
                    tc.result = Some(result);
                    return true;
                }
            }
            tracing::warn!(
                tool_name = %result.tool_name,
                correlation_id = %corr_id,
                "Could not match correlated tool result to any call"
            );
            return false;
        }

        // Fall back to matching by tool name (first unmatched call)
        for tc in &mut self.tool_calls {
            if tc.result.is_none() && tc.call.name == result.tool_name {
                tracing::trace!(
                    tool_name = %result.tool_name,
                    "Matched tool result to call by name"
                );
                tc.result = Some(result);
                return true;
            }
        }

        tracing::warn!(
            tool_name = %result.tool_name,
            correlation_id = ?result.correlation_id,
            "Could not match tool result to any call"
        );
        false
    }

    /// Update the end timestamp if the given timestamp is later.
    pub fn update_end_timestamp(&mut self, timestamp: Option<String>) {
        if let Some(ts) = timestamp {
            match (&self.end_timestamp, &ts) {
                (Some(existing), new) if new > existing => {
                    self.end_timestamp = Some(ts);
                }
                (None, _) => {
                    self.end_timestamp = Some(ts);
                }
                _ => {}
            }
        }
    }

    /// Get the number of tool calls in this group.
    pub fn tool_count(&self) -> usize {
        self.tool_calls.len()
    }

    /// Check if any tool call in this group resulted in an error.
    pub fn has_errors(&self) -> bool {
        self.tool_calls.iter().any(|tc| tc.is_error())
    }

    /// Check if all tool calls have results.
    pub fn all_tools_complete(&self) -> bool {
        self.tool_calls.iter().all(|tc| tc.has_result())
    }

    /// Get a summary of tool call statuses for display.
    pub fn tool_summary(&self) -> (usize, usize, usize) {
        let mut success = 0;
        let mut error = 0;
        let mut pending = 0;
        for tc in &self.tool_calls {
            match tc.effective_status() {
                ToolStatus::Success => success += 1,
                ToolStatus::Error => error += 1,
                ToolStatus::Pending => pending += 1,
            }
        }
        (success, error, pending)
    }
}
