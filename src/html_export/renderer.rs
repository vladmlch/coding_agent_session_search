//! Conversation to HTML rendering.
//!
//! Converts session messages into semantic HTML markup with proper
//! role-based styling, agent-specific theming, and syntax highlighting support.
//!
//! # Features
//!
//! - **Role-based styling**: User, assistant, tool, and system messages
//! - **Agent-specific theming**: Visual differentiation for supported agents
//! - **Code blocks**: Semantic `language-*` classes for readable code markup
//! - **Tool calls**: Collapsible details with formatted JSON
//! - **Long message collapse**: Optional folding for lengthy content
//! - **XSS prevention**: All user content is properly escaped
//! - **Accessible**: Semantic HTML with ARIA attributes

mod agent_identity;
mod render_group;
mod render_message;
mod types;

pub use agent_identity::{agent_css_class, agent_display_name};
pub use render_group::render_message_groups;
pub use render_message::render_message;
pub use types::{
    Message, MessageGroup, MessageGroupType, RenderError, RenderOptions, ToolCall,
    ToolCallWithResult, ToolResult, ToolStatus,
};

#[cfg(test)]
mod tests;
