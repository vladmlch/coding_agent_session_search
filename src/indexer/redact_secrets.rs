//! Re-export shim: canonical implementation lives in `crate::resource_plan::redact_secrets`.

#[allow(unused_imports)]
pub use crate::resource_plan::redact_secrets::*;
#[allow(unused_imports)]
pub(crate) use crate::resource_plan::redact_secrets::{
    ANTHROPIC_API_KEY_PATTERN, AWS_ACCESS_KEY_PATTERN, AWS_SECRET_KEY_PATTERN,
    AWS_SESSION_TOKEN_PATTERN, BEARER_TOKEN_PATTERN, DATABASE_URL_PATTERN,
    GENERIC_SECRET_ASSIGNMENT_PATTERN, GITHUB_TOKEN_PATTERN, JWT_PATTERN, MemoizingRedactor,
    OPENAI_API_KEY_PATTERN, PRIVATE_KEY_BLOCK_PATTERN, SLACK_TOKEN_PATTERN, STRIPE_KEY_PATTERN,
    insert_redacted_json_entry, is_sensitive_json_field,
};
