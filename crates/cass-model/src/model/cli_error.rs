//! Pure CLI error, wrap-config, and robot output format domain types.

use crate::model::cli_error_kind::ErrorKind as CliErrorKind;

/// Output format for robot/automation mode
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum RobotFormat {
    /// Pretty-printed JSON object (default, backward compatible)
    #[default]
    Json,
    /// Newline-delimited JSON: one object per line with optional _meta header
    Jsonl,
    /// Compact single-line JSON (no pretty printing)
    Compact,
    /// Session paths only: one source_path per line (for chained searches)
    Sessions,
    /// Token-Optimized Object Notation (encodes via toon crate)
    Toon,
}

#[derive(Debug, Clone)]
pub struct CliError {
    pub code: i32,
    pub kind: &'static str,
    pub message: String,
    pub hint: Option<String>,
    pub retryable: bool,
}

pub const CLI_ERROR_ALREADY_REPORTED_SENTINEL: &str = "__cass_error_already_reported__";

pub type CliResult<T = ()> = std::result::Result<T, CliError>;

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (code {})", self.message, self.code)
    }
}

impl std::error::Error for CliError {}

impl CliError {
    /// Stable robot error shape shared by ordinary and delegated commands.
    pub fn json_envelope(&self) -> serde_json::Value {
        serde_json::json!({
            "error": {
                "code": self.code,
                "kind": self.kind,
                "message": self.message,
                "hint": self.hint,
                "retryable": self.retryable,
            }
        })
    }

    pub fn already_reported(code: i32, kind: &'static str, retryable: bool) -> Self {
        CliError {
            code,
            kind,
            message: CLI_ERROR_ALREADY_REPORTED_SENTINEL.to_string(),
            hint: None,
            retryable,
        }
    }

    pub fn already_reported_from(err: &CliError) -> Self {
        CliError {
            code: err.code,
            kind: err.kind,
            message: CLI_ERROR_ALREADY_REPORTED_SENTINEL.to_string(),
            hint: err.hint.clone(),
            retryable: err.retryable,
        }
    }

    pub fn was_already_reported(&self) -> bool {
        self.message == CLI_ERROR_ALREADY_REPORTED_SENTINEL
    }

    pub fn usage(message: impl Into<String>, hint: Option<String>) -> Self {
        CliError {
            code: 2,
            kind: CliErrorKind::Usage.kind_str(),
            message: message.into(),
            hint,
            retryable: false,
        }
    }

    pub fn unknown(message: impl Into<String>) -> Self {
        CliError {
            code: 9,
            kind: CliErrorKind::Unknown.kind_str(),
            message: message.into(),
            hint: None,
            retryable: false,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct WrapConfig {
    pub width: Option<usize>,
    pub nowrap: bool,
}

impl WrapConfig {
    pub fn new(width: Option<usize>, nowrap: bool) -> Self {
        WrapConfig { width, nowrap }
    }

    pub fn effective_width(&self) -> Option<usize> {
        if self.nowrap { None } else { self.width }
    }
}
