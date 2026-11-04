//! Version-one bounded JSONL. No partial result is ever written to stdout.
use std::io::{self, BufRead, Write};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const REQUEST_BYTES: usize = 65_536;
pub const RESPONSE_BYTES: usize = 1_048_576;
pub const PAGE_LIMIT: usize = 256;
pub const MAX_CURSORS: usize = 8;
pub const TIMEOUT_MS: u64 = 10_000;
pub const MAX_RAW_HITS: usize = 100_000;

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct Refusal {
    pub kind: &'static str,
    pub message: String,
}

pub fn refuse(kind: &'static str, message: impl Into<String>) -> anyhow::Error {
    Refusal {
        kind,
        message: message.into(),
    }
    .into()
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeFilters {
    #[serde(default)]
    pub agents: Vec<String>,
    #[serde(default)]
    pub workspaces: Vec<String>,
    pub source_id: Option<String>,
    pub created_from: Option<i64>,
    pub created_to: Option<i64>,
}

impl BridgeFilters {
    pub fn validate(&self) -> anyhow::Result<()> {
        let mut values = self
            .agents
            .iter()
            .chain(&self.workspaces)
            .chain(self.source_id.iter());
        if self.agents.len() > 32
            || self.workspaces.len() > 32
            || values.any(|s| s.is_empty() || s.len() > 4096 || s.trim() != s.as_str())
            || matches!((self.created_from, self.created_to), (Some(a), Some(b)) if a > b)
        {
            return Err(refuse(
                "invalid_request",
                "Invalid or excessive engine filters",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScanBudget {
    #[serde(default = "default_raw_hits")]
    pub max_raw_hits: usize,
    #[serde(default = "default_timeout")]
    pub deadline_ms: u64,
}

fn default_raw_hits() -> usize {
    MAX_RAW_HITS
}
fn default_timeout() -> u64 {
    TIMEOUT_MS
}
fn default_page() -> usize {
    PAGE_LIMIT
}

impl Default for ScanBudget {
    fn default() -> Self {
        Self {
            max_raw_hits: MAX_RAW_HITS,
            deadline_ms: TIMEOUT_MS,
        }
    }
}

impl ScanBudget {
    pub fn validate(&self) -> anyhow::Result<()> {
        if !(1..=MAX_RAW_HITS).contains(&self.max_raw_hits)
            || !(1..=TIMEOUT_MS).contains(&self.deadline_ms)
        {
            return Err(refuse(
                "invalid_request",
                "Scan budgets may only lower the profile limits",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Hello {
        id: u64,
    },
    Status {
        id: u64,
    },
    CatalogPage {
        id: u64,
        after_id: Option<i64>,
        #[serde(default = "default_page")]
        limit: usize,
    },
    Session {
        id: u64,
        conversation_id: i64,
    },
    MessagesPage {
        id: u64,
        conversation_id: i64,
        after_idx: Option<i64>,
        #[serde(default = "default_page")]
        limit: usize,
    },
    RankBegin {
        id: u64,
        query: String,
        #[serde(default)]
        filters: BridgeFilters,
        #[serde(default)]
        budget: ScanBudget,
    },
    RankNext {
        id: u64,
        cursor: String,
        max_hits: usize,
    },
    RankCancel {
        id: u64,
        cursor: String,
    },
    Reload {
        id: u64,
    },
    Shutdown {
        id: u64,
    },
}

impl Request {
    pub fn id(&self) -> u64 {
        match self {
            Self::Hello { id }
            | Self::Status { id }
            | Self::CatalogPage { id, .. }
            | Self::Session { id, .. }
            | Self::MessagesPage { id, .. }
            | Self::RankBegin { id, .. }
            | Self::RankNext { id, .. }
            | Self::RankCancel { id, .. }
            | Self::Reload { id }
            | Self::Shutdown { id } => *id,
        }
    }
}

pub fn page_limit(limit: usize) -> anyhow::Result<()> {
    if !(1..=PAGE_LIMIT).contains(&limit) {
        return Err(refuse("invalid_request", "Page limit must be 1..256"));
    }
    Ok(())
}

pub fn success(id: u64, result: Value) -> Value {
    json!({"schema_version":1, "id":id, "ok":true, "result":result})
}

pub fn failure(id: u64, error: &anyhow::Error) -> Value {
    let kind = error
        .downcast_ref::<Refusal>()
        .map_or("backend_read_failed", |e| e.kind);
    let incomplete = matches!(
        kind,
        "ranking_budget_exceeded" | "stale_snapshot" | "unmappable_hit"
    );
    json!({"schema_version":1,"id":id,"ok":false,"error":{
        "kind":kind,"message":error.to_string(),"enumeration_complete":if incomplete { Some(false) } else { None },
    }})
}

pub enum Frame {
    Line(Vec<u8>),
    End,
    TooLarge,
    Unterminated,
}

pub fn read_frame(input: &mut impl BufRead) -> io::Result<Frame> {
    let mut line = Vec::new();
    loop {
        let available = input.fill_buf()?;
        if available.is_empty() {
            return Ok(if line.is_empty() {
                Frame::End
            } else {
                Frame::Unterminated
            });
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let count = newline.unwrap_or(available.len());
        if count > REQUEST_BYTES - line.len() {
            return Ok(Frame::TooLarge);
        }
        line.extend_from_slice(&available[..count]);
        input.consume(count + usize::from(newline.is_some()));
        if newline.is_some() {
            return Ok(Frame::Line(line));
        }
    }
}

struct BoundedBuffer(Vec<u8>);
impl Write for BoundedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > RESPONSE_BYTES - 1 - self.0.len() {
            return Err(io::Error::other("Bridge response exceeds 1 MiB"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn encode_line(value: &impl Serialize) -> io::Result<Vec<u8>> {
    let mut buffer = BoundedBuffer(Vec::new());
    serde_json::to_writer(&mut buffer, value).map_err(io::Error::other)?;
    buffer.0.push(b'\n');
    Ok(buffer.0)
}

pub fn write_frame(output: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    output.write_all(bytes)?;
    output.flush()
}
