//! Uncapped semantic message rows without a conversation-sized result buffer.
//! This is a read-only cursor. Its consumer must not issue reentrant SQL or
//! transaction control; packet construction and identity comparison need neither.

use crate::franken_sync::{Connection, check_connection_admission};
use crate::model::types::{Message, MessageRole};
use anyhow::{Context, Result};

const INDEXED: &str = "SELECT id, idx, role, author, created_at, content
    FROM messages INDEXED BY idx_messages_conv_idx
    WHERE conversation_id = ?1 ORDER BY idx";
const LEGACY: &str = "SELECT id, idx, role, author, created_at, content
    FROM messages WHERE conversation_id = ?1 ORDER BY idx";

fn message(row: &rusqlite::Row<'_>) -> rusqlite::Result<Message> {
    let role: String = row.get(2)?;
    Ok(Message {
        id: Some(row.get(0)?),
        idx: row.get(1)?,
        role: match role.as_str() {
            "user" => MessageRole::User,
            "agent" | "assistant" => MessageRole::Agent,
            "tool" => MessageRole::Tool,
            "system" => MessageRole::System,
            other => MessageRole::Other(other.to_owned()),
        },
        author: row.get(3)?,
        created_at: row.get(4)?,
        // Unlike lexical rebuilds, semantic identity requires the complete text.
        // Extra/raw-source envelopes and snippets do not enter this projection.
        content: row.get(5)?,
        extra_json: serde_json::Value::Null,
        snippets: Vec::new(),
    })
}

enum StepError {
    Sql(rusqlite::Error),
    Consumer(anyhow::Error),
}

pub(super) fn messages(
    connection: &Connection,
    conversation_id: i64,
    consume: &mut dyn FnMut(Message) -> Result<()>,
) -> Result<()> {
    let mut sql = INDEXED;
    loop {
        check_connection_admission()?;
        let raw = connection.try_raw()?;
        let mut saw_row = false;
        let step: std::result::Result<(), StepError> = (|| {
            let mut stmt = raw.prepare(sql).map_err(StepError::Sql)?;
            let mut rows = stmt.query([conversation_id]).map_err(StepError::Sql)?;
            while let Some(row) = rows.next().map_err(StepError::Sql)? {
                saw_row = true;
                let decoded = message(row).map_err(StepError::Sql)?;
                consume(decoded).map_err(StepError::Consumer)?;
            }
            Ok(())
        })();
        match step {
            Ok(()) => return Ok(()),
            Err(StepError::Consumer(error)) => return Err(error),
            Err(StepError::Sql(error))
                if sql == INDEXED
                    && !saw_row
                    && error
                        .to_string()
                        .contains("no such index: idx_messages_conv_idx") =>
            {
                // Only missing legacy schema metadata permits another query.
                // Never replay delivered rows or retry an arbitrary SQL error.
                sql = LEGACY;
            }
            Err(StepError::Sql(error)) => {
                return Err(error).context("stream canonical semantic message rows");
            }
        }
    }
}

#[cfg(test)]
mod tests;
