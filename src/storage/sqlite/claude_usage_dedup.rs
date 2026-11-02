//! Local fix (review 2026-11-02, U1).
//!
//! Claude Code writes one reply as several JSONL records that share
//! `message.id` + `requestId` and each repeat the reply's usage (input and
//! cache identical, output non-decreasing). CASS extracts usage per stored
//! message, so without this every reply was counted once per record. The
//! first record of a key keeps its usage; later records contribute only the
//! output growth. Records without a complete non-blank key stay independent
//! (FAD consumer contract, tests/claude_usage_identity.rs).

use std::collections::HashMap;

use serde_json::Value;

use crate::connectors::ExtractedTokenUsage;

fn nonblank_str<'a>(extra: &'a Value, pointers: [&str; 2]) -> Option<&'a str> {
    pointers
        .into_iter()
        .filter_map(|pointer| extra.pointer(pointer).and_then(Value::as_str))
        .map(str::trim)
        .find(|value| !value.is_empty())
}

pub(super) fn claude_reply_key(extra: &Value) -> Option<(String, String)> {
    let message_id = nonblank_str(extra, ["/cass/message_id", "/message/id"])?;
    let request_id = nonblank_str(extra, ["/cass/request_id", "/requestId"])?;
    Some((message_id.to_owned(), request_id.to_owned()))
}

fn digest(conversation_id: i64, key: &(String, String)) -> (i64, [u8; 32]) {
    let mut hasher = blake3::Hasher::new();
    hasher.update(key.0.as_bytes());
    hasher.update(&[0]);
    hasher.update(key.1.as_bytes());
    (conversation_id, *hasher.finalize().as_bytes())
}

#[derive(Default)]
pub(super) struct ClaudeReplyUsageDedup {
    max_output_seen: HashMap<(i64, [u8; 32]), i64>,
}

impl ClaudeReplyUsageDedup {
    /// Records an already-stored message so a later record of the same
    /// reply is treated as a repeat.
    pub(super) fn observe_stored(
        &mut self,
        conversation_id: i64,
        extra: &Value,
        output_tokens: Option<i64>,
    ) {
        let Some(key) = claude_reply_key(extra) else {
            return;
        };
        let seen = self
            .max_output_seen
            .entry(digest(conversation_id, &key))
            .or_insert(0);
        *seen = (*seen).max(output_tokens.unwrap_or(0));
    }

    pub(super) fn apply(
        &mut self,
        conversation_id: i64,
        extra: &Value,
        usage: &mut ExtractedTokenUsage,
    ) {
        let Some(key) = claude_reply_key(extra) else {
            return;
        };
        let output = usage.output_tokens.unwrap_or(0);
        match self.max_output_seen.get_mut(&digest(conversation_id, &key)) {
            None => {
                self.max_output_seen
                    .insert(digest(conversation_id, &key), output);
            }
            Some(seen) => {
                usage.input_tokens = None;
                usage.cache_read_tokens = None;
                usage.cache_creation_tokens = None;
                usage.thinking_tokens = None;
                usage.output_tokens = Some((output - *seen).max(0));
                *seen = (*seen).max(output);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record(message_id: &str, request_id: Option<&str>, input: i64, output: i64) -> Value {
        let mut value = json!({
            "message": {
                "id": message_id,
                "model": "claude-opus-4-6",
                "usage": {
                    "input_tokens": input,
                    "output_tokens": output,
                    "cache_read_input_tokens": 100,
                    "cache_creation_input_tokens": 20
                }
            }
        });
        if let Some(request_id) = request_id {
            value["requestId"] = json!(request_id);
        }
        value
    }

    fn extracted(extra: &Value) -> ExtractedTokenUsage {
        crate::connectors::extract_tokens_for_agent("claude_code", extra, "x", "assistant")
    }

    #[test]
    fn key_requires_both_nonblank_ids_and_prefers_compact_fields() {
        assert_eq!(
            claude_reply_key(&record("m1", Some("r1"), 1, 1)),
            Some(("m1".into(), "r1".into()))
        );
        assert_eq!(claude_reply_key(&record("m1", None, 1, 1)), None);
        assert_eq!(claude_reply_key(&record("m1", Some("  "), 1, 1)), None);
        let compact = json!({"cass": {"message_id": "cm", "request_id": "cr"}});
        assert_eq!(claude_reply_key(&compact), Some(("cm".into(), "cr".into())));
    }

    #[test]
    fn repeated_records_count_input_and_cache_once_and_output_as_max() {
        let mut dedup = ClaudeReplyUsageDedup::default();
        let mut totals = (0_i64, 0_i64, 0_i64, 0_i64);
        for extra in [
            record("m1", Some("r1"), 10, 3),
            record("m1", Some("r1"), 10, 7),
            record("m1", Some("r1"), 10, 7),
            record("m2", Some("r2"), 5, 2),
        ] {
            let mut usage = extracted(&extra);
            dedup.apply(1, &extra, &mut usage);
            totals.0 += usage.input_tokens.unwrap_or(0);
            totals.1 += usage.output_tokens.unwrap_or(0);
            totals.2 += usage.cache_read_tokens.unwrap_or(0);
            totals.3 += usage.cache_creation_tokens.unwrap_or(0);
        }
        assert_eq!(totals, (15, 9, 200, 40));
    }

    #[test]
    fn records_without_request_id_and_other_conversations_stay_independent() {
        let mut dedup = ClaudeReplyUsageDedup::default();
        let mut input = 0;
        for (conv, extra) in [
            (1, record("m3", None, 4, 1)),
            (1, record("m3", None, 4, 1)),
            (1, record("m1", Some("r1"), 10, 3)),
            (2, record("m1", Some("r1"), 10, 3)),
        ] {
            let mut usage = extracted(&extra);
            dedup.apply(conv, &extra, &mut usage);
            input += usage.input_tokens.unwrap_or(0);
        }
        assert_eq!(input, 4 + 4 + 10 + 10);
    }

    #[test]
    fn stored_records_seed_the_state() {
        let mut dedup = ClaudeReplyUsageDedup::default();
        let first = record("m1", Some("r1"), 10, 3);
        dedup.observe_stored(1, &first, Some(3));
        let later = record("m1", Some("r1"), 10, 7);
        let mut usage = extracted(&later);
        dedup.apply(1, &later, &mut usage);
        assert_eq!(usage.input_tokens, None);
        assert_eq!(usage.output_tokens, Some(4));
    }
}
