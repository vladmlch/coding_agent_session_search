//! OpenCode session facts from v2 row extras. The pinned FAD connector
//! renders every opencode session row into a flat content string and
//! archives the raw row's identity keys (`{message_id, session_id, type,
//! seq}`) in `extra_json` — verified against the 192 persisted opencode
//! messages (key/type structure only here, synthetic text in tests).
//!
//! Connector contract (pinned rev ad5fb23b, `v2_message_content`):
//!   - `user`/`synthetic` rows deliver `data.text` as user text;
//!   - `system` rows deliver `data.text` as system text;
//!   - `skill` rows deliver "[Skill: {name}]\n{text}" as tool-role text;
//!   - `shell` rows deliver "$ {command}\n{output}" as tool-role text;
//!   - `assistant` rows join their parts: text parts as-is (non-empty
//!     only), reasoning parts as "[Reasoning]\n{reasoning}", tool parts
//!     as "[Tool Output]\n{output}" built from the part state's text
//!     results — the part identity (`toolID`), input, status and error
//!     fields are NOT archived, and a tool part whose state carries no
//!     text results renders nothing at all;
//!   - completed `compaction` rows deliver "[Compaction Summary]
//!     \n{summary}" as assistant text; non-completed ones, `idle` rows
//!     and agent/model selection events are dropped before the packet;
//!   - the message author is `data.model.id` for assistant and
//!     compaction rows (the model alias), "user" for user rows, absent
//!     otherwise — the only model evidence the connector preserves.
//!
//! Consequences this extractor encodes:
//!   - a "[Tool Output]" marker proves a tool part existed and its
//!     output text is delivered inline, so the inventory fields degrade
//!     to `partial` (identity, input and state dropped), never complete;
//!   - an assistant row without a marker leaves the part's presence
//!     unverifiable (a no-text tool part renders nothing), so the same
//!     fields are `unavailable` for that record;
//!   - a marker in a plain text part is indistinguishable from a real
//!     one, so the degradation errs toward `partial`, never `complete`;
//!   - no tool call identity survives, so no tool facts are recorded
//!     and file paths are not derivable (`file_fact_count: None`);
//!   - the delivered text (reasoning, tool output, compaction summaries)
//!     stays fully searchable through the generic chat regions — this
//!     extractor adds no provider arm to `search_regions`;
//!   - a zero observed compaction count cannot certify the source had
//!     none (non-completed compactions drop pre-packet), so zero
//!     reports `None` with an explicit reason.

use std::collections::BTreeSet;

use serde_json::Value;

use crate::model::conversation_packet::ConversationPacket;

use super::classification::{self, ClassifyCall};
use super::{CORE_FIELDS, CoverageBuilder, ProviderExtraction, events};

const EXTRACTOR_REVISION: &str = "opencode-facts-v1";

const THINKING_MARKER: &str = "[Reasoning]\n";
const TOOL_MARKER: &str = "[Tool Output]\n";

/// Fields a "[Tool Output]" marker still certifies: the part's output text
/// is delivered inline; its identity, input and state are not.
const TOOL_PART_SCOPES: &[&str] = &["tools", "text_inventory", "block_inventory", "files"];
/// Fields the dropped part state (status/error) makes unverifiable.
const TOOL_STATE_SCOPES: &[&str] = &["tool_exit", "tool_error"];

pub(crate) fn extract(packet: &ConversationPacket) -> ProviderExtraction {
    let input = events(packet);
    let mut coverage = CoverageBuilder::new();
    let mut thinking = 0_i64;
    let mut compactions = 0_i64;
    let mut models: BTreeSet<String> = BTreeSet::new();
    let mut deduped: Vec<(i64, &str, &str)> = Vec::new();

    for (event, message) in input.iter().zip(packet.payload.messages.iter()) {
        deduped.push((event.idx, event.role, event.content));
        let extra = event.extra;
        if !extra.is_object() {
            coverage.unknown("opencode_extra_missing");
            continue;
        }
        if !event.content.is_empty() {
            coverage.evidence("chat");
        }
        // The connector lifts `data.model.id` into the message author for
        // assistant and compaction rows ("user" elsewhere) — the only
        // model evidence the extras carry.
        if let Some(model) = message
            .author
            .as_deref()
            .filter(|author| !author.is_empty() && *author != "user")
        {
            models.insert(model.to_string());
        }
        match extra.get("type").and_then(Value::as_str).unwrap_or("") {
            "assistant" => {
                let reasoning = event.content.matches(THINKING_MARKER).count() as i64;
                if reasoning > 0 {
                    thinking += reasoning;
                    coverage.evidence("thinking");
                }
                if event.content.contains(TOOL_MARKER) {
                    // A tool part provably existed and its output text is
                    // delivered inline; the part identity, input and state
                    // are dropped by the connector.
                    coverage.mark(
                        TOOL_PART_SCOPES,
                        "opencode_tool_part_dropped",
                        "partial",
                    );
                    coverage.mark(
                        TOOL_STATE_SCOPES,
                        "opencode_tool_state_dropped",
                        "unavailable",
                    );
                } else {
                    // No marker: a tool part whose state carried no text
                    // renders nothing, so the part's presence — and with
                    // it every tool fact — is unverifiable for this record.
                    coverage.mark(
                        TOOL_PART_SCOPES,
                        "opencode_tool_part_unverifiable",
                        "unavailable",
                    );
                    coverage.mark(
                        TOOL_STATE_SCOPES,
                        "opencode_tool_part_unverifiable",
                        "unavailable",
                    );
                }
            }
            // These rows render their whole text field(s) per the pinned
            // connector (data.text; "{name}\n{text}" for skill;
            // "$ {command}\n{output}" for shell) — nothing is dropped.
            "user" | "synthetic" | "system" | "skill" | "shell" => {}
            // Completed compactions render as assistant-role summary
            // content; non-completed ones drop pre-packet, so any
            // compaction-typed record here is completed.
            "compaction" => {
                compactions += 1;
            }
            // Idle rows and selection events drop before the packet;
            // anything else is a contract change — drift, never a guess.
            _ => coverage.mark(CORE_FIELDS, "opencode_record_unknown", "partial"),
        }
    }

    if let Some(model) = packet
        .payload
        .metadata_json
        .get("model")
        .and_then(Value::as_str)
        .filter(|model| !model.is_empty())
    {
        models.insert(model.to_string());
    }

    let classify_calls: Vec<ClassifyCall> = Vec::new();
    let (turn_count, step_count, first_prompt, task_category) =
        classification::session_turn_stats(&deduped, &classify_calls);
    let event_ts: std::collections::BTreeMap<i64, Option<i64>> = input
        .iter()
        .map(|event| (event.idx, event.created_at))
        .collect();
    let quality_events: Vec<classification::QualityEvent> = deduped
        .iter()
        .map(|&(idx, role, content)| classification::QualityEvent {
            idx,
            created_at: event_ts.get(&idx).copied().flatten(),
            role,
            content,
        })
        .collect();
    let quality = classification::session_quality_metrics(&quality_events, &classify_calls);

    // A zero count cannot certify the source had no compactions: the
    // connector drops non-completed compaction rows (no summary text is
    // lost — pending compactions carry none). Observed counts are exact.
    let compaction_count = if compactions > 0 {
        Some(compactions)
    } else {
        coverage.mark(
            &["compaction"],
            "opencode_compaction_pending_dropped",
            "unavailable",
        );
        None
    };
    ProviderExtraction {
        extractor: EXTRACTOR_REVISION,
        coverage,
        tools: Vec::new(),
        file_fact_count: None,
        thinking_block_count: Some(thinking),
        compaction_count,
        model_set: models.into_iter().collect(),
        is_subagent: None,
        parent_external_id: None,
        turn_count,
        step_count,
        first_prompt,
        task_category,
        quality: Some(quality),
    }
}
