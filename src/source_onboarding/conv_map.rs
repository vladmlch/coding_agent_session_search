use rayon::prelude::*;

use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::model::types::{Conversation, Message, MessageRole, Snippet};
use crate::resource_plan::redact_secrets::{
    MemoizingRedactor, redact_json, redact_text, redaction_enabled,
};

pub(crate) fn map_role(role: &str) -> MessageRole {
    match role {
        "user" => MessageRole::User,
        "assistant" | "agent" => MessageRole::Agent,
        "tool" => MessageRole::Tool,
        "system" => MessageRole::System,
        other => MessageRole::Other(other.to_string()),
    }
}

/// Extract provenance (source_id, origin_host) from conversation metadata.
///
/// Looks for `metadata.cass.origin` object with source_id/kind/host fields and
/// normalizes them the same way the lexical index does so host-only remote
/// metadata does not get misclassified as local during persistence.
pub(crate) fn extract_provenance(metadata: &serde_json::Value) -> (String, Option<String>) {
    let cass_origin = metadata.get("cass").and_then(|c| c.get("origin"));
    let raw_source_id = cass_origin
        .and_then(|o| o.get("source_id"))
        .and_then(|v| v.as_str());
    let raw_origin_kind = cass_origin
        .and_then(|o| o.get("kind"))
        .and_then(|v| v.as_str());
    let origin_host = crate::model::origin_identity::normalized_index_origin_host(
        cass_origin
            .and_then(|o| o.get("host"))
            .and_then(|v| v.as_str()),
    );
    let source_id = crate::model::origin_identity::normalized_index_source_id(
        raw_source_id,
        raw_origin_kind,
        origin_host.as_deref(),
    );

    (source_id, origin_host)
}

/// Convert a NormalizedConversation to the internal Conversation type for SQLite storage.
///
/// Extracts provenance from `metadata.cass.origin` if present, otherwise defaults to local.
///
/// Applies secret redaction to message content and extra_json before storage
/// (security fix for #112: tool-result secrets were persisted unredacted).
pub fn map_to_internal(conv: &NormalizedConversation) -> Conversation {
    map_to_internal_with_redactor(conv, None)
}

/// gh373 third variant (bead oeu5a): one activity tick per this many
/// mapped messages. A single giant conversation (observed: 60-90MB codex
/// rollouts, ~40k messages) spends minutes inside
/// `map_to_internal_with_redactor` — allocator-heavy secret redaction on
/// one thread, zero WAL writes — which froze all three watchdog atomics
/// and drew a spurious exit(70) at the 300s abort threshold. The stride
/// keeps ticks cheap (one relaxed atomic add per 256 messages) while
/// bounding the silent window to well under any sane threshold.
pub(crate) const MAP_HEARTBEAT_MESSAGE_STRIDE: usize = 256;
pub(crate) const INNER_REDACTION_MIN_MESSAGES: usize = 512;
pub(crate) const INNER_REDACTION_MIN_TEXT_BYTES: usize = 1024 * 1024;

pub(crate) fn use_inner_message_redaction(convs: &[NormalizedConversation]) -> bool {
    let [conv] = convs else {
        return false;
    };
    if !redaction_enabled()
        || rayon::current_num_threads() <= 1
        || conv.messages.len() < INNER_REDACTION_MIN_MESSAGES
    {
        return false;
    }
    let mut bytes = 0usize;
    for message in &conv.messages {
        bytes = bytes.saturating_add(message.content.len());
        if bytes >= INNER_REDACTION_MIN_TEXT_BYTES {
            return true;
        }
        for snippet in &message.snippets {
            bytes = bytes.saturating_add(snippet.snippet_text.as_ref().map_or(0, String::len));
            if bytes >= INNER_REDACTION_MIN_TEXT_BYTES {
                return true;
            }
        }
    }
    false
}

pub(crate) fn map_to_internal_with_redactor(
    conv: &NormalizedConversation,
    redactor: Option<&mut MemoizingRedactor>,
) -> Conversation {
    map_to_internal_with_redactor_and_callback(conv, redactor, || {})
}

pub(crate) fn map_batch_to_internal_with_callback(
    convs: &[NormalizedConversation],
    on_tick: impl Fn() + Sync,
) -> Vec<Conversation> {
    if use_inner_message_redaction(convs) {
        let mut redactor = MemoizingRedactor::new();
        return vec![map_to_internal_with_message_parallelism_and_callback(
            &convs[0],
            Some(&mut redactor),
            &on_tick,
            true,
        )];
    }
    convs
        .par_iter()
        .map_init(MemoizingRedactor::new, |redactor, conv| {
            map_to_internal_with_redactor_and_callback(conv, Some(redactor), &on_tick)
        })
        .collect()
}

pub(crate) fn map_to_internal_with_redactor_and_callback(
    conv: &NormalizedConversation,
    redactor: Option<&mut MemoizingRedactor>,
    on_tick: impl Fn() + Sync,
) -> Conversation {
    map_to_internal_with_message_parallelism_and_callback(conv, redactor, on_tick, false)
}

/// Persist `NormalizedMessage.invocations` into a message's `extra` under
/// the `cass.invocations` shape FAD's Antigravity connector dual-writes:
/// `{"cass":{"invocations":[NormalizedInvocation]}}`.
///
/// FAD populates `invocations` for Claude `tool_use` blocks, Codex
/// `function_call`/`custom_tool_call`/`event_msg tool_call` items, Devin
/// `tool_calls[]`, Antigravity planner and MODEL steps, and the
/// Pi-family assistant messages, among other connectors — but the field
/// was never archived
/// (FAD's `planner_calls.rs` notes "CASS keeps `extra` but not
/// `NormalizedMessage.invocations`"). Merging here — the single seam
/// every persisted message crosses, *before* redaction — makes tool
/// calls durable for every provider under one extraction shape
/// without resurrecting payloads the connectors dropped: only what
/// `invocations` still carries is stored, and arguments flow through
/// the same `redact_json` pass as the rest of `extra`.
///
/// `Cow::Borrowed` on every skip path means the overwhelmingly common
/// empty-invocations case adds no allocation beyond the clone the persist
/// path already performed.
fn merge_invocations_into_extra<'a>(
    extra: &'a serde_json::Value,
    invocations: &[franken_agent_detection::NormalizedInvocation],
) -> std::borrow::Cow<'a, serde_json::Value> {
    // Skip when there is nothing to persist; when `extra.cass.invocations`
    // already exists (Antigravity's planner dual-write, or a source line
    // re-ingested from a recovered archive); when `extra` is not an
    // object/null and cannot host the `cass` subtree without inventing a
    // wrapper shape; or when `extra.cass` exists as a non-object the
    // merge would leave untouched anyway.
    if invocations.is_empty()
        || extra
            .get("cass")
            .and_then(|cass| cass.get("invocations"))
            .is_some()
        || (!extra.is_null() && !extra.is_object())
        || extra.get("cass").is_some_and(|cass| !cass.is_object())
    {
        return std::borrow::Cow::Borrowed(extra);
    }
    // Serialize before mutating: a serialization failure leaves `extra`
    // untouched rather than writing `invocations: []` a later reader
    // would mistake for an authoritative empty list.
    let Ok(serialized) = serde_json::to_value(invocations) else {
        return std::borrow::Cow::Borrowed(extra);
    };
    let mut merged = extra.as_object().cloned().unwrap_or_default();
    {
        let cass_entry = merged
            .entry("cass".to_string())
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
        // A non-object `cass` value is upstream data; never clobber it —
        // the entry is simply left as-is.
        if let Some(cass_object) = cass_entry.as_object_mut() {
            cass_object.insert("invocations".to_string(), serialized);
        }
    }
    std::borrow::Cow::Owned(serde_json::Value::Object(merged))
}

pub(crate) fn map_message_to_internal_with_callback(
    m: &NormalizedMessage,
    mapped_message_index: usize,
    should_redact: bool,
    mut redactor: Option<&mut MemoizingRedactor>,
    on_tick: impl Fn(),
) -> Message {
    if mapped_message_index.is_multiple_of(MAP_HEARTBEAT_MESSAGE_STRIDE) {
        on_tick();
    }
    let content = if should_redact {
        if let Some(r) = redactor.as_mut() {
            r.redact_text(&m.content)
        } else {
            redact_text(&m.content).into_owned()
        }
    } else {
        m.content.clone()
    };
    // Task 7 (index-first export): archive `m.invocations` under
    // `extra.cass.invocations` so index-only export recovers tool calls
    // for every provider — merged BEFORE redaction so invocation
    // arguments flow through the same secret scan as the rest of `extra`.
    let extra = merge_invocations_into_extra(&m.extra, &m.invocations);
    let extra_json = if should_redact {
        if let Some(r) = redactor.as_mut() {
            r.redact_json(&extra)
        } else {
            redact_json(&extra)
        }
    } else {
        extra.into_owned()
    };
    Message {
        id: None,
        idx: m.idx,
        role: map_role(&m.role),
        author: m.author.clone(),
        created_at: m.created_at,
        content,
        extra_json,
        snippets: m
            .snippets
            .iter()
            .map(|s| Snippet {
                id: None,
                file_path: s.file_path.clone(),
                start_line: s.start_line,
                end_line: s.end_line,
                language: s.language.clone(),
                snippet_text: s.snippet_text.as_ref().map(|snippet_text| {
                    if should_redact {
                        if let Some(r) = redactor.as_mut() {
                            r.redact_text(snippet_text)
                        } else {
                            redact_text(snippet_text).into_owned()
                        }
                    } else {
                        snippet_text.clone()
                    }
                }),
            })
            .collect(),
    }
}

pub(crate) fn map_to_internal_with_message_parallelism_and_callback(
    conv: &NormalizedConversation,
    mut redactor: Option<&mut MemoizingRedactor>,
    on_tick: impl Fn() + Sync,
    parallel_messages: bool,
) -> Conversation {
    // Extract provenance from metadata (P2.2)
    let (source_id, origin_host) = extract_provenance(&conv.metadata);
    let should_redact = redaction_enabled();

    Conversation {
        id: None,
        agent_slug: conv.agent_slug.clone(),
        workspace: conv.workspace.clone(),
        external_id: conv.external_id.clone(),
        title: if should_redact {
            conv.title.as_ref().map(|t| {
                if let Some(r) = redactor.as_mut() {
                    r.redact_text(t)
                } else {
                    redact_text(t).into_owned()
                }
            })
        } else {
            conv.title.clone()
        },
        source_path: conv.source_path.clone(),
        started_at: conv.started_at,
        ended_at: conv.ended_at,
        approx_tokens: None,
        metadata_json: if should_redact {
            if let Some(r) = redactor.as_mut() {
                r.redact_json(&conv.metadata)
            } else {
                redact_json(&conv.metadata)
            }
        } else {
            conv.metadata.clone()
        },
        messages: if parallel_messages {
            // At most one chunk/redactor per pool thread. Indexed chunks
            // and ordered collection preserve the original message order.
            let chunk_size = conv.messages.len().div_ceil(rayon::current_num_threads());
            conv.messages
                .par_chunks(chunk_size)
                .enumerate()
                .map(|(chunk_index, chunk)| {
                    let mut worker_redactor = MemoizingRedactor::new();
                    chunk
                        .iter()
                        .enumerate()
                        .map(|(index, message)| {
                            map_message_to_internal_with_callback(
                                message,
                                chunk_index * chunk_size + index,
                                should_redact,
                                Some(&mut worker_redactor),
                                &on_tick,
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
                .into_iter()
                .flatten()
                .collect()
        } else {
            conv.messages
                .iter()
                .enumerate()
                .map(|(index, message)| {
                    map_message_to_internal_with_callback(
                        message,
                        index,
                        should_redact,
                        redactor.as_deref_mut(),
                        &on_tick,
                    )
                })
                .collect()
        },
        source_id,
        origin_host,
    }
}
