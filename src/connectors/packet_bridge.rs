//! Connector-to-packet bridge converting normalized connector conversations into
//! `ConversationPacket` values without introducing a `model -> connectors` dependency.

use crate::connectors::{NormalizedConversation, NormalizedMessage, NormalizedSnippet};
use crate::model::conversation_packet::{
    ConversationPacket, ConversationPacketBuilder, ConversationPacketIdentity,
    ConversationPacketMessage, ConversationPacketPayload, ConversationPacketProvenance,
    ConversationPacketSnippet, normalize_role, path_to_packet_string, timestamps_from_parts,
};

/// Build a [`ConversationPacket`] from a raw normalized connector conversation.
#[must_use]
pub fn conversation_packet_from_normalized(
    conversation: &NormalizedConversation,
    provenance: ConversationPacketProvenance,
) -> ConversationPacket {
    let messages = conversation
        .messages
        .iter()
        .map(packet_message_from_normalized)
        .collect::<Vec<_>>();
    let payload = ConversationPacketPayload {
        identity: ConversationPacketIdentity {
            conversation_id: None,
            agent_slug: conversation.agent_slug.clone(),
            external_id: conversation.external_id.clone(),
            workspace: conversation.workspace.as_deref().map(path_to_packet_string),
            source_path: path_to_packet_string(&conversation.source_path),
            title: conversation.title.clone(),
        },
        provenance,
        timestamps: timestamps_from_parts(
            conversation.started_at,
            conversation.ended_at,
            &messages,
        ),
        metadata_json: conversation.metadata.clone(),
        messages,
    };
    ConversationPacket::from_payload(payload, ConversationPacketBuilder::RawConnectorScan)
}

#[must_use]
pub fn packet_message_from_normalized(message: &NormalizedMessage) -> ConversationPacketMessage {
    ConversationPacketMessage {
        message_id: None,
        idx: message.idx,
        role: normalize_role(&message.role),
        author: message.author.clone(),
        created_at: message.created_at,
        content: message.content.clone(),
        extra_json: message.extra.clone(),
        snippets: message
            .snippets
            .iter()
            .map(packet_snippet_from_normalized)
            .collect(),
    }
}

#[must_use]
pub fn packet_snippet_from_normalized(snippet: &NormalizedSnippet) -> ConversationPacketSnippet {
    ConversationPacketSnippet {
        file_path: snippet.file_path.as_deref().map(path_to_packet_string),
        start_line: snippet.start_line,
        end_line: snippet.end_line,
        language: snippet.language.clone(),
        snippet_text: snippet.snippet_text.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::conversation_packet::CONVERSATION_PACKET_VERSION;
    use crate::model::packet_audit::{
        PacketEquivalenceAuditor, PacketEquivalenceOutcome, PacketEquivalenceTolerance,
        PacketHashDifference, PacketProjectionDifference,
    };
    use crate::model::types::{Conversation, Message, MessageRole, Snippet};
    use serde_json::json;
    use std::path::PathBuf;

    fn raw_conversation() -> NormalizedConversation {
        NormalizedConversation {
            agent_slug: "codex".to_string(),
            external_id: Some("session-1".to_string()),
            title: Some("Packet contract".to_string()),
            workspace: Some(PathBuf::from("/work/cass")),
            source_path: PathBuf::from("/work/cass/.codex/session.jsonl"),
            started_at: Some(1_700_000_000_000),
            ended_at: Some(1_700_000_010_000),
            metadata: json!({"model": "gpt-5", "temperature": 0}),
            messages: vec![
                NormalizedMessage {
                    idx: 0,
                    role: "user".to_string(),
                    author: Some("human".to_string()),
                    created_at: Some(1_700_000_000_000),
                    content: "build the packet".to_string(),
                    extra: json!({"turn": 1}),
                    snippets: vec![NormalizedSnippet {
                        file_path: Some(PathBuf::from("src/main.rs")),
                        start_line: Some(10),
                        end_line: Some(12),
                        language: Some("rust".to_string()),
                        snippet_text: Some("fn main() {}".to_string()),
                    }],
                    invocations: Vec::new(),
                },
                NormalizedMessage {
                    idx: 1,
                    role: "assistant".to_string(),
                    author: None,
                    created_at: Some(1_700_000_001_000),
                    content: "packet built".to_string(),
                    extra: json!({}),
                    snippets: Vec::new(),
                    invocations: Vec::new(),
                },
            ],
        }
    }

    fn canonical_conversation() -> Conversation {
        Conversation {
            id: Some(42),
            agent_slug: "codex".to_string(),
            workspace: Some(PathBuf::from("/work/cass")),
            external_id: Some("session-1".to_string()),
            title: Some("Packet contract".to_string()),
            source_path: PathBuf::from("/work/cass/.codex/session.jsonl"),
            started_at: Some(1_700_000_000_000),
            ended_at: Some(1_700_000_010_000),
            approx_tokens: None,
            metadata_json: json!({"model": "gpt-5", "temperature": 0}),
            source_id: "local".to_string(),
            origin_host: None,
            messages: vec![
                Message {
                    id: Some(100),
                    idx: 0,
                    role: MessageRole::User,
                    author: Some("human".to_string()),
                    created_at: Some(1_700_000_000_000),
                    content: "build the packet".to_string(),
                    extra_json: json!({"turn": 1}),
                    snippets: vec![Snippet {
                        id: Some(7),
                        file_path: Some(PathBuf::from("src/main.rs")),
                        start_line: Some(10),
                        end_line: Some(12),
                        language: Some("rust".to_string()),
                        snippet_text: Some("fn main() {}".to_string()),
                    }],
                },
                Message {
                    id: Some(101),
                    idx: 1,
                    role: MessageRole::Agent,
                    author: None,
                    created_at: Some(1_700_000_001_000),
                    content: "packet built".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
            ],
        }
    }

    fn audit_raw_conversation() -> NormalizedConversation {
        NormalizedConversation {
            agent_slug: "codex".to_string(),
            external_id: Some("session-audit".to_string()),
            title: Some("Audit fixture".to_string()),
            workspace: Some(PathBuf::from("/work/audit")),
            source_path: PathBuf::from("/work/audit/.codex/session.jsonl"),
            started_at: Some(1_700_000_000_000),
            ended_at: Some(1_700_000_010_000),
            metadata: json!({"model": "gpt-5"}),
            messages: vec![
                NormalizedMessage {
                    idx: 0,
                    role: "user".to_string(),
                    author: Some("human".to_string()),
                    created_at: Some(1_700_000_000_000),
                    content: "audit the live persist sink".to_string(),
                    extra: json!({"turn": 1}),
                    snippets: vec![NormalizedSnippet {
                        file_path: Some(PathBuf::from("src/audit.rs")),
                        start_line: Some(1),
                        end_line: Some(1),
                        language: Some("rust".to_string()),
                        snippet_text: Some("// audit".to_string()),
                    }],
                    invocations: Vec::new(),
                },
                NormalizedMessage {
                    idx: 1,
                    role: "assistant".to_string(),
                    author: None,
                    created_at: Some(1_700_000_001_000),
                    content: "auditing".to_string(),
                    extra: json!({}),
                    snippets: Vec::new(),
                    invocations: Vec::new(),
                },
            ],
        }
    }

    fn audit_canonical_conversation() -> Conversation {
        Conversation {
            id: Some(7),
            agent_slug: "codex".to_string(),
            workspace: Some(PathBuf::from("/work/audit")),
            external_id: Some("session-audit".to_string()),
            title: Some("Audit fixture".to_string()),
            source_path: PathBuf::from("/work/audit/.codex/session.jsonl"),
            started_at: Some(1_700_000_000_000),
            ended_at: Some(1_700_000_010_000),
            approx_tokens: None,
            metadata_json: json!({"model": "gpt-5"}),
            source_id: "local".to_string(),
            origin_host: None,
            messages: vec![
                Message {
                    id: Some(70),
                    idx: 0,
                    role: MessageRole::User,
                    author: Some("human".to_string()),
                    created_at: Some(1_700_000_000_000),
                    content: "audit the live persist sink".to_string(),
                    extra_json: json!({"turn": 1}),
                    snippets: vec![Snippet {
                        id: Some(700),
                        file_path: Some(PathBuf::from("src/audit.rs")),
                        start_line: Some(1),
                        end_line: Some(1),
                        language: Some("rust".to_string()),
                        snippet_text: Some("// audit".to_string()),
                    }],
                },
                Message {
                    id: Some(71),
                    idx: 1,
                    role: MessageRole::Agent,
                    author: None,
                    created_at: Some(1_700_000_001_000),
                    content: "auditing".to_string(),
                    extra_json: json!({}),
                    snippets: Vec::new(),
                },
            ],
        }
    }

    #[test]
    fn raw_and_canonical_builders_produce_equivalent_packet_semantics() {
        let provenance = ConversationPacketProvenance::local();
        let raw = conversation_packet_from_normalized(&raw_conversation(), provenance.clone());
        let canonical =
            ConversationPacket::from_canonical_replay(&canonical_conversation(), provenance);

        assert_eq!(raw.version, CONVERSATION_PACKET_VERSION);
        assert!(raw.semantically_equivalent_to(&canonical));
        assert_eq!(raw.payload.messages[1].role, "assistant");
        assert_eq!(canonical.payload.messages[1].role, "assistant");
        assert_eq!(raw.projections.lexical.message_indices, vec![0, 1]);
        assert_eq!(raw.projections.analytics.user_messages, 1);
        assert_eq!(raw.projections.analytics.assistant_messages, 1);
    }

    #[test]
    fn packet_hash_changes_when_normalized_content_changes() {
        let mut changed = raw_conversation();
        changed.messages[1].content = "packet changed".to_string();

        let original = conversation_packet_from_normalized(
            &raw_conversation(),
            ConversationPacketProvenance::local(),
        );
        let changed =
            conversation_packet_from_normalized(&changed, ConversationPacketProvenance::local());

        assert_ne!(original.hashes.semantic_hash, changed.hashes.semantic_hash);
        assert_ne!(original.hashes.message_hash, changed.hashes.message_hash);
    }

    #[test]
    fn semantic_hash_ignores_raw_mirror_bookkeeping_only() {
        let packet = |conversation: &NormalizedConversation| {
            conversation_packet_from_normalized(conversation, ConversationPacketProvenance::local())
        };
        let mut base = raw_conversation();
        base.metadata = json!({"model": "m", "cass": {"origin": {"source_id": "local"}}});
        let mut captured = base.clone();
        captured.metadata["cass"]["raw_mirror"] =
            json!({"manifest_id": "a", "captured_at_ms": 1, "source_mtime_ms": 2});
        let mut recaptured = base.clone();
        recaptured.metadata["cass"]["raw_mirror"] =
            json!({"manifest_id": "b", "captured_at_ms": 99, "chunk_count": 4});

        // Absent, captured and re-captured pointers hash identically.
        let base_hash = packet(&base).hashes.semantic_hash;
        assert_eq!(packet(&captured).hashes.semantic_hash, base_hash);
        assert_eq!(packet(&recaptured).hashes.semantic_hash, base_hash);
        // The packet itself still carries the pointer unchanged.
        assert_eq!(
            packet(&captured).payload.metadata_json["cass"]["raw_mirror"]["manifest_id"],
            json!("a")
        );

        // Every other metadata change still moves the hash, including a
        // sibling under `cass` and a top-level `raw_mirror` key.
        let mut origin_changed = captured.clone();
        origin_changed.metadata["cass"]["origin"]["source_id"] = json!("other");
        assert_ne!(packet(&origin_changed).hashes.semantic_hash, base_hash);
        let mut top_level = base.clone();
        top_level.metadata["raw_mirror"] = json!({"manifest_id": "a"});
        assert_ne!(packet(&top_level).hashes.semantic_hash, base_hash);
        let mut model_changed = captured;
        model_changed.metadata["model"] = json!("other");
        assert_ne!(packet(&model_changed).hashes.semantic_hash, base_hash);
    }

    #[test]
    fn raw_and_canonical_packet_audit_matches_when_content_agrees() {
        let provenance = ConversationPacketProvenance::local();
        let raw =
            conversation_packet_from_normalized(&audit_raw_conversation(), provenance.clone());
        let canonical =
            ConversationPacket::from_canonical_replay(&audit_canonical_conversation(), provenance);

        let auditor = PacketEquivalenceAuditor::new();
        let outcome = auditor.audit_pair(&raw, &canonical, &PacketEquivalenceTolerance::strict());
        assert!(outcome.is_match(), "expected match, got {outcome:?}");
        if let PacketEquivalenceOutcome::Match { semantic_hash } = outcome {
            assert_eq!(semantic_hash, raw.hashes.semantic_hash);
            assert_eq!(semantic_hash.len(), 64, "blake3 hex digest is 64 chars");
        }
    }

    #[test]
    fn role_count_drift_surfaces_as_analytics_projection_difference() {
        let provenance = ConversationPacketProvenance::local();
        let raw =
            conversation_packet_from_normalized(&audit_raw_conversation(), provenance.clone());

        let mut canonical_data = audit_canonical_conversation();
        canonical_data.messages.push(Message {
            id: Some(72),
            idx: 2,
            role: MessageRole::Tool,
            author: Some("ripgrep".to_string()),
            created_at: Some(1_700_000_002_000),
            content: "tool output".to_string(),
            extra_json: json!({}),
            snippets: Vec::new(),
        });
        let canonical = ConversationPacket::from_canonical_replay(&canonical_data, provenance);

        let auditor = PacketEquivalenceAuditor::new();
        let outcome = auditor.audit_pair(&raw, &canonical, &PacketEquivalenceTolerance::strict());
        let PacketEquivalenceOutcome::Mismatch(mismatch) = outcome else {
            panic!("expected mismatch when role counts diverge");
        };
        assert!(
            mismatch.projection_differences.iter().any(|diff| matches!(
                diff,
                PacketProjectionDifference::AnalyticsRoleCounts { a, b }
                    if a.tool_messages == 0 && b.tool_messages == 1
            )),
            "expected analytics tool-message drift, got {:?}",
            mismatch.projection_differences
        );
        assert!(
            !mismatch.is_hash_only(),
            "projection drift must not be downgraded to hash-only"
        );
    }

    #[test]
    fn redaction_drift_is_excused_only_under_explicit_tolerance() {
        let provenance = ConversationPacketProvenance::local();
        let raw =
            conversation_packet_from_normalized(&audit_raw_conversation(), provenance.clone());
        let mut redacted = audit_canonical_conversation();
        let redacted_text = "█".repeat(raw.payload.messages[0].content.chars().count());
        debug_assert_eq!(
            redacted_text.chars().count(),
            raw.payload.messages[0].content.chars().count()
        );
        let want_bytes = raw.payload.messages[0].content.len();
        let mut bytes = Vec::with_capacity(want_bytes);
        bytes.resize(want_bytes, b'#');
        redacted.messages[0].content = String::from_utf8(bytes).unwrap();
        let canonical = ConversationPacket::from_canonical_replay(&redacted, provenance);

        let auditor = PacketEquivalenceAuditor::new();

        let strict = auditor.audit_pair(&raw, &canonical, &PacketEquivalenceTolerance::strict());
        let PacketEquivalenceOutcome::Mismatch(mismatch) = strict else {
            panic!("strict audit should flag content/hash drift");
        };
        assert!(
            mismatch.is_hash_only(),
            "byte-length-preserving redaction should leave only hash drift, got {:?}",
            mismatch
        );
        assert!(
            mismatch
                .hash_differences
                .iter()
                .any(|d| matches!(d, PacketHashDifference::SemanticHash { .. }))
        );

        let tolerant = auditor.audit_pair(
            &raw,
            &canonical,
            &PacketEquivalenceTolerance::allow_redaction(),
        );
        assert!(
            tolerant.is_match(),
            "redaction-tolerant audit must match when only hashes drift, got {tolerant:?}"
        );
    }

    #[test]
    fn audit_outcome_serializes_with_outcome_tag() {
        let provenance = ConversationPacketProvenance::local();
        let raw =
            conversation_packet_from_normalized(&audit_raw_conversation(), provenance.clone());
        let canonical =
            ConversationPacket::from_canonical_replay(&audit_canonical_conversation(), provenance);
        let outcome = PacketEquivalenceAuditor::new().audit_pair(
            &raw,
            &canonical,
            &PacketEquivalenceTolerance::strict(),
        );
        let serialized = serde_json::to_string(&outcome).expect("serialize match outcome");
        assert!(
            serialized.contains("\"outcome\":\"match\""),
            "match outcome should serialize with snake_case `outcome` tag, got {serialized}"
        );
        assert!(serialized.contains("\"semantic_hash\""));
    }
}
