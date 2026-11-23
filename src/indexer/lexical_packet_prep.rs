use super::*;
use rayon::prelude::*;

pub(in crate::indexer) fn lexical_rebuild_noise_role(is_tool_role: bool) -> Option<&'static str> {
    is_tool_role.then_some("tool")
}

pub(in crate::indexer) fn lexical_rebuild_packet_provenance_from_canonical(
    conv: &crate::storage::sqlite::LexicalRebuildConversationRow,
    source_map: &HashMap<String, (SourceKind, Option<String>)>,
) -> (
    LexicalRebuildPacketProvenance,
    LexicalRebuildPacketProvenanceMode,
) {
    let trimmed_source_id = conv.source_id.trim();
    let source_lookup = source_map.get(&conv.source_id).cloned();
    let (kind, host_label, mode) = if let Some((kind, host_label)) = source_lookup {
        (
            kind,
            host_label,
            LexicalRebuildPacketProvenanceMode::SourceMapLookup,
        )
    } else {
        let fallback_kind = if conv.source_id == LOCAL_SOURCE_ID {
            SourceKind::Local
        } else {
            SourceKind::Ssh
        };
        let fallback_mode = if trimmed_source_id.is_empty() {
            if conv
                .origin_host
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_some()
            {
                LexicalRebuildPacketProvenanceMode::HostFallback
            } else {
                LexicalRebuildPacketProvenanceMode::ConversationFields
            }
        } else {
            LexicalRebuildPacketProvenanceMode::ConversationFields
        };
        (fallback_kind, None, fallback_mode)
    };

    let origin_host = crate::search::tantivy::normalized_index_origin_host(
        conv.origin_host.as_deref().or(host_label.as_deref()),
    );
    let source_id = crate::search::tantivy::normalized_index_source_id(
        Some(&conv.source_id),
        Some(kind.as_str()),
        origin_host.as_deref(),
    );
    let origin_kind =
        crate::search::tantivy::normalized_index_origin_kind(&source_id, Some(kind.as_str()));

    (
        LexicalRebuildPacketProvenance {
            source_id,
            origin_kind,
            origin_host,
        },
        mode,
    )
}

#[cfg(test)]
pub(in crate::indexer) fn lexical_rebuild_packet_provenance_from_metadata(
    conv: &NormalizedConversation,
) -> (
    LexicalRebuildPacketProvenance,
    LexicalRebuildPacketProvenanceMode,
) {
    let cass_origin = conv
        .metadata
        .get("cass")
        .and_then(|cass| cass.get("origin"));
    let raw_source_id = cass_origin
        .and_then(|origin| origin.get("source_id"))
        .and_then(|value| value.as_str());
    let raw_origin_kind = cass_origin
        .and_then(|origin| origin.get("kind"))
        .and_then(|value| value.as_str());
    let origin_host = crate::search::tantivy::normalized_index_origin_host(
        cass_origin
            .and_then(|origin| origin.get("host"))
            .and_then(|value| value.as_str()),
    );
    let source_id = crate::search::tantivy::normalized_index_source_id(
        raw_source_id,
        raw_origin_kind,
        origin_host.as_deref(),
    );
    let origin_kind =
        crate::search::tantivy::normalized_index_origin_kind(&source_id, raw_origin_kind);

    let mode = if raw_source_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some()
        || raw_origin_kind
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some()
    {
        LexicalRebuildPacketProvenanceMode::MetadataFields
    } else if origin_host.is_some() {
        LexicalRebuildPacketProvenanceMode::HostFallback
    } else {
        LexicalRebuildPacketProvenanceMode::LocalDefault
    };

    (
        LexicalRebuildPacketProvenance {
            source_id,
            origin_kind,
            origin_host,
        },
        mode,
    )
}

#[cfg(test)]
pub(in crate::indexer) fn lexical_rebuild_grouped_message_from_normalized(
    message: &NormalizedMessage,
) -> crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
    crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
        idx: message.idx,
        is_tool_role: message.role.eq_ignore_ascii_case("tool"),
        created_at: message.created_at,
        content: message.content.clone(),
    }
}

pub(in crate::indexer) fn lexical_rebuild_contract_provenance(
    provenance: &LexicalRebuildPacketProvenance,
) -> ConversationPacketProvenance {
    ConversationPacketProvenance {
        source_id: provenance.source_id.clone(),
        origin_kind: provenance.origin_kind.clone(),
        origin_host: provenance.origin_host.clone(),
    }
}

pub(in crate::indexer) fn lexical_rebuild_contract_from_grouped_messages(
    conversation: &crate::storage::sqlite::LexicalRebuildConversationRow,
    provenance: &LexicalRebuildPacketProvenance,
    messages: &crate::storage::sqlite::LexicalRebuildGroupedMessageRows,
) -> ConversationPacket {
    let canonical_messages = messages
        .iter()
        .map(|message| crate::model::types::Message {
            id: None,
            idx: message.idx,
            role: if message.is_tool_role {
                crate::model::types::MessageRole::Tool
            } else {
                crate::model::types::MessageRole::Agent
            },
            author: None,
            created_at: message.created_at,
            content: message.content.clone(),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        })
        .collect::<Vec<_>>();
    lexical_rebuild_contract_from_canonical_messages(conversation, provenance, canonical_messages)
}

pub(in crate::indexer) fn lexical_rebuild_contract_from_canonical_messages(
    conversation: &crate::storage::sqlite::LexicalRebuildConversationRow,
    provenance: &LexicalRebuildPacketProvenance,
    messages: Vec<crate::model::types::Message>,
) -> ConversationPacket {
    let canonical = crate::model::types::Conversation {
        id: conversation.id,
        agent_slug: conversation.agent_slug.clone(),
        workspace: conversation.workspace.clone(),
        external_id: conversation.external_id.clone(),
        title: conversation.title.clone(),
        source_path: conversation.source_path.clone(),
        started_at: conversation.started_at,
        ended_at: conversation.ended_at,
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages,
        source_id: provenance.source_id.clone(),
        origin_host: provenance.origin_host.clone(),
    };
    ConversationPacket::from_canonical_replay_owned(
        canonical,
        lexical_rebuild_contract_provenance(provenance),
    )
}

impl LexicalRebuildConversationPacket {
    pub(in crate::indexer) fn from_canonical_replay(
        conversation: crate::storage::sqlite::LexicalRebuildConversationRow,
        messages: crate::storage::sqlite::LexicalRebuildGroupedMessageRows,
        last_message_id: Option<i64>,
        source_map: &HashMap<String, (SourceKind, Option<String>)>,
    ) -> Self {
        let (provenance, provenance_mode) =
            lexical_rebuild_packet_provenance_from_canonical(&conversation, source_map);
        let contract =
            lexical_rebuild_contract_from_grouped_messages(&conversation, &provenance, &messages);
        let ConversationPacket {
            hashes: contract_hashes,
            projections: contract_projections,
            ..
        } = contract;
        Self::from_canonical_replay_parts(
            conversation,
            messages,
            last_message_id,
            provenance,
            provenance_mode,
            contract_hashes,
            contract_projections,
        )
    }

    pub(in crate::indexer) fn from_canonical_replay_messages(
        conversation: crate::storage::sqlite::LexicalRebuildConversationRow,
        messages: Vec<crate::model::types::Message>,
        source_map: &HashMap<String, (SourceKind, Option<String>)>,
    ) -> Result<Self> {
        let (provenance, provenance_mode) =
            lexical_rebuild_packet_provenance_from_canonical(&conversation, source_map);
        let contract =
            lexical_rebuild_contract_from_canonical_messages(&conversation, &provenance, messages);
        let ConversationPacket {
            hashes: contract_hashes,
            projections: contract_projections,
            payload,
            ..
        } = contract;
        let mut grouped_rows = crate::storage::sqlite::LexicalRebuildGroupedMessageRows::new();
        grouped_rows.reserve(payload.messages.len());
        let mut last_message_id = None;
        for message in payload.messages {
            let message_id = message.message_id.ok_or_else(|| {
                anyhow::anyhow!(
                    "lexical rebuild batch fetch returned message without id for conversation {}",
                    conversation.id.unwrap_or_default()
                )
            })?;
            last_message_id = Some(last_message_id.unwrap_or(0).max(message_id));
            grouped_rows.push(crate::storage::sqlite::LexicalRebuildGroupedMessageRow {
                idx: message.idx,
                is_tool_role: message.role == "tool",
                created_at: message.created_at,
                content: message.content,
            });
        }
        Ok(Self::from_canonical_replay_parts(
            conversation,
            grouped_rows,
            last_message_id,
            provenance,
            provenance_mode,
            contract_hashes,
            contract_projections,
        ))
    }

    pub(in crate::indexer) fn from_canonical_replay_parts(
        conversation: crate::storage::sqlite::LexicalRebuildConversationRow,
        messages: crate::storage::sqlite::LexicalRebuildGroupedMessageRows,
        last_message_id: Option<i64>,
        provenance: LexicalRebuildPacketProvenance,
        provenance_mode: LexicalRebuildPacketProvenanceMode,
        contract_hashes: ConversationPacketHashes,
        contract_projections: ConversationPacketSinkProjections,
    ) -> Self {
        let message_count = messages.len();
        let message_bytes = contract_projections.lexical.total_content_bytes;
        Self {
            diagnostics: LexicalRebuildPacketDiagnostics {
                version: LEXICAL_REBUILD_PACKET_VERSION,
                source: LexicalRebuildPacketSource::CanonicalReplay,
                provenance_mode,
                missing_conversation_id: conversation.id.is_none(),
            },
            identity: LexicalRebuildPacketIdentity {
                conversation_id: conversation.id,
                external_id: conversation.external_id,
                agent: conversation.agent_slug,
                workspace: conversation
                    .workspace
                    .as_ref()
                    .map(|workspace| workspace.to_string_lossy().to_string()),
                source_path: conversation.source_path.to_string_lossy().to_string(),
                title: conversation.title,
                started_at: conversation.started_at,
                ended_at: conversation.ended_at,
            },
            provenance,
            contract_hashes,
            contract_projections,
            messages,
            message_count,
            message_bytes,
            flow_reservation_bytes: 0,
            last_message_id,
            chunk: None,
        }
    }

    pub(in crate::indexer) fn fingerprint_input(&self) -> LexicalRebuildPacketFingerprintInput<'_> {
        LexicalRebuildPacketFingerprintInput {
            version: self.diagnostics.version,
            agent: self.identity.agent.as_str(),
            external_id: self.identity.external_id.as_deref(),
            workspace: self.identity.workspace.as_deref(),
            source_path: self.identity.source_path.as_str(),
            title: self.identity.title.as_deref(),
            started_at: self.identity.started_at,
            ended_at: self.identity.ended_at,
            source_id: self.provenance.source_id.as_str(),
            origin_kind: self.provenance.origin_kind.as_str(),
            origin_host: self.provenance.origin_host.as_deref(),
            lexical_projected_content_bytes: self.contract_projections.lexical.total_content_bytes,
            messages: &self.messages,
            message_count: self.message_count,
            message_bytes: self.message_bytes,
        }
    }

    pub(in crate::indexer) fn prebuilt_docs(
        &self,
    ) -> Vec<frankensearch::quill::cass::CassDocumentRef<'_>> {
        let Some(conversation_id) = self.identity.conversation_id else {
            return Vec::new();
        };

        let mut docs = Vec::with_capacity(self.contract_projections.lexical.message_indices.len());
        for message_index in &self.contract_projections.lexical.message_indices {
            let Some(message) = self.messages.get(*message_index) else {
                continue;
            };
            if is_hard_message_noise(
                lexical_rebuild_noise_role(message.is_tool_role),
                &message.content,
            ) {
                continue;
            }
            docs.push(frankensearch::quill::cass::CassDocumentRef {
                agent: self.identity.agent.as_str(),
                workspace: self.identity.workspace.as_deref(),
                workspace_original: None,
                source_path: self.identity.source_path.as_str(),
                msg_idx: message.idx.max(0) as u64,
                created_at: message.created_at.or(self.identity.started_at),
                title: self.identity.title.as_deref(),
                content: message.content.as_str(),
                source_id: self.provenance.source_id.as_str(),
                origin_kind: self.provenance.origin_kind.as_str(),
                origin_host: self.provenance.origin_host.as_deref(),
                conversation_id: Some(conversation_id),
            });
        }
        docs
    }

    #[cfg(test)]
    pub(in crate::indexer) fn from_normalized_conversation(conv: &NormalizedConversation) -> Self {
        let (provenance, provenance_mode) = lexical_rebuild_packet_provenance_from_metadata(conv);
        let contract = ConversationPacket::from_normalized_conversation(
            conv,
            lexical_rebuild_contract_provenance(&provenance),
        );
        let messages = conv
            .messages
            .iter()
            .map(lexical_rebuild_grouped_message_from_normalized)
            .collect::<crate::storage::sqlite::LexicalRebuildGroupedMessageRows>();
        let message_count = contract.payload.messages.len();
        let message_bytes = contract.projections.lexical.total_content_bytes;
        let contract_hashes = contract.hashes;
        let contract_projections = contract.projections;
        Self {
            diagnostics: LexicalRebuildPacketDiagnostics {
                version: LEXICAL_REBUILD_PACKET_VERSION,
                source: LexicalRebuildPacketSource::NormalizedConversation,
                provenance_mode,
                missing_conversation_id: true,
            },
            identity: LexicalRebuildPacketIdentity {
                conversation_id: None,
                external_id: conv.external_id.clone(),
                agent: conv.agent_slug.clone(),
                workspace: conv
                    .workspace
                    .as_ref()
                    .map(|workspace| workspace.to_string_lossy().to_string()),
                source_path: conv.source_path.to_string_lossy().to_string(),
                title: conv.title.clone(),
                started_at: conv.started_at,
                ended_at: conv.ended_at,
            },
            provenance,
            contract_hashes,
            contract_projections,
            messages,
            message_count,
            message_bytes,
            flow_reservation_bytes: 0,
            last_message_id: None,
            chunk: None,
        }
    }

    #[cfg(test)]
    pub(in crate::indexer) fn semantic_view(&self) -> LexicalRebuildPacketSemanticView {
        LexicalRebuildPacketSemanticView {
            version: self.diagnostics.version,
            agent: self.identity.agent.clone(),
            external_id: self.identity.external_id.clone(),
            workspace: self.identity.workspace.clone(),
            source_path: self.identity.source_path.clone(),
            title: self.identity.title.clone(),
            started_at: self.identity.started_at,
            ended_at: self.identity.ended_at,
            source_id: self.provenance.source_id.clone(),
            origin_kind: self.provenance.origin_kind.clone(),
            origin_host: self.provenance.origin_host.clone(),
            contract_projections: self.contract_projections.clone(),
            messages: self.messages.clone(),
            message_count: self.message_count,
            message_bytes: self.message_bytes,
        }
    }
}

/// Streaming equivalence proof for the authoritative lexical rebuild path
/// (bead ibuuh.29).
///
/// The test module shipped a sibling struct populated from a fully materialized
/// packet vector so the legacy OFFSET replay and the new keyset-batched replay
/// could be diffed for equivalence on a fixture. The production rebuild cannot
/// buffer every packet, so this struct is the streaming-consumed form of the
/// same proof: document count, a packet-and-doc manifest fingerprint hashed in
/// commit order, and per-probe golden-query digests. Because both the test
/// helper and the production consumer now funnel through
/// [`LexicalRebuildEquivalenceAccumulator`], runtime evidence is diffable
/// against the fixture-derived ledger without materializing the whole corpus.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LexicalRebuildEquivalenceEvidence {
    pub document_count: u64,
    pub manifest_fingerprint: String,
    pub golden_query_digest: String,
    pub golden_query_hit_counts: Vec<LexicalRebuildEquivalenceGoldenHit>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LexicalRebuildEquivalenceGoldenHit {
    pub probe: String,
    pub hit_count: u64,
}

pub(in crate::indexer) const LEXICAL_REBUILD_EQUIVALENCE_DEFAULT_PROBES: &[&str] =
    &["error", "TODO", "function", "import", "test"];

pub(in crate::indexer) struct LexicalRebuildEquivalenceAccumulator {
    pub(in crate::indexer) document_count: u64,
    pub(in crate::indexer) manifest_hasher: blake3::Hasher,
    pub(in crate::indexer) probes: Vec<String>,
    pub(in crate::indexer) probe_hashers: Vec<blake3::Hasher>,
    pub(in crate::indexer) probe_counts: Vec<u64>,
}

impl LexicalRebuildEquivalenceAccumulator {
    pub(in crate::indexer) fn new() -> Self {
        Self::with_probes(
            LEXICAL_REBUILD_EQUIVALENCE_DEFAULT_PROBES
                .iter()
                .map(|probe| (*probe).to_string()),
        )
    }

    pub(in crate::indexer) fn with_probes<I>(probes: I) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        let probes: Vec<String> = probes.into_iter().collect();
        let probe_hashers = probes.iter().map(|_| blake3::Hasher::new()).collect();
        let probe_counts = vec![0_u64; probes.len()];
        Self {
            document_count: 0,
            manifest_hasher: blake3::Hasher::new(),
            probes,
            probe_hashers,
            probe_counts,
        }
    }

    pub(in crate::indexer) fn absorb_packet(&mut self, packet: &LexicalRebuildConversationPacket) {
        let fingerprint = packet.fingerprint_input();
        // Packet header: version, identity, provenance, counters. Length-prefixed
        // length-prefixed strings avoid ambiguity across field boundaries.
        self.manifest_hasher.update(b"pkt");
        self.manifest_hasher
            .update(&fingerprint.version.to_le_bytes());
        lexical_rebuild_equivalence_update_opt_str(
            &mut self.manifest_hasher,
            Some(fingerprint.agent),
        );
        lexical_rebuild_equivalence_update_opt_str(
            &mut self.manifest_hasher,
            fingerprint.external_id,
        );
        lexical_rebuild_equivalence_update_opt_str(
            &mut self.manifest_hasher,
            fingerprint.workspace,
        );
        lexical_rebuild_equivalence_update_opt_str(
            &mut self.manifest_hasher,
            Some(fingerprint.source_path),
        );
        lexical_rebuild_equivalence_update_opt_str(&mut self.manifest_hasher, fingerprint.title);
        self.manifest_hasher
            .update(&fingerprint.started_at.unwrap_or(i64::MIN).to_le_bytes());
        self.manifest_hasher
            .update(&fingerprint.ended_at.unwrap_or(i64::MIN).to_le_bytes());
        lexical_rebuild_equivalence_update_opt_str(
            &mut self.manifest_hasher,
            Some(fingerprint.source_id),
        );
        lexical_rebuild_equivalence_update_opt_str(
            &mut self.manifest_hasher,
            Some(fingerprint.origin_kind),
        );
        lexical_rebuild_equivalence_update_opt_str(
            &mut self.manifest_hasher,
            fingerprint.origin_host,
        );
        self.manifest_hasher
            .update(&(fingerprint.lexical_projected_content_bytes as u64).to_le_bytes());
        self.manifest_hasher
            .update(&(fingerprint.message_count as u64).to_le_bytes());
        self.manifest_hasher
            .update(&(fingerprint.message_bytes as u64).to_le_bytes());

        let docs = packet.prebuilt_docs();
        self.document_count = self.document_count.saturating_add(docs.len() as u64);
        for doc in &docs {
            self.manifest_hasher.update(b"doc");
            lexical_rebuild_equivalence_update_opt_str(&mut self.manifest_hasher, Some(doc.agent));
            lexical_rebuild_equivalence_update_opt_str(&mut self.manifest_hasher, doc.workspace);
            lexical_rebuild_equivalence_update_opt_str(
                &mut self.manifest_hasher,
                Some(doc.source_path),
            );
            self.manifest_hasher.update(&doc.msg_idx.to_le_bytes());
            self.manifest_hasher
                .update(&doc.created_at.unwrap_or(i64::MIN).to_le_bytes());
            lexical_rebuild_equivalence_update_opt_str(&mut self.manifest_hasher, doc.title);
            self.manifest_hasher
                .update(&(doc.content.len() as u64).to_le_bytes());
            self.manifest_hasher.update(doc.content.as_bytes());
            lexical_rebuild_equivalence_update_opt_str(
                &mut self.manifest_hasher,
                Some(doc.source_id),
            );
            lexical_rebuild_equivalence_update_opt_str(
                &mut self.manifest_hasher,
                Some(doc.origin_kind),
            );
            lexical_rebuild_equivalence_update_opt_str(&mut self.manifest_hasher, doc.origin_host);

            for ((probe, hasher), count) in self
                .probes
                .iter()
                .zip(self.probe_hashers.iter_mut())
                .zip(self.probe_counts.iter_mut())
            {
                let probe_str = probe.as_str();
                let hit = doc.content.contains(probe_str)
                    || doc
                        .title
                        .map(|value| value.contains(probe_str))
                        .unwrap_or(false)
                    || doc
                        .workspace
                        .map(|value| value.contains(probe_str))
                        .unwrap_or(false)
                    || doc.source_path.contains(probe_str);
                if hit {
                    *count = count.saturating_add(1);
                    hasher.update(b"hit");
                    lexical_rebuild_equivalence_update_opt_str(hasher, Some(doc.source_path));
                    hasher.update(&doc.msg_idx.to_le_bytes());
                    hasher.update(&doc.created_at.unwrap_or(i64::MIN).to_le_bytes());
                    hasher.update(&(doc.content.len() as u64).to_le_bytes());
                    hasher.update(doc.content.as_bytes());
                }
            }
        }
    }

    pub(in crate::indexer) fn finalize(self) -> LexicalRebuildEquivalenceEvidence {
        let manifest_fingerprint = self.manifest_hasher.finalize().to_hex().to_string();
        let mut combined = blake3::Hasher::new();
        let mut golden_query_hit_counts = Vec::with_capacity(self.probes.len());
        for ((probe, hasher), count) in self
            .probes
            .into_iter()
            .zip(self.probe_hashers)
            .zip(self.probe_counts)
        {
            combined.update(b"probe");
            combined.update(&(probe.len() as u64).to_le_bytes());
            combined.update(probe.as_bytes());
            combined.update(&count.to_le_bytes());
            combined.update(hasher.finalize().as_bytes());
            golden_query_hit_counts.push(LexicalRebuildEquivalenceGoldenHit {
                probe,
                hit_count: count,
            });
        }
        LexicalRebuildEquivalenceEvidence {
            document_count: self.document_count,
            manifest_fingerprint,
            golden_query_digest: combined.finalize().to_hex().to_string(),
            golden_query_hit_counts,
        }
    }
}

pub(in crate::indexer) fn lexical_rebuild_equivalence_update_opt_str(
    hasher: &mut blake3::Hasher,
    value: Option<&str>,
) {
    match value {
        Some(s) => {
            hasher.update(&[0x01_u8]);
            hasher.update(&(s.len() as u64).to_le_bytes());
            hasher.update(s.as_bytes());
        }
        None => {
            hasher.update(&[0x00_u8]);
        }
    }
}

#[derive(Debug)]
pub(in crate::indexer) struct LexicalRebuildPacketPrepInput {
    pub(in crate::indexer) conversation: crate::storage::sqlite::LexicalRebuildConversationRow,
    pub(in crate::indexer) messages: Option<Vec<crate::model::types::Message>>,
}

pub(in crate::indexer) fn prepare_lexical_rebuild_packet_from_canonical(
    input: LexicalRebuildPacketPrepInput,
    source_map: &HashMap<String, (SourceKind, Option<String>)>,
) -> Result<LexicalRebuildConversationPacket> {
    if let Some(messages) = input.messages {
        return LexicalRebuildConversationPacket::from_canonical_replay_messages(
            input.conversation,
            messages,
            source_map,
        );
    }

    Ok(LexicalRebuildConversationPacket::from_canonical_replay(
        input.conversation,
        crate::storage::sqlite::LexicalRebuildGroupedMessageRows::new(),
        None,
        source_map,
    ))
}

pub(in crate::indexer) fn prepare_lexical_rebuild_packet_batch(
    conversation_page: Vec<crate::storage::sqlite::LexicalRebuildConversationRow>,
    mut grouped_messages: HashMap<i64, Vec<crate::model::types::Message>>,
    source_map: &HashMap<String, (SourceKind, Option<String>)>,
    lexical_rebuild_worker_pool: Option<&ThreadPool>,
) -> Result<Vec<LexicalRebuildConversationPacket>> {
    let inputs = conversation_page
        .into_iter()
        .map(|conversation| {
            let messages = conversation
                .id
                .and_then(|conversation_id| grouped_messages.remove(&conversation_id));
            LexicalRebuildPacketPrepInput {
                conversation,
                messages,
            }
        })
        .collect::<Vec<_>>();

    match lexical_rebuild_worker_pool {
        Some(pool) => pool.install(|| {
            inputs
                .into_par_iter()
                .map(|input| prepare_lexical_rebuild_packet_from_canonical(input, source_map))
                .collect::<Result<Vec<_>>>()
        }),
        None => inputs
            .into_iter()
            .map(|input| prepare_lexical_rebuild_packet_from_canonical(input, source_map))
            .collect::<Result<Vec<_>>>(),
    }
}

pub(in crate::indexer) fn assign_lexical_rebuild_flow_reservation_bytes(
    packets: &mut [LexicalRebuildConversationPacket],
    reserved_bytes: usize,
) {
    if packets.is_empty() || reserved_bytes == 0 {
        return;
    }

    let total_message_bytes = packets
        .iter()
        .map(|packet| packet.message_bytes)
        .sum::<usize>();
    if total_message_bytes == 0 {
        if let Some(first_packet) = packets.first_mut() {
            first_packet.flow_reservation_bytes = reserved_bytes;
        }
        return;
    }

    let mut remaining_reserved = reserved_bytes;
    let mut remaining_message_bytes = total_message_bytes;
    let len = packets.len();
    for (idx, packet) in packets.iter_mut().enumerate() {
        let share = if idx + 1 == len {
            remaining_reserved
        } else if remaining_message_bytes == 0 || packet.message_bytes == 0 {
            0
        } else {
            packet.message_bytes.saturating_mul(remaining_reserved) / remaining_message_bytes
        };
        packet.flow_reservation_bytes = share;
        remaining_reserved = remaining_reserved.saturating_sub(share);
        remaining_message_bytes = remaining_message_bytes.saturating_sub(packet.message_bytes);
    }
}

pub(in crate::indexer) struct StreamingBatchFlowReservation<'a> {
    pub(in crate::indexer) limiter: Option<&'a StreamingByteLimiter>,
    pub(in crate::indexer) reserved_bytes: usize,
}

impl<'a> StreamingBatchFlowReservation<'a> {
    pub(in crate::indexer) fn new(
        limiter: Option<&'a StreamingByteLimiter>,
        reserved_bytes: usize,
    ) -> Self {
        Self {
            limiter,
            reserved_bytes,
        }
    }

    pub(in crate::indexer) fn release_now(&mut self) {
        let reserved_bytes = std::mem::take(&mut self.reserved_bytes);
        if reserved_bytes == 0 {
            return;
        }
        if let Some(limiter) = self.limiter {
            limiter.release(reserved_bytes);
        }
    }
}

impl Drop for StreamingBatchFlowReservation<'_> {
    fn drop(&mut self) {
        self.release_now();
    }
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::indexer) struct LexicalRebuildPacketSemanticView {
    pub(in crate::indexer) version: u32,
    pub(in crate::indexer) agent: String,
    pub(in crate::indexer) external_id: Option<String>,
    pub(in crate::indexer) workspace: Option<String>,
    pub(in crate::indexer) source_path: String,
    pub(in crate::indexer) title: Option<String>,
    pub(in crate::indexer) started_at: Option<i64>,
    pub(in crate::indexer) ended_at: Option<i64>,
    pub(in crate::indexer) source_id: String,
    pub(in crate::indexer) origin_kind: String,
    pub(in crate::indexer) origin_host: Option<String>,
    pub(in crate::indexer) contract_projections: ConversationPacketSinkProjections,
    pub(in crate::indexer) messages: crate::storage::sqlite::LexicalRebuildGroupedMessageRows,
    pub(in crate::indexer) message_count: usize,
    pub(in crate::indexer) message_bytes: usize,
}

#[allow(clippy::too_many_arguments)]
pub(in crate::indexer) fn flush_streamed_lexical_rebuild_batch(
    pending_batch: &mut Vec<LexicalRebuildConversationPacket>,
    pending_batch_message_count: &mut usize,
    pending_batch_message_bytes: &mut usize,
    lexical_rebuild_flow_limiter: Option<&StreamingByteLimiter>,
    lexical_rebuild_worker_pool: Option<&ThreadPool>,
    t_index: &mut TantivyIndex,
    indexed_docs: &mut usize,
    messages_since_commit: &mut usize,
    message_bytes_since_commit: &mut usize,
    current_batch_conversation_limit: usize,
    page_size: i64,
    perf_profile: Option<&mut LexicalRebuildPerfProfile>,
) -> Result<()> {
    if pending_batch.is_empty() {
        return Ok(());
    }

    let batch_conversations = pending_batch.len();
    let chunk_message_count = *pending_batch_message_count;
    let chunk_message_bytes = *pending_batch_message_bytes;
    let chunk_missing_conversation_ids = pending_batch
        .iter()
        .filter(|packet| packet.diagnostics.missing_conversation_id)
        .count();
    let chunk_packet_version = pending_batch
        .first()
        .map(|packet| packet.diagnostics.version)
        .unwrap_or(LEXICAL_REBUILD_PACKET_VERSION);
    let chunk_last_message_id = pending_batch
        .iter()
        .filter_map(|packet| packet.last_message_id)
        .max();
    let chunk_flow_reservation_bytes = pending_batch
        .iter()
        .map(|packet| packet.flow_reservation_bytes)
        .sum::<usize>();
    let mut flow_reservation = StreamingBatchFlowReservation::new(
        lexical_rebuild_flow_limiter,
        chunk_flow_reservation_bytes,
    );
    let chunk_limit = current_batch_conversation_limit;
    let prepare_started = perf_profile.as_ref().map(|_| Instant::now());
    let prepared_docs =
        lexical_rebuild_prepare_prebuilt_doc_refs(pending_batch, lexical_rebuild_worker_pool);
    let add_started = perf_profile.as_ref().map(|_| Instant::now());
    *indexed_docs = (*indexed_docs)
        .saturating_add(t_index.add_prebuilt_document_refs_slice(prepared_docs.as_slice())?);
    // `prepared_docs` borrows the packet strings. Drop those views, then free
    // the packets before publishing their byte capacity back to page prep.
    // Releasing first lets a newly awakened worker allocate the next giant
    // page while this batch is still resident, so the limiter temporarily
    // undercounts the real in-flight working set (#320/#413).
    drop(prepared_docs);
    if let Some(profile) = perf_profile {
        profile.batch_flushes = profile.batch_flushes.saturating_add(1);
        profile.batch_conversations = profile
            .batch_conversations
            .saturating_add(batch_conversations);
        profile.batch_messages = profile.batch_messages.saturating_add(chunk_message_count);
        profile.batch_message_bytes = profile
            .batch_message_bytes
            .saturating_add(chunk_message_bytes);
        if let Some(started) = prepare_started {
            profile.prepare_duration += started.elapsed();
        }
        if let Some(started) = add_started {
            profile.add_duration += started.elapsed();
        }
    }
    *messages_since_commit = (*messages_since_commit).saturating_add(chunk_message_count);
    *message_bytes_since_commit = (*message_bytes_since_commit).saturating_add(chunk_message_bytes);
    pending_batch.clear();
    *pending_batch_message_count = 0;
    *pending_batch_message_bytes = 0;
    flow_reservation.release_now();
    tracing::info!(
        page_size,
        packet_version = chunk_packet_version,
        chunk_conversations = batch_conversations,
        chunk_messages = chunk_message_count,
        chunk_message_bytes = chunk_message_bytes,
        chunk_missing_conversation_ids,
        chunk_last_message_id,
        chunk_limit,
        "lexical rebuild flushed a streamed conversation batch"
    );
    Ok(())
}

pub(in crate::indexer) fn lexical_rebuild_prepare_prebuilt_doc_refs<'a>(
    batch: &'a [LexicalRebuildConversationPacket],
    lexical_rebuild_worker_pool: Option<&ThreadPool>,
) -> Vec<frankensearch::quill::cass::CassDocumentRef<'a>> {
    let build_doc_shards = || {
        batch
            .par_iter()
            .map(LexicalRebuildConversationPacket::prebuilt_docs)
            .collect::<Vec<_>>()
    };
    let prepared_doc_shards = if let Some(pool) = lexical_rebuild_worker_pool {
        pool.install(build_doc_shards)
    } else {
        build_doc_shards()
    };
    let doc_capacity = prepared_doc_shards.iter().map(|shard| shard.len()).sum();
    let mut prepared_docs = Vec::with_capacity(doc_capacity);
    for shard in prepared_doc_shards {
        prepared_docs.extend(shard);
    }
    prepared_docs
}

#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::indexer) fn build_lexical_rebuild_shard_index(
    shard_index_path: &Path,
    batch: &[LexicalRebuildConversationPacket],
    lexical_rebuild_worker_pool: Option<&ThreadPool>,
) -> Result<usize> {
    Ok(
        build_lexical_rebuild_shard_index_summary_with_writer_parallelism(
            shard_index_path,
            batch,
            lexical_rebuild_worker_pool,
            None,
            None,
        )?
        .docs,
    )
}

pub(in crate::indexer) fn build_lexical_rebuild_shard_index_summary_with_writer_parallelism(
    shard_index_path: &Path,
    batch: &[LexicalRebuildConversationPacket],
    lexical_rebuild_worker_pool: Option<&ThreadPool>,
    writer_parallelism: Option<usize>,
    heartbeat: Option<crate::search::quill_bridge::EngineHeartbeat>,
) -> Result<SearchableIndexSummary> {
    let mut shard_index = if let Some(writer_parallelism) = writer_parallelism {
        TantivyIndex::open_or_create_with_writer_parallelism(shard_index_path, writer_parallelism)
    } else {
        TantivyIndex::open_or_create(shard_index_path)
    }
    .with_context(|| {
        format!(
            "opening lexical rebuild shard index at {}",
            shard_index_path.display()
        )
    })?;
    // GH #446: a single giant shard build is minutes of silent Quill work
    // between the pipeline counter changes the watchdog can see.
    shard_index.set_heartbeat(heartbeat);
    shard_index.configure_bulk_load_merge_policy();
    let prepared_docs =
        lexical_rebuild_prepare_prebuilt_doc_refs(batch, lexical_rebuild_worker_pool);
    let indexed_docs = shard_index
        .add_prebuilt_document_refs_slice(prepared_docs.as_slice())
        .with_context(|| {
            format!(
                "adding prebuilt lexical rebuild docs into shard index {}",
                shard_index_path.display()
            )
        })?;
    shard_index.commit().with_context(|| {
        format!(
            "committing lexical rebuild shard index at {}",
            shard_index_path.display()
        )
    })?;
    Ok(SearchableIndexSummary {
        docs: indexed_docs,
        segments: shard_index.segment_count(),
    })
}
