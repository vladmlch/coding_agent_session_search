use super::super::*;
use crate::model::types::{Conversation, Message, MessageRole};
use serde_json::json;

/// Counts inference inputs while executing the real hash producer.
pub(super) struct MeasuredHashEmbedder {
    pub(super) inner: HashEmbedder,
    pub(super) calls: std::sync::Arc<std::sync::Mutex<Vec<Vec<String>>>>,
    pub(super) fault: Option<EmbeddingFault>,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum EmbeddingFault {
    Inference,
    Count,
    Dimension,
    NonFinite,
}

impl Embedder for MeasuredHashEmbedder {
    fn embed_sync(&self, text: &str) -> crate::search::embedder::EmbedderResult<Vec<f32>> {
        self.inner.embed_sync(text)
    }

    fn embed_batch_sync(
        &self,
        texts: &[&str],
    ) -> crate::search::embedder::EmbedderResult<Vec<Vec<f32>>> {
        self.calls
            .lock()
            .unwrap()
            .push(texts.iter().map(|text| (*text).to_owned()).collect());
        let mut vectors = self.inner.embed_batch_sync(texts)?;
        match self.fault {
            Some(EmbeddingFault::Inference) => {
                return Err(crate::search::embedder::EmbedderError::EmbeddingFailed {
                    model: self.inner.id().to_owned(),
                    source: Box::new(std::io::Error::other("injected inference failure")),
                });
            }
            Some(EmbeddingFault::Count) => {
                vectors.pop();
            }
            Some(EmbeddingFault::Dimension) => {
                vectors.last_mut().unwrap().pop();
            }
            Some(EmbeddingFault::NonFinite) => {
                vectors.last_mut().unwrap()[0] = f32::NAN;
            }
            None => {}
        }
        Ok(vectors)
    }

    fn dimension(&self) -> usize {
        self.inner.dimension()
    }

    fn id(&self) -> &str {
        self.inner.id()
    }

    fn is_semantic(&self) -> bool {
        self.inner.is_semantic()
    }

    fn category(&self) -> frankensearch::ModelCategory {
        self.inner.category()
    }

    fn tier(&self) -> frankensearch::ModelTier {
        self.inner.tier()
    }
}

pub(super) fn assert_embedding_outputs_bitwise_equal(
    actual: &[EmbeddedMessage],
    expected: &[EmbeddedMessage],
) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(actual.message_id, expected.message_id);
        assert_eq!(actual.created_at_ms, expected.created_at_ms);
        assert_eq!(actual.agent_id, expected.agent_id);
        assert_eq!(actual.workspace_id, expected.workspace_id);
        assert_eq!(actual.source_id, expected.source_id);
        assert_eq!(actual.role, expected.role);
        assert_eq!(actual.chunk_idx, expected.chunk_idx);
        assert_eq!(actual.content_hash, expected.content_hash);
        assert_eq!(
            actual
                .embedding
                .iter()
                .map(|v| v.to_bits())
                .collect::<Vec<_>>(),
            expected
                .embedding
                .iter()
                .map(|v| v.to_bits())
                .collect::<Vec<_>>()
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ComparableSemanticInput {
    pub(super) message_id: u64,
    pub(super) created_at_ms: i64,
    pub(super) agent_id: u32,
    pub(super) workspace_id: u32,
    pub(super) source_id: u32,
    pub(super) role: u8,
    pub(super) content: String,
}

pub(super) fn comparable_semantic_inputs(
    mut inputs: Vec<EmbeddingInput>,
) -> Vec<ComparableSemanticInput> {
    let mut comparable: Vec<ComparableSemanticInput> = inputs
        .drain(..)
        .map(|input| ComparableSemanticInput {
            message_id: input.message_id,
            created_at_ms: input.created_at_ms,
            agent_id: input.agent_id,
            workspace_id: input.workspace_id,
            source_id: input.source_id,
            role: input.role,
            content: input.content,
        })
        .collect();
    comparable.sort();
    comparable
}

pub(super) fn test_conversation(external_id: &str, body: &str) -> Conversation {
    test_conversation_fixture(
        external_id,
        vec![Message {
            id: None,
            idx: 0,
            role: MessageRole::User,
            author: None,
            created_at: Some(1_700_000_000_500),
            content: body.to_string(),
            extra_json: json!({}),
            snippets: Vec::new(),
        }],
        "local",
        None,
    )
}

pub(super) fn test_conversation_with_messages(
    external_id: &str,
    messages: Vec<Message>,
) -> Conversation {
    test_conversation_fixture(external_id, messages, "remote-laptop", Some("builder-host"))
}

fn test_conversation_fixture(
    external_id: &str,
    messages: Vec<Message>,
    source_id: &str,
    origin_host: Option<&str>,
) -> Conversation {
    Conversation {
        id: None,
        agent_slug: "codex".to_string(),
        workspace: None,
        external_id: Some(external_id.to_string()),
        title: Some(format!("semantic {external_id}")),
        source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_001_000),
        approx_tokens: None,
        metadata_json: json!({}),
        messages,
        source_id: source_id.to_string(),
        origin_host: origin_host.map(str::to_string),
    }
}

pub(super) fn default_scheduler_signals() -> SemanticBackfillSchedulerSignals {
    SemanticBackfillSchedulerSignals {
        foreground_pressure: false,
        lexical_repair_active: false,
        force: false,
        operator_disabled: false,
        user_active: false,
    }
}

pub(super) struct EnvVarGuard {
    key: &'static str,
    prior: Option<String>,
}

impl EnvVarGuard {
    pub(super) fn set(key: &'static str, value: &str) -> Self {
        let prior = std::env::var(key).ok();
        // SAFETY: focused tests temporarily mutate process env and restore on drop.
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, prior }
    }

    pub(super) fn remove(key: &'static str) -> Self {
        let prior = std::env::var(key).ok();
        // SAFETY: focused tests temporarily mutate process env and restore on drop.
        unsafe {
            std::env::remove_var(key);
        }
        Self { key, prior }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        // SAFETY: restores the process env value captured by this test guard.
        unsafe {
            match self.prior.as_deref() {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }
}
