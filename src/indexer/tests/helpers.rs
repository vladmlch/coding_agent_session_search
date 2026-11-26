use super::super::*;
use crate::connectors::{Connector, DetectionResult, NormalizedConversation, NormalizedMessage};
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use crate::sources::provenance::SourceKind;

pub(super) fn read_index_run_lock_metadata_for_test(lock_path: &Path) -> Result<String> {
    #[cfg(windows)]
    {
        let sidecar_path =
            crate::search::asset_state::index_run_lock_metadata_sidecar_path(lock_path);
        if let Ok(raw) = std::fs::read_to_string(&sidecar_path)
            && !raw.trim().is_empty()
        {
            return Ok(raw);
        }
    }
    match std::fs::read_to_string(lock_path) {
        Ok(raw) => Ok(raw),
        Err(err) if crate::search::asset_state::windows_lock_conflict(&err) => {
            let sidecar_path =
                crate::search::asset_state::index_run_lock_metadata_sidecar_path(lock_path);
            std::fs::read_to_string(&sidecar_path).with_context(|| {
                format!(
                    "reading index-run lock metadata sidecar {}",
                    sidecar_path.display()
                )
            })
        }
        Err(err) => Err(err).with_context(|| {
            format!(
                "reading index-run lock metadata for test {}",
                lock_path.display()
            )
        }),
    }
}

pub(super) struct FailingDiscoveryConnector {
    pub(super) sources: Vec<crate::connectors::DiscoveredSourceFile>,
}

impl Connector for FailingDiscoveryConnector {
    fn detect(&self) -> DetectionResult {
        DetectionResult::not_found()
    }

    fn scan(
        &self,
        _ctx: &crate::connectors::ScanContext,
    ) -> anyhow::Result<Vec<NormalizedConversation>> {
        Err(anyhow::anyhow!(
            "intentional parser failure after source discovery"
        ))
    }

    fn discover_source_files(
        &self,
        _ctx: &crate::connectors::ScanContext,
    ) -> anyhow::Result<Vec<crate::connectors::DiscoveredSourceFile>> {
        Ok(self.sources.clone())
    }
}

pub(super) fn discovered_test_source(
    root: &ScanRoot,
    source_path: std::path::PathBuf,
    role: crate::connectors::DiscoveredSourceRole,
) -> crate::connectors::DiscoveredSourceFile {
    crate::connectors::DiscoveredSourceFile::new("synthetic", root, source_path, role, true)
        .with_fs_metadata()
}

pub(super) fn raw_mirror_manifest_values(data_dir: &Path) -> Vec<serde_json::Value> {
    let manifest_root = data_dir.join("raw-mirror/v1/manifests");
    if !manifest_root.exists() {
        return Vec::new();
    }
    let mut manifests: Vec<serde_json::Value> = std::fs::read_dir(&manifest_root)
        .expect("manifest dir")
        .collect::<std::io::Result<Vec<_>>>()
        .expect("manifest entries")
        .into_iter()
        .map(|entry| {
            serde_json::from_slice::<serde_json::Value>(
                &std::fs::read(entry.path()).expect("manifest bytes"),
            )
            .expect("manifest json")
        })
        .collect::<Vec<_>>();
    manifests.sort_by(|left, right| {
        left["original_path"]
            .as_str()
            .cmp(&right["original_path"].as_str())
    });
    manifests
}

pub(super) struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(value) = &self.previous {
            // SAFETY: test helper restores prior process env for isolation.
            unsafe {
                std::env::set_var(self.key, value);
            }
        } else {
            // SAFETY: test helper restores prior process env for isolation.
            unsafe {
                std::env::remove_var(self.key);
            }
        }
    }
}

pub(super) struct ActiveSessionSkipReset;

impl Drop for ActiveSessionSkipReset {
    fn drop(&mut self) {
        ACTIVE_SESSION_SOURCE_SKIP_OBSERVED.store(false, Ordering::Relaxed);
    }
}

pub(super) fn set_env_var(key: &'static str, value: impl AsRef<str>) -> EnvGuard {
    let previous = dotenvy::var(key).ok();
    // SAFETY: test helper toggles a process-local env var for isolation.
    unsafe {
        std::env::set_var(key, value.as_ref());
    }
    EnvGuard { key, previous }
}

pub(super) fn unset_env_var(key: &'static str) -> EnvGuard {
    let previous = dotenvy::var(key).ok();
    // SAFETY: test helper toggles a process-local env var for isolation.
    unsafe {
        std::env::remove_var(key);
    }
    EnvGuard { key, previous }
}

pub(super) fn ignore_sources_config() -> EnvGuard {
    let key = "CASS_IGNORE_SOURCES_CONFIG";
    let previous = dotenvy::var(key).ok();
    // SAFETY: test helper toggles a process-local env var for isolation.
    unsafe {
        std::env::set_var(key, "1");
    }
    EnvGuard { key, previous }
}

pub(super) fn never_constructed_connector_factory() -> Box<dyn Connector + Send> {
    panic!("test should not construct connector factories while filtering by config");
}

pub(super) fn set_env(key: &'static str, value: &str) -> EnvGuard {
    let previous = dotenvy::var(key).ok();
    // SAFETY: test helper toggles a process-local env var for isolation.
    unsafe {
        std::env::set_var(key, value);
    }
    EnvGuard { key, previous }
}

/// Restores the one ingest OOM injection it set on this thread when dropped.
pub(super) enum IngestTestOomGuard {
    NonWatch(Option<usize>),
    Lexical(bool),
}

impl Drop for IngestTestOomGuard {
    fn drop(&mut self) {
        match *self {
            Self::NonWatch(previous) => {
                NON_WATCH_INGEST_TEST_OOM_MIN_CONVS.with(|cell| cell.set(previous));
            }
            Self::Lexical(previous) => {
                INCREMENTAL_LEXICAL_UPDATE_TEST_OOM.with(|cell| cell.set(previous));
            }
        }
    }
}

/// Make non-watch ingest on THIS thread fail with a typed OOM for any batch
/// of at least `min_convs` conversations.
pub(super) fn inject_non_watch_ingest_test_oom(min_convs: usize) -> IngestTestOomGuard {
    IngestTestOomGuard::NonWatch(
        NON_WATCH_INGEST_TEST_OOM_MIN_CONVS.with(|cell| cell.replace(Some(min_convs))),
    )
}

/// Make incremental lexical updates on THIS thread fail with an OOM.
pub(super) fn inject_incremental_lexical_update_test_oom() -> IngestTestOomGuard {
    IngestTestOomGuard::Lexical(INCREMENTAL_LEXICAL_UPDATE_TEST_OOM.with(|cell| cell.replace(true)))
}

#[derive(Clone, Default)]
pub(super) struct LogBuffer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for LogBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("log buffer lock")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn capture_logs<F: FnOnce()>(f: F) -> String {
    let writer = LogBuffer::default();
    let drain = writer.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .with_target(false)
        .with_max_level(tracing::Level::INFO)
        .finish();

    tracing::subscriber::with_default(subscriber, f);

    let bytes = drain.0.lock().expect("log buffer lock").clone();
    String::from_utf8_lossy(&bytes).to_string()
}

pub(super) fn ensure_fts_schema(storage: &FrankenStorage) {
    let count: i64 = storage
        .raw()
        .query_row_map(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'fts_messages'",
            &[] as &[ParamValue],
            |row| row.get_typed(0),
        )
        .unwrap();
    if count == 0 {
        storage
            .raw()
            .execute(
                "CREATE VIRTUAL TABLE fts_messages USING fts5(
                    content,
                    title,
                    agent,
                    workspace,
                    source_path,
                    created_at UNINDEXED,
                    message_id UNINDEXED,
                    tokenize='porter'
                )",
            )
            .unwrap();
    }
    assert!(
        storage
            .raw()
            .query("SELECT rowid FROM fts_messages LIMIT 1")
            .is_ok(),
        "fts_messages should remain queryable via SQLite in tests"
    );
}

pub(super) fn norm_msg(idx: i64, created_at: i64) -> NormalizedMessage {
    NormalizedMessage {
        idx,
        role: "user".into(),
        author: Some("u".into()),
        created_at: Some(created_at),
        content: format!("msg-{idx}"),
        extra: serde_json::json!({}),
        snippets: Vec::new(),
        invocations: Vec::new(),
    }
}

pub(super) fn norm_conv(
    external_id: Option<&str>,
    msgs: Vec<NormalizedMessage>,
) -> NormalizedConversation {
    NormalizedConversation {
        agent_slug: "tester".into(),
        external_id: external_id.map(std::borrow::ToOwned::to_owned),
        title: Some("Demo".into()),
        workspace: Some(PathBuf::from("/workspace/demo")),
        source_path: PathBuf::from("/logs/demo.jsonl"),
        started_at: msgs.first().and_then(|m| m.created_at),
        ended_at: msgs.last().and_then(|m| m.created_at),
        metadata: serde_json::json!({}),
        messages: msgs,
    }
}

pub(super) fn seed_lexical_rebuild_fixture(storage: &FrankenStorage) {
    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    for (external_id, base_ts) in [
        ("lexical-fixture-1", 1_700_000_000_000_i64),
        ("lexical-fixture-2", 1_700_000_001_000_i64),
    ] {
        let conversation = Conversation {
            id: None,
            agent_slug: "codex".into(),
            workspace: Some(PathBuf::from("/tmp/workspace")),
            external_id: Some(external_id.to_string()),
            title: Some("Lexical rebuild fixture".into()),
            source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
            started_at: Some(base_ts),
            ended_at: Some(base_ts + 100),
            approx_tokens: Some(64),
            metadata_json: serde_json::Value::Null,
            messages: vec![
                Message {
                    id: None,
                    idx: 0,
                    role: MessageRole::User,
                    author: Some("user".into()),
                    created_at: Some(base_ts + 10),
                    content: format!("{external_id}-first"),
                    extra_json: serde_json::json!({"opaque": true}),
                    snippets: Vec::new(),
                },
                Message {
                    id: None,
                    idx: 1,
                    role: MessageRole::Agent,
                    author: Some("assistant".into()),
                    created_at: Some(base_ts + 20),
                    content: format!("{external_id}-second"),
                    extra_json: serde_json::json!({"opaque": true}),
                    snippets: Vec::new(),
                },
            ],
            source_id: LOCAL_SOURCE_ID.into(),
            origin_host: None,
        };
        storage
            .insert_conversation_tree(agent_id, None, &conversation)
            .unwrap();
    }
}

pub(super) fn lexical_rebuild_test_source_map(
    storage: &FrankenStorage,
) -> HashMap<String, (SourceKind, Option<String>)> {
    storage
        .list_sources()
        .unwrap_or_default()
        .into_iter()
        .map(|source| (source.id, (source.kind, source.host_label)))
        .collect()
}

pub(super) fn legacy_offset_lexical_rebuild_packets(
    storage: &FrankenStorage,
    page_size: i64,
) -> Result<Vec<LexicalRebuildConversationPacket>> {
    let source_map = lexical_rebuild_test_source_map(storage);
    let (agent_slugs, workspace_paths) = storage.build_lexical_rebuild_lookups()?;
    let mut packets = Vec::new();
    let mut offset = 0_i64;

    loop {
        let conversation_page = storage.list_conversations_for_lexical_rebuild_by_offset(
            page_size,
            offset,
            &agent_slugs,
            &workspace_paths,
        )?;
        if conversation_page.is_empty() {
            break;
        }

        offset = offset.saturating_add(i64::try_from(conversation_page.len()).unwrap_or(0));
        for conversation in conversation_page {
            let conversation_id = conversation.id.ok_or_else(|| {
                anyhow::anyhow!("legacy lexical rebuild row missing conversation id")
            })?;
            let grouped_messages = HashMap::from([(
                conversation_id,
                storage.fetch_messages_for_lexical_rebuild(conversation_id)?,
            )]);
            let mut prepared = prepare_lexical_rebuild_packet_batch(
                vec![conversation],
                grouped_messages,
                &source_map,
                None,
            )?;
            packets.push(
                prepared
                    .pop()
                    .expect("single-conversation legacy packet should be prepared"),
            );
        }
    }

    Ok(packets)
}

pub(super) fn keyset_batched_lexical_rebuild_packets(
    storage: &FrankenStorage,
    page_size: i64,
    batch_limit: usize,
) -> Result<Vec<LexicalRebuildConversationPacket>> {
    let source_map = lexical_rebuild_test_source_map(storage);
    let (agent_slugs, workspace_paths) = storage.build_lexical_rebuild_lookups()?;
    let mut packets = Vec::new();
    let mut after_conversation_id = 0_i64;
    let batch_limit = batch_limit.max(1);

    loop {
        let conversation_page = storage.list_conversations_for_lexical_rebuild_after_id(
            page_size,
            after_conversation_id,
            &agent_slugs,
            &workspace_paths,
        )?;
        if conversation_page.is_empty() {
            break;
        }
        after_conversation_id = conversation_page
            .last()
            .and_then(|conversation| conversation.id)
            .ok_or_else(|| anyhow::anyhow!("keyset lexical rebuild page missing terminal id"))?;

        for chunk in conversation_page.chunks(batch_limit) {
            let conversation_chunk = chunk.to_vec();
            let conversation_ids = conversation_chunk
                .iter()
                .filter_map(|conversation| conversation.id)
                .collect::<Vec<_>>();
            let grouped_messages =
                storage.fetch_messages_for_lexical_rebuild_batch(&conversation_ids, None, None)?;
            let mut prepared = prepare_lexical_rebuild_packet_batch(
                conversation_chunk,
                grouped_messages,
                &source_map,
                None,
            )?;
            packets.append(&mut prepared);
        }
    }

    Ok(packets)
}

/// Canonical message documents in the published index for `data_dir`.
/// Task 5 persists search-region documents in the same index
/// (`msg_idx >= REGION_DOC_BASE`); the call sites count the canonical
/// share only, matching their pre-region-document meaning.
pub(super) fn tantivy_doc_count_for_data_dir(data_dir: &Path) -> u64 {
    let index_path = index_dir(data_dir).unwrap();
    let reader = crate::search::quill_bridge::open_cass_reader(&index_path).unwrap();
    let query = crate::search::quill_bridge::canonical_message_query(
        frankensearch::quill::query::Query::all(),
    );
    crate::search::quill_bridge::search_paginated(&reader, &query, 1, 0, true)
        .expect("canonical doc count")
        .total_count
        .unwrap_or(0) as u64
}

pub(super) fn token_usage_extra(input_tokens: i64, output_tokens: i64) -> serde_json::Value {
    serde_json::json!({
        "message": {
            "model": "claude-opus-4-6",
            "usage": {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cache_read_input_tokens": input_tokens / 2,
                "cache_creation_input_tokens": input_tokens / 4,
                "service_tier": "standard"
            }
        }
    })
}

pub(super) fn large_startup_conv(
    agent_slug: &str,
    prefix: &str,
    conv_idx: usize,
    message_count: usize,
    body_bytes: usize,
    base_ts: i64,
) -> NormalizedConversation {
    let mut messages = Vec::with_capacity(message_count);
    for msg_idx in 0..message_count {
        let is_assistant = msg_idx % 2 == 1;
        let ts = base_ts
            .saturating_add((conv_idx as i64).saturating_mul(10_000))
            .saturating_add(msg_idx as i64);
        messages.push(NormalizedMessage {
            idx: msg_idx as i64,
            role: if is_assistant { "assistant" } else { "user" }.to_string(),
            author: Some(if is_assistant {
                format!("{agent_slug}-model")
            } else {
                "user".to_string()
            }),
            created_at: Some(ts),
            content: format!("{prefix}-{conv_idx}-{msg_idx}-{}", "x".repeat(body_bytes)),
            extra: if is_assistant {
                token_usage_extra(1_000 + msg_idx as i64, 500 + msg_idx as i64)
            } else {
                serde_json::json!({})
            },
            snippets: Vec::new(),
            invocations: Vec::new(),
        });
    }

    NormalizedConversation {
        agent_slug: agent_slug.to_string(),
        external_id: Some(format!("{prefix}-{conv_idx}")),
        title: Some(format!("{agent_slug} startup {conv_idx}")),
        workspace: Some(PathBuf::from(format!("/workspace/{agent_slug}/{prefix}"))),
        source_path: PathBuf::from(format!("/logs/{agent_slug}/{prefix}-{conv_idx}.jsonl")),
        started_at: messages.first().and_then(|msg| msg.created_at),
        ended_at: messages.last().and_then(|msg| msg.created_at),
        metadata: serde_json::json!({
            "agent_slug": agent_slug,
            "fixture": "large_startup"
        }),
        messages,
    }
}

pub(super) fn send_done(
    tx: &Sender<IndexMessage>,
    connector_name: &'static str,
    is_discovered: bool,
) {
    tx.send(IndexMessage::Done {
        connector_name,
        scan_ms: 1,
        is_discovered,
        scan_succeeded: true,
        active_source_skipped: false,
    })
    .expect("done message should send");
}

pub(super) struct DeferredBatchConnector;

impl Connector for DeferredBatchConnector {
    fn detect(&self) -> DetectionResult {
        DetectionResult {
            detected: true,
            evidence: vec![String::from("fixture")],
            root_paths: Vec::new(),
        }
    }

    fn scan(
        &self,
        _ctx: &crate::connectors::ScanContext,
    ) -> anyhow::Result<Vec<NormalizedConversation>> {
        Ok(vec![norm_conv(
            Some("deferred-batch"),
            vec![
                norm_msg(0, 1_700_000_000_000),
                norm_msg(1, 1_700_000_000_100),
            ],
        )])
    }

    fn scan_with_callback(
        &self,
        ctx: &crate::connectors::ScanContext,
        on_conversation: &mut dyn FnMut(NormalizedConversation) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        for conversation in self.scan(ctx)? {
            on_conversation(conversation)?;
        }
        Ok(())
    }
}

pub(super) fn deferred_batch_connector_factory() -> Box<dyn Connector + Send> {
    Box::new(DeferredBatchConnector)
}

pub(super) static BATCH_WATERMARK_FIXTURE_ROOT: Mutex<Option<PathBuf>> = Mutex::new(None);

pub(super) struct BatchWatermarkFixtureRootReset;

impl Drop for BatchWatermarkFixtureRootReset {
    fn drop(&mut self) {
        *BATCH_WATERMARK_FIXTURE_ROOT
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = None;
    }
}

pub(super) fn batch_watermark_fixture_conversation(
    agent_slug: &str,
    file_name: &str,
) -> NormalizedConversation {
    let root = BATCH_WATERMARK_FIXTURE_ROOT
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone()
        .expect("batch watermark fixture root should be configured");
    let mut conversation = norm_conv(Some(file_name), vec![norm_msg(0, 1_700_000_000_000)]);
    conversation.agent_slug = agent_slug.to_string();
    conversation.source_path = root.join(file_name);
    conversation
}

pub(super) struct ActiveBatchWatermarkConnector;

impl Connector for ActiveBatchWatermarkConnector {
    fn detect(&self) -> DetectionResult {
        DetectionResult {
            detected: true,
            evidence: vec!["active-batch-watermark-fixture".to_string()],
            root_paths: Vec::new(),
        }
    }

    fn scan(
        &self,
        _ctx: &crate::connectors::ScanContext,
    ) -> anyhow::Result<Vec<NormalizedConversation>> {
        Ok(vec![batch_watermark_fixture_conversation(
            "claude",
            "active.jsonl",
        )])
    }
}

pub(super) fn active_batch_watermark_connector_factory() -> Box<dyn Connector + Send> {
    Box::new(ActiveBatchWatermarkConnector)
}

pub(super) struct SafeBatchWatermarkConnector;

impl Connector for SafeBatchWatermarkConnector {
    fn detect(&self) -> DetectionResult {
        DetectionResult {
            detected: true,
            evidence: vec!["safe-batch-watermark-fixture".to_string()],
            root_paths: Vec::new(),
        }
    }

    fn scan(
        &self,
        _ctx: &crate::connectors::ScanContext,
    ) -> anyhow::Result<Vec<NormalizedConversation>> {
        Ok(vec![batch_watermark_fixture_conversation(
            "codex",
            "safe.jsonl",
        )])
    }
}

pub(super) fn safe_batch_watermark_connector_factory() -> Box<dyn Connector + Send> {
    Box::new(SafeBatchWatermarkConnector)
}

pub(super) struct WatermarkSensitiveRemoteConnector;

impl WatermarkSensitiveRemoteConnector {
    fn conversations_for_context(
        &self,
        ctx: &crate::connectors::ScanContext,
    ) -> Vec<NormalizedConversation> {
        if ctx.since_ts.is_some() {
            return Vec::new();
        }
        let Some(scan_root) = ctx.scan_roots.first() else {
            return Vec::new();
        };

        let mut conversation = norm_conv(
            Some("remote-watermark"),
            vec![norm_msg(0, 1_700_000_000_000)],
        );
        conversation.source_path = scan_root.path.join("remote-watermark.jsonl");
        vec![conversation]
    }
}

impl Connector for WatermarkSensitiveRemoteConnector {
    fn detect(&self) -> DetectionResult {
        DetectionResult {
            detected: true,
            evidence: vec![String::from("fixture")],
            root_paths: Vec::new(),
        }
    }

    fn scan(
        &self,
        ctx: &crate::connectors::ScanContext,
    ) -> anyhow::Result<Vec<NormalizedConversation>> {
        Ok(self.conversations_for_context(ctx))
    }

    fn scan_with_callback(
        &self,
        ctx: &crate::connectors::ScanContext,
        on_conversation: &mut dyn FnMut(NormalizedConversation) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        for conversation in self.conversations_for_context(ctx) {
            on_conversation(conversation)?;
        }
        Ok(())
    }
}

pub(super) fn watermark_sensitive_remote_connector_factory() -> Box<dyn Connector + Send> {
    Box::new(WatermarkSensitiveRemoteConnector)
}

pub(super) fn configured_local_scan_root(path: PathBuf) -> ScanRoot {
    let mut root = ScanRoot::local(path);
    root.origin = Origin {
        source_id: "backup-local".to_string(),
        kind: SourceKind::Local,
        host: None,
    };
    root.platform = Some(Platform::Linux);
    root
}

pub(super) fn ensure_since_ts_matches(
    actual: Option<i64>,
    expected: Option<i64>,
    context: &str,
) -> Result<()> {
    if matches!(actual.cmp(&expected), std::cmp::Ordering::Equal) {
        Ok(())
    } else {
        anyhow::bail!("{context}: expected {expected:?}, got {actual:?}")
    }
}

pub(super) fn ensure_canonical_mutations(
    actual: CanonicalMutationCounts,
    expected: CanonicalMutationCounts,
    context: &str,
) -> Result<()> {
    let conversations_match = matches!(
        actual
            .inserted_conversations
            .cmp(&expected.inserted_conversations),
        std::cmp::Ordering::Equal
    );
    let messages_match = matches!(
        actual.inserted_messages.cmp(&expected.inserted_messages),
        std::cmp::Ordering::Equal
    );
    if conversations_match && messages_match {
        Ok(())
    } else {
        anyhow::bail!("{context}: expected {expected:?}, got {actual:?}")
    }
}

pub(super) static FAILING_EXPLICIT_FILE_ROOT: Mutex<Option<PathBuf>> = Mutex::new(None);

pub(super) struct FailingExplicitFileRootConnector;

impl Connector for FailingExplicitFileRootConnector {
    fn detect(&self) -> DetectionResult {
        let root_path = FAILING_EXPLICIT_FILE_ROOT
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .expect("explicit file root should be configured");
        DetectionResult {
            detected: true,
            evidence: vec!["explicit-file-root".to_string()],
            root_paths: vec![root_path],
        }
    }

    fn scan(
        &self,
        _ctx: &crate::connectors::ScanContext,
    ) -> anyhow::Result<Vec<NormalizedConversation>> {
        Err(anyhow::anyhow!(
            "connector parse failed after source discovery"
        ))
    }
}

pub(super) fn failing_explicit_file_root_connector_factory() -> Box<dyn Connector + Send> {
    Box::new(FailingExplicitFileRootConnector)
}

pub(super) struct DetectedRemoteFailureConnector;

impl Connector for DetectedRemoteFailureConnector {
    fn detect(&self) -> DetectionResult {
        DetectionResult {
            detected: true,
            evidence: vec!["fixture".to_string()],
            root_paths: Vec::new(),
        }
    }

    fn scan(
        &self,
        _ctx: &crate::connectors::ScanContext,
    ) -> anyhow::Result<Vec<NormalizedConversation>> {
        Ok(Vec::new())
    }

    fn scan_with_callback(
        &self,
        ctx: &crate::connectors::ScanContext,
        _on_conversation: &mut dyn FnMut(NormalizedConversation) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        if ctx.scan_roots.is_empty() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("remote exploded"))
        }
    }
}

pub(super) fn detected_remote_failure_connector_factory() -> Box<dyn Connector + Send> {
    Box::new(DetectedRemoteFailureConnector)
}

pub(super) struct PanicConnector;

impl Connector for PanicConnector {
    fn detect(&self) -> DetectionResult {
        DetectionResult {
            detected: true,
            evidence: vec!["fixture".to_string()],
            root_paths: Vec::new(),
        }
    }

    fn scan(
        &self,
        _ctx: &crate::connectors::ScanContext,
    ) -> anyhow::Result<Vec<NormalizedConversation>> {
        Ok(Vec::new())
    }

    fn scan_with_callback(
        &self,
        _ctx: &crate::connectors::ScanContext,
        _on_conversation: &mut dyn FnMut(NormalizedConversation) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        panic!("connector panic during local scan");
    }
}

pub(super) fn panic_connector_factory() -> Box<dyn Connector + Send> {
    Box::new(PanicConnector)
}

pub(super) static DISCONNECT_TEST_COUNTER: Mutex<Option<Arc<AtomicUsize>>> = Mutex::new(None);

pub(super) struct DisconnectAwareConnector;

impl Connector for DisconnectAwareConnector {
    fn detect(&self) -> DetectionResult {
        DetectionResult {
            detected: true,
            evidence: vec!["fixture".to_string()],
            root_paths: Vec::new(),
        }
    }

    fn scan(
        &self,
        _ctx: &crate::connectors::ScanContext,
    ) -> anyhow::Result<Vec<NormalizedConversation>> {
        Ok(Vec::new())
    }

    fn scan_with_callback(
        &self,
        ctx: &crate::connectors::ScanContext,
        on_conversation: &mut dyn FnMut(NormalizedConversation) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let counter = DISCONNECT_TEST_COUNTER
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .expect("disconnect test counter should be configured");
        let scope = if ctx.scan_roots.is_empty() {
            "local"
        } else {
            "remote"
        };

        for idx in 0..3 {
            counter.fetch_add(1, Ordering::Relaxed);
            let oversized = NormalizedMessage {
                content: "x".repeat(DEFAULT_STREAMING_BATCH_LIMITS.max_chars + 1),
                ..norm_msg(idx, 2_000 + idx)
            };
            on_conversation(norm_conv(Some(scope), vec![oversized]))?;
        }

        Ok(())
    }
}

pub(super) fn disconnect_aware_connector_factory() -> Box<dyn Connector + Send> {
    Box::new(DisconnectAwareConnector)
}

pub(super) fn write_watch_codex_messages(path: &Path, id: &str, messages: &[(&str, &str, i64)]) {
    use std::io::Write as _;
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut out = Vec::new();
    let ts = messages.first().map_or(1_700_000_000_000_i64, |m| m.2);
    let iso = |ms: i64| {
        chrono::DateTime::from_timestamp_millis(ms)
            .unwrap_or_else(|| chrono::DateTime::from_timestamp_millis(0).unwrap())
            .to_rfc3339()
    };
    serde_json::to_writer(
        &mut out,
        &serde_json::json!({
            "timestamp": iso(ts),
            "type": "session_meta",
            "payload": {"id": id, "cwd": "/tmp/watch-test"}
        }),
    )
    .unwrap();
    out.push(b'\n');
    for (role, text, created_at) in messages {
        let item_type = if *role == "assistant" {
            "output_text"
        } else {
            "input_text"
        };
        serde_json::to_writer(
            &mut out,
            &serde_json::json!({
                "timestamp": iso(*created_at),
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": role,
                    "content": [{"type": item_type, "text": text}]
                }
            }),
        )
        .unwrap();
        out.push(b'\n');
    }
    let mut f = fs::File::create(path).unwrap();
    f.write_all(&out).unwrap();
}

pub(super) struct WatchLexicalFaultGuard(Option<WatchLexicalTestFault>);

impl WatchLexicalFaultGuard {
    pub(super) fn set(fault: WatchLexicalTestFault) -> Self {
        Self(WATCH_LEXICAL_TEST_FAULT.with(|slot| slot.replace(Some(fault))))
    }
}

impl Drop for WatchLexicalFaultGuard {
    fn drop(&mut self) {
        WATCH_LEXICAL_TEST_FAULT.with(|slot| slot.set(self.0));
    }
}

pub(super) fn watch_lexical_canonical_rows(
    storage: &FrankenStorage,
) -> Vec<(i64, i64, i64, String)> {
    storage
        .raw()
        .query_map_collect(
            "SELECT conversation_id, id, idx, content FROM messages ORDER BY id",
            &[],
            |row| {
                Ok((
                    row.get_typed(0)?,
                    row.get_typed(1)?,
                    row.get_typed(2)?,
                    row.get_typed(3)?,
                ))
            },
        )
        .unwrap()
}

pub(super) fn watch_lexical_search_ids(index_path: &Path, term: &str) -> BTreeSet<String> {
    let reader = crate::search::quill_bridge::open_cass_reader(index_path).unwrap();
    let parser = frankensearch::quill::query::CassQueryParser::new(
        frankensearch::quill::schema::CASS_SEMANTIC_SCHEMA,
    )
    .unwrap();
    let query = parser.parse(
        term,
        &frankensearch::quill::query::CassQueryFilters::default(),
    );
    // Task 5: persisted region documents share the index in the
    // `msg_idx >= REGION_DOC_BASE` space and their meta fragments can
    // match these probe terms; the assertions identify canonical
    // message bodies, so apply the same guard the chat path uses.
    let query = crate::search::quill_bridge::canonical_message_query(query.query);
    let page = crate::search::quill_bridge::search_paginated(&reader, &query, 32, 0, true).unwrap();
    let ids: BTreeSet<_> = page
        .hits
        .iter()
        .map(|hit| hit.document_id.clone())
        .collect();
    assert_eq!(
        ids.len(),
        page.hits.len(),
        "no duplicate lexical identities"
    );
    assert_eq!(page.total_count, Some(ids.len()));
    ids
}

pub(super) fn write_watch_lexical_source(path: &Path, id: &str, marker: &str) {
    write_watch_codex_messages(path, id, &[("user", marker, 1_700_000_000_100)]);
}

pub(super) fn watch_lexical_options(data_dir: &Path) -> IndexOptions {
    IndexOptions {
        full: false,
        watch: false,
        force_rebuild: false,
        db_path: data_dir.join("agent_search.db"),
        data_dir: data_dir.to_path_buf(),
        semantic: false,
        build_hnsw: false,
        embedder: "fastembed".into(),
        progress: None,
        watch_once_paths: None,
        watch_interval_secs: 30,
    }
}

pub(super) fn run_watch_lexical_selection(
    opts: &IndexOptions,
    selected: &Path,
    storage: &Mutex<FrankenStorage>,
    index: &Mutex<Option<TantivyIndex>>,
) -> Result<usize> {
    let mut opts = opts.clone();
    opts.watch_once_paths = Some(vec![selected.to_path_buf()]);
    reindex_paths(
        &opts,
        vec![selected.to_path_buf()],
        &[(
            ConnectorKind::Codex,
            ScanRoot::local(selected.to_path_buf()),
        )],
        &Mutex::new(HashMap::new()),
        storage,
        index,
        &index_dir(&opts.data_dir)?,
        false,
    )
}

pub(super) fn seed_archive_conversation(storage: &FrankenStorage, external_id: &str) {
    let agent = crate::model::types::Agent {
        id: None,
        slug: "tester".into(),
        name: "Tester".into(),
        version: None,
        kind: crate::model::types::AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    let conversation = crate::model::types::Conversation {
        id: None,
        agent_slug: "tester".into(),
        workspace: Some(std::path::PathBuf::from("/tmp/workspace")),
        external_id: Some(external_id.into()),
        title: Some(external_id.into()),
        source_path: std::path::PathBuf::from(format!("/tmp/{external_id}.jsonl")),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![crate::model::types::Message {
            id: None,
            idx: 0,
            role: crate::model::types::MessageRole::User,
            author: Some("user".into()),
            created_at: Some(1_700_000_000_050),
            content: format!("fingerprint cache seed {external_id}"),
            extra_json: serde_json::Value::Null,
            snippets: Vec::new(),
        }],
        source_id: crate::sources::provenance::LOCAL_SOURCE_ID.into(),
        origin_host: None,
    };
    storage
        .insert_conversations_batched(&[(agent_id, None, &conversation)])
        .unwrap();
}
