use super::super::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::franken_sync::Connection as FrankenConnection;
use crate::franken_sync::compat::{ConnectionExt as FrankenConnectionExt, RowExt};
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use crate::search::canonicalize::canonicalize_for_embedding;
use crate::search::embedder::Embedder;
use crate::search::tantivy::TantivyIndex;
use crate::search::vector_index::{ROLE_USER, VectorIndex};
use crate::storage::sqlite::FrankenStorage;
use frankensearch::lexical_tantivy::CASS_SCHEMA_HASH as FS_CASS_SCHEMA_HASH;
use serde_json::json;
use tempfile::TempDir;

// Reference implementation of the stable dedup key prior to bead num7z.
// Kept in tests so the optimized `search_hit_key_doc_id` is pinned to
// byte-identical output; any drift trips this assertion.
pub(super) fn search_hit_key_doc_id_reference_v0(key: &SearchHitKey) -> String {
    let sep = '\u{1f}';
    format!(
        "{}{sep}{}{sep}{}{sep}{}{sep}{}{sep}{}{sep}{}",
        key.source_id,
        key.source_path,
        key.conversation_id
            .map(|v| v.to_string())
            .unwrap_or_default(),
        key.title,
        key.line_number.map(|v| v.to_string()).unwrap_or_default(),
        key.created_at.map(|v| v.to_string()).unwrap_or_default(),
        key.content_hash,
    )
}

pub(super) fn stable_hit_hash_reference_v0(
    content: &str,
    source_path: &str,
    line_number: Option<usize>,
    created_at: Option<i64>,
) -> u64 {
    use xxhash_rust::xxh3::Xxh3;

    let mut hasher = Xxh3::new();
    if !content.is_empty() {
        hasher.update(&stable_content_hash(content).to_le_bytes());
    }
    hasher.update(b"|");
    hasher.update(source_path.as_bytes());
    hasher.update(b"|");
    if let Some(line) = line_number {
        hasher.update(line.to_string().as_bytes());
    }
    hasher.update(b"|");
    if let Some(ts) = created_at {
        hasher.update(ts.to_string().as_bytes());
    }
    hasher.digest()
}

pub(super) fn vector_result(message_id: u64, score: f32) -> VectorSearchResult {
    VectorSearchResult {
        message_id,
        chunk_idx: 0,
        score,
    }
}

pub(super) fn cass_layer_b_test_client(connection: Option<SearchSqliteConnection>) -> SearchClient {
    SearchClient {
        reader: LexicalReaderSlot::default(),
        sqlite: Mutex::new(connection),
        sqlite_path: None,
        prefix_cache: Mutex::new(CacheShards::new(*CACHE_TOTAL_CAP, *CACHE_BYTE_CAP)),
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: "vtest|schema:cass-layer-b".into(),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    }
}

pub(super) fn cass_layer_b_test_request<'a>(
    candidates: &'a [CassLexicalLayerBCandidate],
    session_paths: &'a HashSet<String>,
    limit: usize,
    offset: usize,
    field_mask: FieldMask,
) -> CassLexicalLayerBRequest<'a> {
    CassLexicalLayerBRequest {
        candidates,
        raw_query: "needle",
        session_paths,
        limit,
        offset,
        field_mask,
    }
}

#[derive(Debug)]
pub(super) struct FixedTestEmbedder {
    pub(super) id: String,
    pub(super) vector: Vec<f32>,
}

impl FixedTestEmbedder {
    pub(super) fn new(id: &str, vector: &[f32]) -> Self {
        Self {
            id: id.to_string(),
            vector: vector.to_vec(),
        }
    }
}

#[derive(Debug)]
pub(super) struct BlockingTestEmbedder {
    pub(super) id: String,
    pub(super) vector: Vec<f32>,
    pub(super) started_tx: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    pub(super) unblock_rx: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl BlockingTestEmbedder {
    pub(super) fn new(
        id: &str,
        vector: &[f32],
        started_tx: std::sync::mpsc::Sender<()>,
        unblock_rx: std::sync::mpsc::Receiver<()>,
    ) -> Self {
        Self {
            id: id.to_string(),
            vector: vector.to_vec(),
            started_tx: Mutex::new(Some(started_tx)),
            unblock_rx: Mutex::new(unblock_rx),
        }
    }
}

impl crate::search::embedder::Embedder for BlockingTestEmbedder {
    fn embed_sync(&self, _text: &str) -> crate::search::embedder::EmbedderResult<Vec<f32>> {
        if let Ok(mut guard) = self.started_tx.lock()
            && let Some(tx) = guard.take()
        {
            let _ = tx.send(());
        }
        self.unblock_rx
            .lock()
            .expect("blocking embedder receiver")
            .recv()
            .expect("blocking embedder unblock signal");
        Ok(self.vector.clone())
    }

    fn dimension(&self) -> usize {
        self.vector.len()
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn is_semantic(&self) -> bool {
        false
    }

    fn category(&self) -> frankensearch::ModelCategory {
        frankensearch::ModelCategory::HashEmbedder
    }
}

impl crate::search::embedder::Embedder for FixedTestEmbedder {
    fn embed_sync(&self, _text: &str) -> crate::search::embedder::EmbedderResult<Vec<f32>> {
        Ok(self.vector.clone())
    }

    fn dimension(&self) -> usize {
        self.vector.len()
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn is_semantic(&self) -> bool {
        false
    }

    fn category(&self) -> frankensearch::ModelCategory {
        frankensearch::ModelCategory::HashEmbedder
    }
}

pub(super) struct SemanticTestFixture {
    pub(super) _dir: TempDir,
    pub(super) client: SearchClient,
    pub(super) doc_ids: Vec<String>,
    pub(super) source_paths: Vec<String>,
    pub(super) vector_dir: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SemanticAnnFixtureMode {
    Missing,
    Native,
    StaleSameDocIds,
    CorruptMetadata,
    CrossRoleAlias,
    #[cfg(unix)]
    FinalComponentSymlink,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct SemanticArtifactEntrySnapshot {
    pub(super) relative_path: PathBuf,
    pub(super) file_kind: &'static str,
    pub(super) contents: Option<Vec<u8>>,
    pub(super) modified: std::time::SystemTime,
    pub(super) readonly: bool,
    #[cfg(unix)]
    pub(super) mode: u32,
}

pub(super) fn semantic_artifact_tree_snapshot(
    root: &Path,
) -> Result<Vec<SemanticArtifactEntrySnapshot>> {
    let mut snapshot = Vec::new();
    for entry in walkdir::WalkDir::new(root).min_depth(1).follow_links(false) {
        let entry = entry?;
        let path = entry.path();
        let metadata = std::fs::symlink_metadata(path)?;
        let file_type = metadata.file_type();
        let (file_kind, contents) = if file_type.is_file() {
            ("file", Some(std::fs::read(path)?))
        } else if file_type.is_dir() {
            ("directory", None)
        } else if file_type.is_symlink() {
            (
                "symlink",
                Some(
                    std::fs::read_link(path)?
                        .to_string_lossy()
                        .as_bytes()
                        .to_vec(),
                ),
            )
        } else {
            ("other", None)
        };
        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::PermissionsExt;
            metadata.permissions().mode()
        };
        snapshot.push(SemanticArtifactEntrySnapshot {
            relative_path: path.strip_prefix(root)?.to_path_buf(),
            file_kind,
            contents,
            modified: metadata.modified()?,
            readonly: metadata.permissions().readonly(),
            #[cfg(unix)]
            mode,
        });
    }
    snapshot.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(snapshot)
}

pub(super) struct ProgressiveHybridFixture {
    pub(super) _dir: TempDir,
    pub(super) client: Arc<SearchClient>,
    pub(super) query: String,
}

/// Builds a minimal SearchHit that a `--fields minimal` / `--fields
/// summary` projection would produce: the real metadata is intact, but
/// `content` and `snippet` have been scrubbed to empty strings by the
/// field-projection layer before noise classification runs. Used by
/// the bd-q6xf9 regression tests below.
pub(super) fn projected_minimal_fields_search_hit(title: &str, source_path: &str) -> SearchHit {
    SearchHit {
        title: title.to_string(),
        snippet: String::new(),
        content: String::new(),
        content_hash: 0,
        conversation_id: Some(42),
        score: 1.0,
        source_path: source_path.to_string(),
        agent: "test-agent".into(),
        workspace: "/tmp/workspace".into(),
        workspace_original: None,
        created_at: Some(1_700_000_000_000),
        line_number: Some(1),
        match_type: MatchType::default(),
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
    }
}

pub(super) fn build_semantic_test_fixture() -> Result<SemanticTestFixture> {
    build_semantic_test_fixture_with_options(false, SemanticAnnFixtureMode::Missing)
}

pub(super) fn build_semantic_test_fixture_with_ann(
    ann_mode: SemanticAnnFixtureMode,
) -> Result<SemanticTestFixture> {
    build_semantic_test_fixture_with_options(false, ann_mode)
}

pub(super) fn build_sharded_semantic_test_fixture() -> Result<SemanticTestFixture> {
    build_semantic_test_fixture_with_options(true, SemanticAnnFixtureMode::Missing)
}

pub(super) fn build_semantic_test_fixture_with_options(
    sharded: bool,
    ann_mode: SemanticAnnFixtureMode,
) -> Result<SemanticTestFixture> {
    if sharded && ann_mode != SemanticAnnFixtureMode::Missing {
        bail!("the sharded semantic fixture does not synthesize an ANN topology");
    }
    let dir = TempDir::new()?;
    let db_path = dir.path().join("cass.db");
    let storage = FrankenStorage::open(&db_path)?;

    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent)?;
    let workspace_path = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace_path)?;
    let workspace_id = storage.ensure_workspace(&workspace_path, None)?;

    let documents = [
        ("session-a.jsonl", "top semantic match", [1.0_f32, 0.0_f32]),
        (
            "session-b.jsonl",
            "middle semantic match",
            [0.9_f32, 0.1_f32],
        ),
        ("session-c.jsonl", "late semantic match", [0.8_f32, 0.2_f32]),
    ];
    let base_ts = 1_700_000_000_000_i64;
    let mut doc_ids = Vec::with_capacity(documents.len());
    let mut source_paths = Vec::with_capacity(documents.len());

    for (idx, (name, content, _vector)) in documents.iter().enumerate() {
        let source_path = dir.path().join(name);
        source_paths.push(source_path.to_string_lossy().to_string());

        let conversation = Conversation {
            id: None,
            agent_slug: agent.slug.clone(),
            workspace: Some(workspace_path.clone()),
            external_id: Some(format!("semantic-{idx}")),
            title: Some(format!("semantic session {idx}")),
            source_path,
            started_at: Some(base_ts + idx as i64),
            ended_at: Some(base_ts + idx as i64),
            approx_tokens: Some(16),
            metadata_json: json!({"fixture": "semantic_search"}),
            messages: vec![Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: Some("user".into()),
                created_at: Some(base_ts + idx as i64),
                content: (*content).to_string(),
                extra_json: json!({}),
                snippets: Vec::new(),
            }],
            source_id: crate::sources::provenance::LOCAL_SOURCE_ID.to_string(),
            origin_host: None,
        };

        storage.insert_conversation_tree(agent_id, Some(workspace_id), &conversation)?;
    }

    let message_rows: Vec<(u64, i64)> = storage.raw().query_map_collect(
        "SELECT m.id, COALESCE(m.created_at, c.started_at, 0)
         FROM messages m
         JOIN conversations c ON m.conversation_id = c.id
         ORDER BY c.id",
        &[],
        |row: &crate::franken_sync::Row| {
            let message_id: i64 = row.get_typed(0)?;
            let created_at: i64 = row.get_typed(1)?;
            Ok((u64::try_from(message_id).unwrap_or(u64::MAX), created_at))
        },
    )?;
    assert_eq!(
        message_rows.len(),
        documents.len(),
        "fixture should create 3 messages"
    );

    let filter_maps = SemanticFilterMaps::from_storage(&storage)?;
    let embedder = Arc::new(FixedTestEmbedder::new("test-fixed-2d", &[1.0, 0.0]));
    let source_hash = crc32fast::hash(crate::sources::provenance::LOCAL_SOURCE_ID.as_bytes());
    let vector_dir = dir.path().join("vector_index");
    std::fs::create_dir_all(&vector_dir)?;
    let mut vector_records = Vec::with_capacity(documents.len());

    for ((message_id, created_at_ms), (_, _, vector)) in message_rows.iter().zip(documents) {
        let doc_id = SemanticDocId {
            message_id: *message_id,
            chunk_idx: 0,
            agent_id: u32::try_from(agent_id)?,
            workspace_id: u32::try_from(workspace_id)?,
            source_id: source_hash,
            role: ROLE_USER,
            created_at_ms: *created_at_ms,
            content_hash: None,
        }
        .to_doc_id_string();
        doc_ids.push(doc_id.clone());
        vector_records.push((doc_id, vector));
    }

    let mut artifacts = Vec::new();
    if sharded {
        for (shard_index, chunk) in vector_records.chunks(2).enumerate() {
            let vector_path = vector_dir.join(format!("shard-{shard_index}.fsvi"));
            let mut writer = VectorIndex::create_with_revision(
                &vector_path,
                embedder.id(),
                "rev-1",
                embedder.dimension(),
                frankensearch::index::Quantization::F16,
            )?;
            for (doc_id, vector) in chunk {
                writer.write_record(doc_id, vector)?;
            }
            writer.finish()?;
            artifacts.push(SemanticIndexArtifact::open(&vector_path, None)?);
        }
    } else {
        let vector_path = vector_dir.join("index-test-fixed-2d.fsvi");
        let mut writer = VectorIndex::create_with_revision(
            &vector_path,
            embedder.id(),
            "rev-1",
            embedder.dimension(),
            frankensearch::index::Quantization::F16,
        )?;
        for (doc_id, vector) in &vector_records {
            writer.write_record(doc_id, vector)?;
        }
        writer.finish()?;
        let ann_path = match ann_mode {
            SemanticAnnFixtureMode::Missing => None,
            SemanticAnnFixtureMode::Native => {
                let ann_path = vector_dir.join("selected-native-ann.chsw");
                let source_index = VectorIndex::open(&vector_path)?;
                let ann = FsHnswIndex::build_from_vector_index(
                    &source_index,
                    frankensearch::index::HnswConfig::default(),
                )?;
                ann.save(&ann_path)?;
                Some(ann_path)
            }
            SemanticAnnFixtureMode::StaleSameDocIds => {
                let stale_source_path = vector_dir.join("stale-ann-source.fsvi");
                let mut stale_writer = VectorIndex::create_with_revision(
                    &stale_source_path,
                    embedder.id(),
                    "stale-ann-rev",
                    embedder.dimension(),
                    frankensearch::index::Quantization::F16,
                )?;
                let stale_vectors = [[0.0_f32, 1.0_f32], [0.1_f32, 0.9_f32], [1.0_f32, 0.0_f32]];
                for ((doc_id, _), stale_vector) in vector_records.iter().zip(stale_vectors.iter()) {
                    stale_writer.write_record(doc_id, stale_vector)?;
                }
                stale_writer.finish()?;
                let stale_source_index = VectorIndex::open(&stale_source_path)?;
                let ann = FsHnswIndex::build_from_vector_index(
                    &stale_source_index,
                    frankensearch::index::HnswConfig::default(),
                )?;
                let ann_path = vector_dir.join("selected-stale-ann.chsw");
                ann.save(&ann_path)?;
                Some(ann_path)
            }
            SemanticAnnFixtureMode::CorruptMetadata => {
                let ann_path = vector_dir.join("selected-corrupt-ann.chsw");
                std::fs::write(&ann_path, b"not HNSW metadata")?;
                Some(ann_path)
            }
            SemanticAnnFixtureMode::CrossRoleAlias => {
                let ann_path = vector_dir.join("selected-aliased-ann.chsw");
                std::fs::hard_link(&vector_path, &ann_path)?;
                Some(ann_path)
            }
            #[cfg(unix)]
            SemanticAnnFixtureMode::FinalComponentSymlink => {
                use std::os::unix::fs::symlink;

                let target = vector_dir.join("selected-ann-target.chsw");
                std::fs::write(&target, b"not selected through an alias")?;
                let ann_path = vector_dir.join("selected-ann-symlink.chsw");
                symlink(&target, &ann_path)?;
                Some(ann_path)
            }
        };
        artifacts.push(SemanticIndexArtifact::open(&vector_path, ann_path)?);
    }
    drop(storage);

    let client = SearchClient::open(dir.path(), Some(&db_path))?.expect("db-backed client");
    client.set_semantic_artifacts_context(embedder, artifacts, None, filter_maps, None)?;

    Ok(SemanticTestFixture {
        _dir: dir,
        client,
        doc_ids,
        source_paths,
        vector_dir,
    })
}

pub(super) fn build_progressive_hybrid_fixture() -> Result<ProgressiveHybridFixture> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    let workspace_path = dir.path().join("workspace");
    std::fs::create_dir_all(&workspace_path)?;
    let agent_id = 1_i64;
    let workspace_id = 1_i64;
    let source_id = crate::sources::provenance::LOCAL_SOURCE_ID;
    let source_hash = crc32fast::hash(source_id.as_bytes());
    let conn = SearchSqliteFixture::in_memory()?;
    conn.execute_batch(
        r#"
        CREATE TABLE agents (
            id INTEGER PRIMARY KEY,
            slug TEXT NOT NULL
        );
        CREATE TABLE workspaces (
            id INTEGER PRIMARY KEY,
            path TEXT NOT NULL
        );
        CREATE TABLE sources (
            id TEXT PRIMARY KEY,
            kind TEXT NOT NULL
        );
        CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent_id INTEGER NOT NULL,
            workspace_id INTEGER,
            title TEXT,
            source_path TEXT NOT NULL,
            source_id TEXT NOT NULL,
            origin_host TEXT,
            started_at INTEGER
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            idx INTEGER NOT NULL,
            role TEXT NOT NULL,
            created_at INTEGER,
            content TEXT NOT NULL
        );
        "#,
    )?;
    conn.execute_compat(
        "INSERT INTO agents (id, slug) VALUES (?1, ?2)",
        params![agent_id, "codex"],
    )?;
    conn.execute_compat(
        "INSERT INTO workspaces (id, path) VALUES (?1, ?2)",
        params![workspace_id, workspace_path.to_string_lossy().to_string()],
    )?;
    conn.execute_compat(
        "INSERT INTO sources (id, kind) VALUES (?1, ?2)",
        params![source_id, "local"],
    )?;

    let query = "oauth refresh token middleware session cache".to_string();
    let filler = " context window ranking provenance semantic upgrade lexical overlay";
    let base_ts = 1_700_000_100_000_i64;
    let doc_count = 64usize;
    let mut message_rows = Vec::with_capacity(doc_count);

    for idx in 0..doc_count {
        let conversation_id = i64::try_from(idx + 1)?;
        let message_id = u64::try_from(idx + 1)?;
        let source_path = dir.path().join(format!("progressive-{idx:03}.jsonl"));
        let repeated = filler.repeat(48);
        let content = if idx % 4 == 0 {
            format!("{query} hot path candidate {idx} with detailed search diagnostics.{repeated}")
        } else if idx % 4 == 1 {
            format!(
                "search pipeline benchmark {idx} with lexical overlay and semantic ranking.{repeated}"
            )
        } else if idx % 4 == 2 {
            format!(
                "interactive typing debounce benchmark {idx} for hybrid two tier search.{repeated}"
            )
        } else {
            format!(
                "unrelated background chatter {idx} about build systems and formatting checks.{repeated}"
            )
        };
        let created_at = base_ts + idx as i64;
        let source_path_str = source_path.to_string_lossy().to_string();
        let title = format!("progressive fixture {idx}");

        conn.execute_compat(
            "INSERT INTO conversations (
                id, agent_id, workspace_id, title, source_path, source_id, origin_host, started_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7)",
            params![
                conversation_id,
                agent_id,
                workspace_id,
                title,
                source_path_str.clone(),
                source_id,
                created_at
            ],
        )?;
        conn.execute_compat(
            "INSERT INTO messages (
                id, conversation_id, idx, role, created_at, content
             ) VALUES (?1, ?2, 0, 'user', ?3, ?4)",
            params![
                i64::try_from(message_id)?,
                conversation_id,
                created_at,
                content.clone()
            ],
        )?;
        message_rows.push((message_id, created_at, content.clone()));

        let normalized = NormalizedConversation {
            agent_slug: "codex".into(),
            external_id: Some(format!("progressive-{idx}")),
            title: Some(format!("progressive fixture {idx}")),
            workspace: Some(workspace_path.clone()),
            source_path,
            started_at: Some(created_at),
            ended_at: Some(created_at),
            metadata: json!({}),
            messages: vec![NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: Some("user".into()),
                created_at: Some(created_at),
                content,
                extra: json!({}),
                snippets: Vec::new(),
                invocations: Vec::new(),
            }],
        };
        index.add_conversation(&normalized)?;
    }
    index.commit()?;

    assert_eq!(
        message_rows.len(),
        doc_count,
        "fixture should create the requested number of messages"
    );

    let fast_embedder = Arc::new(crate::search::hash_embedder::HashEmbedder::new(256));
    let quality_embedder = crate::search::hash_embedder::HashEmbedder::new(384);
    let filter_maps = SemanticFilterMaps::for_tests(
        HashMap::from([("codex".to_string(), u32::try_from(agent_id)?)]),
        HashMap::from([(
            workspace_path.to_string_lossy().to_string(),
            u32::try_from(workspace_id)?,
        )]),
        HashMap::from([(source_id.to_string(), source_hash)]),
        HashSet::new(),
    );
    let fast_path = dir.path().join("selected-fast-cass.fsvi");
    let quality_path = dir.path().join("selected-quality-cass.fsvi");

    let mut fast_writer = VectorIndex::create_with_revision(
        &fast_path,
        fast_embedder.id(),
        "rev-progressive-fast",
        fast_embedder.dimension(),
        frankensearch::index::Quantization::F16,
    )?;
    let mut quality_writer = VectorIndex::create_with_revision(
        &quality_path,
        quality_embedder.id(),
        "rev-progressive-quality",
        quality_embedder.dimension(),
        frankensearch::index::Quantization::F16,
    )?;

    for (message_id, created_at_ms, content) in &message_rows {
        let canonical = canonicalize_for_embedding(content);
        let doc_id = SemanticDocId {
            message_id: *message_id,
            chunk_idx: 0,
            agent_id: u32::try_from(agent_id)?,
            workspace_id: u32::try_from(workspace_id)?,
            source_id: source_hash,
            role: ROLE_USER,
            created_at_ms: *created_at_ms,
            content_hash: Some(content_hash(&canonical)),
        }
        .to_doc_id_string();

        let fast_vec = fast_embedder.embed_sync(content)?;
        fast_writer.write_record(&doc_id, &fast_vec)?;
        let quality_vec = quality_embedder.embed_sync(content)?;
        quality_writer.write_record(&doc_id, &quality_vec)?;
    }
    fast_writer.finish()?;
    quality_writer.finish()?;
    std::fs::write(
        dir.path().join("vector.fast.idx"),
        b"corrupt fixed-name decoy",
    )?;
    std::fs::write(
        dir.path().join("vector.idx"),
        b"corrupt fallback-name decoy",
    )?;
    std::fs::write(
        dir.path().join("vector.quality.idx"),
        b"corrupt quality-name decoy",
    )?;

    let reader = crate::search::quill_bridge::open_cass_reader(dir.path())
        .ok()
        .map(|reader| {
            (
                reader,
                crate::search::quill_bridge::QuillCassFields::compiled(),
            )
        });
    let client = SearchClient {
        reader: LexicalReaderSlot::new(dir.path().to_path_buf(), reader),
        sqlite: Mutex::new(Some(conn.into_connection())),
        sqlite_path: None,
        prefix_cache: Mutex::new(CacheShards::new(*CACHE_TOTAL_CAP, *CACHE_BYTE_CAP)),
        reload_on_search: true,
        strict_read_only: false,
        last_reload: Mutex::new(None),
        last_generation: Mutex::new(None),
        reload_epoch: Arc::new(AtomicU64::new(0)),
        warm_tx: None,
        _warm_handle: None,
        metrics: Metrics::default(),
        cache_namespace: format!("v{}|schema:{}", CACHE_KEY_VERSION, FS_CASS_SCHEMA_HASH),
        semantic: Mutex::new(None),
        last_tantivy_total_count: Mutex::new(None),
        last_lexical_degrade_reason: Mutex::new(None),
        last_wildcard_fallback_skip: Mutex::new(None),
    };
    let semantic_embedder: Arc<dyn Embedder> = fast_embedder;
    client.set_semantic_context(
        semantic_embedder,
        SemanticIndexArtifact::open(&fast_path, None)?,
        Some(SemanticIndexArtifact::open(&quality_path, None)?),
        filter_maps,
        None,
    )?;

    Ok(ProgressiveHybridFixture {
        _dir: dir,
        client: Arc::new(client),
        query,
    })
}

pub(super) fn sanitize_query(raw: &str) -> String {
    nfc_sanitize_query(raw)
}

pub(super) fn parse_boolean_query(query: &str) -> Vec<FsCassQueryToken> {
    fs_cass_parse_boolean_query(query)
}

pub(super) fn sqlite_master_name_count(db_path: &Path, name: &str) -> Result<i64> {
    let conn = FrankenConnection::open(db_path.to_string_lossy().as_ref())?;
    Ok(conn.query_row_map(
        "SELECT COUNT(*) FROM sqlite_master WHERE name = ?1",
        &[ParamValue::from(name)],
        |row| row.get_typed(0),
    )?)
}

pub(super) type QueryToken = FsCassQueryToken;
pub(super) type WildcardPattern = FsCassWildcardPattern;
pub(super) type QueryTokenList = Vec<QueryToken>;

// ==========================================================================
// Quickselect Top-K Tests (Opt 2.5)
// ==========================================================================

pub(super) fn make_fused_hit(
    id: &str,
    rrf: f32,
    lexical: Option<usize>,
    semantic: Option<usize>,
) -> FusedHit {
    FusedHit {
        key: SearchHitKey {
            source_id: "local".to_string(),
            source_path: id.to_string(),
            conversation_id: None,
            title: String::new(),
            line_number: None,
            created_at: None,
            content_hash: 0,
        },
        score: HybridScore {
            rrf,
            lexical_rank: lexical,
            semantic_rank: semantic,
            lexical_score: None,
            semantic_score: None,
        },
        hit: SearchHit {
            title: id.into(),
            snippet: "".into(),
            content: "".into(),
            content_hash: 0,
            score: rrf,
            source_path: id.into(),
            agent: "test".into(),
            workspace: "test".into(),
            workspace_original: None,
            created_at: None,
            line_number: None,
            match_type: MatchType::Exact,
            source_id: "local".into(),
            origin_kind: "local".into(),
            origin_host: None,
            conversation_id: None,
        },
    }
}

pub(super) fn make_federated_merge_hit(id: &str, agent: &str) -> SearchHit {
    SearchHit {
        title: id.into(),
        snippet: String::new(),
        content: id.into(),
        content_hash: stable_content_hash(id),
        score: 0.0,
        source_path: format!("{id}.jsonl"),
        agent: agent.into(),
        workspace: "workspace".into(),
        workspace_original: None,
        created_at: Some(1_700_000_000_000),
        line_number: Some(1),
        match_type: MatchType::Exact,
        source_id: "local".into(),
        origin_kind: "local".into(),
        origin_host: None,
        conversation_id: None,
    }
}

pub(super) fn make_federated_ranked_hit(
    shard_index: usize,
    shard_rank: usize,
    id: &str,
) -> FederatedRankedHit {
    FederatedRankedHit {
        hit: make_federated_merge_hit(id, &format!("shard-{shard_index}")),
        shard_index,
        shard_rank,
        fused_score: federated_rrf_score(shard_rank),
    }
}

pub(super) fn lexical_cache_fixture() -> Result<TempDir> {
    let dir = TempDir::new()?;
    let mut index = TantivyIndex::open_or_create(dir.path())?;
    for (id, content) in [
        "cacheword cacheword cacheword cacheword cacheword cacheword rareword",
        "cacheword rareword rareword rareword rareword rareword rareword",
        "cacheword fillerword one two three four five six seven eight nine ten",
        "cacheword fillerword eleven twelve thirteen fourteen fifteen sixteen seventeen",
        "cacheword fillerword eighteen nineteen twenty twentyone twentytwo twentythree",
        "cacheword fillerword twentyfour twentyfive twentysix twentyseven twentyeight",
    ]
    .into_iter()
    .enumerate()
    {
        index.add_conversation(&NormalizedConversation {
            agent_slug: "codex".into(),
            external_id: Some(format!("cache-session-{id}")),
            title: Some(format!("Cache fixture {id}")),
            workspace: Some("/cache-fixture".into()),
            source_path: dir.path().join(format!("session-{id}.jsonl")),
            started_at: Some(1_700_000_000_000 + id as i64),
            ended_at: None,
            metadata: json!({}),
            messages: vec![NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: None,
                created_at: Some(1_700_000_000_000 + id as i64),
                content: content.into(),
                extra: json!({}),
                snippets: vec![],
                invocations: vec![],
            }],
        })?;
    }
    index.commit()?;
    Ok(dir)
}

pub(super) fn open_cache_test_client(path: &Path) -> Result<SearchClient> {
    // Background prewarming has its own tests. Keep publication and
    // epoch changes synchronous in these cache-equivalence regressions.
    SearchClient::open_with_options(
        path,
        None,
        SearchClientOptions {
            enable_reload: true,
            enable_warm: false,
            strict_read_only: true,
        },
    )?
    .ok_or_else(|| anyhow!("cache fixture must have a lexical reader"))
}

pub(super) fn make_test_hit(id: &str, score: f32) -> SearchHit {
    SearchHit {
        title: id.to_string(),
        snippet: String::new(),
        content: id.to_string(),
        content_hash: stable_content_hash(id),
        score,
        source_path: format!("/path/{}.jsonl", id),
        agent: "test".to_string(),
        workspace: "/workspace".to_string(),
        workspace_original: None,
        created_at: Some(1_700_000_000_000),
        line_number: Some(1),
        match_type: MatchType::Exact,
        source_id: "local".to_string(),
        origin_kind: "local".to_string(),
        origin_host: None,
        conversation_id: None,
    }
}
