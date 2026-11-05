//! Shared disposable integration fixture; every child is owned and reaped here.
#![allow(dead_code)] // Each integration target uses a different subset of this fixture API.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use anyhow::{Context, Result, ensure};
use coding_agent_search::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use coding_agent_search::search::quill_bridge::QuillCassIndex;
use coding_agent_search::search::tantivy::{
    expected_index_dir, publish_federated_searchable_index_directories,
};
use coding_agent_search::storage::sqlite::FrankenStorage;
use frankensearch::quill::cass::CassDocument;
use serde_json::{Value, json};

pub struct FixtureConversation {
    pub id: i64,
    pub source_id: String,
    pub text: String,
    pub count: usize,
}

impl FixtureConversation {
    pub fn repeated(id: i64, source_id: &str, text: &str, count: usize) -> Self {
        Self {
            id,
            source_id: source_id.into(),
            text: text.into(),
            count,
        }
    }
}

/// A single message in a fully specified seed row (list_sessions fixtures
/// need provider/workspace/timestamp control the canned fixture lacks).
#[derive(Debug, Clone)]
pub struct SeedMessage {
    pub idx: i64,
    pub created_at: Option<i64>,
    pub role: MessageRole,
    pub content: String,
}

/// Fully specified canonical row for `list_sessions` fixtures: arbitrary
/// provider slug, workspace path, timestamps and provenance — the canned
/// `FixtureConversation` always seeds `codex` with `None` workspace and
/// fixed timestamps, which cannot exercise two-provider ordering, NULL
/// activity or project filters.
#[derive(Debug, Clone)]
pub struct SeedConversation {
    /// Expected canonical `conversations.id` (insertion order = rowid).
    pub id: i64,
    pub agent_slug: String,
    pub source_id: String,
    pub external_id: Option<String>,
    pub source_path: String,
    pub workspace_path: Option<String>,
    pub title: Option<String>,
    pub started_at: Option<i64>,
    pub ended_at: Option<i64>,
    pub origin_host: Option<String>,
    pub metadata: Value,
    pub messages: Vec<SeedMessage>,
}

impl SeedConversation {
    /// Minimal row: `count` agent messages at `created_at` (or NULL).
    pub fn new(id: i64, agent_slug: &str, source_path: &str, external_id: &str) -> Self {
        Self {
            id,
            agent_slug: agent_slug.into(),
            source_id: "local".into(),
            external_id: Some(external_id.into()),
            source_path: source_path.into(),
            workspace_path: None,
            title: Some(format!("fixture-{id}")),
            started_at: None,
            ended_at: None,
            origin_host: None,
            metadata: json!({"fixture":true}),
            messages: Vec::new(),
        }
    }

    pub fn workspace(mut self, path: &str) -> Self {
        self.workspace_path = Some(path.into());
        self
    }

    pub fn started_at(mut self, ms: Option<i64>) -> Self {
        self.started_at = ms;
        self
    }

    pub fn ended_at(mut self, ms: Option<i64>) -> Self {
        self.ended_at = ms;
        self
    }

    /// One agent message at `idx`; `created_at = Some(base + idx)` per call.
    pub fn agent_messages(mut self, created_at: Option<i64>, count: usize) -> Self {
        self.messages = (0..count as i64)
            .map(|idx| SeedMessage {
                idx,
                created_at: created_at.map(|base| base + idx),
                role: MessageRole::Agent,
                content: format!("seed text {}", self.id),
            })
            .collect();
        self
    }
}

#[derive(Debug)]
pub struct RawFixtureHit {
    pub conversation_id: i64,
    pub source_id: String,
    pub message_index: u64,
    pub reader_key: String,
    pub score: f64,
}

fn write_index(path: &Path, docs: &[CassDocument]) -> Result<()> {
    let mut index = QuillCassIndex::open_or_create(path)?;
    index.add_cass_documents(docs)?;
    index.commit()?;
    std::fs::write(
        path.join("schema_hash.json"),
        serde_json::to_vec(&json!({
            "schema_hash": coding_agent_search::search::tantivy::SCHEMA_HASH,
        }))?,
    )?;
    Ok(())
}

fn canonical_fixture(
    db: &Path,
    fixtures: &[FixtureConversation],
) -> Result<Vec<Vec<CassDocument>>> {
    let storage = FrankenStorage::open(db)?;
    let agent = storage.ensure_agent(&Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: None,
        kind: AgentKind::Cli,
    })?;
    let mut groups = Vec::new();
    for fixture in fixtures {
        let path = format!("/missing/source-{}.jsonl", fixture.id);
        let outcome = storage.insert_conversation_tree(
            agent,
            None,
            &Conversation {
                id: None,
                agent_slug: "codex".into(),
                workspace: None,
                external_id: Some(format!("fixture-{}", fixture.id)),
                title: Some("fixture".into()),
                source_path: path.clone().into(),
                started_at: None,
                ended_at: None,
                approx_tokens: None,
                metadata_json: json!({"fixture":true}),
                source_id: fixture.source_id.clone(),
                origin_host: None,
                messages: (0..fixture.count)
                    .map(|idx| Message {
                        id: None,
                        idx: idx as i64,
                        role: MessageRole::Agent,
                        author: None,
                        created_at: Some(1_700_000_000_000 + idx as i64),
                        content: fixture.text.clone(),
                        extra_json: json!({"fixture":idx}),
                        snippets: Vec::new(),
                    })
                    .collect(),
            },
        )?;
        ensure!(
            outcome.conversation_id == fixture.id,
            "Fixture IDs must follow canonical insertion order"
        );
        groups.push(
            (0..fixture.count)
                .map(|idx| CassDocument {
                    agent: "codex".into(),
                    workspace: None,
                    workspace_original: None,
                    source_path: path.clone(),
                    msg_idx: idx as u64,
                    created_at: Some(1_700_000_000_000),
                    title: Some("fixture".into()),
                    content: fixture.text.clone(),
                    source_id: fixture.source_id.clone(),
                    origin_kind: if fixture.source_id == "local" {
                        "local"
                    } else {
                        "remote"
                    }
                    .into(),
                    origin_host: None,
                    conversation_id: Some(outcome.conversation_id),
                })
                .collect(),
        );
    }
    drop(storage);
    Ok(groups)
}

/// Seed canonical rows with full control (providers, workspaces,
/// timestamps). Insertion order defines the canonical `conversations.id`.
fn canonical_seed(db: &Path, fixtures: &[SeedConversation]) -> Result<()> {
    let storage = FrankenStorage::open(db)?;
    let mut agents: BTreeMap<String, i64> = BTreeMap::new();
    let mut workspaces: BTreeMap<String, i64> = BTreeMap::new();
    for fixture in fixtures {
        let agent_id = match agents.get(&fixture.agent_slug) {
            Some(id) => *id,
            None => {
                let id = storage.ensure_agent(&Agent {
                    id: None,
                    slug: fixture.agent_slug.clone(),
                    name: fixture.agent_slug.clone(),
                    version: None,
                    kind: AgentKind::Cli,
                })?;
                agents.insert(fixture.agent_slug.clone(), id);
                id
            }
        };
        let workspace_id = match &fixture.workspace_path {
            Some(path) => match workspaces.get(path) {
                Some(id) => Some(*id),
                None => {
                    let id = storage.ensure_workspace(Path::new(path), None)?;
                    workspaces.insert(path.clone(), id);
                    Some(id)
                }
            },
            None => None,
        };
        let outcome = storage.insert_conversation_tree(
            agent_id,
            workspace_id,
            &Conversation {
                id: None,
                agent_slug: fixture.agent_slug.clone(),
                workspace: fixture.workspace_path.as_ref().map(PathBuf::from),
                external_id: fixture.external_id.clone(),
                title: fixture.title.clone(),
                source_path: fixture.source_path.clone().into(),
                started_at: fixture.started_at,
                ended_at: fixture.ended_at,
                approx_tokens: None,
                metadata_json: fixture.metadata.clone(),
                source_id: fixture.source_id.clone(),
                origin_host: fixture.origin_host.clone(),
                messages: fixture
                    .messages
                    .iter()
                    .map(|msg| Message {
                        id: None,
                        idx: msg.idx,
                        role: msg.role.clone(),
                        author: None,
                        created_at: msg.created_at,
                        content: msg.content.clone(),
                        extra_json: json!({"fixture":msg.idx}),
                        snippets: Vec::new(),
                    })
                    .collect(),
            },
        )?;
        ensure!(
            outcome.conversation_id == fixture.id,
            "Seed rows must follow canonical insertion order: expected {}, got {}",
            fixture.id,
            outcome.conversation_id
        );
    }
    drop(storage);
    Ok(())
}

pub struct BridgeHarness {
    temp: tempfile::TempDir,
    pub data_dir: PathBuf,
    pub db: PathBuf,
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    next_id: u64,
}

impl BridgeHarness {
    pub fn empty() -> Result<Self> {
        Self::with_conversations(&[])
    }

    pub fn with_conversations(fixtures: &[FixtureConversation]) -> Result<Self> {
        Self::build(fixtures, false)
    }

    pub fn with_federated(fixtures: &[FixtureConversation]) -> Result<Self> {
        Self::build(fixtures, true)
    }

    /// Same fixture served by a bridge child launched with the explicit
    /// local write authorization (`--enable-viewer-writes`). Only the narrow
    /// `cass_owned_queries_v1` write ops unlock; arbitrary SQL stays refused.
    pub fn writable_with_conversations(fixtures: &[FixtureConversation]) -> Result<Self> {
        Self::build_with_extra_args(fixtures, false, &["--enable-viewer-writes"])
    }

    /// Fully specified canonical rows (Task 5 `list_sessions` fixtures):
    /// arbitrary providers, workspaces and timestamps with an empty lexical
    /// index. The write lane stays off unless `extra_args` authorizes it.
    pub fn with_seed(fixtures: &[SeedConversation], extra_args: &[&str]) -> Result<Self> {
        let temp = tempfile::tempdir()?;
        let data_dir = temp.path().join("data");
        std::fs::create_dir(&data_dir)?;
        let db = data_dir.join("archive.db");
        canonical_seed(&db, fixtures)?;
        let path = expected_index_dir(&data_dir);
        write_index(&path, &[])?;
        Self::spawn(temp, data_dir, db, extra_args)
    }

    /// Spawn a bridge against an externally prepared fixture: the caller
    /// owns the tempdir and has already seeded the canonical DB and the
    /// published lexical index at `expected_index_dir(data_dir)`. Used by
    /// fixtures that need per-message Quill documents the canned
    /// `FixtureConversation`/`SeedConversation` paths cannot express.
    pub fn spawn_fixture(
        temp: tempfile::TempDir,
        data_dir: PathBuf,
        db: PathBuf,
        extra_args: &[&str],
    ) -> Result<Self> {
        Self::spawn(temp, data_dir, db, extra_args)
    }

    fn spawn(
        temp: tempfile::TempDir,
        data_dir: PathBuf,
        db: PathBuf,
        extra_args: &[&str],
    ) -> Result<Self> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_cass-viewer-bridge"));
        command
            .arg("--stdio")
            .arg("--data-dir")
            .arg(&data_dir)
            .arg("--db")
            .arg(&db);
        for arg in extra_args {
            command.arg(arg);
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let input = child.stdin.take().context("Child stdin")?;
        let output = BufReader::new(child.stdout.take().context("Child stdout")?);
        Ok(Self {
            temp,
            data_dir,
            db,
            child,
            input,
            output,
            next_id: 100,
        })
    }

    fn build(fixtures: &[FixtureConversation], federated: bool) -> Result<Self> {
        Self::build_with_extra_args(fixtures, federated, &[])
    }

    fn build_with_extra_args(
        fixtures: &[FixtureConversation],
        federated: bool,
        extra_args: &[&str],
    ) -> Result<Self> {
        let temp = tempfile::tempdir()?;
        let data_dir = temp.path().join("data");
        std::fs::create_dir(&data_dir)?;
        let db = data_dir.join("archive.db");
        let groups = canonical_fixture(&db, fixtures)?;
        let path = expected_index_dir(&data_dir);
        if federated {
            let mut shards = Vec::new();
            for (idx, docs) in groups.iter().enumerate() {
                let shard = temp.path().join(format!("shard-{idx}"));
                write_index(&shard, docs)?;
                shards.push(shard);
            }
            publish_federated_searchable_index_directories(&path, &shards)?;
        } else {
            let docs = groups.into_iter().flatten().collect::<Vec<_>>();
            write_index(&path, &docs)?;
        }
        Self::spawn(temp, data_dir, db, extra_args)
    }

    pub fn exchange(&mut self, mut value: Value) -> Result<Value> {
        if value.get("id").is_none() {
            value["id"] = self.next_id.into();
            self.next_id += 1;
        }
        serde_json::to_writer(&mut self.input, &value)?;
        self.input.write_all(b"\n")?;
        self.input.flush()?;
        let mut line = String::new();
        ensure!(
            self.output.read_line(&mut line)? > 0,
            "Bridge closed without a complete reply"
        );
        ensure!(
            line.len() <= 1_048_576 && line.ends_with('\n'),
            "Invalid reply framing"
        );
        let reply: Value = serde_json::from_str(&line)?;
        ensure!(
            reply["id"] == value["id"] && reply["schema_version"] == 1,
            "Reply correlation"
        );
        Ok(reply)
    }

    pub fn enumerate_all(&mut self, query: &str, page_size: usize) -> Result<Vec<RawFixtureHit>> {
        let mut page = self.exchange(json!({"op":"rank_begin","query":query}))?;
        let (mut hits, mut epoch, mut total) = (Vec::new(), None, None);
        loop {
            ensure!(page["ok"] == true, "Raw enumeration failed: {page}");
            let body = &page["result"];
            let observed_epoch = body["index_epoch"]
                .as_str()
                .context("index epoch")?
                .to_owned();
            if let Some(previous) = &epoch {
                ensure!(previous == &observed_epoch, "Mixed index epochs");
            }
            epoch = Some(observed_epoch);
            let observed_total = body["exact_raw_total"].as_u64().context("raw total")?;
            if let Some(previous) = total {
                ensure!(previous == observed_total, "Unstable raw total");
            }
            total = Some(observed_total);
            for row in body["hits"].as_array().context("raw hits")? {
                hits.push(RawFixtureHit {
                    conversation_id: row["conversation_id"].as_i64().context("conversation id")?,
                    source_id: row["source_id"].as_str().context("source id")?.to_owned(),
                    message_index: row["message_index_zero_based"]
                        .as_u64()
                        .context("message index")?,
                    reader_key: row["reader_key"].as_str().context("reader key")?.to_owned(),
                    score: row["score"].as_f64().context("score")?,
                });
            }
            if body["enumeration_complete"] == true {
                ensure!(
                    body["next_cursor"].is_null(),
                    "Completed raw cursor retained"
                );
                ensure!(
                    Some(hits.len() as u64) == total,
                    "Exact raw total lost messages"
                );
                return Ok(hits);
            }
            let cursor = body["next_cursor"].as_str().context("continuation")?;
            page = self.exchange(json!({"op":"rank_next","cursor":cursor,"max_hits":page_size}))?;
        }
    }

    pub fn tree(&self) -> Result<BTreeMap<PathBuf, Vec<u8>>> {
        fn visit(root: &Path, path: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) -> Result<()> {
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    visit(root, &entry.path(), files)?;
                } else {
                    files.insert(
                        entry.path().strip_prefix(root)?.to_owned(),
                        std::fs::read(entry.path())?,
                    );
                }
            }
            Ok(())
        }
        let mut files = BTreeMap::new();
        visit(self.temp.path(), self.temp.path(), &mut files)?;
        Ok(files)
    }

    pub fn shutdown(&mut self) -> Result<()> {
        let reply = self.exchange(json!({"op":"shutdown"}))?;
        ensure!(reply["ok"] == true, "Shutdown refused");
        ensure!(self.child.wait()?.success(), "Bridge exited unsuccessfully");
        Ok(())
    }
}

impl Drop for BridgeHarness {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
