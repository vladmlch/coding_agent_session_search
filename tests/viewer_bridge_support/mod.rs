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

    fn build(fixtures: &[FixtureConversation], federated: bool) -> Result<Self> {
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
        let mut child = Command::new(env!("CARGO_BIN_EXE_cass-viewer-bridge"))
            .arg("--stdio")
            .arg("--data-dir")
            .arg(&data_dir)
            .arg("--db")
            .arg(&db)
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
