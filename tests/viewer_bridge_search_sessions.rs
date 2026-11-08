//! Task 9: `search_sessions` — session-level lexical search over Quill +
//! typed SQL constraints, evaluated against a gold fixture corpus.
//!
//! Pins: exact DSL membership (same-block AND, negation, tool/error/scope
//! predicates, category/provider/tag/timestamp controls), deep-link
//! witnesses, generation-bound cursors without duplicate sessions across
//! pages, and typed refusals for unsupported shapes/modes.
#![allow(clippy::missing_errors_doc)]
#[path = "../src/viewer_bridge/protocol.rs"]
#[allow(dead_code)]
mod protocol;
#[path = "../src/viewer_bridge/revisions.rs"]
#[allow(dead_code)]
mod revisions;
#[path = "viewer_bridge_support/mod.rs"]
mod support;

use anyhow::{Result, ensure};
use coding_agent_search::franken_sync::compat::{RowExt, param_slice_to_values};
use coding_agent_search::franken_sync::{Connection, params};
use coding_agent_search::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use coding_agent_search::search::quill_bridge::QuillCassIndex;
use coding_agent_search::search::tantivy::expected_index_dir;
use coding_agent_search::storage::sqlite::FrankenStorage;
use coding_agent_search::viewer_extensions::schema;
use frankensearch::quill::cass::CassDocument;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use support::BridgeHarness;

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

struct GoldSession {
    id: i64,
    agent: &'static str,
    source_id: &'static str,
    external_id: &'static str,
    source_path: &'static str,
    workspace: Option<&'static str>,
    title: &'static str,
    started_at: Option<i64>,
    ended_at: Option<i64>,
    origin_host: Option<&'static str>,
    metadata: Value,
    /// (idx, role, content, extra_json)
    messages: Vec<(i64, MessageRole, &'static str, Option<Value>)>,
}

fn gold_fixture() -> Vec<GoldSession> {
    vec![
        // S1: chat session with foo+bar in ONE message; notes/tags overlay.
        GoldSession {
            id: 1,
            agent: "claude_code",
            source_id: "local",
            external_id: "s-1-chat",
            source_path: "/sessions/s-1-chat.jsonl",
            workspace: Some("/data/projects/alpha"),
            title: "alpha review",
            started_at: Some(1_000),
            ended_at: Some(9_000),
            origin_host: None,
            metadata: json!({"cass":{"origin":{"kind":"local"}}}),
            messages: vec![
                (0, MessageRole::User, "foo bar combined request", None),
                (1, MessageRole::Agent, "done", None),
            ],
        },
        // S2: foo and bar split across DIFFERENT messages (same-block rule).
        GoldSession {
            id: 2,
            agent: "claude_code",
            source_id: "local",
            external_id: "s-2-split",
            source_path: "/sessions/s-2-split.jsonl",
            workspace: Some("/data/projects/alpha"),
            title: "split session",
            started_at: Some(2_000),
            ended_at: Some(8_000),
            origin_host: None,
            metadata: json!({"cass":{"origin":{"kind":"local"}}}),
            messages: vec![
                (0, MessageRole::User, "foo here only", None),
                (1, MessageRole::Agent, "bar there only", None),
            ],
        },
        // S3: tool session — a Bash call recording quux + an erroring result.
        GoldSession {
            id: 3,
            agent: "claude_code",
            source_id: "local",
            external_id: "s-3-tools",
            source_path: "/sessions/s-3-tools.jsonl",
            workspace: Some("/data/projects/beta"),
            title: "tool runner",
            started_at: Some(3_000),
            ended_at: Some(7_000),
            origin_host: None,
            metadata: json!({"cass":{"origin":{"kind":"local"}}}),
            messages: vec![
                (0, MessageRole::User, "run the deploy", None),
                (
                    1,
                    MessageRole::Agent,
                    "calling deploy tool",
                    Some(
                        json!({"type":"assistant","message":{"role":"assistant","content":[
                        {"type":"tool_use","id":"call-1","name":"Bash",
                         "input":{"command":"quux deploy -v"}}]}}),
                    ),
                ),
                (
                    2,
                    MessageRole::Tool,
                    "deploy quux failed",
                    Some(json!({"type":"tool_result","tool_use_id":"call-1",
                        "is_error":true,"exit_code":2,"content":"deploy quux failed"})),
                ),
            ],
        },
        // S4: thinking session — zeta lives only inside a thinking block.
        GoldSession {
            id: 4,
            agent: "claude_code",
            source_id: "local",
            external_id: "s-4-thinking",
            source_path: "/sessions/s-4-thinking.jsonl",
            workspace: None,
            title: "pondering session",
            started_at: Some(4_000),
            ended_at: Some(6_000),
            origin_host: None,
            metadata: json!({"cass":{"origin":{"kind":"local"}}}),
            messages: vec![
                (0, MessageRole::User, "think about it", None),
                (
                    1,
                    MessageRole::Agent,
                    "answer follows",
                    Some(
                        json!({"type":"assistant","message":{"role":"assistant","content":[
                        {"type":"thinking","thinking":"pondering zeta"},
                        {"type":"text","text":"answer follows"}]}}),
                    ),
                ),
            ],
        },
        // S5: remote codex session with quux in chat text.
        GoldSession {
            id: 5,
            agent: "codex",
            source_id: "ssh:builder",
            external_id: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            source_path: "/codex/sessions/2026/01/02/rollout-2026-01-02T03-04-05-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.jsonl",
            workspace: None,
            title: "remote codex",
            started_at: Some(5_000),
            ended_at: Some(6_500),
            origin_host: Some("builder"),
            metadata: json!({"cass":{"origin":{"kind":"remote"}}}),
            messages: vec![(0, MessageRole::Agent, "codex remote quux", None)],
        },
        // S6: subagent child of S1 — claude sidechain provenance evidence
        // (`parentSessionId` + `agent-*` filename under `<parent>/subagents/`).
        GoldSession {
            id: 6,
            agent: "claude_code",
            source_id: "local",
            external_id: "s-1-chat/subagents/agent-worker.jsonl",
            source_path: "/sessions/s-1-chat/subagents/agent-worker.jsonl",
            workspace: Some("/data/projects/alpha"),
            title: "subagent worker zephyr",
            started_at: Some(2_500),
            ended_at: Some(3_500),
            origin_host: None,
            metadata: json!({"source":"claude_code","sidechain":true,
                "parentSessionId":"s-1-chat",
                "cass":{"origin":{"kind":"local"}}}),
            messages: vec![(0, MessageRole::User, "child task input", None)],
        },
        // S7: attachment-bearing session for the artifacts scope.
        GoldSession {
            id: 7,
            agent: "claude_code",
            source_id: "local",
            external_id: "s-7-artifacts",
            source_path: "/sessions/s-7-artifacts.jsonl",
            workspace: None,
            title: "attachment session",
            started_at: Some(1_500),
            ended_at: Some(4_500),
            origin_host: None,
            metadata: json!({"cass":{"origin":{"kind":"local"}}}),
            messages: vec![(
                0,
                MessageRole::Agent,
                "see attached",
                Some(json!({"type":"attachment","file_path":"/tmp/evidence.png",
                    "name":"evidence.png"})),
            )],
        },
    ]
}

/// The canonical `Conversation` one fixture session inserts — kept as a
/// builder because Task 5's region publication replays the same value
/// through `generation_for_canonical` (the ingest path's shared packet +
/// structural projection inputs).
fn gold_conversation(session: &GoldSession) -> Conversation {
    Conversation {
        id: None,
        agent_slug: session.agent.to_owned(),
        workspace: session.workspace.map(PathBuf::from),
        external_id: Some(session.external_id.to_owned()),
        title: Some(session.title.to_owned()),
        source_path: session.source_path.to_owned().into(),
        started_at: session.started_at,
        ended_at: session.ended_at,
        approx_tokens: Some(1_234),
        metadata_json: session.metadata.clone(),
        source_id: session.source_id.to_owned(),
        origin_host: session.origin_host.map(str::to_owned),
        messages: session
            .messages
            .iter()
            .map(|(idx, role, content, extra)| Message {
                id: None,
                idx: *idx,
                role: role.clone(),
                author: None,
                created_at: session.started_at.map(|base| base + idx),
                content: (*content).to_owned(),
                extra_json: extra.clone().unwrap_or_else(|| json!({"seed":idx})),
                snippets: Vec::new(),
            })
            .collect(),
    }
}

/// Seed canonical rows + Quill docs with per-message content, then the
/// `viewer_*` extension profile, overlays AND the Task-5 persisted
/// region inventory (published through the same generation path ingest
/// runs) with its Quill region documents. Returns the spawned harness.
fn gold_harness() -> Result<BridgeHarness> {
    gold_harness_full(&gold_fixture(), &[])
}

/// Seed the canonical rows, `viewer_*` extension profile, overlays
/// (facts/annotations/tags) and the persisted Task-5 region inventory —
/// everything that is NOT the lexical index. Returns the open writer so
/// callers can stage/stamp region documents and run assertions; tests
/// that need the no-index state stop here.
fn gold_seed(fixture: &[GoldSession], db: &Path) -> Result<Connection> {
    {
        let storage = FrankenStorage::open(db)?;
        let mut agents: BTreeMap<String, i64> = BTreeMap::new();
        let mut workspaces: BTreeMap<String, i64> = BTreeMap::new();
        for session in fixture {
            let agent_id = match agents.get(session.agent) {
                Some(id) => *id,
                None => {
                    let id = storage.ensure_agent(&Agent {
                        id: None,
                        slug: session.agent.to_owned(),
                        name: session.agent.to_owned(),
                        version: None,
                        kind: AgentKind::Cli,
                    })?;
                    agents.insert(session.agent.to_owned(), id);
                    id
                }
            };
            let workspace_id = match session.workspace {
                Some(path) => match workspaces.get(path) {
                    Some(id) => Some(*id),
                    None => {
                        let id = storage.ensure_workspace(Path::new(path), None)?;
                        workspaces.insert(path.to_owned(), id);
                        Some(id)
                    }
                },
                None => None,
            };
            let outcome = storage.insert_conversation_tree(
                agent_id,
                workspace_id,
                &gold_conversation(session),
            )?;
            ensure!(
                outcome.conversation_id == session.id,
                "gold fixture insertion order must pin conversation ids"
            );
        }
    }
    // Extension profile + overlays (facts, annotations, tags) must commit
    // BEFORE region publication: `meta:*` rows and the subagents scope
    // derive from exactly these tables.
    let writer = Connection::open(db.to_string_lossy().into_owned())?;
    schema::ensure_schema(&writer)?;
    // S1 annotations: notes/tags/pin/custom title (chat-scope blocks).
    writer.execute(
        "INSERT INTO viewer_annotations \
         (conversation_id,is_pinned,custom_title,notes,revision,updated_at_ms) \
         VALUES (1,1,'custom alpha title','the fluxcap release note',1,1700000000001)",
    )?;
    for tag in ["mcp-server", "release-blocker"] {
        writer.execute_with_params(
            "INSERT INTO viewer_tags (conversation_id,tag,created_at_ms) VALUES (1,?1,1)",
            &param_slice_to_values(params![tag]),
        )?;
    }
    // Facts: S1 category bugfix + full coverage; S6 is_subagent + parent;
    // every extra fixture session gets the same complete-coverage row so
    // case fixtures stay in the known-evidence universe.
    for session in fixture {
        let cid = session.id;
        let (is_sub, parent, category): (i64, Option<i64>, Option<&str>) = match cid {
            1 => (0, None, Some("bugfix")),
            6 => (1, Some(1), Some("feature")),
            _ => (0, None, Some("feature")),
        };
        let coverage = "complete";
        writer.execute_with_params(
            "INSERT INTO viewer_session_facts \
             (conversation_id,extractor_version,source_revision,coverage_state,\
              coverage_detail_json,is_subagent,parent_conversation_id,task_category,\
              thinking_block_count,tool_error_count,subagent_count) \
             VALUES (?1,1,1,?2,?3,?4,?5,?6,?7,?8,?9)",
            &param_slice_to_values(params![
                cid,
                coverage,
                "{\"extractor\":\"fixture-facts-v1\",\"fields\":{\
                 \"chat\":{\"state\":\"complete\",\"reasons\":[]},\
                 \"tools\":{\"state\":\"complete\",\"reasons\":[]},\
                 \"thinking\":{\"state\":\"complete\",\"reasons\":[]},\
                 \"artifacts\":{\"state\":\"complete\",\"reasons\":[]},\
                 \"subagents\":{\"state\":\"complete\",\"reasons\":[]},\
                 \"tool_exit\":{\"state\":\"complete\",\"reasons\":[]},\
                 \"tool_error\":{\"state\":\"complete\",\"reasons\":[]},\
                 \"files\":{\"state\":\"complete\",\"reasons\":[]},\
                 \"title\":{\"state\":\"complete\",\"reasons\":[]},\
                 \"block_inventory\":{\"state\":\"complete\",\"reasons\":[]},\
                 \"text_inventory\":{\"state\":\"complete\",\"reasons\":[]},\
                 \"is_subagent\":{\"state\":\"complete\",\"reasons\":[]},\
                 \"parent_relation\":{\"state\":\"complete\",\"reasons\":[]},\
                 \"metrics\":{\"state\":\"complete\",\"reasons\":[]},\
                 \"mcp\":{\"state\":\"complete\",\"reasons\":[]}}}",
                is_sub,
                parent,
                category,
                if cid == 4 { 1_i64 } else { 0_i64 },
                if cid == 3 { 1_i64 } else { 0_i64 },
                if cid == 1 { 1_i64 } else { 0_i64 },
            ]),
        )?;
    }
    // Task 5: persisted region inventory — each fixture session replays
    // through `generation_for_canonical` (the shared packet + structural
    // projection ingest consumes), then `publish_search_regions` commits
    // the same row set ingest writes, with marks `pending` until the
    // region documents land in Quill below.
    for session in fixture {
        let conv = gold_conversation(session);
        let generation =
            coding_agent_search::viewer_extensions::projection::generation_for_canonical(
                &writer, &conv, session.id,
            )?;
        coding_agent_search::viewer_extensions::search_regions::publish_search_regions(
            &writer,
            session.id,
            &generation.packet,
            &generation.structure,
        )?;
    }
    Ok(writer)
}

/// Same publication as [`gold_harness`] over an arbitrary fixture set —
/// the seven gold sessions plus any case-specific extras — and optional
/// bridge args (e.g. `--enable-viewer-writes` for the annotation lane).
fn gold_harness_full(fixture: &[GoldSession], extra_args: &[&str]) -> Result<BridgeHarness> {
    let temp = tempfile::tempdir()?;
    let data_dir = temp.path().join("data");
    std::fs::create_dir(&data_dir)?;
    let db = data_dir.join("archive.db");
    let writer = gold_seed(fixture, &db)?;
    // Quill projection: one doc per canonical message, then every
    // session's persisted region documents (derived/meta/noise-chat
    // slots) reconciled by the same lane ingest calls.
    {
        let mut docs = Vec::new();
        for session in fixture {
            for (idx, _role, content, _extra) in &session.messages {
                docs.push(CassDocument {
                    agent: session.agent.to_owned(),
                    workspace: session.workspace.map(str::to_owned),
                    workspace_original: session.workspace.map(str::to_owned),
                    source_path: session.source_path.to_owned(),
                    msg_idx: *idx as u64,
                    created_at: session.started_at.map(|base| base + idx),
                    title: Some(session.title.to_owned()),
                    content: (*content).to_owned(),
                    source_id: session.source_id.to_owned(),
                    origin_kind: if session.source_id == "local" {
                        "local".to_owned()
                    } else {
                        "ssh".to_owned()
                    },
                    origin_host: session.origin_host.map(str::to_owned),
                    conversation_id: Some(session.id),
                });
            }
        }
        let path = expected_index_dir(&data_dir);
        let mut index = QuillCassIndex::open_or_create(&path)?;
        index.add_cass_documents(&docs)?;
        // Two-phase publication — the real lane's stage → commit → stamp
        // order, so the fixture asserts exactly what ingest ships.
        let mut stamps = Vec::new();
        for session in fixture {
            let (_docs, stamp) =
                coding_agent_search::viewer_extensions::search_regions::stage_region_docs(
                    &mut index, &writer, session.id,
                )?;
            if let Some(stamp) = stamp {
                stamps.push(stamp);
            }
        }
        index.commit()?;
        coding_agent_search::viewer_extensions::search_regions::stamp_region_docs_published(
            &writer, &stamps,
        )?;
        std::fs::write(
            path.join("schema_hash.json"),
            serde_json::to_vec(&json!({
                "schema_hash": coding_agent_search::search::tantivy::SCHEMA_HASH,
            }))?,
        )?;
    }
    drop(writer);
    BridgeHarness::spawn_fixture(temp, data_dir, db, extra_args)
}

// ---------------------------------------------------------------------------
// Request helpers
// ---------------------------------------------------------------------------

fn search(bridge: &mut BridgeHarness, body: Value) -> Result<Value> {
    let mut request = json!({"op":"search_sessions"});
    request
        .as_object_mut()
        .expect("object")
        .extend(body.as_object().expect("search body").clone());
    bridge.exchange(request)
}

fn ids(page: &Value) -> Vec<i64> {
    let mut out = page["result"]["items"]
        .as_array()
        .expect("items array")
        .iter()
        .map(|item| item["conversation_id"].as_i64().expect("conversation_id"))
        .collect::<Vec<_>>();
    out.sort_unstable();
    out
}

fn assert_members(page: &Value, expected: &[i64]) -> Result<()> {
    ensure!(page["ok"] == true, "search_sessions refused: {page}");
    ensure!(
        ids(page) == expected,
        "membership mismatch: got {:?}, want {expected:?}: {page}",
        ids(page)
    );
    ensure!(
        page["result"]["complete"] == true,
        "page must be complete: {page}"
    );
    ensure!(
        page["result"]["generation"]["canonical_revision"].is_i64(),
        "generation must bind logical revisions: {page}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn search_sessions_requires_extension_profile() -> Result<()> {
    let mut bridge = BridgeHarness::with_conversations(&[])?;
    let page = search(&mut bridge, json!({"ast":{"type":"term","value":"foo"}}))?;
    ensure!(
        page["ok"] == false,
        "unextended archive must refuse: {page}"
    );
    ensure!(
        page["error"]["kind"] == "viewer_extensions_missing",
        "expected viewer_extensions_missing, got {page}"
    );
    bridge.shutdown()
}

#[test]
fn search_sessions_lexical_membership_and_scopes() -> Result<()> {
    let mut bridge = gold_harness()?;

    // Bare term: any region. S1 (chat) and S2 (chat) hold "foo".
    let page = search(&mut bridge, json!({"ast":{"type":"term","value":"foo"}}))?;
    assert_members(&page, &[1, 2])?;

    // Same-block AND: only S1 carries foo and bar in one block.
    let page = search(
        &mut bridge,
        json!({"ast":{"type":"and","children":[
            {"type":"term","value":"foo"},{"type":"term","value":"bar"}]}}),
    )?;
    assert_members(&page, &[1])?;

    // Equivalent DSL string must parse to the same membership.
    let page = search(&mut bridge, json!({"query":"foo AND bar"}))?;
    assert_members(&page, &[1])?;

    // Title scope.
    let page = search(&mut bridge, json!({"query":"in:title alpha"}))?;
    assert_members(&page, &[1])?;

    // Notes metadata block is chat scope.
    let page = search(&mut bridge, json!({"query":"fluxcap"}))?;
    assert_members(&page, &[1])?;
    let page = search(&mut bridge, json!({"query":"in:chat fluxcap"}))?;
    assert_members(&page, &[1])?;

    // Tag text is a chat-scope metadata block too.
    let page = search(&mut bridge, json!({"query":"mcp-server"}))?;
    assert_members(&page, &[1])?;

    // Thinking scope: zeta exists only inside a thinking block.
    let page = search(&mut bridge, json!({"query":"in:thinking zeta"}))?;
    assert_members(&page, &[4])?;
    let page = search(&mut bridge, json!({"query":"in:chat zeta"}))?;
    assert_members(&page, &[])?;

    // Tools scope: quux inside the recorded Bash input. S5 holds quux
    // only in chat — the tools scope must exclude it.
    let page = search(&mut bridge, json!({"query":"in:tools quux"}))?;
    assert_members(&page, &[3])?;
    let page = search(&mut bridge, json!({"query":"tool:Bash quux"}))?;
    assert_members(&page, &[3])?;

    // Artifacts scope via the recorded attachment path.
    let page = search(&mut bridge, json!({"query":"in:artifacts evidence"}))?;
    assert_members(&page, &[7])?;

    // Subagents scope: S1's child descriptor carries the child title.
    let page = search(&mut bridge, json!({"query":"in:subagents zephyr"}))?;
    assert_members(&page, &[1])?;

    // Exact-error and exit predicates on the correlated call/result region.
    let page = search(&mut bridge, json!({"query":"has:error"}))?;
    assert_members(&page, &[3])?;
    let page = search(&mut bridge, json!({"query":"error:tool"}))?;
    assert_members(&page, &[3])?;
    let page = search(&mut bridge, json!({"query":"exit:2"}))?;
    assert_members(&page, &[3])?;
    let page = search(&mut bridge, json!({"query":"exit:0"}))?;
    assert_members(&page, &[])?;

    // Negation: exclude every session with a Bash tools block. S6 is a
    // subagent — outside the default browse universe like the oracle.
    let page = search(&mut bridge, json!({"query":"NOT tool:Bash"}))?;
    assert_members(&page, &[1, 2, 4, 5, 7])?;
    let page = search(&mut bridge, json!({"query":"NOT (tool:Bash quux)"}))?;
    assert_members(&page, &[1, 2, 4, 5, 7])?;
    // Absence semantics: tool:!=Bash is `NOT exists-Bash-block`.
    let page = search(&mut bridge, json!({"query":"tool:!=Bash"}))?;
    assert_members(&page, &[1, 2, 4, 5, 7])?;

    // is:subagent surfaces the child session once subagents are visible.
    let page = search(
        &mut bridge,
        json!({"query":"is:subagent","filters":{"include_subagents":true}}),
    )?;
    assert_members(&page, &[6])?;

    bridge.shutdown()
}

#[test]
fn search_sessions_controls_and_filters() -> Result<()> {
    let mut bridge = gold_harness()?;

    // Provider control narrows the candidate universe.
    let page = search(
        &mut bridge,
        json!({"ast":{"type":"term","value":"quux"},
               "filters":{"provider":"codex"}}),
    )?;
    assert_members(&page, &[5])?;

    // Tag control.
    let page = search(
        &mut bridge,
        json!({"ast":{"type":"term","value":"foo"},"filters":{"tag":"release-blocker"}}),
    )?;
    assert_members(&page, &[1])?;

    // Category control backed by persisted facts.
    let page = search(
        &mut bridge,
        json!({"ast":{"type":"term","value":"foo"},"filters":{"category":"bugfix"}}),
    )?;
    assert_members(&page, &[1])?;

    // Pinned control (viewer is_bookmarked ≈ viewer_annotations.is_pinned).
    let page = search(
        &mut bridge,
        json!({"ast":{"type":"term","value":"foo"},"filters":{"pinned":true}}),
    )?;
    assert_members(&page, &[1])?;

    // Activity window: S3's last-message activity (~3_002) and S5's
    // (~5_000) both survive [2_000, 8_000); a later lower bound admits
    // nothing.
    let page = search(
        &mut bridge,
        json!({"ast":{"type":"term","value":"quux"},
               "filters":{"activity_from_ms":2_000,"activity_to_ms":8_000}}),
    )?;
    assert_members(&page, &[3, 5])?;
    let page = search(
        &mut bridge,
        json!({"ast":{"type":"term","value":"quux"},
               "filters":{"activity_from_ms":8_000}}),
    )?;
    assert_members(&page, &[])?;

    // Subagent exclusion is the browse default: S6 must not appear.
    let page = search(&mut bridge, json!({"query":"zephyr"}))?;
    assert_members(&page, &[1])?;
    let page = search(
        &mut bridge,
        json!({"query":"zephyr","filters":{"include_subagents":true}}),
    )?;
    assert_members(&page, &[1, 6])?;
    let page = search(
        &mut bridge,
        json!({"query":"zephyr","filters":{"is_subagent":true}}),
    )?;
    assert_members(&page, &[6])?;

    // Project control by workspace display name.
    let page = search(
        &mut bridge,
        json!({"ast":{"type":"term","value":"foo"},"filters":{"project":"alpha"}}),
    )?;
    assert_members(&page, &[1, 2])?;

    bridge.shutdown()
}

#[test]
fn search_sessions_witnesses_pagination_and_receipts() -> Result<()> {
    let mut bridge = gold_harness()?;

    // Deep-link witnesses for the tools match: bounded, message-bound,
    // and carrying a branch id + scope.
    let page = search(&mut bridge, json!({"query":"in:tools quux"}))?;
    ensure!(page["ok"] == true, "search refused: {page}");
    let matches = page["result"]["matches"]
        .as_object()
        .expect("matches object");
    let s3_matches = matches
        .get("3")
        .and_then(Value::as_array)
        .expect("S3 witnesses");
    ensure!(!s3_matches.is_empty(), "S3 must carry witnesses");
    ensure!(
        s3_matches
            .iter()
            .any(|m| m["location"] == "tools" && m["message_idx"].as_i64() == Some(2)),
        "expected a tools witness bound to message idx 2: {s3_matches:?}"
    );
    ensure!(
        s3_matches.iter().all(|m| m["anchor_status"] == "verified"),
        "witnesses must bind canonical message coordinates: {s3_matches:?}"
    );
    ensure!(
        page["result"]["matches"]["3"]
            .as_array()
            .is_some_and(|list| list.len() <= 32),
        "witness list must stay bounded"
    );

    // Generation-bound keyset pagination: no duplicate sessions across
    // pages, identical generation on both pages.
    let page1 = search(
        &mut bridge,
        json!({"query":"quux","limit":1,"sort":"updated_desc"}),
    )?;
    ensure!(page1["ok"] == true, "page1 refused: {page1}");
    let cursor = page1["result"]["next_cursor"]
        .as_str()
        .expect("next cursor")
        .to_owned();
    let gen1 = page1["result"]["generation"].clone();
    let page2 = search(
        &mut bridge,
        json!({"query":"quux","limit":1,"sort":"updated_desc","cursor":cursor}),
    )?;
    ensure!(page2["ok"] == true, "page2 refused: {page2}");
    let first: Vec<i64> = page1["result"]["items"]
        .as_array()
        .expect("items")
        .iter()
        .map(|i| i["conversation_id"].as_i64().unwrap())
        .collect();
    let second: Vec<i64> = page2["result"]["items"]
        .as_array()
        .expect("items")
        .iter()
        .map(|i| i["conversation_id"].as_i64().unwrap())
        .collect();
    ensure!(
        first.len() == 1 && second.len() == 1,
        "expected one row/page"
    );
    ensure!(first != second, "duplicate session across pages");
    ensure!(
        page2["result"]["generation"] == gen1,
        "generation must stay bound across pages"
    );
    ensure!(
        page2["result"]["next_cursor"].is_null(),
        "third page must not exist"
    );

    // Item shape carries summary + public identity + score.
    let item = &page1["result"]["items"][0];
    ensure!(item["public_id"].is_string(), "public_id required: {item}");
    ensure!(item["provider"].is_string(), "provider required: {item}");
    ensure!(
        page1["result"]["ranking_complete"] == true,
        "ranking receipt required"
    );

    bridge.shutdown()
}

#[test]
fn search_sessions_typed_refusals() -> Result<()> {
    let mut bridge = gold_harness()?;

    // Non-lexical modes are separate ungranted capabilities.
    let page = search(
        &mut bridge,
        json!({"ast":{"type":"term","value":"foo"},"mode":"semantic"}),
    )?;
    ensure!(page["ok"] == false, "semantic must refuse: {page}");
    ensure!(
        page["error"]["kind"] == "capability_not_granted",
        "expected capability_not_granted, got {page}"
    );
    let page = search(
        &mut bridge,
        json!({"ast":{"type":"term","value":"foo"},"mode":"hybrid"}),
    )?;
    ensure!(
        page["ok"] == false && page["error"]["kind"] == "capability_not_granted",
        "hybrid must refuse: {page}"
    );

    // In the DSL string grammar an unknown `field:value` degrades to a
    // bare term (oracle grammar fallback — it matches nothing).
    let page = search(&mut bridge, json!({"query":"color:red"}))?;
    ensure!(page["ok"] == true, "term fallback must stay live: {page}");
    ensure!(
        page["result"]["items"]
            .as_array()
            .is_some_and(Vec::is_empty),
        "term fallback matches nothing: {page}"
    );

    // The AST lane is the programmatic contract: unknown field is a typed
    // unsupported_filter.
    let page = search(
        &mut bridge,
        json!({"ast":{"type":"filter","field":"color","op":"EQ","value":"red"}}),
    )?;
    ensure!(
        page["ok"] == false
            && (page["error"]["kind"] == "unsupported_filter"
                || page["error"]["kind"] == "invalid_request"),
        "unknown AST field must refuse: {page}"
    );

    // A bogus cursor is a typed cursor_expired.
    let page = search(&mut bridge, json!({"query":"foo","cursor":"not-a-cursor"}))?;
    ensure!(
        page["ok"] == false && page["error"]["kind"] == "cursor_expired",
        "bogus cursor must refuse cursor_expired: {page}"
    );

    // Malformed request: both a positive limit bound and a sane page size.
    let page = search(&mut bridge, json!({"query":"foo","limit":0}))?;
    ensure!(
        page["ok"] == false && page["error"]["kind"] == "invalid_request",
        "limit 0 must refuse invalid_request: {page}"
    );

    bridge.shutdown()
}

/// Task 14 stress — search during mid-index ingest: a canonical commit
/// landing between two keyset pages moves the bound generation, so the
/// continuation MUST be a typed `cursor_expired` (the viewer refetches)
/// rather than a silently mixed-generation page. The lane then keeps
/// serving fresh first pages.
#[test]
fn search_sessions_mid_index_write_expires_cursor() -> Result<()> {
    let mut bridge = gold_harness()?;
    // Canonical observers make a second connection's commit bump
    // `canonical_revision` — the generation every search cursor binds.
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    revisions::ensure_observers(&writer)?;
    drop(writer);

    let page1 = search(
        &mut bridge,
        json!({"query":"quux","limit":1,"sort":"updated_desc"}),
    )?;
    ensure!(page1["ok"] == true, "page1 refused: {page1}");
    let cursor = page1["result"]["next_cursor"]
        .as_str()
        .expect("quux fixture must paginate at limit=1")
        .to_owned();

    // Mid-index: a stock indexer on a SECOND connection re-ingests session
    // 1 between page 1 and page 2 — a metadata write whose guarded
    // conversations-UPDATE trigger bumps canonical_revision inside that
    // commit. (A bare new-conversation insert without a completed fact
    // pass would honestly refuse `data_unavailable` BEFORE the cursor is
    // even decoded — unknown evidence is never silently admitted.)
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer.execute("UPDATE conversations SET title='mid index retitle' WHERE id=1")?;
    drop(writer);

    // The bound generation moved: continuing the old cursor is a typed
    // expiry, never a page mixing two logical generations.
    let page2 = search(
        &mut bridge,
        json!({"query":"quux","limit":1,"sort":"updated_desc","cursor":cursor}),
    )?;
    ensure!(
        page2["ok"] == false && page2["error"]["kind"] == "cursor_expired",
        "a canonical commit between pages must expire the cursor: {page2}"
    );

    // The lane stays live: a fresh first page still serves exact
    // membership (quux lives in sessions 3 and 5).
    let fresh = search(&mut bridge, json!({"query":"quux"}))?;
    assert_members(&fresh, &[3, 5])?;
    bridge.shutdown()
}

#[test]
fn search_sessions_ast_wire_shape_accepted() -> Result<()> {
    let mut bridge = gold_harness()?;
    // The canonical SearchAst wire container ({"root": node}) plus a
    // negated nested filter under OR — exercises the discriminated decode.
    let page = search(
        &mut bridge,
        json!({"ast":{"root":{"type":"or","children":[
            {"type":"and","children":[
                {"type":"filter","field":"tool","op":"EQ","value":"Bash"},
                {"type":"term","value":"quux"}]},
            {"type":"not","expr":{"type":"filter","field":"scope","op":"EQ","value":"chat"}}
        ]}}}),
    )?;
    // (Bash tool region containing quux) OR (NOT any chat region):
    // S3 satisfies the first arm. No session satisfies the second arm
    // (every session has a chat region except... S3's own msgs? S3 msg0 is
    // chat so S3 is out of arm2; arm2 needs a session with NO chat region —
    // none in fixture; membership = {3}).
    assert_members(&page, &[3])?;
    bridge.shutdown()
}

/// Task 5 hydration instrumentation: the indexed path must never re-read
/// the corpus. `foo` lives in exactly two chat regions — the diagnostics
/// pin a bounded hydration count (candidate verification + page snippets),
/// never the session/message universe.
#[test]
fn search_sessions_indexed_hydration_is_bounded() -> Result<()> {
    let mut bridge = gold_harness()?;

    let page = search(&mut bridge, json!({"query":"foo"}))?;
    assert_members(&page, &[1, 2])?;
    let diag = &page["result"]["diagnostics"];
    ensure!(
        diag["candidate_sessions"].as_u64() == Some(6),
        "the SQL-narrowed universe is the browse set (S6 is a subagent): {diag}"
    );
    // Two candidate verifications + at most one snippet re-hydration per
    // emitted witness (S1 + S2). A corpus scan would read all 12 fixture
    // messages; anything past the small bound proves a regression.
    let hydrated = diag["messages_hydrated"]
        .as_u64()
        .expect("messages_hydrated diagnostic");
    ensure!(
        hydrated <= 4,
        "selective query must not hydrate the corpus: {diag}"
    );
    let regions = diag["regions_hydrated"]
        .as_u64()
        .expect("regions_hydrated diagnostic");
    ensure!(regions <= 8, "region hydration stays bounded: {diag}");
    ensure!(
        diag["region_rows_loaded"].as_u64().unwrap() >= regions,
        "row loads count the persisted inventory, not hydration: {diag}"
    );

    // A meta-only literal never touches `messages` at all: notes ride the
    // persisted `indexed_text` fragment end to end.
    let page = search(&mut bridge, json!({"query":"fluxcap"}))?;
    assert_members(&page, &[1])?;
    ensure!(
        page["result"]["diagnostics"]["messages_hydrated"].as_u64() == Some(0),
        "meta fragments must hydrate zero canonical messages: {page}"
    );
    bridge.shutdown()
}

/// Unicode + deep coordinates + the 64 KiB region-text byte budget. S8
/// parks a Cyrillic marker at message idx 1100 (deep-link coordinate);
/// S9 holds a >64 KiB chat body whose terms straddle the hydration bound —
/// in-budget terms verify, out-of-budget hits degrade to honest
/// `unknown` (never a silent false negative and never a slice panic on a
/// multibyte boundary).
#[test]
fn search_sessions_unicode_deep_and_byte_budget() -> Result<()> {
    let mut fixture = gold_fixture();
    let mut deep: Vec<(i64, MessageRole, &'static str, Option<Value>)> = (0..1_100_i64)
        .map(|idx| (idx, MessageRole::Agent, "deep filler", None))
        .collect();
    deep.push((
        1_100,
        MessageRole::Agent,
        "глубокий поисковый маркер 🚀 emoji",
        None,
    ));
    fixture.push(GoldSession {
        id: 8,
        agent: "claude_code",
        source_id: "local",
        external_id: "s-8-deep",
        source_path: "/sessions/s-8-deep.jsonl",
        workspace: None,
        title: "deep session",
        started_at: Some(6_000),
        ended_at: Some(9_500),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        messages: deep,
    });
    // 64 KiB boundary: 13 B prefix + 'a' to byte 65535, then 'ж' (2 B)
    // straddling the bound, padding, and a term beyond it.
    let long_body = format!(
        "boundaryterm {}{}{} beyondterm",
        "a".repeat(65_535 - 13),
        "ж",
        "z".repeat(300)
    );
    let long_body: &'static str = Box::leak(long_body.into_boxed_str());
    fixture.push(GoldSession {
        id: 9,
        agent: "claude_code",
        source_id: "local",
        external_id: "s-9-huge",
        source_path: "/sessions/s-9-huge.jsonl",
        workspace: None,
        title: "huge message session",
        started_at: Some(6_100),
        ended_at: Some(9_400),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        messages: vec![(0, MessageRole::User, long_body, None)],
    });
    let mut bridge = gold_harness_full(&fixture, &[])?;

    // Cyrillic literal at message idx 1100 — the deep coordinate survives
    // on the persisted region row, hydrated on demand.
    let page = search(&mut bridge, json!({"query":"маркер"}))?;
    assert_members(&page, &[8])?;
    let matches = page["result"]["matches"]["8"]
        .as_array()
        .expect("S8 witnesses");
    ensure!(
        matches
            .iter()
            .any(|m| m["message_idx"].as_i64() == Some(1_100)),
        "deep witness must carry message idx 1100: {matches:?}"
    );

    // In-budget term verifies normally.
    let page = search(&mut bridge, json!({"query":"boundaryterm"}))?;
    assert_members(&page, &[9])?;

    // `beyondterm` is indexed in the canonical document but lies past the
    // 64 KiB hydration bound: the hit cannot be verified, so the session
    // is honestly `unknown` — the page refuses instead of answering a
    // silently incomplete membership.
    let page = search(&mut bridge, json!({"query":"beyondterm"}))?;
    ensure!(
        page["ok"] == false,
        "an unverifiable out-of-budget hit must not silently miss: {page}"
    );

    // The boundary-safe truncation did not panic the lane — the bridge is
    // still serving (the searches above already proved it).
    bridge.shutdown()
}

/// An index hit the archived region text cannot reproduce must refuse
/// strict search WITH a truthful reason — never a reasonless
/// `facts_pending`. S8's marker term is published in the canonical
/// document (the lexical lane indexes the full message body) but lies
/// past the 64 KiB hydration bound, so bounded verification can neither
/// confirm nor deny the hit: honest `possible` membership whose Unknown
/// must carry evidence. Before the truthful reason existed, the refusal
/// picked `facts_pending` through a vacuous "every reason is pending"
/// check over an EMPTY reason set — an evidence-free retryable verdict.
#[test]
fn search_sessions_unverifiable_hit_refuses_with_truthful_reason() -> Result<()> {
    let mut fixture = gold_fixture();
    // A >64 KiB chat body whose only occurrence of the marker term sits
    // far past the bound (the real archive shape: an indexed long message
    // whose hit lands beyond every hydrated fragment).
    let long_body = format!("{} farbeyondterm", "a".repeat(70_000));
    let long_body: &'static str = Box::leak(long_body.into_boxed_str());
    fixture.push(GoldSession {
        id: 8,
        agent: "claude_code",
        source_id: "local",
        external_id: "s-8-far",
        source_path: "/sessions/s-8-far.jsonl",
        workspace: None,
        title: "far out-of-budget session",
        started_at: Some(6_200),
        ended_at: Some(9_300),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        messages: vec![(0, MessageRole::User, long_body, None)],
    });
    let mut bridge = gold_harness_full(&fixture, &[])?;

    // Strict: the unverifiable hit is unavailable evidence — a truthful,
    // non-retryable refusal that NAMES what could not be verified.
    let page = search(&mut bridge, json!({"query":"farbeyondterm"}))?;
    ensure!(
        page["ok"] == false,
        "an unverifiable index hit must not silently miss: {page}"
    );
    ensure!(
        page["error"]["kind"] == "data_unavailable",
        "an unreproducible hit is unavailable evidence, not pending facts: {page}"
    );
    let message = page["error"]["message"].as_str().unwrap_or_default();
    ensure!(
        message.contains("quill_hit_not_reproduced"),
        "the refusal must name the unreproducible hit: {page}"
    );

    // allow_partial still answers, with the honest incomplete-membership
    // warning for the unknown session.
    let page = search(
        &mut bridge,
        json!({"query":"farbeyondterm", "allow_partial": true}),
    )?;
    ensure!(page["ok"] == true, "partial mode must answer: {page}");
    let warnings = page["result"]["warnings"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    ensure!(
        warnings.iter().any(|w| w == "membership_incomplete"),
        "the unknown session must keep its warning: {page}"
    );
    ensure!(
        !ids(&page).contains(&8),
        "the unverifiable session is not a member: {page}"
    );
    bridge.shutdown()
}

/// Phrase (quoted, contiguous-token), prefix (non-exact), and code
/// punctuation semantics through the persisted regions — the same
/// semantics the query-time lane produced.
#[test]
fn search_sessions_phrase_prefix_and_punctuation() -> Result<()> {
    let mut bridge = gold_harness()?;

    // Contiguous phrase: S1's "foo bar combined request" matches; S2
    // splits foo/bar across messages and must not.
    let page = search(&mut bridge, json!({"query":"\"foo bar\""}))?;
    assert_members(&page, &[1])?;
    let page = search(&mut bridge, json!({"query":"\"bar there\""}))?;
    assert_members(&page, &[2])?;

    // Prefix semantics on the notes meta fragment.
    let page = search(&mut bridge, json!({"query":"flu*"}))?;
    assert_members(&page, &[1])?;
    let page = search(&mut bridge, json!({"query":"comb*"}))?;
    assert_members(&page, &[1])?;

    // Contiguous phrase inside the recorded Bash input, and dotted
    // punctuation in the attachment path — both survive the region doc +
    // matcher round trip.
    let page = search(
        &mut bridge,
        json!({"ast":{"type":"term","value":"quux deploy","exact":true}}),
    )?;
    assert_members(&page, &[3])?;
    let page = search(&mut bridge, json!({"query":"\"evidence.png\""}))?;
    assert_members(&page, &[7])?;
    let page = search(&mut bridge, json!({"query":"file:evidence.png"}))?;
    assert_members(&page, &[7])?;

    bridge.shutdown()
}

/// Tool ordering + reuse: a result arriving BEFORE its call still
/// correlates by `tool_use_id` (the region anchors the result's message
/// coordinates), and a call id reused by a second call still yields its
/// own region — the correlation is claim-once, not shadowed.
#[test]
fn search_sessions_tool_ordering_and_reuse() -> Result<()> {
    let mut fixture = gold_fixture();
    fixture.push(GoldSession {
        id: 8,
        agent: "claude_code",
        source_id: "local",
        external_id: "s-8-toolorder",
        source_path: "/sessions/s-8-toolorder.jsonl",
        workspace: None,
        title: "reordered tools",
        started_at: Some(6_200),
        ended_at: Some(9_300),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        messages: vec![
            // Result arrives BEFORE the call it names (replayed order).
            (
                0,
                MessageRole::Tool,
                "earlyresult",
                Some(json!({"type":"tool_result","tool_use_id":"dup",
                    "is_error":false,"content":"earlyresult"})),
            ),
            (
                1,
                MessageRole::Agent,
                "first call",
                Some(json!({"type":"assistant","message":{"role":"assistant",
                    "content":[{"type":"tool_use","id":"dup","name":"Bash",
                    "input":{"command":"firstdupcommand"}}]}})),
            ),
            // The same call id reused (post-compaction replay) — a second
            // region must still exist with its own text.
            (
                2,
                MessageRole::Agent,
                "second call",
                Some(json!({"type":"assistant","message":{"role":"assistant",
                    "content":[{"type":"tool_use","id":"dup","name":"Bash",
                    "input":{"command":"seconddupcommand"}}]}})),
            ),
        ],
    });
    let mut bridge = gold_harness_full(&fixture, &[])?;

    // Out-of-order correlation: `earlyresult` lives in the FIRST call's
    // region, anchored to the result message (idx 0).
    let page = search(&mut bridge, json!({"query":"in:tools earlyresult"}))?;
    assert_members(&page, &[8])?;
    let matches = page["result"]["matches"]["8"]
        .as_array()
        .expect("S8 witnesses");
    ensure!(
        matches.iter().any(|m| m["message_idx"].as_i64() == Some(0)),
        "result-first correlation anchors the result coordinate: {matches:?}"
    );

    // Reused call id: the second call's region survives with its own text.
    let page = search(&mut bridge, json!({"query":"in:tools seconddupcommand"}))?;
    assert_members(&page, &[8])?;
    let page = search(&mut bridge, json!({"query":"in:tools firstdupcommand"}))?;
    assert_members(&page, &[8])?;

    bridge.shutdown()
}

/// Annotation edit → targeted meta replacement → Quill convergence: only
/// the `meta:*` rows/docs change (notes + tags here), message-region
/// ordinals stay put, and the marks row returns to `published` inside the
/// write op — the very next search sees the converged membership.
#[test]
fn search_sessions_annotation_edit_converges_region_docs() -> Result<()> {
    let mut bridge = gold_harness_full(&gold_fixture(), &["--enable-viewer-writes"])?;

    // Baseline: S1's notes/tag meta regions are live.
    let page = search(&mut bridge, json!({"query":"fluxcap"}))?;
    assert_members(&page, &[1])?;
    let page = search(&mut bridge, json!({"query":"mcp-server"}))?;
    assert_members(&page, &[1])?;

    // CAS patch (fixture revision is 1): replace notes + the whole tag
    // set; title/pin stay. The write op commits the meta replacement and
    // converges the region documents before answering.
    let patch = bridge.exchange(json!({
        "op":"patch_annotations","conversation_id":1,"expected_revision":1,
        "patch":{"notes":"warpdrive maintenance log","tags":["overhauled"]}
    }))?;
    ensure!(patch["ok"] == true, "patch refused: {patch}");
    ensure!(patch["result"]["annotation"]["revision"] == 2);

    // Cleared fragments stop matching; new fragments are immediately live.
    let page = search(&mut bridge, json!({"query":"fluxcap"}))?;
    assert_members(&page, &[])?;
    let page = search(&mut bridge, json!({"query":"mcp-server"}))?;
    assert_members(&page, &[])?;
    let page = search(&mut bridge, json!({"query":"warpdrive"}))?;
    assert_members(&page, &[1])?;
    let page = search(&mut bridge, json!({"query":"overhauled"}))?;
    assert_members(&page, &[1])?;

    // Message regions are untouched — same anchors, same membership.
    let page = search(&mut bridge, json!({"query":"foo"}))?;
    assert_members(&page, &[1, 2])?;
    let matches = page["result"]["matches"]["1"]
        .as_array()
        .expect("S1 witnesses");
    ensure!(
        matches.iter().any(|m| m["message_idx"].as_i64() == Some(0)),
        "chat region ordinal/coordinate must survive the meta edit: {matches:?}"
    );
    bridge.shutdown()
}

/// Universes above 50k stay truthful: 65k+ synthetic sort rows with no
/// region inventory make every candidate `unknown` — the lane refuses
/// with the retryable `facts_pending` instead of truncating or silently
/// answering.
#[test]
fn search_sessions_large_universe_stays_honest() -> Result<()> {
    let mut bridge = gold_harness()?;
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    // Explicit-id batched VALUES inserts — deterministic, no same-table
    // INSERT…SELECT scan. 128 batches × 512 rows = 65_536 synthetic
    // sessions (>50k) on top of the seven gold ones. The canonical
    // observer triggers (installed by bridge startup) mirror every
    // conversations row into `viewer_session_sort` — writing sort rows
    // directly would collide with the trigger-maintained ones.
    const STRESS_ROWS: i64 = 65_536;
    const BATCH: i64 = 512;
    for base in (8..8 + STRESS_ROWS).step_by(BATCH as usize) {
        let mut conv_sql = String::from(
            "INSERT INTO conversations \
             (id,agent_id,workspace_id,source_id,external_id,title,source_path,started_at,ended_at) \
             VALUES ",
        );
        let end = (base + BATCH).min(8 + STRESS_ROWS);
        for id in base..end {
            if id > base {
                conv_sql.push(',');
            }
            conv_sql.push_str(&format!(
                "({id},1,NULL,'local','stress-{id}','stress','/stress/{id}.jsonl',1,2)"
            ));
        }
        writer.execute(&conv_sql)?;
    }
    drop(writer);

    // Every synthetic row has no facts row, no regions and no marks row —
    // a universe of ~65k `unknown`s must refuse honestly, not clip.
    let page = search(&mut bridge, json!({"query":"zzz_absent_literal"}))?;
    ensure!(
        page["ok"] == false,
        "a 65k pending universe must not silently answer: {page}"
    );
    ensure!(
        page["error"]["kind"] == "facts_pending",
        "pending evidence is the retryable refusal: {page}"
    );
    bridge.shutdown()
}

/// `mcp:` binds tool and meta surfaces — the tool name, every region's
/// persisted `indexed_text` fragment and session tags — exactly what the
/// indexed architecture evaluates from persisted rows. The Python
/// oracle's `fts.tool_name OR fts.content` additionally matched block
/// bodies it already had in memory; here a canonical chat body is a
/// `chat` literal's surface, not tool membership (a predicate must
/// never pay one `messages.content` read per region). Pinned
/// divergence: `mcp:quux` sees S3's tool-call fragment but NOT S5's
/// chat body carrying the same token.
#[test]
fn search_sessions_mcp_binds_tool_and_meta_surfaces() -> Result<()> {
    let mut bridge = gold_harness()?;

    // Tag surface: S1 carries the `mcp-server` tag — the session-level
    // heuristic the oracle keeps.
    let page = search(&mut bridge, json!({"query":"mcp:mcp-server"}))?;
    assert_members(&page, &[1])?;
    let page = search(&mut bridge, json!({"query":"mcp:server"}))?;
    assert_members(&page, &[1])?;

    // Tool-name surface: S3 recorded a Bash call.
    let page = search(&mut bridge, json!({"query":"mcp:Bash"}))?;
    assert_members(&page, &[3])?;

    // Persisted fragment surface: S3's call/result regions carry `quux`
    // in their `indexed_text` — a match. S5's identical token lives only
    // in its canonical chat body — invisible to the predicate, and the
    // pinned test proves it is never hydrated for one.
    let page = search(&mut bridge, json!({"query":"mcp:quux"}))?;
    assert_members(&page, &[3])?;
    // ...while the same token IS found by a bare literal through the
    // chat surface — the division of labour this pins.
    let page = search(&mut bridge, json!({"query":"quux"}))?;
    assert_members(&page, &[3, 5])?;

    bridge.shutdown()
}

/// A notes-ONLY annotation edit converges just the meta rows it owns:
/// the old note text stops matching, the replacement matches, and the
/// untouched tag surface keeps answering — targeted meta replacement
/// must not disturb regions it did not own — while the marks row
/// returns to `published` inside the same write op.
#[test]
fn search_sessions_notes_only_edit_converges_meta_regions() -> Result<()> {
    let mut bridge = gold_harness_full(&gold_fixture(), &["--enable-viewer-writes"])?;

    // Baseline surfaces: S1's notes text and both tags are live.
    let page = search(&mut bridge, json!({"query":"fluxcap"}))?;
    assert_members(&page, &[1])?;
    let page = search(&mut bridge, json!({"query":"mcp-server"}))?;
    assert_members(&page, &[1])?;

    // Notes ONLY — no `tags` key, so the tag set is untouched.
    let patch = bridge.exchange(json!({
        "op":"patch_annotations","conversation_id":1,"expected_revision":1,
        "patch":{"notes":"warpdrive maintenance log"}
    }))?;
    ensure!(patch["ok"] == true, "patch refused: {patch}");
    ensure!(patch["result"]["annotation"]["revision"] == 2);

    // Cleared fragment stops matching; the new fragment is live; the
    // untouched tag surfaces keep answering through the same regions.
    let page = search(&mut bridge, json!({"query":"fluxcap"}))?;
    assert_members(&page, &[])?;
    let page = search(&mut bridge, json!({"query":"warpdrive"}))?;
    assert_members(&page, &[1])?;
    let page = search(&mut bridge, json!({"query":"mcp-server"}))?;
    assert_members(&page, &[1])?;
    let page = search(&mut bridge, json!({"query":"release-blocker"}))?;
    assert_members(&page, &[1])?;

    // The marks row converged back to `published` inside the write op.
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    let marks = writer.query(
        "SELECT docs_state, pending_doc_deletes FROM viewer_search_region_marks \
         WHERE conversation_id = 1",
    )?;
    ensure!(marks.len() == 1, "marks row present: {marks:?}");
    ensure!(
        marks[0].get_typed::<String>(0)? == "published",
        "meta replacement must reconverge to published: {marks:?}"
    );
    ensure!(
        marks[0].get_typed::<String>(1)? == "[]",
        "converged marks carry no pending deletes: {marks:?}"
    );
    bridge.shutdown()
}

/// An annotation edit on an archive with NO published lexical index must
/// not fabricate one holding only region docs: convergence skips, the
/// marks row stays `meta_pending`, and `search_sessions` keeps refusing
/// `index_unavailable` — the honest states the reader contract defines.
/// Before the guard, `open_or_create` minted a real index of ONLY region
/// docs whose fingerprint made every later chat literal a silent false
/// negative.
#[test]
fn search_sessions_annotation_edit_without_index_fabricates_nothing() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let data_dir = temp.path().join("data");
    std::fs::create_dir(&data_dir)?;
    let db = data_dir.join("archive.db");
    // Canonical rows + extension profile + persisted regions — but NO
    // index at `expected_index_dir` (the deferred-lexical state).
    let writer = gold_seed(&gold_fixture(), &db)?;
    let index_path = expected_index_dir(&data_dir);
    ensure!(
        coding_agent_search::search::tantivy::searchable_index_fingerprint(&index_path)?.is_none(),
        "fixture must start without a published index"
    );
    let mut bridge = BridgeHarness::spawn_fixture(temp, data_dir, db, &["--enable-viewer-writes"])?;

    let patch = bridge.exchange(json!({
        "op":"patch_annotations","conversation_id":1,"expected_revision":1,
        "patch":{"notes":"replaced without an index"}
    }))?;
    ensure!(patch["ok"] == true, "patch refused: {patch}");

    // No index was fabricated: the fingerprint is still absent and the
    // marks row honestly reports `meta_pending` for the next ingest run.
    ensure!(
        coding_agent_search::search::tantivy::searchable_index_fingerprint(&index_path)?.is_none(),
        "annotation convergence must not fabricate an index"
    );
    let marks = writer
        .query("SELECT docs_state FROM viewer_search_region_marks WHERE conversation_id = 1")?;
    ensure!(marks.len() == 1, "marks row present: {marks:?}");
    ensure!(
        marks[0].get_typed::<String>(0)? == "meta_pending",
        "absent-index convergence must leave marks meta_pending: {marks:?}"
    );

    let page = search(&mut bridge, json!({"query":"fluxcap"}))?;
    ensure!(page["ok"] == false, "no-index search must refuse: {page}");
    ensure!(
        page["error"]["kind"] == "index_unavailable",
        "the reader contract still refuses honestly: {page}"
    );
    bridge.shutdown()
}

/// Ungated (non-ASCII) literal verification is bounded: a session whose
/// chat regions exceed the hydration budget can no longer be verified
/// exactly, so it degrades to `unseen` — the request refuses
/// `data_unavailable` without `allow_partial` (never a silent false
/// negative), while `allow_partial` serves the verified membership with
/// the `membership_incomplete` warning. Before the bound, a non-ASCII
/// literal hydrated EVERY chat region across the universe — one
/// `messages.content` point read each.
#[test]
fn search_sessions_non_ascii_verification_is_bounded() -> Result<()> {
    let mut fixture = gold_fixture();
    // 4_201 chat regions — one past the 4_096 request budget — and the
    // Cyrillic marker lives in the LAST region so the budget provably
    // gates the check.
    let mut messages: Vec<(i64, MessageRole, &'static str, Option<Value>)> = (0..4_200_i64)
        .map(|idx| (idx, MessageRole::Agent, "plain filler text", None))
        .collect();
    messages.push((
        4_200,
        MessageRole::Agent,
        "кириллический уникальный маркер",
        None,
    ));
    fixture.push(GoldSession {
        id: 8,
        agent: "claude_code",
        source_id: "local",
        external_id: "s-8-cyrillic-bulk",
        source_path: "/sessions/s-8-cyrillic-bulk.jsonl",
        workspace: None,
        title: "cyrillic bulk",
        started_at: Some(6_000),
        ended_at: Some(9_000),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        messages,
    });
    let mut bridge = gold_harness_full(&fixture, &[])?;

    // Without allow_partial the unverifiable session refuses honestly —
    // budget exhaustion is `data_unavailable`, not retryable pending.
    let page = search(&mut bridge, json!({"query":"маркер"}))?;
    ensure!(
        page["ok"] == false,
        "an unverifiable ungated evaluation must refuse: {page}"
    );
    ensure!(
        page["error"]["kind"] == "data_unavailable",
        "budget exhaustion is honest-unavailable, never a pending retry: {page}"
    );

    // allow_partial still answers — the budget-exhausted session drops
    // out as honest `unknown` rather than a fabricated negative.
    let page = search(&mut bridge, json!({"query":"маркер","allow_partial":true}))?;
    ensure!(page["ok"] == true, "partial opt-in must serve: {page}");
    ensure!(
        page["result"]["complete"] == false,
        "an unverifiable session keeps the page incomplete: {page}"
    );
    ensure!(
        ids(&page).is_empty(),
        "no session verifies the gated literal: {page}"
    );
    // `search_sessions_unicode_deep_and_byte_budget` pins the other side:
    // a session INSIDE the budget verifies the same non-ASCII literal
    // exactly (deep coordinate and all).
    bridge.shutdown()
}

/// Toggle write permission recursively — the deterministic commit
/// failure the crash-safe publication test induces.
fn set_tree_readonly(path: &Path, readonly: bool) -> Result<()> {
    let mut perms = std::fs::metadata(path)?.permissions();
    perms.set_readonly(readonly);
    std::fs::set_permissions(path, perms)?;
    if std::fs::metadata(path)?.is_dir() {
        for entry in std::fs::read_dir(path)? {
            set_tree_readonly(&entry?.path(), readonly)?;
        }
    }
    Ok(())
}

/// Two-phase region-doc publication is crash-safe: staging never touches
/// the marks row, a dropped writer or a FAILED COMMIT is the crash a
/// pre-commit `published` stamp used to certify, the failed commit
/// clears its queued stamps so a later commit cannot publish marks for
/// lost documents, and the next converge re-materializes the documents
/// and stamps `published` only after the commit seals them.
#[test]
fn region_doc_publication_is_crash_safe() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let data_dir = temp.path().join("data");
    std::fs::create_dir(&data_dir)?;
    let db = data_dir.join("archive.db");
    let writer = gold_seed(&gold_fixture(), &db)?;
    let index_path = expected_index_dir(&data_dir);
    let mark_state = |state_sql: &str| -> Result<String> {
        Ok(writer.query_row(state_sql)?.get_typed::<String>(0)?)
    };
    let pending_sql = "SELECT docs_state FROM viewer_search_region_marks WHERE conversation_id = 1";

    // Phase 1 — stage only: the marks row must still say `pending`
    // (the single statement that used to stamp it early is gone).
    {
        let mut index =
            coding_agent_search::search::tantivy::TantivyIndex::open_or_create(&index_path)?;
        let docs = index.stage_search_region_docs(&writer, 1)?;
        ensure!(docs > 0, "fixture session must produce region docs");
        let state = mark_state(pending_sql)?;
        ensure!(
            state == "pending",
            "staging must not stamp published: {state}"
        );
        // Crash: the writer drops WITHOUT commit — staged docs are lost.
    }
    let state = mark_state(pending_sql)?;
    ensure!(
        state == "pending",
        "a crashed stage must stay pending, never certified: {state}"
    );

    // Phase 2 — a FAILED commit: stage again, make the index directory
    // read-only so the segment write cannot land, and the commit must
    // fail while dropping its queued stamp. Once writable again a plain
    // commit succeeds — and the marks row is STILL pending, proving the
    // failed commit invalidated its stamp queue (otherwise this commit
    // would certify documents the lost commit may have dropped).
    {
        let mut index =
            coding_agent_search::search::tantivy::TantivyIndex::open_or_create(&index_path)?;
        let docs = index.stage_search_region_docs(&writer, 1)?;
        ensure!(docs > 0, "fixture session must produce region docs");
        set_tree_readonly(&index_path, true)?;
        let failed = index.commit();
        set_tree_readonly(&index_path, false)?;
        ensure!(failed.is_err(), "a read-only index dir must refuse commit");
        let state = mark_state(pending_sql)?;
        ensure!(
            state == "pending",
            "a failed commit must leave marks pending: {state}"
        );
        index.commit()?;
        let state = mark_state(pending_sql)?;
        ensure!(
            state == "pending",
            "a later commit must not stamp marks for the lost stage: {state}"
        );
    }

    // Recovery: converge the still-pending session on the same writer —
    // the commit seals the re-materialized documents, THEN stamps.
    {
        let mut index =
            coding_agent_search::search::tantivy::TantivyIndex::open_or_create(&index_path)?;
        index.converge_pending_search_region_docs(&writer, i64::MAX)?;
        index.commit_with_region_doc_stamps(&writer)?;
        let state = mark_state(pending_sql)?;
        ensure!(
            state == "published",
            "post-commit stamp must publish the converged session: {state}"
        );
    }
    Ok(())
}

/// The post-commit marks stamp replays the stage-time snapshot as a
/// compare-and-swap: a republish or meta edit between stage and stamp is
/// refused — the newer pending state (and its queued delete list) is
/// never overwritten by a stamp that never saw it.
#[test]
fn region_doc_stamp_is_freshness_guarded() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let data_dir = temp.path().join("data");
    std::fs::create_dir(&data_dir)?;
    let db = data_dir.join("archive.db");
    let writer = gold_seed(&gold_fixture(), &db)?;
    let index_path = expected_index_dir(&data_dir);

    let mut index = QuillCassIndex::open_or_create(&index_path)?;
    let (_docs, stamp) = coding_agent_search::viewer_extensions::search_regions::stage_region_docs(
        &mut index, &writer, 1,
    )?;
    index.commit()?;

    // A concurrent republish/meta-edit lands between stage and stamp:
    // new revision, meta_pending, a queued stale identity.
    writer.execute(
        "UPDATE viewer_search_region_marks SET source_revision = source_revision + 1, \
         docs_state = 'meta_pending', pending_doc_deletes = '[\"stale-doc-id\"]' \
         WHERE conversation_id = 1",
    )?;
    let stamped =
        coding_agent_search::viewer_extensions::search_regions::stamp_region_docs_published(
            &writer,
            &[stamp.expect("a marks row must yield a stamp")],
        )?;
    ensure!(
        stamped == 0,
        "a stale stamp must be refused, not clobber the newer state"
    );
    let row = writer.query_row(
        "SELECT docs_state, pending_doc_deletes FROM viewer_search_region_marks \
         WHERE conversation_id = 1",
    )?;
    ensure!(
        row.get_typed::<String>(0)? == "meta_pending"
            && row.get_typed::<String>(1)? == "[\"stale-doc-id\"]",
        "the newer pending state survives the refused stamp: {row:?}"
    );

    // A fresh stage sees the new snapshot — it deletes the queued
    // identity and its stamp applies cleanly.
    let (_docs, stamp) = coding_agent_search::viewer_extensions::search_regions::stage_region_docs(
        &mut index, &writer, 1,
    )?;
    index.commit()?;
    let stamped =
        coding_agent_search::viewer_extensions::search_regions::stamp_region_docs_published(
            &writer,
            &[stamp.expect("a marks row must yield a stamp")],
        )?;
    ensure!(stamped == 1, "a current stamp applies: {stamped}");
    let row = writer.query_row(
        "SELECT docs_state, pending_doc_deletes FROM viewer_search_region_marks \
         WHERE conversation_id = 1",
    )?;
    ensure!(
        row.get_typed::<String>(0)? == "published" && row.get_typed::<String>(1)? == "[]",
        "the current stamp publishes and clears the queue: {row:?}"
    );
    Ok(())
}

/// The live region-doc bound counts ONLY doc-backed rows: chat regions
/// (canonical `quill_idx`, `indexed_text` NULL) own no document — the
/// earlier `marks.region_count` bound charged every persisted row for a
/// doc it never had, under-counted the observed message share, and broke
/// the watch checkpoint's refresh/early-exit equality.
#[test]
fn live_region_doc_bound_counts_only_doc_backed_rows() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let data_dir = temp.path().join("data");
    std::fs::create_dir(&data_dir)?;
    let db = data_dir.join("archive.db");
    let writer = gold_seed(&gold_fixture(), &db)?;

    let doc_backed: i64 = writer
        .query_row(
            "SELECT COUNT(*) FROM viewer_search_regions \
             WHERE quill_idx >= 1099511627776 AND indexed_text IS NOT NULL",
        )?
        .get_typed(0)?;
    let total_rows: i64 = writer
        .query_row("SELECT COUNT(*) FROM viewer_search_regions")?
        .get_typed(0)?;
    ensure!(
        total_rows > doc_backed,
        "fixture must contain non-doc-backed chat rows: {total_rows} vs {doc_backed}"
    );
    let bound =
        coding_agent_search::viewer_extensions::search_regions::live_region_doc_bound(&writer)?;
    ensure!(
        bound == doc_backed as u64,
        "bound must count doc-backed rows only: {bound} != {doc_backed}"
    );
    let marks_total: i64 = writer
        .query_row("SELECT COALESCE(SUM(region_count),0) FROM viewer_search_region_marks")?
        .get_typed(0)?;
    ensure!(
        bound < marks_total as u64,
        "the old region_count bound ({marks_total}) over-counted: {bound}"
    );

    // A queued stale identity raises the bound by exactly its length.
    writer.execute(
        "UPDATE viewer_search_region_marks SET pending_doc_deletes = '[\"a\",\"b\"]' \
         WHERE conversation_id = 1",
    )?;
    let bound =
        coding_agent_search::viewer_extensions::search_regions::live_region_doc_bound(&writer)?;
    ensure!(
        bound == doc_backed as u64 + 2,
        "pending deletes bound stale identities: {bound}"
    );
    Ok(())
}

/// Anchoring is linear in the session size: deriving regions used to
/// scan `structure.steps` per message and `structure.tool_calls` per
/// call (O(M·S + C·T) inside the generation transaction). Deriving at
/// 16k vs 32k messages proves the scaling is ~2× (linear), never ~4×
/// (quadratic), while every tool region still resolves its step/turn
/// anchor through the maps.
#[test]
fn region_derivation_anchors_large_session_in_linear_time() -> Result<()> {
    // Two bulk sessions, 16k and 32k messages, a tool call on every
    // second message — enough structure for the quadratic term to
    // dominate utterly if it still existed.
    let bulk = |id: i64,
                count: i64,
                external_id: &'static str,
                source_path: &'static str|
     -> GoldSession {
        let messages: Vec<(i64, MessageRole, &'static str, Option<Value>)> = (0..count)
            .map(|idx| {
                if idx % 2 == 0 {
                    (
                        idx,
                        MessageRole::Agent,
                        "working",
                        Some(json!({"type":"assistant","message":{"role":"assistant",
                            "content":[{"type":"tool_use","id":format!("call-{id}-{idx}"),
                                "name":"Bash","input":{"command":format!("cmd-{idx}")}}]}})),
                    )
                } else {
                    (idx, MessageRole::Agent, "progress", None)
                }
            })
            .collect();
        GoldSession {
            id,
            agent: "claude_code",
            source_id: "local",
            external_id,
            source_path,
            workspace: None,
            title: "bulk anchors",
            started_at: Some(1_000),
            ended_at: Some(9_000),
            origin_host: None,
            metadata: json!({"cass":{"origin":{"kind":"local"}}}),
            messages,
        }
    };
    let fixture = vec![
        bulk(
            1,
            16_000,
            "s-bulk-anchors-a",
            "/sessions/s-bulk-anchors-a.jsonl",
        ),
        bulk(
            2,
            32_000,
            "s-bulk-anchors-b",
            "/sessions/s-bulk-anchors-b.jsonl",
        ),
    ];

    let temp = tempfile::tempdir()?;
    let data_dir = temp.path().join("data");
    std::fs::create_dir(&data_dir)?;
    let db = data_dir.join("archive.db");
    {
        let storage = FrankenStorage::open(&db)?;
        let agent_id = storage.ensure_agent(&Agent {
            id: None,
            slug: "claude_code".to_owned(),
            name: "claude_code".to_owned(),
            version: None,
            kind: AgentKind::Cli,
        })?;
        for session in &fixture {
            let outcome =
                storage.insert_conversation_tree(agent_id, None, &gold_conversation(session))?;
            ensure!(
                outcome.conversation_id == session.id,
                "bulk fixture insertion order pins conversation ids"
            );
        }
    }
    let writer = Connection::open(db.to_string_lossy().into_owned())?;

    let mut durations = Vec::new();
    for session in &fixture {
        let conv = gold_conversation(session);
        let generation =
            coding_agent_search::viewer_extensions::projection::generation_for_canonical(
                &writer, &conv, session.id,
            )?;
        let started = Instant::now();
        let regions =
            coding_agent_search::viewer_extensions::search_regions::derive_message_regions(
                session.id,
                &generation.packet,
                &generation.structure,
            );
        let elapsed = started.elapsed();
        let tool_regions = regions.iter().filter(|r| r.scope == "tools").count();
        ensure!(
            tool_regions == session.messages.len() / 2,
            "one tool region per call: {tool_regions}"
        );
        ensure!(
            regions
                .iter()
                .filter(|r| r.scope == "tools")
                .all(|r| r.step_index.is_some() && r.message_idx.is_some()),
            "every tool region keeps its anchors through the maps"
        );
        durations.push(elapsed);
    }
    let (small, large) = (durations[0], durations[1]);
    // Linear doubling ≈ 2×, quadratic ≈ 4×. Generous margin against
    // allocator noise while still excluding the old scan's ratio.
    ensure!(
        large < small * 3 && large < Duration::from_secs(30),
        "region derivation must stay linear — 16k={small:?} 32k={large:?}"
    );
    Ok(())
}

/// A `canonical_read_failed` (the replaced-inode signal) must also rebind
/// the deferred converge lane: without it the worker's `OwnedWriter` keeps
/// probing/draining/certifying the DEAD inode — reporting success — while
/// the live file's marks stay `meta_pending` for the rest of the session.
/// The marks row on the live archive is the durable proof: only a converge
/// committed through the REBOUND writer can publish it.
#[cfg(unix)]
#[test]
fn search_sessions_replaced_archive_inode_rebinds_converge_lane() -> Result<()> {
    let mut bridge = gold_harness_full(&gold_fixture(), &["--enable-viewer-writes"])?;

    // One healthy deferred converge first: the worker is spawned and its
    // writer is bound to the ORIGINAL archive inode.
    let patch = bridge.exchange(json!({
        "op":"patch_annotations","conversation_id":1,"expected_revision":1,
        "patch":{"notes":"prebind note"}
    }))?;
    ensure!(patch["ok"] == true, "patch refused: {patch}");
    let page = search(&mut bridge, json!({"query":"prebind"}))?;
    assert_members(&page, &[1])?;

    // Swap the archive path for a symlink — the strict reader refuses a
    // non-regular archive as `canonical_read_failed`; that arm drops the
    // writable handles and must queue the lane rebind too.
    let original = bridge.data_dir.join("archive.db.original");
    std::fs::rename(&bridge.db, &original)?;
    std::os::unix::fs::symlink(&original, &bridge.db)?;
    let refused = bridge.exchange(json!({"op":"session","conversation_id":1}))?;
    ensure!(
        refused["ok"] == false,
        "symlinked archive must refuse: {refused}"
    );
    ensure!(
        refused["error"]["kind"] == "canonical_read_failed",
        "expected canonical_read_failed, got {refused}"
    );

    // Restore byte-identical content on a NEW inode — the live file the
    // rebound lane must write. Every present family member is copied so a
    // live WAL comes along byte-faithfully.
    std::fs::remove_file(&bridge.db)?;
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let from = if suffix.is_empty() {
            original.clone()
        } else {
            bridge.data_dir.join(format!("archive.db{suffix}"))
        };
        if !from.exists() {
            continue;
        }
        let next = bridge.data_dir.join(format!("archive.db.next{suffix}"));
        std::fs::copy(&from, &next)?;
        std::fs::rename(&next, bridge.data_dir.join(format!("archive.db{suffix}")))?;
    }

    // The next patch converges through the REBOUND lane: the queued
    // `Rebind` drops the worker's dead-inode writer before its `Converge`
    // reopens on the replacement. `get_annotations` joins the lane without
    // running an index read, so `published` marks here can only come from
    // the worker's own converge — never the inline fold a search pays.
    let patch = bridge.exchange(json!({
        "op":"patch_annotations","conversation_id":1,"expected_revision":2,
        "patch":{"notes":"postbind note"}
    }))?;
    ensure!(patch["ok"] == true, "patch refused: {patch}");
    let annotations = bridge.exchange(json!({"op":"get_annotations","conversation_id":1}))?;
    ensure!(annotations["ok"] == true, "{annotations}");

    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    let marks = writer
        .query("SELECT docs_state FROM viewer_search_region_marks WHERE conversation_id = 1")?;
    ensure!(marks.len() == 1, "marks row present: {marks:?}");
    ensure!(
        marks[0].get_typed::<String>(0)? == "published",
        "rebound lane must converge the live inode, not the dead one: {marks:?}"
    );
    drop(writer);

    let page = search(&mut bridge, json!({"query":"postbind"}))?;
    assert_members(&page, &[1])?;
    bridge.shutdown()
}

/// The deferred-converge debt latch is PERSISTENT, not a per-flush window:
/// a failed `Converge` followed by TWO flush joins must leave
/// `converge_needed` set until a real drain. The earlier latch reset on
/// the first flush, so the second reported success, cleared the flag and
/// stranded `meta_pending` marks — and every later index read — forever.
#[cfg(unix)]
#[test]
fn search_sessions_failed_converge_debt_survives_repeated_flushes() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut bridge = gold_harness_full(&gold_fixture(), &["--enable-viewer-writes"])?;
    let index_dir = expected_index_dir(&bridge.data_dir);

    // Force the worker's converge pass to fail: the published Quill index
    // stays readable (the fingerprint probe still reports it live) but its
    // directory refuses writes, so the region-doc open/commit errors.
    std::fs::set_permissions(&index_dir, std::fs::Permissions::from_mode(0o555))?;

    let patch = bridge.exchange(json!({
        "op":"patch_annotations","conversation_id":1,"expected_revision":1,
        "patch":{"notes":"debtfragment owed"}
    }))?;
    ensure!(patch["ok"] == true, "patch refused: {patch}");

    // Flush join #1: the queued Converge runs before the ack — under the
    // read-only index it fails, latching the debt. This op folds nothing.
    let annotations = bridge.exchange(json!({"op":"get_annotations","conversation_id":1}))?;
    ensure!(annotations["ok"] == true, "{annotations}");
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    let marks = writer
        .query("SELECT docs_state FROM viewer_search_region_marks WHERE conversation_id = 1")?;
    ensure!(
        marks.len() == 1 && marks[0].get_typed::<String>(0)? == "meta_pending",
        "the injected converge failure must leave marks pending: {marks:?}"
    );

    // Restore writability, then flush join #2 with NO intervening command:
    // the persistent latch must still report the owed work — the old
    // per-flush window reported success here and cleared the flag.
    std::fs::set_permissions(&index_dir, std::fs::Permissions::from_mode(0o755))?;
    let annotations = bridge.exchange(json!({"op":"get_annotations","conversation_id":1}))?;
    ensure!(annotations["ok"] == true, "{annotations}");

    // The index read then folds the owed converge inline: the new meta
    // fragment matches and the durable marks row publishes.
    let page = search(&mut bridge, json!({"query":"debtfragment"}))?;
    assert_members(&page, &[1])?;
    let marks = writer
        .query("SELECT docs_state FROM viewer_search_region_marks WHERE conversation_id = 1")?;
    ensure!(
        marks.len() == 1 && marks[0].get_typed::<String>(0)? == "published",
        "the retried converge must publish the owed marks: {marks:?}"
    );
    drop(writer);
    bridge.shutdown()
}

// ---------------------------------------------------------------------------
// Strict search: text ABSENT AT THE SOURCE vs text NOT READ (owner
// decision 2026-11-07)
// ---------------------------------------------------------------------------
//
// A coverage gap whose reason code is on the closed source-absent
// allowlist — text provably non-existent in ANY local record, e.g. the
// provider-encrypted codex `agent_message` handoff — must NOT block
// strict search: membership is exact over the text that exists, with a
// per-item `source_text_absent` annotation and a response-level count of
// the sessions that carried such gaps. Everything else (unread text,
// unknown shapes, extractor gaps, facts pending, or a reason code the
// allowlist does not name) keeps blocking exactly as before.
// ---------------------------------------------------------------------------

/// Overwrite one session's persisted facts coverage after spawn. The
/// bridge re-reads `viewer_session_facts` on every request, so the next
/// `search_sessions` evaluates the new manifest — the same post-spawn
/// mutation lane the large-universe honesty test uses.
fn set_facts_coverage(bridge: &BridgeHarness, id: i64, state: &str, detail: Value) -> Result<()> {
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer.execute_with_params(
        "UPDATE viewer_session_facts SET coverage_state=?1, coverage_detail_json=?2 \
         WHERE conversation_id=?3",
        &param_slice_to_values(params![state, detail.to_string(), id]),
    )?;
    Ok(())
}

/// The manifest the codex extractor publishes for a session whose ONLY
/// inventory gap is a provider-encrypted `agent_message` handoff — the
/// exact shape `codex_agent_message_encrypted_payload_is_known_unreadable`
/// pins in the ingest suite: `chat` partial (the readable envelope header
/// is evidence), `text_inventory` unavailable with `agent_message_encrypted`,
/// `block_inventory` complete, plus the fixed overlay/metrics/compaction
/// entries every publication carries. The encrypted text never existed in
/// any local record, so its absence is provable at the source.
fn codex_encrypted_manifest() -> Value {
    json!({
        "extractor": "codex-facts-v1",
        "extractor_version": 2,
        "provider": "codex",
        "fields": {
            "chat": {"state":"partial","reasons":["agent_message_encrypted"]},
            "tools": {"state":"complete","reasons":[]},
            "thinking": {"state":"complete","reasons":[]},
            "tool_exit": {"state":"complete","reasons":[]},
            "tool_error": {"state":"complete","reasons":[]},
            "block_inventory": {"state":"complete","reasons":[]},
            "text_inventory": {"state":"unavailable","reasons":["agent_message_encrypted"]},
            "files": {"state":"complete","reasons":[]},
            "mcp": {"state":"complete","reasons":[]},
            "title": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "artifacts": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "subagents": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "is_subagent": {"state":"complete","reasons":[]},
            "parent_relation": {"state":"complete","reasons":[]},
            "metrics": {"state":"partial","reasons":["native_metrics_partial"]},
            "compaction": {"state":"unavailable","reasons":["boundary_records_not_normalized"]}
        },
        "counts": {"tool_calls": 0, "paired_calls": 0}
    })
}

/// The manifest the claude extractor publishes when a tool result's text
/// exists at the source but was not recorded — a NOT-READ gap
/// (`tool_output_not_recorded`): strict search must keep refusing on it.
fn claude_unread_tool_output_manifest() -> Value {
    json!({
        "extractor": "claude-facts-v1",
        "extractor_version": 2,
        "provider": "claude_code",
        "fields": {
            "chat": {"state":"complete","reasons":[]},
            "tools": {"state":"partial","reasons":["tool_output_not_recorded"]},
            "thinking": {"state":"complete","reasons":[]},
            "tool_exit": {"state":"complete","reasons":[]},
            "tool_error": {"state":"complete","reasons":[]},
            "block_inventory": {"state":"complete","reasons":[]},
            "text_inventory": {"state":"partial","reasons":["tool_output_not_recorded"]},
            "files": {"state":"complete","reasons":[]},
            "mcp": {"state":"complete","reasons":[]},
            "title": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "artifacts": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "subagents": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "is_subagent": {"state":"complete","reasons":[]},
            "parent_relation": {"state":"complete","reasons":[]},
            "metrics": {"state":"partial","reasons":["native_metrics_partial"]},
            "compaction": {"state":"unavailable","reasons":["boundary_records_not_normalized"]}
        },
        "counts": {"tool_calls": 1, "paired_calls": 1}
    })
}

fn result_item(page: &Value, id: i64) -> Result<Value> {
    page["result"]["items"]
        .as_array()
        .expect("items array")
        .iter()
        .find(|item| item["conversation_id"] == id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("session {id} missing from items: {page}"))
}

/// (i) A session whose ONLY inventory gap is `agent_message_encrypted`
/// (text provably absent at the source) must ANSWER strict search — no
/// `allow_partial` — because membership over the existing text is exact.
/// The result item carries the `source_text_absent` annotation, clean
/// sessions carry an empty one, and the response counts how many of the
/// evaluated sessions had such a gap.
#[test]
fn search_sessions_source_absent_gap_answers_strict_with_annotation() -> Result<()> {
    let mut bridge = gold_harness()?;
    set_facts_coverage(&bridge, 5, "unavailable", codex_encrypted_manifest())?;

    // S5's chat text ("codex remote quux") still exists and matches: the
    // strict page answers S3+S5 exactly as if the facts were complete.
    let page = search(&mut bridge, json!({"query":"quux"}))?;
    assert_members(&page, &[3, 5])?;
    let s5 = result_item(&page, 5)?;
    ensure!(
        s5["source_text_absent"] == json!(["agent_message_encrypted"]),
        "the source-absent session must be annotated with its reason: {s5}"
    );
    let s3 = result_item(&page, 3)?;
    ensure!(
        s3["source_text_absent"] == json!([]),
        "a session without gaps keeps an empty annotation: {s3}"
    );
    ensure!(
        page["result"]["source_absent_sessions"] == 1,
        "one evaluated session had a source-absent gap: {page}"
    );

    // A term S5's existing text lacks is a PROVEN non-member: S5 drops out
    // of the result set and the strict answer stays complete — but S5 was
    // still evaluated under source-absence, so the response keeps counting
    // it even though it is not a result item.
    let page = search(&mut bridge, json!({"query":"foo"}))?;
    assert_members(&page, &[1, 2])?;
    ensure!(
        page["result"]["source_absent_sessions"] == 1,
        "the count covers the evaluated universe, not just result items: {page}"
    );
    bridge.shutdown()
}

/// (ii) A source-absent session does not unlock strict search for the
/// rest of the universe: one additional NOT-READ gap (`tool_output_not_
/// recorded` — text that exists at the source but was not recorded) keeps
/// the honest `data_unavailable` refusal. Narrowing the universe to the
/// source-absent session alone answers strictly again.
#[test]
fn search_sessions_source_absent_plus_unread_gap_still_refuses() -> Result<()> {
    let mut bridge = gold_harness()?;
    set_facts_coverage(&bridge, 5, "unavailable", codex_encrypted_manifest())?;
    set_facts_coverage(&bridge, 4, "partial", claude_unread_tool_output_manifest())?;

    // S4's unrecorded tool output could still hold "quux": the strict
    // answer stays refused — the (b) relaxation never weakens an (a) gap.
    let page = search(&mut bridge, json!({"query":"quux"}))?;
    ensure!(page["ok"] == false, "the unread gap must refuse: {page}");
    ensure!(
        page["error"]["kind"] == "data_unavailable",
        "expected data_unavailable, got {page}"
    );

    // With the universe narrowed to the source-absent session alone, the
    // strict answer is exact again.
    let page = search(
        &mut bridge,
        json!({"query":"quux","filters":{"provider":"codex"}}),
    )?;
    assert_members(&page, &[5])?;
    let s5 = result_item(&page, 5)?;
    ensure!(
        s5["source_text_absent"] == json!(["agent_message_encrypted"]),
        "the source-absent session must be annotated: {s5}"
    );
    ensure!(
        page["result"]["source_absent_sessions"] == 1,
        "one evaluated session had a source-absent gap: {page}"
    );
    bridge.shutdown()
}

/// (iii) NOT queries over a source-absent session: its existing text
/// provably lacks the term, so the session IS a NOT-match — exact strict
/// membership, annotated like any other result item.
#[test]
fn search_sessions_not_query_treats_source_absent_session_as_exact() -> Result<()> {
    let mut bridge = gold_harness()?;
    set_facts_coverage(&bridge, 5, "unavailable", codex_encrypted_manifest())?;

    // No session contains the term: every non-subagent session is a
    // NOT-match, including the source-absent S5.
    let page = search(&mut bridge, json!({"query":"NOT zzz_absent_literal"}))?;
    assert_members(&page, &[1, 2, 3, 4, 5, 7])?;
    let s5 = result_item(&page, 5)?;
    ensure!(
        s5["source_text_absent"] == json!(["agent_message_encrypted"]),
        "a NOT-matching source-absent session carries the same annotation: {s5}"
    );
    ensure!(
        page["result"]["source_absent_sessions"] == 1,
        "one evaluated session had a source-absent gap: {page}"
    );

    // S5's existing text contains "quux", so under NOT it is a proven
    // non-member: the answer stays strict and complete without it.
    let page = search(&mut bridge, json!({"query":"NOT quux"}))?;
    assert_members(&page, &[1, 2, 4, 7])?;
    ensure!(
        page["result"]["source_absent_sessions"] == 1,
        "S5 was evaluated (and excluded) under source-absence: {page}"
    );
    bridge.shutdown()
}

/// (iv) The classification is a CLOSED ALLOWLIST: a reason code it does
/// not name — however new or plausible — is a NOT-READ gap and keeps
/// blocking, even mixed with an allowlisted code. The queries use `foo`,
/// a term S5's existing text lacks, so S5's membership is genuinely
/// unknown (a verified positive match would dominate regardless — `quux`
/// answers with S5 either way). `allow_partial` serves the verified
/// membership with the gap session dropped, and no source-absent
/// annotation or count is ever claimed for it.
#[test]
fn search_sessions_unknown_reason_code_still_blocks_strict() -> Result<()> {
    let mut bridge = gold_harness()?;

    // A brand-new reason code: not-read by default, strict stays refused.
    let mut unknown = codex_encrypted_manifest();
    unknown["fields"]["chat"]["reasons"] = json!(["brand_new_gap_code"]);
    unknown["fields"]["text_inventory"]["reasons"] = json!(["brand_new_gap_code"]);
    set_facts_coverage(&bridge, 5, "unavailable", unknown)?;
    let page = search(&mut bridge, json!({"query":"foo"}))?;
    ensure!(
        page["ok"] == false,
        "an unnamed reason code must keep strict search refused: {page}"
    );
    ensure!(
        page["error"]["kind"] == "data_unavailable",
        "expected data_unavailable, got {page}"
    );

    // allow_partial serves the verified membership (S1+S2) with S5
    // dropped as honestly unknown — and no annotation or source-absent
    // count is claimed for a session that is not provably absent at
    // source.
    let page = search(&mut bridge, json!({"query":"foo","allow_partial":true}))?;
    ensure!(page["ok"] == true, "partial opt-in must serve: {page}");
    ensure!(
        page["result"]["complete"] == false,
        "the unknown session keeps the page incomplete: {page}"
    );
    assert_eq!(
        ids(&page),
        vec![1, 2],
        "only the verified sessions serve: {page}"
    );
    let s1 = result_item(&page, 1)?;
    ensure!(
        s1["source_text_absent"] == json!([]),
        "no source-absent annotation for unnamed codes: {s1}"
    );
    ensure!(
        page["result"]["source_absent_sessions"] == 0,
        "no source-absent count for unnamed codes: {page}"
    );

    // Mixed allowlisted + unknown reasons on the same field: the unknown
    // member alone is enough to keep strict refused.
    let mut mixed = codex_encrypted_manifest();
    mixed["fields"]["chat"]["reasons"] = json!(["agent_message_encrypted", "brand_new_gap_code"]);
    mixed["fields"]["text_inventory"]["reasons"] =
        json!(["agent_message_encrypted", "brand_new_gap_code"]);
    set_facts_coverage(&bridge, 5, "unavailable", mixed)?;
    let page = search(&mut bridge, json!({"query":"foo"}))?;
    ensure!(
        page["ok"] == false,
        "a mixed reason set must keep strict search refused: {page}"
    );
    ensure!(
        page["error"]["kind"] == "data_unavailable",
        "expected data_unavailable, got {page}"
    );
    bridge.shutdown()
}

/// The manifest the codex extractor publishes for a session whose ONLY
/// inventory gap is an interrupted call (codex-facts-v2): the rollout
/// provably ended mid-call — the call is the packet's last event, or the
/// next task's user message cut the pending turn (codex never accepts user
/// input mid-turn) — so the result text never existed in any local record.
/// `tools` and `text_inventory` go partial with `tool_output_interrupted`
/// (on the source-absent allowlist); the tool-verdict scopes keep their
/// honest unrecorded gaps, which are not text claims.
fn codex_interrupted_call_manifest() -> Value {
    json!({
        "extractor": "codex-facts-v2",
        "extractor_version": 2,
        "provider": "codex",
        "fields": {
            "chat": {"state":"complete","reasons":[]},
            "tools": {"state":"partial","reasons":["tool_output_interrupted"]},
            "thinking": {"state":"complete","reasons":[]},
            "tool_exit": {"state":"unavailable","reasons":["tool_exit_not_recorded"]},
            "tool_error": {"state":"unavailable","reasons":["tool_error_not_recorded"]},
            "block_inventory": {"state":"complete","reasons":[]},
            "text_inventory": {"state":"partial","reasons":["tool_output_interrupted"]},
            "files": {"state":"complete","reasons":[]},
            "mcp": {"state":"complete","reasons":[]},
            "title": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "artifacts": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "subagents": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "is_subagent": {"state":"complete","reasons":[]},
            "parent_relation": {"state":"complete","reasons":[]},
            "metrics": {"state":"partial","reasons":["native_metrics_partial"]},
            "compaction": {"state":"unavailable","reasons":["boundary_records_not_normalized"]}
        },
        "counts": {"tool_calls": 1, "paired_calls": 0}
    })
}

/// (v) The interrupted-call proof joins the source-absent allowlist: the
/// rollout provably holds no output record for the call, so membership
/// over the text that does exist stays exact. The session ANSWERS strict
/// search with the `tool_output_interrupted` annotation.
#[test]
fn search_sessions_interrupted_tool_output_answers_strict_with_annotation() -> Result<()> {
    let mut bridge = gold_harness()?;
    set_facts_coverage(&bridge, 5, "partial", codex_interrupted_call_manifest())?;

    // S5's chat text ("codex remote quux") still exists and matches: the
    // strict page answers S3+S5 exactly as if the facts were complete.
    let page = search(&mut bridge, json!({"query":"quux"}))?;
    assert_members(&page, &[3, 5])?;
    let s5 = result_item(&page, 5)?;
    ensure!(
        s5["source_text_absent"] == json!(["tool_output_interrupted"]),
        "the interrupted session must be annotated with its reason: {s5}"
    );
    ensure!(
        page["result"]["source_absent_sessions"] == 1,
        "one evaluated session had a source-absent gap: {page}"
    );

    // A term S5's existing text lacks is a PROVEN non-member.
    let page = search(&mut bridge, json!({"query":"foo"}))?;
    assert_members(&page, &[1, 2])?;
    bridge.shutdown()
}

/// The manifest the claude extractor publishes for a rewritten-replay pair
/// (claude-facts-v3): the live record survives the facts lane, BOTH texts
/// index as packet messages, and the conflict is recorded on the dedicated
/// `replay_conflict` scope — a custom scope no strict gate consults — so
/// every core text verdict stays complete.
fn claude_rewritten_replay_manifest() -> Value {
    json!({
        "extractor": "claude-facts-v3",
        "extractor_version": 2,
        "provider": "claude_code",
        "fields": {
            "chat": {"state":"complete","reasons":[]},
            "tools": {"state":"complete","reasons":[]},
            "thinking": {"state":"complete","reasons":[]},
            "tool_exit": {"state":"complete","reasons":[]},
            "tool_error": {"state":"complete","reasons":[]},
            "block_inventory": {"state":"complete","reasons":[]},
            "text_inventory": {"state":"complete","reasons":[]},
            "files": {"state":"complete","reasons":[]},
            "mcp": {"state":"complete","reasons":[]},
            "replay_conflict": {"state":"partial","reasons":["claude_rewritten_replay"]},
            "title": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "artifacts": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "subagents": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "is_subagent": {"state":"complete","reasons":[]},
            "parent_relation": {"state":"complete","reasons":[]},
            "metrics": {"state":"partial","reasons":["native_metrics_partial"]},
            "compaction": {"state":"unavailable","reasons":["boundary_records_not_normalized"]}
        },
        "counts": {"tool_calls": 0, "paired_calls": 0}
    })
}

/// (vi) A rewritten-replay conflict rides a custom `replay_conflict` scope:
/// both versions of the record are local indexed packet messages, so the
/// conflict never demotes a text verdict, never blocks strict search and
/// never claims a source-absent annotation (the rewritten text EXISTS —
/// twice).
#[test]
fn search_sessions_replay_conflict_scope_never_blocks_strict() -> Result<()> {
    let mut bridge = gold_harness()?;
    set_facts_coverage(&bridge, 4, "partial", claude_rewritten_replay_manifest())?;

    // S4's text still matches, and the conflict scope does not exclude it.
    let page = search(&mut bridge, json!({"query":"answer"}))?;
    assert_members(&page, &[4])?;
    let s4 = result_item(&page, 4)?;
    ensure!(
        s4["source_text_absent"] == json!([]),
        "a replay conflict is not a source-absent annotation: {s4}"
    );
    ensure!(
        page["result"]["source_absent_sessions"] == 0,
        "no source-absent count for a replay conflict: {page}"
    );

    // A term S4 lacks is a proven non-member; the evaluated conflict scope
    // keeps the strict answer complete.
    let page = search(&mut bridge, json!({"query":"foo"}))?;
    assert_members(&page, &[1, 2])?;
    bridge.shutdown()
}

/// The manifest the codex extractor publishes for a session whose ONLY
/// inventory gap is a reasoning item carrying `encrypted_content` — the
/// exact shape the encrypted arm of `fn reasoning()` in
/// `src/viewer_extensions/providers/codex.rs` produces (the code is
/// emitted only there, never for a parse failure): `thinking` and
/// `text_inventory` unavailable with `reasoning_encrypted`, `chat`
/// complete (the readable chat text is evidence), plus the fixed
/// overlay/metrics/compaction entries. The provider hands the reasoning
/// over already encrypted, so no plaintext ever existed locally.
fn codex_reasoning_encrypted_manifest() -> Value {
    json!({
        "extractor": "codex-facts-v1",
        "extractor_version": 2,
        "provider": "codex",
        "fields": {
            "chat": {"state":"complete","reasons":[]},
            "tools": {"state":"complete","reasons":[]},
            "thinking": {"state":"unavailable","reasons":["reasoning_encrypted"]},
            "tool_exit": {"state":"complete","reasons":[]},
            "tool_error": {"state":"complete","reasons":[]},
            "block_inventory": {"state":"complete","reasons":[]},
            "text_inventory": {"state":"unavailable","reasons":["reasoning_encrypted"]},
            "files": {"state":"complete","reasons":[]},
            "mcp": {"state":"complete","reasons":[]},
            "title": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "artifacts": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "subagents": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "is_subagent": {"state":"complete","reasons":[]},
            "parent_relation": {"state":"complete","reasons":[]},
            "metrics": {"state":"partial","reasons":["native_metrics_partial"]},
            "compaction": {"state":"unavailable","reasons":["boundary_records_not_normalized"]}
        },
        "counts": {"tool_calls": 0, "paired_calls": 0}
    })
}

/// The manifest the claude extractor publishes for a session whose ONLY
/// inventory gap is a `redacted_thinking` block — the exact shape the
/// `redacted_thinking` arm of `fn part()` in
/// `src/viewer_extensions/providers/claude.rs` produces (pinned by the
/// details suite's thinking fixture; the code is emitted only for that
/// explicit block type, never for an unknown shape): `thinking` and
/// `text_inventory` unavailable with `thinking_redacted`, `chat`
/// complete. Anthropic returns redacted thinking only encrypted, so no
/// plaintext ever existed locally.
fn claude_redacted_thinking_manifest() -> Value {
    json!({
        "extractor": "claude-facts-v1",
        "extractor_version": 2,
        "provider": "claude_code",
        "fields": {
            "chat": {"state":"complete","reasons":[]},
            "tools": {"state":"complete","reasons":[]},
            "thinking": {"state":"unavailable","reasons":["thinking_redacted"]},
            "tool_exit": {"state":"complete","reasons":[]},
            "tool_error": {"state":"complete","reasons":[]},
            "block_inventory": {"state":"complete","reasons":[]},
            "text_inventory": {"state":"unavailable","reasons":["thinking_redacted"]},
            "files": {"state":"complete","reasons":[]},
            "mcp": {"state":"complete","reasons":[]},
            "title": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "artifacts": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "subagents": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "is_subagent": {"state":"complete","reasons":[]},
            "parent_relation": {"state":"complete","reasons":[]},
            "metrics": {"state":"partial","reasons":["native_metrics_partial"]},
            "compaction": {"state":"unavailable","reasons":["boundary_records_not_normalized"]}
        },
        "counts": {"tool_calls": 0, "paired_calls": 0}
    })
}

/// (v) Owner follow-up (2026-11-07): the source-absent class covers text
/// the PROVIDER encrypted so no plaintext ever exists locally. A codex
/// session whose only gap is `reasoning_encrypted` (a reasoning item
/// carrying `encrypted_content`) must ANSWER strict search with the
/// `source_text_absent` annotation, exactly like
/// `agent_message_encrypted`.
#[test]
fn search_sessions_reasoning_encrypted_gap_answers_strict_with_annotation() -> Result<()> {
    let mut bridge = gold_harness()?;
    set_facts_coverage(
        &bridge,
        5,
        "unavailable",
        codex_reasoning_encrypted_manifest(),
    )?;

    // S5's chat text ("codex remote quux") still exists and matches: the
    // strict page answers S3+S5 exactly, with the encrypted-reasoning gap
    // annotated.
    let page = search(&mut bridge, json!({"query":"quux"}))?;
    assert_members(&page, &[3, 5])?;
    let s5 = result_item(&page, 5)?;
    ensure!(
        s5["source_text_absent"] == json!(["reasoning_encrypted"]),
        "the provider-encrypted reasoning gap must be annotated: {s5}"
    );
    let s3 = result_item(&page, 3)?;
    ensure!(
        s3["source_text_absent"] == json!([]),
        "a session without gaps keeps an empty annotation: {s3}"
    );
    ensure!(
        page["result"]["source_absent_sessions"] == 1,
        "one evaluated session had a source-absent gap: {page}"
    );

    // A term S5's existing text lacks is a PROVEN non-member: S5 drops
    // out of the result set, the strict answer stays complete, and the
    // gap keeps counting over the evaluated universe.
    let page = search(&mut bridge, json!({"query":"foo"}))?;
    assert_members(&page, &[1, 2])?;
    ensure!(
        page["result"]["source_absent_sessions"] == 1,
        "S5 was evaluated (and excluded) under source-absence: {page}"
    );
    bridge.shutdown()
}

/// (vi) Owner follow-up (2026-11-07), claude side: a session whose only
/// gap is a `redacted_thinking` block (`thinking_redacted`) answers
/// strict search with the annotation — same provider-encrypted class.
#[test]
fn search_sessions_thinking_redacted_gap_answers_strict_with_annotation() -> Result<()> {
    let mut bridge = gold_harness()?;
    set_facts_coverage(
        &bridge,
        4,
        "unavailable",
        claude_redacted_thinking_manifest(),
    )?;

    // S4's chat text ("answer follows") still exists and matches: the
    // strict page answers exactly, with the redacted-thinking gap
    // annotated.
    let page = search(&mut bridge, json!({"query":"answer"}))?;
    assert_members(&page, &[4])?;
    let s4 = result_item(&page, 4)?;
    ensure!(
        s4["source_text_absent"] == json!(["thinking_redacted"]),
        "the redacted-thinking gap must be annotated: {s4}"
    );
    ensure!(
        page["result"]["source_absent_sessions"] == 1,
        "one evaluated session had a source-absent gap: {page}"
    );

    // A term S4's existing text lacks is a PROVEN non-member: S4 drops
    // out, the strict answer stays complete, and the gap keeps counting.
    let page = search(&mut bridge, json!({"query":"foo"}))?;
    assert_members(&page, &[1, 2])?;
    ensure!(
        page["result"]["source_absent_sessions"] == 1,
        "S4 was evaluated (and excluded) under source-absence: {page}"
    );
    bridge.shutdown()
}

/// The manifest the v3 antigravity extractor publishes for a session whose
/// ONLY text gap is the source-side truncation marker — the exact shape
/// `antigravity_f10_truncated_fields_mark_scopes_source_absent` pins in
/// the ingest suite: `chat`/`thinking`/`files` partial and `text_inventory`
/// unavailable, every one with `agy_source_truncated` only. The F10
/// passthrough marker proves the source application cut the named fields
/// BEFORE persisting, so the cut text never existed in any local record.
/// A session without MODEL-step envelopes keeps the block inventory
/// complete, so strict search answers over the surviving text.
fn antigravity_source_truncated_manifest() -> Value {
    json!({
        "extractor": "antigravity-facts-v3",
        "extractor_version": 3,
        "provider": "antigravity",
        "fields": {
            "chat": {"state":"partial","reasons":["agy_source_truncated"]},
            "tools": {"state":"complete","reasons":[]},
            "thinking": {"state":"partial","reasons":["agy_source_truncated"]},
            "tool_exit": {"state":"complete","reasons":[]},
            "tool_error": {"state":"complete","reasons":[]},
            "block_inventory": {"state":"complete","reasons":[]},
            "text_inventory": {"state":"unavailable","reasons":["agy_source_truncated"]},
            "files": {"state":"partial","reasons":["agy_source_truncated"]},
            "mcp": {"state":"complete","reasons":[]},
            "title": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "artifacts": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "subagents": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "is_subagent": {"state":"complete","reasons":[]},
            "parent_relation": {"state":"complete","reasons":[]},
            "metrics": {"state":"partial","reasons":["native_metrics_partial"]},
            "compaction": {"state":"unavailable","reasons":["boundary_records_not_normalized"]}
        },
        "counts": {"tool_calls": 0, "paired_calls": 0}
    })
}

/// (vii) Owner follow-up (2026-11-08), antigravity side: the F10
/// `truncated_fields` passthrough marker is a source-absent gap — the
/// source app cut the named fields before persisting, so no read of any
/// local artifact could recover them. A session whose only gaps carry
/// that marker answers strict search over its surviving text, annotated
/// exactly like the provider-encrypted class.
#[test]
fn search_sessions_agy_source_truncated_gap_answers_strict_with_annotation() -> Result<()> {
    let mut bridge = gold_harness()?;
    set_facts_coverage(
        &bridge,
        5,
        "partial",
        antigravity_source_truncated_manifest(),
    )?;

    // S5's chat text ("codex remote quux") still exists and matches: the
    // strict page answers S3+S5 exactly, with the truncation gap
    // annotated.
    let page = search(&mut bridge, json!({"query":"quux"}))?;
    assert_members(&page, &[3, 5])?;
    let s5 = result_item(&page, 5)?;
    ensure!(
        s5["source_text_absent"] == json!(["agy_source_truncated"]),
        "the source-truncation gap must be annotated: {s5}"
    );
    let s3 = result_item(&page, 3)?;
    ensure!(
        s3["source_text_absent"] == json!([]),
        "a session without gaps keeps an empty annotation: {s3}"
    );
    ensure!(
        page["result"]["source_absent_sessions"] == 1,
        "one evaluated session had a source-absent gap: {page}"
    );

    // A term S5's existing text lacks is a PROVEN non-member: S5 drops
    // out, the strict answer stays complete, and the gap keeps counting.
    let page = search(&mut bridge, json!({"query":"foo"}))?;
    assert_members(&page, &[1, 2])?;
    ensure!(
        page["result"]["source_absent_sessions"] == 1,
        "S5 was evaluated (and excluded) under source-absence: {page}"
    );
    bridge.shutdown()
}

/// The manifest the v2 claude extractor publishes for a Desktop sidecar
/// session whose ONLY inventory gap is the app's own body-cull marker —
/// the exact shape `claude_desktop_sidecar_envelope_is_a_known_shape`
/// pins in the ingest suite: `chat` complete (the metadata lines and the
/// `raw.initialMessage` prompt region are the session's real, indexed
/// text), `block_inventory`/`text_inventory` unavailable with
/// `claude_sidecar_body_culled`. The sidecar's explicit
/// `cass.body_available == false` envelope is the Desktop app stating
/// the CLI conversation body was culled before the sidecar was written,
/// so the missing body text never existed in any local record.
fn claude_sidecar_culled_manifest() -> Value {
    json!({
        "extractor": "claude-facts-v2",
        "extractor_version": 6,
        "provider": "claude_code",
        "fields": {
            "chat": {"state":"complete","reasons":[]},
            "tools": {"state":"complete","reasons":[]},
            "thinking": {"state":"complete","reasons":[]},
            "tool_exit": {"state":"complete","reasons":[]},
            "tool_error": {"state":"complete","reasons":[]},
            "block_inventory": {"state":"unavailable","reasons":["claude_sidecar_body_culled"]},
            "text_inventory": {"state":"unavailable","reasons":["claude_sidecar_body_culled"]},
            "files": {"state":"complete","reasons":[]},
            "mcp": {"state":"complete","reasons":[]},
            "title": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "artifacts": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "subagents": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "is_subagent": {"state":"complete","reasons":[]},
            "parent_relation": {"state":"complete","reasons":[]},
            "metrics": {"state":"partial","reasons":["native_metrics_partial"]},
            "compaction": {"state":"unavailable","reasons":["boundary_records_not_normalized"]}
        },
        "counts": {"tool_calls": 0, "paired_calls": 0}
    })
}

/// (ix) Owner follow-up (2026-11-08), claude side: the Desktop sidecar's
/// body-cull marker is a source-absent gap — the app's own
/// `body_available: false` envelope proves the culled conversation body
/// never existed in any local record. A session whose only gaps carry
/// that marker answers strict search over its surviving text (the
/// metadata lines plus the `initialMessage` prompt region), annotated
/// exactly like the provider-encrypted class.
#[test]
fn search_sessions_claude_sidecar_culled_gap_answers_strict_with_annotation() -> Result<()> {
    let mut bridge = gold_harness()?;
    set_facts_coverage(&bridge, 5, "unavailable", claude_sidecar_culled_manifest())?;

    // S5's chat text ("codex remote quux") still exists and matches: the
    // strict page answers S3+S5 exactly, with the body-cull gap annotated.
    let page = search(&mut bridge, json!({"query":"quux"}))?;
    assert_members(&page, &[3, 5])?;
    let s5 = result_item(&page, 5)?;
    ensure!(
        s5["source_text_absent"] == json!(["claude_sidecar_body_culled"]),
        "the sidecar body-cull gap must be annotated: {s5}"
    );
    let s3 = result_item(&page, 3)?;
    ensure!(
        s3["source_text_absent"] == json!([]),
        "a session without gaps keeps an empty annotation: {s3}"
    );
    ensure!(
        page["result"]["source_absent_sessions"] == 1,
        "one evaluated session had a source-absent gap: {page}"
    );

    // A term S5's existing text lacks is a PROVEN non-member: S5 drops
    // out, the strict answer stays complete, and the gap keeps counting.
    let page = search(&mut bridge, json!({"query":"foo"}))?;
    assert_members(&page, &[1, 2])?;
    ensure!(
        page["result"]["source_absent_sessions"] == 1,
        "S5 was evaluated (and excluded) under source-absence: {page}"
    );
    bridge.shutdown()
}

/// (x) The new allowlist member never over-unlocks: a session whose
/// inventory gap mixes the body-cull marker with an unknown-shape mark is
/// NOT provably source-absent — matching regions could still hide behind
/// the unknown shape — so strict search keeps its honest
/// `data_unavailable` refusal.
#[test]
fn search_sessions_claude_sidecar_cull_mixed_with_unknown_still_refuses_strict() -> Result<()> {
    let mut bridge = gold_harness()?;
    let mut manifest = claude_sidecar_culled_manifest();
    manifest["fields"]["text_inventory"] = json!({"state":"unavailable","reasons":["claude_sidecar_body_culled","claude_shape_unknown"]});
    set_facts_coverage(&bridge, 5, "unavailable", manifest)?;

    let page = search(&mut bridge, json!({"query":"foo"}))?;
    ensure!(
        page["ok"] == false,
        "the mixed cull+unknown gap must keep strict search refused: {page}"
    );
    ensure!(
        page["error"]["kind"] == "data_unavailable",
        "expected data_unavailable, got {page}"
    );
    bridge.shutdown()
}

/// The manifest the v3 antigravity extractor publishes for a REALISTIC F10
/// session that also carries truncation markers: the canonical `agy_type`
/// envelope (block inventory), unrecorded call identities (tools) and the
/// F10 lifecycle-status verdicts (tool_exit/tool_error partial) sit
/// alongside the truncation marker. The envelope and identity marks are
/// honest NOT-READ gaps — structure the connector does not preserve, so
/// matching regions could still hide behind them.
fn antigravity_f10_realistic_manifest() -> Value {
    json!({
        "extractor": "antigravity-facts-v3",
        "extractor_version": 3,
        "provider": "antigravity",
        "fields": {
            "chat": {"state":"partial","reasons":["agy_source_truncated"]},
            "tools": {"state":"partial","reasons":[
                "agy_tool_call_identity_missing",
                "agy_tool_result_identity_missing",
                "agy_source_truncated"
            ]},
            "thinking": {"state":"partial","reasons":["agy_source_truncated"]},
            "tool_exit": {"state":"partial","reasons":["agy_status_unpaired"]},
            "tool_error": {"state":"partial","reasons":["agy_status_unpaired"]},
            "block_inventory": {"state":"partial","reasons":["agy_canonical_envelope"]},
            "text_inventory": {"state":"partial","reasons":["agy_source_truncated"]},
            "files": {"state":"partial","reasons":["agy_source_truncated"]},
            "mcp": {"state":"complete","reasons":[]},
            "title": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "artifacts": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "subagents": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "is_subagent": {"state":"complete","reasons":[]},
            "parent_relation": {"state":"complete","reasons":[]},
            "metrics": {"state":"partial","reasons":["native_metrics_partial"]},
            "compaction": {"state":"unavailable","reasons":["boundary_records_not_normalized"]}
        },
        "counts": {"tool_calls": 0, "paired_calls": 0}
    })
}

/// (viii) The allowlist member never over-unlocks: a realistic agy session
/// also carries the canonical-envelope and call-identity marks — NOT-READ
/// gaps that can still hide matching regions — so strict search keeps its
/// honest `data_unavailable` refusal even though `agy_source_truncated`
/// itself is allowlisted. The probe uses `foo`, a term S5's verified text
/// lacks: a verified positive match would dominate regardless, but S5's
/// non-membership is exactly what the envelope gap leaves unprovable.
#[test]
fn search_sessions_agy_realistic_marks_still_refuse_strict() -> Result<()> {
    let mut bridge = gold_harness()?;
    set_facts_coverage(&bridge, 5, "partial", antigravity_f10_realistic_manifest())?;

    let page = search(&mut bridge, json!({"query":"foo"}))?;
    ensure!(
        page["ok"] == false,
        "the envelope/identity gaps must keep strict search refused: {page}"
    );
    ensure!(
        page["error"]["kind"] == "data_unavailable",
        "expected data_unavailable, got {page}"
    );
    bridge.shutdown()
}

/// The manifest the post-F15 antigravity extractor publishes for the
/// dominant real-archive shape once every emitted record archives its raw
/// source: the block inventory is VERIFIED complete (the raw record is
/// present and every field accounted for), so the last NOT-READ reason
/// on the strict-gated fields is gone; only the allowlisted
/// `agy_source_truncated` remains on the text scopes. Strict search may
/// evaluate such a session.
fn antigravity_raw_verified_manifest() -> Value {
    json!({
        "extractor": "antigravity-facts-v5",
        "extractor_version": 9,
        "provider": "antigravity",
        "fields": {
            "chat": {"state":"partial","reasons":["agy_source_truncated"]},
            "tools": {"state":"partial","reasons":[
                "agy_tool_call_identity_missing",
                "agy_tool_result_identity_missing",
                "agy_source_truncated"
            ]},
            "thinking": {"state":"partial","reasons":["agy_source_truncated"]},
            "tool_exit": {"state":"partial","reasons":["agy_status_unpaired"]},
            "tool_error": {"state":"partial","reasons":["agy_status_unpaired"]},
            "block_inventory": {"state":"complete","reasons":[]},
            "text_inventory": {"state":"partial","reasons":["agy_source_truncated"]},
            "files": {"state":"partial","reasons":["agy_source_truncated"]},
            "mcp": {"state":"complete","reasons":[]},
            "title": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "artifacts": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "subagents": {"state":"unavailable","reasons":["overlay_not_supplied"]},
            "is_subagent": {"state":"complete","reasons":[]},
            "parent_relation": {"state":"complete","reasons":[]},
            "metrics": {"state":"partial","reasons":["native_metrics_partial"]},
            "compaction": {"state":"unavailable","reasons":["boundary_records_not_normalized"]}
        },
        "counts": {"tool_calls": 0, "paired_calls": 0}
    })
}

/// (viii-b) The raw-verified shape is what the F15 extractor now
/// publishes for the dominant archive session: the block inventory is
/// complete, and the only text-scope gap is the allowlisted
/// source-truncation marker — nothing that can hide a matching region —
/// so strict search must NOT refuse the session. This pins the
/// end-to-end outcome of the raw-record verification: with
/// `agy_canonical_envelope` retired, the strict gate evaluates the
/// session instead of degrading to `data_unavailable`.
#[test]
fn search_sessions_agy_raw_verified_no_longer_refuses_strict() -> Result<()> {
    let mut bridge = gold_harness()?;
    set_facts_coverage(&bridge, 5, "partial", antigravity_raw_verified_manifest())?;

    // The probe term is absent from S5's verified regions, so S5 is a
    // proven non-member — but the search must RUN: no
    // `data_unavailable` refusal from an inventory gap.
    let page = search(&mut bridge, json!({"query":"foo"}))?;
    ensure!(
        page["ok"] == true,
        "the raw-verified session must not refuse strict search: {page}"
    );
    bridge.shutdown()
}

/// The fixture session for (ix): the real F10 shape the v3 report
/// quantified as the archive's only not-read gap — a tool-step GENERIC
/// record whose `error` passthrough failure text differs from the step
/// narration, plus a system ERROR_MESSAGE announcing a different
/// failure. The needles "gyrocopter" and "wobbledrive" appear ONLY in
/// the error fields.
fn antigravity_error_field_session() -> GoldSession {
    GoldSession {
        id: 8,
        agent: "antigravity",
        source_id: "local",
        external_id: "s-8-agy-error",
        source_path: "/sessions/s-8-agy-error.jsonl",
        workspace: None,
        title: "agy error passthrough",
        started_at: Some(6_000),
        ended_at: Some(9_500),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        messages: vec![
            (0, MessageRole::User, "assemble the widget", None),
            (
                1,
                MessageRole::Tool,
                "step narration content",
                Some(json!({"agy_type":"GENERIC","status":"ERROR",
                    "error":"step gyrocopter failure detail"})),
            ),
            (
                2,
                MessageRole::System,
                "assembly failure summary",
                Some(json!({"agy_type":"ERROR_MESSAGE","agy_source":"SYSTEM",
                    "status":"ERROR",
                    "error":"pipeline wobbledrive failure detail"})),
            ),
        ],
    }
}

/// (ix) Owner follow-up (2026-11-08): the F10 `error` passthrough text is
/// local and readable, so it INDEXES as a search region instead of
/// keeping the `agy_error_field_not_indexed` not-read gap the v3 report
/// quantified. A strict search for a word that appears only in the error
/// text finds the session — the verified hit dominates the session's
/// structural (envelope/identity) marks — and the session's facts carry
/// no text gap from the error field. The facts row below is the REAL
/// extractor manifest (the same generation path ingest runs), not a
/// hand-written one, so the strict lane evaluates exactly what a real
/// ingest would publish.
#[test]
fn search_sessions_agy_error_field_text_answers_strict_search() -> Result<()> {
    let mut sessions = gold_fixture();
    sessions.push(antigravity_error_field_session());
    let mut bridge = gold_harness_full(&sessions, &[])?;

    // Replace the seeded fixture-complete row with the real manifest.
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    let agy = sessions.pop().expect("the antigravity session");
    let generation = coding_agent_search::viewer_extensions::projection::generation_for_canonical(
        &writer,
        &gold_conversation(&agy),
        agy.id,
    )?;
    drop(writer);
    let facts = generation.facts;
    let mut fields = serde_json::Map::new();
    for entry in &facts.coverage {
        fields.insert(
            entry.field.clone(),
            json!({"state": entry.state, "reasons": entry.reasons}),
        );
    }
    let manifest = json!({
        "extractor": facts.extractor,
        "extractor_version": facts.extractor_version,
        "provider": facts.provider,
        "fields": fields,
        "counts": {
            "tool_calls": facts.tools.len(),
            "paired_calls": facts.tools.iter().filter(|t| t.has_result).count(),
        }
    });
    set_facts_coverage(&bridge, agy.id, facts.coverage_state.as_str(), manifest)?;

    // A word that appears ONLY in the tool-step error text: the strict
    // page answers exactly [8] — the error region is a verified hit.
    let page = search(&mut bridge, json!({"query": "gyrocopter"}))?;
    assert_members(&page, &[8])?;
    // The tools scope and the recorded error flags address the failure
    // text as tool error text, like every provider's erroring result.
    let page = search(&mut bridge, json!({"query": "in:tools gyrocopter"}))?;
    assert_members(&page, &[8])?;
    let page = search(&mut bridge, json!({"query": "error:tool gyrocopter"}))?;
    assert_members(&page, &[8])?;
    // The system ERROR_MESSAGE's differing error text is chat-scope text.
    let page = search(&mut bridge, json!({"query": "in:chat wobbledrive"}))?;
    assert_members(&page, &[8])?;

    // No source-absent gap is involved: the found session carries no
    // annotation (its residual marks are structural, not text gaps).
    let s8 = result_item(&page, 8)?;
    ensure!(
        s8["source_text_absent"] == json!([]),
        "no source-absent annotation for an indexed-error session: {s8}"
    );
    ensure!(
        page["result"]["source_absent_sessions"] == 0,
        "no source-absent count: {page}"
    );

    // Coverage has no not-read gap from the error field: the real
    // manifest carries no reason for it anywhere.
    ensure!(
        !facts.coverage.iter().any(|entry| entry
            .reasons
            .iter()
            .any(|r| r == "agy_error_field_not_indexed")),
        "the retired mark must be gone from the real manifest: {:?}",
        facts.coverage
    );
    bridge.shutdown()
}
