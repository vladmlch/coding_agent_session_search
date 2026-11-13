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

/// Codex writes a continuation rollout `rollout-<ts>-<A>_<B>.jsonl` beside
/// the original `rollout-<ts>-<A>.jsonl`; both verify to the one thread
/// identity `codex:<A>`. When both segments match a query, search must
/// emit that logical session once — folded onto the lowest matched
/// canonical id — never two items carrying the same public id.
#[test]
fn search_sessions_groups_codex_continuation_rollouts_to_one_public_id() -> Result<()> {
    let fixture = vec![
        GoldSession {
            id: 1,
            agent: "codex",
            source_id: "local",
            external_id: "2024/05/06/rollout-2024-05-06T10-00-00-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            source_path: "/home/u/.codex/sessions/2024/05/06/rollout-2024-05-06T10-00-00-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.jsonl",
            workspace: Some("/data/projects/alpha"),
            title: "codex thread",
            started_at: Some(1_000),
            ended_at: Some(2_000),
            origin_host: None,
            metadata: json!({"cass":{"origin":{"kind":"local"}}}),
            messages: vec![(0, MessageRole::User, "fluxcap original turn", None)],
        },
        GoldSession {
            id: 2,
            agent: "codex",
            source_id: "local",
            external_id: "2024/05/06/rollout-2024-05-06T11-00-00-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_99999999-8888-7777-6666-555555555555",
            source_path: "/home/u/.codex/sessions/2024/05/06/rollout-2024-05-06T11-00-00-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_99999999-8888-7777-6666-555555555555.jsonl",
            workspace: Some("/data/projects/alpha"),
            title: "codex thread continued",
            started_at: Some(2_000),
            ended_at: Some(3_000),
            origin_host: None,
            metadata: json!({"cass":{"origin":{"kind":"local"}}}),
            messages: vec![(0, MessageRole::User, "fluxcap continuation turn", None)],
        },
    ];
    let mut bridge = gold_harness_full(&fixture, &[])?;
    let page = search(&mut bridge, json!({"query":"fluxcap","limit":10}))?;
    ensure!(page["ok"] == true, "search refused: {page}");
    let items = page["result"]["items"].as_array().expect("items array");
    let public: Vec<&str> = items
        .iter()
        .map(|item| item["public_id"].as_str().expect("public_id"))
        .collect();
    let mut unique = public.clone();
    unique.sort_unstable();
    unique.dedup();
    ensure!(
        unique.len() == public.len(),
        "search repeated a public id across the page: {public:?}"
    );
    ensure!(
        public
            .iter()
            .filter(|p| **p == "codex:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
            .count()
            == 1,
        "the codex thread must appear exactly once: {public:?}"
    );
    ensure!(
        ids(&page) == [1],
        "continuation folds onto the lowest matched canonical id: {:?}",
        ids(&page)
    );
    ensure!(
        page["result"]["total_sessions"] == 1,
        "total_sessions counts logical sessions, not rollout rows: {page}"
    );
    bridge.shutdown()
}

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
        // 2026-11-11 (B): the page load is gated by foo's raw Quill slots —
        // exactly S1+S2's two chat regions — while hydration counts text
        // loads (verify + snippet per witness: 4), which legitimately
        // exceeds the row count: rows count reads of the persisted
        // inventory, hydration counts text reads over those rows. The
        // corpus-scan regression guards stay the bounds above.
        diag["region_rows_loaded"].as_u64() == Some(2),
        "the gated page load reads exactly foo's two chat regions: {diag}"
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

/// Unicode + deep coordinates + the returned-text byte bound. S8
/// parks a Cyrillic marker at message idx 1100 (deep-link coordinate);
/// S9 holds a >64 KiB chat body whose terms straddle the old hydration
/// bound — in-window terms verify from the bounded preview, and hits
/// past the window verify over the WHOLE body (strict members, never a
/// silent false negative). The 64 KiB cap survives only on text
/// RETURNED to the client; the multibyte 'ж' straddling the bound must
/// never panic the lane.
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

    // In-window term verifies from the bounded preview.
    let page = search(&mut bridge, json!({"query":"boundaryterm"}))?;
    assert_members(&page, &[9])?;

    // `beyondterm` is indexed in the canonical document and lies past the
    // 64 KiB window: verification escalates to the whole body, finds the
    // hit and answers STRICTLY — an exact member, not the old
    // bounded-prefix honest-unknown refusal.
    let page = search(&mut bridge, json!({"query":"beyondterm"}))?;
    assert_members(&page, &[9])?;

    // The boundary-safe truncation did not panic the lane — the bridge is
    // still serving (the searches above already proved it).
    bridge.shutdown()
}

/// A gated index hit past the 64 KiB window is an EXACT strict member:
/// verification sees the whole canonical body, so the hit reproduces
/// wherever it lives. The 64 KiB cap survives only on text RETURNED to
/// the client (snippets/payload); the bounded window stays the first
/// chance because the common in-window hit costs no escalation. The
/// unreproducible-hit arm (`quill_hit_not_reproduced`) remains in the
/// evaluator for genuine drift — an index hit the archived text cannot
/// reproduce at all — but the past-bound placement that used to reach it
/// is fixed here.
#[test]
fn search_sessions_past_bound_hit_is_a_strict_member() -> Result<()> {
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

    // Strict: the past-bound hit verifies over the whole body — an exact
    // member, answered without allow_partial.
    let page = search(&mut bridge, json!({"query":"farbeyondterm"}))?;
    assert_members(&page, &[8])?;
    // The witness deep-links the canonical message, and the snippet is
    // the bounded RETURNED window: verification saw the whole body, but
    // the payload never carries more than the 64 KiB cap.
    let matches = page["result"]["matches"]["8"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    ensure!(
        matches.len() == 1,
        "the member carries exactly one witness: {page}"
    );
    ensure!(
        matches[0]["message_idx"].as_i64() == Some(0),
        "the witness anchors message idx 0: {page}"
    );
    let snippet = matches[0]["snippet"].as_str().unwrap_or_default();
    ensure!(
        !snippet.contains("farbeyondterm"),
        "the returned snippet is the bounded window, not the whole body: {page}"
    );

    // allow_partial answers identically — the session is now KNOWN, so
    // the page is complete with no membership warning.
    let page = search(
        &mut bridge,
        json!({"query":"farbeyondterm", "allow_partial": true}),
    )?;
    assert_members(&page, &[8])?;
    ensure!(
        page["result"]["complete"] == true,
        "a verified past-bound member leaves the page complete: {page}"
    );
    bridge.shutdown()
}

/// Whole-body verification shapes: every hit the 64 KiB window cannot see
/// still decides EXACTLY. The window's edge cuts the first marker token
/// mid-token (`o` of `omega` at byte 65,535) — the truncated window token
/// must neither satisfy the query nor crash the matcher, while the whole
/// body finds the phrase, the bare term, the ungated non-ASCII term past
/// the window and the term at the very END of the body. An absent term
/// over the same huge body is a DEFINITE non-member (strict, no Unknown)
/// — for a gated literal (the index proves no hit) and for an ungated
/// non-ASCII literal (the whole body was scanned and the term is absent).
#[test]
fn search_sessions_past_bound_shapes_verify_over_the_whole_body() -> Result<()> {
    let mut fixture = gold_fixture();
    // 16,383 * "pad " = 65,532 B, then "xy " lands the separator at byte
    // 65,534 and `omega` starts AT byte 65,535 — inside the 64 KiB
    // window ([0, 65_536)) by exactly one byte, so the window holds the
    // truncated token "o" while the full body holds "omega".
    let mut body = String::with_capacity(70_000);
    for _ in 0..16_383 {
        body.push_str("pad ");
    }
    body.push_str("xy omega zero endpoint past the window");
    body.push_str(" middlefiller");
    // Ungated-lane marker (non-ASCII) past the window.
    body.push_str(" хвостовой");
    // …and a term at the very END of the body.
    body.push_str(" trailingfiller finaltail");
    let body: &'static str = Box::leak(body.into_boxed_str());
    fixture.push(GoldSession {
        id: 8,
        agent: "claude_code",
        source_id: "local",
        external_id: "s-8-shapes",
        source_path: "/sessions/s-8-shapes.jsonl",
        workspace: None,
        title: "past-bound shapes session",
        started_at: Some(6_200),
        ended_at: Some(9_300),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        messages: vec![(0, MessageRole::User, body, None)],
    });
    let mut bridge = gold_harness_full(&fixture, &[])?;

    // Exact phrase past the window, its first token cut mid-token by the
    // window edge — the truncated "o" must not satisfy the phrase.
    let page = search(&mut bridge, json!({"query":"\"omega zero endpoint\""}))?;
    assert_members(&page, &[8])?;

    // Bare term whose token the window cut to "o" — the whole body finds
    // the full token.
    let page = search(&mut bridge, json!({"query":"omega"}))?;
    assert_members(&page, &[8])?;

    // Non-ASCII term past the window — the ungated lane scans the whole
    // body and finds it (an exact member, not honest-unknown).
    let page = search(&mut bridge, json!({"query":"хвостовой"}))?;
    assert_members(&page, &[8])?;

    // A term at the very END of the body — no trailing context at all.
    let page = search(&mut bridge, json!({"query":"finaltail"}))?;
    assert_members(&page, &[8])?;

    // Absent term over the huge body: gated (the index proves no hit) —
    // a DEFINITE non-member, strict page answering OK with no members.
    let page = search(&mut bridge, json!({"query":"absentmarker"}))?;
    assert_members(&page, &[])?;
    ensure!(
        page["result"]["complete"] == true,
        "a proven absence leaves the strict page complete: {page}"
    );

    // Absent non-ASCII term: the whole body was scanned (definite
    // absence), so the page is again an exact empty answer — never an
    // Unknown refusal.
    let page = search(&mut bridge, json!({"query":"отсутствует"}))?;
    assert_members(&page, &[])?;
    ensure!(
        page["result"]["complete"] == true,
        "a scanned-and-absent term is a definite non-member: {page}"
    );
    bridge.shutdown()
}

/// The verifier must read region text with the INDEX ANALYZER's token
/// boundaries, or an index-proven hit cannot reproduce even over the
/// whole body. quill indexes `title`/`content` with the CASS hyphen
/// pipeline (ASCII-alphanumeric runs; `_` and `$` are separators;
/// hyphen-joined runs additionally emit their parts), so `readme` is a
/// real TOKEN of `G5_MODE_SEARCH_README.md` to the index — while the old
/// `\w\-$`-joining verifier saw one `g5_mode_search_readme` token and
/// degraded the session to `quill_hit_not_reproduced`. 27 sessions
/// exactly like this kept strict `README` search refusing on the owner
/// archive even after whole-body hydration landed. The literal tokenizer
/// now splits on every non-alphanumeric scalar, mirroring the analyzer
/// (and its edge-prefix fields, which split the same way), so every
/// gated hit on `_`/`$`-glued or hyphen-joined text reproduces.
#[test]
fn search_sessions_verifier_reads_index_analyzer_token_boundaries() -> Result<()> {
    let mut fixture = gold_fixture();
    // The real-archive shape: a directory-listing message whose marker
    // terms are glued by `_`, `-` and `$`.
    let body: &'static str =
        "listing of G5_MODE_SEARCH_README.md MUJOCO_LOG.TXT then mcp-server hookup cost$total";
    fixture.push(GoldSession {
        id: 8,
        agent: "claude_code",
        source_id: "local",
        external_id: "s-8-analyzer",
        source_path: "/sessions/s-8-analyzer.jsonl",
        workspace: None,
        title: "analyzer boundary session",
        started_at: Some(6_200),
        ended_at: Some(9_300),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        messages: vec![(0, MessageRole::User, body, None)],
    });
    let mut bridge = gold_harness_full(&fixture, &[])?;

    // `_`-glued text: the index tokenizes G5_MODE_SEARCH_README.md into
    // [g5, mode, search, readme, md], so bare `readme` is an
    // index-proven hit the verifier must reproduce.
    let page = search(&mut bridge, json!({"query":"readme"}))?;
    assert_members(&page, &[8])?;

    // Exact phrase across the `_` join: [mode, search] are adjacent
    // analyzer tokens.
    let page = search(&mut bridge, json!({"query":"\"mode search\""}))?;
    assert_members(&page, &[8])?;

    // Hyphen-joined text: the analyzer emits the parts of `mcp-server`,
    // so bare `server` is an index-proven hit — in the glued message AND
    // in S1's `meta:tags` region (the fixture's `mcp-server` tag). The
    // old `\w\-$`-joining verifier saw one `mcp-server` token in both
    // places and degraded BOTH sessions to `quill_hit_not_reproduced`
    // for `server`; the aligned verifier reproduces the index on both.
    let page = search(&mut bridge, json!({"query":"server"}))?;
    assert_members(&page, &[1, 8])?;
    let s1_witness = page["result"]["matches"]["1"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    ensure!(
        s1_witness.iter().any(|m| m["block_key"] == "meta:tags"),
        "S1's `server` witness anchors its mcp-server tag region: {page}"
    );

    // `$`-glued text splits the same way.
    let page = search(&mut bridge, json!({"query":"total"}))?;
    assert_members(&page, &[8])?;

    // A hyphen-bearing QUERY gates on the analyzer's joined token
    // (`g5-mode` — the run the CASS tokenizer keeps), and `_`-glued text
    // carries only the PARTS — so the index proves NO hit anywhere: a
    // definite, complete absence, the same verdict as `absentmarker`.
    // (The verifier's [g5, mode] reading is broader than this gate but
    // never diverges: every doc the gate CAN prove carries the joined
    // run, hence both parts, and verifies as a member.)
    let page = search(&mut bridge, json!({"query":"g5-mode"}))?;
    assert_members(&page, &[])?;
    ensure!(
        page["result"]["complete"] == true,
        "a joiner-bearing query with no joined token anywhere is a proven absence: {page}"
    );

    // The spaced counterpart: two ANDed part terms gate on the indexed
    // parts, so `_`-glued text IS a candidate — and a member.
    let page = search(&mut bridge, json!({"query":"g5 mode"}))?;
    assert_members(&page, &[8])?;

    // A pure-joiner ASCII literal sanitizes to an empty lexical query,
    // and the engine REFUSES the whole op with a typed `invalid_request`
    // — pre-existing gate behavior, unchanged by the alignment: the
    // no-silent-substitution contract, not an empty lie. The verifier's
    // never-matcher serves joiner-only literals that SKIP the gate
    // (non-ASCII — the analyzer emits no tokens for them), pinned next.
    let page = search(&mut bridge, json!({"query":"___"}))?;
    ensure!(
        page["ok"] == false && page["error"]["kind"] == "invalid_request",
        "a pure-joiner ASCII literal refuses with a typed error: {page}"
    );

    // A joiner-only non-ASCII literal skips the gate and evaluates
    // through the never-matcher: a complete, definite absence.
    let page = search(&mut bridge, json!({"query":"。。。"}))?;
    assert_members(&page, &[])?;
    ensure!(
        page["result"]["complete"] == true,
        "a joiner-only non-ASCII literal is a proven absence: {page}"
    );

    // The never-matcher arm above needs no scan to find nothing, and the
    // other arms come from reproduced gated hits: a member witness
    // anchors the glued message.
    let page = search(&mut bridge, json!({"query":"readme"}))?;
    let matches = page["result"]["matches"]["8"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    ensure!(
        matches.len() == 1 && matches[0]["message_idx"].as_i64() == Some(0),
        "the glued-text member carries one witness on message 0: {page}"
    );
    bridge.shutdown()
}

/// The whole-body verification budget refuses with a TYPED reason, never
/// a silent cap. Past the request's byte ceiling the affected sessions
/// degrade to honest uncertainty naming
/// `verification_bytes_budget_exceeded` — gated candidates through
/// `text_inventory:<state>:…` (the index-proven hit stays possible
/// membership), ungated evaluation through `literal:…` — and
/// `allow_partial` still serves the verified membership with the
/// `membership_incomplete` warning.
#[test]
fn search_sessions_verification_budget_refusal_is_typed() -> Result<()> {
    let mut fixture = gold_fixture();
    // A body far past the 64 KiB window whose marker only the whole-body
    // escalation can verify.
    let long_body = format!("{} budgetmarker past the window", "b".repeat(100_000));
    let long_body: &'static str = Box::leak(long_body.into_boxed_str());
    fixture.push(GoldSession {
        id: 8,
        agent: "claude_code",
        source_id: "local",
        external_id: "s-8-budget",
        source_path: "/sessions/s-8-budget.jsonl",
        workspace: None,
        title: "budget ceiling session",
        started_at: Some(6_200),
        ended_at: Some(9_300),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        messages: vec![(0, MessageRole::User, long_body, None)],
    });
    // Budget 1 byte: the first whole-body scan overdrafts by design (the
    // ceiling gates every scan AFTER the running total reaches it), so
    // every later region — session 8's gated escalation included —
    // refuses with the typed reason.
    let mut bridge = gold_harness_full(&fixture, &["--text-verification-budget-bytes", "1"])?;

    // Ungated (non-ASCII) evaluation: budget exhaustion is honest
    // `unseen` — the strict page refuses naming the typed reason.
    let page = search(&mut bridge, json!({"query":"маркер"}))?;
    ensure!(
        page["ok"] == false,
        "budget exhaustion must refuse strict search: {page}"
    );
    ensure!(
        page["error"]["kind"] == "data_unavailable",
        "budget exhaustion is honest-unavailable, never pending: {page}"
    );
    let message = page["error"]["message"].as_str().unwrap_or_default();
    ensure!(
        message.contains("literal:verification_bytes_budget_exceeded"),
        "the ungated refusal must name the typed budget reason: {page}"
    );

    // Gated escalation past the budget: the index-proven hit is possible
    // membership whose whole-body verification the budget refused — the
    // strict page names the inventory-scoped reason.
    let page = search(
        &mut bridge,
        json!({"ast":{"type":"or","children":[
        {"type":"term","value":"кириллица"},
        {"type":"term","value":"budgetmarker"}]}}),
    )?;
    ensure!(
        page["ok"] == false,
        "a gated hit the budget cannot verify must refuse: {page}"
    );
    let message = page["error"]["message"].as_str().unwrap_or_default();
    ensure!(
        message.contains("text_inventory:complete:verification_bytes_budget_exceeded"),
        "the gated refusal must name the inventory-scoped budget reason: {page}"
    );

    // allow_partial serves the verified membership: nothing verified to
    // a member, the page is incomplete with the honest warning.
    let page = search(
        &mut bridge,
        json!({"query":"маркер", "allow_partial": true}),
    )?;
    ensure!(page["ok"] == true, "partial opt-in must serve: {page}");
    ensure!(
        page["result"]["complete"] == false,
        "the budget-refused sessions keep the page incomplete: {page}"
    );
    ensure!(
        ids(&page).is_empty(),
        "no session verifies the marker: {page}"
    );
    let warnings = page["result"]["warnings"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    ensure!(
        warnings.iter().any(|w| w == "membership_incomplete"),
        "the budget-refused sessions must keep their warning: {page}"
    );
    bridge.shutdown()
}

/// Review I-1: fast-lane witness discovery that exhausts the request's
/// whole-body verification budget refuses the whole page with the typed
/// `verification_budget_exceeded` kind (design §7). Budget exhaustion is
/// honest unavailability — never §6.4 analyzer drift, and never a
/// doc-claimed member emitted with `complete:true` under the drift
/// warning.
///
/// 2026-11-11 (B) rebase: the page's witness discovery is gated by the
/// claim's raw Quill slots, and a gated candidate whose bounded window
/// (`bound_region_text`, 64 KiB) reproduces the marker verifies through
/// the window at ZERO verification bytes — the same proof the legacy
/// gated lane has used since Stage 1b. The original fixture put the
/// marker inside the window, so the gated pass never needed the
/// whole-body scan the budget meters and the page honestly completed.
/// This gate keeps gating exactly what it gated — "a discovery that NEEDS
/// whole-body scans it cannot pay for refuses the page" — by moving both
/// gated regions' markers PAST the window: the shortcut cannot fire, the
/// first region's full scan overdrafts the 1-byte budget, and the SECOND
/// region's verify hits BudgetExceeded mid-discovery. The (B) improvement
/// (a window-proven member needs no whole-body budget) is pinned
/// explicitly on session 9 below — the pre-(B) discovery refused that
/// page, the gated pass proves both witnesses through the window.
#[test]
fn search_sessions_fast_lane_budget_exhaustion_is_typed_refusal() -> Result<()> {
    let mut fixture = gold_fixture();
    // ~70 KB of filler: the marker lands past the 64 KiB bounded window,
    // so no bounded read can prove it — only the whole-body scan can.
    let past_window_body: &'static str = Box::leak(
        format!(
            "{} budgetmarker9 past-window body",
            "filler ".repeat(10_000)
        )
        .into_boxed_str(),
    );
    fixture.push(GoldSession {
        id: 8,
        agent: "claude_code",
        source_id: "local",
        external_id: "s-8-fast-budget",
        source_path: "/sessions/s-8-fast-budget.jsonl",
        workspace: None,
        title: "fast-lane budget session",
        started_at: Some(6_400),
        ended_at: Some(9_400),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        // Two canonical chat regions, both gated (both contents carry the
        // marker, so both region documents hit the query) and both
        // window-UNPROVEN (the marker is past `bound_region_text`'s 64
        // KiB): discovery must whole-body-scan the first — overdrafting
        // the budget by design (the ceiling gates every scan AFTER the
        // running total reaches it) — and the SECOND region's verify
        // then hits BudgetExceeded mid-discovery.
        messages: vec![
            (0, MessageRole::User, past_window_body, None),
            (1, MessageRole::Agent, past_window_body, None),
        ],
    });
    fixture.push(GoldSession {
        id: 9,
        agent: "claude_code",
        source_id: "local",
        external_id: "s-9-window-budget",
        source_path: "/sessions/s-9-window-budget.jsonl",
        workspace: None,
        title: "window-proven budget session",
        started_at: Some(10_000),
        ended_at: Some(13_000),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        // Both regions carry the marker INSIDE the bounded window: the
        // gated pass proves both witnesses through the window (0
        // verification bytes). The pre-(B) ungated discovery whole-body
        // scanned the first region (26 bytes over the 1-byte budget) and
        // refused the page on the second — the delta this fixture pins.
        messages: vec![
            (
                0,
                MessageRole::User,
                "windowmarker7 early window body",
                None,
            ),
            (
                1,
                MessageRole::Agent,
                "windowmarker7 second early window body",
                None,
            ),
        ],
    });
    // Budget 1 byte: discovery's first whole-body scan overdrafts by
    // design, so the SECOND region's verify hits BudgetExceeded
    // mid-discovery.
    let mut bridge = gold_harness_full(&fixture, &["--text-verification-budget-bytes", "1"])?;
    let page = search(&mut bridge, json!({"query":"budgetmarker9"}))?;
    ensure!(
        page["ok"] == false,
        "fast-lane budget exhaustion must refuse the page, not emit a member: {page}"
    );
    ensure!(
        page["error"]["kind"] == "verification_budget_exceeded",
        "the refusal must carry the typed §7 kind: {page}"
    );
    let message = page["error"]["message"].as_str().unwrap_or_default();
    ensure!(
        message.contains("verification_bytes_budget_exceeded"),
        "the refusal must name the same typed reason as the legacy lane: {page}"
    );
    // The (B) improvement, pinned on the same 1-byte bridge: a member the
    // bounded window proves needs no whole-body budget.
    let page = search(&mut bridge, json!({"query":"windowmarker7"}))?;
    ensure!(
        page["ok"] == true,
        "the window-proven member must not need the whole-body budget: {page}"
    );
    assert_members(&page, &[9])?;
    ensure!(
        page["result"]["complete"] == true,
        "the window-proven page stays complete: {page}"
    );
    let window_matches = page["result"]["matches"]["9"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    ensure!(
        window_matches.len() == 2
            && window_matches[0]["message_idx"].as_i64() == Some(0)
            && window_matches[1]["message_idx"].as_i64() == Some(1),
        "both regions are window-proven witnesses: {page}"
    );
    ensure!(
        page["result"]["diagnostics"]["verification_bytes_scanned"].as_u64() == Some(0),
        "the bounded-window shortcut spends zero verification bytes: {page}"
    );
    bridge.shutdown()?;

    // Control, default budget: the same query answers with the verified
    // witnesses — proving the refusal above is budget-driven, not a
    // fixture artifact, and that discovery normally reproduces the
    // claim without the drift annotation. Both of S8's regions carry the
    // past-window marker, so the whole-body scans verify BOTH witnesses
    // within the default budget.
    let mut bridge = gold_harness_full(&fixture, &[])?;
    let page = search(&mut bridge, json!({"query":"budgetmarker9"}))?;
    ensure!(page["ok"] == true, "the default budget must answer: {page}");
    assert_members(&page, &[8])?;
    ensure!(
        page["result"]["complete"] == true,
        "within the budget the page stays complete: {page}"
    );
    let matches = page["result"]["matches"]["8"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    ensure!(
        matches.len() == 2
            && matches[0]["message_idx"].as_i64() == Some(0)
            && matches[1]["message_idx"].as_i64() == Some(1),
        "both past-window regions are the whole-body-verified witnesses: {page}"
    );
    let item = page["result"]["items"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .find(|it| it["conversation_id"].as_i64() == Some(8))
        .unwrap_or(Value::Null);
    ensure!(
        item["witness_count"].as_i64() == Some(2),
        "the witness count mirrors the verified matches: {page}"
    );
    let warnings = page["result"]["warnings"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    ensure!(
        !warnings.iter().any(|w| w == "witnesses_not_reproduced"),
        "no drift warning when discovery reproduces the witness: {page}"
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

/// Review I-3(b): the conservative stale queue RETIRES a session's old
/// chunks when the session shrinks. Three 30k-char messages pack two chat
/// chunks (messages 0+1, then 2 alone, marker at message 2's start so
/// chunk 1's prefix window claims it); a reindex that drops two messages
/// leaves ONE chat chunk. The republish must queue the retired chunk's
/// document identity — plus the phantom slot the old per-scope region
/// count conservatively covers — the commit must actually delete the
/// retired chunk from the engine, and the post-stamp fast lane must stop
/// claiming the retired marker while claiming the new body's term.
#[test]
fn search_regions_stale_queue_retires_shrunk_session_chunks() -> Result<()> {
    use coding_agent_search::viewer_extensions::search_regions::{
        REGION_DOC_BASE, SESSION_DOC_BASE, SESSION_DOC_SCOPE_SHIFT, session_scope_docs,
        session_scope_ordinal, stage_region_docs, stamp_region_docs_published,
    };
    // The archive outlives both bridges: each harness owns only a
    // throwaway tempdir, the real data dir rides in `archive`.
    let archive = tempfile::tempdir()?;
    let data_dir = archive.path().join("data");
    std::fs::create_dir(&data_dir)?;
    let db = data_dir.join("archive.db");

    let long_a: &'static str = Box::leak("a".repeat(30_000).into_boxed_str());
    let long_b: &'static str = Box::leak("b".repeat(30_000).into_boxed_str());
    let long_c: &'static str =
        Box::leak(format!("retiredchunkmarker {}", "c".repeat(30_000)).into_boxed_str());
    let mut fixture = gold_fixture();
    fixture.push(GoldSession {
        id: 8,
        agent: "claude_code",
        source_id: "local",
        external_id: "s-8-shrink",
        source_path: "/sessions/s-8-shrink.jsonl",
        workspace: None,
        title: "chunk shrink session",
        started_at: Some(6_500),
        ended_at: Some(9_500),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        messages: vec![
            (0, MessageRole::User, long_a, None),
            (1, MessageRole::User, long_b, None),
            (2, MessageRole::User, long_c, None),
        ],
    });
    let writer = gold_seed(&fixture, &db)?;

    // Generation 1 — gold_harness_full's phased body: canonical docs,
    // every session's region docs staged, committed, stamped.
    let index_path = expected_index_dir(&data_dir);
    {
        let mut docs = Vec::new();
        for session in &fixture {
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
                    origin_kind: "local".to_owned(),
                    origin_host: session.origin_host.map(str::to_owned),
                    conversation_id: Some(session.id),
                });
            }
        }
        let mut index = QuillCassIndex::open_or_create(&index_path)?;
        index.add_cass_documents(&docs)?;
        let mut stamps = Vec::new();
        for session in &fixture {
            let (_docs, stamp) = stage_region_docs(&mut index, &writer, session.id)?;
            if let Some(stamp) = stamp {
                stamps.push(stamp);
            }
        }
        index.commit()?;
        stamp_region_docs_published(&writer, &stamps)?;
        std::fs::write(
            index_path.join("schema_hash.json"),
            serde_json::to_vec(&json!({
                "schema_hash": coding_agent_search::search::tantivy::SCHEMA_HASH,
            }))?,
        )?;
    }

    // Gen-1 shape: the chat scope packs TWO chunks — messages 0+1 then
    // message 2 alone — and the marker rides chunk 1's prefix window.
    let chat_ordinal = session_scope_ordinal("chat");
    let chat_slot = |seq: u64| SESSION_DOC_BASE + (chat_ordinal << SESSION_DOC_SCOPE_SHIFT) + seq;
    let docs = session_scope_docs(&writer, 8)?.expect("session 8 owns session docs");
    let doc_slots = || -> Vec<u64> {
        let mut slots: Vec<u64> = docs.iter().map(|doc| doc.slot).collect();
        slots.sort_unstable();
        slots
    };
    // The title scope owns its own single chunk (SESSION_DOC_BASE + 0)
    // alongside the chat scope's two.
    ensure!(
        doc_slots() == vec![SESSION_DOC_BASE, chat_slot(0), chat_slot(1)],
        "90k chat chars pack two chat chunks plus the title doc: {:?}",
        doc_slots()
    );
    let chat_chunk1 = docs
        .iter()
        .find(|doc| doc.slot == chat_slot(1))
        .expect("chat chunk 1");
    ensure!(
        chat_chunk1.text.starts_with("retiredchunkmarker"),
        "the marker rides chunk 1: {:?}",
        chat_chunk1.text.chars().take(40).collect::<String>()
    );

    // Bridge #1: the fast lane claims the marker through chunk 1.
    drop(writer);
    let mut bridge =
        BridgeHarness::spawn_fixture(tempfile::tempdir()?, data_dir.clone(), db.clone(), &[])?;
    let page = search(&mut bridge, json!({"query":"retiredchunkmarker"}))?;
    assert_members(&page, &[8])?;
    bridge.shutdown()?;

    // The shrink: two messages dropped, the survivor's body replaced —
    // the shape a reindex of a truncated session file produces.
    let writer = Connection::open(db.to_string_lossy().into_owned())?;
    writer.execute("DELETE FROM messages WHERE conversation_id = 8 AND idx > 0")?;
    writer.execute_with_params(
        "UPDATE messages SET content = ?1 WHERE conversation_id = 8 AND idx = 0",
        &param_slice_to_values(params!("newbodyterm shrunk single-chunk body")),
    )?;
    let shrunk = GoldSession {
        id: 8,
        agent: "claude_code",
        source_id: "local",
        external_id: "s-8-shrink",
        source_path: "/sessions/s-8-shrink.jsonl",
        workspace: None,
        title: "chunk shrink session",
        started_at: Some(6_500),
        ended_at: Some(9_500),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        messages: vec![(
            0,
            MessageRole::User,
            "newbodyterm shrunk single-chunk body",
            None,
        )],
    };
    let conv = gold_conversation(&shrunk);
    let generation = coding_agent_search::viewer_extensions::projection::generation_for_canonical(
        &writer, &conv, 8,
    )?;
    coding_agent_search::viewer_extensions::search_regions::publish_search_regions(
        &writer,
        8,
        &generation.packet,
        &generation.structure,
    )?;

    // The queue pins the whole retire set:
    // - chat seq 1: the RETIRED chunk-1 document (the old live chunk),
    // - chat seq 2: the phantom slot the old 3-region chat count
    //   conservatively covers (a third chunk never existed),
    // - REGION_DOC_BASE+3: the old meta region row's document — the old
    //   inventory numbered it at ordinal 3 (after three chat regions);
    //   the shrunk inventory renumbers it to ordinal 1, so the pre-Stage-1
    //   region-doc diff queues the old slot.
    // The surviving chat chunk 0 is reclaimed, never queued.
    let row = writer.query_row(
        "SELECT docs_state, pending_doc_deletes FROM viewer_search_region_marks \
         WHERE conversation_id = 8",
    )?;
    let state = row.get_typed::<String>(0)?;
    let pending_raw = row.get_typed::<String>(1)?;
    ensure!(
        state == "pending",
        "the republish re-pends the marks row: {state}"
    );
    let doc_identity = |slot: u64| {
        frankensearch::quill::cass::cass_document_identity(
            "local",
            frankensearch::quill::cass::CassConversationKey {
                source_path: "/sessions/s-8-shrink.jsonl",
                id: Some(8),
            },
            slot,
        )
    };
    let mut queued: Vec<String> = serde_json::from_str(&pending_raw)?;
    let mut expected = vec![
        doc_identity(chat_slot(1)),
        doc_identity(chat_slot(2)),
        doc_identity(REGION_DOC_BASE + 3),
    ];
    queued.sort();
    expected.sort();
    ensure!(
        queued == expected,
        "exactly the retired chunk, its phantom sibling and the renumbered \
         meta region doc are queued: {pending_raw}"
    );

    // The shrunk session packs ONE chat chunk now (plus its title doc).
    let docs = session_scope_docs(&writer, 8)?.expect("session 8 still owns session docs");
    let mut shrunk_slots: Vec<u64> = docs.iter().map(|doc| doc.slot).collect();
    shrunk_slots.sort_unstable();
    ensure!(
        shrunk_slots == vec![SESSION_DOC_BASE, chat_slot(0)],
        "the shrunk session packs one chat chunk plus the title doc: {shrunk_slots:?}"
    );

    // Retire — the production lane from viewer_bridge::Writer::
    // converge_search_region_docs: one converge pass stages every
    // pending session, then the stamped commit executes the queued
    // stale deletes BEFORE flipping the marks row published. Plain
    // commit() seals the replacements but never executes the queue —
    // the retired chunk's posting would stay live while the marks row
    // claimed an empty queue.
    let mut index =
        coding_agent_search::search::tantivy::TantivyIndex::open_or_create(&index_path)?;
    let converged = index.converge_pending_search_region_docs(&writer, i64::MAX)?;
    ensure!(
        converged == 1,
        "only the re-pended session 8 converges: {converged}"
    );
    index.commit_with_region_doc_stamps(&writer)?;
    let row = writer.query_row(
        "SELECT docs_state, pending_doc_deletes FROM viewer_search_region_marks \
         WHERE conversation_id = 8",
    )?;
    ensure!(
        row.get_typed::<String>(0)? == "published" && row.get_typed::<String>(1)? == "[]",
        "the stamp publishes and clears the queue: {row:?}"
    );

    // Bridge #2: the retired chunk no longer claims — the marker's
    // posting set is empty — while the new body's term claims through
    // the repacked single chunk.
    drop(writer);
    let mut bridge =
        BridgeHarness::spawn_fixture(tempfile::tempdir()?, data_dir.clone(), db.clone(), &[])?;
    let page = search(&mut bridge, json!({"query":"retiredchunkmarker"}))?;
    ensure!(
        page["ok"] == true,
        "the retired-marker query must answer: {page}"
    );
    ensure!(
        ids(&page).is_empty(),
        "the retired chunk's document is gone from the posting set: {page}"
    );
    let page = search(&mut bridge, json!({"query":"newbodyterm"}))?;
    assert_members(&page, &[8])?;
    let item = page["result"]["items"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .find(|it| it["conversation_id"].as_i64() == Some(8))
        .unwrap_or(Value::Null);
    ensure!(
        item["witness_count"].as_i64() == Some(1),
        "the new body's witness is discovered on the repacked chunk: {page}"
    );
    bridge.shutdown()
}

/// GH #457 (full-rebuild doc-count refusal), minimized to the queue
/// contract that causes it. The marks queue rides forward across a
/// republish, and the ride-forward filter used to protect only
/// REGION-doc identities: a queued session×scope identity the CURRENT
/// derivation keeps — stale when an earlier epoch queued it, LIVE after
/// the conversation grew — survived the filter, the staged commit
/// executed it against the fresh candidate, and the fresh reader then
/// counted fewer documents than the run had staged (the owner's real
/// archive: 1,085,152 counted, 1,085,145 live, exit 9). The republish
/// must reclaim every live keep identity while still riding genuinely
/// stale entries forward — the ride-forward is the only path a queued
/// delete survives, so over-filtering would strand stale documents.
#[test]
fn pending_queue_riding_forward_never_deletes_a_live_session_doc() -> Result<()> {
    use coding_agent_search::viewer_extensions::search_regions::{
        SESSION_DOC_BASE, SESSION_DOC_SCOPE_SHIFT, session_scope_docs, session_scope_ordinal,
        stage_region_docs, stamp_region_docs_published,
    };

    // Session 8's chat packs TWO chunks; the first carries the probe
    // token. The pre-run archive state is injected verbatim: a pending
    // queue holding a LIVE keep identity plus a phantom beyond the keep.
    let long_z: &'static str =
        Box::leak(format!("ridemarkercero {}", "z".repeat(40_000)).into_boxed_str());
    let long_w: &'static str = Box::leak("w".repeat(40_000).into_boxed_str());
    let mut fixture = gold_fixture();
    fixture.push(GoldSession {
        id: 8,
        agent: "claude_code",
        source_id: "local",
        external_id: "s-8-ride",
        source_path: "/sessions/s-8-ride.jsonl",
        workspace: None,
        title: "gh457 ride forward session",
        started_at: Some(6_500),
        ended_at: Some(9_500),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        messages: vec![
            (0, MessageRole::User, long_z, None),
            (1, MessageRole::User, long_w, None),
        ],
    });

    let archive = tempfile::tempdir()?;
    let data_dir = archive.path().join("data");
    std::fs::create_dir(&data_dir)?;
    let db = data_dir.join("archive.db");
    let writer = gold_seed(&fixture, &db)?;
    let index_path = expected_index_dir(&data_dir);

    // Generation 1 — the real lane's stage → commit → stamp order.
    {
        let mut docs = Vec::new();
        for session in &fixture {
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
                    origin_kind: "local".to_owned(),
                    origin_host: session.origin_host.map(str::to_owned),
                    conversation_id: Some(session.id),
                });
            }
        }
        let mut index = QuillCassIndex::open_or_create(&index_path)?;
        index.add_cass_documents(&docs)?;
        let mut stamps = Vec::new();
        for session in &fixture {
            let (_docs, stamp) = stage_region_docs(&mut index, &writer, session.id)?;
            if let Some(stamp) = stamp {
                stamps.push(stamp);
            }
        }
        index.commit()?;
        stamp_region_docs_published(&writer, &stamps)?;
        std::fs::write(
            index_path.join("schema_hash.json"),
            serde_json::to_vec(&json!({
                "schema_hash": coding_agent_search::search::tantivy::SCHEMA_HASH,
            }))?,
        )?;
    }

    let chat_ordinal = session_scope_ordinal("chat");
    let chat_slot = |seq: u64| SESSION_DOC_BASE + (chat_ordinal << SESSION_DOC_SCOPE_SHIFT) + seq;
    let doc_identity = |slot: u64| {
        frankensearch::quill::cass::cass_document_identity(
            "local",
            frankensearch::quill::cass::CassConversationKey {
                source_path: "/sessions/s-8-ride.jsonl",
                id: Some(8),
            },
            slot,
        )
    };
    let docs = session_scope_docs(&writer, 8)?.expect("session 8 owns session docs");
    let mut live_slots: Vec<u64> = docs.iter().map(|doc| doc.slot).collect();
    live_slots.sort_unstable();
    ensure!(
        live_slots == vec![SESSION_DOC_BASE, chat_slot(0), chat_slot(1)],
        "80k chat chars pack two chat chunks plus the title doc: {live_slots:?}"
    );

    // The pre-run state: an earlier epoch's queue left pending — its
    // chat seq 0 entry was stale against THAT epoch's smaller keep and
    // is LIVE against the current one; chat seq 9 is a phantom.
    let pre_run_queue = serde_json::to_string(&vec![
        doc_identity(chat_slot(0)),
        doc_identity(chat_slot(9)),
    ])?;
    writer.execute_with_params(
        "UPDATE viewer_search_region_marks SET docs_state = 'pending', \
         pending_doc_deletes = ?1 WHERE conversation_id = 8",
        &param_slice_to_values(params![pre_run_queue]),
    )?;

    // The full rebuild's republish: same conversation, same content —
    // exactly what `cass index --full` does for every session.
    let spec = fixture.iter().find(|s| s.id == 8).expect("session 8 spec");
    let conv = gold_conversation(spec);
    let generation = coding_agent_search::viewer_extensions::projection::generation_for_canonical(
        &writer, &conv, 8,
    )?;
    coding_agent_search::viewer_extensions::search_regions::publish_search_regions(
        &writer,
        8,
        &generation.packet,
        &generation.structure,
    )?;

    // The queue contract: no LIVE keep identity may stay queued (the
    // stamped commit would delete it from the fresh candidate — the
    // GH #457 deficit), and the phantom must ride forward untouched.
    let row = writer.query_row(
        "SELECT docs_state, pending_doc_deletes FROM viewer_search_region_marks \
         WHERE conversation_id = 8",
    )?;
    ensure!(
        row.get_typed::<String>(0)? == "pending",
        "the republish re-pends the marks row"
    );
    let pending_raw = row.get_typed::<String>(1)?;
    let mut queued: Vec<String> = serde_json::from_str(&pending_raw)?;
    let keep: Vec<String> = docs.iter().map(|doc| doc_identity(doc.slot)).collect();
    let live_queued: Vec<&String> = queued.iter().filter(|id| keep.contains(id)).collect();
    ensure!(
        live_queued.is_empty(),
        "GH #457: the republish queued {live_queued:?} — identities the \
         current derivation KEEPS; the stamped commit would delete live \
         documents the run already counted: {pending_raw}"
    );
    let mut want = vec![doc_identity(chat_slot(9))];
    queued.sort();
    want.sort();
    ensure!(
        queued == want,
        "only the phantom stale entry may ride forward: {pending_raw}"
    );

    // The production converge lane: one pass stages the re-pended
    // session, then the stamped commit executes the queue against the
    // live archive.
    let mut index =
        coding_agent_search::search::tantivy::TantivyIndex::open_or_create(&index_path)?;
    let converged = index.converge_pending_search_region_docs(&writer, i64::MAX)?;
    ensure!(
        converged == 1,
        "only the re-pended session 8 converges: {converged}"
    );
    index.commit_with_region_doc_stamps(&writer)?;
    let row = writer.query_row(
        "SELECT docs_state, pending_doc_deletes FROM viewer_search_region_marks \
         WHERE conversation_id = 8",
    )?;
    ensure!(
        row.get_typed::<String>(0)? == "published" && row.get_typed::<String>(1)? == "[]",
        "the stamp publishes and clears the queue"
    );

    // The user-visible consequence (GH #457's fresh-reader deficit): the
    // live chunk's posting set must answer after the stamped commit.
    drop(writer);
    let mut bridge =
        BridgeHarness::spawn_fixture(tempfile::tempdir()?, data_dir.clone(), db.clone(), &[])?;
    let page = search(&mut bridge, json!({"query":"ridemarkercero"}))?;
    assert_members(&page, &[8])?;
    ensure!(
        page["result"]["membership_source"] == "session_scope_docs",
        "the live chunk answers on the session-scope-doc lane: {page}"
    );
    bridge.shutdown()
}

/// The meta-replacement lane rides the same marks queue forward
/// (`replace_meta_regions`) and must respect the same contract: a queued
/// LIVE session×scope identity is reclaimed, phantom stale entries ride
/// on. The meta lane rewrites only `meta:*` rows, so the session keep it
/// must honor is the one derived from the surviving rows.
#[test]
fn meta_replacement_riding_forward_never_deletes_a_live_session_doc() -> Result<()> {
    use coding_agent_search::viewer_extensions::search_regions::{
        SESSION_DOC_BASE, SESSION_DOC_SCOPE_SHIFT, replace_meta_regions, session_scope_docs,
        session_scope_ordinal,
    };

    let temp = tempfile::tempdir()?;
    let data_dir = temp.path().join("data");
    std::fs::create_dir(&data_dir)?;
    let db = data_dir.join("archive.db");
    let writer = gold_seed(&gold_fixture(), &db)?;

    let chat_ordinal = session_scope_ordinal("chat");
    let chat_slot = |seq: u64| SESSION_DOC_BASE + (chat_ordinal << SESSION_DOC_SCOPE_SHIFT) + seq;
    let doc_identity = |slot: u64| {
        frankensearch::quill::cass::cass_document_identity(
            "local",
            frankensearch::quill::cass::CassConversationKey {
                source_path: "/sessions/s-1-chat.jsonl",
                id: Some(1),
            },
            slot,
        )
    };

    // S1's chat scope keeps exactly one chunk; the pre-run queue wants
    // that LIVE chunk dead plus a phantom.
    let pre_run_queue = serde_json::to_string(&vec![
        doc_identity(chat_slot(0)),
        doc_identity(chat_slot(9)),
    ])?;
    writer.execute_with_params(
        "UPDATE viewer_search_region_marks SET docs_state = 'pending', \
         pending_doc_deletes = ?1 WHERE conversation_id = 1",
        &param_slice_to_values(params![pre_run_queue]),
    )?;

    replace_meta_regions(&writer, 1)?;

    let row = writer.query_row(
        "SELECT docs_state, pending_doc_deletes FROM viewer_search_region_marks \
         WHERE conversation_id = 1",
    )?;
    ensure!(
        row.get_typed::<String>(0)? == "meta_pending",
        "the meta replacement re-pends the marks row: {:?}",
        row.get_typed::<String>(0)?
    );
    let pending_raw = row.get_typed::<String>(1)?;
    let queued: Vec<String> = serde_json::from_str(&pending_raw)?;
    let keep: Vec<String> = session_scope_docs(&writer, 1)?
        .expect("S1 owns session docs")
        .iter()
        .map(|doc| doc_identity(doc.slot))
        .collect();
    let live_queued: Vec<&String> = queued.iter().filter(|id| keep.contains(id)).collect();
    ensure!(
        live_queued.is_empty(),
        "GH #457 (meta lane): the replacement queued {live_queued:?} — \
         identities the current derivation KEEPS: {pending_raw}"
    );
    ensure!(
        queued.contains(&doc_identity(chat_slot(9))),
        "the phantom stale entry must ride forward: {pending_raw}"
    );
    Ok(())
}

/// GH #457, Layer 2: a crash-left pending queue — or one left by a
/// conversation the receipt-guarded full scan skipped — is replayed by
/// the terminal converge WITHOUT any republish ever re-filtering it: the
/// rebuild stages the current docs, commits, and the stamp replay
/// executes the queue verbatim against the fresh candidate. An identity
/// the staged generation KEEPS is live in the just-committed candidate by
/// construction, so deleting it makes the fresh reader count fewer
/// documents than the run staged (the owner's real archive: 1,085,152
/// counted, 1,085,145 live, exit 9). The stamp's keep set — the exact
/// identities the stage upserted — is the only filter this path has, so
/// the replay must never delete a keep identity while still retiring
/// every genuinely stale one.
#[test]
fn pending_queue_without_republish_never_deletes_a_live_session_doc() -> Result<()> {
    use coding_agent_search::viewer_extensions::search_regions::{
        SESSION_DOC_BASE, SESSION_DOC_SCOPE_SHIFT, session_scope_ordinal, stage_region_docs,
        stamp_region_docs_published,
    };

    // Session 8's chat packs TWO chunks; the first carries the probe
    // token — the LIVE chunk a stale epoch's queue wants dead.
    let long_z: &'static str =
        Box::leak(format!("ridemarkercero {}", "z".repeat(40_000)).into_boxed_str());
    let long_w: &'static str = Box::leak("w".repeat(40_000).into_boxed_str());
    let mut fixture = gold_fixture();
    fixture.push(GoldSession {
        id: 8,
        agent: "claude_code",
        source_id: "local",
        external_id: "s-8-crash",
        source_path: "/sessions/s-8-crash.jsonl",
        workspace: None,
        title: "gh457 crash-left queue session",
        started_at: Some(6_500),
        ended_at: Some(9_500),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        messages: vec![
            (0, MessageRole::User, long_z, None),
            (1, MessageRole::User, long_w, None),
        ],
    });

    let archive = tempfile::tempdir()?;
    let data_dir = archive.path().join("data");
    std::fs::create_dir(&data_dir)?;
    let db = data_dir.join("archive.db");
    let writer = gold_seed(&fixture, &db)?;
    let index_path = expected_index_dir(&data_dir);

    // Generation 1 — the real lane's stage → commit → stamp order.
    {
        let mut docs = Vec::new();
        for session in &fixture {
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
                    origin_kind: "local".to_owned(),
                    origin_host: session.origin_host.map(str::to_owned),
                    conversation_id: Some(session.id),
                });
            }
        }
        let mut index = QuillCassIndex::open_or_create(&index_path)?;
        index.add_cass_documents(&docs)?;
        let mut stamps = Vec::new();
        for session in &fixture {
            let (_docs, stamp) = stage_region_docs(&mut index, &writer, session.id)?;
            if let Some(stamp) = stamp {
                stamps.push(stamp);
            }
        }
        index.commit()?;
        stamp_region_docs_published(&writer, &stamps)?;
        std::fs::write(
            index_path.join("schema_hash.json"),
            serde_json::to_vec(&json!({
                "schema_hash": coding_agent_search::search::tantivy::SCHEMA_HASH,
            }))?,
        )?;
    }

    let chat_ordinal = session_scope_ordinal("chat");
    let chat_slot = |seq: u64| SESSION_DOC_BASE + (chat_ordinal << SESSION_DOC_SCOPE_SHIFT) + seq;
    let doc_identity = |slot: u64| {
        frankensearch::quill::cass::cass_document_identity(
            "local",
            frankensearch::quill::cass::CassConversationKey {
                source_path: "/sessions/s-8-crash.jsonl",
                id: Some(8),
            },
            slot,
        )
    };

    // The crash-left state: an earlier epoch's queue holds a LIVE keep
    // identity (chat chunk 0 — stale when that epoch wrote it, live now)
    // plus a phantom, and NO republish runs to re-filter it — exactly the
    // receipt-skipped conversation of a full rebuild whose terminal
    // converge still replays the pending queue.
    let stale_queue = serde_json::to_string(&vec![
        doc_identity(chat_slot(0)),
        doc_identity(chat_slot(9)),
    ])?;
    writer.execute_with_params(
        "UPDATE viewer_search_region_marks SET docs_state = 'pending', \
         pending_doc_deletes = ?1 WHERE conversation_id = 8",
        &param_slice_to_values(params![stale_queue]),
    )?;

    // The terminal converge stages the CURRENT docs and commits; the
    // stamp replay then executes the queue against the fresh candidate.
    let mut index =
        coding_agent_search::search::tantivy::TantivyIndex::open_or_create(&index_path)?;
    let converged = index.converge_pending_search_region_docs(&writer, i64::MAX)?;
    ensure!(
        converged == 1,
        "only the re-pended session 8 converges: {converged}"
    );
    index.commit_with_region_doc_stamps(&writer)?;
    let row = writer.query_row(
        "SELECT docs_state, pending_doc_deletes FROM viewer_search_region_marks \
         WHERE conversation_id = 8",
    )?;
    ensure!(
        row.get_typed::<String>(0)? == "published" && row.get_typed::<String>(1)? == "[]",
        "the stamp publishes and clears the queue"
    );

    // The user-visible consequence (GH #457's fresh-reader deficit): the
    // LIVE chunk's posting set must still answer after the stamped commit
    // executed the stale queue.
    drop(writer);
    let mut bridge =
        BridgeHarness::spawn_fixture(tempfile::tempdir()?, data_dir.clone(), db.clone(), &[])?;
    let page = search(&mut bridge, json!({"query":"ridemarkercero"}))?;
    assert_members(&page, &[8])?;
    ensure!(
        page["result"]["membership_source"] == "session_scope_docs",
        "the live chunk answers on the session-scope-doc lane: {page}"
    );
    bridge.shutdown()
}

/// The live region-doc bound counts ONLY doc-backed rows — chat regions
/// (canonical `quill_idx`, `indexed_text` NULL) own no document — plus,
/// since Stage 1, the session×scope chunks every marked conversation
/// stages into the same archive (one per (scope, chunk) the packer
/// emits). The earlier `marks.region_count` bound charged every persisted
/// row for a doc it never had, under-counted the observed message share,
/// and broke the watch checkpoint's refresh/early-exit equality.
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
    // The Stage 1 session-doc term: one chunk per (scope, chunk) the
    // packer derives per MARKED conversation — the same public derivation
    // the staging lane runs, so the bound stays exact at convergence.
    let marked: Vec<i64> = {
        let rows = writer.query("SELECT conversation_id FROM viewer_search_region_marks")?;
        let mut ids = Vec::new();
        for row in &rows {
            ids.push(row.get_typed::<i64>(0)?);
        }
        ids
    };
    let mut session_chunks = 0_u64;
    for cid in &marked {
        session_chunks +=
            coding_agent_search::viewer_extensions::search_regions::session_scope_docs(
                &writer, *cid,
            )?
            .map_or(0, |docs| docs.len() as u64);
    }
    ensure!(
        session_chunks > 0,
        "the fixture must stage session×scope chunks: {session_chunks}"
    );
    let bound =
        coding_agent_search::viewer_extensions::search_regions::live_region_doc_bound(&writer)?;
    ensure!(
        bound == doc_backed as u64 + session_chunks,
        "bound must count doc-backed rows plus session×scope chunks: {bound} != {}",
        doc_backed as u64 + session_chunks
    );
    let marks_total: i64 = writer
        .query_row("SELECT COALESCE(SUM(region_count),0) FROM viewer_search_region_marks")?
        .get_typed(0)?;
    ensure!(
        (doc_backed as u64) < marks_total as u64,
        "the old region_count bound ({marks_total}) over-counted doc-backed rows ({doc_backed})"
    );

    // A queued stale identity raises the bound by exactly its length.
    writer.execute(
        "UPDATE viewer_search_region_marks SET pending_doc_deletes = '[\"a\",\"b\"]' \
         WHERE conversation_id = 1",
    )?;
    let bound =
        coding_agent_search::viewer_extensions::search_regions::live_region_doc_bound(&writer)?;
    ensure!(
        bound == doc_backed as u64 + session_chunks + 2,
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

/// The bridge-side manifest JSON for a REAL extractor publication — the
/// shape a real ingest writes to `coverage_detail_json`, so a test that
/// installs it makes the strict lane evaluate exactly what ingest would
/// publish (the seeded fixture rows are hand-written complete manifests).
fn real_facts_manifest(
    facts: &coding_agent_search::viewer_extensions::facts::FactPublication,
) -> Value {
    let mut fields = serde_json::Map::new();
    for entry in &facts.coverage {
        fields.insert(
            entry.field.clone(),
            json!({"state": entry.state, "reasons": entry.reasons}),
        );
    }
    json!({
        "extractor": facts.extractor,
        "extractor_version": facts.extractor_version,
        "provider": facts.provider,
        "fields": fields,
        "counts": {
            "tool_calls": facts.tools.len(),
            "paired_calls": facts.tools.iter().filter(|t| t.has_result).count(),
        }
    })
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

/// (v-b) `has_mcp` rides the same source-absent classification as the
/// text lanes: the interrupted manifest's `tools` gap carries ONLY the
/// allowlisted `tool_output_interrupted` reason, so the tools inventory
/// is provably exhaustive over every record the rollout ever held — the
/// session is a PROVEN non-member for `has_mcp` (no tag, no tool name
/// mentions mcp), never an Unknown that blocks the strict page. The arm
/// must consult `field_blocks_strict("tools")` (source-absent aware),
/// not a bare `coverage_of("tools") == "complete"` comparison, which
/// treats the interrupted session as unread and refuses the query.
#[test]
fn search_sessions_has_mcp_treats_interrupted_tools_as_proven_absent() -> Result<()> {
    let mut bridge = gold_harness()?;
    set_facts_coverage(&bridge, 5, "partial", codex_interrupted_call_manifest())?;

    // has_mcp=true: S1 carries the `mcp-server` tag; the interrupted S5
    // is a proven non-member (not unknown), so the page answers strict.
    let page = search(
        &mut bridge,
        json!({"ast":{"type":"filter","field":"has_mcp","op":"EQ","value":true}}),
    )?;
    assert_members(&page, &[1])?;
    ensure!(
        page["result"]["source_absent_sessions"] == 1,
        "the interrupted session was evaluated and annotated: {page}"
    );

    // has_mcp=false: the interrupted session is now a PROVEN member —
    // its tools inventory is exhaustive, so no MCP call can hide in it.
    let page = search(
        &mut bridge,
        json!({"ast":{"type":"filter","field":"has_mcp","op":"EQ","value":false}}),
    )?;
    assert_members(&page, &[2, 3, 4, 5, 7])?;
    let s5 = result_item(&page, 5)?;
    ensure!(
        s5["source_text_absent"] == json!(["tool_output_interrupted"]),
        "the interrupted session carries its source-absent reason: {s5}"
    );
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

/// The manifest the post-§5.2 antigravity extractor publishes for a
/// REALISTIC F10 session that also carries truncation markers: the
/// canonical `agy_type` envelope (block inventory) and the F10
/// lifecycle-status verdicts (tool_exit/tool_error partial) sit alongside
/// the truncation marker. After §5.2 the name-only planner calls collect
/// under synthetic ids, so `tools` carries the truncation marker only — a
/// pure source-absent gap — and the result-identity reason narrows to the
/// exit/error scopes. The ENVELOPE mark stays an honest NOT-READ gap —
/// structure the connector does not preserve, so matching regions could
/// still hide behind it, and strict search keeps refusing.
fn antigravity_f10_realistic_manifest() -> Value {
    json!({
        "extractor": "antigravity-facts-v6",
        "extractor_version": 13,
        "provider": "antigravity",
        "fields": {
            "chat": {"state":"partial","reasons":["agy_source_truncated"]},
            "tools": {"state":"partial","reasons":["agy_source_truncated"]},
            "thinking": {"state":"partial","reasons":["agy_source_truncated"]},
            "tool_exit": {"state":"partial","reasons":[
                "agy_status_unpaired",
                "agy_tool_result_identity_missing"
            ]},
            "tool_error": {"state":"partial","reasons":[
                "agy_status_unpaired",
                "agy_tool_result_identity_missing"
            ]},
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
/// also carries the canonical-envelope mark — a NOT-READ gap that can
/// still hide matching regions — so strict search keeps its honest
/// `data_unavailable` refusal even though every mark on the text and
/// tools scopes (`agy_source_truncated`) is allowlisted. The probe uses
/// `foo`, a term S5's verified text lacks: a verified positive match would
/// dominate regardless, but S5's non-membership is exactly what the
/// envelope gap leaves unprovable.
#[test]
fn search_sessions_agy_realistic_marks_still_refuse_strict() -> Result<()> {
    let mut bridge = gold_harness()?;
    set_facts_coverage(&bridge, 5, "partial", antigravity_f10_realistic_manifest())?;

    let page = search(&mut bridge, json!({"query":"foo"}))?;
    ensure!(
        page["ok"] == false,
        "the envelope gap must keep strict search refused: {page}"
    );
    ensure!(
        page["error"]["kind"] == "data_unavailable",
        "expected data_unavailable, got {page}"
    );
    bridge.shutdown()
}

/// The manifest the post-F15, post-§5.2 antigravity extractor publishes
/// for the dominant real-archive shape once every emitted record archives
/// its raw source: the block inventory is VERIFIED complete (the raw
/// record is present and every field accounted for), the name-only
/// planner calls collect under synthetic ids (§5.2) so `tools` carries
/// only the allowlisted `agy_source_truncated`, and the result-identity
/// reason narrows to the exit/error scopes. Strict search may evaluate
/// such a session.
fn antigravity_raw_verified_manifest() -> Value {
    json!({
        "extractor": "antigravity-facts-v6",
        "extractor_version": 13,
        "provider": "antigravity",
        "fields": {
            "chat": {"state":"partial","reasons":["agy_source_truncated"]},
            "tools": {"state":"partial","reasons":["agy_source_truncated"]},
            "thinking": {"state":"partial","reasons":["agy_source_truncated"]},
            "tool_exit": {"state":"partial","reasons":[
                "agy_status_unpaired",
                "agy_tool_result_identity_missing"
            ]},
            "tool_error": {"state":"partial","reasons":[
                "agy_status_unpaired",
                "agy_tool_result_identity_missing"
            ]},
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

/// §5.2 end-to-end (owner decision, 2026-11-10): a REAL antigravity session
/// whose planner invocations carry tool names but NO call ids — the exact
/// archive shape (corpus census: all 10,103 archived calls name-only) —
/// evaluated through the REAL extractor generation. Pre-§5.2 the
/// identity-missing partial made every nameless tool-step region
/// `Unknown`, so strict `tool:` refused the whole page; with the synthetic
/// `agy-anon:` ids the tools scope certifies complete and the strict page
/// answers from the region tool-name inventory.
fn antigravity_synthetic_call_session() -> GoldSession {
    GoldSession {
        id: 8,
        agent: "antigravity",
        source_id: "local",
        external_id: "s-8-agy-synthetic",
        source_path: "/sessions/s-8-agy-synthetic.jsonl",
        workspace: None,
        title: "agy synthetic call ids",
        started_at: Some(9_000),
        ended_at: Some(9_900),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        messages: vec![
            // USER_INPUT shape (role user + wrapper-lift keys): a bare
            // message would classify as connector drift and re-block the
            // very scopes this test heals.
            (
                0,
                MessageRole::User,
                "made-up agy user request text",
                Some(json!({"settings_change": "made-up agy settings change text"})),
            ),
            (
                1,
                MessageRole::Agent,
                "made-up agy planner narration",
                Some(json!({
                    "thinking": "made-up agy thinking text",
                    "cass": {"invocations": [
                        {"kind": "tool", "name": "view_file",
                         "arguments": {"TargetFile": "/made/up/agy-syn-target.rs"}}
                    ]}
                })),
            ),
            // A MODEL-step record: nameless tools-scope region text — the
            // exact region shape that was `Unknown` while the tools scope
            // carried the identity gap.
            (
                2,
                MessageRole::Tool,
                "made-up agy step output",
                Some(json!({"agy_type": "GENERIC"})),
            ),
        ],
    }
}

#[test]
fn search_sessions_agy_synthetic_call_ids_answer_strict_tool_predicates() -> Result<()> {
    let mut sessions = gold_fixture();
    sessions.push(antigravity_synthetic_call_session());
    let mut bridge = gold_harness_full(&sessions, &[])?;

    // Replace the seeded fixture-complete row with the REAL manifest the
    // ingest generation computes, so the strict lane evaluates exactly
    // what a real ingest would publish.
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    let agy = sessions.pop().expect("the antigravity session");
    let generation = coding_agent_search::viewer_extensions::projection::generation_for_canonical(
        &writer,
        &gold_conversation(&agy),
        agy.id,
    )?;
    drop(writer);
    let facts = generation.facts;
    ensure!(
        facts
            .coverage
            .iter()
            .any(|entry| entry.field == "tools" && entry.state == "complete"),
        "the real manifest must certify the tools scope complete: {:?}",
        facts.coverage
    );
    set_facts_coverage(
        &bridge,
        agy.id,
        facts.coverage_state.as_str(),
        real_facts_manifest(&facts),
    )?;

    // Strict tool membership over the nameless step region: with the
    // tools scope complete the region evaluates (False for a missing
    // name), so the page answers instead of refusing.
    let page = search(&mut bridge, json!({"query": "tool:Read"}))?;
    ensure!(page["ok"] == true, "strict tool: must not refuse: {page}");
    ensure!(!ids(&page).contains(&8), "S8 has no Read call: {page}");

    // And the synthesized call's name answers as a member from the region
    // tool-name inventory.
    let page = search(&mut bridge, json!({"query": "tool:view_file"}))?;
    assert_members(&page, &[8])?;
    bridge.shutdown()
}

/// §5.3 end-to-end (owner decision, 2026-11-10): a REAL devin session whose
/// sparse records carry the archived `cass.invocations` (the connector's
/// normalized calls — corpus census: 20,978 records carry both the
/// rendered `[tool_call:` line and the invocations). Pre-§5.3 the facts
/// lane could not see the calls: `devin_tool_calls_not_normalized` +
/// `orphan_tool_result` kept `tools`/`files` partial, so strict `file:` and
/// `has_mcp` refused. With the invocations collected through the same
/// `ToolCollector` path, the file path in the call arguments heals `files`
/// and strict `file:` answers.
fn devin_archived_invocations_session() -> GoldSession {
    GoldSession {
        id: 8,
        agent: "devin",
        source_id: "local",
        external_id: "s-8-devin-invocations",
        source_path: "/sessions/s-8-devin-invocations.jsonl",
        workspace: None,
        title: "devin archived invocations",
        started_at: Some(9_000),
        ended_at: Some(9_900),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        messages: vec![
            // cli_user sparse shape (message_id + is_user_input): a bare
            // message would classify as drift and re-block the healed scopes.
            (
                0,
                MessageRole::User,
                "Fix the broken checks.",
                Some(json!({"message_id": "m2", "is_user_input": true})),
            ),
            (
                1,
                MessageRole::Agent,
                "Reading the file.\n[tool_call: read_file]",
                Some(json!({
                    "cass": {"invocations": [
                        {"kind": "tool", "name": "read_file", "call_id": "inv_1",
                         "arguments": {"TargetFile": "/made/up/devin-target.rs"}}
                    ]}
                })),
            ),
            (
                2,
                MessageRole::Tool,
                "made-up devin file body",
                Some(json!({"tool_call_id": "inv_1"})),
            ),
        ],
    }
}

#[test]
fn search_sessions_devin_archived_invocations_answer_strict_file_predicates() -> Result<()> {
    let mut sessions = gold_fixture();
    sessions.push(devin_archived_invocations_session());
    let mut bridge = gold_harness_full(&sessions, &[])?;

    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    let devin = sessions.pop().expect("the devin session");
    let generation = coding_agent_search::viewer_extensions::projection::generation_for_canonical(
        &writer,
        &gold_conversation(&devin),
        devin.id,
    )?;
    drop(writer);
    let facts = generation.facts;
    for field in ["tools", "files"] {
        ensure!(
            facts
                .coverage
                .iter()
                .any(|entry| entry.field == field && entry.state == "complete"),
            "the real manifest must certify {field} complete: {:?}",
            facts.coverage
        );
    }
    set_facts_coverage(
        &bridge,
        devin.id,
        facts.coverage_state.as_str(),
        real_facts_manifest(&facts),
    )?;

    // The file path carried in the invocation arguments: strict `file:`
    // answers with S8 as a member instead of refusing on the files gap.
    let page = search(
        &mut bridge,
        json!({"query": "file:/made/up/devin-target.rs"}),
    )?;
    assert_members(&page, &[8])?;

    // has_mcp rides the same healed tools scope (§5.1's
    // source-absent-aware arm consults `field_blocks_strict`): S8
    // evaluates as a proven False — a member of the negative set —
    // instead of an Unknown that refuses the whole page.
    let page = search(
        &mut bridge,
        json!({"ast":{"type":"filter","field":"has_mcp","op":"EQ","value":false}}),
    )?;
    ensure!(page["ok"] == true, "strict has_mcp must not refuse: {page}");
    ensure!(
        ids(&page).contains(&8),
        "S8 carries no mcp evidence and must evaluate False: {page}"
    );
    bridge.shutdown()
}

/// Follow-up 1 end-to-end (owner decision, 2026-11-11): a REAL codex
/// session whose `exec_command` output carries NO rendered trailer — the
/// shape of 773 of 10,622 corpus outputs. `exec_command` is a proven
/// status renderer (9,785 outputs carry `Process exited with code N`), so
/// the missing trailer is a not-read gap: the exit/error scopes keep
/// `tool_exit_not_recorded`/`tool_error_not_recorded` and strict
/// `exit:`/`has:error` REFUSE with `data_unavailable` instead of
/// evaluating a fabricated False.
fn codex_exec_command_no_rendered_status_session() -> GoldSession {
    GoldSession {
        id: 8,
        agent: "codex",
        source_id: "local",
        external_id: "s-8-codex-no-rendered-exit",
        source_path: "/sessions/s-8-codex-no-rendered-exit.jsonl",
        workspace: None,
        title: "codex exec_command without rendered status",
        started_at: Some(9_000),
        ended_at: Some(9_900),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        messages: vec![
            // event_msg user_message shape: a bare message would classify
            // as drift and re-block the healed scopes.
            (
                0,
                MessageRole::User,
                "run the checks",
                Some(json!({"type": "event_msg",
                       "payload": {"type": "user_message", "message": "run the checks"}})),
            ),
            (
                1,
                MessageRole::Agent,
                "[Tool: exec_command]",
                Some(json!({"type": "response_item",
                       "payload": {"type": "function_call", "call_id": "call_exec",
                                   "name": "exec_command",
                                   "arguments": "{\"cmd\": \"made-up check\"}",
                                   "id": "resp-1"}})),
            ),
            (
                2,
                MessageRole::Tool,
                "made-up check output",
                Some(json!({"type": "response_item",
                       "payload": {"type": "function_call_output", "call_id": "call_exec",
                                   "output": "made-up check output", "id": "resp-2"}})),
            ),
        ],
    }
}

#[test]
fn search_sessions_codex_exec_command_without_rendered_status_blocks_strict() -> Result<()> {
    let mut sessions = gold_fixture();
    sessions.push(codex_exec_command_no_rendered_status_session());
    let mut bridge = gold_harness_full(&sessions, &[])?;

    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    let codex = sessions.pop().expect("the codex session");
    let generation = coding_agent_search::viewer_extensions::projection::generation_for_canonical(
        &writer,
        &gold_conversation(&codex),
        codex.id,
    )?;
    drop(writer);
    let facts = generation.facts;
    for (field, reason) in [
        ("tool_exit", "tool_exit_not_recorded"),
        ("tool_error", "tool_error_not_recorded"),
    ] {
        let entry = facts
            .coverage
            .iter()
            .find(|entry| entry.field == field)
            .unwrap_or_else(|| panic!("{field}"));
        ensure!(
            entry.reasons.iter().any(|r| r == reason),
            "the real manifest must carry the not-read verdict on {field}: {entry:?}"
        );
        ensure!(
            !entry
                .reasons
                .iter()
                .any(|r| r == "codex_tool_status_absent_at_source"),
            "exec_command renders exit status at the source, so the gap is never source-absent: {entry:?}"
        );
    }
    set_facts_coverage(
        &bridge,
        codex.id,
        facts.coverage_state.as_str(),
        real_facts_manifest(&facts),
    )?;

    // Strict exit:0 — S8's status-less exec_command is a not-read gap, so
    // the strict page refuses instead of evaluating a fabricated False.
    let page = search(&mut bridge, json!({"query": "exit:0"}))?;
    ensure!(
        page["ok"] == false,
        "the not-read exit verdict must block strict: {page}"
    );
    ensure!(
        page["error"]["kind"] == "data_unavailable",
        "the refusal is honest-unavailable: {page}"
    );
    let message = page["error"]["message"].as_str().unwrap_or_default();
    ensure!(
        message.contains("tool_exit"),
        "the refusal must name the tool_exit scope: {page}"
    );

    // allow_partial — S8 stays unknown, so it never appears as a member.
    let page = search(
        &mut bridge,
        json!({"query": "exit:0", "allow_partial": true}),
    )?;
    ensure!(page["ok"] == true, "partial must answer: {page}");
    ensure!(
        !ids(&page).contains(&8),
        "S8's exit verdict is not read, so partial must exclude it: {page}"
    );
    bridge.shutdown()
}

/// Follow-up 1 fixtures: the per-name census pins two verdict regimes for
/// strict exit predicates. `exec_command` renders its exit status as text
/// (`Process exited with code N`; 9,785 of 10,622 outputs) — the rendered
/// line IS the recorded verdict, so strict `exit:`/`has:error` answer
/// True/False exactly. `wait_agent` never renders status in any shape
/// (1,385 outputs, zero renderings) — its gap keeps the allowlisted
/// source-absent verdict and the strict page still answers, evaluating
/// the session as a proven False.
fn codex_exec_trailer_session(
    id: i64,
    external: &'static str,
    title: &'static str,
    output: &'static str,
) -> GoldSession {
    GoldSession {
        id,
        agent: "codex",
        source_id: "local",
        external_id: external,
        // Native rollout shape with the external UUID embedded: once the
        // region pairing records the rendered exit code, the session
        // becomes an `exit:`/`has:error` evaluation candidate and the
        // strict codex identity lane (`viewer_bridge/identity.rs::
        // codex_native_id`) demands a rollout filename or bare-UUID
        // external whose UUIDs agree.
        source_path: Box::leak(
            format!("/codex/sessions/2026/11/11/rollout-2026-11-11T00-00-00-{external}.jsonl")
                .into_boxed_str(),
        ),
        workspace: None,
        title,
        started_at: Some(9_000),
        ended_at: Some(9_900),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        messages: vec![
            (
                0,
                MessageRole::User,
                "run the checks",
                Some(json!({"type": "event_msg",
                       "payload": {"type": "user_message", "message": "run the checks"}})),
            ),
            (
                1,
                MessageRole::Agent,
                "[Tool: exec_command]",
                Some(json!({"type": "response_item",
                       "payload": {"type": "function_call", "call_id": "call_exec",
                                   "name": "exec_command",
                                   "arguments": "{\"cmd\": \"made-up check\"}",
                                   "id": "resp-1"}})),
            ),
            (
                2,
                MessageRole::Tool,
                "made-up check output",
                Some(json!({"type": "response_item",
                       "payload": {"type": "function_call_output", "call_id": "call_exec",
                                   "output": output, "id": "resp-2"}})),
            ),
        ],
    }
}

fn codex_wait_agent_session() -> GoldSession {
    GoldSession {
        id: 10,
        agent: "codex",
        source_id: "local",
        // Rollout-shaped identity (see the trailer fixture note): the
        // wait_agent gap evaluates False via the source-absent swap, and
        // the strict codex identity lane runs for every evaluated
        // candidate.
        external_id: "bbbbbbbb-cccc-dddd-eeee-ffffffffffff",
        source_path: "/codex/sessions/2026/11/11/rollout-2026-11-11T00-00-02-bbbbbbbb-cccc-dddd-eeee-ffffffffffff.jsonl",
        workspace: None,
        title: "codex wait_agent status-less",
        started_at: Some(9_000),
        ended_at: Some(9_900),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        messages: vec![
            (
                0,
                MessageRole::User,
                "wait for the agent",
                Some(json!({"type": "event_msg",
                       "payload": {"type": "user_message", "message": "wait for the agent"}})),
            ),
            (
                1,
                MessageRole::Agent,
                "[Tool: wait_agent]",
                Some(json!({"type": "response_item",
                       "payload": {"type": "function_call", "call_id": "call_wait",
                                   "name": "wait_agent",
                                   "arguments": "{\"agent\": \"made-up agent\"}",
                                   "id": "resp-1"}})),
            ),
            (
                2,
                MessageRole::Tool,
                "made-up agent finished",
                Some(json!({"type": "response_item",
                       "payload": {"type": "function_call_output", "call_id": "call_wait",
                                   "output": "made-up agent finished", "id": "resp-2"}})),
            ),
        ],
    }
}

#[test]
fn search_sessions_codex_rendered_exit_trailer_answers_strict_predicates() -> Result<()> {
    let mut sessions = gold_fixture();
    sessions.push(codex_exec_trailer_session(
        8,
        "11111111-2222-3333-4444-555555555555",
        "codex exec_command rendered exit 0",
        "made-up checks passed\nProcess exited with code 0\n",
    ));
    sessions.push(codex_exec_trailer_session(
        9,
        "66666666-7777-8888-9999-aaaaaaaaaaaa",
        "codex exec_command rendered exit 1",
        "made-up checks failed\nProcess exited with code 1\n",
    ));
    sessions.push(codex_wait_agent_session());
    let mut bridge = gold_harness_full(&sessions, &[])?;

    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    let wait = sessions.pop().expect("the wait_agent session");
    let failed = sessions.pop().expect("the exit-1 session");
    let ok = sessions.pop().expect("the exit-0 session");
    for session in [&wait, &failed, &ok] {
        let generation =
            coding_agent_search::viewer_extensions::projection::generation_for_canonical(
                &writer,
                &gold_conversation(session),
                session.id,
            )?;
        let facts = generation.facts;
        set_facts_coverage(
            &bridge,
            session.id,
            facts.coverage_state.as_str(),
            real_facts_manifest(&facts),
        )?;
    }
    drop(writer);

    // exit:0 — S8's rendered `Process exited with code 0` is the recorded
    // verdict (True); S9's is 1 (False); S10's wait_agent gap is proven
    // source-absent (False). Strict answers.
    let page = search(&mut bridge, json!({"query": "exit:0"}))?;
    assert_members(&page, &[8])?;

    // has:error — S9's nonzero render is True (with the base S3 error);
    // S8 (exit 0) and S10 (absent) evaluate False.
    let page = search(&mut bridge, json!({"query": "has:error"}))?;
    assert_members(&page, &[3, 9])?;
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
    set_facts_coverage(
        &bridge,
        agy.id,
        facts.coverage_state.as_str(),
        real_facts_manifest(&facts),
    )?;

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

// ---------------------------------------------------------------------------
// Strict search Stage 1: session×scope docs fast path + lazy fragments
// (owner decision 2026-11-10)
// ---------------------------------------------------------------------------
//
// Plans whose every literal is a single ASCII token (exact or prefix)
// answer from the published session×scope documents: one Quill doc per
// (conversation, scope) chunk, derived and published through the same
// region-doc lane, marks-disciplined. Membership is the session-doc
// posting set; NOT/AND/OR keep the three-valued set algebra; witnesses are
// discovered textually for the emitted page only. The ONE deliberate
// relaxation: a session whose doc claims membership but whose regions
// cannot textually reproduce the literal (analyzer prefix-n-gram drift) is
// EMITTED with `witness_count=0`, a page-level warning and the
// `membership_source` receipt — never silently dropped, never a whole-page
// refusal. Pending marks and not-read coverage gaps keep refusing exactly
// as before. Everything else — multi-token literals, exact phrases,
// non-ASCII, same-block ANDs, literal×region-predicate ANDs — stays on the
// legacy per-region lane. Lazy fragments: the universe load is
// structural-only; `indexed_text` hydrates per emitted page.
// ---------------------------------------------------------------------------

/// Stage 1 (i): single-token ASCII plans answer exactly from the
/// session×scope documents and report the lane receipt. Every probe here is
/// a single token in at least one fixture region, so the fast path and the
/// legacy lane must agree on membership — the assertions pin the RECEIPT
/// (`membership_source`), the exact uncapped `total_sessions`, and
/// membership itself, so a fast path that answers wrong never passes.
#[test]
fn search_sessions_fast_path_single_token_membership() -> Result<()> {
    let mut bridge = gold_harness()?;

    // foo lives in S1+S2 chat — the chat-scope session docs carry it.
    let page = search(&mut bridge, json!({"query":"foo"}))?;
    assert_members(&page, &[1, 2])?;
    ensure!(
        page["result"]["membership_source"] == "session_scope_docs",
        "single-token plans must report the session-scope-doc lane: {page}"
    );
    ensure!(
        page["result"]["total_sessions"] == 2,
        "the fast path is exact, never capped: {page}"
    );

    // quux lives in S3 tool fragments and S5 chat — two providers, one
    // posting-set answer.
    let page = search(&mut bridge, json!({"query":"quux"}))?;
    assert_members(&page, &[3, 5])?;
    ensure!(
        page["result"]["membership_source"] == "session_scope_docs",
        "single-token plans must report the session-scope-doc lane: {page}"
    );

    // zeta lives only inside a thinking block: the thinking-scope chunk
    // carries it even though no chat region does.
    let page = search(&mut bridge, json!({"query":"zeta"}))?;
    assert_members(&page, &[4])?;
    ensure!(
        page["result"]["membership_source"] == "session_scope_docs",
        "scope-chunk membership must cover non-chat scopes: {page}"
    );

    // deploy spans S3's chat, tool-call and tool-result regions.
    let page = search(&mut bridge, json!({"query":"deploy"}))?;
    assert_members(&page, &[3])?;

    // Quoted single token: still one literal; exactness is the matcher's
    // job over the session's own regions, not the lane's.
    let page = search(&mut bridge, json!({"query":"\"deploy\""}))?;
    assert_members(&page, &[3])?;
    ensure!(
        page["result"]["membership_source"] == "session_scope_docs",
        "quoted single tokens ride the same lane: {page}"
    );

    // Prefix literal (`flu*` → the non-exact token "flu") rides the lane;
    // the n-gram gate hits S1's notes fragment and the matcher reproduces.
    let page = search(&mut bridge, json!({"query":"flu*"}))?;
    assert_members(&page, &[1])?;
    ensure!(
        page["result"]["membership_source"] == "session_scope_docs",
        "prefix literals ride the session-scope-doc lane: {page}"
    );

    bridge.shutdown()
}

/// The 4 KiB `content_prefix` window must never hide a prefix-shaped token
/// from a bare term: `dockerfile` sitting past the window (≈12 KiB of benign
/// filler before it) is in the message's canonical region document AND in
/// the packed session×scope chunk, and the bare term `docker` must find it
/// on BOTH strict lanes — the Stage 1 session×scope fast lane (whose
/// membership posting set is the aggregated chunk document) and the legacy
/// per-region lane (forced by staling one session's marks row).
///
/// RED on the incumbent engine: a bare term matches only the windowed
/// edge-gram fields, so a deep-window occurrence silently misses. The
/// frankensearch fork's two-arm Exact|Prefix lowering adds a `content`
/// glob arm, which expands against each leaf's own unwindowed term
/// dictionary; the match set is what matters here, both lanes must carry
/// it, and an over-broad prefix refuses typed instead of under-matching
/// (see the raw-lane refusal test in `viewer_bridge::search`).
#[test]
fn search_sessions_bare_term_finds_deep_window_prefix_occurrence() -> Result<()> {
    let mut fixture = gold_fixture();
    // >4 KiB of filler before the probe, comfortably past the window and
    // inside the 64 KiB region/chunk text caps.
    let mut deep_body = "filler ".repeat(2048);
    deep_body.push_str("dockerfile");
    let deep_body: &'static str = Box::leak(deep_body.into_boxed_str());
    fixture.push(GoldSession {
        id: 8,
        agent: "claude_code",
        source_id: "local",
        external_id: "s-8-deep-window",
        source_path: "/sessions/s-8-deep-window.jsonl",
        workspace: Some("/data/projects/gamma"),
        title: "deep window session",
        started_at: Some(6_000),
        ended_at: Some(6_100),
        origin_host: None,
        metadata: json!({"cass":{"origin":{"kind":"local"}}}),
        messages: vec![(0, MessageRole::User, deep_body, None)],
    });
    let mut bridge = gold_harness_full(&fixture, &[])?;

    // Fast lane: the session×scope chunk carries the occurrence deep in its
    // content; the bare term's posting set must include conversation 8.
    let page = search(&mut bridge, json!({"query":"docker"}))?;
    ensure!(
        page["result"]["membership_source"] == "session_scope_docs",
        "a single-token plan must ride the fast lane: {page}"
    );
    assert_members(&page, &[8])?;
    let matches = page["result"]["matches"]["8"]
        .as_array()
        .expect("S8 fast-lane witnesses");
    ensure!(
        matches.len() == 1 && matches[0]["message_idx"].as_i64() == Some(0),
        "the deep-window member carries one witness on message 0: {page}"
    );

    // Legacy lane: stale conversation 8's marks row so the marks gate falls
    // the whole query back to the per-region enumeration, and the same bare
    // term must still find the deep region document.
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer.execute_with_params(
        "UPDATE viewer_search_region_marks SET region_version=?1 \
         WHERE conversation_id=8",
        &param_slice_to_values(params![
            coding_agent_search::viewer_extensions::search_regions::REGION_VERSION - 1
        ]),
    )?;
    drop(writer);
    let page = search(&mut bridge, json!({"query":"docker"}))?;
    ensure!(
        page["result"]["membership_source"] == "region_literals",
        "the staled mark must force the legacy region lane: {page}"
    );
    assert_members(&page, &[8])?;
    let matches = page["result"]["matches"]["8"]
        .as_array()
        .expect("S8 legacy-lane witnesses");
    ensure!(
        matches.len() == 1 && matches[0]["message_idx"].as_i64() == Some(0),
        "the legacy lane must witness the deep region too: {page}"
    );
    bridge.shutdown()
}

/// Stage 1 (ii): NOT is the set complement, OR the union, AND NOT the
/// intersection-with-complement — all exact over the posting sets, with
/// source-absent sessions staying exact members under their annotation
/// (owner decision 2026-11-07 preserved on the fast lane).
#[test]
fn search_sessions_fast_path_not_complement_and_source_absent() -> Result<()> {
    let mut bridge = gold_harness()?;

    // NOT over session docs: complement over the browse universe (S6 stays
    // out of it as everywhere else).
    let page = search(&mut bridge, json!({"query":"NOT foo"}))?;
    assert_members(&page, &[3, 4, 5, 7])?;
    ensure!(
        page["result"]["membership_source"] == "session_scope_docs",
        "NOT over a fast literal stays on the fast lane: {page}"
    );
    ensure!(
        page["result"]["total_sessions"] == 4,
        "the complement must be exact: {page}"
    );

    // OR: union of the two posting sets.
    let page = search(&mut bridge, json!({"query":"foo OR zeta"}))?;
    assert_members(&page, &[1, 2, 4])?;
    ensure!(
        page["result"]["membership_source"] == "session_scope_docs",
        "OR over fast literals stays on the fast lane: {page}"
    );

    // AND NOT: intersection with complement, exact count — the
    // `error AND NOT test` shape.
    let page = search(&mut bridge, json!({"query":"quux AND NOT deploy"}))?;
    assert_members(&page, &[5])?;
    ensure!(
        page["result"]["total_sessions"] == 1,
        "the intersection must be exact: {page}"
    );
    ensure!(
        page["result"]["membership_source"] == "session_scope_docs",
        "AND NOT over fast literals stays on the fast lane: {page}"
    );

    // Source-absent gap (provider-encrypted reasoning, allowlisted) keeps
    // the strict answer exact with the annotation on the fast lane too.
    set_facts_coverage(
        &bridge,
        5,
        "unavailable",
        codex_reasoning_encrypted_manifest(),
    )?;
    let page = search(&mut bridge, json!({"query":"quux"}))?;
    assert_members(&page, &[3, 5])?;
    let s5 = result_item(&page, 5)?;
    ensure!(
        s5["source_text_absent"] == json!(["reasoning_encrypted"]),
        "the encrypted-reasoning gap must stay annotated on the fast lane: {s5}"
    );
    ensure!(
        page["result"]["source_absent_sessions"] == 1,
        "the source-absent count survives the fast lane: {page}"
    );
    // A term S5's existing text lacks is still a PROVEN non-member under
    // source absence — negatives keep today's semantics.
    let page = search(&mut bridge, json!({"query":"warpdrive"}))?;
    assert_members(&page, &[])?;
    ensure!(
        page["result"]["source_absent_sessions"] == 1,
        "S5 was evaluated (and excluded) under source absence: {page}"
    );
    bridge.shutdown()
}

/// Stage 1 (iii): the full keyset walk at limit 1 equals the one-shot
/// member set — no duplicates, no losses, one generation throughout, and
/// every page carries the exact `total_sessions`.
#[test]
fn search_sessions_fast_path_cursor_walk_equals_one_shot() -> Result<()> {
    let mut bridge = gold_harness()?;

    let whole = search(&mut bridge, json!({"query":"quux", "sort":"updated_desc"}))?;
    assert_members(&whole, &[3, 5])?;
    ensure!(
        whole["result"]["total_sessions"] == 2,
        "one-shot total must be exact: {whole}"
    );
    let generation = whole["result"]["generation"].clone();

    let mut walked: Vec<i64> = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..10 {
        let mut body = json!({"query":"quux","limit":1,"sort":"updated_desc"});
        if let Some(raw) = &cursor {
            body["cursor"] = json!(raw);
        }
        let page = search(&mut bridge, body)?;
        ensure!(page["ok"] == true, "walk page refused: {page}");
        ensure!(
            page["result"]["generation"] == generation,
            "generation must stay bound across fast-path pages: {page}"
        );
        ensure!(
            page["result"]["total_sessions"] == 2,
            "every fast-path page carries the exact total: {page}"
        );
        for item in page["result"]["items"].as_array().expect("items") {
            let id = item["conversation_id"].as_i64().expect("id");
            ensure!(
                !walked.contains(&id),
                "keyset walk must never repeat a session: {walked:?}"
            );
            walked.push(id);
        }
        cursor = page["result"]["next_cursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    walked.sort_unstable();
    ensure!(
        walked == vec![3, 5],
        "the full cursor walk must equal the one-shot member set: {walked:?}"
    );
    bridge.shutdown()
}

/// Stage 1 (iv): marks discipline. The session-scope fast lane engages only
/// when EVERY universe session's marks are converged — `docs_state`
/// published at the current `REGION_VERSION`. One unconverged session
/// (`docs_state` pending, a stale `region_version` from an older binary, or
/// a missing marks row) keeps the whole query on the legacy region lane,
/// refusing and answering exactly as today: the pending gap still degrades
/// an unwitnessed candidate to `facts_pending` under strict and to the
/// incomplete `allow_partial` page, and the lane receipt names what ran.
/// The 65k no-marks universe of the existing
/// `search_sessions_large_universe_stays_honest` stress keeps its cap
/// refusal through the same fallback.
#[test]
fn search_sessions_fast_path_marks_pending_refuses() -> Result<()> {
    let mut bridge = gold_harness()?;
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;

    // docs_state pending: S1's session documents are not converged. S1 has
    // no quux witness, so its pending inventory gap is the honest Unknown
    // today — and the fast lane must not bypass it by trusting docs.
    writer.execute(
        "UPDATE viewer_search_region_marks SET docs_state='pending' \
         WHERE conversation_id=1",
    )?;
    drop(writer);
    let page = search(&mut bridge, json!({"query":"quux"}))?;
    ensure!(
        page["ok"] == false,
        "pending session docs must keep the strict refusal: {page}"
    );
    ensure!(
        page["error"]["kind"] == "facts_pending",
        "pending session docs are reindexable facts: {page}"
    );

    // allow_partial: S1 drops to unknown, the witnessed members stay, the
    // page is honestly incomplete with the standing warning.
    let page = search(&mut bridge, json!({"query":"quux","allow_partial":true}))?;
    ensure!(page["ok"] == true, "partial opt-in must serve: {page}");
    ensure!(
        page["result"]["complete"] == false,
        "the pending session keeps the page incomplete: {page}"
    );
    ensure!(
        ids(&page) == vec![3, 5],
        "the witnessed members stay under allow_partial: {page}"
    );
    let warnings = page["result"]["warnings"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    ensure!(
        warnings.iter().any(|w| w == "membership_incomplete"),
        "the incomplete page must keep its warning: {page}"
    );

    // A published marks row from an OLDER region version carries no current
    // session docs. The query must not trust them: it falls back to the
    // legacy region lane, answers today's exact member set, and receipts
    // the lane that ran.
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer.execute_with_params(
        "UPDATE viewer_search_region_marks SET docs_state='published', region_version=?1 \
         WHERE conversation_id=1",
        &param_slice_to_values(params![
            coding_agent_search::viewer_extensions::search_regions::REGION_VERSION - 1
        ]),
    )?;
    drop(writer);
    let page = search(&mut bridge, json!({"query":"quux"}))?;
    ensure!(
        page["ok"] == true,
        "a stale region_version falls back to the legacy lane, not a refusal: {page}"
    );
    assert_members(&page, &[3, 5])?;
    ensure!(
        page["result"]["membership_source"] == "region_literals",
        "a stale region_version must never feed the fast lane: {page}"
    );
    ensure!(
        page["result"]["complete"] == true,
        "the legacy fallback keeps the exact strict answer: {page}"
    );

    // A missing marks row (never published) is the same pending gap.
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer.execute("DELETE FROM viewer_search_region_marks WHERE conversation_id=1")?;
    drop(writer);
    let page = search(&mut bridge, json!({"query":"quux"}))?;
    ensure!(
        page["ok"] == false && page["error"]["kind"] == "facts_pending",
        "a missing marks row must refuse facts_pending: {page}"
    );
    bridge.shutdown()
}

/// Stage 1 (v): witness discovery runs for the emitted page's sessions
/// only — bounded by `max_matches_per_session`, anchored, snippet-bearing,
/// with a per-item `witness_count` receipt — and the one deliberate
/// relaxation: an analyzer n-gram hit the region text cannot reproduce
/// ("depl" is an n-gram of "deploy", never a whole token) is EMITTED with
/// `witness_count=0` plus a page-level warning, never a whole-page refusal
/// and never a silent member.
#[test]
fn search_sessions_fast_path_witnesses_and_drift_annotation() -> Result<()> {
    let mut bridge = gold_harness()?;

    let page = search(&mut bridge, json!({"query":"quux"}))?;
    assert_members(&page, &[3, 5])?;
    let matches = page["result"]["matches"].as_object().expect("matches");
    let s3 = matches
        .get("3")
        .and_then(Value::as_array)
        .expect("S3 witnesses");
    ensure!(!s3.is_empty(), "S3 must carry witnesses: {page}");
    ensure!(
        s3.iter()
            .any(|m| m["location"] == "tools" && m["message_idx"] == json!(2)),
        "expected a tools witness bound to message idx 2: {s3:?}"
    );
    ensure!(
        s3.iter().all(|m| m["anchor_status"] == "verified"),
        "witnesses must bind canonical message coordinates: {s3:?}"
    );
    let s5 = matches
        .get("5")
        .and_then(Value::as_array)
        .expect("S5 witnesses");
    ensure!(
        s5.iter().any(|m| m["snippet"]
            .as_str()
            .is_some_and(|snippet| snippet.contains("quux"))),
        "S5's chat witness must carry a snippet: {s5:?}"
    );
    let s3_item = result_item(&page, 3)?;
    ensure!(
        s3_item["witness_count"].as_i64() == Some(s3.len() as i64),
        "witness_count must match the delivered witness list: {s3_item}"
    );

    // The bound: max_matches_per_session caps discovery per session.
    let page = search(
        &mut bridge,
        json!({"query":"quux","max_matches_per_session":2}),
    )?;
    assert_members(&page, &[3, 5])?;
    let s3 = page["result"]["matches"]["3"]
        .as_array()
        .expect("S3 witnesses");
    ensure!(
        s3.len() == 2,
        "witness discovery must stop at max_matches_per_session: {s3:?}"
    );
    let s3_item = result_item(&page, 3)?;
    ensure!(
        s3_item["witness_count"] == 2,
        "witness_count must report the delivered bound: {s3_item}"
    );

    // The drift case: the session doc gates a member the regions cannot
    // reproduce textually. Emitted, warned, annotated — never refused.
    let page = search(
        &mut bridge,
        json!({"ast":{"type":"term","value":"depl","exact":true}}),
    )?;
    ensure!(
        page["ok"] == true,
        "an n-gram drift hit must not refuse the strict page: {page}"
    );
    ensure!(
        ids(&page) == vec![3],
        "the doc-claimed member is emitted exactly: {page}"
    );
    ensure!(
        page["result"]["membership_source"] == "session_scope_docs",
        "the drift annotation rides the fast-lane receipt: {page}"
    );
    let s3_item = result_item(&page, 3)?;
    ensure!(
        s3_item["witness_count"] == 0,
        "no textual witness reproduces the n-gram hit: {s3_item}"
    );
    let warnings = page["result"]["warnings"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    ensure!(
        warnings.iter().any(|w| w == "witnesses_not_reproduced"),
        "the drift must carry a page-level warning, never silence: {page}"
    );
    ensure!(
        page["result"]["matches"]
            .as_object()
            .is_some_and(|m| m.get("3").is_none()),
        "a session with no reproduced witness carries no witness list: {page}"
    );
    bridge.shutdown()
}

/// Stage 1 (vi): the composition boundary. Same-block ANDs, multi-token
/// literals, exact phrases, non-ASCII literals, literal×region-predicate
/// ANDs and literal-free plans keep the per-region semantics on the legacy
/// lane (`membership_source` = `region_literals`) — the fast path must
/// never silently change what `foo AND bar` means. An OR over a fast
/// literal and a region predicate stays fast: OR truth is per-arm.
#[test]
fn search_sessions_fast_path_boundary_stays_legacy() -> Result<()> {
    let mut bridge = gold_harness()?;

    // Two positive literals under AND carry same-block semantics the
    // session docs cannot express.
    let page = search(&mut bridge, json!({"query":"foo AND bar"}))?;
    assert_members(&page, &[1])?;
    ensure!(
        page["result"]["membership_source"] == "region_literals",
        "two-positive-literal ANDs keep same-block semantics on the legacy lane: {page}"
    );

    // Multi-token non-exact literal (mcp-server → tokens mcp+server).
    let page = search(&mut bridge, json!({"query":"mcp-server"}))?;
    assert_members(&page, &[1])?;
    ensure!(
        page["result"]["membership_source"] == "region_literals",
        "multi-token literals stay on the legacy lane: {page}"
    );

    // Exact phrase: legacy.
    let page = search(&mut bridge, json!({"query":"\"foo bar\""}))?;
    assert_members(&page, &[1])?;
    ensure!(
        page["result"]["membership_source"] == "region_literals",
        "exact phrases stay on the legacy lane: {page}"
    );

    // Non-ASCII literal: invisible to the analyzer, ungated evaluation.
    let page = search(&mut bridge, json!({"query":"кириллица"}))?;
    assert_members(&page, &[])?;
    ensure!(
        page["result"]["membership_source"] == "region_literals",
        "non-ASCII literals stay on the legacy lane: {page}"
    );

    // A literal ANDed with a region-level predicate keeps region truth.
    let page = search(&mut bridge, json!({"query":"in:tools quux"}))?;
    assert_members(&page, &[3])?;
    ensure!(
        page["result"]["membership_source"] == "region_literals",
        "literal×scope ANDs stay on the legacy lane: {page}"
    );
    let page = search(&mut bridge, json!({"query":"tool:Bash quux"}))?;
    assert_members(&page, &[3])?;
    ensure!(
        page["result"]["membership_source"] == "region_literals",
        "literal×tool ANDs stay on the legacy lane: {page}"
    );

    // A literal-free plan has no session-doc work: legacy.
    let page = search(&mut bridge, json!({"query":"NOT tool:Bash"}))?;
    assert_members(&page, &[1, 2, 4, 5, 7])?;
    ensure!(
        page["result"]["membership_source"] == "region_literals",
        "literal-free plans stay on the legacy lane: {page}"
    );

    // OR over a fast literal and a region predicate: per-arm truth, fast.
    let page = search(&mut bridge, json!({"query":"foo OR has:error"}))?;
    assert_members(&page, &[1, 2, 3])?;
    ensure!(
        page["result"]["membership_source"] == "session_scope_docs",
        "OR over a fast literal and a region predicate stays fast: {page}"
    );
    bridge.shutdown()
}

/// Stage 1 (vii): the fast path inherits the generation-bound cursor. A
/// canonical commit between two pages moves the bound generation — the
/// continuation is a typed `cursor_expired`, never a mixed-generation page,
/// and fresh first pages stay exact.
#[test]
fn search_sessions_fast_path_stale_generation_cursor_refused() -> Result<()> {
    let mut bridge = gold_harness()?;
    // Canonical observers make a second connection's commit bump
    // `canonical_revision` (the same mechanics as the legacy mid-index
    // test).
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    revisions::ensure_observers(&writer)?;
    drop(writer);

    let page1 = search(
        &mut bridge,
        json!({"query":"quux","limit":1,"sort":"updated_desc"}),
    )?;
    ensure!(page1["ok"] == true, "page1 refused: {page1}");
    ensure!(
        page1["result"]["membership_source"] == "session_scope_docs",
        "the paginated fast plan must report its lane: {page1}"
    );
    let cursor = page1["result"]["next_cursor"]
        .as_str()
        .expect("quux fixture must paginate at limit=1")
        .to_owned();

    // Mid-index: a guarded conversations-UPDATE on a SECOND connection
    // bumps canonical_revision inside its commit.
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer.execute("UPDATE conversations SET title='fast path retitle' WHERE id=1")?;
    drop(writer);

    let page2 = search(
        &mut bridge,
        json!({"query":"quux","limit":1,"sort":"updated_desc","cursor":cursor}),
    )?;
    ensure!(
        page2["ok"] == false && page2["error"]["kind"] == "cursor_expired",
        "a moved generation must expire the fast-path cursor: {page2}"
    );

    // The lane stays live: a fresh first page still serves exact membership.
    let fresh = search(&mut bridge, json!({"query":"quux"}))?;
    assert_members(&fresh, &[3, 5])?;
    bridge.shutdown()
}

/// Stage 1 (viii): lazy fragments. The universe load is structural-only —
/// `indexed_text` never rides it — and every derived/meta snippet stays
/// byte-identical to the eager loader's answer on this same fixture (the
/// golden snippets were captured from the incumbent loader before the
/// change). `fragments_loaded` receipts the lazy reads. Stage 1b extends
/// the receipts: `region_rows_loaded` counts only what the request read
/// (page sessions on the fast lane, gated seeks on the legacy lane), and
/// `region_load_mode`/`region_load_reason` name the strategy and, for the
/// structural fallback, its cause.
#[test]
fn search_sessions_lazy_fragments_equivalence() -> Result<()> {
    let mut bridge = gold_harness()?;

    // Fast lane + meta fragment: the notes snippet hydrates lazily.
    let page = search(&mut bridge, json!({"query":"fluxcap"}))?;
    assert_members(&page, &[1])?;
    ensure!(
        page["result"]["diagnostics"]["fragments_loaded"]
            .as_u64()
            .is_some(),
        "the lazy-fragment lane must receipt its fragment reads: {page}"
    );
    let witnesses = page["result"]["matches"]["1"]
        .as_array()
        .expect("S1 witnesses");
    ensure!(
        witnesses
            .iter()
            .any(|m| m["snippet"] == json!("the fluxcap release note")),
        "fluxcap snippet must stay byte-identical: {witnesses:?}"
    );
    let s1_item = result_item(&page, 1)?;
    ensure!(
        s1_item["witness_count"].as_i64() == Some(witnesses.len() as i64),
        "witness_count must match the delivered list: {s1_item}"
    );

    // Legacy lane + derived fragment: the thinking snippet is unchanged.
    let page = search(&mut bridge, json!({"query":"in:thinking zeta"}))?;
    assert_members(&page, &[4])?;
    let witnesses = page["result"]["matches"]["4"]
        .as_array()
        .expect("S4 witnesses");
    ensure!(
        witnesses
            .iter()
            .any(|m| m["snippet"] == json!("pondering zeta")),
        "zeta snippet must stay byte-identical: {witnesses:?}"
    );

    // Legacy lane + derived fragment: the artifacts attachment path.
    let page = search(&mut bridge, json!({"query":"in:artifacts evidence"}))?;
    assert_members(&page, &[7])?;
    let witnesses = page["result"]["matches"]["7"]
        .as_array()
        .expect("S7 witnesses");
    ensure!(
        witnesses
            .iter()
            .any(|m| m["snippet"] == json!("/tmp/evidence.png")),
        "evidence snippet must stay byte-identical: {witnesses:?}"
    );

    // Legacy lane + meta fragment: the subagents descriptor.
    let page = search(&mut bridge, json!({"query":"in:subagents zephyr"}))?;
    assert_members(&page, &[1])?;
    let witnesses = page["result"]["matches"]["1"]
        .as_array()
        .expect("S1 witnesses");
    ensure!(
        witnesses.iter().any(|m| m["snippet"]
            == json!("subagent worker zephyr\ns-1-chat/subagents/agent-worker.jsonl")),
        "zephyr snippet must stay byte-identical: {witnesses:?}"
    );

    // Stage 1b receipts. "foo" rides the fast lane, so its region read is
    // the EMITTED page's sessions only (S1 + S2) — and since 2026-11-11
    // (B), gated further by foo's raw Quill slots: exactly the two chat
    // regions carrying foo, a strict subset of the emitted sessions'
    // structural rows (the COUNT(*) below). `region_rows_loaded` reflects
    // exactly what was read, and `region_load_mode` names the strategy.
    // A zero-hit or deeper page never pays for sessions it did not emit.
    let structural_rows: i64 = {
        let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
        writer
            .query_row("SELECT COUNT(*) FROM viewer_search_regions WHERE conversation_id IN (1,2)")?
            .get_typed(0)?
    };
    let page = search(&mut bridge, json!({"query":"foo"}))?;
    let gated_rows = page["result"]["diagnostics"]["region_rows_loaded"].as_u64();
    ensure!(
        gated_rows == Some(2) && gated_rows <= Some(structural_rows as u64),
        "the gated page load reads foo's two chat regions, never the full \
         structural set ({structural_rows}): {page}"
    );
    ensure!(
        page["result"]["diagnostics"]["region_load_mode"] == json!("emitted_page_sessions"),
        "the fast lane must receipt its page-time region load: {page}"
    );
    // A structural-fallback plan (a block-domain predicate — `scope` here)
    // keeps today's eager load: every persisted universe row is counted
    // (S6 is outside the default browse universe), the mode names the
    // fallback and the reason names its cause.
    let expected_rows: i64 = {
        let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
        writer
            .query_row("SELECT COUNT(*) FROM viewer_search_regions WHERE conversation_id != 6")?
            .get_typed(0)?
    };
    let page = search(&mut bridge, json!({"query":"in:thinking zeta"}))?;
    ensure!(
        page["result"]["diagnostics"]["region_rows_loaded"].as_u64() == Some(expected_rows as u64),
        "the structural fallback must still count every row: {page}"
    );
    ensure!(
        page["result"]["diagnostics"]["region_load_mode"] == json!("universe_structural")
            && page["result"]["diagnostics"]["region_load_reason"]
                == json!("region_domain_predicate"),
        "the structural fallback must receipt its mode and reason: {page}"
    );
    // A legacy gated plan (two single-token conjuncts are not
    // fast-lane-composable) reads ONLY the gated rows: strictly fewer than
    // the universe unless every region is gated.
    let page = search(&mut bridge, json!({"query":"foo bar"}))?;
    ensure!(
        page["result"]["diagnostics"]["region_load_mode"] == json!("gated_quill_seeks"),
        "the legacy gated lane must receipt its seek strategy: {page}"
    );
    let gated_rows = page["result"]["diagnostics"]["region_rows_loaded"]
        .as_u64()
        .unwrap_or(0);
    ensure!(
        gated_rows <= expected_rows as u64,
        "gated seeks must never read more than the universe: {page}"
    );
    bridge.shutdown()
}

/// Stage 1b: on-demand region loading — A/B equivalence against the Stage 1
/// eager universe loader (commit 87822ef2), pinned on this gold fixture.
///
/// The pinned digests ARE the Stage 1 answers for this corpus: the same
/// corpus ran against the 87822ef2 tree before the Stage 1b change and the
/// full responses were captured (receipt:
/// `.scratch/glm/2026-11-10-session-scope-docs/` — stage1 capture
/// `stage1_true_answers.json`, Stage 1b capture `stage1b_run_r1.json`; the
/// digests below are the Stage 1 values, and the two captures agree
/// byte-for-byte). Each digest covers the WHOLE response minus per-run
/// fields only (`result.diagnostics`, `result.archive_stamp` — the harness
/// tempdir path + inode, `result.catalog_revision` — a hash over that path,
/// `result.index_epoch` + `result.next_cursor` — fresh per harness index),
/// so members, witnesses, snippets, scores, verdicts, warnings and cursor
/// resumption all ride the pin.
///
/// The pinned DIGESTS are the unchanging contract: members, witnesses,
/// snippets, scores, verdicts, warnings and cursor resumption must be
/// byte-identical across every region-read strategy change. The counters
/// (`region_rows_loaded`, `regions_hydrated`, `messages_hydrated`,
/// `fragments_loaded`, `verification_bytes_scanned`) are per-strategy
/// RECEIPTS — only what was actually read. 2026-11-11 (B) gates the fast
/// lane's page-time witness discovery by the claims' raw Quill slots: a
/// fast-lane pin reads only its claim-gated rows (structural rows ->
/// gated rows; verification bytes -> 0 where the bounded-window shortcut
/// proves the member), while every legacy-lane pin keeps its receipts
/// unchanged. `region_load_mode`/`
/// `region_load_reason` name the strategy (fast lane:
/// `emitted_page_sessions`; legacy lane: `gated_quill_seeks`; seek lane:
/// `block_predicate_seeks`; fallback plans: `universe_structural` +
/// reason).
///
/// 2026-11-11, fork rev bb7d899a (literal-drop, pinned by the committed
/// `[patch.crates-io]` table): the fork still lowers every non-CJK bare-term
/// literal to `Term(searchable) OR Glob(content, term*)`, but the glob arm
/// now drops its own literal from the expansion AFTER the cap — a doc whose
/// only glob match is the literal itself keeps its stock-quill score. On
/// this fixture corpus no term has a true prefix extension beyond the
/// 4-KiB prefix window, so ALL eight bare-term pins (`foo`, `fluxcap`,
/// `quux` limit-1, `foo bar`, `in:thinking zeta`, the tool=Bash∧quux AST
/// pin, `foo`+tag and the page-2 cursor walk) reverted byte-for-byte to the
/// stock-quill Stage 1 digests, verified by a collector re-capture whose
/// receipts — membership verdicts, hit sets, load mode/reason/rows,
/// hydration counters — were identical for every pin; only the score lines
/// had moved under the earlier rev (ce5f1bd6), which scored the self-literal
/// glob match +1.0. The `search score` golden reverted to main's exact
/// value in the same motion. Fixtures whose terms DO own prefix extensions
/// beyond the window will pin genuinely new digests — that is the fork's
/// recall gain, not drift — so a future pin change must first prove the
/// hit-set delta is exactly the extension matches.
#[test]
fn search_sessions_on_demand_regions_equivalence() -> Result<()> {
    struct CorpusPin {
        request: Value,
        digest: &'static str,
        mode: Option<&'static str>,
        reason: Option<&'static str>,
        rows: Option<u64>,
        regions_hydrated: Option<u64>,
        messages_hydrated: Option<u64>,
        fragments_loaded: Option<u64>,
        verification_bytes: Option<u64>,
    }
    let corpus = vec![
        CorpusPin {
            // 2026-11-11 (B): the page load is gated by foo's raw Quill
            // slots — S1+S2's two chat regions alone (was 9 structural
            // rows). Both witnesses verify through the bounded-window
            // shortcut (0 verification bytes) and the snippets re-hydrate
            // the same two canonical regions (verify + snippet = 2
            // regions x 2 sessions). The answer digest is the pre-(B)
            // capture, byte-identical.
            request: json!({"query":"foo"}),
            digest: "4d2333b27872f0b1cb934401af0e286fca4abb729cf388b1cc273c7abaf9e76f",
            mode: Some("emitted_page_sessions"),
            reason: None,
            rows: Some(2),
            regions_hydrated: Some(4),
            messages_hydrated: Some(4),
            fragments_loaded: Some(0),
            verification_bytes: Some(0),
        },
        CorpusPin {
            // 2026-11-11 (B): claims-empty (a pure NOT carries no positive
            // literal claim) — the page loads NOTHING (was 16 structural
            // rows). The digest is unchanged: matches, witnesses and
            // verdicts never depended on the load.
            request: json!({"query":"NOT quux"}),
            digest: "bf524b0ce5c324a9900b4aadba5408d6e993ec2452ceaef0a63e6ed35b798c80",
            mode: Some("emitted_page_sessions"),
            reason: None,
            rows: Some(0),
            regions_hydrated: Some(0),
            messages_hydrated: Some(0),
            fragments_loaded: Some(0),
            verification_bytes: Some(0),
        },
        CorpusPin {
            // 2026-11-11 (B): fluxcap's slot is S1's notes region alone
            // (was 6 structural rows); it is fragment-backed, so it is
            // verified and snippet-hydrated through the fragment read
            // (no bounded-window shortcut for non-canonical regions, but
            // fragment-backed regions never pay verification bytes).
            request: json!({"query":"fluxcap"}),
            digest: "388baa7c176771f6fd8ba94052e654401e08991d55a5d7f5c9e590046d6cf5e6",
            mode: Some("emitted_page_sessions"),
            reason: None,
            rows: Some(1),
            regions_hydrated: Some(2),
            messages_hydrated: Some(0),
            fragments_loaded: Some(1),
            verification_bytes: Some(0),
        },
        CorpusPin {
            // Refused before evaluation: the typed error envelope is the
            // pinned answer; no diagnostics object exists to pin.
            request: json!({"query":"foo","limit":0}),
            digest: "0245e06d26c32690ebb232b6328e30b64824cda5df17b0f9f3f178699ea19297",
            mode: None,
            reason: None,
            rows: None,
            regions_hydrated: None,
            messages_hydrated: None,
            fragments_loaded: None,
            verification_bytes: None,
        },
        CorpusPin {
            // 2026-11-11 (B): the emitted page-1 session's quux slot is its
            // chat region alone (was 2 structural rows); the bounded-window
            // shortcut proves the member without full-text bytes, and the
            // snippet re-hydrates the same canonical region (verify +
            // snippet = 2 region reads, 2 message reads).
            request: json!({"query":"quux","limit":1,"sort":"updated_desc"}),
            digest: "152296651769cd7de5ced3cf40d89f9764b1757b8d9a15f458088bbc7e48ebb2",
            mode: Some("emitted_page_sessions"),
            reason: None,
            rows: Some(1),
            regions_hydrated: Some(2),
            messages_hydrated: Some(2),
            fragments_loaded: Some(0),
            verification_bytes: Some(0),
        },
        CorpusPin {
            // Two single-token conjuncts are not fast-lane composable:
            // the legacy lane reads ONLY the gated rows.
            request: json!({"query":"foo bar"}),
            digest: "8ffdad9cbe1dc6952010c3ae41ea85e43cc16b6c20efcb45429ac08a90433b86",
            mode: Some("gated_quill_seeks"),
            reason: None,
            rows: Some(3),
            regions_hydrated: Some(6),
            messages_hydrated: Some(6),
            fragments_loaded: Some(0),
            verification_bytes: Some(0),
        },
        CorpusPin {
            request: json!({"query":"\"combined request\""}),
            digest: "38efbed035f58a43d4bf4b801ded4417e381a6536b48d76bb9cb5a1f1e13d86d",
            mode: Some("gated_quill_seeks"),
            reason: None,
            rows: Some(1),
            regions_hydrated: Some(2),
            messages_hydrated: Some(2),
            fragments_loaded: Some(0),
            verification_bytes: Some(0),
        },
        CorpusPin {
            request: json!({"query":"in:thinking zeta"}),
            digest: "629cf0da4f4cad4b251a5502ac2d405cebe913cc347f3aaa01dbad4ba4c78f10",
            mode: Some("universe_structural"),
            reason: Some("region_domain_predicate"),
            rows: Some(23),
            regions_hydrated: Some(2),
            messages_hydrated: Some(0),
            fragments_loaded: Some(1),
            verification_bytes: Some(0),
        },
        CorpusPin {
            // 2026-11-11 (A): `tool:Bash ∧ quux` seeks — the quux gates load
            // their resident rows (3: S3's m2:chat + S3's tool region + S5's
            // chat region) and the tool predicate's seek re-reads S3's Bash
            // row (1) — `region_rows_loaded` counts reads, not distinct
            // rows. The answer (digest) is the Stage 1 capture,
            // byte-identical.
            request: json!({"ast":{"type":"and","children":[
                {"type":"filter","field":"tool","op":"EQ","value":"Bash"},
                {"type":"term","value":"quux"}]}}),
            digest: "585abc2da1b038f2ad113adbe5a4f94990e7c2bb613bdd12c763946bac3e41a2",
            mode: Some("block_predicate_seeks"),
            reason: None,
            rows: Some(4),
            regions_hydrated: Some(3),
            messages_hydrated: Some(1),
            fragments_loaded: Some(1),
            verification_bytes: Some(0),
        },
        CorpusPin {
            // 2026-11-11 (A): exit:2 — one filtered read over the
            // unindexed exit_code column (the seek's SQL applies the
            // comparison) returning S3's single exit row.
            request: json!({"ast":{"type":"filter","field":"exit","op":"EQ","value":2}}),
            digest: "a8e7f6fde3072ce81662cc2ab23330a268e4833b07ea8b43b53c57185afddb93",
            mode: Some("block_predicate_seeks"),
            reason: None,
            rows: Some(1),
            regions_hydrated: Some(0),
            messages_hydrated: Some(0),
            fragments_loaded: Some(0),
            verification_bytes: Some(0),
        },
        CorpusPin {
            // 2026-11-11 (A): has_mcp — the mcp-named vocabulary seek over
            // tool_name returns zero rows on this fixture (no mcp/`/`
            // tool name; S1's membership rides its `mcp-server` tag, so
            // named-reads are zero and NO probe exists for has_mcp).
            request: json!({"ast":{"type":"filter","field":"has_mcp","op":"EQ","value":true}}),
            digest: "4817f5d0dcc866e43c5acc2e9ebe8cb998d384d15eef81374dcd218b52125eb2",
            mode: Some("block_predicate_seeks"),
            reason: None,
            rows: Some(0),
            regions_hydrated: Some(0),
            messages_hydrated: Some(0),
            fragments_loaded: Some(0),
            verification_bytes: Some(0),
        },
        CorpusPin {
            // A non-ASCII literal cannot be index-gated: the whole region
            // inventory stays honest evidence (ungated structural path).
            request: json!({"query":"жжж"}),
            digest: "9798dfd5f20aea62b1ffb2f2e846b70a4b0fafc216e4e0b7b77ca6816b3736b6",
            mode: Some("universe_structural"),
            reason: Some("ungated_literal"),
            rows: Some(23),
            regions_hydrated: Some(13),
            messages_hydrated: Some(0),
            fragments_loaded: Some(6),
            verification_bytes: Some(159),
        },
        CorpusPin {
            // Zero hits: NO region row is read at all — the request's
            // region cost is zero, not the universe size.
            request: json!({"query":"wumpus"}),
            digest: "f00b3478e8790d67250cfd0a88e93acf7d59548a48ae44915c097a4622e75963",
            mode: Some("emitted_page_sessions"),
            reason: None,
            rows: Some(0),
            regions_hydrated: Some(0),
            messages_hydrated: Some(0),
            fragments_loaded: Some(0),
            verification_bytes: Some(0),
        },
        CorpusPin {
            // No literals at all: nothing is gated, nothing is read.
            request: json!({"query":null}),
            digest: "209414278977ba923879c4054d3797f887eca11d0583c5aa9abf58c5c488d230",
            mode: Some("gated_quill_seeks"),
            reason: None,
            rows: Some(0),
            regions_hydrated: Some(0),
            messages_hydrated: Some(0),
            fragments_loaded: Some(0),
            verification_bytes: Some(0),
        },
        CorpusPin {
            // 2026-11-11 (B): the tag filter narrows the page to S1, whose
            // foo slot is its chat region alone (was 6 structural rows);
            // shortcut-verified (0 bytes) + snippet re-hydration.
            request: json!({"query":"foo","filters":{"tag":"release-blocker"}}),
            digest: "f2869af8d370d4e8ba525bba30ad1d7888d80d0c1ffd6d3f798ae118af171674",
            mode: Some("emitted_page_sessions"),
            reason: None,
            rows: Some(1),
            regions_hydrated: Some(2),
            messages_hydrated: Some(2),
            fragments_loaded: Some(0),
            verification_bytes: Some(0),
        },
        // -----------------------------------------------------------------
        // 2026-11-11 strict-search-seeks (A): block-domain predicates seek
        // instead of scanning. Every pin below keeps its Stage 1 ANSWER
        // byte-for-byte (the digest is the pre-change capture); only the
        // region-read strategy moves: `region_load_mode` becomes
        // `block_predicate_seeks` and `region_rows_loaded` drops from the
        // whole universe (23 rows) to the rows the predicates' SQL seeks
        // returned — the RED assertion that failed before the change.
        // -----------------------------------------------------------------
        CorpusPin {
            // Session-domain tool predicate, no literals: no gates, no
            // probes (no session blocks tools coverage on this fixture) —
            // the only rows read are the tool_name-seek's Bash rows (S3's
            // one tool region).
            request: json!({"query":"tool:Bash"}),
            digest: "a8e7f6fde3072ce81662cc2ab23330a268e4833b07ea8b43b53c57185afddb93",
            mode: Some("block_predicate_seeks"),
            reason: None,
            rows: Some(1),
            regions_hydrated: Some(0),
            messages_hydrated: Some(0),
            fragments_loaded: Some(0),
            verification_bytes: Some(0),
        },
        CorpusPin {
            // file: — vocabulary-distinct paths matched by the unchanged
            // like_match, then one file_path seek: S7's attachment region.
            request: json!({"query":"file:evidence.png"}),
            digest: "8f076b0d07c16c6cda6855c898e6d1d7cc0b8734c9de2e048a9e780f3e275e88",
            mode: Some("block_predicate_seeks"),
            reason: None,
            rows: Some(1),
            regions_hydrated: Some(0),
            messages_hydrated: Some(0),
            fragments_loaded: Some(0),
            verification_bytes: Some(0),
        },
        CorpusPin {
            request: json!({"query":"has:error"}),
            digest: "a8e7f6fde3072ce81662cc2ab23330a268e4833b07ea8b43b53c57185afddb93",
            mode: Some("block_predicate_seeks"),
            reason: None,
            rows: Some(1),
            regions_hydrated: Some(0),
            messages_hydrated: Some(0),
            fragments_loaded: Some(0),
            verification_bytes: Some(0),
        },
        CorpusPin {
            request: json!({"query":"error:tool"}),
            digest: "a8e7f6fde3072ce81662cc2ab23330a268e4833b07ea8b43b53c57185afddb93",
            mode: Some("block_predicate_seeks"),
            reason: None,
            rows: Some(1),
            regions_hydrated: Some(0),
            messages_hydrated: Some(0),
            fragments_loaded: Some(0),
            verification_bytes: Some(0),
        },
        CorpusPin {
            // Negated tool predicate: the inner predicate's seek is the
            // whole region read; the Not collapses the sets, so the answer
            // is the complement verdict byte-for-byte.
            request: json!({"query":"NOT tool:Bash"}),
            digest: "7c0e00134812ec8a4a06d4b6ef41922ec99a0fa9ce92dc7431e69c91308c693b",
            mode: Some("block_predicate_seeks"),
            reason: None,
            rows: Some(1),
            regions_hydrated: Some(0),
            messages_hydrated: Some(0),
            fragments_loaded: Some(0),
            verification_bytes: Some(0),
        },
        CorpusPin {
            // OR mix: two distinct predicates, two seeks, both hitting the
            // same S3 region — the receipt counts the rows each seek
            // returned (2 reads of 1 physical row).
            request: json!({"query":"tool:Bash OR has:error"}),
            digest: "a8e7f6fde3072ce81662cc2ab23330a268e4833b07ea8b43b53c57185afddb93",
            mode: Some("block_predicate_seeks"),
            reason: None,
            rows: Some(2),
            regions_hydrated: Some(0),
            messages_hydrated: Some(0),
            fragments_loaded: Some(0),
            verification_bytes: Some(0),
        },
        CorpusPin {
            // Same-block AND of TWO positive seekable predicates stays on
            // the eager structural lane: the narrowed sets are exact under
            // AND only for at most one predicate atom per AND node (the
            // UP∩K cross terms of a second predicate cannot be proven by
            // value seeks), so the shape falls back and receipts why.
            request: json!({"query":"tool:Bash AND error:tool"}),
            digest: "a8e7f6fde3072ce81662cc2ab23330a268e4833b07ea8b43b53c57185afddb93",
            mode: Some("universe_structural"),
            reason: Some("block_predicate_same_block_and"),
            rows: Some(23),
            regions_hydrated: Some(0),
            messages_hydrated: Some(0),
            fragments_loaded: Some(0),
            verification_bytes: Some(0),
        },
        CorpusPin {
            // has_mcp joined with a literal rides the FAST lane: the
            // seek lane replaces the eager universe load underneath it
            // (has_mcp's `named` evidence comes from the mcp-named seek;
            // no probe — its Unknown is coverage-conditional), so the
            // region rows read are the emitted page's structural load
            // alone, and the fast-lane receipt stays
            // `emitted_page_sessions`.
            request: json!({"query":"has:mcp foo"}),
            // 2026-11-11 (B): same shape as the foo+tag pin — the page
            // narrows to S1 and foo's slot is its chat region (was 6
            // structural rows); shortcut-verified + snippet re-hydrated.
            digest: "5dd4eb83dd11db182bdd0cc42914c67ab48ad46431ef9bccf0f5a75db92ff737",
            mode: Some("emitted_page_sessions"),
            reason: None,
            rows: Some(1),
            regions_hydrated: Some(2),
            messages_hydrated: Some(2),
            fragments_loaded: Some(0),
            verification_bytes: Some(0),
        },
    ];
    let mut bridge = gold_harness()?;
    let digest_of = |page: &Value| -> Result<String> {
        let mut stripped = page.clone();
        // The top-level `id` is the harness's sequential request counter —
        // transport plumbing, not answer content. Covering it made every
        // digest depend on its POSITION in the corpus: adding the
        // block-predicate pins shifted the quux page-2 walk's id 116 -> 124
        // and broke the committed page-2 pin on a byte-identical answer
        // (reproduced on the pre-change tree: both computed
        // 78e0d8a0ac9d9d5bb0d14e01db4859da0ba0a099e3550c1cf1d030fa9cf0f2a4).
        // A reply-id echo divergence cannot hide here: `search` matches
        // replies by id, so a broken echo fails the test by timeout long
        // before any digest is compared.
        if let Some(object) = stripped.as_object_mut() {
            object.remove("id");
        }
        if let Some(result) = stripped.get_mut("result").and_then(|r| r.as_object_mut()) {
            result.remove("diagnostics");
            result.remove("archive_stamp");
            result.remove("catalog_revision");
            result.remove("index_epoch");
            result.remove("next_cursor");
            // 2026-11-13: every item now carries the bounded-preview facts
            // (`title_truncated`/`title_bytes`). No corpus title exceeds
            // the preview budget, so each item must report the
            // not-truncated pair; it is checked here and then removed,
            // which keeps every Stage 1 digest pinned byte-for-byte over
            // the rest of the answer.
            if let Some(items) = result.get_mut("items").and_then(|i| i.as_array_mut()) {
                for item in items {
                    let item = item.as_object_mut().expect("search item object");
                    ensure!(
                        // shift_remove keeps the remaining key order (the
                        // map preserves insertion order; remove() swaps).
                        item.shift_remove("title_truncated") == Some(json!(false))
                            && item.shift_remove("title_bytes") == Some(Value::Null),
                        "corpus item must report an untruncated title preview: {item:?}"
                    );
                }
            }
        }
        Ok(blake3::hash(&serde_json::to_vec(&stripped)?)
            .to_hex()
            .to_string())
    };
    for pin in &corpus {
        let page = search(&mut bridge, pin.request.clone())?;
        let digest = digest_of(&page)?;
        ensure!(
            digest == pin.digest,
            "Stage 1b diverged from the Stage 1 answer for {:?} (computed {digest}): {page}",
            pin.request
        );
        let diag = &page["result"]["diagnostics"];
        if let Some(mode) = pin.mode {
            ensure!(
                diag["region_load_mode"] == json!(mode),
                "region_load_mode receipt for {:?}: {page}",
                pin.request
            );
            ensure!(
                match pin.reason {
                    Some(reason) => diag["region_load_reason"] == json!(reason),
                    None => diag["region_load_reason"].is_null(),
                },
                "region_load_reason receipt for {:?}: {page}",
                pin.request
            );
            ensure!(
                diag["region_rows_loaded"].as_u64() == pin.rows,
                "region_rows_loaded receipt for {:?}: {page}",
                pin.request
            );
            ensure!(
                diag["regions_hydrated"].as_u64() == pin.regions_hydrated
                    && diag["messages_hydrated"].as_u64() == pin.messages_hydrated
                    && diag["fragments_loaded"].as_u64() == pin.fragments_loaded
                    && diag["verification_bytes_scanned"].as_u64() == pin.verification_bytes,
                "hydration counters must stay identical to Stage 1 for {:?}: {page}",
                pin.request
            );
        } else {
            ensure!(
                page["ok"] == false && diag.is_null(),
                "the refused shape must carry no diagnostics: {page}"
            );
        }
    }
    // Cursor page 2 of the quux walk: the page-2 session's regions load at
    // ITS page time — resumption answers are byte-identical too.
    let page1 = search(
        &mut bridge,
        json!({"query":"quux","limit":1,"sort":"updated_desc"}),
    )?;
    let cursor = page1["result"]["next_cursor"]
        .as_str()
        .map(str::to_owned)
        .expect("page 1 cursor");
    let page2 = search(
        &mut bridge,
        json!({"query":"quux","limit":1,"sort":"updated_desc","cursor":cursor}),
    )?;
    let digest2 = digest_of(&page2)?;
    ensure!(
        // 2026-11-11 (A): re-pinned id-free (the digest_of change above) —
        // the answer content is the committed Stage 1 page-2 capture.
        digest2 == "b193d1fca4f8ee9c9810ca473c2dc2731427230e0fd0afe08ac378b04f90b6e5",
        "Stage 1b diverged from the Stage 1 page-2 answer (computed {digest2}): {page2}"
    );
    ensure!(
        page2["result"]["diagnostics"]["region_load_mode"] == json!("emitted_page_sessions")
            // 2026-11-11 (B): page 2's emitted session reads only its
            // quux-gated rows — the chat region + the tool region (was 5
            // structural rows). The resumption answer (digest2) is
            // unchanged.
            && page2["result"]["diagnostics"]["region_rows_loaded"].as_u64() == Some(2),
        "page 2 must load only its own session's gated regions: {page2}"
    );
    bridge.shutdown()
}

// ---------------------------------------------------------------------------
// Cooperative deadline: typed, retryable refusal — never exit(70)
// ---------------------------------------------------------------------------

/// An expired request deadline must answer THAT request with the typed
/// `deadline_exceeded` refusal — `retryable: true`, honest about the
/// incomplete enumeration, naming the phase it died in — and the same
/// bridge process must keep serving the next requests normally. The
/// refusal is a per-request answer; the process-killing watchdog stays
/// only as the last-resort backstop (see
/// `hard_backstop_still_kills_a_genuinely_stuck_request` in
/// viewer_bridge_extensions.rs). `budget.deadline_ms: 0` is the
/// wire-native way to force expiry: it refuses at the first cooperative
/// boundary, deterministically.
#[test]
fn search_sessions_deadline_refusal_is_typed_retryable_and_process_survives() -> Result<()> {
    let mut bridge = gold_harness()?;
    let refused = search(
        &mut bridge,
        json!({"query":"foo","budget":{"deadline_ms":0}}),
    )?;
    ensure!(
        refused["ok"] == false,
        "an expired deadline must refuse, never answer partially: {refused}"
    );
    ensure!(
        refused["error"]["kind"] == "deadline_exceeded",
        "expected the typed deadline_exceeded refusal, got {refused}"
    );
    ensure!(
        refused["error"]["retryable"] == true,
        "a deadline refusal is load-proportional and must be retryable: {refused}"
    );
    ensure!(
        refused["error"]["enumeration_complete"] == false,
        "a deadline refusal must never claim a complete enumeration: {refused}"
    );
    let message = refused["error"]["message"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    ensure!(
        message.contains("phase"),
        "the refusal must name the phase and completed work: {refused}"
    );
    // Process survival: the very next requests on the SAME stdio session
    // must succeed — one slow search must not take down catalog polling,
    // session reads or the other search lane (the exit-70 behavior this
    // replaces killed every lane until the supervisor restarted the bridge).
    let hello = bridge.exchange(json!({"op":"hello"}))?;
    ensure!(
        hello["ok"] == true,
        "hello must serve after a refusal: {hello}"
    );
    let catalog = bridge.exchange(json!({"op":"catalog_page","limit":10}))?;
    ensure!(
        catalog["ok"] == true,
        "catalog polling must serve after a refusal: {catalog}"
    );
    let session = bridge.exchange(json!({"op":"session","conversation_id":1}))?;
    ensure!(
        session["ok"] == true,
        "session reads must serve after a refusal: {session}"
    );
    let page = search(&mut bridge, json!({"query":"foo"}))?;
    assert_members(&page, &[1, 2])?;
    let rank = bridge.exchange(json!({"op":"rank_begin","query":"foo"}))?;
    ensure!(
        rank["ok"] == true,
        "rank must serve after a refusal: {rank}"
    );
    // The handshake documents the deadline contract: the backstop margin
    // and the retryable refusal kinds are client-visible.
    let limits = &hello["result"]["limits"];
    ensure!(
        limits["hard_backstop_grace_ms"].as_u64().is_some(),
        "hello limits must document the backstop margin: {hello}"
    );
    ensure!(
        limits["retryable_refusal_kinds"]
            .as_array()
            .is_some_and(|kinds| kinds.iter().any(|kind| kind == "deadline_exceeded")),
        "hello limits must document the retryable refusal kinds: {hello}"
    );
    bridge.shutdown()
}

/// The `search_sessions` deadline budget may only LOWER the profile limit:
/// a budget above `TIMEOUT_MS` is refused as `invalid_request`, while the
/// full `TIMEOUT_MS` budget answers normally. `0` (first-boundary refusal)
/// is legal — see the survival test above.
#[test]
fn search_sessions_deadline_budget_only_lowers_the_profile_limit() -> Result<()> {
    let mut bridge = gold_harness()?;
    let too_big = search(
        &mut bridge,
        json!({"query":"foo","budget":{"deadline_ms":10_001}}),
    )?;
    ensure!(
        too_big["ok"] == false,
        "an over-profile deadline budget must refuse: {too_big}"
    );
    ensure!(
        too_big["error"]["kind"] == "invalid_request",
        "over-profile budget must refuse as invalid_request, got {too_big}"
    );
    let full = search(
        &mut bridge,
        json!({"query":"foo","budget":{"deadline_ms":10_000}}),
    )?;
    assert_members(&full, &[1, 2])?;
    bridge.shutdown()
}

#[test]
fn search_sessions_oversized_title_lists_with_bounded_preview() -> Result<()> {
    let mut bridge = gold_harness()?;
    // One session whose title is >1 MiB must not stall or refuse the page.
    let huge_len = 1_200_006_i64;
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer.execute_with_params(
        "UPDATE conversations SET title=?1 WHERE id=1",
        &param_slice_to_values(params![format!("{} alpha", "t".repeat(1_200_000))]),
    )?;
    drop(writer);
    let page = search(&mut bridge, json!({"query":"foo"}))?;
    ensure!(
        page["ok"] == true,
        "oversized title must not refuse/stall: {page}"
    );
    let items = page["result"]["items"].as_array().expect("items");
    let s1 = items
        .iter()
        .find(|i| i["conversation_id"] == json!(1))
        .expect("S1 must survive the page");
    let title = s1["title"].as_str().expect("title");
    ensure!(
        title.len() <= 16 * 1024,
        "bounded title, got {}",
        title.len()
    );
    ensure!(
        s1["title_truncated"] == json!(true),
        "truncation fact: {s1}"
    );
    ensure!(
        s1["title_bytes"] == json!(huge_len),
        "original length: {s1}"
    );
    bridge.shutdown()
}
