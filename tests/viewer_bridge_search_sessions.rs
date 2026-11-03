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
use coding_agent_search::franken_sync::compat::param_slice_to_values;
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

/// Same publication as [`gold_harness`] over an arbitrary fixture set —
/// the seven gold sessions plus any case-specific extras — and optional
/// bridge args (e.g. `--enable-viewer-writes` for the annotation lane).
fn gold_harness_full(fixture: &[GoldSession], extra_args: &[&str]) -> Result<BridgeHarness> {
    let temp = tempfile::tempdir()?;
    let data_dir = temp.path().join("data");
    std::fs::create_dir(&data_dir)?;
    let db = data_dir.join("archive.db");
    {
        let storage = FrankenStorage::open(&db)?;
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
        drop(storage);
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
        let generation = coding_agent_search::viewer_extensions::projection::generation_for_canonical(
            &writer,
            &conv,
            session.id,
        )?;
        coding_agent_search::viewer_extensions::search_regions::publish_search_regions(
            &writer,
            session.id,
            &generation.packet,
            &generation.structure,
        )?;
    }
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
        for session in fixture {
            coding_agent_search::viewer_extensions::search_regions::publish_region_docs(
                &mut index,
                &writer,
                session.id,
            )?;
        }
        index.commit()?;
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
        matches
            .iter()
            .any(|m| m["message_idx"].as_i64() == Some(0)),
        "result-first correlation anchors the result coordinate: {matches:?}"
    );

    // Reused call id: the second call's region survives with its own text.
    let page = search(
        &mut bridge,
        json!({"query":"in:tools seconddupcommand"}),
    )?;
    assert_members(&page, &[8])?;
    let page = search(
        &mut bridge,
        json!({"query":"in:tools firstdupcommand"}),
    )?;
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
        matches
            .iter()
            .any(|m| m["message_idx"].as_i64() == Some(0)),
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

