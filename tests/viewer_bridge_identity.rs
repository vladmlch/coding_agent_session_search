//! Task 3: stable, source-evidence-bound public session identity for the
//! CASS-owned Viewer backend.
//!
//! `resolve_public_id` derives the qualified public CID
//! (`{provider}:{source_cid}`) from canonical provenance columns only —
//! provider-specific rules mirror the Viewer's identity oracle — and
//! `resolve_conversation_id` inverts it through the canonical provenance
//! index. Ambiguous or unverifiable evidence is a typed `identity_conflict`;
//! `conversation_id` is never a public ID and basenames alone are never
//! evidence.
//!
//! `src/viewer_bridge` is compiled into the bridge binary (not the library),
//! so this test includes the pure modules it exercises directly.
#[path = "../src/viewer_bridge/extra.rs"]
#[allow(dead_code)]
mod extra;
#[path = "../src/viewer_bridge/identity.rs"]
#[allow(dead_code)]
mod identity;
#[path = "../src/viewer_bridge/protocol.rs"]
#[allow(dead_code)]
mod protocol;

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Result, ensure};
use coding_agent_search::franken_sync::Connection;
use coding_agent_search::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use coding_agent_search::storage::sqlite::FrankenStorage;
use serde_json::{Value, json};

struct FixtureRow {
    agent: &'static str,
    source_id: &'static str,
    external_id: Option<&'static str>,
    source_path: &'static str,
    origin_host: Option<&'static str>,
    metadata: Value,
}

impl FixtureRow {
    fn new(
        agent: &'static str,
        source_path: &'static str,
        external_id: Option<&'static str>,
    ) -> Self {
        Self {
            agent,
            source_id: "local",
            external_id,
            source_path,
            origin_host: None,
            metadata: json!(null),
        }
    }

    fn remote(mut self, source_id: &'static str, origin_host: &'static str) -> Self {
        self.source_id = source_id;
        self.origin_host = Some(origin_host);
        self
    }

    fn metadata(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }
}

struct IdentityFixture {
    _temp: tempfile::TempDir,
    db: PathBuf,
}

impl IdentityFixture {
    fn build(rows: &[FixtureRow]) -> Result<Self> {
        let temp = tempfile::tempdir()?;
        let db = temp.path().join("archive.db");
        let storage = FrankenStorage::open(&db)?;
        let mut agents: HashMap<&str, i64> = HashMap::new();
        for row in rows {
            let agent_id = match agents.get(row.agent) {
                Some(id) => *id,
                None => {
                    let id = storage.ensure_agent(&Agent {
                        id: None,
                        slug: row.agent.into(),
                        name: row.agent.into(),
                        version: None,
                        kind: AgentKind::Cli,
                    })?;
                    agents.insert(row.agent, id);
                    id
                }
            };
            storage.insert_conversation_tree(
                agent_id,
                None,
                &Conversation {
                    id: None,
                    agent_slug: row.agent.into(),
                    workspace: None,
                    external_id: row.external_id.map(str::to_owned),
                    title: Some("fixture".into()),
                    source_path: PathBuf::from(row.source_path),
                    started_at: None,
                    ended_at: None,
                    approx_tokens: None,
                    metadata_json: row.metadata.clone(),
                    messages: vec![Message {
                        id: None,
                        idx: 0,
                        role: MessageRole::Agent,
                        author: None,
                        created_at: Some(1_700_000_000_000),
                        content: "fixture".into(),
                        extra_json: json!(null),
                        snippets: Vec::new(),
                    }],
                    source_id: row.source_id.into(),
                    origin_host: row.origin_host.map(str::to_owned),
                },
            )?;
        }
        drop(storage);
        Ok(Self { _temp: temp, db })
    }

    fn connect(&self) -> Result<Connection> {
        Ok(Connection::open(self.db.to_string_lossy().into_owned())?)
    }
}

fn refusal_kind(error: &anyhow::Error) -> Option<&'static str> {
    error.downcast_ref::<protocol::Refusal>().map(|r| r.kind)
}

const CLAUDE_UUID: &str = "11111111-2222-3333-4444-555555555555";
const CODEX_UUID: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

#[test]
fn claude_local_identity_round_trips_both_directions() -> Result<()> {
    let fixture = IdentityFixture::build(&[FixtureRow::new(
        "claude",
        "/home/u/.claude/projects/p/11111111-2222-3333-4444-555555555555.jsonl",
        Some(CLAUDE_UUID),
    )])?;
    let conn = fixture.connect()?;

    let resolved = identity::resolve_public_id(&conn, 1)?;
    ensure!(resolved.provider == "claude", "provider: {resolved:?}");
    ensure!(resolved.source_id == "local", "source: {resolved:?}");
    ensure!(
        resolved.public_id == format!("claude:{CLAUDE_UUID}"),
        "public id: {resolved:?}"
    );

    let conversation = identity::resolve_conversation_id(&conn, &resolved.public_id)?;
    ensure!(conversation == Some(1), "reverse lookup: {conversation:?}");

    // The bare native CID also resolves inside its owning provider.
    ensure!(identity::resolve_conversation_id(&conn, &format!("claude:{CLAUDE_UUID}"))? == Some(1));
    Ok(())
}

#[test]
fn codex_rollout_extracts_thread_uuid_from_path_form_external_id() -> Result<()> {
    let fixture = IdentityFixture::build(&[FixtureRow::new(
        "codex",
        "/home/u/.codex/sessions/2024/05/06/rollout-2024-05-06T10-20-30-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.jsonl",
        // The pinned connector stores a sessions-relative path WITHOUT the
        // extension, unlike source_path (viewer evidence contract).
        Some(
            "sessions/2024/05/06/rollout-2024-05-06T10-20-30-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        ),
    )])?;
    let conn = fixture.connect()?;

    let resolved = identity::resolve_public_id(&conn, 1)?;
    ensure!(resolved.provider == "codex");
    ensure!(
        resolved.public_id == format!("codex:{CODEX_UUID}"),
        "public id: {resolved:?}"
    );
    ensure!(identity::resolve_conversation_id(&conn, &resolved.public_id)? == Some(1));
    Ok(())
}

#[test]
fn devin_container_selector_binds_composite_native_id() -> Result<()> {
    let fixture = IdentityFixture::build(&[FixtureRow::new(
        "devin",
        "/state/devin-work.db#agent-9",
        Some("devin:devin-work:agent-9"),
    )])?;
    let conn = fixture.connect()?;

    let resolved = identity::resolve_public_id(&conn, 1)?;
    ensure!(resolved.provider == "devin");
    ensure!(
        resolved.public_id == "devin:devin:devin-work:agent-9",
        "public id: {resolved:?}"
    );
    ensure!(identity::resolve_conversation_id(&conn, &resolved.public_id)? == Some(1));
    Ok(())
}

#[test]
fn antigravity_opaque_external_id_is_the_native_cid() -> Result<()> {
    let fixture = IdentityFixture::build(&[FixtureRow::new(
        "antigravity",
        "/home/u/.gemini/antigravity/brain/session-abc/impl",
        Some("session-abc"),
    )])?;
    let conn = fixture.connect()?;
    let resolved = identity::resolve_public_id(&conn, 1)?;
    ensure!(resolved.provider == "antigravity");
    ensure!(resolved.public_id == "antigravity:session-abc");
    ensure!(identity::resolve_conversation_id(&conn, "antigravity:session-abc")? == Some(1));
    Ok(())
}

#[test]
fn remote_sources_get_source_qualified_public_ids_without_colliding() -> Result<()> {
    // The same native ID on the local machine and two remote sources must
    // produce three distinct public IDs; remote IDs encode their origin.
    let fixture = IdentityFixture::build(&[
        FixtureRow::new("claude", "/sessions/native-9.jsonl", Some("native-9")),
        FixtureRow::new("claude", "/sessions/native-9.jsonl", Some("native-9"))
            .remote("laptop-a", "laptop-a"),
        FixtureRow::new("claude", "/sessions/native-9.jsonl", Some("native-9"))
            .remote("laptop-b", "laptop-b"),
    ])?;
    let conn = fixture.connect()?;

    let local = identity::resolve_public_id(&conn, 1)?;
    let remote_a = identity::resolve_public_id(&conn, 2)?;
    let remote_b = identity::resolve_public_id(&conn, 3)?;
    ensure!(local.public_id == "claude:native-9", "local: {local:?}");
    ensure!(remote_a.source_id == "laptop-a");
    ensure!(remote_b.source_id == "laptop-b");
    ensure!(
        remote_a.public_id != remote_b.public_id,
        "distinct remote sources must not collide: {remote_a:?} vs {remote_b:?}"
    );
    ensure!(
        remote_a.public_id.starts_with("claude:cassremote:")
            && remote_b.public_id.starts_with("claude:cassremote:"),
        "remote identity must encode its source namespace"
    );
    ensure!(
        local.public_id != remote_a.public_id && local.public_id != remote_b.public_id,
        "remote rows must never borrow the local namespace"
    );

    ensure!(identity::resolve_conversation_id(&conn, &remote_a.public_id)? == Some(2));
    ensure!(identity::resolve_conversation_id(&conn, &remote_b.public_id)? == Some(3));
    ensure!(identity::resolve_conversation_id(&conn, "claude:native-9")? == Some(1));
    Ok(())
}

#[test]
fn local_and_remote_same_native_id_do_not_cross_resolve() -> Result<()> {
    let fixture = IdentityFixture::build(&[
        FixtureRow::new("antigravity", "/dup/s.bin", Some("dup-ext")),
        FixtureRow::new("antigravity", "/dup/s.bin", Some("dup-ext")).remote("office", "office"),
    ])?;
    let conn = fixture.connect()?;
    let local = identity::resolve_public_id(&conn, 1)?;
    let remote = identity::resolve_public_id(&conn, 2)?;
    ensure!(local.public_id == "antigravity:dup-ext");
    ensure!(remote.public_id.starts_with("antigravity:cassremote:"));
    // Neither direction may cross the source boundary.
    ensure!(identity::resolve_conversation_id(&conn, "antigravity:dup-ext")? == Some(1));
    ensure!(identity::resolve_conversation_id(&conn, &remote.public_id)? == Some(2));
    Ok(())
}

#[test]
fn ambiguous_claude_evidence_is_a_typed_identity_conflict() -> Result<()> {
    // Path stem and external_id disagree: no verified owner is possible.
    let fixture = IdentityFixture::build(&[FixtureRow::new(
        "claude",
        "/sessions/aaa-111.jsonl",
        Some("bbb-222"),
    )])?;
    let conn = fixture.connect()?;
    let error = identity::resolve_public_id(&conn, 1).unwrap_err();
    ensure!(
        refusal_kind(&error) == Some("identity_conflict"),
        "expected identity_conflict, got {error:?}"
    );

    // The row plausibly claims the queried ID via external_id, so a reverse
    // lookup must surface the conflict rather than silently missing it.
    let error = identity::resolve_conversation_id(&conn, "claude:bbb-222").unwrap_err();
    ensure!(
        refusal_kind(&error) == Some("identity_conflict"),
        "expected identity_conflict, got {error:?}"
    );
    Ok(())
}

#[test]
fn claude_unverified_subagent_and_desktop_shapes_conflict() -> Result<()> {
    let fixture = IdentityFixture::build(&[
        // agent-* files without verified sidechain metadata are child
        // transcripts whose native identity needs real source evidence.
        FixtureRow::new("claude", "/p/x/agent-deadbeef.jsonl", None),
        // local_* desktop containers likewise refuse without verification.
        FixtureRow::new("claude", "/p/local_abc123.json", None),
    ])?;
    let conn = fixture.connect()?;
    for id in 1..=2 {
        let error = identity::resolve_public_id(&conn, id).unwrap_err();
        ensure!(
            refusal_kind(&error) == Some("identity_conflict"),
            "conversation {id}: expected identity_conflict, got {error:?}"
        );
    }
    Ok(())
}

#[test]
fn claude_verified_sidechain_child_gets_compound_native_id() -> Result<()> {
    let fixture = IdentityFixture::build(&[FixtureRow::new(
        "claude",
        "/home/u/.claude/projects/p/99999999-parent/subagents/agent-c0ffee.jsonl",
        None,
    )
    .metadata(json!({
        "source": "claude_code",
        "sidechain": true,
        "parentSessionId": "99999999-parent",
    }))])?;
    let conn = fixture.connect()?;
    let resolved = identity::resolve_public_id(&conn, 1)?;
    ensure!(
        resolved.public_id == "claude:99999999-parent--agent-c0ffee",
        "verified child id: {resolved:?}"
    );
    ensure!(identity::resolve_conversation_id(&conn, &resolved.public_id)? == Some(1));
    Ok(())
}

#[test]
fn unverifiable_generic_connector_is_identity_conflict_not_basename() -> Result<()> {
    // No external_id, no embedded/verified evidence: the basename must never
    // be promoted into a public ID.
    let fixture = IdentityFixture::build(&[
        FixtureRow::new("antigravity", "/brain/session-zzz/impl.pb", None),
        // A codex source without a rollout filename or UUID external_id is
        // likewise unverifiable.
        FixtureRow::new("codex", "/codex/sessions/notes.txt", Some("not-a-uuid")),
    ])?;
    let conn = fixture.connect()?;
    for id in 1..=2 {
        let error = identity::resolve_public_id(&conn, id).unwrap_err();
        ensure!(
            refusal_kind(&error) == Some("identity_conflict"),
            "conversation {id}: expected identity_conflict, got {error:?}"
        );
    }
    Ok(())
}

#[test]
fn provenance_contradictions_are_provenance_conflict_not_identity() -> Result<()> {
    // A 'local'-kind source claiming a remote source_id disagrees with itself.
    let fixture = IdentityFixture::build(&[
        FixtureRow::new("claude", "/s/r1.jsonl", Some("r1")).remote("edge-1", "edge-1")
    ])?;
    let conn = fixture.connect()?;
    conn.execute("UPDATE sources SET kind='local' WHERE id='edge-1'")?;
    let error = identity::resolve_public_id(&conn, 1).unwrap_err();
    ensure!(
        refusal_kind(&error) == Some("provenance_conflict"),
        "expected provenance_conflict, got {error:?}"
    );
    Ok(())
}

#[test]
fn local_source_with_origin_host_is_provenance_conflict() -> Result<()> {
    let fixture = IdentityFixture::build(&[FixtureRow::new("claude", "/s/h1.jsonl", Some("h1"))])?;
    let conn = fixture.connect()?;
    conn.execute("UPDATE conversations SET origin_host='ghost' WHERE id=1")?;
    let error = identity::resolve_public_id(&conn, 1).unwrap_err();
    ensure!(
        refusal_kind(&error) == Some("provenance_conflict"),
        "expected provenance_conflict, got {error:?}"
    );
    Ok(())
}

#[test]
fn reverse_lookup_unknown_and_malformed_inputs_are_typed() -> Result<()> {
    let fixture = IdentityFixture::build(&[FixtureRow::new(
        "claude",
        "/sessions/known.jsonl",
        Some("known"),
    )])?;
    let conn = fixture.connect()?;

    ensure!(identity::resolve_conversation_id(&conn, "claude:missing")? == None);
    ensure!(identity::resolve_conversation_id(&conn, "junie:whatever")? == None);
    // An unregistered prefix is never de-qualified: the whole input is a bare
    // CID and simply matches nothing.
    ensure!(identity::resolve_conversation_id(&conn, "UPPER BAD:id")? == None);

    for malformed in [
        "",
        "claude:",
        ":",
        "cassremote:x:y",
        "claude:cassremote:BAD",
    ] {
        let error = identity::resolve_conversation_id(&conn, malformed).unwrap_err();
        ensure!(
            refusal_kind(&error) == Some("invalid_request"),
            "input {malformed:?}: expected invalid_request, got {error:?}"
        );
    }
    Ok(())
}

#[test]
fn bare_native_id_resolves_only_when_unambiguous() -> Result<()> {
    let fixture = IdentityFixture::build(&[
        FixtureRow::new("claude", "/c/dup-1.jsonl", Some("dup-1")),
        // A different provider claiming the same bare native ID is ambiguous.
        FixtureRow::new("junie", "/j/x.bin", Some("dup-1")),
        FixtureRow::new("claude", "/c/solo.jsonl", Some("solo")),
    ])?;
    let conn = fixture.connect()?;
    ensure!(identity::resolve_conversation_id(&conn, "solo")? == Some(3));
    let error = identity::resolve_conversation_id(&conn, "dup-1").unwrap_err();
    ensure!(
        refusal_kind(&error) == Some("identity_conflict"),
        "expected identity_conflict, got {error:?}"
    );
    Ok(())
}

#[test]
fn multi_chunk_same_public_id_collapses_to_lowest_conversation_id() -> Result<()> {
    // Two codex rollout chunks of one thread under one dated root share the
    // native UUID: both rows verify to one public ID, bound to the lowest id.
    let fixture = IdentityFixture::build(&[
        FixtureRow::new(
            "codex",
            "/s/2024/05/06/rollout-2024-05-06T10-00-00-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.jsonl",
            Some(
                "sessions/2024/05/06/rollout-2024-05-06T10-00-00-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            ),
        ),
        FixtureRow::new(
            "codex",
            "/s/2024/05/06/rollout-2024-05-06T11-00-00-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.jsonl",
            Some(
                "sessions/2024/05/06/rollout-2024-05-06T11-00-00-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            ),
        ),
    ])?;
    let conn = fixture.connect()?;
    for id in 1..=2 {
        let resolved = identity::resolve_public_id(&conn, id)?;
        ensure!(resolved.public_id == format!("codex:{CODEX_UUID}"));
    }
    ensure!(
        identity::resolve_conversation_id(&conn, &format!("codex:{CODEX_UUID}"))? == Some(1),
        "equal identities bind to the lowest canonical row"
    );
    Ok(())
}

#[test]
fn conversation_id_never_leaks_into_public_identity() -> Result<()> {
    let fixture =
        IdentityFixture::build(&[FixtureRow::new("claude", "/c/pub-1.jsonl", Some("pub-1"))])?;
    let conn = fixture.connect()?;
    let resolved = identity::resolve_public_id(&conn, 1)?;
    ensure!(resolved.public_id != "1" && !resolved.public_id.ends_with(":1"));
    // And the reverse direction never treats a bare numeric as an ID either.
    ensure!(identity::resolve_conversation_id(&conn, "1")? == None);
    // Missing canonical row is a typed refusal, not a fabricated identity.
    let error = identity::resolve_public_id(&conn, 999).unwrap_err();
    ensure!(
        refusal_kind(&error).is_some(),
        "expected typed refusal: {error:?}"
    );
    Ok(())
}

#[test]
fn both_lookup_directions_use_provenance_indexes() -> Result<()> {
    let fixture = IdentityFixture::build(&[FixtureRow::new("claude", "/c/ix.jsonl", Some("ix"))])?;
    let conn = fixture.connect()?;
    let plan_rows = |sql: &str| -> Result<Vec<String>> {
        let mut lines = Vec::new();
        for row in conn.query(&format!("EXPLAIN QUERY PLAN {sql}"))? {
            use coding_agent_search::franken_sync::compat::RowExt;
            lines.push(row.get_typed::<String>(3)?);
        }
        Ok(lines)
    };
    let forward = plan_rows(identity::FORWARD_ROW_SQL)?;
    eprintln!("forward plan: {forward:?}");
    ensure!(
        forward
            .iter()
            .any(|line| { line.contains("SEARCH conversations USING INTEGER PRIMARY KEY") }),
        "forward lookup must be a primary-key probe, not a scan: {forward:?}"
    );
    let reverse = plan_rows(identity::REVERSE_CANDIDATE_SQL)?;
    eprintln!("reverse plan: {reverse:?}");
    ensure!(
        reverse
            .iter()
            .all(|line| !line.starts_with("SCAN conversations")),
        "reverse lookup must not scan the catalog: {reverse:?}"
    );
    ensure!(
        reverse.iter().any(|line| {
            line.contains("SEARCH conversations")
                && (line.contains("idx_conversations_source_id")
                    || line.contains("idx_conversations_provenance"))
        }),
        "reverse lookup must use a provenance index: {reverse:?}"
    );
    Ok(())
}
