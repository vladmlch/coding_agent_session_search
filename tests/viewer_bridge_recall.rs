#[path = "viewer_bridge_support/mod.rs"]
mod support;

use std::collections::BTreeSet;
use anyhow::Result;
use serde_json::json;
use support::{BridgeHarness, FixtureConversation};

#[test]
fn matching_session_beyond_stock_1024_window_is_not_lost() -> Result<()> {
    let mut bridge = BridgeHarness::with_conversations(&[
        FixtureConversation::repeated(1, "local", "needle", 1500),
        FixtureConversation::repeated(2, "local", "needle with many unrelated filler words", 1),
    ])?;
    let hits = bridge.enumerate_all("needle", 50)?;
    assert_eq!(hits.len(), 1501);
    assert_eq!(hits.iter().map(|hit| hit.conversation_id).collect::<BTreeSet<_>>(), [1,2].into_iter().collect());
    assert_eq!(hits.iter().map(|hit| (&hit.source_id,hit.conversation_id,hit.message_index)).collect::<BTreeSet<_>>().len(), 1501);
    assert!(hits.iter().all(|hit| hit.score.is_finite()));
    bridge.shutdown()
}

#[test]
fn every_federated_reader_preserves_provenance_and_raw_ties() -> Result<()> {
    let mut bridge = BridgeHarness::with_federated(&[
        FixtureConversation::repeated(1, "local", "needle", 301),
        FixtureConversation::repeated(2, "remote:δ", "needle", 301),
    ])?;
    let hits = bridge.enumerate_all("needle", 17)?;
    assert_eq!(hits.len(), 602);
    assert_eq!(hits.iter().map(|hit| &hit.reader_key).collect::<BTreeSet<_>>().len(), 2);
    assert_eq!(hits.iter().map(|hit| hit.source_id.as_str()).collect::<BTreeSet<_>>(), ["local","remote:δ"].into_iter().collect());
    bridge.shutdown()
}

#[test]
fn reserved_local_filter_is_case_insensitive_and_remote_ids_keep_their_case() -> Result<()> {
    let mut bridge = BridgeHarness::with_conversations(&[
        FixtureConversation::repeated(1, "local", "needle", 1),
        FixtureConversation::repeated(2, "Remote:MiXeD", "needle", 1),
        FixtureConversation::repeated(3, "remote:mixed", "needle", 1),
    ])?;
    for (source_filter, source_id, conversation_id) in [
        ("local", "local", 1),
        ("LOCAL", "local", 1),
        ("LoCaL", "local", 1),
        ("Remote:MiXeD", "Remote:MiXeD", 2),
        ("remote:mixed", "remote:mixed", 3),
    ] {
        let reply = bridge.exchange(json!({
            "op":"rank_begin","query":"needle","filters":{"source_id":source_filter}
        }))?;
        assert_eq!(reply["ok"], true);
        let page = &reply["result"];
        assert_eq!(page["enumeration_complete"], true);
        assert_eq!(page["exact_raw_total"], 1);
        assert!(page["next_cursor"].is_null());
        assert_eq!(page["hits"].as_array().map(Vec::len), Some(1));
        assert_eq!(page["hits"][0]["source_id"], source_id);
        assert_eq!(page["hits"][0]["conversation_id"], conversation_id);
    }
    bridge.shutdown()
}

#[test]
fn budget_refusal_is_an_error_and_reload_expires_the_pinned_cursor() -> Result<()> {
    let mut bridge = BridgeHarness::with_conversations(&[
        FixtureConversation::repeated(1, "local", "needle", 300),
    ])?;
    let budget = bridge.exchange(json!({"op":"rank_begin","query":"needle","budget":{"max_raw_hits":100,"deadline_ms":10000}}))?;
    assert_eq!(budget["ok"], false);
    assert_eq!(budget["error"]["kind"], "ranking_budget_exceeded");
    assert_eq!(budget["error"]["enumeration_complete"], false);
    let first = bridge.exchange(json!({"op":"rank_begin","query":"needle"}))?;
    assert_eq!(first["ok"], true);
    assert_eq!(first["result"]["enumeration_complete"], false);
    let token = first["result"]["next_cursor"].clone();
    let old_epoch = first["result"]["index_epoch"].clone();
    let reload = bridge.exchange(json!({"op":"reload"}))?;
    assert_ne!(reload["result"]["index_epoch"], old_epoch);
    let stale = bridge.exchange(json!({"op":"rank_next","cursor":token,"max_hits":50}))?;
    assert_eq!(stale["ok"], false);
    assert_eq!(stale["error"]["kind"], "stale_snapshot");
    bridge.shutdown()
}

#[test]
fn cancelled_cursor_cannot_continue_under_a_new_query() -> Result<()> {
    let mut bridge = BridgeHarness::with_conversations(&[
        FixtureConversation::repeated(1, "local", "needle", 257),
    ])?;
    let first = bridge.exchange(json!({"op":"rank_begin","query":"needle"}))?;
    let token = first["result"]["next_cursor"].clone();
    let cancelled = bridge.exchange(json!({"op":"rank_cancel","cursor":token}))?;
    assert_eq!(cancelled["result"]["cancelled"], true);
    let stale = bridge.exchange(json!({"op":"rank_next","cursor":token,"max_hits":1}))?;
    assert_eq!(stale["ok"], false);
    let empty = bridge.exchange(json!({"op":"rank_begin","query":"absent"}))?;
    assert_eq!(empty["result"]["enumeration_complete"], true);
    assert_eq!(empty["result"]["exact_raw_total"], 0);
    bridge.shutdown()
}

#[test]
fn canonical_commit_between_raw_pages_cannot_masquerade_as_one_snapshot() -> Result<()> {
    use coding_agent_search::franken_sync::Connection;
    let mut bridge = BridgeHarness::with_conversations(&[
        FixtureConversation::repeated(1, "local", "needle", 300),
    ])?;
    let first = bridge.exchange(json!({"op":"rank_begin","query":"needle"}))?;
    assert_eq!(first["ok"], true);
    let token = first["result"]["next_cursor"].clone();
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer.execute("UPDATE conversations SET title='changed' WHERE id=1")?;
    drop(writer);
    let changed = bridge.exchange(json!({"op":"rank_next","cursor":token,"max_hits":1}))?;
    assert_eq!(changed["ok"], false);
    assert_eq!(changed["error"]["kind"], "stale_snapshot");
    bridge.shutdown()
}

#[test]
fn raw_cursor_capacity_and_page_bounds_are_enforced() -> Result<()> {
    let mut bridge = BridgeHarness::with_conversations(&[
        FixtureConversation::repeated(1, "local", "needle", 300),
    ])?;
    let mut cursors = Vec::new();
    for _ in 0..8 {
        let page = bridge.exchange(json!({"op":"rank_begin","query":"needle"}))?;
        assert_eq!(page["ok"], true);
        cursors.push(page["result"]["next_cursor"].clone());
    }
    let refused = bridge.exchange(json!({"op":"rank_begin","query":"needle"}))?;
    assert_eq!(refused["ok"], false);
    assert_eq!(refused["error"]["kind"], "ranking_budget_exceeded");
    for limit in [0, 257] {
        let invalid = bridge.exchange(json!({"op":"rank_next","cursor":cursors[0],"max_hits":limit}))?;
        assert_eq!(invalid["error"]["kind"], "invalid_request");
    }
    bridge.shutdown()
}

#[test]
fn malformed_operator_only_query_is_refused_and_recovered_syntax_reports_issues() -> Result<()> {
    let mut bridge = BridgeHarness::with_conversations(&[
        FixtureConversation::repeated(1, "local", "needle", 3),
    ])?;
    for bad_query in ["NOT", "AND", "OR", "()"] {
        let reply = bridge.exchange(json!({"op":"rank_begin","query":bad_query}))?;
        assert_eq!(reply["ok"], false, "query {bad_query:?} should be refused");
        assert_eq!(reply["error"]["kind"], "invalid_request");
    }
    let recovered = bridge.exchange(json!({"op":"rank_begin","query":"needle AND"}))?;
    assert_eq!(recovered["ok"], true);
    let issues = recovered["result"]["issues"].as_array().expect("issues array");
    assert!(!issues.is_empty(), "expected parser syntax recovery issue in response");
    bridge.shutdown()
}
