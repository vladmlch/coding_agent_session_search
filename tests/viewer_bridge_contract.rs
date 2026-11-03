#[path = "viewer_bridge_support/mod.rs"]
mod support;

use anyhow::Result;
use serde_json::json;
use support::{BridgeHarness, FixtureConversation};

#[test]
fn strict_bridge_has_versioned_limits_and_does_not_claim_runtime_qualification() -> Result<()> {
    let mut bridge = BridgeHarness::empty()?;
    let before = bridge.tree()?;
    let hello = bridge.exchange(json!({"op":"hello","id":1}))?;
    assert_eq!(hello["result"]["bridge_protocol"], 1);
    assert_eq!(hello["result"]["db_schema_version"], 23);
    assert_eq!(hello["result"]["exact_rank"], false);
    assert_eq!(hello["result"]["rank_enumeration_supported"], true);
    assert_eq!(hello["result"]["qualification"], "static_only");
    assert_eq!(hello["result"]["limits"]["page_limit"], 256);
    for value in [
        json!({"op":"arbitrary_sql","id":2,"sql":"DELETE FROM messages"}),
        json!({"op":"hello","id":3,"unexpected":true}),
        json!({"op":"catalog_page","id":4,"limit":0}),
        json!({"op":"messages_page","id":5,"conversation_id":1,"limit":257}),
    ] {
        let result = bridge.exchange(value)?;
        assert_eq!(result["ok"], false);
        assert_eq!(result["error"]["kind"], "invalid_request");
    }
    bridge.shutdown()?;
    assert_eq!(
        bridge.tree()?,
        before,
        "Read-only service changed canonical/index bytes"
    );
    Ok(())
}

#[test]
fn canonical_pages_keep_exact_coordinates_nullable_values_and_extra_provenance() -> Result<()> {
    let mut bridge = BridgeHarness::with_conversations(&[
        FixtureConversation::repeated(1, "local", "полный текст 🚲", 3),
        FixtureConversation::repeated(2, "remote:δ", "remote text", 1),
    ])?;
    let page = bridge.exchange(json!({"op":"catalog_page","limit":1}))?;
    assert_eq!(page["ok"], true);
    assert_eq!(page["result"]["rows"][0]["id"], 1);
    assert_eq!(page["result"]["next_conversation_id"], 1);
    assert!(
        page["result"]["rows"][0]
            .get("estimated_cost_usd")
            .is_some()
    );
    assert_eq!(page["result"]["rows"][0]["metadata"]["complete"], true);
    let second = bridge.exchange(json!({"op":"catalog_page","limit":1,"after_id":1}))?;
    assert_eq!(second["result"]["rows"][0]["source_id"], "remote:δ");
    assert_eq!(
        second["result"]["catalog_revision"],
        page["result"]["catalog_revision"]
    );
    assert!(second["result"]["next_conversation_id"].is_null());
    let messages = bridge.exchange(json!({"op":"messages_page","conversation_id":1,"limit":2}))?;
    assert_eq!(messages["result"]["items"][0]["idx"], 0);
    assert_eq!(messages["result"]["items"][0]["content"], "полный текст 🚲");
    assert_eq!(messages["result"]["items"][0]["extra"]["origin"], "msgpack");
    assert_eq!(messages["result"]["next_idx"], 1);
    let tail = bridge
        .exchange(json!({"op":"messages_page","conversation_id":1,"limit":2,"after_idx":1}))?;
    assert_eq!(tail["result"]["items"].as_array().map(Vec::len), Some(1));
    assert_eq!(tail["result"]["items"][0]["idx"], 2);
    let missing = bridge.exchange(json!({"op":"session","conversation_id":99}))?;
    assert_eq!(missing["ok"], true);
    assert!(missing["result"]["row"].is_null());
    bridge.shutdown()
}

#[test]
fn malformed_preferred_metadata_retains_json_fallback_but_never_claims_complete() -> Result<()> {
    use coding_agent_search::franken_sync::compat::param_slice_to_values;
    use coding_agent_search::franken_sync::{Connection, params};
    let mut bridge = BridgeHarness::with_conversations(&[FixtureConversation::repeated(
        1, "local", "fixture", 1,
    )])?;
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer.execute_with_params(
        "UPDATE conversations SET metadata_bin=?1,metadata_json=?2 WHERE id=1",
        &param_slice_to_values(params![vec![0xc1_u8], "{\"recovered\":true}"]),
    )?;
    drop(writer);
    let row = bridge.exchange(json!({"op":"session","conversation_id":1}))?;
    assert_eq!(row["ok"], true);
    assert_eq!(row["result"]["row"]["metadata"]["value"]["recovered"], true);
    assert_eq!(row["result"]["row"]["metadata"]["complete"], false);
    assert_eq!(row["result"]["row"]["metadata"]["origin"], "json");
    assert!(
        row["result"]["row"]["metadata"]["issues"]
            .as_array()
            .is_some_and(|issues| issues.contains(&json!("invalid_msgpack")))
    );
    bridge.shutdown()
}

#[test]
fn schema_mismatch_and_oversized_canonical_body_are_explicit_refusals() -> Result<()> {
    use coding_agent_search::franken_sync::compat::param_slice_to_values;
    use coding_agent_search::franken_sync::{Connection, params};
    let mut bridge = BridgeHarness::with_conversations(&[FixtureConversation::repeated(
        1, "local", "fixture", 1,
    )])?;
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer.execute_with_params(
        "UPDATE messages SET content=?1 WHERE conversation_id=1",
        &param_slice_to_values(params!["x".repeat(524_289)]),
    )?;
    drop(writer);
    let body = bridge.exchange(json!({"op":"messages_page","conversation_id":1,"limit":1}))?;
    assert_eq!(body["ok"], false);
    assert_eq!(body["error"]["kind"], "canonical_payload_too_large");
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    // One past the pinned profile: simulates an archive from a newer CASS.
    writer.execute("UPDATE meta SET value='24' WHERE key='schema_version'")?;
    drop(writer);
    let mismatch = bridge.exchange(json!({"op":"reload"}))?;
    assert_eq!(mismatch["ok"], false);
    assert_eq!(mismatch["error"]["kind"], "incompatible_schema");
    bridge.shutdown()
}

#[test]
fn multi_row_page_truncates_at_byte_budget_and_paginates_cleanly() -> Result<()> {
    let chunk = "x".repeat(100_000);
    let mut bridge = BridgeHarness::with_conversations(&[FixtureConversation::repeated(
        1, "local", &chunk, 16,
    )])?;
    let mut seen = Vec::new();
    let mut after_idx = serde_json::Value::Null;
    loop {
        let mut req = json!({"op":"messages_page","conversation_id":1,"limit":256});
        if !after_idx.is_null() {
            req["after_idx"] = after_idx.clone();
        }
        let page = bridge.exchange(req)?;
        assert_eq!(page["ok"], true);
        let items = page["result"]["items"].as_array().expect("items array");
        assert!(!items.is_empty());
        assert!(items.len() < 16);
        for item in items {
            seen.push(item["idx"].as_i64().expect("idx"));
        }
        if !page["result"]["next_idx"].is_null() {
            after_idx = page["result"]["next_idx"].clone();
        } else {
            break;
        }
    }
    assert_eq!(seen, (0..16).collect::<Vec<_>>());
    bridge.shutdown()
}

#[test]
fn deep_metadata_is_rejected_before_exceeding_viewer_wire_frame_depth() -> Result<()> {
    use coding_agent_search::franken_sync::compat::param_slice_to_values;
    use coding_agent_search::franken_sync::{Connection, params};
    let mut bridge = BridgeHarness::with_conversations(&[FixtureConversation::repeated(
        1, "local", "fixture", 1,
    )])?;
    let mut deep = "leaf".to_owned();
    for _ in 0..60 {
        deep = format!("[{deep}]");
    }
    let writer = Connection::open(bridge.db.to_string_lossy().into_owned())?;
    writer.execute_with_params(
        "UPDATE conversations SET metadata_bin=NULL,metadata_json=?1 WHERE id=1",
        &param_slice_to_values(params![deep]),
    )?;
    drop(writer);
    let row = bridge.exchange(json!({"op":"session","conversation_id":1}))?;
    assert_eq!(row["ok"], true);
    assert_eq!(row["result"]["row"]["metadata"]["complete"], false);
    assert!(
        row["result"]["row"]["metadata"]["issues"]
            .as_array()
            .is_some_and(|issues| issues.contains(&json!("invalid_json")))
    );
    bridge.shutdown()
}

#[test]
fn single_index_missing_schema_hash_is_refused_on_open_or_reload() -> Result<()> {
    let mut bridge = BridgeHarness::with_conversations(&[FixtureConversation::repeated(
        1, "local", "needle", 1,
    )])?;
    let schema_hash = coding_agent_search::search::tantivy::expected_index_dir(&bridge.data_dir)
        .join("schema_hash.json");
    std::fs::remove_file(schema_hash)?;
    let reload = bridge.exchange(json!({"op":"reload"}))?;
    assert_eq!(reload["ok"], true);
    assert!(reload["result"]["index_epoch"].is_null());
    assert!(
        !reload["result"]["index_issues"]
            .as_array()
            .expect("issues")
            .is_empty()
    );
    let rank = bridge.exchange(json!({"op":"rank_begin","query":"needle"}))?;
    assert_eq!(rank["ok"], false);
    assert_eq!(rank["error"]["kind"], "index_unavailable");
    bridge.shutdown()
}

#[test]
fn unextended_archive_reports_capability_unavailable_and_refuses_writes() -> Result<()> {
    // Protocol-1 compatibility: an archive without the extension profile
    // advertises `cass_owned_queries_v1` as unavailable, reports no logical
    // generation, and refuses mutating RPC with a typed denial — the strict
    // read-only lane is otherwise byte-identical.
    let mut bridge = BridgeHarness::with_conversations(&[FixtureConversation::repeated(
        1, "local", "fixture", 1,
    )])?;
    let before = bridge.tree()?;
    let hello = bridge.exchange(json!({"op":"hello","id":1}))?;
    assert_eq!(hello["ok"], true);
    assert_eq!(hello["result"]["bridge_protocol"], 1);
    let capability = &hello["result"]["capabilities"]["cass_owned_queries_v1"];
    assert_eq!(capability["available"], false);
    assert_eq!(capability["privileges"]["annotations_write"], false);
    assert!(hello["result"]["generation"].is_null());

    for value in [
        json!({"op":"patch_annotations","id":2,"conversation_id":1,
               "expected_revision":0,"patch":{"is_pinned":true}}),
        json!({"op":"patch_annotations","id":3,"conversation_id":1,
               "expected_revision":0,"patch":{"notes":"n"}}),
    ] {
        let result = bridge.exchange(value)?;
        assert_eq!(result["ok"], false);
        assert_eq!(result["error"]["kind"], "capability_not_granted");
    }
    let annotations =
        bridge.exchange(json!({"op":"get_annotations","id":4,"conversation_id":1}))?;
    assert_eq!(annotations["ok"], true);
    assert_eq!(annotations["result"]["installed"], false);
    assert!(annotations["result"]["annotation"].is_null());

    bridge.shutdown()?;
    assert_eq!(
        bridge.tree()?,
        before,
        "Refused write RPCs still changed canonical/index bytes"
    );
    Ok(())
}
