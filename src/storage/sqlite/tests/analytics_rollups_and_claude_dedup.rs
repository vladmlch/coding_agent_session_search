use super::*;
use crate::franken_sync::{
    Connection as FrankenConnection, Row as FrankenRow,
    compat::{ConnectionExt as FrankenConnectionExt, RowExt as FrankenRowExt},
};
use tempfile::TempDir;

// =========================================================================
// Current analytics/schema smoke test (bead z9fse.11)
// =========================================================================

#[test]
fn migration_v13_creates_analytics_tables() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    // Schema version should be current.
    let version = storage.schema_version().unwrap();
    assert_eq!(
        version, CURRENT_SCHEMA_VERSION,
        "Schema version must match CURRENT_SCHEMA_VERSION after migration"
    );

    let conn = storage.raw();

    // Helper: collect column names from PRAGMA table_info
    fn col_names(conn: &FrankenConnection, table: &str) -> Vec<String> {
        conn.query_map_collect(
            &format!("PRAGMA table_info({})", table),
            fparams![],
            |row: &FrankenRow| row.get_typed(1),
        )
        .unwrap()
    }

    // Helper: collect index names from PRAGMA index_list
    fn idx_names(conn: &FrankenConnection, table: &str) -> Vec<String> {
        conn.query_map_collect(
            &format!("PRAGMA index_list({})", table),
            fparams![],
            |row: &FrankenRow| row.get_typed(1),
        )
        .unwrap()
    }

    // Verify message_metrics table exists with expected columns
    let mm_cols = col_names(conn, "message_metrics");
    for expected in &[
        "message_id",
        "hour_id",
        "day_id",
        "content_tokens_est",
        "model_name",
        "model_family",
        "model_tier",
        "provider",
        "api_input_tokens",
        "has_plan",
        "agent_slug",
        "role",
        "api_data_source",
    ] {
        assert!(
            mm_cols.contains(&expected.to_string()),
            "message_metrics missing column: {expected}"
        );
    }

    // Verify usage_hourly table
    let uh_cols = col_names(conn, "usage_hourly");
    for expected in &[
        "hour_id",
        "plan_message_count",
        "plan_content_tokens_est_total",
        "plan_api_tokens_total",
        "api_coverage_message_count",
        "content_tokens_est_user",
        "api_thinking_tokens_total",
    ] {
        assert!(
            uh_cols.contains(&expected.to_string()),
            "usage_hourly missing column: {expected}"
        );
    }

    // Verify usage_daily table
    let ud_cols = col_names(conn, "usage_daily");
    for expected in &[
        "day_id",
        "plan_content_tokens_est_total",
        "plan_api_tokens_total",
        "api_thinking_tokens_total",
        "content_tokens_est_assistant",
        "message_count",
    ] {
        assert!(
            ud_cols.contains(&expected.to_string()),
            "usage_daily missing column: {expected}"
        );
    }

    // Verify usage_models_daily table
    let umd_cols = col_names(conn, "usage_models_daily");
    for expected in &[
        "day_id",
        "model_family",
        "model_tier",
        "message_count",
        "api_tokens_total",
        "api_coverage_message_count",
    ] {
        assert!(
            umd_cols.contains(&expected.to_string()),
            "usage_models_daily missing column: {expected}"
        );
    }

    // Verify indexes on message_metrics
    let mm_idxs = idx_names(conn, "message_metrics");
    assert!(
        mm_idxs.iter().any(|n| n.contains("idx_mm_hour")),
        "message_metrics must have hour index"
    );
    assert!(
        mm_idxs.iter().any(|n| n.contains("idx_mm_agent_day")),
        "message_metrics must have agent+day index"
    );
    assert!(
        mm_idxs
            .iter()
            .any(|n| n.contains("idx_mm_model_family_day")),
        "message_metrics must have model_family+day index"
    );

    // Verify indexes on usage_hourly
    let uh_idxs = idx_names(conn, "usage_hourly");
    assert!(
        uh_idxs.iter().any(|n| n.contains("idx_uh_agent")),
        "usage_hourly must have agent index"
    );

    // Verify indexes on usage_daily
    let ud_idxs = idx_names(conn, "usage_daily");
    assert!(
        ud_idxs.iter().any(|n| n.contains("idx_ud_agent")),
        "usage_daily must have agent index"
    );

    // Verify indexes on usage_models_daily
    let umd_idxs = idx_names(conn, "usage_models_daily");
    assert!(
        umd_idxs.iter().any(|n| n.contains("idx_umd_model_day")),
        "usage_models_daily must have model+day index"
    );

    let conversation_cols = col_names(conn, "conversations");
    assert!(
        conversation_cols.contains(&"last_message_idx".to_string())
            && conversation_cols.contains(&"last_message_created_at".to_string()),
        "fresh schema must include V15 tail columns without ALTER TABLE on conversations"
    );
    let fts_schema_rows: i64 = conn
        .query_row_map(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'fts_messages'",
            fparams![],
            |row: &FrankenRow| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(
        fts_schema_rows, 1,
        "fresh schema v1 creates derived fts_messages at birth"
    );
    let integrity: Vec<String> = conn
        .query_map_collect("PRAGMA integrity_check;", fparams![], |row: &FrankenRow| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(
        integrity,
        vec!["ok".to_string()],
        "fresh schema must pass SQLite integrity_check"
    );
}

#[test]
fn hour_id_round_trip() {
    // 2026-02-06 12:00:00 UTC
    let ts_ms = 1_770_508_800_000_i64;
    let hour_id = SqliteStorage::hour_id_from_millis(ts_ms);
    let day_id = SqliteStorage::day_id_from_millis(ts_ms);

    // hour_id should be 24x day_id (approximately)
    assert_eq!(hour_id / 24, day_id, "hour_id/24 should equal day_id");

    // Round-trip: millis_from_hour_id should give start of that hour
    let back = SqliteStorage::millis_from_hour_id(hour_id);
    assert!(
        back <= ts_ms && ts_ms - back < 3_600_000,
        "Round-trip should land within the same hour"
    );
}

#[test]
fn day_and_hour_ids_floor_negative_millis() {
    // One millisecond before the Unix epoch should still floor into the
    // previous second/hour/day rather than truncating toward zero.
    let ts_ms = -1_i64;
    let expected_secs = -1_i64;
    let epoch_2020_secs = 1_577_836_800_i64;

    assert_eq!(
        SqliteStorage::day_id_from_millis(ts_ms),
        (expected_secs - epoch_2020_secs).div_euclid(86_400)
    );
    assert_eq!(
        (expected_secs - epoch_2020_secs).div_euclid(3_600),
        SqliteStorage::hour_id_from_millis(ts_ms)
    );
}

// =========================================================================
// Analytics ingest integration test (bead z9fse.2)
// =========================================================================

#[test]
fn analytics_ingest_populates_metrics_and_rollups() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    // Register agent + workspace
    let agent = Agent {
        id: None,
        slug: "claude_code".into(),
        name: "Claude Code".into(),
        version: Some("1.0".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    // Create a synthetic conversation with 3 messages at a known timestamp
    // 2026-02-06 10:30:00 UTC → day_id = 2228, hour_id = 53472
    let ts_ms = 1_770_551_400_000_i64;
    let expected_day = SqliteStorage::day_id_from_millis(ts_ms);
    let expected_hour = SqliteStorage::hour_id_from_millis(ts_ms);

    // Include a JSON usage block on the assistant message (like Claude Code data)
    let usage_json = serde_json::json!({
        "message": {
            "model": "claude-opus-4-6",
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50,
                "cache_read_input_tokens": 200,
                "cache_creation_input_tokens": 30,
                "service_tier": "standard"
            }
        }
    });

    let conv = Conversation {
        id: None,
        agent_slug: "claude_code".into(),
        workspace: None,
        external_id: Some("test-conv-1".into()),
        title: Some("Test conversation".into()),
        source_path: PathBuf::from("/tmp/test.jsonl"),
        started_at: Some(ts_ms),
        ended_at: Some(ts_ms + 60_000),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: None,
                created_at: Some(ts_ms),
                content: "Hello, can you help me with a plan?".into(),
                extra_json: serde_json::Value::Null,
                snippets: vec![],
            },
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: None,
                created_at: Some(ts_ms + 30_000),
                content: "## Plan\n\n1. First step\n2. Second step\n3. Third step".into(),
                extra_json: usage_json,
                snippets: vec![],
            },
            Message {
                id: None,
                idx: 2,
                role: MessageRole::User,
                author: None,
                created_at: Some(ts_ms + 60_000),
                content: "Great, let's proceed!".into(),
                extra_json: serde_json::Value::Null,
                snippets: vec![],
            },
        ],
        source_id: "local".into(),
        origin_host: None,
    };

    let outcomes = storage
        .insert_conversations_batched_with_analytics(&[(agent_id, None, &conv)], false)
        .unwrap();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].inserted_indices.len(), 3);

    let conn = storage.raw();

    // Verify message_metrics rows
    let mm_count: i64 = conn
        .query_row_map("SELECT COUNT(*) FROM message_metrics", &[], |row| {
            row.get_typed::<i64>(0)
        })
        .unwrap();
    assert_eq!(mm_count, 3, "Should have 3 message_metrics rows");

    // Verify hour_id and day_id are correct
    #[allow(clippy::type_complexity)]
    let rows: Vec<(i64, i64, String, i64, i64, String, String, String, String)> = conn
        .query_map_collect(
            "SELECT hour_id, day_id, role, content_tokens_est, has_plan, api_data_source, model_family, model_tier, provider FROM message_metrics ORDER BY message_id",
            fparams![],
            |row: &FrankenRow| {
                Ok((
                    row.get_typed(0)?,
                    row.get_typed(1)?,
                    row.get_typed(2)?,
                    row.get_typed(3)?,
                    row.get_typed(4)?,
                    row.get_typed(5)?,
                    row.get_typed(6)?,
                    row.get_typed(7)?,
                    row.get_typed(8)?,
                ))
            },
        )
        .unwrap();

    assert_eq!(rows.len(), 3);
    // All messages in the same hour/day
    assert_eq!(rows[0].0, expected_hour);
    assert_eq!(rows[0].1, expected_day);
    // First message is user
    assert_eq!(rows[0].2, "user");
    // Second message (assistant) should have has_plan=1 (contains "## Plan" + numbered steps)
    assert_eq!(
        rows[1].4, 1,
        "Assistant message with plan should have has_plan=1"
    );
    // Second message should have api data source
    assert_eq!(
        rows[1].5, "api",
        "Claude Code assistant message should have api data source"
    );
    // First and third (user) messages should be estimated
    assert_eq!(rows[0].5, "estimated");
    assert_eq!(rows[2].5, "estimated");
    assert_eq!(rows[1].6, "claude");
    assert_eq!(rows[1].7, "opus");
    assert_eq!(rows[1].8, "anthropic");
    assert_eq!(rows[0].6, "unknown");
    // content_tokens_est = chars / 4
    let user_chars = "Hello, can you help me with a plan?".len() as i64;
    assert_eq!(rows[0].3, user_chars / 4);

    // Verify usage_hourly rollup
    let (uh_msg, uh_user, uh_asst, uh_plan, uh_plan_content, uh_plan_api, uh_api_cov): (
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
    ) = conn
        .query_row_map(
            "SELECT message_count, user_message_count, assistant_message_count, plan_message_count,
                    plan_content_tokens_est_total, plan_api_tokens_total, api_coverage_message_count
             FROM usage_hourly WHERE hour_id = ?",
            fparams![expected_hour],
            |row: &FrankenRow| {
                Ok((
                    row.get_typed(0)?,
                    row.get_typed(1)?,
                    row.get_typed(2)?,
                    row.get_typed(3)?,
                    row.get_typed(4)?,
                    row.get_typed(5)?,
                    row.get_typed(6)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(uh_msg, 3, "Hourly rollup should have 3 messages");
    assert_eq!(uh_user, 2, "Hourly rollup should have 2 user messages");
    assert_eq!(uh_asst, 1, "Hourly rollup should have 1 assistant message");
    assert_eq!(uh_plan, 1, "Hourly rollup should have 1 plan message");
    assert!(
        uh_plan_content > 0,
        "Hourly rollup should include plan content tokens"
    );
    assert!(
        uh_plan_api > 0,
        "Hourly rollup should include plan API tokens"
    );
    assert_eq!(
        uh_api_cov, 1,
        "Hourly rollup should have 1 API-covered message"
    );

    // Verify usage_daily rollup matches hourly (same day)
    let (ud_msg, ud_api_cov): (i64, i64) = conn
        .query_row_map(
            "SELECT message_count, api_coverage_message_count FROM usage_daily WHERE day_id = ?",
            fparams![expected_day],
            |row: &FrankenRow| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert_eq!(ud_msg, 3, "Daily rollup should match hourly");
    assert_eq!(
        ud_api_cov, 1,
        "Daily api_coverage should be 1 (only assistant msg has real API data)"
    );

    // Verify the API input tokens from message_metrics (only API-sourced)
    let api_only_input: i64 = conn
        .query_row_map(
            "SELECT COALESCE(SUM(api_input_tokens), 0) FROM message_metrics WHERE day_id = ? AND api_data_source = 'api'",
            fparams![expected_day],
            |row: &FrankenRow| row.get_typed::<i64>(0),
        )
        .unwrap();
    assert_eq!(
        api_only_input, 100,
        "Only API-sourced input tokens should be 100"
    );

    // Verify rollups match summed message_metrics
    let mm_total_content_est: i64 = conn
        .query_row_map(
            "SELECT SUM(content_tokens_est) FROM message_metrics WHERE day_id = ?",
            fparams![expected_day],
            |row| row.get_typed::<i64>(0),
        )
        .unwrap();
    let mm_plan_content_est: i64 = conn
        .query_row_map(
                "SELECT COALESCE(SUM(content_tokens_est), 0) FROM message_metrics WHERE day_id = ? AND has_plan = 1",
            fparams![expected_day],
            |row: &FrankenRow| row.get_typed::<i64>(0),
        )
        .unwrap();
    let mm_plan_api_total: i64 = conn
        .query_row_map(
            "SELECT COALESCE(SUM(COALESCE(api_input_tokens, 0) + COALESCE(api_output_tokens, 0) + COALESCE(api_cache_read_tokens, 0) + COALESCE(api_cache_creation_tokens, 0) + COALESCE(api_thinking_tokens, 0)), 0)
             FROM message_metrics WHERE day_id = ? AND has_plan = 1 AND api_data_source = 'api'",
            fparams![expected_day],
            |row: &FrankenRow| row.get_typed::<i64>(0),
        )
        .unwrap();
    let ud_content_est: i64 = conn
        .query_row_map(
            "SELECT content_tokens_est_total FROM usage_daily WHERE day_id = ?",
            fparams![expected_day],
            |row| row.get_typed::<i64>(0),
        )
        .unwrap();
    let (ud_plan_content_est, ud_plan_api_total): (i64, i64) = conn
        .query_row_map(
            "SELECT plan_content_tokens_est_total, plan_api_tokens_total FROM usage_daily WHERE day_id = ?",
            fparams![expected_day],
            |row: &FrankenRow| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert_eq!(
        mm_total_content_est, ud_content_est,
        "Daily rollup content_tokens_est_total must equal SUM of message_metrics"
    );
    assert_eq!(
        mm_plan_content_est, ud_plan_content_est,
        "Daily rollup plan_content_tokens_est_total must equal planned message_metrics content sum"
    );
    assert_eq!(
        mm_plan_api_total, ud_plan_api_total,
        "Daily rollup plan_api_tokens_total must equal planned message_metrics API token sum"
    );

    // Verify model rollup rows
    let (claude_msg, claude_user, claude_asst, claude_api_total, claude_api_cov): (
            i64,
            i64,
        i64,
        i64,
        i64,
    ) = conn
        .query_row_map(
            "SELECT message_count, user_message_count, assistant_message_count, api_tokens_total, api_coverage_message_count
             FROM usage_models_daily
             WHERE day_id = ? AND model_family = 'claude' AND model_tier = 'opus'",
            fparams![expected_day],
            |row: &FrankenRow| Ok((row.get_typed(0)?, row.get_typed(1)?, row.get_typed(2)?, row.get_typed(3)?, row.get_typed(4)?)),
        )
        .unwrap();
    assert_eq!(claude_msg, 1);
    assert_eq!(claude_user, 0);
    assert_eq!(claude_asst, 1);
    assert_eq!(claude_api_total, 380);
    assert_eq!(claude_api_cov, 1);

    let unknown_msg: i64 = conn
        .query_row_map(
            "SELECT message_count FROM usage_models_daily
             WHERE day_id = ? AND model_family = 'unknown' AND model_tier = 'unknown'",
            fparams![expected_day],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(
        unknown_msg, 2,
        "user messages should land in unknown model bucket"
    );
}

fn claude_usage_record(
    idx: i64,
    at: i64,
    message_id: &str,
    request_id: Option<&str>,
    input: i64,
    output: i64,
) -> crate::model::types::Message {
    let mut extra = serde_json::json!({
        "message": {
            "id": message_id,
            "model": "claude-opus-4-6",
            "usage": {
                "input_tokens": input,
                "output_tokens": output,
                "cache_read_input_tokens": 100,
                "cache_creation_input_tokens": 20
            }
        }
    });
    if let Some(request_id) = request_id {
        extra["requestId"] = serde_json::json!(request_id);
    }
    crate::model::types::Message {
        id: None,
        idx,
        role: crate::model::types::MessageRole::Agent,
        author: None,
        created_at: Some(at),
        content: format!("part {idx}"),
        extra_json: extra,
        snippets: vec![],
    }
}

fn claude_usage_conversation(
    messages: Vec<crate::model::types::Message>,
) -> crate::model::types::Conversation {
    crate::model::types::Conversation {
        id: None,
        agent_slug: "claude_code".into(),
        workspace: None,
        external_id: Some("u1-conv".into()),
        title: Some("U1".into()),
        source_path: std::path::PathBuf::from("/tmp/u1.jsonl"),
        started_at: Some(1_770_551_400_000),
        ended_at: Some(1_770_551_460_000),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages,
        source_id: "local".into(),
        origin_host: None,
    }
}

fn token_usage_sums(storage: &SqliteStorage) -> (i64, i64, i64, i64) {
    storage
        .raw()
        .query_row_map(
            "SELECT COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0), \
             COALESCE(SUM(cache_read_tokens),0), COALESCE(SUM(cache_creation_tokens),0) \
             FROM token_usage",
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

fn claude_usage_agent(storage: &SqliteStorage) -> i64 {
    storage
        .ensure_agent(&crate::model::types::Agent {
            id: None,
            slug: "claude_code".into(),
            name: "Claude Code".into(),
            version: None,
            kind: crate::model::types::AgentKind::Cli,
        })
        .unwrap()
}

// review 2026-11-02 U1
#[test]
fn claude_reply_split_across_records_is_counted_once_on_insert() {
    let dir = TempDir::new().unwrap();
    let storage = SqliteStorage::open(&dir.path().join("u1.db")).unwrap();
    let agent_id = claude_usage_agent(&storage);
    let t = 1_770_551_400_000;
    let conv = claude_usage_conversation(vec![
        claude_usage_record(0, t, "m1", Some("r1"), 10, 3),
        claude_usage_record(1, t + 1, "m1", Some("r1"), 10, 7),
        claude_usage_record(2, t + 2, "m2", Some("r2"), 5, 2),
        claude_usage_record(3, t + 3, "m3", None, 4, 1),
    ]);
    storage
        .insert_conversations_batched_with_analytics(&[(agent_id, None, &conv)], false)
        .unwrap();
    // m1 once (10 in, 7 out), m2 (5, 2), m3 independent (4, 1).
    // Cache: m1 once + m2 + m3 = 3 x (100, 20).
    assert_eq!(token_usage_sums(&storage), (19, 10, 300, 60));
}

// review 2026-11-02 U1: the second record of a reply arrives in a later
// ingest of the same (re-parsed) conversation.
#[test]
fn claude_reply_split_across_ingests_is_counted_once() {
    let dir = TempDir::new().unwrap();
    let storage = SqliteStorage::open(&dir.path().join("u1b.db")).unwrap();
    let agent_id = claude_usage_agent(&storage);
    let t = 1_770_551_400_000;
    let first = claude_usage_conversation(vec![claude_usage_record(0, t, "m1", Some("r1"), 10, 3)]);
    storage
        .insert_conversations_batched_with_analytics(&[(agent_id, None, &first)], false)
        .unwrap();
    let full = claude_usage_conversation(vec![
        claude_usage_record(0, t, "m1", Some("r1"), 10, 3),
        claude_usage_record(1, t + 1, "m1", Some("r1"), 10, 7),
    ]);
    storage
        .insert_conversations_batched_with_analytics(&[(agent_id, None, &full)], false)
        .unwrap();
    assert_eq!(token_usage_sums(&storage), (10, 7, 100, 20));
}

// review 2026-11-02 U1: rebuild must apply the same rule as ingest.
#[test]
fn claude_reply_dedup_survives_analytics_rebuild() {
    let dir = TempDir::new().unwrap();
    let storage = SqliteStorage::open(&dir.path().join("u1c.db")).unwrap();
    let agent_id = claude_usage_agent(&storage);
    let t = 1_770_551_400_000;
    let conv = claude_usage_conversation(vec![
        claude_usage_record(0, t, "m1", Some("r1"), 10, 3),
        claude_usage_record(1, t + 1, "m1", Some("r1"), 10, 7),
        claude_usage_record(2, t + 2, "m2", Some("r2"), 5, 2),
    ]);
    storage
        .insert_conversations_batched_with_analytics(&[(agent_id, None, &conv)], false)
        .unwrap();
    let conn = storage.raw();
    for table in [
        "message_metrics",
        "usage_hourly",
        "usage_daily",
        "usage_models_daily",
    ] {
        conn.execute(&format!("DELETE FROM {table}")).unwrap();
    }
    storage.rebuild_analytics().unwrap();
    let (input, output): (i64, i64) = conn
        .query_row_map(
            "SELECT COALESCE(SUM(api_input_tokens),0), COALESCE(SUM(api_output_tokens),0) \
             FROM message_metrics WHERE api_data_source = 'api'",
            &[],
            |row| Ok((row.get_typed(0)?, row.get_typed(1)?)),
        )
        .unwrap();
    assert_eq!((input, output), (15, 9));
}

#[test]
fn has_plan_heuristic_detects_plans() {
    assert!(has_plan_heuristic(
        "## Plan\n\n1. First step\n2. Second step"
    ));
    assert!(has_plan_heuristic(
        "# Plan\nHere is what we will do:\n1. Step one\n2. Step two"
    ));
    assert!(has_plan_heuristic(
        "Plan:\n- Gather baseline\n- Implement changes\n- Validate with tests"
    ));
    assert!(has_plan_heuristic(
        "Next steps:\n1. Update schema\n2. Rebuild rollups"
    ));
    assert!(!has_plan_heuristic("Hello world"));
    assert!(!has_plan_heuristic("Short"));
    assert!(!has_plan_heuristic(
        "This is a regular message without plans"
    ));
    assert!(!has_plan_heuristic(
        "```json\n{\"tool\":\"shell\",\"stdout\":\"1. install\\n2. run\"}\n```"
    ));
}

#[test]
fn has_plan_for_role_only_counts_assistant_messages() {
    let plan_text = "## Plan\n1. First\n2. Second";
    assert!(has_plan_for_role("assistant", plan_text));
    assert!(has_plan_for_role("agent", plan_text));
    assert!(has_plan_for_role("Assistant", plan_text));
    assert!(!has_plan_for_role("user", plan_text));
    assert!(!has_plan_for_role("tool", plan_text));
}

#[test]
fn api_rollups_require_api_data_source() {
    let mut agg = AnalyticsRollupAggregator::new();

    let estimated_plan = MessageMetricsEntry {
        message_id: 1,
        created_at_ms: 0,
        hour_id: 1,
        day_id: 1,
        agent_slug: "codex".into(),
        workspace_id: 0,
        source_id: "local".into(),
        role: "assistant".into(),
        content_chars: 120,
        content_tokens_est: 30,
        model_name: None,
        model_family: "unknown".into(),
        model_tier: "unknown".into(),
        provider: "unknown".into(),
        api_input_tokens: Some(100),
        api_output_tokens: Some(50),
        api_cache_read_tokens: Some(0),
        api_cache_creation_tokens: Some(0),
        api_thinking_tokens: Some(0),
        api_service_tier: None,
        api_data_source: "estimated".into(),
        tool_call_count: 0,
        has_tool_calls: false,
        has_plan: true,
    };
    agg.record(&estimated_plan);

    let api_plan = MessageMetricsEntry {
        message_id: 2,
        created_at_ms: 0,
        hour_id: 1,
        day_id: 1,
        agent_slug: "codex".into(),
        workspace_id: 0,
        source_id: "local".into(),
        role: "assistant".into(),
        content_chars: 80,
        content_tokens_est: 20,
        model_name: None,
        model_family: "unknown".into(),
        model_tier: "unknown".into(),
        provider: "unknown".into(),
        api_input_tokens: Some(40),
        api_output_tokens: Some(10),
        api_cache_read_tokens: Some(0),
        api_cache_creation_tokens: Some(0),
        api_thinking_tokens: Some(0),
        api_service_tier: None,
        api_data_source: "api".into(),
        tool_call_count: 0,
        has_tool_calls: false,
        has_plan: true,
    };
    agg.record(&api_plan);

    let key = (1_i64, "codex".to_string(), 0_i64, "local".to_string());
    let hourly = agg.hourly.get(&key).expect("hourly rollup key must exist");
    let daily = agg.daily.get(&key).expect("daily rollup key must exist");
    let model_key = (
        1_i64,
        "codex".to_string(),
        0_i64,
        "local".to_string(),
        "unknown".to_string(),
        "unknown".to_string(),
    );
    let models_daily = agg
        .models_daily
        .get(&model_key)
        .expect("model rollup key must exist");

    // Content rollup includes both plan messages.
    assert_eq!(hourly.plan_message_count, 2);
    assert_eq!(hourly.plan_content_tokens_est_total, 50);
    // API plan tokens must include only api_data_source='api' rows.
    assert_eq!(hourly.plan_api_tokens_total, 50);
    assert_eq!(daily.plan_api_tokens_total, 50);
    assert_eq!(models_daily.plan_api_tokens_total, 50);
    // Overall API totals must also exclude estimated rows.
    assert_eq!(hourly.api_tokens_total, 50);
    assert_eq!(hourly.api_input_tokens_total, 40);
    assert_eq!(hourly.api_output_tokens_total, 10);
    assert_eq!(hourly.api_coverage_message_count, 1);
    assert_eq!(daily.api_tokens_total, 50);
    assert_eq!(models_daily.api_tokens_total, 50);
}

#[test]
fn has_plan_heuristic_curated_corpus_thresholds() {
    // Cross-agent-style positives.
    let positives = [
        "## Plan\n1. Inspect current schema\n2. Add migration\n3. Verify rebuild",
        "Plan:\n1) Reproduce\n2) Patch\n3) Add tests",
        "Implementation plan:\n- Parse inputs\n- Update rollups\n- Run checks",
        "Next steps:\n1. Reserve file\n2. Implement\n3. Report status",
        "# Plan\n1. Gather requirements\n2. Ship changes",
        "Action plan:\n- Identify root cause\n- Fix it\n- Validate",
    ];

    // Typical false positives we want to avoid.
    let negatives = [
        "The plan is to move fast and fix things later.",
        "```json\n{\"tool\":\"shell\",\"stdout\":\"1. ls\\n2. cat\"}\n```",
        "stdout:\n1. Build started\n2. Build finished\nexit code: 0",
        "I can help with that request. Let me know if you want details.",
        "Here is a list:\n- apples\n- oranges",
        "Status update: completed tasks and blockers below.",
    ];

    let tp = positives
        .iter()
        .filter(|msg| has_plan_heuristic(msg))
        .count();
    let fp = negatives
        .iter()
        .filter(|msg| has_plan_heuristic(msg))
        .count();

    let recall = tp as f64 / positives.len() as f64;
    let false_positive_rate = fp as f64 / negatives.len() as f64;

    assert!(
        recall >= 0.80,
        "plan heuristic recall too low: got {recall:.2}"
    );
    assert!(
        false_positive_rate <= 0.20,
        "plan heuristic false-positive rate too high: got {false_positive_rate:.2}"
    );
}

#[test]
fn rebuild_analytics_repopulates_from_messages() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    // Register agent
    let agent = Agent {
        id: None,
        slug: "claude_code".into(),
        name: "Claude Code".into(),
        version: Some("1.0".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    // 2026-02-06 10:30:00 UTC
    let ts_ms = 1_770_551_400_000_i64;
    let expected_day = SqliteStorage::day_id_from_millis(ts_ms);
    let expected_hour = SqliteStorage::hour_id_from_millis(ts_ms);

    let usage_json = serde_json::json!({
        "message": {
            "model": "claude-opus-4-6",
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50,
                "cache_read_input_tokens": 200,
                "cache_creation_input_tokens": 30,
                "service_tier": "standard"
            }
        }
    });

    let conv = Conversation {
        id: None,
        agent_slug: "claude_code".into(),
        workspace: None,
        external_id: Some("test-rebuild-1".into()),
        title: Some("Test conversation".into()),
        source_path: PathBuf::from("/tmp/test.jsonl"),
        started_at: Some(ts_ms),
        ended_at: Some(ts_ms + 60_000),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: vec![
            Message {
                id: None,
                idx: 0,
                role: MessageRole::User,
                author: None,
                created_at: Some(ts_ms),
                content: "Hello, can you help me with a plan?".into(),
                extra_json: serde_json::Value::Null,
                snippets: vec![],
            },
            Message {
                id: None,
                idx: 1,
                role: MessageRole::Agent,
                author: None,
                created_at: Some(ts_ms + 30_000),
                content: "## Plan\n\n1. First step\n2. Second step\n3. Third step".into(),
                extra_json: usage_json,
                snippets: vec![],
            },
            Message {
                id: None,
                idx: 2,
                role: MessageRole::User,
                author: None,
                created_at: Some(ts_ms + 60_000),
                content: "Great, let's proceed!".into(),
                extra_json: serde_json::Value::Null,
                snippets: vec![],
            },
        ],
        source_id: "local".into(),
        origin_host: None,
    };

    storage
        .insert_conversations_batched_with_analytics(&[(agent_id, None, &conv)], false)
        .unwrap();

    // Save original analytics state
    let conn = storage.raw();
    let orig_mm: i64 = conn
        .query_row_map("SELECT COUNT(*) FROM message_metrics", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    let orig_hourly: i64 = conn
        .query_row_map("SELECT COUNT(*) FROM usage_hourly", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    let orig_daily: i64 = conn
        .query_row_map("SELECT COUNT(*) FROM usage_daily", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    let orig_models_daily: i64 = conn
        .query_row_map("SELECT COUNT(*) FROM usage_models_daily", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    let orig_api_input: i64 = conn
        .query_row_map(
            "SELECT COALESCE(SUM(api_input_tokens), 0) FROM message_metrics WHERE api_data_source = 'api'",
            &[],
            |row: &FrankenRow| row.get_typed(0),
        )
        .unwrap();

    assert_eq!(orig_mm, 3);
    assert!(orig_hourly > 0);
    assert!(orig_daily > 0);
    assert!(orig_models_daily > 0);

    // Destroy analytics tables (simulate corruption)
    conn.execute("DELETE FROM message_metrics").unwrap();
    conn.execute("DELETE FROM usage_hourly").unwrap();
    conn.execute("DELETE FROM usage_daily").unwrap();
    conn.execute("DELETE FROM usage_models_daily").unwrap();

    // Verify they're empty
    let zero: i64 = conn
        .query_row_map("SELECT COUNT(*) FROM message_metrics", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(zero, 0);

    // Rebuild analytics
    let result = storage.rebuild_analytics().unwrap();

    assert_eq!(result.message_metrics_rows, 3);
    assert!(result.usage_hourly_rows > 0);
    assert!(result.usage_daily_rows > 0);
    assert!(result.usage_models_daily_rows > 0);
    assert!(
        result.elapsed_ms < 10_000,
        "Rebuild should be fast for 3 msgs"
    );

    // Verify rebuilt data matches
    let conn = storage.raw();
    let rebuilt_mm: i64 = conn
        .query_row_map("SELECT COUNT(*) FROM message_metrics", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(
        rebuilt_mm, orig_mm,
        "Rebuilt message_metrics count should match"
    );

    let rebuilt_hourly: i64 = conn
        .query_row_map("SELECT COUNT(*) FROM usage_hourly", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(
        rebuilt_hourly, orig_hourly,
        "Rebuilt hourly rows should match"
    );

    let rebuilt_daily: i64 = conn
        .query_row_map("SELECT COUNT(*) FROM usage_daily", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(rebuilt_daily, orig_daily, "Rebuilt daily rows should match");

    let rebuilt_models_daily: i64 = conn
        .query_row_map("SELECT COUNT(*) FROM usage_models_daily", &[], |row| {
            row.get_typed(0)
        })
        .unwrap();
    assert_eq!(
        rebuilt_models_daily, orig_models_daily,
        "Rebuilt model rollup rows should match"
    );

    // Verify API token data preserved through rebuild
    let rebuilt_api_input: i64 = conn
        .query_row_map(
            "SELECT COALESCE(SUM(api_input_tokens), 0) FROM message_metrics WHERE api_data_source = 'api'",
            &[],
            |row: &FrankenRow| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(
        rebuilt_api_input, orig_api_input,
        "Rebuilt API input tokens should match original"
    );

    // Verify rollups have correct data
    let (uh_msg, uh_user, uh_asst, uh_plan, uh_plan_content, uh_plan_api): (
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
    ) = conn
        .query_row_map(
            "SELECT message_count, user_message_count, assistant_message_count, plan_message_count,
                    plan_content_tokens_est_total, plan_api_tokens_total
             FROM usage_hourly WHERE hour_id = ?",
            fparams![expected_hour],
            |row: &FrankenRow| {
                Ok((
                    row.get_typed(0)?,
                    row.get_typed(1)?,
                    row.get_typed(2)?,
                    row.get_typed(3)?,
                    row.get_typed(4)?,
                    row.get_typed(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(uh_msg, 3);
    assert_eq!(uh_user, 2);
    assert_eq!(uh_asst, 1);
    assert_eq!(uh_plan, 1);
    assert!(uh_plan_content > 0);
    assert!(uh_plan_api > 0);

    let ud_msg: i64 = conn
        .query_row_map(
            "SELECT message_count FROM usage_daily WHERE day_id = ?",
            fparams![expected_day],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(ud_msg, 3);
}

/// GH #412: `rebuild_analytics_since` must rebuild only the day-aligned
/// window, leave older rollups untouched, and be idempotent.
/// GH #424: the keyset runs over `messages` alone with the day cutoff
/// applied in Rust. A chunk made entirely of out-of-window rows must
/// still advance the cursor, and a message whose conversation row is
/// gone must be dropped (the former inner JOIN's behaviour) rather than
/// failing the rebuild.
#[test]
fn rebuild_analytics_keyset_advances_past_filtered_rows_and_drops_orphans() {
    use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
    use std::path::PathBuf;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let storage = SqliteStorage::open(&db_path).unwrap();

    let agent = Agent {
        id: None,
        slug: "claude_code".into(),
        name: "Claude Code".into(),
        version: Some("1.0".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();

    let day1_ts = 1_770_551_400_000_i64;
    let day2_ts = day1_ts + 3 * 86_400_000;
    let day1 = SqliteStorage::day_id_from_millis(day1_ts);
    let day2 = SqliteStorage::day_id_from_millis(day2_ts);

    let make_conv = |ext: &str, ts: i64, n: usize| Conversation {
        id: None,
        agent_slug: "claude_code".into(),
        workspace: None,
        external_id: Some(ext.into()),
        title: None,
        source_path: PathBuf::from(format!("/tmp/{ext}.jsonl")),
        started_at: Some(ts),
        ended_at: Some(ts + 60_000),
        approx_tokens: None,
        metadata_json: serde_json::Value::Null,
        messages: (0..n)
            .map(|i| Message {
                id: None,
                idx: i as i64,
                role: if i % 2 == 0 {
                    MessageRole::User
                } else {
                    MessageRole::Agent
                },
                author: None,
                created_at: Some(ts + (i as i64) * 1_000),
                content: format!("message {i} of {ext}"),
                extra_json: serde_json::Value::Null,
                snippets: vec![],
            })
            .collect(),
        source_id: "local".into(),
        origin_host: None,
    };
    // Day-1 conversation first so its (older) rows fill the first
    // keyset chunks entirely with out-of-window messages.
    let conv1 = make_conv("keyset-day1", day1_ts, 3);
    let conv2 = make_conv("keyset-day2", day2_ts, 3);
    storage
        .insert_conversations_batched_with_analytics(
            &[(agent_id, None, &conv1), (agent_id, None, &conv2)],
            false,
        )
        .unwrap();

    let conn = storage.raw();
    let metrics_count = |day: i64| -> i64 {
        conn.query_row_map(
            "SELECT COUNT(*) FROM message_metrics WHERE day_id = ?1",
            fparams![day],
            |row| row.get_typed(0),
        )
        .unwrap()
    };
    let daily_count = |day: i64| -> i64 {
        conn.query_row_map(
            "SELECT COALESCE(SUM(message_count), 0) FROM usage_daily WHERE day_id = ?1",
            fparams![day],
            |row| row.get_typed(0),
        )
        .unwrap()
    };

    conn.execute("DELETE FROM message_metrics").unwrap();
    conn.execute("DELETE FROM usage_hourly").unwrap();
    conn.execute("DELETE FROM usage_daily").unwrap();
    conn.execute("DELETE FROM usage_models_daily").unwrap();

    // Chunk size 2: chunk 1 = two day-1 rows (all filtered), chunk 2 =
    // last day-1 row + first day-2 row, chunk 3 = remaining day-2 rows.
    let result = storage
        .rebuild_analytics_since_with_chunk_size(Some(day2_ts), 2)
        .unwrap();
    assert_eq!(result.message_metrics_rows, 3);
    assert_eq!(metrics_count(day1), 0, "out-of-window rows stay out");
    assert_eq!(metrics_count(day2), 3);
    assert_eq!(daily_count(day2), 3);

    let day1_conversation_id: i64 = conn
        .query_row_map(
            "SELECT id FROM conversations WHERE external_id = ?1",
            fparams!["keyset-day1"],
            |row| row.get_typed(0),
        )
        .unwrap();
    conn.execute_compat(
        "INSERT INTO messages (id, conversation_id, idx, role, author, created_at, content, extra_json, extra_bin)
             VALUES (?1, ?2, 3, 'user', NULL, ?3, 'minimum id', NULL, NULL)",
        fparams![i64::MIN, day1_conversation_id, day1_ts + 3_000],
    )
    .unwrap();

    // An orphaned message (conversation row gone) is dropped, not fatal.
    // FK enforcement is on by default; disable it only to plant the
    // orphan the way real-world damage would leave it.
    conn.execute("PRAGMA foreign_keys = OFF").unwrap();
    conn.execute_compat(
        "INSERT INTO messages (conversation_id, idx, role, author, created_at, content, extra_json, extra_bin)
         VALUES (?1, 0, 'user', NULL, ?2, 'orphan', NULL, NULL)",
        fparams![999_999_i64, day2_ts],
    )
    .unwrap();
    conn.execute("PRAGMA foreign_keys = ON").unwrap();
    let full = storage
        .rebuild_analytics_since_with_chunk_size(None, 2)
        .unwrap();
    assert_eq!(
        full.message_metrics_rows, 7,
        "the minimum-id row and six ordinary rows are rebuilt; the orphan is dropped"
    );
    assert_eq!(metrics_count(day1), 4);
    assert_eq!(metrics_count(day2), 3);
    let orphan_metrics: i64 = conn
        .query_row_map(
            "SELECT COUNT(*) FROM message_metrics mm
             WHERE NOT EXISTS (SELECT 1 FROM messages m JOIN conversations c ON c.id = m.conversation_id WHERE m.id = mm.message_id)",
            fparams![],
            |row| row.get_typed(0),
        )
        .unwrap();
    assert_eq!(orphan_metrics, 0);
}
