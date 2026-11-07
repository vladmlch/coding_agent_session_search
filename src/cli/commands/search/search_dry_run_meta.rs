pub(crate) fn search_dry_run_meta(robot_meta: bool, elapsed_ms: u128) -> serde_json::Value {
    let mut meta = serde_json::json!({
        "dry_run": true,
    });
    if robot_meta && let serde_json::Value::Object(ref mut map) = meta {
        map.insert("elapsed_ms".to_string(), serde_json::json!(elapsed_ms));
    }
    meta
}
