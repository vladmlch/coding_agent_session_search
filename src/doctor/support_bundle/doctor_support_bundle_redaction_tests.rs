use super::doctor_support_bundle_redact_value;
use serde_json::json;
use std::path::Path;

#[test]
pub(crate) fn structured_redaction_scrubs_keys_values_and_preserves_collisions() {
    let data_dir = Path::new("/tmp/private-cass-data");
    let github_token = ["ghp_", "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij"].concat();
    let openai_token = ["sk-", "ABCDEFGHIJKLMNOPQRSTUVWXYZ123456"].concat();

    let mut nested = serde_json::Map::new();
    nested.insert("pin".to_string(), json!(1234));
    nested.insert("credentials".to_string(), json!({"value": "short"}));
    nested.insert("keyframe".to_string(), json!(7));
    nested.insert("token_count".to_string(), json!(9));
    nested.insert(github_token.clone(), json!("first"));
    nested.insert(openai_token.clone(), json!("second"));
    nested.insert(
        "message".to_string(),
        json!(format!(
            "failure at {}/doctor with {github_token}",
            data_dir.display()
        )),
    );

    let redacted = doctor_support_bundle_redact_value(
        json!({"nested": serde_json::Value::Object(nested)}),
        data_dir,
    );
    let serialized = serde_json::to_string(&redacted).expect("serialize redacted bundle");
    assert!(!serialized.contains(&github_token));
    assert!(!serialized.contains(&openai_token));
    assert!(!serialized.contains(&data_dir.display().to_string()));
    assert_eq!(redacted["nested"]["pin"], "[REDACTED]");
    assert_eq!(redacted["nested"]["credentials"], "[REDACTED]");
    assert_eq!(redacted["nested"]["keyframe"], 7);
    assert_eq!(redacted["nested"]["token_count"], 9);
    assert_eq!(
        redacted["nested"]
            .as_object()
            .expect("redacted nested object")
            .len(),
        7
    );
}
