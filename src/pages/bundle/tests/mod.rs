mod builder_and_assets;
mod publish_recovery;

use std::fs::{self, File};
use std::io::BufWriter;
use std::path::Path;

use super::{UnencryptedConfig, UnencryptedPayload};

pub(super) fn write_unencrypted_source(root: &Path, payload_name: &str, body: &str) {
    let payload_dir = root.join("payload");
    fs::create_dir_all(&payload_dir).unwrap();
    let payload_path = payload_dir.join(payload_name);
    fs::write(&payload_path, body).unwrap();

    let config = UnencryptedConfig {
        encrypted: false,
        version: "1.0.0".to_string(),
        payload: UnencryptedPayload {
            path: format!("payload/{payload_name}"),
            format: "sqlite".to_string(),
            size_bytes: Some(body.len() as u64),
        },
        warning: Some("UNENCRYPTED".to_string()),
    };

    let file = File::create(root.join("config.json")).unwrap();
    serde_json::to_writer_pretty(BufWriter::new(file), &config).unwrap();
}
