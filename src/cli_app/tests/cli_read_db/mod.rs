#[allow(unused_imports)]
pub(super) use super::*;
#[allow(unused_imports)]
pub(crate) use crate::storage::sqlite::FrankenStorage;
#[allow(unused_imports)]
pub(crate) use fs2::FileExt;
#[allow(unused_imports)]
pub(crate) use serial_test::serial;
#[allow(unused_imports)]
pub(crate) use tempfile::TempDir;

mod rebuild_meta;
mod resume_detect;
mod state_probes;

pub(crate) fn seed_cli_db() -> (TempDir, PathBuf) {
    let temp = TempDir::new().expect("tempdir");
    let db_path = temp.path().join("agent_search.db");
    let storage = FrankenStorage::open(&db_path).expect("open cass db");
    storage
        .set_last_indexed_at(1_733_000_000_000)
        .expect("set last_indexed_at");
    storage
        .set_last_scan_ts(1_732_999_999_000)
        .expect("set last_scan_ts");
    drop(storage);
    (temp, db_path)
}

/// zn1xn: a full index run that leaves the canonical FTS shadow half-built
/// records a marker that `state_meta_json` (hence `index --json`/`status`)

pub(crate) fn write_index_lock_metadata(
    lock_path: &Path,
    lock_file: &mut std::fs::File,
    metadata: &str,
) {
    use std::io::{Seek, SeekFrom, Write};

    lock_file.set_len(0).expect("truncate index lock metadata");
    lock_file
        .seek(SeekFrom::Start(0))
        .expect("rewind index lock metadata");
    lock_file
        .write_all(metadata.as_bytes())
        .expect("write lock metadata");
    lock_file.flush().expect("flush lock metadata");
    crate::search::asset_state::write_index_run_lock_metadata_sidecar(lock_path, metadata)
        .expect("write lock metadata sidecar");
}

pub(crate) struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(value) = &self.previous {
            unsafe {
                std::env::set_var(self.key, value);
            }
        } else {
            unsafe {
                std::env::remove_var(self.key);
            }
        }
    }
}

pub(crate) fn set_env(key: &'static str, value: &str) -> EnvGuard {
    let previous = dotenvy::var(key).ok();
    unsafe {
        std::env::set_var(key, value);
    }
    EnvGuard { key, previous }
}
