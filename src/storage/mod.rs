//! Persistent storage interfaces.
pub(crate) mod archive_fingerprint;
pub(crate) mod archive_lease;
pub mod codex_layout_migration;
pub(crate) mod lexical_checkpoint_io;
pub(crate) mod omp_paths;
pub mod run_lock;
pub mod sqlite;
