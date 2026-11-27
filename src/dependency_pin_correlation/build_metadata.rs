//! Runtime-registered build and ingest-contract metadata.
//!
//! Workspace sub-crates (`cass-model`, `cass-storage`, `cass-indexer`, etc.)
//! cannot rely on compiling their own `build.rs` env vars or reading root
//! `env!`/`option_env!` directly. Instead, the root crate / binary entrypoints
//! register the root build's metadata once via [`BuildMetadata::register_idempotent`]
//! (through `coding_agent_search::ensure_runtime_hooks_registered()`).

use std::sync::OnceLock;

/// Process-wide build metadata supplied by the root crate at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuildMetadata {
    pub ingest_contract: &'static str,
    pub ingest_reuse: bool,
    pub ingest_reuse_blocker: &'static str,
    pub build_commit: Option<&'static str>,
    pub build_commit_date: Option<&'static str>,
    pub git_sha: Option<&'static str>,
}

static REGISTERED: OnceLock<BuildMetadata> = OnceLock::new();

/// Fail-closed default used in `--lib` unit tests when no thread-local
/// override is installed.
#[cfg(test)]
pub const ISOLATED_TEST_DEFAULT: BuildMetadata = BuildMetadata {
    ingest_contract: "isolated_unit_test",
    ingest_reuse: false,
    ingest_reuse_blocker: "isolated_unit_test",
    build_commit: None,
    build_commit_date: None,
    git_sha: None,
};

/// Pinned test metadata for `--lib` unit tests that explicitly exercise
/// `ingest_reuse: true` on their own thread.
#[cfg(test)]
pub const PINNED_TEST_METADATA: BuildMetadata = BuildMetadata {
    ingest_contract: "pinned_unit_test_contract",
    ingest_reuse: true,
    ingest_reuse_blocker: "",
    build_commit: None,
    build_commit_date: None,
    git_sha: None,
};

#[cfg(test)]
thread_local! {
    static TEST_OVERRIDE: std::cell::Cell<Option<&'static BuildMetadata>> =
        const { std::cell::Cell::new(None) };
}

/// RAII guard restoring the previous thread-local [`BuildMetadata`] override
/// on drop.
#[cfg(test)]
pub struct TestBuildMetadataGuard(Option<&'static BuildMetadata>);

#[cfg(test)]
impl Drop for TestBuildMetadataGuard {
    fn drop(&mut self) {
        let previous = self.0;
        TEST_OVERRIDE.with(|slot| slot.set(previous));
    }
}

/// Install a thread-local [`BuildMetadata`] override for the lifetime of the
/// returned RAII guard.
#[cfg(test)]
#[must_use]
pub fn install_thread_local_test_metadata(
    meta: &'static BuildMetadata,
) -> TestBuildMetadataGuard {
    let previous = TEST_OVERRIDE.with(|slot| slot.replace(Some(meta)));
    TestBuildMetadataGuard(previous)
}

impl BuildMetadata {
    #[cfg(test)]
    pub const ISOLATED_TEST_DEFAULT: Self = ISOLATED_TEST_DEFAULT;

    #[cfg(test)]
    pub const PINNED_TEST_METADATA: Self = PINNED_TEST_METADATA;

    /// Register process-wide build metadata idempotently.
    ///
    /// Repeated calls with identical metadata are no-ops; a conflicting
    /// registration panics immediately.
    pub fn register_idempotent(meta: BuildMetadata) {
        let existing = REGISTERED.get_or_init(|| meta);
        if *existing != meta {
            panic!("BuildMetadata::register_idempotent called with conflicting metadata");
        }
    }

    /// Return the active build metadata.
    ///
    /// Outside `--lib` unit tests this strictly requires prior registration via
    /// `coding_agent_search::ensure_runtime_hooks_registered()` and panics if
    /// unregistered.
    #[cfg(not(test))]
    pub fn current() -> &'static BuildMetadata {
        REGISTERED.get().expect(
            "BuildMetadata not registered; call coding_agent_search::ensure_runtime_hooks_registered() at entry",
        )
    }

    /// Return the active build metadata for `--lib` unit tests.
    ///
    /// Reads the thread-local override first, then any registered metadata, and
    /// otherwise falls back to [`ISOLATED_TEST_DEFAULT`] without mutating
    /// global state.
    #[cfg(test)]
    pub fn current() -> &'static BuildMetadata {
        TEST_OVERRIDE
            .with(std::cell::Cell::get)
            .or_else(|| REGISTERED.get())
            .unwrap_or(&ISOLATED_TEST_DEFAULT)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thread_local_override_restores_previous_metadata_on_drop() {
        assert_eq!(BuildMetadata::current(), &ISOLATED_TEST_DEFAULT);
        assert!(!BuildMetadata::current().ingest_reuse);
        {
            let _pinned = install_thread_local_test_metadata(&PINNED_TEST_METADATA);
            assert_eq!(BuildMetadata::current(), &PINNED_TEST_METADATA);
            assert!(BuildMetadata::current().ingest_reuse);
            {
                let _isolated = install_thread_local_test_metadata(&ISOLATED_TEST_DEFAULT);
                assert_eq!(BuildMetadata::current(), &ISOLATED_TEST_DEFAULT);
            }
            assert_eq!(BuildMetadata::current(), &PINNED_TEST_METADATA);
        }
        assert_eq!(BuildMetadata::current(), &ISOLATED_TEST_DEFAULT);
    }
}
