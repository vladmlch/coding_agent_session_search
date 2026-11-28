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
#[cfg(any(test, feature = "test-support"))]
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
#[cfg(any(test, feature = "test-support"))]
pub const PINNED_TEST_METADATA: BuildMetadata = BuildMetadata {
    ingest_contract: "pinned_unit_test_contract",
    ingest_reuse: true,
    ingest_reuse_blocker: "",
    build_commit: None,
    build_commit_date: None,
    git_sha: None,
};

#[cfg(any(test, feature = "test-support"))]
thread_local! {
    static TEST_OVERRIDE: std::cell::Cell<Option<&'static BuildMetadata>> =
        const { std::cell::Cell::new(None) };
}

/// RAII guard restoring the previous thread-local [`BuildMetadata`] override
/// on drop.
#[cfg(any(test, feature = "test-support"))]
pub struct TestBuildMetadataGuard(Option<&'static BuildMetadata>);

#[cfg(any(test, feature = "test-support"))]
impl Drop for TestBuildMetadataGuard {
    fn drop(&mut self) {
        let previous = self.0;
        TEST_OVERRIDE.with(|slot| slot.set(previous));
    }
}

/// Install a thread-local [`BuildMetadata`] override for the lifetime of the
/// returned RAII guard.
#[cfg(any(test, feature = "test-support"))]
#[must_use]
pub fn install_thread_local_test_metadata(meta: &'static BuildMetadata) -> TestBuildMetadataGuard {
    let previous = TEST_OVERRIDE.with(|slot| slot.replace(Some(meta)));
    TestBuildMetadataGuard(previous)
}

/// Registered metadata for ordinary library/binary consumers.
pub fn current() -> &'static BuildMetadata {
    BuildMetadata::current()
}

/// Unit-test-only access; never writes defaults into REGISTERED.
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub fn current_for_unit_test() -> &'static BuildMetadata {
    TEST_OVERRIDE
        .with(std::cell::Cell::get)
        .or_else(|| REGISTERED.get())
        .unwrap_or(&ISOLATED_TEST_DEFAULT)
}

impl BuildMetadata {
    #[cfg(any(test, feature = "test-support"))]
    pub const ISOLATED_TEST_DEFAULT: Self = ISOLATED_TEST_DEFAULT;

    #[cfg(any(test, feature = "test-support"))]
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
        current_for_unit_test()
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
    #[test]
    fn nested_guards_and_unwind_restore_previous_metadata() {
        let pinned = install_thread_local_test_metadata(&PINNED_TEST_METADATA);
        assert_eq!(current_for_unit_test(), &PINNED_TEST_METADATA);
        let isolated = install_thread_local_test_metadata(&ISOLATED_TEST_DEFAULT);
        assert_eq!(current_for_unit_test(), &ISOLATED_TEST_DEFAULT);
        drop(isolated);
        assert_eq!(current_for_unit_test(), &PINNED_TEST_METADATA);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _nested = install_thread_local_test_metadata(&ISOLATED_TEST_DEFAULT);
            panic!("forced unwind");
        }));
        assert!(result.is_err());
        assert_eq!(current_for_unit_test(), &PINNED_TEST_METADATA);
        drop(pinned);
        assert_eq!(current_for_unit_test(), &ISOLATED_TEST_DEFAULT);
    }

    #[test]
    fn second_thread_without_guard_sees_isolated_default() {
        let _pinned = install_thread_local_test_metadata(&PINNED_TEST_METADATA);
        let observed = std::thread::spawn(|| *current_for_unit_test())
            .join()
            .expect("observer thread");
        assert_eq!(observed, ISOLATED_TEST_DEFAULT);
        assert!(REGISTERED.get().is_none() || REGISTERED.get() != Some(&ISOLATED_TEST_DEFAULT));
    }
}
