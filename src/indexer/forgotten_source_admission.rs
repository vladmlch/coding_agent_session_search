//! Local fix (review 2026-11-02, I1).
//!
//! `cass forget` stores the source file's size+mtime. Admission used to
//! compare that with `SourceFileStamp::of(path)`, which returns the
//! "absent/unreadable" default on any metadata error. An unreadable file
//! therefore looked "changed" and lifted the tombstone. An observation that
//! could not be made is not evidence of change.

use std::path::Path;

use crate::storage::sqlite::SourceFileStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ForgottenSourceObservation {
    /// Same stamp as recorded by `cass forget`: stay forgotten.
    Unchanged,
    /// Readable and different: the user's file really changed, re-admit.
    Changed,
    /// Metadata could not be read now: keep the tombstone.
    Unavailable,
}

pub(super) fn observe_forgotten_source(
    stored: &SourceFileStamp,
    path: &Path,
) -> ForgottenSourceObservation {
    let current = SourceFileStamp::of(path);
    if current == *stored {
        ForgottenSourceObservation::Unchanged
    } else if current == SourceFileStamp::default() {
        ForgottenSourceObservation::Unavailable
    } else {
        ForgottenSourceObservation::Changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn classifies_unchanged_changed_and_unavailable() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("s.jsonl");
        std::fs::write(&path, b"one").unwrap();
        let stored = SourceFileStamp::of(&path);
        assert_eq!(
            observe_forgotten_source(&stored, &path),
            ForgottenSourceObservation::Unchanged
        );

        std::fs::write(&path, b"one plus more bytes").unwrap();
        assert_eq!(
            observe_forgotten_source(&stored, &path),
            ForgottenSourceObservation::Changed
        );

        let missing = dir.path().join("missing.jsonl");
        assert_eq!(
            observe_forgotten_source(&stored, &missing),
            ForgottenSourceObservation::Unavailable
        );
    }

    #[test]
    fn a_source_forgotten_while_absent_stays_unchanged_while_absent() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("never.jsonl");
        assert_eq!(
            observe_forgotten_source(&SourceFileStamp::default(), &missing),
            ForgottenSourceObservation::Unchanged
        );
    }
}
