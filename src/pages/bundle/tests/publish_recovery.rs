use std::fs;
use tempfile::TempDir;

use crate::pages::bundle::publish::{
    cleanup_journaled_bundle_tree_after_publish_with, recover_interrupted_bundle_publish,
    replace_dir_from_temp, replace_dir_from_temp_via_recoverable_rename_pair,
};
use crate::pages::bundle::publish_journal::{
    acquire_bundle_publish_guard, bundle_publish_lock_path, bundle_publish_recovery_journal_path,
    first_unowned_bundle_publish_backup, inspect_bundle_tree, inspect_bundle_tree_with_entry_limit,
    legacy_bundle_publish_in_progress_backup_path, require_bundle_publish_guard,
    unpredictable_bundle_publish_backup_path, write_bundle_publish_journal,
};

#[test]
fn test_replace_dir_from_temp_overwrites_existing_bundle() {
    let temp = TempDir::new().unwrap();
    let final_dir = temp.path().join("bundle");
    let staged_dir = temp.path().join("bundle.staged");

    fs::create_dir_all(final_dir.join("site")).unwrap();
    fs::write(final_dir.join("site/old.txt"), "old").unwrap();

    fs::create_dir_all(staged_dir.join("site")).unwrap();
    fs::write(staged_dir.join("site/new.txt"), "new").unwrap();

    let mut retain_temp_on_error = false;
    replace_dir_from_temp(&staged_dir, &final_dir, &mut retain_temp_on_error).unwrap();

    assert!(!staged_dir.exists());
    assert!(final_dir.join("site/new.txt").exists());
    assert!(!final_dir.join("site/old.txt").exists());
    assert!(
        !bundle_publish_recovery_journal_path(&final_dir).exists(),
        "completed publication journal should be cleaned up"
    );
    assert!(
        first_unowned_bundle_publish_backup(&final_dir, None)
            .unwrap()
            .is_none(),
        "completed publication should not leave a recovery backup"
    );
}

#[test]
fn test_concurrent_publisher_is_refused_while_lock_is_held() {
    // 70o8f: per-output single-publisher arbitration — while one
    // publisher holds the advisory lock, a second publication attempt
    // for the SAME output fails fast with the contention error instead
    // of racing the rename pair.
    let temp = TempDir::new().unwrap();
    let final_dir = temp.path().join("bundle");
    let staged_dir = temp.path().join("bundle.staged");
    fs::create_dir_all(staged_dir.join("site")).unwrap();
    fs::write(staged_dir.join("site/new.txt"), "new").unwrap();

    let held = acquire_bundle_publish_guard(&final_dir).unwrap();
    let mut retain_temp_on_error = false;
    let error = replace_dir_from_temp(&staged_dir, &final_dir, &mut retain_temp_on_error)
        .expect_err("second publisher must be refused while the lock is held");
    assert!(
        error.to_string().contains("already active"),
        "expected lock-contention arbitration error, got: {error:#}"
    );
    assert!(
        staged_dir.join("site/new.txt").exists(),
        "refused publisher must leave its staged bundle untouched"
    );
    assert!(
        !final_dir.exists(),
        "refused publisher must not create the output"
    );
    drop(held);

    // Once the first publisher releases, the same publication succeeds.
    let mut retain_temp_on_error = false;
    replace_dir_from_temp(&staged_dir, &final_dir, &mut retain_temp_on_error).unwrap();
    assert!(final_dir.join("site/new.txt").exists());
}

#[test]
fn test_swapped_lock_entry_is_refused_by_guard_revalidation() {
    // 70o8f: an attacker (or a confused cleaner) replacing the lock
    // pathname after acquisition must be detected — the guard re-checks
    // the pathname's file identity before authorizing mutation.
    let temp = TempDir::new().unwrap();
    let final_dir = temp.path().join("bundle");
    let guard = acquire_bundle_publish_guard(&final_dir).unwrap();
    let lock_path = bundle_publish_lock_path(&final_dir);

    // Swap the lock entry: remove and recreate at the same pathname.
    fs::remove_file(&lock_path).unwrap();
    fs::write(&lock_path, b"impostor").unwrap();

    let error = require_bundle_publish_guard(&final_dir, &guard)
        .expect_err("guard revalidation must refuse a swapped lock entry");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("identity"),
        "expected an identity-mismatch refusal, got: {rendered}"
    );
}

#[test]
fn test_non_regular_lock_entry_is_refused_fail_closed() {
    // 70o8f: a pre-existing non-regular-file entry at the lock pathname
    // (e.g. a directory planted by an attacker) must fail closed rather
    // than being followed or replaced.
    let temp = TempDir::new().unwrap();
    let final_dir = temp.path().join("bundle");
    let lock_path = bundle_publish_lock_path(&final_dir);
    fs::create_dir_all(&lock_path).unwrap();

    let error = acquire_bundle_publish_guard(&final_dir)
        .expect_err("a directory at the lock pathname must be refused");
    assert!(
        error.to_string().contains("not a regular file"),
        "expected fail-closed non-regular-file refusal, got: {error:#}"
    );
}

#[test]
fn test_replace_dir_from_temp_preserves_first_publish_behavior() {
    let temp = TempDir::new().unwrap();
    let final_dir = temp.path().join("bundle");
    let staged_dir = temp.path().join("bundle.staged");

    fs::create_dir_all(staged_dir.join("site")).unwrap();
    fs::write(staged_dir.join("site/new.txt"), "new").unwrap();

    let mut retain_temp_on_error = false;
    replace_dir_from_temp(&staged_dir, &final_dir, &mut retain_temp_on_error).unwrap();

    assert!(!retain_temp_on_error);
    assert!(!staged_dir.exists());
    assert_eq!(
        fs::read_to_string(final_dir.join("site/new.txt")).unwrap(),
        "new"
    );
    assert!(!bundle_publish_recovery_journal_path(&final_dir).exists());
    assert!(
        first_unowned_bundle_publish_backup(&final_dir, None)
            .unwrap()
            .is_none()
    );
}

#[test]
fn test_recover_interrupted_bundle_publish_restores_parked_only_live_bundle() {
    let temp = TempDir::new().unwrap();
    let final_dir = temp.path().join("bundle");
    let staged_dir = temp.path().join("bundle.staged");

    fs::create_dir_all(final_dir.join("private")).unwrap();
    fs::write(final_dir.join("private/old-private.txt"), "old").unwrap();
    fs::create_dir_all(staged_dir.join("private")).unwrap();
    fs::write(staged_dir.join("private/new-private.txt"), "new").unwrap();
    let prior = inspect_bundle_tree(&final_dir, "prior").unwrap();
    let candidate = inspect_bundle_tree(&staged_dir, "candidate").unwrap();
    let backup_dir = unpredictable_bundle_publish_backup_path(&final_dir).unwrap();
    write_bundle_publish_journal(&final_dir, &backup_dir, &prior, &candidate).unwrap();

    // Failpoint state: the fallback publisher durably parked OLD, then
    // the process died before installing NEW at the live handle.
    fs::rename(&final_dir, &backup_dir).unwrap();
    assert!(!final_dir.exists());

    recover_interrupted_bundle_publish(&final_dir).unwrap();

    assert_eq!(
        fs::read_to_string(final_dir.join("private/old-private.txt")).unwrap(),
        "old"
    );
    assert!(!backup_dir.exists());
    assert_eq!(
        fs::read_to_string(staged_dir.join("private/new-private.txt")).unwrap(),
        "new",
        "recovery must not consume the next staged candidate"
    );
    assert!(!bundle_publish_recovery_journal_path(&final_dir).exists());
}

#[test]
fn test_recover_interrupted_fallback_publish_keeps_new_live_and_cleans_parked_old() {
    let temp = TempDir::new().unwrap();
    let final_dir = temp.path().join("bundle");
    let staged_dir = temp.path().join("bundle.staged");

    fs::create_dir_all(final_dir.join("private")).unwrap();
    fs::write(final_dir.join("private/old-private.txt"), "old").unwrap();
    fs::create_dir_all(staged_dir.join("private")).unwrap();
    fs::write(staged_dir.join("private/new-private.txt"), "new").unwrap();
    let prior = inspect_bundle_tree(&final_dir, "prior").unwrap();
    let candidate = inspect_bundle_tree(&staged_dir, "candidate").unwrap();
    let backup_dir = unpredictable_bundle_publish_backup_path(&final_dir).unwrap();
    write_bundle_publish_journal(&final_dir, &backup_dir, &prior, &candidate).unwrap();

    // Failpoint state: OLD was parked and NEW reached the live handle,
    // then the process died before removing OLD.
    fs::rename(&final_dir, &backup_dir).unwrap();
    fs::rename(&staged_dir, &final_dir).unwrap();

    recover_interrupted_bundle_publish(&final_dir).unwrap();

    assert_eq!(
        fs::read_to_string(final_dir.join("private/new-private.txt")).unwrap(),
        "new"
    );
    assert!(!final_dir.join("private/old-private.txt").exists());
    assert!(!backup_dir.exists());
    assert!(!bundle_publish_recovery_journal_path(&final_dir).exists());
}

#[test]
fn test_recover_interrupted_publish_preserves_unowned_legacy_sidecar() {
    let temp = TempDir::new().unwrap();
    let final_dir = temp.path().join("bundle");
    let backup_dir = legacy_bundle_publish_in_progress_backup_path(&final_dir);

    fs::create_dir_all(final_dir.join("site")).unwrap();
    fs::write(final_dir.join("site/live.txt"), "live").unwrap();
    fs::create_dir_all(backup_dir.join("unrelated")).unwrap();
    fs::write(backup_dir.join("unrelated/sentinel.txt"), "preserve").unwrap();

    let error = recover_interrupted_bundle_publish(&final_dir)
        .expect_err("an unowned legacy sidecar must be preserved");
    let message = format!("{error:#}");

    assert!(
        message.contains("unowned Pages bundle recovery artifact"),
        "unexpected error: {message}"
    );
    assert_eq!(
        fs::read_to_string(final_dir.join("site/live.txt")).unwrap(),
        "live"
    );
    assert_eq!(
        fs::read_to_string(backup_dir.join("unrelated/sentinel.txt")).unwrap(),
        "preserve"
    );
}

#[test]
fn test_recover_missing_live_never_adopts_unowned_legacy_sidecar() {
    let temp = TempDir::new().unwrap();
    let final_dir = temp.path().join("bundle");
    let backup_dir = legacy_bundle_publish_in_progress_backup_path(&final_dir);
    fs::create_dir_all(backup_dir.join("unrelated")).unwrap();
    fs::write(backup_dir.join("unrelated/sentinel.txt"), "preserve").unwrap();

    let error = recover_interrupted_bundle_publish(&final_dir)
        .expect_err("an unjournaled directory must never become the live bundle");

    assert!(error.to_string().contains("refusing to infer ownership"));
    assert!(!final_dir.exists());
    assert_eq!(
        fs::read_to_string(backup_dir.join("unrelated/sentinel.txt")).unwrap(),
        "preserve"
    );
}

#[test]
fn test_legacy_constant_marker_cannot_authorize_recursive_cleanup() {
    let temp = TempDir::new().unwrap();
    let final_dir = temp.path().join("bundle");
    let backup_dir = legacy_bundle_publish_in_progress_backup_path(&final_dir);
    fs::create_dir_all(final_dir.join("site")).unwrap();
    fs::write(
        final_dir.join(".cass-pages-publish-in-progress-v1"),
        b"cass-pages-publish-in-progress-v1\n",
    )
    .unwrap();
    fs::create_dir_all(backup_dir.join("unrelated")).unwrap();
    fs::write(backup_dir.join("unrelated/sentinel.txt"), "preserve").unwrap();

    recover_interrupted_bundle_publish(&final_dir)
        .expect_err("a public legacy marker must not confer ownership");

    assert!(final_dir.exists());
    assert_eq!(
        fs::read_to_string(backup_dir.join("unrelated/sentinel.txt")).unwrap(),
        "preserve"
    );
}

#[test]
fn test_recover_interrupted_atomic_publish_before_exchange_keeps_prior_live() {
    let temp = TempDir::new().unwrap();
    let final_dir = temp.path().join("bundle");
    let staged_dir = temp.path().join("bundle.staged");

    fs::create_dir_all(final_dir.join("private")).unwrap();
    fs::write(final_dir.join("private/old-private.txt"), "old").unwrap();
    fs::create_dir_all(staged_dir.join("private")).unwrap();
    fs::write(staged_dir.join("private/new-private.txt"), "new").unwrap();
    let prior = inspect_bundle_tree(&final_dir, "prior").unwrap();
    let candidate = inspect_bundle_tree(&staged_dir, "candidate").unwrap();
    let exchange_dir = unpredictable_bundle_publish_backup_path(&final_dir).unwrap();
    write_bundle_publish_journal(&final_dir, &exchange_dir, &prior, &candidate).unwrap();

    // Failpoint state: NEW reached the journaled exchange handle,
    // but the process died before the atomic exchange committed.
    fs::rename(&staged_dir, &exchange_dir).unwrap();

    recover_interrupted_bundle_publish(&final_dir).unwrap();

    assert_eq!(
        fs::read_to_string(final_dir.join("private/old-private.txt")).unwrap(),
        "old"
    );
    assert!(!final_dir.join("private/new-private.txt").exists());
    assert!(!exchange_dir.exists());
    assert!(!bundle_publish_recovery_journal_path(&final_dir).exists());
}

#[test]
fn test_recover_interrupted_atomic_publish_immediately_after_exchange_keeps_new_live() {
    let temp = TempDir::new().unwrap();
    let final_dir = temp.path().join("bundle");
    let staged_dir = temp.path().join("bundle.staged");
    let parked_dir = temp.path().join("bundle.parked-for-test");
    fs::create_dir_all(final_dir.join("private")).unwrap();
    fs::write(final_dir.join("private/old-private.txt"), "old").unwrap();
    fs::create_dir_all(staged_dir.join("private")).unwrap();
    fs::write(staged_dir.join("private/new-private.txt"), "new").unwrap();
    let prior = inspect_bundle_tree(&final_dir, "prior").unwrap();
    let candidate = inspect_bundle_tree(&staged_dir, "candidate").unwrap();
    let exchange_dir = unpredictable_bundle_publish_backup_path(&final_dir).unwrap();
    write_bundle_publish_journal(&final_dir, &exchange_dir, &prior, &candidate).unwrap();

    // Failpoint state immediately after exchange: NEW is live and OLD is
    // at the exact random path authenticated by the durable journal.
    fs::rename(&final_dir, &parked_dir).unwrap();
    fs::rename(&staged_dir, &final_dir).unwrap();
    fs::rename(&parked_dir, &exchange_dir).unwrap();

    recover_interrupted_bundle_publish(&final_dir).unwrap();

    assert_eq!(
        fs::read_to_string(final_dir.join("private/new-private.txt")).unwrap(),
        "new"
    );
    assert!(!final_dir.join("private/old-private.txt").exists());
    assert!(!exchange_dir.exists());
    assert!(!bundle_publish_recovery_journal_path(&final_dir).exists());
}

#[test]
fn test_recovery_rejects_journaled_backup_after_content_mutation() {
    let temp = TempDir::new().unwrap();
    let final_dir = temp.path().join("bundle");
    let staged_dir = temp.path().join("bundle.staged");
    fs::create_dir_all(final_dir.join("private")).unwrap();
    fs::write(final_dir.join("private/old-private.txt"), "old").unwrap();
    fs::create_dir_all(staged_dir.join("private")).unwrap();
    fs::write(staged_dir.join("private/new-private.txt"), "new").unwrap();
    let prior = inspect_bundle_tree(&final_dir, "prior").unwrap();
    let candidate = inspect_bundle_tree(&staged_dir, "candidate").unwrap();
    let backup_dir = unpredictable_bundle_publish_backup_path(&final_dir).unwrap();
    write_bundle_publish_journal(&final_dir, &backup_dir, &prior, &candidate).unwrap();
    fs::rename(&final_dir, &backup_dir).unwrap();
    fs::write(backup_dir.join("private/old-private.txt"), "tampered").unwrap();

    let error = recover_interrupted_bundle_publish(&final_dir)
        .expect_err("mutated journaled bytes must not be restored");

    assert!(
        error
            .to_string()
            .contains("does not match the prior generation")
    );
    assert!(!final_dir.exists());
    assert_eq!(
        fs::read_to_string(backup_dir.join("private/old-private.txt")).unwrap(),
        "tampered"
    );
    assert!(bundle_publish_recovery_journal_path(&final_dir).exists());
}

#[test]
fn test_recoverable_rename_pair_replaces_and_cleans_prior_bundle() {
    let temp = TempDir::new().unwrap();
    let final_dir = temp.path().join("bundle");
    let staged_dir = temp.path().join("bundle.staged");

    fs::create_dir_all(final_dir.join("site")).unwrap();
    fs::write(final_dir.join("site/old.txt"), "old").unwrap();
    fs::create_dir_all(staged_dir.join("site")).unwrap();
    fs::write(staged_dir.join("site/new.txt"), "new").unwrap();
    let prior = inspect_bundle_tree(&final_dir, "prior").unwrap();
    let candidate = inspect_bundle_tree(&staged_dir, "candidate").unwrap();
    let backup_dir = unpredictable_bundle_publish_backup_path(&final_dir).unwrap();
    let journal =
        write_bundle_publish_journal(&final_dir, &backup_dir, &prior, &candidate).unwrap();

    let mut retain_temp_on_error = false;
    replace_dir_from_temp_via_recoverable_rename_pair(
        &staged_dir,
        &final_dir,
        &backup_dir,
        &journal,
        &prior,
        &candidate,
        &mut retain_temp_on_error,
    )
    .unwrap();

    assert!(!retain_temp_on_error);
    assert_eq!(
        fs::read_to_string(final_dir.join("site/new.txt")).unwrap(),
        "new"
    );
    assert!(!backup_dir.exists());
    assert!(!bundle_publish_recovery_journal_path(&final_dir).exists());
}

#[test]
fn test_prior_private_bundle_cleanup_failure_names_every_retained_path() {
    let temp = TempDir::new().unwrap();
    let final_dir = temp.path().join("bundle");
    let backup_dir = temp.path().join("journaled-backup");
    fs::create_dir_all(final_dir.join("site")).unwrap();
    fs::create_dir_all(backup_dir.join("private")).unwrap();
    fs::write(backup_dir.join("private/recovery-material.txt"), "private").unwrap();
    let expected = inspect_bundle_tree(&backup_dir, "prior").unwrap();

    let error = cleanup_journaled_bundle_tree_after_publish_with(
        &backup_dir,
        &final_dir,
        "journaled prior Pages bundle",
        &expected,
        |_| {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "injected cleanup denial",
            ))
        },
    )
    .expect_err("cleanup failure must fail the publication result");
    let message = format!("{error:#}");

    assert!(message.contains(&final_dir.display().to_string()));
    assert!(message.contains(&backup_dir.display().to_string()));
    assert!(message.contains("injected cleanup denial"));
    assert!(message.contains("private artifacts retained"));
    assert!(backup_dir.join("private/recovery-material.txt").exists());
}

#[test]
fn test_cleanup_refuses_mutated_tree_before_recursive_removal() {
    let temp = TempDir::new().unwrap();
    let final_dir = temp.path().join("bundle");
    let backup_dir = temp.path().join("journaled-backup");
    fs::create_dir_all(final_dir.join("site")).unwrap();
    fs::create_dir_all(backup_dir.join("private")).unwrap();
    let private_file = backup_dir.join("private/recovery-material.txt");
    fs::write(&private_file, "private").unwrap();
    let expected = inspect_bundle_tree(&backup_dir, "prior").unwrap();
    fs::write(&private_file, "changed after journal").unwrap();
    let remove_called = std::cell::Cell::new(false);

    cleanup_journaled_bundle_tree_after_publish_with(
        &backup_dir,
        &final_dir,
        "journaled prior Pages bundle",
        &expected,
        |_| {
            remove_called.set(true);
            Ok(())
        },
    )
    .expect_err("tree mutation must stop before recursive cleanup");

    assert!(!remove_called.get());
    assert_eq!(
        fs::read_to_string(private_file).unwrap(),
        "changed after journal"
    );
}

#[test]
fn test_bundle_tree_evidence_entry_bound_fails_closed() {
    let temp = TempDir::new().unwrap();
    let tree = temp.path().join("bundle");
    fs::create_dir_all(&tree).unwrap();
    for (name, body) in [("a.txt", "a"), ("b.txt", "b"), ("c.txt", "c")] {
        fs::write(tree.join(name), body).unwrap();
    }

    let error = inspect_bundle_tree_with_entry_limit(&tree, "bounded bundle", 2)
        .expect_err("a tree beyond the evidence bound must fail closed");

    assert!(
        error
            .to_string()
            .contains("2-entry recovery-evidence bound")
    );
    assert_eq!(fs::read_to_string(tree.join("a.txt")).unwrap(), "a");
    assert_eq!(fs::read_to_string(tree.join("b.txt")).unwrap(), "b");
    assert_eq!(fs::read_to_string(tree.join("c.txt")).unwrap(), "c");
}

#[test]
#[cfg(unix)]
fn test_recover_interrupted_bundle_publish_rejects_symlinked_backup() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();
    let final_dir = temp.path().join("bundle");
    let staged_dir = temp.path().join("bundle.staged");
    fs::create_dir_all(final_dir.join("site")).unwrap();
    fs::write(final_dir.join("site/old.txt"), "old").unwrap();
    fs::create_dir_all(staged_dir.join("site")).unwrap();
    fs::write(staged_dir.join("site/new.txt"), "new").unwrap();
    let prior = inspect_bundle_tree(&final_dir, "prior").unwrap();
    let candidate = inspect_bundle_tree(&staged_dir, "candidate").unwrap();
    let backup_dir = unpredictable_bundle_publish_backup_path(&final_dir).unwrap();
    write_bundle_publish_journal(&final_dir, &backup_dir, &prior, &candidate).unwrap();
    fs::create_dir_all(outside.path().join("private")).unwrap();
    fs::write(outside.path().join("private/material.txt"), "private").unwrap();
    symlink(outside.path(), &backup_dir).unwrap();

    let error = recover_interrupted_bundle_publish(&final_dir)
        .expect_err("a symlinked recovery backup must fail closed");

    assert!(error.to_string().contains("must not be a symlink"));
    assert!(
        error
            .to_string()
            .contains(&backup_dir.display().to_string())
    );
    assert_eq!(
        fs::read_to_string(outside.path().join("private/material.txt")).unwrap(),
        "private"
    );
    assert_eq!(
        fs::read_to_string(final_dir.join("site/old.txt")).unwrap(),
        "old"
    );
}

#[test]
#[cfg(unix)]
fn test_replace_dir_from_temp_rejects_symlinked_staging_tree() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();
    let final_dir = temp.path().join("bundle");
    let staged_dir = temp.path().join("bundle.staged");
    fs::create_dir_all(final_dir.join("site")).unwrap();
    fs::write(final_dir.join("site/old.txt"), "old").unwrap();
    fs::create_dir_all(outside.path().join("site")).unwrap();
    fs::write(outside.path().join("site/new.txt"), "new").unwrap();
    symlink(outside.path(), &staged_dir).unwrap();

    let mut retain_temp_on_error = false;
    let error = replace_dir_from_temp(&staged_dir, &final_dir, &mut retain_temp_on_error)
        .expect_err("a symlinked staged bundle must fail closed");

    assert!(error.to_string().contains("must not be a symlink"));
    assert!(
        error
            .to_string()
            .contains(&staged_dir.display().to_string())
    );
    assert_eq!(
        fs::read_to_string(final_dir.join("site/old.txt")).unwrap(),
        "old"
    );
    assert_eq!(
        fs::read_to_string(outside.path().join("site/new.txt")).unwrap(),
        "new"
    );
    assert!(!retain_temp_on_error);
}

#[test]
#[cfg(unix)]
fn test_replace_dir_from_temp_rejects_dangling_symlink_target() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let final_dir = temp.path().join("bundle");
    let staged_dir = temp.path().join("bundle.staged");

    fs::create_dir_all(staged_dir.join("site")).unwrap();
    fs::write(staged_dir.join("site/new.txt"), "new").unwrap();
    symlink(temp.path().join("missing-target"), &final_dir).unwrap();

    let mut retain_temp_on_error = false;
    let err =
        replace_dir_from_temp(&staged_dir, &final_dir, &mut retain_temp_on_error).unwrap_err();
    assert!(
        err.to_string().contains("must not be a symlink"),
        "unexpected error: {err:#}"
    );
    assert!(staged_dir.join("site/new.txt").exists());
    assert!(
        !retain_temp_on_error,
        "ordinary validation failures do not require recovery retention"
    );
    assert!(
        fs::symlink_metadata(&final_dir)
            .unwrap()
            .file_type()
            .is_symlink(),
        "dangling symlink target must not be silently replaced"
    );
}
