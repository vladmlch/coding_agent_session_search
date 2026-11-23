use super::super::*;
use super::helpers::*;
use crate::model::types::{Agent, AgentKind, Conversation, Message, MessageRole};
use serial_test::serial;
use tempfile::TempDir;

/// An interrupted prior publish left a `.<name>.publish-in-progress.bak`
/// sidecar AND the live index has since been re-created (e.g. a
/// partially-recovered run followed by a fresh rebuild that
/// short-circuited on the completed checkpoint). The recovery path at
/// recover_or_finalize_interrupted_lexical_publish_backup() must move
/// the stale in-progress backup into retained-backup storage rather
/// than overwrite the newer live index.
///
/// Bead 0k0sk: retention is now bounded (default 1). Raise the cap
/// to 2 inside this test so both the sidecar-recovered backup AND
/// the publish's own retained prior-live remain observable — the
/// recovery-semantic invariant this test pins is orthogonal to the
/// retention policy. Marked #[serial] because it mutates a
/// process-wide env var.
#[test]
#[serial]
fn publish_staged_lexical_index_retains_stale_in_progress_backup_when_live_present() {
    let _prior = std::env::var("CASS_LEXICAL_PUBLISH_BACKUP_RETENTION").ok();
    unsafe {
        std::env::set_var("CASS_LEXICAL_PUBLISH_BACKUP_RETENTION", "2");
    }
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let index_path = index_dir(&data_dir).unwrap();

    // Populate the current live index with a "newer" conversation.
    let live_conv = norm_conv(
        Some("live-newer"),
        vec![
            norm_msg(0, 1_700_000_200_000),
            norm_msg(1, 1_700_000_200_100),
        ],
    );
    let mut live_index = TantivyIndex::open_or_create(&index_path).unwrap();
    live_index
        .add_messages_with_conversation_id(&live_conv, &live_conv.messages, Some(20))
        .unwrap();
    live_index.commit().unwrap();
    drop(live_index);

    // Leave a stale `.publish-in-progress.bak` sibling that looks
    // like an interrupted prior publish with its OWN (older)
    // conversation content. Different doc count so we can tell them
    // apart after recovery.
    let stale_conv = norm_conv(Some("stale-older"), vec![norm_msg(0, 1_700_000_100_000)]);
    let in_progress_backup_path = lexical_publish_in_progress_backup_path(&index_path);
    let mut stale_index = TantivyIndex::open_or_create(&in_progress_backup_path).unwrap();
    stale_index
        .add_messages_with_conversation_id(&stale_conv, &stale_conv.messages, Some(10))
        .unwrap();
    stale_index.commit().unwrap();
    drop(stale_index);
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&in_progress_backup_path)
            .unwrap()
            .unwrap()
            .docs,
        1,
        "precondition: stale in-progress backup has exactly 1 doc"
    );

    // Publish a brand-new staged index: this exercises the
    // recovery-then-publish path where BOTH a live index and a stale
    // in-progress backup exist.
    let stage_root = TempDirBuilder::new()
        .prefix("cass-test-stale-recovery-stage.")
        .tempdir_in(data_dir.parent().unwrap())
        .unwrap();
    let staged_index_path = stage_root.path().join("staged");
    let fresh_conv = norm_conv(
        Some("freshly-staged"),
        vec![
            norm_msg(0, 1_700_000_300_000),
            norm_msg(1, 1_700_000_300_100),
            norm_msg(2, 1_700_000_300_200),
        ],
    );
    let mut staged_index = TantivyIndex::open_or_create(&staged_index_path).unwrap();
    staged_index
        .add_messages_with_conversation_id(&fresh_conv, &fresh_conv.messages, Some(30))
        .unwrap();
    staged_index.commit().unwrap();
    drop(staged_index);

    publish_staged_lexical_index(&staged_index_path, &index_path).unwrap();

    // The live index must expose the freshly-staged publish (3 docs).
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        3,
        "live index must reflect the freshly-staged publish"
    );
    // The in-progress sidecar must be gone — recovery moved it.
    assert!(
        !in_progress_backup_path.exists(),
        "stale in-progress backup must be consumed by recovery"
    );

    // Retained backups dir must contain BOTH:
    //   - the stale older backup (1 doc) recovered from the sidecar
    //   - the prior live (2 docs) moved during the actual publish
    let backups_dir = lexical_publish_backups_dir(&index_path);
    let retained_backups: Vec<_> = fs::read_dir(&backups_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(
        retained_backups.len(),
        2,
        "both the stale sidecar and the prior live index must be retained as separate backups; got {retained_backups:?}"
    );
    let mut backup_doc_counts: Vec<usize> = retained_backups
        .iter()
        .map(|path| {
            crate::search::tantivy::searchable_index_summary(path)
                .unwrap()
                .unwrap()
                .docs
        })
        .collect();
    backup_doc_counts.sort_unstable();
    assert_eq!(
        backup_doc_counts,
        vec![1, 2],
        "retained backups must preserve both the stale-older (1 doc) \
         and the just-displaced-prior-live (2 docs) artifacts"
    );

    // Restore the prior env state so other serial tests see the
    // default retention cap.
    unsafe {
        match _prior {
            Some(v) => std::env::set_var("CASS_LEXICAL_PUBLISH_BACKUP_RETENTION", v),
            None => std::env::remove_var("CASS_LEXICAL_PUBLISH_BACKUP_RETENTION"),
        }
    }
}

/// Bead 0k0sk (cross-review finding): publish_staged_lexical_index
/// retains EVERY prior live index indefinitely under
/// .lexical-publish-backups/, with no pruning. For real corpora
/// (~50k docs, multi-hundred-MB Tantivy trees) this linearly exhausts
/// disk across repeated rebuilds. This test characterizes the
/// current unbounded-growth behavior so a follow-up fix (bounded
/// retention, K-most-recent policy) can flip this assertion to
/// `<= RETENTION_CAP` once landed. Until then, it prevents an
/// accidental "silent prune" change from going unnoticed.
///
/// Updated by bead 0k0sk: bounded retention is now implemented.
/// Default cap = 1 (keep only the most-recent prior-live), overridable
/// via `CASS_LEXICAL_PUBLISH_BACKUP_RETENTION`. This test now asserts
/// A COMPLETED checkpoint must never resume into a surviving scratch dir.
///
/// Regression for a bug in the staged-publish work: the resume branch keyed
/// only on "a scratch directory exists". But a scratch also survives a crash
/// in the window between the publish RENAME_EXCHANGE and the rename that
/// parks the prior-live index as a backup — and in THAT state the scratch
/// holds the PRIOR-LIVE index while the live path already holds the newer
/// one. Resuming into it would publish the older index back over the newer
/// one and record it as a completed rebuild.
///
/// The guard is `rebuild_state.is_incomplete()`. This test pins the decision
/// the guard makes, so removing it turns the assertion red.
#[test]
fn completed_checkpoint_must_not_resume_into_a_surviving_scratch_dir() {
    let tmp = TempDir::new().expect("temp dir");
    let index_path = tmp.path().join("v9-quill");
    std::fs::create_dir_all(&index_path).expect("create live index dir");

    let scratch = staged_lexical_rebuild_scratch_path(&index_path);
    std::fs::create_dir_all(&scratch).expect("create surviving scratch");
    assert!(scratch.is_dir(), "fixture must have a scratch directory");

    let db_state = LexicalRebuildDbState {
        db_path: "/fixture/agent_search.db".to_string(),
        total_conversations: 1,
        total_messages: 1,
        storage_fingerprint: "content-v1:1:1:1".to_string(),
    };
    let mut completed = LexicalRebuildState::new(db_state.clone(), LEXICAL_REBUILD_PAGE_SIZE);
    completed.mark_completed(Some("fingerprint".to_string()));
    assert!(
        !completed.is_incomplete(),
        "fixture must model a COMPLETED checkpoint"
    );

    let mut interrupted = LexicalRebuildState::new(db_state, LEXICAL_REBUILD_PAGE_SIZE);
    interrupted.processed_conversations = 7;
    assert!(
        interrupted.is_incomplete(),
        "fixture must model an INTERRUPTED checkpoint"
    );

    // This mirrors the production predicate in
    // `rebuild_tantivy_from_db_with_options`: resume into scratch only when
    // the scratch exists AND the checkpoint is incomplete.
    let resumes = |state: &LexicalRebuildState| scratch.is_dir() && state.is_incomplete();

    assert!(
        !resumes(&completed),
        "a completed checkpoint must NOT adopt a surviving scratch dir: that scratch \
         holds the PRIOR-LIVE index and republishing it would roll the live index back"
    );
    assert!(
        resumes(&interrupted),
        "an interrupted checkpoint SHOULD resume into its partial scratch build"
    );
}

/// During a staged build the checkpoint fingerprint must track the CONTENT
/// directory, not the live one.
///
/// Regression: `commit_lexical_rebuild_progress` committed content to the
/// scratch index but fingerprinted `index_path` — the live directory, which
/// a staged build never touches. The fingerprint exists to detect "the index
/// changed underneath this checkpoint", so pointing it at the wrong
/// directory made it watch something unrelated: a concurrent publish to the
/// live path would invalidate a checkpoint whose staged content was fine.
#[test]
fn staged_rebuild_checkpoint_fingerprints_the_content_dir_not_the_live_dir() {
    let tmp = TempDir::new().expect("temp dir");
    let index_path = tmp.path().join("v9-quill");
    std::fs::create_dir_all(&index_path).expect("create live index dir");
    let content_path = staged_lexical_rebuild_scratch_path(&index_path);
    std::fs::create_dir_all(&content_path).expect("create scratch content dir");

    // Two DIFFERENT published manifests, so the fingerprints cannot collide.
    std::fs::write(
        index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"live-manifest-bytes",
    )
    .expect("write live manifest");
    std::fs::write(
        content_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER),
        b"staged-manifest-bytes-which-differ",
    )
    .expect("write staged manifest");

    let live_fp = index_meta_fingerprint(&index_path)
        .expect("live fingerprint")
        .expect("live index must fingerprint");
    let content_fp = index_meta_fingerprint(&content_path)
        .expect("content fingerprint")
        .expect("staged index must fingerprint");

    assert_ne!(
        live_fp, content_fp,
        "fixture is vacuous unless the two directories fingerprint differently"
    );

    // What a staged commit must record is the CONTENT fingerprint.
    let mut state = LexicalRebuildState::new(
        LexicalRebuildDbState {
            db_path: "/fixture/agent_search.db".to_string(),
            total_conversations: 1,
            total_messages: 1,
            storage_fingerprint: "content-v1:1:1:1".to_string(),
        },
        LEXICAL_REBUILD_PAGE_SIZE,
    );
    state.finalize_commit(Some(content_fp.clone()));
    assert_eq!(
        state.committed_meta_fingerprint.as_deref(),
        Some(content_fp.as_str()),
        "a staged checkpoint must record the fingerprint of the index it just committed"
    );
    assert_ne!(
        state.committed_meta_fingerprint.as_deref(),
        Some(live_fp.as_str()),
        "recording the LIVE fingerprint would watch a directory the staged build never writes"
    );
}

/// the default-cap behavior and is serialized on the env lock because
/// it touches a process-wide env var.
#[test]
#[serial]
fn publish_staged_lexical_index_prunes_retained_backups_to_default_retention_cap() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let index_path = index_dir(&data_dir).unwrap();

    // Force default retention (remove the env if anything else set it).
    // The test is #[serial] so we can safely mutate the env for the
    // duration of this test.
    let _prior = std::env::var("CASS_LEXICAL_PUBLISH_BACKUP_RETENTION").ok();
    unsafe {
        std::env::remove_var("CASS_LEXICAL_PUBLISH_BACKUP_RETENTION");
    }

    // Seed an initial live index so the first publish exercises the
    // "prior-live-exists" branch that actually retains a backup.
    let seed = norm_conv(Some("retention-seed"), vec![norm_msg(0, 1_700_000_500_000)]);
    let mut live = TantivyIndex::open_or_create(&index_path).unwrap();
    live.add_messages_with_conversation_id(&seed, &seed.messages, Some(100))
        .unwrap();
    live.commit().unwrap();
    drop(live);

    let publish_count = 4_usize;
    for iteration in 0..publish_count {
        let stage_root = TempDirBuilder::new()
            .prefix(&format!("cass-test-retention-stage-{iteration}."))
            .tempdir_in(data_dir.parent().unwrap())
            .unwrap();
        let staged_index_path = stage_root.path().join("staged");
        let conv = norm_conv(
            Some(&format!("retention-iter-{iteration}")),
            vec![norm_msg(0, 1_700_000_510_000 + iteration as i64)],
        );
        let mut staged = TantivyIndex::open_or_create(&staged_index_path).unwrap();
        staged
            .add_messages_with_conversation_id(&conv, &conv.messages, Some(200 + iteration as i64))
            .unwrap();
        staged.commit().unwrap();
        drop(staged);

        publish_staged_lexical_index(&staged_index_path, &index_path).unwrap();
    }

    let backups_dir = lexical_publish_backups_dir(&index_path);
    let retained_backups: Vec<_> = fs::read_dir(&backups_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();

    // New contract (bead 0k0sk): default retention cap of 1 means
    // exactly 1 retained backup regardless of publish count.
    assert_eq!(
        retained_backups.len(),
        1,
        "default retention cap is 1, got {} retained after {publish_count} publishes: {retained_backups:?}",
        retained_backups.len()
    );

    // Restore prior env state for any subsequent tests.
    unsafe {
        match _prior {
            Some(v) => std::env::set_var("CASS_LEXICAL_PUBLISH_BACKUP_RETENTION", v),
            None => std::env::remove_var("CASS_LEXICAL_PUBLISH_BACKUP_RETENTION"),
        }
    }
}

/// Regression guard for bead 0k0sk: the retention cap is configurable
/// via `CASS_LEXICAL_PUBLISH_BACKUP_RETENTION`. This test exercises
/// two distinct non-default values (0 = no retention, 3 = deeper
/// rollback history) to prove the knob actually flows through
/// `lexical_publish_backup_retention_limit()` into the prune step.
#[test]
#[serial]
fn publish_staged_lexical_index_retention_cap_is_env_configurable() {
    let _prior = std::env::var("CASS_LEXICAL_PUBLISH_BACKUP_RETENTION").ok();

    for (cap_value, expected_retained) in [("0", 0_usize), ("3", 3_usize)] {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().join("data");
        fs::create_dir_all(&data_dir).unwrap();
        let index_path = index_dir(&data_dir).unwrap();

        unsafe {
            std::env::set_var("CASS_LEXICAL_PUBLISH_BACKUP_RETENTION", cap_value);
        }

        // Seed live + do 5 successive publishes.
        let seed = norm_conv(
            Some(&format!("retention-env-seed-{cap_value}")),
            vec![norm_msg(0, 1_700_000_600_000)],
        );
        let mut live = TantivyIndex::open_or_create(&index_path).unwrap();
        live.add_messages_with_conversation_id(&seed, &seed.messages, Some(100))
            .unwrap();
        live.commit().unwrap();
        drop(live);

        for iteration in 0..5_usize {
            let stage_root = TempDirBuilder::new()
                .prefix(&format!("cass-test-retention-env-{cap_value}-{iteration}."))
                .tempdir_in(data_dir.parent().unwrap())
                .unwrap();
            let staged_index_path = stage_root.path().join("staged");
            let conv = norm_conv(
                Some(&format!("retention-env-{cap_value}-iter-{iteration}")),
                vec![norm_msg(0, 1_700_000_610_000 + iteration as i64)],
            );
            let mut staged = TantivyIndex::open_or_create(&staged_index_path).unwrap();
            staged
                .add_messages_with_conversation_id(
                    &conv,
                    &conv.messages,
                    Some(200 + iteration as i64),
                )
                .unwrap();
            staged.commit().unwrap();
            drop(staged);

            publish_staged_lexical_index(&staged_index_path, &index_path).unwrap();
        }

        let backups_dir = lexical_publish_backups_dir(&index_path);
        let retained: Vec<_> = if backups_dir.exists() {
            fs::read_dir(&backups_dir)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect()
        } else {
            Vec::new()
        };
        assert_eq!(
            retained.len(),
            expected_retained,
            "CASS_LEXICAL_PUBLISH_BACKUP_RETENTION={cap_value} should produce {expected_retained} retained backups after 5 publishes; got {}: {retained:?}",
            retained.len()
        );
    }

    // Restore prior env state.
    unsafe {
        match _prior {
            Some(v) => std::env::set_var("CASS_LEXICAL_PUBLISH_BACKUP_RETENTION", v),
            None => std::env::remove_var("CASS_LEXICAL_PUBLISH_BACKUP_RETENTION"),
        }
    }
}

/// Staged path must exist before `publish_staged_lexical_index` is
/// called. Calling it with a missing staged tree must return Err
/// without mutating the live index — a partial-mutation leak here
/// would silently roll the live surface backward.
#[test]
fn publish_staged_lexical_index_errors_cleanly_when_staged_path_does_not_exist() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let index_path = index_dir(&data_dir).unwrap();

    // Populate the live index so we can assert it stays intact if the
    // publish call errors.
    let seed = norm_conv(
        Some("missing-staged-sentinel"),
        vec![norm_msg(0, 1_700_000_700_000)],
    );
    let mut live_index = TantivyIndex::open_or_create(&index_path).unwrap();
    live_index
        .add_messages_with_conversation_id(&seed, &seed.messages, Some(700))
        .unwrap();
    live_index.commit().unwrap();
    drop(live_index);
    let live_docs_before = crate::search::tantivy::searchable_index_summary(&index_path)
        .unwrap()
        .unwrap()
        .docs;

    let nonexistent_staged = data_dir.parent().unwrap().join("definitely-not-staged");
    assert!(
        !nonexistent_staged.exists(),
        "precondition: staged path must be missing"
    );

    let result = publish_staged_lexical_index(&nonexistent_staged, &index_path);
    assert!(
        result.is_err(),
        "publish with missing staged path must return Err, got {result:?}"
    );

    // Live index must be untouched: docs preserved, path still a
    // directory, no partial `.publish-in-progress.bak` sidecar left
    // behind.
    let live_docs_after = crate::search::tantivy::searchable_index_summary(&index_path)
        .unwrap()
        .unwrap()
        .docs;
    assert_eq!(
        live_docs_after, live_docs_before,
        "live index must not regress after a missing-staged publish error"
    );
    let in_progress = lexical_publish_in_progress_backup_path(&index_path);
    assert!(
        !in_progress.exists(),
        "no stale .publish-in-progress.bak should be left behind when publish errors early"
    );
}

/// `recover_or_finalize_interrupted_lexical_publish_backup` is the
/// entry point every publish calls before touching the live surface.
/// It must be idempotent: calling it on a fully-recovered state
/// (live present, no in-progress backup, no retained backups) MUST
/// be a pure no-op — no fs::rename attempts, no backup directory
/// churn, no spurious log events.
#[test]
fn recover_or_finalize_interrupted_lexical_publish_backup_is_idempotent_no_op() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let index_path = index_dir(&data_dir).unwrap();

    let seed = norm_conv(
        Some("idempotent-recovery"),
        vec![norm_msg(0, 1_700_000_800_000)],
    );
    let mut live_index = TantivyIndex::open_or_create(&index_path).unwrap();
    live_index
        .add_messages_with_conversation_id(&seed, &seed.messages, Some(800))
        .unwrap();
    live_index.commit().unwrap();
    drop(live_index);

    // No in-progress backup exists — function should early-return Ok
    // without creating the backups directory or otherwise mutating
    // the filesystem.
    assert!(
        !lexical_publish_in_progress_backup_path(&index_path).exists(),
        "precondition: no in-progress backup sidecar"
    );
    let backups_dir = lexical_publish_backups_dir(&index_path);
    assert!(
        !backups_dir.exists(),
        "precondition: no retained-backup dir yet"
    );

    for call in 0..3 {
        recover_or_finalize_interrupted_lexical_publish_backup(&index_path).unwrap_or_else(|err| {
            panic!("idempotent recovery call {call} must not error; got {err:#}")
        });
    }

    // Live index untouched.
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        1,
        "live index doc count must not change across no-op recovery calls"
    );
    // Backups dir must NOT have been created by the no-op path —
    // `ensure_lexical_publish_backups_dir` is only called when there
    // is actual work to do, so idempotent recovery must not leave
    // empty `.lexical-publish-backups/` directories for garbage
    // collection to clean up later.
    assert!(
        !backups_dir.exists(),
        "no-op recovery must NOT create an empty backups directory; \
         found {backups_dir:?}"
    );
}

/// Rollback-during-publish crash model (non-Linux path, shared with
/// the Linux recovery flow): if the rename that parks the prior
/// live index into `.publish-in-progress.bak` succeeded but the
/// process crashed before the staged → live rename, a restart must
/// observe:
///   - live index path missing (rename took effect)
///   - in-progress backup sidecar populated with the prior content
///
/// The next call to `publish_staged_lexical_index` must restore the
/// prior live index from the sidecar AND land the staged publish on
/// top, producing one retained backup holding the old prior-live
/// content. This is the "crash between park and swap" rollback
/// contract that complements the two existing recovery tests from
/// b757d822.
#[test]
#[serial]
fn publish_staged_lexical_index_recovers_from_crash_between_park_and_swap() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let index_path = index_dir(&data_dir).unwrap();

    // Build the prior live index.
    let prior_conv = norm_conv(
        Some("crash-prior"),
        vec![
            norm_msg(0, 1_700_000_900_000),
            norm_msg(1, 1_700_000_900_100),
            norm_msg(2, 1_700_000_900_200),
        ],
    );
    let mut prior_live = TantivyIndex::open_or_create(&index_path).unwrap();
    prior_live
        .add_messages_with_conversation_id(&prior_conv, &prior_conv.messages, Some(900))
        .unwrap();
    prior_live.commit().unwrap();
    drop(prior_live);
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        3,
        "precondition: prior-live index has 3 docs"
    );

    // Simulate the crash exactly between the two renames: park the
    // live index as the in-progress sidecar, then DON'T perform the
    // staged-to-live rename. This matches the non-Linux crash
    // window and also corresponds to the Linux path's retain-rename
    // failure mode (atomic_exchange succeeded, fs::rename(staged,
    // retained) was never reached).
    let in_progress_backup_path = lexical_publish_in_progress_backup_path(&index_path);
    fs::rename(&index_path, &in_progress_backup_path).unwrap();
    assert!(!index_path.exists());
    assert!(in_progress_backup_path.exists());

    // Now the next cass invocation stages a new index and calls
    // publish. Recovery must restore prior-live under index_path
    // before the new staged publish lands on top.
    let stage_root = TempDirBuilder::new()
        .prefix("cass-test-crash-recovery-stage.")
        .tempdir_in(data_dir.parent().unwrap())
        .unwrap();
    let staged_index_path = stage_root.path().join("staged");
    let new_conv = norm_conv(Some("crash-new"), vec![norm_msg(0, 1_700_000_910_000)]);
    let mut staged = TantivyIndex::open_or_create(&staged_index_path).unwrap();
    staged
        .add_messages_with_conversation_id(&new_conv, &new_conv.messages, Some(901))
        .unwrap();
    staged.commit().unwrap();
    drop(staged);

    publish_staged_lexical_index(&staged_index_path, &index_path).unwrap();

    // Live = new staged (1 doc).
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        1,
        "post-recovery live must be the new staged publish (1 doc)"
    );
    // In-progress sidecar consumed.
    assert!(
        !in_progress_backup_path.exists(),
        "crash-window sidecar must be consumed by recovery"
    );
    // Exactly one retained backup holding the original 3-doc
    // prior-live content.
    let backups_dir = lexical_publish_backups_dir(&index_path);
    let retained: Vec<_> = fs::read_dir(&backups_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(retained.len(), 1);
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&retained[0])
            .unwrap()
            .unwrap()
            .docs,
        3,
        "retained backup must preserve the prior-live artifact recovered from the crash sidecar"
    );
}

/// Simulate a hard-kill restart boundary after the prior live index
/// has already been parked at `.<name>.publish-in-progress.bak` but
/// before any subsequent publish starts. Startup recovery must be
/// able to restore the prior live index from the stranded sidecar
/// without requiring a new staged publish to complete first.
#[test]
fn recover_or_finalize_interrupted_lexical_publish_backup_restores_live_index_without_new_publish()
{
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let index_path = index_dir(&data_dir).unwrap();

    let prior_conv = norm_conv(
        Some("restart-recovery-prior"),
        vec![
            norm_msg(0, 1_700_000_905_000),
            norm_msg(1, 1_700_000_905_100),
            norm_msg(2, 1_700_000_905_200),
        ],
    );
    let mut prior_live = TantivyIndex::open_or_create(&index_path).unwrap();
    prior_live
        .add_messages_with_conversation_id(&prior_conv, &prior_conv.messages, Some(905))
        .unwrap();
    prior_live.commit().unwrap();
    drop(prior_live);

    let in_progress_backup_path = lexical_publish_in_progress_backup_path(&index_path);
    fs::rename(&index_path, &in_progress_backup_path).unwrap();
    assert!(
        !index_path.exists(),
        "precondition: live index path should be absent after the simulated hard-kill window"
    );
    assert!(
        in_progress_backup_path.exists(),
        "precondition: interrupted publish sidecar should hold the recoverable live index"
    );

    recover_or_finalize_interrupted_lexical_publish_backup(&index_path).unwrap();

    assert!(
        index_path.exists(),
        "startup recovery must restore the live index path from the stranded sidecar"
    );
    assert!(
        !in_progress_backup_path.exists(),
        "startup recovery must consume the stranded sidecar after restoring the live index"
    );
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        3,
        "startup recovery must restore the exact prior-live document surface"
    );
    assert!(
        !lexical_publish_backups_dir(&index_path).exists(),
        "pure restore recovery should not materialize retained-backup storage when there is no newer live index yet"
    );
}

/// Simulate an ENOSPC-style failure while the Linux atomic publish
/// path is parking OLD at the canonical sidecar after the swap but
/// before retention. The publish must roll the swap back so the
/// prior live generation stays live and the new generation remains
/// stranded only in the staged tempdir.
#[test]
#[serial]
#[cfg(target_os = "linux")]
fn publish_staged_lexical_index_rolls_back_when_enospc_blocks_linux_sidecar_park() {
    const ENOSPC_RAW_OS_ERROR: i32 = 28;

    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let index_path = index_dir(&data_dir).unwrap();

    let prior_conv = norm_conv(
        Some("enospc-park-prior"),
        vec![
            norm_msg(0, 1_700_012_000_000),
            norm_msg(1, 1_700_012_000_100),
            norm_msg(2, 1_700_012_000_200),
        ],
    );
    let mut prior_live = TantivyIndex::open_or_create(&index_path).unwrap();
    prior_live
        .add_messages_with_conversation_id(&prior_conv, &prior_conv.messages, Some(1_200))
        .unwrap();
    prior_live.commit().unwrap();
    drop(prior_live);

    let stage_root = TempDirBuilder::new()
        .prefix("cass-test-enospc-park-stage.")
        .tempdir_in(data_dir.parent().unwrap())
        .unwrap();
    let staged_index_path = stage_root.path().join("staged");
    let new_conv = norm_conv(
        Some("enospc-park-new"),
        vec![norm_msg(0, 1_700_012_001_000)],
    );
    let mut staged = TantivyIndex::open_or_create(&staged_index_path).unwrap();
    staged
        .add_messages_with_conversation_id(&new_conv, &new_conv.messages, Some(1_201))
        .unwrap();
    staged.commit().unwrap();
    drop(staged);

    let _fault = inject_lexical_publish_rename_failure_once(
        LexicalPublishRenameSite::LinuxParkPriorLiveToCanonicalSidecar,
        ENOSPC_RAW_OS_ERROR,
    );
    let err = publish_staged_lexical_index(&staged_index_path, &index_path)
        .expect_err("ENOSPC while parking OLD must fail the publish");
    let err_text = format!("{err:#}");
    assert!(
        err_text.contains("rolled back to keep previous live index"),
        "expected rollback context in ENOSPC publish error, got: {err_text}"
    );

    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        3,
        "ENOSPC while parking OLD must leave the prior live index intact after rollback"
    );
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&staged_index_path)
            .unwrap()
            .unwrap()
            .docs,
        1,
        "rollback must return the new generation to the staged tempdir instead of discarding it"
    );
    assert!(
        !lexical_publish_in_progress_backup_path(&index_path).exists(),
        "rollback must not leave a stale canonical sidecar behind"
    );
    let retained_backups: Vec<_> = fs::read_dir(lexical_publish_backups_dir(&index_path))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert!(
        retained_backups.is_empty(),
        "failed pre-commit publish must not retain any prior-live backups yet; got {retained_backups:?}"
    );
}

/// Simulate an ENOSPC-style failure after the Linux atomic publish
/// has already committed NEW live content but before OLD can be
/// moved from the canonical sidecar into retained-backup storage.
/// The publish should still succeed, leave the sidecar intact, and
/// let the next recovery pass finish retention without changing live.
#[test]
#[serial]
#[cfg(target_os = "linux")]
fn publish_staged_lexical_index_recovers_after_enospc_blocks_linux_retained_backup_move() {
    const ENOSPC_RAW_OS_ERROR: i32 = 28;

    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let index_path = index_dir(&data_dir).unwrap();

    let prior_conv = norm_conv(
        Some("enospc-retain-prior"),
        vec![
            norm_msg(0, 1_700_012_100_000),
            norm_msg(1, 1_700_012_100_100),
            norm_msg(2, 1_700_012_100_200),
        ],
    );
    let mut prior_live = TantivyIndex::open_or_create(&index_path).unwrap();
    prior_live
        .add_messages_with_conversation_id(&prior_conv, &prior_conv.messages, Some(1_210))
        .unwrap();
    prior_live.commit().unwrap();
    drop(prior_live);

    let stage_root = TempDirBuilder::new()
        .prefix("cass-test-enospc-retain-stage.")
        .tempdir_in(data_dir.parent().unwrap())
        .unwrap();
    let staged_index_path = stage_root.path().join("staged");
    let new_conv = norm_conv(
        Some("enospc-retain-new"),
        vec![norm_msg(0, 1_700_012_101_000)],
    );
    let mut staged = TantivyIndex::open_or_create(&staged_index_path).unwrap();
    staged
        .add_messages_with_conversation_id(&new_conv, &new_conv.messages, Some(1_211))
        .unwrap();
    staged.commit().unwrap();
    drop(staged);

    let _fault = inject_lexical_publish_rename_failure_once(
        LexicalPublishRenameSite::LinuxRetainCanonicalSidecar,
        ENOSPC_RAW_OS_ERROR,
    );
    publish_staged_lexical_index(&staged_index_path, &index_path)
        .expect("ENOSPC while retaining OLD after the swap should keep NEW live");

    let canonical_sidecar = lexical_publish_in_progress_backup_path(&index_path);
    assert!(
        canonical_sidecar.exists(),
        "retain failure after commit must preserve the canonical sidecar for later recovery"
    );
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&canonical_sidecar)
            .unwrap()
            .unwrap()
            .docs,
        3,
        "the canonical sidecar must continue to hold the old live generation after retain ENOSPC"
    );
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        1,
        "retain ENOSPC must not roll back the already-published live generation"
    );
    let retained_backups: Vec<_> = fs::read_dir(lexical_publish_backups_dir(&index_path))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert!(
        retained_backups.is_empty(),
        "retain ENOSPC should leave recovery work in the canonical sidecar, not partially retain backups: {retained_backups:?}"
    );

    recover_or_finalize_interrupted_lexical_publish_backup(&index_path).unwrap();

    assert!(
        !canonical_sidecar.exists(),
        "recovery after retain ENOSPC must consume the canonical sidecar"
    );
    let retained_backups: Vec<_> = fs::read_dir(lexical_publish_backups_dir(&index_path))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(
        retained_backups.len(),
        1,
        "recovery should retain exactly one old live artifact after post-commit ENOSPC"
    );
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&retained_backups[0])
            .unwrap()
            .unwrap()
            .docs,
        3,
        "recovery must preserve the old live generation in retained-backup storage"
    );
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        1,
        "recovery after retain ENOSPC must not disturb the already-published live generation"
    );
}

/// Regression guard for bead coding_agent_session_search-9wkx5:
/// Linux atomic-swap publish path now parks OLD at the canonical
/// `.<name>.publish-in-progress.bak` sidecar AFTER renameat2
/// succeeds but BEFORE moving it to `.lexical-publish-backups/`.
///
/// This test simulates the resulting mid-publish state — sidecar
/// holds OLD, live holds NEW — then verifies that a subsequent
/// `publish_staged_lexical_index` call correctly recovers by
/// moving the sidecar into retained-backup storage as part of its
/// `recover_or_finalize_interrupted_lexical_publish_backup` pass.
///
/// Without the A.5 canonical-sidecar step in the Linux path, OLD
/// would have been stranded inside a `cass-lexical-merge.*`
/// TempDir and the retained-backup contract broken.
///
/// Bead 0k0sk: retention is now bounded (default 1). Raise the cap
/// to 2 inside this test so BOTH the A.5-sidecar-recovered OLD and
/// the prior-live NEW remain observable — the crash-recovery invariant
/// this test pins is orthogonal to the retention policy. Marked
/// #[serial] because it mutates a process-wide env var.
#[test]
#[serial]
fn publish_staged_lexical_index_recovers_from_crash_between_linux_swap_and_retain() {
    let _prior = std::env::var("CASS_LEXICAL_PUBLISH_BACKUP_RETENTION").ok();
    unsafe {
        std::env::set_var("CASS_LEXICAL_PUBLISH_BACKUP_RETENTION", "2");
    }
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let index_path = index_dir(&data_dir).unwrap();

    // Build the crash-state: live holds NEW (post-renameat2),
    // canonical sidecar holds OLD (post-A.5), but retain step (B)
    // never completed.
    let new_live_conv = norm_conv(
        Some("wkx5-linux-new-live"),
        vec![norm_msg(0, 1_700_009_000_000)],
    );
    let mut new_live = TantivyIndex::open_or_create(&index_path).unwrap();
    new_live
        .add_messages_with_conversation_id(&new_live_conv, &new_live_conv.messages, Some(1))
        .unwrap();
    new_live.commit().unwrap();
    drop(new_live);

    let canonical_sidecar = lexical_publish_in_progress_backup_path(&index_path);
    let old_backup_conv = norm_conv(
        Some("wkx5-linux-old-backup"),
        vec![
            norm_msg(0, 1_700_008_000_000),
            norm_msg(1, 1_700_008_000_100),
            norm_msg(2, 1_700_008_000_200),
        ],
    );
    let mut sidecar_index = TantivyIndex::open_or_create(&canonical_sidecar).unwrap();
    sidecar_index
        .add_messages_with_conversation_id(&old_backup_conv, &old_backup_conv.messages, Some(2))
        .unwrap();
    sidecar_index.commit().unwrap();
    drop(sidecar_index);

    assert!(
        canonical_sidecar.exists(),
        "precondition: canonical sidecar with OLD content"
    );
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        1,
        "precondition: live has NEW (1 doc)"
    );

    // Stage a third-generation index and publish. The publish will
    // first recover the A.5-sidecar via
    // `recover_or_finalize_interrupted_lexical_publish_backup`,
    // moving OLD into `.lexical-publish-backups/` before the new
    // generation lands.
    let stage_root = TempDirBuilder::new()
        .prefix("cass-test-wkx5-linux-stage.")
        .tempdir_in(data_dir.parent().unwrap())
        .unwrap();
    let staged_index_path = stage_root.path().join("staged");
    let third_gen_conv = norm_conv(
        Some("wkx5-linux-third-gen"),
        vec![
            norm_msg(0, 1_700_010_000_000),
            norm_msg(1, 1_700_010_000_100),
        ],
    );
    let mut third_gen = TantivyIndex::open_or_create(&staged_index_path).unwrap();
    third_gen
        .add_messages_with_conversation_id(&third_gen_conv, &third_gen_conv.messages, Some(3))
        .unwrap();
    third_gen.commit().unwrap();
    drop(third_gen);

    publish_staged_lexical_index(&staged_index_path, &index_path).unwrap();

    // Live index == third-gen (2 docs).
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        2,
        "live must be third-gen publish after recovery + publish"
    );
    // Canonical sidecar must be consumed.
    assert!(
        !canonical_sidecar.exists(),
        "canonical sidecar must be moved into retained-backup storage by recovery"
    );
    // Retained backups contain BOTH:
    //  - the OLD (3 docs) recovered from the A.5 sidecar,
    //  - the NEW (1 doc) moved during the third-gen publish's own retain step.
    let backups_dir = lexical_publish_backups_dir(&index_path);
    let retained: Vec<_> = fs::read_dir(&backups_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(
        retained.len(),
        2,
        "both the A.5 sidecar (OLD, 3 docs) and the prior-live (NEW, 1 doc) must be retained; got {retained:?}"
    );
    let mut doc_counts: Vec<usize> = retained
        .iter()
        .map(|path| {
            crate::search::tantivy::searchable_index_summary(path)
                .unwrap()
                .unwrap()
                .docs
        })
        .collect();
    doc_counts.sort_unstable();
    assert_eq!(
        doc_counts,
        vec![1, 3],
        "retained backups must have exactly the expected doc counts (1 = NEW prior-live, 3 = OLD from A.5 sidecar)"
    );

    // Restore prior env state so other serial tests see the default cap.
    unsafe {
        match _prior {
            Some(v) => std::env::set_var("CASS_LEXICAL_PUBLISH_BACKUP_RETENTION", v),
            None => std::env::remove_var("CASS_LEXICAL_PUBLISH_BACKUP_RETENTION"),
        }
    }
}

/// On Unix, unlinking a retained backup while a reader still has
/// that old index open must not corrupt the reader. This pins the
/// common GC case where bounded retention prunes older backup
/// generations after a successful publish, while some concurrent
/// diagnostic or stale reader still has the old directory mapped.
#[test]
#[serial]
#[cfg(unix)]
// Asserts staged-publish backup retention, which only runs under the
// staged-shard rebuild strategy — Tantivy-only and disabled for the Quill
// backend (see the `staged_shard_plan` binding). Ignored rather than
// weakened so it still guards the behavior if that strategy returns.
#[ignore = "staged-shard strategy is Tantivy-only; disabled on the Quill backend"]
fn publish_staged_lexical_index_prunes_retained_backup_without_breaking_open_reader() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).unwrap();
    let index_path = index_dir(&data_dir).unwrap();

    let build_generation =
        |path: &Path, label: &str, doc_count: usize, conversation_id: i64, base_ts: i64| {
            let messages = (0..doc_count)
                .map(|idx| norm_msg(idx as i64, base_ts + (idx as i64 * 100)))
                .collect::<Vec<_>>();
            let conv = norm_conv(Some(label), messages);
            let mut index = TantivyIndex::open_or_create(path).unwrap();
            index
                .add_messages_with_conversation_id(&conv, &conv.messages, Some(conversation_id))
                .unwrap();
            index.commit().unwrap();
            drop(index);
        };

    build_generation(
        &index_path,
        "retained-reader-seed",
        1,
        1_100,
        1_700_011_000_000,
    );

    let (oldest_backup_path, oldest_backup_reader) = {
        let _retention_two = set_env("CASS_LEXICAL_PUBLISH_BACKUP_RETENTION", "2");

        for (iteration, doc_count, conversation_id) in
            [(1_usize, 2_usize, 1_101_i64), (2_usize, 3_usize, 1_102_i64)]
        {
            let stage_root = TempDirBuilder::new()
                .prefix(&format!("cass-test-open-reader-retention-{iteration}."))
                .tempdir_in(data_dir.parent().unwrap())
                .unwrap();
            let staged_index_path = stage_root.path().join("staged");
            build_generation(
                &staged_index_path,
                &format!("retained-reader-stage-{iteration}"),
                doc_count,
                conversation_id,
                1_700_011_100_000 + iteration as i64 * 1_000,
            );
            publish_staged_lexical_index(&staged_index_path, &index_path).unwrap();
        }

        let backups_dir = lexical_publish_backups_dir(&index_path);
        let mut retained_with_docs: Vec<(usize, PathBuf)> = fs::read_dir(&backups_dir)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                let docs = crate::search::tantivy::searchable_index_summary(&path)
                    .unwrap()
                    .unwrap()
                    .docs;
                (docs, path)
            })
            .collect();
        retained_with_docs.sort_by_key(|(docs, _path)| *docs);
        assert_eq!(
            retained_with_docs
                .iter()
                .map(|(docs, _)| *docs)
                .collect::<Vec<_>>(),
            vec![1, 2],
            "retention=2 should retain the oldest two prior-live generations before pruning is tightened"
        );

        let oldest_backup_path = retained_with_docs[0].1.clone();
        let oldest_backup_index = TantivyIndex::open_or_create(&oldest_backup_path).unwrap();
        let oldest_backup_reader = oldest_backup_index.reader().unwrap();
        crate::search::quill_bridge::refresh_reader(&oldest_backup_reader).unwrap();
        assert_eq!(
            oldest_backup_reader.doc_count().expect("doc count"),
            1,
            "precondition: the pinned reader must observe the oldest retained backup generation"
        );
        drop(oldest_backup_index);

        (oldest_backup_path, oldest_backup_reader)
    };

    let _retention_one = set_env("CASS_LEXICAL_PUBLISH_BACKUP_RETENTION", "1");
    let stage_root = TempDirBuilder::new()
        .prefix("cass-test-open-reader-retention-prune.")
        .tempdir_in(data_dir.parent().unwrap())
        .unwrap();
    let staged_index_path = stage_root.path().join("staged");
    build_generation(
        &staged_index_path,
        "retained-reader-stage-prune",
        4,
        1_103,
        1_700_011_200_000,
    );
    publish_staged_lexical_index(&staged_index_path, &index_path).unwrap();

    assert!(
        !oldest_backup_path.exists(),
        "bounded retention should prune the oldest retained backup path once the cap drops to 1"
    );
    assert_eq!(
        oldest_backup_reader.doc_count().expect("doc count"),
        1,
        "an open reader pinned to a pruned retained backup must keep serving the prior doc surface"
    );
    assert_eq!(
        crate::search::tantivy::searchable_index_summary(&index_path)
            .unwrap()
            .unwrap()
            .docs,
        4,
        "live index must advance to the newly published generation while stale-reader GC runs"
    );

    let backups_dir = lexical_publish_backups_dir(&index_path);
    let retained_doc_counts: Vec<usize> = fs::read_dir(&backups_dir)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            crate::search::tantivy::searchable_index_summary(&path)
                .unwrap()
                .unwrap()
                .docs
        })
        .collect();
    assert_eq!(
        retained_doc_counts,
        vec![3],
        "retention=1 should leave exactly the immediately prior live generation after pruning"
    );
}

#[test]
#[serial]
// Asserts the staged-shard rebuild strategy, which builds N independent
// index directories and merges them. That merge is Tantivy-only (Quill's
// concat_merge joins segments WITHIN one manifest), so the strategy is
// disabled for the Quill backend — see the `staged_shard_plan` binding.
// Ignored rather than deleted: these are exactly the tests that must pass
// if an N-directory merge is ever built, and weakening them would hide
// that the strategy no longer runs.
#[ignore = "staged-shard strategy is Tantivy-only; disabled on the Quill backend"]
fn rebuild_tantivy_from_db_publishes_bounded_final_frontier_without_reduction() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let db_path = data_dir.join("db.sqlite");
    let storage = FrankenStorage::open(&db_path).unwrap();
    ensure_fts_schema(&storage);
    seed_lexical_rebuild_fixture(&storage);
    let agent = Agent {
        id: None,
        slug: "codex".into(),
        name: "Codex".into(),
        version: Some("0.2.3".into()),
        kind: AgentKind::Cli,
    };
    let agent_id = storage.ensure_agent(&agent).unwrap();
    for suffix in 3..=15 {
        let base_ts = 1_700_000_000_000_i64 + (i64::from(suffix) * 1_000_i64);
        let external_id = format!("lexical-fixture-{suffix}");
        storage
            .insert_conversation_tree(
                agent_id,
                None,
                &Conversation {
                    id: None,
                    agent_slug: "codex".into(),
                    workspace: Some(PathBuf::from("/tmp/workspace")),
                    external_id: Some(external_id.clone()),
                    title: Some("Lexical rebuild fixture".into()),
                    source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
                    started_at: Some(base_ts),
                    ended_at: Some(base_ts + 100),
                    approx_tokens: Some(64),
                    metadata_json: serde_json::Value::Null,
                    messages: vec![
                        Message {
                            id: None,
                            idx: 0,
                            role: MessageRole::User,
                            author: Some("user".into()),
                            created_at: Some(base_ts + 10),
                            content: format!("{external_id}-first"),
                            extra_json: serde_json::json!({"opaque": true}),
                            snippets: Vec::new(),
                        },
                        Message {
                            id: None,
                            idx: 1,
                            role: MessageRole::Agent,
                            author: Some("assistant".into()),
                            created_at: Some(base_ts + 20),
                            content: format!("{external_id}-second"),
                            extra_json: serde_json::json!({"opaque": true}),
                            snippets: Vec::new(),
                        },
                    ],
                    source_id: LOCAL_SOURCE_ID.into(),
                    origin_host: None,
                },
            )
            .unwrap();
    }

    let _workers = set_env("CASS_TANTIVY_REBUILD_WORKERS", "6");
    let _writer_threads = set_env("CASS_TANTIVY_MAX_WRITER_THREADS", "2");
    let _fetch_conversations = set_env("CASS_TANTIVY_REBUILD_BATCH_FETCH_CONVERSATIONS", "1");
    let _startup_fetch_conversations = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_BATCH_FETCH_CONVERSATIONS",
        "1",
    );
    let _commit_conversations = set_env("CASS_TANTIVY_REBUILD_COMMIT_EVERY_CONVERSATIONS", "1");
    let _startup_commit_conversations = set_env(
        "CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_CONVERSATIONS",
        "1",
    );

    let mut rebuild_equivalence: Option<crate::indexer::LexicalRebuildEquivalenceEvidence> = None;
    let logs = capture_logs(|| {
        let rebuild = rebuild_tantivy_from_db_with_options(
            &db_path,
            &data_dir,
            2,
            None,
            LexicalRebuildStartupOptions {
                defer_initial_content_fingerprint: true,
            },
            None,
        )
        .unwrap();
        assert_eq!(rebuild.indexed_docs - rebuild.indexed_region_docs, 30);
        assert_eq!(rebuild.observed_messages, Some(30));
        assert!(rebuild.exact_checkpoint_persisted);
        rebuild_equivalence = rebuild.equivalence.clone();
    });
    let evidence = rebuild_equivalence
        .expect("staged shard rebuild must emit equivalence evidence like the normal path");

    assert!(
        logs.contains("staged shard-build path"),
        "expected staged shard-build log, got:\n{logs}"
    );
    assert!(
        logs.contains(
            "publishing staged lexical rebuild as federated lexical shard set without final assembly collapse"
        ),
        "expected bounded final frontier to publish as a federated bundle, got:\n{logs}"
    );
    assert!(
        !logs.contains("draining staged lexical rebuild final merge frontier via merge workers"),
        "bounded final frontier should not pay foreground reduction cost: {logs}"
    );
    assert!(
        !logs.contains("running staged lexical rebuild merge round"),
        "bounded final frontier should avoid the fallback merge-tree tail: {logs}"
    );
    let index_path = index_dir(&data_dir).unwrap();
    let state = load_lexical_rebuild_state(&index_path)
        .unwrap()
        .expect("completed lexical rebuild state after bounded final-frontier publish");
    assert_eq!(
        state.committed_meta_fingerprint.as_deref(),
        index_meta_fingerprint(&index_path).unwrap().as_deref(),
        "completed staged rebuild state should retain the live published meta fingerprint"
    );
    let evidence_path = lexical_rebuild_equivalence_evidence_path(&index_path);
    let persisted_evidence: crate::indexer::LexicalRebuildEquivalenceEvidence =
        serde_json::from_slice(&std::fs::read(&evidence_path).unwrap()).unwrap();
    assert_eq!(
        persisted_evidence, evidence,
        "staged shard rebuild must persist the same equivalence evidence it returns"
    );
    let manifest = lexical_generation::load_manifest(&index_path)
        .unwrap()
        .expect("staged shard rebuild must persist a published generation manifest");
    assert_eq!(
        manifest.build_state,
        lexical_generation::LexicalGenerationBuildState::Validated
    );
    assert_eq!(
        manifest.publish_state,
        lexical_generation::LexicalGenerationPublishState::Published
    );
    assert_eq!(
        manifest.equivalence_manifest_fingerprint.as_deref(),
        Some(evidence.manifest_fingerprint.as_str()),
        "staged shard generation manifest must point at the persisted equivalence evidence"
    );
    assert!(
        logs.contains("lexical generation manifest published"),
        "expected staged shard path to log generation manifest publish, got:\n{logs}"
    );
    assert_eq!(tantivy_doc_count_for_data_dir(&data_dir), 30);
}
