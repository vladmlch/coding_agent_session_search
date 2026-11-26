use super::super::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::indexer::LexicalPopulationStrategy;
use crate::search::tantivy::TantivyIndex;
use crate::storage::sqlite::FrankenStorage;

static ENV_LOCK: std::sync::LazyLock<std::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(()));
std::thread_local! {
    static ENV_LOCK_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(super) struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
    _lock: Option<std::sync::MutexGuard<'static, ()>>,
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(value) = &self.previous {
            // SAFETY: test helper restores process env key it changed.
            unsafe {
                std::env::set_var(self.key, value);
            }
        } else {
            // SAFETY: test helper restores process env key it changed.
            unsafe {
                std::env::remove_var(self.key);
            }
        }
        ENV_LOCK_DEPTH.with(|depth| {
            let current = depth.get();
            debug_assert!(current > 0, "env lock depth underflow");
            depth.set(current.saturating_sub(1));
        });
    }
}

fn acquire_env_lock() -> Option<std::sync::MutexGuard<'static, ()>> {
    let mut guard = None;
    ENV_LOCK_DEPTH.with(|depth| {
        let current = depth.get();
        if current == 0 {
            // Recover from poisoning: the lock only serializes env
            // mutation and `EnvGuard::drop` restores the variables even
            // while unwinding, so a panic in one test must not cascade
            // PoisonError failures through every later test.
            guard = Some(
                ENV_LOCK
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
        }
        depth.set(current + 1);
    });
    guard
}

pub(super) fn set_env(key: &'static str, value: &str) -> EnvGuard {
    let _lock = acquire_env_lock();
    let previous = dotenvy::var(key).ok();
    // SAFETY: isolated test mutates a process env var and restores via guard.
    unsafe {
        std::env::set_var(key, value);
    }
    EnvGuard {
        key,
        previous,
        _lock,
    }
}

/// Helper: create a SQLite database with schema applied.
pub(super) fn create_franken_db(path: &std::path::Path) -> FrankenStorage {
    let fs = FrankenStorage::open(path).expect("open sqlite db");
    fs.run_migrations().expect("run migrations");
    fs
}

/// Two-message session whose second answer is `answer`; everything else
/// is fixed, so persisting the same value again is an unchanged re-ingest.
pub(super) fn region_reingest_session(
    dir: &std::path::Path,
    answer: &str,
) -> NormalizedConversation {
    NormalizedConversation {
        agent_slug: "claude_code".into(),
        external_id: Some("region-reingest".into()),
        title: Some("Region re-ingest".into()),
        workspace: Some(dir.join("workspace")),
        source_path: dir.join("region-reingest.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_050),
        metadata: serde_json::json!({"model": "fixture-model"}),
        messages: vec![
            NormalizedMessage {
                idx: 0,
                role: "user".into(),
                author: None,
                created_at: Some(1_700_000_000_000),
                content: "first question".into(),
                extra: serde_json::json!({"uuid": "u-0"}),
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
            NormalizedMessage {
                idx: 1,
                role: "assistant".into(),
                author: None,
                created_at: Some(1_700_000_000_050),
                content: answer.into(),
                extra: serde_json::json!({"uuid": "a-1"}),
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
        ],
    }
}

/// One pass of the incremental lexical lane over `conv`: persist, then the
/// writer's post-commit stamp pass that marks region documents published.
pub(super) fn incremental_lane_pass(
    storage: &FrankenStorage,
    index: &mut TantivyIndex,
    conv: &NormalizedConversation,
) {
    persist_conversations_batched(
        storage,
        Some(&mut *index),
        std::slice::from_ref(conv),
        LexicalPopulationStrategy::IncrementalInline,
        false,
    )
    .unwrap();
    index.commit_with_region_doc_stamps(storage.raw()).unwrap();
}

/// Canonical message documents live in the committed index.
/// Task 5 persists search-region documents alongside them in the
/// `msg_idx >= REGION_DOC_BASE` space; every assertion in this
/// module counts the canonical share only, matching the meaning
/// the call sites had before region documents existed.
pub(super) fn tantivy_doc_count(index: &mut crate::search::tantivy::TantivyIndex) -> u64 {
    index.commit().expect("commit tantivy");
    let reader = index.reader().expect("reader");
    crate::search::quill_bridge::refresh_reader(&reader).expect("reload");
    let query = crate::search::quill_bridge::canonical_message_query(
        frankensearch::quill::query::Query::all(),
    );
    crate::search::quill_bridge::search_paginated(&reader, &query, 1, 0, true)
        .expect("canonical doc count")
        .total_count
        .unwrap_or(0) as u64
}
