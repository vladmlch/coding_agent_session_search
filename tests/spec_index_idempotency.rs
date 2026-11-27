//! INV-cass-20 — `cass index --json` idempotency contract.
//!
//! Indexing is the data-ingestion engine for cass. A regression where
//! every `cass index --json` invocation does a full rebuild — instead
//! of detecting that the corpus is unchanged and taking the incremental
//! fast path — would silently burn CI budget, multiply local-dev wait
//! time, and (in production) thrash the lexical-publish atomic-swap
//! pipeline on every wake.
//!
//! The planner emits two `indexing_stats.lexical_strategy` values that
//! distinguish the two paths verbatim:
//!
//! - first index against a fresh data-dir:
//!   `"deferred_authoritative_db_rebuild"`
//!   reason: `"empty_archive_defers_lexical_rebuild_to_authoritative_db_rebuild"`
//!   (at3us: an empty archive is ingested first and its index built once
//!   from the database, as `--full` does; rebuilding inline from the scan
//!   re-verified the whole index at every batch commit)
//! - second + later index against the same corpus:
//!   `"incremental_inline"`
//!   reason: `"incremental_scan_applies_inline_lexical_updates_only_for_new_messages"`
//!
//! This file pins four invariants:
//!
//! 1. Two consecutive `cass index --json` invocations against the same
//!    stable corpus produce stable `conversations` and `messages`
//!    total counts. No double-counting, no under-counting.
//! 2. The first run's `lexical_strategy` is
//!    `"deferred_authoritative_db_rebuild"` for the empty-archive reason,
//!    and its lexical index already finds the fixture's text.
//! 3. The second run's `lexical_strategy` is `"incremental_inline"`
//!    (the no-op fast-path). A regression where every run does a
//!    full rebuild would emit a rebuild strategy twice and fail this
//!    assertion immediately.
//! 4. A populated archive whose lexical index is lost never takes the
//!    empty-archive path (it is repaired from the database before its
//!    scan), and its rebuilt index finds the fixture's text.
//!
//! Verified by staging a single isolated `claude_code` session under a
//! temporary HOME (`CLAUDE_CONFIG_DIR`) and indexing against a fresh
//! data-dir twice in a row.

use std::cmp::Ordering;
use std::error::Error;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use assert_cmd::Command;
use coding_agent_search::franken_sync::compat::{
    ConnectionExt, OpenFlags, RowExt, open_with_flags,
};
use serde_json::{Value, json};
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn Error>>;

fn test_error(message: impl Into<String>) -> Box<dyn Error> {
    std::io::Error::other(message.into()).into()
}

fn ensure(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(test_error(message))
    }
}

/// Install a single-session `claude_code` fixture under an isolated HOME so
/// `cass index --json` discovers exactly one conversation containing the
/// searchable token `"refactor"`. Returns `(tmp, project_dir, data_dir)`.
fn install_claude_fixture_project() -> Result<(TempDir, PathBuf, PathBuf), Box<dyn Error>> {
    let tmp = TempDir::new()?;
    let project_dir = tmp.path().join("claude_project");
    let session_dir = tmp
        .path()
        .join(".claude")
        .join("projects")
        .join("idempotency-fixture");
    let data_dir = tmp.path().join("cass_data");
    fs::create_dir_all(&project_dir)?;
    fs::create_dir_all(&session_dir)?;
    fs::create_dir_all(&data_dir)?;
    let dst = session_dir.join("session-idempotency.jsonl");
    let mut file = fs::File::create(&dst)?;
    for line in [
        json!({
            "type": "user",
            "timestamp": 1_705_334_400_000i64,
            "message": {
                "role": "user",
                "content": "Please refactor the retry handler so repeated runs stay idempotent."
            }
        }),
        json!({
            "type": "assistant",
            "timestamp": 1_705_334_460_000i64,
            "message": {
                "role": "assistant",
                "content": "I completed the refactor and verified the incremental index path."
            }
        }),
    ] {
        writeln!(file, "{}", serde_json::to_string(&line)?)?;
    }
    Ok((tmp, project_dir, data_dir))
}

fn isolate_cass_env(cmd: &mut Command, home: &Path) {
    cmd.env("HOME", home)
        .env("CLAUDE_CONFIG_DIR", home.join(".claude"))
        .env("CODEX_HOME", home.join("missing-codex"))
        .env("GEMINI_HOME", home.join("missing-gemini"))
        .env("CASS_QWEN_HOME", home.join("missing-qwen"))
        .env("CASS_ANTIGRAVITY_DATA_ROOT", home.join("missing-agy"))
        .env("CASS_DEVIN_DATA_ROOT", home.join("missing-devin"))
        .env("OPENCODE_STORAGE_ROOT", home.join("missing-opencode"))
        .env("OPENCODE_SQLITE_DB", home.join("missing-opencode.db"))
        .env("XDG_CONFIG_HOME", home.join("config"))
        .env("XDG_DATA_HOME", home.join("data"))
        .env("XDG_CACHE_HOME", home.join("cache"))
        .env("CASS_IGNORE_SOURCES_CONFIG", "1")
        .env("CASS_AUTO_REFRESH", "0")
        .env("NO_COLOR", "1")
        .env_remove("CASS_DB_PATH")
        .env_remove("CASS_DATA_DIR");
}

/// Run `cass index --json --no-progress-events --data-dir <data_dir>`
/// against the isolated `home`. Returns the parsed JSON envelope and asserts exit 0.
fn run_index_in(project_dir: &Path, data_dir: &Path, home: &Path) -> Result<Value, Box<dyn Error>> {
    let mut cmd = Command::cargo_bin("cass")?;
    isolate_cass_env(&mut cmd, home);
    let output = cmd
        .current_dir(project_dir)
        .args(["--color=never", "index", "--json", "--no-progress-events"])
        .args(["--data-dir", data_dir.to_str().ok_or("non-utf8 path")?])
        .output()?;
    let code = output
        .status
        .code()
        .ok_or_else(|| test_error("cass index killed by signal"))?;
    if !matches!(code.cmp(&0), Ordering::Equal) {
        return Err(test_error(format!(
            "cass index --json exited {code}; stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    let parsed: Value = serde_json::from_slice(&output.stdout)?;
    Ok(parsed)
}

fn lexical_strategy(envelope: &Value) -> Result<&str, Box<dyn Error>> {
    envelope
        .pointer("/indexing_stats/lexical_strategy")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            test_error(format!(
                "envelope missing indexing_stats.lexical_strategy: {envelope}"
            ))
        })
}

fn conversation_count(envelope: &Value) -> Result<i64, Box<dyn Error>> {
    envelope
        .get("conversations")
        .and_then(Value::as_i64)
        .ok_or_else(|| {
            test_error(format!(
                "envelope missing integer `conversations`: {envelope}"
            ))
        })
}

fn archive_totals(data_dir: &Path) -> Result<(usize, usize), Box<dyn Error>> {
    let db_path = data_dir.join("agent_search.db");
    let conn = open_with_flags(&db_path.to_string_lossy(), OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let conversations: i64 =
        conn.query_row_map("SELECT COUNT(*) FROM conversations", &[], |row| {
            row.get_typed(0)
        })?;
    let messages: i64 =
        conn.query_row_map("SELECT COUNT(*) FROM messages", &[], |row| row.get_typed(0))?;
    conn.close_without_checkpoint()?;
    Ok((usize::try_from(conversations)?, usize::try_from(messages)?))
}

fn lexical_strategy_reason(envelope: &Value) -> Result<&str, Box<dyn Error>> {
    envelope
        .pointer("/indexing_stats/lexical_strategy_reason")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            test_error(format!(
                "envelope missing indexing_stats.lexical_strategy_reason: {envelope}"
            ))
        })
}

/// Lexical hits for `query` in `data_dir`, from a fresh `cass search`.
fn lexical_hit_count(data_dir: &Path, home: &Path, query: &str) -> Result<usize, Box<dyn Error>> {
    let mut cmd = Command::cargo_bin("cass")?;
    isolate_cass_env(&mut cmd, home);
    let output = cmd
        .args(["--color=never", "search", query, "--json"])
        .args(["--mode", "lexical"])
        .args(["--data-dir", data_dir.to_str().ok_or("non-utf8 path")?])
        .output()?;
    if !output.status.success() {
        return Err(test_error(format!(
            "cass search exited {:?}; stderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    let parsed: Value = serde_json::from_slice(&output.stdout)?;
    parsed
        .get("hits")
        .and_then(Value::as_array)
        .map(Vec::len)
        .ok_or_else(|| test_error(format!("search envelope missing `hits`: {parsed}")))
}

#[test]
fn first_index_against_a_fresh_data_dir_rebuilds_lexical_from_the_database() -> TestResult {
    let (tmp, project, data) = install_claude_fixture_project()?;
    let envelope = run_index_in(&project, &data, tmp.path())?;
    let strategy = lexical_strategy(&envelope)?;
    let reason = lexical_strategy_reason(&envelope)?;
    ensure(
        strategy == "deferred_authoritative_db_rebuild"
            && reason == "empty_archive_defers_lexical_rebuild_to_authoritative_db_rebuild",
        format!(
            "first index against a fresh data-dir should ingest first and rebuild the \
             lexical index once from the database; got {strategy:?} / {reason:?}.\n\
             envelope.indexing_stats: {}",
            envelope.pointer("/indexing_stats").unwrap_or(&Value::Null)
        ),
    )?;
    // Also sanity: the fixture should produce at least one conversation,
    // otherwise the rest of the test setup is silently broken.
    let n_conv = conversation_count(&envelope)?;
    ensure(
        !matches!(n_conv.cmp(&0), Ordering::Less | Ordering::Equal),
        format!("first index should find >=1 conversation; got {n_conv}"),
    )?;
    // The deferred strategy skips inline lexical writes during the scan, so
    // only the post-scan rebuild makes the text searchable. A run that took
    // the strategy without that rebuild leaves the index empty.
    let hits = lexical_hit_count(&data, tmp.path(), "refactor")?;
    ensure(
        hits > 0,
        "the first index's lexical index must already find the fixture's text",
    )?;
    Ok(())
}

#[test]
fn populated_archive_lexical_rebuild_never_takes_the_empty_archive_path() -> TestResult {
    let (tmp, project, data) = install_claude_fixture_project()?;
    run_index_in(&project, &data, tmp.path())?;
    // Lose the lexical index of the now-populated archive.
    fs::remove_dir_all(data.join("index"))?;
    let envelope = run_index_in(&project, &data, tmp.path())?;
    let reason = lexical_strategy_reason(&envelope)?;
    ensure(
        reason != "empty_archive_defers_lexical_rebuild_to_authoritative_db_rebuild",
        format!(
            "a populated archive must keep its existing repair; got {} / {reason:?}",
            lexical_strategy(&envelope)?
        ),
    )?;
    let hits = lexical_hit_count(&data, tmp.path(), "refactor")?;
    ensure(hits > 0, "the rebuilt index must find the fixture's text")?;
    Ok(())
}

#[test]
fn second_index_uses_incremental_inline_strategy_proving_idempotency() -> TestResult {
    let (tmp, project, data) = install_claude_fixture_project()?;
    // First run primes the corpus.
    let _envelope1 = run_index_in(&project, &data, tmp.path())?;
    // Second run is the assertion target.
    let envelope2 = run_index_in(&project, &data, tmp.path())?;
    let strategy = lexical_strategy(&envelope2)?;
    ensure(
        strategy == "incremental_inline",
        format!(
            "second index against the same corpus should use the no-op \
             `incremental_inline` strategy (proving idempotency); \
             got {strategy:?}.\n\
             A regression here means every cass index invocation is doing a \
             full rebuild — burning CI budget and thrashing the atomic-swap pipeline.\n\
             envelope.indexing_stats: {}",
            envelope2.pointer("/indexing_stats").unwrap_or(&Value::Null)
        ),
    )?;
    Ok(())
}

#[test]
fn consecutive_indexes_produce_stable_conversation_and_message_totals() -> TestResult {
    let (tmp, project, data) = install_claude_fixture_project()?;
    run_index_in(&project, &data, tmp.path())?;
    // Index envelopes may report only the work observed during that run
    // (#192). Idempotency concerns the archive, including unchanged rows.
    let (conv1, msg1) = archive_totals(&data)?;
    ensure(conv1 > 0 && msg1 > 0, "fixture must populate the archive")?;
    run_index_in(&project, &data, tmp.path())?;
    let (conv2, msg2) = archive_totals(&data)?;

    ensure(
        matches!(conv1.cmp(&conv2), Ordering::Equal),
        format!(
            "conversation counts drifted across consecutive idempotent runs: \
             first={conv1}, second={conv2}. Either the planner is double-counting \
             or the source is being re-discovered as new on every run."
        ),
    )?;
    ensure(
        matches!(msg1.cmp(&msg2), Ordering::Equal),
        format!(
            "message counts drifted across consecutive idempotent runs: \
             first={msg1}, second={msg2}."
        ),
    )?;
    Ok(())
}
