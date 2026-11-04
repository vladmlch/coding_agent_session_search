//! Separate additive Viewer adapter. All existing CASS files remain untouched.
mod analytics;
mod annotations;
mod catalog;
mod changes;
mod deadline;
mod detail;
mod event_stream;
mod extra;
mod facts_bag;
mod identity;
mod protocol;
mod query;
mod reader;
mod revisions;
mod search;
mod search_cursor;
mod search_sessions;
mod writer;

use std::ffi::OsString;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::rc::Rc;

use anyhow::{Context, Result};
use clap::Parser;
use coding_agent_search::franken_sync::compat::RowExt;
use serde_json::{Value, json};

use protocol::{Frame, Request, refuse};
use reader::StrictArchive;
use search::ReaderSet;
use search_cursor::CursorStore;

#[derive(Parser)]
#[command(name = "cass-viewer-bridge", version)]
struct Args {
    #[arg(long, required = true)]
    stdio: bool,
    #[arg(long)]
    data_dir: PathBuf,
    #[arg(long)]
    db: PathBuf,
    /// One-shot authorized writer: install the additive `viewer_*` extension
    /// tables inside the canonical archive, then exit without serving. The
    /// normal lane stays strictly read-only and never opens a writable
    /// canonical connection.
    #[arg(long)]
    init_extensions: bool,
    /// One-shot authorized writer: replay committed canonical conversations
    /// through the ingest-time `viewer_session_facts`/structural-projection
    /// publications for archives indexed BEFORE the extension profile
    /// existed, in bounded per-batch transactions. Self-sufficient — applies
    /// ensure_schema + ensure_observers first — and idempotent: a re-run
    /// reports every already-covered conversation as skipped. Canonical
    /// tables are only read, never written.
    #[arg(long)]
    backfill_viewer_extensions: bool,
    /// Bound a `--backfill-viewer-extensions` run to N conversations
    /// (testing and operator control on large archives). A capped run is
    /// resumable by re-running the flag; no cursor state is kept.
    #[arg(long)]
    backfill_limit: Option<u64>,
    /// Explicit local authorization for the `cass_owned_queries_v1` write
    /// lane: serve the narrow annotation ops through one owned writable
    /// extension connection (`writer::OwnedWriter`). Without this flag the
    /// bridge never opens a writable handle and mutating RPCs are refused
    /// with `capability_not_granted`.
    #[arg(long)]
    enable_viewer_writes: bool,
    /// Explicit local authorization for the Task-12 committed-change push
    /// channel: bind a `127.0.0.1` event listener and publish its endpoint
    /// in `<db>.viewer_events.addr`. Without this flag the bridge opens no
    /// listener, spawns no event threads and writes no sidecar — the
    /// unmanaged archive stays untouched; `changes_since` stays available
    /// as an ordinary read op either way.
    #[arg(long)]
    enable_viewer_events: bool,
    /// Safety-net journal recheck cadence (ms) for the event pump; the
    /// IPC wakeup (`{"op":"notify"}`) is the primary low-latency path.
    #[arg(long, default_value_t = 250)]
    event_poll_ms: u64,
}

/// Run the one-shot extension-schema writer against `db`, then report.
/// Installs the `viewer_*` profile AND the canonical observer triggers so
/// even a purely read-only serving lane reports honest logical revisions.
fn init_extensions(db: &Path) -> Result<()> {
    let connection =
        coding_agent_search::franken_sync::Connection::open(db.to_string_lossy().as_ref())?;
    coding_agent_search::viewer_extensions::schema::ensure_schema(&connection)?;
    revisions::ensure_observers(&connection)?;
    // Schema v5: the sort-row mirrors are trigger-maintained, but the
    // public tie key needs the canonical-identity recomputation the
    // triggers cannot express — converge every pending row now so the
    // first browse page is never blocked on `facts_pending` ties.
    let sort = coding_agent_search::viewer_extensions::sort::repair_sort_keys(&connection)
        .context("converging browse sort tie keys")?;
    if sort.resolved + sort.refused > 0 {
        eprintln!(
            "cass-viewer-bridge: resolved {} browse sort tie key(s); {} row(s) \
             carry a persisted identity refusal (browse surfaces the typed kind)",
            sort.resolved, sort.refused,
        );
    }
    // Discoverability for the Task-14 gap: schema install cannot populate
    // facts/projection for conversations indexed before it ran — that is
    // the explicit --backfill-viewer-extensions lane.
    let pending = connection
        .query_row(
            "SELECT COUNT(*) FROM conversations c \
             WHERE NOT EXISTS (\
                 SELECT 1 FROM viewer_session_facts f \
                 WHERE f.conversation_id = c.id\
             )",
        )
        .ok()
        .and_then(|row| row.get_typed::<i64>(0).ok())
        .unwrap_or(0);
    if pending > 0 {
        eprintln!(
            "cass-viewer-bridge: {pending} canonical conversation(s) predate the \
             extension profile; run --backfill-viewer-extensions to populate \
             viewer_session_facts and the structural projection"
        );
    }
    Ok(())
}

/// Run the Task-14 backfill one-shot writer against `db`: replay committed
/// canonical conversations through the ingest-time facts/structural
/// publications in bounded transactions, then run ONE journal notify pass
/// for the whole run (a watching bridge sees a single coherent
/// publication, not N partial waves). Self-sufficient: the extension
/// profile and observers are ensured first so the flag alone repairs the
/// archive.
fn backfill_viewer_extensions(db: &Path, limit: Option<u64>) -> Result<()> {
    use coding_agent_search::viewer_extensions::backfill;
    let connection =
        coding_agent_search::franken_sync::Connection::open(db.to_string_lossy().as_ref())?;
    coding_agent_search::viewer_extensions::schema::ensure_schema(&connection)?;
    revisions::ensure_observers(&connection)?;
    let options = backfill::BackfillOptions {
        batch_size: backfill::DEFAULT_BATCH_SIZE,
        limit: limit.map(|n| usize::try_from(n).unwrap_or(usize::MAX)),
    };
    let stats = backfill::backfill_viewer_extensions(&connection, &options, |stats| {
        eprintln!(
            "cass-viewer-bridge: backfill progress: scanned={} \
             facts=+{}/skip{} structure=+{}/skip{} failures={}",
            stats.conversations_scanned,
            stats.facts_published,
            stats.facts_skipped_unchanged,
            stats.structure_published,
            stats.structure_skipped_unchanged,
            stats.failures,
        );
    })?;
    // One post-commit maintenance pass for the whole run: any observer-
    // journaled events (a concurrent canonical writer) coalesce into a
    // single index_ready instead of one marker per batch.
    coding_agent_search::viewer_extensions::changes::notify_committed_change(&connection, 0)?;
    // Wakeup-only: a listening bridge's poll fallback rechecks anyway.
    coding_agent_search::viewer_extensions::changes::send_bridge_wakeup_best_effort(db);
    eprintln!(
        "cass-viewer-bridge: backfill complete: scanned={} \
         facts=+{}/skip{} structure=+{}/skip{} failures={}{}",
        stats.conversations_scanned,
        stats.facts_published,
        stats.facts_skipped_unchanged,
        stats.structure_published,
        stats.structure_skipped_unchanged,
        stats.failures,
        if stats.limit_reached {
            " (limit reached; re-run to continue)"
        } else {
            ""
        },
    );
    if stats.failures > 0 {
        anyhow::bail!(
            "extension backfill recorded {} failed conversation(s) {:?}; \
             they remain unpublished and are retried by the next run",
            stats.failures,
            stats.failed_conversation_ids,
        );
    }
    Ok(())
}

struct Session {
    data_dir: PathBuf,
    db: PathBuf,
    worker_epoch: String,
    write_enabled: bool,
    archive: Option<StrictArchive>,
    readers: Option<Rc<ReaderSet>>,
    cursors: CursorStore,
    writer: Option<writer::OwnedWriter>,
    /// Opt-in Task-12 push channel: loopback event listener + pump threads,
    /// fully independent of this serialized request/response FIFO.
    events: Option<event_stream::EventStreamHandle>,
    event_poll: std::time::Duration,
}

impl Session {
    fn archive(&mut self) -> Result<&mut StrictArchive> {
        if self.archive.is_none() {
            self.archive = Some(StrictArchive::open(&self.db)?);
        }
        self.archive
            .as_mut()
            .ok_or_else(|| refuse("canonical_db_missing", "Archive not loaded"))
    }

    fn readers(&mut self) -> Result<Rc<ReaderSet>> {
        if let Some(readers) = &self.readers {
            return Ok(Rc::clone(readers));
        }
        let readers = Rc::new(ReaderSet::open(&self.data_dir, &self.worker_epoch)?);
        self.readers = Some(Rc::clone(&readers));
        Ok(readers)
    }

    /// The owned writable extension connection, opened lazily on the first
    /// granted write op. Kept strictly separate from `StrictArchive` —
    /// two connections, one thread, never shared state.
    fn writer(&mut self) -> Result<&mut writer::OwnedWriter> {
        if self.writer.is_none() {
            self.writer = Some(writer::OwnedWriter::open(&self.db)?);
        }
        self.writer
            .as_mut()
            .ok_or_else(|| refuse("writer_unavailable", "Extension writer lane not loaded"))
    }

    fn hello(&mut self) -> Result<Value> {
        self.archive()?;
        let (index_epoch, index_issues) = match self.readers() {
            Ok(readers) => (Some(readers.index_epoch.clone()), Vec::<String>::new()),
            Err(error) => (None, vec![error.to_string()]),
        };
        let archive = self.archive()?;
        let revision = archive.observe()?;
        let extension = archive.extension_state()?;
        let stamp = archive.stamp.clone();
        let write_privilege = self.write_enabled && extension.installed;
        Ok(json!({
            "bridge_protocol":1,"cass_crate_version":env!("CARGO_PKG_VERSION"),
            "cass_build_commit":option_env!("CASS_BUILD_COMMIT").unwrap_or("unknown"),
            "source_api_version":1,"source_contract_version":"1","db_schema_version":reader::STRICT_SCHEMA_VERSION,
            "db_path":stamp.canonical_path,"data_dir":self.data_dir,
            "archive_stamp":stamp,"catalog_revision":revision,"worker_epoch":self.worker_epoch,
            "loaded":true,"index_epoch":index_epoch,"index_issues":index_issues,
            "exact_rank":false,"rank_enumeration_supported":true,"qualification":"static_only",
            "capabilities":{
                protocol::CAPABILITY_CASS_OWNED_QUERIES_V1:{
                    "available":extension.installed,
                    "canonical_observers":extension.canonical_observers,
                    "privileges":{"annotations_write":write_privilege},
                    "change_stream":{"changes_since":true,"event_stream":self.events.is_some()},
                },
            },
            "event_stream":{
                "enabled":self.events.is_some(),
                "endpoint":self.events.as_ref().map(|handle| handle.endpoint()),
                "protocol":"cass-viewer-events/1",
                "journal":"viewer_change_log",
                // Honest capability report: nothing yet bumps
                // lexical_generation on Quill publication, so index_ready
                // events carry lexical_ready:false (never an unverified
                // lexical claim).
                "lexical_tracking":"unwired",
            },
            "generation":extension.generation,
            "features":["readonly","catalog_page","session","messages_page","rank_begin","rank_next","rank_cancel","reload","get_annotations","list_sessions","get_session_details","get_tool_output","search_sessions","projects","dashboard","health","changes_since","subscribe_events"],
            "limits":{"request_bytes":protocol::REQUEST_BYTES,"response_bytes":protocol::RESPONSE_BYTES,
                "page_limit":protocol::PAGE_LIMIT,"max_active_cursors":protocol::MAX_CURSORS,
                "default_timeout_ms":protocol::TIMEOUT_MS,"max_raw_hits":protocol::MAX_RAW_HITS,
                "cursor_idle_ttl_ms":60000,"max_readers":32},
        }))
    }

    /// Resolve the conversation an annotation op addresses: exactly one of
    /// `conversation_id` (canonical coordinate) or `public_id` (verified
    /// through `identity::resolve_conversation_id` on the strict
    /// connection). Resolution runs inside the snapshot discipline so the
    /// provenance evidence and the page share one generation.
    fn resolve_conversation_target(
        &mut self,
        conversation_id: Option<i64>,
        public_id: Option<String>,
        not_found_kind: &'static str,
    ) -> Result<i64> {
        match (conversation_id, public_id) {
            (Some(id), None) => {
                if id <= 0 {
                    return Err(refuse(
                        "invalid_request",
                        "conversation_id must be positive",
                    ));
                }
                Ok(id)
            }
            (None, Some(public_id)) => {
                let archive = self.archive()?;
                let (resolved, _) = archive.snapshot(|connection| {
                    identity::resolve_conversation_id(connection, &public_id)
                })?;
                resolved.ok_or_else(|| {
                    refuse(
                        not_found_kind,
                        "No canonical conversation verifies to this public ID",
                    )
                })
            }
            _ => Err(refuse(
                "invalid_request",
                "Address exactly one of conversation_id or public_id",
            )),
        }
    }

    /// Read-lane `cass_owned_queries_v1` op: annotation state plus the
    /// committed logical generation, under the same snapshot discipline as
    /// canonical pages.
    fn get_annotations(&mut self, conversation_id: i64) -> Result<Value> {
        if conversation_id <= 0 {
            return Err(refuse(
                "invalid_request",
                "conversation_id must be positive",
            ));
        }
        let archive = self.archive()?;
        let (mut page, revision) = archive.snapshot(|connection| {
            if !revisions::extensions_installed(connection)? {
                return Ok(
                    json!({"installed":false,"annotation":Value::Null,"generation":Value::Null}),
                );
            }
            let annotation =
                annotations::read_annotation(connection, conversation_id)?.unwrap_or(Value::Null);
            let generation = revisions::generation(connection)?;
            Ok(json!({"installed":true,"annotation":annotation,"generation":generation}))
        })?;
        page["catalog_revision"] = revision.into();
        Ok(page)
    }

    /// `cass_owned_queries_v1` read op: indexed session list. One bounded
    /// page query under the same snapshot discipline as canonical pages;
    /// `generation` in the body binds the logical revisions the page was
    /// read at, `catalog_revision` binds the physical snapshot.
    fn list_sessions(
        &mut self,
        filters: &protocol::ListFilters,
        sort: Option<&str>,
        pin_first: Option<bool>,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Value> {
        protocol::page_limit(limit)?;
        let archive = self.archive()?;
        let (mut page, revision) = archive.snapshot(|connection| {
            query::list_sessions(connection, filters, sort, pin_first, cursor, limit)
        })?;
        page["archive_stamp"] = serde_json::to_value(&archive.stamp)?;
        page["catalog_revision"] = revision.into();
        Ok(page)
    }

    /// `cass_owned_queries_v1` read op: bounded rich detail for ONE
    /// addressed conversation. Public-ID resolution runs inside the same
    /// snapshot as the page so the verified identity and the detail share
    /// one generation.
    fn get_session_details(
        &mut self,
        conversation_id: Option<i64>,
        public_id: Option<String>,
        sections: &[String],
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Value> {
        protocol::page_limit(limit)?;
        let archive = self.archive()?;
        let (mut page, revision) = archive.snapshot(|connection| {
            let id = detail::resolve_target(connection, conversation_id, public_id.as_deref())?;
            detail::session_details(connection, id, sections, cursor, limit)
        })?;
        page["archive_stamp"] = serde_json::to_value(&archive.stamp)?;
        page["catalog_revision"] = revision.into();
        Ok(page)
    }

    /// `cass_owned_queries_v1` read op: bounded replay of the committed
    /// change journal under the same snapshot discipline as canonical
    /// pages — `{events, next_seq, truncated, current_revisions}` plus the
    /// physical `catalog_revision` binding. The refetch fallback the push
    /// channel overflows to; an ordinary read op needing no opt-in flag.
    fn changes_since(&mut self, after_seq: Option<i64>, limit: Option<usize>) -> Result<Value> {
        let after = after_seq.unwrap_or(0);
        if after < 0 {
            return Err(refuse("invalid_request", "after_seq must be nonnegative"));
        }
        let limit = limit.unwrap_or(protocol::PAGE_LIMIT);
        if limit == 0 || limit > changes::CHANGES_PAGE_LIMIT {
            return Err(refuse(
                "invalid_request",
                "changes_since limit must be in 1..=1024",
            ));
        }
        let archive = self.archive()?;
        let (mut page, revision) = archive.snapshot(|connection| {
            Ok(serde_json::to_value(changes::changes_since(
                connection, after, limit,
            )?)?)
        })?;
        page["catalog_revision"] = revision.into();
        page["archive_stamp"] = serde_json::to_value(&archive.stamp)?;
        Ok(page)
    }

    /// `cass_owned_queries_v1` read op: one recorded tool output bound to
    /// a verified canonical-message anchor and the logical
    /// `canonical_revision` the anchor was observed at.
    fn get_tool_output(
        &mut self,
        conversation_id: Option<i64>,
        public_id: Option<String>,
        anchor: &protocol::StepAnchor,
        expected_revision: i64,
        byte_offset: i64,
    ) -> Result<Value> {
        let archive = self.archive()?;
        let (mut page, revision) = archive.snapshot(|connection| {
            let id = detail::resolve_target(connection, conversation_id, public_id.as_deref())?;
            detail::tool_output(connection, id, anchor, expected_revision, byte_offset)
        })?;
        page["archive_stamp"] = serde_json::to_value(&archive.stamp)?;
        page["catalog_revision"] = revision.into();
        Ok(page)
    }

    fn handle(&mut self, request: Request) -> Result<(Value, bool)> {
        let result = match request {
            Request::Hello { .. } | Request::Status { .. } => self.hello()?,
            Request::CatalogPage {
                after_id, limit, ..
            } => self.archive()?.read_catalog_page(after_id, limit)?,
            Request::Session {
                conversation_id, ..
            } => self.archive()?.session(conversation_id)?,
            Request::MessagesPage {
                conversation_id,
                after_idx,
                limit,
                ..
            } => self
                .archive()?
                .read_messages_page(conversation_id, after_idx, limit)?,
            Request::RankBegin {
                query,
                filters,
                budget,
                ..
            } => {
                filters.validate()?;
                budget.validate()?;
                if query.trim().is_empty() || query.len() > 4096 {
                    return Err(refuse(
                        "invalid_request",
                        "Lexical query must contain 1..4096 UTF-8 bytes",
                    ));
                }
                self.archive()?;
                let readers = self.readers()?;
                let archive = self
                    .archive
                    .as_mut()
                    .ok_or_else(|| refuse("canonical_db_missing", "Archive not loaded"))?;
                self.cursors
                    .begin(readers, archive, query, filters, budget)?
            }
            Request::RankNext {
                cursor, max_hits, ..
            } => {
                let archive = self.archive.as_mut().ok_or_else(|| {
                    refuse("stale_snapshot", "Raw cursor has no retained archive")
                })?;
                self.cursors.next(archive, &cursor, max_hits)?
            }
            Request::RankCancel { cursor, .. } => json!({"cancelled":self.cursors.cancel(&cursor)}),
            Request::GetAnnotations {
                conversation_id,
                public_id,
                ..
            } => {
                let conversation_id = self.resolve_conversation_target(
                    conversation_id,
                    public_id,
                    "session_not_found",
                )?;
                self.get_annotations(conversation_id)?
            }
            Request::PatchAnnotations {
                conversation_id,
                public_id,
                expected_revision,
                patch,
                ..
            } => {
                if !self.write_enabled {
                    return Err(refuse(
                        "capability_not_granted",
                        "Mutating RPC requires the explicit --enable-viewer-writes \
                         local authorization",
                    ));
                }
                let conversation_id = self.resolve_conversation_target(
                    conversation_id,
                    public_id,
                    "unknown_conversation",
                )?;
                let result =
                    self.writer()?
                        .patch_annotations(conversation_id, expected_revision, &patch)?;
                // Task 5: fold the just-committed meta replacement into the
                // published index now. Best-effort — a contended or absent
                // index leaves `meta_pending`, and the marks row degrades
                // the session to honest `unknown` rather than stale hits.
                let index_dir =
                    coding_agent_search::search::tantivy::expected_index_dir(&self.data_dir);
                if let Err(error) = self
                    .writer()?
                    .converge_search_region_docs(&index_dir)
                {
                    tracing::warn!(
                        error = %format!("{error:#}"),
                        "search-region doc convergence after annotation edit deferred"
                    );
                } else {
                    // The write lane just changed the published index —
                    // the cached reader set is pinned to the pre-edit
                    // snapshot and would keep surfacing the REPLACED meta
                    // documents (a stale hit hydrates the new fragment and
                    // degrades the session to a reasonless `unknown`).
                    // Drop it so the next search reopens on the converged
                    // index; generation-bound cursors expire honestly
                    // through the moved `index_epoch`, never mix pages.
                    self.readers = None;
                }
                // Task 12: the commit already journaled annotations_updated
                // via the extension triggers; the wakeup merely shortens a
                // listening pump's (possibly another bridge's) poll wait.
                coding_agent_search::viewer_extensions::changes::send_bridge_wakeup_best_effort(
                    &self.db,
                );
                result
            }
            Request::ListSessions {
                filters,
                sort,
                pin_first,
                cursor,
                limit,
                ..
            } => self.list_sessions(
                &filters,
                sort.as_deref(),
                pin_first,
                cursor.as_deref(),
                limit,
            )?,
            Request::GetSessionDetails {
                conversation_id,
                public_id,
                sections,
                cursor,
                limit,
                ..
            } => self.get_session_details(
                conversation_id,
                public_id,
                &sections,
                cursor.as_deref(),
                limit,
            )?,
            Request::GetToolOutput {
                conversation_id,
                public_id,
                step_anchor,
                expected_revision,
                byte_offset,
                ..
            } => self.get_tool_output(
                conversation_id,
                public_id,
                &step_anchor,
                expected_revision,
                byte_offset.unwrap_or(0),
            )?,
            Request::SearchSessions {
                ast,
                query,
                filters,
                scopes,
                sort,
                mode,
                allow_partial,
                cursor,
                limit,
                max_matches_per_session,
                ..
            } => {
                protocol::page_limit(limit)?;
                let max_matches = max_matches_per_session.unwrap_or(32).min(64);
                self.archive()?;
                let readers = self.readers()?;
                let archive = self
                    .archive
                    .as_mut()
                    .ok_or_else(|| refuse("canonical_db_missing", "Archive not loaded"))?;
                let (mut page, revision) = archive.snapshot(|connection| {
                    search_sessions::search_sessions(
                        connection,
                        &readers,
                        ast.as_ref(),
                        query.as_deref(),
                        &filters,
                        scopes.as_deref(),
                        sort.as_deref(),
                        mode.as_deref(),
                        allow_partial.unwrap_or(false),
                        cursor.as_deref(),
                        limit,
                        max_matches,
                    )
                })?;
                page["archive_stamp"] = serde_json::to_value(&archive.stamp)?;
                page["catalog_revision"] = revision.into();
                page
            }
            Request::Projects { scope, .. } => {
                let archive = self.archive()?;
                let (mut page, revision) =
                    archive.snapshot(|connection| analytics::projects(connection, &scope))?;
                page["catalog_revision"] = revision.into();
                page
            }
            Request::Dashboard { scope, .. } => {
                let archive = self.archive()?;
                let (mut page, revision) =
                    archive.snapshot(|connection| analytics::dashboard(connection, &scope))?;
                page["catalog_revision"] = revision.into();
                page
            }
            Request::Health { scope, .. } => {
                let archive = self.archive()?;
                let (mut page, revision) =
                    archive.snapshot(|connection| analytics::health(connection, &scope))?;
                page["catalog_revision"] = revision.into();
                page
            }
            Request::ChangesSince {
                after_seq, limit, ..
            } => self.changes_since(after_seq, limit)?,
            Request::Reload { .. } => {
                self.cursors.clear();
                self.readers = None;
                self.archive = None;
                // Re-verify the extension profile and observers on the next
                // write instead of trusting a stale writable handle.
                self.writer = None;
                // A reload may mean the archive inode was replaced; restart
                // the event channel so its pump connection re-binds to the
                // current file instead of replaying a pinned, stale journal.
                // Drop BEFORE re-starting: the old handle's teardown removes
                // its sidecar, so the new listener must publish after it.
                if self.events.is_some() {
                    self.events = None;
                    self.events =
                        match event_stream::EventStreamHandle::start(&self.db, self.event_poll) {
                            Ok(handle) => Some(handle),
                            Err(error) => {
                                eprintln!(
                                    "cass-viewer-bridge: event stream restart failed: {error:#}"
                                );
                                None
                            }
                        };
                }
                self.hello()?
            }
            Request::Shutdown { .. } => {
                self.cursors.clear();
                return Ok((json!({"shutdown":true}), true));
            }
        };
        Ok((result, false))
    }
}

fn serve(session: &mut Session, input: &mut impl BufRead, output: &mut impl Write) -> Result<()> {
    let watchdog = deadline::Watchdog::start()?;
    loop {
        let line = match protocol::read_frame(input)? {
            Frame::End => return Ok(()),
            Frame::Unterminated => {
                let error = refuse(
                    "invalid_request",
                    "EOF before the JSONL newline; the worker is closing",
                );
                protocol::write_frame(
                    output,
                    &protocol::encode_line(&protocol::failure(0, &error))?,
                )?;
                return Err(error);
            }
            Frame::TooLarge => {
                let error = refuse(
                    "request_too_large",
                    "Request exceeds 64 KiB; the worker is closing",
                );
                protocol::write_frame(
                    output,
                    &protocol::encode_line(&protocol::failure(0, &error))?,
                )?;
                return Err(error);
            }
            Frame::Line(line) => line,
        };
        let request = match serde_json::from_slice::<Request>(&line) {
            Ok(request) => request,
            Err(_) => {
                let id = serde_json::from_slice::<Value>(&line)
                    .ok()
                    .and_then(|value| value.get("id").and_then(Value::as_u64))
                    .unwrap_or(0);
                let error = refuse(
                    "invalid_request",
                    "Invalid UTF-8/JSON, unknown operation/field, or malformed typed request",
                );
                protocol::write_frame(
                    output,
                    &protocol::encode_line(&protocol::failure(id, &error))?,
                )?;
                continue;
            }
        };
        let id = request.id();
        let timeout = match &request {
            Request::RankBegin { budget, .. } => budget.deadline_ms.min(protocol::TIMEOUT_MS),
            // A continuation shares the initial scan deadline, including an
            // engine call that cannot reach the cooperative cursor checks.
            Request::RankNext { cursor, .. } => session.cursors.remaining_timeout_ms(cursor),
            _ => protocol::TIMEOUT_MS,
        };
        watchdog.begin(timeout);
        let (response, stop) = match session.handle(request) {
            Ok((result, stop)) => (protocol::success(id, result), stop),
            Err(error) => {
                if error
                    .downcast_ref::<protocol::Refusal>()
                    .is_some_and(|e| e.kind == "stale_snapshot")
                {
                    session.cursors.clear();
                }
                if error
                    .downcast_ref::<protocol::Refusal>()
                    .is_some_and(|e| e.kind == "canonical_read_failed")
                {
                    session.cursors.clear();
                    session.archive = None;
                    // The writable handle may point at a replaced archive
                    // inode; re-open it against the current file next write.
                    session.writer = None;
                }
                (protocol::failure(id, &error), false)
            }
        };
        let bytes = protocol::encode_line(&response).or_else(|_| {
            session.cursors.clear();
            protocol::encode_line(&protocol::failure(
                id,
                &refuse(
                    "response_too_large",
                    "Response exceeds 1 MiB; request fewer rows",
                ),
            ))
        })?;
        protocol::write_frame(output, &bytes)?;
        watchdog.done();
        if stop {
            return Ok(());
        }
    }
}

pub fn run(args: Vec<OsString>) -> ExitCode {
    let args = match Args::try_parse_from(args) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    if !args.stdio {
        eprintln!("cass-viewer-bridge: --stdio is required");
        return ExitCode::from(2);
    }
    if args.init_extensions || args.backfill_viewer_extensions {
        let result = (|| -> Result<()> {
            if args.init_extensions {
                init_extensions(&args.db).context("--init-extensions")?;
            }
            if args.backfill_viewer_extensions {
                backfill_viewer_extensions(&args.db, args.backfill_limit)
                    .context("--backfill-viewer-extensions")?;
            }
            Ok(())
        })();
        return match result {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("cass-viewer-bridge: extension maintenance failed: {error:#}");
                ExitCode::FAILURE
            }
        };
    }
    if args.backfill_limit.is_some() {
        eprintln!(
            "cass-viewer-bridge: --backfill-limit requires --backfill-viewer-extensions"
        );
        return ExitCode::from(2);
    }
    let data_dir = match std::fs::canonicalize(&args.data_dir) {
        Ok(path) if path.is_dir() => path,
        _ => {
            eprintln!("cass-viewer-bridge: data directory must already exist");
            return ExitCode::from(2);
        }
    };
    let event_poll = std::time::Duration::from_millis(args.event_poll_ms);
    let events = if args.enable_viewer_events {
        match event_stream::EventStreamHandle::start(&args.db, event_poll) {
            Ok(handle) => Some(handle),
            Err(error) => {
                eprintln!("cass-viewer-bridge: event stream failed to start: {error:#}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        None
    };
    let mut session = Session {
        data_dir,
        db: args.db,
        worker_epoch: format!("{:032x}", rand::random::<u128>()),
        write_enabled: args.enable_viewer_writes,
        archive: None,
        readers: None,
        cursors: CursorStore::default(),
        writer: None,
        events,
        event_poll,
    };
    let result = serve(
        &mut session,
        &mut io::stdin().lock(),
        &mut io::stdout().lock(),
    );
    drop(session);
    if !coding_agent_search::shutdown_thread_local_bridge_runtimes() {
        eprintln!("cass-viewer-bridge: engine runtime cleanup did not complete");
        return ExitCode::FAILURE;
    }
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("cass-viewer-bridge: {error}");
            ExitCode::FAILURE
        }
    }
}
