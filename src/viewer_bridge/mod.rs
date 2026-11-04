//! Separate additive Viewer adapter. All existing CASS files remain untouched.
mod catalog;
mod deadline;
mod extra;
mod protocol;
mod reader;
mod search;
mod search_cursor;

use std::ffi::OsString;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::rc::Rc;

use anyhow::Result;
use clap::Parser;
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
}

struct Session {
    data_dir: PathBuf,
    db: PathBuf,
    worker_epoch: String,
    archive: Option<StrictArchive>,
    readers: Option<Rc<ReaderSet>>,
    cursors: CursorStore,
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

    fn hello(&mut self) -> Result<Value> {
        self.archive()?;
        let (index_epoch, index_issues) = match self.readers() {
            Ok(readers) => (Some(readers.index_epoch.clone()), Vec::<String>::new()),
            Err(error) => (None, vec![error.to_string()]),
        };
        let archive = self.archive()?;
        let revision = archive.observe()?;
        let stamp = archive.stamp.clone();
        Ok(json!({
            "bridge_protocol":1,"cass_crate_version":env!("CARGO_PKG_VERSION"),
            "cass_build_commit":option_env!("CASS_BUILD_COMMIT").unwrap_or("unknown"),
            "source_api_version":1,"source_contract_version":"1","db_schema_version":22,
            "db_path":stamp.canonical_path,"data_dir":self.data_dir,
            "archive_stamp":stamp,"catalog_revision":revision,"worker_epoch":self.worker_epoch,
            "loaded":true,"index_epoch":index_epoch,"index_issues":index_issues,
            "exact_rank":false,"rank_enumeration_supported":true,"qualification":"static_only",
            "features":["readonly","catalog_page","session","messages_page","rank_begin","rank_next","rank_cancel","reload"],
            "limits":{"request_bytes":protocol::REQUEST_BYTES,"response_bytes":protocol::RESPONSE_BYTES,
                "page_limit":protocol::PAGE_LIMIT,"max_active_cursors":protocol::MAX_CURSORS,
                "default_timeout_ms":protocol::TIMEOUT_MS,"max_raw_hits":protocol::MAX_RAW_HITS,
                "cursor_idle_ttl_ms":60000,"max_readers":32},
        }))
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
            Request::Reload { .. } => {
                self.cursors.clear();
                self.readers = None;
                self.archive = None;
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
    let data_dir = match std::fs::canonicalize(&args.data_dir) {
        Ok(path) if path.is_dir() => path,
        _ => {
            eprintln!("cass-viewer-bridge: data directory must already exist");
            return ExitCode::from(2);
        }
    };
    let mut session = Session {
        data_dir,
        db: args.db,
        worker_epoch: format!("{:032x}", rand::random::<u128>()),
        archive: None,
        readers: None,
        cursors: CursorStore::default(),
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
