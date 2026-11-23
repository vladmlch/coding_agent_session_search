#[allow(unused_imports)]
pub(crate) use crate::franken_sync::compat::{
    ConnectionExt, OpenFlags as FrankenOpenFlags, RowExt,
    open_with_flags as open_franken_with_flags,
};
#[allow(unused_imports)]
pub(crate) use crate::indexer::IndexOptions;
#[allow(unused_imports)]
pub(crate) use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
#[allow(unused_imports)]
pub(crate) use crate::robot_budget_envelope::BudgetSnapshot;
#[allow(unused_imports)]
pub(crate) use crate::{cli_app::*, cli_root::*, lib_doctor::*, *};
#[allow(unused_imports)]
pub(crate) use anyhow::Result;
#[allow(unused_imports)]
pub(crate) use base64::prelude::*;
#[allow(unused_imports)]
pub(crate) use chrono::Utc;
#[allow(unused_imports)]
pub(crate) use clap::{
    Arg, ArgAction, Command, CommandFactory, Parser, Subcommand, ValueEnum, ValueHint,
};
#[allow(unused_imports)]
pub(crate) use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
pub(crate) use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
#[allow(unused_imports)]
pub(crate) use std::fs::{File, OpenOptions};
#[allow(unused_imports)]
pub(crate) use std::io::{self, BufRead, BufReader, IsTerminal, Read, Seek, SeekFrom, Write};
#[allow(unused_imports)]
pub(crate) use std::path::{Path, PathBuf};
#[allow(unused_imports)]
pub(crate) use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
pub(crate) use std::time::{Duration, Instant};
#[allow(unused_imports)]
pub(crate) use tracing::{info, warn};
#[allow(unused_imports)]
pub(crate) use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

mod admin_cmds_tests;
mod cli_read_db;
mod doctor_fts;
mod doctor_probes_and_gates;
mod export_misc_tests;
mod export_score_and_grouping;
mod indexed_conversation_fallback;
mod response_schema;
mod root_cause_signal_gather;
mod stall_diagnostics;
mod subcommand_robot_and_probes;

#[cfg(test)]
pub(crate) fn normalized_source_id_sql_expr(column: &str) -> String {
    format!(
        "CASE WHEN TRIM(COALESCE({column}, '')) = '' THEN '{local}'          WHEN LOWER(TRIM(COALESCE({column}, ''))) = '{local}' THEN '{local}'          ELSE TRIM(COALESCE({column}, '')) END",
        local = crate::sources::provenance::LOCAL_SOURCE_ID,
    )
}

#[cfg(test)]
pub(crate) fn try_load_indexed_conversation_from_db(
    source_path: &Path,
    db_path: &Path,
) -> Option<crate::conversation_view::ConversationView> {
    try_load_indexed_conversation_from_db_with_source(source_path, db_path, None, None)
}

/// Run `f` on a dedicated thread with a 16 MiB stack. Used for clap-parse
/// tests whose monomorphized parser depth can exceed the 2 MiB default test
/// stack in debug builds. Lifting this out of any one test module so both
/// `subcommand_robot_output_tests` and `pages_cli_flag_tests` can share it.
///
/// If `f` panics, the panic payload is resumed on the calling thread so the
/// real assertion message and backtrace surface to the test runner instead of
/// being replaced by a generic "child thread panicked" wrapper.
#[cfg(test)]
pub(crate) fn run_on_large_stack<F>(f: F)
where
    F: FnOnce() + Send + 'static,
{
    let handle = std::thread::Builder::new()
        .name("cass-clap-parse-test".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(f)
        .expect("spawn large-stack test thread");
    match handle.join() {
        Ok(()) => {}
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
