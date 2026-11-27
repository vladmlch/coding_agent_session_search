#[allow(unused_imports)]
use crate::franken_sync::compat::{
    ConnectionExt, OpenFlags as FrankenOpenFlags, RowExt,
    open_with_flags as open_franken_with_flags,
};
#[allow(unused_imports)]
use crate::indexer::IndexOptions;
#[allow(unused_imports)]
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
#[allow(unused_imports)]
use crate::robot_budget_envelope::BudgetSnapshot;
#[allow(unused_imports)]
use crate::{cli_app::*, cli_root::*, lib_doctor::*, *};
#[allow(unused_imports)]
use anyhow::Result;
#[allow(unused_imports)]
use base64::prelude::*;
#[allow(unused_imports)]
use chrono::Utc;
#[allow(unused_imports)]
use clap::{Arg, ArgAction, Command, CommandFactory, Parser, Subcommand, ValueEnum, ValueHint};
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
#[allow(unused_imports)]
use std::fs::{File, OpenOptions};
#[allow(unused_imports)]
use std::io::{self, BufRead, BufReader, IsTerminal, Read, Seek, SeekFrom, Write};
#[allow(unused_imports)]
use std::path::{Path, PathBuf};
#[allow(unused_imports)]
use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
use std::time::{Duration, Instant};
#[allow(unused_imports)]
use tracing::{info, warn};
#[allow(unused_imports)]
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub(crate) async fn import_chatgpt_export(
    export_path: &Path,
    output_dir: Option<&Path>,
    output_format: Option<RobotFormat>,
) -> CliResult<()> {
    use std::io::Write;

    // Validate export file exists
    if !export_path.exists() {
        return Err(CliError {
            code: 1,
            kind: CliErrorKind::Io.kind_str(),
            message: format!("Export file not found: {}", export_path.display()),
            hint: Some(
                "Provide the path to conversations.json from ChatGPT web export \
                 (Settings \u{2192} Data Controls \u{2192} Export)"
                    .into(),
            ),
            retryable: false,
        });
    }

    // Determine output directory
    let base_dir = if let Some(dir) = output_dir {
        dir.to_path_buf()
    } else {
        // Try macOS ChatGPT app support dir first
        #[cfg(target_os = "macos")]
        {
            dirs::home_dir()
                .map(|h| h.join("Library/Application Support/com.openai.chat"))
                .unwrap_or_else(|| {
                    dirs::data_local_dir()
                        .unwrap_or_else(|| PathBuf::from("."))
                        .join("cass/chatgpt")
                })
        }
        #[cfg(not(target_os = "macos"))]
        {
            dirs::data_local_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join("cass/chatgpt")
        }
    };

    let conv_dir = base_dir.join("conversations-web-export");
    std::fs::create_dir_all(&conv_dir).map_err(|e| CliError {
        code: 1,
        kind: CliErrorKind::Io.kind_str(),
        message: format!("Failed to create output directory: {e}"),
        hint: None,
        retryable: false,
    })?;

    // Read and parse export file
    let content = std::fs::read_to_string(export_path).map_err(|e| CliError {
        code: 1,
        kind: CliErrorKind::Io.kind_str(),
        message: format!("Failed to read export file: {e}"),
        hint: None,
        retryable: false,
    })?;

    let conversations: Vec<serde_json::Value> =
        serde_json::from_str(&content).map_err(|e| CliError {
            code: 1,
            kind: CliErrorKind::ParseError.kind_str(),
            message: format!("Failed to parse conversations.json: {e}"),
            hint: Some("Expected a JSON array of conversation objects".into()),
            retryable: false,
        })?;

    let total = conversations.len();
    let mut imported = 0u64;
    let mut skipped = 0u64;

    for conv in &conversations {
        let filepath = conv_dir.join(format!("{}.json", chatgpt_import_file_stem(conv)));

        // Idempotent: skip if already exists
        if filepath.exists() {
            skipped += 1;
            continue;
        }

        // Write individual conversation file
        let mut file = std::fs::File::create(&filepath).map_err(|e| CliError {
            code: 1,
            kind: CliErrorKind::Io.kind_str(),
            message: format!("Failed to write {}: {e}", filepath.display()),
            hint: None,
            retryable: false,
        })?;
        serde_json::to_writer(&mut file, conv).map_err(|e| CliError {
            code: 1,
            kind: CliErrorKind::Io.kind_str(),
            message: format!("Failed to serialize conversation: {e}"),
            hint: None,
            retryable: false,
        })?;
        file.flush().map_err(|e| CliError {
            code: 1,
            kind: CliErrorKind::Io.kind_str(),
            message: format!("Failed to flush: {e}"),
            hint: None,
            retryable: false,
        })?;

        imported += 1;
    }

    // GH #378: the indexer only scans a connector's *default* roots when
    // franken_agent_detection reports the agent as installed, and the ChatGPT
    // connector's default root is macOS-only (`com.openai.chat`). On Linux and
    // Windows — and on macOS with a custom `--output-dir` — the files written
    // above therefore sat in a directory nothing ever scanned, so `cass index`
    // "succeeded" and silently indexed none of them. Explicit scan roots from
    // `sources.toml` are scanned regardless of detection, so register the
    // resolved output directory as a local source. Idempotent: re-imports
    // reuse the same source and only append a path that is not already there.
    let registered_scan_root = if output_dir.is_some() || !cfg!(target_os = "macos") {
        match register_import_scan_root(CHATGPT_IMPORT_SOURCE_NAME, &conv_dir) {
            Ok(state) => Some(state),
            Err(err) => {
                eprintln!(
                    "Warning: imported conversations were written to {} but could not be \
                     registered as a scan root in sources.toml ({err}). Add it by hand \
                     (see `cass robot-docs sources`):\n\
                     [[sources]]\n  name = \"{CHATGPT_IMPORT_SOURCE_NAME}\"\n  type = \"local\"\n  \
                     paths = [{:?}]\n\
                     Until then `cass index` will not see the imported conversations.",
                    conv_dir.display(),
                    conv_dir.display().to_string()
                );
                None
            }
        }
    } else {
        None
    };

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(_fmt) = structured_format {
        let result = serde_json::json!({
            "success": true,
            "total": total,
            "imported": imported,
            "skipped": skipped,
            "output_dir": conv_dir.display().to_string(),
            "scan_root": registered_scan_root.as_ref().map(|state| serde_json::json!({
                "source": CHATGPT_IMPORT_SOURCE_NAME,
                "path": conv_dir.display().to_string(),
                "registration": state.as_str(),
            })),
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&result).unwrap_or_default()
        );
    } else {
        println!("Import complete!");
        println!("  Total conversations: {total}");
        println!("  Newly imported:      {imported}");
        println!("  Skipped (existing):  {skipped}");
        println!("  Output directory:    {}", conv_dir.display());
        if let Some(state) = registered_scan_root {
            println!(
                "  Scan root:           {} (sources.toml source '{CHATGPT_IMPORT_SOURCE_NAME}', {})",
                conv_dir.display(),
                state.as_str()
            );
        }
        println!();
        println!("Next step: Run `cass index` to index the conversations.");
    }

    Ok(())
}

/// File stem for one imported ChatGPT conversation.
///
/// The export's `id`/`conversation_id` is user-controlled text that lands in a
/// filename, so it is accepted only when it is a plain token (`[A-Za-z0-9._-]`,
/// not `.`/`..`) — anything else (path separators, whitespace, a missing id)
/// falls back to a BLAKE3 digest of the conversation itself. The digest, not
/// the array position, is the fallback because import is idempotent *by
/// filename*: a positional `conv-{i}` name would make a re-export with a
/// different ordering silently skip different conversations.
pub(crate) fn chatgpt_import_file_stem(conv: &serde_json::Value) -> String {
    let plain_id = conv
        .get("id")
        .or_else(|| conv.get("conversation_id"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|id| {
            !id.is_empty()
                && *id != "."
                && *id != ".."
                && id.len() <= 128
                && id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        });
    match plain_id {
        Some(id) => id.to_string(),
        None => {
            let mut hasher = blake3::Hasher::new();
            hasher.update(conv.to_string().as_bytes());
            format!("conv-{}", &hasher.finalize().to_hex()[..32])
        }
    }
}

#[cfg(test)]
mod chatgpt_import_file_stem_tests {
    use super::chatgpt_import_file_stem;

    #[test]
    fn plain_ids_are_used_verbatim() {
        let conv = serde_json::json!({ "id": "7f3a-conv_01.v2", "title": "x" });
        assert_eq!(chatgpt_import_file_stem(&conv), "7f3a-conv_01.v2");
        let alt = serde_json::json!({ "conversation_id": "abc123" });
        assert_eq!(chatgpt_import_file_stem(&alt), "abc123");
    }

    #[test]
    fn unsafe_or_missing_ids_fall_back_to_a_content_digest() {
        for bad in ["../escape", "a/b", "", "  ", ".", "..", "with space"] {
            let conv = serde_json::json!({ "id": bad, "title": "t" });
            let stem = chatgpt_import_file_stem(&conv);
            assert!(stem.starts_with("conv-"), "{bad:?} -> {stem}");
            assert!(!stem.contains('/') && !stem.contains(".."), "{stem}");
        }
        let missing = serde_json::json!({ "title": "no id" });
        assert!(chatgpt_import_file_stem(&missing).starts_with("conv-"));
    }

    #[test]
    fn digest_fallback_is_content_keyed_not_positional() {
        let a = serde_json::json!({ "title": "first" });
        let b = serde_json::json!({ "title": "second" });
        assert_ne!(chatgpt_import_file_stem(&a), chatgpt_import_file_stem(&b));
        assert_eq!(chatgpt_import_file_stem(&a), chatgpt_import_file_stem(&a));
    }
}

/// `sources.toml` source name under which `cass import chatgpt` registers its
/// output directory so the indexer scans it without relying on connector
/// auto-detection (GH #378).
pub(crate) const CHATGPT_IMPORT_SOURCE_NAME: &str = "chatgpt-import";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImportScanRootRegistration {
    /// The source did not exist and was created with this path.
    Created,
    /// The source existed; this path was appended to it.
    PathAdded,
    /// The source already listed this path; nothing written.
    AlreadyRegistered,
}

impl ImportScanRootRegistration {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::PathAdded => "path_added",
            Self::AlreadyRegistered => "already_registered",
        }
    }
}

/// Register `path` as a local explicit scan root under the named source in
/// `sources.toml` (GH #378). Loads the current config, mutates only the named
/// source, and saves atomically through `SourcesConfig::save`.
pub(crate) fn register_import_scan_root(
    source_name: &str,
    path: &Path,
) -> Result<ImportScanRootRegistration, crate::sources::config::ConfigError> {
    let mut config = crate::sources::config::SourcesConfig::load()?;
    let state = register_import_scan_root_in(&mut config, source_name, path)?;
    if state != ImportScanRootRegistration::AlreadyRegistered {
        config.save()?;
    }
    Ok(state)
}

/// Pure half of [`register_import_scan_root`]: mutates `config` in memory and
/// reports what changed, so callers decide whether a save is needed.
pub(crate) fn register_import_scan_root_in(
    config: &mut crate::sources::config::SourcesConfig,
    source_name: &str,
    path: &Path,
) -> Result<ImportScanRootRegistration, crate::sources::config::ConfigError> {
    use crate::sources::config::SourceDefinition;

    let path_str = path.to_string_lossy().into_owned();
    if let Some(source) = config.find_source_mut(source_name) {
        if source.paths.iter().any(|existing| existing == &path_str) {
            return Ok(ImportScanRootRegistration::AlreadyRegistered);
        }
        // ubs:ignore[rust.security.path-traversal] — Vec<String>::push, not PathBuf::push
        source.paths.push(path_str);
        source.validate()?;
        Ok(ImportScanRootRegistration::PathAdded)
    } else {
        let mut source = SourceDefinition::local(source_name);
        // ubs:ignore[rust.security.path-traversal] — Vec<String>::push, not PathBuf::push
        source.paths.push(path_str);
        config.add_source(source)?;
        Ok(ImportScanRootRegistration::Created)
    }
}

#[cfg(test)]
mod chatgpt_import_scan_root_tests {
    use super::{
        CHATGPT_IMPORT_SOURCE_NAME, ImportScanRootRegistration, register_import_scan_root_in,
    };
    use crate::sources::config::SourcesConfig;
    use crate::sources::provenance::SourceKind;

    /// GH #378: the import output directory must become an explicit local
    /// scan root, and re-running the import must not duplicate it.
    #[test]
    fn import_registers_its_output_dir_once_as_a_local_source() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let conv_dir = tmp.path().join("chatgpt").join("conversations-web-export");
        std::fs::create_dir_all(&conv_dir).expect("fixture dir");
        let mut config = SourcesConfig::default();

        assert_eq!(
            register_import_scan_root_in(&mut config, CHATGPT_IMPORT_SOURCE_NAME, &conv_dir)
                .expect("first registration"),
            ImportScanRootRegistration::Created
        );
        let source = config
            .find_source(CHATGPT_IMPORT_SOURCE_NAME)
            .expect("source created");
        assert_eq!(source.source_type, SourceKind::Local);
        assert!(!source.is_remote());
        assert_eq!(source.paths, vec![conv_dir.to_string_lossy().into_owned()]);

        assert_eq!(
            register_import_scan_root_in(&mut config, CHATGPT_IMPORT_SOURCE_NAME, &conv_dir)
                .expect("re-registration"),
            ImportScanRootRegistration::AlreadyRegistered
        );
        assert_eq!(
            config
                .find_source(CHATGPT_IMPORT_SOURCE_NAME)
                .expect("source")
                .paths
                .len(),
            1,
            "a re-import must not duplicate the path"
        );

        // A second import with a different --output-dir joins the same source.
        let other_dir = tmp
            .path()
            .join("elsewhere")
            .join("conversations-web-export");
        std::fs::create_dir_all(&other_dir).expect("fixture dir");
        assert_eq!(
            register_import_scan_root_in(&mut config, CHATGPT_IMPORT_SOURCE_NAME, &other_dir)
                .expect("second path"),
            ImportScanRootRegistration::PathAdded
        );
        assert_eq!(
            config
                .find_source(CHATGPT_IMPORT_SOURCE_NAME)
                .expect("source")
                .paths
                .len(),
            2
        );
    }
}
