use chrono::Utc;
use std::collections::{BTreeMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Default tracing directive for interactive human mode: emit INFO and above
/// but suppress frankensqlite internal telemetry that spams at INFO level.
///
/// `EnvFilter` uses `::` as the hierarchy separator, so `fsqlite=warn` covers
/// `fsqlite::runtime`, `fsqlite::cx`, etc. Crate-level targets like
/// `fsqlite_vdbe` and `fsqlite_core` need their own directives. Custom
/// dot-separated targets (`fsqlite.statement_reuse`, `fsqlite.compat`, etc.) are
/// NOT matched by the hierarchical prefix, so each must be listed explicitly.
///
/// This list is only consulted in interactive human mode (and only when
/// `RUST_LOG` is unset). Robot/quiet mode never reaches it — those modes pin the
/// filter to `error` via [`robot_aware_log_directive`], which is the actual
/// stdout/stderr hygiene guarantee. A missing entry here can therefore only make
/// human-mode stderr noisier, never corrupt a robot JSON stream.
pub(crate) const DEFAULT_DEP_LOG_SUPPRESSION: &str = concat!(
    "info",
    // Hierarchical (::) targets
    ",fsqlite=warn",
    ",fsqlite_core=warn",
    ",fsqlite_vdbe=warn",
    ",fsqlite_mvcc=warn",
    ",fsqlite_pager=warn",
    ",fsqlite_func=warn",
    ",fsqlite_vfs=warn",
    ",fsqlite_wal=warn",
    ",fsqlite_c_api=warn",
    ",fsqlite_planner=warn",
    ",fsqlite_types=warn",
    ",fsqlite_observability=warn",
    // Dot-separated custom targets
    ",fsqlite.compat=warn",
    ",fsqlite.compat_trace=warn",
    ",fsqlite.statement_reuse=warn",
    ",fsqlite.statement=warn",
    ",fsqlite.execution=warn",
    ",fsqlite.execute_path=warn",
    ",fsqlite.plan=warn",
    ",fsqlite.planner=warn",
    ",fsqlite.planner_runtime=warn",
    ",fsqlite.parse=warn",
    ",fsqlite.provenance=warn",
    ",fsqlite.dp=warn",
    ",fsqlite.udf=warn",
    ",fsqlite.vdbe=warn",
    ",fsqlite.rcu=warn",
    ",fsqlite.seqlock=warn",
    ",fsqlite.left_right=warn",
    ",fsqlite.flat_combine=warn",
    ",fsqlite.commit_combine=warn",
    ",fsqlite.snapshot_publication=warn",
    ",fsqlite.wal_publication=warn",
    ",fsqlite.storage_wiring=warn",
    ",fsqlite.cx_propagation=warn",
    ",fsqlite.sketch_telemetry=warn",
    ",fsqlite.time_travel=warn",
    ",fsqlite.trace_export=warn",
    ",fsqlite.txn_slot=warn",
    ",fsqlite.evidence=warn",
    ",fsqlite.lab_schedule=warn",
);

/// Decide the tracing filter directive for the active output mode.
///
/// Returns `Some(directive)` when the directive is fixed regardless of the
/// `RUST_LOG` environment variable, and `None` when the caller should consult
/// `RUST_LOG` (falling back to [`DEFAULT_DEP_LOG_SUPPRESSION`]).
///
/// Robot and quiet modes are machine-consumed: they pin the filter to `error`
/// (unless the operator explicitly opts into `--verbose`) so that dependency
/// INFO/WARN logging — frankensqlite telemetry in particular — can never
/// interleave with a JSON stream, even when `RUST_LOG` is set in the
/// environment. This is the single source of truth for robot stdout/stderr
/// hygiene; the regression suite in `tests/cli_robot_log_hygiene.rs` exercises it
/// end-to-end under deliberately noisy `RUST_LOG` settings.
pub(crate) fn robot_aware_log_directive(
    robot_mode: bool,
    verbose: bool,
    quiet: bool,
) -> Option<&'static str> {
    if quiet || robot_mode {
        // Robot/quiet mode implies error-only output unless --verbose is set.
        Some(if verbose { "debug" } else { "error" })
    } else if verbose {
        Some("debug")
    } else {
        None
    }
}

/// Build the concrete [`EnvFilter`] for the active output mode, applying the
/// robot/quiet hygiene policy from [`robot_aware_log_directive`].
pub(crate) fn build_robot_aware_log_filter(
    robot_mode: bool,
    verbose: bool,
    quiet: bool,
) -> EnvFilter {
    match robot_aware_log_directive(robot_mode, verbose, quiet) {
        Some(directive) => EnvFilter::new(directive),
        None => EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(DEFAULT_DEP_LOG_SUPPRESSION)),
    }
}

/// Default filter for the explicit `--trace-file` surface.
///
/// CASS-owned targets retain DEBUG detail. Dependencies retain every WARN/ERROR
/// event, but their DEBUG/INFO chatter is excluded by default. The redaction
/// memo target is also pinned to WARN because trace serialization itself invokes
/// redaction; retaining its routine hit/miss DEBUG events would amplify the
/// artifact with diagnostics about producing diagnostics. Memo invalidation and
/// quarantine warnings remain visible.
pub(crate) const DEFAULT_TRACE_FILE_LOG_DIRECTIVE: &str =
    "warn,coding_agent_search=debug,cass=debug,cass::redact::memo=warn";

/// Default hard ceiling for one trace artifact. Operators may lower or raise it
/// with `CASS_TRACE_MAX_BYTES`; values are clamped to a safe 4 KiB..1 GiB range.
pub(crate) const TRACE_SCHEMA_VERSION: &str = "cass-trace-v1";

pub(crate) const DEFAULT_TRACE_FILE_MAX_BYTES: u64 = 16 * 1024 * 1024;

pub(crate) const MIN_TRACE_FILE_MAX_BYTES: u64 = 4 * 1024;

pub(crate) const MAX_TRACE_FILE_MAX_BYTES: u64 = 1024 * 1024 * 1024;

pub(crate) const DEFAULT_TRACE_FILE_MAX_EVENTS: u64 = 50_000;

pub(crate) const MIN_TRACE_FILE_MAX_EVENTS: u64 = 16;

pub(crate) const MAX_TRACE_FILE_MAX_EVENTS: u64 = 10_000_000;

pub(crate) const TRACE_SUMMARY_RESERVE_MAX_BYTES: u64 = 64 * 1024;

pub(crate) const TRACE_TRUNCATION_RESERVE_MAX_BYTES: u64 = 8 * 1024;

pub(crate) const TRACE_FAILURE_TAIL_RESERVE_MAX_BYTES: u64 = 32 * 1024;

pub(crate) const TRACE_EVENT_BUFFER_MAX_BYTES: usize = 256 * 1024;

pub(crate) const TRACE_FILTER_TARGET_LIMIT: usize = 8;

pub(crate) const TRACE_CORRELATION_MAX_CHARS: usize = 256;

pub(crate) const TRACE_SUMMARY_ARG_LIMIT: usize = 16;

pub(crate) const TRACE_SUMMARY_ARG_MAX_CHARS: usize = 256;

pub(crate) const TRACE_SUMMARY_ERROR_MAX_CHARS: usize = 1024;

pub(crate) const TRACE_RECEIPT_TARGET_MAX_CHARS: usize = 128;

pub(crate) fn trace_file_log_filter_override() -> Option<EnvFilter> {
    let directive = dotenvy::var("CASS_TRACE_FILTER").ok()?;
    let directive = directive.trim();
    if directive.is_empty() || directive.eq(DEFAULT_TRACE_FILE_LOG_DIRECTIVE) {
        return None;
    }
    match EnvFilter::try_new(directive) {
        Ok(filter) => Some(filter),
        Err(error) => {
            eprintln!("invalid CASS_TRACE_FILTER; using the bounded default policy: {error}");
            None
        }
    }
}

pub(crate) fn trace_file_max_bytes() -> u64 {
    dotenvy::var("CASS_TRACE_MAX_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(|value| value.clamp(MIN_TRACE_FILE_MAX_BYTES, MAX_TRACE_FILE_MAX_BYTES))
        .unwrap_or(DEFAULT_TRACE_FILE_MAX_BYTES)
}

pub(crate) fn trace_file_max_events() -> u64 {
    dotenvy::var("CASS_TRACE_MAX_EVENTS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(|value| value.clamp(MIN_TRACE_FILE_MAX_EVENTS, MAX_TRACE_FILE_MAX_EVENTS))
        .unwrap_or(DEFAULT_TRACE_FILE_MAX_EVENTS)
}

pub(crate) fn trace_target_is_cass_owned(target: &str) -> bool {
    target.eq("cass")
        || target.starts_with("cass::")
        || target.eq("coding_agent_search")
        || target.starts_with("coding_agent_search::")
}

pub(crate) fn default_trace_metadata_enabled(metadata: &tracing::Metadata<'_>) -> bool {
    if metadata.target().eq("cass::redact::memo") {
        return matches!(
            *metadata.level(),
            tracing::Level::ERROR | tracing::Level::WARN
        );
    }
    matches!(
        *metadata.level(),
        tracing::Level::ERROR | tracing::Level::WARN
    ) || (trace_target_is_cass_owned(metadata.target())
        && matches!(
            *metadata.level(),
            tracing::Level::INFO | tracing::Level::DEBUG
        ))
}

pub(crate) fn trace_field_is_sensitive(field: &str) -> bool {
    crate::indexer::redact_secrets::is_sensitive_json_field(field)
}

pub(crate) static TRACE_REDACTION_ENGINE: once_cell::sync::Lazy<
    crate::pages::redact::RedactionEngine,
> = once_cell::sync::Lazy::new(|| {
    crate::pages::redact::RedactionEngine::new(
        crate::pages::redact::swarm_evidence_redaction_config(),
    )
});

pub(crate) fn redact_trace_text(text: &str) -> String {
    let secret_redacted = crate::indexer::redact_secrets::redact_text(text).into_owned();
    TRACE_REDACTION_ENGINE.redact_text(&secret_redacted).output
}

pub(crate) fn truncate_trace_text(text: &str, max_chars: usize) -> String {
    let mut chars = text.chars();
    let mut truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        truncated.push_str("...[TRUNCATED]");
    }
    truncated
}

pub(crate) fn redact_bounded_trace_text(text: &str, max_chars: usize) -> String {
    truncate_trace_text(&redact_trace_text(text), max_chars)
}

pub(crate) fn enrich_trace_json(value: &serde_json::Value) -> serde_json::Value {
    let mut enriched = value.clone();
    if let Some(root) = enriched.as_object_mut() {
        root.entry("schema_version")
            .or_insert_with(|| serde_json::Value::String(TRACE_SCHEMA_VERSION.to_string()));
        root.entry("trace_id").or_insert_with(|| {
            dotenvy::var("CASS_TRACE_ID")
                .map(|value| {
                    serde_json::Value::String(redact_bounded_trace_text(
                        &value,
                        TRACE_CORRELATION_MAX_CHARS,
                    ))
                })
                .unwrap_or(serde_json::Value::Null)
        });
        root.entry("test_id").or_insert_with(|| {
            dotenvy::var("CASS_TRACE_TEST_ID")
                .map(|value| {
                    serde_json::Value::String(redact_bounded_trace_text(
                        &value,
                        TRACE_CORRELATION_MAX_CHARS,
                    ))
                })
                .unwrap_or(serde_json::Value::Null)
        });
        if let Some(fields) = root
            .get_mut("fields")
            .and_then(serde_json::Value::as_object_mut)
        {
            let has_event_identity = ["event", "message"].into_iter().any(|field| {
                fields
                    .get(field)
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|value| !value.is_empty())
            });
            if !has_event_identity {
                fields.insert(
                    "event".to_string(),
                    serde_json::Value::String("diagnostic".to_string()),
                );
            }
        }
    }
    enriched
}

pub(crate) fn redact_trace_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(text) => serde_json::Value::String(redact_trace_text(text)),
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(redact_trace_json).collect())
        }
        serde_json::Value::Object(fields) => {
            let mut redacted = serde_json::Map::with_capacity(fields.len());
            let mut next_suffixes = std::collections::HashMap::new();
            for (field, value) in fields {
                let sanitized_trace_value = if trace_field_is_sensitive(field) {
                    serde_json::Value::String("[REDACTED]".to_string())
                } else {
                    redact_trace_json(value)
                };
                let field = redact_trace_text(field);
                crate::indexer::redact_secrets::insert_redacted_json_entry(
                    &mut redacted,
                    &mut next_suffixes,
                    field,
                    sanitized_trace_value,
                );
            }
            serde_json::Value::Object(redacted)
        }
        other => other.clone(),
    }
}

pub(crate) fn redact_trace_args(args: &[String]) -> Vec<String> {
    let retained = args.len().min(TRACE_SUMMARY_ARG_LIMIT);
    let mut redacted = Vec::with_capacity(retained.saturating_add(1));
    let mut redact_next = false;

    for arg in args.iter().take(TRACE_SUMMARY_ARG_LIMIT) {
        if redact_next {
            redacted.push("[REDACTED]".to_string());
            redact_next = false;
            continue;
        }

        if let Some((flag, _value)) = arg.split_once('=')
            && flag.starts_with('-')
            && trace_field_is_sensitive(flag.trim_start_matches('-'))
        {
            redacted.push(truncate_trace_text(
                &format!("{flag}=[REDACTED]"),
                TRACE_SUMMARY_ARG_MAX_CHARS,
            ));
            continue;
        }

        if arg.starts_with('-') && trace_field_is_sensitive(arg.trim_start_matches('-')) {
            redacted.push(truncate_trace_text(arg, TRACE_SUMMARY_ARG_MAX_CHARS));
            redact_next = true;
            continue;
        }

        redacted.push(truncate_trace_text(
            &redact_trace_text(arg),
            TRACE_SUMMARY_ARG_MAX_CHARS,
        ));
    }

    if args.len() > retained {
        redacted.push(format!(
            "[{} ARGUMENTS OMITTED]",
            args.len().saturating_sub(retained)
        ));
    }

    redacted
}

pub(crate) fn trace_json_has_valid_envelope(value: &serde_json::Value) -> bool {
    value.is_object()
        && value
            .get("schema_version")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|schema| schema.eq(TRACE_SCHEMA_VERSION))
        && value["timestamp"].as_str().is_some_and(|timestamp| {
            !timestamp.is_empty() && chrono::DateTime::parse_from_rfc3339(timestamp).is_ok()
        })
        && value["level"]
            .as_str()
            .is_some_and(|level| !level.is_empty())
        && value["target"]
            .as_str()
            .is_some_and(|target| !target.is_empty())
        && value.get("trace_id").is_some()
        && value.get("test_id").is_some()
        && value["fields"].as_object().is_some_and(|fields| {
            ["event", "message"].into_iter().any(|field| {
                fields
                    .get(field)
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|identity| !identity.is_empty())
            })
        })
}

pub(crate) fn encode_trace_line(value: &serde_json::Value) -> io::Result<Vec<u8>> {
    let enriched = enrich_trace_json(value);
    let redacted = redact_trace_json(&enriched);
    let mut encoded = serde_json::to_vec(&redacted).map_err(io::Error::other)?;
    encoded.push(b'\n');
    Ok(encoded)
}

pub(crate) fn trace_summary_reserve(max_bytes: u64) -> u64 {
    TRACE_SUMMARY_RESERVE_MAX_BYTES.min(max_bytes / 4)
}

pub(crate) fn trace_truncation_reserve(max_bytes: u64) -> u64 {
    TRACE_TRUNCATION_RESERVE_MAX_BYTES.min(max_bytes / 4)
}

pub(crate) fn trace_failure_tail_reserve(max_bytes: u64) -> u64 {
    TRACE_FAILURE_TAIL_RESERVE_MAX_BYTES.min(max_bytes / 8)
}

pub(crate) fn open_trace_append_file(
    path: &Path,
    max_bytes: u64,
) -> io::Result<(File, u64, u64, bool)> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;
    fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("trace artifact already has an active writer: {error}"),
        )
    })?;
    let mut bytes_written = file.metadata()?.len();
    let mut events_written = 0_u64;
    let mut diagnostic_head_closed = false;

    if bytes_written > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "existing trace is {bytes_written} bytes, exceeding the configured {max_bytes}-byte ceiling"
            ),
        ));
    }

    if bytes_written > 0 {
        file.seek(SeekFrom::Start(0))?;
        {
            let mut reader = BufReader::new(&mut file);
            let mut line = Vec::new();
            let mut line_number = 0_u64;
            loop {
                line.clear();
                if reader.read_until(b'\n', &mut line)?.eq(&0) {
                    break;
                }
                line_number = line_number.saturating_add(1);
                let record = line.strip_suffix(b"\n").unwrap_or(&line);
                let record = record.strip_suffix(b"\r").unwrap_or(record);
                if record.is_empty() {
                    continue;
                }
                let value =
                    serde_json::from_slice::<serde_json::Value>(record).map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("existing trace line {line_number} is not valid JSON: {error}"),
                        )
                    })?;
                if !trace_json_has_valid_envelope(&value) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "existing trace line {line_number} is not a valid {TRACE_SCHEMA_VERSION} envelope"
                        ),
                    ));
                }
                let event = value["fields"]["event"].as_str();
                if matches!(event, Some("trace_truncated")) {
                    diagnostic_head_closed = true;
                }
                if !matches!(
                    event,
                    Some("command_summary" | "trace_filter_summary" | "trace_truncated")
                ) {
                    events_written = events_written.saturating_add(1);
                }
            }
        }
    }

    if bytes_written > 0 {
        file.seek(SeekFrom::End(-1))?;
        let mut last = [0_u8; 1];
        file.read_exact(&mut last)?;
        if !last.first().is_some_and(|byte| byte.eq(&b'\n')) {
            if bytes_written >= max_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "existing trace is a valid JSON record but lacks its final newline and already consumes the configured {max_bytes}-byte ceiling"
                    ),
                ));
            }
            file.write_all(b"\n")?;
            bytes_written += 1;
        }
    }

    Ok((file, bytes_written, events_written, diagnostic_head_closed))
}

pub(crate) struct BoundedTraceState {
    pub(crate) file: File,
    pub(crate) max_bytes: u64,
    pub(crate) max_events: u64,
    pub(crate) bytes_written: u64,
    pub(crate) events_written: u64,
    pub(crate) diagnostic_head_closed: bool,
    pub(crate) suppressed_events: u64,
    pub(crate) suppressed_bytes: u64,
    pub(crate) byte_budget_suppressed_events: u64,
    pub(crate) event_budget_suppressed_events: u64,
    pub(crate) oversize_suppressed_events: u64,
    pub(crate) filtered_events: u64,
    pub(crate) filtered_by_target: BTreeMap<String, u64>,
    pub(crate) suppressed_by_target: BTreeMap<String, u64>,
    pub(crate) failure_tail: VecDeque<(serde_json::Value, u64)>,
    pub(crate) failure_tail_bytes: u64,
    pub(crate) failure_tail_dropped_events: u64,
    pub(crate) finalized: bool,
    pub(crate) last_error: Option<String>,
}

#[derive(Clone, Copy)]
pub(crate) enum TraceSuppressionReason {
    ByteBudget,
    EventBudget,
    OversizeEvent,
}

pub(crate) fn increment_trace_target_count(counts: &mut BTreeMap<String, u64>, target: &str) {
    if let Some(count) = counts.get_mut(target) {
        *count = count.saturating_add(1);
    } else {
        counts.insert(target.to_string(), 1);
    }
}

pub(crate) fn trace_receipt_target_limit(max_bytes: u64) -> usize {
    if max_bytes < 16 * 1024 {
        2
    } else {
        TRACE_FILTER_TARGET_LIMIT
    }
}

pub(crate) fn ranked_trace_targets(
    counts: &BTreeMap<String, u64>,
    limit: usize,
) -> (Vec<serde_json::Value>, u64) {
    let mut ranked = counts
        .iter()
        .map(|(target, count)| (target.as_str(), *count))
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(right.0)));
    let overflow_events = ranked
        .iter()
        .skip(limit)
        .fold(0_u64, |total, (_, count)| total.saturating_add(*count));
    ranked.truncate(limit);
    let targets = ranked
        .into_iter()
        .map(|(target, count)| {
            serde_json::json!({
                "target": truncate_trace_text(target, TRACE_RECEIPT_TARGET_MAX_CHARS),
                "count": count,
            })
        })
        .collect();
    (targets, overflow_events)
}

impl BoundedTraceState {
    pub(crate) fn diagnostic_limit(&self) -> u64 {
        self.max_bytes
            .saturating_sub(trace_summary_reserve(self.max_bytes))
            .saturating_sub(trace_truncation_reserve(self.max_bytes))
            .saturating_sub(trace_failure_tail_reserve(self.max_bytes))
    }

    pub(crate) fn failure_tail_limit(&self) -> u64 {
        self.max_bytes
            .saturating_sub(trace_summary_reserve(self.max_bytes))
            .saturating_sub(trace_truncation_reserve(self.max_bytes))
    }

    pub(crate) fn receipt_limit(&self) -> u64 {
        self.max_bytes
            .saturating_sub(trace_summary_reserve(self.max_bytes))
    }

    pub(crate) fn record_suppressed(
        &mut self,
        target: &str,
        bytes: u64,
        reason: TraceSuppressionReason,
    ) {
        self.suppressed_events = self.suppressed_events.saturating_add(1);
        self.suppressed_bytes = self.suppressed_bytes.saturating_add(bytes);
        let reason_count = match reason {
            TraceSuppressionReason::ByteBudget => &mut self.byte_budget_suppressed_events,
            TraceSuppressionReason::EventBudget => &mut self.event_budget_suppressed_events,
            TraceSuppressionReason::OversizeEvent => &mut self.oversize_suppressed_events,
        };
        *reason_count = reason_count.saturating_add(1);
        increment_trace_target_count(&mut self.suppressed_by_target, target);
    }

    pub(crate) fn record_filtered(&mut self, target: &str) {
        self.filtered_events = self.filtered_events.saturating_add(1);
        increment_trace_target_count(&mut self.filtered_by_target, target);
    }

    pub(crate) fn record_failure_tail(&mut self, value: &serde_json::Value) {
        let reserve = trace_failure_tail_reserve(self.max_bytes);
        if reserve.eq(&0) {
            self.failure_tail_dropped_events = self.failure_tail_dropped_events.saturating_add(1);
            return;
        }

        let value = redact_trace_json(&enrich_trace_json(value));
        let encoded_len = serde_json::to_vec(&value)
            .map(|encoded| u64::try_from(encoded.len() + 1).unwrap_or(u64::MAX))
            .unwrap_or(u64::MAX);
        if encoded_len > reserve {
            self.failure_tail_dropped_events = self.failure_tail_dropped_events.saturating_add(1);
            return;
        }
        while self.failure_tail_bytes.saturating_add(encoded_len) > reserve {
            let Some((_discarded, discarded_len)) = self.failure_tail.pop_front() else {
                break;
            };
            self.failure_tail_bytes = self.failure_tail_bytes.saturating_sub(discarded_len);
            self.failure_tail_dropped_events = self.failure_tail_dropped_events.saturating_add(1);
        }
        self.failure_tail.push_back((value, encoded_len));
        self.failure_tail_bytes = self.failure_tail_bytes.saturating_add(encoded_len);
    }

    pub(crate) fn write_line_with_limit(
        &mut self,
        value: &serde_json::Value,
        limit: u64,
    ) -> io::Result<bool> {
        let encoded = encode_trace_line(value)?;
        self.write_encoded_line_with_limit(&encoded, limit)
    }

    pub(crate) fn write_encoded_line_with_limit(
        &mut self,
        encoded: &[u8],
        limit: u64,
    ) -> io::Result<bool> {
        let encoded_len = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
        if self.bytes_written.saturating_add(encoded_len) > limit {
            return Ok(false);
        }
        self.file.write_all(encoded)?;
        self.bytes_written = self.bytes_written.saturating_add(encoded_len);
        Ok(true)
    }

    pub(crate) fn write_event(&mut self, bytes: &[u8], overflow_bytes: u64) -> io::Result<()> {
        if self.finalized {
            return Ok(());
        }
        if overflow_bytes > 0 {
            self.record_suppressed(
                "oversize_event",
                u64::try_from(bytes.len())
                    .unwrap_or(u64::MAX)
                    .saturating_add(overflow_bytes),
                TraceSuppressionReason::OversizeEvent,
            );
            return Ok(());
        }

        let trimmed = bytes.strip_suffix(b"\n").unwrap_or(bytes);
        let trimmed = trimmed.strip_suffix(b"\r").unwrap_or(trimmed);
        if trimmed.is_empty() {
            return Ok(());
        }

        let value = serde_json::from_slice::<serde_json::Value>(trimmed).unwrap_or_else(|error| {
            serde_json::json!({
                "timestamp": Utc::now().to_rfc3339(),
                "level": "ERROR",
                "target": "cass::trace",
                "fields": {
                    "event": "trace_format_error",
                    "message": error.to_string(),
                },
            })
        });
        let trace_target_name = value
            .get("target")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let is_failure_context = value
            .get("level")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|level| matches!(level, "WARN" | "ERROR"));
        let encoded = encode_trace_line(&value)?;
        let encoded_len = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
        if self.events_written >= self.max_events {
            self.diagnostic_head_closed = true;
            self.record_suppressed(
                trace_target_name,
                encoded_len,
                TraceSuppressionReason::EventBudget,
            );
            if is_failure_context {
                self.record_failure_tail(&value);
            }
        } else if self.diagnostic_head_closed {
            self.record_suppressed(
                trace_target_name,
                encoded_len,
                TraceSuppressionReason::ByteBudget,
            );
            if is_failure_context {
                self.record_failure_tail(&value);
            }
        } else if self.write_encoded_line_with_limit(&encoded, self.diagnostic_limit())? {
            self.events_written = self.events_written.saturating_add(1);
        } else {
            self.diagnostic_head_closed = true;
            self.record_suppressed(
                trace_target_name,
                encoded_len,
                TraceSuppressionReason::ByteBudget,
            );
            if is_failure_context {
                self.record_failure_tail(&value);
            }
        }
        Ok(())
    }

    pub(crate) fn finalize(&mut self) -> io::Result<()> {
        if self.finalized {
            return Ok(());
        }
        self.finalized = true;

        let mut failure_tail_written_events = 0_u64;
        let mut failure_tail_written_bytes = 0_u64;
        let mut tail_result = Ok(());
        while let Some((value, encoded_len)) = self.failure_tail.pop_front() {
            match self.write_line_with_limit(&value, self.failure_tail_limit()) {
                Ok(true) => {
                    failure_tail_written_events = failure_tail_written_events.saturating_add(1);
                    failure_tail_written_bytes =
                        failure_tail_written_bytes.saturating_add(encoded_len);
                    self.events_written = self.events_written.saturating_add(1);
                    self.failure_tail_bytes = self.failure_tail_bytes.saturating_sub(encoded_len);
                }
                Ok(false) => {
                    self.failure_tail_dropped_events = self
                        .failure_tail_dropped_events
                        .saturating_add(1)
                        .saturating_add(u64::try_from(self.failure_tail.len()).unwrap_or(u64::MAX));
                    self.failure_tail.clear();
                    self.failure_tail_bytes = 0;
                    tail_result = Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "trace failure-tail reserve could not fit buffered WARN/ERROR events",
                    ));
                    break;
                }
                Err(error) => {
                    self.failure_tail_dropped_events = self
                        .failure_tail_dropped_events
                        .saturating_add(1)
                        .saturating_add(u64::try_from(self.failure_tail.len()).unwrap_or(u64::MAX));
                    self.failure_tail.clear();
                    self.failure_tail_bytes = 0;
                    tail_result = Err(error);
                    break;
                }
            }
        }

        let receipt_result = if self.suppressed_events.eq(&0) && self.filtered_events.eq(&0) {
            Ok(())
        } else {
            let target_limit = trace_receipt_target_limit(self.max_bytes);
            let (filtered_targets, filtered_target_overflow_events) =
                ranked_trace_targets(&self.filtered_by_target, target_limit);
            let (suppressed_targets, suppressed_target_overflow_events) =
                ranked_trace_targets(&self.suppressed_by_target, target_limit);
            let event = if self.suppressed_events > 0 {
                "trace_truncated"
            } else {
                "trace_filter_summary"
            };
            let active_suppression_reasons = [
                self.byte_budget_suppressed_events,
                self.event_budget_suppressed_events,
                self.oversize_suppressed_events,
            ]
            .into_iter()
            .filter(|count| *count > 0)
            .count();
            let reason = match active_suppression_reasons {
                0 => "default_filter_policy",
                1 if self.byte_budget_suppressed_events > 0 => "byte_budget",
                1 if self.event_budget_suppressed_events > 0 => "event_budget",
                1 => "oversize_event",
                _ => "multiple_limits",
            };
            let receipt = serde_json::json!({
                "timestamp": Utc::now().to_rfc3339(),
                "level": if self.suppressed_events > 0 { "WARN" } else { "INFO" },
                "target": "cass::trace",
                "fields": {
                    "event": event,
                    "reason": reason,
                    "artifact_complete": self.suppressed_events.eq(&0),
                    "max_bytes": self.max_bytes,
                    "max_events": self.max_events,
                    "bytes_written_before_receipt": self.bytes_written,
                    "events_written_before_receipt": self.events_written,
                    "suppressed_events": self.suppressed_events,
                    "suppressed_bytes": self.suppressed_bytes,
                    "suppression_reasons": {
                        "byte_budget": self.byte_budget_suppressed_events,
                        "event_budget": self.event_budget_suppressed_events,
                        "oversize_event": self.oversize_suppressed_events,
                    },
                    "suppressed_targets": suppressed_targets,
                    "suppressed_target_overflow_events": suppressed_target_overflow_events,
                    "failure_tail_events": failure_tail_written_events,
                    "failure_tail_bytes": failure_tail_written_bytes,
                    "failure_tail_dropped_events": self.failure_tail_dropped_events,
                    "filtered_events": self.filtered_events,
                    "filtered_targets": filtered_targets,
                    "filtered_target_overflow_events": filtered_target_overflow_events,
                },
            });
            let receipt_limit = self.receipt_limit();
            match self.write_line_with_limit(&receipt, receipt_limit) {
                Ok(true) => Ok(()),
                Ok(false) => {
                    let compact_receipt = serde_json::json!({
                        "timestamp": Utc::now().to_rfc3339(),
                        "level": if self.suppressed_events > 0 { "WARN" } else { "INFO" },
                        "target": "cass::trace",
                        "fields": {
                            "event": event,
                            "receipt_compact": true,
                            "reason": reason,
                            "artifact_complete": self.suppressed_events.eq(&0),
                            "max_bytes": self.max_bytes,
                            "max_events": self.max_events,
                            "bytes_written_before_receipt": self.bytes_written,
                            "events_written_before_receipt": self.events_written,
                            "suppressed_events": self.suppressed_events,
                            "suppressed_bytes": self.suppressed_bytes,
                            "suppression_reasons": {
                                "byte_budget": self.byte_budget_suppressed_events,
                                "event_budget": self.event_budget_suppressed_events,
                                "oversize_event": self.oversize_suppressed_events,
                            },
                            "suppressed_target_overflow_events": self.suppressed_events,
                            "failure_tail_events": failure_tail_written_events,
                            "failure_tail_bytes": failure_tail_written_bytes,
                            "failure_tail_dropped_events": self.failure_tail_dropped_events,
                            "filtered_events": self.filtered_events,
                            "filtered_target_overflow_events": self.filtered_events,
                        },
                    });
                    match self.write_line_with_limit(&compact_receipt, receipt_limit) {
                        Ok(true) => Ok(()),
                        Ok(false) => Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "trace tail reserve could not fit even the compact filter/truncation receipt",
                        )),
                        Err(error) => Err(error),
                    }
                }
                Err(error) => Err(error),
            }
        };
        let unlock_result = fs2::FileExt::unlock(&self.file);
        tail_result.and(receipt_result).and(unlock_result)
    }
}

#[derive(Clone)]
pub(crate) struct BoundedTraceMakeWriter {
    pub(crate) state: Arc<Mutex<BoundedTraceState>>,
}

impl BoundedTraceMakeWriter {
    pub(crate) fn open(
        path: &Path,
        max_bytes: u64,
        max_events: u64,
    ) -> io::Result<(Self, CliTraceGuard)> {
        let (file, bytes_written, events_written, prior_head_closed) =
            open_trace_append_file(path, max_bytes)?;
        let diagnostic_limit = max_bytes
            .saturating_sub(trace_summary_reserve(max_bytes))
            .saturating_sub(trace_truncation_reserve(max_bytes))
            .saturating_sub(trace_failure_tail_reserve(max_bytes));
        let diagnostic_head_closed =
            prior_head_closed || events_written >= max_events || bytes_written >= diagnostic_limit;
        let state = Arc::new(Mutex::new(BoundedTraceState {
            file,
            max_bytes,
            max_events,
            bytes_written,
            events_written,
            diagnostic_head_closed,
            suppressed_events: 0,
            suppressed_bytes: 0,
            byte_budget_suppressed_events: 0,
            event_budget_suppressed_events: 0,
            oversize_suppressed_events: 0,
            filtered_events: 0,
            filtered_by_target: BTreeMap::new(),
            suppressed_by_target: BTreeMap::new(),
            failure_tail: VecDeque::new(),
            failure_tail_bytes: 0,
            failure_tail_dropped_events: 0,
            finalized: false,
            last_error: None,
        }));
        Ok((
            Self {
                state: Arc::clone(&state),
            },
            CliTraceGuard { state },
        ))
    }
}

pub(crate) struct BoundedTraceEventWriter {
    pub(crate) state: Arc<Mutex<BoundedTraceState>>,
    pub(crate) buffer: Vec<u8>,
    pub(crate) overflow_bytes: u64,
}

impl Write for BoundedTraceEventWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let available = TRACE_EVENT_BUFFER_MAX_BYTES.saturating_sub(self.buffer.len());
        let accepted = available.min(bytes.len());
        self.buffer.extend_from_slice(&bytes[..accepted]);
        self.overflow_bytes = self
            .overflow_bytes
            .saturating_add(u64::try_from(bytes.len() - accepted).unwrap_or(u64::MAX));
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for BoundedTraceEventWriter {
    fn drop(&mut self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if let Err(error) = state.write_event(&self.buffer, self.overflow_bytes) {
            state.last_error.get_or_insert_with(|| error.to_string());
        }
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BoundedTraceMakeWriter {
    type Writer = BoundedTraceEventWriter;

    fn make_writer(&'a self) -> Self::Writer {
        BoundedTraceEventWriter {
            state: Arc::clone(&self.state),
            buffer: Vec::with_capacity(1024),
            overflow_bytes: 0,
        }
    }
}

pub(crate) struct CliTraceGuard {
    pub(crate) state: Arc<Mutex<BoundedTraceState>>,
}

impl Drop for CliTraceGuard {
    fn drop(&mut self) {
        let Ok(mut state) = self.state.lock() else {
            eprintln!("trace-write error: trace state lock poisoned");
            return;
        };
        if let Err(error) = state.finalize() {
            state.last_error.get_or_insert_with(|| error.to_string());
        }
        if let Some(error) = state.last_error.take() {
            eprintln!("trace-write error: {error}");
        }
    }
}

/// Initialize the tracing subscriber for non-TUI commands with robot-safe
/// stdout/stderr hygiene plus an optional explicit `--trace-file` sink.
///
/// The stderr layer carries the robot-aware `filter` (pinned to `error` in
/// robot/quiet mode by [`build_robot_aware_log_filter`], per uojcg.2.1), so
/// dependency INFO/WARN logging can never interleave with a JSON stream. When
/// `trace_file` is `Some`, a second layer writes valid JSONL, retains CASS DEBUG
/// plus global WARN/ERROR events, redacts secret-bearing fields, and stops at a
/// hard byte/event budget with a structured `trace_truncated` receipt.
///
/// The returned guard finalizes the truncation receipt before the command summary
/// is appended. The caller MUST keep it alive for the duration of the command.
pub(crate) fn init_cli_tracing(
    filter: EnvFilter,
    trace_file: Option<&Path>,
    ansi: bool,
) -> Option<CliTraceGuard> {
    use tracing_subscriber::Layer;

    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .compact()
        .with_target(false)
        .with_ansi(ansi)
        .with_filter(filter);

    let (trace_layer, guard) = match trace_file {
        Some(path) => match BoundedTraceMakeWriter::open(
            path,
            trace_file_max_bytes(),
            trace_file_max_events(),
        ) {
            Ok((writer, guard)) => {
                let layer = if let Some(filter) = trace_file_log_filter_override() {
                    tracing_subscriber::fmt::layer()
                        .json()
                        .with_writer(writer)
                        .with_ansi(false)
                        .with_filter(filter)
                        .boxed()
                } else {
                    let filter_state = Arc::clone(&writer.state);
                    // The policy never enables TRACE. Publishing that ceiling is
                    // essential: without it a dynamic filter marks TRACE
                    // callsites "sometimes", so per-character tokenizer
                    // instrumentation still invokes this closure for every
                    // character even though every record is rejected.
                    let filter =
                        tracing_subscriber::filter::dynamic_filter_fn(move |metadata, _context| {
                            let enabled = default_trace_metadata_enabled(metadata);
                            if !enabled
                                && metadata.is_event()
                                && let Ok(mut state) = filter_state.lock()
                            {
                                state.record_filtered(metadata.target());
                            }
                            enabled
                        })
                        .with_max_level_hint(tracing_subscriber::filter::LevelFilter::DEBUG);
                    tracing_subscriber::fmt::layer()
                        .json()
                        .with_writer(writer)
                        .with_ansi(false)
                        .with_filter(filter)
                        .boxed()
                };
                (Some(layer), Some(guard))
            }
            Err(e) => {
                eprintln!("trace-file open error ({}): {e}", path.display());
                (None, None)
            }
        },
        None => (None, None),
    };

    tracing_subscriber::registry()
        .with(stderr_layer)
        .with(trace_layer)
        .init();
    guard
}
