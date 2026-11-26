use super::*;

/// Why a single-conversation OOM became a *quarantine* rather than a defer
/// (#364). Recorded in the poison record so post-mortem triage (and
/// `cass diag --quarantine`) can distinguish a genuine host-memory-pressure
/// OOM from a precautionary quarantine of a large conversation taken when host
/// pressure could not be determined — the exact confusion behind the "false
/// OOM quarantine" report.
#[derive(Debug, Clone, Copy)]
pub(in crate::indexer) struct PoisonMemoryAttribution {
    /// `watch_oom_under_real_memory_pressure()` at quarantine time: `Some(true)`
    /// = genuine host pressure, `Some(false)` = a bounded-allocation guard with
    /// no host pressure, `None` = pressure could not be sampled.
    pub(in crate::indexer) real_memory_pressure: Option<bool>,
    /// Indexed text size of the conversation that OOMed.
    pub(in crate::indexer) indexed_text_bytes: usize,
}

impl PoisonMemoryAttribution {
    pub(in crate::indexer) fn memory_pressure_label(self) -> &'static str {
        match self.real_memory_pressure {
            Some(true) => "real",
            Some(false) => "none",
            None => "unknown",
        }
    }

    /// The trigger that turned this OOM into a quarantine (mirrors the
    /// `should_quarantine` decision in the ingest paths).
    pub(in crate::indexer) fn quarantine_trigger(self) -> &'static str {
        match self.real_memory_pressure {
            Some(true) => "real_host_memory_pressure",
            Some(false) => "bounded_allocation_guard_no_pressure",
            None => "unknown_pressure_large_conversation_precaution",
        }
    }
}

pub(in crate::indexer) fn record_watch_poison_conversation(
    data_dir: &Path,
    conv: &NormalizedConversation,
    error: &anyhow::Error,
    attribution: Option<PoisonMemoryAttribution>,
) -> Result<()> {
    record_poison_conversation(
        data_dir,
        WATCH_INGEST_POISON_FILE,
        "watch-ingest-out-of-memory",
        conv,
        error,
        attribution,
    )
}

pub(in crate::indexer) fn record_index_poison_conversation(
    data_dir: &Path,
    conv: &NormalizedConversation,
    error: &anyhow::Error,
    attribution: Option<PoisonMemoryAttribution>,
) -> Result<()> {
    record_poison_conversation(
        data_dir,
        INDEX_INGEST_POISON_FILE,
        "index-ingest-out-of-memory",
        conv,
        error,
        attribution,
    )
}

pub(in crate::indexer) const WATCH_INGEST_POISON_FILE: &str = "watch_ingest_poison.jsonl";
pub(in crate::indexer) const INDEX_INGEST_POISON_FILE: &str = "index_ingest_poison.jsonl";
pub(in crate::indexer) const POISON_CONVERSATION_QUARANTINE_SCHEMA_VERSION: i64 = 1;
pub(in crate::indexer) const INGEST_QUARANTINE_CIRCUIT_DEFAULT_WINDOW_SECONDS: i64 = 3_600;
pub(in crate::indexer) const INGEST_QUARANTINE_CIRCUIT_DEFAULT_LIMIT: usize = 25;

pub(in crate::indexer) fn record_poison_conversation(
    data_dir: &Path,
    file_name: &str,
    reason: &str,
    conv: &NormalizedConversation,
    error: &anyhow::Error,
    attribution: Option<PoisonMemoryAttribution>,
) -> Result<()> {
    let quarantine_dir = data_dir.join("quarantine");
    fs::create_dir_all(&quarantine_dir).with_context(|| {
        format!(
            "creating ingest quarantine directory {}",
            quarantine_dir.display()
        )
    })?;

    let now_ms = FrankenStorage::now_millis();
    let conversation_id = poison_conversation_id(conv);
    let schema_version_at_quarantine = crate::storage::sqlite::CURRENT_SCHEMA_VERSION;
    let key = (conversation_id.clone(), schema_version_at_quarantine);
    let path = quarantine_dir.join(file_name);
    let mut raw_preserved_lines = Vec::new();
    let mut records = BTreeMap::<(String, i64), serde_json::Value>::new();

    if path.exists() {
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("reading ingest quarantine file {}", path.display()))?;
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            match serde_json::from_str::<serde_json::Value>(trimmed) {
                Ok(value) => {
                    if let Some(existing_key) = poison_record_key_from_value(&value) {
                        records.insert(existing_key, value);
                    } else {
                        raw_preserved_lines.push(trimmed.to_string());
                    }
                }
                Err(_) => raw_preserved_lines.push(trimmed.to_string()),
            }
        }
    }

    let previous = records.remove(&key);
    let first_quarantined_at_ms = previous
        .as_ref()
        .and_then(poison_record_first_quarantined_at_ms)
        .unwrap_or(now_ms);
    let attempt_count = previous
        .as_ref()
        .and_then(|value| value.get("attempt_count"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
        .saturating_add(1);

    // Capture the FULL error chain in `last_error` so post-mortem triage can
    // see why the OOM bubbled up (which crate, which operation). Without this
    // we only saw the top-level `anyhow::Error::Display` rendering, which for
    // `FrankenError::OutOfMemory` is just the bare string "out of memory" and
    // tells us nothing about which subsystem failed.
    let full_error_chain = format_error_chain(error);
    let mut record = serde_json::json!({
        "schema_version": POISON_CONVERSATION_QUARANTINE_SCHEMA_VERSION,
        "conversation_id": conversation_id,
        "schema_version_at_quarantine": schema_version_at_quarantine,
        "first_quarantined_at_ms": first_quarantined_at_ms,
        "last_attempt_at_ms": now_ms,
        "attempt_count": attempt_count,
        "cass_version_at_quarantine": current_cass_version(),
        "reason": reason,
        "error_kind": "out-of-memory",
        "last_error": full_error_chain,
        "error": error.to_string(),
        "agent_slug": conv.agent_slug,
        "external_id": conv.external_id.as_deref(),
        "source_path": conv.source_path.display().to_string(),
        "workspace": conv.workspace.as_ref().map(|path| path.display().to_string()),
        "started_at": conv.started_at,
        "ended_at": conv.ended_at,
        "message_count": conv.messages.len(),
    });
    // #364: record WHY this OOM became a quarantine (genuine host pressure vs a
    // precautionary large-conversation quarantine), additively so pre-#364
    // records without attribution stay shape-compatible.
    if let Some(attribution) = attribution
        && let Some(object) = record.as_object_mut()
    {
        object.insert(
            "memory_pressure".to_string(),
            serde_json::json!(attribution.memory_pressure_label()),
        );
        object.insert(
            "quarantine_trigger".to_string(),
            serde_json::json!(attribution.quarantine_trigger()),
        );
        object.insert(
            "indexed_text_bytes".to_string(),
            serde_json::json!(attribution.indexed_text_bytes),
        );
    }
    records.insert(key, record);

    let mut lines = raw_preserved_lines;
    lines.extend(records.values().map(serde_json::Value::to_string));
    write_poison_jsonl_atomically(&path, &lines)?;
    record_structured_poison_quarantine_state(
        data_dir,
        &conversation_id,
        schema_version_at_quarantine,
        reason,
        error,
        now_ms,
    );
    Ok(())
}

pub(in crate::indexer) fn current_cass_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::indexer) struct StaleIndexIngestQuarantineRetry {
    pub(in crate::indexer) stale_records: usize,
    pub(in crate::indexer) legacy_records: usize,
    pub(in crate::indexer) previous_versions: Vec<String>,
}

#[derive(Default)]
pub(in crate::indexer) struct StalePoisonVersionAccumulator {
    pub(in crate::indexer) stale_keys: BTreeSet<(String, i64)>,
    pub(in crate::indexer) legacy_keys: BTreeSet<(String, i64)>,
    pub(in crate::indexer) previous_versions: BTreeSet<String>,
    /// Keys that have been observed with `cass_version == current_version` in
    /// at least one storage surface (JSONL or structured).  A key here must
    /// not be promoted to `stale_keys` even if a *second* observation from a
    /// different surface reports it as `None` (legacy) or an older version.
    ///
    /// Without this, the cross-surface race where
    /// `mark_stale_index_ingest_jsonl_retry_attempted` succeeds but
    /// `mark_stale_index_ingest_structured_retry_attempted`'s `state.save()`
    /// fails would leave the structured record at `None`, causing
    /// `stale_index_ingest_quarantine_version_retry` to signal a retry every
    /// subsequent run even though the JSONL surface already records the retry
    /// as complete for the current version — an unbounded retry storm
    /// (cass#261).
    pub(in crate::indexer) already_current_keys: BTreeSet<(String, i64)>,
}

impl StalePoisonVersionAccumulator {
    pub(in crate::indexer) fn observe(
        &mut self,
        key: (String, i64),
        cass_version: Option<&str>,
        current_version: &str,
    ) {
        match cass_version {
            Some(version) if version == current_version => {
                // This key is already marked as retried under the current
                // binary.  Record it so that a stale observation from another
                // storage surface cannot re-promote it to stale_keys.
                self.stale_keys.remove(&key);
                self.legacy_keys.remove(&key);
                self.already_current_keys.insert(key);
            }
            Some(version) => {
                if !self.already_current_keys.contains(&key) {
                    self.stale_keys.insert(key);
                    self.previous_versions.insert(version.to_string());
                }
            }
            None => {
                if !self.already_current_keys.contains(&key) {
                    self.legacy_keys.insert(key.clone());
                    self.stale_keys.insert(key);
                    self.previous_versions.insert("unknown".to_string());
                }
            }
        }
    }

    pub(in crate::indexer) fn finish(self) -> Option<StaleIndexIngestQuarantineRetry> {
        (!self.stale_keys.is_empty()).then(|| StaleIndexIngestQuarantineRetry {
            stale_records: self.stale_keys.len(),
            legacy_records: self.legacy_keys.len(),
            previous_versions: self.previous_versions.into_iter().collect(),
        })
    }
}

pub(in crate::indexer) fn stale_index_ingest_quarantine_version_retry(
    data_dir: &Path,
) -> Result<Option<StaleIndexIngestQuarantineRetry>> {
    let current_version = current_cass_version();
    let mut accumulator = StalePoisonVersionAccumulator::default();
    let path = data_dir.join("quarantine").join(INDEX_INGEST_POISON_FILE);
    if path.exists() {
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("reading index ingest quarantine file {}", path.display()))?;
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) else {
                continue;
            };
            if let Some(key) = poison_record_key_from_value(&value) {
                accumulator.observe(key, poison_record_cass_version(&value), current_version);
            }
        }
    }

    let state = QuarantineState::load(data_dir);
    for (key, record) in state.iter() {
        if record.last_reason.starts_with("index-ingest-out-of-memory") {
            accumulator.observe(
                (key.conversation_id, i64::from(key.schema_version)),
                record.cass_version_at_quarantine.as_deref(),
                current_version,
            );
        }
    }

    Ok(accumulator.finish())
}

pub(in crate::indexer) fn mark_stale_index_ingest_quarantine_retry_attempted(
    data_dir: &Path,
) -> Result<usize> {
    let jsonl_marked = mark_stale_index_ingest_jsonl_retry_attempted(data_dir)?;
    let structured_marked = mark_stale_index_ingest_structured_retry_attempted(data_dir);
    Ok(jsonl_marked.saturating_add(structured_marked))
}

pub(in crate::indexer) fn mark_stale_index_ingest_jsonl_retry_attempted(
    data_dir: &Path,
) -> Result<usize> {
    let path = data_dir.join("quarantine").join(INDEX_INGEST_POISON_FILE);
    if !path.exists() {
        return Ok(0);
    }

    let contents = fs::read_to_string(&path)
        .with_context(|| format!("reading index ingest quarantine file {}", path.display()))?;
    let current_version = current_cass_version();
    let mut retained_lines = Vec::new();
    let mut marked = 0usize;

    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(mut value) = serde_json::from_str::<serde_json::Value>(trimmed) else {
            retained_lines.push(trimmed.to_string());
            continue;
        };
        let should_mark = poison_record_key_from_value(&value).is_some()
            && poison_record_version_needs_retry(
                poison_record_cass_version(&value),
                current_version,
            );
        if should_mark && let Some(object) = value.as_object_mut() {
            object.insert(
                "cass_version_at_quarantine".to_string(),
                serde_json::json!(current_version),
            );
            marked = marked.saturating_add(1);
            retained_lines.push(value.to_string());
        } else {
            retained_lines.push(trimmed.to_string());
        }
    }

    if marked == 0 {
        return Ok(0);
    }

    write_poison_jsonl_atomically(&path, &retained_lines)?;
    Ok(marked)
}

pub(in crate::indexer) fn mark_stale_index_ingest_structured_retry_attempted(
    data_dir: &Path,
) -> usize {
    let mut state = QuarantineState::load(data_dir);
    if state.is_empty() {
        return 0;
    }

    let current_version = current_cass_version();
    let mut marked = 0usize;
    for record in state.entries.values_mut() {
        if record.last_reason.starts_with("index-ingest-out-of-memory")
            && record.is_version_stale_for_retry(current_version)
        {
            record.cass_version_at_quarantine = Some(current_version.to_string());
            marked = marked.saturating_add(1);
        }
    }
    if marked == 0 {
        return 0;
    }
    if let Err(err) = state.save(data_dir) {
        tracing::warn!(
            data_dir = %data_dir.display(),
            error = %err,
            "failed to persist structured ingest quarantine retry marker"
        );
        return 0;
    }
    marked
}

pub(in crate::indexer) fn clear_poison_conversations_after_successful_ingest(
    data_dir: &Path,
    file_name: &str,
    reason: &str,
    convs: &[NormalizedConversation],
) {
    if convs.is_empty() {
        return;
    }
    let conversation_ids = convs
        .iter()
        .map(poison_conversation_id)
        .collect::<BTreeSet<_>>();

    let jsonl_cleared =
        match clear_poison_jsonl_records(data_dir, file_name, reason, &conversation_ids) {
            Ok(cleared) => cleared,
            Err(err) => {
                tracing::warn!(
                    data_dir = %data_dir.display(),
                    file_name,
                    error = %err,
                    "failed to clear successful ingest records from poison quarantine JSONL"
                );
                0
            }
        };
    let structured_cleared =
        clear_structured_poison_quarantine_records(data_dir, reason, &conversation_ids);

    let cleared = jsonl_cleared.saturating_add(structured_cleared);
    if cleared > 0 {
        tracing::info!(
            data_dir = %data_dir.display(),
            file_name,
            reason,
            cleared,
            "cleared poison quarantine records after successful ingest retry"
        );
    }
}

pub(in crate::indexer) fn clear_poison_jsonl_records(
    data_dir: &Path,
    file_name: &str,
    reason: &str,
    conversation_ids: &BTreeSet<String>,
) -> Result<usize> {
    clear_poison_jsonl_records_where(data_dir, file_name, reason, |conversation_id, _| {
        conversation_ids.contains(conversation_id)
    })
}

pub(in crate::indexer) fn clear_poison_jsonl_key_records(
    data_dir: &Path,
    file_name: &str,
    reason: &str,
    quarantine_keys: &BTreeSet<(String, i64)>,
) -> Result<usize> {
    clear_poison_jsonl_records_where(
        data_dir,
        file_name,
        reason,
        |conversation_id, schema_version| {
            quarantine_keys.contains(&(conversation_id.to_string(), schema_version))
        },
    )
}

/// Read the keyed poison-ledger records used by operator-facing quarantine
/// commands. The structured checkpoint can legitimately be missing after an
/// earlier best-effort save failure, so list/clear must include these records
/// rather than claiming the quarantine is empty while health remains degraded.
pub(crate) fn operator_quarantine_poison_records(
    data_dir: &Path,
) -> Result<BTreeMap<(String, i64), serde_json::Value>> {
    let quarantine_dir = data_dir.join("quarantine");
    let mut records = BTreeMap::new();
    let current_version = current_cass_version();
    for file_name in [WATCH_INGEST_POISON_FILE, INDEX_INGEST_POISON_FILE] {
        let path = quarantine_dir.join(file_name);
        let contents = match fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("reading ingest quarantine file {}", path.display()));
            }
        };
        for line in contents.lines() {
            let Ok(value) = serde_json::from_str::<serde_json::Value>(line.trim()) else {
                continue;
            };
            if let Some(key) = poison_record_key_from_value(&value) {
                match records.entry(key) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(value);
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        let existing = entry.get();
                        let existing_is_current =
                            poison_record_cass_version(existing) == Some(current_version);
                        let candidate_is_current =
                            poison_record_cass_version(&value) == Some(current_version);
                        let existing_last_attempt = existing
                            .get("last_attempt_at_ms")
                            .and_then(serde_json::Value::as_i64)
                            .or_else(|| {
                                existing
                                    .get("recorded_at_ms")
                                    .and_then(serde_json::Value::as_i64)
                            })
                            .unwrap_or(i64::MIN);
                        let candidate_last_attempt = value
                            .get("last_attempt_at_ms")
                            .and_then(serde_json::Value::as_i64)
                            .or_else(|| {
                                value
                                    .get("recorded_at_ms")
                                    .and_then(serde_json::Value::as_i64)
                            })
                            .unwrap_or(i64::MIN);
                        if (candidate_is_current && !existing_is_current)
                            || (candidate_is_current == existing_is_current
                                && candidate_last_attempt > existing_last_attempt)
                        {
                            entry.insert(value);
                        }
                    }
                }
            }
        }
    }
    Ok(records)
}

/// Clear operator-selected quarantine keys from both poison ledgers. The
/// structured checkpoint is handled by the caller so it can restore that
/// checkpoint if either ledger rewrite fails.
pub(crate) fn clear_operator_quarantine_poison_records(
    data_dir: &Path,
    quarantine_keys: &BTreeSet<(String, i64)>,
) -> Result<usize> {
    let mut cleared = 0usize;
    for (file_name, reason) in [
        (WATCH_INGEST_POISON_FILE, "watch-ingest-out-of-memory"),
        (INDEX_INGEST_POISON_FILE, "index-ingest-out-of-memory"),
    ] {
        cleared = cleared.saturating_add(clear_poison_jsonl_key_records(
            data_dir,
            file_name,
            reason,
            quarantine_keys,
        )?);
    }
    Ok(cleared)
}

pub(in crate::indexer) fn clear_poison_jsonl_records_where(
    data_dir: &Path,
    file_name: &str,
    reason: &str,
    should_clear_key: impl Fn(&str, i64) -> bool,
) -> Result<usize> {
    let path = data_dir.join("quarantine").join(file_name);
    if !path.exists() {
        return Ok(0);
    }

    let contents = fs::read_to_string(&path)
        .with_context(|| format!("reading ingest quarantine file {}", path.display()))?;
    let mut retained_lines = Vec::new();
    let mut cleared = 0usize;

    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let should_clear = serde_json::from_str::<serde_json::Value>(trimmed)
            .ok()
            .and_then(|value| {
                let record_reason_matches = value
                    .get("reason")
                    .and_then(serde_json::Value::as_str)
                    .is_none_or(|record_reason| record_reason == reason);
                record_reason_matches
                    .then(|| poison_record_key_from_value(&value))
                    .flatten()
            })
            .is_some_and(|(conversation_id, schema_version)| {
                should_clear_key(&conversation_id, schema_version)
            });
        if should_clear {
            cleared = cleared.saturating_add(1);
        } else {
            retained_lines.push(trimmed.to_string());
        }
    }

    if cleared == 0 {
        return Ok(0);
    }

    write_poison_jsonl_atomically(&path, &retained_lines)?;
    Ok(cleared)
}

/// Replace one poison ledger without ever exposing a truncated or partially
/// rewritten file. The temp file lives beside the destination so the final
/// rename stays on the same filesystem and is atomic for readers.
pub(in crate::indexer) fn write_poison_jsonl_atomically(
    path: &Path,
    lines: &[String],
) -> Result<()> {
    let temp_path = unique_atomic_temp_path(path);
    {
        let file = create_new_atomic_sidecar_file(&temp_path).with_context(|| {
            format!(
                "creating temporary ingest quarantine file {}",
                temp_path.display()
            )
        })?;
        let mut writer = BufWriter::new(file);
        for line in lines {
            writeln!(writer, "{line}").with_context(|| {
                format!(
                    "writing temporary ingest quarantine file {}",
                    temp_path.display()
                )
            })?;
        }
        writer.flush().with_context(|| {
            format!(
                "flushing temporary ingest quarantine file {}",
                temp_path.display()
            )
        })?;
        writer.get_ref().sync_all().with_context(|| {
            format!(
                "syncing temporary ingest quarantine file {}",
                temp_path.display()
            )
        })?;
    }
    replace_file_from_temp(&temp_path, path)
        .with_context(|| format!("replacing ingest quarantine file {}", path.display()))
}

pub(in crate::indexer) fn clear_structured_poison_quarantine_records(
    data_dir: &Path,
    reason: &str,
    conversation_ids: &BTreeSet<String>,
) -> usize {
    let mut state = QuarantineState::load(data_dir);
    if state.is_empty() {
        return 0;
    }

    let keys = state
        .iter()
        .filter(|(key, record)| {
            conversation_ids.contains(&key.conversation_id)
                && record.last_reason.starts_with(reason)
        })
        .map(|(key, _)| key)
        .collect::<Vec<_>>();
    if keys.is_empty() {
        return 0;
    }

    let mut cleared = 0usize;
    for key in keys {
        if state.clear(&key) {
            cleared = cleared.saturating_add(1);
        }
    }
    if let Err(err) = state.save(data_dir) {
        tracing::warn!(
            data_dir = %data_dir.display(),
            error = %err,
            "failed to persist structured ingest quarantine cleanup"
        );
        return 0;
    }
    cleared
}

pub(in crate::indexer) fn record_structured_poison_quarantine_state(
    data_dir: &Path,
    conversation_id: &str,
    schema_version_at_quarantine: i64,
    reason: &str,
    error: &anyhow::Error,
    now_ms: i64,
) {
    let Ok(schema_version) = u32::try_from(schema_version_at_quarantine) else {
        tracing::warn!(
            schema_version_at_quarantine,
            "skipping structured ingest quarantine state update because schema version is out of range"
        );
        return;
    };
    let Some(now) = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(now_ms) else {
        tracing::warn!(
            now_ms,
            "skipping structured ingest quarantine state update because timestamp is invalid"
        );
        return;
    };

    let mut state = QuarantineState::load(data_dir);
    let key = QuarantineKey::new(conversation_id, schema_version);
    state.record_attempt(&key, format!("{reason}: {error}"), now);
    if let Err(err) = state.save(data_dir) {
        tracing::warn!(
            data_dir = %data_dir.display(),
            error = %err,
            "failed to persist structured ingest quarantine state"
        );
    }
}

pub(in crate::indexer) fn poison_conversation_id(conv: &NormalizedConversation) -> String {
    let workspace = conv
        .workspace
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_default();
    let external_id = conv.external_id.as_deref().unwrap_or("");
    format!(
        "{}|{}|{}|{}|{}|{}|{}",
        conv.agent_slug,
        conv.source_path.display(),
        workspace,
        external_id,
        conv.started_at.unwrap_or_default(),
        conv.ended_at.unwrap_or_default(),
        conv.messages.len()
    )
}

pub(in crate::indexer) fn poison_record_key_from_value(
    value: &serde_json::Value,
) -> Option<(String, i64)> {
    let schema_version_at_quarantine = value
        .get("schema_version_at_quarantine")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(crate::storage::sqlite::CURRENT_SCHEMA_VERSION);
    if let Some(conversation_id) = value
        .get("conversation_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .filter(|id| !id.trim().is_empty())
    {
        return Some((conversation_id, schema_version_at_quarantine));
    }

    let agent_slug = value
        .get("agent_slug")
        .and_then(serde_json::Value::as_str)?;
    let source_path = value
        .get("source_path")
        .and_then(serde_json::Value::as_str)?;
    let workspace = value
        .get("workspace")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let external_id = value
        .get("external_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let started_at = value
        .get("started_at")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_default();
    let ended_at = value
        .get("ended_at")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_default();
    let message_count = value
        .get("message_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    Some((
        format!(
            "{agent_slug}|{source_path}|{workspace}|{external_id}|{started_at}|{ended_at}|{message_count}"
        ),
        schema_version_at_quarantine,
    ))
}

pub(in crate::indexer) fn poison_record_cass_version(value: &serde_json::Value) -> Option<&str> {
    value
        .get("cass_version_at_quarantine")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|version| !version.is_empty())
}

pub(in crate::indexer) fn poison_record_version_needs_retry(
    cass_version: Option<&str>,
    current_version: &str,
) -> bool {
    !matches!(cass_version, Some(version) if version == current_version)
}

pub(in crate::indexer) fn poison_record_first_quarantined_at_ms(
    value: &serde_json::Value,
) -> Option<i64> {
    value
        .get("first_quarantined_at_ms")
        .and_then(serde_json::Value::as_i64)
        .or_else(|| {
            value
                .get("recorded_at_ms")
                .and_then(serde_json::Value::as_i64)
        })
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ConversationIngestQuarantineSummary {
    pub schema_version: i64,
    pub status: String,
    pub quarantined_conversations: usize,
    /// Conversations the watcher keeps deferring after a bounded-guard NoMem
    /// (GH#510): never quarantined, but missing from search until one ingests.
    pub deferred_conversations: usize,
    pub recent_quarantined_conversations: usize,
    pub recent_window_seconds: i64,
    pub circuit_breaker_limit: usize,
    pub circuit_breaker_active: bool,
    pub quarantine_files: Vec<String>,
    pub newest_last_attempt_at_ms: Option<i64>,
    pub recommended_action: Option<String>,
}

pub fn conversation_ingest_quarantine_summary(
    data_dir: &Path,
) -> ConversationIngestQuarantineSummary {
    let quarantine_dir = data_dir.join("quarantine");
    let mut keys = BTreeSet::<(String, i64)>::new();
    // The circuit breaker fires on a *recent burst* of quarantines. A stable,
    // already-triaged backlog of same-version irreducible failures recurs on
    // every refresh (same `cass_version_at_quarantine`, `last_attempt_at` bumped)
    // but its content is already searchable from a prior pass — surfacing that as
    // a `critical` breaker (and thus `health_level=critical`) is a misreport
    // (#290). So `recent_keys` counts only *fresh* recent quarantines: those that
    // are retry-eligible (version-stale/legacy/new), i.e. NOT same-version
    // re-skips. Same-version recurrences are still counted in the total backlog
    // (`keys`) and reported as `degraded`, just not as a `critical` burst.
    let current_version = current_cass_version();
    let mut recent_keys = BTreeSet::<(String, i64)>::new();
    let mut quarantine_files = Vec::new();
    let mut newest_last_attempt_at_ms: Option<i64> = None;
    let recent_window_seconds = ingest_quarantine_circuit_window_seconds();
    let recent_cutoff_ms =
        FrankenStorage::now_millis().saturating_sub(recent_window_seconds.saturating_mul(1_000));
    let circuit_breaker_limit = ingest_quarantine_circuit_limit();

    let state_path = QuarantineState::path(data_dir);
    if state_path.exists() {
        quarantine_files.push(state_path.display().to_string());
        let state = QuarantineState::load(data_dir);
        for (key, record) in state.iter() {
            let last_attempt_at_ms = record.last_attempt_at.timestamp_millis();
            let summary_key = (key.conversation_id, i64::from(key.schema_version));
            keys.insert(summary_key.clone());
            if last_attempt_at_ms >= recent_cutoff_ms
                && record.is_version_stale_for_retry(current_version)
            {
                recent_keys.insert(summary_key);
            }
            newest_last_attempt_at_ms = Some(
                newest_last_attempt_at_ms.map_or(last_attempt_at_ms, |current| {
                    current.max(last_attempt_at_ms)
                }),
            );
        }
    }

    for file_name in [WATCH_INGEST_POISON_FILE, INDEX_INGEST_POISON_FILE] {
        let path = quarantine_dir.join(file_name);
        if !path.exists() {
            continue;
        }
        quarantine_files.push(path.display().to_string());
        let Ok(contents) = fs::read_to_string(&path) else {
            continue;
        };
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) else {
                continue;
            };
            let key = poison_record_key_from_value(&value);
            let is_fresh_quarantine = poison_record_version_needs_retry(
                poison_record_cass_version(&value),
                current_version,
            );
            if let Some(last_attempt_at_ms) = value
                .get("last_attempt_at_ms")
                .and_then(serde_json::Value::as_i64)
                .or_else(|| {
                    value
                        .get("recorded_at_ms")
                        .and_then(serde_json::Value::as_i64)
                })
            {
                if let Some(key) = key.as_ref()
                    && last_attempt_at_ms >= recent_cutoff_ms
                    && is_fresh_quarantine
                {
                    recent_keys.insert(key.clone());
                }
                newest_last_attempt_at_ms = Some(
                    newest_last_attempt_at_ms.map_or(last_attempt_at_ms, |current| {
                        current.max(last_attempt_at_ms)
                    }),
                );
            }
            if let Some(key) = key {
                keys.insert(key);
            }
        }
    }

    let quarantined_conversations = keys.len();
    let recent_quarantined_conversations = recent_keys.len();
    let circuit_breaker_active =
        circuit_breaker_limit > 0 && recent_quarantined_conversations >= circuit_breaker_limit;
    // GH#510: deferred conversations are counted but leave `status` alone;
    // they are retried on a backoff and are not a quarantine.
    let deferred_conversations = WatchNomemDeferrals::load(data_dir).entries.len();
    ConversationIngestQuarantineSummary {
        schema_version: POISON_CONVERSATION_QUARANTINE_SCHEMA_VERSION,
        status: if circuit_breaker_active {
            "critical".to_string()
        } else if quarantined_conversations > 0 {
            "degraded".to_string()
        } else {
            "ok".to_string()
        },
        quarantined_conversations,
        deferred_conversations,
        recent_quarantined_conversations,
        recent_window_seconds,
        circuit_breaker_limit,
        circuit_breaker_active,
        quarantine_files,
        newest_last_attempt_at_ms,
        recommended_action: if circuit_breaker_active {
            Some(
                "Quarantine volume exceeded the recent circuit-breaker threshold; pause the watcher, inspect the listed quarantine file(s), then retry repaired source paths with `cass index --watch-once <path> --json --no-progress-events` before resuming watch."
                    .to_string(),
            )
        } else if quarantined_conversations > 0 {
            Some(
                "Run `cass quarantine list --json` to see which conversations are excluded and whether `cass quarantine retry` can re-ingest them."
                    .to_string(),
            )
        } else {
            (deferred_conversations > 0).then(|| {
                format!(
                    "{deferred_conversations} conversation(s) are deferred by the watcher after repeated bounded-allocation NoMem and are missing from search until they ingest; `cass quarantine list --json` lists them."
                )
            })
        },
    }
}

/// Build the set of quarantined conversation ids whose recorded source log is
/// gone (so a retry could never succeed). The poison JSONL records carry the
/// `source_path`; if a record has a path that no longer exists, that
/// conversation is source-missing.
pub(crate) fn quarantine_source_missing_ids(data_dir: &Path) -> BTreeSet<String> {
    let quarantine_dir = data_dir.join("quarantine");
    let mut missing = BTreeSet::new();
    for file_name in [WATCH_INGEST_POISON_FILE, INDEX_INGEST_POISON_FILE] {
        let path = quarantine_dir.join(file_name);
        let Ok(contents) = fs::read_to_string(&path) else {
            continue;
        };
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) else {
                continue;
            };
            let Some(conversation_id) = value
                .get("conversation_id")
                .and_then(serde_json::Value::as_str)
            else {
                continue;
            };
            if let Some(source_path) = value.get("source_path").and_then(serde_json::Value::as_str)
                && !Path::new(source_path).exists()
            {
                missing.insert(conversation_id.to_string());
            }
        }
    }
    missing
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::indexer) struct QuarantineRetrySource {
    pub(in crate::indexer) provider: String,
    pub(in crate::indexer) source_path: PathBuf,
    pub(in crate::indexer) external_id: Option<String>,
    pub(in crate::indexer) workspace: Option<PathBuf>,
    pub(in crate::indexer) started_at: Option<i64>,
    pub(in crate::indexer) ended_at: Option<i64>,
    pub(in crate::indexer) message_count: Option<usize>,
}

/// Join structured quarantine keys back to the exact source log and connector
/// recorded in the poison ledger. The ledger deliberately stores no message
/// body, so retry reparses the authoritative source instead of replaying a
/// stale or secret-bearing payload from quarantine metadata.
pub(in crate::indexer) fn quarantine_retry_sources(
    data_dir: &Path,
) -> BTreeMap<QuarantineKey, QuarantineRetrySource> {
    let quarantine_dir = data_dir.join("quarantine");
    let mut sources = BTreeMap::new();
    for file_name in [WATCH_INGEST_POISON_FILE, INDEX_INGEST_POISON_FILE] {
        let path = quarantine_dir.join(file_name);
        let Ok(contents) = fs::read_to_string(&path) else {
            continue;
        };
        for line in contents.lines() {
            let Ok(value) = serde_json::from_str::<serde_json::Value>(line.trim()) else {
                continue;
            };
            let Some((conversation_id, schema_version)) = poison_record_key_from_value(&value)
            else {
                continue;
            };
            let Ok(schema_version) = u32::try_from(schema_version) else {
                continue;
            };
            let provider = value
                .get("agent_slug")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|provider| !provider.is_empty())
                .or_else(|| conversation_id.split('|').next().map(str::trim))
                .map(str::to_string);
            let source_path = value
                .get("source_path")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|path| !path.is_empty());
            let (Some(provider), Some(source_path)) = (provider, source_path) else {
                continue;
            };
            sources.insert(
                QuarantineKey::new(conversation_id, schema_version),
                QuarantineRetrySource {
                    provider,
                    source_path: PathBuf::from(source_path),
                    external_id: value
                        .get("external_id")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string),
                    workspace: value
                        .get("workspace")
                        .and_then(serde_json::Value::as_str)
                        .map(PathBuf::from),
                    started_at: value.get("started_at").and_then(serde_json::Value::as_i64),
                    ended_at: value.get("ended_at").and_then(serde_json::Value::as_i64),
                    message_count: value
                        .get("message_count")
                        .and_then(serde_json::Value::as_u64)
                        .and_then(|count| usize::try_from(count).ok()),
                },
            );
        }
    }
    sources
}

pub(in crate::indexer) fn targeted_quarantine_retry(
    data_dir: &Path,
    key: &QuarantineKey,
    source: Option<&QuarantineRetrySource>,
) -> quarantine_retry::AttemptResult {
    match targeted_quarantine_retry_inner(data_dir, key, source) {
        Ok(result) => result,
        Err(error) if error_is_out_of_memory(&error) => {
            quarantine_retry::AttemptResult::OutOfMemory
        }
        Err(error) => quarantine_retry::AttemptResult::Failed(error.to_string()),
    }
}

pub(in crate::indexer) fn targeted_quarantine_retry_inner(
    data_dir: &Path,
    key: &QuarantineKey,
    source: Option<&QuarantineRetrySource>,
) -> Result<quarantine_retry::AttemptResult> {
    let source = source.context("quarantine retry source metadata is missing")?;
    if !source.source_path.is_file() {
        anyhow::bail!(
            "quarantine retry source is not a readable file: {}",
            source.source_path.display()
        );
    }
    let kind = ConnectorKind::from_slug(&source.provider).with_context(|| {
        format!(
            "quarantine retry does not recognize connector provider {}",
            source.provider
        )
    })?;
    let connector = kind.create_connector(Some(data_dir));
    let root = ScanRoot::local(source.source_path.clone());
    let context = crate::connectors::ScanContext::with_roots(
        source.source_path.clone(),
        vec![root.clone()],
        None,
    );
    let conversations = connector.scan(&context).with_context(|| {
        format!(
            "targeted quarantine retry could not parse {}",
            source.source_path.display()
        )
    })?;
    let mut candidates = conversations
        .into_iter()
        .filter(|conversation| {
            conversation.agent_slug == source.provider
                && conversation.source_path == source.source_path
                && source
                    .started_at
                    .is_none_or(|started_at| conversation.started_at == Some(started_at))
                && source
                    .ended_at
                    .is_none_or(|ended_at| conversation.ended_at == Some(ended_at))
                && source
                    .message_count
                    .is_none_or(|message_count| conversation.messages.len() == message_count)
        })
        .collect::<Vec<_>>();
    if candidates.len() > 1
        && let Some(expected_external_id) = source.external_id.as_deref()
    {
        candidates.retain(|conversation| {
            conversation.external_id.as_deref() == Some(expected_external_id)
        });
    }
    if candidates.len() != 1 {
        anyhow::bail!(
            "targeted quarantine retry expected exactly one matching conversation in {}, found {}",
            source.source_path.display(),
            candidates.len()
        );
    }
    let mut conversation = candidates
        .into_iter()
        .next()
        .context("targeted quarantine retry candidate disappeared")?;

    // The same file may have originally been discovered through a directory
    // root, while this retry intentionally scans only the exact source. Some
    // connectors derive external_id/workspace from the scan root, so restore
    // the canonical identity recorded at quarantine before preparing it again.
    conversation.external_id.clone_from(&source.external_id);
    conversation.workspace.clone_from(&source.workspace);

    let db_path = data_dir.join("agent_search.db");
    let storage = FrankenStorage::open(&db_path)
        .with_context(|| format!("opening quarantine retry archive {}", db_path.display()))?;
    storage.run_migrations()?;
    let raw_source_exclusions = RawSourceExclusions::load(&storage)?;
    let origin = Origin::local();
    prepare_conversation_for_ingest(
        data_dir,
        kind.slug(),
        &origin,
        Some(&root),
        &mut conversation,
        &raw_source_exclusions,
    );
    anyhow::ensure!(
        poison_conversation_id(&conversation) == key.conversation_id,
        "targeted quarantine retry identity changed after canonical preparation"
    );
    let conversations = [conversation];
    let index_path = index_dir(data_dir)?;
    let mut lexical = TantivyIndex::open_or_create(&index_path)?;
    let outcome = ingest_watch_batch_with_oom_split(
        &storage,
        &mut lexical,
        data_dir,
        &conversations,
        &None,
        true,
        false,
    )?;

    if outcome.quarantined_conversations > 0 {
        return Ok(quarantine_retry::AttemptResult::OutOfMemory);
    }
    if outcome.deferred_conversations > 0 {
        return Ok(quarantine_retry::AttemptResult::Failed(
            "targeted retry deferred after repeated out-of-memory without attributable host pressure"
                .to_string(),
        ));
    }
    if outcome.processed_conversations != 1 {
        return Ok(quarantine_retry::AttemptResult::Failed(format!(
            "targeted retry processed {} conversations instead of one",
            outcome.processed_conversations
        )));
    }
    if outcome.batch_outcome.lexical_update_deferred {
        return Ok(quarantine_retry::AttemptResult::Failed(
            outcome
                .batch_outcome
                .lexical_update_error
                .unwrap_or_else(|| "targeted retry deferred its lexical update".to_string()),
        ));
    }
    lexical.commit_with_region_doc_stamps(storage.raw())?;
    persist::with_ephemeral_writer(
        &storage,
        false,
        "updating targeted quarantine retry last_indexed_at",
        |writer| writer.set_last_indexed_at(FrankenStorage::now_millis()),
    )?;
    Ok(quarantine_retry::AttemptResult::Reindexed)
}

/// Read-only dry-run for the live quarantine retry command.
pub fn plan_quarantine_retry(
    data_dir: &Path,
    config: &quarantine_retry::RetryConfig,
) -> Result<quarantine_retry::RetryPlan> {
    let state = QuarantineState::load_for_operator(data_dir)
        .context("loading quarantine state for retry plan")?;
    let source_missing = quarantine_source_missing_ids(data_dir);
    Ok(quarantine_retry::plan_retry(
        &state,
        current_cass_version(),
        config,
        &source_missing,
    ))
}

/// Holds the authoritative data-dir maintenance lock while an operator
/// command mutates canonical storage or derived search assets.
///
/// The heartbeat is declared first so it stops before the underlying lock is
/// released. Otherwise its worker could race the lock guard's final metadata
/// cleanup during drop.
pub(crate) struct SearchMaintenanceMutationGuard {
    _heartbeat: IndexRunLockHeartbeat,
    lock: IndexRunLockGuard,
}

impl SearchMaintenanceMutationGuard {
    pub(in crate::indexer) fn new(data_dir: &Path, lock: IndexRunLockGuard) -> Self {
        let heartbeat = IndexRunLockHeartbeat::start(
            data_dir.to_path_buf(),
            index_run_lock_heartbeat_interval(),
            Arc::clone(&lock.metadata_write_lock),
            Arc::clone(&lock.last_progress_at_ms_atomic),
        );
        Self {
            _heartbeat: heartbeat,
            lock,
        }
    }

    pub(crate) fn mark_progress(&self) {
        bump_index_run_lock_progress_atomic(&self.lock.last_progress_at_ms_atomic);
    }

    pub(crate) fn progress_atomic(&self) -> Arc<AtomicI64> {
        Arc::clone(&self.lock.last_progress_at_ms_atomic)
    }
}

pub(crate) fn acquire_quarantine_mutation_lock(
    data_dir: &Path,
) -> Result<SearchMaintenanceMutationGuard> {
    let db_path = data_dir.join("agent_search.db");
    acquire_search_maintenance_mutation_lock(
        data_dir,
        &db_path,
        maintenance_job_kind_for_mode(SearchMaintenanceMode::Index),
    )
    .context("acquiring quarantine mutation lock")
}

pub(crate) fn acquire_semantic_backfill_lock(
    data_dir: &Path,
    db_path: &Path,
) -> Result<SearchMaintenanceMutationGuard> {
    acquire_search_maintenance_mutation_lock(
        data_dir,
        db_path,
        SearchMaintenanceJobKind::SemanticRebuild,
    )
}

/// Try to admit a daemon embedding job through the same writer ownership as
/// standalone indexing. A busy archive leaves the queued job intact; all
/// other failures retain their original error and context. The daemon is
/// Unix-only.
#[cfg(any(unix, test))]
pub(crate) fn try_acquire_semantic_backfill_lock(
    data_dir: &Path,
    db_path: &Path,
) -> Result<Option<SearchMaintenanceMutationGuard>> {
    match acquire_semantic_backfill_lock(data_dir, db_path) {
        Ok(guard) => Ok(Some(guard)),
        Err(error) if error.downcast_ref::<IndexRunBusy>().is_some() => Ok(None),
        Err(error) => Err(error),
    }
}

pub(crate) fn acquire_search_maintenance_mutation_lock(
    data_dir: &Path,
    db_path: &Path,
    job_kind: SearchMaintenanceJobKind,
) -> Result<SearchMaintenanceMutationGuard> {
    let lock = acquire_index_run_lock_with_job_kind(
        data_dir,
        db_path,
        SearchMaintenanceMode::Index,
        job_kind,
    )?;
    Ok(SearchMaintenanceMutationGuard::new(data_dir, lock))
}

/// Admit maintenance that only changes data-dir assets, such as HNSW output
/// or quarantine metadata. Callers must not open canonical storage through a
/// recovery-capable reader or a writer while holding only this guard. Keeping
/// this narrow path allows derived-asset work with an unwritable archive.
pub(crate) fn acquire_search_asset_mutation_lock(
    data_dir: &Path,
    db_path: &Path,
    job_kind: SearchMaintenanceJobKind,
) -> Result<SearchMaintenanceMutationGuard> {
    let lock = acquire_index_run_lock_with_archive_admission(
        data_dir,
        db_path,
        SearchMaintenanceMode::Index,
        job_kind,
        ArchiveAdmission::DataDirOnly,
    )?;
    Ok(SearchMaintenanceMutationGuard::new(data_dir, lock))
}

/// Operator-facing bounded quarantine retry (#292 ask #3, `cass quarantine
/// retry`). Wires the internal bounded retry classifier
/// ([`quarantine_retry::plan_retry`]) to a CLI action.
///
/// This is intentionally bounded: dry-run only classifies entries; `apply`
/// reparses and persists exactly the quarantined conversation named by each
/// eligible key. It never substitutes a broad corpus rebuild, never deletes a
/// source log, and never clears a quarantine record before that exact replay
/// has durably reached SQLite and the lexical index.
pub fn run_quarantine_retry(
    data_dir: &Path,
    config: &quarantine_retry::RetryConfig,
    apply: bool,
) -> Result<quarantine_retry::RetryReport> {
    let _mutation_guard = apply
        .then(|| acquire_quarantine_mutation_lock(data_dir))
        .transpose()?;
    let mut state = QuarantineState::load_for_operator(data_dir)
        .context("loading quarantine state for targeted retry")?;
    let current_version = current_cass_version();
    let source_missing = quarantine_source_missing_ids(data_dir);
    let now = chrono::Utc::now();

    // Plan first (always), so the report reflects the classification even on a
    // dry run.
    let plan = quarantine_retry::plan_retry(&state, current_version, config, &source_missing);

    if !apply {
        // Dry run: synthesize a report from the plan without mutating anything.
        return Ok(quarantine_retry_report_from_plan(&plan, current_version));
    }

    let retry_sources = quarantine_retry_sources(data_dir);

    let report = quarantine_retry::execute_retry(
        &mut state,
        current_version,
        config,
        &source_missing,
        now,
        |key| {
            if let Some(guard) = &_mutation_guard {
                guard.mark_progress();
            }
            let result = targeted_quarantine_retry(data_dir, key, retry_sources.get(key));
            if let Some(guard) = &_mutation_guard {
                guard.mark_progress();
            }
            result
        },
    );

    // Persist the structured state (the durable resume checkpoint) and clear the
    // matching poison JSONL records for cleared conversations.
    state
        .save(data_dir)
        .context("persisting quarantine state after targeted retry")?;
    let cleared_keys: BTreeSet<(String, i64)> = report
        .entries
        .iter()
        .filter(|entry| entry.outcome == quarantine_retry::RetryOutcome::RetriedCleared)
        .map(|entry| {
            (
                entry.conversation_id.clone(),
                i64::from(entry.schema_version),
            )
        })
        .collect();
    if !cleared_keys.is_empty() {
        for (file_name, reason) in [
            (WATCH_INGEST_POISON_FILE, "watch-ingest-out-of-memory"),
            (INDEX_INGEST_POISON_FILE, "index-ingest-out-of-memory"),
        ] {
            if let Err(err) =
                clear_poison_jsonl_key_records(data_dir, file_name, reason, &cleared_keys)
            {
                tracing::warn!(
                    data_dir = %data_dir.display(),
                    file = file_name,
                    error = %err,
                    "failed to clear poison JSONL records after quarantine retry"
                );
            }
        }
    }

    Ok(report)
}

/// Build a dry-run [`RetryReport`] from a [`RetryPlan`] without mutating state.
pub(in crate::indexer) fn quarantine_retry_report_from_plan(
    plan: &quarantine_retry::RetryPlan,
    current_version: &str,
) -> quarantine_retry::RetryReport {
    use quarantine_retry::{PlannedDisposition, RetryEntryResult, RetryOutcome};
    let entries = plan
        .entries
        .iter()
        .map(|entry| {
            let outcome = match entry.disposition {
                // On a dry run nothing is attempted; an eligible entry maps to
                // the action that *would* happen (cleared → re-attempted next
                // index).
                PlannedDisposition::Retry => RetryOutcome::RetriedCleared,
                PlannedDisposition::SkipIrreducible => RetryOutcome::SkippedIrreducible,
                PlannedDisposition::SkipSourceMissing => RetryOutcome::SkippedSourceMissing,
                PlannedDisposition::SkipBudgetExhausted => RetryOutcome::SkippedBudgetExhausted,
            };
            RetryEntryResult {
                conversation_id: entry.conversation_id.clone(),
                schema_version: entry.schema_version,
                outcome,
                attempt_count_before: entry.attempt_count,
                reason: entry.last_reason.clone(),
            }
        })
        .collect();
    quarantine_retry::RetryReport {
        current_version: current_version.to_string(),
        total_quarantined_before: plan.total_quarantined,
        attempted: 0,
        cleared: 0,
        re_quarantined_oom: 0,
        re_quarantined_failed: 0,
        skipped_source_missing: plan.skip_source_missing,
        skipped_irreducible: plan.skip_irreducible,
        skipped_budget_exhausted: plan.skip_budget_exhausted,
        remaining_quarantined: plan.total_quarantined,
        made_progress: false,
        stalled: false,
        resume_recommended: plan.resume_recommended,
        entries,
        summary: plan.summary.clone(),
        next_safe_command: plan.next_safe_command.clone(),
    }
}

pub(in crate::indexer) fn ingest_quarantine_circuit_window_seconds() -> i64 {
    dotenvy::var("CASS_INGEST_QUARANTINE_CIRCUIT_WINDOW_SECS")
        .ok()
        .and_then(|raw| raw.trim().parse::<i64>().ok())
        .filter(|seconds| *seconds > 0)
        .unwrap_or(INGEST_QUARANTINE_CIRCUIT_DEFAULT_WINDOW_SECONDS)
}

pub(in crate::indexer) fn ingest_quarantine_circuit_limit() -> usize {
    dotenvy::var("CASS_INGEST_QUARANTINE_CIRCUIT_LIMIT")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .unwrap_or(INGEST_QUARANTINE_CIRCUIT_DEFAULT_LIMIT)
}

#[cfg(test)]
pub(in crate::indexer) fn should_inject_watch_ingest_test_oom(
    convs: &[NormalizedConversation],
) -> bool {
    if WATCH_LEXICAL_TEST_FAULT.with(|fault| {
        matches!(fault.get(), Some(WatchLexicalTestFault::SplitAt(min)) if convs.len() >= min)
    }) {
        return true;
    }
    dotenvy::var("CASS_TEST_WATCH_INGEST_OOM_MIN_CONVS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|min| min > 0 && convs.len() >= min)
}

#[cfg(not(test))]
pub(in crate::indexer) fn should_inject_watch_ingest_test_oom(
    _convs: &[NormalizedConversation],
) -> bool {
    false
}

/// Force the #298 solo isolate-retry to also OOM, so tests can exercise the
/// genuine-quarantine path (a conversation that fails even in isolation).
#[cfg(test)]
pub(in crate::indexer) fn should_force_watch_solo_retry_oom() -> bool {
    dotenvy::var("CASS_TEST_WATCH_SOLO_RETRY_OOM")
        .map(|value| value == "1")
        .unwrap_or(false)
}

#[cfg(not(test))]
pub(in crate::indexer) fn should_force_watch_solo_retry_oom() -> bool {
    false
}

// Test-only ingest OOM injection. These are thread-local, not process env
// vars: libtest runs every test on its own thread while others run beside it,
// and `#[serial]` only orders tests that are themselves `#[serial]`. An env
// var set by one test injected OOMs into unrelated ingests running at the same
// time, which then inserted nothing (bead xicw9). Non-watch and incremental
// ingest run on the thread that called them, so a fault set here reaches
// exactly the calling test's ingest and nobody else's.
#[cfg(test)]
std::thread_local! {
    pub(in crate::indexer) static NON_WATCH_INGEST_TEST_OOM_MIN_CONVS: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
    pub(in crate::indexer) static INCREMENTAL_LEXICAL_UPDATE_TEST_OOM: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(in crate::indexer) fn should_inject_non_watch_ingest_test_oom(
    convs: &[NormalizedConversation],
) -> bool {
    NON_WATCH_INGEST_TEST_OOM_MIN_CONVS
        .with(std::cell::Cell::get)
        .is_some_and(|min| min > 0 && convs.len() >= min)
}

#[cfg(not(test))]
pub(in crate::indexer) fn should_inject_non_watch_ingest_test_oom(
    _convs: &[NormalizedConversation],
) -> bool {
    false
}
