use super::*;

/// Detect all active roots for watching/scanning.
///
/// Includes:
/// 1. Local roots detected by connectors
/// 2. Remote mirror roots (assigned to ALL connectors since we don't know the mapping)
pub(in crate::indexer) fn build_watch_roots(
    additional_scan_roots: Vec<ScanRoot>,
) -> Vec<(ConnectorKind, ScanRoot)> {
    let factories = configured_connector_factories();
    let mut roots = Vec::new();
    let mut all_kinds = Vec::new();

    for (name, factory) in factories {
        if let Some(kind) = ConnectorKind::from_slug(name) {
            all_kinds.push((name, kind));
            let conn = factory();
            let detection = conn.detect();
            if detection.detected {
                for root_path in detection.root_paths {
                    roots.push((kind, ScanRoot::local(root_path)));
                }
            }
        }
    }

    // Add explicitly configured roots for every connector that may own them.
    for configured_root in additional_scan_roots {
        for (name, kind) in &all_kinds {
            if mirrored_root_may_hold(name, &configured_root) {
                roots.push((*kind, configured_root.clone()));
            }
        }
    }

    coalesce_nested_watch_roots(roots)
}

/// txy4o: a connector's detection can return both a sessions directory and
/// its parent (OMP does). Every root is watched recursively and every event
/// is classified against every root, so one file change under the nested
/// pair used to trigger the parse/capture/persist/lexical work twice. Keep,
/// per (connector, origin, platform), only the shallowest root of each
/// nested chain — the ancestor's recursive watch already covers the
/// descendant, so coverage is unchanged — and drop exact duplicates.
/// Roots for different connectors, origins, or platforms never coalesce.
pub(in crate::indexer) fn coalesce_nested_watch_roots(
    roots: Vec<(ConnectorKind, ScanRoot)>,
) -> Vec<(ConnectorKind, ScanRoot)> {
    let same_family = |a: &(ConnectorKind, ScanRoot), b: &(ConnectorKind, ScanRoot)| {
        a.0.cmp_key_eq(&b.0)
            && a.1.origin.cmp_key_eq(&b.1.origin)
            && a.1.platform.cmp_key_eq(&b.1.platform)
    };
    let mut kept: Vec<(ConnectorKind, ScanRoot)> = Vec::with_capacity(roots.len());
    for candidate in roots {
        // Covered by an already-kept ancestor (or an exact duplicate)?
        if kept.iter().any(|existing| {
            same_family(existing, &candidate) && candidate.1.path.starts_with(&existing.1.path)
        }) {
            continue;
        }
        // The candidate is an ancestor of previously kept roots: it replaces
        // them so the shallowest root of the chain wins regardless of order.
        kept.retain(|existing| {
            !(same_family(existing, &candidate) && existing.1.path.starts_with(&candidate.1.path))
        });
        kept.push(candidate);
    }
    kept
}

/// Structural equality without `==` (kept `PartialEq`-based but explicit so
/// the coalescing key is obvious at the call site).
pub(in crate::indexer) trait CmpKeyEq {
    fn cmp_key_eq(&self, other: &Self) -> bool;
}

impl<T: PartialEq> CmpKeyEq for T {
    fn cmp_key_eq(&self, other: &Self) -> bool {
        self.eq(other)
    }
}

#[cfg(test)]
pub(in crate::indexer) mod watch_root_coalescing_tests {
    use super::{ConnectorKind, ScanRoot, coalesce_nested_watch_roots};
    use std::path::PathBuf;

    fn local(kind: ConnectorKind, path: &str) -> (ConnectorKind, ScanRoot) {
        (kind, ScanRoot::local(PathBuf::from(path)))
    }

    #[test]
    fn nested_roots_for_one_connector_collapse_to_the_shallowest() {
        let roots = vec![
            local(ConnectorKind::Codex, "/home/u/.codex/sessions"),
            local(ConnectorKind::Codex, "/home/u/.codex"),
            local(ConnectorKind::Codex, "/home/u/.codex/archived/sessions"),
            local(ConnectorKind::Codex, "/home/u/.codex"),
        ];
        let kept = coalesce_nested_watch_roots(roots);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].1.path, PathBuf::from("/home/u/.codex"));
    }

    #[test]
    fn order_independent_and_siblings_survive() {
        let kept = coalesce_nested_watch_roots(vec![
            local(ConnectorKind::Codex, "/a/b"),
            local(ConnectorKind::Codex, "/a/c"),
            local(ConnectorKind::Codex, "/a/b/deep"),
        ]);
        let paths: Vec<_> = kept.iter().map(|(_, r)| r.path.clone()).collect();
        assert_eq!(paths, vec![PathBuf::from("/a/b"), PathBuf::from("/a/c")]);
    }

    #[test]
    fn different_connectors_never_coalesce() {
        let kept = coalesce_nested_watch_roots(vec![
            local(ConnectorKind::Claude, "/shared"),
            local(ConnectorKind::Gemini, "/shared/sessions"),
            local(ConnectorKind::Codex, "/shared"),
        ]);
        assert_eq!(kept.len(), 3);
    }

    #[test]
    fn prefix_only_names_are_not_ancestors() {
        // `/a/b` is not an ancestor of `/a/bc`; Path::starts_with is
        // component-wise, so both must survive.
        let kept = coalesce_nested_watch_roots(vec![
            local(ConnectorKind::Codex, "/a/b"),
            local(ConnectorKind::Codex, "/a/bc"),
        ]);
        assert_eq!(kept.len(), 2);
    }
}

impl ConnectorKind {
    pub(in crate::indexer) fn from_slug(slug: &str) -> Option<Self> {
        match slug {
            "codex" => Some(Self::Codex),
            "gemini" => Some(Self::Gemini),
            "antigravity" => Some(Self::Antigravity),
            "claude" => Some(Self::Claude),
            "opencode" => Some(Self::OpenCode),
            "cursor" => Some(Self::Cursor),
            "chatgpt" => Some(Self::ChatGpt),
            "qwen" => Some(Self::Qwen),
            "devin" => Some(Self::Devin),
            "qoder" => Some(Self::Qoder),
            _ => None,
        }
    }

    pub(in crate::indexer) fn slug(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Gemini => "gemini",
            Self::Antigravity => "antigravity",
            Self::Claude => "claude",
            Self::OpenCode => "opencode",
            Self::Cursor => "cursor",
            Self::ChatGpt => "chatgpt",
            Self::Qwen => "qwen",
            Self::Devin => "devin",
            Self::Qoder => "qoder",
        }
    }

    /// Create a boxed connector instance for this kind.
    /// Centralizes connector instantiation to avoid duplicate match arms.
    pub(in crate::indexer) fn create_connector(
        &self,
        archive_data_dir: Option<&Path>,
    ) -> Box<dyn Connector + Send> {
        match self {
            Self::Codex => Box::new(match archive_data_dir {
                Some(data_dir) => CodexConnector::new().with_archive_data_dir(data_dir),
                None => CodexConnector::new(),
            }),
            Self::Gemini => Box::new(GeminiConnector::new()),
            Self::Antigravity => Box::new(AntigravityConnector::new()),
            Self::Claude => Box::new(ClaudeCodeConnector::new()),
            Self::OpenCode => Box::new(OpenCodeConnector::new()),
            Self::Cursor => Box::new(CursorConnector::new()),
            Self::ChatGpt => Box::new(ChatGptConnector::new()),
            Self::Qwen => Box::new(QwenConnector::new()),
            Self::Devin => Box::new(franken_agent_detection::DevinConnector::new()),
            Self::Qoder => Box::new(franken_agent_detection::QoderConnector::new()),
        }
    }
}

pub(in crate::indexer) fn watch_ingest_chunk_size() -> usize {
    match dotenvy::var("CASS_WATCH_INGEST_CHUNK_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    {
        Some(0) => WATCH_INGEST_DEFAULT_CHUNK_SIZE,
        Some(value) if value > WATCH_INGEST_CHUNK_SIZE_MAX => {
            tracing::warn!(
                env_var = "CASS_WATCH_INGEST_CHUNK_SIZE",
                requested = value,
                cap = WATCH_INGEST_CHUNK_SIZE_MAX,
                "watch ingest chunk size exceeds safe cap; clamping"
            );
            WATCH_INGEST_CHUNK_SIZE_MAX
        }
        Some(value) => value,
        None => WATCH_INGEST_DEFAULT_CHUNK_SIZE,
    }
}

pub(in crate::indexer) fn observe_index_raw_mirror_io(
    progress: Option<&Arc<IndexingProgress>>,
) -> Option<crate::raw_mirror::RawMirrorIoScope> {
    progress.map(|progress| {
        let progress_tick = Arc::clone(progress);
        let cancellation = Arc::clone(progress);
        crate::raw_mirror::observe_raw_mirror_io(
            Arc::new(move || progress_tick.tick_activity()),
            Arc::new(move || cancellation.stop_requested()),
        )
    })
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::indexer) struct WatchConversationSortKey {
    pub(in crate::indexer) watermark: Option<i64>,
    pub(in crate::indexer) source_path: PathBuf,
    pub(in crate::indexer) external_id: Option<String>,
}

pub(in crate::indexer) enum WatchConversationPayload {
    Memory(Box<NormalizedConversation>),
    Spilled { offset: u64, length: u64 },
}

pub(in crate::indexer) struct WatchConversationEntry {
    pub(in crate::indexer) key: WatchConversationSortKey,
    pub(in crate::indexer) retained_bytes: usize,
    pub(in crate::indexer) payload: WatchConversationPayload,
    pub(in crate::indexer) non_utf8_paths: Vec<WatchSpoolPath>,
}

pub(in crate::indexer) enum WatchSpoolPath {
    Source(PathBuf),
    Workspace(PathBuf),
    Snippet {
        message: usize,
        snippet: usize,
        path: PathBuf,
    },
}

impl WatchSpoolPath {
    pub(in crate::indexer) fn restore(&self, conversation: &mut NormalizedConversation) {
        match self {
            Self::Source(path) => conversation.source_path.clone_from(path),
            Self::Workspace(path) => conversation.workspace = Some(path.clone()),
            Self::Snippet {
                message,
                snippet,
                path,
            } => {
                conversation.messages[*message].snippets[*snippet].file_path = Some(path.clone());
            }
        }
    }
}

/// serde's PathBuf codec accepts UTF-8 only. Preserve the rare native paths
/// that cannot round-trip through JSON in the compact entry, replacing them
/// only for serialization and restoring them before returning to the caller.
pub(in crate::indexer) fn take_non_utf8_watch_paths(
    conversation: &mut NormalizedConversation,
) -> Vec<WatchSpoolPath> {
    let mut paths = Vec::new();
    if conversation.source_path.to_str().is_none() {
        paths.push(WatchSpoolPath::Source(std::mem::take(
            &mut conversation.source_path,
        )));
    }
    if let Some(path) = conversation.workspace.as_mut()
        && path.to_str().is_none()
    {
        paths.push(WatchSpoolPath::Workspace(std::mem::take(path)));
    }
    for (message, value) in conversation.messages.iter_mut().enumerate() {
        for (snippet, value) in value.snippets.iter_mut().enumerate() {
            if let Some(path) = value.file_path.as_mut()
                && path.to_str().is_none()
            {
                paths.push(WatchSpoolPath::Snippet {
                    message,
                    snippet,
                    path: std::mem::take(path),
                });
            }
        }
    }
    paths
}

/// Watch connectors enumerate in filesystem order. Keep the existing global
/// watermark/path/id sort without retaining a whole provider's transcripts in
/// RAM before the first durable batch (GH #538). Only compact sort keys remain
/// resident after the payload buffer fills. The anonymous file disappears on
/// close or process termination, so an interrupted scan has no recovery debris.
pub(in crate::indexer) struct WatchConversationSpool {
    pub(in crate::indexer) entries: Vec<WatchConversationEntry>,
    pub(in crate::indexer) file: Option<BufWriter<File>>,
    pub(in crate::indexer) data_dir: PathBuf,
    pub(in crate::indexer) memory_bytes: usize,
    pub(in crate::indexer) written_bytes: u64,
    pub(in crate::indexer) disk_budget: u64,
    pub(in crate::indexer) memory_limit: usize,
    pub(in crate::indexer) count_limit: usize,
}

impl WatchConversationSpool {
    pub(in crate::indexer) fn new(data_dir: &Path, count_limit: usize) -> Self {
        Self {
            entries: Vec::new(),
            file: None,
            data_dir: data_dir.to_path_buf(),
            memory_bytes: 0,
            written_bytes: 0,
            disk_budget: 0,
            memory_limit: DEFAULT_STREAMING_BATCH_LIMITS.max_chars,
            count_limit: count_limit.max(1),
        }
    }

    pub(in crate::indexer) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(in crate::indexer) fn push(
        &mut self,
        mut conversation: NormalizedConversation,
        progress: Option<&IndexingProgress>,
    ) -> Result<()> {
        let retained_bytes = conversation_batch_footprint(&conversation).retained_bytes;
        if self.file.is_none()
            && (self.entries.len() >= self.count_limit
                || self.memory_bytes.saturating_add(retained_bytes) > self.memory_limit)
        {
            self.file = Some(BufWriter::with_capacity(
                64 * 1024,
                tempfile::tempfile_in(&self.data_dir).context("create watch scan spool")?,
            ));
            for entry in &mut self.entries {
                let WatchConversationPayload::Memory(conversation) = &mut entry.payload else {
                    continue;
                };
                entry.payload = write_watch_spool_conversation(
                    self.file.as_mut().expect("watch spool is open"),
                    &self.data_dir,
                    &mut self.written_bytes,
                    &mut self.disk_budget,
                    conversation,
                    &mut entry.non_utf8_paths,
                    progress,
                )?;
            }
            self.memory_bytes = 0;
        }
        let key = WatchConversationSortKey {
            watermark: conversation_payload_watermark_ms(&conversation),
            source_path: conversation.source_path.clone(),
            external_id: conversation.external_id.clone(),
        };
        self.entries
            .try_reserve(1)
            .context("reserve watch scan sort entry")?;
        let mut non_utf8_paths = Vec::new();
        let payload = if let Some(file) = self.file.as_mut() {
            write_watch_spool_conversation(
                file,
                &self.data_dir,
                &mut self.written_bytes,
                &mut self.disk_budget,
                &mut conversation,
                &mut non_utf8_paths,
                progress,
            )?
        } else {
            self.memory_bytes = self.memory_bytes.saturating_add(retained_bytes);
            WatchConversationPayload::Memory(Box::new(conversation))
        };
        self.entries.push(WatchConversationEntry {
            key,
            retained_bytes,
            payload,
            non_utf8_paths,
        });
        if let Some(progress) = progress {
            progress.tick_activity();
        }
        Ok(())
    }

    pub(in crate::indexer) fn into_sorted_batches(mut self) -> Result<WatchConversationBatches> {
        self.entries.sort_by(|left, right| left.key.cmp(&right.key));
        let file = self
            .file
            .map(|writer| writer.into_inner().map_err(|error| error.into_error()))
            .transpose()
            .context("flush watch scan spool")?;
        Ok(WatchConversationBatches {
            entries: self.entries.into_iter().peekable(),
            file,
            memory_limit: self.memory_limit,
            count_limit: self.count_limit,
        })
    }
}

/// Streaming serialization avoids making a second, potentially huge JSON
/// allocation for a single conversation. Check free space and cancellation
/// every buffer-sized write; completed writes, never a timer, report liveness.
pub(in crate::indexer) struct WatchSpoolWriter<'a> {
    pub(in crate::indexer) writer: &'a mut BufWriter<File>,
    pub(in crate::indexer) data_dir: &'a Path,
    pub(in crate::indexer) written_bytes: &'a mut u64,
    pub(in crate::indexer) progress: Option<&'a IndexingProgress>,
    pub(in crate::indexer) remaining: &'a mut u64,
}

impl Write for WatchSpoolWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let bytes = &bytes[..bytes.len().min(64 * 1024)];
        if self.progress.is_some_and(IndexingProgress::stop_requested) {
            return Err(std::io::Error::other("watch scan spool interrupted"));
        }
        if *self.remaining < bytes.len() as u64 {
            self.writer.flush()?;
            let available = if index_disk_headroom_check_disabled() {
                u64::MAX
            } else {
                fs2::available_space(self.data_dir)?.saturating_sub(INDEX_MIN_FREE_SPACE_BYTES)
            };
            if available < bytes.len() as u64 {
                return Err(std::io::Error::other(
                    "watch scan spool would consume the index disk-space reserve",
                ));
            }
            // Re-probe while writing, including within one oversized source.
            *self.remaining = available.min(4 * 1024 * 1024);
            if let Some(progress) = self.progress {
                progress.tick_activity();
            }
        }
        let written = self.writer.write(bytes)?;
        *self.remaining -= written as u64;
        *self.written_bytes = self.written_bytes.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

pub(in crate::indexer) fn write_watch_spool_conversation(
    writer: &mut BufWriter<File>,
    data_dir: &Path,
    written_bytes: &mut u64,
    disk_budget: &mut u64,
    conversation: &mut NormalizedConversation,
    non_utf8_paths: &mut Vec<WatchSpoolPath>,
    progress: Option<&IndexingProgress>,
) -> Result<WatchConversationPayload> {
    let offset = *written_bytes;
    *non_utf8_paths = take_non_utf8_watch_paths(conversation);
    let result = serde_json::to_writer(
        WatchSpoolWriter {
            writer,
            data_dir,
            written_bytes,
            progress,
            remaining: disk_budget,
        },
        &*conversation,
    );
    for path in non_utf8_paths.iter() {
        path.restore(conversation);
    }
    if let Some(progress) = progress {
        progress.check_stop()?;
    }
    result.context("write watch scan spool")?;
    Ok(WatchConversationPayload::Spilled {
        offset,
        length: written_bytes.saturating_sub(offset),
    })
}

pub(in crate::indexer) struct WatchConversationBatches {
    pub(in crate::indexer) entries: std::iter::Peekable<std::vec::IntoIter<WatchConversationEntry>>,
    pub(in crate::indexer) file: Option<File>,
    pub(in crate::indexer) memory_limit: usize,
    pub(in crate::indexer) count_limit: usize,
}

pub(in crate::indexer) struct WatchSpoolReader<'a, R> {
    pub(in crate::indexer) reader: R,
    pub(in crate::indexer) progress: Option<&'a IndexingProgress>,
}

impl<R: Read> Read for WatchSpoolReader<'_, R> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        if self.progress.is_some_and(IndexingProgress::stop_requested) {
            return Err(std::io::Error::other("watch scan spool interrupted"));
        }
        let limit = bytes.len().min(64 * 1024);
        let read = self.reader.read(&mut bytes[..limit])?;
        if read > 0
            && let Some(progress) = self.progress
        {
            progress.tick_activity();
        }
        Ok(read)
    }
}

impl WatchConversationBatches {
    pub(in crate::indexer) fn next_batch(
        &mut self,
        progress: Option<&IndexingProgress>,
    ) -> Result<Vec<NormalizedConversation>> {
        let mut batch = Vec::new();
        let mut retained_bytes = 0usize;
        while let Some(next) = self.entries.peek() {
            if !batch.is_empty()
                && (batch.len() >= self.count_limit
                    || retained_bytes.saturating_add(next.retained_bytes) > self.memory_limit)
            {
                break;
            }
            if let Some(progress) = progress {
                progress.check_stop()?;
            }
            let entry = self.entries.next().expect("peeked watch conversation");
            let mut conversation = match entry.payload {
                WatchConversationPayload::Memory(conversation) => *conversation,
                WatchConversationPayload::Spilled { offset, length } => {
                    let file = self.file.as_mut().expect("spilled watch source has a file");
                    file.seek(SeekFrom::Start(offset))
                        .context("seek watch scan spool")?;
                    // Buffer outside the checked reader: serde consumes bytes
                    // individually, but cancellation/liveness belong to each
                    // completed read, including within an oversized payload.
                    let reader = WatchSpoolReader {
                        reader: file.take(length),
                        progress,
                    };
                    let result =
                        serde_json::from_reader(BufReader::with_capacity(64 * 1024, reader));
                    if let Some(progress) = progress {
                        progress.check_stop()?;
                    }
                    result.context("read watch scan spool")?
                }
            };
            for path in &entry.non_utf8_paths {
                path.restore(&mut conversation);
            }
            retained_bytes = retained_bytes
                .saturating_add(conversation_batch_footprint(&conversation).retained_bytes);
            batch.push(conversation);
            if let Some(progress) = progress {
                progress.tick_activity();
            }
        }
        Ok(batch)
    }
}

/// Keep deferred sources in the watch loop, including sources skipped during
/// startup. An active writer may close without producing another filesystem
/// event; the cooldown must therefore drive retries itself (GH455).
pub(in crate::indexer) fn dispatch_watch_callback<F>(
    pending: &mut BTreeSet<PathBuf>,
    roots: &[(ConnectorKind, ScanRoot)],
    is_rebuild: bool,
    callback: &F,
) -> bool
where
    F: Fn(Vec<PathBuf>, &[(ConnectorKind, ScanRoot)], bool) -> Result<BTreeSet<PathBuf>>,
{
    let paths = if is_rebuild {
        Vec::new()
    } else {
        pending.iter().cloned().collect()
    };
    match callback(paths, roots, is_rebuild) {
        Ok(deferred) => {
            *pending = deferred;
            true
        }
        Err(error) => {
            tracing::warn!(%error, is_rebuild, "watch callback failed; retaining sources for retry");
            if is_rebuild {
                pending.extend(roots.iter().map(|(_, root)| root.path.clone()));
            }
            false
        }
    }
}

// Both the notification-thread mailbox and the consumer's debounced pending
// set have these bounds. Once an event cannot fit, one full-root scan replaces
// the individual paths. BTreeSet overhead is bounded by the path count; the
// byte bound charges actual PathBuf capacities, including overallocated paths.
pub(in crate::indexer) const WATCH_PENDING_MAX_PATHS: usize = 4096;
pub(in crate::indexer) const WATCH_PENDING_MAX_PATH_BYTES: usize = 4 * 1024 * 1024;

#[derive(Default)]
pub(in crate::indexer) struct WatchEventBacklog {
    pub(in crate::indexer) paths: BTreeSet<PathBuf>,
    pub(in crate::indexer) path_bytes: usize,
}

impl WatchEventBacklog {
    pub(in crate::indexer) fn extend(&mut self, paths: impl IntoIterator<Item = PathBuf>) -> bool {
        for path in paths {
            if self.paths.contains(&path) {
                continue;
            }
            let path_bytes = self.path_bytes.saturating_add(path.capacity());
            if self.paths.len() >= WATCH_PENDING_MAX_PATHS
                || path_bytes > WATCH_PENDING_MAX_PATH_BYTES
            {
                self.clear();
                return false;
            }
            self.path_bytes = path_bytes;
            self.paths.insert(path);
        }
        true
    }

    pub(in crate::indexer) fn clear(&mut self) {
        self.paths.clear();
        self.path_bytes = 0;
    }
}

/// Notification producers share one path/count budget across the coalesced
/// backlog and its nonblocking contention handoff. The consumer never holds
/// the backlog lock while scanning, and producers never wait for that lock.
pub(in crate::indexer) struct WatchNotificationMailbox {
    pub(in crate::indexer) backlog: Mutex<WatchEventBacklog>,
    pub(in crate::indexer) deferred_tx: Sender<PathBuf>,
    pub(in crate::indexer) deferred_rx: Receiver<PathBuf>,
    // High 32 bits count paths; low 32 bits charge PathBuf capacities. Both
    // limits fit their halves, so one atomic reservation covers both queues.
    pub(in crate::indexer) retained: AtomicU64,
}

impl Default for WatchNotificationMailbox {
    fn default() -> Self {
        let (deferred_tx, deferred_rx) = bounded(WATCH_PENDING_MAX_PATHS);
        Self {
            backlog: Mutex::new(WatchEventBacklog::default()),
            deferred_tx,
            deferred_rx,
            retained: AtomicU64::new(0),
        }
    }
}

impl WatchNotificationMailbox {
    pub(in crate::indexer) const PATH_CHARGE: u64 = 1 << 32;

    pub(in crate::indexer) fn reserve_path(&self, path: &PathBuf) -> bool {
        let Ok(bytes) = u64::try_from(path.capacity()) else {
            return false;
        };
        if bytes > WATCH_PENDING_MAX_PATH_BYTES as u64 {
            return false;
        }
        self.retained
            .try_update(Ordering::AcqRel, Ordering::Acquire, |retained| {
                let paths = retained >> 32;
                let path_bytes = retained & u64::from(u32::MAX);
                (paths < WATCH_PENDING_MAX_PATHS as u64
                    && path_bytes + bytes <= WATCH_PENDING_MAX_PATH_BYTES as u64)
                    .then_some(retained + Self::PATH_CHARGE + bytes)
            })
            .is_ok()
    }

    pub(in crate::indexer) fn release_paths(&self, paths: usize, bytes: usize) {
        self.retained.fetch_sub(
            (paths as u64) * Self::PATH_CHARGE + bytes as u64,
            Ordering::AcqRel,
        );
    }
}

/// Preserve event paths even when a producer overlaps the consumer's brief
/// mailbox drain. Only actual event loss (kernel errors or a full budget)
/// requires a watermark-free full-root census.
pub(in crate::indexer) fn enqueue_watch_notification(
    result: notify::Result<notify::Event>,
    mailbox: &WatchNotificationMailbox,
    rescan: &AtomicBool,
    wake: &Sender<()>,
) {
    match result {
        Ok(event) if event.need_rescan() => rescan.store(true, Ordering::Release),
        Ok(event) => {
            if !watch_event_should_trigger_reindex(&event) || event.paths.is_empty() {
                return;
            }
            match mailbox.backlog.try_lock() {
                Ok(mut backlog) => {
                    for path in event.paths {
                        if backlog.paths.contains(&path) {
                            continue;
                        }
                        let previous_paths = backlog.paths.len();
                        let previous_bytes = backlog.path_bytes;
                        if !mailbox.reserve_path(&path) {
                            backlog.clear();
                            mailbox.release_paths(previous_paths, previous_bytes);
                            rescan.store(true, Ordering::Release);
                            break;
                        }
                        let path_bytes = path.capacity();
                        if !backlog.extend([path]) {
                            mailbox.release_paths(previous_paths + 1, previous_bytes + path_bytes);
                            rescan.store(true, Ordering::Release);
                            break;
                        }
                    }
                }
                Err(_) => {
                    for path in event.paths {
                        if !mailbox.reserve_path(&path) {
                            rescan.store(true, Ordering::Release);
                            break;
                        }
                        if let Err(error) = mailbox.deferred_tx.try_send(path) {
                            let path = error.into_inner();
                            mailbox.release_paths(1, path.capacity());
                            rescan.store(true, Ordering::Release);
                            break;
                        }
                    }
                }
            }
        }
        Err(error) => {
            tracing::warn!(%error, "filesystem watcher error; scheduling a full-root scan");
            rescan.store(true, Ordering::Release);
        }
    }
    // A full channel already contains the wakeup for this coalesced work.
    let _ = wake.try_send(());
}

pub(in crate::indexer) fn collect_watch_notifications(
    mailbox: &WatchNotificationMailbox,
    rescan: &AtomicBool,
    pending: &mut WatchEventBacklog,
    pending_rescan: &mut bool,
) {
    let received = {
        let mut backlog = mailbox
            .backlog
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        std::mem::take(&mut *backlog)
    };
    mailbox.release_paths(received.paths.len(), received.path_bytes);
    *pending_rescan |= rescan.swap(false, Ordering::AcqRel);
    if *pending_rescan {
        pending.clear();
    } else if !pending.extend(received.paths) {
        *pending_rescan = true;
    }
    // Drain only the observed bounded batch. A producer that keeps adding
    // events must not delay cancellation or control commands indefinitely.
    let deferred_count = mailbox.deferred_rx.len();
    for _ in 0..deferred_count {
        let Ok(path) = mailbox.deferred_rx.try_recv() else {
            break;
        };
        mailbox.release_paths(1, path.capacity());
        if !*pending_rescan && !pending.extend([path]) {
            *pending_rescan = true;
        }
    }
}

pub(in crate::indexer) fn dispatch_pending_watch_callback<F>(
    pending: &mut WatchEventBacklog,
    pending_rescan: &mut bool,
    roots: &[(ConnectorKind, ScanRoot)],
    callback: &F,
) where
    F: Fn(Vec<PathBuf>, &[(ConnectorKind, ScanRoot)], bool) -> Result<BTreeSet<PathBuf>>,
{
    let is_rebuild = std::mem::take(pending_rescan);
    let mut paths = std::mem::take(&mut pending.paths);
    pending.path_bytes = 0;
    let succeeded = dispatch_watch_callback(&mut paths, roots, is_rebuild, callback);
    // A failed full-root scan cannot become a timestamp-filtered retry: the
    // lost notification may describe a source older than the directory mtime.
    if (is_rebuild && !succeeded) || !pending.extend(paths) {
        *pending_rescan = true;
    }
}

pub(in crate::indexer) enum WatchLoopEvent {
    Control(IndexerEvent),
    Filesystem,
    Timeout,
    Disconnected,
}

pub(in crate::indexer) fn next_watch_loop_event(
    controls: &Receiver<IndexerEvent>,
    notifications: &Receiver<()>,
    timeout: Duration,
) -> WatchLoopEvent {
    // Control traffic has its own channel and always wins over filesystem
    // notifications. A busy filesystem cannot crowd out operator commands.
    crossbeam_channel::select_biased! {
        recv(controls) -> event => match event {
            Ok(event) => WatchLoopEvent::Control(event),
            Err(_) => WatchLoopEvent::Disconnected,
        },
        recv(notifications) -> event => match event {
            Ok(()) => WatchLoopEvent::Filesystem,
            Err(_) => WatchLoopEvent::Disconnected,
        },
        default(timeout) => WatchLoopEvent::Timeout,
    }
}

pub(in crate::indexer) fn wait_watch_loop_event(
    controls: &Receiver<IndexerEvent>,
    notifications: &Receiver<()>,
    timeout: Duration,
    progress: Option<&Arc<IndexingProgress>>,
) -> Result<WatchLoopEvent> {
    let _idle_scope = progress.map(|progress| {
        progress.watch_idle.store(true, Ordering::Release);
        WatchIdleWaitScope(progress.as_ref())
    });
    let started = Instant::now();
    loop {
        if let Some(progress) = progress {
            progress.check_stop()?;
        }
        let remaining = timeout.saturating_sub(started.elapsed());
        let poll = if progress.is_some() {
            remaining.min(Duration::from_secs(1))
        } else {
            remaining
        };
        match next_watch_loop_event(controls, notifications, poll) {
            WatchLoopEvent::Timeout if started.elapsed() < timeout => continue,
            event => return Ok(event),
        }
    }
}

pub(in crate::indexer) struct WatchIdleWaitScope<'a>(&'a IndexingProgress);

impl Drop for WatchIdleWaitScope<'_> {
    fn drop(&mut self) {
        // Events, cancellation, errors and unwinding all re-arm the watchdog
        // before any classification, source I/O or persistence can begin.
        self.0.watch_idle.store(false, Ordering::Release);
    }
}
