//! Native and legacy OpenClaw history, parsed by franken-agent-detection.
//!
//! The adapter forwards the upstream connector's streaming and per-source
//! boundary capabilities and adds what upstream does not provide: the
//! OPENCLAW_STATE_DIR override, and recovery of WAL-only commits, which FAD's
//! SQLite mtime prefilter still skips at this pin because the primary file is
//! untouched while its `-wal` grows. Recovered stores are delivered under the
//! caller's hooks — admission before the re-read, completion naming the
//! committed `-wal` — and both them and the legacy migration copies of the
//! sessions they cover are suppressed from the delegated traversal. Full scans
//! delegate unchanged: FAD's own traversal reads committed WALs and suppresses
//! covered legacy copies by itself. Do not fork its schema admission or event
//! parser.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::Result;
use franken_agent_detection::connectors::file_modified_since;
use franken_agent_detection::{
    Connector, DetectionResult, DiscoveredSourceFile, DiscoveredSourceRole, NormalizedConversation,
    ScanContext, ScanRoot, SourceCompletion, SourceScanHooks,
};

#[derive(Default)]
pub struct OpenClawConnector;

impl OpenClawConnector {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    fn wal_only_sources(ctx: &ScanContext) -> Result<Vec<DiscoveredSourceFile>> {
        if ctx.since_ts.is_none() {
            return Ok(Vec::new());
        }
        let mut full = ctx.clone();
        full.since_ts = None;
        Ok(franken_agent_detection::OpenClawConnector::new()
            .discover_source_files(&full)?
            .into_iter()
            .filter(|source| {
                let wal = wal_path(&source.source_path);
                source.role == DiscoveredSourceRole::SqliteDatabase
                    && !file_modified_since(&source.source_path, ctx.since_ts)
                    && wal.is_file()
                    && file_modified_since(&wal, ctx.since_ts)
            })
            .collect())
    }

    /// Re-scan the stores FAD's mtime prefilter skips because their latest
    /// commit lives only in the `-wal`, and deliver them under the caller's
    /// hooks: admission before the re-read, completion naming the `-wal`
    /// once every conversation of the store has been delivered. The returned
    /// recovery names the re-read stores and the sessions they cover so the
    /// delegated traversal can suppress their redundant sources.
    fn recover_wal_only_sources(
        ctx: &ScanContext,
        since: i64,
        hooks: &mut SourceScanHooks<'_>,
        on_conversation: &mut dyn FnMut(NormalizedConversation) -> Result<()>,
    ) -> Result<CutoffRecovery> {
        let inner = franken_agent_detection::OpenClawConnector::new();
        let mut recovery = CutoffRecovery::default();
        for pending in Self::wal_only_sources(ctx)? {
            // Admission precedes any read; a denied source is neither re-read
            // nor completed, and without its session coverage its legacy
            // copies are not suppressed either.
            let source = pending.with_fs_metadata();
            if !hooks.should_scan(&source) {
                continue;
            }
            let wal = wal_sidecar_of(&source);
            let mut scoped = ctx.clone();
            scoped.since_ts = None;
            // Only the selected database is reopened, never adjacent stores.
            let root = ctx
                .scan_roots
                .iter()
                .find(|root| source.source_path.starts_with(&root.path))
                .cloned()
                .unwrap_or_else(|| ScanRoot::local(source.source_path.clone()));
            scoped.scan_roots = vec![root.with_path(source.source_path.clone())];
            let native = inner.scan(&scoped)?;
            if native.is_empty() {
                // An unreadable/unrecognized store must not suppress legacy
                // fallback or any results already returned by the dependency.
                continue;
            }
            let covered = native
                .iter()
                .filter_map(|conversation| {
                    conversation
                        .metadata
                        .get("session_id")
                        .and_then(|value| value.as_str())
                        .map(str::to_string)
                })
                .collect::<HashSet<_>>();
            recovery.stores.insert(source.source_path.clone());
            if let Some(root) = legacy_sessions_root(&source.source_path) {
                recovery
                    .legacy_sessions
                    .entry(root)
                    .or_default()
                    .extend(covered);
            }
            let mut emitted = 0usize;
            for conversation in native {
                // Match FAD's session-level cutoff and clock-skew allowance.
                // Keep every event of a selected session, not only new messages.
                let recent = conversation
                    .metadata
                    .get("last_event_at")
                    .and_then(|value| value.as_i64())
                    .is_none_or(|last| last >= since.saturating_sub(1_000));
                if !recent {
                    continue;
                }
                on_conversation(conversation)?;
                emitted += 1;
            }
            let changed = source.fs_metadata_changed()
                || wal
                    .as_ref()
                    .is_some_and(DiscoveredSourceFile::fs_metadata_changed);
            if emitted > 0 && !changed {
                hooks.complete(&SourceCompletion {
                    source,
                    required_sidecars: wal.into_iter().collect(),
                    conversations_emitted: emitted,
                })?;
            }
        }
        Ok(recovery)
    }
}

// OpenClaw documents OPENCLAW_STATE_DIR as the mutable-state override.
// Resolve it once per operation, and never append the real profile as fallback.
fn state_override() -> Option<PathBuf> {
    let value = dotenvy::var("OPENCLAW_STATE_DIR").ok()?;
    state_path(&value, dirs::home_dir().as_deref())
}

fn state_path(value: &str, home: Option<&Path>) -> Option<PathBuf> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    let path = PathBuf::from(value);
    match (path.strip_prefix("~"), home) {
        (Ok(tail), Some(home)) => Some(home.join(tail)),
        _ => Some(path),
    }
}

fn selected_context(ctx: &ScanContext) -> Cow<'_, ScanContext> {
    if !ctx.use_default_detection() {
        return Cow::Borrowed(ctx);
    }
    let Some(state) = state_override() else {
        return Cow::Borrowed(ctx);
    };
    let mut selected = ctx.clone();
    selected.scan_roots = vec![ScanRoot::local(state.join("agents"))];
    Cow::Owned(selected)
}

/// Final registry adapter shared by the application and focused consumer gate.
pub(super) fn with_wal_freshness(
    name: &str,
    factory: fn() -> Box<dyn Connector + Send>,
) -> fn() -> Box<dyn Connector + Send> {
    if name == "openclaw" {
        || Box::new(OpenClawConnector::new())
    } else {
        factory
    }
}

fn wal_path(database: &Path) -> PathBuf {
    let mut path = database.as_os_str().to_owned();
    path.push("-wal");
    PathBuf::from(path)
}

/// The store's committed `-wal`, declared exactly like discovery declares it:
/// a required reconstruction sidecar whose fingerprint authorizes reuse.
fn wal_sidecar_of(source: &DiscoveredSourceFile) -> Option<DiscoveredSourceFile> {
    let wal = wal_path(&source.source_path);
    wal.is_file().then(|| {
        DiscoveredSourceFile {
            source_path: wal,
            role: DiscoveredSourceRole::MetadataSidecar,
            required_for_reconstruction: true,
            ..source.clone()
        }
        .with_fs_metadata()
    })
}

/// `<agent>/sessions` next to a `<agent>/agent/openclaw-agent.sqlite` store,
/// where OpenClaw's pre-SQLite migration copies live.
fn legacy_sessions_root(database: &Path) -> Option<PathBuf> {
    database
        .parent()
        .filter(|parent| parent.file_name().is_some_and(|name| name == "agent"))
        .and_then(Path::parent)
        .map(|agent| agent.join("sessions"))
}

/// WAL-only commits recovered before the delegated traversal, and the
/// sources whose re-parse they make redundant.
#[derive(Default)]
struct CutoffRecovery {
    /// Stores re-scanned by the recovery; the upstream traversal must not
    /// read them again even if a writer makes them look fresh mid-scan.
    stores: HashSet<PathBuf>,
    /// Sessions root -> native session ids covered by the recovered stores.
    /// Their leftover migration copies stay out of the index.
    legacy_sessions: HashMap<PathBuf, HashSet<String>>,
}

impl CutoffRecovery {
    fn is_empty(&self) -> bool {
        self.stores.is_empty()
    }

    /// Whether the delegated traversal should skip a discovered source.
    fn suppresses(&self, source: &DiscoveredSourceFile) -> bool {
        self.stores.contains(&source.source_path)
            || source
                .source_path
                .parent()
                .and_then(|parent| {
                    self.legacy_sessions.get(parent).and_then(|covered| {
                        source
                            .source_path
                            .file_stem()
                            .and_then(|stem| stem.to_str())
                            .map(|stem| covered.contains(stem))
                    })
                })
                .unwrap_or(false)
    }
}

impl Connector for OpenClawConnector {
    fn detect(&self) -> DetectionResult {
        if let Some(state) = state_override() {
            let agents = state.join("agents");
            return if agents.is_dir() {
                DetectionResult {
                    detected: true,
                    evidence: vec![format!("OpenClaw state override: {}", state.display())],
                    root_paths: vec![agents],
                }
            } else {
                DetectionResult::not_found()
            };
        }
        franken_agent_detection::OpenClawConnector::new().detect()
    }

    fn discover_source_files(&self, ctx: &ScanContext) -> Result<Vec<DiscoveredSourceFile>> {
        let selected = selected_context(ctx);
        let ctx = selected.as_ref();
        let inner = franken_agent_detection::OpenClawConnector::new();
        let mut sources = inner.discover_source_files(ctx)?;
        let mut paths: HashSet<_> = sources.iter().map(|s| s.source_path.clone()).collect();
        for source in Self::wal_only_sources(ctx)? {
            if paths.insert(source.source_path.clone()) {
                sources.push(source);
            }
        }
        // The committed WAL is part of the source, not merely a cache. Declare
        // it for pre-mirroring even on full scans; never treat SHM as history.
        let mut sidecars = Vec::new();
        for source in &sources {
            if source.role == DiscoveredSourceRole::SqliteDatabase
                && let Some(wal) = wal_sidecar_of(source)
                && paths.insert(wal.source_path.clone())
            {
                sidecars.push(wal);
            }
        }
        sources.extend(sidecars);
        Ok(sources)
    }

    fn scan(&self, ctx: &ScanContext) -> Result<Vec<NormalizedConversation>> {
        let mut conversations = Vec::new();
        self.scan_with_callback(ctx, &mut |conversation| {
            conversations.push(conversation);
            Ok(())
        })?;
        Ok(conversations)
    }

    fn supports_streaming_scan(&self) -> bool {
        franken_agent_detection::OpenClawConnector::new().supports_streaming_scan()
    }

    fn scan_with_callback(
        &self,
        ctx: &ScanContext,
        on_conversation: &mut dyn FnMut(NormalizedConversation) -> Result<()>,
    ) -> Result<()> {
        self.scan_with_source_boundaries(ctx, &mut SourceScanHooks::default(), on_conversation)
    }

    fn supports_source_boundaries(&self) -> bool {
        franken_agent_detection::OpenClawConnector::new().supports_source_boundaries()
    }

    fn scan_with_source_boundaries(
        &self,
        ctx: &ScanContext,
        hooks: &mut SourceScanHooks<'_>,
        on_conversation: &mut dyn FnMut(NormalizedConversation) -> Result<()>,
    ) -> Result<()> {
        let selected = selected_context(ctx);
        let ctx = selected.as_ref();
        let Some(since) = ctx.since_ts else {
            // Full scans see committed WALs through FAD's own traversal, which
            // also suppresses the legacy copies of every covered session; the
            // adapter adds nothing beyond the state-directory selection.
            return franken_agent_detection::OpenClawConnector::new().scan_with_source_boundaries(
                ctx,
                hooks,
                on_conversation,
            );
        };
        let recovery = Self::recover_wal_only_sources(ctx, since, hooks, on_conversation)?;
        if recovery.is_empty() {
            return franken_agent_detection::OpenClawConnector::new().scan_with_source_boundaries(
                ctx,
                hooks,
                on_conversation,
            );
        }
        let SourceScanHooks {
            should_scan_source,
            on_source_complete,
        } = hooks;
        let mut before = |source: &DiscoveredSourceFile| {
            !recovery.suppresses(source)
                && should_scan_source
                    .as_mut()
                    .is_none_or(|predicate| predicate(source))
        };
        let mut complete = |completion: &SourceCompletion| {
            on_source_complete
                .as_mut()
                .map_or(Ok(()), |sink| sink(completion))
        };
        franken_agent_detection::OpenClawConnector::new().scan_with_source_boundaries(
            ctx,
            &mut SourceScanHooks {
                should_scan_source: Some(&mut before),
                on_source_complete: Some(&mut complete),
            },
            on_conversation,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::franken_sync::Connection;
    use crate::franken_sync::compat::{ConnectionExt, ParamValue};
    use serde_json::json;
    use std::fs;
    use std::time::{Duration, UNIX_EPOCH};

    const OLD: i64 = 1_780_000_000_000;
    const NEW: i64 = OLD + 60_000;

    fn insert(conn: &Connection, id: &str, seq: i64, at: i64, text: &str) -> Result<()> {
        let body = json!({"type":"message", "message":{"role":"user", "content":text}}).to_string();
        conn.execute_compat(
            "INSERT INTO transcript_events VALUES (?1, ?2, ?3, ?4)",
            &[
                ParamValue::from(id),
                ParamValue::from(seq),
                ParamValue::from(body.as_str()),
                ParamValue::from(at),
            ],
        )?;
        Ok(())
    }

    /// A native OpenClaw store whose latest commit lives only in its WAL:
    /// the primary file's mtime predates the cutoff, so FAD's prefilter
    /// skips the store, while its legacy `sessions/` copies stay fresh.
    struct WalOnlyStore {
        temp: tempfile::TempDir,
        database: PathBuf,
        cutoff: i64,
    }

    impl WalOnlyStore {
        fn sessions_dir(&self) -> PathBuf {
            self.database
                .parent()
                .and_then(Path::parent)
                .expect("fixture store lives under <agent>/agent")
                .join("sessions")
        }

        fn session_file(&self, name: &str) -> PathBuf {
            self.sessions_dir().join(format!("{name}.jsonl"))
        }

        fn wal(&self) -> PathBuf {
            wal_path(&self.database)
        }

        fn scan_context(&self) -> ScanContext {
            ScanContext::with_roots(
                self.temp.path().join("cass"),
                vec![ScanRoot::local(self.temp.path().to_path_buf())],
                Some(self.cutoff),
            )
        }

        fn snapshot(
            &self,
        ) -> Result<(
            Vec<u8>,
            Vec<u8>,
            std::time::SystemTime,
            std::time::SystemTime,
        )> {
            Ok((
                fs::read(&self.database)?,
                fs::read(self.wal())?,
                fs::metadata(&self.database)?.modified()?,
                fs::metadata(self.wal())?.modified()?,
            ))
        }
    }

    fn wal_only_store_fixture() -> Result<WalOnlyStore> {
        let temp = tempfile::tempdir()?;
        let agent = temp.path().join(".openclaw/agents/main");
        fs::create_dir_all(agent.join("agent"))?;
        fs::create_dir_all(agent.join("sessions"))?;
        let database = agent.join("agent/openclaw-agent.sqlite");
        let conn = Connection::open(database.to_string_lossy().into_owned())?;
        conn.execute("PRAGMA journal_mode = DELETE")?;
        conn.execute("CREATE TABLE transcript_events (session_id TEXT, seq INTEGER, event_json TEXT, created_at INTEGER)")?;
        insert(&conn, "fresh", 0, OLD, "original prefix")?;
        insert(&conn, "stale", 0, OLD, "native stale history")?;
        conn.close()?;
        let writer = Connection::open(database.to_string_lossy().into_owned())?;
        writer.execute("PRAGMA journal_mode = WAL")?;
        insert(&writer, "fresh", 1, NEW, "WAL append")?;
        writer.close_without_checkpoint()?;
        fs::File::options()
            .write(true)
            .open(&database)?
            .set_modified(UNIX_EPOCH + Duration::from_millis(OLD as u64))?;
        let store = WalOnlyStore {
            temp,
            database,
            cutoff: NEW - 1_000,
        };
        assert!(
            fs::metadata(store.wal())?.len() > 32,
            "fixture must commit the append to the WAL"
        );
        assert!(
            !file_modified_since(&store.database, Some(store.cutoff)),
            "fixture must exercise the stale database gate"
        );
        for name in ["fresh", "stale", "legacy-only"] {
            let body = json!({"type":"message", "message":{"role":"user", "content":format!("legacy {name}")}});
            fs::write(store.session_file(name), format!("{body}\n"))?;
        }
        Ok(store)
    }

    #[test]
    fn incremental_wal_updates_preserve_prefixes_and_suppress_stale_migration_copies() -> Result<()>
    {
        let store = wal_only_store_fixture()?;
        let before = store.snapshot()?;
        let ctx = store.scan_context();
        let (_, factory) = crate::connectors::get_connector_factories()
            .into_iter()
            .find(|(name, _)| *name == "openclaw")
            .unwrap();
        let connector = factory();
        let sources = connector.discover_source_files(&ctx)?;
        assert!(
            sources.iter().any(|s| s.source_path == store.database
                && s.role == DiscoveredSourceRole::SqliteDatabase)
        );
        assert!(sources.iter().any(|s| s.source_path == store.wal()
            && s.role == DiscoveredSourceRole::MetadataSidecar
            && s.required_for_reconstruction));
        let conversations = connector.scan(&ctx)?;
        assert_eq!(conversations.len(), 2);
        let fresh = conversations
            .iter()
            .find(|c| c.external_id.as_deref() == Some("main/fresh"))
            .unwrap();
        assert_eq!(fresh.source_path, store.database);
        assert_eq!(fresh.messages.len(), 2);
        assert_eq!(fresh.messages[0].content, "original prefix");
        assert_eq!(fresh.messages[1].content, "WAL append");
        assert!(
            conversations
                .iter()
                .any(|c| c.external_id.as_deref() == Some("main/legacy-only"))
        );
        assert_eq!(store.snapshot()?, before);
        Ok(())
    }

    #[test]
    fn registry_adapter_forwards_upstream_scan_capabilities() {
        let adapter = OpenClawConnector::new();
        let upstream = franken_agent_detection::OpenClawConnector::new();
        assert_eq!(
            adapter.supports_streaming_scan(),
            upstream.supports_streaming_scan()
        );
        assert_eq!(
            adapter.supports_source_boundaries(),
            upstream.supports_source_boundaries()
        );
        // At the pinned FAD rev both hold: openclaw streams its traversal and
        // owns trustworthy per-source lifecycle events. Indexer lanes branch
        // on these flags, so the adapter must not silently drop them.
        assert!(adapter.supports_streaming_scan());
        assert!(adapter.supports_source_boundaries());
    }

    #[test]
    fn streaming_callback_scan_recovers_wal_only_commits_like_the_materialized_scan() -> Result<()>
    {
        let store = wal_only_store_fixture()?;
        let before = store.snapshot()?;
        let connector = OpenClawConnector::new();
        let mut conversations = Vec::new();
        connector.scan_with_callback(&store.scan_context(), &mut |conversation| {
            conversations.push(conversation);
            Ok(())
        })?;
        assert_eq!(conversations.len(), 2);
        let fresh = conversations
            .iter()
            .find(|c| c.external_id.as_deref() == Some("main/fresh"))
            .unwrap();
        assert_eq!(fresh.source_path, store.database);
        assert_eq!(fresh.messages.len(), 2);
        assert_eq!(fresh.messages[0].content, "original prefix");
        assert_eq!(fresh.messages[1].content, "WAL append");
        assert!(
            conversations
                .iter()
                .any(|c| c.external_id.as_deref() == Some("main/legacy-only"))
        );
        assert_eq!(store.snapshot()?, before);
        Ok(())
    }

    #[test]
    fn boundary_events_cover_recovered_wal_only_stores() -> Result<()> {
        let store = wal_only_store_fixture()?;
        let before = store.snapshot()?;
        let connector = OpenClawConnector::new();
        let admitted_paths = std::cell::RefCell::new(Vec::<PathBuf>::new());
        let completed = std::cell::RefCell::new(Vec::new());
        let mut should_scan = |source: &DiscoveredSourceFile| {
            admitted_paths.borrow_mut().push(source.source_path.clone());
            true
        };
        let mut complete = |completion: &SourceCompletion| {
            completed.borrow_mut().push((
                completion.source.source_path.clone(),
                completion.source.role,
                completion
                    .required_sidecars
                    .iter()
                    .map(|sidecar| {
                        (
                            sidecar.source_path.clone(),
                            sidecar.role,
                            sidecar.required_for_reconstruction,
                        )
                    })
                    .collect::<Vec<_>>(),
                completion.conversations_emitted,
            ));
            Ok(())
        };
        let mut conversations = Vec::new();
        connector.scan_with_source_boundaries(
            &store.scan_context(),
            &mut SourceScanHooks {
                should_scan_source: Some(&mut should_scan),
                on_source_complete: Some(&mut complete),
            },
            &mut |conversation| {
                conversations.push(conversation);
                Ok(())
            },
        )?;
        // The same recovery the materialized scan pins: the WAL-only commit
        // is visible, and only the uncovered legacy copy survives.
        assert_eq!(conversations.len(), 2);
        let fresh = conversations
            .iter()
            .find(|c| c.external_id.as_deref() == Some("main/fresh"))
            .unwrap();
        assert_eq!(fresh.source_path, store.database);
        assert_eq!(fresh.messages.len(), 2);
        assert_eq!(fresh.messages[1].content, "WAL append");
        assert!(
            conversations
                .iter()
                .any(|c| c.external_id.as_deref() == Some("main/legacy-only"))
        );
        // The recovered store is admitted before its re-scan and completes
        // with its committed -wal as the reuse evidence a WAL-only commit
        // leaves behind — the primary file's observation cannot see it.
        let completions = completed.borrow();
        let store_completion = completions
            .iter()
            .find(|(path, ..)| *path == store.database)
            .expect("recovered store must complete");
        assert_eq!(
            store_completion.2,
            vec![(store.wal(), DiscoveredSourceRole::MetadataSidecar, true)]
        );
        assert_eq!(store_completion.3, 1);
        // The legacy copies of recovered sessions never reach admission; the
        // uncovered legacy copy is admitted and completes without sidecars.
        let admitted = admitted_paths.borrow();
        assert!(!admitted.contains(&store.session_file("fresh")));
        assert!(!admitted.contains(&store.session_file("stale")));
        assert!(admitted.contains(&store.session_file("legacy-only")));
        let legacy_completion = completions
            .iter()
            .find(|(path, ..)| *path == store.session_file("legacy-only"))
            .expect("uncovered legacy copy must complete");
        assert!(legacy_completion.2.is_empty());
        assert_eq!(legacy_completion.3, 1);
        assert_eq!(completions.len(), 2);
        assert_eq!(store.snapshot()?, before);
        Ok(())
    }

    #[test]
    fn full_scan_boundaries_delegate_to_the_upstream_traversal() -> Result<()> {
        let store = wal_only_store_fixture()?;
        let mut ctx = store.scan_context();
        ctx.since_ts = None;
        let connector = OpenClawConnector::new();
        let admitted_paths = std::cell::RefCell::new(Vec::<PathBuf>::new());
        let completed = std::cell::RefCell::new(Vec::new());
        let mut should_scan = |source: &DiscoveredSourceFile| {
            admitted_paths.borrow_mut().push(source.source_path.clone());
            true
        };
        let mut complete = |completion: &SourceCompletion| {
            completed.borrow_mut().push((
                completion.source.source_path.clone(),
                completion.conversations_emitted,
                completion
                    .required_sidecars
                    .iter()
                    .map(|sidecar| sidecar.source_path.clone())
                    .collect::<Vec<_>>(),
            ));
            Ok(())
        };
        let mut conversations = Vec::new();
        connector.scan_with_source_boundaries(
            &ctx,
            &mut SourceScanHooks {
                should_scan_source: Some(&mut should_scan),
                on_source_complete: Some(&mut complete),
            },
            &mut |conversation| {
                conversations.push(conversation);
                Ok(())
            },
        )?;
        // Without a cutoff every store passes the upstream mtime
        // prefilter, so FAD's own traversal scans the store, sees the
        // committed WAL, and keeps its legacy-copy suppression: the adapter
        // adds nothing beyond the state-dir override.
        assert_eq!(conversations.len(), 3);
        let fresh = conversations
            .iter()
            .find(|c| c.external_id.as_deref() == Some("main/fresh"))
            .unwrap();
        assert_eq!(fresh.source_path, store.database);
        assert_eq!(fresh.messages.len(), 2);
        assert_eq!(fresh.messages[1].content, "WAL append");
        assert!(
            conversations
                .iter()
                .any(|c| c.external_id.as_deref() == Some("main/stale"))
        );
        assert!(
            conversations
                .iter()
                .any(|c| c.external_id.as_deref() == Some("main/legacy-only"))
        );
        let completions = completed.borrow();
        let store_completion = completions
            .iter()
            .find(|(path, ..)| *path == store.database)
            .expect("upstream traversal completes the store it scanned");
        assert_eq!(store_completion.1, 2);
        assert!(
            store_completion.2.iter().any(|path| *path == store.wal()),
            "upstream completion names the live -wal as reuse evidence"
        );
        let admitted = admitted_paths.borrow();
        assert!(!admitted.contains(&store.session_file("fresh")));
        assert!(!admitted.contains(&store.session_file("stale")));
        assert!(admitted.contains(&store.session_file("legacy-only")));
        Ok(())
    }

    #[test]
    fn admitted_out_wal_only_store_emits_nothing_and_completes_nothing() -> Result<()> {
        let store = wal_only_store_fixture()?;
        let connector = OpenClawConnector::new();
        let completed = std::cell::RefCell::new(Vec::<PathBuf>::new());
        let mut should_scan = |source: &DiscoveredSourceFile| source.source_path != store.database;
        let mut complete = |completion: &SourceCompletion| {
            completed
                .borrow_mut()
                .push(completion.source.source_path.clone());
            Ok(())
        };
        let mut conversations = Vec::new();
        connector.scan_with_source_boundaries(
            &store.scan_context(),
            &mut SourceScanHooks {
                should_scan_source: Some(&mut should_scan),
                on_source_complete: Some(&mut complete),
            },
            &mut |conversation| {
                conversations.push(conversation);
                Ok(())
            },
        )?;
        // A skipped source emits no conversations and no completion of its
        // own. The store was never re-read, so no session coverage is known
        // and its legacy copies are not suppressed either: each one is
        // admitted, emits, and completes on its own.
        let completed_paths: HashSet<_> = completed.borrow().iter().cloned().collect();
        assert_eq!(
            completed_paths,
            HashSet::from([
                store.session_file("fresh"),
                store.session_file("stale"),
                store.session_file("legacy-only"),
            ])
        );
        assert!(
            conversations
                .iter()
                .all(|conversation| conversation.source_path != store.database)
        );
        let ids: HashSet<_> = conversations
            .iter()
            .filter_map(|conversation| conversation.external_id.clone())
            .collect();
        assert_eq!(
            ids,
            HashSet::from([
                "main/fresh".to_string(),
                "main/stale".to_string(),
                "main/legacy-only".to_string(),
            ])
        );
        Ok(())
    }

    #[test]
    fn state_override_trims_and_expands_only_the_home_component() {
        let home = Path::new("profile");
        assert_eq!(
            state_path("  ~/openclaw-state  ", Some(home)),
            Some(home.join("openclaw-state"))
        );
        assert_eq!(
            state_path("~other/state", Some(home)),
            Some(PathBuf::from("~other/state"))
        );
        assert_eq!(
            state_path(" custom/state ", None),
            Some(PathBuf::from("custom/state"))
        );
        assert_eq!(state_path("  ", Some(home)), None);
    }

    #[test]
    fn registry_adapter_preserves_other_providers() {
        fn original() -> Box<dyn Connector + Send> {
            Box::new(franken_agent_detection::CodexConnector::new())
        }
        assert!(std::ptr::fn_addr_eq(
            with_wal_freshness("codex", original),
            original as fn() -> Box<dyn Connector + Send>
        ));
    }

    #[test]
    fn wal_sidecar_suffix_preserves_non_extension_path_identity() {
        assert_eq!(
            wal_path(Path::new("agent/openclaw-agent.sqlite")),
            PathBuf::from("agent/openclaw-agent.sqlite-wal")
        );
        assert_eq!(
            wal_path(Path::new("agent/history")),
            PathBuf::from("agent/history-wal")
        );
    }
}
