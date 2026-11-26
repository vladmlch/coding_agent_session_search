use super::*;

// Lexical-rebuild batching defaults are tuned for cold rebuilds over tens of
// thousands of conversations on a developer workstation (~512MB tantivy heap,
// 4 writer threads). Raise via env vars on constrained hosts or when a corpus
// exceeds these budgets. The previous (much smaller) defaults served
// correctness during an earlier storage migration but capped throughput at
// ~20 docs/sec on a 4.7M-message corpus; the values below keep Tantivy's
// writer threads fed and cut SQL round-trips by ~5x without blowing the
// configured heap.

pub(in crate::indexer) fn lexical_rebuild_commit_interval_conversations() -> usize {
    dotenvy::var("CASS_TANTIVY_REBUILD_COMMIT_EVERY_CONVERSATIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(10_000)
}

pub(in crate::indexer) fn lexical_rebuild_initial_commit_interval_conversations() -> usize {
    dotenvy::var("CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_CONVERSATIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(2_048)
}

pub(in crate::indexer) fn lexical_rebuild_commit_interval_messages() -> usize {
    dotenvy::var("CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(800_000)
}

pub(in crate::indexer) fn lexical_rebuild_initial_commit_interval_messages() -> usize {
    // The initial rebuild slice is already bounded by smaller conversation and
    // byte ceilings. Keeping an extra 50k-message cap forced unnecessary early
    // commit fences on large canonical rebuilds without providing meaningfully
    // earlier restartability.
    dotenvy::var("CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_MESSAGES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(800_000)
}

pub(in crate::indexer) fn lexical_rebuild_commit_interval_message_bytes() -> usize {
    lexical_rebuild_setting("CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGE_BYTES")
        .unwrap_or(512 * 1024 * 1024)
}

pub(in crate::indexer) fn lexical_rebuild_initial_commit_interval_message_bytes() -> usize {
    dotenvy::var("CASS_TANTIVY_REBUILD_INITIAL_COMMIT_EVERY_MESSAGE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(128 * 1024 * 1024)
}

pub(in crate::indexer) fn lexical_rebuild_commit_intervals_for_state(
    rebuild_state: &LexicalRebuildState,
) -> (usize, usize, usize) {
    let steady_conversations = lexical_rebuild_commit_interval_conversations();
    let steady_messages = lexical_rebuild_commit_interval_messages();
    let steady_message_bytes = lexical_rebuild_commit_interval_message_bytes();
    if rebuild_state.processed_conversations > 0 {
        return (steady_conversations, steady_messages, steady_message_bytes);
    }

    (
        lexical_rebuild_initial_commit_interval_conversations().min(steady_conversations),
        lexical_rebuild_initial_commit_interval_messages().min(steady_messages),
        lexical_rebuild_initial_commit_interval_message_bytes().min(steady_message_bytes),
    )
}

pub(in crate::indexer) fn lexical_rebuild_progress_heartbeat_interval_conversations() -> usize {
    dotenvy::var("CASS_TANTIVY_REBUILD_PROGRESS_HEARTBEAT_EVERY_CONVERSATIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(2_000)
}

pub(in crate::indexer) fn lexical_rebuild_progress_heartbeat_interval() -> Duration {
    Duration::from_millis(
        dotenvy::var("CASS_TANTIVY_REBUILD_PROGRESS_HEARTBEAT_EVERY_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(10_000),
    )
}

pub(in crate::indexer) fn lexical_rebuild_batch_fetch_conversation_limit(page_size: i64) -> usize {
    // Each chunk within a page is the unit of SQL message-fetch batching and
    // Tantivy writer feeding. The historical fixed default (512 conversations)
    // was tuned for a 4-thread writer; on larger hosts it silently under-fed
    // the writer path even after prep parallelism improved. Scale the default
    // with the writer-thread hint while still capping at `page_size` so we
    // never fetch beyond what's been paged from the conversations table.
    lexical_rebuild_setting("CASS_TANTIVY_REBUILD_BATCH_FETCH_CONVERSATIONS")
        .unwrap_or_else(|| {
            lexical_rebuild_default_batch_fetch_conversation_limit(
                page_size,
                crate::search::tantivy::tantivy_writer_parallelism_hint(),
            )
        })
        .min(usize::try_from(page_size.max(1)).unwrap_or(usize::MAX))
}

pub(in crate::indexer) fn lexical_rebuild_default_batch_fetch_conversation_limit(
    page_size: i64,
    tantivy_writer_parallelism: usize,
) -> usize {
    let page_size = usize::try_from(page_size.max(1)).unwrap_or(usize::MAX);
    512_usize
        .max(tantivy_writer_parallelism.max(1).saturating_mul(128))
        .min(page_size.max(1))
}

pub(in crate::indexer) fn lexical_rebuild_initial_batch_fetch_conversation_limit(
    default_limit: usize,
) -> usize {
    // Keep the very first durable rebuild slice bounded to one SQL batch by
    // default. This caps peak RSS and gets the first restartable checkpoint to
    // disk much sooner on corpora with unusually large conversations. After
    // the first durable commit, the rebuild immediately ramps back to the
    // steady-state `batch_fetch_conversation_limit`.
    dotenvy::var("CASS_TANTIVY_REBUILD_INITIAL_BATCH_FETCH_CONVERSATIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map(|value| value.min(default_limit.max(1)))
        .unwrap_or_else(|| 32.min(default_limit.max(1)))
}

pub(in crate::indexer) fn lexical_rebuild_available_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1)
        .max(1)
}

pub(in crate::indexer) fn lexical_rebuild_default_reserved_cores_for_available(
    available_parallelism: usize,
) -> usize {
    match available_parallelism {
        0 | 1 => 0,
        2..=4 => 1,
        5..=15 => 2,
        cores => (cores / 8).clamp(2, 8),
    }
}

pub(in crate::indexer) fn lexical_rebuild_reserved_cores_for_available(
    available_parallelism: usize,
) -> usize {
    dotenvy::var("CASS_TANTIVY_REBUILD_RESERVED_CORES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_else(|| {
            lexical_rebuild_default_reserved_cores_for_available(available_parallelism)
        })
        .min(available_parallelism.saturating_sub(1))
}

pub(in crate::indexer) fn lexical_rebuild_worker_parallelism_for_available_and_reserved(
    available_parallelism: usize,
    reserved_cores: usize,
) -> usize {
    available_parallelism
        .max(1)
        .saturating_sub(reserved_cores)
        .clamp(1, 64)
}

#[cfg(test)]
pub(in crate::indexer) fn lexical_rebuild_default_worker_parallelism_for_available(
    available_parallelism: usize,
) -> usize {
    lexical_rebuild_worker_parallelism_for_available_and_reserved(
        available_parallelism,
        lexical_rebuild_default_reserved_cores_for_available(available_parallelism),
    )
}

pub(in crate::indexer) fn lexical_rebuild_configured_worker_parallelism_for_available(
    available_parallelism: usize,
) -> usize {
    lexical_rebuild_worker_parallelism_for_available_and_reserved(
        available_parallelism,
        lexical_rebuild_reserved_cores_for_available(available_parallelism),
    )
}

/// GH #483: a named set of lexical-rebuild settings for `cass index
/// --rebuild-profile`. A profile only supplies values for knobs the operator
/// left unset: an explicit positive `CASS_TANTIVY_REBUILD_*` value still wins,
/// and the responsiveness governor can still lower the result. It adds no
/// second pipeline; it is the documented environment recipe under one name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LexicalRebuildProfile {
    /// The host-sized defaults.
    #[default]
    Default,
    /// The single-core, low-memory recipe for a wedged or memory-starved
    /// archive: one worker, one page-prep thread, a one-page channel, 64
    /// conversations per fetch and a durable commit every 64 MiB of text.
    Bounded,
}

impl LexicalRebuildProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Bounded => "bounded",
        }
    }

    /// The knobs this profile sets, by their environment variable names.
    pub fn settings(self) -> &'static [(&'static str, usize)] {
        match self {
            Self::Default => &[],
            Self::Bounded => &[
                ("CASS_TANTIVY_REBUILD_WORKERS", 1),
                ("CASS_TANTIVY_REBUILD_PAGE_PREP_WORKERS", 1),
                ("CASS_TANTIVY_REBUILD_PIPELINE_CHANNEL_SIZE", 1),
                ("CASS_TANTIVY_REBUILD_BATCH_FETCH_CONVERSATIONS", 64),
                (
                    "CASS_TANTIVY_REBUILD_COMMIT_EVERY_MESSAGE_BYTES",
                    64 * 1024 * 1024,
                ),
            ],
        }
    }

    pub(in crate::indexer) fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Bounded,
            _ => Self::Default,
        }
    }

    pub(in crate::indexer) fn to_u8(self) -> u8 {
        match self {
            Self::Default => 0,
            Self::Bounded => 1,
        }
    }
}

pub(in crate::indexer) static LEXICAL_REBUILD_PROFILE: AtomicU8 = AtomicU8::new(0);

/// Select the rebuild profile for this process. `cass index` calls it once,
/// before any rebuild setting is read.
pub fn set_lexical_rebuild_profile(profile: LexicalRebuildProfile) {
    LEXICAL_REBUILD_PROFILE.store(profile.to_u8(), Ordering::Relaxed);
}

pub fn lexical_rebuild_profile() -> LexicalRebuildProfile {
    LexicalRebuildProfile::from_u8(LEXICAL_REBUILD_PROFILE.load(Ordering::Relaxed))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::indexer) enum LexicalRebuildSettingSource {
    Env,
    Profile,
}

impl LexicalRebuildSettingSource {
    pub(in crate::indexer) fn as_str(self) -> &'static str {
        match self {
            Self::Env => "env",
            Self::Profile => "profile",
        }
    }
}

/// An explicit positive environment value wins; otherwise the profile's
/// value; `None` leaves the knob at its computed default.
pub(in crate::indexer) fn resolve_lexical_rebuild_setting(
    env_value: Option<&str>,
    profile: LexicalRebuildProfile,
    name: &str,
) -> Option<(usize, LexicalRebuildSettingSource)> {
    if let Some(value) = env_value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
    {
        return Some((value, LexicalRebuildSettingSource::Env));
    }
    profile
        .settings()
        .iter()
        .find(|(setting, _)| *setting == name)
        .map(|&(_, value)| (value, LexicalRebuildSettingSource::Profile))
}

pub(in crate::indexer) fn lexical_rebuild_setting(name: &str) -> Option<usize> {
    resolve_lexical_rebuild_setting(
        dotenvy::var(name).ok().as_deref(),
        lexical_rebuild_profile(),
        name,
    )
    .map(|(value, _)| value)
}

/// What `cass index --json` reports under `rebuild_profile`: the selected
/// profile, where each of its knobs came from, and the settings a lexical
/// rebuild in this process would run with (after the governor).
pub fn lexical_rebuild_profile_report() -> serde_json::Value {
    lexical_rebuild_profile_report_for(
        lexical_rebuild_profile(),
        |name| dotenvy::var(name).ok(),
        &lexical_rebuild_pipeline_settings_snapshot(),
    )
}

pub(in crate::indexer) fn lexical_rebuild_profile_report_for(
    profile: LexicalRebuildProfile,
    env: impl Fn(&str) -> Option<String>,
    effective: &LexicalRebuildPipelineSettingsSnapshot,
) -> serde_json::Value {
    let settings: Vec<serde_json::Value> = profile
        .settings()
        .iter()
        .filter_map(|&(name, _)| {
            let (value, source) =
                resolve_lexical_rebuild_setting(env(name).as_deref(), profile, name)?;
            Some(serde_json::json!({
                "env": name,
                "value": value,
                "source": source.as_str(),
            }))
        })
        .collect();
    serde_json::json!({
        "name": profile.as_str(),
        "settings": settings,
        "effective": {
            "workers": effective.workers,
            "page_prep_workers": effective.page_prep_workers,
            "staged_shard_builders": effective.staged_shard_builders,
            "staged_merge_workers": effective.staged_merge_workers,
            "tantivy_writer_threads": effective.tantivy_writer_threads,
            "pipeline_channel_size": effective.pipeline_channel_size,
            "steady_batch_fetch_conversations": effective.steady_batch_fetch_conversations,
            "steady_commit_every_message_bytes": effective.steady_commit_every_message_bytes,
            "pipeline_max_message_bytes_in_flight": effective.pipeline_max_message_bytes_in_flight,
        },
    })
}

pub(in crate::indexer) fn lexical_rebuild_worker_parallelism() -> usize {
    lexical_rebuild_setting("CASS_TANTIVY_REBUILD_WORKERS").unwrap_or_else(|| {
        lexical_rebuild_configured_worker_parallelism_for_available(
            lexical_rebuild_available_parallelism(),
        )
    })
}

pub(in crate::indexer) fn build_lexical_rebuild_worker_pool() -> Result<Option<ThreadPool>> {
    // Apply the responsiveness governor to the pool size so we don't spawn
    // more worker threads than the host can comfortably service. This affects
    // only the pool itself; each downstream consumer (page-prep, shard
    // builders, mergers) still applies its own governor clamp on top of its
    // own hard cap, so you cannot accidentally exceed the pool size.
    let raw = lexical_rebuild_worker_parallelism();
    let parallelism = responsiveness::effective_worker_count(raw).max(1);
    if parallelism <= 1 {
        return Ok(None);
    }

    ThreadPoolBuilder::new()
        .num_threads(parallelism)
        .thread_name(|idx| format!("cass-lexical-rebuild-{idx}"))
        .build()
        .map(Some)
        .map_err(anyhow::Error::new)
        .with_context(|| format!("building lexical rebuild worker pool with {parallelism} threads"))
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct LexicalRebuildPipelineSettingsSnapshot {
    pub workers: usize,
    pub available_parallelism: usize,
    pub reserved_cores: usize,
    pub tantivy_writer_threads: usize,
    pub staged_shard_builders: usize,
    pub staged_merge_workers: usize,
    pub controller_mode: String,
    pub controller_restore_clear_samples: usize,
    pub controller_restore_hold_ms: u64,
    pub controller_loadavg_high_watermark_1m_milli: Option<u32>,
    pub controller_loadavg_low_watermark_1m_milli: Option<u32>,
    pub page_size: i64,
    pub steady_batch_fetch_conversations: usize,
    pub startup_batch_fetch_conversations: usize,
    pub steady_commit_every_conversations: usize,
    pub startup_commit_every_conversations: usize,
    pub steady_commit_every_messages: usize,
    pub startup_commit_every_messages: usize,
    pub steady_commit_every_message_bytes: usize,
    pub startup_commit_every_message_bytes: usize,
    pub pipeline_channel_size: usize,
    pub page_prep_workers: usize,
    pub pipeline_max_message_bytes_in_flight: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::indexer) enum LexicalRebuildResponsivenessPolicy {
    Auto,
    Steady,
    Conservative,
}

impl LexicalRebuildResponsivenessPolicy {
    pub(in crate::indexer) fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Steady => "steady",
            Self::Conservative => "conservative",
        }
    }
}

pub(in crate::indexer) fn lexical_rebuild_responsiveness_policy()
-> LexicalRebuildResponsivenessPolicy {
    if let Some(policy) = dotenvy::var("CASS_TANTIVY_REBUILD_CONTROLLER_MODE")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
        .map(|value| match value {
            "steady" | "pinned_steady" | "off" | "disabled" => {
                LexicalRebuildResponsivenessPolicy::Steady
            }
            "conservative" | "startup" | "pinned_conservative" => {
                LexicalRebuildResponsivenessPolicy::Conservative
            }
            _ => LexicalRebuildResponsivenessPolicy::Auto,
        })
    {
        return policy;
    }

    if responsiveness::disabled_via_env() {
        LexicalRebuildResponsivenessPolicy::Steady
    } else {
        LexicalRebuildResponsivenessPolicy::Auto
    }
}

pub(in crate::indexer) fn lexical_rebuild_controller_restore_clear_samples() -> usize {
    dotenvy::var("CASS_TANTIVY_REBUILD_CONTROLLER_RESTORE_CLEAR_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(3)
}

pub(in crate::indexer) fn lexical_rebuild_controller_restore_hold() -> Duration {
    Duration::from_millis(
        dotenvy::var("CASS_TANTIVY_REBUILD_CONTROLLER_RESTORE_HOLD_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(5_000),
    )
}

pub(in crate::indexer) fn parse_lexical_rebuild_loadavg_1m_milli(contents: &str) -> Option<u32> {
    let first = contents.split_whitespace().next()?;
    let parsed = first.parse::<f64>().ok()?;
    if !parsed.is_finite() || parsed < 0.0 {
        return None;
    }
    let milli = (parsed * 1000.0).round();
    if !(0.0..=(u32::MAX as f64)).contains(&milli) {
        return None;
    }
    Some(milli as u32)
}

#[cfg(target_os = "linux")]
pub(in crate::indexer) fn lexical_rebuild_host_loadavg_1m_milli() -> Option<u32> {
    fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|contents| parse_lexical_rebuild_loadavg_1m_milli(&contents))
}

#[cfg(not(target_os = "linux"))]
pub(in crate::indexer) fn lexical_rebuild_host_loadavg_1m_milli() -> Option<u32> {
    None
}

pub(in crate::indexer) fn lexical_rebuild_default_controller_loadavg_high_watermark_1m_milli_for_available_and_reserved(
    available_parallelism: usize,
    reserved_cores: usize,
) -> Option<u32> {
    if !cfg!(target_os = "linux") || available_parallelism <= 1 {
        return None;
    }

    let reserved_headroom = reserved_cores.max(1);
    let high_slots = available_parallelism
        .saturating_sub(reserved_headroom.saturating_sub(1))
        .max(1);
    u32::try_from(high_slots.saturating_mul(1000)).ok()
}

pub(in crate::indexer) fn parse_lexical_rebuild_loadavg_override_milli(var: &str) -> Option<u32> {
    dotenvy::var(var)
        .ok()
        .and_then(|value| parse_lexical_rebuild_loadavg_1m_milli(&value))
}

pub(in crate::indexer) fn lexical_rebuild_controller_loadavg_high_watermark_1m_milli_for_available_and_reserved(
    available_parallelism: usize,
    reserved_cores: usize,
) -> Option<u32> {
    let env_override = parse_lexical_rebuild_loadavg_override_milli(
        "CASS_TANTIVY_REBUILD_CONTROLLER_LOADAVG_HIGH_WATERMARK_1M",
    );
    env_override.or_else(|| {
        if responsiveness::disabled_via_env() {
            None
        } else {
            lexical_rebuild_default_controller_loadavg_high_watermark_1m_milli_for_available_and_reserved(
                available_parallelism,
                reserved_cores,
            )
        }
    })
}

pub(in crate::indexer) fn lexical_rebuild_controller_loadavg_low_watermark_1m_milli_from_high(
    high_watermark_1m_milli: Option<u32>,
) -> Option<u32> {
    parse_lexical_rebuild_loadavg_override_milli(
        "CASS_TANTIVY_REBUILD_CONTROLLER_LOADAVG_LOW_WATERMARK_1M",
    )
    .or_else(|| high_watermark_1m_milli.map(|high| high.saturating_sub(1000)))
}

pub(crate) fn lexical_rebuild_pipeline_settings_snapshot() -> LexicalRebuildPipelineSettingsSnapshot
{
    lexical_rebuild_pipeline_settings_snapshot_inner(true)
}

pub(crate) fn lexical_rebuild_pipeline_settings_snapshot_passive()
-> LexicalRebuildPipelineSettingsSnapshot {
    lexical_rebuild_pipeline_settings_snapshot_inner(false)
}

pub(in crate::indexer) fn lexical_rebuild_pipeline_settings_snapshot_inner(
    apply_responsiveness_governor: bool,
) -> LexicalRebuildPipelineSettingsSnapshot {
    let steady_batch_fetch_conversations =
        lexical_rebuild_batch_fetch_conversation_limit(LEXICAL_REBUILD_PAGE_SIZE);
    let startup_batch_fetch_conversations =
        lexical_rebuild_initial_batch_fetch_conversation_limit(steady_batch_fetch_conversations);
    let steady_commit_every_conversations = lexical_rebuild_commit_interval_conversations();
    let startup_commit_every_conversations =
        lexical_rebuild_initial_commit_interval_conversations()
            .min(steady_commit_every_conversations);
    let steady_commit_every_messages = lexical_rebuild_commit_interval_messages();
    let startup_commit_every_messages =
        lexical_rebuild_initial_commit_interval_messages().min(steady_commit_every_messages);
    let steady_commit_every_message_bytes = lexical_rebuild_commit_interval_message_bytes();
    let startup_commit_every_message_bytes =
        lexical_rebuild_initial_commit_interval_message_bytes()
            .min(steady_commit_every_message_bytes);
    let pipeline_channel_size = lexical_rebuild_pipeline_channel_size();
    let pipeline_max_message_bytes_in_flight = lexical_rebuild_pipeline_max_message_bytes_in_flight(
        startup_commit_every_message_bytes,
        pipeline_channel_size,
    );
    let available_parallelism = lexical_rebuild_available_parallelism();
    let reserved_cores = lexical_rebuild_reserved_cores_for_available(available_parallelism);
    let tantivy_writer_threads_raw =
        crate::search::tantivy::tantivy_writer_parallelism_hint_for_available(
            available_parallelism,
        );
    let tantivy_writer_threads = if apply_responsiveness_governor {
        responsiveness::effective_worker_count(tantivy_writer_threads_raw).max(1)
    } else {
        tantivy_writer_threads_raw
    };
    let controller_restore_hold = lexical_rebuild_controller_restore_hold();
    let controller_loadavg_high_watermark_1m_milli =
        lexical_rebuild_controller_loadavg_high_watermark_1m_milli_for_available_and_reserved(
            available_parallelism,
            reserved_cores,
        );
    let controller_loadavg_low_watermark_1m_milli =
        lexical_rebuild_controller_loadavg_low_watermark_1m_milli_from_high(
            controller_loadavg_high_watermark_1m_milli,
        );

    LexicalRebuildPipelineSettingsSnapshot {
        workers: lexical_rebuild_worker_parallelism(),
        available_parallelism,
        reserved_cores,
        tantivy_writer_threads,
        staged_shard_builders: if apply_responsiveness_governor {
            lexical_rebuild_staged_shard_builder_parallelism()
        } else {
            lexical_rebuild_staged_shard_builder_parallelism_configured()
        },
        staged_merge_workers: if apply_responsiveness_governor {
            lexical_rebuild_staged_merge_worker_parallelism()
        } else {
            lexical_rebuild_staged_merge_worker_parallelism_configured()
        },
        controller_mode: lexical_rebuild_responsiveness_policy().as_str().to_string(),
        controller_restore_clear_samples: lexical_rebuild_controller_restore_clear_samples(),
        controller_restore_hold_ms: controller_restore_hold.as_millis() as u64,
        controller_loadavg_high_watermark_1m_milli,
        controller_loadavg_low_watermark_1m_milli,
        page_size: LEXICAL_REBUILD_PAGE_SIZE,
        steady_batch_fetch_conversations,
        startup_batch_fetch_conversations,
        steady_commit_every_conversations,
        startup_commit_every_conversations,
        steady_commit_every_messages,
        startup_commit_every_messages,
        steady_commit_every_message_bytes,
        startup_commit_every_message_bytes,
        pipeline_channel_size,
        page_prep_workers: if apply_responsiveness_governor {
            lexical_rebuild_page_prep_worker_parallelism()
        } else {
            lexical_rebuild_page_prep_worker_parallelism_configured()
        },
        pipeline_max_message_bytes_in_flight,
    }
}
