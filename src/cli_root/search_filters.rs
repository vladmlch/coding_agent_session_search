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

/// Time filter helper for search commands
#[derive(Debug, Clone, Default)]
pub struct TimeFilter {
    pub since: Option<i64>,
    pub until: Option<i64>,
    /// The flag that set `since` (`--days 7`, `--since 2026-09-01`, ...),
    /// echoed in `_meta.effective.time_window`.
    pub since_from: Option<String>,
    /// The flag that set `until`.
    pub until_from: Option<String>,
}

/// Semantic search options from CLI flags (bd-3bbv)
///
/// These options control model selection, reranking, and daemon usage
/// for semantic search operations.
#[derive(Debug, Clone, Default)]
pub struct SemanticSearchOptions {
    /// Embedding model to use (overrides config/default)
    pub model: Option<String>,
    /// Enable reranking of results
    pub rerank: bool,
    /// Reranker model to use (if rerank is enabled)
    pub reranker: Option<String>,
    /// Use daemon for warm model inference
    pub use_daemon: bool,
    /// Auto-spawn the daemon if no existing instance is reachable.
    pub auto_spawn_daemon: bool,
    /// Use approximate nearest neighbor search when available
    pub approximate: bool,
    /// Optional two-tier execution strategy for semantic mode.
    pub tier_mode: crate::search::query::SemanticTierMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SemanticDaemonPolicy {
    pub(crate) use_existing: bool,
    pub(crate) auto_spawn: bool,
}

pub(crate) const fn semantic_daemon_policy(daemon: bool, no_daemon: bool) -> SemanticDaemonPolicy {
    SemanticDaemonPolicy {
        use_existing: !no_daemon,
        auto_spawn: daemon && !no_daemon,
    }
}

#[cfg(any(unix, test))]
pub(crate) fn compose_verified_daemon_embedder_or_local(
    daemon: Arc<dyn crate::search::daemon_client::DaemonClient>,
    embedder: Arc<dyn crate::search::embedder::Embedder>,
    config: crate::search::daemon_client::DaemonRetryConfig,
    expected_connection: crate::search::daemon_client::DaemonConnectionIdentityV1,
    verifier: crate::search::daemon_client::PinnedDaemonVerifierV1,
) -> Arc<dyn crate::search::embedder::Embedder> {
    let daemon_reachable = daemon.is_available();
    let checked_local = match crate::search::daemon_client::IdentityCheckedLocalEmbedder::new(
        Arc::clone(&embedder),
        expected_connection.embedding_identity.clone(),
    ) {
        Ok(checked) => Arc::new(checked) as Arc<dyn crate::search::embedder::Embedder>,
        Err(error) => {
            tracing::warn!(
                error = %error,
                embedder_id = %embedder.id(),
                "Authenticated daemon identity is incompatible with the local fallback"
            );
            return embedder;
        }
    };
    match crate::search::daemon_client::DaemonFallbackEmbedder::new_verified(
        daemon,
        Some(checked_local),
        config,
        expected_connection,
        verifier,
    ) {
        Ok(verified) => Arc::new(
            crate::search::daemon_client::CassVerifiedDaemonEmbedder::new(
                verified,
                embedder.id(),
                embedder.model_name(),
            ),
        ) as Arc<dyn crate::search::embedder::Embedder>,
        Err(error) => {
            tracing::warn!(
                error = %error,
                embedder_id = %embedder.id(),
                daemon_reachable,
                "Daemon embedding identity is unverifiable; using the verified local embedder"
            );
            if daemon_reachable {
                eprintln!(
                    "Warning: a local embedding daemon is running but was not used ({error}); \
                     falling back to in-process {} inference because its producer \
                     attestation was rejected; pass --no-daemon to skip daemon discovery.",
                    embedder.id()
                );
            }
            embedder
        }
    }
}

#[cfg(test)]
#[test]
fn issue_347_semantic_daemon_policy_discovers_existing_by_default_and_respects_overrides() {
    assert_eq!(
        semantic_daemon_policy(false, false),
        SemanticDaemonPolicy {
            use_existing: true,
            auto_spawn: false,
        }
    );
    assert_eq!(
        semantic_daemon_policy(true, false),
        SemanticDaemonPolicy {
            use_existing: true,
            auto_spawn: true,
        }
    );
    assert_eq!(
        semantic_daemon_policy(false, true),
        SemanticDaemonPolicy {
            use_existing: false,
            auto_spawn: false,
        }
    );
}

#[cfg(test)]
#[test]
fn unverifiable_daemon_composition_preserves_the_verified_local_embedding_space() {
    use crate::search::daemon_client::{
        DaemonConnectionIdentityV1, DaemonRetryConfig, NoopDaemonClient, PinnedDaemonVerifierV1,
    };
    use crate::search::embedder::Embedder;
    use crate::search::hash_embedder::HashEmbedder;
    use frankensearch::{DAEMON_CONNECTION_IDENTITY_SCHEMA_V1, Embedder as _, ModelCategory};

    let local: Arc<dyn Embedder> = Arc::new(HashEmbedder::new(32));
    let expected = local
        .embed_sync("preserve verified local vectors")
        .expect("local control embedding");
    let identity_source =
        frankensearch::HashEmbedder::new(32, frankensearch::HashAlgorithm::FnvModular);
    let connection = DaemonConnectionIdentityV1 {
        schema_version: DAEMON_CONNECTION_IDENTITY_SCHEMA_V1,
        endpoint_fingerprint: "11".repeat(32),
        executable_fingerprint: "22".repeat(32),
        protocol_revision: "test-attested-v1".to_string(),
        key_id: "test-key-v1".to_string(),
        generation: 1,
        embedding_identity: identity_source
            .identity()
            .expect("Frankensearch hash identity")
            .clone(),
        model_category: ModelCategory::HashEmbedder,
    };
    let daemon = Arc::new(NoopDaemonClient::new("unverified-test-daemon"));
    let verifier =
        PinnedDaemonVerifierV1::new("test-key-v1", vec![7_u8; 32]).expect("test verifier");
    let composed = compose_verified_daemon_embedder_or_local(
        daemon,
        local,
        DaemonRetryConfig::default(),
        connection,
        verifier,
    );

    assert_eq!(composed.id(), "fnv1a-32");
    assert_eq!(
        composed
            .embed_sync("preserve verified local vectors")
            .expect("composed embedding"),
        expected,
        "an unverifiable daemon must not change the query embedding space"
    );
}

impl TimeFilter {
    /// Resolve the search/pack time window.
    ///
    /// An explicit `--since`/`--until` that cannot be parsed is a usage
    /// error; it used to be dropped silently, so a typo ran an unfiltered
    /// query with exit 0 (2l1b0.64).
    pub fn new(
        days: Option<u32>,
        today: bool,
        yesterday: bool,
        week: bool,
        since_str: Option<&str>,
        until_str: Option<&str>,
    ) -> CliResult<Self> {
        use chrono::{Datelike, Duration, Local, TimeZone};

        let now = Local::now();
        let today_start = Local
            .with_ymd_and_hms(now.year(), now.month(), now.day(), 0, 0, 0)
            .single()
            .unwrap_or(now);

        let (since, until, preset) = if today {
            (
                Some(today_start.timestamp_millis()),
                None,
                Some("--today".to_string()),
            )
        } else if yesterday {
            let yesterday_start = today_start - Duration::days(1);
            (
                Some(yesterday_start.timestamp_millis()),
                Some(today_start.timestamp_millis()),
                Some("--yesterday".to_string()),
            )
        } else if week {
            let week_ago = now - Duration::days(7);
            (
                Some(week_ago.timestamp_millis()),
                None,
                Some("--week".to_string()),
            )
        } else if let Some(d) = days {
            let days_ago = now - Duration::days(i64::from(d));
            (
                Some(days_ago.timestamp_millis()),
                None,
                Some(format!("--days {d}")),
            )
        } else {
            (None, None, None)
        };
        let mut since_from = since.and(preset.clone());
        let mut until_from = until.and(preset);

        // Explicit --since/--until override the convenience flags.
        let since = match since_str {
            Some(raw) => {
                since_from = Some(format!("--since {raw}"));
                Some(parse_search_time_bound("--since", raw, TimeBound::Since)?)
            }
            None => since,
        };
        let until = match until_str {
            Some(raw) => {
                until_from = Some(format!("--until {raw}"));
                Some(parse_search_time_bound("--until", raw, TimeBound::Until)?)
            }
            None => until,
        };
        if let (Some(since), Some(until)) = (since, until)
            && since > until
        {
            return Err(CliError::usage(
                "the time window is empty because --since is later than --until",
                Some("Choose an --until value at or after --since.".into()),
            ));
        }

        Ok(TimeFilter {
            since,
            until,
            since_from,
            until_from,
        })
    }
}

/// Which end of a time window a user-supplied value bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TimeBound {
    Since,
    Until,
}

pub(crate) fn parse_search_time_bound(flag: &str, raw: &str, bound: TimeBound) -> CliResult<i64> {
    parse_time_bound(raw, bound).ok_or_else(|| {
        CliError::usage(
            format!("could not parse {flag} value {raw:?}"),
            Some(
                "Use an ISO date (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS), a US date (MM/DD/YYYY), \
                 a keyword (now/today/yesterday), a relative offset (-7d, -24h, 30m) or a unix \
                 timestamp."
                    .into(),
            ),
        )
    })
}

/// Parse a `--since`/`--until` value. A value that names a whole local day
/// (a date without a time, `today`, `yesterday`) starts at local midnight as
/// `--since` and ends at the day's last millisecond as `--until`, so
/// `--until 2026-09-01` includes September 1 (README "Flexible Time
/// Input"). Every other form is an instant and is used as-is.
pub(crate) fn parse_time_bound(raw: &str, bound: TimeBound) -> Option<i64> {
    let start = parse_datetime_str(raw)?;
    if bound == TimeBound::Since || !names_whole_local_day(raw) {
        return Some(start);
    }
    let day = chrono::DateTime::from_timestamp_millis(start)?
        .with_timezone(&chrono::Local)
        .date_naive();
    let next_day = day.succ_opt()?.and_hms_opt(0, 0, 0)?;
    let next_start = match chrono::TimeZone::from_local_datetime(&chrono::Local, &next_day) {
        chrono::LocalResult::Single(local) | chrono::LocalResult::Ambiguous(local, _) => {
            local.timestamp_millis()
        }
        chrono::LocalResult::None => return Some(start),
    };
    Some(next_start - 1)
}

pub(crate) fn names_whole_local_day(raw: &str) -> bool {
    use chrono::NaiveDate;
    let value = raw.trim().to_ascii_lowercase();
    matches!(value.as_str(), "today" | "yesterday")
        || ["%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%m-%d-%Y"]
            .iter()
            .any(|format| NaiveDate::parse_from_str(&value, format).is_ok())
}

pub(crate) fn parse_datetime_str(s: &str) -> Option<i64> {
    use chrono::{Local, NaiveDate, NaiveDateTime, TimeZone};

    fn local_from_naive(dt: NaiveDateTime) -> i64 {
        match Local.from_local_datetime(&dt) {
            chrono::LocalResult::Single(local) => local.timestamp_millis(),
            chrono::LocalResult::Ambiguous(local, _) => local.timestamp_millis(),
            chrono::LocalResult::None => Local.from_utc_datetime(&dt).timestamp_millis(),
        }
    }

    fn local_midnight_ts(date: NaiveDate) -> Option<i64> {
        let dt = date.and_hms_opt(0, 0, 0)?;
        Some(local_from_naive(dt))
    }

    // Try full datetime first: YYYY-MM-DDTHH:MM:SS
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(local_from_naive(dt));
    }

    // Try date only: YYYY-MM-DD
    if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return local_midnight_ts(date);
    }

    crate::time_parser::parse_time_input(s)
}

#[cfg(test)]
mod search_time_filter_tests {
    use super::*;

    const DAY_MS: i64 = 86_400_000;

    /// 2l1b0.64: an unparseable explicit bound used to be dropped, so the
    /// search ran unfiltered with exit 0.
    #[test]
    fn unparseable_bounds_are_usage_errors() {
        for (since, until) in [(Some("2026-13-01"), None), (None, Some("not-a-date"))] {
            let error = TimeFilter::new(None, false, false, false, since, until)
                .expect_err("an unparseable bound must not be ignored");
            assert_eq!(error.code, 2, "{error:?}");
            assert_eq!(error.kind, "usage", "{error:?}");
            let flag = if since.is_some() {
                "--since"
            } else {
                "--until"
            };
            assert!(error.message.contains(flag), "{error:?}");
        }
    }

    /// README: a date-only `--until` includes the named day.
    #[test]
    fn date_only_until_covers_the_whole_local_day() {
        for raw in ["2026-07-15", "2026/07/15", "07/15/2026", "07-15-2026"] {
            let start = parse_time_bound(raw, TimeBound::Since).expect("date parses");
            let end = parse_time_bound(raw, TimeBound::Until).expect("date parses");
            assert_eq!(end - start, DAY_MS - 1, "{raw}");
        }
        let window = TimeFilter::new(None, false, false, false, None, Some("2026-07-15"))
            .expect("valid bound");
        let noon = parse_datetime_str("2026-07-15T12:00:00").expect("instant parses");
        assert!(
            window.until.is_some_and(|until| until >= noon),
            "noon on the named day must be inside --until 2026-07-15"
        );
        let previous_day = TimeFilter::new(None, false, false, false, None, Some("2026-07-14"))
            .expect("valid bound");
        assert!(
            previous_day.until.is_some_and(|until| until < noon),
            "noon on the next day must be outside --until 2026-07-14"
        );
    }

    #[test]
    fn instants_and_relative_bounds_are_unchanged() {
        let instant = "2026-07-15T12:00:00";
        assert_eq!(
            parse_time_bound(instant, TimeBound::Until),
            parse_datetime_str(instant)
        );
        let relative_since = parse_time_bound("-7d", TimeBound::Since).expect("relative parses");
        let relative_until = parse_time_bound("-7d", TimeBound::Until).expect("relative parses");
        assert!((relative_until - relative_since).abs() < 60_000);
    }

    #[test]
    fn inverted_window_is_a_usage_error() {
        let error = TimeFilter::new(
            None,
            false,
            false,
            false,
            Some("2026-07-16"),
            Some("2026-07-15"),
        )
        .expect_err("empty window");
        assert_eq!(error.kind, "usage");
        let same_day = TimeFilter::new(
            None,
            false,
            false,
            false,
            Some("2026-07-15"),
            Some("2026-07-15"),
        )
        .expect("a single-day window is valid");
        assert!(same_day.since < same_day.until);
    }
}

/// Compute aggregations from search hits
pub(crate) fn compute_aggregations(
    hits: &[crate::search::query::SearchHit],
    fields: &[AggregateField],
) -> Aggregations {
    use std::collections::HashMap;

    const MAX_BUCKETS: usize = 10;
    let mut aggregations = Aggregations::default();

    for field in fields {
        let mut counts: HashMap<String, u64> = HashMap::new();

        // Count occurrences based on field type
        for hit in hits {
            let key = match field {
                AggregateField::Agent => hit.agent.clone(),
                AggregateField::Workspace => hit.workspace.clone(),
                AggregateField::Date => {
                    // Group by date (YYYY-MM-DD)
                    hit.created_at
                        .and_then(|ts| {
                            chrono::DateTime::from_timestamp_millis(ts)
                                .map(|d| d.format("%Y-%m-%d").to_string())
                        })
                        .unwrap_or_else(|| "unknown".to_string())
                }
                AggregateField::MatchType => format!("{:?}", hit.match_type).to_lowercase(),
            };
            *counts.entry(key).or_insert(0) += 1;
        }

        // Sort by count descending, take top N
        let mut sorted: Vec<_> = counts.into_iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

        let total_count: u64 = sorted.iter().map(|(_, c)| *c).sum();
        let top_buckets: Vec<AggregationBucket> = sorted
            .iter()
            .take(MAX_BUCKETS)
            .map(|(key, count)| AggregationBucket {
                key: key.clone(),
                count: *count,
            })
            .collect();
        let top_sum: u64 = top_buckets.iter().map(|b| b.count).sum();
        let other_count = total_count.saturating_sub(top_sum);

        let agg = FieldAggregation {
            buckets: top_buckets,
            other_count,
        };

        match field {
            AggregateField::Agent => aggregations.agent = Some(agg),
            AggregateField::Workspace => aggregations.workspace = Some(agg),
            AggregateField::Date => aggregations.date = Some(agg),
            AggregateField::MatchType => aggregations.match_type = Some(agg),
        }
    }

    aggregations
}

pub(crate) const VALID_AGGREGATE_FIELDS: &str = "agent, workspace, date, match_type";

/// Parse aggregate field strings into enum values.
pub(crate) fn parse_aggregate_fields(fields: &[String]) -> CliResult<Vec<AggregateField>> {
    let mut parsed = Vec::with_capacity(fields.len());
    let mut invalid = Vec::new();

    for field in fields {
        let trimmed = field.trim();
        if trimmed.is_empty() {
            invalid.push("<empty>".to_string());
            continue;
        }

        match AggregateField::from_str(trimmed) {
            Some(field) => parsed.push(field),
            None => invalid.push(field.clone()),
        }
    }

    if invalid.is_empty() {
        Ok(parsed)
    } else {
        Err(CliError::usage(
            format!("invalid --aggregate field(s): {}", invalid.join(", ")),
            Some(format!("Valid aggregate fields: {VALID_AGGREGATE_FIELDS}")),
        ))
    }
}

#[cfg(test)]
mod aggregate_field_validation_tests {
    use super::{AggregateField, parse_aggregate_fields};

    #[test]
    fn aggregate_fields_reject_unknown_values() {
        let err = parse_aggregate_fields(&["agnet".to_string()]).unwrap_err();

        assert_eq!(err.code, 2);
        assert_eq!(err.kind, "usage");
        assert!(err.message.contains("agnet"));
        assert!(
            err.hint
                .as_deref()
                .is_some_and(|hint| hint.contains("agent, workspace, date, match_type"))
        );
    }

    #[test]
    fn aggregate_fields_trim_whitespace() {
        let fields = parse_aggregate_fields(&["agent".to_string(), " workspace ".to_string()])
            .expect("aggregate fields should parse");

        assert_eq!(
            fields,
            vec![AggregateField::Agent, AggregateField::Workspace]
        );
    }
}

pub(crate) fn search_dry_run_meta(robot_meta: bool, elapsed_ms: u128) -> serde_json::Value {
    let mut meta = serde_json::json!({
        "dry_run": true,
    });
    if robot_meta && let serde_json::Value::Object(ref mut map) = meta {
        map.insert("elapsed_ms".to_string(), serde_json::json!(elapsed_ms));
    }
    meta
}

#[cfg(test)]
mod search_dry_run_meta_tests {
    use super::search_dry_run_meta;

    #[test]
    fn dry_run_meta_is_stable_without_robot_meta() {
        let first = search_dry_run_meta(false, 1);
        let second = search_dry_run_meta(false, 9_999);

        assert_eq!(first, second);
        assert_eq!(first["dry_run"].as_bool(), Some(true));
        assert!(first.get("elapsed_ms").is_none());
    }

    #[test]
    fn dry_run_meta_keeps_elapsed_ms_when_requested() {
        let meta = search_dry_run_meta(true, 42);

        assert_eq!(meta["dry_run"].as_bool(), Some(true));
        assert_eq!(meta["elapsed_ms"].as_u64(), Some(42));
    }
}
