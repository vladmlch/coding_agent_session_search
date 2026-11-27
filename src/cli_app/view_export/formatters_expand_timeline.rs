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
use crate::timeline_json;
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

#[allow(unused_imports)]
pub(crate) use crate::export::message_parse::{
    extract_role, extract_text_content, extract_text_content_without_tool_blocks,
    flatten_pi_family_text_content, format_as_html, format_as_markdown, format_as_text,
    html_escape, normalized_export_role, smart_truncate, strip_flattened_tool_call_lines,
    strip_tool_marker,
};

/// Show messages around a specific line in a session file
pub(crate) fn run_expand(
    path: &Path,
    db_override: Option<PathBuf>,
    source_id: Option<&str>,
    line: usize,
    context: usize,
    output_format: Option<RobotFormat>,
) -> CliResult<()> {
    if let Some(source_id) = source_id {
        validate_followup_source_id(source_id, "cass expand")?;
    }
    let normalized_source_id = canonical_followup_source_id(source_id);
    let source_id = normalized_source_id.as_deref();
    if line == 0 {
        return Err(CliError {
            code: 2,
            kind: CliErrorKind::InvalidLine.kind_str(),
            message: "Line numbers start at 1, not 0".to_string(),
            hint: Some("Use -n 1 for the first line".to_string()),
            retryable: false,
        });
    }

    let db_path = db_override.unwrap_or_else(default_db_path);
    let indexed_view =
        try_load_indexed_conversation_from_db_with_source(path, &db_path, source_id, None);
    let allow_direct_file = followup_source_is_local(source_id) || source_id.is_none();

    let prefer_direct_file = prefers_direct_jsonl_file(path, source_id);

    let raw_lines: Vec<String> = if prefer_direct_file {
        match read_followup_file_lines(path) {
            Ok(lines) => lines,
            Err(err) => {
                if let Some(view) = indexed_view.as_ref() {
                    serialize_indexed_view_lines(view)?
                } else {
                    return Err(err);
                }
            }
        }
    } else if let Some(view) = indexed_view.as_ref() {
        serialize_indexed_view_lines(view)?
    } else if allow_direct_file && path.exists() {
        return Err(CliError {
            code: 9,
            kind: CliErrorKind::IndexedSessionRequired.kind_str(),
            message: format!(
                "Local expand for '{}' requires an indexed conversation or a JSONL session",
                path.display()
            ),
            hint: Some(
                "Run 'cass index --full' first, or expand directly from a JSONL session."
                    .to_string(),
            ),
            retryable: false,
        });
    } else {
        return Err(CliError {
            code: 3,
            kind: CliErrorKind::FileNotFound.kind_str(),
            message: match source_id {
                Some(source_id) => format!(
                    "No indexed session found for source '{}' at {}",
                    source_id,
                    path.display()
                ),
                None => format!("Session file not found: {}", path.display()),
            },
            hint: Some(match source_id {
                Some(_) => "Use the exact source_id from search output or omit --source to prefer the local file/path.".to_string(),
                None => "Use 'cass search' to find session paths".to_string(),
            }),
            retryable: false,
        });
    };

    let mut messages: Vec<(usize, serde_json::Value)> = Vec::new();
    let mut target_msg_idx: Option<usize> = None;
    let mut current_line: usize = 0;

    for raw_line in raw_lines {
        current_line += 1;
        if raw_line.trim().is_empty() {
            continue;
        }
        if let Ok(msg) = serde_json::from_str::<serde_json::Value>(&raw_line) {
            if current_line == line {
                target_msg_idx = Some(messages.len());
            }
            messages.push((current_line, msg));
        }
    }

    // GH493: only an exact raw-line match can be marked as the target.

    let target_idx = target_msg_idx.ok_or_else(|| CliError {
        code: 2,
        kind: CliErrorKind::LineNotFound.kind_str(),
        message: format!("No message found at exact line {}", line),
        hint: Some(format!("File has {} messages", messages.len())),
        retryable: false,
    })?;

    let start = target_idx.saturating_sub(context);
    let end = target_idx
        .saturating_add(context)
        .saturating_add(1)
        .min(messages.len());

    let context_messages: Vec<_> = messages[start..end]
        .iter()
        .enumerate()
        .map(|(i, (line_num, msg))| {
            let is_target = start + i == target_idx;
            (line_num, msg, is_target)
        })
        .collect();

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    if let Some(fmt) = structured_format {
        let output: Vec<serde_json::Value> = context_messages
            .iter()
            .map(|(line_num, msg, is_target)| {
                let role = extract_role(msg);
                let content = extract_text_content(msg);
                serde_json::json!({
                    "line": line_num,
                    "role": role,
                    "is_target": is_target,
                    "content": content,
                })
            })
            .collect();
        return output_structured_value(serde_json::Value::Array(output), fmt);
    }

    println!("\n📍 Context around line {} in {}\n", line, path.display());
    println!("{}", "─".repeat(60));

    for (line_num, msg, is_target) in context_messages {
        let role = extract_role(msg);
        let content = extract_text_content(msg);
        let preview: String = content.chars().take(300).collect();
        let marker = if is_target { ">>>" } else { "   " };
        let role_icon = match role.as_str() {
            "user" => "👤",
            "assistant" => "🤖",
            _ => "📝",
        };

        println!(
            "{} L{:>4} {} {}",
            marker,
            line_num,
            role_icon,
            role.to_uppercase()
        );
        println!("        {}", preview.replace('\n', " "));
        if content.len() > 300 {
            println!("        ... ({} more chars)", content.len() - 300);
        }
        println!();
    }

    println!("{}", "─".repeat(60));
    println!(
        "Showing messages {} to {} of {} total",
        start + 1,
        end,
        messages.len()
    );
    Ok(())
}

pub(crate) fn extract_message_timestamp(msg: &serde_json::Value) -> Option<i64> {
    msg.get("timestamp")
        .and_then(crate::connectors::parse_timestamp)
        .or_else(|| {
            msg.get("message")
                .and_then(|m| m.get("timestamp"))
                .and_then(crate::connectors::parse_timestamp)
        })
        .or_else(|| {
            msg.get("payload")
                .and_then(|p| p.get("timestamp"))
                .and_then(crate::connectors::parse_timestamp)
        })
}

/// Show activity timeline for a time range
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_timeline(
    since: Option<&str>,
    until: Option<&str>,
    today: bool,
    agents: &[String],
    data_dir: &Option<PathBuf>,
    db_override: Option<PathBuf>,
    output_format: Option<RobotFormat>,
    group_by: TimelineGrouping,
    source: Option<String>,
) -> CliResult<()> {
    use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt, TransactionExt};
    use crate::sources::provenance::SourceFilter;
    use chrono::{Local, TimeZone, Utc};
    use std::collections::HashMap;

    let agents = normalize_agent_filters(agents)?;

    // Parse source filter (P3.2)
    let source_filter = source.as_ref().map(|s| SourceFilter::parse(s));

    let now = Local::now();
    let (start_ts, end_ts) = if today {
        let start_of_day = now
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .expect("valid start of day");
        let local_start = match Local.from_local_datetime(&start_of_day) {
            chrono::LocalResult::Single(dt) => dt,
            chrono::LocalResult::Ambiguous(dt, _) => dt,
            chrono::LocalResult::None => Local.from_utc_datetime(&start_of_day),
        };
        (local_start.timestamp_millis(), now.timestamp_millis())
    } else {
        // The same grammar as search's --since/--until. An unparseable value
        // is a usage error: it used to become the 7-day default silently, so
        // `--since 5m` ran a week's timeline with exit 0.
        let start = match since {
            Some(raw) => parse_search_time_bound("--since", raw, TimeBound::Since)?,
            None => (now - chrono::Duration::days(7)).timestamp_millis(),
        };
        let end = match until {
            Some(raw) => parse_search_time_bound("--until", raw, TimeBound::Until)?,
            None => now.timestamp_millis(),
        };
        (start, end)
    };

    // Fail loudly on an inverted range rather than silently returning 0 rows.
    // A bad relative offset (e.g. a future `--since`) used to compute
    // start > end and match nothing without explanation (#280).
    if start_ts > end_ts {
        return Err(CliError::usage(
            "--since resolves to a time after --until (empty range)",
            Some(format!(
                "Computed window start ({start_ts}ms) is after end ({end_ts}ms). \
                 Check the --since/--until values; relative offsets like `-7d` mean \"7 days ago\"."
            )),
        ));
    }

    let db_path = analytics_db_path(data_dir, db_override.as_ref());
    let conn = open_franken_analytics_db(data_dir, db_override.as_ref())?;

    // LEFT JOIN + COALESCE keeps legacy conversations with NULL agent_id.
    // Filter on the same projected identity so --agent unknown includes them.
    let mut sql = String::from(
        "SELECT c.id, COALESCE(a.slug, 'unknown') as agent, c.title, c.started_at, c.ended_at, c.source_path,
                0 as message_count, COALESCE(c.source_id, 'local') as source_id, c.origin_host, s.kind as origin_kind
         FROM conversations c
         LEFT JOIN agents a ON c.agent_id = a.id
         LEFT JOIN sources s ON c.source_id = s.id
         WHERE c.started_at >= ?1 AND c.started_at <= ?2",
    );

    let mut params: Vec<ParamValue> = vec![start_ts.into(), end_ts.into()];

    if !agents.is_empty() {
        use std::fmt::Write as _;
        sql.push_str(" AND COALESCE(a.slug, 'unknown') IN (");
        for (i, agent) in agents.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            let _ = write!(sql, "?{}", params.len() + 1);
            params.push(agent.clone().into());
        }
        sql.push(')');
    }

    // Source filter (P3.2)
    if let Some(ref filter) = source_filter {
        append_source_filter_condition(&mut sql, &mut params, filter);
    }

    // GH#521: joining messages into GROUP BY sends this query through the
    // engine's materializing fallback, including message bodies outside the
    // requested window. Select only conversation metadata first; none of the
    // time/agent/source predicates may depend on the messages table.
    sql.push_str(" ORDER BY c.started_at DESC");

    // Keep metadata and the later indexed counts on the same read snapshot,
    // just as the former single SELECT did while indexing runs concurrently.
    let mut snapshot = conn.transaction().map_err(|e| CliError {
        code: 9,
        kind: CliErrorKind::DbQuery.kind_str(),
        message: format!("Failed to start timeline snapshot: {e}"),
        hint: None,
        retryable: false,
    })?;
    let rows = conn
        .query_map_collect(&sql, &params, |row: &crate::franken_sync::Row| {
            Ok((
                row.get_typed::<i64>(0)?,            // id
                row.get_typed::<String>(1)?,         // agent
                row.get_typed::<Option<String>>(2)?, // title
                row.get_typed::<i64>(3)?,            // started_at
                row.get_typed::<Option<i64>>(4)?,    // ended_at
                row.get_typed::<String>(5)?,         // source_path
                row.get_typed::<i64>(6)?,            // message_count
                row.get_typed::<String>(7)?,         // source_id (P3.2)
                row.get_typed::<Option<String>>(8)?, // origin_host (P3.5)
                row.get_typed::<Option<String>>(9)?, // origin_kind (P3.5)
            ))
        })
        .map_err(|e| CliError {
            code: 9,
            kind: CliErrorKind::DbQuery.kind_str(),
            message: format!("Query failed: {e}"),
            hint: None,
            retryable: false,
        })?;
    #[allow(clippy::type_complexity)]
    let mut sessions: Vec<(
        i64,
        String,
        Option<String>,
        i64,
        Option<i64>,
        String,
        i64,
        String,
        Option<String>,
        Option<String>,
    )> = rows;
    let has_conv_idx = conn
        .query_map_collect(
            "PRAGMA index_list(messages)",
            &[],
            |row: &crate::franken_sync::Row| row.get_typed::<String>(1),
        )
        .map(|names| names.iter().any(|name| name == "idx_messages_conv_idx"))
        .unwrap_or(false);
    let count_sql = if has_conv_idx {
        "SELECT idx FROM messages INDEXED BY idx_messages_conv_idx
         WHERE conversation_id = ?1"
    } else {
        "SELECT idx FROM messages
         WHERE conversation_id = ?1"
    };
    for session in &mut sessions {
        // COUNT(*) with a WHERE clause also scans the whole messages table in
        // this engine version (see run_sessions). Stream the covering index's
        // idx column instead: no message bodies or per-session Vec of rows,
        // even for a single conversation with hundreds of thousands of turns.
        conn.query_with_params_for_each(
            count_sql,
            &[crate::franken_sync::SqliteValue::Integer(session.0)],
            |_| {
                session.6 += 1;
                Ok(())
            },
        )
        .map_err(|e| CliError {
            code: 9,
            kind: CliErrorKind::DbQuery.kind_str(),
            message: format!("Failed to count timeline messages: {e}"),
            hint: None,
            retryable: false,
        })?;
    }
    snapshot.commit().map_err(|e| CliError {
        code: 9,
        kind: CliErrorKind::DbQuery.kind_str(),
        message: format!("Failed to finish timeline snapshot: {e}"),
        hint: None,
        retryable: false,
    })?;
    drop(snapshot);
    close_franken_cli_read_db(conn, &db_path, "timeline")?;

    let structured_format = output_format.or_else(robot_format_from_env).map(|fmt| {
        if matches!(fmt, RobotFormat::Sessions) {
            RobotFormat::Compact
        } else {
            fmt
        }
    });

    // GH#521: serialize borrowed summaries directly, rather than cloning all
    // selected title/path strings into a second JSON Value tree. Keep the
    // other robot formats on their existing projection/formatting path.
    if let Some(fmt) = structured_format
        && matches!(fmt, RobotFormat::Json | RobotFormat::Compact)
    {
        let stdout = std::io::stdout();
        let mut writer = std::io::BufWriter::new(stdout.lock());
        timeline_json::write(
            &mut writer,
            &sessions,
            start_ts,
            end_ts,
            group_by,
            matches!(fmt, RobotFormat::Json),
        )
        .and_then(|()| writer.flush())
        .map_err(|error| CliError {
            code: 9,
            kind: CliErrorKind::EncodeJson.kind_str(),
            message: format!("failed to write structured output: {error}"),
            hint: None,
            retryable: false,
        })?;
        return Ok(());
    }

    if let Some(fmt) = structured_format {
        let output = match group_by {
            TimelineGrouping::None => {
                let items: Vec<serde_json::Value> = sessions
                    .iter()
                    .map(
                        |(
                            id,
                            agent,
                            title,
                            started,
                            ended,
                            path,
                            msg_count,
                            source_id,
                            origin_host,
                            origin_kind,
                        )| {
                            let duration_ms = ended.map(|e| e - started);
                            let duration_secs = duration_ms.map(|ms| ms / 1000);
                            let normalized_source_id = normalized_provenance_source_id(
                                source_id,
                                origin_kind.as_deref(),
                                origin_host.as_deref(),
                            );
                            let normalized_origin_kind = normalized_provenance_origin_kind(
                                normalized_source_id.as_str(),
                                origin_kind.as_deref(),
                            );
                            let normalized_origin_host =
                                normalized_provenance_origin_host(origin_host.as_deref());
                            serde_json::json!({
                                "id": id, "agent": agent, "title": title,
                                "started_at": started, "ended_at": ended,
                                "duration_seconds": duration_secs, "source_path": path,
                                "message_count": msg_count,
                                // Provenance fields (P3.5)
                                "source_id": normalized_source_id,
                                "origin_kind": normalized_origin_kind,
                                "origin_host": normalized_origin_host,
                            })
                        },
                    )
                    .collect();
                serde_json::json!({
                    "range": { "start": start_ts, "end": end_ts },
                    "total_sessions": sessions.len(),
                    "sessions": items,
                })
            }
            TimelineGrouping::Hour | TimelineGrouping::Day => {
                let mut groups: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
                for (
                    id,
                    agent,
                    title,
                    started,
                    ended,
                    path,
                    msg_count,
                    source_id,
                    origin_host,
                    origin_kind,
                ) in &sessions
                {
                    let dt = Utc
                        .timestamp_millis_opt(*started)
                        .single()
                        .unwrap_or_else(Utc::now);
                    let key = match group_by {
                        TimelineGrouping::Hour => dt.format("%Y-%m-%d %H:00").to_string(),
                        TimelineGrouping::Day => dt.format("%Y-%m-%d").to_string(),
                        _ => unreachable!(),
                    };
                    let normalized_source_id = normalized_provenance_source_id(
                        source_id,
                        origin_kind.as_deref(),
                        origin_host.as_deref(),
                    );
                    let normalized_origin_kind = normalized_provenance_origin_kind(
                        normalized_source_id.as_str(),
                        origin_kind.as_deref(),
                    );
                    let normalized_origin_host =
                        normalized_provenance_origin_host(origin_host.as_deref());
                    groups.entry(key).or_default().push(serde_json::json!({
                        "id": id, "agent": agent, "title": title,
                        "started_at": started, "ended_at": ended,
                        "source_path": path, "message_count": msg_count,
                        // Provenance fields (P3.5)
                        "source_id": normalized_source_id,
                        "origin_kind": normalized_origin_kind,
                        "origin_host": normalized_origin_host,
                    }));
                }
                serde_json::json!({
                    "range": { "start": start_ts, "end": end_ts },
                    "total_sessions": sessions.len(),
                    "groups": groups,
                })
            }
        };
        return output_structured_value(output, fmt);
    }

    let start_dt = Utc
        .timestamp_millis_opt(start_ts)
        .single()
        .unwrap_or_else(Utc::now);
    let end_dt = Utc
        .timestamp_millis_opt(end_ts)
        .single()
        .unwrap_or_else(Utc::now);

    println!("\n📅 Activity Timeline");
    println!(
        "   {} to {}",
        start_dt.format("%Y-%m-%d %H:%M"),
        end_dt.format("%Y-%m-%d %H:%M")
    );
    println!("{}", "─".repeat(70));

    if sessions.is_empty() {
        println!("\n   No sessions found in this time range.\n");
        return Ok(());
    }

    let mut current_group = String::new();
    for (
        _id,
        agent,
        title,
        started,
        ended,
        _path,
        msg_count,
        source_id,
        origin_host,
        origin_kind,
    ) in &sessions
    {
        let dt = Utc
            .timestamp_millis_opt(*started)
            .single()
            .unwrap_or_else(Utc::now);

        let group_key = match group_by {
            TimelineGrouping::Hour => dt.format("%Y-%m-%d %H:00").to_string(),
            TimelineGrouping::Day => dt.format("%Y-%m-%d (%A)").to_string(),
            TimelineGrouping::None => String::new(),
        };

        if group_key != current_group && group_by != TimelineGrouping::None {
            println!("\n  📆 {}", group_key);
            current_group = group_key;
        }

        let duration = ended.map(|e| {
            // Timestamps are in milliseconds, divide by 60_000 to get minutes.
            // Guard against negative durations from clock skew or corrupt data.
            let mins = (e - started).max(0) / 60_000;
            if mins < 60 {
                format!("{}m", mins)
            } else {
                format!("{}h{}m", mins / 60, mins % 60)
            }
        });

        let title_str = title.as_deref().unwrap_or("(untitled)");
        let title_preview: String = title_str.chars().take(40).collect();

        let agent_icon = match agent.as_str() {
            "claude_code" => "🟣",
            "codex" => "🟢",
            "gemini" => "🔵",
            "amp" => "🟡",
            "cursor" => "⚪",
            "pi_agent" => "🟠",
            "omp" => "🟤",
            _ => "⚫",
        };

        // Source badge for remote sessions (P3.2, P3.5)
        // Prefer origin_host if available, otherwise use normalized source_id.
        let normalized_source_id = normalized_provenance_source_id(
            source_id,
            origin_kind.as_deref(),
            origin_host.as_deref(),
        );
        let normalized_origin_kind = normalized_provenance_origin_kind(
            normalized_source_id.as_str(),
            origin_kind.as_deref(),
        );
        let source_badge = if normalized_origin_kind != crate::sources::provenance::LOCAL_SOURCE_ID
        {
            let label = origin_host
                .as_deref()
                .map(str::trim)
                .filter(|label| !label.is_empty())
                .unwrap_or(normalized_source_id.as_str());
            if label.is_empty() {
                String::new()
            } else {
                format!(" [{}]", label)
            }
        } else {
            String::new()
        };

        println!(
            "     {} {} {:>5} │ {:>3} msgs │ {}{}",
            dt.format("%H:%M"),
            agent_icon,
            duration.as_deref().unwrap_or(""),
            msg_count,
            title_preview,
            source_badge
        );
    }

    println!("\n{}", "─".repeat(70));
    println!("   Total: {} sessions\n", sessions.len());
    Ok(())
}
