use super::*;
use crate::franken_sync::Connection;
use crate::pages::secret_scan::SecretScanSummary;
use chrono::DateTime;
use tempfile::TempDir;

fn create_test_db() -> (TempDir, Connection) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let conn = Connection::open(db_path.to_string_lossy().as_ref()).unwrap();

    conn.execute_batch(
        "CREATE TABLE agents (
            id INTEGER PRIMARY KEY,
            slug TEXT NOT NULL UNIQUE
        );
        CREATE TABLE workspaces (
            id INTEGER PRIMARY KEY,
            path TEXT NOT NULL UNIQUE
        );
        CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent_id INTEGER NOT NULL,
            workspace_id INTEGER,
            title TEXT,
            source_path TEXT NOT NULL,
            started_at INTEGER,
            ended_at INTEGER,
            message_count INTEGER,
            metadata_json TEXT,
            FOREIGN KEY (agent_id) REFERENCES agents(id),
            FOREIGN KEY (workspace_id) REFERENCES workspaces(id)
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            idx INTEGER NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at INTEGER,
            FOREIGN KEY (conversation_id) REFERENCES conversations(id)
        );",
    )
    .unwrap();

    (dir, conn)
}

fn create_test_db_without_message_count() -> (TempDir, Connection) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test-no-message-count.db");
    let conn = Connection::open(db_path.to_string_lossy().as_ref()).unwrap();

    conn.execute_batch(
        "CREATE TABLE agents (
            id INTEGER PRIMARY KEY,
            slug TEXT NOT NULL UNIQUE
        );
        CREATE TABLE workspaces (
            id INTEGER PRIMARY KEY,
            path TEXT NOT NULL UNIQUE
        );
        CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            agent_id INTEGER NOT NULL,
            workspace_id INTEGER,
            title TEXT,
            source_path TEXT NOT NULL,
            started_at INTEGER,
            ended_at INTEGER,
            metadata_json TEXT,
            FOREIGN KEY (agent_id) REFERENCES agents(id),
            FOREIGN KEY (workspace_id) REFERENCES workspaces(id)
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            idx INTEGER NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at INTEGER,
            FOREIGN KEY (conversation_id) REFERENCES conversations(id)
        );",
    )
    .unwrap();

    (dir, conn)
}

fn insert_test_data(conn: &Connection) {
    use crate::franken_sync::compat::ConnectionExt;
    use crate::franken_sync::params;

    conn.execute("INSERT INTO agents (id, slug) VALUES (1, 'claude-code');")
        .unwrap();
    conn.execute("INSERT INTO agents (id, slug) VALUES (2, 'aider');")
        .unwrap();
    conn.execute("INSERT INTO workspaces (id, path) VALUES (1, '/home/user/project-a');")
        .unwrap();
    conn.execute("INSERT INTO workspaces (id, path) VALUES (2, '/home/user/project-b');")
        .unwrap();

    // Insert conversations
    conn.execute(
        "INSERT INTO conversations (id, agent_id, workspace_id, title, source_path, started_at)
         VALUES (1, 1, 1, 'Fix authentication bug', '/path/a.jsonl', 1700000000000);",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO conversations (id, agent_id, workspace_id, title, source_path, started_at)
         VALUES (2, 1, 1, 'Add user profile', '/path/b.jsonl', 1700100000000);",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO conversations (id, agent_id, workspace_id, title, source_path, started_at)
         VALUES (3, 2, 2, 'Setup database', '/path/c.jsonl', 1700200000000);",
    )
    .unwrap();

    // Insert messages
    for conv_id in 1..=3i64 {
        let msg_count = match conv_id {
            1 => 5,
            2 => 3,
            3 => 4,
            _ => 0,
        };
        for idx in 0..msg_count {
            let role = if idx % 2 == 0 { "user" } else { "assistant" };
            let created_at = 1700000000000i64 + (conv_id * 100000000) + (idx as i64 * 1000);
            conn.execute_compat(
                "INSERT INTO messages (conversation_id, idx, role, content, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    conv_id,
                    idx as i64,
                    role,
                    format!("Test message {} for conversation {}", idx, conv_id),
                    created_at
                ],
            )
            .unwrap();
        }
    }
}

#[test]
fn test_summary_generation() {
    let (_dir, conn) = create_test_db();
    insert_test_data(&conn);

    let generator = SummaryGenerator::new(&conn);
    let summary = generator.generate(None).unwrap();

    assert_eq!(summary.total_conversations, 3);
    assert_eq!(summary.total_messages, 12);
    assert!(summary.total_characters > 0);
    assert_eq!(summary.workspaces.len(), 2);
    assert_eq!(summary.agents.len(), 2);
}

#[test]
fn test_summary_generation_without_conversation_message_count_column() {
    let (_dir, conn) = create_test_db_without_message_count();
    insert_test_data(&conn);

    let generator = SummaryGenerator::new(&conn);
    let summary = generator.generate(None).unwrap();

    assert_eq!(summary.total_conversations, 3);
    assert_eq!(summary.total_messages, 12);
    assert_eq!(summary.workspaces.len(), 2);
    assert_eq!(summary.agents.len(), 2);

    let project_a = summary
        .workspaces
        .iter()
        .find(|w| w.path == "/home/user/project-a")
        .unwrap();
    assert_eq!(project_a.message_count, 8);

    let claude = summary
        .agents
        .iter()
        .find(|a| a.name.as_str().eq("claude-code"))
        .unwrap();
    assert_eq!(claude.message_count, 8);
}

#[test]
fn test_summary_with_filters() {
    let (_dir, conn) = create_test_db();
    insert_test_data(&conn);

    let filters = SummaryFilters {
        agents: Some(vec!["claude-code".to_string()]),
        ..Default::default()
    };

    let generator = SummaryGenerator::new(&conn);
    let summary = generator.generate(Some(&filters)).unwrap();

    assert_eq!(summary.total_conversations, 2);
    assert_eq!(summary.total_messages, 8); // 5 + 3
}

#[test]
fn test_summary_with_empty_agent_filter_matches_nothing() {
    let (_dir, conn) = create_test_db();
    insert_test_data(&conn);

    let filters = SummaryFilters {
        agents: Some(vec![]),
        ..Default::default()
    };

    let generator = SummaryGenerator::new(&conn);
    let summary = generator.generate(Some(&filters)).unwrap();

    assert_eq!(summary.total_conversations, 0);
    assert_eq!(summary.total_messages, 0);
    assert!(summary.workspaces.is_empty());
    assert!(summary.agents.is_empty());
}

#[test]
fn test_summary_with_empty_workspace_filter_matches_nothing() {
    let (_dir, conn) = create_test_db();
    insert_test_data(&conn);

    let filters = SummaryFilters {
        workspaces: Some(vec![]),
        ..Default::default()
    };

    let generator = SummaryGenerator::new(&conn);
    let summary = generator.generate(Some(&filters)).unwrap();

    assert_eq!(summary.total_conversations, 0);
    assert_eq!(summary.total_messages, 0);
    assert!(summary.workspaces.is_empty());
    assert!(summary.agents.is_empty());
}

#[test]
fn test_workspace_summary_message_counts_respect_time_filter() {
    let (_dir, conn) = create_test_db();
    insert_test_data(&conn);

    let filters = SummaryFilters {
        since_ts: Some(1_700_050_000_000),
        ..Default::default()
    };

    let generator = SummaryGenerator::new(&conn);
    let summary = generator.generate(Some(&filters)).unwrap();

    let project_a = summary
        .workspaces
        .iter()
        .find(|w| w.path == "/home/user/project-a")
        .unwrap();
    assert_eq!(project_a.conversation_count, 1);
    assert_eq!(project_a.message_count, 3);
    assert!(
        project_a
            .sample_titles
            .iter()
            .all(|t| t != "Fix authentication bug")
    );
}

#[test]
fn test_agent_summary_message_counts_respect_time_filter() {
    let (_dir, conn) = create_test_db();
    insert_test_data(&conn);

    let filters = SummaryFilters {
        since_ts: Some(1_700_050_000_000),
        ..Default::default()
    };

    let generator = SummaryGenerator::new(&conn);
    let summary = generator.generate(Some(&filters)).unwrap();

    let claude = summary
        .agents
        .iter()
        .find(|a| a.name.as_str().eq("claude-code"))
        .unwrap();
    assert_eq!(claude.conversation_count, 1);
    assert_eq!(claude.message_count, 3);
}

#[test]
fn test_workspace_summary() {
    let (_dir, conn) = create_test_db();
    insert_test_data(&conn);

    let generator = SummaryGenerator::new(&conn);
    let summary = generator.generate(None).unwrap();

    let project_a = summary
        .workspaces
        .iter()
        .find(|w| w.path.contains("project-a"));
    assert!(project_a.is_some());
    let project_a = project_a.unwrap();
    assert_eq!(project_a.conversation_count, 2);
    assert_eq!(project_a.display_name, "project-a");
    assert!(!project_a.sample_titles.is_empty());
}

#[test]
fn test_agent_summary() {
    let (_dir, conn) = create_test_db();
    insert_test_data(&conn);

    let generator = SummaryGenerator::new(&conn);
    let summary = generator.generate(None).unwrap();

    let claude = summary
        .agents
        .iter()
        .find(|a| a.name.as_str().eq("claude-code"));
    assert!(claude.is_some());
    let claude = claude.unwrap();
    assert_eq!(claude.conversation_count, 2);
    assert!((claude.percentage - 66.67).abs() < 1.0);
}

#[test]
fn test_date_histogram() {
    let (_dir, conn) = create_test_db();
    insert_test_data(&conn);

    let generator = SummaryGenerator::new(&conn);
    let summary = generator.generate(None).unwrap();

    // Each conversation is on a different day
    assert!(!summary.date_histogram.is_empty());
}

#[test]
fn test_exclusion_set() {
    let mut exclusions = ExclusionSet::new();

    exclusions.exclude_workspace("/home/user/project-a");
    assert!(exclusions.is_workspace_excluded("/home/user/project-a"));
    assert!(!exclusions.is_workspace_excluded("/home/user/project-b"));

    exclusions.exclude_conversation(42);
    assert!(exclusions.is_conversation_excluded(42));
    assert!(!exclusions.is_conversation_excluded(43));

    exclusions.add_pattern("(?i)secret").unwrap();
    assert!(exclusions.is_excluded("This is a Secret task"));
    assert!(!exclusions.is_excluded("This is a normal task"));
}

#[test]
fn test_exclusion_should_exclude() {
    let mut exclusions = ExclusionSet::new();
    exclusions.exclude_workspace("/excluded");
    exclusions.exclude_conversation(99);
    exclusions.add_pattern("^Private:").unwrap();

    // Excluded by workspace
    assert!(exclusions.should_exclude(Some("/excluded"), 1, "Normal title"));
    // Excluded by conversation ID
    assert!(exclusions.should_exclude(Some("/normal"), 99, "Normal title"));
    // Excluded by pattern
    assert!(exclusions.should_exclude(Some("/normal"), 1, "Private: Secret stuff"));
    // Not excluded
    assert!(!exclusions.should_exclude(Some("/normal"), 1, "Normal title"));
}

#[test]
fn test_summary_with_exclusions() {
    let (_dir, conn) = create_test_db();
    insert_test_data(&conn);

    let mut exclusions = ExclusionSet::new();
    exclusions.exclude_workspace("/home/user/project-b");

    let generator = SummaryGenerator::new(&conn);
    let summary = generator
        .generate_with_exclusions(None, &exclusions)
        .unwrap();

    // project-b should be marked as not included
    let project_b = summary
        .workspaces
        .iter()
        .find(|w| w.path.contains("project-b"));
    assert!(project_b.is_some());
    let project_b = project_b.unwrap();
    assert!(!project_b.included);
    assert_eq!(project_b.conversation_count, 1);
    assert_eq!(project_b.message_count, 4);

    let project_a = summary
        .workspaces
        .iter()
        .find(|w| w.path.contains("project-a"))
        .unwrap();
    assert!(project_a.included);
    assert_eq!(project_a.conversation_count, 2);
    assert_eq!(project_a.message_count, 8);
    assert_eq!(summary.total_conversations, 2);
    assert_eq!(summary.total_messages, 8);
    assert_eq!(summary.agents.len(), 1);
    assert_eq!(summary.agents[0].name, "claude-code");
    assert_eq!(summary.agents[0].conversation_count, 2);
    assert!((summary.agents[0].percentage - 100.0).abs() < f64::EPSILON);
    assert_eq!(
        summary
            .date_histogram
            .iter()
            .map(|day| day.message_count)
            .sum::<usize>(),
        8
    );
    assert_eq!(
        summary.latest_timestamp,
        DateTime::from_timestamp_millis(1_700_100_000_000)
    );
}

#[test]
fn test_size_estimation() {
    let size = estimate_compressed_size(1_000_000);
    // Should be roughly 40% * 1.05 = 42% of original
    assert!(size > 400_000);
    assert!(size < 450_000);
}

#[test]
fn test_format_size() {
    assert_eq!(format_size(500), "500 bytes");
    assert_eq!(format_size(1500), "1.5 KB");
    assert_eq!(format_size(1_500_000), "1.4 MB");
    assert_eq!(format_size(1_500_000_000), "1.4 GB");
}

#[test]
fn test_date_range() {
    let range = DateRange::from_timestamps(Some(1700000000000), Some(1700100000000));
    assert!(range.earliest.is_some());
    assert!(range.latest.is_some());
    assert!(range.span_days().unwrap() >= 1);
}

#[test]
fn test_scan_report_summary() {
    let summary = ScanReportSummary::default();
    assert_eq!(summary.total_findings, 0);
    assert!(!summary.has_critical);
    assert!(!summary.truncated);
}

#[test]
fn truncated_scan_summary_is_never_presented_as_clean() {
    let scan = SecretScanSummary {
        total: 0,
        by_severity: Default::default(),
        has_critical: false,
        truncated: true,
    };

    let summary = ScanReportSummary::from_summary(&scan);

    assert!(summary.truncated);
    assert!(!summary.status_message.contains("No secrets detected"));
    assert!(summary.status_message.contains("incomplete"));
}

#[test]
fn test_encryption_summary() {
    let enc = EncryptionSummary::default();
    assert_eq!(enc.algorithm, "none");
    assert_eq!(enc.key_derivation, "none");
}

#[test]
fn test_render_overview() {
    let (_dir, conn) = create_test_db();
    insert_test_data(&conn);

    let generator = SummaryGenerator::new(&conn);
    let summary = generator.generate(None).unwrap();
    let overview = summary.render_overview();

    assert!(overview.contains("CONTENT OVERVIEW"));
    assert!(overview.contains("Conversations: 3"));
    assert!(overview.contains("WORKSPACES"));
    assert!(overview.contains("AGENTS"));
    assert!(overview.contains("SECURITY"));
}

#[test]
fn test_empty_database() {
    let (_dir, conn) = create_test_db();
    // Don't insert any data

    let generator = SummaryGenerator::new(&conn);
    let summary = generator.generate(None).unwrap();

    assert_eq!(summary.total_conversations, 0);
    assert_eq!(summary.total_messages, 0);
    assert_eq!(summary.total_characters, 0);
    assert!(summary.workspaces.is_empty());
    assert!(summary.agents.is_empty());
}

#[test]
fn test_exclusion_compile_patterns() {
    let mut exclusions = ExclusionSet::new();
    exclusions.excluded_pattern_strings = vec!["test.*pattern".to_string()];

    exclusions.compile_patterns().unwrap();

    assert_eq!(exclusions.excluded_patterns.len(), 1);
    assert!(exclusions.is_excluded("test123pattern"));
}

#[test]
fn exclusion_set_round_trip_recompiles_patterns() {
    let mut exclusions = ExclusionSet::new();
    exclusions.exclude_workspace("/private/project");
    exclusions.exclude_conversation(41);
    exclusions.add_pattern("(?i)^private:").unwrap();

    let encoded = serde_json::to_string(&exclusions).unwrap();
    let decoded: ExclusionSet = serde_json::from_str(&encoded).unwrap();

    assert!(decoded.should_exclude(Some("/private/project"), 1, "public"));
    assert!(decoded.should_exclude(Some("/public/project"), 41, "public"));
    assert!(decoded.should_exclude(Some("/public/project"), 1, "Private: notes"));
    assert_eq!(decoded.exclusion_counts(), (1, 1, 1));
    assert!(decoded.has_exclusions());
}

#[test]
fn exclusion_set_deserialization_rejects_invalid_patterns() {
    let error = serde_json::from_str::<ExclusionSet>(r#"{"excluded_pattern_strings":["["]}"#)
        .expect_err("an invalid persisted exclusion must fail closed");

    assert!(
        error.to_string().contains("Invalid exclusion pattern"),
        "unexpected deserialization error: {error}"
    );
}

#[test]
fn exclusion_pattern_recompile_is_atomic_on_error() {
    let mut exclusions = ExclusionSet::new();
    exclusions.add_pattern("^keep$").unwrap();
    exclusions.excluded_pattern_strings.push("[".to_string());

    exclusions
        .compile_patterns()
        .expect_err("invalid replacement patterns must be rejected");

    assert_eq!(exclusions.excluded_patterns.len(), 1);
    assert!(exclusions.is_excluded("keep"));
}

#[test]
fn test_key_slot_type_label() {
    assert_eq!(KeySlotType::Password.label(), "Password");
    assert_eq!(KeySlotType::QrCode.label(), "QR Code");
    assert_eq!(KeySlotType::Recovery.label(), "Recovery Key");
}

#[test]
fn test_exclusion_recount_keeps_workspace_less_conversations() {
    let (_dir, conn) = create_test_db();

    // Conversation without workspace should still be counted when exclusions
    // are active but do not match this conversation.
    conn.execute("INSERT INTO agents (id, slug) VALUES (3, 'codex');")
        .unwrap();
    conn.execute(
        "INSERT INTO conversations (id, agent_id, workspace_id, title, source_path, started_at, message_count)
         VALUES (10, 3, NULL, 'General session', '/path/no-workspace.jsonl', 1700300000000, 1);",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO messages (conversation_id, idx, role, content, created_at)
         VALUES (10, 0, 'user', 'Workspace-less message', 1700300001000);",
    )
    .unwrap();

    let mut exclusions = ExclusionSet::new();
    exclusions.add_pattern("^DOES_NOT_MATCH$").unwrap();

    let generator = SummaryGenerator::new(&conn);
    let summary = generator
        .generate_with_exclusions(None, &exclusions)
        .unwrap();

    assert_eq!(summary.total_conversations, 1);
    assert_eq!(summary.total_messages, 1);
    assert!(summary.workspaces.is_empty());
}

#[test]
fn test_exclusion_recount_keeps_workspace_less_conversations_with_other_workspaces() {
    let (_dir, conn) = create_test_db();
    insert_test_data(&conn);

    conn.execute("INSERT INTO agents (id, slug) VALUES (3, 'codex');")
        .unwrap();
    conn.execute(
        "INSERT INTO conversations (id, agent_id, workspace_id, title, source_path, started_at, message_count)
         VALUES (10, 3, NULL, 'General session', '/path/no-workspace.jsonl', 1700300000000, 1);",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO messages (conversation_id, idx, role, content, created_at)
         VALUES (10, 0, 'user', 'Workspace-less message', 1700300001000);",
    )
    .unwrap();

    let mut exclusions = ExclusionSet::new();
    exclusions.add_pattern("^DOES_NOT_MATCH$").unwrap();

    let generator = SummaryGenerator::new(&conn);
    let summary = generator
        .generate_with_exclusions(None, &exclusions)
        .unwrap();

    assert_eq!(summary.total_conversations, 4);
    assert_eq!(summary.total_messages, 13);
    assert!(
        summary
            .agents
            .iter()
            .any(|agent| agent.name.as_str().eq("codex"))
    );
}

#[test]
fn test_exclusion_recount_respects_active_filters() {
    let (_dir, conn) = create_test_db();
    insert_test_data(&conn);

    // Restrict to a single claude-code conversation in project-a.
    let filters = SummaryFilters {
        agents: Some(vec!["claude-code".to_string()]),
        since_ts: Some(1_700_050_000_000),
        ..Default::default()
    };

    let generator = SummaryGenerator::new(&conn);
    let baseline = generator.generate(Some(&filters)).unwrap();
    assert_eq!(baseline.total_conversations, 1);
    assert_eq!(baseline.total_messages, 3);

    // Trigger recount path without excluding any actual rows.
    let mut exclusions = ExclusionSet::new();
    exclusions.add_pattern("^DOES_NOT_MATCH$").unwrap();
    let summary = generator
        .generate_with_exclusions(Some(&filters), &exclusions)
        .unwrap();

    assert_eq!(summary.total_conversations, 1);
    assert_eq!(summary.total_messages, 3);
}

#[test]
fn test_pattern_exclusion_recounts_breakdowns() {
    let (_dir, conn) = create_test_db();
    insert_test_data(&conn);

    let mut exclusions = ExclusionSet::new();
    exclusions.add_pattern("^Add user profile$").unwrap();

    let generator = SummaryGenerator::new(&conn);
    let summary = generator
        .generate_with_exclusions(None, &exclusions)
        .unwrap();

    assert_eq!(summary.total_conversations, 2);
    assert_eq!(summary.total_messages, 9);
    assert_eq!(
        summary
            .date_histogram
            .iter()
            .map(|day| day.message_count)
            .sum::<usize>(),
        9
    );

    let project_a = summary
        .workspaces
        .iter()
        .find(|w| w.path == "/home/user/project-a")
        .unwrap();
    assert!(project_a.included);
    assert_eq!(project_a.conversation_count, 1);
    assert_eq!(project_a.message_count, 5);
    assert!(
        project_a
            .sample_titles
            .iter()
            .all(|title| title != "Add user profile")
    );

    let claude = summary
        .agents
        .iter()
        .find(|agent| agent.name.as_str().eq("claude-code"))
        .unwrap();
    assert_eq!(claude.conversation_count, 1);
    assert_eq!(claude.message_count, 5);
    assert!((claude.percentage - 50.0).abs() < f64::EPSILON);

    let aider = summary
        .agents
        .iter()
        .find(|agent| agent.name.as_str().eq("aider"))
        .unwrap();
    assert_eq!(aider.conversation_count, 1);
    assert_eq!(aider.message_count, 4);
    assert!((aider.percentage - 50.0).abs() < f64::EPSILON);
}
