use super::super::engine::{ExportEngine, ExportFilter, ExportStats, PathMode};
use chrono::{Datelike, TimeZone, Utc};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

#[test]
fn test_export_filter_default_values() {
    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Full,
    };

    assert!(filter.agents.is_none());
    assert!(filter.workspaces.is_none());
    assert!(filter.since.is_none());
    assert!(filter.until.is_none());
    assert_eq!(filter.path_mode, PathMode::Full);
}

#[test]
fn test_export_filter_with_agents() {
    let filter = ExportFilter {
        agents: Some(vec!["claude".to_string(), "codex".to_string()]),
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Relative,
    };

    let agents = filter.agents.as_ref().unwrap();
    assert_eq!(agents.len(), 2);
    assert!(agents.contains(&"claude".to_string()));
    assert!(agents.contains(&"codex".to_string()));
}

#[test]
fn test_export_filter_with_workspaces() {
    let filter = ExportFilter {
        agents: None,
        workspaces: Some(vec![
            PathBuf::from("/home/user/project1"),
            PathBuf::from("/home/user/project2"),
        ]),
        since: None,
        until: None,
        path_mode: PathMode::Basename,
    };

    let workspaces = filter.workspaces.as_ref().unwrap();
    assert_eq!(workspaces.len(), 2);
}

#[test]
fn test_export_filter_with_time_range() {
    let since = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let until = Utc.with_ymd_and_hms(2025, 12, 31, 23, 59, 59).unwrap();

    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: Some(since),
        until: Some(until),
        path_mode: PathMode::Hash,
    };

    assert_eq!(filter.since.unwrap().year(), 2025);
    assert_eq!(filter.until.unwrap().month(), 12);
}

#[test]
fn test_export_filter_clone() {
    let filter = ExportFilter {
        agents: Some(vec!["gemini".to_string()]),
        workspaces: Some(vec![PathBuf::from("/tmp/test")]),
        since: None,
        until: None,
        path_mode: PathMode::Full,
    };

    let cloned = filter.clone();
    assert_eq!(cloned.agents, filter.agents);
    assert_eq!(cloned.workspaces, filter.workspaces);
    assert_eq!(cloned.path_mode, filter.path_mode);
}

// ==================== PathMode tests ====================

#[test]
fn test_path_mode_equality() {
    assert_eq!(PathMode::Relative, PathMode::Relative);
    assert_eq!(PathMode::Basename, PathMode::Basename);
    assert_eq!(PathMode::Full, PathMode::Full);
    assert_eq!(PathMode::Hash, PathMode::Hash);
}

#[test]
fn test_path_mode_inequality() {
    assert_ne!(PathMode::Relative, PathMode::Full);
    assert_ne!(PathMode::Basename, PathMode::Hash);
    assert_ne!(PathMode::Full, PathMode::Relative);
}

#[test]
fn test_path_mode_clone() {
    let mode = PathMode::Hash;
    let cloned = mode;
    assert_eq!(mode, cloned);
}

#[test]
fn test_path_mode_copy() {
    let mode = PathMode::Relative;
    let copied: PathMode = mode;
    assert_eq!(copied, PathMode::Relative);
}

#[test]
fn test_path_mode_debug() {
    let debug_str = format!("{:?}", PathMode::Full);
    assert!(debug_str.contains("Full"));
}

// ==================== ExportEngine::new() tests ====================

#[test]
fn test_export_engine_new_stores_paths() {
    let source = Path::new("/tmp/source.db");
    let output = Path::new("/tmp/output.db");
    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Full,
    };

    let engine = ExportEngine::new(source, output, filter);

    assert_eq!(engine.source_db_path, PathBuf::from("/tmp/source.db"));
    assert_eq!(engine.output_path, PathBuf::from("/tmp/output.db"));
}

#[test]
fn test_export_engine_new_with_relative_paths() {
    let source = Path::new("relative/source.db");
    let output = Path::new("relative/output.db");
    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Basename,
    };

    let engine = ExportEngine::new(source, output, filter);

    assert_eq!(engine.source_db_path, PathBuf::from("relative/source.db"));
    assert_eq!(engine.output_path, PathBuf::from("relative/output.db"));
}

// ==================== ExportEngine::transform_path() tests ====================

#[test]
fn test_transform_path_full_mode() {
    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Full,
    };
    let engine = ExportEngine::new(Path::new("/tmp/s.db"), Path::new("/tmp/o.db"), filter);

    let result = engine.transform_path("/home/user/project/file.rs", &None);
    assert_eq!(result, "/home/user/project/file.rs");
}

#[test]
fn test_transform_path_full_mode_with_workspace() {
    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Full,
    };
    let engine = ExportEngine::new(Path::new("/tmp/s.db"), Path::new("/tmp/o.db"), filter);

    let workspace = Some("/home/user/project".to_string());
    let result = engine.transform_path("/home/user/project/src/main.rs", &workspace);
    // Full mode ignores workspace
    assert_eq!(result, "/home/user/project/src/main.rs");
}

#[test]
fn test_transform_path_basename_mode() {
    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Basename,
    };
    let engine = ExportEngine::new(Path::new("/tmp/s.db"), Path::new("/tmp/o.db"), filter);

    let result = engine.transform_path("/home/user/project/src/main.rs", &None);
    assert_eq!(result, "main.rs");
}

#[test]
fn test_transform_path_basename_mode_nested() {
    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Basename,
    };
    let engine = ExportEngine::new(Path::new("/tmp/s.db"), Path::new("/tmp/o.db"), filter);

    let result = engine.transform_path("/very/deep/nested/path/to/file.txt", &None);
    assert_eq!(result, "file.txt");
}

#[test]
fn test_transform_path_basename_mode_no_extension() {
    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Basename,
    };
    let engine = ExportEngine::new(Path::new("/tmp/s.db"), Path::new("/tmp/o.db"), filter);

    let result = engine.transform_path("/usr/bin/cargo", &None);
    assert_eq!(result, "cargo");
}

#[test]
fn test_transform_path_relative_mode_with_workspace() {
    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Relative,
    };
    let engine = ExportEngine::new(Path::new("/tmp/s.db"), Path::new("/tmp/o.db"), filter);

    let workspace = Some("/home/user/project".to_string());
    let result = engine.transform_path("/home/user/project/src/main.rs", &workspace);
    assert_eq!(result, "src/main.rs");
}

#[test]
fn test_transform_path_relative_mode_without_workspace() {
    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Relative,
    };
    let engine = ExportEngine::new(Path::new("/tmp/s.db"), Path::new("/tmp/o.db"), filter);

    let result = engine.transform_path("/home/user/project/src/main.rs", &None);
    // Without workspace, returns full path
    assert_eq!(result, "/home/user/project/src/main.rs");
}

#[test]
fn test_transform_path_relative_mode_path_not_under_workspace() {
    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Relative,
    };
    let engine = ExportEngine::new(Path::new("/tmp/s.db"), Path::new("/tmp/o.db"), filter);

    let workspace = Some("/home/user/project".to_string());
    let result = engine.transform_path("/other/path/file.rs", &workspace);
    // Path not under workspace, returns full path
    assert_eq!(result, "/other/path/file.rs");
}

#[test]
fn test_transform_path_relative_mode_strips_leading_slash() {
    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Relative,
    };
    let engine = ExportEngine::new(Path::new("/tmp/s.db"), Path::new("/tmp/o.db"), filter);

    let workspace = Some("/home/user".to_string());
    let result = engine.transform_path("/home/user/file.rs", &workspace);
    assert_eq!(result, "file.rs");
}

#[test]
fn test_transform_path_hash_mode() {
    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Hash,
    };
    let engine = ExportEngine::new(Path::new("/tmp/s.db"), Path::new("/tmp/o.db"), filter);

    let result = engine.transform_path("/home/user/project/file.rs", &None);
    // Hash should be 16 hex characters
    assert_eq!(result.len(), 16);
    assert!(result.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn test_transform_path_hash_mode_deterministic() {
    let filter1 = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Hash,
    };
    let engine1 = ExportEngine::new(Path::new("/tmp/s.db"), Path::new("/tmp/o.db"), filter1);

    let filter2 = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Hash,
    };
    let engine2 = ExportEngine::new(Path::new("/tmp/s.db"), Path::new("/tmp/o.db"), filter2);

    let path = "/home/user/project/file.rs";
    let result1 = engine1.transform_path(path, &None);
    let result2 = engine2.transform_path(path, &None);

    assert_eq!(result1, result2);
}

#[test]
fn test_transform_path_hash_mode_different_paths_different_hashes() {
    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Hash,
    };
    let engine = ExportEngine::new(Path::new("/tmp/s.db"), Path::new("/tmp/o.db"), filter);

    let result1 = engine.transform_path("/path/one/file.rs", &None);
    let result2 = engine.transform_path("/path/two/file.rs", &None);

    assert_ne!(result1, result2);
}

// ==================== ExportStats tests ====================

#[test]
fn test_export_stats_default_values() {
    let stats = ExportStats {
        conversations_processed: 0,
        messages_processed: 0,
    };

    assert_eq!(stats.conversations_processed, 0);
    assert_eq!(stats.messages_processed, 0);
}

#[test]
fn test_export_stats_with_values() {
    let stats = ExportStats {
        conversations_processed: 100,
        messages_processed: 5000,
    };

    assert_eq!(stats.conversations_processed, 100);
    assert_eq!(stats.messages_processed, 5000);
}

// ==================== Edge case tests ====================

#[test]
fn test_transform_path_empty_path() {
    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Full,
    };
    let engine = ExportEngine::new(Path::new("/tmp/s.db"), Path::new("/tmp/o.db"), filter);

    let result = engine.transform_path("", &None);
    assert_eq!(result, "");
}

#[test]
fn test_transform_path_basename_empty_returns_original() {
    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Basename,
    };
    let engine = ExportEngine::new(Path::new("/tmp/s.db"), Path::new("/tmp/o.db"), filter);

    // Empty path has no file_name
    let result = engine.transform_path("", &None);
    assert_eq!(result, "");
}

#[test]
fn test_transform_path_with_special_characters() {
    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Basename,
    };
    let engine = ExportEngine::new(Path::new("/tmp/s.db"), Path::new("/tmp/o.db"), filter);

    let result = engine.transform_path("/path/to/file with spaces.rs", &None);
    assert_eq!(result, "file with spaces.rs");
}

#[test]
fn test_transform_path_hash_with_unicode() {
    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Hash,
    };
    let engine = ExportEngine::new(Path::new("/tmp/s.db"), Path::new("/tmp/o.db"), filter);

    let result = engine.transform_path("/path/to/файл.rs", &None);
    // Should still produce valid 16-char hex hash
    assert_eq!(result.len(), 16);
    assert!(result.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn test_export_filter_empty_agents_list() {
    let filter = ExportFilter {
        agents: Some(vec![]),
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Full,
    };

    assert!(filter.agents.as_ref().unwrap().is_empty());
}

#[test]
fn test_export_filter_empty_workspaces_list() {
    let filter = ExportFilter {
        agents: None,
        workspaces: Some(vec![]),
        since: None,
        until: None,
        path_mode: PathMode::Full,
    };

    assert!(filter.workspaces.as_ref().unwrap().is_empty());
}

// ==================== Integration-style tests (with real temp files) ====================

#[test]
fn test_export_engine_new_with_tempdir() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let source = temp_dir.path().join("source.db");
    let output = temp_dir.path().join("output.db");

    let filter = ExportFilter {
        agents: None,
        workspaces: None,
        since: None,
        until: None,
        path_mode: PathMode::Full,
    };

    let engine = ExportEngine::new(&source, &output, filter);

    assert!(engine.source_db_path.starts_with(temp_dir.path()));
    assert!(engine.output_path.starts_with(temp_dir.path()));
}
