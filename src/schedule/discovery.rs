//! Read-only diagnostics for legacy scheduling knobs left in `sources.toml`.
//!
//! The scheduler performs no remote synchronization: `sync_schedule` is a
//! dead field that nothing executes. The file is only inspected — never used
//! to plan work and never written — so `schedule status` can tell the
//! operator how many sources still carry the ignored setting.

use std::path::Path;

use crate::sources::config::{SourcesConfig, SyncSchedule};

/// Warn when `sources.toml` still carries `sync_schedule`. Remote
/// synchronization was removed, so every value is ignored; the scheduler only
/// indexes the local archive. Returns the notice `schedule status` should
/// surface, or `None` when there is nothing to report — no configuration, or
/// no non-`manual` `sync_schedule`. An unreadable configuration still earns a
/// notice: whatever it might set is ignored all the same. The file is never
/// created, rewritten, or repaired by this diagnostic.
pub(super) fn legacy_sync_schedule_notice() -> Option<String> {
    // The test-isolation switch means "act as if sources.toml does not
    // exist"; a diagnostic honors it like every other consumer does.
    if dotenvy::var("CASS_IGNORE_SOURCES_CONFIG").is_ok() {
        return None;
    }
    let path = SourcesConfig::config_path().ok()?;
    legacy_sync_schedule_notice_at(&path)
}

fn legacy_sync_schedule_notice_at(path: &Path) -> Option<String> {
    match SourcesConfig::load_from(&path.to_path_buf()) {
        Ok(config) => {
            let count = config
                .sources
                .iter()
                .filter(|source| !matches!(source.sync_schedule, SyncSchedule::Manual))
                .count();
            (count > 0).then(|| {
                format!(
                    "ignoring deprecated sync_schedule on {count} source{}; remote synchronization was removed, only local indexing is scheduled",
                    if count == 1 { "" } else { "s" }
                )
            })
        }
        Err(_) => Some(
            "cannot read sources.toml; any sync_schedule it sets is ignored — remote synchronization was removed, only local indexing is scheduled".to_string(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const AUTOMATIC: &str = "[[sources]]\nname = 'fixture'\ntype = 'ssh'\nhost = 'fixture.invalid'\npaths = ['~/.claude/projects']\nsync_schedule = 'hourly'\n";

    #[test]
    fn missing_and_manual_only_configurations_report_nothing() {
        let directory = tempfile::tempdir().unwrap();
        let config = directory.path().join("sources.toml");
        assert_eq!(legacy_sync_schedule_notice_at(&config), None);
        assert!(!config.exists());
        std::fs::write(&config, AUTOMATIC.replace("hourly", "manual")).unwrap();
        assert_eq!(legacy_sync_schedule_notice_at(&config), None);
        assert_eq!(
            std::fs::read_to_string(&config).unwrap(),
            AUTOMATIC.replace("hourly", "manual")
        );
    }

    #[test]
    fn every_non_manual_sync_schedule_is_counted_without_touching_the_file() {
        let directory = tempfile::tempdir().unwrap();
        let config = directory.path().join("sources.toml");
        std::fs::write(&config, AUTOMATIC).unwrap();
        let notice = legacy_sync_schedule_notice_at(&config).unwrap();
        assert!(notice.contains("sync_schedule"), "{notice}");
        assert!(notice.contains("1 source"), "{notice}");
        assert!(notice.contains("ignoring deprecated"), "{notice}");
        assert_eq!(std::fs::read_to_string(&config).unwrap(), AUTOMATIC);
        // Both automatic entries are counted; the manual one is not.
        std::fs::write(
            &config,
            format!(
                "{AUTOMATIC}[[sources]]\nname = 'second'\ntype = 'ssh'\nhost = 'fixture.invalid'\npaths = ['~/.codex']\nsync_schedule = 'daily'\n[[sources]]\nname = 'local-box'\ntype = 'local'\npaths = ['/x']\nsync_schedule = 'manual'\n"
            ),
        )
        .unwrap();
        let notice = legacy_sync_schedule_notice_at(&config).unwrap();
        assert!(notice.contains("2 sources"), "{notice}");
    }

    #[test]
    fn an_unreadable_configuration_is_still_an_ignored_setting() {
        let directory = tempfile::tempdir().unwrap();
        let config = directory.path().join("sources.toml");
        let bytes = "private-configuration-sentinel = [unterminated";
        std::fs::write(&config, bytes).unwrap();
        let notice = legacy_sync_schedule_notice_at(&config).unwrap();
        assert!(notice.contains("sync_schedule"), "{notice}");
        assert!(notice.contains("ignored"), "{notice}");
        assert!(notice.contains("cannot read"), "{notice}");
        // The diagnostic discloses nothing from the file it failed to parse.
        assert!(!notice.contains("private-configuration-sentinel"));
        assert_eq!(std::fs::read_to_string(&config).unwrap(), bytes);
    }
}
