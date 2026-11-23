#[allow(unused_imports)]
pub(super) use super::*;

#[cfg(test)]
mod source_doctor_local_storage_tests {
    use super::*;

    #[test]
    fn existing_directory_check_is_explicitly_non_mutating() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source_dir = temp.path().join("fixture-source");
        std::fs::create_dir(&source_dir).expect("create source directory");
        let members_before = std::fs::read_dir(&source_dir)
            .expect("list source directory before check")
            .count();

        let check = check_local_storage_path(&source_dir);

        assert_eq!(check.status, "pass");
        assert!(check.message.contains("appears writable"));
        assert!(check.message.contains("no write attempted"));
        assert_eq!(
            std::fs::read_dir(&source_dir)
                .expect("list source directory after check")
                .count(),
            members_before
        );
    }

    #[test]
    fn missing_target_uses_nearest_existing_ancestor_without_creating_it() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source_dir = temp.path().join("remotes").join("fixture-source");

        let check = check_local_storage_path(&source_dir);

        assert_eq!(check.status, "pass");
        assert!(check.message.contains(&temp.path().display().to_string()));
        assert!(check.message.contains("no write attempted"));
        assert!(!source_dir.exists());
        assert!(!temp.path().join("remotes").exists());
    }

    #[test]
    fn non_directory_target_fails_without_writing() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source_dir = temp.path().join("fixture-source");
        std::fs::write(&source_dir, b"not a directory").expect("write fixture file");
        let contents_before = std::fs::read(&source_dir).expect("read fixture before check");

        let check = check_local_storage_path(&source_dir);

        assert_eq!(check.status, "fail");
        assert!(check.message.contains("is not a directory"));
        assert!(check.message.contains("no write attempted"));
        assert_eq!(
            std::fs::read(&source_dir).expect("read fixture after check"),
            contents_before
        );
    }
}

#[cfg(test)]
mod semantic_identity_backfill_checkpoint_tests {
    use super::semantic_identity_backfill_fingerprint;
    use crate::search::policy::{CHUNKING_STRATEGY_VERSION, SEMANTIC_SCHEMA_VERSION};
    use crate::search::semantic_manifest::{BuildCheckpoint, TierKind};

    fn fast_checkpoint() -> BuildCheckpoint {
        BuildCheckpoint {
            tier: TierKind::Fast,
            embedder_id: "fnv1a-384".to_string(),
            last_offset: 41,
            docs_embedded: 41,
            conversations_processed: 41,
            total_conversations: 82,
            db_fingerprint: "content-v1:82:82:820".to_string(),
            schema_version: SEMANTIC_SCHEMA_VERSION,
            chunking_version: CHUNKING_STRATEGY_VERSION,
            saved_at_ms: 1,
            last_message_id: Some(410),
            cursor_exhausted: false,
        }
    }

    #[test]
    fn identity_generation_invalidates_count_compatible_semantic_checkpoints() {
        let canonical = "content-v1:82:82:820";
        let checkpoint = fast_checkpoint();
        assert!(checkpoint.is_valid(canonical));

        let first_generation =
            semantic_identity_backfill_fingerprint(canonical, Some("ownership-context-a"));
        let second_generation =
            semantic_identity_backfill_fingerprint(canonical, Some("ownership-context-b"));
        assert!(
            !checkpoint.is_valid(&first_generation),
            "a count/max-id-compatible Pi-era checkpoint must not resume after an OMP identity rewrite"
        );
        assert_ne!(first_generation, second_generation);
        let mut first_generation_checkpoint = checkpoint.clone();
        first_generation_checkpoint.db_fingerprint = first_generation;
        assert!(
            !first_generation_checkpoint.is_valid(&second_generation),
            "a later ownership-context rewrite must invalidate an earlier partial identity rebuild"
        );
        assert_eq!(
            semantic_identity_backfill_fingerprint(canonical, None),
            canonical
        );
    }
}

/// Timeline and sessions parse their time flags with search's grammar
/// (parse_time_bound); these pin the relative forms they rely on.
#[cfg(test)]
mod timeline_datetime_parse_tests {
    use super::{TimeBound, parse_time_bound};

    #[test]
    fn signed_relative_offsets_use_magnitude() {
        let positive = parse_time_bound("7d", TimeBound::Since).expect("7d parses");
        let negative = parse_time_bound("-7d", TimeBound::Since).expect("-7d parses");

        assert!(
            positive.abs_diff(negative) < 2_000,
            "7d and -7d should resolve to the same past window, got {positive} vs {negative}"
        );
    }

    /// `--since 5m` used to fall back to the 7-day default with exit 0.
    #[test]
    fn minutes_are_minutes_ago() {
        let now = chrono::Utc::now().timestamp_millis();
        for raw in ["5m", "-5m", "5 minutes ago"] {
            let start = parse_time_bound(raw, TimeBound::Since).expect("minutes parse");
            assert!(
                (now - start - 300_000).abs() < 60_000,
                "{raw} should be five minutes ago, got {} ms before now",
                now - start
            );
        }
        assert!(parse_time_bound("5x", TimeBound::Since).is_none());
    }

    #[test]
    fn overflowing_relative_offsets_are_rejected_without_panicking() {
        assert!(parse_time_bound("-9223372036854775808d", TimeBound::Since).is_none());
        assert!(parse_time_bound("9223372036854775807h", TimeBound::Since).is_none());
    }
}

#[cfg(all(test, unix))]
mod daemon_cli_config_tests {
    use super::*;
    use serial_test::serial;
    use std::time::Duration;

    fn with_env_vars(vars: &[(&str, Option<&str>)], test: impl FnOnce()) {
        let saved: Vec<(String, Option<String>)> = vars
            .iter()
            .map(|(key, _)| ((*key).to_string(), dotenvy::var(key).ok()))
            .collect();

        for (key, value) in vars {
            match value {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }

        test();

        for (key, value) in saved {
            match value {
                Some(value) => unsafe { std::env::set_var(&key, value) },
                None => unsafe { std::env::remove_var(&key) },
            }
        }
    }

    #[test]
    #[serial]
    fn daemon_cli_uses_env_defaults_when_flags_are_absent() {
        with_env_vars(
            &[
                ("CASS_DAEMON_SOCKET", Some("/tmp/cass-daemon-env.sock")),
                ("CASS_DAEMON_IDLE_TIMEOUT_SECS", Some("42")),
                ("CASS_DAEMON_MAX_CONNECTIONS", Some("7")),
                ("CASS_DAEMON_REQUEST_TIMEOUT_SECS", Some("9")),
                ("CASS_DAEMON_MEMORY_LIMIT", Some("12345")),
            ],
            || {
                let config = resolved_daemon_config(None, None, None);
                assert_eq!(
                    config.socket_path,
                    PathBuf::from("/tmp/cass-daemon-env.sock")
                );
                assert_eq!(config.idle_timeout, Duration::from_secs(42));
                assert_eq!(config.max_connections, 7);
                assert_eq!(config.request_timeout, Duration::from_secs(9));
                assert_eq!(config.memory_limit, 12_345);
            },
        );
    }

    #[test]
    #[serial]
    fn daemon_cli_uses_one_hour_idle_default_without_env_or_flags() {
        with_env_vars(
            &[
                ("CASS_DAEMON_SOCKET", None),
                ("CASS_DAEMON_IDLE_TIMEOUT_SECS", None),
                ("CASS_DAEMON_MAX_CONNECTIONS", None),
            ],
            || {
                let config = resolved_daemon_config(None, None, None);
                assert_eq!(config.idle_timeout, Duration::from_secs(3600));
                assert_eq!(config.max_connections, 16);
            },
        );
    }

    #[test]
    #[serial]
    fn daemon_cli_flags_override_env_selected_fields_only() {
        with_env_vars(
            &[
                ("CASS_DAEMON_SOCKET", Some("/tmp/cass-daemon-env.sock")),
                ("CASS_DAEMON_IDLE_TIMEOUT_SECS", Some("42")),
                ("CASS_DAEMON_MAX_CONNECTIONS", Some("7")),
                ("CASS_DAEMON_REQUEST_TIMEOUT_SECS", Some("9")),
            ],
            || {
                let config = resolved_daemon_config(
                    Some(PathBuf::from("/tmp/cass-daemon-cli.sock")),
                    Some(0),
                    Some(3),
                );
                assert_eq!(
                    config.socket_path,
                    PathBuf::from("/tmp/cass-daemon-cli.sock")
                );
                assert_eq!(config.idle_timeout, Duration::from_secs(0));
                assert_eq!(config.max_connections, 3);
                assert_eq!(config.request_timeout, Duration::from_secs(9));
            },
        );
    }
}
