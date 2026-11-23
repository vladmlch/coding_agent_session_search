include!("../gh473_tests.rs");
include!("../gh494_tests.rs");

use super::*;
use crate::franken_sync::compat::RowExt;
use tempfile::TempDir;

mod helpers;
use helpers::*;

mod admission_lock_tests;
mod checkpoint_commit_fingerprint_tests;
mod checkpoint_wal_deadline_tests;
mod completed_checkpoint_connectors_tests;
mod daily_stats_repair_salvage_tests;
mod expected_docs_rebuild_core_tests;
mod lexical_checkpoint_refresh_tests;
mod lexical_controller_planner_tests;
mod lexical_flow_merge_tests;
mod lexical_responsiveness_producer_tests;
mod preflight_watchdog_tests;
mod provenance_scan_roots_tests;
mod raw_mirror_exclusions_tests;
mod staged_publish_recovery_tests;
mod staged_publish_swap_tests;
mod staged_resume_watch_once_tests;
mod storage_headroom_gc_tests;
mod streaming_connector_fault_tests;
mod streaming_oom_quarantine_tests;
mod streaming_scan_storage_open_tests;
mod watch_backlog_state_tests;
mod watch_lexical_spool_tests;
mod watch_oom_replay_debt_tests;
