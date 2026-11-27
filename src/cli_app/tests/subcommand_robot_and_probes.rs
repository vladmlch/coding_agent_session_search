#[allow(unused_imports)]
pub(super) use super::*;

#[cfg(test)]
mod subcommand_robot_output_tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn subcommand_json_flags_enable_robot_mode_and_structured_output() {
        run_on_large_stack(|| {
            let cases = [
                vec!["cass", "search", "needle", "--json"],
                vec!["cass", "index", "--json"],
                vec!["cass", "health", "--json"],
                vec!["cass", "selftest", "--json"],
                vec!["cass", "sessions", "--json"],
                vec!["cass", "timeline", "--json"],
                vec![
                    "cass",
                    "pages",
                    "--export-only",
                    "/tmp/export",
                    "--dry-run",
                    "--json",
                ],
                vec!["cass", "export-html", "session.jsonl", "--json"],
                vec!["cass", "sources", "list", "--json"],
            ];

            for args in cases {
                let cli = Cli::try_parse_from(args.clone()).expect("parse command with --json");
                let command = cli.command.as_ref().expect("command");
                assert!(
                    is_robot_mode(command, &cli),
                    "expected robot mode for args: {args:?}"
                );

                let format = match command {
                    Commands::Search { json, .. }
                    | Commands::Index { json, .. }
                    | Commands::Health { json, .. }
                    | Commands::Selftest { json }
                    | Commands::Sessions { json, .. }
                    | Commands::Timeline { json, .. }
                    | Commands::Pages { json, .. }
                    | Commands::ExportHtml { json, .. } => {
                        resolve_subcommand_structured_format(&cli, *json)
                    }
                    Commands::Sources(SourcesCommand::List { json, .. }) => {
                        resolve_subcommand_structured_format(&cli, *json)
                    }
                    other => panic!("unexpected command variant for args {args:?}: {other:?}"),
                };

                assert_eq!(
                    format,
                    Some(RobotFormat::Json),
                    "expected structured JSON output for args: {args:?}"
                );
            }
        });
    }

    #[test]
    fn global_robot_format_overrides_subcommand_json_format() {
        run_on_large_stack(|| {
            let cli = Cli::try_parse_from(["cass", "--robot-format", "jsonl", "index", "--json"])
                .expect("parse index command");
            let command = cli.command.as_ref().expect("command");

            let json = match command {
                Commands::Index { json, .. } => *json,
                other => panic!("expected index command, got {other:?}"),
            };

            assert!(is_robot_mode(command, &cli));
            assert_eq!(
                resolve_subcommand_structured_format(&cli, json),
                Some(RobotFormat::Jsonl)
            );
        });
    }

    #[test]
    fn removed_sources_subcommands_error_through_removed_commands() {
        run_on_large_stack(|| {
            for args in [
                vec!["cass", "sources", "sync"],
                vec!["cass", "sources", "setup"],
                vec!["cass", "sources", "discover"],
                vec!["cass", "sources", "reingest", "--from-mirror"],
                vec!["cass", "sources", "artifact-manifest"],
                vec!["cass", "sources", "schedule", "laptop", "daily"],
            ] {
                let argv: Vec<String> = args.iter().map(|arg| arg.to_string()).collect();
                let error = removed_invocation_error(&argv, REMOVED_COMMANDS, REMOVED_FLAGS)
                    .unwrap_or_else(|| panic!("expected removed-command error for {argv:?}"));
                assert_eq!(error.kind, CliErrorKind::RemovedCommand.kind_str());
            }
        });
    }

    #[test]
    fn search_without_mode_uses_hybrid_preferred_default_intent() {
        run_on_large_stack(|| {
            let cli = match Cli::try_parse_from(["cass", "search", "needle", "--json"]) {
                Ok(cli) => cli,
                Err(err) => panic!("parse search command: {err}"),
            };
            let Some(command) = cli.command.as_ref() else {
                panic!("parsed search command should include command");
            };
            let Commands::Search { mode, .. } = command else {
                panic!("expected search command, got {command:?}");
            };

            assert!(mode.is_none(), "absent --mode should remain inspectable");
            assert_eq!(
                crate::search::query::SearchMode::default(),
                crate::search::query::SearchMode::Hybrid
            );
        });
    }

    #[test]
    fn default_hybrid_fallback_meta_reports_lexical_realization() {
        let mut meta = SearchModeMeta::new(crate::search::query::SearchMode::Hybrid, true);
        meta.fall_back_to_lexical("semantic assets unavailable");

        assert_eq!(meta.requested, crate::search::query::SearchMode::Hybrid);
        assert_eq!(meta.realized, crate::search::query::SearchMode::Lexical);
        assert_eq!(meta.fallback_tier, Some("lexical"));
        assert!(!meta.semantic_refinement());
    }

    #[test]
    fn hybrid_search_intent_is_fail_open_even_when_explicit() {
        let default_hybrid = SearchModeMeta::new(crate::search::query::SearchMode::Hybrid, true);
        let explicit_hybrid = SearchModeMeta::new(crate::search::query::SearchMode::Hybrid, false);
        let explicit_semantic =
            SearchModeMeta::new(crate::search::query::SearchMode::Semantic, false);

        assert!(default_hybrid.fail_open_on_semantic_unavailable());
        assert!(explicit_hybrid.fail_open_on_semantic_unavailable());
        assert!(!explicit_semantic.fail_open_on_semantic_unavailable());
    }

    #[test]
    fn robot_budget_policy_never_demotes_explicit_semantic_to_lexical() -> Result<(), String> {
        use crate::robot_budget_envelope::BudgetPhase;
        use crate::search::query::SearchMode;

        fn snapshot(phase: BudgetPhase) -> BudgetSnapshot {
            match phase {
                BudgetPhase::Healthy => BudgetSnapshot::from_elapsed(0, 10),
                BudgetPhase::NearLimit => BudgetSnapshot::from_elapsed(8, 10),
                BudgetPhase::Exhausted => BudgetSnapshot::from_elapsed(10, 10),
            }
        }

        macro_rules! require_matches {
            ($observed:expr, $expected:pat, $context:expr) => {{
                match $observed {
                    $expected => {}
                    budget_policy_observed => {
                        return Err(format!(
                            "{}: unexpected {budget_policy_observed:?}",
                            $context
                        ));
                    }
                }
            }};
        }

        let explicit_semantic = SearchModeMeta::new(SearchMode::Semantic, false);
        let explicit_hybrid = SearchModeMeta::new(SearchMode::Hybrid, false);
        let default_hybrid = SearchModeMeta::new(SearchMode::Hybrid, true);
        let lexical_mode_meta = SearchModeMeta::new(SearchMode::Lexical, false);

        for phase in [None, Some(BudgetPhase::Healthy)] {
            require_matches!(
                semantic_budget_action(&explicit_semantic, phase.map(snapshot)),
                SemanticBudgetAction::Execute,
                "healthy explicit semantic"
            );
            require_matches!(
                semantic_budget_action(&explicit_hybrid, phase.map(snapshot)),
                SemanticBudgetAction::Execute,
                "healthy explicit hybrid"
            );
            require_matches!(
                semantic_budget_action(&default_hybrid, phase.map(snapshot)),
                SemanticBudgetAction::Execute,
                "healthy default hybrid"
            );
            require_matches!(
                semantic_budget_action(&lexical_mode_meta, phase.map(snapshot)),
                SemanticBudgetAction::Execute,
                "healthy lexical"
            );
        }

        for phase in [BudgetPhase::NearLimit, BudgetPhase::Exhausted] {
            require_matches!(
                semantic_budget_action(&explicit_semantic, Some(snapshot(phase))),
                SemanticBudgetAction::StrictSemanticTimeout,
                "explicit semantic must fail closed instead of changing its realized mode"
            );
            require_matches!(
                semantic_budget_action(&explicit_hybrid, Some(snapshot(phase))),
                SemanticBudgetAction::HybridLexicalFallback,
                "hybrid may fail open when semantic work no longer fits the budget"
            );
            require_matches!(
                semantic_budget_action(&default_hybrid, Some(snapshot(phase))),
                SemanticBudgetAction::HybridLexicalFallback,
                "default hybrid mode must retain the same explicit fail-open policy"
            );
            require_matches!(
                semantic_budget_action(&lexical_mode_meta, Some(snapshot(phase))),
                SemanticBudgetAction::Execute,
                "lexical mode has no semantic work to shed"
            );
        }

        let mut meta = SearchModeMeta::new(SearchMode::Semantic, false);
        if matches!(
            semantic_budget_action(&meta, Some(snapshot(BudgetPhase::NearLimit))),
            SemanticBudgetAction::HybridLexicalFallback
        ) {
            meta.fall_back_to_lexical("budget pressure");
        }
        require_matches!(
            meta.requested,
            SearchMode::Semantic,
            "strict requested mode"
        );
        require_matches!(meta.realized, SearchMode::Semantic, "strict realized mode");
        require_matches!(meta.fallback_tier, None, "strict fallback tier");
        require_matches!(
            meta.fallback_reason.as_deref(),
            None,
            "strict fallback reason"
        );

        let mut unavailable_hybrid = SearchModeMeta::new(SearchMode::Hybrid, false);
        unavailable_hybrid.fall_back_to_lexical("semantic context unavailable: no index");
        for phase in [BudgetPhase::NearLimit, BudgetPhase::Exhausted] {
            require_matches!(
                semantic_budget_action(&unavailable_hybrid, Some(snapshot(phase))),
                SemanticBudgetAction::Execute,
                "a prior semantic-availability fallback must keep its original cause"
            );
        }
        require_matches!(
            unavailable_hybrid.fallback_reason.as_deref(),
            Some("semantic context unavailable: no index"),
            "prior fallback reason"
        );

        Ok(())
    }

    #[test]
    fn strict_semantic_budget_error_preserves_the_decision_snapshot() {
        use crate::robot_budget_envelope::BudgetPhase;
        use crate::search::query::SearchMode;

        // The timing fields deliberately describe an exhausted budget while
        // the captured phase remains NearLimit. This cannot come from
        // `capture`, but it proves the policy and diagnostic consume the exact
        // same immutable value instead of recomputing the phase or re-reading
        // a live clock at the error boundary.
        let snapshot = BudgetSnapshot::from_parts_for_test(BudgetPhase::NearLimit, 10, 10, 0);
        let mode_meta = SearchModeMeta::new(SearchMode::Semantic, false);

        assert_eq!(
            semantic_budget_action(&mode_meta, Some(snapshot)),
            SemanticBudgetAction::StrictSemanticTimeout
        );
        let error = strict_semantic_budget_error(
            &mode_meta,
            Some(snapshot),
            SemanticBudgetCheckpoint::BeforeSearch,
        );
        assert_eq!(error.code, 10);
        assert!(error.retryable);
        for expected in [
            "checkpoint=before-search",
            "phase=near-limit",
            "elapsed_ms=10",
            "budget_ms=10",
            "remaining_ms=0",
        ] {
            assert!(
                error.message.contains(expected),
                "strict error must preserve {expected:?}: {}",
                error.message
            );
        }
        assert!(
            !error.message.contains("phase=exhausted"),
            "strict error must not recompute a later phase: {}",
            error.message
        );
    }

    #[test]
    fn sparse_fallback_threshold_tracks_visible_search_limit_not_cursor_probe() {
        assert_eq!(sparse_threshold_for_visible_limit(3, 1, false), 1);
        assert_eq!(sparse_threshold_for_visible_limit(3, 2, false), 2);
        assert_eq!(sparse_threshold_for_visible_limit(3, 10, false), 3);
        assert_eq!(sparse_threshold_for_visible_limit(3, 0, false), 3);
        assert_eq!(sparse_threshold_for_visible_limit(3, 1, true), 3);
    }

    #[test]
    fn refresh_catchup_flags_are_opt_in_for_search() {
        run_on_large_stack(|| {
            for args in [
                vec!["cass", "search", "needle", "--refresh"],
                vec!["cass", "search", "needle", "--catch-up"],
            ] {
                let cli = Cli::try_parse_from(args.clone()).expect("parse search refresh flag");
                let Some(Commands::Search { refresh, .. }) = cli.command else {
                    panic!("expected search command for args {args:?}");
                };
                assert!(refresh, "refresh should be enabled for args {args:?}");
            }

            let cli = Cli::try_parse_from(["cass", "search", "needle"])
                .expect("parse search without refresh");
            let Some(Commands::Search { refresh, .. }) = cli.command else {
                panic!("expected search command without refresh");
            };
            assert!(!refresh, "refresh should stay opt-in");
        });
    }

    #[test]
    fn gh422_no_maintenance_is_explicit_and_conflicts_with_refresh() {
        run_on_large_stack(|| {
            let cli =
                Cli::try_parse_from(["cass", "search", "needle", "--json", "--no-maintenance"])
                    .expect("parse strict read-only search");
            let Some(Commands::Search {
                no_maintenance,
                refresh,
                ..
            }) = cli.command
            else {
                panic!("expected search command");
            };
            assert!(no_maintenance);
            assert!(!refresh);

            assert!(
                Cli::try_parse_from(["cass", "search", "needle", "--no-maintenance", "--refresh",])
                    .is_err(),
                "a read-only search cannot also authorize maintenance"
            );
            assert!(
                Cli::try_parse_from(["cass", "search", "needle", "--no-maintenance", "--daemon",])
                    .is_err(),
                "a read-only search cannot authorize daemon auto-spawn"
            );
        });
    }
}

// Tests for `--include-attachments` removed: the flag was accepted
// but unimplemented and has been removed from the pages CLI surface
// (bead adyyt). Any future attachment-bundling work will add a new
// flag with end-to-end implementation + fresh tests at that time.

#[cfg(test)]
mod cli_models_resolution_tests {
    use super::*;

    #[test]
    fn resolve_cli_model_name_accepts_verified_native_embedder_aliases() -> Result<(), String> {
        for alias in [
            ("all-minilm-l6-v2", "minilm"),
            ("minilm", "minilm"),
            ("minilm-384", "minilm"),
            ("fastembed", "minilm"),
            ("MINILM", "minilm"),
            ("multilingual", "multilingual-minilm"),
            ("multilingual-minilm-384", "multilingual-minilm"),
            (
                "paraphrase-multilingual-minilm-l12-v2",
                "multilingual-minilm",
            ),
        ] {
            let resolved = resolve_cli_model_name(alias.0).map_err(|error| error.message)?;
            assert_eq!(resolved, alias.1);
        }
        Ok(())
    }

    #[test]
    fn resolve_cli_model_name_rejects_unimplemented_embedder_topologies() -> Result<(), &'static str>
    {
        for alias in ["snowflake-arctic-s", "nomic-embed-text-v1.5"] {
            let error = match resolve_cli_model_name(alias) {
                Err(error) => error,
                Ok(_) => return Err("unimplemented embedder topology unexpectedly resolved"),
            };
            if error.code != 20 {
                return Err("unimplemented embedder topology returned the wrong error code");
            }
            if !error.message.contains("supports all-MiniLM-L6-v2")
                || !error
                    .message
                    .contains("paraphrase-multilingual-MiniLM-L12-v2")
            {
                return Err("unimplemented embedder topology returned the wrong error message");
            }
            for forbidden in ["install --model snowflake", "install --model nomic"] {
                if error.message.contains(forbidden) {
                    return Err("embedder topology error advertised an unavailable route");
                }
            }
        }
        Ok(())
    }

    #[test]
    fn resolve_cli_model_name_rejects_unimplemented_reranker_topology() -> Result<(), String> {
        let error = match resolve_cli_model_name("jina-reranker-v1-turbo-en") {
            Err(error) => error,
            Ok(canonical) => {
                return Err(format!(
                    "unimplemented Jina topology resolved to {canonical}"
                ));
            }
        };
        if error.code != 20
            || !error
                .message
                .contains("supports only ms-marco-MiniLM-L-6-v2")
            || error.hint.as_deref() != Some("Use --model ms-marco, or omit reranking")
        {
            return Err(format!("unexpected Jina topology error: {error:?}"));
        }
        Ok(())
    }

    /// `coding_agent_session_search-v3of1`: unknown names must be rejected
    /// with code=20, kind="model", and a hint pointing at `cass models status`.
    #[test]
    fn resolve_cli_model_name_rejects_unknown_with_useful_hint() -> Result<(), String> {
        let err = match resolve_cli_model_name("e5-large-v2") {
            Err(error) => error,
            Ok(canonical) => return Err(format!("unknown model resolved to {canonical}")),
        };
        if err.code != 20
            || err.kind != "model"
            || !err.message.contains("Unknown model 'e5-large-v2'")
            || !err.message.contains("all-minilm-l6-v2")
            || err.message.contains("snowflake-arctic-s")
            || err.message.contains("nomic-embed")
            || err.message.contains("jina-reranker")
            || err.hint.as_deref() != Some("Use 'cass models status' to see available models")
            || err.retryable
        {
            return Err(format!("unexpected unknown-model error: {err:?}"));
        }
        Ok(())
    }

    /// `coding_agent_session_search-v3of1`: every name that
    /// `resolve_cli_model_name` returns MUST be a name that both
    /// `ModelManifest::for_embedder` and `FastEmbedder::model_dir_for`
    /// accept. Otherwise install/remove would resolve the alias and
    /// then crash at the manifest/dir lookup. This is the cross-module
    /// contract that the original hardcoded `all-minilm-l6-v2` check
    /// silently maintained — making it explicit prevents drift.
    #[test]
    fn every_resolved_canonical_name_has_manifest_and_dir_mapping() -> Result<(), String> {
        use crate::search::fastembed_embedder::FastEmbedder;
        use crate::search::model_download::ModelManifest;

        let probe_data_dir = std::path::Path::new("/tmp/cass-v3of1-probe");
        for canonical in ["minilm", "multilingual-minilm"] {
            assert!(
                ModelManifest::for_embedder(canonical).is_some(),
                "canonical name {canonical:?} returned by resolve_cli_model_name must have a \
                 ModelManifest registered"
            );
            assert!(
                FastEmbedder::model_dir_for(probe_data_dir, canonical).is_some(),
                "canonical name {canonical:?} returned by resolve_cli_model_name must have a \
                 FastEmbedder::model_dir_for mapping"
            );
        }
        Ok(())
    }

    /// Reranker aliases must round-trip through `resolve_cli_model_name`
    /// and `resolve_model_install_dir` so `cass models install --model
    /// ms-marco` actually lands the files where `FastEmbedReranker` looks
    /// for them. Without this contract the resolver accepts the alias,
    /// the install path ships the bytes, and the daemon still logs
    /// "reranker model directory not found" because the manifest `id`
    /// drifted from the loader's hard-coded directory name.
    #[test]
    fn reranker_aliases_resolve_and_install_path_matches_loader_dir() -> Result<(), String> {
        use crate::search::model_download::ModelManifest;
        use crate::search::reranker_registry::RERANKERS;

        for (alias, canonical) in [
            ("ms-marco", "ms-marco"),
            ("ms-marco-minilm", "ms-marco"),
            ("ms-marco-minilm-l-6-v2", "ms-marco"),
            ("ms-marco-minilm-l6-v2", "ms-marco"),
            ("MS-MARCO", "ms-marco"),
        ] {
            let resolved = resolve_cli_model_name(alias).map_err(|error| error.message)?;
            assert_eq!(
                resolved, canonical,
                "reranker alias {alias:?} must resolve to {canonical:?}"
            );
            assert!(
                ModelManifest::for_reranker(canonical).is_some(),
                "canonical reranker {canonical:?} must have a ModelManifest registered"
            );
        }

        // The install path uses `manifest.id` as the directory name. That
        // name MUST equal the last path segment of the loader's
        // `RegisteredReranker::model_dir`, otherwise install delivers
        // files to one location and the loader reads from another —
        // the exact bug that left "WARN Failed to load reranker" on
        // every fresh daemon start (cass#242).
        let probe = std::path::Path::new("/tmp/cass-reranker-probe");
        let canonical = "ms-marco";
        let manifest = ModelManifest::for_reranker(canonical)
            .ok_or_else(|| format!("{canonical} manifest must be registered"))?;
        let registered = RERANKERS
            .iter()
            .find(|r| r.name == canonical)
            .ok_or_else(|| format!("{canonical} must be in RERANKERS"))?;
        let loader_dir = registered
            .model_dir(probe)
            .ok_or_else(|| format!("{canonical} must have a model_dir"))?;
        let loader_dir_name = loader_dir
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| format!("{canonical} model_dir must have a final segment"))?;
        assert_eq!(
            manifest.id, loader_dir_name,
            "{canonical} manifest.id must match the loader's model_dir name so install and loader \
             agree on the on-disk directory"
        );
        Ok(())
    }
}

#[cfg(test)]
mod state_db_integrity_probe_tests {
    use super::*;

    /// Build a minimal canonical-shaped DB (the two tables the bounded probe
    /// projects) with one real row each.
    fn seed_probe_shaped_db(db_path: &Path) {
        let conn = crate::franken_sync::Connection::open(db_path.display().to_string())
            .expect("create probe fixture db");
        conn.execute_batch(
            "PRAGMA application_id = 0x43415353;
             PRAGMA user_version = 1;
             CREATE TABLE conversations (
                 id INTEGER PRIMARY KEY,
                 title TEXT,
                 source_path TEXT NOT NULL
             );
             CREATE TABLE messages (
                 id INTEGER PRIMARY KEY,
                 conversation_id INTEGER NOT NULL,
                 role TEXT NOT NULL
             );
             INSERT INTO conversations (id, title, source_path)
                 VALUES (1, 'probe fixture', '/tmp/fixture.jsonl');
             INSERT INTO messages (id, conversation_id, role)
                 VALUES (1, 1, 'user');",
        )
        .expect("seed probe fixture rows");
        // Explicit close runs the final passive WAL checkpoint so the table
        // pages land in the MAIN file — the corruption test smashes those
        // pages, and a dropped-not-closed connection leaves them in the WAL
        // sidecar (observed: a 4096-byte single-page main file).
        conn.close().expect("checkpoint fixture into the main file");
    }

    /// i6upe: the bounded probe reports nothing for a missing or healthy DB —
    /// it must never manufacture a corruption verdict.
    #[test]
    fn bounded_corruption_probe_passes_missing_and_healthy_dbs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("agent_search.db");
        assert_eq!(
            bounded_canonical_db_corruption_probe(&db_path, "i6upe-missing"),
            None,
            "a missing DB is not corrupt"
        );

        seed_probe_shaped_db(&db_path);
        assert_eq!(
            bounded_canonical_db_corruption_probe(&db_path, "i6upe-healthy"),
            None,
            "a healthy DB with real rows must pass the probe"
        );
    }

    /// GH #413: the pre-index corruption guard has a fixed 100-byte I/O
    /// budget. Corrupt header fields must still fail closed without using a
    /// SQL query whose physical work can scale with archive size.
    #[test]
    fn bounded_corruption_probe_flags_malformed_header() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("agent_search.db");
        seed_probe_shaped_db(&db_path);
        let mut bytes = std::fs::read(&db_path).expect("read fixture database");
        bytes[16] = 0;
        bytes[17] = 3;
        std::fs::write(&db_path, bytes).expect("write malformed page-size header");

        let verdict = bounded_canonical_db_corruption_probe(&db_path, "gh413-corrupt")
            .expect("invalid fixed header fields must produce a corruption verdict");
        assert!(
            verdict.contains("page size 3"),
            "unexpected verdict: {verdict}"
        );
    }

    /// GH #413: once the physical bundle exceeds the explicit ceiling, the
    /// pre-index guard must stop after the fixed header read and never open
    /// SQLite. Namespace identity sidecars are a causal open marker.
    #[test]
    fn bounded_corruption_probe_does_not_open_large_archive() {
        use std::io::Write as _;

        let dir = tempfile::tempdir().expect("tempdir");
        let seed_path = dir.path().join("seed.db");
        seed_probe_shaped_db(&seed_path);
        let seed_header = std::fs::read(&seed_path).expect("read seed database");

        let db_path = dir.path().join("large-agent-search.db");
        let mut large = std::fs::File::create(&db_path).expect("create sparse large archive");
        large
            .write_all(&seed_header[..100])
            .expect("write valid SQLite header");
        let encoded_page_size = u16::from_be_bytes([seed_header[16], seed_header[17]]);
        let page_size = if encoded_page_size == 1 {
            65_536_u64
        } else {
            u64::from(encoded_page_size)
        };
        let first_page_boundary_above_limit = STATUS_COUNT_SCAN_MAX_DB_BYTES
            .saturating_add(page_size)
            .div_ceil(page_size)
            .saturating_mul(page_size);
        large
            .set_len(first_page_boundary_above_limit)
            .expect("extend sparse large archive beyond deep-probe ceiling");
        drop(large);

        assert_eq!(
            bounded_canonical_db_corruption_probe(&db_path, "gh413-large"),
            None,
            "a valid large-file header must not trigger a false corruption verdict"
        );
        for suffix in ["-fsqlite-ns-gate", "-fsqlite-ns-use"] {
            let sidecar = doctor_sqlite_sidecar_path(&db_path, suffix).expect("sidecar path");
            assert!(
                !sidecar.exists(),
                "large-archive guard opened SQLite and created {}",
                sidecar.display()
            );
        }
    }

    /// i6upe: below the explicit archive-bundle byte ceiling, keep the
    /// real-column edge probes that catch damage outside the SQLite header.
    #[test]
    fn bounded_corruption_probe_flags_smashed_table_pages_for_small_archive() {
        use std::io::{Seek as _, SeekFrom, Write as _};

        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("agent_search.db");
        seed_probe_shaped_db(&db_path);

        let len = std::fs::metadata(&db_path).expect("stat fixture").len();
        assert!(
            len > 4096,
            "fixture must span multiple pages for a mid-file smash (len={len})"
        );
        let mut db = std::fs::OpenOptions::new()
            .write(true)
            .open(&db_path)
            .expect("open fixture for corruption");
        db.seek(SeekFrom::Start(4096)).expect("seek past page 1");
        db.write_all(&vec![0xFF_u8; (len - 4096) as usize])
            .expect("smash table pages");
        drop(db);

        let wal_path = doctor_sqlite_sidecar_path(&db_path, "-wal").expect("WAL path");
        if let Ok(wal_len) = std::fs::metadata(&wal_path).map(|metadata| metadata.len())
            && wal_len > 0
        {
            let mut wal = std::fs::OpenOptions::new()
                .write(true)
                .open(&wal_path)
                .expect("open WAL sidecar for corruption");
            wal.write_all(&vec![0xFF_u8; wal_len as usize])
                .expect("smash WAL frames");
        }

        let verdict = bounded_canonical_db_corruption_probe(&db_path, "i6upe-corrupt")
            .expect("small smashed archive must produce a corruption verdict");
        let lower = verdict.to_ascii_lowercase();
        assert!(
            lower.contains("malformed")
                || lower.contains("corrupt")
                || lower.contains("disk image"),
            "unexpected smashed-page verdict: {verdict}"
        );
    }
}
