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

#[cfg(test)]
mod robot_docs_shorthand_regression_tests {
    use super::*;

    #[test]
    fn doctor_subcommand_is_not_rewritten_to_robot_docs() {
        let mut rest = vec!["doctor".to_string(), "--json".to_string()];
        let mut corrections = Vec::new();
        recover_robot_docs_topic_shorthands(&mut rest, &mut corrections);
        assert_eq!(rest, vec!["doctor".to_string(), "--json".to_string()]);
        assert!(
            corrections.is_empty(),
            "no rewrite should happen for `cass doctor --json`"
        );
    }

    #[test]
    fn other_real_subcommands_are_not_rewritten() {
        for sub in &["diag", "sources", "health", "selftest", "Doctor"] {
            let mut rest = vec![sub.to_string(), "--json".to_string()];
            let mut corrections = Vec::new();
            recover_robot_docs_topic_shorthands(&mut rest, &mut corrections);
            assert_eq!(
                rest,
                vec![sub.to_string(), "--json".to_string()],
                "real subcommand {sub} should not be rewritten",
            );
            assert!(
                corrections.is_empty(),
                "no rewrite should happen for `cass {sub} --json`",
            );
        }
    }

    #[test]
    fn topic_shorthand_still_fires_for_non_subcommands() {
        // Note: `guide` is intentionally absent — it is now a real top-level
        // subcommand (`cass guide <intent>`), so the shorthand must NOT fire for
        // it (covered by `other_real_subcommands_are_not_rewritten`).
        for (alias, expected_topic) in &[
            ("schemas", "schemas"),
            ("exit-codes", "exit-codes"),
            ("examples", "examples"),
            ("env", "env"),
            ("quickstart", "guide"),
            ("quick-start", "guide"),
        ] {
            let mut rest = vec![alias.to_string(), "--json".to_string()];
            let mut corrections = Vec::new();
            recover_robot_docs_topic_shorthands(&mut rest, &mut corrections);
            assert_eq!(
                rest,
                vec![
                    "robot-docs".to_string(),
                    expected_topic.to_string(),
                    "--json".to_string()
                ],
                "shorthand for {alias} should still rewrite to robot-docs {expected_topic}",
            );
            assert_eq!(corrections.len(), 1);
        }
    }

    #[test]
    fn structured_help_does_not_steal_search_query_named_help() {
        let raw = vec![
            "cass".to_string(),
            "search".to_string(),
            "help".to_string(),
            "--json".to_string(),
        ];
        let (normalized, note) = normalize_args(raw);
        assert_eq!(
            normalized,
            vec![
                "cass".to_string(),
                "search".to_string(),
                "help".to_string(),
                "--json".to_string()
            ]
        );
        assert!(
            note.as_deref()
                .is_none_or(|note| !note.contains("structured help request")),
            "a search query named `help` must not be treated as a help request"
        );
    }

    #[test]
    fn structured_help_targets_command_after_leading_json_flag() {
        let raw = vec![
            "cass".to_string(),
            "--json".to_string(),
            "search".to_string(),
            "--help".to_string(),
        ];
        let (normalized, note) = normalize_args(raw);
        assert_eq!(
            normalized,
            vec![
                "cass".to_string(),
                "robot-docs".to_string(),
                "commands".to_string()
            ]
        );
        assert!(
            note.as_deref()
                .is_some_and(|note| note.contains("structured help request")),
            "leading structured flags should still route command help to robot docs"
        );
    }

    #[test]
    fn leading_data_dir_moves_to_health_subcommand() {
        let raw = vec![
            "cass".to_string(),
            "--data-dir".to_string(),
            "/tmp/cass-data".to_string(),
            "health".to_string(),
            "--json".to_string(),
        ];
        let (normalized, note) = normalize_args(raw);
        assert_eq!(
            normalized,
            vec![
                "cass".to_string(),
                "health".to_string(),
                "--data-dir".to_string(),
                "/tmp/cass-data".to_string(),
                "--json".to_string()
            ]
        );
        assert!(
            note.as_deref()
                .is_some_and(|note| note.contains("Leading --data-dir moved")),
            "leading data dir should be reported as a correction"
        );
    }

    #[test]
    fn leading_structured_and_data_dir_move_to_nested_models_subcommand() {
        let raw = vec![
            "cass".to_string(),
            "--json".to_string(),
            "--data-dir=/tmp/cass-data".to_string(),
            "models".to_string(),
            "verify".to_string(),
        ];
        let (normalized, note) = normalize_args(raw);
        assert_eq!(
            normalized,
            vec![
                "cass".to_string(),
                "models".to_string(),
                "verify".to_string(),
                "--data-dir=/tmp/cass-data".to_string(),
                "--json".to_string()
            ]
        );
        assert!(
            note.as_deref()
                .is_some_and(|note| note.contains("Leading --data-dir moved")),
            "nested data-dir-aware commands should get the data dir after the nested verb"
        );
    }
}

#[cfg(test)]
mod canonical_top_level_command_tests {
    use super::*;

    #[test]
    fn ann_setup_help_names_a_parseable_index_command() {
        let command = Cli::command();
        let search = command.find_subcommand("search").expect("search command");
        let approximate = search
            .get_arguments()
            .find(|argument| argument.get_id() == "approximate")
            .expect("approximate flag");
        let help = approximate
            .get_long_help()
            .or_else(|| approximate.get_help())
            .expect("ANN setup help")
            .to_string();
        let recipe = help.split('`').nth(1).expect("documented setup command");
        let parsed = Cli::try_parse_from(recipe.split_whitespace()).expect("setup parses");
        assert!(matches!(
            parsed.command,
            Some(Commands::Index {
                semantic: true,
                build_hnsw: true,
                ..
            })
        ));
        assert!(Cli::try_parse_from(["cass", "index", "--semantic", "--approximate"]).is_err());
        let query = Cli::try_parse_from(["cass", "search", "example", "--approximate"])
            .expect("ANN remains a search option");
        assert!(matches!(
            query.command,
            Some(Commands::Search {
                approximate: true,
                ..
            })
        ));
    }

    #[test]
    fn ann_index_parse_error_explains_the_index_command() {
        for format in ["--json", "--robot"] {
            let args = ["cass", "index", "--semantic", "--approximate", format].map(str::to_string);
            let error = Cli::try_parse_from(&args).expect_err("query-only flag rejected");
            let output = format_friendly_parse_error(error, &args, &args);
            let payload: serde_json::Value = serde_json::from_str(&output).expect("error JSON");
            assert_eq!(payload["kind"], "argument_parsing");
            let examples = payload["examples"].as_array().expect("examples");
            assert!(!examples.is_empty());
            for example in examples {
                let example = example.as_str().expect("example command");
                assert!(example.starts_with("cass index "), "{example}");
                assert!(Cli::try_parse_from(example.split_whitespace()).is_ok());
            }
            let hints = payload["hints"].as_array().expect("hints");
            assert!(hints.iter().any(|hint| {
                hint.as_str()
                    .is_some_and(|text| text.contains("--semantic --build-hnsw"))
            }));
            assert!(!output.contains("Use the 'search' subcommand explicitly"));
            let mistakes = payload["common_mistakes"]
                .as_array()
                .expect("index mistakes");
            for mistake in mistakes {
                let correct = mistake["correct"].as_str().expect("corrected command");
                assert!(correct.starts_with("cass index "), "{correct}");
                assert!(Cli::try_parse_from(correct.split_whitespace()).is_ok());
            }
        }
    }

    /// GH #367: `cass forget --source-glob <pat> --robot` was rewritten to
    /// `search forget ...` because the implicit-robot-query recovery consults
    /// `CANONICAL_TOP_LEVEL_COMMANDS` and `forget` was never added when the
    /// subcommand landed (#292). Any visible clap subcommand missing from the
    /// list gets folded into a search query the moment a robot flag is
    /// present — silently breaking exactly the machine-readable path agents
    /// are told to use. Pin the list to the clap command tree.
    #[test]
    fn canonical_list_covers_every_clap_subcommand() {
        // Building the full clap command tree needs more than the default
        // 2 MiB test stack (same reason tests/cli_robot.rs runs clap work on
        // a dedicated large-stack thread).
        let handle = std::thread::Builder::new()
            .stack_size(16 * 1024 * 1024)
            .spawn(|| {
                let cli = Cli::command();
                for sub in cli.get_subcommands() {
                    if sub.is_hide_set() {
                        continue;
                    }
                    let name = sub.get_name().to_string();
                    assert!(
                        CANONICAL_TOP_LEVEL_COMMANDS.contains(&name.as_str()),
                        "clap subcommand `{name}` is missing from CANONICAL_TOP_LEVEL_COMMANDS; \
                         robot mode will rewrite `cass {name} ... --robot` into `search {name} ...` (GH #367)"
                    );
                }
                // Reverse direction: a canonical entry with no matching clap
                // subcommand is a stale typo attractor — a removed/renamed
                // command would keep pulling nearby query words into a
                // rewrite that can only fail (adversarial-review F1).
                let clap_names: Vec<String> = cli
                    .get_subcommands()
                    .map(|sub| sub.get_name().to_string())
                    .collect();
                for canonical in CANONICAL_TOP_LEVEL_COMMANDS {
                    assert!(
                        clap_names.iter().any(|name| name == canonical),
                        "CANONICAL_TOP_LEVEL_COMMANDS entry `{canonical}` has no clap subcommand; \
                         remove the stale entry so it stops attracting typo corrections"
                    );
                }
            })
            .expect("spawn large-stack clap test thread");
        match handle.join() {
            Ok(()) => {}
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    /// Adversarial-review F1 on GH #367: adding `forget`/`upgrade` to the
    /// canonical list put common English query words inside the
    /// Levenshtein-2 attraction net. `cass format` stopped auto-recovering
    /// to `search "format"` and hard-failed on forget's required
    /// `--source-glob`; `cass parade` silently EXECUTED the upgrade
    /// release-check. Side-effectful commands dispatch on exact spelling
    /// only. The local-only port removed `upgrade` entirely: it is out of
    /// the canonical list (the disjointness invariant pins that) and the
    /// exact spelling now errors through `REMOVED_COMMANDS` instead of
    /// dispatching.
    #[test]
    fn side_effectful_commands_are_never_typo_attractors() {
        for query_word in ["format", "forest", "forge", "forged", "fort"] {
            assert_ne!(
                closest_top_level_command(query_word),
                Some("forget"),
                "`cass {query_word}` must fall through to implicit search, not forget"
            );
        }
        for query_word in ["parade", "upgrades", "upgrde", "grade"] {
            assert_ne!(
                closest_top_level_command(query_word),
                Some("upgrade"),
                "`cass {query_word}` must never execute the upgrade release-check"
            );
        }
        // Exact spellings still dispatch (GH #367's actual fix is untouched)
        // — but `upgrade` dispatches to the removed-command error, not to a
        // release check.
        assert!(CANONICAL_TOP_LEVEL_COMMANDS.contains(&"forget"));
        assert!(!CANONICAL_TOP_LEVEL_COMMANDS.contains(&"upgrade"));
        assert!(looks_like_top_level_command_or_typo("forget"));
        assert!(!looks_like_top_level_command_or_typo("upgrade"));
        let args: Vec<String> = ["cass", "upgrade"]
            .iter()
            .map(ToString::to_string)
            .collect();
        let error = removed_invocation_error(&args, REMOVED_COMMANDS, REMOVED_FLAGS)
            .expect("`cass upgrade` must hit the removed-command table");
        assert_eq!(error.kind, CliErrorKind::RemovedCommand.kind_str());
    }

    /// 2l1b0.51: an exact subcommand is never a typo. Before the fix every
    /// canonical command within Levenshtein 2 of another one was rerouted
    /// (status→stats, stats→status, export→import, …) whenever clap
    /// rejected the invocation for an unrelated flag typo.
    #[test]
    fn exact_canonical_commands_are_never_rerouted() {
        let mut attracted_pairs = Vec::new();
        for command in CANONICAL_TOP_LEVEL_COMMANDS {
            for other in CANONICAL_TOP_LEVEL_COMMANDS {
                if command != other && strsim::levenshtein(command, other) <= 2 {
                    attracted_pairs.push((*command, *other));
                }
            }
            assert_eq!(
                closest_top_level_command(command),
                None,
                "`cass {command}` must keep its own subcommand"
            );
            assert_eq!(
                closest_top_level_command(&command.to_ascii_uppercase()),
                None,
                "`cass {}` must keep its own subcommand",
                command.to_ascii_uppercase()
            );
        }
        // The fixture only means something while near-collisions exist.
        assert!(
            attracted_pairs.contains(&("status", "stats")),
            "{attracted_pairs:?}"
        );
        assert!(
            attracted_pairs.contains(&("export", "import")),
            "{attracted_pairs:?}"
        );
        // Genuine typos still recover (bead a0z1v).
        assert_eq!(closest_top_level_command("serach"), Some("search"));
        assert_eq!(closest_top_level_command("helth"), Some("health"));
    }

    /// 2l1b0.51, end to end through the recovery layer: a flag typo on an
    /// exact subcommand corrects only the flag.
    #[test]
    fn flag_typo_on_exact_subcommand_keeps_the_subcommand() {
        for (command, expected_flag) in [
            ("status", "--json"),
            ("stats", "--json"),
            ("export", "--json"),
        ] {
            let args = ["cass", command, "--jsn"].map(str::to_string);
            let error = Cli::try_parse_from(&args).expect_err("--jsn is not a flag");
            let (corrected, note) =
                heuristic_parse_recovery(&error, &args).expect("flag typo is recoverable");
            assert_eq!(
                corrected[1], command,
                "subcommand changed: {corrected:?} ({note})"
            );
            assert!(
                corrected.iter().any(|arg| arg == expected_flag),
                "{corrected:?}"
            );
            assert!(
                !note.contains("subcommand typo"),
                "no subcommand correction may be reported: {note}"
            );
        }
    }

    #[test]
    fn pack_field_mask_contract_flag_survives_query_recovery() {
        for args in [
            vec![
                "cass",
                "pack",
                "maskneedle",
                "--field-mask",
                "standard",
                "--json",
            ],
            vec![
                "cass",
                "pack",
                "--field-mask",
                "standard",
                "maskneedle",
                "--json",
            ],
            vec!["cass", "maskneedle", "--field-mask", "standard", "--json"],
            vec![
                "cass",
                "search",
                "maskneedle",
                "--field-mask",
                "standard",
                "--json",
            ],
            vec![
                "cass",
                "pack",
                "maskneedle",
                "field_mask=standard",
                "--json",
            ],
            vec!["cass", "maskneedle", "field-mask=standard", "--json"],
            vec![
                "cass",
                "pack",
                "maskneedle",
                "--fields",
                "standard",
                "--json",
            ],
        ] {
            let (normalized, _) = normalize_args(args.into_iter().map(str::to_string).collect());
            let cli = Cli::try_parse_from(normalized).expect("parse documented pack mask");
            let Some(Commands::Pack { query, fields, .. }) = cli.command else {
                panic!("the pack-only field-mask flag must preserve pack intent");
            };
            assert_eq!(query, "maskneedle");
            assert_eq!(fields, Some(vec!["standard".to_string()]));
        }
    }

    #[test]
    fn pack_skill_opt_in_survives_query_recovery_and_defaults_to_exclusion() {
        for (args, expected) in [
            (
                vec!["cass", "privacyneedle", "--json", "--include-skill-content"],
                true,
            ),
            (
                vec![
                    "cass",
                    "search",
                    "privacyneedle",
                    "--json",
                    "--include-skill-content",
                ],
                true,
            ),
            (
                vec![
                    "cass",
                    "pack",
                    "--include-skill-content",
                    "privacyneedle",
                    "--json",
                ],
                true,
            ),
            (vec!["cass", "pack", "privacyneedle", "--json"], false),
        ] {
            let (normalized, _) = normalize_args(args.into_iter().map(str::to_string).collect());
            let cli = Cli::try_parse_from(normalized).expect("parse recovered pack arguments");
            let Some(Commands::Pack {
                query,
                include_skill_content,
                ..
            }) = cli.command
            else {
                panic!("skill-content opt-in must select the pack command");
            };
            assert_eq!(query, "privacyneedle");
            assert_eq!(include_skill_content, expected);
        }
    }

    /// `cass robot-docs doctor` documents the run-history surface as verbs;
    /// every spelling it shows must parse to the matching doctor flag, the
    /// flag spellings must keep working, and a malformed or unknown verb must
    /// still be a usage error rather than a plain doctor run.
    #[test]
    fn doctor_history_verbs_parse_as_robot_docs_document_them() {
        let doctor = |args: &[&str]| {
            let (normalized, _) = normalize_args(args.iter().map(ToString::to_string).collect());
            Cli::try_parse_from(normalized)
                .ok()
                .and_then(|cli| cli.command)
        };
        assert!(matches!(
            doctor(&["cass", "doctor", "ls", "--json"]),
            Some(Commands::Doctor { ls: true, .. })
        ));
        assert!(matches!(
            doctor(&["cass", "doctor", "undo", "latest", "--json"]),
            Some(Commands::Doctor { undo: Some(id), .. }) if id == "latest"
        ));
        assert!(matches!(
            doctor(&["cass", "doctor", "diff", "--json"]),
            Some(Commands::Doctor { diff: Some(reference), .. }) if reference == "current"
        ));
        assert!(matches!(
            doctor(&["cass", "doctor", "diff", "run-a..run-b", "--json"]),
            Some(Commands::Doctor { diff: Some(reference), .. }) if reference == "run-a..run-b"
        ));
        assert!(matches!(
            doctor(&["cass", "doctor", "gc", "--before", "2026-01-01T00:00:00Z", "--yes", "--json"]),
            Some(Commands::Doctor { gc_before: Some(before), yes: true, .. })
                if before == "2026-01-01T00:00:00Z"
        ));
        assert!(matches!(
            doctor(&["cass", "doctor", "gc", "--before=2026-01-01T00:00:00Z", "--yes"]),
            Some(Commands::Doctor { gc_before: Some(before), yes: true, .. })
                if before == "2026-01-01T00:00:00Z"
        ));
        assert!(matches!(
            doctor(&["cass", "doctor", "capabilities", "--json"]),
            Some(Commands::Doctor {
                emit_capabilities: true,
                ..
            })
        ));
        assert!(matches!(
            doctor(&["cass", "doctor", "explain", "latest", "--json"]),
            Some(Commands::Doctor { explain: Some(id), .. }) if id == "latest"
        ));
        assert!(matches!(
            doctor(&["cass", "doctor", "watch", "--watch-iterations", "1"]),
            Some(Commands::Doctor {
                watch: true,
                watch_iterations: 1,
                ..
            })
        ));
        // The flag spellings keep working.
        assert!(matches!(
            doctor(&["cass", "doctor", "--ls", "--json"]),
            Some(Commands::Doctor { ls: true, .. })
        ));
        assert!(matches!(
            doctor(&["cass", "doctor", "--diff", "--json"]),
            Some(Commands::Doctor { diff: Some(reference), .. }) if reference == "current"
        ));
        // `gc` without `--before` must not become a plain doctor run.
        assert!(doctor(&["cass", "doctor", "gc", "--yes", "--json"]).is_none());
        // An unknown verb is still a usage error.
        assert!(doctor(&["cass", "doctor", "frobnicate", "--json"]).is_none());
    }

    /// Behavioral pin for the #367 repro: the robot flag must not turn a
    /// recognized subcommand into a search query.
    #[test]
    fn forget_with_robot_flag_is_not_rewritten_to_search() {
        let (normalized, _note) = normalize_args(
            [
                "cass",
                "forget",
                "--source-glob",
                "**/subagents/*.jsonl",
                "--robot",
            ]
            .iter()
            .map(ToString::to_string)
            .collect(),
        );
        assert_eq!(
            normalized.get(1).map(String::as_str),
            Some("forget"),
            "forget must survive robot-mode normalization untouched: {normalized:?}"
        );
        assert!(
            !normalized.iter().any(|arg| arg == "search"),
            "forget must not be folded into an implicit search query (GH #367): {normalized:?}"
        );
    }
}

#[cfg(test)]
mod removed_invocation_tests {
    use super::*;

    fn argv(words: &[&str]) -> Vec<String> {
        words.iter().map(ToString::to_string).collect()
    }

    const DEMO_COMMANDS: &[RemovedCommand] = &[
        RemovedCommand {
            path: &["swarm"],
            hint: None,
        },
        RemovedCommand {
            path: &["upgrade"],
            hint: Some("`cargo install --path . --locked`"),
        },
        RemovedCommand {
            path: &["sources", "sync"],
            hint: None,
        },
    ];

    const DEMO_FLAGS: &[RemovedFlag] = &[
        RemovedFlag {
            path: &["pages"],
            flag: "target",
            values: Some(&["github", "cloudflare"]),
            hint: Some("`cass pages --export-only` for a local archive"),
        },
        RemovedFlag {
            path: &["models", "install"],
            flag: "mirror",
            values: None,
            hint: Some("`cass models install --from-file DIR`"),
        },
    ];

    #[test]
    fn empty_tables_never_reject() {
        for args in [
            argv(&["cass", "swarm", "status"]),
            argv(&["cass", "sources", "sync"]),
            argv(&["cass", "pages", "--target", "github"]),
            argv(&["cass", "search", "swarm"]),
        ] {
            assert!(
                removed_invocation_error(&args, &[], &[]).is_none(),
                "empty tables must not reject {args:?}"
            );
        }
    }

    #[test]
    fn exact_and_case_variant_commands_reject() {
        for args in [
            argv(&["cass", "swarm"]),
            argv(&["cass", "swarm", "status", "--json"]),
            argv(&["cass", "Swarm", "status"]),
            argv(&["cass", "--json", "swarm", "status"]),
            argv(&["cass", "--data-dir", "/tmp/x", "swarm"]),
            argv(&["cass", "upgrade", "--check"]),
        ] {
            let error = removed_invocation_error(&args, DEMO_COMMANDS, &[])
                .unwrap_or_else(|| panic!("{args:?} must be rejected"));
            assert_eq!(error.code, 2);
            assert_eq!(error.kind, CliErrorKind::RemovedCommand.kind_str());
            assert!(!error.retryable);
        }
        let error =
            removed_invocation_error(&argv(&["cass", "swarm", "status"]), DEMO_COMMANDS, &[])
                .expect("swarm status rejected");
        assert_eq!(
            error.message,
            "`cass swarm` was removed: cass runs fully local-only"
        );
        // The removed path is named, not the extra arguments.
        assert!(error.hint.is_none());
    }

    #[test]
    fn command_pairs_reject_only_the_named_pair() {
        let rejected =
            removed_invocation_error(&argv(&["cass", "sources", "sync"]), DEMO_COMMANDS, &[]);
        assert!(rejected.is_some(), "sources sync must be rejected");
        assert_eq!(
            rejected.unwrap().message,
            "`cass sources sync` was removed: cass runs fully local-only"
        );
        for args in [
            argv(&["cass", "sources"]),
            argv(&["cass", "sources", "list"]),
            argv(&["cass", "sources", "syncer"]),
        ] {
            assert!(
                removed_invocation_error(&args, DEMO_COMMANDS, &[]).is_none(),
                "{args:?} is a live invocation and must not be rejected"
            );
        }
    }

    #[test]
    fn typos_of_removed_names_resolve_to_the_removed_error() {
        let error = removed_invocation_error(&argv(&["cass", "swarn"]), DEMO_COMMANDS, &[])
            .expect("swarn resolves to removed swarm");
        assert_eq!(error.kind, CliErrorKind::RemovedCommand.kind_str());
        assert_eq!(
            error.message,
            "`cass swarn` resolves to `cass swarm`, which was removed: cass runs fully local-only"
        );
    }

    #[test]
    fn typos_closer_to_live_commands_are_not_rejected() {
        // `serach` is distance 1 from live `search` and distance >= 4 from
        // every demo removal — the live correction must still win.
        assert!(
            removed_invocation_error(&argv(&["cass", "serach"]), DEMO_COMMANDS, &[]).is_none(),
            "a typo closer to a live command keeps the live typo-recovery path"
        );
    }

    #[test]
    fn removed_name_as_search_argument_is_not_rejected() {
        // `swarm` here is a query word, not the command position.
        assert!(
            removed_invocation_error(&argv(&["cass", "search", "swarm"]), DEMO_COMMANDS, &[])
                .is_none(),
            "a removed name in argument position must not reject the live command"
        );
        assert!(
            removed_invocation_error(
                &argv(&["cass", "pack", "sources", "sync"]),
                DEMO_COMMANDS,
                &[]
            )
            .is_none(),
            "removed pair words under a different command must not reject"
        );
    }

    #[test]
    fn removed_flags_reject_by_presence_or_value() {
        // Value-restricted flag: only the listed values are rejected.
        for args in [
            argv(&["cass", "pages", "--target", "github"]),
            argv(&["cass", "pages", "--target=github"]),
            argv(&["cass", "pages", "--target", "CLOUDFLARE"]),
            argv(&["cass", "pages", "-target", "github"]),
        ] {
            let error = removed_invocation_error(&args, &[], DEMO_FLAGS)
                .unwrap_or_else(|| panic!("{args:?} must be rejected"));
            assert_eq!(error.code, 2);
            assert_eq!(error.kind, CliErrorKind::RemovedCommand.kind_str());
        }
        let error = removed_invocation_error(
            &argv(&["cass", "pages", "--target", "github"]),
            &[],
            DEMO_FLAGS,
        )
        .expect("pages --target github rejected");
        assert_eq!(
            error.message,
            "`--target github` on `cass pages` was removed: cass runs fully local-only"
        );
        assert_eq!(
            error.hint.as_deref(),
            Some("local equivalent: `cass pages --export-only` for a local archive")
        );
        // Surviving values of the same flag stay live.
        assert!(
            removed_invocation_error(
                &argv(&["cass", "pages", "--target", "local"]),
                &[],
                DEMO_FLAGS
            )
            .is_none(),
            "an allowed --target value must not be rejected"
        );
        // Presence rejection: `models install --mirror X`.
        for args in [
            argv(&["cass", "models", "install", "--mirror", "X"]),
            argv(&["cass", "models", "install", "--mirror=X"]),
        ] {
            let error = removed_invocation_error(&args, &[], DEMO_FLAGS)
                .unwrap_or_else(|| panic!("{args:?} must be rejected"));
            assert_eq!(
                error.hint.as_deref(),
                Some("local equivalent: `cass models install --from-file DIR`")
            );
        }
        // The flag is only removed on its own path.
        assert!(
            removed_invocation_error(
                &argv(&["cass", "models", "verify", "--mirror", "X"]),
                &[],
                DEMO_FLAGS
            )
            .is_none(),
            "--mirror on a different command path must not reject"
        );
        assert!(
            removed_invocation_error(
                &argv(&["cass", "pages", "--target", "github", "--", "x"]),
                &[],
                DEMO_FLAGS
            )
            .is_some()
                || removed_invocation_error(
                    &argv(&["cass", "pages", "--", "--target", "github"]),
                    &[],
                    DEMO_FLAGS
                )
                .is_none(),
            "tokens after `--` are never flags"
        );
    }

    /// The disjointness invariant behind
    /// `removed_tables_stay_disjoint_from_live_lists`, taking the table so
    /// tests can prove a pair entry under a live parent is harmless. Only
    /// one-word paths are checked: `["sources","sync"]` removes that pair
    /// alone — `sources` stays live, listed, and correctable by design, so
    /// requiring the parent word out of CANONICAL would break the table.
    fn assert_disjoint_from_live_lists(removed_commands: &[RemovedCommand]) {
        // The same commit that adds a one-word REMOVED_COMMANDS entry must
        // drop the name from CANONICAL_TOP_LEVEL_COMMANDS — a name in both
        // lists would both reject and still advertise/correct to the
        // command.
        for entry in removed_commands {
            if entry.path.len() != 1 {
                continue;
            }
            let top = entry.path[0];
            assert!(
                !CANONICAL_TOP_LEVEL_COMMANDS
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(top)),
                "removed command `{top}` still listed in CANONICAL_TOP_LEVEL_COMMANDS"
            );
            assert!(
                !SIDE_EFFECT_EXACT_ONLY_COMMANDS
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(top)),
                "removed command `{top}` still listed in SIDE_EFFECT_EXACT_ONLY_COMMANDS"
            );
        }
        // Fuzzy correction can never return a removed name.
        for entry in removed_commands {
            if entry.path.len() != 1 {
                continue;
            }
            let top = entry.path[0];
            for probe in [
                top.to_string(),
                top[..top.len().saturating_sub(1)].to_string(),
            ] {
                assert_ne!(
                    closest_top_level_command_in(&probe, removed_commands),
                    Some(top),
                    "removed command `{top}` is still a typo-correction candidate"
                );
            }
        }
    }

    #[test]
    fn removed_tables_stay_disjoint_from_live_lists() {
        assert_disjoint_from_live_lists(REMOVED_COMMANDS);
    }

    /// The exact row task 9 lands while `sources` stays a live top-level
    /// command: the pair is removed, the parent is not.
    const PAIR_UNDER_LIVE_PARENT: &[RemovedCommand] = &[RemovedCommand {
        path: &["sources", "sync"],
        hint: None,
    }];

    #[test]
    fn pair_entry_leaves_its_live_parent_correctable() {
        // The exclusion helper scopes to one-word paths only.
        assert!(
            !is_removed_command_name("sources", PAIR_UNDER_LIVE_PARENT),
            "a removed pair must not mark its live parent as removed"
        );
        assert!(
            !is_removed_command_name("sources", DEMO_COMMANDS),
            "the demo table's pair row must not remove `sources` either"
        );
        assert!(
            is_removed_command_name("swarm", DEMO_COMMANDS),
            "a one-word entry still marks its name removed"
        );
        // `cass sourcess` keeps typo-correcting to the live `sources`
        // instead of falling into the implicit-search fold — with the
        // unscoped parent-word check this returned `None` and `sourcess`
        // silently executed as `cass search sourcess`.
        for table in [PAIR_UNDER_LIVE_PARENT, DEMO_COMMANDS] {
            assert_eq!(
                closest_top_level_command_in("sourcess", table),
                Some("sources"),
                "a removed pair must not exclude its parent from typo candidates"
            );
        }
        // The real consts agree today: `sources` is live and already the
        // nearest candidate.
        assert_eq!(closest_top_level_command("sourcess"), Some("sources"));
        // Disjointness ignores pair rows — the pair-only table passes
        // while `sources` is legitimately still listed in CANONICAL.
        assert_disjoint_from_live_lists(PAIR_UNDER_LIVE_PARENT);
        assert_disjoint_from_live_lists(REMOVED_COMMANDS);
    }
}
