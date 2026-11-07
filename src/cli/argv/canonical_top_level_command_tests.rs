use super::heuristic_parse_recovery::heuristic_parse_recovery;
use super::normalize::{
    CANONICAL_TOP_LEVEL_COMMANDS, REMOVED_COMMANDS, REMOVED_FLAGS, closest_top_level_command,
    format_friendly_parse_error, normalize_args, removed_invocation_error,
};
use super::structured_output_request_span_at::looks_like_top_level_command_or_typo;
use crate::cli::args::{Cli, Commands};
use crate::model::cli_error_kind::ErrorKind as CliErrorKind;
use clap::{CommandFactory, Parser};

#[test]
pub(crate) fn ann_setup_help_names_a_parseable_index_command() {
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
pub(crate) fn ann_index_parse_error_explains_the_index_command() {
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
pub(crate) fn canonical_list_covers_every_clap_subcommand() {
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
pub(crate) fn side_effectful_commands_are_never_typo_attractors() {
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
pub(crate) fn exact_canonical_commands_are_never_rerouted() {
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
pub(crate) fn flag_typo_on_exact_subcommand_keeps_the_subcommand() {
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
pub(crate) fn pack_field_mask_contract_flag_survives_query_recovery() {
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
pub(crate) fn pack_skill_opt_in_survives_query_recovery_and_defaults_to_exclusion() {
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
pub(crate) fn doctor_history_verbs_parse_as_robot_docs_document_them() {
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
pub(crate) fn forget_with_robot_flag_is_not_rewritten_to_search() {
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
