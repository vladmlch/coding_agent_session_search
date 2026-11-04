# Local-only verification journal

The plan runs every check on the developer machine. Each entry records the
command, date (UTC), OS/architecture, and result. Linux and Windows variants
are listed as not executed (no machine available); code-level portability is
kept (`cfg` branches, paths, no Unix-only dependencies outside `cfg(unix)`).

## Resolved decisions (local-only plan, confirmed 2026-11-04)

| # | Decision |
| - | -------- |
| D1 | Keep upstream mergeability. Local-only deletions land as grouped commits and every deleted path is logged in the ledger below; **on merge conflict between upstream and a local deletion, keep the deletion**. `AGENTS.md` "# State" carries the same rule. |
| D2 | Previously synced mirror directories (`<data-dir>/remotes/*/mirror`) stay as read-only archive roots with their original identity; DB rows are always kept. |
| D3 | `cass sources mappings` stays — it is a local archive path-rewrite. |
| D4 | `cass sources add` becomes the local form `sources add <name> --path ...`. |
| D5 | The `cass pages` wizard drops publication steps and keeps local interactive export (filters, password, recovery key, preview on 127.0.0.1). |
| D6 | Flags that lost their meaning (`export-html --no-cdns`, `models install --mirror`) are removed with a clear error, not kept as hidden no-ops. |
| D7 | `.beads/` (and `tests/.beads/`) stay untouched as task history; beads references are removed from instructions and code only. |
| D8 | The robot-contract break is explicit: `contract_version` bumped, crate `0.10.0 → 0.11.0`, recorded in `docs/VERSION_HISTORY.md`. |
| D9 | The `.github/workflows` ban in `scripts/gate.sh` and `tests/no_github_actions.rs` stays, reframed as "the project has no remote CI". |
| D10 | The pre-push bead-id hook (`scripts/git-hooks/{install,pre-push}.sh`), its e2e (`scripts/tests/cuu3f_prepush_hook_e2e.sh`) and `tests/agents_md_documents_commit_convention.rs` are deleted — a coordination-era rule. |
| D11 | The three anti-reward-hacking principles are inlined into `AGENTS.md` "RULE 0.5" (never weaken a gate; a self-speedup is maintenance, not a win; reporting a loss is a success). The `/data/projects/AGENTS.md` link and the mandatory Morph Warp Grep reference are removed. |
| D12 | The 166 `CODING_AGENT_SEARCH_NO_UPDATE_PROMPT=1` lines in tests go in one mechanical commit (Task 15). |
| D13 | One verification run is budgeted at 15 minutes; long suites are split by targets/filters instead of raising timeouts. |

## Deleted paths log

Ledger for D1: every path deleted by the local-only port, grouped by task.
**On merge conflict with upstream, keep the deletion.** Newest task first;
each task appends its own group.

### Task 5 — coordination-era hooks and bead-id convention (2026-11-04)

- `scripts/git-hooks/install.sh` — opt-in installer that symlinked the pre-push hook into `.git/hooks/` (D10)
- `scripts/git-hooks/pre-push.sh` — warned on pushes to `main` whose commits named no bead-id (D10)
- `scripts/tests/cuu3f_prepush_hook_e2e.sh` — e2e exercising the hook against a temp repo (D10)
- `tests/agents_md_documents_commit_convention.rs` — pinned the bead-id commit convention in AGENTS.md (D10)

### Task 4 — release publication and network installers (2026-11-04)

- `install.sh`, `install.ps1` — network installers; local install is `cargo install --path . --locked`
- `packaging/homebrew/coding-agent-search.rb` — Homebrew formula
- `docs/INSTALLER_SPEC.md`, `docs/planning/DOCTOR_V2_RELEASE_CHECKLIST.md` — installer/release docs for the removed flow
- `tests/install_scripts.rs`, `tests/e2e_install_easy.rs`, `tests/doctor_release_checklist.rs` — tests of the removed flow
- `tests/fixtures/install/` — installer fixtures (7 files)

### Task 3 — remote execution removed from helper scripts (2026-11-04)

- `.rch/config.toml`, `.rchignore` — remote-build fleet configuration
- `scripts/e2e/multi_machine_sync.sh`, `scripts/e2e/sources_sync.sh` — remote sync e2e scripts
- `scripts/e2e/live_fleet_search.py` — live fleet search harness
- `tests/docker/Dockerfile.sshd`, `tests/docker/entrypoint.sh` — sshd fixtures for remote tests
- `tests/e2e/cloudflare/cloudflare-smoke.spec.ts` — hosted deploy smoke spec

### Task 1 — local cargo defaults (2026-11-04)

- `tests/scripts_rch_compliance.rs` — superseded by `tests/scripts_local_cargo_compliance.rs`, which asserts the whole `scripts/` tree invokes cargo locally

## Task 2 — `scripts/gate.sh` local blocking gate (2026-11-04)

Machine: macOS 27.0.1 (Darwin 27.0.0, arm64, Apple Silicon). Rust nightly-2026-08-31 (pinned). UBS v5.4.4 pinned at `scripts/ubs-version.txt`, installed at `~/tools/ultimate_bug_scanner` (`UBS_BIN`/`UBS_MODULE_DIR` set for gate runs).

| Date (UTC) | Command | Result |
| ---------- | ------- | ------ |
| 2026-11-04 | `cargo fetch --locked` | ok (rc=0) |
| 2026-11-04 | `CARGO_NET_OFFLINE=true scripts/gate.sh --lib-only` (first run, pre-fix) | RED in 575s: `fmt EXIT=1`, `clippy EXIT=101`, `lib-tests EXIT=101`, `ubs EXIT=1`; receipt `/tmp/gate-libonly.receipt`. Pre-existing baseline drift/lints found; fixed in place (see report). |
| 2026-11-04 | `CARGO_NET_OFFLINE=true scripts/gate.sh --no-lib` (first run, UBS env unset) | RED in 393s: `fmt/clippy EXIT=0`, `test-cli_robot EXIT=101` (1 baseline failure), `ubs EXIT=1` (runner not found — live proof of the missing-runner diagnostic); receipt `/tmp/gate-nolib.log`. |
| 2026-11-04 | `scripts/gate.sh --lib-only --lib-filter gate_task2_broken_test_expects_red` (deliberate injected failing test) | RED in 240s as required: `STAGE=lib-tests EXIT=101`, `gate: RED`. Receipt `/tmp/gate-red1.log`. |
| 2026-11-04 | `scripts/gate.sh --lib-only --lib-filter test_apply_ionice` while appending a line to `scripts/test-quick.sh` mid-run | RED as required: `gate: checkout changed during validation (working-tree diff changed, input content changed)`, `STAGE=source-stability EXIT=1`. Receipt `/tmp/gate-red2b.log`. A prior attempt that wrote to the wrong worktree reported `STAGE=source-stability EXIT=0` — the stable-tree direction observed live. |
| 2026-11-04 | `CARGO_NET_OFFLINE=true scripts/gate.sh --lib-only` (post-fix, UBS env set) | exceeded D13 budget: killed at 900s during the UBS scan (lib-tests alone took 784s under shared-target-dir load). Stage evidence captured: `fmt 0, clippy 0, lib-build 0, lib-tests 101` (same 38 baseline failures). Receipt `cass-gate.XXXXXX.msEQCSbwn9`. Full post-fix pipeline additionally proven by the two deliberate-RED receipts, which exercise every stage end to end. |
| 2026-11-04 | `CARGO_NET_OFFLINE=true scripts/gate.sh --no-lib` (post-fix, UBS env set) | RED in 365s: `fmt 0, clippy 0, test-cli_robot 101` (1 baseline failure), all other test stages 0, `ubs 1` (baseline scope findings), `source-stability 0`, `job-complete 0`; complete receipt `cass-gate.XXXXXX.7X6jhLZWEc`, log `/tmp/gate-nolib2.log`. |
| 2026-11-04 | `cargo test --locked --test scripts_local_cargo_compliance --test no_github_actions` | ok: 15+2 passed |
| 2026-11-04 | `scripts/tests/dpfvr_ubs_gate_e2e.sh --batched-only` | ok: PASS=92 FAIL=0 |
| 2026-11-04 | `scripts/tests/dpfvr_ubs_gate_e2e.sh --formatter-only` | ok: PASS=5 FAIL=0 |
| 2026-11-04 | `scripts/tests/dpfvr_ubs_gate_e2e.sh` (live tail) | ok: PASS=3 FAIL=0 |
| 2026-11-04 | `cargo test --locked --test cli_robot html_export_aliases_route_to_export_html_command` (standalone baseline check) | FAILED — identical outside the gate; depends on ambient default data dir (fixture must be pre-indexed). Not caused by this change. |
| — | `unshare -rn … scripts/gate.sh` (Linux sandboxed variant) | not executed — no Linux machine available |

Baseline notes: `lib-tests` reports 38 failures of the documented macOS
environmental classes (non-UTF8 path `os error 92`, `/var` vs `/private/var`
symlink handling, `nice` range, annotation sidecar, daemon timing); 8923 pass.
The UBS stage scans files changed vs `git merge-base origin/main HEAD` — on
this branch that is the full local-only/feature stack (32+ files), so its
finding counts are dominated by pre-existing heuristic findings in baseline
files; files changed by this task contributed only `SC1091` info notes.

## Task 4 — remove release publication and network installers (2026-11-04)

Machine: macOS (Darwin arm64, Apple Silicon). Rust nightly-2026-08-31 (pinned).

| Date (UTC) | Command | Result |
| ---------- | ------- | ------ |
| 2026-11-04 | `cargo metadata --locked --no-deps` | ok (rc=0); package now reports `publish: []` |
| 2026-11-04 | `cargo test --locked --test docs_safety_invariants` | ok: 3 passed, 0 failed |
| 2026-11-04 | `cargo test --locked --test e2e_jsonl_schema_test --no-run` | ok: compiles (sanity for the sources-list edit) |
| 2026-11-04 | `scripts/validate_docs.sh` | rc=1 — all 165 errors are pre-existing/environmental, none introduced by this change: 125 `README key … no matching arm` come from BSD `grep` lacking `-P` (validate_docs.sh:388) on macOS, and 40 `Broken link` reports are `[text](#anchor)` TOC false positives present on the base revision (verified identical link counts vs `git show HEAD:<file>`). README section checks, `cass … --help` flag truth (492 usages), env-var truth (70 vars), and `cargo doc` all pass. |

## Task 5 — AGENTS.md local-only rules + hook removal (2026-11-04)

Machine: macOS (Darwin arm64, Apple Silicon). Rust nightly-2026-08-31 (pinned).

| Date (UTC) | Command | Result |
| ---------- | ------- | ------ |
| 2026-11-04 | `cargo test --locked --test docs_safety_invariants` | ok: 3 passed, 0 failed |
| 2026-11-04 | `cargo test --locked --test golden_robot_json agents_md_does_not_recommend_remote_offload` | ok: 1 passed (rewritten docs-consistency test — pins local cargo, forbids rch/dsr/warp_grep/MCP tokens in AGENTS.md) |
| 2026-11-04 | `cargo check --all-targets` | ok (rc=0; only the pre-existing `nix v0.28.0` future-incompat note) |
| 2026-11-04 | `scripts/validate_docs.sh` | rc=1 — failure set byte-identical to the pre-change baseline (166 errors: 125 `README key … no matching arm` from BSD `grep` lacking `-P`, 41 `[text](#anchor)` TOC false positives). Zero introduced by this change. |

## Mandatory checklist: every manually-maintained command list (Tasks 7–14)

Task 6 built the single rejection mechanism (`REMOVED_COMMANDS` /
`REMOVED_FLAGS` in `src/lib.rs`, kind `removed-command`). Removing a
command from clap is **not** enough — the surfaces below enumerate
commands, flags and workflows by hand and must be updated **in the same
commit** that lands a `REMOVED_*` entry, plus the matching
`REMOVED_COMMAND_CASES` row in `tests/cli_dispatch_coverage.rs` flips
`pending: false`. Line numbers are the 2026-11-04 snapshot — locate by
name.

### `src/lib.rs`

- `Commands` enum variant + clap attributes — removing it automatically
  drops the command from CLI parse, `--help`, `cass completions`,
  `cass man`, and capabilities `commands[]` (all clap-derived).
- `REMOVED_COMMANDS` / `REMOVED_FLAGS` (~line 6691) — append the entry;
  `hint` may name a local equivalent (reported, never executed).
- `CANONICAL_TOP_LEVEL_COMMANDS` (~line 6605) — drop the name;
  `removed_invocation_tests::removed_tables_stay_disjoint_from_live_lists`
  pins the disjointness invariant.
- `SIDE_EFFECT_EXACT_ONLY_COMMANDS` (~line 6664) — drop if present.
- `SUBCOMMAND_ALIASES` (inside `normalize_args`, ~line 5664) — drop every
  alias whose target is now removed.
- `KNOWN_LONG_FLAGS` (inside `normalize_args`, ~line 5499) — drop removed
  flags so `-mirror`/`--MIRROR`/`--mirror_x` stop normalizing into live
  flags.
- `describe_command` (~line 25390) — drop the variant arm.
- `format_missing_subcommand_error` (~line 6243) — drop removed
  groups/subcommands from its listings.
- `robot_docs_topic_shorthand` + `recover_structured_help_requests`
  (~lines 3752, 4060) — drop shorthand words routing to removed topics.
- `print_robot_help` + `print_robot_docs` (~line 27750) — the `commands`,
  `env`, `sources`, `guide`, `examples`, `exit-codes` topic bodies are
  hand-written line lists: remove every removed invocation/flag mention.
- `heuristic_parse_recovery` `known_flags` (~line 7185) — drop removed
  flags so a flag typo cannot correct into a removed flag.
- Dispatch arms: `execute_cli`, `try_run_with_parsed_fast` (e.g. the swarm
  observer fast path), `command_uses_current_thread` in `src/main.rs`.
- Capabilities builders: `build_workflow_capabilities` (~99067),
  `build_mistake_recovery_capabilities` (~99171),
  `build_env_var_capabilities` (~98712), `build_global_flag_schemas`
  (~98445), `build_exit_code_capabilities` (~98572), the `features` list
  (~99500), `capabilities_connector_names` (~86701).
- Triage/readiness: `readiness_recommended_commands` (~24667),
  `triage_recovery_recommended_commands` (~88490), `starter_workflows`
  (~88773), `response_schema_recommended_commands` (~100713).
- Introspect: every `response_schema_*` describing the removed surface
  (~100596–102203+) and its registration in the introspect payload.
- `src/main.rs` `raw_command_name` flag lists and the early-dispatch
  commands (`serve`, `archive`, `codex-layout-migrate`,
  `lexical-quarantine`, semantic-reclaim args) — only if one of those
  names is removed.

### Robot-docs topic modules

- `src/recipes_robot_docs.rs` — recipes naming removed commands/flags.
- `src/doctor_robot_docs.rs` — doctor topic text for removed surfaces.

### TUI

- `src/ui/components/palette.rs` — palette entries (e.g. `OpenSwarm`).
- `src/ui/shortcuts.rs` — shortcut list entries.
- `src/ui/app.rs` — help overlay lines, `AppSurface` variants and nav
  order, update-banner actions.
- `src/ui/components/help_strip.rs` — help strip entries.

### Tests that pin these lists (drift guards)

- `tests/spec_starter_workflows_drift.rs`
- `tests/spec_recommended_commands_drift.rs`
- `tests/spec_mistake_recoveries_contract.rs`
- `tests/spec_robot_docs_topics.rs` (`DOCUMENTED_TOPICS`)
- `tests/spec_introspect_envelope_completeness.rs`,
  `tests/spec_introspect_schema_integrity.rs`
- `tests/spec_exit_codes_documented.rs`,
  `tests/spec_aliases_resolve.rs`,
  `tests/spec_auto_correction_contracts.rs`
- `tests/cli_dispatch_coverage.rs` `REMOVED_COMMAND_CASES` — flip the row.
- Golden files under `tests/golden/robot/` and `tests/golden/` that embed
  the command (capabilities, introspect, onboarding, diag, doctor, …) —
  regenerate only with `UPDATE_GOLDENS=1`, read every diff line, explain
  it in the commit message.

### Docs

- `SKILL.md`, `README.md`, `docs/ROBOT_MODE.md`,
  `docs/ERROR_CODES.md` (new kinds), this file's journal.

## Task 6 — unified rejection for removed commands (2026-11-04)

Machine: macOS (Darwin arm64, Apple Silicon). Rust nightly-2026-08-31 (pinned).

|| Date (UTC) | Command | Result |
|| ---------- | ------- | ------ |
|| 2026-11-04 | `cargo test --locked --lib removed_invocation_tests` | ok: 8 passed, 0 failed (synthetic tables: exact/case/pair/flag/flag-value rejection, typo resolves-to-removed, live-typo precedence, `--` boundary) |
|| 2026-11-04 | `cargo test --locked --test cli_dispatch_coverage removed_commands_error_without_redirection` | ok: 1 passed (17 pending rows probed live via `<cmd> --help`, empty `PATH`, empty data dir) |
|| 2026-11-04 | `UPDATE_GOLDENS=1 cargo test --locked --test golden_error_envelope` | golden regen: +`removed-command` kind `[2]`; `total_kinds` 87→89 (+6 kinds — `removed-command` plus `ambiguous-identity`, `conversation-not-indexed`, `index-read-failure`, `index-unavailable`, `no-renderable-messages` already emitted on HEAD (stale golden); −4 kinds with no remaining producer: `opencode-parse`, `opencode-sqlite-parse`, `session-not-found`, `session-parse`; `unknown-agent` exit_codes `[3]`→`[2,3]`) — full diff explanation in commit `dc0286ff` |
|| 2026-11-04 | `cargo test --locked --test cli_dispatch_coverage --test spec_auto_correction_contracts --test golden_error_envelope --test spec_robot_docs_topics` | ok — see commit message |
