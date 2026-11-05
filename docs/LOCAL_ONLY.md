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

### Task 15 — remaining remote deps, contract v2, D12 env sweep (2026-11-05)

No whole-file deletions this task. Removed in place:

- `Cargo.toml` — direct `ssh2` (SFTP fallback), vendored `openssl` (Unix-only
  target section), and `url` dropped; `semver` moved to
  `[dev-dependencies]` (only `spec_version_semver_format` and
  `spec_diag_version_coherence` consume it); `asupersync` feature
  `tls-native-roots` dropped (no remote TLS callers remain).
- `Cargo.lock` — `ssh2`, `libssh2-sys`, `openssl`,
  `openssl-sys`, `openssl-src`, `openssl-macros`, `openssl-probe`,
  `foreign-types`, `schannel`, `rustls`, `rustls-pki-types`,
  `rustls-webpki`, `rustls-native-certs`, `asn1-rs*`, `der-parser`,
  `oid-registry`, `rusticata-macros`, `x509-parser`, `num-bigint`,
  `num-integer`, `libz-sys` leave the graph; `url` stays only as a
  `franken-agent-detection` transitive. Review fix-round 1 disclosure:
  not strictly removal-only — cass's `base64` dependency edge re-unified
  `0.23.1 → 0.22.1` (resolver noise from the removed crates; both versions
  remain in the graph and cass's `base64 = ">=0.21"` requirement is
  satisfied by either). No other version changes; the crate's own version
  bump `0.10.0 → 0.11.0` is the D8 contract bump, not dep drift.
- `build.rs` — asupersync `expected_features` contract updated to
  `["test-internals"]` (exact-match gate).
- `src/lib.rs` — `CONTRACT_VERSION` `"1" → "2"` (D8); the
  `CODING_AGENT_SEARCH_NO_UPDATE_PROMPT` `deprecated_env_vars` entry removed
  inside the mechanical D12 commit — **re-added in review fix-round 1**:
  the rg-zero mandate applies to test/script setup lines, while the
  deprecated-list entry is the tolerance mechanism that gives a stale env
  var its deprecation diagnostic (parity with `CASS_SKIP_UPDATE` /
  `CASS_UPDATE_API_BASE_URL`, which stayed deprecated-ignored).
- `tests/html_export_e2e.rs` — three `export_redaction_*` tests moved from
  removed `cass sources reingest --source fixture --full --json` to
  `cass index --full` pinned to the scratch `HOME`; `index --full` reads the
  fixture's existing `data/remotes/fixture/mirror` read-only, the documented
  local equivalent.
- `tests/cli_robot.rs`, `tests/golden/robot/*.golden`,
  `tests/fixtures/cli_contract/{api_version,capabilities,introspect}.json` —
  `contract_version` assertions/values updated `"1" → "2"`.
- `AGENTS.md` — dependency table rows for `reqwest` and `ssh2` removed.
- `deny.toml`/`audit.toml` — stale advisory ignores for crates no longer in
  the graph (`bincode`, `rustls-pemfile`, `yaml-rust`/`syntect`, `lru`
  0.12.5) removed; `paste` exception retained (still transitive).
- `docs/VERSION_HISTORY.md` — 0.11.0 breaking-change entry + matrix row.

Deferred-orphan evaluation (D-deferred): `src/dependency_pin_correlation.rs`
and the `SwarmEvidence*` API in `src/pages/redact.rs` are exported pure-logic
diagnostic surfaces with zero in-tree consumers; retained as public library
API (file deletion is out of scope without explicit authorization).
`with_verified_bundle_for_deployment` is consumed by in-module tests.
`explainability.rs` `run cass models install` card text is live local-command
guidance, not stale. Exit-143 in `src/lib.rs` is really emitted: the
`(signal(SignalKind::terminate()), 143)` tuple at
`IndexShutdownSignals::new` carries the code as a runtime value through
`received`/`poll_exit_code()` into five `CliError` constructions (index
join path, index completed-after-stop, `abort_if_needed` stderr
envelope, both `run_models_backfill` cancellation arms) and one
`process::exit`, and the backfill report classifies cancellation via
`matches!(error.code, 130 | 143)`. Review fix-round 1 corrected this
entry's earlier assessment: those emissions WERE scanner-invisible
(variable `code:` fields) and 143 was missing from the capabilities
exit_codes contract. Every emission site now spells per-arm literal
`code: <digits>` CliErrors / `process::exit(<digits>)` (semantics
identical), and 143 is documented in `cass capabilities --json`,
robot-docs exit-codes, and the README/AGENTS tables. The ssh2/SFTP clause
in `docs/UNIT_TEST_HARNESS_REQUIREMENTS.md` is retained
historical spec text for a completed rollout bead.

### Task 13 — external HTTP embedding provider (2026-11-05)

Whole paths removed (all pre-authorized in the task brief's «Удаляемые пути»):

- `docs/EXTERNAL_EMBEDDINGS.md` — operator doc for the removed endpoint
- `src/search/external_embedder.rs`, `src/search/external_embedder/retry.rs`, `src/search/external_embedder/wire.rs` — HTTP client, retry policy and request/response wire format
- `src/search/external_embedder/tests.rs`, `src/search/external_embedder/tests/admission.rs`, `src/search/external_embedder/tests/retries.rs` — provider-specific test module
- `src/search/model_manager/external.rs`, `src/search/model_manager/external/tests.rs`, `src/search/model_manager/external/tests/admission_races.rs` — external availability and admission machinery
- `tests/cli_search_service_external.rs`, `tests/external_backfill_runtime.rs`, `tests/external_backfill_runtime/cli.rs` — integration/runtime suites for the HTTP provider

Removed in place (no further whole-file deletions):

- `Cargo.toml`/`Cargo.lock` — `reqwest` and `rustls` (plus transitive-only deps) dropped; cass no longer links an HTTP client or TLS stack
- `src/search/embedder_registry.rs` — `external` is no longer a selectable provider; unrecognized/legacy values fall back to local policy
- `src/search/semantic_manifest.rs` — `is_external_identity` moved here; legacy external identities detected for provenance
- `src/search/asset_state.rs` + status/doctor/health/state/triage serializers — tiers now emit `legacy_external_embedding_space`, and the shared introspect schema declares it
- `src/indexer/semantic/engine.rs` + callers — `crate::search::external_embedder::CancelCheck` replaced by a local semantic `CancelCheck`; `check_external_cancelled` → `check_cancelled`, `external_cancelled` → general field
- `src/search_service.rs`, `src/search_service/semantic.rs` — `--semantic-embedder external` still parses (help marks it REMOVED) and refuses startup with a removed-command envelope; the `external` status branch deleted
- `src/search_service/mcp.rs` — `EXTERNAL_INSTRUCTIONS` deleted; instructions state `cass_semantic_search` sends no queries to external services; tool metadata is `readOnlyHint: true`, `openWorldHint: false`
- `src/lib.rs` — value-aware `REMOVED_FLAGS` entry rejects `--embedder external` with local alternatives; ten `CASS_EXTERNAL_EMBEDDING*` env vars reported `deprecated-ignored`; `CASS_SEMANTIC_EMBEDDER=external` ignored in favour of canonical local selection; external backfill validation and model-revision branches deleted
- `src/schedule.rs` — legacy `external` quality policy treated as unknown → local MiniLM probe; fast scheduled backfill uses `hash`
- `src/indexer/mod.rs`, `src/indexer/semantic.rs`, `src/indexer/semantic/delegate*.rs`, `src/indexer/semantic/engine/replay_cache.rs`, `src/indexer/semantic/unchanged*` — external admission/cancellation plumbing removed or renamed; cancellation tests rewritten against the local `hash` embedder (no HTTP fixture)
- `tests/golden/robot/*`, `tests/fixtures/cli_contract/introspect.json` — regenerated: `--embedder` description lists only local providers; semantic tier payloads and their introspect schemas carry `legacy_external_embedding_space`

Kept per the brief: `EmbedderChoice::External` remains a clap value whose
help text says REMOVED, so `serve --semantic-embedder external` emits the
targeted local-only refusal instead of a generic invalid-value error.
Unrelated `external` vocabulary (`external_id`, external archive sources,
`external_upstream_source` taxonomy, external deadline supervision) is
untouched. Legacy external artifacts stay on disk: manifests remain
readable, tier status reports them via `legacy_external_embedding_space`,
and they are never admitted for serving. `EXTERNAL_EMBEDDER` and
`EXTERNAL_VECTOR_SPACE_REVISION` were compile-time constants, not env vars,
so they do not appear in the deprecated-env table.

### Task 12 — model downloads/update-check/consent machinery (2026-11-04)

Whole paths removed (all pre-authorized in the task brief's «Удаляемые пути»):

- `tests/golden/robot/models_check_update_not_installed_shape.json.golden` — orphaned golden of the removed `models check-update` response shape
- `tests/snapshots/cassapp_consent_dialog.snap` — orphaned TUI snapshot of the model-download consent dialog
- `tests/snapshots/cassapp_consent_dialog_downloading.snap` — orphaned TUI snapshot of the download-progress dialog

Removed in place (no further whole-file deletions):

- `src/search/model_download.rs` — HTTP downloader, resumable downloads, mirrors, Hugging Face URL building, `ensure_rustls_crypto_provider`, download progress/state machine arms; the file is now the local `--from-file` installer (manifest, pinned SHA256, `<target>.installing` staging, `.verified` marker, atomic promote/rollback)
- `src/search/model_acquisition.rs` — download/offline/repair lifecycle states (`offline_blocked`, `partial_download`, `needs_update`) → local states (`partial_install`, `explicit_install_required`); `next_command` now emits `cass models install --model <id> --from-file <DIR>`
- `src/search/policy.rs`, `src/policy_registry.rs` — `ModelDownloadPolicy`, its env lookup and report field removed; `CASS_SEMANTIC_DOWNLOAD_POLICY` survives only as a deprecated-ignored introspection entry
- `src/lib.rs` — `ModelsCommand::CheckUpdate`, `Install::{mirror, yes}`, `Verify::repair` deleted; removed-command/removed-flag tables carry the replacements; exit-code contract drops unemitted 21–23 (emitted model codes are now 20 and 24)
- `src/search/model_manager.rs`, `src/search/asset_state.rs`, `src/ui/app.rs` — `NeedsConsent`/`Downloading`/`Verifying`/`UpdateAvailable` UI states gone; `SemanticAvailability::IndexStale` serializes `index_stale` (was `update_available`); consent dialog replaced by the local-install hint
- `tests/golden/robot/` + `tests/fixtures/cli_contract/introspect.json` — regenerated sans `models-check-update` schema, download policy fields and dead exit codes

Kept per the brief: HF repo ids as provenance metadata (`huggingface_id`
fields, `ModelSource::Mirror` parse-only provenance for `.verified` markers
written by the pre-local-only downloader), `ErrorKind::Download` as
historical-envelope vocabulary (same convention as the retained `ssh`,
`setup`, `lexical-generation` kinds), and the doctor wire fields
`network_allowed`/`auto_download_*` whose `false` values now truthfully
describe local-only behaviour.

### Task 9 — remote source sync/setup/discovery machinery (2026-11-04)

- `src/sources/sync.rs`, `src/sources/sync/rrsync_tests.rs` — SSH/SFTP/rsync mirror sync engine and its rrsync tests
- `src/sources/install.rs` — remote cass install over SSH
- `src/sources/probe.rs` — remote host probing
- `src/sources/setup.rs` — interactive/non-interactive remote setup wizard driver
- `src/sources/interactive.rs` — setup wizard TUI flow
- `src/sources/index.rs`, `src/sources/index/job.rs` — remote index job runner
- `src/sources/tailscale_tests.rs` — Tailscale peer discovery tests
- `tests/ssh_sync_integration.rs`, `tests/ssh_test_helper.rs`, `tests/e2e_ssh_sources.rs` — live-SSH sync suites and the SSH test helper
- `tests/tailscale_discovery.rs` — Tailscale discovery suite
- `tests/setup_workflow.rs` — remote setup workflow suite

Kept (checked per the brief's conditions): `src/sources/child_output.rs`
(used by non-SSH child-process capture) and
`tests/reproduction_sync_oscillation.rs` (adapted: reproduces the archived
mirror-oscillation regression through ordinary `cass index`, no live sync).

### Task 10 — scheduler remote-sync stages (2026-11-04)

No whole files deleted — remote scheduling machinery removed in place:

- `src/schedule.rs` — remote-source sync stage (`due_remote_sources` call, `sources-sync:{name}`/`sources-discovery`/`sources-sync` steps, `use crate::sources::sync` reach), `pub use discovery::{SourceScheduleError, due_remote_sources}`, sync mentions in module/enum/step docs
- `src/schedule/discovery.rs` — `due_remote_sources`/`due_sources_at` remote-due planner, `SourceScheduleError::{Configuration, SyncHistory}`, `use crate::sources::sync::{SourceSyncAction, SyncStatus}` — module kept as the read-only legacy-`sync_schedule` diagnostic
- `tests/schedule_admission.rs` — source-planning refusal cases (`sources-discovery`, `source_configuration_unavailable`, `source_sync_history_unavailable`) replaced by never-consulted coverage

### Task 8 — fleet, release verification, self-update and remote doctor surfaces (2026-11-04)

- `src/fleet_archive_coverage.rs`, `src/fleet_doctor_schema.rs`, `src/fleet_platform_compat.rs`, `src/fleet_probe.rs`, `src/fleet_upgrade_rehearsal.rs`, `src/fleet_version_skew.rs` — fleet command tree: archive coverage, doctor schema, platform compat, live probe, upgrade rehearsal, version skew
- `src/search/fleet_cheap_probes.rs` — cheap fleet-probe evidence feeders for search incidents
- `src/release_verify.rs` — release-channel verification (`release-verify`)
- `src/source_doctor_health.rs` — remote source-doctor health probes
- `src/update_check.rs` — update checker (GitHub release polling, `update_state.json`, TUI banner)
- `tests/release_verify_cli.rs`, `tests/e2e_fleet_upgrade_rehearsal_gate.rs` — CLI/e2e suites for the removed flows
- `tests/golden/robot/fleet_upgrade_rehearsal_shape.json.golden` — orphaned golden
- `tests/doctor_fixtures/fm-update_check-clock-rollback/` — update-check clock-rollback doctor fixture

### Task 7 — swarm, Agent Mail, Beads, lessons and operations dashboards (2026-11-04)

- `src/swarm_status.rs`, `src/swarm_replay_fixture.rs` — swarm operations snapshot and its replay-fixture harness
- `src/operations_dashboard.rs`, `src/operations_dashboard/projection.rs`, `src/operations_dashboard/repository.rs` — operations dashboard projection/repository
- `src/lessons.rs`, `src/lessons_extraction.rs` — durable-lessons store and mining
- `src/context_pack.rs`, `src/repro_capsule.rs` — coordination handoff builders (context pack, repro capsule)
- `src/dependency_drift.rs` — sibling dependency-drift sentinel for coordinated releases
- `src/workflow_analytics.rs` — multi-agent workflow analytics fed by coordination events
- `tests/cli_live_swarm_dashboard.rs`, `tests/coordination_intelligence_gate.rs`, `tests/e2e_guided_ops_golden_gate.rs`, `tests/e2e_lessons_gate.rs`, `tests/e2e_repro_capsule_gate.rs`, `tests/operations_dashboard_contract.rs`, `tests/swarm_status_contract.rs` — contract/e2e suites for the removed surfaces
- `tests/fixtures/guided_ops/` (3), `tests/fixtures/lessons/corpus.evidence.json`, `tests/fixtures/swarm_status/` (9) — orphaned fixtures
- `tests/golden/guided_ops/` (2), `tests/golden/swarm_status/` (8) — orphaned goldens

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

## Task 7 — swarm/Agent Mail/Beads/lessons/operations surfaces removed (2026-11-04)

Machine: macOS (Darwin arm64, Apple Silicon). Rust nightly-2026-08-31 (pinned).

|| Date (UTC) | Command | Result |
|| ---------- | ------- | ------ |
|| 2026-11-04 | `cargo test --test cli_dispatch_coverage` | ok: 110 passed (swarm/lessons rows active, `pending: false`; structured `removed-command` rejection verified live) |
|| 2026-11-04 | `cargo test --test e2e_search_trust_meta_gate` | ok: 61 passed — trust correlation rewritten to local-git commit provenance only; no `bead:`/`lesson:`/`release:` refs |
|| 2026-11-04 | `cargo test --test legacy_settings_tolerance` | ok: 59 passed — `CASS_SWARM_AGENT_MAIL_{URL,TOKEN}` ignored (status `deprecated-ignored`, never dereferenced) |
|| 2026-11-04 | `cargo test --test golden_robot_docs` | ok: 16 passed — robot-docs goldens regenerated sans swarm/lessons entries |
|| 2026-11-04 | `cargo test --test cli_robot capabilities_matches_golden_contract` + introspect golden tests | ok — capabilities/introspect goldens updated for removed commands/env vars |
|| 2026-11-04 | `cargo test --test tui_flows` | ok: 65 passed — swarm surface removed; persisted `surface:"swarm"` falls back to Search |
|| 2026-11-04 | `cargo test --test e2e_guide_gate` | ok: 52 passed — guide adapters now evaluate resource-plan/privacy-preview in-process (`cass-internal` pseudo-argv) |
|| 2026-11-04 | `cargo fmt --check` | ok |
|| 2026-11-04 | `cargo clippy --all-targets --all-features -- -D warnings` | lint-clean on compiled targets; 3 pre-existing diagnostics fixed (2 test files). `--locked` re-run blocked by machine-level disk-full (`os error 28`, 116Mi free on /) — no lint emitted before I/O failure |
|| 2026-11-04 | `cargo test --test cli_robot` | 324 passed, 1 pre-existing failure: `html_export_aliases_route_to_export_html_command` — depends on ambient default data dir (fixture must be pre-indexed); identical on HEAD (see Task 2 journal row) |
|| 2026-11-04 | `cargo test --test spec_exit_codes_documented` | pre-existing failure `every_documented_exit_code_is_emitted`: capabilities documents exit 70 (index stall watchdog, `src/indexer/checkpoint_deadline.rs`), but the spec's `SOURCES` list (lib.rs + doctor*.rs) has never included the emitting file — unemitted={70} on HEAD too |
|| 2026-11-04 | `cargo test --test spec_starter_workflows_drift --test spec_recommended_commands_drift --test spec_mistake_recoveries_contract --test spec_robot_docs_topics --test spec_introspect_envelope_completeness --test spec_introspect_schema_integrity --test spec_aliases_resolve --test spec_auto_correction_contracts` | ok |

## Task 8 — fleet/release-verify/upgrade/update machinery removed (2026-11-04)

Machine: macOS (Darwin arm64, Apple Silicon). Rust nightly-2026-08-31 (pinned).

| Date (UTC) | Command | Result |
| ---------- | ------- | ------ |
| 2026-11-04 | `cargo clippy --all-targets --locked -- -D warnings` | ok |
| 2026-11-04 | `cargo fmt --all -- --check` | ok |
| 2026-11-04 | `cargo test --locked --test cli_dispatch_coverage` | ok: 110 passed — `fleet`, `release-verify`, `upgrade` rows active (`pending: false`), each rejects through the structured `removed-command` error with a local-equivalent hint |
| 2026-11-04 | `cargo test --locked --test cli_doctor` | ok: 117 passed — `remote_source_sync` report block gone; `network_source_sync` reports `"status": "removed"`; `local_mirror_source_coverage` scenario replaces multi-machine sync coverage |
| 2026-11-04 | `cargo test --locked --test doctor_e2e_runner` | ok: 99 passed — remote/sync/fleet root-cause families removed |
| 2026-11-04 | `cargo test --locked --test cli_status` | ok: 10 passed — `remote_source_sync` asserted absent from `status --json`/`health --json` |
| 2026-11-04 | `cargo test --locked --test e2e_sources` | ok: 115 passed — `sources sync`/`sources discover`/Tailscale discovery kept live (separate sources workstream) |
| 2026-11-04 | `cargo test --locked --test e2e_guide_gate` | ok: 65 passed — remote steps dropped; `fix-ci-regression`/`prepare-release` macros removed |
| 2026-11-04 | `cargo test --locked --test e2e_guide_apply_gate` | ok: 65 passed |
| 2026-11-04 | `cargo test --locked --test e2e_guide_live_proofs` | ok: 67 passed |
| 2026-11-04 | `cargo test --locked --test e2e_tui_smoke_flows` | ok: 77 passed — replacement test `tui_pty_launches_without_update_banner_or_release_requests` points `CASS_UPDATE_API_BASE_URL` at a 127.0.0.1 stub and asserts zero requests, no banner/prompt |
| 2026-11-04 | `cargo test --locked --test tui_flows` | ok: 6 passed |
| 2026-11-04 | `cargo test --locked --test golden_robot_json` | ok: 52 passed — goldens regenerated sans removed commands/env vars |
| 2026-11-04 | `cargo test --locked --test golden_robot_docs` | ok: 16 passed |
| 2026-11-04 | `cargo test --locked --test e2e_resilience_capstone_gate` | ok: 65 passed — coverage golden regenerated for the 15 → 14 subsystem matrix (`update_check` row removed) |
| 2026-11-04 | `cargo test --locked --test cli_robot` | 325/326 — one pre-existing environmental failure `html_export_aliases_route_to_export_html_command` (ambient default data dir; identical on HEAD, see Task 2 journal row). `CASS_FLEET_*`/`CASS_SKIP_UPDATE`/`CODING_AGENT_SEARCH_NO_UPDATE_PROMPT`/`CASS_UPDATE_API_BASE_URL` all reported `deprecated-ignored`, never honored |

One flaky observation: `cli_dispatch_coverage::
gh519_export_indexed_source_live_wal_exits_successfully_repeatedly` failed once
under parallel load and passed on re-run and in the final full run.

## Task 10 — scheduler remote-sync stages dropped (2026-11-04)

Machine: macOS (Darwin arm64, Apple Silicon). Rust nightly-2026-08-31 (pinned).

| Date (UTC) | Command | Result |
| ---------- | ------- | ------ |
| 2026-11-04 | `cargo test --locked --lib schedule::` | ok: 48 passed — new `schedule::discovery` unit tests pin the ignored-`sync_schedule` notice; existing outcome/lease/backfill tests unchanged |
| 2026-11-04 | `cargo test --locked --test schedule_admission` | ok: 4 passed — forced incremental+nightly on a `type = "ssh"`, `sync_schedule = "hourly"` fixture index locally, spawn no `sources-*` step, and leave `sync_status.json` byte-identical/never created; lease and lock-control refusals intact |
| 2026-11-04 | `cargo test --locked --test cli_schedule` | ok: 9 passed — `schedule status --json` reports `warnings: ["ignoring deprecated sync_schedule on 1 source; …"]`, no sync fields; install dry-run renders identical unit argv |
| 2026-11-04 | `cass schedule install --dry-run --json` (manual, isolated HOME) | unit `ProgramArguments` still `cass --color=never schedule run --job {incremental,nightly} --data-dir <dir> --json` — unchanged vs Task 0 snapshot; existing launchd/systemd units need no rewrite |
| 2026-11-04 | `cargo fmt --all -- --check` | ok |
| 2026-11-04 | `cargo clippy --all-targets --locked -- -D warnings` | ok |

## Task 9 — sources local-only; remote sync/setup/discovery removed (2026-11-04)

Machine: macOS (Darwin arm64, Apple Silicon). Rust nightly-2026-08-31 (pinned).

| Date (UTC) | Command | Result |
| ---------- | ------- | ------ |
| 2026-11-04 | `cargo check --tests --locked` | ok — after `cargo clean` on the shared target dir recovered 35.4 GiB (disk was at 116 MiB free, `os error 28`) |
| 2026-11-04 | `cargo clippy --all-targets --locked -- -D warnings` | ok |
| 2026-11-04 | `cargo fmt --all -- --check` | ok |
| 2026-11-04 | `UPDATE_GOLDENS=1 cargo test --locked --test golden_robot_docs` | ok: 16 passed — regenerated `sources`/`env`/`exit-codes` goldens; every diff line reviewed (local-only sources text, `CASS_SSH_CONFIG` deprecated-ignored, exit 8 unused) |
| 2026-11-04 | `cargo test --locked --test e2e_sources` | ok: 104 passed — local `add NAME --path`, legacy `type="ssh"` tolerance, ordinary `cass index` over `<data-dir>/remotes/*/mirror`, `user@host` add → removed-command |
| 2026-11-04 | `cargo test --locked --test cli_dispatch_coverage` | ok: 106 passed — six Task-9 REMOVED_COMMAND_CASES rows flipped `pending:false` (each rejects with kind `removed-command`, exit 2); four `sources_artifact_manifest_verify_*` tests removed with their helpers |
| 2026-11-04 | `cargo test --locked --test multi_source_integration --test cli_stats_source_filter --test legacy_settings_tolerance --test local_archive_preservation --test tui_flows --test remote_mirror_ingestion --test e2e_remote_mirror_subtrees --test reproduction_sync_oscillation` | ok: 72+5+61+59+6+2+0(linux-only)+2 passed — legacy list JSON keeps `host`/`sync_schedule`/`platform`/`paths` with `kind: "legacy-remote"`, `supported: false`; index warns on stderr only; `agents exclude` preserves legacy fields |
| 2026-11-04 | `cargo test --locked --lib guide_runner` | ok: 25 passed |
| 2026-11-04 | `cargo test --locked --lib connectors::omp` | ok: 14 passed |
| 2026-11-04 | `cargo test --locked --lib indexer::` | 1532 passed, 27 failed — all failures are the documented macOS environmental classes (non-UTF8 path `os error 92`, `/var` vs `/private/var` symlink canonicalization, watch timing) identical in character to the Task 2 baseline notes; the `mirror_source_ids` removal is semantics-preserving (the parameter was always `None`, every collapsed branch kept the `None` arm) and cannot affect them |
| 2026-11-04 | `cargo test --locked --test spec_starter_workflows_drift --test spec_recommended_commands_drift --test spec_mistake_recoveries_contract --test spec_robot_docs_topics --test spec_aliases_resolve --test spec_auto_correction_contracts` | ok |
| 2026-11-04 | `cargo test --locked --test spec_exit_codes_documented` | ok: 5 passed — `every_documented_exit_code_is_emitted` green for the first time: dead `8` entry removed from the contract + README/AGENTS rows, the real `process::exit(70)` emitter (`src/viewer_bridge/deadline.rs`) added to the scan SOURCES (fixes the pre-existing {70} baseline gap), and the still-real 130 exit made scannable via a literal `code: 130` CliError on the no-signal-registered `IndexInterrupted` arm |
| 2026-11-04 | `UPDATE_GOLDENS=1 cargo test --locked --test golden_robot_json` | ok: 52 passed — capabilities golden regenerated; diff is exactly the removed `code: "8"` entry and the retexted `130` `agent_action` |

## Task 11 — full reindex preserves archived remote rows (2026-11-04)

Machine: macOS (Darwin arm64, Apple Silicon). Rust nightly-2026-08-31 (pinned).

| Date (UTC) | Command | Result |
| ---------- | ------- | ------ |
| 2026-11-04 | `cargo test --test local_archive_preservation` | ok: 63 passed — extended the Task-0 fixture with: configured `type = "ssh"` mirror ingest under `remotes/<src>/mirror/<safe>/<basename>/…`, emptied `sources.toml` (DB-fallback scan roots), manual mirror-dir deletion, remote row with no `sources` entry, `index --full` vs `--force-rebuild` snapshot equality, remote `origin_kind` after force rebuild, `cass forget` tombstone surviving a later `--full`, and a read-surface sweep (search/view/expand/export/`export-html --encrypt`/`pages --export-only`/timeline/`analytics tokens`/`models backfill` + semantic hit) |
| 2026-11-04 | `cargo test --test remote_mirror_ingestion` | ok: 2 passed |
| 2026-11-04 | `cargo test --test cli_forget` | ok: 78 passed — explicit deletion path intact |
| 2026-11-04 | `cargo test --lib indexer::` | 1533 passed, 26 failed — same documented macOS environmental classes as the Task 9 baseline (non-UTF8 `os error 92`, `/var`↔`/private/var` canonicalization, HNSW index-version, watch timing); all `build_scan_roots_*`, `mirror_scan_skip_tests` and `forgotten_source_admission` tests green |
| 2026-11-04 | `cargo fmt --all -- --check` | ok |
| 2026-11-04 | `cargo clippy --test local_archive_preservation` | ok |
| 2026-11-04 | `cass doctor --json` on fixture with remote mirror dir removed | `sources_config` pass; `sessions` warn is `source-authority-unsafe`/`verify-source-authority-before-rebuild` with `fix_available: false`, `default_outcome_kind: "blocked"` — informational, never a deletion/auto-repair trigger |

No production change was needed: `build_scan_roots_with_missing` produces no scan root for a vanished source and nothing prunes canonical rows on that account; `sync_sources_config_to_db` only upserts; `--force-rebuild` on a populated archive is a canonical-only lexical rebuild; deletion stays confined to `forget`/`sources remove --purge`/dedup paths. One fixture correction: the mirror fixture now uses the basename-preserving layout `<safe>/session/<session>/messages.jsonl` — the `mirrored_store` resolver accepts only that shape (or a trailing-slash source) because contents-copy containers require `source.ends_with('/')`.

## Task 12 — models install from local files only (2026-11-04)

Machine: macOS (Darwin arm64, Apple Silicon). Rust nightly-2026-08-31 (pinned).

| Date (UTC) | Command | Result |
| ---------- | ------- | ------ |
| 2026-11-04 | `cargo fmt --all -- --check` | ok |
| 2026-11-04 | `cargo check --all-targets` | ok |
| 2026-11-04 | `cargo clippy --all-targets -- -D warnings` | ok |
| 2026-11-04 | `cargo test --lib search::model_download` | ok: 37 passed — local installer: missing/corrupt manifest files rejected, SHA256 enforced, `.installing` staging + `.verified` marker, atomic promote, previous install preserved on failure, legacy `.downloading` debris reaped |
| 2026-11-04 | `cargo test --test cli_model_lifecycle_contract` | ok: 14 passed — `install` requires `--from-file`; `--mirror`/`--yes`/`--repair`/`check-update` reject via removed-command/flag machinery; verify read-only and fail-open |
| 2026-11-04 | `cargo test --test cli_dispatch_coverage` | ok: 106 passed |
| 2026-11-04 | `cargo test --test e2e_model_acquisition_no_download_gate` | ok: 2 passed — `skipped_network_reason=explicit_install_required`, `fallback_mode=lexical`, `next_command` asserts `cass models install --model <id> --from-file <DIR>` |
| 2026-11-04 | `cargo test --test e2e_models_build_hnsw_gate` | ok: 61 passed |
| 2026-11-04 | `cargo test --test tui_flows` | ok: 6 passed — consent snapshots deleted per «Удаляемые пути», no test referenced them |
| 2026-11-04 | `cargo test --test golden_robot_json` | ok: 52 passed — capabilities/introspect/models-status/health/doctor goldens regenerated; every diff reviewed (local lifecycle states, `--from-file` next steps, `index_stale` wire value, dead exit codes 21–23 dropped, no `models-check-update` schema) |
| 2026-11-04 | `cargo test --test golden_error_envelope` | ok: 4 passed — regenerated: `download`/`ssh`/`setup`/`lexical-generation` kinds no longer emitted; `model` emits {20}, `io` drops 22 |
| 2026-11-04 | `cargo test --test spec_exit_codes_documented` | ok: 5 passed — capabilities, README and AGENTS tables now document only emitted codes; `21–23 unused` noted on the robot-docs line like `8 unused` |
| 2026-11-04 | `cargo test --test semantic_integration` | ok: 85 passed, 2 ignored — `test_offline_mode_environment` migrated to `models status` (scoped via `CASS_DATA_DIR`; `models status` takes no `--data-dir`) |
| 2026-11-04 | `cargo test --test lifecycle_matrix` | ok: 104 passed |
| 2026-11-04 | `cargo test --test metamorphic_introspect_schema` | ok: 1 passed — pack probe budget raised 1200 → 4000 tokens: the pack JSON envelope alone is ~1.7k estimated tokens, so a 1200 budget always evicts evidence into `omitted` before the schema probe can inspect it |
| 2026-11-04 | `cargo test --test cli_robot` | 325/326 — same pre-existing environmental failure as Tasks 2/8 (`html_export_aliases_route_to_export_html_command` requires a populated default data dir; identical on HEAD) |
| 2026-11-04 | manual probes: `models install` (no flag), `--mirror`, `--yes`, `verify --repair`, `check-update` | all reject: missing-arg clap error for the bare form; removed-command/removed-flag errors with `install --from-file DIR` hints for the rest |

`tests/fixtures/cli_contract/introspect.json` regenerated from the built
binary (same procedure as Task 9). `models status --json` emits only local
lifecycle states and `cass models install --from-file` next steps; semantic
and hybrid search fail open to lexical when the model is absent.

## Task 13 — external HTTP embedding provider removed (2026-11-05)

Machine: macOS (Darwin arm64, Apple Silicon). Rust nightly-2026-08-31 (pinned).

| Date (UTC) | Command | Result |
| ---------- | ------- | ------ |
| 2026-11-04 | `cargo check --all-targets` | ok |
| 2026-11-04 | `cargo clippy --all-targets -- -D warnings` | ok |
| 2026-11-04 | `cargo test --test cli_dispatch_coverage` | ok: 106 passed — expected removed-row count updated 17 → 19 (`models backfill --embedder external` added in both `--embedder external` and `--embedder=external` spellings; the flag itself stays live for local providers) |
| 2026-11-04 | `cargo test --test cli_search_semantic_flags` | ok: 14 passed |
| 2026-11-04 | `cargo test --test legacy_settings_tolerance` | ok: 61 passed — `CASS_EXTERNAL_EMBEDDING_URL` etc. tolerated and reported deprecated-ignored |
| 2026-11-04 | `cargo test --test search_service_once_semantic` | ok: 5 passed |
| 2026-11-04 | `cargo test --test lifecycle_matrix` | ok: 104 passed |
| 2026-11-04 | `cargo test --test semantic_integration` | ok: 85 passed, 2 ignored |
| 2026-11-04 | `cargo test --lib` | 611 passed; 15 pre-existing environmental failures (`Index version mismatch … expected v1, found v2` in native HNSW incremental tests — identical on HEAD, unrelated to this diff) |
| 2026-11-05 | `cargo test --test e2e_semantic_search` | ok: 71 passed |
| 2026-11-05 | `cargo test --test e2e_models_build_hnsw_gate` | ok: 61 passed |
| 2026-11-05 | `cargo test --test golden_robot_json` (UPDATE_GOLDENS=1, then plain) | ok: 52 passed — regenerated; every diff reviewed (embedder description drops `external`; tier payloads + introspect schemas gain `legacy_external_embedding_space`) |
| 2026-11-05 | `cargo test --test golden_error_envelope` | ok: 4 passed |
| 2026-11-05 | `cargo test --test spec_exit_codes_documented` | ok: 5 passed |
| 2026-11-05 | `cargo test --test metamorphic_introspect_schema` | ok: 1 passed — initially flagged the new runtime field as missing from the shared tier introspect schema; fixed by declaring `legacy_external_embedding_space` in the fast/quality tier schema block in `src/lib.rs` |
| 2026-11-05 | `cargo test --test tui_flows` | ok: 6 passed |
| 2026-11-05 | `cargo test --test cli_robot` | 325/326 — same pre-existing environmental failure as Tasks 2/8/12 (`html_export_aliases_route_to_export_html_command` requires a populated default data dir; identical on HEAD) |
| 2026-11-05 | `cargo fmt --all -- --check` | ok |
| 2026-11-05 | `cargo clippy --all-targets --locked -- -D warnings` | ok |

`tests/fixtures/cli_contract/introspect.json` regenerated from the built
binary (same procedure as Task 9/12). `repomix-output.xml.zip`,
`rs_files_over_1000_lines.txt` and `scripts/count_large_rs_files.sh` remain
untracked leftovers, not staged.

## Task 14 — Pages/HTML local archives + loopback preview only (2026-11-05)

Machine: macOS (Darwin arm64, Apple Silicon). Rust nightly-2026-08-31 (pinned).

| Date (UTC) | Command | Result |
| ---------- | ------- | ------ |
| 2026-11-05 | `cargo check --locked --lib` | ok |
| 2026-11-05 | `cargo fmt --all -- --check` | ok (after `cargo fmt` reformatted the touched regions) |
| 2026-11-05 | `cargo clippy --all-targets --locked -- -D warnings` | ok |
| 2026-11-05 | `cargo test --locked --test pages_bundle --test pages_preview_integration --test pages_wizard --test e2e_pages --test html_export_integration --test metamorphic_html_export --test spec_export_html_envelopes` | ok: 76+29+2+65+68+81+5 passed |
| 2026-11-05 | `cargo test --locked --test cli_dispatch_coverage` | ok: 106 passed — eight Task-14 rows flipped `pending:false` (`pages --target github|cloudflare`, `--project`, `--repo`, `--branch`, `--account-id`, `--api-token`, `export-html --no-cdns`); table length assertion updated 19 → 26; `pages_help_shows_options` now asserts hosted flags absent from `--help` |
| 2026-11-05 | `cargo test --locked --test pages_master_e2e --test pages_pipeline_e2e` | ok: 69+65 passed |
| 2026-11-05 | `cargo test --locked --test docs --test export_index_first --test pages_export_golden` | ok: 20+22+2 passed |
| 2026-11-05 | `UPDATE_GOLDENS=1 cargo test --locked --test golden_robot_json` then plain | ok: 52 passed — capabilities/introspect goldens regenerated; diff reviewed: `export-html --no-cdns` flag block removed, `pages --target` enum reduced to `local` with retexted description, `--project`/`--branch`/`--account-id`/`--api-token` blocks removed |
| 2026-11-05 | `UPDATE_GOLDENS=1 cargo test --locked --test pages_export_golden` | ok: 2 passed — `basic_export.html.golden`/`encrypted_export.html.golden` regenerated; every diff reviewed: `<html>` drops `class="no-prism no-tailwind"`, CDN-fallback CSS blocks removed, `Prism.highlightAllUnder` call removed from the decryption hook |
| 2026-11-05 | `cargo test --locked --lib -- pages:: html_export:: removed_ pages_deploy config_input` | 878 passed, 2 failed — `removed_invocation_tests::removed_flags_reject_by_presence_or_value` fixed in-diff (message now names the removed value `` `--target github` ``); `pages::tests::sqlite_wal_segment_paths_mirror_lossy_non_utf8_producer_basename` fails at `std::fs::write` creating a non-UTF8 basename (`os error 92`) — same documented macOS environmental class as Tasks 9/11 baselines, untouched by this diff |
| 2026-11-05 | `cargo test --locked --lib -- removed_invocation_tests` | ok: 9 passed |
| 2026-11-05 | `cargo test --locked --lib -- pages::docs:: pages::preview::` | ok: 31 passed — includes `test_preview_listener_binds_loopback_only` asserting the listener is loopback |
| 2026-11-05 | `cargo test --locked --test html_export_e2e` | 29 passed, 3 pre-existing failures — `export_redaction_{parity_for_live_and_archived_sources,policy_applies_before_encryption,raw_fidelity_requires_explicit_sensitive_option}` shell out to `cass sources reingest`, removed by Task 9 (`bb8bd8ad`); test file untouched by this diff and fails identically without it |
| 2026-11-05 | `rg 'TcpListener::bind|UdpSocket::bind'` | only `src/pages/preview.rs:391` binds TCP outside tests, on `127.0.0.1` (plus the loopback-bind unit test); all other listeners are Unix sockets (daemon, viewer bridge, test helpers) |
| 2026-11-05 | browser lane `scripts/browser_e2e_lane.sh` | not run — Playwright deps not provisioned on this machine; the lane now exits 2 with manual install guidance instead of auto-running `npm ci`/`playwright install`, invokes `npx --no-install playwright`, and builds via `run_cargo` |

Notes:

- `tests/fixtures/cli_contract/introspect.json` regenerated from the built
  binary (same procedure as Tasks 9/12/13); diff reviewed — exactly the
  removed `no-cdns` flag, the `local`-only target enum/description, and the
  removed hosted flags. `tests/fixtures/cli_contract/capabilities.json` is a
  static 0.4.1 contract fixture, not generated — untouched.
- `PagesDeployTarget` now has a single `Local` variant so hosted values can
  never appear in `--help`; `github`/`cloudflare` are rejected by
  `REMOVED_FLAGS` on raw args before clap parses.
- `src/pages_assets/index.html` footer GitHub link replaced with plain text;
  generated README says "This directory" and points to `cass pages --preview`
  / `index.html`.
- Config `deployment.{repo,branch,account_id,api_token}` fields survive only
  as diagnosed legacy keys (non-`local` target → `unsupported_deployment_target`;
  `env:` credentials are never resolved — CLOUDFLARE_* is never read).
- `tests/html_export_e2e.rs` reingest fallout (3 tests) reported for the
  owning task's backlog; not weakened or rewritten here.
- Untracked leftovers `repomix-output.xml.zip`, `rs_files_over_1000_lines.txt`,
  `scripts/count_large_rs_files.sh` remain untracked, not staged. Test-run
  byproducts `tests/fixtures/search_demo_data/agent_search.db.*` also stay
  untracked.

## Task 15 — remaining remote deps, contract v2, D12 env sweep (2026-11-05)

Machine: macOS (Darwin arm64, Apple Silicon). Rust nightly-2026-08-31 (pinned).

| Date (UTC) | Command | Result |
| ---------- | ------- | ------ |
| 2026-11-05 | `rg -l 'NO_UPDATE_PROMPT' .` | at sweep time: only `docs/LOCAL_ONLY.md` historical journal refs (2) outside `.beads/`/`.superpowers/`/`test-results/` artifacts. Review fix-round 1 re-added the `src/lib.rs` `deprecated_env_vars` entry and the README deprecated-vars row (the tolerance mechanism, not setup code) |
| 2026-11-05 | `cargo fmt --all -- --check` | ok |
| 2026-11-05 | `cargo check --tests --locked` (D12 commit) | ok — 156-file env-var sweep compiles |
| 2026-11-05 | `cargo check` (lockfile regen) | ok — lock diff removes ssh2, libssh2-sys, openssl*, rustls*, libz-sys and exclusive transitives; review fix-round 1 disclosed one non-removal: cass's `base64` edge re-unified 0.23.1→0.22.1 (both stay in graph, `>=0.21` satisfied) |
| 2026-11-05 | `cargo tree -i {ssh2,libssh2-sys,openssl,openssl-sys,rustls,webpki-roots,rustls-native-certs}` | all absent from the resolved graph |
| 2026-11-05 | `python3 scripts/check_dependency_features.py` | ok — 11/11 explicit dependency feature contracts match |
| 2026-11-05 | `cargo check --tests` | ok (semver dev-dep move, html_export_e2e repair, contract v2) |
| 2026-11-05 | `cargo clippy --all-targets --locked -- -D warnings` | ok |
| 2026-11-05 | `cargo test --locked --test html_export_e2e export_redaction` | ok: 5/5 — the three tests red since Task 14 (`parity_for_live_and_archived_sources`, `policy_applies_before_encryption`, `raw_fidelity_requires_explicit_sensitive_option`) pass on `cass index --full` against the fixture mirror |
| 2026-11-05 | `cargo test --locked --test cli_robot` | 325/326 — only pre-existing environmental `html_export_aliases_route_to_export_html_command` (ambient default data dir; identical class as Tasks 7/14 baselines, unrelated to this diff) |
| 2026-11-05 | `cargo test --locked --test docs_dependency_pins_match_cargo --test spec_version_semver_format --test spec_diag_version_coherence` | ok: 3+3+2 — pins test green after repairing the stale FAD git-rev doc rows (pre-existing drift: docs said `6b337ba…`, manifest pins `4544aa0c…`) |

Notes:

- Two commits: `0892a7f8` (mechanical D12 env sweep, 156 files, −371/+23)
  and the semantic Task 15 commit carrying deps, build.rs contract, D8
  version/contract bump, html_export_e2e repair, policy exceptions, and
  docs/ledger updates.
- `cass index --full` is the documented local reingest equivalent: it reads
  existing `<data-dir>/remotes/<name>/mirror/` directories read-only. The
  export-privacy tests pin `HOME` to the scratch root so `--full` ingests
  only the fixture mirror plus zero discovered local roots.
- Orphan evaluation conclusions recorded in the Task 15 ledger above;
  `src/dependency_pin_correlation.rs` and the `SwarmEvidence*` API stay as
  exported diagnostic surface (no file deletion without authorization).
- `audit.toml`/`deny.toml` shrank to the `paste` exception — removing
  ignores strengthens the advisory gate.
- Untracked leftovers (`repomix-output.xml.zip`,
  `rs_files_over_1000_lines.txt`, `scripts/count_large_rs_files.sh`,
  `tests/fixtures/search_demo_data/agent_search.db.*`) remain untracked,
  not staged, matching Task 14 precedent.

## Task 16 — stale-reference sweep: docs, plans, comments, goldens (2026-11-05)

Machine: macOS (Darwin arm64, Apple Silicon). Rust nightly-2026-08-31 (pinned).

| Date (UTC) | Command | Result |
| ---------- | ------- | ------ |
| 2026-11-05 | `UPDATE_GOLDENS=1 cargo test --test golden_robot_docs --test golden_robot_json` | ok — 16+52; diffs limited to the `pages` description (`static hosting` → `local browsing`) in capabilities/introspect and the model-cache `notes` string (`opt-in downloads` → `operator-installed files`) in doctor/doctor_quarantine |
| 2026-11-05 | `cargo test --test golden_robot_docs --test golden_robot_json` (non-update) | ok — 16+52 deterministic |
| 2026-11-05 | `cargo build --locked --bin cass` | ok |
| 2026-11-05 | `cargo test --locked --test docs_safety_invariants --test spec_robot_docs_topics --test golden_robot_docs` | ok — 3+4+16 |
| 2026-11-05 | `scripts/validate_docs.sh --truth` | 122 errors, all in the pre-existing macOS `grep -P`/`--include` cascade (README key-arm check); real checks pass (460 flag usages, 87 env vars). The earlier run also flagged a bogus `CASS_EXTERNAL_EMBEDDING_` glob token introduced by this sweep's env row — fixed by spelling the vars out, count back to the base class |

Notes:

- Sweep covered docs/, README.md, AGENTS.md, SKILL.md, src/ comments/help,
  tests comments/matrices, and dev configs. Stale `rch exec` offload
  prescriptions replaced with local Cargo everywhere they prescribed action;
  historical bead IDs and removal ledgers kept verbatim.
- `docs/reference/E2E_LOGGING_SCHEMA.md` strict-RCH section rewritten to
  describe the local bundle contract actually implemented by
  `scripts/e2e_logging_acceptance_test.sh` (no SSH handoff/artifact sync).
- `docs/UNIT_TEST_HARNESS_REQUIREMENTS.md` §6 rewritten from removed
  fleet/source-sync files to the surviving local source-archive surface;
  `docs/RESILIENCE_TEST_MATRIX.md` Epics 6/8 annotated `(removed 0.11.0)`
  rather than deleted (matrix is a historical rollout record).
- `tests/e2e_jsonl_schema_test.rs` source-scan list dropped three deleted
  files (`e2e_deploy.rs`, `e2e_ssh_sources.rs`, `ssh_sync_integration.rs`)
  that made `fs::read_to_string` fail — restores the test instead of
  weakening it.
- Retained-for-compat names reported, not renamed: `fleet_fixtures()`
  (readiness fixture generator), `redact_swarm_text` /
  `swarm_evidence_redaction_config` (pages redactor), `RunMode::Live` and
  `PerfWorkloadKind::SourceSync` (wire enums), `PLACEHOLDER_CHECKSUM`
  sentinel, `source_sync_deferral_card` (uncalled, kept per no-delete rule).
- `tests/fixtures/cli_contract/introspect.json` still carries the old
  `static hosting` pages description; unreferenced by tests and under the
  protected `tests/fixtures/**` umbrella — left untouched, flagged here.
- Untracked leftovers (`repomix-output.xml.zip`,
  `rs_files_over_1000_lines.txt`, `scripts/count_large_rs_files.sh`,
  `tests/fixtures/search_demo_data/agent_search.db.*`) remain untracked,
  not staged, matching Task 14/15 precedent.

## Task 17 — final audit: source sweep, CLI-surface diff, offline proof (2026-11-05)

Machine: macOS (Darwin 27.0.0 arm64, sw_vers 27.0.1). Rust nightly-2026-08-31 (pinned). Debug binary at the shared target dir.

### Audit gate (new)

`tests/local_only_audit.rs` — 8 tests, wired into `scripts/gate.sh`
`INTEGRATION_TESTS` (runs under `--no-lib`):

- `src_code_has_no_remote_client_or_remote_tool` — production `src/**`
  (every `#[cfg(test)]` item stripped, comments masked, the named
  diagnostics allowlist exempted): no `reqwest::`/`ssh2::`, no
  `Command::new` literal or `sh -c` wrapper naming
  `ssh|scp|sftp|rsync|tailscale|gh|wrangler|curl|wget`, no
  non-loopback `TcpStream::connect`/`TcpListener::bind`/`UdpSocket`, no
  `0.0.0.0`/`INADDR_ANY` literal.
- `pages_assets_load_only_archive_local_resources` — every text asset
  under `src/pages_assets/**` incl. `vendor/**` (minus
  `vendor/manifest.json` + comments): no `http(s)://`/`//` arg to
  `src=`, restricted `rel=`, `@import`, `url(`, `fetch(`,
  `importScripts(`, `new Worker(`, `import(`, `location=`.
- `export_templates_reference_no_external_resources` — same patterns on
  `src/html_export/*.rs` production code.
- `scripts_and_docs_carry_no_remote_instructions` — `scripts/**`,
  `docs/**` (.md/.txt/.sh/.py/.ps1/.toml), `AGENTS.md`, `README.md`,
  `SKILL.md`, `.config/nextest.toml`, `.cargo/config.toml`,
  `Cargo.toml`: no `rch exec`, `dsr`, `ssh `/`scp `/`sftp `/`rsync `/
  `tailscale `, `curl|wget … |sh`, removed `cass sources …|swarm|
  lessons|fleet|upgrade|release-verify` invocations, `models
  check-update`, `models install` without `--from-file`, removed
  `pages` deploy flags, `--embedder external`, `--allow-rch`,
  `--no-cdns` — unless the enclosing paragraph is a marked removal
  ledger / historical note.
- `cargo_manifest_and_lockfile_carry_no_remote_deps` — banned-crate
  list absent from `Cargo.toml`/`Cargo.lock`; `url` only via
  `franken-agent-detection`; `semver` dev-only.
- `removed_command_matrix_is_fully_active` — `REMOVED_COMMANDS` /
  `REMOVED_FLAGS` live in `src/lib.rs`; all 26
  `REMOVED_COMMAND_CASES` rows `pending: false`.
- `scanner_detects_synthetic_violations` — anti-tautology self-check:
  the scanner must fire on planted `Command::new("ssh")`,
  `reqwest::`, remote `<script src>`/stylesheet/`fetch`, and an
  unannotated `cass swarm` line (and stay silent on `<a href>`,
  relative src, `#[cfg(test)]` code, annotated ledgers).
- `audit_exception_surface_is_narrow` — every allowlist file still
  exists; every doc exemption keeps its marker; `.github/workflows`
  absent.

First gate run: `cargo test --test local_only_audit` → 8 passed / 0 failed (0.93 s).

### CLI surface comparison vs Task 0 baseline (`$TMPDIR/cass-baseline` → `cass-current`, same capture script)

| Surface | Result |
| ------- | ------ |
| Recursive `--help` | 116 files vs baseline 150. The 34 vanished files are exactly the removed-command set: `fleet*`(3), `lessons*`(5), `models check-update`, `release-verify`, `sources sync|setup|discover|reingest|artifact-manifest|schedule`(6), `swarm*`(16), `upgrade`. No retained command lost its help file. |
| Retained `--help` diffs (16 files) | All removal deltas: `export-html` (`--no-cdns` gone, `--include-secrets` reordered), `guide` (`fix-ci`/`prepare-release` intents + `--allow-rch` gone), `index`/`models backfill` (`external` embedder gone), `models install` (`--mirror`,`-y` gone; `--from-file` required), `models verify` (`--repair` gone), `pages` (deploy flags gone, "static hosting"→"local browsing"), `schedule run` ("+ due remote syncs" gone), `search` ("downloaded"→"installed"), `serve` (external embedder marked REMOVED), `sources`/`sources add`/`sources doctor`/`sources remove` (remote verbs/flags gone, local-only wording), `root` (removed entries). |
| `robot-docs` (10 of 14 topics differ) | commands: removed blocks; contracts: swarm rows gone; doctor: `pre-push.sh` hook gone; env: update-prompt + `CASS_FLEET_*` gone, `CASS_SSH_CONFIG`/`CASS_EXTERNAL_EMBEDDING*` now `deprecated-ignored`; exit-codes: 8→unused, 12→source, 23→unused, `download` kind gone, 143 added; examples: `rch exec` contributor check → `cargo test`; guide/recipes/schemas/sources: removed blocks, `legacy_external_embedding_space` field added. |
| `capabilities`/`introspect`/`api-version` JSON | version 0.10.0→0.11.0, `contract_version` 1→2; removed command/env/exit-code entries gone; `remote_source_sync`/`remote-source-sync-gaps`/`models-check-update` schemas gone; models-status contract rewritten (`partial_download`→`partial_install`, `expected_download_bytes`→`expected_install_bytes`, `skipped_network_reason` = never acquires outside `install --from-file`); subsystem_coverage 15→14. |
| `completions` (all 5 shells) | Diffs = removed commands/flags gone; only-in-current tokens are new local-only wording (`--from-file`, "local browsing", "Retained for CLI compatibility", "REMOVED:", "hosted targets removed"). |
| `man` | 0.10.0→0.11.0 + the same removed entries/descriptions. |
| Removed names in current surface | `fleet` only inside "the 2026-06-08 fleet report" (dated report name); `sources sync|setup|…` and `` `sources add user@host` `` only inside "Removed remote verbs (each fails with a removed-command error)"; `'local' or 'ssh'` only as the inert origin-kind field description; `SSH/rsync/SFTP` only inside "never attribute a failure to". No live invocations. |

### Offline proof — sandbox-exec, macOS (2026-11-05T03:08Z)

Profile (verbatim, per the brief):

```
(version 1)
(allow default)
(deny network-outbound)
(allow network-outbound (remote ip "localhost:*"))
```

All proxy vars unset (`env -u HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy NO_PROXY no_proxy`); scratch `HOME`/`CODEX_HOME`/`CASS_DATA_DIR` bounding the corpus to one real Codex-format fixture session; `CASS_AUTO_REFRESH=0` so no background indexer spawns. Every stderr scanned for `connection refused|network is unreachable|could not resolve|temporary failure|timed out|econnrefused|dns|no route to host` → zero hits.

| # | Command (under sandbox) | Exit | Evidence |
| - | ----------------------- | ---- | -------- |
| 1 | `cass index --full --data-dir $SBX/data --json` | 0 | session indexed, lexical published |
| 2 | `cass models install --model all-minilm-l6-v2 --from-file $SBX/model-stub/nonexistent --data-dir … --robot-format json` | 21 | `kind: model`, `retryable: false`: "source … is not a directory" — local I/O diagnostic, no network attempt |
| 3 | `cass models install --model all-minilm-l6-v2 --from-file $SBX/model-stub …` (stub files, wrong bytes) | 21 | "verification failed for model.safetensors: expected 53aa5117… got 6a38abc4…" — pinned SHA256 checked locally, names required files, `retryable: false` |
| 4 | `cass search "rate limiting" --mode lexical --json` | 0 | 4 hits from the fixture session |
| 5 | `cass index --semantic --embedder hash --data-dir … --json` | 0 | hash-embedder vector artifacts built locally |
| 6 | `cass search "rate limiting" --mode hybrid --json` | 0 | 4 hits (`--mode hybrid` is the hybrid-preferred clap value; `hybrid-preferred` literal is rejected with a tip — flag spelling drift vs the brief, not a remote path) |
| 7 | `TUI_HEADLESS=1 cass tui --once` | 0 | headless frame rendered |
| 8 | `printf 'pw\n' \| cass export-html <session> --source local --output-dir … --encrypt --password-stdin --json` | 0 | `codex_standalone_2024_11_20_1600_session.html` (encrypted) written |
| 9 | `cass pages --export-only …/archive.casspages --json` | 0 | SQLite bundle, `secret_scan.approval_state: clean` |
| 10 | `cass pages --config cfg --json` (password via `env:CASS_EXPORT_PASSWORD`) | 0 | full encrypted export → `site2/site` (40 files) |
| 11 | `cass pages --verify $SBX/site2/site --json` | 0 | `"status": "valid"` — required_files, config_schema, payload_manifest, size_limits, integrity, no_secrets_in_site, share_profile all passed |
| 12 | `cass pages --preview $SBX/site2/site --port 18080 --no-open` + `curl http://127.0.0.1:18080/` inside the same sandbox | serve + 200 | loopback-only outbound allowed by the profile; `default-src 'self'` CSP served; server stopped after probe |
| 13 | `cass schedule run --job incremental --force --data-dir … --json` | 0 | scheduled incremental job ran locally |
| 14 | `cass doctor --json --data-dir …` | 0 | structured doctor report offline |

`models install` happy path: not runnable — the pinned MiniLM files
(90,868,376-byte `model.safetensors`, sha256 `53aa5117…`) do not exist on
this machine and cannot be fetched offline; acquiring them is exactly the
operator's out-of-band step. Both failure paths prove the verifier works
entirely on local bytes with pinned SHA256s and emits local-file
diagnostics, never a download attempt. This is itself the strongest
evidence the acquisition path cannot reach the network.

### Platform matrix

| Platform | Targeted tests (T1–16) | `gate.sh --lib-only` | `gate.sh --no-lib` | Offline scenario |
| -------- | -------------------- | -------------------- | ------------------ | ---------------- |
| macOS (executed, this machine) | local_only_audit 8/8 green (see report for the per-task suite table) | fmt/clippy/lib-build green; lib-tests: 8092 passed / 54 failed → 18 stale-test debt cases fixed in place (list below); 36 remain — 14 frankensearch v1↔v2 format drift + ~22 macOS-environmental, categorized in the report | all stages green incl. `test-local_only_audit` and `test-cli_robot` (326/326 after hermetic fix); `ubs` red on first-ever run — 73 critical/29.5k warnings, heuristic noise in test fixtures, categorized in the report | sandbox-exec run above: all commands green or correctly-failing locally, zero network errors |
| Linux (`unshare -rn`) | not executed — no Linux machine available | not executed | not executed | not executed |
| Windows | not executed — no Windows machine available; semantic portability audited by source inspection (no cfg-gated remote code remains to port) | not executed | not executed | not executed |

### Gate findings fixed during this task

`test-cli_robot` had one real failure: `html_export_aliases_route_to_export_html_command`
(`tests/cli_robot.rs`) pointed the three export aliases at the *ambient* archive —
a leftover of the pre-index-first era (`c38aef9c`) that `aef4862e`'s index-first
test migration missed; it only passed on hosts whose real data dir already held
`tests/fixtures/html_export/edge_cases/single_message.jsonl` indexed. Fixed
hermetically (smallest integration point = the test itself): stage the fixture
into a private Claude-shaped home, ingest with `cass index --watch-once`, resolve
the stored identity via `SqliteStorage::open_readonly`, then assert the same
dry-run JSON contract per alias — mirroring
`tests/html_export_integration.rs::stage_indexed_session`. Win/lose split of
what the fix admits: identical alias-routing + dry-run assertions, now
deterministic; the ambient-archive dependence it loses was accidental, never a
designed assertion. Verified: 326/326 green inside the gate.

The `--lib-only` run (8092 passed / 54 failed) also carried real leftover debt
from the removal commits, each fixed at its smallest point:

- `src/model/cli_error_kind.rs` — `dc0286ff` added `ErrorKind::RemovedCommand`
  but never bumped `AUDITED_KIND_COUNT` (97→98) or its audit comment.
- `src/lib.rs` `subcommand_robot_output_tests` — one case still parsed removed
  `cass sources setup`; the match arm already expected `SourcesCommand::List`,
  so the argv is now `sources list`.
- `src/search/e2e_scenarios.rs` — `7a87ffed` updated the CI scenario count
  8→7 when it deleted `remote_unreachable_old_host` + `live_remote_replay`
  but left the command total at 11; now 9 with a comment naming the removals.
- `src/ui/app.rs` help test — expected an "Updates" help section that no
  longer exists; removed from the list with a note.
- `tests/snapshots/cassapp_search_surface_*.snap` (6 files) — stale goldens
  still rendering the removed `· Swarm ·` tab; regenerated with `BLESS=1`
  after verifying every removed content line contains `Swarm` and nothing
  else (win/lose: production intentionally changed, goldens stale — the
  only legitimate regen case).
- `src/top_session_summary.rs` — test fixtures used the deleted
  `remote_sync_auth` incident category; substituted retained `host_pressure`.
- `src/incident_discovery.rs` — fixture messages triggered the deleted
  `remote_sync_auth` category; substituted retained `semantic` signal text.
- `src/storage/sqlite.rs` — `reinclude_exact_session` seeded a "sibling"
  differing only in source_path, but the canonical merge identity
  `(source_id, agent, external_id)` has long ignored path; a distinct
  external id restores it to a real path sibling.
- `src/storage/sqlite/local_lookup_indexes.rs` — REAL PRODUCT BUG found by
  the audit: `ensure_snippets_message_id_index` ran `CREATE INDEX … ON
  snippets` on archives that predate the `snippets` table (v13 fixture),
  failing the whole open with `no such table`. Now guards on table
  existence first.

Remaining 36 failures are not local-only scope (categorized, all in the
report): 14 frankensearch fsvi v1↔v2 format-drift tests (12
`hnsw::incremental_tests` + 2 `ann_shards::wal_tests`), ~22 macOS-environmental
cases (non-UTF8 pathname `os error 92`, `/var`→`/private/var` TMPDIR
canonicalization, daemon `nice` range / non-UTF8 socket path, one
timeout-sensitive reconnect assertion).

Final `--no-lib` receipt on the committed tree (2026-11-05T03:50Z):
`cass-gate.XXXXXX.640SZ3RZLz`, DIFF_SHA256 `39fe969f…a6b5b` — all product
stages `EXIT=0` (fmt, clippy, all six integration tests, bin, goldens),
`ubs EXIT=1` as categorized above.

### User-data immutability (real default data dir)

SHA-256 before/after the whole sandboxed run — byte-identical:

| File | SHA-256 |
| ---- | ------- |
| `tui_state.json` | `d2a83c76…d3d049` |
| `update_state.json` | `b1aaff75…f4186` (legacy inert record, untouched) |
| `auto-refresh-state.json` | `504a801e…18ba` |
| `watch_state.json` | `5b598288…3d73` |
| `theme.json` | `b594de15…1ee90` |
| `sources.toml` | ABSENT before → ABSENT after (never created) |
| `~/Library/LaunchAgents/*cass*` | ABSENT before → ABSENT after |
| `*external*`/`*vector*` dirs | none exist |
| scratch `sources.toml` | ABSENT (run created none) |
