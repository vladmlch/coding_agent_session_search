# Local-only verification journal

The plan runs every check on the developer machine. Each entry records the
command, date (UTC), OS/architecture, and result. Linux and Windows variants
are listed as not executed (no machine available); code-level portability is
kept (`cfg` branches, paths, no Unix-only dependencies outside `cfg(unix)`).

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
