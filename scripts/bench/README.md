# Synthetic-archive A/B bench (`synthetic_ab.py`)

Measures two **prebuilt** cass installs side by side, in **one invocation**,
interleaved (`A B A B …`, alternating who goes first each repeat) on a fully
synthetic agent-session archive. Honors RULE 0.5: the incumbent and the
challenger see the same inputs in the same run — `cass index --full` wall
time + peak RSS on a fresh data dir, an incremental `cass index` after a
small appended batch, and strict viewer-search latency through
`cass-viewer-bridge search_sessions` (median + p90 per query, result counts
compared A vs B and against the generator's own expected counts).

Python 3 **stdlib only**; no cass source is modified, built, or forked by the
bench. It takes the two builds' binary directories as arguments.

## What it generates

A deterministic synthetic archive (seeded `random.Random(f"cass-ab-bench/{seed}/{index}")`
per session) under a scratch `$HOME`:

- **Claude Code** JSONL (`~/.claude/projects/<slug>/<uuid>.jsonl`):
  user/assistant turns, `tool_use`/`tool_result` blocks (`Read`, `Edit`,
  `Grep`, `Bash` — with `is_error` results), `ai-title` records, `cwd` →
  workspace.
- **Codex** rollouts (`~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl`):
  `session_meta` + `response_item` message/function_call records.
- **Strict-search lanes exercised:** a controlled vocabulary with a common
  term (`quokka`, ~80% of sessions), a rare term (`axolotl`, ~2%), a planted
  adjacent phrase (`quantum walrus`, ~5%), a guaranteed `Bash` tool_use in
  ~70% of Claude sessions, and >64K-char assistant texts every Nth session
  (`--big-every`) so region chunking matters. Planted tokens never appear in
  the filler pool, so every per-query session count is provable from the
  manifest.

The first batch's file mtimes are pinned to a fixed past epoch and the
incremental batch is stamped `now − 10 s` by the runner, so the full scans
never see the appended files and the incremental scans always do — far from
the 1 s `file_modified_since` grace boundary.

## Isolation

Every child (`cass index`, `cass-viewer-bridge`) runs with an environment
**constructed from scratch, never inherited**: scratch `HOME` (the synthetic
archive), scratch `CASS_DATA_DIR`/`--data-dir`, scratch `XDG_DATA_HOME` /
`XDG_CONFIG_HOME` / `TMPDIR`, and `CASS_AUTO_REFRESH=0`. The real user home,
the owner's archive and any live SQLite store are never read. The bench
refuses to run if the scratch `HOME` would be (or contain) the real `HOME`,
and never deletes anything.

## Building the two installs

Build each side separately, one compilation at a time, no-LTO release
profile (both sides of an A/B must use the same profile):

```bash
cd <repo>
export CARGO_PROFILE_RELEASE_LTO=false CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
CARGO_TARGET_DIR=<targets>/a lockf -k /tmp/cass-cargo.lock cargo build --release --bins --locked
CARGO_TARGET_DIR=<targets>/b lockf -k /tmp/cass-cargo.lock cargo build --release --bins --locked
```

`--a-dir`/`--b-dir` then point at the two `…/release/` directories (each
containing `cass` and `cass-viewer-bridge`).

## Smoke (A/A null — same commit built twice)

Small archive (~5k messages), 2 interleaved repeats, timed search rounds with
warm-up rounds first. Because both sides are the same source, the A–B deltas
**are** the measured noise floor:

```bash
python3 scripts/bench/synthetic_ab.py \
  --a-dir <targets>/a/release --b-dir <targets>/b/release \
  --work-dir /tmp/cass-ab-smoke \
  --messages 5000 --seed 42 --repeats 2 \
  --search-rounds 3 --search-repeats 3 --search-warmup-rounds 1 \
  --null-run --label "A/A null (5k smoke, $(git rev-parse --short HEAD))"
```

Exit 0 = all runs ok and all result counts match A vs B; exit 1 = see the
verdict section of the report.

## Full run (300k messages) — exact orchestrator command

Run on a standing Colab VM (branch pushed to the owner fork first), with both
builds of the comparison's two commits made there with the same no-LTO
release profile, then:

```bash
python3 scripts/bench/synthetic_ab.py \
  --a-dir <targets>/a/release --b-dir <targets>/b/release \
  --work-dir /tmp/cass-ab-full-300k \
  --messages 300000 --seed 42 --repeats 2 \
  --search-rounds 5 --search-repeats 3 --search-warmup-rounds 1 \
  --incremental-fraction 0.01 \
  --label "cass A/B 300k synthetic (A=<revA>, B=<revB>)" \
  --report-dir scripts/bench/results/full-300k-<date>
```

`--raw-mirror off` halves the run-dir disk use on large archives (the raw
mirror copies every source file into the data dir). Timing-sensitive
comparisons should use a quiet machine; the A/A null's noise floor from the
smoke is the threshold any A/B delta must clear.

Note on refusals: every `search_sessions` request runs under the bridge's
10 s request ceiling (`TIMEOUT_MS`, also the max budget). On large archives a
heavy strict query (e.g. `not_common`) may refuse on both builds — recorded
per sample and reported per query. The verdict demands *parity*: both builds
refusing identically is a pass for the A/B comparison; one refusing where the
other completes is a behavioral mismatch and fails the run.

## Outputs

- `<report-dir>/ab_report.json` — full machine-readable report (per-run
  receipts, per-query aggregates, count verdicts, delta percentages).
- `<report-dir>/ab_report.md` — human summary with the verdict table.
- `<work-dir>/logs/` — every child's argv, exit code, stdout/stderr tail
  (index runs) and per-batch bridge logs.
- `<work-dir>/archive/manifest.json` — generator bookkeeping: session/message
  counts, expected strict-search counts per query, vocabulary.

`--self-test` validates the generator with no cass binaries: determinism
(two generations byte-identical), planted-count honesty (disk scan vs
manifest), JSONL parseability, query wire shapes, and the helper parsers.
`--generate-only` writes an archive + manifest and exits, for inspection.
