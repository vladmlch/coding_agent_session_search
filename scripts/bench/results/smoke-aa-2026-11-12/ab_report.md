# Synthetic A/B bench — A/A null (5k smoke, b4fe3dad)

- generated: `2026-11-12T01:12:08Z`
- platform: Darwin 27.0.0 (arm64), python 3.14.7
- A: `/Volumes/SSD_1TB_M2/work/targets/cass-ab-bench/a/release/cass` (sha256 `90af394ba716cc4c…`)
- B: `/Volumes/SSD_1TB_M2/work/targets/cass-ab-bench/b/release/cass` (sha256 `90af394ba716cc4c…`)
- binaries identical: True
- archive: **82 sessions / 5216 messages** (claude 50, codex 32; v1 79 sessions + incremental 3); seed 42

## Index — `cass index --full` (fresh data dir)

| build | ok | wall (mean) | wall (min…max) | peak RSS | conversations | messages |
|---|---|---|---|---|---|---|
| A | yes | 13.655s | 13.134s…14.176s | 607.2 MiB | 79 | 5057 |
| B | yes | 13.939s | 13.522s…14.356s | 497.9 MiB | 79 | 5057 |

## Index — incremental `cass index` (after the appended batch)

| build | ok | wall (mean) | wall (min…max) | peak RSS | conversations | messages |
|---|---|---|---|---|---|---|
| A | yes | 1.798s | 1.750s…1.847s | 556.9 MiB | 3 | 159 |
| B | yes | 1.758s | 1.758s…1.758s | 555.2 MiB | 3 | 159 |

## Strict viewer search — `search_sessions` (per-query latency)

| query | A median | A p90 | B median | B p90 | Δmed | Δp90 | total (A/B) | complete (A/B) | verdict |
|---|---|---|---|---|---|---|---|---|---|
| `common` | 0.016s | 0.017s | 0.015s | 0.016s | 2.56% | 4.79% | 66 / 66 | True/True | OK |
| `rare` | 0.006s | 0.006s | 0.006s | 0.006s | 3.45% | 1.56% | 2 / 2 | True/True | OK |
| `phrase` | 0.006s | 0.007s | 0.006s | 0.007s | 1.75% | 2.86% | 4 / 4 | True/True | OK |
| `not_common` | 0.005s | 0.005s | 0.005s | 0.006s | 0.0% | 8.47% | 16 / 16 | True/True | OK |
| `tool_bash` | 0.008s | 0.009s | 0.009s | 0.009s | 5.88% | 0.0% | 42 / 42 | True/True | OK |

## A/A noise floor

This run compares two builds of the same source, so the A–B deltas above ARE the measured noise floor of bench + machine:

- `index --full` wall: **2.04%**
- incremental `index` wall: **2.22%**
- search per-query medians: max **5.88%** (median 2.56%)
- search per-query p90: max **8.47%**

## Verdict

- all runs ok: **True**
- all result counts match A vs B: **True**

