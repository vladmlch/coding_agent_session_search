# What the index retains for export

`cass export` reads the canonical archive (`conversations`, `messages`
tables) only; provider source files are never consulted. This document
summarizes what indexing captures per provider so exported content can be
trusted — and where the archive intentionally or unavoidably lacks data.

Every message row stores the normalized `content` plus an `extra_json` /
`extra_bin` payload. Two capture shapes matter for export:

- **Verbatim envelope**: the raw source record is archived whole, at every
  session size — Claude Code, Codex modern `.jsonl`/`.zst` and Codex legacy
  `rollout-*.json` alike. (The former large-session compaction that replaced
  the envelope with a `cass`-only skeleton past a size cutoff is gone since
  2026-11-08; there is no threshold and no env knob.) Export projects tool
  calls and results straight from the envelope.
- **`cass.invocations`**: `NormalizedMessage.invocations` is merged into
  `extra.cass.invocations` at ingest (`merge_invocations_into_extra`,
  `src/indexer/mod.rs`). FAD populates that field with
  `{kind, name, call_id?, arguments?}` for each provider's tool calls; the
  merge runs *before* secret redaction, so call names, ids, and arguments
  are redacted like any other archived field. A `cass.invocations` already
  written by the connector (Antigravity planner) is never overwritten.

## Per-provider retention

| Provider | Dialogue | Tool calls | Tool results | Reasoning |
|---|---|---|---|---|
| Claude Code | flattened text + `[Tool:]`/`[Thinking]` markers | envelope `tool_use` blocks; `cass.invocations` | `tool` rows with `tool_use_id`/`is_error`; envelope blocks | `[Thinking]` text in content; `redacted_thinking` ciphertext absent |
| Codex modern | `message`/`user_message`/`agent_message` text; args text in enriched `[Tool:]` lines | whole `payload`; `cass.invocations` | `function_call_output` text; correlate to the call row via its `call_id` (payload) or `cass.invocations` — result rows carry no id | `agent_reasoning` → assistant, `author=reasoning`; encrypted reasoning never captured |
| Codex legacy `.json` | `items[]` flattened | tool blocks in `items[].content`; `cass.invocations` | same, shape-dependent | — |
| Devin | main-chain user/assistant/tool text | `cass.invocations` (name, `call_id`, arguments; > 32 KiB args → `[OMITTED …]` marker) | `tool` rows; `tool_call_id` kept in `extra` | `extra.thinking` (captured, not emitted per export policy) |
| Antigravity | user requests, planner text, step results | `cass.invocations` — planner dual-written by FAD, MODEL steps via index merge | `tool` rows keyed by `agy_type`; tool name also in `author` | `extra.thinking` (captured, not emitted) |

## Documented absences

Indexing deliberately or unavoidably drops the following; export never
invents them:

- **Codex encrypted `reasoning` items** — no plaintext exists; skipped by
  design.
- **Claude `redacted_thinking`** ciphertext; `summary`,
  `queue-operation`, `file-history-snapshot` and other non-message record
  types — connector scoping.
- **Devin `system` nodes, `hidden` sessions, abandoned non-`main_chain_id`
  branches** — connector scoping.
- **Antigravity `CONVERSATION_HISTORY` markers**.
- **Raw `chat_message`/transcript records for Devin and Antigravity** —
  the connectors archive minimal `extra` maps, not whole source events;
  tool detail arrives through `cass.invocations` instead.
- **Older archives**: rows indexed before `cass.invocations` persistence
  (or before a provider populated the field) simply lack the key. Rows
  indexed while the former large-session extra compaction was live (FAD
  ≥ 32 MiB source; cass Codex ingest ≥ 16 MiB — both removed 2026-11-08)
  hold the compact `{cass:{model?, attachments?, ...}}` skeleton instead of
  the raw envelope; the viewer reports them as `extra_compacted`. Export
  still shows their dialogue; full inventories require a reindex.

No schema change is involved: `cass.invocations` is additive content inside
the existing `extra_json`/`extra_bin` message columns.
