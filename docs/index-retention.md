# What the index retains for export

`cass export` reads the canonical archive (`conversations`, `messages`
tables) only; provider source files are never consulted. This document
summarizes what indexing captures per provider so exported content can be
trusted — and where the archive intentionally or unavoidably lacks data.

Every message row stores the normalized `content` plus an `extra_json` /
`extra_bin` payload. Three capture shapes matter for export:

- **Verbatim envelope**: the raw source record is archived (Claude
  < 32 MiB, Codex modern < 16 MiB, Codex legacy < 32 MiB). Export projects
  tool calls and results straight from the envelope.
- **`cass.invocations`**: `NormalizedMessage.invocations` is merged into
  `extra.cass.invocations` at ingest (`merge_invocations_into_extra`,
  `src/indexer/mod.rs`). FAD populates that field with
  `{kind, name, call_id?, arguments?}` for each provider's tool calls; the
  merge runs *after* extra compaction and *before* secret redaction, so
  call names, ids, and arguments survive the size cutoffs and are redacted
  like any other archived field. A `cass.invocations` already written by
  the connector (Antigravity planner) is never overwritten.
- **Compacted extras**: past the cutoffs below, the raw envelope is
  replaced by `{cass:{model?, attachments?, ...}}`. `cass.invocations`
  still lands inside that object; the raw record itself is gone.

## Size cutoffs

| Provider | Cutoff | Above it |
|---|---|---|
| Claude Code | 32 MiB source file (FAD, `LARGE_SESSION_EXTRA_COMPACT_THRESHOLD_BYTES`) | `extra` → `{cass:{model, message_id, request_id, token_usage, tool_call_count, attachments}}` |
| Codex modern `.jsonl`/`.zst` | 16 MiB source (CASS ingest, `CODEX_INDEXER_EXTRA_COMPACT_THRESHOLD_BYTES`; FAD also compacts ≥ 32 MiB decompressed) | every `extra` → `{cass:{model?, attachments?}}` |
| Codex legacy `rollout-*.json` | 32 MiB (FAD) | same compact shape |

## Per-provider retention

| Provider | Dialogue | Tool calls | Tool results | Reasoning |
|---|---|---|---|---|
| Claude Code | flattened text + `[Tool:]`/`[Thinking]` markers | envelope `tool_use` blocks (< 32 MiB); `cass.invocations` any size | `tool` rows with `tool_use_id`/`is_error`; envelope blocks < 32 MiB | `[Thinking]` text in content; `redacted_thinking` ciphertext absent |
| Codex modern | `message`/`user_message`/`agent_message` text; args text in enriched `[Tool:]` lines | whole `payload` (< 16 MiB); `cass.invocations` any size | `function_call_output` text; `call_id` via payload < 16 MiB or invocations | `agent_reasoning` → assistant, `author=reasoning`; encrypted reasoning never captured |
| Codex legacy `.json` | `items[]` flattened | tool blocks in `items[].content` (< 32 MiB); `cass.invocations` | same, shape-dependent | — |
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
  (or before a provider populated the field) simply lack the key. Export
  still shows their dialogue; invocation detail requires a reindex.

No schema change is involved: `cass.invocations` is additive content inside
the existing `extra_json`/`extra_bin` message columns.
