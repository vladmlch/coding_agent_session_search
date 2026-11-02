# Index-first Session Export Implementation Plan

> **For coding agents:** Execute tasks sequentially with test-first development and a review gate after each task. Read the repository's `AGENTS.md` and applicable suite rules before modifying code. Record changed files, tests, and evidence for each task. Do not skip failing regression tests or silently relax assertions.

**Goal:** Make CASS's canonical indexed conversations the **sole source of truth** for exporting Claude Code, Codex, Devin, and Antigravity sessions via CLI and TUI, with correct message and tool rendering and explicit failure on missing/invalid index data.

**Architecture:** Resolve the exact indexed conversation using source identity (and conversation ID where available), load it read-only from the canonical archive, project it into one agent-neutral export model, then render Markdown, text, JSON, or HTML. Original agent files, connector parsers, filesystem existence checks, schema sniffing, and decompression belong to **indexing only**, not exporting.

**Tech stack:** Rust 2024, `frankensqlite`, existing CASS canonical storage and `ConversationView`, existing `ExportPrivacy`, current CLI/TUI and HTML renderer, FAD provider connectors at index time.

**Target repo:** `vladmlch/coding_agent_session_search` (`main`). Place this plan at `docs/superpowers/plans/2026-11-02-index-first-export.md` in the working tree when implementing.

## Invariants (non-negotiable)

1. **Index-only:** `cass export`, `cass export-html` and TUI session export load content exclusively from the canonical indexed conversation. The supplied source path is an **opaque identity key**, not a file to open. An export must work when that original file/SQLite database is missing, renamed, unreadable, or changed after indexing.
2. **No invisible freshness changes:** Export never scans agent directories, invokes FAD connectors, decompresses `.zst`, writes the index, or auto-runs `cass index`. The exported state is the indexed snapshot, even if source files have changed. Missing/stale-index situations have explicit diagnostics; freshness is repaired by a separate user-initiated indexing operation.
3. **Exact identity:** Bind the result to the requested `source_path` and `source_id` (including remote provenance). TUI should also use its `conversation_id`. Do not substitute a different same-path/same-ID conversation. If CLI selection is ambiguous, fail with actionable disambiguation rather than silently selecting the newest row. Retain `--db` and `--source` semantics.
4. **One normalized model:** All four agents use the existing indexed `MessageRole`, `content`, `created_at`, `author`, and index-retained metadata/tools. Renderers contain no checks for `.db`, `.jsonl`, `.zst`, `USER_INPUT`, `PLANNER_RESPONSE`, or Codex `response_item`.
5. **No false success:** A non-empty indexed user/assistant conversation may not produce an empty successful export. Unknown roles must have an explicit projection policy with diagnostics, rather than being silently filtered. A requested but unavailable data category must not be fabricated from source files.
6. **Tool semantics:** Default output keeps ordinary conversation text; `--include-tools` includes all *indexed* tool messages/calls/results supported by the canonical schema. Excluded tools stay excluded. Preserve ordered messages and tool association where the index has that information. Reasoning/private thinking is not silently emitted; determine current indexed availability and existing policy before changing inclusion behavior.
7. **Privacy and compatibility:** Keep redaction, skill filtering, HTML encryption, clipboard, output paths, robot-mode error behavior, and CLI options. Redact every emitted field (including tool arguments/results and metadata). Preserve the existing JSON array envelope and stable public fields; make any deliberate schema addition additive and golden-tested.
8. **Repository safety:** Honor all applicable `AGENTS.md` policies: exclusive reservations for shared monoliths, exact-path staging, no unapproved deletion/destructive Git commands, frankensqlite-only new DB code, pinned dependencies, and the standard `scripts/gate.sh` verification. Avoid unrelated `cass view`, `cass expand`, search, or indexing changes unless needed to preserve indexed export completeness.

## Target data flow

```text
CLI path + --source / TUI SearchHit (conversation_id)
  -> exact canonical archive resolution (read-only snapshot)
  -> ConversationView { convo, messages[] }
  -> shared ExportConversation / ExportMessage projection
  -> common privacy and inclusion policy
  -> Markdown | text | JSON | HTML renderer
  -> output/clipboard/TUI notification
```

**Error contract:** `IndexUnavailable`, `ConversationNotIndexed`, `AmbiguousIdentity`, `IndexReadFailure`, `NoRenderableMessages`, `OutputFailure` (names are proposed, not existing code). All should become the project's existing structured `CliError`/TUI error shapes. No fallback to the original source on any of these errors.

## File ownership map

- `src/export/indexed.rs` (new): read-only, provenance-aware exact indexed conversation resolution; typed errors.
- `src/export/projection.rs` (new): index -> `ExportConversation`/`ExportMessage`, stable roles, order, tool information, completeness counts.
- `src/export.rs` (existing): export module integration; centralized `ExportPrivacy` and inclusion policy.
- `src/lib.rs` (existing): switch `run_export` and `run_export_html` from direct-path dispatch to the shared index pipeline; preserve CLI reporting/output behavior.
- `src/ui/app.rs` (existing): TUI export consumes the same pipeline and uses exact search-hit identity rather than loading local raw JSONL.
- `src/html_export/*` (existing, if needed): reuse current HTML renderer, adapting only its indexed-message input, not duplicating provider parsing.
- `src/ui/data.rs` (only if necessary): expose exact, read-only conversation lookup utilities; avoid using cached `ORDER BY ... LIMIT 1` path-only lookup when identity is ambiguous.
- `src/storage/sqlite.rs` and index ingestion (only if an evidenced gap exists): durable index storage of minimal normalized tool/metadata fields; not a source-file reader in export.
- `tests/export_index_first.rs` (new): cross-format identity, missing-source, privacy and failure regression tests.
- `tests/export_index_providers.rs` (new): provider-specific indexed end-to-end fixtures.
- `docs/` and affected goldens: document snapshot semantics and intentionally changed output contracts.

## Task 1 — Baseline and red regression tests

**Files:** Add `tests/export_index_first.rs`, `tests/export_index_providers.rs`; inspect existing `src/lib.rs` export unit tests, `tests/connector_codex.rs`, `tests/connector_antigravity.rs` and relevant fixtures.

- [ ] Read actual `AGENTS.md`, confirm local FAD revision and repository status; reserve `src/lib.rs` and `src/ui/app.rs` before edits. Capture current behavior without modifying production code.
- [ ] Seed a temporary canonical archive with at least one user and one assistant indexed message whose `source_path` refers to a **nonexistent** `.jsonl` path; add failing CLI Markdown, JSON, and HTML tests asserting export succeeds from that archive. This verifies index-only behavior, not merely parser success.
- [ ] Add a virtual `sessions.db/session-id` record with `agent_slug=devin`, while a real non-OpenCode `sessions.db` may exist. Assert `cass export` uses the indexed messages and does not invoke the OpenCode reader.
- [ ] Seed a complete indexed Antigravity conversation and a conflicting valid but raw-format `transcript.jsonl`; assert export contains indexed user/assistant messages, not the raw or a header-only document.
- [ ] Add missing-index, missing-conversation and conflicting-source tests: non-zero failure, no partial output file, no fallback to files. Verify each new regression is red against the baseline wherever it currently reproduces.

**Done when:** Every targeted scenario has a deterministic test with a meaningful assertion and baseline outcome recorded. Never weaken existing “prefer local JSONL” tests: change their expectations only after implementing and documenting the new explicit index-only product contract.

## Task 2 — Exact, read-only index resolver

**Files:** Create `src/export/indexed.rs`; update `src/export.rs`, and optionally `src/ui/data.rs` if shared identity logic needs an extraction.

**Interface to introduce (shape; finalize concrete field types against existing code):**

```rust
struct ExportSelector {
    source_path: PathBuf,
    source_id: Option<String>,
    conversation_id: Option<i64>,
}
fn load_indexed_export(
    archive_path: &Path,
    selector: &ExportSelector,
) -> Result<ConversationView, IndexedExportError>;
```

- [ ] Test that a missing or corrupt archive produces a distinct error and cannot create/repair/migrate a database; use existing `FrankenStorage::open_readonly`.
- [ ] Test remote/local source collisions and stale/mismatched TUI conversation IDs. Reuse provenance normalization already used by `src/ui/data.rs` and reject identity mismatches.
- [ ] Test ambiguous CLI path selection. Implement an exact-count/identity check instead of relying on the current `ORDER BY started_at DESC LIMIT 1` fallback. Use a single consistent read snapshot for metadata and messages.
- [ ] Run resolver tests, format changed files, and commit only this bounded change.

**Done when:** Resolver returns only a validated `ConversationView`, distinguishes errors, opens the index read-only, and never checks `source_path.exists()`.

## Task 3 — Shared export projection and completeness policy

**Files:** Create `src/export/projection.rs`; update `src/export.rs`, existing formatters as required.

**Interface to introduce:**

```rust
struct ExportConversation {
    agent_slug: String,
    title: Option<String>,
    started_at: Option<i64>,
    messages: Vec<ExportMessage>,
}
struct ExportMessage {
    index: i64,
    role: String,
    content: String,
    author: Option<String>,
    timestamp: Option<i64>,
    // Typed tool/metadata fields derived only from indexed information.
}
fn project_indexed_conversation(
    view: &ConversationView,
    options: &ExportOptions,
) -> Result<ExportConversation, ExportProjectionError>;
```

- [ ] Write provider-independent tests mapping `MessageRole::Agent` -> `assistant`, preserving user/system/tool and unknown-role policy; preserve indexed message order (`idx`) and timestamps.
- [ ] Inventory current `Message.extra_json`, index-time normalized invocations and tool/reasoning fields. Define a minimal typed projection without restoring from source files. When data is absent from the index, expose the absence honestly.
- [ ] Add completeness tests: a populated dialogue cannot become an empty successful Markdown/text/HTML export; `--include-tools=false` suppresses tool payloads; `--include-tools=true` includes tool payloads if indexed.
- [ ] Apply existing privacy/skill policies after projection and before rendering, covering body, metadata, tool arguments/results, and HTML/JSON alike.
- [ ] Commit when projection tests pass independently.

**Done when:** Every renderer consumes exactly the same provider-neutral indexed message semantics and has no provider-specific event parsing.

## Task 4 — Migrate `cass export` to index-only

**Files:** Modify `src/lib.rs` (`run_export`, existing Markdown/text/JSON/HTML conversion); test `tests/export_index_first.rs`.

- [ ] Replace `should_defer_indexed_lookup_for_direct_export`, `prefers_direct_jsonl_file`, `detect_opencode_sqlite_session` and raw JSONL dispatch **inside `run_export`** with `load_indexed_export` -> `project_indexed_conversation` -> renderer. Leave helpers still used by `view`/`expand` untouched.
- [ ] Test `--format markdown`, `text`, `json`, `html`, `-o`, clipboard and output errors with a missing original source file. Keep JSON as an array of normalized messages and verify public role/content fields.
- [ ] Confirm `source_path` is never opened and `.jsonl.zst` uses the canonical indexed row without zstd decoding at export time.
- [ ] Update existing tests that intentionally preferred fresh local JSONL so that they now demand the archived snapshot. Test that changing the source after indexing does **not** change exported bytes, subject to deterministic metadata/redaction.
- [ ] Run targeted tests and commit.

**Done when:** The CLI exports exactly the indexed snapshot for Claude, Codex, Devin, and Antigravity, regardless of original path shape or availability.

## Task 5 — Migrate `cass export-html` and TUI export

**Files:** Modify `src/lib.rs` (`run_export_html`), `src/ui/app.rs` (`export_session_task`); adapt `src/html_export/*` only where needed.

- [ ] Add HTML and TUI tests using an indexed Devin virtual path and an Antigravity `transcript.jsonl` that would otherwise lead to empty output.
- [ ] Replace both entry points' local JSONL/OpenCode fallback with the shared resolver and projection. TUI passes `SearchHit.conversation_id` plus source identity for exact selection.
- [ ] Preserve HTML tool controls, encryption, filenames, timestamps, robot-mode output shapes and error notifications; verify no TUI `load_local_export_raw_messages` call remains in this export route.
- [ ] For a missing or unreadable archive, assert a visible error instead of a silently successful file or fallback to source.
- [ ] Run tests and commit.

**Done when:** CLI and TUI exports return equivalent normalized content and all use the same indexed snapshot and privacy semantics.

## Task 6 — Four provider end-to-end contracts

**Files:** `tests/export_index_providers.rs`; reuse existing checked-in fixtures under `tests/fixtures/*`; create narrowly scoped non-sensitive test records where needed.

- [ ] **Claude Code:** Index user/assistant content blocks, system/summary metadata and tool_use/tool_result events; assert the dialogue, role/order and supported indexed tools appear according to `--include-tools`.
- [ ] **Codex:** Index modern rollout JSONL, legacy JSON (`rollout-*.json`), and compressed `.jsonl.zst` supported by FAD; compare exports for semantically equivalent indexed conversation data. Assert tool/reasoning events follow the declared index schema, not raw-file heuristics.
- [ ] **Devin:** Index a SQLite-backed conversation (`sessions`/`message_nodes`, selected main chain), export by its virtual `sessions.db/<id>` path without consulting the Devin DB or OpenCode decoder. Include multiple session IDs to verify exact identity.
- [ ] **Antigravity:** Index existing `transcript.jsonl` fixture; assert user request and both planner responses survive, tool steps are rendered when included, and the export is never merely `# Conversation Export`.
- [ ] Run each provider through Markdown, JSON, and HTML. Add a test where the original source path is absent but the index remains available. Assert actual body text and role/order, not just `exit_code == 0`.

**Done when:** Four independent provider fixtures pass against each export surface, using only archived data during export.

## Task 7 — Close index-fidelity gaps (only where verified)

**Files:** Index ingestion/storage paths and index schema **only if** Task 6 demonstrates missing required normalized information; inspect `Message.extra_json`, `src/storage/sqlite.rs`, `src/indexer/*` and pinned FAD contracts before editing.

- [ ] Produce an evidence table for each provider: source event -> normalized message -> archived content/extra_json -> exported result. Distinguish “not captured during indexing” from “captured but dropped by export”.
- [ ] For missing required tool or message semantics, extend **index-time** normalization or minimal durable index metadata. Keep database upgrades compatible, preserve older archives, and use `frankensqlite`. Do not retain entire unredacted source events merely to make export easier.
- [ ] Cover both a newly indexed archive and an older archive lacking the new metadata: older archives still export available dialogue; optional tool detail is omitted with an explicit documented limitation, never invented.
- [ ] Add privacy tests for tool arguments/results and a targeted reindex regression. Commit separately from renderer changes.

**Done when:** All required data that can be exported is present in the canonical archive; no exporter depends on original provider files.

## Task 8 — Documentation, compatibility and full gates

**Files:** CLI/export docs, affected goldens, `tests/export_index_first.rs`, `tests/export_index_providers.rs`.

- [ ] Document: “export reads the last indexed snapshot; run `cass index` to refresh”; source paths are identifiers, original files are optional; missing archive is an error; `--include-tools` only exports archived tool data.
- [ ] Verify existing flags and privacy defaults; compare old/new CLI JSON and robot JSON outputs, and update goldens only for deliberate, reviewed changes.
- [ ] Run targeted regression tests, `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and the repository's `scripts/gate.sh` using its documented execution environment (`rch` where available, `RUST_MIN_STACK=134217728` for tests). Report unavailable infrastructure separately from code failures.
- [ ] Ensure `cass view`, `cass expand`, search, and non-target agent exports are not accidentally broken; run relevant existing compatibility tests.
- [ ] Provide a final evidence matrix (provider × CLI Markdown/JSON/HTML × TUI × source missing × tools on/off × redaction) with passing test names and outstanding gaps. Stage and commit only files actually changed.

**Done when:** Gates pass, the source-of-truth contract is documented, and no success status hides missing conversation content.

## Final acceptance checklist

- [ ] Export paths never inspect agent source files, extensions, SQLite schemas, or FAD connectors.
- [ ] A source that changes or disappears after indexing does not change exported content.
- [ ] A missing/unreadable index, absent conversation, or ambiguous identity fails explicitly.
- [ ] Claude Code, Codex, Devin, and Antigravity correctly export archived dialogue to Markdown, JSON, HTML and TUI HTML.
- [ ] Indexed tool data follows `--include-tools`; missing index-time details are reported, not fabricated.
- [ ] No empty successful exports for indexed non-empty user/assistant conversations.
- [ ] Provenance, read-only access, redaction, encryption, and current user-facing options remain correct.
- [ ] Existing test coverage and required project gates pass; all new behavior is proven by specific assertions.

**Scope note:** This plan changes **export operations**, not the indexing source-of-truth policy for `cass view`/`cass expand`. Provider-specific format handling remains in FAD/indexing. A historic index that omitted information cannot have that information recovered at export time without a separate reindex.
