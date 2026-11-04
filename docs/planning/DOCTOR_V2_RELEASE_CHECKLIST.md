# Doctor v2 Release Checklist and Rollout Notes

Status: release gate document for the archive-first doctor v2 work.
Last updated: 2026-05-06.
Owning bead: `coding_agent_session_search-h00ou`.

This checklist is the auditable release record for doctor v2. It is intentionally
self-contained so a future maintainer can review the release posture without
rerunning commands against a real user archive. A release is not approved until
every required gate below has either a passing evidence entry or an explicit
release blocker.

## Release Position

Doctor v2 is an archive-first recovery system, not a cleanup shortcut. The safe
mental model for users and agents is:

- `cass doctor check --json` and plain `cass doctor --json` are read-only.
- Legacy `cass doctor --fix --json` is safe-auto only: it can apply repairs that
  are proven low risk, but it must refuse archive-risky work and route the user
  to an explicit dry-run/apply command.
- Risky mutation requires a prior dry-run or rehearsal plus the exact
  `plan_fingerprint`:
  - `cass doctor repair --dry-run --json`
  - `cass doctor repair --yes --plan-fingerprint <fingerprint> --json`
  - `cass doctor backups restore <backup-id> --dry-run --json`
  - `cass doctor backups restore <backup-id> --yes --plan-fingerprint <fingerprint> --json`
  - `cass doctor cleanup --json`
  - `cass doctor cleanup --yes --plan-fingerprint <fingerprint> --json`
  - `cass doctor archive-normalize --dry-run --json`
  - `cass doctor archive-normalize --yes --plan-fingerprint <fingerprint> --json`
  - `cass doctor archive export --dry-run --json`
  - `cass doctor archive export --yes --plan-fingerprint <fingerprint> --json`
- Candidate repair must build in isolation, verify coverage, record receipts,
  run post-repair probes, and promote atomically or roll back.
- Backups, raw mirrors, DB/WAL/SHM files, receipts, support bundles, configs,
  bookmarks, and failure markers are precious evidence. They are not deleted by
  normal doctor repair.
- Raw session mirror bytes stay local by default. Robot output, event logs,
  support bundles, and public/export surfaces expose redacted metadata and
  checksums unless an explicit sensitive-evidence mode is added and approved.
- Semantic, vector, model, memoization, and lexical indexes are derived assets.
  They may be rebuilt or cleaned only through explicit derived-asset plans; their
  failure must not block lexical/archive recovery.
- Multi-machine source gaps are diagnostic inputs. Doctor does not SSH to live
  remotes during fast health/status checks and must not assume a remote log is
  still available when cass has local archive evidence.
- Never use bare `cass` in automation; it launches the interactive TUI. Use
  `--json`, `--robot`, or the scripted e2e runner.
- Never recommend manual deletion of cass archive evidence to solve a doctor
  warning. If disk pressure exists, use archive export/relocate or fingerprinted
  cleanup for derived assets.

## Evidence Index

Current local proof snapshot from 2026-05-06:

| Gate | Evidence |
| --- | --- |
| Candidate repair, coverage gates, promotion, rollback, restore | `env CARGO_TARGET_DIR=/data/tmp/cass_57xo8_verify cargo test --test doctor_e2e_runner doctor_e2e_runner_reconstructs_candidate_from_mirror_when_db_is_corrupt -- --nocapture`; `doctor_e2e_runner_blocks_coverage_decreasing_candidate_promotion`; `doctor_e2e_runner_promotes_corrupt_db_candidate_and_records_derived_followup`; `doctor_e2e_runner_rolls_back_candidate_promotion_after_component_replace_failpoint`; `doctor_e2e_runner_rolls_back_candidate_promotion_before_parent_sync_failpoint`; `env CARGO_TARGET_DIR=/data/tmp/cass_57xo8_verify cargo test --test cli_doctor doctor_backups_restore_apply_promotes_backup_and_preserves_pre_restore_backup -- --nocapture` |
| Repeated repair refusal and post-repair failure markers | `env CARGO_TARGET_DIR=/data/tmp/cass_57xo8_verify cargo test --test cli_doctor doctor_fix_refuses_repeated_repair_when_failure_marker_exists -- --nocapture`; `doctor_cleanup_apply_reports_verification_failed_when_post_repair_probe_fails`; scripted artifacts under `/data/tmp/cass-doctor-v2-proof/run-20260506T185419Z-165122` and `/data/tmp/cass-doctor-v2-proof/run-20260506T185429Z-169162` |
| Raw mirror, pre-parse capture, privacy, backfill, source ledger | `env CARGO_TARGET_DIR=/data/tmp/cass_57xo8_verify cargo test --lib raw_mirror -- --nocapture` passed 30 tests; `env CARGO_TARGET_DIR=/data/tmp/cass_57xo8_verify cargo test --test cli_doctor doctor_json_reports_missing_upstream_source_as_coverage_risk_not_data_loss -- --nocapture`; `doctor_fix_backfills_legacy_raw_mirror_metadata_without_touching_provider_files`; `doctor_json_verifies_raw_mirror_after_upstream_source_is_pruned` |
| Multi-machine source/sync gaps | `env CARGO_TARGET_DIR=/data/tmp/cass_57xo8_verify cargo test --lib doctor_remote_source_sync_report -- --nocapture` |
| Read-only source-pruned, mirror-missing, multi-file source artifacts | Focused tests passed: `doctor_e2e_runner_records_truncated_source_with_verified_mirror`; `doctor_e2e_runner_reports_no_safe_rebuild_authority_without_mirror`; `doctor_e2e_runner_records_multi_file_source_artifacts`; `doctor_e2e_backup_exclusion_risk_warns_without_mutating_fixture`. Scripted proof passed: `env CARGO_TARGET_DIR=/data/tmp/cass_57xo8_verify scripts/e2e/doctor_v2.sh run --scenario quick-source-pruned,quick-mirror-missing,multi-file-source-artifacts --fail-fast --json --no-build --artifact-dir /data/tmp/cass-doctor-v2-proof`; artifacts under `/data/tmp/cass-doctor-v2-proof/run-20260506T190058Z-345851` |
| Safe-auto and post-repair scripted journeys | `env CARGO_TARGET_DIR=/data/tmp/cass_57xo8_verify scripts/e2e/doctor_v2.sh run --scenario candidate-promote-post-repair-probe-failure --fail-fast --json --no-build --artifact-dir /data/tmp/cass-doctor-v2-proof`; `safe-auto-repeated-repair-refusal`; artifacts under `/data/tmp/cass-doctor-v2-proof/run-20260506T185419Z-165122` and `/data/tmp/cass-doctor-v2-proof/run-20260506T185429Z-169162` |
| TUI and robot automation state | `env CARGO_TARGET_DIR=/data/tmp/cass_57xo8_verify cargo test --lib doctor_hud_footer -- --nocapture`; `env CARGO_TARGET_DIR=/data/tmp/cass_57xo8_verify cargo test --test cli_robot capabilities_json_includes_expected_features -- --nocapture`; `introspect_response_schemas_advertise_doctor_v2_surfaces` |
| Full focused doctor e2e runner registry | `env CARGO_TARGET_DIR=/data/tmp/cass_57xo8_verify cargo test --test doctor_e2e_runner --no-fail-fast -- --nocapture` previously passed 96/96 in this release-prep session; rerun before tagging if any doctor/test files change after this checklist |
| Graph health | `br dep cycles --json` returned `{"cycles":[],"count":0}` before closing `py1bx` and `wh75l` |
| No new rusqlite in current diff | The copy-pasteable release hygiene scan below returned no matches on 2026-05-06 |
| Unsafe cleanup docs scan | The copy-pasteable release hygiene scan below was reviewed on 2026-05-06; findings were README's bare-cass warning, doctor_v2's robot-safe note, temp/sandbox cleanup traps in e2e scripts, and docs that explicitly forbid manual archive deletion |

## Required Pre-Release Gates

Run these from the repository root. Record the exact command, status, and
artifact path in the evidence index before release.

### Build, Lint, and Format

- [ ] `cargo fmt --check`
- [ ] `cargo check --all-targets`
- [ ] `cargo clippy --all-targets -- -D warnings`
- [ ] If a shared target dir is locked, record the lock owner and rerun rather
  than silently skipping.

### Golden Contracts

- [ ] `UPDATE_GOLDENS=1 cargo test --test golden_robot_json --test golden_robot_docs`
- [ ] Review `git diff tests/golden/` manually.
- [ ] Confirm changed doctor fields are intentional, stable, redacted, and
  documented in robot-docs or introspect schemas.
- [ ] Confirm `.actual` files remain ignored and untracked.

### Doctor v2 Scripted E2E

- [ ] `scripts/e2e/doctor_v2.sh describe --scenario quick-source-pruned --json`
- [ ] `scripts/e2e/doctor_v2.sh run --label quick --fail-fast --json --artifact-dir <absolute-artifact-dir>`
- [ ] `scripts/e2e/doctor_v2.sh run --label safe-auto --fail-fast --json --artifact-dir <absolute-artifact-dir>`
- [ ] `scripts/e2e/doctor_v2.sh run --label promotion --fail-fast --json --artifact-dir <absolute-artifact-dir>`
- [ ] `scripts/e2e/doctor_v2.sh run --label cleanup --fail-fast --json --artifact-dir <absolute-artifact-dir>`
- [ ] `scripts/e2e/doctor_v2.sh run --scenario backups-restore-fixture-journey,backups-restore-rollback-failpoint --fail-fast --json --artifact-dir <absolute-artifact-dir>`
- [ ] Each run must preserve `scenario-manifest.json`, `run-summary.json`,
  `commands.jsonl`, `doctor-events.jsonl`, `receipts.jsonl`, redaction reports,
  before/after file trees, and any failure_context artifacts.

### Representative Data-Dir Copy Dry Run

- [ ] Create or select a copied representative cass data dir. Do not run release
  dry runs against the only live archive.
- [ ] Run `cass doctor check --json --data-dir <copy>`.
- [ ] Run `cass doctor repair --dry-run --json --data-dir <copy>` if check
  recommends repair.
- [ ] Run `cass doctor support-bundle --json --data-dir <copy>` and verify the
  bundle manifest is scrubbed by default.
- [ ] If an apply step is needed, run it only with the fingerprint from the
  copied dry run and record the receipt path.

### Privacy and Redaction

- [ ] Confirm support bundles include manifest/checksum evidence and redacted
  summaries by default, not raw mirror bytes or full private paths.
- [ ] Confirm redaction reports exist for scripted e2e artifacts.
- [ ] Confirm public Pages/HTML export paths exclude raw mirror bytes and
  sensitive attachments unless a future explicit sensitive mode is approved.
- [ ] Confirm failure_context and reproduction commands are shell-quoted and do
  not expose raw user session content.

### Migration and Compatibility

- [ ] Confirm legacy `cass doctor --json` remains read-only.
- [ ] Confirm legacy `cass doctor --fix --json` maps to safe-auto and refuses
  archive-risky mutation.
- [ ] Confirm explicit command surfaces reject ignored mutation controls rather
  than silently accepting them.
- [ ] Confirm old archives migrate additively: DB/WAL/SHM, raw mirrors, backup
  manifests, receipts, bookmarks, configs, failure markers, and support bundles
  are preserved.
- [ ] Confirm every workflow preserves `agent_search.db`, WAL/SHM, raw mirrors,
  backups, support bundles, and receipts unless an explicit fingerprinted
  archive export or restore workflow says otherwise.

### Release Hygiene Scans

- [ ] No new rusqlite usage:
  `git diff -- Cargo.toml src tests scripts docs README.md CHANGELOG.md | rg -n '(^|[^[:alnum:]_])r[u]sqlite([[:space:]]*::|[[:space:]]*=|[[:space:]]*;|[[:space:]]+as[[:space:]])' || true`
- [ ] No unsafe archive cleanup docs:
  `rg -n 'rm[[:space:]]+-rf|git[[:space:]]+reset[[:space:]]+--hard|git[[:space:]]+clean[[:space:]]+-fd|d[e]lete[^\n]{0,80}(agent_search|raw mirror|raw_mirror|session log|archive|backup)|m[a]nual[^\n]{0,80}(d[e]lete|remove)[^\n]{0,80}(agent_search|raw mirror|raw_mirror|session log|archive|backup)|bare[[:space:]]+cass' README.md docs scripts --glob '!docs/artifacts/refactor-runs/**' --glob '!docs/artifacts/migration-baseline/**' || true`
- [ ] Review every hit. Temp-directory cleanup in isolated tests is not a
  release blocker; manual archive-evidence deletion guidance is.
- [ ] Confirm automation examples use `--json`, `--robot`, or
  `scripts/e2e/doctor_v2.sh`, never bare `cass`.

## Rollout Notes

- Default release messaging should say: doctor v2 protects the cass archive
  first, then repairs derived state.
- `--fix` is retained for compatibility but should be described as safe-auto,
  not as a blanket repair command.
- Fingerprint-approved apply commands are intentionally more verbose. That is a
  safety feature: the user approves exactly the inspected plan.
- A blocked repair is not necessarily a failure. Coverage-shrink refusal,
  mirror-missing refusal, repeated-repair refusal, lock-busy, and failed
  post-repair probes are successful safety outcomes when they preserve evidence
  and provide next actions.
- Support requests should ask for the scrubbed support bundle, the relevant
  `manifest.json`, `failure_context.json` when present, receipts, and the
  command log. They should not ask users to send raw mirror bytes unless a
  future sensitive-evidence workflow exists.
- Known limitations for release notes:
  - Doctor does not auto-download semantic models.
  - Fast health/status surfaces may report unchecked coverage and point to
    doctor check for the full ledger.
  - Multi-machine diagnostics use local sync metadata and local mirrors unless
    an explicit source operation is run.
  - Heavy/browser e2e tests remain CI-oriented per project policy; doctor v2
    scripted e2e runs are the local release proof surface.

## Release Blockers

Treat any of these as a stop-ship issue:

- A mutating doctor command can reduce archive conversation or message coverage
  without an explicit blocked outcome.
- A dry-run/apply command accepts a fingerprint or approval control that it
  silently ignores.
- A support bundle, robot JSON, or event log leaks raw session text or full
  private paths by default.
- A cleanup path removes raw mirrors, DB/WAL/SHM, backups, receipts, bookmarks,
  configs, or failure markers outside an explicitly fingerprinted archival
  export/restore workflow.
- A scripted e2e artifact cannot explain what changed, what stayed untouched,
  and which receipt or failure_context proves it.
- `cargo check`, `cargo clippy -D warnings`, or `cargo fmt --check` is red for
  doctor v2 code touched by this release.
