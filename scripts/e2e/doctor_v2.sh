#!/usr/bin/env bash
# scripts/e2e/doctor_v2.sh
# Scripted cass doctor v2 E2E runner. The Rust runner creates isolated
# scenario roots and durable artifacts under test-results/e2e/doctor-v2/.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/run_cargo.sh
source "${PROJECT_ROOT}/scripts/lib/run_cargo.sh"

MODE="run"
if [[ $# -gt 0 ]]; then
  case "$1" in
    run|list|describe|verify-goldens|update-goldens)
      MODE="$1"
      shift
      ;;
  esac
fi

LABELS="quick"
LABELS_EXPLICIT=0
if [[ "$MODE" == "list" || "$MODE" == "describe" ]]; then
  LABELS=""
fi
SCENARIOS=""
EXCLUDE_LABELS=""
EXCLUDE_SCENARIOS=""
NO_BUILD=0
INCLUDE_FAILURE_SELF_TEST=0
FAIL_FAST=0
KEEP_TEMP=0
JSON_OUTPUT=0
ARTIFACT_BASE_DIR=""
RUN_ROOT_OVERRIDE=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/e2e/doctor_v2.sh list [--json] [--include-failure-self-test]
  scripts/e2e/doctor_v2.sh describe --scenario <id> [--json]
  scripts/e2e/doctor_v2.sh run [--label quick,fault,safe-auto,cleanup,low-disk,promotion] [--scenario quick-source-pruned] [--exclude-label mutation] [--exclude-scenario cleanup-low-disk-derived-only] [--artifact-dir <absolute-base-dir>] [--run-root <absolute-run-root>] [--fail-fast] [--include-failure-self-test] [--no-build|--force-build] [--json]
  scripts/e2e/doctor_v2.sh verify-goldens
  scripts/e2e/doctor_v2.sh update-goldens

Artifacts:
  test-results/e2e/doctor-v2/run-*/artifacts/<scenario>/
  test-results/e2e/doctor-v2/run-*/scenario-manifest.json

Environment:
  CARGO_TARGET_DIR / CARGO_BUILD_JOBS / RUST_TEST_THREADS
                shared Cargo defaults resolved by scripts/lib/run_cargo.sh

  --artifact-dir names a base directory; each invocation creates a fresh
  run-* child so repeated runs preserve earlier evidence. Use --run-root only
  when a deterministic exact run root is required.

Safety:
  The runner only invokes robot-safe cass commands. It never launches bare cass.
  Scenario data is generated in isolated fixtures. Mutating scenarios are explicit
  and operate on fixture data only.

Common commands:
  scripts/e2e/doctor_v2.sh list --json
  scripts/e2e/doctor_v2.sh describe --scenario quick-source-pruned --json
  scripts/e2e/doctor_v2.sh run --label quick --json
  scripts/e2e/doctor_v2.sh run --label safe-auto --fail-fast --json
  scripts/e2e/doctor_v2.sh run --scenario candidate-promote-corrupt-db-derived-followup --fail-fast --json
  scripts/e2e/doctor_v2.sh run --scenario cleanup-low-disk-derived-only --fail-fast
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --label|--labels)
      LABELS="${2:?--label requires a comma-separated value}"
      LABELS_EXPLICIT=1
      shift 2
      ;;
    --scenario|--scenarios)
      SCENARIOS="${2:?--scenario requires a comma-separated value}"
      shift 2
      ;;
    --exclude-label|--exclude-labels)
      EXCLUDE_LABELS="${2:?--exclude-label requires a comma-separated value}"
      shift 2
      ;;
    --exclude-scenario|--exclude-scenarios)
      EXCLUDE_SCENARIOS="${2:?--exclude-scenario requires a comma-separated value}"
      shift 2
      ;;
    --artifact-dir)
      ARTIFACT_BASE_DIR="${2:?--artifact-dir requires an absolute path}"
      shift 2
      ;;
    --run-root)
      RUN_ROOT_OVERRIDE="${2:?--run-root requires an absolute path}"
      shift 2
      ;;
    --no-build)
      NO_BUILD=1
      shift
      ;;
    --force-build)
      NO_BUILD=0
      shift
      ;;
    --include-failure-self-test)
      INCLUDE_FAILURE_SELF_TEST=1
      shift
      ;;
    --fail-fast)
      FAIL_FAST=1
      shift
      ;;
    --keep-temp)
      KEEP_TEMP=1
      shift
      ;;
    --json)
      JSON_OUTPUT=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      if [[ "$MODE" == "describe" && -z "$SCENARIOS" && "$1" != --* ]]; then
        SCENARIOS="$1"
        shift
      else
        echo "unknown argument: $1" >&2
        usage >&2
        exit 2
      fi
      ;;
  esac
done

cd "$PROJECT_ROOT"

if [[ "$MODE" == "run" && -n "$SCENARIOS" && "$LABELS_EXPLICIT" -eq 0 ]]; then
  LABELS=""
fi

if [[ "$MODE" == "describe" && -z "$SCENARIOS" ]]; then
  echo "describe requires --scenario <id> or a scenario id argument" >&2
  exit 2
fi

RUN_ID="run-$(date -u +%Y%m%dT%H%M%SZ)-$$"
if [[ -n "$RUN_ROOT_OVERRIDE" ]]; then
  case "$RUN_ROOT_OVERRIDE" in
    /*) RUN_ROOT="$RUN_ROOT_OVERRIDE" ;;
    *)
      echo "--run-root must be absolute: $RUN_ROOT_OVERRIDE" >&2
      exit 2
      ;;
  esac
elif [[ -n "$ARTIFACT_BASE_DIR" ]]; then
  case "$ARTIFACT_BASE_DIR" in
    /*) RUN_ROOT="${ARTIFACT_BASE_DIR}/${RUN_ID}" ;;
    *)
      echo "--artifact-dir must be absolute: $ARTIFACT_BASE_DIR" >&2
      exit 2
      ;;
  esac
else
  RUN_ROOT="${PROJECT_ROOT}/test-results/e2e/doctor-v2/${RUN_ID}"
fi

run_golden_tests() {
  local update="$1"
  if [[ "$update" == "1" ]]; then
    export UPDATE_GOLDENS=1
  fi
  run_cargo test --locked --test golden_robot_docs robot_docs_schemas_matches_golden -- --nocapture
  run_cargo test --locked --test golden_robot_docs robot_docs_guide_matches_golden -- --nocapture
  run_cargo test --locked --test golden_robot_json introspect_json_matches_golden -- --nocapture
  run_cargo test --locked --test golden_robot_json introspect_shape_matches_golden -- --nocapture
}

run_cargo_logged() {
  local log_path="$1"
  shift

  if [[ "$JSON_OUTPUT" -eq 0 ]]; then
    "$@"
    return $?
  fi

  mkdir -p "$(dirname "$log_path")"
  if "$@" >"$log_path" 2>&1; then
    echo "cargo log: ${log_path}" >&2
    return 0
  fi

  local status=$?
  echo "cargo command failed with exit ${status}: $*" >&2
  echo "cargo log: ${log_path}" >&2
  tail -n 120 "$log_path" >&2 || true
  return "$status"
}

json_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  value="${value//$'\r'/\\r}"
  value="${value//$'\t'/\\t}"
  printf '%s' "$value"
}

emit_run_summary_json() {
  local status_value="$1"
  local exit_code="${2:-}"
  local escaped_run_root
  local escaped_scenario_manifest
  local escaped_run_summary

  escaped_run_root="$(json_escape "$RUN_ROOT")"
  escaped_scenario_manifest="$(json_escape "${RUN_ROOT}/scenario-manifest.json")"
  escaped_run_summary="$(json_escape "${RUN_ROOT}/run-summary.json")"

  if [[ -n "$exit_code" ]]; then
    printf '{"status":"%s","exit_code":%d,"run_root":"%s","scenario_manifest_path":"%s","run_summary_path":"%s"}\n' \
      "$status_value" "$exit_code" "$escaped_run_root" "$escaped_scenario_manifest" "$escaped_run_summary"
  else
    printf '{"status":"%s","run_root":"%s","scenario_manifest_path":"%s","run_summary_path":"%s"}\n' \
      "$status_value" "$escaped_run_root" "$escaped_scenario_manifest" "$escaped_run_summary"
  fi
}

if [[ "$MODE" == "verify-goldens" ]]; then
  run_golden_tests 0
  exit 0
fi

if [[ "$MODE" == "update-goldens" ]]; then
  run_golden_tests 1
  echo "Reviewed diff required: git diff tests/golden/"
  exit 0
fi

export CASS_DOCTOR_E2E_LABELS="$LABELS"
export CASS_DOCTOR_E2E_SCENARIOS="$SCENARIOS"
export CASS_DOCTOR_E2E_EXCLUDE_LABELS="$EXCLUDE_LABELS"
export CASS_DOCTOR_E2E_EXCLUDE_SCENARIOS="$EXCLUDE_SCENARIOS"
export CASS_DOCTOR_E2E_RUN_ROOT="$RUN_ROOT"
if [[ "$INCLUDE_FAILURE_SELF_TEST" -eq 1 ]]; then
  export CASS_DOCTOR_E2E_INCLUDE_FAILURE_SELF_TEST=1
fi
if [[ "$FAIL_FAST" -eq 1 ]]; then
  export CASS_DOCTOR_E2E_FAIL_FAST=1
fi
if [[ "$KEEP_TEMP" -eq 1 ]]; then
  export CASS_DOCTOR_E2E_KEEP_TEMP=1
fi

if [[ "$MODE" == "list" || "$MODE" == "describe" ]]; then
  export CASS_DOCTOR_E2E_LIST_ONLY=1
  mkdir -p "$RUN_ROOT"
  run_cargo test --locked --test doctor_e2e_runner doctor_e2e_scripted_scenarios -- --nocapture >"${RUN_ROOT}/cargo-list.log" 2>&1
  cat "${RUN_ROOT}/scenario-manifest.json"
  exit 0
fi

if [[ "$NO_BUILD" -eq 0 ]]; then
  run_cargo_logged "${RUN_ROOT}/cargo-build.log" run_cargo build --locked --bin cass
fi

if run_cargo_logged "${RUN_ROOT}/cargo-doctor-e2e.log" run_cargo test --locked --test doctor_e2e_runner doctor_e2e_scripted_scenarios -- --nocapture; then
  if [[ "$JSON_OUTPUT" -eq 1 ]]; then
    emit_run_summary_json "pass"
  else
    echo "Artifacts: ${RUN_ROOT}"
  fi
else
  status=$?
  if [[ "$JSON_OUTPUT" -eq 1 ]]; then
    emit_run_summary_json "fail" "$status"
  else
    echo "Doctor e2e failed. Artifacts: ${RUN_ROOT}" >&2
  fi
  exit "$status"
fi
