#!/usr/bin/env bash
# yvv7r_rollback_envvars_e2e.sh — exercise rollback env vars via cass health.
#
# Per coding_agent_session_search-yvv7r. Runs `cass health --json` under each
# combination of (CASS_SIMD_DOT, CASS_PARALLEL_SEARCH, CASS_F16_PRECONVERT)
# bool flags and asserts the runtime_optimizations field reflects the env-var
# state. Also exercises invalid values to confirm the default-with-warn path.
#
# Output: structured one-line-per-scenario log + a final summary.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=../lib/run_cargo.sh
source "$SCRIPT_DIR/../lib/run_cargo.sh"
cass_cargo_resolve

# Locate the cass binary. Prefer the resolved Cargo target dir, fall back to
# the project-local target/ or an installed cass on PATH.
CASS_WORK_DIR="${CASS_WORK_DIR:-${TMPDIR:-/tmp}/cass-yvv7r}"
LOG="$CASS_WORK_DIR/yvv7r-e2e.log"
mkdir -p "$CASS_WORK_DIR"
exec > >(tee -a "$LOG") 2>&1

cleanup() {
    local rc=$?
    if [ "$rc" -ne 0 ]; then
        echo ""
        echo "[yvv7r_e2e] FAILURE — last 50 log lines:" >&2
        tail -n 50 "$LOG" | sed 's/^/[yvv7r_e2e]   /' >&2
    fi
    exit "$rc"
}
trap cleanup EXIT

CASS_BIN=""
for candidate in \
    "$CASS_CARGO_TARGET_DIR/release/cass" \
    "$CASS_CARGO_TARGET_DIR/debug/cass" \
    "$PROJECT_ROOT/target/release/cass" \
    "$PROJECT_ROOT/target/debug/cass" \
    "$(command -v cass 2>/dev/null || true)"; do
    if [ -x "$candidate" ]; then
        CASS_BIN="$candidate"
        break
    fi
done

if [ -z "$CASS_BIN" ]; then
    echo "[yvv7r_e2e] cass binary not found; building locally..."
    (cd "$PROJECT_ROOT" && run_cargo build --bin cass)
    CASS_BIN="$CASS_CARGO_TARGET_DIR/debug/cass"
    [ -x "$CASS_BIN" ] || {
        echo "[yvv7r_e2e] FAIL: build did not produce $CASS_BIN" >&2
        exit 1
    }
fi
echo "[yvv7r_e2e] using cass binary: $CASS_BIN"

scenario() {
    local name="$1"
    shift
    local env_pairs=("$@")
    local data_dir
    data_dir="$(mktemp -d -t yvv7r-data-XXXXXX)"
    local out
    # shellcheck disable=SC2068
    out="$(env -i HOME="$HOME" PATH="$PATH" CASS_DATA_DIR="$data_dir" ${env_pairs[@]} \
        "$CASS_BIN" health --json 2>&1 || true)"
    rm -rf "$data_dir"
    if ! echo "$out" | head -c 1 | grep -q "{"; then
        # Health may exit 1 on fresh data dir but still emit valid JSON.
        # If output isn't JSON-shaped, surface the failure.
        echo "[yvv7r_e2e] scenario=$name FAIL: output not JSON-shaped: $(echo "$out" | head -c 200)"
        return 1
    fi
    local simd parallel preconvert source
    simd="$(echo "$out" | jq -r '.runtime_optimizations.simd_dot // "missing"')"
    parallel="$(echo "$out" | jq -r '.runtime_optimizations.parallel_search // "missing"')"
    preconvert="$(echo "$out" | jq -r '.runtime_optimizations.preconvert_f16 // "missing"')"
    source="$(echo "$out" | jq -r '.runtime_optimizations.config_source // "missing"')"
    echo "[yvv7r_e2e] scenario=$name env=${env_pairs[*]} simd=$simd parallel=$parallel preconvert=$preconvert source=$source"
    # Caller asserts via grep over the log.
}

PASS=0
FAIL=0
expect_grep() {
    local description="$1"
    local pattern="$2"
    if grep -q "$pattern" "$LOG"; then
        echo "[yvv7r_e2e] OK: $description"
        PASS=$((PASS + 1))
    else
        echo "[yvv7r_e2e] FAIL: $description (pattern not found: $pattern)"
        FAIL=$((FAIL + 1))
    fi
}

scenario "default_no_env"
expect_grep "default scenario simd=true" "scenario=default_no_env env=  *simd=true parallel=true preconvert=true source=default"

scenario "simd_off" "CASS_SIMD_DOT=0"
expect_grep "simd_off shows simd=false" "scenario=simd_off .*simd=false .*source=env"

scenario "parallel_off" "CASS_PARALLEL_SEARCH=false"
expect_grep "parallel_off shows parallel=false" "scenario=parallel_off .*parallel=false .*source=env"

scenario "preconvert_off" "CASS_F16_PRECONVERT=no"
expect_grep "preconvert_off shows preconvert=false" "scenario=preconvert_off .*preconvert=false .*source=env"

scenario "all_off" "CASS_SIMD_DOT=0" "CASS_PARALLEL_SEARCH=off" "CASS_F16_PRECONVERT=no"
expect_grep "all_off shows all three false" "scenario=all_off .*simd=false parallel=false preconvert=false source=env"

scenario "invalid_value_defaults" "CASS_SIMD_DOT=banana"
expect_grep "invalid value falls back to enabled" "scenario=invalid_value_defaults .*simd=true"

scenario "case_insensitive" "CASS_SIMD_DOT=OFF" "CASS_PARALLEL_SEARCH=YES"
expect_grep "case-insensitive parses correctly" "scenario=case_insensitive .*simd=false parallel=true"

echo ""
echo "[yvv7r_e2e] SUMMARY: PASS=$PASS FAIL=$FAIL"
echo "[yvv7r_e2e] log written to: $LOG"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
echo "[yvv7r_e2e] ALL PASS"
