#!/usr/bin/env bash
# waijq_f16_preconvert_e2e.sh — exercise CASS_F16_PRECONVERT env-toggle.
#
# Per coding_agent_session_search-waijq. The runtime_optimizations infrastructure
# (shared with yvv7r) caches the env value; this script verifies the surface
# updates correctly under each env-var combination.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=../lib/run_cargo.sh
source "$SCRIPT_DIR/../lib/run_cargo.sh"
cass_cargo_resolve
CASS_WORK_DIR="${CASS_WORK_DIR:-${TMPDIR:-/tmp}/cass-waijq}"
LOG="$CASS_WORK_DIR/waijq-e2e.log"
mkdir -p "$CASS_WORK_DIR"
exec > >(tee -a "$LOG") 2>&1

cleanup() {
    local rc=$?
    if [ "$rc" -ne 0 ]; then
        echo ""
        echo "[waijq_e2e] FAILURE — last 50 log lines:" >&2
        tail -n 50 "$LOG" | sed 's/^/[waijq_e2e]   /' >&2
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
    echo "[waijq_e2e] cass binary not found; building locally..."
    (cd "$PROJECT_ROOT" && run_cargo build --bin cass)
    CASS_BIN="$CASS_CARGO_TARGET_DIR/debug/cass"
fi
echo "[waijq_e2e] using cass binary: $CASS_BIN"

PASS=0
FAIL=0
expect_value() {
    local description="$1"
    local actual="$2"
    local expected="$3"
    if [ "$actual" = "$expected" ]; then
        echo "[waijq_e2e] OK: $description (got=$actual)"
        PASS=$((PASS + 1))
    else
        echo "[waijq_e2e] FAIL: $description (expected=$expected got=$actual)"
        FAIL=$((FAIL + 1))
    fi
}

scenario() {
    local name="$1"
    local env_value="$2"
    local expected="$3"
    local data_dir
    data_dir="$(mktemp -d -t waijq-data-XXXXXX)"
    local out
    if [ "$env_value" = "__UNSET__" ]; then
        out="$(env -i HOME="$HOME" PATH="$PATH" CASS_DATA_DIR="$data_dir" \
            "$CASS_BIN" health --json 2>&1 || true)"
    else
        out="$(env -i HOME="$HOME" PATH="$PATH" CASS_DATA_DIR="$data_dir" \
            CASS_F16_PRECONVERT="$env_value" \
            "$CASS_BIN" health --json 2>&1 || true)"
    fi
    rm -rf "$data_dir"
    local got
    got="$(echo "$out" | jq -r '.runtime_optimizations.preconvert_f16 // "missing"')"
    expect_value "scenario=$name CASS_F16_PRECONVERT=$env_value" "$got" "$expected"
}

# Default (unset) → enabled.
scenario "default_unset" "__UNSET__" "true"
# Explicit truthy.
scenario "explicit_1" "1" "true"
scenario "explicit_true" "true" "true"
scenario "explicit_yes" "yes" "true"
scenario "explicit_on" "ON" "true"
# Explicit falsy.
scenario "explicit_0" "0" "false"
scenario "explicit_false" "false" "false"
scenario "explicit_no" "no" "false"
scenario "explicit_off" "OFF" "false"
# Invalid value → defaults to enabled.
scenario "invalid_banana" "banana" "true"

echo ""
echo "[waijq_e2e] SUMMARY: PASS=$PASS FAIL=$FAIL"
echo "[waijq_e2e] log written to: $LOG"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
echo "[waijq_e2e] ALL PASS"
