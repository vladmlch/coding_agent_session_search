#!/usr/bin/env bash
# 50m1v_degradation_tier_contract_e2e.sh — exercise degradation tier contract.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=../lib/run_cargo.sh
source "$SCRIPT_DIR/../lib/run_cargo.sh"
CASS_WORK_DIR="${CASS_WORK_DIR:-${TMPDIR:-/tmp}/cass-50m1v}"
LOG="$CASS_WORK_DIR/degradation-tier-contract-e2e.log"
mkdir -p "$CASS_WORK_DIR"
exec > >(tee -a "$LOG") 2>&1

cleanup() {
    local rc=$?
    if [ "$rc" -ne 0 ]; then
        tail -n 50 "$LOG" >&2
    fi
    exit "$rc"
}
trap cleanup EXIT

cd "$PROJECT_ROOT"
echo "[50m1v_e2e] running cargo test degradation_tier_contract"
TEST_OUT="$CASS_WORK_DIR/test.out"
PASS=0; FAIL=0
if run_cargo test --test degradation_tier_contract -- --nocapture > "$TEST_OUT" 2>&1; then
    if grep -q "test result: ok" "$TEST_OUT"; then
        echo "[50m1v_e2e] OK: contract tests passed"
        PASS=$((PASS + 1))
    else
        echo "[50m1v_e2e] FAIL: cargo exited 0 but no 'test result: ok'"
        FAIL=$((FAIL + 1))
    fi
else
    echo "[50m1v_e2e] FAIL: cargo test failed"
    tail -50 "$TEST_OUT" | sed 's/^/[50m1v_e2e]   /'
    FAIL=$((FAIL + 1))
fi

echo "[50m1v_e2e] SUMMARY: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ] && echo "[50m1v_e2e] ALL PASS" || exit 1
