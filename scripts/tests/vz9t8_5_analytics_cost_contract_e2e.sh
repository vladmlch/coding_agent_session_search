#!/usr/bin/env bash
# vz9t8_5_analytics_cost_contract_e2e.sh — exercise the analytics cost
# contract tests.
#
# Per coding_agent_session_search-vz9t8.5 Path B (document removal + ship
# alternate path). Validates PricingTable + compute_cost remain available
# in src/storage/sqlite.rs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=../lib/run_cargo.sh
source "$SCRIPT_DIR/../lib/run_cargo.sh"
CASS_WORK_DIR="${CASS_WORK_DIR:-${TMPDIR:-/tmp}/cass-vz9t8-5}"
LOG="$CASS_WORK_DIR/analytics-cost-contract-e2e.log"
mkdir -p "$CASS_WORK_DIR"
exec > >(tee -a "$LOG") 2>&1

cleanup() {
    local rc=$?
    if [ "$rc" -ne 0 ]; then
        echo ""
        echo "[vz9t8_5_e2e] FAILURE — last 50 log lines:" >&2
        tail -n 50 "$LOG" | sed 's/^/[vz9t8_5_e2e]   /' >&2
    fi
    exit "$rc"
}
trap cleanup EXIT

PASS=0
FAIL=0

cd "$PROJECT_ROOT"
echo "[vz9t8_5_e2e] running cargo test analytics_cost_pricing_table_contract"
TEST_OUT="$CASS_WORK_DIR/test.out"
if run_cargo test --test analytics_cost_pricing_table_contract -- --nocapture \
    > "$TEST_OUT" 2>&1; then
    if grep -q "test result: ok" "$TEST_OUT"; then
        echo "[vz9t8_5_e2e] OK: contract tests passed"
        PASS=$((PASS + 1))
    else
        echo "[vz9t8_5_e2e] FAIL: cargo exited 0 but no 'test result: ok'"
        FAIL=$((FAIL + 1))
    fi
else
    echo "[vz9t8_5_e2e] FAIL: cargo test failed"
    tail -50 "$TEST_OUT" | sed 's/^/[vz9t8_5_e2e]   /'
    FAIL=$((FAIL + 1))
fi

echo "[vz9t8_5_e2e] SUMMARY: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ] && echo "[vz9t8_5_e2e] ALL PASS" || exit 1
