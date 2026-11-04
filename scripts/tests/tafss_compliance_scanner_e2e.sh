#!/usr/bin/env bash
# tafss_compliance_scanner_e2e.sh — end-to-end exercise of the local-cargo
# compliance scanner.
#
# Per coding_agent_session_search-tafss. Validates:
#   1. The scripts/lib/run_cargo.sh helper sources cleanly.
#   2. The cargo-test target (tests/scripts_local_cargo_compliance.rs) passes
#      against HEAD.
#   3. The synthetic-fixture tests (in the same test file) verify scanner
#      correctness.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Shared local cargo defaults (target dir, jobs, test threads).
source "$PROJECT_ROOT/scripts/lib/run_cargo.sh"

LOG_DIR="${TMPDIR:-/tmp}/cass-local-cargo-compliance"
LOG="$LOG_DIR/tafss-e2e.log"
mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG") 2>&1

cleanup() {
    local rc=$?
    if [ "$rc" -ne 0 ]; then
        echo ""
        echo "[tafss_e2e] FAILURE — last 50 log lines:" >&2
        tail -n 50 "$LOG" | sed 's/^/[tafss_e2e]   /' >&2
    fi
    exit "$rc"
}
trap cleanup EXIT

PASS=0
FAIL=0

cd "$PROJECT_ROOT"

# Source the helper to confirm it's syntactically valid bash and defines run_cargo.
echo "[tafss_e2e] sourcing scripts/lib/run_cargo.sh"
if ( source "$PROJECT_ROOT/scripts/lib/run_cargo.sh" && declare -f run_cargo >/dev/null ); then
    echo "[tafss_e2e] OK: run_cargo.sh sources cleanly and defines run_cargo"
    PASS=$((PASS + 1))
else
    echo "[tafss_e2e] FAIL: run_cargo.sh failed to source or define run_cargo"
    FAIL=$((FAIL + 1))
fi

# Run the cargo-test target.
echo "[tafss_e2e] running cargo test scripts_local_cargo_compliance"
if run_cargo test --locked --test scripts_local_cargo_compliance -- --nocapture 2>&1 | tee "$LOG_DIR/tafss-cargo.log"; then
    if grep -q "test result: ok" "$LOG_DIR/tafss-cargo.log"; then
        echo "[tafss_e2e] OK: scanner tests pass"
        PASS=$((PASS + 1))
    else
        echo "[tafss_e2e] FAIL: cargo exited 0 but no 'test result: ok' marker"
        FAIL=$((FAIL + 1))
    fi
else
    echo "[tafss_e2e] FAIL: cargo test failed (see $LOG_DIR/tafss-cargo.log)"
    FAIL=$((FAIL + 1))
fi

echo ""
echo "[tafss_e2e] SUMMARY: PASS=$PASS FAIL=$FAIL"
echo "[tafss_e2e] log: $LOG"
[ "$FAIL" -eq 0 ] && echo "[tafss_e2e] ALL PASS" || exit 1
