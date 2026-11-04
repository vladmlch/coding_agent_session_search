#!/usr/bin/env bash
# 8tgic_simd_tests_e2e.sh — exercise the SIMD dot-product test suite end-to-end.
#
# Per coding_agent_session_search-8tgic. Runs every simd_tests::* test in
# isolation, captures wall time, and aggregates a structured summary.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=../lib/run_cargo.sh
source "$SCRIPT_DIR/../lib/run_cargo.sh"
CASS_WORK_DIR="${CASS_WORK_DIR:-${TMPDIR:-/tmp}/cass-8tgic}"
LOG="$CASS_WORK_DIR/simd-tests-e2e.log"
mkdir -p "$CASS_WORK_DIR"
exec > >(tee -a "$LOG") 2>&1

cleanup() {
    local rc=$?
    if [ "$rc" -ne 0 ]; then
        echo ""
        echo "[8tgic_e2e] FAILURE — last 50 log lines:" >&2
        tail -n 50 "$LOG" | sed 's/^/[8tgic_e2e]   /' >&2
    fi
    exit "$rc"
}
trap cleanup EXIT

run_test() {
    local name="$1"
    local start
    start=$(date +%s%N)
    local out
    if out="$(cd "$PROJECT_ROOT" && run_cargo \
        test --test simd_tests "$name" -- --nocapture --test-threads=1 2>&1)"; then
        local elapsed_ns=$(($(date +%s%N) - start))
        local elapsed_ms=$((elapsed_ns / 1000000))
        echo "[8tgic_e2e] OK: $name (${elapsed_ms}ms)"
        return 0
    else
        echo "[8tgic_e2e] FAIL: $name"
        echo "$out" | tail -30 | sed 's/^/[8tgic_e2e]   /'
        return 1
    fi
}

PASS=0
FAIL=0
TESTS=(
    simd_dot_fp_tolerance_against_scalar
    simd_dot_random_inputs_proptest_lite
    simd_dot_edge_zeros
    simd_dot_edge_unit_vector
    simd_dot_edge_denormals
    simd_dot_edge_nan_propagation
    simd_dot_edge_infinity
    simd_dot_edge_mismatched_length_panics
    simd_dot_f16_fp_tolerance_against_scalar
    simd_dot_f16_edge_zeros
)

for t in "${TESTS[@]}"; do
    if run_test "$t"; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
    fi
done

echo ""
echo "[8tgic_e2e] SUMMARY: PASS=$PASS FAIL=$FAIL TOTAL=${#TESTS[@]}"
echo "[8tgic_e2e] log written to: $LOG"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
echo "[8tgic_e2e] ALL PASS"
