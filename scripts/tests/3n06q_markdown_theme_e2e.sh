#!/usr/bin/env bash
# 3n06q_markdown_theme_e2e.sh — orchestrate markdown theme distinct-color tests.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=../lib/run_cargo.sh
source "$SCRIPT_DIR/../lib/run_cargo.sh"
CASS_WORK_DIR="${CASS_WORK_DIR:-${TMPDIR:-/tmp}/cass-3n06q}"
LOG="$CASS_WORK_DIR/markdown-theme-e2e.log"
mkdir -p "$CASS_WORK_DIR"
exec > >(tee -a "$LOG") 2>&1

cleanup() {
    local rc=$?
    if [ "$rc" -ne 0 ]; then
        echo ""
        echo "[3n06q_e2e] FAILURE — last 50 log lines:" >&2
        tail -n 50 "$LOG" | sed 's/^/[3n06q_e2e]   /' >&2
    fi
    exit "$rc"
}
trap cleanup EXIT

PASS=0
FAIL=0

cd "$PROJECT_ROOT"
echo "[3n06q_e2e] running cargo test markdown_theme_distinct_colors"
TEST_OUT="$CASS_WORK_DIR/test.out"
if run_cargo test --test markdown_theme_distinct_colors -- --nocapture \
    > "$TEST_OUT" 2>&1; then
    if grep -q "test result: ok" "$TEST_OUT"; then
        echo "[3n06q_e2e] OK: tests passed"
        PASS=$((PASS + 1))
    else
        echo "[3n06q_e2e] FAIL: cargo exited 0 but no 'test result: ok'"
        FAIL=$((FAIL + 1))
    fi
else
    echo "[3n06q_e2e] FAIL: cargo test failed"
    tail -50 "$TEST_OUT" | sed 's/^/[3n06q_e2e]   /'
    FAIL=$((FAIL + 1))
fi

echo "[3n06q_e2e] SUMMARY: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ] && echo "[3n06q_e2e] ALL PASS" || exit 1
