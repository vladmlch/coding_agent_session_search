#!/usr/bin/env bash
# regenerate_plans_snapshots.sh — regenerate Plans-subview snapshot artifacts.
#
# Per coding_agent_session_search-vz9t8.3. Runs the FTUI snapshot harness
# under UPDATE_GOLDENS=1 with the Plans-fixture DB loaded. Writes 6 snapshot
# files (compact+wide × normal/sparse/empty).
#
# Prerequisites (deferred to follow-up bead):
#   - tests/fixtures/analytics/plans_normal.db
#   - tests/fixtures/analytics/plans_sparse.db
#   - tests/fixtures/analytics/plans_empty.db

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=../lib/run_cargo.sh
source "$SCRIPT_DIR/../lib/run_cargo.sh"
CASS_WORK_DIR="${CASS_WORK_DIR:-${TMPDIR:-/tmp}/cass-vz9t8-3}"
LOG="$CASS_WORK_DIR/regenerate-plans-snapshots.log"
mkdir -p "$CASS_WORK_DIR"
exec > >(tee -a "$LOG") 2>&1

cleanup() {
    local rc=$?
    if [ "$rc" -ne 0 ]; then
        echo ""
        echo "[regen_plans] FAILURE — last 50 log lines:" >&2
        tail -n 50 "$LOG" | sed 's/^/[regen_plans]   /' >&2
    fi
    exit "$rc"
}
trap cleanup EXIT

FIX_DIR="$PROJECT_ROOT/tests/fixtures/analytics"
mkdir -p "$FIX_DIR"

for fix in plans_normal.db plans_sparse.db plans_empty.db; do
    if [ ! -f "$FIX_DIR/$fix" ]; then
        echo "[regen_plans] WARN: fixture missing: $FIX_DIR/$fix"
        echo "[regen_plans] hint: see tests/fixtures/analytics/README.md (or"
        echo "[regen_plans]       file a follow-up bead) for fixture-generation steps."
    fi
done

if [ -z "${ALLOW_REGEN_WITHOUT_FIXTURES:-}" ]; then
    echo ""
    echo "[regen_plans] Required fixture DBs are not yet committed."
    echo "[regen_plans] Set ALLOW_REGEN_WITHOUT_FIXTURES=1 to attempt regeneration"
    echo "[regen_plans] against synthesized in-memory data (will produce stale snapshots)."
    exit 0
fi

# Real regeneration path (only when fixtures are present).
cd "$PROJECT_ROOT"
echo "[regen_plans] running cargo test ftui_harness_snapshots cassapp_plans"
UPDATE_GOLDENS=1 run_cargo \
    test --test ftui_harness_snapshots cassapp_plans -- --nocapture

echo "[regen_plans] DONE. Re-stage and commit the regenerated .snap files."
