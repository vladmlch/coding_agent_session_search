#!/usr/bin/env bash
# cuu3f_prepush_hook_e2e.sh — exercise the pre-push hook against a temp repo.
#
# Per coding_agent_session_search-cuu3f. Validates that the pre-push hook:
#   1. Warns when pushing commits to main without any bead-ID reference.
#   2. Stays quiet when commits include a bead-ID token.
#   3. Stays quiet when pushing to a non-main branch (only main triggers).
#   4. Always exits 0 (warning, not block).

set -euo pipefail

# Resolve project paths regardless of cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
HOOK="$PROJECT_ROOT/scripts/git-hooks/pre-push.sh"

[ -x "$HOOK" ] || {
    echo "ERROR: $HOOK not found or not executable" >&2
    exit 1
}

WORK_ROOT="$(mktemp -d -t cuu3f-prepush-XXXXXXXX)"
LOG="${CASS_WORK_DIR:-${TMPDIR:-/tmp}/cass-cuu3f}/prepush-hook-e2e.log"
mkdir -p "$(dirname "$LOG")"
exec > >(tee -a "$LOG") 2>&1

cleanup() {
    local rc=$?
    if [ "$rc" -ne 0 ]; then
        echo ""
        echo "[cuu3f_e2e] FAILURE — last 50 log lines:" >&2
        tail -n 50 "$LOG" | sed 's/^/[cuu3f_e2e]   /' >&2
    fi
    rm -rf "$WORK_ROOT"
    exit "$rc"
}
trap cleanup EXIT

start_ts="$(date +%s)"
fail() {
    echo "[cuu3f_e2e] FAIL: $*" >&2
    exit 1
}
log() {
    echo "[cuu3f_e2e] $*"
}

# ---------------- Set up upstream + clone ----------------
UPSTREAM="$WORK_ROOT/upstream.git"
LOCAL="$WORK_ROOT/local"
git init --bare "$UPSTREAM" >/dev/null
git -c init.defaultBranch=main init "$LOCAL" >/dev/null
cd "$LOCAL"
git config user.email "test@example.test"
git config user.name "test"
git remote add origin "$UPSTREAM"

# Seed a baseline commit so we can compare ranges later.
echo "seed" > seed.txt
git add seed.txt
git commit -m "init" >/dev/null
git push --set-upstream origin main >/dev/null 2>&1

# Install the project's pre-push hook into this temp repo.
cp "$HOOK" .git/hooks/pre-push
chmod +x .git/hooks/pre-push

# ---------------- Scenario 1: push to main WITHOUT bead ID — should warn ----------------
log "scenario=1 push to main without bead-ID — expect WARN"
echo "noref" > a.txt
git add a.txt
git commit -m "feat: add a.txt without any bead-id" >/dev/null
push_out="$(git push origin main 2>&1)"
log "scenario=1 push_out=<<<$push_out>>>"
echo "$push_out" | grep -qi "warning: pre-push" || fail "scenario 1: expected 'warning: pre-push' in stderr; got: $push_out"
log "scenario=1 OK (warning emitted)"

# ---------------- Scenario 2: push to main WITH bead ID — should be quiet ----------------
log "scenario=2 push to main WITH bead-ID — expect no warn"
echo "withref" > b.txt
git add b.txt
git commit -m "feat: add b.txt (coding_agent_session_search-cuu3f)" >/dev/null
push_out="$(git push origin main 2>&1)"
log "scenario=2 push_out=<<<$push_out>>>"
echo "$push_out" | grep -qi "warning: pre-push" && fail "scenario 2: did NOT expect warning when bead-ID present; got: $push_out"
log "scenario=2 OK (no warning)"

# ---------------- Scenario 3: push to non-main branch — should be quiet regardless ----------------
log "scenario=3 push to feature branch without bead-ID — expect no warn"
git checkout -b feature/test >/dev/null
echo "feat" > c.txt
git add c.txt
git commit -m "feat: c.txt without bead-id (allowed on feature branch)" >/dev/null
push_out="$(git push --set-upstream origin feature/test 2>&1)"
log "scenario=3 push_out=<<<$push_out>>>"
echo "$push_out" | grep -qi "warning: pre-push" && fail "scenario 3: warning fired on non-main branch; got: $push_out"
log "scenario=3 OK (no warning on non-main)"

# ---------------- Scenario 4: hook never blocks (always exit 0) ----------------
log "scenario=4 confirming hook exit code is 0 even when warning emitted"
git checkout main >/dev/null
echo "block-test" > d.txt
git add d.txt
git commit -m "chore: d.txt no bead-id" >/dev/null
if ! git push origin main 2>&1 | tee /tmp/cuu3f-scenario4.tmp; then
    fail "scenario 4: hook BLOCKED the push (should only warn)"
fi
log "scenario=4 OK (push succeeded despite warning)"

elapsed=$(( $(date +%s) - start_ts ))
echo ""
echo "[cuu3f_e2e] ALL PASS (4/4 scenarios) — ${elapsed}s wall"
echo "[cuu3f_e2e] log written to: $LOG"
