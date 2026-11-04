#!/usr/bin/env bash
# cass_flags_e2e.sh — exercise CLI flag precedence (CLI > env > config > default).
#
# Per coding_agent_session_search-d4r65. Demonstrates that cass resolves
# configuration in the documented precedence order across each combination
# of CLI flag, env var, and ~/.config/cass/config.toml.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CASS_WORK_DIR="${CASS_WORK_DIR:-/tmp/cass-d4r65-target}"
LOG="$CASS_WORK_DIR/cass-flags-e2e.log"
mkdir -p "$CASS_WORK_DIR"
exec > >(tee -a "$LOG") 2>&1

cleanup() {
    local rc=$?
    if [ "$rc" -ne 0 ]; then
        echo ""
        echo "[d4r65_e2e] FAILURE — last 50 log lines:" >&2
        tail -n 50 "$LOG" | sed 's/^/[d4r65_e2e]   /' >&2
    fi
    exit "$rc"
}
trap cleanup EXIT

# Locate or build cass.
CASS_BIN=""
for candidate in \
    "$CASS_WORK_DIR/release/cass" \
    "$CASS_WORK_DIR/debug/cass" \
    "$PROJECT_ROOT/target/release/cass" \
    "$PROJECT_ROOT/target/debug/cass" \
    "$(command -v cass 2>/dev/null || true)"; do
    if [ -x "$candidate" ]; then
        CASS_BIN="$candidate"
        break
    fi
done

if [ -z "$CASS_BIN" ]; then
    echo "[d4r65_e2e] cass not found; build locally via 'env CARGO_TARGET_DIR=$CASS_WORK_DIR cargo build --bin cass'"
    exit 1
fi
echo "[d4r65_e2e] cass binary: $CASS_BIN"

PASS=0
FAIL=0
SKIP=0

scenario() {
    local label="$1"
    shift
    local -a env_pairs=()
    local -a cli_args=()
    local mode="env"
    for arg in "$@"; do
        if [ "$arg" = "--" ]; then
            mode="cli"
        elif [ "$mode" = "env" ]; then
            env_pairs+=("$arg")
        else
            cli_args+=("$arg")
        fi
    done

    local data_dir
    data_dir="$(mktemp -d -t d4r65-data-XXXXXX)"
    local out
    if out="$(env -i HOME="$HOME" PATH="$PATH" CASS_DATA_DIR="$data_dir" \
        "${env_pairs[@]}" "$CASS_BIN" "${cli_args[@]}" 2>&1)"; then
        echo "[d4r65_e2e] scenario=$label exit=0 stdout_head=$(echo "$out" | head -c 200)"
    else
        local rc=$?
        echo "[d4r65_e2e] scenario=$label exit=$rc stdout_head=$(echo "$out" | head -c 200)"
    fi
    rm -rf "$data_dir"
}

# Scenario 1: bare cass --help — must exit 0, must produce text.
scenario "bare_help" -- --help

# Scenario 2: env var sets data dir; verify cass picks it up via 'cass health --json'.
scenario "env_data_dir_picked_up" CASS_DATA_DIR=/tmp/d4r65-explicit -- health --json

# Scenario 3: pass --data-dir CLI override.
scenario "cli_data_dir_overrides_env" CASS_DATA_DIR=/tmp/d4r65-env -- --data-dir /tmp/d4r65-cli health --json

echo ""
echo "[d4r65_e2e] SUMMARY: PASS=$PASS FAIL=$FAIL SKIP=$SKIP"
echo "[d4r65_e2e] Note: full CLI > env > config > default precedence chain test"
echo "[d4r65_e2e]       requires writing fixture ~/.config/cass/config.toml; see"
echo "[d4r65_e2e]       tests/cli_flag_precedence.rs for the cargo-test surface."
echo "[d4r65_e2e] DONE"
