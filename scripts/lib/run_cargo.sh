# scripts/lib/run_cargo.sh — shared local cargo invocation helper.
#
# This file is INTENTIONALLY meant to be `source`d, not executed. It exposes
# `run_cargo` as a function plus the `cass_cargo_default` value resolver, and
# exports the resolved defaults (CARGO_TARGET_DIR, CARGO_BUILD_JOBS,
# RUST_TEST_THREADS, NEXTEST_TEST_THREADS) on first use.
#
# Usage:
#     # at top of a shell script
#     source "$(dirname "$0")/lib/run_cargo.sh"
#     run_cargo build --release
#     run_cargo test --test foo -- --nocapture
#
# cass runs every cargo invocation on the local machine. There is no remote
# executor: `run_cargo` resolves the shared defaults and calls `cargo` in the
# caller's working directory.
#
# Resolution order for each parameter (first hit wins):
#   1. user environment variable (CARGO_TARGET_DIR, CARGO_BUILD_JOBS,
#      RUST_TEST_THREADS / NEXTEST_TEST_THREADS)
#   2. user's Cargo configuration (`cargo config get` — this also covers a
#      repo-level .cargo/config.toml; the shipped one sets neither
#      build.target-dir nor build.jobs on purpose)
#   3. the project default below
# An explicit cargo flag in `"$@"` (e.g. `--jobs 8`, `--target-dir ...`,
# `-- --test-threads N`) still outranks everything, because Cargo itself
# orders flags above env vars and config files.

# Absolute path of this file's directory, resolved at source time so the
# project default target dir does not depend on the caller's cwd.
_CASS_RUN_CARGO_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# cass_cargo_default <env-var> <config-key> <default> <value-out> <source-out>
#
# Resolve one parameter per the order above and write the winning value into
# the variable named by <value-out> and the source ("env", "config" or
# "default") into <source-out>. <config-key> may be empty to skip the Cargo
# config lookup (no config key exists for test threads).
#
# The Cargo config is read with
#   cargo -Zunstable-options config get --format json-value <key>
# which needs the pinned nightly toolchain; any error (unset key, non-nightly
# cargo, unreadable config) is treated as "value not set".
cass_cargo_default() {
    local env_name="$1" config_key="$2" default_value="$3"
    local out_value="$4" out_source="$5"
    local value source
    if [ -n "${!env_name:-}" ]; then
        value="${!env_name}"
        source="env"
    elif [ -n "$config_key" ] \
        && value="$(cargo -Zunstable-options config get --format json-value "$config_key" 2>/dev/null)"; then
        # `--format json-value` prints a JSON literal: unwrap the surrounding
        # quotes on string values (numbers and booleans pass through).
        case "$value" in
            \"*\") value="${value#\"}"; value="${value%\"}" ;;
        esac
        source="config"
    else
        value="$default_value"
        source="default"
    fi
    printf -v "$out_value" '%s' "$value"
    printf -v "$out_source" '%s' "$source"
}

# cass_default_target_dir — print the project default build directory:
# `<main checkout>/target`, computed from this file's own location so every
# worktree shares the main checkout's dependency cache. Bare repositories
# (no checkout to attach `target/` to) fall back to
# ${XDG_CACHE_HOME:-$HOME/.cache}/cass/target.
cass_default_target_dir() {
    local common
    if [ "$(git -C "$_CASS_RUN_CARGO_LIB_DIR" rev-parse --is-bare-repository 2>/dev/null)" = "false" ] \
        && common="$(git -C "$_CASS_RUN_CARGO_LIB_DIR" rev-parse --path-format=absolute --git-common-dir 2>/dev/null)" \
        && [ -n "$common" ]; then
        case "$common" in
            */.git) printf '%s\n' "${common%/.git}/target" ;;
            *)      printf '%s\n' "$common/../target" ;;
        esac
    else
        printf '%s\n' "${XDG_CACHE_HOME:-$HOME/.cache}/cass/target"
    fi
}

# Resolved values (filled in by cass_cargo_resolve on the first run_cargo
# call). Resolution is lazy so it always runs after the calling script has
# settled into its working directory.
CASS_CARGO_RESOLVED=0
CASS_CARGO_TARGET_DIR=""
CASS_CARGO_TARGET_DIR_SOURCE=""
CASS_CARGO_BUILD_JOBS=""
CASS_CARGO_BUILD_JOBS_SOURCE=""
CASS_CARGO_TEST_THREADS=""
CASS_CARGO_TEST_THREADS_SOURCE=""

cass_cargo_resolve() {
    [ "$CASS_CARGO_RESOLVED" -eq 1 ] && return 0

    # Build directory: default shares the main checkout's target/ cache.
    cass_cargo_default CARGO_TARGET_DIR build.target-dir \
        "$(cass_default_target_dir)" CASS_CARGO_TARGET_DIR CASS_CARGO_TARGET_DIR_SOURCE

    # Build parallelism: project default is 2 jobs.
    cass_cargo_default CARGO_BUILD_JOBS build.jobs 2 \
        CASS_CARGO_BUILD_JOBS CASS_CARGO_BUILD_JOBS_SOURCE

    # Test threads: no Cargo config key exists, so this is env-or-default.
    # RUST_TEST_THREADS covers `cargo test` binaries; NEXTEST_TEST_THREADS is
    # honoured as a fallback so a nextest-only setting is not silently ignored.
    if [ -n "${RUST_TEST_THREADS:-}" ] || [ -n "${NEXTEST_TEST_THREADS:-}" ]; then
        CASS_CARGO_TEST_THREADS="${RUST_TEST_THREADS:-$NEXTEST_TEST_THREADS}"
        CASS_CARGO_TEST_THREADS_SOURCE="env"
    else
        CASS_CARGO_TEST_THREADS=2
        CASS_CARGO_TEST_THREADS_SOURCE="default"
    fi

    export CARGO_TARGET_DIR="$CASS_CARGO_TARGET_DIR"
    export CARGO_BUILD_JOBS="$CASS_CARGO_BUILD_JOBS"
    export RUST_TEST_THREADS="$CASS_CARGO_TEST_THREADS"
    export NEXTEST_TEST_THREADS="$CASS_CARGO_TEST_THREADS"
    CASS_CARGO_RESOLVED=1
}

# run_cargo: log one structured line to stderr, then run cargo locally with
# the resolved defaults in the environment.
run_cargo() {
    cass_cargo_resolve
    printf '[run_cargo] target=%s (source=%s) jobs=%s (source=%s) test_threads=%s (source=%s)\n' \
        "$CASS_CARGO_TARGET_DIR" "$CASS_CARGO_TARGET_DIR_SOURCE" \
        "$CASS_CARGO_BUILD_JOBS" "$CASS_CARGO_BUILD_JOBS_SOURCE" \
        "$CASS_CARGO_TEST_THREADS" "$CASS_CARGO_TEST_THREADS_SOURCE" >&2
    cargo "$@"
}

# Export so subshells inherit the function (POSIX `export -f` is a bashism;
# this script is bash-targeted, which AGENTS.md already mandates).
export -f run_cargo 2>/dev/null || true
