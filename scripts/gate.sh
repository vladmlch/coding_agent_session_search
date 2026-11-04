#!/usr/bin/env bash
# gate.sh — the blocking quality gate, run locally in one batched pass.
#
# The project has no remote CI, so the only gate is agent-run. An agent that
# runs `cargo check`, then clippy, then tests loses most of an hour to
# context switches — and the temptation to push unverified follows. This
# script runs the full check set locally, in one pass, and prints one
# receipt line per stage:
#
#   STAGE=<name> EXIT=<code>
#
# The receipt is what a bead closure or a push cites. Nothing here weakens a
# gate: a stage failure never stops the later stages (you want the whole
# picture from one run).
#
# Usage:
#   scripts/gate.sh                 # fmt, clippy, lib tests, targeted integration, goldens
#   scripts/gate.sh --lib-filter 'quill_bridge::tests health_watermark'   # narrow the lib run
#   scripts/gate.sh --lib-only      # only fmt, clippy and the lib suite
#   scripts/gate.sh --no-lib        # skip the lib suite (integration + goldens)
#   scripts/gate.sh --no-bin        # skip the cass bin's own unit tests (stage bin-cass)
#
# Exit code: 0 when every stage exited 0 and the checkout stayed stable for
# the whole run, 1 otherwise.

set -uo pipefail

# Under `set -u`, bash < 4.4 expands an empty "${arr[@]}" as an unbound
# variable and aborts; this script relies on the empty-array expansion
# throughout (and `declare -A` below already rules out bash 3.x). Refuse
# early with a clear diagnostic instead of dying mid-run on
# 'arr[@]: unbound variable'.
if [ "${BASH_VERSINFO[0]:-0}" -lt 4 ] \
    || { [ "${BASH_VERSINFO[0]:-0}" -eq 4 ] && [ "${BASH_VERSINFO[1]:-0}" -lt 4 ]; }; then
    echo "gate: bash >= 4.4 required (running ${BASH_VERSION:-unknown}); install a newer bash (on macOS: brew install bash) and re-run" >&2
    exit 2
fi

_GATE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)" || exit 1
# Shared local cargo helpers: run_cargo, cass_cargo_resolve (jobs, test
# threads, target dir), sha256 and run_with_timeout.
source "$_GATE_DIR/lib/run_cargo.sh" || {
    echo "gate: cannot load ${_GATE_DIR}/lib/run_cargo.sh" >&2
    exit 2
}

test_log_counts() {
    local log="$1"
    shift
    local filters_nl=""
    if [ "$#" -gt 0 ]; then
        printf -v filters_nl '%s\n' "$@"
    fi
    GATE_TEST_FILTERS="$filters_nl" awk '
        BEGIN {
            n = split(ENVIRON["GATE_TEST_FILTERS"], raw_filters, "\n")
            for (i = 1; i <= n; i++) {
                if (length(raw_filters[i]) > 0) {
                    filters[++filter_count] = raw_filters[i]
                }
            }
        }
        /^running [0-9]+ tests?$/ {
            announced[++depth] = $2
            next
        }
        depth == 1 && /^test [^ ].* \.\.\. ok$/ {
            line = $0
            sub(/^test /, "", line)
            sub(/ \.\.\. ok$/, "", line)
            sub(/ - should panic$/, "", line)
            for (f = 1; f <= filter_count; f++) {
                if (index(line, filters[f]) > 0) {
                    matched[f]++
                }
            }
            next
        }
        /^test result: ok\. [0-9]+ passed; [0-9]+ failed; [0-9]+ ignored; [0-9]+ measured; [0-9]+ filtered out/ {
            if (depth == 0) { incomplete++; next }
            if ($6 != 0 || $4 + $6 + $8 + $10 != announced[depth]) incomplete++
            if ($4 == 0) empty++
            delete announced[depth--]
            # Inherited subprocess output nests inside its parent test run.
            # Count each top-level binary once; every child must still finish.
            if (depth == 0) { binaries++; passed += $4 }
            next
        }
        /^test result:/ { incomplete++ }
        END {
            for (f = 1; f <= filter_count; f++) {
                if (!matched[f]) {
                    printf "gate: test filter matched 0 passing tests: %s\n", filters[f] > "/dev/stderr"
                    unmatched++
                }
            }
            if (binaries == 0 || empty > 0 || incomplete > 0 || depth != 0 || unmatched > 0) exit 1
            printf "PASSED=%d BINARIES=%d\n", passed, binaries
        }
    ' "$log"
}

verify_receipt() {
    local receipt="$1" runner_rc="$2"
    shift 2
    local failed=0 line name stage last_stage=""
    local -A seen=() counts=()
    if [ "$runner_rc" -ne 0 ]; then
        echo "STAGE=runner EXIT=${runner_rc} (runner did not complete normally)"
        failed=1
    fi
    while IFS= read -r line; do
        if [[ "$line" =~ ^TEST_COUNT=([a-z0-9_-]+)\ PASSED=([1-9][0-9]*)\ BINARIES=([1-9][0-9]*)$ ]]; then
            counts["${BASH_REMATCH[1]}"]=1
        elif [[ "$line" =~ ^STAGE=([a-z0-9_-]+)\ EXIT=([0-9]+)$ ]]; then
            name="${BASH_REMATCH[1]}"
            [ -n "${seen[$name]:-}" ] && failed=1
            seen["$name"]=1
            last_stage="$name"
            [ "${BASH_REMATCH[2]}" -eq 0 ] || failed=1
            echo "$line"
        fi
    done < "$receipt"
    for stage in "$@"; do
        if [ -z "${seen[$stage]:-}" ]; then
            echo "STAGE=${stage} EXIT=missing (no terminal receipt)"
            failed=1
        fi
        case "$stage" in
            lib-tests|test-*|goldens|goldens-regen)
                if [ -z "${counts[$stage]:-}" ]; then
                    echo "gate: ${stage} has no positive terminal test count" >&2
                    failed=1
                fi ;;
        esac
    done
    if [ "$last_stage" != job-complete ]; then
        echo "gate: missing final job-complete receipt" >&2
        failed=1
    fi
    return "$failed"
}

run_fmt() {
    local formatter wrapper rc
    formatter="${RUSTFMT:-$(command -v rustfmt)}" || return 1
    wrapper="$(mktemp -t cass-gate-rustfmt.XXXXXX)" || return 1
    # cargo-fmt maps a signal-killed rustfmt's missing exit code to success.
    # A waiting shell converts that signal into a normal nonzero exit status.
    # Keep the wrapper for diagnostics and never exec away its waiting shell.
    cat > "$wrapper" <<'SH'
#!/usr/bin/env bash
"${CASS_GATE_RUSTFMT:?}" "$@"
exit "$?"
SH
    chmod +x "$wrapper" || return 1
    CASS_GATE_RUSTFMT="$formatter" RUSTFMT="$wrapper" cargo fmt --check
    rc=$?
    return "$rc"
}

# The complete set of paths any stage can read: tracked files plus untracked
# non-ignored files under the input roots. Nothing is transferred anywhere,
# so every byte the compilers and tests see is verified — no exclusions.
source_paths() {
    git ls-files --cached --others --exclude-standard -z -- \
        Cargo.toml Cargo.lock rust-toolchain.toml build.rs build_support \
        src tests benches scripts .cargo .github README.md | sort -z
}

# Combined SHA-256 of every input file's own digest line.
source_content() {
    source_paths | xargs -0 bash -c 'sha256 "$@"' _ | sha256 | cut -d' ' -f1
}

# Digest of everything not yet committed: the working-tree diff plus a
# per-file digest of each untracked, non-ignored file in the repo.
source_digest() {
    {
        git diff --binary HEAD
        git ls-files --others --exclude-standard -z | xargs -0 bash -c 'sha256 "$@"' _
    } | sha256 | cut -d' ' -f1
}

# Recompute the three source attestations captured before the run and
# compare. Any drift — a commit, an edit, a new file — means the receipt
# describes a different tree than the one still checked out.
verify_source_stability() {
    local stored_head="$1" stored_diff="$2" stored_content="$3" rc=0
    local reason=""
    if [ "$(git rev-parse HEAD 2>/dev/null)" != "$stored_head" ]; then
        reason="HEAD moved"
        rc=1
    fi
    if [ "$(source_digest)" != "$stored_diff" ]; then
        reason="${reason:+$reason, }working-tree diff changed"
        rc=1
    fi
    if [ "$(source_content)" != "$stored_content" ]; then
        reason="${reason:+$reason, }input content changed"
        rc=1
    fi
    if [ "$rc" -ne 0 ]; then
        echo "gate: checkout changed during validation (${reason})" >&2
    fi
    echo "STAGE=source-stability EXIT=${rc}"
    return "$rc"
}

# Keep complete terminal output and require a positive test count. Cargo exits
# zero when a misspelled filter selects no tests; that is not validation.
run_tests() {
    local stage="$1"
    shift
    local -a filters=()
    while [ "${1:-}" = --filter ]; do
        filters+=("$2")
        shift 2
    done
    local log rc counts
    local -a statuses
    log="$(mktemp -t cass-gate-tests.XXXXXX)"
    "$@" 2>&1 | tee "$log"
    statuses=("${PIPESTATUS[@]}")
    rc=${statuses[0]}
    [ "${statuses[1]}" -eq 0 ] || rc=1
    if [ "$rc" -eq 0 ]; then
        if counts="$(test_log_counts "$log" "${filters[@]}")"; then
            echo "TEST_COUNT=${stage} ${counts}"
        else
            echo "gate: ${stage} has missing or empty test binaries (log=${log})" >&2
            rc=1
        fi
    fi
    echo "STAGE=${stage} EXIT=${rc}"
}

# The scanner is always local: `ubs` from UBS_BIN or PATH, pinned by SHA-256
# on both the runner and its module routing contract. The gate never
# downloads it — a missing or mismatched install fails the stage and prints
# the git-clone install hint.
run_ubs() {
    local expected actual scanner module_dir expected_sha expected_contract_sha
    expected="$(tr -d '[:space:]' < scripts/ubs-version.txt)"
    # The runner verifies its language modules against embedded release hashes.
    # Updating the pin also requires reviewing the new runner digest here.
    if [ "$expected" != v5.4.4 ]; then
        echo "gate: no reviewed UBS runner digest for ${expected}" >&2
        return 1
    fi
    expected_sha=bdeccbb35f2056e6177c73b3f0f9966d20d1cbeff22b7f084133c4a6ac625ee2
    expected_contract_sha=7319b2e5e085d0dd2434e5114291b2dee9fbbb96e2cfe02027eb263abcb06ac3
    scanner="${UBS_BIN:-$(command -v ubs || true)}"
    if [ -z "$scanner" ] || [ ! -f "$scanner" ]; then
        echo "gate: UBS ${expected} not found (set UBS_BIN or put ubs on PATH)" >&2
        echo "gate: install: git clone --depth 1 --branch ${expected} https://github.com/Dicklesworthstone/ultimate_bug_scanner <dir>" >&2
        return 1
    fi
    if [ "$(sha256 "$scanner" | cut -d' ' -f1)" != "$expected_sha" ]; then
        echo "gate: UBS runner bytes differ from the pinned release (${scanner})" >&2
        echo "gate: install: git clone --depth 1 --branch ${expected} https://github.com/Dicklesworthstone/ultimate_bug_scanner <dir>" >&2
        return 1
    fi
    # Multi-file language routing runs before any scan. A pinned runner
    # without its contract would classify every explicit target as
    # unsupported and exit without scanning anything.
    module_dir="${UBS_MODULE_DIR:-}"
    if [ -z "$module_dir" ] || [ ! -f "$module_dir/contract.json" ]; then
        echo "gate: UBS module dir not found (set UBS_MODULE_DIR to the scanner's modules/ directory)" >&2
        echo "gate: install: git clone --depth 1 --branch ${expected} https://github.com/Dicklesworthstone/ultimate_bug_scanner <dir>" >&2
        return 1
    fi
    if [ "$(sha256 "$module_dir/contract.json" | cut -d' ' -f1)" != "$expected_contract_sha" ]; then
        echo "gate: UBS routing contract bytes differ from the pinned release (${module_dir}/contract.json)" >&2
        echo "gate: install: git clone --depth 1 --branch ${expected} https://github.com/Dicklesworthstone/ultimate_bug_scanner <dir>" >&2
        return 1
    fi
    actual="$(bash "$scanner" --version)" || return 1
    echo "UBS_VERSION=${actual} UBS_PIN=${expected} UBS_SHA256=${expected_sha}"
    echo "UBS_MODULE_DIR=${module_dir}"
    echo "UBS_CONTRACT_SHA256=${expected_contract_sha}"
    if [ "$#" -eq 0 ]; then
        echo "UBS_FILES=0 (no changed scanner-supported source files)"
        return 0
    fi
    printf 'UBS_FILE=%s\n' "$@"
    # Text retains categories and source samples needed to diagnose a red gate.
    bash "$scanner" --module-dir="$module_dir" --no-auto-update --format=text --ci --fail-on-warning "$@"
}

# Exercise the exact receipt parser without running a build or substituting
# tools. The shell regression suite feeds it recorded terminal-output shapes.
if [ "${1:-}" = --verify-receipt ]; then
    shift
    verify_receipt "$@"
    exit $?
fi
if [ "${1:-}" = --verify-test-log ]; then
    shift
    test_log_counts "$@"
    exit $?
fi
if [ "${1:-}" = --verify-fmt ]; then
    run_fmt
    exit $?
fi
# Re-verify stored source attestations (see verify_source_stability); the
# regression suite captures a state, perturbs the tree, and expects RED.
if [ "${1:-}" = --verify-source-stability ]; then
    shift
    verify_source_stability "$@"
    exit $?
fi
# Print the three attestations a run records before starting, for the
# regression suite to feed back into --verify-source-stability.
if [ "${1:-}" = --print-source-state ]; then
    state_head="$(git rev-parse HEAD)" || exit 1
    state_diff="$(source_digest)" || exit 1
    state_content="$(source_content)" || exit 1
    printf 'SOURCE_HEAD=%s\nSOURCE_DIFF_SHA256=%s\nSOURCE_CONTENT_SHA256=%s\n' \
        "$state_head" "$state_diff" "$state_content"
    exit 0
fi
# Run the pinned UBS stage as it runs inside the gate, then report the stage
# verdict the same way the run does.
if [ "${1:-}" = --verify-ubs ]; then
    shift
    run_ubs "$@"
    ubs_rc=$?
    echo "STAGE=ubs EXIT=${ubs_rc}"
    exit "$ubs_rc"
fi

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)" || exit 1
cd "$PROJECT_ROOT" || exit 1

# The project has no remote CI. A workflow file is never a verification or
# publication lane: finished fixes parked in one for a job that never runs
# sat unapplied on main (bead nqbwq). Refuse to gate a tree carrying one.
if [ -d .github/workflows ] && [ -n "$(ls -A .github/workflows)" ]; then
    echo "gate: .github/workflows must stay empty (the project has no remote CI; verify with scripts/gate.sh):" >&2
    ls -A .github/workflows >&2
    exit 2
fi

LIB_FILTER=""
RUN_LIB=1
RUN_INTEGRATION=1
RUN_BIN=1
RUN_GOLDENS=1
REGEN_GOLDENS=0
RUN_DOCS_TRUTH=0
PRINT_UBS_FILES=0
UBS_FILES=()
# Each entry is `<test-binary>` or `<test-binary>:<filter words>`; the filter
# is passed after `--` so one run can select only the tests a change touches.
# search_metamorphic is the set-algebra oracle for search semantics (terms,
# Boolean grammar, time windows; bead 2l1b0.68): a wrong-answer regression
# there is invisible to every golden. pages_bundle holds the Pages viewer's
# safety checks (CSP, sanitizer, crypto worker, auth expiry, service worker);
# 20 of them sat red for days because no gate ran the target (2l1b0.77).
# search_service compiles the `cass serve` sources and their unit tests (the
# cass bin itself is `test = false`); 8 of them sat red for 9 days (5188e).
INTEGRATION_TESTS=(cli_robot bookmarks_cli search_metamorphic pages_bundle search_service)
while [ $# -gt 0 ]; do
    case "$1" in
        --lib-filter) LIB_FILTER="$2"; shift 2 ;;
        --no-lib) RUN_LIB=0; shift ;;
        --ubs-files) IFS=',' read -r -a UBS_FILES <<<"$2"; shift 2 ;;
        --integration) IFS=',' read -r -a INTEGRATION_TESTS <<<"$2"; shift 2 ;;
        # Regenerate goldens (UPDATE_GOLDENS=1) before verifying them. The
        # verify stage still runs, and every resulting diff under tests/golden
        # must be read before it is committed — regeneration is never a way to
        # make a red golden green.
        --regen-goldens) REGEN_GOLDENS=1; shift ;;
        # A full run can be split: `--lib-only` (fmt, clippy, lib suite) and a
        # second run with `--no-lib` (integration + goldens).
        --lib-only) RUN_INTEGRATION=0; RUN_GOLDENS=0; shift ;;
        --no-goldens) RUN_GOLDENS=0; shift ;;
        --no-bin) RUN_BIN=0; shift ;;
        # README ↔ code truth (WS-A.9): key bindings, `cass … --flag` usages
        # and env vars, checked with the debug binary the integration stage
        # just built. Opt-in until the README is clean against main.
        --docs-truth) RUN_DOCS_TRUTH=1; shift ;;
        # Print the final scanner file set (after .ubsignore filtering) and
        # stop — the regression suite checks scope selection with it.
        --print-ubs-files) PRINT_UBS_FILES=1; shift ;;
        -h|--help) sed -n '2,24p' "$0"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

# Knobs removed with the remote execution path are diagnosed, not silently
# ignored: the gate always runs locally, once, with the shared cargo defaults.
for removed_env in GATE_RETRIES GATE_RETRY_SLEEP GATE_BUILD_JOBS GATE_TARGET_DIR; do
    if [ -n "${!removed_env:-}" ]; then
        echo "gate: unknown argument: ${removed_env} is no longer supported (the gate runs locally; jobs, threads and the target dir come from scripts/lib/run_cargo.sh and standard Cargo variables)" >&2
        exit 2
    fi
done

# A wedged test (a producer parked forever at ~0 CPU) would otherwise hold
# the whole run until killed by hand. The per-stage timeout turns that into a
# loud EXIT=124 on that stage instead. It bounds the test run only: the
# matching build stage compiles first, because a cold cache can spend the
# whole budget compiling before any test runs (gate b82 once reported
# lib-tests EXIT=124 before a single test, after a 72-minute clippy compile).
LIB_TIMEOUT="${GATE_LIB_TIMEOUT_SECS:-2400}"
if [[ ! "$LIB_TIMEOUT" =~ ^[1-9][0-9]*$ ]]; then
    echo "gate: GATE_LIB_TIMEOUT_SECS must be a positive integer" >&2
    exit 2
fi
BIN_TIMEOUT="${GATE_BIN_TIMEOUT_SECS:-900}"
if [[ ! "$BIN_TIMEOUT" =~ ^[1-9][0-9]*$ ]]; then
    echo "gate: GATE_BIN_TIMEOUT_SECS must be a positive integer" >&2
    exit 2
fi
UBS_TIMEOUT="${GATE_UBS_TIMEOUT_SECS:-300}"
if [[ ! "$UBS_TIMEOUT" =~ ^[1-9][0-9]*$ ]]; then
    echo "gate: GATE_UBS_TIMEOUT_SECS must be a positive integer" >&2
    exit 2
fi

# Resolve and export the shared cargo defaults (target dir, build jobs, test
# threads) for everything the run invokes — including the plain `cargo` run
# under `timeout`, which cannot exec a shell function.
cass_cargo_resolve

SOURCE_HEAD="$(git rev-parse HEAD)" || exit 1
if [ "${#UBS_FILES[@]}" -eq 0 ]; then
    # Diff against the published main when it is known (origin/main), else the
    # local main branch; GATE_BASE overrides either.
    if [ -n "${GATE_BASE:-}" ]; then
        UBS_BASE="$GATE_BASE"
    elif git rev-parse --verify --quiet "origin/main^{commit}" >/dev/null; then
        UBS_BASE="$(git merge-base origin/main "$SOURCE_HEAD")" || exit 1
    else
        UBS_BASE="$(git merge-base main "$SOURCE_HEAD")" || exit 1
    fi
    while IFS= read -r -d '' path; do
        [ -f "$path" ] && UBS_FILES+=("$path")
    done < <({
        git diff --name-only -z "$UBS_BASE" -- '*.rs' '*.py' '*.sh' '*.js' '*.ts' '*.tsx' '*.jsx'
        git ls-files --others --exclude-standard -z -- '*.rs' '*.py' '*.sh' '*.js' '*.ts' '*.tsx' '*.jsx'
    } | sort -zu)
fi
# Honour .ubsignore. The runner scans every file it is named, even one its
# own .ubsignore, --ignore-file or --exclude covers, so the config-level
# suppression AGENTS.md prescribes only takes effect if the gate drops those
# paths before naming them. Each skipped path is printed in the receipt.
if [ -f .ubsignore ] && [ "${#UBS_FILES[@]}" -gt 0 ]; then
    declare -A ubs_ignored=()
    while IFS= read -r -d '' path; do
        ubs_ignored["$path"]=1
    done < <(git ls-files -z -c -o -i --exclude-from=.ubsignore -- "${UBS_FILES[@]}")
    ubs_kept=()
    for path in "${UBS_FILES[@]}"; do
        if [ -n "${ubs_ignored[${path#./}]:-}" ]; then
            echo "gate: UBS skips .ubsignore path ${path}" >&2
        else
            ubs_kept+=("$path")
        fi
    done
    UBS_FILES=("${ubs_kept[@]}")
fi
for path in "${UBS_FILES[@]}"; do
    if [[ "$path" = /* || "$path" = .. || "$path" = ../* || "$path" = */../* ]] || [ ! -f "$path" ]; then
        echo "gate: UBS source must be an existing repository-relative file: ${path}" >&2
        exit 2
    fi
done
if [ "$PRINT_UBS_FILES" = 1 ]; then
    if [ "${#UBS_FILES[@]}" -gt 0 ]; then
        printf '%s\n' "${UBS_FILES[@]}"
    else
        echo "gate: no scanner-supported source files changed"
    fi
    exit 0
fi

SOURCE_DIFF="$(source_digest)" || exit 1
SOURCE_CONTENT="$(source_content)" || exit 1

# lib filter words: positional args after `--`, plus --filter pairs for the
# receipt's "every declared filter matched" check.
lib_filter_words=()
lib_filter_checks=()
read -r -a lib_filter_words <<< "$LIB_FILTER"
for fw in "${lib_filter_words[@]}"; do
    lib_filter_checks+=(--filter "$fw")
done
# The cass bin is `test = false`, so the unit tests of its own modules
# (logical_archive's backup/restore codec, the CLI modules) ran in no gate;
# one hung for hours unseen (m5cob). search_service's share runs as
# --test search_service. Every other skip names the bead whose fix
# removes it.
BIN_SKIPS=(
    search_service::
    logical_archive::import::publication_tests::publication_materializes_wal_rows_without_borrowing_replay_sidecars # svhcf
    logical_archive::export::ordering_tests::explicit_or_unattested_collations_keep_binary_order_and_verify # p94yt
)
bin_skip_args=()
for skip in "${BIN_SKIPS[@]}"; do
    bin_skip_args+=(--skip "$skip")
done

# Every stage the run will execute, in order. The receipt check below
# requires each one to report: a run cut short leaves later stages missing,
# and a missing stage is RED, never green.
EXPECTED_STAGES=(fmt clippy)
if [ "$RUN_LIB" = 1 ]; then
    EXPECTED_STAGES+=(lib-build lib-tests)
fi
if [ "$RUN_INTEGRATION" = 1 ]; then
    declare -A integration_targets=()
    for entry in "${INTEGRATION_TESTS[@]}"; do
        [ -n "$entry" ] || continue
        t="${entry%%:*}"
        if [[ ! "$t" =~ ^[a-zA-Z0-9_-]+$ ]]; then
            echo "gate: invalid integration target ${t}" >&2
            exit 2
        fi
        if [ -n "${integration_targets[$t]:-}" ]; then
            echo "gate: repeated target ${t}; combine its filters in one target:filter1 filter2 entry" >&2
            exit 2
        fi
        integration_targets["$t"]=1
        EXPECTED_STAGES+=("test-${t}")
    done
    if [ "$RUN_BIN" = 1 ]; then
        EXPECTED_STAGES+=(bin-build bin-cass)
    fi
fi
if [ "$RUN_GOLDENS" = 1 ]; then
    [ "$REGEN_GOLDENS" = 1 ] && EXPECTED_STAGES+=(goldens-regen)
    EXPECTED_STAGES+=(goldens)
fi
if [ "$RUN_DOCS_TRUTH" = 1 ]; then
    EXPECTED_STAGES+=(docs-build docs-binary-identity docs-truth)
    docs_binary="${CASS_CARGO_TARGET_DIR}/debug/cass"
fi
# Scan after behavioral validation: a slow scanner must not consume the whole
# run before any product test runs. UBS remains mandatory and blocking.
EXPECTED_STAGES+=(ubs job-complete)

# Every stage records its own exit code and preserves complete test output.
run_stages() {
    local entry t filter fw build_rc
    local -a int_filter_words int_filter_checks
    echo "SOURCE_HEAD=${SOURCE_HEAD}"
    run_fmt
    echo "STAGE=fmt EXIT=$?"
    run_cargo clippy --locked --all-targets -- -D warnings
    echo "STAGE=clippy EXIT=$?"
    if [ "$RUN_LIB" = 1 ]; then
        run_cargo test --locked --lib --no-run
        echo "STAGE=lib-build EXIT=$?"
        # `timeout` needs a real binary, so the bounded run calls cargo with
        # the already-exported resolved defaults instead of run_cargo.
        run_tests lib-tests "${lib_filter_checks[@]}" \
            run_with_timeout lib-tests "$LIB_TIMEOUT" \
            cargo test --locked --lib -- "${lib_filter_words[@]}"
    fi
    if [ "$RUN_INTEGRATION" = 1 ]; then
        for entry in "${INTEGRATION_TESTS[@]}"; do
            [ -n "$entry" ] || continue
            t="${entry%%:*}"
            filter=""
            if [ "$entry" != "$t" ]; then
                filter="${entry#*:}"
            fi
            int_filter_words=()
            int_filter_checks=()
            read -r -a int_filter_words <<< "$filter"
            for fw in "${int_filter_words[@]}"; do
                int_filter_checks+=(--filter "$fw")
            done
            run_tests "test-${t}" "${int_filter_checks[@]}" \
                run_cargo test --locked --test "$t" -- "${int_filter_words[@]}"
        done
        if [ "$RUN_BIN" = 1 ]; then
            run_cargo test --locked --bin cass --no-run
            echo "STAGE=bin-build EXIT=$?"
            # As for lib-tests, the timeout bounds the test run, not the compile.
            run_tests bin-cass \
                run_with_timeout bin-cass "$BIN_TIMEOUT" \
                cargo test --locked --bin cass -- "${bin_skip_args[@]}"
        fi
    fi
    if [ "$RUN_GOLDENS" = 1 ]; then
        if [ "$REGEN_GOLDENS" = 1 ]; then
            UPDATE_GOLDENS=1 run_tests goldens-regen \
                run_cargo test --locked --test golden_robot_json --test golden_robot_docs
        fi
        run_tests goldens \
            run_cargo test --locked --test golden_robot_json --test golden_robot_docs
    fi
    if [ "$RUN_DOCS_TRUTH" = 1 ]; then
        # Never inspect a pre-existing target binary when the selected tests
        # did not build it. This stage must establish its source and ELF first.
        run_cargo build --locked --bin cass
        build_rc=$?
        echo "STAGE=docs-build EXIT=${build_rc}"
        if [ "$build_rc" -eq 0 ]; then
            sha256 "$docs_binary"
            echo "STAGE=docs-binary-identity EXIT=$?"
            CASS_BIN="$docs_binary" scripts/validate_docs.sh --truth
            echo "STAGE=docs-truth EXIT=$?"
        fi
    fi
    run_ubs "${UBS_FILES[@]}"
    echo "STAGE=ubs EXIT=$?"
    echo "STAGE=job-complete EXIT=0"
}

# Match .cargo/config.toml: storage futures have exceeded a 16 MiB stack.
# This is virtual address reservation; pages are committed as needed.
export RUST_MIN_STACK=134217728
export UBS_MODULE_TIMEOUT="$UBS_TIMEOUT"

# The receipt survives a RED run for post-mortem (GATE_RECEIPT_FILE overrides).
receipt_file="${GATE_RECEIPT_FILE:-$(mktemp -t cass-gate.XXXXXX)}"
if [ -s "$receipt_file" ]; then
    echo "gate: refusing to overwrite existing receipt ${receipt_file}" >&2
    exit 2
fi
printf 'SOURCE_HEAD=%s SOURCE_DIFF_SHA256=%s SOURCE_CONTENT_SHA256=%s\n' \
    "$SOURCE_HEAD" "$SOURCE_DIFF" "$SOURCE_CONTENT" >> "$receipt_file"
attempt_file="$(mktemp "${receipt_file}.attempt.XXXXXX")"
echo "gate: local run started $(date +%T)" >&2
# Both streams through tee: a receipt that misses the STAGE lines reads as a
# truncated run.
run_stages 2>&1 | tee "$attempt_file"
pipeline_status=("${PIPESTATUS[@]}")
rc=${pipeline_status[0]}
[ "${pipeline_status[1]}" -eq 0 ] || rc=1
cat "$attempt_file" >> "$receipt_file"

echo "---- gate receipt $(date -u +%Y-%m-%dT%H:%M:%SZ) HEAD=${SOURCE_HEAD} DIFF_SHA256=${SOURCE_DIFF} CONTENT_SHA256=${SOURCE_CONTENT} runner_exit=${rc} receipt=${receipt_file} lines=$(wc -l < "$receipt_file") ----"
failed=0
verify_source_stability "$SOURCE_HEAD" "$SOURCE_DIFF" "$SOURCE_CONTENT" || failed=1
verify_receipt "$attempt_file" "$rc" "${EXPECTED_STAGES[@]}" || failed=1
if [ "$failed" -ne 0 ]; then
    echo "gate: RED (receipt kept at ${receipt_file})" >&2
    exit 1
fi
echo "gate: GREEN (receipt kept at ${receipt_file})"
