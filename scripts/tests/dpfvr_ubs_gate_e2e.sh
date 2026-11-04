#!/usr/bin/env bash
# dpfvr_ubs_gate_e2e.sh — exercise scripts/gate.sh's changed-file UBS logic.
#
# Per coding_agent_session_search-dpfvr. The gate runs locally (the project
# has no remote CI); this script feeds the gate's parsers recorded
# terminal-output shapes and perturbs a scratch checkout — inputs are
# fixtures, not substitute cargo/ubs executables or live proof.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# These cases exercise the production batched-gate parsers directly. Inputs are
# terminal-output fixtures, not substitute cargo/ubs executables or live proof.
if [ "${1:-}" = --batched-only ] || [ "${1:-}" = --formatter-only ]; then
    proof_dir="$(mktemp -d -t cass-gate-parser.XXXXXX)"
    gate="$PROJECT_ROOT/scripts/gate.sh"
    pass=0
    fail=0
    check_exit() {
        local label="$1" expected="$2" actual=0
        shift 2
        "$@" > "$proof_dir/$label.log" 2>&1 || actual=$?
        if [ "$actual" -eq "$expected" ]; then
            echo "PASS $label"
            pass=$((pass + 1))
        else
            echo "FAIL $label expected=$expected actual=$actual"
            cat "$proof_dir/$label.log"
            fail=$((fail + 1))
        fi
    }
    if [ "${1:-}" = --formatter-only ]; then
        # Use real cargo-fmt and rustfmt for both format outcomes. Then inject
        # process failures through cargo-fmt's RUSTFMT override: the waiting
        # shell must preserve a normal error, SIGABRT, and silent SIGKILL.
        # No project source is formatted in place and all fixtures are retained.
        ulimit -c 0
        export RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-$(rustup show active-toolchain | cut -d' ' -f1)}"
        export CASS_TEST_REAL_RUSTFMT="$(rustup which rustfmt)"
        mkdir -p "$proof_dir/crate/src"
        cat > "$proof_dir/crate/Cargo.toml" <<'TOML'
[package]
name = "cass-format-gate-probe"
version = "0.0.0"
edition = "2024"
[workspace]
TOML
        printf 'fn main() {}\n' > "$proof_dir/crate/src/main.rs"
        cd "$proof_dir/crate" || exit 1
        check_exit formatter-clean 0 env RUSTFMT="$CASS_TEST_REAL_RUSTFMT" bash "$gate" --verify-fmt
        printf 'fn main(){ }\n' > src/main.rs
        check_exit formatter-diff 1 env RUSTFMT="$CASS_TEST_REAL_RUSTFMT" bash "$gate" --verify-fmt
        cat > "$proof_dir/formatter-process-failure" <<'SH'
#!/usr/bin/env bash
if [ "${1:-}" = --version ]; then
    exec "${CASS_TEST_REAL_RUSTFMT:?}" "$@"
fi
case "${CASS_TEST_FORMATTER_FAILURE:?}" in
    error) exit 42 ;;
    abort) kill -ABRT "$$" ;;
    kill) kill -KILL "$$" ;;
    *) exit 99 ;;
esac
SH
        chmod +x "$proof_dir/formatter-process-failure"
        check_exit formatter-error 42 env RUSTFMT="$proof_dir/formatter-process-failure" CASS_TEST_FORMATTER_FAILURE=error bash "$gate" --verify-fmt
        check_exit formatter-abort 134 env RUSTFMT="$proof_dir/formatter-process-failure" CASS_TEST_FORMATTER_FAILURE=abort bash "$gate" --verify-fmt
        check_exit formatter-killed 137 env RUSTFMT="$proof_dir/formatter-process-failure" CASS_TEST_FORMATTER_FAILURE=kill bash "$gate" --verify-fmt
        echo "Formatter gate: PASS=$pass FAIL=$fail fixtures=$proof_dir"
        [ "$fail" -eq 0 ]
        exit $?
    fi
    # run_ubs reads the pin at scripts/ubs-version.txt relative to the working
    # directory, exactly as a real run does after cd'ing to the project root.
    cd "$PROJECT_ROOT" || exit 1
    receipt="$proof_dir/complete.txt"
    cat > "$receipt" <<'RECEIPT'
STAGE=fmt EXIT=0
STAGE=clippy EXIT=0
TEST_COUNT=lib-tests PASSED=4 BINARIES=1
STAGE=lib-tests EXIT=0
TEST_COUNT=test-e2e_lexical_fail_open PASSED=1 BINARIES=1
STAGE=test-e2e_lexical_fail_open EXIT=0
TEST_COUNT=goldens PASSED=20 BINARIES=2
STAGE=goldens EXIT=0
STAGE=ubs EXIT=0
STAGE=job-complete EXIT=0
RECEIPT
    expected=(fmt clippy lib-tests test-e2e_lexical_fail_open goldens ubs job-complete)
    check_exit digits-and-positive-counts 0 bash "$gate" --verify-receipt "$receipt" 0 "${expected[@]}"
    check_exit runner-failure 1 bash "$gate" --verify-receipt "$receipt" 103 "${expected[@]}"
    check_exit runner-timeout 1 bash "$gate" --verify-receipt "$receipt" 124 "${expected[@]}"
    check_exit runner-failure-reported 0 rg -x -F 'STAGE=runner EXIT=103 (runner did not complete normally)' "$proof_dir/runner-failure.log"
    for stage in fmt clippy lib-tests test-e2e_lexical_fail_open goldens ubs job-complete; do
        awk -v stage="$stage" '$0 != "STAGE=" stage " EXIT=0"' "$receipt" > "$proof_dir/missing-$stage.txt"
        check_exit "missing-$stage" 1 bash "$gate" --verify-receipt "$proof_dir/missing-$stage.txt" 0 "${expected[@]}"
        awk -v stage="$stage" '{if ($0 == "STAGE=" stage " EXIT=0") print "STAGE=" stage " EXIT=1"; else print}' \
            "$receipt" > "$proof_dir/failing-$stage.txt"
        check_exit "failing-$stage" 1 bash "$gate" --verify-receipt "$proof_dir/failing-$stage.txt" 0 "${expected[@]}"
    done
    # An incomplete log — no terminal job-complete — must read RED.
    sed '$d' "$receipt" > "$proof_dir/truncated.txt"
    check_exit incomplete-log 1 bash "$gate" --verify-receipt "$proof_dir/truncated.txt" 0 "${expected[@]}"
    awk '!/^TEST_COUNT=/' "$receipt" > "$proof_dir/no-counts.txt"
    check_exit no-test-counts 1 bash "$gate" --verify-receipt "$proof_dir/no-counts.txt" 0 "${expected[@]}"
    cat "$receipt" "$receipt" > "$proof_dir/duplicate.txt"
    check_exit duplicate-stage 1 bash "$gate" --verify-receipt "$proof_dir/duplicate.txt" 0 "${expected[@]}"
    { cat "$receipt"; echo 'STAGE=unexpected-after-completion EXIT=0'; } > "$proof_dir/not-terminal.txt"
    check_exit nonterminal-job-complete 1 bash "$gate" --verify-receipt "$proof_dir/not-terminal.txt" 0 "${expected[@]}"
    check_exit stale-docs-binary 1 bash "$gate" --verify-receipt "$receipt" 0 "${expected[@]}" docs-build docs-binary-identity docs-truth

    # Exercise the actual scanner-file scope builder with no gate run.
    # Ignored paths must be dropped before they are named to the scanner;
    # production and test sources must remain selected.
    scope_root="$proof_dir/scope"
    mkdir -p "$scope_root/scripts/lib" "$scope_root/src" "$scope_root/tests" \
        "$scope_root/.rch-tmp/cass-gate-ubs.example/modules"
    cp "$gate" "$scope_root/scripts/gate.sh"
    cp "$PROJECT_ROOT/scripts/lib/run_cargo.sh" "$scope_root/scripts/lib/run_cargo.sh"
    cp "$PROJECT_ROOT/scripts/ubs-version.txt" "$scope_root/scripts/ubs-version.txt"
    cp "$PROJECT_ROOT/.ubsignore" "$scope_root/.ubsignore"
    printf '%s\n' 'pub fn parse(raw: &str) -> u64 { raw.parse().unwrap() }' > "$scope_root/src/production.rs"
    printf '%s\n' '#[test] fn selected_test() { assert!(true); }' > "$scope_root/tests/selected.rs"
    printf '%s\n' 'raise RuntimeError("retained scanner fixture")' > "$scope_root/.rch-tmp/cass-gate-ubs.example/modules/scanner.py"
    git -C "$scope_root" init -q --initial-branch=main
    git -C "$scope_root" add scripts/gate.sh scripts/lib/run_cargo.sh scripts/ubs-version.txt .ubsignore
    git -C "$scope_root" -c user.name=gate-test -c user.email=gate-test@example.invalid \
        -c commit.gpgsign=false commit -q -m 'isolated gate scope baseline'
    check_exit ubs-scope-printed 0 env GATE_BASE=HEAD bash "$scope_root/scripts/gate.sh" --print-ubs-files
    check_exit production-unwrap-still-selected 0 rg -x -F 'src/production.rs' "$proof_dir/ubs-scope-printed.log"
    check_exit test-source-still-selected 0 rg -x -F 'tests/selected.rs' "$proof_dir/ubs-scope-printed.log"
    # check_exit combines stdout/stderr, so match a bare listed-path line
    # rather than the separate stderr note announcing the skipped path.
    check_exit ignored-scanner-not-selected 1 rg -x -F '.rch-tmp/cass-gate-ubs.example/modules/scanner.py' "$proof_dir/ubs-scope-printed.log"
    check_exit ignored-path-announced 0 rg -F 'gate: UBS skips .ubsignore path .rch-tmp/' "$proof_dir/ubs-scope-printed.log"
    check_exit repeated-integration-target 2 env GATE_BASE=HEAD bash "$scope_root/scripts/gate.sh" --integration 'e2e_lexical_fail_open:a,e2e_lexical_fail_open:b'

    # When every changed file is .ubsignore-covered, the surviving set is an
    # empty array: under `set -u` its "${arr[@]}" expansions (the ubs_kept
    # reassignment and the validation loop) must expand to zero words, not
    # abort. This path is exactly what bash < 4.4 could not run.
    empty_root="$proof_dir/scope-empty"
    mkdir -p "$empty_root/scripts/lib" "$empty_root/src"
    cp "$gate" "$empty_root/scripts/gate.sh"
    cp "$PROJECT_ROOT/scripts/lib/run_cargo.sh" "$empty_root/scripts/lib/run_cargo.sh"
    cp "$PROJECT_ROOT/scripts/ubs-version.txt" "$empty_root/scripts/ubs-version.txt"
    printf '%s\n' 'src/' > "$empty_root/.ubsignore"
    printf '%s\n' 'pub fn ignored() {}' > "$empty_root/src/ignored.rs"
    git -C "$empty_root" init -q --initial-branch=main
    git -C "$empty_root" add scripts/gate.sh scripts/lib/run_cargo.sh scripts/ubs-version.txt .ubsignore
    git -C "$empty_root" -c user.name=gate-test -c user.email=gate-test@example.invalid \
        -c commit.gpgsign=false commit -q -m 'isolated empty-scope baseline'
    check_exit empty-scope-clean-exit 0 env GATE_BASE=HEAD bash "$empty_root/scripts/gate.sh" --print-ubs-files
    check_exit empty-scope-announced 0 rg -x -F 'gate: no scanner-supported source files changed' "$proof_dir/empty-scope-clean-exit.log"

    # Removed remote-execution switches are hard errors, never silent no-ops.
    check_exit local-flag-refused 2 bash "$gate" --local
    check_exit local-flag-named 0 rg -F 'unknown argument: --local' "$proof_dir/local-flag-refused.log"
    check_exit print-remote-flag-refused 2 bash "$gate" --print-remote-script
    check_exit print-remote-flag-named 0 rg -F 'unknown argument: --print-remote-script' "$proof_dir/print-remote-flag-refused.log"
    for knob in GATE_RETRIES GATE_RETRY_SLEEP GATE_BUILD_JOBS GATE_TARGET_DIR; do
        check_exit "removed-knob-$knob" 2 env "$knob=2" bash "$gate" --no-lib
        check_exit "removed-knob-$knob-named" 0 rg -F "unknown argument: $knob" "$proof_dir/removed-knob-$knob.log"
    done

    # Source stability: capture the run-start attestation, then verify it
    # stays green only while the checkout is truly unchanged.
    state_out="$proof_dir/source-state.txt"
    (cd "$scope_root" && bash scripts/gate.sh --print-source-state) > "$state_out" 2>"$proof_dir/source-state.err"
    state_head="$(sed -n 's/^SOURCE_HEAD=//p' "$state_out")"
    state_diff="$(sed -n 's/^SOURCE_DIFF_SHA256=//p' "$state_out")"
    state_content="$(sed -n 's/^SOURCE_CONTENT_SHA256=//p' "$state_out")"
    check_exit source-state-captured 0 test -n "$state_head"
    check_exit source-diff-captured 0 test -n "$state_diff"
    check_exit source-content-captured 0 test -n "$state_content"
    check_exit unchanged-tree-stable 0 bash -c 'cd "$1" && shift && exec bash scripts/gate.sh --verify-source-stability "$@"' \
        _ "$scope_root" "$state_head" "$state_diff" "$state_content"
    check_exit unchanged-tree-reports-stage 0 rg -x -F 'STAGE=source-stability EXIT=0' "$proof_dir/unchanged-tree-stable.log"

    # A PATH without sha256sum still verifies sources via `shasum -a 256`.
    if command -v shasum >/dev/null 2>&1; then
        no_sha_bin="$proof_dir/bin-no-sha256sum"
        mkdir -p "$no_sha_bin"
        for tool in bash git sort xargs shasum cut tr dirname pwd basename \
            cat ls sed awk grep wc date env uname mktemp head tail; do
            tool_path="$(command -v "$tool" || true)"
            [ -n "$tool_path" ] && ln -sf "$tool_path" "$no_sha_bin/$tool"
        done
        check_exit stub-path-lacks-sha256sum 1 env PATH="$no_sha_bin" bash -c 'command -v sha256sum'
        check_exit stability-under-shasum-fallback 0 env PATH="$no_sha_bin" bash -c \
            'cd "$1" && shift && exec bash scripts/gate.sh --verify-source-stability "$@"' \
            _ "$scope_root" "$state_head" "$state_diff" "$state_content"
    else
        echo "SKIP shasum-fallback cases (no shasum on this host)"
    fi

    # A PATH without timeout/gtimeout makes run_with_timeout fail the stage
    # instead of running it unbounded.
    no_timeout_bin="$proof_dir/bin-no-timeout"
    mkdir -p "$no_timeout_bin"
    for tool in bash git sort xargs sha256sum shasum cut tr dirname pwd \
        basename cat ls sed awk grep wc date env uname mktemp sleep; do
        tool_path="$(command -v "$tool" || true)"
        [ -n "$tool_path" ] && ln -sf "$tool_path" "$no_timeout_bin/$tool"
    done
    check_exit stub-path-lacks-timeout 1 env PATH="$no_timeout_bin" bash -c 'command -v timeout || command -v gtimeout'
    check_exit timeout-missing-fails-stage 127 env PATH="$no_timeout_bin" bash -c \
        'source "$1" && run_with_timeout probe-stage 5 true' _ "$PROJECT_ROOT/scripts/lib/run_cargo.sh"
    check_exit timeout-missing-diagnostic 0 rg -F 'STAGE=probe-stage EXIT=127' "$proof_dir/timeout-missing-fails-stage.log"
    check_exit timeout-passthrough 0 bash -c 'source "$1" && run_with_timeout probe-stage 5 true' _ "$PROJECT_ROOT/scripts/lib/run_cargo.sh"
    check_exit timeout-expiry 124 bash -c 'source "$1" && run_with_timeout probe-stage 1 sleep 5' _ "$PROJECT_ROOT/scripts/lib/run_cargo.sh"

    # A bash older than 4.4 must be refused up front with a clear diagnostic,
    # not a mid-run 'arr[@]: unbound variable' abort — the gate expands
    # possibly-empty arrays throughout under set -u. Runs only on hosts whose
    # stock /bin/bash predates 4.4 (e.g. macOS's 3.2.57).
    if /bin/bash -c 'exit $(( BASH_VERSINFO[0] < 4 || (BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] < 4) ? 0 : 1 ))' 2>/dev/null; then
        check_exit unsupported-bash-refused 2 /bin/bash "$gate" --help
        check_exit unsupported-bash-named 0 rg -F 'gate: bash >= 4.4 required' "$proof_dir/unsupported-bash-refused.log"
    else
        echo "SKIP unsupported-bash cases (/bin/bash is >= 4.4)"
    fi

    # Editing a tracked input mid-run must fail source-stability.
    printf '%s\n' '# mid-run probe edit' >> "$scope_root/scripts/gate.sh"
    check_exit modified-input-refused 1 bash -c 'cd "$1" && shift && exec bash scripts/gate.sh --verify-source-stability "$@"' \
        _ "$scope_root" "$state_head" "$state_diff" "$state_content"
    check_exit modified-input-reports-stage 0 rg -x -F 'STAGE=source-stability EXIT=1' "$proof_dir/modified-input-refused.log"
    check_exit modified-input-reason 0 rg -F 'working-tree diff changed' "$proof_dir/modified-input-refused.log"
    git -C "$scope_root" checkout -q scripts/gate.sh

    # A new untracked file mid-run must fail source-stability too.
    printf '%s\n' 'pub fn late() {}' > "$scope_root/tests/late_probe.rs"
    check_exit untracked-input-refused 1 bash -c 'cd "$1" && shift && exec bash scripts/gate.sh --verify-source-stability "$@"' \
        _ "$scope_root" "$state_head" "$state_diff" "$state_content"

    # A commit mid-run moves HEAD.
    printf '%s\n' 'pub fn committed() {}' > "$scope_root/src/committed.rs"
    git -C "$scope_root" add src/committed.rs
    git -C "$scope_root" -c user.name=gate-test -c user.email=gate-test@example.invalid \
        -c commit.gpgsign=false commit -q -m 'mid-run head move'
    check_exit moved-head-refused 1 bash -c 'cd "$1" && shift && exec bash scripts/gate.sh --verify-source-stability "$@"' \
        _ "$scope_root" "$state_head" "$state_diff" "$state_content"
    check_exit moved-head-reason 0 rg -F 'HEAD moved' "$proof_dir/moved-head-refused.log"

    # UBS is mandatory and pinned; missing or wrong bytes must fail the stage.
    # "$BASH" pins the interpreter: the stubbed PATH must hide ubs without
    # also downgrading the shell below the gate's bash >= 4.4 floor (a bare
    # `bash` in that PATH resolves to the stock /bin/bash, e.g. 3.2 on macOS).
    check_exit ubs-missing-refused 1 env -u UBS_BIN -u UBS_MODULE_DIR PATH=/usr/bin:/bin \
        "$BASH" "$gate" --verify-ubs
    check_exit ubs-missing-reports-stage 0 rg -x -F 'STAGE=ubs EXIT=1' "$proof_dir/ubs-missing-refused.log"
    check_exit ubs-missing-install-hint 0 rg -F \
        'git clone --depth 1 --branch v5.4.4 https://github.com/Dicklesworthstone/ultimate_bug_scanner' \
        "$proof_dir/ubs-missing-refused.log"
    check_exit ubs-wrong-runner-digest 1 env -u UBS_MODULE_DIR UBS_BIN=/bin/ls bash "$gate" --verify-ubs
    check_exit ubs-wrong-runner-hint 0 rg -F \
        'git clone --depth 1 --branch v5.4.4 https://github.com/Dicklesworthstone/ultimate_bug_scanner' \
        "$proof_dir/ubs-wrong-runner-digest.log"
    test_ubs="${UBS_BIN:-$(command -v ubs || true)}"
    if [ -n "$test_ubs" ]; then
        check_exit ubs-missing-module-dir 1 env -u UBS_MODULE_DIR UBS_BIN="$test_ubs" bash "$gate" --verify-ubs
        mkdir -p "$proof_dir/fake-modules"
        printf '{}\n' > "$proof_dir/fake-modules/contract.json"
        check_exit ubs-wrong-contract-digest 1 env UBS_BIN="$test_ubs" \
            UBS_MODULE_DIR="$proof_dir/fake-modules" bash "$gate" --verify-ubs
        if [ -n "${UBS_MODULE_DIR:-}" ]; then
            printf 'fn main() {}\n' > "$proof_dir/probe.rs"
            check_exit ubs-pinned-install-accepted 0 env UBS_BIN="$test_ubs" \
                UBS_MODULE_DIR="$UBS_MODULE_DIR" bash "$gate" --verify-ubs "$proof_dir/probe.rs"
            check_exit ubs-stage-reported 0 rg -x -F 'STAGE=ubs EXIT=0' "$proof_dir/ubs-pinned-install-accepted.log"
        fi
    else
        echo "SKIP live UBS cases (no ubs on PATH and UBS_BIN unset)"
    fi

    printf 'running 3 tests\ntest result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out\n' > "$proof_dir/tests-positive.txt"
    printf 'running 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 9 filtered out\n' > "$proof_dir/tests-empty.txt"
    printf 'running 3 tests\n' > "$proof_dir/tests-truncated.txt"
    cat "$proof_dir/tests-positive.txt" "$proof_dir/tests-empty.txt" > "$proof_dir/tests-mixed.txt"
    cat "$proof_dir/tests-positive.txt" "$proof_dir/tests-positive.txt" > "$proof_dir/tests-two-binaries.txt"
    check_exit positive-tests 0 bash "$gate" --verify-test-log "$proof_dir/tests-positive.txt"
    check_exit two-positive-binaries 0 bash "$gate" --verify-test-log "$proof_dir/tests-two-binaries.txt"
    check_exit zero-selected-tests 1 bash "$gate" --verify-test-log "$proof_dir/tests-empty.txt"
    check_exit truncated-tests 1 bash "$gate" --verify-test-log "$proof_dir/tests-truncated.txt"
    check_exit positive-and-empty-binary 1 bash "$gate" --verify-test-log "$proof_dir/tests-mixed.txt"
    cat "$proof_dir/tests-positive.txt" "$proof_dir/tests-truncated.txt" > "$proof_dir/tests-tail-truncated.txt"
    check_exit positive-and-truncated-binary 1 bash "$gate" --verify-test-log "$proof_dir/tests-tail-truncated.txt"
    printf 'test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out\n' > "$proof_dir/tests-missing-start.txt"
    check_exit missing-test-start 1 bash "$gate" --verify-test-log "$proof_dir/tests-missing-start.txt"
    { cat "$proof_dir/tests-positive.txt"; echo 'test result: FAILED. 1 passed; 1 failed;'; } > "$proof_dir/tests-failed.txt"
    check_exit positive-and-failed-binary 1 bash "$gate" --verify-test-log "$proof_dir/tests-failed.txt"

    # A real lib test self-spawns with inherited output: its one-test summary
    # appears between the outer run start and its final 45-test summary.
    # Accept balanced nesting without inflating the top-level assertion count.
    cat > "$proof_dir/tests-child.txt" <<'TESTS'
running 1 test
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 7073 filtered out
TESTS
    {
        echo 'running 45 tests'
        cat "$proof_dir/tests-child.txt"
        echo 'test result: ok. 45 passed; 0 failed; 0 ignored; 0 measured; 7029 filtered out'
    } > "$proof_dir/tests-nested.txt"
    check_exit nested-child-tests 0 bash "$gate" --verify-test-log "$proof_dir/tests-nested.txt"
    check_exit nested-child-counted-once 0 rg -x -F 'PASSED=45 BINARIES=1' "$proof_dir/nested-child-tests.log"
    {
        echo 'running 2 tests'
        cat "$proof_dir/tests-nested.txt"
        echo 'test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out'
        cat "$proof_dir/tests-positive.txt"
    } > "$proof_dir/tests-deep-nested.txt"
    check_exit deep-nesting-and-next-binary 0 bash "$gate" --verify-test-log "$proof_dir/tests-deep-nested.txt"
    check_exit deep-nesting-counted-once 0 rg -x -F 'PASSED=5 BINARIES=2' "$proof_dir/deep-nesting-and-next-binary.log"
    { echo 'running 45 tests'; cat "$proof_dir/tests-child.txt"; } > "$proof_dir/tests-parent-truncated.txt"
    check_exit completed-child-missing-parent 1 bash "$gate" --verify-test-log "$proof_dir/tests-parent-truncated.txt"
    {
        echo 'running 45 tests'
        echo 'running 1 test'
        echo 'test result: ok. 45 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out'
    } > "$proof_dir/tests-child-truncated.txt"
    check_exit parent-cannot-close-missing-child 1 bash "$gate" --verify-test-log "$proof_dir/tests-child-truncated.txt"
    {
        echo 'running 3 tests'
        cat "$proof_dir/tests-empty.txt"
        echo 'test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out'
    } > "$proof_dir/tests-child-empty.txt"
    check_exit nested-empty-child 1 bash "$gate" --verify-test-log "$proof_dir/tests-child-empty.txt"
    {
        echo 'running 3 tests'
        echo 'running 1 test'
        echo 'test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out'
        echo 'test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out'
    } > "$proof_dir/tests-child-failed.txt"
    check_exit nested-failed-child 1 bash "$gate" --verify-test-log "$proof_dir/tests-child-failed.txt"
    printf 'running 3 tests\ntest result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n' > "$proof_dir/tests-count-mismatch.txt"
    check_exit mismatched-test-count 1 bash "$gate" --verify-test-log "$proof_dir/tests-count-mismatch.txt"
    printf 'running 3 tests\ntest result: ok. 1 passed; 0 failed; 1 ignored; 1 measured; 0 filtered out\n' > "$proof_dir/tests-accounted.txt"
    check_exit ignored-and-measured-accounted 0 bash "$gate" --verify-test-log "$proof_dir/tests-accounted.txt"
    printf 'running 3 tests\ntest result: ok. 2 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out\n' > "$proof_dir/tests-false-ok.txt"
    check_exit nonzero-failures-in-ok-summary 1 bash "$gate" --verify-test-log "$proof_dir/tests-false-ok.txt"
    { cat "$proof_dir/tests-positive.txt"; echo 'test result: ok. 1 passed;'; } > "$proof_dir/tests-malformed-summary.txt"
    check_exit malformed-terminal-summary 1 bash "$gate" --verify-test-log "$proof_dir/tests-malformed-summary.txt"
    cat > "$proof_dir/tests-named.txt" <<'TESTS'
running 2 tests
test indexer::tests::gh494_mid_run_resume ... ok
test storage::sqlite::tests::fts_shadow_recreation - should panic ... ok
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 7000 filtered out
TESTS
    check_exit all-declared-filters-matched 0 bash "$gate" --verify-test-log "$proof_dir/tests-named.txt" indexer::tests::gh494 fts_shadow_recreation
    check_exit mixed-filter-with-typo-rejected 1 bash "$gate" --verify-test-log "$proof_dir/tests-named.txt" indexer::tests::gh494 indexer::gh494_tests
    check_exit mixed-filter-typo-named 0 rg -F 'gate: test filter matched 0 passing tests: indexer::gh494_tests' "$proof_dir/mixed-filter-with-typo-rejected.log"
    {
        echo 'running 1 test'
        echo 'running 1 test'
        echo 'test child::only_match ... ok'
        echo 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out'
        echo 'test parent::real_match ... ok'
        echo 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out'
    } > "$proof_dir/tests-nested-filter.txt"
    check_exit child-only-filter-match-rejected 1 bash "$gate" --verify-test-log "$proof_dir/tests-nested-filter.txt" parent::real_match child::only_match

    echo "Batched gate parser: PASS=$pass FAIL=$fail fixtures=$proof_dir"
    [ "$fail" -eq 0 ]
    exit $?
fi

DPFVR_TMP="${DPFVR_TMP:-/tmp/cass-dpfvr}"
LOG="$DPFVR_TMP/dpfvr-e2e.log"
mkdir -p "$DPFVR_TMP"
exec > >(tee -a "$LOG") 2>&1

cleanup() {
    local rc=$?
    if [ "$rc" -ne 0 ]; then
        echo ""
        echo "[dpfvr_e2e] FAILURE — last 50 log lines:" >&2
        tail -n 50 "$LOG" | sed 's/^/[dpfvr_e2e]   /' >&2
    fi
    exit "$rc"
}
trap cleanup EXIT

# Ensure ubs is reachable (UBS_BIN or PATH); otherwise mark live checks skipped.
UBS_RUNNER="${UBS_BIN:-$(command -v ubs || true)}"
if [ -z "$UBS_RUNNER" ]; then
    echo "[dpfvr_e2e] WARN: ubs not found (UBS_BIN unset, not on PATH) — skipping live invocation tests."
    echo "[dpfvr_e2e] install: git clone --depth 1 --branch v5.4.4 https://github.com/Dicklesworthstone/ultimate_bug_scanner <dir>"
    UBS_AVAILABLE=0
else
    UBS_AVAILABLE=1
    echo "[dpfvr_e2e] ubs binary: $UBS_RUNNER"
    bash "$UBS_RUNNER" --version 2>&1 || true
fi

PASS=0
FAIL=0
expect_eq() {
    local description="$1" actual="$2" want="$3"
    if [ "$actual" = "$want" ]; then
        echo "[dpfvr_e2e] OK: $description (got=$actual)"
        PASS=$((PASS + 1))
    else
        echo "[dpfvr_e2e] FAIL: $description (want=$want got=$actual)"
        FAIL=$((FAIL + 1))
    fi
}

# Scenario 1: the gate's own scanner scope listing must name only real files.
listed=0
bad=0
scope_listing="$(bash "$PROJECT_ROOT/scripts/gate.sh" --print-ubs-files 2>/dev/null)" && list_rc=0 || list_rc=$?
expect_eq "scenario=print_ubs_files runs" "$list_rc" "0"
while IFS= read -r path; do
    case "$path" in "" | gate:*) continue ;; esac
    listed=$((listed + 1))
    [ -f "$PROJECT_ROOT/$path" ] || { echo "[dpfvr_e2e] nonexistent scope entry: $path"; bad=$((bad + 1)); }
done <<< "$scope_listing"
expect_eq "scenario=print_ubs_files lists only existing files" "$bad" "0"
echo "[dpfvr_e2e] scope files listed: $listed"

# Scenario 2: ubs available — run a quick happy-path invocation.
if [ "$UBS_AVAILABLE" -eq 1 ]; then
    if UBS_MODULE_DIR="${UBS_MODULE_DIR:-}" bash "$PROJECT_ROOT/scripts/gate.sh" --verify-ubs \
        "$(realpath "${BASH_SOURCE[0]}")" >/dev/null 2>&1; then
        echo "[dpfvr_e2e] OK: scenario=ubs_clean_file ran without failures"
        PASS=$((PASS + 1))
    else
        echo "[dpfvr_e2e] WARN: scenario=ubs_clean_file ubs reported issues (may be expected)"
    fi
fi

echo ""
echo "[dpfvr_e2e] SUMMARY: PASS=$PASS FAIL=$FAIL"
echo "[dpfvr_e2e] log written to: $LOG"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
echo "[dpfvr_e2e] ALL PASS"
