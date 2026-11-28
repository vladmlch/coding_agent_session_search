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
    CASS_GATE_RUSTFMT="$formatter" RUSTFMT="$wrapper" cargo fmt --all --check
    rc=$?
    return "$rc"
}

# The complete set of paths any stage can read: tracked files plus untracked
# non-ignored files under the input roots. Nothing is transferred anywhere,
# so every byte the compilers and tests see is verified — no exclusions.
source_paths() {
    git ls-files --cached --others --exclude-standard -z -- \
        Cargo.toml Cargo.lock rust-toolchain.toml build.rs build_support \
        crates fuzz/Cargo.toml fuzz/Cargo.lock src tests benches scripts .cargo .github README.md | sort -z
}

# Combined SHA-256 of every input file's own digest line.
source_content() {
    source_paths | xargs -0 bash -c 'sha256 "$@"' _ | sha256 | cut -d' ' -f1
}

run_loc() {
    python3 - <<'PY'
from pathlib import Path
import sys

roots = [Path("src"), *sorted(Path("crates").glob("*/src"))]
violations = []
count = 0
for root in roots:
    if not root.is_dir():
        continue
    for path in sorted(root.rglob("*.rs")):
        count += 1
        lines = len(path.read_text(encoding="utf-8").splitlines())
        if lines > 2000:
            violations.append((path, lines))
print(f"LOC_SCAN_FILES={count} LOC_MAX=2000 LOC_VIOLATIONS={len(violations)}")
for path, lines in violations:
    print(f"gate: LOC violation {path}: {lines} > 2000", file=sys.stderr)
sys.exit(1 if violations else 0)
PY
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
# on the runner, its module routing contract, and the Rust language module.
# The gate never downloads it — a missing or mismatched install fails the
# stage and prints the git-clone install hint.
verify_ubs_install() {
    local expected actual scanner module_dir expected_sha expected_contract_sha expected_rust_mod_sha
    expected="$(tr -d '[:space:]' < scripts/ubs-version.txt)"
    # The runner verifies its language modules against embedded release hashes.
    # Updating the pin also requires reviewing the new runner digest here.
    if [ "$expected" != v5.5.27 ]; then
        echo "gate: no reviewed UBS runner digest for ${expected}" >&2
        return 1
    fi
    expected_sha=559d0ad9414ea94f4cb53e8499c19db4a4a20d8647f1ce8614347b79e26b510c
    expected_contract_sha=90b82afcc916b8f4608f1561be59fc874228028c40e957b42fba2791991ba970
    expected_rust_mod_sha=a42d9509cac907fd5ccbc805575641fa4a5775c814c694a827f42fd5710ddd96
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
    if [ ! -f "$module_dir/ubs-rust.sh" ] || [ "$(sha256 "$module_dir/ubs-rust.sh" | cut -d' ' -f1)" != "$expected_rust_mod_sha" ]; then
        echo "gate: UBS Rust module bytes differ from the pinned release (${module_dir}/ubs-rust.sh)" >&2
        echo "gate: install: git clone --depth 1 --branch ${expected} https://github.com/Dicklesworthstone/ultimate_bug_scanner <dir>" >&2
        return 1
    fi
    actual="$(bash "$scanner" --version)" || return 1
    echo "UBS_VERSION=${actual} UBS_PIN=${expected} UBS_SHA256=${expected_sha}"
    echo "UBS_MODULE_DIR=${module_dir}"
    echo "UBS_CONTRACT_SHA256=${expected_contract_sha}"
    echo "UBS_RUST_MODULE_SHA256=${expected_rust_mod_sha}"
    UBS_VERIFIED_SCANNER="$scanner"
    UBS_VERIFIED_MODULE_DIR="$module_dir"
}

run_ubs() {
    verify_ubs_install || return 1
    if [ "$#" -eq 0 ]; then
        echo "UBS_FILES=0 (no changed scanner-supported source files)"
        return 0
    fi
    printf 'UBS_FILE=%s\n' "$@"
    # Text retains categories and source samples needed to diagnose a red gate.
    bash "$UBS_VERIFIED_SCANNER" --module-dir="$UBS_VERIFIED_MODULE_DIR" --no-auto-update --format=text --ci --fail-on-warning "$@"
}

# Filter NUL-separated repository-relative paths through .ubsignore into a
# caller-provided array variable name (bash >= 4.4 nameref).
filter_ubsignore_into() {
    local out_name="$1"
    shift
    local -n out_ref="$out_name"
    out_ref=()
    [ "$#" -gt 0 ] || return 0
    if [ -f .ubsignore ]; then
        local -A ubs_ignored=()
        local path
        while IFS= read -r -d '' path; do
            ubs_ignored["$path"]=1
        done < <(git ls-files -z -c -o -i --exclude-from=.ubsignore -- "$@")
        for path in "$@"; do
            if [ -n "${ubs_ignored[${path#./}]:-}" ]; then
                echo "gate: UBS skips .ubsignore path ${path}" >&2
            else
                out_ref+=("$path")
            fi
        done
    else
        out_ref=("$@")
    fi
}

# Evaluate production UBS JSONL ledger per D1:
# rust.perf.string-alloc-in-loop and rust.collections.clone-in-loop are moved
# to the informational report; any Critical/Warning in other rules (or an
# incomplete/failed scan) fails the blocking stage.
ubs_eval_prod_jsonl() {
    local raw_rc="$1" jsonl_path="$2" info_report="$3" prod_count="$4"
    python3 - "$raw_rc" "$jsonl_path" "$info_report" "$prod_count" <<'PY'
import json
import sys

raw_rc = int(sys.argv[1])
jsonl_path = sys.argv[2]
info_report = sys.argv[3]
prod_count = int(sys.argv[4])
excluded_rules = {
    "rust.perf.string-alloc-in-loop",
    "rust.collections.clone-in-loop",
}

if raw_rc not in (0, 1):
    print(f"gate: production UBS exited with error status {raw_rc}", file=sys.stderr)
    sys.exit(1)

try:
    with open(jsonl_path, encoding="utf-8") as fh:
        records = [json.loads(line) for line in fh if line.strip()]
except (OSError, json.JSONDecodeError) as exc:
    print(f"gate: failed to read production UBS JSONL {jsonl_path}: {exc}", file=sys.stderr)
    sys.exit(1)

totals = [r for r in records if r.get("type") == "totals"]
if len(totals) != 1:
    print(f"gate: expected 1 UBS totals record in {jsonl_path}, got {len(totals)}", file=sys.stderr)
    sys.exit(1)
tot = totals[0]
if tot.get("status") != "ok" or tot.get("failed_modules"):
    print(f"gate: production UBS scan incomplete/failed: status={tot.get('status')} failed={tot.get('failed_modules')}", file=sys.stderr)
    sys.exit(1)
occ_export = tot.get("occurrence_export") or {}
if not occ_export.get("analysis_complete", False):
    print("gate: production UBS occurrence_export.analysis_complete is false", file=sys.stderr)
    sys.exit(1)

rule_summaries = [r for r in records if r.get("type") == "rule_summary"]
rule_crit_warn = sum(
    int(r.get("reported", 0))
    for r in rule_summaries
    if r.get("severity") in ("critical", "warning")
)
tot_crit_warn = int(tot.get("critical", 0)) + int(tot.get("warning", 0))
if rule_crit_warn != tot_crit_warn:
    print(
        f"gate: UBS rule_summary total ({rule_crit_warn}) != totals ({tot_crit_warn})",
        file=sys.stderr,
    )
    sys.exit(1)

blocking = []
excluded_summaries = []
for r in rule_summaries:
    sev = r.get("severity")
    rep = int(r.get("reported", 0))
    rid = r.get("rule_id", "")
    if sev not in ("critical", "warning") or rep <= 0:
        continue
    if rid in excluded_rules:
        excluded_summaries.append(r)
    else:
        blocking.append(r)

excluded_findings = [
    r for r in records
    if r.get("type") == "finding"
    and r.get("rule_id") in excluded_rules
    and not r.get("suppressed", False)
]

with open(info_report, "w", encoding="utf-8") as out:
    out.write("=== UBS INFORMATIONAL: PRODUCTION LOOP RULES (D1) ===\n")
    for r in excluded_summaries:
        out.write(f"RULE_SUMMARY {r.get('rule_id')} severity={r.get('severity')} reported={r.get('reported')}\n")
    for f in excluded_findings:
        out.write(f"{f.get('file')}:{f.get('line')}: [{f.get('severity')}] {f.get('rule_id')} — {f.get('message')}\n")
    out.write("\n=== UBS INFORMATIONAL: TESTS & BENCHES ===\n")

block_crit = sum(int(r.get("reported", 0)) for r in blocking if r.get("severity") == "critical")
block_warn = sum(int(r.get("reported", 0)) for r in blocking if r.get("severity") == "warning")
info_loop_warn = sum(int(r.get("reported", 0)) for r in excluded_summaries)
print(
    f"UBS_PROD_FILES={prod_count} UBS_PROD_BLOCKING_CRITICAL={block_crit} "
    f"UBS_PROD_BLOCKING_WARNING={block_warn} UBS_PROD_INFO_LOOP_WARNINGS={info_loop_warn}"
)
if blocking:
    for r in blocking:
        print(
            f"gate: UBS blocking finding rule={r.get('rule_id')} severity={r.get('severity')} count={r.get('reported')}",
            file=sys.stderr,
        )
    sys.exit(1)
sys.exit(0)
PY
}


collect_ubs_default_filesets() {
    local prod_out="$1" info_out="$2"
    local -n prod_ref="$prod_out"
    local -n info_ref="$info_out"
    local path
    local -a raw_src=() raw_prod=() raw_info=()
    local -A pure_test_src=(
        ["src/connectors/codex/source_budget/coverage_tests.rs"]=1
        ["src/connectors/codex/source_budget/empty_source/tests.rs"]=1
        ["src/connectors/codex/source_budget/limit_tests.rs"]=1
        ["src/connectors/codex/source_budget/recovery/contract_tests.rs"]=1
        ["src/connectors/codex/source_budget/recovery/resume_tests.rs"]=1
        ["src/connectors/codex/source_budget/recovery/tests.rs"]=1
        ["src/connectors/codex/source_budget/tests.rs"]=1
        ["src/indexer/gh473_tests.rs"]=1
        ["src/indexer/gh494_tests.rs"]=1
        ["src/indexer/lexical_generation/cleanup_tests.rs"]=1
        ["src/indexer/semantic/shard_publication_tests.rs"]=1
        ["src/raw_mirror/policy_tests.rs"]=1
        ["src/raw_mirror/retention_tests.rs"]=1
        ["src/cli_root/gh494_repair_tests.rs"]=1
        ["src/search_service/mcp_refinement_evidence_tests.rs"]=1
    )
    prod_ref=()
    info_ref=()
    while IFS= read -r -d '' path; do
        [ -f "$path" ] && raw_src+=("$path")
    done < <(git ls-files -z -- 'src/*.rs' 'src/**/*.rs' 'crates/*/src/*.rs' 'crates/*/src/**/*.rs' | sort -zu)
    for path in "${raw_src[@]}"; do
        if [[ "$path" == src/tests/* || "$path" == */tests/* ]] || [ -n "${pure_test_src[$path]:-}" ]; then
            raw_info+=("$path")
        else
            raw_prod+=("$path")
        fi
    done
    while IFS= read -r -d '' path; do
        [ -f "$path" ] && raw_info+=("$path")
    done < <(git ls-files -z -- 'tests/*.rs' 'tests/**/*.rs' 'benches/*.rs' 'benches/**/*.rs' 'crates/*/tests/*.rs' 'crates/*/tests/**/*.rs' | sort -zu)
    filter_ubsignore_into prod_ref "${raw_prod[@]}"
    filter_ubsignore_into info_ref "${raw_info[@]}"
}

run_ubs_default_lanes() {
    verify_ubs_install || return 1
    local -a prod_ubs_files=() info_ubs_files=()
    collect_ubs_default_filesets prod_ubs_files info_ubs_files

    local prod_jsonl="${receipt_file}.ubs-prod.jsonl"
    local info_report="${receipt_file}.ubs-tests.log"
    local raw_prod_rc=0 prod_rc=0 ubs_info_rc=0
    if [ "${#prod_ubs_files[@]}" -eq 0 ]; then
        echo "UBS_PROD_FILES=0 UBS_PROD_BLOCKING_CRITICAL=0 UBS_PROD_BLOCKING_WARNING=0 UBS_PROD_INFO_LOOP_WARNINGS=0"
        : > "$info_report"
    else
        bash "$UBS_VERIFIED_SCANNER" --module-dir="$UBS_VERIFIED_MODULE_DIR" \
            --no-auto-update --only=rust --no-cargo --exclude-tests \
            --jobs="${CARGO_BUILD_JOBS:-8}" --format=text \
            --also="jsonl:${prod_jsonl}" --all-occurrences --max-occurrences=100000 \
            --ci --fail-on-warning "${prod_ubs_files[@]}" || raw_prod_rc=$?
        ubs_eval_prod_jsonl "$raw_prod_rc" "$prod_jsonl" "$info_report" "${#prod_ubs_files[@]}" || prod_rc=$?
    fi
    if [ "${#info_ubs_files[@]}" -gt 0 ]; then
        bash "$UBS_VERIFIED_SCANNER" --module-dir="$UBS_VERIFIED_MODULE_DIR" \
            --no-auto-update --only=rust --no-cargo \
            --jobs="${CARGO_BUILD_JOBS:-8}" --format=text \
            --ci "${info_ubs_files[@]}" >> "$info_report" 2>&1 || ubs_info_rc=$?
    fi
    echo "UBS_INFO_REPORT=${info_report} UBS_INFO_EXIT=${ubs_info_rc}"
    return "$prod_rc"
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
if [ "${1:-}" = --verify-loc ]; then
    run_loc
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
if [ "${1:-}" = --print-ubs-prod-files ]; then
    print_prod_files=()
    print_info_files=()
    collect_ubs_default_filesets print_prod_files print_info_files
    printf '%s
' "${print_prod_files[@]}"
    exit 0
fi
if [ "${1:-}" = --print-ubs-info-files ]; then
    print_prod_files=()
    print_info_files=()
    collect_ubs_default_filesets print_prod_files print_info_files
    printf '%s
' "${print_info_files[@]}"
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
# Exercise the default production/informational lane selection itself. The
# shell regression suite uses isolated staged fixtures to prove member crates
# are inside the blocking lane; no cargo stage runs here.
if [ "${1:-}" = --verify-ubs-default ]; then
    receipt_file="$(mktemp -t cass-gate-ubs-default.XXXXXX)"
    run_ubs_default_lanes
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
UBS_FILES_EXPLICIT=0
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
# local_only_audit is the Task 17 final audit: remote-client/remote-spawn/
# non-loopback-bind source scan, pages-assets + export-template resource
# scan, docs/scripts remote-instruction scan, Cargo manifest+lockfile
# banned-crate scan, and the removed-command matrix liveness check.
# cli_dispatch_coverage holds the pending:false removed-command rejection
# probes the audit cross-checks; outside the gate a regrowing matrix ran
# in no receipt.
INTEGRATION_TESTS=(cli_robot bookmarks_cli search_metamorphic pages_bundle search_service local_only_audit cli_dispatch_coverage)
while [ $# -gt 0 ]; do
    case "$1" in
        --lib-filter) LIB_FILTER="$2"; shift 2 ;;
        --no-lib) RUN_LIB=0; shift ;;
        --ubs-files) IFS=',' read -r -a UBS_FILES <<<"$2"; UBS_FILES_EXPLICIT=1; shift 2 ;;
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
if [ "${#UBS_FILES[@]}" -gt 0 ]; then
    filter_ubsignore_into UBS_FILES "${UBS_FILES[@]}"
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
)
bin_skip_args=()
for skip in "${BIN_SKIPS[@]}"; do
    bin_skip_args+=(--skip "$skip")
done

# Every stage the run will execute, in order. The receipt check below
# requires each one to report: a run cut short leaves later stages missing,
# and a missing stage is RED, never green.
EXPECTED_STAGES=(fmt loc clippy)
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
EXPECTED_STAGES+=(doc-tests doc ubs job-complete)

# Every stage records its own exit code and preserves complete test output.
run_stages() {
    local entry t filter fw build_rc
    local -a int_filter_words int_filter_checks
    echo "SOURCE_HEAD=${SOURCE_HEAD}"
    run_fmt
    echo "STAGE=fmt EXIT=$?"
    run_loc
    echo "STAGE=loc EXIT=$?"
    run_cargo clippy --workspace --locked --all-targets -- -D warnings
    echo "STAGE=clippy EXIT=$?"
    if [ "$RUN_LIB" = 1 ]; then
        run_cargo test --workspace --locked --lib --no-run
        echo "STAGE=lib-build EXIT=$?"
        # `timeout` needs a real binary, so the bounded run calls cargo with
        # the already-exported resolved defaults instead of run_cargo.
        run_tests lib-tests "${lib_filter_checks[@]}" \
            run_with_timeout lib-tests "$LIB_TIMEOUT" \
            cargo test --workspace --locked --lib -- "${lib_filter_words[@]}"
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
                run_cargo test -p coding-agent-search --locked --test "$t" -- "${int_filter_words[@]}"
        done
        if [ "$RUN_BIN" = 1 ]; then
            run_cargo test -p coding-agent-search --locked --bin cass --no-run
            echo "STAGE=bin-build EXIT=$?"
            # As for lib-tests, the timeout bounds the test run, not the compile.
            run_tests bin-cass \
                run_with_timeout bin-cass "$BIN_TIMEOUT" \
                cargo test -p coding-agent-search --locked --bin cass -- "${bin_skip_args[@]}"
        fi
    fi
    if [ "$RUN_GOLDENS" = 1 ]; then
        if [ "$REGEN_GOLDENS" = 1 ]; then
            UPDATE_GOLDENS=1 run_tests goldens-regen \
                run_cargo test -p coding-agent-search --locked --test golden_robot_json --test golden_robot_docs
        fi
        run_tests goldens \
            run_cargo test -p coding-agent-search --locked --test golden_robot_json --test golden_robot_docs
    fi
    if [ "$RUN_DOCS_TRUTH" = 1 ]; then
        # Never inspect a pre-existing target binary when the selected tests
        # did not build it. This stage must establish its source and ELF first.
        run_cargo build -p coding-agent-search --locked --bin cass
        build_rc=$?
        echo "STAGE=docs-build EXIT=${build_rc}"
        if [ "$build_rc" -eq 0 ]; then
            sha256 "$docs_binary"
            echo "STAGE=docs-binary-identity EXIT=$?"
            CASS_BIN="$docs_binary" scripts/validate_docs.sh --truth
            echo "STAGE=docs-truth EXIT=$?"
        fi
    fi
    run_cargo test --workspace --locked --doc
    echo "STAGE=doc-tests EXIT=$?"
    RUSTDOCFLAGS="-D warnings" run_cargo doc --workspace --locked --no-deps
    echo "STAGE=doc EXIT=$?"
    if [ "$UBS_FILES_EXPLICIT" = 1 ]; then
        run_ubs "${UBS_FILES[@]}"
    else
        run_ubs_default_lanes
    fi
    echo "STAGE=ubs EXIT=$?"
    echo "STAGE=job-complete EXIT=0"
}

# Match .cargo/config.toml: keep debug test-thread stack headroom above 2 MiB.
# This is virtual address reservation; pages are committed as needed.
export RUST_MIN_STACK=16777216
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
