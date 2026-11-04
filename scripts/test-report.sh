#!/usr/bin/env bash
# test-report.sh - Run tests with JUnit XML and optional HTML report
#
# Usage:
#   ./scripts/test-report.sh           # Run all tests with JUnit output
#   ./scripts/test-report.sh --quick   # Run unit tests only (skip E2E)
#   ./scripts/test-report.sh --e2e     # Run E2E tests only
#   ./scripts/test-report.sh --open    # Open HTML report after generation
#
# Reports are generated in ${CARGO_TARGET_DIR}/nextest/<profile>/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=lib/run_cargo.sh
source "${PROJECT_ROOT}/scripts/lib/run_cargo.sh"
cass_cargo_resolve

QUICK_MODE=false
E2E_ONLY=false
OPEN_REPORT=false
PROFILE="full"

# Parse arguments
for arg in "$@"; do
    case $arg in
        --quick)
            QUICK_MODE=true
            ;;
        --e2e)
            E2E_ONLY=true
            PROFILE="e2e"
            ;;
        --open)
            OPEN_REPORT=true
            ;;
        --help|-h)
            echo "Usage: $0 [--quick] [--e2e] [--open]"
            echo ""
            echo "Options:"
            echo "  --quick    Run unit tests only (skip E2E tests)"
            echo "  --e2e      Run E2E tests only (uses e2e profile)"
            echo "  --open     Open HTML report in browser (requires junit2html)"
            echo ""
            echo "Profiles:"
            echo "  full - Full test suite with JUnit XML output (local default)"
            echo "  e2e  - E2E tests with sequential execution"
            echo ""
            echo "Environment:"
            echo "  CARGO_TARGET_DIR  local cargo target dir"
            echo "                    (resolved by scripts/lib/run_cargo.sh)"
            echo ""
            echo "Reports generated in: \$CARGO_TARGET_DIR/nextest/<profile>/junit.xml"
            echo ""
            exit 0
            ;;
    esac
done

cd "$PROJECT_ROOT"

# Check if cargo-nextest is available in the local cargo toolchain.
if ! run_cargo nextest --version > /dev/null 2>&1; then
    echo "Error: cargo-nextest is unavailable locally"
    echo ""
    echo "Install it with: cargo install cargo-nextest --locked"
    echo ""
    exit 1
fi

echo "==================================="
echo "  cass Test Report Generator"
echo "==================================="
echo ""

# Build first
echo "Building project..."
run_cargo build --tests --quiet

# Determine filter expression
FILTER_ARGS=()
if [ "$E2E_ONLY" = true ]; then
    FILTER_ARGS=(-E "binary(e2e_index_tui) | binary(e2e_filters) | binary(e2e_multi_connector)")
    echo "Running: E2E tests only"
elif [ "$QUICK_MODE" = true ]; then
    FILTER_ARGS=(-E "not (binary(~e2e))")
    echo "Running: Unit tests (skipping E2E tests)"
else
    FILTER_ARGS=()
    echo "Running: All tests"
fi

echo "Profile: $PROFILE"
echo "Target dir: $CASS_CARGO_TARGET_DIR"
echo ""

# Run tests
echo "Running tests..."
echo "-----------------------------------"

run_cargo nextest run --profile "$PROFILE" "${FILTER_ARGS[@]}" --no-fail-fast 2>&1 || true

echo ""
echo "-----------------------------------"
echo "Test Report"
echo "-----------------------------------"

JUNIT_PATH="$CASS_CARGO_TARGET_DIR/nextest/$PROFILE/junit.xml"

if [ -f "$JUNIT_PATH" ]; then
    echo "JUnit XML report: $JUNIT_PATH"

    # Parse basic stats from JUnit XML
    if command -v xmllint &> /dev/null; then
        # Use /testsuites for aggregate stats (nextest puts totals on root element)
        TESTS=$(xmllint --xpath 'string(/testsuites/@tests)' "$JUNIT_PATH" 2>/dev/null || echo "?")
        FAILURES=$(xmllint --xpath 'string(/testsuites/@failures)' "$JUNIT_PATH" 2>/dev/null || echo "?")
        ERRORS=$(xmllint --xpath 'string(/testsuites/@errors)' "$JUNIT_PATH" 2>/dev/null || echo "?")
        TIME=$(xmllint --xpath 'string(/testsuites/@time)' "$JUNIT_PATH" 2>/dev/null || echo "?")

        echo ""
        echo "Summary:"
        echo "  Tests:    $TESTS"
        echo "  Failures: $FAILURES"
        echo "  Errors:   $ERRORS"
        echo "  Time:     ${TIME}s"
    fi

    # Generate HTML report if junit2html is available
    if [ "$OPEN_REPORT" = true ]; then
        if command -v junit2html &> /dev/null; then
            HTML_PATH="$CASS_CARGO_TARGET_DIR/nextest/$PROFILE/report.html"
            echo ""
            echo "Generating HTML report..."
            junit2html "$JUNIT_PATH" "$HTML_PATH"
            echo "HTML report: $HTML_PATH"

            # Open in browser
            if command -v open &> /dev/null; then
                open "$HTML_PATH"  # macOS
            elif command -v xdg-open &> /dev/null; then
                xdg-open "$HTML_PATH"  # Linux
            else
                echo "Could not detect browser opener. Open manually: $HTML_PATH"
            fi
        else
            echo ""
            echo "Note: Install junit2html for HTML reports: pip install junit2html"
            echo "Opening raw XML file..."
            if command -v open &> /dev/null; then
                open "$JUNIT_PATH"  # macOS
            elif command -v xdg-open &> /dev/null; then
                xdg-open "$JUNIT_PATH"  # Linux
            fi
        fi
    fi
else
    echo "Warning: JUnit XML report not found at $JUNIT_PATH"
    echo "Tests may have failed to run."
fi

echo ""
echo "Done!"
