#!/usr/bin/env bash
# scripts/e2e/e2e_logging_acceptance_test.sh
#
# Alias for the repository-root E2E Logging Acceptance Test.
#
# The repository-root entry point owns the single run-bundle contract: exact
# scoped test selection, publish-last receipt ordering, and isolated per-run
# artifacts. Keep this historical path as an exact alias so the two paths
# cannot drift back into independently globbing process-global evidence.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

exec "$PROJECT_ROOT/scripts/e2e_logging_acceptance_test.sh" "$@"
