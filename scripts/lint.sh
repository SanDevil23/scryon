#!/usr/bin/env bash
set -euo pipefail

# Lint each module in the workspace individually.
# golangci-lint doesn't support go.work natively so we cd into each module.
# The workspace (go.work) stays active so cross-module imports resolve correctly.

MODULES=(
  "pkg/config"
  "pkg/logger"
  "pkg/middleware"
  "services/ingest"
  "services/processor"
)

FAILED=()

for mod in "${MODULES[@]}"; do
  echo ""
  echo "━━━ Linting $mod ━━━"
  if ! (cd "$mod" && golangci-lint run --timeout=5m ./...); then
    FAILED+=("$mod")
  fi
done

if [ ${#FAILED[@]} -gt 0 ]; then
  echo ""
  echo "Lint failed for:"
  for mod in "${FAILED[@]}"; do
    echo "   - $mod"
  done
  exit 1
fi

echo ""
echo "All modules passed lint"