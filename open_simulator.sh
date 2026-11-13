#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INDEX_PATH="${DIR}/demos/onboarding-tui/index.html"

echo "⚡ Opening Agent Substrate Onboarding Interactive Web Simulator..."

if command -v open >/dev/null 2>&1; then
    open "${INDEX_PATH}"
elif command -v xdg-open >/dev/null 2>&1; then
    xdg-open "${INDEX_PATH}"
elif command -v start >/dev/null 2>&1; then
    start "${INDEX_PATH}"
else
    echo "Open the following file in your browser:"
    echo "file://${INDEX_PATH}"
fi
