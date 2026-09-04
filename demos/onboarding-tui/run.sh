#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PYTHONPATH="${DIR}:${PYTHONPATH:-}"

python3 "${DIR}/onboard.py" "$@"
