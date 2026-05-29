#!/usr/bin/env bash
# Render the substrate Helm chart into manifests/ate-install/ (mTLS-mode
# install) — the canonical kubectl-apply install path. The chart at
# charts/substrate/ is the single source of truth; this script only renders.
#
# Usage:
#   hack/render-manifests.sh            # write into manifests/ate-install/
#   hack/render-manifests.sh --check    # fail if rendered output differs
#
set -o errexit -o nounset -o pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${ROOT}/manifests/ate-install"
CHART_DIR="${ROOT}/charts/substrate"
CHECK_MODE="false"

if [ "${1:-}" = "--check" ]; then
  CHECK_MODE="true"
fi

if ! command -v helm >/dev/null 2>&1; then
  echo "helm not found in PATH" >&2
  exit 1
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

helm template substrate "${CHART_DIR}" \
  --namespace ate-system \
  --set auth.mode=mtls \
  --set createNamespace=true \
  > "${TMP_DIR}/all.yaml"

# Split into per-source files so the directory structure mirrors the chart
# templates, making diffs friendlier.
python3 - "${TMP_DIR}/all.yaml" "${TMP_DIR}/out" <<'PY'
import os, re, sys, yaml
in_path, out_dir = sys.argv[1], sys.argv[2]
os.makedirs(out_dir, exist_ok=True)

with open(in_path) as f:
    raw = f.read()

# Helm prepends a "# Source: <chart>/templates/<file>" comment to each doc.
docs_by_source = {}
for doc in raw.split('\n---\n'):
    m = re.search(r'#\s*Source:\s*\S+/templates/(\S+)', doc)
    src = m.group(1) if m else "misc.yaml"
    # Drop the leading "# Source:" line from the written file.
    cleaned = re.sub(r'^\s*#\s*Source:.*\n', '', doc, count=1, flags=re.MULTILINE)
    if not cleaned.strip():
        continue
    docs_by_source.setdefault(src, []).append(cleaned.strip())

for src, docs in docs_by_source.items():
    header = (
        "#  Copyright 2026 Google LLC\n"
        "#\n"
        "#  Licensed under the Apache License, Version 2.0 (the \"License\");\n"
        "#  you may not use this file except in compliance with the License.\n"
        "#  You may obtain a copy of the License at\n"
        "#\n"
        "#      http://www.apache.org/licenses/LICENSE-2.0\n"
        "#\n"
        "#  Unless required by applicable law or agreed to in writing, software\n"
        "#  distributed under the License is distributed on an \"AS IS\" BASIS,\n"
        "#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
        "#  See the License for the specific language governing permissions and\n"
        "#  limitations under the License.\n"
        "\n"
        "# DO NOT EDIT — generated from charts/substrate by hack/render-manifests.sh.\n"
        "# Run `make helm-template` to regenerate.\n"
        "\n"
    )
    with open(os.path.join(out_dir, src), "w") as out:
        out.write(header)
        out.write("\n---\n".join(docs))
        out.write("\n")
PY

if [ "${CHECK_MODE}" = "true" ]; then
  # Only compare top-level files; subdirs like generated/ and kind/ are not
  # produced by the chart and live alongside it intentionally.
  CHECK_TMP="$(mktemp -d)"
  trap 'rm -rf "$TMP_DIR" "$CHECK_TMP"' EXIT
  mkdir -p "${CHECK_TMP}/current"
  find "${OUT_DIR}" -maxdepth 1 -type f -name '*.yaml' -exec cp {} "${CHECK_TMP}/current/" \;
  if ! diff -ruN "${CHECK_TMP}/current" "${TMP_DIR}/out" >/dev/null 2>&1; then
    echo "manifests/ate-install/ is out of date. Run: make helm-template" >&2
    diff -ruN "${CHECK_TMP}/current" "${TMP_DIR}/out" | head -60 >&2 || true
    exit 1
  fi
  echo "manifests/ate-install/ matches chart output."
  exit 0
fi

# Replace contents (preserve kind/ and generated/ subdirs which are not chart output).
mkdir -p "${OUT_DIR}"
find "${OUT_DIR}" -maxdepth 1 -type f -name '*.yaml' -delete
cp "${TMP_DIR}/out/"*.yaml "${OUT_DIR}/"
echo "Rendered $(ls "${OUT_DIR}"/*.yaml | wc -l | xargs) manifest files into ${OUT_DIR}"
