#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Guards against partial-deploy drift: kind-mode component deploys must emit exactly
# the component overlay render, component and full-overlay renders must match, and
# each container must carry its OTel env — value-checked per container because a
# renamed container turns a strategic-merge patch into a silent ghost sibling.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "FAIL: kubectl is required for kind overlay verification" >&2
  exit 1
fi

OUT="$(mktemp -d)"
trap 'rm -rf "${OUT}"' EXIT

kubectl kustomize manifests/ate-install/kind --load-restrictor LoadRestrictionsNone > "${OUT}/root.yaml"
for c in atenet ate-api-server atelet; do
  kubectl kustomize "manifests/ate-install/kind/${c}" --load-restrictor LoadRestrictionsNone > "${OUT}/${c}.yaml"
  (
    # shellcheck disable=SC2030  # subshell-local env is the point: lib-mode source must not leak
    export ATE_INSTALL_LIB_ONLY=1 NO_DEV_ENV=true KUBECTL_CONTEXT=verify-only
    # shellcheck source=/dev/null
    source hack/install-ate.sh
    ATE_INSTALL_KIND=true component_manifests "${c}"
  ) > "${OUT}/${c}.deploy.yaml"
  if ! diff -u "${OUT}/${c}.yaml" "${OUT}/${c}.deploy.yaml" > "${OUT}/${c}.routing.diff"; then
    echo "FAIL: install-ate.sh component_manifests ${c} (kind mode) does not emit the ${c} overlay render:" >&2
    head -20 "${OUT}/${c}.routing.diff" >&2
    exit 1
  fi
done

# component_manifests being correct is not enough — a deploy function applying
# base manifests directly would still pass above. Stub side effects and assert
# each deploy function reaches component_manifests.
deploy_fn_for() {
  case "$1" in
    atenet) echo deploy_atenet ;;
    ate-api-server) echo deploy_ate_apiserver ;;
    atelet) echo deploy_atelet ;;
  esac
}
for c in atenet ate-api-server atelet; do
  fn="$(deploy_fn_for "${c}")"
  (
    # shellcheck disable=SC2030,SC2031  # subshell-local env is the point: lib-mode source must not leak
    export ATE_INSTALL_LIB_ONLY=1 NO_DEV_ENV=true KUBECTL_CONTEXT=verify-only
    # shellcheck source=/dev/null
    source hack/install-ate.sh
    # shellcheck disable=SC2317  # stubs are called indirectly through "${fn}"
    {
      log_step() { :; }
      ensure_crds() { :; }
      ensure_apiserver_prerequisites() { :; }
      run_kubectl() { :; }
      run_ko() { :; }
      component_manifests() { echo "$1" >> "${OUT}/deploy-calls"; }
    }
    "${fn}"
  )
  if ! grep -qx "${c}" "${OUT}/deploy-calls" 2>/dev/null; then
    echo "FAIL: ${fn} does not route its manifests through component_manifests ${c}" >&2
    exit 1
  fi
done

python3 - "${OUT}" <<'EOF'
import re
import sys

out = sys.argv[1]
ENDPOINT = "http://opentelemetry-collector.otel-system.svc:4317"
INTERVAL = ("OTEL_METRIC_EXPORT_INTERVAL", "10000")
TIMEOUT = ("OTEL_METRIC_EXPORT_TIMEOUT", "10000")

def docs(path):
    """Split a kustomize render into per-resource docs keyed by (kind, name)."""
    result = {}
    for doc in open(path).read().split("\n---\n"):
        kind = name = None
        in_metadata = False
        for line in doc.splitlines():
            if line.startswith("kind: "):
                kind = line.split(": ", 1)[1]
            elif line == "metadata:":
                in_metadata = True
            elif in_metadata and line.startswith("  name: "):
                name = line.split(": ", 1)[1]
                in_metadata = False
            elif in_metadata and not line.startswith("  "):
                in_metadata = False
        if kind and name:
            result[(kind, name)] = doc.strip()
    return result

def container_block(doc, container):
    """Return only the named container's lines: a doc-wide search would still find
    the env on a ghost sibling left by a mistargeted patch. kustomize sorts map
    keys, so "name:" sits inside the block, not necessarily on the dash line."""
    lines = doc.splitlines()
    for ci, line in enumerate(lines):
        m = re.match(r"^(\s*)containers:$", line)
        if not m:
            continue
        indent = m.group(1)
        name_line = indent + "  name: " + container
        dash_name_line = indent + "- name: " + container
        items, start = [], None
        i = ci + 1
        while i < len(lines):
            ln = lines[i]
            if ln.startswith(indent + "- "):
                if start is not None:
                    items.append((start, i))
                start = i
            elif ln.strip() and len(ln) - len(ln.lstrip()) <= len(indent):
                break
            i += 1
        if start is not None:
            items.append((start, i))
        for s, e in items:
            block = lines[s:e]
            if block[0] == dash_name_line or name_line in block:
                return "\n".join(block)
    return None

# (component overlay, workload kind, workload name, container, required env)
WORKLOADS = [
    ("atenet", "Deployment", "atenet-router", "atenet-router",
     [("OTEL_EXPORTER_OTLP_ENDPOINT", ENDPOINT)]),
    ("atenet", "Deployment", "dns", None, []),
    ("ate-api-server", "Deployment", "ate-api-server", "ate-api-server",
     [("OTEL_EXPORTER_OTLP_ENDPOINT", ENDPOINT), INTERVAL, TIMEOUT]),
    ("atelet", "DaemonSet", "atelet", "atelet",
     [("OTEL_EXPORTER_OTLP_ENDPOINT", ENDPOINT), INTERVAL, TIMEOUT]),
]

root = docs(f"{out}/root.yaml")
failures = []
for component, kind, name, container, required_env in WORKLOADS:
    comp = docs(f"{out}/{component}.yaml")
    key = (kind, name)
    if key not in comp:
        failures.append(f"{component}: {kind} {name} missing from component overlay render")
        continue
    if key not in root:
        failures.append(f"root overlay: {kind} {name} missing")
        continue
    if comp[key] != root[key]:
        failures.append(
            f"{component}: {kind} {name} renders differently in the component overlay "
            f"and the full kind overlay — the same patch must exist in exactly one place"
        )
    if container is None:
        continue
    block = container_block(comp[key], container)
    if block is None:
        failures.append(f"{component}: {kind} {name} has no container {container!r}")
        continue
    for env_name, env_value in required_env:
        # Name and value must match on the same env item: interval and timeout
        # share "10000", so independent searches cross-match. kustomize quotes
        # numeric-looking values.
        item = re.compile(
            r"- name: " + re.escape(env_name) + r"\n\s*value: \"?" + re.escape(env_value) + r"\"?(\n|$)"
        )
        if not item.search(block):
            failures.append(
                f"{component}: {kind} {name} container {container!r} lacks "
                f"{env_name}={env_value} — the env patch no longer matches it"
            )

if failures:
    print("kind overlay verification FAILED:", file=sys.stderr)
    for f in failures:
        print(f"  - {f}", file=sys.stderr)
    sys.exit(1)
print(f"kind overlays OK: {len(WORKLOADS)} workloads consistent, install routing and OTel env verified")
EOF
