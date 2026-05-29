#!/usr/bin/env bash
# Generate the controller ClusterRole into the Helm chart and templatize its
# name so multi-release installs do not collide on a cluster-scoped resource.
#
# controller-gen emits a YAML file with a fixed `roleName=` value. We post-
# process that file to swap the static name for the chart's fullname helper,
# matching the convention used by every other resource in charts/substrate/.
#
# Invoked via `go generate ./internal/controllers/...`.
set -o errexit -o nounset -o pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${ROOT}/charts/substrate/templates/role.yaml"

bash "${ROOT}/hack/run-tool.sh" controller-gen \
  "rbac:headerFile=${ROOT}/hack/boilerplate/yaml.txt,roleName=ate-controller" \
  paths="${ROOT}/internal/controllers/..." \
  "output:rbac:artifacts:config=${ROOT}/charts/substrate/templates/"

# Templatize the ClusterRole name. controller-gen emits `  name: ate-controller`
# at column 0; the substitution is exact-match to stay robust.
sed -i 's|^  name: ate-controller$|  name: {{ include "substrate.fullname" (list "ate-controller" .) }}|' "${OUT}"
