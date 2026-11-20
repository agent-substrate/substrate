#!/usr/bin/env bash

# Copyright 2026 The Agent Substrate Authors
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

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

GENERATED_DIR="manifests/ate-install/generated"
CHART_TEMPLATES_DIR="charts/substrate-crds/templates"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

mkdir -p "${TMP_DIR}/generated" "${TMP_DIR}/chart"
cp "${GENERATED_DIR}/"ate.dev_*.yaml "${TMP_DIR}/generated/"
cp "${CHART_TEMPLATES_DIR}/"ate.dev_*.yaml "${TMP_DIR}/chart/"

# The generated CRDs start with a leading document separator after the
# boilerplate header. In chart templates that separator renders as a
# comment-only YAML document, so the chart copies intentionally omit it.
# The chart copies also carry the helm.sh/resource-policy: keep annotation, so
# an uninstall keeps the CRDs and the custom resources.
for file in "${TMP_DIR}/generated/"*.yaml; do
  awk 'BEGIN { removed = 0 } /^---$/ && removed == 0 { removed = 1; next } { print } /^  annotations:$/ { print "    helm.sh/resource-policy: keep" }' "${file}" > "${file}.tmp"
  mv "${file}.tmp" "${file}"
done

# The chart Role is the generated Role with a release-aware name and namespace.
sed \
  -e 's|^  name: ate-controller$|  name: {{ include "substrate.fullname" (list "ate-controller" .) }}|' \
  -e 's|^  namespace: ate-system$|  namespace: {{ .Release.Namespace }}|' \
  "${GENERATED_DIR}/role.yaml" > "${TMP_DIR}/generated/role.yaml"
cp charts/substrate/templates/role.yaml "${TMP_DIR}/chart/role.yaml"

if ! diff -ruN "${TMP_DIR}/generated" "${TMP_DIR}/chart" >/dev/null 2>&1; then
  echo "The chart copies are out of sync with ${GENERATED_DIR}." >&2
  echo "Copy updated CRDs into charts/substrate-crds/templates and keep their" >&2
  echo "helm.sh/resource-policy: keep annotation. Copy role.yaml into" >&2
  echo "charts/substrate/templates with the name and namespace templated." >&2
  diff -ruN "${TMP_DIR}/generated" "${TMP_DIR}/chart" | head -80 >&2 || true
  exit 1
fi

echo "The chart CRDs and controller Role match ${GENERATED_DIR}."
