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
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-kind-kind}"
BUCKET_NAME="${BUCKET_NAME:-ate-snapshots}"
ARCH="$(go env GOARCH)"
CACHE_DIR="${ROOT}/bin/gvisor-assets/${ARCH}"

kubectl --context="${KUBECTL_CONTEXT}" -n ate-system wait \
  --for=condition=complete job/rustfs-bucket-init --timeout=120s

node="$(kubectl --context="${KUBECTL_CONTEXT}" -n ate-system get pod \
  -l app=rustfs --field-selector=status.phase=Running \
  -o jsonpath='{.items[0].spec.nodeName}')"
cluster_ip="$(kubectl --context="${KUBECTL_CONTEXT}" -n ate-system get service rustfs \
  -o jsonpath='{.spec.clusterIP}')"
aws_image="$(kubectl --context="${KUBECTL_CONTEXT}" -n ate-system get job rustfs-bucket-init \
  -o jsonpath='{.spec.template.spec.containers[0].image}')"

endpoint="http://${cluster_ip}:9000"
if [[ "${cluster_ip}" == *:* ]]; then
  endpoint="http://[${cluster_ip}]:9000"
fi
aws_cli=(
  docker run --rm -i
  --network "container:${node}"
  -e AWS_ACCESS_KEY_ID=rustfsadmin
  -e AWS_SECRET_ACCESS_KEY=rustfsadmin
  -e AWS_REGION=us-east-1
  -e "AWS_ENDPOINT_URL=${endpoint}"
  "${aws_image}"
)

mkdir -p "${CACHE_DIR}"
assets="$(
  kubectl --context="${KUBECTL_CONTEXT}" get sandboxconfig gvisor-default \
    -o go-template="{{range \$i, \$v := .spec.versions}}{{if ne \$v.state \"Disabled\"}}{{\$asset := index (index \$v.assets \"${ARCH}\") \"gvisor\"}}{{\$i}}{{\"\\t\"}}{{\$v.name}}{{\"\\t\"}}{{\$asset.url}}{{\"\\t\"}}{{\$asset.sha256}}{{\"\\n\"}}{{end}}{{end}}"
)"
if [[ -z "${assets}" ]]; then
  echo "error: gvisor-default has no enabled ${ARCH} assets" >&2
  exit 1
fi
while IFS=$'\t' read -r index version url sha; do
  object="${url#gs://}"
  if [[ "${object}" == "${url}" ]]; then
    echo "error: gVisor asset URL is not a gs:// URL: ${url}" >&2
    exit 1
  fi
  name="${url##*/}"
  file="${CACHE_DIR}/${sha}-${name}"
  if [[ ! -f "${file}" ]] || [[ "$(sha256sum "${file}" | cut -d ' ' -f 1)" != "${sha}" ]]; then
    curl --fail --location --retry 3 "https://storage.googleapis.com/${object}" -o "${file}"
  fi
  if [[ "$(sha256sum "${file}" | cut -d ' ' -f 1)" != "${sha}" ]]; then
    echo "error: checksum mismatch for ${url}" >&2
    exit 1
  fi

  key="gvisor-assets/${ARCH}/${sha}/${name}"
  "${aws_cli[@]}" s3 cp - "s3://${BUCKET_NAME}/${key}" < "${file}"
  kubectl --context="${KUBECTL_CONTEXT}" patch sandboxconfig gvisor-default --type=json \
    -p="[{\"op\":\"replace\",\"path\":\"/spec/versions/${index}/assets/${ARCH}/gvisor/url\",\"value\":\"s3://${BUCKET_NAME}/${key}\"}]"
  echo "Staged ${version}/${ARCH}/gvisor"
done <<< "${assets}"
