#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Measures Envoy's memory with slow peers, or its throughput, under one set of
# socket options. Each measurement gets a fresh Envoy and origin:
#
#   envoy-sockmem-envoy    Envoy with 2 CPUs and 2 GiB of memory, configured
#                          like atenet-router (ingress) or the egress
#                          gateway (egress)
#   envoy-sockmem-origin   Envoy's upstream
#   envoy-sockmem-client   the peer, sharing Envoy's PID namespace to read
#                          its cgroup
#
#   benchmarking/envoy-sockmem/run-docker.sh --kind egress --conns 100,400 \
#     --tcp-notsent-lowat 16384
#
# See README.md for the options.

set -o errexit -o nounset -o pipefail

kind=egress
modes=""
label=""
conns=100
hold=""
netem_delay=""
out=""
config_args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --kind) kind="$2"; shift 2 ;;
    --modes) modes="$2"; shift 2 ;;
    --conns) conns="$2"; shift 2 ;;
    --hold) hold="$2"; shift 2 ;;
    --netem-delay) netem_delay="$2"; shift 2 ;;
    --out) out="$2"; shift 2 ;;
    --tcp-notsent-lowat | --tcp-send-buffer | --tcp-receive-buffer | --upstream-stream-window)
      config_args+=("$1" "$2")
      # Each result row is named after the settings it ran with.
      case "$1" in
        --tcp-notsent-lowat) label+=" lowat=$2" ;;
        --tcp-send-buffer) label+=" sndbuf=$2" ;;
        --tcp-receive-buffer) label+=" rcvbuf=$2" ;;
        --upstream-stream-window) label+=" window=$2" ;;
      esac
      shift 2 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done
if [[ -n "${netem_delay}" ]]; then
  label+=" +${netem_delay}"
fi
label="${label# }"
label="${label:-off}"
if [[ -z "${modes}" ]]; then
  modes="slow-downstream,slow-upstream"
fi

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../.." && pwd)"
out="${out:-${here}/runs/$(date +%Y%m%d-%H%M%S)}"
mkdir -p "${out}"
work="${here}/runs/.work-$$"
mkdir -p "${work}/image" "${work}/pki"
image=envoy-sockmem:local
# The base of the dataplane image (cmd/dataplane/envoy/Dockerfile), without
# Substrate's dynamic modules, which this configuration does not load.
envoy_image=envoyproxy/envoy:v1.39.5@sha256:8fc594a5d85eb76dca24155cfb9208882406873932b20cf33649a0a611194c43
network=envoy-sockmem
containers=(envoy-sockmem-client envoy-sockmem-envoy envoy-sockmem-origin)

arch="$(docker info --format '{{.Architecture}}')"
case "${arch}" in
  x86_64) goarch=amd64 ;;
  aarch64 | arm64) goarch=arm64 ;;
  *) echo "unsupported docker architecture ${arch}" >&2; exit 1 ;;
esac
(cd "${repo}" && CGO_ENABLED=0 GOOS=linux GOARCH="${goarch}" go build -o "${work}/image/" ./benchmarking/envoy-sockmem)
docker build --quiet -t "${image}" -f "${here}/Dockerfile" "${work}/image" >/dev/null

remove_all() {
  for c in "${containers[@]}"; do
    docker rm -f "${c}" >/dev/null 2>&1 || true
  done
}
cleanup() {
  local code=$?
  remove_all
  docker network rm "${network}" >/dev/null 2>&1 || true
  rm -rf "${work}"
  echo "results in ${out}"
  exit "${code}"
}
trap cleanup EXIT
remove_all
docker network rm "${network}" >/dev/null 2>&1 || true
docker network create "${network}" >/dev/null

# A GKE node's TCP buffer limits, which Docker Desktop sets higher. They are
# per network namespace, so each container gets its own.
node_tcp_opts=(--sysctl "net.ipv4.tcp_rmem=4096 87380 6291456" --sysctl "net.ipv4.tcp_wmem=4096 16384 4194304")

docker run --rm --user 65532 -v "${work}/pki:/pki" "${image}" envoy-sockmem pki --dir /pki
docker run --rm "${image}" envoy-sockmem config --kind "${kind}" ${config_args[@]+"${config_args[@]}"} >"${work}/pki/envoy.yaml"
safe_label="$(tr -c 'A-Za-z0-9._+-' '_' <<<"${label}" | sed 's/_$//')"
cp "${work}/pki/envoy.yaml" "${out}/envoy-${safe_label}-${kind}.yaml"

IFS=, read -r -a mode_list <<<"${modes}"
IFS=, read -r -a conn_list <<<"${conns}"
for mode in "${mode_list[@]}"; do
  for n in "${conn_list[@]}"; do
    remove_all
    docker run --detach --network "${network}" ${node_tcp_opts[@]+"${node_tcp_opts[@]}"} --name envoy-sockmem-origin --network-alias origin \
      "${image}" envoy-sockmem origin >/dev/null
    docker run --detach --network "${network}" ${node_tcp_opts[@]+"${node_tcp_opts[@]}"} --name envoy-sockmem-envoy --network-alias envoy \
      --cpus 2 --memory 2g --memory-swap 2g --cgroupns private \
      -v "${work}/pki:/pki:ro" "${envoy_image}" \
      envoy -c /pki/envoy.yaml --concurrency 2 --log-level warn >/dev/null
    if [[ -n "${netem_delay}" ]]; then
      docker run --rm --network container:envoy-sockmem-envoy --user 0 --cap-add NET_ADMIN "${image}" \
        envoy-sockmem netem --dev eth0 --delay "${netem_delay}"
    fi
    ready=false
    for _ in $(seq 50); do
      if docker run --rm --network "${network}" "${image}" envoy-sockmem client --mode ready --pki-dir / >/dev/null 2>&1; then
        ready=true
        break
      fi
      sleep 0.2
    done
    if [[ "${ready}" != true ]]; then
      echo "Envoy did not become ready:" >&2
      docker logs envoy-sockmem-envoy 2>&1 | tail -20 >&2
      exit 1
    fi
    h="${hold:-$((n / 200 + 15))s}"
    echo "${label} ${kind} ${mode} ${n} conns, hold ${h}" >&2
    docker run --rm --network "${network}" ${node_tcp_opts[@]+"${node_tcp_opts[@]}"} --name envoy-sockmem-client --pid container:envoy-sockmem-envoy \
      --user 0 --cap-add SYS_PTRACE -v "${work}/pki:/pki:ro" "${image}" \
      envoy-sockmem client --kind "${kind}" --mode "${mode}" --conns "${n}" --hold "${h}" --label "${label}" \
      | tee -a "${out}/results.jsonl" >/dev/null || echo "${label} ${mode} ${n}: client failed" | tee -a "${out}/deaths.txt" >&2
    if ! docker inspect envoy-sockmem-envoy --format '{{.State.Running}}' | grep -q true; then
      echo "${label} ${mode} ${n}: Envoy died, $(docker inspect envoy-sockmem-envoy --format 'OOM-killed {{.State.OOMKilled}}, exit {{.State.ExitCode}}')" | tee -a "${out}/deaths.txt" >&2
    fi
  done
done
docker run --rm -i "${image}" envoy-sockmem report <"${out}/results.jsonl" | tee "${out}/results.txt"
