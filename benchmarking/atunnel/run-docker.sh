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

# Runs the atunnel benchmark as three containers on one docker network:
#
#   atunnel-worker  privileged; atunnel in a cgroup of its own with the
#                   CPU and memory limits, hosting actors with the worker's own
#                   code, plus the fake atelet and the ingress and egress actors
#   atunnel-sink    the fake egress gateway
#   atunnel-load    the load generator, in the foreground
#
# Arguments after the options go to `atunnel-bench load`:
#
#   benchmarking/atunnel/run-docker.sh --tunnel-cpus 1 --tunnel-memory 1g -- --rungs 1000,2000,4000
#
# Options:
#   --tunnel-cpus N     CPU limit for atunnel (default 1)
#   --tunnel-memory M   memory limit for atunnel, without swap (default 1g)
#   --actors N          actors atunnel hosts (default 10)
#   --cpuset-worker S   pin the worker container to these CPUs, e.g. 0-3
#   --cpuset-rig S      pin the generator and sink to these CPUs, e.g. 4-9
#
# Results land in benchmarking/atunnel/runs/<timestamp>/: results.txt,
# results.json, profiles/, and logs/.

set -o errexit -o nounset -o pipefail

tunnel_cpus=1
tunnel_memory=1g
actors=10
cpuset_worker=""
cpuset_rig=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --tunnel-cpus) tunnel_cpus="$2"; shift 2 ;;
    --tunnel-memory) tunnel_memory="$2"; shift 2 ;;
    --actors) actors="$2"; shift 2 ;;
    --cpuset-worker) cpuset_worker="$2"; shift 2 ;;
    --cpuset-rig) cpuset_rig="$2"; shift 2 ;;
    --) shift; break ;;
    *) break ;;
  esac
done

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "${here}/../.." && pwd)"
out="${here}/runs/$(date +%Y%m%d-%H%M%S)"
# Under runs/ rather than /tmp so Docker Desktop can always mount it.
work="${here}/runs/.work-$$"
ctx="${work}/image"
pki="${work}/pki"
mkdir -p "${ctx}" "${pki}" "${out}/profiles" "${out}/logs"
image=atunnel-bench:local
network=atunnel-bench
# The egress actor and the ingress actors serve on Unix sockets here. A named
# volume, because sockets do not work on Docker Desktop's host mounts.
shared=atunnel-bench-shared
arch="$(docker info --format '{{.Architecture}}')"
case "${arch}" in
  x86_64) goarch=amd64 ;;
  aarch64 | arm64) goarch=arm64 ;;
  *) echo "unsupported docker architecture ${arch}" >&2; exit 1 ;;
esac

echo "building linux/${goarch} binaries and ${image}"
(cd "${repo}" && CGO_ENABLED=0 GOOS=linux GOARCH="${goarch}" go build -o "${ctx}/" ./benchmarking/atunnel/cmd/...)
docker build --quiet -t "${image}" -f "${here}/Dockerfile" "${ctx}" >/dev/null

containers=(atunnel-load atunnel-worker atunnel-sink)
remove_all() {
  for c in "${containers[@]}"; do
    docker rm -f "${c}" >/dev/null 2>&1 || true
  done
  docker network rm "${network}" >/dev/null 2>&1 || true
  docker volume rm "${shared}" >/dev/null 2>&1 || true
}
cleanup() {
  local code=$?
  # The worker logs every atunnel death, with the OOM kills its cgroup counted.
  if docker cp atunnel-worker:/shared/tunnel-deaths.log "${out}/logs/" >/dev/null 2>&1; then
    local deaths ooms
    deaths="$(wc -l <"${out}/logs/tunnel-deaths.log" | tr -d ' ')"
    ooms="$(grep -c 'OOM kills: [1-9]' "${out}/logs/tunnel-deaths.log" || true)"
    echo "atunnel died ${deaths} time(s), ${ooms} of them OOM-killed at its ${tunnel_memory} limit" | tee -a "${out}/results.txt"
  fi
  echo "run in ${out}"
  for c in "${containers[@]}"; do
    docker logs "${c}" >"${out}/logs/${c}.log" 2>&1 || true
  done
  remove_all
  rm -rf "${work}"
  exit "${code}"
}
trap cleanup EXIT
remove_all
docker network create "${network}" >/dev/null
docker volume create "${shared}" >/dev/null

# The nonroot image runs as uid 65532; it writes the PKI and the results.
chmod 0777 "${out}" "${pki}" "${out}/profiles"
docker run --rm --user 65532 -v "${pki}:/pki" "${image}" \
  atunnel-bench pki --dir /pki --worker-hosts atunnel-worker --gateway-hosts atunnel-sink

# The generator and atunnel open many short connections; without these a run
# runs out of ephemeral ports to sockets in TIME_WAIT.
port_opts=(--sysctl "net.ipv4.ip_local_port_range=1024 65535" --sysctl net.ipv4.tcp_tw_reuse=1)
rig_opts=()
if [[ -n "${cpuset_rig}" ]]; then
  rig_opts+=(--cpuset-cpus "${cpuset_rig}")
fi
worker_opts=()
if [[ -n "${cpuset_worker}" ]]; then
  worker_opts+=(--cpuset-cpus "${cpuset_worker}")
fi

docker run --detach --network "${network}" ${rig_opts[@]+"${rig_opts[@]}"} --name atunnel-sink \
  -v "${pki}:/pki:ro" "${image}" \
  atunnel-bench sink --pki-dir /pki >/dev/null

# Privileged for the actors' network namespaces, nftables, and cgroups. The
# worker's credentials are mounted where a worker Pod's are projected, so
# atunnel runs on its default flags.
docker run --detach --network "${network}" --privileged --user 0 --cgroupns private \
  "${port_opts[@]}" ${worker_opts[@]+"${worker_opts[@]}"} --name atunnel-worker \
  -v "${pki}:/pki:ro" -v "${shared}:/shared" \
  -v "${pki}/worker.pem:/run/podidentity.podcert.ate.dev/credential-bundle.pem:ro" \
  -v "${pki}/ca.pem:/run/podidentity.podcert.ate.dev/trust-bundle.pem:ro" \
  -v "${pki}/ca.pem:/run/servicedns.podcert.ate.dev/trust-bundle.pem:ro" \
  "${image}" \
  atunnel-bench worker --pki-dir /pki --shared-dir /shared \
  --tunnel-cpus "${tunnel_cpus}" --tunnel-memory "${tunnel_memory}" -- \
  --actors "${actors}" --gateway atunnel-sink:9443 --dns-egress-policy enforce-in-atunnel >/dev/null

# The generator runs in the foreground; Ctrl-C stops it and the trap cleans up.
docker run --network "${network}" "${port_opts[@]}" ${rig_opts[@]+"${rig_opts[@]}"} --name atunnel-load \
  -v "${pki}:/pki:ro" -v "${shared}:/shared" -v "${out}:/out" "${image}" \
  atunnel-bench load --pki-dir /pki --shared-dir /shared \
  --results-json /out/results.json --profile-dir /out/profiles "$@" | tee "${out}/results.txt"
