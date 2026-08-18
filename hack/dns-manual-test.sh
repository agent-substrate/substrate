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

# Serves the rendered actor DNS zone with the pinned CoreDNS image and checks
# the response code CoreDNS returns for each query type. The unit tests pin
# what we generate; this checks what CoreDNS does with it.

set -o errexit -o nounset -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

COREDNS_IMAGE="coredns/coredns:1.11.1"
ALPINE_IMAGE="alpine:3.19"
BUSYBOX_IMAGE="busybox:1"

ROUTER_IP="10.240.0.10"
SUFFIX="actors.resources.substrate.ate.dev"
ACTOR="demo.default.${SUFFIX}"

NET="ate-dns-manual"
SERVER="ate-dns-manual-server"
CLIENT="ate-dns-manual-client"
VOLUME="ate-dns-manual-conf"

corefile=""
tmpdir=""

usage() {
  cat <<EOF
Usage: ${0##*/} [--corefile PATH]

  --corefile PATH  Serve this Corefile instead of rendering the current tree's.
                   Point it at a zone rendered from before the fix to use the
                   run as a negative control -- the AAAA case must SERVFAIL.

Requires a reachable Docker daemon. Where the daemon is not on the host socket
(a Lima VM, say), point DOCKER at a client that reaches it:

  DOCKER="limactl shell docker-nested docker" ${0##*/}
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --corefile) corefile="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown flag: $1" >&2; usage >&2; exit 1 ;;
  esac
done

read -ra docker_cmd <<< "${DOCKER:-docker}"
dk() { "${docker_cmd[@]}" "$@"; }

remove_containers() {
  dk rm -f "$SERVER" "$CLIENT" >/dev/null 2>&1 || true
  dk volume rm "$VOLUME" >/dev/null 2>&1 || true
  dk network rm "$NET" >/dev/null 2>&1 || true
}
cleanup() {
  remove_containers
  [[ -n "$tmpdir" ]] && rm -rf "$tmpdir"
}
trap cleanup EXIT

if ! dk version >/dev/null 2>&1; then
  echo "cannot reach a Docker daemon; see --help" >&2
  exit 1
fi

if [[ -z "$corefile" ]]; then
  tmpdir="$(mktemp -d)"
  ( cd "$ROOT" && COREFILE_DUMP_DIR="$tmpdir" COREFILE_DUMP_ROUTER_IP="$ROUTER_IP" \
      go test ./cmd/atenet/internal/dns/... -run TestDumpCorefile -count=1 >/dev/null )
  corefile="$tmpdir/Corefile"
fi
echo "Serving ${corefile}:"
sed 's/^/    /' "$corefile"
echo

remove_containers
dk network create "$NET" >/dev/null
dk volume create "$VOLUME" >/dev/null
# Stream the Corefile into a volume rather than bind-mounting it: the daemon may
# be in a VM that cannot see this path.
dk run -i --rm -v "$VOLUME":/out "$BUSYBOX_IMAGE" sh -c 'cat > /out/Corefile' < "$corefile"
dk run -d --name "$SERVER" --network "$NET" -v "$VOLUME":/etc/coredns \
  "$COREDNS_IMAGE" -conf /etc/coredns/Corefile >/dev/null
dk run -d --name "$CLIENT" --network "$NET" "$ALPINE_IMAGE" sleep 600 >/dev/null
dk exec "$CLIENT" apk add --no-cache bind-tools >/dev/null 2>&1

for _ in $(seq 20); do
  if dk exec "$CLIENT" dig +time=1 +tries=1 "@${SERVER}" -q "$ACTOR" -t A >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

failures=0

# check LABEL NAME QTYPE WANT_STATUS WANT_ANSWERS WANT_AUTHORITY
check() {
  local label="$1" name="$2" qtype="$3" want_status="$4" want_answers="$5" want_authority="$6"
  local out status answers authority
  # -q/-t, not positional: a malformed name starting with "-" is otherwise
  # parsed as a dig flag.
  out="$(dk exec "$CLIENT" dig +noall +comment "@${SERVER}" -q "$name" -t "$qtype" 2>&1 || true)"
  status="$(sed -n 's/.*status: \([A-Z]*\).*/\1/p' <<<"$out" | head -1)"
  answers="$(sed -n 's/.*ANSWER: \([0-9]*\).*/\1/p' <<<"$out" | head -1)"
  authority="$(sed -n 's/.*AUTHORITY: \([0-9]*\).*/\1/p' <<<"$out" | head -1)"
  if [[ "$status" == "$want_status" && "$answers" == "$want_answers" && "$authority" == "$want_authority" ]]; then
    printf 'PASS  %-34s %-8s %s answers=%s authority=%s\n' "$label" "$qtype" "$status" "$answers" "$authority"
  else
    printf 'FAIL  %-34s %-8s got %s answers=%s authority=%s, want %s answers=%s authority=%s\n' \
      "$label" "$qtype" "${status:-none}" "${answers:-none}" "${authority:-none}" \
      "$want_status" "$want_answers" "$want_authority"
    failures=$((failures + 1))
  fi
}

# A already worked; it is here so the NODATA blocks cannot silently break it.
check "valid actor"          "$ACTOR"                A     NOERROR  1 0
# The fix: an empty answer with an SOA, not SERVFAIL.
check "valid actor"          "$ACTOR"                AAAA  NOERROR  0 1
check "valid actor"          "$ACTOR"                HTTPS NOERROR  0 1
check "valid actor"          "$ACTOR"                SRV   NOERROR  0 1
check "name in zone, no actor" "nope.${SUFFIX}"      A     NXDOMAIN 0 1
check "malformed actor name" "-bad.default.${SUFFIX}" A    NXDOMAIN 0 1

got_a="$(dk exec "$CLIENT" dig +short "@${SERVER}" -q "$ACTOR" -t A 2>&1 | tr -d '\r')"
if [[ "$got_a" == "$ROUTER_IP" ]]; then
  printf 'PASS  %-34s %-8s %s\n' "A record points at the router" "A" "$got_a"
else
  printf 'FAIL  %-34s %-8s got %q, want %q\n' "A record points at the router" "A" "$got_a" "$ROUTER_IP"
  failures=$((failures + 1))
fi

# The user-visible bug: musl resolves A and AAAA in parallel and fails the pair
# when either half errors, so this is what SERVFAIL actually costs.
server_ip="$(dk inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$SERVER")"
if dk run --rm --network "$NET" --dns "$server_ip" "$ALPINE_IMAGE" \
     getent ahosts "$ACTOR" 2>/dev/null | grep -q "$ROUTER_IP"; then
  printf 'PASS  %-34s %-8s resolved %s\n' "alpine getent ahosts" "musl" "$ROUTER_IP"
else
  printf 'FAIL  %-34s %-8s did not resolve\n' "alpine getent ahosts" "musl"
  failures=$((failures + 1))
fi

echo
if (( failures > 0 )); then
  echo "${failures} check(s) failed"
  exit 1
fi
echo "all checks passed"
