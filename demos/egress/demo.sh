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

# A presenter-driven walkthrough of the egress path. See README.md.
#
# This drives internal/e2e/fixtures/egressprobe deployed as an Actor. The probe
# is a test fixture, not a product surface -- everything customer-facing here is
# the narration and the formatting, and the raw Result is one --verbose away for
# whichever engineer in the room asks for it.
#
# The demo makes no claim the suite does not already assert. What it adds is an
# order to put the claims in, and a destination that echoes, so the injected
# header can be read rather than inferred.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# The atespace and actor names are NOT free choices. internal/extproc/hardcoded.go
# keys the policy table on the atespace/name pair, so these strings are what
# select a policy. Renaming an actor here silently selects no policy at all,
# which presents as a denied CONNECT rather than as a configuration error.
ATESPACE="acme-prod"
ACTOR_BARE="actor-without-github-access"   # KindAllowByHostname     -- allowlist, no credential
ACTOR_INJECT="actor-with-github-access"     # KindBasicCredentialInject -- allowlist + injection
ACTOR_DENIED="quarantined"     # KindDenyAll             -- no egress at all

TEMPLATE_NS="ate-demo-egress"
TEMPLATE_NAME="egressprobe"
# Three actors on stage at once, plus a spare to absorb a worker still draining.
# The fixture ships with 2 because the e2e suite runs one actor at a time; a live
# demo cannot afford a suspend/resume between acts.
POOL_REPLICAS=4

ROUTER="127.0.0.1:8080"
# TEST-NET-1, and deliberately unroutable: nothing on the internet answers it, so
# any response at all proves the connection was intercepted and rebuilt as a
# tunnel. A real destination IP would make a success ambiguous -- it could mean
# the nftables REDIRECT never fired and the Actor simply dialed out.
DESTINATION="192.0.2.1:443"

ECHO_HOST="postman-echo.com"
GITHUB_HOST="api.github.com"
UNLISTED_HOST="example.com"    # in sdsmintd's --allow, in no actor's policy

VERBOSE=0
PAUSE=1
WITH_ECHO=0

# ---------------------------------------------------------------------------
# Presentation
# ---------------------------------------------------------------------------

if [[ -t 1 ]] && [[ "${NO_COLOR:-}" == "" ]]; then
  B=$'\033[1m'; DIM=$'\033[2m'; GREEN=$'\033[32m'; RED=$'\033[31m'
  CYAN=$'\033[36m'; R=$'\033[0m'
else
  B=""; DIM=""; GREEN=""; RED=""; CYAN=""; R=""
fi

act_number=0

# act prints the frame for one claim: what we are about to do and what should
# happen. Stating the expected result BEFORE running is the whole difference
# between a demo and a debugging session -- the audience gets to be right.
act() {
  act_number=$((act_number + 1))
  echo
  echo "${B}${CYAN}━━━ Act ${act_number} · ${1} ━━━${R}"
  echo
  echo "  ${2}"
  echo
  # read fails at EOF, which is normal when stdin is not a terminal -- under
  # `set -e` that would end the demo rather than skip the pause.
  [[ "${PAUSE}" == "1" ]] && { printf '  %s[enter]%s' "${DIM}" "${R}"; read -r || true; echo; }
  return 0
}

say()     { echo "  ${1}"; }
verdict() { echo; echo "  ${B}${GREEN}▸ ${1}${R}"; }
nope()    { echo; echo "  ${B}${RED}▸ ${1}${R}"; }
field()   { printf '    %-28s %s\n' "${1}" "${2}"; }
note()    { echo "  ${DIM}${1}${R}"; }
die()     { echo "${RED}error:${R} ${1}" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Driving the probe
# ---------------------------------------------------------------------------

# probe ACTOR SNI PATH -- one trip through the gateway, as that actor.
#
# via=direct is what makes this an Actor test rather than a proxy test: the probe
# dials as if no gateway existed, ateom's nftables rule REDIRECTs it, and atunnel
# builds the tunnel with a certificate the probe never sees. Nothing in the
# workload knows a gateway is involved.
probe() {
  local actor="${1}" sni="${2}" path="${3}" body result
  body=$(printf '{"via":"direct","destination":"%s","sni":"%s","request_path":"%s"}' \
    "${DESTINATION}" "${sni}" "${path}")

  if [[ "${VERBOSE}" == "1" ]]; then
    echo "  ${DIM}POST http://${ROUTER}/probe${R}"
    echo "  ${DIM}Host: ${actor}.${ATESPACE}.actors.resources.substrate.ate.dev${R}"
    echo "  ${DIM}${body}${R}"
    echo
  fi

  # The router routes on Host, not on a path or a header: this is the same way
  # every Actor in the system is reached.
  result=$(curl -sS --max-time 90 -X POST "http://${ROUTER}/probe" \
    -H "Host: ${actor}.${ATESPACE}.actors.resources.substrate.ate.dev" \
    -H 'Content-Type: application/json' \
    -d "${body}") || die "the probe API did not answer -- is the port-forward still up?"

  if [[ "${result}" != "{"* ]]; then
    die "router answered with something that is not a Result: ${result}"
  fi
  if [[ "${VERBOSE}" == "1" ]]; then
    echo "${result}" | jq . | sed 's/^/  /'
    echo
  fi
  echo "${result}"
}

# echo_headers pulls the request headers back out of an echo service's response.
# Empty when the body is not the JSON we expect, which keeps a Cloudflare error
# page from being reported as "no headers arrived".
echo_headers() {
  echo "${1}" | jq -r 'try (.http_body | fromjson | .headers) // empty'
}

# ---------------------------------------------------------------------------
# Acts
# ---------------------------------------------------------------------------

act_zero() {
  act "Nothing up my sleeve" \
"Before anything runs: this workload holds no credentials.
  Two files say so -- the code that builds the request, and the template that
  deploys it."

  say "${B}The request the workload sends${R} ${DIM}(probeapi.go)${R}"
  echo
  sed -n '108,110p' internal/e2e/fixtures/egressprobe/probeapi/probeapi.go | sed 's/^/    /'
  echo
  say "${B}The ActorTemplate that deploys it${R} ${DIM}(egressprobe-actor.yaml.tmpl)${R}"
  echo
  note "    no secretKeyRef, no token env var, no credential volume:"
  echo
  grep -n 'secretKeyRef\|env:\|volumeMounts:' \
    internal/e2e/fixtures/egressprobe/egressprobe-actor.yaml.tmpl \
    | sed 's/^/    /' || say "    ${GREEN}(no matches -- there are none)${R}"

  verdict "The workload cannot authenticate to anything. Keep that in mind."
}

act_echo_bare() {
  act "An Actor with no credential, seen from the far end" \
"${ACTOR_BARE} is allowed to reach ${ECHO_HOST}, and its policy attaches nothing.
  The echo service reports the request exactly as it received it -- so this is
  the baseline: what the workload sent, and only that."

  local result headers
  result=$(probe "${ACTOR_BARE}" "${ECHO_HOST}" "/get")
  headers=$(echo_headers "${result}")
  [[ -n "${headers}" ]] || { nope "no echo body came back"; echo "${result}" | jq .; return; }

  echo "${headers}" | jq -r 'to_entries[] | "    \(.key): \(.value)"' | grep -iv '^ *x-\|cf-\|cdn-' || true
  echo

  if echo "${headers}" | jq -e 'has("authorization")' >/dev/null; then
    nope "An authorization header arrived. It should not have -- check the policy table."
  else
    verdict "No authorization header. The destination saw exactly what the workload sent."
  fi
}

act_echo_inject() {
  act "The same request, from a different Actor" \
"${ACTOR_INJECT} now. Same image, same code, same request, same destination.
  The only thing that changed is which Actor is making it."

  local result headers auth
  result=$(probe "${ACTOR_INJECT}" "${ECHO_HOST}" "/get")
  headers=$(echo_headers "${result}")
  [[ -n "${headers}" ]] || { nope "no echo body came back"; echo "${result}" | jq .; return; }

  echo "${headers}" | jq -r 'to_entries[] | "    \(.key): \(.value)"' | grep -iv '^ *x-\|cf-\|cdn-' || true
  echo

  auth=$(echo "${headers}" | jq -r '.authorization // empty')
  if [[ -z "${auth}" ]]; then
    nope "No authorization header arrived. The injection did not fire."
    return
  fi

  field "authorization" "${B}${auth}${R}"
  echo
  say "That header was added by the gateway. The workload never held that value,"
  say "cannot read it, and has no way to discover it."
  echo

  # A security-minded audience asks this before you finish the sentence, so get
  # ahead of it: proving a credential goes OUT is only half the claim if
  # substrate-internal identity headers leak out alongside it.
  say "${B}And what did not leak:${R}"
  if echo "${headers}" | jq -e 'keys[] | select(startswith("x-ate-") or . == "x-forwarded-client-cert")' >/dev/null 2>&1; then
    nope "substrate-internal headers reached the destination -- extprocd's header hygiene broke."
  else
    field "x-ate-*" "${GREEN}absent${R}"
    field "x-forwarded-client-cert" "${GREEN}absent${R}"
  fi

  verdict "A credential the workload never had, and nothing about substrate, reached the destination."
}

act_github() {
  act "A real third party reacts to it" \
"An echo service will print anything. ${GITHUB_HOST} has an opinion.
  Both Actors ask GitHub the same question -- who am I? -- and GitHub
  distinguishes 'you sent nothing' from 'you sent a token and it is wrong'."

  local bare inject
  bare=$(probe "${ACTOR_BARE}" "${GITHUB_HOST}" "/user")
  inject=$(probe "${ACTOR_INJECT}" "${GITHUB_HOST}" "/user")

  say "${B}${ACTOR_BARE}${R} ${DIM}(no injection)${R}"
  field "status" "$(echo "${bare}" | jq -r '.http_status')"
  field "message" "$(echo "${bare}" | jq -r 'try (.http_body|fromjson.message) // .http_body')"
  field "x-ratelimit-limit" "$(echo "${bare}" | jq -r '.http_headers["X-Ratelimit-Limit"][0] // "(absent)"')"
  echo
  say "${B}${ACTOR_INJECT}${R} ${DIM}(credential injected)${R}"
  field "status" "$(echo "${inject}" | jq -r '.http_status')"
  field "message" "$(echo "${inject}" | jq -r 'try (.http_body|fromjson.message) // .http_body')"
  field "x-ratelimit-limit" "$(echo "${inject}" | jq -r '.http_headers["X-Ratelimit-Limit"][0] // "(absent)"')"
  echo

  note "Both are 401 because the demo ships a deliberately invalid token -- there is"
  note "no live secret anywhere in this repo. Two things separate them."
  note ""
  note "The wording. \"Requires authentication\" is GitHub saying nothing arrived;"
  note "\"Bad credentials\" is GitHub saying a bearer token arrived and was rejected."
  note ""
  note "The rate limit. An anonymous request gets a bucket -- 60/hour by source IP."
  note "A rejected credential gets no bucket at all: GitHub cannot attribute it to"
  note "an account, and will not call it anonymous either. The header disappears."

  verdict "GitHub confirms independently that a credential reached it -- and only for one Actor."
}

act_denied_host() {
  act "The credential is not a blank cheque" \
"${ACTOR_INJECT} can reach ${GITHUB_HOST}. Here it tries ${UNLISTED_HOST}, which
  the gateway will happily mint a certificate for but no policy permits.
  Injection and destination control are the same decision, not two features."

  local result
  result=$(probe "${ACTOR_INJECT}" "${UNLISTED_HOST}" "/")

  field "status" "$(echo "${result}" | jq -r '.http_status')"
  field "body"   "$(echo "${result}" | jq -r '.http_body' | head -1)"
  echo

  if echo "${result}" | jq -e '.http_body | test("egress denied")' >/dev/null 2>&1; then
    verdict "Refused by the gateway, naming the policy that refused it."
  else
    nope "That 403 did not come from the gateway -- read the body before believing it."
  fi
}

act_quarantined() {
  act "The floor" \
"${ACTOR_DENIED}'s policy is deny-all. Same image again, same request.
  Watch where this one fails: earlier than every previous act."

  local result
  result=$(probe "${ACTOR_DENIED}" "${GITHUB_HOST}" "/user")

  field "connected"      "$(echo "${result}" | jq -r '.connected')"
  field "handshake_ok"   "$(echo "${result}" | jq -r '.handshake_ok')"
  field "handshake_error" "$(echo "${result}" | jq -r '.handshake_error // "none"')"
  echo

  note "connected: true is not egress. The REDIRECT is local, so the socket comes up"
  note "inside the sandbox before atunnel has spoken to the gateway at all. The"
  note "gateway then refuses, atunnel closes, and the TLS handshake dies on the reset."
  note "Note the peer address: 192.0.2.1 is unroutable, so that reset came from"
  note "inside the cluster. Nothing was ever dialed."

  if [[ "$(echo "${result}" | jq -r '.handshake_ok')" == "false" ]]; then
    verdict "No bytes left the sandbox. The same mechanism that grants also denies."
  else
    nope "The handshake completed. A deny-all Actor reached the internet -- stop the demo."
  fi
}

closing() {
  echo
  echo "${B}${CYAN}━━━ What the customer just saw ━━━${R}"
  echo
  say "One image. One codebase. No credentials anywhere in the workload."
  say "Four different outcomes, decided entirely by ${B}which Actor made the call${R}:"
  echo
  # Plain ASCII in the left column: printf pads by byte count, so a multibyte
  # arrow here silently shifts the whole table one place left.
  field "${ACTOR_BARE}"    "reaches the allowlist, bare"
  field "${ACTOR_INJECT}"  "reaches the allowlist, with a credential it never held"
  field "  ...to ${UNLISTED_HOST}" "refused, by name"
  field "${ACTOR_DENIED}"  "never leaves the sandbox"
  echo
  say "There is no secret to steal from the workload, no config naming an identity,"
  say "and no request parameter that selects one."
  echo
}

# ---------------------------------------------------------------------------
# Setup and teardown
# ---------------------------------------------------------------------------

kate() { go run ./cmd/kubectl-ate "$@"; }

setup() {
  echo "${B}Setting up${R}"
  command -v jq >/dev/null || die "jq is required"

  kubectl get ns "${TEMPLATE_NS}" >/dev/null 2>&1 \
    || die "${TEMPLATE_NS} not found -- run: hack/install-ate.sh --deploy-demo-egress"

  echo "  scaling the worker pool to ${POOL_REPLICAS} (three actors on stage at once)"
  kubectl patch workerpool "${TEMPLATE_NAME}" -n "${TEMPLATE_NS}" --type=merge \
    -p "{\"spec\":{\"replicas\":${POOL_REPLICAS}}}"

  kate get atespaces "${ATESPACE}" >/dev/null 2>&1 || kate create atespace "${ATESPACE}"

  for actor in "${ACTOR_BARE}" "${ACTOR_INJECT}" "${ACTOR_DENIED}"; do
    echo "  ${actor}"
    kate create actor "${actor}" -t "${TEMPLATE_NS}/${TEMPLATE_NAME}" -a "${ATESPACE}" 2>/dev/null || true
    kate resume actor "${actor}" -a "${ATESPACE}"
  done

  echo
  echo "  Start the port-forward in another shell, then run the demo:"
  echo "    ${DIM}kubectl -n ate-system port-forward svc/atenet-router 8080:80${R}"
  echo "    ${DIM}demos/egress/demo.sh${R}"
}

teardown() {
  echo "${B}Tearing down${R}"
  for actor in "${ACTOR_BARE}" "${ACTOR_INJECT}" "${ACTOR_DENIED}"; do
    kate suspend actor "${actor}" -a "${ATESPACE}" 2>/dev/null || true
    kate delete  actor "${actor}" -a "${ATESPACE}" 2>/dev/null || true
  done
  # Back to what the e2e fixture expects, so a later suite run is not surprised.
  kubectl patch workerpool "${TEMPLATE_NAME}" -n "${TEMPLATE_NS}" --type=merge \
    -p '{"spec":{"replicas":2}}'
}

# preflight fails loudly and early rather than partway through an act, which is a
# bad time to discover an Actor never resumed.
#
# Reaching the router is not enough to check: it answers for an Actor that does
# not exist too, just with a different body. Every Actor gets probed, because a
# demo that dies at Act 5 has already wasted the audience's time.
preflight() {
  command -v jq >/dev/null || die "jq is required"

  local actors=("${ACTOR_BARE}" "${ACTOR_INJECT}" "${ACTOR_DENIED}") actor answer
  for actor in "${actors[@]}"; do
    answer=$(curl -sS --max-time 10 "http://${ROUTER}/healthz" \
      -H "Host: ${actor}.${ATESPACE}.actors.resources.substrate.ate.dev" 2>&1) \
      || die "no answer from ${ROUTER}
  Start it with: kubectl -n ate-system port-forward svc/atenet-router 8080:80"

    case "${answer}" in
      *"not found"*)
        die "Actor ${ATESPACE}/${actor} does not exist.
  Create all three with: demos/egress/demo.sh --setup" ;;
      *"no healthy upstream"*|*"upstream connect error"*)
        die "Actor ${ATESPACE}/${actor} exists but is not running.
  Resume it with: go run ./cmd/kubectl-ate resume actor ${actor} -a ${ATESPACE}" ;;
    esac
  done
}

usage() {
  cat <<EOF
demos/egress/demo.sh -- a walkthrough of the egress path

  --setup        create the three Actors and scale the pool
  --teardown     delete them and restore the pool
  --with-echo    include the echo-service acts (needs one-time config; see README)
  --verbose      print each request and the full Result
  --no-pause     do not wait for [enter] between acts (for recording)
  --port PORT    router port-forward, default 8080
EOF
}

main() {
  while [[ $# -gt 0 ]]; do
    case "${1}" in
      --setup)     setup; exit 0 ;;
      --teardown)  teardown; exit 0 ;;
      --with-echo) WITH_ECHO=1 ;;
      --verbose)   VERBOSE=1 ;;
      --no-pause)  PAUSE=0 ;;
      --port)      ROUTER="127.0.0.1:${2}"; shift ;;
      -h|--help)   usage; exit 0 ;;
      *)           usage; die "unknown flag ${1}" ;;
    esac
    shift
  done

  preflight

  echo
  echo "${B}The workload holds no credentials.${R}"
  echo "${DIM}What an Actor can reach is decided by who it is, not by what it carries.${R}"

  act_zero
  if [[ "${WITH_ECHO}" == "1" ]]; then
    act_echo_bare
    act_echo_inject
  else
    echo
    note "Skipping the echo acts -- the strongest ones. See README.md § Seeing the"
    note "header, then re-run with --with-echo."
  fi
  act_github
  act_denied_host
  act_quarantined
  closing
}

main "$@"
