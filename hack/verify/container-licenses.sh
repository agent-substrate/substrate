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

# Verifies that no Dockerfile in this repository bakes a copyleft-licensed base
# image into an image we build and publish.
#
# The rule this encodes:
#
#   * Referencing an image at runtime (a `image:` field in a Kubernetes
#     manifest, a `docker run` in a dev script, a test fixture string) is NOT
#     distribution. The cluster pulls the image straight from its upstream
#     registry; we never convey the binary, so no GPL source-offer obligation
#     attaches to us. Those references are intentionally NOT checked here.
#
#   * Baking an image into one of ours via `FROM` or `COPY --from=` and then
#     publishing the result DOES make us a distributor of whatever that base
#     contains. For GPL-2.0 bases that means owing complete corresponding
#     source or a written offer, and it cannot be undone once a release image
#     is public. That is the path this script gates.
#
# The denylist below is a policy list, not a derived legal truth. Distro bases
# such as debian and ubuntu also ship GPL userland (bash, coreutils) and are
# deliberately absent: they are redistributed verbatim from upstream with the
# distro's own corresponding-source availability, which is the common-practice
# carve-out. Adjust the list to match whatever the project's license policy
# actually says.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Base images that may not be baked into an image we build. Keep the reason
# attached to each entry so the next person can re-evaluate it.
DENY_IMAGES=(
  busybox # GPL-2.0-only.
  alpine  # Bundles busybox, so it carries the same GPL-2.0-only obligation.
)

# Denied references found so far, as tab-separated file/line/ref/context
# records. Kept as data rather than printed on the spot so that the self-test
# below can assert on exactly what the parser matched.
FINDINGS=()

# Reduces an image reference to its bare repository name so that
# `docker.io/library/busybox:1.36` and `busybox@sha256:...` both compare equal
# to `busybox`.
normalize_image() {
  local ref="$1" last prefix

  ref="${ref%@*}" # Strip any digest.

  # Strip the tag from the final path component only, so a registry host with
  # a port (localhost:5000/foo) survives intact.
  last="${ref##*/}"
  prefix="${ref%"${last}"}"
  last="${last%%:*}"
  ref="${prefix}${last}"

  ref="${ref#docker.io/}"
  ref="${ref#index.docker.io/}"
  ref="${ref#library/}"

  printf '%s' "${ref}"
}

# Reports an image reference if it resolves to a denied base image.
report_if_denied() {
  local file="$1" lineno="$2" ref="$3" context="$4"
  local norm deny

  # Build args cannot be resolved statically; `scratch` is not a real image.
  if [[ "${ref}" == *'$'* || "${ref}" == "scratch" ]]; then
    return 0
  fi

  norm="$(normalize_image "${ref}")"

  for deny in "${DENY_IMAGES[@]}"; do
    if [[ "${norm}" == "${deny}" ]]; then
      FINDINGS+=("${file}"$'\t'"${lineno}"$'\t'"${ref}"$'\t'"${context}")
      return 0
    fi
  done
}

# Renders the recorded findings for human consumption.
print_findings() {
  local finding file lineno ref context

  for finding in "${FINDINGS[@]}"; do
    IFS=$'\t' read -r file lineno ref context <<<"${finding}"
    echo "${file}:${lineno}: disallowed base image '${ref}'"
    echo "    ${context}"
  done
}

# Scans a single Dockerfile for FROM and COPY --from= references.
check_dockerfile() {
  local file="$1"
  local -A stages=()
  local lineno=0 line kw img src token i
  local -a tok=() lines=()

  mapfile -t lines <"${file}"
  if [[ ${#lines[@]} -eq 0 ]]; then
    return 0
  fi

  for line in "${lines[@]}"; do
    lineno=$((lineno + 1))

    line="${line%$'\r'}"
    line="${line#"${line%%[![:space:]]*}"}" # Trim leading whitespace.

    if [[ -z "${line}" || "${line}" == '#'* ]]; then
      continue
    fi

    # shellcheck disable=SC2206 # Word splitting on Dockerfile tokens is intended.
    tok=(${line})
    kw="${tok[0],,}"

    if [[ "${kw}" == "from" ]]; then
      # Skip flags such as --platform=linux/amd64.
      i=1
      while [[ ${i} -lt ${#tok[@]} && "${tok[${i}]}" == --* ]]; do
        i=$((i + 1))
      done
      if [[ ${i} -ge ${#tok[@]} ]]; then
        continue
      fi

      img="${tok[${i}]}"

      # Record the stage alias from `FROM <img> AS <name>` so that a later
      # `COPY --from=<name>` is understood as a stage, not an image.
      if [[ $((i + 2)) -lt ${#tok[@]} && "${tok[$((i + 1))],,}" == "as" ]]; then
        stages["${tok[$((i + 2))],,}"]=1
      fi

      report_if_denied "${file}" "${lineno}" "${img}" "${line}"

    elif [[ "${kw}" == "copy" && ${#tok[@]} -gt 1 ]]; then
      for token in "${tok[@]:1}"; do
        if [[ "${token}" != --from=* ]]; then
          continue
        fi

        src="${token#--from=}"

        # A named earlier stage or a numeric stage index is not an image.
        if [[ -n "${stages[${src,,}]:-}" || "${src}" =~ ^[0-9]+$ ]]; then
          continue
        fi

        report_if_denied "${file}" "${lineno}" "${src}" "${line}"
      done
    fi
  done
}

# Joins its arguments one per line, tolerating an empty argument list.
join_lines() {
  if [[ $# -eq 0 ]]; then
    printf ''
  else
    printf '%s\n' "$@"
  fi
}

# Exercises the parser against a fixture with a known answer.
#
# This exists because the failure mode of this script is silent: if an edit
# breaks the parsing so that nothing ever matches, the check keeps exiting 0
# and reporting a clean tree forever. A gate that has quietly stopped gating is
# worse than no gate at all, so every run proves the detector still detects
# before anyone trusts a clean result.
#
# The fixture is written to a temporary file rather than checked in, because a
# real Dockerfile under testdata/ would be picked up by the repository scan
# below and would need an exclusion -- and that exclusion would become a place
# to hide a Dockerfile from this check.
self_test() {
  local fixture finding lineno ref got_joined want_joined
  local -a got=()

  fixture="$(mktemp)"
  cat >"${fixture}" <<'FIXTURE'
# FROM busybox here is a comment and must not match.
FROM busybox:1.36
FROM --platform=linux/amd64 docker.io/library/alpine:3.19
FROM golang:1.26-bookworm AS build
COPY --from=build /a /b
COPY --from=busybox@sha256:0123456789 /bin/sh /bin/sh
COPY --from=0 /c /d
FROM scratch
FROM ${BASE_IMAGE}
FROM localhost:5000/mycorp/busybox-tools:v1
FROM index.docker.io/library/busybox AS final
FROM golang:1.26-bookworm AS alpine
COPY --from=alpine /e /f
FIXTURE

  FINDINGS=()
  check_dockerfile "${fixture}"
  rm -f "${fixture}"

  for finding in "${FINDINGS[@]+"${FINDINGS[@]}"}"; do
    IFS=$'\t' read -r _ lineno ref _ <<<"${finding}"
    got+=("${lineno} ${ref}")
  done

  # Denied bases on lines 2, 3, 6 and 11. Everything else in the fixture must
  # be skipped: the comment, the `build` stage alias, the numeric stage index,
  # `scratch`, the unresolvable build arg, and mycorp/busybox-tools, whose name
  # merely contains a denied one.
  #
  # The last two fixture lines name a stage `alpine` and then copy from it.
  # That is what makes losing stage tracking detectable: an alias that is not
  # itself a denied name would simply normalize to a harmless string and the
  # regression would pass unnoticed.
  want_joined="$(join_lines \
    "2 busybox:1.36" \
    "3 docker.io/library/alpine:3.19" \
    "6 busybox@sha256:0123456789" \
    "11 index.docker.io/library/busybox")"
  got_joined="$(join_lines "${got[@]+"${got[@]}"}")"

  if [[ "${got_joined}" != "${want_joined}" ]]; then
    cat >&2 <<EOF
$0: SELF-TEST FAILED -- the Dockerfile parser no longer behaves as expected,
so a clean result from this check cannot be trusted. Fix the parser before
relying on it.

want:
${want_joined}
got:
${got_joined}
EOF
    exit 1
  fi

  FINDINGS=()
}

self_test

mapfile -t DOCKERFILES < <(
  git ls-files -- '*Dockerfile' '*Dockerfile.*' '*.dockerfile' ':(exclude)vendor/*' | sort -u
)

if [[ ${#DOCKERFILES[@]} -eq 0 ]]; then
  echo "No Dockerfiles found; nothing to verify."
  exit 0
fi

for DOCKERFILE in "${DOCKERFILES[@]}"; do
  check_dockerfile "${DOCKERFILE}"
done

if [[ ${#FINDINGS[@]} -ne 0 ]]; then
  print_findings
  cat <<EOF

Found ${#FINDINGS[@]} disallowed base image reference(s).

Publishing an image built on a copyleft base makes this project a distributor
of that base's binaries, which triggers obligations we cannot satisfy after a
release is public. Use a permissively licensed or distroless base instead.

Referencing such an image at runtime (for example an 'image:' field in a
manifest) is fine and is not flagged by this check. If a base image genuinely
needs an exception, get it cleared by whoever owns license policy for this
project and then edit DENY_IMAGES in $0.
EOF
  exit 1
fi

echo "Checked ${#DOCKERFILES[@]} Dockerfile(s) (detector self-test passed);" \
  "no disallowed base images found."
