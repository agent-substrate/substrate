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

# Checks the metric registry with OpenTelemetry Weaver. Weaver reads
# docs/metrics/registry/ and makes sure that each group, each attribute and
# each reference is correct.
#
# The script uses a local `weaver` binary if there is one. If there is none, it
# uses the official image, like hack/verify/shellcheck.sh does.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

REGISTRY_DIR="docs/metrics/registry"

# The version of weaver that this script accepts. It gates the local binary,
# and it names the release to install. The digest below is what CI actually
# pulls; keep this in sync with WEAVER_IMAGE.
WEAVER_VERSION="v0.25.1"
WEAVER_IMAGE="docker.io/otel/weaver:v0.25.1@sha256:9ad46ca9cd4fa5974b121f886aa3e9946a8ef8ea905001a96c018d21f9db87ca"

# Allow overriding the docker CLI, as hack/third_party/kubernetes does.
DOCKER="${DOCKER:-docker}"

run_weaver() {
  if command -v weaver >/dev/null 2>&1; then
    # Use the local binary only if it is the version that CI uses. A different
    # version can give a different answer, and then a green run on a laptop
    # means nothing.
    local local_version
    local_version="$(weaver --version | awk '{print $NF}')"
    if [[ "v${local_version}" != "${WEAVER_VERSION}" ]]; then
      cat >&2 <<EOF
FAIL: the local weaver is v${local_version}, and CI uses ${WEAVER_VERSION}.

  Install ${WEAVER_VERSION} from
  https://github.com/open-telemetry/weaver/releases/tag/${WEAVER_VERSION}
  or remove weaver from your PATH to use the pinned image.
EOF
      # Exit here, and do not return. The registry is not the problem, thus
      # the message at the end of this script would name the wrong file.
      exit 1
    fi
    echo "Using the local weaver ${WEAVER_VERSION} binary."
    weaver "$@"
    return
  fi

  if ! command -v "${DOCKER}" >/dev/null 2>&1; then
    echo "FAIL: this script needs either the weaver binary or ${DOCKER}." >&2
    echo "Install Weaver from https://github.com/open-telemetry/weaver/releases" >&2
    exit 1
  fi

  echo "Using the weaver ${WEAVER_VERSION} docker image."
  "${DOCKER}" run \
    --rm \
    --user "$(id -u):$(id -g)" \
    --volume "${ROOT}:/workspace:ro" \
    --workdir /workspace \
    "${WEAVER_IMAGE}" \
    "$@"
}

# --future makes the checks that Weaver plans to make default give an error and
# not a warning. A duplicate metric_name is one of them: without the flag the
# command prints a warning and ends with 0.
if ! run_weaver registry check --registry "${REGISTRY_DIR}" --future; then
  cat >&2 <<EOF

FAIL: the metric registry is not correct.

  Change ${REGISTRY_DIR}/metrics.yaml and run this script again.

  The rules that Weaver cannot hold are in docs/metrics/substrate.yaml. Keep
  that file beside the registry directory, and not in it. Weaver refuses a
  registry file that has a top-level key other than groups or imports.
EOF
  exit 1
fi
