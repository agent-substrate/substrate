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
#
# Installs or deletes the cluster-wide micro-VM (kata + cloud-hypervisor)
# dependencies.
#
# This is a translation shim: cmd/ate-setup carries out the action (`deploy
# microvm-deps` / `delete microvm-deps`), and this script only maps the
# historical `--install` and `--delete` flags onto those commands so existing
# scripts and CI jobs keep working unchanged.
#
# The environment variables the installer reads -- BUCKET_NAME, KUBECTL_CONTEXT,
# PROJECT_ID, KO_DEFAULTPLATFORMS, ARCH, OUT, ATE_INSTALL_KIND, NO_DEV_ENV --
# and .ate-dev-env.sh are read by ate-setup directly.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

usage() {
  cat <<EOF
Usage: $0 (--install | --delete)

Options:
  --install   Assemble + stage micro-VM assets and apply the cluster-wide
              microvm SandboxConfig.
  --delete    Remove the microvm SandboxConfig from the cluster.
              (Bucket contents are left alone.)
  -h, --help  Show this message.
EOF
}

action=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --install) action="deploy" ;;
    --delete)  action="delete" ;;
    -h|--help) usage; exit 0   ;;
    *) echo "Error: unknown argument $1" >&2; usage; exit 1 ;;
  esac
  shift
done

if [[ -z "${action}" ]]; then
  usage
  exit 1
fi

go run ./cmd/ate-setup "${action}" microvm-deps
