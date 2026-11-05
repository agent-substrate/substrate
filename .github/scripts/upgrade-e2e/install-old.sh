#!/usr/bin/env bash

# Copyright 2026 The Agent Substrate Authors
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

# Installs the release to upgrade from with its own installer, so the cluster
# starts out as that release left it, plus the counter demo of SANDBOX
# (gvisor or microvm) that the suite's actors run on.
#
#   (cd <old checkout> && <new checkout>/.github/scripts/upgrade-e2e/install-old.sh SANDBOX)
#
# Runs in the checkout of the old release. A release whose deploy ate-system
# does not install the microVM dependencies has
# hack/run-microvm-demo-kind.sh, which installs them, the control plane and
# counter-microvm.
#
# Used by .github/workflows/upgrade-e2e.yaml.

set -o errexit -o nounset -o pipefail

sandbox="${1:?usage: install-old.sh gvisor|microvm}"

case "${sandbox}" in
gvisor)
  demo=counter
  ;;
microvm)
  if [[ -x hack/run-microvm-demo-kind.sh ]]; then
    exec hack/run-microvm-demo-kind.sh
  fi
  demo=counter-microvm
  ;;
*)
  echo "unknown sandbox ${sandbox}, want gvisor or microvm" >&2
  exit 1
  ;;
esac

go run ./cmd/ate-setup --kind deploy ate-system
go run ./cmd/ate-setup --kind deploy demo "${demo}"
