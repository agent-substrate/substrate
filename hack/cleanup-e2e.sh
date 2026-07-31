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

set -o errexit -o nounset -o pipefail

# Deletes the namespaces a failed e2e run left behind.
#
# A failed suite keeps its namespaces so the worker pods — and the ateom logs
# inside them, which is where an actor failure is actually explained — survive
# long enough to read (see RetainNamespaces in internal/e2e/namespace.go).
# Nothing reclaims them afterwards, and each holds a WorkerPool's worth of
# running pods, so run this once the logs have served their purpose.
#
# This deletes EVERY e2e namespace in the cluster, so don't run it while a suite
# is running.
#
# Env: KUBECTL_CONTEXT (optional) kube context to target; KIND_CLUSTER_NAME
# (optional) shorthand for the matching kind context.

if [[ -z "${KUBECTL_CONTEXT:-}" && -n "${KIND_CLUSTER_NAME:-}" ]]; then
  KUBECTL_CONTEXT="kind-${KIND_CLUSTER_NAME}"
fi
kubectl_args=()
if [[ -n "${KUBECTL_CONTEXT:-}" ]]; then
  kubectl_args+=("--context=${KUBECTL_CONTEXT}")
fi

# The label CreateNamespace stamps on every namespace the suites create.
selector="ate.dev/e2e"

leftover="$(kubectl "${kubectl_args[@]}" get namespaces -l "${selector}" -o name)"
if [[ -z "${leftover}" ]]; then
  echo "No leftover e2e namespaces."
  exit 0
fi

echo "Deleting leftover e2e namespaces:"
echo "${leftover}"
kubectl "${kubectl_args[@]}" delete namespaces -l "${selector}" --ignore-not-found
