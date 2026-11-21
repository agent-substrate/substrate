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

# Moves the control plane onto two infra nodes and keeps workers off them, so
# the measured nodes run only workers and the DaemonSets every node runs.
#
# - Labels the two nodes bench-role=infra.
# - Pins the ate-system Deployments, Postgres, the podcertificate controller
#   and the CSI controller to them with a nodeSelector.
# - Gives the benchmark WorkerPool a node affinity that avoids them.
#
# ate-setup deploy keeps these pins, because it only changes the fields it
# sets. --undo removes the pins and the node labels.
#
# Usage: isolate_infra.sh <infra-node-1> <infra-node-2>
#        isolate_infra.sh --undo
# Env: CSI_NAMESPACE, CSI_CONTROLLER (default gcp-filestore-csi-driver and
# gcp-filestore-csi-controller). WORKER_POOL, WORKER_NAMESPACE (default
# benchmark-worker and benchmark-workloads).

set -o errexit -o nounset -o pipefail

usage="usage: isolate_infra.sh <infra-node-1> <infra-node-2> | --undo"
csi_ns="${CSI_NAMESPACE:-gcp-filestore-csi-driver}"
csi_controller="${CSI_CONTROLLER:-gcp-filestore-csi-controller}"
pool="${WORKER_POOL:-benchmark-worker}"
pool_ns="${WORKER_NAMESPACE:-benchmark-workloads}"
podcert_ns=podcertificate-controller-system
deploys=(ate-api-server ate-controller ate-idp-server atenet-router atenet-egress)

if [[ "${1:?${usage}}" == "--undo" ]]; then
  # A null value in a merge patch deletes the key.
  sel='{"spec":{"template":{"spec":{"nodeSelector":{"bench-role":null}}}}}'
else
  n1="${1}"
  n2="${2:?${usage}}"
  sel='{"spec":{"template":{"spec":{"nodeSelector":{"bench-role":"infra"}}}}}'
  kubectl label node "${n1}" "${n2}" bench-role=infra --overwrite
fi

for d in "${deploys[@]}"; do
  kubectl patch deploy "${d}" -n ate-system --type merge -p "${sel}"
done
kubectl patch statefulset postgres -n ate-system --type merge -p "${sel}"
kubectl patch deploy podcertificate-controller -n "${podcert_ns}" --type merge -p "${sel}"
kubectl patch deploy "${csi_controller}" -n "${csi_ns}" --type merge -p "${sel}"
if [[ "${1}" != "--undo" ]]; then
  kubectl patch workerpool "${pool}" -n "${pool_ns}" --type merge -p '{"spec":{"template":{
    "nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[
      {"matchExpressions":[{"key":"bench-role","operator":"NotIn","values":["infra"]}]}]}}}}}'
fi

for d in "${deploys[@]}"; do
  kubectl rollout status deploy "${d}" -n ate-system --timeout=300s
done
kubectl rollout status statefulset postgres -n ate-system --timeout=300s
kubectl rollout status deploy podcertificate-controller -n "${podcert_ns}" --timeout=300s
kubectl rollout status deploy "${csi_controller}" -n "${csi_ns}" --timeout=300s

if [[ "${1}" == "--undo" ]]; then
  kubectl label nodes -l bench-role=infra bench-role-
fi
