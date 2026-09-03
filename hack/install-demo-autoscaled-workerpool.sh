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
# This is sourced as part of install-ate.sh. Do not run directly.

# This demo is kind-only: it ships its own prometheus-adapter and a kind
# specific HPA.
if [[ "${ATE_INSTALL_KIND:-false}" == "true" ]]; then
  ATE_DEMOS+=(demo-autoscaled-workerpool) # register demo-autoscaled-workerpool
fi

demo-autoscaled-workerpool_cmdline() {
  case "${1}" in
    --deploy-demo-autoscaled-workerpool) demo-autoscaled-workerpool_deploy ;;
    --delete-demo-autoscaled-workerpool) demo-autoscaled-workerpool_delete ;;
    *)
      return 1
      ;;
  esac
  return 0
}

demo-autoscaled-workerpool_deploy() {
  log_step "demo-autoscaled-workerpool_deploy"
  if [[ "${ATE_INSTALL_KIND:-false}" == "false" ]]; then
    echo "Error: --deploy-demo-autoscaled-workerpool is not supported on GKE yet"  >&2
    exit 1
  fi

  # Deploys the pool, then creates the actor template and waits for its
  # golden snapshot.
  deploy_substrate_demo render_demo_manifest \
    demos/autoscaled-workerpool/autoscaled-workerpool.yaml.tmpl \
    ate-demo-autoscaled-workerpool counter 300 \
    demos/autoscaled-workerpool/autoscaled-workerpool-template.yaml.tmpl counter

  log_step "Deploying prometheus-adapter and HPA for kind..."
  run_kubectl apply -f demos/autoscaled-workerpool/prometheus-adapter.yaml
  run_kubectl rollout status deployment/prometheus-adapter -n ate-demo-autoscaled-workerpool --timeout=120s
  run_kubectl apply -f demos/autoscaled-workerpool/hpa-kind.yaml
}

demo-autoscaled-workerpool_delete() {
  log_step "demo-autoscaled-workerpool_delete"
  if [[ "${ATE_INSTALL_KIND:-false}" != "true" ]]; then
    echo "Error: --delete-demo-autoscaled-workerpool is not supported on GKE" >&2
    exit 1
  fi

  # The HPA goes first so it cannot scale the pool back up while the
  # workload is being removed.
  run_kubectl delete --ignore-not-found -f demos/autoscaled-workerpool/hpa-kind.yaml
  run_kubectl delete --ignore-not-found -f demos/autoscaled-workerpool/prometheus-adapter.yaml

  delete_substrate_demo render_demo_manifest \
    demos/autoscaled-workerpool/autoscaled-workerpool.yaml.tmpl \
    ate-demo-autoscaled-workerpool counter
}
