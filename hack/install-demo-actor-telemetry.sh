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

ATE_DEMOS+=(demo-actor-telemetry) # register demo-actor-telemetry

demo-actor-telemetry_cmdline() {
  case "${1}" in
    --deploy-demo-actor-telemetry) demo-actor-telemetry_deploy ;;
    --delete-demo-actor-telemetry) demo-actor-telemetry_delete ;;
    *)
      return 1
      ;;
  esac
  return 0
}

demo-actor-telemetry_deploy() {
  log_step "demo-actor-telemetry_deploy"
  ensure_crds
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" demos/actor-telemetry/actor-telemetry.yaml.tmpl \
    | run_ko apply -f -

  log_step "Waiting for actor-telemetry demo to be ready..."
  run_kubectl rollout status deployment/actor-telemetry -n ate-demo-actor-telemetry --timeout=300s
  run_kubectl wait --for=condition=Ready actortemplate/actor-telemetry -n ate-demo-actor-telemetry --timeout=300s
}

demo-actor-telemetry_delete() {
  log_step "demo-actor-telemetry_delete"
  delete_demo_actors ate-demo-actor-telemetry actor-telemetry
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" demos/actor-telemetry/actor-telemetry.yaml.tmpl \
    | run_kubectl delete --ignore-not-found -f -
}
