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

ATE_DEMOS+=(demo-parking) # register demo-parking

demo-parking_cmdline() {
  case "${1}" in
    --deploy-demo-parking) demo-parking_deploy ;;
    --delete-demo-parking) demo-parking_delete ;;
    *)
      return 1
      ;;
  esac
  return 0
}

demo-parking_deploy() {
  log_step "demo-parking_deploy"
  ensure_crds
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" demos/parking/parking.yaml.tmpl \
    | run_ko apply -f -

  # Wait for the demo to be fully ready before returning: the small WorkerPool
  # must be rolled out and the ActorTemplate's golden snapshot built.
  log_step "Waiting for parking demo to be ready..."
  run_kubectl rollout status deployment/parking -n ate-demo-parking --timeout=300s
  run_kubectl wait --for=condition=Ready actortemplate/parking -n ate-demo-parking --timeout=300s
}

demo-parking_delete() {
  log_step "demo-parking_delete"
  delete_demo_actors ate-demo-parking parking
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" demos/parking/parking.yaml.tmpl \
    | run_kubectl delete --ignore-not-found -f -
}
