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

ATE_DEMOS+=(demo-multi-template) # register demo-multi-template

demo-multi-template_cmdline() {
  case "${1}" in
    --deploy-demo-multi-template) demo-multi-template_deploy ;;
    --delete-demo-multi-template) demo-multi-template_delete ;;
    *)
      return 1
      ;;
  esac
  return 0
}

demo-multi-template_deploy() {
  log_step "demo-multi-template_deploy"
  ensure_crds
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" demos/multi-template/multi-template.yaml.tmpl \
    | run_ko apply -f -

  # Wait for both ActorTemplates to be ready before returning.
  log_step "Waiting for multi-template demo to be ready..."
  run_kubectl rollout status deployment/shared-pool -n ate-demo-multi-template-pool --timeout=300s
  run_kubectl wait --for=condition=Ready actortemplate/counter -n ate-demo-multi-template-counter --timeout=300s
  run_kubectl wait --for=condition=Ready actortemplate/fspersist -n ate-demo-multi-template-fspersist --timeout=300s
}

demo-multi-template_delete() {
  log_step "demo-multi-template_delete"
  delete_demo_actors \
    ate-demo-multi-template-counter counter \
    ate-demo-multi-template-fspersist fspersist
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" demos/multi-template/multi-template.yaml.tmpl \
    | run_kubectl delete --ignore-not-found -f -
}
