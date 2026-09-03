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

# The demo's two templates live in two different atespaces to show that pool
# selection is atespace-agnostic, so this composes the substrate helpers
# itself instead of using deploy_substrate_demo's one-atespace shape.
demo-multi-template_deploy() {
  log_step "demo-multi-template_deploy"
  ensure_crds
  render_demo_manifest demos/multi-template/multi-template.yaml.tmpl \
    | run_ko apply -f -

  log_step "Waiting for the shared-pool worker pool rollout..."
  wait_for_pool_rollout_fatal shared-pool ate-demo-multi-template-pool

  ensure_atespace ate-demo-multi-template-counter
  ensure_atespace ate-demo-multi-template-fspersist
  create_demo_actor_template render_demo_manifest \
    demos/multi-template/counter-template.yaml.tmpl \
    ate-demo-multi-template-counter counter
  create_demo_actor_template render_demo_manifest \
    demos/multi-template/fspersist-template.yaml.tmpl \
    ate-demo-multi-template-fspersist fspersist
}

demo-multi-template_delete() {
  log_step "demo-multi-template_delete"
  local atespace
  delete_demo_actor_template ate-demo-multi-template-counter counter
  delete_demo_actor_template ate-demo-multi-template-fspersist fspersist
  for atespace in ate-demo-multi-template-counter ate-demo-multi-template-fspersist; do
    run_kubectl_ate delete atespace "${atespace}" 2>/dev/null \
      || log_step "atespace ${atespace} not deleted (may not exist or is not empty)"
  done
  render_demo_manifest demos/multi-template/multi-template.yaml.tmpl \
    | run_kubectl delete --ignore-not-found -f -
}
