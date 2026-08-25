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

ATE_DEMOS+=(demo-jupyter) # register demo-jupyter

demo-jupyter_usage() {
  echo "  --deploy-demo-jupyter                         Deploy demo-jupyter"
}

demo-jupyter_cmdline() {
  case "${1}" in
    --deploy-demo-jupyter) demo-jupyter_deploy ;;
    --delete-demo-jupyter) demo-jupyter_delete ;;
    *)
      return 1
      ;;
  esac
  return 0
}

demo-jupyter_deploy() {
  log_step "demo-jupyter_deploy"
  ensure_crds

  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      demos/jupyter/jupyter.yaml.tmpl \
    | run_ko apply -f -

  log_step "Waiting for jupyter demo to be ready..."
  run_kubectl wait --for=condition=Ready actortemplate/jupyter -n ate-demo-jupyter --timeout=300s
}

demo-jupyter_delete() {
  log_step "demo-jupyter_delete"
  delete_demo_actors ate-demo-jupyter jupyter
  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      demos/jupyter/jupyter.yaml.tmpl \
    | run_kubectl delete --ignore-not-found -f -
}
