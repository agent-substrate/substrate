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
  deploy_substrate_demo render_demo_manifest \
    demos/jupyter/jupyter.yaml.tmpl ate-demo-jupyter jupyter 300 \
    demos/jupyter/jupyter-template.yaml.tmpl jupyter
}

demo-jupyter_delete() {
  log_step "demo-jupyter_delete"
  delete_substrate_demo render_demo_manifest \
    demos/jupyter/jupyter.yaml.tmpl ate-demo-jupyter jupyter
}
