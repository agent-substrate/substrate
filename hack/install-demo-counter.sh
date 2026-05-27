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

ATE_DEMOS+=(demo-counter) # register demo-counter

demo-counter_cmdline() {
  case "${1}" in
    --deploy-demo-counter) demo-counter_deploy ;;
    --delete-demo-counter) demo-counter_delete ;;
    *)
      return 1
      ;;
  esac
  return 0
}

demo-counter_deploy() {
  log_step "demo-counter_deploy"
  ensure_crds
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" demos/counter/counter.yaml.tmpl \
    | run_ko apply -f -
}

demo-counter_delete() {
  log_step "demo-counter_delete"
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" demos/counter/counter.yaml.tmpl \
    | run_kubectl delete --ignore-not-found -f -
}
