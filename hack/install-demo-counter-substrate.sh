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
#
# Substrate-resource variant of the counter demo: the worker pool is still a
# CRD manifest, but the ActorTemplate is created through the ate API with
# `kubectl ate create actor-template` instead of being applied as a CRD.
# The micro-VM variant additionally needs the cluster-wide `microvm`
# SandboxConfig from hack/install-microvm-deps.sh --install.

ATE_DEMOS+=(demo-counter-substrate) # register demo-counter-substrate

demo-counter-substrate_usage() {
  echo "  --deploy-demo-counter-substrate-microvm       Deploy demo-counter-substrate on micro-VM workers (needs install-microvm-deps.sh)"
  echo "  --delete-demo-counter-substrate-microvm       Delete the micro-VM variant"
}

# demo-counter-substrate_delete tears down both variants; called by delete_all.
demo-counter-substrate_delete() {
  demo-counter-substrate_delete_variant \
    demos/counter/counter-substrate.yaml.tmpl \
    ate-demo-counter-substrate counter
  demo-counter-substrate_delete_variant \
    demos/counter/counter-substrate-microvm.yaml.tmpl \
    ate-demo-counter-substrate-microvm counter-microvm
}

demo-counter-substrate_cmdline() {
  case "${1}" in
    --deploy-demo-counter-substrate)
      demo-counter-substrate_deploy_variant \
        demos/counter/counter-substrate.yaml.tmpl \
        demos/counter/counter-substrate-template.yaml.tmpl \
        ate-demo-counter-substrate counter-substrate counter
      ;;
    --delete-demo-counter-substrate)
      demo-counter-substrate_delete_variant \
        demos/counter/counter-substrate.yaml.tmpl \
        ate-demo-counter-substrate counter
      ;;
    --deploy-demo-counter-substrate-microvm)
      demo-counter-substrate_deploy_variant \
        demos/counter/counter-substrate-microvm.yaml.tmpl \
        demos/counter/counter-substrate-microvm-template.yaml.tmpl \
        ate-demo-counter-substrate-microvm counter-substrate-microvm counter-microvm
      ;;
    --delete-demo-counter-substrate-microvm)
      demo-counter-substrate_delete_variant \
        demos/counter/counter-substrate-microvm.yaml.tmpl \
        ate-demo-counter-substrate-microvm counter-microvm
      ;;
    *)
      return 1
      ;;
  esac
  return 0
}

# The namespace holding the worker pool doubles as the atespace holding the
# actor template, keeping the substrate variant's naming parallel to the CRD
# demo's namespace/template pairs.
demo-counter-substrate_deploy_variant() {
  local pool_manifest="$1"
  local template_manifest="$2"
  local atespace="$3" # also the pool's k8s namespace
  local pool="$4"
  local template="$5"
  log_step "demo-counter-substrate_deploy (${atespace}/${template})"
  ensure_crds

  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" "${pool_manifest}" \
    | run_ko apply -f -

  log_step "Waiting for the ${pool} worker pool rollout..."
  run_kubectl_fatal rollout status "deployment/${pool}" -n "${atespace}" --timeout=300s

  # The store enforces that the template's atespace exists at create time.
  if ! run_kubectl_ate create atespace "${atespace}" >/dev/null 2>&1 \
      && ! run_kubectl_ate get atespace "${atespace}" >/dev/null 2>&1; then
    echo "error: failed to create atespace ${atespace}" >&2
    exit 1
  fi

  # ko resolve builds the ko:// image references and replaces them with pushed
  # digests before the manifest reaches kubectl-ate. Actor templates are
  # immutable (no update RPC), so an existing template is left in place:
  # delete the demo and redeploy to change it.
  if ! sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" "${template_manifest}" \
      | run_ko resolve -f - \
      | run_kubectl_ate create actor-template -f -; then
    if run_kubectl_ate get actor-template "${template}" -a "${atespace}" >/dev/null 2>&1; then
      log_step "actor template ${atespace}/${template} already exists; keeping it (delete the demo to replace it)"
    else
      echo "error: failed to create actor template ${atespace}/${template}" >&2
      exit 1
    fi
  fi

  # Block until the golden snapshot exists, mirroring the CRD demo's
  # `kubectl wait --for=condition=Ready actortemplate/...` (there is no
  # kubectl wait for substrate resources).
  log_step "Waiting for the ${atespace}/${template} golden snapshot..."
  if ! wait_actortemplate_ready "${atespace}" "${template}" 300; then
    exit 1
  fi
}

demo-counter-substrate_delete_variant() {
  local pool_manifest="$1"
  local atespace="$2"
  local template="$3"
  log_step "demo-counter-substrate_delete (${atespace}/${template})"

  delete_demo_actors_substrate "${atespace}" "${template}"
  # Also removes the template's golden actor and golden snapshot server-side.
  run_kubectl_ate delete actor-template "${template}" -a "${atespace}" 2>/dev/null \
    || log_step "actor template ${atespace}/${template} not deleted (may not exist)"
  run_kubectl_ate delete atespace "${atespace}" 2>/dev/null \
    || log_step "atespace ${atespace} not deleted (may not exist or is not empty)"

  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" "${pool_manifest}" \
    | run_kubectl delete --ignore-not-found -f -
}
