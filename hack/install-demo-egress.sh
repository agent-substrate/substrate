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

ATE_DEMOS+=(demo-egress) # register demo-egress
# The micro-VM variant is its own demo rather than a flag on demo-egress: that
# gets it into --help and into delete_all's teardown sweep for free, and the two
# can be installed side by side (the networking suite runs against whichever the
# sandbox class under test selects).
ATE_DEMOS+=(demo-egress-microvm) # register demo-egress-microvm
# The MITM variants are separate demos for the same reason, plus one of their
# own: they project the egress gateway trust bundle, which only resolves on an
# sdsmint install, so they cannot be part of the demos a passthrough install
# deploys.
ATE_DEMOS+=(demo-egress-mitm)         # register demo-egress-mitm
ATE_DEMOS+=(demo-egress-microvm-mitm) # register demo-egress-microvm-mitm

demo-egress_cmdline() {
  case "${1}" in
    --deploy-demo-egress) demo-egress_deploy ;;
    --delete-demo-egress) demo-egress_delete ;;
    *)
      return 1
      ;;
  esac
  return 0
}

demo-egress-microvm_cmdline() {
  case "${1}" in
    --deploy-demo-egress-microvm) demo-egress-microvm_deploy ;;
    --delete-demo-egress-microvm) demo-egress-microvm_delete ;;
    *)
      return 1
      ;;
  esac
  return 0
}

demo-egress-mitm_cmdline() {
  case "${1}" in
    --deploy-demo-egress-mitm) demo-egress-mitm_deploy ;;
    --delete-demo-egress-mitm) demo-egress-mitm_delete ;;
    *)
      return 1
      ;;
  esac
  return 0
}

demo-egress-microvm-mitm_cmdline() {
  case "${1}" in
    --deploy-demo-egress-microvm-mitm) demo-egress-microvm-mitm_deploy ;;
    --delete-demo-egress-microvm-mitm) demo-egress-microvm-mitm_delete ;;
    *)
      return 1
      ;;
  esac
  return 0
}

demo-egress_deploy() {
  log_step "demo-egress_deploy"
  ensure_crds
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" demos/egress/egress.yaml.tmpl \
    | run_ko apply -f -

  log_step "Waiting for egress demo to be ready..."
  # The WorkerPool controller names the Deployment after the WorkerPool
  # ("egress"), the same way demo-counter gets "deployment/counter". The old
  # "egress-deployment" name was NotFound on every successful deploy.
  run_kubectl rollout status deployment/egress -n ate-demo-egress --timeout=300s
  run_kubectl wait --for=condition=Ready actortemplate/egress -n ate-demo-egress --timeout=300s
}

demo-egress_delete() {
  log_step "demo-egress_delete"
  delete_demo_actors ate-demo-egress egress
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" demos/egress/egress.yaml.tmpl \
    | run_kubectl delete --ignore-not-found -f -
}

demo-egress-microvm_usage() {
  echo "  Needs hack/install-microvm-deps.sh --install to have run (cluster-wide microvm SandboxConfig)."
}

demo-egress-microvm_deploy() {
  log_step "demo-egress-microvm_deploy"
  ensure_crds
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" demos/egress/egress-microvm.yaml.tmpl \
    | run_ko apply -f -

  log_step "Waiting for micro-VM egress demo to be ready..."
  run_kubectl rollout status deployment/egress-microvm -n ate-demo-egress-microvm --timeout=300s
  # A micro-VM golden is a cloud-hypervisor cold boot plus a checkpoint, on
  # nested KVM in CI, so it needs a longer budget than the gVisor one above.
  run_kubectl wait --for=condition=Ready actortemplate/egress-microvm \
    -n ate-demo-egress-microvm --timeout=600s
}

demo-egress-microvm_delete() {
  log_step "demo-egress-microvm_delete"
  delete_demo_actors ate-demo-egress-microvm egress-microvm
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" demos/egress/egress-microvm.yaml.tmpl \
    | run_kubectl delete --ignore-not-found -f -
}

demo-egress-mitm_usage() {
  echo "  Needs an sdsmint install (--deploy-atenet --experimental-use-sdsmint): the actors"
  echo "  project the egress gateway trust bundle, which does not resolve otherwise."
}

demo-egress-mitm_deploy() {
  log_step "demo-egress-mitm_deploy"
  ensure_crds
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" demos/egress/egress-mitm.yaml.tmpl \
    | run_ko apply -f -

  log_step "Waiting for MITM egress demo to be ready..."
  run_kubectl rollout status deployment/egress-mitm -n ate-demo-egress-mitm --timeout=300s
  # The golden snapshot only becomes Ready once an actor starts, and an actor
  # whose trust bundle does not resolve never does — so a timeout here is the
  # symptom of a missing sdsmint install (see demo-egress-mitm_usage).
  run_kubectl wait --for=condition=Ready actortemplate/egress-mitm \
    -n ate-demo-egress-mitm --timeout=300s
}

demo-egress-mitm_delete() {
  log_step "demo-egress-mitm_delete"
  delete_demo_actors ate-demo-egress-mitm egress-mitm
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" demos/egress/egress-mitm.yaml.tmpl \
    | run_kubectl delete --ignore-not-found -f -
}

demo-egress-microvm-mitm_usage() {
  echo "  Needs hack/install-microvm-deps.sh --install to have run (cluster-wide microvm SandboxConfig),"
  echo "  and an sdsmint install (--deploy-atenet --experimental-use-sdsmint) for the trust bundle."
}

demo-egress-microvm-mitm_deploy() {
  log_step "demo-egress-microvm-mitm_deploy"
  ensure_crds
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" demos/egress/egress-microvm-mitm.yaml.tmpl \
    | run_ko apply -f -

  log_step "Waiting for micro-VM MITM egress demo to be ready..."
  run_kubectl rollout status deployment/egress-microvm-mitm \
    -n ate-demo-egress-microvm-mitm --timeout=300s
  # A micro-VM golden is a cloud-hypervisor cold boot plus a checkpoint, on
  # nested KVM in CI, so it needs a longer budget than the gVisor one above.
  run_kubectl wait --for=condition=Ready actortemplate/egress-microvm-mitm \
    -n ate-demo-egress-microvm-mitm --timeout=600s
}

demo-egress-microvm-mitm_delete() {
  log_step "demo-egress-microvm-mitm_delete"
  delete_demo_actors ate-demo-egress-microvm-mitm egress-microvm-mitm
  sed "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" demos/egress/egress-microvm-mitm.yaml.tmpl \
    | run_kubectl delete --ignore-not-found -f -
}
