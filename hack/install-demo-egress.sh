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
# The egress demo: each variant applies a worker pool manifest, then creates
# its ActorTemplate as a substrate resource through the ate API with
# `kubectl ate create actor-template`. The micro-VM variants need the
# cluster-wide `microvm` SandboxConfig from hack/install-microvm-deps.sh
# --install; the MITM variants need an sdsmint install
# (--experimental-use-sdsmint), because their actors project the egress
# gateway trust bundle, which does not resolve otherwise.

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
  deploy_substrate_demo render_demo_manifest \
    demos/egress/egress.yaml.tmpl \
    ate-demo-egress egress 300 \
    demos/egress/egress-template.yaml.tmpl egress
}

demo-egress_delete() {
  delete_substrate_demo render_demo_manifest \
    demos/egress/egress.yaml.tmpl \
    ate-demo-egress egress
}

demo-egress-microvm_usage() {
  echo "  Needs hack/install-microvm-deps.sh --install to have run (cluster-wide microvm SandboxConfig)."
}

demo-egress-microvm_deploy() {
  # 600s golden budget: a micro-VM golden is a cloud-hypervisor cold boot
  # plus checkpoint, on nested KVM in CI.
  deploy_substrate_demo render_demo_manifest \
    demos/egress/egress-microvm.yaml.tmpl \
    ate-demo-egress-microvm egress-microvm 600 \
    demos/egress/egress-microvm-template.yaml.tmpl egress-microvm
}

demo-egress-microvm_delete() {
  delete_substrate_demo render_demo_manifest \
    demos/egress/egress-microvm.yaml.tmpl \
    ate-demo-egress-microvm egress-microvm
}

demo-egress-mitm_usage() {
  echo "  Needs an sdsmint install (--deploy-atenet --experimental-use-sdsmint): the actors"
  echo "  project the egress gateway trust bundle, which does not resolve otherwise."
}

demo-egress-mitm_deploy() {
  # The golden snapshot only exists once an actor starts, and an actor whose
  # trust bundle does not resolve never does — so a timeout here is the
  # symptom of a missing sdsmint install (see demo-egress-mitm_usage).
  deploy_substrate_demo render_demo_manifest \
    demos/egress/egress-mitm.yaml.tmpl \
    ate-demo-egress-mitm egress-mitm 300 \
    demos/egress/egress-mitm-template.yaml.tmpl egress-mitm
}

demo-egress-mitm_delete() {
  delete_substrate_demo render_demo_manifest \
    demos/egress/egress-mitm.yaml.tmpl \
    ate-demo-egress-mitm egress-mitm
}

demo-egress-microvm-mitm_usage() {
  echo "  Needs hack/install-microvm-deps.sh --install to have run (cluster-wide microvm SandboxConfig),"
  echo "  and an sdsmint install (--deploy-atenet --experimental-use-sdsmint) for the trust bundle."
}

demo-egress-microvm-mitm_deploy() {
  deploy_substrate_demo render_demo_manifest \
    demos/egress/egress-microvm-mitm.yaml.tmpl \
    ate-demo-egress-microvm-mitm egress-microvm-mitm 600 \
    demos/egress/egress-microvm-mitm-template.yaml.tmpl egress-microvm-mitm
}

demo-egress-microvm-mitm_delete() {
  delete_substrate_demo render_demo_manifest \
    demos/egress/egress-microvm-mitm.yaml.tmpl \
    ate-demo-egress-microvm-mitm egress-microvm-mitm
}
