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
# The micro-VM variant additionally needs the cluster-wide `microvm`
# SandboxConfig from hack/install-microvm-deps.sh --install.

ATE_DEMOS+=(demo-counter) # register demo-counter

demo-counter_usage() {
  echo "  --deploy-demo-counter-with-external-volume    Deploy demo-counter with external volume validation"
  echo "  --deploy-demo-counter-microvm                 Deploy demo-counter on micro-VM workers (needs install-microvm-deps.sh)"
  echo "  --delete-demo-counter-microvm                 Delete the micro-VM variant"
}

demo-counter_cmdline() {
  case "${1}" in
    --deploy-demo-counter) demo-counter_deploy "false" ;;
    --deploy-demo-counter-with-external-volume) demo-counter_deploy "true" ;;
    --delete-demo-counter)
      delete_substrate_demo demo-counter_render_plain \
        demos/counter/counter.yaml.tmpl ate-demo-counter counter
      ;;
    --deploy-demo-counter-microvm)
      # 600s golden budget: a micro-VM golden is a cloud-hypervisor cold boot
      # plus checkpoint, on nested KVM in CI.
      deploy_substrate_demo render_demo_manifest \
        demos/counter/counter-microvm.yaml.tmpl ate-demo-counter-microvm counter-microvm 600 \
        demos/counter/counter-microvm-template.yaml.tmpl counter-microvm
      ;;
    --delete-demo-counter-microvm)
      delete_substrate_demo render_demo_manifest \
        demos/counter/counter-microvm.yaml.tmpl ate-demo-counter-microvm counter-microvm
      ;;
    *)
      return 1
      ;;
  esac
  return 0
}

# demo-counter_render substitutes the demo's placeholders in a manifest:
#   demo-counter_render <with_external_volume> <manifest>
# The external-volume lines are dropped unless <with_external_volume> is true.
demo-counter_render() {
  local with_external_volume="$1"
  local manifest="$2"
  local validate_cmd=("-e" "/\${VALIDATE_EXISTING_FILE_PATH_ARG}/d")
  local ext_vol_mount_cmd=("-e" "/\${EXTERNAL_VOLUME_MOUNTS}/d")
  local ext_vol_spec_cmd=("-e" "/\${EXTERNAL_VOLUMES}/d")
  if [[ "${with_external_volume}" == "true" ]]; then
    # csi-hostpath-sc only exists when hack/setup-csi-hostpath-kind.sh has run (via SETUP_CSI=true).
    # Otherwise fall back to the default "standard" StorageClass.
    local storage_class="standard"
    if [[ "${SETUP_CSI:-false}" == "true" ]]; then
      storage_class="csi-hostpath-sc"
    fi

    validate_cmd=("-e" "s|\${VALIDATE_EXISTING_FILE_PATH_ARG}|  - --validate-existing-file-path=/external-data/test.txt|g")
    ext_vol_mount_cmd=("-e" "s|\${EXTERNAL_VOLUME_MOUNTS}|  - name: external-data\n    mountPath: /external-data|g")
    ext_vol_spec_cmd=("-e" "s|\${EXTERNAL_VOLUMES}|- name: external-data\n  type: ExternalVolumeTemplate\n  externalVolumeTemplate:\n    capacity: 1Gi\n    storageClassName: ${storage_class}|g")
  fi

  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      "${validate_cmd[@]}" \
      "${ext_vol_mount_cmd[@]}" \
      "${ext_vol_spec_cmd[@]}" \
      "${manifest}"
}

# Wrappers in the helpers' one-argument <render_fn> shape.
demo-counter_render_plain() { demo-counter_render false "$1"; }
demo-counter_render_ext_vol() { demo-counter_render true "$1"; }

demo-counter_deploy() {
  local with_external_volume="${1:-false}"
  log_step "demo-counter_deploy (with_external_volume=${with_external_volume})"
  local render_fn=demo-counter_render_plain
  if [[ "${with_external_volume}" == "true" ]]; then
    render_fn=demo-counter_render_ext_vol
  fi
  deploy_substrate_demo "${render_fn}" \
    demos/counter/counter.yaml.tmpl ate-demo-counter counter 300 \
    demos/counter/counter-template.yaml.tmpl counter
}

# demo-counter_delete tears down both variants; called by delete_all.
demo-counter_delete() {
  delete_substrate_demo demo-counter_render_plain \
    demos/counter/counter.yaml.tmpl ate-demo-counter counter
  delete_substrate_demo render_demo_manifest \
    demos/counter/counter-microvm.yaml.tmpl ate-demo-counter-microvm counter-microvm
}
