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

demo-counter_usage() {
  echo "  --deploy-demo-counter-with-external-volume    Deploy demo-counter with external volume validation"
  echo "  --deploy-demo-counter-atv                     Deploy demo-counter on control-plane ActorTemplate/ActorTemplateVersion"
}

demo-counter_cmdline() {
  case "${1}" in
    --deploy-demo-counter) demo-counter_deploy "false" ;;
    --deploy-demo-counter-with-external-volume) demo-counter_deploy "true" ;;
    --deploy-demo-counter-atv) demo-counter-atv_deploy ;;
    --delete-demo-counter) demo-counter_delete ;;
    --delete-demo-counter-atv) demo-counter-atv_delete ;;
    *)
      return 1
      ;;
  esac
  return 0
}

demo-counter_deploy() {
  local with_external_volume="${1:-false}"
  log_step "demo-counter_deploy (with_external_volume=${with_external_volume})"
  ensure_crds

  local validate_cmd=("-e" "/\${VALIDATE_EXISTING_FILE_PATH_ARG}/d")
  local ext_vol_mount_cmd=("-e" "/\${EXTERNAL_VOLUME_MOUNTS}/d")
  local ext_vol_spec_cmd=("-e" "/\${EXTERNAL_VOLUMES}/d")
  if [[ "${with_external_volume}" == "true" ]]; then
    validate_cmd=("-e" "s|\${VALIDATE_EXISTING_FILE_PATH_ARG}|    - --validate-existing-file-path=/external-data/test.txt|g")
    ext_vol_mount_cmd=("-e" "s|\${EXTERNAL_VOLUME_MOUNTS}|    - name: external-data\n      mountPath: /external-data|g")
    ext_vol_spec_cmd=("-e" "s|\${EXTERNAL_VOLUMES}|  - name: external-data\n    externalVolumeTemplate:\n      capacity: 1Gi\n      storageClassName: standard|g")
  fi

  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      "${validate_cmd[@]}" \
      "${ext_vol_mount_cmd[@]}" \
      "${ext_vol_spec_cmd[@]}" \
      demos/counter/counter.yaml.tmpl \
    | run_ko apply -f -

  # Wait for the demo to be fully ready before returning. On a cold cluster the
  # first ActorTemplate golden snapshot pays one-time costs (downloading the
  # gVisor runsc binary, first gVisor pod start, image pulls). Blocking here
  # means callers -- notably the e2e suite, which creates its own ActorTemplate
  # with a tight readiness deadline -- run against an already-warm node instead
  # of racing that cold-start work.
  log_step "Waiting for counter demo to be ready..."
  run_kubectl rollout status deployment/counter -n ate-demo-counter --timeout=300s
  run_kubectl wait --for=condition=Ready actortemplate/counter -n ate-demo-counter --timeout=300s
}

# Deploys the counter demo on control-plane ActorTemplate/ActorTemplateVersion
# resources. Reuses the CRD demo's WorkerPool (and its warm node), so the CRD
# demo is deployed first; the AT/ATV manifests then go through ko resolve
# (digest-pins the ko:// image, as the version spec requires) and
# kubectl ate apply.
demo-counter-atv_deploy() {
  log_step "demo-counter-atv_deploy"
  demo-counter_deploy "false"

  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      demos/counter/counter-atv.yaml.tmpl \
    | run_ko resolve -f - \
    | run_kubectl_ate apply -f -

  wait_for_template_version_ready counter-atv-v1
  wait_for_template_version_ready counter-atv-v2
}

# Polls a control-plane ActorTemplateVersion until its golden snapshot build
# reaches STATE_READY; fails fast on STATE_FAILED.
wait_for_template_version_ready() {
  local version="${1}"
  log_step "Waiting for ActorTemplateVersion ${version} to be READY..."
  local deadline=$((SECONDS + 300))
  while true; do
    local json
    json="$(run_kubectl_ate get actor-template-versions "${version}" -o json 2>/dev/null || true)"
    if grep -q '"state": "STATE_READY"' <<<"${json}"; then
      break
    fi
    if grep -q '"state": "STATE_FAILED"' <<<"${json}"; then
      echo "ActorTemplateVersion ${version} failed to build:" >&2
      grep '"message"' <<<"${json}" >&2 || true
      return 1
    fi
    if ((SECONDS >= deadline)); then
      echo "Timed out waiting for ActorTemplateVersion ${version} to be READY" >&2
      return 1
    fi
    sleep 5
  done
}

demo-counter-atv_delete() {
  log_step "demo-counter-atv_delete"

  # Native actors carry actorTemplate (global name), not the CRD
  # namespace/name pair delete_demo_actors filters on.
  if command -v jq &>/dev/null; then
    local actors_json atespace actor_name
    if actors_json=$(run_kubectl_ate get actors -A -o json 2>/dev/null); then
      while IFS=$'\t' read -r atespace actor_name; do
        [[ -z "${actor_name}" ]] && continue
        log_step "  preparing actor ${atespace}/${actor_name} for delete"
        prepare_actor_for_delete "${actor_name}" "${atespace}"
        run_kubectl_ate delete actor "${actor_name}" -a "${atespace}"
      done < <(
        jq -r '.actors[]? | select(.actorTemplate == "counter-atv") | "\(.metadata.atespace)\t\(.metadata.name)"' \
          <<<"${actors_json}"
      )
    fi
  fi

  run_kubectl_ate delete actor-template-version counter-atv-v2 || true
  run_kubectl_ate delete actor-template-version counter-atv-v1 --clear-default || true
  run_kubectl_ate delete actor-template counter-atv || true
}

demo-counter_delete() {
  log_step "demo-counter_delete"
  delete_demo_actors ate-demo-counter counter
  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      -e "/\${VALIDATE_EXISTING_FILE_PATH_ARG}/d" \
      -e "/\${EXTERNAL_VOLUME_MOUNTS}/d" \
      -e "/\${EXTERNAL_VOLUMES}/d" \
      demos/counter/counter.yaml.tmpl \
    | run_kubectl delete --ignore-not-found -f -
}
