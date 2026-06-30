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

ATE_DEMOS+=(sub-agent-multiplex) # register sub-agent-multiplex

sub-agent-multiplex_cmdline() {
  case "${1}" in
    --deploy-sub-agent-multiplex) sub-agent-multiplex_deploy ;;
    --delete-sub-agent-multiplex) sub-agent-multiplex_delete ;;
    *)
      return 1
      ;;
  esac
  return 0
}

sub-agent-multiplex_build_image() {
  local repo="${KO_DOCKER_REPO}/sub-agent-multiplex"
  local stage_tag="${repo}:build-$(date +%s)"
  cp bin/kubectl-ate demos/sub-agent-multiplex/
  docker buildx build \
    --platform=linux/amd64 \
    --push \
    -t "${stage_tag}" \
    demos/sub-agent-multiplex >&2
  local digest
  digest=$(docker buildx imagetools inspect "${stage_tag}" --format '{{json .}}' \
             | jq -r '.manifest.digest')
  if [[ -z "${digest}" || "${digest}" == "null" ]]; then
    echo "Failed to resolve sub-agent-multiplex image digest from ${stage_tag}" >&2
    return 1
  fi
  echo "${repo}@${digest}"
}

sub-agent-multiplex_deploy() {
  log_step "sub-agent-multiplex_deploy"
  if [[ -z "${BUCKET_NAME:-}" ]]; then
    echo "BUCKET_NAME must be set" >&2
    return 1
  fi
  if [[ -z "${KO_DOCKER_REPO:-}" ]]; then
    echo "KO_DOCKER_REPO must be set" >&2
    return 1
  fi

  local image
  image=$(sub-agent-multiplex_build_image)
  if [[ -z "${image}" ]]; then
    return 1
  fi
  log_step "  sub-agent-multiplex image: ${image}"

  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      -e "s|\${SUB_AGENT_IMAGE}|${image}|g" \
      -e "s|\${WHATSAPP_PHONE}|${WHATSAPP_PHONE:-1234567890}|g" \
      demos/sub-agent-multiplex/sub-agent-multiplex.yaml.tmpl \
    | run_kubectl apply -f -

  sed -e "s|\${SUB_AGENT_IMAGE}|${image}|g" \
      demos/sub-agent-multiplex/ui-deployment.yaml.tmpl \
    | run_kubectl apply -f -
}

sub-agent-multiplex_delete() {
  log_step "sub-agent-multiplex_delete"
  sed -e "s|\${BUCKET_NAME}|placeholder|g" \
      -e "s|\${SUB_AGENT_IMAGE}|placeholder|g" \
      demos/sub-agent-multiplex/sub-agent-multiplex.yaml.tmpl \
    | run_kubectl delete --ignore-not-found -f -

  sed -e "s|\${SUB_AGENT_IMAGE}|placeholder|g" \
      demos/sub-agent-multiplex/ui-deployment.yaml.tmpl \
    | run_kubectl delete --ignore-not-found -f -
}

sub-agent-multiplex_usage() {
  echo ""
  echo "  Required env: BUCKET_NAME, KO_DOCKER_REPO"
}
