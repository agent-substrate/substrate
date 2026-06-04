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

ATE_DEMOS+=(demo-openclaw) # register demo-openclaw

demo-openclaw_cmdline() {
  case "${1}" in
    --deploy-demo-openclaw) demo-openclaw_deploy ;;
    --delete-demo-openclaw) demo-openclaw_delete ;;
    *)
      return 1
      ;;
  esac
  return 0
}

# Build the unified image (UI + Workload), push to ${KO_DOCKER_REPO}, and echo 
# the resolved digest-pinned reference.
# This is a TypeScript application, so it uses docker buildx rather than ko.
demo-openclaw_build_image() {
  local repo="${KO_DOCKER_REPO}/openclaw-demo"
  local stage_tag="${repo}:build-$(date +%s)"
  docker buildx build \
    --platform=linux/amd64 \
    --push \
    -t "${stage_tag}" \
    demos/openclaw >&2
  local digest
  digest=$(docker buildx imagetools inspect "${stage_tag}" --format '{{json .}}' \
             | jq -r '.manifest.digest')
  if [[ -z "${digest}" || "${digest}" == "null" ]]; then
    echo "Failed to resolve openclaw image digest from ${stage_tag}" >&2
    return 1
  fi
  echo "${repo}@${digest}"
}

demo-openclaw_deploy() {
  log_step "demo-openclaw_deploy"
  if [[ -z "${BUCKET_NAME:-}" ]]; then
    echo "BUCKET_NAME must be set" >&2
    return 1
  fi
  if [[ -z "${KO_DOCKER_REPO:-}" ]]; then
    echo "KO_DOCKER_REPO must be set (see hack/ate-dev-env.sh.example)" >&2
    return 1
  fi

  local openclaw_image
  openclaw_image=$(demo-openclaw_build_image)
  if [[ -z "${openclaw_image}" ]]; then
    return 1
  fi
  log_step "  openclaw image: ${openclaw_image}"

  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      -e "s|\${OPENCLAW_IMAGE}|${openclaw_image}|g" \
      demos/openclaw/openclaw-multiplex.yaml.tmpl \
    | run_kubectl apply -f -
}

demo-openclaw_delete() {
  log_step "demo-openclaw_delete"
  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME:-placeholder}|g" \
      -e "s|\${OPENCLAW_IMAGE}|placeholder|g" \
      demos/openclaw/openclaw-multiplex.yaml.tmpl \
    | run_kubectl delete --ignore-not-found -f -
}

demo-openclaw_usage() {
  echo ""
  echo "  Required env: BUCKET_NAME, KO_DOCKER_REPO"
  echo "  See demos/openclaw/README.md for the walkthrough."
}
