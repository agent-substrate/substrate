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

ATE_DEMOS+=(demo-claude-code-multiplex) # register demo-claude-code-multiplex

demo-claude-code-multiplex_cmdline() {
  case "${1}" in
    --deploy-demo-claude-code-multiplex) demo-claude-code-multiplex_deploy ;;
    --delete-demo-claude-code-multiplex) demo-claude-code-multiplex_delete ;;
    *)
      return 1
      ;;
  esac
  return 0
}

# Build the workload image, push to ${KO_DOCKER_REPO}, and echo the resolved
# digest-pinned reference (e.g. gcr.io/.../claude-multiplex-demo-workload@sha256:...).
# The workload is a Dockerfile-based Python+Claude-Code wrapper (not a Go
# binary), so it uses docker buildx rather than ko.
demo-claude-code-multiplex_build_workload() {
  local repo="${KO_DOCKER_REPO}/claude-multiplex-demo-workload"
  local stage_tag="${repo}:build-$(date +%s)"
  docker buildx build \
    --platform=linux/amd64 \
    --push \
    -t "${stage_tag}" \
    demos/claude-code-multiplex/workload >&2
  local digest
  digest=$(docker buildx imagetools inspect "${stage_tag}" --format '{{json .}}' \
             | jq -r '.manifest.digest')
  if [[ -z "${digest}" || "${digest}" == "null" ]]; then
    echo "Failed to resolve workload image digest from ${stage_tag}" >&2
    return 1
  fi
  echo "${repo}@${digest}"
}

demo-claude-code-multiplex_deploy() {
  log_step "demo-claude-code-multiplex_deploy"
  if [[ -z "${ANTHROPIC_API_KEY:-}" ]]; then
    echo "ANTHROPIC_API_KEY must be set" >&2
    return 1
  fi
  if [[ -z "${BUCKET_NAME:-}" ]]; then
    echo "BUCKET_NAME must be set" >&2
    return 1
  fi
  if [[ -z "${KO_DOCKER_REPO:-}" ]]; then
    echo "KO_DOCKER_REPO must be set (see hack/ate-dev-env.sh.example)" >&2
    return 1
  fi

  local workload_image
  workload_image=$(demo-claude-code-multiplex_build_workload)
  if [[ -z "${workload_image}" ]]; then
    return 1
  fi
  log_step "  workload image: ${workload_image}"

  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      -e "s|\${ANTHROPIC_API_KEY}|${ANTHROPIC_API_KEY}|g" \
      -e "s|\${WORKLOAD_IMAGE}|${workload_image}|g" \
      demos/claude-code-multiplex/claude-code-multiplex.yaml.tmpl \
    | run_kubectl apply -f -
}

demo-claude-code-multiplex_delete() {
  log_step "demo-claude-code-multiplex_delete"
  # Delete-time substitution doesn't need a real image — k8s identifies
  # resources by metadata, not container spec. Use placeholders so sed
  # produces valid YAML even when the env vars aren't set.
  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME:-placeholder}|g" \
      -e "s|\${ANTHROPIC_API_KEY}|${ANTHROPIC_API_KEY:-placeholder}|g" \
      -e "s|\${WORKLOAD_IMAGE}|placeholder|g" \
      demos/claude-code-multiplex/claude-code-multiplex.yaml.tmpl \
    | run_kubectl delete --ignore-not-found -f -
}

demo-claude-code-multiplex_usage() {
  echo ""
  echo "  Required env: ANTHROPIC_API_KEY, BUCKET_NAME, KO_DOCKER_REPO"
  echo "  See demos/claude-code-multiplex/README.md for the walkthrough."
}
