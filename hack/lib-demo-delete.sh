#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Shared helpers for demo teardown. Sourced by hack/install-ate.sh.

# get_actor_status echoes the actor's status enum (e.g. STATUS_SUSPENDED).
get_actor_status() {
  local actor_id="$1"
  local json

  if ! json=$(run_kubectl_ate get actor "${actor_id}" -o json 2>/dev/null); then
    return 1
  fi
  jq -r '.actors[0].status // empty' <<<"${json}"
}

# prepare_actor_for_delete suspends (or resumes then suspends) until DeleteActor
# is allowed. Actors must be STATUS_SUSPENDED before deletion.
prepare_actor_for_delete() {
  local actor_id="$1"
  local timeout_secs="${2:-120}"
  local deadline=$((SECONDS + timeout_secs))
  local status

  while ((SECONDS < deadline)); do
    if ! status=$(get_actor_status "${actor_id}"); then
      return 0
    fi

    case "${status}" in
      STATUS_SUSPENDED)
        return 0
        ;;
      STATUS_PAUSED)
        run_kubectl_ate resume actor "${actor_id}" -o json >/dev/null
        ;;
      STATUS_RUNNING)
        run_kubectl_ate suspend actor "${actor_id}" -o json >/dev/null
        ;;
      STATUS_RESUMING | STATUS_SUSPENDING | STATUS_PAUSING)
        ;;
      *)
        echo "cannot delete actor ${actor_id}: unexpected status ${status}" >&2
        return 1
        ;;
    esac
    sleep 2
  done

  echo "timed out waiting for actor ${actor_id} to reach STATUS_SUSPENDED" >&2
  return 1
}

# delete_demo_actors removes all actors for one or more (namespace, template)
# pairs before the demo manifests are deleted. Arguments are alternating
# namespace and template name, e.g.:
#   delete_demo_actors ate-demo-counter counter
#   delete_demo_actors ns-a tmpl-a ns-b tmpl-b
delete_demo_actors() {
  if ! command -v jq &>/dev/null; then
    echo "jq is required to delete demo actors" >&2
    return 1
  fi

  if (($# == 0 || $# % 2 != 0)); then
    echo "delete_demo_actors expects namespace/template pairs" >&2
    return 1
  fi

  if ! run_kubectl get deployment/ate-api-server-deployment -n ate-system >/dev/null 2>&1; then
    log_step "ate-api-server not found; skipping actor cleanup"
    return 0
  fi

  local actors_json
  if ! actors_json=$(run_kubectl_ate get actors -o json 2>/dev/null); then
    echo "warning: could not list actors; skipping actor cleanup" >&2
    return 0
  fi

  local ns tmpl actor_id
  while (($# > 0)); do
    ns="$1"
    tmpl="$2"
    shift 2

    log_step "Deleting actors for ${ns}/${tmpl}"
    while IFS= read -r actor_id; do
      [[ -z "${actor_id}" ]] && continue
      log_step "  preparing actor ${actor_id} for delete"
      prepare_actor_for_delete "${actor_id}"
      run_kubectl_ate delete actor "${actor_id}"
    done < <(
      jq -r --arg ns "${ns}" --arg tmpl "${tmpl}" \
        '.actors[]? | select(.actorTemplateNamespace == $ns and .actorTemplateName == $tmpl) | .actorId' \
        <<<"${actors_json}"
    )
  done
}
