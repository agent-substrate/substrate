#!/usr/bin/env bash

# Copyright 2026 The Agent Substrate Authors
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

# gh_with_retries <gh args...> runs gh, retrying HTTP 5xx errors (5 attempts,
# 2s/4s/8s/16s apart). Other errors fail immediately. Only use it for commands
# that are safe to repeat and read no stdin.
#
# A retried attempt's output is dropped, because gh api writes error bodies to
# stdout. On the final failure, gh's error and any error body go to stderr.
gh_with_retries() {
  local attempts=5 backoff=2 attempt=1 rc out err
  out="$(mktemp)"
  err="$(mktemp)"
  while true; do
    rc=0
    gh "$@" >"${out}" 2>"${err}" || rc=$?
    if [[ ${rc} -eq 0 ]]; then
      cat "${out}"
      cat "${err}" >&2
      break
    fi
    if ! grep -qE 'HTTP 5[0-9]{2}' "${err}"; then
      _gh_retry_print_failure "${err}" "${out}"
      break
    fi
    if ((attempt >= attempts)); then
      _gh_retry_print_failure "${err}" "${out}"
      echo "gave up after ${attempts} attempts" >&2
      break
    fi
    sleep "${backoff}"
    backoff=$((backoff * 2))
    attempt=$((attempt + 1))
  done
  rm -f "${out}" "${err}"
  return "${rc}"
}

# Prints gh's error and any error body, adding the newline gh api leaves off.
_gh_retry_print_failure() {
  cat "$1" "$2" >&2
  if [[ -s "$2" && -n "$(tail -c 1 "$2")" ]]; then
    echo >&2
  fi
}
