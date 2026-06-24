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

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

OUTDIR="LICENSES" # under $ROOT

# Ensure the tool is built and up-to-date
GO_LICENSES_BIN="$(bash "${ROOT}/hack/run-tool.sh" --print-bin-path go-licenses)"

# Clean out previous licenses
rm -rf "${OUTDIR}"
mkdir -p "${OUTDIR}"

# TODO: determine full release target set and dedupe ...
targets=(
  "linux/amd64"
  "linux/arm64"
  "darwin/amd64"
  "darwin/arm64"
)

tmpfile="" # make shellcheck happy
tmpfile="$(mktemp -t "update-licenses.XXXXXX")"
# shellcheck disable=SC2064 # evaluate $tmpfile immediately
trap "rm -f ${tmpfile}" EXIT

for target in "${targets[@]}"; do
  IFS="/" read -r target_os target_arch <<< "${target}"

  # Create a temporary output folder for each target
  tmp_out="$(mktemp -d -t "update-licenses-out.XXXXXX")"

  GOOS="${target_os}" \
  GOARCH="${target_arch}" \
  GOROOT=$(go env GOROOT) \
  CGO_ENABLED=1 \
  "${GO_LICENSES_BIN}" save ./... \
      --include_tests \
      --force \
      --ignore=./vendor \
      --save_path="${tmp_out}" > "${tmpfile}" 2>&1 || \
  {
    echo "Failed for ${target_os}/${target_arch}:" >&2
    cat "${tmpfile}"
    rm -rf "${tmp_out}"
    exit 1
  }

  # Bug in go-licenses?  Our repo gets included in a loop
  rm -rf "${tmp_out}/github.com/agent-substrate/substrate"

  # Merge the results into the main OUTDIR
  if [ "$(ls -A "${tmp_out}")" ]; then
    chmod -R u+w "${OUTDIR}" 2>/dev/null || true
    cp -R "${tmp_out}/." "${OUTDIR}/"
  fi
  rm -rf "${tmp_out}"
done

# Collect upstream licenses for forked / copied-in third-party SOURCE under in-tree
# third_party/ dirs. go-licenses only covers module dependencies (and treats our own
# module — including these copies — as first-party), so mirror their LICENSE/NOTICE/
# COPYING files into LICENSES/third_party/, keyed by their path under third_party/.
# `git ls-files` is used so gitignored paths (e.g. nested worktrees under .claude/,
# bin/) are skipped automatically; vendor/ and the output dir hold module deps /
# generated output (not forked source), so skip those too. Mirrors kubernetes
# hack/update-vendor-licenses.sh (see its LICENSES/third_party/). License paths are
# plain ASCII, so newline-delimited iteration is safe.
git ls-files --cached --others --exclude-standard \
  | { grep -iE '(^|/)third_party/.*(licen[sc]e|notice|copying)[^/]*$' || true; } \
  | while IFS= read -r f; do
      case "${f}" in vendor/* | "${OUTDIR}"/*) continue ;; esac
      dest="${OUTDIR}/third_party/${f##*third_party/}" # e.g. LICENSES/third_party/kata/agentpb/LICENSE
      mkdir -p "$(dirname "${dest}")"
      cp "${f}" "${dest}"
    done

# Clean up empty directories
find "${OUTDIR}" -type d -empty -delete
