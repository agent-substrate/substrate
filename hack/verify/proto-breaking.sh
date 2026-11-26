#!/usr/bin/env bash

# Copyright 2026 The Agent Substrate Authors
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

# Rejects breaking proto changes across each proto package.
#
# ateapi stores resources as binary protobuf in bytea columns, and atepg.go
# unmarshals them with DiscardUnknown. kubectl-ate parses and prints resource
# manifests as protojson, and atelet writes ateletpb.Snapshot to disk as
# protojson. External callers and plugins import the generated Go packages
# under api/ and pkg/proto/.
#
# buf.yaml checks the protos under api/, internal/proto/, and pkg/proto/ with
# buf's PACKAGE rules. Within a proto package, declarations may move between
# .proto files, but messages, enums, services, RPCs, fields, enum values, and
# reserved ranges cannot be deleted, renamed, renumbered, or retyped.
#
# The baseline is a release tag, including release candidates. Only protos
# that shipped in a release can be in stored data or on a user's node. Main
# gives no compatibility guarantee between commits, so protos added since the
# last release may still change.
#
# Release tags live on release-X.Y branches, which are cut from main. On main,
# the newest release branch, or a newly cut one, the baseline is the newest
# tag. On an older release branch, it is the newest tag in that branch's
# history. Tags tell the cases apart, so this works for any local branch name.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

if (( $# > 1 )); then
  echo "Usage: $0 [base-ref]" >&2
  echo "  base-ref is compared as is. To compare against a branch point, pass" >&2
  echo "  \"\$(git merge-base HEAD upstream/main)\"." >&2
  exit 2
fi

if (( $# == 1 )); then
  BASELINE="ref=$1"
else
  # versionsort.suffix sorts v1.0.0-rc.3 below v1.0.0.
  NEWEST="$(git -c versionsort.suffix=- tag -l 'v[0-9]*' --sort=-v:refname | head -n 1)"
  if [ -z "${NEWEST}" ]; then
    echo "proto-breaking: no release tag found; fetch tags (git fetch upstream --tags)" >&2
    exit 1
  fi
  if [ -z "$(git tag -l 'v[0-9]*' --merged HEAD --no-merged "${NEWEST}")" ]; then
    # NEWEST contains every release tag in HEAD's history: main or the newest line.
    TAG="${NEWEST}"
  else
    # NEWEST lacks a release tag in HEAD's history: an older release branch.
    TAG="$(git -c versionsort.suffix=- tag -l 'v[0-9]*' --merged HEAD --sort=-v:refname | head -n 1)"
  fi
  echo "proto-breaking: comparing against ${TAG}" >&2
  BASELINE="tag=${TAG}"
fi

# The input is the working tree, so an incompatible edit is reported before it
# is ever committed. --against-config reads the baseline with the current
# buf.yaml, so a baseline commit without one still builds.
BIN="$("${ROOT}"/hack/run-tool.sh --print-bin-path buf)"
"${BIN}" breaking --against ".git#${BASELINE}" --against-config buf.yaml
