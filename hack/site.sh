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

# Serves or builds the website in site/. See site/README.md.
#
#   hack/site.sh serve   live-reloading dev server
#   hack/site.sh build   production build into site/public, as CI runs it
#
# Needs Hugo extended and Go; build also needs npm, for PostCSS. Set HUGO to
# use a hugo binary that is not on PATH.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}/site"

HUGO="${HUGO:-hugo}"

case "${1:-}" in
  serve)
    shift
    exec "${HUGO}" server "$@"
    ;;
  build)
    shift
    npm ci
    exec "${HUGO}" build --environment production --minify "$@"
    ;;
  *)
    echo "usage: $0 serve|build [hugo flags...]" >&2
    exit 2
    ;;
esac
