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

# Serves, builds, or tests the website in site/. See site/README.md.
#
#   hack/site.sh serve          live-reloading dev server
#   hack/site.sh build          production build into site/public, as CI runs it
#   hack/site.sh content        generate site/content/ stubs from docs/ and demos/,
#                               and the API reference from ateapi.proto
#   hack/site.sh release-data   write site/data/release.json (serve and build
#                               run this first)
#   hack/site.sh test           build against site/testdata and check the
#                               rendered templates
#
# Needs Hugo extended and Go; build also needs npm, for PostCSS. Set HUGO to
# use a hugo binary that is not on PATH, and SITE_REF to the git ref the site
# is built from (release builds set it to the tag).

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

HUGO="${HUGO:-hugo}"
SITE_REF="${SITE_REF:-HEAD}"
export HUGO

# Generates Hugo content stubs under site/content/ from docs/ and demos/, and
# the API reference page body under site/assets/api-reference/.
generate_content() {
  go run ./tools/site-content --out site/content
  # The reference documents main as latest, and each release since v1.0.0.
  go -C tools/apitool run . generate-docs --out-dir "${ROOT}/site/assets/api-reference" --releases-since v1.0.0
}

# Lists the docs added since the previous full release so the sidebar can mark
# them new.
release_data() {
  go run ./tools/site-release-data --ref "${SITE_REF}" --out site/data/release.json
}

case "${1:-}" in
  content)
    generate_content
    ;;
  release-data)
    release_data
    ;;
  serve)
    shift
    generate_content
    release_data
    cd site
    exec "${HUGO}" server "$@"
    ;;
  build)
    shift
    generate_content
    release_data
    cd site
    npm ci
    exec "${HUGO}" build --environment production --minify --cleanDestinationDir "$@"
    ;;
  test)
    exec hack/test-site-templates.sh
    ;;
  *)
    echo "usage: $0 serve|build|content|release-data|test [hugo flags...]" >&2
    exit 2
    ;;
esac
