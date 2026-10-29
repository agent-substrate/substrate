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

# Builds the website against site/testdata and checks how the templates
# render doc front matter. Needs Hugo extended and the site's npm packages;
# see site/README.md. Set HUGO to use a Hugo binary that is not on PATH.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
HUGO="${HUGO:-hugo}"
OUT="$(mktemp -d)"
trap 'rm -rf "${OUT}"' EXIT
cd "${ROOT}/site"

failures=0
fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}

# build NAME CONFIG... builds the site with hugo.yaml and the given configs
# into ${OUT}/NAME, logging to ${OUT}/NAME.log.
build() {
  local name="$1"
  shift
  local configs="hugo.yaml"
  local c
  for c in "$@"; do
    configs="${configs},${c}"
  done
  "${HUGO}" build --config "${configs}" --destination "${OUT}/${name}" >"${OUT}/${name}.log" 2>&1
}

# page NAME PATH prints a built file with newlines removed, so patterns can
# span the templates' line breaks.
page() {
  tr -d '\n' <"${OUT}/$1/$2"
}

# has NAME PATH ERE fails unless the built file matches the pattern.
has() {
  page "$1" "$2" | grep -Eq -- "$3" || fail "$1/$2: expected /$3/"
}

# lacks NAME PATH ERE fails if the built file matches the pattern.
lacks() {
  if page "$1" "$2" | grep -Eq -- "$3"; then
    fail "$1/$2: unexpected /$3/"
  fi
}

# glossary_error NAME TEXT builds content-glossary with the glossary fixture
# testdata/glossary-errors/NAME and fails unless the build fails with TEXT.
glossary_error() {
  local name="$1" want="$2"
  cat >"${OUT}/${name}.yaml" <<EOF
baseURL: 'https://example.org/'
module:
  mounts:
    - {source: layouts, target: layouts}
    - {source: assets, target: assets}
    - {source: static, target: static}
    - {source: testdata/content-glossary, target: content}
    - {source: testdata/glossary-errors/${name}, target: assets/repo}
EOF
  if build "${name}" "${OUT}/${name}.yaml"; then
    fail "${name}: glossary build succeeded"
  elif ! grep -q -- "${want}" "${OUT}/${name}.log"; then
    fail "${name}: build error does not mention '${want}'"
  fi
}

F=docs/fixtures

if ! build main testdata/hugo.test.yaml; then
  cat "${OUT}/main.log" >&2
  exit 1
fi

# Front matter is parsed, never rendered, and the source title is dropped.
for p in plain experimental deprecated override new new-deprecated planned; do
  lacks main "${F}/${p}/index.html" 'status: (experimental|deprecated)|status_note:'
  lacks main "${F}/${p}/index.html" 'Source Title'
done
lacks main llms-full.txt 'status: (experimental|deprecated)|status_note:'
lacks main llms-full.txt 'Source Title'
has main "${F}/deprecated/index.html" 'Deprecated body\.'

# A thematic break in the body is not front matter.
has main "${F}/plain/index.html" 'Plain body before rule\..*<hr>.*Plain body after rule\.'

# Status banners come from the source's front matter; the stub wins.
has main "${F}/experimental/index.html" 'class="as-status-banner as-status-banner--experimental"'
# The banner sits above the document header, not inside it.
has main "${F}/experimental/index.html" 'class="as-status-banner as-status-banner--experimental".*<div class="as-doc-header">'
has main "${F}/experimental/index.html" 'This feature is experimental and may change or be removed without notice\.'
has main "${F}/deprecated/index.html" 'class="as-status-banner as-status-banner--deprecated"'
has main "${F}/deprecated/index.html" 'Use the <a href="/docs/fixtures/experimental/">experimental page</a> instead\.'
lacks main "${F}/deprecated/index.html" 'will be removed in a future release'
has main "${F}/planned/index.html" 'class="as-status-banner as-status-banner--planned"'
has main "${F}/planned/index.html" 'This describes planned design that is not fully implemented yet\.'
has main "${F}/override/index.html" 'as-status-banner--experimental'
lacks main "${F}/override/index.html" 'as-status-banner--deprecated'
lacks main "${F}/plain/index.html" 'as-status-banner'
lacks main "${F}/index.html" 'as-status-banner'

# Copy as Markdown and llms-full.txt carry the status line.
has main "${F}/experimental/index.html" '\*\*Status: Experimental\.\*\* This feature is experimental'
has main llms-full.txt '(>|&gt;) \*\*Status: Deprecated\.\*\* Use the \[experimental page\]\(experimental\.md\) instead\.'
lacks main "${F}/plain/index.html" '\*\*Status:'

# A status outside the allowed set fails the build and names the value.
if build bogus testdata/hugo.bogus.yaml; then
  fail "bogus: build succeeded with status: experimantal"
elif ! grep -q 'experimantal' "${OUT}/bogus.log"; then
  fail "bogus: build error does not name the bad status"
fi

# Sidebar pills: status from front matter, "New" for docs added this release,
# but never "New" on a deprecated page.
SB="${F}/plain/index.html"
link='class="as-sb__link[^"]*"[^>]*>[[:space:]]*'
has main "${SB}" "${link}<span>Experimental Page</span>[[:space:]]*<span class=\"as-pill as-pill--experimental\">Experimental</span>"
has main "${SB}" "${link}<span>Deprecated Page</span>[[:space:]]*<span class=\"as-pill as-pill--deprecated\">Deprecated</span>"
has main "${SB}" "${link}<span>New Page</span>[[:space:]]*<span class=\"as-pill as-pill--new\" title=\"New in v0.3.0\">New</span>"
has main "${SB}" "${link}<span>New Deprecated Page</span>[[:space:]]*<span class=\"as-pill as-pill--deprecated\">Deprecated</span>[[:space:]]*</a>"
has main "${SB}" "${link}<span>Planned Page</span>[[:space:]]*<span class=\"as-pill as-pill--planned\">Planned</span>"
has main "${SB}" "${link}<span>Plain Page</span>[[:space:]]*</a>"

# Section cards show status pills but not "New".
has main "${F}/index.html" '<span>Experimental Page</span>[[:space:]]*<span class="as-pill as-pill--experimental">Experimental</span>'
has main "${F}/index.html" '<span>New Page</span>[[:space:]]*</p>'

# Without release data nothing is new.
if ! build nodata testdata/hugo.nodata.yaml; then
  cat "${OUT}/nodata.log" >&2
  fail "nodata: build failed"
else
  lacks nodata "${SB}" 'as-pill--new'
fi

# The glossary data comes from docs/glossary.md: notes dropped, continuation
# lines joined, sub-bullets skipped, fragment links pointed at the glossary.
# hugo build defaults to production, so the file name carries a fingerprint.
glossary_files=("${OUT}"/main/glossary*.json)
G="${glossary_files[0]##*/}"
[[ -f "${OUT}/main/${G}" ]] || fail "main: no glossary*.json published"
[[ "$(page main "${G}" | grep -o '"id":' | wc -l)" -eq 5 ]] || fail "${G}: expected 5 terms"
has main "${G}" '"glossaryURL":"/docs/reference/glossary/"'
has main "${G}" '"id":"actortemplate"'
has main "${G}" '"id":"ate-api-server"'
has main "${G}" '"id":"golden-snapshot"'
has main "${G}" 'the definition of an actor class, spanning two lines\.'
has main "${G}" 'glossary/#snapshots'
lacks main "${G}" 'API resource|binary|everything in memory'

# Doc pages point at the data; the glossary page does not decorate itself.
has main "${F}/plain/index.html" 'id="as-glossary-meta"'
has main "${F}/plain/index.html" '<script defer src="/js/glossary(\.min)?(\.[0-9a-f]+)?\.js"'
lacks main docs/reference/glossary/index.html 'js/glossary'
lacks main docs/reference/glossary/index.html 'id="as-glossary-meta"'

glossary_error empty 'no terms found'
glossary_error duplicate 'duplicate term'

# Every term gets an anchor on the glossary page; a label the anchor rewrite
# cannot find fails the build instead of shipping a dead card link.
for id in actor actortemplate snapshot-scope ate-api-server golden-snapshot; do
  has main docs/reference/glossary/index.html "<strong id=\"${id}\">"
done
glossary_error codeterm 'not found on the glossary page'

# A site without a glossary page builds without glossary data or script.
cat >"${OUT}/noglossary.yaml" <<EOF
baseURL: 'https://example.org/'
module:
  mounts:
    - {source: layouts, target: layouts}
    - {source: assets, target: assets}
    - {source: static, target: static}
    - {source: testdata/content, target: content, excludeFiles: ['docs/reference/**']}
    - {source: testdata/data, target: data}
    - {source: testdata/repo, target: assets/repo}
EOF
if ! build noglossary "${OUT}/noglossary.yaml"; then
  cat "${OUT}/noglossary.log" >&2
  fail "noglossary: build failed"
else
  noglossary_files=("${OUT}"/noglossary/glossary*.json)
  [[ ! -e "${noglossary_files[0]}" ]] || fail "noglossary: published ${noglossary_files[0]##*/}"
  lacks noglossary "${F}/plain/index.html" 'as-glossary-meta|js/glossary'
fi

# The card matching rules, without a browser.
# Node expands the quoted glob itself.
node --test "${ROOT}/site/testdata/js/*.test.js" >"${OUT}/node.log" 2>&1 || {
  cat "${OUT}/node.log" >&2
  fail "glossary.js unit tests"
}

if [[ "${failures}" -gt 0 ]]; then
  echo "${failures} check(s) failed" >&2
  exit 1
fi
echo "site templates OK"
