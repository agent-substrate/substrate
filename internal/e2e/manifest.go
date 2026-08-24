// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// renderManifest substitutes placeholders into the manifest template at relPath
// (repo-relative), writes the result into the test's temp dir and returns that
// path. Both an apply and a later delete can then consume the same file, with
// no shell involved.
//
// Templates carry two kinds of ${...} placeholder:
//
//   - inline, substituted wherever they appear (an empty value just disappears);
//   - block, which must be the entire content of their line. They expand to a
//     YAML fragment that brings its own indentation, and an empty value takes
//     the whole line with it — the same trick hack/install-demo-counter.sh
//     plays with `sed /.../d`. Requiring the placeholder to be the whole line
//     is what lets a comment mention one without being deleted.
func renderManifest(t *testing.T, relPath string, inline, blocks map[string]string) string {
	t.Helper()
	root, err := FindRepoRoot()
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(root, relPath))
	if err != nil {
		t.Fatalf("reading manifest template %s: %v", relPath, err)
	}

	var out []string
	for line := range strings.SplitSeq(string(raw), "\n") {
		if value, isBlock := blocks[strings.TrimSpace(line)]; isBlock {
			if value != "" {
				out = append(out, value)
			}
			continue
		}
		for placeholder, value := range inline {
			line = strings.ReplaceAll(line, placeholder, value)
		}
		out = append(out, line)
	}

	rendered := strings.TrimSuffix(filepath.Join(t.TempDir(), filepath.Base(relPath)), ".tmpl")
	if err := os.WriteFile(rendered, []byte(strings.Join(out, "\n")), 0o644); err != nil {
		t.Fatalf("writing rendered manifest %s: %v", rendered, err)
	}
	return rendered
}

// yamlListBlock renders items as `key:` followed by their YAML, every line
// indented by indent spaces, for substitution into a block placeholder. An
// empty list renders to "", which takes the placeholder's whole line — key
// included, since a bare `volumes:` with nothing under it is not what the
// caller meant.
//
// Marshaling the real API types rather than asking callers for YAML text is
// what keeps a fragment honest: a misspelled field is a compile error here,
// where in a template it would apply cleanly and do nothing.
func yamlListBlock[T any](t *testing.T, key string, items []T, indent int) string {
	t.Helper()
	if len(items) == 0 {
		return ""
	}
	raw, err := yaml.Marshal(items)
	if err != nil {
		t.Fatalf("marshaling %s for the manifest: %v", key, err)
	}
	pad := strings.Repeat(" ", indent)
	out := []string{pad + key + ":"}
	for line := range strings.SplitSeq(strings.TrimRight(string(raw), "\n"), "\n") {
		out = append(out, pad+line)
	}
	return strings.Join(out, "\n")
}

// koApply builds and pushes the ko:// images named in manifest and applies it.
//
// Through the repo's pinned ko (hack/run-tool.sh), because CI does not install
// ko on PATH and every other deploy in this repo goes through that wrapper. The
// trailing `-- --context=...` mirrors run_ko in hack/install-ate.sh: ko's apply
// subcommand forwards args after `--` to kubectl. KO_CONFIG_PATH is required
// because ko resolves .ko.yaml from its working directory, which is the test's
// package dir rather than the repo root; without it the build silently loses
// defaultPlatforms and produces images that cannot run on the cluster's nodes.
func koApply(t *testing.T, manifest string) {
	t.Helper()
	root, err := FindRepoRoot()
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}
	applyArgs := []string{"ko", "apply", "-f", manifest}
	if KubeContext != "" {
		applyArgs = append(applyArgs, "--", "--context="+KubeContext)
	}
	RunCmdWithEnv(t, []string{"KO_CONFIG_PATH=" + root}, filepath.Join(root, "hack/run-tool.sh"), applyArgs...)
}
