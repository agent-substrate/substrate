// Copyright 2026 The Agent Substrate Authors
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

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFiles(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for rel, content := range files {
		full := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}
}

func TestGithubSlug(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{{
		name:  "plain words",
		input: "Quick Start",
		want:  "quick-start",
	}, {
		name:  "punctuation with spaces keeps double hyphen",
		input: "Optional: Prometheus + Grafana",
		want:  "optional-prometheus--grafana",
	}, {
		name:  "parentheses and numbers",
		input: "1. Quickstart (Development)",
		want:  "1-quickstart-development",
	}, {
		name:  "underscores preserved",
		input: "12. Sensitive fields (debug_redact)",
		want:  "12-sensitive-fields-debug_redact",
	}, {
		name:  "em-dash surrounded by spaces",
		input: "2.3 ObjectRef — reference type",
		want:  "23-objectref--reference-type",
	}, {
		name:  "apostrophe and question mark",
		input: "What's next?",
		want:  "whats-next",
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := githubSlug(tc.input); got != tc.want {
				t.Errorf("githubSlug(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestCheckFile(t *testing.T) {
	baseFiles := map[string]string{
		"README.md": strings.Join([]string{
			"# Project",
			"",
			"## Quickstart (Development)",
			"",
			"## Settings",
			"",
			"## Settings",
			"",
			"Setext Heading",
			"==============",
			"",
			"## https://example.com",
			"",
			"## ![icon](assets/logo.svg) With Icon",
			"",
			`<a id="CustomAnchor"></a>`,
			`<a name="legacy-anchor"></a>`,
		}, "\n"),
		"docs/target.md": strings.Join([]string{
			"---",
			"title: Target",
			"---",
			"",
			"# Target Doc",
			"",
			"## Optional: Prometheus + Grafana",
			"",
			"### `debug_redact` & Friends",
		}, "\n"),
		"docs/sub/README.md":  "# Subdir\n\n## Details\n",
		"docs/empty/keep.txt": "keep\n",
		"docs/has space.md":   "# Spaced\n",
		"docs/foo(1).md":      "# Parens\n",
		"docs/a&b.md":         "# Ampersand\n",
		"assets/logo.svg":     "<svg/>\n",
		"assets/dark.svg":     "<svg/>\n",
		"cmd/main.go":         "package main\n",
	}

	tests := []struct {
		name    string
		srcPath string
		srcBody string
		want    []problem
	}{{
		name:    "valid links across syntax forms",
		srcPath: "docs/index.md",
		srcBody: strings.Join([]string{
			"---",
			"title: Index",
			"---",
			"",
			"# Index",
			"",
			"See [readme](../README.md#quickstart-development).",
			"Duplicate heading: [second](../README.md#settings-1).",
			"Setext heading: [setext](../README.md#setext-heading).",
			"Autolink heading: [auto](../README.md#httpsexamplecom).",
			"Image in heading skipped: [icon](../README.md#with-icon).",
			"HTML anchors: [id](../README.md#CustomAnchor) and [name](../README.md#legacy-anchor).",
			"Cross-file heading: [prom](target.md#optional-prometheus--grafana).",
			"Code in heading: [redact](target.md#debug_redact--friends).",
			"Escaped punctuation and entities: [p](foo\\(1\\).md), [u](target.md#debug\\_redact--friends), [e](a&amp;b.md).",
			"Angle-bracket path with spaces: [angle](<has space.md#spaced>).",
			"Same-file and top-of-page: [self](#index), [top](#).",
			"Root-relative: [root](/docs/target.md#target-doc).",
			"Directory with README: [sub](sub/#details).",
			"Directory without fragment: [empty](empty/).",
			"Percent-encoded path: [spaced](has%20space.md#spaced).",
			"Link with title: [titled](target.md \"Target\").",
			"Reference link: [ref link][t].",
			"Image: ![logo](../assets/logo.svg).",
			"HTML link: <a href=\"../README.md#project\">README</a>.",
			"<picture>",
			`  <source media="(prefers-color-scheme: dark)" srcset="../assets/dark.svg">`,
			`  <img src="../assets/logo.svg" srcset="../assets/logo.svg 1x, ../assets/dark.svg 2x" alt="logo">`,
			"</picture>",
			"Non-markdown fragment: [code](../cmd/main.go#L1).",
			"Query string: [q](target.md?plain=1#target-doc).",
			"External links: [web](https://example.com/404), [mail](mailto:a@b.c), [proto](//example.com/x).",
			"Inline code ignored: `[text](does-not-exist.md)` and `^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`.",
			"<!-- <a href=\"commented-out.md\">ignored</a> -->",
			"<div><script>var s = '<a href=\"ignored-in-script.md\">x</a>';</script></div>",
			"",
			"```markdown",
			"[fenced](missing-in-fence.md)",
			"<a href=\"missing-html-in-fence.md\">x</a>",
			"```",
			"",
			"[t]: target.md#target-doc",
		}, "\n"),
	}, {
		name:    "invalid missing relative file",
		srcPath: "docs/broken.md",
		srcBody: "# Broken\n\nSee [missing](nope.md).\n",
		want: []problem{{
			file:   "docs/broken.md",
			line:   3,
			target: "nope.md",
			reason: "file not found",
		}},
	}, {
		name:    "invalid missing same-file and cross-file anchors with CRLF front matter",
		srcPath: "docs/broken.md",
		srcBody: strings.Join([]string{
			"---",
			"title: Broken",
			"---",
			"",
			"# Broken",
			"",
			"See [self](#absent) and [other](target.md#no-such-heading).",
		}, "\r\n"),
		want: []problem{{
			file:   "docs/broken.md",
			line:   7,
			target: "#absent",
			reason: "anchor #absent not found",
		}, {
			file:   "docs/broken.md",
			line:   7,
			target: "target.md#no-such-heading",
			reason: "anchor #no-such-heading not found",
		}},
	}, {
		name:    "invalid wrong-case anchor",
		srcPath: "docs/broken.md",
		srcBody: "# Broken\n\n[bad](target.md#Target-Doc) and [html](../README.md#customanchor)\n",
		want: []problem{{
			file:   "docs/broken.md",
			line:   3,
			target: "target.md#Target-Doc",
			reason: "anchor #Target-Doc not found",
		}, {
			file:   "docs/broken.md",
			line:   3,
			target: "../README.md#customanchor",
			reason: "anchor #customanchor not found",
		}},
	}, {
		name:    "invalid path escape, trailing slash, empty fragment, empty target, and file URL",
		srcPath: "docs/broken.md",
		srcBody: strings.Join([]string{
			"# Broken",
			"",
			"[escape](../../outside.md)",
			"[slash](target.md/)",
			"[dangling](target.md#)",
			"[empty]()",
			"[local](file:///etc/hosts)",
		}, "\n"),
		want: []problem{{
			file:   "docs/broken.md",
			line:   3,
			target: "../../outside.md",
			reason: "path escapes repository root",
		}, {
			file:   "docs/broken.md",
			line:   4,
			target: "target.md/",
			reason: "not a directory",
		}, {
			file:   "docs/broken.md",
			line:   5,
			target: "target.md#",
			reason: "empty anchor fragment",
		}, {
			file:   "docs/broken.md",
			line:   6,
			target: "",
			reason: "empty link target",
		}, {
			file:   "docs/broken.md",
			line:   7,
			target: "file:///etc/hosts",
			reason: "local file:// URL",
		}},
	}, {
		name:    "invalid HTML href, img src, and picture source srcset",
		srcPath: "docs/broken.md",
		srcBody: strings.Join([]string{
			"# Broken",
			"",
			`<a href="missing.md">link</a>`,
			`<img src="missing.svg" alt="missing">`,
			`<picture>`,
			`  <source srcset="../assets/logo.svg 1x, ../assets/missing-dark.svg 2x">`,
			`</picture>`,
		}, "\n"),
		want: []problem{{
			file:   "docs/broken.md",
			line:   3,
			target: "missing.md",
			reason: "file not found",
		}, {
			file:   "docs/broken.md",
			line:   4,
			target: "missing.svg",
			reason: "file not found",
		}, {
			file:   "docs/broken.md",
			line:   6,
			target: "../assets/missing-dark.svg",
			reason: "file not found",
		}},
	}, {
		name:    "invalid directory fragment without README",
		srcPath: "docs/broken.md",
		srcBody: "# Broken\n\n[empty dir](empty/#heading)\n",
		want: []problem{{
			file:   "docs/broken.md",
			line:   3,
			target: "empty/#heading",
			reason: "anchor #heading not found (directory has no README.md)",
		}},
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFiles(t, root, baseFiles)
			writeFiles(t, root, map[string]string{tc.srcPath: tc.srcBody})

			c, err := newChecker(root)
			if err != nil {
				t.Fatalf("newChecker: %v", err)
			}
			got, err := c.checkFile(tc.srcPath)
			if err != nil {
				t.Fatalf("checkFile: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %d problems %v, want %d %v", len(got), got, len(tc.want), tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("problem[%d] = %+v, want %+v", i, got[i], tc.want[i])
				}
			}
		})
	}
}
