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

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// newRepo returns an empty git repository with commit signing off.
func newRepo(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	dir := t.TempDir()
	mustGit(t, dir, "init", "--quiet", "--initial-branch=main")
	mustGit(t, dir, "config", "user.email", "test@example.com")
	mustGit(t, dir, "config", "user.name", "Test")
	mustGit(t, dir, "config", "commit.gpgsign", "false")
	mustGit(t, dir, "config", "tag.gpgsign", "false")
	return dir
}

func mustGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	if _, err := git(dir, args...); err != nil {
		t.Fatal(err)
	}
}

// commit writes each path with distinct content, so rename detection only
// pairs real moves, commits everything, and applies the given tags.
func commit(t *testing.T, dir string, paths []string, tags ...string) {
	t.Helper()
	for _, p := range paths {
		full := filepath.Join(dir, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		content := "# " + p + "\n\nBody of " + p + ", long enough to be a distinct file.\n"
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	mustGit(t, dir, "add", "-A")
	mustGit(t, dir, "commit", "--quiet", "--allow-empty", "-m", "commit")
	for _, tag := range tags {
		mustGit(t, dir, "tag", tag)
	}
}

func TestCompute(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, dir string)
		ref   string
		want  releaseData
	}{
		{
			name: "no release tags",
			setup: func(t *testing.T, dir string) {
				commit(t, dir, []string{"site/hugo.yaml", "docs/a.md"})
			},
			ref:  "HEAD",
			want: releaseData{Added: []string{}},
		},
		{
			name: "previous release predates the site",
			setup: func(t *testing.T, dir string) {
				commit(t, dir, []string{"docs/a.md"}, "v0.2.0")
				commit(t, dir, []string{"site/hugo.yaml", "docs/b.md"}, "v0.3.0")
			},
			ref:  "v0.3.0",
			want: releaseData{Version: "v0.3.0", Previous: "v0.2.0", Added: []string{}},
		},
		{
			name: "added markdown since the previous release",
			setup: func(t *testing.T, dir string) {
				commit(t, dir, []string{"site/hugo.yaml", "docs/a.md", "docs/keep.md"}, "v0.2.0")
				mustGit(t, dir, "mv", "docs/a.md", "docs/moved.md")
				commit(t, dir, []string{"docs/b.md", "demos/x/README.md", "docs/img.png"}, "v0.3.0")
			},
			ref:  "v0.3.0",
			want: releaseData{Version: "v0.3.0", Previous: "v0.2.0", Added: []string{"demos/x/README.md", "docs/b.md"}},
		},
		{
			name: "refs/tags prefix is accepted",
			setup: func(t *testing.T, dir string) {
				commit(t, dir, []string{"site/hugo.yaml"}, "v0.2.0")
				commit(t, dir, []string{"docs/b.md"}, "v0.3.0")
			},
			ref:  "refs/tags/v0.3.0",
			want: releaseData{Version: "v0.3.0", Previous: "v0.2.0", Added: []string{"docs/b.md"}},
		},
		{
			name: "versions compare numerically, not as strings",
			setup: func(t *testing.T, dir string) {
				commit(t, dir, []string{"site/hugo.yaml"}, "v0.9.0")
				commit(t, dir, []string{"docs/ten.md"}, "v0.10.0")
				commit(t, dir, []string{"docs/c.md"})
			},
			ref:  "HEAD",
			want: releaseData{Previous: "v0.10.0", Added: []string{"docs/c.md"}},
		},
		{
			name: "previous is the highest release below the ref",
			setup: func(t *testing.T, dir string) {
				commit(t, dir, []string{"site/hugo.yaml"}, "v0.2.0")
				commit(t, dir, []string{"docs/three.md"}, "v0.3.0")
				mustGit(t, dir, "checkout", "--quiet", "-b", "release-0.2", "v0.2.0")
				commit(t, dir, []string{"docs/patch.md"}, "v0.2.1")
			},
			ref:  "v0.2.1",
			want: releaseData{Version: "v0.2.1", Previous: "v0.2.0", Added: []string{"docs/patch.md"}},
		},
		{
			name: "pre-release tags are ignored",
			setup: func(t *testing.T, dir string) {
				commit(t, dir, []string{"site/hugo.yaml"}, "v0.2.0")
				commit(t, dir, []string{"docs/rc.md"}, "v0.3.0-rc.1")
			},
			ref:  "v0.3.0-rc.1",
			want: releaseData{Previous: "v0.2.0", Added: []string{"docs/rc.md"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := newRepo(t)
			tc.setup(t, dir)
			got, err := compute(dir, tc.ref)
			if err != nil {
				t.Fatalf("compute: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("compute(%q) = %+v, want %+v", tc.ref, got, tc.want)
			}
		})
	}
}

func TestComputeUnknownRef(t *testing.T) {
	dir := newRepo(t)
	commit(t, dir, []string{"docs/a.md"})
	_, err := compute(dir, "nope")
	if err == nil || !strings.Contains(err.Error(), `"nope"`) {
		t.Fatalf("compute(nope) error = %v, want one naming the ref", err)
	}
}

func TestRunWritesJSON(t *testing.T) {
	dir := newRepo(t)
	commit(t, dir, []string{"docs/a.md"})
	out := filepath.Join(t.TempDir(), "data", "release.json")
	if err := run(dir, "HEAD", out); err != nil {
		t.Fatalf("run: %v", err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	want := "{\n  \"version\": \"\",\n  \"previous\": \"\",\n  \"added\": []\n}\n"
	if string(got) != want {
		t.Errorf("release.json = %q, want %q", got, want)
	}
}
