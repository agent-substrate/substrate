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

package validate

import (
	"os"
	"path/filepath"
	"testing"
)

// wantRepoRoot computes the repository root from this test file's known
// location, rather than relying on cwd the way repoRoot does.
func wantRepoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs(filepath.Join(wd, "..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("computed repo root %s does not contain go.mod: %v", root, err)
	}
	return root
}

func TestFindRepoRoot(t *testing.T) {
	// Computed once, before any subtest chdirs anywhere: wantRepoRoot
	// derives the root from the current working directory, so it must run
	// while cwd is still wherever `go test` started it.
	want := wantRepoRoot(t)

	orig, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })

	t.Run("succeeds from the repository root", func(t *testing.T) {
		if err := os.Chdir(want); err != nil {
			t.Fatal(err)
		}
		got, err := repoRoot()
		if err != nil {
			t.Fatalf("repoRoot() error = %v", err)
		}
		if got != want {
			t.Errorf("repoRoot() = %q, want %q", got, want)
		}
	})

	t.Run("succeeds from tools/apitool itself", func(t *testing.T) {
		// The real invocation pattern: apitool is its own module, so it's
		// always run as `cd tools/apitool && go run .` - cwd is never the
		// repository root itself, only a descendant of it.
		if err := os.Chdir(filepath.Join(want, "tools", "apitool")); err != nil {
			t.Fatal(err)
		}
		got, err := repoRoot()
		if err != nil {
			t.Fatalf("repoRoot() error = %v", err)
		}
		if got != want {
			t.Errorf("repoRoot() = %q, want %q", got, want)
		}
	})

	t.Run("fails outside the repository entirely", func(t *testing.T) {
		if err := os.Chdir(t.TempDir()); err != nil {
			t.Fatal(err)
		}
		if _, err := repoRoot(); err == nil {
			t.Fatal("repoRoot() error = nil, want error when not run from the repository root")
		}
	})
}
