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

package parser

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// repoRoot computes the repository root from this test file's known
// location, rather than relying on cwd the way findRepoRoot does.
func repoRoot(t *testing.T) string {
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

func TestBuildFileDescriptor(t *testing.T) {
	root := repoRoot(t)

	fd, err := buildFileDescriptor(context.Background(), root)
	if err != nil {
		t.Fatalf("buildFileDescriptor() error = %v", err)
	}

	md := fd.Messages().ByName("ResourceMetadata")
	if md == nil {
		t.Fatal("ResourceMetadata message not found in descriptor")
	}
	if got := fd.SourceLocations().ByDescriptor(md).LeadingComments; !strings.Contains(got, "ResourceMetadata holds the common fields") {
		t.Errorf("ResourceMetadata leading comment = %q, want substring %q", got, "ResourceMetadata holds the common fields")
	}

	atespace := md.Fields().ByName("atespace")
	if atespace == nil {
		t.Fatal("ResourceMetadata.atespace field not found in descriptor")
	}
	if got := fd.SourceLocations().ByDescriptor(atespace).LeadingComments; !strings.Contains(got, "atespace is the namespace") {
		t.Errorf("atespace leading comment = %q, want substring %q", got, "atespace is the namespace")
	}
}

func TestFindRepoRoot(t *testing.T) {
	// Computed once, before any subtest chdirs anywhere: repoRoot derives
	// the root from the current working directory, so it must run while cwd
	// is still wherever `go test` started it.
	root := repoRoot(t)

	orig, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })

	t.Run("succeeds from the repository root", func(t *testing.T) {
		if err := os.Chdir(root); err != nil {
			t.Fatal(err)
		}
		got, err := findRepoRoot()
		if err != nil {
			t.Fatalf("findRepoRoot() error = %v", err)
		}
		if got != root {
			t.Errorf("findRepoRoot() = %q, want %q", got, root)
		}
	})

	t.Run("succeeds from tools/apitool itself", func(t *testing.T) {
		// The real invocation pattern: apitool is its own module, so it's
		// always run as `cd tools/apitool && go run .` - cwd is never the
		// repository root itself, only a descendant of it.
		if err := os.Chdir(filepath.Join(root, "tools", "apitool")); err != nil {
			t.Fatal(err)
		}
		got, err := findRepoRoot()
		if err != nil {
			t.Fatalf("findRepoRoot() error = %v", err)
		}
		if got != root {
			t.Errorf("findRepoRoot() = %q, want %q", got, root)
		}
	})

	t.Run("fails outside the repository entirely", func(t *testing.T) {
		if err := os.Chdir(t.TempDir()); err != nil {
			t.Fatal(err)
		}
		if _, err := findRepoRoot(); err == nil {
			t.Fatal("findRepoRoot() error = nil, want error when not run from the repository root")
		}
	})
}

// TestParse confirms the public entry point composes findRepoRoot and
// buildFileDescriptor correctly when run from a real cwd inside the repo
// (which `go test` gives it by default: the package's own directory).
func TestParse(t *testing.T) {
	fd, err := Parse(context.Background())
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if fd.Messages().ByName("ResourceMetadata") == nil {
		t.Error("Parse() descriptor missing ResourceMetadata message")
	}
}
