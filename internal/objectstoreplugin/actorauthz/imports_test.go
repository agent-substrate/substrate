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

package actorauthz

import (
	"errors"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

// Every provider's exchange imports this package, so it may not depend on one
// provider's wire format, credential or cloud SDK, directly or through
// another package.
func TestImportsNoProviderPackages(t *testing.T) {
	forbidden := []string{
		"go.ate.dev/substrate/internal/proto/objectstoreexchangepb",
		"go.ate.dev/substrate/internal/objectstoreplugin/credential",
		"go.ate.dev/substrate/internal/objectstoreplugin/gcsboundary",
		"go.ate.dev/substrate/internal/objectstoreplugin/gcsissuer",
		"cloud.google.com/go",
		"google.golang.org/api",
		"golang.org/x/oauth2/google",
		"github.com/aws",
		"github.com/Azure",
		"github.com/AzureAD",
	}
	// Vendor mode is what the build uses, and keeps a GOFLAGS=-mod=mod
	// environment from rewriting go.sum.
	out, err := exec.Command("go", "list", "-mod=vendor", "-deps", "-f", "{{.ImportPath}}", ".").Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			t.Fatalf("go list: %v\n%s", err, exitErr.Stderr)
		}
		t.Fatalf("go list: %v", err)
	}
	deps := strings.Fields(string(out))
	if !slices.Contains(deps, "go.ate.dev/substrate/internal/objectstoreplugin/actorauthz") {
		t.Fatalf("go list did not list this package:\n%s", out)
	}
	for _, dep := range deps {
		for _, f := range forbidden {
			if dep == f || strings.HasPrefix(dep, f+"/") {
				t.Errorf("actorauthz depends on %s", dep)
			}
		}
	}
}
