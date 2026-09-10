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

package installdefaults

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// This guard exists because hardcoded install-layout assumptions fail closed
// in a relocated or renamed install, in ways that do not point at the naming
// (see the ateletauth, ateletdial and NetworkPolicy histories). New namespace
// or SPIFFE-identity literals belong in this package, in a flag default, or —
// deliberately — in the allowlist below.

// excludedTrees are directories whose tooling targets the canonical install
// layout by design and is not expected to work against a relocated one.
var excludedTrees = []string{
	"cmd/ate-setup",         // canonical GCP installer
	"cmd/benchmarking",      // canonical load-testing workers
	"internal/benchmarking", // canonical load-testing harness
}

// allowedLiterals maps a repo-relative file to the canonical-layout string
// literals it may carry. Everything here is either a flag/env default that an
// operator overrides (the sanctioned pattern: default to the canonical
// layout, configure the rest), or a string nothing verifies against.
var allowedLiterals = map[string][]string{
	// Flag defaults, overridden by the deployment or the e2e manifest templates.
	"cmd/atecontroller/main.go":                       {"k8s:///api.ate-system.svc:443"},
	"cmd/atelet/main.go":                              {"k8s:///api.ate-system.svc:443", "api.ate-system.svc"},
	"cmd/atenet/internal/router/cmd.go":               {"k8s:///api.ate-system.svc:443", "spiffe://cluster.local/"},
	"internal/e2e/fixtures/testserver/egressprobe.go": {"atenet-egress.ate-system.svc:443"},
	// Env-var defaults, overridden by ATE_* / E2E_* variables.
	"internal/ateclient/builder.go": {"api.ate-system.svc"},
	// Inert: the actor JWT issuer is an upstream TODO nothing validates, and
	// an x509 template's Issuer field is overwritten by the signer.
	"cmd/ateapi/internal/actoridentity/actoridentity.go": {"https://api.ate-system.svc", "api.ate-system.svc.cluster.local"},
}

// suspect reports whether a string literal encodes canonical install layout.
func suspect(s string) bool {
	return strings.Contains(s, "ate-system") || strings.Contains(s, "spiffe://cluster.local")
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test file")
	}
	return filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
}

func allowed(relPath, lit string) bool {
	for _, tree := range excludedTrees {
		if strings.HasPrefix(relPath, tree+string(filepath.Separator)) {
			return true
		}
	}
	for _, a := range allowedLiterals[filepath.ToSlash(relPath)] {
		if a == lit {
			return true
		}
	}
	return false
}

// TestNoNewHardcodedInstallLayoutInGo walks every non-test Go file under cmd/
// and internal/ and fails on string literals that name the canonical
// namespace or a full SPIFFE identity outside this package. Comments are not
// scanned, so kubebuilder markers (which must name a literal namespace) pass.
func TestNoNewHardcodedInstallLayoutInGo(t *testing.T) {
	root := repoRoot(t)
	selfDir := filepath.Join("internal", "installdefaults")

	var offenders []string
	for _, tree := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, tree), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			if strings.HasPrefix(rel, selfDir) {
				return nil
			}
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			ast.Inspect(f, func(n ast.Node) bool {
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				val, err := strconv.Unquote(lit.Value)
				if err != nil {
					return true
				}
				if suspect(val) && !allowed(rel, val) {
					offenders = append(offenders, fset.Position(lit.Pos()).String()+": "+lit.Value)
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	if len(offenders) > 0 {
		t.Errorf("hardcoded install-layout strings outside internal/installdefaults; "+
			"derive them from installdefaults or a configured value, or allowlist them "+
			"in this test with a justification:\n  %s", strings.Join(offenders, "\n  "))
	}
}
