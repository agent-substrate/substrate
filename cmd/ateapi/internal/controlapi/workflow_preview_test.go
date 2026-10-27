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

package controlapi

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// TestWorkflowsOptInToPreview checks that every exported workflow method
// begins with preview.ContextWithOptIn, so a request's preview opt-out does
// not limit the system state the workflow writes.
func TestWorkflowsOptInToPreview(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	found := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || !fn.Name.IsExported() || !isWorkflowReceiver(fn.Recv) {
				continue
			}
			found++
			if !startsWithOptIn(fn) {
				t.Errorf("%s: %s must begin with `ctx = preview.ContextWithOptIn(ctx)`", fset.Position(fn.Pos()), fn.Name.Name)
			}
		}
	}
	if found == 0 {
		t.Fatal("found no workflow methods; is the test looking in the right place?")
	}
}

func isWorkflowReceiver(recv *ast.FieldList) bool {
	star, ok := recv.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	id, ok := star.X.(*ast.Ident)
	return ok && (id.Name == "ActorWorkflow" || id.Name == "WorkerWorkflow")
}

func startsWithOptIn(fn *ast.FuncDecl) bool {
	if fn.Body == nil || len(fn.Body.List) == 0 {
		return false
	}
	assign, ok := fn.Body.List[0].(*ast.AssignStmt)
	if !ok || assign.Tok != token.ASSIGN || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
		return false
	}
	if lhs, ok := assign.Lhs[0].(*ast.Ident); !ok || lhs.Name != "ctx" {
		return false
	}
	call, ok := assign.Rhs[0].(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "ContextWithOptIn" {
		return false
	}
	if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "preview" {
		return false
	}
	arg, ok := call.Args[0].(*ast.Ident)
	return ok && arg.Name == "ctx"
}
