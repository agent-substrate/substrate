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

package lint_test

import (
	"testing"

	"github.com/agent-substrate/substrate/tools/apitool/internal/lint"
	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

func TestDeleteRequestShape(t *testing.T) {
	objectRefField := model.Field{Name: "atespace", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}
	misnamedObjectRefField := model.Field{Name: "space", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}
	optionsField := model.Field{Name: "options", TypeKind: "message", TypeFullName: "ateapi.DeleteOptions"}

	tests := []struct {
		name        string
		reqFields   []model.Field
		wantFinding bool
	}{
		{"ObjectRef alone", []model.Field{objectRefField}, false},
		{"ObjectRef plus DeleteOptions", []model.Field{objectRefField, optionsField}, false},
		{"no fields", nil, true},
		{"two ObjectRef fields", []model.Field{objectRefField, objectRefField}, true},
		{"two DeleteOptions fields", []model.Field{objectRefField, optionsField, optionsField}, true},
		{"loose control field outside DeleteOptions", []model.Field{objectRefField, {Name: "dry_run"}}, true},
		{"ObjectRef field, wrong name", []model.Field{misnamedObjectRefField}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.DeleteRequestShape.Check(deleteMethodAPI(tt.reqFields))
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if got := len(findings) > 0; got != tt.wantFinding {
				t.Errorf("Check() findings = %+v, want a finding: %v", findings, tt.wantFinding)
			}
		})
	}
}

// TestDeleteRequestShape_IgnoresCreateUpdate confirms the rule ignores
// Create/Update, which legitimately embed the full resource.
func TestDeleteRequestShape_IgnoresCreateUpdate(t *testing.T) {
	findings, err := lint.DeleteRequestShape.Check(createAtespaceAPI())
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if len(findings) != 0 {
		t.Errorf("Check() = %+v, want no findings for a Create method", findings)
	}
}

// deleteOptionsAPI builds a minimal API with just the shared
// ateapi.DeleteOptions message, with the given fields.
func deleteOptionsAPI(fields []model.Field) *model.API {
	return &model.API{
		Messages: []model.Message{
			{FullName: "ateapi.DeleteOptions", Name: "DeleteOptions", Fields: fields},
		},
	}
}

func TestDeleteOptionsShape(t *testing.T) {
	version := model.Field{Name: "version", TypeDisplay: "int64"}
	uid := model.Field{Name: "uid", TypeDisplay: "string"}

	tests := []struct {
		name        string
		fields      []model.Field
		wantFinding bool
	}{
		{"version and uid", []model.Field{version, uid}, false},
		{"version alone - presence not required", []model.Field{version}, false},
		{"uid alone - presence not required", []model.Field{uid}, false},
		{"no fields at all", nil, false},
		{"version wrong type", []model.Field{{Name: "version", TypeDisplay: "string"}, uid}, true},
		{"uid wrong type", []model.Field{version, {Name: "uid", TypeDisplay: "int64"}}, true},
		{"extra field beyond version/uid", []model.Field{version, uid, {Name: "dry_run", TypeDisplay: "bool"}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.DeleteOptionsShape.Check(deleteOptionsAPI(tt.fields))
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if got := len(findings) > 0; got != tt.wantFinding {
				t.Errorf("Check() findings = %+v, want a finding: %v", findings, tt.wantFinding)
			}
		})
	}
}

// TestDeleteOptionsShape_NotDeclared confirms the rule is a no-op when the
// API doesn't declare ateapi.DeleteOptions at all.
func TestDeleteOptionsShape_NotDeclared(t *testing.T) {
	findings, err := lint.DeleteOptionsShape.Check(&model.API{})
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if len(findings) != 0 {
		t.Errorf("Check() = %+v, want no findings when DeleteOptions isn't declared", findings)
	}
}
