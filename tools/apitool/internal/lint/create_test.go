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

func TestCreateRequestShape(t *testing.T) {
	resourceField := model.Field{Name: "atespace", TypeKind: "message", TypeFullName: "test.Atespace"}
	misnamedResourceField := model.Field{Name: "space", TypeKind: "message", TypeFullName: "test.Atespace"}
	optionsField := model.Field{Name: "options", TypeKind: "message", TypeFullName: "ateapi.CreateOptions"}

	tests := []struct {
		name        string
		reqFields   []model.Field
		wantFinding bool
	}{
		{"resource field alone", []model.Field{resourceField}, false},
		{"resource field plus CreateOptions", []model.Field{resourceField, optionsField}, false},
		{"no fields", nil, true},
		{"two resource fields", []model.Field{resourceField, resourceField}, true},
		{"two CreateOptions fields", []model.Field{resourceField, optionsField, optionsField}, true},
		{"loose control field outside CreateOptions", []model.Field{resourceField, {Name: "dry_run"}}, true},
		{"resource field, wrong name", []model.Field{misnamedResourceField}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.CreateRequestShape.Check(createMethodAPI(tt.reqFields))
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if got := len(findings) > 0; got != tt.wantFinding {
				t.Errorf("Check() findings = %+v, want a finding: %v", findings, tt.wantFinding)
			}
		})
	}
}

// TestCreateRequestShape_IgnoresGetDelete confirms the rule ignores
// Get/Delete, which reference the resource via ObjectRef instead.
func TestCreateRequestShape_IgnoresGetDelete(t *testing.T) {
	findings, err := lint.CreateRequestShape.Check(deleteMethodAPI([]model.Field{
		{Name: "atespace", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"},
	}))
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if len(findings) != 0 {
		t.Errorf("Check() = %+v, want no findings for a Delete method", findings)
	}
}
