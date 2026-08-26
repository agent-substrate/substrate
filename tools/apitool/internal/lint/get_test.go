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

func TestGetRequestSingleObjectRef(t *testing.T) {
	objectRefField := model.Field{Name: "atespace", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}
	misnamedObjectRefField := model.Field{Name: "space", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}

	tests := []struct {
		name        string
		reqFields   []model.Field
		wantFinding bool
	}{
		{"single ObjectRef field", []model.Field{objectRefField}, false},
		{"no fields", nil, true},
		{"two fields", []model.Field{objectRefField, {Name: "extra"}}, true},
		{"single field, wrong type", []model.Field{{Name: "name"}}, true},
		{"single field, wrong name", []model.Field{misnamedObjectRefField}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := standardMethodAPI("test.Atespace", nil)
			api.Messages[1].Fields = tt.reqFields // test.GetAtespaceRequest
			findings, err := lint.GetRequestSingleObjectRef.Check(api)
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if got := len(findings) > 0; got != tt.wantFinding {
				t.Errorf("Check() findings = %+v, want a finding: %v", findings, tt.wantFinding)
			}
		})
	}
}

// TestGetRequestSingleObjectRef_IgnoresCreateUpdate confirms the rule
// ignores Create/Update, which legitimately embed the full resource.
func TestGetRequestSingleObjectRef_IgnoresCreateUpdate(t *testing.T) {
	findings, err := lint.GetRequestSingleObjectRef.Check(createAtespaceAPI())
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if len(findings) != 0 {
		t.Errorf("Check() = %+v, want no findings for a Create method", findings)
	}
}
