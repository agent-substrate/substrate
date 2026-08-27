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

// resourceAPI builds a minimal API with one Control.GetAtespace method
// resolving to an "Atespace" resource with the given fields. The method
// name must contain a real entry in resourceNames (internal/model/resources.go) -
// Resources rejects anything else.
func resourceAPI(fields []model.Field) *model.API {
	return &model.API{
		Services: []model.Service{{
			Name: "Control",
			Methods: []model.Method{
				{Name: "GetAtespace", ServiceName: "Control", InputName: "test.GetAtespaceRequest", OutputName: "test.Atespace"},
			},
		}},
		Messages: []model.Message{
			{FullName: "test.Atespace", Name: "Atespace", Fields: fields},
			{FullName: "test.GetAtespaceRequest", Name: "GetAtespaceRequest"},
		},
	}
}

func TestResourceMetadata(t *testing.T) {
	tests := []struct {
		name       string
		fields     []model.Field
		wantFindig bool
	}{
		{
			name: "compliant",
			fields: []model.Field{
				{Name: "metadata", Number: 1, TypeKind: "message", TypeFullName: "ateapi.ResourceMetadata"},
			},
			wantFindig: false,
		},
		{
			name:       "missing entirely",
			fields:     []model.Field{{Name: "name", Number: 1}},
			wantFindig: true,
		},
		{
			name: "wrong type",
			fields: []model.Field{
				{Name: "metadata", Number: 1, TypeKind: "message", TypeFullName: "ateapi.ObjectRef"},
			},
			wantFindig: true,
		},
		{
			name: "wrong field number",
			fields: []model.Field{
				{Name: "metadata", Number: 2, TypeKind: "message", TypeFullName: "ateapi.ResourceMetadata"},
			},
			wantFindig: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.ResourceMetadata.Check(resourceAPI(tt.fields))
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if got := len(findings) > 0; got != tt.wantFindig {
				t.Errorf("Check() findings = %+v, want a finding: %v", findings, tt.wantFindig)
			}
		})
	}
}
