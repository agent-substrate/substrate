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

func TestDocumented(t *testing.T) {
	tests := []struct {
		name           string
		messageComment string
		enumComment    string
		fieldComment   string
		methodComment  string
		wantFindings   int
	}{
		{"all documented", "a request", "a state", "does a thing", "does another thing", 0},
		{"message undocumented", "", "a state", "does a thing", "does another thing", 1},
		{"enum undocumented", "a request", "", "does a thing", "does another thing", 1},
		{"field undocumented", "a request", "a state", "", "does another thing", 1},
		{"method undocumented", "a request", "a state", "does a thing", "", 1},
		{"nothing documented", "", "", "", "", 4},
		{"whitespace-only comment counts as undocumented", "\t", "\t", "   ", "\n", 4},
		{"empty comment lines", "a request", "a state", "\n\n", "does another thing", 1},
		{"tags only", "+k8s:required", "+k8s:optional", "+k8s:required\n+k8s:format=k8s-short-name", "does another thing", 3},
		{"prose and tags", "a request\n\n+k8s:required", "a state", "does a thing\n\n+k8s:immutable", "does another thing", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := &model.API{
				Services: []model.Service{{
					Name: "Control",
					Methods: []model.Method{
						{Name: "DoThing", ServiceName: "Control", Comment: tt.methodComment, InputName: "test.DoThingRequest", OutputName: "test.DoThingResponse"},
					},
				}},
				Messages: []model.Message{
					{FullName: "test.DoThingRequest", Name: "DoThingRequest", Comment: tt.messageComment, Fields: []model.Field{
						{Name: "widget", Comment: tt.fieldComment},
					}},
				},
				Enums: []model.Enum{
					{FullName: "test.Widget.State", Name: "Widget.State", ParentFullName: "test.Widget", Comment: tt.enumComment},
				},
			}
			findings, err := lint.Documented.Check(api)
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if len(findings) != tt.wantFindings {
				t.Errorf("Check() = %+v, want %d finding(s)", findings, tt.wantFindings)
			}
		})
	}
}
