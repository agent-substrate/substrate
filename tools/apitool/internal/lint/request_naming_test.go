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

// requestNamingAPI builds a minimal API with one Control.DoThing method
// whose request is named requestName.
func requestNamingAPI(requestName string) *model.API {
	return &model.API{
		Services: []model.Service{{
			Name: "Control",
			Methods: []model.Method{
				{Name: "DoThing", ServiceName: "Control", InputName: "test." + requestName, OutputName: "test.DoThingResponse"},
			},
		}},
		Messages: []model.Message{
			{FullName: "test." + requestName, Name: requestName},
			{FullName: "test.DoThingResponse", Name: "DoThingResponse"},
		},
	}
}

func TestRequestNameMatchesMethod(t *testing.T) {
	tests := []struct {
		name        string
		requestName string
		wantFinding bool
	}{
		{"compliant", "DoThingRequest", false},
		{"wrong suffix", "DoThingReq", true},
		{"wrong verb", "GetThingRequest", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.RequestNameMatchesMethod.Check(requestNamingAPI(tt.requestName))
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if got := len(findings) > 0; got != tt.wantFinding {
				t.Errorf("Check() findings = %+v, want a finding: %v", findings, tt.wantFinding)
			}
		})
	}
}
