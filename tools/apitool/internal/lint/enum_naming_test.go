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

func enumAPI(enums []model.Enum) *model.API {
	return &model.API{Enums: enums}
}

func TestEnumZeroValueUnspecified(t *testing.T) {
	tests := []struct {
		name        string
		enum        model.Enum
		wantFinding bool
	}{
		{
			name: "compliant top-level",
			enum: model.Enum{FullName: "test.ActorState", Name: "ActorState", Values: []model.EnumValue{
				{Name: "ACTOR_STATE_UNSPECIFIED", Number: 0},
				{Name: "ACTOR_STATE_RUNNING", Number: 1},
			}},
			wantFinding: false,
		},
		{
			name: "compliant nested",
			enum: model.Enum{FullName: "test.Worker.State", Name: "Worker.State", ParentFullName: "test.Worker", Values: []model.EnumValue{
				{Name: "STATE_UNSPECIFIED", Number: 0},
				{Name: "STATE_ACTIVE", Number: 1},
			}},
			wantFinding: false,
		},
		{
			name: "wrong zero-value name",
			enum: model.Enum{FullName: "test.ActorState", Name: "ActorState", Values: []model.EnumValue{
				{Name: "UNKNOWN", Number: 0},
			}},
			wantFinding: true,
		},
		{
			name: "no zero value at all",
			enum: model.Enum{FullName: "test.ActorState", Name: "ActorState", Values: []model.EnumValue{
				{Name: "ACTOR_STATE_RUNNING", Number: 1},
			}},
			wantFinding: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.EnumZeroValueUnspecified.Check(enumAPI([]model.Enum{tt.enum}))
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if got := len(findings) > 0; got != tt.wantFinding {
				t.Errorf("Check() findings = %+v, want a finding: %v", findings, tt.wantFinding)
			}
		})
	}
}

func TestEnumValuesPrefixed(t *testing.T) {
	tests := []struct {
		name        string
		enum        model.Enum
		wantFinding bool
	}{
		{
			name: "compliant top-level",
			enum: model.Enum{FullName: "test.SandboxClass", Name: "SandboxClass", Values: []model.EnumValue{
				{Name: "SANDBOX_CLASS_UNSPECIFIED", Number: 0},
				{Name: "SANDBOX_CLASS_GVISOR", Number: 1},
			}},
			wantFinding: false,
		},
		{
			name: "unprefixed top-level value",
			enum: model.Enum{FullName: "test.SandboxClass", Name: "SandboxClass", Values: []model.EnumValue{
				{Name: "UNSPECIFIED", Number: 0},
				{Name: "GVISOR", Number: 1},
			}},
			wantFinding: true,
		},
		{
			name: "compliant nested",
			enum: model.Enum{FullName: "test.Worker.State", Name: "Worker.State", ParentFullName: "test.Worker", Values: []model.EnumValue{
				{Name: "STATE_UNSPECIFIED", Number: 0},
				{Name: "STATE_ACTIVE", Number: 1},
			}},
			wantFinding: false,
		},
		{
			name: "unprefixed nested value",
			enum: model.Enum{FullName: "test.Worker.State", Name: "Worker.State", ParentFullName: "test.Worker", Values: []model.EnumValue{
				{Name: "UNSPECIFIED", Number: 0},
				{Name: "ACTIVE", Number: 1},
			}},
			wantFinding: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.EnumValuesPrefixed.Check(enumAPI([]model.Enum{tt.enum}))
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if got := len(findings) > 0; got != tt.wantFinding {
				t.Errorf("Check() findings = %+v, want a finding: %v", findings, tt.wantFinding)
			}
		})
	}
}
