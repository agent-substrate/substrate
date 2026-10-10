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

	"github.com/google/go-cmp/cmp"

	"github.com/agent-substrate/substrate/tools/apitool/internal/lint"
	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

func TestRequestNameMatchesMethod(t *testing.T) {
	tests := []struct {
		name string
		api  *model.API
		want []lint.Finding
	}{
		{
			name: "compliant",
			api: methodAPI(model.Method{Name: "DoThing", InputName: "test.DoThingRequest", OutputName: "test.DoThingResponse"},
				msg("DoThingRequest"),
				msg("DoThingResponse")),
		},
		{
			name: "wrong suffix",
			api: methodAPI(model.Method{Name: "DoThing", InputName: "test.DoThingReq", OutputName: "test.DoThingResponse"},
				msg("DoThingReq"),
				msg("DoThingResponse")),
			want: []lint.Finding{
				{Subject: "test.Control.DoThing", Message: `request message is named "DoThingReq", want "DoThingRequest"`},
			},
		},
		{
			name: "wrong verb",
			api: methodAPI(model.Method{Name: "DoThing", InputName: "test.GetThingRequest", OutputName: "test.DoThingResponse"},
				msg("GetThingRequest"),
				msg("DoThingResponse")),
			want: []lint.Finding{
				{Subject: "test.Control.DoThing", Message: `request message is named "GetThingRequest", want "DoThingRequest"`},
			},
		},
		{
			name: "request not declared in this API",
			api: methodAPI(model.Method{Name: "DoThing", InputName: "test.DoThingRequest", OutputName: "test.DoThingResponse"},
				msg("DoThingResponse")),
			want: []lint.Finding{
				{Subject: "test.Control.DoThing", Message: "request type test.DoThingRequest not found"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.RequestNameMatchesMethod.Check(tt.api)
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSubresourceNaming(t *testing.T) {
	tests := []struct {
		name string
		api  *model.API
		want []lint.Finding
	}{
		{
			name: "Get naming a parent",
			api: methodAPI(rpc("GetGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil)),
		},
		{
			name: "Get naming Global",
			api: methodAPI(rpc("GetGlobalWidget", "Widget", "Widget"),
				resource("Widget", []string{"Global"})),
		},
		{
			name: "Get naming the second of two parents",
			api: methodAPI(rpc("GetGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Global", "Gadget"}),
				resource("Gadget", nil)),
		},
		{
			name: "Get naming no parent",
			api: methodAPI(rpc("GetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil)),
			want: []lint.Finding{
				{Subject: "test.Control.GetWidget", Message: "want Get, then one of the parents Gadget, then Widget"},
			},
		},
		{
			name: "Get naming a resource that isn't a parent",
			api: methodAPI(rpc("GetGadgetBoxWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("GadgetBox", nil),
				resource("Gadget", nil)),
			want: []lint.Finding{
				{Subject: "test.Control.GetGadgetBoxWidget", Message: "want Get, then one of the parents Gadget, then Widget"},
			},
		},
		{
			name: "Get naming neither of two parents",
			api: methodAPI(rpc("GetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Global", "Gadget"}),
				resource("Gadget", nil)),
			want: []lint.Finding{
				{Subject: "test.Control.GetWidget", Message: "want Get, then one of the parents Global/Gadget, then Widget"},
			},
		},
		{
			name: "List naming a parent and the plural",
			api: methodAPI(rpc("ListGadgetWidgets", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil)),
		},
		{
			name: "List with the singular",
			api: methodAPI(rpc("ListGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil)),
			want: []lint.Finding{
				{Subject: "test.Control.ListGadgetWidget", Message: "want List, then one of the parents Gadget, then Widgets"},
			},
		},
		{
			name: "List naming no parent",
			api: methodAPI(rpc("ListWidgets", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil)),
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: "want List, then one of the parents Gadget, then Widgets"},
			},
		},
		{
			name: "List pluralizing a consonant-y name",
			api: methodAPI(rpc("ListGadgetPolicies", "Policy", "Policy"),
				resource("Policy", []string{"Gadget"}),
				resource("Gadget", nil)),
		},
		{
			name: "List pluralizing a consonant-y name with s",
			api: methodAPI(rpc("ListGadgetPolicys", "Policy", "Policy"),
				resource("Policy", []string{"Gadget"}),
				resource("Gadget", nil)),
			want: []lint.Finding{
				{Subject: "test.Control.ListGadgetPolicys", Message: "want List, then one of the parents Gadget, then Policies"},
			},
		},
		{
			name: "List pluralizing a vowel-y name",
			api: methodAPI(rpc("ListGadgetKeys", "Key", "Key"),
				resource("Key", []string{"Gadget"}),
				resource("Gadget", nil)),
		},
		{
			name: "List pluralizing a name ending in s",
			api: methodAPI(rpc("ListGadgetAddresses", "Address", "Address"),
				resource("Address", []string{"Gadget"}),
				resource("Gadget", nil)),
		},
		{
			name: "custom method",
			api: methodAPI(rpc("DrainWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil)),
		},
		{
			name: "word starting with a verb is not a verb",
			api: methodAPI(rpc("Getaway", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil)),
		},
		{
			name: "top-level resource is not checked",
			api: methodAPI(rpc("GetWidgetThing", "Widget", "Widget"),
				resource("Widget", nil)),
		},
		{
			name: "message named with its parent type",
			api: methodAPI(rpc("GetGadgetGadgetWidget", "GadgetWidget", "GadgetWidget"),
				resource("GadgetWidget", []string{"Gadget"}),
				resource("Gadget", nil)),
			want: []lint.Finding{
				{Subject: "test.GadgetWidget", Message: "message is named with its parent type Gadget - name it for the subresource alone"},
			},
		},
		{
			name: "message named with a parent type of Global is fine",
			api: methodAPI(rpc("GetGlobalGlobalWidget", "GlobalWidget", "GlobalWidget"),
				resource("GlobalWidget", []string{"Global"})),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.SubresourceNaming.Check(tt.api)
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func enumAPI(enums []model.Enum) *model.API {
	return &model.API{Enums: enums}
}

func TestEnumZeroValueUnspecified(t *testing.T) {
	tests := []struct {
		name string
		enum model.Enum
		want []lint.Finding
	}{
		{
			name: "compliant top-level",
			enum: model.Enum{FullName: "test.ActorState", Name: "ActorState", Values: []model.EnumValue{
				{Name: "ACTOR_STATE_UNSPECIFIED", Number: 0},
				{Name: "ACTOR_STATE_RUNNING", Number: 1},
			}},
		},
		{
			name: "compliant nested",
			enum: model.Enum{FullName: "test.Worker.State", Name: "Worker.State", ParentFullName: "test.Worker", Values: []model.EnumValue{
				{Name: "STATE_UNSPECIFIED", Number: 0},
				{Name: "STATE_ACTIVE", Number: 1},
			}},
		},
		{
			name: "wrong zero-value name",
			enum: model.Enum{FullName: "test.ActorState", Name: "ActorState", Values: []model.EnumValue{
				{Name: "UNKNOWN", Number: 0},
			}},
			want: []lint.Finding{
				{Subject: "test.ActorState", Message: `zero value is named "UNKNOWN", want "ACTOR_STATE_UNSPECIFIED"`},
			},
		},
		{
			name: "no zero value at all",
			enum: model.Enum{FullName: "test.ActorState", Name: "ActorState", Values: []model.EnumValue{
				{Name: "ACTOR_STATE_RUNNING", Number: 1},
			}},
			want: []lint.Finding{
				{Subject: "test.ActorState", Message: "has no value numbered 0"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.EnumZeroValueUnspecified.Check(enumAPI([]model.Enum{tt.enum}))
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestEnumValuesPrefixed(t *testing.T) {
	tests := []struct {
		name string
		enum model.Enum
		want []lint.Finding
	}{
		{
			name: "compliant top-level",
			enum: model.Enum{FullName: "test.SandboxClass", Name: "SandboxClass", Values: []model.EnumValue{
				{Name: "SANDBOX_CLASS_UNSPECIFIED", Number: 0},
				{Name: "SANDBOX_CLASS_GVISOR", Number: 1},
			}},
		},
		{
			name: "unprefixed top-level value",
			enum: model.Enum{FullName: "test.SandboxClass", Name: "SandboxClass", Values: []model.EnumValue{
				{Name: "UNSPECIFIED", Number: 0},
				{Name: "GVISOR", Number: 1},
			}},
			want: []lint.Finding{
				{Subject: "test.SandboxClass.UNSPECIFIED", Message: `not prefixed with "SANDBOX_CLASS_"`},
				{Subject: "test.SandboxClass.GVISOR", Message: `not prefixed with "SANDBOX_CLASS_"`},
			},
		},
		{
			name: "compliant nested",
			enum: model.Enum{FullName: "test.Worker.State", Name: "Worker.State", ParentFullName: "test.Worker", Values: []model.EnumValue{
				{Name: "STATE_UNSPECIFIED", Number: 0},
				{Name: "STATE_ACTIVE", Number: 1},
			}},
		},
		{
			name: "unprefixed nested value",
			enum: model.Enum{FullName: "test.Worker.State", Name: "Worker.State", ParentFullName: "test.Worker", Values: []model.EnumValue{
				{Name: "UNSPECIFIED", Number: 0},
				{Name: "ACTIVE", Number: 1},
			}},
			want: []lint.Finding{
				{Subject: "test.Worker.State.UNSPECIFIED", Message: `not prefixed with "STATE_"`},
				{Subject: "test.Worker.State.ACTIVE", Message: `not prefixed with "STATE_"`},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.EnumValuesPrefixed.Check(enumAPI([]model.Enum{tt.enum}))
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
