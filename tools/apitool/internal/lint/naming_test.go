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
			name: "request declared outside this API",
			api: methodAPI(model.Method{Name: "DoThing", InputName: "google.protobuf.Empty", OutputName: "test.DoThingResponse"},
				msg("DoThingResponse")),
			want: []lint.Finding{
				{Subject: "test.Control.DoThing", Message: `request message is named "Empty", want "DoThingRequest"`},
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

func TestCustomResponseNameMatchesMethod(t *testing.T) {
	tests := []struct {
		name string
		api  *model.API
		want []lint.Finding
	}{
		{
			name: "compliant",
			api: methodAPI(rpc("DrainWidget", "Widget", "DrainWidgetResponse"),
				resource("Widget", nil)),
		},
		{
			name: "returns the resource",
			api: methodAPI(rpc("DrainWidget", "Widget", "Widget"),
				resource("Widget", nil)),
			want: []lint.Finding{
				{Subject: "test.Control.DrainWidget", Message: `response message is named "Widget", want "DrainWidgetResponse"`},
			},
		},
		{
			name: "wrong name",
			api: methodAPI(rpc("DrainWidget", "Widget", "DrainResponse"),
				resource("Widget", nil)),
			want: []lint.Finding{
				{Subject: "test.Control.DrainWidget", Message: `response message is named "DrainResponse", want "DrainWidgetResponse"`},
			},
		},
		{
			name: "a standard method is not checked",
			api: methodAPI(rpc("GetWidget", "Widget", "Widget"),
				resource("Widget", nil)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.CustomResponseNameMatchesMethod.Check(tt.api)
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestStandardMethodNaming(t *testing.T) {
	tests := []struct {
		name    string
		api     *model.API
		wantErr string
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
			wantErr: "test.Control.GetWidget: a method named with Get is a standard method - want Get, then one of the parents Gadget, then Widget, or name it with a custom verb",
		},
		{
			name: "Get naming a resource that isn't a parent",
			api: methodAPI(rpc("GetGadgetBoxWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("GadgetBox", nil),
				resource("Gadget", nil)),
			wantErr: "test.Control.GetGadgetBoxWidget: a method named with Get is a standard method - want Get, then one of the parents Gadget, then Widget, or name it with a custom verb",
		},
		{
			name: "Get naming neither of two parents",
			api: methodAPI(rpc("GetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Global", "Gadget"}),
				resource("Gadget", nil)),
			wantErr: "test.Control.GetWidget: a method named with Get is a standard method - want Get, then one of the parents Global/Gadget, then Widget, or name it with a custom verb",
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
			wantErr: "test.Control.ListGadgetWidget: a method named with List is a standard method - want List, then one of the parents Gadget, then Widgets, or name it with a custom verb",
		},
		{
			name: "List naming no parent",
			api: methodAPI(rpc("ListWidgets", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil)),
			wantErr: "test.Control.ListWidgets: a method named with List is a standard method - want List, then one of the parents Gadget, then Widgets, or name it with a custom verb",
		},
		{
			name: "List with the declared plural",
			api: methodAPI(rpc("ListGadgetPeople", "Person", "Person"),
				pluralResource("Person", "People", []string{"Gadget"}),
				resource("Gadget", nil)),
		},
		{
			name: "List with a plural other than the declared one",
			api: methodAPI(rpc("ListGadgetPersons", "Person", "Person"),
				pluralResource("Person", "People", []string{"Gadget"}),
				resource("Gadget", nil)),
			wantErr: "test.Control.ListGadgetPersons: a method named with List is a standard method - want List, then one of the parents Gadget, then People, or name it with a custom verb",
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
			name: "top-level Get",
			api: methodAPI(rpc("GetWidget", "Widget", "Widget"),
				resource("Widget", nil)),
		},
		{
			name: "top-level List",
			api: methodAPI(rpc("ListWidgets", "Widget", "Widget"),
				resource("Widget", nil)),
		},
		{
			name: "top-level Get with the plural",
			api: methodAPI(rpc("GetWidgets", "Widget", "Widget"),
				resource("Widget", nil)),
			wantErr: "test.Control.GetWidgets: a method named with Get is a standard method - want Get, then Widget, or name it with a custom verb",
		},
		{
			name: "top-level List with the singular",
			api: methodAPI(rpc("ListWidget", "Widget", "Widget"),
				resource("Widget", nil)),
			wantErr: "test.Control.ListWidget: a method named with List is a standard method - want List, then Widgets, or name it with a custom verb",
		},
		{
			name: "top-level Get naming a parent it doesn't have",
			api: methodAPI(rpc("GetGadgetWidget", "Widget", "Widget"),
				resource("Widget", nil),
				resource("Gadget", nil)),
			wantErr: "test.Control.GetGadgetWidget: a method named with Get is a standard method - want Get, then Widget, or name it with a custom verb",
		},
		{
			name: "message named with its parent type",
			api: methodAPI(rpc("GetGadgetGadgetWidget", "GadgetWidget", "GadgetWidget"),
				resource("GadgetWidget", []string{"Gadget"}),
				resource("Gadget", nil)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Every rule over standard methods parses them the same way;
			// StandardMethodReturnsResource stands in for all of them.
			_, err := lint.StandardMethodReturnsResource.Check(tt.api)
			var gotErr string
			if err != nil {
				gotErr = err.Error()
			}
			if gotErr != tt.wantErr {
				t.Errorf("Check() error = %q, want %q", gotErr, tt.wantErr)
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
			name: "message named with its parent type",
			api: methodAPI(rpc("GetGadgetGadgetWidget", "GadgetWidget", "GadgetWidget"),
				resource("GadgetWidget", []string{"Gadget"}),
				resource("Gadget", nil)),
			want: []lint.Finding{
				{Subject: "test.GadgetWidget", Message: "message is named with its parent type Gadget - name it for the subresource alone"},
			},
		},
		{
			name: "message named with Global",
			api: methodAPI(rpc("GetGlobalGlobalWidget", "GlobalWidget", "GlobalWidget"),
				resource("GlobalWidget", []string{"Global", "Gadget"}),
				resource("Gadget", nil)),
			want: []lint.Finding{
				{Subject: "test.GlobalWidget", Message: "message is named with its parent type Global - name it for the subresource alone"},
			},
		},
		{
			name: "top-level resource named after another resource",
			api: methodAPI(rpc("GetGadgetWidget", "GadgetWidget", "GadgetWidget"),
				resource("GadgetWidget", nil),
				resource("Gadget", nil)),
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
