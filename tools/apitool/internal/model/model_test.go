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

package model_test

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/reflect/protoreflect"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/tools/apitool/internal/model"
)

func buildAPI(t *testing.T, protoBody string) *model.API {
	t.Helper()
	protoText := "syntax = \"proto3\";\npackage fixture;\n\n" + protoBody
	api, err := model.Build(t.Context(), protoText)
	if err != nil {
		t.Fatalf("compiling fixture: %v", err)
	}
	return api
}

func TestBuild_MapField(t *testing.T) {
	got := buildAPI(t, `
message Widget {
  map<string, string> labels = 1;
}
`)

	want := &model.API{
		Messages: []model.Message{
			{FullName: "fixture.Widget", Name: "Widget", Fields: []model.Field{
				{Name: "labels", Number: 1, Kind: protoreflect.MessageKind, TypeDisplay: "map<string, string>", MapValueKind: protoreflect.StringKind},
			}},
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("API mismatch (-want +got):\n%s", diff)
	}
}

func TestBuild_RepeatedMessageField(t *testing.T) {
	got := buildAPI(t, `
message Widget {
  repeated Gadget gadgets = 1;
}

message Gadget {
  string id = 1;
}
`)

	want := &model.API{
		Messages: []model.Message{
			{FullName: "fixture.Widget", Name: "Widget", Fields: []model.Field{
				{Name: "gadgets", Number: 1, Repeated: true, TypeDisplay: "repeated Gadget", TypeFullName: "fixture.Gadget", Kind: protoreflect.MessageKind},
			}},
			{FullName: "fixture.Gadget", Name: "Gadget", Fields: []model.Field{
				{Name: "id", Number: 1, Kind: protoreflect.StringKind, TypeDisplay: "string"},
			}},
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("API mismatch (-want +got):\n%s", diff)
	}
}

func TestBuild_MapValueMessageField(t *testing.T) {
	got := buildAPI(t, `
message Widget {
  map<string, Gadget> gadgets_by_id = 1;
}

message Gadget {
  string id = 1;
}
`)

	want := &model.API{
		Messages: []model.Message{
			{FullName: "fixture.Widget", Name: "Widget", Fields: []model.Field{
				{Name: "gadgets_by_id", Number: 1, Kind: protoreflect.MessageKind, TypeDisplay: "map<string, Gadget>", MapValueFullName: "fixture.Gadget", MapValueKind: protoreflect.MessageKind},
			}},
			{FullName: "fixture.Gadget", Name: "Gadget", Fields: []model.Field{
				{Name: "id", Number: 1, Kind: protoreflect.StringKind, TypeDisplay: "string"},
			}},
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("API mismatch (-want +got):\n%s", diff)
	}
}

func TestBuild_Oneof(t *testing.T) {
	got := buildAPI(t, `
message Ref {
  oneof reference {
    string snapshot = 1;
    string tag = 2;
  }
}
`)

	want := &model.API{
		Messages: []model.Message{
			{FullName: "fixture.Ref", Name: "Ref", Fields: []model.Field{
				{Name: "snapshot", Number: 1, Kind: protoreflect.StringKind, TypeDisplay: "string", OneofName: "reference"},
				{Name: "tag", Number: 2, Kind: protoreflect.StringKind, TypeDisplay: "string", OneofName: "reference"},
			}},
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("API mismatch (-want +got):\n%s", diff)
	}
}

func TestBuild_NestedEnums(t *testing.T) {
	got := buildAPI(t, `
message Widget {
  Status status = 1;

  enum Status {
    STATUS_UNSPECIFIED = 0;
    STATUS_ACTIVE = 1;
  }
}
`)

	want := &model.API{
		Messages: []model.Message{
			{FullName: "fixture.Widget", Name: "Widget", Fields: []model.Field{
				{Name: "status", Number: 1, TypeDisplay: "Widget.Status", TypeFullName: "fixture.Widget.Status", Kind: protoreflect.EnumKind},
			}},
		},
		Enums: []model.Enum{
			{
				FullName: "fixture.Widget.Status", Name: "Widget.Status", ParentFullName: "fixture.Widget",
				Values: []model.EnumValue{
					{Name: "STATUS_UNSPECIFIED", Number: 0},
					{Name: "STATUS_ACTIVE", Number: 1},
				},
			},
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("API mismatch (-want +got):\n%s", diff)
	}
}

func TestBuild_Comments(t *testing.T) {
	got := buildAPI(t, `
// Widget's doc comment.
message Widget {
  // name's doc comment.
  string name = 1;
}
`)

	want := &model.API{
		Messages: []model.Message{
			{
				FullName: "fixture.Widget", Name: "Widget", Comment: "Widget's doc comment.",
				Fields: []model.Field{
					{Name: "name", Number: 1, Kind: protoreflect.StringKind, TypeDisplay: "string", Comment: "name's doc comment."},
				},
			},
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("API mismatch (-want +got):\n%s", diff)
	}
}

func TestBuild_Proto3Optional(t *testing.T) {
	got := buildAPI(t, `
message Widget {
  optional string nickname = 1;
}
`)

	want := &model.API{
		Messages: []model.Message{
			{FullName: "fixture.Widget", Name: "Widget", Fields: []model.Field{
				{Name: "nickname", Number: 1, Kind: protoreflect.StringKind, TypeDisplay: "string", Proto3Optional: true},
			}},
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("API mismatch (-want +got):\n%s", diff)
	}
}

func TestBuild_Annotations(t *testing.T) {
	api, err := model.Build(t.Context(), ateapi.Source)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	gotResources := map[string]*model.ResourceAnnotation{}
	for _, m := range api.Messages {
		if m.IsResource() {
			gotResources[m.Name] = m.ResourceAnnotation
		}
	}
	wantResources := map[string]*model.ResourceAnnotation{
		"Actor":           {Scope: ateapi.Scope_SCOPE_ATESPACE, Plural: "Actors"},
		"EgressPolicy":    {Parents: []string{"Actor"}, Singleton: true, Plural: "EgressPolicies"},
		"Tag":             {Scope: ateapi.Scope_SCOPE_ATESPACE, Plural: "Tags"},
		"Atespace":        {Scope: ateapi.Scope_SCOPE_GLOBAL, Plural: "Atespaces"},
		"ActorTemplate":   {Scope: ateapi.Scope_SCOPE_ATESPACE, Plural: "ActorTemplates"},
		"Worker":          {Scope: ateapi.Scope_SCOPE_GLOBAL, Plural: "Workers"},
		"ActorAssignment": {Parents: []string{"Worker"}, Plural: "ActorAssignments"},
		"AccessPolicy":    {Parents: []string{"Global", "Atespace"}, Singleton: true, Plural: "AccessPolicies"},
	}
	if diff := cmp.Diff(wantResources, gotResources); diff != "" {
		t.Errorf("resource annotations mismatch (-want +got):\n%s", diff)
	}

	wantMethods := map[protoreflect.Name]string{
		"GetActor":                   "Actor",
		"SuspendActor":               "Actor",
		"GetActorEgressPolicy":       "EgressPolicy",
		"GetGlobalAccessPolicy":      "AccessPolicy",
		"GetAtespaceAccessPolicy":    "AccessPolicy",
		"ListWorkerActorAssignments": "ActorAssignment",
	}
	gotMethods := map[protoreflect.Name]string{}
	for _, svc := range api.Services {
		for _, m := range svc.Methods {
			if _, ok := wantMethods[m.Name]; ok {
				gotMethods[m.Name] = m.Resource
			}
		}
	}
	if diff := cmp.Diff(wantMethods, gotMethods); diff != "" {
		t.Errorf("method annotations mismatch (-want +got):\n%s", diff)
	}
}

func TestResources(t *testing.T) {
	api := &model.API{
		Services: []model.Service{{
			Name: "Control",
			Methods: []model.Method{
				{Name: "GetActor", ServiceName: "Control", Resource: "Actor"},
				{Name: "GetWorker", ServiceName: "Control", Resource: "Worker"},
				{Name: "SuspendActor", ServiceName: "Control", Resource: "Actor"},
			},
		}},
		Messages: []model.Message{
			{FullName: "test.GetActorRequest", Name: "GetActorRequest"},
			{FullName: "test.Worker", Name: "Worker", ResourceAnnotation: &model.ResourceAnnotation{Scope: ateapi.Scope_SCOPE_GLOBAL, Plural: "Workers"}},
			{FullName: "test.Actor", Name: "Actor", ResourceAnnotation: &model.ResourceAnnotation{Scope: ateapi.Scope_SCOPE_ATESPACE, Plural: "Actors"}},
			{FullName: "test.EgressPolicy", Name: "EgressPolicy", ResourceAnnotation: &model.ResourceAnnotation{Parents: []string{"Global", "Actor"}, Plural: "EgressPolicies"}},
		},
	}

	groups, err := model.Resources(api)
	if err != nil {
		t.Fatalf("Resources() error = %v", err)
	}

	got := map[string][]protoreflect.Name{}
	var gotOrder []string
	for _, g := range groups {
		gotOrder = append(gotOrder, g.Message.Name)
		got[g.Message.Name] = []protoreflect.Name{}
		for _, m := range g.Methods {
			got[g.Message.Name] = append(got[g.Message.Name], m.Name)
		}
	}
	if diff := cmp.Diff([]string{"Worker", "Actor", "EgressPolicy"}, gotOrder); diff != "" {
		t.Errorf("resource order mismatch (-want +got):\n%s", diff)
	}
	want := map[string][]protoreflect.Name{
		"Worker":       {"GetWorker"},
		"Actor":        {"GetActor", "SuspendActor"},
		"EgressPolicy": {},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("methods by resource mismatch (-want +got):\n%s", diff)
	}
}

func TestResources_Invalid(t *testing.T) {
	tests := []struct {
		name    string
		api     *model.API
		wantErr string
	}{
		{
			name: "method without annotation",
			api: &model.API{
				Services: []model.Service{{Name: "Control", Methods: []model.Method{{Name: "GetActor"}}}},
				Messages: []model.Message{{FullName: "test.Actor", Name: "Actor", ResourceAnnotation: &model.ResourceAnnotation{Scope: ateapi.Scope_SCOPE_ATESPACE, Plural: "Actors"}}},
			},
			wantErr: "Control.GetActor: no resource annotation",
		},
		{
			name: "method annotated with a message that isn't a resource",
			api: &model.API{
				Services: []model.Service{{Name: "Control", Methods: []model.Method{{Name: "GetActor", Resource: "Actor"}}}},
				Messages: []model.Message{{FullName: "test.Actor", Name: "Actor"}},
			},
			wantErr: `annotated resource "Actor" is not a resource in this API`,
		},
		{
			name: "method annotated with an unknown resource",
			api: &model.API{
				Services: []model.Service{{Name: "Control", Methods: []model.Method{{Name: "GetActor", Resource: "Actor"}}}},
			},
			wantErr: `annotated resource "Actor" is not a resource in this API`,
		},
		{
			name: "unknown parent",
			api: &model.API{
				Messages: []model.Message{
					{FullName: "test.EgressPolicy", Name: "EgressPolicy", ResourceAnnotation: &model.ResourceAnnotation{Parents: []string{"Actor"}, Plural: "EgressPolicies"}},
				},
			},
			wantErr: `test.EgressPolicy: parent "Actor" is neither "Global" nor a resource in this API`,
		},
		{
			name: "resource without plural",
			api: &model.API{
				Messages: []model.Message{{FullName: "test.Actor", Name: "Actor", ResourceAnnotation: &model.ResourceAnnotation{Scope: ateapi.Scope_SCOPE_ATESPACE}}},
			},
			wantErr: "test.Actor: resource has no plural",
		},
		{
			name: "top-level resource without scope",
			api: &model.API{
				Messages: []model.Message{{FullName: "test.Actor", Name: "Actor", ResourceAnnotation: &model.ResourceAnnotation{Plural: "Actors"}}},
			},
			wantErr: "test.Actor: top-level resource has no scope",
		},
		{
			name: "subresource with scope",
			api: &model.API{
				Messages: []model.Message{
					{FullName: "test.Actor", Name: "Actor", ResourceAnnotation: &model.ResourceAnnotation{Scope: ateapi.Scope_SCOPE_ATESPACE, Plural: "Actors"}},
					{FullName: "test.EgressPolicy", Name: "EgressPolicy", ResourceAnnotation: &model.ResourceAnnotation{Parents: []string{"Actor"}, Scope: ateapi.Scope_SCOPE_ATESPACE, Plural: "EgressPolicies"}},
				},
			},
			wantErr: "test.EgressPolicy: subresource has scope SCOPE_ATESPACE",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := model.Resources(tt.api)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Resources() error = %v, want one containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestScopeToService(t *testing.T) {
	control := model.Service{Name: "Control", Methods: []model.Method{
		{Name: "GetActorEgressPolicy", ServiceName: "Control", InputName: "test.GetActorEgressPolicyRequest", OutputName: "test.EgressPolicy", Resource: "EgressPolicy"},
	}}
	actor := model.Message{FullName: "test.Actor", Name: "Actor", ResourceAnnotation: &model.ResourceAnnotation{Scope: ateapi.Scope_SCOPE_ATESPACE, Plural: "Actors"}}
	egressPolicy := model.Message{FullName: "test.EgressPolicy", Name: "EgressPolicy", ResourceAnnotation: &model.ResourceAnnotation{Parents: []string{"Global", "Actor"}, Plural: "EgressPolicies"}}
	request := model.Message{FullName: "test.GetActorEgressPolicyRequest", Name: "GetActorEgressPolicyRequest"}
	unused := model.Message{FullName: "test.Unused", Name: "Unused"}
	api := &model.API{
		Services: []model.Service{control, {Name: "Other"}},
		Messages: []model.Message{actor, unused, egressPolicy, request},
	}

	got := model.ScopeToService(api, "Control")

	want := &model.API{
		Services: []model.Service{control},
		Messages: []model.Message{actor, egressPolicy, request},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("ScopeToService() mismatch (-want +got):\n%s", diff)
	}
}
