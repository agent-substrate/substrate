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
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
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
				{Name: "labels", Number: 1, TypeDisplay: "map<string, string>"},
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
				{Name: "gadgets", Number: 1, Repeated: true, TypeDisplay: "repeated Gadget", TypeFullName: "fixture.Gadget", TypeKind: "message"},
			}},
			{FullName: "fixture.Gadget", Name: "Gadget", Fields: []model.Field{
				{Name: "id", Number: 1, TypeDisplay: "string"},
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
				{Name: "gadgets_by_id", Number: 1, TypeDisplay: "map<string, Gadget>", MapValueFullName: "fixture.Gadget", MapValueKind: "message"},
			}},
			{FullName: "fixture.Gadget", Name: "Gadget", Fields: []model.Field{
				{Name: "id", Number: 1, TypeDisplay: "string"},
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
				{Name: "snapshot", Number: 1, TypeDisplay: "string", OneofName: "reference"},
				{Name: "tag", Number: 2, TypeDisplay: "string", OneofName: "reference"},
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
				{Name: "status", Number: 1, TypeDisplay: "Widget.Status", TypeFullName: "fixture.Widget.Status", TypeKind: "enum"},
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
					{Name: "name", Number: 1, TypeDisplay: "string", Comment: "name's doc comment."},
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
				{Name: "nickname", Number: 1, TypeDisplay: "string", Proto3Optional: true},
			}},
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("API mismatch (-want +got):\n%s", diff)
	}
}

func TestBuild_Annotations(t *testing.T) {
	api, err := model.Build(t.Context(), ateapipb.Source)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	gotResources := map[string]*model.ResourceAnnotation{}
	for _, m := range api.Messages {
		if m.Resource != nil {
			gotResources[m.Name] = m.Resource
		}
	}
	wantResources := map[string]*model.ResourceAnnotation{
		"Actor":           {},
		"EgressPolicy":    {Parents: []string{"Actor"}, Singleton: true},
		"Tag":             {},
		"Atespace":        {},
		"ActorTemplate":   {},
		"Worker":          {},
		"ActorAssignment": {Parents: []string{"Worker"}},
		"AccessPolicy":    {Parents: []string{"Global", "Atespace"}, Singleton: true},
	}
	if diff := cmp.Diff(wantResources, gotResources); diff != "" {
		t.Errorf("resource annotations mismatch (-want +got):\n%s", diff)
	}

	wantMethods := map[string]string{
		"GetActor":                   "Actor",
		"SuspendActor":               "Actor",
		"GetActorEgressPolicy":       "EgressPolicy",
		"GetGlobalAccessPolicy":      "AccessPolicy",
		"GetAtespaceAccessPolicy":    "AccessPolicy",
		"ListWorkerActorAssignments": "ActorAssignment",
	}
	gotMethods := map[string]string{}
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
			{FullName: "test.Worker", Name: "Worker", Resource: &model.ResourceAnnotation{}},
			{FullName: "test.Actor", Name: "Actor", Resource: &model.ResourceAnnotation{}},
			{FullName: "test.EgressPolicy", Name: "EgressPolicy", Resource: &model.ResourceAnnotation{Parents: []string{"Global", "Actor"}}},
		},
	}

	groups, err := model.Resources(api)
	if err != nil {
		t.Fatalf("Resources() error = %v", err)
	}

	got := map[string][]string{}
	var gotOrder []string
	for _, g := range groups {
		gotOrder = append(gotOrder, g.Message.Name)
		got[g.Message.Name] = []string{}
		for _, m := range g.Methods {
			got[g.Message.Name] = append(got[g.Message.Name], m.Name)
		}
	}
	if diff := cmp.Diff([]string{"Worker", "Actor", "EgressPolicy"}, gotOrder); diff != "" {
		t.Errorf("resource order mismatch (-want +got):\n%s", diff)
	}
	want := map[string][]string{
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
		name string
		api  *model.API
	}{
		{
			name: "method without annotation",
			api: &model.API{
				Services: []model.Service{{Name: "Control", Methods: []model.Method{{Name: "GetActor"}}}},
				Messages: []model.Message{{FullName: "test.Actor", Name: "Actor", Resource: &model.ResourceAnnotation{}}},
			},
		},
		{
			name: "method annotated with a message that isn't a resource",
			api: &model.API{
				Services: []model.Service{{Name: "Control", Methods: []model.Method{{Name: "GetActor", Resource: "Actor"}}}},
				Messages: []model.Message{{FullName: "test.Actor", Name: "Actor"}},
			},
		},
		{
			name: "method annotated with an unknown resource",
			api: &model.API{
				Services: []model.Service{{Name: "Control", Methods: []model.Method{{Name: "GetActor", Resource: "Actor"}}}},
			},
		},
		{
			name: "unknown parent",
			api: &model.API{
				Messages: []model.Message{
					{FullName: "test.EgressPolicy", Name: "EgressPolicy", Resource: &model.ResourceAnnotation{Parents: []string{"Actor"}}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := model.Resources(tt.api); err == nil {
				t.Error("Resources() error = nil, want an error")
			}
		})
	}
}
