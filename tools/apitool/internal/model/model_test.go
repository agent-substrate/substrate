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

// The resource names below ("Actor", "Worker") aren't arbitrary -
// model.Resources itself hardcodes that vocabulary (see resourceNames in
// model.go), so this fixture has to use it too.
func TestResources(t *testing.T) {
	api := buildAPI(t, `
service FixtureService {
  rpc GetActor(GetActorRequest) returns (Actor);
  rpc CreateActor(CreateActorRequest) returns (Actor);
  rpc GetWorker(GetWorkerRequest) returns (Worker);
}

message GetActorRequest { string name = 1; }
message CreateActorRequest { string name = 1; }
message GetWorkerRequest { string name = 1; }

message Actor { string name = 1; }
message Worker { string name = 1; }
`)

	groups, err := model.Resources(api)
	if err != nil {
		t.Fatalf("Resources() error = %v", err)
	}

	var actorGroup *model.Resource
	for i := range groups {
		if groups[i].Message.Name == "Actor" {
			actorGroup = &groups[i]
		}
	}
	if actorGroup == nil {
		t.Fatalf("no Resource for Actor; groups = %+v", groups)
	}

	var gotNames []string
	for _, m := range actorGroup.Methods {
		gotNames = append(gotNames, m.Name)
	}
	wantNames := []string{"GetActor", "CreateActor"}
	if len(gotNames) != len(wantNames) {
		t.Fatalf("Actor group methods = %v, want %v", gotNames, wantNames)
	}
	for i, want := range wantNames {
		if gotNames[i] != want {
			t.Errorf("Actor group methods[%d] = %q, want %q (declaration order matters)", i, gotNames[i], want)
		}
	}
}

func TestResources_InvalidMethodName(t *testing.T) {
	tests := []struct {
		name       string
		methodName string
	}{
		{"no resource name matches", "DoThing"},
		{"two equally-specific resource names match", "ActorSnapshotAndActorTemplate"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := &model.API{
				Services: []model.Service{{
					Name: "Bogus",
					Methods: []model.Method{
						{Name: tt.methodName, ServiceName: "Bogus"},
					},
				}},
			}
			if _, err := model.Resources(api); err == nil {
				t.Errorf("Resources() error = nil for method %q, want an error", tt.methodName)
			}
		})
	}
}
