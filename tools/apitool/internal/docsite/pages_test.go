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

package docsite

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/reflect/protoreflect"

	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"go.ate.dev/substrate/tools/apitool/internal/model"
)

// controlAPI builds an API whose test.Control service has methods, plus
// messages.
func controlAPI(methods []model.Method, messages ...model.Message) *model.API {
	for i := range methods {
		methods[i].ServiceFullName = "test.Control"
		methods[i].ServiceName = "Control"
	}
	return &model.API{
		Services: []model.Service{{FullName: "test.Control", Name: "Control", Methods: methods}},
		Messages: messages,
	}
}

// rpc returns a method named name, annotated with resource, taking
// test.{name}Request and returning test.{output}.
func rpc(name, resource, output string) model.Method {
	return model.Method{Name: protoreflect.Name(name), Resource: resource, InputName: protoreflect.FullName("test." + name + "Request"), OutputName: protoreflect.FullName("test." + output)}
}

// resource returns a resource message test.{name} annotated with a.
func resource(name string, a model.ResourceAnnotation) model.Message {
	return model.Message{FullName: protoreflect.FullName("test." + name), Name: name, ResourceAnnotation: &a}
}

// method returns rpc(name, resource, output) as it appears in api, owned by
// test.Control.
func method(name, resource, output string) model.Method {
	m := rpc(name, resource, output)
	m.ServiceFullName = "test.Control"
	m.ServiceName = "Control"
	return m
}

func TestResourcePages(t *testing.T) {
	tests := []struct {
		name string
		api  *model.API
		want []resourcePage
	}{
		{
			name: "top-level resource",
			api: controlAPI(
				[]model.Method{rpc("GetActor", "Actor", "Actor"), rpc("SuspendActor", "Actor", "SuspendActorResponse")},
				resource("Actor", model.ResourceAnnotation{Scope: ateapipb.Scope_SCOPE_ATESPACE, Plural: "Actors"}),
			),
			want: []resourcePage{{
				Message: resource("Actor", model.ResourceAnnotation{Scope: ateapipb.Scope_SCOPE_ATESPACE, Plural: "Actors"}),
				Scope:   "atespace",
				Methods: []model.Method{method("GetActor", "Actor", "Actor"), method("SuspendActor", "Actor", "SuspendActorResponse")},
			}},
		},
		{
			name: "subresource with one parent keeps its custom methods",
			api: controlAPI(
				[]model.Method{rpc("ListWorkerActorAssignments", "ActorAssignment", "ListWorkerActorAssignmentsResponse"), rpc("EvictActorAssignment", "ActorAssignment", "ActorAssignment")},
				resource("Worker", model.ResourceAnnotation{Scope: ateapipb.Scope_SCOPE_GLOBAL, Plural: "Workers"}),
				resource("ActorAssignment", model.ResourceAnnotation{Parents: []string{"Worker"}, Plural: "ActorAssignments"}),
			),
			want: []resourcePage{
				{Message: resource("Worker", model.ResourceAnnotation{Scope: ateapipb.Scope_SCOPE_GLOBAL, Plural: "Workers"}), Scope: "global"},
				{
					Message: resource("ActorAssignment", model.ResourceAnnotation{Parents: []string{"Worker"}, Plural: "ActorAssignments"}),
					Parent:  "Worker",
					Methods: []model.Method{method("ListWorkerActorAssignments", "ActorAssignment", "ListWorkerActorAssignmentsResponse"), method("EvictActorAssignment", "ActorAssignment", "ActorAssignment")},
				},
			},
		},
		{
			name: "subresource with several parents gets a page per parent",
			api: controlAPI(
				[]model.Method{
					rpc("GetGlobalAccessPolicy", "AccessPolicy", "AccessPolicy"),
					rpc("GetAtespaceAccessPolicy", "AccessPolicy", "AccessPolicy"),
					rpc("UpdateGlobalAccessPolicy", "AccessPolicy", "AccessPolicy"),
				},
				resource("Atespace", model.ResourceAnnotation{Scope: ateapipb.Scope_SCOPE_GLOBAL, Plural: "Atespaces"}),
				resource("AccessPolicy", model.ResourceAnnotation{Parents: []string{"Global", "Atespace"}, Singleton: true, Plural: "AccessPolicies"}),
			),
			want: []resourcePage{
				{Message: resource("Atespace", model.ResourceAnnotation{Scope: ateapipb.Scope_SCOPE_GLOBAL, Plural: "Atespaces"}), Scope: "global"},
				{
					Message:   resource("AccessPolicy", model.ResourceAnnotation{Parents: []string{"Global", "Atespace"}, Singleton: true, Plural: "AccessPolicies"}),
					Parent:    "Global",
					Singleton: true,
					Methods:   []model.Method{method("GetGlobalAccessPolicy", "AccessPolicy", "AccessPolicy"), method("UpdateGlobalAccessPolicy", "AccessPolicy", "AccessPolicy")},
				},
				{
					Message:   resource("AccessPolicy", model.ResourceAnnotation{Parents: []string{"Global", "Atespace"}, Singleton: true, Plural: "AccessPolicies"}),
					Parent:    "Atespace",
					Singleton: true,
					Methods:   []model.Method{method("GetAtespaceAccessPolicy", "AccessPolicy", "AccessPolicy")},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resourcePages(tc.api)
			if err != nil {
				t.Fatalf("resourcePages() error = %v", err)
			}
			if diff := cmp.Diff(tc.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("resourcePages() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResourcePages_Invalid(t *testing.T) {
	tests := []struct {
		name string
		api  *model.API
	}{
		{
			name: "method without a resource annotation",
			api: controlAPI(
				[]model.Method{rpc("GetActor", "", "Actor")},
				resource("Actor", model.ResourceAnnotation{Scope: ateapipb.Scope_SCOPE_ATESPACE, Plural: "Actors"}),
			),
		},
		{
			name: "custom method on a resource with several parents",
			api: controlAPI(
				[]model.Method{rpc("ResetAccessPolicy", "AccessPolicy", "AccessPolicy")},
				resource("Atespace", model.ResourceAnnotation{Scope: ateapipb.Scope_SCOPE_GLOBAL, Plural: "Atespaces"}),
				resource("AccessPolicy", model.ResourceAnnotation{Parents: []string{"Global", "Atespace"}, Singleton: true, Plural: "AccessPolicies"}),
			),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := resourcePages(tc.api); err == nil {
				t.Error("resourcePages() error = nil, want an error")
			}
		})
	}
}
