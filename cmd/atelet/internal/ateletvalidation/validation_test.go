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

package ateletvalidation

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func validRequestActorSuspendRequest(mutate ...func(*ateletpb.RequestActorSuspendRequest)) *ateletpb.RequestActorSuspendRequest {
	r := &ateletpb.RequestActorSuspendRequest{
		ActorAtespace: "team-a",
		ActorName:     "actor-1",
		ActorUid:      "01234567-89ab-cdef-0123-456789abcdef",
	}
	for _, m := range mutate {
		m(r)
	}
	return r
}

func TestValidateRequestActorSuspendRequest(t *testing.T) {
	valid := validRequestActorSuspendRequest

	tests := []struct {
		name string
		obj  *ateletpb.RequestActorSuspendRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "missing actor_atespace",
		obj:  valid(func(r *ateletpb.RequestActorSuspendRequest) { r.ActorAtespace = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_atespace"), "")},
	}, {
		name: "invalid actor_atespace: uppercase",
		obj:  valid(func(r *ateletpb.RequestActorSuspendRequest) { r.ActorAtespace = "Team-A" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_name",
		obj:  valid(func(r *ateletpb.RequestActorSuspendRequest) { r.ActorName = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_name"), "")},
	}, {
		name: "invalid actor_name: trailing dash",
		obj:  valid(func(r *ateletpb.RequestActorSuspendRequest) { r.ActorName = "actor-" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_uid",
		obj:  valid(func(r *ateletpb.RequestActorSuspendRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid actor_uid: not a uuid",
		obj:  valid(func(r *ateletpb.RequestActorSuspendRequest) { r.ActorUid = "not-a-uuid" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_RequestActorSuspendRequest(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

// TestValidateRequestActorSuspendRequestEdge covers the handler-facing
// wrapper: valid passes, invalid comes back as InvalidArgument.
func TestValidateRequestActorSuspendRequestEdge(t *testing.T) {
	if err := ValidateRequestActorSuspendRequest(context.Background(), validRequestActorSuspendRequest()); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	err := ValidateRequestActorSuspendRequest(context.Background(), &ateletpb.RequestActorSuspendRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty request error = %v, want InvalidArgument", err)
	}
}

func validTerminateRequest(mutate ...func(*ateletpb.TerminateRequest)) *ateletpb.TerminateRequest {
	r := &ateletpb.TerminateRequest{
		TargetAteomUid:        "0f9a3b1c-2d4e-5f60-7182-93a4b5c6d7e8",
		Atespace:              "team-a",
		ActorName:             "actor-1",
		ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
		ActorTemplateAtespace: "team-a",
		ActorTemplateName:     "tmpl-1",
		Spec:                  &ateletpb.WorkloadSpec{},
	}
	for _, m := range mutate {
		m(r)
	}
	return r
}

// TestValidateTerminateRequest exercises the identity header shared by every
// AteomHerder request; the other herder requests reuse the same generated
// checks for those fields.
func TestValidateTerminateRequest(t *testing.T) {
	valid := validTerminateRequest

	tests := []struct {
		name string
		obj  *ateletpb.TerminateRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "missing target_ateom_uid",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.TargetAteomUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("target_ateom_uid"), "")},
	}, {
		name: "missing atespace",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.Atespace = "" }),
		want: field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		name: "missing actor_name",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.ActorName = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_name"), "")},
	}, {
		name: "missing actor_uid",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "unset template identity is allowed",
		obj: valid(func(r *ateletpb.TerminateRequest) {
			r.ActorTemplateAtespace = ""
			r.ActorTemplateName = ""
		}),
	}, {
		name: "unset spec is allowed",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.Spec = nil }),
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_TerminateRequest(context.Background(), op, nil, tt.obj, nil))
		})
	}
}

func TestValidateSetWorkerCapacityRequest(t *testing.T) {
	tests := []struct {
		name string
		obj  *ateletpb.SetWorkerCapacityRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj: &ateletpb.SetWorkerCapacityRequest{
			Capacity: &ateapipb.WorkerResources{},
		},
	}, {
		name: "missing capacity",
		obj:  &ateletpb.SetWorkerCapacityRequest{},
		want: field.ErrorList{field.Required(field.NewPath("capacity"), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_SetWorkerCapacityRequest(context.Background(), op, nil, tt.obj, nil))
		})
	}
}
