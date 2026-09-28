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

package apivalidation

import (
	"context"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

var createOp = operation.Operation{Type: operation.Create}

func expectErrors(t *testing.T, want, got field.ErrorList) {
	t.Helper()
	field.ErrorMatcher{}.ByType().ByField().ByOrigin().Test(t, want, got)
}

// expectEdge checks the handler-facing wrapper: a valid request passes and an
// invalid one comes back as InvalidArgument.
func expectEdge(t *testing.T, err error, wantInvalid bool) {
	t.Helper()
	if !wantInvalid {
		if err != nil {
			t.Errorf("valid request rejected: %v", err)
		}
		return
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("error = %v, want InvalidArgument", err)
	}
}

func TestValidateRequestActorSuspendRequest(t *testing.T) {
	valid := func(mutate ...func(*ateletpb.RequestActorSuspendRequest)) *ateletpb.RequestActorSuspendRequest {
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
			expectErrors(t, tt.want, Validate_RequestActorSuspendRequest(context.Background(), createOp, nil, tt.obj, nil))
			expectEdge(t, ValidateRequestActorSuspendRequest(context.Background(), tt.obj), len(tt.want) > 0)
		})
	}
}

func TestValidateMintActorCertificateRequest(t *testing.T) {
	valid := func(mutate ...func(*ateletpb.MintActorCertificateRequest)) *ateletpb.MintActorCertificateRequest {
		r := &ateletpb.MintActorCertificateRequest{
			ActorAtespace:             "team-a",
			ActorName:                 "actor-1",
			ActorUid:                  "01234567-89ab-cdef-0123-456789abcdef",
			CertificateSigningRequest: []byte("der-bytes"),
		}
		for _, m := range mutate {
			m(r)
		}
		return r
	}

	tests := []struct {
		name string
		obj  *ateletpb.MintActorCertificateRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "missing actor_atespace",
		obj:  valid(func(r *ateletpb.MintActorCertificateRequest) { r.ActorAtespace = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_atespace"), "")},
	}, {
		name: "invalid actor_atespace: uppercase",
		obj:  valid(func(r *ateletpb.MintActorCertificateRequest) { r.ActorAtespace = "Team-A" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_name",
		obj:  valid(func(r *ateletpb.MintActorCertificateRequest) { r.ActorName = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_name"), "")},
	}, {
		name: "invalid actor_name: trailing dash",
		obj:  valid(func(r *ateletpb.MintActorCertificateRequest) { r.ActorName = "actor-" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_uid",
		obj:  valid(func(r *ateletpb.MintActorCertificateRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid actor_uid: not a uuid",
		obj:  valid(func(r *ateletpb.MintActorCertificateRequest) { r.ActorUid = "not-a-uuid" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "missing certificate_signing_request",
		obj:  valid(func(r *ateletpb.MintActorCertificateRequest) { r.CertificateSigningRequest = nil }),
		want: field.ErrorList{field.Required(field.NewPath("certificate_signing_request"), "")},
	}, {
		name: "certificate_signing_request at the bound",
		obj: valid(func(r *ateletpb.MintActorCertificateRequest) {
			r.CertificateSigningRequest = make([]byte, 16384)
		}),
	}, {
		name: "certificate_signing_request too large",
		obj: valid(func(r *ateletpb.MintActorCertificateRequest) {
			r.CertificateSigningRequest = make([]byte, 16385)
		}),
		want: field.ErrorList{field.TooLong(field.NewPath("certificate_signing_request"), nil, 16384).WithOrigin("maxBytes")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expectErrors(t, tt.want, Validate_MintActorCertificateRequest(context.Background(), createOp, nil, tt.obj, nil))
			expectEdge(t, ValidateMintActorCertificateRequest(context.Background(), tt.obj), len(tt.want) > 0)
		})
	}
}

func TestValidateSetWorkerCapacityRequest(t *testing.T) {
	withLimits := func(limits ...*ateapipb.Limits) *ateletpb.SetWorkerCapacityRequest {
		return &ateletpb.SetWorkerCapacityRequest{
			Capacity: &ateapipb.WorkerResources{Resources: &ateapipb.Resources{Limits: limits}},
		}
	}
	limitsPath := field.NewPath("capacity", "resources", "limits")

	tests := []struct {
		name string
		obj  *ateletpb.SetWorkerCapacityRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  &ateletpb.SetWorkerCapacityRequest{Capacity: &ateapipb.WorkerResources{}},
	}, {
		name: "missing capacity",
		obj:  &ateletpb.SetWorkerCapacityRequest{},
		want: field.ErrorList{field.Required(field.NewPath("capacity"), "")},
	}, {
		name: "full capacity",
		obj: &ateletpb.SetWorkerCapacityRequest{
			Capacity: &ateapipb.WorkerResources{Actors: 4, Resources: &ateapipb.Resources{
				Limits: []*ateapipb.Limits{{Name: "cpu", Quantity: "4"}, {Name: "memory", Quantity: "8Gi"}},
			}},
		},
	}, {
		name: "negative actors",
		obj:  &ateletpb.SetWorkerCapacityRequest{Capacity: &ateapipb.WorkerResources{Actors: -1}},
		want: field.ErrorList{field.Invalid(field.NewPath("capacity", "actors"), nil, "").WithOrigin("minimum")},
	}, {
		name: "unsupported resource name",
		obj:  withLimits(&ateapipb.Limits{Name: "gpu", Quantity: "1"}),
		want: field.ErrorList{field.NotSupported[string](limitsPath.Index(0).Child("name"), nil, nil)},
	}, {
		name: "missing resource name",
		obj:  withLimits(&ateapipb.Limits{Quantity: "1"}),
		want: field.ErrorList{
			field.Required(limitsPath.Index(0).Child("name"), ""),
			field.NotSupported[string](limitsPath.Index(0).Child("name"), nil, nil),
		},
	}, {
		name: "resource name too long",
		obj:  withLimits(&ateapipb.Limits{Name: strings.Repeat("x", 17), Quantity: "1"}),
		want: field.ErrorList{
			field.TooLong(limitsPath.Index(0).Child("name"), nil, 16).WithOrigin("maxLength"),
			field.NotSupported[string](limitsPath.Index(0).Child("name"), nil, nil),
		},
	}, {
		name: "quantity too long",
		obj:  withLimits(&ateapipb.Limits{Name: "memory", Quantity: strings.Repeat("1", 33)}),
		want: field.ErrorList{field.TooLong(limitsPath.Index(0).Child("quantity"), nil, 32).WithOrigin("maxLength")},
	}, {
		name: "duplicate resource name",
		obj:  withLimits(&ateapipb.Limits{Name: "cpu", Quantity: "1"}, &ateapipb.Limits{Name: "cpu", Quantity: "2"}),
		want: field.ErrorList{field.Duplicate(limitsPath.Index(1), nil)},
	}, {
		name: "missing quantity",
		obj:  withLimits(&ateapipb.Limits{Name: "cpu"}),
		want: field.ErrorList{field.Required(limitsPath.Index(0).Child("quantity"), "")},
	}, {
		name: "malformed quantity",
		obj:  withLimits(&ateapipb.Limits{Name: "cpu", Quantity: "not-a-quantity"}),
		want: field.ErrorList{field.Invalid(limitsPath.Index(0).Child("quantity"), nil, "")},
	}, {
		name: "negative quantity",
		obj:  withLimits(&ateapipb.Limits{Name: "memory", Quantity: "-1Gi"}),
		want: field.ErrorList{field.Invalid(limitsPath.Index(0).Child("quantity"), nil, "")},
	}, {
		name: "zero quantity",
		obj:  withLimits(&ateapipb.Limits{Name: "memory", Quantity: "0"}),
		want: field.ErrorList{field.Invalid(limitsPath.Index(0).Child("quantity"), nil, "")},
	}, {
		name: "cpu at the bound",
		obj:  withLimits(&ateapipb.Limits{Name: "cpu", Quantity: "1000"}),
		want: field.ErrorList{field.Invalid(limitsPath.Index(0).Child("quantity"), nil, "")},
	}, {
		name: "too many limits",
		obj: withLimits(
			&ateapipb.Limits{Name: "cpu", Quantity: "1"},
			&ateapipb.Limits{Name: "memory", Quantity: "1Gi"},
			&ateapipb.Limits{Name: "cpu", Quantity: "2"},
		),
		want: field.ErrorList{field.TooMany(limitsPath, 3, 2).WithOrigin("maxItems")},
	}, {
		name: "nil limit entry",
		obj:  withLimits(nil),
		want: field.ErrorList{field.Required(limitsPath.Index(0), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expectErrors(t, tt.want, Validate_SetWorkerCapacityRequest(context.Background(), createOp, nil, tt.obj, nil))
			expectEdge(t, ValidateSetWorkerCapacityRequest(context.Background(), tt.obj), len(tt.want) > 0)
		})
	}
}
