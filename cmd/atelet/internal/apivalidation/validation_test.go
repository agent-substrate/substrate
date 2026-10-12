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
	"fmt"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func assertValidateErr(t *testing.T, got field.ErrorList, want field.ErrorList) {
	t.Helper()
	field.ErrorMatcher{}.ByType().ByField().ByOrigin().Test(t, want, got)
}

// runtimeWithAttributes returns a valid gVisor runtime carrying n attributes
// with distinct keys.
func runtimeWithAttributes(n int) *ateletpb.SandboxRuntime {
	attrs := make([]*ateletpb.AttributeEntry, n)
	for i := range attrs {
		attrs[i] = &ateletpb.AttributeEntry{Key: fmt.Sprintf("k%d", i), Value: "v"}
	}
	return &ateletpb.SandboxRuntime{
		SandboxClass: "gvisor",
		Version:      &ateletpb.VersionedSandboxCompat{SchemaVersion: "v1", Attributes: attrs},
	}
}

// sandboxRuntimes returns n runtimes built by mk.
func sandboxRuntimes(n int, mk func() *ateletpb.SandboxRuntime) []*ateletpb.SandboxRuntime {
	rts := make([]*ateletpb.SandboxRuntime, n)
	for i := range rts {
		rts[i] = mk()
	}
	return rts
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
			assertValidateErr(t, ValidateRequestActorSuspendRequest(context.Background(), tt.obj), tt.want)
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
			assertValidateErr(t, ValidateMintActorCertificateRequest(context.Background(), tt.obj), tt.want)
		})
	}
}

func TestValidateRegisterWorkerRequest(t *testing.T) {
	testRuntime := func() *ateletpb.SandboxRuntime {
		return &ateletpb.SandboxRuntime{
			SandboxClass: "gvisor",
			Version: &ateletpb.VersionedSandboxCompat{
				SchemaVersion: "v1",
				Attributes: []*ateletpb.AttributeEntry{
					{Key: "architecture", Value: "amd64"},
				},
			},
		}
	}
	withLimits := func(limits ...*ateletpb.Limits) *ateletpb.RegisterWorkerRequest {
		return &ateletpb.RegisterWorkerRequest{
			Capacity:       &ateletpb.WorkerResources{Resources: &ateletpb.Resources{Limits: limits}},
			DefaultRuntime: testRuntime(),
		}
	}
	limitsPath := field.NewPath("capacity", "resources", "limits")
	defaultRuntimePath := field.NewPath("default_runtime")
	restorablePath := field.NewPath("restorable_runtimes")

	tests := []struct {
		name string
		obj  *ateletpb.RegisterWorkerRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  &ateletpb.RegisterWorkerRequest{Capacity: &ateletpb.WorkerResources{}, DefaultRuntime: testRuntime()},
	}, {
		name: "valid with restorable runtimes",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity:           &ateletpb.WorkerResources{},
			DefaultRuntime:     testRuntime(),
			RestorableRuntimes: []*ateletpb.SandboxRuntime{testRuntime()},
		},
	}, {
		name: "missing capacity",
		obj:  &ateletpb.RegisterWorkerRequest{DefaultRuntime: testRuntime()},
		want: field.ErrorList{field.Required(field.NewPath("capacity"), "")},
	}, {
		name: "missing default_runtime",
		obj:  &ateletpb.RegisterWorkerRequest{Capacity: &ateletpb.WorkerResources{}},
		want: field.ErrorList{field.Required(defaultRuntimePath, "")},
	}, {
		name: "missing default_runtime.sandbox_class",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity: &ateletpb.WorkerResources{},
			DefaultRuntime: &ateletpb.SandboxRuntime{
				Version: testRuntime().Version,
			},
		},
		want: field.ErrorList{field.Required(defaultRuntimePath.Child("sandbox_class"), "")},
	}, {
		name: "default_runtime.sandbox_class too long",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity: &ateletpb.WorkerResources{},
			DefaultRuntime: &ateletpb.SandboxRuntime{
				SandboxClass: strings.Repeat("c", 64),
				Version:      testRuntime().Version,
			},
		},
		want: field.ErrorList{field.TooLong(defaultRuntimePath.Child("sandbox_class"), "", 63).WithOrigin("maxLength")},
	}, {
		name: "missing default_runtime.version",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity:       &ateletpb.WorkerResources{},
			DefaultRuntime: &ateletpb.SandboxRuntime{SandboxClass: "gvisor"},
		},
		want: field.ErrorList{field.Required(defaultRuntimePath.Child("version"), "")},
	}, {
		name: "missing default_runtime.version.schema_version",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity: &ateletpb.WorkerResources{},
			DefaultRuntime: &ateletpb.SandboxRuntime{
				SandboxClass: "gvisor",
				Version: &ateletpb.VersionedSandboxCompat{
					Attributes: []*ateletpb.AttributeEntry{{Key: "architecture", Value: "amd64"}},
				},
			},
		},
		want: field.ErrorList{field.Required(defaultRuntimePath.Child("version", "schema_version"), "")},
	}, {
		name: "default_runtime.version.schema_version too long",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity: &ateletpb.WorkerResources{},
			DefaultRuntime: &ateletpb.SandboxRuntime{
				SandboxClass: "gvisor",
				Version: &ateletpb.VersionedSandboxCompat{
					SchemaVersion: strings.Repeat("v", 65),
					Attributes:    []*ateletpb.AttributeEntry{{Key: "architecture", Value: "amd64"}},
				},
			},
		},
		want: field.ErrorList{field.TooLong(defaultRuntimePath.Child("version", "schema_version"), "", 64).WithOrigin("maxLength")},
	}, {
		name: "missing default_runtime.version.attributes",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity: &ateletpb.WorkerResources{},
			DefaultRuntime: &ateletpb.SandboxRuntime{
				SandboxClass: "gvisor",
				Version:      &ateletpb.VersionedSandboxCompat{SchemaVersion: "v1"},
			},
		},
		want: field.ErrorList{field.Required(defaultRuntimePath.Child("version", "attributes"), "")},
	}, {
		name: "duplicate attribute key",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity: &ateletpb.WorkerResources{},
			DefaultRuntime: &ateletpb.SandboxRuntime{
				SandboxClass: "gvisor",
				Version: &ateletpb.VersionedSandboxCompat{
					SchemaVersion: "v1",
					Attributes: []*ateletpb.AttributeEntry{
						{Key: "architecture", Value: "amd64"},
						{Key: "architecture", Value: "arm64"},
					},
				},
			},
		},
		want: field.ErrorList{field.Duplicate(defaultRuntimePath.Child("version", "attributes").Index(1), nil)},
	}, {
		name: "missing attribute key",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity: &ateletpb.WorkerResources{},
			DefaultRuntime: &ateletpb.SandboxRuntime{
				SandboxClass: "gvisor",
				Version: &ateletpb.VersionedSandboxCompat{
					SchemaVersion: "v1",
					Attributes:    []*ateletpb.AttributeEntry{{Value: "v"}},
				},
			},
		},
		want: field.ErrorList{field.Required(defaultRuntimePath.Child("version", "attributes").Index(0).Child("key"), "")},
	}, {
		name: "attribute key too long",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity: &ateletpb.WorkerResources{},
			DefaultRuntime: &ateletpb.SandboxRuntime{
				SandboxClass: "gvisor",
				Version: &ateletpb.VersionedSandboxCompat{
					SchemaVersion: "v1",
					Attributes:    []*ateletpb.AttributeEntry{{Key: strings.Repeat("k", 129), Value: "v"}},
				},
			},
		},
		want: field.ErrorList{field.TooLong(defaultRuntimePath.Child("version", "attributes").Index(0).Child("key"), "", 128).WithOrigin("maxLength")},
	}, {
		name: "missing attribute value",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity: &ateletpb.WorkerResources{},
			DefaultRuntime: &ateletpb.SandboxRuntime{
				SandboxClass: "gvisor",
				Version: &ateletpb.VersionedSandboxCompat{
					SchemaVersion: "v1",
					Attributes:    []*ateletpb.AttributeEntry{{Key: "k"}},
				},
			},
		},
		want: field.ErrorList{field.Required(defaultRuntimePath.Child("version", "attributes").Index(0).Child("value"), "")},
	}, {
		name: "attribute value too long",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity: &ateletpb.WorkerResources{},
			DefaultRuntime: &ateletpb.SandboxRuntime{
				SandboxClass: "gvisor",
				Version: &ateletpb.VersionedSandboxCompat{
					SchemaVersion: "v1",
					Attributes:    []*ateletpb.AttributeEntry{{Key: "k", Value: strings.Repeat("v", 257)}},
				},
			},
		},
		want: field.ErrorList{field.TooLong(defaultRuntimePath.Child("version", "attributes").Index(0).Child("value"), "", 256).WithOrigin("maxLength")},
	}, {
		name: "attributes at the bound",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity:       &ateletpb.WorkerResources{},
			DefaultRuntime: runtimeWithAttributes(32),
		},
	}, {
		name: "too many attributes",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity:       &ateletpb.WorkerResources{},
			DefaultRuntime: runtimeWithAttributes(33),
		},
		want: field.ErrorList{field.TooMany(defaultRuntimePath.Child("version", "attributes"), 33, 32).WithOrigin("maxItems")},
	}, {
		name: "nil restorable_runtimes entry",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity:           &ateletpb.WorkerResources{},
			DefaultRuntime:     testRuntime(),
			RestorableRuntimes: []*ateletpb.SandboxRuntime{nil},
		},
		want: field.ErrorList{field.Required(restorablePath.Index(0), "")},
	}, {
		name: "invalid restorable_runtimes entry",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity:           &ateletpb.WorkerResources{},
			DefaultRuntime:     testRuntime(),
			RestorableRuntimes: []*ateletpb.SandboxRuntime{{SandboxClass: "gvisor"}},
		},
		want: field.ErrorList{field.Required(restorablePath.Index(0).Child("version"), "")},
	}, {
		name: "restorable_runtimes at the bound",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity:           &ateletpb.WorkerResources{},
			DefaultRuntime:     testRuntime(),
			RestorableRuntimes: sandboxRuntimes(32, testRuntime),
		},
	}, {
		name: "too many restorable_runtimes",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity:           &ateletpb.WorkerResources{},
			DefaultRuntime:     testRuntime(),
			RestorableRuntimes: sandboxRuntimes(33, testRuntime),
		},
		want: field.ErrorList{field.TooMany(restorablePath, 33, 32).WithOrigin("maxItems")},
	}, {
		name: "full capacity",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity: &ateletpb.WorkerResources{Actors: 4, Resources: &ateletpb.Resources{
				Limits: []*ateletpb.Limits{{Name: "cpu", Quantity: "4"}, {Name: "memory", Quantity: "8Gi"}},
			}},
			DefaultRuntime: testRuntime(),
		},
	}, {
		name: "negative actors",
		obj:  &ateletpb.RegisterWorkerRequest{Capacity: &ateletpb.WorkerResources{Actors: -1}, DefaultRuntime: testRuntime()},
		want: field.ErrorList{field.Invalid(field.NewPath("capacity", "actors"), nil, "").WithOrigin("minimum")},
	}, {
		name: "unsupported resource name",
		obj:  withLimits(&ateletpb.Limits{Name: "gpu", Quantity: "1"}),
		want: field.ErrorList{field.NotSupported[string](limitsPath.Index(0).Child("name"), nil, nil)},
	}, {
		name: "missing resource name",
		obj:  withLimits(&ateletpb.Limits{Quantity: "1"}),
		want: field.ErrorList{
			field.Required(limitsPath.Index(0).Child("name"), ""),
			field.NotSupported[string](limitsPath.Index(0).Child("name"), nil, nil),
		},
	}, {
		name: "resource name too long",
		obj:  withLimits(&ateletpb.Limits{Name: strings.Repeat("x", 17), Quantity: "1"}),
		want: field.ErrorList{
			field.TooLong(limitsPath.Index(0).Child("name"), nil, 16).WithOrigin("maxLength"),
			field.NotSupported[string](limitsPath.Index(0).Child("name"), nil, nil),
		},
	}, {
		name: "quantity too long",
		obj:  withLimits(&ateletpb.Limits{Name: "memory", Quantity: strings.Repeat("1", 33)}),
		want: field.ErrorList{field.TooLong(limitsPath.Index(0).Child("quantity"), nil, 32).WithOrigin("maxLength")},
	}, {
		name: "duplicate resource name",
		obj:  withLimits(&ateletpb.Limits{Name: "cpu", Quantity: "1"}, &ateletpb.Limits{Name: "cpu", Quantity: "2"}),
		want: field.ErrorList{field.Duplicate(limitsPath.Index(1), nil)},
	}, {
		name: "missing quantity",
		obj:  withLimits(&ateletpb.Limits{Name: "cpu"}),
		want: field.ErrorList{field.Required(limitsPath.Index(0).Child("quantity"), "")},
	}, {
		name: "malformed quantity",
		obj:  withLimits(&ateletpb.Limits{Name: "cpu", Quantity: "not-a-quantity"}),
		want: field.ErrorList{field.Invalid(limitsPath.Index(0).Child("quantity"), nil, "")},
	}, {
		name: "negative quantity",
		obj:  withLimits(&ateletpb.Limits{Name: "memory", Quantity: "-1Gi"}),
		want: field.ErrorList{field.Invalid(limitsPath.Index(0).Child("quantity"), nil, "")},
	}, {
		name: "zero quantity",
		obj:  withLimits(&ateletpb.Limits{Name: "memory", Quantity: "0"}),
		want: field.ErrorList{field.Invalid(limitsPath.Index(0).Child("quantity"), nil, "")},
	}, {
		name: "cpu below the bound",
		obj:  withLimits(&ateletpb.Limits{Name: "cpu", Quantity: "999"}),
	}, {
		name: "cpu at the bound",
		obj:  withLimits(&ateletpb.Limits{Name: "cpu", Quantity: "1000"}),
		want: field.ErrorList{field.Invalid(limitsPath.Index(0).Child("quantity"), nil, "")},
	}, {
		name: "too many limits",
		obj: withLimits(
			&ateletpb.Limits{Name: "cpu", Quantity: "1"},
			&ateletpb.Limits{Name: "memory", Quantity: "1Gi"},
			&ateletpb.Limits{Name: "cpu", Quantity: "2"},
		),
		want: field.ErrorList{field.TooMany(limitsPath, 3, 2).WithOrigin("maxItems")},
	}, {
		name: "nil limit entry",
		obj:  withLimits(nil),
		want: field.ErrorList{field.Required(limitsPath.Index(0), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateRegisterWorkerRequest(context.Background(), tt.obj), tt.want)
		})
	}
}
