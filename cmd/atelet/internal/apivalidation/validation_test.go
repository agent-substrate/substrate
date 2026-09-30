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
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const testDigestImage = "example.com/app@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// createOp is for tests of nested messages, which have no request wrapper.
var createOp = operation.Operation{Type: operation.Create}

func assertValidateErr(t *testing.T, got field.ErrorList, want field.ErrorList) {
	t.Helper()
	field.ErrorMatcher{}.ByType().ByField().ByOrigin().Test(t, want, got)
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

func TestValidateTerminateRequest(t *testing.T) {
	valid := func(mutate ...func(*ateletpb.TerminateRequest)) *ateletpb.TerminateRequest {
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
		name: "invalid target_ateom_uid: path escape",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.TargetAteomUid = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("target_ateom_uid"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing atespace",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.Atespace = "" }),
		want: field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		name: "invalid atespace: uppercase",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.Atespace = "Team-A" }),
		want: field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_name",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.ActorName = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_name"), "")},
	}, {
		name: "invalid actor_name: path escape",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.ActorName = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_uid",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid actor_uid: not a uuid",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.ActorUid = "not-a-uuid" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "invalid actor_template_atespace: uppercase",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.ActorTemplateAtespace = "Team-A" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_template_atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "invalid actor_template_name: trailing dash",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.ActorTemplateName = "tmpl-" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_template_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "spec errors surface at the spec's path",
		obj: valid(func(r *ateletpb.TerminateRequest) {
			r.Spec = &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "pause", Image: testDigestImage}}}
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("spec", "containers").Index(0).Child("name"), nil, "")},
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
			assertValidateErr(t, ValidateTerminateRequest(context.Background(), tt.obj), tt.want)
		})
	}
}

func TestValidateSetWorkerCapacityRequest(t *testing.T) {
	withLimits := func(limits ...*ateletpb.Limits) *ateletpb.SetWorkerCapacityRequest {
		return &ateletpb.SetWorkerCapacityRequest{
			Capacity: &ateletpb.WorkerResources{Resources: &ateletpb.Resources{Limits: limits}},
		}
	}
	limitsPath := field.NewPath("capacity", "resources", "limits")

	tests := []struct {
		name string
		obj  *ateletpb.SetWorkerCapacityRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  &ateletpb.SetWorkerCapacityRequest{Capacity: &ateletpb.WorkerResources{}},
	}, {
		name: "missing capacity",
		obj:  &ateletpb.SetWorkerCapacityRequest{},
		want: field.ErrorList{field.Required(field.NewPath("capacity"), "")},
	}, {
		name: "full capacity",
		obj: &ateletpb.SetWorkerCapacityRequest{
			Capacity: &ateletpb.WorkerResources{Actors: 4, Resources: &ateletpb.Resources{
				Limits: []*ateletpb.Limits{{Name: "cpu", Quantity: "4"}, {Name: "memory", Quantity: "8Gi"}},
			}},
		},
	}, {
		name: "negative actors",
		obj:  &ateletpb.SetWorkerCapacityRequest{Capacity: &ateletpb.WorkerResources{Actors: -1}},
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
			assertValidateErr(t, ValidateSetWorkerCapacityRequest(context.Background(), tt.obj), tt.want)
		})
	}
}

// TestValidateWorkloadSpec covers the rules WorkloadSpec owns: the containers
// list. One nested case proves the element validator runs; its own rules are
// covered by TestValidateContainer.
func TestValidateWorkloadSpec(t *testing.T) {
	ctr := func(name string) *ateletpb.Container {
		return &ateletpb.Container{Name: name, Image: testDigestImage}
	}
	valid := func(mutate ...func(*ateletpb.WorkloadSpec)) *ateletpb.WorkloadSpec {
		s := &ateletpb.WorkloadSpec{
			Containers: []*ateletpb.Container{ctr("main"), ctr("sidecar")},
		}
		for _, m := range mutate {
			m(s)
		}
		return s
	}

	tests := []struct {
		name string
		obj  *ateletpb.WorkloadSpec
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "empty spec",
		obj:  &ateletpb.WorkloadSpec{},
	}, {
		name: "duplicate container names",
		obj: valid(func(s *ateletpb.WorkloadSpec) {
			s.Containers = []*ateletpb.Container{ctr("main"), ctr("main")}
		}),
		want: field.ErrorList{field.Duplicate(field.NewPath("containers").Index(1), nil)},
	}, {
		name: "too many containers",
		obj: valid(func(s *ateletpb.WorkloadSpec) {
			s.Containers = nil
			for _, n := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"} {
				s.Containers = append(s.Containers, ctr(n))
			}
		}),
		want: field.ErrorList{field.TooMany(field.NewPath("containers"), 11, 10).WithOrigin("maxItems")},
	}, {
		name: "container errors surface at the container's path",
		obj: valid(func(s *ateletpb.WorkloadSpec) {
			s.Containers[0].Resources = &ateletpb.ResourceLimits{CpuMillis: -1}
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(0).Child("resources", "cpu_millis"), nil, "").WithOrigin("minimum")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, Validate_WorkloadSpec(context.Background(), createOp, nil, tt.obj, nil), tt.want)
		})
	}
}

// TestValidateContainer covers Container and every message it holds, with
// errors asserted at their paths under the container.
func TestValidateContainer(t *testing.T) {
	valid := func(mutate ...func(*ateletpb.Container)) *ateletpb.Container {
		c := &ateletpb.Container{
			Name:    "main",
			Image:   testDigestImage,
			Command: []string{"/bin/app"},
			Args:    []string{"--serve"},
			Env:     []*ateletpb.EnvEntry{{Name: "PORT", Value: "8080"}},
			VolumeMounts: []*ateletpb.VolumeMount{
				{Name: "data", MountPath: "/data"},
				{Name: "data", MountPath: "/mnt/data"},
			},
		}
		for _, m := range mutate {
			m(c)
		}
		return c
	}
	env := func(mutate func(*ateletpb.EnvEntry)) *ateletpb.Container {
		return valid(func(c *ateletpb.Container) { mutate(c.Env[0]) })
	}
	// mount replaces the mounts with a single one, so the nesting check
	// between mounts stays out of the way.
	mount := func(name, path string) *ateletpb.Container {
		return valid(func(c *ateletpb.Container) {
			c.VolumeMounts = []*ateletpb.VolumeMount{{Name: name, MountPath: path}}
		})
	}
	probe := func(mutate ...func(*ateletpb.WakeupProbe)) *ateletpb.Container {
		p := &ateletpb.WakeupProbe{
			HttpGet:        &ateletpb.HTTPGetAction{Path: "/healthz", Port: 8080},
			TimeoutSeconds: 30,
		}
		for _, m := range mutate {
			m(p)
		}
		return valid(func(c *ateletpb.Container) { c.WakeupProbe = p })
	}
	caps := func(add, drop []string) *ateletpb.Container {
		return valid(func(c *ateletpb.Container) {
			c.SecurityContext = &ateletpb.SecurityContext{Capabilities: &ateletpb.Capabilities{Add: add, Drop: drop}}
		})
	}
	limits := func(r *ateletpb.ResourceLimits) *ateletpb.Container {
		return valid(func(c *ateletpb.Container) { c.Resources = r })
	}

	envPath := field.NewPath("env").Index(0)
	mountPath := field.NewPath("volume_mounts").Index(0)
	httpGetPath := field.NewPath("wakeup_probe", "http_get")
	capsPath := field.NewPath("security_context", "capabilities")
	invalidMountPath := field.ErrorList{field.Invalid(mountPath.Child("mount_path"), nil, "")}

	tests := []struct {
		name string
		obj  *ateletpb.Container
		want field.ErrorList
	}{
		// Container name, image, command, and args.
		{
			name: "valid: the same volume mounted at two paths",
			obj:  valid(),
		}, {
			name: "missing name",
			obj:  valid(func(c *ateletpb.Container) { c.Name = "" }),
			want: field.ErrorList{field.Required(field.NewPath("name"), "")},
		}, {
			name: "invalid name: uppercase",
			obj:  valid(func(c *ateletpb.Container) { c.Name = "Main" }),
			want: field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
		}, {
			name: "reserved name pause",
			obj:  valid(func(c *ateletpb.Container) { c.Name = "pause" }),
			want: field.ErrorList{field.Invalid(field.NewPath("name"), nil, "")},
		}, {
			name: "missing image",
			obj:  valid(func(c *ateletpb.Container) { c.Image = "" }),
			want: field.ErrorList{field.Required(field.NewPath("image"), "")},
		}, {
			name: "image not pinned by digest",
			obj:  valid(func(c *ateletpb.Container) { c.Image = "example.com/app:v1" }),
			want: field.ErrorList{field.Invalid(field.NewPath("image"), nil, "")},
		}, {
			name: "image with a malformed digest",
			obj:  valid(func(c *ateletpb.Container) { c.Image = "example.com/app@sha256:abc" }),
			want: field.ErrorList{field.Invalid(field.NewPath("image"), nil, "")},
		}, {
			name: "empty command and args are allowed",
			obj: valid(func(c *ateletpb.Container) {
				c.Command = nil
				c.Args = nil
			}),
		}, {
			name: "too many command items",
			obj:  valid(func(c *ateletpb.Container) { c.Command = make([]string, 65) }),
			want: field.ErrorList{field.TooMany(field.NewPath("command"), 65, 64).WithOrigin("maxItems")},
		}, {
			name: "arg over the length guardrail",
			obj:  valid(func(c *ateletpb.Container) { c.Args = []string{strings.Repeat("a", 4097)} }),
			want: field.ErrorList{field.TooLong(field.NewPath("args").Index(0), nil, 4096).WithOrigin("maxLength")},
		},

		// EnvEntry.
		{
			name: "duplicate env names",
			obj: valid(func(c *ateletpb.Container) {
				c.Env = append(c.Env, &ateletpb.EnvEntry{Name: "PORT", Value: "9"})
			}),
			want: field.ErrorList{field.Duplicate(field.NewPath("env").Index(1), nil)},
		}, {
			name: "env: missing name",
			obj:  env(func(e *ateletpb.EnvEntry) { e.Name = "" }),
			want: field.ErrorList{field.Required(envPath.Child("name"), "")},
		}, {
			name: "env: name with equals sign",
			obj:  env(func(e *ateletpb.EnvEntry) { e.Name = "A=B" }),
			want: field.ErrorList{field.Invalid(envPath.Child("name"), nil, "")},
		}, {
			name: "env: name with spaces and punctuation is allowed",
			obj:  env(func(e *ateletpb.EnvEntry) { e.Name = "weird name!" }),
		}, {
			name: "env: name with a non-ASCII rune",
			obj:  env(func(e *ateletpb.EnvEntry) { e.Name = "café" }),
			want: field.ErrorList{field.Invalid(envPath.Child("name"), nil, "")},
		}, {
			name: "env: name too long",
			obj:  env(func(e *ateletpb.EnvEntry) { e.Name = strings.Repeat("N", 257) }),
			want: field.ErrorList{field.TooLong(envPath.Child("name"), nil, 256).WithOrigin("maxLength")},
		}, {
			name: "env: empty value is allowed",
			obj:  env(func(e *ateletpb.EnvEntry) { e.Value = "" }),
		}, {
			name: "env: value too long",
			obj:  env(func(e *ateletpb.EnvEntry) { e.Value = strings.Repeat("v", 32769) }),
			want: field.ErrorList{field.TooLong(envPath.Child("value"), nil, 32768).WithOrigin("maxLength")},
		},

		// VolumeMount.
		{
			name: "duplicate mount paths",
			obj:  valid(func(c *ateletpb.Container) { c.VolumeMounts[1].MountPath = "/data" }),
			want: field.ErrorList{field.Duplicate(field.NewPath("volume_mounts").Index(1), nil)},
		}, {
			name: "nested mount paths",
			obj:  valid(func(c *ateletpb.Container) { c.VolumeMounts[1].MountPath = "/data/nested" }),
			want: field.ErrorList{field.Invalid(field.NewPath("volume_mounts").Index(1).Child("mount_path"), nil, "")},
		}, {
			name: "nesting rejected regardless of order",
			obj:  valid(func(c *ateletpb.Container) { c.VolumeMounts[0].MountPath = "/mnt/data/nested" }),
			want: field.ErrorList{field.Invalid(field.NewPath("volume_mounts").Index(1).Child("mount_path"), nil, "")},
		}, {
			name: "sibling paths with a shared segment prefix are allowed",
			obj: valid(func(c *ateletpb.Container) {
				c.VolumeMounts[0].MountPath = "/data/a"
				c.VolumeMounts[1].MountPath = "/data/ab"
			}),
		}, {
			name: "mount: missing name",
			obj:  mount("", "/var/data"),
			want: field.ErrorList{field.Required(mountPath.Child("name"), "")},
		}, {
			name: "mount: invalid name: uppercase",
			obj:  mount("Data", "/var/data"),
			want: field.ErrorList{field.Invalid(mountPath.Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
		}, {
			name: "mount: missing mount_path",
			obj:  mount("data", ""),
			want: field.ErrorList{field.Required(mountPath.Child("mount_path"), "")},
		},
		{name: "mount: relative mount_path", obj: mount("data", "var/data"), want: invalidMountPath},
		{name: "mount: root mount_path", obj: mount("data", "/"), want: invalidMountPath},
		{name: "mount: trailing slash", obj: mount("data", "/var/data/"), want: invalidMountPath},
		{name: "mount: double slash", obj: mount("data", "/var//data"), want: invalidMountPath},
		{name: "mount: colon", obj: mount("data", "/var/da:ta"), want: invalidMountPath},
		{name: "mount: dot-dot segment", obj: mount("data", "/var/../etc"), want: invalidMountPath},
		{name: "mount: control character", obj: mount("data", "/var/da\x01ta"), want: invalidMountPath},

		// WakeupProbe and HTTPGetAction.
		{
			name: "valid probe",
			obj:  probe(),
		}, {
			name: "probe: missing http_get",
			obj:  probe(func(p *ateletpb.WakeupProbe) { p.HttpGet = nil }),
			want: field.ErrorList{field.Required(httpGetPath, "")},
		}, {
			name: "probe: missing timeout",
			obj:  probe(func(p *ateletpb.WakeupProbe) { p.TimeoutSeconds = 0 }),
			want: field.ErrorList{field.Required(field.NewPath("wakeup_probe", "timeout_seconds"), "")},
		}, {
			name: "probe: timeout above the bound",
			obj:  probe(func(p *ateletpb.WakeupProbe) { p.TimeoutSeconds = 3601 }),
			want: field.ErrorList{field.Invalid(field.NewPath("wakeup_probe", "timeout_seconds"), nil, "").WithOrigin("maximum")},
		}, {
			name: "probe: missing path",
			obj:  probe(func(p *ateletpb.WakeupProbe) { p.HttpGet.Path = "" }),
			want: field.ErrorList{field.Required(httpGetPath.Child("path"), "")},
		}, {
			name: "probe: path without a leading slash",
			obj:  probe(func(p *ateletpb.WakeupProbe) { p.HttpGet.Path = "healthz" }),
			want: field.ErrorList{field.Invalid(httpGetPath.Child("path"), nil, "")},
		}, {
			name: "probe: path with a query string",
			obj:  probe(func(p *ateletpb.WakeupProbe) { p.HttpGet.Path = "/healthz?verbose=1" }),
			want: field.ErrorList{field.Invalid(httpGetPath.Child("path"), nil, "")},
		}, {
			name: "probe: path with a valid percent escape",
			obj:  probe(func(p *ateletpb.WakeupProbe) { p.HttpGet.Path = "/health%20z" }),
		}, {
			name: "probe: path with a malformed percent escape",
			obj:  probe(func(p *ateletpb.WakeupProbe) { p.HttpGet.Path = "/health%2" }),
			want: field.ErrorList{field.Invalid(httpGetPath.Child("path"), nil, "")},
		}, {
			name: "probe: missing port",
			obj:  probe(func(p *ateletpb.WakeupProbe) { p.HttpGet.Port = 0 }),
			want: field.ErrorList{field.Required(httpGetPath.Child("port"), "")},
		}, {
			name: "probe: port above the range",
			obj:  probe(func(p *ateletpb.WakeupProbe) { p.HttpGet.Port = 65536 }),
			want: field.ErrorList{field.Invalid(httpGetPath.Child("port"), nil, "").WithOrigin("maximum")},
		},

		// SecurityContext.
		{
			name: "valid capabilities",
			obj:  caps([]string{"NET_BIND_SERVICE"}, []string{"ALL"}),
		}, {
			name: "empty security context",
			obj:  valid(func(c *ateletpb.Container) { c.SecurityContext = &ateletpb.SecurityContext{} }),
		}, {
			name: "capabilities: add does not accept ALL",
			obj:  caps([]string{"ALL"}, nil),
			want: field.ErrorList{field.Invalid(capsPath.Child("add").Index(0), nil, "")},
		}, {
			name: "capabilities: drop accepts ALL",
			obj:  caps(nil, []string{"ALL"}),
		}, {
			name: "capabilities: CAP_ prefix rejected",
			obj:  caps([]string{"CAP_NET_BIND_SERVICE"}, nil),
			want: field.ErrorList{field.Invalid(capsPath.Child("add").Index(0), nil, "")},
		}, {
			name: "capabilities: lowercase rejected",
			obj:  caps(nil, []string{"net_bind_service"}),
			want: field.ErrorList{field.Invalid(capsPath.Child("drop").Index(0), nil, "")},
		}, {
			name: "capabilities: duplicate rejected by the set",
			obj:  caps([]string{"SYS_TIME", "SYS_TIME"}, nil),
			want: field.ErrorList{field.Duplicate(capsPath.Child("add").Index(1), nil)},
		},

		// ResourceLimits.
		{
			name: "valid limits",
			obj:  limits(&ateletpb.ResourceLimits{MemoryBytes: 1 << 30, CpuMillis: 500}),
		}, {
			name: "limits: zero means unset",
			obj:  limits(&ateletpb.ResourceLimits{}),
		}, {
			name: "limits: negative memory_bytes",
			obj:  limits(&ateletpb.ResourceLimits{MemoryBytes: -1}),
			want: field.ErrorList{field.Invalid(field.NewPath("resources", "memory_bytes"), nil, "").WithOrigin("minimum")},
		}, {
			name: "limits: negative cpu_millis",
			obj:  limits(&ateletpb.ResourceLimits{CpuMillis: -1}),
			want: field.ErrorList{field.Invalid(field.NewPath("resources", "cpu_millis"), nil, "").WithOrigin("minimum")},
		}, {
			name: "limits: cpu_millis at the cap",
			obj:  limits(&ateletpb.ResourceLimits{CpuMillis: 999999}),
		}, {
			name: "limits: cpu_millis above the cap",
			obj:  limits(&ateletpb.ResourceLimits{CpuMillis: 1000000}),
			want: field.ErrorList{field.Invalid(field.NewPath("resources", "cpu_millis"), nil, "").WithOrigin("maximum")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, Validate_Container(context.Background(), createOp, nil, tt.obj, nil), tt.want)
		})
	}
}
