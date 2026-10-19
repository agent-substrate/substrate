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

	"go.ate.dev/substrate/internal/proto/ateletpb"
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

func TestValidateGetActorEgressPolicyRequest(t *testing.T) {
	valid := func(mutate ...func(*ateletpb.GetActorEgressPolicyRequest)) *ateletpb.GetActorEgressPolicyRequest {
		r := &ateletpb.GetActorEgressPolicyRequest{
			ActorAtespace: "team-a",
			ActorName:     "actor-1",
		}
		for _, m := range mutate {
			m(r)
		}
		return r
	}

	tests := []struct {
		name string
		obj  *ateletpb.GetActorEgressPolicyRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "missing actor_atespace",
		obj:  valid(func(r *ateletpb.GetActorEgressPolicyRequest) { r.ActorAtespace = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_atespace"), "")},
	}, {
		name: "invalid actor_atespace: uppercase",
		obj:  valid(func(r *ateletpb.GetActorEgressPolicyRequest) { r.ActorAtespace = "Team-A" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_name",
		obj:  valid(func(r *ateletpb.GetActorEgressPolicyRequest) { r.ActorName = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_name"), "")},
	}, {
		name: "invalid actor_name: trailing dash",
		obj:  valid(func(r *ateletpb.GetActorEgressPolicyRequest) { r.ActorName = "actor-" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateGetActorEgressPolicyRequest(context.Background(), tt.obj), tt.want)
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

func TestValidateRunRequest(t *testing.T) {
	valid := func(mutate ...func(*ateletpb.RunRequest)) *ateletpb.RunRequest {
		r := &ateletpb.RunRequest{
			WorkerPodUid:          "0f9a3b1c-2d4e-5f60-7182-93a4b5c6d7e8",
			Atespace:              "team-a",
			ActorName:             "actor-1",
			ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
			ActorTemplateAtespace: "team-a",
			ActorTemplateName:     "tmpl-1",
			Spec:                  &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "worker", Image: testDigestImage}}},
			SandboxRuntime:        runtimeWithAttributes(1),
			CpuMilli:              500,
			MemoryBytes:           1 << 30,
		}
		for _, m := range mutate {
			m(r)
		}
		return r
	}
	containerName := field.NewPath("spec", "containers").Index(0).Child("name")

	tests := []struct {
		name string
		obj  *ateletpb.RunRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "missing worker_pod_uid",
		obj:  valid(func(r *ateletpb.RunRequest) { r.WorkerPodUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("worker_pod_uid"), "")},
	}, {
		name: "invalid worker_pod_uid: path escape",
		obj:  valid(func(r *ateletpb.RunRequest) { r.WorkerPodUid = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("worker_pod_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "invalid worker_pod_uid: not a pod UID",
		obj:  valid(func(r *ateletpb.RunRequest) { r.WorkerPodUid = "worker-1" }),
		want: field.ErrorList{field.Invalid(field.NewPath("worker_pod_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "missing atespace",
		obj:  valid(func(r *ateletpb.RunRequest) { r.Atespace = "" }),
		want: field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		name: "invalid atespace: path escape",
		obj:  valid(func(r *ateletpb.RunRequest) { r.Atespace = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_name",
		obj:  valid(func(r *ateletpb.RunRequest) { r.ActorName = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_name"), "")},
	}, {
		name: "invalid actor_name: path escape",
		obj:  valid(func(r *ateletpb.RunRequest) { r.ActorName = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_uid",
		obj:  valid(func(r *ateletpb.RunRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid actor_uid: path escape",
		obj:  valid(func(r *ateletpb.RunRequest) { r.ActorUid = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "invalid actor_template_atespace: uppercase",
		obj:  valid(func(r *ateletpb.RunRequest) { r.ActorTemplateAtespace = "Team-A" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_template_atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "invalid actor_template_name: trailing dash",
		obj:  valid(func(r *ateletpb.RunRequest) { r.ActorTemplateName = "tmpl-" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_template_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "unset template identity is allowed",
		obj: valid(func(r *ateletpb.RunRequest) {
			r.ActorTemplateAtespace = ""
			r.ActorTemplateName = ""
		}),
	}, {
		name: "missing spec",
		obj:  valid(func(r *ateletpb.RunRequest) { r.Spec = nil }),
		want: field.ErrorList{field.Required(field.NewPath("spec"), "")},
	}, {
		name: "invalid container name: path escape",
		obj: valid(func(r *ateletpb.RunRequest) {
			r.Spec.Containers[0].Name = "../escape"
		}),
		want: field.ErrorList{field.Invalid(containerName, nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "duplicate container name",
		obj: valid(func(r *ateletpb.RunRequest) {
			r.Spec.Containers = append(r.Spec.Containers, &ateletpb.Container{Name: "worker", Image: testDigestImage})
		}),
		want: field.ErrorList{field.Duplicate(field.NewPath("spec", "containers").Index(1), nil)},
	}, {
		name: "egress gateway with a DNS name",
		obj: valid(func(r *ateletpb.RunRequest) {
			r.EgressGateway = &ateletpb.EgressGateway{Address: "atenet-egress.ate-system.svc:443"}
		}),
	}, {
		name: "egress gateway with an IPv6 address",
		obj: valid(func(r *ateletpb.RunRequest) {
			r.EgressGateway = &ateletpb.EgressGateway{Address: "[fd00::1]:443"}
		}),
	}, {
		name: "egress gateway missing address",
		obj: valid(func(r *ateletpb.RunRequest) {
			r.EgressGateway = &ateletpb.EgressGateway{}
		}),
		want: field.ErrorList{field.Required(field.NewPath("egress_gateway", "address"), "")},
	}, {
		name: "egress gateway address without a port",
		obj: valid(func(r *ateletpb.RunRequest) {
			r.EgressGateway = &ateletpb.EgressGateway{Address: "atenet-egress.ate-system.svc"}
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("egress_gateway", "address"), nil, "")},
	}, {
		name: "egress gateway address too long",
		obj: valid(func(r *ateletpb.RunRequest) {
			r.EgressGateway = &ateletpb.EgressGateway{Address: strings.Repeat("a", 258) + ":443"}
		}),
		want: field.ErrorList{
			field.TooLong(field.NewPath("egress_gateway", "address"), nil, 261).WithOrigin("maxLength"),
			field.Invalid(field.NewPath("egress_gateway", "address"), nil, ""),
		},
	}, {
		name: "negative cpu_milli",
		obj:  valid(func(r *ateletpb.RunRequest) { r.CpuMilli = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("cpu_milli"), nil, "").WithOrigin("minimum")},
	}, {
		name: "cpu_milli at the bound",
		obj:  valid(func(r *ateletpb.RunRequest) { r.CpuMilli = 999999 }),
	}, {
		name: "cpu_milli over the bound",
		obj:  valid(func(r *ateletpb.RunRequest) { r.CpuMilli = 1000000 }),
		want: field.ErrorList{field.Invalid(field.NewPath("cpu_milli"), nil, "").WithOrigin("maximum")},
	}, {
		name: "negative memory_bytes",
		obj:  valid(func(r *ateletpb.RunRequest) { r.MemoryBytes = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("memory_bytes"), nil, "").WithOrigin("minimum")},
	}, {
		name: "unset sizes are allowed",
		obj: valid(func(r *ateletpb.RunRequest) {
			r.CpuMilli = 0
			r.MemoryBytes = 0
		}),
	}, {
		name: "missing sandbox_runtime",
		obj:  valid(func(r *ateletpb.RunRequest) { r.SandboxRuntime = nil }),
		want: field.ErrorList{field.Required(field.NewPath("sandbox_runtime"), "")},
	}, {
		name: "sandbox_runtime errors surface at their path",
		obj: valid(func(r *ateletpb.RunRequest) {
			r.SandboxRuntime.SandboxClass = "kata"
		}),
		want: field.ErrorList{field.NotSupported[string](field.NewPath("sandbox_runtime", "sandbox_class"), nil, nil)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateRunRequest(context.Background(), tt.obj), tt.want)
		})
	}
}

func TestValidateCheckpointRequest(t *testing.T) {
	const snapshotURI = "gs://bucket/root/atespaces/team-a/actors/01234567-89ab-cdef-0123-456789abcdef/snapshots/snap-1"
	valid := func(mutate ...func(*ateletpb.CheckpointRequest)) *ateletpb.CheckpointRequest {
		r := &ateletpb.CheckpointRequest{
			WorkerPodUid:          "0f9a3b1c-2d4e-5f60-7182-93a4b5c6d7e8",
			Atespace:              "team-a",
			ActorName:             "actor-1",
			ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
			ActorTemplateAtespace: "team-a",
			ActorTemplateName:     "tmpl-1",
			Spec:                  &ateletpb.WorkloadSpec{},
			Snapshot: &ateletpb.Snapshot{
				SnapshotUuid: "snap-1",
				Generation:   1,
				Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
				Object:       &ateletpb.ObjectStorage{SnapshotUri: snapshotURI},
			},
			UploadConfig: &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE},
		}
		for _, m := range mutate {
			m(r)
		}
		return r
	}
	deferred := func(name string) func(*ateletpb.CheckpointRequest) {
		return func(r *ateletpb.CheckpointRequest) {
			r.Snapshot.SnapshotUuid = name
			r.UploadConfig = &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED}
		}
	}

	tests := []struct {
		name string
		obj  *ateletpb.CheckpointRequest
		want field.ErrorList
	}{{
		name: "valid immediate",
		obj:  valid(),
	}, {
		name: "valid deferred",
		obj:  valid(deferred("pause-snap-1")),
	}, {
		name: "valid volumes fidelity",
		obj: valid(func(r *ateletpb.CheckpointRequest) {
			r.Snapshot.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
		}),
	}, {
		name: "missing worker_pod_uid",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.WorkerPodUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("worker_pod_uid"), "")},
	}, {
		name: "invalid worker_pod_uid: path escape",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.WorkerPodUid = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("worker_pod_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "invalid worker_pod_uid: not a pod UID",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.WorkerPodUid = "worker-1" }),
		want: field.ErrorList{field.Invalid(field.NewPath("worker_pod_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "invalid atespace: path escape",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.Atespace = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "invalid actor_name: uppercase",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.ActorName = "UPPER" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "invalid actor_uid: not a UUID",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.ActorUid = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "invalid actor_template_name: trailing dash",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.ActorTemplateName = "tmpl-" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_template_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "unset template identity is allowed",
		obj: valid(func(r *ateletpb.CheckpointRequest) {
			r.ActorTemplateAtespace = ""
			r.ActorTemplateName = ""
		}),
	}, {
		name: "missing spec",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.Spec = nil }),
		want: field.ErrorList{field.Required(field.NewPath("spec"), "")},
	}, {
		name: "spec errors surface at the spec's path",
		obj: valid(func(r *ateletpb.CheckpointRequest) {
			r.Spec = &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "_pause", Image: testDigestImage}}}
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("spec", "containers").Index(0).Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing snapshot",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.Snapshot = nil }),
		want: field.ErrorList{field.Required(field.NewPath("snapshot"), "")},
	}, {
		name: "missing upload_config",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.UploadConfig = nil }),
		want: field.ErrorList{field.Required(field.NewPath("upload_config"), "")},
	}, {
		name: "unspecified upload mode",
		obj: valid(func(r *ateletpb.CheckpointRequest) {
			r.UploadConfig = &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_UNSPECIFIED}
		}),
		want: field.ErrorList{field.Required(field.NewPath("upload_config", "mode"), "")},
	}, {
		name: "unknown upload mode",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.UploadConfig.Mode = ateletpb.UploadMode(3) }),
		want: field.ErrorList{field.Invalid(field.NewPath("upload_config", "mode"), nil, "").WithOrigin("maximum")},
	}, {
		name: "immediate upload without object storage",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.Snapshot.Object = nil }),
		want: field.ErrorList{field.Required(field.NewPath("snapshot", "object"), "")},
	}, {
		name: "deferred upload without object storage",
		obj: valid(deferred("pause-snap-1"), func(r *ateletpb.CheckpointRequest) {
			r.Snapshot.Object = nil
		}),
		want: field.ErrorList{field.Required(field.NewPath("snapshot", "object"), "")},
	}, {
		name: "missing snapshot_uuid",
		obj:  valid(deferred("")),
		want: field.ErrorList{field.Required(field.NewPath("snapshot", "snapshot_uuid"), "")},
	}, {
		name: "invalid snapshot_uuid: path escape",
		obj:  valid(deferred("../escape")),
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot", "snapshot_uuid"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing generation",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.Snapshot.Generation = 0 }),
		want: field.ErrorList{field.Required(field.NewPath("snapshot", "generation"), "")},
	}, {
		name: "negative generation",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.Snapshot.Generation = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot", "generation"), nil, "").WithOrigin("minimum")},
	}, {
		name: "missing snapshot_uri",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.Snapshot.Object.SnapshotUri = "" }),
		want: field.ErrorList{field.Required(field.NewPath("snapshot", "object", "snapshot_uri"), "")},
	}, {
		name: "invalid snapshot_uri: no bucket",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.Snapshot.Object.SnapshotUri = "relative/path" }),
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot", "object", "snapshot_uri"), nil, "")},
	}, {
		name: "snapshot_uri too long",
		obj: valid(func(r *ateletpb.CheckpointRequest) {
			r.Snapshot.Object.SnapshotUri = "gs://bucket/" + strings.Repeat("p", 2048)
		}),
		want: field.ErrorList{
			field.TooLong(field.NewPath("snapshot", "object", "snapshot_uri"), nil, 2048).WithOrigin("maxLength"),
			field.Invalid(field.NewPath("snapshot", "object", "snapshot_uri"), nil, ""),
		},
	}, {
		name: "unspecified fidelity",
		obj: valid(func(r *ateletpb.CheckpointRequest) {
			r.Snapshot.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
		}),
		want: field.ErrorList{field.Required(field.NewPath("snapshot", "fidelity"), "")},
	}, {
		name: "unknown fidelity",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.Snapshot.Fidelity = 4 }),
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot", "fidelity"), nil, "").WithOrigin("maximum")},
	}, {
		name: "rootfs fidelity is forwarded for the runtime to judge",
		obj: valid(func(r *ateletpb.CheckpointRequest) {
			r.Snapshot.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
		}),
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateCheckpointRequest(context.Background(), tt.obj), tt.want)
		})
	}
}

func TestValidateRestoreRequest(t *testing.T) {
	const snapshotURI = "gs://bucket/root/atespaces/team-a/actors/01234567-89ab-cdef-0123-456789abcdef/snapshots/snap-1"
	valid := func(mutate ...func(*ateletpb.RestoreRequest)) *ateletpb.RestoreRequest {
		r := &ateletpb.RestoreRequest{
			WorkerPodUid:          "0f9a3b1c-2d4e-5f60-7182-93a4b5c6d7e8",
			Atespace:              "team-a",
			ActorName:             "actor-1",
			ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
			ActorTemplateAtespace: "team-a",
			ActorTemplateName:     "tmpl-1",
			Spec:                  &ateletpb.WorkloadSpec{},
			Snapshot: &ateletpb.Snapshot{
				SnapshotUuid: "snap-1",
				Generation:   1,
				Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
				Object:       &ateletpb.ObjectStorage{SnapshotUri: snapshotURI},
			},
			SandboxRuntime: runtimeWithAttributes(1),
		}
		for _, m := range mutate {
			m(r)
		}
		return r
	}

	tests := []struct {
		name string
		obj  *ateletpb.RestoreRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "missing object storage",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.Snapshot.Object = nil }),
		want: field.ErrorList{field.Required(field.NewPath("snapshot", "object"), "")},
	}, {
		name: "valid volumes fidelity",
		obj: valid(func(r *ateletpb.RestoreRequest) {
			r.Snapshot.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
		}),
	}, {
		name: "valid size and egress",
		obj: valid(func(r *ateletpb.RestoreRequest) {
			r.CpuMilli = 999999
			r.MemoryBytes = 1 << 30
			r.EgressGateway = &ateletpb.EgressGateway{Address: "10.0.0.1:15001"}
		}),
	}, {
		name: "missing worker_pod_uid",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.WorkerPodUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("worker_pod_uid"), "")},
	}, {
		name: "invalid worker_pod_uid: path escape",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.WorkerPodUid = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("worker_pod_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "invalid worker_pod_uid: not a pod UID",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.WorkerPodUid = "worker-1" }),
		want: field.ErrorList{field.Invalid(field.NewPath("worker_pod_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "invalid atespace: path escape",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.Atespace = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "invalid actor_name: uppercase",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.ActorName = "UPPER" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "invalid actor_uid: not a UUID",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.ActorUid = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "invalid actor_template_atespace: slash",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.ActorTemplateAtespace = "no/slashes" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_template_atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "unset template identity is allowed",
		obj: valid(func(r *ateletpb.RestoreRequest) {
			r.ActorTemplateAtespace = ""
			r.ActorTemplateName = ""
		}),
	}, {
		name: "missing spec",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.Spec = nil }),
		want: field.ErrorList{field.Required(field.NewPath("spec"), "")},
	}, {
		name: "spec errors surface at the spec's path",
		obj: valid(func(r *ateletpb.RestoreRequest) {
			r.Spec = &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "_pause", Image: testDigestImage}}}
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("spec", "containers").Index(0).Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing snapshot",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.Snapshot = nil }),
		want: field.ErrorList{field.Required(field.NewPath("snapshot"), "")},
	}, {
		name: "missing snapshot_uuid",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.Snapshot.SnapshotUuid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("snapshot", "snapshot_uuid"), "")},
	}, {
		name: "invalid snapshot_uuid: path escape",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.Snapshot.SnapshotUuid = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot", "snapshot_uuid"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing generation",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.Snapshot.Generation = 0 }),
		want: field.ErrorList{field.Required(field.NewPath("snapshot", "generation"), "")},
	}, {
		name: "negative generation",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.Snapshot.Generation = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot", "generation"), nil, "").WithOrigin("minimum")},
	}, {
		name: "missing snapshot_uri",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.Snapshot.Object.SnapshotUri = "" }),
		want: field.ErrorList{field.Required(field.NewPath("snapshot", "object", "snapshot_uri"), "")},
	}, {
		name: "invalid snapshot_uri: no bucket",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.Snapshot.Object.SnapshotUri = "relative/path" }),
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot", "object", "snapshot_uri"), nil, "")},
	}, {
		name: "unspecified fidelity",
		obj: valid(func(r *ateletpb.RestoreRequest) {
			r.Snapshot.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
		}),
		want: field.ErrorList{field.Required(field.NewPath("snapshot", "fidelity"), "")},
	}, {
		name: "unknown fidelity",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.Snapshot.Fidelity = ateletpb.SnapshotFidelity(4) }),
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot", "fidelity"), nil, "").WithOrigin("maximum")},
	}, {
		name: "rootfs fidelity is forwarded for the runtime to judge",
		obj: valid(func(r *ateletpb.RestoreRequest) {
			r.Snapshot.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
		}),
	}, {
		name: "negative cpu_milli",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.CpuMilli = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("cpu_milli"), nil, "").WithOrigin("minimum")},
	}, {
		name: "cpu_milli too large",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.CpuMilli = 1000000 }),
		want: field.ErrorList{field.Invalid(field.NewPath("cpu_milli"), nil, "").WithOrigin("maximum")},
	}, {
		name: "negative memory_bytes",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.MemoryBytes = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("memory_bytes"), nil, "").WithOrigin("minimum")},
	}, {
		name: "empty egress gateway address",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.EgressGateway = &ateletpb.EgressGateway{} }),
		want: field.ErrorList{field.Required(field.NewPath("egress_gateway", "address"), "")},
	}, {
		name: "missing sandbox_runtime",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.SandboxRuntime = nil }),
		want: field.ErrorList{field.Required(field.NewPath("sandbox_runtime"), "")},
	}, {
		name: "sandbox_runtime errors surface at their path",
		obj: valid(func(r *ateletpb.RestoreRequest) {
			r.SandboxRuntime.SandboxClass = "kata"
		}),
		want: field.ErrorList{field.NotSupported[string](field.NewPath("sandbox_runtime", "sandbox_class"), nil, nil)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateRestoreRequest(context.Background(), tt.obj), tt.want)
		})
	}
}

func TestValidatePromoteSnapshotRequest(t *testing.T) {
	valid := func(mutate ...func(*ateletpb.PromoteSnapshotRequest)) *ateletpb.PromoteSnapshotRequest {
		r := &ateletpb.PromoteSnapshotRequest{
			Atespace:              "team-a",
			ActorName:             "actor-1",
			ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
			ActorTemplateAtespace: "team-a",
			ActorTemplateName:     "tmpl-1",
			Snapshot: &ateletpb.Snapshot{
				SnapshotUuid: "pause-snap-1",
				Generation:   1,
				Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
				Object: &ateletpb.ObjectStorage{
					SnapshotUri: "gs://bucket/root/atespaces/team-a/actors/01234567-89ab-cdef-0123-456789abcdef/snapshots/snap-1",
				},
			},
		}
		for _, m := range mutate {
			m(r)
		}
		return r
	}

	tests := []struct {
		name string
		obj  *ateletpb.PromoteSnapshotRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "valid volumes fidelity",
		obj: valid(func(r *ateletpb.PromoteSnapshotRequest) {
			r.Snapshot.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
		}),
	}, {
		name: "missing atespace",
		obj:  valid(func(r *ateletpb.PromoteSnapshotRequest) { r.Atespace = "" }),
		want: field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		name: "invalid atespace: path escape",
		obj:  valid(func(r *ateletpb.PromoteSnapshotRequest) { r.Atespace = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "invalid actor_name: uppercase",
		obj:  valid(func(r *ateletpb.PromoteSnapshotRequest) { r.ActorName = "UPPER" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_uid",
		obj:  valid(func(r *ateletpb.PromoteSnapshotRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid actor_uid: not a UUID",
		obj:  valid(func(r *ateletpb.PromoteSnapshotRequest) { r.ActorUid = "actor-uid-1" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "invalid actor_template_atespace: slash",
		obj:  valid(func(r *ateletpb.PromoteSnapshotRequest) { r.ActorTemplateAtespace = "no/slashes" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_template_atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "unset template identity is allowed",
		obj: valid(func(r *ateletpb.PromoteSnapshotRequest) {
			r.ActorTemplateAtespace = ""
			r.ActorTemplateName = ""
		}),
	}, {
		name: "missing snapshot",
		obj:  valid(func(r *ateletpb.PromoteSnapshotRequest) { r.Snapshot = nil }),
		want: field.ErrorList{field.Required(field.NewPath("snapshot"), "")},
	}, {
		name: "missing snapshot_uuid",
		obj:  valid(func(r *ateletpb.PromoteSnapshotRequest) { r.Snapshot.SnapshotUuid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("snapshot", "snapshot_uuid"), "")},
	}, {
		name: "invalid snapshot_uuid: path escape",
		obj:  valid(func(r *ateletpb.PromoteSnapshotRequest) { r.Snapshot.SnapshotUuid = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot", "snapshot_uuid"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing generation",
		obj:  valid(func(r *ateletpb.PromoteSnapshotRequest) { r.Snapshot.Generation = 0 }),
		want: field.ErrorList{field.Required(field.NewPath("snapshot", "generation"), "")},
	}, {
		name: "negative generation",
		obj:  valid(func(r *ateletpb.PromoteSnapshotRequest) { r.Snapshot.Generation = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot", "generation"), nil, "").WithOrigin("minimum")},
	}, {
		name: "missing snapshot.object",
		obj:  valid(func(r *ateletpb.PromoteSnapshotRequest) { r.Snapshot.Object = nil }),
		want: field.ErrorList{field.Required(field.NewPath("snapshot", "object"), "")},
	}, {
		name: "missing snapshot_uri",
		obj:  valid(func(r *ateletpb.PromoteSnapshotRequest) { r.Snapshot.Object.SnapshotUri = "" }),
		want: field.ErrorList{field.Required(field.NewPath("snapshot", "object", "snapshot_uri"), "")},
	}, {
		name: "invalid snapshot_uri",
		obj:  valid(func(r *ateletpb.PromoteSnapshotRequest) { r.Snapshot.Object.SnapshotUri = "not-a-uri" }),
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot", "object", "snapshot_uri"), nil, "")},
	}, {
		name: "snapshot_uri too long",
		obj: valid(func(r *ateletpb.PromoteSnapshotRequest) {
			r.Snapshot.Object.SnapshotUri = "gs://bucket/" + strings.Repeat("p", 2048)
		}),
		want: field.ErrorList{
			field.TooLong(field.NewPath("snapshot", "object", "snapshot_uri"), nil, 2048).WithOrigin("maxLength"),
			field.Invalid(field.NewPath("snapshot", "object", "snapshot_uri"), nil, ""),
		},
	}, {
		name: "unspecified fidelity",
		obj: valid(func(r *ateletpb.PromoteSnapshotRequest) {
			r.Snapshot.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
		}),
		want: field.ErrorList{field.Required(field.NewPath("snapshot", "fidelity"), "")},
	}, {
		name: "unknown fidelity",
		obj: valid(func(r *ateletpb.PromoteSnapshotRequest) {
			r.Snapshot.Fidelity = 4
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot", "fidelity"), nil, "").WithOrigin("maximum")},
	}, {
		name: "rootfs fidelity is forwarded for the runtime to judge",
		obj: valid(func(r *ateletpb.PromoteSnapshotRequest) {
			r.Snapshot.Fidelity = ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
		}),
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidatePromoteSnapshotRequest(context.Background(), tt.obj), tt.want)
		})
	}
}

func TestValidateTerminateRequest(t *testing.T) {
	valid := func(mutate ...func(*ateletpb.TerminateRequest)) *ateletpb.TerminateRequest {
		r := &ateletpb.TerminateRequest{
			WorkerPodUid:          "0f9a3b1c-2d4e-5f60-7182-93a4b5c6d7e8",
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
		name: "unset worker_pod_uid is allowed: node-only cleanup",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.WorkerPodUid = "" }),
	}, {
		name: "invalid worker_pod_uid: path escape",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.WorkerPodUid = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("worker_pod_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "invalid worker_pod_uid: not a pod UID",
		obj:  valid(func(r *ateletpb.TerminateRequest) { r.WorkerPodUid = "worker-1" }),
		want: field.ErrorList{field.Invalid(field.NewPath("worker_pod_uid"), nil, "").WithOrigin("format=k8s-uuid")},
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
			r.Spec = &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "_pause", Image: testDigestImage}}}
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("spec", "containers").Index(0).Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
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

func TestValidateDetachActorRequest(t *testing.T) {
	valid := func(mutate ...func(*ateletpb.DetachActorRequest)) *ateletpb.DetachActorRequest {
		r := &ateletpb.DetachActorRequest{
			Atespace:  "team-a",
			ActorName: "actor-1",
			ActorUid:  "01234567-89ab-cdef-0123-456789abcdef",
		}
		for _, m := range mutate {
			m(r)
		}
		return r
	}

	tests := []struct {
		name string
		obj  *ateletpb.DetachActorRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "missing atespace",
		obj:  valid(func(r *ateletpb.DetachActorRequest) { r.Atespace = "" }),
		want: field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		name: "invalid atespace: uppercase",
		obj:  valid(func(r *ateletpb.DetachActorRequest) { r.Atespace = "Team-A" }),
		want: field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_name",
		obj:  valid(func(r *ateletpb.DetachActorRequest) { r.ActorName = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_name"), "")},
	}, {
		name: "invalid actor_name: path escape",
		obj:  valid(func(r *ateletpb.DetachActorRequest) { r.ActorName = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_uid",
		obj:  valid(func(r *ateletpb.DetachActorRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid actor_uid: not a uuid",
		obj:  valid(func(r *ateletpb.DetachActorRequest) { r.ActorUid = "not-a-uuid" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateDetachActorRequest(context.Background(), tt.obj), tt.want)
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
		want: field.ErrorList{
			field.TooLong(defaultRuntimePath.Child("sandbox_class"), "", 63).WithOrigin("maxLength"),
			field.NotSupported[string](defaultRuntimePath.Child("sandbox_class"), nil, nil),
		},
	}, {
		name: "unsupported default_runtime.sandbox_class",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity: &ateletpb.WorkerResources{},
			DefaultRuntime: &ateletpb.SandboxRuntime{
				SandboxClass: "kata",
				Version:      testRuntime().Version,
			},
		},
		want: field.ErrorList{field.NotSupported[string](defaultRuntimePath.Child("sandbox_class"), nil, nil)},
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
		name: "unsupported restorable_runtimes sandbox_class",
		obj: &ateletpb.RegisterWorkerRequest{
			Capacity:       &ateletpb.WorkerResources{},
			DefaultRuntime: testRuntime(),
			RestorableRuntimes: []*ateletpb.SandboxRuntime{{
				SandboxClass: "GVISOR",
				Version:      testRuntime().Version,
			}},
		},
		want: field.ErrorList{field.NotSupported[string](restorablePath.Index(0).Child("sandbox_class"), nil, nil)},
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
		obj:  withLimits(&ateletpb.Limits{Name: "memory", Quantity: "1." + strings.Repeat("0", 31)}),
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

// TestValidateWorkloadSpec covers the rules WorkloadSpec owns: the volumes and
// containers lists. One nested case per list proves the element validators
// run; their own rules are covered by TestValidateVolume and
// TestValidateContainer.
func TestValidateWorkloadSpec(t *testing.T) {
	ctr := func(name string) *ateletpb.Container {
		return &ateletpb.Container{Name: name, Image: testDigestImage}
	}
	valid := func(mutate ...func(*ateletpb.WorkloadSpec)) *ateletpb.WorkloadSpec {
		s := &ateletpb.WorkloadSpec{
			Volumes:    []*ateletpb.Volume{{Name: "data", DurableDir: &ateletpb.DurableDirVolumeSource{}}},
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
		name: "no volumes: the delete flow may send containers only",
		obj:  valid(func(s *ateletpb.WorkloadSpec) { s.Volumes = nil }),
	}, {
		name: "duplicate volume names",
		obj: valid(func(s *ateletpb.WorkloadSpec) {
			s.Volumes = append(s.Volumes, &ateletpb.Volume{Name: "data", Image: &ateletpb.ImageVolumeSource{Reference: testDigestImage}})
		}),
		want: field.ErrorList{field.Duplicate(field.NewPath("volumes").Index(1), nil)},
	}, {
		name: "too many volumes",
		obj: valid(func(s *ateletpb.WorkloadSpec) {
			s.Volumes = nil
			for i := range 33 {
				s.Volumes = append(s.Volumes, &ateletpb.Volume{Name: fmt.Sprintf("v%d", i), DurableDir: &ateletpb.DurableDirVolumeSource{}})
			}
		}),
		want: field.ErrorList{field.TooMany(field.NewPath("volumes"), 33, 32).WithOrigin("maxItems")},
	}, {
		name: "volume errors surface at the volume's path",
		obj:  valid(func(s *ateletpb.WorkloadSpec) { s.Volumes[0].DurableDir = nil }),
		want: field.ErrorList{field.Invalid(field.NewPath("volumes").Index(0), nil, "").WithOrigin("union")},
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
	}, {
		name: "mount references a declared volume",
		obj: valid(func(s *ateletpb.WorkloadSpec) {
			s.Containers[0].VolumeMounts = []*ateletpb.VolumeMount{{Name: "data", MountPath: "/data"}}
		}),
	}, {
		name: "mount references an undeclared volume",
		obj: valid(func(s *ateletpb.WorkloadSpec) {
			s.Containers[1].VolumeMounts = []*ateletpb.VolumeMount{{Name: "data", MountPath: "/data"}, {Name: "ghost", MountPath: "/ghost"}}
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(1).Child("volume_mounts").Index(1).Child("name"), nil, "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, Validate_WorkloadSpec(context.Background(), createOp, nil, tt.obj, nil), tt.want)
		})
	}
}

// TestValidateVolume covers Volume and every source it can hold, with errors
// asserted at their paths under the volume.
func TestValidateVolume(t *testing.T) {
	durable := func(mutate ...func(*ateletpb.Volume)) *ateletpb.Volume {
		v := &ateletpb.Volume{Name: "data", DurableDir: &ateletpb.DurableDirVolumeSource{}}
		for _, m := range mutate {
			m(v)
		}
		return v
	}
	external := func(mutate ...func(*ateletpb.ExternalVolumeSource)) *ateletpb.Volume {
		e := &ateletpb.ExternalVolumeSource{
			StorageVolumeId: "projects/p/zones/z/disks/vol-1",
			VolumeType:      "substrate.io/mock",
			VolumeContext:   map[string]string{"fsType": "ext4"},
		}
		for _, m := range mutate {
			m(e)
		}
		return &ateletpb.Volume{Name: "data", External: e}
	}
	image := func(ref string) *ateletpb.Volume {
		return &ateletpb.Volume{Name: "data", Image: &ateletpb.ImageVolumeSource{Reference: ref}}
	}
	systemInfo := func(ds ...*ateletpb.SystemInfoDataSource) *ateletpb.Volume {
		return &ateletpb.Volume{Name: "data", SystemInfo: &ateletpb.SystemInfoVolumeSource{DataSources: ds}}
	}
	bundle := func(name, path string) *ateletpb.SystemInfoDataSource {
		return &ateletpb.SystemInfoDataSource{TrustBundle: &ateletpb.TrustBundleDataSource{Names: []string{name}, Path: path}}
	}
	item := func(f ateletpb.ActorMetadataField, path string) *ateletpb.ActorMetadataItem {
		return &ateletpb.ActorMetadataItem{Field: f, Path: path}
	}
	metadata := func(items ...*ateletpb.ActorMetadataItem) *ateletpb.SystemInfoDataSource {
		return &ateletpb.SystemInfoDataSource{ActorMetadata: &ateletpb.ActorMetadataDataSource{Items: items}}
	}
	fieldName := ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME

	extPath := field.NewPath("external")
	dsPath := field.NewPath("system_info", "data_sources")
	bundlePath := dsPath.Index(0).Child("trust_bundle")
	itemsPath := dsPath.Index(0).Child("actor_metadata", "items")

	tests := []struct {
		name string
		obj  *ateletpb.Volume
		want field.ErrorList
	}{
		// Volume.
		{
			name: "valid durable dir",
			obj:  durable(),
		}, {
			name: "missing name",
			obj:  durable(func(v *ateletpb.Volume) { v.Name = "" }),
			want: field.ErrorList{field.Required(field.NewPath("name"), "")},
		}, {
			name: "invalid name: uppercase",
			obj:  durable(func(v *ateletpb.Volume) { v.Name = "Data" }),
			want: field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
		}, {
			name: "no source set",
			obj:  durable(func(v *ateletpb.Volume) { v.DurableDir = nil }),
			want: field.ErrorList{field.Invalid(nil, nil, "").WithOrigin("union")},
		}, {
			name: "two sources set",
			obj: durable(func(v *ateletpb.Volume) {
				v.External = &ateletpb.ExternalVolumeSource{StorageVolumeId: "vol-1"}
			}),
			want: field.ErrorList{field.Invalid(nil, nil, "").WithOrigin("union")},
		},

		// ExternalVolumeSource.
		{
			name: "valid external",
			obj:  external(),
		}, {
			name: "external: missing storage_volume_id",
			obj:  external(func(e *ateletpb.ExternalVolumeSource) { e.StorageVolumeId = "" }),
			want: field.ErrorList{field.Required(extPath.Child("storage_volume_id"), "")},
		}, {
			name: "external: storage_volume_id with a control character",
			obj:  external(func(e *ateletpb.ExternalVolumeSource) { e.StorageVolumeId = "vol\x01" }),
			want: field.ErrorList{field.Invalid(extPath.Child("storage_volume_id"), nil, "")},
		}, {
			name: "external: storage_volume_id too long",
			obj:  external(func(e *ateletpb.ExternalVolumeSource) { e.StorageVolumeId = strings.Repeat("x", 257) }),
			want: field.ErrorList{field.TooLong(extPath.Child("storage_volume_id"), nil, 256).WithOrigin("maxLength")},
		}, {
			name: "external: unset volume_type is allowed",
			obj:  external(func(e *ateletpb.ExternalVolumeSource) { e.VolumeType = "" }),
		}, {
			name: "external: invalid volume_type: uppercase",
			obj:  external(func(e *ateletpb.ExternalVolumeSource) { e.VolumeType = "substrate.io/Mock" }),
			want: field.ErrorList{field.Invalid(extPath.Child("volume_type"), nil, "")},
		}, {
			name: "external: volume_context with one long value, as a CSI driver may return",
			obj: external(func(e *ateletpb.ExternalVolumeSource) {
				e.VolumeContext = map[string]string{"k": strings.Repeat("v", 4000)}
			}),
		}, {
			name: "external: volume_context over the size bound",
			obj: external(func(e *ateletpb.ExternalVolumeSource) {
				e.VolumeContext = map[string]string{"k": strings.Repeat("v", 40*1024)}
			}),
			want: field.ErrorList{field.TooLong(extPath.Child("volume_context"), nil, 40*1024)},
		}, {
			name: "external: publish_context with many entries, as a CSI driver may return",
			obj: external(func(e *ateletpb.ExternalVolumeSource) {
				e.PublishContext = make(map[string]string)
				for i := range 100 {
					e.PublishContext[fmt.Sprintf("key-%03d", i)] = "v"
				}
			}),
		}, {
			name: "external: publish_context over the size bound",
			obj: external(func(e *ateletpb.ExternalVolumeSource) {
				e.PublishContext = map[string]string{"a": strings.Repeat("v", 20*1024), "b": strings.Repeat("v", 20*1024)}
			}),
			want: field.ErrorList{field.TooLong(extPath.Child("publish_context"), nil, 40*1024)},
		},

		// ImageVolumeSource.
		{
			name: "valid image",
			obj:  image(testDigestImage),
		}, {
			name: "image: missing reference",
			obj:  image(""),
			want: field.ErrorList{field.Required(field.NewPath("image", "reference"), "")},
		}, {
			name: "image: reference not pinned by digest",
			obj:  image("example.com/app:v1"),
			want: field.ErrorList{field.Invalid(field.NewPath("image", "reference"), nil, "")},
		},

		// SystemInfoVolumeSource and SystemInfoDataSource.
		{
			name: "valid system info",
			obj:  systemInfo(bundle("podcert", "a.pem"), bundle("podcert", "b.pem"), metadata(item(fieldName, "name"))),
		}, {
			name: "system info: data source with neither set",
			obj:  systemInfo(&ateletpb.SystemInfoDataSource{}),
			want: field.ErrorList{field.Invalid(dsPath.Index(0), nil, "").WithOrigin("union")},
		}, {
			name: "system info: data source with both set",
			obj: systemInfo(&ateletpb.SystemInfoDataSource{
				ActorMetadata: &ateletpb.ActorMetadataDataSource{Items: []*ateletpb.ActorMetadataItem{item(fieldName, "name")}},
				TrustBundle:   &ateletpb.TrustBundleDataSource{Names: []string{"podcert"}, Path: "p"},
			}),
			want: field.ErrorList{field.Invalid(dsPath.Index(0), nil, "").WithOrigin("union")},
		}, {
			name: "system info: duplicate path across trust bundles",
			obj:  systemInfo(bundle("podcert", "a.pem"), bundle("podcert", "a.pem")),
			want: field.ErrorList{field.Duplicate(dsPath.Index(1).Child("trust_bundle", "path"), nil)},
		}, {
			name: "system info: duplicate path between bundle and metadata item",
			obj:  systemInfo(bundle("podcert", "name"), metadata(item(fieldName, "name"))),
			want: field.ErrorList{field.Duplicate(dsPath.Index(1).Child("actor_metadata", "items").Index(0).Child("path"), nil)},
		}, {
			name: "system info: second actor_metadata entry",
			obj:  systemInfo(metadata(item(fieldName, "name")), metadata(item(fieldName, "name-2"))),
			want: field.ErrorList{field.Forbidden(dsPath.Index(1).Child("actor_metadata"), "")},
		}, {
			name: "system info: too many data sources",
			obj: systemInfo(
				bundle("podcert", "a"), bundle("podcert", "b"), bundle("podcert", "c"),
				bundle("podcert", "d"), bundle("podcert", "e"), bundle("podcert", "f"),
				bundle("podcert", "g"), bundle("podcert", "h"), bundle("podcert", "i"),
			),
			want: field.ErrorList{field.TooMany(dsPath, 9, 8).WithOrigin("maxItems")},
		},

		// TrustBundleDataSource.
		{
			name: "trust bundle: missing path",
			obj:  systemInfo(bundle("podcert", "")),
			want: field.ErrorList{field.Required(bundlePath.Child("path"), "")},
		}, {
			name: "trust bundle: absolute path",
			obj:  systemInfo(bundle("podcert", "/trust/bundle.pem")),
			want: field.ErrorList{field.Invalid(bundlePath.Child("path"), nil, "")},
		}, {
			name: "trust bundle: path too long",
			obj:  systemInfo(bundle("podcert", strings.Repeat("p", 256))),
			want: field.ErrorList{field.TooLong(bundlePath.Child("path"), nil, 255).WithOrigin("maxLength")},
		}, {
			name: "trust bundle: no names",
			obj:  systemInfo(&ateletpb.SystemInfoDataSource{TrustBundle: &ateletpb.TrustBundleDataSource{Path: "trust/bundle.pem"}}),
			want: field.ErrorList{field.Required(bundlePath.Child("names"), "")},
		}, {
			name: "trust bundle: several names",
			obj:  systemInfo(&ateletpb.SystemInfoDataSource{TrustBundle: &ateletpb.TrustBundleDataSource{Names: []string{"egress-mitm.ate.dev", "system-roots.ate.dev"}, Path: "trust/bundle.pem"}}),
		}, {
			name: "trust bundle: too many names",
			obj:  systemInfo(&ateletpb.SystemInfoDataSource{TrustBundle: &ateletpb.TrustBundleDataSource{Names: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}, Path: "trust/bundle.pem"}}),
			want: field.ErrorList{field.TooMany(bundlePath.Child("names"), 9, 8).WithOrigin("maxItems")},
		}, {
			name: "trust bundle: duplicate names",
			obj:  systemInfo(&ateletpb.SystemInfoDataSource{TrustBundle: &ateletpb.TrustBundleDataSource{Names: []string{"a", "a"}, Path: "trust/bundle.pem"}}),
			want: field.ErrorList{field.Duplicate(bundlePath.Child("names").Index(1), nil)},
		}, {
			name: "trust bundle: empty name",
			obj:  systemInfo(bundle("", "trust/bundle.pem")),
			want: field.ErrorList{field.TooShort(bundlePath.Child("names").Index(0), "", 1).WithOrigin("minLength")},
		}, {
			name: "trust bundle: name too long",
			obj:  systemInfo(bundle(strings.Repeat("n", 254), "trust/bundle.pem")),
			want: field.ErrorList{field.TooLong(bundlePath.Child("names").Index(0), nil, 253).WithOrigin("maxLength")},
		},

		// ActorMetadataDataSource.
		{
			name: "actor metadata: several fields",
			obj: systemInfo(metadata(
				item(fieldName, "name"),
				item(ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_UID, "ids/uid"),
			)),
		}, {
			name: "actor metadata: empty items",
			obj:  systemInfo(metadata()),
			want: field.ErrorList{field.Required(itemsPath, "")},
		}, {
			name: "actor metadata: same field projected twice",
			obj:  systemInfo(metadata(item(fieldName, "a"), item(fieldName, "b"))),
			want: field.ErrorList{field.Duplicate(itemsPath.Index(1), nil)},
		}, {
			name: "actor metadata: unspecified field",
			obj:  systemInfo(metadata(item(ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_UNSPECIFIED, "a"))),
			want: field.ErrorList{field.Required(itemsPath.Index(0).Child("field"), "")},
		}, {
			name: "actor metadata: field outside the enum",
			obj:  systemInfo(metadata(item(ateletpb.ActorMetadataField(4), "a"))),
			want: field.ErrorList{field.Invalid(itemsPath.Index(0).Child("field"), nil, "").WithOrigin("maximum")},
		}, {
			name: "actor metadata: escaping item path",
			obj:  systemInfo(metadata(item(fieldName, "../name"))),
			want: field.ErrorList{field.Invalid(itemsPath.Index(0).Child("path"), nil, "")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, Validate_Volume(context.Background(), createOp, nil, tt.obj, nil), tt.want)
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
			Env:     []*ateletpb.EnvVar{{Name: "PORT", Value: "8080"}},
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
	env := func(mutate func(*ateletpb.EnvVar)) *ateletpb.Container {
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
			HttpGet:        &ateletpb.HttpGetAction{Path: "/healthz", Port: 8080},
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
			name: "missing image",
			obj:  valid(func(c *ateletpb.Container) { c.Image = "" }),
			want: field.ErrorList{field.Required(field.NewPath("image"), "")},
		}, {
			name: "image not pinned by digest",
			obj:  valid(func(c *ateletpb.Container) { c.Image = "example.com/app:v1" }),
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

		// EnvVar.
		{
			name: "duplicate env names",
			obj: valid(func(c *ateletpb.Container) {
				c.Env = append(c.Env, &ateletpb.EnvVar{Name: "PORT", Value: "9"})
			}),
			want: field.ErrorList{field.Duplicate(field.NewPath("env").Index(1), nil)},
		}, {
			name: "env: missing name",
			obj:  env(func(e *ateletpb.EnvVar) { e.Name = "" }),
			want: field.ErrorList{field.Required(envPath.Child("name"), "")},
		}, {
			name: "env: name with equals sign",
			obj:  env(func(e *ateletpb.EnvVar) { e.Name = "A=B" }),
			want: field.ErrorList{field.Invalid(envPath.Child("name"), nil, "")},
		}, {
			name: "env: name too long",
			obj:  env(func(e *ateletpb.EnvVar) { e.Name = strings.Repeat("N", 257) }),
			want: field.ErrorList{field.TooLong(envPath.Child("name"), nil, 256).WithOrigin("maxLength")},
		}, {
			name: "env: empty value is allowed",
			obj:  env(func(e *ateletpb.EnvVar) { e.Value = "" }),
		}, {
			name: "env: value too long",
			obj:  env(func(e *ateletpb.EnvVar) { e.Value = strings.Repeat("v", 32769) }),
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
		}, {
			name: "mount: relative mount_path",
			obj:  mount("data", "var/data"),
			want: field.ErrorList{field.Invalid(mountPath.Child("mount_path"), nil, "")},
		},

		// WakeupProbe and HttpGetAction.
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
