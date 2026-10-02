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
	"github.com/agent-substrate/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const testDigestImage = "example.com/app@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

const testSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func validSandboxAssets(mutate ...func(*ateletpb.SandboxAssets)) *ateletpb.SandboxAssets {
	a := &ateletpb.SandboxAssets{
		SandboxClass: "gvisor",
		PauseImage:   testDigestImage,
		Assets: map[string]*ateletpb.ArchAssets{
			"amd64": {Files: map[string]*ateletpb.AssetFile{"gvisor": {Url: "gs://bucket/gvisor.tar.zstd", Sha256: testSHA256}}},
			"arm64": {Files: map[string]*ateletpb.AssetFile{"gvisor": {Url: "gs://bucket/gvisor.tar.zstd", Sha256: testSHA256}}},
		},
	}
	for _, m := range mutate {
		m(a)
	}
	return a
}

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

func TestValidateRunRequest(t *testing.T) {
	valid := func(mutate ...func(*ateletpb.RunRequest)) *ateletpb.RunRequest {
		r := &ateletpb.RunRequest{
			TargetAteomUid:        "0f9a3b1c-2d4e-5f60-7182-93a4b5c6d7e8",
			Atespace:              "team-a",
			ActorName:             "actor-1",
			ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
			ActorTemplateAtespace: "team-a",
			ActorTemplateName:     "tmpl-1",
			Spec:                  &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "worker", Image: testDigestImage}}},
			SandboxAssets:         validSandboxAssets(),
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
		name: "missing target_ateom_uid",
		obj:  valid(func(r *ateletpb.RunRequest) { r.TargetAteomUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("target_ateom_uid"), "")},
	}, {
		name: "invalid target_ateom_uid: path escape",
		obj:  valid(func(r *ateletpb.RunRequest) { r.TargetAteomUid = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("target_ateom_uid"), nil, "").WithOrigin("format=k8s-short-name")},
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
		name: "unset spec is allowed",
		obj:  valid(func(r *ateletpb.RunRequest) { r.Spec = nil }),
	}, {
		name: "missing sandbox_assets",
		obj:  valid(func(r *ateletpb.RunRequest) { r.SandboxAssets = nil }),
		want: field.ErrorList{field.Required(field.NewPath("sandbox_assets"), "")},
	}, {
		name: "sandbox asset errors surface at the sandbox_assets path",
		obj:  valid(func(r *ateletpb.RunRequest) { r.SandboxAssets.PauseImage = "" }),
		want: field.ErrorList{field.Required(field.NewPath("sandbox_assets", "pause_image"), "")},
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
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateRunRequest(context.Background(), tt.obj), tt.want)
		})
	}
}

// TestValidateSandboxAssets covers SandboxAssets and the asset files it holds.
func TestValidateSandboxAssets(t *testing.T) {
	file := func(url, sha string) *ateletpb.ArchAssets {
		return &ateletpb.ArchAssets{Files: map[string]*ateletpb.AssetFile{"gvisor": {Url: url, Sha256: sha}}}
	}
	amd64 := field.NewPath("assets").Key("amd64")
	amd64File := amd64.Child("files").Key("gvisor")

	tests := []struct {
		name string
		obj  *ateletpb.SandboxAssets
		want field.ErrorList
	}{{
		name: "valid gvisor",
		obj:  validSandboxAssets(),
	}, {
		name: "valid microvm",
		obj:  validSandboxAssets(func(a *ateletpb.SandboxAssets) { a.SandboxClass = "microvm" }),
	}, {
		name: "missing sandbox_class",
		obj:  validSandboxAssets(func(a *ateletpb.SandboxAssets) { a.SandboxClass = "" }),
		want: field.ErrorList{field.Required(field.NewPath("sandbox_class"), "")},
	}, {
		name: "unknown sandbox_class",
		obj:  validSandboxAssets(func(a *ateletpb.SandboxAssets) { a.SandboxClass = "kvm" }),
		want: field.ErrorList{field.NotSupported[string](field.NewPath("sandbox_class"), nil, nil)},
	}, {
		name: "sandbox_class is case-sensitive",
		obj:  validSandboxAssets(func(a *ateletpb.SandboxAssets) { a.SandboxClass = "GVISOR" }),
		want: field.ErrorList{field.NotSupported[string](field.NewPath("sandbox_class"), nil, nil)},
	}, {
		name: "missing pause_image",
		obj:  validSandboxAssets(func(a *ateletpb.SandboxAssets) { a.PauseImage = "" }),
		want: field.ErrorList{field.Required(field.NewPath("pause_image"), "")},
	}, {
		name: "pause_image not pinned by digest",
		obj:  validSandboxAssets(func(a *ateletpb.SandboxAssets) { a.PauseImage = "registry.k8s.io/pause:3.10.2" }),
		want: field.ErrorList{field.Invalid(field.NewPath("pause_image"), nil, "")},
	}, {
		name: "missing assets",
		obj:  validSandboxAssets(func(a *ateletpb.SandboxAssets) { a.Assets = nil }),
		want: field.ErrorList{field.Required(field.NewPath("assets"), "")},
	}, {
		name: "too many architectures",
		obj: validSandboxAssets(func(a *ateletpb.SandboxAssets) {
			for _, arch := range []string{"386", "arm", "loong64", "mips64", "ppc64le", "riscv64", "s390x"} {
				a.Assets[arch] = file("gs://bucket/gvisor.tar.zstd", testSHA256)
			}
		}),
		want: field.ErrorList{field.TooMany(field.NewPath("assets"), 9, 8).WithOrigin("maxProperties")},
	}, {
		name: "architecture with no files",
		obj:  validSandboxAssets(func(a *ateletpb.SandboxAssets) { a.Assets["amd64"] = &ateletpb.ArchAssets{} }),
		want: field.ErrorList{field.Required(amd64.Child("files"), "")},
	}, {
		name: "missing url",
		obj:  validSandboxAssets(func(a *ateletpb.SandboxAssets) { a.Assets["amd64"] = file("", testSHA256) }),
		want: field.ErrorList{field.Required(amd64File.Child("url"), "")},
	}, {
		name: "missing sha256",
		obj:  validSandboxAssets(func(a *ateletpb.SandboxAssets) { a.Assets["amd64"] = file("gs://bucket/gvisor.tar.zstd", "") }),
		want: field.ErrorList{field.Required(amd64File.Child("sha256"), "")},
	}, {
		name: "sha256 too short",
		obj:  validSandboxAssets(func(a *ateletpb.SandboxAssets) { a.Assets["amd64"] = file("gs://bucket/gvisor.tar.zstd", "deadbeef") }),
		want: field.ErrorList{field.Invalid(amd64File.Child("sha256"), nil, "")},
	}, {
		name: "sha256 with a path escape",
		obj: validSandboxAssets(func(a *ateletpb.SandboxAssets) {
			a.Assets["amd64"] = file("gs://bucket/gvisor.tar.zstd", "../"+testSHA256[3:])
		}),
		want: field.ErrorList{field.Invalid(amd64File.Child("sha256"), nil, "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, Validate_SandboxAssets(context.Background(), createOp, nil, tt.obj, nil), tt.want)
		})
	}
}

func TestValidateCheckpointRequest(t *testing.T) {
	const snapshotURI = "gs://bucket/root/atespaces/team-a/actors/01234567-89ab-cdef-0123-456789abcdef/snapshots/snap-1"
	valid := func(mutate ...func(*ateletpb.CheckpointRequest)) *ateletpb.CheckpointRequest {
		r := &ateletpb.CheckpointRequest{
			TargetAteomUid:        "0f9a3b1c-2d4e-5f60-7182-93a4b5c6d7e8",
			Atespace:              "team-a",
			ActorName:             "actor-1",
			ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
			ActorTemplateAtespace: "team-a",
			ActorTemplateName:     "tmpl-1",
			Spec:                  &ateletpb.WorkloadSpec{},
			Type:                  ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
			ExternalConfig:        &ateletpb.ExternalCheckpointConfiguration{SnapshotUri: snapshotURI},
			Scope:                 ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		}
		for _, m := range mutate {
			m(r)
		}
		return r
	}
	local := func(name string) func(*ateletpb.CheckpointRequest) {
		return func(r *ateletpb.CheckpointRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			r.ExternalConfig = nil
			r.LocalConfig = &ateletpb.LocalCheckpointConfiguration{SnapshotName: name}
		}
	}

	tests := []struct {
		name string
		obj  *ateletpb.CheckpointRequest
		want field.ErrorList
	}{{
		name: "valid external",
		obj:  valid(),
	}, {
		name: "valid local",
		obj:  valid(local("pause-snap-1")),
	}, {
		name: "valid data scope",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.Scope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA }),
	}, {
		name: "missing target_ateom_uid",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.TargetAteomUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("target_ateom_uid"), "")},
	}, {
		name: "invalid target_ateom_uid: path escape",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.TargetAteomUid = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("target_ateom_uid"), nil, "").WithOrigin("format=k8s-short-name")},
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
		name: "unspecified type",
		obj: valid(func(r *ateletpb.CheckpointRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_UNSPECIFIED
		}),
		want: field.ErrorList{field.Required(field.NewPath("type"), "")},
	}, {
		name: "unknown type",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.Type = ateletpb.CheckpointType(3) }),
		want: field.ErrorList{field.Invalid(field.NewPath("type"), nil, "").WithOrigin("maximum")},
	}, {
		name: "no config",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.ExternalConfig = nil }),
		want: field.ErrorList{
			field.Invalid(nil, nil, "").WithOrigin("union"),
			field.Required(field.NewPath("external_config"), ""),
		},
	}, {
		name: "both configs",
		obj: valid(func(r *ateletpb.CheckpointRequest) {
			r.LocalConfig = &ateletpb.LocalCheckpointConfiguration{SnapshotName: "pause-snap-1"}
		}),
		want: field.ErrorList{field.Invalid(nil, nil, "").WithOrigin("union")},
	}, {
		name: "local type with external config",
		obj: valid(func(r *ateletpb.CheckpointRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
		}),
		want: field.ErrorList{field.Required(field.NewPath("local_config"), "")},
	}, {
		name: "external type with local config",
		obj: valid(local("pause-snap-1"), func(r *ateletpb.CheckpointRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL
		}),
		want: field.ErrorList{field.Required(field.NewPath("external_config"), "")},
	}, {
		name: "missing local snapshot_name",
		obj:  valid(local("")),
		want: field.ErrorList{field.Required(field.NewPath("local_config", "snapshot_name"), "")},
	}, {
		name: "invalid local snapshot_name: path escape",
		obj:  valid(local("../escape")),
		want: field.ErrorList{field.Invalid(field.NewPath("local_config", "snapshot_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing external snapshot_uri",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.ExternalConfig.SnapshotUri = "" }),
		want: field.ErrorList{field.Required(field.NewPath("external_config", "snapshot_uri"), "")},
	}, {
		name: "invalid external snapshot_uri: no bucket",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.ExternalConfig.SnapshotUri = "relative/path" }),
		want: field.ErrorList{field.Invalid(field.NewPath("external_config", "snapshot_uri"), nil, "")},
	}, {
		name: "external snapshot_uri too long",
		obj: valid(func(r *ateletpb.CheckpointRequest) {
			r.ExternalConfig.SnapshotUri = "gs://bucket/" + strings.Repeat("p", 2048)
		}),
		want: field.ErrorList{
			field.TooLong(field.NewPath("external_config", "snapshot_uri"), nil, 2048).WithOrigin("maxLength"),
			field.Invalid(field.NewPath("external_config", "snapshot_uri"), nil, ""),
		},
	}, {
		name: "unspecified scope",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.Scope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_UNSPECIFIED }),
		want: field.ErrorList{field.Required(field.NewPath("scope"), "")},
	}, {
		name: "unknown scope",
		obj:  valid(func(r *ateletpb.CheckpointRequest) { r.Scope = 3 }),
		want: field.ErrorList{field.Invalid(field.NewPath("scope"), nil, "").WithOrigin("maximum")},
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
			TargetAteomUid:        "0f9a3b1c-2d4e-5f60-7182-93a4b5c6d7e8",
			Atespace:              "team-a",
			ActorName:             "actor-1",
			ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
			ActorTemplateAtespace: "team-a",
			ActorTemplateName:     "tmpl-1",
			Spec:                  &ateletpb.WorkloadSpec{},
			Type:                  ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
			ExternalConfig:        &ateletpb.ExternalRestoreConfiguration{SnapshotUri: snapshotURI},
			Scope:                 ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
			SandboxAssets:         validSandboxAssets(),
		}
		for _, m := range mutate {
			m(r)
		}
		return r
	}
	local := func(name string) func(*ateletpb.RestoreRequest) {
		return func(r *ateletpb.RestoreRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			r.ExternalConfig = nil
			r.LocalConfig = &ateletpb.LocalCheckpointConfiguration{SnapshotName: name}
		}
	}

	tests := []struct {
		name string
		obj  *ateletpb.RestoreRequest
		want field.ErrorList
	}{{
		name: "valid external",
		obj:  valid(),
	}, {
		name: "valid local",
		obj:  valid(local("pause-snap-1")),
	}, {
		name: "valid data scope",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.Scope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA }),
	}, {
		name: "valid size and egress",
		obj: valid(func(r *ateletpb.RestoreRequest) {
			r.CpuMilli = 999999
			r.MemoryBytes = 1 << 30
			r.EgressGateway = &ateletpb.EgressGateway{Address: "10.0.0.1:15001"}
		}),
	}, {
		name: "missing target_ateom_uid",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.TargetAteomUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("target_ateom_uid"), "")},
	}, {
		name: "invalid target_ateom_uid: path escape",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.TargetAteomUid = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("target_ateom_uid"), nil, "").WithOrigin("format=k8s-short-name")},
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
		name: "unspecified type",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_UNSPECIFIED }),
		want: field.ErrorList{field.Required(field.NewPath("type"), "")},
	}, {
		name: "unknown type",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.Type = ateletpb.CheckpointType(3) }),
		want: field.ErrorList{field.Invalid(field.NewPath("type"), nil, "").WithOrigin("maximum")},
	}, {
		name: "no config",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.ExternalConfig = nil }),
		want: field.ErrorList{
			field.Invalid(nil, nil, "").WithOrigin("union"),
			field.Required(field.NewPath("external_config"), ""),
		},
	}, {
		name: "both configs",
		obj: valid(func(r *ateletpb.RestoreRequest) {
			r.LocalConfig = &ateletpb.LocalCheckpointConfiguration{SnapshotName: "pause-snap-1"}
		}),
		want: field.ErrorList{field.Invalid(nil, nil, "").WithOrigin("union")},
	}, {
		name: "local type with external config",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL }),
		want: field.ErrorList{field.Required(field.NewPath("local_config"), "")},
	}, {
		name: "external type with local config",
		obj: valid(local("pause-snap-1"), func(r *ateletpb.RestoreRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL
		}),
		want: field.ErrorList{field.Required(field.NewPath("external_config"), "")},
	}, {
		name: "invalid local snapshot_name: path escape",
		obj:  valid(local("../escape")),
		want: field.ErrorList{field.Invalid(field.NewPath("local_config", "snapshot_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing external snapshot_uri",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.ExternalConfig.SnapshotUri = "" }),
		want: field.ErrorList{field.Required(field.NewPath("external_config", "snapshot_uri"), "")},
	}, {
		name: "invalid external snapshot_uri: no bucket",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.ExternalConfig.SnapshotUri = "relative/path" }),
		want: field.ErrorList{field.Invalid(field.NewPath("external_config", "snapshot_uri"), nil, "")},
	}, {
		name: "unspecified scope",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.Scope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_UNSPECIFIED }),
		want: field.ErrorList{field.Required(field.NewPath("scope"), "")},
	}, {
		name: "unknown scope",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.Scope = ateletpb.SnapshotScope(3) }),
		want: field.ErrorList{field.Invalid(field.NewPath("scope"), nil, "").WithOrigin("maximum")},
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
		name: "missing sandbox_assets",
		obj:  valid(func(r *ateletpb.RestoreRequest) { r.SandboxAssets = nil }),
		want: field.ErrorList{field.Required(field.NewPath("sandbox_assets"), "")},
	}, {
		name: "sandbox_assets errors surface at their path",
		obj: valid(func(r *ateletpb.RestoreRequest) {
			r.SandboxAssets = validSandboxAssets(func(a *ateletpb.SandboxAssets) { a.SandboxClass = "kata" })
		}),
		want: field.ErrorList{field.NotSupported[string](field.NewPath("sandbox_assets", "sandbox_class"), nil, nil)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateRestoreRequest(context.Background(), tt.obj), tt.want)
		})
	}
}

func TestValidateUploadPausedCheckpointRequest(t *testing.T) {
	valid := func(mutate ...func(*ateletpb.UploadPausedCheckpointRequest)) *ateletpb.UploadPausedCheckpointRequest {
		r := &ateletpb.UploadPausedCheckpointRequest{
			Atespace:               "team-a",
			ActorName:              "actor-1",
			ActorUid:               "01234567-89ab-cdef-0123-456789abcdef",
			ActorTemplateAtespace:  "team-a",
			ActorTemplateName:      "tmpl-1",
			LocalSnapshotName:      "pause-snap-1",
			DestinationSnapshotUri: "gs://bucket/root/atespaces/team-a/actors/01234567-89ab-cdef-0123-456789abcdef/snapshots/snap-1",
			DesiredScope:           ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		}
		for _, m := range mutate {
			m(r)
		}
		return r
	}

	tests := []struct {
		name string
		obj  *ateletpb.UploadPausedCheckpointRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "valid data scope",
		obj: valid(func(r *ateletpb.UploadPausedCheckpointRequest) {
			r.DesiredScope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA
		}),
	}, {
		name: "missing atespace",
		obj:  valid(func(r *ateletpb.UploadPausedCheckpointRequest) { r.Atespace = "" }),
		want: field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		name: "invalid atespace: path escape",
		obj:  valid(func(r *ateletpb.UploadPausedCheckpointRequest) { r.Atespace = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "golden atespace",
		obj:  valid(func(r *ateletpb.UploadPausedCheckpointRequest) { r.Atespace = resources.GoldenActorAtespace }),
		want: field.ErrorList{field.Forbidden(field.NewPath("atespace"), "")},
	}, {
		name: "invalid actor_name: uppercase",
		obj:  valid(func(r *ateletpb.UploadPausedCheckpointRequest) { r.ActorName = "UPPER" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_uid",
		obj:  valid(func(r *ateletpb.UploadPausedCheckpointRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid actor_uid: not a UUID",
		obj:  valid(func(r *ateletpb.UploadPausedCheckpointRequest) { r.ActorUid = "actor-uid-1" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "invalid actor_template_atespace: slash",
		obj:  valid(func(r *ateletpb.UploadPausedCheckpointRequest) { r.ActorTemplateAtespace = "no/slashes" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_template_atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "unset template identity is allowed",
		obj: valid(func(r *ateletpb.UploadPausedCheckpointRequest) {
			r.ActorTemplateAtespace = ""
			r.ActorTemplateName = ""
		}),
	}, {
		name: "missing local_snapshot_name",
		obj:  valid(func(r *ateletpb.UploadPausedCheckpointRequest) { r.LocalSnapshotName = "" }),
		want: field.ErrorList{field.Required(field.NewPath("local_snapshot_name"), "")},
	}, {
		name: "invalid local_snapshot_name: path escape",
		obj:  valid(func(r *ateletpb.UploadPausedCheckpointRequest) { r.LocalSnapshotName = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("local_snapshot_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing destination_snapshot_uri",
		obj:  valid(func(r *ateletpb.UploadPausedCheckpointRequest) { r.DestinationSnapshotUri = "" }),
		want: field.ErrorList{field.Required(field.NewPath("destination_snapshot_uri"), "")},
	}, {
		name: "invalid destination_snapshot_uri",
		obj:  valid(func(r *ateletpb.UploadPausedCheckpointRequest) { r.DestinationSnapshotUri = "not-a-uri" }),
		want: field.ErrorList{field.Invalid(field.NewPath("destination_snapshot_uri"), nil, "")},
	}, {
		name: "destination_snapshot_uri too long",
		obj: valid(func(r *ateletpb.UploadPausedCheckpointRequest) {
			r.DestinationSnapshotUri = "gs://bucket/" + strings.Repeat("p", 2048)
		}),
		want: field.ErrorList{
			field.TooLong(field.NewPath("destination_snapshot_uri"), nil, 2048).WithOrigin("maxLength"),
			field.Invalid(field.NewPath("destination_snapshot_uri"), nil, ""),
		},
	}, {
		name: "unspecified desired_scope",
		obj: valid(func(r *ateletpb.UploadPausedCheckpointRequest) {
			r.DesiredScope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_UNSPECIFIED
		}),
		want: field.ErrorList{field.Required(field.NewPath("desired_scope"), "")},
	}, {
		name: "unknown desired_scope",
		obj: valid(func(r *ateletpb.UploadPausedCheckpointRequest) {
			r.DesiredScope = 3
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("desired_scope"), nil, "").WithOrigin("maximum")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateUploadPausedCheckpointRequest(context.Background(), tt.obj), tt.want)
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
			Volumes:    []*ateletpb.Volume{{Name: "data", DurableDir: &ateletpb.DurableDirVolume{}}},
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
				s.Volumes = append(s.Volumes, &ateletpb.Volume{Name: fmt.Sprintf("v%d", i), DurableDir: &ateletpb.DurableDirVolume{}})
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
		v := &ateletpb.Volume{Name: "data", DurableDir: &ateletpb.DurableDirVolume{}}
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
		return &ateletpb.Volume{Name: "data", SystemInfo: &ateletpb.SystemInfoVolume{DataSources: ds}}
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
			name: "external: volume_context key too long",
			obj: external(func(e *ateletpb.ExternalVolumeSource) {
				e.VolumeContext = map[string]string{strings.Repeat("k", 129): "v"}
			}),
			want: field.ErrorList{field.TooLong(extPath.Child("volume_context"), nil, 128).WithOrigin("maxLength")},
		}, {
			name: "external: volume_context value too long",
			obj: external(func(e *ateletpb.ExternalVolumeSource) {
				e.VolumeContext = map[string]string{"k": strings.Repeat("v", 257)}
			}),
			want: field.ErrorList{field.TooLong(extPath.Child("volume_context").Key("k"), nil, 256).WithOrigin("maxLength")},
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

		// SystemInfoVolume and SystemInfoDataSource.
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
			name: "trust bundle: too many names",
			obj:  systemInfo(&ateletpb.SystemInfoDataSource{TrustBundle: &ateletpb.TrustBundleDataSource{Names: []string{"a", "b"}, Path: "trust/bundle.pem"}}),
			want: field.ErrorList{field.TooMany(bundlePath.Child("names"), 2, 1).WithOrigin("maxItems")},
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
