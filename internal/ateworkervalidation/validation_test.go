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

package ateworkervalidation

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"google.golang.org/grpc/codes"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Every request is validated down to its leaves: identity, paths, the spec's
// containers, their probes and mounts, and the egress gateway.

// maxContainers is the +k8s:maxItems bound on WorkloadSpec.containers.
const maxContainers = 10

func fullSpec() *ateworkerpb.WorkloadSpec {
	return &ateworkerpb.WorkloadSpec{Containers: []*ateworkerpb.Container{{
		Name:                   "main",
		WakeupProbe:            &ateworkerpb.WakeupProbe{HttpGet: &ateworkerpb.HttpGetAction{Path: "/healthz", Port: 8080}, TimeoutSeconds: 30},
		DurableDirVolumeMounts: []*ateworkerpb.VolumeMount{{Name: "data", MountPath: "/data"}},
		ExternalVolumeMounts:   []*ateworkerpb.VolumeMount{{Name: "ext", MountPath: "/ext"}},
		SystemInfoVolumeMounts: []*ateworkerpb.VolumeMount{{Name: "sysinfo", MountPath: "/etc/sysinfo"}},
		ImageVolumeMounts:      []*ateworkerpb.VolumeMount{{Name: "img", MountPath: "/img"}},
	}}}
}

func fullActorDirs() *ateworkerpb.ActorDirs {
	return &ateworkerpb.ActorDirs{
		RootDir:                   "/actors/uid",
		OciBundleDir:              "/actors/uid/bundles",
		CheckpointDir:             "/actors/uid/checkpoint",
		RestoreDir:                "/actors/uid/restore",
		DurableDirVolumeMountsDir: "/actors/uid/durable",
		SystemInfoVolumeRootsDir:  "/actors/uid/sysinfo",
		ExternalVolumeMountsDir:   "/actors/uid/volumes",
		SandboxAssetsDir:          "/actors/uid/sandbox-assets",
	}
}

func validRunWorkloadRequest(mutate ...func(*ateworkerpb.RunWorkloadRequest)) *ateworkerpb.RunWorkloadRequest {
	r := &ateworkerpb.RunWorkloadRequest{
		Atespace:              "team-a",
		ActorName:             "actor-1",
		ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
		ActorTemplateAtespace: "team-a",
		ActorTemplateName:     "template-1",
		RunscPath:             "/opt/runsc",
		Spec:                  fullSpec(),
		RuntimeAssetPaths:     map[string]string{"kata-kernel": "/opt/kernel", "virtiofsd": "/opt/virtiofsd"},
		EgressGateway:         &ateworkerpb.EgressGateway{Address: "gateway:443"},
		Resources:             &ateworkerpb.ResourceRequirements{Limits: &ateworkerpb.Resources{CpuMillicores: 1500, MemoryBytes: 1 << 30}},
		ActorDirs:             fullActorDirs(),
	}
	for _, m := range mutate {
		m(r)
	}
	return r
}

func TestValidateRunWorkloadRequest(t *testing.T) {
	valid := validRunWorkloadRequest
	assets := field.NewPath("runtime_asset_paths")
	probe := field.NewPath("spec", "containers").Index(0).Child("wakeup_probe")

	tests := []struct {
		name string
		obj  *ateworkerpb.RunWorkloadRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "valid: gVisor shape, no assets, no egress, unset size",
		obj: valid(func(r *ateworkerpb.RunWorkloadRequest) {
			r.RuntimeAssetPaths, r.EgressGateway, r.Resources = nil, nil, nil
		}),
	}, {
		name: "missing atespace",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.Atespace = "" }),
		want: field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		name: "invalid atespace: uppercase",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.Atespace = "Team-A" }),
		want: field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_name",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.ActorName = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_name"), "")},
	}, {
		name: "missing actor_uid",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid actor_uid: not a uuid",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.ActorUid = "uid-a" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "invalid actor_template_name: underscore",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.ActorTemplateName = "template_1" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_template_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "runsc_path too long",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.RunscPath = "/" + strings.Repeat("x", 4096) }),
		want: field.ErrorList{field.TooLong(field.NewPath("runsc_path"), nil, 4096).WithOrigin("maxLength")},
	}, {
		name: "missing spec",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.Spec = nil }),
		want: field.ErrorList{field.Required(field.NewPath("spec"), "")},
	}, {
		name: "too many runtime_asset_paths",
		obj: valid(func(r *ateworkerpb.RunWorkloadRequest) {
			r.RuntimeAssetPaths = map[string]string{}
			for i := range 17 {
				r.RuntimeAssetPaths[fmt.Sprintf("asset-%d", i)] = "/opt/asset"
			}
		}),
		want: field.ErrorList{field.TooMany(assets, 17, 16).WithOrigin("maxProperties")},
	}, {
		name: "runtime asset name too long",
		obj: valid(func(r *ateworkerpb.RunWorkloadRequest) {
			r.RuntimeAssetPaths = map[string]string{strings.Repeat("k", 65): "/opt/asset"}
		}),
		want: field.ErrorList{field.TooLong(assets, nil, 64).WithOrigin("maxLength")}, // keys are reported at the map
	}, {
		name: "runtime asset path too long",
		obj: valid(func(r *ateworkerpb.RunWorkloadRequest) {
			r.RuntimeAssetPaths = map[string]string{"kata-kernel": "/" + strings.Repeat("x", 4096)}
		}),
		want: field.ErrorList{field.TooLong(assets.Key("kata-kernel"), nil, 4096).WithOrigin("maxLength")},
	}, {
		name: "negative cpu_millicores",
		obj: valid(func(r *ateworkerpb.RunWorkloadRequest) {
			r.Resources = &ateworkerpb.ResourceRequirements{Limits: &ateworkerpb.Resources{CpuMillicores: -1}}
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("resources", "limits", "cpu_millicores"), nil, "").WithOrigin("minimum")},
	}, {
		name: "cpu_millicores past atelet's bound",
		obj: valid(func(r *ateworkerpb.RunWorkloadRequest) {
			r.Resources = &ateworkerpb.ResourceRequirements{Limits: &ateworkerpb.Resources{CpuMillicores: 1000000}}
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("resources", "limits", "cpu_millicores"), nil, "").WithOrigin("maximum")},
	}, {
		name: "negative memory_bytes",
		obj: valid(func(r *ateworkerpb.RunWorkloadRequest) {
			r.Resources = &ateworkerpb.ResourceRequirements{Limits: &ateworkerpb.Resources{MemoryBytes: -1}}
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("resources", "limits", "memory_bytes"), nil, "").WithOrigin("minimum")},
	}, {
		name: "empty egress_gateway",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.EgressGateway = &ateworkerpb.EgressGateway{} }),
		want: field.ErrorList{field.Required(field.NewPath("egress_gateway", "address"), "")},
	}, {
		name: "egress_gateway address without port",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.EgressGateway.Address = "gateway" }),
		want: field.ErrorList{field.Invalid(field.NewPath("egress_gateway", "address"), nil, "")},
	}, {
		name: "egress_gateway address too long",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.EgressGateway.Address = strings.Repeat("h", 258) + ":443" }),
		want: field.ErrorList{
			field.TooLong(field.NewPath("egress_gateway", "address"), nil, 261).WithOrigin("maxLength"),
			field.Invalid(field.NewPath("egress_gateway", "address"), nil, ""),
		},
	}, {
		name: "missing actor_dirs",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.ActorDirs = nil }),
		want: field.ErrorList{field.Required(field.NewPath("actor_dirs"), "")},
	}, {
		name: "relative oci_bundle_dir",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.ActorDirs.OciBundleDir = "bundles" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_dirs", "oci_bundle_dir"), nil, "")},
	}, {
		name: "spec without containers",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.Spec = &ateworkerpb.WorkloadSpec{} }),
		want: field.ErrorList{field.Required(field.NewPath("spec", "containers"), "")},
	}, {
		name: "bad container name",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.Spec.Containers[0].Name = "Main" }),
		want: field.ErrorList{field.Invalid(field.NewPath("spec", "containers").Index(0).Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "valid: no wakeup probe",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.Spec.Containers[0].WakeupProbe = nil }),
	}, {
		name: "empty wakeup probe",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.Spec.Containers[0].WakeupProbe = &ateworkerpb.WakeupProbe{} }),
		want: field.ErrorList{
			field.Required(probe.Child("http_get"), ""),
			field.Required(probe.Child("timeout_seconds"), ""),
		},
	}, {
		name: "wakeup probe timeout too long",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.Spec.Containers[0].WakeupProbe.TimeoutSeconds = 3601 }),
		want: field.ErrorList{field.Invalid(probe.Child("timeout_seconds"), nil, "").WithOrigin("maximum")},
	}, {
		name: "empty http_get",
		obj: valid(func(r *ateworkerpb.RunWorkloadRequest) {
			r.Spec.Containers[0].WakeupProbe.HttpGet = &ateworkerpb.HttpGetAction{}
		}),
		want: field.ErrorList{
			field.Required(probe.Child("http_get", "path"), ""),
			field.Required(probe.Child("http_get", "port"), ""),
		},
	}, {
		name: "http_get path without leading slash",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.Spec.Containers[0].WakeupProbe.HttpGet.Path = "healthz" }),
		want: field.ErrorList{field.Invalid(probe.Child("http_get", "path"), nil, "")},
	}, {
		name: "http_get path with query",
		obj: valid(func(r *ateworkerpb.RunWorkloadRequest) {
			r.Spec.Containers[0].WakeupProbe.HttpGet.Path = "/healthz?x=1"
		}),
		want: field.ErrorList{field.Invalid(probe.Child("http_get", "path"), nil, "")},
	}, {
		name: "http_get path too long",
		obj: valid(func(r *ateworkerpb.RunWorkloadRequest) {
			r.Spec.Containers[0].WakeupProbe.HttpGet.Path = "/" + strings.Repeat("x", 1024)
		}),
		want: field.ErrorList{field.TooLong(probe.Child("http_get", "path"), nil, 1024).WithOrigin("maxLength")},
	}, {
		name: "http_get port past the range",
		obj:  valid(func(r *ateworkerpb.RunWorkloadRequest) { r.Spec.Containers[0].WakeupProbe.HttpGet.Port = 65536 }),
		want: field.ErrorList{field.Invalid(probe.Child("http_get", "port"), nil, "").WithOrigin("maximum")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_RunWorkloadRequest(context.Background(), createOp(), nil, tt.obj, nil))
		})
	}
}

// TestValidateRunWorkloadRequestEdge covers the handler-facing wrapper: valid
// passes, invalid comes back as InvalidArgument.
func TestValidateRunWorkloadRequestEdge(t *testing.T) {
	if err := ValidateRunWorkloadRequest(context.Background(), validRunWorkloadRequest()); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	err := ValidateRunWorkloadRequest(context.Background(), &ateworkerpb.RunWorkloadRequest{})
	if apierror.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty request error = %v, want InvalidArgument", err)
	}
}

func validCheckpointWorkloadRequest(mutate ...func(*ateworkerpb.CheckpointWorkloadRequest)) *ateworkerpb.CheckpointWorkloadRequest {
	r := &ateworkerpb.CheckpointWorkloadRequest{
		Atespace:              "team-a",
		ActorName:             "actor-1",
		ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
		ActorTemplateAtespace: "team-a",
		ActorTemplateName:     "template-1",
		RunscPath:             "/opt/runsc",
		Spec:                  fullSpec(),
		Fidelity:              ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		ActorDirs:             fullActorDirs(),
	}
	for _, m := range mutate {
		m(r)
	}
	return r
}

// Checkpoint shares its identity fields with Run, which the Run table
// covers; this table holds the shared gates at one case each and covers what
// only Checkpoint has: fidelity and actor_dirs.
func TestValidateCheckpointWorkloadRequest(t *testing.T) {
	valid := validCheckpointWorkloadRequest

	tests := []struct {
		name string
		obj  *ateworkerpb.CheckpointWorkloadRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "valid: volumes fidelity",
		obj: valid(func(r *ateworkerpb.CheckpointWorkloadRequest) {
			r.Fidelity = ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
		}),
	}, {
		name: "valid: rootfs fidelity, which runtimes judge for themselves",
		obj: valid(func(r *ateworkerpb.CheckpointWorkloadRequest) {
			r.Fidelity = ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
		}),
	}, {
		name: "missing actor_uid",
		obj:  valid(func(r *ateworkerpb.CheckpointWorkloadRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid actor_name: trailing dash",
		obj:  valid(func(r *ateworkerpb.CheckpointWorkloadRequest) { r.ActorName = "actor-" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "runsc_path too long",
		obj:  valid(func(r *ateworkerpb.CheckpointWorkloadRequest) { r.RunscPath = "/" + strings.Repeat("x", 4096) }),
		want: field.ErrorList{field.TooLong(field.NewPath("runsc_path"), nil, 4096).WithOrigin("maxLength")},
	}, {
		name: "missing spec",
		obj:  valid(func(r *ateworkerpb.CheckpointWorkloadRequest) { r.Spec = nil }),
		want: field.ErrorList{field.Required(field.NewPath("spec"), "")},
	}, {
		name: "missing fidelity",
		obj: valid(func(r *ateworkerpb.CheckpointWorkloadRequest) {
			r.Fidelity = ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
		}),
		want: field.ErrorList{field.Required(field.NewPath("fidelity"), "")},
	}, {
		name: "fidelity past the enum",
		obj:  valid(func(r *ateworkerpb.CheckpointWorkloadRequest) { r.Fidelity = ateworkerpb.SnapshotFidelity(4) }),
		want: field.ErrorList{field.Invalid(field.NewPath("fidelity"), nil, "").WithOrigin("maximum")},
	}, {
		name: "missing actor_dirs",
		obj:  valid(func(r *ateworkerpb.CheckpointWorkloadRequest) { r.ActorDirs = nil }),
		want: field.ErrorList{field.Required(field.NewPath("actor_dirs"), "")},
	}, {
		name: "relative checkpoint_dir",
		obj:  valid(func(r *ateworkerpb.CheckpointWorkloadRequest) { r.ActorDirs.CheckpointDir = "checkpoint" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_dirs", "checkpoint_dir"), nil, "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_CheckpointWorkloadRequest(context.Background(), createOp(), nil, tt.obj, nil))
		})
	}
}

// TestValidateCheckpointWorkloadRequestEdge covers the handler-facing
// wrapper: valid passes, invalid comes back as InvalidArgument.
func TestValidateCheckpointWorkloadRequestEdge(t *testing.T) {
	if err := ValidateCheckpointWorkloadRequest(context.Background(), validCheckpointWorkloadRequest()); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	err := ValidateCheckpointWorkloadRequest(context.Background(), &ateworkerpb.CheckpointWorkloadRequest{})
	if apierror.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty request error = %v, want InvalidArgument", err)
	}
}

// WorkloadSpec is reached from all four lifecycle requests; the Run table
// carries one case to show the path prefix, and this table holds the rules.
func TestValidateWorkloadSpec(t *testing.T) {
	container := func(name string) *ateworkerpb.Container { return &ateworkerpb.Container{Name: name} }
	tests := []struct {
		name string
		obj  *ateworkerpb.WorkloadSpec
		want field.ErrorList
	}{{
		name: "valid",
		obj:  fullSpec(),
	}, {
		name: "valid: at the containers bound",
		obj: func() *ateworkerpb.WorkloadSpec {
			spec := &ateworkerpb.WorkloadSpec{}
			for i := range maxContainers {
				spec.Containers = append(spec.Containers, container(fmt.Sprintf("c-%d", i)))
			}
			return spec
		}(),
	}, {
		name: "no containers: Terminate's fallback spec for a template-less actor",
		obj:  &ateworkerpb.WorkloadSpec{},
	}, {
		name: "over the containers bound",
		obj: func() *ateworkerpb.WorkloadSpec {
			spec := &ateworkerpb.WorkloadSpec{}
			for i := range maxContainers + 1 {
				spec.Containers = append(spec.Containers, container(fmt.Sprintf("c-%d", i)))
			}
			return spec
		}(),
		want: field.ErrorList{field.TooMany(field.NewPath("containers"), maxContainers+1, maxContainers).WithOrigin("maxItems")},
	}, {
		name: "duplicate container name",
		obj:  &ateworkerpb.WorkloadSpec{Containers: []*ateworkerpb.Container{container("app"), container("app")}},
		want: field.ErrorList{field.Duplicate(field.NewPath("containers").Index(1), nil)},
	}, {
		name: "nil container",
		obj:  &ateworkerpb.WorkloadSpec{Containers: []*ateworkerpb.Container{nil}},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0), "")},
	}, {
		name: "bad name inside the list",
		obj:  &ateworkerpb.WorkloadSpec{Containers: []*ateworkerpb.Container{container("app"), container("Sidecar")}},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(1).Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_WorkloadSpec(context.Background(), createOp(), nil, tt.obj, nil))
		})
	}
}

// The list rules (no nil entries, at most 32, unique mount paths within a
// list) come from the tags; the items' own rules and the cross-list nesting
// and duplicate checks are covered below. The probe is covered from the Run
// table.
func TestValidateContainer(t *testing.T) {
	valid := func(mutate ...func(*ateworkerpb.Container)) *ateworkerpb.Container {
		c := fullSpec().Containers[0]
		for _, m := range mutate {
			m(c)
		}
		return c
	}
	tests := []struct {
		name string
		obj  *ateworkerpb.Container
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "valid: name only",
		obj:  &ateworkerpb.Container{Name: "app"},
	}, {
		name: "missing name",
		obj:  valid(func(c *ateworkerpb.Container) { c.Name = "" }),
		want: field.ErrorList{field.Required(field.NewPath("name"), "")},
	}, {
		name: "invalid name: the sandbox's own container",
		obj:  valid(func(c *ateworkerpb.Container) { c.Name = "_sandbox" }),
		want: field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "too many external mounts",
		obj: valid(func(c *ateworkerpb.Container) {
			c.ExternalVolumeMounts = nil
			for i := range 33 {
				c.ExternalVolumeMounts = append(c.ExternalVolumeMounts, &ateworkerpb.VolumeMount{Name: "v", MountPath: fmt.Sprintf("/m/%d", i)})
			}
		}),
		want: field.ErrorList{field.TooMany(field.NewPath("external_volume_mounts"), 33, 32).WithOrigin("maxItems")},
	}, {
		name: "duplicate durable-dir mount path",
		obj: valid(func(c *ateworkerpb.Container) {
			c.DurableDirVolumeMounts = []*ateworkerpb.VolumeMount{{Name: "a", MountPath: "/data"}, {Name: "b", MountPath: "/data"}}
		}),
		want: field.ErrorList{field.Duplicate(field.NewPath("durable_dir_volume_mounts").Index(1), nil)},
	}, {
		name: "nil image mount",
		obj:  valid(func(c *ateworkerpb.Container) { c.ImageVolumeMounts = []*ateworkerpb.VolumeMount{nil} }),
		want: field.ErrorList{field.Required(field.NewPath("image_volume_mounts").Index(0), "")},
	}, {
		name: "valid: no mounts",
		obj: valid(func(c *ateworkerpb.Container) {
			c.DurableDirVolumeMounts, c.ExternalVolumeMounts, c.SystemInfoVolumeMounts, c.ImageVolumeMounts = nil, nil, nil, nil
		}),
	}, {
		name: "empty system-info mount",
		obj:  valid(func(c *ateworkerpb.Container) { c.SystemInfoVolumeMounts = []*ateworkerpb.VolumeMount{{}} }),
		want: field.ErrorList{
			field.Required(field.NewPath("system_info_volume_mounts").Index(0).Child("name"), ""),
			field.Required(field.NewPath("system_info_volume_mounts").Index(0).Child("mount_path"), ""),
		},
	}, {
		name: "invalid external mount name: uppercase",
		obj:  valid(func(c *ateworkerpb.Container) { c.ExternalVolumeMounts[0].Name = "EXT" }),
		want: field.ErrorList{field.Invalid(field.NewPath("external_volume_mounts").Index(0).Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "relative durable-dir mount_path",
		obj:  valid(func(c *ateworkerpb.Container) { c.DurableDirVolumeMounts[0].MountPath = "data" }),
		want: field.ErrorList{field.Invalid(field.NewPath("durable_dir_volume_mounts").Index(0).Child("mount_path"), nil, "")},
	}, {
		name: "image mount_path with dot-dot",
		obj:  valid(func(c *ateworkerpb.Container) { c.ImageVolumeMounts[0].MountPath = "/img/../etc" }),
		want: field.ErrorList{field.Invalid(field.NewPath("image_volume_mounts").Index(0).Child("mount_path"), nil, "")},
	}, {
		name: "image mount_path too long",
		obj:  valid(func(c *ateworkerpb.Container) { c.ImageVolumeMounts[0].MountPath = "/" + strings.Repeat("x", 4096) }),
		want: field.ErrorList{field.TooLong(field.NewPath("image_volume_mounts").Index(0).Child("mount_path"), nil, 4096).WithOrigin("maxLength")},
	}, {
		name: "external mount nested under another in its list",
		obj: valid(func(c *ateworkerpb.Container) {
			c.ExternalVolumeMounts = append(c.ExternalVolumeMounts, &ateworkerpb.VolumeMount{Name: "ext-2", MountPath: "/ext/inner"})
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("external_volume_mounts").Index(1).Child("mount_path"), nil, "")},
	}, {
		name: "system-info mount nested under an external mount",
		obj: valid(func(c *ateworkerpb.Container) {
			c.SystemInfoVolumeMounts = append(c.SystemInfoVolumeMounts, &ateworkerpb.VolumeMount{Name: "sysinfo-2", MountPath: "/ext/inner"})
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("system_info_volume_mounts").Index(1).Child("mount_path"), nil, "")},
	}, {
		name: "image mount nested over a system-info mount",
		obj: valid(func(c *ateworkerpb.Container) {
			c.ImageVolumeMounts = append(c.ImageVolumeMounts, &ateworkerpb.VolumeMount{Name: "etc", MountPath: "/etc"})
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("image_volume_mounts").Index(1).Child("mount_path"), nil, "")},
	}, {
		name: "same mount path in two lists",
		obj: valid(func(c *ateworkerpb.Container) {
			c.ImageVolumeMounts = append(c.ImageVolumeMounts, &ateworkerpb.VolumeMount{Name: "data-again", MountPath: "/data"})
		}),
		want: field.ErrorList{field.Duplicate(field.NewPath("image_volume_mounts").Index(1).Child("mount_path"), nil)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_Container(context.Background(), createOp(), nil, tt.obj, nil))
		})
	}
}

// ActorDirs is reached from all four lifecycle requests; each request table
// carries one case to show the path prefix, and this table holds the rules.
func TestValidateActorDirs(t *testing.T) {
	valid := func(mutate ...func(*ateworkerpb.ActorDirs)) *ateworkerpb.ActorDirs {
		d := fullActorDirs()
		for _, m := range mutate {
			m(d)
		}
		return d
	}
	tests := []struct {
		name string
		obj  *ateworkerpb.ActorDirs
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "missing root_dir",
		obj:  valid(func(d *ateworkerpb.ActorDirs) { d.RootDir = "" }),
		want: field.ErrorList{field.Required(field.NewPath("root_dir"), "")},
	}, {
		name: "missing external_volume_mounts_dir",
		obj:  valid(func(d *ateworkerpb.ActorDirs) { d.ExternalVolumeMountsDir = "" }),
		want: field.ErrorList{field.Required(field.NewPath("external_volume_mounts_dir"), "")},
	}, {
		name: "missing sandbox_assets_dir",
		obj:  valid(func(d *ateworkerpb.ActorDirs) { d.SandboxAssetsDir = "" }),
		want: field.ErrorList{field.Required(field.NewPath("sandbox_assets_dir"), "")},
	}, {
		name: "relative sandbox_assets_dir",
		obj:  valid(func(d *ateworkerpb.ActorDirs) { d.SandboxAssetsDir = "sandbox-assets" }),
		want: field.ErrorList{field.Invalid(field.NewPath("sandbox_assets_dir"), nil, "")},
	}, {
		name: "relative oci_bundle_dir",
		obj:  valid(func(d *ateworkerpb.ActorDirs) { d.OciBundleDir = "bundles" }),
		want: field.ErrorList{field.Invalid(field.NewPath("oci_bundle_dir"), nil, "")},
	}, {
		name: "unclean restore_dir",
		obj:  valid(func(d *ateworkerpb.ActorDirs) { d.RestoreDir = "/actors/uid/../other/restore" }),
		want: field.ErrorList{field.Invalid(field.NewPath("restore_dir"), nil, "")},
	}, {
		name: "trailing slash on durable_dir_volume_mounts_dir",
		obj:  valid(func(d *ateworkerpb.ActorDirs) { d.DurableDirVolumeMountsDir = "/actors/uid/durable/" }),
		want: field.ErrorList{field.Invalid(field.NewPath("durable_dir_volume_mounts_dir"), nil, "")},
	}, {
		name: "system_info_volume_roots_dir too long",
		obj:  valid(func(d *ateworkerpb.ActorDirs) { d.SystemInfoVolumeRootsDir = "/" + strings.Repeat("x", 4096) }),
		want: field.ErrorList{field.TooLong(field.NewPath("system_info_volume_roots_dir"), nil, 4096).WithOrigin("maxLength")},
	}, {
		name: "two bad dirs are both reported",
		obj:  valid(func(d *ateworkerpb.ActorDirs) { d.RootDir, d.CheckpointDir = "", "checkpoint" }),
		want: field.ErrorList{
			field.Required(field.NewPath("root_dir"), ""),
			field.Invalid(field.NewPath("checkpoint_dir"), nil, ""),
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_ActorDirs(context.Background(), createOp(), nil, tt.obj, nil))
		})
	}
}

func validRestoreWorkloadRequest(mutate ...func(*ateworkerpb.RestoreWorkloadRequest)) *ateworkerpb.RestoreWorkloadRequest {
	r := &ateworkerpb.RestoreWorkloadRequest{
		Atespace:              "team-a",
		ActorName:             "actor-1",
		ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
		ActorTemplateAtespace: "team-a",
		ActorTemplateName:     "template-1",
		RunscPath:             "/opt/runsc",
		Spec:                  fullSpec(),
		RuntimeAssetPaths:     map[string]string{"kata-kernel": "/opt/kernel"},
		Fidelity:              ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
		EgressGateway:         &ateworkerpb.EgressGateway{Address: "gateway:443"},
		Resources:             &ateworkerpb.ResourceRequirements{Limits: &ateworkerpb.Resources{CpuMillicores: 1500, MemoryBytes: 1 << 30}},
		ActorDirs:             fullActorDirs(),
	}
	for _, m := range mutate {
		m(r)
	}
	return r
}

// Restore shares its identity, size, and asset fields with Run, which the
// Run table covers; this table holds the shared gates at one case each and
// covers what only Restore has.
func TestValidateRestoreWorkloadRequest(t *testing.T) {
	valid := validRestoreWorkloadRequest

	tests := []struct {
		name string
		obj  *ateworkerpb.RestoreWorkloadRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "valid: memory fidelity",
		obj: valid(func(r *ateworkerpb.RestoreWorkloadRequest) {
			r.Fidelity = ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
		}),
	}, {
		name: "valid: rootfs fidelity, which runtimes judge for themselves",
		obj: valid(func(r *ateworkerpb.RestoreWorkloadRequest) {
			r.Fidelity = ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
		}),
	}, {
		name: "missing actor_uid",
		obj:  valid(func(r *ateworkerpb.RestoreWorkloadRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid atespace: uppercase",
		obj:  valid(func(r *ateworkerpb.RestoreWorkloadRequest) { r.Atespace = "Team-A" }),
		want: field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing spec",
		obj:  valid(func(r *ateworkerpb.RestoreWorkloadRequest) { r.Spec = nil }),
		want: field.ErrorList{field.Required(field.NewPath("spec"), "")},
	}, {
		name: "spec without containers",
		obj:  valid(func(r *ateworkerpb.RestoreWorkloadRequest) { r.Spec = &ateworkerpb.WorkloadSpec{} }),
		want: field.ErrorList{field.Required(field.NewPath("spec", "containers"), "")},
	}, {
		name: "missing actor_dirs",
		obj:  valid(func(r *ateworkerpb.RestoreWorkloadRequest) { r.ActorDirs = nil }),
		want: field.ErrorList{field.Required(field.NewPath("actor_dirs"), "")},
	}, {
		name: "negative cpu_millicores",
		obj: valid(func(r *ateworkerpb.RestoreWorkloadRequest) {
			r.Resources = &ateworkerpb.ResourceRequirements{Limits: &ateworkerpb.Resources{CpuMillicores: -1}}
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("resources", "limits", "cpu_millicores"), nil, "").WithOrigin("minimum")},
	}, {
		name: "cpu_millicores past atelet's bound",
		obj: valid(func(r *ateworkerpb.RestoreWorkloadRequest) {
			r.Resources = &ateworkerpb.ResourceRequirements{Limits: &ateworkerpb.Resources{CpuMillicores: 1000000}}
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("resources", "limits", "cpu_millicores"), nil, "").WithOrigin("maximum")},
	}, {
		name: "runtime asset path too long",
		obj: valid(func(r *ateworkerpb.RestoreWorkloadRequest) {
			r.RuntimeAssetPaths = map[string]string{"kata-kernel": "/" + strings.Repeat("x", 4096)}
		}),
		want: field.ErrorList{field.TooLong(field.NewPath("runtime_asset_paths").Key("kata-kernel"), nil, 4096).WithOrigin("maxLength")},
	}, {
		name: "egress_gateway address without port",
		obj:  valid(func(r *ateworkerpb.RestoreWorkloadRequest) { r.EgressGateway.Address = "gateway" }),
		want: field.ErrorList{field.Invalid(field.NewPath("egress_gateway", "address"), nil, "")},
	}, {
		name: "missing fidelity",
		obj: valid(func(r *ateworkerpb.RestoreWorkloadRequest) {
			r.Fidelity = ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
		}),
		want: field.ErrorList{field.Required(field.NewPath("fidelity"), "")},
	}, {
		name: "fidelity past the enum",
		obj:  valid(func(r *ateworkerpb.RestoreWorkloadRequest) { r.Fidelity = ateworkerpb.SnapshotFidelity(4) }),
		want: field.ErrorList{field.Invalid(field.NewPath("fidelity"), nil, "").WithOrigin("maximum")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_RestoreWorkloadRequest(context.Background(), createOp(), nil, tt.obj, nil))
		})
	}
}

// TestValidateRestoreWorkloadRequestEdge covers the handler-facing wrapper:
// valid passes, invalid comes back as InvalidArgument.
func TestValidateRestoreWorkloadRequestEdge(t *testing.T) {
	if err := ValidateRestoreWorkloadRequest(context.Background(), validRestoreWorkloadRequest()); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	err := ValidateRestoreWorkloadRequest(context.Background(), &ateworkerpb.RestoreWorkloadRequest{})
	if apierror.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty request error = %v, want InvalidArgument", err)
	}
}

func validTerminateWorkloadRequest(mutate ...func(*ateworkerpb.TerminateWorkloadRequest)) *ateworkerpb.TerminateWorkloadRequest {
	r := &ateworkerpb.TerminateWorkloadRequest{
		Atespace:              "team-a",
		ActorName:             "actor-1",
		ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
		ActorTemplateAtespace: "team-a",
		ActorTemplateName:     "template-1",
		RunscPath:             "/opt/runsc",
		Spec:                  fullSpec(),
		ActorDirs:             fullActorDirs(),
	}
	for _, m := range mutate {
		m(r)
	}
	return r
}

func TestValidateTerminateWorkloadRequest(t *testing.T) {
	valid := validTerminateWorkloadRequest

	tests := []struct {
		name string
		obj  *ateworkerpb.TerminateWorkloadRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "valid: no template ref, no runsc_path",
		obj: valid(func(r *ateworkerpb.TerminateWorkloadRequest) {
			r.ActorTemplateAtespace, r.ActorTemplateName, r.RunscPath = "", "", ""
		}),
	}, {
		name: "missing atespace",
		obj:  valid(func(r *ateworkerpb.TerminateWorkloadRequest) { r.Atespace = "" }),
		want: field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		name: "invalid atespace: uppercase",
		obj:  valid(func(r *ateworkerpb.TerminateWorkloadRequest) { r.Atespace = "Team-A" }),
		want: field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_name",
		obj:  valid(func(r *ateworkerpb.TerminateWorkloadRequest) { r.ActorName = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_name"), "")},
	}, {
		name: "invalid actor_name: trailing dash",
		obj:  valid(func(r *ateworkerpb.TerminateWorkloadRequest) { r.ActorName = "actor-" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_uid",
		obj:  valid(func(r *ateworkerpb.TerminateWorkloadRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid actor_uid: not a uuid",
		obj:  valid(func(r *ateworkerpb.TerminateWorkloadRequest) { r.ActorUid = "uid-a" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "invalid actor_template_atespace: uppercase",
		obj:  valid(func(r *ateworkerpb.TerminateWorkloadRequest) { r.ActorTemplateAtespace = "Team-A" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_template_atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "invalid actor_template_name: underscore",
		obj:  valid(func(r *ateworkerpb.TerminateWorkloadRequest) { r.ActorTemplateName = "template_1" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_template_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "runsc_path too long",
		obj:  valid(func(r *ateworkerpb.TerminateWorkloadRequest) { r.RunscPath = "/" + strings.Repeat("x", 4096) }),
		want: field.ErrorList{field.TooLong(field.NewPath("runsc_path"), nil, 4096).WithOrigin("maxLength")},
	}, {
		name: "missing spec",
		obj:  valid(func(r *ateworkerpb.TerminateWorkloadRequest) { r.Spec = nil }),
		want: field.ErrorList{field.Required(field.NewPath("spec"), "")},
	}, {
		name: "valid: spec without containers, the fallback for a template-less actor",
		obj:  valid(func(r *ateworkerpb.TerminateWorkloadRequest) { r.Spec = &ateworkerpb.WorkloadSpec{} }),
	}, {
		name: "missing actor_dirs",
		obj:  valid(func(r *ateworkerpb.TerminateWorkloadRequest) { r.ActorDirs = nil }),
		want: field.ErrorList{field.Required(field.NewPath("actor_dirs"), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_TerminateWorkloadRequest(context.Background(), createOp(), nil, tt.obj, nil))
		})
	}
}

// TestValidateTerminateWorkloadRequestEdge covers the handler-facing wrapper:
// valid passes, invalid comes back as InvalidArgument.
func TestValidateTerminateWorkloadRequestEdge(t *testing.T) {
	if err := ValidateTerminateWorkloadRequest(context.Background(), validTerminateWorkloadRequest()); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	err := ValidateTerminateWorkloadRequest(context.Background(), &ateworkerpb.TerminateWorkloadRequest{})
	if apierror.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty request error = %v, want InvalidArgument", err)
	}
}

func TestValidateGetWorkloadStatsRequest(t *testing.T) {
	tests := []struct {
		name string
		obj  *ateworkerpb.GetWorkloadStatsRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  &ateworkerpb.GetWorkloadStatsRequest{ActorUid: "01234567-89ab-cdef-0123-456789abcdef"},
	}, {
		name: "missing actor_uid",
		obj:  &ateworkerpb.GetWorkloadStatsRequest{},
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid actor_uid: not a uuid",
		obj:  &ateworkerpb.GetWorkloadStatsRequest{ActorUid: "uid-a"},
		want: field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_GetWorkloadStatsRequest(context.Background(), createOp(), nil, tt.obj, nil))
		})
	}
}

// TestValidateGetWorkloadStatsRequestEdge covers the handler-facing wrapper:
// valid passes, invalid comes back as InvalidArgument.
func TestValidateGetWorkloadStatsRequestEdge(t *testing.T) {
	valid := &ateworkerpb.GetWorkloadStatsRequest{ActorUid: "01234567-89ab-cdef-0123-456789abcdef"}
	if err := ValidateGetWorkloadStatsRequest(context.Background(), valid); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	err := ValidateGetWorkloadStatsRequest(context.Background(), &ateworkerpb.GetWorkloadStatsRequest{})
	if apierror.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty request error = %v, want InvalidArgument", err)
	}
}
