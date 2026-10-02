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

package ateomvalidation

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"google.golang.org/grpc/codes"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Every request is validated. Still opaque: egress_gateway, the wakeup probe,
// and the items of the four mount lists; their contents get rules in
// follow-ups.

func fullSpec() *ateompb.WorkloadSpec {
	return &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{
		Name:                   "main",
		WakeupProbe:            &ateompb.WakeupProbe{HttpGet: &ateompb.HttpGetAction{Path: "/healthz", Port: 8080}, TimeoutSeconds: 30},
		DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{{VolumeName: "data", MountPath: "/data"}},
		CsiVolumeMounts:        []*ateompb.VolumeMount{{VolumeName: "csi", MountPath: "/csi"}},
		SystemInfoVolumeMounts: []*ateompb.SystemInfoVolumeMount{{VolumeName: "sysinfo", MountPath: "/etc/sysinfo"}},
		ImageVolumeMounts:      []*ateompb.ImageVolumeMount{{VolumeName: "img", MountPath: "/img"}},
	}}}
}

func fullActorDirs() *ateompb.ActorDirs {
	return &ateompb.ActorDirs{
		RootDir:                   "/actors/uid",
		OciBundleDir:              "/actors/uid/bundles",
		CheckpointDir:             "/actors/uid/checkpoint",
		RestoreDir:                "/actors/uid/restore",
		DurableDirVolumeMountsDir: "/actors/uid/durable",
		SystemInfoVolumeRootsDir:  "/actors/uid/sysinfo",
		VolumesDir:                "/actors/uid/volumes",
	}
}

func validRunWorkloadRequest(mutate ...func(*ateompb.RunWorkloadRequest)) *ateompb.RunWorkloadRequest {
	r := &ateompb.RunWorkloadRequest{
		Atespace:              "team-a",
		ActorName:             "actor-1",
		ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
		ActorTemplateAtespace: "team-a",
		ActorTemplateName:     "template-1",
		RunscPath:             "/opt/runsc",
		Spec:                  fullSpec(),
		RuntimeAssetPaths:     map[string]string{"kata-kernel": "/opt/kernel", "virtiofsd": "/opt/virtiofsd"},
		EgressGateway:         &ateompb.EgressGateway{Address: "gateway:443"},
		CpuMilli:              1500,
		MemoryBytes:           1 << 30,
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

	tests := []struct {
		name string
		obj  *ateompb.RunWorkloadRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "valid: gVisor shape, no assets, no egress, unset size",
		obj: valid(func(r *ateompb.RunWorkloadRequest) {
			r.RuntimeAssetPaths, r.EgressGateway, r.CpuMilli, r.MemoryBytes = nil, nil, 0, 0
		}),
	}, {
		name: "missing atespace",
		obj:  valid(func(r *ateompb.RunWorkloadRequest) { r.Atespace = "" }),
		want: field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		name: "invalid atespace: uppercase",
		obj:  valid(func(r *ateompb.RunWorkloadRequest) { r.Atespace = "Team-A" }),
		want: field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_name",
		obj:  valid(func(r *ateompb.RunWorkloadRequest) { r.ActorName = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_name"), "")},
	}, {
		name: "missing actor_uid",
		obj:  valid(func(r *ateompb.RunWorkloadRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid actor_uid: not a uuid",
		obj:  valid(func(r *ateompb.RunWorkloadRequest) { r.ActorUid = "uid-a" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "invalid actor_template_name: underscore",
		obj:  valid(func(r *ateompb.RunWorkloadRequest) { r.ActorTemplateName = "template_1" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_template_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "runsc_path too long",
		obj:  valid(func(r *ateompb.RunWorkloadRequest) { r.RunscPath = "/" + strings.Repeat("x", 4096) }),
		want: field.ErrorList{field.TooLong(field.NewPath("runsc_path"), nil, 4096).WithOrigin("maxLength")},
	}, {
		name: "missing spec",
		obj:  valid(func(r *ateompb.RunWorkloadRequest) { r.Spec = nil }),
		want: field.ErrorList{field.Required(field.NewPath("spec"), "")},
	}, {
		name: "too many runtime_asset_paths",
		obj: valid(func(r *ateompb.RunWorkloadRequest) {
			r.RuntimeAssetPaths = map[string]string{}
			for i := range 17 {
				r.RuntimeAssetPaths[fmt.Sprintf("asset-%d", i)] = "/opt/asset"
			}
		}),
		want: field.ErrorList{field.TooMany(assets, 17, 16).WithOrigin("maxProperties")},
	}, {
		name: "runtime asset name too long",
		obj: valid(func(r *ateompb.RunWorkloadRequest) {
			r.RuntimeAssetPaths = map[string]string{strings.Repeat("k", 65): "/opt/asset"}
		}),
		want: field.ErrorList{field.TooLong(assets, nil, 64).WithOrigin("maxLength")}, // keys are reported at the map
	}, {
		name: "runtime asset path too long",
		obj: valid(func(r *ateompb.RunWorkloadRequest) {
			r.RuntimeAssetPaths = map[string]string{"kata-kernel": "/" + strings.Repeat("x", 4096)}
		}),
		want: field.ErrorList{field.TooLong(assets.Key("kata-kernel"), nil, 4096).WithOrigin("maxLength")},
	}, {
		name: "negative cpu_milli",
		obj:  valid(func(r *ateompb.RunWorkloadRequest) { r.CpuMilli = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("cpu_milli"), nil, "").WithOrigin("minimum")},
	}, {
		name: "cpu_milli past atelet's bound",
		obj:  valid(func(r *ateompb.RunWorkloadRequest) { r.CpuMilli = 1000000 }),
		want: field.ErrorList{field.Invalid(field.NewPath("cpu_milli"), nil, "").WithOrigin("maximum")},
	}, {
		name: "negative memory_bytes",
		obj:  valid(func(r *ateompb.RunWorkloadRequest) { r.MemoryBytes = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("memory_bytes"), nil, "").WithOrigin("minimum")},
	}, {
		name: "empty egress_gateway",
		obj:  valid(func(r *ateompb.RunWorkloadRequest) { r.EgressGateway = &ateompb.EgressGateway{} }),
		want: field.ErrorList{field.Required(field.NewPath("egress_gateway", "address"), "")},
	}, {
		name: "egress_gateway address without port",
		obj:  valid(func(r *ateompb.RunWorkloadRequest) { r.EgressGateway.Address = "gateway" }),
		want: field.ErrorList{field.Invalid(field.NewPath("egress_gateway", "address"), nil, "")},
	}, {
		name: "egress_gateway address too long",
		obj:  valid(func(r *ateompb.RunWorkloadRequest) { r.EgressGateway.Address = strings.Repeat("h", 258) + ":443" }),
		want: field.ErrorList{
			field.TooLong(field.NewPath("egress_gateway", "address"), nil, 261).WithOrigin("maxLength"),
			field.Invalid(field.NewPath("egress_gateway", "address"), nil, ""),
		},
	}, {
		name: "missing actor_dirs",
		obj:  valid(func(r *ateompb.RunWorkloadRequest) { r.ActorDirs = nil }),
		want: field.ErrorList{field.Required(field.NewPath("actor_dirs"), "")},
	}, {
		name: "relative oci_bundle_dir",
		obj:  valid(func(r *ateompb.RunWorkloadRequest) { r.ActorDirs.OciBundleDir = "bundles" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_dirs", "oci_bundle_dir"), nil, "")},
	}, {
		name: "spec without containers",
		obj:  valid(func(r *ateompb.RunWorkloadRequest) { r.Spec = &ateompb.WorkloadSpec{} }),
		want: field.ErrorList{field.Required(field.NewPath("spec", "containers"), "")},
	}, {
		name: "bad container name",
		obj:  valid(func(r *ateompb.RunWorkloadRequest) { r.Spec.Containers[0].Name = "Main" }),
		want: field.ErrorList{field.Invalid(field.NewPath("spec", "containers").Index(0).Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
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
	err := ValidateRunWorkloadRequest(context.Background(), &ateompb.RunWorkloadRequest{})
	if apierror.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty request error = %v, want InvalidArgument", err)
	}
}

func validCheckpointWorkloadRequest(mutate ...func(*ateompb.CheckpointWorkloadRequest)) *ateompb.CheckpointWorkloadRequest {
	r := &ateompb.CheckpointWorkloadRequest{
		Atespace:              "team-a",
		ActorName:             "actor-1",
		ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
		ActorTemplateAtespace: "team-a",
		ActorTemplateName:     "template-1",
		RunscPath:             "/opt/runsc",
		Spec:                  fullSpec(),
		SnapshotUri:           "gs://bucket/root/atespaces/team-a/actors/uid/snapshots/1",
		RuntimeAssetPaths:     map[string]string{"kata-kernel": "/opt/kernel"},
		Fidelity:              ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		ActorDirs:             fullActorDirs(),
	}
	for _, m := range mutate {
		m(r)
	}
	return r
}

// Checkpoint shares its identity and asset fields with Run, which the Run
// table covers; this table holds the shared gates at one case each and
// covers what only Checkpoint has: snapshot_uri, fidelity, and actor_dirs.
func TestValidateCheckpointWorkloadRequest(t *testing.T) {
	valid := validCheckpointWorkloadRequest

	tests := []struct {
		name string
		obj  *ateompb.CheckpointWorkloadRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "valid: volumes fidelity, gVisor shape",
		obj: valid(func(r *ateompb.CheckpointWorkloadRequest) {
			r.Fidelity, r.RuntimeAssetPaths = ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, nil
		}),
	}, {
		name: "valid: rootfs fidelity, which runtimes judge for themselves",
		obj: valid(func(r *ateompb.CheckpointWorkloadRequest) {
			r.Fidelity = ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
		}),
	}, {
		name: "missing actor_uid",
		obj:  valid(func(r *ateompb.CheckpointWorkloadRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid actor_name: trailing dash",
		obj:  valid(func(r *ateompb.CheckpointWorkloadRequest) { r.ActorName = "actor-" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "runsc_path too long",
		obj:  valid(func(r *ateompb.CheckpointWorkloadRequest) { r.RunscPath = "/" + strings.Repeat("x", 4096) }),
		want: field.ErrorList{field.TooLong(field.NewPath("runsc_path"), nil, 4096).WithOrigin("maxLength")},
	}, {
		name: "missing spec",
		obj:  valid(func(r *ateompb.CheckpointWorkloadRequest) { r.Spec = nil }),
		want: field.ErrorList{field.Required(field.NewPath("spec"), "")},
	}, {
		name: "missing snapshot_uri",
		obj:  valid(func(r *ateompb.CheckpointWorkloadRequest) { r.SnapshotUri = "" }),
		want: field.ErrorList{field.Required(field.NewPath("snapshot_uri"), "")},
	}, {
		name: "invalid snapshot_uri: fragment",
		obj:  valid(func(r *ateompb.CheckpointWorkloadRequest) { r.SnapshotUri = "gs://bucket/snapshots/1#frag" }),
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot_uri"), nil, "")},
	}, {
		name: "too many runtime_asset_paths",
		obj: valid(func(r *ateompb.CheckpointWorkloadRequest) {
			r.RuntimeAssetPaths = map[string]string{}
			for i := range 17 {
				r.RuntimeAssetPaths[fmt.Sprintf("asset-%d", i)] = "/opt/asset"
			}
		}),
		want: field.ErrorList{field.TooMany(field.NewPath("runtime_asset_paths"), 17, 16).WithOrigin("maxProperties")},
	}, {
		name: "missing fidelity",
		obj: valid(func(r *ateompb.CheckpointWorkloadRequest) {
			r.Fidelity = ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
		}),
		want: field.ErrorList{field.Required(field.NewPath("fidelity"), "")},
	}, {
		name: "fidelity past the enum",
		obj:  valid(func(r *ateompb.CheckpointWorkloadRequest) { r.Fidelity = ateompb.SnapshotFidelity(4) }),
		want: field.ErrorList{field.Invalid(field.NewPath("fidelity"), nil, "").WithOrigin("maximum")},
	}, {
		name: "missing actor_dirs",
		obj:  valid(func(r *ateompb.CheckpointWorkloadRequest) { r.ActorDirs = nil }),
		want: field.ErrorList{field.Required(field.NewPath("actor_dirs"), "")},
	}, {
		name: "relative checkpoint_dir",
		obj:  valid(func(r *ateompb.CheckpointWorkloadRequest) { r.ActorDirs.CheckpointDir = "checkpoint" }),
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
	err := ValidateCheckpointWorkloadRequest(context.Background(), &ateompb.CheckpointWorkloadRequest{})
	if apierror.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty request error = %v, want InvalidArgument", err)
	}
}

// WorkloadSpec is reached from all four lifecycle requests; the Run table
// carries one case to show the path prefix, and this table holds the rules.
func TestValidateWorkloadSpec(t *testing.T) {
	container := func(name string) *ateompb.Container { return &ateompb.Container{Name: name} }
	tests := []struct {
		name string
		obj  *ateompb.WorkloadSpec
		want field.ErrorList
	}{{
		name: "valid",
		obj:  fullSpec(),
	}, {
		name: "valid: at the containers bound",
		obj: func() *ateompb.WorkloadSpec {
			spec := &ateompb.WorkloadSpec{}
			for i := range MaxContainers {
				spec.Containers = append(spec.Containers, container(fmt.Sprintf("c-%d", i)))
			}
			return spec
		}(),
	}, {
		name: "no containers: Terminate's fallback spec for a template-less actor",
		obj:  &ateompb.WorkloadSpec{},
	}, {
		name: "over the containers bound",
		obj: func() *ateompb.WorkloadSpec {
			spec := &ateompb.WorkloadSpec{}
			for i := range MaxContainers + 1 {
				spec.Containers = append(spec.Containers, container(fmt.Sprintf("c-%d", i)))
			}
			return spec
		}(),
		want: field.ErrorList{field.TooMany(field.NewPath("containers"), MaxContainers+1, MaxContainers).WithOrigin("maxItems")},
	}, {
		name: "duplicate container name",
		obj:  &ateompb.WorkloadSpec{Containers: []*ateompb.Container{container("app"), container("app")}},
		want: field.ErrorList{field.Duplicate(field.NewPath("containers").Index(1), nil)},
	}, {
		name: "nil container",
		obj:  &ateompb.WorkloadSpec{Containers: []*ateompb.Container{nil}},
		want: field.ErrorList{field.Required(field.NewPath("containers").Index(0), "")},
	}, {
		name: "bad name inside the list",
		obj:  &ateompb.WorkloadSpec{Containers: []*ateompb.Container{container("app"), container("Sidecar")}},
		want: field.ErrorList{field.Invalid(field.NewPath("containers").Index(1).Child("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tt.want, Validate_WorkloadSpec(context.Background(), createOp(), nil, tt.obj, nil))
		})
	}
}

// The mount lists' items and the probe are still opaque; the list rules
// (no nil entries, at most 32, unique mount paths) apply already.
func TestValidateContainer(t *testing.T) {
	valid := func(mutate ...func(*ateompb.Container)) *ateompb.Container {
		c := fullSpec().Containers[0]
		for _, m := range mutate {
			m(c)
		}
		return c
	}
	tests := []struct {
		name string
		obj  *ateompb.Container
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "valid: name only",
		obj:  &ateompb.Container{Name: "app"},
	}, {
		name: "missing name",
		obj:  valid(func(c *ateompb.Container) { c.Name = "" }),
		want: field.ErrorList{field.Required(field.NewPath("name"), "")},
	}, {
		name: "invalid name: the sandbox's own container",
		obj:  valid(func(c *ateompb.Container) { c.Name = "_pause" }),
		want: field.ErrorList{field.Invalid(field.NewPath("name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "too many csi mounts",
		obj: valid(func(c *ateompb.Container) {
			c.CsiVolumeMounts = nil
			for i := range 33 {
				c.CsiVolumeMounts = append(c.CsiVolumeMounts, &ateompb.VolumeMount{VolumeName: "v", MountPath: fmt.Sprintf("/m/%d", i)})
			}
		}),
		want: field.ErrorList{field.TooMany(field.NewPath("csi_volume_mounts"), 33, 32).WithOrigin("maxItems")},
	}, {
		name: "duplicate durable-dir mount path",
		obj: valid(func(c *ateompb.Container) {
			c.DurableDirVolumeMounts = []*ateompb.DurableDirVolumeMount{{VolumeName: "a", MountPath: "/data"}, {VolumeName: "b", MountPath: "/data"}}
		}),
		want: field.ErrorList{field.Duplicate(field.NewPath("durable_dir_volume_mounts").Index(1), nil)},
	}, {
		name: "nil image mount",
		obj:  valid(func(c *ateompb.Container) { c.ImageVolumeMounts = []*ateompb.ImageVolumeMount{nil} }),
		want: field.ErrorList{field.Required(field.NewPath("image_volume_mounts").Index(0), "")},
	}, {
		name: "opaque: an empty system-info mount passes for now",
		obj:  valid(func(c *ateompb.Container) { c.SystemInfoVolumeMounts = []*ateompb.SystemInfoVolumeMount{{}} }),
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
	valid := func(mutate ...func(*ateompb.ActorDirs)) *ateompb.ActorDirs {
		d := fullActorDirs()
		for _, m := range mutate {
			m(d)
		}
		return d
	}
	tests := []struct {
		name string
		obj  *ateompb.ActorDirs
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "missing root_dir",
		obj:  valid(func(d *ateompb.ActorDirs) { d.RootDir = "" }),
		want: field.ErrorList{field.Required(field.NewPath("root_dir"), "")},
	}, {
		name: "missing volumes_dir",
		obj:  valid(func(d *ateompb.ActorDirs) { d.VolumesDir = "" }),
		want: field.ErrorList{field.Required(field.NewPath("volumes_dir"), "")},
	}, {
		name: "relative oci_bundle_dir",
		obj:  valid(func(d *ateompb.ActorDirs) { d.OciBundleDir = "bundles" }),
		want: field.ErrorList{field.Invalid(field.NewPath("oci_bundle_dir"), nil, "")},
	}, {
		name: "unclean restore_dir",
		obj:  valid(func(d *ateompb.ActorDirs) { d.RestoreDir = "/actors/uid/../other/restore" }),
		want: field.ErrorList{field.Invalid(field.NewPath("restore_dir"), nil, "")},
	}, {
		name: "trailing slash on durable_dir_volume_mounts_dir",
		obj:  valid(func(d *ateompb.ActorDirs) { d.DurableDirVolumeMountsDir = "/actors/uid/durable/" }),
		want: field.ErrorList{field.Invalid(field.NewPath("durable_dir_volume_mounts_dir"), nil, "")},
	}, {
		name: "system_info_volume_roots_dir too long",
		obj:  valid(func(d *ateompb.ActorDirs) { d.SystemInfoVolumeRootsDir = "/" + strings.Repeat("x", 4096) }),
		want: field.ErrorList{field.TooLong(field.NewPath("system_info_volume_roots_dir"), nil, 4096).WithOrigin("maxLength")},
	}, {
		name: "two bad dirs are both reported",
		obj:  valid(func(d *ateompb.ActorDirs) { d.RootDir, d.CheckpointDir = "", "checkpoint" }),
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

func validRestoreWorkloadRequest(mutate ...func(*ateompb.RestoreWorkloadRequest)) *ateompb.RestoreWorkloadRequest {
	r := &ateompb.RestoreWorkloadRequest{
		Atespace:              "team-a",
		ActorName:             "actor-1",
		ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
		ActorTemplateAtespace: "team-a",
		ActorTemplateName:     "template-1",
		RunscPath:             "/opt/runsc",
		Spec:                  fullSpec(),
		SnapshotUri:           "gs://bucket/root/atespaces/team-a/actors/uid/snapshots/1",
		RuntimeAssetPaths:     map[string]string{"kata-kernel": "/opt/kernel"},
		Fidelity:              ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
		EgressGateway:         &ateompb.EgressGateway{Address: "gateway:443"},
		CpuMilli:              1500,
		MemoryBytes:           1 << 30,
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
		obj  *ateompb.RestoreWorkloadRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "valid: memory fidelity",
		obj: valid(func(r *ateompb.RestoreWorkloadRequest) {
			r.Fidelity = ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
		}),
	}, {
		name: "valid: rootfs fidelity, which runtimes judge for themselves",
		obj: valid(func(r *ateompb.RestoreWorkloadRequest) {
			r.Fidelity = ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
		}),
	}, {
		name: "missing actor_uid",
		obj:  valid(func(r *ateompb.RestoreWorkloadRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid atespace: uppercase",
		obj:  valid(func(r *ateompb.RestoreWorkloadRequest) { r.Atespace = "Team-A" }),
		want: field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing spec",
		obj:  valid(func(r *ateompb.RestoreWorkloadRequest) { r.Spec = nil }),
		want: field.ErrorList{field.Required(field.NewPath("spec"), "")},
	}, {
		name: "spec without containers",
		obj:  valid(func(r *ateompb.RestoreWorkloadRequest) { r.Spec = &ateompb.WorkloadSpec{} }),
		want: field.ErrorList{field.Required(field.NewPath("spec", "containers"), "")},
	}, {
		name: "missing actor_dirs",
		obj:  valid(func(r *ateompb.RestoreWorkloadRequest) { r.ActorDirs = nil }),
		want: field.ErrorList{field.Required(field.NewPath("actor_dirs"), "")},
	}, {
		name: "negative cpu_milli",
		obj:  valid(func(r *ateompb.RestoreWorkloadRequest) { r.CpuMilli = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("cpu_milli"), nil, "").WithOrigin("minimum")},
	}, {
		name: "cpu_milli past atelet's bound",
		obj:  valid(func(r *ateompb.RestoreWorkloadRequest) { r.CpuMilli = 1000000 }),
		want: field.ErrorList{field.Invalid(field.NewPath("cpu_milli"), nil, "").WithOrigin("maximum")},
	}, {
		name: "runtime asset path too long",
		obj: valid(func(r *ateompb.RestoreWorkloadRequest) {
			r.RuntimeAssetPaths = map[string]string{"kata-kernel": "/" + strings.Repeat("x", 4096)}
		}),
		want: field.ErrorList{field.TooLong(field.NewPath("runtime_asset_paths").Key("kata-kernel"), nil, 4096).WithOrigin("maxLength")},
	}, {
		name: "missing snapshot_uri",
		obj:  valid(func(r *ateompb.RestoreWorkloadRequest) { r.SnapshotUri = "" }),
		want: field.ErrorList{field.Required(field.NewPath("snapshot_uri"), "")},
	}, {
		name: "snapshot_uri too long",
		obj:  valid(func(r *ateompb.RestoreWorkloadRequest) { r.SnapshotUri = "gs://bucket/" + strings.Repeat("x", 2048) }),
		want: field.ErrorList{field.TooLong(field.NewPath("snapshot_uri"), nil, 2048).WithOrigin("maxLength")},
	}, {
		name: "invalid snapshot_uri: no bucket",
		obj:  valid(func(r *ateompb.RestoreWorkloadRequest) { r.SnapshotUri = "snapshots/1" }),
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot_uri"), nil, "")},
	}, {
		name: "invalid snapshot_uri: query string",
		obj:  valid(func(r *ateompb.RestoreWorkloadRequest) { r.SnapshotUri = "gs://bucket/snapshots/1?x=1" }),
		want: field.ErrorList{field.Invalid(field.NewPath("snapshot_uri"), nil, "")},
	}, {
		name: "egress_gateway address without port",
		obj:  valid(func(r *ateompb.RestoreWorkloadRequest) { r.EgressGateway.Address = "gateway" }),
		want: field.ErrorList{field.Invalid(field.NewPath("egress_gateway", "address"), nil, "")},
	}, {
		name: "missing fidelity",
		obj: valid(func(r *ateompb.RestoreWorkloadRequest) {
			r.Fidelity = ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
		}),
		want: field.ErrorList{field.Required(field.NewPath("fidelity"), "")},
	}, {
		name: "fidelity past the enum",
		obj:  valid(func(r *ateompb.RestoreWorkloadRequest) { r.Fidelity = ateompb.SnapshotFidelity(4) }),
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
	err := ValidateRestoreWorkloadRequest(context.Background(), &ateompb.RestoreWorkloadRequest{})
	if apierror.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty request error = %v, want InvalidArgument", err)
	}
}

func validTerminateWorkloadRequest(mutate ...func(*ateompb.TerminateWorkloadRequest)) *ateompb.TerminateWorkloadRequest {
	r := &ateompb.TerminateWorkloadRequest{
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
		obj  *ateompb.TerminateWorkloadRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(),
	}, {
		name: "valid: no template ref, no runsc_path",
		obj: valid(func(r *ateompb.TerminateWorkloadRequest) {
			r.ActorTemplateAtespace, r.ActorTemplateName, r.RunscPath = "", "", ""
		}),
	}, {
		name: "missing atespace",
		obj:  valid(func(r *ateompb.TerminateWorkloadRequest) { r.Atespace = "" }),
		want: field.ErrorList{field.Required(field.NewPath("atespace"), "")},
	}, {
		name: "invalid atespace: uppercase",
		obj:  valid(func(r *ateompb.TerminateWorkloadRequest) { r.Atespace = "Team-A" }),
		want: field.ErrorList{field.Invalid(field.NewPath("atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_name",
		obj:  valid(func(r *ateompb.TerminateWorkloadRequest) { r.ActorName = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_name"), "")},
	}, {
		name: "invalid actor_name: trailing dash",
		obj:  valid(func(r *ateompb.TerminateWorkloadRequest) { r.ActorName = "actor-" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_uid",
		obj:  valid(func(r *ateompb.TerminateWorkloadRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid actor_uid: not a uuid",
		obj:  valid(func(r *ateompb.TerminateWorkloadRequest) { r.ActorUid = "uid-a" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "invalid actor_template_atespace: uppercase",
		obj:  valid(func(r *ateompb.TerminateWorkloadRequest) { r.ActorTemplateAtespace = "Team-A" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_template_atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "invalid actor_template_name: underscore",
		obj:  valid(func(r *ateompb.TerminateWorkloadRequest) { r.ActorTemplateName = "template_1" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_template_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "runsc_path too long",
		obj:  valid(func(r *ateompb.TerminateWorkloadRequest) { r.RunscPath = "/" + strings.Repeat("x", 4096) }),
		want: field.ErrorList{field.TooLong(field.NewPath("runsc_path"), nil, 4096).WithOrigin("maxLength")},
	}, {
		name: "missing spec",
		obj:  valid(func(r *ateompb.TerminateWorkloadRequest) { r.Spec = nil }),
		want: field.ErrorList{field.Required(field.NewPath("spec"), "")},
	}, {
		name: "valid: spec without containers, the fallback for a template-less actor",
		obj:  valid(func(r *ateompb.TerminateWorkloadRequest) { r.Spec = &ateompb.WorkloadSpec{} }),
	}, {
		name: "missing actor_dirs",
		obj:  valid(func(r *ateompb.TerminateWorkloadRequest) { r.ActorDirs = nil }),
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
	err := ValidateTerminateWorkloadRequest(context.Background(), &ateompb.TerminateWorkloadRequest{})
	if apierror.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty request error = %v, want InvalidArgument", err)
	}
}

func TestValidateGetWorkloadStatsRequest(t *testing.T) {
	tests := []struct {
		name string
		obj  *ateompb.GetWorkloadStatsRequest
		want field.ErrorList
	}{{
		name: "valid",
		obj:  &ateompb.GetWorkloadStatsRequest{ActorUid: "01234567-89ab-cdef-0123-456789abcdef"},
	}, {
		name: "missing actor_uid",
		obj:  &ateompb.GetWorkloadStatsRequest{},
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid actor_uid: not a uuid",
		obj:  &ateompb.GetWorkloadStatsRequest{ActorUid: "uid-a"},
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
	valid := &ateompb.GetWorkloadStatsRequest{ActorUid: "01234567-89ab-cdef-0123-456789abcdef"}
	if err := ValidateGetWorkloadStatsRequest(context.Background(), valid); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	err := ValidateGetWorkloadStatsRequest(context.Background(), &ateompb.GetWorkloadStatsRequest{})
	if apierror.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty request error = %v, want InvalidArgument", err)
	}
}
