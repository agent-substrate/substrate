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

package ocispec

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/sizing"
	"github.com/opencontainers/runtime-spec/specs-go"
)

// parityOptions mounts one volume of every kind.
var parityOptions = Options{
	ActorUID:      testActorUID,
	ContainerName: "app",
	Args:          []string{"/app"},
	Volumes: []*ateletpb.Volume{
		durableVolume("data"),
		{Name: "sysinfo", Source: &ateletpb.Volume_SystemInfo{SystemInfo: &ateletpb.SystemInfoVolume{}}},
		{Name: "csi", Source: &ateletpb.Volume_External{External: &ateletpb.ExternalVolumeSource{}}},
		{Name: "agent", Source: &ateletpb.Volume_Image{Image: &ateletpb.ImageVolumeSource{}}},
	},
	VolumeMounts: []*ateletpb.VolumeMount{
		{Name: "data", MountPath: "/var/data"},
		// System-info volume mount.
		{Name: "sysinfo", MountPath: "/run/ate"},
		{Name: "csi", MountPath: "/mnt/csi"},
		{Name: "agent", MountPath: "/ate"},
	},
}

var paritySize = sizing.SandboxSize{MilliCPU: 500, MemoryBytes: 512 << 20}

// Build's output contains no runtime-specific literals.
func TestBuild_IsRuntimeNeutral(t *testing.T) {
	b, err := json.Marshal(Build(parityOptions))
	if err != nil {
		t.Fatal(err)
	}
	for _, literal := range []string{"runsc", "io.kubernetes.cri.", "dev.gvisor.", "kata"} {
		if strings.Contains(string(b), literal) {
			t.Errorf("base spec contains the runtime-specific literal %q:\n%s", literal, b)
		}
	}
}

// Both shapers keep every declared volume mount, with a non-empty source.
func TestShapers_PreserveEveryVolumeMount(t *testing.T) {
	for _, tc := range []struct {
		runtime string
		shape   func(*specs.Spec) error
	}{{
		runtime: "gvisor",
		shape: func(s *specs.Spec) error {
			ShapeGVisor(s, GVisorOptions{ActorUID: testActorUID, ContainerName: "app", Size: paritySize})
			return nil
		},
	}, {
		runtime: "microvm",
		shape: func(s *specs.Spec) error {
			return ShapeMicroVM(s, MicroVMOptions{ActorUID: testActorUID, ContainerID: "app"})
		},
	}} {
		t.Run(tc.runtime, func(t *testing.T) {
			spec := Build(parityOptions)
			if err := tc.shape(spec); err != nil {
				t.Fatalf("shaping the spec: %v", err)
			}
			for _, vm := range parityOptions.VolumeMounts {
				m := mountFor(t, spec, vm.GetMountPath())
				if m.Source == "" {
					t.Errorf("mount %q has no source after shaping", vm.GetMountPath())
				}
			}
			// Neither shaper overrides the hostname.
			if spec.Hostname != "actor" {
				t.Errorf("hostname = %q, want actor", spec.Hostname)
			}
			// Both shapers leave the container with cgroup settings; where the
			// limits come from is runtime-specific (see sizing's package doc).
			if spec.Linux.Resources == nil {
				t.Error("Linux.Resources = nil after shaping")
			}
		})
	}
}

// ShapeMicroVM rewrites bind sources to their guest share paths.
func TestShapeMicroVM_TranslatesSourcesIntoTheShare(t *testing.T) {
	spec := Build(parityOptions)
	if err := ShapeMicroVM(spec, MicroVMOptions{ActorUID: testActorUID, ContainerID: "app"}); err != nil {
		t.Fatalf("ShapeMicroVM() = %v", err)
	}
	for _, tc := range []struct{ dest, wantSource string }{
		{"/var/data", GuestSharedDir + "/" + ShareDurable + "/data"},
		{"/run/ate", GuestSharedDir + "/" + ShareSystemInfo + "/sysinfo"},
		{"/mnt/csi", GuestSharedDir + "/" + ShareCSI + "/csi"},
		{"/ate", GuestSharedDir + "/app/" + ShareVolumes + "/agent"},
	} {
		if got := mountFor(t, spec, tc.dest).Source; got != tc.wantSource {
			t.Errorf("%s source = %q, want %q", tc.dest, got, tc.wantSource)
		}
	}
	// Guest system mounts replace the host's.
	if got := mountFor(t, spec, "/dev/shm").Source; got != "shm" {
		t.Errorf("/dev/shm source = %q, want the guest system mount", got)
	}
}

// ShapeMicroVM errors on a bind that is not staged into the share.
func TestShapeMicroVM_UnstagedSourceIsAnError(t *testing.T) {
	spec := Build(Options{ActorUID: testActorUID, ContainerName: "app", Args: []string{"/app"}})
	spec.Mounts = append(spec.Mounts, specs.Mount{Destination: "/mnt/new", Type: "bind", Source: "/var/lib/ate/new-kind/x"})
	if err := ShapeMicroVM(spec, MicroVMOptions{ActorUID: testActorUID, ContainerID: "app"}); err == nil {
		t.Fatal("ShapeMicroVM() = nil, want an error for a bind that is not staged into the share")
	}
}

// ShapeGVisor inserts resolv.conf ahead of the volume bind mounts.
func TestShapeGVisor_ResolvConfPrecedesVolumes(t *testing.T) {
	spec := Build(parityOptions)
	ShapeGVisor(spec, GVisorOptions{ActorUID: testActorUID, ContainerName: "app", Size: paritySize})
	at := func(dest string) int {
		return slices.IndexFunc(spec.Mounts, func(m specs.Mount) bool { return m.Destination == dest })
	}
	if got, want := at(resolvConf), at("/var/data"); got > want {
		t.Errorf("%s is at index %d, after the first volume bind at %d", resolvConf, got, want)
	}
}

// Shaping a spec twice does not accumulate mounts.
func TestShapeGVisor_Idempotent(t *testing.T) {
	spec := Build(parityOptions)
	o := GVisorOptions{ActorUID: testActorUID, ContainerName: PauseContainer, DurableVolumes: []string{"data"}, Size: paritySize}
	ShapeGVisor(spec, o)
	first := len(spec.Mounts)
	ShapeGVisor(spec, o)
	if len(spec.Mounts) != first {
		t.Errorf("mount count = %d after a second shaping, want %d", len(spec.Mounts), first)
	}
	if got := spec.Annotations["io.kubernetes.cri.container-type"]; got != "sandbox" {
		t.Errorf("pause container-type = %q, want sandbox", got)
	}
}

// gVisor sizes the container's cgroup leaf from the actor-level limits: one
// sentry backs every container, so the sandbox cgroup is the only one that
// binds.
func TestShapeGVisor_AppliesTheActorSize(t *testing.T) {
	spec := Build(parityOptions)
	ShapeGVisor(spec, GVisorOptions{ActorUID: testActorUID, ContainerName: "app", Size: paritySize})
	if got := *spec.Linux.Resources.Memory.Limit; got != paritySize.MemoryBytes {
		t.Errorf("memory limit = %d, want %d", got, paritySize.MemoryBytes)
	}
}
