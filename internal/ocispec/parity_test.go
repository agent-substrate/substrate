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

	"github.com/opencontainers/runtime-spec/specs-go"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/sizing"
)

const testActorUID = "actor_uid"

var parityActorDirs = &ateworkerpb.ActorDirs{
	RootDir:                   "/node/actors/a",
	OciBundleDir:              "/node/actors/a/bundles",
	DurableDirVolumeMountsDir: "/node/actors/a/durable-dir",
	SystemInfoVolumeRootsDir:  "/node/actors/a/system-info",
	ExternalVolumeMountsDir:   "/node/actors/a/volumes",
}

// parityOptions mounts one volume of every kind.
var parityOptions = Options{
	ActorUID:  testActorUID,
	ActorDirs: parityActorDirs,
	Container: &ateworkerpb.Container{
		Name:                   "app",
		ContainerSpec:          &ateworkerpb.ContainerSpec{Args: []string{"/app"}},
		DurableDirVolumeMounts: []*ateworkerpb.VolumeMount{{Name: "data", MountPath: "/var/data"}},
		SystemInfoVolumeMounts: []*ateworkerpb.VolumeMount{{Name: "sysinfo", MountPath: "/run/ate"}},
		ExternalVolumeMounts:   []*ateworkerpb.VolumeMount{{Name: "csi", MountPath: "/mnt/csi"}},
		ImageVolumeMounts:      []*ateworkerpb.VolumeMount{{Name: "agent", MountPath: "/ate"}},
	},
}

// parityMountPaths are where parityOptions mounts its volumes.
var parityMountPaths = []string{"/var/data", "/run/ate", "/mnt/csi", "/ate"}

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
			return ShapeMicroVM(s, MicroVMOptions{ActorDirs: parityActorDirs, ContainerName: "app"})
		},
	}} {
		t.Run(tc.runtime, func(t *testing.T) {
			spec := Build(parityOptions)
			if err := tc.shape(spec); err != nil {
				t.Fatalf("shaping the spec: %v", err)
			}
			for _, dest := range parityMountPaths {
				if m := mountFor(t, spec, dest); m.Source == "" {
					t.Errorf("mount %q has no source after shaping", dest)
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
	if err := ShapeMicroVM(spec, MicroVMOptions{ActorDirs: parityActorDirs, ContainerName: "app"}); err != nil {
		t.Fatalf("ShapeMicroVM() = %v", err)
	}
	for _, tc := range []struct{ dest, wantSource string }{
		{"/var/data", GuestSharedDir + "/" + ShareDurable + "/data"},
		{"/run/ate", GuestSharedDir + "/" + ShareSystemInfo + "/sysinfo"},
		{"/mnt/csi", GuestSharedDir + "/" + ShareCSI + "/csi"},
		{"/ate", GuestSharedDir + "/c_app/" + ShareVolumes + "/agent"},
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
	spec := Build(Options{Container: &ateworkerpb.Container{ContainerSpec: &ateworkerpb.ContainerSpec{Args: []string{"/app"}}}})
	spec.Mounts = append(spec.Mounts, specs.Mount{Destination: "/mnt/new", Type: "bind", Source: "/var/lib/ate/new-kind/x"})
	if err := ShapeMicroVM(spec, MicroVMOptions{ActorDirs: parityActorDirs, ContainerName: "app"}); err == nil {
		t.Fatal("ShapeMicroVM() = nil, want an error for a bind that is not staged into the share")
	}
}

// Shaping a spec twice does not accumulate mounts.
func TestShapeGVisor_Idempotent(t *testing.T) {
	spec := Build(parityOptions)
	o := GVisorOptions{ActorUID: testActorUID, ActorDirs: parityActorDirs, ContainerName: "app", Size: paritySize}
	ShapeGVisor(spec, o)
	first := len(spec.Mounts)
	ShapeGVisor(spec, o)
	if len(spec.Mounts) != first {
		t.Errorf("mount count = %d after a second shaping, want %d", len(spec.Mounts), first)
	}
}

// The sandbox spec has no process, rootfs, or mounts, and declares the
// durable-dir mount hints.
func TestShapeGVisor_Sandbox(t *testing.T) {
	spec := BuildSandbox(testActorUID)
	ShapeGVisor(spec, GVisorOptions{ActorUID: testActorUID, ActorDirs: parityActorDirs, ContainerName: SandboxID, DurableVolumes: []string{"data"}, Size: paritySize})
	if spec.Process != nil || spec.Root != nil || spec.Mounts != nil {
		t.Errorf("sandbox spec has process %v, root %v, mounts %v; want none", spec.Process, spec.Root, spec.Mounts)
	}
	if got := spec.Annotations["io.kubernetes.cri.container-type"]; got != "sandbox" {
		t.Errorf("sandbox container-type = %q, want sandbox", got)
	}
	if got := spec.Annotations["dev.gvisor.spec.mount.data.type"]; got != "tmpfs" {
		t.Errorf("durable-dir mount type annotation = %q, want tmpfs", got)
	}
	if got := spec.Annotations["dev.gvisor.spec.mount.data.share"]; got != "container" {
		t.Errorf("durable-dir mount share annotation = %q, want container", got)
	}
	if got, want := spec.Annotations["dev.gvisor.spec.mount.data.source"], "/node/actors/a/durable-dir/data"; got != want {
		t.Errorf("durable-dir mount source annotation = %q, want %q", got, want)
	}
}

// gVisor sizes the container's cgroup leaf from the actor-level limits: one
// sentry backs every container, so the sandbox cgroup is the only one that
// binds. The sandbox spec keeps its limits, cgroup, and namespaces.
func TestShapeGVisor_AppliesTheActorSize(t *testing.T) {
	for _, name := range []string{"app", SandboxID} {
		spec := Build(parityOptions)
		if name == SandboxID {
			spec = BuildSandbox(testActorUID)
		}
		ShapeGVisor(spec, GVisorOptions{ActorUID: testActorUID, ContainerName: name, Size: paritySize})
		if got := *spec.Linux.Resources.Memory.Limit; got != paritySize.MemoryBytes {
			t.Errorf("%s memory limit = %d, want %d", name, got, paritySize.MemoryBytes)
		}
		if want := "/" + GVisorCgroupLeaf(testActorUID, name); spec.Linux.CgroupsPath != want {
			t.Errorf("%s cgroupsPath = %q, want %q", name, spec.Linux.CgroupsPath, want)
		}
		if !slices.ContainsFunc(spec.Linux.Namespaces, func(ns specs.LinuxNamespace) bool { return ns.Type == "network" && ns.Path != "" }) {
			t.Errorf("%s namespaces = %v, want the actor's network namespace", name, spec.Linux.Namespaces)
		}
		if spec.Hostname == "" {
			t.Errorf("%s hostname is empty", name)
		}
	}
}
