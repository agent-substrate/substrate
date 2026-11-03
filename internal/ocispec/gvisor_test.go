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
	"reflect"
	"strings"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/sizing"
)

func TestShapeGVisorGivesEachActorItsOwnCgroupLeaf(t *testing.T) {
	shape := func(actorUID, containerName string) string {
		spec := &specs.Spec{}
		ShapeGVisor(spec, GVisorOptions{ActorUID: actorUID, ContainerName: containerName})
		return spec.Linux.CgroupsPath
	}

	first := shape("uid-a", SandboxID)
	second := shape("uid-b", SandboxID)
	if first == second {
		t.Errorf("two actors share the cgroup leaf %q", first)
	}
	if !strings.Contains(first, "uid-a") {
		t.Errorf("cgroupsPath %q does not name the actor", first)
	}
	// runsc interprets colons as systemd cgroup syntax.
	if strings.Contains(first, ":") {
		t.Errorf("cgroupsPath %q contains a colon", first)
	}

	if a, b := shape("uid-a", SandboxID), shape("uid-a", "app"); a == b {
		t.Errorf("two containers of one actor share the leaf %q", a)
	}
}

func TestGVisorCgroupLeafMatchesTheShapedPath(t *testing.T) {
	spec := &specs.Spec{}
	ShapeGVisor(spec, GVisorOptions{ActorUID: "uid-a", ContainerName: SandboxID})
	if want := "/" + GVisorCgroupLeaf("uid-a", SandboxID); spec.Linux.CgroupsPath != want {
		t.Errorf("shaped cgroupsPath = %q, want %q", spec.Linux.CgroupsPath, want)
	}
}

// runsc replaces /dev and /dev/pts with its own mounts and ignores mqueue
// mounts and the nodev and strictatime options, so ShapeGVisor drops them.
// /tmp is capped at half the memory limit when tmpfs.
func TestShapeGVisor_SystemMounts(t *testing.T) {
	const durableDir = "/node/actors/a/durable-dir"
	for _, tc := range []struct {
		name     string
		memory   int64
		mounts   []*ateworkerpb.VolumeMount
		wantAll  []specs.Mount
		wantEach []specs.Mount
	}{
		{
			name:   "memory limit",
			memory: 512 << 20,
			wantAll: []specs.Mount{
				{Destination: "/proc", Type: "proc", Source: "proc", Options: []string{"nosuid", "noexec"}},
				{Destination: "/dev/shm", Type: "tmpfs", Source: "shm", Options: []string{"nosuid", "noexec", "mode=1777", "size=65536k"}},
				{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: []string{"nosuid", "noexec", "ro"}},
				{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "mode=1777", "size=268435456"}},
			},
		},
		{
			name:   "minimum positive memory limit",
			memory: 1,
			wantEach: []specs.Mount{
				{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "mode=1777", "size=1"}},
			},
		},
		{
			name:   "no memory limit",
			memory: 0,
			wantEach: []specs.Mount{
				{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "mode=1777"}},
			},
		},
		{
			name:   "volumes at /dev/mqueue and /tmp",
			memory: 512 << 20,
			mounts: []*ateworkerpb.VolumeMount{
				{Name: "mq", MountPath: "/dev/mqueue"},
				{Name: "tmp", MountPath: "/tmp"},
			},
			wantEach: []specs.Mount{
				{Destination: "/dev/mqueue", Type: "bind", Source: durableDir + "/mq", Options: []string{"bind", "rw"}},
				{Destination: "/tmp", Type: "bind", Source: durableDir + "/tmp", Options: []string{"bind", "rw"}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actorDirs := &ateworkerpb.ActorDirs{DurableDirVolumeMountsDir: durableDir}
			spec := Build(Options{
				ActorDirs: actorDirs,
				Container: &ateworkerpb.Container{
					ContainerSpec:          &ateworkerpb.ContainerSpec{Args: []string{"/app"}},
					DurableDirVolumeMounts: tc.mounts,
				},
			})
			ShapeGVisor(spec, GVisorOptions{ActorUID: "uid-a", ActorDirs: actorDirs, ContainerName: "app", Size: sizing.SandboxSize{MemoryBytes: tc.memory}})
			if tc.wantAll != nil && !reflect.DeepEqual(spec.Mounts, tc.wantAll) {
				t.Errorf("Mounts =\n%+v\nwant\n%+v", spec.Mounts, tc.wantAll)
			}
			for _, want := range tc.wantEach {
				if got := mountFor(t, spec, want.Destination); !reflect.DeepEqual(got, want) {
					t.Errorf("%s =\n%+v\nwant\n%+v", want.Destination, got, want)
				}
			}
		})
	}
}
