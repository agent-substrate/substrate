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

// runsc mounts its own filesystems at /dev and /dev/pts and warns about the
// nodev and strictatime options, so the gVisor spec leaves them out.
func TestShapeGVisor_SystemMounts(t *testing.T) {
	spec := Build(Options{Container: &ateworkerpb.Container{ContainerSpec: &ateworkerpb.ContainerSpec{Args: []string{"/app"}}}})
	ShapeGVisor(spec, GVisorOptions{ActorUID: "uid-a", ContainerName: "app"})
	want := []specs.Mount{
		{Destination: "/proc", Type: "proc", Source: "proc", Options: []string{"nosuid", "noexec"}},
		{Destination: "/dev/shm", Type: "tmpfs", Source: "shm", Options: []string{"nosuid", "noexec", "mode=1777", "size=65536k"}},
		{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: []string{"nosuid", "noexec", "ro"}},
		{Destination: "/run", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "mode=755", "size=65536k"}},
		{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "mode=1777"}},
	}
	if !reflect.DeepEqual(spec.Mounts, want) {
		t.Errorf("Mounts =\n%+v\nwant\n%+v", spec.Mounts, want)
	}
}
