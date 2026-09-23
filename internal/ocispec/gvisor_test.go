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
	"strings"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func TestShapeGVisorGivesEachActorItsOwnCgroupLeaf(t *testing.T) {
	shape := func(actorUID, containerName string) string {
		spec := &specs.Spec{}
		ShapeGVisor(spec, GVisorOptions{ActorUID: actorUID, ContainerName: containerName})
		return spec.Linux.CgroupsPath
	}

	first := shape("uid-a", PauseContainer)
	second := shape("uid-b", PauseContainer)
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

	if a, b := shape("uid-a", PauseContainer), shape("uid-a", "app"); a == b {
		t.Errorf("two containers of one actor share the leaf %q", a)
	}
}

func TestGVisorCgroupLeafMatchesTheShapedPath(t *testing.T) {
	spec := &specs.Spec{}
	ShapeGVisor(spec, GVisorOptions{ActorUID: "uid-a", ContainerName: PauseContainer})
	if want := "/" + GVisorCgroupLeaf("uid-a", PauseContainer); spec.Linux.CgroupsPath != want {
		t.Errorf("shaped cgroupsPath = %q, want %q", spec.Linux.CgroupsPath, want)
	}
}

func TestShapeGVisorBindsTheNamedResolvConf(t *testing.T) {
	for _, tc := range []struct {
		name       string
		resolvConf string
		wantSource string
	}{
		{name: "default is the worker pod's", wantSource: "/etc/resolv.conf"},
		{
			name:       "a sandbox may name its own",
			resolvConf: "/var/lib/ateom-gvisor/actors/a/resolv.conf",
			wantSource: "/var/lib/ateom-gvisor/actors/a/resolv.conf",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := &specs.Spec{}
			ShapeGVisor(spec, GVisorOptions{
				ActorUID: "a", ContainerName: "app", ResolvConf: tc.resolvConf,
			})
			var got string
			for _, m := range spec.Mounts {
				if m.Destination == "/etc/resolv.conf" {
					got = m.Source
				}
			}
			if got != tc.wantSource {
				t.Errorf("resolv.conf bound from %q, want %q", got, tc.wantSource)
			}
		})
	}
}
