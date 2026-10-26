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
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/opencontainers/runtime-spec/specs-go"

	"go.ate.dev/substrate/internal/sizing"
)

const kataTestUID = "0b1a6a3c-7d5e-4f8a-9c2d-3e4f5a6b7c8d"

func TestKataContainerID(t *testing.T) {
	tests := []struct {
		name    string
		ctr     string
		bootsVM bool
		want    string
	}{
		{"boots the VM", "app", true, kataTestUID},
		{"joins the VM", "app", false, "c_app"},
		{"one letter name", "w", false, "c_w"},
		{"longest name", strings.Repeat("a", 63), false, "c_" + strings.Repeat("a", 63)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := KataContainerID(kataTestUID, tt.ctr, tt.bootsVM)
			if got != tt.want {
				t.Errorf("KataContainerID() = %q, want %q", got, tt.want)
			}
			if !kataID.MatchString(got) {
				t.Errorf("KataContainerID() = %q, which kata rejects", got)
			}
		})
	}
}

func TestShapeKata(t *testing.T) {
	tests := []struct {
		name          string
		options       KataOptions
		wantType      string
		wantCgroup    string
		wantContainer string
	}{
		{
			name:          "container that boots the VM",
			options:       KataOptions{ActorUID: kataTestUID, ContainerName: "app", BootsVM: true},
			wantType:      "sandbox",
			wantCgroup:    "/" + kataTestUID,
			wantContainer: "app",
		},
		{
			name:          "container that joins the VM",
			options:       KataOptions{ActorUID: kataTestUID, ContainerName: "sidecar"},
			wantType:      "container",
			wantCgroup:    "/" + kataTestUID + "-sidecar",
			wantContainer: "sidecar",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := Build(parityOptions)
			ShapeKata(spec, tt.options)

			wantAnnotations := map[string]string{
				"io.kubernetes.cri.container-name": tt.wantContainer,
				"io.kubernetes.cri.container-type": tt.wantType,
				"io.kubernetes.cri.sandbox-id":     kataTestUID,
			}
			if diff := cmp.Diff(wantAnnotations, spec.Annotations); diff != "" {
				t.Errorf("annotations mismatch (-want +got):\n%s", diff)
			}
			if spec.Linux.CgroupsPath != tt.wantCgroup {
				t.Errorf("cgroupsPath = %q, want %q", spec.Linux.CgroupsPath, tt.wantCgroup)
			}
			assertDefaultDeviceAllowlist(t, spec.Linux.Resources.Devices)

			// The guest's system mounts replace the host's, and every volume
			// keeps its host source for the shim to share.
			var dests []string
			for _, m := range spec.Mounts {
				dests = append(dests, m.Destination)
			}
			for _, want := range append([]string{"/proc", "/dev", "/dev/pts", "/dev/shm", "/sys", "/run"}, parityMountPaths...) {
				if !slices.Contains(dests, want) {
					t.Errorf("mounts = %v, want one at %s", dests, want)
				}
			}
			for _, dest := range parityMountPaths {
				m := mountFor(t, spec, dest)
				if m.Type != "bind" || !strings.HasPrefix(m.Source, "/node/actors/a/") {
					t.Errorf("mount %s = %+v, want a bind with its host source", dest, m)
				}
			}
			if got := mountFor(t, spec, "/dev").Options; !slices.Contains(got, "mode=755") {
				t.Errorf("/dev options = %v, want the guest tmpfs options", got)
			}
		})
	}
}

// Shaping again must not add mounts.
func TestShapeKata_Idempotent(t *testing.T) {
	spec := Build(parityOptions)
	o := KataOptions{ActorUID: kataTestUID, ContainerName: "app", BootsVM: true}
	ShapeKata(spec, o)
	first := slices.Clone(spec.Mounts)
	ShapeKata(spec, o)
	if diff := cmp.Diff(first, spec.Mounts); diff != "" {
		t.Errorf("mounts changed on a second shaping (-first +second):\n%s", diff)
	}
}

// The container's own limits survive the merge with Kata's defaults.
func TestShapeKata_KeepsContainerLimits(t *testing.T) {
	limit := int64(64 << 20)
	spec := &specs.Spec{Linux: &specs.Linux{Resources: &specs.LinuxResources{Memory: &specs.LinuxMemory{Limit: &limit}}}}
	ShapeKata(spec, KataOptions{ActorUID: kataTestUID, ContainerName: "app", BootsVM: true})
	if got := spec.Linux.Resources.Memory; got == nil || *got.Limit != limit {
		t.Errorf("memory = %v, want limit %d", got, limit)
	}
}

// The actor's size sizes the VM through the CRI sandbox annotations, which the
// shim reads at boot, and the container's limits in the guest.
func TestShapeKata_ActorSize(t *testing.T) {
	size := sizing.SandboxSize{MilliCPU: 2500, MemoryBytes: 512 << 20}
	tests := []struct {
		name        string
		options     KataOptions
		wantSizing  map[string]string
		wantMemory  bool
		wantCPUQuot int64
	}{
		{
			name:    "container that boots the VM",
			options: KataOptions{ActorUID: kataTestUID, ContainerName: "app", BootsVM: true, Size: size},
			wantSizing: map[string]string{
				"io.kubernetes.cri.sandbox-cpu-period": "100000",
				"io.kubernetes.cri.sandbox-cpu-quota":  "250000",
				"io.kubernetes.cri.sandbox-memory":     "536870912",
			},
			wantMemory:  true,
			wantCPUQuot: 250000,
		},
		{
			name:    "container that joins the VM",
			options: KataOptions{ActorUID: kataTestUID, ContainerName: "side", Size: size},
		},
		{
			name:    "size unset",
			options: KataOptions{ActorUID: kataTestUID, ContainerName: "app", BootsVM: true},
		},
		{
			name:    "memory only",
			options: KataOptions{ActorUID: kataTestUID, ContainerName: "app", BootsVM: true, Size: sizing.SandboxSize{MemoryBytes: 1 << 30}},
			wantSizing: map[string]string{
				"io.kubernetes.cri.sandbox-memory": "1073741824",
			},
			wantMemory: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := Build(parityOptions)
			ShapeKata(spec, tt.options)

			got := map[string]string{}
			for k, v := range spec.Annotations {
				if strings.HasPrefix(k, "io.kubernetes.cri.sandbox-") && k != "io.kubernetes.cri.sandbox-id" {
					got[k] = v
				}
			}
			if len(got) == 0 {
				got = nil
			}
			if diff := cmp.Diff(tt.wantSizing, got); diff != "" {
				t.Errorf("sizing annotations mismatch (-want +got):\n%s", diff)
			}
			r := spec.Linux.Resources
			if gotMemory := r.Memory != nil && r.Memory.Limit != nil; gotMemory != tt.wantMemory {
				t.Errorf("memory limit set = %v, want %v", gotMemory, tt.wantMemory)
			}
			var quota int64
			if r.CPU != nil && r.CPU.Quota != nil {
				quota = *r.CPU.Quota
			}
			if quota != tt.wantCPUQuot {
				t.Errorf("cpu quota = %d, want %d", quota, tt.wantCPUQuot)
			}
		})
	}
}
