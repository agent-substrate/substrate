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
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/opencontainers/runtime-spec/specs-go"
)

// The device allowlist and CPU shares from guestResources are the proven-good
// kata shape. Overlaying a memory limit must not drop them: a container without
// the allowlist fails in ways that do not point back here.
func TestMergeKataResources_KeepsDefaults(t *testing.T) {
	limit := int64(64 * 1024 * 1024)
	got := mergeKataResources(&specs.LinuxResources{
		Memory: &specs.LinuxMemory{Limit: &limit},
	})

	assertDefaultDeviceAllowlist(t, got.Devices)
	if got.CPU == nil || got.CPU.Shares == nil || *got.CPU.Shares != 1024 {
		t.Errorf("CPU.Shares = %v, want 1024", got.CPU)
	}
	if got.Memory == nil || got.Memory.Limit == nil || *got.Memory.Limit != limit {
		t.Errorf("Memory.Limit = %v, want %d", got.Memory, limit)
	}
}

func TestMergeKataResources_NilIsDefaults(t *testing.T) {
	got := mergeKataResources(nil)
	assertDefaultDeviceAllowlist(t, got.Devices)
	if got.Memory != nil {
		t.Errorf("Memory = %v, want nil when nothing was set", got.Memory)
	}
}

// A field the merge does not know about must reach the guest rather than being
// dropped: silently discarding an upstream addition is the failure this merge
// exists to prevent, and it would look identical to a runtime that ignores it.
func TestMergeKataResources_CarriesUnknownFields(t *testing.T) {
	pids := int64(128)
	nvidia := int64(195)
	got := mergeKataResources(&specs.LinuxResources{
		Pids: &specs.LinuxPids{Limit: &pids},
		Devices: []specs.LinuxDeviceCgroup{
			{Allow: true, Type: "c", Major: &nvidia, Access: "rwm"},
		},
	})

	if got.Pids == nil || got.Pids.Limit == nil || *got.Pids.Limit != pids {
		t.Errorf("Pids = %v, want the caller's limit %d to survive", got.Pids, pids)
	}
	if len(got.Devices) != 1 || got.Devices[0].Major == nil || *got.Devices[0].Major != nvidia {
		t.Errorf("Devices = %+v, want the caller's own allowlist to win", got.Devices)
	}
}

func TestMergeKataResources_CPUQuotaOverlaid(t *testing.T) {
	quota := int64(20000)
	period := uint64(100000)
	got := mergeKataResources(&specs.LinuxResources{
		CPU: &specs.LinuxCPU{Quota: &quota, Period: &period},
	})

	if got.CPU == nil || got.CPU.Quota == nil || *got.CPU.Quota != quota {
		t.Fatalf("CPU.Quota = %v, want %d", got.CPU, quota)
	}
	if got.CPU.Period == nil || *got.CPU.Period != period {
		t.Errorf("CPU.Period = %v, want %d", got.CPU.Period, period)
	}
	if got.CPU.Shares == nil || *got.CPU.Shares != 1024 {
		t.Errorf("CPU.Shares = %v, want the default 1024 to survive", got.CPU.Shares)
	}
}

// assertDefaultDeviceAllowlist compares the entries, not just the count: a
// same-length slice of zero-valued or wrongly-populated rules would pass a
// length check while denying the devices a container needs.
func assertDefaultDeviceAllowlist(t *testing.T, got []specs.LinuxDeviceCgroup) {
	t.Helper()
	want := guestResources().Devices
	if len(got) != len(want) {
		t.Fatalf("Devices = %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Allow != w.Allow || g.Type != w.Type || g.Access != w.Access {
			t.Errorf("Devices[%d] = {allow:%v type:%q access:%q}, want {allow:%v type:%q access:%q}",
				i, g.Allow, g.Type, g.Access, w.Allow, w.Type, w.Access)
			continue
		}
		if !eqInt64Ptr(g.Major, w.Major) || !eqInt64Ptr(g.Minor, w.Minor) {
			t.Errorf("Devices[%d] major/minor = %v/%v, want %v/%v (nil is the wildcard)",
				i, g.Major, g.Minor, w.Major, w.Minor)
		}
	}
}

func eqInt64Ptr(a, b *int64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// A container's own declared limit is what must bind inside the guest: it is
// the only input to the spec, so nothing can stamp over it and silently
// unbound the container.
func TestShapeMicroVM_KeepsDeclaredContainerLimits(t *testing.T) {
	const declared = 64 * 1024 * 1024
	spec := Build(Options{
		ActorUID: testActorUID, ContainerName: "app", Args: []string{"/app"},
		Resources: &ateletpb.ResourceLimits{MemoryBytes: declared},
	})
	if err := ShapeMicroVM(spec, MicroVMOptions{ActorUID: testActorUID, ContainerID: "app"}); err != nil {
		t.Fatalf("ShapeMicroVM() = %v", err)
	}

	if spec.Linux.Resources.Memory == nil || spec.Linux.Resources.Memory.Limit == nil {
		t.Fatal("memory limit = nil, want the declared 64Mi")
	}
	if v := *spec.Linux.Resources.Memory.Limit; v != declared {
		t.Errorf("memory limit = %d, want %d (the container's own declared limit)", v, declared)
	}
}

// A container that declares nothing must stay unbounded inside the guest: guest
// RAM is the real ceiling, and a cap equal to the whole guest can never bind.
func TestShapeMicroVM_LeavesUndeclaredContainerUnlimited(t *testing.T) {
	spec := Build(Options{ActorUID: testActorUID, ContainerName: "app", Args: []string{"/app"}})
	if err := ShapeMicroVM(spec, MicroVMOptions{ActorUID: testActorUID, ContainerID: "app"}); err != nil {
		t.Fatalf("ShapeMicroVM() = %v", err)
	}

	if m := spec.Linux.Resources.Memory; m != nil && m.Limit != nil && *m.Limit > 0 {
		t.Errorf("memory limit = %d, want unset for a container that declared none", *m.Limit)
	}
	if c := spec.Linux.Resources.CPU; c != nil && c.Quota != nil && *c.Quota > 0 {
		t.Errorf("cpu quota = %d, want unset for a container that declared none", *c.Quota)
	}
}
