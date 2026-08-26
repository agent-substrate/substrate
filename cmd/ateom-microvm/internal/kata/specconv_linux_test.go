//go:build linux

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

package kata

import (
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// The agent applies cgroup limits from the spec it is sent, so anything this
// conversion drops is silently unenforced in the guest.
func TestSpecToAgentPB_ResourceLimits(t *testing.T) {
	memLimit := int64(67108864)
	quota := int64(20000)
	period := uint64(100000)
	shares := uint64(1024)

	got := SpecToAgentPB(&specs.Spec{
		Linux: &specs.Linux{
			Resources: &specs.LinuxResources{
				Memory: &specs.LinuxMemory{Limit: &memLimit},
				CPU:    &specs.LinuxCPU{Shares: &shares, Quota: &quota, Period: &period},
			},
		},
	})

	if got.Linux == nil || got.Linux.Resources == nil {
		t.Fatalf("Linux.Resources = nil, want the limits carried through")
	}
	r := got.Linux.Resources

	if r.Memory == nil {
		t.Fatalf("Resources.Memory = nil, want a limit of %d", memLimit)
	}
	if r.Memory.Limit != memLimit {
		t.Errorf("Memory.Limit = %d, want %d", r.Memory.Limit, memLimit)
	}
	if r.CPU == nil {
		t.Fatal("Resources.CPU = nil, want cpu limits")
	}
	if r.CPU.Quota != quota {
		t.Errorf("CPU.Quota = %d, want %d", r.CPU.Quota, quota)
	}
	if r.CPU.Period != period {
		t.Errorf("CPU.Period = %d, want %d", r.CPU.Period, period)
	}
	if r.CPU.Shares != shares {
		t.Errorf("CPU.Shares = %d, want %d to still be carried", r.CPU.Shares, shares)
	}
}

// A spec with no memory or quota must not gain empty sub-messages, so specs
// that declare no limits convert exactly as they did before.
func TestSpecToAgentPB_NoLimitsUnchanged(t *testing.T) {
	shares := uint64(1024)
	got := SpecToAgentPB(&specs.Spec{
		Linux: &specs.Linux{
			Resources: &specs.LinuxResources{
				CPU: &specs.LinuxCPU{Shares: &shares},
			},
		},
	})

	r := got.Linux.Resources
	if r.Memory != nil {
		t.Errorf("Resources.Memory = %v, want nil when the spec sets none", r.Memory)
	}
	if r.CPU == nil || r.CPU.Shares != shares {
		t.Errorf("CPU = %v, want shares %d", r.CPU, shares)
	}
	if r.CPU.Quota != 0 || r.CPU.Period != 0 {
		t.Errorf("CPU quota/period = %d/%d, want 0/0 when unset", r.CPU.Quota, r.CPU.Period)
	}
}

// period is optional in OCI but a plain uint64 on the wire, so a quota with no
// period would reach the agent as an unsatisfiable "quota per zero period".
func TestSpecToAgentPB_QuotaWithoutPeriodGetsDefault(t *testing.T) {
	quota := int64(20000)
	got := SpecToAgentPB(&specs.Spec{
		Linux: &specs.Linux{
			Resources: &specs.LinuxResources{CPU: &specs.LinuxCPU{Quota: &quota}},
		},
	})

	cpu := got.Linux.Resources.CPU
	if cpu == nil || cpu.Quota != quota {
		t.Fatalf("CPU = %v, want quota %d", cpu, quota)
	}
	if cpu.Period == 0 {
		t.Error("CPU.Period = 0 with a live quota, want the CFS default")
	}
	if cpu.Period != DefaultCPUPeriodUS {
		t.Errorf("CPU.Period = %d, want DefaultCPUPeriodUS (%d)", cpu.Period, DefaultCPUPeriodUS)
	}
}

// Quota and period are plain integers on the wire, where zero is
// indistinguishable from unset. A non-positive quota is "unlimited" in the OCI
// spec, so it is dropped rather than sent as a zero the guest would apply as no
// CPU at all; a zero period is dropped so a live quota keeps the CFS default it
// is expressed against.
func TestSpecToAgentPB_NonPositiveCPUValuesAreDropped(t *testing.T) {
	quota := int64(20000)
	zeroQuota := int64(0)
	negQuota := int64(-1)
	zeroPeriod := uint64(0)
	livePeriod := uint64(50000)

	tests := []struct {
		name       string
		cpu        *specs.LinuxCPU
		wantQuota  int64
		wantPeriod uint64
	}{{
		name:       "zero period keeps the CFS default",
		cpu:        &specs.LinuxCPU{Quota: &quota, Period: &zeroPeriod},
		wantQuota:  quota,
		wantPeriod: DefaultCPUPeriodUS,
	}, {
		name:       "zero quota is unlimited",
		cpu:        &specs.LinuxCPU{Quota: &zeroQuota, Period: &livePeriod},
		wantQuota:  0,
		wantPeriod: livePeriod,
	}, {
		name:       "negative quota is unlimited",
		cpu:        &specs.LinuxCPU{Quota: &negQuota, Period: &livePeriod},
		wantQuota:  0,
		wantPeriod: livePeriod,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SpecToAgentPB(&specs.Spec{
				Linux: &specs.Linux{Resources: &specs.LinuxResources{CPU: tt.cpu}},
			})
			cpu := got.Linux.Resources.CPU
			if cpu == nil {
				t.Fatal("CPU = nil, want a converted cpu block")
			}
			if cpu.Quota != tt.wantQuota {
				t.Errorf("CPU.Quota = %d, want %d", cpu.Quota, tt.wantQuota)
			}
			if cpu.Period != tt.wantPeriod {
				t.Errorf("CPU.Period = %d, want %d", cpu.Period, tt.wantPeriod)
			}
		})
	}
}
