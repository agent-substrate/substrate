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

package main

import (
	"math"
	"strings"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/utils/ptr"
)

// The device allowlist and CPU shares from defaultKataResources are the
// proven-good kata shape. Overlaying a memory limit must not drop them: a
// container without the allowlist fails in ways that do not point back here.
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

// assertDefaultDeviceAllowlist compares the entries, not just the count: a
// same-length slice of zero-valued or wrongly-populated rules would pass a
// length check while denying the devices a container needs.
func assertDefaultDeviceAllowlist(t *testing.T, got []specs.LinuxDeviceCgroup) {
	t.Helper()
	want := defaultKataResources().Devices
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

// Limits the guest can never satisfy must be rejected before the containers
// reach the agent, and as InvalidArgument: the template spec is immutable, so
// the failure is permanent and must not read as a server fault.
func TestCheckResourceEnvelope(t *testing.T) {
	mem := func(name string, bytes int64) actorContainer {
		return actorContainer{name: name, spec: &specs.Spec{Linux: &specs.Linux{
			Resources: &specs.LinuxResources{Memory: &specs.LinuxMemory{Limit: ptr.To(bytes)}},
		}}}
	}
	cpu := func(name string, quota int64, period uint64) actorContainer {
		c := &specs.LinuxCPU{Quota: ptr.To(quota)}
		if period > 0 {
			c.Period = ptr.To(period)
		}
		return actorContainer{name: name, spec: &specs.Spec{Linux: &specs.Linux{
			Resources: &specs.LinuxResources{CPU: c},
		}}}
	}
	const mib = 1024 * 1024

	tests := []struct {
		name    string
		ctrs    []actorContainer
		wantErr string
	}{{
		name: "within the envelope",
		ctrs: []actorContainer{mem("ok", 64*mib)},
	}, {
		name:    "memory over the guest",
		ctrs:    []actorContainer{mem("toobig", 4096*mib)},
		wantErr: "toobig",
	}, {
		name: "memory equal to the whole guest is allowed",
		ctrs: []actorContainer{mem("exact", 2048*mib)},
	}, {
		name:    "limits that fit alone but not together",
		ctrs:    []actorContainer{mem("a", 1536*mib), mem("b", 1024*mib)},
		wantErr: "in total",
	}, {
		name:    "cpu over the guest",
		ctrs:    []actorContainer{cpu("toofast", 400000, 100000)},
		wantErr: "toofast",
	}, {
		name:    "cpu summed over the guest",
		ctrs:    []actorContainer{cpu("a", 60000, 100000), cpu("b", 60000, 100000)},
		wantErr: "in total",
	}, {
		// A quota with no period must be read against the default, not skipped:
		// skipping it let an over-large limit past the guard entirely.
		name:    "cpu quota with no period is still checked",
		ctrs:    []actorContainer{cpu("noperiod", 400000, 0)},
		wantErr: "noperiod",
	}, {
		name:    "quota large enough to overflow the millis conversion",
		ctrs:    []actorContainer{cpu("huge", math.MaxInt64/100, 100000)},
		wantErr: "out of range",
	}, {
		name: "no limits",
		ctrs: []actorContainer{{name: "plain", spec: &specs.Spec{Linux: &specs.Linux{}}}},
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := checkResourceEnvelope(tc.ctrs, 2048, 1)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("checkResourceEnvelope() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("checkResourceEnvelope() = nil, want an error mentioning %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not mention %q", err, tc.wantErr)
			}
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Errorf("status code = %v, want InvalidArgument so a permanent misconfiguration does not read as a server fault", got)
			}
		})
	}
}
