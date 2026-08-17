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
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/utils/ptr"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
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
		// A non-positive limit is "unlimited" in the OCI spec, not a claim on the
		// guest, so it must not be summed: counting it would net these three down
		// to 1024MiB against a 2048MiB guest and let the 2560MiB overrun through.
		name:    "an unlimited sibling cannot offset limits that overrun",
		ctrs:    []actorContainer{mem("a", 1536*mib), mem("b", 1024*mib), mem("unlimited", -1536*mib)},
		wantErr: "in total",
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
			err := checkResourceEnvelope(tc.ctrs, guestEnvelope{memMiB: 2048, vcpus: 1})
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

// When the actor declared its own size, the guest ceiling comes from that limit
// minus the VMM reserve, so the error must point at the actor's limit rather
// than at the SandboxConfig the user cannot usefully change.
func TestCheckResourceEnvelope_ErrorNamesActorLimitWhenDeclared(t *testing.T) {
	const mib = 1024 * 1024
	ctr := actorContainer{name: "hog", spec: &specs.Spec{Linux: &specs.Linux{
		Resources: &specs.LinuxResources{Memory: &specs.LinuxMemory{Limit: ptr.To(int64(2048 * mib))}},
	}}}

	err := checkResourceEnvelope([]actorContainer{ctr}, guestEnvelope{
		memMiB: 768, vcpus: 1, declaredBytes: 1024 * mib, reserveMiB: 256,
	})
	if err == nil {
		t.Fatal("checkResourceEnvelope() = nil, want an error")
	}
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", got)
	}
	for _, want := range []string{"hog", "1024", "256", "spec.resources.limits.memory"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err.Error(), want)
		}
	}
}

// A CPU-shortfall error must name spec.resources.limits.cpu, not
// spec.resources.limits.memory, even when the actor also declares a memory
// limit: raising memory cannot change the vCPU count.
func TestCheckResourceEnvelope_CPUErrorNamesCPULimitEvenWithDeclaredMemory(t *testing.T) {
	const mib = 1024 * 1024
	period := uint64(kata.DefaultCPUPeriodUS)
	quota := int64(2000 * kata.DefaultCPUPeriodUS / 1000) // 2000m
	ctr := actorContainer{name: "hog", spec: &specs.Spec{Linux: &specs.Linux{
		Resources: &specs.LinuxResources{CPU: &specs.LinuxCPU{Quota: &quota, Period: &period}},
	}}}

	err := checkResourceEnvelope([]actorContainer{ctr}, guestEnvelope{
		memMiB: 2048, vcpus: 1, declaredBytes: 2048 * mib, reserveMiB: 256,
	})
	if err == nil {
		t.Fatal("checkResourceEnvelope() = nil, want an error")
	}
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", got)
	}
	if !strings.Contains(err.Error(), "spec.resources.limits.cpu") {
		t.Errorf("error %q does not mention spec.resources.limits.cpu", err.Error())
	}
	if strings.Contains(err.Error(), "spec.resources.limits.memory") {
		t.Errorf("error %q unexpectedly mentions spec.resources.limits.memory", err.Error())
	}
}

// The ceiling containers are measured against is the post-reserve guest, not the
// declared limit and not the SandboxConfig default. RunWorkload gets there by
// calling resolveGuestMemMiB before checkResourceEnvelope; this pins the
// arithmetic those two steps compose into, which nothing else asserts.
func TestCheckResourceEnvelope_MeasuresAgainstPostReserveGuest(t *testing.T) {
	const mib = 1024 * 1024
	const declaredMiB, reserveMiB = 1024, 256

	guestMiB, err := resolveGuestMemMiB(int64(declaredMiB)*mib, reserveMiB, 2048)
	if err != nil {
		t.Fatalf("resolveGuestMemMiB() = %v", err)
	}
	if guestMiB != declaredMiB-reserveMiB {
		t.Fatalf("guest = %dMiB, want %dMiB (declared minus reserve)", guestMiB, declaredMiB-reserveMiB)
	}

	// A container asking for the full declared limit does not fit the guest,
	// because the reserve is not the container's to spend.
	ctr := actorContainer{name: "hog", spec: &specs.Spec{Linux: &specs.Linux{
		Resources: &specs.LinuxResources{Memory: &specs.LinuxMemory{Limit: ptr.To(int64(declaredMiB) * mib)}},
	}}}
	env := guestEnvelope{memMiB: guestMiB, vcpus: 1, declaredBytes: int64(declaredMiB) * mib, reserveMiB: reserveMiB}
	if err := checkResourceEnvelope([]actorContainer{ctr}, env); err == nil {
		t.Error("checkResourceEnvelope() = nil, want an error: the declared limit does not fit once the reserve is held back")
	}
}

// With no actor-level limit the guest is the SandboxConfig default, so that
// remains the right thing to point at.
func TestCheckResourceEnvelope_ErrorNamesSandboxConfigWhenUndeclared(t *testing.T) {
	const mib = 1024 * 1024
	ctr := actorContainer{name: "hog", spec: &specs.Spec{Linux: &specs.Linux{
		Resources: &specs.LinuxResources{Memory: &specs.LinuxMemory{Limit: ptr.To(int64(4096 * mib))}},
	}}}

	err := checkResourceEnvelope([]actorContainer{ctr}, guestEnvelope{memMiB: 2048, vcpus: 1})
	if err == nil {
		t.Fatal("checkResourceEnvelope() = nil, want an error")
	}
	if !strings.Contains(err.Error(), "SandboxConfig") {
		t.Errorf("error %q does not mention SandboxConfig", err.Error())
	}
}

// A container's own declared limit is what must bind inside the guest: it is
// the only input to the spec, so nothing can stamp over it and silently
// unbound the container.
func TestEnsureKataCompatibleSpec_KeepsDeclaredContainerLimits(t *testing.T) {
	const declared = 64 * 1024 * 1024
	bundle := t.TempDir()
	in := specs.Spec{Linux: &specs.Linux{Resources: &specs.LinuxResources{
		Memory: &specs.LinuxMemory{Limit: ptr.To(int64(declared))},
	}}}
	b, err := json.Marshal(&in)
	if err != nil {
		t.Fatalf("marshaling input spec: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "config.json"), b, 0o600); err != nil {
		t.Fatalf("writing config.json: %v", err)
	}

	got, err := ensureKataCompatibleSpec(bundle, "actor-uid", "/proc/1/ns/net")
	if err != nil {
		t.Fatalf("ensureKataCompatibleSpec() = %v", err)
	}

	if got.Linux.Resources.Memory == nil || got.Linux.Resources.Memory.Limit == nil {
		t.Fatal("memory limit = nil, want the declared 64Mi")
	}
	if v := *got.Linux.Resources.Memory.Limit; v != declared {
		t.Errorf("memory limit = %d, want %d (the container's own declared limit)", v, declared)
	}
}

// A container that declares nothing must stay unbounded inside the guest: guest
// RAM is the real ceiling, and a cap equal to the whole guest can never bind.
func TestEnsureKataCompatibleSpec_LeavesUndeclaredContainerUnlimited(t *testing.T) {
	bundle := t.TempDir()
	in := specs.Spec{Linux: &specs.Linux{}}
	b, err := json.Marshal(&in)
	if err != nil {
		t.Fatalf("marshaling input spec: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "config.json"), b, 0o600); err != nil {
		t.Fatalf("writing config.json: %v", err)
	}

	got, err := ensureKataCompatibleSpec(bundle, "actor-uid", "/proc/1/ns/net")
	if err != nil {
		t.Fatalf("ensureKataCompatibleSpec() = %v", err)
	}

	if m := got.Linux.Resources.Memory; m != nil && m.Limit != nil && *m.Limit > 0 {
		t.Errorf("memory limit = %d, want unset for a container that declared none", *m.Limit)
	}
	if c := got.Linux.Resources.CPU; c != nil && c.Quota != nil && *c.Quota > 0 {
		t.Errorf("cpu quota = %d, want unset for a container that declared none", *c.Quota)
	}
}
