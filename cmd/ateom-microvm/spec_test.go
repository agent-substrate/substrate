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
	"path/filepath"
	"strings"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/utils/ptr"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/ocispec"
)

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

// A volume's path relative to the share is the same on the host and in the
// guest.
func TestVolumeSubtreePathsMatchTheGuestShare(t *testing.T) {
	const uid, cid, vol = "uid", "app", "data"
	for name, tc := range map[string]struct{ host, guest string }{
		"durable":     {filepath.Join(kata.SharedDir(uid), ocispec.ShareDurable, vol), filepath.Join(ocispec.GuestSharedDir, ocispec.ShareDurable, vol)},
		"csi":         {filepath.Join(kata.SharedDir(uid), ocispec.ShareCSI, vol), filepath.Join(ocispec.GuestSharedDir, ocispec.ShareCSI, vol)},
		"system-info": {filepath.Join(kata.SharedDir(uid), ocispec.ShareSystemInfo, vol), filepath.Join(ocispec.GuestSharedDir, ocispec.ShareSystemInfo, vol)},
		"image":       {kata.SharedVolumeDir(uid, cid, vol), filepath.Join(ocispec.GuestSharedDir, cid, ocispec.ShareVolumes, vol)},
	} {
		hostRel, err := filepath.Rel(kata.SharedDir(uid), tc.host)
		if err != nil {
			t.Fatalf("%s: Rel(host): %v", name, err)
		}
		guestRel := strings.TrimPrefix(tc.guest, ocispec.GuestSharedDir+"/")
		if hostRel != guestRel {
			t.Errorf("%s: host-relative path %q != guest-relative path %q; find-paths would re-open the wrong file", name, hostRel, guestRel)
		}
	}
}
