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

// Package sizing right-sizes a sandbox to the actor's declared resource limits.
// The actor's limits arrive over the ateom RPCs (RunWorkload / RestoreWorkload)
// as a SandboxSize. ateom-gvisor calls ApplyToOCISpec to write them into the
// sandbox container's OCI spec, which runsc then applies to the host cgroup
// leaf. ateom-microvm does not call ApplyToOCISpec: it uses SandboxSize only to
// size the VM itself (VCPUs, and MemoryBytes via resolveGuestMemMiB). A
// micro-VM container's own cgroup limit comes instead from that container's
// declared `spec.containers[].resources`, written into its OCI spec by atelet.
package sizing

import (
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

const (
	// cpuQuotaPeriodMicros is the cgroup v2 cpu.max period (100ms, the kernel
	// default) against which the CPU quota is expressed.
	cpuQuotaPeriodMicros = 100000
)

// SandboxSize is the sandbox's target size, derived from the actor's declared
// resource limits. A zero field means "unset": the caller keeps its own default
// (the kata config for the micro-VM, unlimited for gVisor).
type SandboxSize struct {
	// MilliCPU is the CPU limit in millicores (1000 = one core), or 0 if unset.
	MilliCPU int64
	// MemoryBytes is the memory limit in bytes, or 0 if unset.
	MemoryBytes int64
}

// FromLimits builds a SandboxSize from an actor's declared limits (millicores and
// bytes) as carried on the ateom RPCs. It is runtime-agnostic; both ateom-gvisor
// and ateom-microvm call it. Negative values are clamped to zero ("unset").
func FromLimits(milliCPU, memoryBytes int64) SandboxSize {
	if milliCPU < 0 {
		milliCPU = 0
	}
	if memoryBytes < 0 {
		memoryBytes = 0
	}
	return SandboxSize{MilliCPU: milliCPU, MemoryBytes: memoryBytes}
}

// VCPUs converts the CPU limit to a whole vCPU count for the micro-VM, rounding
// up so a fractional limit still yields a usable core (minimum 1 when a limit is
// set). Returns 0 when the CPU limit is unset, letting the caller keep its
// default.
func (s SandboxSize) VCPUs() int {
	if s.MilliCPU <= 0 {
		return 0
	}
	v := (s.MilliCPU + 999) / 1000
	if v < 1 {
		v = 1
	}
	return int(v)
}

// ApplyToOCISpec writes the pod's CPU/memory limits into the container OCI spec's
// linux.resources so the sandbox cgroup is created with the right values. This is
// the gVisor sandbox-cgroup path: only ateom-gvisor calls it, so runsc applies the
// result to the host cgroup leaf. Fields that are unset in SandboxSize are left
// untouched, preserving any existing values on the spec.
func (s SandboxSize) ApplyToOCISpec(spec *specs.Spec) {
	if s.MilliCPU <= 0 && s.MemoryBytes <= 0 {
		return
	}
	if spec.Linux == nil {
		spec.Linux = &specs.Linux{}
	}
	if spec.Linux.Resources == nil {
		spec.Linux.Resources = &specs.LinuxResources{}
	}
	if s.MilliCPU > 0 {
		if spec.Linux.Resources.CPU == nil {
			spec.Linux.Resources.CPU = &specs.LinuxCPU{}
		}
		period := uint64(cpuQuotaPeriodMicros)
		quota := s.MilliCPU * cpuQuotaPeriodMicros / 1000
		spec.Linux.Resources.CPU.Period = &period
		spec.Linux.Resources.CPU.Quota = &quota
	}
	if s.MemoryBytes > 0 {
		if spec.Linux.Resources.Memory == nil {
			spec.Linux.Resources.Memory = &specs.LinuxMemory{}
		}
		limit := s.MemoryBytes
		spec.Linux.Resources.Memory.Limit = &limit
	}
}
