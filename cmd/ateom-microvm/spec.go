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
	"fmt"
	"math"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
)

// guestEnvelope is the ceiling an actor's container limits must fit inside.
// declaredBytes is the actor-level memory limit (0 when the template declares
// none), and reserveMiB the guest RAM held back for the VMM; together they
// explain where memMiB came from, so the error can name the field the user can
// actually raise.
//
// memMiB is already net of reserveMiB, while vcpus is not. The asymmetry is
// deliberate: an unreduced memory limit would push the worker pod past its own
// and get it OOM-killed, whereas CPU is compressible, so the shortfall only
// slows the workload down. cloud-hypervisor's vCPU threads, virtiofsd and ateom
// are host processes drawing on the same worker-pod CPU quota, and the
// scheduler's capacity check sets none of it aside, so a container gets somewhat
// less CPU than it declared and the in-guest quota is throttled by the host
// before it binds. Carving out a CPU reserve is left for a follow-up.
type guestEnvelope struct {
	memMiB        int
	vcpus         int
	declaredBytes int64
	reserveMiB    int
}

// remedy names the field that raises the memory ceiling.
func (e guestEnvelope) remedy() string {
	if e.declaredBytes > 0 {
		return fmt.Sprintf("raise spec.resources.limits.memory (declared %dMiB, less the %dMiB VMM reserve) or lower the container limits",
			e.declaredBytes/(1024*1024), e.reserveMiB)
	}
	return "lower the limits or use a SandboxConfig with a larger guest"
}

// cpuRemedy names the field that raises the vCPU ceiling. Unlike remedy, it
// takes no reserve into account: the VMM reserve applies to guest memory, not
// vCPU count (see guestEnvelope).
func (e guestEnvelope) cpuRemedy() string {
	return "raise spec.resources.limits.cpu or lower the container limits"
}

// checkResourceEnvelope rejects limits the guest can never satisfy. The guest is
// sized from the actor's own declared limits, or from the pool's SandboxConfig
// when the template declares none, so a limit above the guest can never bind:
// the container would hit the guest's own ceiling instead, with an error
// pointing nowhere useful.
//
// Limits are summed across the actor's containers rather than checked one at a
// time, because they share one guest. Errors carry codes.InvalidArgument: the
// template spec is immutable, so this can never succeed on a retry and must not
// read as a server fault.
func checkResourceEnvelope(ctrs []actorContainer, env guestEnvelope) error {
	guestBytes := int64(env.memMiB) * 1024 * 1024
	guestMillis := int64(env.vcpus) * 1000

	var totalBytes, totalMillis int64
	for _, c := range ctrs {
		if c.spec == nil || c.spec.Linux == nil || c.spec.Linux.Resources == nil {
			continue
		}
		r := c.spec.Linux.Resources
		// A non-positive limit means "unlimited" in the OCI spec, so it is not a
		// claim on the guest: skip it rather than summing it, which would let a
		// negative offset another container's limit and slip the total through.
		// cpuLimitMillis treats a non-positive quota the same way.
		if r.Memory != nil && r.Memory.Limit != nil && *r.Memory.Limit > 0 {
			limit := *r.Memory.Limit
			if limit > guestBytes {
				return status.Errorf(codes.InvalidArgument,
					"container %q asks for %d bytes of memory but the guest has %d MiB; %s",
					c.name, limit, env.memMiB, env.remedy())
			}
			totalBytes += limit
		}
		millis, err := cpuLimitMillis(c.name, r.CPU)
		if err != nil {
			return err
		}
		if millis > guestMillis {
			return status.Errorf(codes.InvalidArgument,
				"container %q asks for %dm CPU but the guest has %d vCPU; %s",
				c.name, millis, env.vcpus, env.cpuRemedy())
		}
		totalMillis += millis
	}

	if totalBytes > guestBytes {
		return status.Errorf(codes.InvalidArgument,
			"the actor's containers ask for %d bytes of memory in total but the guest has %d MiB; %s",
			totalBytes, env.memMiB, env.remedy())
	}
	if totalMillis > guestMillis {
		return status.Errorf(codes.InvalidArgument,
			"the actor's containers ask for %dm CPU in total but the guest has %d vCPU; %s",
			totalMillis, env.vcpus, env.cpuRemedy())
	}
	return nil
}

// cpuLimitMillis converts a container's CFS quota back to milli-cores. A quota
// with no period is read against the default the quota is expressed against
// rather than skipped, so a spec that omits it cannot slip past the envelope.
func cpuLimitMillis(name string, cpu *specs.LinuxCPU) (int64, error) {
	if cpu == nil || cpu.Quota == nil || *cpu.Quota <= 0 {
		return 0, nil
	}
	period := int64(kata.DefaultCPUPeriodUS)
	if cpu.Period != nil && *cpu.Period > 0 {
		if *cpu.Period > math.MaxInt64 {
			return 0, status.Errorf(codes.InvalidArgument,
				"container %q has a cpu period of %d, which is out of range", name, *cpu.Period)
		}
		period = int64(*cpu.Period)
	}
	quota := *cpu.Quota
	if quota > math.MaxInt64/1000 {
		return 0, status.Errorf(codes.InvalidArgument,
			"container %q has a cpu quota of %d, which is out of range", name, quota)
	}
	return quota * 1000 / period, nil
}
