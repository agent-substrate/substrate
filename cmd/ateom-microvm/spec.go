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
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
)

// ensureKataCompatibleSpec augments the bundle's config.json with the fields
// kata's OCI conversion requires but atelet's (gVisor-oriented) spec omits.
// Without linux.resources, kata's ContainerConfig nil-derefs and the shim
// crashes. This shaper is a bridge; a future atelet change should emit
// runtime-appropriate specs so it can retire.
func ensureKataCompatibleSpec(bundle, id, netnsPath string) (*specs.Spec, error) {
	specPath := filepath.Join(bundle, "config.json")
	b, err := os.ReadFile(specPath)
	if err != nil {
		return nil, fmt.Errorf("reading %q: %w", specPath, err)
	}
	var spec specs.Spec
	if err := json.Unmarshal(b, &spec); err != nil {
		return nil, fmt.Errorf("parsing %q: %w", specPath, err)
	}

	if spec.Linux == nil {
		spec.Linux = &specs.Linux{}
	}
	spec.Linux.Resources = mergeKataResources(spec.Linux.Resources)
	if spec.Linux.CgroupsPath == "" {
		spec.Linux.CgroupsPath = "/ateomchv/" + id
	}

	// atelet's spec carries gVisor pause-model CRI annotations
	// (container-type=container, sandbox-id=pause). kata reads those and waits
	// for a separate "pause" sandbox that we never create, failing with "the
	// sandbox hasn't been created". Strip them so kata treats this single
	// container as its own sandbox (creates the VM), as in the integration tests.
	for k := range spec.Annotations {
		if strings.HasPrefix(k, "io.kubernetes.cri.") {
			delete(spec.Annotations, k)
		}
	}

	// NB: no overlay-related annotations here. The rootfs overlay is assembled on
	// the HOST (see kata.StageMergedRootfs); this spec is used directly for the
	// container the kata-agent runs on the shared merged tree (see RunWorkload) —
	// stock agent, no patched shim.

	// Point the network namespace at our interior netns (which holds the pod's
	// eth0); kata finds eth0 there and wires it to the VM's virtio-net.
	netnsSet := false
	for i := range spec.Linux.Namespaces {
		if spec.Linux.Namespaces[i].Type == specs.NetworkNamespace {
			spec.Linux.Namespaces[i].Path = netnsPath
			netnsSet = true
		}
	}
	if !netnsSet {
		spec.Linux.Namespaces = append(spec.Linux.Namespaces, specs.LinuxNamespace{
			Type: specs.NetworkNamespace, Path: netnsPath,
		})
	}

	// Replace atelet's gVisor-oriented mounts (minimal /dev tmpfs, a
	// /etc/resolv.conf host bind that ENOENTs against the distroless rootfs) with
	// the exact set `ctr run --runtime io.containerd.kata.v2` emits, which kata's
	// agent accepts. (Static shaper; pod DNS integration is future work.)
	//
	// Dropping atelet's volume bind mounts here is fine: host-path binds can't
	// attach inside the guest anyway. Volumes reach micro-VM containers as
	// subtrees of the single per-actor virtio-fs share instead — durable-dir
	// volumes (writable, durable.go), CSI volumes (csi.go), and system-info
	// volumes (read-only, systeminfo.go) — with the binds added to the
	// workload specs ateom drives through the kata-agent (see workloadSpec).
	spec.Mounts = defaultKataMounts()

	out, err := json.MarshalIndent(&spec, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshaling spec: %w", err)
	}
	if err := os.WriteFile(specPath, out, 0o600); err != nil {
		return nil, fmt.Errorf("writing %q: %w", specPath, err)
	}
	return &spec, nil
}

// defaultKataMounts mirrors the mount set `ctr run --runtime io.containerd.kata.v2`
// produces (the proven-good shape for the kata agent).
func defaultKataMounts() []specs.Mount {
	return []specs.Mount{
		{Destination: "/proc", Type: "proc", Source: "proc", Options: []string{"nosuid", "noexec", "nodev"}},
		{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
		{Destination: "/dev/pts", Type: "devpts", Source: "devpts", Options: []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"}},
		{Destination: "/dev/shm", Type: "tmpfs", Source: "shm", Options: []string{"nosuid", "noexec", "nodev", "mode=1777", "size=65536k"}},
		{Destination: "/dev/mqueue", Type: "mqueue", Source: "mqueue", Options: []string{"nosuid", "noexec", "nodev"}},
		{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: []string{"nosuid", "noexec", "nodev", "ro"}},
		{Destination: "/run", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
	}
}

// defaultKataResources mirrors the device allowlist + cpu shares that
// `ctr run --runtime io.containerd.kata.v2` emits (the proven-good shape).
func defaultKataResources() *specs.LinuxResources {
	dev := func(t string, major, minor int64, access string) specs.LinuxDeviceCgroup {
		d := specs.LinuxDeviceCgroup{Allow: true, Type: t, Access: access}
		if major != 0 {
			d.Major = &major
		}
		if minor >= 0 {
			d.Minor = &minor
		}
		return d
	}
	shares := uint64(1024)
	return &specs.LinuxResources{
		Devices: []specs.LinuxDeviceCgroup{
			{Allow: false, Access: "rwm"},
			dev("c", 1, 3, "rwm"),    // /dev/null
			dev("c", 1, 8, "rwm"),    // /dev/random
			dev("c", 1, 7, "rwm"),    // /dev/full
			dev("c", 5, 0, "rwm"),    // /dev/tty
			dev("c", 1, 5, "rwm"),    // /dev/zero
			dev("c", 1, 9, "rwm"),    // /dev/urandom
			dev("c", 5, 1, "rwm"),    // /dev/console
			dev("c", 136, -1, "rwm"), // pts
			dev("c", 5, 2, "rwm"),    // /dev/ptmx
		},
		CPU: &specs.LinuxCPU{Shares: &shares},
	}
}

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

// mergeKataResources fills in the kata defaults that the caller's spec leaves
// unset. The caller's values win, and anything it sets that has no default is
// carried through untouched — the defaults supply the device allowlist and CPU
// shares kata itself emits, which a container needs to open /dev/null and the
// like, and whose absence fails in ways that do not point back here.
//
// It fills gaps rather than allowlisting known fields so that a field added
// upstream (a pids limit, device entries for a passed-through GPU) reaches the
// guest instead of being silently dropped here.
func mergeKataResources(from *specs.LinuxResources) *specs.LinuxResources {
	def := defaultKataResources()
	if from == nil {
		return def
	}
	out := *from
	if len(out.Devices) == 0 {
		out.Devices = def.Devices
	}
	if out.CPU == nil {
		out.CPU = def.CPU
	} else if out.CPU.Shares == nil && def.CPU != nil {
		cpu := *out.CPU
		cpu.Shares = def.CPU.Shares
		out.CPU = &cpu
	}
	return &out
}
