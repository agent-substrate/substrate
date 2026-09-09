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
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/opencontainers/runtime-spec/specs-go"
)

// Sub-share names inside the micro-VM virtio-fs share.
const (
	// GuestSharedDir is where the kata agent mounts the share in the guest.
	GuestSharedDir = "/run/kata-containers/shared/containers"
	// ShareDurable, ShareCSI and ShareSystemInfo hold one subdirectory per
	// volume; ShareVolumes holds a container's image volumes at
	// <containerID>/volumes/<name>.
	ShareDurable    = "durable"
	ShareCSI        = "csi"
	ShareSystemInfo = "system-info"
	ShareVolumes    = "volumes"
)

// MicroVMOptions describes the micro-VM context of one actor container.
type MicroVMOptions struct {
	ActorUID    string
	ContainerID string
}

// ShapeMicroVM replaces host system mounts with guest mounts, repoints volume
// bind mounts to guest share paths, and fills in kata's default resources. It
// must run on an unshaped spec, and errors on a bind it cannot place in the
// guest.
func ShapeMicroVM(spec *specs.Spec, o MicroVMOptions) error {
	// Translate volume bind mounts into guest share paths.
	volumes := make([]specs.Mount, 0, len(spec.Mounts))
	for _, m := range spec.Mounts {
		if m.Type != "bind" {
			continue
		}
		src, err := guestVolumeSource(m.Source, o.ActorUID, o.ContainerID)
		if err != nil {
			return fmt.Errorf("mount %q: %w", m.Destination, err)
		}
		m.Source = src
		// Use rbind, so nested mount points come across.
		m.Options = slices.Clone(m.Options)
		for i, opt := range m.Options {
			if opt == "bind" {
				m.Options[i] = "rbind"
			}
		}
		volumes = append(volumes, m)
	}
	spec.Mounts = append(guestSystemMounts(), volumes...)

	if spec.Linux == nil {
		spec.Linux = &specs.Linux{}
	}
	// The container's own declared limits survive the merge; StartRootfsContainer
	// sets CgroupsPath.
	spec.Linux.Resources = mergeKataResources(spec.Linux.Resources)
	return nil
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
	def := guestResources()
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

// guestVolumeSource maps a volume's host directory to its guest path.
func guestVolumeSource(hostPath, actorUID, containerID string) (string, error) {
	for _, staged := range []struct{ host, guest string }{
		{ateompath.DurableDirVolumeMountsDir(actorUID), path.Join(GuestSharedDir, ShareDurable)},
		{ateompath.VolumesDir(actorUID), path.Join(GuestSharedDir, ShareCSI)},
		{ateompath.SystemInfoVolumeRootsDir(actorUID), path.Join(GuestSharedDir, ShareSystemInfo)},
		{ateompath.ImageVolumeMountPath(actorUID, containerID, ""), path.Join(GuestSharedDir, containerID, ShareVolumes)},
	} {
		if rel, ok := strings.CutPrefix(hostPath, staged.host+"/"); ok {
			return path.Join(staged.guest, rel), nil
		}
	}
	return "", fmt.Errorf("host path %q is not staged into the guest share", hostPath)
}

// guestSystemMounts returns the standard kata guest mount set.
func guestSystemMounts() []specs.Mount {
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

// guestResources returns the standard kata device allowlist and CPU shares.
func guestResources() *specs.LinuxResources {
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
