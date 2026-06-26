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
	"os"
	"path/filepath"
	"strings"

	specs "github.com/opencontainers/runtime-spec/specs-go"
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
	if spec.Linux.Resources == nil {
		spec.Linux.Resources = defaultKataResources()
	}
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

	// NB: no virtio-fs-overlay annotation here. With the STOCK shim, this spec is
	// for the "carrier" container that only boots the VM + shares the RO base over
	// virtio-fs. ateom assembles the actual overlay rootfs itself by driving the
	// kata-agent CreateContainer over ttrpc (see RunWorkload) — no patched shim.

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
	// KNOWN GAP vs the gVisor runtime: this also drops atelet's read-only actor
	// identity bind mount (/run/ate/actor-id). The micro-VM guest can't see host
	// paths (the rootfs is a virtio-blk disk, not a shared filesystem), and
	// reset-to-golden restores guest RAM + rootfs from the golden snapshot, so a
	// per-actor file written into the rootfs would be shadowed/incorrect on restore.
	// Exposing the identity needs a per-actor volume injected from OUTSIDE the golden
	// state; not yet implemented. No micro-VM workload depends on it today.
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
