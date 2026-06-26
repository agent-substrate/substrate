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
	"context"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// StartBlkWorkload starts the actor container with its rootfs backed by a single
// boot-time virtio-blk disk (devPath, e.g. "/dev/vdb") — the virtio-blk-rootfs
// path. There is NO overlay, NO virtio-fs, NO tmpfs upper: the agent direct-mounts
// devPath (ext4) as the container rootfs, so rootfs writes land on the host-backed
// disk file (off guest RAM) and the CH snapshot stays memory-only with no balloon.
//
// One "blk" storage: source is the /dev node (kata's block storage handler mounts
// it directly when source starts with /dev — no uevent/auto-enumeration wait,
// unlike a hotplugged disk), fstype ext4, mounted at the container rootfs path.
// The spec's Root.Path is set to that mount point, which the agent's setup_bundle
// then uses as the container root.
func (a *AgentClient) StartBlkWorkload(ctx context.Context, containerID, devPath string, spec *specs.Spec) error {
	rootfs := "/run/kata-containers/" + containerID + "/rootfs"
	storages := []*agentpb.Storage{
		{
			Driver:     "blk",
			Source:     devPath,
			Fstype:     "ext4",
			MountPoint: rootfs,
			Options:    []string{"rw"},
		},
	}

	pbSpec := SpecToAgentPB(spec)
	pbSpec.Root = &agentpb.Root{Path: rootfs, Readonly: false}

	if err := a.CreateContainer(ctx, &agentpb.CreateContainerRequest{
		ContainerId: containerID,
		ExecId:      containerID,
		Storages:    storages,
		OCI:         pbSpec,
	}); err != nil {
		return fmt.Errorf("creating blk workload %q: %w", containerID, err)
	}
	if err := a.StartContainer(ctx, containerID); err != nil {
		return fmt.Errorf("starting blk workload %q: %w", containerID, err)
	}
	return nil
}

// SpecToAgentPB converts an OCI runtime spec into the kata-agent's protobuf Spec
// (agentpb.Spec) for a CreateContainer ttrpc call. The shim normally does this
// conversion; ateom does it itself when it drives the agent directly ("be your
// own hook scheduler"). A blind json round-trip does NOT work: agentpb's Spec
// JSON tags are PascalCase (from oci.proto), while OCI config.json is lowercase.
//
// Only the fields the kata-agent needs to create + start a container are mapped
// (process, root, mounts, linux namespaces/resources/cgroup/masked+readonly
// paths). The container rootfs is provided out-of-band as storages; the caller
// is expected to set the returned spec's Root.Path to the overlay mount point.
func SpecToAgentPB(s *specs.Spec) *agentpb.Spec {
	if s == nil {
		return nil
	}
	out := &agentpb.Spec{
		Version:     s.Version,
		Hostname:    s.Hostname,
		Annotations: s.Annotations,
	}

	if s.Process != nil {
		p := &agentpb.Process{
			Args:            s.Process.Args,
			Env:             s.Process.Env,
			Cwd:             s.Process.Cwd,
			NoNewPrivileges: s.Process.NoNewPrivileges,
			User: &agentpb.User{
				UID:            s.Process.User.UID,
				GID:            s.Process.User.GID,
				AdditionalGids: s.Process.User.AdditionalGids,
				Username:       s.Process.User.Username,
			},
		}
		if c := s.Process.Capabilities; c != nil {
			p.Capabilities = &agentpb.LinuxCapabilities{
				Bounding:    c.Bounding,
				Effective:   c.Effective,
				Inheritable: c.Inheritable,
				Permitted:   c.Permitted,
				Ambient:     c.Ambient,
			}
		}
		for _, rl := range s.Process.Rlimits {
			p.Rlimits = append(p.Rlimits, &agentpb.POSIXRlimit{
				Type: rl.Type, Hard: rl.Hard, Soft: rl.Soft,
			})
		}
		out.Process = p
	}

	if s.Root != nil {
		out.Root = &agentpb.Root{Path: s.Root.Path, Readonly: s.Root.Readonly}
	}

	for _, m := range s.Mounts {
		out.Mounts = append(out.Mounts, &agentpb.Mount{
			Destination: m.Destination,
			Source:      m.Source,
			Type:        m.Type,
			Options:     m.Options,
		})
	}

	if s.Linux != nil {
		l := &agentpb.Linux{
			CgroupsPath:   s.Linux.CgroupsPath,
			MaskedPaths:   s.Linux.MaskedPaths,
			ReadonlyPaths: s.Linux.ReadonlyPaths,
		}
		// TODO: forward the remaining OCI security knobs the kata-agent supports
		// for parity with the OCI spec — Linux.Seccomp and Linux.Sysctl here, and
		// Process.ApparmorProfile / Process.SelinuxLabel above. The MVP runs the
		// actor with kata's defaults for these.
		for _, ns := range s.Linux.Namespaces {
			// Mirror the kata shim (kata_agent.go constrainGRPCSpec): the
			// network/cgroup/time namespaces are handled on the host / unsupported
			// in the guest agent, so DROP them (dropping the network ns makes the
			// container share the guest sandbox network = eth0/actor IP). Every
			// other namespace's host Path MUST be emptied, else the agent tries to
			// join a host namespace path inside the guest and fails ENOENT.
			switch ns.Type {
			case specs.NetworkNamespace, specs.CgroupNamespace, specs.TimeNamespace:
				continue
			}
			l.Namespaces = append(l.Namespaces, &agentpb.LinuxNamespace{Type: string(ns.Type)})
		}
		if r := s.Linux.Resources; r != nil {
			res := &agentpb.LinuxResources{}
			for _, d := range r.Devices {
				dc := &agentpb.LinuxDeviceCgroup{Allow: d.Allow, Type: d.Type, Access: d.Access}
				if d.Major != nil {
					dc.Major = *d.Major
				}
				if d.Minor != nil {
					dc.Minor = *d.Minor
				}
				res.Devices = append(res.Devices, dc)
			}
			if r.CPU != nil {
				cpu := &agentpb.LinuxCPU{}
				if r.CPU.Shares != nil {
					cpu.Shares = *r.CPU.Shares
				}
				res.CPU = cpu
			}
			l.Resources = res
		}
		out.Linux = l
	}

	return out
}
