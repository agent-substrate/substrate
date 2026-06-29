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

package ch

import (
	"context"
	"fmt"
)

// VmConfig is the body of /api/v1/vm.create — the subset of cloud-hypervisor's
// VmConfig ateom sets to boot the kata guest. Modeled on kata's clh driver
// (src/runtime/virtcontainers/clh.go). vm.create + vm.boot are issued with PUT.
type VmConfig struct {
	Cpus     CpusConfig      `json:"cpus"`
	Memory   MemoryConfig    `json:"memory"`
	Payload  PayloadConfig   `json:"payload"`
	Disks    []DiskConfig    `json:"disks,omitempty"`
	Fs       []FsConfig      `json:"fs,omitempty"`
	Rng      *RngConfig      `json:"rng,omitempty"`
	Serial   *ConsoleConfig  `json:"serial,omitempty"`
	Console  *ConsoleConfig  `json:"console,omitempty"`
	Vsock    *VsockConfig    `json:"vsock,omitempty"`
	Platform *PlatformConfig `json:"platform,omitempty"`
}

// FsConfig is a virtio-fs device backed by a vhost-user (virtiofsd) socket. The
// overlay rootfs path uses it as the RO lower; the guest mounts it via the FsTag.
type FsConfig struct {
	Tag        string `json:"tag"`
	Socket     string `json:"socket"`
	NumQueues  int32  `json:"num_queues,omitempty"`
	QueueSize  int32  `json:"queue_size,omitempty"`
	PciSegment int32  `json:"pci_segment,omitempty"`
}

// PlatformConfig sets VM-wide platform options. NumPciSegments must be >1 when a
// virtio-fs device sits on a non-zero PCI segment (kata puts fs on segment 1).
type PlatformConfig struct {
	NumPciSegments int32 `json:"num_pci_segments,omitempty"`
}

// CpusConfig sets the boot/max vCPU counts.
type CpusConfig struct {
	BootVcpus int32 `json:"boot_vcpus"`
	MaxVcpus  int32 `json:"max_vcpus"`
}

// MemoryConfig sets guest RAM. Shared=true makes CH back RAM with a memfd, which
// is what lets vm.snapshot write a SPARSE image (the memory-only snapshot the
// rest of ateom relies on).
type MemoryConfig struct {
	Size   int64 `json:"size"`
	Shared bool  `json:"shared"`
}

// PayloadConfig points at the guest kernel + its cmdline (initramfs/firmware
// unused: the kata guest boots from a virtio-blk image disk, root=/dev/vda1).
type PayloadConfig struct {
	Kernel  string `json:"kernel"`
	Cmdline string `json:"cmdline"`
}

// DiskConfig is one virtio-blk disk. The only disk is the kata guest image
// (/dev/vda, read-only); the actor rootfs is an overlay served over virtio-fs, not a
// disk. NumQueues/QueueSize mirror kata's clh (num_queues = vcpus, queue_size = 1024).
type DiskConfig struct {
	Path      string `json:"path"`
	Readonly  bool   `json:"readonly"`
	Direct    bool   `json:"direct"`
	NumQueues int32  `json:"num_queues,omitempty"`
	QueueSize int32  `json:"queue_size,omitempty"`
	ImageType string `json:"image_type,omitempty"`
}

// RngConfig sets the entropy source (kata uses /dev/urandom).
type RngConfig struct {
	Src string `json:"src"`
}

// ConsoleConfig is a serial/console device. Mode "Off" disables it; "File" with
// File set captures the guest console (for boot debugging); "Tty" to a pty.
type ConsoleConfig struct {
	Mode string `json:"mode"`
	File string `json:"file,omitempty"`
}

// VsockConfig is the hybrid-vsock the kata-agent listens on. Cid is the guest
// CID (kata uses 3); Socket is the host unix socket (kata.VsockSocketPath) that
// ateom then dials (DialAgent) to drive the agent.
type VsockConfig struct {
	Cid    int64  `json:"cid"`
	Socket string `json:"socket"`
}

// CreateVM creates (but does not boot) the VM from cfg via /api/v1/vm.create.
// The VMM must already be up (LaunchVMM). After this the VM is in "Created".
func (c *Client) CreateVM(ctx context.Context, cfg VmConfig) error {
	if err := c.api.put(ctx, "/api/v1/vm.create", cfg); err != nil {
		return fmt.Errorf("vm.create: %w", err)
	}
	return nil
}

// BootVM boots a created VM via /api/v1/vm.boot (transitions Created -> Running).
func (c *Client) BootVM(ctx context.Context) error {
	if err := c.api.put(ctx, "/api/v1/vm.boot", nil); err != nil {
		return fmt.Errorf("vm.boot: %w", err)
	}
	return nil
}
