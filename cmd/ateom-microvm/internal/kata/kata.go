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

// Package kata holds the helpers ateom uses to boot and drive a kata guest in a
// cloud-hypervisor micro-VM WITHOUT the kata shim: ateom boots cloud-hypervisor
// itself (see internal/ch), then drives the stock kata-agent over its
// hybrid-vsock ttrpc API (DialAgent / AgentClient) to create the sandbox and
// start the actor's container on a writable virtio-blk rootfs (StartBlkWorkload).
//
// It also renders the kata configuration.toml (for the agent kernel_params +
// guest sizing) from runtime-fetched assets (config.go), builds the actor's ext4
// rootfs disk (BuildExt4Image), and sweeps leftover per-sandbox host-side state
// (CleanupSandboxState).
package kata

import (
	"path/filepath"
)

// vcVMDir is the per-sandbox runtime dir convention kata uses (it holds the
// cloud-hypervisor API socket and the hybrid-vsock socket).
const vcVMDir = "/run/vc/vm"

// CLHSocketPath returns the default cloud-hypervisor API socket path for the
// sandbox with the given id (the per-sandbox runtime dir). ateom records the
// actual api-socket it launched the VMM on, but uses this as the fallback.
func CLHSocketPath(id string) string {
	return filepath.Join(vcVMDir, id, "clh-api.sock")
}
