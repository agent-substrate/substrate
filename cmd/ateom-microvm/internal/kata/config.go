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
	"fmt"
	"strings"

	toml "github.com/pelletier/go-toml/v2"
)

// KataConfig holds the values ateom reads from a kata configuration.toml. ateom
// owns the cloud-hypervisor boot and points it at the runtime-fetched asset paths
// directly, so the only things it needs from the config are the guest sizing and
// the agent kernel command line.
type KataConfig struct {
	// MemoryMiB is the guest RAM size ([hypervisor.clh] default_memory).
	MemoryMiB int
	// VCPUs is the guest vCPU count ([hypervisor.clh] default_vcpus).
	VCPUs int
	// KernelParams is the guest kernel command line ([hypervisor.clh]
	// kernel_params): the kata-agent parameters (agent.log, the systemd target,
	// etc.). The owned boot appends these to the cloud-hypervisor payload cmdline,
	// since there is no kata shim to inject them.
	KernelParams string
}

// clhConfigTOML mirrors the subset of a kata configuration.toml ateom reads.
// Unmarshalling ignores every other key, so it stays valid across kata releases.
type clhConfigTOML struct {
	Hypervisor struct {
		CLH struct {
			DefaultMemory int    `toml:"default_memory"`
			DefaultVCPUs  int    `toml:"default_vcpus"`
			KernelParams  string `toml:"kernel_params"`
		} `toml:"clh"`
	} `toml:"hypervisor"`
}

// ParseConfig reads the guest sizing and kernel_params from a kata
// configuration.toml. memDefault/vcpuDefault are substituted when the key is
// absent or non-positive (kata also accepts default_vcpus = -1 meaning "all host
// CPUs", which the owned boot does not support).
func ParseConfig(base []byte, memDefault, vcpuDefault int) (KataConfig, error) {
	var c clhConfigTOML
	if err := toml.Unmarshal(base, &c); err != nil {
		return KataConfig{}, fmt.Errorf("parsing kata config: %w", err)
	}
	cfg := KataConfig{
		MemoryMiB:    c.Hypervisor.CLH.DefaultMemory,
		VCPUs:        c.Hypervisor.CLH.DefaultVCPUs,
		KernelParams: c.Hypervisor.CLH.KernelParams,
	}
	if cfg.MemoryMiB <= 0 {
		cfg.MemoryMiB = memDefault
	}
	if cfg.VCPUs <= 0 {
		cfg.VCPUs = vcpuDefault
	}
	return cfg, nil
}

// WithDebugConsole appends the kata-agent debug-console kernel parameters so the
// guest agent binds a root debug shell on vsock port 1026, which DebugConsoleDump
// connects to for in-guest diagnostics. Both params are required: agent.debug_console
// enables the console and agent.debug_console_vport=1026 makes the agent bind it on
// the vsock port (the agent only binds a vsock listener when the vport is > 0).
// Idempotent.
func WithDebugConsole(kernelParams string) string {
	return appendKernelParams(kernelParams, "agent.debug_console_vport",
		"agent.debug_console agent.debug_console_vport=1026")
}

// WithAgentDebug appends agent.log=debug so the guest kata-agent emits
// debug-level logs (including the failing path on errors) over its vsock log
// channel. Idempotent.
func WithAgentDebug(kernelParams string) string {
	return appendKernelParams(kernelParams, "agent.log=", "agent.log=debug agent.debug_console")
}

// appendKernelParams appends add to a kernel_params string unless marker is
// already present (so repeated calls are no-ops).
func appendKernelParams(kernelParams, marker, add string) string {
	if strings.Contains(kernelParams, marker) {
		return kernelParams
	}
	if kernelParams == "" {
		return add
	}
	return kernelParams + " " + add
}
