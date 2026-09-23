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
	"strings"
)

const (
	// TODO(#1724): Tune the following values for Substrate actors.
	// DefaultMemoryMiB is the default guest memory size (MiB).
	DefaultMemoryMiB = 2048
	// DefaultVCPUs is the default guest vCPU count.
	DefaultVCPUs = 1
)

const (
	// baseKernelParams is the guest kernel command line parameters ateom boots with;
	// there is no kata shim to inject them.
	baseKernelParams         = "cgroup_no_v1=all systemd.unified_cgroup_hierarchy=1"
	debugConsoleKernelParams = baseKernelParams + " agent.debug_console agent.debug_console_vport=1026"
)

// WithDebugConsole returns the kernel params with the kata-agent debug console
// enabled, so the guest agent binds a root debug shell on vsock port 1026, which
// DebugConsoleDump connects to for in-guest diagnostics. Both params are required:
// agent.debug_console enables the console and agent.debug_console_vport=1026 makes
// the agent bind it on the vsock port (the agent only binds a vsock listener when
// the vport is > 0).
func WithDebugConsole() string {
	return debugConsoleKernelParams
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
