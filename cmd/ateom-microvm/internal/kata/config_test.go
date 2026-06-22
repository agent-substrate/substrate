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
	"testing"
)

// stockConfig mirrors the [hypervisor.clh] keys ateom reads from a kata
// configuration.toml (every other key is ignored by ParseConfig).
const stockConfig = `[hypervisor.clh]
path = "/usr/local/bin/cloud-hypervisor"
kernel = "/opt/kata/share/kata-containers/vmlinux.container"
image = "/opt/kata/share/kata-containers/kata-containers.img"
default_memory = 512
default_vcpus = 2
kernel_params = "agent.foo=bar systemd.unit=kata-containers.target"
shared_fs = "virtio-fs"
`

func TestParseConfig(t *testing.T) {
	cfg, err := ParseConfig([]byte(stockConfig), 2048, 1)
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.MemoryMiB != 512 {
		t.Errorf("MemoryMiB = %d, want 512", cfg.MemoryMiB)
	}
	if cfg.VCPUs != 2 {
		t.Errorf("VCPUs = %d, want 2", cfg.VCPUs)
	}
	if want := "agent.foo=bar systemd.unit=kata-containers.target"; cfg.KernelParams != want {
		t.Errorf("KernelParams = %q, want %q", cfg.KernelParams, want)
	}
}

// TestParseConfigDefaults asserts the mem/vcpu defaults kick in when the keys are
// absent or non-positive (kata also accepts default_vcpus = -1 meaning "all host
// CPUs", which the owned boot does not support).
func TestParseConfigDefaults(t *testing.T) {
	for _, tc := range []struct {
		name string
		toml string
	}{
		{"absent", "[hypervisor.clh]\nkernel_params = \"x\"\n"},
		{"nonpositive", "[hypervisor.clh]\ndefault_memory = 0\ndefault_vcpus = -1\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseConfig([]byte(tc.toml), 2048, 1)
			if err != nil {
				t.Fatalf("ParseConfig: %v", err)
			}
			if cfg.MemoryMiB != 2048 {
				t.Errorf("MemoryMiB = %d, want default 2048", cfg.MemoryMiB)
			}
			if cfg.VCPUs != 1 {
				t.Errorf("VCPUs = %d, want default 1", cfg.VCPUs)
			}
		})
	}
}

func TestWithDebugConsole(t *testing.T) {
	got := WithDebugConsole("root=/dev/vda1")
	if !strings.Contains(got, "agent.debug_console") || !strings.Contains(got, "agent.debug_console_vport=1026") {
		t.Errorf("WithDebugConsole did not append the debug-console params: %q", got)
	}
	// Idempotent: a second call must not append the params again.
	if again := WithDebugConsole(got); again != got {
		t.Errorf("WithDebugConsole not idempotent:\n first = %q\nsecond = %q", got, again)
	}
}

func TestWithAgentDebug(t *testing.T) {
	got := WithAgentDebug("root=/dev/vda1")
	if !strings.Contains(got, "agent.log=debug") {
		t.Errorf("WithAgentDebug did not append agent.log=debug: %q", got)
	}
	// Idempotent: a second call must not append agent.log again.
	if again := WithAgentDebug(got); again != got {
		t.Errorf("WithAgentDebug not idempotent:\n first = %q\nsecond = %q", got, again)
	}
}
