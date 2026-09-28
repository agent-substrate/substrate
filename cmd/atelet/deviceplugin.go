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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"

	"github.com/agent-substrate/substrate/internal/deviceplugin"
)

// hostDevRoot is where the node's /dev is mounted into atelet (see
// manifests/ate-install/atelet.yaml), read only to detect which device nodes
// exist; workers are handed the real host paths, which kubelet resolves.
const hostDevRoot = "/host/dev"

// hostShmemTHPPath is where the node's shmem THP policy knob is mounted into
// atelet (see manifests/ate-install/atelet.yaml).
const hostShmemTHPPath = "/host/sys/kernel/mm/transparent_hugepage/shmem_enabled"

// writeShmemTHPFile is a test seam for os.WriteFile so unit tests can exercise
// write-error handling even when running as root.
var writeShmemTHPFile = os.WriteFile

// ensureShmemTHP configures /sys/kernel/mm/transparent_hugepage/shmem_enabled
// to "advise" when the node is currently set to the kernel default ("[never]").
//
// cloud-hypervisor backs shared guest RAM with a memfd and calls
// madvise(MADV_HUGEPAGE) on it (MemoryConfig.thp defaults to true), so "advise"
// lets KVM map guest RAM with 2 MiB EPT pages instead of 4 KiB pages without
// changing huge-page backing for other pods' un-advised shared mappings on the
// node. Any non-"[never]" setting ("[advise]", "[within_size]", "[always]",
// "[deny]", "[force]") is left untouched so a deliberate node-level choice is
// respected.
func ensureShmemTHP(ctx context.Context, path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading %s: %w", path, err)
	}
	cur := strings.TrimSpace(string(b))
	if !strings.Contains(cur, "[never]") {
		slog.InfoContext(ctx, "Leaving shmem transparent hugepages setting unchanged",
			slog.String("shmem_enabled", cur))
		return nil
	}

	const mode = "advise"
	if err := writeShmemTHPFile(path, []byte(mode+"\n"), 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	slog.InfoContext(ctx, "Enabled shmem transparent hugepages",
		slog.String("mode", mode), slog.String("previous", cur))
	return nil
}

// microvmNodeCapable reports whether this node can host micro-VM workers:
// cloud-hypervisor needs /dev/kvm (VmCreate fails with EPERM without it), and
// worker pods request the matching extended resource, so they only schedule to
// nodes where the device exists. Device presence is therefore the earliest
// reliable eligibility signal — known at atelet startup, before any WorkerPool
// schedules here.
//
// TODO(https://github.com/agent-substrate/substrate/pull/1207): /dev/kvm is
// not the only micro-VM hypervisor device; once /dev/mshv support lands, an
// mshv-only node would be wrongly reported incapable here. Treat presence of
// any micro-VM hypervisor device in SandboxDevices as capability, not KVM
// alone.
func microvmNodeCapable(devRoot string) bool {
	for _, d := range deviceplugin.SandboxDevices {
		if d.ResourceName == deviceplugin.ResourceKVM {
			return d.Present(devRoot)
		}
	}
	return false
}

// startDevicePlugins advertises the sandbox host devices present on this node to
// kubelet as extended resources, in the background for the lifetime of ctx. This
// is what lets a worker be granted /dev/kvm without running privileged. atelet
// already runs per node, so it hosts this rather than adding a second DaemonSet.
//
// Failures are logged, never fatal: a node that cannot advertise devices still
// runs every sandbox class that needs none.
func startDevicePlugins(ctx context.Context) {
	// Without the kubelet plugin directory there is nobody to register with, and
	// atelet runs in environments that do not mount it (tests, minimal installs).
	if _, err := os.Stat(pluginapi.DevicePluginPath); err != nil {
		slog.InfoContext(ctx, "Kubelet device plugin directory unavailable; not advertising host devices",
			slog.String("path", pluginapi.DevicePluginPath), slog.Any("err", err))
		return
	}

	devices := deviceplugin.Available(deviceplugin.SandboxDevices, hostDevRoot)
	if len(devices) == 0 {
		slog.InfoContext(ctx, "No sandbox host devices present on this node; not advertising any",
			slog.String("devRoot", hostDevRoot))
		return
	}

	for _, dev := range devices {
		go func() {
			if err := deviceplugin.New(dev).Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				slog.ErrorContext(ctx, "Device plugin stopped",
					slog.String("resource", dev.ResourceName), slog.Any("err", err))
			}
		}()
	}
}
