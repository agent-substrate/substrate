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
	"log/slog"
	"os"

	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"

	"github.com/agent-substrate/substrate/internal/deviceplugin"
)

// hostDevRoot is where the node's /dev is mounted into atelet (see
// manifests/ate-install/atelet.yaml), read only to detect which device nodes
// exist; workers are handed the real host paths, which kubelet resolves.
const hostDevRoot = "/host/dev"

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
