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

package netns

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// AllowUnprivilegedPorts lets this namespace bind ports below 1024 without
// CAP_NET_BIND_SERVICE, which is how atunnel answers a sandbox's DNS on 53.
// The sysctl is per-namespace and grants nothing outside it.
func AllowUnprivilegedPorts() error {
	return setSysctl("net/ipv4/ip_unprivileged_port_start", "0")
}

// setSysctl writes value to the named sysctl in the current network
// namespace, remounting /proc/sys read-write when the runtime bind-mounted it
// read-only. A no-op when it already reads that way.
func setSysctl(key, value string) error {
	path := "/proc/sys/" + key
	if b, err := os.ReadFile(path); err == nil && strings.TrimSpace(string(b)) == value {
		return nil
	}
	// Only EROFS is worth remounting for; any other error is returned as is.
	if err := os.WriteFile(path, []byte(value+"\n"), 0o644); !errors.Is(err, unix.EROFS) {
		if err != nil {
			return fmt.Errorf("while setting %s in the current netns: %w", key, err)
		}
		return nil
	}
	if err := unix.Mount("none", "/proc/sys", "", unix.MS_BIND|unix.MS_REMOUNT, ""); err != nil {
		return fmt.Errorf("while remounting /proc/sys read-write to set %s: %w", key, err)
	}
	defer func() {
		_ = unix.Mount("none", "/proc/sys", "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY, "")
	}()
	if err := os.WriteFile(path, []byte(value+"\n"), 0o644); err != nil {
		return fmt.Errorf("while setting %s in the current netns: %w", key, err)
	}
	return nil
}
