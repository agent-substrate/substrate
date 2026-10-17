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
	"sync"

	"golang.org/x/sys/unix"
)

// AllowUnprivilegedPorts lets this namespace bind ports below 1024 without
// CAP_NET_BIND_SERVICE, which is how atunnel answers a sandbox's DNS on 53.
// The sysctl is per-namespace and grants nothing outside it.
func AllowUnprivilegedPorts() error {
	return setSysctl("net/ipv4/ip_unprivileged_port_start", "0")
}

// procSysMu serializes setSysctl. Sandboxes are set up concurrently and share
// one /proc/sys mount, so otherwise one caller's read-only restore can land
// between another's remount and write, failing that write with EROFS.
var procSysMu sync.Mutex

// procSys wraps the /proc/sys file and mount operations so that unit tests can
// substitute a fake mount. Production code uses hostProcSys.
type procSys struct {
	readFile  func(path string) ([]byte, error)
	writeFile func(path string, data []byte) error
	remount   func(readOnly bool) error
}

var hostProcSys = procSys{
	readFile: os.ReadFile,
	writeFile: func(path string, data []byte) error {
		return os.WriteFile(path, data, 0o644)
	},
	remount: func(readOnly bool) error {
		flags := uintptr(unix.MS_BIND | unix.MS_REMOUNT)
		if readOnly {
			flags |= unix.MS_RDONLY
		}
		return unix.Mount("none", "/proc/sys", "", flags, "")
	},
}

// setSysctl writes value to the named sysctl in the current network
// namespace, remounting /proc/sys read-write when the runtime bind-mounted it
// read-only. A no-op when it already reads that way.
func setSysctl(key, value string) error {
	return hostProcSys.set(key, value)
}

func (p procSys) set(key, value string) error {
	procSysMu.Lock()
	defer procSysMu.Unlock()

	path := "/proc/sys/" + key
	if b, err := p.readFile(path); err == nil && strings.TrimSpace(string(b)) == value {
		return nil
	}
	// Only EROFS is worth remounting for; any other error is returned as is.
	if err := p.writeFile(path, []byte(value+"\n")); !errors.Is(err, unix.EROFS) {
		if err != nil {
			return fmt.Errorf("while setting %s in the current netns: %w", key, err)
		}
		return nil
	}
	if err := p.remount(false); err != nil {
		return fmt.Errorf("while remounting /proc/sys read-write to set %s: %w", key, err)
	}
	defer func() {
		_ = p.remount(true)
	}()
	if err := p.writeFile(path, []byte(value+"\n")); err != nil {
		return fmt.Errorf("while setting %s in the current netns: %w", key, err)
	}
	return nil
}
