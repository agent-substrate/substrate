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
	"io/fs"
	"os"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

// Sysctl is a key-value sysctl setting.
type Sysctl struct {
	Key   string
	Value string
}

// SetSysctls writes each sysctl, in order, in the current network namespace,
// under a single lock hold and at most one /proc/sys read-write remount.
// Every key is written even if it already reads that way, because writing one
// key can change another: turning net/ipv4/conf/all/forwarding off sets
// net/ipv4/conf/all/accept_redirects. Missing net/ipv6/ keys are skipped.
func SetSysctls(sysctls ...Sysctl) error {
	return hostProcSys.set(sysctls)
}

// procSysMu serializes SetSysctls. Sandboxes are set up concurrently and share
// one /proc/sys mount, so otherwise one caller's read-only restore can land
// between another's remount and write, failing that write with EROFS.
var procSysMu sync.Mutex

// procSys wraps the /proc/sys file and mount operations so that unit tests can
// substitute a fake mount. Production code uses hostProcSys.
type procSys struct {
	writeFile func(path string, data []byte) error
	remount   func(readOnly bool) error
}

var hostProcSys = procSys{
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

func (p procSys) set(sysctls []Sysctl) error {
	procSysMu.Lock()
	defer procSysMu.Unlock()

	writeAll := func() error {
		for _, s := range sysctls {
			err := p.writeFile("/proc/sys/"+s.Key, []byte(s.Value+"\n"))
			if errors.Is(err, fs.ErrNotExist) && strings.HasPrefix(s.Key, "net/ipv6/") {
				continue
			}
			if err != nil {
				return fmt.Errorf("while setting %s in the current netns: %w", s.Key, err)
			}
		}
		return nil
	}

	// Only EROFS is worth remounting for; any other error is returned as is.
	err := writeAll()
	if err == nil || !errors.Is(err, unix.EROFS) {
		return err
	}

	if err := p.remount(false); err != nil {
		return fmt.Errorf("while remounting /proc/sys read-write: %w", err)
	}
	defer func() {
		_ = p.remount(true)
	}()
	return writeAll()
}
