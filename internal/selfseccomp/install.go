//go:build linux

// Copyright 2026 The Agent Substrate Authors
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

package selfseccomp

import (
	"fmt"
	"os"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

const installedEnv = "_ATE_SELFSECCOMP_INSTALLED"

// InstallOnce installs the worker seccomp filter unless a parent process
// already installed it (tracked across pid1.RunIfPID1's re-exec via
// installedEnv). PID 1 always installs regardless of the environment. Call
// ClearMarker after pid1.RunIfPID1 returns so the marker is not leaked to
// child runtimes.
func InstallOnce() error {
	if os.Getpid() != 1 && os.Getenv(installedEnv) == "1" {
		return nil
	}
	if err := Install(); err != nil {
		return err
	}
	return os.Setenv(installedEnv, "1")
}

// ClearMarker removes the inheritance marker set by InstallOnce.
func ClearMarker() {
	_ = os.Unsetenv(installedEnv)
}

// Install compiles and installs the seccomp-bpf filter for runtime.GOARCH
// across all existing OS threads using SECCOMP_FILTER_FLAG_TSYNC. It sets
// PR_SET_NO_NEW_PRIVS first so unprivileged callers can install the filter too.
func Install() error {
	filter, err := compile(runtime.GOARCH)
	if err != nil {
		return fmt.Errorf("compiling seccomp filter for %s: %w", runtime.GOARCH, err)
	}
	if len(filter) > unix.BPF_MAXINSNS {
		return fmt.Errorf("seccomp filter has %d instructions, limit is %d", len(filter), unix.BPF_MAXINSNS)
	}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return fmt.Errorf("prctl(PR_SET_NO_NEW_PRIVS): %w", err)
	}
	prog := unix.SockFprog{
		Len:    uint16(len(filter)),
		Filter: &filter[0],
	}
	_, _, errno := unix.Syscall(unix.SYS_SECCOMP,
		unix.SECCOMP_SET_MODE_FILTER,
		unix.SECCOMP_FILTER_FLAG_TSYNC,
		uintptr(unsafe.Pointer(&prog)))
	runtime.KeepAlive(filter)
	runtime.KeepAlive(prog)
	if errno != 0 {
		return fmt.Errorf("seccomp(SECCOMP_SET_MODE_FILTER, TSYNC): %w", errno)
	}
	return nil
}
