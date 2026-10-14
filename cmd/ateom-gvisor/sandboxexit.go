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

package main

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// sandboxExitTimeout bounds the wait for a checkpointed sandbox to go away.
const sandboxExitTimeout = 10 * time.Second

// sandboxProcess is the runsc sandbox across a MEMORY checkpoint, which stops
// it. runsc treats a sandbox as live while its pid exists, zombie included, so
// a delete issued before the sandbox is reaped fails.
type sandboxProcess struct {
	pid   int
	pidfd int
}

// openSandboxProcess pins the sandbox before it can exit, so a reused pid is
// never mistaken for it.
func openSandboxProcess(actorDirs *ateompb.ActorDirs) (*sandboxProcess, error) {
	b, err := os.ReadFile(pidFilePath(actorDirs, ocispec.PauseContainer))
	if err != nil {
		return nil, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return nil, fmt.Errorf("parsing sandbox pid %q: %w", b, err)
	}
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return nil, fmt.Errorf("opening pidfd for sandbox %d: %w", pid, err)
	}
	return &sandboxProcess{pid: pid, pidfd: fd}, nil
}

func (p *sandboxProcess) Close() {
	_ = unix.Close(p.pidfd)
}

// waitGone blocks until the sandbox has exited and been reaped.
func (p *sandboxProcess) waitGone(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	// The pidfd turns readable once the process exits.
	for {
		fds := []unix.PollFd{{Fd: int32(p.pidfd), Events: unix.POLLIN}}
		n, err := unix.Poll(fds, int(time.Until(deadline).Milliseconds()))
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return fmt.Errorf("polling sandbox %d: %w", p.pid, err)
		}
		if n == 0 {
			return fmt.Errorf("sandbox %d still running after %v", p.pid, timeout)
		}
		break
	}
	// ateominit reaps it as soon as it exits.
	for isZombie(p.pid) {
		if time.Now().After(deadline) {
			return fmt.Errorf("sandbox %d still unreaped after %v", p.pid, timeout)
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}

// isZombie reports whether pid is an unreaped zombie. Once the pidfd reports
// the exit, a live process at pid is a reuse, not the sandbox.
func isZombie(pid int) bool {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	// The state follows the parenthesized command, which may itself hold ")".
	i := bytes.LastIndexByte(b, ')')
	return i >= 0 && i+2 < len(b) && b[i+2] == 'Z'
}
