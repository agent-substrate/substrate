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

package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/internal/pidfd"

	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

// sandboxExitTimeout bounds the wait for a checkpointed sandbox to exit.
const sandboxExitTimeout = 10 * time.Second

// sandboxProcess is the runsc sandbox across a checkpoint, which stops it.
// The sandbox exits shortly after the checkpoint returns, and cleanup must wait
// for that.
type sandboxProcess struct {
	pid   int
	pidfd int
}

// openSandboxProcess pins the sandbox before it can exit, so a reused pid is
// never mistaken for it.
func openSandboxProcess(actorDirs *ateworkerpb.ActorDirs) (*sandboxProcess, error) {
	pid, err := sandboxPid(actorDirs)
	if err != nil {
		return nil, err
	}
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return nil, fmt.Errorf("opening pidfd for sandbox %d: %w", pid, err)
	}
	return &sandboxProcess{pid: pid, pidfd: fd}, nil
}

// sandboxPid reads the sandbox's pid from the pid file runsc writes for the
// sandbox.
func sandboxPid(actorDirs *ateworkerpb.ActorDirs) (int, error) {
	b, err := os.ReadFile(pidFilePath(actorDirs, ocispec.SandboxID))
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return 0, fmt.Errorf("parsing sandbox pid %q: %w", b, err)
	}
	return pid, nil
}

// exited returns a channel closed once the sandbox exits.
func (p *sandboxProcess) exited() (<-chan struct{}, error) {
	return pidfd.ExitedFD(p.pidfd)
}

func (p *sandboxProcess) Close() {
	_ = unix.Close(p.pidfd)
}

// waitExited blocks until the sandbox has exited.
func (p *sandboxProcess) waitExited(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			return fmt.Errorf("sandbox %d still running after %v", p.pid, timeout)
		}
		// the pidfd turns readable once the process exits, zombie included
		fds := []unix.PollFd{{Fd: int32(p.pidfd), Events: unix.POLLIN}}
		n, err := unix.Poll(fds, int(left.Milliseconds())+1)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return fmt.Errorf("polling sandbox %d: %w", p.pid, err)
		}
		if n > 0 {
			if fds[0].Revents&unix.POLLNVAL != 0 {
				return fmt.Errorf("polling sandbox %d: invalid pidfd", p.pid)
			}
			return nil
		}
	}
}
