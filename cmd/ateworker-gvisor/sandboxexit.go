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

// sandboxPid reads the sandbox's pid from the pause container's pid file,
// which runsc writes for the sandbox.
func sandboxPid(actorDirs *ateworkerpb.ActorDirs) (int, error) {
	b, err := os.ReadFile(pidFilePath(actorDirs, ocispec.PauseContainer))
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

// waitGone blocks until the sandbox has exited and been reaped.
func (p *sandboxProcess) waitGone(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	// usually gone within a few milliseconds of the checkpoint returning
	backoff := time.Millisecond
	for {
		// ESRCH only once the process is reaped. EPERM still means it exists.
		err := unix.PidfdSendSignal(p.pidfd, 0, nil, 0)
		if err == unix.ESRCH {
			return nil
		}
		if err != nil && err != unix.EPERM {
			return fmt.Errorf("signaling sandbox %d: %w", p.pid, err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("sandbox %d still present after %v", p.pid, timeout)
		}
		time.Sleep(backoff)
		backoff = min(backoff*2, 50*time.Millisecond)
	}
}
