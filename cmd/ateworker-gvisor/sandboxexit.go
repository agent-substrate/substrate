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
	"context"
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
// Since gvisor/gvisor@f80859d424 (release-20260921.0) runsc treats an exited
// sandbox as stopped, zombie included, so cleanup can delete it once it exits.
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
func (p *sandboxProcess) waitExited(ctx context.Context, timeout time.Duration) error {
	exited, err := p.exited()
	if err != nil {
		return fmt.Errorf("watching sandbox %d: %w", p.pid, err)
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-exited:
		return nil
	case <-timer.C:
		return fmt.Errorf("sandbox %d still running after %v", p.pid, timeout)
	case <-ctx.Done():
		return fmt.Errorf("waiting for sandbox %d to exit: %w", p.pid, ctx.Err())
	}
}
