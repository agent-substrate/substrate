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

// Package child runs the long-lived subprocesses ateworker-microvm owns, and reaps
// each as soon as it exits.
package child

import (
	"os"
	"os/exec"
	"sync/atomic"

	"go.ate.dev/substrate/internal/pidfd"
)

// Process is a started subprocess. A goroutine waits on it from the start, so
// it never lingers as a zombie, whether it is stopped or dies on its own.
type Process struct {
	cmd      *exec.Cmd
	done     chan struct{}
	state    *os.ProcessState
	stopping atomic.Bool
}

// Start starts cmd and begins waiting on it.
func Start(cmd *exec.Cmd) (*Process, error) {
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	exited, err := pidfd.Exited(cmd.Process.Pid)
	if err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return nil, err
	}
	p := &Process{cmd: cmd, done: make(chan struct{})}
	go func() {
		// Wait only once it has exited, so no thread blocks in wait for the
		// life of the process. Process.Wait, not cmd.Wait: cmd.Wait also waits
		// for the output pipes to close, and a grandchild (virtiofsd's sandbox)
		// can hold them open.
		<-exited
		p.state, _ = cmd.Process.Wait()
		close(p.done)
	}()
	return p, nil
}

// Done is closed once the process has exited and been reaped.
func (p *Process) Done() <-chan struct{} {
	return p.done
}

// Stop kills the process and waits until it is reaped. After Stop, Stopped
// reports true, so a watcher on Done can tell this exit from a crash.
func (p *Process) Stop() {
	p.stopping.Store(true)
	// Signals go through a pidfd, so a process already reaped is not
	// mistaken for a reused pid.
	_ = p.cmd.Process.Kill()
	<-p.done
}

// Stopped reports whether Stop was called.
func (p *Process) Stopped() bool {
	return p.stopping.Load()
}

// Exit describes how the process exited. Only valid once Done is closed.
func (p *Process) Exit() string {
	if p.state == nil {
		return "exited"
	}
	return p.state.String()
}
