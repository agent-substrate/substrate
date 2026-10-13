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

// Package childreap reaps orphaned children without racing subprocess waits.
// A subprocess started through RunCommand is tracked from its fork, and the
// reaper collects only zombie children it does not track, so a wait here can
// never consume an exit status that os/exec expects.
//go:build unix

package childreap

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// sweepInterval bounds how long an orphan stays a zombie when its SIGCHLD was
// coalesced away or arrived before Run.
const sweepInterval = 10 * time.Second

// Reaper collects orphaned children. The zero value is not usable; call New.
type Reaper struct {
	// forks is held shared across a fork and its registration and exclusively
	// across a reap, so a reap never sees a child that is not tracked yet.
	forks sync.RWMutex
	mu    sync.Mutex
	// tracked holds the pids whose exit status belongs to a RunCommand.
	tracked map[int]struct{}
}

func New() *Reaper {
	return &Reaper{tracked: map[int]struct{}{}}
}

// RunCommand runs cmd; its exit status is reserved for cmd.Wait.
func (r *Reaper) RunCommand(cmd *exec.Cmd) error {
	if err := r.start(cmd); err != nil {
		return err
	}
	err := cmd.Wait()
	r.mu.Lock()
	delete(r.tracked, cmd.Process.Pid)
	r.mu.Unlock()
	return err
}

// CombinedOutput is RunCommand for callers that need cmd.CombinedOutput.
func (r *Reaper) CombinedOutput(cmd *exec.Cmd) ([]byte, error) {
	if cmd.Stdout != nil {
		return nil, errors.New("exec: Stdout already set")
	}
	if cmd.Stderr != nil {
		return nil, errors.New("exec: Stderr already set")
	}
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := r.RunCommand(cmd)
	return out.Bytes(), err
}

func (r *Reaper) start(cmd *exec.Cmd) error {
	r.forks.RLock()
	defer r.forks.RUnlock()
	if err := cmd.Start(); err != nil {
		return err
	}
	r.mu.Lock()
	r.tracked[cmd.Process.Pid] = struct{}{}
	r.mu.Unlock()
	return nil
}

// Run reaps until ctx ends. Call it once in its own goroutine.
func (r *Reaper) Run(ctx context.Context) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, unix.SIGCHLD)
	defer signal.Stop(sigs)
	sweep := time.NewTicker(sweepInterval)
	defer sweep.Stop()
	for {
		r.reapOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-sigs:
			// A burst of SIGCHLD needs only one pass.
			drain(sigs)
		case <-sweep.C:
		}
	}
}

// drain empties ch without blocking.
func drain(ch <-chan os.Signal) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// reapOnce collects every zombie child that no RunCommand is waiting for.
func (r *Reaper) reapOnce(ctx context.Context) {
	r.forks.Lock()
	defer r.forks.Unlock()
	zombies, err := zombieChildren(os.Getpid())
	if err != nil {
		slog.WarnContext(ctx, "Listing zombie children failed", slog.Any("err", err))
		return
	}
	for _, pid := range zombies {
		r.mu.Lock()
		_, tracked := r.tracked[pid]
		r.mu.Unlock()
		if tracked {
			continue
		}
		reap(ctx, pid)
	}
}

// reap collects one zombie. WNOHANG: a thread group still exiting is not
// waitable yet, and the next pass gets it.
func reap(ctx context.Context, pid int) {
	var status unix.WaitStatus
	for {
		_, err := unix.Wait4(pid, &status, unix.WNOHANG, nil)
		switch {
		case err == nil, errors.Is(err, unix.ECHILD):
			return
		case errors.Is(err, unix.EINTR):
			continue
		default:
			slog.WarnContext(ctx, "Reaping a child failed", slog.Int("pid", pid), slog.Any("err", err))
			return
		}
	}
}
