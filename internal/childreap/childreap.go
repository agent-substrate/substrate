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
// Reaping waits for tracked subprocesses to finish because wait4(-1) can
// consume an exit status expected by os/exec.
package childreap

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// MaxDefer is how long reaping waits before blocking new subprocesses.
const MaxDefer = 60 * time.Second

// MaxDrain is how long a forced drain waits before abandoning the reap.
const MaxDrain = 30 * time.Second

// Reaper collects orphaned children. The zero value is not usable; call New.
type Reaper struct {
	mu sync.Mutex
	// cond is broadcast when inFlight reaches zero or reaping ends.
	cond *sync.Cond
	// inFlight counts tracked subprocesses.
	inFlight int
	// reaping blocks new subprocesses while wait4 runs.
	reaping bool
	// draining blocks new subprocesses until inFlight reaches zero.
	draining bool
}

func New() *Reaper {
	r := &Reaper{}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// Enter prevents reaping until the returned function is called.
//
//	defer reaper.Enter()()
//
// Do not use Enter for long-lived subprocesses; they prevent reaping.
func (r *Reaper) Enter() func() {
	r.enter()
	return r.leave
}

// RunCommand runs cmd while excluding child reaping.
func (r *Reaper) RunCommand(cmd *exec.Cmd) error {
	defer r.Enter()()
	return cmd.Run()
}

// CombinedOutput is RunCommand for callers that need cmd.CombinedOutput.
func (r *Reaper) CombinedOutput(cmd *exec.Cmd) ([]byte, error) {
	defer r.Enter()()
	return cmd.CombinedOutput()
}

func (r *Reaper) enter() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for r.reaping || r.draining {
		r.cond.Wait()
	}
	r.inFlight++
}

func (r *Reaper) leave() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inFlight--
	if r.inFlight == 0 {
		r.cond.Broadcast()
	}
}

// Run reaps until ctx ends. Call it once in its own goroutine.
func (r *Reaper) Run(ctx context.Context) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, unix.SIGCHLD)
	defer signal.Stop(sigs)

	for {
		select {
		case <-ctx.Done():
			return
		case <-sigs:
		}
		// A burst of SIGCHLD needs only one pass over wait4.
		drain(sigs)
		r.reapOnce(ctx)
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

// reapOnce waits for a quiet moment, then collects everything waiting.
func (r *Reaper) reapOnce(ctx context.Context) {
	if !r.acquire(ctx) {
		return
	}
	defer r.release()
	for {
		var status unix.WaitStatus
		pid, err := unix.Wait4(-1, &status, unix.WNOHANG, nil)
		switch err {
		case nil:
			if pid <= 0 {
				// Children exist but none have exited. Another SIGCHLD will
				// bring us back.
				return
			}
		case unix.EINTR:
			continue
		case unix.ECHILD:
			return
		default:
			slog.WarnContext(ctx, "Reaping children failed", slog.Any("err", err))
			return
		}
	}
}

// acquire waits for in-flight subprocesses, blocking new ones after MaxDefer.
// It returns false if ctx ends or the drain exceeds MaxDrain.
func (r *Reaper) acquire(ctx context.Context) bool {
	// sync.Cond has no timed wait, so each deadline is a timer that broadcasts
	// and the loop reads the clock itself.
	drainAt := time.Now().Add(MaxDefer)
	giveUpAt := drainAt.Add(MaxDrain)
	for _, at := range []time.Time{drainAt, giveUpAt} {
		timer := time.AfterFunc(time.Until(at), r.wake)
		defer timer.Stop()
	}

	stop := context.AfterFunc(ctx, r.wake)
	defer stop()

	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		if ctx.Err() != nil {
			r.stopDrainingLocked()
			return false
		}
		// Another reap is in progress (it drained on our behalf); let it.
		if r.reaping {
			r.cond.Wait()
			continue
		}
		now := time.Now()
		if r.inFlight == 0 {
			if !now.Before(drainAt) {
				slog.WarnContext(ctx, "Reaped children only after holding off subprocesses",
					slog.Duration("waited", MaxDefer))
			}
			r.draining = false
			r.reaping = true
			return true
		}
		if !now.Before(giveUpAt) {
			// Reaping now could consume the tracked subprocess's exit status.
			slog.ErrorContext(ctx, "Gave up reaping: a subprocess has held off the reaper too long",
				slog.Duration("waited", MaxDefer+MaxDrain), slog.Int("inFlight", r.inFlight))
			r.stopDrainingLocked()
			return false
		}
		if !now.Before(drainAt) {
			r.draining = true
		}
		r.cond.Wait()
	}
}

// wake only signals acquire; timer callbacks may run after acquire returns.
func (r *Reaper) wake() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cond.Broadcast()
}

// stopDrainingLocked ends an abandoned drain. Callers hold r.mu.
func (r *Reaper) stopDrainingLocked() {
	r.draining = false
	r.cond.Broadcast()
}

func (r *Reaper) release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reaping = false
	r.cond.Broadcast()
}
