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
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// fakeGuestAgent stands in for *kata.AgentClient. It records the signals
// stopGuestWorkload delivers and unblocks WaitProcess when the workload is
// configured to die on one of them.
type fakeGuestAgent struct {
	mu      sync.Mutex
	signals []syscall.Signal

	// exitOn is the signal that makes the workload exit. Zero means it never
	// does, which is how the wedged-guest case is set up.
	exitOn syscall.Signal
	// waitErr is what WaitProcess reports once it unblocks, standing in for a
	// dead agent connection rather than a process that stopped.
	waitErr error
	// signalErr maps a signal to the error SignalProcess returns for it.
	signalErr map[syscall.Signal]error

	exited     chan struct{}
	exitedOnce sync.Once
}

func newFakeGuestAgent(exitOn syscall.Signal, waitErr error, signalErr map[syscall.Signal]error) *fakeGuestAgent {
	return &fakeGuestAgent{exitOn: exitOn, waitErr: waitErr, signalErr: signalErr, exited: make(chan struct{})}
}

func (f *fakeGuestAgent) SignalProcess(_ context.Context, _, _ string, signal uint32) error {
	sig := syscall.Signal(signal)

	f.mu.Lock()
	f.signals = append(f.signals, sig)
	f.mu.Unlock()

	if f.exitOn != 0 && f.exitOn == sig {
		f.exitedOnce.Do(func() { close(f.exited) })
	}
	return f.signalErr[sig]
}

func (f *fakeGuestAgent) WaitProcess(ctx context.Context, _, _ string) (int32, error) {
	select {
	case <-f.exited:
		return 0, f.waitErr
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (f *fakeGuestAgent) sentSignals() []syscall.Signal {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]syscall.Signal(nil), f.signals...)
}

// TestStopGuestWorkload covers the SIGTERM-then-SIGKILL escalation against the
// shared drain deadline. The deadlines here are milliseconds rather than the
// production grace period, which is what the package-level vars are for.
func TestStopGuestWorkload(t *testing.T) {
	tests := []struct {
		name string
		// exitOn, waitErr and signalErr configure the fake agent.
		exitOn    syscall.Signal
		waitErr   error
		signalErr map[syscall.Signal]error
		// deadlineIn is the drain deadline relative to the start of the call. A
		// negative value is a deadline the caller has already spent elsewhere.
		deadlineIn  time.Duration
		wantSignals []syscall.Signal
		wantErr     string
	}{
		{
			name:        "workload exits on SIGTERM inside the deadline",
			exitOn:      syscall.SIGTERM,
			deadlineIn:  time.Minute,
			wantSignals: []syscall.Signal{syscall.SIGTERM},
		},
		{
			name:        "workload ignoring SIGTERM is killed at the deadline",
			exitOn:      syscall.SIGKILL,
			deadlineIn:  20 * time.Millisecond,
			wantSignals: []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL},
		},
		{
			// The lock wait ate the whole grace period, so the workload gets
			// SIGTERM and no time at all before the escalation.
			name:        "deadline already spent leaves no grace",
			exitOn:      syscall.SIGKILL,
			deadlineIn:  -time.Second,
			wantSignals: []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL},
		},
		{
			name:        "workload surviving SIGKILL is reported",
			deadlineIn:  20 * time.Millisecond,
			wantSignals: []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL},
			wantErr:     "failed to exit even after SIGKILL",
		},
		{
			name:        "undeliverable SIGTERM does not escalate",
			signalErr:   map[syscall.Signal]error{syscall.SIGTERM: errors.New("ttrpc closed")},
			deadlineIn:  time.Minute,
			wantSignals: []syscall.Signal{syscall.SIGTERM},
			wantErr:     "while propagating SIGTERM to workload",
		},
		{
			// A WaitProcess that errors is a dead agent connection, not a bad
			// exit, so liveness is unknown and the caller is told rather than
			// the workload killed.
			name:        "failed wait does not escalate",
			exitOn:      syscall.SIGTERM,
			waitErr:     errors.New("ttrpc: closed"),
			deadlineIn:  time.Minute,
			wantSignals: []syscall.Signal{syscall.SIGTERM},
			wantErr:     `while waiting for workload "counter" to exit`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			origKillTimeout := workloadKillTimeout
			workloadKillTimeout = 20 * time.Millisecond
			t.Cleanup(func() { workloadKillTimeout = origKillTimeout })

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			f := newFakeGuestAgent(tc.exitOn, tc.waitErr, tc.signalErr)
			err := stopGuestWorkload(ctx, f, "actor-1", "counter", time.Now().Add(tc.deadlineIn))

			switch {
			case tc.wantErr == "" && err != nil:
				t.Errorf("stopGuestWorkload() = %v, want nil", err)
			case tc.wantErr != "" && err == nil:
				t.Errorf("stopGuestWorkload() = nil, want error containing %q", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Errorf("stopGuestWorkload() = %v, want error containing %q", err, tc.wantErr)
			}

			if got := f.sentSignals(); !reflect.DeepEqual(got, tc.wantSignals) {
				t.Errorf("signals delivered = %v, want %v", got, tc.wantSignals)
			}
		})
	}
}

// TestStopGuestWorkloadHonorsParentCancellation asserts that a cancelled shutdown
// context stops the drain instead of escalating: ateom is going away anyway, and
// the guest goes down with the VM.
func TestStopGuestWorkloadHonorsParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := newFakeGuestAgent(0, nil, nil)

	// Cancel once SIGTERM has been delivered and the wait is under way.
	go func() {
		for {
			if len(f.sentSignals()) > 0 {
				cancel()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	err := stopGuestWorkload(ctx, f, "actor-1", "counter", time.Now().Add(time.Minute))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("stopGuestWorkload() = %v, want context.Canceled", err)
	}
	if got := f.sentSignals(); !reflect.DeepEqual(got, []syscall.Signal{syscall.SIGTERM}) {
		t.Errorf("signals delivered = %v, want [SIGTERM]", got)
	}
}
