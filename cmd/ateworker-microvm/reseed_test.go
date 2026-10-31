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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Short durations keep these tests well under a second.
const (
	testAttemptTimeout = 30 * time.Millisecond
	testBudget         = 350 * time.Millisecond
)

var testNonce = []byte("nonce")

// fakeAgentConn is a kata-agent connection whose ReseedRandomDev runs reseed.
type fakeAgentConn struct {
	reseed func(ctx context.Context, data []byte) error
	closed atomic.Bool
}

func (c *fakeAgentConn) ReseedRandomDev(ctx context.Context, data []byte) error {
	return c.reseed(ctx, data)
}

func (c *fakeAgentConn) Close() error {
	c.closed.Store(true)
	return nil
}

// hang blocks until the attempt's context ends, as a stalled agent does.
func hang(ctx context.Context, _ []byte) error {
	<-ctx.Done()
	return ctx.Err()
}

func answer(context.Context, []byte) error { return nil }

func busy(context.Context, []byte) error { return errors.New("agent busy") }

// fakeDialer hands out fresh connections from next and records each one.
type fakeDialer struct {
	mu    sync.Mutex
	next  func(n int) (*fakeAgentConn, error)
	conns []*fakeAgentConn
}

func (d *fakeDialer) dial(context.Context) (agentReseeder, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	c, err := d.next(len(d.conns) + 1)
	if err != nil {
		d.conns = append(d.conns, nil)
		return nil, err
	}
	d.conns = append(d.conns, c)
	return c, nil
}

func (d *fakeDialer) calls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.conns)
}

// requireAllClosed fails the test if a fresh connection was left open.
func (d *fakeDialer) requireAllClosed(t *testing.T) {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, c := range d.conns {
		if c != nil && !c.closed.Load() {
			t.Errorf("fresh connection %d was not closed", i+1)
		}
	}
}

// requireOpen fails the test if the caller's connection was closed.
func requireOpen(t *testing.T, first *fakeAgentConn) {
	t.Helper()
	if first.closed.Load() {
		t.Error("closed the caller's connection")
	}
}

func freshConns(reseed func(context.Context, []byte) error) func(int) (*fakeAgentConn, error) {
	return func(int) (*fakeAgentConn, error) { return &fakeAgentConn{reseed: reseed}, nil }
}

func guestStoppedDial(int) (*fakeAgentConn, error) {
	return nil, fmt.Errorf("%w (cloud-hypervisor removed %q)", errGuestStopped, "/run/vsock.sock")
}

func TestReseedGuestCRNG(t *testing.T) {
	var calls int
	var gotNonce []byte
	var gotDeadline time.Time
	start := time.Now()
	first := &fakeAgentConn{reseed: func(ctx context.Context, data []byte) error {
		calls++
		gotNonce = append([]byte(nil), data...)
		gotDeadline, _ = ctx.Deadline()
		return nil
	}}
	d := &fakeDialer{next: freshConns(answer)}
	if err := reseedGuestCRNG(t.Context(), "actor-a", first, d.dial); err != nil {
		t.Fatalf("reseedGuestCRNG: %v", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
	if n := d.calls(); n != 0 {
		t.Errorf("dial calls = %d, want 0 after the first attempt succeeded", n)
	}
	if len(gotNonce) != 32 {
		t.Errorf("nonce length = %d, want 32", len(gotNonce))
	}
	if gotDeadline.IsZero() || gotDeadline.Before(start.Add(reseedAttemptTimeout)) || gotDeadline.After(time.Now().Add(reseedAttemptTimeout)) {
		t.Errorf("call deadline = %v, want about %v after the call started at %v", gotDeadline, reseedAttemptTimeout, start)
	}
	requireOpen(t, first)
}

func TestReseedWithRetries(t *testing.T) {
	for _, tc := range []struct {
		name string
		// first is the caller's connection; next hands out fresh ones.
		first func(context.Context, []byte) error
		next  func(n int) (*fakeAgentConn, error)
		// cancel makes the first attempt cancel the caller's context.
		cancel                  bool
		attemptTimeout, budget  time.Duration
		wantErr                 bool
		wantErrIs, wantErrIsNot error
		wantErrText             string
		// wantDials is the exact dial count; -1 skips the check.
		wantDials int
		check     func(t *testing.T, err error, d *fakeDialer, logs string)
	}{
		{
			name: "retry on a fresh connection", first: hang,
			next: freshConns(func(_ context.Context, data []byte) error {
				if !bytes.Equal(data, testNonce) {
					return fmt.Errorf("retry sent nonce %q, want %q", data, testNonce)
				}
				return nil
			}),
			wantDials: 1,
		},
		{
			name: "gives up after the budget", first: hang,
			next: func(n int) (*fakeAgentConn, error) {
				if n%2 == 0 {
					return &fakeAgentConn{reseed: busy}, nil
				}
				return &fakeAgentConn{reseed: hang}, nil
			},
			wantErr: true, wantErrIs: context.DeadlineExceeded, wantDials: -1,
			check: func(t *testing.T, err error, d *fakeDialer, _ string) {
				if want := fmt.Sprintf("over %d attempts", d.calls()+1); !strings.Contains(err.Error(), want) {
					t.Errorf("error = %q, want it to report %q", err, want)
				}
			},
		},
		{
			name: "dial error, then success", first: busy,
			next: func(n int) (*fakeAgentConn, error) {
				if n == 1 {
					return nil, errors.New("connection refused")
				}
				return &fakeAgentConn{reseed: answer}, nil
			},
			wantDials: 2,
			check: func(t *testing.T, _ error, _ *fakeDialer, logs string) {
				if !strings.Contains(logs, "outcome="+reseedOutcomeDialError) {
					t.Errorf("logs do not record a %s attempt:\n%s", reseedOutcomeDialError, logs)
				}
			},
		},
		{
			// A long attempt timeout and budget, so only the cancel ends the call quickly.
			name: "caller cancels", first: hang, next: freshConns(hang), cancel: true,
			attemptTimeout: time.Minute, budget: time.Minute,
			wantErr: true, wantErrIs: context.Canceled, wantDials: 0,
			check: func(t *testing.T, err error, _ *fakeDialer, logs string) {
				// The caller gave up, not the guest: no give-up warning.
				if strings.Contains(logs, "reseed gave up") || strings.Contains(err.Error(), "no successful answer") {
					t.Errorf("a caller cancel was reported as the reseed giving up: %v\n%s", err, logs)
				}
			},
		},
		{
			// A stop during the reseed stays retryable for the caller, as before.
			name: "guest stopped", first: busy, next: guestStoppedDial,
			wantErr: true, wantErrIsNot: errGuestStopped, wantErrText: "guest stopped during the CRNG reseed",
			wantDials: 1,
		},
		{
			name: "pause between instant failures", first: busy, next: freshConns(busy),
			wantErr: true, wantDials: -1,
			check: func(t *testing.T, _ error, d *fakeDialer, _ string) {
				// With reseedRetryPause between attempts the budget fits at most
				// budget/pause + 1 attempts; without it, instant failures would loop
				// thousands of times.
				attempts := d.calls() + 1
				if maxAttempts := int(testBudget/reseedRetryPause) + 1; attempts > maxAttempts {
					t.Errorf("attempts = %d, want at most %d with a %v pause", attempts, maxAttempts, reseedRetryPause)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			old := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(old) })

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reseed := tc.first
			if tc.cancel {
				reseed = func(c context.Context, data []byte) error {
					cancel()
					return tc.first(c, data)
				}
			}
			first := &fakeAgentConn{reseed: reseed}
			d := &fakeDialer{next: tc.next}
			attemptTimeout, budget := testAttemptTimeout, testBudget
			if tc.attemptTimeout != 0 {
				attemptTimeout, budget = tc.attemptTimeout, tc.budget
			}

			err := reseedWithRetries(ctx, "actor-a", first, d.dial, testNonce, attemptTimeout, budget)
			if (err != nil) != tc.wantErr {
				t.Fatalf("reseedWithRetries error = %v, want error %v", err, tc.wantErr)
			}
			if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
				t.Errorf("error = %v, want it to match %v", err, tc.wantErrIs)
			}
			if tc.wantErrIsNot != nil && errors.Is(err, tc.wantErrIsNot) {
				t.Errorf("error = %v matches %v, want it not to", err, tc.wantErrIsNot)
			}
			if tc.wantErrText != "" && !strings.Contains(err.Error(), tc.wantErrText) {
				t.Errorf("error = %q, want it to contain %q", err, tc.wantErrText)
			}
			if tc.wantDials >= 0 && d.calls() != tc.wantDials {
				t.Errorf("dial calls = %d, want %d", d.calls(), tc.wantDials)
			}
			if tc.check != nil {
				tc.check(t, err, d, logs.String())
			}
			d.requireAllClosed(t)
			requireOpen(t, first)
		})
	}
}

// guestStoppedReseedErr runs a reseed whose retry finds the guest stopped, and
// returns reseedWithRetries' error.
func guestStoppedReseedErr(t *testing.T) error {
	t.Helper()
	first := &fakeAgentConn{reseed: busy}
	d := &fakeDialer{next: guestStoppedDial}
	err := reseedWithRetries(t.Context(), "actor-a", first, d.dial, testNonce, testAttemptTimeout, testBudget)
	requireOpen(t, first)
	return err
}
