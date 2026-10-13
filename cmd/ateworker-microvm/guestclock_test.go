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
	"fmt"
	"testing"
	"time"
)

type fakeClockSetter struct {
	calls []time.Time
	// block makes SetGuestDateTime wait for its context, like a wedged agent.
	block bool
	err   error
	// clock, when set, is advanced by delays[i] during call i (by the last
	// entry once delays runs out), standing in for the call's round trip.
	clock  *fakeClock
	delays []time.Duration
}

func (f *fakeClockSetter) SetGuestDateTime(ctx context.Context, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.calls = append(f.calls, now)
	if f.clock != nil && len(f.delays) > 0 {
		f.clock.t = f.clock.t.Add(f.delays[min(len(f.calls), len(f.delays))-1])
	}
	if f.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.err
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func TestSyncGuestClock(t *testing.T) {
	now := time.Unix(1_700_000_000, 123_456_000)
	clock := func() time.Time { return now }

	t.Run("mshv sets host time", func(t *testing.T) {
		f := &fakeClockSetter{}
		if err := syncGuestClock(t.Context(), hypervisorMSHV, f, clock, guestClockTimeout); err != nil {
			t.Fatalf("syncGuestClock: %v", err)
		}
		if len(f.calls) != 1 || !f.calls[0].Equal(now) {
			t.Errorf("SetGuestDateTime calls = %v, want [%v]", f.calls, now)
		}
	})

	// Cloud Hypervisor advances a KVM guest's clock itself.
	t.Run("kvm is a no-op", func(t *testing.T) {
		f := &fakeClockSetter{err: errors.New("must not be called")}
		if err := syncGuestClock(t.Context(), hypervisorKVM, f, clock, guestClockTimeout); err != nil {
			t.Fatalf("syncGuestClock: %v", err)
		}
		if len(f.calls) != 0 {
			t.Errorf("SetGuestDateTime called %d times on KVM", len(f.calls))
		}
	})

	t.Run("agent error fails closed", func(t *testing.T) {
		agentErr := errors.New("agent unavailable")
		f := &fakeClockSetter{err: agentErr}
		if err := syncGuestClock(t.Context(), hypervisorMSHV, f, clock, guestClockTimeout); !errors.Is(err, agentErr) {
			t.Errorf("syncGuestClock() = %v, want %v", err, agentErr)
		}
	})

	t.Run("late reply is discarded and the clock set again", func(t *testing.T) {
		c := &fakeClock{t: now}
		f := &fakeClockSetter{clock: c, delays: []time.Duration{3 * time.Second, guestClockMaxLag}}
		if err := syncGuestClock(t.Context(), hypervisorMSHV, f, c.now, guestClockTimeout); err != nil {
			t.Fatalf("syncGuestClock: %v", err)
		}
		// The second set carries the host time after the late reply, not
		// the stale timestamp of the first.
		want := []time.Time{now, now.Add(3 * time.Second)}
		if len(f.calls) != len(want) || !f.calls[0].Equal(want[0]) || !f.calls[1].Equal(want[1]) {
			t.Errorf("SetGuestDateTime calls = %v, want %v", f.calls, want)
		}
	})

	t.Run("replies that stay late within the late bound are accepted after the last attempt", func(t *testing.T) {
		c := &fakeClock{t: now}
		f := &fakeClockSetter{clock: c, delays: []time.Duration{guestClockMaxLateLag}}
		if err := syncGuestClock(t.Context(), hypervisorMSHV, f, c.now, guestClockTimeout); err != nil {
			t.Fatalf("syncGuestClock: %v", err)
		}
		if len(f.calls) != guestClockAttempts {
			t.Errorf("SetGuestDateTime called %d times, want %d", len(f.calls), guestClockAttempts)
		}
	})

	t.Run("replies that stay later than the late bound fail after the last attempt", func(t *testing.T) {
		c := &fakeClock{t: now}
		f := &fakeClockSetter{clock: c, delays: []time.Duration{guestClockMaxLateLag + time.Millisecond}}
		err := syncGuestClock(t.Context(), hypervisorMSHV, f, c.now, guestClockTimeout)
		if err == nil {
			t.Fatal("syncGuestClock() = nil, want an error for a set later than the late bound")
		}
		if len(f.calls) != guestClockAttempts {
			t.Errorf("SetGuestDateTime called %d times, want %d", len(f.calls), guestClockAttempts)
		}
	})

	// Only the last set's lag counts: an early slow set followed by a slow
	// last one within the late bound is accepted, and one slower than it is not.
	t.Run("only the last set's lag decides", func(t *testing.T) {
		c := &fakeClock{t: now}
		f := &fakeClockSetter{clock: c, delays: []time.Duration{3 * time.Second, 3 * time.Second, 500 * time.Millisecond}}
		if err := syncGuestClock(t.Context(), hypervisorMSHV, f, c.now, guestClockTimeout); err != nil {
			t.Fatalf("syncGuestClock: %v", err)
		}
		c = &fakeClock{t: now}
		f = &fakeClockSetter{clock: c, delays: []time.Duration{500 * time.Millisecond, 500 * time.Millisecond, 2 * time.Second}}
		if err := syncGuestClock(t.Context(), hypervisorMSHV, f, c.now, guestClockTimeout); err == nil {
			t.Fatal("syncGuestClock() = nil, want an error when the last set lags more than the late bound")
		}
	})

	// The host wall clock steps during the call. The guest was set to the
	// wall time before the step, so it is off by the step: the set is
	// discarded and made again from the stepped clock.
	for _, step := range []time.Duration{-2 * time.Second, 2 * time.Second} {
		t.Run(fmt.Sprintf("host clock step of %v during the call is set again", step), func(t *testing.T) {
			c := &fakeClock{t: now}
			f := &fakeClockSetter{clock: c, delays: []time.Duration{step, time.Millisecond}}
			if err := syncGuestClock(t.Context(), hypervisorMSHV, f, c.now, guestClockTimeout); err != nil {
				t.Fatalf("syncGuestClock: %v", err)
			}
			want := []time.Time{now, now.Add(step)}
			if len(f.calls) != len(want) || !f.calls[0].Equal(want[0]) || !f.calls[1].Equal(want[1]) {
				t.Errorf("SetGuestDateTime calls = %v, want %v", f.calls, want)
			}
		})
	}

	t.Run("a step back on every call fails after the last attempt", func(t *testing.T) {
		c := &fakeClock{t: now}
		f := &fakeClockSetter{clock: c, delays: []time.Duration{-guestClockMaxLateLag - time.Millisecond}}
		if err := syncGuestClock(t.Context(), hypervisorMSHV, f, c.now, guestClockTimeout); err == nil {
			t.Fatal("syncGuestClock() = nil, want an error when the guest ends further ahead than the late bound")
		}
		if len(f.calls) != guestClockAttempts {
			t.Errorf("SetGuestDateTime called %d times, want %d", len(f.calls), guestClockAttempts)
		}
	})

	// The guest gets wall time only, with no monotonic reading attached.
	t.Run("sets wall time", func(t *testing.T) {
		f := &fakeClockSetter{}
		if err := syncGuestClock(t.Context(), hypervisorMSHV, f, time.Now, guestClockTimeout); err != nil {
			t.Fatalf("syncGuestClock: %v", err)
		}
		if len(f.calls) != 1 || f.calls[0] != f.calls[0].Round(0) {
			t.Errorf("SetGuestDateTime calls = %v, want one wall-only time", f.calls)
		}
	})

	t.Run("wedged agent times out", func(t *testing.T) {
		f := &fakeClockSetter{block: true}
		start := time.Now()
		err := syncGuestClock(t.Context(), hypervisorMSHV, f, clock, 50*time.Millisecond)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("syncGuestClock() = %v, want %v", err, context.DeadlineExceeded)
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Errorf("syncGuestClock() returned after %v, want the 50ms bound", elapsed)
		}
	})
}
