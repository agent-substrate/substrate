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
	"testing"
	"time"
)

type fakeClockSetter struct {
	calls []time.Time
	// block makes SetGuestDateTime wait for its context, like a wedged agent.
	block bool
	err   error
}

func (f *fakeClockSetter) SetGuestDateTime(ctx context.Context, now time.Time) error {
	f.calls = append(f.calls, now)
	if f.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.err
}

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
