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

package memborrow

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// want is more than any test pod has to lend
const want = 1 << 40

func ledger(limit, current int64, unbooked *atomic.Int64) *Ledger {
	return &Ledger{
		Unbooked:  func(int64) int64 { return unbooked.Load() },
		PodMemory: func() (int64, int64, error) { return limit, current, nil },
	}
}

func unbookedOf(n int64) *atomic.Int64 {
	var v atomic.Int64
	v.Store(n)
	return &v
}

func TestTake(t *testing.T) {
	const limit = 4 << 30
	for _, tc := range []struct {
		name     string
		current  int64
		unbooked int64
		want     int64
		got      int64
	}{
		{name: "capped by unbooked capacity", current: 1 << 30, unbooked: 2 << 30, want: want, got: 2 << 30},
		{name: "capped by free pod memory", current: limit - 512<<20, unbooked: 2 << 30, want: want, got: 512<<20 - margin},
		{name: "capped by what it wants", current: 0, unbooked: 2 << 30, want: 100 << 20, got: 100 << 20},
		{name: "pod full", current: limit, unbooked: 2 << 30, want: want, got: 0},
		{name: "nothing wanted", current: 0, unbooked: 2 << 30, want: -1, got: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ledger(limit, tc.current, unbookedOf(tc.unbooked)).Take(tc.want); got != tc.got {
				t.Errorf("Take() = %d, want %d", got, tc.got)
			}
		})
	}
}

func TestTakeSharesUnbooked(t *testing.T) {
	l := ledger(4<<30, 0, unbookedOf(2<<30))
	first := l.Take(want)
	if second := l.Take(want); second != 0 {
		t.Errorf("second Take() = %d, want 0 with %d already borrowed", second, first)
	}
	l.Return(first)
	if again := l.Take(want); again != first {
		t.Errorf("Take() after Return = %d, want %d", again, first)
	}
}

func TestTakeWithoutPodLimit(t *testing.T) {
	l := ledger(0, 1<<30, unbookedOf(0))
	if got := l.Take(want); got != want {
		t.Errorf("Take() without a pod limit = %d, want %d", got, int64(want))
	}
	if err := l.Wait(context.Background()); err != nil {
		t.Errorf("Wait() without a pod limit = %v", err)
	}
}

func TestWait(t *testing.T) {
	unbooked := unbookedOf(2 << 30)
	l := ledger(4<<30, 1<<30, unbooked)
	borrowed := l.Take(1 << 30)
	// a placement that still leaves room for the borrow goes ahead
	if err := l.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	// one that would grow into it waits for its return
	unbooked.Store(512 << 20)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := l.Wait(ctx); err == nil {
		t.Fatal("placement into borrowed memory did not wait")
	}
	done := make(chan error, 1)
	go func() { done <- l.Wait(context.Background()) }()
	l.Return(borrowed)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("placement still waiting after the borrow was returned")
	}
}
