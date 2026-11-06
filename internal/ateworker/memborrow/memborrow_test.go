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

package memborrow

import (
	"context"
	"errors"
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
	if tight, _ := l.Tight(want); tight {
		t.Error("Tight() without a pod limit = true, want false")
	}
}

func TestTight(t *testing.T) {
	const limit = 4 << 30
	for _, tc := range []struct {
		name string
		// current and borrowed already include this snapshot's dirty pages and loan.
		current, unbooked, borrowed, lent int64
		want                              bool
	}{
		{name: "room to spare", current: 2 << 30, unbooked: 3 << 30, borrowed: 1 << 30, lent: 1 << 30, want: false},
		{name: "more than a third unbooked", current: 2 << 30, unbooked: 2 << 30, borrowed: 512 << 20, lent: 512 << 20, want: false},
		{name: "at most a third unbooked", current: 2 << 30, unbooked: 2 << 30, borrowed: 1 << 30, lent: 1 << 30, want: true},
		{name: "a placement booked into the loan", current: 2 << 30, unbooked: 512 << 20, borrowed: 1 << 30, lent: 1 << 30, want: true},
		{name: "short of free memory", current: limit - margin + 1, unbooked: 3 << 30, borrowed: 1 << 30, lent: 1 << 30, want: true},
		{name: "nothing lent", current: 0, unbooked: 3 << 30, borrowed: 0, lent: 0, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := ledger(limit, tc.current, unbookedOf(tc.unbooked))
			l.bytes = tc.borrowed
			if got, _ := l.Tight(tc.lent); got != tc.want {
				t.Errorf("Tight() = %v, want %v", got, tc.want)
			}
		})
	}
	failing := &Ledger{
		Unbooked:  func(int64) int64 { return 2 << 30 },
		PodMemory: func() (int64, int64, error) { return 0, 0, errors.New("no cgroup") },
	}
	if tight, _ := failing.Tight(1 << 30); !tight {
		t.Error("Tight() when pod memory can't be read = false, want true")
	}
}

// On microVM, StartWriteback runs before the actor is torn down, so its memory
// is still booked when Tight checks.
func TestTightRightAfterTake(t *testing.T) {
	var current atomic.Int64
	current.Store(1 << 30)
	l := &Ledger{
		Unbooked:  func(int64) int64 { return 3 << 30 },
		PodMemory: func() (int64, int64, error) { return 4 << 30, current.Load(), nil },
	}
	lent := l.Take(1 << 30)
	current.Add(lent) // include the snapshot's dirty pages
	if tight, _ := l.Tight(lent); tight {
		t.Errorf("Tight() right after Take(%d) = true, want false", lent)
	}
}

func TestTightAskedAgainWhenWaitBlocks(t *testing.T) {
	unbooked := unbookedOf(3 << 30)
	l := ledger(4<<30, 1<<30, unbooked)
	lent := l.Take(1 << 30)
	tight, recheck := l.Tight(lent)
	if tight {
		t.Fatal("Tight() with room to spare = true, want false")
	}
	// A new actor books most of the remaining memory and blocks in Wait.
	unbooked.Store(512 << 20)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = l.Wait(ctx) }()
	select {
	case <-recheck:
	case <-time.After(10 * time.Second):
		t.Fatal("Wait held back a placement without closing Tight's channel")
	}
	if tight, _ := l.Tight(lent); !tight {
		t.Error("Tight() with a placement waiting = false, want true")
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

func TestTakeAll(t *testing.T) {
	l := ledger(4<<30, 1<<30, unbookedOf(64<<20))
	if l.TakeAll(128 << 20) {
		t.Error("TakeAll() lent more than is unbooked")
	}
	if got := l.Take(want); got != 64<<20 {
		t.Errorf("Take() after a refused TakeAll = %d, want all %d still unbooked", got, int64(64<<20))
	}
	l = ledger(4<<30, 1<<30, unbookedOf(256<<20))
	if !l.TakeAll(128 << 20) {
		t.Fatal("TakeAll() refused what is unbooked")
	}
	if got := l.Take(want); got != 128<<20 {
		t.Errorf("Take() after TakeAll = %d, want the %d left", got, int64(128<<20))
	}
}
