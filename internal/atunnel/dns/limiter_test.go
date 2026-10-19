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

package dns

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestNewLimiter(t *testing.T) {
	lim := newLimiter()
	if lim.inFlight == nil || lim.inFlight.max != maxInFlight {
		t.Errorf("inFlight = %+v, want max=%d", lim.inFlight, maxInFlight)
	}
	if lim.connections == nil || lim.connections.max != maxConnections {
		t.Errorf("connections = %+v, want max=%d", lim.connections, maxConnections)
	}
	if got := lim.inFlight.occupied(); got != 0 {
		t.Errorf("initial inFlight.occupied() = %d, want 0", got)
	}
	if got := lim.connections.occupied(); got != 0 {
		t.Errorf("initial connections.occupied() = %d, want 0", got)
	}
}

func TestLimiterSlotsAcquireAndRelease(t *testing.T) {
	const cap = 3
	s := &limiterSlots{max: cap}

	for i := int32(1); i <= cap; i++ {
		if !s.tryAcquire() {
			t.Fatalf("tryAcquire %d failed before reaching capacity %d", i, cap)
		}
		if got := s.occupied(); got != i {
			t.Errorf("occupied() = %d after %d acquisitions, want %d", got, i, i)
		}
	}

	// At capacity: further acquisitions fail and do not change occupied count.
	if s.tryAcquire() {
		t.Fatal("tryAcquire succeeded at capacity")
	}
	if got := s.occupied(); got != cap {
		t.Errorf("occupied() after rejected tryAcquire = %d, want %d", got, cap)
	}

	// Releasing one slot allows one more acquisition.
	s.release()
	if got := s.occupied(); got != cap-1 {
		t.Errorf("occupied() after release = %d, want %d", got, cap-1)
	}
	if !s.tryAcquire() {
		t.Fatal("tryAcquire failed after releasing a slot")
	}

	// Drain all slots.
	for i := cap - 1; i >= 0; i-- {
		s.release()
		if got := s.occupied(); got != int32(i) {
			t.Errorf("occupied() = %d, want %d", got, i)
		}
	}
}

func TestLimiterSlotsNilReceiver(t *testing.T) {
	var s *limiterSlots
	if !s.tryAcquire() {
		t.Error("nil limiterSlots.tryAcquire() = false, want true")
	}
	// Must not panic.
	s.release()
}

func TestLimiterSlotsReleaseUnderflowPanics(t *testing.T) {
	s := &limiterSlots{max: 1}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when releasing an unacquired slot, got none")
		}
	}()
	s.release()
}

func TestLimiterSlotsConcurrent(t *testing.T) {
	const (
		maxSlots   = 8
		goroutines = 32
		iterations = 200
	)
	s := &limiterSlots{max: maxSlots}

	var active atomic.Int32
	var maxObserved atomic.Int32
	var wg sync.WaitGroup

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				if !s.tryAcquire() {
					continue
				}
				cur := active.Add(1)
				for {
					prev := maxObserved.Load()
					if cur <= prev || maxObserved.CompareAndSwap(prev, cur) {
						break
					}
				}
				active.Add(-1)
				s.release()
			}
		}()
	}
	wg.Wait()

	if got := maxObserved.Load(); got > maxSlots {
		t.Errorf("max concurrent slots held = %d, exceeds limit %d", got, maxSlots)
	}
	if got := s.occupied(); got != 0 {
		t.Errorf("occupied() after all goroutines finished = %d, want 0", got)
	}
}
