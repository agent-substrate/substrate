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
	"sync"
	"sync/atomic"
	"testing"
)

func TestActorLocks(t *testing.T) {
	var l actorLocks

	releaseA1, ok := l.tryLock("actor-a")
	if !ok {
		t.Fatal("tryLock on a free actor returned false")
	}
	if _, ok := l.tryLock("actor-a"); ok {
		t.Error("tryLock on a held actor returned true")
	}

	// Locks are per actor: a busy actor must not block any other.
	releaseB, ok := l.tryLock("actor-b")
	if !ok {
		t.Error("tryLock on a different actor returned false")
	}
	releaseB()

	// Holder 1 releases the lock.
	releaseA1()

	// Holder 2 acquires the lock for the same actor.
	releaseA2, ok := l.tryLock("actor-a")
	if !ok {
		t.Fatal("tryLock after release returned false")
	}
	defer releaseA2()

	// Idempotency: a second release from Holder 1 must not free Holder 2's claim.
	releaseA1()
	if _, ok := l.tryLock("actor-a"); ok {
		t.Error("stale release from earlier holder freed a later holder's lock")
	}

	// Released entries are dropped rather than accumulating one per actor seen.
	l.mu.Lock()
	held := len(l.held)
	l.mu.Unlock()
	if held != 1 {
		t.Errorf("held = %d entries, want 1 (only the outstanding lock)", held)
	}
}

func TestActorLocks_ConcurrentContention(t *testing.T) {
	var l actorLocks
	const goroutines = 20
	const iterations = 100
	const actorUID = "actor-contended"

	var wg sync.WaitGroup
	var activeHolders atomic.Int32

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				release, ok := l.tryLock(actorUID)
				if !ok {
					continue
				}
				curr := activeHolders.Add(1)
				if curr > 1 {
					t.Errorf("multiple goroutines held the lock concurrently: %d", curr)
				}

				activeHolders.Add(-1)
				release()
				release() // test idempotency under concurrency
			}
		}()
	}

	wg.Wait()

	// All entries should be released.
	l.mu.Lock()
	held := len(l.held)
	l.mu.Unlock()
	if held != 0 {
		t.Errorf("held = %d entries after all releases, want 0", held)
	}
}
