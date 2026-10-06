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

package actorlock

import (
	"context"
	"testing"
	"time"
)

// The property the split exists for: an activation must not wait on an
// unrelated actor's.
func TestActorLocksDoNotBlockOtherActors(t *testing.T) {
	locks := New()
	if !locks.Lock(context.Background(), "actor-a") {
		t.Fatal("could not take actor-a's lock")
	}
	defer locks.Unlock("actor-a")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if !locks.Lock(ctx, "actor-b") {
		t.Fatal("actor-b blocked on actor-a's lock")
	}
	locks.Unlock("actor-b")
}

// Two RPCs naming one actor still take turns, so a checkpoint cannot
// interleave with a restore of the same sandbox.
func TestActorLocksSerializeTheSameActor(t *testing.T) {
	locks := New()
	if !locks.Lock(context.Background(), "actor-a") {
		t.Fatal("could not take actor-a's lock")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if locks.Lock(ctx, "actor-a") {
		locks.Unlock("actor-a")
		t.Fatal("a second holder took actor-a's lock while it was held")
	}
	locks.Unlock("actor-a")

	// Released, so it is available again.
	if !locks.Lock(context.Background(), "actor-a") {
		t.Fatal("actor-a's lock was not released")
	}
	locks.Unlock("actor-a")
}

// Busy is true from Lock to Unlock and only for the actor that holds it.
func TestActorLocksBusy(t *testing.T) {
	locks := New()
	if locks.Busy("actor-a") {
		t.Fatal("Busy(actor-a) = true before any Lock")
	}
	if !locks.Lock(context.Background(), "actor-a") {
		t.Fatal("could not take actor-a's lock")
	}
	if !locks.Busy("actor-a") {
		t.Error("Busy(actor-a) = false while held")
	}
	if locks.Busy("actor-b") {
		t.Error("Busy(actor-b) = true while only actor-a is held")
	}
	locks.Unlock("actor-a")
	if locks.Busy("actor-a") {
		t.Error("Busy(actor-a) = true after Unlock")
	}
}

// Busy counts waiters as well as the holder. A waiter that gives up, or that
// takes the lock over when the holder releases it, never leaves the actor
// looking idle while a lifecycle RPC still holds the lock.
func TestActorLocksBusyWaiter(t *testing.T) {
	// startWaiter blocks a second Lock behind the holder and returns once it
	// has registered, which is when the entry holds two references.
	startWaiter := func(t *testing.T, locks *Locks, ctx context.Context) <-chan bool {
		t.Helper()
		got := make(chan bool, 1)
		go func() { got <- locks.Lock(ctx, "actor-a") }()
		deadline := time.Now().Add(5 * time.Second)
		for {
			locks.mu.Lock()
			refs := locks.held["actor-a"].refs
			locks.mu.Unlock()
			if refs == 2 {
				return got
			}
			if time.Now().After(deadline) {
				t.Fatal("the second Lock never started waiting")
			}
			time.Sleep(time.Millisecond)
		}
	}

	t.Run("waiter gives up", func(t *testing.T) {
		locks := New()
		if !locks.Lock(context.Background(), "actor-a") {
			t.Fatal("could not take actor-a's lock")
		}
		ctx, cancel := context.WithCancel(context.Background())
		got := startWaiter(t, locks, ctx)
		cancel()
		if <-got {
			t.Fatal("the cancelled waiter took the lock")
		}
		if !locks.Busy("actor-a") {
			t.Error("Busy(actor-a) = false after the waiter gave up, while the holder still holds it")
		}
		locks.Unlock("actor-a")
		if locks.Busy("actor-a") {
			t.Error("Busy(actor-a) = true after the holder released it")
		}
	})

	t.Run("waiter takes over", func(t *testing.T) {
		locks := New()
		if !locks.Lock(context.Background(), "actor-a") {
			t.Fatal("could not take actor-a's lock")
		}
		got := startWaiter(t, locks, context.Background())
		locks.Unlock("actor-a")
		if !<-got {
			t.Fatal("the waiter did not take the lock")
		}
		if !locks.Busy("actor-a") {
			t.Error("Busy(actor-a) = false while the former waiter holds it")
		}
		locks.Unlock("actor-a")
		if locks.Busy("actor-a") {
			t.Error("Busy(actor-a) = true after the last holder released it")
		}
	})
}

// The map must not grow by one entry per actor the worker has ever hosted.
func TestActorLocksForgetIdleActors(t *testing.T) {
	locks := New()
	for _, uid := range []string{"a", "b", "c"} {
		if !locks.Lock(context.Background(), uid) {
			t.Fatalf("could not take %s's lock", uid)
		}
		locks.Unlock(uid)
	}
	locks.mu.Lock()
	defer locks.mu.Unlock()
	if len(locks.held) != 0 {
		t.Errorf("locks retained for %d idle actors, want none", len(locks.held))
	}
}
