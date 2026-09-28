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
