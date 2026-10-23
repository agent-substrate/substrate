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

package e2e

import (
	"context"
	"errors"
	"testing"
	"time"
)

func fixedSize(n int) func(context.Context) (int, error) {
	return func(context.Context) (int, error) { return n, nil }
}

func TestWorkerSlotsBlocksWhenFull(t *testing.T) {
	slots := NewWorkerSlots("pool", fixedSize(2))
	ctx := context.Background()
	noWait := func(int) { t.Errorf("waited with a slot free") }

	first, err := slots.acquire(ctx, noWait)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if _, err := slots.acquire(ctx, noWait); err != nil {
		t.Fatalf("second acquire: %v", err)
	}

	waited := make(chan int, 1)
	acquired := make(chan error, 1)
	go func() {
		_, err := slots.acquire(ctx, func(held int) { waited <- held })
		acquired <- err
	}()

	if held := <-waited; held != 2 {
		t.Errorf("waiting reported %d workers held, want 2", held)
	}
	select {
	case err := <-acquired:
		t.Fatalf("third acquire returned (%v) with both slots held", err)
	case <-time.After(50 * time.Millisecond):
	}

	first()
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("third acquire after a release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("third acquire still blocked after a slot was released")
	}
}

func TestWorkerSlotsGivesUpWithContext(t *testing.T) {
	slots := NewWorkerSlots("pool", fixedSize(1))
	if _, err := slots.acquire(context.Background(), func(int) {}); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := slots.acquire(ctx, func(int) {}); !errors.Is(err, context.Canceled) {
		t.Fatalf("acquire on a full pool with a canceled context = %v, want %v", err, context.Canceled)
	}
}

func TestWorkerSlotsSizeErrors(t *testing.T) {
	sizeErr := errors.New("no such pool")
	for _, tc := range []struct {
		name string
		size func(context.Context) (int, error)
	}{
		{"lookup fails", func(context.Context) (int, error) { return 0, sizeErr }},
		{"no workers", fixedSize(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			slots := NewWorkerSlots("pool", func(ctx context.Context) (int, error) {
				calls++
				if calls == 1 {
					return tc.size(ctx)
				}
				return 1, nil
			})
			if _, err := slots.acquire(context.Background(), func(int) {}); err == nil {
				t.Fatal("first acquire succeeded on a pool that could not be sized")
			}
			release, err := slots.acquire(context.Background(), func(int) {})
			if err != nil {
				t.Fatalf("second acquire after transient sizing error failed: %v", err)
			}
			release()
			if _, err := slots.acquire(context.Background(), func(int) {}); err != nil {
				t.Fatalf("third acquire failed: %v", err)
			}
			if calls != 2 {
				t.Errorf("sized the pool %d times, want 2 (retried after failure, cached after success)", calls)
			}
		})
	}
}

func TestWorkerSlotsAcquireReleasesAfterActorCleanup(t *testing.T) {
	slots := NewWorkerSlots("pool", fixedSize(1))
	actorCleanedUp := false
	t.Run("holder", func(t *testing.T) {
		slots.Acquire(t, context.Background())
		t.Cleanup(func() {
			if len(slots.slots) != 1 {
				t.Errorf("slot released before actor cleanup ran")
			}
			actorCleanedUp = true
		})
	})
	if !actorCleanedUp {
		t.Fatal("actor cleanup did not run")
	}
	if len(slots.slots) != 0 {
		t.Fatalf("%d slots still held after holder subtest finished, want 0", len(slots.slots))
	}
}
