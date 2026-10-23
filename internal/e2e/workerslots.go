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
	"fmt"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// acquireDeadlineMargin is how long before the test binary's deadline
// Acquire stops waiting, leaving that long for cleanups.
const acquireDeadlineMargin = time.Minute

// WorkerSlots bounds how many parallel tests in one binary hold an actor on a
// WorkerPool at once to the number of workers it has.
//
// ResumeActorAwaitCapacity waits a fixed budget for a free worker, which
// covers neighbors holding a worker briefly. A suite whose parallel tests
// outnumber a small pool's workers would instead queue behind each other for
// longer than that budget. Taking a slot before resuming makes those tests
// wait their turn, for as long as the test binary's timeout allows, while the
// work they do before needing an actor (deploying origins) still overlaps.
type WorkerSlots struct {
	name string
	size func(context.Context) (int, error)

	mu    sync.Mutex
	slots chan struct{}
}

// NewWorkerSlots returns slots for the pool called name, sized on first use
// by size.
func NewWorkerSlots(name string, size func(context.Context) (int, error)) *WorkerSlots {
	return &WorkerSlots{name: name, size: size}
}

// WorkerPoolReplicas returns the replicas the WorkerPool namespace/name
// declares, for use as a WorkerSlots size.
func WorkerPoolReplicas(ctx context.Context, namespace, name string) (int, error) {
	pool, err := GetClients().SubstrateK8s.AteV1().WorkerPools(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("getting WorkerPool %s/%s: %w", namespace, name, err)
	}
	return int(pool.Spec.Replicas), nil
}

// Acquire takes a slot, waiting while every one is held, and gives it back
// when t ends. Call it before creating the actor, so the actor's own cleanup
// (registered later, so run earlier) has suspended it by the time the slot is
// released.
//
// The wait gives up a minute before the test binary's deadline (when the
// remaining time exceeds a minute), so a test still queued then fails with its
// cleanups run instead of being killed.
func (s *WorkerSlots) Acquire(t *testing.T, ctx context.Context) {
	t.Helper()
	if deadline, ok := t.Deadline(); ok {
		if cutoff := deadline.Add(-acquireDeadlineMargin); cutoff.After(time.Now()) {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, cutoff)
			defer cancel()
		}
	}
	release, err := s.acquire(ctx, func(held int) {
		t.Logf("all %d workers of %s are held by other tests; waiting for one", held, s.name)
	})
	if err != nil {
		t.Fatalf("acquiring a worker of %s: %v", s.name, err)
	}
	t.Cleanup(release)
}

// acquire is Acquire without the testing.T. waiting is called once if no slot
// is free on arrival.
func (s *WorkerSlots) acquire(ctx context.Context, waiting func(held int)) (func(), error) {
	slots, err := s.initSlots(ctx)
	if err != nil {
		return nil, err
	}

	release := func() { <-slots }
	select {
	case slots <- struct{}{}:
		return release, nil
	default:
	}
	waiting(cap(slots))
	select {
	case slots <- struct{}{}:
		return release, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *WorkerSlots) initSlots(ctx context.Context) (chan struct{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.slots != nil {
		return s.slots, nil
	}
	n, err := s.size(ctx)
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, fmt.Errorf("pool has %d workers", n)
	}
	s.slots = make(chan struct{}, n)
	return s.slots, nil
}
