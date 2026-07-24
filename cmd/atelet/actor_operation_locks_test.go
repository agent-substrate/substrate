// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestActorOperationLocks(t *testing.T) {
	var locks actorOperationLocks

	releaseActor1, ok := locks.tryLock("actor-1")
	if !ok {
		t.Fatal("first lock for actor-1 failed")
	}
	if _, ok := locks.tryLock("actor-1"); ok {
		t.Error("overlapping lock for actor-1 succeeded")
	}
	releaseActor2, ok := locks.tryLock("actor-2")
	if !ok {
		t.Error("lock for a different actor failed")
	}

	releaseActor1()
	releaseActor1Again, ok := locks.tryLock("actor-1")
	if !ok {
		t.Error("actor-1 could not be locked again after unlock")
	}
	releaseActor1Again()
	releaseActor2()
	if len(locks.locked) != 0 {
		t.Errorf("lock map contains %d entries after unlocks, want 0", len(locks.locked))
	}
}

func TestActorOperationLocksStaleReleaseDoesNotUnlockNewLease(t *testing.T) {
	var locks actorOperationLocks

	staleRelease, ok := locks.tryLock("actor-1")
	if !ok {
		t.Fatal("first lock for actor-1 failed")
	}
	staleRelease()

	currentRelease, ok := locks.tryLock("actor-1")
	if !ok {
		t.Fatal("second lock for actor-1 failed")
	}
	staleRelease()
	if overlappingRelease, acquired := locks.tryLock("actor-1"); acquired {
		overlappingRelease()
		t.Error("stale release unlocked the current lease")
	}

	currentRelease()
}

func TestActorOperationLockHeldUntilExplicitRelease(t *testing.T) {
	var locks actorOperationLocks

	ctx, cancel := context.WithCancel(context.Background())
	release, ok := locks.tryLock("actor-1")
	if !ok {
		t.Fatal("first lock for actor-1 failed")
	}

	cancel()
	<-ctx.Done()
	if overlappingRelease, acquired := locks.tryLock("actor-1"); acquired {
		overlappingRelease()
		t.Error("context cancellation released the actor lock")
	}

	release()
	reacquiredRelease, ok := locks.tryLock("actor-1")
	if !ok {
		t.Error("actor-1 could not be locked after explicit release")
		return
	}
	reacquiredRelease()
}

func TestActorOperationLocksConcurrent(t *testing.T) {
	var locks actorOperationLocks
	const callers = 64

	start := make(chan struct{})
	release := make(chan struct{})
	results := make(chan bool, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			endOperation, acquired := locks.tryLock("actor-1")
			results <- acquired
			if acquired {
				<-release
				endOperation()
			}
		}()
	}

	close(start)
	acquired := 0
	for range callers {
		if <-results {
			acquired++
		}
	}
	close(release)
	wg.Wait()

	if acquired != 1 {
		t.Errorf("%d concurrent callers acquired the same actor lock, want 1", acquired)
	}
	if len(locks.locked) != 0 {
		t.Errorf("lock map contains %d entries after concurrent unlock, want 0", len(locks.locked))
	}
}

func TestRPCBoundariesRejectOverlappingActorOperation(t *testing.T) {
	s := &AteomHerder{}
	ctx := context.Background()

	tests := []struct {
		name    string
		request func() error
	}{
		{
			name: "Run",
			request: func() error {
				req := validRunRequest()
				req.SandboxAssets = &ateletpb.SandboxAssets{
					SandboxClass: "gvisor",
					Assets: map[string]*ateletpb.ArchAssets{
						runtime.GOARCH: {
							Files: map[string]*ateletpb.AssetFile{
								"runsc": {
									Url:    "gs://bucket/runsc",
									Sha256: strings.Repeat("a", 64),
								},
							},
						},
					},
				}
				_, err := s.Run(ctx, req)
				return err
			},
		},
		{
			name: "Checkpoint",
			request: func() error {
				_, err := s.Checkpoint(ctx, validCheckpointRequest())
				return err
			},
		},
		{
			name: "Restore",
			request: func() error {
				_, err := s.Restore(ctx, validRestoreRequest())
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const actorUID = "123e4567-e89b-12d3-a456-426614174000"
			endOperation, ok := s.actorOperations.tryLock(actorUID)
			if !ok {
				t.Fatalf("failed to set up lock for %s", actorUID)
			}
			t.Cleanup(endOperation)

			err := tt.request()
			if got := status.Code(err); got != codes.Aborted {
				t.Errorf("status.Code(err) = %v, want %v (err: %v)", got, codes.Aborted, err)
			}
		})
	}
}
