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

package controlapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/ateredis"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newLockTestWorkflow(t *testing.T) (*miniredis.Miniredis, *ActorWorkflow) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{mr.Addr()}})
	return mr, &ActorWorkflow{
		store:            ateredis.NewPersistence(rdb),
		workflowDeadline: 30 * time.Second,
	}
}

func TestAcquireActorLock_HeartbeatKeepsLockAlivePastTTL(t *testing.T) {
	mr, w := newLockTestWorkflow(t)

	lockTTL := 150 * time.Millisecond
	heartbeat := 40 * time.Millisecond

	ctx, release, err := w.acquireActorLock(context.Background(), "actor-1", lockTTL, heartbeat)
	if err != nil {
		t.Fatalf("acquireActorLock: %v", err)
	}
	defer release()

	// Wait through multiple TTLs. If the heartbeat is working the lock key
	// must still be in Redis — its TTL is being PEXPIRE'd back to `lockTTL`
	// every `heartbeat`.
	time.Sleep(4 * lockTTL)

	if !mr.Exists("lock:actor:actor-1") {
		t.Fatalf("lock key disappeared from Redis despite heartbeat; ctx err=%v cause=%v", ctx.Err(), context.Cause(ctx))
	}
	if ctx.Err() != nil {
		t.Fatalf("workflow ctx cancelled while heartbeat was healthy: err=%v cause=%v", ctx.Err(), context.Cause(ctx))
	}
}

func TestAcquireActorLock_LostLockCancelsWorkflow(t *testing.T) {
	mr, w := newLockTestWorkflow(t)

	lockTTL := 200 * time.Millisecond
	heartbeat := 30 * time.Millisecond

	ctx, release, err := w.acquireActorLock(context.Background(), "actor-2", lockTTL, heartbeat)
	if err != nil {
		t.Fatalf("acquireActorLock: %v", err)
	}
	defer release()

	// Simulate a peer stealing the lock (or the TTL lapsing and someone else
	// re-acquiring): wipe our key so the next heartbeat refresh's CAS fails.
	mr.Del("lock:actor:actor-2")

	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("workflow ctx was not cancelled after lock was lost")
	}

	if cause := context.Cause(ctx); !errors.Is(cause, errLostActorLock) {
		t.Errorf("context.Cause = %v, want errLostActorLock", cause)
	}
}

func TestAcquireActorLock_ReleaseRemovesLock(t *testing.T) {
	mr, w := newLockTestWorkflow(t)

	_, release, err := w.acquireActorLock(context.Background(), "actor-3", 200*time.Millisecond, 60*time.Millisecond)
	if err != nil {
		t.Fatalf("acquireActorLock: %v", err)
	}

	if !mr.Exists("lock:actor:actor-3") {
		t.Fatalf("lock key not in Redis after acquire")
	}
	release()
	if mr.Exists("lock:actor:actor-3") {
		t.Errorf("lock key still in Redis after release")
	}
}

func TestAcquireActorLock_ConflictReturnsAborted(t *testing.T) {
	_, w := newLockTestWorkflow(t)

	_, release, err := w.acquireActorLock(context.Background(), "actor-4", 5*time.Second, 1*time.Second)
	if err != nil {
		t.Fatalf("first acquireActorLock: %v", err)
	}
	defer release()

	_, _, err = w.acquireActorLock(context.Background(), "actor-4", 5*time.Second, 1*time.Second)
	if err == nil {
		t.Fatalf("expected second acquireActorLock to fail")
	}
}
