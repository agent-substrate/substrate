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

package snapshottracker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/protobuf/proto"
	clocktesting "k8s.io/utils/clock/testing"
)

// useActorDir roots atelet's on-node state in temp directories and creates the
// tracked actor's directory, so the tracker writes its snapshot state file.
func useActorDir(t *testing.T) {
	t.Helper()
	useTempNodeDirs(t)
	if err := os.MkdirAll(ateletpath.ActorPath(trackedActorUID), 0o700); err != nil {
		t.Fatal(err)
	}
}

// returnsPromptly fails the test unless f returns within a minute.
func returnsPromptly(t *testing.T, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
	case <-time.After(time.Minute):
		t.Fatalf("%s did not return within a minute", what)
	}
}

// beginOp begins a background operation on target for the tracked actor, as a
// worker does, and returns it with the context it runs under.
func beginOp(t *testing.T, tr *Tracker, target SnapshotRef) (context.Context, *actorOp) {
	t.Helper()
	tr.lock(trackedActorUID)
	defer tr.unlock(trackedActorUID)
	ctx, op := tr.beginOpLocked(tr.findOrAddLocked(trackedActorUID), target)
	if op == nil {
		t.Fatal("beginOpLocked: the actor already has an operation in progress")
	}
	return ctx, op
}

// endOp ends op, as a worker does, and reports whether it was still the
// tracked actor's operation in progress. It then finishes like a worker
// whose operation changed nothing, calling requeueLocked.
func endOp(tr *Tracker, op *actorOp) bool {
	tr.lock(trackedActorUID)
	defer tr.unlock(trackedActorUID)
	a := tr.endOpLocked(trackedActorUID, op)
	if a == nil {
		return false
	}
	tr.requeueLocked(a)
	return true
}

// inMemory reports whether the tracker holds the tracked actor's entry in
// memory.
func inMemory(tr *Tracker) bool {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	_, ok := tr.actors[trackedActorUID]
	return ok
}

// Start runs the workers until the tracker's context ends. Wait then returns
// once they have stopped, even when a worker has taken an actor whose lock an
// RPC still holds.
func TestWorkersStopWhenContextEnds(t *testing.T) {
	useTempNodeDirs(t)
	ctx, cancel := context.WithCancel(t.Context())
	tr := New(ctx, clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0)))
	tr.Start()
	tr.lock(trackedActorUID)
	defer tr.unlock(trackedActorUID)
	tr.queue.Add(trackedActorUID)
	cancel()
	returnsPromptly(t, "Wait", func() {
		if err := tr.Wait(t.Context()); err != nil {
			t.Errorf("Wait: %v", err)
		}
	})
}

// Wait gives up once its own context ends, while the workers still run.
func TestWaitGivesUpWhenItsContextEnds(t *testing.T) {
	useTempNodeDirs(t)
	tr := newTracker(t)
	tr.Start()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if err := tr.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Wait with the workers running = %v, want %v", err, context.DeadlineExceeded)
	}
}

// An actor's entry stays in memory while it has an operation in progress,
// even as commits save it, and requeueLocked drops it once the operation ends.
func TestEntryStaysInMemoryDuringOp(t *testing.T) {
	useActorDir(t)
	tr := newTracker(t)
	s1 := testRef("s1", 1, time.Now())
	tr.CommitLocal(trackedActorUID, s1)
	if inMemory(tr) {
		t.Error("entry in memory after a commit with no operation in progress, want it dropped")
	}

	_, op := beginOp(t, tr, s1)
	tr.CommitLocal(trackedActorUID, testRef("s2", 2, time.Now()))
	if !inMemory(tr) {
		t.Error("entry dropped from memory during an operation, want it kept")
	}

	if !endOp(tr, op) {
		t.Fatal("endOpLocked = nil, want the operation still the actor's")
	}
	if inMemory(tr) {
		t.Error("entry in memory after the operation ended, want it dropped")
	}
	if got := uuidOf(newTracker(t).State(trackedActorUID).GetLatestLocal()); got != "s2" {
		t.Errorf("latest_local from the state file = %q, want %q", got, "s2")
	}
}

// Start queues each actor whose snapshot state file survived an atelet
// restart, and skips actor directories without one. A node with no actor
// directories yet queues nothing.
func TestStartQueuesActorsWithSnapshotState(t *testing.T) {
	useTempNodeDirs(t)
	// queued starts a tracker, as a restarted atelet does, and returns the
	// actors it queued.
	queued := func() []string {
		tr := newTracker(t)
		tr.workers = 0 // Leave the queue for the test to read.
		tr.Start()
		var actorUIDs []string
		for tr.queue.Len() > 0 {
			actorUID, _ := tr.queue.Get()
			actorUIDs = append(actorUIDs, actorUID)
			tr.queue.Done(actorUID)
		}
		slices.Sort(actorUIDs)
		return actorUIDs
	}
	if got := queued(); len(got) != 0 {
		t.Errorf("queued on a node with no actor directories = %v, want none", got)
	}

	for _, actorUID := range []string{"actor-a", "actor-b", "actor-without-state"} {
		if err := os.MkdirAll(ateletpath.ActorPath(actorUID), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(nodepath.ActorsDir, "not-an-actor"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	before := newTracker(t)
	before.CommitLocal("actor-a", testRef("s1", 1, time.Now()))
	before.CommitDurable("actor-b", testRef("s2", 2, time.Now()))
	if got, want := queued(), []string{"actor-a", "actor-b"}; !slices.Equal(got, want) {
		t.Errorf("queued after a restart = %v, want %v", got, want)
	}
}

// Forget does not wait for the actor's background operation in progress. It
// cancels the operation, which finds itself abandoned when it ends, so it
// leaves alone both the forgotten actor's state and an operation that began
// for the actor after Forget.
func TestForgetAbandonsInFlightOp(t *testing.T) {
	useActorDir(t)
	tr := newTracker(t)
	s1 := testRef("s1", 1, time.Now())
	tr.CommitLocal(trackedActorUID, s1)
	opCtx, op := beginOp(t, tr, s1)

	returnsPromptly(t, "Forget during a background operation", func() {
		if err := tr.Forget(trackedActorUID); err != nil {
			t.Errorf("Forget: %v", err)
		}
	})
	if opCtx.Err() == nil {
		t.Error("the operation's context is live after Forget, want it cancelled")
	}
	_, next := beginOp(t, tr, s1)
	if endOp(tr, op) {
		t.Error("endOpLocked = true after Forget, want the operation abandoned")
	}
	if got := tr.State(trackedActorUID); !proto.Equal(got, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("state after forget = %v, want empty", got)
	}
	if !endOp(tr, next) {
		t.Error("endOpLocked = false for the operation begun after Forget, want the abandoned one's end to leave it alone")
	}
}

// A Checkpoint's commits, PromoteSnapshot's, and State neither wait for the
// actor's background operation in progress nor cancel it.
func TestCommitsDoNotWaitForInFlightOp(t *testing.T) {
	useActorDir(t)
	tr := newTracker(t)
	t0 := time.Now()
	s1 := testRef("s1", 1, t0)
	tr.CommitLocal(trackedActorUID, s1)
	opCtx, op := beginOp(t, tr, s1)

	for _, c := range []struct {
		name string
		call func()
	}{
		{"CommitLocal", func() { tr.CommitLocal(trackedActorUID, testRef("s2", 2, t0)) }},
		{"ForgetLocal", func() { tr.ForgetLocal(trackedActorUID) }},
		{"CommitDurable", func() { tr.CommitDurable(trackedActorUID, testRef("s3", 3, t0)) }},
		{"CommitPromoted", func() { tr.CommitPromoted(trackedActorUID, testRef("s4", 4, t0)) }},
		{"State", func() { tr.State(trackedActorUID) }},
	} {
		returnsPromptly(t, c.name+" during a background operation", c.call)
	}
	if err := opCtx.Err(); err != nil {
		t.Errorf("the operation's context ended with %v, want it still running", err)
	}
	if !endOp(tr, op) {
		t.Error("endOpLocked = false, want the operation still the actor's")
	}
}

// An actor has at most one background operation in progress, and another
// actor's operation does not stand in its way. The operation keeps its own
// copy of its target.
func TestBeginOpLockedAllowsOneOpPerActor(t *testing.T) {
	tr := newTracker(t)
	s1 := testRef("s1", 1, time.Now())
	_, op := beginOp(t, tr, s1)

	tr.lock(trackedActorUID)
	_, second := tr.beginOpLocked(tr.findOrAddLocked(trackedActorUID), s1)
	tr.unlock(trackedActorUID)
	if second != nil {
		t.Error("beginOpLocked began a second operation for the actor, want none")
	}

	const otherActorUID = "other-actor-uid"
	tr.lock(otherActorUID)
	_, other := tr.beginOpLocked(tr.findOrAddLocked(otherActorUID), s1)
	tr.unlock(otherActorUID)
	if other == nil {
		t.Error("beginOpLocked for another actor = nil, want an operation")
	}

	if !endOp(tr, op) {
		t.Fatal("endOpLocked = false, want the operation still the actor's")
	}
	_, op = beginOp(t, tr, s1)
	s1.Snapshot.SnapshotUuid = "changed"
	if got := op.target.Snapshot.GetSnapshotUuid(); got != "s1" {
		t.Errorf("operation target = %q after the caller changed its snapshot, want %q", got, "s1")
	}
}

// A failure holds the actor's background work back for snapshotRetryDelay,
// doubling with each consecutive failure up to maxSnapshotRetryDelay. Making
// the actor's latest snapshot durable, or forgetting the actor, starts the
// doubling over.
func TestTrackerBackoff(t *testing.T) {
	useActorDir(t)
	clk := clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0))
	tr := New(t.Context(), clk)
	fail := func() time.Duration {
		tr.lock(trackedActorUID)
		defer tr.unlock(trackedActorUID)
		return tr.backOffLocked(trackedActorUID).Sub(clk.Now())
	}
	for i, want := range []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 10 * time.Minute, 10 * time.Minute} {
		if got := fail(); got != want {
			t.Errorf("failure %d held the actor back %v, want %v", i+1, got, want)
		}
	}

	for _, lift := range []struct {
		name string
		call func()
	}{
		{"CommitDurable", func() { tr.CommitDurable(trackedActorUID, testRef("s1", 1, clk.Now())) }},
		{"CommitPromoted", func() { tr.CommitPromoted(trackedActorUID, testRef("s2", 2, clk.Now())) }},
		{"Forget", func() {
			if err := tr.Forget(trackedActorUID); err != nil {
				t.Fatalf("Forget: %v", err)
			}
		}},
	} {
		fail()
		fail()
		lift.call()
		if got := fail(); got != snapshotRetryDelay {
			t.Errorf("the first failure after %s held the actor back %v, want %v", lift.name, got, snapshotRetryDelay)
		}
	}
}
