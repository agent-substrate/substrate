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
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
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
	ctx, op := tr.beginOpLocked(tr.ctx, tr.findOrAddLocked(trackedActorUID), opUpload, target)
	if op == nil {
		t.Fatal("beginOpLocked: the actor already has an operation in progress")
	}
	return ctx, op
}

// endOp ends op, as a worker does whose operation changed nothing: it calls
// requeueLocked and closes op.done.
func endOp(tr *Tracker, op *actorOp) {
	defer close(op.done)
	tr.lock(trackedActorUID)
	defer tr.unlock(trackedActorUID)
	tr.requeueLocked(tr.endOpLocked(trackedActorUID, op))
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
	tr := New(ctx, nil, clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0)))
	tr.Start()
	tr.lock(trackedActorUID)
	defer tr.unlock(trackedActorUID)
	for _, q := range tr.queues {
		q.Add(trackedActorUID)
	}
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

	endOp(tr, op)
	if inMemory(tr) {
		t.Error("entry in memory after the operation ended, want it dropped")
	}
	if got := uuidOf(newTracker(t).State(trackedActorUID).GetLatestLocal()); got != "s2" {
		t.Errorf("latest_local from the state file = %q, want %q", got, "s2")
	}
}

// Start reads each actor's snapshot state file once, queues the actor only on
// the queues whose work is due, and drops its entry from memory. It skips actor
// directories without a state file, and queues nothing on an empty node.
func TestStartQueuesActorsWithSnapshotState(t *testing.T) {
	useTempNodeDirs(t)
	// queued starts a tracker, as a restarted atelet does, and returns the
	// actors it queued ready to run on each queue.
	queued := func() [numOpKinds][]string {
		tr := newStoreTracker(t, newFakeSnapshotStore())
		setWorkers(tr, 0, 0) // Leave the queues for the test to read.
		tr.Start()
		if len(tr.actors) != 0 {
			t.Errorf("in-memory actors after Start = %d, want 0", len(tr.actors))
		}
		var actorUIDs [numOpKinds][]string
		for k, q := range tr.queues {
			for q.Len() > 0 {
				actorUID, _ := q.Get()
				actorUIDs[k] = append(actorUIDs[k], actorUID)
				q.Done(actorUID)
			}
			slices.Sort(actorUIDs[k])
		}
		return actorUIDs
	}
	if got := queued(); len(got[opUpload]) != 0 || len(got[opDetach]) != 0 {
		t.Errorf("queued on a node with no actor directories = %v, want none", got)
	}

	for _, actorUID := range []string{"actor-both", "actor-upload", "actor-detach", "actor-durable", "actor-without-state"} {
		if err := os.MkdirAll(ateletpath.ActorPath(actorUID), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(nodepath.ActorsDir, "not-an-actor"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	// Commit a detach delay before the restarted tracker's clock, so any
	// scheduled upload or detach is due at Start.
	t0 := time.Unix(1_700_000_000, 0).Add(-testDetachDelay)
	before := New(t.Context(), newFakeSnapshotStore(), clocktesting.NewFakeClock(t0), WithUploadDelay(testUploadDelay), WithDetachDelay(testDetachDelay))
	before.CommitLocal("actor-both", testRef("s1", 1, t0))
	before.CommitLocal("actor-upload", testRef("s2", 1, t0))
	endStart, err := before.BeginStart(t.Context(), "actor-upload")
	if err != nil {
		t.Fatalf("BeginStart: %v", err)
	}
	endStart()
	s3 := testRef("s3", 1, t0)
	before.CommitDurable("actor-detach", s3)
	before.CommitLocal("actor-detach", s3)
	before.CommitDurable("actor-durable", testRef("s4", 1, t0))

	got := queued()
	if want := []string{"actor-both", "actor-upload"}; !slices.Equal(got[opUpload], want) {
		t.Errorf("upload queue after restart = %v, want %v", got[opUpload], want)
	}
	if want := []string{"actor-both", "actor-detach"}; !slices.Equal(got[opDetach], want) {
		t.Errorf("detach queue after restart = %v, want %v", got[opDetach], want)
	}
}

// Forget cancels the actor's background operation in progress and waits for it
// to end, releasing the actor's lock meanwhile, before it drops the actor's
// state in memory and on disk.
func TestForgetWaitsForInFlightOp(t *testing.T) {
	useActorDir(t)
	tr := newTracker(t)
	s1 := testRef("s1", 1, time.Now())
	tr.CommitLocal(trackedActorUID, s1)
	opCtx, op := beginOp(t, tr, s1)

	forgetDone := make(chan error, 1)
	go func() { forgetDone <- tr.Forget(trackedActorUID) }()
	select {
	case <-opCtx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Forget to cancel the operation")
	}
	returnsPromptly(t, "lock while Forget waits", func() {
		tr.lock(trackedActorUID)
		tr.unlock(trackedActorUID)
	})
	if !inMemory(tr) {
		t.Error("entry dropped from memory while Forget waits, want it kept")
	}
	if got := uuidOf(newTracker(t).State(trackedActorUID).GetLatestLocal()); got != "s1" {
		t.Errorf("latest_local a restarted atelet reads while Forget waits = %q, want %q", got, "s1")
	}
	select {
	case err := <-forgetDone:
		t.Fatalf("Forget returned (%v) before the canceled operation ended", err)
	case <-time.After(50 * time.Millisecond):
	}
	endOp(tr, op)
	select {
	case err := <-forgetDone:
		if err != nil {
			t.Errorf("Forget: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Forget to return")
	}
	if inMemory(tr) {
		t.Error("entry in memory after Forget, want it dropped")
	}
	if st := newTracker(t).State(trackedActorUID); !proto.Equal(st, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("state a restarted atelet reads after Forget = %v, want empty", st)
	}
}

// From BeginTerminate on, no background upload or detach of the actor begins,
// though both are due. If the terminate fails before Forget, its end lets them
// begin again; once Forget has dropped the actor, its end leaves the actor
// alone and queues nothing for it.
func TestTrackerTerminateHoldsOffBackgroundWork(t *testing.T) {
	for _, tc := range []struct {
		name   string
		forget bool
	}{
		{name: "terminate fails before Forget"},
		{name: "Forget drops the actor", forget: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := newIdleTestTracker(t, newFakeSnapshotStore())
			t0 := tr.clock.Now()
			tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
			stepTo(tr, t0.Add(testDetachDelay)) // An upload and a detach are both due.
			eventually(t, "upload and detach are queued", func() bool {
				return queuedNow(tr) == 2
			})
			for _, q := range tr.queues {
				uid, _ := q.Get()
				q.Done(uid)
			}

			end := tr.BeginTerminate(trackedActorUID)
			for _, kind := range []opKind{opUpload, opDetach} {
				if run := tr.beginDueWork(trackedActorUID, kind); run != nil {
					t.Errorf("beginDueWork(%v) began work during Terminate, want none", kind)
					run()
				}
			}
			if tc.forget {
				opCtx, op := beginOp(t, tr, testRef("s1", 1, t0))
				go func() {
					<-opCtx.Done()
					endOp(tr, op)
				}()
				if err := tr.Forget(trackedActorUID); err != nil {
					t.Fatalf("Forget: %v", err)
				}
			}
			end()

			if tc.forget {
				if inMemory(tr) {
					t.Error("entry in memory after Forget and the terminate's end, want it dropped")
				}
				if n := queuedNow(tr); n != 0 {
					t.Errorf("queued = %d after Forget and the terminate's end, want nothing queued for the forgotten actor", n)
				}
				return
			}
			run := tr.beginDueWork(trackedActorUID, opUpload)
			if run == nil {
				t.Fatal("beginDueWork(upload) began nothing once the terminate failed, want the due upload")
			}
			run()
		})
	}
}

// CommitLocal, LocalSnapshotsToKeep, and State neither wait for the actor's
// background upload in progress nor cancel it.
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
		{"LocalSnapshotsToKeep", func() { tr.LocalSnapshotsToKeep(trackedActorUID) }},
		{"State", func() { tr.State(trackedActorUID) }},
	} {
		returnsPromptly(t, c.name+" during a background operation", c.call)
	}
	if err := opCtx.Err(); err != nil {
		t.Errorf("the operation's context ended with %v, want it still running", err)
	}
	endOp(tr, op)
}

// An actor has at most one background operation in progress, and another
// actor's operation does not stand in its way. The operation keeps its own
// copy of its target.
func TestBeginOpLockedAllowsOneOpPerActor(t *testing.T) {
	tr := newTracker(t)
	s1 := testRef("s1", 1, time.Now())
	_, op := beginOp(t, tr, s1)

	tr.lock(trackedActorUID)
	_, second := tr.beginOpLocked(tr.ctx, tr.findOrAddLocked(trackedActorUID), opUpload, s1)
	tr.unlock(trackedActorUID)
	if second != nil {
		t.Error("beginOpLocked began a second operation for the actor, want none")
	}

	const otherActorUID = "other-actor-uid"
	tr.lock(otherActorUID)
	_, other := tr.beginOpLocked(tr.ctx, tr.findOrAddLocked(otherActorUID), opUpload, s1)
	tr.unlock(otherActorUID)
	if other == nil {
		t.Error("beginOpLocked for another actor = nil, want an operation")
	}

	endOp(tr, op)
	_, op = beginOp(t, tr, s1)
	s1.Snapshot.SnapshotUuid = "changed"
	if got := op.target.Snapshot.GetSnapshotUuid(); got != "s1" {
		t.Errorf("operation target = %q after the caller changed its snapshot, want %q", got, "s1")
	}
}

// A failure holds the actor's background work back for snapshotRetryDelay,
// doubling with each consecutive failure up to maxSnapshotRetryDelay. A
// deferred checkpoint, making the actor's latest snapshot durable, or
// forgetting the actor starts the doubling over.
func TestTrackerBackoff(t *testing.T) {
	useActorDir(t)
	clk := clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0))
	tr := New(t.Context(), nil, clk)
	fail := func() time.Duration {
		tr.lock(trackedActorUID)
		defer tr.unlock(trackedActorUID)
		return tr.backOffLocked(tr.findOrAddLocked(trackedActorUID)).Sub(clk.Now())
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
		{"CommitLocal", func() { tr.CommitLocal(trackedActorUID, testRef("s1", 1, clk.Now())) }},
		{"CommitDurable", func() { tr.CommitDurable(trackedActorUID, testRef("s2", 2, clk.Now())) }},
		{"CommitPromoted", func() { tr.CommitPromoted(trackedActorUID, testRef("s3", 3, clk.Now())) }},
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

// recordingHandler is a slog.Handler that keeps the records it handles.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *recordingHandler) WithGroup(string) slog.Handler { return h }

// logOp logs a finished operation at info level, a failed background one as a
// warning, and a background one canceled by its context at info level. A
// failed foreground operation goes back to its caller unlogged.
func TestLogOp(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name       string
		ctx        context.Context
		background bool
		err        error
		wantLevel  slog.Level
		wantMsg    string
	}{
		{name: "finished", ctx: context.Background(), background: true, wantLevel: slog.LevelInfo, wantMsg: "Snapshot operation finished"},
		{name: "background failure", ctx: context.Background(), background: true, err: errors.New("boom"), wantLevel: slog.LevelWarn, wantMsg: "Snapshot operation failed"},
		{name: "background canceled", ctx: canceled, background: true, err: context.Canceled, wantLevel: slog.LevelInfo, wantMsg: "Snapshot operation canceled"},
		{name: "foreground failure", ctx: context.Background(), err: errors.New("boom")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &recordingHandler{}
			defaultLogger := slog.Default()
			slog.SetDefault(slog.New(h))
			t.Cleanup(func() { slog.SetDefault(defaultLogger) })

			logOp(tc.ctx, trackedActorUID, opUpload, testRef("s1", 1, time.Time{}), false, tc.background, time.Second, tc.err)
			h.mu.Lock()
			defer h.mu.Unlock()
			if tc.wantMsg == "" {
				if len(h.records) != 0 {
					t.Errorf("logged %q, want nothing", h.records[0].Message)
				}
				return
			}
			if len(h.records) != 1 || h.records[0].Message != tc.wantMsg || h.records[0].Level != tc.wantLevel {
				t.Errorf("logged %v, want one %v record %q", h.records, tc.wantLevel, tc.wantMsg)
			}
		})
	}
}

// A failed background detach logs a warning rather than a cancellation:
// finishDetach logs before endOp ends the detach's context.
func TestBackgroundDetachFailureLogsWarning(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setErr(errors.New("boom"))
	tr := newIdleTestTracker(t, store)
	t0 := tr.clock.Now()
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))

	h := &recordingHandler{}
	defaultLogger := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(defaultLogger) })

	stepTo(tr, t0.Add(testDetachDelay))
	processNext(t, tr, opDetach)
	h.mu.Lock()
	defer h.mu.Unlock()
	var got []string
	for _, r := range h.records {
		if strings.HasPrefix(r.Message, "Snapshot operation") {
			got = append(got, r.Level.String()+" "+r.Message)
		}
	}
	if want := []string{"WARN Snapshot operation failed"}; !slices.Equal(got, want) {
		t.Errorf("logged %v, want %v", got, want)
	}
}

// fakeClockTimeout's contexts end with DeadlineExceeded when the fake clock
// reaches their deadline, and with Canceled when the parent or the returned
// cancel func ends first.
func TestFakeClockTimeout(t *testing.T) {
	t.Run("clock reaches the deadline", func(t *testing.T) {
		clk := clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0))
		ctx, cancel := fakeClockTimeout(clk)(context.Background(), time.Minute)
		defer cancel()
		if got, ok := ctx.Deadline(); !ok || !got.Equal(clk.Now().Add(time.Minute)) {
			t.Fatalf("Deadline() = (%v, %v), want %v", got, ok, clk.Now().Add(time.Minute))
		}
		clk.Step(time.Minute)
		waitClosed(t, ctx.Done(), "fake clock timeout")
		if got := ctx.Err(); !errors.Is(got, context.DeadlineExceeded) {
			t.Errorf("Err() = %v, want DeadlineExceeded", got)
		}
	})

	t.Run("parent canceled first", func(t *testing.T) {
		clk := clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0))
		parent, cancelParent := context.WithCancel(context.Background())
		ctx, cancel := fakeClockTimeout(clk)(parent, time.Minute)
		defer cancel()
		cancelParent()
		waitClosed(t, ctx.Done(), "parent cancel")
		if got := ctx.Err(); !errors.Is(got, context.Canceled) {
			t.Errorf("Err() = %v, want Canceled", got)
		}
	})

	t.Run("canceled first", func(t *testing.T) {
		clk := clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0))
		ctx, cancel := fakeClockTimeout(clk)(context.Background(), time.Minute)
		cancel()
		waitClosed(t, ctx.Done(), "cancel")
		if got := ctx.Err(); !errors.Is(got, context.Canceled) {
			t.Errorf("Err() = %v, want Canceled", got)
		}
	})
}

// Outside tests, a background upload attempt times out on real time, whatever
// clock the tracker measures due times on.
func TestUploadTimeoutRunsOnRealTime(t *testing.T) {
	tr := New(t.Context(), nil, clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0)))
	ctx, cancel := tr.withTimeout(t.Context(), time.Hour)
	defer cancel()
	if got, ok := ctx.Deadline(); !ok || time.Until(got) < 59*time.Minute {
		t.Errorf("Deadline() = (%v, %v), want an hour from now", got, ok)
	}
}

// A timed-out background upload logs a warning rather than an info
// cancellation, because only the upload attempt timed out while the operation
// context itself was not canceled.
func TestBackgroundUploadTimeoutLogsWarning(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	tr := newTestTracker(t, store)

	h := &recordingHandler{}
	defaultLogger := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(defaultLogger) })

	tr.CommitLocal(trackedActorUID, testRef("s1", 1, tr.clock.Now()))
	done := startUpload(t, tr, store)
	stepTo(tr, tr.clock.Now().Add(testUploadTimeout))
	waitClosed(t, done, "the timed-out upload")

	h.mu.Lock()
	defer h.mu.Unlock()
	var got []string
	for _, r := range h.records {
		if strings.HasPrefix(r.Message, "Snapshot operation") {
			got = append(got, r.Level.String()+" "+r.Message)
		}
	}
	if want := []string{"WARN Snapshot operation failed"}; !slices.Equal(got, want) {
		t.Errorf("logged %v, want %v", got, want)
	}
}
