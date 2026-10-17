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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/client-go/util/workqueue"
	clocktesting "k8s.io/utils/clock/testing"
)

const trackedActorUID = "actor-uid-1"

// useTempNodeDirs roots atelet's on-node state in temp directories so a test
// can drive the real filesystem layout. Not parallel-safe: the paths are
// process-global.
func useTempNodeDirs(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	origActors := nodepath.ActorsDir
	nodepath.ActorsDir = filepath.Join(root, "actors")
	t.Cleanup(func() {
		nodepath.ActorsDir = origActors
	})
}

// newTracker returns a tracker on a fake clock whose context ends with the
// test. Its workers are not started.
func newTracker(t *testing.T) *Tracker {
	return New(t.Context(), nil, clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0)))
}

// testRef is a MEMORY snapshot of the tracked actor.
func testRef(uuid string, generation int32, commitTime time.Time) SnapshotRef {
	return SnapshotRef{
		Snapshot: &ateletpb.Snapshot{
			SnapshotUuid: uuid,
			Generation:   generation,
			Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			Object:       &ateletpb.ObjectStorage{SnapshotUri: "gs://test-bucket/atespaces/ate-demo/actors/" + trackedActorUID + "/snapshots/" + uuid},
		},
		CommitTime: commitTime,
	}
}

// testActor is the actor testRef's snapshots belong to.
var testActor = resources.ActorRef{Atespace: "ate-demo", Name: "counter"}

func TestSnapshotRefJSONKeepsTheActor(t *testing.T) {
	ref := testRef("s1", 1, time.Unix(1_700_000_000, 0).UTC())
	ref.Actor = testActor
	data, err := json.Marshal(ref)
	if err != nil {
		t.Fatal(err)
	}
	var got SnapshotRef
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Actor != testActor || !proto.Equal(got.Snapshot, ref.Snapshot) || !got.CommitTime.Equal(ref.CommitTime) {
		t.Errorf("round trip = %+v, want %+v", got, ref)
	}
}

func timeOf(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

func uuidOf(r *ateletpb.SnapshotReport) string {
	return r.GetSnapshot().GetSnapshotUuid()
}

// The delays of the trackers newStoreTracker builds. Nothing is due until a
// test advances the tracker's clock.
const (
	testDetachDelay   = 4 * time.Hour
	testRetryDelay    = 30 * time.Minute
	testMaxRetryDelay = 2 * time.Hour
)

// fakeSnapshotStore is a Store that records its calls. A call for a URI it
// already holds uploads nothing.
type fakeSnapshotStore struct {
	// started receives each call's snapshot uuid as the call starts.
	started chan string
	// release ends a blocked call with the error it receives.
	release chan error

	mu       sync.Mutex
	calls    []string
	actors   map[string]resources.ActorRef
	uploaded map[string]bool
	blocking bool
	err      error
}

func newFakeSnapshotStore() *fakeSnapshotStore {
	return &fakeSnapshotStore{started: make(chan string, 100), release: make(chan error), actors: map[string]resources.ActorRef{}}
}

// setBlocking makes later calls wait for release, or their context.
func (f *fakeSnapshotStore) setBlocking(blocking bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blocking = blocking
}

// setErr makes later calls fail with err, or succeed when it is nil.
func (f *fakeSnapshotStore) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeSnapshotStore) uploads() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// actorOf returns the actor the upload of the snapshot acted for.
func (f *fakeSnapshotStore) actorOf(uuid string) resources.ActorRef {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.actors[uuid]
}

func (f *fakeSnapshotStore) UploadLocalSnapshot(ctx context.Context, _ string, ref SnapshotRef) (bool, error) {
	f.mu.Lock()
	f.calls = append(f.calls, ref.uuid())
	f.actors[ref.uuid()] = ref.Actor
	blocking, err := f.blocking, f.err
	f.mu.Unlock()
	f.started <- ref.uuid()
	if blocking {
		select {
		case err = <-f.release:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	if err != nil {
		return false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	uri := ref.Snapshot.GetObject().GetSnapshotUri()
	if f.uploaded[uri] {
		return false, nil
	}
	if f.uploaded == nil {
		f.uploaded = map[string]bool{}
	}
	f.uploaded[uri] = true
	return true, nil
}

// newStoreTracker returns a tracker over store on a fake clock, with the test
// delays, whose workers have not started: background work starts only once a
// test advances the clock to when it is due (see stepTo). Its workers stop
// once the test ends, before the node directories are restored.
func newStoreTracker(t *testing.T, store Store) *Tracker {
	t.Helper()
	tr := New(t.Context(), store, clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0)))
	tr.detachDelay = testDetachDelay
	tr.retries = workqueue.NewTypedItemExponentialFailureRateLimiter[string](testRetryDelay, testMaxRetryDelay)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := tr.Wait(ctx); err != nil {
			t.Errorf("workers still running a minute after the test ended: %v", err)
		}
	})
	return tr
}

// newIdleTestTracker returns a tracker like newStoreTracker's, on fresh node
// directories that hold the tracked actor's directory.
func newIdleTestTracker(t *testing.T, store Store) *Tracker {
	t.Helper()
	useActorDir(t)
	return newStoreTracker(t, store)
}

// newTestTracker returns a tracker like newIdleTestTracker's, with its workers
// running.
func newTestTracker(t *testing.T, store Store) *Tracker {
	t.Helper()
	tr := newIdleTestTracker(t, store)
	tr.Start()
	return tr
}

// stepTo advances the tracker's fake clock to at, unless it is there already,
// so the background work due by then starts.
func stepTo(tr *Tracker, at time.Time) {
	clk := tr.clock.(*clocktesting.FakeClock)
	if d := at.Sub(clk.Now()); d > 0 {
		clk.Step(d)
	}
}

// waitStarted waits for the next store call to start, and returns the uuid of
// the snapshot it uploads.
func waitStarted(t *testing.T, store *fakeSnapshotStore) string {
	t.Helper()
	select {
	case uuid := <-store.started:
		return uuid
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for an upload to start")
		return ""
	}
}

// opDone returns a channel closed once the actor's operation in progress has
// ended, deleting the local snapshots it no longer needs included.
func opDone(t *testing.T, tr *Tracker, actorUID string) <-chan struct{} {
	t.Helper()
	tr.lock(actorUID)
	defer tr.unlock(actorUID)
	a := tr.findLocked(actorUID)
	if a == nil || a.op == nil {
		t.Fatal("no operation in progress")
	}
	return a.op.done
}

func waitClosed(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// processNext takes the next actor from the queue of a tracker whose workers
// have not started, and runs its background work as a worker would.
func processNext(t *testing.T, tr *Tracker) {
	t.Helper()
	next := make(chan string, 1)
	go func() {
		if actorUID, shutdown := tr.queue.Get(); !shutdown {
			next <- actorUID
		}
	}()
	select {
	case actorUID := <-next:
		tr.syncActor(actorUID)
		tr.queue.Done(actorUID)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for an actor to be queued")
	}
}

// State reports the latest local snapshot CommitLocal records and the latest
// durable one CommitDurable records, as an immediate checkpoint records its
// upload before its local copy. Each update is written to the actor's snapshot
// state file and evicted from memory; Forget deletes the file and drops any
// in-memory state.
func TestTrackerState(t *testing.T) {
	useTempNodeDirs(t)
	if err := os.MkdirAll(ateletpath.ActorPath(trackedActorUID), 0o700); err != nil {
		t.Fatal(err)
	}
	tr := newTracker(t)
	t0 := time.Now()
	st := tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	if got := st.GetLatestLocal(); uuidOf(got) != "s1" || !timeOf(got.GetLocalCommitTime()).Equal(t0) {
		t.Errorf("latest_local = %v, want s1 committed at %v", got, t0)
	}
	if st.GetLatestDurable() != nil {
		t.Errorf("latest_durable = %v, want unset", st.GetLatestDurable())
	}
	if len(tr.actors) != 0 {
		t.Errorf("in-memory actors after CommitLocal = %d, want 0 after writing to file", len(tr.actors))
	}

	s2 := testRef("s2", 2, t0.Add(time.Minute))
	durable := s2
	durable.UploadTime = s2.CommitTime.Add(time.Second)
	st = tr.CommitDurable(trackedActorUID, durable)
	if uuidOf(st.GetLatestLocal()) != "s1" {
		t.Errorf("latest_local = %v after CommitDurable, want s1 until CommitLocal", st.GetLatestLocal())
	}
	if got := st.GetLatestDurable(); uuidOf(got) != "s2" || !timeOf(got.GetDurableCompleteTime()).Equal(durable.UploadTime) {
		t.Errorf("latest_durable = %v, want s2 uploaded at %v", got, durable.UploadTime)
	}
	if len(tr.actors) != 0 {
		t.Errorf("in-memory actors after CommitDurable = %d, want 0 after writing to file", len(tr.actors))
	}
	st = tr.CommitLocal(trackedActorUID, s2)
	if got := st.GetLatestLocal(); uuidOf(got) != "s2" || !timeOf(got.GetLocalCommitTime()).Equal(s2.CommitTime) || got.GetDurableCompleteTime() != nil {
		t.Errorf("latest_local = %v, want s2 committed at %v, with no upload time", got, s2.CommitTime)
	}

	st = tr.CommitLocal(trackedActorUID, testRef("s3", 3, t0.Add(2*time.Minute)))
	if uuidOf(st.GetLatestLocal()) != "s3" || uuidOf(st.GetLatestDurable()) != "s2" {
		t.Errorf("state = %v, want s3 local and s2 still durable", st)
	}
	if got := tr.State(trackedActorUID); !proto.Equal(got, st) {
		t.Errorf("State() = %v, want %v, as CommitLocal returned", got, st)
	}
	if got := newTracker(t).State(trackedActorUID); !proto.Equal(got, st) {
		t.Errorf("State() on a fresh tracker = %v, want %v from the state file", got, st)
	}

	if err := tr.Forget(trackedActorUID); err != nil {
		t.Fatalf("forget: %v", err)
	}
	if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(trackedActorUID)); !os.IsNotExist(err) {
		t.Errorf("state file after forget: stat = %v, want it deleted", err)
	}
	if got := tr.State(trackedActorUID); !proto.Equal(got, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("state after forget = %v, want empty", got)
	}
}

// ForgetLocal drops the local snapshot and keeps the durable one. With no
// local snapshot to drop, it writes nothing.
func TestTrackerForgetLocal(t *testing.T) {
	useTempNodeDirs(t)
	if err := os.MkdirAll(ateletpath.ActorPath(trackedActorUID), 0o700); err != nil {
		t.Fatal(err)
	}
	tr := newTracker(t)
	tr.ForgetLocal(trackedActorUID)
	if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(trackedActorUID)); !os.IsNotExist(err) {
		t.Errorf("state file after forgetting nothing: stat = %v, want none written", err)
	}

	t0 := time.Now()
	tr.CommitDurable(trackedActorUID, testRef("s1", 1, t0))
	tr.CommitLocal(trackedActorUID, testRef("s2", 2, t0.Add(time.Minute)))
	tr.ForgetLocal(trackedActorUID)
	if st := newTracker(t).State(trackedActorUID); st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "s1" {
		t.Errorf("state from the file = %v, want no local snapshot and s1 still durable", st)
	}
	if len(tr.actors) != 0 {
		t.Errorf("in-memory actors = %d, want 0 after writing to file", len(tr.actors))
	}
}

// CommitPromoted makes the promoted local snapshot the latest durable one,
// keeping the local copy and the commit time recorded for it. A retried
// promote of the same snapshot keeps what was recorded.
func TestTrackerCommitPromoted(t *testing.T) {
	useTempNodeDirs(t)
	if err := os.MkdirAll(ateletpath.ActorPath(trackedActorUID), 0o700); err != nil {
		t.Fatal(err)
	}
	tr := newTracker(t)
	t0 := time.Now()
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))

	promoted := testRef("s1", 1, time.Time{})
	promoted.UploadTime = t0.Add(time.Minute)
	st := tr.CommitPromoted(trackedActorUID, promoted)
	if got := st.GetLatestLocal(); uuidOf(got) != "s1" || !timeOf(got.GetLocalCommitTime()).Equal(t0) || got.GetDurableCompleteTime() != nil {
		t.Errorf("latest_local = %v, want s1 committed at %v, with no upload time", got, t0)
	}
	if got := st.GetLatestDurable(); uuidOf(got) != "s1" || !timeOf(got.GetLocalCommitTime()).Equal(t0) || !timeOf(got.GetDurableCompleteTime()).Equal(promoted.UploadTime) {
		t.Errorf("latest_durable = %v, want s1 committed at %v and uploaded at %v", got, t0, promoted.UploadTime)
	}
	if got := newTracker(t).State(trackedActorUID); !proto.Equal(got, st) {
		t.Errorf("State() on a fresh tracker = %v, want %v from the state file", got, st)
	}

	retry := testRef("s1", 1, time.Time{})
	retry.UploadTime = t0.Add(time.Hour)
	if got := tr.CommitPromoted(trackedActorUID, retry); !proto.Equal(got, st) {
		t.Errorf("state after a retried promote = %v, want %v", got, st)
	}

	// Promoting a snapshot other than the local one carries no commit time and
	// leaves the local snapshot untouched.
	tr.CommitLocal(trackedActorUID, testRef("s2", 2, t0.Add(2*time.Hour)))
	other := testRef("s3", 3, time.Time{})
	other.UploadTime = t0.Add(3 * time.Hour)
	st = tr.CommitPromoted(trackedActorUID, other)
	if got := st.GetLatestDurable(); uuidOf(got) != "s3" || got.GetLocalCommitTime() != nil || uuidOf(st.GetLatestLocal()) != "s2" {
		t.Errorf("state = %v, want s3 durable without a commit time, and s2 still local", st)
	}
}

// IsDurable reports whether a snapshot is recorded as LatestDurable, and fails
// with FailedPrecondition when LatestDurable has the same UUID with a
// different generation or URI.
func TestTrackerIsDurable(t *testing.T) {
	useTempNodeDirs(t)
	if err := os.MkdirAll(ateletpath.ActorPath(trackedActorUID), 0o700); err != nil {
		t.Fatal(err)
	}
	tr := newTracker(t)
	s1 := testRef("s1", 1, time.Now())

	if ok, err := tr.IsDurable(trackedActorUID, s1); err != nil || ok {
		t.Errorf("untracked: IsDurable = (%v, %v), want (false, nil)", ok, err)
	}

	tr.CommitLocal(trackedActorUID, s1)
	if ok, err := tr.IsDurable(trackedActorUID, s1); err != nil || ok {
		t.Errorf("local only: IsDurable = (%v, %v), want (false, nil)", ok, err)
	}

	tr.CommitPromoted(trackedActorUID, s1)
	if ok, err := tr.IsDurable(trackedActorUID, s1); err != nil || !ok {
		t.Errorf("matching durable: IsDurable = (%v, %v), want (true, nil)", ok, err)
	}
	if ok, err := tr.IsDurable(trackedActorUID, testRef("s2", 2, time.Now())); err != nil || ok {
		t.Errorf("other UUID: IsDurable = (%v, %v), want (false, nil)", ok, err)
	}

	wrongGen := testRef("s1", 2, time.Now())
	if _, err := tr.IsDurable(trackedActorUID, wrongGen); apierror.Code(err) != codes.FailedPrecondition {
		t.Errorf("mismatched generation: IsDurable = %v, want FailedPrecondition", err)
	}

	wrongURI := testRef("s1", 1, time.Now())
	wrongURI.Snapshot.Object.SnapshotUri += "-other"
	if _, err := tr.IsDurable(trackedActorUID, wrongURI); apierror.Code(err) != codes.FailedPrecondition {
		t.Errorf("mismatched URI: IsDurable = %v, want FailedPrecondition", err)
	}
}

// A promote of a paused actor's local snapshot leaves its detach schedule as
// it is: the detach its checkpoint scheduled, or none for an actor whose Run
// or Restore has begun since the snapshot was taken.
func TestTrackerCommitPromotedKeepsDetachSchedule(t *testing.T) {
	for _, started := range []bool{false, true} {
		t.Run(fmt.Sprintf("started=%v", started), func(t *testing.T) {
			tr := newIdleTestTracker(t, newFakeSnapshotStore())
			// A pause snapshot names no object storage location.
			paused := testRef("s1", 1, tr.clock.Now())
			paused.Snapshot.Object = nil
			tr.CommitLocal(trackedActorUID, paused)
			want := tr.clock.Now().Add(testDetachDelay)
			if started {
				end, err := tr.BeginStart(t.Context(), trackedActorUID)
				if err != nil {
					t.Fatalf("beginStart: %v", err)
				}
				end()
				want = time.Time{}
			}
			stepTo(tr, tr.clock.Now().Add(time.Hour))
			tr.CommitPromoted(trackedActorUID, testRef("s1", 1, time.Time{}))
			if got := detachDueOf(tr, trackedActorUID); !got.Equal(want) {
				t.Errorf("detachDue after the promote = %v, want %v", got, want)
			}
		})
	}
}

// When a save fails, the entry stays in memory and is reported instead of the
// state file, until a later save succeeds.
func TestTrackerKeepsUnsavedStateInMemory(t *testing.T) {
	useTempNodeDirs(t)
	stateFile := ateletpath.ActorSnapshotStateFile(trackedActorUID)
	// A directory where the state file goes makes the write's rename fail.
	if err := os.MkdirAll(stateFile, 0o700); err != nil {
		t.Fatal(err)
	}
	tr := newTracker(t)
	t0 := time.Now()
	st := tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	if len(tr.actors) != 1 {
		t.Fatalf("in-memory actors = %d after a failed save, want 1", len(tr.actors))
	}
	if got := tr.State(trackedActorUID); !proto.Equal(got, st) {
		t.Errorf("State() = %v, want %v from memory", got, st)
	}

	if err := os.Remove(stateFile); err != nil {
		t.Fatal(err)
	}
	st = tr.CommitLocal(trackedActorUID, testRef("s2", 2, t0.Add(time.Minute)))
	if len(tr.actors) != 0 {
		t.Errorf("in-memory actors = %d after a successful save, want 0", len(tr.actors))
	}
	if got := newTracker(t).State(trackedActorUID); !proto.Equal(got, st) {
		t.Errorf("State() on a fresh tracker = %v, want %v from the state file", got, st)
	}
}

// A save that finds the actor's directory gone, as after the actor was
// terminated on this node, writes nothing and drops the entry.
func TestTrackerDropsActorWithoutDirectory(t *testing.T) {
	useTempNodeDirs(t)
	tr := newTracker(t)
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, time.Now()))
	if len(tr.actors) != 0 {
		t.Errorf("in-memory actors = %d, want 0 for an actor whose directory is gone", len(tr.actors))
	}
	if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(trackedActorUID)); !os.IsNotExist(err) {
		t.Errorf("state file: stat = %v, want none written", err)
	}
	if got := tr.State(trackedActorUID); !proto.Equal(got, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("State() = %v, want empty", got)
	}
}

// Commits for one actor take turns, so a racing deferred checkpoint never
// loses an immediate checkpoint's durable snapshot. The control plane never
// sends one actor's checkpoints concurrently: the race only exercises the
// lock, so either commit may land last, even leaving a local snapshot older
// than the durable one.
func TestTrackerSerializesEachActor(t *testing.T) {
	useTempNodeDirs(t)
	if err := os.MkdirAll(ateletpath.ActorPath(trackedActorUID), 0o700); err != nil {
		t.Fatal(err)
	}
	tr := newTracker(t)
	t0 := time.Now()
	for i := range 20 {
		local := testRef(fmt.Sprintf("local-%d", i), int32(2*i+1), t0)
		durable := testRef(fmt.Sprintf("durable-%d", i), int32(2*i+2), t0)
		var wg sync.WaitGroup
		wg.Go(func() { tr.CommitLocal(trackedActorUID, local) })
		wg.Go(func() { tr.CommitDurable(trackedActorUID, durable) })
		wg.Wait()
		if got, want := uuidOf(tr.State(trackedActorUID).GetLatestDurable()), durable.Snapshot.GetSnapshotUuid(); got != want {
			t.Fatalf("round %d: latest_durable = %q, want %q", i, got, want)
		}
	}
	if len(tr.actors) != 0 {
		t.Errorf("in-memory actors = %d, want 0 once every commit is written", len(tr.actors))
	}
}

// A caller holding one actor's lock does not hold up another actor's commit.
func TestTrackerLocksEachActorSeparately(t *testing.T) {
	useTempNodeDirs(t)
	tr := newTracker(t)
	tr.lock("other-actor-uid")
	defer tr.unlock("other-actor-uid")

	done := make(chan struct{})
	go func() {
		defer close(done)
		tr.CommitLocal(trackedActorUID, testRef("s1", 1, time.Now()))
	}()
	select {
	case <-done:
	case <-time.After(time.Minute):
		t.Fatal("CommitLocal waited for another actor's lock")
	}
}

// An unreadable state file on disk is skipped and treated as empty state.
func TestTrackerSkipsUnreadableSnapshotState(t *testing.T) {
	useTempNodeDirs(t)
	if err := os.MkdirAll(ateletpath.ActorPath(trackedActorUID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ateletpath.ActorSnapshotStateFile(trackedActorUID), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	tr := newTracker(t)
	if got := tr.State(trackedActorUID); !proto.Equal(got, &ateletpb.ActorSnapshotState{}) {
		t.Errorf("state with a broken file = %v, want empty", got)
	}
}
