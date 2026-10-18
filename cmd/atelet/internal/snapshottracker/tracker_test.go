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
	"errors"
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
	testUploadDelay   = time.Hour
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
	if f.uploaded[ref.uri()] {
		return false, nil
	}
	if f.uploaded == nil {
		f.uploaded = map[string]bool{}
	}
	f.uploaded[ref.uri()] = true
	return true, nil
}

// newStoreTracker returns a tracker over store on a fake clock, with the test
// delays and opts, whose workers have not started: background work starts only
// once a test advances the clock to when it is due (see stepTo). Its workers
// stop once the test ends, before the node directories are restored.
func newStoreTracker(t *testing.T, store Store, opts ...Option) *Tracker {
	t.Helper()
	opts = append([]Option{WithUploadDelay(testUploadDelay), WithDetachDelay(testDetachDelay)}, opts...)
	tr := New(t.Context(), store, clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0)), opts...)
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

// processNext takes the next actor from kind's queue of a tracker whose
// workers have not started, and runs its background work as a worker of that
// queue would.
func processNext(t *testing.T, tr *Tracker, kind opKind) {
	t.Helper()
	q := tr.queues[kind]
	next := make(chan string, 1)
	go func() {
		if actorUID, shutdown := q.Get(); !shutdown {
			next <- actorUID
		}
	}()
	select {
	case actorUID := <-next:
		tr.syncActor(actorUID, kind)
		q.Done(actorUID)
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for an actor to be queued for %s", kind)
	}
}

// queuedNow returns how many actors tr's queues hold ready for a worker,
// leaving out those queued for later.
func queuedNow(tr *Tracker) int {
	n := 0
	for _, q := range tr.queues {
		n += q.Len()
	}
	return n
}

// setWorkers gives tr uploads upload workers and detaches detach workers in
// place of the defaults, as WithUploadWorkers and WithDetachWorkers do. Call
// it before Start.
func setWorkers(tr *Tracker, uploads, detaches int) {
	WithUploadWorkers(uploads)(tr)
	WithDetachWorkers(detaches)(tr)
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
	if st.GetLatestLocal() != nil {
		t.Errorf("latest_local = %v after CommitDurable, want unset: s2 supersedes s1", st.GetLatestLocal())
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

// LocalSnapshotsToKeep names the latest local snapshot. CommitDurable drops an
// older local snapshot, which the durable one supersedes, but keeps a local
// copy of the durable snapshot itself.
func TestTrackerCommitDurableDropsOlderLocal(t *testing.T) {
	useActorDir(t)
	tr := newTracker(t)
	if keep := tr.LocalSnapshotsToKeep(trackedActorUID); keep != nil {
		t.Errorf("LocalSnapshotsToKeep for an untracked actor = %v, want nil", keep)
	}

	t0 := time.Now()
	tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	if keep := tr.LocalSnapshotsToKeep(trackedActorUID); !slices.Equal(keep, []string{"s1"}) {
		t.Errorf("LocalSnapshotsToKeep = %v, want [s1]", keep)
	}
	s2 := testRef("s2", 2, t0.Add(time.Minute))
	tr.CommitDurable(trackedActorUID, s2)
	if st := newTracker(t).State(trackedActorUID); st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "s2" {
		t.Errorf("state from the file = %v, want no local snapshot and s2 durable", st)
	}
	if keep := tr.LocalSnapshotsToKeep(trackedActorUID); len(keep) != 0 {
		t.Errorf("LocalSnapshotsToKeep after CommitDurable = %v, want none", keep)
	}

	tr.CommitLocal(trackedActorUID, s2)
	if st := tr.CommitDurable(trackedActorUID, s2); uuidOf(st.GetLatestLocal()) != "s2" {
		t.Errorf("latest_local after CommitDurable of the same snapshot = %v, want s2 kept", st.GetLatestLocal())
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

	// An older promote finishing after a newer checkpoint keeps the newer
	// durable and local snapshots.
	tr.CommitLocal(trackedActorUID, testRef("s4", 4, t0.Add(4*time.Hour)))
	older := testRef("s2", 2, time.Time{})
	older.UploadTime = t0.Add(5 * time.Hour)
	st = tr.CommitPromoted(trackedActorUID, older)
	if uuidOf(st.GetLatestDurable()) != "s3" || uuidOf(st.GetLatestLocal()) != "s4" {
		t.Errorf("state after older promote = %v, want s3 durable and s4 local", st)
	}
}

// A promote of a paused actor's local snapshot leaves its detach schedule as
// it is: the detach its checkpoint scheduled, or none for an actor whose Run
// or Restore has begun since the snapshot was taken.
func TestTrackerCommitPromotedKeepsDetachSchedule(t *testing.T) {
	for _, started := range []bool{false, true} {
		t.Run(fmt.Sprintf("started=%v", started), func(t *testing.T) {
			tr := newIdleTestTracker(t, newFakeSnapshotStore())
			paused := testRef("s1", 1, tr.clock.Now())
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

// Promoting the latest local snapshot makes it durable and clears its upload
// due time, whereas promoting an older snapshot while newer local snapshots
// still need an upload keeps the upload due when the oldest of them was,
// including any retry backoff.
func TestTrackerCommitPromotedClearsUploadDue(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newIdleTestTracker(t, store)
	t0 := tr.clock.Now()
	st := tr.CommitLocal(trackedActorUID, testRef("s1", 1, t0))
	if st.GetUploadDueTime() == nil {
		t.Fatal("upload_due_time unset before promote, want set")
	}

	promoted := testRef("s1", 1, time.Time{})
	promoted.UploadTime = t0.Add(time.Minute)
	if st = tr.CommitPromoted(trackedActorUID, promoted); st.GetUploadDueTime() != nil {
		t.Errorf("upload_due_time after promoting latest local = %v, want unset", st.GetUploadDueTime())
	}

	t2 := t0.Add(2 * time.Minute)
	tr.CommitLocal(trackedActorUID, testRef("s2", 2, t2))
	tr.CommitLocal(trackedActorUID, testRef("s3", 3, t2.Add(time.Minute)))
	tr.CommitLocal(trackedActorUID, testRef("s4", 4, t2.Add(2*time.Minute)))

	older := testRef("s0", 0, time.Time{})
	older.UploadTime = t2.Add(3 * time.Minute)
	st = tr.CommitPromoted(trackedActorUID, older)
	if got := uuidOf(st.GetLatestDurable()); got != "s1" {
		t.Errorf("latest_durable after promoting below durable = %q, want s1", got)
	}
	if got, want := timeOf(st.GetUploadDueTime()), t2.Add(testUploadDelay); !got.Equal(want) {
		t.Errorf("upload_due_time after promoting below durable = %v, want %v: an upload delay after s2", got, want)
	}

	between := testRef("s2", 2, time.Time{})
	between.UploadTime = t2.Add(3 * time.Minute)
	st = tr.CommitPromoted(trackedActorUID, between)
	if got := uuidOf(st.GetLatestDurable()); got != "s2" {
		t.Errorf("latest_durable after promoting s2 = %q, want s2", got)
	}
	if got, want := timeOf(st.GetUploadDueTime()), t2.Add(testUploadDelay); !got.Equal(want) {
		t.Errorf("upload_due_time after promoting between durable and local = %v, want %v: an upload delay after s2", got, want)
	}

	stepTo(tr, t2.Add(testUploadDelay))
	store.setErr(errors.New("boom"))
	if _, err := tr.Detach(t.Context(), trackedActorUID); err == nil {
		t.Fatal("detach succeeded, want the store's error")
	}
	wantRetry := tr.clock.Now().Add(testRetryDelay)
	st = tr.CommitPromoted(trackedActorUID, older)
	if got := timeOf(st.GetUploadDueTime()); !got.Equal(wantRetry) {
		t.Errorf("upload_due_time after promoting below durable during backoff = %v, want %v", got, wantRetry)
	}
	betweenBackoff := testRef("s3", 3, time.Time{})
	betweenBackoff.UploadTime = tr.clock.Now()
	st = tr.CommitPromoted(trackedActorUID, betweenBackoff)
	if got := uuidOf(st.GetLatestDurable()); got != "s3" {
		t.Errorf("latest_durable after promoting s3 during backoff = %q, want s3", got)
	}
	if got := timeOf(st.GetUploadDueTime()); !got.Equal(wantRetry) {
		t.Errorf("upload_due_time after promoting between durable and local during backoff = %v, want %v", got, wantRetry)
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

// Commits for one actor take turns, and an older local commit never replaces
// a newer durable snapshot.
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
		st := tr.State(trackedActorUID)
		if got, want := uuidOf(st.GetLatestDurable()), durable.Snapshot.GetSnapshotUuid(); got != want {
			t.Fatalf("round %d: latest_durable = %q, want %q", i, got, want)
		}
		if got := st.GetLatestLocal(); got != nil {
			t.Fatalf("round %d: latest_local = %v, want nil when durable has a higher generation", i, got)
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

// BeginCheckpoint and BeginPromote accept any generation when atelet holds no
// snapshot for the actor, and otherwise reject a generation below the actor's
// newest snapshot or equal to it under another snapshot_uuid with
// FailedPrecondition.
func TestTrackerCheckGeneration(t *testing.T) {
	useTempNodeDirs(t)
	if err := os.MkdirAll(ateletpath.ActorPath(trackedActorUID), 0o700); err != nil {
		t.Fatal(err)
	}
	tr := newTracker(t)
	t0 := time.Now()
	ref := func(uuid string, generation int32) SnapshotRef {
		return testRef(uuid, generation, t0)
	}
	snap := func(uuid string, generation int32) *ateletpb.Snapshot {
		return ref(uuid, generation).Snapshot
	}
	checkBegin := func(t *testing.T, tr *Tracker, r SnapshotRef, wantCode codes.Code) {
		t.Helper()
		end, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, r.Snapshot)
		if err == nil {
			end()
		}
		if got := apierror.Code(err); got != wantCode {
			t.Errorf("BeginCheckpoint(%v) code = %v, want %v", r.Snapshot, got, wantCode)
		}
		_, end, err = tr.BeginPromote(t.Context(), trackedActorUID, r)
		if err == nil {
			end()
		}
		if got := apierror.Code(err); got != wantCode {
			t.Errorf("BeginPromote(%v) code = %v, want %v", r.Snapshot, got, wantCode)
		}
	}

	checkBegin(t, tr, ref("s1", 1), codes.OK)

	// Only a local snapshot is tracked (generation 2).
	tr.CommitLocal(trackedActorUID, ref("s1", 2))
	for _, tc := range []struct {
		name     string
		ref      SnapshotRef
		wantCode codes.Code
	}{
		{name: "same snapshot at newest generation", ref: ref("s1", 2), wantCode: codes.OK},
		{name: "higher generation", ref: ref("s2", 3), wantCode: codes.OK},
		{name: "generation below newest", ref: ref("s0", 1), wantCode: codes.FailedPrecondition},
		{name: "same uuid at lower generation", ref: ref("s1", 1), wantCode: codes.FailedPrecondition},
		{name: "different uuid at newest generation", ref: ref("other", 2), wantCode: codes.FailedPrecondition},
	} {
		t.Run("localNewest/"+tc.name, func(t *testing.T) {
			checkBegin(t, tr, tc.ref, tc.wantCode)
			checkBegin(t, newTracker(t), tc.ref, tc.wantCode)
		})
	}

	// Only a durable snapshot is tracked (generation 4); CommitDurable drops
	// the older local snapshot.
	tr.CommitDurable(trackedActorUID, ref("s2", 4))
	for _, tc := range []struct {
		name     string
		ref      SnapshotRef
		wantCode codes.Code
	}{
		{name: "same snapshot at newest generation", ref: ref("s2", 4), wantCode: codes.OK},
		{name: "higher generation", ref: ref("s3", 5), wantCode: codes.OK},
		{name: "generation below newest", ref: ref("s1", 3), wantCode: codes.FailedPrecondition},
		{name: "different uuid at newest generation", ref: ref("other", 4), wantCode: codes.FailedPrecondition},
	} {
		t.Run("durableNewest/"+tc.name, func(t *testing.T) {
			checkBegin(t, tr, tc.ref, tc.wantCode)
		})
	}

	// Both local (generation 6) and durable (generation 4) are tracked: local is newest.
	tr.CommitLocal(trackedActorUID, ref("s3", 6))
	checkBegin(t, tr, ref("s2", 4), codes.FailedPrecondition)
	checkBegin(t, tr, ref("s3", 6), codes.OK)

	// While a Checkpoint or PromoteSnapshot holds an uncommitted snapshot at
	// generation 8, it is newest until end() runs.
	endCp, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, snap("s4", 8))
	if err != nil {
		t.Fatalf("BeginCheckpoint(s4@8): %v", err)
	}
	checkBegin(t, tr, ref("s3", 6), codes.FailedPrecondition)
	checkBegin(t, tr, ref("between", 7), codes.FailedPrecondition)
	checkBegin(t, tr, ref("other", 8), codes.FailedPrecondition)
	checkBegin(t, tr, ref("s4", 8), codes.OK)
	checkBegin(t, tr, ref("s5", 9), codes.OK)
	endCp()
	// Ending without committing drops the in-flight generation, so s3@6 is newest again.
	checkBegin(t, tr, ref("s3", 6), codes.OK)
	checkBegin(t, tr, ref("between", 7), codes.OK)

	_, endPromote, err := tr.BeginPromote(t.Context(), trackedActorUID, ref("s4", 8))
	if err != nil {
		t.Fatalf("BeginPromote(s4@8): %v", err)
	}
	checkBegin(t, tr, ref("between", 7), codes.FailedPrecondition)
	checkBegin(t, tr, ref("other", 8), codes.FailedPrecondition)
	checkBegin(t, tr, ref("s4", 8), codes.OK)
	endPromote()
	checkBegin(t, tr, ref("between", 7), codes.OK)

	// If durable has a higher generation than local, durable is newest.
	a := &actorSnapshots{
		actorUID: trackedActorUID,
		actorSnapshotsRecord: actorSnapshotsRecord{
			LatestLocal:   ref("local", 3).clone(),
			LatestDurable: ref("durable", 5).clone(),
		},
	}
	if got := apierror.Code(a.checkGeneration(snap("local", 3))); got != codes.FailedPrecondition {
		t.Errorf("checkGeneration(older local@3) code = %v, want FailedPrecondition", got)
	}
	if got := apierror.Code(a.checkGeneration(snap("other", 5))); got != codes.FailedPrecondition {
		t.Errorf("checkGeneration(other@5) code = %v, want FailedPrecondition", got)
	}
	if got := apierror.Code(a.checkGeneration(snap("durable", 5))); got != codes.OK {
		t.Errorf("checkGeneration(durable@5) code = %v, want OK", got)
	}

	// If Uploading holds a newer snapshot, it is newest even while no op runs.
	a.Uploading = ref("saved-uploading", 6).clone()
	if got := apierror.Code(a.checkGeneration(snap("durable", 5))); got != codes.FailedPrecondition {
		t.Errorf("checkGeneration(older durable@5) code = %v, want FailedPrecondition", got)
	}
	if got := apierror.Code(a.checkGeneration(snap("other", 6))); got != codes.FailedPrecondition {
		t.Errorf("checkGeneration(other@6) code = %v, want FailedPrecondition", got)
	}
	if got := apierror.Code(a.checkGeneration(snap("saved-uploading", 6))); got != codes.OK {
		t.Errorf("checkGeneration(saved-uploading@6) code = %v, want OK", got)
	}

	// If an operation in progress acts on a newer snapshot, its target is newest.
	a.op = &actorOp{target: ref("uploading", 7)}
	if got := apierror.Code(a.checkGeneration(snap("saved-uploading", 6))); got != codes.FailedPrecondition {
		t.Errorf("checkGeneration(older saved-uploading@6) code = %v, want FailedPrecondition", got)
	}
	if got := apierror.Code(a.checkGeneration(snap("other", 7))); got != codes.FailedPrecondition {
		t.Errorf("checkGeneration(other@7) code = %v, want FailedPrecondition", got)
	}
	if got := apierror.Code(a.checkGeneration(snap("uploading", 7))); got != codes.OK {
		t.Errorf("checkGeneration(uploading@7) code = %v, want OK", got)
	}
}

// CommitLocal and CommitDurable skip older generations, and an older
// CommitDurable leaves a newer LatestLocal's upload schedule and backoff intact.
func TestTrackerCommitsSkipOlderGenerations(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newIdleTestTracker(t, store)
	t0 := tr.clock.Now()

	st := tr.CommitLocal(trackedActorUID, testRef("s3", 3, t0))
	for _, stale := range []SnapshotRef{
		testRef("s2", 2, t0.Add(time.Minute)),
		testRef("other", 3, t0.Add(time.Minute)),
		testRef("s3", 3, t0.Add(time.Minute)),
	} {
		got := tr.CommitLocal(trackedActorUID, stale)
		if !proto.Equal(got, st) {
			t.Errorf("CommitLocal(%v) = %v, want unchanged %v", stale.Snapshot, got, st)
		}
		if keep := tr.LocalSnapshotsToKeep(trackedActorUID); !slices.Equal(keep, []string{"s3"}) {
			t.Errorf("LocalSnapshotsToKeep after CommitLocal(%v) = %v, want [s3]", stale.Snapshot, keep)
		}
	}

	s4 := testRef("s4", 4, t0.Add(2*time.Minute))
	s4.UploadTime = t0.Add(2 * time.Minute)
	st = tr.CommitDurable(trackedActorUID, s4)
	for _, stale := range []SnapshotRef{
		testRef("s2", 2, t0.Add(3*time.Minute)),
		testRef("other", 4, t0.Add(3*time.Minute)),
		testRef("s4", 4, t0.Add(3*time.Minute)),
	} {
		if got := tr.CommitDurable(trackedActorUID, stale); !proto.Equal(got, st) {
			t.Errorf("CommitDurable(%v) = %v, want unchanged %v", stale.Snapshot, got, st)
		}
	}
	for _, staleLocal := range []SnapshotRef{
		testRef("s3", 3, t0.Add(3*time.Minute)),
		testRef("other", 4, t0.Add(3*time.Minute)),
	} {
		if got := tr.CommitLocal(trackedActorUID, staleLocal); !proto.Equal(got, st) {
			t.Errorf("CommitLocal(%v) below durable = %v, want unchanged %v", staleLocal.Snapshot, got, st)
		}
	}

	// CommitLocal(s6) sets UndurableSince; CommitDurable(s5) between LatestDurable
	// (s4) and LatestLocal (s6) advances LatestDurable without clearing
	// LatestLocal, UndurableSince, or retry backoff.
	t6 := t0.Add(4 * time.Minute)
	tr.CommitLocal(trackedActorUID, testRef("s6", 6, t6))
	stepTo(tr, t6.Add(testUploadDelay))
	store.setErr(errors.New("boom"))
	if _, err := tr.Detach(t.Context(), trackedActorUID); err == nil {
		t.Fatal("detach succeeded, want the store's error")
	}
	wantRetry := tr.clock.Now().Add(testRetryDelay)

	s5 := testRef("s5", 5, t6.Add(time.Minute))
	s5.UploadTime = tr.clock.Now()
	st = tr.CommitDurable(trackedActorUID, s5)
	if got := uuidOf(st.GetLatestDurable()); got != "s5" {
		t.Errorf("latest_durable after CommitDurable(s5) = %q, want s5", got)
	}
	if got := uuidOf(st.GetLatestLocal()); got != "s6" {
		t.Errorf("latest_local after CommitDurable(s5) = %q, want s6", got)
	}
	if got := timeOf(st.GetUploadDueTime()); !got.Equal(wantRetry) {
		t.Errorf("upload_due_time after CommitDurable(s5) during backoff = %v, want %v", got, wantRetry)
	}
}
