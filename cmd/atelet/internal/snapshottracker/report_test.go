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
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/client-go/util/workqueue"
	clocktesting "k8s.io/utils/clock/testing"
)

// The report retry delays of the trackers startReporting sets up. A minute is
// well short of the test upload and detach delays, so stepping the clock past
// a retry starts no other background work.
const (
	testReportRetryDelay    = time.Minute
	testMaxReportRetryDelay = 10 * time.Minute
)

// sentReport is one report a fakeReporter received.
type sentReport struct {
	actorUID string
	actor    resources.ActorRef
	state    *ateletpb.ActorSnapshotState
}

// fakeReporter is a Reporter that records its reports.
type fakeReporter struct {
	// reported receives each report as it starts.
	reported chan sentReport
	// release ends a blocked report with the error it receives.
	release chan error

	mu       sync.Mutex
	sent     []sentReport
	errs     []error
	blocking bool
}

func newFakeReporter() *fakeReporter {
	return &fakeReporter{reported: make(chan sentReport, 100), release: make(chan error)}
}

// failNext makes the next reports fail with errs, in order.
func (f *fakeReporter) failNext(errs ...error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs = append(f.errs, errs...)
}

// setBlocking makes later reports wait for release, or their context.
func (f *fakeReporter) setBlocking(blocking bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blocking = blocking
}

// reports returns the reports received so far.
func (f *fakeReporter) reports() []sentReport {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.sent)
}

func (f *fakeReporter) ReportActorSnapshotState(ctx context.Context, actorUID string, actor resources.ActorRef, state *ateletpb.ActorSnapshotState) error {
	r := sentReport{actorUID: actorUID, actor: actor, state: state}
	f.mu.Lock()
	f.sent = append(f.sent, r)
	var err error
	if len(f.errs) > 0 {
		err, f.errs = f.errs[0], f.errs[1:]
	}
	blocking := f.blocking
	f.mu.Unlock()
	f.reported <- r
	if blocking {
		select {
		case err = <-f.release:
		case <-ctx.Done():
			if err == nil {
				// As a gRPC client fails a call whose context ends.
				err = status.FromContextError(ctx.Err()).Err()
			}
		}
	}
	return err
}

// committedRef is testRef's snapshot as a commit records it, naming testActor.
func committedRef(uuid string, generation int32, commitTime time.Time) SnapshotRef {
	ref := testRef(uuid, generation, commitTime)
	ref.Actor = testActor
	return ref
}

// startReporting makes tr report through reporter, retrying a failed report
// testReportRetryDelay later, and starts it without report workers: a test
// sends each report with processNextReport.
func startReporting(tr *Tracker, reporter Reporter) {
	tr.reporting.reporter = reporter
	tr.reporting.workers = 0
	tr.reporting.retries = workqueue.NewTypedItemExponentialFailureRateLimiter[string](testReportRetryDelay, testMaxReportRetryDelay)
	tr.reporting.queueRetries = workqueue.NewTypedItemExponentialFailureRateLimiter[string](testReportRetryDelay, testMaxReportRetryDelay)
	tr.reporting.startupJitter = 0
	tr.Start()
}

// newReportingTracker returns a tracker like newTestTracker's that reports
// through reporter, as startReporting sets up.
func newReportingTracker(t *testing.T, store Store, reporter Reporter) *Tracker {
	t.Helper()
	tr := newIdleTestTracker(t, store)
	startReporting(tr, reporter)
	return tr
}

// processNextReport takes the next actor from the reports queue of a tracker
// without report workers, and reports it as a report worker would.
func processNextReport(t *testing.T, tr *Tracker) {
	t.Helper()
	next := make(chan string, 1)
	go func() {
		if actorUID, shutdown := tr.reporting.queue.Get(); !shutdown {
			next <- actorUID
		}
	}()
	select {
	case actorUID := <-next:
		tr.report(actorUID)
		tr.reporting.queue.Done(actorUID)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for a report to be queued")
	}
}

// waitReported waits for reporter's next report to start, and returns it.
func waitReported(t *testing.T, reporter *fakeReporter) sentReport {
	t.Helper()
	select {
	case r := <-reporter.reported:
		return r
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for a report")
		return sentReport{}
	}
}

// reportDue reports whether a report of the tracked actor is due.
func reportDue(tr *Tracker) bool {
	tr.lock(trackedActorUID)
	defer tr.unlock(trackedActorUID)
	a := tr.findLocked(trackedActorUID)
	return a != nil && a.reportDue()
}

// reportDueOnDisk reports whether the tracked actor's snapshot state file
// records a report due.
func reportDueOnDisk() bool {
	a := readSnapshotState(trackedActorUID)
	return a != nil && a.reportDue()
}

// A background upload that makes a snapshot durable reports the actor's whole
// state, for the actor the snapshot's commit named. Once the control plane
// acknowledges the report, it is no longer due, in memory or on disk.
func TestTrackerReportsBackgroundUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	reporter := newFakeReporter()
	tr := newReportingTracker(t, store, reporter)
	tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
	uploadNow(t, tr, store, nil)
	if !reportDueOnDisk() {
		t.Fatal("state file after the background upload records no report due")
	}

	processNextReport(t, tr)
	sent := reporter.reports()
	if len(sent) != 1 {
		t.Fatalf("reports = %d, want 1", len(sent))
	}
	if got := sent[0]; got.actorUID != trackedActorUID || got.actor != testActor {
		t.Errorf("report for actor %q %+v, want %q %+v", got.actorUID, got.actor, trackedActorUID, testActor)
	}
	st := sent[0].state
	if uuidOf(st.GetLatestDurable()) != "s1" || uuidOf(st.GetLatestLocal()) != "s1" {
		t.Errorf("reported state = %v, want s1 durable and still local", st)
	}
	if want := tr.State(trackedActorUID); !proto.Equal(st, want) {
		t.Errorf("reported state = %v, want the actor's state %v", st, want)
	}
	if reportDue(tr) || reportDueOnDisk() {
		t.Error("report still due once the control plane acknowledged it")
	}
	if n := tr.reporting.queue.Len(); n != 0 {
		t.Errorf("reports queued = %d once acknowledged, want none", n)
	}
}

// A background detach reports the actor's state once its local snapshots are
// gone: nothing local or uploading, which the control plane reads as a
// detach, and the detached snapshot durable.
func TestTrackerReportsBackgroundDetach(t *testing.T) {
	wantDetached := func(t *testing.T, got sentReport) {
		t.Helper()
		st := got.state
		if st.GetLatestLocal() != nil || st.GetUploading() != nil || uuidOf(st.GetLatestDurable()) != "s1" {
			t.Errorf("reported state = %v, want nothing local or uploading, and s1 durable", st)
		}
		if got.actor != testActor {
			t.Errorf("report for actor %+v, want %+v", got.actor, testActor)
		}
	}

	t.Run("that uploads the snapshot", func(t *testing.T) {
		store := newFakeSnapshotStore()
		reporter := newFakeReporter()
		tr := newReportingTracker(t, store, reporter)
		tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
		// With no upload workers, the detach runs and makes s1 durable itself,
		// and reports once, at its end.
		detachNow(t, tr)
		processNextReport(t, tr)
		sent := reporter.reports()
		if len(sent) != 1 {
			t.Fatalf("reports = %d, want 1", len(sent))
		}
		wantDetached(t, sent[0])
		if reportDue(tr) || tr.reporting.queue.Len() != 0 {
			t.Errorf("report due = %v, queued = %d once acknowledged, want neither", reportDue(tr), tr.reporting.queue.Len())
		}
	})

	t.Run("after a background upload", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		reporter := newFakeReporter()
		tr := newReportingTracker(t, store, reporter)
		tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
		uploadNow(t, tr, store, nil)
		processNextReport(t, tr)

		detachNow(t, tr)
		processNextReport(t, tr)
		sent := reporter.reports()
		if len(sent) != 2 {
			t.Fatalf("reports = %d, want one for the upload and one for the detach", len(sent))
		}
		wantDetached(t, sent[1])
	})
}

// A Detach call marks a detach report due and keeps snapshot-state.json on
// disk until the report is acknowledged or refused with NotFound. On give-up
// or any other refusal, snapshot-state.json stays on disk so a later Detach
// call still returns LatestDurable.
func TestTrackerReportsDetachCall(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		giveUp     bool
		wantForget bool
	}{
		{name: "acknowledged", wantForget: true},
		{name: "refused with NotFound", err: status.Error(codes.NotFound, "actor gone"), wantForget: true},
		{name: "refused with Unimplemented", err: status.Error(codes.Unimplemented, "older ateapi"), wantForget: false},
		{name: "refused with FailedPrecondition", err: status.Error(codes.FailedPrecondition, "conflict"), wantForget: false},
		{name: "gives up after timeout", err: status.Error(codes.Unavailable, "ateapi down"), giveUp: true, wantForget: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeSnapshotStore()
			reporter := newFakeReporter()
			if tc.err != nil {
				if tc.giveUp {
					reporter.failNext(tc.err, tc.err)
				} else {
					reporter.failNext(tc.err)
				}
			}
			tr := newReportingTracker(t, store, reporter)
			tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
			dueAt := tr.clock.Now()
			if _, err := tr.Detach(t.Context(), trackedActorUID); err != nil {
				t.Fatalf("detach: %v", err)
			}
			if !reportDue(tr) || !reportDueOnDisk() || tr.reporting.queue.Len() != 1 {
				t.Fatalf("report due = %v, on disk = %v, queued = %d after Detach, want true, true, 1", reportDue(tr), reportDueOnDisk(), tr.reporting.queue.Len())
			}

			// A repeated Detach while the report is still pending returns
			// LatestDurable and leaves the report due.
			st, err := tr.Detach(t.Context(), trackedActorUID)
			if err != nil {
				t.Fatalf("second detach: %v", err)
			}
			if st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "s1" {
				t.Errorf("second detach state = %v, want s1 durable and nothing local", st)
			}
			if !reportDue(tr) || !reportDueOnDisk() {
				t.Fatal("second detach cleared the pending detach report")
			}

			processNextReport(t, tr)
			if tc.giveUp {
				stepTo(tr, dueAt.Add(DefaultReportGiveUpAfter))
				processNextReport(t, tr)
			}
			if reportDue(tr) || reportDueOnDisk() {
				t.Error("report still due once report ended")
			}
			if !tc.wantForget {
				if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(trackedActorUID)); err != nil {
					t.Errorf("snapshot-state.json missing after %s: %v", tc.name, err)
				}
			}
			st, err = tr.Detach(t.Context(), trackedActorUID)
			if err != nil {
				t.Fatalf("third detach: %v", err)
			}
			if tc.wantForget {
				if _, err := os.Stat(ateletpath.ActorPath(trackedActorUID)); !os.IsNotExist(err) {
					t.Errorf("actor dir %s still exists: %v", ateletpath.ActorPath(trackedActorUID), err)
				}
				if !proto.Equal(st, &ateletpb.ActorSnapshotState{}) {
					t.Errorf("third detach state = %v, want empty", st)
				}
			} else {
				if st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "s1" {
					t.Errorf("third detach state = %v, want s1 durable kept", st)
				}
				if !reportDue(tr) || !reportDueOnDisk() {
					t.Fatal("third detach after report failure did not re-queue a detach report")
				}
				processNextReport(t, tr)
				if _, err := os.Stat(ateletpath.ActorPath(trackedActorUID)); !os.IsNotExist(err) {
					t.Errorf("actor dir %s still exists after re-queued detach report acknowledged: %v", ateletpath.ActorPath(trackedActorUID), err)
				}
			}
		})
	}

	t.Run("started while detach report is due", func(t *testing.T) {
		store := newFakeSnapshotStore()
		reporter := newFakeReporter()
		tr := newReportingTracker(t, store, reporter)
		s1 := committedRef("s1", 1, tr.clock.Now())
		tr.CommitLocal(trackedActorUID, s1)
		if _, err := tr.Detach(t.Context(), trackedActorUID); err != nil {
			t.Fatalf("detach: %v", err)
		}

		// BeginStart recreates the actor's state file on disk (which still
		// exists while the detach report is due) with Started = true.
		endStart, err := tr.BeginStart(t.Context(), trackedActorUID)
		if err != nil {
			t.Fatalf("BeginStart: %v", err)
		}
		endStart()

		processNextReport(t, tr)
		if reportDue(tr) || reportDueOnDisk() {
			t.Error("detach report still due once acknowledged")
		}
		if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(trackedActorUID)); err != nil {
			t.Errorf("snapshot-state.json removed for a started actor: %v", err)
		}
		if uuidOf(tr.State(trackedActorUID).GetLatestDurable()) != "s1" {
			t.Errorf("state = %v, want s1 durable kept", tr.State(trackedActorUID))
		}
		if newTracker(t).NotStartedSince(trackedActorUID, s1.Snapshot) {
			t.Error("NotStartedSince on a fresh tracker = true, want false kept in snapshot-state.json")
		}
	})
}

// A Detach call whose upload succeeds and cleanup fails still reports the upload.
func TestTrackerReportsDetachCallUploadWhenPruneFails(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newReportingTracker(t, store, newFakeReporter())
	tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
	// A populated volume mount directory fails RemoveActorFilesExceptState.
	stillMounted := filepath.Join(ateletpath.ExternalVolumeHostPath(trackedActorUID, "data"), "payload")
	if err := os.MkdirAll(filepath.Dir(stillMounted), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stillMounted, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Detach(t.Context(), trackedActorUID); err == nil {
		t.Fatal("detach succeeded, want the cleanup's error")
	}
	if st := tr.State(trackedActorUID); uuidOf(st.GetLatestDurable()) != "s1" {
		t.Fatalf("state = %v, want s1 durable", st)
	}
	if !reportDue(tr) || !reportDueOnDisk() {
		t.Error("no report due for the upload after the cleanup failed")
	}
}

// A Detach call with nothing left to detach keeps a report already due.
func TestTrackerDetachCallKeepsReportDue(t *testing.T) {
	store := newFakeSnapshotStore()
	tr := newReportingTracker(t, store, newFakeReporter())
	tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
	detachNow(t, tr)
	if !reportDue(tr) {
		t.Fatal("report not due after the background detach")
	}
	st, err := tr.Detach(t.Context(), trackedActorUID)
	if err != nil {
		t.Fatalf("detach: %v", err)
	}
	if st.GetLatestLocal() != nil || uuidOf(st.GetLatestDurable()) != "s1" {
		t.Errorf("state = %v, want s1 durable and nothing local", st)
	}
	if !reportDue(tr) || !reportDueOnDisk() {
		t.Error("report no longer due after the Detach call")
	}
}

// A background upload or detach makes no report due when it changes neither
// LatestDurable nor LatestLocal.
func TestTrackerDoesNotReportWhenNothingChanged(t *testing.T) {
	for _, tc := range []struct {
		name string
		snap SnapshotRef
		// cancels is set when snap is newer than the upload's snapshot, so
		// CommitDurable cancels the upload and waits for it to stop.
		cancels bool
	}{
		{"newer durable snapshot", committedRef("s2", 2, time.Time{}), true},
		{"same generation already durable", committedRef("s1", 1, time.Time{}), false},
	} {
		t.Run("upload overtaken by "+tc.name, func(t *testing.T) {
			store := newFakeSnapshotStore()
			store.setBlocking(true)
			tr := newReportingTracker(t, store, newFakeReporter())
			t0 := tr.clock.Now()
			tr.CommitLocal(trackedActorUID, committedRef("s1", 1, t0))
			done := startUpload(t, tr, store)
			durable := tc.snap
			durable.CommitTime = t0.Add(time.Minute)
			durable.UploadTime = durable.CommitTime.Add(time.Second)
			tr.CommitDurable(trackedActorUID, durable)

			if !tc.cancels {
				store.release <- nil
			}
			waitClosed(t, done, "the upload of s1")
			if reportDue(tr) || tr.reporting.queue.Len() != 0 {
				t.Errorf("report due = %v, queued = %d after an overtaken upload, want neither", reportDue(tr), tr.reporting.queue.Len())
			}
			if got := timeOf(tr.State(trackedActorUID).GetLatestDurable().GetDurableCompleteTime()); !got.Equal(durable.UploadTime) {
				t.Errorf("durable_complete_time = %v, want %v from CommitDurable", got, durable.UploadTime)
			}
		})
	}

	t.Run("failed detach of already durable snapshot", func(t *testing.T) {
		store := newFakeSnapshotStore()
		tr := newIdleTestTracker(t, store)
		tr.reporting.reporter = newFakeReporter()
		t0 := tr.clock.Now()
		local := committedRef("s1", 1, t0)
		durable := local
		durable.UploadTime = t0.Add(time.Second)
		tr.CommitDurable(trackedActorUID, durable)
		tr.CommitLocal(trackedActorUID, local)
		// A populated volume mount directory fails RemoveActorFilesExceptState.
		stillMounted := filepath.Join(ateletpath.ExternalVolumeHostPath(trackedActorUID, "data"), "payload")
		if err := os.MkdirAll(filepath.Dir(stillMounted), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(stillMounted, nil, 0o600); err != nil {
			t.Fatal(err)
		}

		stepTo(tr, t0.Add(testDetachDelay))
		processNext(t, tr, opDetach)
		if reportDue(tr) || tr.reporting.queue.Len() != 0 {
			t.Errorf("report due = %v, queued = %d after a detach that changed nothing, want neither", reportDue(tr), tr.reporting.queue.Len())
		}
	})
}

// A report that fails for a reason a later attempt can pass is retried once
// the retry delay has passed, not before, and stays due until the control
// plane acknowledges it. Aborted is expected while another operation changes
// the actor, so it is logged at Info; other retryable errors warn.
func TestTrackerRetriesFailedReport(t *testing.T) {
	for _, tc := range []struct {
		err   error
		level slog.Level
	}{
		{status.Error(codes.Aborted, "another operation is changing the actor"), slog.LevelInfo},
		{status.Error(codes.Unavailable, "ateapi is restarting"), slog.LevelWarn},
		{status.Error(codes.DeadlineExceeded, "ateapi is slow"), slog.LevelWarn},
		{status.Error(codes.Canceled, "call canceled while atelet is still running"), slog.LevelWarn},
		{errors.New("not a status"), slog.LevelWarn},
	} {
		t.Run(status.Code(tc.err).String(), func(t *testing.T) {
			levels := recordLogLevels(t, "Failed to report the actor's snapshot state")
			store := newFakeSnapshotStore()
			store.setBlocking(true)
			reporter := newFakeReporter()
			reporter.failNext(tc.err)
			tr := newReportingTracker(t, store, reporter)
			tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
			uploadNow(t, tr, store, nil)

			failedAt := tr.clock.Now()
			processNextReport(t, tr)
			if !reportDue(tr) || !reportDueOnDisk() {
				t.Fatal("report no longer due after a failed attempt")
			}
			if got := levels(); !slices.Equal(got, []slog.Level{tc.level}) {
				t.Errorf("failed report logged at %v, want once at %v", got, tc.level)
			}
			stepTo(tr, failedAt.Add(testReportRetryDelay-time.Second))
			time.Sleep(50 * time.Millisecond)
			if n := tr.reporting.queue.Len(); n != 0 {
				t.Fatalf("reports queued = %d before the retry delay passed, want none", n)
			}

			stepTo(tr, failedAt.Add(testReportRetryDelay))
			processNextReport(t, tr)
			if got := len(reporter.reports()); got != 2 {
				t.Errorf("reports = %d, want the failed one and its retry", got)
			}
			if reportDue(tr) || reportDueOnDisk() {
				t.Error("report still due once the retry was acknowledged")
			}
		})
	}
}

// A report that is still failing once giveUpAfter has passed since it
// became due is dropped, so atelet stops retrying it. A later background
// change starts a fresh window.
func TestTrackerGivesUpOnReportAfterTimeout(t *testing.T) {
	t.Run("drops after 10 minutes", func(t *testing.T) {
		gaveUp := recordLogLevels(t, "Giving up on reporting the actor's snapshot state")
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		reporter := newFakeReporter()
		unavailable := status.Error(codes.Unavailable, "ateapi is down")
		reporter.failNext(unavailable, unavailable, unavailable)
		tr := newReportingTracker(t, store, reporter)
		tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
		uploadNow(t, tr, store, nil)
		dueAt := tr.clock.Now()

		// Attempt 1 fails at dueAt; retry is queued for +1m.
		processNextReport(t, tr)
		// Step just short of the 10-minute window (+9m59s): attempt 2 fails
		// and queues another retry (+2m, which lands past +10m).
		stepTo(tr, dueAt.Add(DefaultReportGiveUpAfter-time.Second))
		processNextReport(t, tr)
		if !reportDue(tr) || !reportDueOnDisk() {
			t.Fatal("report dropped before DefaultReportGiveUpAfter elapsed")
		}
		if got := gaveUp(); len(got) != 0 {
			t.Fatalf("gave up logged early at %v, want none yet", got)
		}

		// Step past the retry delay, now beyond the 10-minute window.
		stepTo(tr, tr.clock.Now().Add(2*testReportRetryDelay))
		processNextReport(t, tr)
		if reportDue(tr) || reportDueOnDisk() {
			t.Error("report still due after DefaultReportGiveUpAfter elapsed")
		}
		if got := gaveUp(); !slices.Equal(got, []slog.Level{slog.LevelWarn}) {
			t.Errorf("give-up logged at %v, want once at Warn", got)
		}
		stepTo(tr, tr.clock.Now().Add(testMaxReportRetryDelay))
		time.Sleep(50 * time.Millisecond)
		if n := tr.reporting.queue.Len(); n != 0 {
			t.Errorf("reports queued = %d after giving up, want none", n)
		}

		// A later background detach starts a fresh 10-minute window.
		detachNow(t, tr)
		processNextReport(t, tr)
		sent := reporter.reports()
		if len(sent) != 4 || sent[3].state.GetLatestLocal() != nil {
			t.Errorf("reports after a later detach = %d (last = %v), want 4 with the detach reported", len(sent), sent[len(sent)-1].state)
		}
	})

	t.Run("honors WithReportGiveUpAfter", func(t *testing.T) {
		const giveUp = 2 * time.Minute
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		reporter := newFakeReporter()
		unavailable := status.Error(codes.Unavailable, "ateapi is down")
		reporter.failNext(unavailable, unavailable)
		useActorDir(t)
		tr := New(t.Context(), store, clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0)),
			WithUploadDelay(testUploadDelay), WithDetachDelay(testDetachDelay), WithReportGiveUpAfter(giveUp))
		startReporting(tr, reporter)
		tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
		uploadNow(t, tr, store, nil)
		dueAt := tr.clock.Now()

		processNextReport(t, tr)
		stepTo(tr, dueAt.Add(giveUp))
		processNextReport(t, tr)
		if reportDue(tr) || reportDueOnDisk() {
			t.Error("report still due after configured give-up duration elapsed")
		}
	})

	t.Run("keeps a newer report made due during the last attempt", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		reporter := newFakeReporter()
		reporter.setBlocking(true)
		tr := newIdleTestTracker(t, store)
		tr.reporting.reporter = reporter
		tr.Start()
		tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
		uploadNow(t, tr, store, nil)
		first := waitReported(t, reporter)

		// Advance past the 10-minute window of the upload report, then run a
		// background detach while the upload report is still in flight.
		stepTo(tr, tr.clock.Now().Add(DefaultReportGiveUpAfter))
		detachNow(t, tr)

		clk := tr.clock.(*clocktesting.FakeClock)
		waitersBefore := clk.Waiters()
		reporter.setBlocking(false)
		reporter.release <- status.Error(codes.Unavailable, "ateapi was down")
		eventually(t, "the report queue backs off", func() bool { return clk.Waiters() > waitersBefore })
		stepTo(tr, tr.clock.Now().Add(reportRetryDelay))
		second := waitReported(t, reporter)
		if first.state.GetLatestLocal() == nil || second.state.GetLatestLocal() != nil {
			t.Errorf("reports = (%v, %v), want the upload report then the detach report", first.state, second.state)
		}
		eventually(t, "the detach report is acknowledged", func() bool { return !reportDue(tr) })
	})
}

// A control-plane failure backs off the whole report queue, so other actors
// waiting to report do not all hit an overloaded ateapi at once. Once an
// attempt gets a response, the queue backoff resets; Aborted, which means
// only that single actor conflicted, does not hold back other actors.
func TestTrackerBacksOffReportQueueOnServerFailure(t *testing.T) {
	makeActorReportDue := func(t *testing.T, tr *Tracker, store *fakeSnapshotStore, actorUID, uuid string) {
		t.Helper()
		if err := os.MkdirAll(ateletpath.ActorPath(actorUID), 0o700); err != nil {
			t.Fatal(err)
		}
		tr.CommitLocal(actorUID, committedRef(uuid, 1, tr.clock.Now()))
		stepTo(tr, timeOf(tr.State(actorUID).GetUploadDueTime()))
		waitStarted(t, store)
		done := opDone(t, tr, actorUID)
		store.release <- nil
		waitClosed(t, done, "the upload")
	}

	newQueueBackoffTracker := func(t *testing.T, store *fakeSnapshotStore, reporter *fakeReporter) *Tracker {
		t.Helper()
		tr := newIdleTestTracker(t, store)
		tr.reporting.reporter = reporter
		// Short of giveUpAfter so the second actor's upload step does not
		// expire the first actor's report window.
		tr.uploadDelay = 2 * time.Minute
		tr.reporting.startupJitter = 0
		tr.reporting.retries = workqueue.NewTypedItemExponentialFailureRateLimiter[string](testReportRetryDelay, testMaxReportRetryDelay)
		tr.reporting.queueRetries = workqueue.NewTypedItemExponentialFailureRateLimiter[string](testReportRetryDelay, testMaxReportRetryDelay)
		tr.Start()
		return tr
	}

	t.Run("Unavailable holds back the next actor", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		reporter := newFakeReporter()
		reporter.setBlocking(true)
		tr := newQueueBackoffTracker(t, store, reporter)

		// Put actor-uid-1 in flight in reporter, and queue actor-uid-2 and
		// actor-uid-3 behind it.
		makeActorReportDue(t, tr, store, "actor-uid-1", "s1")
		if got := waitReported(t, reporter); got.actorUID != "actor-uid-1" {
			t.Fatalf("first report for %q, want actor-uid-1", got.actorUID)
		}
		makeActorReportDue(t, tr, store, "actor-uid-2", "s2")
		makeActorReportDue(t, tr, store, "actor-uid-3", "s3")

		clk := tr.clock.(*clocktesting.FakeClock)
		waitersBefore := clk.Waiters()
		failedAt := clk.Now()
		reporter.setBlocking(false)
		reporter.release <- status.Error(codes.Unavailable, "ateapi overloaded")
		// Wait until both actor-uid-1's retry and the queue backoff timer are waiting.
		eventually(t, "the report queue backs off", func() bool { return clk.Waiters() >= waitersBefore+2 })

		// The jittered queue delay is in [testReportRetryDelay/2, testReportRetryDelay).
		stepTo(tr, failedAt.Add(testReportRetryDelay/2-time.Second))
		time.Sleep(50 * time.Millisecond)
		if got := len(reporter.reports()); got != 1 {
			t.Fatalf("reports before the queue backoff passed = %d, want only the 1 failed attempt", got)
		}

		// Step past the jittered queue delay, just short of actor-uid-1's own retry.
		stepTo(tr, failedAt.Add(testReportRetryDelay-time.Nanosecond))
		if got := waitReported(t, reporter); got.actorUID != "actor-uid-2" {
			t.Errorf("second report for %q, want actor-uid-2", got.actorUID)
		}
		// actor-uid-2 succeeded, resetting the queue backoff: actor-uid-3 reports right away.
		if got := waitReported(t, reporter); got.actorUID != "actor-uid-3" {
			t.Errorf("third report for %q, want actor-uid-3 without stepping the clock", got.actorUID)
		}
	})

	t.Run("Aborted does not hold back other actors", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		reporter := newFakeReporter()
		reporter.setBlocking(true)
		tr := newQueueBackoffTracker(t, store, reporter)

		makeActorReportDue(t, tr, store, "actor-uid-1", "s1")
		if got := waitReported(t, reporter); got.actorUID != "actor-uid-1" {
			t.Fatalf("first report for %q, want actor-uid-1", got.actorUID)
		}
		makeActorReportDue(t, tr, store, "actor-uid-2", "s2")

		reporter.setBlocking(false)
		reporter.release <- status.Error(codes.Aborted, "another operation is changing the actor")
		if got := waitReported(t, reporter); got.actorUID != "actor-uid-2" {
			t.Errorf("second report for %q, want actor-uid-2 without stepping the clock", got.actorUID)
		}
	})

	t.Run("queue backoff caps at maxReportQueueRetryDelay", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		reporter := newFakeReporter()
		unavailable := status.Error(codes.Unavailable, "ateapi down")
		reporter.failNext(unavailable, unavailable, unavailable, unavailable, unavailable)
		tr := newIdleTestTracker(t, store)
		tr.reporting.reporter = reporter
		tr.reporting.workers = 0
		tr.reporting.startupJitter = 0
		tr.Start()

		var delay time.Duration
		for i := range 5 {
			actorUID := "actor-uid-cap-" + string(rune('1'+i))
			makeActorReportDue(t, tr, store, actorUID, "s1")
			delay = tr.report(actorUID)
		}
		if delay < maxReportQueueRetryDelay/2 || delay >= maxReportQueueRetryDelay {
			t.Errorf("5th queue backoff delay = %v, want in [%v, %v)", delay, maxReportQueueRetryDelay/2, maxReportQueueRetryDelay)
		}
	})
}

// A report the control plane refuses, about an actor gone from this node or
// one that conflicts with its record, or because it does not serve reports,
// is not retried and no longer due. NotFound and Unimplemented are expected,
// so they are logged at Info rather than Warn.
func TestTrackerDropsRefusedReport(t *testing.T) {
	for _, tc := range []struct {
		code  codes.Code
		level slog.Level
	}{
		{codes.NotFound, slog.LevelInfo},
		{codes.FailedPrecondition, slog.LevelWarn},
		{codes.InvalidArgument, slog.LevelWarn},
		{codes.Unimplemented, slog.LevelInfo},
	} {
		t.Run(tc.code.String(), func(t *testing.T) {
			levels := recordLogLevels(t, "The control plane refused the actor's snapshot state")
			store := newFakeSnapshotStore()
			store.setBlocking(true)
			reporter := newFakeReporter()
			reporter.failNext(status.Error(tc.code, "refused"))
			tr := newReportingTracker(t, store, reporter)
			tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
			uploadNow(t, tr, store, nil)

			processNextReport(t, tr)
			if reportDue(tr) || reportDueOnDisk() {
				t.Error("report still due after the control plane refused it")
			}
			if got := levels(); !slices.Equal(got, []slog.Level{tc.level}) {
				t.Errorf("refusal logged at %v, want once at %v", got, tc.level)
			}
			stepTo(tr, tr.clock.Now().Add(testMaxReportRetryDelay))
			time.Sleep(50 * time.Millisecond)
			if n := tr.reporting.queue.Len(); n != 0 {
				t.Errorf("reports queued = %d after a refusal, want none", n)
			}
		})
	}
}

// recordLogLevels sends the default logger's records to a handler that
// records the level of each one logged with msg, until the test ends, and
// returns a function that lists those levels.
func recordLogLevels(t *testing.T, msg string) func() []slog.Level {
	t.Helper()
	h := &levelRecorder{msg: msg}
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return func() []slog.Level {
		h.mu.Lock()
		defer h.mu.Unlock()
		return slices.Clone(h.levels)
	}
}

// levelRecorder is a slog.Handler that records the level of each record
// logged with msg.
type levelRecorder struct {
	msg    string
	mu     sync.Mutex
	levels []slog.Level
}

func (h *levelRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *levelRecorder) Handle(_ context.Context, r slog.Record) error {
	if r.Message == h.msg {
		h.mu.Lock()
		h.levels = append(h.levels, r.Level)
		h.mu.Unlock()
	}
	return nil
}

func (h *levelRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *levelRecorder) WithGroup(string) slog.Handler { return h }

// A report due survives an atelet restart: a tracker started over the same
// node directories sends it. Once it is acknowledged, a further restart sends
// nothing.
func TestTrackerReportsAfterRestart(t *testing.T) {
	t.Run("sends saved report and clears it", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		// The first tracker has no reporter: it saves the report due, as an
		// atelet that restarts before sending it does, but queues nothing.
		before := newTestTracker(t, store)
		before.CommitLocal(trackedActorUID, committedRef("s1", 1, before.clock.Now()))
		uploadNow(t, before, store, nil)
		if n := before.reporting.queue.Len(); n != 0 {
			t.Errorf("reports queued = %d without a reporter, want none", n)
		}

		reporter := newFakeReporter()
		after := newStoreTracker(t, store)
		startReporting(after, reporter)
		processNextReport(t, after)
		sent := reporter.reports()
		if len(sent) != 1 || uuidOf(sent[0].state.GetLatestDurable()) != "s1" || sent[0].actor != testActor {
			t.Fatalf("reports after the restart = %+v, want one of s1 durable, for %+v", sent, testActor)
		}

		again := newStoreTracker(t, store)
		startReporting(again, reporter)
		if n := again.reporting.queue.Len(); n != 0 {
			t.Errorf("reports queued after a second restart = %d, want 0", n)
		}
		if got := len(reporter.reports()); got != 1 {
			t.Errorf("reports after a second restart = %d, want still 1", got)
		}
	})

	t.Run("sends saved detach report and removes actor directory", func(t *testing.T) {
		store := newFakeSnapshotStore()
		// The first tracker has no reporter: it saves the detach report due,
		// as an atelet that restarts before sending it does, but queues
		// nothing.
		before := newTestTracker(t, store)
		before.CommitLocal(trackedActorUID, committedRef("s1", 1, before.clock.Now()))
		detachNow(t, before)
		if !reportDueOnDisk() {
			t.Fatal("saved detach report not due on disk before restart")
		}

		reporter := newFakeReporter()
		after := newStoreTracker(t, store)
		startReporting(after, reporter)
		processNextReport(t, after)
		sent := reporter.reports()
		if len(sent) != 1 || sent[0].state.GetLatestLocal() != nil || uuidOf(sent[0].state.GetLatestDurable()) != "s1" || sent[0].actor != testActor {
			t.Fatalf("reports after the restart = %+v, want one detached report with s1 durable, for %+v", sent, testActor)
		}
		if _, err := os.Stat(ateletpath.ActorPath(trackedActorUID)); !os.IsNotExist(err) {
			t.Errorf("actor dir %s after saved detach report acknowledged: %v", ateletpath.ActorPath(trackedActorUID), err)
		}
	})

	t.Run("jitters saved reports at startup", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		before := newTestTracker(t, store)
		before.CommitLocal(trackedActorUID, committedRef("s1", 1, before.clock.Now()))
		uploadNow(t, before, store, nil)

		reporter := newFakeReporter()
		after := newStoreTracker(t, store)
		after.reporting.reporter = reporter
		after.reporting.workers = 0
		after.Start()
		if n := after.reporting.queue.Len(); n != 0 {
			t.Fatalf("reports queued right at startup = %d, want delayed by startup jitter", n)
		}

		stepTo(after, after.clock.Now().Add(reportStartupJitter))
		processNextReport(t, after)
		if got := len(reporter.reports()); got != 1 {
			t.Errorf("reports after startup jitter elapsed = %d, want 1", got)
		}
	})

	t.Run("fresh background change does not wait for startup jitter", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		before := newTestTracker(t, store)
		before.CommitLocal(trackedActorUID, committedRef("s1", 1, before.clock.Now()))
		uploadNow(t, before, store, nil)

		reporter := newFakeReporter()
		after := newStoreTracker(t, store)
		after.reporting.reporter = reporter
		after.reporting.workers = 0
		after.Start()

		detachNow(t, after)
		processNextReport(t, after)
		sent := reporter.reports()
		if len(sent) != 1 || sent[0].state.GetLatestLocal() != nil {
			t.Errorf("reports after fresh detach = %+v, want 1 detached report without waiting for startup jitter", sent)
		}
	})
}

// A report whose actor's LatestLocal or LatestDurable changed in the
// background while it ran is sent again with the new state, and stays due
// until a report of the current state is acknowledged. A change to Uploading
// alone does not queue another report.
func TestTrackerReportsStateChangedDuringReport(t *testing.T) {
	t.Run("reports again when LatestLocal changes", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		reporter := newFakeReporter()
		reporter.setBlocking(true)
		tr := newIdleTestTracker(t, store)
		tr.reporting.reporter = reporter
		tr.Start()
		tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
		uploadNow(t, tr, store, nil)
		first := waitReported(t, reporter)

		detachNow(t, tr)
		reporter.setBlocking(false)
		reporter.release <- nil
		second := waitReported(t, reporter)
		if got, want := []string{uuidOf(first.state.GetLatestLocal()), uuidOf(second.state.GetLatestLocal())}, []string{"s1", ""}; !slices.Equal(got, want) {
			t.Errorf("reported latest_local = %v, want %v", got, want)
		}
		eventually(t, "the report is no longer due", func() bool { return !reportDue(tr) })
	})

	t.Run("ignores Uploading starting during report", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		reporter := newFakeReporter()
		reporter.setBlocking(true)
		tr := newIdleTestTracker(t, store)
		tr.reporting.reporter = reporter
		tr.Start()
		tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
		s1Done := startUpload(t, tr, store)
		tr.CommitLocal(trackedActorUID, committedRef("s2", 2, tr.clock.Now()))
		store.release <- nil
		waitClosed(t, s1Done, "s1 upload")
		first := waitReported(t, reporter)
		if first.state.GetUploading() != nil {
			t.Fatalf("first report uploading = %v, want nil", first.state.GetUploading())
		}

		// Start s2's background upload while s1's report is still in flight.
		s2Done := startUpload(t, tr, store)
		reporter.setBlocking(false)
		reporter.release <- nil
		eventually(t, "s1 report finishes", func() bool { return !reportDue(tr) })
		if got := len(reporter.reports()); got != 1 {
			t.Errorf("reports = %d after Uploading started during s1 report, want only 1", got)
		}

		// Releasing s2's upload then reports s2 durable as usual.
		store.release <- nil
		waitClosed(t, s2Done, "s2 upload")
		second := waitReported(t, reporter)
		if got := uuidOf(second.state.GetLatestDurable()); got != "s2" {
			t.Errorf("second report latest_durable = %q, want s2", got)
		}
	})
}

// A Checkpoint or PromoteSnapshot while a local snapshot is kept returns the
// actor's latest state to the control plane, so it cancels any pending
// background upload report, whether queued in retry backoff or already in
// flight.
func TestTrackerCancelsReportOnForegroundCall(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(t *testing.T, tr *Tracker)
	}{
		{
			name: "CommitLocal",
			call: func(_ *testing.T, tr *Tracker) {
				tr.CommitLocal(trackedActorUID, committedRef("s2", 2, tr.clock.Now()))
			},
		},
		{
			name: "CommitDurable",
			call: func(_ *testing.T, tr *Tracker) {
				tr.CommitDurable(trackedActorUID, committedRef("s2", 2, tr.clock.Now()))
			},
		},
		{
			name: "CommitPromoted",
			call: func(_ *testing.T, tr *Tracker) {
				tr.CommitPromoted(trackedActorUID, committedRef("s2", 2, tr.clock.Now()))
			},
		},
		{
			name: "CommitPromoted of already durable snapshot",
			call: func(_ *testing.T, tr *Tracker) {
				tr.CommitPromoted(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
			},
		},
		{
			name: "CommittedCheckpointState with local snapshot",
			call: func(_ *testing.T, tr *Tracker) {
				tr.CommittedCheckpointState(trackedActorUID)
			},
		},
	} {
		makeReportDue := func(t *testing.T, tr *Tracker, store *fakeSnapshotStore) {
			t.Helper()
			tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
			store.setBlocking(true)
			uploadNow(t, tr, store, nil)
			store.setBlocking(false)
		}

		t.Run(tc.name+"/queued in retry backoff", func(t *testing.T) {
			store := newFakeSnapshotStore()
			reporter := newFakeReporter()
			reporter.failNext(status.Error(codes.Unavailable, "ateapi is restarting"))
			tr := newReportingTracker(t, store, reporter)
			makeReportDue(t, tr, store)
			processNextReport(t, tr)
			if !reportDue(tr) || !reportDueOnDisk() {
				t.Fatal("report not due before the foreground call")
			}

			tc.call(t, tr)
			if reportDue(tr) || reportDueOnDisk() {
				t.Error("report still due after the foreground call")
			}
			stepTo(tr, tr.clock.Now().Add(testMaxReportRetryDelay))
			processNextReport(t, tr)
			if got := len(reporter.reports()); got != 1 {
				t.Errorf("reports = %d, want only the 1 attempt before the foreground call", got)
			}
		})

		for _, inFlight := range []struct {
			name string
			err  error
		}{
			{"in flight succeeds", nil},
			{"in flight fails with Aborted", status.Error(codes.Aborted, "actor is SUSPENDING")},
		} {
			t.Run(tc.name+"/"+inFlight.name, func(t *testing.T) {
				failedLogs := recordLogLevels(t, "Failed to report the actor's snapshot state")
				store := newFakeSnapshotStore()
				reporter := newFakeReporter()
				reporter.setBlocking(true)
				tr := newReportingTracker(t, store, reporter)
				makeReportDue(t, tr, store)
				done := make(chan struct{})
				go func() {
					defer close(done)
					processNextReport(t, tr)
				}()
				waitReported(t, reporter)

				tc.call(t, tr)
				if reportDue(tr) || reportDueOnDisk() {
					t.Fatal("report still due right after the foreground call")
				}
				reporter.setBlocking(false)
				reporter.release <- inFlight.err
				waitClosed(t, done, "the in-flight report")

				stepTo(tr, tr.clock.Now().Add(testMaxReportRetryDelay))
				time.Sleep(50 * time.Millisecond)
				if reportDue(tr) || reportDueOnDisk() {
					t.Error("report became due again after the canceled in-flight report ended")
				}
				if n := tr.reporting.queue.Len(); n != 0 {
					t.Errorf("reports queued = %d after the canceled in-flight report ended, want none", n)
				}
				if got := failedLogs(); len(got) != 0 {
					t.Errorf("canceled in-flight report logged failure at %v, want none", got)
				}
			})
		}
	}
}

// CommitPromoted or CommittedCheckpointState after a background detach cleared
// LatestLocal leaves the pending detach report due, so the report still
// completes and removes the actor's state file and directory.
func TestTrackerKeepsDetachReportOnCommitPromotedWhenDetached(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(tr *Tracker)
	}{
		{
			name: "CommitPromoted",
			call: func(tr *Tracker) {
				tr.CommitPromoted(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
			},
		},
		{
			name: "CommittedCheckpointState",
			call: func(tr *Tracker) {
				tr.CommittedCheckpointState(trackedActorUID)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeSnapshotStore()
			reporter := newFakeReporter()
			tr := newReportingTracker(t, store, reporter)
			tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
			detachNow(t, tr)
			if !reportDue(tr) || !reportDueOnDisk() {
				t.Fatal("detach report not due before foreground call")
			}

			tc.call(tr)
			if !reportDue(tr) || !reportDueOnDisk() {
				t.Fatalf("detach report canceled by %s when LatestLocal was nil", tc.name)
			}
			processNextReport(t, tr)
			sent := reporter.reports()
			if len(sent) != 1 || sent[0].state.GetLatestLocal() != nil || uuidOf(sent[0].state.GetLatestDurable()) != "s1" {
				t.Errorf("reports = %+v, want 1 detach report with s1 durable", sent)
			}
			if reportDue(tr) || reportDueOnDisk() {
				t.Error("detach report still due once acknowledged")
			}
			if _, err := os.Stat(ateletpath.ActorPath(trackedActorUID)); !os.IsNotExist(err) {
				t.Errorf("actor dir %s still exists after detach report acknowledged: %v", ateletpath.ActorPath(trackedActorUID), err)
			}
		})
	}
}

// Forget does not wait for a report in progress, which records nothing once
// it ends.
func TestTrackerForgetDuringReport(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	reporter := newFakeReporter()
	reporter.setBlocking(true)
	tr := newIdleTestTracker(t, store)
	tr.reporting.reporter = reporter
	tr.Start()
	tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
	uploadNow(t, tr, store, nil)
	waitReported(t, reporter)

	returnsPromptly(t, "Forget during a report", func() {
		if err := tr.Forget(trackedActorUID); err != nil {
			t.Errorf("Forget: %v", err)
		}
	})
	reporter.release <- nil
	select {
	case r := <-reporter.reported:
		t.Errorf("reported %v after Forget, want nothing", r.state)
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := os.Stat(ateletpath.ActorSnapshotStateFile(trackedActorUID)); !os.IsNotExist(err) {
		t.Errorf("state file once the report ended: stat = %v, want none", err)
	}
}

// A report in progress as atelet shuts down stays due, for the next atelet to
// send, and is logged at Info rather than Warn, whether it fails with Canceled
// or another retryable error.
func TestTrackerReportStaysDueAtShutdown(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"canceled by shutdown", nil},
		{"Unavailable during shutdown", status.Error(codes.Unavailable, "connection closing")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useActorDir(t)
			levels := recordLogLevels(t, "Failed to report the actor's snapshot state")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			store := newFakeSnapshotStore()
			store.setBlocking(true)
			reporter := newFakeReporter()
			reporter.setBlocking(true)
			if tc.err != nil {
				reporter.failNext(tc.err)
			}
			tr := New(ctx, store, clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0)),
				WithUploadDelay(testUploadDelay), WithDetachDelay(testDetachDelay), WithReporter(reporter))
			tr.Start()
			tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
			uploadNow(t, tr, store, nil)
			waitReported(t, reporter)

			// Step past DefaultReportGiveUpAfter so retryReport would drop the
			// report if shutdown fell through to it.
			stepTo(tr, tr.clock.Now().Add(DefaultReportGiveUpAfter))
			cancel()
			if err := tr.Wait(t.Context()); err != nil {
				t.Fatalf("Wait: %v", err)
			}
			if !reportDueOnDisk() {
				t.Error("state file after shutdown records no report due, want the interrupted report still due")
			}
			if got := levels(); !slices.Equal(got, []slog.Level{slog.LevelInfo}) {
				t.Errorf("shutdown-interrupted report logged at %v, want once at Info", got)
			}
		})
	}
}

// A report waits while a Run, Restore, Checkpoint, PromoteSnapshot, or
// Terminate holds the actor, since the control plane holds the actor's lease
// throughout that call and would refuse the report with Aborted. Once the hold
// ends without a commit canceling the report, or a Terminate forgetting the
// actor, the report is queued.
func TestTrackerDefersReportDuringHold(t *testing.T) {
	holds := []struct {
		name  string
		begin func(t *testing.T, tr *Tracker) func()
	}{
		{
			name: "BeginStart",
			begin: func(t *testing.T, tr *Tracker) func() {
				end, err := tr.BeginStart(t.Context(), trackedActorUID)
				if err != nil {
					t.Fatalf("BeginStart: %v", err)
				}
				return end
			},
		},
		{
			name: "BeginCheckpoint",
			begin: func(t *testing.T, tr *Tracker) func() {
				_, end, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, committedRef("s2", 2, tr.clock.Now()).Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
				if err != nil {
					t.Fatalf("BeginCheckpoint: %v", err)
				}
				return end
			},
		},
		{
			name: "BeginCheckpoint immediate",
			begin: func(t *testing.T, tr *Tracker) func() {
				_, end, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, committedRef("s2", 2, tr.clock.Now()).Snapshot, ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE)
				if err != nil {
					t.Fatalf("BeginCheckpoint: %v", err)
				}
				return end
			},
		},
		{
			name: "BeginPromote",
			begin: func(t *testing.T, tr *Tracker) func() {
				// Promote a snapshot other than s1 so BeginPromote does not
				// wait for the background upload of s1.
				_, end, err := tr.BeginPromote(t.Context(), trackedActorUID, committedRef("s2", 2, tr.clock.Now()))
				if err != nil {
					t.Fatalf("BeginPromote: %v", err)
				}
				return end
			},
		},
		{
			// A Terminate that fails before Forget.
			name: "BeginTerminate",
			begin: func(t *testing.T, tr *Tracker) func() {
				return tr.BeginTerminate(trackedActorUID)
			},
		},
	}

	for _, h := range holds {
		t.Run(h.name+"/upload finishes during hold", func(t *testing.T) {
			store := newFakeSnapshotStore()
			store.setBlocking(true)
			reporter := newFakeReporter()
			tr := newReportingTracker(t, store, reporter)
			tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
			done := startUpload(t, tr, store)

			end := h.begin(t, tr)
			store.release <- nil
			waitClosed(t, done, "the upload")
			if n := tr.reporting.queue.Len(); n != 0 {
				t.Fatalf("reports queued during hold = %d, want 0", n)
			}
			if !reportDue(tr) || !reportDueOnDisk() {
				t.Fatal("report not marked due after the upload finished")
			}

			end()
			processNextReport(t, tr)
			if got := len(reporter.reports()); got != 1 {
				t.Errorf("reports after hold ended = %d, want 1", got)
			}
			if reportDue(tr) {
				t.Error("report still due once acknowledged")
			}
		})

		t.Run(h.name+"/queued report popped during hold", func(t *testing.T) {
			store := newFakeSnapshotStore()
			store.setBlocking(true)
			reporter := newFakeReporter()
			tr := newReportingTracker(t, store, reporter)
			tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
			uploadNow(t, tr, store, nil)

			end := h.begin(t, tr)
			processNextReport(t, tr)
			if got := len(reporter.reports()); got != 0 {
				t.Fatalf("reports sent during hold = %d, want 0", got)
			}
			if n := tr.reporting.queue.Len(); n != 0 {
				t.Fatalf("reports queued while hold still open = %d, want 0", n)
			}
			if !reportDue(tr) {
				t.Fatal("report no longer due while deferred by hold")
			}

			end()
			processNextReport(t, tr)
			if got := len(reporter.reports()); got != 1 {
				t.Errorf("reports after hold ended = %d, want 1", got)
			}
			if reportDue(tr) {
				t.Error("report still due once acknowledged")
			}
		})
	}

	t.Run("commit during hold cancels deferred report", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		reporter := newFakeReporter()
		tr := newReportingTracker(t, store, reporter)
		tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
		uploadNow(t, tr, store, nil)

		s2 := committedRef("s2", 2, tr.clock.Now())
		_, end, err := tr.BeginCheckpoint(t.Context(), trackedActorUID, s2.Snapshot, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
		if err != nil {
			t.Fatalf("BeginCheckpoint: %v", err)
		}
		processNextReport(t, tr)
		tr.CommitLocal(trackedActorUID, s2)
		end()

		if n := tr.reporting.queue.Len(); n != 0 {
			t.Errorf("reports queued after commit during hold = %d, want 0", n)
		}
		if reportDue(tr) {
			t.Error("report still due after commit during hold")
		}
	})

	t.Run("hold during retry backoff keeps retry delay", func(t *testing.T) {
		store := newFakeSnapshotStore()
		store.setBlocking(true)
		reporter := newFakeReporter()
		reporter.failNext(status.Error(codes.Unavailable, "ateapi down"))
		tr := newReportingTracker(t, store, reporter)
		tr.CommitLocal(trackedActorUID, committedRef("s1", 1, tr.clock.Now()))
		uploadNow(t, tr, store, nil)
		failedAt := tr.clock.Now()
		processNextReport(t, tr)

		end, err := tr.BeginStart(t.Context(), trackedActorUID)
		if err != nil {
			t.Fatalf("BeginStart: %v", err)
		}
		end()

		stepTo(tr, failedAt.Add(testReportRetryDelay-time.Second))
		time.Sleep(50 * time.Millisecond)
		if n := tr.reporting.queue.Len(); n != 0 {
			t.Fatalf("reports queued before retry delay passed = %d, want 0", n)
		}
		stepTo(tr, failedAt.Add(testReportRetryDelay))
		processNextReport(t, tr)
		if got := len(reporter.reports()); got != 2 {
			t.Errorf("reports after retry delay = %d, want 2", got)
		}
	})
}
