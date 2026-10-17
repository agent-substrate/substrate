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
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/wait"
	clocktesting "k8s.io/utils/clock/testing"

	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
)

// reportCall is one call a fakeReporter received.
type reportCall struct {
	actor    resources.ActorRef
	actorUID string
	state    *ateletpb.ActorSnapshotState
}

// fakeReporter is a Reporter that records its calls and answers each with the
// next scripted error, or nil once the script runs out.
type fakeReporter struct {
	// block, if not nil, holds every call until it is closed or the call's
	// context ends.
	block chan struct{}

	mu     sync.Mutex
	script []error
	calls  []reportCall
}

func (f *fakeReporter) ReportActorSnapshotState(ctx context.Context, actor resources.ActorRef, actorUID string, state *ateletpb.ActorSnapshotState) error {
	f.mu.Lock()
	f.calls = append(f.calls, reportCall{actor: actor, actorUID: actorUID, state: proto.CloneOf(state)})
	var err error
	if len(f.script) > 0 {
		err, f.script = f.script[0], f.script[1:]
	}
	block := f.block
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (f *fakeReporter) reports() []reportCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]reportCall(nil), f.calls...)
}

// fastReportBackoff retries reports at once, so tests need not wait.
var fastReportBackoff = wait.Backoff{Steps: defaultReportBackoff.Steps, Duration: time.Millisecond, Factor: 2}

// waitReports waits until rep has received n calls.
func waitReports(t *testing.T, rep *fakeReporter, n int) []reportCall {
	t.Helper()
	eventually(t, "the reports are made", func() bool { return len(rep.reports()) >= n })
	return rep.reports()
}

// noMoreReports fails the test if rep receives more than n calls within a
// short while.
func noMoreReports(t *testing.T, rep *fakeReporter, n int) {
	t.Helper()
	time.Sleep(100 * time.Millisecond)
	if got := len(rep.reports()); got != n {
		t.Fatalf("got %d reports, want %d", got, n)
	}
}

// newReportTracker returns a tracker with no store that reports to rep and
// retries at once, for tests that call report directly.
func newReportTracker(ctx context.Context, rep *fakeReporter) *Tracker {
	tr := New(ctx, nil, clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0)), WithReporter(rep))
	tr.reportBackoff = fastReportBackoff
	return tr
}

// newReportingTracker returns a tracker like newTestTracker's that reports to
// rep, with its workers running.
func newReportingTracker(t *testing.T, store Store, rep *fakeReporter) *Tracker {
	t.Helper()
	tr := newIdleTestTracker(t, store)
	tr.reporter = rep
	tr.reportBackoff = fastReportBackoff
	tr.Start()
	return tr
}

// actorRef is testRef's snapshot, recorded for testActor as a Checkpoint
// records it.
func actorRef(uuid string, generation int32, commitTime time.Time) SnapshotRef {
	ref := testRef(uuid, generation, commitTime)
	ref.Actor = testActor
	return ref
}

// A background upload that succeeds reports the state it leaves: the snapshot
// durable, and its local copy kept.
func TestTrackerReportsAfterBackgroundUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	rep := &fakeReporter{}
	tr := newReportingTracker(t, store, rep)
	tr.CommitLocal(trackedActorUID, actorRef("s1", 1, tr.clock.Now()))

	uploadNow(t, tr, store, nil)

	got := waitReports(t, rep, 1)
	noMoreReports(t, rep, 1)
	if got[0].actor != testActor || got[0].actorUID != trackedActorUID {
		t.Errorf("reported for %+v/%s, want %+v/%s", got[0].actor, got[0].actorUID, testActor, trackedActorUID)
	}
	if d, l := uuidOf(got[0].state.GetLatestDurable()), uuidOf(got[0].state.GetLatestLocal()); d != "s1" || l != "s1" {
		t.Errorf("reported latest_durable %q, latest_local %q, want s1 and s1", d, l)
	}
}

// A background upload that fails reports nothing: the control plane's record
// is still right.
func TestTrackerDoesNotReportFailedUpload(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	rep := &fakeReporter{}
	tr := newReportingTracker(t, store, rep)
	tr.CommitLocal(trackedActorUID, actorRef("s1", 1, tr.clock.Now()))

	uploadNow(t, tr, store, errors.New("object store down"))

	noMoreReports(t, rep, 0)
}

// A background detach reports the state it leaves, no local snapshot and the
// latest snapshot durable, whether or not it had to upload that snapshot.
func TestTrackerReportsAfterBackgroundDetach(t *testing.T) {
	for _, tc := range []struct {
		name        string
		promote     bool
		wantUploads int
	}{
		{name: "uploads the latest local snapshot", wantUploads: 1},
		{name: "latest local snapshot already durable", promote: true, wantUploads: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeSnapshotStore()
			rep := &fakeReporter{}
			tr := newReportingTracker(t, store, rep)
			tr.CommitLocal(trackedActorUID, actorRef("s1", 1, tr.clock.Now()))
			if tc.promote {
				tr.CommitPromoted(trackedActorUID, actorRef("s1", 1, time.Time{}))
			}

			detachNow(t, tr)

			got := waitReports(t, rep, 1)
			noMoreReports(t, rep, 1)
			if st := got[0].state; st.GetLatestLocal() != nil || st.GetUploading() != nil || uuidOf(st.GetLatestDurable()) != "s1" {
				t.Errorf("reported %v, want only latest_durable s1", st)
			}
			if n := len(store.uploads()); n != tc.wantUploads {
				t.Errorf("uploads = %d, want %d", n, tc.wantUploads)
			}
		})
	}
}

// A Detach call returns the state to its caller, so it reports nothing.
func TestTrackerDetachCallDoesNotReport(t *testing.T) {
	store := newFakeSnapshotStore()
	rep := &fakeReporter{}
	tr := newReportingTracker(t, store, rep)
	tr.CommitLocal(trackedActorUID, actorRef("s1", 1, tr.clock.Now()))

	if _, err := tr.Detach(t.Context(), trackedActorUID); err != nil {
		t.Fatalf("Detach: %v", err)
	}

	noMoreReports(t, rep, 0)
}

// A report in flight holds neither the actor's lock nor its operation, so a
// Checkpoint or a Detach call of the actor goes ahead meanwhile.
func TestTrackerReportDoesNotHoldTheActor(t *testing.T) {
	store := newFakeSnapshotStore()
	store.setBlocking(true)
	rep := &fakeReporter{block: make(chan struct{})}
	tr := newReportingTracker(t, store, rep)
	tr.CommitLocal(trackedActorUID, actorRef("s1", 1, tr.clock.Now()))
	uploadNow(t, tr, store, nil)
	waitReports(t, rep, 1)

	returnsPromptly(t, "BeginCheckpoint during a report", func() {
		end, err := tr.BeginCheckpoint(t.Context(), trackedActorUID)
		if err != nil {
			t.Errorf("BeginCheckpoint: %v", err)
			return
		}
		end()
	})
	returnsPromptly(t, "Detach during a report", func() {
		if _, err := tr.Detach(t.Context(), trackedActorUID); err != nil {
			t.Errorf("Detach: %v", err)
		}
	})
	close(rep.block)
}

// report retries only what the control plane says is worth retrying, at most
// reportBackoff's steps, and reads the state afresh for each attempt.
func TestReportRetries(t *testing.T) {
	aborted := status.Error(codes.Aborted, "busy")
	unavailable := status.Error(codes.Unavailable, "down")
	for _, tc := range []struct {
		name      string
		script    []error
		wantCalls int
	}{
		{name: "ok", wantCalls: 1},
		{name: "aborted then ok", script: []error{aborted}, wantCalls: 2},
		{name: "deadline then ok", script: []error{status.Error(codes.DeadlineExceeded, "slow")}, wantCalls: 2},
		{name: "unavailable every time gives up", script: []error{unavailable, unavailable, unavailable, unavailable}, wantCalls: defaultReportBackoff.Steps},
		{name: "not found", script: []error{status.Error(codes.NotFound, "gone")}, wantCalls: 1},
		{name: "failed precondition", script: []error{status.Error(codes.FailedPrecondition, "conflict")}, wantCalls: 1},
		{name: "invalid argument", script: []error{status.Error(codes.InvalidArgument, "bad")}, wantCalls: 1},
		{name: "not a status", script: []error{errors.New("boom")}, wantCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useActorDir(t)
			rep := &fakeReporter{script: tc.script}
			tr := newReportTracker(t.Context(), rep)
			tr.CommitLocal(trackedActorUID, actorRef("s1", 1, tr.clock.Now()))

			done := make(chan struct{})
			go func() {
				defer close(done)
				tr.report(trackedActorUID, testActor, opUpload)
			}()
			waitReports(t, rep, tc.wantCalls)
			waitClosed(t, done, "the report")
			if got := len(rep.reports()); got != tc.wantCalls {
				t.Fatalf("got %d calls, want %d", got, tc.wantCalls)
			}
			for i, c := range rep.reports() {
				if uuidOf(c.state.GetLatestLocal()) != "s1" {
					t.Errorf("call %d reported %v, want latest_local s1", i, c.state)
				}
			}
		})
	}
}

// report stops once the tracker's context ends, even mid-attempt.
func TestReportStopsWhenTrackerStops(t *testing.T) {
	useActorDir(t)
	ctx, cancel := context.WithCancel(t.Context())
	rep := &fakeReporter{block: make(chan struct{})}
	tr := newReportTracker(ctx, rep)
	tr.CommitLocal(trackedActorUID, actorRef("s1", 1, tr.clock.Now()))

	done := make(chan struct{})
	go func() {
		defer close(done)
		tr.report(trackedActorUID, testActor, opUpload)
	}()
	waitReports(t, rep, 1)
	cancel()
	waitClosed(t, done, "the report after the tracker stopped")
	if got := len(rep.reports()); got != 1 {
		t.Errorf("got %d calls, want 1", got)
	}
}

// A snapshot that names no actor cannot be reported, since the control plane
// names actors by atespace and name.
func TestReportSkipsSnapshotWithoutActor(t *testing.T) {
	useActorDir(t)
	rep := &fakeReporter{}
	tr := newReportTracker(t.Context(), rep)
	tr.report(trackedActorUID, resources.ActorRef{}, opUpload)
	if got := len(rep.reports()); got != 0 {
		t.Errorf("got %d calls, want 0", got)
	}
}

func TestRetryableReportErr(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{nil, false},
		{status.Error(codes.Aborted, ""), true},
		{status.Error(codes.Unavailable, ""), true},
		{status.Error(codes.DeadlineExceeded, ""), true},
		{status.Error(codes.NotFound, ""), false},
		{status.Error(codes.FailedPrecondition, ""), false},
		{status.Error(codes.InvalidArgument, ""), false},
		{status.Error(codes.PermissionDenied, ""), false},
		{status.Error(codes.Internal, ""), false},
		{errors.New("boom"), false},
	} {
		if got := retryableReportErr(tc.err); got != tc.want {
			t.Errorf("retryableReportErr(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}
