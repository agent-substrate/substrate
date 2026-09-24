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

package glutton

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/boomerutil"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/agent-substrate/substrate/internal/benchmarking/glutton/fake"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGluttonIterate_SuspendMode(t *testing.T) {
	srv := &fake.Server{}
	fakeCtrl := &fakeControlClient{}
	cfg := newTestConfig(t, srv, &userclass.Config{
		APIStub:  fakeCtrl,
		Atespace: "bench-test",
		Dyn: dynconfig.NewHolder(dynconfig.Config{
			LifecycleMode: dynconfig.LifecycleModeSuspend,
		}),
	})

	rt := &taskRuntime{cfg: cfg}
	rt.iterate()

	calls := fakeCtrl.recordedCalls()
	if !slices.Contains(calls, "SuspendActor") {
		t.Errorf("expected SuspendActor in calls, got: %v", calls)
	}
	if slices.Contains(calls, "PauseActor") {
		t.Errorf("did not expect PauseActor in suspend mode calls, got: %v", calls)
	}
}

func TestGluttonIterate_PauseMode(t *testing.T) {
	srv := &fake.Server{}
	fakeCtrl := &fakeControlClient{}
	cfg := newTestConfig(t, srv, &userclass.Config{
		APIStub:  fakeCtrl,
		Atespace: "bench-test",
		Dyn: dynconfig.NewHolder(dynconfig.Config{
			LifecycleMode: dynconfig.LifecycleModePause,
		}),
	})

	rt := &taskRuntime{cfg: cfg}
	rt.iterate()

	calls := fakeCtrl.recordedCalls()
	if !slices.Contains(calls, "PauseActor") {
		t.Errorf("expected PauseActor in calls, got: %v", calls)
	}
	if slices.Contains(calls, "SuspendActor") {
		t.Errorf("did not expect SuspendActor in pause mode calls, got: %v", calls)
	}
}

func TestGluttonShutdown_PauseModeRunningActor(t *testing.T) {
	srv := &fake.Server{}
	fakeCtrl := &fakeControlClient{}
	cfg := newTestConfig(t, srv, &userclass.Config{
		APIStub:  fakeCtrl,
		Atespace: "bench-test",
		Dyn: dynconfig.NewHolder(dynconfig.Config{
			LifecycleMode: dynconfig.LifecycleModePause,
		}),
	})

	u := &gluttonUser{actors: []*gluttonActor{{
		cfg:          cfg,
		actorName:    "running-actor",
		actorRunning: true,
	}}}

	rt := &taskRuntime{cfg: cfg}
	rt.users.Store(boomerutil.GoroutineID(), u)
	rt.shutdown(context.Background())

	calls := fakeCtrl.recordedCalls()
	if len(calls) < 2 || calls[len(calls)-2] != "PauseActor" || calls[len(calls)-1] != "DeleteActor" {
		t.Errorf("recordedCalls must end with [PauseActor, DeleteActor], got %v", calls)
	}
	reqs := fakeCtrl.recordedDeleteRequests()
	if len(reqs) == 0 || !reqs[0].GetAnyState() {
		t.Errorf("DeleteActor must set AnyState=true, got %v", reqs)
	}
}

func TestGluttonShutdown_DeleteSetsAnyState(t *testing.T) {
	srv := &fake.Server{}
	fakeCtrl := &fakeControlClient{}
	cfg := newTestConfig(t, srv, &userclass.Config{
		APIStub:  fakeCtrl,
		Atespace: "bench-test",
		Dyn: dynconfig.NewHolder(dynconfig.Config{
			LifecycleMode: dynconfig.LifecycleModeSuspend,
		}),
	})

	u := &gluttonUser{actors: []*gluttonActor{{
		cfg:          cfg,
		actorName:    "stopped-actor",
		actorRunning: false,
	}}}

	rt := &taskRuntime{cfg: cfg}
	rt.users.Store(boomerutil.GoroutineID(), u)
	rt.shutdown(context.Background())

	calls := fakeCtrl.recordedCalls()
	if len(calls) != 1 || calls[0] != "DeleteActor" {
		t.Errorf("expected only DeleteActor call, got %v", calls)
	}
	reqs := fakeCtrl.recordedDeleteRequests()
	if len(reqs) == 0 || !reqs[0].GetAnyState() {
		t.Errorf("DeleteActor must set AnyState=true, got %v", reqs)
	}
}

// newReplacementRuntime builds a one-actor-per-VU suspend-mode runtime whose
// ResumeActor returns resumeErrs in order.
func newReplacementRuntime(t *testing.T, resumeErrs ...error) (*taskRuntime, *fakeControlClient) {
	t.Helper()
	fakeCtrl := &fakeControlClient{resumeErrs: resumeErrs}
	cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{
		APIStub:  fakeCtrl,
		Atespace: "bench-test",
		Dyn: dynconfig.NewHolder(dynconfig.Config{
			LifecycleMode: dynconfig.LifecycleModeSuspend,
		}),
	})
	return &taskRuntime{cfg: cfg}, fakeCtrl
}

func countCalls(calls []string, name string) int {
	n := 0
	for _, c := range calls {
		if c == name {
			n++
		}
	}
	return n
}

// FailedPrecondition is the left-in-SUSPENDING case: replaced on the first
// failure.
func TestGluttonIterate_ReplacesActorOnTerminalResumeFailure(t *testing.T) {
	rt, fakeCtrl := newReplacementRuntime(t, status.Error(codes.FailedPrecondition, "AssignWorker prerequisite not met (got: ACTOR_STATE_SUSPENDING)"))

	rt.iterate()

	calls := fakeCtrl.recordedCalls()
	if got := countCalls(calls, "CreateActor"); got != 2 {
		t.Errorf("CreateActor count = %d, want 2 (initial + replacement); calls = %v", got, calls)
	}
	if got := countCalls(calls, "DeleteActor"); got != 1 {
		t.Errorf("DeleteActor count = %d, want 1 (deleted broken actor); calls = %v", got, calls)
	}
}

// A saturated pool reports ResourceExhausted to every VU at once, so the
// actor must survive however long the shortage lasts.
func TestGluttonIterate_KeepsActorOnTransientResumeFailure(t *testing.T) {
	const iterations = maxConsecutiveFailures + 2
	errs := make([]error, iterations)
	for i := range errs {
		errs[i] = status.Error(codes.ResourceExhausted, "no free workers available")
	}
	rt, fakeCtrl := newReplacementRuntime(t, errs...)

	for range iterations {
		rt.iterate()
	}

	calls := fakeCtrl.recordedCalls()
	if got := countCalls(calls, "DeleteActor"); got != 0 {
		t.Errorf("DeleteActor count = %d, want 0 (capacity shortage is not the actor's fault); calls = %v", got, calls)
	}
	if got := countCalls(calls, "CreateActor"); got != 1 {
		t.Errorf("CreateActor count = %d, want 1 (the initial actor only); calls = %v", got, calls)
	}
}

// An exhausted conflict-retry budget only replaces the actor once it has
// repeated maxConsecutiveFailures times.
func TestGluttonIterate_ReplacesActorAfterRepeatedConflicts(t *testing.T) {
	errs := make([]error, resumeMaxAttempts*maxConsecutiveFailures)
	for i := range errs {
		errs[i] = conflictErr()
	}
	rt, fakeCtrl := newReplacementRuntime(t, errs...)

	for i := range maxConsecutiveFailures {
		rt.iterate()
		got := countCalls(fakeCtrl.recordedCalls(), "DeleteActor")
		want := 0
		if i == maxConsecutiveFailures-1 {
			want = 1
		}
		if got != want {
			t.Errorf("after iteration %d: DeleteActor count = %d, want %d", i+1, got, want)
		}
	}
}

// A retryable suspend failure strands the actor RUNNING/SUSPENDING: the next
// iteration re-drives the suspend rather than resume, which would fail
// FailedPrecondition and cost the actor for a transient error.
func TestGluttonIterate_RetriesStrandedHibernate(t *testing.T) {
	fakeCtrl := &fakeControlClient{
		suspendErrs: []error{status.Error(codes.Unavailable, "ate-api-server restarting")},
	}
	cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{
		APIStub:  fakeCtrl,
		Atespace: "bench-test",
		Dyn: dynconfig.NewHolder(dynconfig.Config{
			LifecycleMode: dynconfig.LifecycleModeSuspend,
		}),
	})
	rt := &taskRuntime{cfg: cfg}

	rt.iterate() // resume, ping, suspend → suspend fails
	before := fakeCtrl.recordedCalls()
	rt.iterate() // must re-drive the suspend, not resume

	after := fakeCtrl.recordedCalls()
	if got := after[len(after)-1]; got != "SuspendActor" {
		t.Errorf("last call = %q, want SuspendActor; calls = %v", got, after)
	}
	if got, want := countCalls(after, "ResumeActor"), countCalls(before, "ResumeActor"); got != want {
		t.Errorf("ResumeActor count = %d, want %d (no resume from a stranded actor); calls = %v", got, want, after)
	}
	if got := countCalls(after, "DeleteActor"); got != 0 {
		t.Errorf("DeleteActor count = %d, want 0 (Unavailable is not the actor's fault); calls = %v", got, after)
	}

	rt.iterate() // the suspend succeeded, so the normal cycle resumes
	final := fakeCtrl.recordedCalls()
	if got, want := countCalls(final, "ResumeActor"), countCalls(after, "ResumeActor")+1; got != want {
		t.Errorf("ResumeActor count = %d, want %d after the suspend cleared; calls = %v", got, want, final)
	}
}

func TestClassifyLifecycleFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want failureAction
	}{
		{"actor gone", status.Error(codes.NotFound, "Actor not found"), replaceNow},
		{"snapshot unreadable", status.Error(codes.DataLoss, "external snapshot"), replaceNow},
		{"stuck state", status.Error(codes.FailedPrecondition, "MarkSuspending prerequisite not met"), replaceNow},
		{"crashed", status.Error(codes.Aborted, "actor bench/sb-1 crashed"), replaceNow},
		{"no capacity", status.Error(codes.ResourceExhausted, "no free workers available"), retryLater},
		{"api server down", status.Error(codes.Unavailable, "connection refused"), retryLater},
		{"timeout", status.Error(codes.DeadlineExceeded, "context deadline exceeded"), retryLater},
		{"canceled", status.Error(codes.Canceled, "context canceled"), retryLater},
		{"misconfigured run", status.Error(codes.InvalidArgument, "bad template"), retryLater},
		{"unauthorized", status.Error(codes.PermissionDenied, "denied"), retryLater},
		{"update conflict", conflictErr(), replaceIfPersistent},
		{"wrapped atelet error", status.Error(codes.Unknown, "while checkpointing workload"), replaceIfPersistent},
		{"internal", status.Error(codes.Internal, "boom"), replaceIfPersistent},
		{"not a status", errors.New("plain error"), replaceIfPersistent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyLifecycleFailure(tc.err); got != tc.want {
				t.Errorf("classifyLifecycleFailure(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestNoteFailureResetsOnSuccess(t *testing.T) {
	a := &gluttonActor{actorName: "sb-1"}
	for range maxConsecutiveFailures - 1 {
		if a.noteFailure(conflictErr()) {
			t.Fatal("noteFailure = true before the threshold")
		}
	}
	a.noteSuccess()
	if a.noteFailure(conflictErr()) {
		t.Error("noteFailure = true right after a success; the count must reset")
	}
}

// retryLater must not push an actor toward the threshold, or a long capacity
// shortage would replace every actor at once.
func TestNoteFailureIgnoresRetryLater(t *testing.T) {
	a := &gluttonActor{actorName: "sb-1"}
	for range maxConsecutiveFailures * 2 {
		if a.noteFailure(status.Error(codes.Unavailable, "down")) {
			t.Fatal("noteFailure = true for a cluster-wide transient error")
		}
	}
	if a.consecutiveFailures != 0 {
		t.Errorf("consecutiveFailures = %d, want 0", a.consecutiveFailures)
	}
}
