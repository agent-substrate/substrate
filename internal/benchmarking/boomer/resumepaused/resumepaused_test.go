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

package resumepaused

import (
	"context"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.ate.dev/substrate/internal/benchmarking/boomer/dynconfig"
	"go.ate.dev/substrate/internal/benchmarking/boomer/userclass"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeControlClient keeps each actor's state, so a resume of a running actor
// reports Resumed false the way ateapi does.
type fakeControlClient struct {
	ateapipb.ControlClient
	mu     sync.Mutex
	calls  []string // "<RPC> <actor>"
	states map[string]ateapipb.ActorState
	// resumeErr, when set, fails every ResumeActor without changing state.
	resumeErr error
	// resumeFailures is how many ResumeActor calls fail with Unavailable,
	// without changing state, before resumes succeed.
	resumeFailures int
	// pauseErr, when set, fails every async SuspendActor without changing state.
	pauseErr error
	// atespaceErr, when set, fails CreateAtespace.
	atespaceErr error
	// createErr, when set, fails every CreateActor without creating it.
	createErr error
	// deleteAborts is how many DeleteActor calls fail with Aborted, as when
	// another operation holds the actor lease, before deletes succeed.
	deleteAborts int
	// resumeEntered and resumeGate, when set, hold every ResumeActor: it
	// signals resumeEntered and then waits for resumeGate to close or its
	// context to end.
	resumeEntered chan struct{}
	resumeGate    chan struct{}
}

func newFake() *fakeControlClient {
	return &fakeControlClient{states: map[string]ateapipb.ActorState{}}
}

func (f *fakeControlClient) record(rpc, actor string) {
	f.calls = append(f.calls, rpc+" "+actor)
}

func (f *fakeControlClient) setState(name string, s ateapipb.ActorState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states[name] = s
}

func (f *fakeControlClient) state(name string) ateapipb.ActorState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.states[name]
}

func (f *fakeControlClient) CreateAtespace(ctx context.Context, in *ateapipb.CreateAtespaceRequest, opts ...grpc.CallOption) (*ateapipb.Atespace, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("CreateAtespace", in.GetAtespace().GetMetadata().GetName())
	if f.atespaceErr != nil {
		return nil, f.atespaceErr
	}
	return &ateapipb.Atespace{}, nil
}

func (f *fakeControlClient) CreateActor(ctx context.Context, in *ateapipb.CreateActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetMetadata().GetName()
	f.record("CreateActor", name)
	if f.createErr != nil {
		return nil, f.createErr
	}
	f.states[name] = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	return &ateapipb.Actor{}, nil
}

func (f *fakeControlClient) GetActor(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetName()
	f.record("GetActor", name)
	s, ok := f.states[name]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such actor")
	}
	return &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: s}}, nil
}

func (f *fakeControlClient) ResumeActor(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	if f.resumeGate != nil {
		f.resumeEntered <- struct{}{}
		select {
		case <-f.resumeGate:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetName()
	f.record("ResumeActor", name)
	if f.resumeErr != nil {
		return nil, f.resumeErr
	}
	if f.resumeFailures > 0 {
		f.resumeFailures--
		return nil, status.Error(codes.Unavailable, "down")
	}
	switch f.states[name] {
	case ateapipb.ActorState_ACTOR_STATE_RUNNING:
		return &ateapipb.ResumeActorResponse{Resumed: false}, nil
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
		f.states[name] = ateapipb.ActorState_ACTOR_STATE_RUNNING
		return &ateapipb.ResumeActorResponse{Resumed: true}, nil
	}
	return nil, status.Error(codes.Aborted, "actor crashed")
}

// SuspendActor fakes only an async suspend, which is how the user pauses.
func (f *fakeControlClient) SuspendActor(ctx context.Context, in *ateapipb.SuspendActorRequest, opts ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetName()
	f.record("SuspendActor(async)", name)
	if in.GetOptions().GetMode() != ateapipb.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS {
		return nil, status.Error(codes.Unimplemented, "only async suspend is faked")
	}
	if f.pauseErr != nil {
		return nil, f.pauseErr
	}
	if f.states[name] != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		return nil, status.Error(codes.FailedPrecondition, "not running")
	}
	// A paused actor is SUSPENDED holding a node-local snapshot; the fake
	// tracks only the state.
	f.states[name] = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	return &ateapipb.SuspendActorResponse{}, nil
}

func (f *fakeControlClient) DeleteActor(ctx context.Context, in *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetName()
	f.record("DeleteActor", name)
	if f.deleteAborts > 0 {
		f.deleteAborts--
		return nil, status.Error(codes.Aborted, "lease held")
	}
	delete(f.states, name)
	return &ateapipb.Actor{}, nil
}

// callsTo returns the actors of every recorded call to rpc, in order.
func (f *fakeControlClient) callsTo(rpc string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, c := range f.calls {
		if r, actor, _ := strings.Cut(c, " "); r == rpc {
			out = append(out, actor)
		}
	}
	return out
}

func (f *fakeControlClient) resetCalls() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
}

func newTestRuntime(fake *fakeControlClient) *runtime {
	r := newRuntime(&userclass.Config{
		APIStub:  fake,
		Atespace: "bench",
		Dyn:      dynconfig.Static(knobs{}),
		Tracer:   otel.Tracer("test"),
	})
	r.retryBackoff = time.Millisecond
	r.setupRetryWait = 0
	return r
}

// setUpUsers runs the first iteration of n users, each on its own goroutine
// as boomer runs them, and returns their actors sorted.
func setUpUsers(t *testing.T, r *runtime, n int) []string {
	t.Helper()
	var wg sync.WaitGroup
	for range n {
		wg.Go(r.iterate)
	}
	wg.Wait()
	got := userActors(r)
	if len(got) != n {
		t.Fatalf("set up %d users, got actors %v", n, got)
	}
	return got
}

// userActors returns the actor of every user, sorted.
func userActors(r *runtime) []string {
	var out []string
	r.users.Range(func(_, v any) bool {
		out = append(out, v.(string))
		return true
	})
	slices.Sort(out)
	return out
}

// trackedActors returns every actor shutdown would delete, sorted.
func trackedActors(r *runtime) []string {
	r.actorsMu.Lock()
	defer r.actorsMu.Unlock()
	return slices.Sorted(maps.Keys(r.actors))
}

func TestRegistered(t *testing.T) {
	e, ok := userclass.Lookup("resumepaused")
	if !ok {
		t.Fatal("resumepaused is not registered")
	}
	if e.LocustFile != "resumepaused.py" || e.UserClass != userClass {
		t.Errorf("got entry %+v", e)
	}
}

func TestStartEnsuresTheAtespace(t *testing.T) {
	fake := newFake()
	r := newTestRuntime(fake)
	if err := r.start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := fake.callsTo("CreateAtespace"); !slices.Equal(got, []string{"bench"}) {
		t.Errorf("CreateAtespace calls: got %v", got)
	}
	if got := fake.callsTo("CreateActor"); len(got) != 0 {
		t.Errorf("start created actors %v, want none before a user runs", got)
	}
}

func TestStartFailsWhenTheAtespaceCannotBeEnsured(t *testing.T) {
	fake := newFake()
	fake.atespaceErr = status.Error(codes.PermissionDenied, "denied")
	r := newTestRuntime(fake)
	if err := r.start(context.Background()); err == nil {
		t.Fatal("start succeeded without an atespace")
	}
}

func TestFirstIterationMakesTheUsersActorCold(t *testing.T) {
	fake := newFake()
	r := newTestRuntime(fake)
	cold := requestCount(t, "ResumeActorCold", "success")

	r.iterate()
	actors := userActors(r)
	if len(actors) != 1 {
		t.Fatalf("got user actors %v, want 1", actors)
	}
	name := actors[0]
	want := []string{"CreateActor " + name, "ResumeActor " + name, "SuspendActor(async) " + name}
	if !slices.Equal(fake.calls, want) {
		t.Errorf("calls: got %v, want %v", fake.calls, want)
	}
	if got := fake.state(name); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("actor is %v, want paused (SUSPENDED)", got)
	}
	if got := trackedActors(r); !slices.Equal(got, actors) {
		t.Errorf("tracked %v, want %v", got, actors)
	}
	if got := requestCount(t, "ResumeActorCold", "success") - cold; got != 0 {
		t.Errorf("ResumeActorCold successes grew by %v during setup, want 0", got)
	}
}

func TestIterateResumesThenPausesTheUsersActor(t *testing.T) {
	fake := newFake()
	r := newTestRuntime(fake)
	r.iterate()
	name := userActors(r)[0]
	fake.resetCalls()
	before := requestCount(t, "ResumeActorCold", "success")

	for range 3 {
		r.iterate()
	}
	want := []string{"ResumeActor " + name, "SuspendActor(async) " + name}
	want = slices.Concat(want, want, want)
	if !slices.Equal(fake.calls, want) {
		t.Errorf("calls: got %v, want %v", fake.calls, want)
	}
	if got := requestCount(t, "ResumeActorCold", "success") - before; got != 3 {
		t.Errorf("ResumeActorCold successes grew by %v, want 3", got)
	}
}

func TestEachUserCyclesItsOwnActor(t *testing.T) {
	fake := newFake()
	r := newTestRuntime(fake)
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			for range 4 {
				r.iterate()
			}
		})
	}
	wg.Wait()

	actors := userActors(r)
	if len(actors) != 3 {
		t.Fatalf("got user actors %v, want 3", actors)
	}
	if got := fake.callsTo("CreateActor"); len(got) != 3 {
		t.Errorf("CreateActor calls = %v, want one per user", got)
	}
	// Each user warms its actor once, then cycles it three times.
	resumes := map[string]int{}
	for _, name := range fake.callsTo("ResumeActor") {
		resumes[name]++
	}
	for _, name := range actors {
		if resumes[name] != 4 {
			t.Errorf("actor %s resumed %d times, want 4", name, resumes[name])
		}
	}
}

func TestSetupIsNotPaced(t *testing.T) {
	fake := newFake()
	r := newTestRuntime(fake)
	r.cfg.Dyn = dynconfig.Static(knobs{TargetRPS: 1})
	start := time.Now()
	setUpUsers(t, r, 5)
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("setting up 5 users at 1 resume per second took %v, want well under 1s", elapsed)
	}
}

func TestSetupDeletesAnActorThatDoesNotWarmAndRetries(t *testing.T) {
	fake := newFake()
	fake.resumeFailures = 1
	r := newTestRuntime(fake)

	r.iterate()
	if got := userActors(r); len(got) != 0 {
		t.Fatalf("user has actor %v after a failed warm-up", got)
	}
	created, deleted := fake.callsTo("CreateActor"), fake.callsTo("DeleteActor")
	if len(created) != 1 || !slices.Equal(deleted, created) {
		t.Errorf("created %v, deleted %v, want the one actor deleted", created, deleted)
	}
	if got := trackedActors(r); len(got) != 0 {
		t.Errorf("tracked %v, want none", got)
	}

	r.iterate()
	actors := userActors(r)
	if len(actors) != 1 || actors[0] == created[0] {
		t.Fatalf("got user actors %v, want a new one", actors)
	}
	if got := fake.state(actors[0]); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("actor is %v, want paused (SUSPENDED)", got)
	}
}

func TestSetupKeepsTrackOfAnActorItCouldNotDelete(t *testing.T) {
	fake := newFake()
	fake.resumeErr = status.Error(codes.Unavailable, "down")
	fake.deleteAborts = 5
	r := newTestRuntime(fake)

	r.iterate()
	created := fake.callsTo("CreateActor")
	if got := trackedActors(r); !slices.Equal(got, created) {
		t.Errorf("tracked %v, want %v for shutdown to delete", got, created)
	}
}

func TestSetupRetriesAFailedCreate(t *testing.T) {
	fake := newFake()
	fake.createErr = status.Error(codes.Unavailable, "down")
	r := newTestRuntime(fake)

	r.iterate()
	if got := fake.callsTo("DeleteActor"); len(got) != 0 {
		t.Errorf("deleted %v, want nothing after a failed create", got)
	}
	if got := trackedActors(r); len(got) != 0 {
		t.Errorf("tracked %v, want none", got)
	}

	fake.createErr = nil
	r.iterate()
	if got := userActors(r); len(got) != 1 {
		t.Errorf("got user actors %v, want 1", got)
	}
}

func TestIterateFailsAResumeOfARunningActor(t *testing.T) {
	fake := newFake()
	r := newTestRuntime(fake)
	r.iterate()
	name := userActors(r)[0]
	fake.setState(name, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	fake.resetCalls()
	before := requestCount(t, "ResumeActorCold", "failure")
	recoveries := requestCount(t, "SuspendActorAsyncRecovery", "success")

	r.iterate()
	if got := requestCount(t, "ResumeActorCold", "failure") - before; got != 1 {
		t.Errorf("ResumeActorCold failures grew by %v, want 1", got)
	}
	if got := requestCount(t, "SuspendActorAsyncRecovery", "success") - recoveries; got != 1 {
		t.Errorf("SuspendActorAsyncRecovery successes grew by %v, want 1", got)
	}
	// Recovery pauses the running actor.
	want := []string{"ResumeActor " + name, "SuspendActor(async) " + name}
	if !slices.Equal(fake.calls, want) {
		t.Errorf("calls: got %v, want %v", fake.calls, want)
	}
	if got := fake.state(name); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("actor is %v, want paused (SUSPENDED)", got)
	}
}

func TestIterateKeepsTheActorAfterAFailedResume(t *testing.T) {
	fake := newFake()
	r := newTestRuntime(fake)
	r.iterate()
	name := userActors(r)[0]
	fake.resumeErr = status.Error(codes.ResourceExhausted, "no free workers available")

	r.iterate()
	if got := userActors(r); !slices.Equal(got, []string{name}) {
		t.Errorf("user actors: got %v, want [%s]", got, name)
	}
	if got := fake.state(name); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("actor is %v, want paused (SUSPENDED)", got)
	}
}

func TestIteratePacesResumes(t *testing.T) {
	fake := newFake()
	r := newTestRuntime(fake)
	r.iterate()

	r.cfg.Dyn = dynconfig.Static(knobs{TargetRPS: 200})
	start := time.Now()
	for range 21 {
		r.iterate()
	}
	// The first resume goes at once; the other 20 take 5 ms each.
	if elapsed := time.Since(start); elapsed < 95*time.Millisecond {
		t.Errorf("21 resumes at 200 per second took %v, want >= 100ms", elapsed)
	}
}

func TestIterateRecordsTheRecoveryPauseOfAPausedActorAsASuccess(t *testing.T) {
	fake := newFake()
	r := newTestRuntime(fake)
	r.iterate()
	name := userActors(r)[0]
	fake.resetCalls()
	fake.resumeErr = status.Error(codes.Aborted, "lease held")
	resumeFailures := requestCount(t, "ResumeActorCold", "failure")
	pauseFailures := requestCount(t, "SuspendActorAsync", "failure")
	recoverySuccesses := requestCount(t, "SuspendActorAsyncRecovery", "success")
	recoveryFailures := requestCount(t, "SuspendActorAsyncRecovery", "failure")

	// The resume fails with the actor still paused, so the recovery pause
	// finds it paused: a success, as the actor is cold.
	r.iterate()
	if got := fake.callsTo("SuspendActor(async)"); !slices.Equal(got, []string{name}) {
		t.Errorf("async SuspendActor calls = %v, want the recovery pause of %s", got, name)
	}
	if got := requestCount(t, "ResumeActorCold", "failure") - resumeFailures; got != 1 {
		t.Errorf("ResumeActorCold failures grew by %v, want 1", got)
	}
	if got := requestCount(t, "SuspendActorAsync", "failure") - pauseFailures; got != 0 {
		t.Errorf("SuspendActorAsync failures grew by %v, want 0", got)
	}
	if got := requestCount(t, "SuspendActorAsyncRecovery", "success") - recoverySuccesses; got != 1 {
		t.Errorf("SuspendActorAsyncRecovery successes grew by %v, want 1", got)
	}
	if got := requestCount(t, "SuspendActorAsyncRecovery", "failure") - recoveryFailures; got != 0 {
		t.Errorf("SuspendActorAsyncRecovery failures grew by %v, want 0", got)
	}
}

func TestIterateRecordsAFailedRecoveryPause(t *testing.T) {
	fake := newFake()
	r := newTestRuntime(fake)
	r.iterate()
	fake.pauseErr = status.Error(codes.Unavailable, "down")
	pauseFailures := requestCount(t, "SuspendActorAsync", "failure")
	recoveryFailures := requestCount(t, "SuspendActorAsyncRecovery", "failure")

	// The resume succeeds, the measured pause fails, and so does the
	// recovery pause of the running actor.
	r.iterate()
	if got := requestCount(t, "SuspendActorAsync", "failure") - pauseFailures; got != 1 {
		t.Errorf("SuspendActorAsync failures grew by %v, want 1", got)
	}
	if got := requestCount(t, "SuspendActorAsyncRecovery", "failure") - recoveryFailures; got != 1 {
		t.Errorf("SuspendActorAsyncRecovery failures grew by %v, want 1", got)
	}
}

func TestShutdownDeletesEveryActor(t *testing.T) {
	fake := newFake()
	r := newTestRuntime(fake)
	want := setUpUsers(t, r, 3)
	r.shutdown(context.Background())

	got := fake.callsTo("DeleteActor")
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("deleted %v, want %v", got, want)
	}
}

func TestShutdownRetriesAnAbortedDelete(t *testing.T) {
	fake := newFake()
	r := newTestRuntime(fake)
	name := setUpUsers(t, r, 1)[0]
	fake.deleteAborts = 2
	r.shutdown(context.Background())

	if got := fake.callsTo("DeleteActor"); len(got) != 3 {
		t.Errorf("DeleteActor calls = %v, want 3 calls", got)
	}
	if _, err := fake.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: r.ref(name)}); status.Code(err) != codes.NotFound {
		t.Errorf("actor still exists after shutdown: err = %v", err)
	}
}

func TestShutdownWaitsForARunningCycle(t *testing.T) {
	fake := newFake()
	r := newTestRuntime(fake)
	r.iterate()
	name := userActors(r)[0]
	fake.resumeEntered = make(chan struct{})
	fake.resumeGate = make(chan struct{})
	cycleDone := make(chan struct{})
	go func() {
		r.cycle(name)
		close(cycleDone)
	}()
	<-fake.resumeEntered

	shutdownDone := make(chan struct{})
	go func() {
		r.shutdown(context.Background())
		close(shutdownDone)
	}()
	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned while a cycle was running")
	case <-time.After(20 * time.Millisecond):
	}
	if got := fake.callsTo("DeleteActor"); len(got) != 0 {
		t.Errorf("deleted %v while a cycle was running", got)
	}

	close(fake.resumeGate)
	<-cycleDone
	<-shutdownDone
	if got := fake.callsTo("DeleteActor"); !slices.Equal(got, []string{name}) {
		t.Errorf("deleted %v, want [%s]", got, name)
	}
	r.iterate()
	if got := fake.callsTo("ResumeActor"); len(got) != 2 {
		t.Errorf("ResumeActor calls = %v, want the warm-up and the one cycle only", got)
	}
}

func TestShutdownWaitsForARunningSetupAndDeletesItsActor(t *testing.T) {
	fake := newFake()
	r := newTestRuntime(fake)
	fake.resumeEntered = make(chan struct{})
	fake.resumeGate = make(chan struct{})
	setupDone := make(chan struct{})
	go func() {
		r.iterate()
		close(setupDone)
	}()
	<-fake.resumeEntered

	shutdownDone := make(chan struct{})
	go func() {
		r.shutdown(context.Background())
		close(shutdownDone)
	}()
	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned while a setup was running")
	case <-time.After(20 * time.Millisecond):
	}

	close(fake.resumeGate)
	<-setupDone
	<-shutdownDone
	created := fake.callsTo("CreateActor")
	if got := fake.callsTo("DeleteActor"); len(created) != 1 || !slices.Equal(got, created) {
		t.Errorf("created %v, deleted %v, want the setup's actor deleted", created, got)
	}

	// No setup starts once shutdown has begun.
	r.iterate()
	if got := fake.callsTo("CreateActor"); len(got) != 1 {
		t.Errorf("CreateActor calls = %v after shutdown, want the one before", got)
	}
}

func TestShutdownRecordsOnlyToPrometheus(t *testing.T) {
	fake := newFake()
	r := newTestRuntime(fake)
	names := setUpUsers(t, r, 3)
	var toLocust atomic.Int32
	r.recordSuccess = func(string, string, string, time.Duration, int64) { toLocust.Add(1) }
	r.recordFailure = func(string, string, string, time.Duration, string) { toLocust.Add(1) }

	r.cycle(names[0])
	if got := toLocust.Load(); got != 2 {
		t.Fatalf("a cycle sent %d samples to locust, want the resume and the pause", got)
	}

	// A cycle that finishes once shutdown has begun, and the deletes, record
	// to Prometheus only: boomer may no longer drain its stats channel.
	fake.resumeEntered = make(chan struct{})
	fake.resumeGate = make(chan struct{})
	go r.cycle(names[0])
	<-fake.resumeEntered
	resumes := requestCount(t, "ResumeActorCold", "success")
	deletes := requestCount(t, "DeleteActor", "success")
	toLocust.Store(0)
	shutdownDone := make(chan struct{})
	go func() {
		r.shutdown(context.Background())
		close(shutdownDone)
	}()
	for !closing(r) {
		time.Sleep(time.Millisecond)
	}
	close(fake.resumeGate)
	<-shutdownDone

	if got := toLocust.Load(); got != 0 {
		t.Errorf("shutdown sent %d samples to locust, want 0", got)
	}
	if got := requestCount(t, "ResumeActorCold", "success") - resumes; got != 1 {
		t.Errorf("ResumeActorCold successes grew by %v, want the in-flight resume", got)
	}
	if got := requestCount(t, "DeleteActor", "success") - deletes; got != 3 {
		t.Errorf("DeleteActor successes grew by %v, want 3", got)
	}
}

func TestShutdownCancelsACycleThatOutlastsItsShare(t *testing.T) {
	fake := newFake()
	r := newTestRuntime(fake)
	r.iterate()
	name := userActors(r)[0]
	fake.resumeEntered = make(chan struct{})
	fake.resumeGate = make(chan struct{}) // never closed: the resume hangs
	cycleDone := make(chan struct{})
	go func() {
		r.cycle(name)
		close(cycleDone)
	}()
	<-fake.resumeEntered

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	r.shutdown(ctx)
	if ctx.Err() != nil {
		t.Fatal("shutdown spent its whole budget")
	}
	select {
	case <-cycleDone:
	case <-time.After(time.Second):
		t.Fatal("the hung cycle was not canceled")
	}
	if got := fake.callsTo("DeleteActor"); !slices.Equal(got, []string{name}) {
		t.Errorf("deleted %v, want [%s]", got, name)
	}
	if _, err := fake.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: r.ref(name)}); status.Code(err) != codes.NotFound {
		t.Errorf("actor still exists after shutdown: err = %v", err)
	}
}

// closing reports whether shutdown has begun.
func closing(r *runtime) bool {
	r.cycleMu.Lock()
	defer r.cycleMu.Unlock()
	return r.closing
}

// requestCount scrapes the default registry for the locust_requests_total
// of one request name and status, which is global to the test binary.
func requestCount(t *testing.T, name, result string) float64 {
	t.Helper()
	return scrape(t, `locust_requests_total{method="grpc",name="`+name+`",status="`+result+`",user_class="ResumePausedUser"} `)
}

func scrape(t *testing.T, prefix string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if v, ok := strings.CutPrefix(line, prefix); ok {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
			return f
		}
	}
	return 0
}
