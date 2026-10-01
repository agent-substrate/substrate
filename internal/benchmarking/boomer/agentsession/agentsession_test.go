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

package agentsession

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/agent-substrate/substrate/internal/benchmarking/glutton/fake"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestSessionScriptIsWellFormed pins the script's invariants: unique step
// names, positive think times, and no op that consumes a sandbox object
// before an earlier step created it. A broken ordering would fail at run
// time with NotFound from glutton; this catches it at test time.
func TestSessionScriptIsWellFormed(t *testing.T) {
	steps := Session()
	if len(steps) != 20 {
		t.Fatalf("Session has %d steps, want 20", len(steps))
	}

	seen := map[string]bool{}
	ramFilled := map[string]bool{}
	diskWritten := map[string]bool{}

	for i, s := range steps {
		if s.Name == "" || s.Agent == "" {
			t.Errorf("step %d: Name and Agent must be set", i)
		}
		if seen[s.Name] {
			t.Errorf("step %q: duplicate name", s.Name)
		}
		seen[s.Name] = true
		if s.Think <= 0 {
			t.Errorf("step %q: think time must be positive", s.Name)
		}
		if len(s.Ops) == 0 {
			t.Errorf("step %q: has no ops", s.Name)
		}
		for _, o := range s.Ops {
			switch o.kind {
			case opFillRAM:
				ramFilled[o.key] = true
			case opChurnRAM, opWalkRAM:
				if !ramFilled[o.key] {
					t.Errorf("step %q: %s RAM op before any fill of %q", s.Name, opName(o.kind), o.key)
				}
			case opIngest, opWriteDisk:
				diskWritten[o.key] = true
			case opReadDiskDigest, opReadDiskData:
				if !diskWritten[o.key] {
					t.Errorf("step %q: read of %q before any write", s.Name, o.key)
				}
			}
		}
	}
}

func opName(k opKind) string {
	switch k {
	case opChurnRAM:
		return "churn"
	case opWalkRAM:
		return "walk"
	default:
		return "op"
	}
}

// TestSessionBudgets bounds the bytes the script itself declares: resident
// RAM (largest fill per key) and disk (largest object per key). It does NOT
// model the guest's real peak — kernel, kata-agent, and allocator transients
// sit on top — so the budgets are deliberately far below the 1Gi the
// template calls for. A script edit that outgrows them must come with a
// fresh look at the actor memory guidance.
func TestSessionBudgets(t *testing.T) {
	const (
		ramBudget  = 128 << 20 // bytes
		diskBudget = 256 << 20
	)
	ramMax := map[string]int64{}
	diskMax := map[string]int64{}
	for _, s := range Session() {
		for _, o := range s.Ops {
			switch o.kind {
			case opFillRAM:
				if o.bytes > ramMax[o.key] {
					ramMax[o.key] = o.bytes
				}
			case opIngest, opWriteDisk:
				if o.bytes > diskMax[o.key] {
					diskMax[o.key] = o.bytes
				}
			}
		}
	}
	var ramTotal, diskTotal int64
	for _, v := range ramMax {
		ramTotal += v
	}
	for _, v := range diskMax {
		diskTotal += v
	}
	if ramTotal > ramBudget {
		t.Errorf("script fills %d bytes of RAM, budget %d", ramTotal, ramBudget)
	}
	if diskTotal > diskBudget {
		t.Errorf("script writes %d bytes of disk, budget %d", diskTotal, diskBudget)
	}
}

// TestExecOpAgainstFake replays every scripted op against the fake glutton
// server, proving each op marshals a request the actor-side routes accept.
func TestExecOpAgainstFake(t *testing.T) {
	fakeSrv := &fake.Server{Data: []byte("filecontents")}
	ts := fakeSrv.Start(t)

	u := &sessionUser{
		cfg: &userclass.Config{
			HTTPClient: http.DefaultClient,
			RouterURL:  ts.URL,
			Atespace:   "benchmark",
			Dyn:        dynconfig.NewHolder(dynconfig.Config{}),
		},
		actorName: "agent-test",
	}

	var opCount int
	for _, s := range Session() {
		for i, o := range s.Ops {
			// Cap ingest payloads in the unit test: transport shape is what
			// matters here, not moving tens of MiB through httptest.
			if o.kind == opIngest && o.bytes > 1<<10 {
				o.bytes = 1 << 10
			}
			if o.kind == opBurnCPU {
				o.millis = 1
			}
			if err := u.execOp(context.Background(), o); err != nil {
				t.Fatalf("step %q op %d: %v", s.Name, i, err)
			}
			opCount++
		}
	}
	if got := len(fakeSrv.RecordedPaths()); got != opCount {
		t.Errorf("fake served %d requests, want %d", got, opCount)
	}
	for _, n := range fakeSrv.RecordedIngestSizes() {
		if n > 1<<10 {
			t.Errorf("ingest payload of %d bytes reached the server, cap is %d", n, 1<<10)
		}
	}
	for _, ms := range fakeSrv.RecordedBurnMillis() {
		if ms != 1 {
			t.Errorf("burn of %dms reached the server, override is 1ms", ms)
		}
	}
}

// TestThinkScaling checks the think-time multiplier and its jitter bounds.
func TestThinkScaling(t *testing.T) {
	r := &runtime{
		cfg: &userclass.Config{
			Dyn: dynconfig.NewHolder(dynconfig.Config{AgentSessionThinkScale: 0.5}),
		},
	}
	s := Step{Think: 10 * time.Second}
	for i := 0; i < 100; i++ {
		got := r.think(s)
		if got < 4*time.Second || got > 6*time.Second {
			t.Fatalf("think = %v, want within [4s, 6s] (scale 0.5, jitter ±20%%)", got)
		}
	}

	// Zero scale reads as 1.0.
	r.cfg.Dyn.Store(dynconfig.Config{})
	for i := 0; i < 100; i++ {
		got := r.think(s)
		if got < 8*time.Second || got > 12*time.Second {
			t.Fatalf("think = %v with unset scale, want within [8s, 12s]", got)
		}
	}
}

// fakeControlClient records control-plane calls and fails ResumeActor /
// SuspendActor with queued errors, in order, until each queue drains.
type fakeControlClient struct {
	ateapipb.ControlClient
	mu          sync.Mutex
	calls       []string
	resumeErrs  []error
	suspendErrs []error
	// sawDeadline is set when a call arrived with a context deadline.
	sawDeadline bool
}

func nextErr(errs *[]error) error {
	if len(*errs) == 0 {
		return nil
	}
	err := (*errs)[0]
	*errs = (*errs)[1:]
	return err
}

func (f *fakeControlClient) record(ctx context.Context, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, name)
	if _, ok := ctx.Deadline(); ok {
		f.sawDeadline = true
	}
}

func (f *fakeControlClient) ResumeActor(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	f.record(ctx, "ResumeActor")
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := nextErr(&f.resumeErrs); err != nil {
		return nil, err
	}
	return &ateapipb.ResumeActorResponse{}, nil
}

func (f *fakeControlClient) SuspendActor(ctx context.Context, in *ateapipb.SuspendActorRequest, opts ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	f.record(ctx, "SuspendActor")
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := nextErr(&f.suspendErrs); err != nil {
		return nil, err
	}
	return &ateapipb.SuspendActorResponse{}, nil
}

func (f *fakeControlClient) PauseActor(ctx context.Context, in *ateapipb.PauseActorRequest, opts ...grpc.CallOption) (*ateapipb.PauseActorResponse, error) {
	f.record(ctx, "PauseActor")
	return &ateapipb.PauseActorResponse{}, nil
}

func (f *fakeControlClient) DeleteActor(ctx context.Context, in *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.record(ctx, "DeleteActor")
	return &ateapipb.Actor{}, nil
}

func (f *fakeControlClient) recordedCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
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

func newTestUser(t *testing.T, srv *fake.Server, ctl *fakeControlClient, dyn dynconfig.Config) *sessionUser {
	t.Helper()
	ts := srv.Start(t)
	return &sessionUser{
		cfg: &userclass.Config{
			APIStub:    ctl,
			HTTPClient: ts.Client(),
			RouterURL:  ts.URL,
			Atespace:   "benchmark",
			Dyn:        dynconfig.NewHolder(dyn),
			Tracer:     otel.Tracer("test"),
		},
		actorName: "agent-test",
	}
}

var pingStep = Step{Name: "01_test", Agent: "testing", Think: time.Second, Ops: []op{ping()}}

// A retryable suspend failure strands the actor RUNNING. The next step must
// finish the suspend rather than wake an already-awake actor, which would
// book a few-ms WakeFirstTouch success.
func TestRunStep_RetriesStrandedHibernate(t *testing.T) {
	srv := &fake.Server{}
	ctl := &fakeControlClient{suspendErrs: []error{status.Error(codes.Unavailable, "ate-api-server restarting")}}
	u := newTestUser(t, srv, ctl, dynconfig.Config{})

	if !u.runStep(context.Background(), pingStep) {
		t.Fatal("runStep = false; the step's ops succeeded and must count")
	}
	if !u.hibernatePending || u.broken {
		t.Fatalf("after failed suspend: hibernatePending=%v broken=%v, want true/false", u.hibernatePending, u.broken)
	}
	served := len(srv.RecordedPaths())

	if u.runStep(context.Background(), pingStep) {
		t.Error("runStep = true while re-driving the hibernate; the step must not advance")
	}
	calls := ctl.recordedCalls()
	if got := calls[len(calls)-1]; got != "SuspendActor" {
		t.Errorf("last call = %q, want SuspendActor; calls = %v", got, calls)
	}
	if got := len(srv.RecordedPaths()); got != served {
		t.Errorf("router requests = %d, want %d (no wake ping against a stranded actor)", got, served)
	}
	if u.hibernatePending || u.broken {
		t.Errorf("after re-driven suspend: hibernatePending=%v broken=%v, want false/false", u.hibernatePending, u.broken)
	}

	if !u.runStep(context.Background(), pingStep) {
		t.Error("runStep = false once the suspend cleared")
	}
}

// ateapi reports a CRASHED actor on ResumeActor; it never recovers, so the
// session must replace it on the first failure, not the third.
func TestRunStep_ReplacesCrashedActorImmediately(t *testing.T) {
	ctl := &fakeControlClient{resumeErrs: []error{status.Error(codes.Aborted, "actor benchmark/agent-test crashed")}}
	u := newTestUser(t, &fake.Server{}, ctl, dynconfig.Config{ResumeMode: dynconfig.ResumeModeExplicit})

	if u.runStep(context.Background(), pingStep) {
		t.Fatal("runStep = true with a crashed actor")
	}
	if !u.broken {
		t.Error("broken = false after a crashed verdict; want immediate replacement")
	}
}

// In implicit mode the wake is an HTTP request, so the gRPC verdict arrives
// from the hibernate that follows the failed step: FailedPrecondition means
// a state nothing the driver can call moves the actor out of.
func TestRunStep_ReplacesStuckActorAfterFailedWake(t *testing.T) {
	ctl := &fakeControlClient{suspendErrs: []error{status.Error(codes.FailedPrecondition, "MarkSuspending prerequisite not met (got: ACTOR_STATE_CRASHED)")}}
	u := newTestUser(t, &fake.Server{Status: 503}, ctl, dynconfig.Config{})

	if u.runStep(context.Background(), pingStep) {
		t.Fatal("runStep = true with a failing router")
	}
	if !u.broken {
		t.Error("broken = false after FailedPrecondition on suspend; want immediate replacement")
	}
}

// A saturated pool reports ResourceExhausted to every VU at once; a
// replacement would need the same capacity, so the actor must be kept.
func TestRunStep_KeepsActorThroughCapacityShortage(t *testing.T) {
	const rounds = maxConsecutiveStepFailures + 2
	errs := make([]error, rounds)
	for i := range errs {
		errs[i] = status.Error(codes.ResourceExhausted, "no free workers available")
	}
	ctl := &fakeControlClient{resumeErrs: errs}
	u := newTestUser(t, &fake.Server{}, ctl, dynconfig.Config{ResumeMode: dynconfig.ResumeModeExplicit})

	for range rounds {
		if u.runStep(context.Background(), pingStep) {
			t.Fatal("runStep = true while resume is failing")
		}
	}
	if u.broken || u.consecutiveFailures != 0 {
		t.Errorf("broken=%v consecutiveFailures=%d after capacity errors, want false/0", u.broken, u.consecutiveFailures)
	}
}

// HTTP failures carry no gRPC code, so they count toward the threshold: an
// actor whose sandbox is dead but whose record looks healthy is replaced
// after maxConsecutiveStepFailures steps.
func TestRunStep_ReplacesActorAfterRepeatedStepFailures(t *testing.T) {
	ctl := &fakeControlClient{}
	u := newTestUser(t, &fake.Server{Status: 502}, ctl, dynconfig.Config{})

	for i := 1; i <= maxConsecutiveStepFailures; i++ {
		u.runStep(context.Background(), pingStep)
		if want := i == maxConsecutiveStepFailures; u.broken != want {
			t.Fatalf("after %d failed steps: broken = %v, want %v", i, u.broken, want)
		}
	}
}

func TestControlRPCsCarryADeadline(t *testing.T) {
	ctl := &fakeControlClient{}
	u := newTestUser(t, &fake.Server{}, ctl, dynconfig.Config{ResumeMode: dynconfig.ResumeModeExplicit})

	u.runStep(context.Background(), pingStep)
	u.suspendAndDelete(context.Background())

	if !ctl.sawDeadline {
		t.Error("control-plane calls arrived without a context deadline")
	}
	for _, name := range []string{"ResumeActor", "SuspendActor", "DeleteActor"} {
		if got := countCalls(ctl.recordedCalls(), name); got == 0 {
			t.Errorf("%s was never called; calls = %v", name, ctl.recordedCalls())
		}
	}
}

func TestBurnRatePerGoroutine(t *testing.T) {
	rate, ok := burnRatePerGoroutine(burn(500, 2), 10_000)
	if !ok || rate != 10_000 {
		t.Errorf("burnRatePerGoroutine(500ms x2, 10k) = %v, %v; want 10000, true", rate, ok)
	}
	if _, ok := burnRatePerGoroutine(burn(0, 1), 5); ok {
		t.Error("a zero-duration burn must not report a rate")
	}
}
