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
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/benchmarking/boomer/boomerutil"
	"go.ate.dev/substrate/internal/benchmarking/boomer/dynconfig"
	"go.ate.dev/substrate/internal/benchmarking/glutton"
	"go.ate.dev/substrate/internal/benchmarking/glutton/fake"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var turnStep = Step{Name: pingStep.Name, Agent: pingStep.Agent, Think: time.Hour, Ops: pingStep.Ops}

func turnSessions(n int, awake bool) []*sessionUser {
	us := make([]*sessionUser, n)
	for i := range us {
		us[i] = &sessionUser{actorName: "agent-" + strconv.Itoa(i), awake: awake, cmdCh: make(chan turnCmd, 1)}
	}
	return us
}

func commands(us []*sessionUser) map[turnCmd][]*sessionUser {
	got := map[turnCmd][]*sessionUser{}
	for _, u := range us {
		select {
		case cmd := <-u.cmdCh:
			got[cmd] = append(got[cmd], u)
		default:
		}
	}
	return got
}

func finish(c *conductor, cmds map[turnCmd][]*sessionUser) {
	for _, u := range cmds[cmdPark] {
		u.awake = false
		c.settle(u)
	}
	for _, u := range cmds[cmdWake] {
		u.awake = true
		c.settle(u)
	}
}

func TestConductorFill(t *testing.T) {
	now := time.Unix(1000, 0)
	c := newConductor(func() time.Time { return now })
	fleet := turnSessions(20, false)
	for _, u := range fleet {
		c.join(u)
	}
	const awake = 10

	now = now.Add(registrationQuiet - time.Second)
	c.tick(awake, 5)
	if got := commands(fleet); len(got) != 0 {
		t.Fatalf("tick while registration was going on sent %d wakes and %d parks, want none",
			len(got[cmdWake]), len(got[cmdPark]))
	}

	now = now.Add(time.Second)
	for _, want := range []int{fillConcurrency, awake - fillConcurrency} {
		c.tick(awake, 5)
		got := commands(fleet)
		if len(got[cmdWake]) != want || len(got[cmdPark]) != 0 {
			t.Fatalf("fill tick sent %d wakes and %d parks, want %d wakes", len(got[cmdWake]), len(got[cmdPark]), want)
		}
		finish(c, got)
	}
	c.tick(awake, 5)
	if got := commands(fleet); len(got) != 0 || !c.filled {
		t.Fatalf("tick after %d sessions woke: %d wakes, %d parks, filled %v; want none and true",
			awake, len(got[cmdWake]), len(got[cmdPark]), c.filled)
	}

	c.tick(awake, 5)
	got := commands(fleet)
	if len(got[cmdPark]) != 5 || len(got[cmdWake]) != 5 {
		t.Fatalf("first swap tick sent %d parks and %d wakes, want 5 of each", len(got[cmdPark]), len(got[cmdWake]))
	}
	for _, u := range got[cmdPark] {
		if !u.awake {
			t.Errorf("parked session %s was not awake", u.actorName)
		}
	}
	for _, u := range got[cmdWake] {
		if u.awake {
			t.Errorf("woke session %s was already awake", u.actorName)
		}
	}
}

func TestConductorSwaps(t *testing.T) {
	c := newConductor(time.Now)
	c.filled = true
	fleet := append(turnSessions(4, true), turnSessions(4, false)...)
	for _, u := range fleet {
		c.settle(u)
	}
	stepping := fleet[0]
	if !c.claim(stepping) {
		t.Fatal("claim of an idle session failed")
	}
	for i, tc := range []struct {
		rate  float64
		swaps int
	}{
		{1.5, 1},
		{1.5, 2},
		{0, 0},
		{1, 1},
	} {
		c.tick(4, tc.rate)
		got := commands(fleet)
		if len(got[cmdPark]) != tc.swaps || len(got[cmdWake]) != tc.swaps {
			t.Fatalf("tick %d at rate %v sent %d parks and %d wakes, want %d of each", i, tc.rate, len(got[cmdPark]), len(got[cmdWake]), tc.swaps)
		}
		for _, u := range got[cmdPark] {
			if u == stepping {
				t.Fatalf("tick %d parked the session running a step", i)
			}
		}
		finish(c, got)
	}
}

func TestConductorRestoresAwakeCount(t *testing.T) {
	for _, tc := range []struct {
		name   string
		filled bool
		fail   turnCmd
	}{
		{"fill wake fails", false, cmdWake},
		{"swap wake fails", true, cmdWake},
		{"swap park fails", true, cmdPark},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(1000, 0)
			c := newConductor(func() time.Time { return now })
			fleet := turnSessions(12, false)
			for _, u := range fleet {
				c.join(u)
			}
			now = now.Add(registrationQuiet)
			const awake = 5
			c.tick(awake, 1)
			got := commands(fleet)
			if tc.filled {
				finish(c, got)
				c.tick(awake, 1) // ends the fill
				c.tick(awake, 1) // swaps one
				got = commands(fleet)
			}
			// A failed command leaves the session as it was, and a failed
			// park leaves it with a park to finish.
			failed := got[tc.fail][0]
			failed.hibernatePending = tc.fail == cmdPark
			c.settle(failed)
			got[tc.fail] = got[tc.fail][1:]
			finish(c, got)

			c.tick(awake, 0)
			got = commands(fleet)
			if len(got) != 1 || len(got[tc.fail]) != 1 {
				t.Fatalf("tick after the failure sent %d wakes and %d parks, want one more of the failed command",
					len(got[cmdWake]), len(got[cmdPark]))
			}
			if tc.fail == cmdPark && got[cmdPark][0] != failed {
				t.Errorf("the surplus park went to %s, want %s, whose park failed", got[cmdPark][0].actorName, failed.actorName)
			}
		})
	}
}

func TestConductorHalt(t *testing.T) {
	for _, filled := range []bool{false, true} {
		now := time.Unix(1000, 0)
		c := newConductor(func() time.Time { return now })
		fleet := turnSessions(4, false)
		for _, u := range fleet {
			c.join(u)
		}
		now = now.Add(registrationQuiet)
		c.filled = filled
		c.halt()
		c.tick(2, 1)
		if got := commands(fleet); len(got) != 0 {
			t.Errorf("tick after halt with filled %v sent %d wakes and %d parks, want none",
				filled, len(got[cmdWake]), len(got[cmdPark]))
		}
	}
}

func TestTurnoverSessionCarriesOutCommands(t *testing.T) {
	ctl := &fakeControlClient{}
	dyn := knobs{Awake: 1, Lifecycle: dynconfig.Lifecycle{ResumeMode: dynconfig.ResumeModeExplicit, LifecycleMode: dynconfig.LifecycleModeAsync}}
	u := newTestUser(t, &fake.Server{}, ctl, dyn)
	u.steps = []Step{turnStep}
	rt := newRuntime(u.cfg)
	ctx := context.Background()

	rt.conductor.step(ctx, u, dyn)
	if !u.joined || len(rt.conductor.parked) != 1 {
		t.Fatalf("after the first iteration: joined %v, %d parked; want true, 1", u.joined, len(rt.conductor.parked))
	}

	rt.conductor.mu.Lock()
	rt.conductor.send(&rt.conductor.parked, cmdWake)
	rt.conductor.mu.Unlock()
	expectRows(t, "TimeToWake", 1, 0, func() { rt.conductor.step(ctx, u, dyn) })
	if !u.awake || countCalls(ctl.recordedCalls(), "ResumeActor") != 1 || len(rt.conductor.idle) != 1 {
		t.Fatalf("after the wake: awake %v, ResumeActor calls %d, %d idle; want true, 1, 1",
			u.awake, countCalls(ctl.recordedCalls(), "ResumeActor"), len(rt.conductor.idle))
	}

	rt.conductor.mu.Lock()
	rt.conductor.send(&rt.conductor.idle, cmdPark)
	rt.conductor.mu.Unlock()
	rt.conductor.step(ctx, u, dyn)
	reqs := ctl.recordedSuspendReqs()
	if u.awake || len(reqs) != 1 || len(rt.conductor.parked) != 1 {
		t.Fatalf("after the park: awake %v, %d SuspendActor calls, %d parked; want false, 1, 1", u.awake, len(reqs), len(rt.conductor.parked))
	}
	if got := reqs[0].GetOptions().GetMode(); got != ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS {
		t.Errorf("SuspendActor mode = %v, want asynchronous", got)
	}
}

const twoStepTurnScript = `
name: two-step
min_actor_memory: 64Mi
steps:
- name: 01_seed
  agent: builds initial state
  think: 1h
  ops:
  - fill_ram: {key: heap, size: 1Mi}
- name: 02_work
  agent: runs a turn
  think: 1h
  ops:
  - walk_ram: {key: heap}
`

func writeTurnScript(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "script.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestTurnoverRegistrationBuildsStateAndRunsNextStepOnWake(t *testing.T) {
	srv := &fake.Server{}
	ctl := &fakeControlClient{}
	dyn := knobs{
		Awake:      1,
		ScriptFile: writeTurnScript(t, twoStepTurnScript),
		Lifecycle:  dynconfig.Lifecycle{ResumeMode: dynconfig.ResumeModeExplicit, LifecycleMode: dynconfig.LifecycleModeAsync},
	}
	seed := newTestUser(t, srv, ctl, dyn)
	rt := newRuntime(seed.cfg)
	loaded, err := rt.loadScript()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	var u *sessionUser
	expectRows(t, "Step_01_seed", 1, 0, func() {
		u, err = rt.createSession(ctx, dyn, loaded)
	})
	if err != nil {
		t.Fatal(err)
	}
	if u.awake || u.stepIndex != 1 || u.stepDue {
		t.Fatalf("after createSession: awake %v, stepIndex %d, stepDue %v; want false, 1, false",
			u.awake, u.stepIndex, u.stepDue)
	}
	if countCalls(srv.RecordedPaths(), glutton.WriteRAMRoute) != 1 || countCalls(ctl.recordedCalls(), "SuspendActor") != 1 {
		t.Fatalf("after createSession: paths %v, calls %v; want WriteRAM before one SuspendActor",
			srv.RecordedPaths(), ctl.recordedCalls())
	}

	rt.conductor.step(ctx, u, dyn)
	rt.conductor.mu.Lock()
	rt.conductor.send(&rt.conductor.parked, cmdWake)
	rt.conductor.mu.Unlock()
	expectRows(t, "Step_02_work", 0, 0, func() { rt.conductor.step(ctx, u, dyn) })
	if !u.stepDue {
		t.Fatal("after wake: stepDue = false, want true")
	}

	expectRows(t, "Step_02_work", 1, 0, func() { rt.conductor.step(ctx, u, dyn) })
	if u.stepIndex != 0 || u.stepDue {
		t.Errorf("after due step: stepIndex %d, stepDue %v; want 0, false", u.stepIndex, u.stepDue)
	}
}

func TestTurnoverRegistrationCleansUpOnFailedInitialStep(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == glutton.WriteRAMRoute {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	t.Cleanup(ts.Close)
	ctl := &fakeControlClient{}
	dyn := knobs{
		Awake:      1,
		ScriptFile: writeTurnScript(t, twoStepTurnScript),
		Lifecycle:  dynconfig.Lifecycle{LifecycleMode: dynconfig.LifecycleModeAsync},
	}
	seed := newTestUser(t, &fake.Server{}, ctl, dyn)
	seed.cfg.RouterURL = ts.URL
	seed.cfg.HTTPClient = ts.Client()
	rt := newRuntime(seed.cfg)
	loaded, err := rt.loadScript()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.createSession(context.Background(), dyn, loaded); err == nil {
		t.Fatal("createSession succeeded despite a failing step 1")
	}
	if calls := ctl.recordedCalls(); countCalls(calls, "DeleteActor") != 1 {
		t.Errorf("calls = %v, want one DeleteActor", calls)
	}
}

func TestTurnoverBrokenSessionDoesNotJoin(t *testing.T) {
	ctl := &fakeControlClient{suspendErrs: []error{status.Error(codes.FailedPrecondition, "MarkSuspending prerequisite not met (got: ACTOR_STATE_CRASHED)")}}
	dyn := knobs{
		Awake:      1,
		ScriptFile: writeTurnScript(t, twoStepTurnScript),
		Lifecycle:  dynconfig.Lifecycle{LifecycleMode: dynconfig.LifecycleModeAsync},
	}
	u := newTestUser(t, &fake.Server{}, ctl, dyn)
	rt := newRuntime(u.cfg)

	rt.iterate()
	calls := ctl.recordedCalls()
	if len(rt.conductor.parked) != 0 {
		t.Errorf("%d sessions parked in the conductor, want 0", len(rt.conductor.parked))
	}
	if _, ok := rt.users.Load(boomerutil.GoroutineID()); ok {
		t.Error("the broken session stayed in users")
	}
	if countCalls(calls, "SuspendActor") != 2 || countCalls(calls, "DeleteActor") != 1 {
		t.Errorf("calls = %v, want two SuspendActor and one DeleteActor", calls)
	}
}

func TestTurnoverSessionParksAfterFailedWake(t *testing.T) {
	deadline := status.Error(codes.DeadlineExceeded, "context deadline exceeded")
	for _, tc := range []struct {
		name         string
		resumeErr    error
		suspendErrs  []error
		suspends     int
		parked, idle int
	}{
		{"no room", status.Error(codes.ResourceExhausted, "no worker has room for the actor"), nil, 0, 1, 0},
		{"crashed", status.Error(codes.Aborted, "actor benchmark/agent-test crashed"), nil, 0, 0, 0},
		{"deadline", deadline, nil, 1, 1, 0},
		{"deadline and failed park", deadline, []error{status.Error(codes.Unavailable, "ate-api-server restarting")}, 1, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctl := &fakeControlClient{resumeErrs: []error{tc.resumeErr}, suspendErrs: tc.suspendErrs}
			dyn := knobs{Awake: 1, Lifecycle: dynconfig.Lifecycle{ResumeMode: dynconfig.ResumeModeExplicit, LifecycleMode: dynconfig.LifecycleModeAsync}}
			u := newTestUser(t, &fake.Server{}, ctl, dyn)
			u.steps = []Step{turnStep}
			u.joined, u.cmdCh = true, make(chan turnCmd, 1)
			rt := newRuntime(u.cfg)
			rt.conductor.parkRetry = 0
			rt.conductor.settle(u)
			rt.conductor.mu.Lock()
			rt.conductor.send(&rt.conductor.parked, cmdWake)
			rt.conductor.mu.Unlock()
			ctx := context.Background()

			rt.conductor.step(ctx, u, dyn)
			suspends := countCalls(ctl.recordedCalls(), "SuspendActor")
			if suspends != tc.suspends || len(rt.conductor.parked) != tc.parked || len(rt.conductor.idle) != tc.idle {
				t.Fatalf("after the wake: %d SuspendActor calls, %d parked, %d idle; want %d, %d, %d",
					suspends, len(rt.conductor.parked), len(rt.conductor.idle), tc.suspends, tc.parked, tc.idle)
			}
			if tc.idle == 0 {
				return
			}
			rt.conductor.step(ctx, u, dyn)
			suspends = countCalls(ctl.recordedCalls(), "SuspendActor")
			if suspends != tc.suspends+1 || u.hibernatePending || len(rt.conductor.parked) != 1 || len(rt.conductor.idle) != 0 {
				t.Errorf("after the due step: %d SuspendActor calls, hibernatePending %v, %d parked, %d idle; want %d, false, 1, 0",
					suspends, u.hibernatePending, len(rt.conductor.parked), len(rt.conductor.idle), tc.suspends+1)
			}
		})
	}
}

func TestTurnoverSessionRunsDueSteps(t *testing.T) {
	ctl := &fakeControlClient{}
	dyn := knobs{Awake: 1, Lifecycle: dynconfig.Lifecycle{LifecycleMode: dynconfig.LifecycleModeAsync}}
	u := newTestUser(t, &fake.Server{}, ctl, dyn)
	u.steps = []Step{turnStep}
	u.awake, u.joined, u.cmdCh = true, true, make(chan turnCmd, 1)
	rt := newRuntime(u.cfg)
	rt.conductor.settle(u)
	ctx := context.Background()

	u.stepDue = true
	expectRows(t, "Step_"+turnStep.Name, 1, 0, func() { rt.conductor.step(ctx, u, dyn) })
	if !u.awake || u.stepDue || countCalls(ctl.recordedCalls(), "SuspendActor") != 0 || len(rt.conductor.idle) != 1 {
		t.Fatalf("after the step: awake %v, stepDue %v, SuspendActor calls %d, %d idle; want true, false, 0, 1",
			u.awake, u.stepDue, countCalls(ctl.recordedCalls(), "SuspendActor"), len(rt.conductor.idle))
	}

	rt.conductor.mu.Lock()
	rt.conductor.send(&rt.conductor.idle, cmdPark)
	rt.conductor.mu.Unlock()
	u.stepDue = true
	expectRows(t, "Step_"+turnStep.Name, 0, 0, func() {
		rt.conductor.step(ctx, u, dyn)
	})
	if u.awake || u.stepDue || countCalls(ctl.recordedCalls(), "SuspendActor") != 1 {
		t.Errorf("after the park: awake %v, stepDue %v, SuspendActor calls %d; want false, false, 1", u.awake, u.stepDue, countCalls(ctl.recordedCalls(), "SuspendActor"))
	}
}

func TestConductorFillWaitsForInFlightStarts(t *testing.T) {
	now := time.Unix(1000, 0)
	var inFlight int64 = 3
	c := newConductor(func() time.Time { return now })
	c.starting = func() int64 { return inFlight }
	u := &sessionUser{cmdCh: make(chan turnCmd, 1)}
	c.join(u)

	now = now.Add(2 * registrationQuiet)
	c.tick(1, 0)
	if len(c.parked) != 1 || c.filled {
		t.Fatalf("with starts in flight: parked %d, filled %v; want 1, false", len(c.parked), c.filled)
	}
	inFlight = 0
	c.tick(1, 0)
	if len(c.inFlight) != 1 {
		t.Errorf("after starts finished: inFlight %d, want 1", len(c.inFlight))
	}
}
