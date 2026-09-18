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

// Package parking exercises request parking end to end through the real
// Envoy → ext_proc → ateapi → worker path: a deliberately 1-worker pool is
// oversubscribed by two actors, so a request for the suspended actor parks
// until the worker frees (ParkThenServed) or the park budget elapses
// (BudgetExhaustion). It runs with the router's default parking configuration
// (budget 5s); flag-dependent behavior (lot-full shed, parking disabled,
// custom budgets) is covered by unit tests instead, because the shared router
// cannot be reconfigured per test.
package parking

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// parkingAtespace is where deployParkingFixture applies the fixture, and the
// k8s namespace holding its pool.
var parkingAtespace = e2e.FixtureName("ate-e2e") + "-parking"

// The park budget the deployed router runs with (its flag default). The
// timing assertions below are windows around it, wide enough for scheduling
// jitter but narrow enough to prove parking happened and that the router —
// not an Envoy timeout — produced the verdict.
const routerParkBudget = 5 * time.Second

func TestRequestParking(t *testing.T) {
	ctx := context.Background()
	clients := e2e.GetClients()

	// One worker, two actors: the minimal deterministic oversubscription.
	at := deployParkingFixture(t, ctx, clients)

	actorA := "parked-a"
	actorB := "parked-b"
	for _, name := range []string{actorA, actorB} {
		createActor(ctx, t, clients, at, name)
	}

	router, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("creating router client: %v", err)
	}
	defer router.Close()
	dataplane := e2e.CurrentAtenetDataplane()
	parking, err := dataplane.NewParkingObserver(ctx)
	if err != nil {
		t.Fatalf("creating parking observer: %v", err)
	}
	defer parking.Close()

	t.Run("ParkThenServed", func(t *testing.T) {
		// Occupy the only worker with actor A.
		resumeActor(ctx, t, clients, actorA)
		waitForActorState(ctx, t, clients, actorA, ateapipb.ActorState_ACTOR_STATE_RUNNING)

		// Request actor B: the pool is full, so the request parks. Freeing
		// the worker is asynchronous — SuspendActor(A) returns before the
		// suspend completes, and on the micro-VM class the snapshot upload
		// routinely outlives the 5s park budget under CI contention. A
		// budget-exhausted 503 while the suspend is still in flight is the
		// router behaving correctly, so the request is retried: each attempt
		// parks anew, and the suspend's completion lets one of them resume B.
		// A stranded worker (#675's root cause) fails every attempt, so the
		// regression this subtest pins still fails it.
		type result struct {
			resp *http.Response
			body string
			err  error
		}
		resCh := make(chan result, 1)
		var res result
		var elapsed time.Duration
		for attempt := 1; ; attempt++ {
			start := time.Now()
			go func() {
				resp, err := router.Get(ctx, resources.ActorRef{Atespace: parkingAtespace, Name: actorB}, "/whoami")
				var body string
				if err == nil {
					b, _ := io.ReadAll(resp.Body)
					resp.Body.Close()
					body = string(b)
				}
				resCh <- result{resp, body, err}
			}()
			if attempt == 1 {
				// Free the worker only once the request is observably parked —
				// the dataplane's active-parking gauge, not a sleep, is the
				// synchronization point.
				waitForParkedCount(ctx, t, parking, func(active int) bool { return active >= 1 })
				suspendActor(ctx, t, clients, actorA)
			}
			res = <-resCh
			elapsed = time.Since(start)
			if res.err != nil {
				t.Fatalf("parked request failed transport-level: %v", res.err)
			}
			retryableBudgetExhaustion := dataplane.IsRetryableParkingBudgetExhaustion(res.resp.StatusCode, res.body)
			if retryableBudgetExhaustion && attempt < 3 {
				t.Logf("attempt %d budget-exhausted while the worker was still freeing (HTTP %d after %v); retrying", attempt, res.resp.StatusCode, elapsed)
				continue
			}
			break
		}
		if res.resp.StatusCode != http.StatusOK {
			t.Fatalf("parked request: status = %d (body %q), want 200", res.resp.StatusCode, res.body)
		}
		// The parked request must have been served by the actor it named, not
		// by whichever one happened to hold the worker.
		if !strings.Contains(res.body, actorB) {
			t.Errorf("parked request body = %q, want the probe to name %q", res.body, actorB)
		}
		// No upper bound on elapsed here: a 200 proves the router served the
		// request before Envoy's ext_proc timeout, and a slow-but-successful
		// restore under CI contention is a pass, not a flake.
		t.Logf("parked request served after %v", elapsed)

		// The flake's root cause stranded actors in RESUMING with the worker
		// claimed (#675): pin that B really converges and a follow-up request
		// is served warm — a stranded actor would 503 it.
		waitForActorState(ctx, t, clients, actorB, ateapipb.ActorState_ACTOR_STATE_RUNNING)
		followUp, err := router.Get(ctx, resources.ActorRef{Atespace: parkingAtespace, Name: actorB}, "/whoami")
		if err != nil {
			t.Fatalf("follow-up request failed transport-level: %v", err)
		}
		followUpBody, _ := io.ReadAll(followUp.Body)
		followUp.Body.Close()
		if followUp.StatusCode != http.StatusOK {
			t.Errorf("follow-up request: status = %d (body %q), want 200 from the resumed actor", followUp.StatusCode, string(followUpBody))
		}

		// The slot must be released once served.
		waitForParkedCount(ctx, t, parking, func(active int) bool { return active == 0 })
	})

	t.Run("BudgetExhaustion", func(t *testing.T) {
		// State from the previous subtest: actor B runs on the only worker and
		// nothing will free it; actor A is suspended. Requesting A must park
		// for the full budget and surface the capacity error — from the
		// router, not from an Envoy timeout.
		start := time.Now()
		resp, err := router.Get(ctx, resources.ActorRef{Atespace: parkingAtespace, Name: actorA}, "/whoami")
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("budget-exhausted request failed transport-level: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)

		wantStatus := dataplane.ParkingBudgetStatus()
		if resp.StatusCode != wantStatus {
			t.Fatalf("status = %d (body %q), want %d", resp.StatusCode, string(body), wantStatus)
		}
		if wantStatus == http.StatusServiceUnavailable && !strings.Contains(string(body), "no free workers available") {
			t.Errorf("body = %q, want the router's capacity verdict", string(body))
		}
		if wantStatus == http.StatusServiceUnavailable {
			if ct := resp.Header.Get("content-type"); ct != "text/plain" {
				t.Errorf("content-type = %q, want text/plain", ct)
			}
		}
		// Lower bound proves the request parked (fail-fast would answer in
		// milliseconds); upper bound proves the router's own verdict landed
		// before Envoy's ext_proc timeout (budget+5s) could.
		if elapsed < routerParkBudget-time.Second {
			t.Errorf("503 after %v: too fast, the request did not park for the budget", elapsed)
		}
		if elapsed > routerParkBudget+4*time.Second {
			t.Errorf("503 after %v: too slow, likely an Envoy timeout rather than the router's verdict", elapsed)
		}
		t.Logf("budget exhausted after %v with HTTP %d", elapsed, wantStatus)
	})
}

// deployParkingFixture installs the parking probe fixture for the sandbox
// class under test and waits for its golden snapshot. The fixture declares
// both halves of the oversubscription: one worker, sized to one of its own
// actors (see probe-parking.yaml.tmpl).
func deployParkingFixture(t *testing.T, ctx context.Context, clients *e2e.Clients) *ateapipb.ActorTemplate {
	t.Helper()
	env, err := e2e.CheckEnv("BUCKET_NAME")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	_, templates := e2e.DeploySubstrateFixture(t, ctx, clients, e2e.SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/probe/probe-parking.yaml.tmpl",
		Template: "internal/e2e/fixtures/probe/probe-parking-template.yaml.tmpl",
	}, env["BUCKET_NAME"], "parking", false)
	return templates[0]
}

func createActor(ctx context.Context, t *testing.T, clients *e2e.Clients, at *ateapipb.ActorTemplate, name string) {
	t.Helper()
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: parkingAtespace, Name: name},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create actor %q: %v", name, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		// Deletion requires the actor to be suspended first; both are
		// best-effort so one failed cleanup doesn't mask the test result.
		_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapipb.SuspendActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: parkingAtespace, Name: name},
		})
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: parkingAtespace, Name: name},
		})
	})
}

func resumeActor(ctx context.Context, t *testing.T, clients *e2e.Clients, name string) {
	t.Helper()
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: parkingAtespace, Name: name},
	}); err != nil {
		t.Fatalf("failed to resume actor %q: %v", name, err)
	}
}

func suspendActor(ctx context.Context, t *testing.T, clients *e2e.Clients, name string) {
	t.Helper()
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: parkingAtespace, Name: name},
	}); err != nil {
		t.Fatalf("failed to suspend actor %q: %v", name, err)
	}
}

func waitForActorState(ctx context.Context, t *testing.T, clients *e2e.Clients, name string, want ateapipb.ActorState) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: parkingAtespace, Name: name},
		})
		if err == nil && resp.GetStatus().GetState() == want {
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("timed out waiting for actor %q to reach %v", name, want)
}

// waitForParkedCount polls the dataplane's active-parking gauge until cond holds.
// The deadline is short: a parking request becomes visible within its first
// retry interval (~100ms), and a served one releases its slot immediately.
func waitForParkedCount(ctx context.Context, t *testing.T, parking e2e.ParkingObserver, cond func(active int) bool) {
	t.Helper()
	last, err := parking.WaitForCount(ctx, cond)
	if err != nil {
		t.Fatalf("waiting for parking gauge (last active=%d): %v", last, err)
	}
}
