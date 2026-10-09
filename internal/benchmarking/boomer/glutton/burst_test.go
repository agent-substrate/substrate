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
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/atenet"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/agent-substrate/substrate/internal/benchmarking/glutton/fake"
	"github.com/google/go-cmp/cmp"
	"github.com/myzhan/boomer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// testWait bounds every wait on the code under test.
const testWait = 10 * time.Second

// waitFor polls cond until it holds, or fails the test after testWait.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testWait)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitClosed waits until ch is closed, or fails the test after testWait.
func waitClosed(t *testing.T, what string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(testWait):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// startBurst starts BurstUser's run on a fresh runtime, as its first VU
// does. rt.done closes once the run has deleted its actors; the VU's
// goroutine then blocks for good, as in spawn's tests.
func startBurst(t *testing.T, cfg *userclass.Config) *burstRuntime {
	t.Helper()
	rt := newBurstRuntime(cfg)
	go rt.iterate()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testWait)
		defer cancel()
		rt.shutdown(ctx)
	})
	return rt
}

// holdResumesAfter lets the first pass ResumeActor calls through and holds
// every later one until its ctx is done. The returned channel closes once
// held calls are waiting: the batches the passed calls cover are then fully
// booked, and the next resume burst is in flight.
func holdResumesAfter(f *fakeControlClient, pass, held int32) <-chan struct{} {
	allHeld := make(chan struct{})
	var calls, waiting atomic.Int32
	f.hook = func(ctx context.Context, method, _ string) error {
		if method != "ResumeActor" || calls.Add(1) <= pass {
			return nil
		}
		if waiting.Add(1) == held {
			close(allHeld)
		}
		<-ctx.Done()
		return ctx.Err()
	}
	return allHeld
}

// failRoute puts a front server before cfg's router: a request that fail
// picks by target actor and path gets HTTP 500, and every other request
// goes through.
func failRoute(t *testing.T, cfg *userclass.Config, fail func(actor, path string) bool) {
	t.Helper()
	router, err := url.Parse(cfg.RouterURL)
	if err != nil {
		t.Fatalf("parse router URL: %v", err)
	}
	proxy := httputil.NewSingleHostReverseProxy(router)
	proxy.Transport = cfg.HTTPClient.Transport
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail(r.Header.Get(atenet.TargetActorHeader), r.URL.Path) {
			http.Error(w, "injected failure", http.StatusInternalServerError)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(front.Close)
	cfg.HTTPClient = front.Client()
	cfg.RouterURL = front.URL
}

// hang points cfg at a router that answers nothing until the test ends.
func hang(t *testing.T, cfg *userclass.Config) {
	t.Helper()
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-release
	}))
	t.Cleanup(ts.Close)
	// Cleanups run last-in first-out, so the held requests end before
	// Close waits for them.
	t.Cleanup(func() { close(release) })
	cfg.HTTPClient = ts.Client()
	cfg.RouterURL = ts.URL
}

// phaseRows is what one burst over n actors books when every op succeeds.
func phaseRows(p burstPhase, n int) map[string]float64 {
	rows := map[string]float64{
		p.actorMetric + "/success": float64(n),
		p.allMetric + "/success":   1,
	}
	for _, k := range deciles {
		rows[fmt.Sprintf("%s_%dpct/success", p.decilePrefix, k)] = 1
	}
	return rows
}

// addRows adds every row of srcs into dst and returns dst.
func addRows(dst map[string]float64, srcs ...map[string]float64) map[string]float64 {
	for _, src := range srcs {
		for k, v := range src {
			dst[k] += v
		}
	}
	return dst
}

// rowSnapshot holds the request rows booked so far under BurstUser, for
// burst's own rows, and under GluttonUser, for the per-RPC and workload
// rows burst books through gluttonActor.
type rowSnapshot struct{ burst, glutton map[string]float64 }

func snapRows(t *testing.T) rowSnapshot {
	t.Helper()
	return rowSnapshot{burst: requestRows(t, burstUserClass), glutton: requestRows(t, userClass)}
}

// checkRows fails unless, since before, burst's own rows grew by exactly
// wantBurst and the GluttonUser rows by exactly wantGlutton.
func checkRows(t *testing.T, before rowSnapshot, wantBurst, wantGlutton map[string]float64) {
	t.Helper()
	if diff := cmp.Diff(wantBurst, rowsGrown(before.burst, requestRows(t, burstUserClass))); diff != "" {
		t.Errorf("BurstUser rows grown (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wantGlutton, rowsGrown(before.glutton, requestRows(t, userClass))); diff != "" {
		t.Errorf("GluttonUser rows grown (-want +got):\n%s", diff)
	}
}

// checkDeletes fails unless every actor CreateActor was sent for got
// exactly one DeleteActor, or two if its name ends in one of twice, and no
// other actor got one.
func checkDeletes(t *testing.T, f *fakeControlClient, twice ...string) {
	t.Helper()
	want := map[string]int{}
	for _, r := range f.recordedCreateRequests() {
		name := r.GetActor().GetMetadata().GetName()
		want[name] = 1
		for _, suffix := range twice {
			if strings.HasSuffix(name, suffix) {
				want[name] = 2
			}
		}
	}
	got := map[string]int{}
	for _, r := range f.recordedDeleteRequests() {
		got[r.GetActor().GetName()]++
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("DeleteActor calls per actor (-want +got):\n%s", diff)
	}
}

func TestNewBurstRun(t *testing.T) {
	tests := []struct {
		name         string
		total        int
		deadline     time.Duration
		dyn          dynconfig.Config
		wantTotal    int
		wantDeadline time.Duration
	}{
		{
			name:  "flags",
			total: 4, deadline: time.Minute,
			wantTotal: 4, wantDeadline: time.Minute,
		},
		{
			name:  "web UI values override the flags",
			total: 4, deadline: time.Minute,
			dyn:       dynconfig.Config{TotalActors: 6, ActorDeadline: 2 * time.Minute},
			wantTotal: 6, wantDeadline: 2 * time.Minute,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{
				Atespace:      "bench-test",
				TotalActors:   tc.total,
				ActorDeadline: tc.deadline,
				Dyn:           dynconfig.NewHolder(tc.dyn),
			})
			b := newBurstRun(context.Background(), cfg)
			if b.total != tc.wantTotal || b.deadline != tc.wantDeadline {
				t.Errorf("total, deadline = %d, %v; want %d, %v", b.total, b.deadline, tc.wantTotal, tc.wantDeadline)
			}
			checkCohort(t, b)
		})
	}
}

// checkCohort fails unless b holds one actor per cohort slot, each named
// for the run and not yet resumed.
func checkCohort(t *testing.T, b *burstRun) {
	t.Helper()
	if len(b.actors) != b.total {
		t.Fatalf("cohort has %d actors, want %d", len(b.actors), b.total)
	}
	prefix := strings.TrimSuffix(b.actors[0].actorName, "-1")
	if !strings.HasPrefix(prefix, "burst-") {
		t.Errorf("actor name %q does not start with burst-", b.actors[0].actorName)
	}
	for i, a := range b.actors {
		if want := fmt.Sprintf("%s-%d", prefix, i+1); a.actorName != want {
			t.Errorf("actor %d is named %q, want %q", i, a.actorName, want)
		}
		if !a.firstResume {
			t.Errorf("actor %s: firstResume = false, want true", a.actorName)
		}
	}
}

// A web UI change made during a run has no effect, except on the settings
// the run reads again before each batch.
func TestNewBurstRunFreezesSettings(t *testing.T) {
	cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{Atespace: "bench-test", TotalActors: 1, ActorDeadline: time.Minute})
	b := newBurstRun(context.Background(), cfg)
	cfg.Dyn.Store(dynconfig.Config{LifecycleMode: dynconfig.LifecycleModePause, MaxLive: time.Second})
	if got := b.actors[0].cfg.Dyn.Load().LifecycleMode; got != "" {
		t.Errorf("actor lifecycle mode after a mid-run change = %q, want the frozen %q", got, "")
	}
	if got := b.live.Load().MaxLive; got != time.Second {
		t.Errorf("live MaxLive after a mid-run change = %v, want %v", got, time.Second)
	}
}

// A run resumes all N actors at once, whatever --spawn-concurrency says,
// and books every batch's rows; with --mem-target unset no RAM step runs.
// The batch a stop cuts off books no burst rows, and each actor is deleted
// once.
func TestBurstTwoBatches(t *testing.T) {
	const n = 4
	srv := &fake.Server{}
	fakeCtrl := &fakeControlClient{}
	held := holdResumesAfter(fakeCtrl, 2*n, n)
	hold := fakeCtrl.hook
	allIn, notAtOnce := make(chan struct{}), make(chan struct{})
	var arrivals atomic.Int32
	var notAtOnceOnce sync.Once
	fakeCtrl.hook = func(ctx context.Context, method, actor string) error {
		// Batch 1's resumes each wait for all n to arrive, which only
		// happens if they run at once.
		if method == "ResumeActor" {
			if k := arrivals.Add(1); k <= n {
				if k == n {
					close(allIn)
				}
				select {
				case <-allIn:
				case <-time.After(testWait / 2):
					notAtOnceOnce.Do(func() { close(notAtOnce) })
					return status.Error(codes.FailedPrecondition, "batch 1's resumes did not run at once")
				}
			}
		}
		return hold(ctx, method, actor)
	}
	cfg := newTestConfig(t, srv, &userclass.Config{
		APIStub: fakeCtrl, Atespace: "bench-test", TotalActors: n, SpawnConcurrency: 1, ActorDeadline: testWait,
	})
	before := snapRows(t)

	rt := startBurst(t, cfg)
	select {
	case <-held:
	case <-notAtOnce:
		t.Fatalf("batch 1's %d resumes did not all run at once", n)
	case <-time.After(testWait):
		t.Fatal("timed out waiting for batch 3's resume burst")
	}
	rt.cancelRun()
	waitClosed(t, "the run's teardown", rt.done)

	checkRows(t, before,
		addRows(map[string]float64{"GluttonReadyPing/success": 2 * n},
			phaseRows(firstResumePhase, n), phaseRows(firstSuspendPhase, n),
			phaseRows(resumePhase, n), phaseRows(suspendPhase, n)),
		map[string]float64{
			"CreateAtespace/success":         1,
			"CreateActor/success":            n,
			"ResumeActorFirstResume/success": n,
			"ResumeActor/success":            n,
			"ResumeActor/failure":            n, // batch 3's, cut off by the stop
			"GluttonPing/success":            2 * n,
			"SuspendActor/success":           2 * n,
			"DeleteActor/success":            n,
		})
	if slices.Contains(srv.RecordedPaths(), fake.WriteRAMRoute) {
		t.Error("WriteRAM was sent with --mem-target unset")
	}
	checkDeletes(t, fakeCtrl)
}

// With the workload flags set, batch 1 fills RAM and starts the CPU load,
// and later batches read and churn the RAM; the CPU load is never
// restarted. Pause mode hibernates with PauseActor.
func TestBurstWorkloads(t *testing.T) {
	const n = 2
	fakeCtrl := &fakeControlClient{}
	held := holdResumesAfter(fakeCtrl, 2*n, n)
	cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{
		APIStub: fakeCtrl, Atespace: "bench-test", TotalActors: n, ActorDeadline: testWait,
		Dyn: dynconfig.NewHolder(dynconfig.Config{
			LifecycleMode: dynconfig.LifecycleModePause,
			MemTarget:     "4Mi",
			MemChurn:      "1Mi",
			MemRead:       memReadAll,
			CPUCores:      1,
			CPUDutyCycle:  0.5,
		}),
	})
	before := snapRows(t)

	rt := startBurst(t, cfg)
	waitClosed(t, "batch 3's resume burst", held)
	rt.cancelRun()
	waitClosed(t, "the run's teardown", rt.done)

	checkRows(t, before,
		addRows(map[string]float64{"GluttonReadyPing/success": 2 * n},
			phaseRows(firstResumePhase, n), phaseRows(firstSuspendPhase, n),
			phaseRows(resumePhase, n), phaseRows(suspendPhase, n)),
		map[string]float64{
			"CreateAtespace/success":         1,
			"CreateActor/success":            n,
			"ResumeActorFirstResume/success": n,
			"ResumeActor/success":            n,
			"ResumeActor/failure":            n, // batch 3's, cut off by the stop
			"GluttonPing/success":            2 * n,
			"PauseActor/success":             2 * n,
			"DeleteActor/success":            n,
			// Batch 1 sets each actor up.
			"GluttonFillRAM/success": n,
			"GluttonUseCPU/success":  n,
			// Batch 2 reads what the snapshot kept, then churns it.
			"GluttonReadRAM/success":  n,
			"GluttonChurnRAM/success": n,
		})
	checkDeletes(t, fakeCtrl)
}

// An actor whose batch-1 setup fails is retired and deleted at once, and
// the run goes on with the rest.
func TestBurstSetupFailure(t *testing.T) {
	const n = 3
	fakeCtrl := &fakeControlClient{}
	held := holdResumesAfter(fakeCtrl, n, n-1)
	cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{
		APIStub: fakeCtrl, Atespace: "bench-test", TotalActors: n, ActorDeadline: testWait,
		Dyn: dynconfig.NewHolder(dynconfig.Config{MemTarget: "4Mi"}),
	})
	failRoute(t, cfg, func(actor, path string) bool {
		return path == fake.WriteRAMRoute && strings.HasSuffix(actor, "-1")
	})
	before := snapRows(t)

	rt := startBurst(t, cfg)
	waitClosed(t, "batch 2's resume burst", held)
	rt.cancelRun()
	waitClosed(t, "the run's teardown", rt.done)

	checkRows(t, before,
		addRows(map[string]float64{
			"GluttonReadyPing/success": n,
			"RetiredSetup/failure":     1,
		}, phaseRows(firstResumePhase, n), phaseRows(firstSuspendPhase, n-1)),
		map[string]float64{
			"CreateAtespace/success":         1,
			"CreateActor/success":            n,
			"ResumeActorFirstResume/success": n,
			"GluttonFillRAM/success":         n - 1,
			"GluttonFillRAM/failure":         1,
			"GluttonPing/success":            n - 1,
			"SuspendActor/success":           n - 1,
			"ResumeActor/failure":            n - 1, // batch 2's, cut off by the stop
			"DeleteActor/success":            n,
		})
	checkDeletes(t, fakeCtrl)
}

// A failed resume retires the actor, and a run whose every actor is
// retired ends on its own.
func TestBurstResumeFailures(t *testing.T) {
	const n = 2
	fakeCtrl := &fakeControlClient{}
	fakeCtrl.hook = func(_ context.Context, method, _ string) error {
		if method == "ResumeActor" {
			return status.Error(codes.FailedPrecondition, "no room on any node")
		}
		return nil
	}
	cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{
		APIStub: fakeCtrl, Atespace: "bench-test", TotalActors: n, ActorDeadline: testWait,
	})
	before := snapRows(t)

	rt := startBurst(t, cfg)
	waitClosed(t, "the run to end on its own", rt.done)

	checkRows(t, before,
		map[string]float64{
			"ActorTimeToFirstResume/failure": n,
			"TimeToAllFirstResumed/failure":  1,
		},
		map[string]float64{
			"CreateAtespace/success":         1,
			"CreateActor/success":            n,
			"ResumeActorFirstResume/failure": n,
			"DeleteActor/success":            n,
		})
	checkDeletes(t, fakeCtrl)
}

// A stop from boomer ends the run, and no burst row is booked after it.
// The teardown then deletes each actor once, in one pass: the actors the
// cut-off burst retired are left to it.
func TestBurstStop(t *testing.T) {
	const n = 3
	fakeCtrl := &fakeControlClient{}
	// Batch 2's first resume goes through; the other n-1 are held.
	held := holdResumesAfter(fakeCtrl, n+1, n-1)
	hold := fakeCtrl.hook
	var deletes atomic.Int32
	allDeletes, split := make(chan struct{}), make(chan struct{})
	var splitOnce sync.Once
	fakeCtrl.hook = func(ctx context.Context, method, actor string) error {
		if method != "DeleteActor" {
			return hold(ctx, method, actor)
		}
		// Each delete waits for all n, which only happens in one pass.
		if deletes.Add(1) == n {
			close(allDeletes)
		}
		select {
		case <-allDeletes:
			return nil
		case <-time.After(testWait / 2):
			splitOnce.Do(func() { close(split) })
			return status.Error(codes.NotFound, "the deletes did not run in one pass")
		}
	}
	cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{
		APIStub: fakeCtrl, Atespace: "bench-test", TotalActors: n, ActorDeadline: testWait,
	})
	// boomer.Events is process-wide, so the subscription outlives this
	// test; no other test publishes boomer:stop.
	taskFn, shutdown := initBurst(cfg)
	before := snapRows(t)
	grew := rowCounter(t, burstUserClass)
	go taskFn()

	waitClosed(t, "batch 2's held resumes", held)
	waitFor(t, "batch 2's first resume", func() bool { return grew("ActorTimeToResume/success") == 1 })
	boomer.Events.Publish(boomer.EVENT_STOP)
	waitFor(t, "the teardown's deletes", func() bool { return len(fakeCtrl.recordedDeleteRequests()) == n })
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	shutdown(ctx)

	select {
	case <-split:
		t.Error("the teardown deleted the actors in more than one pass")
	default:
	}
	checkRows(t, before,
		addRows(map[string]float64{
			"GluttonReadyPing/success":  n + 1,
			"ActorTimeToResume/success": 1,
		}, phaseRows(firstResumePhase, n), phaseRows(firstSuspendPhase, n)),
		map[string]float64{
			"CreateAtespace/success":         1,
			"CreateActor/success":            n,
			"ResumeActorFirstResume/success": n,
			"GluttonPing/success":            n,
			"SuspendActor/success":           n,
			"ResumeActor/success":            1,
			"ResumeActor/failure":            n - 1,
			"DeleteActor/success":            n,
		})
	checkDeletes(t, fakeCtrl)
}

// A stop during setup books no burst row and deletes each actor
// CreateActor was sent for, once.
func TestBurstStopDuringSetup(t *testing.T) {
	const n = 3
	var entered atomic.Int32
	allIn, release := make(chan struct{}), make(chan struct{})
	fakeCtrl := &fakeControlClient{createActorFn: func(string) error {
		if entered.Add(1) == n {
			close(allIn)
		}
		<-release
		return nil
	}}
	cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{
		APIStub: fakeCtrl, Atespace: "bench-test", TotalActors: n, ActorDeadline: testWait,
	})
	before := snapRows(t)

	rt := startBurst(t, cfg)
	releaseCreates := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseCreates)
	waitClosed(t, "every CreateActor", allIn)
	rt.cancelRun()
	releaseCreates()
	waitClosed(t, "the run's teardown", rt.done)

	checkRows(t, before, map[string]float64{}, map[string]float64{
		"CreateAtespace/success": 1,
		"CreateActor/success":    n,
		"DeleteActor/success":    n,
	})
	checkDeletes(t, fakeCtrl)
}

// A stop that lands while retired actors are being deleted ends that
// round, and the teardown deletes whatever is left, so the actor whose
// delete the stop cut off gets a second DeleteActor.
func TestBurstStopDuringDelete(t *testing.T) {
	const n = 3
	deleting := make(chan struct{})
	var deletes atomic.Int32
	fakeCtrl := &fakeControlClient{}
	fakeCtrl.hook = func(ctx context.Context, method, _ string) error {
		// The first delete, the retired actor's, waits until its ctx is
		// done; the teardown's go through.
		if method != "DeleteActor" || deletes.Add(1) > 1 {
			return nil
		}
		close(deleting)
		<-ctx.Done()
		return ctx.Err()
	}
	cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{
		APIStub: fakeCtrl, Atespace: "bench-test", TotalActors: n, ActorDeadline: testWait,
		Dyn: dynconfig.NewHolder(dynconfig.Config{MemTarget: "4Mi"}),
	})
	failRoute(t, cfg, func(actor, path string) bool {
		return path == fake.WriteRAMRoute && strings.HasSuffix(actor, "-1")
	})
	before := snapRows(t)

	rt := startBurst(t, cfg)
	waitClosed(t, "the retired actor's delete", deleting)
	rt.cancelRun()
	waitClosed(t, "the run's teardown", rt.done)

	checkRows(t, before,
		addRows(map[string]float64{
			"GluttonReadyPing/success": n,
			"RetiredSetup/failure":     1,
		}, phaseRows(firstResumePhase, n)),
		map[string]float64{
			"CreateAtespace/success":         1,
			"CreateActor/success":            n,
			"ResumeActorFirstResume/success": n,
			"GluttonFillRAM/success":         n - 1,
			"GluttonFillRAM/failure":         1,
			"GluttonPing/success":            n - 1,
			"DeleteActor/failure":            1, // the delete the stop cut off
			"DeleteActor/success":            n,
		})
	checkDeletes(t, fakeCtrl, "-1")
}

// An actor whose CreateActor fails is retired and deleted during setup, in
// case the create landed anyway, and the batches run on the rest.
func TestBurstCreateFailure(t *testing.T) {
	const n = 3
	fakeCtrl := &fakeControlClient{createActorFn: func(name string) error {
		if strings.HasSuffix(name, "-1") {
			return status.Error(codes.InvalidArgument, "bad actor spec")
		}
		return nil
	}}
	held := holdResumesAfter(fakeCtrl, n-1, n-1)
	cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{
		APIStub: fakeCtrl, Atespace: "bench-test", TotalActors: n, ActorDeadline: testWait,
	})
	before := snapRows(t)

	rt := startBurst(t, cfg)
	waitClosed(t, "batch 2's resume burst", held)
	rt.cancelRun()
	waitClosed(t, "the run's teardown", rt.done)

	checkRows(t, before,
		addRows(map[string]float64{"GluttonReadyPing/success": n - 1},
			phaseRows(firstResumePhase, n-1), phaseRows(firstSuspendPhase, n-1)),
		map[string]float64{
			"CreateAtespace/success":         1,
			"CreateActor/success":            n - 1,
			"CreateActor/failure":            1,
			"ResumeActorFirstResume/success": n - 1,
			"GluttonPing/success":            n - 1,
			"SuspendActor/success":           n - 1,
			"ResumeActor/failure":            n - 1, // batch 2's, cut off by the stop
			"DeleteActor/success":            n,
		})
	checkDeletes(t, fakeCtrl)
	calls := fakeCtrl.recordedCalls()
	if del, res := slices.Index(calls, "DeleteActor"), slices.Index(calls, "ResumeActor"); del < 0 || del > res {
		t.Errorf("calls = %v, want the failed actor's DeleteActor before the first ResumeActor", calls)
	}
}

// run starts nothing without at least one actor and a deadline above 0, or
// when CreateAtespace fails, and the run then ends on its own.
func TestBurstRunNotStarted(t *testing.T) {
	tests := []struct {
		name        string
		total       int
		deadline    time.Duration
		atespaceErr error
		wantCalls   []string
		wantGlutton map[string]float64
	}{
		{name: "no actors", deadline: testWait, wantGlutton: map[string]float64{}},
		{name: "negative actors", total: -1, deadline: testWait, wantGlutton: map[string]float64{}},
		{name: "no deadline", total: 1, wantGlutton: map[string]float64{}},
		{
			name: "CreateAtespace fails", total: 1, deadline: testWait,
			atespaceErr: status.Error(codes.PermissionDenied, "no access"),
			wantCalls:   []string{"CreateAtespace"},
			wantGlutton: map[string]float64{"CreateAtespace/failure": 1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeCtrl := &fakeControlClient{}
			fakeCtrl.hook = func(_ context.Context, method, _ string) error {
				if method == "CreateAtespace" {
					return tc.atespaceErr
				}
				return nil
			}
			cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{
				APIStub: fakeCtrl, Atespace: "bench-test", TotalActors: tc.total, ActorDeadline: tc.deadline,
			})
			before := snapRows(t)

			rt := startBurst(t, cfg)
			waitClosed(t, "the run to end", rt.done)

			if diff := cmp.Diff(tc.wantCalls, fakeCtrl.recordedCalls()); diff != "" {
				t.Errorf("control-plane calls (-want +got):\n%s", diff)
			}
			checkRows(t, before, map[string]float64{}, tc.wantGlutton)
		})
	}
}

// Like spawn, a worker runs one burst: a second VU starts no run, and
// neither does a VU that starts after the run is over.
func TestBurstIterateRunsOnce(t *testing.T) {
	const n = 2
	fakeCtrl := &fakeControlClient{}
	held := holdResumesAfter(fakeCtrl, n, n)
	cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{
		APIStub: fakeCtrl, Atespace: "bench-test", TotalActors: n, ActorDeadline: testWait,
	})
	rt := startBurst(t, cfg)
	go rt.iterate() // a second VU

	waitClosed(t, "batch 2's resume burst", held)
	rt.cancelRun()
	waitClosed(t, "the run's teardown", rt.done)
	go rt.iterate() // a VU that starts after the run
	time.Sleep(50 * time.Millisecond)

	creates := 0
	for _, call := range fakeCtrl.recordedCalls() {
		if call == "CreateActor" {
			creates++
		}
	}
	if creates != n {
		t.Errorf("CreateActor sent %d times, want %d: one run", creates, n)
	}
}

// shutdown returns at once when no VU started a run, and no run starts
// after it.
func TestBurstShutdownWithoutRun(t *testing.T) {
	fakeCtrl := &fakeControlClient{}
	rt := newBurstRuntime(newTestConfig(t, &fake.Server{}, &userclass.Config{
		APIStub: fakeCtrl, Atespace: "bench-test", TotalActors: 1, ActorDeadline: testWait,
	}))
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	rt.shutdown(ctx)
	if ctx.Err() != nil {
		t.Fatal("shutdown waited for a run that never started")
	}

	go rt.iterate()
	time.Sleep(50 * time.Millisecond)
	if calls := fakeCtrl.recordedCalls(); len(calls) != 0 {
		t.Errorf("a VU after shutdown sent %v, want nothing", calls)
	}
}

// work's steps each do nothing unless their flag is set; here all are set.
// Batch 1 fills RAM and starts the CPU load, and either failing retires the
// actor. Later batches read and churn the RAM: a failed step books its own
// row and the actor stays, and only a step still running at the deadline
// retires it.
func TestBurstWork(t *testing.T) {
	tests := []struct {
		name        string
		first       bool
		fail        []string // router paths that answer HTTP 500
		hang        bool     // the router never answers
		wantKept    bool
		wantBurst   map[string]float64
		wantGlutton map[string]float64
	}{
		{
			name: "batch 1", first: true, wantKept: true,
			wantBurst:   map[string]float64{},
			wantGlutton: map[string]float64{"GluttonFillRAM/success": 1, "GluttonUseCPU/success": 1},
		},
		{
			name: "batch 1, fill fails", first: true, fail: []string{fake.WriteRAMRoute},
			wantBurst:   map[string]float64{"RetiredSetup/failure": 1},
			wantGlutton: map[string]float64{"GluttonFillRAM/failure": 1},
		},
		{
			name: "batch 1, CPU load fails", first: true, fail: []string{fake.UseCPURoute},
			wantBurst:   map[string]float64{"RetiredSetup/failure": 1},
			wantGlutton: map[string]float64{"GluttonFillRAM/success": 1, "GluttonUseCPU/failure": 1},
		},
		{
			name: "later batch", wantKept: true,
			wantBurst:   map[string]float64{},
			wantGlutton: map[string]float64{"GluttonReadRAM/success": 1, "GluttonChurnRAM/success": 1},
		},
		{
			name: "later batch, router errors", fail: []string{fake.ReadRAMRoute, fake.WriteRAMRoute}, wantKept: true,
			wantBurst:   map[string]float64{},
			wantGlutton: map[string]float64{"GluttonReadRAM/failure": 1, "GluttonChurnRAM/failure": 1},
		},
		{
			name: "later batch, deadline", hang: true,
			wantBurst:   map[string]float64{"RetiredChurn/failure": 1},
			wantGlutton: map[string]float64{"GluttonReadRAM/failure": 1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{
				Atespace: "bench-test",
				Dyn: dynconfig.NewHolder(dynconfig.Config{
					MemTarget: "4Mi", MemChurn: "1Mi", MemRead: memReadAll, CPUCores: 1, CPUDutyCycle: 0.5,
				}),
			})
			deadline := testWait
			if tc.hang {
				hang(t, cfg)
				deadline = 100 * time.Millisecond
			}
			if len(tc.fail) > 0 {
				failRoute(t, cfg, func(_, path string) bool { return slices.Contains(tc.fail, path) })
			}
			b := &burstRun{ctx: context.Background(), deadline: deadline}
			a := &burstActor{gluttonActor: &gluttonActor{cfg: cfg, actorName: "burst-test-1"}}
			if !tc.first {
				a.ramFilled, a.cpuLoaded = true, true
			}
			before := snapRows(t)

			kept := b.work(context.Background(), a, tc.first)
			if kept != tc.wantKept || a.retired == tc.wantKept {
				t.Errorf("work kept the actor = %v (retired %v), want %v", kept, a.retired, tc.wantKept)
			}
			checkRows(t, before, tc.wantBurst, tc.wantGlutton)
		})
	}
}

// resumeOne pings until the actor answers, and books each ping on burst's
// GluttonReadyPing row.
func TestBurstResumeOnePingsUntilReady(t *testing.T) {
	cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{Atespace: "bench-test"})
	var pings atomic.Int32
	failRoute(t, cfg, func(_, path string) bool {
		return path == fake.PingRoute && pings.Add(1) <= 2
	})
	b := &burstRun{ctx: context.Background()}
	a := &burstActor{gluttonActor: &gluttonActor{cfg: cfg, actorName: "burst-test-1", firstResume: true}}
	before := snapRows(t)

	if err := b.resumeOne(context.Background(), a); err != nil {
		t.Fatalf("resumeOne: %v", err)
	}
	checkRows(t, before,
		map[string]float64{"GluttonReadyPing/failure": 2, "GluttonReadyPing/success": 1},
		map[string]float64{"ResumeActorFirstResume/success": 1})
}

// deleteActors retries a failed delete until it lands, takes only NotFound
// as already deleted, skips actors CreateActor was never sent for, never
// deletes an actor twice, and counts the actors still not deleted, which
// the next round tries again.
func TestDeleteActors(t *testing.T) {
	fakeCtrl := &fakeControlClient{}
	var mu sync.Mutex
	attempts := map[string]int{}
	fakeCtrl.hook = func(_ context.Context, method, actor string) error {
		if method != "DeleteActor" {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		attempts[actor]++
		switch {
		case actor == "flaky" && attempts[actor] < 3:
			return status.Error(codes.Unavailable, "control plane busy")
		case actor == "gone":
			return status.Error(codes.NotFound, "actor not found")
		case actor == "stuck":
			return status.Error(codes.FailedPrecondition, "actor is stuck")
		}
		return nil
	}
	cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{APIStub: fakeCtrl, Atespace: "bench-test"})
	actor := func(name string, created bool) *burstActor {
		return &burstActor{gluttonActor: &gluttonActor{cfg: cfg, actorName: name}, created: created}
	}
	actors := []*burstActor{
		actor("flaky", true), actor("gone", true), actor("ok", true), actor("stuck", true), actor("unsent", false),
	}
	before := snapRows(t)

	for round := 1; round <= 2; round++ {
		if left := deleteActors(context.Background(), actors); left != 1 {
			t.Errorf("round %d: %d actors not deleted, want 1", round, left)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	// FailedPrecondition is terminal, so stuck gets one attempt per round.
	if diff := cmp.Diff(map[string]int{"flaky": 3, "gone": 1, "ok": 1, "stuck": 2}, attempts); diff != "" {
		t.Errorf("DeleteActor attempts per actor (-want +got):\n%s", diff)
	}
	for _, a := range actors {
		if want := a.created && a.actorName != "stuck"; a.deleteDone != want {
			t.Errorf("%s: deleteDone = %v, want %v", a.actorName, a.deleteDone, want)
		}
	}
	checkRows(t, before, map[string]float64{}, map[string]float64{"DeleteActor/success": 2, "DeleteActor/failure": 5})
}

func TestRunEach(t *testing.T) {
	t.Run("every index once, at most conc at a time", func(t *testing.T) {
		const n, conc = 50, 4
		var inFlight, peak atomic.Int32
		runs := make([]atomic.Int32, n)
		runEach(context.Background(), n, conc, func(_ context.Context, i int) {
			cur := inFlight.Add(1)
			for {
				p := peak.Load()
				if cur <= p || peak.CompareAndSwap(p, cur) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			runs[i].Add(1)
			inFlight.Add(-1)
		})
		for i := range runs {
			if got := runs[i].Load(); got != 1 {
				t.Errorf("index %d ran %d times, want 1", i, got)
			}
		}
		if got := peak.Load(); got > conc {
			t.Errorf("peak calls in flight = %d, want at most %d", got, conc)
		}
	})
	t.Run("no new call once ctx is done", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var calls atomic.Int32
		runEach(ctx, 10, 1, func(context.Context, int) {
			calls.Add(1)
			cancel()
		})
		if got := calls.Load(); got != 1 {
			t.Errorf("calls = %d, want 1", got)
		}
	})
}

func TestBurstBookPhase(t *testing.T) {
	p := burstPhase{"TestBookActor", "TestBook", "TestBookAll"}
	const ms = time.Millisecond
	tests := []struct {
		name     string
		stopped  bool
		times    []time.Duration
		wantRows map[string]float64 // count per row
		wantSums map[string]float64 // latency sum in ms per row
	}{
		{
			// With 4 actors, rank r is the k% mark for every k with
			// ceil(4k/100) = r.
			name:  "3 of 4 succeed",
			times: []time.Duration{10 * ms, 20 * ms, 30 * ms},
			wantRows: map[string]float64{
				"TestBook_10pct/success": 1, "TestBook_20pct/success": 1,
				"TestBook_30pct/success": 1, "TestBook_40pct/success": 1, "TestBook_50pct/success": 1,
				"TestBook_60pct/success": 1, "TestBook_70pct/success": 1,
				"TestBookAll/success": 1,
			},
			wantSums: map[string]float64{
				"TestBook_10pct/success": 10, "TestBook_20pct/success": 10,
				"TestBook_30pct/success": 20, "TestBook_40pct/success": 20, "TestBook_50pct/success": 20,
				"TestBook_60pct/success": 30, "TestBook_70pct/success": 30,
				"TestBookAll/success": 30,
			},
		},
		{
			name:     "none succeed",
			wantRows: map[string]float64{"TestBookAll/failure": 1},
			wantSums: map[string]float64{},
		},
		{
			name:     "run stopped",
			stopped:  true,
			times:    []time.Duration{10 * ms},
			wantRows: map[string]float64{},
			wantSums: map[string]float64{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.stopped {
				cancel()
			}
			b := &burstRun{ctx: ctx}
			rows := requestRows(t, burstUserClass)
			sums := metricRows(t, "locust_request_duration_milliseconds", burstUserClass)

			b.bookPhase(p, 4, tc.times)
			if diff := cmp.Diff(tc.wantRows, rowsGrown(rows, requestRows(t, burstUserClass))); diff != "" {
				t.Errorf("rows grown (-want +got):\n%s", diff)
			}
			gotSums := rowsGrown(sums, metricRows(t, "locust_request_duration_milliseconds", burstUserClass))
			if diff := cmp.Diff(tc.wantSums, gotSums); diff != "" {
				t.Errorf("latency sums grown in ms (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPercentile(t *testing.T) {
	sorted := []time.Duration{1, 2, 3, 4}
	for _, tc := range []struct {
		p    int
		want time.Duration
	}{{0, 1}, {1, 1}, {25, 1}, {50, 2}, {90, 4}, {100, 4}} {
		if got := percentile(sorted, tc.p); got != tc.want {
			t.Errorf("percentile(%v, %d) = %v, want %v", sorted, tc.p, got, tc.want)
		}
	}
	if got := percentile(nil, 50); got != 0 {
		t.Errorf("percentile(nil, 50) = %v, want 0", got)
	}
}

func TestSleepCtx(t *testing.T) {
	if !sleepCtx(context.Background(), time.Millisecond) {
		t.Error("sleepCtx on a live context = false, want true")
	}
	if !sleepCtx(context.Background(), 0) {
		t.Error("sleepCtx(0) on a live context = false, want true")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sleepCtx(ctx, testWait) {
		t.Error("sleepCtx on a canceled context = true, want false")
	}
	if sleepCtx(ctx, 0) {
		t.Error("sleepCtx(0) on a canceled context = true, want false")
	}
}
