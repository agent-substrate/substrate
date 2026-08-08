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

// End-to-end tests for the runner against fake cAdvisor, Envoy and actor endpoints.

package routercap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var loadgenKey = ContainerKey{Namespace: "benchmarking", Pod: "routercap-runner-abc", Container: "loadgen"}

// loadgenFixture adds the generator's own container to a cAdvisor payload. The
// run's most important guard reads it, so a fake node without it would exercise
// the orchestration with that guard silently absent.
func loadgenFixture(at time.Time, cpuSeconds, quota float64) string {
	ms := at.UnixMilli()
	var b strings.Builder
	row := func(metric string, v float64) {
		fmt.Fprintf(&b, "%s{container=\"loadgen\",namespace=\"benchmarking\",pod=\"routercap-runner-abc\"} %g %d\n", metric, v, ms)
	}
	row(metricCPUUsageSeconds, cpuSeconds)
	row(metricMemoryWorkingSet, 3e8)
	row(metricCFSPeriods, 1000)
	row(metricCFSThrottledPeriods, 0)
	row(metricSpecCPUQuota, quota)
	row(metricSpecCPUPeriod, 100000)
	return b.String()
}

// fakeNode is a kubelet whose housekeeping timestamp advances on a real
// wall-clock grid, so windows the runner produces are genuine intervals of the
// test's own execution. That lets load and CPU statistics in one record
// describe the same moment — the property under test.
type fakeNode struct {
	start time.Time
	grid  time.Duration

	envoyCores   float64
	routerCores  float64
	loadgenCores float64
	loadgenQuota float64
}

func (f *fakeNode) fetch(context.Context) (io.ReadCloser, error) {
	k := time.Since(f.start) / f.grid
	at := f.start.Add(k * f.grid)
	// Counters are a linear function of the *quantized* instant, so every
	// derived rate comes out at exactly the configured core count regardless of
	// when the fetch happened to land.
	secs := at.Sub(f.start).Seconds()
	body := cadvisorFixture(at, 100+f.envoyCores*secs, 20+f.routerCores*secs) +
		loadgenFixture(at, f.loadgenCores*secs, f.loadgenQuota)
	return io.NopCloser(strings.NewReader(body)), nil
}

// fakeAdmin serves Envoy admin payloads whose counters climb, so consecutive
// scrapes produce a non-degenerate delta.
type fakeAdmin struct {
	mu sync.Mutex
	n  int
}

func (f *fakeAdmin) fetch(context.Context) (io.ReadCloser, error) {
	f.mu.Lock()
	n := f.n
	f.n++
	f.mu.Unlock()
	return io.NopCloser(strings.NewReader(envoyFixture(
		300+float64(n)*10,      // cx_total
		900000+float64(n)*5000, // rq_total
		295,                    // cx_active
	))), nil
}

func staticFetch(body string) func(context.Context) (io.ReadCloser, error) {
	return func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(body)), nil
	}
}

// fakeClient answers instantly and reports whatever transport picture the test
// wants the guards to see.
type fakeClient struct {
	mu         sync.Mutex
	sent       int
	connsInUse int64
	newConns   float64
	reqsPerCx  float64
}

func (c *fakeClient) Send(context.Context) (Outcome, int) {
	c.mu.Lock()
	c.sent++
	c.mu.Unlock()
	return OutcomeOK, 200
}

func (c *fakeClient) Stats() ClientStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return ClientStats{
		NewConnections:        c.newConns,
		RequestsPerConnection: c.reqsPerCx,
		ConnectionsInUse:      c.connsInUse,
	}
}

func (c *fakeClient) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sent
}

type memSink struct {
	mu      sync.Mutex
	samples []Sample
	fine    []FineSample
}

func (s *memSink) Sample(v Sample) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.samples = append(s.samples, v)
	return nil
}

func (s *memSink) Fine(v FineSample) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fine = append(s.fine, v)
	return nil
}

func (s *memSink) all() ([]Sample, []FineSample) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Sample(nil), s.samples...), append([]FineSample(nil), s.fine...)
}

// newTestRunner wires a runner against fakes for every external source. The
// grid is small so a ladder that would take minutes in a cluster takes a
// fraction of a second here, without changing any of the logic under test.
func newTestRunner(t *testing.T, client Client, sink Sink, ladder LadderSpec) (*Runner, *fakeNode) {
	t.Helper()
	node := &fakeNode{
		start:        time.Now(),
		grid:         60 * time.Millisecond,
		envoyCores:   4,
		routerCores:  0.5,
		loadgenCores: 2,
		loadgenQuota: 100 * 100000, // 100 cores
	}
	guards := DefaultGuardConfig()
	guards.WorkerPods = 100
	// The fakes answer instantly, so the generator cannot fall behind for any
	// reason a cluster would produce; the tiny grid makes the scheduler's own
	// jitter the only source of lag, and it is not what these tests are about.
	guards.DispatchLagP95Ms = 0

	return &Runner{
		Arm:    40,
		Pass:   1,
		Rungs:  ladder.Build(),
		Client: client,
		Sink:   sink,
		Windows: &WindowDriver{
			Client:       &CadvisorClient{Fetch: node.fetch},
			Anchor:       envoyKey,
			PollInterval: 3 * time.Millisecond,
			MaxWait:      5 * time.Second,
		},
		Envoy:  &EnvoyClient{Fetch: (&fakeAdmin{}).fetch},
		Router: &RouterClient{Fetch: staticFetch(routerFixture)},
		Targets: []Target{
			{Role: RoleEnvoy, Key: envoyKey},
			{Role: RoleSidecar, Key: routerKey},
			{Role: RoleLoadgen, Key: loadgenKey},
		},
		Guards:              guards,
		PortRange:           DefaultPortRange(),
		CircuitBreakerLimit: 20000,
		MaxInFlight:         4096,
		TickCap:             time.Millisecond,
		FineInterval:        20 * time.Millisecond,
		DrainTimeout:        2 * time.Second,
	}, node
}

func TestRunnerProducesAnAlignedSeries(t *testing.T) {
	client := &fakeClient{connsInUse: 40, newConns: 2, reqsPerCx: 500}
	sink := &memSink{}
	ladder := LadderSpec{StartQPS: 200, StepQPS: 200, Rungs: 3, Hold: 150 * time.Millisecond, Warmup: 40 * time.Millisecond}
	r, _ := newTestRunner(t, client, sink, ladder)

	res, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	samples, fine := sink.all()
	if len(samples) < 3 {
		t.Fatalf("got %d aligned samples over a 450ms ladder on a 60ms grid, want at least 3", len(samples))
	}
	if len(fine) == 0 {
		t.Fatal("no fine samples written")
	}
	if res.Windows != len(samples) {
		t.Errorf("result counted %d windows but %d were written", res.Windows, len(samples))
	}
	if res.FineSamples != len(fine) {
		t.Errorf("result counted %d fine samples but %d were written", res.FineSamples, len(fine))
	}
	if !res.Drained {
		t.Errorf("arm ended with requests still in flight against instantaneous fakes")
	}
	if len(res.Rungs) != 3 {
		t.Errorf("ran %d rungs, want all 3", len(res.Rungs))
	}

	t.Run("EveryRecordDescribesOneRealInterval", func(t *testing.T) {
		for i, s := range samples {
			if !s.T1.After(s.T0) {
				t.Fatalf("sample %d has a degenerate interval [%v, %v)", i, s.T0, s.T1)
			}
			if want := s.T0.Add(s.T1.Sub(s.T0) / 2); !s.T.Equal(want) {
				t.Errorf("sample %d: t = %v, want the interval midpoint %v", i, s.T, want)
			}
			if got := s.WindowSeconds; math.Abs(got-0.06) > 1e-6 {
				t.Errorf("sample %d: window = %vs, want the 60ms kubelet grid", i, got)
			}
			if i > 0 && !samples[i-1].T1.Equal(s.T0) {
				t.Errorf("sample %d starts at %v but the previous ended at %v: the series has a gap",
					i, s.T0, samples[i-1].T1)
			}
			if len(s.Missing) != 0 {
				t.Errorf("sample %d reported missing containers: %v", i, s.Missing)
			}
			if len(s.Errors) != 0 {
				t.Errorf("sample %d reported errors: %v", i, s.Errors)
			}
		}
	})

	t.Run("ResourceSeriesAreReadPerContainer", func(t *testing.T) {
		s := samples[len(samples)-1]
		if got := s.Containers[RoleEnvoy].CPUCores; math.Abs(got-4) > 1e-6 {
			t.Errorf("envoy cpu = %v cores, want 4", got)
		}
		if got := s.Containers[RoleSidecar].CPUCores; math.Abs(got-0.5) > 1e-6 {
			t.Errorf("sidecar cpu = %v cores, want 0.5", got)
		}
		if got := s.Containers[RoleLoadgen].CPUCores; math.Abs(got-2) > 1e-6 {
			t.Errorf("loadgen cpu = %v cores, want 2", got)
		}
		if got := s.Containers[RoleEnvoy].MemoryWorkingSetBytes; got != 1.5e9 {
			t.Errorf("envoy memory = %v, want 1.5e9", got)
		}
	})

	t.Run("LoadAndResourcesShareTheWindow", func(t *testing.T) {
		// The claim the whole design rests on: find a record inside a rung and
		// confirm the load figures for it are non-zero, i.e. computed over the
		// same interval the CPU number came from rather than over a timer's own.
		var found bool
		for _, s := range samples {
			if s.Rung < 0 {
				continue
			}
			found = true
			if s.Load.OfferedQPS <= 0 {
				t.Errorf("rung %d record offered %v QPS; the schedule says otherwise", s.Rung, s.Load.OfferedQPS)
			}
			if s.RungQPS <= 0 {
				t.Errorf("rung %d record has no nominal rate", s.Rung)
			}
			if s.Containers[RoleEnvoy].CPUCores <= 0 {
				t.Errorf("rung %d record has load but no CPU: the panels would not line up", s.Rung)
			}
		}
		if !found {
			t.Error("no record fell inside a rung")
		}
	})

	t.Run("WarmupIsMarkedNotDropped", func(t *testing.T) {
		// A rung's first seconds are where the pool grows; they belong in the
		// file, flagged, so exclusion is the analysis's decision and not the
		// harness's.
		var warm int
		for _, s := range samples {
			if s.Warmup {
				warm++
			}
		}
		if warm == 0 {
			t.Error("no record was flagged as warmup across three rungs with a 40ms warmup each")
		}
	})

	t.Run("EnvoyAndRouterSectionsAreDifferenced", func(t *testing.T) {
		s := samples[len(samples)-1]
		if s.Envoy == nil {
			t.Fatal("no envoy section")
		}
		if s.Envoy.Concurrency != 40 {
			t.Errorf("envoy concurrency = %v, want the 40 the fixture reports", s.Envoy.Concurrency)
		}
		actor, ok := s.Envoy.Clusters[ActorClusterName]
		if !ok {
			t.Fatalf("no %s cluster in %v", ActorClusterName, s.Envoy.Clusters)
		}
		if actor.NewConnections != 10 {
			t.Errorf("new connections = %v, want the 10 the counter advanced by", actor.NewConnections)
		}
		// The window's own ratio: 5000 requests over the 10 connections it
		// opened. Present because this window did open some.
		if actor.WindowRqPerCx == nil || *actor.WindowRqPerCx != 500 {
			t.Errorf("window_rq_per_cx = %v, want 500 (5000 requests over 10 connections)", actor.WindowRqPerCx)
		}
		// The headline ratio is cumulative, so it reflects every connection the
		// proxy has ever opened rather than only this window's.
		if actor.RqPerCx <= 1 {
			t.Errorf("rq_per_cx = %v, want well above 1: pooling is in force in the fixture", actor.RqPerCx)
		}
		if s.Router == nil || !s.Router.Measured {
			t.Fatalf("router parking section = %+v, want it measured", s.Router)
		}
		if s.Router.ParkingActive != 12 {
			t.Errorf("parking active = %v, want the fixture's 12", s.Router.ParkingActive)
		}
	})

	t.Run("PortBudgetIsDerivedFromTheMeasuredRange", func(t *testing.T) {
		s := samples[len(samples)-1]
		if s.Ports.Available != 28232 {
			t.Errorf("available ports = %d, want 28232", s.Ports.Available)
		}
		if s.Ports.ActiveConnections != 295 {
			t.Errorf("active connections = %v, want Envoy's cx_active of 295", s.Ports.ActiveConnections)
		}
		if s.Ports.CircuitBreakerLimit >= s.Ports.Available {
			t.Errorf("breaker limit %d is not below the %d-port budget; the kernel would run out first",
				s.Ports.CircuitBreakerLimit, s.Ports.Available)
		}
	})

	t.Run("EveryRequestTheClientSentWasAskedForByThePacer", func(t *testing.T) {
		// 200+400+600 QPS held 150ms each = 30+60+90.
		if got, want := client.count(), 180; got > want {
			t.Errorf("client sent %d requests, more than the %d the ladder scheduled", got, want)
		}
		if client.count() == 0 {
			t.Fatal("the ladder sent nothing")
		}
	})

	if res.EnvoyConcurrency != 40 {
		t.Errorf("result envoy concurrency = %v, want 40", res.EnvoyConcurrency)
	}
	if res.ClockSkewMs < 0 {
		t.Errorf("clock skew = %vms; the sample cannot postdate the fetch that read it", res.ClockSkewMs)
	}
}

func TestRunnerFineSeriesCarriesNoResourceFields(t *testing.T) {
	// Structural, not stylistic: the 1s series is not aligned to the resource
	// panels, and the only durable way to stop someone plotting it against one
	// is for the columns not to exist.
	client := &fakeClient{connsInUse: 40, newConns: 2, reqsPerCx: 500}
	sink := &memSink{}
	ladder := LadderSpec{StartQPS: 200, StepQPS: 0, Rungs: 1, Hold: 120 * time.Millisecond}
	r, _ := newTestRunner(t, client, sink, ladder)

	if _, err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	_, fine := sink.all()
	if len(fine) == 0 {
		t.Fatal("no fine samples")
	}
	b, err := json.Marshal(fine[0])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, forbidden := range []string{"cpu", "memory", "containers", "groups", "envoy", "ports"} {
		if strings.Contains(string(b), forbidden) {
			t.Errorf("fine sample carries %q: %s", forbidden, b)
		}
	}
	if !strings.Contains(string(b), `"offered_qps"`) {
		t.Errorf("fine sample lost the generator series it exists for: %s", b)
	}
}

func TestRunnerStopsTheLadderOnAFatalGuard(t *testing.T) {
	// A rig-limited run must stop rather than keep emitting numbers that
	// describe the load generator.
	sink := &memSink{}
	cfg := DefaultGuardConfig()
	client := &fakeClient{
		connsInUse: int64(cfg.ClientConnectionCeiling) + 1,
		newConns:   2,
		reqsPerCx:  500,
	}
	// Long enough that finishing normally would take many seconds; the guard
	// should cut it off inside the first rung.
	ladder := LadderSpec{StartQPS: 100, StepQPS: 100, Rungs: 10, Hold: time.Second}
	r, _ := newTestRunner(t, client, sink, ladder)

	res, err := r.Run(context.Background())

	var rigErr *RigLimitedError
	if !errors.As(err, &rigErr) {
		t.Fatalf("Run returned %v, want a RigLimitedError", err)
	}
	if len(res.FatalTrips) == 0 {
		t.Fatal("result carries no fatal trips to explain the stop")
	}
	if got := res.FatalTrips[0].Guard; got != GuardClientPorts {
		t.Errorf("tripped %s, want %s", got, GuardClientPorts)
	}
	if len(res.Rungs) >= 10 {
		t.Errorf("ran %d of 10 rungs; the guard did not stop the ladder", len(res.Rungs))
	}

	samples, _ := sink.all()
	if len(samples) == 0 {
		t.Fatal("no sample was written; the run directory would not explain why it stopped")
	}
	last := samples[len(samples)-1]
	if !AnyFatal(last.Guards) {
		t.Errorf("the last written sample does not carry the trip: %+v", last.Guards)
	}
}

func TestRunnerStopsOnAnInterrupt(t *testing.T) {
	// Ctrl-C, or the Job being deleted: exit promptly and say it was cut short
	// rather than reporting a complete ladder.
	client := &fakeClient{connsInUse: 40, newConns: 2, reqsPerCx: 500}
	sink := &memSink{}
	ladder := LadderSpec{StartQPS: 100, StepQPS: 100, Rungs: 10, Hold: time.Second}
	r, _ := newTestRunner(t, client, sink, ladder)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(150*time.Millisecond, cancel)

	done := make(chan struct{})
	var res RunResult
	var err error
	go func() {
		defer close(done)
		res, err = r.Run(ctx)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if !res.Interrupted {
		t.Error("result does not record that the arm was interrupted")
	}
	if len(res.Rungs) >= 10 {
		t.Errorf("ran %d of 10 rungs after a 150ms cancel", len(res.Rungs))
	}
}

func TestRunnerRejectsAnIncompleteConfiguration(t *testing.T) {
	cases := map[string]func(*Runner){
		"NoClient":  func(r *Runner) { r.Client = nil },
		"NoSink":    func(r *Runner) { r.Sink = nil },
		"NoWindows": func(r *Runner) { r.Windows = nil },
		"NoRungs":   func(r *Runner) { r.Rungs = nil },
	}
	for name, break_ := range cases {
		t.Run(name, func(t *testing.T) {
			r, _ := newTestRunner(t, &fakeClient{}, &memSink{},
				LadderSpec{StartQPS: 10, Rungs: 1, Hold: 10 * time.Millisecond})
			break_(r)
			if _, err := r.Run(context.Background()); err == nil {
				t.Fatal("Run accepted a runner missing one of its sources")
			}
		})
	}
}

func TestRunnerFailsWhenTheAnchorIsAbsent(t *testing.T) {
	// Expected right after an arm change: the router pod was replaced, so the
	// anchor key no longer resolves and the caller must re-resolve it. Failing
	// at prime is the point — an arm that silently measured nothing is worse.
	r, _ := newTestRunner(t, &fakeClient{}, &memSink{},
		LadderSpec{StartQPS: 10, Rungs: 1, Hold: 10 * time.Millisecond})
	r.Windows.Client = &CadvisorClient{Fetch: staticFetch("# empty\n")}

	if _, err := r.Run(context.Background()); !errors.Is(err, ErrAnchorMissing) {
		t.Fatalf("err = %v, want ErrAnchorMissing", err)
	}
}

func TestCollectorPeaksAreIndependentPerSeries(t *testing.T) {
	// The two series read the same collector at different cadences. A shared
	// high-water slot would let the 1s series reset the maximum the ~10s series
	// is about to report, so in-flight — the one number that explains a port
	// wall — would read as a fraction of its real peak.
	c := NewCollector(&Schedule{})
	now := time.Now()
	for i := 0; i < 5; i++ {
		c.RecordDispatch(now, now)
	}
	// Drain back down to 1 before either read. Without this the live count
	// still equals the peak, so the reset baseline equals what was consumed and
	// a shared slot would pass the test it exists to fail.
	for i := 0; i < 4; i++ {
		c.RecordCompletion(now, now, OutcomeOK, 200)
	}

	fine := c.FineStats(now.Add(-time.Second), now.Add(time.Second))
	aligned := c.Stats(now.Add(-time.Second), now.Add(time.Second))
	if fine.InFlightMax != 5 {
		t.Errorf("fine series peak = %d, want 5", fine.InFlightMax)
	}
	if aligned.InFlightMax != 5 {
		t.Errorf("aligned series peak = %d, want 5: the fine read consumed it", aligned.InFlightMax)
	}
}

func TestLadderSpecBuildsEvenRungs(t *testing.T) {
	l := LadderSpec{StartQPS: 1000, StepQPS: 1000, Rungs: 16, Hold: 45 * time.Second, Warmup: 10 * time.Second}
	rungs := l.Build()
	if len(rungs) != 16 {
		t.Fatalf("built %d rungs, want 16", len(rungs))
	}
	if rungs[0].RateQPS != 1000 || rungs[15].RateQPS != 16000 {
		t.Errorf("rates run %v..%v, want 1000..16000", rungs[0].RateQPS, rungs[15].RateQPS)
	}
	if l.PeakQPS() != 16000 {
		t.Errorf("PeakQPS = %v, want 16000", l.PeakQPS())
	}
	for i, r := range rungs {
		if r.Index != i {
			t.Errorf("rung %d has index %d", i, r.Index)
		}
		if r.Hold != 45*time.Second || r.Warmup != 10*time.Second {
			t.Errorf("rung %d: hold=%v warmup=%v, want the same for every rung", i, r.Hold, r.Warmup)
		}
		if !r.StartAt.IsZero() {
			t.Errorf("rung %d was built with a start time; only the pacer knows when a rung actually begins", i)
		}
	}
}

func TestJSONLSinkWritesOneLinePerRecord(t *testing.T) {
	dir := t.TempDir()
	sink, err := OpenJSONLSink(dir)
	if err != nil {
		t.Fatalf("OpenJSONLSink: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := sink.Sample(Sample{Arm: 40, Rung: i}); err != nil {
			t.Fatalf("Sample: %v", err)
		}
		if err := sink.Fine(FineSample{Arm: 40, Rung: i}); err != nil {
			t.Fatalf("Fine: %v", err)
		}
	}

	// Read before Close: unbuffered writes are what make a killed run's output
	// usable, and a test that only reads after a clean Close would not notice
	// buffering creeping in.
	b, err := os.ReadFile(filepath.Join(dir, SamplesFile))
	if err != nil {
		t.Fatalf("read samples: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d sample lines before Close, want 3: output is being buffered", len(lines))
	}
	var got Sample
	if err := json.Unmarshal([]byte(lines[2]), &got); err != nil {
		t.Fatalf("unmarshal line 3: %v", err)
	}
	if got.Rung != 2 {
		t.Errorf("last line is rung %d, want 2", got.Rung)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fb, err := os.ReadFile(filepath.Join(dir, FineFile))
	if err != nil {
		t.Fatalf("read fine: %v", err)
	}
	if n := len(strings.Split(strings.TrimSpace(string(fb)), "\n")); n != 3 {
		t.Errorf("got %d fine lines, want 3", n)
	}
}

func TestStreamSinkTagsEveryLineSoTheStreamsCanBeSplitApart(t *testing.T) {
	// The in-cluster Job's only usable output channel is stdout, shared by
	// both series and the header. Without the tag, run.sh would have to guess
	// the stream from field names.
	var buf bytes.Buffer
	s := NewStreamSink(&buf)
	if err := s.Sample(Sample{Arm: 40, Rung: 2}); err != nil {
		t.Fatalf("Sample: %v", err)
	}
	if err := s.Fine(FineSample{Arm: 40, Rung: 2}); err != nil {
		t.Fatalf("Fine: %v", err)
	}
	if err := s.Header(RunHeader{Name: "routercap"}); err != nil {
		t.Fatalf("Header: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	want := []string{StreamSample, StreamFine, StreamHeader}
	if len(lines) != len(want) {
		t.Fatalf("got %d lines, want %d", len(lines), len(want))
	}
	for i, w := range want {
		var got struct {
			Stream string          `json:"stream"`
			Record json.RawMessage `json:"record"`
		}
		if err := json.Unmarshal([]byte(lines[i]), &got); err != nil {
			t.Fatalf("line %d: %v", i, err)
		}
		if got.Stream != w {
			t.Errorf("line %d is stream %q, want %q", i, got.Stream, w)
		}
		if len(got.Record) == 0 {
			t.Errorf("line %d carries no record", i)
		}
	}
}

func TestMultiSinkKeepsGoingWhenOneSinkFails(t *testing.T) {
	// A full disk on the laptop copy must not cost us the stdout copy, which in
	// an in-cluster run is the only copy that leaves the pod.
	var buf bytes.Buffer
	stream := NewStreamSink(&buf)
	m := MultiSink{failingSink{}, stream}

	if err := m.Sample(Sample{Arm: 40}); err == nil {
		t.Error("MultiSink hid a sink's write failure")
	}
	if err := m.Fine(FineSample{Arm: 40}); err == nil {
		t.Error("MultiSink hid a sink's write failure")
	}
	if n := len(strings.Split(strings.TrimSpace(buf.String()), "\n")); n != 2 {
		t.Errorf("the healthy sink got %d lines, want 2: a failing sink stopped the fan-out", n)
	}
}

type failingSink struct{}

func (failingSink) Sample(Sample) error   { return errors.New("disk full") }
func (failingSink) Fine(FineSample) error { return errors.New("disk full") }

func TestWriteHeaderRecordsTheOrderingTheDesignClaims(t *testing.T) {
	dir := t.TempDir()
	h := RunHeader{
		Name:                "routercap",
		StartedAt:           time.Now(),
		PortRange:           PortRange{Low: 32768, High: 60999, Source: PortRangeMeasured},
		CircuitBreakerLimit: 20000,
		ExtProcMaxRequests:  20000,
		ArmCores:            []int{10, 20, 40, 70},
		Actors:              100,
		Ladder:              LadderSpec{StartQPS: 1000, StepQPS: 1000, Rungs: 16, Hold: 45 * time.Second},
		Guards:              DefaultGuardConfig(),
		Caveats:             StandingCaveats(),
	}
	if err := WriteHeader(dir, h); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}

	b, err := os.ReadFile(filepath.Join(dir, HeaderFile))
	if err != nil {
		t.Fatalf("read header: %v", err)
	}
	var got RunHeader
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// The design's central ordering claim, checkable from the output alone.
	if got.CircuitBreakerLimit >= got.PortRange.Size() {
		t.Errorf("breaker limit %d is not below the %d-port range; the kernel would exhaust before Envoy counted an overflow",
			got.CircuitBreakerLimit, got.PortRange.Size())
	}
	if got.ExtProcMaxRequests >= got.PortRange.Size() {
		t.Errorf("ext_proc limit %d is not below the %d-port range", got.ExtProcMaxRequests, got.PortRange.Size())
	}
	if got.PortRange.Source != PortRangeMeasured {
		t.Errorf("port range source = %q, want it to record how the range was obtained", got.PortRange.Source)
	}
	if got.Guards.LoadgenCPUUtilization != DefaultGuardConfig().LoadgenCPUUtilization {
		t.Error("guard thresholds did not survive the round trip; a loosened threshold would be invisible to a reader")
	}
	if len(got.Caveats) == 0 {
		t.Error("header carries no caveats")
	}
}
