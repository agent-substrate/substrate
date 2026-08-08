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

// Tests for cAdvisor parsing, per-container usage deltas, and the window driver's
// wait for the kubelet clock to advance.

package routercap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"
)

// cadvisorFixture renders a kubelet-shaped cAdvisor exposition for the router
// pod's two containers at one housekeeping instant. Shaped after the real
// endpoint: HELP/TYPE preamble, per-sample millisecond timestamps, pod-level
// and sandbox rows interleaved with the real containers.
func cadvisorFixture(at time.Time, envoyCPU, routerCPU float64) string {
	ms := at.UnixMilli()
	var b strings.Builder
	b.WriteString("# HELP container_cpu_usage_seconds_total Cumulative cpu time consumed in seconds.\n")
	b.WriteString("# TYPE container_cpu_usage_seconds_total counter\n")
	row := func(metric, container string, v float64) {
		fmt.Fprintf(&b, "%s{container=\"%s\",id=\"/kubepods/pod-abc\",image=\"img\",name=\"k8s_%s\",namespace=\"ate-system\",pod=\"atenet-router-7d9\"} %g %d\n",
			metric, container, container, v, ms)
	}
	// Pod-level cgroup and sandbox rows: these double-count the containers
	// below them and must not reach the series.
	fmt.Fprintf(&b, "container_cpu_usage_seconds_total{container=\"\",id=\"/kubepods/pod-abc\",namespace=\"ate-system\",pod=\"atenet-router-7d9\"} %g %d\n", envoyCPU+routerCPU, ms)
	fmt.Fprintf(&b, "container_cpu_usage_seconds_total{container=\"POD\",id=\"/kubepods/pod-abc/sandbox\",namespace=\"ate-system\",pod=\"atenet-router-7d9\"} 0.4 %d\n", ms)

	row(metricCPUUsageSeconds, "envoy", envoyCPU)
	row(metricCPUUsageSeconds, "atenet-router", routerCPU)
	row(metricMemoryWorkingSet, "envoy", 1.5e9)
	row(metricMemoryWorkingSet, "atenet-router", 0.5e9)
	row(metricMemoryRSS, "envoy", 1.2e9)
	row(metricMemoryRSS, "atenet-router", 0.4e9)
	row(metricCFSPeriods, "envoy", 1000)
	row(metricCFSThrottledPeriods, "envoy", 0)
	row(metricCFSThrottledSeconds, "envoy", 0)
	row(metricSpecCPUQuota, "envoy", 800000)
	row(metricSpecCPUPeriod, "envoy", 100000)
	row(metricSpecCPUQuota, "atenet-router", 800000)
	row(metricSpecCPUPeriod, "atenet-router", 100000)
	// An unrelated pod on the same node, to confirm filtering by key.
	fmt.Fprintf(&b, "container_cpu_usage_seconds_total{container=\"glutton\",namespace=\"benchmark-workloads\",pod=\"worker-1\"} 12.5 %d\n", ms)
	return b.String()
}

var (
	envoyKey  = ContainerKey{Namespace: "ate-system", Pod: "atenet-router-7d9", Container: "envoy"}
	routerKey = ContainerKey{Namespace: "ate-system", Pod: "atenet-router-7d9", Container: "atenet-router"}
)

func TestParseCadvisor(t *testing.T) {
	at := time.UnixMilli(1_800_000_000_000)
	got, err := parseCadvisor(strings.NewReader(cadvisorFixture(at, 100.0, 20.0)), at.Add(300*time.Millisecond))
	if err != nil {
		t.Fatalf("parseCadvisor: %v", err)
	}

	t.Run("DropsPodAndSandboxRows", func(t *testing.T) {
		for k := range got.Containers {
			if k.Container == "" || k.Container == "POD" {
				t.Errorf("kept aggregate row %s; it double-counts the real containers", k)
			}
		}
		if len(got.Containers) != 3 {
			t.Errorf("got %d containers, want 3 (envoy, atenet-router, glutton)", len(got.Containers))
		}
	})

	t.Run("ReadsTheContainersWeCareAbout", func(t *testing.T) {
		envoy, ok := got.Containers[envoyKey]
		if !ok {
			t.Fatalf("envoy container missing; have %v", got.Containers)
		}
		if envoy.CPUSecondsTotal != 100.0 {
			t.Errorf("envoy cpu = %v, want 100", envoy.CPUSecondsTotal)
		}
		if envoy.MemoryWorkingSetBytes != 1.5e9 {
			t.Errorf("envoy working set = %v, want 1.5e9", envoy.MemoryWorkingSetBytes)
		}
		if got := envoy.LimitCores(); got != 8 {
			t.Errorf("envoy limit = %v cores, want 8 (quota 800000 / period 100000)", got)
		}
	})

	t.Run("UsesCadvisorTimestampNotFetchTime", func(t *testing.T) {
		envoy := got.Containers[envoyKey]
		if !envoy.At.Equal(at) {
			t.Errorf("sample time = %v, want the exposition's own timestamp %v", envoy.At, at)
		}
		skew, ok := got.SkewAgainst(envoyKey)
		if !ok || skew != 300*time.Millisecond {
			t.Errorf("skew = %v (ok=%v), want 300ms behind fetch time", skew, ok)
		}
	})
}

func TestParseCadvisorHandlesAwkwardLabels(t *testing.T) {
	// Container runtimes put command lines in the `name` label, and those
	// contain quotes, braces and commas. A naive split on ',' or '}' mangles
	// the labels that follow.
	in := `container_cpu_usage_seconds_total{name="k8s_envoy_{\"a\":\"b, c\"}",container="envoy",namespace="ate-system",pod="p1"} 7.5 1800000000000
container_memory_working_set_bytes{container="envoy",namespace="ate-system",pod="p1"} 1024 1800000000000
`
	got, err := parseCadvisor(strings.NewReader(in), time.UnixMilli(1_800_000_000_000))
	if err != nil {
		t.Fatalf("parseCadvisor: %v", err)
	}
	c, ok := got.Containers[ContainerKey{Namespace: "ate-system", Pod: "p1", Container: "envoy"}]
	if !ok {
		t.Fatalf("container not found; parser was confused by the quoted label. got %v", got.Containers)
	}
	if c.CPUSecondsTotal != 7.5 || c.MemoryWorkingSetBytes != 1024 {
		t.Errorf("got cpu=%v mem=%v, want 7.5 and 1024", c.CPUSecondsTotal, c.MemoryWorkingSetBytes)
	}
}

func TestUsageBetween(t *testing.T) {
	t0 := time.UnixMilli(1_800_000_000_000)
	t1 := t0.Add(10 * time.Second)

	prev := ContainerSample{Key: envoyKey, At: t0, CPUSecondsTotal: 100, CFSPeriods: 1000, CFSThrottledPeriods: 10, CPUQuota: 800000, CPUPeriod: 100000}
	cur := ContainerSample{Key: envoyKey, At: t1, CPUSecondsTotal: 140, CFSPeriods: 2000, CFSThrottledPeriods: 110, CPUQuota: 800000, CPUPeriod: 100000, MemoryWorkingSetBytes: 2e9}

	t.Run("CPUIsARateNotALevel", func(t *testing.T) {
		u, err := usageBetween(prev, cur)
		if err != nil {
			t.Fatalf("usageBetween: %v", err)
		}
		// 40 CPU-seconds over 10 wall-clock seconds is 4 cores.
		if math.Abs(u.CPUCores-4) > 1e-9 {
			t.Errorf("cpu = %v cores, want 4", u.CPUCores)
		}
		if math.Abs(u.CPUUtilization-0.5) > 1e-9 {
			t.Errorf("utilization = %v, want 0.5 of an 8-core limit", u.CPUUtilization)
		}
		if u.MemoryWorkingSetBytes != 2e9 {
			t.Errorf("memory = %v, want the level at t1, not a rate", u.MemoryWorkingSetBytes)
		}
		if math.Abs(u.ThrottledFraction-0.1) > 1e-9 {
			t.Errorf("throttled fraction = %v, want 0.1 (100 of 1000 periods)", u.ThrottledFraction)
		}
	})

	t.Run("RestartIsAnErrorNotANegativeRate", func(t *testing.T) {
		restarted := cur
		restarted.CPUSecondsTotal = 2
		if _, err := usageBetween(prev, restarted); err == nil {
			t.Error("a counter that went backwards produced a usage value; it must be reported as a restart")
		}
	})

	t.Run("NonAdvancingTimestampIsAnError", func(t *testing.T) {
		stale := cur
		stale.At = t0
		if _, err := usageBetween(prev, stale); err == nil {
			t.Error("identical timestamps produced a usage value; the interval is undefined")
		}
	})

	t.Run("MismatchedContainersRejected", func(t *testing.T) {
		other := cur
		other.Key = routerKey
		if _, err := usageBetween(prev, other); err == nil {
			t.Error("samples from different containers were combined")
		}
	})
}

// stubFetcher serves a scripted sequence of cAdvisor payloads. If repeatLast
// is set, the final body is served forever instead of the sequence running
// out — which is what a genuinely stuck kubelet does, and keeps a timeout test
// from depending on how many polls fit inside its deadline.
type stubFetcher struct {
	bodies     []string
	repeatLast bool
	calls      int
}

func (s *stubFetcher) fetch(context.Context) (io.ReadCloser, error) {
	if s.calls >= len(s.bodies) {
		if !s.repeatLast || len(s.bodies) == 0 {
			return nil, errors.New("stub exhausted")
		}
		s.calls++
		return io.NopCloser(strings.NewReader(s.bodies[len(s.bodies)-1])), nil
	}
	b := s.bodies[s.calls]
	s.calls++
	return io.NopCloser(strings.NewReader(b)), nil
}

func TestWindowDriverWaitsForTheKubeletClock(t *testing.T) {
	// The core alignment property: a stale scrape must extend the wait, never
	// produce a window. Two repeats of the same timestamp, then an advance.
	t0 := time.UnixMilli(1_800_000_000_000)
	t1 := t0.Add(10 * time.Second)
	stub := &stubFetcher{bodies: []string{
		cadvisorFixture(t0, 100, 20), // Prime
		cadvisorFixture(t0, 100, 20), // stale
		cadvisorFixture(t0, 100, 20), // stale
		cadvisorFixture(t1, 140, 25), // kubelet housekept
	}}
	d := &WindowDriver{
		Client:       &CadvisorClient{Fetch: stub.fetch},
		Anchor:       envoyKey,
		PollInterval: time.Millisecond,
		MaxWait:      5 * time.Second,
	}

	w, err := d.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if !w.T0.Equal(t0) || !w.T1.Equal(t1) {
		t.Errorf("window = [%v, %v), want [%v, %v) from cAdvisor's own timestamps", w.T0, w.T1, t0, t1)
	}
	if w.Polls != 3 {
		t.Errorf("polls = %d, want 3: the two stale scrapes must extend the wait", w.Polls)
	}
	if w.Duration() != 10*time.Second {
		t.Errorf("duration = %v, want 10s", w.Duration())
	}
	if got := w.Mid(); !got.Equal(t0.Add(5 * time.Second)) {
		t.Errorf("mid = %v, want the interval midpoint", got)
	}

	usage, spread, missing, errs := w.Usage([]ContainerKey{envoyKey, routerKey})
	if len(missing) != 0 || len(errs) != 0 {
		t.Fatalf("usage missing=%v errs=%v", missing, errs)
	}
	if got := usage[envoyKey].CPUCores; math.Abs(got-4) > 1e-9 {
		t.Errorf("envoy cpu = %v cores, want 4 (40 cpu-seconds over 10s)", got)
	}
	if got := usage[routerKey].CPUCores; math.Abs(got-0.5) > 1e-9 {
		t.Errorf("router cpu = %v cores, want 0.5", got)
	}
	if spread != 0 {
		t.Errorf("spread = %v, want 0: both containers share the anchor's interval", spread)
	}
}

func TestWindowDriverTimesOutOnAStuckKubelet(t *testing.T) {
	// A kubelet that stops housekeeping must fail loudly. Emitting a record
	// anyway would pair a fresh QPS number with a CPU reading from minutes ago
	// and look entirely plausible on the chart.
	t0 := time.UnixMilli(1_800_000_000_000)
	stub := &stubFetcher{bodies: []string{cadvisorFixture(t0, 100, 20)}, repeatLast: true}
	d := &WindowDriver{
		Client:       &CadvisorClient{Fetch: stub.fetch},
		Anchor:       envoyKey,
		PollInterval: time.Millisecond,
		MaxWait:      50 * time.Millisecond,
	}
	if _, err := d.Next(context.Background()); err == nil {
		t.Fatal("Next returned a window from a stuck kubelet")
	} else if !strings.Contains(err.Error(), "stuck") {
		t.Errorf("error = %v, want it to name the stuck timestamp", err)
	}
}

func TestWindowDriverReportsAMissingAnchor(t *testing.T) {
	// Expected across an arm change: the router pod is replaced, so the anchor
	// key no longer resolves and the caller must re-resolve the pod.
	t0 := time.UnixMilli(1_800_000_000_000)
	stub := &stubFetcher{bodies: []string{cadvisorFixture(t0, 100, 20), `# nothing here`}}
	d := &WindowDriver{
		Client:       &CadvisorClient{Fetch: stub.fetch},
		Anchor:       envoyKey,
		PollInterval: time.Millisecond,
		MaxWait:      time.Second,
	}
	if err := d.Prime(context.Background()); err != nil {
		t.Fatalf("Prime: %v", err)
	}
	_, err := d.Next(context.Background())
	if !errors.Is(err, ErrAnchorMissing) {
		t.Errorf("error = %v, want ErrAnchorMissing so the caller can re-resolve the pod", err)
	}
}

func TestWindowUsageReportsMissingContainersSeparately(t *testing.T) {
	// "The router used no CPU" and "we could not see the router" must not
	// render as the same point.
	t0 := time.UnixMilli(1_800_000_000_000)
	t1 := t0.Add(10 * time.Second)
	prev, _ := parseCadvisor(strings.NewReader(cadvisorFixture(t0, 100, 20)), t0)
	cur, _ := parseCadvisor(strings.NewReader(cadvisorFixture(t1, 140, 25)), t1)
	w := Window{T0: t0, T1: t1, Prev: prev, Cur: cur}

	ghost := ContainerKey{Namespace: "ate-system", Pod: "atenet-router-7d9", Container: "not-a-container"}
	usage, _, missing, errs := w.Usage([]ContainerKey{envoyKey, ghost})
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(missing) != 1 || missing[0] != ghost {
		t.Errorf("missing = %v, want exactly the absent container", missing)
	}
	if _, present := usage[ghost]; present {
		t.Error("absent container got a usage entry")
	}
	if _, present := usage[envoyKey]; !present {
		t.Error("one missing container suppressed the containers that were present")
	}
}

func TestMultiNodeClientMergesEveryNode(t *testing.T) {
	// The control-plane throttling guard is only answerable from the system
	// node's kubelet, and the router's kubelet reports only the router's node.
	// One Window has to span both.
	at := time.UnixMilli(1_800_000_000_000)
	systemRow := fmt.Sprintf(
		"container_cpu_usage_seconds_total{container=\"ate-api-server\",namespace=\"ate-system\",pod=\"ate-api-server-1\"} 55 %d\n",
		at.UnixMilli())

	m := &MultiNodeClient{Clients: []*CadvisorClient{
		{Fetch: (&stubFetcher{bodies: []string{cadvisorFixture(at, 100, 20)}, repeatLast: true}).fetch},
		{Fetch: (&stubFetcher{bodies: []string{systemRow}, repeatLast: true}).fetch},
	}}

	got, err := m.Scrape(context.Background())
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	if _, ok := got.Containers[envoyKey]; !ok {
		t.Errorf("router node's containers missing from the merge: %v", got.Containers)
	}
	apiKey := ContainerKey{Namespace: "ate-system", Pod: "ate-api-server-1", Container: "ate-api-server"}
	if _, ok := got.Containers[apiKey]; !ok {
		t.Errorf("system node's containers missing from the merge: %v", got.Containers)
	}
}

func TestMultiNodeClientFailsWhenAnyNodeFails(t *testing.T) {
	// A partial merge would silently drop whichever node was unreachable, and
	// a guard that cannot see the control plane never trips.
	at := time.UnixMilli(1_800_000_000_000)
	m := &MultiNodeClient{Clients: []*CadvisorClient{
		{Fetch: (&stubFetcher{bodies: []string{cadvisorFixture(at, 100, 20)}, repeatLast: true}).fetch},
		{Fetch: func(context.Context) (io.ReadCloser, error) { return nil, errors.New("kubelet unreachable") }},
	}}

	if _, err := m.Scrape(context.Background()); err == nil {
		t.Fatal("Scrape succeeded with one node down")
	} else if !strings.Contains(err.Error(), "kubelet unreachable") {
		t.Errorf("err = %v, want it to name the failure", err)
	}
}
