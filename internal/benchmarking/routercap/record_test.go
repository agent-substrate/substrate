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

// Tests for sample assembly, the port budget, and the JSON shape charts.py reads.

package routercap

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func TestAggregateKeepsMaximaForThrottling(t *testing.T) {
	// One throttled container among many idle ones must survive aggregation.
	// Averaging is what would hide it.
	g := aggregate([]ContainerUsage{
		{Container: "ate-controller", CPUCores: 0.2, CPUUtilization: 0.10},
		{Container: "ate-api-server", CPUCores: 15, CPUUtilization: 0.94,
			ThrottledPeriods: 40, Periods: 100, ThrottledSeconds: 0.2, ThrottledFraction: 0.4},
		{Container: "coredns", CPUCores: 0.1, CPUUtilization: 0.10},
	})

	if g.Containers != 3 {
		t.Errorf("Containers = %d, want 3", g.Containers)
	}
	if want := 15.3; math.Abs(g.CPUCores-want) > 1e-9 {
		t.Errorf("CPUCores = %v, want %v (sum)", g.CPUCores, want)
	}
	if g.ThrottledFractionMax != 0.4 || g.ThrottledMaxOf != "ate-api-server" {
		t.Errorf("throttling max = %v of %q, want 0.4 of ate-api-server", g.ThrottledFractionMax, g.ThrottledMaxOf)
	}
	if g.CPUUtilizationMax != 0.94 || g.CPUUtilizationMaxOf != "ate-api-server" {
		t.Errorf("utilization max = %v of %q, want 0.94 of ate-api-server", g.CPUUtilizationMax, g.CPUUtilizationMaxOf)
	}
}

func TestPortBudget(t *testing.T) {
	// The Linux default range, the configured breaker, 5000 live upstream
	// connections and 100 new per second.
	p := portBudget(32768, 60999, 20000, 5000, 100)

	if p.Available != 28232 {
		t.Errorf("Available = %d, want 28232", p.Available)
	}
	if p.TimeWaitEstimate != 6000 {
		t.Errorf("TimeWaitEstimate = %v, want 6000 (100/s over a 60s TIME_WAIT)", p.TimeWaitEstimate)
	}
	if p.EstimatedInUse != 11000 {
		t.Errorf("EstimatedInUse = %v, want 11000", p.EstimatedInUse)
	}
	if p.Headroom != 17232 {
		t.Errorf("Headroom = %v, want 17232", p.Headroom)
	}
	// The ordering the whole design depends on: Envoy's counted cap trips
	// before the kernel's opaque one.
	if p.CircuitBreakerLimit >= p.Available {
		t.Errorf("circuit breaker %d is not below the port budget %d; the kernel would run out first and the failure would be an unattributable EADDRNOTAVAIL",
			p.CircuitBreakerLimit, p.Available)
	}
}

func TestPortBudgetWithAnUnreadRange(t *testing.T) {
	// If the range could not be read out of the live pod, no utilization is
	// better than one computed against an assumed default.
	p := portBudget(0, 0, 20000, 5000, 100)
	if p.Available != 0 || p.Utilization != 0 || p.Headroom != 0 {
		t.Errorf("an unread port range produced derived values: %+v", p)
	}
	if p.EstimatedInUse != 11000 {
		t.Errorf("EstimatedInUse = %v, want the raw estimate to survive", p.EstimatedInUse)
	}
}

func TestBuildSampleSplitsRolesAndNamesWhatIsMissing(t *testing.T) {
	t0 := time.UnixMilli(1700000000000)
	t1 := t0.Add(10 * time.Second)

	mk := func(k ContainerKey, at time.Time, cpu float64) ContainerSample {
		return ContainerSample{Key: k, At: at, CPUSecondsTotal: cpu, CPUQuota: 4000000, CPUPeriod: 100000}
	}
	envoy := ContainerKey{"ate-system", "atenet-router-abc", "envoy"}
	sidecar := ContainerKey{"ate-system", "atenet-router-abc", "atenet-router"}
	api := ContainerKey{"ate-system", "ate-api-server-1", "ate-api-server"}
	ctrl := ContainerKey{"ate-system", "ate-controller-1", "ate-controller"}
	gone := ContainerKey{"benchmarking", "routercap-xyz", "loadgen"}

	w := Window{
		T0: t0, T1: t1,
		Prev: CadvisorScrape{Containers: map[ContainerKey]ContainerSample{
			envoy:   mk(envoy, t0, 100),
			sidecar: mk(sidecar, t0, 50),
			api:     mk(api, t0, 10),
			ctrl:    mk(ctrl, t0, 1),
		}},
		Cur: CadvisorScrape{Containers: map[ContainerKey]ContainerSample{
			envoy:   mk(envoy, t1, 120),
			sidecar: mk(sidecar, t1, 60),
			api:     mk(api, t1, 15),
			ctrl:    mk(ctrl, t1, 1.1),
		}},
	}
	targets := []Target{
		{RoleEnvoy, envoy}, {RoleSidecar, sidecar},
		{RoleControlPlane, api}, {RoleControlPlane, ctrl},
		{RoleLoadgen, gone},
	}

	containers, groups, spread, missing, errs := buildSample(w, targets)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if got := containers[RoleEnvoy].CPUCores; got != 2 {
		t.Errorf("envoy CPUCores = %v, want 2 (20 cpu-seconds over 10s)", got)
	}
	if _, ok := containers[RoleControlPlane]; ok {
		t.Error("a many-container role leaked into Containers instead of Groups")
	}
	if g := groups[RoleControlPlane]; g.Containers != 2 || math.Abs(g.CPUCores-0.51) > 1e-9 {
		t.Errorf("control plane group = %+v, want 2 containers summing to 0.51 cores", g)
	}
	// The loadgen container was never in either scrape. It must be named, not
	// dropped, or "we could not see it" reads as "it used nothing".
	if len(missing) != 1 || !strings.Contains(missing[0], RoleLoadgen) {
		t.Errorf("missing = %v, want the loadgen container named", missing)
	}
	if _, ok := containers[RoleLoadgen]; ok {
		t.Error("a missing container produced a usage entry")
	}
	if spread != 0 {
		t.Errorf("spread = %v, want 0: every container shared the anchor's timestamps", spread)
	}
}

func TestBuildSampleReportsSpreadAgainstTheAnchor(t *testing.T) {
	// A container whose own cAdvisor timestamps differ from the anchor's is
	// still measured, but the disagreement has to reach the output so the
	// alignment claim can be checked rather than assumed.
	t0 := time.UnixMilli(1700000000000)
	t1 := t0.Add(10 * time.Second)
	envoy := ContainerKey{"ate-system", "atenet-router-abc", "envoy"}
	worker := ContainerKey{"benchmark-workloads", "glutton-7", "ateom"}

	w := Window{
		T0: t0, T1: t1,
		Prev: CadvisorScrape{Containers: map[ContainerKey]ContainerSample{
			envoy:  {Key: envoy, At: t0, CPUSecondsTotal: 100},
			worker: {Key: worker, At: t0.Add(-1500 * time.Millisecond), CPUSecondsTotal: 5},
		}},
		Cur: CadvisorScrape{Containers: map[ContainerKey]ContainerSample{
			envoy:  {Key: envoy, At: t1, CPUSecondsTotal: 120},
			worker: {Key: worker, At: t1.Add(-1500 * time.Millisecond), CPUSecondsTotal: 6},
		}},
	}

	_, groups, spread, _, errs := buildSample(w, []Target{{RoleEnvoy, envoy}, {RoleWorker, worker}})
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if spread != 1500*time.Millisecond {
		t.Errorf("spread = %v, want 1.5s", spread)
	}
	if groups[RoleWorker].Containers != 1 {
		t.Errorf("worker group = %+v, want 1 container", groups[RoleWorker])
	}
}

func TestSampleRoundTripsThroughJSON(t *testing.T) {
	// samples.jsonl is the deliverable and charts.py reads nothing else, so the
	// interval bounds and every headline series must survive the encoding.
	t0 := time.UnixMilli(1700000000000).UTC()
	t1 := t0.Add(10 * time.Second)
	in := Sample{
		Arm: 40, Rung: 3, RungQPS: 4000,
		T: t0.Add(5 * time.Second), T0: t0, T1: t1, WindowSeconds: 10, WindowPolls: 4,
		Load: GenStats{
			OfferedQPS: 4000, AchievedQPS: 3990, InFlightEnd: 52,
			Latency:  LatencyStats{Count: 39900, P50Ms: 4.1, P95Ms: 21.3},
			Outcomes: map[Outcome]int{OutcomeOK: 39900},
		},
		Containers: map[string]ContainerUsage{
			RoleEnvoy:   {Container: "envoy", CPUCores: 22.5, CPULimitCores: 40, CPUUtilization: 0.5625},
			RoleSidecar: {Container: "atenet-router", CPUCores: 4.1, CPULimitCores: 8},
		},
		Ports: portBudget(32768, 60999, 20000, 52, 3),
	}

	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out Sample
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !out.T0.Equal(t0) || !out.T1.Equal(t1) {
		t.Errorf("interval bounds = %s..%s, want %s..%s", out.T0, out.T1, t0, t1)
	}
	if out.Load.OfferedQPS != 4000 || out.Load.Latency.P95Ms != 21.3 {
		t.Errorf("load did not survive: %+v", out.Load)
	}
	if out.Containers[RoleEnvoy].CPUCores != 22.5 {
		t.Errorf("envoy cpu did not survive: %+v", out.Containers)
	}
	if out.Ports.Available != 28232 {
		t.Errorf("port budget did not survive: %+v", out.Ports)
	}
	// Guards and the two scraper sections are absent here and must not appear
	// as nulls a chart would have to special-case. Checked on the encoded form
	// as well as the decoded one, since a null decodes back to nil either way.
	if out.Envoy != nil || out.Router != nil || out.Guards != nil {
		t.Errorf("absent optional sections decoded as non-nil: envoy=%v router=%v guards=%v", out.Envoy, out.Router, out.Guards)
	}
	for _, key := range []string{`"guards"`, `"router":`, `,"envoy":{"concurrency"`} {
		if strings.Contains(string(b), key) {
			t.Errorf("empty optional section %s was encoded: %s", key, b)
		}
	}
}
