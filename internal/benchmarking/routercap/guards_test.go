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

// Tests for each guard's trip condition and for which trips are fatal.

package routercap

import (
	"fmt"
	"io"
	"log/slog"
	"testing"
)

// healthySample is a window where nothing is wrong: the generator is idle
// relative to its limit, keeping its schedule and reusing connections, and the
// control plane is not throttling. Each test perturbs exactly one thing.
func healthySample(cfg *GuardConfig) *Sample {
	cfg.WorkerPods = 100
	return &Sample{
		Load: GenStats{
			OfferedQPS:  8000,
			AchievedQPS: 8000,
			Latency:     LatencyStats{P95Ms: 12},
			DispatchLag: LatencyStats{P95Ms: 2},
		},
		Client: ClientStats{NewConnections: 4, RequestsPerConnection: 2000},
		Containers: map[string]ContainerUsage{
			RoleLoadgen: {Container: "loadgen", CPUCores: 20, CPULimitCores: 100, CPUUtilization: 0.20},
			RoleEnvoy:   {Container: "envoy", CPUCores: 30, CPULimitCores: 40, CPUUtilization: 0.75},
		},
		Groups: map[string]GroupUsage{
			RoleControlPlane: {Containers: 8, CPUCores: 6},
		},
		Envoy: &EnvoyDelta{Clusters: map[string]ClusterDelta{
			ActorClusterName: {NewConnectionsPerSec: 100},
		}},
	}
}

func tripFor(trips []GuardTrip, name GuardName) (GuardTrip, bool) {
	for _, t := range trips {
		if t.Guard == name {
			return t, true
		}
	}
	return GuardTrip{}, false
}

func TestGuardsPassOnAHealthyWindow(t *testing.T) {
	cfg := DefaultGuardConfig()
	s := healthySample(&cfg)
	if trips := cfg.Check(s); len(trips) != 0 {
		t.Fatalf("healthy window tripped %d guards: %+v", len(trips), trips)
	}
}

func TestGuardLoadgenCPU(t *testing.T) {
	cfg := DefaultGuardConfig()
	s := healthySample(&cfg)
	s.Containers[RoleLoadgen] = ContainerUsage{Container: "loadgen", CPUCores: 85, CPULimitCores: 100, CPUUtilization: 0.85}

	trips := cfg.Check(s)
	tr, ok := tripFor(trips, GuardLoadgenCPU)
	if !ok {
		t.Fatalf("loadgen at 85%% of its limit did not trip: %+v", trips)
	}
	if !tr.Fatal {
		t.Error("loadgen CPU trip is not fatal; the offered rate is unreliable past this point")
	}
}

func TestGuardLoadgenCPUWithoutALimitIsItselfATrip(t *testing.T) {
	// An unlimited generator container silently disables the run's most
	// important guard, so it has to be an error rather than a skip.
	cfg := DefaultGuardConfig()
	s := healthySample(&cfg)
	s.Containers[RoleLoadgen] = ContainerUsage{Container: "loadgen", CPUCores: 3}

	tr, ok := tripFor(cfg.Check(s), GuardLoadgenCPU)
	if !ok || !tr.Fatal {
		t.Fatalf("an unlimited loadgen container must trip fatally; got ok=%v trip=%+v", ok, tr)
	}
}

func TestGuardControlPlaneThrottleTripsOnAnyThrottling(t *testing.T) {
	cfg := DefaultGuardConfig()
	s := healthySample(&cfg)
	s.Groups[RoleControlPlane] = GroupUsage{
		Containers: 8, ThrottledPeriods: 1, ThrottledSeconds: 0.004,
		ThrottledFractionMax: 0.001, ThrottledMaxOf: "ate-api-server",
	}

	tr, ok := tripFor(cfg.Check(s), GuardControlPlaneThrottle)
	if !ok || !tr.Fatal {
		t.Fatalf("a single throttled period in ate-system must trip fatally; got ok=%v trip=%+v", ok, tr)
	}
}

func TestGuardWorkerConnRate(t *testing.T) {
	cfg := DefaultGuardConfig()
	s := healthySample(&cfg)
	// 50,000/s across 100 pods is 500/s each, above the 400/s threshold and
	// close to the ~470/s TIME_WAIT ceiling.
	s.Envoy.Clusters[ActorClusterName] = ClusterDelta{NewConnectionsPerSec: 50000}

	tr, ok := tripFor(cfg.Check(s), GuardWorkerConnRate)
	if !ok || !tr.Fatal {
		t.Fatalf("500 new conns/sec per worker pod must trip; got ok=%v trip=%+v", ok, tr)
	}
	if tr.Value != 500 {
		t.Errorf("Value = %v, want 500 (cluster rate divided by pod count)", tr.Value)
	}
}

func TestGuardClientKeepAlive(t *testing.T) {
	cfg := DefaultGuardConfig()
	s := healthySample(&cfg)
	s.Client = ClientStats{NewConnections: 8000, RequestsPerConnection: 1}

	tr, ok := tripFor(cfg.Check(s), GuardClientKeepAlive)
	if !ok || !tr.Fatal {
		t.Fatalf("a connection per request must trip the keep-alive guard; got ok=%v trip=%+v", ok, tr)
	}
}

func TestGuardClientKeepAliveIgnoresPerfectReuse(t *testing.T) {
	// Zero new connections is the healthy steady state, not a divide-by-zero
	// worth reporting as a ratio of 0.
	cfg := DefaultGuardConfig()
	s := healthySample(&cfg)
	s.Client = ClientStats{NewConnections: 0, RequestsPerConnection: 0}

	if tr, ok := tripFor(cfg.Check(s), GuardClientKeepAlive); ok {
		t.Fatalf("perfect connection reuse tripped the keep-alive guard: %+v", tr)
	}
}

func TestGuardClientKeepAliveIgnoresPreLadderWindow(t *testing.T) {
	// The pre-ladder window covers the actor pre-warm: a low ratio by
	// construction that says nothing about keep-alive. It tripped a real arm
	// at rung 0, so the offered-load floor is a regression test.
	cfg := DefaultGuardConfig()
	s := healthySample(&cfg)
	s.Load.OfferedQPS = 0
	s.Load.AchievedQPS = 0
	s.Client = ClientStats{NewConnections: 32, RequestsPerConnection: 9.53}

	if tr, ok := tripFor(cfg.Check(s), GuardClientKeepAlive); ok {
		t.Fatalf("pre-ladder warm-up window tripped the keep-alive guard: %+v", tr)
	}

	// The same ratio under real load is still fatal.
	s.Load.OfferedQPS = 1000
	s.Load.AchievedQPS = 1000
	if tr, ok := tripFor(cfg.Check(s), GuardClientKeepAlive); !ok || !tr.Fatal {
		t.Fatalf("the same ratio under load must still trip; got ok=%v trip=%+v", ok, tr)
	}
}

func TestGuardClientPorts(t *testing.T) {
	// The generator dials one router pod IP, so its own source-port range is
	// the same 28232 the router has. It would hit that first, and the cliff
	// would belong to the rig.
	cfg := DefaultGuardConfig()
	s := healthySample(&cfg)
	s.Client.ConnectionsInUse = int64(cfg.ClientConnectionCeiling) + 1

	tr, ok := tripFor(cfg.Check(s), GuardClientPorts)
	if !ok || !tr.Fatal {
		t.Fatalf("exceeding the generator's connection ceiling must trip fatally; got ok=%v trip=%+v", ok, tr)
	}
	if cfg.ClientConnectionCeiling >= defaultEphemeralPorts {
		t.Errorf("ceiling %d leaves no headroom below the %d-port range", cfg.ClientConnectionCeiling, defaultEphemeralPorts)
	}
}

func TestGuardDispatchLagTripsWhenTheSystemIsHealthy(t *testing.T) {
	cfg := DefaultGuardConfig()
	s := healthySample(&cfg)
	s.Load.DispatchLag.P95Ms = 120

	tr, ok := tripFor(cfg.Check(s), GuardDispatchLag)
	if !ok {
		t.Fatal("120ms of dispatch lag did not trip the guard")
	}
	if !tr.Fatal {
		t.Error("dispatch lag is fatal when the system is keeping up: the generator, not the router, is the bottleneck")
	}
}

func TestGuardDispatchLagIsNotFatalWhenSaturated(t *testing.T) {
	// The exception the ladder depends on. Without it the run would abort at
	// precisely the rungs it exists to measure.
	cases := []struct {
		name  string
		apply func(*Sample)
	}{
		{"LatencySaysSaturated", func(s *Sample) { s.Load.Latency.P95Ms = 400 }},
		{"ThroughputSaysSaturated", func(s *Sample) { s.Load.AchievedQPS = 5000 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultGuardConfig()
			s := healthySample(&cfg)
			s.Load.DispatchLag.P95Ms = 120
			tc.apply(s)

			tr, ok := tripFor(cfg.Check(s), GuardDispatchLag)
			if !ok {
				t.Fatal("the trip must still be recorded so the reader can see which windows to distrust")
			}
			if tr.Fatal {
				t.Errorf("dispatch lag was fatal under saturation: %s", tr.Detail)
			}
			if AnyFatal(cfg.Check(s)) {
				t.Error("AnyFatal reported true on a saturated window")
			}
		})
	}
}

// TestResolveClientCeiling pins the three behaviors: the default ceiling
// follows the pod's measured range, an explicit override survives untouched,
// and a failed read keeps the conservative default rather than guessing.
func TestResolveClientCeiling(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	g := DefaultGuardConfig()
	g.ResolveClientCeiling(func(string) ([]byte, error) {
		return []byte("1025\t65535\n"), nil
	}, log)
	if want := (65535 - 1025 + 1) * 8 / 10; g.ClientConnectionCeiling != want {
		t.Errorf("ceiling = %d, want %d (80%% of the widened range)", g.ClientConnectionCeiling, want)
	}

	// A flag override is a decision already made; the measured range must not
	// undo it. Zero is the strongest override — it disables the guard.
	for _, override := range []int{5000, 0} {
		g = DefaultGuardConfig()
		g.ClientConnectionCeiling = override
		g.ResolveClientCeiling(func(string) ([]byte, error) {
			return []byte("1025 65535"), nil
		}, log)
		if g.ClientConnectionCeiling != override {
			t.Errorf("override %d was replaced with %d", override, g.ClientConnectionCeiling)
		}
	}

	g = DefaultGuardConfig()
	before := g.ClientConnectionCeiling
	g.ResolveClientCeiling(func(string) ([]byte, error) {
		return nil, fmt.Errorf("no proc here")
	}, log)
	if g.ClientConnectionCeiling != before {
		t.Errorf("failed read changed the ceiling to %d, want the default %d kept", g.ClientConnectionCeiling, before)
	}

	g = DefaultGuardConfig()
	g.ResolveClientCeiling(func(string) ([]byte, error) {
		return []byte("garbage"), nil
	}, log)
	if g.ClientConnectionCeiling != before {
		t.Errorf("garbage read changed the ceiling to %d, want the default %d kept", g.ClientConnectionCeiling, before)
	}
}
