// Copyright 2026 The Agent Substrate Authors
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

package networking

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"slices"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/resources"
)

// dnsLatencyOutEnv names the file TestActorDNSLatency writes its results to.
// The test is a benchmark and runs only when this is set.
const dnsLatencyOutEnv = "E2E_DNS_LATENCY_OUT"

// dnsLatencyModeEnv labels the results with the cluster's --dns-egress-policy.
const dnsLatencyModeEnv = "E2E_DNS_EGRESS_POLICY"

// egressIdleTimeout is longer than the egress gateway closes an idle atunnel
// connection after (30s), so a lookup after it pays a new mTLS handshake when
// DNS goes to the gateway.
const egressIdleTimeout = 35 * time.Second

type dnsProbe struct {
	Name        string `json:"name"`
	Count       int    `json:"count"`
	Concurrency int    `json:"concurrency"`
	TCP         bool   `json:"tcp,omitempty"`
	UniqueLabel bool   `json:"uniqueLabel,omitempty"`
}

type dnsProbeResult struct {
	Nameserver      string         `json:"nameserver"`
	RCodes          map[string]int `json:"rcodes"`
	ExtendedErrors  map[int]int    `json:"extendedErrors"`
	LatenciesMicros []int64        `json:"latenciesMicros"`
	Failures        int            `json:"failures"`
	Error           string         `json:"error"`
}

type dnsLatencySummary struct {
	Scenario       string         `json:"scenario"`
	Queries        int            `json:"queries"`
	Failures       int            `json:"failures"`
	RCodes         map[string]int `json:"rcodes"`
	ExtendedErrors map[int]int    `json:"extendedErrors,omitempty"`
	P50Micros      int64          `json:"p50Micros"`
	P90Micros      int64          `json:"p90Micros"`
	P99Micros      int64          `json:"p99Micros"`
	MaxMicros      int64          `json:"maxMicros"`
	MeanMicros     int64          `json:"meanMicros"`
	// ElapsedMillis is the wall time of the scenario inside the actor.
	ElapsedMillis int64 `json:"elapsedMillis"`
}

// TestActorDNSLatency measures DNS lookups from inside an actor: an allowed
// name sequentially and concurrently, a blocked name, lookups over TCP, and
// lookups after the gateway connection has idled out.
func TestActorDNSLatency(t *testing.T) {
	out := os.Getenv(dnsLatencyOutEnv)
	if out == "" {
		t.Skipf("%s is not set", dnsLatencyOutEnv)
	}
	ctx := context.Background()
	origin := egressHTTPTarget()
	target := e2e.DeployServerPod(t, ctx, origin)
	allowed := fmt.Sprintf("%s.%s.svc.cluster.local.", origin.Name, target.Namespace)
	wildcard := fmt.Sprintf("*.%s.svc.cluster.local", target.Namespace)

	_, actorName, _ := createAndResumeActorWithEgress(t, ctx, "dns-latency", e2e.EgressFixture(), e2e.EgressAllowHTTP(wildcard))
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	waitForActorRoute(t, ctx, router, actorRef)

	probe := func(req dnsProbe) (dnsProbeResult, time.Duration) {
		t.Helper()
		payload, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		status, body := postThroughEgressActorUntil(t, ctx, router, actorRef, "/dns", payload, reached)
		elapsed := time.Since(start)
		if status != http.StatusOK {
			t.Fatalf("DNS probe %+v returned HTTP %d: %s", req, status, body)
		}
		var res dnsProbeResult
		if err := json.Unmarshal(body, &res); err != nil {
			t.Fatalf("decoding the DNS probe result: %v (body %q)", err, body)
		}
		return res, elapsed
	}

	// The policy reaches the enforcer within seconds of the actor's creation.
	deadline := time.Now().Add(30 * time.Second)
	for {
		res, _ := probe(dnsProbe{Name: allowed, Count: 1, Concurrency: 1})
		if res.RCodes["RCodeSuccess"] == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not resolve within 30s: %+v", allowed, res)
		}
		time.Sleep(500 * time.Millisecond)
	}

	var summaries []dnsLatencySummary
	record := func(scenario, wantRCode string, res dnsProbeResult, elapsed time.Duration) {
		t.Helper()
		s := summarizeDNSLatency(scenario, res, elapsed)
		summaries = append(summaries, s)
		t.Logf("%-22s n=%d failures=%d p50=%dµs p90=%dµs p99=%dµs max=%dµs rcodes=%v ede=%v",
			s.Scenario, s.Queries, s.Failures, s.P50Micros, s.P90Micros, s.P99Micros, s.MaxMicros, s.RCodes, s.ExtendedErrors)
		if res.RCodes[wantRCode] != len(res.LatenciesMicros) {
			t.Errorf("%s: rcodes = %v, want every answer %s (first error: %s)", scenario, res.RCodes, wantRCode, res.Error)
		}
	}

	scenarios := []struct {
		name      string
		wantRCode string
		req       dnsProbe
	}{
		{"warm_sequential", "RCodeSuccess", dnsProbe{Name: allowed, Count: 2000, Concurrency: 1}},
		{"warm_concurrency_16", "RCodeSuccess", dnsProbe{Name: allowed, Count: 2000, Concurrency: 16}},
		{"warm_concurrency_48", "RCodeSuccess", dnsProbe{Name: allowed, Count: 2000, Concurrency: 48}},
		{"blocked_sequential", "RCodeNameError", dnsProbe{Name: "blocked.example.", Count: 2000, Concurrency: 1, UniqueLabel: true}},
		{"tcp_sequential", "RCodeSuccess", dnsProbe{Name: allowed, Count: 500, Concurrency: 1, TCP: true}},
	}
	for _, sc := range scenarios {
		res, elapsed := probe(sc.req)
		record(sc.name, sc.wantRCode, res, elapsed)
	}

	// Each cold lookup is its own probe after the connection has idled out.
	cold := dnsProbeResult{RCodes: map[string]int{}}
	const coldLookups = 8
	coldStart := time.Now()
	for range coldLookups {
		time.Sleep(egressIdleTimeout)
		res, _ := probe(dnsProbe{Name: allowed, Count: 1, Concurrency: 1})
		cold.LatenciesMicros = append(cold.LatenciesMicros, res.LatenciesMicros...)
		cold.Failures += res.Failures
		for code, n := range res.RCodes {
			cold.RCodes[code] += n
		}
	}
	record("cold_after_idle", "RCodeSuccess", cold, time.Since(coldStart))

	report, err := json.MarshalIndent(map[string]any{
		"dnsEgressPolicy": os.Getenv(dnsLatencyModeEnv),
		"sandboxClass":    e2e.FixtureName("gvisor"),
		"scenarios":       summaries,
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out, append(report, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func summarizeDNSLatency(scenario string, res dnsProbeResult, elapsed time.Duration) dnsLatencySummary {
	s := dnsLatencySummary{
		Scenario:       scenario,
		Queries:        len(res.LatenciesMicros) + res.Failures,
		Failures:       res.Failures,
		RCodes:         res.RCodes,
		ExtendedErrors: res.ExtendedErrors,
		ElapsedMillis:  elapsed.Milliseconds(),
	}
	if len(res.LatenciesMicros) == 0 {
		return s
	}
	sorted := slices.Sorted(slices.Values(res.LatenciesMicros))
	percentile := func(p float64) int64 {
		return sorted[min(len(sorted)-1, int(p*float64(len(sorted))))]
	}
	var sum int64
	for _, v := range sorted {
		sum += v
	}
	s.P50Micros, s.P90Micros, s.P99Micros = percentile(0.50), percentile(0.90), percentile(0.99)
	s.MaxMicros = sorted[len(sorted)-1]
	s.MeanMicros = sum / int64(len(sorted))
	return s
}
