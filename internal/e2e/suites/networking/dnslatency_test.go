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

// dnsLatencyEnv enables TestActorDNSLatency, which is a benchmark.
const dnsLatencyEnv = "E2E_DNS_LATENCY"

// dnsLatencyLookups is how many lookups each scenario times.
const dnsLatencyLookups = 2000

type dnsProbe struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type dnsProbeResult struct {
	RCodes          map[string]int `json:"rcodes"`
	LatenciesMicros []int64        `json:"latenciesMicros"`
	Failures        int            `json:"failures"`
	Error           string         `json:"error"`
}

// TestActorDNSLatency logs how long DNS lookups take from inside an actor, for
// a name its EgressPolicy allows and for one it does not. Lookups are sent one
// at a time over UDP. Compare runs under different --dns-egress-policy modes.
func TestActorDNSLatency(t *testing.T) {
	if os.Getenv(dnsLatencyEnv) == "" {
		t.Skipf("%s is not set", dnsLatencyEnv)
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

	probe := func(name string, count int) dnsProbeResult {
		t.Helper()
		payload, err := json.Marshal(dnsProbe{Name: name, Count: count})
		if err != nil {
			t.Fatal(err)
		}
		status, body := postThroughEgressActorUntil(t, ctx, router, actorRef, "/dns", payload, reached)
		if status != http.StatusOK {
			t.Fatalf("DNS probe of %s returned HTTP %d: %s", name, status, body)
		}
		var res dnsProbeResult
		if err := json.Unmarshal(body, &res); err != nil {
			t.Fatalf("decoding the DNS probe result: %v (body %q)", err, body)
		}
		return res
	}

	// The policy reaches the enforcer within seconds of the actor's creation.
	deadline := time.Now().Add(30 * time.Second)
	for probe(allowed, 1).RCodes["RCodeSuccess"] != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not resolve within 30s", allowed)
		}
		time.Sleep(500 * time.Millisecond)
	}

	scenarios := []struct {
		label     string
		name      string
		wantRCode string
	}{
		{label: "allowed", name: allowed, wantRCode: "RCodeSuccess"},
		{label: "blocked", name: "blocked.example.", wantRCode: "RCodeNameError"},
	}
	for _, sc := range scenarios {
		res := probe(sc.name, dnsLatencyLookups)
		if res.RCodes[sc.wantRCode] != dnsLatencyLookups {
			t.Errorf("%s: rcodes = %v with %d failures, want %d %s (first error: %s)",
				sc.label, res.RCodes, res.Failures, dnsLatencyLookups, sc.wantRCode, res.Error)
			continue
		}
		sorted := slices.Sorted(slices.Values(res.LatenciesMicros))
		percentile := func(p float64) time.Duration {
			return time.Duration(sorted[int(p*float64(len(sorted)))]) * time.Microsecond
		}
		t.Logf("DNS lookup latency, %s name, %d lookups: p50=%v p90=%v p99=%v max=%v",
			sc.label, len(sorted), percentile(0.50), percentile(0.90), percentile(0.99),
			time.Duration(sorted[len(sorted)-1])*time.Microsecond)
	}
}
