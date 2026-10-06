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

package networking

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
)

// The EgressPolicy half of egress: the other TestActorEgress* tests give their
// actors an allow-everything policy and prove traffic flows; these give theirs
// a narrow one and prove what does not. A request the gateway can read is
// decided per request on its Host, by the http rules in the clear and the
// https rules once decrypted.

// notTransient stops the retry loop on anything but the 503 a request sees
// while the actor's route is still propagating; a denial is a final answer.
func notTransient(status int, _ []byte) bool { return status != http.StatusServiceUnavailable }

// reached stops the retry loop only on success. A lane expecting the fetch to
// work sees more transients than the 503 above (the minted leaf fails
// verification until kubelet has propagated the CA pool, public origins
// hiccup), all of them 502s the actor cannot tell from a denial.
func reached(status int, _ []byte) bool { return status == http.StatusOK }

// TestActorEgressPolicyDeniesUnlistedHost: the policy names one hostname, so
// the origin is reachable by that name and refused by its address, which is
// the same server.
func TestActorEgressPolicyDeniesUnlistedHost(t *testing.T) {
	ctx := context.Background()
	dataplane := e2e.CurrentAtenetDataplane()
	origin := egressHTTPTarget()
	target := e2e.DeployServerPod(t, ctx, origin)
	allowed := fmt.Sprintf("%s.%s.svc.cluster.local", origin.Name, target.Namespace)

	_, actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-policy", e2e.EgressFixture(), e2e.EgressAllowHTTP(allowed))
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}

	url := "http://" + allowed + "/healthz"
	status, body := fetchThroughEgressActor(t, ctx, router, actorRef, url)
	if status != http.StatusOK {
		t.Fatalf("fetch of the allowed host %s returned HTTP %d, want 200; body: %s", url, status, body)
	}

	url = fmt.Sprintf("http://%s/healthz", target.Address())
	status, body = fetchThroughEgressActorUntil(t, ctx, router, actorRef, url, notTransient)
	if !dataplane.IsEgressPolicyDenied(status, string(body)) {
		t.Fatalf("fetch of the same origin by address %s returned HTTP %d, want an egress-policy denial; body: %s", url, status, body)
	}
	t.Logf("fetch by address was denied as expected: %s", body)
}

// TestActorEgressRequiresPolicy: an actor with no EgressPolicy is denied.
func TestActorEgressRequiresPolicy(t *testing.T) {
	ctx := context.Background()
	dataplane := e2e.CurrentAtenetDataplane()
	target := e2e.DeployServerPod(t, ctx, egressHTTPTarget())

	_, actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-nopolicy", e2e.EgressFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}

	// There is no allowed fetch to prove the route is up, so wait on the
	// demo's readiness endpoint; otherwise a 503 from a route not yet
	// propagated would look like the answer we want.
	waitForActorRoute(t, ctx, router, actorRef)

	url := fmt.Sprintf("http://%s/healthz", target.Address())
	status, body := fetchThroughEgressActorUntil(t, ctx, router, actorRef, url, notTransient)
	if !dataplane.IsEgressPolicyDenied(status, string(body)) {
		t.Fatalf("fetch by an actor with no policy returned HTTP %d, want an egress-policy denial; body: %s", status, body)
	}
	t.Logf("egress was denied as expected: %s", body)
}

// hostnamePolicyActor creates an actor whose policy allows HTTPS to
// example.com and nothing else, and waits until it is routable.
func hostnamePolicyActor(t *testing.T, ctx context.Context) (*e2e.RouterClient, resources.ActorRef) {
	t.Helper()
	_, actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-sni", e2e.EgressFixture(), e2e.EgressAllowHTTPS("example.com"), e2e.EgressAllowPassthrough("example.edu"))
	router := mustRouterClient(t, ctx)
	t.Cleanup(func() { router.Close() })
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	waitForActorRoute(t, ctx, router, actorRef)
	return router, actorRef
}

func isMitmCert(body string) bool {
	// example.* domains use certs signed by Google Trust Services
	// If this substring is not present it means dataplane used a minted cert
	return !strings.Contains(body, "O=Google Trust Services")
}

// TestActorEgressHTTPSByHostnameMITM: the gateway terminates the TLS and decides
// each request by name: example.com 200, example.org 403.
func TestActorEgressHTTPSByHostnameMITM(t *testing.T) {
	ctx := context.Background()
	dataplane := e2e.CurrentAtenetDataplane()
	router, actorRef := hostnamePolicyActor(t, ctx)

	status, body := fetchThroughEgressActorUntil(t, ctx, router, actorRef, "https://example.com/", reached)
	if status != http.StatusOK {
		t.Fatalf("fetch of the allowed host returned HTTP %d, want 200; body: %s", status, body)
	}
	if !isMitmCert(string(body)) {
		t.Fatalf("request did not use minted cert; body: %s", body)
	}
	status, body = fetchThroughEgressActorUntil(t, ctx, router, actorRef, "https://example.org/", notTransient)
	if !dataplane.IsEgressPolicyDenied(status, string(body)) {
		t.Fatalf("fetch of a host outside the policy returned HTTP %d, want an egress-policy denial; body: %s", status, body)
	}
	t.Logf("denied on the decrypted request: %s", body)
}

// TestActorEgressHTTPSByHostnamePassthrough: the gateway acts as TCP proxy fetching
// allowed SNI.
func TestActorEgressHTTPSByHostnamePassthrough(t *testing.T) {
	ctx := context.Background()
	dataplane := e2e.CurrentAtenetDataplane()
	router, actorRef := hostnamePolicyActor(t, ctx)

	status, body := fetchThroughEgressActorUntil(t, ctx, router, actorRef, "https://example.edu/", reached)
	if status != http.StatusOK {
		t.Fatalf("fetch of the allowed host returned HTTP %d, want 200; body: %s", status, body)
	}
	if isMitmCert(string(body)) {
		t.Fatalf("request used minted cert; body: %s", body)
	}
	status, body = fetchThroughEgressActorUntil(t, ctx, router, actorRef, "https://example.org/", notTransient)
	if !dataplane.IsEgressPolicyDenied(status, string(body)) {
		t.Fatalf("fetch of a host outside the policy returned HTTP %d, want an egress-policy denial; body: %s", status, body)
	}
}

// TestActorEgressPolicyCache sends 5 requests to example.com and checks that
// the egress policy cache counters record 4 hits and 1 miss, and that the
// outer CONNECT ext_proc filter is invoked only once.
func TestActorEgressPolicyCache(t *testing.T) {
	ctx := context.Background()
	router, actorRef := hostnamePolicyActor(t, ctx)

	beforeScrape, err := e2e.ScrapeEgressEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeEgressEnvoyMetrics before fetches: %v", err)
	}
	beforeHits, beforeMisses := e2e.EgressPolicyCacheCounts(beforeScrape)
	beforeExtProc := e2e.EgressExtProcStreamCounts(beforeScrape)

	const numRequests = 5
	payload := []byte(`{"url":"https://example.com/","disableKeepAlive":true}`)
	for i := range numRequests {
		status, body := postThroughEgressActorUntil(t, ctx, router, actorRef, "/", payload, reached)
		if status != http.StatusOK {
			t.Fatalf("request %d to example.com returned HTTP %d, want 200; body: %s", i+1, status, body)
		}
	}

	if !e2e.CurrentAtenetDataplane().IsDataplaneConcurrencyDisabled() {
		t.Log("skipping counter verification: dataplane concurrency is not disabled")
		return
	}

	afterScrape, err := e2e.ScrapeEgressEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeEgressEnvoyMetrics after fetches: %v", err)
	}
	afterHits, afterMisses := e2e.EgressPolicyCacheCounts(afterScrape)
	afterExtProc := e2e.EgressExtProcStreamCounts(afterScrape)

	const (
		wantHits         = 4
		wantMisses       = 1
		wantExtProcCalls = 1
	)
	if got := afterHits - beforeHits; got != wantHits {
		t.Errorf("egress policy cache hits delta = %d, want %d", got, wantHits)
	}
	if got := afterMisses - beforeMisses; got != wantMisses {
		t.Errorf("egress policy cache misses delta = %d, want %d", got, wantMisses)
	}
	if got := afterExtProc[e2e.EgressExtProcIdentityStatPrefix] - beforeExtProc[e2e.EgressExtProcIdentityStatPrefix]; got != wantExtProcCalls {
		t.Errorf("egress_identity ext_proc streams_started delta = %d, want %d", got, wantExtProcCalls)
	}
}

// TestActorEgressPolicyCacheExpiration verifies that cached egress policy
// entries expire after the configured cache_ttl (5s default), triggering a
// cache miss and a new ext_proc lookup on the next request.
func TestActorEgressPolicyCacheExpiration(t *testing.T) {
	ctx := context.Background()
	router, actorRef := hostnamePolicyActor(t, ctx)

	beforeScrape, err := e2e.ScrapeEgressEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeEgressEnvoyMetrics before fetches: %v", err)
	}
	beforeHits, beforeMisses := e2e.EgressPolicyCacheCounts(beforeScrape)
	beforeExtProc := e2e.EgressExtProcStreamCounts(beforeScrape)

	payload := []byte(`{"url":"https://example.com/","disableKeepAlive":true}`)
	for i := range 2 {
		status, body := postThroughEgressActorUntil(t, ctx, router, actorRef, "/", payload, reached)
		if status != http.StatusOK {
			t.Fatalf("pre-expiration request %d to example.com returned HTTP %d, want 200; body: %s", i+1, status, body)
		}
	}

	// Wait longer than the 5s default cache_ttl so the cached policy expires.
	time.Sleep(6 * time.Second)

	for i := range 2 {
		status, body := postThroughEgressActorUntil(t, ctx, router, actorRef, "/", payload, reached)
		if status != http.StatusOK {
			t.Fatalf("post-expiration request %d to example.com returned HTTP %d, want 200; body: %s", i+1, status, body)
		}
	}

	if !e2e.CurrentAtenetDataplane().IsDataplaneConcurrencyDisabled() {
		t.Log("skipping counter verification: dataplane concurrency is not disabled")
		return
	}

	afterScrape, err := e2e.ScrapeEgressEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeEgressEnvoyMetrics after fetches: %v", err)
	}
	afterHits, afterMisses := e2e.EgressPolicyCacheCounts(afterScrape)
	afterExtProc := e2e.EgressExtProcStreamCounts(afterScrape)

	const (
		wantHits         = 2
		wantMisses       = 2
		wantExtProcCalls = 2
	)
	if got := afterHits - beforeHits; got != wantHits {
		t.Errorf("egress policy cache hits delta = %d, want %d", got, wantHits)
	}
	if got := afterMisses - beforeMisses; got != wantMisses {
		t.Errorf("egress policy cache misses delta = %d, want %d", got, wantMisses)
	}
	if got := afterExtProc[e2e.EgressExtProcIdentityStatPrefix] - beforeExtProc[e2e.EgressExtProcIdentityStatPrefix]; got != wantExtProcCalls {
		t.Errorf("egress_identity ext_proc streams_started delta = %d, want %d", got, wantExtProcCalls)
	}
}

// fetchThroughEgressActorUntil is fetchThroughEgressActor with the caller
// deciding which answer is final.
func fetchThroughEgressActorUntil(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef, url string, done func(status int, body []byte) bool) (int, []byte) {
	t.Helper()
	payload := []byte(fmt.Sprintf(`{"url":%q}`, url))
	return postThroughEgressActorUntil(t, ctx, router, actorRef, "/", payload, done)
}

// waitForActorRoute polls the demo app's readiness endpoint through the router
// until it answers, which is when the actor's route has reached the ingress
// dataplane.
func waitForActorRoute(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		response, err := router.Get(ctx, actorRef, "/readyz")
		if err != nil {
			t.Fatalf("GET /readyz on the egress Actor through ingress: %v", err)
		}
		response.Body.Close()
		if response.StatusCode == http.StatusOK {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the egress Actor's route did not come up: GET /readyz returned HTTP %d", response.StatusCode)
		}
		time.Sleep(time.Second)
	}
}
