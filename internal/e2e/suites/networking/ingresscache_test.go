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
	"io"
	"net/http"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/grpcechopb"
	"go.ate.dev/substrate/internal/resources"
)

// TestIngressCache_HitAndTTL verifies that requests to an actor hit the ingress
// cache after the first request, record hit/miss metrics on atenet-router, and
// expire after the cache TTL (5 seconds).
func TestIngressCache_HitAndTTL(t *testing.T) {
	e2e.RequireIngressCache(t)
	ctx := context.Background()
	_, actorName, _ := createAndResumeSubstrateActor(t, ctx, "cache-hit", e2e.SubstrateCounterFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()

	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}

	// 1. Initial request: Warm up / establish route.
	waitForRouteReady(t, "Warm up actor route", func() (*http.Response, error) {
		return router.Get(ctx, actorRef, "/readyz")
	})

	// Scrape baseline router metrics.
	scrape1, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeRouterEnvoyMetrics baseline: %v", err)
	}
	missBefore := e2e.RouterCacheCounterValue(scrape1, "ate_ingress_cache_miss")
	hitBefore := e2e.RouterCacheCounterValue(scrape1, "ate_ingress_cache_hit")

	// 2. Fire 3 rapid GET requests within the 5s TTL.
	for i := range 3 {
		resp, err := router.Get(ctx, actorRef, "/readyz")
		if err != nil {
			t.Fatalf("rapid GET %d: %v", i, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("rapid GET %d status %d, want 200", i, resp.StatusCode)
		}
	}

	// Scrape metrics after rapid requests: hits must have increased by at least 3.
	scrape2, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeRouterEnvoyMetrics after rapid requests: %v", err)
	}
	hitAfter := e2e.RouterCacheCounterValue(scrape2, "ate_ingress_cache_hit")
	if hitAfter < hitBefore+3 {
		t.Fatalf("expected at least 3 cache hits, got hit count before=%.0f, after=%.0f", hitBefore, hitAfter)
	}

	// 3. Wait 6 seconds for the 5-second cache TTL to expire.
	time.Sleep(6 * time.Second)

	// Send a request after TTL expiration: must be a cache miss that re-populates the cache.
	resp, err := router.Get(ctx, actorRef, "/readyz")
	if err != nil {
		t.Fatalf("GET after TTL: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET after TTL returned status %d, want 200", resp.StatusCode)
	}

	scrape3, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeRouterEnvoyMetrics after TTL: %v", err)
	}
	missAfter := e2e.RouterCacheCounterValue(scrape3, "ate_ingress_cache_miss")
	if missAfter <= missBefore {
		t.Fatalf("expected cache miss count to increase after TTL, before=%.0f, after=%.0f", missBefore, missAfter)
	}
}

// TestIngressCache_WorkerDisconnectionEvictsWithoutReplay tests that when an
// actor's connection terminates or upstream closes (503 UC), ingress-cache evicts
// the stale binding without proxy-side stream replay (erring on the side of
// caution against duplicate side effects), allowing a single client retry to
// succeed immediately via ext_proc without waiting for cache TTL.
func TestIngressCache_WorkerDisconnectionEvictsWithoutReplay(t *testing.T) {
	e2e.RequireIngressCache(t)
	ctx := context.Background()
	clients := e2e.GetClients()
	_, actorName, _ := createAndResumeSubstrateActor(t, ctx, "cache-evict", e2e.SubstrateCounterFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()

	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}

	// 1. Warm route and cache.
	waitForRouteReady(t, "Warm up actor route", func() (*http.Response, error) {
		return router.Get(ctx, actorRef, "/readyz")
	})

	scrapeBefore, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeRouterEnvoyMetrics before suspend: %v", err)
	}
	replayBefore := e2e.RouterCacheCounterValue(scrapeBefore, "ate_ingress_cache_replay")
	evictBefore := e2e.RouterCacheCounterValue(scrapeBefore, "ate_ingress_cache_eviction")

	// 2. Suspend the actor so the worker immediately deactivates it.
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: networkingAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	waitForActorState(t, ctx, clients, networkingAtespace, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	// 3. Send a GET request. The router connects to the old worker which closes
	// the tunnel (503 UC). Per design, mid-flight terminations (UC) are NOT replayed
	// to prevent duplicate side-effects. The client receives 503, and the cache
	// entry is evicted.
	resp, err := router.Get(ctx, actorRef, "/readyz")
	if err != nil {
		t.Logf("GET encountered error on terminated connection as expected: %v", err)
	} else {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("expected status 503 UC on mid-flight connection termination, got %d", resp.StatusCode)
		}
	}

	// 4. Verify eviction counter incremented and replay counter did NOT increment.
	scrapeAfter, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeRouterEnvoyMetrics after suspend: %v", err)
	}
	replayAfter := e2e.RouterCacheCounterValue(scrapeAfter, "ate_ingress_cache_replay")
	evictAfter := e2e.RouterCacheCounterValue(scrapeAfter, "ate_ingress_cache_eviction")
	if evictAfter <= evictBefore {
		t.Fatalf("expected ate_ingress_cache_eviction to increment, before=%.0f, after=%.0f", evictBefore, evictAfter)
	}
	if replayAfter != replayBefore {
		t.Fatalf("expected ate_ingress_cache_replay NOT to increment for UC, before=%.0f, after=%.0f", replayBefore, replayAfter)
	}

	// 5. A single client retry now succeeds cleanly via ext_proc, which resumes the actor.
	retryResp, err := router.Get(ctx, actorRef, "/readyz")
	if err != nil {
		t.Fatalf("retry GET failed: %v", err)
	}
	defer retryResp.Body.Close()
	if retryResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(retryResp.Body)
		t.Fatalf("retry GET returned status %d, want 200; body: %s", retryResp.StatusCode, body)
	}
}

// TestIngressCache_BodyBypassesCache verifies that a request with a body is
// never served from the cache: even with a stale entry for a suspended actor, it
// goes through ext_proc, which resumes the actor, and counts as a bypass rather
// than a hit.
func TestIngressCache_BodyBypassesCache(t *testing.T) {
	e2e.RequireIngressCache(t)
	ctx := context.Background()
	clients := e2e.GetClients()
	_, actorName, _ := createAndResumeSubstrateActor(t, ctx, "cache-body", e2e.SubstrateCounterFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()

	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}

	// 1. Warm route and cache with a bodiless request.
	waitForRouteReady(t, "Warm up actor route", func() (*http.Response, error) {
		return router.Get(ctx, actorRef, "/readyz")
	})

	// 2. Suspend the actor. The cache entry still points at its old worker.
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: networkingAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	waitForActorState(t, ctx, clients, networkingAtespace, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	scrapeBefore, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeRouterEnvoyMetrics before POST: %v", err)
	}
	hitBefore := e2e.RouterCacheCounterValue(scrapeBefore, "ate_ingress_cache_hit")
	bypassBefore := e2e.RouterCacheCounterValue(scrapeBefore, "ate_ingress_cache_bypass")

	// 3. A POST with a body skips the stale entry and succeeds through ext_proc.
	resp, err := router.PostJSON(ctx, actorRef, "/count", []byte(`{"count": 1}`))
	if err != nil {
		t.Fatalf("POST with body failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST with body returned status %d, want 200; body: %s", resp.StatusCode, body)
	}

	// 4. It counted as a bypass, not a hit.
	scrapeAfter, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeRouterEnvoyMetrics after POST: %v", err)
	}
	if hitAfter := e2e.RouterCacheCounterValue(scrapeAfter, "ate_ingress_cache_hit"); hitAfter != hitBefore {
		t.Fatalf("expected ate_ingress_cache_hit unchanged for a request with a body, before=%.0f, after=%.0f", hitBefore, hitAfter)
	}
	if bypassAfter := e2e.RouterCacheCounterValue(scrapeAfter, "ate_ingress_cache_bypass"); bypassAfter <= bypassBefore {
		t.Fatalf("expected ate_ingress_cache_bypass to increment, before=%.0f, after=%.0f", bypassBefore, bypassAfter)
	}
}

// TestIngressCache_UntrustedCachedHeaderStripped verifies that an external untrusted
// client cannot forge ate-endpoint-cached: 1 on an un-cached actor to bypass
// ext_proc resolution or corrupt routing.
func TestIngressCache_UntrustedCachedHeaderStripped(t *testing.T) {
	e2e.RequireIngressCache(t)
	ctx := context.Background()
	_, actorName, _ := createAndResumeSubstrateActor(t, ctx, "cache-spoof", e2e.SubstrateCounterFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()

	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}

	// 1. Warm up the route to ensure the actor is ready.
	waitForRouteReady(t, "Warm up actor route", func() (*http.Response, error) {
		return router.Get(ctx, actorRef, "/readyz")
	})

	// 2. Wait 6 seconds for the 5-second cache entry to expire, ensuring a cache miss.
	time.Sleep(6 * time.Second)

	// 3. Send request with forged ate-endpoint-cached: 1 on the expired (uncached) actor.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, router.BaseURL()+"/readyz", nil)
	if err != nil {
		t.Fatalf("creating request: %v", err)
	}
	req.Header.Set(atenet.TargetActorHeader, actorRef.String())
	req.Header.Set("ate-endpoint-cached", "1")

	scrapeBefore, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeRouterEnvoyMetrics before spoof request: %v", err)
	}
	missBefore := e2e.RouterCacheCounterValue(scrapeBefore, "ate_ingress_cache_miss")

	resp, err := router.DoReq(req)
	if err != nil {
		t.Fatalf("request with forged ate-endpoint-cached failed: %v", err)
	}
	defer resp.Body.Close()

	// Ingress cache rejects untrusted downstream requests containing ate-endpoint-cached
	// on a cache miss with HTTP 400 Bad Request, preventing ext_proc bypass.
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("request with forged header returned status %d, want 400; body: %s", resp.StatusCode, body)
	}

	scrapeAfter, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeRouterEnvoyMetrics after spoof request: %v", err)
	}
	missAfter := e2e.RouterCacheCounterValue(scrapeAfter, "ate_ingress_cache_miss")
	if missAfter <= missBefore {
		t.Fatalf("expected cache miss counter to increment for spoofed request, before=%.0f, after=%.0f", missBefore, missAfter)
	}
}

// TestIngressCache_DisabledByDefault verifies that without the IngressCache
// preview gate a forged ate-endpoint-cached header has no effect: the router
// has no route that skips ext_proc, so the request is resolved and served
// normally, and the ingress-cache filter never sees it.
func TestIngressCache_DisabledByDefault(t *testing.T) {
	if preview.IsEnabled(preview.GateIngressCache) {
		t.Skipf("preview gate %q is enabled; this test covers the default install", preview.GateIngressCache)
	}
	ctx := context.Background()
	_, actorName, _ := createAndResumeSubstrateActor(t, ctx, "cache-off", e2e.SubstrateCounterFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()

	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	waitForRouteReady(t, "Warm up actor route", func() (*http.Response, error) {
		return router.Get(ctx, actorRef, "/readyz")
	})

	countCacheMetrics := e2e.CurrentAtenetDataplane().SupportsIngressCache()
	var lookupsBefore float64
	if countCacheMetrics {
		scrape, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
		if err != nil {
			t.Fatalf("ScrapeRouterEnvoyMetrics before forged request: %v", err)
		}
		lookupsBefore = e2e.RouterCacheCounterValue(scrape, "ate_ingress_cache_hit") +
			e2e.RouterCacheCounterValue(scrape, "ate_ingress_cache_miss") +
			e2e.RouterCacheCounterValue(scrape, "ate_ingress_cache_bypass")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, router.BaseURL()+"/readyz", nil)
	if err != nil {
		t.Fatalf("creating request: %v", err)
	}
	req.Header.Set(atenet.TargetActorHeader, actorRef.String())
	req.Header.Set("ate-endpoint-cached", "1")
	resp, err := router.DoReq(req)
	if err != nil {
		t.Fatalf("request with forged ate-endpoint-cached failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("request with forged header returned status %d, want 200; body: %s", resp.StatusCode, body)
	}

	if countCacheMetrics {
		scrape, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
		if err != nil {
			t.Fatalf("ScrapeRouterEnvoyMetrics after forged request: %v", err)
		}
		lookupsAfter := e2e.RouterCacheCounterValue(scrape, "ate_ingress_cache_hit") +
			e2e.RouterCacheCounterValue(scrape, "ate_ingress_cache_miss") +
			e2e.RouterCacheCounterValue(scrape, "ate_ingress_cache_bypass")
		if lookupsAfter != lookupsBefore {
			t.Fatalf("ingress cache lookups changed with the gate off, before=%.0f, after=%.0f", lookupsBefore, lookupsAfter)
		}
	}
}

// TestIngressCache_GRPCBypassesCache verifies that gRPC calls, which always
// carry a body, never use the cache: after the actor is suspended, the next call
// goes through ext_proc, which resumes the actor, instead of hitting the stale
// entry.
func TestIngressCache_GRPCBypassesCache(t *testing.T) {
	e2e.RequireIngressCache(t)
	if _, err := e2e.CheckEnv("KO_DOCKER_REPO"); err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	fixture := deployGRPCEchoTemplate(t, ctx)
	_, actorName, _ := createAndResumeSubstrateActor(t, ctx, "cache-grpc", fixture)
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	ctx = metadata.AppendToOutgoingContext(ctx,
		atenet.TargetActorHeader, actorRef.String(),
	)

	rc := mustRouterClient(t, ctx)
	t.Cleanup(rc.Close)
	conn, err := grpc.NewClient(rc.Address(), grpc.WithTransportCredentials(credentials.NewTLS(rc.TLSConfig())))
	if err != nil {
		t.Fatalf("creating gRPC client: %v", err)
	}
	defer conn.Close()
	client := grpcechopb.NewEchoClient(conn)

	// 1. A successful call resolves the route; its response is learned into the cache.
	const message = "grpc bypasses the cache"
	waitForGRPCRouteReady(t, ctx, client, message)

	// 2. Suspend the actor. The cache entry still points at its old worker.
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: networkingAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	waitForActorState(t, ctx, clients, networkingAtespace, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	scrapeBefore, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeRouterEnvoyMetrics before call: %v", err)
	}
	hitBefore := e2e.RouterCacheCounterValue(scrapeBefore, "ate_ingress_cache_hit")
	bypassBefore := e2e.RouterCacheCounterValue(scrapeBefore, "ate_ingress_cache_bypass")

	// 3. The next call skips the stale entry and succeeds through ext_proc.
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := client.Echo(callCtx, &grpcechopb.EchoRequest{Message: message})
	if err != nil {
		t.Fatalf("Echo after suspend failed: %v", err)
	}
	if resp.GetMessage() != message {
		t.Fatalf("Echo after suspend got %q, want %q", resp.GetMessage(), message)
	}

	// 4. It counted as a bypass, not a hit.
	scrapeAfter, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeRouterEnvoyMetrics after call: %v", err)
	}
	if hitAfter := e2e.RouterCacheCounterValue(scrapeAfter, "ate_ingress_cache_hit"); hitAfter != hitBefore {
		t.Fatalf("expected ate_ingress_cache_hit unchanged for a gRPC call, before=%.0f, after=%.0f", hitBefore, hitAfter)
	}
	if bypassAfter := e2e.RouterCacheCounterValue(scrapeAfter, "ate_ingress_cache_bypass"); bypassAfter <= bypassBefore {
		t.Fatalf("expected ate_ingress_cache_bypass to increment, before=%.0f, after=%.0f", bypassBefore, bypassAfter)
	}
}

func waitForActorState(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, name string, want ateapi.ActorState) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: atespace, Name: name},
		})
		if err == nil && resp.GetStatus().GetState() == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for actor %s/%s to reach %v", atespace, name, want)
}
