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
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"k8s.io/apimachinery/pkg/util/wait"

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

// TestIngressCache_StaleHitReplaysAfterSuspend verifies that a bodiless request
// that hits a stale entry after its actor was suspended while idle is replayed
// through ext_proc, which resumes the actor, so the client gets a 200. On the
// default CONNECT path, atunnel refuses the tunnel and the router reports a
// 503 UC; the tunnel_watcher network filter marks the entry stale, and
// it isn't spoiled because no request was cut off mid-flight.
func TestIngressCache_StaleHitReplaysAfterSuspend(t *testing.T) {
	e2e.RequireIngressCache(t)
	ctx := context.Background()
	clients := e2e.GetClients()
	_, actorName, _ := createAndResumeSubstrateActor(t, ctx, "cache-stale", e2e.SubstrateCounterFixture())
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
	staleBefore := e2e.RouterCacheCounterValue(scrapeBefore, "ate_ingress_cache_marked_stale")
	spoiledBefore := e2e.RouterCacheCounterValue(scrapeBefore, "ate_ingress_cache_marked_spoiled")

	// 2. Suspend the actor while it is idle. The cache entry still points at its old worker.
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: networkingAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	waitForActorState(t, ctx, clients, networkingAtespace, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	// 3. The stale hit is refused before reaching the actor and replayed through ext_proc.
	resp, err := router.Get(ctx, actorRef, "/readyz")
	if err != nil {
		t.Fatalf("GET on stale entry failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET on stale entry returned status %d, want 200 after replay; body: %s", resp.StatusCode, body)
	}

	// 4. The request was replayed, its entry marked stale, and nothing marked spoiled.
	scrapeAfter, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeRouterEnvoyMetrics after GET: %v", err)
	}
	if replayAfter := e2e.RouterCacheCounterValue(scrapeAfter, "ate_ingress_cache_replay"); replayAfter <= replayBefore {
		t.Fatalf("expected ate_ingress_cache_replay to increment, before=%.0f, after=%.0f", replayBefore, replayAfter)
	}
	if staleAfter := e2e.RouterCacheCounterValue(scrapeAfter, "ate_ingress_cache_marked_stale"); staleAfter <= staleBefore {
		t.Fatalf("expected ate_ingress_cache_marked_stale to increment, before=%.0f, after=%.0f", staleBefore, staleAfter)
	}
	if spoiledAfter := e2e.RouterCacheCounterValue(scrapeAfter, "ate_ingress_cache_marked_spoiled"); spoiledAfter != spoiledBefore {
		t.Fatalf("expected ate_ingress_cache_marked_spoiled unchanged for an idle suspend, before=%.0f, after=%.0f", spoiledBefore, spoiledAfter)
	}
}

// TestIngressCache_SuspendMidRequestIsNotReplayed verifies that a cache hit cut
// off by a suspend after it reached the actor gets a 503 without a replay, and
// that the tunnel_watcher network filter marks its entry spoiled. No refusal
// marks the entry stale here, so the 503 doesn't depend on the spoiled mark: the
// counter checks the mark itself.
func TestIngressCache_SuspendMidRequestIsNotReplayed(t *testing.T) {
	e2e.RequireIngressCache(t)
	ctx := context.Background()
	clients := e2e.GetClients()
	fixture := deployWebsocketFixture(t, ctx)
	_, actorName, _ := createAndResumeSubstrateActor(t, ctx, "cache-spoil", fixture)
	router := mustRouterClient(t, ctx)
	defer router.Close()

	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}

	// 1. Warm the cache. The router client reuses this connection for the next
	// request, so it lands on the same Envoy worker thread and hits the entry.
	waitForRouteReady(t, "Warm up actor route", func() (*http.Response, error) {
		return router.Get(ctx, actorRef, "/readyz")
	})

	scrapeBefore, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeRouterEnvoyMetrics before request: %v", err)
	}
	spoiledBefore := e2e.RouterCacheCounterValue(scrapeBefore, "ate_ingress_cache_marked_spoiled")

	// 2. Send a request the actor never answers.
	type result struct {
		status int
		err    error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := router.Get(ctx, actorRef, "/hang")
		if err != nil {
			done <- result{err: err}
			return
		}
		resp.Body.Close()
		done <- result{status: resp.StatusCode}
	}()

	// 3. Wait until it reached the actor, asking on another connection.
	err = wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		resp, err := router.Get(ctx, actorRef, "/hanging")
		if err != nil {
			return false, err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return false, err
		}
		return resp.StatusCode == http.StatusOK && strings.TrimSpace(string(body)) != "0", nil
	})
	if err != nil {
		t.Fatalf("waiting for /hang to reach the actor: %v", err)
	}

	// 4. Suspend the actor with the request in flight.
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: networkingAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}

	// 5. A replay would resume the actor and hang again, so a 503 means no replay.
	// Don't check the replay counter: other tests running in parallel can replay.
	res := <-done
	if res.err != nil {
		t.Fatalf("GET /hang failed: %v", res.err)
	}
	if res.status != http.StatusServiceUnavailable {
		t.Fatalf("GET /hang returned status %d, want 503 without a replay", res.status)
	}

	scrapeAfter, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeRouterEnvoyMetrics after request: %v", err)
	}
	if spoiledAfter := e2e.RouterCacheCounterValue(scrapeAfter, "ate_ingress_cache_marked_spoiled"); spoiledAfter <= spoiledBefore {
		t.Fatalf("expected ate_ingress_cache_marked_spoiled to increment, before=%.0f, after=%.0f", spoiledBefore, spoiledAfter)
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
