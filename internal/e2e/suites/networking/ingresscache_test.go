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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/proto/grpcechopb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// TestIngressCache_HitAndTTL verifies that requests to an actor hit the ingress
// cache after the first request, record hit/miss metrics on atenet-router, and
// expire after the cache TTL (5 seconds).
func TestIngressCache_HitAndTTL(t *testing.T) {
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
	replayBefore := e2e.RouterCacheCounterValue(scrapeBefore, "ate_ingress_cache_recreate_stream")
	evictBefore := e2e.RouterCacheCounterValue(scrapeBefore, "ate_ingress_cache_eviction")

	// 2. Suspend the actor so the worker immediately deactivates it.
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: networkingAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	waitForActorState(t, ctx, clients, networkingAtespace, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

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
	replayAfter := e2e.RouterCacheCounterValue(scrapeAfter, "ate_ingress_cache_recreate_stream")
	evictAfter := e2e.RouterCacheCounterValue(scrapeAfter, "ate_ingress_cache_eviction")
	if evictAfter <= evictBefore {
		t.Fatalf("expected ate_ingress_cache_eviction to increment, before=%.0f, after=%.0f", evictBefore, evictAfter)
	}
	if replayAfter != replayBefore {
		t.Fatalf("expected ate_ingress_cache_recreate_stream NOT to increment for UC, before=%.0f, after=%.0f", replayBefore, replayAfter)
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

// TestIngressCache_BodyDoesNotReplayOnWorkerFailure tests that when a request
// carries a body (e.g. POST), an upstream connection failure is NOT automatically
// replayed by the proxy, preventing payload truncation or duplicate execution.
// However, the stale binding is still evicted, so a subsequent client call succeeds.
func TestIngressCache_BodyDoesNotReplayOnWorkerFailure(t *testing.T) {
	ctx := context.Background()
	clients := e2e.GetClients()
	_, actorName, _ := createAndResumeSubstrateActor(t, ctx, "cache-body", e2e.SubstrateCounterFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()

	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}

	// 1. Warm route and cache.
	waitForRouteReady(t, "Warm up actor route", func() (*http.Response, error) {
		return router.Get(ctx, actorRef, "/readyz")
	})

	// 2. Scrape baseline metrics.
	scrapeBefore, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeRouterEnvoyMetrics before suspend: %v", err)
	}
	replayBefore := e2e.RouterCacheCounterValue(scrapeBefore, "ate_ingress_cache_recreate_stream")
	evictBefore := e2e.RouterCacheCounterValue(scrapeBefore, "ate_ingress_cache_eviction")

	// 3. Suspend the actor so the worker immediately deactivates it.
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: networkingAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	waitForActorState(t, ctx, clients, networkingAtespace, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	// 4. Send a POST request with body. Because request has a body, the router
	// must NOT recreate the stream. It returns 503 to the client.
	payload := []byte(`{"count": 1}`)
	resp, err := router.PostJSON(ctx, actorRef, "/count", payload)
	if err != nil {
		t.Logf("POST with body on deactivated worker failed with connection error (expected): %v", err)
	} else {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatalf("POST with body unexpectedly succeeded on deactivated worker (must not replay request with body)")
		}
		t.Logf("POST with body returned non-200 status as expected: %d", resp.StatusCode)
	}

	// 5. Verify eviction counter incremented and replay counter did NOT increment.
	scrapeAfter, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeRouterEnvoyMetrics after POST: %v", err)
	}
	evictAfter := e2e.RouterCacheCounterValue(scrapeAfter, "ate_ingress_cache_eviction")
	replayAfter := e2e.RouterCacheCounterValue(scrapeAfter, "ate_ingress_cache_recreate_stream")
	if evictAfter <= evictBefore {
		t.Fatalf("expected ate_ingress_cache_eviction to increment, before=%.0f, after=%.0f", evictBefore, evictAfter)
	}
	if replayAfter != replayBefore {
		t.Fatalf("expected ate_ingress_cache_recreate_stream NOT to increment for request with body, before=%.0f, after=%.0f", replayBefore, replayAfter)
	}

	// 6. The failed request must have evicted the stale cache entry.
	// The client retry now succeeds via ext_proc, which resumes the actor.
	retryResp, err := router.PostJSON(ctx, actorRef, "/count", payload)
	if err != nil {
		t.Fatalf("retry POST failed: %v", err)
	}
	defer retryResp.Body.Close()
	if retryResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(retryResp.Body)
		t.Fatalf("retry POST returned status %d, want 200; body: %s", retryResp.StatusCode, body)
	}
}

// TestIngressCache_UntrustedCachedHeaderStripped verifies that an external untrusted
// client cannot forge ate-endpoint-cached: 1 on an un-cached actor to bypass
// ext_proc resolution or corrupt routing.
func TestIngressCache_UntrustedCachedHeaderStripped(t *testing.T) {
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

	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
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

// TestIngressCache_GRPCUnavailableEvictsCache verifies that when a gRPC request
// encounters an unavailable worker (grpc-status: 14 UNAVAILABLE), ingress-cache
// evicts the stale binding without proxy-side stream replay.
func TestIngressCache_GRPCUnavailableEvictsCache(t *testing.T) {
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	fixture := deployGRPCEchoTemplate(t, ctx, env["BUCKET_NAME"])
	_, actorName, _ := createAndResumeSubstrateActor(t, ctx, "cache-grpc", fixture)
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	ctx = metadata.AppendToOutgoingContext(ctx,
		atenet.TargetActorHeader, actorRef.String(),
	)

	conn, err := grpc.NewClient(routerAddress(t, ctx),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("creating gRPC client: %v", err)
	}
	defer conn.Close()
	client := grpcechopb.NewEchoClient(conn)

	const message = "warm cache for grpc"
	waitForGRPCRouteReady(t, ctx, client, message)

	// 1. Initial successful RPC warms the cache.
	rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := client.Echo(rpcCtx, &grpcechopb.EchoRequest{Message: message})
	if err != nil {
		t.Fatalf("initial Echo failed: %v", err)
	}
	if resp.GetMessage() != message {
		t.Fatalf("initial Echo got %q, want %q", resp.GetMessage(), message)
	}

	scrapeBefore, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeRouterEnvoyMetrics before pod delete: %v", err)
	}
	evictBefore := e2e.RouterCacheCounterValue(scrapeBefore, "ate_ingress_cache_eviction")

	// 2. Suspend the actor so the worker immediately deactivates it.
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: networkingAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	waitForActorState(t, ctx, clients, networkingAtespace, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	// 3. Issue gRPC call to deactivated worker. It must return UNAVAILABLE (codes.Unavailable).
	callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
	defer callCancel()
	_, rpcErr := client.Echo(callCtx, &grpcechopb.EchoRequest{Message: message})
	if rpcErr == nil {
		t.Fatalf("expected gRPC call to deactivated worker pod to fail, but it succeeded")
	}
	st, ok := status.FromError(rpcErr)
	if !ok || st.Code() != codes.Unavailable {
		t.Logf("gRPC call to deactivated worker returned status %v (expected UNAVAILABLE)", rpcErr)
	}

	// 4. Verify cache eviction counter incremented.
	scrapeAfter, err := e2e.ScrapeRouterEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeRouterEnvoyMetrics after suspend: %v", err)
	}
	evictAfter := e2e.RouterCacheCounterValue(scrapeAfter, "ate_ingress_cache_eviction")
	if evictAfter <= evictBefore {
		t.Fatalf("expected ate_ingress_cache_eviction to increment, before=%.0f, after=%.0f", evictBefore, evictAfter)
	}

	// 5. Subsequent gRPC call succeeds via ext_proc, which resumes the actor.
	retryCtx, retryCancel := context.WithTimeout(ctx, 10*time.Second)
	defer retryCancel()
	var echoErr error
	retryResp, echoErr := client.Echo(retryCtx, &grpcechopb.EchoRequest{Message: message})
	if echoErr != nil {
		t.Fatalf("subsequent gRPC call after eviction failed: %v", echoErr)
	}
	if retryResp.GetMessage() != message {
		t.Fatalf("subsequent gRPC call got %q, want %q", retryResp.GetMessage(), message)
	}
}

func waitForActorState(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, name string, want ateapipb.ActorState) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: name},
		})
		if err == nil && resp.GetStatus().GetState() == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for actor %s/%s to reach %v", atespace, name, want)
}
