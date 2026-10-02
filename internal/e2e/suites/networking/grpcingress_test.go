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
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/agent-substrate/substrate/internal/atenet"
	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/proto/grpcechopb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// grpcEchoFixtureManifests name the fixture this suite installs to get a
// gRPC-speaking Actor. It runs the same `testserver grpc` echo origin
// grpcegress_test.go deploys as a plain pod; see
// internal/e2e/fixtures/testserver.
var grpcEchoFixtureManifests = e2e.SubstrateFixtureManifests{
	Pool:     "internal/e2e/fixtures/testserver/grpcecho.yaml.tmpl",
	Template: "internal/e2e/fixtures/testserver/grpcecho-template.yaml.tmpl",
}

// TestIngressProtocolDowngrade pins the ingress protocol contract end to end:
// a client that negotiates HTTP/2 with the router must still be able to reach
// an HTTP/1.1-only actor (the counter demo), because ingress
// downgrades non-gRPC traffic to HTTP/1.1. A gRPC-shaped request, by
// contrast, is carried to the actor as real HTTP/2 — so against this
// non-gRPC actor it must fail loudly rather than silently fall back to
// HTTP/1.1 (which would strip the trailers gRPC needs).
//
// TestIngressGRPC below is the positive counterpart: the same path, against an
// actor that really does speak gRPC.
func TestIngressProtocolDowngrade(t *testing.T) {
	ctx := context.Background()
	_, actorName, _ := createAndResumeSubstrateActor(t, ctx, "protodowngrade", e2e.SubstrateCounterFixture())
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}

	rc := mustRouterClient(t, ctx)
	t.Cleanup(rc.Close)
	base := rc.BaseURL()
	h1 := &http.Client{Transport: routerTransport(rc, false), Timeout: 30 * time.Second}
	h2 := &http.Client{Transport: routerTransport(rc, true), Timeout: 30 * time.Second}

	request := func(client *http.Client, method, path, contentType string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, method, base+path, http.NoBody)
		if err != nil {
			return nil, err
		}
		req.Header.Set(atenet.TargetActorHeader, actorRef.String())
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		return client.Do(req)
	}

	// Wait for the route over plain HTTP/1.1 first, so the protocol
	// assertions below never race actor readiness.
	waitForRouteReady(t, "HTTP/1.1 access through ingress", func() (*http.Response, error) {
		return request(h1, http.MethodGet, "/readyz", "")
	})

	t.Run("h2 client reaches h1-only actor", func(t *testing.T) {
		resp, err := request(h2, http.MethodGet, "/readyz", "")
		if err != nil {
			t.Fatalf("h2 request through ingress: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.Proto != "HTTP/2.0" {
			t.Errorf("downstream proto = %s, want HTTP/2.0 (the client really negotiated h2)", resp.Proto)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("h2 GET = %d (body %q), want 200: non-gRPC HTTP/2 must be downgraded for HTTP/1.1-only actors", resp.StatusCode, body)
		}
		if !strings.Contains(string(body), "ok") {
			t.Errorf("h2 GET body = %q, want the actor's health payload", body)
		}
	})

	t.Run("grpc to non-grpc actor fails loudly", func(t *testing.T) {
		resp, err := request(h2, http.MethodPost, "/count", "application/grpc")
		if err != nil {
			t.Fatalf("gRPC-shaped request through ingress: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		// The counter cannot speak h2c. Envoy returns HTTP 502; agentgateway
		// reports the upstream failure with gRPC's Unavailable status.
		grpcStatus := resp.Header.Get("Grpc-Status")
		if grpcStatus == "" {
			grpcStatus = resp.Trailer.Get("Grpc-Status")
		}
		if resp.StatusCode != http.StatusBadGateway && !(resp.StatusCode == http.StatusOK && grpcStatus == "14") {
			t.Fatalf("gRPC-shaped POST = %d, grpc-status %q (body %q), want HTTP 502 or gRPC Unavailable: gRPC must not be silently downgraded to HTTP/1.1", resp.StatusCode, grpcStatus, body)
		}
	})
}

// TestIngressGRPC is the gRPC-positive half of the ingress protocol contract:
// a real gRPC client reaching a real gRPC Actor through atenet-router. Where
// TestIngressProtocolDowngrade proves gRPC is not silently downgraded, this
// proves the traffic that survives the ingress path is still usable gRPC.
//
// All three streaming shapes, because each one fails differently and only the
// first is covered by anything else in this suite: unary needs the status to
// arrive in trailers (after the body), a server-stream needs many frames over a
// connection held open across the response, and a bidirectional stream needs
// frames moving both ways at once and then a clean half-close. A path that
// parsed the request as HTTP/1.1 or dropped trailers would fail every one of
// them, and a path that merely buffered would fail the last.
//
// The Actor runs `testserver grpc`, the same echo origin TestActorEgressGRPC
// deploys as a plain pod, so the two directions cannot disagree about what a
// working RPC is.
func TestIngressGRPC(t *testing.T) {
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()

	fixture := deployGRPCEchoTemplate(t, ctx, env["BUCKET_NAME"])
	_, actorName, _ := createAndResumeSubstrateActor(t, ctx, "grpcingress", fixture)
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	ctx = metadata.AppendToOutgoingContext(ctx,
		atenet.TargetActorHeader, actorRef.String(),
	)

	// Cleartext h2c to the router's HTTP port, or h2 negotiated by ALPN on its
	// TLS port when the router requires mTLS. Explicit metadata identifies the
	// Actor; the conventional actor authority remains application metadata.
	rc := mustRouterClient(t, ctx)
	t.Cleanup(rc.Close)
	creds := insecure.NewCredentials()
	if tlsConfig := rc.TLSConfig(); tlsConfig != nil {
		creds = credentials.NewTLS(tlsConfig)
	}
	conn, err := grpc.NewClient(rc.Address(), grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatalf("creating the gRPC client for %s: %v", actorRef, err)
	}
	defer conn.Close()
	client := grpcechopb.NewEchoClient(conn)

	const message = "hello over grpc ingress"

	// Rides out the window between ResumeActor returning and the route reaching
	// atenet-router's xDS snapshot, as waitForRouteReady does for HTTP. It has
	// to be an RPC rather than a GET: the Actor serves only gRPC on port 80, so
	// an HTTP/1.1 probe would never come back 200 no matter how ready it is.
	waitForGRPCRouteReady(t, ctx, client, message)

	t.Run("unary", func(t *testing.T) {
		rpcCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		// A returned error here is itself the trailer assertion: grpc-go reports
		// a missing or malformed status as an error, so a path that dropped
		// trailers cannot reach the comparison below.
		response, err := client.Echo(rpcCtx, &grpcechopb.EchoRequest{Message: message})
		if err != nil {
			t.Fatalf("unary Echo through ingress: %v", err)
		}
		if response.GetMessage() != message {
			t.Errorf("unary Echo returned %q, want %q", response.GetMessage(), message)
		}
	})

	t.Run("server stream", func(t *testing.T) {
		rpcCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		const count = 3
		stream, err := client.EchoStream(rpcCtx, &grpcechopb.EchoStreamRequest{Message: message, Count: count})
		if err != nil {
			t.Fatalf("EchoStream through ingress: %v", err)
		}
		var got []*grpcechopb.EchoResponse
		for {
			response, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("EchoStream Recv after %d responses: %v", len(got), err)
			}
			got = append(got, response)
		}
		if len(got) != count {
			t.Fatalf("EchoStream returned %d responses, want %d", len(got), count)
		}
		// Indexes are what separate an intact stream from a reordered or
		// deduplicated one.
		for i, response := range got {
			if response.GetMessage() != message {
				t.Errorf("stream response %d message = %q, want %q", i, response.GetMessage(), message)
			}
			if int(response.GetIndex()) != i {
				t.Errorf("stream response %d index = %d, want %d", i, response.GetIndex(), i)
			}
		}
	})

	t.Run("bidi stream", func(t *testing.T) {
		rpcCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		stream, err := client.EchoBidi(rpcCtx)
		if err != nil {
			t.Fatalf("EchoBidi through ingress: %v", err)
		}
		// One message at a time, each blocking on its response before the next
		// is sent. A path that carried one direction at a time would not return
		// short answers here, it would hang until the context deadline.
		const count = 3
		for i := range count {
			want := fmt.Sprintf("%s-%d", message, i)
			if err := stream.Send(&grpcechopb.EchoRequest{Message: want}); err != nil {
				t.Fatalf("EchoBidi Send %d: %v", i, err)
			}
			response, err := stream.Recv()
			if err != nil {
				t.Fatalf("EchoBidi Recv %d: %v", i, err)
			}
			if response.GetMessage() != want {
				t.Errorf("bidi response %d message = %q, want %q", i, response.GetMessage(), want)
			}
			if int(response.GetIndex()) != i {
				t.Errorf("bidi response %d index = %d, want %d", i, response.GetIndex(), i)
			}
		}
		// Half-close the request direction: the server must still end this one
		// with OK, which a path that mishandled the half-close would not produce
		// even though everything above already echoed.
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("EchoBidi CloseSend: %v", err)
		}
		if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
			t.Errorf("EchoBidi Recv after CloseSend = %v, want io.EOF", err)
		}
	})
}

// deployGRPCEchoTemplate installs the gRPC Actor fixture for the sandbox class
// under test, waits for its golden snapshot and returns it. The suite gets its
// own copy of the fixture: suite packages run as concurrent processes, so a
// shared one would be deleted out from under another.
func deployGRPCEchoTemplate(t *testing.T, ctx context.Context, bucket string) e2e.SubstrateFixture {
	t.Helper()
	atespace, _ := e2e.DeploySubstrateFixture(t, ctx, e2e.GetClients(), grpcEchoFixtureManifests, bucket, "networking", false)
	return e2e.SubstrateFixture{
		Atespace:   atespace,
		Name:       "grpcecho",
		DeployWith: "the networking suite itself (see deployGRPCEchoTemplate)",
	}
}

// waitForGRPCRouteReady retries a unary Echo until it succeeds, riding out the
// window between ResumeActor returning and the Actor's route reaching
// atenet-router's xDS snapshot. Requests sent in that window come back as
// Unavailable, which is not a failure of anything this file tests.
func waitForGRPCRouteReady(t *testing.T, ctx context.Context, client grpcechopb.EchoClient, message string) {
	t.Helper()
	const timeout = 60 * time.Second
	deadline := time.Now().Add(timeout)
	for {
		rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := client.Echo(rpcCtx, &grpcechopb.EchoRequest{Message: message})
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("gRPC through ingress did not become ready within %v: %v", timeout, err)
		}
		t.Logf("gRPC through ingress failed: %v; retrying...", err)
		time.Sleep(time.Second)
	}
}

// routerTransport returns an HTTP transport for rc's router listener that
// speaks HTTP/2 when http2 is set and HTTP/1.1 otherwise. Against the plaintext
// listener HTTP/2 is cleartext h2c by prior knowledge; against the TLS
// listener the router requires in static-mtls mode, it is negotiated by ALPN.
//
// e2e.RouterClient's own client is not usable here: it speaks HTTP/1.1 only,
// and the whole point of both tests in this file is to control the protocol
// the client negotiates with the router.
func routerTransport(rc *e2e.RouterClient, http2 bool) *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	protocols := new(http.Protocols)
	if tlsConfig := rc.TLSConfig(); tlsConfig != nil {
		transport.TLSClientConfig = tlsConfig
		if http2 {
			protocols.SetHTTP2(true)
		} else {
			protocols.SetHTTP1(true)
		}
	} else {
		if http2 {
			protocols.SetUnencryptedHTTP2(true)
		} else {
			protocols.SetHTTP1(true)
		}
	}
	transport.Protocols = protocols
	return transport
}
