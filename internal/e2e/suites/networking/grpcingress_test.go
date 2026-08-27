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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/portforward"
	"github.com/agent-substrate/substrate/internal/proto/grpcechopb"
	"github.com/agent-substrate/substrate/internal/resources"
	"k8s.io/client-go/kubernetes"
)

// grpcEchoFixtureManifest is the ActorTemplate this suite installs to get a
// gRPC-speaking Actor. It runs the same `testserver grpc` echo origin
// grpcegress_test.go deploys as a plain pod; see
// internal/e2e/fixtures/testserver.
const grpcEchoFixtureManifest = "internal/e2e/fixtures/testserver/grpcecho.yaml.tmpl"

// TestIngressProtocolDowngrade pins the ingress protocol contract end to end:
// a client that negotiates HTTP/2 with the router must still be able to reach
// an HTTP/1.1-only actor (the counter demo), because the atunnel leg
// downgrades non-gRPC traffic to HTTP/1.1. A gRPC-shaped request, by
// contrast, is carried to the actor as real HTTP/2 — so against this
// non-gRPC actor it must fail loudly rather than silently fall back to
// HTTP/1.1 (which would strip the trailers gRPC needs).
//
// TestIngressGRPC below is the positive counterpart: the same path, against an
// actor that really does speak gRPC.
func TestIngressProtocolDowngrade(t *testing.T) {
	ctx := context.Background()
	actorName, _ := createAndResumeActor(t, ctx, "protodowngrade", e2e.CounterFixture())
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}

	base := "http://" + routerAddress(t, ctx)

	h1 := &http.Client{Timeout: 30 * time.Second}
	h2c := &http.Client{Transport: h2cTransport(), Timeout: 30 * time.Second}

	request := func(client *http.Client, method, path, contentType string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, method, base+path, http.NoBody)
		if err != nil {
			return nil, err
		}
		req.Host = resources.ActorDNSName(actorRef)
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
		resp, err := request(h2c, http.MethodGet, "/readyz", "")
		if err != nil {
			t.Fatalf("h2c request through ingress: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.Proto != "HTTP/2.0" {
			t.Errorf("downstream proto = %s, want HTTP/2.0 (the client really negotiated h2)", resp.Proto)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("h2c GET = %d (body %q), want 200: non-gRPC HTTP/2 must be downgraded for HTTP/1.1-only actors", resp.StatusCode, body)
		}
		if !strings.Contains(string(body), "ok") {
			t.Errorf("h2c GET body = %q, want the actor's readyz payload", body)
		}
	})

	t.Run("grpc to non-grpc actor fails loudly", func(t *testing.T) {
		resp, err := request(h2c, http.MethodPost, "/count", "application/grpc")
		if err != nil {
			t.Fatalf("gRPC-shaped request through ingress: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		// atunnel forwards gRPC as real h2c, which the HTTP/1.1-only counter
		// cannot speak — a 502 from atunnel, not a silently-downgraded 200.
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("gRPC-shaped POST = %d (body %q), want 502: gRPC must not be silently downgraded to HTTP/1.1", resp.StatusCode, body)
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
	actorName, _ := createAndResumeActor(t, ctx, "grpcingress", fixture)
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}

	// Cleartext h2c to the router's HTTP port, with the Actor's DNS name as the
	// :authority — the same routing key every other ingress test in this suite
	// uses, just carried by a gRPC client instead of an HTTP one. The h2 ALPN
	// knob (--https-h2) is about the *TLS* listener; nothing here needs it.
	conn, err := grpc.NewClient(routerAddress(t, ctx),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithAuthority(resources.ActorDNSName(actorRef)),
	)
	if err != nil {
		t.Fatalf("creating the gRPC client for %s: %v", resources.ActorDNSName(actorRef), err)
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
// under test, waits for its golden snapshot and returns it. Mirrors the
// capabilities and sizing suites: render one manifest, build and apply it
// through the repo's pinned ko, delete the same file on the way out.
func deployGRPCEchoTemplate(t *testing.T, ctx context.Context, bucket string) e2e.Fixture {
	t.Helper()
	root, err := e2e.FindRepoRoot()
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}

	// The suite's own copy of the fixture: suite packages run as concurrent
	// processes, so a shared one would be deleted out from under another.
	manifest := e2e.RenderFixtureManifest(t, grpcEchoFixtureManifest, bucket, "networking")

	// KO_CONFIG_PATH is required because ko resolves .ko.yaml from its working
	// directory, which here is this package rather than the repo root.
	applyArgs := []string{"ko", "apply", "-f", manifest}
	if e2e.KubeContext != "" {
		applyArgs = append(applyArgs, "--", "--context="+e2e.KubeContext)
	}
	e2e.RunCmdWithEnv(t, []string{"KO_CONFIG_PATH=" + root}, filepath.Join(root, "hack/run-tool.sh"), applyArgs...)

	t.Cleanup(func() {
		// Deletion needs no image build, so go straight to kubectl; `ko delete`
		// rejects this arg shape.
		delArgs := []string{"delete", "--ignore-not-found", "-f", manifest}
		if e2e.KubeContext != "" {
			delArgs = append([]string{"--context=" + e2e.KubeContext}, delArgs...)
		}
		e2e.RunCmd(t, "kubectl", delArgs...)
	})

	fixture := e2e.Fixture{
		Namespace:  e2e.FixtureName("ate-e2e-grpcecho") + "-networking",
		Name:       "grpcecho",
		DeployWith: "the networking suite itself (see deployGRPCEchoTemplate)",
	}
	e2e.WaitForTemplateReady(ctx, t, e2e.GetClients(), fixture.Namespace, fixture.Name)
	return fixture
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

// routerAddress port-forwards to atenet-router's HTTP port and returns the
// local host:port, torn down when the test ends.
//
// e2e.RouterClient is not usable here: it speaks HTTP/1.1 only, and the whole
// point of both tests in this file is to control the protocol the client
// negotiates with the router.
func routerAddress(t *testing.T, ctx context.Context) string {
	t.Helper()
	config, err := ateclient.LoadConfig(e2e.KubeConfig, e2e.KubeContext)
	if err != nil {
		t.Fatalf("loading kubeconfig: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("creating k8s client: %v", err)
	}
	localPort, stop, err := portforward.ServicePortForward(ctx, config, clientset, "ate-system", "atenet-router", 80)
	if err != nil {
		t.Fatalf("port-forwarding to the router: %v", err)
	}
	t.Cleanup(stop)
	return fmt.Sprintf("127.0.0.1:%d", localPort)
}

// h2cTransport is an HTTP transport that speaks cleartext HTTP/2 by prior
// knowledge, so a test can reach the router's plain HTTP port as an h2 client
// without any ALPN negotiation.
func h2cTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	transport.Protocols = protocols
	return transport
}
