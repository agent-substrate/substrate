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
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/portforward"
	"github.com/agent-substrate/substrate/internal/resources"
	"k8s.io/client-go/kubernetes"
)

// TestIngressProtocolDowngrade pins the ingress protocol contract end to end:
// a client that negotiates HTTP/2 with the router must still be able to reach
// an HTTP/1.1-only actor (the counter demo), because the atunnel leg
// downgrades non-gRPC traffic to HTTP/1.1. A gRPC-shaped request, by
// contrast, is carried to the actor as real HTTP/2 — so against this
// non-gRPC actor it must fail loudly rather than silently fall back to
// HTTP/1.1 (which would strip the trailers gRPC needs).
//
// TODO(liorlieberman): add the gRPC-positive counterpart (a gRPC actor
// answering over the same path) once a gRPC actor fixture exists — glutton
// --mode=grpc is the natural candidate.
func TestIngressProtocolDowngrade(t *testing.T) {
	ctx := context.Background()
	actorName, _ := createAndResumeActor(t, ctx, "protodowngrade", e2e.CounterFixture())
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}

	config, err := ateclient.LoadConfig(e2e.KubeConfig, e2e.KubeContext)
	if err != nil {
		t.Fatalf("loading kubeconfig: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("creating k8s client: %v", err)
	}
	// RouterClient only speaks HTTP/1.1, so this test manages its own
	// port-forward and clients: the point is to control the protocol the
	// client negotiates with the router.
	localPort, stop, err := portforward.ServicePortForward(ctx, config, clientset, "ate-system", "atenet-router", 80)
	if err != nil {
		t.Fatalf("port-forwarding to the router: %v", err)
	}
	defer stop()
	base := fmt.Sprintf("http://127.0.0.1:%d", localPort)

	h1 := &http.Client{Timeout: 30 * time.Second}
	h2cTransport := http.DefaultTransport.(*http.Transport).Clone()
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	h2cTransport.Protocols = protocols
	h2c := &http.Client{Transport: h2cTransport, Timeout: 30 * time.Second}

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
