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
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
)

// counterExtraPort is the counter demo's second listener (see
// demos/counter/counter.yaml.tmpl's --extra-port flag), distinct from the
// primary port 80 every other assertion in this suite addresses.
const counterExtraPort = 9090

// TestActorArbitraryPortAccess defines the acceptance contract for
// atenet-router's arbitrary-port ingress support: a client reaches a port
// other than an actor's primary one via HTTP CONNECT, with the target port
// carried in the CONNECT authority (e.g. "<actor-dns>:9090") rather than a
// separate header -- see ingress.Handler.HandleRequestHeaders and
// atunnel.TargetPortHeader.
//
// CONNECT establishment itself (RouterClient.Connect) succeeds as soon as
// atenet-router's connect_terminate listener reinjects the tunnel into
// main_internal -- an in-process handoff that never touches the actor -- so
// it does not by itself prove the target port was reached. Both "reachable"
// and "unlisted" cases below send a real HTTP request through the
// established tunnel and inspect *that* response, which is where actor
// resolution and atunnel's dial to the actor's pod actually happen.
func TestActorArbitraryPortAccess(t *testing.T) {
	ctx := context.Background()
	actorName, _ := createAndResumeSubstrateActor(t, ctx, "arbitraryport", e2e.SubstrateCounterFixture())
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	router := mustRouterClient(t, ctx)
	defer router.Close()

	t.Run("regression: default port still reachable", func(t *testing.T) {
		// The new capability must not disturb the existing default-port path.
		body := waitForRouteReady(t, "default-port access through ingress", func() (*http.Response, error) {
			return router.Get(ctx, actorRef, "/readyz")
		})
		t.Logf("default-port access through ingress succeeded; body: %s", body)
	})

	t.Run("arbitrary port reachable", func(t *testing.T) {
		body := waitForTunneledRouteReady(t, ctx, router, actorRef, counterExtraPort)
		if !strings.Contains(body, "extra port 9090") {
			t.Fatalf("tunneled response body = %q, want it to mention extra port 9090", body)
		}

		// Cross-check against the default port's own response, both self-reported
		// by the same counter binary via getCurrentIP() -- either sandbox has its
		// own network stack (the gVisor sentry's, or the micro-VM guest's), so
		// this address is not the pod's Kubernetes IP, but it is consistent
		// between the two ports on one actor. Asserting they match proves the
		// tunnel reached the same actor's extra port rather than some other,
		// unrelated open port.
		defaultResp, err := router.Get(ctx, actorRef, "/")
		if err != nil {
			t.Fatalf("GET the actor's default port through ingress: %v", err)
		}
		defaultBody, err := io.ReadAll(defaultResp.Body)
		defaultResp.Body.Close()
		if err != nil {
			t.Fatalf("reading default-port response body: %v", err)
		}
		wantIP := selfReportedIP(t, string(defaultBody))
		if !strings.Contains(body, wantIP) {
			t.Fatalf("tunneled response body = %q, want it to mention the same self-reported IP as the default port (%s): %q", body, wantIP, defaultBody)
		}
		t.Logf("arbitrary-port access through CONNECT succeeded; body: %s", body)
	})

	t.Run("unlisted port rejected", func(t *testing.T) {
		// Nothing on the actor listens here. The tunnel itself still
		// establishes (see the doc comment above), so the failure has to
		// surface on the request sent through it -- atunnel's reverse proxy
		// fails to dial the actor and reports it as an HTTP error, rather
		// than the connection hanging or a request through it succeeding.
		const unlistedPort = 6553
		conn, err := router.Connect(ctx, actorRef, unlistedPort)
		if err != nil {
			t.Fatalf("CONNECT to an unlisted port unexpectedly failed to establish: %v", err)
		}
		defer conn.Close()

		conn.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: " + resources.ActorDNSName(actorRef) + "\r\nConnection: close\r\n\r\n")); err != nil {
			t.Fatalf("writing tunneled request: %v", err)
		}
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			// A reset/EOF while atunnel fails to dial the unlisted port is an
			// acceptable rejection too, not just a well-formed error response.
			t.Logf("tunneled request to an unlisted port correctly failed to read a response: %v", err)
			return
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatalf("tunneled request to an unlisted port unexpectedly returned HTTP 200; body: %s", body)
		}
		t.Logf("tunneled request to an unlisted port correctly returned HTTP %d; body: %s", resp.StatusCode, body)
	})
}

// waitForTunneledRouteReady retries an entire CONNECT exchange while the
// resumed actor's route propagates through atenet-router. A CONNECT tunnel is
// tied to one upstream connection, so each retry must establish a fresh tunnel
// rather than reusing a response that Envoy has already closed.
func waitForTunneledRouteReady(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef, port int) string {
	t.Helper()
	const timeout = 30 * time.Second
	deadline := time.Now().Add(timeout)
	var lastErr error
	var lastStatus int
	var lastBody string

	for {
		conn, err := router.Connect(ctx, actorRef, port)
		if err == nil {
			resp, body, requestErr := requestTunneled(conn, resources.ActorDNSName(actorRef))
			_ = conn.Close()
			if requestErr == nil && resp.StatusCode == http.StatusOK {
				return body
			}
			lastErr = requestErr
			if resp != nil {
				lastStatus = resp.StatusCode
				lastBody = body
			}
		} else {
			lastErr = err
		}

		if time.Now().After(deadline) {
			if lastErr != nil {
				t.Fatalf("tunneled request to port %d did not become ready within %s: %v", port, timeout, lastErr)
			}
			t.Fatalf("tunneled request to port %d returned HTTP %d after %s, want 200; body: %s", port, lastStatus, timeout, lastBody)
		}
		if lastErr != nil {
			t.Logf("tunneled request to port %d failed: %v; retrying...", port, lastErr)
		} else {
			t.Logf("tunneled request to port %d returned HTTP %d; retrying...", port, lastStatus)
		}
		time.Sleep(time.Second)
	}
}

func requestTunneled(conn interface {
	io.ReadWriter
	SetDeadline(time.Time) error
}, host string) (*http.Response, string, error) {
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return nil, "", fmt.Errorf("setting tunneled request deadline: %w", err)
	}
	if _, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n\r\n")); err != nil {
		return nil, "", fmt.Errorf("writing tunneled request: %w", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		return nil, "", fmt.Errorf("reading tunneled response: %w", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return resp, "", fmt.Errorf("reading tunneled response body (HTTP %d): %w", resp.StatusCode, err)
	}
	return resp, string(body), nil
}

// counterDefaultPortIPPattern matches the "hello from: <IP> | ..." response
// counter.go's default (port 80) handler returns.
var counterDefaultPortIPPattern = regexp.MustCompile(`^hello from: (\S+)`)

// selfReportedIP extracts the IP counter.go's getCurrentIP() reported in a
// default-port response body.
func selfReportedIP(t *testing.T, body string) string {
	t.Helper()
	m := counterDefaultPortIPPattern.FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("default-port response body = %q, want it to match %q", body, counterDefaultPortIPPattern)
	}
	return m[1]
}
