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
	"encoding/json"
	"net/http"
	"testing"

	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/resources"
)

// The only CONNECT the egress gateway tunnels is the one atunnel wraps the
// actor's traffic in, authenticated by the actor's certificate and addressed
// to the IP:port the actor dialed. A CONNECT the actor sends itself -- what a
// client configured with a forward proxy sends first, in the clear or inside
// TLS -- arrives as a request inside that tunnel, and the gateway refuses it
// with a 403 (docs/egress-traffic.md): the request would otherwise be policed
// as its authority's host on the port the actor dialed, while the tunnel went
// to the port the authority names.
//
// Each test sends the CONNECT to two proxies. One is an origin the test
// deploys, which answers any CONNECT 200: the 403 the actor sees cannot be
// its doing, so the gateway refused the CONNECT before it reached anything
// that would have accepted it. The other is the gateway's own Service, the
// address a client that knew about the gateway would configure.

// connectDeniedBody is the body both dataplanes refuse the CONNECT with.
const connectDeniedBody = "CONNECT egress is not supported"

// egressConnectResponse mirrors the egress demo's /connect observation: the
// proxy's answer to the CONNECT.
type egressConnectResponse struct {
	StatusCode int    `json:"statusCode"`
	Body       string `json:"body"`
	TLS        bool   `json:"tls"`
}

// connectTunnelTarget is what the actor asks for a tunnel to. The gateway
// refuses the CONNECT without resolving it, so it need not exist.
const connectTunnelTarget = "example.com:443"

// egressGatewayAddress is the egress gateway's CONNECT front door, the
// default-egress-gateway-address ate-api-server hands every atelet.
func egressGatewayAddress() string {
	return "atenet-egress." + e2e.SystemNamespace() + ".svc:443"
}

// connectProxySpec is testserver's http subcommand answering CONNECT the way a
// forward proxy does, published on port.
func connectProxySpec(port, tlsPort int) e2e.ServerPod {
	return e2e.ServerPod{
		Name:       "connect-proxy",
		ImportPath: "go.ate.dev/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"http", "--accept-connect"},
		Port:       port,
		TargetPort: 8080,
		TLSPort:    tlsPort,
	}
}

// TestActorEgressConnect: the actor sends a cleartext CONNECT, as a client
// configured with an http:// proxy does. The proxy listens on 3128, a
// conventional proxy port and one that is neither 80 nor 443, so the refusal
// is shown not to depend on the port the actor dialed.
func TestActorEgressConnect(t *testing.T) {
	ctx := t.Context()
	_, proxy, _ := prepareProtocolOrigin(t, ctx, connectProxySpec(3128, 0), false)
	_, router, actorRef := prepareProtocolActor(t, ctx, "connect")

	t.Run("to a proxy", func(t *testing.T) {
		assertActorConnectRefused(t, ctx, router, actorRef, proxy, false)
	})
	t.Run("to the gateway", func(t *testing.T) {
		assertActorConnectRefused(t, ctx, router, actorRef, egressGatewayAddress(), false)
	})
}

// TestActorEgressSecureConnect: the actor sends its CONNECT inside TLS, as a
// client configured with an https:// proxy does. The gateway terminates that
// TLS under the actor's https rule, so the CONNECT is read and refused on the
// decrypted leg, and the actor sees the 403 over a connection it verified.
func TestActorEgressSecureConnect(t *testing.T) {
	ctx := t.Context()
	_, proxy, _ := prepareProtocolOrigin(t, ctx, connectProxySpec(80, 443), true)
	_, router, actorRef := prepareProtocolActor(t, ctx, "connect-tls")

	t.Run("to a proxy", func(t *testing.T) {
		assertActorConnectRefused(t, ctx, router, actorRef, proxy, true)
	})
	t.Run("to the gateway", func(t *testing.T) {
		assertActorConnectRefused(t, ctx, router, actorRef, egressGatewayAddress(), true)
	})
}

// assertActorConnectRefused has the actor send proxy a CONNECT, over TLS when
// encrypted, and checks that the gateway answered it 403. The actor's answer is
// retried until it has one: a failed TLS handshake is reported as a 502 by the
// demo, and the first handshake with a minted leaf can fail until kubelet has
// propagated the gateway's CA to the actor.
func assertActorConnectRefused(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef, proxy string, encrypted bool) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"proxy": proxy, "target": connectTunnelTarget, "tls": encrypted})
	if err != nil {
		t.Fatalf("marshaling the CONNECT request: %v", err)
	}
	status, raw := postThroughEgressActorUntil(t, ctx, router, actorRef, "/connect", payload, reached)
	if status != http.StatusOK {
		t.Fatalf("the actor got no answer to its CONNECT %s via %s: HTTP %d; body: %s", connectTunnelTarget, proxy, status, raw)
	}
	var got egressConnectResponse
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decoding the CONNECT observation: %v (body %q)", err, raw)
	}
	if got.StatusCode/100 == 2 {
		t.Fatalf("CONNECT %s via %s was answered %d: the gateway opened a tunnel for the actor's own CONNECT: %+v", connectTunnelTarget, proxy, got.StatusCode, got)
	}
	if got.StatusCode != http.StatusForbidden || got.Body != connectDeniedBody {
		t.Fatalf("CONNECT %s via %s was answered %d %q, want %d %q: %+v", connectTunnelTarget, proxy, got.StatusCode, got.Body, http.StatusForbidden, connectDeniedBody, got)
	}
	if got.TLS != encrypted {
		t.Errorf("CONNECT %s via %s went over verified TLS = %t, want %t: %+v", connectTunnelTarget, proxy, got.TLS, encrypted, got)
	}
	t.Logf("the gateway refused the actor's CONNECT %s via %s: %+v", connectTunnelTarget, proxy, got)
}
