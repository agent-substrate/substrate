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
	"net"
	"net/http"
	"net/netip"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
)

// The EgressPolicy half of egress: the other TestActorEgress* tests give their
// actors an allow-everything policy and prove traffic flows; these give theirs
// a narrow one and prove what does not. A request either gateway can read is
// decided per request, the rules in order over its Host and the address the
// actor dialed. Opaque TCP, and TLS on the plain gateway, are decided by
// address alone at the CONNECT.

// egressMITM reports whether the suite runs against the sdsmint gateway.
func egressMITM() bool { return os.Getenv("E2E_EGRESS_MITM") != "" }

// notTransient stops the retry loop on anything but the 503 a request sees
// while the actor's route is still propagating; a denial is a final answer.
func notTransient(status int, _ []byte) bool { return status != http.StatusServiceUnavailable }

// reached stops the retry loop only on success. A lane expecting the fetch to
// work sees more transients than the 503 above (the sdsmint leaf fails
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

	_, actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-policy", egressFixture(), e2e.EgressAllowHostnames(allowed))
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

	_, actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-nopolicy", egressFixture())
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

// TestActorEgressPolicyAllowsByAddress: the policy names the origin's address
// only. Both gateways allow the fetch by address, and the same origin by name,
// because the request is checked against the address the actor dialed and
// sent there. A name that resolves to any other address is denied.
func TestActorEgressPolicyAllowsByAddress(t *testing.T) {
	ctx := context.Background()
	dataplane := e2e.CurrentAtenetDataplane()
	origin := egressHTTPTarget()
	target := e2e.DeployServerPod(t, ctx, origin)
	block := netip.MustParseAddr(target.ClusterIP)
	cidr := netip.PrefixFrom(block, block.BitLen()).String()

	_, actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-address", egressFixture(), e2e.EgressAllowCIDRs(cidr))
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}

	url := fmt.Sprintf("http://%s/healthz", target.Address())
	status, body := fetchThroughEgressActor(t, ctx, router, actorRef, url)
	if status != http.StatusOK {
		t.Fatalf("fetch of the allowed address %s returned HTTP %d, want 200; body: %s", url, status, body)
	}

	url = fmt.Sprintf("http://%s.%s.svc.cluster.local/healthz", origin.Name, target.Namespace)
	status, body = fetchThroughEgressActor(t, ctx, router, actorRef, url)
	if status != http.StatusOK {
		t.Fatalf("fetch of the allowed address by name %s returned HTTP %d, want 200; body: %s", url, status, body)
	}

	// The API server's ClusterIP is outside the block, and the policy has no
	// hostname rule that could allow a request inside, so the request is denied.
	url = "http://kubernetes.default.svc.cluster.local/healthz"
	status, body = fetchThroughEgressActorUntil(t, ctx, router, actorRef, url, notTransient)
	if !dataplane.IsEgressPolicyDenied(status, string(body)) {
		t.Fatalf("fetch of an address outside the policy %s returned HTTP %d, want an egress-policy denial; body: %s", url, status, body)
	}
	t.Logf("egress to an address outside the policy was denied as expected: %s", body)
}

// hostnamePolicyActor creates an actor whose policy names example.com and
// nothing else, and waits until it is routable.
func hostnamePolicyActor(t *testing.T, ctx context.Context) (*e2e.RouterClient, resources.ActorRef) {
	t.Helper()
	_, actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-sni", egressFixture(), e2e.EgressAllowHostnames("example.com"))
	router := mustRouterClient(t, ctx)
	t.Cleanup(func() { router.Close() })
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	waitForActorRoute(t, ctx, router, actorRef)
	return router, actorRef
}

// TestActorEgressHTTPSByHostnameMITM: sdsmint terminates the TLS and decides
// each request by name: example.com 200, example.org 403.
func TestActorEgressHTTPSByHostnameMITM(t *testing.T) {
	if !egressMITM() {
		t.Skip("covers the sdsmint gateway; set E2E_EGRESS_MITM")
	}
	ctx := context.Background()
	dataplane := e2e.CurrentAtenetDataplane()
	router, actorRef := hostnamePolicyActor(t, ctx)

	status, body := fetchThroughEgressActorUntil(t, ctx, router, actorRef, "https://example.com/", reached)
	if status != http.StatusOK {
		t.Fatalf("fetch of the allowed host returned HTTP %d, want 200; body: %s", status, body)
	}
	status, body = fetchThroughEgressActorUntil(t, ctx, router, actorRef, "https://example.org/", notTransient)
	if !dataplane.IsEgressPolicyDenied(status, string(body)) {
		t.Fatalf("fetch of a host outside the policy returned HTTP %d, want an egress-policy denial; body: %s", status, body)
	}
	t.Logf("denied on the decrypted request: %s", body)
}

// TestActorEgressHTTPSByHostnamePassthrough: the plain gateway cannot read
// TLS, and no address rule allows either destination, so both connections are
// closed before a byte reaches an origin, the allowed name included. The demo
// app reports each as a 502.
func TestActorEgressHTTPSByHostnamePassthrough(t *testing.T) {
	if egressMITM() {
		t.Skip("covers the plain gateway; sdsmint is TestActorEgressHTTPSByHostnameMITM")
	}
	if !e2e.CurrentAtenetDataplane().SupportsTLSPassthroughEgressPolicy() {
		t.Skip("TODO: AgentGateway must enforce substrateEgress for TLS passthrough")
	}
	ctx := context.Background()
	router, actorRef := hostnamePolicyActor(t, ctx)

	for _, url := range []string{"https://example.com/", "https://example.org/"} {
		status, body := fetchThroughEgressActorUntil(t, ctx, router, actorRef, url, notTransient)
		if status != http.StatusBadGateway || !strings.Contains(string(body), "request failed") {
			t.Fatalf("fetch of %s returned HTTP %d, want a failed fetch (502) from a tunnel closed before the handshake; body: %s", url, status, body)
		}
	}
	t.Log("both tunnels closed before the handshake")
}

// TestActorEgressHTTPSByAddress: the policy names the addresses example.com
// resolves to and no hostname. The plain gateway decides HTTPS by address at
// the CONNECT. sdsmint decides the decrypted request, which the address rule
// allows, and sends it to the dialed address with the origin's certificate
// checked against the name. Both fetch.
func TestActorEgressHTTPSByAddress(t *testing.T) {
	ctx := context.Background()
	addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip", "example.com")
	if err != nil || len(addrs) == 0 {
		t.Fatalf("resolving example.com: %v", err)
	}
	// The actor resolves the name itself. Allow the blocks around every
	// address seen here, so a rotation within the origin's ranges between the
	// two lookups does not fail the test. Addresses often share a block, and
	// the API refuses a repeated CIDR.
	var cidrs []string
	for _, addr := range addrs {
		bits := 24
		if addr.Unmap().Is6() {
			bits = 48
		}
		cidr := netip.PrefixFrom(addr.Unmap(), bits).Masked().String()
		if !slices.Contains(cidrs, cidr) {
			cidrs = append(cidrs, cidr)
		}
	}

	_, actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-ipblock", egressFixture(), e2e.EgressAllowCIDRs(cidrs...))
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	waitForActorRoute(t, ctx, router, actorRef)

	status, body := fetchThroughEgressActorUntil(t, ctx, router, actorRef, "https://example.com/", reached)
	if status != http.StatusOK {
		t.Fatalf("fetch of an allowed address returned HTTP %d, want 200 (allowed %v); body: %s", status, cidrs, body)
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
