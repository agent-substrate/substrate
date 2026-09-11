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
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
)

// The EgressPolicy half of egress: the other TestActorEgress* tests give their
// actors an allow-everything policy and prove traffic flows; these give theirs
// a narrow one and prove what does not. Both gateways police cleartext HTTP per
// request on its Host and opaque TCP by address alone. They differ on HTTPS:
// sdsmint terminates it and polices each request by name, the plain gateway
// polices the connection by address.

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
	origin := egressHTTPTarget()
	target := e2e.DeployServerPod(t, ctx, origin)
	allowed := fmt.Sprintf("%s.%s.svc.cluster.local", origin.Name, target.Namespace)

	actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-policy", egressFixture(), e2e.EgressAllowHostnames(allowed))
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
	if status != http.StatusForbidden || !strings.Contains(string(body), "egress denied") {
		t.Fatalf("fetch of the same origin by address %s returned HTTP %d, want 403 egress denied; body: %s", url, status, body)
	}
	t.Logf("fetch by address was denied as expected: %s", body)
}

// TestActorEgressRequiresPolicy: an actor with no EgressPolicy gets no tunnel.
// The CONNECT is refused, so the demo app reports a 502, not a 403, on both
// gateways.
func TestActorEgressRequiresPolicy(t *testing.T) {
	ctx := context.Background()
	target := e2e.DeployServerPod(t, ctx, egressHTTPTarget())

	actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-nopolicy", egressFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}

	// There is no allowed fetch to prove the route is up, so wait on the
	// demo's readiness endpoint; otherwise a 503 from a route not yet
	// propagated would look like the answer we want.
	waitForActorRoute(t, ctx, router, actorRef)

	url := fmt.Sprintf("http://%s/healthz", target.Address())
	status, body := fetchThroughEgressActorUntil(t, ctx, router, actorRef, url, notTransient)
	if status != http.StatusBadGateway || !strings.Contains(string(body), "request failed") {
		t.Fatalf("fetch by an actor with no policy returned HTTP %d, want 502 from a refused tunnel; body: %s", status, body)
	}
	t.Logf("tunnel was refused as expected: %s", body)
}

// TestActorEgressPolicyAllowsByAddress: the policy names the origin's address
// only. Both gateways allow the fetch by address. The same origin by name is
// refused, because the gateway dials what the request names.
func TestActorEgressPolicyAllowsByAddress(t *testing.T) {
	ctx := context.Background()
	origin := egressHTTPTarget()
	target := e2e.DeployServerPod(t, ctx, origin)
	block := netip.MustParseAddr(target.ClusterIP)
	cidr := netip.PrefixFrom(block, block.BitLen()).String()

	actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-address", egressFixture(), e2e.EgressAllowCIDRs(cidr))
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}

	url := fmt.Sprintf("http://%s/healthz", target.Address())
	status, body := fetchThroughEgressActor(t, ctx, router, actorRef, url)
	if status != http.StatusOK {
		t.Fatalf("fetch of the allowed address %s returned HTTP %d, want 200; body: %s", url, status, body)
	}

	url = fmt.Sprintf("http://%s.%s.svc.cluster.local/healthz", origin.Name, target.Namespace)
	status, body = fetchThroughEgressActorUntil(t, ctx, router, actorRef, url, notTransient)
	if status != http.StatusForbidden || !strings.Contains(string(body), "egress denied") {
		t.Fatalf("fetch of the same origin by name %s returned HTTP %d, want 403 egress denied; body: %s", url, status, body)
	}
	t.Logf("fetch by name was denied as expected: %s", body)
}

// TestActorEgressHTTPSByHostname: the policy names example.com. sdsmint
// terminates the TLS and polices by name: example.com 200, example.org 403.
// The plain gateway cannot read the request and no address rule allows either
// destination, so both connections are closed before a byte reaches an origin.
func TestActorEgressHTTPSByHostname(t *testing.T) {
	ctx := context.Background()
	actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-sni", egressFixture(), e2e.EgressAllowHostnames("example.com"))
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	waitForActorRoute(t, ctx, router, actorRef)

	allowedHost := notTransient
	if egressMITM() {
		allowedHost = reached
	}
	status, body := fetchThroughEgressActorUntil(t, ctx, router, actorRef, "https://example.com/", allowedHost)
	if egressMITM() {
		if status != http.StatusOK {
			t.Fatalf("fetch of the allowed host returned HTTP %d, want 200 through the sdsmint gateway; body: %s", status, body)
		}
	} else if status != http.StatusBadGateway || !strings.Contains(string(body), "request failed") {
		t.Fatalf("fetch of a host the policy names returned HTTP %d, want a failed fetch (502) through the plain gateway, whose hostname rules do not apply to TLS; body: %s", status, body)
	}

	status, body = fetchThroughEgressActorUntil(t, ctx, router, actorRef, "https://example.org/", notTransient)
	if egressMITM() {
		if status != http.StatusForbidden || !strings.Contains(string(body), "egress denied") {
			t.Fatalf("fetch of a host outside the policy returned HTTP %d, want 403 egress denied through the sdsmint gateway; body: %s", status, body)
		}
		t.Logf("denied on the decrypted request (sdsmint gateway): %s", body)
		return
	}
	if status != http.StatusBadGateway || !strings.Contains(string(body), "request failed") {
		t.Fatalf("fetch of a host outside the policy returned HTTP %d, want a failed fetch (502) through the plain gateway; body: %s", status, body)
	}
	t.Logf("closed before the handshake (plain gateway): %s", body)
}

// TestActorEgressHTTPSByAddress: the policy names the addresses example.com
// resolves to and no hostname. The plain gateway decides HTTPS by address at
// the CONNECT, so the fetch succeeds. sdsmint polices the request by name,
// which no rule allows.
func TestActorEgressHTTPSByAddress(t *testing.T) {
	ctx := context.Background()
	addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip", "example.com")
	if err != nil || len(addrs) == 0 {
		t.Fatalf("resolving example.com: %v", err)
	}
	// The actor resolves the name itself. Allow the blocks around every
	// address seen here, so a rotation within the origin's ranges between the
	// two lookups does not fail the test.
	var cidrs []string
	for _, addr := range addrs {
		bits := 24
		if addr.Unmap().Is6() {
			bits = 48
		}
		cidrs = append(cidrs, netip.PrefixFrom(addr.Unmap(), bits).Masked().String())
	}

	actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-ipblock", egressFixture(), e2e.EgressAllowCIDRs(cidrs...))
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	waitForActorRoute(t, ctx, router, actorRef)

	allowedAddress := reached
	if egressMITM() {
		allowedAddress = notTransient
	}
	status, body := fetchThroughEgressActorUntil(t, ctx, router, actorRef, "https://example.com/", allowedAddress)
	if egressMITM() {
		if status != http.StatusForbidden || !strings.Contains(string(body), "egress denied") {
			t.Fatalf("fetch by name with an address-only policy returned HTTP %d, want 403 egress denied through the sdsmint gateway, which polices the request's name; body: %s", status, body)
		}
		t.Logf("denied by name on the decrypted request (sdsmint gateway): %s", body)
		return
	}
	if status != http.StatusOK {
		t.Fatalf("fetch of an allowed address returned HTTP %d, want 200 through the plain gateway (allowed %v); body: %s", status, cidrs, body)
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
