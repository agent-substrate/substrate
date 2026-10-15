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
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The EgressPolicy half of egress: the other TestActorEgress* tests give their
// actors an allow-everything policy and prove traffic flows; these give theirs
// a narrow one and prove what does not. A request the gateway can read is
// decided per request on its Host, by the http rules in the clear and the
// https rules once decrypted.

// notTransient stops the retry loop on anything but the 503 a request sees
// while the actor's route is still propagating; a denial is a final answer.
func notTransient(status int, _ []byte) bool { return status != http.StatusServiceUnavailable }

// reached stops the retry loop only on success. A lane expecting the fetch to
// work sees more transients than the 503 above (the minted leaf fails
// verification until kubelet has propagated the CA pool), all of them 502s the
// actor cannot tell from a denial.
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

	_, actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-policy", e2e.EgressFixture(), e2e.EgressAllowHTTP(allowed))
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

	_, actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-nopolicy", e2e.EgressFixture())
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

// TestActorEgressHTTPSByHostname: the policy allows one origin by name for
// interception and another for passthrough, and the gateway handles each the
// way its rule says and refuses a name no rule lists.
//
// Both origins serve service-DNS certificates, which the gateway trusts and
// the actor does not, so the certificate the actor verifies tells the two
// paths apart.
func TestActorEgressHTTPSByHostname(t *testing.T) {
	ctx := context.Background()
	dataplane := e2e.CurrentAtenetDataplane()

	const mitmBody, passthroughBody = "mitm origin", "passthrough origin"
	mitm := e2e.DeployServerPod(t, ctx, egressHTTPSTarget("mitm-origin", mitmBody))
	passthroughSpec := egressHTTPSTarget("passthrough-origin", passthroughBody)
	passthroughSpec.Namespace = mitm.Namespace
	passthrough := e2e.DeployServerPod(t, ctx, passthroughSpec)

	// Include an HTTP rule for the .cluster.local FQDNs so cluster DNS search-path
	// resolution succeeds while port 443 TLS rules still match only SvcName().
	_, actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-sni", e2e.EgressFixture(),
		e2e.EgressAllowHTTP(mitm.SvcName()+".cluster.local", passthrough.SvcName()+".cluster.local"),
		e2e.EgressAllowHTTPS(mitm.SvcName()),
		e2e.EgressAllowPassthrough(passthrough.SvcName()),
	)
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	waitForActorRoute(t, ctx, router, actorRef)

	t.Run("mitm", func(t *testing.T) {
		// The actor's default roots hold the gateway's CA but not the
		// service-DNS signer, so a verified fetch means the gateway minted the
		// leaf the actor saw.
		url := "https://" + mitm.SvcName() + "/healthz"
		status, body := fetchThroughEgressActorUntil(t, ctx, router, actorRef, url, reached)
		if status != http.StatusOK {
			t.Fatalf("fetch of %s returned HTTP %d, want 200; body: %s", url, status, body)
		}
		if got := decodeEgressFetch(t, body); got.Body != mitmBody || !got.TLS {
			t.Errorf("fetch of %s = %+v, want verified TLS and body %q", url, got, mitmBody)
		}
	})

	t.Run("passthrough", func(t *testing.T) {
		// Trusting only the service-DNS signer, the fetch verifies against the
		// origin's own certificate alone, which an intercepting gateway would
		// have replaced with a minted leaf.
		url := "https://" + passthrough.SvcName() + "/healthz"
		payload, err := json.Marshal(map[string]string{"url": url, "rootCA": e2e.ServiceDNSTrustBundle(t, ctx, e2e.GetClients())})
		if err != nil {
			t.Fatalf("marshaling the fetch request for %s: %v", url, err)
		}
		status, body := postThroughEgressActorUntil(t, ctx, router, actorRef, "/", payload, reached)
		if status != http.StatusOK {
			t.Fatalf("fetch of %s returned HTTP %d, want 200; body: %s", url, status, body)
		}
		if got := decodeEgressFetch(t, body); got.Body != passthroughBody || !got.TLS {
			t.Errorf("fetch of %s = %+v, want verified TLS and body %q", url, got, passthroughBody)
		}
	})

	t.Run("unlisted host denied", func(t *testing.T) {
		// The MITM origin by its fully qualified name, which no rule lists
		// (rules match names exactly) and its certificate does not cover. A
		// gateway that wrongly intercepted it fails upstream with a 503; one
		// that wrongly passed it through hands the actor that certificate.
		url := "https://" + mitm.SvcName() + ".cluster.local/healthz"
		status, body := fetchThroughEgressActorUntil(t, ctx, router, actorRef, url, notTransient)
		if !dataplane.IsEgressPolicyDenied(status, string(body)) {
			t.Fatalf("fetch of %s, outside the policy, returned HTTP %d, want an egress-policy denial; body: %s", url, status, body)
		}
		// Envoy denies by closing the connection, which the actor reports as
		// the same 502 as any failed fetch; a certificate error in it means the
		// handshake reached the origin.
		if got := decodeEgressFetch(t, body); strings.Contains(got.Error, "x509") || strings.Contains(got.Error, "certificate") {
			t.Fatalf("fetch of %s, outside the policy, failed certificate verification instead of being denied, so the gateway passed it through; body: %s", url, body)
		}
		t.Logf("fetch of %s was denied as expected: %s", url, body)
	})
}

// This test validates that TLS passthrough connection is denied when policy changes
// even if there are outstanding connections that were allowed by the previous policy.
func TestActorEgressPassthroughPolicyRefresh(t *testing.T) {
	ctx := t.Context()
	dataplane := e2e.CurrentAtenetDataplane()
	_, address, rootCA := prepareProtocolOrigin(t, ctx, e2e.ServerPod{
		Name:       "passthrough-origin",
		ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"http"},
		Port:       80,
		TargetPort: 8080,
		TLSPort:    443,
	}, true)

	actorAtespace, actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-passthrough-refresh", e2e.EgressFixture(), e2e.EgressAllowPassthrough("*"))
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	waitForActorRoute(t, ctx, router, actorRef)

	// Ask egres actor to POST to /fetch on the test server. Test server /fetch handler
	// waits for the entire body before responding.
	url := "https://" + address + "/fetch"
	postPayload, err := json.Marshal(map[string]string{"url": url, "rootCA": rootCA})
	if err != nil {
		t.Fatalf("marshaling /post request: %v", err)
	}

	type postResult struct {
		status int
		body   []byte
	}
	since := metav1.NewTime(time.Now().Add(-1 * time.Minute))
	firstResult := make(chan postResult, 1)
	go func() {
		status, body := postThroughEgressActorUntil(t, ctx, router, actorRef, "/post", postPayload, notTransient)
		firstResult <- postResult{status: status, body: body}
	}()

	// Allow connection to be fully established before changing the policy.
	assertEgressGatewayConnect(t, ctx, since, actorAtespace, actorName, "443")

	e2e.EnsureEgressPolicy(t, ctx, e2e.GetClients(), actorRef.ToObjectRef(), e2e.EgressAllowPassthrough("foo.bar.baz"))
	// Wait for the policy cached in ext_proc server to expire, so it will fetch the new one.
	// ext_proc cache freshness interval is 10 seconds.
	time.Sleep(11 * time.Second)

	status, body := postThroughEgressActorUntil(t, ctx, router, actorRef, "/post", postPayload, notTransient)
	if !dataplane.IsEgressPolicyDenied(status, string(body)) {
		t.Fatalf("second POST to %s after policy update returned HTTP %d, want an egress-policy denial; body: %s", url, status, body)
	}

	first := <-firstResult
	if first.status != http.StatusOK {
		t.Fatalf("first POST to %s returned HTTP %d, want 200; body: %s", url, first.status, first.body)
	}
}

func TestActorEgressPendingPlaintextHeadersDoNotBlockMITM(t *testing.T) {
	ctx := t.Context()
	const mitmBody = "mitm origin"
	mitm := e2e.DeployServerPod(t, ctx, egressHTTPSTarget("mitm-origin", mitmBody))
	plaintextSpec := egressHTTPTarget()
	plaintextSpec.Namespace = mitm.Namespace
	plaintext := e2e.DeployServerPod(t, ctx, plaintextSpec)

	actorAtespace, actorName, _ := createAndResumeActorWithEgress(
		t, ctx, "egress-slow-headers", e2e.EgressFixture(),
		e2e.EgressAllowHTTPS(mitm.SvcName(), mitm.SvcName()+".cluster.local"),
		e2e.EgressAllowHTTP(plaintext.SvcName(), plaintext.SvcName()+".cluster.local"),
	)
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	waitForActorRoute(t, ctx, router, actorRef)

	plaintextURL := "http://" + plaintext.SvcName() + "/healthz"
	slowPayload, err := json.Marshal(map[string]string{"url": plaintextURL})
	if err != nil {
		t.Fatalf("marshaling /slow-headers request: %v", err)
	}

	type postResult struct {
		status int
		body   []byte
	}
	since := metav1.NewTime(time.Now().Add(-1 * time.Minute))
	pendingResult := make(chan postResult, 1)
	go func() {
		status, body := postThroughEgressActorUntil(t, ctx, router, actorRef, "/slow-headers", slowPayload, notTransient)
		pendingResult <- postResult{status: status, body: body}
	}()

	// Allow the plaintext request to send its incomplete headers before issuing the MITM request.
	assertEgressGatewayConnect(t, ctx, since, actorAtespace, actorName, "80")

	mitmURL := "https://" + mitm.SvcName() + "/healthz"
	status, body := fetchThroughEgressActorUntil(t, ctx, router, actorRef, mitmURL, reached)
	if status != http.StatusOK {
		t.Fatalf("fetch of %s returned HTTP %d, want 200; body: %s", mitmURL, status, body)
	}
	if got := decodeEgressFetch(t, body); got.Body != mitmBody || !got.TLS {
		t.Fatalf("fetch of %s = %+v, want verified TLS and body %q", mitmURL, got, mitmBody)
	}

	pending := <-pendingResult
	if pending.status != http.StatusOK {
		t.Fatalf("fetch of %s returned HTTP %d, want 200; body: %s", plaintextURL, pending.status, pending.body)
	}
}

// TestActorDNSRequiresPolicy: the sandbox's DNS relay refuses the queries of an
// actor with no EgressPolicy or for a host outside its policy, and answers them
// once a rule in the policy matches the host, without a restart.
func TestActorDNSRequiresPolicy(t *testing.T) {
	ctx := context.Background()
	origin := egressHTTPTarget()
	target := e2e.DeployServerPod(t, ctx, origin)
	name := fmt.Sprintf("%s.%s.svc.cluster.local", origin.Name, target.Namespace)
	url := "http://" + name + "/healthz"

	_, actorName, _ := createAndResumeActorWithEgress(t, ctx, "dns-nopolicy", e2e.EgressFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	waitForActorRoute(t, ctx, router, actorRef)

	status, body := fetchThroughEgressActorUntil(t, ctx, router, actorRef, url, notTransient)
	if status != http.StatusBadGateway || !isRefusedLookup(string(body), name) {
		t.Fatalf("fetch of %s by an actor with no policy returned HTTP %d, want its lookup refused; body: %s", url, status, body)
	}
	t.Logf("lookup was refused as expected: %s", body)

	// The relay fetches the policy again within seconds, so the retries below
	// see it. A wildcard rule covering the target namespace's Services allows
	// name while still refusing an unlisted host.
	wildcard := fmt.Sprintf("*.%s.svc.cluster.local", target.Namespace)
	e2e.EnsureEgressPolicy(t, ctx, e2e.GetClients(), actorRef.ToObjectRef(), e2e.EgressAllowHTTP(wildcard))
	status, body = fetchThroughEgressActorUntil(t, ctx, router, actorRef, url, reached)
	if status != http.StatusOK {
		t.Fatalf("fetch of %s after the actor was given a policy returned HTTP %d, want 200; body: %s", url, status, body)
	}

	const unlisted = "unlisted.example.com"
	status, body = fetchThroughEgressActorUntil(t, ctx, router, actorRef, "http://"+unlisted+"/healthz", notTransient)
	if status != http.StatusBadGateway || !isRefusedLookup(string(body), unlisted) {
		t.Fatalf("fetch of unlisted host %s returned HTTP %d, want its lookup refused; body: %s", unlisted, status, body)
	}
}

// isRefusedLookup reports whether body is the egress demo's report of a lookup
// of name that the resolver refused. Go's resolver reports a REFUSED answer as
// "server misbehaving".
func isRefusedLookup(body, name string) bool {
	return strings.Contains(body, "lookup "+name) && strings.Contains(body, "server misbehaving")
}

// hostnamePolicyActor creates an actor whose policy allows HTTPS to
// example.com, TLS passthrough to example.edu, and cleartext HTTP to
// example.org (so example.org resolves in DNS while still being denied over
// HTTPS on 443), and waits until it is routable.
func hostnamePolicyActor(t *testing.T, ctx context.Context) (*e2e.RouterClient, resources.ActorRef) {
	t.Helper()
	_, actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-sni", e2e.EgressFixture(),
		e2e.EgressAllowHTTPS("example.com"),
		e2e.EgressAllowPassthrough("example.edu"),
		e2e.EgressAllowHTTP("example.org"),
	)
	router := mustRouterClient(t, ctx)
	t.Cleanup(func() { router.Close() })
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	waitForActorRoute(t, ctx, router, actorRef)
	return router, actorRef
}

func isMitmCert(body string) bool {
	// example.* domains use certs signed by Google Trust Services
	// If this substring is not present it means dataplane used a minted cert
	return !strings.Contains(body, "O=Google Trust Services")
}

// TestActorEgressHTTPSByHostnameMITM: the gateway terminates the TLS and decides
// each request by name: example.com 200, example.org 403.
func TestActorEgressHTTPSByHostnameMITM(t *testing.T) {
	ctx := context.Background()
	dataplane := e2e.CurrentAtenetDataplane()
	router, actorRef := hostnamePolicyActor(t, ctx)

	status, body := fetchThroughEgressActorUntil(t, ctx, router, actorRef, "https://example.com/", reached)
	if status != http.StatusOK {
		t.Fatalf("fetch of the allowed host returned HTTP %d, want 200; body: %s", status, body)
	}
	if !isMitmCert(string(body)) {
		t.Fatalf("request did not use minted cert; body: %s", body)
	}
	status, body = fetchThroughEgressActorUntil(t, ctx, router, actorRef, "https://example.org/", notTransient)
	if !dataplane.IsEgressPolicyDenied(status, string(body)) {
		t.Fatalf("fetch of a host outside the policy returned HTTP %d, want an egress-policy denial; body: %s", status, body)
	}
	t.Logf("denied on the decrypted request: %s", body)
}

// TestActorEgressHTTPSByHostnamePassthrough: the gateway acts as TCP proxy fetching
// allowed SNI.
func TestActorEgressHTTPSByHostnamePassthrough(t *testing.T) {
	ctx := context.Background()
	dataplane := e2e.CurrentAtenetDataplane()
	router, actorRef := hostnamePolicyActor(t, ctx)

	status, body := fetchThroughEgressActorUntil(t, ctx, router, actorRef, "https://example.edu/", reached)
	if status != http.StatusOK {
		t.Fatalf("fetch of the allowed host returned HTTP %d, want 200; body: %s", status, body)
	}
	if isMitmCert(string(body)) {
		t.Fatalf("request used minted cert; body: %s", body)
	}
	status, body = fetchThroughEgressActorUntil(t, ctx, router, actorRef, "https://example.org/", notTransient)
	if !dataplane.IsEgressPolicyDenied(status, string(body)) {
		t.Fatalf("fetch of a host outside the policy returned HTTP %d, want an egress-policy denial; body: %s", status, body)
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
