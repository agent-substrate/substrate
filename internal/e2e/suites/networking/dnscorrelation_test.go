// Copyright 2026 The Agent Substrate Authors
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
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
)

// dnsCorrelationManifests is the probe fixture on a WorkerPool with DNS
// correlation on.
var dnsCorrelationManifests = e2e.SubstrateFixtureManifests{
	Pool:     "internal/e2e/fixtures/probe/probe-dnscorrelation.yaml.tmpl",
	Template: "internal/e2e/fixtures/probe/probe-template.yaml.tmpl",
}

// fakeAddresses is the range DNS correlation answers from.
var fakeAddresses = netip.MustParsePrefix("198.18.0.0/15")

// TestActorDNSCorrelation: on a pool with DNS correlation, an actor's lookup
// of an allowed name returns a fake address, and its connection to that
// address reaches the egress gateway as a CONNECT to the name. After a suspend
// and resume, the address the actor was given still leads to the name, and
// the name still resolves to it.
//
// DNS correlation is a preview feature: install the cluster with its gate and
// pass the gate to the suite, which skips the test otherwise.
//
//	ATE_PREVIEW=DnsCorrelationInWorker go run ./cmd/ate-setup --kind deploy ate-system --credential-provider='{"name":"k8s.io"}'
//	hack/run-e2e-kind.sh ./internal/e2e/suites/networking -run TestActorDNSCorrelation -v -args --no-color --preview=DnsCorrelationInWorker
func TestActorDNSCorrelation(t *testing.T) {
	e2e.RequirePreview(t, preview.GateDNSCorrelationInWorker)
	if e2e.IsAgentGateway() {
		t.Skip("agentgateway connects plain HTTP to the address in the CONNECT, so its actors cannot be given fake addresses")
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	origin := egressHTTPTarget()
	target := e2e.DeployServerPod(t, ctx, origin)
	name := fmt.Sprintf("%s.%s.svc.cluster.local", origin.Name, target.Namespace)
	originURL := "http://" + name + "/healthz"
	authority := net.JoinHostPort(name, "80")

	atespace, _ := e2e.DeploySubstrateFixture(t, ctx, clients, dnsCorrelationManifests, "dnscorrelation", nil)
	const id = "correlated"
	ref := &ateapi.ObjectRef{Atespace: atespace, Name: id}
	actorRef := resources.ActorRef{Atespace: atespace, Name: id}
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: atespace, Name: id},
		ActorTemplate: &ateapi.ObjectRef{Atespace: atespace, Name: e2e.ProbeName},
	}}); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.SuspendActor(context.Background(), &ateapi.SuspendActorRequest{Actor: ref})
		_, _ = clients.SubstrateAPI.DeleteActor(context.Background(), &ateapi.DeleteActorRequest{Actor: ref})
	})
	// Before the resume, so the actor's first lookup already finds the policy.
	e2e.EnsureEgressPolicy(t, ctx, clients, ref, e2e.EgressAllowHTTP(name))
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	router := mustRouterClient(t, ctx)
	defer router.Close()

	// Old enough to hold every CONNECT of this test whatever the clock skew
	// between here and the gateway.
	since := metav1.NewTime(time.Now().Add(-time.Minute))
	// Each activation of the actor gets its own certificate, so the serial of
	// the certificate a CONNECT came with tells the two activations apart.
	serials := map[string]bool{}
	// expectConnectByName waits for a CONNECT to authority that the gateway
	// accepted from an activation of the actor not seen before.
	expectConnectByName := func(when string) {
		t.Helper()
		actorSAN := "/worker-for-actor/" + atespace + "/" + id
		waitForAccessLog(t, ctx, since, fmt.Sprintf("an accepted CONNECT to %s %s", authority, when), func(lines []gatewayAccessLogLine) bool {
			for _, line := range lines {
				got, _ := accessLogField(line.text, "authority")
				code, _ := accessLogField(line.text, "code")
				san, _ := accessLogField(line.text, "peer_san")
				serial, ok := accessLogField(line.text, "peer_serial")
				if got != authority || code != "200" || !strings.HasSuffix(san, actorSAN) || !ok || serials[serial] {
					continue
				}
				serials[serial] = true
				return true
			}
			return false
		})
	}

	fake := lookupFromProbe(t, ctx, router, actorRef, name)
	if !fakeAddresses.Contains(fake) {
		t.Fatalf("%s resolved to %s inside the actor, want an address in %s", name, fake, fakeAddresses)
	}
	if status := fetchFromProbe(t, ctx, router, actorRef, originURL, ""); status != "200" {
		t.Fatalf("fetch of %s returned status %q, want 200", originURL, status)
	}
	expectConnectByName("for the address the actor looked up")

	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: ref}); err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	waitForActorState(t, ctx, clients, atespace, id, ateapi.ActorState_ACTOR_STATE_SUSPENDED)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor after suspend: %v", err)
	}

	// The actor connects to the address it was given before the suspend,
	// without looking the name up again.
	kept := netip.AddrPortFrom(fake, 80).String()
	if status := fetchFromProbe(t, ctx, router, actorRef, originURL, kept); status != "200" {
		t.Fatalf("after a resume, fetch of %s dialed at %s returned status %q, want 200", originURL, kept, status)
	}
	expectConnectByName("for the address the actor kept across a suspend")
	if again := lookupFromProbe(t, ctx, router, actorRef, name); again != fake {
		t.Errorf("after a resume, %s resolved to %s, want %s as before the suspend", name, again, fake)
	}
}

// lookupFromProbe resolves host from inside the probe actor and returns the
// IPv4 address it resolved to.
func lookupFromProbe(t *testing.T, ctx context.Context, router *e2e.RouterClient, actor resources.ActorRef, host string) netip.Addr {
	t.Helper()
	var out struct {
		Addrs []string `json:"addrs"`
		Error string   `json:"error"`
	}
	getFromProbe(t, ctx, router, actor, "/lookup?host="+url.QueryEscape(host), &out)
	if out.Error != "" {
		t.Fatalf("lookup of %s inside the actor failed: %s", host, out.Error)
	}
	for _, raw := range out.Addrs {
		if addr, err := netip.ParseAddr(raw); err == nil && addr.Is4() {
			return addr
		}
	}
	t.Fatalf("lookup of %s inside the actor returned %v, want an IPv4 address", host, out.Addrs)
	return netip.Addr{}
}

// fetchFromProbe has the probe actor fetch origin and returns the origin's
// HTTP status. With dial, the probe connects to that address instead of
// resolving origin's host. A fetch that fails or returns a 5xx is retried for
// a while: the gateway's path to the origin may still be propagating.
func fetchFromProbe(t *testing.T, ctx context.Context, router *e2e.RouterClient, actor resources.ActorRef, origin, dial string) string {
	t.Helper()
	// roots=system: the fetch is plain HTTP, and the fixture projects no
	// trust bundle for the default mode to load.
	path := "/fetch?roots=system&url=" + url.QueryEscape(origin)
	if dial != "" {
		path += "&dial=" + url.QueryEscape(dial)
	}
	deadline := time.Now().Add(time.Minute)
	for {
		var out struct {
			Status string `json:"status"`
			Error  string `json:"error"`
		}
		getFromProbe(t, ctx, router, actor, path, &out)
		if out.Error == "" && len(out.Status) > 0 && out.Status[0] != '5' {
			return out.Status
		}
		if time.Now().After(deadline) {
			t.Fatalf("fetch of %s from the actor kept failing: status %q, error %q", origin, out.Status, out.Error)
		}
		time.Sleep(2 * time.Second)
	}
}

// getFromProbe GETs path on the probe actor through the router and decodes
// the JSON reply into out, retrying while the actor's route is not up.
func getFromProbe(t *testing.T, ctx context.Context, router *e2e.RouterClient, actor resources.ActorRef, path string, out any) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		resp, err := router.Get(ctx, actor, path)
		if err != nil {
			t.Fatalf("GET %s on the probe actor: %v", path, err)
		}
		if resp.StatusCode == http.StatusOK {
			err := json.NewDecoder(resp.Body).Decode(out)
			resp.Body.Close()
			if err != nil {
				t.Fatalf("decoding the probe's reply to %s: %v", path, err)
			}
			return
		}
		resp.Body.Close()
		if time.Now().After(deadline) {
			t.Fatalf("GET %s on the probe actor returned HTTP %d", path, resp.StatusCode)
		}
		time.Sleep(time.Second)
	}
}
