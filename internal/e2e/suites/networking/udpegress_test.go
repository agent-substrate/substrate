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
	"net/url"
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
)

// The destination ports TestActorUDPEgress sends to. They are Service ports,
// all published by the one echo origin and all mapped to udpEchoListenPort, so
// the only thing that differs between a datagram that arrives and one that does
// not is the number the actor addressed it to.
const (
	// dnsPort is the exception the forward-chain rule carves out, and the
	// reason the compatibility masquerade exists at all.
	dnsPort = 53
	// quicPort is the case the rule was written for: an actor that speaks
	// HTTP/3 would otherwise have its own HTTPS egress, out through the
	// masquerade with no CONNECT authority, no access log and no policy hook.
	quicPort = 443
	// arbitraryUDPPort stands for every other port, so a rule that happened to
	// name 443 rather than "not 53" would still fail this test.
	arbitraryUDPPort = 9999
	// udpEchoListenPort is the single listener behind all three. It is also the
	// origin's TCP port, where it answers the readiness probe and the /healthz
	// the DNS subtest fetches.
	udpEchoListenPort = 8053
)

// udpEchoTarget is the origin TestActorUDPEgress addresses: testserver's
// udpecho subcommand, published on the three UDP ports above plus its own TCP
// port.
func udpEchoTarget() e2e.ServerPod {
	return e2e.ServerPod{
		Name:       "udpecho",
		ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"udpecho"},
		Port:       udpEchoListenPort,
		UDPPorts:   []int{dnsPort, quicPort, arbitraryUDPPort},
	}
}

// TestActorUDPEgress covers the forward-chain rule InstallActorNftablesRules
// adds inside the worker pod's netns: actor UDP egress is dropped to every
// destination port but 53.
//
// Only TCP is redirected into atunnel, so before that rule a datagram left the
// worker pod through the compatibility masquerade with no CONNECT authority, no
// access log and no policy hook -- which made HTTP/3 on 443 a way for an actor
// to run its own unintercepted HTTPS egress. The rule is installed whether or
// not an egress gateway is configured, so this test needs no gateway fixture
// and makes no claim about one; it asserts only which datagrams leave.
//
// The proof needs an origin that answers, because a datagram dropped on the way
// out and one delivered to a server that stays quiet are the same observation
// from inside the sandbox. One echo origin publishes all three ports onto one
// listener, and the port-53 case runs first as a gate: if the echo path were
// broken, every port would look blocked and the negative cases would pass
// having proven nothing.
func TestActorUDPEgress(t *testing.T) {
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()

	// The origin first, so a fixture failure costs no Actor resume.
	echo := e2e.DeployServerPod(t, ctx, udpEchoTarget())

	atespace, _ := e2e.DeployProbe(t, env["BUCKET_NAME"], "networking")
	actorName, _ := createAndResumeSubstrateActor(t, ctx, "udpegress", e2e.SubstrateFixture{
		Atespace: atespace,
		Name:     e2e.ProbeName,
		// Unlike the demo fixtures this suite's other tests build actors from,
		// the probe is deployed by the line above rather than by an install
		// script, so a failure here is a bug and not a missing prerequisite.
		DeployWith: "e2e.DeployProbe, which this test already ran",
	})
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}

	allowed := probeUDPEcho(t, ctx, router, actorRef, echo.AddressOnPort(dnsPort))
	if !allowed.Echoed {
		t.Fatalf("UDP to port %d was not echoed after %d attempts (%s): the DNS exception is gone, or the echo origin is unreachable -- either way the drop assertions below would prove nothing",
			dnsPort, allowed.Attempts, allowed.Error)
	}
	t.Logf("UDP to port %d was echoed after %d attempt(s)", dnsPort, allowed.Attempts)

	for _, port := range []int{quicPort, arbitraryUDPPort} {
		t.Run(fmt.Sprintf("port %d dropped", port), func(t *testing.T) {
			got := probeUDPEcho(t, ctx, router, actorRef, echo.AddressOnPort(port))
			if got.Echoed {
				t.Fatalf("UDP to port %d was echoed: actor egress on that port reached the masquerade, unintercepted", port)
			}
			t.Logf("UDP to port %d went unanswered after %d attempts, as expected: %s", port, got.Attempts, got.Error)
		})
	}

	// The rule spares port 53 so that name resolution keeps working, and that
	// is a claim about the cluster's DNS rather than about one echo origin
	// that happens to be published on 53. Resolving a name is what checks it.
	// A lookup, not a fetch: the answer then depends on nothing but DNS, and
	// in particular not on the egress path this test makes no claim about.
	t.Run("DNS still resolves", func(t *testing.T) {
		host := fmt.Sprintf("%s.%s.svc.cluster.local", udpEchoTarget().Name, echo.Namespace)
		got := probeResolve(t, ctx, router, actorRef, host)
		if got.Error != "" {
			t.Fatalf("resolving %s from the actor failed: %s -- the DNS exception in the drop rule is not carrying real resolution", host, got.Error)
		}
		if !slices.Contains(got.Addresses, echo.ClusterIP) {
			t.Fatalf("%s resolved to %v, want it to include the Service's ClusterIP %s", host, got.Addresses, echo.ClusterIP)
		}
		t.Logf("%s resolved to %v from inside the actor", host, got.Addresses)
	})
}

// udpEchoResult mirrors the probe's /udpecho response body.
type udpEchoResult struct {
	Addr     string `json:"addr"`
	Echoed   bool   `json:"echoed"`
	Attempts int    `json:"attempts"`
	Error    string `json:"error"`
}

// probeUDPEcho asks the probe Actor to send UDP to addr and reports whether it
// was echoed back. A datagram that never comes back is a result, not a probe
// failure: the probe answers 200 either way, so only a router-level failure
// retries here.
func probeUDPEcho(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef, addr string) udpEchoResult {
	t.Helper()
	var out udpEchoResult
	probeGet(t, ctx, router, actorRef, "/udpecho?addr="+url.QueryEscape(addr), "UDP echo probe of "+addr, &out)
	return out
}

// probeResolveResult mirrors the probe's /resolve response body.
type probeResolveResult struct {
	Host      string   `json:"host"`
	Addresses []string `json:"addresses"`
	Error     string   `json:"error"`
}

// probeResolve asks the probe Actor to look host up through the sandbox's own
// resolver.
func probeResolve(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef, host string) probeResolveResult {
	t.Helper()
	var out probeResolveResult
	probeGet(t, ctx, router, actorRef, "/resolve?host="+url.QueryEscape(host), "lookup of "+host, &out)
	return out
}

// probeGet GETs path on the probe Actor through the router and decodes the JSON
// body into out.
func probeGet(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef, path, what string, out any) {
	t.Helper()
	body := waitForRouteReady(t, what, func() (*http.Response, error) {
		return router.Get(ctx, actorRef, path)
	})
	if err := json.Unmarshal([]byte(body), out); err != nil {
		t.Fatalf("decoding the probe's response to the %s: %v (body %q)", what, err, body)
	}
}
