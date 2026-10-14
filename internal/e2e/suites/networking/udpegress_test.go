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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
)

// The destination ports TestActorUDPEgress sends to. They are Service ports,
// all published by the one echo origin and all mapped to udpEchoListenPort, so
// the only thing that differs between them is the number a datagram is
// addressed to.
const (
	// dnsPort is the port a rule that spared DNS by number would let through.
	// The sandbox serves DNS from a relay at its own gateway address instead,
	// so UDP to port 53 anywhere else must be blocked like any other.
	dnsPort = 53
	// quicPort is HTTP/3: an actor that could send it would have HTTPS egress
	// with no CONNECT authority, no access log and no policy hook.
	quicPort = 443
	// arbitraryUDPPort stands for every other port.
	arbitraryUDPPort = 9999
	// udpEchoListenPort is the single listener behind all three. It is also the
	// origin's TCP port, where it answers the readiness probe.
	udpEchoListenPort = 8053
	// udpClientPort is where the control probe pod serves its HTTP API.
	udpClientPort = 8080
)

// udpEchoPorts are the UDP ports udpEchoTarget publishes.
var udpEchoPorts = []int{dnsPort, quicPort, arbitraryUDPPort}

// udpEchoTarget is the origin TestActorUDPEgress addresses: testserver's
// udpecho subcommand, published on udpEchoPorts plus its own TCP port.
func udpEchoTarget() e2e.ServerPod {
	return e2e.ServerPod{
		Name:       "udpecho",
		ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"udpecho"},
		Port:       udpEchoListenPort,
		UDPPorts:   udpEchoPorts,
	}
}

// udpClientControl runs the probe binary as a plain pod, outside any sandbox,
// so the same /udpecho code the actor runs can show the origin answers on
// every port.
func udpClientControl() e2e.ServerPod {
	return e2e.ServerPod{
		Name:       "udpclient",
		ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/probe",
		Port:       udpClientPort,
	}
}

// TestActorUDPEgress covers docs/egress-traffic.md's UDP rows: DNS is answered,
// and every other datagram is dropped.
//
// Each actor runs in its own network namespace whose only way out is its
// gateway address (internal/ateomnet.SetupSandboxNetwork): TCP is redirected
// to atunnel, and DNS over UDP and TCP is answered by the relay listening on
// the gateway address. A datagram for any other address, port 53 included,
// has nowhere to go. So the claim is that none of an actor's UDP reaches an
// origin that answers everything, while name resolution still works.
//
// A datagram dropped on the way out and one delivered to a server that stays
// quiet look the same from inside the sandbox, so a control pod outside any
// sandbox, running the same probe code, first has to get an echo on every
// port. Without it, a broken origin or Service would let the drop assertions
// pass having proven nothing.
func TestActorUDPEgress(t *testing.T) {
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()

	// The origins first, so a fixture failure costs no Actor resume.
	servers := e2e.DeployServerPods(t, ctx, udpEchoTarget(), udpClientControl())
	echo, control := servers[0], servers[1]

	// kube-proxy programs a new Service's UDP ports on its own schedule, and
	// until it does a datagram to the ClusterIP draws an ICMP port-unreachable
	// ("connection refused"). Wait the control out rather than fail on it.
	for _, port := range udpEchoPorts {
		got := waitForUDPEchoFromPod(t, ctx, control.Namespace, udpClientControl().Name, echo.AddressOnPort(port))
		t.Logf("UDP from the control pod to port %d was echoed after %d attempt(s)", port, got.Attempts)
	}

	atespace, _ := e2e.DeployProbe(t, env["BUCKET_NAME"], "networking")
	_, actorName, _ := createAndResumeSubstrateActor(t, ctx, "udpegress", e2e.SubstrateFixture{
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

	for _, port := range udpEchoPorts {
		t.Run(fmt.Sprintf("port %d dropped", port), func(t *testing.T) {
			got := probeUDPEcho(t, ctx, router, actorRef, echo.AddressOnPort(port))
			assertUDPDropped(t, port, got)
			t.Logf("UDP from the actor to port %d went unanswered after %d attempts, as expected: %s", port, got.Attempts, got.Error)
		})
	}

	// DNS is the one UDP the sandbox answers, through its own relay. Resolving
	// a name is what checks it; a lookup rather than a fetch, so the answer
	// depends on nothing but DNS.
	t.Run("DNS still resolves", func(t *testing.T) {
		host := fmt.Sprintf("%s.%s.svc.cluster.local", udpEchoTarget().Name, echo.Namespace)
		got := probeResolve(t, ctx, router, actorRef, host)
		if got.Error != "" {
			t.Fatalf("resolving %s from the actor failed: %s", host, got.Error)
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

// probeUDPEchoAttempts is the probe's udpEchoAttempts: how many datagrams
// /udpecho sends before it reports no echo.
const probeUDPEchoAttempts = 5

// assertUDPDropped requires got to be what a silently dropped datagram looks
// like: every attempt sent, and the last one ending in a read timeout. No echo
// alone would also be the result of a dial or send that failed, or of an ICMP
// error ("connection refused"), none of which shows a drop.
func assertUDPDropped(t *testing.T, port int, got udpEchoResult) {
	t.Helper()
	if got.Echoed {
		t.Fatalf("UDP from the actor to port %d was echoed: actor UDP left the sandbox", port)
	}
	if got.Attempts != probeUDPEchoAttempts {
		t.Fatalf("UDP from the actor to port %d stopped after %d of %d attempts (%s): the probe did not get to send and wait every time", port, got.Attempts, probeUDPEchoAttempts, got.Error)
	}
	if !strings.HasPrefix(got.Error, "waiting for the echo from ") || !strings.HasSuffix(got.Error, "i/o timeout") {
		t.Fatalf("UDP from the actor to port %d failed with %q, want the read timeout of a datagram that was silently dropped", port, got.Error)
	}
}

// waitForUDPEchoFromPod repeats probeUDPEchoFromPod until the datagram is
// echoed, failing after udpControlTimeout.
func waitForUDPEchoFromPod(t *testing.T, ctx context.Context, namespace, podName, addr string) udpEchoResult {
	t.Helper()
	const udpControlTimeout = 60 * time.Second
	deadline := time.Now().Add(udpControlTimeout)
	for {
		got := probeUDPEchoFromPod(t, ctx, namespace, podName, addr)
		if got.Echoed {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("UDP from the control pod to %s was not echoed within %v (last: %d attempts, %s): the echo origin or its Service is broken, so the actor's drops would prove nothing",
				addr, udpControlTimeout, got.Attempts, got.Error)
		}
		t.Logf("UDP from the control pod to %s not echoed yet (%s); retrying", addr, got.Error)
		time.Sleep(2 * time.Second)
	}
}

// probeUDPEchoFromPod is probeUDPEcho against the probe running as the plain
// pod podName, reached through the API server's pod proxy.
func probeUDPEchoFromPod(t *testing.T, ctx context.Context, namespace, podName, addr string) udpEchoResult {
	t.Helper()
	body, err := e2e.GetClients().K8s.CoreV1().RESTClient().Get().
		Namespace(namespace).
		Resource("pods").
		Name(podName+":"+strconv.Itoa(udpClientPort)).
		SubResource("proxy").
		Suffix("udpecho").
		Param("addr", addr).
		DoRaw(ctx)
	if err != nil {
		t.Fatalf("UDP echo probe of %s from pod %s/%s: %v (body %q)", addr, namespace, podName, err, body)
	}
	var out udpEchoResult
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decoding pod %s/%s's response to the UDP echo probe of %s: %v (body %q)", namespace, podName, addr, err, body)
	}
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
