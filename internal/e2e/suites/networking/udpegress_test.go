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
	"strconv"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/resources"
)

const (
	dnsPort           = 53
	quicPort          = 443
	arbitraryUDPPort  = 9999
	udpEchoListenPort = 8053
	udpClientPort     = 8080
)

var udpEchoPorts = []int{dnsPort, quicPort, arbitraryUDPPort}

func udpEchoTarget() e2e.ServerPod {
	return e2e.ServerPod{
		Name:       "udpecho",
		ImportPath: "go.ate.dev/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"udpecho"},
		Port:       udpEchoListenPort,
		UDPPorts:   udpEchoPorts,
	}
}

func udpClientControl() e2e.ServerPod {
	return e2e.ServerPod{
		Name:       "udpclient",
		ImportPath: "go.ate.dev/substrate/demos/egress",
		Port:       udpClientPort,
		HealthPath: "/readyz",
	}
}

// TestActorUDPEgress checks that actor UDP egress to non-gateway destinations
// is silently dropped, including on ports 53 and 443. A control pod outside any
// sandbox first verifies that the echo Service answers on each port.
func TestActorUDPEgress(t *testing.T) {
	ctx := context.Background()

	echo := e2e.DeployServerPod(t, ctx, udpEchoTarget())
	control := e2e.DeployServerPod(t, ctx, udpClientControl())

	for _, port := range udpEchoPorts {
		got := waitForUDPEchoFromPod(t, ctx, control.Namespace, udpClientControl().Name, echo.AddressOnPort(port))
		t.Logf("UDP from the control pod to port %d was echoed after %d attempt(s)", port, got.Attempts)
	}

	_, actorName, _ := createAndResumeActorWithEgress(t, ctx, "udpegress", egressFixture(), e2e.EgressAllowAll()...)
	router := mustRouterClient(t, ctx)
	t.Cleanup(router.Close)
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	waitForActorRoute(t, ctx, router, actorRef)

	for _, port := range udpEchoPorts {
		t.Run(fmt.Sprintf("port %d dropped", port), func(t *testing.T) {
			t.Parallel()
			got := probeUDPEcho(t, ctx, router, actorRef, echo.AddressOnPort(port))
			assertUDPDropped(t, port, got)
		})
	}
}

type udpEchoResult struct {
	Addr     string `json:"addr"`
	Echoed   bool   `json:"echoed"`
	Attempts int    `json:"attempts"`
	Error    string `json:"error"`
}

func probeUDPEcho(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef, addr string) udpEchoResult {
	t.Helper()
	body := waitForRouteReady(t, "UDP echo probe of "+addr, func() (*http.Response, error) {
		return router.Get(ctx, actorRef, "/udpecho?addr="+url.QueryEscape(addr))
	})
	var out udpEchoResult
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("decoding UDP echo response: %v (body %q)", err, body)
	}
	return out
}

const probeUDPEchoAttempts = 5

func assertUDPDropped(t *testing.T, port int, got udpEchoResult) {
	t.Helper()
	if got.Echoed {
		t.Fatalf("UDP from the actor to port %d was echoed", port)
	}
	if got.Attempts != probeUDPEchoAttempts {
		t.Fatalf("UDP from the actor to port %d stopped after %d of %d attempts (%s)", port, got.Attempts, probeUDPEchoAttempts, got.Error)
	}
	if !strings.HasPrefix(got.Error, "waiting for the echo from ") || !strings.HasSuffix(got.Error, "i/o timeout") {
		t.Fatalf("UDP from the actor to port %d failed with %q, want a read timeout", port, got.Error)
	}
}

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
			t.Fatalf("UDP from the control pod to %s was not echoed within %v (last: %d attempts, %s)",
				addr, udpControlTimeout, got.Attempts, got.Error)
		}
		t.Logf("UDP from the control pod to %s not echoed yet (%s); retrying", addr, got.Error)
		time.Sleep(2 * time.Second)
	}
}

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
		t.Fatalf("decoding pod %s/%s UDP echo response: %v (body %q)", namespace, podName, err, body)
	}
	return out
}
