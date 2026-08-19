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
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const networkingAtespace = "networking-e2e"

func TestActorDirectAccess(t *testing.T) {
	ctx := context.Background()
	actorName, actor := createAndResumeActor(t, ctx, "direct", e2e.CounterFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()

	t.Run("direct", func(t *testing.T) {
		assertDirectActorAccess(t, ctx, e2e.GetClients(), actor)
	})
	t.Run("via ingress", func(t *testing.T) {
		actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
		body := waitForRouteReady(t, "Actor access through ingress", func() (*http.Response, error) {
			return router.Get(ctx, actorRef, "/readyz")
		})
		t.Logf("Actor access through ingress succeeded; body: %s", body)
	})
}

// TestActorEgress exercises the full egress path. The Actor's outbound TCP
// connection is transparently redirected by nftables into atunnel, wrapped in
// mTLS with the Actor's own actor-identity certificate plus an HTTP CONNECT to
// atenet-egress, authorized there against that certificate, and only then
// dialed out. A masqueraded (pre-gateway) egress would also return 200, so this
// asserts the gateway is deployed and that it did not reject the Actor.
func TestActorEgress(t *testing.T) {
	ctx := context.Background()
	actorName, _ := createAndResumeActor(t, ctx, "egress", e2e.EgressFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()

	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	status, body := fetchThroughEgressActor(t, ctx, router, actorRef, "http://example.com/")
	if status != http.StatusOK {
		t.Fatalf("Actor egress fetch returned HTTP %d, want 200; body: %s", status, body)
	}
	t.Logf("Actor egress fetch succeeded; body: %s", body)
}

// TestActorEgressHTTPS covers the same path as TestActorEgress with a TLS
// origin, where the gateway cannot see inside the request. atenet-egress
// authorizes the CONNECT against the Actor's actor-identity certificate and
// then relays raw TCP: it never decrypts, so the TLS session runs end to end
// between the Actor and the origin.
func TestActorEgressHTTPS(t *testing.T) {
	ctx := context.Background()
	actorName, _ := createAndResumeActor(t, ctx, "egress-https", e2e.EgressFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()

	// Bound the access-log scan below to lines this test could have produced.
	// The slack absorbs clock skew between here and the gateway's node.
	since := metav1.NewTime(time.Now().Add(-1 * time.Minute))

	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	status, body := fetchThroughEgressActor(t, ctx, router, actorRef, "https://example.com/")
	if status != http.StatusOK {
		t.Fatalf("Actor HTTPS egress fetch returned HTTP %d, want 200; body: %s", status, body)
	}
	t.Logf("Actor HTTPS egress fetch succeeded; body: %s", body)

	assertEgressGatewayConnect(t, ctx, since, actorName, "443")
}

// httpTarget is the origin TestActorEgressNonStandardPort dials: a plain HTTP
// server on a port that is neither 80 nor 443.
//
// It runs the egressprobe binary rather than one of its own. egressprobe's main
// only parses flags and serves /healthz, and the credential bundles it reads
// are read lazily inside /handshake, which this target never calls -- so it
// starts with nothing mounted and needs no fixture of its own.
var httpTarget = e2e.ServerPod{
	Name:       "httptarget",
	ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/egressprobe",
	Port:       8080,
}

// TestActorEgressNonStandardPort covers plaintext HTTP/1.1 egress to a port
// that is neither 80 nor 443, the shape most in-cluster services actually take.
//
// The port is worth its own test because nothing in the egress path holds it as
// a constant or derives it from the scheme: it is the Actor's own TCP
// destination port, recovered from SO_ORIGINAL_DST by TCPOriginalDestination
// after the prerouting REDIRECT that InstallActorNftablesRules adds inside the
// worker pod's netns, and then written verbatim into the CONNECT authority by
// atunnel's Client.DialContext. The other two tests would still pass if that
// port were defaulted from the scheme, because 80 and 443 are exactly what such
// a default would produce.
func TestActorEgressNonStandardPort(t *testing.T) {
	ctx := context.Background()

	// Stand the target up first: a fixture failure here should not leave a
	// resumed Actor idling in the cluster waiting for a destination.
	target := e2e.DeployServerPod(t, ctx, httpTarget)

	actorName, _ := createAndResumeActor(t, ctx, "egress-port", e2e.EgressFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()

	since := metav1.NewTime(time.Now().Add(-1 * time.Minute))

	// Address() is the ClusterIP literal, not the Service's DNS name: the
	// authority atunnel sends is always an address, so the name would add
	// nothing but a dependency on the sandbox's DNS-over-UDP masquerade path --
	// turning a DNS failure into something that reads as an egress-port
	// failure. kube-proxy's service DNAT happens later, in the host netns, so
	// <ClusterIP>:8080 is what SO_ORIGINAL_DST returns and what has to reach
	// the gateway.
	url := fmt.Sprintf("http://%s/healthz", target.Address())
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	status, body := fetchThroughEgressActor(t, ctx, router, actorRef, url)
	if status != http.StatusOK {
		t.Fatalf("Actor egress fetch of %s returned HTTP %d, want 200; body: %s", url, status, body)
	}
	t.Logf("Actor egress fetch of %s succeeded", url)

	assertEgressGatewayConnect(t, ctx, since, actorName, strconv.Itoa(httpTarget.Port))
}

// fetchThroughEgressActor asks the egress demo Actor to fetch url and returns
// the status and body it echoes back.
func fetchThroughEgressActor(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef, url string) (int, []byte) {
	t.Helper()
	payload, err := json.Marshal(map[string]string{"url": url})
	if err != nil {
		t.Fatalf("marshaling the fetch request for %s: %v", url, err)
	}
	return postThroughEgressActor(t, ctx, router, actorRef, "/", payload)
}

// postThroughEgressActor POSTs payload to path on the egress demo Actor and
// returns the status and body it answered with. Retries a non-200 response for
// up to 30s: ResumeActor can return before its route reaches atenet-router's
// xDS snapshot, and a request sent in that window sees a transient 503. The
// retry also rides out an origin that is reachable but not yet answering, which
// the Actor reports as a 502.
func postThroughEgressActor(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef, path string, payload []byte) (int, []byte) {
	t.Helper()

	const timeout = 30 * time.Second
	deadline := time.Now().Add(timeout)
	for {
		response, err := router.PostJSON(ctx, actorRef, path, payload)
		if err != nil {
			t.Fatalf("POST %s to egress Actor through ingress: %v", path, err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatalf("reading egress response body (HTTP %d): %v", response.StatusCode, err)
		}
		if response.StatusCode == http.StatusOK || time.Now().After(deadline) {
			return response.StatusCode, body
		}
		t.Logf("POST %s through egress Actor returned HTTP %d; retrying... body: %s", path, response.StatusCode, body)
		time.Sleep(1 * time.Second)
	}
}

// assertEgressGatewayConnect waits for the atenet-egress access log to show a
// CONNECT to port opened by actorName.
func assertEgressGatewayConnect(t *testing.T, ctx context.Context, since metav1.Time, actorName, port string) {
	t.Helper()
	want := fmt.Sprintf("a CONNECT to port %s by actor %s", port, actorName)
	waitForAccessLog(t, ctx, since, want, func(lines []string) (bool, error) {
		for _, line := range lines {
			authority, ok := accessLogField(line, "authority")
			if !ok || !strings.HasSuffix(authority, ":"+port) {
				continue
			}
			if !strings.Contains(line, "/actor/"+actorName) {
				continue
			}
			t.Logf("egress gateway tunneled the request: %s", line)
			return true, nil
		}
		return false, nil
	})
}

// waitForAccessLog polls the atenet-egress access log, across every gateway
// replica, until predicate accepts the lines written since.
func waitForAccessLog(t *testing.T, ctx context.Context, since metav1.Time, want string, predicate func(lines []string) (bool, error)) {
	t.Helper()
	const (
		gatewayNamespace = "ate-system"
		gatewaySelector  = "app=atenet-egress"
		gatewayContainer = "envoy"
		// The access log's line prefix, from the HttpConnectionManager
		// text_format_source in manifests/ate-install/atenet-egress.yaml.
		accessLogPrefix = "[egress] "
	)

	clients := e2e.GetClients()
	pods, err := clients.K8s.CoreV1().Pods(gatewayNamespace).List(ctx, metav1.ListOptions{LabelSelector: gatewaySelector})
	if err != nil {
		t.Fatalf("listing %s pods in %s: %v", gatewaySelector, gatewayNamespace, err)
	}
	if len(pods.Items) == 0 {
		t.Fatalf("no %s pods in %s; the egress gateway is not deployed", gatewaySelector, gatewayNamespace)
	}

	// Poll for the access log line (it may show up asynchronously from the actual traffic).
	const timeout = 30 * time.Second
	deadline := time.Now().Add(timeout)
	for {
		var lines []string
		for _, pod := range pods.Items {
			logs, err := clients.K8s.CoreV1().Pods(gatewayNamespace).GetLogs(pod.Name, &corev1.PodLogOptions{
				Container: gatewayContainer,
				SinceTime: &since,
			}).DoRaw(ctx)
			if err != nil {
				t.Fatalf("reading logs of %s/%s: %v", gatewayNamespace, pod.Name, err)
			}
			for line := range strings.SplitSeq(string(logs), "\n") {
				if strings.Contains(line, accessLogPrefix) {
					lines = append(lines, line)
				}
			}
		}

		matched, err := predicate(lines)
		if err != nil {
			t.Fatalf("looking for %s in the atenet-egress access log: %v", want, err)
		}
		if matched {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no atenet-egress access-log line for %s after %v; lines seen:\n%s",
				want, timeout, strings.Join(lines, "\n"))
		}
		time.Sleep(1 * time.Second)
	}
}

// accessLogField returns the value of the key=value field named key in an Envoy
// access log line whose fields are separated by spaces.
func accessLogField(line, key string) (string, bool) {
	_, rest, ok := strings.Cut(line, key+"=")
	if !ok {
		return "", false
	}
	value, _, _ := strings.Cut(rest, " ")
	return value, true
}

func createAndResumeActor(t *testing.T, ctx context.Context, prefix string, template e2e.Fixture) (string, *ateapipb.Actor) {
	t.Helper()
	clients := e2e.GetClients()
	actorName := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	actorRef := &ateapipb.ObjectRef{Atespace: networkingAtespace, Name: actorName}

	t.Logf("creating actor %s/%s", networkingAtespace, actorName)
	_, _ = clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: networkingAtespace}},
	})
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: networkingAtespace, Name: actorName},
		ActorTemplateNamespace: template.Namespace,
		ActorTemplateName:      template.Name,
	}}); err != nil {
		t.Fatalf("CreateActor from %s/%s: %v (deploy the fixture with %s)", template.Namespace, template.Name, err, template.DeployWith)
	}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{Actor: actorRef})
		_, _ = clients.SubstrateAPI.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{Actor: actorRef})
	})

	resumeResponse, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	t.Logf("resumed actor %s/%s", networkingAtespace, actorName)
	return actorName, resumeResponse.GetActor()
}

func mustRouterClient(t *testing.T, ctx context.Context) *e2e.RouterClient {
	t.Helper()
	router, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	return router
}

// waitForRouteReady retries request until it returns a 200 response or
// timeout elapses, and returns that response's body. This rides out the race
// between ResumeActor returning and its route reaching atenet-router's xDS
// snapshot: a request sent in that window sees a transient 503 connection
// timeout, not a real failure, and every caller through the router hits it.
// what names the request in log/failure output.
func waitForRouteReady(t *testing.T, what string, request func() (*http.Response, error)) string {
	t.Helper()
	const timeout = 30 * time.Second
	deadline := time.Now().Add(timeout)
	for {
		response, err := request()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatalf("reading %s response body (HTTP %d): %v", what, response.StatusCode, err)
		}
		if response.StatusCode == http.StatusOK {
			return string(body)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s returned HTTP %d after %v; body: %s", what, response.StatusCode, timeout, body)
		}
		t.Logf("%s returned HTTP %d; retrying...", what, response.StatusCode)
		time.Sleep(1 * time.Second)
	}
}

func assertDirectActorAccess(t *testing.T, ctx context.Context, clients *e2e.Clients, actor *ateapipb.Actor) {
	t.Helper()
	if actor.GetStatus().GetWorkerAssignment().GetWorkerNamespace() == "" || actor.GetStatus().GetWorkerAssignment().GetWorkerPod() == "" {
		t.Fatalf("resumed Actor has no worker pod assignment: %+v", actor)
	}

	// The Kubernetes pod proxy performs this request from inside the cluster to
	// the assigned worker's port 80. It bypasses atenet-router and therefore
	// verifies that the old direct path remains unavailable without relying on
	// the test runner having a route to the pod CIDR.
	result := clients.K8s.CoreV1().RESTClient().Get().
		Namespace(actor.GetStatus().GetWorkerAssignment().GetWorkerNamespace()).
		Resource("pods").
		Name(actor.GetStatus().GetWorkerAssignment().GetWorkerPod() + ":80").
		SubResource("proxy").
		Suffix("readyz").
		Do(ctx)
	body, err := result.Raw()

	if err == nil {
		t.Fatalf("direct Actor access through %s/%s:80 unexpectedly succeeded; body: %s", actor.GetStatus().GetWorkerAssignment().GetWorkerNamespace(), actor.GetStatus().GetWorkerAssignment().GetWorkerPod(), body)
	}
	t.Logf("direct Actor access through %s/%s:80 was blocked as expected: %v", actor.GetStatus().GetWorkerAssignment().GetWorkerNamespace(), actor.GetStatus().GetWorkerAssignment().GetWorkerPod(), err)
}
