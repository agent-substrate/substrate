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

// actorTemplate identifies a demo ActorTemplate to build test Actors from,
// along with the hack/install-ate.sh flag that deploys it.
type actorTemplate struct {
	namespace string
	name      string
	// deployFlag names the install flag that creates the template, so a
	// missing fixture reports how to fix it rather than just failing.
	deployFlag string
}

var (
	counterTemplate = actorTemplate{namespace: "ate-demo-counter", name: "counter", deployFlag: "--deploy-demo-counter"}
	egressTemplate  = actorTemplate{namespace: "ate-demo-egress", name: "egress", deployFlag: "--deploy-demo-egress"}
)

func TestActorDirectAccess(t *testing.T) {
	ctx := context.Background()
	actorName, actor := createAndResumeActor(t, ctx, "direct", counterTemplate)
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

// TestActorEgressHTTP exercises the full egress path. The Actor's outbound TCP
// connection is transparently redirected by nftables into atunnel, wrapped in
// mTLS with the Actor's own actor-identity certificate plus an HTTP CONNECT to
// atenet-egress, authorized there against that certificate, and only then
// dialed out. A masqueraded (pre-gateway) egress would also return 200, so this
// asserts the gateway is deployed and that it did not reject the Actor.
func TestActorEgressHTTP(t *testing.T) {
	ctx := context.Background()
	actorName, _ := createAndResumeActor(t, ctx, "egress", egressTemplate)
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
	actorName, _ := createAndResumeActor(t, ctx, "egress-https", egressTemplate)
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

// TestActorEgressRawTCP covers egress for a payload that is neither HTTP nor
// TLS, from both sides of the who-speaks-first divide. HTTP and HTTPS both have
// the client send the first bytes, so neither notices a path that waits for
// downstream data before dialing upstream, or that inspects those first bytes
// to route.
//
// The two subtests dial different ports of the same origin, because that is the
// only way to select the behavior: the server decides whether to greet before
// it has read anything, so no field in the request could choose for it. Distinct
// ports also keep the access-log assertions unambiguous while both subtests
// share one Actor.
func TestActorEgressRawTCP(t *testing.T) {
	// The greeting from demos/egress/bannerserver, which must stay in step with
	// the Banner constant there.
	const banner = "TESTBANNER/1.0\r\n"

	tests := []struct {
		name string
		port int
		// wantBanner is what the origin volunteers before being spoken to, so
		// empty means it stayed silent.
		wantBanner string
		// timeout bounds each read the probe does.
		timeout string
	}{
		{name: "server speaks first", port: bannerServerPort, wantBanner: banner, timeout: "10s"},
		{name: "client speaks first", port: bannerServerQuietPort, wantBanner: "", timeout: "2s"},
	}

	ctx := context.Background()
	clusterIP := bannerServerClusterIP(t, ctx)
	actorName, _ := createAndResumeActor(t, ctx, "egress-tcp", egressTemplate)
	router := mustRouterClient(t, ctx)
	defer router.Close()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Bound the access-log scan to lines this subtest could have
			// produced. The slack absorbs clock skew with the gateway's node.
			since := metav1.NewTime(time.Now().Add(-1 * time.Minute))
			// Dial by address: the sandbox does not resolve cluster Service names.
			address := fmt.Sprintf("%s:%d", clusterIP, test.port)

			// Distinct per run, so the echo cannot be satisfied by anything stale.
			sent := fmt.Sprintf("ping-%d", time.Now().UnixNano())
			payload, err := json.Marshal(map[string]any{"address": address, "send": sent, "timeout": test.timeout})
			if err != nil {
				t.Fatalf("marshaling the TCP probe request for %s: %v", address, err)
			}
			status, body := postThroughEgressActor(t, ctx, router, resources.ActorRef{Atespace: networkingAtespace, Name: actorName}, "/tcp", payload)
			if status != http.StatusOK {
				t.Fatalf("Actor raw TCP probe of %s returned HTTP %d, want 200; body: %s", address, status, body)
			}

			var probe struct {
				Banner   string `json:"banner"`
				Received string `json:"received"`
				Error    string `json:"error"`
			}
			if err := json.Unmarshal(body, &probe); err != nil {
				t.Fatalf("decoding the TCP probe response %s: %v", body, err)
			}
			// The probe reads before it writes, so this distinguishes an origin
			// that spoke unprompted through the tunnel from one that did not.
			if probe.Banner != test.wantBanner {
				t.Fatalf("banner from %s = %q, want %q (error: %q)", address, probe.Banner, test.wantBanner, probe.Error)
			}
			if probe.Received != sent {
				t.Fatalf("echo from %s = %q, want %q (error: %q)", address, probe.Received, sent, probe.Error)
			}
			t.Logf("Actor raw TCP probe of %s succeeded; banner: %q", address, probe.Banner)

			port := strconv.Itoa(test.port)
			assertEgressGatewayConnect(t, ctx, since, actorName, port)
			assertEgressGatewayTunneledBytes(t, ctx, since, actorName, port)
		})
	}
}

// TestActorEgressSSH is TestActorEgressRawTCP against a real server-speaks-first
// protocol.
func TestActorEgressSSH(t *testing.T) {
	// RFC 4253 §4.2: the SSH server sends its identification string first.
	const identificationPrefix = "SSH-2.0-"
	const address = "github.com:22"

	ctx := context.Background()
	actorName, _ := createAndResumeActor(t, ctx, "egress-ssh", egressTemplate)
	router := mustRouterClient(t, ctx)
	defer router.Close()

	since := metav1.NewTime(time.Now().Add(-1 * time.Minute))

	// No Send: the identification string alone shows the transport carried the
	// server's first bytes, and anything written after it would start a key
	// exchange this test has no reason to hold up its end of.
	payload, err := json.Marshal(map[string]any{"address": address, "timeout": "10s"})
	if err != nil {
		t.Fatalf("marshaling the SSH probe request: %v", err)
	}
	status, body := postThroughEgressActor(t, ctx, router, resources.ActorRef{Atespace: networkingAtespace, Name: actorName}, "/tcp", payload)
	if status != http.StatusOK {
		t.Fatalf("Actor SSH probe of %s returned HTTP %d, want 200; body: %s", address, status, body)
	}

	var probe struct {
		Banner string `json:"banner"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		t.Fatalf("decoding the SSH probe response %s: %v", body, err)
	}
	if !strings.HasPrefix(probe.Banner, identificationPrefix) {
		t.Fatalf("banner from %s = %q, want a %q prefix (error: %q)", address, probe.Banner, identificationPrefix, probe.Error)
	}
	t.Logf("Actor SSH probe of %s succeeded; identification string: %q", address, strings.TrimSpace(probe.Banner))

	assertEgressGatewayConnect(t, ctx, since, actorName, "22")
}

// TestActorEgressStreamingDuration covers egress for connections that stay open
// long after the request that opened them. Every other egress test finishes in
// a second or two, so all of them pass against a gateway that silently cuts a
// connection once some timer expires -- and several timers on this path do
// exactly that unless they are explicitly disabled: Envoy applies a route
// timeout to a CONNECT tunnel's whole lifetime rather than just its headers,
// and the MITM gateway's HTTP chains apply one to a response body that, for a
// stream, never ends.
//
// The two subtests are the two ways a long-lived stream is normally built. SSE
// is one HTTP response that never finishes; WebSocket is an upgrade that leaves
// HTTP behind entirely, which a proxy carries only if it has been configured to
// proxy that upgrade.
//
// The Actor reads the stream in the background and is polled for progress,
// because the ingress route in front of it has a timeout of its own: a request
// that waited out the whole hold would be cut by the ingress before it could
// report on the egress.
func TestActorEgressStreamingDuration(t *testing.T) {
	// The hold has to clear every timeout on the egress path that a stream can
	// trip -- Envoy's 15s default route timeout on the CONNECT tunnel and the
	// 30s route timeout on the MITM leg's HTTP chains -- or the test passes
	// against a gateway that would still cut a real stream.
	const hold = 35 * time.Second
	// The origin ticks once a second, so a stream that survives the hold
	// delivers roughly one event per second of it. The margin absorbs connection
	// setup and the first tick's interval; the assertion that matters is that
	// events kept arriving for the whole hold, not their exact count.
	const minEvents = 25

	tests := []struct {
		name     string
		protocol string
		// url builds the origin address from the fixture's cluster IP. The
		// sandbox does not resolve cluster Service names, so it must be dialed
		// by address.
		url func(clusterIP string) string
	}{
		{
			name:     "server-sent events",
			protocol: "sse",
			url:      func(ip string) string { return fmt.Sprintf("http://%s:%d/sse", ip, streamServerPort) },
		},
		{
			name:     "websocket",
			protocol: "websocket",
			url:      func(ip string) string { return fmt.Sprintf("ws://%s:%d/ws", ip, streamServerPort) },
		},
	}

	ctx := context.Background()
	clusterIP := egressFixtureClusterIP(t, ctx, "streamserver")
	actorName, _ := createAndResumeActor(t, ctx, "egress-stream", egressTemplate)
	router := mustRouterClient(t, ctx)
	defer router.Close()

	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Bound the access-log scan to lines this subtest could have
			// produced. The slack absorbs clock skew with the gateway's node.
			since := metav1.NewTime(time.Now().Add(-1 * time.Minute))
			url := test.url(clusterIP)

			payload, err := json.Marshal(map[string]any{"url": url, "protocol": test.protocol, "hold": hold.String()})
			if err != nil {
				t.Fatalf("marshaling the stream probe request for %s: %v", url, err)
			}
			// The Actor answers 202: it has started reading, not finished. A
			// retry on anything else must not treat 202 as the failure, or every
			// pass would start another stream and leave it running.
			status, body := postThroughEgressActorExpecting(t, ctx, router, actorRef, "/stream", payload, http.StatusAccepted)
			if status != http.StatusAccepted {
				t.Fatalf("starting the stream probe of %s returned HTTP %d, want 202; body: %s", url, status, body)
			}
			var started struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(body, &started); err != nil {
				t.Fatalf("decoding the stream probe start response %s: %v", body, err)
			}

			probe := awaitStreamProbe(t, ctx, router, actorRef, started.ID, hold)
			if probe.Error != "" {
				t.Fatalf("stream of %s ended after %v with %d events: %s (want it held for %v)",
					url, time.Duration(probe.ElapsedMs)*time.Millisecond, probe.Events, probe.Error, hold)
			}
			// A stream can be cut without anyone reporting an error -- a proxy
			// that closes a CONNECT tunnel on its route timeout looks to the
			// reader like an origin that hung up -- so check the clock too.
			if elapsed := time.Duration(probe.ElapsedMs) * time.Millisecond; elapsed < hold {
				t.Fatalf("stream of %s stayed open %v, want at least %v (%d events, last %q)",
					url, elapsed, hold, probe.Events, probe.Last)
			}
			if probe.Events < minEvents {
				t.Fatalf("stream of %s delivered %d events over %v, want at least %d; it stalled rather than closed (first %q, last %q)",
					url, probe.Events, hold, minEvents, probe.First, probe.Last)
			}
			t.Logf("Actor held the %s stream of %s open for %dms across %d events (first %q, last %q)",
				test.protocol, url, probe.ElapsedMs, probe.Events, probe.First, probe.Last)

			port := strconv.Itoa(streamServerPort)
			assertEgressGatewayConnect(t, ctx, since, actorName, port)
		})
	}
}

// streamProbeStatus mirrors the response of the egress demo Actor's /stream
// endpoint, from demos/egress/stream.go.
type streamProbeStatus struct {
	Events    int    `json:"events"`
	First     string `json:"first"`
	Last      string `json:"last"`
	ElapsedMs int64  `json:"elapsedMs"`
	Done      bool   `json:"done"`
	Error     string `json:"error"`
}

// awaitStreamProbe polls the Actor until the probe it started reports itself
// done, and returns its final state. A probe that never finishes is a failure
// in its own right, so the wait is bounded well above the hold it was given.
func awaitStreamProbe(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef, id string, hold time.Duration) streamProbeStatus {
	t.Helper()

	// Slack over the hold for the reader's own setup and for the poll interval.
	deadline := time.Now().Add(hold + 30*time.Second)
	const pollInterval = 2 * time.Second
	for {
		response, err := router.Get(ctx, actorRef, "/stream?id="+id)
		if err != nil {
			t.Fatalf("polling stream probe %s: %v", id, err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatalf("reading stream probe %s status (HTTP %d): %v", id, response.StatusCode, err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("polling stream probe %s returned HTTP %d; body: %s", id, response.StatusCode, body)
		}

		var status streamProbeStatus
		if err := json.Unmarshal(body, &status); err != nil {
			t.Fatalf("decoding stream probe %s status %s: %v", id, body, err)
		}
		if status.Done {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream probe %s never finished; last seen with %d events over %dms", id, status.Events, status.ElapsedMs)
		}
		time.Sleep(pollInterval)
	}
}

// The ports the egress fixture Services publish, from
// demos/egress/egress.yaml.tmpl. The banner origin greets on the first and
// stays silent until spoken to on the second.
const (
	bannerServerPort      = 2222
	bannerServerQuietPort = 2223
	streamServerPort      = 8080
)

// bannerServerClusterIP returns the address of the in-cluster TCP origin the
// raw-TCP test dials.
func bannerServerClusterIP(t *testing.T, ctx context.Context) string {
	t.Helper()
	return egressFixtureClusterIP(t, ctx, "bannerserver")
}

// egressFixtureClusterIP returns the address of one of the egress demo's origin
// Services, which the Actor must dial by address because the sandbox does not
// resolve cluster Service names.
func egressFixtureClusterIP(t *testing.T, ctx context.Context, name string) string {
	t.Helper()
	service, err := e2e.GetClients().K8s.CoreV1().Services(egressTemplate.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting Service %s/%s: %v (deploy the fixture with %s)", egressTemplate.namespace, name, err, egressTemplate.deployFlag)
	}
	if service.Spec.ClusterIP == "" || service.Spec.ClusterIP == corev1.ClusterIPNone {
		t.Fatalf("Service %s/%s has no cluster IP to dial: %q", egressTemplate.namespace, name, service.Spec.ClusterIP)
	}
	return service.Spec.ClusterIP
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
// returns the status and body it answers with, retrying anything other than a
// 200 as postThroughEgressActorExpecting describes.
func postThroughEgressActor(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef, path string, payload []byte) (int, []byte) {
	t.Helper()
	return postThroughEgressActorExpecting(t, ctx, router, actorRef, path, payload, http.StatusOK)
}

// postThroughEgressActorExpecting POSTs payload to path on the egress demo
// Actor and returns the status and body it answers with. Retries any status
// other than want for up to 30s: ResumeActor can return before its route
// reaches atenet-router's xDS snapshot, and a request sent in that window sees
// a transient 503.
//
// want is a parameter rather than a fixed 200 because a POST that only starts
// work answers 202, and retrying that would start the work again on every pass.
func postThroughEgressActorExpecting(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef, path string, payload []byte, want int) (int, []byte) {
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
		if response.StatusCode == want || time.Now().After(deadline) {
			return response.StatusCode, body
		}
		t.Logf("POST %s to egress Actor returned HTTP %d, want %d; retrying...", path, response.StatusCode, want)
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

// assertEgressGatewayTunneledBytes waits for the access-log record the gateway
// writes when the tunnel closes, and requires bytes to have crossed it in both
// directions. The CONNECT record alone only says the tunnel was authorized and
// opened; these counters are the gateway's own evidence that it relayed the
// payload, rather than the Actor having reached the origin some other way.
func assertEgressGatewayTunneledBytes(t *testing.T, ctx context.Context, since metav1.Time, actorName, port string) {
	t.Helper()
	want := fmt.Sprintf("a closed tunnel to port %s by actor %s carrying bytes both ways", port, actorName)
	waitForAccessLog(t, ctx, since, want, func(lines []string) (bool, error) {
		for _, line := range lines {
			authority, ok := accessLogField(line, "authority")
			if !ok || !strings.HasSuffix(authority, ":"+port) {
				continue
			}
			if !strings.Contains(line, "/actor/"+actorName) {
				continue
			}
			// The counters only carry their final values on the record flushed
			// at close; the one flushed on establishment reports zeroes.
			up, upOK := accessLogCount(line, "up_bytes")
			down, downOK := accessLogCount(line, "down_bytes")
			if !upOK || !downOK {
				return false, fmt.Errorf("access-log line has no byte counters, so the log format changed: %s", line)
			}
			if up == 0 || down == 0 {
				continue
			}
			t.Logf("egress gateway relayed %d bytes up and %d down: %s", up, down, line)
			return true, nil
		}
		return false, nil
	})
}

// accessLogCount parses the field named key as a count. A missing field and an
// unparseable one are both reported as absent, since either means the caller's
// expectation of the log format no longer holds.
func accessLogCount(line, key string) (int, bool) {
	raw, ok := accessLogField(line, key)
	if !ok {
		return 0, false
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	return value, true
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

func createAndResumeActor(t *testing.T, ctx context.Context, prefix string, template actorTemplate) (string, *ateapipb.Actor) {
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
		ActorTemplateNamespace: template.namespace,
		ActorTemplateName:      template.name,
	}}); err != nil {
		t.Fatalf("CreateActor from %s/%s: %v (deploy the fixture with %s)", template.namespace, template.name, err, template.deployFlag)
	}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{Actor: actorRef})
		_, _ = clients.SubstrateAPI.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{Actor: actorRef})
	})

	resumeResponse, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef})
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
	if actor.GetWorkerAssignment().GetWorkerNamespace() == "" || actor.GetWorkerAssignment().GetWorkerPod() == "" {
		t.Fatalf("resumed Actor has no worker pod assignment: %+v", actor)
	}

	// The Kubernetes pod proxy performs this request from inside the cluster to
	// the assigned worker's port 80. It bypasses atenet-router and therefore
	// verifies that the old direct path remains unavailable without relying on
	// the test runner having a route to the pod CIDR.
	result := clients.K8s.CoreV1().RESTClient().Get().
		Namespace(actor.GetWorkerAssignment().GetWorkerNamespace()).
		Resource("pods").
		Name(actor.GetWorkerAssignment().GetWorkerPod() + ":80").
		SubResource("proxy").
		Suffix("readyz").
		Do(ctx)
	body, err := result.Raw()

	if err == nil {
		t.Fatalf("direct Actor access through %s/%s:80 unexpectedly succeeded; body: %s", actor.GetWorkerAssignment().GetWorkerNamespace(), actor.GetWorkerAssignment().GetWorkerPod(), body)
	}
	t.Logf("direct Actor access through %s/%s:80 was blocked as expected: %v", actor.GetWorkerAssignment().GetWorkerNamespace(), actor.GetWorkerAssignment().GetWorkerPod(), err)
}
