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

// Package egresscredinject e2e-tests egress credential injection: a matching
// EgressPolicy https rule with a replace_headers effect makes the egress
// gateway's MITM leg resolve the credential through the
// k8s-credential-provider and replace the actor's placeholder header with it
// before re-originating upstream. See TestActorEgressCredentialInjection for
// the proof structure and how to run this locally.
package egresscredinject

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const probeTemplate = "probe"

var probeNamespace string

// The suite's hostnames, one per injection outcome: each is covered by exactly
// one https rule, so each selects exactly one CredentialHeader. echoHost is the
// only one whose response matters — it echoes the request headers it
// received back as JSON, which is what proves the header was on the wire.
const (
	echoHost           = "httpbin.org"
	unfetchableHost    = "example.com"
	unservedHost       = "example.org"
	unauthorizedHost   = "example.net"
	echoOrigin         = "https://" + echoHost + "/headers"
	echoOriginPlain    = "http://" + echoHost + "/headers"
	unfetchableOrigin  = "https://" + unfetchableHost + "/"
	unservedOrigin     = "https://" + unservedHost + "/"
	unauthorizedOrigin = "https://" + unauthorizedHost + "/"
)

// placeholder is the Authorization value the actor sends. replace_headers
// replaces a header only when the request carries it, so every fetch that
// should trigger injection sends it.
const placeholder = "Bearer actor-placeholder"

var withPlaceholder = []string{"header=" + url.QueryEscape("Authorization:"+placeholder)}

// TestActorEgressCredentialInjection proves the injected credential reaches
// the upstream, and that every way injection can go wrong lands on the
// documented side of fail-open vs fail-closed (see egress.Handler.applyEffects):
//
//   - replaced: a fetch of the echo origin carrying the placeholder echoes the
//     injected "Authorization: Bearer <token>" among the headers the origin
//     received — the on-the-wire proof, not an inference from a status code —
//     and not the placeholder, so an actor cannot choose the value that
//     leaves.
//   - cleartext skip: the same fetch over plain HTTP, allowed by an http rule
//     with the same effect, echoes the placeholder and not the credential —
//     the secret never rides a cleartext wire, and the request is passed
//     through rather than denied.
//   - fail closed: a credential the policy requires but the provider will
//     not or cannot produce denies the request — 403 for an unfetchable
//     secret and for a namespace outside the atespace's authorization
//     (default-deny), 500 for a URI naming a provider this gateway does not
//     serve.
//
// A request without the header is not covered: the API forwards it without
// the credential, which the gateway does not implement yet.
//
// The gate: this needs the egress gateway deployed with injection enabled
// plus the k8s-credential-provider, which the suite deploys itself. Locally:
//
//	hack/install-ate-kind.sh --deploy-atenet --experimental-egress-credential-injection
//	E2E_EGRESS_CREDINJECT=1 hack/run-e2e-kind.sh ./internal/e2e/suites/egresscredinject -v -args --no-color
func TestActorEgressCredentialInjection(t *testing.T) {
	if os.Getenv("E2E_EGRESS_CREDINJECT") == "" {
		t.Skip("needs the egress gateway with credential injection: deploy with hack/install-ate-kind.sh --deploy-atenet --experimental-egress-credential-injection, then set E2E_EGRESS_CREDINJECT=1")
	}
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	e2e.DeployCredentialProvider(t)

	probeNamespace, _ = e2e.DeployProbe(t, env["BUCKET_NAME"], "egresscredinject", e2e.WithTrustBundle())

	const id = "probe-credinject"
	createAndResumeActor(t, ctx, clients, id)
	waitForActorState(t, ctx, clients, id, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	rc, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer rc.Close()

	// The gateway discards the placeholder and forwards the credential in its
	// place, so the actor cannot choose the value that leaves.
	wantHeader := "Bearer " + e2e.CredentialInjectionToken
	replaced := fetchEcho(t, ctx, rc, id, echoOrigin, withPlaceholder)
	if got := assertEchoedAuthorization(t, "injection fetch", replaced); got != wantHeader {
		t.Errorf("upstream received Authorization %q, want the injected %q", got, wantHeader)
	}

	// The same origin over plain HTTP: the cleartext leg skips injection and
	// passes the request through, so the fetch succeeds and the upstream sees
	// the placeholder, not the credential. (The probe does not follow
	// redirects, so an origin-side upgrade to HTTPS would surface as a non-200
	// here rather than silently re-running the TLS case.)
	cleartext := fetchEcho(t, ctx, rc, id, echoOriginPlain, withPlaceholder)
	if cleartext.Error != "" {
		t.Errorf("cleartext fetch of %s failed at the transport: %s", echoOriginPlain, cleartext.Error)
	} else if cleartext.Status != "200" {
		t.Errorf("cleartext fetch of %s: status %s, want 200 (the request should pass through without the credential)", echoOriginPlain, cleartext.Status)
	} else if got := decodeEchoedHeaders(t, "cleartext fetch", cleartext.Body)["Authorization"]; got != placeholder {
		t.Errorf("cleartext request arrived with Authorization %q, want the actor's placeholder %q: injection must be skipped on a cleartext wire", got, placeholder)
	}

	// Fail closed: each of these rules names a credential that cannot be
	// injected, and the denial must be the mapped status, not a request that
	// went out without the credential. No retries: the gateway answers these
	// itself.
	tests := []struct {
		name       string
		origin     string
		wantStatus string
	}{{
		name:       "unfetchable secret",
		origin:     unfetchableOrigin,
		wantStatus: "403",
	}, {
		name:       "unserved provider",
		origin:     unservedOrigin,
		wantStatus: "500",
	}, {
		name:       "unauthorized namespace",
		origin:     unauthorizedOrigin,
		wantStatus: "403",
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := probeFetch(t, ctx, rc, id, tt.origin, withPlaceholder)
			if got.Error != "" {
				t.Fatalf("fetch of %s failed at the transport (%s), want an HTTP %s from the gateway", tt.origin, got.Error, tt.wantStatus)
			}
			if got.Status != tt.wantStatus {
				t.Errorf("fetch of %s returned status %s, want %s", tt.origin, got.Status, tt.wantStatus)
			}
		})
	}
}

const echoRetryWindow = 2 * time.Minute

// fetchEcho is probeFetch for echo fetches that should return 200. It retries
// transient failures for up to echoRetryWindow:
//
//   - certificate errors, until the gateway's signing pool propagates;
//   - 503, while the gateway reconnects to a redeployed provider;
//   - 502/503/504 from httpbin.org itself.
func fetchEcho(t *testing.T, ctx context.Context, rc *e2e.RouterClient, id, origin string, extraParams []string) fetchResponse {
	t.Helper()
	deadline := time.Now().Add(echoRetryWindow)
	for {
		resp := probeFetch(t, ctx, rc, id, origin, extraParams)
		if !transientEchoFailure(resp) || time.Now().After(deadline) {
			return resp
		}
		t.Logf("fetch of %s: transient failure (status %q, error %q), retrying", origin, resp.Status, resp.Error)
		time.Sleep(5 * time.Second)
	}
}

// transientEchoFailure reports whether fetchEcho should retry resp.
func transientEchoFailure(resp fetchResponse) bool {
	if resp.Error != "" {
		return strings.Contains(resp.Error, "certificate") || strings.Contains(resp.Error, "x509")
	}
	switch resp.Status {
	case "502", "503", "504":
		return true
	}
	return false
}

// echoedHeaders is the echo origin's response shape: the request headers it
// received, echoed back. (httpbin.org/headers returns {"headers": {...}}.)
type echoedHeaders struct {
	Headers map[string]string `json:"headers"`
}

// decodeEchoedHeaders parses the echo origin's body into the headers the
// upstream received.
func decodeEchoedHeaders(t *testing.T, step, body string) map[string]string {
	t.Helper()
	var echoed echoedHeaders
	if err := json.Unmarshal([]byte(body), &echoed); err != nil {
		t.Fatalf("%s: decoding the echo origin's body: %v (body %q)", step, err, body)
	}
	return echoed.Headers
}

// assertEchoedAuthorization fails on any transport- or HTTP-level failure of
// an echo fetch and returns the Authorization value the upstream received.
func assertEchoedAuthorization(t *testing.T, step string, resp fetchResponse) string {
	t.Helper()
	if resp.Error != "" {
		t.Fatalf("%s: TLS through the MITM egress gateway failed: %s", step, resp.Error)
	}
	if resp.Status != "200" {
		t.Fatalf("%s: status %s, want 200 (an injection failure would deny with 403/500/503; is the provider deployed and the gateway installed with --experimental-egress-credential-injection?) body %q", step, resp.Status, resp.Body)
	}
	return decodeEchoedHeaders(t, step, resp.Body)["Authorization"]
}

type fetchResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
	Body   string `json:"body"`
}

// probeFetch asks the probe to fetch origin with the projected trust bundle,
// passing any extra pre-encoded query parameters through. Router-level
// failures are retried for up to 30s (a resume can return before the route
// reaches the router's xDS snapshot); probe-level TLS failures are results,
// returned for the caller to assert on.
func probeFetch(t *testing.T, ctx context.Context, rc *e2e.RouterClient, id, origin string, extraParams []string) fetchResponse {
	t.Helper()
	path := "/fetch?roots=bundle&url=" + url.QueryEscape(origin)
	for _, p := range extraParams {
		path += "&" + p
	}
	ref := resources.ActorRef{Atespace: probeNamespace, Name: id}

	deadline := time.Now().Add(30 * time.Second)
	for attempt := 1; ; attempt++ {
		resp, err := rc.Get(ctx, ref, path)
		if err != nil {
			t.Fatalf("GET %s for %q: %v", path, id, err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			t.Fatalf("reading %s response for %q: %v", path, id, readErr)
		}
		if resp.StatusCode == http.StatusOK {
			var out fetchResponse
			if err := json.Unmarshal(body, &out); err != nil {
				t.Fatalf("decoding %s response for %q: %v (body %q)", path, id, err, body)
			}
			return out
		}
		if time.Now().After(deadline) {
			t.Fatalf("GET %s for %q: status %d after %d attempts, body %q", path, id, resp.StatusCode, attempt, body)
		}
		t.Logf("GET %s for %q: attempt %d: status %d, body %q; retrying", path, id, attempt, resp.StatusCode, body)
		time.Sleep(2 * time.Second)
	}
}

// createAndResumeActor first deletes any actor left by an earlier run, since
// actor records outlive the fixture namespace.
func createAndResumeActor(t *testing.T, ctx context.Context, clients *e2e.Clients, id string) {
	t.Helper()
	ref := &ateapipb.ObjectRef{Atespace: probeNamespace, Name: id}
	// NotFound is the normal case on a fresh run. Other errors don't stop the
	// test, but they are logged in case CreateActor then fails.
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref}); err != nil && status.Code(err) != codes.NotFound {
		t.Logf("removing leftover actor %q: SuspendActor: %v", id, err)
	}
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref}); err != nil && status.Code(err) != codes.NotFound {
		t.Logf("removing leftover actor %q: DeleteActor: %v", id, err)
	}
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: probeNamespace, Name: id},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: probeNamespace, Name: probeTemplate},
	}}); err != nil {
		t.Fatalf("CreateActor %q: %v", id, err)
	}
	t.Cleanup(func() {
		if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref}); err != nil {
			t.Logf("cleanup: SuspendActor %q: %v", id, err)
		}
		if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref}); err != nil {
			t.Logf("cleanup: DeleteActor %q failed, actor leaked (remove with: kubectl ate delete actor %s -a %s): %v", id, id, probeNamespace, err)
		}
	})
	// One https rule per hostname, each carrying the injection whose outcome
	// that host is used to observe, and only these hosts are allowed at all.
	// echoHost also gets an http rule with the same injection, which the
	// cleartext fetch uses to prove the gateway skips it there.
	e2e.EnsureEgressPolicy(t, ctx, clients, ref,
		e2e.EgressInjectHeader("Authorization", "Bearer ", e2e.CredentialInjectionURI, echoHost),
		e2e.EgressInjectHeaderHTTP("Authorization", "Bearer ", e2e.CredentialInjectionURI, echoHost),
		e2e.EgressInjectHeader("Authorization", "Bearer ",
			"ate-secret://k8s.io/default/"+e2e.CredentialSecretsNamespace+"/no-such-secret/token", unfetchableHost),
		e2e.EgressInjectHeader("Authorization", "Bearer ",
			"ate-secret://other.io/default/"+e2e.CredentialSecretsNamespace+"/api-token/token", unservedHost),
		e2e.EgressInjectHeader("Authorization", "Bearer ",
			"ate-secret://k8s.io/default/kube-system/api-token/token", unauthorizedHost),
	)
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor %q: %v", id, err)
	}
}

func waitForActorState(t *testing.T, ctx context.Context, clients *e2e.Clients, actorName string, want ateapipb.ActorState) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: probeNamespace, Name: actorName},
		})
		if err == nil && resp.GetStatus().GetState() == want {
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("timed out waiting for actor %q to reach state %v", actorName, want)
}
