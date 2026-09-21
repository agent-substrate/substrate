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
// EgressPolicy rule with an inject_static_headers effect makes the sdsmint
// egress gateway's MITM leg resolve the credential through the
// k8s-credential-provider and set it as a request header before
// re-originating upstream. See TestActorEgressCredentialInjection for the
// proof structure and how to run this locally.
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
)

const probeTemplate = "probe"

var probeNamespace string

// The suite's hostnames, one per injection outcome: rules match by hostname,
// so each host selects exactly one CredentialHeaderInjection. echoHost is the
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

// TestActorEgressCredentialInjection proves the injected credential reaches
// the upstream, and that every way injection can go wrong lands on the
// documented side of fail-open vs fail-closed (see egress.Handler.applyEffects):
//
//   - injected: a fetch of the echo origin returns the injected
//     "Authorization: Bearer <token>" among the headers the origin received —
//     the on-the-wire proof, not an inference from a status code.
//   - overwritten: the same fetch with a pre-seeded Authorization header
//     still echoes the injected value, so an actor cannot smuggle its own.
//   - cleartext skip: the same origin over plain HTTP echoes NO Authorization
//     header — the secret never rides a cleartext wire, and the request is
//     passed through rather than denied.
//   - fail closed: a credential the policy requires but the provider will
//     not or cannot produce denies the request — 403 for an unfetchable
//     secret and for a namespace outside the atespace's authorization
//     (default-deny), 500 for a URI naming a provider this gateway does not
//     serve.
//
// The gate: this needs the sdsmint egress gateway with injection enabled
// (which replaces the passthrough gateway cluster-wide) plus the
// k8s-credential-provider, which the suite deploys itself. Locally:
//
//	hack/install-ate-kind.sh --deploy-atenet --experimental-use-sdsmint --experimental-egress-credential-injection
//	E2E_EGRESS_CREDINJECT=1 hack/run-e2e-kind.sh ./internal/e2e/suites/egresscredinject -v -args --no-color
func TestActorEgressCredentialInjection(t *testing.T) {
	if os.Getenv("E2E_EGRESS_CREDINJECT") == "" {
		t.Skip("needs the sdsmint (MITM) egress gateway with credential injection: deploy with hack/install-ate-kind.sh --deploy-atenet --experimental-use-sdsmint --experimental-egress-credential-injection, then set E2E_EGRESS_CREDINJECT=1")
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

	// The first fetch retries two transient failure modes for one
	// propagation window before counting:
	//
	//   - certificate errors: sdsmintd signs with the pool mounted into the
	//     gateway pod, and kubelet propagates Secret contents into that mount
	//     on its own schedule (see TestActorEgressMITMTrust);
	//   - 503 denials: the injector maps a provider it cannot reach to a
	//     retryable 503 by design (see egress.mapCredentialProviderError).
	//     The gateway's gRPC channel to the provider outlives this suite's
	//     provider redeploys, so right after one — a rerun, most likely — it
	//     can still be in connect backoff against the old, deleted Service.
	deadline := time.Now().Add(2 * time.Minute)
	var injected fetchResponse
	for {
		injected = probeFetch(t, ctx, rc, id, echoOrigin, nil)
		isCertErr := strings.Contains(injected.Error, "certificate") || strings.Contains(injected.Error, "x509")
		retryable := (injected.Error != "" && isCertErr) || (injected.Error == "" && injected.Status == "503")
		if !retryable || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Second)
	}
	wantHeader := "Bearer " + e2e.CredentialInjectionToken
	if got := assertEchoedAuthorization(t, "injection fetch", injected); got != wantHeader {
		t.Errorf("upstream received Authorization %q, want the injected %q", got, wantHeader)
	}

	// An actor-set Authorization header must not survive injection: the
	// gateway overwrites it, so a client cannot pre-seed a credential.
	seeded := probeFetch(t, ctx, rc, id, echoOrigin, []string{"header=" + url.QueryEscape("Authorization:Bearer actor-forged")})
	if got := assertEchoedAuthorization(t, "pre-seeded-header fetch", seeded); got != wantHeader {
		t.Errorf("upstream received Authorization %q after the actor pre-seeded its own, want the injected %q", got, wantHeader)
	}

	// The same origin over plain HTTP: the cleartext leg skips injection and
	// passes the request through, so the fetch succeeds and the upstream sees
	// no Authorization header at all. (The probe does not follow redirects, so
	// an origin-side upgrade to HTTPS would surface as a non-200 here rather
	// than silently re-running the TLS case.)
	cleartext := probeFetch(t, ctx, rc, id, echoOriginPlain, nil)
	if cleartext.Error != "" {
		t.Errorf("cleartext fetch of %s failed at the transport: %s", echoOriginPlain, cleartext.Error)
	} else if cleartext.Status != "200" {
		t.Errorf("cleartext fetch of %s: status %s, want 200 (the request should pass through without the credential)", echoOriginPlain, cleartext.Status)
	} else if headers := decodeEchoedHeaders(t, "cleartext fetch", cleartext.Body); headers["Authorization"] != "" {
		t.Errorf("cleartext request arrived with Authorization %q, want none: a credential was put on a cleartext wire", headers["Authorization"])
	}

	// Fail closed: each of these rules names a credential that cannot be
	// injected, and the denial must be the mapped status, not a request that
	// went out without the credential.
	for _, tc := range []struct {
		name       string
		origin     string
		wantStatus string
	}{
		{"unfetchable secret", unfetchableOrigin, "403"},
		{"unserved provider", unservedOrigin, "500"},
		{"unauthorized namespace", unauthorizedOrigin, "403"},
	} {
		got := probeFetch(t, ctx, rc, id, tc.origin, nil)
		if got.Error != "" {
			t.Errorf("%s: fetch of %s failed at the transport (%s), want an HTTP %s from the gateway", tc.name, tc.origin, got.Error, tc.wantStatus)
		} else if got.Status != tc.wantStatus {
			t.Errorf("%s: fetch of %s returned status %s, want %s", tc.name, tc.origin, got.Status, tc.wantStatus)
		}
	}
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
	for {
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
			t.Fatalf("GET %s for %q: status %d, body %q", path, id, resp.StatusCode, body)
		}
		time.Sleep(2 * time.Second)
	}
}

// createAndResumeActor mirrors the egressmitm suite's self-healing actor
// lifecycle (actor records outlive the fixture namespace); DeployProbe has
// already waited for the template's golden snapshot.
func createAndResumeActor(t *testing.T, ctx context.Context, clients *e2e.Clients, id string) {
	t.Helper()
	ref := &ateapipb.ObjectRef{Atespace: probeNamespace, Name: id}
	_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
	_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref})
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: probeNamespace, Name: id},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: probeNamespace, Name: probeTemplate},
	}}); err != nil {
		t.Fatalf("CreateActor %q: %v", id, err)
	}
	// One rule per hostname, each carrying the injection whose outcome that
	// host is used to observe. Effects apply only on the first matching rule,
	// and only these hosts are allowed at all.
	e2e.EnsureEgressPolicy(t, ctx, clients, ref,
		e2e.EgressInjectHeader("Authorization", "Bearer ", e2e.CredentialInjectionURI, echoHost),
		e2e.EgressInjectHeader("Authorization", "Bearer ",
			"ate-secret://k8s.io/default/"+e2e.CredentialSecretsNamespace+"/no-such-secret/token", unfetchableHost),
		e2e.EgressInjectHeader("Authorization", "Bearer ",
			"ate-secret://other.io/default/"+e2e.CredentialSecretsNamespace+"/api-token/token", unservedHost),
		e2e.EgressInjectHeader("Authorization", "Bearer ",
			"ate-secret://k8s.io/default/kube-system/api-token/token", unauthorizedHost),
	)
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
		if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref}); err != nil {
			t.Logf("cleanup: DeleteActor %q failed, actor leaked (remove with: kubectl ate delete actor %s -a %s): %v", id, id, probeNamespace, err)
		}
	})
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
