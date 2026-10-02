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

// Package egressmitm e2e-tests the trust half of MITM'd egress TLS (#871):
// an actor that projects the egress trust bundle can complete a TLS
// handshake with the egress gateway's per-SNI minted leaf, using
// ONLY the projected anchors. See TestActorEgressMITMTrust for the proof
// structure and how to run this locally.
package egressmitm

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

const probeTemplate = "probe"

var probeNamespace string

// TestActorEgressMITMTrust proves an actor can do TLS through the MITM
// egress gateway using only the projected trust bundle. The origins run in the
// cluster and serve certificates from the service-DNS signer, which the
// gateway trusts once hack/setup-e2e-egress-tls-kind.sh has run and the
// probe's bundle does not:
//
//   - positive: /fetch with roots=bundle succeeds — the gateway's per-SNI
//     minted leaf (signed from the egress-mitm-ca-pool) validates against
//     the anchors atelet projected from the reconciler-published bundle.
//     The bundle holds no service-DNS CA, so a pass certifies interception
//     is on.
//   - negative: /fetch trusting only the service-DNS CA fails certificate
//     verification — the actor saw the minted leaf, not the origin's own
//     certificate (were the traffic relayed, this fetch would succeed, and
//     the positive case would be meaningless).
//
// CI runs this in both lanes (see pr-workflow.yaml) — once per sandbox class,
// since trust delivery differs per class (gVisor RO bind vs the micro-VM
// unified virtio-fs share). Locally:
//
//	hack/setup-e2e-egress-tls-kind.sh
//	hack/run-e2e-kind.sh ./internal/e2e/suites/egressmitm -v -args --no-color
//	E2E_SANDBOX_CLASS=microvm hack/run-e2e-kind.sh ./internal/e2e/suites/egressmitm -v -args --no-color
//
// The micro-VM variant additionally needs the micro-VM deps installed
// (hack/run-microvm-demo-kind.sh, or hack/install-microvm-deps.sh --install).
func TestActorEgressMITMTrust(t *testing.T) {
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	// Ensure, never replace: the gateway signs with the pool mounted into its
	// pod, and kubelet propagates Secret updates into that mount on
	// its own schedule — replacing the pool here would race the propagation
	// and flake the handshake. The install created the pool; we
	// only wait for the reconciler-derived bundle so actor start can resolve
	// the projection. (DeployProbe ensures too; this makes the dependency
	// explicit and fails with the clearer message when the reconciler is
	// missing.)
	e2e.EnsureEgressTrustBundle(t, ctx, clients)

	// One pod answers http and https for the intercepted name; passthrough is
	// decided by name, so it needs an origin of its own.
	mitm := e2e.DeployServerPod(t, ctx, originSpec("mitm-origin"))
	passthroughSpec := originSpec("passthrough-origin")
	passthroughSpec.Namespace = mitm.Namespace
	passthrough := e2e.DeployServerPod(t, ctx, passthroughSpec)
	originRoots := "ca=" + url.QueryEscape(e2e.ServiceDNSTrustBundle(t, ctx, clients))

	probeNamespace, _ = e2e.DeployProbe(t, env["BUCKET_NAME"], "egressmitm", e2e.WithTrustBundle(), e2e.WithSystemRoots())

	const id = "probe-mitm"
	createAndResumeActor(t, ctx, clients, id, mitm, passthrough)
	waitForActorState(t, ctx, clients, id, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	rc, err := e2e.NewRouterClient(t, ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer rc.Close()

	origin := "https://" + mitm.SvcName() + "/healthz"

	pos := fetchUntilReached(t, ctx, rc, id, origin, "bundle")
	if pos.Error != "" {
		t.Fatalf("TLS through the MITM egress gateway with the projected trust bundle failed: %s — the projected anchors did not validate the gateway's minted leaf (or interception/minting is broken)", pos.Error)
	}
	if pos.Status != "200" {
		t.Fatalf("fetch %s via projected bundle: status %s, want 200 (a 5xx is the gateway failing to reach or trust the origin; has hack/setup-e2e-egress-tls-kind.sh run?) body %q", origin, pos.Status, pos.Body)
	}

	neg := probeFetch(t, ctx, rc, id, origin, "pem", originRoots)
	if neg.Error == "" {
		t.Errorf("fetch trusting only the origin's CA unexpectedly succeeded (status %s): the actor saw the origin's own certificate — is the egress gateway actually terminating TLS?", neg.Status)
	} else if !strings.Contains(neg.Error, "certificate") && !strings.Contains(neg.Error, "x509") {
		t.Errorf("fetch trusting only the origin's CA failed, but not with a certificate-verification error: %s", neg.Error)
	}

	// A port in the Host is never dialed: no rule checked it. The gateway
	// dials the Host's name on the port the actor dialed, and nothing answers
	// on port 1.
	for _, tc := range []struct{ origin, dial string }{
		{origin: "https://" + mitm.SvcName() + ":1/healthz", dial: mitm.TLSAddress()},
		{origin: "http://" + mitm.SvcName() + ":1/healthz", dial: mitm.Address()},
	} {
		res := probeFetch(t, ctx, rc, id, tc.origin, "bundle", "dial="+url.QueryEscape(tc.dial))
		switch {
		case res.Error != "":
			t.Errorf("fetch of %s dialed at %s failed: %s", tc.origin, tc.dial, res.Error)
		case res.Status != "200":
			t.Errorf("fetch of %s dialed at %s: status %s, want 200 — the gateway dialed the port in the Host instead of the one the actor dialed", tc.origin, tc.dial, res.Status)
		}
	}

	// The gateway relays passthrough TLS unread, so the origin's own
	// certificate must validate against its CA alone. That is also the proof it
	// was not intercepted.
	passthroughOrigin := "https://" + passthrough.SvcName() + "/healthz"
	pt := fetchUntilReached(t, ctx, rc, id, passthroughOrigin, "pem", originRoots)
	if pt.Error != "" {
		t.Fatalf("TLS passthrough to %s failed: %s — the origin's certificate did not validate against its CA, so the connection was intercepted, misrouted, or closed", passthroughOrigin, pt.Error)
	}
	if pt.Status != "200" {
		t.Fatalf("passthrough fetch %s: status %s, want 200; body %q", passthroughOrigin, pt.Status, pt.Body)
	}

	// The gateway dials the name the SNI claims.
	bySNI := fetchUntilReached(t, ctx, rc, id, passthroughOrigin, "pem", originRoots, "dial="+url.QueryEscape(unreachableAddress))
	switch {
	case bySNI.Error != "":
		t.Errorf("TLS passthrough to %s dialed at %s failed: %s — the gateway dialed the actor's address instead of resolving the SNI", passthroughOrigin, unreachableAddress, bySNI.Error)
	case bySNI.Status != "200":
		t.Errorf("passthrough fetch %s dialed at %s: status %s, want 200", passthroughOrigin, unreachableAddress, bySNI.Status)
	}

	// Envoy closes denied connections at the ClientHello; agentgateway
	// terminates TLS to return 403. Trust the gateway's CA so a certificate
	// failure cannot masquerade as either policy-denial response.
	//
	// Each name is an origin's fully qualified alias: rules match names
	// exactly, so only the wrong-port rule lists the first and none the
	// second. A gateway that relayed either would hand the actor a
	// certificate the bundle rejects, and the dial keeps the actor off DNS.
	for _, tc := range []struct{ name, origin, dial string }{
		{"passthrough rule excludes port 443", "https://" + passthrough.SvcName() + ".cluster.local/healthz", passthrough.TLSAddress()},
		{"host outside the policy", "https://" + mitm.SvcName() + ".cluster.local/healthz", mitm.TLSAddress()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			denied := probeFetch(t, ctx, rc, id, tc.origin, "bundle", "dial="+url.QueryEscape(tc.dial))
			switch {
			case denied.Status == "403" && denied.Error == "":
				// An explicit denial after completing TLS with the actor.
			case denied.Error == "":
				t.Errorf("fetch of %s returned status %s, want 403 or a connection closed at the ClientHello", tc.origin, denied.Status)
			case strings.Contains(denied.Error, "certificate") || strings.Contains(denied.Error, "x509"):
				t.Errorf("fetch of %s failed certificate verification instead of returning a policy denial: %s", tc.origin, denied.Error)
			case denied.Status != "":
				t.Errorf("fetch of %s got status %s with error %q, want no HTTP exchange after a TLS failure", tc.origin, denied.Status, denied.Error)
			}
		})
	}

	// With every origin in the cluster, no fetch here verifies a public
	// certificate, so check that atelet merged its WebPKI roots into the
	// projected bundle, as the egress demo's template asks it to.
	t.Run("projected bundle holds the system roots", func(t *testing.T) {
		if !holdsCertificate(t, probeTrust(t, ctx, rc, id), systemRoot) {
			t.Errorf("the probe's projected trust bundle has no certificate for %q; atelet did not merge %s into it", systemRoot, "system-roots.ate.dev")
		}
	})
}

// originSpec is an origin answering /healthz over https and http alike.
func originSpec(name string) e2e.ServerPod {
	return e2e.ServiceDNSTLS(e2e.ServerPod{
		Name:       name,
		ImportPath: "go.ate.dev/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"http"},
	})
}

const (
	// unreachableAddress is in the reserved 240/4 block: a gateway that dialed
	// it instead of the resolved SNI would reach nothing.
	unreachableAddress = "240.0.0.1:443"
	// systemRoot is the common name of a WebPKI root atelet's image ships.
	systemRoot = "ISRG Root X1"
)

type fetchResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
	Body   string `json:"body"`
}

// reachWindow bounds the retries of a fetch that must return 200.
const reachWindow = 2 * time.Minute

// fetchUntilReached is probeFetch for a fetch that must return 200. It retries
// transient failures for up to reachWindow, then returns the last result for
// the caller to fail on. A denial or any other final status returns at once.
// The failures it rides out:
//
//   - a certificate error: the gateway signs with the pool mounted into its
//     pod, and kubelet propagates Secret contents into that mount on its own
//     schedule (up to ~1 minute). In CI the pool predates the gateway pod,
//     but a LOCAL rerun can recreate the pool moments before this fetch (a
//     prior run's cleanup deleted it), leaving the gateway briefly signing
//     with the old CA;
//   - any other transport error, and a 5xx: the actor's route, or the
//     gateway's path to the origin, is still propagating.
//
// It never skips. Reaching the origins is part of what the lane proves, and a
// skip would report that as a pass (docs/dev/best-practices/ci-fail-closed.md).
func fetchUntilReached(t *testing.T, ctx context.Context, rc *e2e.RouterClient, id, origin, roots string, extraParams ...string) fetchResponse {
	t.Helper()
	deadline := time.Now().Add(reachWindow)
	for {
		resp := probeFetch(t, ctx, rc, id, origin, roots, extraParams...)
		if !transientFailure(resp) || time.Now().After(deadline) {
			return resp
		}
		t.Logf("fetch of %s: transient failure (status %q, error %q), retrying", origin, resp.Status, resp.Error)
		time.Sleep(5 * time.Second)
	}
}

// transientFailure reports whether fetchUntilReached should retry resp.
func transientFailure(resp fetchResponse) bool {
	return resp.Error != "" || strings.HasPrefix(resp.Status, "5")
}

// probeFetch asks the probe to fetch origin with the given roots mode;
// extraParams are further query parameters, already encoded.
// Router-level failures are retried for up to 30s (a resume can return
// before the route reaches the router's xDS snapshot); probe-level TLS
// failures are results, returned for the caller to assert on.
func probeFetch(t *testing.T, ctx context.Context, rc *e2e.RouterClient, id, origin, roots string, extraParams ...string) fetchResponse {
	t.Helper()
	path := "/fetch?roots=" + roots + "&url=" + url.QueryEscape(origin)
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

// probeTrust returns the trust bundle the probe's /whoami reads from its
// projected trust file.
func probeTrust(t *testing.T, ctx context.Context, rc *e2e.RouterClient, id string) string {
	t.Helper()
	ref := resources.ActorRef{Atespace: probeNamespace, Name: id}
	resp, err := rc.Get(ctx, ref, "/whoami")
	if err != nil {
		t.Fatalf("GET /whoami for %q: %v", id, err)
	}
	defer resp.Body.Close()
	var who struct {
		Trust string `json:"trust"`
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&who); err != nil {
		t.Fatalf("decoding /whoami for %q (status %d): %v", id, resp.StatusCode, err)
	}
	if who.Trust == "" {
		t.Fatalf("/whoami for %q returned no trust bundle; error: %s", id, who.Error)
	}
	return who.Trust
}

// holdsCertificate reports whether bundle has a certificate whose subject
// common name is commonName.
func holdsCertificate(t *testing.T, bundle, commonName string) bool {
	t.Helper()
	for rest := []byte(bundle); ; {
		var block *pem.Block
		if block, rest = pem.Decode(rest); block == nil {
			return false
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatalf("parsing a certificate in the trust bundle: %v", err)
		}
		if cert.Subject.CommonName == commonName {
			return true
		}
	}
}

// createAndResumeActor mirrors the identity suite's self-healing actor
// lifecycle (actor records outlive the fixture namespace); DeployProbe has
// already waited for the template's golden snapshot.

func createAndResumeActor(t *testing.T, ctx context.Context, clients *e2e.Clients, id string, mitm, passthrough e2e.Server) {
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
	// The gateway refuses every tunnel for an actor without a policy. Naming
	// only the origins also lets the same actor show a denial. The
	// passthrough origin's fully qualified alias is allowed on 8443 only,
	// while the probe dials 443.
	e2e.EnsureEgressPolicy(t, ctx, clients, ref,
		e2e.EgressAllowHTTP(mitm.SvcName(), mitm.SvcName()+".cluster.local"),
		e2e.EgressAllowHTTPS(mitm.SvcName()),
		e2e.EgressAllowPassthrough(passthrough.SvcName()),
		e2e.EgressAllowPassthroughOnPorts([]int32{8443}, passthrough.SvcName()+".cluster.local"),
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
