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
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const probeTemplate = "probe"

var probeNamespace string

// TestActorEgressMITMTrust proves an actor can do TLS through the MITM
// egress gateway using only the projected trust bundle:
//
//   - positive: /fetch with roots=bundle succeeds — the gateway's per-SNI
//     minted leaf (signed from the egress-mitm-ca-pool) validates against
//     the anchors atelet projected from the reconciler-published bundle.
//     The bundle holds no public CAs, so a pass certifies interception is
//     on.
//   - negative: /fetch with roots=system fails certificate verification —
//     the minted leaf chains to no public CA, proving the traffic really is
//     intercepted rather than relayed (were it relayed this fetch would
//     succeed, and the positive case would be meaningless).
//
// CI runs this in both lanes (see pr-workflow.yaml) — once per sandbox class,
// since trust delivery differs per class (gVisor RO bind vs the micro-VM
// unified virtio-fs share). Locally:
//
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

	probeNamespace, _ = e2e.DeployProbe(t, env["BUCKET_NAME"], "egressmitm", e2e.WithTrustBundle())

	const id = "probe-mitm"
	createAndResumeActor(t, ctx, clients, id)
	waitForActorState(t, ctx, clients, id, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	rc, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer rc.Close()

	const origin = "https://" + egressOriginHost + "/"

	// The gateway signs with the pool mounted into its pod, and kubelet
	// propagates Secret contents into that mount on its own schedule (up to
	// ~1 minute). In CI the pool predates the gateway pod, but a LOCAL rerun
	// can recreate the pool moments before this fetch (a prior run's cleanup
	// deleted it), leaving the gateway briefly signing with the old CA — so
	// certificate failures retry for one propagation window before counting.
	deadline := time.Now().Add(2 * time.Minute)
	var (
		beforeExtProc map[string]int
		pos           fetchResponse
	)
	for {
		beforeScrape, err := e2e.ScrapeEgressEnvoyMetrics(ctx)
		if err != nil {
			t.Fatalf("ScrapeEgressEnvoyMetrics before fetches: %v", err)
		}
		beforeExtProc = e2e.EgressExtProcStreamCounts(beforeScrape)

		pos = probeFetch(t, ctx, rc, id, origin, "bundle")
		isCertErr := strings.Contains(pos.Error, "certificate") || strings.Contains(pos.Error, "x509")
		if pos.Error == "" || !isCertErr || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Second)
	}
	if pos.Error != "" {
		t.Fatalf("TLS through the MITM egress gateway with the projected trust bundle failed: %s — the projected anchors did not validate the gateway's minted leaf (or interception/minting is broken)", pos.Error)
	}
	if pos.Status != "200" {
		t.Fatalf("fetch %s via projected bundle: status %s, want 200", origin, pos.Status)
	}

	neg := probeFetch(t, ctx, rc, id, origin, "system")
	if neg.Error == "" {
		t.Errorf("fetch with system roots unexpectedly succeeded (status %s): the minted leaf should chain to no public CA — is the egress gateway actually terminating TLS?", neg.Status)
	} else if !strings.Contains(neg.Error, "certificate") && !strings.Contains(neg.Error, "x509") {
		t.Errorf("fetch with system roots failed, but not with a certificate-verification error: %s", neg.Error)
	}

	// The gateway relays passthrough TLS unread, so the origin's own certificate
	// must validate with the system roots. That is also the proof it was not
	// intercepted.
	const passthroughOrigin = "https://" + egressOriginPassthroughHost + "/"
	pt := probeFetch(t, ctx, rc, id, passthroughOrigin, "system")
	if pt.Error != "" {
		t.Fatalf("TLS passthrough to %s failed: %s — the origin's certificate did not validate against the system roots, so the connection was intercepted, misrouted, or closed", passthroughOrigin, pt.Error)
	}
	if pt.Status != "200" {
		t.Fatalf("passthrough fetch %s: status %s, want 200", passthroughOrigin, pt.Status)
	}

	// The gateway dials the name the SNI claims.
	bySNI := probeFetch(t, ctx, rc, id, passthroughOrigin, "system", "dial="+url.QueryEscape(unreachableAddress))
	switch {
	case bySNI.Error != "":
		t.Errorf("TLS passthrough to %s dialed at %s failed: %s — the gateway dialed the actor's address instead of resolving the SNI", passthroughOrigin, unreachableAddress, bySNI.Error)
	case bySNI.Status != "200":
		t.Errorf("passthrough fetch %s dialed at %s: status %s, want 200", passthroughOrigin, unreachableAddress, bySNI.Status)
	}

	// The rule for this name covers 8443 only, so the ClientHello on 443 is
	// closed.
	const wrongPortOrigin = "https://" + egressOriginPassthroughWrongPortHost + "/"
	wrongPort := probeFetch(t, ctx, rc, id, wrongPortOrigin, "system")
	switch {
	case wrongPort.Error == "":
		t.Errorf("fetch of %s succeeded with status %s, want the connection closed at the ClientHello: its passthrough rule names port 8443, not 443", wrongPortOrigin, wrongPort.Status)
	case strings.Contains(wrongPort.Error, "certificate") || strings.Contains(wrongPort.Error, "x509"):
		t.Errorf("fetch of %s was intercepted (certificate error %q), want the connection closed at the ClientHello", wrongPortOrigin, wrongPort.Error)
	case wrongPort.Status != "":
		t.Errorf("fetch of %s got status %s with error %q, want no HTTP exchange at all", wrongPortOrigin, wrongPort.Status, wrongPort.Error)
	}

	// A host outside the policy is closed at the ClientHello: expect a
	// transport error, not a certificate error or an HTTP status. The error
	// text varies, so only its presence is checked.
	denied := probeFetch(t, ctx, rc, id, "https://example.org/", "bundle")
	switch {
	case denied.Error == "":
		t.Errorf("fetch of a host outside the policy succeeded with status %s, want the connection closed at the ClientHello", denied.Status)
	case strings.Contains(denied.Error, "certificate") || strings.Contains(denied.Error, "x509"):
		t.Errorf("fetch of a host outside the policy was intercepted (certificate error %q), want the connection closed at the ClientHello", denied.Error)
	case denied.Status != "":
		t.Errorf("fetch of a host outside the policy got status %s with error %q, want no HTTP exchange at all", denied.Status, denied.Error)
	}

	if !e2e.CurrentAtenetDataplane().IsDataplaneConcurrencyDisabled() {
		t.Log("skipping counter verification: dataplane concurrency is not disabled")
		return
	}

	afterScrape, err := e2e.ScrapeEgressEnvoyMetrics(ctx)
	if err != nil {
		t.Fatalf("ScrapeEgressEnvoyMetrics after fetches: %v", err)
	}
	afterExtProc := e2e.EgressExtProcStreamCounts(afterScrape)

	// Each of the 6 fetches opens a CONNECT tunnel (egress_identity), while
	// only the positive MITM fetch completes the inner TLS handshake and sends
	// an HTTP request through the MITM chain (egress_policy_mitm).
	// Since caching is enabled only the first CONNECT request will result in ext_proc call.
	const (
		wantIdentityCalls   = 1
		wantMITMPolicyCalls = 1
	)
	if got := afterExtProc[e2e.EgressExtProcIdentityStatPrefix] - beforeExtProc[e2e.EgressExtProcIdentityStatPrefix]; got != wantIdentityCalls {
		t.Errorf("egress ext_proc %s streams_started delta = %d, want %d", e2e.EgressExtProcIdentityStatPrefix, got, wantIdentityCalls)
	}
	if got := afterExtProc[e2e.EgressExtProcPolicyMITMStatPrefix] - beforeExtProc[e2e.EgressExtProcPolicyMITMStatPrefix]; got != wantMITMPolicyCalls {
		t.Errorf("egress ext_proc %s streams_started delta = %d, want %d", e2e.EgressExtProcPolicyMITMStatPrefix, got, wantMITMPolicyCalls)
	}
}

// egressOriginHost is the one host the probe actor's EgressPolicy allows.
const (
	egressOriginHost            = "example.com"
	egressOriginPassthroughHost = "example.edu"
	// egressOriginPassthroughWrongPortHost is allowed by passthrough on 8443
	// only, while the probe dials 443.
	egressOriginPassthroughWrongPortHost = "example.net"
	// unreachableAddress is in the reserved 240/4 block: a gateway that dialed
	// it instead of the resolved SNI would reach nothing.
	unreachableAddress = "240.0.0.1:443"
)

type fetchResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
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

// createAndResumeActor mirrors the identity suite's self-healing actor
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
	// The gateway refuses every tunnel for an actor without a policy. Naming
	// only the origin also lets the same actor show a denial.
	e2e.EnsureEgressPolicy(t, ctx, clients, ref,
		e2e.EgressAllowHTTPS(egressOriginHost),
		e2e.EgressAllowPassthrough(egressOriginPassthroughHost),
		e2e.EgressAllowPassthroughOnPorts([]int32{8443}, egressOriginPassthroughWrongPortHost),
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
