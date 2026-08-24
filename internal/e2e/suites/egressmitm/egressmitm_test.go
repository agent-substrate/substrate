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
// handshake with the sdsmint egress gateway's per-SNI minted leaf, using
// ONLY the projected anchors. See TestActorEgressMITMTrust for the proof
// structure and how to run this locally.
package egressmitm

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
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const probeTemplate = "probe"

var probeNamespace string

// TestActorEgressMITMTrust proves an actor can do TLS through the MITM
// egress gateway using only the projected trust bundle:
//
//   - positive: /fetch with roots=bundle succeeds — the gateway's per-SNI
//     minted leaf (signed from the egress-mitm-ca-pool) validates against
//     the anchors atelet projected from the reconciler-published bundle.
//     This also fails under a PASSTHROUGH gateway (the bundle holds no
//     public CAs), so a pass certifies interception is on.
//   - negative: /fetch with roots=system fails certificate verification —
//     the minted leaf chains to no public CA, proving the traffic really is
//     intercepted rather than relayed (under passthrough this fetch would
//     succeed, and the positive case would be meaningless).
//
// The gate: this needs the sdsmint egress gateway variant, which replaces
// the passthrough gateway cluster-wide, so CI runs it as separate steps
// after the standard lanes (see pr-workflow.yaml) — once per sandbox class,
// since trust delivery differs per class (gVisor RO bind vs the micro-VM
// unified virtio-fs share). Locally:
//
//	hack/install-ate-kind.sh --deploy-atenet --experimental-use-sdsmint
//	E2E_EGRESS_MITM=1 hack/run-e2e-kind.sh ./internal/e2e/suites/egressmitm -v -args --no-color
//	E2E_EGRESS_MITM=1 E2E_SANDBOX_CLASS=microvm hack/run-e2e-kind.sh ./internal/e2e/suites/egressmitm -v -args --no-color
//
// The micro-VM variant additionally needs the micro-VM deps installed
// (hack/run-microvm-demo-kind.sh, or hack/install-microvm-deps.sh --install).
func TestActorEgressMITMTrust(t *testing.T) {
	if os.Getenv("E2E_EGRESS_MITM") == "" {
		t.Skip("needs the sdsmint (MITM) egress gateway: deploy with hack/install-ate-kind.sh --deploy-atenet --experimental-use-sdsmint, then set E2E_EGRESS_MITM=1")
	}
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	// Ensure, never replace: sdsmintd signs with the pool mounted into the
	// gateway pod, and kubelet propagates Secret updates into that mount on
	// its own schedule — replacing the pool here would race the propagation
	// and flake the handshake. The sdsmint install path created the pool; we
	// only wait for the reconciler-derived bundle so actor start can resolve
	// the projection. (DeployProbe ensures too; this makes the dependency
	// explicit and fails with the clearer message when the reconciler is
	// missing.)
	e2e.EnsureEgressTrustBundle(t, ctx, clients)

	probeNamespace = e2e.DeployProbe(t, env["BUCKET_NAME"], "egressmitm", e2e.WithTrustBundle())
	waitForGolden(t, ctx, clients)

	const id = "probe-mitm"
	createAndResumeActor(t, ctx, clients, id)
	waitForActorState(t, ctx, clients, id, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	rc, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer rc.Close()

	const origin = "https://example.com/"

	// sdsmintd signs with the pool mounted into the gateway pod, and kubelet
	// propagates Secret contents into that mount on its own schedule (up to
	// ~1 minute). In CI the pool predates the gateway pod, but a LOCAL rerun
	// can recreate the pool moments before this fetch (a prior run's cleanup
	// deleted it), leaving the gateway briefly signing with the old CA — so
	// certificate failures retry for one propagation window before counting.
	deadline := time.Now().Add(2 * time.Minute)
	var pos fetchResponse
	for {
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
		t.Errorf("fetch with system roots unexpectedly succeeded (status %s): the minted leaf should chain to no public CA — is the sdsmint (MITM) gateway actually deployed, or is egress running in passthrough mode?", neg.Status)
	} else if !strings.Contains(neg.Error, "certificate") && !strings.Contains(neg.Error, "x509") {
		t.Errorf("fetch with system roots failed, but not with a certificate-verification error: %s", neg.Error)
	}
}

type fetchResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
}

// probeFetch asks the probe to fetch origin with the given roots mode.
// Router-level failures are retried for up to 30s (a resume can return
// before the route reaches the router's xDS snapshot); probe-level TLS
// failures are results, returned for the caller to assert on.
func probeFetch(t *testing.T, ctx context.Context, rc *e2e.RouterClient, id, origin, roots string) fetchResponse {
	t.Helper()
	path := "/fetch?roots=" + roots + "&url=" + url.QueryEscape(origin)
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

// The helpers below mirror the identity suite's: fixture golden wait and a
// self-healing actor lifecycle (actor records outlive the fixture namespace).

func waitForGolden(t *testing.T, ctx context.Context, clients *e2e.Clients) {
	t.Helper()
	deadline := time.Now().Add(e2e.TemplateReadyTimeout(t))
	for time.Now().Before(deadline) {
		at, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(probeNamespace).Get(ctx, probeTemplate, metav1.GetOptions{})
		if err == nil {
			switch at.Status.Phase {
			case v1alpha1.PhaseReady:
				return
			case v1alpha1.PhaseFailed:
				t.Fatalf("probe ActorTemplate entered PhaseFailed")
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for probe ActorTemplate to be Ready")
}

func createAndResumeActor(t *testing.T, ctx context.Context, clients *e2e.Clients, id string) {
	t.Helper()
	_, _ = clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: probeNamespace}}})
	ref := &ateapipb.ObjectRef{Atespace: probeNamespace, Name: id}
	_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
	_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref})
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: probeNamespace, Name: id},
		ActorTemplateNamespace: probeNamespace,
		ActorTemplateName:      probeTemplate,
	}}); err != nil {
		t.Fatalf("CreateActor %q: %v", id, err)
	}
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
