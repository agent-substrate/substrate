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

package egressmitm

import (
	"context"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/preview"
)

var bootFetchManifests = e2e.SubstrateFixtureManifests{
	Pool:     "internal/e2e/fixtures/probe/probe.yaml.tmpl",
	Template: "internal/e2e/fixtures/probe/probe-bootfetch-template.yaml.tmpl",
}

// TestGoldenBootEgress proves an actor has egress while it boots: the
// probe-bootfetch workload fetches an in-cluster origin through the MITM
// gateway before it listens, and exits if it cannot, so its template gets a
// golden snapshot only if the worker tunneled the golden actor's egress
// before the wakeup probe and the gateway admitted the actor while it was
// still resuming. The template's goldenEgressPolicy gives the golden actor its
// policy before the controller resumes it. An actor restored from that
// snapshot then has to fetch again as itself before its wakeup probe passes,
// which covers the restore paths.
//
// goldenEgressPolicy is a preview feature: install the cluster with its gate
// and pass the gate to the suite, which skips the test otherwise.
//
//	ATE_PREVIEW=GoldenEgressPolicy hack/install-ate-kind.sh
//	hack/run-e2e-kind.sh ./internal/e2e/suites/egressmitm -run TestGoldenBootEgress -v -args --no-color --preview=GoldenEgressPolicy
func TestGoldenBootEgress(t *testing.T) {
	e2e.RequirePreview(t, preview.GateGoldenEgressPolicy)
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()
	e2e.EnsureEgressTrustBundle(t, ctx, clients)
	origin := e2e.DeployServerPod(t, ctx, originSpec("bootfetch-origin"))

	// DeploySubstrateFixture returns once every template has its golden
	// snapshot, which is the assertion.
	atespace, _ := e2e.DeploySubstrateFixture(t, ctx, clients, bootFetchManifests, env["BUCKET_NAME"], "egressmitm-bootfetch", []string{e2e.EgressTrustBundleName},
		e2e.WithTemplateValue("BOOT_FETCH_HOST", origin.SvcName()),
		e2e.WithTemplateValue("BOOT_FETCH_URL", "https://"+origin.SvcName()+"/healthz"))

	t.Run("restore", func(t *testing.T) {
		const id = "bootfetch-restore"
		ref := &ateapi.ObjectRef{Atespace: atespace, Name: id}
		if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
			Metadata:      &ateapi.ResourceMetadata{Atespace: atespace, Name: id},
			ActorTemplate: &ateapi.ObjectRef{Atespace: atespace, Name: "probe-bootfetch"},
		}}); err != nil {
			t.Fatalf("CreateActor %q: %v", id, err)
		}
		t.Cleanup(func() {
			_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: ref})
			if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: ref}); err != nil {
				t.Logf("cleanup: DeleteActor %q failed: %v", id, err)
			}
		})
		// Before the resume, so the gateway never caches a missing policy.
		e2e.EnsureEgressPolicy(t, ctx, clients, ref, e2e.EgressAllowHTTPS(origin.SvcName(), origin.SvcName()+".cluster.local"))
		if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
			t.Fatalf("ResumeActor %q: %v", id, err)
		}
		waitForActorState(t, ctx, clients, atespace, id, ateapi.ActorState_ACTOR_STATE_RUNNING)
	})
}
