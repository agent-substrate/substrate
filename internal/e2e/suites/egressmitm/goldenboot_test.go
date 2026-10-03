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

	"github.com/agent-substrate/substrate/internal/e2e"
)

var bootFetchManifests = e2e.SubstrateFixtureManifests{
	Pool:     "internal/e2e/fixtures/probe/probe.yaml.tmpl",
	Template: "internal/e2e/fixtures/probe/probe-bootfetch-template.yaml.tmpl",
}

// TestGoldenBootEgress proves an actor has egress while it boots: the
// probe-bootfetch workload fetches https://example.com/ through the MITM
// gateway before it listens, and exits if it cannot, so its template gets a
// golden snapshot only if the worker tunneled the golden actor's egress
// before the wakeup probe and the gateway admitted the actor while it was
// still resuming.
//
//	hack/run-e2e-kind.sh ./internal/e2e/suites/egressmitm -run TestGoldenBootEgress -v -args --no-color
func TestGoldenBootEgress(t *testing.T) {
	if !e2e.CurrentAtenetDataplane().SupportsEgressWhileResuming() {
		t.Skip("the agentgateway dataplane admits egress from RUNNING actors only")
	}
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()
	e2e.EnsureEgressTrustBundle(t, ctx, clients)

	// DeploySubstrateFixture returns once every template has its golden
	// snapshot, which is the assertion.
	e2e.DeploySubstrateFixture(t, ctx, clients, bootFetchManifests, env["BUCKET_NAME"], "egressmitm-bootfetch", true,
		e2e.WithGoldenEgressPolicy(e2e.EgressAllowHTTPS(egressOriginHost)))
}
