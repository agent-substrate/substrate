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
	"testing"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	corev1 "k8s.io/api/core/v1"
)

// TestActorEgressPerFamily asserts an Actor reaches an in-cluster destination
// over the family that destination is published in, including -- the case no
// single-family cluster can construct -- a destination published in both.
//
// The destination is in-cluster on purpose. The suite's other egress tests
// fetch example.com, so which families are in play is decided by whatever
// resolver the cluster inherited: when the name has no AAAA the Actor never
// attempts IPv6 and a broken IPv6 path passes. Services over one backend make
// the families a property of the test instead.
//
// The dual subtest is the load-bearing one. An Actor with an IPv6 address
// prefers the AAAA, so a dual-homed destination becomes unreachable the moment
// IPv6 egress is broken -- even though a working A record is right there, and
// even though every IPv4-only destination still works. That is invisible on an
// IPv4-only cluster (nothing to prefer) and on an IPv6-only one (nothing to
// fall back to), which is why it belongs here and nowhere else.
func TestActorEgressPerFamily(t *testing.T) {
	ctx := context.Background()

	namespace := e2e.DeployEchoDest(t, "networking")
	families := e2e.ClusterIPFamilies(t, ctx)

	for _, tc := range []struct {
		name     string
		families []corev1.IPFamily
		// want is the family the destination must be reached over. Empty for
		// the dual case: which one a dual-homed name resolves to is the
		// Actor's resolver's choice, and asserting it would be asserting
		// RFC 6724 rather than anything this system promises.
		want string
	}{
		{name: "ipv4", families: []corev1.IPFamily{corev1.IPv4Protocol}, want: "ipv4"},
		// There is deliberately no IPv6-only case. An Actor cannot reach an
		// IPv6-only destination at all today, so asserting it would be a
		// standing red rather than a regression guard. Adding it is one line
		// once that works; until then the dual case below is the one that
		// matters, and it already fails the moment IPv6 is preferred but broken.
		{name: "dual", families: []corev1.IPFamily{corev1.IPv4Protocol, corev1.IPv6Protocol}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, f := range tc.families {
				if !families[f] {
					t.Skipf("cluster has no %s address family", f)
				}
			}
			service := e2e.CreateEchoDestService(t, ctx, namespace, "echodest-"+tc.name, tc.families)

			actorName, _ := createAndResumeActor(t, ctx, "egress-"+tc.name, e2e.EgressFixture())
			router := mustRouterClient(t, ctx)
			defer router.Close()

			actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
			url := fmt.Sprintf("http://%s.%s.svc.cluster.local:8080/healthz", service, namespace)
			status, body := fetchThroughEgressActor(t, ctx, router, actorRef, url)
			if status != http.StatusOK {
				t.Fatalf("Actor egress to the %s destination returned HTTP %d, want 200; body: %s", tc.name, status, body)
			}

			// The Actor echoes the destination's response back, so the family
			// assertion reads the destination's view of the connection rather
			// than trusting a 200.
			got := destinationFamily(t, body)
			if tc.want != "" && got != tc.want {
				t.Fatalf("Actor egress to the %s destination arrived over %s, want %s", tc.name, got, tc.want)
			}
			t.Logf("Actor egress to the %s destination arrived over %s", tc.name, got)
		})
	}
}

// destinationFamily pulls echodest's reported family out of the body the
// egress Actor echoed back.
func destinationFamily(t *testing.T, body []byte) string {
	t.Helper()
	var echoed struct {
		Body string `json:"body"`
	}
	payload := body
	if err := json.Unmarshal(body, &echoed); err == nil && echoed.Body != "" {
		payload = []byte(echoed.Body)
	}
	var reply struct {
		Family string `json:"family"`
	}
	if err := json.Unmarshal(payload, &reply); err != nil {
		t.Fatalf("parsing the destination's reply %q: %v", payload, err)
	}
	if reply.Family == "" {
		t.Fatalf("the destination reported no family; reply: %s", payload)
	}
	return reply.Family
}
