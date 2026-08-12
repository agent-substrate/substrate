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
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
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
		// Retry until the ingress routes are programmed. After ResumeActor returns
		// the xDS update from the control plane may not have reached the router yet,
		// causing a transient 503 connection timeout.
		const timeout = 30 * time.Second
		deadline := time.Now().Add(timeout)
		for {
			response, err := router.Get(ctx, actorRef, "/readyz")
			if err != nil {
				t.Fatalf("GET Actor through ingress: %v", err)
			}
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil {
				t.Fatalf("reading ingress response body (HTTP %d): %v", response.StatusCode, err)
			}
			if response.StatusCode == http.StatusOK {
				t.Logf("Actor access through ingress succeeded; body: %s", body)
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("Actor access through ingress returned HTTP %d after %v; body: %s", response.StatusCode, timeout, body)
			}
			t.Logf("Actor access through ingress returned HTTP %d; retrying...", response.StatusCode)
			time.Sleep(1 * time.Second)
		}
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
	actorName, _ := createAndResumeActor(t, ctx, "egress", egressTemplate)
	router := mustRouterClient(t, ctx)
	defer router.Close()

	// The egress demo fetches the URL it is given and echoes the upstream
	// status and body back.
	payload := []byte(`{"url":"http://example.com/"}`)
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	response, err := router.PostJSON(ctx, actorRef, "/", payload)
	if err != nil {
		t.Fatalf("POST to egress Actor through ingress: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading egress response body (HTTP %d): %v", response.StatusCode, err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("Actor egress fetch returned HTTP %d, want 200; body: %s", response.StatusCode, body)
	}
	t.Logf("Actor egress fetch succeeded; body: %s", body)
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
