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

package demo

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/resources"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A routed request to a CRASHED actor answers 503 with the actor's state in
// the body, and does so within 2 s. A parked request would first wait out the
// router's 5 s budget.
func TestRoutedRequestToCrashedActorFailsFast(t *testing.T) {
	t.Parallel()
	if e2e.IsAgentGateway() {
		t.Skip("fail-fast on a CRASHED actor is specified for Envoy only")
	}
	nsObj := e2e.CreateNamespace(t)
	ctx := context.Background()
	clients := e2e.GetClients()

	at, err := createActorTemplate(ctx, t, clients, nsObj, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	actorName := "crashed-request-actor-" + nsObj.Name
	actorRef := &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorName}
	routedRef := resources.ActorRef{Atespace: demoAtespace, Name: actorName}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapi.DeleteActorRequest{Actor: actorRef})
	})

	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_SUSPENDED)

	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_RUNNING)

	resp, err := callActor(t, routedRef)
	if err != nil {
		t.Fatalf("failed to call actor before crash: %v", err)
	}
	validateCounterResponse(t, resp, "before crash", 1, 1)

	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("failed to get Actor: %v", err)
	}
	podName := actor.GetStatus().GetWorkerAssignment().GetWorkerPod()
	podNamespace := actor.GetStatus().GetWorkerAssignment().GetWorkerNamespace()
	if podName == "" || podNamespace == "" {
		t.Fatalf("running actor has no pod recorded: %v", actor.GetStatus().GetWorkerAssignment())
	}

	// Deleting the pod is how a real crash happens, as in TestRevertCrashedActor.
	if err := clients.K8s.CoreV1().Pods(podNamespace).Delete(ctx, podName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("failed to delete worker pod %s/%s: %v", podNamespace, podName, err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapi.ActorState_ACTOR_STATE_CRASHED)

	router, err := e2e.NewRouterClient(t, ctx)
	if err != nil {
		t.Fatalf("connecting to atenet-router: %v", err)
	}
	t.Cleanup(router.Close)

	reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	start := time.Now()
	httpResp, err := router.Do(reqCtx, http.MethodPost, routedRef, "/", nil)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("routed request to CRASHED actor failed at transport level after %v: %v", elapsed, err)
	}
	defer httpResp.Body.Close()
	b, _ := io.ReadAll(httpResp.Body)
	body := string(b)
	t.Logf("routed request to CRASHED actor: status=%d elapsed=%v body=%q", httpResp.StatusCode, elapsed, body)

	if httpResp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", httpResp.StatusCode, http.StatusServiceUnavailable)
	}
	if !strings.Contains(body, "ACTOR_STATE_CRASHED") {
		t.Errorf("body = %q, want it to name ACTOR_STATE_CRASHED", body)
	}
	// Well under the 5 s budget proves the router answered on the first
	// ResumeActor attempt.
	if elapsed >= 2*time.Second {
		t.Errorf("request took %v, want under 2s: the router parked a request no retry can serve", elapsed)
	}
}
