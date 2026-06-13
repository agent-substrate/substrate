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
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestActorLifecycle(t *testing.T) {
	// Create namespace
	nsObj := e2e.CreateNamespace(t)

	ctx := context.Background()
	clients := e2e.GetClients()

	// Create actor template.
	at, err := createActorTemplate(ctx, t, clients, nsObj)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	tests := []struct {
		name string
		f    func(ctx context.Context, t *testing.T, clients *e2e.Clients, ns *e2e.Namespace, at *v1alpha1.ActorTemplate) error
	}{
		{
			name: "CreateActor",
			f:    createActor,
		},
		{
			name: "PauseResumeActor",
			f:    pauseActor,
		},
		{
			name: "SuspendResumeActor",
			f:    suspendActor,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := tc.f(ctx, t, clients, nsObj, at); err != nil {
				t.Errorf("Test %q failed: %v", tc.name, err)
			}
		})
	}

}

func createActor(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, at *v1alpha1.ActorTemplate) error {
	// Create an Actor using the ATE API.
	actorID := "demo-actor-1-" + nsObj.Name

	t.Logf("Creating Actor %q using Substrate API...", actorID)
	createResp, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{
		ActorId:                actorID,
		ActorTemplateNamespace: nsObj.Name,
		ActorTemplateName:      at.Name,
	})
	if err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	t.Logf("Successfully created Actor: %s", createResp.GetActor().GetActorId())
	defer func() {
		clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
			ActorId: actorID,
		})
	}()

	listResp, err := clients.SubstrateAPI.ListActors(ctx, &ateapipb.ListActorsRequest{})
	if err != nil {
		t.Fatalf("ListActors RPC failed: %v", err)
	}

	var myActors []*ateapipb.Actor
	for _, actor := range listResp.GetActors() {
		if actor.GetActorTemplateNamespace() == nsObj.Name && actor.GetActorId() == actorID {
			myActors = append(myActors, actor)
		}
	}

	// Check that we have our Actor created.
	if len(myActors) != 1 {
		t.Fatalf("expected actor %s in namespace %s, got %d actors: %v", actorID, nsObj.Name, len(myActors), myActors)
	}

	actor := myActors[0]
	if actor.GetActorId() != actorID {
		t.Errorf("expected actor ID %s, got %s", actorID, actor.GetActorId())
	}
	if actor.GetActorTemplateName() != at.Name {
		t.Errorf("expected actor template name %s, got %s", at.Name, actor.GetActorTemplateName())
	}
	if actor.Status != ateapipb.Actor_STATUS_SUSPENDED {
		t.Errorf("expected actor status to be SUSPENDED, got %v", actor.Status)
	}

	t.Logf("Successfully queried Substrate API. Found %d active actors total, %d in our namespace %s.",
		len(listResp.GetActors()), len(myActors), nsObj.Name)

	return nil
}

func pauseActor(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, at *v1alpha1.ActorTemplate) error {
	actorID := "pause-actor-" + nsObj.Name

	// Creating an actor
	t.Logf("Creating Actor %q...", actorID)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{
		ActorId:                actorID,
		ActorTemplateNamespace: nsObj.Name,
		ActorTemplateName:      at.Name,
	}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorID, ateapipb.Actor_STATUS_SUSPENDED)

	// Resuming the actor
	t.Logf("Resuming Actor %q...", actorID)
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		ActorId: actorID,
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorID, ateapipb.Actor_STATUS_RUNNING)

	// Pausing the actor
	t.Logf("Pausing Actor %q...", actorID)
	if _, err := clients.SubstrateAPI.PauseActor(ctx, &ateapipb.PauseActorRequest{
		ActorId: actorID,
	}); err != nil {
		t.Fatalf("failed to pause Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorID, ateapipb.Actor_STATUS_PAUSED)

	// Resuming the actor again
	t.Logf("Resuming Actor %q again...", actorID)
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		ActorId: actorID,
	}); err != nil {
		t.Fatalf("failed to resume Actor again: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorID, ateapipb.Actor_STATUS_RUNNING)

	// Suspending the actor before deletion
	t.Logf("Suspending Actor %q before deletion...", actorID)
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		ActorId: actorID,
	}); err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorID, ateapipb.Actor_STATUS_SUSPENDED)

	// Deleting the actor
	t.Logf("Deleting Actor %q...", actorID)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		ActorId: actorID,
	}); err != nil {
		t.Fatalf("failed to delete Actor: %v", err)
	}
	// Verify deletion
	if _, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		ActorId: actorID,
	}); err == nil {
		t.Fatalf("expected actor %q to be deleted, but it still exists", actorID)
	}

	return nil
}

func suspendActor(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, at *v1alpha1.ActorTemplate) error {
	actorID := "suspend-actor-" + nsObj.Name

	// Creating an actor
	t.Logf("Creating Actor %q...", actorID)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{
		ActorId:                actorID,
		ActorTemplateNamespace: nsObj.Name,
		ActorTemplateName:      at.Name,
	}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorID, ateapipb.Actor_STATUS_SUSPENDED)

	// Resuming the actor
	t.Logf("Resuming Actor %q...", actorID)
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		ActorId: actorID,
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorID, ateapipb.Actor_STATUS_RUNNING)

	// Pausing the actor
	t.Logf("Pausing Actor %q...", actorID)
	if _, err := clients.SubstrateAPI.PauseActor(ctx, &ateapipb.PauseActorRequest{
		ActorId: actorID,
	}); err != nil {
		t.Fatalf("failed to pause Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorID, ateapipb.Actor_STATUS_PAUSED)

	// Resuming the actor again
	t.Logf("Resuming Actor %q again...", actorID)
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		ActorId: actorID,
	}); err != nil {
		t.Fatalf("failed to resume Actor again: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorID, ateapipb.Actor_STATUS_RUNNING)

	// Suspending the actor before deletion
	t.Logf("Suspending Actor %q before deletion...", actorID)
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		ActorId: actorID,
	}); err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorID, ateapipb.Actor_STATUS_SUSPENDED)

	// Deleting the actor
	t.Logf("Deleting Actor %q...", actorID)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		ActorId: actorID,
	}); err != nil {
		t.Fatalf("failed to delete Actor: %v", err)
	}
	// Verify deletion
	if _, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		ActorId: actorID,
	}); err == nil {
		t.Fatalf("expected actor %q to be deleted, but it still exists", actorID)
	}

	return nil
}

func createActorTemplate(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace) (*v1alpha1.ActorTemplate, error) {
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}

	// Query existing WorkerPool and ActorTemplate to get the resolved container images
	existingWp, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools("ate-demo-counter").Get(ctx, "counter", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get existing WorkerPool: %v", err)
	}

	existingAt, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates("ate-demo-counter").Get(ctx, "counter", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get existing ActorTemplate: %v", err)
	}

	// Create WorkerPool
	wp := &v1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "counter",
			Namespace: nsObj.Name,
		},
		Spec: v1alpha1.WorkerPoolSpec{
			Replicas:   5,
			AteomImage: existingWp.Spec.AteomImage,
		},
	}
	_, err = clients.SubstrateK8s.ApiV1alpha1().WorkerPools(nsObj.Name).Create(ctx, wp, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create WorkerPool: %v", err)
	}

	// Create ActorTemplate
	at := &v1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "counter",
			Namespace: nsObj.Name,
		},
		Spec: v1alpha1.ActorTemplateSpec{
			WorkerPoolRef: corev1.ObjectReference{
				Namespace: nsObj.Name,
				Name:      "counter",
			},
			Runsc:      existingAt.Spec.Runsc,
			PauseImage: existingAt.Spec.PauseImage,
			Containers: existingAt.Spec.Containers,
			SnapshotsConfig: v1alpha1.SnapshotsConfig{
				Location: "gs://" + env["BUCKET_NAME"] + "/ate-demo-counter",
			},
		},
	}
	_, err = clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(nsObj.Name).Create(ctx, at, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create ActorTemplate: %v", err)
	}

	// Wait for ActorTemplate to be Ready (golden snapshot created) before creating an actor.
	t.Logf("Waiting for ActorTemplate %s to be Ready...", at.Name)
	const tmplTimeout = 90 * time.Second
	tmplCtx, tmplCancel := context.WithTimeout(ctx, tmplTimeout)
	defer tmplCancel()
	var lastPhase v1alpha1.PhaseType
	for {
		curAt, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(nsObj.Name).Get(tmplCtx, at.Name, metav1.GetOptions{})
		if err == nil {
			lastPhase = curAt.Status.Phase
			if lastPhase == v1alpha1.PhaseReady {
				t.Logf("ActorTemplate %s is Ready with golden snapshot %q", at.Name, curAt.Status.GoldenSnapshot)
				break
			}
			if lastPhase == v1alpha1.PhaseFailed {
				t.Fatalf("ActorTemplate %s transitioned to PhaseFailed!", at.Name)
			}
		}
		select {
		case <-tmplCtx.Done():
			t.Fatalf("Timed out waiting for ActorTemplate %q to be Ready after %v (last phase: %s, err: %v)", at.Name, tmplTimeout, lastPhase, err)
		case <-time.After(1 * time.Second):
			// Keep polling.
		}
	}

	return at, nil
}

func waitForActorStatus(ctx context.Context, t *testing.T, clients *e2e.Clients, actorID string, expectedStatus ateapipb.Actor_Status) {
	t.Helper()
	t.Logf("Waiting for Actor %q to be %v...", actorID, expectedStatus)
	timeout := 60 * time.Second
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
			ActorId: actorID,
		})
		if err == nil {
			if resp.GetActor().GetStatus() == expectedStatus {
				t.Logf("Actor %q reached status %v", actorID, expectedStatus)
				return
			}
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("timed out waiting for actor %q to reach status %v", actorID, expectedStatus)
}
