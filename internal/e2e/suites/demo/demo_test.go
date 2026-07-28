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
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

const demoAtespace = "demo"

func TestActorLifecycle(t *testing.T) {
	// Create namespace
	nsObj := e2e.CreateNamespace(t)

	ctx := context.Background()
	clients := e2e.GetClients()

	// CreateActor requires the atespace to exist first.
	_, _ = clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: demoAtespace}}})

	// Create actor template.
	at, err := createActorTemplate(ctx, t, clients, nsObj, v1alpha1.SnapshotScopeFull, v1alpha1.SnapshotScopeFull)
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

func TestDurableDirLifecycle(t *testing.T) {
	tests := []struct {
		name string
		tc   actorLifecycleTestCase
	}{
		{
			name: "onCommit:Full, onPause:Full",
			tc: actorLifecycleTestCase{
				onCommit:               v1alpha1.SnapshotScopeFull,
				onPause:                v1alpha1.SnapshotScopeFull,
				wantMemoryAfterPause:   2,
				wantFileAfterPause:     2,
				wantMemoryAfterSuspend: 3,
				wantFileAfterSuspend:   3,
			},
		},
		{
			name: "onCommit:Data, onPause:Full",
			tc: actorLifecycleTestCase{
				onCommit:               v1alpha1.SnapshotScopeData,
				onPause:                v1alpha1.SnapshotScopeFull,
				wantMemoryAfterPause:   2,
				wantFileAfterPause:     2,
				wantMemoryAfterSuspend: 1,
				wantFileAfterSuspend:   3,
			},
		},
		{
			name: "onCommit:Data, onPause:Data",
			tc: actorLifecycleTestCase{
				onCommit:               v1alpha1.SnapshotScopeData,
				onPause:                v1alpha1.SnapshotScopeData,
				wantMemoryAfterPause:   1,
				wantFileAfterPause:     2,
				wantMemoryAfterSuspend: 1,
				wantFileAfterSuspend:   3,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			runActorLifecycleTestCase(t, "durabledir-lifecycle", createActorTemplate, test.tc)
		})
	}
}

func TestExternalVolumeLifecycle(t *testing.T) {
	if isMicroVMEnvironment() {
		t.Skip("Skipping TestExternalVolumeLifecycle for microVM environment")
	}

	tests := []struct {
		name string
		tc   actorLifecycleTestCase
	}{
		{
			name: "onCommit:Data, onPause:Data",
			tc: actorLifecycleTestCase{
				onCommit:               v1alpha1.SnapshotScopeData,
				onPause:                v1alpha1.SnapshotScopeData,
				wantMemoryAfterPause:   1,
				wantFileAfterPause:     2,
				wantMemoryAfterSuspend: 1,
				wantFileAfterSuspend:   3,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			runActorLifecycleTestCase(t, "extvol-lifecycle", createActorTemplateWithExternalVolume, test.tc)
		})
	}
}

// Verify that file and memory counters behavior after pause and suspend, for different snapshot scopes.
// Test case:
//  1. Create actor.
//  2. Call to actor and validate memory and file counters.
//  3. Pause & Resume actor.
//  4. Call to actor and validate memory and file counters.
//  5. Suspend & Resume actor.
//  6. Call to actor and validate memory and file counters.
type actorLifecycleTestCase struct {
	onCommit               v1alpha1.SnapshotScope
	onPause                v1alpha1.SnapshotScope
	wantMemoryAfterPause   int
	wantFileAfterPause     int
	wantMemoryAfterSuspend int
	wantFileAfterSuspend   int
}

func runActorLifecycleTestCase(t *testing.T, prefix string, createTemplate func(context.Context, *testing.T, *e2e.Clients, *e2e.Namespace, v1alpha1.SnapshotScope, v1alpha1.SnapshotScope) (*v1alpha1.ActorTemplate, error), tc actorLifecycleTestCase) {
	// Create namespace
	nsObj := e2e.CreateNamespace(t)

	ctx := context.Background()
	clients := e2e.GetClients()

	// CreateActor requires the atespace to exist first.
	_, _ = clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: demoAtespace}}})

	// Create actor template.
	at, err := createTemplate(ctx, t, clients, nsObj, tc.onCommit, tc.onPause)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	//
	// Create an Actor.
	//
	actorID := prefix + "-" + nsObj.Name

	t.Logf("Creating Actor %q using Substrate API...", actorID)
	createResp, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorID},
		ActorTemplateNamespace: nsObj.Name,
		ActorTemplateName:      at.Name,
	}})
	if err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	t.Logf("Successfully created Actor: %s", createResp.GetMetadata().GetName())
	defer func() {
		clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
		})
	}()

	// Resuming the actor
	t.Logf("Resuming Actor %q...", actorID)
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorID, ateapipb.Actor_STATUS_RUNNING)

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorID})
	if err != nil {
		t.Fatalf("failed to call actor: %v", err)
	}
	validateCounterResponse(t, resp, "after creation", 1, 1)

	//
	// Pausing the actor
	//
	t.Logf("Pausing Actor %q...", actorID)
	if _, err := clients.SubstrateAPI.PauseActor(ctx, &ateapipb.PauseActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to pause Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorID, ateapipb.Actor_STATUS_PAUSED)

	// Resuming the actor
	t.Logf("Resuming Actor %q again...", actorID)
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to resume Actor again: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorID, ateapipb.Actor_STATUS_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorID})
	if err != nil {
		t.Fatalf("failed to call actor again: %v", err)
	}
	validateCounterResponse(t, resp, "after pause", tc.wantMemoryAfterPause, tc.wantFileAfterPause)

	//
	// Suspending the actor
	//
	t.Logf("Suspending Actor %q...", actorID)
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorID, ateapipb.Actor_STATUS_SUSPENDED)

	// Resuming the actor
	t.Logf("Resuming Actor %q again...", actorID)
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to resume Actor again: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorID, ateapipb.Actor_STATUS_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorID})
	if err != nil {
		t.Fatalf("failed to call actor again: %v", err)
	}
	validateCounterResponse(t, resp, "after suspend", tc.wantMemoryAfterSuspend, tc.wantFileAfterSuspend)
}

func validateCounterResponse(t *testing.T, resp string, stage string, wantMemory, wantFile int) {
	memoryCounterPrefix := "preserved memory count: "
	fileCounterPrefix := "preserved file counter: "

	if !strings.Contains(resp, memoryCounterPrefix+fmt.Sprintf("%d", wantMemory)) {
		t.Errorf("[%s] expected memory count %d, got response: %s", stage, wantMemory, resp)
	}
	if !strings.Contains(resp, fileCounterPrefix+fmt.Sprintf("%d", wantFile)) {
		t.Errorf("[%s] expected file count %d, got response: %s", stage, wantFile, resp)
	}
}

func createActor(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, at *v1alpha1.ActorTemplate) error {
	// Create an Actor using the ATE API.
	actorName := "demo-actor-1-" + nsObj.Name

	t.Logf("Creating Actor %q using Substrate API...", actorName)
	createResp, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplateNamespace: nsObj.Name,
		ActorTemplateName:      at.Name,
	}})
	if err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	t.Logf("Successfully created Actor: %s", createResp.GetMetadata().GetName())
	defer func() {
		clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
		})
	}()

	listResp, err := clients.SubstrateAPI.ListActors(ctx, &ateapipb.ListActorsRequest{Atespace: demoAtespace})
	if err != nil {
		t.Fatalf("ListActors RPC failed: %v", err)
	}

	var myActors []*ateapipb.Actor
	for _, actor := range listResp.GetActors() {
		if actor.GetActorTemplateNamespace() == nsObj.Name && actor.GetMetadata().GetName() == actorName {
			myActors = append(myActors, actor)
		}
	}

	// Check that we have our Actor created.
	if len(myActors) != 1 {
		t.Fatalf("expected actor %s in namespace %s, got %d actors: %v", actorName, nsObj.Name, len(myActors), myActors)
	}

	actor := myActors[0]
	if actor.GetMetadata().GetName() != actorName {
		t.Errorf("expected actor name %s, got %s", actorName, actor.GetMetadata().GetName())
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
	actorName := "pause-actor-" + nsObj.Name

	// Creating an actor
	t.Logf("Creating Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplateNamespace: nsObj.Name,
		ActorTemplateName:      at.Name,
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorName, ateapipb.Actor_STATUS_SUSPENDED)

	// Resuming the actor
	t.Logf("Resuming Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorName, ateapipb.Actor_STATUS_RUNNING)

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor: %v", err)
	}

	validateCounterResponse(t, resp, "after creation", 1, 1)

	// Pausing the actor
	t.Logf("Pausing Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.PauseActor(ctx, &ateapipb.PauseActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to pause Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorName, ateapipb.Actor_STATUS_PAUSED)

	// Resuming the actor again
	t.Logf("Resuming Actor %q again...", actorName)
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor again: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorName, ateapipb.Actor_STATUS_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor again: %v", err)
	}
	validateCounterResponse(t, resp, "after pause", 2, 2)

	// Suspending the actor before deletion
	t.Logf("Suspending Actor %q before deletion...", actorName)
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorName, ateapipb.Actor_STATUS_SUSPENDED)

	// Deleting the actor
	t.Logf("Deleting Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to delete Actor: %v", err)
	}
	// Verify deletion
	if _, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err == nil {
		t.Fatalf("expected actor %q to be deleted, but it still exists", actorName)
	}

	return nil
}

func suspendActor(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, at *v1alpha1.ActorTemplate) error {
	actorName := "suspend-actor-" + nsObj.Name

	// Creating an actor
	t.Logf("Creating Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplateNamespace: nsObj.Name,
		ActorTemplateName:      at.Name,
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorName, ateapipb.Actor_STATUS_SUSPENDED)

	// Resuming the actor
	t.Logf("Resuming Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorName, ateapipb.Actor_STATUS_RUNNING)

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor: %v", err)
	}
	validateCounterResponse(t, resp, "after creation", 1, 1)

	// Suspending the actor
	t.Logf("Suspending Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorName, ateapipb.Actor_STATUS_SUSPENDED)

	// Resuming the actor again
	t.Logf("Resuming Actor %q again...", actorName)
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor again: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorName, ateapipb.Actor_STATUS_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor again: %v", err)
	}
	validateCounterResponse(t, resp, "after suspend", 2, 2)

	// Suspending the actor before deletion
	t.Logf("Suspending Actor %q before deletion...", actorName)
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorName, ateapipb.Actor_STATUS_SUSPENDED)

	// Deleting the actor
	t.Logf("Deleting Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to delete Actor: %v", err)
	}
	// Verify deletion
	if _, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err == nil {
		t.Fatalf("expected actor %q to be deleted, but it still exists", actorName)
	}

	return nil
}

func createActorTemplateInternal(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, name string, onCommit, onPause v1alpha1.SnapshotScope, modifyTemplate func(*v1alpha1.ActorTemplate)) (*v1alpha1.ActorTemplate, error) {
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}

	// The source WorkerPool+ActorTemplate to copy the resolved runtime (sandbox class,
	// ateom image, container images) from. Defaults to the gVisor counter demo; CI
	// overrides these to point this same lifecycle test at the micro-VM counter.
	srcNS := "ate-demo-counter"
	if v := os.Getenv("E2E_TEMPLATE_NAMESPACE"); v != "" {
		srcNS = v
	}
	srcName := "counter"
	if v := os.Getenv("E2E_TEMPLATE_NAME"); v != "" {
		srcName = v
	}

	// Query existing WorkerPool and ActorTemplate to get the resolved container images
	existingWp, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(srcNS).Get(ctx, srcName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get existing WorkerPool %s/%s: %v", srcNS, srcName, err)
	}

	existingAt, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(srcNS).Get(ctx, srcName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get existing ActorTemplate %s/%s: %v", srcNS, srcName, err)
	}

	// Create WorkerPool. Labeled uniquely to this test's namespace so the
	// cluster-wide scheduler doesn't make this pool's workers eligible for
	// (or eligible to receive) any other namespace's actors.
	wp := &v1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: nsObj.Name,
			Labels:    map[string]string{"demo": nsObj.Name},
		},
		Spec: v1alpha1.WorkerPoolSpec{
			Replicas:          5,
			AteomImage:        existingWp.Spec.AteomImage,
			SandboxClass:      existingWp.Spec.SandboxClass,
			SandboxConfigName: existingWp.Spec.SandboxConfigName,
		},
	}
	_, err = clients.SubstrateK8s.ApiV1alpha1().WorkerPools(nsObj.Name).Create(ctx, wp, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create WorkerPool: %v", err)
	}

	// Create ActorTemplate
	at := &v1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: nsObj.Name,
		},
		Spec: v1alpha1.ActorTemplateSpec{
			WorkerSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"demo": nsObj.Name},
			},
			// SandboxClass must match the per-test WorkerPool's (copied above) so the
			// ActorTemplate↔WorkerPool match succeeds. The micro-VM source sets
			// "microvm"; the gVisor source leaves it "" — copying keeps both correct.
			SandboxClass: existingAt.Spec.SandboxClass,
			PauseImage:   existingAt.Spec.PauseImage,
			Containers:   existingAt.Spec.Containers,
			SnapshotsConfig: v1alpha1.SnapshotsConfig{
				Location: "gs://" + env["BUCKET_NAME"] + "/ate-demo-" + name,
				OnPause:  onPause,
				OnCommit: onCommit,
			},
			Volumes: existingAt.Spec.Volumes,
		},
	}
	if modifyTemplate != nil {
		modifyTemplate(at)
	}
	_, err = clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(nsObj.Name).Create(ctx, at, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create ActorTemplate: %v", err)
	}

	// Wait for ActorTemplate to be Ready (golden snapshot created) before creating an actor.
	// The micro-VM golden (CH boot + checkpoint on nested KVM) is slower than gVisor, so
	// CI raises this via E2E_TEMPLATE_READY_TIMEOUT.
	t.Logf("Waiting for ActorTemplate %s to be Ready...", at.Name)
	tmplTimeout := 90 * time.Second
	if v := os.Getenv("E2E_TEMPLATE_READY_TIMEOUT"); v != "" {
		d, perr := time.ParseDuration(v)
		if perr != nil {
			t.Fatalf("invalid E2E_TEMPLATE_READY_TIMEOUT %q: %v", v, perr)
		}
		tmplTimeout = d
	}
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

func createActorTemplate(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, onCommit, onPause v1alpha1.SnapshotScope) (*v1alpha1.ActorTemplate, error) {
	return createActorTemplateInternal(ctx, t, clients, nsObj, "counter", onCommit, onPause, nil)
}

func createActorTemplateWithExternalVolume(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, onCommit, onPause v1alpha1.SnapshotScope) (*v1alpha1.ActorTemplate, error) {
	modify := func(at *v1alpha1.ActorTemplate) {
		var res []v1alpha1.Container
		for _, c := range at.Spec.Containers {
			if c.Name == "counter" {
				c.Command = []string{"/ko-app/counter", "--file-counter-directory=/external-data", "--validate-existing-file-path=/external-data/test.txt"}
				hasExtMount := false
				for _, vm := range c.VolumeMounts {
					if vm.Name == "external-data" {
						hasExtMount = true
						break
					}
				}
				if !hasExtMount {
					c.VolumeMounts = append(c.VolumeMounts, v1alpha1.VolumeMount{
						Name:      "external-data",
						MountPath: "/external-data",
					})
				}
			}
			res = append(res, c)
		}
		at.Spec.Containers = res

		hasExtVol := false
		for _, v := range at.Spec.Volumes {
			if v.Name == "external-data" {
				hasExtVol = true
				break
			}
		}
		if !hasExtVol {
			at.Spec.Volumes = append(at.Spec.Volumes, v1alpha1.Volume{
				Name: "external-data",
				VolumeSource: v1alpha1.VolumeSource{
					ExternalVolumeTemplate: &v1alpha1.ExternalVolumeTemplate{
						Capacity:         resource.MustParse("1Gi"),
						StorageClassName: "standard",
					},
				},
			})
		}
	}
	return createActorTemplateInternal(ctx, t, clients, nsObj, "counter-ext-vol", onCommit, onPause, modify)
}

func waitForActorStatus(ctx context.Context, t *testing.T, clients *e2e.Clients, actorName string, expectedStatus ateapipb.Actor_Status) {
	t.Helper()
	t.Logf("Waiting for Actor %q to be %v...", actorName, expectedStatus)
	timeout := 60 * time.Second
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
		})
		if err == nil {
			if resp.GetStatus() == expectedStatus {
				t.Logf("Actor %q reached status %v", actorName, expectedStatus)
				return
			}
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("timed out waiting for actor %q to reach status %v", actorName, expectedStatus)
}

func callActor(t *testing.T, actorRef resources.ActorRef) (string, error) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := callActorOnce(t, actorRef)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		time.Sleep(1 * time.Second)
	}

	return "", fmt.Errorf("timed out waiting for actor response: %w", lastErr)
}

func callActorOnce(t *testing.T, actorRef resources.ActorRef) (string, error) {
	t.Helper()
	clients := e2e.GetClients()

	svc, err := clients.K8s.CoreV1().Services("ate-system").Get(context.Background(), "atenet-router", metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get atenet-router service: %w", err)
	}

	selector := labels.SelectorFromSet(svc.Spec.Selector).String()
	pods, err := clients.K8s.CoreV1().Pods("ate-system").List(context.Background(), metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return "", fmt.Errorf("failed to list atenet-router pods: %w", err)
	}
	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no atenet-router pods found")
	}
	targetPod := pods.Items[0]

	config, err := ateclient.LoadConfig(e2e.KubeConfig, e2e.KubeContext)
	if err != nil {
		return "", fmt.Errorf("failed to load kubeconfig: %w", err)
	}

	reqConfig := clients.K8s.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(targetPod.Namespace).
		Name(targetPod.Name).
		SubResource("portforward")

	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return "", fmt.Errorf("failed to create SPDY transport: %w", err)
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, reqConfig.URL())

	stopCh := make(chan struct{})
	readyCh := make(chan struct{})
	defer close(stopCh)

	fw, err := portforward.New(dialer, []string{"0:8080"}, stopCh, readyCh, io.Discard, io.Discard)
	if err != nil {
		return "", fmt.Errorf("failed to create port forwarder: %w", err)
	}

	errCh := make(chan error, 1)
	go func() {
		if err := fw.ForwardPorts(); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-readyCh:
	case err := <-errCh:
		return "", fmt.Errorf("port forwarding failed: %w", err)
	case <-time.After(10 * time.Second):
		return "", fmt.Errorf("timeout waiting for port-forward")
	}

	forwardedPorts, err := fw.GetPorts()
	if err != nil || len(forwardedPorts) == 0 {
		return "", fmt.Errorf("failed to get forwarded ports: %w", err)
	}
	localPort := forwardedPorts[0].Local

	reqHttp, err := http.NewRequest("POST", fmt.Sprintf("http://127.0.0.1:%d", localPort), nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	reqHttp.Host = actorRef.DNSName()

	httpClient := &http.Client{Timeout: 15 * time.Second}
	resp, err := httpClient.Do(reqHttp)
	if err != nil {
		return "", fmt.Errorf("failed to do request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	return string(body), nil
}

// isMicroVMEnvironment reports whether the suite is running against the
// micro-VM demo template, which does not support every volume type yet (see
// TestExternalVolumeLifecycle).
func isMicroVMEnvironment() bool {
	return os.Getenv("E2E_TEMPLATE_NAMESPACE") == "ate-demo-counter-microvm"
}

func TestWorkerPodDeletion(t *testing.T) {
	// Create namespace
	nsObj := e2e.CreateNamespace(t)

	ctx := context.Background()
	clients := e2e.GetClients()

	// CreateActor requires the atespace to exist first.
	_, _ = clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: demoAtespace}}})

	// Create actor template.
	at, err := createActorTemplate(ctx, t, clients, nsObj, v1alpha1.SnapshotScopeFull, v1alpha1.SnapshotScopeFull)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	actorName := "crash-actor-" + nsObj.Name

	// Creating an actor
	t.Logf("Creating Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplateNamespace: nsObj.Name,
		ActorTemplateName:      at.Name,
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	defer func() {
		clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
		})
	}()

	waitForActorStatus(ctx, t, clients, actorName, ateapipb.Actor_STATUS_SUSPENDED)

	// Resuming the actor
	t.Logf("Resuming Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorStatus(ctx, t, clients, actorName, ateapipb.Actor_STATUS_RUNNING)

	// Get actor to find the pod details
	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	})
	if err != nil {
		t.Fatalf("failed to get Actor: %v", err)
	}

	podName := actor.GetAteomPodName()
	podNamespace := actor.GetAteomPodNamespace()
	if podName == "" || podNamespace == "" {
		t.Fatalf("actor is running but pod details are missing: podName=%q, podNamespace=%q", podName, podNamespace)
	}

	// Verify worker is in ListWorkers
	t.Logf("Verifying worker for pod %s/%s is listed...", podNamespace, podName)
	foundWorker := false
	workersResp, err := clients.SubstrateAPI.ListWorkers(ctx, &ateapipb.ListWorkersRequest{})
	if err != nil {
		t.Fatalf("failed to list workers: %v", err)
	}
	for _, w := range workersResp.GetWorkers() {
		if w.GetWorkerNamespace() == podNamespace && w.GetWorkerPod() == podName {
			foundWorker = true
			break
		}
	}
	if !foundWorker {
		t.Fatalf("worker for pod %s/%s was not found in ListWorkers response: %v", podNamespace, podName, workersResp.GetWorkers())
	}

	// Delete the worker pod
	t.Logf("Deleting worker pod %s/%s...", podNamespace, podName)
	err = clients.K8s.CoreV1().Pods(podNamespace).Delete(ctx, podName, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("failed to delete pod %s/%s: %v", podNamespace, podName, err)
	}

	// Wait for the actor to be marked as CRASHED
	t.Logf("Waiting for actor %q to transition to CRASHED...", actorName)
	waitForActorStatus(ctx, t, clients, actorName, ateapipb.Actor_STATUS_CRASHED)

	// Verify the worker is cleaned up (deleted) from store
	t.Logf("Verifying worker for pod %s/%s is removed from store...", podNamespace, podName)
	deadline := time.Now().Add(30 * time.Second)
	var lastWorkers []*ateapipb.Worker
	for time.Now().Before(deadline) {
		workersResp, err = clients.SubstrateAPI.ListWorkers(ctx, &ateapipb.ListWorkersRequest{})
		if err != nil {
			t.Fatalf("failed to list workers: %v", err)
		}
		lastWorkers = workersResp.GetWorkers()
		stillExists := false
		for _, w := range lastWorkers {
			if w.GetWorkerNamespace() == podNamespace && w.GetWorkerPod() == podName {
				stillExists = true
				break
			}
		}
		if !stillExists {
			t.Logf("Worker for pod %s/%s successfully removed.", podNamespace, podName)
			return
		}
		time.Sleep(1 * time.Second)
	}

	t.Errorf("worker for pod %s/%s was not cleaned up within 30s. Current workers: %v", podNamespace, podName, lastWorkers)
}
