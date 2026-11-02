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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestGracefulWorkerTermination exercises the propagated-SIGTERM eviction flow
// end to end: an actor is scheduled onto a worker pod, that pod is deleted
// (simulating a Kubernetes eviction), and the control plane is expected to mark
// the worker DRAINING, then remove it and detach the actor once the pod is gone.
//
// The demo counter actor installs a SIGTERM handler that sleeps
// before exiting, simulating a real-world workload that waits for graceful
// termination. The actor eventually lands in a terminal, non-RUNNING state
// (CRASHED). We assert the control-plane state
// machine rather than any in-actor state saving, which is the application's responsibility.
func TestGracefulWorkerTermination(t *testing.T) {
	t.Parallel()
	nsObj := e2e.CreateNamespace(t)

	ctx := context.Background()
	clients := e2e.GetClients()

	at, err := createActorTemplate(ctx, t, clients, nsObj, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	actorID := "graceful-term-" + nsObj.Name
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{
		Actor: &ateapi.Actor{
			Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actorID},
			ActorTemplate: e2e.TemplateRef(at),
		},
	}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	defer func() {
		_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorID},
		})
	}()

	// Bring the actor up on a worker so it is bound to a pod.
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorID, ateapi.ActorState_ACTOR_STATE_RUNNING)

	// Set the sigterm sleep interval to 15 seconds.
	if _, err := callActorPath(t, resources.ActorRef{Atespace: demoAtespace, Name: actorID}, "GET", "/set-sigterm-sleep?duration=15"); err != nil {
		t.Fatalf("failed to set sigterm sleep: %v", err)
	}

	running, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorID},
	})
	if err != nil {
		t.Fatalf("failed to get running Actor: %v", err)
	}
	podNS := running.GetStatus().GetWorkerAssignment().GetWorkerNamespace()
	podName := running.GetStatus().GetWorkerAssignment().GetWorkerPod()
	if podNS == "" || podName == "" {
		t.Fatalf("running actor has no bound worker pod: ns=%q name=%q", podNS, podName)
	}
	t.Logf("Actor %q bound to worker pod %s/%s", actorID, podNS, podName)

	// Evict the worker pod. The kubelet sends SIGTERM to worker, which propagates
	// it into the sandbox; the control plane marks the worker DRAINING on the
	// DeletionTimestamp watch event and cleans up when the pod is finally gone.
	if err := clients.K8s.CoreV1().Pods(podNS).Delete(ctx, podName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("failed to delete worker pod %s/%s: %v", podNS, podName, err)
	}

	// The worker record must eventually be removed once the pod is gone.
	if err := waitForWorkerRemoved(ctx, t, clients, podName, 60*time.Second); err != nil {
		t.Fatalf("worker %s not removed after pod deletion: %v", podName, err)
	}

	// Verify the actor lands in ACTOR_STATE_CRASHED.
	waitForActorState(ctx, t, clients, actorID, ateapi.ActorState_ACTOR_STATE_CRASHED)

	// Verify the pod assignment was cleared.
	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorID},
	})
	if err != nil {
		t.Fatalf("failed to get actor: %v", err)
	}
	if pod := actor.GetStatus().GetWorkerAssignment().GetWorkerPod(); pod != "" {
		t.Errorf("actor still bound to worker pod %q, expected empty", pod)
	}
}

// waitForWorkerRemoved polls ListWorkers until the named worker is absent.
func waitForWorkerRemoved(ctx context.Context, t *testing.T, clients *e2e.Clients, podName string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		resp, err := clients.SubstrateAPI.ListWorkers(ctx, &ateapi.ListWorkersRequest{})
		if err == nil {
			found := false
			for _, w := range resp.GetWorkers() {
				if w.GetWorkerPod() == podName {
					found = true
					break
				}
			}
			if !found {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return context.DeadlineExceeded
		}
		time.Sleep(time.Second)
	}
}

// TestGracefulWorkerTerminationSuspend exercises the case where a worker pod is
// deleted (evicted), and while the container is in its SIGTERM shutdown phase,
// we initiate a suspend. Suspend either completes before worker deletion crashes
// the actor, or reports that concurrent crash without overwriting it.
func TestGracefulWorkerTerminationSuspend(t *testing.T) {
	t.Parallel()
	nsObj := e2e.CreateNamespace(t)

	ctx := context.Background()
	clients := e2e.GetClients()

	at, err := createActorTemplate(ctx, t, clients, nsObj, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	actorID := "graceful-term-suspend-" + nsObj.Name
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{
		Actor: &ateapi.Actor{
			Metadata:      &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: actorID},
			ActorTemplate: e2e.TemplateRef(at),
		},
	}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	defer func() {
		_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorID},
		})
	}()

	// Bring the actor up on a worker so it is bound to a pod.
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorID, ateapi.ActorState_ACTOR_STATE_RUNNING)

	// Set the sigterm sleep interval to 30 seconds.
	if _, err := callActorPath(t, resources.ActorRef{Atespace: demoAtespace, Name: actorID}, "GET", "/set-sigterm-sleep?duration=30"); err != nil {
		t.Fatalf("failed to set sigterm sleep: %v", err)
	}

	running, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorID},
	})
	if err != nil {
		t.Fatalf("failed to get running Actor: %v", err)
	}
	podNS := running.GetStatus().GetWorkerAssignment().GetWorkerNamespace()
	podName := running.GetStatus().GetWorkerAssignment().GetWorkerPod()
	if podNS == "" || podName == "" {
		t.Fatalf("running actor has no bound worker pod: ns=%q name=%q", podNS, podName)
	}
	t.Logf("Actor %q bound to worker pod %s/%s", actorID, podNS, podName)
	previousSnapshotURI := durableSnapshotURI(running.GetStatus())
	if previousSnapshotURI == "" {
		t.Fatal("running actor has no completed durable snapshot")
	}

	// Evict the worker pod. The kubelet sends SIGTERM to worker, which propagates
	// it into the sandbox; the container hangs for 30s.
	if err := clients.K8s.CoreV1().Pods(podNS).Delete(ctx, podName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("failed to delete worker pod %s/%s: %v", podNS, podName, err)
	}

	// Wait 2 seconds to make sure the SIGTERM was sent and the container is in its sleep phase.
	time.Sleep(2 * time.Second)

	// Suspend the actor.
	t.Logf("Suspending actor %q during termination", actorID)
	_, suspendErr := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor:   &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorID},
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	wantState := ateapi.ActorState_ACTOR_STATE_SUSPENDED
	switch status.Code(suspendErr) {
	case codes.OK:
	case codes.FailedPrecondition, codes.Aborted:
		// Worker deletion can crash the actor before finalize reads it, or
		// after that read but before its version-guarded update. Both must
		// preserve the crash and report failure instead of returning success.
		wantState = ateapi.ActorState_ACTOR_STATE_CRASHED
		t.Logf("Suspend reported concurrent worker deletion: %v", suspendErr)
	default:
		t.Fatalf("failed to suspend Actor: %v", suspendErr)
	}

	// The worker record must eventually be removed once the pod is gone.
	if err := waitForWorkerRemoved(ctx, t, clients, podName, 60*time.Second); err != nil {
		t.Fatalf("worker %s not removed after pod deletion: %v", podName, err)
	}

	// The RPC result and persisted state must agree after the worker is gone.
	waitForActorState(ctx, t, clients, actorID, wantState)

	// Verify the pod assignment was cleared.
	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: demoAtespace, Name: actorID},
	})
	if err != nil {
		t.Fatalf("failed to get actor: %v", err)
	}
	if pod := actor.GetStatus().GetWorkerAssignment().GetWorkerPod(); pod != "" {
		t.Errorf("actor still bound to worker pod %q, expected empty", pod)
	}
	if got := durableSnapshotURI(actor.GetStatus()); got == "" {
		t.Error("actor lost its completed durable snapshot")
	} else if wantState == ateapi.ActorState_ACTOR_STATE_CRASHED && got != previousSnapshotURI {
		t.Errorf("crashed actor snapshot URI = %q, want previous snapshot %q", got, previousSnapshotURI)
	}
	if wantState == ateapi.ActorState_ACTOR_STATE_CRASHED && actor.GetStatus().GetCrash().GetMessage() == "" {
		t.Error("crashed actor lost its crash reason")
	}
	if wantState == ateapi.ActorState_ACTOR_STATE_SUSPENDED && actor.GetStatus().GetCrash() != nil {
		t.Errorf("successful suspend retained a crash record: %v", actor.GetStatus().GetCrash())
	}
}
