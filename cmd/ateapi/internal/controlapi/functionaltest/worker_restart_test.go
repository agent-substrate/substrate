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

package functionaltest

import (
	"context"
	"slices"
	"testing"

	"go.ate.dev/substrate/cmd/ateapi/internal/controlapi"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestResumeActor_RetriedAfterWorkerRestartDuringRestore drives a resume whose
// restore finished on a worker process that was replaced before the resume
// committed RUNNING. The resume reports Unavailable and keeps its assignment;
// the reconciler for the new epoch leaves the RESUMING actor to it; a retry
// while the new process has yet to report its runtimes waits; and the next
// retry restores the actor on the new process, stamped with its epoch and IPs.
func TestResumeActor_RetriedAfterWorkerRestartDuringRestore(t *testing.T) {
	ns := namespaceForTest("ns-restart-restore")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()

	createTemplate(t, tc, ns)
	podUID := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"}
	if _, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: ref.GetAtespace(), Name: ref.GetName()},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	const newEpoch = reportedEpoch + 1
	newIPs := []string{"127.0.0.2"}
	updateWorker := func(mutate func(*ateapipb.WorkerStatus)) {
		t.Helper()
		worker, err := tc.persistence.GetWorker(ctx, podUID)
		if err != nil {
			t.Fatalf("GetWorker: %v", err)
		}
		if _, err := tc.persistence.UpdateWorker(ctx, podUID, store.PreconditionFrom(worker), func(w *ateapipb.Worker) error {
			mutate(w.Status)
			return nil
		}); err != nil {
			t.Fatalf("UpdateWorker: %v", err)
		}
	}
	getActor := func() *ateapipb.Actor {
		t.Helper()
		actor, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
		if err != nil {
			t.Fatalf("GetActor failed: %v", err)
		}
		return actor
	}

	// The restore finishes on the original process, which then exits; a new
	// process registers before the resume commits. Registering activates its
	// epoch, replaces the IPs, and clears the runtimes until it reports them.
	var restores int
	tc.fakeAtelet.RestoreWorkerEpoch = func() int64 {
		restores++
		if restores == 1 {
			updateWorker(func(s *ateapipb.WorkerStatus) {
				s.RegisteredEpoch = newEpoch
				s.Ips = newIPs
				s.SandboxClasses = nil
			})
			return reportedEpoch
		}
		return newEpoch
	}

	_, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref})
	if got := status.Code(err); got != codes.Unavailable {
		t.Fatalf("first ResumeActor = %v (err %v), want Unavailable", got, err)
	}
	if got := getActor().GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RESUMING {
		t.Fatalf("actor state after the first resume = %v, want RESUMING", got)
	}

	// The reconciler for the new epoch leaves the RESUMING actor, and its
	// claim, to the resume.
	if err := controlapi.NewWorkerWorkflow(tc.persistence, nil).ReconcileAssignments(ctx, podUID); err != nil {
		t.Fatalf("ReconcileAssignments: %v", err)
	}
	if got := getActor().GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RESUMING {
		t.Fatalf("actor state after reconcile = %v, want RESUMING", got)
	}

	// Until the new process reports its runtimes the retry waits, without
	// restoring or crashing anything.
	_, err = tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref})
	if got := status.Code(err); got != codes.Unavailable {
		t.Fatalf("ResumeActor before runtimes are reported = %v (err %v), want Unavailable", got, err)
	}
	if restores != 1 {
		t.Fatalf("restores = %d before runtimes are reported, want 1", restores)
	}

	updateWorker(func(s *ateapipb.WorkerStatus) {
		s.SandboxClasses = workerSandboxClasses(defaultWorkerRuntime("gvisor"))
	})
	resp, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("ResumeActor after the restart: %v", err)
	}
	if restores != 2 {
		t.Errorf("restores = %d, want the retry to restore again", restores)
	}
	actor := resp.GetActor()
	if got := actor.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("actor state = %v, want RUNNING", got)
	}
	assignment := actor.GetStatus().GetWorkerAssignment()
	if got := assignment.GetWorkerEpoch(); got != newEpoch {
		t.Errorf("worker_epoch = %d, want %d", got, newEpoch)
	}
	if got := assignment.GetWorkerPodIps(); !slices.Equal(got, newIPs) {
		t.Errorf("worker_pod_ips = %v, want %v", got, newIPs)
	}

	// The claim taken by the first attempt is the one the actor runs under:
	// the worker still books one actor, and a later reconcile keeps it.
	if err := controlapi.NewWorkerWorkflow(tc.persistence, nil).ReconcileAssignments(ctx, podUID); err != nil {
		t.Fatalf("ReconcileAssignments: %v", err)
	}
	if got := getActor().GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state after a later reconcile = %v, want RUNNING", got)
	}
	worker, err := tc.persistence.GetWorker(ctx, podUID)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if got := worker.GetStatus().GetAllocated().GetActors(); got != 1 {
		t.Errorf("worker allocated actors = %d, want 1", got)
	}
}
