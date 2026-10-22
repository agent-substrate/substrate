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

package controlapi_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"errors"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"
	"go.ate.dev/substrate/cmd/ateapi/internal/controlapi"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/cmd/ateapi/internal/workerservice"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/localca"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
)

// TestMintWorkerActorCertificateAfterWorkerRelease drives each of
// ReleaseWindowWorkflows into the window between releasing the actor's worker
// and rewriting its status, by failing the second write. The status still
// names the worker there, and keeps doing so until the workflow is retried.
// The worker must not get a worker-for-actor certificate for the actor in that
// window, nor once it hosts another actor or the status is finalized, and
// another worker on the node never gets one.
func TestMintWorkerActorCertificateAfterWorkerRelease(t *testing.T) {
	// The worker is the one WireTestAssignment names, so an async or sync suspend
	// reaches the workflow's fake atelet on its node.
	wa := controlapi.WireTestAssignment()
	node := wa.GetNodeName()
	for _, wf := range controlapi.ReleaseWindowWorkflows() {
		t.Run(wf.Name, func(t *testing.T) {
			ctx := context.Background()
			persistence := controlapi.NewTestPersistence(t)
			controlapi.SeedSuspendTemplate(t, ctx, persistence, &ateapipb.SnapshotConfig{StorageLocation: controlapi.TestStorageLocation})
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			// wa's worker hosts the actor; the neighbor is another worker on
			// its node.
			workerName := wa.GetWorker().GetName()
			neighborName := "neighbor-1"
			for _, w := range []struct{ name, pod, podUID string }{
				{workerName, wa.GetWorkerPod(), wa.GetWorkerPodUid()},
				{neighborName, "neighbor-pod", "neighbor-pod-uid"},
			} {
				if _, err := persistence.CreateWorker(ctx, &ateapipb.Worker{
					Metadata:        &ateapipb.ResourceMetadata{Name: w.name},
					WorkerNamespace: wa.GetWorkerNamespace(),
					WorkerPool:      wa.GetWorkerPool(),
					WorkerPod:       w.pod,
					WorkerPodUid:    w.podUID,
					NodeName:        node,
					Status: &ateapipb.WorkerStatus{
						State:          ateapipb.WorkerState_WORKER_STATE_ACTIVE,
						Capacity:       &ateapipb.WorkerResources{Actors: 1},
						SandboxClasses: []*ateapipb.WorkerSandboxClass{{SandboxClass: "gvisor"}},
					},
				}); err != nil {
					t.Fatalf("CreateWorker(%s): %v", w.name, err)
				}
			}
			status := &ateapipb.ActorStatus{
				State:                  wf.State,
				AssignedNode:           node,
				WorkerAssignment:       controlapi.WireTestAssignment(),
				LastAssignedGeneration: 1,
			}
			if wf.Snapshot != nil {
				status.Snapshots = []*ateapipb.Snapshot{wf.Snapshot(t)}
			}
			created := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorTemplate: controlapi.SuspendTestTemplateRef(),
				Status:        status,
			})
			uid := created.GetMetadata().GetUid()
			controlapi.SeedAssignment(t, persistence, workerName, &ateapipb.ActorAssignment{
				Actor:    &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorUid: uid,
			})

			minter := newTestMinter(t, persistence)
			csr := newTestCSR(t)
			// mintAs asks for the actor's certificate as worker, through the
			// atelet on node.
			mintAs := func(worker string) codes.Code {
				t.Helper()
				_, err := minter.MintWorkerActorCertificate(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, node)), &ateapipb.MintWorkerActorCertificateRequest{
					Actor:                     &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name},
					ActorUid:                  uid,
					CertificateSigningRequest: csr,
					Worker:                    &ateapipb.ObjectRef{Name: worker},
				})
				return apierror.Code(err)
			}
			// mint asks for the actor's certificate as the worker that the
			// actor is assigned to and returns the actor as the mint saw it
			// and the mint's code.
			mint := func(stage string) (*ateapipb.Actor, codes.Code) {
				t.Helper()
				code := mintAs(workerName)
				actor, gerr := persistence.GetActor(ctx, actorRef)
				if gerr != nil {
					t.Fatalf("GetActor: %v", gerr)
				}
				t.Logf("%s: actor_state=%s status_worker_set=%t cert_code=%s",
					stage, actor.GetStatus().GetState(), actor.GetStatus().GetWorkerAssignment() != nil, code)
				return actor, code
			}

			actor, got := mint("hosted")
			if got != codes.OK {
				t.Fatalf("certificate mint while the worker holds the actor: code %v, want OK", got)
			}
			if got := mintAs(neighborName); got != codes.NotFound {
				t.Errorf("certificate mint by another worker on the node: code %v, want NotFound", got)
			}

			// The release commits; recording the new state fails.
			if err := wf.Finalize(t, ctx, controlapi.ConflictingUpdateStore{Interface: persistence}, actorRef, actor); err == nil {
				t.Fatal("finalize succeeded through a store that fails every actor update")
			}
			if _, err := persistence.GetWorkerAssignment(ctx, workerName, uid); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("worker assignment after release: err %v, want ErrNotFound", err)
			}
			actor, got = mint("released, status not finalized")
			if actor.GetStatus().GetState() != wf.State || actor.GetStatus().GetWorkerAssignment() == nil {
				t.Fatalf("actor after the failed update: state %v, worker assignment %v; want %v naming the worker", actor.GetStatus().GetState(), actor.GetStatus().GetWorkerAssignment(), wf.State)
			}
			if got != codes.NotFound {
				t.Errorf("certificate mint after the worker was released: code %v, want NotFound", got)
			}

			// The worker takes another actor.
			controlapi.SeedAssignment(t, persistence, workerName, &ateapipb.ActorAssignment{
				Actor:    &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: "actor-2"},
				ActorUid: "5c1d2e3f-4a5b-4c6d-8e7f-9a0b1c2d3e4f",
			})
			if _, got := mint("worker rebooked for another actor"); got != codes.NotFound {
				t.Errorf("certificate mint after the worker was rebooked: code %v, want NotFound", got)
			}

			// A retry records the new state.
			if err := wf.Finalize(t, ctx, persistence, actorRef, actor); apierror.Code(err) != wf.WantRetry {
				t.Fatalf("finalize retry: code %v, want %v (err: %v)", apierror.Code(err), wf.WantRetry, err)
			}
			actor, got = mint("finalized")
			if actor.GetStatus().GetState() != wf.WantFinalState {
				t.Errorf("actor state once finalized = %v, want %v", actor.GetStatus().GetState(), wf.WantFinalState)
			}
			// No worker runs the actor once finalized, with or without a local snapshot.
			if got != codes.NotFound {
				t.Errorf("certificate mint once finalized: code %v, want NotFound", got)
			}
		})
	}
}

// newTestMinter returns a WorkerService that mints worker-for-actor
// certificates against st for the atelets ateletauthtest certifies.
func newTestMinter(t *testing.T, st store.Interface) *workerservice.Server {
	t.Helper()
	ca, err := localca.GenerateCA("1", localca.KeyTypeECDSAP256, 24*time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	caPool := &localca.ConcretePool{CAs: []*localca.CA{ca}, ActiveForSigning: "1"}
	ateletID := installdefaults.SPIFFEID(installdefaults.SystemNamespace, installdefaults.AteletServiceAccount)
	return workerservice.New(st, nil, ateletID, caPool, "", nil)
}

// newTestCSR returns a certificate signing request for a fresh actor key.
func newTestCSR(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatalf("CreateCertificateRequest: %v", err)
	}
	return csr
}
