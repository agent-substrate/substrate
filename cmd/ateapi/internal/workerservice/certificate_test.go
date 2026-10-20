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

package workerservice

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/localca"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/substratex509"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
)

type fakeActorStore struct {
	store.Interface
	actors  map[resources.ActorRef]*ateapipb.Actor
	workers map[string]*ateapipb.Worker
	// assignments maps an actor UID to the worker holding its assignment.
	assignments map[string]string
	// assignmentErr, when set, fails every GetWorkerAssignment.
	assignmentErr error
}

func (f *fakeActorStore) GetWorkerAssignment(_ context.Context, workerName, actorUID string) (*ateapipb.ActorAssignment, error) {
	if f.assignmentErr != nil {
		return nil, f.assignmentErr
	}
	if holder, ok := f.assignments[actorUID]; !ok || holder != workerName {
		return nil, store.ErrNotFound
	}
	return &ateapipb.ActorAssignment{ActorUid: actorUID}, nil
}

func (f *fakeActorStore) GetWorker(_ context.Context, name string) (*ateapipb.Worker, error) {
	w, ok := f.workers[name]
	if !ok {
		return nil, store.ErrNotFound
	}
	return w, nil
}

func (f *fakeActorStore) GetActor(_ context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
	a, ok := f.actors[actorRef]
	if !ok {
		return nil, store.ErrNotFound
	}
	return a, nil
}

func newFakeStoreWithActor(atespace, name, uid string) (*fakeActorStore, *ateapipb.Actor) {
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: atespace,
			Name:     name,
			Uid:      uid,
		},
		ActorTemplate: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     "template-1",
		},
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: "worker-1"}},
		},
	}
	return &fakeActorStore{
		actors: map[resources.ActorRef]*ateapipb.Actor{
			{Atespace: atespace, Name: name}: actor,
		},
		// worker-1 runs on node-1, the node the tests' atelet certificate names.
		workers: map[string]*ateapipb.Worker{
			"worker-1": {Metadata: &ateapipb.ResourceMetadata{Name: "worker-1"}, NodeName: "node-1"},
		},
		assignments: map[string]string{uid: "worker-1"},
	}, actor
}

func generateTestCSR(t *testing.T) ([]byte, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatalf("create CSR: %v", err)
	}
	return csr, key
}

func newTestCAPool(t *testing.T) *localca.ConcretePool {
	t.Helper()
	ca, err := localca.GenerateCA("1", localca.KeyTypeECDSAP256, 24*time.Hour)
	if err != nil {
		t.Fatalf("generate CA: %v", err)
	}
	return &localca.ConcretePool{
		CAs:              []*localca.CA{ca},
		ActiveForSigning: "1",
	}
}

func TestMintWorkerActorCertificate(t *testing.T) {
	st, actor := newFakeStoreWithActor("team-a", "my-actor", "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21")
	caPool := newTestCAPool(t)
	s := New(st, &fakeControl{}, testAteletSPIFFEID, caPool, "", nil)

	csr, key := generateTestCSR(t)

	ctx := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "node-1"))
	resp, err := s.MintWorkerActorCertificate(ctx, &ateapipb.MintWorkerActorCertificateRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: "team-a",
			Name:     "my-actor",
		},
		ActorUid:                  actor.GetMetadata().GetUid(),
		CertificateSigningRequest: csr,
		Worker:                    &ateapipb.ObjectRef{Name: "worker-1"},
	})
	if err != nil {
		t.Fatalf("MintWorkerActorCertificate() failed: %v", err)
	}

	chain := resp.GetActorCertificates()
	if len(chain) == 0 {
		t.Fatal("MintWorkerActorCertificate() returned empty chain")
	}

	leaf, err := x509.ParseCertificate(chain[0])
	if err != nil {
		t.Fatalf("parse leaf certificate: %v", err)
	}
	if !key.PublicKey.Equal(leaf.PublicKey) {
		t.Error("leaf public key does not match CSR key")
	}

	wantURI := "spiffe://substrate-actor.local/worker-for-actor/team-a/my-actor"
	if len(leaf.URIs) != 1 || leaf.URIs[0].String() != wantURI {
		t.Errorf("leaf URIs = %v, want [%s]", leaf.URIs, wantURI)
	}

	identity, err := substratex509.ActorIdentityFromCertificate(leaf)
	if err != nil {
		t.Fatalf("ActorIdentityFromCertificate: %v", err)
	}
	wantIdentity := &substratex509.ActorIdentity{
		Atespace:  "team-a",
		ActorName: "my-actor",
		ActorUid:  actor.GetMetadata().GetUid(),
	}
	if diff := cmp.Diff(wantIdentity, identity); diff != "" {
		t.Errorf("ActorIdentity mismatch (-want +got):\n%s", diff)
	}
}

func TestMintWorkerActorCertificate_Errors(t *testing.T) {
	st, actor := newFakeStoreWithActor("team-a", "my-actor", "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21")
	caPool := newTestCAPool(t)
	s := New(st, &fakeControl{}, testAteletSPIFFEID, caPool, "", nil)

	csr, _ := generateTestCSR(t)
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "node-1"))
	otherNode := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "node-2"))

	valid := func() *ateapipb.MintWorkerActorCertificateRequest {
		return &ateapipb.MintWorkerActorCertificateRequest{
			Actor:                     &ateapipb.ObjectRef{Atespace: "team-a", Name: "my-actor"},
			ActorUid:                  actor.GetMetadata().GetUid(),
			CertificateSigningRequest: csr,
			Worker:                    &ateapipb.ObjectRef{Name: "worker-1"},
		}
	}
	const (
		staleUID      = "7d1c0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a"
		otherActorUID = "9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d"
	)
	// onWorker is an actor status naming worker-1, which runs on node-1.
	onWorker := func(state ateapipb.ActorState) *ateapipb.ActorStatus {
		return &ateapipb.ActorStatus{State: state, WorkerAssignment: &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: "worker-1"}}}
	}
	// pausingOnWorker is an async suspend in progress: SUSPENDING on worker-1
	// with a latest snapshot that is not yet captured.
	pausingOnWorker := func() *ateapipb.ActorStatus {
		status := onWorker(ateapipb.ActorState_ACTOR_STATE_SUSPENDING)
		status.AssignedNode = "node-1"
		status.LastAssignedGeneration = 1
		status.Snapshots = []*ateapipb.Snapshot{{Uuid: "snap-1", Generation: 1, Owner: ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR}}
		return status
	}
	released := map[string]string{}
	rebooked := map[string]string{otherActorUID: "worker-1"}
	tests := []struct {
		name   string
		ctx    context.Context
		mutate func(*ateapipb.MintWorkerActorCertificateRequest)
		// status, when set, replaces the actor's status for the case.
		status *ateapipb.ActorStatus
		// assignments, when set, replaces the store's worker assignments
		// (actor UID to worker) for the case. By default worker-1 holds the
		// actor.
		assignments map[string]string
		// assignmentErr, when set, fails the store's worker assignment
		// lookup.
		assignmentErr error
		want          codes.Code
	}{
		{
			name:   "running actor on its worker's node",
			ctx:    authed,
			status: onWorker(ateapipb.ActorState_ACTOR_STATE_RUNNING),
			want:   codes.OK,
		},
		{
			// atunnel mints while the actor resumes, before it runs.
			name:   "resuming actor on its worker's node",
			ctx:    authed,
			status: onWorker(ateapipb.ActorState_ACTOR_STATE_RESUMING),
			want:   codes.OK,
		},
		{
			name:   "running actor being suspended on its worker's node",
			ctx:    authed,
			status: onWorker(ateapipb.ActorState_ACTOR_STATE_SUSPENDING),
			want:   codes.OK,
		},
		{
			name:   "running actor being paused on its worker's node",
			ctx:    authed,
			status: pausingOnWorker(),
			want:   codes.OK,
		},
		{
			// Suspend and crash release the worker before they clear the
			// actor's status, which keeps naming the worker until they do.
			name:        "running actor whose worker was released",
			ctx:         authed,
			status:      onWorker(ateapipb.ActorState_ACTOR_STATE_RUNNING),
			assignments: released,
			want:        codes.NotFound,
		},
		{
			// A resume that finds its worker ineligible releases it, then
			// crashes the actor.
			name:        "resuming actor whose worker was released",
			ctx:         authed,
			status:      onWorker(ateapipb.ActorState_ACTOR_STATE_RESUMING),
			assignments: released,
			want:        codes.NotFound,
		},
		{
			name:        "suspending actor whose worker was released",
			ctx:         authed,
			status:      onWorker(ateapipb.ActorState_ACTOR_STATE_SUSPENDING),
			assignments: released,
			want:        codes.NotFound,
		},
		{
			name:        "pausing actor whose worker was released",
			ctx:         authed,
			status:      pausingOnWorker(),
			assignments: released,
			want:        codes.NotFound,
		},
		{
			// The released worker took another actor. Its node hosts that one,
			// not this one.
			name:        "running actor whose worker was rebooked for another actor",
			ctx:         authed,
			status:      onWorker(ateapipb.ActorState_ACTOR_STATE_RUNNING),
			assignments: rebooked,
			want:        codes.NotFound,
		},
		{
			name:        "resuming actor whose worker was rebooked for another actor",
			ctx:         authed,
			status:      onWorker(ateapipb.ActorState_ACTOR_STATE_RESUMING),
			assignments: rebooked,
			want:        codes.NotFound,
		},
		{
			name:        "suspending actor whose worker was rebooked for another actor",
			ctx:         authed,
			status:      onWorker(ateapipb.ActorState_ACTOR_STATE_SUSPENDING),
			assignments: rebooked,
			want:        codes.NotFound,
		},
		{
			name:        "pausing actor whose worker was rebooked for another actor",
			ctx:         authed,
			status:      pausingOnWorker(),
			assignments: rebooked,
			want:        codes.NotFound,
		},
		{
			// The store binds the actor to a different worker than its status
			// names, so the status's worker does not host it.
			name:        "actor held by a worker other than the one its status names",
			ctx:         authed,
			status:      onWorker(ateapipb.ActorState_ACTOR_STATE_RUNNING),
			assignments: map[string]string{actor.GetMetadata().GetUid(): "worker-2"},
			want:        codes.NotFound,
		},
		{
			// A failed lookup refuses the mint rather than trusting the status.
			name:          "worker assignment lookup fails",
			ctx:           authed,
			status:        onWorker(ateapipb.ActorState_ACTOR_STATE_RUNNING),
			assignmentErr: errors.New("connection reset"),
			want:          codes.Internal,
		},
		{
			// The actor's worker runs on node-1 and the caller proves it is on node-2.
			name: "actor hosted on another node",
			ctx:  otherNode,
			want: codes.NotFound,
		},
		{
			// A foreign node must not learn that the actor exists from the UID
			// check answering Aborted: the node check runs first.
			name:   "actor hosted on another node, stale uid",
			ctx:    otherNode,
			mutate: func(r *ateapipb.MintWorkerActorCertificateRequest) { r.ActorUid = staleUID },
			want:   codes.NotFound,
		},
		{
			// worker-2 runs on the actor's node, but the actor is not its own.
			name:   "another worker on the actor's node",
			ctx:    authed,
			mutate: func(r *ateapipb.MintWorkerActorCertificateRequest) { r.Worker.Name = "worker-2" },
			want:   codes.NotFound,
		},
		{
			// The worker check also runs before the UID check.
			name: "another worker on the actor's node, stale uid",
			ctx:  authed,
			mutate: func(r *ateapipb.MintWorkerActorCertificateRequest) {
				r.Worker.Name = "worker-2"
				r.ActorUid = staleUID
			},
			want: codes.NotFound,
		},
		{
			// MintWorkerActorJwt serves a node holding an actor's local snapshot. No
			// worker runs a SUSPENDED actor, so no worker gets its certificate.
			name:        "paused actor on this node",
			ctx:         authed,
			status:      pausedStatus(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "node-1"),
			assignments: released,
			want:        codes.NotFound,
		},
		{
			name:        "paused actor with its upload pending on this node",
			ctx:         authed,
			status:      uploadPendingStatus("node-1"),
			assignments: released,
			want:        codes.NotFound,
		},
		{
			// Resume binds the worker in the store before it writes the
			// assignment to the actor's status, and starts the actor only
			// after that. In between, the worker holds the store's assignment
			// but runs nothing, and HostingNode trusts assigned_node without
			// reading the store, so only the status comparison refuses.
			name:        "paused actor bound to a worker on this node before its status names the worker",
			ctx:         authed,
			status:      pausedStatus(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "node-1"),
			assignments: map[string]string{actor.GetMetadata().GetUid(): "worker-1"},
			want:        codes.NotFound,
		},
		{
			name:        "paused actor on another node",
			ctx:         authed,
			status:      pausedStatus(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "node-2"),
			assignments: released,
			want:        codes.NotFound,
		},
		{
			// An empty worker name must not match a SUSPENDED actor's empty
			// assignment.
			name:        "paused actor, empty worker name",
			ctx:         authed,
			status:      pausedStatus(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "node-1"),
			assignments: released,
			mutate:      func(r *ateapipb.MintWorkerActorCertificateRequest) { r.Worker.Name = "" },
			want:        codes.InvalidArgument,
		},
		{
			// A crash clears the worker but leaves assigned_node on the last
			// node. Nothing is hosted there, so nothing may be minted.
			name:   "crashed actor on its last node",
			ctx:    authed,
			status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_CRASHED, AssignedNode: "node-1"},
			want:   codes.NotFound,
		},
		{
			// A revert releases the worker but keeps the assignment until it
			// finalizes. The node no longer runs the actor.
			name:   "reverting actor with a stale assignment",
			ctx:    authed,
			status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_REVERTING, WorkerAssignment: &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: "worker-1"}}},
			want:   codes.NotFound,
		},
		{
			name:   "actor hosted by no node",
			ctx:    authed,
			status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
			want:   codes.NotFound,
		},
		{
			// The assignment still binds worker-gone, but its Worker record
			// is gone.
			name:        "actor assigned to a worker the store has lost",
			ctx:         authed,
			status:      &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING, WorkerAssignment: &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: "worker-gone"}}},
			assignments: map[string]string{actor.GetMetadata().GetUid(): "worker-gone"},
			want:        codes.NotFound,
		},
		{
			name: "unauthenticated",
			ctx:  ateletauthtest.ContextWith(nil),
			want: codes.Unauthenticated,
		},
		{
			name:   "missing actor",
			ctx:    authed,
			mutate: func(r *ateapipb.MintWorkerActorCertificateRequest) { r.Actor = nil },
			want:   codes.InvalidArgument,
		},
		{
			name:   "missing actor uid",
			ctx:    authed,
			mutate: func(r *ateapipb.MintWorkerActorCertificateRequest) { r.ActorUid = "" },
			want:   codes.InvalidArgument,
		},
		{
			name:   "missing csr",
			ctx:    authed,
			mutate: func(r *ateapipb.MintWorkerActorCertificateRequest) { r.CertificateSigningRequest = nil },
			want:   codes.InvalidArgument,
		},
		{
			name:   "missing worker",
			ctx:    authed,
			mutate: func(r *ateapipb.MintWorkerActorCertificateRequest) { r.Worker = nil },
			want:   codes.InvalidArgument,
		},
		{
			name:   "worker with an atespace",
			ctx:    authed,
			mutate: func(r *ateapipb.MintWorkerActorCertificateRequest) { r.Worker.Atespace = "team-a" },
			want:   codes.InvalidArgument,
		},
		{
			name:   "actor not found",
			ctx:    authed,
			mutate: func(r *ateapipb.MintWorkerActorCertificateRequest) { r.Actor.Name = "nonexistent" },
			want:   codes.NotFound,
		},
		{
			// The assigned worker asks for an incarnation that was deleted and
			// recreated.
			name:   "stale actor uid",
			ctx:    authed,
			mutate: func(r *ateapipb.MintWorkerActorCertificateRequest) { r.ActorUid = staleUID },
			want:   codes.Aborted,
		},
	}
	hostedStatus := actor.GetStatus()
	hostedAssignments := st.assignments
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actor.Status = hostedStatus
			if tc.status != nil {
				actor.Status = tc.status
			}
			st.assignments = hostedAssignments
			if tc.assignments != nil {
				st.assignments = tc.assignments
			}
			st.assignmentErr = tc.assignmentErr
			req := valid()
			if tc.mutate != nil {
				tc.mutate(req)
			}
			_, err := s.MintWorkerActorCertificate(tc.ctx, req)
			if got := apierror.Code(err); got != tc.want {
				t.Errorf("MintWorkerActorCertificate() code = %v, want %v (err: %v)", got, tc.want, err)
			}
		})
	}
}
