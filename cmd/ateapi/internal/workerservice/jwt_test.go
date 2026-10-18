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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"
	"go.ate.dev/substrate/internal/actoridjwt"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/localjwtauthority"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
)

const testActorJWTIssuer = "https://idp.ate-system.svc"

func newTestJWTPool(t *testing.T) localjwtauthority.Pool {
	t.Helper()
	authority, err := localjwtauthority.GenerateAuthority("ES256", "1")
	if err != nil {
		t.Fatalf("generate JWT authority: %v", err)
	}
	return &localjwtauthority.ConcretePool{Authorities: []*localjwtauthority.Authority{authority}, ActiveForSigning: "1"}
}

// decodeJWTClaims returns the payload of a compact JWS without verifying it.
// The signing path is covered by the pool's own tests.
func decodeJWTClaims(t *testing.T, token string) actoridjwt.WireClaims {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d parts, want 3", len(parts))
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var claims actoridjwt.WireClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	return claims
}

func TestMintWorkerActorJwt(t *testing.T) {
	st, actor := newFakeStoreWithActor("team-a", "my-actor", "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21")
	s := New(st, &fakeSuspender{}, testAteletSPIFFEID, nil, testActorJWTIssuer, newTestJWTPool(t))

	ctx := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "node-1"))
	before := time.Now().Truncate(time.Second)
	resp, err := s.MintWorkerActorJwt(ctx, &ateapipb.MintWorkerActorJwtRequest{
		Actor:     &ateapipb.ObjectRef{Atespace: "team-a", Name: "my-actor"},
		ActorUid:  actor.GetMetadata().GetUid(),
		Audiences: []string{"snapshot-plugin"},
	})
	if err != nil {
		t.Fatalf("MintWorkerActorJwt() failed: %v", err)
	}

	claims := decodeJWTClaims(t, resp.GetActorJwt())
	if claims.Issuer != testActorJWTIssuer {
		t.Errorf("iss = %q, want %q", claims.Issuer, testActorJWTIssuer)
	}
	if want := "worker-for-actor/team-a/my-actor"; claims.Subject != want {
		t.Errorf("sub = %q, want %q", claims.Subject, want)
	}
	var audiences []string
	if err := json.Unmarshal(claims.Audiences, &audiences); err != nil {
		t.Fatalf("aud is not a JSON array: %s", claims.Audiences)
	}
	if len(audiences) != 1 || audiences[0] != "snapshot-plugin" {
		t.Errorf("aud = %v, want [snapshot-plugin]", audiences)
	}
	kind, substrate, ok := claims.Substrate()
	if !ok || kind != actoridjwt.KindWorkerForActor {
		t.Fatalf("token kind = %v (ok %v), want KindWorkerForActor under worker-for-actor.ate.dev", kind, ok)
	}
	if got, want := *substrate, (actoridjwt.WireSubstrateClaims{Atespace: "team-a", ActorName: "my-actor", ActorUID: actor.GetMetadata().GetUid()}); got != want {
		t.Errorf("worker-for-actor.ate.dev = %+v, want %+v", got, want)
	}

	expiresAt := resp.GetExpireTime().AsTime()
	if got := time.Unix(int64(claims.Expiration), 0); !got.Equal(expiresAt) {
		t.Errorf("exp = %v, expire_time = %v; want equal", got, expiresAt)
	}
	lifetime := expiresAt.Sub(time.Unix(int64(claims.IssuedAt), 0))
	if lifetime != workerActorJWTLifetime {
		t.Errorf("exp - iat = %v, want %v", lifetime, workerActorJWTLifetime)
	}
	if expiresAt.Before(before.Add(workerActorJWTLifetime)) {
		t.Errorf("expire_time = %v is earlier than %v + lifetime", expiresAt, before)
	}
}

// pausedStatus is how a paused actor looks: SUSPENDED, no worker, assigned_node
// on the node holding its local snapshot, and a latest generation that is
// local-only. The latest generation is the one last_assigned_generation names.
func pausedStatus(state ateapipb.ActorState, node string) *ateapipb.ActorStatus {
	return &ateapipb.ActorStatus{
		State:                  state,
		AssignedNode:           node,
		LastAssignedGeneration: 1,
		Snapshots:              []*ateapipb.Snapshot{{Uuid: "snap-1", Generation: 1, Owner: ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, Locality: node}},
	}
}

// uploadPendingStatus is how a paused actor looks until atelet's background
// upload of its local snapshot is recorded: SUSPENDED, no worker, and the
// latest generation's durable copy PENDING.
func uploadPendingStatus(node string) *ateapipb.ActorStatus {
	status := pausedStatus(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, node)
	status.Snapshots[0].DurableSnapshot = &ateapipb.SnapshotStorage{
		Status: ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING,
		Object: &ateapipb.ObjectSnapshot{SnapshotUri: "gs://snaps/root/atespaces/team-a/actors/uid/snap-1"},
	}
	return status
}

func TestMintWorkerActorJwt_Errors(t *testing.T) {
	st, actor := newFakeStoreWithActor("team-a", "my-actor", "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21")
	s := New(st, &fakeSuspender{}, testAteletSPIFFEID, nil, testActorJWTIssuer, newTestJWTPool(t))
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "node-1"))

	valid := func() *ateapipb.MintWorkerActorJwtRequest {
		return &ateapipb.MintWorkerActorJwtRequest{
			Actor:     &ateapipb.ObjectRef{Atespace: "team-a", Name: "my-actor"},
			ActorUid:  actor.GetMetadata().GetUid(),
			Audiences: []string{"snapshot-plugin"},
		}
	}
	const otherActorUID = "9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d"
	// onWorker is an actor status naming worker-1, which runs on node-1.
	onWorker := func(state ateapipb.ActorState) *ateapipb.ActorStatus {
		return &ateapipb.ActorStatus{State: state, WorkerAssignment: &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: "worker-1"}}}
	}
	// pausingOnWorker is a pause in progress: SUSPENDING on worker-1 with a
	// latest snapshot that is node-local and not yet captured.
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
		mutate func(*ateapipb.MintWorkerActorJwtRequest)
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
			name:   "resuming actor on its worker's node",
			ctx:    authed,
			status: onWorker(ateapipb.ActorState_ACTOR_STATE_RESUMING),
			want:   codes.OK,
		},
		{
			// Suspending a running actor uploads its checkpoint from its worker.
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
			// Pause, suspend and crash release the worker before they clear the
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
			name: "unauthenticated",
			ctx:  ateletauthtest.ContextWith(nil),
			want: codes.Unauthenticated,
		},
		{
			name:   "missing actor",
			ctx:    authed,
			mutate: func(r *ateapipb.MintWorkerActorJwtRequest) { r.Actor = nil },
			want:   codes.InvalidArgument,
		},
		{
			name:   "missing actor uid",
			ctx:    authed,
			mutate: func(r *ateapipb.MintWorkerActorJwtRequest) { r.ActorUid = "" },
			want:   codes.InvalidArgument,
		},
		{
			name:   "missing audiences",
			ctx:    authed,
			mutate: func(r *ateapipb.MintWorkerActorJwtRequest) { r.Audiences = nil },
			want:   codes.InvalidArgument,
		},
		{
			name:   "empty audience",
			ctx:    authed,
			mutate: func(r *ateapipb.MintWorkerActorJwtRequest) { r.Audiences = []string{""} },
			want:   codes.InvalidArgument,
		},
		{
			// The actor's worker runs on node-1 and the caller proves it is on node-2.
			name: "actor hosted on another node",
			ctx:  ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "node-2")),
			want: codes.NotFound,
		},
		{
			// A foreign node must not learn that the actor exists from the UID
			// check answering Aborted: the node check runs first.
			name:   "actor hosted on another node, stale uid",
			ctx:    ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "node-2")),
			mutate: func(r *ateapipb.MintWorkerActorJwtRequest) { r.ActorUid = "7d1c0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a" },
			want:   codes.NotFound,
		},
		{
			name:   "paused actor on this node",
			ctx:    authed,
			status: pausedStatus(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "node-1"),
			want:   codes.OK,
		},
		{
			name:   "paused actor on another node",
			ctx:    authed,
			status: pausedStatus(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "node-2"),
			want:   codes.NotFound,
		},
		{
			// atelet uploads the paused actor's checkpoint in the background
			// from the node that holds it, with no worker assigned.
			name:   "paused actor with its upload pending on this node",
			ctx:    authed,
			status: uploadPendingStatus("node-1"),
			want:   codes.OK,
		},
		{
			name:   "paused actor with its upload pending on another node",
			ctx:    authed,
			status: uploadPendingStatus("node-2"),
			want:   codes.NotFound,
		},
		{
			// The node still holds the snapshot's files until its detach report
			// clears the locality, so it still acts for the actor.
			name: "paused actor whose snapshot is uploaded, local copy kept",
			ctx:  authed,
			status: func() *ateapipb.ActorStatus {
				status := uploadPendingStatus("node-1")
				status.Snapshots[0].DurableSnapshot.Status = ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
				return status
			}(),
			want: codes.OK,
		},
		{
			// Only a SUSPENDED actor is hosted by a node with no worker.
			name: "suspending actor with no worker",
			ctx:  authed,
			status: func() *ateapipb.ActorStatus {
				status := uploadPendingStatus("node-1")
				status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDING
				return status
			}(),
			want: codes.NotFound,
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
			// A suspended actor is hosted nowhere, so no node may mint for it.
			name:   "actor hosted by no node",
			ctx:    authed,
			status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
			want:   codes.NotFound,
		},
		{
			name:   "actor assigned to a worker the store has lost",
			ctx:    authed,
			status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING, WorkerAssignment: &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: "worker-gone"}}},
			want:   codes.NotFound,
		},
		{
			name:   "actor not found",
			ctx:    authed,
			mutate: func(r *ateapipb.MintWorkerActorJwtRequest) { r.Actor.Name = "nonexistent" },
			want:   codes.NotFound,
		},
		{
			// A recreated actor has a new UID, and a token must not be minted
			// for the old incarnation. Aborted, as MintWorkerActorCertificate answers.
			name:   "stale actor uid",
			ctx:    authed,
			mutate: func(r *ateapipb.MintWorkerActorJwtRequest) { r.ActorUid = "7d1c0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a" },
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
			_, err := s.MintWorkerActorJwt(tc.ctx, req)
			if got := apierror.Code(err); got != tc.want {
				t.Errorf("MintWorkerActorJwt() code = %v, want %v (err: %v)", got, tc.want, err)
			}
		})
	}
}

// TestNodeRefusalLogNamesCredential checks that each mint's node refusal logs
// the credential that RPC mints.
func TestNodeRefusalLogNamesCredential(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	st, actor := newFakeStoreWithActor("team-a", "my-actor", "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21")
	csr, _ := generateTestCSR(t)
	s := New(st, &fakeSuspender{}, testAteletSPIFFEID, newTestCAPool(t), testActorJWTIssuer, newTestJWTPool(t))
	// The actor's worker runs on node-1.
	otherNode := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "node-2"))
	actorRef := &ateapipb.ObjectRef{Atespace: "team-a", Name: "my-actor"}

	tests := []struct {
		name string
		mint func() error
		want string
	}{
		{
			name: "MintWorkerActorJwt",
			mint: func() error {
				_, err := s.MintWorkerActorJwt(otherNode, &ateapipb.MintWorkerActorJwtRequest{
					Actor:     actorRef,
					ActorUid:  actor.GetMetadata().GetUid(),
					Audiences: []string{"snapshot-plugin"},
				})
				return err
			},
			want: "Refusing a worker-for-actor JWT for an actor the caller's node does not host",
		},
		{
			name: "MintWorkerActorCertificate",
			mint: func() error {
				_, err := s.MintWorkerActorCertificate(otherNode, &ateapipb.MintWorkerActorCertificateRequest{
					Actor:                     actorRef,
					ActorUid:                  actor.GetMetadata().GetUid(),
					CertificateSigningRequest: csr,
					Worker:                    &ateapipb.ObjectRef{Name: "worker-1"},
				})
				return err
			},
			want: "Refusing a worker-for-actor certificate for an actor the caller's node does not host",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf.Reset()
			if got := apierror.Code(tc.mint()); got != codes.NotFound {
				t.Fatalf("code = %v, want %v", got, codes.NotFound)
			}
			if !strings.Contains(buf.String(), tc.want) {
				t.Errorf("log = %q, want it to contain %q", buf.String(), tc.want)
			}
		})
	}
}
