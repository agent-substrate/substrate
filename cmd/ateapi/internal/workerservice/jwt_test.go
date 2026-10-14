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
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"
	"github.com/agent-substrate/substrate/internal/actoridjwt"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/localjwtauthority"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
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

func TestMintAteomActorJwt(t *testing.T) {
	st, actor := newFakeStoreWithActor("team-a", "my-actor", "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21")
	s := New(st, &fakeSuspender{}, testAteletSPIFFEID, nil, testActorJWTIssuer, newTestJWTPool(t))

	ctx := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "node-1"))
	before := time.Now().Truncate(time.Second)
	resp, err := s.MintAteomActorJwt(ctx, &ateapipb.MintAteomActorJwtRequest{
		Actor:     &ateapipb.ObjectRef{Atespace: "team-a", Name: "my-actor"},
		ActorUid:  actor.GetMetadata().GetUid(),
		Audiences: []string{"snapshot-plugin"},
	})
	if err != nil {
		t.Fatalf("MintAteomActorJwt() failed: %v", err)
	}

	claims := decodeJWTClaims(t, resp.GetActorJwt())
	if claims.Issuer != testActorJWTIssuer {
		t.Errorf("iss = %q, want %q", claims.Issuer, testActorJWTIssuer)
	}
	if want := "ateom-for-actor/team-a/my-actor"; claims.Subject != want {
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
	if !ok || kind != actoridjwt.KindAteomForActor {
		t.Fatalf("token kind = %v (ok %v), want KindAteomForActor under ateom-for-actor.ate.dev", kind, ok)
	}
	if got, want := *substrate, (actoridjwt.WireSubstrateClaims{Atespace: "team-a", ActorName: "my-actor", ActorUID: actor.GetMetadata().GetUid()}); got != want {
		t.Errorf("ateom-for-actor.ate.dev = %+v, want %+v", got, want)
	}

	expiresAt := resp.GetExpireTime().AsTime()
	if got := time.Unix(int64(claims.Expiration), 0); !got.Equal(expiresAt) {
		t.Errorf("exp = %v, expire_time = %v; want equal", got, expiresAt)
	}
	lifetime := expiresAt.Sub(time.Unix(int64(claims.IssuedAt), 0))
	if lifetime != ateomActorJWTLifetime {
		t.Errorf("exp - iat = %v, want %v", lifetime, ateomActorJWTLifetime)
	}
	if expiresAt.Before(before.Add(ateomActorJWTLifetime)) {
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

func TestMintAteomActorJwt_Errors(t *testing.T) {
	st, actor := newFakeStoreWithActor("team-a", "my-actor", "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21")
	s := New(st, &fakeSuspender{}, testAteletSPIFFEID, nil, testActorJWTIssuer, newTestJWTPool(t))
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "node-1"))

	valid := func() *ateapipb.MintAteomActorJwtRequest {
		return &ateapipb.MintAteomActorJwtRequest{
			Actor:     &ateapipb.ObjectRef{Atespace: "team-a", Name: "my-actor"},
			ActorUid:  actor.GetMetadata().GetUid(),
			Audiences: []string{"snapshot-plugin"},
		}
	}
	tests := []struct {
		name   string
		ctx    context.Context
		mutate func(*ateapipb.MintAteomActorJwtRequest)
		// status, when set, replaces the actor's status for the case.
		status *ateapipb.ActorStatus
		want   codes.Code
	}{
		{
			name: "unauthenticated",
			ctx:  ateletauthtest.ContextWith(nil),
			want: codes.Unauthenticated,
		},
		{
			name:   "missing actor",
			ctx:    authed,
			mutate: func(r *ateapipb.MintAteomActorJwtRequest) { r.Actor = nil },
			want:   codes.InvalidArgument,
		},
		{
			name:   "missing actor uid",
			ctx:    authed,
			mutate: func(r *ateapipb.MintAteomActorJwtRequest) { r.ActorUid = "" },
			want:   codes.InvalidArgument,
		},
		{
			name:   "missing audiences",
			ctx:    authed,
			mutate: func(r *ateapipb.MintAteomActorJwtRequest) { r.Audiences = nil },
			want:   codes.InvalidArgument,
		},
		{
			name:   "empty audience",
			ctx:    authed,
			mutate: func(r *ateapipb.MintAteomActorJwtRequest) { r.Audiences = []string{""} },
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
			mutate: func(r *ateapipb.MintAteomActorJwtRequest) { r.ActorUid = "7d1c0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a" },
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
			// assigned_node without a local-only snapshot is not a paused actor,
			// whatever left it set.
			name:   "suspended actor with a stale assigned_node",
			ctx:    authed,
			status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, AssignedNode: "node-1"},
			want:   codes.NotFound,
		},
		{
			// Suspending a paused actor uploads its checkpoint from the node that
			// holds it, with no worker assigned.
			name:   "paused actor being suspended on this node",
			ctx:    authed,
			status: pausedStatus(ateapipb.ActorState_ACTOR_STATE_SUSPENDING, "node-1"),
			want:   codes.OK,
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
			mutate: func(r *ateapipb.MintAteomActorJwtRequest) { r.Actor.Name = "nonexistent" },
			want:   codes.NotFound,
		},
		{
			// A recreated actor has a new UID, and a token must not be minted
			// for the old incarnation. Aborted, as MintAteomActorCertificate answers.
			name:   "stale actor uid",
			ctx:    authed,
			mutate: func(r *ateapipb.MintAteomActorJwtRequest) { r.ActorUid = "7d1c0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a" },
			want:   codes.Aborted,
		},
	}
	hostedStatus := actor.GetStatus()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actor.Status = hostedStatus
			if tc.status != nil {
				actor.Status = tc.status
			}
			req := valid()
			if tc.mutate != nil {
				tc.mutate(req)
			}
			_, err := s.MintAteomActorJwt(tc.ctx, req)
			if got := apierror.Code(err); got != tc.want {
				t.Errorf("MintAteomActorJwt() code = %v, want %v (err: %v)", got, tc.want, err)
			}
		})
	}
}
