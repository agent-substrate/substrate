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

package exchange

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/actoridjwt"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorscope"
	"go.ate.dev/substrate/internal/objectstoreplugin/credential"
	"go.ate.dev/substrate/internal/oidcjwt"
	exchangepb "go.ate.dev/substrate/internal/proto/objectstoreexchangepb"
	"go.ate.dev/substrate/internal/resources"
)

// The checks behind the exchange are tested in actorauthz. These tests cover
// what the exchange adds: the issuer call, its errors and the response.

const (
	atespace = "team-a"
	name     = "bot"
	uid      = "9f1e0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a"
	tagURI   = "gs://snaps/root/atespaces/ate-golden/tags/7a3c0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a"
)

// fakeVerifier accepts any token as one of the given kind for the actor on
// node-1.
type fakeVerifier struct {
	kind actoridjwt.Kind
}

func (f fakeVerifier) Verify(context.Context, string, time.Time) (*oidcjwt.Claims, error) {
	return &oidcjwt.Claims{SubstrateClaims: oidcjwt.SubstrateClaims{
		Kind:      f.kind,
		Substrate: &actoridjwt.WireSubstrateClaims{Atespace: atespace, ActorName: name, ActorUID: uid, Node: "node-1"},
	}}, nil
}

// fakeLookup answers with one actor.
type fakeLookup struct {
	actor *ateapi.Actor
}

func (f fakeLookup) Actor(context.Context, *actoridjwt.WireSubstrateClaims) (*ateapi.Actor, error) {
	return f.actor, nil
}

// fakeIssuer records what it was asked to mint.
type fakeIssuer struct {
	err    error
	calls  int
	uid    string
	grants []actorscope.Grant
	force  bool
	expiry time.Time
}

func (f *fakeIssuer) Issue(_ context.Context, actorUID string, grants []actorscope.Grant, force bool) (credential.Token, error) {
	f.calls++
	f.uid, f.grants, f.force = actorUID, grants, force
	if f.err != nil {
		return credential.Token{}, f.err
	}
	return credential.Token{AccessToken: "downscoped", Expiry: f.expiry}, nil
}

// actorWithTag is a running actor on node-1 that still carries the tag
// generation it was created from.
func actorWithTag() *ateapi.Actor {
	return &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Atespace: atespace, Name: name, Uid: uid},
		Status: &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_RUNNING, AssignedNode: "node-1", Snapshots: []*ateapi.Snapshot{{
			Generation: 1, Owner: ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG,
			DurableSnapshot: &ateapi.SnapshotStorage{
				Status: ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED,
				Object: &ateapi.ObjectSnapshot{SnapshotUri: tagURI},
			},
		}}},
	}
}

func TestExchangeActorJwt(t *testing.T) {
	expiry := time.Now().Add(50 * time.Minute).Truncate(time.Second)
	issuerOK := &fakeIssuer{expiry: expiry}
	srv := New(fakeVerifier{kind: actoridjwt.KindWorkerForActor}, fakeLookup{actor: actorWithTag()}, issuerOK)

	for _, force := range []bool{false, true} {
		resp, err := srv.ExchangeActorJwt(t.Context(), &exchangepb.ExchangeActorJwtRequest{ActorJwt: "jwt", Force: force})
		if err != nil {
			t.Fatalf("force=%v: %v", force, err)
		}
		if resp.GetAccessToken() != "downscoped" || !resp.GetExpireTime().AsTime().Equal(expiry) {
			t.Errorf("force=%v: response = %v", force, resp)
		}
		if issuerOK.uid != uid || issuerOK.force != force {
			t.Errorf("force=%v: issuer asked for uid=%s force=%v", force, issuerOK.uid, issuerOK.force)
		}
		// Read-write on the actor's own prefix at the tag's location, read-only
		// on the tag it was created from.
		want := []actorscope.Grant{
			{Prefix: storagePrefix(t, "gs://snaps/root/atespaces/"+atespace+"/actors/"+uid), Access: actorscope.ReadWrite},
			{Prefix: storagePrefix(t, tagURI), Access: actorscope.ReadOnly},
		}
		if !slices.Equal(issuerOK.grants, want) {
			t.Errorf("force=%v: issuer asked for grants %v, want %v", force, issuerOK.grants, want)
		}
	}
	if issuerOK.calls != 2 {
		t.Errorf("issuer calls = %d, want 2", issuerOK.calls)
	}
}

func storagePrefix(t *testing.T, uri string) resources.StoragePrefix {
	t.Helper()
	p, err := resources.ParseStoragePrefix(uri)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestExchangeActorJwt_Errors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		kind    actoridjwt.Kind
		issuer  *fakeIssuer
		want    codes.Code
		wantMsg string
	}{
		{"actor's own token", actoridjwt.KindActor, &fakeIssuer{}, codes.PermissionDenied, "only a worker-for-actor token may be exchanged"},
		{"issuer failing", actoridjwt.KindWorkerForActor, &fakeIssuer{err: errors.New("STS 503")}, codes.Unavailable, "while issuing the credential: STS 503"},
		{"issuer cannot express the grants", actoridjwt.KindWorkerForActor, &fakeIssuer{err: status.Error(codes.Internal, "bad boundary")}, codes.Internal, "while issuing the credential: rpc error: code = Internal desc = bad boundary"},
		{"caller gave up at STS", actoridjwt.KindWorkerForActor, &fakeIssuer{err: fmt.Errorf("Post sts: %w", context.Canceled)}, codes.Canceled, "while issuing the credential: Post sts: context canceled"},
		{"caller's deadline passed at STS", actoridjwt.KindWorkerForActor, &fakeIssuer{err: fmt.Errorf("Post sts: %w", context.DeadlineExceeded)}, codes.DeadlineExceeded, "while issuing the credential: Post sts: context deadline exceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := New(fakeVerifier{kind: tc.kind}, fakeLookup{actor: actorWithTag()}, tc.issuer)
			resp, err := srv.ExchangeActorJwt(t.Context(), &exchangepb.ExchangeActorJwtRequest{ActorJwt: "jwt"})
			if resp != nil {
				t.Errorf("response = %v, want nil", resp)
			}
			if st := status.Convert(err); st.Code() != tc.want || st.Message() != tc.wantMsg {
				t.Errorf("error = %v %q, want %v %q", st.Code(), st.Message(), tc.want, tc.wantMsg)
			}
			wantCalls := 0
			if tc.issuer.err != nil {
				wantCalls = 1
			}
			if tc.issuer.calls != wantCalls {
				t.Errorf("issuer was called %d times, want %d", tc.issuer.calls, wantCalls)
			}
		})
	}
}
