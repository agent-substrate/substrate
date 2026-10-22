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

package actorauthz

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/actoridjwt"
	"go.ate.dev/substrate/internal/localjwtauthority"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorlookup"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorscope"
	"go.ate.dev/substrate/internal/oidcjwt"
	"go.ate.dev/substrate/internal/resources"
)

const (
	issuer   = "https://idp.ate-system.svc"
	audience = "objectstore-plugin.ate.dev"
	atespace = "team-a"
	name     = "bot"
	uid      = "9f1e0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a"
	tagURI   = "gs://snaps/root/atespaces/ate-golden/tags/7a3c0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a"
)

// signer mints real JWTs from a pool whose public keys the verifier reads.
type signer struct {
	pool *localjwtauthority.ConcretePool
	now  time.Time
	// node is the node the signed tokens are minted for.
	node string
}

func newSigner(t *testing.T) (*signer, *oidcjwt.Verifier) {
	t.Helper()
	auth, err := localjwtauthority.GenerateAuthority("ES256", "k1")
	if err != nil {
		t.Fatal(err)
	}
	pool := &localjwtauthority.ConcretePool{Authorities: []*localjwtauthority.Authority{auth}, ActiveForSigning: "k1"}
	jwks, err := pool.JWKS()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(path, jwks, 0o600); err != nil {
		t.Fatal(err)
	}
	v := oidcjwt.NewVerifier(issuer, []string{audience}, oidcjwt.FileKeySource(path), oidcjwt.WithSubstrateClaims())
	return &signer{pool: pool, now: time.Now(), node: "node-1"}, v
}

func (s *signer) jwt(t *testing.T, kind actoridjwt.Kind, aud string, actorUID string) string {
	t.Helper()
	substrate := actoridjwt.SubstrateClaims{Atespace: atespace, ActorName: name, ActorUID: actorUID}
	if kind == actoridjwt.KindWorkerForActor {
		// Only a worker-for-actor token is tied to a node.
		substrate.Node = s.node
	}
	jwt, err := s.pool.SignJWT(&actoridjwt.Claims{
		Kind:       kind,
		Issuer:     issuer,
		Subject:    kind.SubjectPrefix() + "/" + atespace + "/" + name,
		Audiences:  []string{aud},
		Expiration: s.now.Add(time.Hour),
		NotBefore:  s.now.Add(-time.Minute),
		IssuedAt:   s.now,
		Substrate:  substrate,
	})
	if err != nil {
		t.Fatal(err)
	}
	return jwt
}

// rawJWT signs an arbitrary claim set with the signer's key, for tokens
// actoridjwt refuses to build.
func (s *signer) rawJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "ES256", "typ": "JWT", "kid": "k1"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	input := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	key := s.pool.Authorities[0].SigningKey.(*ecdsa.PrivateKey)
	digest := sha256.Sum256([]byte(input))
	r, sg, err := ecdsa.Sign(rand.Reader, key, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	sig := make([]byte, 64)
	r.FillBytes(sig[:32])
	sg.FillBytes(sig[32:])
	return input + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// rawClaims is a valid worker-for-actor claim set, for rawJWT to vary.
func (s *signer) rawClaims() map[string]any {
	substrate := map[string]string{"atespace": atespace, "actorName": name, "actorUID": uid, "node": s.node}
	return map[string]any{
		"iss": issuer, "sub": "worker-for-actor/" + atespace + "/" + name, "aud": audience,
		"exp": s.now.Add(time.Hour).Unix(), "nbf": s.now.Add(-time.Minute).Unix(), "iat": s.now.Unix(),
		"worker-for-actor.ate.dev": substrate,
	}
}

// rawJWTWithout signs rawClaims with one field of the substrate claim removed.
func (s *signer) rawJWTWithout(t *testing.T, field string) string {
	t.Helper()
	claims := s.rawClaims()
	delete(claims["worker-for-actor.ate.dev"].(map[string]string), field)
	return s.rawJWT(t, claims)
}

// fakeVerifier returns fixed claims, for claim sets oidcjwt never produces.
type fakeVerifier struct {
	claims *oidcjwt.Claims
	err    error
}

func (f fakeVerifier) Verify(context.Context, string, time.Time) (*oidcjwt.Claims, error) {
	return f.claims, f.err
}

// fakeLookup answers for one actor and counts the lookups.
type fakeLookup struct {
	actor *ateapi.Actor
	err   error
	calls int
}

func (f *fakeLookup) Actor(_ context.Context, claims *actoridjwt.WireSubstrateClaims) (*ateapi.Actor, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if claims.ActorUID != f.actor.GetMetadata().GetUid() {
		return nil, actorlookup.ErrStale
	}
	return f.actor, nil
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

// actorLocalOnly is a suspended actor whose only snapshot is on node-1.
func actorLocalOnly() *ateapi.Actor {
	return &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Atespace: atespace, Name: name, Uid: uid},
		Status: &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED, AssignedNode: "node-1",
			Snapshots: []*ateapi.Snapshot{{Generation: 1, Owner: ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, Locality: "node-1"}}},
	}
}

// actorWithBadURI is actorWithTag with a snapshot URI actorscope cannot parse.
func actorWithBadURI() *ateapi.Actor {
	a := actorWithTag()
	a.Status.Snapshots[0].DurableSnapshot.Object.SnapshotUri = "gs://snaps/not-a-snapshot"
	return a
}

// actorIn returns actorWithTag in the given state on the given node.
func actorIn(state ateapi.ActorState, node string) *ateapi.Actor {
	a := actorWithTag()
	a.Status.State, a.Status.AssignedNode = state, node
	return a
}

func storagePrefix(t *testing.T, uri string) resources.StoragePrefix {
	t.Helper()
	p, err := resources.ParseStoragePrefix(uri)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestAuthorize pins the actor states a token minted on node-1 is good for,
// and what the decision carries.
func TestAuthorize(t *testing.T) {
	s, v := newSigner(t)
	jwt := s.jwt(t, actoridjwt.KindWorkerForActor, audience, uid)
	// Read-write on the actor's own prefix at the tag's location, read-only
	// on the tag it was created from.
	want := &Decision{
		Atespace:  atespace,
		ActorName: name,
		ActorUID:  uid,
		Node:      "node-1",
		Grants: []actorscope.Grant{
			{Prefix: storagePrefix(t, "gs://snaps/root/atespaces/"+atespace+"/actors/"+uid), Access: actorscope.ReadWrite},
			{Prefix: storagePrefix(t, tagURI), Access: actorscope.ReadOnly},
		},
	}
	for _, state := range []ateapi.ActorState{
		ateapi.ActorState_ACTOR_STATE_RESUMING,
		ateapi.ActorState_ACTOR_STATE_RUNNING,
		ateapi.ActorState_ACTOR_STATE_SUSPENDING,
		ateapi.ActorState_ACTOR_STATE_SUSPENDED,
	} {
		t.Run(state.String(), func(t *testing.T) {
			a := New(v, &fakeLookup{actor: actorIn(state, "node-1")})
			a.now = func() time.Time { return s.now }
			got, err := a.Authorize(t.Context(), jwt)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("decision = %+v, want %+v", got, want)
			}
		})
	}
}

func TestAuthorize_Refusals(t *testing.T) {
	s, v := newSigner(t)
	stranger, _ := newSigner(t)
	good := s.jwt(t, actoridjwt.KindWorkerForActor, audience, uid)
	noClaim := s.rawClaims()
	delete(noClaim, "worker-for-actor.ate.dev")
	bothClaims := s.rawClaims()
	bothClaims["actor.ate.dev"] = bothClaims["worker-for-actor.ate.dev"]
	badSignature := stranger.jwt(t, actoridjwt.KindWorkerForActor, audience, uid)
	wrongAudience := s.jwt(t, actoridjwt.KindWorkerForActor, "other", uid)
	bothKinds := s.rawJWT(t, bothClaims)
	// notValid is the message for a token the real verifier refuses, which
	// wraps the verifier's own error.
	notValid := func(jwt string) string {
		_, err := v.Verify(t.Context(), jwt, s.now)
		if err == nil {
			t.Fatal("verifier accepted a token the row expects it to refuse")
		}
		return "actor_jwt is not valid: " + err.Error()
	}
	_, errBadURI := actorscope.FromActor(actorWithBadURI())
	if errBadURI == nil {
		t.Fatal("actorscope accepted the bad snapshot URI")
	}
	const (
		notWorker = "only a worker-for-actor token may be exchanged"
		emptyRef  = "actor_jwt must carry the actor's atespace, name and UID and the minting node, and one of them is empty"
	)
	for _, tc := range []struct {
		name string
		jwt  string
		// verifier replaces the real one when set.
		verifier Verifier
		lookup   *fakeLookup
		want     codes.Code
		wantMsg  string
		// lookedUp says whether the refusal comes after the lookup.
		lookedUp bool
	}{
		{name: "empty jwt", want: codes.InvalidArgument, wantMsg: "actor_jwt is required"},
		{name: "verifier refuses", jwt: good, verifier: fakeVerifier{err: errors.New("expired")}, want: codes.Unauthenticated, wantMsg: "actor_jwt is not valid: expired"},
		{name: "bad signature", jwt: badSignature, want: codes.Unauthenticated, wantMsg: notValid(badSignature)},
		{name: "wrong audience", jwt: wrongAudience, want: codes.Unauthenticated, wantMsg: notValid(wrongAudience)},
		{name: "both substrate claims", jwt: bothKinds, want: codes.Unauthenticated, wantMsg: notValid(bothKinds)},
		{name: "actor's own token", jwt: s.jwt(t, actoridjwt.KindActor, audience, uid), want: codes.PermissionDenied, wantMsg: notWorker},
		{name: "no substrate claim", jwt: s.rawJWT(t, noClaim), want: codes.PermissionDenied, wantMsg: notWorker},
		{name: "worker-for-actor kind without claims", jwt: good, verifier: fakeVerifier{claims: &oidcjwt.Claims{SubstrateClaims: oidcjwt.SubstrateClaims{Kind: actoridjwt.KindWorkerForActor}}}, want: codes.Unauthenticated, wantMsg: emptyRef},
		{name: "claim without the atespace", jwt: s.rawJWTWithout(t, "atespace"), want: codes.Unauthenticated, wantMsg: emptyRef},
		{name: "claim without the actor name", jwt: s.rawJWTWithout(t, "actorName"), want: codes.Unauthenticated, wantMsg: emptyRef},
		{name: "claim without the actor uid", jwt: s.rawJWTWithout(t, "actorUID"), want: codes.Unauthenticated, wantMsg: emptyRef},
		{name: "token without a node", jwt: s.rawJWTWithout(t, "node"), want: codes.Unauthenticated, wantMsg: emptyRef},
		{name: "recreated actor", jwt: s.jwt(t, actoridjwt.KindWorkerForActor, audience, "old-uid"), want: codes.NotFound, wantMsg: "actor not found", lookedUp: true},
		{name: "actor gone", jwt: good, lookup: &fakeLookup{err: status.Error(codes.NotFound, "no such actor")}, want: codes.NotFound, wantMsg: "actor not found", lookedUp: true},
		{name: "ate-api-server rejects the actor ref", jwt: good, lookup: &fakeLookup{err: status.Error(codes.InvalidArgument, "bad name")}, want: codes.Unauthenticated, wantMsg: "actor_jwt names an invalid actor: rpc error: code = InvalidArgument desc = bad name", lookedUp: true},
		{name: "exchange lacks the actor read role", jwt: good, lookup: &fakeLookup{err: status.Error(codes.PermissionDenied, "not allowed")}, want: codes.FailedPrecondition, wantMsg: "the exchange may not read actors: rpc error: code = PermissionDenied desc = not allowed", lookedUp: true},
		{name: "exchange's identity is refused", jwt: good, lookup: &fakeLookup{err: status.Error(codes.Unauthenticated, "bad cert")}, want: codes.FailedPrecondition, wantMsg: "the exchange may not read actors: rpc error: code = Unauthenticated desc = bad cert", lookedUp: true},
		{name: "caller gave up during the lookup", jwt: good, lookup: &fakeLookup{err: status.Error(codes.Canceled, "canceled")}, want: codes.Canceled, wantMsg: "canceled", lookedUp: true},
		{name: "caller's deadline passed during the lookup", jwt: good, lookup: &fakeLookup{err: status.Error(codes.DeadlineExceeded, "deadline")}, want: codes.DeadlineExceeded, wantMsg: "deadline", lookedUp: true},
		{name: "ate-api-server down", jwt: good, lookup: &fakeLookup{err: status.Error(codes.Unavailable, "connection refused")}, want: codes.Unavailable, wantMsg: "while looking up the actor: rpc error: code = Unavailable desc = connection refused", lookedUp: true},
		{name: "lookup fails without a status", jwt: good, lookup: &fakeLookup{err: errors.New("dial failed")}, want: codes.Unavailable, wantMsg: "while looking up the actor: dial failed", lookedUp: true},
		{name: "resumed on another node", jwt: good, lookup: &fakeLookup{actor: actorIn(ateapi.ActorState_ACTOR_STATE_RUNNING, "node-2")}, want: codes.PermissionDenied, wantMsg: "actor is no longer on the token's node", lookedUp: true},
		{name: "detached from the node", jwt: good, lookup: &fakeLookup{actor: actorIn(ateapi.ActorState_ACTOR_STATE_SUSPENDED, "")}, want: codes.PermissionDenied, wantMsg: "actor is no longer on the token's node", lookedUp: true},
		{name: "crashed on the token's node", jwt: good, lookup: &fakeLookup{actor: actorIn(ateapi.ActorState_ACTOR_STATE_CRASHED, "node-1")}, want: codes.PermissionDenied, wantMsg: "actor is ACTOR_STATE_CRASHED, no transfer is due", lookedUp: true},
		{name: "deleting on the token's node", jwt: good, lookup: &fakeLookup{actor: actorIn(ateapi.ActorState_ACTOR_STATE_DELETING, "node-1")}, want: codes.PermissionDenied, wantMsg: "actor is ACTOR_STATE_DELETING, no transfer is due", lookedUp: true},
		{name: "reverting on the token's node", jwt: good, lookup: &fakeLookup{actor: actorIn(ateapi.ActorState_ACTOR_STATE_REVERTING, "node-1")}, want: codes.PermissionDenied, wantMsg: "actor is ACTOR_STATE_REVERTING, no transfer is due", lookedUp: true},
		{name: "snapshot URI actorscope cannot parse", jwt: good, lookup: &fakeLookup{actor: actorWithBadURI()}, want: codes.Internal, wantMsg: "while deriving the actor's storage grants: " + errBadURI.Error(), lookedUp: true},
		{name: "no snapshot location", jwt: good, lookup: &fakeLookup{actor: actorLocalOnly()}, want: codes.PermissionDenied, wantMsg: "actor has no snapshot location to grant", lookedUp: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			verifier := tc.verifier
			if verifier == nil {
				verifier = v
			}
			lookup := tc.lookup
			if lookup == nil {
				lookup = &fakeLookup{actor: actorWithTag()}
			}
			a := New(verifier, lookup)
			a.now = func() time.Time { return s.now }
			got, err := a.Authorize(t.Context(), tc.jwt)
			if got != nil {
				t.Errorf("decision = %+v, want nil", got)
			}
			if st := status.Convert(err); st.Code() != tc.want || st.Message() != tc.wantMsg {
				t.Errorf("error = %v %q, want %v %q", st.Code(), st.Message(), tc.want, tc.wantMsg)
			}
			if wantCalls := map[bool]int{true: 1, false: 0}[tc.lookedUp]; lookup.calls != wantCalls {
				t.Errorf("lookup was called %d times, want %d", lookup.calls, wantCalls)
			}
		})
	}
}
