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
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/actoridjwt"
	"go.ate.dev/substrate/internal/localjwtauthority"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorlookup"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorscope"
	"go.ate.dev/substrate/internal/objectstoreplugin/credential"
	"go.ate.dev/substrate/internal/objectstoreplugin/gcsissuer"
	"go.ate.dev/substrate/internal/oidcjwt"
	exchangepb "go.ate.dev/substrate/internal/proto/objectstoreexchangepb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

const (
	issuer   = "https://idp.ate-system.svc"
	atespace = "team-a"
	name     = "bot"
	uid      = "9f1e0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a"
	tagURI   = "gs://snaps/root/atespaces/ate-golden/tags/7a3c0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a"
	ownURI   = "gs://snaps/root/atespaces/" + atespace + "/actors/" + uid + "/snapshots/2"
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
	v := oidcjwt.NewVerifier(issuer, []string{ActorJWTAudience}, oidcjwt.FileKeySource(path), oidcjwt.WithSubstrateClaims())
	return &signer{pool: pool, now: time.Now(), node: "node-1"}, v
}

func (s *signer) jwt(t *testing.T, kind actoridjwt.Kind, audience string, actorUID string) string {
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
		Audiences:  []string{audience},
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

// fakeLookup answers for one actor and records the claims it was asked for.
type fakeLookup struct {
	actor *ateapipb.Actor
	err   error
}

func (f *fakeLookup) Actor(_ context.Context, claims *actoridjwt.WireSubstrateClaims) (*ateapipb.Actor, error) {
	if f.err != nil {
		return nil, f.err
	}
	if claims.ActorUID != f.actor.GetMetadata().GetUid() {
		return nil, actorlookup.ErrStale
	}
	return f.actor, nil
}

// fakeIssuer records what it was asked to mint.
type fakeIssuer struct {
	err    error
	calls  int
	uid    string
	node   string
	grants []actorscope.Grant
	force  bool
	expiry time.Time
}

func (f *fakeIssuer) Issue(_ context.Context, actorUID, node string, grants []actorscope.Grant, force bool) (credential.Token, error) {
	f.calls++
	f.uid, f.node, f.grants, f.force = actorUID, node, grants, force
	if f.err != nil {
		return credential.Token{}, f.err
	}
	return credential.Token{AccessToken: "downscoped", Expiry: f.expiry}, nil
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
		"iss": issuer, "sub": "worker-for-actor/" + atespace + "/" + name, "aud": ActorJWTAudience,
		"exp": s.now.Add(time.Hour).Unix(), "nbf": s.now.Add(-time.Minute).Unix(), "iat": s.now.Unix(),
		"worker-for-actor.ate.dev": substrate,
	}
}

// actorWithTag is a running actor on node-1 that still carries the tag
// generation it was created from.
func actorWithTag() *ateapipb.Actor {
	return &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: name, Uid: uid},
		Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING, AssignedNode: "node-1", Snapshots: []*ateapipb.Snapshot{{
			Generation: 1, Owner: ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG,
			DurableSnapshot: &ateapipb.SnapshotStorage{
				Status: ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED,
				Object: &ateapipb.ObjectSnapshot{SnapshotUri: tagURI},
			},
		}}},
	}
}

// actorLocalOnly is a paused actor whose only snapshot is on node-1.
func actorLocalOnly() *ateapipb.Actor {
	return &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: name, Uid: uid},
		Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, AssignedNode: "node-1",
			Snapshots: []*ateapipb.Snapshot{{Generation: 1, Owner: ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, Locality: "node-1"}}},
	}
}

// actorIn returns actorWithTag in the given state on the given node.
func actorIn(state ateapipb.ActorState, node string) *ateapipb.Actor {
	a := actorWithTag()
	a.Status.State, a.Status.AssignedNode = state, node
	return a
}

// TestExchangeActorJwt_NodeBinding pins which actor states and placements a
// token minted on node-1 is good for.
func TestExchangeActorJwt_NodeBinding(t *testing.T) {
	s, v := newSigner(t)
	for _, tc := range []struct {
		name  string
		actor *ateapipb.Actor
		want  codes.Code
	}{
		{"resuming on the token's node", actorIn(ateapipb.ActorState_ACTOR_STATE_RESUMING, "node-1"), codes.OK},
		{"running on the token's node", actorIn(ateapipb.ActorState_ACTOR_STATE_RUNNING, "node-1"), codes.OK},
		{"suspending on the token's node", actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDING, "node-1"), codes.OK},
		{"suspended with its snapshot on the token's node", actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "node-1"), codes.OK},
		{"resumed on another node", actorIn(ateapipb.ActorState_ACTOR_STATE_RUNNING, "node-2"), codes.PermissionDenied},
		{"detached from the node", actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ""), codes.PermissionDenied},
		{"crashed on the token's node", actorIn(ateapipb.ActorState_ACTOR_STATE_CRASHED, "node-1"), codes.PermissionDenied},
		{"deleting on the token's node", actorIn(ateapipb.ActorState_ACTOR_STATE_DELETING, "node-1"), codes.PermissionDenied},
		{"reverting on the token's node", actorIn(ateapipb.ActorState_ACTOR_STATE_REVERTING, "node-1"), codes.PermissionDenied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			issuer := &fakeIssuer{expiry: s.now.Add(time.Hour)}
			srv := New(v, &fakeLookup{actor: tc.actor}, issuer)
			srv.now = func() time.Time { return s.now }
			_, err := srv.ExchangeActorJwt(t.Context(), &exchangepb.ExchangeActorJwtRequest{ActorJwt: s.jwt(t, actoridjwt.KindWorkerForActor, ActorJWTAudience, uid)})
			if status.Code(err) != tc.want {
				t.Fatalf("code = %v (%v), want %v", status.Code(err), err, tc.want)
			}
			if wantCalls := map[bool]int{true: 1, false: 0}[tc.want == codes.OK]; issuer.calls != wantCalls {
				t.Errorf("issuer was called %d times, want %d", issuer.calls, wantCalls)
			}
		})
	}
	t.Run("token without a node", func(t *testing.T) {
		// Refused before the lookup: a lookup that would answer NotFound must
		// not be reached.
		s.node = ""
		defer func() { s.node = "node-1" }()
		srv := New(v, &fakeLookup{err: status.Error(codes.NotFound, "no such actor")}, &fakeIssuer{})
		srv.now = func() time.Time { return s.now }
		_, err := srv.ExchangeActorJwt(t.Context(), &exchangepb.ExchangeActorJwtRequest{ActorJwt: s.jwt(t, actoridjwt.KindWorkerForActor, ActorJWTAudience, uid)})
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("code = %v (%v), want Unauthenticated", status.Code(err), err)
		}
	})
}

func TestExchangeActorJwt(t *testing.T) {
	s, v := newSigner(t)
	expiry := s.now.Add(50 * time.Minute).Truncate(time.Second)
	issuerOK := &fakeIssuer{expiry: expiry}
	srv := New(v, &fakeLookup{actor: actorWithTag()}, issuerOK)
	srv.now = func() time.Time { return s.now }

	for _, force := range []bool{false, true} {
		resp, err := srv.ExchangeActorJwt(t.Context(), &exchangepb.ExchangeActorJwtRequest{ActorJwt: s.jwt(t, actoridjwt.KindWorkerForActor, ActorJWTAudience, uid), Force: force})
		if err != nil {
			t.Fatalf("force=%v: %v", force, err)
		}
		if resp.GetAccessToken() != "downscoped" || !resp.GetExpireTime().AsTime().Equal(expiry) {
			t.Errorf("force=%v: response = %v", force, resp)
		}
		if issuerOK.uid != uid || issuerOK.node != "node-1" || issuerOK.force != force {
			t.Errorf("force=%v: issuer asked for uid=%s node=%s force=%v", force, issuerOK.uid, issuerOK.node, issuerOK.force)
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

// fakeSTS answers every STS exchange with a new access token, so two mints
// never return the same string.
type fakeSTS struct{ calls int }

func (f *fakeSTS) RoundTrip(*http.Request) (*http.Response, error) {
	f.calls++
	body := fmt.Sprintf(`{"access_token":"downscoped-%d","issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":"Bearer"}`, f.calls)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}, nil
}

// TestExchangeActorJwt_GCSTokenStaysOnItsNode runs the exchange against the
// real GCS issuer and moves the actor between nodes: a token minted for one
// node must never be handed to another.
func TestExchangeActorJwt_GCSTokenStaysOnItsNode(t *testing.T) {
	s, v := newSigner(t)
	sts := &fakeSTS{}
	src := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "source", Expiry: s.now.Add(time.Hour)})
	issuer, err := gcsissuer.New(t.Context(), gcsissuer.WithTokenSource(src),
		gcsissuer.WithHTTPClient(&http.Client{Transport: sts}), gcsissuer.WithClock(func() time.Time { return s.now }))
	if err != nil {
		t.Fatal(err)
	}
	lookup := &fakeLookup{}
	srv := New(v, lookup, issuer)
	srv.now = func() time.Time { return s.now }
	exchangeOn := func(node string) (string, error) {
		t.Helper()
		s.node = node
		resp, err := srv.ExchangeActorJwt(t.Context(), &exchangepb.ExchangeActorJwtRequest{ActorJwt: s.jwt(t, actoridjwt.KindWorkerForActor, ActorJWTAudience, uid)})
		return resp.GetAccessToken(), err
	}

	lookup.actor = actorIn(ateapipb.ActorState_ACTOR_STATE_RUNNING, "node-1")
	first, err := exchangeOn("node-1")
	if err != nil {
		t.Fatal(err)
	}
	again, err := exchangeOn("node-1")
	if err != nil {
		t.Fatal(err)
	}
	if again != first || sts.calls != 1 {
		t.Fatalf("a second exchange on the same node got %q after %q with %d STS calls, want the cached token and 1 call", again, first, sts.calls)
	}

	// The actor suspends, which prunes its snapshots to the one it uploaded,
	// and resumes on node-2. Its grants shrink to its own prefix, which
	// node-1's cached token still covers.
	resumed := actorIn(ateapipb.ActorState_ACTOR_STATE_RUNNING, "node-2")
	resumed.Status.Snapshots = []*ateapipb.Snapshot{{
		Generation: 2, Owner: ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR,
		DurableSnapshot: &ateapipb.SnapshotStorage{
			Status: ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED,
			Object: &ateapipb.ObjectSnapshot{SnapshotUri: ownURI},
		},
	}}
	lookup.actor = resumed
	moved, err := exchangeOn("node-2")
	if err != nil {
		t.Fatal(err)
	}
	if moved == first {
		t.Fatalf("node-2 was handed node-1's token %q", first)
	}
	if sts.calls != 2 {
		t.Errorf("STS calls = %d, want a fresh mint for node-2", sts.calls)
	}
	if _, err := exchangeOn("node-1"); status.Code(err) != codes.PermissionDenied {
		t.Errorf("node-1 after the move: code = %v (%v), want PermissionDenied", status.Code(err), err)
	}

	// An actor's own token is still refused before the issuer is reached.
	s.node = "node-2"
	_, err = srv.ExchangeActorJwt(t.Context(), &exchangepb.ExchangeActorJwtRequest{ActorJwt: s.jwt(t, actoridjwt.KindActor, ActorJWTAudience, uid)})
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("actor's own token: code = %v (%v), want PermissionDenied", status.Code(err), err)
	}
	if sts.calls != 2 {
		t.Errorf("STS calls = %d after an actor's own token, want 2", sts.calls)
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
	s, v := newSigner(t)
	stranger, _ := newSigner(t)
	good := s.jwt(t, actoridjwt.KindWorkerForActor, ActorJWTAudience, uid)
	noClaim := s.rawClaims()
	delete(noClaim, "worker-for-actor.ate.dev")
	bothClaims := s.rawClaims()
	bothClaims["actor.ate.dev"] = bothClaims["worker-for-actor.ate.dev"]
	noUID := s.rawClaims()
	noUID["worker-for-actor.ate.dev"] = map[string]string{"atespace": atespace, "actorName": name}
	for _, tc := range []struct {
		name   string
		jwt    string
		lookup *fakeLookup
		issuer *fakeIssuer
		want   codes.Code
	}{
		{"empty jwt", "", &fakeLookup{actor: actorWithTag()}, &fakeIssuer{}, codes.InvalidArgument},
		{"bad signature", stranger.jwt(t, actoridjwt.KindWorkerForActor, ActorJWTAudience, uid), &fakeLookup{actor: actorWithTag()}, &fakeIssuer{}, codes.Unauthenticated},
		{"wrong audience", s.jwt(t, actoridjwt.KindWorkerForActor, "other", uid), &fakeLookup{actor: actorWithTag()}, &fakeIssuer{}, codes.Unauthenticated},
		{"actor's own token", s.jwt(t, actoridjwt.KindActor, ActorJWTAudience, uid), &fakeLookup{actor: actorWithTag()}, &fakeIssuer{}, codes.PermissionDenied},
		{"no substrate claim", s.rawJWT(t, noClaim), &fakeLookup{actor: actorWithTag()}, &fakeIssuer{}, codes.PermissionDenied},
		{"both substrate claims", s.rawJWT(t, bothClaims), &fakeLookup{actor: actorWithTag()}, &fakeIssuer{}, codes.Unauthenticated},
		{"claim without the actor uid", s.rawJWT(t, noUID), &fakeLookup{actor: actorWithTag()}, &fakeIssuer{}, codes.Unauthenticated},
		{"ate-api-server rejects the actor ref", good, &fakeLookup{err: status.Error(codes.InvalidArgument, "bad name")}, &fakeIssuer{}, codes.Unauthenticated},
		{"caller gave up during the lookup", good, &fakeLookup{err: status.Error(codes.Canceled, "canceled")}, &fakeIssuer{}, codes.Canceled},
		{"caller's deadline passed during the lookup", good, &fakeLookup{err: status.Error(codes.DeadlineExceeded, "deadline")}, &fakeIssuer{}, codes.DeadlineExceeded},
		{"recreated actor", s.jwt(t, actoridjwt.KindWorkerForActor, ActorJWTAudience, "old-uid"), &fakeLookup{actor: actorWithTag()}, &fakeIssuer{}, codes.NotFound},
		{"actor gone", good, &fakeLookup{err: status.Error(codes.NotFound, "no such actor")}, &fakeIssuer{}, codes.NotFound},
		{"ate-api-server down", good, &fakeLookup{err: status.Error(codes.Unavailable, "connection refused")}, &fakeIssuer{}, codes.Unavailable},
		{"exchange lacks the actor read role", good, &fakeLookup{err: status.Error(codes.PermissionDenied, "not allowed")}, &fakeIssuer{}, codes.FailedPrecondition},
		{"no snapshot location", good, &fakeLookup{actor: actorLocalOnly()}, &fakeIssuer{}, codes.PermissionDenied},
		{"issuer failing", good, &fakeLookup{actor: actorWithTag()}, &fakeIssuer{err: errors.New("STS 503")}, codes.Unavailable},
		{"issuer cannot express the grants", good, &fakeLookup{actor: actorWithTag()}, &fakeIssuer{err: status.Error(codes.Internal, "bad boundary")}, codes.Internal},
		{"caller gave up at STS", good, &fakeLookup{actor: actorWithTag()}, &fakeIssuer{err: fmt.Errorf("Post sts: %w", context.Canceled)}, codes.Canceled},
		{"caller's deadline passed at STS", good, &fakeLookup{actor: actorWithTag()}, &fakeIssuer{err: fmt.Errorf("Post sts: %w", context.DeadlineExceeded)}, codes.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := New(v, tc.lookup, tc.issuer)
			srv.now = func() time.Time { return s.now }
			_, err := srv.ExchangeActorJwt(t.Context(), &exchangepb.ExchangeActorJwtRequest{ActorJwt: tc.jwt})
			if status.Code(err) != tc.want {
				t.Fatalf("code = %v (%v), want %v", status.Code(err), err, tc.want)
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
