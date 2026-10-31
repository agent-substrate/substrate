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

package actorauthz_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/actoridjwt"
	"go.ate.dev/substrate/internal/localca"
	"go.ate.dev/substrate/internal/localjwtauthority"
	"go.ate.dev/substrate/internal/objectstore"
	internalauthz "go.ate.dev/substrate/internal/objectstoreplugin/actorauthz"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorscope"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/testcert"
	"go.ate.dev/substrate/pkg/objectstore/actorauthz"
)

const (
	issuer     = "https://idp.ate-system.svc"
	serverName = "api.ate-system.svc"
	atespace   = "team-a"
	name       = "bot"
	uid        = "9f1e0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a"
	location   = "gs://snaps/root"
	tagURI     = location + "/atespaces/ate-golden/tags/7a3c0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a"
)

// env is what every Authorizer in a test shares: the JWT keys and the PKI the
// fake ate-api-server and its client use.
type env struct {
	pool     *localjwtauthority.ConcretePool
	jwksFile string
	ca       *localca.CA
	caFile   string
	bundle   string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dir := t.TempDir()
	auth, err := localjwtauthority.GenerateAuthority("ES256", "k1")
	if err != nil {
		t.Fatal(err)
	}
	pool := &localjwtauthority.ConcretePool{Authorities: []*localjwtauthority.Authority{auth}, ActiveForSigning: "k1"}
	jwks, err := pool.JWKS()
	if err != nil {
		t.Fatal(err)
	}
	e := &env{pool: pool, jwksFile: writeFile(t, dir, "jwks.json", jwks)}
	e.ca, err = localca.GenerateCA("test", localca.KeyTypeECDSAP256, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	e.caFile = writeFile(t, dir, "ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: e.ca.RootCertificate.Raw}))
	e.bundle = writeFile(t, dir, "client.pem", clientBundle(t, e.ca))
	return e
}

func writeFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// clientBundle issues the pod identity certificate the Authorizer presents.
func clientBundle(t *testing.T, ca *localca.CA) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	spiffeID, err := url.Parse("spiffe://ate.dev/ns/ate-system/sa/ate-api-server")
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(2),
		URIs:         []*url.URL{spiffeID},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, ca.RootCertificate, key.Public(), ca.SigningKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return append(
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...,
	)
}

// fakeControl answers GetActor for the one actor it holds, or with err.
type fakeControl struct {
	ateapi.UnimplementedControlServer
	actor *ateapi.Actor
	err   error
}

func (f *fakeControl) GetActor(_ context.Context, req *ateapi.GetActorRequest) (*ateapi.Actor, error) {
	if f.err != nil {
		return nil, f.err
	}
	if req.GetActor().GetAtespace() != f.actor.GetMetadata().GetAtespace() || req.GetActor().GetName() != f.actor.GetMetadata().GetName() {
		return nil, status.Errorf(codes.NotFound, "no actor %s/%s", req.GetActor().GetAtespace(), req.GetActor().GetName())
	}
	return f.actor, nil
}

// serve runs fake as ate-api-server over mTLS on the loopback, and returns
// the Config of an Authorizer that reaches it.
func (e *env) serve(t *testing.T, fake *fakeControl) actorauthz.Config {
	t.Helper()
	material := testcert.ServerTLSWithCA(t, e.ca, net.ParseIP("127.0.0.1"), serverName)
	cert, err := tls.X509KeyPair(material.Certificate, material.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	clientCAs := x509.NewCertPool()
	clientCAs.AddCert(e.ca.RootCertificate)
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	})))
	ateapi.RegisterControlServer(srv, fake)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return actorauthz.Config{
		Issuer:   issuer,
		JWKSFile: e.jwksFile,
		APIServer: actorauthz.APIServerConfig{
			Address:          lis.Addr().String(),
			CAFile:           e.caFile,
			ServerName:       serverName,
			ClientCredBundle: e.bundle,
		},
	}
}

// token is what jwt mints. The zero value is a worker-for-actor token for
// the actor uid on node-1, for the objectstore plugin audience.
type token struct {
	kind     actoridjwt.Kind
	audience string
	uid      string
}

func (e *env) jwt(t *testing.T, tok token) string {
	t.Helper()
	if tok.kind == actoridjwt.KindUnspecified {
		tok.kind = actoridjwt.KindWorkerForActor
	}
	if tok.audience == "" {
		tok.audience = actorauthz.Audience
	}
	if tok.uid == "" {
		tok.uid = uid
	}
	substrate := actoridjwt.SubstrateClaims{Atespace: atespace, ActorName: name, ActorUID: tok.uid}
	if tok.kind == actoridjwt.KindWorkerForActor {
		substrate.Node = "node-1"
	}
	now := time.Now()
	jwt, err := e.pool.SignJWT(&actoridjwt.Claims{
		Kind:       tok.kind,
		Issuer:     issuer,
		Subject:    tok.kind.SubjectPrefix() + "/" + atespace + "/" + name,
		Audiences:  []string{tok.audience},
		Expiration: now.Add(time.Hour),
		NotBefore:  now.Add(-time.Minute),
		IssuedAt:   now,
		Substrate:  substrate,
	})
	if err != nil {
		t.Fatal(err)
	}
	return jwt
}

// actor returns a RUNNING actor on node-1 with the given UID and snapshots.
func actor(actorUID string, snapshots ...*ateapi.Snapshot) *ateapi.Actor {
	return &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Atespace: atespace, Name: name, Uid: actorUID},
		Status:   &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_RUNNING, AssignedNode: "node-1", Snapshots: snapshots},
	}
}

func durable(gen int32, owner ateapi.SnapshotOwner, st ateapi.SnapshotStorageState, uri string) *ateapi.Snapshot {
	return &ateapi.Snapshot{Generation: gen, Owner: owner, DurableSnapshot: &ateapi.SnapshotStorage{State: st, Object: &ateapi.ObjectSnapshot{SnapshotUri: uri}}}
}

var (
	ownerActor = ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR
	ownerTag   = ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG
	completed  = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED
	pending    = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING
)

func actorPrefix(actorUID string) string {
	return actorPrefixAt(location, actorUID)
}

func actorPrefixAt(loc, actorUID string) string {
	return loc + "/atespaces/" + atespace + "/actors/" + actorUID
}

func TestAuthorize(t *testing.T) {
	e := newEnv(t)
	for _, tc := range []struct {
		name  string
		actor *ateapi.Actor
		jwt   token
		want  *actorauthz.Decision
		// inside and outside are object URIs a string prefix match on the
		// grants must and must not accept.
		inside, outside []string
	}{
		{
			name:  "actor created from a tag",
			actor: actor(uid, durable(1, ownerTag, completed, tagURI)),
			want: &actorauthz.Decision{
				ActorUID: uid,
				Grants: []actorauthz.Grant{
					{Prefix: actorPrefix(uid) + "/", Access: actorauthz.ReadWrite},
					{Prefix: tagURI + "/", Access: actorauthz.ReadOnly},
				},
			},
			inside: []string{actorPrefix(uid) + "/snapshots/s1/manifest", tagURI + "/manifest"},
		},
		{
			name:  "UID that begins another actor's UID",
			actor: actor("abc", durable(2, ownerActor, pending, actorPrefix("abc")+"/snapshots/s1")),
			jwt:   token{uid: "abc"},
			want: &actorauthz.Decision{
				ActorUID: "abc",
				Grants:   []actorauthz.Grant{{Prefix: actorPrefix("abc") + "/", Access: actorauthz.ReadWrite}},
			},
			inside:  []string{actorPrefix("abc") + "/snapshots/s1/manifest"},
			outside: []string{actorPrefix("abcd") + "/snapshots/s1/manifest", actorPrefix("abcd")},
		},
		{
			name:  "location with an escaped space",
			actor: actor(uid, durable(2, ownerActor, pending, actorPrefixAt("gs://snaps/my%20root", uid)+"/snapshots/s1")),
			want: &actorauthz.Decision{
				ActorUID: uid,
				Grants:   []actorauthz.Grant{{Prefix: actorPrefixAt("gs://snaps/my%20root", uid) + "/", Access: actorauthz.ReadWrite}},
			},
			inside: []string{actorPrefixAt("gs://snaps/my%20root", uid) + "/snapshots/s1/manifest"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, err := actorauthz.New(t.Context(), e.serve(t, &fakeControl{actor: tc.actor}))
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() {
				if err := a.Close(); err != nil {
					t.Errorf("Close: %v", err)
				}
			})
			got, err := a.Authorize(t.Context(), e.jwt(t, tc.jwt))
			if err != nil {
				t.Fatalf("Authorize: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("decision = %+v, want %+v", got, tc.want)
			}
			for _, g := range got.Grants {
				assertGCSBoundaryPrefix(t, g.Prefix)
			}
			for _, uri := range tc.inside {
				if !coveredByString(got.Grants, uri) {
					t.Errorf("no grant prefix matches %s", uri)
				}
			}
			for _, uri := range tc.outside {
				if coveredByString(got.Grants, uri) {
					t.Errorf("a grant prefix matches %s", uri)
				}
			}
		})
	}
}

func coveredByString(grants []actorauthz.Grant, uri string) bool {
	for _, g := range grants {
		if strings.HasPrefix(uri, g.Prefix) {
			return true
		}
	}
	return false
}

// assertGCSBoundaryPrefix checks prefix parses to the bucket and object path
// the GCS boundary builds its condition from, through objectstore.BucketPrefix.
func assertGCSBoundaryPrefix(t *testing.T, prefix string) {
	t.Helper()
	sp, err := resources.ParseStoragePrefix(strings.TrimSuffix(prefix, "/"))
	if err != nil {
		t.Fatal(err)
	}
	bucket, path, err := objectstore.BucketPrefix(sp)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(prefix)
	if err != nil {
		t.Fatal(err)
	}
	if u.Host != bucket || strings.TrimPrefix(u.Path, "/") != path {
		t.Errorf("grant prefix %s parses to bucket %q path %q, want %q %q from objectstore.BucketPrefix", prefix, u.Host, u.Path, bucket, path)
	}
}

func TestAuthorize_Refusals(t *testing.T) {
	e := newEnv(t)
	// notValid is the message for a token the verifier refuses, which wraps
	// the verifier's own error.
	verifier, err := internalauthz.NewVerifier(t.Context(), issuer, e.jwksFile)
	if err != nil {
		t.Fatal(err)
	}
	notValid := func(jwt string) string {
		_, err := verifier.Verify(t.Context(), jwt, time.Now())
		if err == nil {
			t.Fatal("verifier accepted a token the row expects it to refuse")
		}
		return "actor_jwt is not valid: " + err.Error()
	}
	wrongAudience := e.jwt(t, token{audience: "other"})
	badURI := actor(uid, durable(1, ownerTag, completed, "gs://snaps/not-a-snapshot"))
	_, errBadURI := actorscope.FromActor(badURI)
	if errBadURI == nil {
		t.Fatal("actorscope accepted the bad snapshot URI")
	}
	tagged := actor(uid, durable(1, ownerTag, completed, tagURI))
	elsewhere := actor(uid, durable(1, ownerTag, completed, tagURI))
	elsewhere.Status.AssignedNode = "node-2"
	crashed := actor(uid, durable(1, ownerTag, completed, tagURI))
	crashed.Status.State = ateapi.ActorState_ACTOR_STATE_CRASHED
	for _, tc := range []struct {
		name     string
		jwt      string
		fake     *fakeControl
		canceled bool
		want     codes.Code
		wantMsg  string
	}{
		{name: "empty jwt", want: codes.InvalidArgument, wantMsg: "actor_jwt is required"},
		{name: "wrong audience", jwt: wrongAudience, want: codes.Unauthenticated, wantMsg: notValid(wrongAudience)},
		{name: "actor's own token", jwt: e.jwt(t, token{kind: actoridjwt.KindActor}), want: codes.PermissionDenied, wantMsg: "only a worker-for-actor token may be exchanged"},
		{name: "recreated actor", jwt: e.jwt(t, token{uid: "old-uid"}), want: codes.NotFound, wantMsg: "actor not found"},
		{name: "actor gone", fake: &fakeControl{err: status.Error(codes.NotFound, "no such actor")}, want: codes.NotFound, wantMsg: "actor not found"},
		{name: "ate-api-server rejects the actor ref", fake: &fakeControl{err: status.Error(codes.InvalidArgument, "bad name")}, want: codes.Unauthenticated, wantMsg: "actor_jwt names an invalid actor: rpc error: code = InvalidArgument desc = bad name"},
		{name: "exchange lacks the actor read role", fake: &fakeControl{err: status.Error(codes.PermissionDenied, "not allowed")}, want: codes.FailedPrecondition, wantMsg: "the exchange may not read actors: rpc error: code = PermissionDenied desc = not allowed"},
		{name: "caller's deadline passed during the lookup", fake: &fakeControl{err: status.Error(codes.DeadlineExceeded, "deadline")}, want: codes.DeadlineExceeded, wantMsg: "deadline"},
		{name: "caller canceled before the lookup", canceled: true, want: codes.Canceled, wantMsg: "context canceled"},
		{name: "ate-api-server fails", fake: &fakeControl{err: status.Error(codes.Internal, "boom")}, want: codes.Unavailable, wantMsg: "while looking up the actor: rpc error: code = Internal desc = boom"},
		{name: "crashed on the token's node", fake: &fakeControl{actor: crashed}, want: codes.PermissionDenied, wantMsg: "actor is ACTOR_STATE_CRASHED, no transfer is due"},
		{name: "resumed on another node", fake: &fakeControl{actor: elsewhere}, want: codes.PermissionDenied, wantMsg: "actor is no longer on the token's node"},
		{name: "snapshot URI actorscope cannot parse", fake: &fakeControl{actor: badURI}, want: codes.Internal, wantMsg: "while deriving the actor's storage grants: " + errBadURI.Error()},
		{name: "no snapshot location", fake: &fakeControl{actor: actor(uid)}, want: codes.PermissionDenied, wantMsg: "actor has no snapshot location to grant"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := tc.fake
			if fake == nil {
				fake = &fakeControl{actor: tagged}
			}
			jwt := tc.jwt
			if jwt == "" && tc.want != codes.InvalidArgument {
				jwt = e.jwt(t, token{})
			}
			a, err := actorauthz.New(t.Context(), e.serve(t, fake))
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer a.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			got, err := a.Authorize(ctx, jwt)
			if got != nil {
				t.Errorf("decision = %+v, want nil", got)
			}
			if st := status.Convert(err); st.Code() != tc.want || st.Message() != tc.wantMsg {
				t.Errorf("error = %v %q, want %v %q", st.Code(), st.Message(), tc.want, tc.wantMsg)
			}
		})
	}
}

func TestNew_Errors(t *testing.T) {
	e := newEnv(t)
	good := e.serve(t, &fakeControl{})
	dir := t.TempDir()
	notJSON := writeFile(t, dir, "not-json", []byte("not a key set"))
	for _, tc := range []struct {
		name    string
		edit    func(*actorauthz.Config)
		wantErr string
	}{
		{"no issuer", func(c *actorauthz.Config) { c.Issuer = "" }, "Issuer is required"},
		{"http issuer", func(c *actorauthz.Config) { c.Issuer = "http://idp.ate-system.svc" }, "must be https://<host>"},
		{"no JWKS file", func(c *actorauthz.Config) { c.JWKSFile = "" }, "JWKSFile is required"},
		{"missing JWKS file", func(c *actorauthz.Config) { c.JWKSFile = filepath.Join(dir, "absent.json") }, "while reading the actor JWT key set"},
		{"invalid JWKS file", func(c *actorauthz.Config) { c.JWKSFile = notJSON }, "while reading the actor JWT key set"},
		{"no ate-api-server address", func(c *actorauthz.Config) { c.APIServer.Address = "" }, "APIServer.Address is required"},
		{"no CA file", func(c *actorauthz.Config) { c.APIServer.CAFile = "" }, "APIServer.CAFile is required"},
		{"no ate-api-server name", func(c *actorauthz.Config) { c.APIServer.ServerName = "" }, "APIServer.ServerName is required"},
		{"no client credential bundle", func(c *actorauthz.Config) { c.APIServer.ClientCredBundle = "" }, "APIServer.ClientCredBundle is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := good
			tc.edit(&cfg)
			a, err := actorauthz.New(t.Context(), cfg)
			if err == nil {
				a.Close()
				t.Fatal("New succeeded")
			}
			if !strings.HasPrefix(err.Error(), "actorauthz: ") || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %v, want it to start with %q and contain %q", err, "actorauthz: ", tc.wantErr)
			}
		})
	}
}

// TestAudience pins Audience to the audience the in-tree verifier requires.
func TestAudience(t *testing.T) {
	if actorauthz.Audience != internalauthz.Audience {
		t.Errorf("Audience = %q, want %q", actorauthz.Audience, internalauthz.Audience)
	}
}

// TestAccess pins Access to actorscope's values and names, which Authorize
// converts from.
func TestAccess(t *testing.T) {
	for _, tc := range []struct {
		internal actorscope.Access
		public   actorauthz.Access
	}{
		{actorscope.ReadOnly, actorauthz.ReadOnly},
		{actorscope.ReadWrite, actorauthz.ReadWrite},
		{actorscope.Access(7), actorauthz.Access(7)},
	} {
		if actorauthz.Access(tc.internal) != tc.public || tc.public.String() != tc.internal.String() {
			t.Errorf("Access %d %q, want %d %q", tc.public, tc.public, tc.internal, tc.internal)
		}
	}
}
