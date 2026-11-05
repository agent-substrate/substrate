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
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
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
	"go.ate.dev/substrate/internal/objectstoreplugin/actorscope"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/testcert"
	"go.ate.dev/substrate/pkg/objectstore/actorauthz"
)

const (
	serverName = "api.ate-system.svc"
	atespace   = "team-a"
	name       = "bot"
	uid        = "9f1e0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a"
	location   = "gs://snaps/root"
	tagURI     = location + "/atespaces/ate-golden/tags/7a3c0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a"
)

// env is what every Authorizer in a test shares: the JWT keys, the issuer
// that serves them, and the PKI the fake ate-api-server and its client use.
type env struct {
	pool *localjwtauthority.ConcretePool
	// issuer is the URL of a TLS server that serves OIDC discovery and the
	// pool's JWKS, the way ate-idp-server does. issuerCAFile holds its
	// certificate.
	issuer       string
	issuerCAFile string
	ca           *localca.CA
	caFile       string
	bundle       string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dir := t.TempDir()
	auth, err := localjwtauthority.GenerateAuthority("ES256", "k1")
	if err != nil {
		t.Fatal(err)
	}
	pool := &localjwtauthority.ConcretePool{Authorities: []*localjwtauthority.Authority{auth}, ActiveForSigning: "k1"}
	srv := serveIssuer(t, pool, nil)
	e := &env{pool: pool, issuer: srv.URL, issuerCAFile: writeServerCA(t, srv)}
	e.ca, err = localca.GenerateCA("test", localca.KeyTypeECDSAP256, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	e.caFile = writeFile(t, dir, "ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: e.ca.RootCertificate.Raw}))
	e.bundle = writeFile(t, dir, "client.pem", clientBundle(t, e.ca))
	return e
}

// serveIssuer starts a TLS server that serves OIDC discovery and pool's JWKS.
// connState, if set, is the server's http.Server.ConnState.
func serveIssuer(t *testing.T, pool *localjwtauthority.ConcretePool, connState func(net.Conn, http.ConnState)) *httptest.Server {
	t.Helper()
	keys, err := pool.VerificationKeys()
	if err != nil {
		t.Fatal(err)
	}
	jwks, err := localjwtauthority.JWKS(keys)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	var srv *httptest.Server
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"issuer": srv.URL, "jwks_uri": srv.URL + "/jwks"})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(jwks) })
	srv = httptest.NewUnstartedServer(mux)
	srv.Config.ConnState = connState
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

// writeServerCA writes srv's certificate to a PEM file and returns its path.
func writeServerCA(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	return writeFile(t, t.TempDir(), "issuer-ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
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
		Issuer:       e.issuer,
		IssuerCAFile: e.issuerCAFile,
		APIServer: actorauthz.APIServerConfig{
			Address:          lis.Addr().String(),
			CAFile:           e.caFile,
			ServerName:       serverName,
			ClientCredBundle: e.bundle,
		},
	}
}

// token is what jwt mints. The zero value is a worker-for-actor token from
// the env's issuer for the actor uid on node-1, for the objectstore plugin
// audience.
type token struct {
	kind     actoridjwt.Kind
	issuer   string
	audience string
	uid      string
	// exp defaults to an hour from now.
	exp time.Time
}

func (e *env) jwt(t *testing.T, tok token) string {
	t.Helper()
	if tok.kind == actoridjwt.KindUnspecified {
		tok.kind = actoridjwt.KindWorkerForActor
	}
	if tok.issuer == "" {
		tok.issuer = e.issuer
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
	if tok.exp.IsZero() {
		tok.exp = now.Add(time.Hour)
	}
	jwt, err := e.pool.SignJWT(&actoridjwt.Claims{
		Kind:       tok.kind,
		Issuer:     tok.issuer,
		Subject:    tok.kind.SubjectPrefix() + "/" + atespace + "/" + name,
		Audiences:  []string{tok.audience},
		Expiration: tok.exp,
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
	// exp is whole seconds, as the JWT carries it.
	exp := time.Unix(time.Now().Add(time.Hour).Unix(), 0)
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
				Expiry: exp,
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
				Expiry:   exp,
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
				Expiry:   exp,
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
			tok := tc.jwt
			tok.exp = exp
			got, err := a.Authorize(t.Context(), e.jwt(t, tok))
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
	// dead is an issuer that has stopped, so its keys cannot be fetched.
	dead := serveIssuer(t, e.pool, nil)
	dead.Close()
	badURI := actor(uid, durable(1, ownerTag, completed, "gs://snaps/not-a-snapshot"))
	if _, err := actorscope.FromActor(badURI); err == nil {
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
		edit     func(*actorauthz.Config)
		canceled bool
		want     codes.Code
	}{
		{name: "empty jwt", want: codes.InvalidArgument},
		{name: "jwt over MaxJWTSize", jwt: strings.Repeat("a", actorauthz.MaxJWTSize+1), want: codes.InvalidArgument},
		{name: "garbage jwt of MaxJWTSize", jwt: strings.Repeat("a", actorauthz.MaxJWTSize), want: codes.Unauthenticated},
		{name: "wrong audience", jwt: e.jwt(t, token{audience: "other"}), want: codes.Unauthenticated},
		{name: "issuer unreachable", jwt: e.jwt(t, token{issuer: dead.URL}), edit: func(c *actorauthz.Config) { c.Issuer = dead.URL }, want: codes.Unavailable},
		{name: "actor's own token", jwt: e.jwt(t, token{kind: actoridjwt.KindActor}), want: codes.PermissionDenied},
		{name: "recreated actor", jwt: e.jwt(t, token{uid: "old-uid"}), want: codes.NotFound},
		{name: "actor gone", fake: &fakeControl{err: status.Error(codes.NotFound, "no such actor")}, want: codes.NotFound},
		{name: "ate-api-server rejects the actor ref", fake: &fakeControl{err: status.Error(codes.InvalidArgument, "bad name")}, want: codes.Unauthenticated},
		{name: "exchange lacks the actor read role", fake: &fakeControl{err: status.Error(codes.PermissionDenied, "not allowed")}, want: codes.FailedPrecondition},
		{name: "caller's deadline passed during the lookup", fake: &fakeControl{err: status.Error(codes.DeadlineExceeded, "deadline")}, want: codes.DeadlineExceeded},
		{name: "caller canceled before the lookup", canceled: true, want: codes.Canceled},
		{name: "ate-api-server fails", fake: &fakeControl{err: status.Error(codes.Internal, "boom")}, want: codes.Unavailable},
		{name: "crashed on the token's node", fake: &fakeControl{actor: crashed}, want: codes.PermissionDenied},
		{name: "resumed on another node", fake: &fakeControl{actor: elsewhere}, want: codes.PermissionDenied},
		{name: "snapshot URI actorscope cannot parse", fake: &fakeControl{actor: badURI}, want: codes.Internal},
		{name: "no snapshot location", fake: &fakeControl{actor: actor(uid)}, want: codes.PermissionDenied},
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
			cfg := e.serve(t, fake)
			if tc.edit != nil {
				tc.edit(&cfg)
			}
			a, err := actorauthz.New(t.Context(), cfg)
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
			if got := status.Code(err); got != tc.want {
				t.Errorf("error = %v, want code %v", err, tc.want)
			}
		})
	}
}

func TestNew_Errors(t *testing.T) {
	e := newEnv(t)
	good := e.serve(t, &fakeControl{})
	dir := t.TempDir()
	absent := filepath.Join(dir, "absent.pem")
	noCerts := writeFile(t, dir, "no-certs.pem", []byte("not a certificate\n"))
	for _, tc := range []struct {
		name    string
		edit    func(*actorauthz.Config)
		wantErr string
	}{
		{"no issuer", func(c *actorauthz.Config) { c.Issuer = "" }, "Issuer is required"},
		{"http issuer", func(c *actorauthz.Config) { c.Issuer = "http://idp.ate-system.svc" }, "must be https://<host>"},
		{"no issuer CA file", func(c *actorauthz.Config) { c.IssuerCAFile = "" }, "IssuerCAFile is required"},
		{"missing issuer CA file", func(c *actorauthz.Config) { c.IssuerCAFile = absent }, absent},
		{"issuer CA file without certificates", func(c *actorauthz.Config) { c.IssuerCAFile = noCerts }, "contains no certificates"},
		// e.caFile is a valid CA that did not sign the issuer's certificate.
		{"issuer certificate not trusted", func(c *actorauthz.Config) { c.IssuerCAFile = e.caFile }, "does not verify against the system roots or " + e.caFile},
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

// TestClose checks the Authorizer leaves no connection open, after Close or
// a New that fails once the issuer's keys are fetched.
func TestClose(t *testing.T) {
	e := newEnv(t)
	for _, tc := range []struct {
		name string
		// edit makes New fail after the key fetch.
		edit func(*actorauthz.Config)
	}{
		{name: "Close"},
		{name: "New fails after the key fetch", edit: func(c *actorauthz.Config) { c.APIServer.CAFile = filepath.Join(t.TempDir(), "absent.pem") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conns := &connCounter{closedCh: make(chan struct{}, 1)}
			issuer := serveIssuer(t, e.pool, conns.connState)
			cfg := e.serve(t, &fakeControl{actor: actor(uid, durable(1, ownerTag, completed, tagURI))})
			cfg.Issuer, cfg.IssuerCAFile = issuer.URL, writeServerCA(t, issuer)
			if tc.edit != nil {
				tc.edit(&cfg)
				a, err := actorauthz.New(t.Context(), cfg)
				if err == nil {
					a.Close()
					t.Fatal("New succeeded")
				}
				if !strings.Contains(err.Error(), "absent.pem") {
					t.Fatalf("New: %v, want it to name absent.pem", err)
				}
				conns.waitAllClosed(t)
				return
			}
			a, err := actorauthz.New(t.Context(), cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			jwt := e.jwt(t, token{issuer: issuer.URL})
			if _, err := a.Authorize(t.Context(), jwt); err != nil {
				t.Fatalf("Authorize: %v", err)
			}
			if err := a.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			conns.waitAllClosed(t)
			// The keys are cached, so this reaches the closed lookup.
			if _, err := a.Authorize(t.Context(), jwt); status.Code(err) != codes.Canceled {
				t.Errorf("Authorize after Close: %v, want code %v", err, codes.Canceled)
			}
		})
	}
}

// connCounter counts the connections a server opens and closes.
type connCounter struct {
	opened, closed atomic.Int32
	closedCh       chan struct{}
}

func (c *connCounter) connState(_ net.Conn, s http.ConnState) {
	switch s {
	case http.StateNew:
		c.opened.Add(1)
	case http.StateClosed, http.StateHijacked:
		c.closed.Add(1)
		select {
		case c.closedCh <- struct{}{}:
		default:
		}
	}
}

// waitAllClosed waits until at least one connection was opened and every
// opened one is closed.
func (c *connCounter) waitAllClosed(t *testing.T) {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for c.opened.Load() == 0 || c.closed.Load() < c.opened.Load() {
		select {
		case <-c.closedCh:
		case <-timeout:
			t.Fatalf("%d of %d issuer connections closed", c.closed.Load(), c.opened.Load())
		}
	}
}
