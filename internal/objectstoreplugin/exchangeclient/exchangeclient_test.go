// Copyright 2026 The Agent Substrate Authors
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

package exchangeclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/utils/lru"

	"go.ate.dev/substrate/internal/objectstoreplugin/credential"
	exchangepb "go.ate.dev/substrate/internal/proto/objectstoreexchangepb"
	"go.ate.dev/substrate/internal/testcert"
	"go.ate.dev/substrate/pkg/objectstorage"
)

// fakeExchange answers ExchangeActorJwt with one token and records the calls.
type fakeExchange struct {
	exchangepb.CredentialExchangeClient
	token  string
	expiry time.Time
	err    error
	calls  []*exchangepb.ExchangeActorJwtRequest
}

func (f *fakeExchange) ExchangeActorJwt(_ context.Context, in *exchangepb.ExchangeActorJwtRequest, _ ...grpc.CallOption) (*exchangepb.ExchangeActorJwtResponse, error) {
	f.calls = append(f.calls, in)
	if f.err != nil {
		return nil, f.err
	}
	return &exchangepb.ExchangeActorJwtResponse{AccessToken: f.token, ExpireTime: timestamppb.New(f.expiry)}, nil
}

// fakeClient is the storage client built for a token.
type fakeClient struct {
	objectstorage.ObjectStorage
	token string
}

func newTestSource(exchange *fakeExchange, now time.Time) (*Source, *int) {
	built := 0
	s := newSource(exchange, func(_ context.Context, tok credential.Token) (objectstorage.ObjectStorage, error) {
		built++
		return &fakeClient{token: tok.AccessToken}, nil
	})
	s.now = func() time.Time { return now }
	return s, &built
}

func tokenOf(client objectstorage.ObjectStorage) string { return client.(*fakeClient).token }

func TestClient_CachesTheTokenAndClientPerJWTUntilNearExpiry(t *testing.T) {
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	exchange := &fakeExchange{token: "tok-1", expiry: now.Add(time.Hour)}
	s, built := newTestSource(exchange, now)

	first, err := s.Client(t.Context(), "jwt-a", false)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.Client(t.Context(), "jwt-a", false)
	if err != nil {
		t.Fatal(err)
	}
	if again != first || tokenOf(first) != "tok-1" {
		t.Fatalf("second transfer got %v, want the same client on tok-1", again)
	}
	if len(exchange.calls) != 1 || exchange.calls[0].GetForce() || exchange.calls[0].GetActorJwt() != "jwt-a" {
		t.Fatalf("exchange calls = %d (%v), want one unforced with the JWT", len(exchange.calls), exchange.calls)
	}
	if *built != 1 {
		t.Fatalf("built %d clients, want 1 shared by both transfers", *built)
	}

	// Another JWT is a miss, even one naming the same actor: the key is the
	// JWT the exchange verified, never a claim read out of it.
	if _, err := s.Client(t.Context(), "jwt-b-same-actor", false); err != nil {
		t.Fatal(err)
	}
	if len(exchange.calls) != 2 {
		t.Fatalf("a JWT the exchange has not seen must exchange, calls = %d", len(exchange.calls))
	}

	// Under the floor the cache is not served and the entry is replaced.
	s.now = func() time.Time { return now.Add(time.Hour - credential.MinServeLifetime + time.Second) }
	exchange.token, exchange.expiry = "tok-2", now.Add(2*time.Hour)
	client, err := s.Client(t.Context(), "jwt-a", false)
	if err != nil || tokenOf(client) != "tok-2" {
		t.Fatalf("near expiry client = %v, %v, want tok-2 from a fresh exchange", client, err)
	}
	if client, err := s.Client(t.Context(), "jwt-a", false); err != nil || tokenOf(client) != "tok-2" || len(exchange.calls) != 3 {
		t.Fatalf("after the refresh: client %v, err %v, calls %d, want tok-2 from the cache", client, err, len(exchange.calls))
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
}

func TestClient_ForceSkipsBothCaches(t *testing.T) {
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	exchange := &fakeExchange{token: "tok-1", expiry: now.Add(time.Hour)}
	s, built := newTestSource(exchange, now)
	if _, err := s.Client(t.Context(), "jwt", false); err != nil {
		t.Fatal(err)
	}
	exchange.token = "tok-2"
	client, err := s.Client(t.Context(), "jwt", true)
	if err != nil || tokenOf(client) != "tok-2" {
		t.Fatalf("forced client = %v, %v, want one on tok-2", client, err)
	}
	if len(exchange.calls) != 2 || !exchange.calls[1].GetForce() {
		t.Fatalf("exchange calls = %v, want the second forced", exchange.calls)
	}
	// The forced token replaces the cached one.
	client, err = s.Client(t.Context(), "jwt", false)
	if err != nil || tokenOf(client) != "tok-2" || len(exchange.calls) != 2 {
		t.Fatalf("after force: client %v, err %v, calls %d, want tok-2 from the cache", client, err, len(exchange.calls))
	}
	if *built != 2 {
		t.Fatalf("built %d clients, want one per token", *built)
	}
}

func TestClient_DropsTheRefusedEntryEvenWhenTheForcedExchangeFails(t *testing.T) {
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	exchange := &fakeExchange{token: "tok-1", expiry: now.Add(time.Hour)}
	s, _ := newTestSource(exchange, now)
	if _, err := s.Client(t.Context(), "jwt", false); err != nil {
		t.Fatal(err)
	}
	// GCS refused tok-1, and the actor has since left the node.
	exchange.err = status.Error(codes.NotFound, "actor not found")
	if _, err := s.Client(t.Context(), "jwt", true); status.Code(err) != codes.NotFound {
		t.Fatalf("forced exchange error = %v, want NotFound", err)
	}
	// The refused token must not come back from the cache.
	if _, err := s.Client(t.Context(), "jwt", false); status.Code(err) != codes.NotFound {
		t.Fatalf("after a failed force: err = %v, want NotFound from a new exchange", err)
	}
	if len(exchange.calls) != 3 {
		t.Fatalf("exchange calls = %d, want three", len(exchange.calls))
	}
}

func TestClient_DropsTheRefusedEntryWhenTheForcedTokenIsShort(t *testing.T) {
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	exchange := &fakeExchange{token: "tok-1", expiry: now.Add(time.Hour)}
	s, _ := newTestSource(exchange, now)
	if _, err := s.Client(t.Context(), "jwt", false); err != nil {
		t.Fatal(err)
	}
	exchange.token, exchange.expiry = "short", now.Add(credential.MinServeLifetime-time.Minute)
	if client, err := s.Client(t.Context(), "jwt", true); err != nil || tokenOf(client) != "short" {
		t.Fatalf("forced client = %v, %v, want one on the short token", client, err)
	}
	// Neither the refused tok-1 nor the short token is cached.
	if client, err := s.Client(t.Context(), "jwt", false); err != nil || tokenOf(client) != "short" || len(exchange.calls) != 3 {
		t.Fatalf("after force: client %v, err %v, calls %d, want a third exchange", client, err, len(exchange.calls))
	}
}

func TestClient_EvictsTheOldestJWTWhenFull(t *testing.T) {
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	exchange := &fakeExchange{token: "tok", expiry: now.Add(time.Hour)}
	s, _ := newTestSource(exchange, now)
	s.cache = lru.New(1)
	for _, jwt := range []string{"jwt-a", "jwt-b", "jwt-a"} {
		if _, err := s.Client(t.Context(), jwt, false); err != nil {
			t.Fatal(err)
		}
	}
	if len(exchange.calls) != 3 {
		t.Fatalf("exchange calls = %d, want 3: jwt-a was evicted by jwt-b and exchanged again", len(exchange.calls))
	}
}

func TestClient_UsesButDoesNotCacheAShortToken(t *testing.T) {
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	exchange := &fakeExchange{token: "short", expiry: now.Add(credential.MinServeLifetime - time.Minute)}
	s, _ := newTestSource(exchange, now)
	for range 2 {
		client, err := s.Client(t.Context(), "jwt", false)
		if err != nil || tokenOf(client) != "short" {
			t.Fatalf("client = %v, %v", client, err)
		}
	}
	if len(exchange.calls) != 2 {
		t.Fatalf("a short token must not be cached, calls = %d", len(exchange.calls))
	}
}

func TestClient_Errors(t *testing.T) {
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		jwt      string
		exchange *fakeExchange
		want     codes.Code
	}{
		{"empty jwt", "", &fakeExchange{token: "t", expiry: now.Add(time.Hour)}, codes.InvalidArgument},
		{"exchange refuses", "jwt", &fakeExchange{err: status.Error(codes.PermissionDenied, "actor is no longer on the token's node")}, codes.PermissionDenied},
		{"exchange unavailable", "jwt", &fakeExchange{err: status.Error(codes.Unavailable, "down")}, codes.Unavailable},
		{"exchange returns no token", "jwt", &fakeExchange{token: "", expiry: now.Add(time.Hour)}, codes.Internal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newTestSource(tc.exchange, now)
			_, err := s.Client(t.Context(), tc.jwt, false)
			if status.Code(err) != tc.want {
				t.Fatalf("code = %v (%v), want %v", status.Code(err), err, tc.want)
			}
			if tc.jwt == "" && len(tc.exchange.calls) != 0 {
				t.Errorf("the exchange was called %d times for an empty token", len(tc.exchange.calls))
			}
		})
	}
}

func TestClient_ReportsAClientBuildFailure(t *testing.T) {
	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	s := newSource(&fakeExchange{token: "t", expiry: now.Add(time.Hour)}, func(context.Context, credential.Token) (objectstorage.ObjectStorage, error) {
		return nil, errors.New("no transport")
	})
	_, err := s.Client(t.Context(), "jwt", false)
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v (%v), want Internal", status.Code(err), err)
	}
}

func TestNew_ChecksItsConfig(t *testing.T) {
	if _, err := New(Config{CAFile: "x"}); err == nil {
		t.Error("New without an address succeeded")
	}
	if _, err := New(Config{Address: "api:8445", CAFile: filepath.Join(t.TempDir(), "absent.pem")}); err == nil {
		t.Error("New with a missing CA bundle succeeded")
	}
}

type actorTokenExchange struct {
	exchangepb.CredentialExchangeClient
	expiry time.Time
}

func (e *actorTokenExchange) ExchangeActorJwt(_ context.Context, in *exchangepb.ExchangeActorJwtRequest, _ ...grpc.CallOption) (*exchangepb.ExchangeActorJwtResponse, error) {
	tok := "tok-" + in.GetActorJwt()
	if in.GetForce() {
		tok += "-forced"
	}
	return &exchangepb.ExchangeActorJwtResponse{
		AccessToken: tok,
		ExpireTime:  timestamppb.New(e.expiry),
	}, nil
}

func TestSource_SharesTransportsAcrossActors(t *testing.T) {
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("GCE_METADATA_HOST", "127.0.0.1:1")

	var (
		totalConns  atomic.Int64
		liveConns   atomic.Int64
		connsMu     sync.Mutex
		connHits    = make(map[string]int)
		holdEntered = make(chan struct{})
		holdRelease = make(chan struct{})
		holdOnce    sync.Once
		badReq      atomic.Pointer[string]
	)
	releaseHold := sync.OnceFunc(func() { close(holdRelease) })
	recordFailure := func(msg string) {
		badReq.CompareAndSwap(nil, &msg)
	}

	gcs := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			recordFailure(fmt.Sprintf("proto = %s, want HTTP/2", r.Proto))
			http.Error(w, "want HTTP/2", http.StatusHTTPVersionNotSupported)
			return
		}
		connsMu.Lock()
		connHits[r.RemoteAddr]++
		connsMu.Unlock()
		var object string
		switch r.Method {
		case http.MethodPost:
			object = r.URL.Query().Get("name")
		case http.MethodGet:
			object = strings.TrimPrefix(r.URL.Path, "/bucket/")
		default:
			recordFailure(fmt.Sprintf("unexpected %s %s", r.Method, r.URL))
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
			return
		}
		actor, rest, ok := strings.Cut(object, "/")
		if !ok {
			recordFailure(fmt.Sprintf("unexpected object %q", object))
			http.Error(w, "bad object", http.StatusBadRequest)
			return
		}
		wantAuth := "Bearer tok-" + actor
		if strings.HasPrefix(rest, "forced") {
			wantAuth += "-forced"
		}
		if got := r.Header.Get("Authorization"); got != wantAuth {
			recordFailure(fmt.Sprintf("object %q got Authorization %q, want %q", object, got, wantAuth))
			http.Error(w, "wrong token", http.StatusForbidden)
			return
		}
		if r.Method == http.MethodGet {
			body := "payload-" + actor
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(body)-1, len(body)))
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = io.WriteString(w, body)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		if rest == "inflight" {
			holdOnce.Do(func() { close(holdEntered) })
			select {
			case <-holdRelease:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"name":%q,"bucket":"bucket"}`, object)
	}))
	gcs.EnableHTTP2 = true
	gcs.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			totalConns.Add(1)
			liveConns.Add(1)
		case http.StateClosed, http.StateHijacked:
			liveConns.Add(-1)
		}
	}
	gcs.StartTLS()
	t.Cleanup(gcs.Close)
	t.Cleanup(releaseHold)

	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	base := gcs.Client().Transport.(*http.Transport).Clone()
	base.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, network, gcs.Listener.Addr().String())
	}
	base.TLSClientConfig.ServerName = "example.com"
	base.HTTP2 = &http.HTTP2Config{MaxReadFrameSize: 32 << 10}
	s := newPoolSource(&actorTokenExchange{expiry: now.Add(time.Hour)}, base)
	t.Cleanup(func() { _ = s.Close() })
	if base.HTTP2.SendPingTimeout != 0 {
		t.Fatalf("newPoolSource mutated base.HTTP2.SendPingTimeout = %v", base.HTTP2.SendPingTimeout)
	}
	for i, tr := range s.pool.transports {
		if tr.MaxIdleConnsPerHost != 100 || tr.HTTP2 == nil || tr.HTTP2.MaxReadFrameSize != 32<<10 || tr.HTTP2.SendPingTimeout != 31*time.Second {
			t.Fatalf("transport %d settings = (MaxIdleConnsPerHost=%d, HTTP2=%+v)", i, tr.MaxIdleConnsPerHost, tr.HTTP2)
		}
	}
	s.now = func() time.Time { return now }
	s.cache = lru.New(2)

	// Open all transportPoolSize HTTP/2 connections before concurrent transfers
	// so cold ALPN handshakes do not race.
	warmClient, err := s.Client(t.Context(), "actor-1", false)
	if err != nil {
		t.Fatal(err)
	}
	for i := range transportPoolSize {
		if err := warmClient.PutObject(t.Context(), "bucket", fmt.Sprintf("actor-1/warm-%d", i), bytes.NewReader([]byte("snap"))); err != nil {
			t.Fatalf("warm PutObject %d: %v", i, err)
		}
	}
	if got := totalConns.Load(); got != transportPoolSize {
		t.Fatalf("warmed %d TLS connections, want %d", got, transportPoolSize)
	}

	// Hold an upload open on actor-0 while later actors evict it from s.cache.
	evictedClient, err := s.Client(t.Context(), "actor-0", false)
	if err != nil {
		t.Fatal(err)
	}
	heldErr := make(chan error, 1)
	go func() {
		heldErr <- evictedClient.PutObject(t.Context(), "bucket", "actor-0/inflight", bytes.NewReader([]byte("snap")))
	}()
	select {
	case <-holdEntered:
	case err := <-heldErr:
		t.Fatalf("actor-0 upload exited before hold: %v", err)
	case <-t.Context().Done():
		t.Fatal("timed out waiting for actor-0 upload to reach server")
	}

	const actors = 40
	grp, gctx := errgroup.WithContext(t.Context())
	for i := 2; i < 2+actors; i++ {
		actor := fmt.Sprintf("actor-%d", i)
		grp.Go(func() error {
			c, err := s.Client(gctx, actor, false)
			if err != nil {
				return err
			}
			if err := c.PutObject(gctx, "bucket", actor+"/put", bytes.NewReader([]byte("snap"))); err != nil {
				return err
			}
			rc, err := c.GetObject(gctx, "bucket", actor+"/get")
			if err != nil {
				return err
			}
			got, readErr := io.ReadAll(rc)
			if err := errors.Join(readErr, rc.Close()); err != nil {
				return err
			}
			if want := "payload-" + actor; string(got) != want {
				return fmt.Errorf("GetObject(%s) = %q, want %q", actor, got, want)
			}
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		t.Fatalf("actor transfers failed: %v", err)
	}

	forced, err := s.Client(t.Context(), "actor-2", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := forced.PutObject(t.Context(), "bucket", "actor-2/forced", bytes.NewReader([]byte("snap"))); err != nil {
		t.Fatalf("forced PutObject: %v", err)
	}

	releaseHold()
	if err := <-heldErr; err != nil {
		t.Fatalf("evicted actor-0 upload failed: %v", err)
	}
	if msg := badReq.Load(); msg != nil {
		t.Fatalf("server saw invalid request: %s", *msg)
	}
	if got := totalConns.Load(); got != transportPoolSize {
		t.Fatalf("opened %d TLS connections across %d actors, want %d", got, actors+2, transportPoolSize)
	}
	if got := liveConns.Load(); got != transportPoolSize {
		t.Fatalf("live TLS connections = %d, want %d", got, transportPoolSize)
	}
	connsMu.Lock()
	distinctConns := len(connHits)
	for addr, hits := range connHits {
		if hits < 2 {
			t.Errorf("connection %s served %d requests, want >= 2 across actor churn", addr, hits)
		}
	}
	connsMu.Unlock()
	if distinctConns != transportPoolSize {
		t.Fatalf("requests used %d distinct TLS connections, want %d", distinctConns, transportPoolSize)
	}
}

func TestSource_CloseClosesIdleConnections(t *testing.T) {
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("GCE_METADATA_HOST", "127.0.0.1:1")

	var (
		liveConns  atomic.Int64
		connClosed = make(chan struct{}, transportPoolSize)
	)
	gcs := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"name":%q,"bucket":"bucket"}`, r.URL.Query().Get("name"))
	}))
	gcs.EnableHTTP2 = true
	gcs.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			liveConns.Add(1)
		case http.StateClosed, http.StateHijacked:
			liveConns.Add(-1)
			connClosed <- struct{}{}
		}
	}
	gcs.StartTLS()
	t.Cleanup(gcs.Close)

	now := time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)
	base := gcs.Client().Transport.(*http.Transport).Clone()
	base.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, network, gcs.Listener.Addr().String())
	}
	base.TLSClientConfig.ServerName = "example.com"
	s := newPoolSource(&actorTokenExchange{expiry: now.Add(time.Hour)}, base)
	s.now = func() time.Time { return now }

	client, err := s.Client(t.Context(), "actor-1", false)
	if err != nil {
		t.Fatal(err)
	}
	for i := range transportPoolSize {
		if err := client.PutObject(t.Context(), "bucket", fmt.Sprintf("actor-1/put-%d", i), bytes.NewReader([]byte("snap"))); err != nil {
			t.Fatalf("PutObject %d: %v", i, err)
		}
	}
	if got := liveConns.Load(); got != transportPoolSize {
		t.Fatalf("live TLS connections before Close = %d, want %d", got, transportPoolSize)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	for liveConns.Load() > 0 {
		select {
		case <-connClosed:
		case <-t.Context().Done():
			t.Fatalf("Close() left %d idle TLS connections open", liveConns.Load())
		}
	}
}

type exchangeServer struct {
	exchangepb.UnimplementedCredentialExchangeServer
	expiry time.Time
}

func (s *exchangeServer) ExchangeActorJwt(_ context.Context, in *exchangepb.ExchangeActorJwtRequest) (*exchangepb.ExchangeActorJwtResponse, error) {
	return &exchangepb.ExchangeActorJwtResponse{
		AccessToken: "tok-" + in.GetActorJwt(),
		ExpireTime:  timestamppb.New(s.expiry),
	}, nil
}

func TestNew_ExchangesAndCloses(t *testing.T) {
	var gotAuth atomic.Pointer[string]
	emulator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		gotAuth.Store(&auth)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"obj","bucket":"bucket"}`))
	}))
	t.Cleanup(emulator.Close)
	t.Setenv("STORAGE_EMULATOR_HOST", strings.TrimPrefix(emulator.URL, "http://"))

	serverTLS := testcert.NewServerTLS(t, net.ParseIP("127.0.0.1"))
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, serverTLS.RootCA, 0o600); err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(serverTLS.Certificate, serverTLS.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcSrv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
	})))
	exchangepb.RegisterCredentialExchangeServer(grpcSrv, &exchangeServer{expiry: time.Now().Add(time.Hour)})
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	s, err := New(Config{Address: lis.Addr().String(), CAFile: caFile})
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	client, err := s.Client(t.Context(), "actor-a", false)
	if err != nil {
		t.Fatalf("Client() = %v", err)
	}
	if err := client.PutObject(t.Context(), "bucket", "obj", bytes.NewReader([]byte("data"))); err != nil {
		t.Fatalf("PutObject() = %v", err)
	}
	if auth := gotAuth.Load(); auth == nil || *auth != "Bearer tok-actor-a" {
		t.Fatalf("Authorization = %v, want %q", auth, "Bearer tok-actor-a")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if _, err := s.Client(t.Context(), "actor-b", false); status.Code(err) != codes.Canceled {
		t.Fatalf("Client() after Close() = %v, want Canceled", err)
	}
}

var _ io.Closer = (*Source)(nil)
