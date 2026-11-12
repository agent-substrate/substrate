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

// Package exchangeclient is the GCS node plugin's side of the credential
// exchange. For each transfer it trades the actor JWT atelet handed it for a
// GCS access token scoped to that actor's snapshot prefixes, caches the token
// and the storage client built with it under that JWT, and hands the client
// to the transfer. It is this binary's: a provider's own plugin brings its
// own.
package exchangeclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/utils/lru"

	"go.ate.dev/substrate/internal/credbundle"
	"go.ate.dev/substrate/internal/objectstoreplugin/credential"
	exchangepb "go.ate.dev/substrate/internal/proto/objectstoreexchangepb"
	"go.ate.dev/substrate/pkg/objectstorage"
)

// defaultCacheSize bounds the cache, one entry per JWT atelet holds, which
// is one per actor the node transferred for within a JWT lifetime. Eviction
// only costs the next transfer an exchange and a new client.
const defaultCacheSize = 1_000

// transportPoolSize is how many HTTP/2 transports Source shares across actor
// clients on a node, sized so concurrent ranged restores across several actors
// do not bottleneck on a single transfer's 8-stream width.
const transportPoolSize = 32

// Config is how the exchange is reached.
type Config struct {
	// Address is the exchange's host and port, the ate-api-server Service.
	Address string
	// CAFile verifies the exchange's serving certificate.
	CAFile string
	// ServerName is the DNS name expected on that certificate.
	ServerName string
}

// Source implements objectstoreplugin.Credentials for GCS.
type Source struct {
	exchange exchangepb.CredentialExchangeClient
	conn     io.Closer
	pool     *transportPool
	// newClient builds the storage client a token is used through.
	newClient func(ctx context.Context, tok credential.Token) (objectstorage.ObjectStorage, error)
	now       func() time.Time
	// cache maps an actor JWT to its entry: the token the exchange returned
	// for that JWT and the storage client built with the token. Nothing
	// closes a client that leaves the cache. A transfer still using it
	// finishes on the shared transport pool.
	cache *lru.Cache
}

// entry is a token the exchange returned for one JWT and the storage client
// built with it.
type entry struct {
	token  credential.Token
	client objectstorage.ObjectStorage
}

// New dials the exchange over TLS. Nothing is sent until the first transfer.
func New(cfg Config) (*Source, error) {
	if cfg.Address == "" {
		return nil, errors.New("exchangeclient: Address is required")
	}
	pool, err := credbundle.ParsePool(cfg.CAFile)
	if err != nil {
		return nil, fmt.Errorf("exchangeclient: while loading the exchange CA bundle: %w", err)
	}
	creds := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pool, ServerName: cfg.ServerName})
	conn, err := grpc.NewClient(cfg.Address, grpc.WithTransportCredentials(creds), grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	if err != nil {
		return nil, fmt.Errorf("exchangeclient: while creating the exchange client: %w", err)
	}
	s := newPoolSource(exchangepb.NewCredentialExchangeClient(conn), http.DefaultTransport.(*http.Transport))
	s.conn = conn
	return s, nil
}

func newSource(exchange exchangepb.CredentialExchangeClient, newClient func(context.Context, credential.Token) (objectstorage.ObjectStorage, error)) *Source {
	return &Source{exchange: exchange, newClient: newClient, now: time.Now, cache: lru.New(defaultCacheSize)}
}

func newPoolSource(exchange exchangepb.CredentialExchangeClient, base *http.Transport) *Source {
	pool := newTransportPool(base)
	s := newSource(exchange, pool.newClient)
	s.pool = pool
	return s
}

// transportPool spreads GCS requests round-robin across a fixed set of
// HTTP/2 transports shared by every actor client built from a Source.
type transportPool struct {
	transports []*http.Transport
	next       atomic.Uint64
}

func newTransportPool(base *http.Transport) *transportPool {
	transports := make([]*http.Transport, transportPoolSize)
	for i := range transports {
		t := base.Clone()
		// WithHTTPClient skips google.golang.org/api/transport/http's
		// defaultBaseTransport, so apply its idle pool and HTTP/2 ping defaults
		// here.
		t.MaxIdleConnsPerHost = 100
		if t.HTTP2 == nil {
			t.HTTP2 = &http.HTTP2Config{}
		}
		t.HTTP2.SendPingTimeout = 31 * time.Second
		transports[i] = t
	}
	return &transportPool{transports: transports}
}

func (p *transportPool) RoundTrip(req *http.Request) (*http.Response, error) {
	i := (p.next.Add(1) - 1) % uint64(len(p.transports))
	return p.transports[i].RoundTrip(req)
}

func (p *transportPool) closeIdleConnections() {
	for _, t := range p.transports {
		t.CloseIdleConnections()
	}
}

// newClient builds a client that authenticates with tok alone over p. It
// outlives the request that built it, so it must not carry that request's
// cancellation.
func (p *transportPool) newClient(ctx context.Context, tok credential.Token) (objectstorage.ObjectStorage, error) {
	bg := context.WithoutCancel(ctx)
	source := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: tok.AccessToken, Expiry: tok.Expiry})
	rt, err := htransport.NewTransport(bg, p, option.WithTokenSource(source))
	if err != nil {
		return nil, err
	}
	// rt already attaches tok. WithoutAuthentication keeps storage.NewClient
	// from probing ambient credentials that WithHTTPClient ignores.
	return objectstorage.NewGCSClient(bg, option.WithoutAuthentication(), option.WithHTTPClient(&http.Client{Transport: rt}))
}

// Close closes idle GCS connections in the shared transport pool and closes
// the connection to the exchange.
func (s *Source) Close() error {
	if s.pool != nil {
		s.pool.closeIdleConnections()
	}
	if s.conn == nil {
		return nil
	}
	return s.conn.Close()
}

// Client returns a storage client scoped to the actor actorJWT names: the
// cached one for that JWT while its token has at least
// credential.MinServeLifetime left, otherwise one built on a fresh exchange,
// which replaces the cached one. force skips the cache, after GCS refused a
// request made with the cached token, and asks the exchange to skip its own.
// A refused or nearly expired entry is dropped before the exchange, so it is
// not served again if the exchange fails or returns a token too short to
// keep. An exchange error reaches the caller unchanged.
func (s *Source) Client(ctx context.Context, actorJWT string, force bool) (objectstorage.ObjectStorage, error) {
	if actorJWT == "" {
		return nil, status.Error(codes.InvalidArgument, "actor_jwt is required on GCS")
	}
	if !force {
		if v, ok := s.cache.Get(actorJWT); ok {
			if e := v.(*entry); e.token.Expiry.Sub(s.now()) >= credential.MinServeLifetime {
				return e.client, nil
			}
		}
	}
	s.cache.Remove(actorJWT)
	resp, err := s.exchange.ExchangeActorJwt(ctx, &exchangepb.ExchangeActorJwtRequest{ActorJwt: actorJWT, Force: force})
	if err != nil {
		return nil, err
	}
	tok := credential.Token{AccessToken: resp.GetAccessToken(), Expiry: expiry(resp.GetExpireTime())}
	if tok.AccessToken == "" {
		return nil, status.Error(codes.Internal, "the exchange returned no access token")
	}
	client, err := s.newClient(ctx, tok)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "while building the actor's storage client: %v", err)
	}
	// A token shorter than the floor serves this transfer and is not kept.
	if tok.Expiry.Sub(s.now()) >= credential.MinServeLifetime {
		s.cache.Add(actorJWT, &entry{token: tok, client: client})
	}
	return client, nil
}

func expiry(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}
