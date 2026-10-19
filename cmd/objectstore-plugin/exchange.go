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

package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.ate.dev/substrate/internal/credbundle"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorlookup"
	"go.ate.dev/substrate/internal/objectstoreplugin/exchange"
	"go.ate.dev/substrate/internal/objectstoreplugin/gcsissuer"
	"go.ate.dev/substrate/internal/oidcjwt"
	exchangepb "go.ate.dev/substrate/internal/proto/objectstoreexchangepb"
	"go.ate.dev/substrate/internal/serverboot"
	"go.ate.dev/substrate/pkg/objectstorage"
)

// exchangeConfig is what control mode needs to serve the credential exchange.
type exchangeConfig struct {
	listen     string
	credBundle string
	issuer     string
	jwksFile   string
	ateapi     actorlookup.Config
}

// newExchangeService builds the exchange handler for the configured backend.
// The returned function closes what the handler holds open, the loopback
// connection to ate-api-server, and is called at shutdown. GCS serves the
// exchange. S3 answers UNIMPLEMENTED, since the S3 node plugin never calls
// it: it ignores actor_jwt and transfers with the pod's own credentials. The
// backend seam is exchange.Issuer, the one piece of the exchange that is not
// shared between backends.
func newExchangeService(ctx context.Context, cfg exchangeConfig) (exchangepb.CredentialExchangeServer, func(), error) {
	if objectstorage.UsesS3() {
		return exchangepb.UnimplementedCredentialExchangeServer{}, func() {}, nil
	}
	if cfg.issuer == "" {
		return nil, nil, errors.New("--actor-jwt-issuer is required to serve the exchange on GCS")
	}
	if cfg.jwksFile == "" {
		return nil, nil, errors.New("--actor-jwt-jwks-file is required to serve the exchange on GCS")
	}
	// Read the key set once now, so a wrong path fails the start rather
	// than the first exchange.
	keys := oidcjwt.FileKeySource(cfg.jwksFile)
	if _, err := keys.Keys(ctx); err != nil {
		return nil, nil, fmt.Errorf("while reading the actor JWT key set: %w", err)
	}
	verifier := oidcjwt.NewVerifier(cfg.issuer, []string{exchange.ActorJWTAudience}, keys, oidcjwt.WithSubstrateClaims())
	lookup, err := actorlookup.New(cfg.ateapi)
	if err != nil {
		return nil, nil, err
	}
	issuer, err := gcsissuer.New(ctx)
	if err != nil {
		lookup.Close()
		return nil, nil, fmt.Errorf("while setting up the GCS credential issuer: %w", err)
	}
	return exchange.New(verifier, lookup, issuer), func() { _ = lookup.Close() }, nil
}

// exchangeTransportCredentials serves the pod's service DNS certificate,
// re-read on rotation. The exchange runs TLS without client certificates:
// the JWT is the credential, and proof that the caller is atelet is a later
// addition.
func exchangeTransportCredentials(credBundle string) (credentials.TransportCredentials, error) {
	load := credbundle.Loader(credBundle)
	if _, err := load(nil); err != nil {
		return nil, fmt.Errorf("while loading the exchange serving certificate: %w", err)
	}
	return credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, GetCertificate: load}), nil
}

// serveExchange starts the exchange on its own TLS listener, beside the
// plugin's Unix socket server, and returns a function that drains it.
func serveExchange(ctx context.Context, cfg exchangeConfig) (func(), error) {
	svc, release, err := newExchangeService(ctx, cfg)
	if err != nil {
		return nil, err
	}
	creds, err := exchangeTransportCredentials(cfg.credBundle)
	if err != nil {
		release()
		return nil, err
	}
	lis, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		release()
		return nil, fmt.Errorf("while listening on %s: %w", cfg.listen, err)
	}
	srv := grpc.NewServer(grpc.Creds(creds), grpc.StatsHandler(otelgrpc.NewServerHandler()))
	exchangepb.RegisterCredentialExchangeServer(srv, svc)
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go func() {
		slog.InfoContext(ctx, "Serving credential exchange", slog.String("address", lis.Addr().String()))
		// GracefulStop makes Serve return nil. Anything else leaves the
		// node plugins without an exchange while the socket keeps the pod
		// looking healthy, so the process exits and restarts instead.
		if err := srv.Serve(lis); err != nil {
			serverboot.Fatal(ctx, "Credential exchange failed", err)
		}
	}()
	return func() {
		srv.GracefulStop()
		release()
	}, nil
}
