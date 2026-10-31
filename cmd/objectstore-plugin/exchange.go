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
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.ate.dev/substrate/internal/credbundle"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorauthz"
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
	listen          string
	credBundle      string
	issuer          string
	issuerCAFile    string
	transferAccount string
	ateapi          actorlookup.Config
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
	if cfg.transferAccount == "" {
		return nil, nil, errors.New("--transfer-service-account is required to serve the exchange on GCS")
	}
	verifier, err := newActorVerifier(ctx, cfg.issuer, cfg.issuerCAFile)
	if err != nil {
		return nil, nil, err
	}
	lookup, err := actorlookup.New(cfg.ateapi)
	if err != nil {
		return nil, nil, err
	}
	issuer, err := gcsissuer.New(ctx, cfg.transferAccount)
	if err != nil {
		lookup.Close()
		return nil, nil, fmt.Errorf("while setting up the GCS credential issuer: %w", err)
	}
	return exchange.New(actorauthz.New(verifier, lookup), issuer), func() { _ = lookup.Close() }, nil
}

// newActorVerifier returns the exchange's actor JWT verifier, with the
// issuer's keys already fetched by checkIssuerCertificate.
func newActorVerifier(ctx context.Context, issuer, caFile string) (*oidcjwt.Verifier, error) {
	client, err := oidcjwt.NewHTTPClient(issuer, caFile, "")
	if err != nil {
		return nil, fmt.Errorf("while setting up the actor JWT issuer client: %w", err)
	}
	verifier := actorauthz.NewVerifier(issuer, client)
	if err := checkIssuerCertificate(ctx, issuer, verifier); err != nil {
		return nil, err
	}
	return verifier, nil
}

// issuerCheckTimeout bounds the start-time fetch from the actor JWT issuer.
const issuerCheckTimeout = 10 * time.Second

// checkIssuerCertificate fetches the issuer's keys into verifier's cache so a
// certificate the client does not trust fails the start instead of every
// exchange. Other errors may be transient, such as an issuer that is not up
// yet, so they are logged and the exchange serves.
func checkIssuerCertificate(ctx context.Context, issuer string, verifier *oidcjwt.Verifier) error {
	ctx, cancel := context.WithTimeout(ctx, issuerCheckTimeout)
	defer cancel()
	err := verifier.Refresh(ctx, time.Now())
	if certErr := (*tls.CertificateVerificationError)(nil); errors.As(err, &certErr) {
		return fmt.Errorf("actor JWT issuer serves a certificate that does not verify against --actor-jwt-issuer-ca-file, or the system roots when it is empty: %w", err)
	}
	if err != nil {
		slog.WarnContext(ctx, "Could not fetch actor JWT signing keys at start; exchanges retry the fetch", slog.String("issuer", issuer), slog.Any("err", err))
	}
	return nil
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
