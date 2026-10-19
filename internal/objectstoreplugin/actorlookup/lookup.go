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

// Package actorlookup fetches the actor an exchange request names from
// ate-api-server. The control plugin is a sidecar in the ate-api-server pod, so
// it dials the loopback address and presents the pod identity certificate,
// which ate-api-server accepts as an mTLS client.
package actorlookup

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"go.ate.dev/substrate/internal/actoridjwt"
	"go.ate.dev/substrate/internal/ateapiauth"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// ErrStale is returned when the actor at the name a JWT carries has a
// different UID: the actor was deleted and recreated since the JWT was minted.
var ErrStale = errors.New("actor UID does not match the JWT")

// Config says how to reach ate-api-server. Every file is re-read on each
// handshake, so certificate rotation needs no restart.
type Config struct {
	// Address is the gRPC target, 127.0.0.1:443 in the pod.
	Address string
	// CAFile verifies the serving certificate.
	CAFile string
	// ServerName is the DNS name expected on the serving certificate, since
	// Address is not a name the certificate carries.
	ServerName string
	// ClientCredBundle is the PEM bundle with the pod identity certificate
	// and key.
	ClientCredBundle string
}

// Lookup resolves actors over one connection to ate-api-server.
type Lookup struct {
	conn   *grpc.ClientConn
	client ateapipb.ControlClient
}

// New dials ate-api-server. The connection is lazy, so a server that is not up
// yet fails the first lookup rather than New.
func New(cfg Config) (*Lookup, error) {
	if cfg.Address == "" {
		return nil, errors.New("actorlookup: Address is required")
	}
	opts, err := ateapiauth.DialOptions(ateapiauth.ClientConfig{
		CAFile:           cfg.CAFile,
		ServerName:       cfg.ServerName,
		ClientCredBundle: cfg.ClientCredBundle,
	})
	if err != nil {
		return nil, err
	}
	// Carry the exchange's trace into GetActor, as the other ate-api-server
	// clients do.
	opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	conn, err := grpc.NewClient(cfg.Address, opts...)
	if err != nil {
		return nil, fmt.Errorf("actorlookup: while creating the ate-api-server client: %w", err)
	}
	return &Lookup{conn: conn, client: ateapipb.NewControlClient(conn)}, nil
}

// Actor returns the actor the claims name. The claims come from a verified
// JWT, so the atespace and name are trusted, but the actor may have been
// recreated since, which the UID detects. A gRPC status error from
// ate-api-server, such as NotFound, is returned unchanged.
func (l *Lookup) Actor(ctx context.Context, claims *actoridjwt.WireSubstrateClaims) (*ateapipb.Actor, error) {
	if claims == nil || claims.Atespace == "" || claims.ActorName == "" || claims.ActorUID == "" {
		return nil, errors.New("actorlookup: claims lack atespace, actor name, or UID")
	}
	actor, err := l.client.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: claims.Atespace, Name: claims.ActorName},
	})
	if err != nil {
		return nil, err
	}
	if actor.GetMetadata().GetUid() != claims.ActorUID {
		return nil, ErrStale
	}
	return actor, nil
}

// Close closes the connection.
func (l *Lookup) Close() error { return l.conn.Close() }
