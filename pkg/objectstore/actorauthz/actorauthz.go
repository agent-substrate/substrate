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

// Package actorauthz runs the checks Substrate's GCS credential exchange runs,
// for an object-store provider that serves an exchange of its own. Authorize
// verifies the worker-for-actor JWT atelet sends with a transfer, looks the
// actor up in ate-api-server, refuses a token for an actor that has left the
// token's node or has no transfer due, and returns the storage prefixes the
// credential must be limited to. The provider issues the credential and owns
// its wire format.
//
// The exchange must run in the ate-api-server pod, as the objectstore-plugin
// control sidecar does. ate-api-server lets that pod's identity read actors
// through the api_server_sidecar role, see docs/guides/authorization.md.
// APIServerConfig dials ate-api-server over the loopback with the pod identity
// certificate.
//
// The JWTs' public keys come from the jwks key of the actor-id-jwt-pool
// Secret, mounted into the pod as a file that Config.JWKSFile names. Mount it
// without subPath, so a rotated key set is picked up.
//
// This package imports packages under internal/, so that a provider runs the
// same code as the in-tree exchange and only this package's API is public. Go
// allows it: the internal rule checks the importer's path, and every package
// under go.ate.dev/substrate may import go.ate.dev/substrate/internal/...,
// while code outside the module may import this package.
package actorauthz

import (
	"context"
	"errors"
	"fmt"

	"go.ate.dev/substrate/internal/actoridjwt"
	internalauthz "go.ate.dev/substrate/internal/objectstoreplugin/actorauthz"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorlookup"
)

// Audience is the audience of the actor_jwt field in the objectstore v1 API
// (go.ate.dev/substrate/api/objectstore/v1). atelet requests it on the
// worker-for-actor JWT it sends with a transfer. Authorize refuses a token
// minted for any other audience.
const Audience = "objectstore-plugin.ate.dev"

// Config says how an Authorizer verifies JWTs and reaches ate-api-server.
type Config struct {
	// Issuer is the issuer URL of the actor JWTs, the value ate-api-server
	// runs with. It must be https:// followed by a host and optional port,
	// with no path, query or fragment.
	Issuer string
	// JWKSFile is the JWK set file holding the public keys the JWTs are
	// verified with. It is re-read when the verifier refreshes its keys, so
	// it must be replaced atomically, as a projected volume is.
	JWKSFile string
	// APIServer says how to reach ate-api-server.
	APIServer APIServerConfig
}

// APIServerConfig says how to reach ate-api-server. Every file is re-read on
// each handshake, so certificate rotation needs no restart.
type APIServerConfig struct {
	// Address is the gRPC target, 127.0.0.1:443 in the pod.
	Address string
	// CAFile verifies the serving certificate.
	CAFile string
	// ServerName is the DNS name expected on the serving certificate, since
	// Address is not a name the certificate carries. It is required: with no
	// name, any certificate the CA signed would be accepted.
	ServerName string
	// ClientCredBundle is the PEM bundle with the pod identity certificate
	// and key.
	ClientCredBundle string
}

// Access is what a grant permits under its prefix.
type Access int

const (
	// ReadOnly covers the tag or golden snapshot the actor was created from.
	ReadOnly Access = iota + 1
	// ReadWrite covers the actor's own snapshots.
	ReadWrite
)

func (a Access) String() string {
	switch a {
	case ReadOnly:
		return "ro"
	case ReadWrite:
		return "rw"
	}
	return fmt.Sprintf("Access(%d)", int(a))
}

// Grant is one storage prefix and the access allowed under it.
type Grant struct {
	// Prefix is the storage prefix URI, such as
	// gs://bucket/root/atespaces/team-a/actors/<uid>/. It ends in "/", so a
	// string prefix match cannot reach a sibling whose name starts the same
	// way, such as another actor whose UID begins with this one's. It is
	// percent-encoded, as snapshot URIs are. Parse it with url.Parse before
	// comparing it with object names: the host is the bucket, and the path
	// without its leading "/" is the object name prefix.
	Prefix string
	Access Access
}

// Decision is what Authorize allowed: the UID of the actor a credential may
// be issued for, and the grants the credential must be limited to. Grants are
// in the order the GCS exchange gets them, ReadWrite before ReadOnly.
type Decision struct {
	ActorUID string
	Grants   []Grant
}

// Authorizer runs the checks. It is safe for concurrent use.
type Authorizer struct {
	checks *internalauthz.Authorizer
	lookup *actorlookup.Lookup
}

// New returns an Authorizer that accepts tokens from cfg.Issuer for Audience.
// It reads the key set once, so a bad JWKSFile fails here rather than on the
// first Authorize. The connection to ate-api-server is lazy, so a server that
// is not up yet fails the first Authorize instead. Close releases it.
func New(ctx context.Context, cfg Config) (*Authorizer, error) {
	if cfg.Issuer == "" {
		return nil, errors.New("actorauthz: Issuer is required")
	}
	if err := actoridjwt.ValidateIssuer(cfg.Issuer); err != nil {
		return nil, fmt.Errorf("actorauthz: %w", err)
	}
	if cfg.JWKSFile == "" {
		return nil, errors.New("actorauthz: JWKSFile is required")
	}
	if cfg.APIServer.Address == "" {
		return nil, errors.New("actorauthz: APIServer.Address is required")
	}
	if cfg.APIServer.CAFile == "" {
		return nil, errors.New("actorauthz: APIServer.CAFile is required")
	}
	if cfg.APIServer.ServerName == "" {
		return nil, errors.New("actorauthz: APIServer.ServerName is required")
	}
	if cfg.APIServer.ClientCredBundle == "" {
		return nil, errors.New("actorauthz: APIServer.ClientCredBundle is required")
	}
	verifier, err := internalauthz.NewVerifier(ctx, cfg.Issuer, cfg.JWKSFile)
	if err != nil {
		return nil, fmt.Errorf("actorauthz: %w", err)
	}
	lookup, err := actorlookup.New(actorlookup.Config(cfg.APIServer))
	if err != nil {
		return nil, fmt.Errorf("actorauthz: %w", err)
	}
	return &Authorizer{checks: internalauthz.New(verifier, lookup), lookup: lookup}, nil
}

// Close closes the connection to ate-api-server.
func (a *Authorizer) Close() error { return a.lookup.Close() }

// Authorize checks jwt and returns what a credential may be issued for. Only
// a worker-for-actor token is accepted, never an actor's own. Every error is
// a gRPC status error the exchange can return as is. Callers may rely on the
// codes, not the messages:
//
//   - InvalidArgument: jwt is empty.
//   - Unauthenticated: jwt fails verification, lacks the actor's atespace,
//     name, UID or node, or names an actor ate-api-server rejects.
//   - PermissionDenied: jwt is an actor's own token, the actor is in a state
//     with no transfer due or has left the token's node, or the actor has no
//     snapshot location.
//   - NotFound: the actor is gone or was recreated with another UID.
//   - FailedPrecondition: ate-api-server refuses to let this pod read actors.
//   - Unavailable: the actor lookup failed otherwise.
//   - Internal: the actor's storage grants cannot be derived from its
//     snapshot URIs.
//   - Canceled, DeadlineExceeded: the actor lookup was canceled or timed out.
func (a *Authorizer) Authorize(ctx context.Context, jwt string) (*Decision, error) {
	d, err := a.checks.Authorize(ctx, jwt)
	if err != nil {
		return nil, err
	}
	grants := make([]Grant, 0, len(d.Grants))
	for _, g := range d.Grants {
		// objectstore.BucketPrefix appends "/" the same way for the GCS
		// boundary. It is not called here because its package links cloud
		// SDKs.
		grants = append(grants, Grant{Prefix: g.Prefix.String() + "/", Access: Access(g.Access)})
	}
	return &Decision{ActorUID: d.ActorUID, Grants: grants}, nil
}
