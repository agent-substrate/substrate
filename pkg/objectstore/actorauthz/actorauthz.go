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
// The exchange must run in the ate-api-server pod and present that pod's
// identity certificate, which atelet accepts for every Atelet RPC, so the
// provider's sidecar must be as trusted as ate-api-server. ate-api-server lets
// that identity read actors through the api_server_sidecar role, see
// docs/guides/authorization.md. APIServerConfig dials ate-api-server over the
// loopback with the certificate.
//
// The JWT is a bearer token: serve the exchange over TLS, admit only atelet,
// call Authorize for every request, and issue nothing beyond Grants.
//
// The JWTs' public keys come from the issuer through OpenID Connect discovery
// at <Issuer>/.well-known/openid-configuration. The issuer's certificate must
// verify against Config.IssuerCAFile, or the system roots if the issuer's host
// does not end in .svc.
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
	"net/http"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/actoridjwt"
	internalauthz "go.ate.dev/substrate/internal/objectstoreplugin/actorauthz"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorlookup"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorscope"
)

// Audience is the audience of the actor_jwt field in the objectstore v1 API
// (go.ate.dev/substrate/api/objectstore/v1). atelet requests it on the
// worker-for-actor JWT it sends with a transfer. Authorize refuses a token
// minted for any other audience.
const Audience = internalauthz.Audience

// MaxJWTSize is the largest jwt, in bytes, Authorize accepts.
const MaxJWTSize = 8 << 10

// Config says how an Authorizer verifies JWTs and reaches ate-api-server.
type Config struct {
	// Issuer is the issuer URL of the actor JWTs, the value ate-api-server
	// runs with. It must be https:// followed by a host and optional port,
	// with no path, query or fragment.
	Issuer string
	// IssuerCAFile is a PEM CA bundle that verifies Issuer's serving
	// certificate, such as the service DNS trust bundle the in-tree
	// objectstore-plugin sidecar uses. For an Issuer whose host ends in .svc,
	// it is the only trusted bundle. For any other Issuer, the system roots
	// are trusted too. It is required. It is re-read when it changes. If a
	// re-read fails, the last bundle stays in use.
	IssuerCAFile string
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
	// Expiry is the token's exp. A credential issued from a Decision must not
	// outlive it.
	Expiry time.Time
}

// Authorizer runs the checks. It is safe for concurrent use.
type Authorizer struct {
	checks       *internalauthz.Authorizer
	lookup       *actorlookup.Lookup
	issuerClient *http.Client
}

// New returns an Authorizer that accepts tokens from cfg.Issuer for Audience.
// It fetches the issuer's keys once, with a short timeout. It fails if
// IssuerCAFile cannot be read or holds no certificate, or if the issuer's
// certificate does not verify. Any other fetch error is logged, and Authorize
// fetches again, returning Unavailable while the keys cannot be fetched. The
// connection to ate-api-server is lazy, so a server that is not up yet fails
// the first Authorize instead. Close releases it.
func New(ctx context.Context, cfg Config) (*Authorizer, error) {
	if cfg.Issuer == "" {
		return nil, errors.New("actorauthz: Issuer is required")
	}
	if err := actoridjwt.ValidateIssuer(cfg.Issuer); err != nil {
		return nil, fmt.Errorf("actorauthz: %w", err)
	}
	if cfg.IssuerCAFile == "" {
		return nil, errors.New("actorauthz: IssuerCAFile is required")
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
	verifier, issuerClient, err := internalauthz.StartVerifier(ctx, cfg.Issuer, cfg.IssuerCAFile)
	if err != nil {
		return nil, fmt.Errorf("actorauthz: %w", err)
	}
	lookup, err := actorlookup.New(actorlookup.Config(cfg.APIServer))
	if err != nil {
		issuerClient.CloseIdleConnections()
		return nil, fmt.Errorf("actorauthz: %w", err)
	}
	return &Authorizer{checks: internalauthz.New(verifier, lookup), lookup: lookup, issuerClient: issuerClient}, nil
}

// Close closes the connection to ate-api-server and the idle connections to
// the issuer. Do not call Authorize after Close.
func (a *Authorizer) Close() error {
	a.issuerClient.CloseIdleConnections()
	return a.lookup.Close()
}

// Authorize checks jwt and returns what a credential may be issued for. Only
// a worker-for-actor token is accepted, never an actor's own. Every error is
// a gRPC status error the exchange can return as is. Callers may rely on the
// codes, not the messages:
//
//   - InvalidArgument: jwt is empty or longer than MaxJWTSize.
//   - Unauthenticated: jwt fails verification, lacks the actor's atespace,
//     name, UID or node, or names an actor ate-api-server rejects.
//   - PermissionDenied: jwt is an actor's own token, the actor is in a state
//     with no transfer due or has left the token's node, or the actor has no
//     snapshot location.
//   - NotFound: the actor is gone or was recreated with another UID.
//   - FailedPrecondition: ate-api-server refuses to let this pod read actors.
//   - Unavailable: the issuer's signing keys cannot be fetched, or the actor
//     lookup failed otherwise.
//   - Internal: the actor's storage grants cannot be derived from its
//     snapshot URIs, or a grant has an access this package doesn't know.
//   - Canceled, DeadlineExceeded: ctx ended during the key fetch, or the
//     actor lookup was canceled or timed out.
func (a *Authorizer) Authorize(ctx context.Context, jwt string) (*Decision, error) {
	if len(jwt) > MaxJWTSize {
		return nil, status.Errorf(codes.InvalidArgument, "actor_jwt is longer than %d bytes", MaxJWTSize)
	}
	d, err := a.checks.Authorize(ctx, jwt)
	if err != nil {
		return nil, err
	}
	grants, err := toGrants(d.Grants)
	if err != nil {
		return nil, err
	}
	return &Decision{ActorUID: d.ActorUID, Grants: grants, Expiry: d.Expiry}, nil
}

// toGrants converts the internal grants. It refuses an access it has no
// public value for, so a provider never sees an Access it doesn't know.
func toGrants(in []actorscope.Grant) ([]Grant, error) {
	grants := make([]Grant, 0, len(in))
	for _, g := range in {
		var access Access
		switch g.Access {
		case actorscope.ReadOnly:
			access = ReadOnly
		case actorscope.ReadWrite:
			access = ReadWrite
		default:
			return nil, status.Errorf(codes.Internal, "grant for %s has unknown access %v", g.Prefix, g.Access)
		}
		// objectstore.BucketPrefix appends "/" the same way for the GCS
		// boundary. It is not called here because its package links cloud
		// SDKs.
		grants = append(grants, Grant{Prefix: g.Prefix.String() + "/", Access: access})
	}
	return grants, nil
}
