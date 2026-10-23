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

// Package exchange serves this plugin's credential exchange. The node plugin
// presents the worker-for-actor JWT atelet gave it and gets back a GCS access
// token scoped to that actor's snapshot prefixes. The handler runs the checks
// in actorauthz, which yield the actor's grants, and hands the grants to an
// Issuer. actorauthz is provider-neutral and is what another provider's
// exchange reuses. This package and its proto are not: they are shaped for
// this binary's credential, and the Issuer interface is the seam between its
// backends.
package exchange

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.ate.dev/substrate/internal/objectstoreplugin/actorauthz"
	"go.ate.dev/substrate/internal/objectstoreplugin/actorscope"
	"go.ate.dev/substrate/internal/objectstoreplugin/credential"
	exchangepb "go.ate.dev/substrate/internal/proto/objectstoreexchangepb"
)

// DefaultPort is the port the control sidecar serves the exchange on and the
// node plugin dials by default, through the ate-api-server Service.
const DefaultPort = 8445

// Issuer mints a credential limited to grants for the actor. force skips any
// cache the issuer keeps. An error carrying a gRPC status code other than
// Unavailable is passed to the caller unchanged, for failures the same request
// would hit again, and one wrapping the context's error keeps the caller's
// code. Any other error is reported as Unavailable. The GCS issuer is
// gcsissuer.
type Issuer interface {
	Issue(ctx context.Context, actorUID string, grants []actorscope.Grant, force bool) (credential.Token, error)
}

// Lookup resolves the actor a verified JWT names.
type Lookup = actorauthz.Lookup

// Verifier checks a JWT and returns its claims.
type Verifier = actorauthz.Verifier

// Server implements CredentialExchange.
type Server struct {
	exchangepb.UnimplementedCredentialExchangeServer
	authorizer *actorauthz.Authorizer
	issuer     Issuer
}

// New returns a Server. The verifier must accept actorauthz.Audience.
func New(verifier Verifier, lookup Lookup, issuer Issuer) *Server {
	return &Server{authorizer: actorauthz.New(verifier, lookup), issuer: issuer}
}

// ExchangeActorJwt is the exchange. It issues a credential only for a token
// actorauthz.Authorizer.Authorize accepts.
func (s *Server) ExchangeActorJwt(ctx context.Context, req *exchangepb.ExchangeActorJwtRequest) (*exchangepb.ExchangeActorJwtResponse, error) {
	decision, err := s.authorizer.Authorize(ctx, req.GetActorJwt())
	if err != nil {
		return nil, err
	}
	tok, err := s.issuer.Issue(ctx, decision.ActorUID, decision.Grants, req.GetForce())
	if err != nil {
		return nil, status.Errorf(issueCode(err), "while issuing the credential: %v", err)
	}
	return &exchangepb.ExchangeActorJwtResponse{AccessToken: tok.AccessToken, ExpireTime: timestamppb.New(tok.Expiry)}, nil
}

// issueCode is the code the caller sees for an issuer error. A context error
// keeps the caller's code, a gRPC status keeps its own, and anything else is
// a backend failure worth retrying.
func issueCode(err error) codes.Code {
	switch {
	case errors.Is(err, context.Canceled):
		return codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return codes.DeadlineExceeded
	case status.Code(err) == codes.Unknown:
		return codes.Unavailable
	}
	return status.Code(err)
}
