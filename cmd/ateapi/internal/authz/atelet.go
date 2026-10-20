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

package authz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"go.ate.dev/substrate/cmd/ateapi/internal/ateletauth"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/principal"
	"go.ate.dev/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// ActorLocator reports where Actors are hosted. The interceptor uses it to
// limit atelet to the Actors on its node.
type ActorLocator interface {
	// ActorHostedOnNode reports whether node hosts the Actor. It returns
	// false, not an error, if the Actor does not exist.
	ActorHostedOnNode(ctx context.Context, actor resources.ActorRef, node string) (bool, error)
}

// AteletAuthorizer recognizes atelet by its SPIFFE ID and authorizes it by
// node. A nil *AteletAuthorizer treats no caller as atelet.
type AteletAuthorizer struct {
	spiffeID string
	locator  ActorLocator
}

// NewAteletAuthorizer returns an AteletAuthorizer for atelet's SPIFFE ID. It
// fails on an invalid ID or a nil locator.
func NewAteletAuthorizer(spiffeID string, locator ActorLocator) (*AteletAuthorizer, error) {
	if _, err := spiffeid.FromString(spiffeID); err != nil {
		return nil, fmt.Errorf("invalid atelet SPIFFE ID %q: %w", spiffeID, err)
	}
	if locator == nil {
		return nil, errors.New("atelet authorizer needs an ActorLocator")
	}
	return &AteletAuthorizer{spiffeID: spiffeID, locator: locator}, nil
}

// actorExtractor returns the Actor a request names. A missing or malformed
// reference is codes.InvalidArgument; any other error is codes.Internal.
type actorExtractor func(req any) (resources.ActorRef, error)

// actorOf adapts actorFor, written against the concrete request type T, into
// an actorExtractor.
func actorOf[T any](actorFor func(T) (resourceRef, *field.Path)) actorExtractor {
	return func(req any) (resources.ActorRef, error) {
		r, ok := req.(T)
		if !ok {
			return resources.ActorRef{}, apierror.Internal("authz: unexpected request type %T", req)
		}
		ref, fldPath := actorFor(r)
		if errs := validateRef(ref, fldPath); len(errs) > 0 {
			return resources.ActorRef{}, resources.ToAPIError(errs)
		}
		return resources.ActorRef{Atespace: ref.GetAtespace(), Name: ref.GetName()}, nil
	}
}

// ateletOnNode lets atelet call the RPC only for an Actor on its own node.
// For atelet, this node check replaces the OpenFGA checks. The rule must be
// built by actorRule.
func (r rpcRule) ateletOnNode() rpcRule {
	r.ateletByNode = true
	return r
}

// node returns the node of the calling atelet, and whether the caller is
// atelet at all. The caller is atelet when it authenticated over mTLS as
// atelet's SPIFFE ID; its node then comes from its certificate.
func (a *AteletAuthorizer) node(ctx context.Context) (string, bool, error) {
	if a == nil {
		return "", false, nil
	}
	p, ok := principal.FromContext(ctx)
	if !ok || p.Kind != principal.KindMTLS || p.ID != a.spiffeID {
		return "", false, nil
	}
	// TODO: Put NodeName on PrincipalInfo; apiauthn reads it from the
	// PodIdentity extension.
	caller, err := ateletauth.Authenticate(ctx, a.spiffeID)
	if err != nil {
		return "", true, err
	}
	return caller.NodeName, true, nil
}

// authorizeOnNode allows atelet, on node, to call an RPC only for an Actor
// that node hosts.
//
// Every refusal is NotFound, whether the Actor is on another node, on none, or
// does not exist, so a node learns nothing about Actors it does not host, not
// even that they exist.
func (a *AteletAuthorizer) authorizeOnNode(ctx context.Context, rule rpcRule, req any, node string) error {
	actorRef, err := rule.actor(req)
	if err != nil {
		return err
	}
	hosted, err := a.locator.ActorHostedOnNode(ctx, actorRef, node)
	if err != nil {
		return err
	}
	if !hosted {
		slog.WarnContext(ctx, "Refusing atelet a request for an actor its node does not host",
			slog.String("actor", actorRef.String()),
			slog.String("caller_node", node))
		return apierror.NotFound("Actor %s not found", actorRef)
	}
	return nil
}
