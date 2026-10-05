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
	"strings"

	"github.com/agent-substrate/substrate/internal/principal"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/server"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Authorizer is the read-only policy decision point that evaluates runtime
// OpenFGA permissions against Substrate's store and authorization model.
type Authorizer struct {
	fgaServer *server.Server
	storeID   string
	modelID   string

	// bootstrapOwners holds the OpenFGA user strings of the server-configured
	// global owners. They hold owner on global:root through a contextual tuple
	// on every Check rather than a stored tuple, so they are not part of any
	// AccessPolicy, cannot be revoked through the API, and lose access once
	// the server runs without them in its configuration.
	bootstrapOwners map[string]struct{}

	// systemGrants maps the OpenFGA user strings of Substrate's own
	// components to the system roles they hold on global:root. Like
	// bootstrapOwners they are contextual tuples, but they only apply to
	// principals authenticated over mTLS.
	//
	// TODO: consider storing system grants as tuples, written from the
	// configuration at startup, so they are visible through the API. That
	// needs principal IDs qualified by authentication method, so a stored
	// tuple can't match a JWT subject, and AccessPolicy reconciliation
	// limited to its own relations, so it doesn't delete them.
	systemGrants map[string][]string
}

// Check verifies that the principal in ctx has relation on object.
// Structural hierarchy links (global:root as parent_global of every atespace
// and worker, and each atespace as parent_atespace of its actors and actor templates) and
// the caller's bootstrap owner and system grants, if any, are injected as OpenFGA
// ContextualTuples at evaluation time rather than persisted in the tuple table.
func (a *Authorizer) Check(ctx context.Context, relation, object string) error {
	if IsBypassed(ctx) {
		return nil
	}
	if a == nil || a.fgaServer == nil {
		return status.Error(codes.Internal, "authz: authorizer is not initialized")
	}
	p, ok := principal.FromContext(ctx)
	if !ok || p.ID == "" {
		return status.Error(codes.Unauthenticated, "unauthenticated: missing principal in context")
	}
	user := formatUser(p.ID)
	var grants []*openfgav1.TupleKey
	// A bearer token's subject is chosen by its issuer, so only a SPIFFE ID
	// proven by a client certificate can carry a system role.
	if p.Kind == principal.KindMTLS {
		for _, role := range a.systemGrants[user] {
			grants = append(grants, &openfgav1.TupleKey{
				User:     user,
				Relation: role,
				Object:   GlobalRootObject,
			})
		}
	}
	allowed, err := a.checkRaw(ctx, user, relation, object, grants...)
	if err != nil {
		return err
	}
	if !allowed {
		return status.Errorf(codes.PermissionDenied, "permission denied: principal %q lacks %q on %q", user, relation, object)
	}
	return nil
}

// checkRaw reports whether user has relation on object. Besides the stored
// tuples, OpenFGA evaluates these as contextual tuples: object's structural
// parent links, user's bootstrap owner grant if configured, and grants.
func (a *Authorizer) checkRaw(ctx context.Context, user, relation, object string, grants ...*openfgav1.TupleKey) (bool, error) {
	tuples := append(contextualTuples(object), grants...)
	// A Check only evaluates the caller, so only the caller's own bootstrap
	// grant can affect the result.
	if _, ok := a.bootstrapOwners[user]; ok {
		tuples = append(tuples, &openfgav1.TupleKey{
			User:     user,
			Relation: RoleOwner,
			Object:   GlobalRootObject,
		})
	}
	var ctxTuples *openfgav1.ContextualTupleKeys
	if len(tuples) > 0 {
		ctxTuples = &openfgav1.ContextualTupleKeys{TupleKeys: tuples}
	}
	resp, err := a.fgaServer.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              a.storeID,
		AuthorizationModelId: a.modelID,
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User:     user,
			Relation: relation,
			Object:   object,
		},
		ContextualTuples: ctxTuples,
	})
	if err != nil {
		return false, statusFromFGAError(err)
	}
	return resp.GetAllowed(), nil
}

// statusFromFGAError translates an error returned by the embedded OpenFGA
// server into a gRPC status error. Context cancellation and deadline expiry
// (which OpenFGA maps to its own custom error codes) are preserved as
// codes.Canceled and codes.DeadlineExceeded so client disconnects and timeouts
// do not surface as 500s; all other OpenFGA errors (such as model/tuple
// validation or storage failures) indicate a server-side fault and fail closed
// with codes.Internal.
func statusFromFGAError(err error) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, serverErrors.ErrRequestCancelled):
		return status.Errorf(codes.Canceled, "authz check canceled: %v", err)
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, serverErrors.ErrRequestDeadlineExceeded):
		return status.Errorf(codes.DeadlineExceeded, "authz check deadline exceeded: %v", err)
	default:
		return status.Errorf(codes.Internal, "authz check failed: %v", err)
	}
}

// contextualTuples synthesizes the invariant structural hierarchy tuples for an
// object so OpenFGA can traverse parent-child inheritance (e.g. `owner from parent_global`
// in model.fga) in memory during Check evaluation without persisting structural
// tuples in PostgreSQL.
//
// Why contextual tuples are used instead of storing parent links in the database:
//  1. Deterministic structure: Every `atespace:<name>` and `worker:<name>`
//     unconditionally has `global:root` as its `parent_global`, and every
//     `actor:<atespace>/<name>` and `actor_template:<atespace>/<name>` has
//     `atespace:<atespace>` as its `parent_atespace`. Because these
//     relationships are derived purely from the object type/ID, storing a row
//     per resource in the OpenFGA `tuple` table would be redundant.
//  2. No write amplification on create: `CreateAtespace`, `CreateWorker`,
//     `CreateActorTemplate`, and `CreateActor` can insert their rows without
//     opening an OpenFGA write transaction just to link the parent.
func contextualTuples(object string) []*openfgav1.TupleKey {
	objectType, id, _ := strings.Cut(object, ":")
	switch objectType {
	case "atespace", "worker":
		return []*openfgav1.TupleKey{parentGlobalTuple(object)}
	case "actor", "actor_template":
		atespace, _, ok := strings.Cut(id, "/")
		if !ok {
			return nil
		}
		// atespacedID escapes the atespace the same way AtespaceObject does.
		parent := "atespace:" + atespace
		return []*openfgav1.TupleKey{
			{User: parent, Relation: "parent_atespace", Object: object},
			parentGlobalTuple(parent),
		}
	}
	return nil
}

// parentGlobalTuple links a global-scoped object (an atespace or a worker) to
// global:root.
func parentGlobalTuple(object string) *openfgav1.TupleKey {
	return &openfgav1.TupleKey{
		User:     GlobalRootObject,
		Relation: "parent_global",
		Object:   object,
	}
}
