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
	"strings"

	"github.com/agent-substrate/substrate/internal/principal"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/server"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Authorizer is the read-only policy decision point that evaluates runtime
// OpenFGA permissions against Substrate's store and authorization model.
type Authorizer struct {
	fgaServer *server.Server
	storeID   string
	modelID   string
}

// Check verifies that the principal in ctx has relation on object.
// Structural hierarchy links (such as global:root as parent_global of every
// atespace) are injected as OpenFGA ContextualTuples at evaluation time rather
// than persisted in the tuple table.
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
	allowed, err := a.checkRaw(ctx, user, relation, object)
	if err != nil {
		return err
	}
	if !allowed {
		return status.Errorf(codes.PermissionDenied, "permission denied: principal %q lacks %q on %q", user, relation, object)
	}
	return nil
}

func (a *Authorizer) checkRaw(ctx context.Context, user, relation, object string) (bool, error) {
	var ctxTuples *openfgav1.ContextualTupleKeys
	if tuples := contextualTuples(object); len(tuples) > 0 {
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
		return false, status.Errorf(codes.Internal, "authz check failed: %v", err)
	}
	return resp.GetAllowed(), nil
}

// contextualTuples synthesizes the invariant structural hierarchy tuples for an
// object so OpenFGA can traverse parent-child inheritance (e.g. `owner from parent_global`
// in model.fga) in memory during Check evaluation without persisting structural
// tuples in PostgreSQL.
//
// Why contextual tuples are used instead of storing `parent_global` in the database:
//  1. Deterministic structure: Every `atespace:<name>` unconditionally has
//     `global:root` as its `parent_global`. Because this relationship is derived
//     purely from the object type/ID, storing a row per atespace in the OpenFGA
//     `tuple` table would be redundant.
//  2. No write amplification on CreateAtespace: `CreateAtespace` can insert into
//     the `atespaces` table without opening an OpenFGA write transaction just to
//     link `parent_global`.
func contextualTuples(object string) []*openfgav1.TupleKey {
	if strings.HasPrefix(object, "atespace:") {
		return []*openfgav1.TupleKey{
			{
				User:     GlobalRootObject,
				Relation: "parent_global",
				Object:   object,
			},
		}
	}
	return nil
}
