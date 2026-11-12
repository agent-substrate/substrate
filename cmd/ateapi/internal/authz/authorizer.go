// Copyright 2026 The Agent Substrate Authors
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

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/server"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/principal"
	"go.ate.dev/substrate/internal/resources"
)

// Authorizer is the read-only policy decision point that evaluates runtime
// OpenFGA permissions against Substrate's store and authorization model.
type Authorizer struct {
	fgaServer *server.Server
	storeID   string
	modelID   string

	pureGoShortCircuit bool

	// bootstrapOwners holds the OpenFGA user strings of the server-configured
	// global owners. They hold owner on global:root through a contextual tuple
	// on every Check rather than a stored tuple, so they are not part of any
	// AccessPolicy, cannot be revoked through the API, and lose access once
	// the server runs without them in its configuration.
	bootstrapOwners map[string]struct{}

	// systemPrincipals maps the OpenFGA user strings of Substrate's own
	// components to the system roles they hold on global:root. Like
	// bootstrapOwners they are contextual tuples, but they only apply to
	// principals authenticated over mTLS.
	//
	// TODO: consider storing system grants as tuples, written from the
	// configuration at startup, so they are visible through the API. That
	// needs principal IDs qualified by authentication method, so a stored
	// tuple can't match a JWT subject, and AccessPolicy reconciliation
	// limited to its own relations, so it doesn't delete them.
	systemPrincipals map[string][]string
}

// Option configures Authorizer behavior.
type Option func(*Authorizer)

// WithPureGoShortCircuit enables or disables the pure-Go immutable contextual-tuple
// fast-path for bootstrap owners and mTLS system roles.
func WithPureGoShortCircuit(enabled bool) Option {
	return func(a *Authorizer) {
		a.pureGoShortCircuit = enabled
	}
}

// Check verifies that the principal in ctx has relation on object.
// Structural hierarchy links (global:root as parent_global of every atespace
// and worker, and each atespace as parent_atespace of its actors, actor
// templates and tags) and
// the caller's bootstrap owner and system role grants, if any, are injected as OpenFGA
// ContextualTuples at evaluation time rather than persisted in the tuple table.
func (a *Authorizer) Check(ctx context.Context, relation, object string) error {
	if IsBypassed(ctx) {
		return nil
	}
	if a == nil || a.fgaServer == nil {
		return apierror.Internal("authz: authorizer is not initialized")
	}
	p, ok := principal.FromContext(ctx)
	if !ok || p.ID == "" {
		return apierror.Unauthenticated("unauthenticated: missing principal in context")
	}
	user := formatUser(p.ID)
	if a.canShortCircuitInGo(p.Kind, user, relation, object) {
		return nil
	}
	var grants []*openfgav1.TupleKey
	if p.Kind == principal.KindMTLS {
		for _, role := range a.systemPrincipals[user] {
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
		return apierror.PermissionDenied("permission denied: principal %q lacks %q on %q", user, relation, object)
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
		return false, err
	}
	return resp.GetAllowed(), nil
}

// contextualTuples synthesizes the invariant structural hierarchy tuples for an
// object so OpenFGA can traverse parent-child inheritance (e.g. `owner from parent_global`
// in model.fga) in memory during Check evaluation without persisting structural
// tuples in PostgreSQL.
//
// Why contextual tuples are used instead of storing parent links in the database:
//  1. Deterministic structure: Every `atespace:<name>` and `worker:<name>`
//     unconditionally has `global:root` as its `parent_global`, and every
//     `actor:<atespace>/<name>`, `actor_template:<atespace>/<name>` and
//     `tag:<atespace>/<name>` has `atespace:<atespace>` as its
//     `parent_atespace`. Because these
//     relationships are derived purely from the object type/ID, storing a row
//     per resource in the OpenFGA `tuple` table would be redundant.
//  2. No write amplification on create: `CreateAtespace`, `CreateWorker`,
//     `CreateActorTemplate`, `CreateActor` and `CreateTag` can insert their rows without
//     opening an OpenFGA write transaction just to link the parent.
func contextualTuples(object string) []*openfgav1.TupleKey {
	objectType, id, _ := strings.Cut(object, ":")
	switch objectType {
	case "atespace", "worker":
		return []*openfgav1.TupleKey{parentGlobalTuple(object)}
	case "actor", "actor_template", "tag":
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

var bootstrapOwnerAllowedRelations = map[string]map[string]bool{
	"global": {
		"can_create_atespace":      true,
		"can_list_atespaces":       true,
		"can_create_worker":        true,
		"can_list_workers":         true,
		"can_list_actors":          true,
		"can_list_actor_templates": true,
		"can_list_tags":            true,
		"can_create_access_policy": true,
		"can_get_access_policy":    true,
		"can_update_access_policy": true,
	},
	"atespace": {
		"can_create_access_policy":  true,
		"can_get_access_policy":     true,
		"can_update_access_policy":  true,
		"can_delete_access_policy":  true,
		"can_create_actor":          true,
		"can_create_actor_template": true,
		"can_list_actors":           true,
		"can_list_actor_templates":  true,
		"can_create_tag":            true,
		"can_list_tags":             true,
		"can_get":                   true,
		"can_delete":                true,
	},
	"worker": {
		"can_get":                    true,
		"can_list_actor_assignments": true,
		"can_update":                 true,
		"can_delete":                 true,
		"can_drain":                  true,
	},
	"actor_template": {
		"can_get":          true,
		"can_use_template": true,
		"can_delete":       true,
	},
	"tag": {
		"can_get":     true,
		"can_update":  true,
		"can_delete":  true,
		"can_use_tag": true,
	},
	"actor": {
		"can_get":                  true,
		"can_update":               true,
		"can_delete":               true,
		"can_get_egress_policy":    true,
		"can_create_egress_policy": true,
		"can_update_egress_policy": true,
		"can_delete_egress_policy": true,
		"can_resume":               true,
		"can_suspend":              true,
		"can_revert":               true,
	},
}

var systemRoleAllowedRelations = map[string]map[string]map[string]bool{
	RoleIngressRouter: {
		"actor": {
			RelationCanResume: true,
		},
		"global": {
			RelationCanListActors: true,
		},
	},
}

func validateStructuralObject(object string) (string, bool) {
	objectType, id, ok := strings.Cut(object, ":")
	if !ok || id == "" {
		return "", false
	}
	switch objectType {
	case "global":
		if id == "root" {
			return objectType, true
		}
		return "", false
	case "atespace", "worker":
		if resources.IsValidResourceName(id) {
			return objectType, true
		}
		return "", false
	case "actor", "actor_template", "tag":
		atespace, name, ok := strings.Cut(id, "/")
		if !ok || !resources.IsValidResourceName(atespace) || !resources.IsValidResourceName(name) {
			return "", false
		}
		return objectType, true
	default:
		return "", false
	}
}

// canShortCircuitInGo evaluates immutable in-memory contextual-tuple grants
// (bootstrap global owners on non-mint relations, and mTLS ingress routers on
// actor can_resume) directly in Go without querying OpenFGA/PostgreSQL and
// without any cache or TTL staleness.
func (a *Authorizer) canShortCircuitInGo(kind, user, relation, object string) bool {
	if !a.pureGoShortCircuit {
		return false
	}
	objectType, ok := validateStructuralObject(object)
	if !ok {
		return false
	}
	if _, isBootstrapOwner := a.bootstrapOwners[user]; isBootstrapOwner {
		if allowed := bootstrapOwnerAllowedRelations[objectType]; allowed != nil && allowed[relation] {
			return true
		}
	}
	if kind == principal.KindMTLS {
		for _, role := range a.systemPrincipals[user] {
			if allowedTypes, ok := systemRoleAllowedRelations[role]; ok {
				if allowedRel, ok := allowedTypes[objectType]; ok && allowedRel[relation] {
					return true
				}
			}
		}
	}
	return false
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
