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
	"sync"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/server"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/principal"
)

const (
	maxScopeCacheEntries = 4096
	// scopeProbeSuffix is an underscore-delimited sentinel resource name used
	// to probe whether a caller's permission holds across an entire atespace or
	// global scope. Underscores are rejected in resource names, so no stored
	// per-resource tuple can ever target this object.
	scopeProbeSuffix = "__ate_scope_probe__"
)

type scopeCheckKey struct {
	kind     string
	user     string
	relation string
	scopeObj string
}

type scopeCheckEntry struct {
	generation uint64
	expiresAt  time.Time
}

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

	cache        *checkCache
	scopeMu      sync.RWMutex
	scopeAllowed map[scopeCheckKey]scopeCheckEntry
}

// scopeCanonicalObject maps a Check target to its scope-level canonical object
// when scope caching can safely represent it. exact reports whether scopeObj is
// the literal object checked (global:root or atespace:<name>) rather than a
// synthetic child probe (<type>:<atespace>/__ate_scope_probe__).
func scopeCanonicalObject(relation, object string) (scopeObj string, exact bool) {
	if relation == "can_mint_worker_actor_credential" {
		return "", false
	}
	objectType, id, ok := strings.Cut(object, ":")
	if !ok || id == "" {
		return "", false
	}
	switch objectType {
	case "global":
		if object != GlobalRootObject {
			return "", false
		}
		return object, true
	case "atespace":
		if strings.Contains(id, "/") {
			return "", false
		}
		return object, true
	case "worker":
		if id == scopeProbeSuffix || strings.Contains(id, "/") {
			return "", false
		}
		return "worker:" + scopeProbeSuffix, false
	case "actor", "actor_template", "tag":
		atespace, name, hasSep := strings.Cut(id, "/")
		if !hasSep || atespace == "" || name == "" || name == scopeProbeSuffix || strings.Contains(name, "/") {
			return "", false
		}
		return objectType + ":" + atespace + "/" + scopeProbeSuffix, false
	}
	return "", false
}

func (a *Authorizer) getScopeAllowed(key scopeCheckKey, gen uint64, now time.Time) bool {
	a.scopeMu.RLock()
	entry, ok := a.scopeAllowed[key]
	a.scopeMu.RUnlock()
	return ok && entry.generation == gen && now.Before(entry.expiresAt)
}

func (a *Authorizer) putScopeAllowed(key scopeCheckKey, gen uint64, now time.Time) {
	a.scopeMu.Lock()
	if a.scopeAllowed == nil || len(a.scopeAllowed) >= maxScopeCacheEntries {
		a.scopeAllowed = make(map[scopeCheckKey]scopeCheckEntry, 64)
	}
	a.scopeAllowed[key] = scopeCheckEntry{generation: gen, expiresAt: now.Add(a.cache.ttl)}
	a.scopeMu.Unlock()
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
	if err := ctx.Err(); err != nil {
		return apierror.Internal("authz: %v", err)
	}
	p, ok := principal.FromContext(ctx)
	if !ok || p.ID == "" {
		return apierror.Unauthenticated("unauthenticated: missing principal in context")
	}
	user := formatUser(p.ID)

	var (
		scopeObj string
		exact    bool
		gen      uint64
		sKey     scopeCheckKey
	)
	if a.cache != nil {
		scopeObj, exact = scopeCanonicalObject(relation, object)
		if scopeObj != "" {
			gen = a.cache.generation.Load()
			sKey = scopeCheckKey{kind: p.Kind, user: user, relation: relation, scopeObj: scopeObj}
			if a.getScopeAllowed(sKey, gen, time.Now()) {
				return nil
			}
		}
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

	if scopeObj != "" && !exact {
		if scopeOK, err := a.checkRaw(ctx, user, relation, scopeObj, grants...); err == nil && scopeOK {
			a.putScopeAllowed(sKey, gen, time.Now())
			return nil
		}
	}

	allowed, err := a.checkRaw(ctx, user, relation, object, grants...)
	if err != nil {
		return err
	}
	if !allowed {
		return apierror.PermissionDenied("permission denied: principal %q lacks %q on %q", user, relation, object)
	}
	if scopeObj != "" && exact {
		a.putScopeAllowed(sKey, gen, time.Now())
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

// parentGlobalTuple links a global-scoped object (an atespace or a worker) to
// global:root.
func parentGlobalTuple(object string) *openfgav1.TupleKey {
	return &openfgav1.TupleKey{
		User:     GlobalRootObject,
		Relation: "parent_global",
		Object:   object,
	}
}
