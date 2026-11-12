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
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/server"
	"github.com/openfga/openfga/pkg/storage"
	"go.ate.dev/substrate/internal/principal"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// memFGADatastore is a lightweight, thread-safe in-memory implementation of
// storage.OpenFGADatastore for unit tests and microbenchmarks that exercise the
// embedded OpenFGA evaluation engine without requiring a PostgreSQL container.
type memFGADatastore struct {
	mu     sync.RWMutex
	stores map[string]*openfgav1.Store
	models map[string][]*openfgav1.AuthorizationModel
	tuples map[string][]*openfgav1.Tuple
}

func newMemFGADatastore() *memFGADatastore {
	return &memFGADatastore{
		stores: make(map[string]*openfgav1.Store),
		models: make(map[string][]*openfgav1.AuthorizationModel),
		tuples: make(map[string][]*openfgav1.Tuple),
	}
}

func (m *memFGADatastore) Read(_ context.Context, store string, filter storage.ReadFilter, _ storage.ReadOptions) (storage.TupleIterator, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*openfgav1.Tuple
	for _, t := range m.tuples[store] {
		k := t.GetKey()
		if filter.Object != "" && !matchesObjectFilter(k.GetObject(), filter.Object) {
			continue
		}
		if filter.Relation != "" && k.GetRelation() != filter.Relation {
			continue
		}
		if filter.User != "" && k.GetUser() != filter.User {
			continue
		}
		out = append(out, t)
	}
	return storage.NewStaticTupleIterator(out), nil
}

func matchesObjectFilter(obj, filter string) bool {
	if strings.HasSuffix(filter, ":") {
		return strings.HasPrefix(obj, filter)
	}
	return obj == filter
}

func (m *memFGADatastore) ReadPage(ctx context.Context, store string, filter storage.ReadFilter, _ storage.ReadPageOptions) ([]*openfgav1.Tuple, string, error) {
	it, err := m.Read(ctx, store, filter, storage.ReadOptions{})
	if err != nil {
		return nil, "", err
	}
	defer it.Stop()
	var out []*openfgav1.Tuple
	for {
		t, err := it.Next(ctx)
		if err != nil {
			break
		}
		out = append(out, t)
	}
	return out, "", nil
}

func (m *memFGADatastore) ReadUserTuple(_ context.Context, store string, filter storage.ReadUserTupleFilter, _ storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, t := range m.tuples[store] {
		k := t.GetKey()
		if k.GetObject() == filter.Object && k.GetRelation() == filter.Relation && k.GetUser() == filter.User {
			return t, nil
		}
	}
	return nil, storage.ErrNotFound
}

func (m *memFGADatastore) ReadUsersetTuples(_ context.Context, store string, filter storage.ReadUsersetTuplesFilter, _ storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*openfgav1.Tuple
	for _, t := range m.tuples[store] {
		k := t.GetKey()
		if k.GetObject() != filter.Object || k.GetRelation() != filter.Relation {
			continue
		}
		if !strings.Contains(k.GetUser(), "#") && !strings.HasSuffix(k.GetUser(), ":*") {
			continue
		}
		if len(filter.AllowedUserTypeRestrictions) > 0 && !matchesUserTypeRestrictions(k.GetUser(), filter.AllowedUserTypeRestrictions) {
			continue
		}
		out = append(out, t)
	}
	return storage.NewStaticTupleIterator(out), nil
}

func matchesUserTypeRestrictions(user string, restrictions []*openfgav1.RelationReference) bool {
	uType, rem, ok := strings.Cut(user, ":")
	if !ok {
		return false
	}
	_, uRel, hasRel := strings.Cut(rem, "#")
	isWildcard := rem == "*"
	for _, r := range restrictions {
		if r.GetType() != uType {
			continue
		}
		switch v := r.GetRelationOrWildcard().(type) {
		case *openfgav1.RelationReference_Wildcard:
			if isWildcard {
				return true
			}
		case *openfgav1.RelationReference_Relation:
			if hasRel && v.Relation == uRel {
				return true
			}
		default:
			if !isWildcard && !hasRel {
				return true
			}
		}
	}
	return false
}

func (m *memFGADatastore) ReadStartingWithUser(_ context.Context, store string, filter storage.ReadStartingWithUserFilter, _ storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	users := make(map[string]struct{}, len(filter.UserFilter))
	for _, uf := range filter.UserFilter {
		u := uf.GetObject()
		if uf.GetRelation() != "" {
			u += "#" + uf.GetRelation()
		}
		users[u] = struct{}{}
	}
	prefix := filter.ObjectType + ":"
	var out []*openfgav1.Tuple
	for _, t := range m.tuples[store] {
		k := t.GetKey()
		if !strings.HasPrefix(k.GetObject(), prefix) || k.GetRelation() != filter.Relation {
			continue
		}
		if _, ok := users[k.GetUser()]; !ok {
			continue
		}
		if filter.ObjectIDs != nil && !filter.ObjectIDs.Exists(strings.TrimPrefix(k.GetObject(), prefix)) {
			continue
		}
		out = append(out, t)
	}
	return storage.NewStaticTupleIterator(out), nil
}

func (m *memFGADatastore) Write(_ context.Context, store string, deletes storage.Deletes, writes storage.Writes, _ ...storage.TupleWriteOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur := m.tuples[store]
	for _, d := range deletes {
		filtered := cur[:0]
		for _, existing := range cur {
			k := existing.GetKey()
			if k.GetObject() == d.GetObject() && k.GetRelation() == d.GetRelation() && k.GetUser() == d.GetUser() {
				continue
			}
			filtered = append(filtered, existing)
		}
		cur = filtered
	}
	now := timestamppb.Now()
	for _, w := range writes {
		cur = append(cur, &openfgav1.Tuple{Key: w, Timestamp: now})
	}
	m.tuples[store] = cur
	return nil
}

func (m *memFGADatastore) MaxTuplesPerWrite() int { return storage.DefaultMaxTuplesPerWrite }

func (m *memFGADatastore) ReadAuthorizationModel(_ context.Context, store, id string) (*openfgav1.AuthorizationModel, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, mdl := range m.models[store] {
		if mdl.GetId() == id {
			return mdl, nil
		}
	}
	return nil, storage.ErrNotFound
}

func (m *memFGADatastore) ReadAuthorizationModels(_ context.Context, store string, _ storage.ReadAuthorizationModelsOptions) ([]*openfgav1.AuthorizationModel, string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	models := m.models[store]
	out := make([]*openfgav1.AuthorizationModel, len(models))
	for i := range models {
		out[len(models)-1-i] = models[i]
	}
	return out, "", nil
}

func (m *memFGADatastore) FindLatestAuthorizationModel(_ context.Context, store string) (*openfgav1.AuthorizationModel, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	models := m.models[store]
	if len(models) == 0 {
		return nil, storage.ErrNotFound
	}
	return models[len(models)-1], nil
}

func (m *memFGADatastore) MaxTypesPerAuthorizationModel() int {
	return storage.DefaultMaxTypesPerAuthorizationModel
}

func (m *memFGADatastore) WriteAuthorizationModel(_ context.Context, store string, model *openfgav1.AuthorizationModel) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.models[store] = append(m.models[store], model)
	return nil
}

func (m *memFGADatastore) CreateStore(_ context.Context, st *openfgav1.Store) (*openfgav1.Store, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.stores[st.GetId()]; exists {
		return nil, storage.ErrCollision
	}
	now := timestamppb.Now()
	saved := &openfgav1.Store{
		Id:        st.GetId(),
		Name:      st.GetName(),
		CreatedAt: now,
		UpdatedAt: now,
	}
	m.stores[saved.GetId()] = saved
	return saved, nil
}

func (m *memFGADatastore) DeleteStore(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.stores, id)
	return nil
}

func (m *memFGADatastore) GetStore(_ context.Context, id string) (*openfgav1.Store, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	st, ok := m.stores[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return st, nil
}

func (m *memFGADatastore) ListStores(_ context.Context, opts storage.ListStoresOptions) ([]*openfgav1.Store, string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*openfgav1.Store
	for _, st := range m.stores {
		if opts.Name != "" && st.GetName() != opts.Name {
			continue
		}
		out = append(out, st)
	}
	return out, "", nil
}

func (m *memFGADatastore) WriteAssertions(context.Context, string, string, []*openfgav1.Assertion) error {
	return nil
}

func (m *memFGADatastore) ReadAssertions(context.Context, string, string) ([]*openfgav1.Assertion, error) {
	return nil, nil
}

func (m *memFGADatastore) ReadChanges(context.Context, string, storage.ReadChangesFilter, storage.ReadChangesOptions) ([]*openfgav1.TupleChange, string, error) {
	return nil, "", storage.ErrNotFound
}

func (m *memFGADatastore) IsReady(context.Context) (storage.ReadinessStatus, error) {
	return storage.ReadinessStatus{IsReady: true}, nil
}

func (m *memFGADatastore) Close() {}

func newInMemoryFGAServer(tb testing.TB) (*server.Server, string, string) {
	tb.Helper()
	ds := newMemFGADatastore()
	srv, err := server.NewServerWithOpts(server.WithDatastore(ds))
	if err != nil {
		tb.Fatalf("server.NewServerWithOpts: %v", err)
	}
	tb.Cleanup(srv.Close)

	storeID, modelID, err := ensureStoreAndModel(context.Background(), srv)
	if err != nil {
		tb.Fatalf("ensureStoreAndModel: %v", err)
	}
	return srv, storeID, modelID
}

func newBenchAuthorizer(b *testing.B) (*Authorizer, context.Context, context.Context) {
	b.Helper()
	fgaSrv, storeID, modelID := newInMemoryFGAServer(b)

	const (
		bootstrapID = "bootstrap-admin@example.com"
		routerID    = "spiffe://cluster.local/ns/ate-system/sa/atenet-router"
	)

	owners, err := parseBootstrapOwners([]string{bootstrapID})
	if err != nil {
		b.Fatalf("parseBootstrapOwners: %v", err)
	}
	sysPrincipals, err := parseSystemPrincipals(SystemPrincipals{
		ControllerSPIFFEID:       "spiffe://cluster.local/ns/ate-system/sa/ate-controller",
		EgressGatewaySPIFFEIDs:   []string{"spiffe://cluster.local/ns/ate-system/sa/atenet-egress"},
		IngressRouterSPIFFEID:    routerID,
		AteletSPIFFEID:           "spiffe://cluster.local/ns/ate-system/sa/atelet",
		APIServerSidecarSPIFFEID: "spiffe://cluster.local/ns/ate-system/sa/ate-api-server",
	})
	if err != nil {
		b.Fatalf("parseSystemPrincipals: %v", err)
	}

	auth := &Authorizer{
		fgaServer:          fgaSrv,
		storeID:            storeID,
		modelID:            modelID,
		pureGoShortCircuit: true,
		bootstrapOwners:    owners,
		systemPrincipals:   sysPrincipals,
	}

	ownerCtx := principal.InjectContext(context.Background(), principal.PrincipalInfo{
		ID:   bootstrapID,
		Kind: principal.KindJWT,
	})
	routerCtx := principal.InjectContext(context.Background(), principal.PrincipalInfo{
		ID:   routerID,
		Kind: principal.KindMTLS,
	})
	return auth, ownerCtx, routerCtx
}

func BenchmarkAuthorizerCheck(b *testing.B) {
	auth, ownerCtx, routerCtx := newBenchAuthorizer(b)
	actorObj := ActorObject("ate-env", "sandbox-1")
	atespaceObj := AtespaceObject("ate-env")

	b.Run("BootstrapOwner_ActorResume", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := auth.Check(ownerCtx, RelationCanResume, actorObj); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("BootstrapOwner_AtespaceCreateActor", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := auth.Check(ownerCtx, RelationCanCreateActor, atespaceObj); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("IngressRouter_ActorResume", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := auth.Check(routerCtx, RelationCanResume, actorObj); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkAuthorizerCheckParallel(b *testing.B) {
	auth, ownerCtx, routerCtx := newBenchAuthorizer(b)
	actorObj := ActorObject("ate-env", "sandbox-1")

	b.Run("BootstrapOwner_ActorResume", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := auth.Check(ownerCtx, RelationCanResume, actorObj); err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	b.Run("IngressRouter_ActorResume", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := auth.Check(routerCtx, RelationCanResume, actorObj); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}
