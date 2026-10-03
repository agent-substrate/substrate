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

// Package storetest provides isolated database-backed stores for tests.
//
// SetupTestStore uses PostgreSQL unless ATE_TEST_STORE_BACKEND is "mysql". One
// container per backend is shared by every test in a package; each test gets
// its own database. Nothing stops those containers when the test binary exits,
// so packages using this package must call [Shutdown] from their TestMain.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/authz"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// BackendEnv selects the database SetupTestStore uses: "postgres" (the
// default) or "mysql".
const BackendEnv = "ATE_TEST_STORE_BACKEND"

// databaseCount numbers the per-test databases across both backends.
var databaseCount atomic.Uint64

// SetupTestStore returns a real database-backed store with a database unique
// to this test. A shared container keeps this suitable for packages that run
// subtests in parallel; databases are dropped during cleanup.
func SetupTestStore(t *testing.T) (store.Interface, func()) {
	t.Helper()
	s, _ := SetupAuthzTestStore(t)
	return s, func() {}
}

// AuthzStore is a test store that also takes an authz policy manager.
type AuthzStore interface {
	store.Interface
	SetPolicyManager(*authz.PolicyManager)
}

// SetupAuthzTestStore is SetupTestStore for tests that wire their own OpenFGA
// authorizer: it also returns the authz backend over the store's pool.
func SetupAuthzTestStore(t *testing.T) (AuthzStore, authz.Backend) {
	t.Helper()
	switch backend := os.Getenv(BackendEnv); backend {
	case "", "postgres":
		p := SetupPostgresPersistence(t)
		return p, authz.PostgresBackend(p.Pool())
	case "mysql":
		p := SetupMySQLPersistence(t)
		return p, authz.MySQLBackend(p.DB())
	default:
		t.Fatalf("%s must be postgres or mysql, got %q", BackendEnv, backend)
		return nil, authz.Backend{}
	}
}

func nextDatabaseName() string {
	return fmt.Sprintf("ateapi_test_%d", databaseCount.Add(1))
}

// MustCreateAtespace creates name unless it already exists. Both backends enforce
// the parent relationship for actor and snapshot records, so test fixtures use
// this before seeding those resources.
func MustCreateAtespace(t *testing.T, ctx context.Context, s store.Interface, name string) {
	t.Helper()
	_, err := s.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: name}})
	if err != nil && !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("creating test atespace %q: %v", name, err)
	}
}

// MustCreateActor ensures actor's parent atespace exists, then creates actor.
// Use the store method directly only in tests that exercise missing-parent
// behavior.
func MustCreateActor(t *testing.T, ctx context.Context, s store.Interface, actor *ateapipb.Actor) *ateapipb.Actor {
	t.Helper()
	atespace := actor.GetMetadata().GetAtespace()
	MustCreateAtespace(t, ctx, s, atespace)
	created, err := s.CreateActor(ctx, actor)
	if err != nil {
		t.Fatalf("creating test actor %q/%q: %v", atespace, actor.GetMetadata().GetName(), err)
	}
	return created
}

// MustCreateTag ensures tag's parent atespace exists, then stores
// tag as given — ready or still pending, whichever state the test needs to
// start from. Use the store method directly only in tests that exercise
// missing-parent behavior.
func MustCreateTag(t *testing.T, ctx context.Context, s store.Interface, tag *ateapipb.Tag) *ateapipb.Tag {
	t.Helper()
	atespace := tag.GetMetadata().GetAtespace()
	name := tag.GetMetadata().GetName()
	MustCreateAtespace(t, ctx, s, atespace)
	created, err := s.CreateTag(ctx, tag)
	if err != nil {
		t.Fatalf("creating test tag %q/%q: %v", atespace, name, err)
	}
	return created
}

// RunTests runs m and terminates the shared database containers afterwards.
// Packages with no other TestMain work should use it as their whole TestMain;
// the rest must call [Shutdown] themselves.
func RunTests(m *testing.M) {
	code := m.Run()
	Shutdown()
	os.Exit(code)
}

// Shutdown terminates the shared database containers, if any were started.
func Shutdown() {
	shutdownPostgres()
	shutdownMySQL()
}
