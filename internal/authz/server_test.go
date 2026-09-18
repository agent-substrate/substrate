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
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/principal"
	"github.com/jackc/pgx/v5/pgxpool"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func configureDockerEnv(ctx context.Context) error {
	if os.Getenv("DOCKER_HOST") != "" {
		return nil
	}
	output, err := exec.CommandContext(ctx, "docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}").Output()
	if err != nil {
		return err
	}
	host := strings.TrimSpace(string(output))
	if host == "" {
		return nil
	}
	_ = os.Setenv("DOCKER_HOST", host)
	if os.Getenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE") == "" {
		socket := host
		if runtime.GOOS == "darwin" {
			socket = "/var/run/docker.sock"
		}
		_ = os.Setenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", socket)
	}
	return nil
}

func startPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	if err := configureDockerEnv(ctx); err != nil {
		t.Skipf("Docker not available for testcontainers: %v", err)
	}

	pgContainer, err := postgres.Run(ctx, "postgres:18-alpine",
		postgres.WithDatabase("authz_test"),
		postgres.WithUsername("authz"),
		postgres.WithPassword("authz"),
	)
	if err != nil {
		t.Fatalf("starting postgres container: %v", err)
	}
	t.Cleanup(func() {
		_ = pgContainer.Terminate(context.Background())
	})

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("getting postgres connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("creating pgxpool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
	})

	var pingErr error
	for i := 0; i < 30; i++ {
		pingErr = pool.Ping(ctx)
		if pingErr == nil {
			return pool
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for postgres ping: %v", pingErr)
	return nil
}

func TestNewServer_NilPool(t *testing.T) {
	_, err := NewServer(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error when pool is nil, got nil")
	}
}

func TestNewServer_InitializeAndCheck(t *testing.T) {
	pool := startPostgres(t)
	ctx := context.Background()

	srv, err := NewServer(ctx, pool)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer srv.Close()

	if srv.StoreID() == "" {
		t.Fatal("expected non-empty StoreID")
	}
	if srv.ModelID() == "" {
		t.Fatal("expected non-empty ModelID")
	}
	if srv.FGAServer() == nil {
		t.Fatal("expected non-nil FGAServer")
	}

	// Write relationship tuples and verify authorization checks against the model.
	_, err = srv.FGAServer().Write(ctx, &openfgav1.WriteRequest{
		StoreId:              srv.StoreID(),
		AuthorizationModelId: srv.ModelID(),
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{
					User:     "user:alice",
					Relation: "owner",
					Object:   "global:root",
				},
				{
					User:     "global:root",
					Relation: "parent_global",
					Object:   "atespace:space-1",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Write tuples failed: %v", err)
	}

	checkResp, err := srv.FGAServer().Check(ctx, &openfgav1.CheckRequest{
		StoreId:              srv.StoreID(),
		AuthorizationModelId: srv.ModelID(),
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User:     "user:alice",
			Relation: "can_set_policy",
			Object:   "atespace:space-1",
		},
	})
	if err != nil {
		t.Fatalf("Check alice can_set_policy failed: %v", err)
	}
	if !checkResp.GetAllowed() {
		t.Errorf("expected alice to be allowed can_set_policy on atespace:space-1 via global owner inheritance")
	}

	checkBob, err := srv.FGAServer().Check(ctx, &openfgav1.CheckRequest{
		StoreId:              srv.StoreID(),
		AuthorizationModelId: srv.ModelID(),
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User:     "user:bob",
			Relation: "can_set_policy",
			Object:   "atespace:space-1",
		},
	})
	if err != nil {
		t.Fatalf("Check bob can_set_policy failed: %v", err)
	}
	if checkBob.GetAllowed() {
		t.Errorf("expected bob to be denied can_set_policy on atespace:space-1")
	}

	// Verify idempotent re-initialization reuses the existing store and model.
	srv2, err := NewServer(ctx, pool)
	if err != nil {
		t.Fatalf("second NewServer failed: %v", err)
	}
	defer srv2.Close()

	if srv2.StoreID() != srv.StoreID() {
		t.Errorf("expected same StoreID %q on re-init, got %q", srv.StoreID(), srv2.StoreID())
	}
	if srv2.ModelID() != srv.ModelID() {
		t.Errorf("expected same ModelID %q on re-init, got %q", srv.ModelID(), srv2.ModelID())
	}
}

func TestServer_CheckPermissionAndAtespaceLifecycle(t *testing.T) {
	pool := startPostgres(t)
	ctx := context.Background()

	srv, err := NewServer(ctx, pool)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer srv.Close()

	// Seed alice and a Kubernetes ServiceAccount as global owners via BootstrapGlobalOwners.
	if err := srv.BootstrapGlobalOwners(ctx, []string{"alice", "system:serviceaccount:default:default"}); err != nil {
		t.Fatalf("BootstrapGlobalOwners failed: %v", err)
	}

	// 1. Missing principal -> Unauthenticated.
	err = srv.CheckPermission(ctx, RelationCanCreateAtespace, GlobalRootObject)
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated for empty context, got %v", err)
	}

	// 2. Bypass context -> allowed even without principal.
	if err := srv.CheckPermission(WithBypass(ctx), RelationCanCreateAtespace, GlobalRootObject); err != nil {
		t.Errorf("expected bypass context to succeed, got %v", err)
	}

	aliceCtx := principal.InjectContext(ctx, principal.PrincipalInfo{ID: "alice", Kind: principal.KindJWT})
	saCtx := principal.InjectContext(ctx, principal.PrincipalInfo{ID: "system:serviceaccount:default:default", Kind: principal.KindJWT})
	bobCtx := principal.InjectContext(ctx, principal.PrincipalInfo{ID: "bob", Kind: principal.KindJWT})

	// 3. Alice and SA (global owners) can create and list atespaces; Bob cannot.
	for _, c := range []context.Context{aliceCtx, saCtx} {
		if err := srv.CheckPermission(c, RelationCanCreateAtespace, GlobalRootObject); err != nil {
			t.Errorf("expected global owner to be allowed can_create_atespace, got %v", err)
		}
		if err := srv.CheckPermission(c, RelationCanListAtespaces, GlobalRootObject); err != nil {
			t.Errorf("expected global owner to be allowed can_list_atespaces, got %v", err)
		}
	}
	if err := srv.CheckPermission(bobCtx, RelationCanCreateAtespace, GlobalRootObject); status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected bob to be denied can_create_atespace, got %v", err)
	}
	if err := srv.CheckPermission(bobCtx, RelationCanListAtespaces, GlobalRootObject); status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected bob to be denied can_list_atespaces, got %v", err)
	}

	// 4. Before OnCreateAtespace (or for a nonexistent atespace):
	// Global owner (alice) is allowed through by CheckPermission fallback so DB can return NotFound,
	// whereas unprivileged user (bob) is denied immediately (preventing existence enumeration).
	if err := srv.CheckPermission(aliceCtx, RelationCanGet, AtespaceObject("team-x")); err != nil {
		t.Errorf("expected global owner alice allowed fallback on unlinked atespace, got %v", err)
	}
	if err := srv.CheckPermission(bobCtx, RelationCanGet, AtespaceObject("team-x")); status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected unprivileged bob denied on unlinked atespace, got %v", err)
	}

	// 5. OnCreateAtespace links parent_global -> alice inherits can_get, can_update, can_delete.
	if err := srv.OnCreateAtespace(ctx, "team-x"); err != nil {
		t.Fatalf("OnCreateAtespace failed: %v", err)
	}
	for _, rel := range []string{RelationCanGet, RelationCanUpdate, RelationCanDelete} {
		if err := srv.CheckPermission(aliceCtx, rel, AtespaceObject("team-x")); err != nil {
			t.Errorf("expected alice allowed %s on team-x after OnCreateAtespace, got %v", rel, err)
		}
		if err := srv.CheckPermission(bobCtx, rel, AtespaceObject("team-x")); status.Code(err) != codes.PermissionDenied {
			t.Errorf("expected bob denied %s on team-x, got %v", rel, err)
		}
	}

	// 6. ListAccessibleAtespaces for global owner vs unprivileged user.
	all, _, err := srv.ListAccessibleAtespaces(aliceCtx)
	if err != nil || !all {
		t.Errorf("expected alice ListAccessibleAtespaces all=true, got all=%t err=%v", all, err)
	}
	if _, _, err := srv.ListAccessibleAtespaces(bobCtx); status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected unprivileged bob ListAccessibleAtespaces PermissionDenied, got %v", err)
	}

	// Grant bob direct editor access on team-x.
	_, err = srv.FGAServer().Write(ctx, &openfgav1.WriteRequest{
		StoreId:              srv.StoreID(),
		AuthorizationModelId: srv.ModelID(),
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{
					User:     "user:bob",
					Relation: "editor",
					Object:   AtespaceObject("team-x"),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Write bob editor tuple failed: %v", err)
	}
	if err := srv.CheckPermission(bobCtx, RelationCanUpdate, AtespaceObject("team-x")); err != nil {
		t.Errorf("expected bob allowed can_update on team-x, got %v", err)
	}

	// Now bob's ListAccessibleAtespaces returns all=false and allowedObjects={"atespace:team-x": true}.
	all, allowedObjs, err := srv.ListAccessibleAtespaces(bobCtx)
	if err != nil || all || !allowedObjs[AtespaceObject("team-x")] {
		t.Errorf("expected bob ListAccessibleAtespaces scoped to team-x, got all=%t allowed=%v err=%v", all, allowedObjs, err)
	}

	// 7. OnDeleteAtespace removes ALL tuples on team-x (parent_global AND bob's direct editor tuple).
	if err := srv.OnDeleteAtespace(ctx, "team-x"); err != nil {
		t.Fatalf("OnDeleteAtespace failed: %v", err)
	}
	if err := srv.CheckPermission(bobCtx, RelationCanUpdate, AtespaceObject("team-x")); status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected bob's direct tuple removed after OnDeleteAtespace, got %v", err)
	}
	if _, _, err := srv.ListAccessibleAtespaces(bobCtx); status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected bob ListAccessibleAtespaces PermissionDenied after OnDeleteAtespace, got %v", err)
	}

	// 8. Simulate orphaned tuples (e.g. DB row deleted but OnDeleteAtespace failed):
	// Write an orphaned editor tuple for bob on team-x, then call OnCreateAtespace(team-x)
	// to verify that recreating the atespace purges bob's orphaned tuple.
	_, err = srv.FGAServer().Write(ctx, &openfgav1.WriteRequest{
		StoreId:              srv.StoreID(),
		AuthorizationModelId: srv.ModelID(),
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{
					User:     "user:bob",
					Relation: "editor",
					Object:   AtespaceObject("team-x"),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Write orphaned tuple failed: %v", err)
	}
	if err := srv.OnCreateAtespace(ctx, "team-x"); err != nil {
		t.Fatalf("OnCreateAtespace failed: %v", err)
	}
	if err := srv.CheckPermission(bobCtx, RelationCanUpdate, AtespaceObject("team-x")); status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected OnCreateAtespace to purge bob's orphaned tuple, got %v", err)
	}
}
