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

	"github.com/jackc/pgx/v5/pgxpool"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
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
	testcontainers.SkipIfProviderIsNotHealthy(t)

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

	// Create a second dedicated pool to test idempotent re-initialization after
	// closing the first server (since srv.Close() closes its dedicated pool).
	pool2, err := pgxpool.NewWithConfig(ctx, pool.Config())
	if err != nil {
		t.Fatalf("creating second pgxpool: %v", err)
	}
	srv.Close()

	// Verify idempotent re-initialization reuses the existing store and model.
	srv2, err := NewServer(ctx, pool2)
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
