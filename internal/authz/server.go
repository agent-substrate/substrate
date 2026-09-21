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
	_ "embed"
	"fmt"
	"io/fs"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/language/pkg/go/transformer"
	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/pkg/server"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	// DefaultStoreName is the name of the OpenFGA store managed by Substrate.
	DefaultStoreName = "substrate"

	// migrationTableName tracks OpenFGA schema migrations separately from
	// Substrate's own schema_migrations table.
	migrationTableName = "goose_db_version"
)

//go:embed model.fga
var modelDSL string

// Server wraps an embedded OpenFGA server backed by PostgreSQL and
// initialized with Substrate's authorization model.
type Server struct {
	closeOnce sync.Once
	fgaServer *server.Server
	datastore storage.OpenFGADatastore
	storeID   string
	modelID   string
}

// NewServer initializes OpenFGA database migrations on pool, constructs the
// PostgreSQL storage adapter, creates the OpenFGA server, and ensures the
// default store and checked-in authorization model are present.
//
// NewServer takes ownership of pool: calling Close on the returned Server (or
// an error during NewServer initialization) closes pool. Callers must provide
// a dedicated pool rather than a shared pool.
func NewServer(ctx context.Context, pool *pgxpool.Pool) (*Server, error) {
	if pool == nil {
		return nil, fmt.Errorf("postgres pool must not be nil")
	}

	// Ensure OpenFGA database tables (tuple, store, authorization_model, changelog)
	// are migrated and ready in PostgreSQL before initializing the storage adapter.
	// Goose uses PostgresSessionLocker to serialize migrations safely across replicas.
	if err := applyMigrations(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("applying OpenFGA migrations: %w", err)
	}

	cfg := sqlcommon.NewConfig()
	datastore, err := postgres.NewWithDB(pool, nil, cfg)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("creating OpenFGA postgres adapter: %w", err)
	}

	fgaServer, err := server.NewServerWithOpts(
		server.WithDatastore(datastore),
	)
	if err != nil {
		datastore.Close()
		return nil, fmt.Errorf("creating OpenFGA server: %w", err)
	}

	unlock, err := acquireInitLock(ctx, pool)
	if err != nil {
		fgaServer.Close()
		datastore.Close()
		return nil, err
	}

	storeID, modelID, err := ensureStoreAndModel(ctx, fgaServer)
	unlock()
	if err != nil {
		fgaServer.Close()
		datastore.Close()
		return nil, fmt.Errorf("initializing OpenFGA store and model: %w", err)
	}

	slog.InfoContext(ctx, "OpenFGA server initialized",
		slog.String("store_id", storeID),
		slog.String("model_id", modelID),
	)

	return &Server{
		fgaServer: fgaServer,
		datastore: datastore,
		storeID:   storeID,
		modelID:   modelID,
	}, nil
}

// FGAServer returns the underlying OpenFGA server instance.
func (s *Server) FGAServer() *server.Server {
	return s.fgaServer
}

// StoreID returns the active OpenFGA store ID.
func (s *Server) StoreID() string {
	return s.storeID
}

// ModelID returns the active OpenFGA authorization model ID.
func (s *Server) ModelID() string {
	return s.modelID
}

// Close releases resources held by the OpenFGA server and datastore.
// Calling Close multiple times is safe and idempotent.
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		if s.fgaServer != nil {
			s.fgaServer.Close()
		}
		if s.datastore != nil {
			s.datastore.Close()
		}
	})
}

// ateFGAInitLockID is a 64-bit identifier ("atefga") for serializing
// OpenFGA store provisioning across replicas.
const ateFGAInitLockID = int64(0x6174656667610000) // "atefga\0\0"

func acquireInitLock(ctx context.Context, pool *pgxpool.Pool) (func(), error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring connection for OpenFGA init lock: %w", err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, ateFGAInitLockID); err != nil {
		conn.Release()
		return nil, fmt.Errorf("acquiring OpenFGA init advisory lock: %w", err)
	}
	return func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, ateFGAInitLockID)
		conn.Release()
	}, nil
}

// applyMigrations runs OpenFGA's embedded PostgreSQL migrations against pool
// using Goose, tracking applied migration versions in the goose_db_version table.
func applyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	migrations, err := fs.Sub(assets.EmbedMigrations, assets.PostgresMigrationDir)
	if err != nil {
		return fmt.Errorf("open embedded OpenFGA migrations: %w", err)
	}

	locker, err := lock.NewPostgresSessionLocker(
		lock.WithLockID(ateFGAInitLockID),
		lock.WithLockTimeout(1, 300),
	)
	if err != nil {
		return fmt.Errorf("create OpenFGA migration locker: %w", err)
	}

	db := stdlib.OpenDBFromPool(pool)
	provider, err := goose.NewProvider(
		goose.DialectPostgres,
		db,
		migrations,
		goose.WithTableName(migrationTableName),
		goose.WithSessionLocker(locker),
	)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("create OpenFGA migration provider: %w", err)
	}

	_, err = provider.Up(ctx)
	closeErr := provider.Close()
	if err != nil {
		return fmt.Errorf("run OpenFGA migrations: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close OpenFGA migration provider: %w", closeErr)
	}
	return nil
}

// ensureStoreAndModel compiles the embedded model.fga DSL into an OpenFGA proto,
// finds or creates the default store, and ensures the authorization model matches
// the current schema. If an identical model already exists in the store, its ID
// is reused; otherwise, the new model is written and its ID is returned.
func ensureStoreAndModel(ctx context.Context, srv *server.Server) (string, string, error) {
	modelProto, err := transformer.TransformDSLToProto(modelDSL)
	if err != nil {
		return "", "", fmt.Errorf("transform model.fga DSL to proto: %w", err)
	}

	storeID, err := findOrCreateStore(ctx, srv, DefaultStoreName)
	if err != nil {
		return "", "", err
	}

	modelsResp, err := srv.ReadAuthorizationModels(ctx, &openfgav1.ReadAuthorizationModelsRequest{
		StoreId:  storeID,
		PageSize: wrapperspb.Int32(1),
	})
	if err != nil {
		return "", "", fmt.Errorf("read existing authorization models: %w", err)
	}

	if len(modelsResp.GetAuthorizationModels()) > 0 {
		latest := modelsResp.GetAuthorizationModels()[0]
		if modelsEqual(latest, modelProto) {
			return storeID, latest.GetId(), nil
		}
	}

	writeResp, err := srv.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   modelProto.GetSchemaVersion(),
		TypeDefinitions: modelProto.GetTypeDefinitions(),
		Conditions:      modelProto.GetConditions(),
	})
	if err != nil {
		return "", "", fmt.Errorf("write authorization model: %w", err)
	}

	return storeID, writeResp.GetAuthorizationModelId(), nil
}

// findOrCreateStore looks up an existing OpenFGA store by name across all pages.
// If found, its existing store ID is returned to ensure idempotency across restarts.
// If no store with the given name exists, a new store is created and returned.
func findOrCreateStore(ctx context.Context, srv *server.Server, name string) (string, error) {
	var continuationToken string
	for {
		listResp, err := srv.ListStores(ctx, &openfgav1.ListStoresRequest{
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return "", fmt.Errorf("list stores: %w", err)
		}
		for _, st := range listResp.GetStores() {
			if st.GetName() == name {
				return st.GetId(), nil
			}
		}
		if listResp.GetContinuationToken() == "" {
			break
		}
		continuationToken = listResp.GetContinuationToken()
	}

	createResp, err := srv.CreateStore(ctx, &openfgav1.CreateStoreRequest{
		Name: name,
	})
	if err != nil {
		return "", fmt.Errorf("create store %q: %w", name, err)
	}
	return createResp.GetId(), nil
}

func modelsEqual(existing, desired *openfgav1.AuthorizationModel) bool {
	if existing.GetSchemaVersion() != desired.GetSchemaVersion() {
		return false
	}
	a := &openfgav1.AuthorizationModel{
		SchemaVersion:   existing.GetSchemaVersion(),
		TypeDefinitions: existing.GetTypeDefinitions(),
		Conditions:      existing.GetConditions(),
	}
	b := &openfgav1.AuthorizationModel{
		SchemaVersion:   desired.GetSchemaVersion(),
		TypeDefinitions: desired.GetTypeDefinitions(),
		Conditions:      desired.GetConditions(),
	}
	return proto.Equal(a, b)
}
