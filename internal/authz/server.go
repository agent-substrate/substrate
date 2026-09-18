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

	"github.com/agent-substrate/substrate/internal/principal"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"strings"
)

const (
	// DefaultStoreName is the name of the OpenFGA store managed by Substrate.
	DefaultStoreName = "substrate"

	// migrationTableName tracks OpenFGA schema migrations separately from
	// Substrate's own schema_migrations table.
	migrationTableName = "goose_db_version"

	// GlobalRootObject is the singleton global scope object identifier in OpenFGA.
	GlobalRootObject = "global:root"

	RelationCanCreateAtespace = "can_create_atespace"
	RelationCanListAtespaces  = "can_list_atespaces"
	RelationCanGet            = "can_get"
	RelationCanUpdate         = "can_update"
	RelationCanDelete         = "can_delete"
	RelationCanSetPolicy      = "can_set_policy"
)

type bypassKey struct{}

// WithBypass returns a context that bypasses runtime authorization checks (for internal system reconcilers).
func WithBypass(ctx context.Context) context.Context {
	return context.WithValue(ctx, bypassKey{}, true)
}

// IsBypassed reports whether ctx has authorization checks bypassed.
func IsBypassed(ctx context.Context) bool {
	v, _ := ctx.Value(bypassKey{}).(bool)
	return v
}

var tupleReplacer = strings.NewReplacer(
	"%", "%25",
	":", "%3A",
	"#", "%23",
	" ", "%20",
	"*", "%2A",
)

// AtespaceObject formats an atespace name as an OpenFGA object string.
func AtespaceObject(name string) string {
	return "atespace:" + tupleReplacer.Replace(name)
}

// FormatUser formats a principal ID as a valid OpenFGA user string.
// OpenFGA disallows ':', '#', whitespace, and treats '*' as a public wildcard;
// these characters (plus '%') are percent-encoded to prevent collisions and
// wildcard injection while preserving '/', '@', '.', '-', and '_'.
func FormatUser(id string) string {
	id = strings.TrimPrefix(id, "user:")
	return "user:" + tupleReplacer.Replace(id)
}

//go:embed model.fga
var modelDSL string

// Server wraps an embedded OpenFGA server backed by PostgreSQL and
// initialized with Substrate's authorization model.
type Server struct {
	fgaServer *server.Server
	datastore storage.OpenFGADatastore
	storeID   string
	modelID   string
}

// NewServer initializes OpenFGA database migrations on pool, constructs the
// PostgreSQL storage adapter, creates the OpenFGA server, and ensures the
// default store and checked-in authorization model are present.
func NewServer(ctx context.Context, pool *pgxpool.Pool) (*Server, error) {
	if pool == nil {
		return nil, fmt.Errorf("postgres pool must not be nil")
	}

	// Ensure OpenFGA database tables (tuple, store, authorization_model, changelog)
	// are migrated and ready in PostgreSQL before initializing the storage adapter.
	// Goose uses PostgresSessionLocker to serialize migrations safely across replicas.
	if err := applyMigrations(ctx, pool); err != nil {
		return nil, fmt.Errorf("applying OpenFGA migrations: %w", err)
	}

	cfg := sqlcommon.NewConfig()
	datastore, err := postgres.NewWithDB(pool, nil, cfg)
	if err != nil {
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
func (s *Server) Close() {
	if s.fgaServer != nil {
		s.fgaServer.Close()
	}
	if s.datastore != nil {
		s.datastore.Close()
	}
}

func (s *Server) checkRaw(ctx context.Context, user, relation, object string) (bool, error) {
	resp, err := s.fgaServer.Check(ctx, &openfgav1.CheckRequest{
		StoreId:              s.storeID,
		AuthorizationModelId: s.modelID,
		TupleKey: &openfgav1.CheckRequestTupleKey{
			User:     user,
			Relation: relation,
			Object:   object,
		},
	})
	if err != nil {
		return false, status.Errorf(codes.Internal, "authz check failed: %v", err)
	}
	return resp.GetAllowed(), nil
}

// CheckPermission verifies that the principal in ctx has relation on object.
// For atespace:<name> objects that do not yet have a parent_global link in OpenFGA
// (e.g. nonexistent or already-deleted atespaces), callers who hold the corresponding
// global permission on global:root are allowed through so the storage layer can return
// codes.NotFound (or self-heal the tuple if the atespace exists).
func (s *Server) CheckPermission(ctx context.Context, relation, object string) error {
	if s == nil || IsBypassed(ctx) {
		return nil
	}
	p, ok := principal.FromContext(ctx)
	if !ok || p.ID == "" {
		return status.Error(codes.Unauthenticated, "unauthenticated: missing principal in context")
	}
	user := FormatUser(p.ID)
	allowed, err := s.checkRaw(ctx, user, relation, object)
	if err != nil {
		return err
	}
	if allowed {
		return nil
	}

	if strings.HasPrefix(object, "atespace:") {
		var globalRel string
		switch relation {
		case RelationCanGet:
			globalRel = RelationCanGet
		case RelationCanUpdate, RelationCanDelete, RelationCanSetPolicy:
			globalRel = RelationCanCreateAtespace
		}
		if globalRel != "" {
			globalAllowed, gErr := s.checkRaw(ctx, user, globalRel, GlobalRootObject)
			if gErr == nil && globalAllowed {
				return nil
			}
		}
	}

	return status.Errorf(codes.PermissionDenied, "permission denied: principal %q lacks %q on %q", user, relation, object)
}

// ListAccessibleAtespaces determines whether the caller in ctx can list all atespaces
// (via can_list_atespaces on global:root) or returns the set of specific atespace
// objects ("atespace:<name>") on which the caller has can_get permission.
// If the caller has neither global list permission nor access to any atespace,
// it returns codes.PermissionDenied.
func (s *Server) ListAccessibleAtespaces(ctx context.Context) (all bool, allowedObjects map[string]bool, err error) {
	if s == nil || IsBypassed(ctx) {
		return true, nil, nil
	}
	p, ok := principal.FromContext(ctx)
	if !ok || p.ID == "" {
		return false, nil, status.Error(codes.Unauthenticated, "unauthenticated: missing principal in context")
	}
	user := FormatUser(p.ID)

	canListAll, err := s.checkRaw(ctx, user, RelationCanListAtespaces, GlobalRootObject)
	if err != nil {
		return false, nil, err
	}
	if canListAll {
		return true, nil, nil
	}

	listResp, err := s.fgaServer.ListObjects(ctx, &openfgav1.ListObjectsRequest{
		StoreId:              s.storeID,
		AuthorizationModelId: s.modelID,
		User:                 user,
		Relation:             RelationCanGet,
		Type:                 "atespace",
	})
	if err != nil {
		return false, nil, status.Errorf(codes.Internal, "authz list objects failed: %v", err)
	}
	objs := listResp.GetObjects()
	if len(objs) == 0 {
		return false, nil, status.Errorf(codes.PermissionDenied, "permission denied: principal %q lacks %q on %q and has no accessible atespaces", user, RelationCanListAtespaces, GlobalRootObject)
	}
	allowedObjects = make(map[string]bool, len(objs))
	for _, o := range objs {
		allowedObjects[o] = true
	}
	return false, allowedObjects, nil
}

// EnsureParentGlobal idempotently writes the parent_global: global:root tuple for name
// without deleting any existing tuples on the atespace.
func (s *Server) EnsureParentGlobal(ctx context.Context, name string) error {
	if s == nil {
		return nil
	}
	_, err := s.fgaServer.Write(ctx, &openfgav1.WriteRequest{
		StoreId:              s.storeID,
		AuthorizationModelId: s.modelID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{
					User:     GlobalRootObject,
					Relation: "parent_global",
					Object:   AtespaceObject(name),
				},
			},
			OnDuplicate: "ignore",
		},
	})
	if err != nil {
		return fmt.Errorf("writing parent_global tuple for atespace %q: %w", name, err)
	}
	return nil
}

// OnCreateAtespace purges any stale tuples for name from prior lifecycles and links
// the newly created atespace to global:root in OpenFGA.
func (s *Server) OnCreateAtespace(ctx context.Context, name string) error {
	if s == nil {
		return nil
	}
	if err := s.OnDeleteAtespace(ctx, name); err != nil {
		return fmt.Errorf("purging stale tuples before creating atespace %q: %w", name, err)
	}
	return s.EnsureParentGlobal(ctx, name)
}

// BootstrapGlobalOwners idempotently grants the 'owner' relation on 'global:root'
// to each non-empty principal ID in owners.
func (s *Server) BootstrapGlobalOwners(ctx context.Context, owners []string) error {
	if s == nil || len(owners) == 0 {
		return nil
	}
	var keys []*openfgav1.TupleKey
	for _, owner := range owners {
		owner = strings.TrimSpace(owner)
		if owner == "" {
			continue
		}
		keys = append(keys, &openfgav1.TupleKey{
			User:     FormatUser(owner),
			Relation: "owner",
			Object:   GlobalRootObject,
		})
	}
	if len(keys) == 0 {
		return nil
	}
	_, err := s.fgaServer.Write(ctx, &openfgav1.WriteRequest{
		StoreId:              s.storeID,
		AuthorizationModelId: s.modelID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys:   keys,
			OnDuplicate: "ignore",
		},
	})
	if err != nil {
		return fmt.Errorf("bootstrapping global owners in OpenFGA: %w", err)
	}
	return nil
}

// OnDeleteAtespace removes all tuples associated with a deleted atespace in OpenFGA.
func (s *Server) OnDeleteAtespace(ctx context.Context, name string) error {
	if s == nil {
		return nil
	}
	obj := AtespaceObject(name)
	var toDelete []*openfgav1.TupleKeyWithoutCondition
	var contToken string
	for {
		readResp, err := s.fgaServer.Read(ctx, &openfgav1.ReadRequest{
			StoreId:           s.storeID,
			TupleKey:          &openfgav1.ReadRequestTupleKey{Object: obj},
			ContinuationToken: contToken,
		})
		if err != nil {
			return fmt.Errorf("reading tuples for deleted atespace %q: %w", name, err)
		}
		for _, t := range readResp.GetTuples() {
			if tk := t.GetKey(); tk != nil {
				toDelete = append(toDelete, &openfgav1.TupleKeyWithoutCondition{
					User:     tk.GetUser(),
					Relation: tk.GetRelation(),
					Object:   tk.GetObject(),
				})
			}
		}
		if readResp.GetContinuationToken() == "" {
			break
		}
		contToken = readResp.GetContinuationToken()
	}
	if len(toDelete) == 0 {
		return nil
	}
	_, err := s.fgaServer.Write(ctx, &openfgav1.WriteRequest{
		StoreId:              s.storeID,
		AuthorizationModelId: s.modelID,
		Deletes: &openfgav1.WriteRequestDeletes{
			TupleKeys: toDelete,
			OnMissing: "ignore",
		},
	})
	if err != nil {
		return fmt.Errorf("deleting tuples for atespace %q: %w", name, err)
	}
	return nil
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
