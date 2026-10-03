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
	"database/sql"
	_ "embed"
	"fmt"
	"log/slog"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgxpool"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/language/pkg/go/transformer"
	"github.com/openfga/openfga/pkg/server"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	// defaultStoreName is the name of the OpenFGA store managed by Substrate.
	defaultStoreName = "substrate"

	// GlobalRootObject is the singleton global scope object identifier in OpenFGA.
	GlobalRootObject = "global:root"

	RoleOwner  = "owner"
	RoleEditor = "editor"
	RoleViewer = "viewer"

	RelationCanCreateAtespace     = "can_create_atespace"
	RelationCanListAtespaces      = "can_list_atespaces"
	RelationCanGet                = "can_get"
	RelationCanDelete             = "can_delete"
	RelationCanCreateAccessPolicy = "can_create_access_policy"
	RelationCanGetAccessPolicy    = "can_get_access_policy"
	RelationCanUpdateAccessPolicy = "can_update_access_policy"
	RelationCanDeleteAccessPolicy = "can_delete_access_policy"

	// maxTuplesPerWrite is OpenFGA's default maximum number of tuples allowed in a single Write request.
	maxTuplesPerWrite = 100
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

// formatUser formats a principal ID as a valid OpenFGA user string.
// OpenFGA disallows ':', '#', whitespace, and treats '*' as a public wildcard;
// these characters (plus '%') are percent-encoded to prevent collisions and
// wildcard injection while preserving '/', '@', '.', '-', and '_'.
func formatUser(id string) string {
	return "user:" + tupleReplacer.Replace(id)
}

// maxTupleUserLength is the longest OpenFGA user string a tuple can hold:
// OpenFGA's MySQL schema stores it in VARCHAR(256). PostgreSQL has no limit,
// but both backends enforce this one so a policy valid on one is valid on the
// other.
const maxTupleUserLength = 256

// FormatMember validates a policy member string (such as "user:alice@example.com")
// and returns the percent-encoded OpenFGA user string. Control characters are
// rejected because OpenFGA does not accept them in tuple user IDs.
func FormatMember(member string) (string, error) {
	id, ok := strings.CutPrefix(member, "user:")
	if !ok || strings.TrimSpace(id) == "" {
		return "", fmt.Errorf("member %q must have non-empty \"user:<id>\" format", member)
	}
	if id == "*" {
		return "", fmt.Errorf("wildcard member %q is not allowed", member)
	}
	if strings.ContainsFunc(id, unicode.IsControl) {
		return "", fmt.Errorf("member %q must not contain control characters", member)
	}
	user := formatUser(id)
	if n := utf8.RuneCountInString(user); n > maxTupleUserLength {
		return "", fmt.Errorf("member %q is %d characters once encoded, longer than %d", member, n, maxTupleUserLength)
	}
	return user, nil
}

//go:embed model.fga
var modelDSL string

// Backend is the store database that holds the OpenFGA tables. Build it with
// PostgresBackend or MySQLBackend; the zero value names no database.
type Backend struct {
	pool *pgxpool.Pool
	db   *sql.DB
}

// PostgresBackend names the PostgreSQL store's connection pool.
func PostgresBackend(pool *pgxpool.Pool) Backend { return Backend{pool: pool} }

// MySQLBackend names the MySQL store's connection pool.
func MySQLBackend(db *sql.DB) Backend { return Backend{db: db} }

// NewOpenFGAServer creates an embedded OpenFGA server backed by a
// transaction-aware datastore on the backend's pool. Calling Close on the
// returned server stops OpenFGA's background workers without closing the
// caller-owned pool.
func NewOpenFGAServer(backend Backend) (*server.Server, error) {
	var datastore storage.OpenFGADatastore
	switch {
	case backend.pool != nil:
		rawDatastore, err := postgres.NewWithDB(backend.pool, nil, sqlcommon.NewConfig())
		if err != nil {
			return nil, fmt.Errorf("creating OpenFGA postgres adapter: %w", err)
		}
		datastore = newTransactionalDatastore(rawDatastore)
	case backend.db != nil:
		mysqlDatastore, err := newMySQLTransactionalDatastore(backend.db)
		if err != nil {
			return nil, err
		}
		datastore = mysqlDatastore
	default:
		return nil, fmt.Errorf("store backend must not be empty")
	}
	fgaServer, err := server.NewServerWithOpts(
		server.WithDatastore(datastore),
	)
	if err != nil {
		return nil, fmt.Errorf("creating OpenFGA server: %w", err)
	}
	return fgaServer, nil
}

// EnsureStoreAndModel ensures the default OpenFGA store and checked-in authorization model
// are provisioned on fgaServer (serialized across replicas via a database lock
// on the backend) and returns the provisioned (storeID, modelID).
func EnsureStoreAndModel(ctx context.Context, backend Backend, fgaServer *server.Server) (string, string, error) {
	if backend.pool == nil && backend.db == nil {
		return "", "", fmt.Errorf("store backend must not be empty")
	}
	if fgaServer == nil {
		return "", "", fmt.Errorf("fgaServer must not be nil")
	}

	unlock, err := acquireInitLock(ctx, backend)
	if err != nil {
		return "", "", err
	}
	defer unlock()

	storeID, modelID, err := ensureStoreAndModel(ctx, fgaServer)
	if err != nil {
		return "", "", fmt.Errorf("initializing OpenFGA store and model: %w", err)
	}

	slog.InfoContext(ctx, "OpenFGA store and model ready",
		slog.String("store_id", storeID),
		slog.String("model_id", modelID),
	)

	return storeID, modelID, nil
}

// New provisions the default OpenFGA store and authorization model via
// EnsureStoreAndModel and returns the read-path Authorizer and write-path
// PolicyManager. bootstrapOwners are principal IDs (with or without the
// "user:" prefix) that the Authorizer treats as owners of global:root on every
// check, independent of any stored AccessPolicy.
func New(ctx context.Context, backend Backend, fgaServer *server.Server, bootstrapOwners []string) (*Authorizer, *PolicyManager, error) {
	owners, err := parseBootstrapOwners(bootstrapOwners)
	if err != nil {
		return nil, nil, err
	}
	storeID, modelID, err := EnsureStoreAndModel(ctx, backend, fgaServer)
	if err != nil {
		return nil, nil, err
	}
	authorizer := &Authorizer{
		fgaServer:       fgaServer,
		storeID:         storeID,
		modelID:         modelID,
		bootstrapOwners: owners,
	}
	policyManager := &PolicyManager{
		fgaServer: fgaServer,
		storeID:   storeID,
		modelID:   modelID,
	}
	return authorizer, policyManager, nil
}

// parseBootstrapOwners validates principal IDs and returns the set of their
// OpenFGA user strings.
func parseBootstrapOwners(ids []string) (map[string]struct{}, error) {
	owners := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		user, err := FormatMember("user:" + strings.TrimPrefix(strings.TrimSpace(id), "user:"))
		if err != nil {
			return nil, fmt.Errorf("invalid bootstrap owner %q: %w", id, err)
		}
		owners[user] = struct{}{}
	}
	return owners, nil
}

// ateFGAInitLockID is a 64-bit identifier ("atefga") for serializing
// OpenFGA store provisioning across replicas.
const ateFGAInitLockID = int64(0x6174656667610000) // "atefga\0\0"

func acquireInitLock(ctx context.Context, backend Backend) (func(), error) {
	if backend.db != nil {
		return acquireMySQLInitLock(ctx, backend.db)
	}
	conn, err := backend.pool.Acquire(ctx)
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

// ensureStoreAndModel compiles the embedded model.fga DSL into an OpenFGA proto,
// finds or creates the default store, and ensures the authorization model matches
// the current schema. If an identical model already exists in the store, its ID
// is reused; otherwise, the new model is written and its ID is returned.
func ensureStoreAndModel(ctx context.Context, srv *server.Server) (string, string, error) {
	modelProto, err := transformer.TransformDSLToProto(modelDSL)
	if err != nil {
		return "", "", fmt.Errorf("transform model.fga DSL to proto: %w", err)
	}

	storeID, err := findOrCreateStore(ctx, srv, defaultStoreName)
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
