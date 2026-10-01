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
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/language/pkg/go/transformer"
	"github.com/openfga/openfga/pkg/server"
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

	RelationCanCreateAtespace = "can_create_atespace"
	RelationCanListAtespaces  = "can_list_atespaces"
	RelationCanGet            = "can_get"
	RelationCanDelete         = "can_delete"

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

//go:embed model.fga
var modelDSL string

// NewOpenFGAServer creates an embedded OpenFGA server backed by a transaction-aware
// PostgreSQL datastore on pool. Calling Close on the returned server stops
// OpenFGA's background workers without closing the caller-owned pool.
func NewOpenFGAServer(pool *pgxpool.Pool) (*server.Server, error) {
	if pool == nil {
		return nil, fmt.Errorf("postgres pool must not be nil")
	}
	rawDatastore, err := postgres.NewWithDB(pool, nil, sqlcommon.NewConfig())
	if err != nil {
		return nil, fmt.Errorf("creating OpenFGA postgres adapter: %w", err)
	}
	fgaServer, err := server.NewServerWithOpts(
		server.WithDatastore(newTransactionalDatastore(rawDatastore)),
	)
	if err != nil {
		return nil, fmt.Errorf("creating OpenFGA server: %w", err)
	}
	return fgaServer, nil
}

// EnsureStoreAndModel ensures the default OpenFGA store and checked-in authorization model
// are provisioned on fgaServer (serialized across replicas via a PostgreSQL
// advisory lock on pool) and returns the provisioned (storeID, modelID).
func EnsureStoreAndModel(ctx context.Context, pool *pgxpool.Pool, fgaServer *server.Server) (string, string, error) {
	if pool == nil {
		return "", "", fmt.Errorf("postgres pool must not be nil")
	}
	if fgaServer == nil {
		return "", "", fmt.Errorf("fgaServer must not be nil")
	}

	unlock, err := acquireInitLock(ctx, pool)
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
// PolicyManager.
func New(ctx context.Context, pool *pgxpool.Pool, fgaServer *server.Server) (*Authorizer, *PolicyManager, error) {
	storeID, modelID, err := EnsureStoreAndModel(ctx, pool, fgaServer)
	if err != nil {
		return nil, nil, err
	}
	authorizer := &Authorizer{
		fgaServer: fgaServer,
		storeID:   storeID,
		modelID:   modelID,
	}
	policyManager := &PolicyManager{
		fgaServer: fgaServer,
		storeID:   storeID,
		modelID:   modelID,
	}
	return authorizer, policyManager, nil
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
