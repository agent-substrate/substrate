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
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/language/pkg/go/transformer"
	"github.com/openfga/openfga/pkg/server"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
)

const (
	// defaultStoreName is the name of the OpenFGA store managed by Substrate.
	defaultStoreName = "substrate"

	// GlobalRootObject is the singleton global scope object identifier in OpenFGA.
	GlobalRootObject = "global:root"

	RoleOwner  = "owner"
	RoleEditor = "editor"
	RoleViewer = "viewer"

	RoleController       = "controller"
	RoleEgressGateway    = "egress_gateway"
	RoleIngressRouter    = "ingress_router"
	RoleAtelet           = "atelet"
	RoleAPIServerSidecar = "api_server_sidecar"

	RelationCanCreateAtespace       = "can_create_atespace"
	RelationCanListAtespaces        = "can_list_atespaces"
	RelationCanCreateActor          = "can_create_actor"
	RelationCanListActors           = "can_list_actors"
	RelationCanCreateActorTemplate  = "can_create_actor_template"
	RelationCanListActorTemplates   = "can_list_actor_templates"
	RelationCanCreateWorker         = "can_create_worker"
	RelationCanListWorkers          = "can_list_workers"
	RelationCanGet                  = "can_get"
	RelationCanUseTemplate          = "can_use_template"
	RelationCanUpdate               = "can_update"
	RelationCanDelete               = "can_delete"
	RelationCanResume               = "can_resume"
	RelationCanSuspend              = "can_suspend"
	RelationCanRevert               = "can_revert"
	RelationCanDrain                = "can_drain"
	RelationCanListActorAssignments = "can_list_actor_assignments"
	RelationCanCreateAccessPolicy   = "can_create_access_policy"
	RelationCanGetAccessPolicy      = "can_get_access_policy"
	RelationCanUpdateAccessPolicy   = "can_update_access_policy"
	RelationCanDeleteAccessPolicy   = "can_delete_access_policy"
	RelationCanCreateEgressPolicy   = "can_create_egress_policy"
	RelationCanGetEgressPolicy      = "can_get_egress_policy"
	RelationCanUpdateEgressPolicy   = "can_update_egress_policy"
	RelationCanDeleteEgressPolicy   = "can_delete_egress_policy"
	RelationCanCreateTag            = "can_create_tag"
	RelationCanListTags             = "can_list_tags"
	RelationCanUseTag               = "can_use_tag"
	RelationCanMintActorJWT         = "can_mint_actor_jwt"

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

// objectIDReplacer escapes resource names in OpenFGA object IDs. Unlike
// tupleReplacer it also escapes '/', which separates the atespace from the
// name in atespaced object IDs, so each ID names exactly one resource.
var objectIDReplacer = strings.NewReplacer(
	"%", "%25",
	":", "%3A",
	"#", "%23",
	" ", "%20",
	"*", "%2A",
	"/", "%2F",
)

// AtespaceObject formats an atespace name as an OpenFGA object string.
func AtespaceObject(name string) string {
	return "atespace:" + objectIDReplacer.Replace(name)
}

// WorkerObject formats a worker name as an OpenFGA object string. Workers are
// global-scoped.
func WorkerObject(name string) string {
	return "worker:" + objectIDReplacer.Replace(name)
}

// ActorObject formats an actor as an OpenFGA object string.
func ActorObject(atespace, name string) string {
	return "actor:" + atespacedID(atespace, name)
}

// ActorTemplateObject formats an actor template as an OpenFGA object string.
func ActorTemplateObject(atespace, name string) string {
	return "actor_template:" + atespacedID(atespace, name)
}

// TagObject formats a tag as an OpenFGA object string.
func TagObject(atespace, name string) string {
	return "tag:" + atespacedID(atespace, name)
}

// atespacedID formats the object ID of an atespaced resource as
// "<atespace>/<name>". contextualTuples parses the atespace back out of it.
func atespacedID(atespace, name string) string {
	return objectIDReplacer.Replace(atespace) + "/" + objectIDReplacer.Replace(name)
}

// formatUser formats a principal ID as a valid OpenFGA user string.
// OpenFGA disallows ':', '#', whitespace, and treats '*' as a public wildcard;
// these characters (plus '%') are percent-encoded to prevent collisions and
// wildcard injection while preserving '/', '@', '.', '-', and '_'.
func formatUser(id string) string {
	return "user:" + tupleReplacer.Replace(id)
}

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
	return formatUser(id), nil
}

//go:embed model.fga
var modelDSL string

// NewOpenFGAServer creates an embedded OpenFGA server backed by ds. The store
// backend provides ds, which must write tuples in the store's transaction
// carried by ctx. Calling Close on the returned server closes ds.
func NewOpenFGAServer(ds storage.OpenFGADatastore) (*server.Server, error) {
	if ds == nil {
		return nil, fmt.Errorf("datastore must not be nil")
	}
	fgaServer, err := server.NewServerWithOpts(server.WithDatastore(ds))
	if err != nil {
		return nil, fmt.Errorf("creating OpenFGA server: %w", err)
	}
	return fgaServer, nil
}

// InitLocker serializes OpenFGA store provisioning across replicas.
// store.Interface implements it.
type InitLocker interface {
	AcquireLease(ctx context.Context, key string) (*store.Lease, error)
}

// EnsureStoreAndModel ensures the default OpenFGA store and checked-in authorization model
// are provisioned on fgaServer (serialized across replicas by a lease from
// initLock) and returns the provisioned (storeID, modelID).
func EnsureStoreAndModel(ctx context.Context, initLock InitLocker, fgaServer *server.Server) (string, string, error) {
	if initLock == nil {
		return "", "", fmt.Errorf("initLock must not be nil")
	}
	if fgaServer == nil {
		return "", "", fmt.Errorf("fgaServer must not be nil")
	}

	lease, err := acquireInitLock(ctx, initLock)
	if err != nil {
		return "", "", err
	}
	defer lease.Close()

	storeID, modelID, err := ensureStoreAndModel(lease.Context(), fgaServer)
	if err != nil {
		return "", "", fmt.Errorf("initializing OpenFGA store and model: %w", err)
	}

	slog.InfoContext(ctx, "OpenFGA store and model ready",
		slog.String("store_id", storeID),
		slog.String("model_id", modelID),
	)

	return storeID, modelID, nil
}

// SystemPrincipals holds the mTLS SPIFFE IDs of Substrate's own components.
// The Authorizer grants each one its system component role on global:root on
// every check, independent of any stored AccessPolicy.
type SystemPrincipals struct {
	ControllerSPIFFEID       string
	EgressGatewaySPIFFEIDs   []string
	IngressRouterSPIFFEID    string
	AteletSPIFFEID           string
	APIServerSidecarSPIFFEID string
}

// New provisions the default OpenFGA store and authorization model via
// EnsureStoreAndModel and returns the read-path Authorizer and write-path
// PolicyManager. bootstrapOwners are principal IDs (with or without the
// "user:" prefix) that the Authorizer treats as owners of global:root on every
// check, independent of any stored AccessPolicy.
func New(ctx context.Context, initLock InitLocker, fgaServer *server.Server, bootstrapOwners []string, system SystemPrincipals) (*Authorizer, *PolicyManager, error) {
	owners, err := parseBootstrapOwners(bootstrapOwners)
	if err != nil {
		return nil, nil, err
	}
	systemPrincipals, err := parseSystemPrincipals(system)
	if err != nil {
		return nil, nil, err
	}
	storeID, modelID, err := EnsureStoreAndModel(ctx, initLock, fgaServer)
	if err != nil {
		return nil, nil, err
	}
	authorizer := &Authorizer{
		fgaServer:        fgaServer,
		storeID:          storeID,
		modelID:          modelID,
		bootstrapOwners:  owners,
		systemPrincipals: systemPrincipals,
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

// parseSystemPrincipals validates system's SPIFFE IDs and maps each OpenFGA
// user string to the system component roles it holds.
func parseSystemPrincipals(system SystemPrincipals) (map[string][]string, error) {
	if len(system.EgressGatewaySPIFFEIDs) == 0 {
		return nil, fmt.Errorf("no %s SPIFFE ID", RoleEgressGateway)
	}
	type binding struct {
		role     string
		spiffeID string
	}
	bindings := []binding{
		{role: RoleController, spiffeID: system.ControllerSPIFFEID},
		{role: RoleIngressRouter, spiffeID: system.IngressRouterSPIFFEID},
		{role: RoleAtelet, spiffeID: system.AteletSPIFFEID},
		{role: RoleAPIServerSidecar, spiffeID: system.APIServerSidecarSPIFFEID},
	}
	for _, id := range system.EgressGatewaySPIFFEIDs {
		bindings = append(bindings, binding{role: RoleEgressGateway, spiffeID: id})
	}
	out := make(map[string][]string, len(bindings))
	for _, b := range bindings {
		if _, err := spiffeid.FromString(b.spiffeID); err != nil {
			return nil, fmt.Errorf("invalid %s SPIFFE ID %q: %w", b.role, b.spiffeID, err)
		}
		user, err := FormatMember("user:" + b.spiffeID)
		if err != nil {
			return nil, fmt.Errorf("invalid %s SPIFFE ID %q: %w", b.role, b.spiffeID, err)
		}
		out[user] = append(out[user], b.role)
	}
	return out, nil
}

const (
	// initLeaseKey is the lease that serializes OpenFGA store provisioning.
	initLeaseKey = "lease:openfga:init"
	// initLeaseRetryPeriod is how often a replica retries the lease while
	// another replica provisions.
	initLeaseRetryPeriod = time.Second
)

// acquireInitLock blocks until it holds the init lease or ctx ends. If the
// holder dies, the others wait for its lease to expire.
func acquireInitLock(ctx context.Context, initLock InitLocker) (*store.Lease, error) {
	for {
		lease, err := initLock.AcquireLease(ctx, initLeaseKey)
		if err == nil {
			return lease, nil
		}
		if !errors.Is(err, store.ErrLeaseConflict) {
			return nil, fmt.Errorf("acquiring OpenFGA init lease: %w", err)
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("waiting for OpenFGA init lease: %w", ctx.Err())
		case <-time.After(initLeaseRetryPeriod):
		}
	}
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
