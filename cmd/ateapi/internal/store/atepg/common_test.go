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

package atepg

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/authz"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/installdefaults"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// clearAll truncates every table so the next test starts from an empty store
// without paying for a fresh database. Nothing in production mass-deletes
// state, so the statement lives here rather than on Persistence.
func clearAll(t *testing.T, p *Persistence) {
	t.Helper()
	if _, err := p.pool.Exec(context.Background(), `TRUNCATE atespaces, global_access_policy, atespace_access_policies, actors, actor_egress_policies, actor_templates, tags, workers, worker_assignments, leases, worker_outbox, worker_outbox_trim, tuple, changelog`); err != nil {
		t.Fatalf("truncating tables: %v", err)
	}
}

func setupPostgresPersistence(t *testing.T) *Persistence {
	t.Helper()
	ctx := context.Background()
	p, err := NewPersistence(ctx, requirePool(t))
	if err != nil {
		t.Fatalf("NewPersistence failed: %v", err)
	}
	t.Cleanup(p.Close)
	clearAll(t, p)
	setTestPolicyManager(t, p)
	return p
}

// setTestPolicyManager gives p an OpenFGA-backed PolicyManager, as the server
// always does.
func setTestPolicyManager(t *testing.T, p *Persistence) {
	t.Helper()
	fgaServer, err := authz.NewOpenFGAServer(p.pool)
	if err != nil {
		t.Fatalf("NewOpenFGAServer failed: %v", err)
	}
	t.Cleanup(fgaServer.Close)
	_, policyManager, err := authz.New(t.Context(), p.pool, fgaServer, nil, authz.SystemPrincipals{
		ControllerSPIFFEID:       installdefaults.ControllerSPIFFEID(installdefaults.SystemNamespace),
		EgressGatewaySPIFFEIDs:   []string{installdefaults.EgressSPIFFEID(installdefaults.SystemNamespace)},
		IngressRouterSPIFFEID:    installdefaults.RouterSPIFFEID(installdefaults.SystemNamespace),
		AteletSPIFFEID:           installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace),
		APIServerSidecarSPIFFEID: installdefaults.APIServerSPIFFEID(installdefaults.SystemNamespace),
	})
	if err != nil {
		t.Fatalf("authz.New failed: %v", err)
	}
	p.SetPolicyManager(policyManager)
}

func newTestAtespace(name string) *ateapi.Atespace {
	return &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: name}}
}

func createTestAtespace(t *testing.T, s *Persistence, name string) {
	t.Helper()
	if _, err := s.CreateAtespace(context.Background(), newTestAtespace(name)); err != nil {
		t.Fatalf("CreateAtespace(%q) failed: %v", name, err)
	}
}

// metadataEdits are mutate closures that try to change metadata the store owns.
var metadataEdits = map[string]func(obj hasMetadata){
	"changed": func(obj hasMetadata) {
		md := obj.GetMetadata()
		md.Name = "other"
		md.Atespace = "other"
		md.Uid = "other-uid"
		md.Version = 99
		md.CreateTime = nil
		md.DeleteTime = timestamppb.Now()
	},
	"cleared": func(obj hasMetadata) {
		m := obj.ProtoReflect()
		m.Clear(m.Descriptor().Fields().ByName("metadata"))
	},
}

type hasMetadata interface {
	proto.Message
	GetMetadata() *ateapi.ResourceMetadata
}

// testUpdateDiscardsMetadataEdits creates an object with create, updates it
// with each of metadataEdits, and checks that get returns the created metadata
// at version 2. Atespace team-a exists before create runs.
func testUpdateDiscardsMetadataEdits[T hasMetadata](
	t *testing.T,
	create func(t *testing.T, s *Persistence) T,
	update func(ctx context.Context, s *Persistence, precondition store.Precondition, mutate func(T) error) (T, error),
	get func(ctx context.Context, s *Persistence) (T, error),
) {
	t.Helper()
	for name, edit := range metadataEdits {
		t.Run(name, func(t *testing.T) {
			s := setupPostgresPersistence(t)
			ctx := t.Context()
			createTestAtespace(t, s, "team-a")
			created := create(t, s)
			if _, err := update(ctx, s, store.PreconditionFrom(created), func(obj T) error {
				edit(obj)
				return nil
			}); err != nil {
				t.Fatalf("update failed: %v", err)
			}
			got, err := get(ctx, s)
			if err != nil {
				t.Fatalf("get failed: %v", err)
			}
			want := proto.CloneOf(created.GetMetadata())
			want.Version = 2
			if diff := cmp.Diff(want, got.GetMetadata(), protocmp.Transform(), protocmp.IgnoreFields(&ateapi.ResourceMetadata{}, "update_time")); diff != "" {
				t.Errorf("stored metadata (-want +got):\n%s", diff)
			}
		})
	}
}
