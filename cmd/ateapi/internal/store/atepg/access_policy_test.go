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

package atepg

import (
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/authz"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestGlobalAccessPolicy_Lifecycle(t *testing.T) {
	p := setupPostgresPersistence(t)
	ctx := t.Context()

	if _, err := p.GetGlobalAccessPolicy(ctx); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetGlobalAccessPolicy before create = %v, want ErrNotFound", err)
	}

	policy := &ateapipb.AccessPolicy{
		Bindings: []*ateapipb.Binding{
			{Role: authz.RoleViewer, Members: []string{"user:bob"}},
			{Role: authz.RoleOwner, Members: []string{"user:alice"}},
		},
	}
	created, err := p.CreateGlobalAccessPolicy(ctx, policy)
	if err != nil {
		t.Fatalf("CreateGlobalAccessPolicy failed: %v", err)
	}
	if created.GetMetadata().GetName() != "default" || created.GetMetadata().GetVersion() != 1 || created.GetMetadata().GetUid() == "" {
		t.Fatalf("unexpected created metadata: %+v", created.GetMetadata())
	}
	if diff := cmp.Diff(policy.GetBindings(), created.GetBindings(), protocmp.Transform()); diff != "" {
		t.Errorf("created bindings (-want +got):\n%s", diff)
	}

	if _, err := p.CreateGlobalAccessPolicy(ctx, policy); !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("second CreateGlobalAccessPolicy = %v, want ErrAlreadyExists", err)
	}

	got, err := p.GetGlobalAccessPolicy(ctx)
	if err != nil || !cmp.Equal(got, created, protocmp.Transform()) {
		t.Fatalf("GetGlobalAccessPolicy = %v, %v; want %v", got, err, created)
	}

	if _, err := p.UpdateGlobalAccessPolicy(ctx, store.Precondition{}, func(*ateapipb.AccessPolicy) error { return nil }); !errors.Is(err, store.ErrPreconditionRequired) {
		t.Fatalf("UpdateGlobalAccessPolicy without precondition = %v, want ErrPreconditionRequired", err)
	}
	if _, err := p.UpdateGlobalAccessPolicy(ctx, store.Precondition{UID: created.GetMetadata().GetUid(), Version: 99}, func(*ateapipb.AccessPolicy) error { return nil }); !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("UpdateGlobalAccessPolicy wrong version = %v, want ErrVersionConflict", err)
	}
	if _, err := p.UpdateGlobalAccessPolicy(ctx, store.Precondition{UID: "wrong-uid", Version: 1}, func(*ateapipb.AccessPolicy) error { return nil }); !errors.Is(err, store.ErrUIDConflict) {
		t.Fatalf("UpdateGlobalAccessPolicy wrong uid = %v, want ErrUIDConflict", err)
	}

	updated, err := p.UpdateGlobalAccessPolicy(ctx, store.PreconditionFrom(created), func(toUpdate *ateapipb.AccessPolicy) error {
		toUpdate.Bindings = []*ateapipb.Binding{
			{Role: authz.RoleOwner, Members: []string{"user:alice", "user:carol"}},
		}
		return nil
	})
	if err != nil || updated.GetMetadata().GetVersion() != 2 {
		t.Fatalf("UpdateGlobalAccessPolicy = %v, %v; want version 2", updated, err)
	}
}

func TestAtespaceAccessPolicy_LifecycleAndCascade(t *testing.T) {
	p := setupPostgresPersistence(t)
	ctx := t.Context()

	policy := &ateapipb.AccessPolicy{
		Bindings: []*ateapipb.Binding{
			{Role: authz.RoleEditor, Members: []string{"user:bob"}},
		},
	}
	if _, err := p.CreateAtespaceAccessPolicy(ctx, "missing-space", policy); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Fatalf("CreateAtespaceAccessPolicy on missing atespace = %v, want ErrFailedPrecondition", err)
	}

	if _, err := p.CreateAtespace(ctx, newTestAtespace("team-a")); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	if _, err := p.GetAtespaceAccessPolicy(ctx, "team-a"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetAtespaceAccessPolicy before create = %v, want ErrNotFound", err)
	}

	created, err := p.CreateAtespaceAccessPolicy(ctx, "team-a", policy)
	if err != nil {
		t.Fatalf("CreateAtespaceAccessPolicy failed: %v", err)
	}
	if _, err := p.CreateAtespaceAccessPolicy(ctx, "team-a", policy); !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("second CreateAtespaceAccessPolicy = %v, want ErrAlreadyExists", err)
	}

	updated, err := p.UpdateAtespaceAccessPolicy(ctx, "team-a", store.PreconditionFrom(created), func(toUpdate *ateapipb.AccessPolicy) error {
		toUpdate.Bindings = []*ateapipb.Binding{
			{Role: authz.RoleViewer, Members: []string{"user:dave"}},
		}
		return nil
	})
	if err != nil || updated.GetMetadata().GetVersion() != 2 {
		t.Fatalf("UpdateAtespaceAccessPolicy = %v, %v; want version 2", updated, err)
	}

	if _, err := p.DeleteAtespaceAccessPolicy(ctx, "team-a", store.DeletePreconditions{Version: 99}); !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("DeleteAtespaceAccessPolicy wrong version = %v, want ErrVersionConflict", err)
	}
	if _, err := p.DeleteAtespaceAccessPolicy(ctx, "team-a", store.DeletePreconditions{UID: "wrong-uid"}); !errors.Is(err, store.ErrUIDConflict) {
		t.Fatalf("DeleteAtespaceAccessPolicy wrong uid = %v, want ErrUIDConflict", err)
	}

	deleted, err := p.DeleteAtespaceAccessPolicy(ctx, "team-a", store.DeletePreconditions{
		UID:     updated.GetMetadata().GetUid(),
		Version: updated.GetMetadata().GetVersion(),
	})
	if err != nil || !cmp.Equal(deleted, updated, protocmp.Transform()) {
		t.Fatalf("DeleteAtespaceAccessPolicy = %v, %v; want %v", deleted, err, updated)
	}
	if _, err := p.GetAtespaceAccessPolicy(ctx, "team-a"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetAtespaceAccessPolicy after delete = %v, want ErrNotFound", err)
	}

	// Recreate policy and verify DeleteAtespace cascades to atespace_access_policies.
	if _, err := p.CreateAtespaceAccessPolicy(ctx, "team-a", policy); err != nil {
		t.Fatalf("re-creating access policy failed: %v", err)
	}
	if _, err := p.DeleteAtespace(ctx, "team-a", store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteAtespace failed: %v", err)
	}
	if _, err := p.GetAtespaceAccessPolicy(ctx, "team-a"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetAtespaceAccessPolicy after DeleteAtespace = %v, want ErrNotFound", err)
	}
}

// globalTuples returns the stored OpenFGA tuples on global:root as
// "relation user" strings, read straight from the tuple table.
func globalTuples(t *testing.T, p *Persistence) []string {
	t.Helper()
	rows, err := p.pool.Query(t.Context(), `SELECT relation, _user FROM tuple WHERE object_type = 'global' AND object_id = 'root'`)
	if err != nil {
		t.Fatalf("reading tuples: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var relation, user string
		if err := rows.Scan(&relation, &user); err != nil {
			t.Fatalf("scanning tuple: %v", err)
		}
		out = append(out, relation+" "+user)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("reading tuples: %v", err)
	}
	slices.Sort(out)
	return out
}

// TestGlobalAccessPolicy_UpdateIsAtomic checks that the policy row and its
// OpenFGA tuples commit or roll back together.
func TestGlobalAccessPolicy_UpdateIsAtomic(t *testing.T) {
	p := setupPostgresPersistence(t)
	ctx := t.Context()

	created, err := p.CreateGlobalAccessPolicy(ctx, &ateapipb.AccessPolicy{
		Metadata: &ateapipb.ResourceMetadata{Name: "default"},
		Bindings: []*ateapipb.Binding{{Role: authz.RoleOwner, Members: []string{"user:alice"}}},
	})
	if err != nil {
		t.Fatalf("CreateGlobalAccessPolicy failed: %v", err)
	}
	wantTuples := []string{"owner user:alice"}
	if diff := cmp.Diff(wantTuples, globalTuples(t, p)); diff != "" {
		t.Fatalf("tuples after create (-want +got):\n%s", diff)
	}
	pre := store.Precondition{UID: created.GetMetadata().GetUid(), Version: created.GetMetadata().GetVersion()}

	t.Run("failed tuple write rolls back the row and earlier tuple deletes", func(t *testing.T) {
		// Reconciling deletes alice's tuple and then fails writing a relation
		// the model does not define, after the row UPDATE has run.
		_, err := p.UpdateGlobalAccessPolicy(ctx, pre, func(ap *ateapipb.AccessPolicy) error {
			ap.Bindings = []*ateapipb.Binding{{Role: "not-a-relation", Members: []string{"user:bob"}}}
			return nil
		})
		if err == nil {
			t.Fatal("UpdateGlobalAccessPolicy with an undefined relation succeeded, want an error")
		}
		got, err := p.GetGlobalAccessPolicy(ctx)
		if err != nil {
			t.Fatalf("GetGlobalAccessPolicy failed: %v", err)
		}
		if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
			t.Errorf("policy row changed by failed update (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff(wantTuples, globalTuples(t, p)); diff != "" {
			t.Errorf("tuples changed by failed update (-want +got):\n%s", diff)
		}
	})

	t.Run("concurrent updates leave tuples matching the winning row", func(t *testing.T) {
		members := []string{"user:bob", "user:carol"}
		errs := make([]error, len(members))
		var wg sync.WaitGroup
		for i, member := range members {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, errs[i] = p.UpdateGlobalAccessPolicy(ctx, pre, func(ap *ateapipb.AccessPolicy) error {
					ap.Bindings = []*ateapipb.Binding{{Role: authz.RoleOwner, Members: []string{member}}}
					return nil
				})
			}()
		}
		wg.Wait()

		winners := 0
		for i, err := range errs {
			switch {
			case err == nil:
				winners++
			case !errors.Is(err, store.ErrVersionConflict):
				t.Errorf("update %d = %v, want nil or ErrVersionConflict", i, err)
			}
		}
		if winners != 1 {
			t.Fatalf("%d updates succeeded, want exactly 1 (errs: %v)", winners, errs)
		}
		got, err := p.GetGlobalAccessPolicy(ctx)
		if err != nil {
			t.Fatalf("GetGlobalAccessPolicy failed: %v", err)
		}
		if got.GetMetadata().GetVersion() != pre.Version+1 {
			t.Errorf("version = %d, want %d", got.GetMetadata().GetVersion(), pre.Version+1)
		}
		want := []string{"owner " + got.GetBindings()[0].GetMembers()[0]}
		if diff := cmp.Diff(want, globalTuples(t, p)); diff != "" {
			t.Errorf("tuples do not match the committed row (-row +tuples):\n%s", diff)
		}
	})
}
