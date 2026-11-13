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
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/authz"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestGlobalAccessPolicy_Lifecycle(t *testing.T) {
	p := setupPostgresPersistence(t)
	ctx := t.Context()

	if _, err := p.GetGlobalAccessPolicy(ctx); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetGlobalAccessPolicy before create = %v, want ErrNotFound", err)
	}

	policy := &ateapi.AccessPolicy{
		Metadata: &ateapi.ResourceMetadata{Name: "default"},
		Bindings: []*ateapi.Binding{
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

	if _, err := p.UpdateGlobalAccessPolicy(ctx, store.Precondition{}, func(*ateapi.AccessPolicy) error { return nil }); !errors.Is(err, store.ErrPreconditionRequired) {
		t.Fatalf("UpdateGlobalAccessPolicy without precondition = %v, want ErrPreconditionRequired", err)
	}
	if _, err := p.UpdateGlobalAccessPolicy(ctx, store.Precondition{UID: created.GetMetadata().GetUid(), Version: 99}, func(*ateapi.AccessPolicy) error { return nil }); !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("UpdateGlobalAccessPolicy wrong version = %v, want ErrVersionConflict", err)
	}
	if _, err := p.UpdateGlobalAccessPolicy(ctx, store.Precondition{UID: "wrong-uid", Version: 1}, func(*ateapi.AccessPolicy) error { return nil }); !errors.Is(err, store.ErrUIDConflict) {
		t.Fatalf("UpdateGlobalAccessPolicy wrong uid = %v, want ErrUIDConflict", err)
	}

	updated, err := p.UpdateGlobalAccessPolicy(ctx, store.PreconditionFrom(created), func(toUpdate *ateapi.AccessPolicy) error {
		toUpdate.Bindings = []*ateapi.Binding{
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

	policy := &ateapi.AccessPolicy{
		Metadata: &ateapi.ResourceMetadata{Name: "default"},
		Bindings: []*ateapi.Binding{
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

	updated, err := p.UpdateAtespaceAccessPolicy(ctx, "team-a", store.PreconditionFrom(created), func(toUpdate *ateapi.AccessPolicy) error {
		toUpdate.Bindings = []*ateapi.Binding{
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
	if err != nil || !cmp.Equal(deleted, updated, protocmp.Transform(), protocmp.IgnoreFields(&ateapi.ResourceMetadata{}, "delete_time")) {
		t.Fatalf("DeleteAtespaceAccessPolicy = %v, %v; want %v", deleted, err, updated)
	}
	if deleted.GetMetadata().GetDeleteTime() == nil {
		t.Error("the deleted policy carries no delete_time")
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

	created, err := p.CreateGlobalAccessPolicy(ctx, &ateapi.AccessPolicy{
		Metadata: &ateapi.ResourceMetadata{Name: "default"},
		Bindings: []*ateapi.Binding{{Role: authz.RoleOwner, Members: []string{"user:alice"}}},
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
		_, err := p.UpdateGlobalAccessPolicy(ctx, pre, func(ap *ateapi.AccessPolicy) error {
			ap.Bindings = []*ateapi.Binding{{Role: "not-a-relation", Members: []string{"user:bob"}}}
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
				_, errs[i] = p.UpdateGlobalAccessPolicy(ctx, pre, func(ap *ateapi.AccessPolicy) error {
					ap.Bindings = []*ateapi.Binding{{Role: authz.RoleOwner, Members: []string{member}}}
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

func externalVolumeClassTuples(t *testing.T, p *Persistence, className string) []string {
	t.Helper()
	rows, err := p.pool.Query(t.Context(),
		`SELECT relation, _user FROM tuple WHERE object_type = 'external_volume_class' AND object_id = $1`,
		className)
	if err != nil {
		t.Fatalf("reading external volume class tuples: %v", err)
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
		t.Fatalf("reading external volume class tuples: %v", err)
	}
	slices.Sort(out)
	return out
}

func newTestExternalVolumeClass(name string) *ateapi.ExternalVolumeClass {
	return &ateapi.ExternalVolumeClass{
		Metadata:   &ateapi.ResourceMetadata{Name: name},
		DriverName: "nfs.csi.k8s.io",
	}
}

func TestExternalVolumeClassAccessPolicy_LifecycleAndCascade(t *testing.T) {
	p := setupPostgresPersistence(t)
	ctx := t.Context()

	for _, space := range []string{"team-a", "team-b", "team-c"} {
		createTestAtespace(t, p, space)
	}

	policy := &ateapi.AccessPolicy{
		Metadata: &ateapi.ResourceMetadata{Name: "default"},
		Bindings: []*ateapi.Binding{
			{Role: authz.RoleAllowedAtespace, Members: []string{"atespace:team-a", "atespace:team-b"}},
		},
	}

	// Missing parent ExternalVolumeClass -> ErrFailedPrecondition.
	if _, err := p.CreateExternalVolumeClassAccessPolicy(ctx, "missing-class", policy); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Fatalf("CreateExternalVolumeClassAccessPolicy on missing class = %v, want ErrFailedPrecondition", err)
	}

	if _, err := p.CreateExternalVolumeClass(ctx, newTestExternalVolumeClass("nfs-gold")); err != nil {
		t.Fatalf("CreateExternalVolumeClass(nfs-gold) failed: %v", err)
	}
	if _, err := p.GetExternalVolumeClassAccessPolicy(ctx, "nfs-gold"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetExternalVolumeClassAccessPolicy before create = %v, want ErrNotFound", err)
	}

	// Nonexistent member atespace on Create -> ErrFailedPrecondition, leaving no row or tuples.
	if _, err := p.CreateExternalVolumeClassAccessPolicy(ctx, "nfs-gold", &ateapi.AccessPolicy{
		Metadata: &ateapi.ResourceMetadata{Name: "default"},
		Bindings: []*ateapi.Binding{
			{Role: authz.RoleAllowedAtespace, Members: []string{"atespace:team-a", "atespace:ghost"}},
		},
	}); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Fatalf("CreateExternalVolumeClassAccessPolicy with missing member = %v, want ErrFailedPrecondition", err)
	}
	if _, err := p.GetExternalVolumeClassAccessPolicy(ctx, "nfs-gold"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetExternalVolumeClassAccessPolicy after failed create = %v, want ErrNotFound", err)
	}
	if got := externalVolumeClassTuples(t, p, "nfs-gold"); len(got) != 0 {
		t.Fatalf("tuples after failed create = %v, want empty", got)
	}

	created, err := p.CreateExternalVolumeClassAccessPolicy(ctx, "nfs-gold", policy)
	if err != nil {
		t.Fatalf("CreateExternalVolumeClassAccessPolicy failed: %v", err)
	}
	if created.GetMetadata().GetName() != "default" || created.GetMetadata().GetVersion() != 1 || created.GetMetadata().GetUid() == "" {
		t.Fatalf("unexpected created metadata: %+v", created.GetMetadata())
	}
	wantInitialTuples := []string{"allowed_atespace atespace:team-a", "allowed_atespace atespace:team-b"}
	if diff := cmp.Diff(wantInitialTuples, externalVolumeClassTuples(t, p, "nfs-gold")); diff != "" {
		t.Fatalf("tuples after create (-want +got):\n%s", diff)
	}

	if _, err := p.CreateExternalVolumeClassAccessPolicy(ctx, "nfs-gold", policy); !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("second CreateExternalVolumeClassAccessPolicy = %v, want ErrAlreadyExists", err)
	}

	got, err := p.GetExternalVolumeClassAccessPolicy(ctx, "nfs-gold")
	if err != nil || !cmp.Equal(got, created, protocmp.Transform()) {
		t.Fatalf("GetExternalVolumeClassAccessPolicy = %v, %v; want %v", got, err, created)
	}

	// Update precondition and missing-policy checks.
	if _, err := p.UpdateExternalVolumeClassAccessPolicy(ctx, "nfs-gold", store.Precondition{}, func(*ateapi.AccessPolicy) error { return nil }); !errors.Is(err, store.ErrPreconditionRequired) {
		t.Fatalf("UpdateExternalVolumeClassAccessPolicy without precondition = %v, want ErrPreconditionRequired", err)
	}
	if _, err := p.UpdateExternalVolumeClassAccessPolicy(ctx, "missing-class", store.PreconditionFrom(created), func(*ateapi.AccessPolicy) error { return nil }); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("UpdateExternalVolumeClassAccessPolicy on missing policy = %v, want ErrNotFound", err)
	}
	if _, err := p.UpdateExternalVolumeClassAccessPolicy(ctx, "nfs-gold", store.Precondition{UID: created.GetMetadata().GetUid(), Version: 99}, func(*ateapi.AccessPolicy) error { return nil }); !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("UpdateExternalVolumeClassAccessPolicy wrong version = %v, want ErrVersionConflict", err)
	}
	if _, err := p.UpdateExternalVolumeClassAccessPolicy(ctx, "nfs-gold", store.Precondition{UID: "wrong-uid", Version: 1}, func(*ateapi.AccessPolicy) error { return nil }); !errors.Is(err, store.ErrUIDConflict) {
		t.Fatalf("UpdateExternalVolumeClassAccessPolicy wrong uid = %v, want ErrUIDConflict", err)
	}

	// Nonexistent member atespace on Update -> ErrFailedPrecondition, leaving row and tuples untouched.
	if _, err := p.UpdateExternalVolumeClassAccessPolicy(ctx, "nfs-gold", store.PreconditionFrom(created), func(toUpdate *ateapi.AccessPolicy) error {
		toUpdate.Bindings = []*ateapi.Binding{
			{Role: authz.RoleAllowedAtespace, Members: []string{"atespace:team-b", "atespace:ghost"}},
		}
		return nil
	}); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Fatalf("UpdateExternalVolumeClassAccessPolicy with missing member = %v, want ErrFailedPrecondition", err)
	}

	// Failed OpenFGA write rolls back both row UPDATE and earlier tuple deletes.
	if _, err := p.UpdateExternalVolumeClassAccessPolicy(ctx, "nfs-gold", store.PreconditionFrom(created), func(toUpdate *ateapi.AccessPolicy) error {
		toUpdate.Bindings = []*ateapi.Binding{
			{Role: "not-a-relation", Members: []string{"atespace:team-b"}},
		}
		return nil
	}); err == nil {
		t.Fatal("UpdateExternalVolumeClassAccessPolicy with undefined relation succeeded, want error")
	}
	afterFailedUpdate, err := p.GetExternalVolumeClassAccessPolicy(ctx, "nfs-gold")
	if err != nil || !cmp.Equal(afterFailedUpdate, created, protocmp.Transform()) {
		t.Fatalf("policy row changed after failed update: got %v, err %v", afterFailedUpdate, err)
	}
	if diff := cmp.Diff(wantInitialTuples, externalVolumeClassTuples(t, p, "nfs-gold")); diff != "" {
		t.Fatalf("tuples changed after failed update (-want +got):\n%s", diff)
	}

	updated, err := p.UpdateExternalVolumeClassAccessPolicy(ctx, "nfs-gold", store.PreconditionFrom(created), func(toUpdate *ateapi.AccessPolicy) error {
		toUpdate.Metadata.Name = "tampered"
		toUpdate.Metadata.DeleteTime = timestamppb.Now()
		toUpdate.Bindings = []*ateapi.Binding{
			{Role: authz.RoleAllowedAtespace, Members: []string{"atespace:team-b", "atespace:team-c"}},
		}
		return nil
	})
	if err != nil || updated.GetMetadata().GetVersion() != 2 {
		t.Fatalf("UpdateExternalVolumeClassAccessPolicy = %v, %v; want version 2", updated, err)
	}
	if updated.GetMetadata().GetName() != "default" || updated.GetMetadata().GetUid() != created.GetMetadata().GetUid() || updated.GetMetadata().GetDeleteTime() != nil {
		t.Fatalf("UpdateExternalVolumeClassAccessPolicy did not preserve server-managed metadata: %+v", updated.GetMetadata())
	}
	wantUpdatedTuples := []string{"allowed_atespace atespace:team-b", "allowed_atespace atespace:team-c"}
	if diff := cmp.Diff(wantUpdatedTuples, externalVolumeClassTuples(t, p, "nfs-gold")); diff != "" {
		t.Fatalf("tuples after update (-want +got):\n%s", diff)
	}

	// DeleteExternalVolumeClassAccessPolicy preconditions and cleanup.
	if _, err := p.DeleteExternalVolumeClassAccessPolicy(ctx, "missing-class", store.DeletePreconditions{}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("DeleteExternalVolumeClassAccessPolicy on missing policy = %v, want ErrNotFound", err)
	}
	if _, err := p.DeleteExternalVolumeClassAccessPolicy(ctx, "nfs-gold", store.DeletePreconditions{Version: 99}); !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("DeleteExternalVolumeClassAccessPolicy wrong version = %v, want ErrVersionConflict", err)
	}
	if _, err := p.DeleteExternalVolumeClassAccessPolicy(ctx, "nfs-gold", store.DeletePreconditions{UID: "wrong-uid"}); !errors.Is(err, store.ErrUIDConflict) {
		t.Fatalf("DeleteExternalVolumeClassAccessPolicy wrong uid = %v, want ErrUIDConflict", err)
	}

	deleted, err := p.DeleteExternalVolumeClassAccessPolicy(ctx, "nfs-gold", store.DeletePreconditions{
		UID:     updated.GetMetadata().GetUid(),
		Version: updated.GetMetadata().GetVersion(),
	})
	if err != nil || !cmp.Equal(deleted, updated, protocmp.Transform(), protocmp.IgnoreFields(&ateapi.ResourceMetadata{}, "delete_time")) {
		t.Fatalf("DeleteExternalVolumeClassAccessPolicy = %v, %v; want %v", deleted, err, updated)
	}
	if deleted.GetMetadata().GetDeleteTime() == nil {
		t.Error("deleted policy carries no delete_time")
	}
	if _, err := p.GetExternalVolumeClassAccessPolicy(ctx, "nfs-gold"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetExternalVolumeClassAccessPolicy after delete = %v, want ErrNotFound", err)
	}
	if got := externalVolumeClassTuples(t, p, "nfs-gold"); len(got) != 0 {
		t.Fatalf("tuples after DeleteExternalVolumeClassAccessPolicy = %v, want empty", got)
	}

	// Recreate policy and verify DeleteExternalVolumeClass cascades both row and tuples atomically.
	if _, err := p.CreateExternalVolumeClassAccessPolicy(ctx, "nfs-gold", policy); err != nil {
		t.Fatalf("re-creating access policy failed: %v", err)
	}
	if _, err := p.DeleteExternalVolumeClass(ctx, "nfs-gold", store.DeletePreconditions{Version: 99}); !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("DeleteExternalVolumeClass wrong version = %v, want ErrVersionConflict", err)
	}
	if diff := cmp.Diff(wantInitialTuples, externalVolumeClassTuples(t, p, "nfs-gold")); diff != "" {
		t.Fatalf("tuples changed after failed DeleteExternalVolumeClass (-want +got):\n%s", diff)
	}
	if _, err := p.DeleteExternalVolumeClass(ctx, "nfs-gold", store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteExternalVolumeClass failed: %v", err)
	}
	if _, err := p.GetExternalVolumeClassAccessPolicy(ctx, "nfs-gold"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetExternalVolumeClassAccessPolicy after DeleteExternalVolumeClass = %v, want ErrNotFound", err)
	}
	if got := externalVolumeClassTuples(t, p, "nfs-gold"); len(got) != 0 {
		t.Fatalf("tuples after DeleteExternalVolumeClass = %v, want empty", got)
	}
}

func TestDeleteAtespace_ScrubsExternalVolumeClassAccessPolicies(t *testing.T) {
	p := setupPostgresPersistence(t)
	ctx := t.Context()

	for _, space := range []string{"team-a", "team-b", "team-c"} {
		createTestAtespace(t, p, space)
	}
	for _, className := range []string{"nfs-gold", "ssd-fast", "untouched"} {
		if _, err := p.CreateExternalVolumeClass(ctx, newTestExternalVolumeClass(className)); err != nil {
			t.Fatalf("CreateExternalVolumeClass(%s) failed: %v", className, err)
		}
	}

	nfsCreated, err := p.CreateExternalVolumeClassAccessPolicy(ctx, "nfs-gold", &ateapi.AccessPolicy{
		Metadata: &ateapi.ResourceMetadata{Name: "default"},
		Bindings: []*ateapi.Binding{
			{Role: authz.RoleAllowedAtespace, Members: []string{"atespace:team-a", "atespace:team-b"}},
		},
	})
	if err != nil {
		t.Fatalf("CreateExternalVolumeClassAccessPolicy(nfs-gold) failed: %v", err)
	}
	if _, err := p.CreateExternalVolumeClassAccessPolicy(ctx, "ssd-fast", &ateapi.AccessPolicy{
		Metadata: &ateapi.ResourceMetadata{Name: "default"},
		Bindings: []*ateapi.Binding{
			{Role: authz.RoleAllowedAtespace, Members: []string{"atespace:team-a"}},
		},
	}); err != nil {
		t.Fatalf("CreateExternalVolumeClassAccessPolicy(ssd-fast) failed: %v", err)
	}
	untouchedBefore, err := p.CreateExternalVolumeClassAccessPolicy(ctx, "untouched", &ateapi.AccessPolicy{
		Metadata: &ateapi.ResourceMetadata{Name: "default"},
		Bindings: []*ateapi.Binding{
			{Role: authz.RoleAllowedAtespace, Members: []string{"atespace:team-b"}},
		},
	})
	if err != nil {
		t.Fatalf("CreateExternalVolumeClassAccessPolicy(untouched) failed: %v", err)
	}

	// Precondition mismatch aborts before scrubbing policy rows or sweeping tuples.
	if _, err := p.DeleteAtespace(ctx, "team-a", store.DeletePreconditions{Version: 99}); !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("DeleteAtespace wrong version = %v, want ErrVersionConflict", err)
	}
	if diff := cmp.Diff([]string{"allowed_atespace atespace:team-a", "allowed_atespace atespace:team-b"}, externalVolumeClassTuples(t, p, "nfs-gold")); diff != "" {
		t.Fatalf("nfs-gold tuples changed on failed delete (-want +got):\n%s", diff)
	}

	// Deleting team-a scrubs it from nfs-gold and ssd-fast without touching untouched.
	if _, err := p.DeleteAtespace(ctx, "team-a", store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteAtespace(team-a) failed: %v", err)
	}

	nfsPol, err := p.GetExternalVolumeClassAccessPolicy(ctx, "nfs-gold")
	if err != nil {
		t.Fatalf("GetExternalVolumeClassAccessPolicy(nfs-gold) failed: %v", err)
	}
	if nfsPol.GetMetadata().GetVersion() != 2 || nfsPol.GetMetadata().GetUid() != nfsCreated.GetMetadata().GetUid() || nfsPol.GetMetadata().GetName() != "default" {
		t.Errorf("nfs-gold metadata after scrub = %+v, want version 2 with preserved uid/name", nfsPol.GetMetadata())
	}
	wantNFSBindings := []*ateapi.Binding{
		{Role: authz.RoleAllowedAtespace, Members: []string{"atespace:team-b"}},
	}
	if diff := cmp.Diff(wantNFSBindings, nfsPol.GetBindings(), protocmp.Transform()); diff != "" {
		t.Errorf("nfs-gold bindings after scrub (-want +got):\n%s", diff)
	}

	ssdPol, err := p.GetExternalVolumeClassAccessPolicy(ctx, "ssd-fast")
	if err != nil {
		t.Fatalf("GetExternalVolumeClassAccessPolicy(ssd-fast) failed: %v", err)
	}
	if ssdPol.GetMetadata().GetVersion() != 2 {
		t.Errorf("ssd-fast policy version = %d, want 2", ssdPol.GetMetadata().GetVersion())
	}
	if len(ssdPol.GetBindings()) != 0 {
		t.Errorf("ssd-fast bindings after sole-member scrub = %v, want empty (no empty-members Binding)", ssdPol.GetBindings())
	}

	untouchedAfter, err := p.GetExternalVolumeClassAccessPolicy(ctx, "untouched")
	if err != nil || !cmp.Equal(untouchedBefore, untouchedAfter, protocmp.Transform()) {
		t.Errorf("untouched policy changed after DeleteAtespace(team-a): got %v, err %v", untouchedAfter, err)
	}

	// Recreating team-a grants no access, and a read-modify-write Update on
	// nfs-gold does not re-add team-a.
	createTestAtespace(t, p, "team-a")
	if diff := cmp.Diff([]string{"allowed_atespace atespace:team-b"}, externalVolumeClassTuples(t, p, "nfs-gold")); diff != "" {
		t.Fatalf("nfs-gold tuples after recreating team-a (-want +got):\n%s", diff)
	}
	if got := externalVolumeClassTuples(t, p, "ssd-fast"); len(got) != 0 {
		t.Fatalf("ssd-fast tuples after recreating team-a = %v, want empty", got)
	}

	updatedNFS, err := p.UpdateExternalVolumeClassAccessPolicy(ctx, "nfs-gold", store.PreconditionFrom(nfsPol), func(toUpdate *ateapi.AccessPolicy) error {
		toUpdate.Bindings[0].Members = append(toUpdate.Bindings[0].Members, "atespace:team-c")
		return nil
	})
	if err != nil || updatedNFS.GetMetadata().GetVersion() != 3 {
		t.Fatalf("read-modify-write UpdateExternalVolumeClassAccessPolicy = %v, %v; want version 3", updatedNFS, err)
	}
	if diff := cmp.Diff([]string{"allowed_atespace atespace:team-b", "allowed_atespace atespace:team-c"}, externalVolumeClassTuples(t, p, "nfs-gold")); diff != "" {
		t.Errorf("nfs-gold tuples after read-modify-write update (-want +got):\n%s", diff)
	}
}

func TestExternalVolumeClassAccessPolicy_ConcurrentUpdateAndDeleteAtespace(t *testing.T) {
	p := setupPostgresPersistence(t)
	ctx := t.Context()

	createTestAtespace(t, p, "keeper")
	const classesCount = 3
	for c := range classesCount {
		className := fmt.Sprintf("class-%d", c)
		if _, err := p.CreateExternalVolumeClass(ctx, newTestExternalVolumeClass(className)); err != nil {
			t.Fatalf("CreateExternalVolumeClass(%s) failed: %v", className, err)
		}
		if _, err := p.CreateExternalVolumeClassAccessPolicy(ctx, className, &ateapi.AccessPolicy{
			Metadata: &ateapi.ResourceMetadata{Name: "default"},
			Bindings: []*ateapi.Binding{
				{Role: authz.RoleAllowedAtespace, Members: []string{"atespace:keeper"}},
			},
		}); err != nil {
			t.Fatalf("CreateExternalVolumeClassAccessPolicy(%s) failed: %v", className, err)
		}
	}
	if _, err := p.CreateExternalVolumeClass(ctx, newTestExternalVolumeClass("adder-update")); err != nil {
		t.Fatalf("CreateExternalVolumeClass(adder-update) failed: %v", err)
	}
	if _, err := p.CreateExternalVolumeClassAccessPolicy(ctx, "adder-update", &ateapi.AccessPolicy{
		Metadata: &ateapi.ResourceMetadata{Name: "default"},
		Bindings: []*ateapi.Binding{
			{Role: authz.RoleAllowedAtespace, Members: []string{"atespace:keeper"}},
		},
	}); err != nil {
		t.Fatalf("CreateExternalVolumeClassAccessPolicy(adder-update) failed: %v", err)
	}

	assertPolicyMatchesTuplesWithoutMember := func(className, deletedMember string) {
		t.Helper()
		pol, err := p.GetExternalVolumeClassAccessPolicy(ctx, className)
		if errors.Is(err, store.ErrNotFound) {
			if got := externalVolumeClassTuples(t, p, className); len(got) != 0 {
				t.Fatalf("%s has no policy row, but tuples = %v", className, got)
			}
			return
		}
		if err != nil {
			t.Fatalf("Get(%s) failed: %v", className, err)
		}
		var wantTuples []string
		for _, b := range pol.GetBindings() {
			for _, m := range b.GetMembers() {
				if m == deletedMember {
					t.Fatalf("%s retained deleted member %q in stored policy: %v", className, deletedMember, pol.GetBindings())
				}
				wantTuples = append(wantTuples, b.GetRole()+" "+m)
			}
		}
		slices.Sort(wantTuples)
		if diff := cmp.Diff(wantTuples, externalVolumeClassTuples(t, p, className)); diff != "" {
			t.Fatalf("%s stored policy vs OpenFGA tuples (-policy +tuples):\n%s", className, diff)
		}
	}

	for iter := range 10 {
		ephemeral := fmt.Sprintf("ephemeral-%d", iter)
		ephemeralMember := "atespace:" + ephemeral
		adderCreateClass := fmt.Sprintf("adder-create-%d", iter)
		createTestAtespace(t, p, ephemeral)
		if _, err := p.CreateExternalVolumeClass(ctx, newTestExternalVolumeClass(adderCreateClass)); err != nil {
			t.Fatalf("CreateExternalVolumeClass(%s) failed: %v", adderCreateClass, err)
		}
		for c := range classesCount {
			className := fmt.Sprintf("class-%d", c)
			pol, err := p.GetExternalVolumeClassAccessPolicy(ctx, className)
			if err != nil {
				t.Fatalf("GetExternalVolumeClassAccessPolicy(%s) failed: %v", className, err)
			}
			if _, err := p.UpdateExternalVolumeClassAccessPolicy(ctx, className, store.PreconditionFrom(pol), func(ap *ateapi.AccessPolicy) error {
				ap.Bindings = []*ateapi.Binding{
					{Role: authz.RoleAllowedAtespace, Members: []string{"atespace:keeper", ephemeralMember}},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed UpdateExternalVolumeClassAccessPolicy(%s) failed: %v", className, err)
			}
		}
		adderUpdatePol, err := p.GetExternalVolumeClassAccessPolicy(ctx, "adder-update")
		if err != nil {
			t.Fatalf("GetExternalVolumeClassAccessPolicy(adder-update) failed: %v", err)
		}

		// Race DeleteAtespace(ephemeral) simultaneously against:
		// - per-class updates removing ephemeral (class-0..2),
		// - an update adding ephemeral (adder-update), and
		// - a create adding ephemeral (adder-create-<iter>).
		// Neither side may fail with 40P01 deadlock, ephemeral must not survive on
		// any class, and every stored row must match its OpenFGA tuples.
		var wg sync.WaitGroup
		errCh := make(chan error, classesCount+3)

		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.DeleteAtespace(ctx, ephemeral, store.DeletePreconditions{}); err != nil {
				errCh <- fmt.Errorf("DeleteAtespace(%s): %w", ephemeral, err)
			}
		}()

		for c := range classesCount {
			className := fmt.Sprintf("class-%d", c)
			wg.Add(1)
			go func() {
				defer wg.Done()
				pol, err := p.GetExternalVolumeClassAccessPolicy(ctx, className)
				if err != nil {
					errCh <- fmt.Errorf("Get(%s): %w", className, err)
					return
				}
				_, err = p.UpdateExternalVolumeClassAccessPolicy(ctx, className, store.PreconditionFrom(pol), func(ap *ateapi.AccessPolicy) error {
					ap.Bindings = []*ateapi.Binding{
						{Role: authz.RoleAllowedAtespace, Members: []string{"atespace:keeper"}},
					}
					return nil
				})
				if err != nil && !errors.Is(err, store.ErrVersionConflict) {
					errCh <- fmt.Errorf("Update(%s): %w", className, err)
				}
			}()
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.UpdateExternalVolumeClassAccessPolicy(ctx, "adder-update", store.PreconditionFrom(adderUpdatePol), func(ap *ateapi.AccessPolicy) error {
				ap.Bindings = []*ateapi.Binding{
					{Role: authz.RoleAllowedAtespace, Members: []string{"atespace:keeper", ephemeralMember}},
				}
				return nil
			})
			if err != nil && !errors.Is(err, store.ErrFailedPrecondition) && !errors.Is(err, store.ErrVersionConflict) {
				errCh <- fmt.Errorf("Update(adder-update): %w", err)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.CreateExternalVolumeClassAccessPolicy(ctx, adderCreateClass, &ateapi.AccessPolicy{
				Metadata: &ateapi.ResourceMetadata{Name: "default"},
				Bindings: []*ateapi.Binding{
					{Role: authz.RoleAllowedAtespace, Members: []string{"atespace:keeper", ephemeralMember}},
				},
			})
			if err != nil && !errors.Is(err, store.ErrFailedPrecondition) {
				errCh <- fmt.Errorf("Create(%s): %w", adderCreateClass, err)
			}
		}()

		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Fatalf("iter %d concurrent error: %v", iter, err)
		}

		for c := range classesCount {
			assertPolicyMatchesTuplesWithoutMember(fmt.Sprintf("class-%d", c), ephemeralMember)
		}
		assertPolicyMatchesTuplesWithoutMember("adder-update", ephemeralMember)
		assertPolicyMatchesTuplesWithoutMember(adderCreateClass, ephemeralMember)
	}
}
