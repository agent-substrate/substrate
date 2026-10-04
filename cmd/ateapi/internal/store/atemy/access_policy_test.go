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

package atemy

import (
	"errors"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/authz"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestGlobalAccessPolicy_UpdateIsAtomic checks that the policy row and its
// OpenFGA tuples commit or roll back together.
func TestGlobalAccessPolicy_UpdateIsAtomic(t *testing.T) {
	p := setupMySQLPersistence(t)
	ctx := t.Context()

	created, err := p.CreateGlobalAccessPolicy(ctx, &ateapipb.AccessPolicy{
		Metadata: &ateapipb.ResourceMetadata{Name: "default"},
		Bindings: []*ateapipb.Binding{{Role: authz.RoleOwner, Members: []string{"user:alice"}}},
	})
	if err != nil {
		t.Fatalf("CreateGlobalAccessPolicy failed: %v", err)
	}
	wantTuples := []string{"owner user:alice"}
	if diff := cmp.Diff(wantTuples, tuplesOn(t, p, "global", "root")); diff != "" {
		t.Fatalf("tuples after create (-want +got):\n%s", diff)
	}
	pre := store.Precondition{UID: created.GetMetadata().GetUid(), Version: created.GetMetadata().GetVersion()}

	t.Run("failed tuple write rolls back the row and earlier tuple deletes", func(t *testing.T) {
		// Reconciling deletes alice's tuple and then fails writing a relation
		// the model does not define, after the row UPDATE has run. A failed
		// statement does not abort a MySQL transaction, so only the rollback
		// undoes the statements before it.
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
		if diff := cmp.Diff(wantTuples, tuplesOn(t, p, "global", "root")); diff != "" {
			t.Errorf("tuples changed by failed update (-want +got):\n%s", diff)
		}
	})

	t.Run("concurrent updates leave tuples matching the winning row", func(t *testing.T) {
		members := []string{"user:bob", "user:carol"}
		errs := make([]error, len(members))
		var wg sync.WaitGroup
		for i, member := range members {
			wg.Go(func() {
				_, errs[i] = p.UpdateGlobalAccessPolicy(ctx, pre, func(ap *ateapipb.AccessPolicy) error {
					ap.Bindings = []*ateapipb.Binding{{Role: authz.RoleOwner, Members: []string{member}}}
					return nil
				})
			})
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
		if diff := cmp.Diff(want, tuplesOn(t, p, "global", "root")); diff != "" {
			t.Errorf("tuples do not match the committed row (-row +tuples):\n%s", diff)
		}
	})
}
