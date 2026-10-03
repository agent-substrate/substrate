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

// Tests for the transactions that stand in for foreign keys (relations.go):
// a child insert and a parent delete that race must never leave a child row
// without its parent.

package atemy

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/authz"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// requireNoOrphans fails if any row outlives the parent a foreign key would
// tie it to.
func requireNoOrphans(t *testing.T, p *Persistence) {
	t.Helper()
	for what, query := range map[string]string{
		"actors": `SELECT COUNT(*) FROM actors c
			WHERE NOT EXISTS (SELECT 1 FROM atespaces p WHERE p.name = c.atespace)`,
		"actor templates": `SELECT COUNT(*) FROM actor_templates c
			WHERE NOT EXISTS (SELECT 1 FROM atespaces p WHERE p.name = c.atespace)`,
		"tags": `SELECT COUNT(*) FROM tags c
			WHERE NOT EXISTS (SELECT 1 FROM atespaces p WHERE p.name = c.atespace)`,
		"atespace access policies": `SELECT COUNT(*) FROM atespace_access_policies c
			WHERE NOT EXISTS (SELECT 1 FROM atespaces p WHERE p.name = c.atespace_name)`,
		"egress policies": `SELECT COUNT(*) FROM actor_egress_policies c
			WHERE NOT EXISTS (SELECT 1 FROM actors p WHERE p.atespace = c.atespace AND p.name = c.actor_name)`,
		"worker assignments": `SELECT COUNT(*) FROM worker_assignments c
			WHERE NOT EXISTS (SELECT 1 FROM workers p WHERE p.name = c.worker_name)`,
		"atespace tuples": `SELECT COUNT(*) FROM tuple c
			WHERE c.object_type = 'atespace'
			AND NOT EXISTS (SELECT 1 FROM atespaces p WHERE p.name = c.object_id COLLATE utf8mb4_0900_bin)`,
	} {
		if n := countRows(t, p, query); n != 0 {
			t.Errorf("%d %s outlive their parent", n, what)
		}
	}
}

// raceOutcome checks the errors of one parent delete racing child creates:
// either the delete won and every create was refused, or a create won and the
// delete was refused. It reports whether the delete won.
func raceOutcome(t *testing.T, deleteErr error, createErrs []error) bool {
	t.Helper()
	created := 0
	for i, err := range createErrs {
		switch {
		case err == nil:
			created++
		case !errors.Is(err, store.ErrFailedPrecondition):
			t.Errorf("create %d = %v, want nil or ErrFailedPrecondition", i, err)
		}
	}
	switch {
	case deleteErr == nil && created != 0:
		t.Errorf("parent delete succeeded, but %d children were created", created)
	case errors.Is(deleteErr, store.ErrFailedPrecondition) && created == 0:
		t.Errorf("parent delete was refused for children, but none was created")
	case deleteErr != nil && !errors.Is(deleteErr, store.ErrFailedPrecondition):
		t.Errorf("parent delete = %v, want nil or ErrFailedPrecondition", deleteErr)
	}
	return deleteErr == nil
}

func TestDeleteAtespace_RacingChildCreatesLeaveNoOrphans(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	const iterations, creators = 10, 12
	deletesWon := 0
	for i := range iterations {
		atespace := fmt.Sprintf("race-%d", i)
		createTestAtespace(t, s, atespace)

		start := make(chan struct{})
		createErrs := make([]error, creators)
		var deleteErr error
		var wg sync.WaitGroup
		for c := range creators {
			wg.Go(func() {
				<-start
				name := fmt.Sprintf("child-%d", c)
				meta := &ateapipb.ResourceMetadata{Atespace: atespace, Name: name}
				switch c % 3 {
				case 0:
					_, createErrs[c] = s.CreateActor(ctx, newTestActor(atespace, name))
				case 1:
					_, createErrs[c] = s.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{Metadata: meta})
				default:
					_, createErrs[c] = s.CreateTag(ctx, &ateapipb.Tag{Metadata: meta, Status: &ateapipb.TagStatus{StorageLocation: "gs://bucket"}})
				}
			})
		}
		wg.Go(func() {
			<-start
			_, deleteErr = s.DeleteAtespace(ctx, atespace, store.DeletePreconditions{})
		})
		close(start)
		wg.Wait()
		if raceOutcome(t, deleteErr, createErrs) {
			deletesWon++
		}
	}
	t.Logf("the delete won %d of %d races", deletesWon, iterations)
	requireNoOrphans(t, s)
}

func TestDeleteAtespace_RacingAccessPolicyCreateLeavesNoOrphans(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	policy := &ateapipb.AccessPolicy{Bindings: []*ateapipb.Binding{{Role: authz.RoleEditor, Members: []string{"user:bob"}}}}
	for i := range 10 {
		atespace := fmt.Sprintf("policy-race-%d", i)
		createTestAtespace(t, s, atespace)

		start := make(chan struct{})
		var createErr, deleteErr error
		var wg sync.WaitGroup
		wg.Go(func() {
			<-start
			_, createErr = s.CreateAtespaceAccessPolicy(ctx, atespace, policy)
		})
		wg.Go(func() {
			<-start
			_, deleteErr = s.DeleteAtespace(ctx, atespace, store.DeletePreconditions{})
		})
		close(start)
		wg.Wait()
		// An access policy does not block its atespace's deletion; it goes
		// with it.
		if deleteErr != nil {
			t.Errorf("DeleteAtespace = %v, want nil", deleteErr)
		}
		if createErr != nil && !errors.Is(createErr, store.ErrFailedPrecondition) {
			t.Errorf("CreateAtespaceAccessPolicy = %v, want nil or ErrFailedPrecondition", createErr)
		}
	}
	requireNoOrphans(t, s)
}

// UpdateAtespaceAccessPolicy locks only the policy row, not the atespace, so
// its tuple writes must still land before DeleteAtespace reads the tuples to
// remove, or fail because the policy is gone.
func TestDeleteAtespace_RacingAccessPolicyUpdateLeavesNoOrphans(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	policy := &ateapipb.AccessPolicy{Bindings: []*ateapipb.Binding{{Role: authz.RoleEditor, Members: []string{"user:bob"}}}}
	for i := range 10 {
		atespace := fmt.Sprintf("policy-update-race-%d", i)
		createTestAtespace(t, s, atespace)
		created, err := s.CreateAtespaceAccessPolicy(ctx, atespace, policy)
		if err != nil {
			t.Fatalf("CreateAtespaceAccessPolicy failed: %v", err)
		}

		start := make(chan struct{})
		var updateErr, deleteErr error
		var wg sync.WaitGroup
		wg.Go(func() {
			<-start
			_, updateErr = s.UpdateAtespaceAccessPolicy(ctx, atespace, store.PreconditionFrom(created), func(ap *ateapipb.AccessPolicy) error {
				ap.Bindings = []*ateapipb.Binding{{Role: authz.RoleViewer, Members: []string{"user:carol"}}}
				return nil
			})
		})
		wg.Go(func() {
			<-start
			_, deleteErr = s.DeleteAtespace(ctx, atespace, store.DeletePreconditions{})
		})
		close(start)
		wg.Wait()
		if deleteErr != nil {
			t.Errorf("DeleteAtespace = %v, want nil", deleteErr)
		}
		if updateErr != nil && !errors.Is(updateErr, store.ErrNotFound) {
			t.Errorf("UpdateAtespaceAccessPolicy = %v, want nil or ErrNotFound", updateErr)
		}
	}
	requireNoOrphans(t, s)
}

func TestDeleteActor_RacingEgressPolicyCreateLeavesNoOrphans(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	createTestAtespace(t, s, "team-a")
	for i := range 20 {
		actor, err := s.CreateActor(ctx, newTestActor("team-a", fmt.Sprintf("actor-%d", i)))
		if err != nil {
			t.Fatalf("CreateActor failed: %v", err)
		}
		actorRef := resources.ActorRefFromActor(actor)

		start := make(chan struct{})
		var createErr, deleteErr error
		var wg sync.WaitGroup
		wg.Go(func() {
			<-start
			_, createErr = s.CreateEgressPolicy(ctx, actorRef, &ateapipb.EgressPolicy{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "default"},
			})
		})
		wg.Go(func() {
			<-start
			_, deleteErr = s.DeleteActor(ctx, actorRef, store.DeletePreconditions{})
		})
		close(start)
		wg.Wait()
		if deleteErr != nil {
			t.Errorf("DeleteActor = %v, want nil", deleteErr)
		}
		if createErr != nil && !errors.Is(createErr, store.ErrFailedPrecondition) {
			t.Errorf("CreateEgressPolicy = %v, want nil or ErrFailedPrecondition", createErr)
		}
		if _, err := s.GetEgressPolicy(ctx, actorRef); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("GetEgressPolicy after DeleteActor = %v, want ErrNotFound", err)
		}
	}
	requireNoOrphans(t, s)
}

func TestDeleteWorker_RacingBindLeavesNoOrphans(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	for i := range 20 {
		name := fmt.Sprintf("bind-race-worker-%d", i)
		if _, err := s.CreateWorker(ctx, newTestWorker(name)); err != nil {
			t.Fatalf("CreateWorker failed: %v", err)
		}

		start := make(chan struct{})
		var bindErr, deleteErr error
		var wg sync.WaitGroup
		wg.Go(func() {
			<-start
			bindErr = s.BindActorToWorker(ctx, name, &ateapipb.ActorAssignment{ActorUid: fmt.Sprintf("uid-%d", i)}, nil)
		})
		wg.Go(func() {
			<-start
			_, deleteErr = s.DeleteWorker(ctx, name, store.DeletePreconditions{})
		})
		close(start)
		wg.Wait()
		if deleteErr != nil {
			t.Errorf("DeleteWorker = %v, want nil", deleteErr)
		}
		if bindErr != nil && !errors.Is(bindErr, store.ErrNotFound) {
			t.Errorf("BindActorToWorker = %v, want nil or ErrNotFound", bindErr)
		}
	}
	requireNoOrphans(t, s)
}

// atespaceTuples returns the stored OpenFGA tuples on an atespace as
// "relation user" strings, read straight from the tuple table.
func atespaceTuples(t *testing.T, p *Persistence, name string) []string {
	t.Helper()
	return tuplesOn(t, p, "atespace", name)
}

func tuplesOn(t *testing.T, p *Persistence, objectType, objectID string) []string {
	t.Helper()
	rows, err := p.db.QueryContext(t.Context(), `SELECT relation, _user FROM tuple WHERE object_type = ? AND object_id = ?`, objectType, objectID)
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

func TestDeleteAtespace_RemovesAccessPolicyAndTuples(t *testing.T) {
	s := setupMySQLPersistence(t)
	ctx := t.Context()
	createTestAtespace(t, s, "team-a")
	createTestAtespace(t, s, "team-b")
	policy := &ateapipb.AccessPolicy{Bindings: []*ateapipb.Binding{{Role: authz.RoleEditor, Members: []string{"user:bob"}}}}
	for _, name := range []string{"team-a", "team-b"} {
		if _, err := s.CreateAtespaceAccessPolicy(ctx, name, policy); err != nil {
			t.Fatalf("CreateAtespaceAccessPolicy(%s) failed: %v", name, err)
		}
	}
	wantTuples := []string{"editor user:bob"}
	if got := atespaceTuples(t, s, "team-a"); !slices.Equal(got, wantTuples) {
		t.Fatalf("tuples after create = %q, want %q", got, wantTuples)
	}

	// A refused delete rolls back with everything it touched.
	if _, err := s.CreateActor(ctx, newTestActor("team-a", "a1")); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if _, err := s.DeleteAtespace(ctx, "team-a", store.DeletePreconditions{}); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Fatalf("DeleteAtespace of a non-empty atespace = %v, want ErrFailedPrecondition", err)
	}
	if _, err := s.GetAtespaceAccessPolicy(ctx, "team-a"); err != nil {
		t.Errorf("GetAtespaceAccessPolicy after a refused delete = %v, want the policy", err)
	}
	if got := atespaceTuples(t, s, "team-a"); !slices.Equal(got, wantTuples) {
		t.Errorf("tuples after a refused delete = %q, want %q", got, wantTuples)
	}

	if _, err := s.DeleteActor(ctx, resources.ActorRef{Atespace: "team-a", Name: "a1"}, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
	if _, err := s.DeleteAtespace(ctx, "team-a", store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteAtespace failed: %v", err)
	}
	if _, err := s.GetAtespaceAccessPolicy(ctx, "team-a"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetAtespaceAccessPolicy after DeleteAtespace = %v, want ErrNotFound", err)
	}
	if got := atespaceTuples(t, s, "team-a"); len(got) != 0 {
		t.Errorf("tuples after DeleteAtespace = %q, want none", got)
	}
	// Another atespace's policy and tuples are untouched.
	if _, err := s.GetAtespaceAccessPolicy(ctx, "team-b"); err != nil {
		t.Errorf("GetAtespaceAccessPolicy(team-b) = %v, want the policy", err)
	}
	if got := atespaceTuples(t, s, "team-b"); !slices.Equal(got, wantTuples) {
		t.Errorf("team-b tuples = %q, want %q", got, wantTuples)
	}
}
