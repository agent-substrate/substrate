/*
Copyright 2026 The Agent Substrate Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package atepg

import (
	"context"
	"fmt"
	"testing"

	ateapi "go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
)

func TestRepairAssignmentTotals(t *testing.T) {
	p := setupPostgresPersistence(t)
	ctx := context.Background()
	for name, tc := range map[string]struct {
		stored *ateapi.ActorAssignmentsStatus
		rows   int
	}{
		"unset with rows": {stored: nil, rows: 3},
		"too high":        {stored: &ateapi.ActorAssignmentsStatus{Total: 7}, rows: 2},
		"too low":         {stored: &ateapi.ActorAssignmentsStatus{Total: 1}, rows: 4},
		"set with none":   {stored: &ateapi.ActorAssignmentsStatus{Total: 2}, rows: 0},
		"right":           {stored: &ateapi.ActorAssignmentsStatus{Total: 2}, rows: 2},
	} {
		worker := &ateapi.Worker{
			Metadata:        &ateapi.ResourceMetadata{Name: "repair-" + name},
			WorkerNamespace: "ns",
			WorkerPool:      "pool",
			WorkerPod:       "pod-" + name,
		}
		created, err := p.CreateWorker(ctx, worker)
		if err != nil {
			t.Fatalf("CreateWorker(%s) failed: %v", name, err)
		}
		// rows as an older version leaves them, without touching the total
		for i := range tc.rows {
			if _, err := p.pool.Exec(ctx, `INSERT INTO worker_assignments (actor_uid, worker_name, proto) VALUES ($1, $2, '')`,
				fmt.Sprintf("%s-%d", name, i), worker.GetMetadata().GetName()); err != nil {
				t.Fatalf("inserting assignment: %v", err)
			}
		}
		if _, err := p.UpdateWorker(ctx, worker.GetMetadata().GetName(), store.PreconditionFrom(created), func(w *ateapi.Worker) error {
			if w.Status == nil {
				w.Status = &ateapi.WorkerStatus{}
			}
			w.Status.Assignments = tc.stored
			return nil
		}); err != nil {
			t.Fatalf("UpdateWorker(%s) failed: %v", name, err)
		}
	}
	right, err := p.GetWorker(ctx, "repair-right")
	if err != nil {
		t.Fatalf("GetWorker failed: %v", err)
	}

	if err := p.repairAssignmentTotals(ctx); err != nil {
		t.Fatalf("repairAssignmentTotals failed: %v", err)
	}

	for name, want := range map[string]int64{"unset with rows": 3, "too high": 2, "too low": 4, "set with none": 0, "right": 2} {
		w, err := p.GetWorker(ctx, "repair-"+name)
		if err != nil {
			t.Fatalf("GetWorker(%s) failed: %v", name, err)
		}
		if got := w.GetStatus().GetAssignments().GetTotal(); got != want {
			t.Errorf("%s: total = %d after repair, want %d", name, got, want)
		}
	}
	w, err := p.GetWorker(ctx, "repair-right")
	if err != nil {
		t.Fatalf("GetWorker failed: %v", err)
	}
	if got, want := w.GetMetadata().GetVersion(), right.GetMetadata().GetVersion(); got != want {
		t.Errorf("a worker with the right total was rewritten: version %d, want %d", got, want)
	}
}
