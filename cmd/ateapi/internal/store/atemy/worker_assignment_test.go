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
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func newTestWorker(name string) *ateapipb.Worker {
	return &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: name},
		WorkerNamespace: "ns",
		WorkerPool:      "pool",
		WorkerPod:       name + "-pod",
	}
}

// saveWorker checks the stored version on top of the row lock its callers
// hold, so a Worker read before another write, or before a delete, cannot
// overwrite the row.
func TestSaveWorker_RejectsAStaleCopy(t *testing.T) {
	for _, tc := range []struct {
		name  string
		after func(*testing.T, *Persistence, *ateapipb.Worker)
	}{
		{"updated since read", func(t *testing.T, p *Persistence, w *ateapipb.Worker) {
			if _, err := p.UpdateWorker(t.Context(), w.GetMetadata().GetName(), store.PreconditionFrom(w), func(toUpdate *ateapipb.Worker) error {
				toUpdate.Ips = []string{"10.0.0.1"}
				return nil
			}); err != nil {
				t.Fatalf("UpdateWorker failed: %v", err)
			}
		}},
		{"deleted since read", func(t *testing.T, p *Persistence, w *ateapipb.Worker) {
			if _, err := p.DeleteWorker(t.Context(), w.GetMetadata().GetName(), store.DeletePreconditions{}); err != nil {
				t.Fatalf("DeleteWorker failed: %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := setupMySQLPersistence(t)
			ctx := t.Context()
			created, err := p.CreateWorker(ctx, newTestWorker("stale-worker"))
			if err != nil {
				t.Fatalf("CreateWorker failed: %v", err)
			}
			tc.after(t, p, created)

			tx, err := p.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("BeginTx failed: %v", err)
			}
			defer tx.Rollback() //nolint:errcheck // never committed
			if err := saveWorker(ctx, tx, created); !errors.Is(err, store.ErrVersionConflict) {
				t.Errorf("saveWorker() = %v, want ErrVersionConflict", err)
			}
		})
	}
}
