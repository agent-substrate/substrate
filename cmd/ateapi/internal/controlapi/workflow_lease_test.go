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

package controlapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
)

type leaseStore struct{ store.Interface }

func (leaseStore) AcquireLease(ctx context.Context, _ string) (*store.Lease, error) {
	return store.NewLease(ctx, func() {}), nil
}

func TestAcquireActorLeaseWorkflowDeadline(t *testing.T) {
	w := &ActorWorkflow{store: leaseStore{}, workflowDeadline: 20 * time.Millisecond}

	ctx, lease, err := w.acquireActorLease(context.Background(), resources.ActorRef{Atespace: "space", Name: "actor"})
	if err != nil {
		t.Fatalf("acquireActorLease: %v", err)
	}
	t.Cleanup(lease.Close)

	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatalf("context error = %v, want DeadlineExceeded", ctx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("workflow context did not reach its deadline")
	}
}
