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
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
)

// ctxLeaseHolder derives the lease context from its argument, as atepg does.
type ctxLeaseHolder struct{}

func (ctxLeaseHolder) AcquireLease(ctx context.Context, _ string) (*store.Lease, error) {
	leaseCtx, cancel := context.WithCancel(ctx)
	return store.NewLease(leaseCtx, cancel), nil
}

type callerValueKey struct{}

func TestAcquireLeaseOutlivesCaller(t *testing.T) {
	caller, cancelCaller := context.WithCancel(context.WithValue(context.Background(), callerValueKey{}, "kept"))
	defer cancelCaller()
	leaseCtx, lease, err := acquireLease(caller, ctxLeaseHolder{}, "lease:test", "test")
	if err != nil {
		t.Fatalf("acquireLease: %v", err)
	}
	defer lease.Close()

	cancelCaller()
	if err := leaseCtx.Err(); err != nil {
		t.Fatalf("the lease context ended with its caller: %v", err)
	}
	if got := leaseCtx.Value(callerValueKey{}); got != "kept" {
		t.Errorf("the lease context lost the caller's values: got %v", got)
	}
	deadline, ok := leaseCtx.Deadline()
	if left := time.Until(deadline); !ok || left > workflowDeadline || left < workflowDeadline-time.Minute {
		t.Errorf("deadline = %v (set %v), want about %v from now", deadline, ok, workflowDeadline)
	}

	lease.Close()
	if leaseCtx.Err() == nil {
		t.Error("the lease context is still alive after Close")
	}
}
