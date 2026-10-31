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

package harness

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/api/ateapi/v1"
)

func TestFixtures(t *testing.T) {
	t.Parallel()
	h := New(t, WithPreviewClient())
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"), WithActorCapacity(4))
	tmpl := h.NewTemplate(as, pool, WithExternalVolume("data", "/data"))

	running := h.RunningActor(as, tmpl)
	if got := running.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("RunningActor is %v, want RUNNING", got)
	}
	h.WorkerOf(running)

	suspended := h.SuspendedActor(as, tmpl)
	if got := suspended.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("SuspendedActor is %v, want SUSPENDED", got)
	}
	snapshots := suspended.GetStatus().GetSnapshots()
	if len(snapshots) == 0 {
		t.Fatal("SuspendedActor holds no snapshot")
	}
	if names, _ := h.ObjectStore.Snapshot(snapshots[len(snapshots)-1].GetDurableSnapshot().GetObject().GetSnapshotUri()); len(names) == 0 {
		t.Error("SuspendedActor holds no durable snapshot in object storage")
	}

	crashed := h.CrashedActor(as, tmpl)
	if crashed.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("CrashedActor holds worker assignment %v, want none", crashed.GetStatus().GetWorkerAssignment())
	}
}

func TestRetryAborted(t *testing.T) {
	t.Parallel()
	aborted := status.Error(codes.Aborted, "lease held")

	t.Run("retries while the call fails with ABORTED", func(t *testing.T) {
		calls := 0
		err := RetryAborted(t.Context(), func() error {
			calls++
			if calls < 3 {
				return aborted
			}
			return nil
		})
		if err != nil || calls != 3 {
			t.Errorf("RetryAborted = %v after %d calls, want nil after 3", err, calls)
		}
	})

	t.Run("returns any other error at once", func(t *testing.T) {
		calls := 0
		err := RetryAborted(t.Context(), func() error {
			calls++
			return status.Error(codes.FailedPrecondition, "wrong state")
		})
		if status.Code(err) != codes.FailedPrecondition || calls != 1 {
			t.Errorf("RetryAborted = %v after %d calls, want FailedPrecondition after 1", err, calls)
		}
	})

	t.Run("returns ABORTED once the context ends", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		calls := 0
		err := RetryAborted(ctx, func() error {
			calls++
			cancel()
			return aborted
		})
		if status.Code(err) != codes.Aborted || calls != 1 {
			t.Errorf("RetryAborted = %v after %d calls, want Aborted after 1", err, calls)
		}
	})
}
