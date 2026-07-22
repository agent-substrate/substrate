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
	"fmt"
	"log/slog"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maybeCrashActor inspects err returned by an atelet RPC, it crashes
// the actor if the err carries the actorCrashed=true metadata directive.
func maybeCrashActor(ctx context.Context, st store.Interface, atespace, actorName string, err error, wrapMsg string) error {
	if err == nil {
		return nil
	}

	if ateerrors.ActorCrashRequested(err) {
		slog.ErrorContext(ctx, "Setting Actor to crashed due to error", slog.Any("error", err))
		if cerr := crashActor(ctx, st, atespace, actorName); cerr != nil {
			slog.ErrorContext(ctx, "Failed to crash actor", slog.Any("cerr", cerr))
			return cerr
		}
		return status.Errorf(codes.DataLoss, "actor %s crashed", actorName)
	}
	return fmt.Errorf("%s: %w", wrapMsg, err)
}

// crashActor moves the actor to CRASHED state and frees the worker it was
// assigned to, if any, so the worker can host other actors.
func crashActor(ctx context.Context, st store.Interface, atespace, actorName string) error {
	actor, err := st.GetActor(ctx, atespace, actorName)
	if err != nil {
		return fmt.Errorf("while loading actor to crash: %w", err)
	}

	var errCollected []error
	if err := releaseWorker(ctx, st, actor); err != nil {
		errCollected = append(errCollected, err)
	}

	actor.Status = ateapipb.Actor_STATUS_CRASHED

	// InProgressSnapshot is kept for debugging; failed workflow
	// steps must never promote it to LatestSnapshotInfo.
	actor.AteomPodNamespace = ""
	actor.AteomPodName = ""
	actor.AteomPodIp = ""
	actor.AteomPodUid = ""
	actor.WorkerPoolName = ""

	if _, err := st.UpdateActor(ctx, actor, actor.GetMetadata().GetVersion()); err != nil {
		errCollected = append(errCollected, fmt.Errorf("while marking actor crashed: %w", err))
	}
	return errors.Join(errCollected...)
}

// releaseWorker clears the worker's assignment if it still points at the given
// actor. A missing worker or an already-cleared assignment is not an error.
func releaseWorker(ctx context.Context, st store.Interface, actor *ateapipb.Actor) error {
	podNamespace := actor.GetAteomPodNamespace()
	podName := actor.GetAteomPodName()
	podUid := actor.GetAteomPodUid()
	poolName := actor.GetWorkerPoolName()

	if podNamespace == "" || podName == "" || poolName == "" {
		slog.WarnContext(ctx, "Actor's worker assignment is already cleared")
		return nil
	}

	worker, err := st.GetWorker(ctx, podNamespace, poolName, podName)
	if errors.Is(err, store.ErrNotFound) {
		// No need to release if the worker is not found.
		slog.WarnContext(ctx, "Worker already gone while crashing actor, skipping release", slog.String("worker", podUid))
		return nil
	}
	if err != nil {
		return fmt.Errorf("while getting worker to release: %w", err)
	}
	wass := worker.GetAssignment()
	if wass == nil {
		slog.WarnContext(ctx, "Worker's assignment is already nil, skipping release", slog.String("worker", podUid))
		return nil
	}
	// Only free it if it still belongs to us
	if wass.GetActor().GetAtespace() != actor.GetMetadata().GetAtespace() || wass.GetActor().GetName() != actor.GetMetadata().GetName() {
		slog.WarnContext(ctx, "Worker already assigned to another Actor", slog.String("worker", podUid))
		return nil
	}

	worker.Assignment = nil
	if err := st.UpdateWorker(ctx, worker, worker.GetVersion()); err != nil {
		return fmt.Errorf("while releasing worker: %w", err)
	}
	return nil
}
