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
	"fmt"
	"slices"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maybeCrashActor inspects err returned by an atelet RPC. If err carries the
// CRASH_ACTOR reason, it crashes the actor and returns a DataLoss error;
// otherwise it wraps err with wrapMsg. A nil err returns nil.
func maybeCrashActor(ctx context.Context, st store.Interface, atespace, actorID string, err error, wrapMsg string) error {
	if err == nil {
		return nil
	}
	if slices.Contains(ateerrors.ErrorReasonsFromStatus(err), ateerrors.ErrReasonCrashActor.Error()) {
		if cerr := crashActor(ctx, st, atespace, actorID); cerr != nil {
			return cerr
		}
		return status.Errorf(codes.DataLoss, "actor %s crashed", actorID)
	}
	return fmt.Errorf("%s: %w", wrapMsg, err)
}

// crashActor moves the actor to CRASHED and removes its worker pointer.
func crashActor(ctx context.Context, st store.Interface, atespace, actorID string) error {
	actor, err := st.GetActor(ctx, atespace, actorID)
	if err != nil {
		return fmt.Errorf("while loading actor to crash: %w", err)
	}
	actor.Status = ateapipb.Actor_STATUS_CRASHED
	// TODO(zoezhao): Mark the worker as unhealthy if needed.
	actor.AteomPodNamespace = ""
	actor.AteomPodName = ""
	actor.AteomPodIp = ""
	actor.AteomPodUid = ""
	actor.WorkerPoolName = ""
	if err := st.UpdateActor(ctx, actor, actor.GetVersion()); err != nil {
		return fmt.Errorf("while marking actor crashed: %w", err)
	}

	return nil
}
