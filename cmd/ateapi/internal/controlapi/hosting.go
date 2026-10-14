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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// HostingNode is the node that holds the actor, and "" when no node does. The
// actor's state decides which status field to trust, since both can outlive
// the hosting they record: a revert releases the worker but keeps the
// assignment until it finalizes, and a crash clears the worker but keeps
// assigned_node as a record of the last node. The rules:
//   - SUSPENDED or SUSPENDING with no worker and a local-only latest
//     snapshot, the shape of a paused actor: assigned_node, the node holding
//     that snapshot.
//   - RESUMING, RUNNING and SUSPENDING with a worker assigned: the worker's
//     node, or "" if the worker is gone from the store.
//   - Anything else: "".
//
// WorkerService uses it to refuse a node that asks for credentials for an
// actor it does not hold.
func HostingNode(ctx context.Context, st store.Interface, actor *ateapipb.Actor) (string, error) {
	status := actor.GetStatus()
	assignment := status.GetWorkerAssignment()
	if assignment == nil {
		switch status.GetState() {
		case ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
			if latestSnapshotIsLocalOnly(status) {
				return status.GetAssignedNode(), nil
			}
		}
		return "", nil
	}
	switch status.GetState() {
	case ateapipb.ActorState_ACTOR_STATE_RESUMING, ateapipb.ActorState_ACTOR_STATE_RUNNING,
		ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
	default:
		return "", nil
	}
	workerName := assignment.GetWorker().GetName()
	worker, err := st.GetWorker(ctx, workerName)
	if errors.Is(err, store.ErrNotFound) {
		return "", nil
	} else if err != nil {
		return "", fmt.Errorf("while retrieving worker %s: %w", workerName, err)
	}
	return worker.GetNodeName(), nil
}
