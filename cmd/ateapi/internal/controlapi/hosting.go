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
	"slices"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/resources"
)

// HostingNode is the node that holds the actor, and "" when no node does. The
// actor's state decides which status field to trust, since a crash keeps
// status.assigned_node as a record of the last node. The rules:
//   - SUSPENDED: assigned_node. It is set only while a snapshot whose
//     locality is that node still holds files there, uploaded or not, and
//     cleared once none does.
//   - RESUMING, RUNNING and SUSPENDING: the node of the worker in
//     status.worker_assignment, which the worker's assignment row is written
//     with.
//   - Anything else: "".
//
// WorkerService uses it to refuse a node that asks for credentials for an
// actor it does not hold.
func HostingNode(actor *ateapi.Actor) string {
	status := actor.GetStatus()
	// The branches below pick the field to trust within the states a node
	// transfers in.
	if !slices.Contains(resources.TransferStates, status.GetState()) {
		return ""
	}
	// Of these states, only a SUSPENDED actor names no worker.
	if assignment := status.GetWorkerAssignment(); assignment != nil {
		return assignment.GetNodeName()
	}
	return status.GetAssignedNode()
}
