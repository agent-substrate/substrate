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

package resources

import "go.ate.dev/substrate/pkg/proto/ateapipb"

// TransferStates are the states in which a node may be moving the actor's
// snapshot between itself and object storage:
//   - RESUMING: a resume fetches the snapshot.
//   - RUNNING: the actor keeps the credentials its resume minted.
//   - SUSPENDING: a suspend uploads the snapshot.
//   - SUSPENDED: a paused actor's background upload runs.
//
// In every other state no node acts for the actor. ate-api-server's
// worker-for-actor mints and the objectstore plugin's credential exchange
// refuse a new credential outside this list; one already issued stays valid
// until it expires. Inside the list they can disagree: the mints' HostingNode
// also needs the store's worker row, so between a suspend's releaseWorker and
// its finalize write the mints refuse while the exchange, which reads only
// the state and status.assigned_node, still issues.
var TransferStates = []ateapipb.ActorState{
	ateapipb.ActorState_ACTOR_STATE_RESUMING,
	ateapipb.ActorState_ACTOR_STATE_RUNNING,
	ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
	ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
}
