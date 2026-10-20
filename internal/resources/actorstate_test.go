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

import (
	"slices"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
)

// TestTransferStatesClassifyEveryState fails when a state is added to the
// enum without deciding whether a node transfers snapshots in it, since the
// mint's host check and the credential exchange both refuse unlisted states.
func TestTransferStatesClassifyEveryState(t *testing.T) {
	want := map[ateapi.ActorState]bool{
		ateapi.ActorState_ACTOR_STATE_UNSPECIFIED: false,
		ateapi.ActorState_ACTOR_STATE_RESUMING:    true,
		ateapi.ActorState_ACTOR_STATE_RUNNING:     true,
		ateapi.ActorState_ACTOR_STATE_SUSPENDING:  true,
		ateapi.ActorState_ACTOR_STATE_SUSPENDED:   true,
		ateapi.ActorState_ACTOR_STATE_CRASHED:     false,
		ateapi.ActorState_ACTOR_STATE_REVERTING:   false,
		ateapi.ActorState_ACTOR_STATE_DELETING:    false,
	}
	for value, name := range ateapi.ActorState_name {
		state := ateapi.ActorState(value)
		transfers, classified := want[state]
		if !classified {
			t.Errorf("%s is not classified: decide whether a node transfers snapshots in it and add it to this test", name)
			continue
		}
		if got := slices.Contains(TransferStates, state); got != transfers {
			t.Errorf("TransferStates contains %s = %v, want %v", name, got, transfers)
		}
	}
}
