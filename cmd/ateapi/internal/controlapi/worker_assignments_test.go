// Copyright 2026 The Agent Substrate Authors
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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
)

type assignmentReaderFunc func(workerName, actorUID string) (*ateapi.ActorAssignment, error)

func (f assignmentReaderFunc) GetWorkerAssignment(_ context.Context, workerName, actorUID string) (*ateapi.ActorAssignment, error) {
	return f(workerName, actorUID)
}

func TestWorkerHostsActor(t *testing.T) {
	for _, tc := range []struct {
		name       string
		assignment *ateapi.ActorAssignment
		err        error
		want       bool
	}{
		{name: "running", assignment: &ateapi.ActorAssignment{State: ateapi.ActorAssignmentState_ACTOR_ASSIGNMENT_STATE_RUNNING}, want: true},
		{name: "unset state", assignment: &ateapi.ActorAssignment{}, want: true},
		{name: "suspended", assignment: &ateapi.ActorAssignment{State: ateapi.ActorAssignmentState_ACTOR_ASSIGNMENT_STATE_SUSPENDED}},
		{name: "none", err: store.ErrNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := assignmentReaderFunc(func(string, string) (*ateapi.ActorAssignment, error) { return tc.assignment, tc.err })
			got, err := workerHostsActor(context.Background(), st, "worker", "uid")
			if err != nil || got != tc.want {
				t.Errorf("workerHostsActor() = %v, %v, want %v", got, err, tc.want)
			}
		})
	}
}
