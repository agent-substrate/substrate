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

type workerAssignmentReader interface {
	GetWorkerAssignment(ctx context.Context, workerName, actorUID string) (*ateapipb.ActorAssignment, error)
}

// workerHostsActor reports whether a Worker holds an assignment for actorUID.
// It asks the store: the Worker record does not carry its assignments, and the
// watch-fed cache cannot see a binding committed moments ago.
func workerHostsActor(ctx context.Context, st workerAssignmentReader, workerName, actorUID string) (bool, error) {
	_, err := st.GetWorkerAssignment(ctx, workerName, actorUID)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("while checking whether worker %q hosts actor %q: %w", workerName, actorUID, err)
	}
	return true, nil
}
