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

package scheduling

import (
	"context"
	"errors"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/hardware"
)

// resumeConstraints and scheduleResume are how assignWorkerAttempt asks for a
// placement: the snapshot's runtime is required and, when the actor may fall
// back, a second Schedule with the requirement dropped follows ErrNoCapacity.

func resumeConstraints(c Constraints, snap hardware.Requirement) Constraints {
	c.RequiredRuntime = snap
	c.SnapshotRuntime = snap
	return c
}

func scheduleResume(ctx context.Context, s Scheduler, c Constraints, relaxable bool) (*ateapi.Worker, error) {
	w, err := s.Schedule(ctx, c)
	if errors.Is(err, ErrNoCapacity) && relaxable {
		c.RequiredRuntime = hardware.Requirement{}
		return s.Schedule(ctx, c)
	}
	return w, err
}
