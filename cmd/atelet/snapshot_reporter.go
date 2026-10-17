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

package main

import (
	"context"

	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/snapshotstate"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// snapshotReporter is the snapshottracker.Reporter that reports an actor's
// snapshot state to ate-api-server over WorkerService, with atelet's own
// client certificate.
type snapshotReporter struct {
	workers ateapipb.WorkerServiceClient
}

// ReportActorSnapshotState sends state, what atelet holds for the actor, to
// ate-api-server.
func (r *snapshotReporter) ReportActorSnapshotState(ctx context.Context, actor resources.ActorRef, actorUID string, state *ateletpb.ActorSnapshotState) error {
	_, err := r.workers.ReportActorSnapshotState(ctx, &ateapipb.ReportActorSnapshotStateRequest{
		Actor:    &ateapipb.ObjectRef{Atespace: actor.Atespace, Name: actor.Name},
		ActorUid: actorUID,
		State:    snapshotstate.ToAPI(state),
	})
	return err
}
