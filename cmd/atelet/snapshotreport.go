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

	"go.ate.dev/substrate/cmd/atelet/internal/snapshottracker"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// trackerReporter is the snapshottracker.Reporter that reports to the control
// plane's WorkerService, with atelet's own client certificate.
type trackerReporter struct{ workers ateapipb.WorkerServiceClient }

var _ snapshottracker.Reporter = (*trackerReporter)(nil)

// ReportActorSnapshotState sends state with
// WorkerService.ReportActorSnapshotState, and returns the control plane's
// error unwrapped, so the tracker can tell a refusal from a failure to reach
// it.
func (r *trackerReporter) ReportActorSnapshotState(ctx context.Context, actorUID string, actor resources.ActorRef, state *ateletpb.ActorSnapshotState) error {
	_, err := r.workers.ReportActorSnapshotState(ctx, &ateapipb.ReportActorSnapshotStateRequest{
		Actor:    actor.ToObjectRef(),
		ActorUid: actorUID,
		State:    toAteAPIActorSnapshotState(state),
	})
	return err
}

// toAteAPIActorSnapshotState converts atelet's ActorSnapshotState to the
// control plane's, which carries each snapshot's fields inline. A snapshot
// atelet does not hold stays unset, since the control plane reads a state with
// neither latest_local nor uploading as a detach.
func toAteAPIActorSnapshotState(in *ateletpb.ActorSnapshotState) *ateapipb.ActorSnapshotState {
	if in == nil {
		return nil
	}
	return &ateapipb.ActorSnapshotState{
		LatestLocal:   toAteAPISnapshotReport(in.GetLatestLocal()),
		Uploading:     toAteAPISnapshotReport(in.GetUploading()),
		LatestDurable: toAteAPISnapshotReport(in.GetLatestDurable()),
		UploadDueTime: in.GetUploadDueTime(),
	}
}

// toAteAPISnapshotReport converts one of atelet's SnapshotReports to the
// control plane's. A report that names no snapshot converts to nil.
func toAteAPISnapshotReport(in *ateletpb.SnapshotReport) *ateapipb.SnapshotReport {
	snap := in.GetSnapshot()
	if snap == nil {
		return nil
	}
	out := &ateapipb.SnapshotReport{
		Uuid:                snap.GetSnapshotUuid(),
		Generation:          snap.GetGeneration(),
		Fidelity:            toAteAPIFidelity(snap.GetFidelity()),
		LocalCommitTime:     in.GetLocalCommitTime(),
		DurableCompleteTime: in.GetDurableCompleteTime(),
	}
	if obj := snap.GetObject(); obj != nil {
		out.Object = &ateapipb.ObjectSnapshot{SnapshotUri: obj.GetSnapshotUri()}
	}
	return out
}

// toAteAPIFidelity maps atelet's fidelity enum onto the identical public one,
// level by level. Anything else goes out as UNSPECIFIED, which the control
// plane rejects.
func toAteAPIFidelity(in ateletpb.SnapshotFidelity) ateapipb.SnapshotFidelity {
	switch in {
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES:
		return ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS:
		return ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY:
		return ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	default:
		return ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
	}
}
