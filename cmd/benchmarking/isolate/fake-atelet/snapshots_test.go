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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

// ignoreReportTimes leaves out the times each SnapshotReport records.
var ignoreReportTimes = protocmp.IgnoreFields(&ateletpb.SnapshotReport{}, "local_commit_time", "durable_complete_time")

func testSnapshot(generation int32, uri string) *ateletpb.Snapshot {
	return &ateletpb.Snapshot{
		SnapshotUuid: fmt.Sprintf("snap-%d", generation),
		Generation:   generation,
		Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		Object:       &ateletpb.ObjectStorage{SnapshotUri: uri},
	}
}

func checkpointActor(t *testing.T, h *herder, snap *ateletpb.Snapshot, mode ateletpb.UploadMode) *ateletpb.ActorSnapshotState {
	t.Helper()
	resp, err := h.Checkpoint(context.Background(), &ateletpb.CheckpointRequest{
		ActorUid:     testActorUID,
		Snapshot:     snap,
		UploadConfig: &ateletpb.UploadConfig{Mode: mode},
	})
	if err != nil {
		t.Fatalf("Checkpoint(%v): %v", mode, err)
	}
	return resp.GetState()
}

func detachActor(ctx context.Context, h *herder) (*ateletpb.ActorSnapshotState, error) {
	resp, err := h.DetachActor(ctx, &ateletpb.DetachActorRequest{ActorUid: testActorUID})
	return resp.GetState(), err
}

// ate-api-server keeps an actor on its node when Checkpoint reports the new
// snapshot kept locally, and records it durable when the state says so.
func TestHerderCheckpointReportsState(t *testing.T) {
	snap := testSnapshot(1, testSnapshotURI)
	for _, tc := range []struct {
		mode ateletpb.UploadMode
		want *ateletpb.ActorSnapshotState
	}{
		{
			mode: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED,
			want: &ateletpb.ActorSnapshotState{LatestLocal: &ateletpb.SnapshotReport{Snapshot: snap}},
		},
		{
			mode: ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE,
			want: &ateletpb.ActorSnapshotState{
				LatestLocal:   &ateletpb.SnapshotReport{Snapshot: snap},
				LatestDurable: &ateletpb.SnapshotReport{Snapshot: snap},
			},
		},
	} {
		t.Run(tc.mode.String(), func(t *testing.T) {
			got := checkpointActor(t, &herder{storage: &recordingStorage{}}, snap, tc.mode)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform(), ignoreReportTimes); diff != "" {
				t.Errorf("Checkpoint state mismatch (-want +got):\n%s", diff)
			}
			if got.GetLatestLocal().GetLocalCommitTime() == nil {
				t.Error("Checkpoint reported no local commit time")
			}
			if got.GetLatestDurable() != nil && got.GetLatestDurable().GetDurableCompleteTime() == nil {
				t.Error("Checkpoint reported no durable complete time")
			}
		})
	}
}

// A deferred upload leaves the actor's earlier durable snapshot its latest
// durable one.
func TestHerderDeferredCheckpointKeepsEarlierDurable(t *testing.T) {
	h := &herder{storage: &recordingStorage{}}
	first, second := testSnapshot(1, testSnapshotURI), testSnapshot(2, testSnapshotURI+"-2")
	checkpointActor(t, h, first, ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE)
	got := checkpointActor(t, h, second, ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	want := &ateletpb.ActorSnapshotState{
		LatestLocal:   &ateletpb.SnapshotReport{Snapshot: second},
		LatestDurable: &ateletpb.SnapshotReport{Snapshot: first},
	}
	if diff := cmp.Diff(want, got, protocmp.Transform(), ignoreReportTimes); diff != "" {
		t.Errorf("Checkpoint state mismatch (-want +got):\n%s", diff)
	}
}

// ate-api-server detaches a suspended actor from a full node and resumes it
// elsewhere from its durable snapshot, so DetachActor makes the latest local
// snapshot durable and reports none kept locally. Once detached, the actor is
// forgotten.
func TestHerderDetachActor(t *testing.T) {
	const placeholder = "bench-bucket/benchmark-workloads/sleep/atespaces/team-a/actors/0f9c1d2e-3b4a-4c5d-8e6f-7a8b9c0d1e2f/snapshots/snap-1/" + placeholderObject
	snap := testSnapshot(1, testSnapshotURI)
	for _, tc := range []struct {
		name       string
		checkpoint ateletpb.UploadMode // unspecified takes no checkpoint
		want       *ateletpb.ActorSnapshotState
		wantWrites []string
	}{
		{
			name:       "deferred upload",
			checkpoint: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED,
			want:       &ateletpb.ActorSnapshotState{LatestDurable: &ateletpb.SnapshotReport{Snapshot: snap}},
			wantWrites: []string{placeholder},
		},
		{
			name:       "immediate upload",
			checkpoint: ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE,
			want:       &ateletpb.ActorSnapshotState{LatestDurable: &ateletpb.SnapshotReport{Snapshot: snap}},
			wantWrites: []string{placeholder}, // the Checkpoint's
		},
		{
			name: "no snapshot",
			want: &ateletpb.ActorSnapshotState{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage := &recordingStorage{}
			h := &herder{storage: storage}
			if tc.checkpoint != ateletpb.UploadMode_UPLOAD_MODE_UNSPECIFIED {
				checkpointActor(t, h, snap, tc.checkpoint)
			}
			got, err := detachActor(context.Background(), h)
			if err != nil {
				t.Fatalf("DetachActor: %v", err)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform(), ignoreReportTimes); diff != "" {
				t.Errorf("DetachActor state mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantWrites, storage.objects()); diff != "" {
				t.Errorf("objects written mismatch (-want +got):\n%s", diff)
			}
			again, err := detachActor(context.Background(), h)
			if err != nil {
				t.Fatalf("second DetachActor: %v", err)
			}
			if diff := cmp.Diff(&ateletpb.ActorSnapshotState{}, again, protocmp.Transform()); diff != "" {
				t.Errorf("second DetachActor state mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// Like atelet, DetachActor fails when the latest local snapshot has nowhere to
// be uploaded.
func TestHerderDetachActorWithoutObjectFails(t *testing.T) {
	h := &herder{storage: &recordingStorage{}}
	checkpointActor(t, h, testSnapshot(1, ""), ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	if _, err := detachActor(context.Background(), h); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("DetachActor: got %v, want %v", err, codes.FailedPrecondition)
	}
}

// A detach does not end with its call: a caller that gives up during the delay
// finds the actor detached when it calls again.
func TestHerderDetachActorCompletesWhenCallerGivesUp(t *testing.T) {
	// The state a deferred Checkpoint leaves, without waiting its hour.
	h := &herder{delays: uniformDelays(time.Hour), storage: &recordingStorage{}}
	h.snapshots.commitLocal(testActorUID, testSnapshot(1, testSnapshotURI), time.Now())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := detachActor(ctx, h); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DetachActor: got %v, want %v", err, context.DeadlineExceeded)
	}
	// With nothing left to detach, the second call returns without its delay.
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got, err := detachActor(ctx, h)
	if err != nil {
		t.Fatalf("second DetachActor: %v", err)
	}
	if diff := cmp.Diff(&ateletpb.ActorSnapshotState{}, got, protocmp.Transform()); diff != "" {
		t.Errorf("second DetachActor state mismatch (-want +got):\n%s", diff)
	}
}

// Like atelet, Terminate drops the actor's snapshot state.
func TestHerderTerminateForgetsSnapshots(t *testing.T) {
	storage := &recordingStorage{}
	h := &herder{storage: storage}
	checkpointActor(t, h, testSnapshot(1, testSnapshotURI), ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	if _, err := h.Terminate(context.Background(), &ateletpb.TerminateRequest{ActorUid: testActorUID}); err != nil {
		t.Fatalf("Terminate: %v", err)
	}
	got, err := detachActor(context.Background(), h)
	if err != nil {
		t.Fatalf("DetachActor: %v", err)
	}
	if diff := cmp.Diff(&ateletpb.ActorSnapshotState{}, got, protocmp.Transform()); diff != "" {
		t.Errorf("DetachActor state mismatch (-want +got):\n%s", diff)
	}
	if got := storage.objects(); len(got) != 0 {
		t.Errorf("wrote %v, want nothing for a terminated actor", got)
	}
}
