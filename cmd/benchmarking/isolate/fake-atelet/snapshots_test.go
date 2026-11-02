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
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
)

const testSnapshotURI2 = "gs://bench-bucket/benchmark-workloads/sleep/atespaces/team-a/actors/0f9c1d2e-3b4a-4c5d-8e6f-7a8b9c0d1e2f/snapshots/snap-2"

var (
	ignoreReportTimes = protocmp.IgnoreFields(&ateletpb.SnapshotReport{}, "local_commit_time", "durable_complete_time")
	ignoreAPITimes    = protocmp.IgnoreFields(&ateapi.SnapshotReport{}, "local_commit_time", "durable_complete_time")
)

func testSnapshot(uuid string, gen int32, uri string) *ateletpb.Snapshot {
	return &ateletpb.Snapshot{
		SnapshotUuid: uuid,
		Generation:   gen,
		Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		Object:       &ateletpb.ObjectStorage{SnapshotUri: uri},
	}
}

type recordingReporter struct {
	mu   sync.Mutex
	reqs []*ateapi.ReportActorSnapshotStateRequest
	errs []error
}

func newRecordingReporter() *recordingReporter {
	return &recordingReporter{}
}

func (r *recordingReporter) ReportActorSnapshotState(_ context.Context, in *ateapi.ReportActorSnapshotStateRequest, _ ...grpc.CallOption) (*ateapi.ReportActorSnapshotStateResponse, error) {
	r.mu.Lock()
	r.reqs = append(r.reqs, proto.CloneOf(in))
	var err error
	if len(r.errs) > 0 {
		err = r.errs[0]
		r.errs = r.errs[1:]
	}
	r.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &ateapi.ReportActorSnapshotStateResponse{}, nil
}

type signalingClock struct {
	*clocktesting.FakeClock
	timers chan time.Duration
}

func (c *signalingClock) NewTimer(d time.Duration) clock.Timer {
	t := c.FakeClock.NewTimer(d)
	select {
	case c.timers <- d:
	default:
	}
	return t
}

func (r *recordingReporter) reports() []*ateapi.ReportActorSnapshotStateRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.reqs)
}

// Checkpoint reports LatestLocal on a deferred upload and both LatestLocal and
// LatestDurable on an immediate upload, with commit timestamps populated.
func TestHerderCheckpointReportsState(t *testing.T) {
	snap := testSnapshot("snap-1", 1, testSnapshotURI)
	for name, tc := range map[string]struct {
		mode ateletpb.UploadMode
		want *ateletpb.ActorSnapshotState
	}{
		"deferred": {
			mode: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED,
			want: &ateletpb.ActorSnapshotState{
				LatestLocal: &ateletpb.SnapshotReport{Snapshot: snap},
			},
		},
		"immediate": {
			mode: ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE,
			want: &ateletpb.ActorSnapshotState{
				LatestLocal:   &ateletpb.SnapshotReport{Snapshot: snap},
				LatestDurable: &ateletpb.SnapshotReport{Snapshot: snap},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := &herder{storage: &recordingStorage{}}
			resp, err := h.Checkpoint(context.Background(), &ateletpb.CheckpointRequest{
				Atespace:     "team-a",
				ActorName:    "sleep-1",
				ActorUid:     testActorUID,
				Snapshot:     snap,
				UploadConfig: &ateletpb.UploadConfig{Mode: tc.mode},
			})
			if err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			if diff := cmp.Diff(tc.want, resp.GetState(), protocmp.Transform(), ignoreReportTimes); diff != "" {
				t.Errorf("Checkpoint state mismatch (-want +got):\n%s", diff)
			}
			if resp.GetState().GetLatestLocal().GetLocalCommitTime() == nil {
				t.Error("LatestLocal.LocalCommitTime is nil")
			}
			if tc.mode == ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE {
				if resp.GetState().GetLatestDurable().GetLocalCommitTime() == nil {
					t.Error("LatestDurable.LocalCommitTime is nil")
				}
				if resp.GetState().GetLatestDurable().GetDurableCompleteTime() == nil {
					t.Error("LatestDurable.DurableCompleteTime is nil")
				}
			}
		})
	}
}

// A deferred checkpoint after an immediate checkpoint advances LatestLocal
// while keeping the earlier LatestDurable intact.
func TestHerderDeferredCheckpointKeepsEarlierDurable(t *testing.T) {
	snap1 := testSnapshot("snap-1", 1, testSnapshotURI)
	snap2 := testSnapshot("snap-2", 2, testSnapshotURI2)
	h := &herder{storage: &recordingStorage{}}
	ctx := context.Background()

	if _, err := h.Checkpoint(ctx, &ateletpb.CheckpointRequest{
		ActorUid:     testActorUID,
		Snapshot:     snap1,
		UploadConfig: &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE},
	}); err != nil {
		t.Fatalf("first Checkpoint: %v", err)
	}
	resp, err := h.Checkpoint(ctx, &ateletpb.CheckpointRequest{
		ActorUid:     testActorUID,
		Snapshot:     snap2,
		UploadConfig: &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED},
	})
	if err != nil {
		t.Fatalf("second Checkpoint: %v", err)
	}
	want := &ateletpb.ActorSnapshotState{
		LatestLocal:   &ateletpb.SnapshotReport{Snapshot: snap2},
		LatestDurable: &ateletpb.SnapshotReport{Snapshot: snap1},
	}
	if diff := cmp.Diff(want, resp.GetState(), protocmp.Transform(), ignoreReportTimes); diff != "" {
		t.Errorf("Checkpoint state mismatch (-want +got):\n%s", diff)
	}
}

// Repeated deferred checkpoints keep the upload timer anchored to the first
// undurable checkpoint so an actor that suspends and resumes faster than
// defaultUploadDelay still uploads its latest snapshot on schedule.
func TestHerderDeferredCheckpointUploadsAndReportsAfterDelay(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0))
	storage := &recordingStorage{}
	reporter := newRecordingReporter()
	h := &herder{clk: clk, storage: storage, reporter: reporter}
	t.Cleanup(h.snapshots.close)
	ctx := context.Background()

	snap1 := testSnapshot("snap-1", 1, testSnapshotURI)
	snap2 := testSnapshot("snap-2", 2, testSnapshotURI2)

	if _, err := h.Checkpoint(ctx, &ateletpb.CheckpointRequest{
		Atespace:     "team-a",
		ActorName:    "sleep-1",
		ActorUid:     testActorUID,
		Snapshot:     snap1,
		UploadConfig: &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED},
	}); err != nil {
		t.Fatalf("first Checkpoint: %v", err)
	}

	// Advance 2m (< defaultUploadDelay), resume, and take a second deferred
	// checkpoint. The upload timer should stay due at 5m from the first.
	clk.Step(2 * time.Minute)
	if _, err := h.Restore(ctx, &ateletpb.RestoreRequest{ActorUid: testActorUID}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, err := h.Checkpoint(ctx, &ateletpb.CheckpointRequest{
		Atespace:     "team-a",
		ActorName:    "sleep-1",
		ActorUid:     testActorUID,
		Snapshot:     snap2,
		UploadConfig: &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED},
	}); err != nil {
		t.Fatalf("second Checkpoint: %v", err)
	}
	if got := len(reporter.reports()); got != 0 {
		t.Fatalf("got %d reports before defaultUploadDelay, want 0", got)
	}

	// Stepping the remaining 3m reaches defaultUploadDelay from snap1.
	clk.Step(defaultUploadDelay - 2*time.Minute)
	h.snapshots.wg.Wait()

	const wantObj = "bench-bucket/benchmark-workloads/sleep/atespaces/team-a/actors/0f9c1d2e-3b4a-4c5d-8e6f-7a8b9c0d1e2f/snapshots/snap-2/" + placeholderObject
	if got := storage.objects(); !slices.Equal(got, []string{wantObj}) {
		t.Errorf("uploaded %v, want [%s]", got, wantObj)
	}

	got := reporter.reports()
	if len(got) != 1 {
		t.Fatalf("got %d reports, want 1", len(got))
	}
	wantReport := &ateapi.ReportActorSnapshotStateRequest{
		Actor:    &ateapi.ObjectRef{Atespace: "team-a", Name: "sleep-1"},
		ActorUid: testActorUID,
		State: &ateapi.ActorSnapshotState{
			LatestLocal: &ateapi.SnapshotReport{
				Uuid:       "snap-2",
				Generation: 2,
				Fidelity:   ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
				Object:     &ateapi.ObjectSnapshot{SnapshotUri: testSnapshotURI2},
			},
			LatestDurable: &ateapi.SnapshotReport{
				Uuid:       "snap-2",
				Generation: 2,
				Fidelity:   ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
				Object:     &ateapi.ObjectSnapshot{SnapshotUri: testSnapshotURI2},
			},
		},
	}
	if diff := cmp.Diff(wantReport, got[0], protocmp.Transform(), ignoreAPITimes); diff != "" {
		t.Errorf("ReportActorSnapshotState mismatch (-want +got):\n%s", diff)
	}
}

// A suspended actor that stays idle for defaultDetachDelay detaches its local
// snapshot and reports LatestLocal cleared; starting the actor again before the
// delay cancels the detach until the next Checkpoint.
func TestHerderBackgroundDetachReportsAfterDelay(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0))
	reporter := newRecordingReporter()
	h := &herder{clk: clk, storage: &recordingStorage{}, reporter: reporter}
	t.Cleanup(h.snapshots.close)
	ctx := context.Background()

	snap := testSnapshot("snap-1", 1, testSnapshotURI)
	if _, err := h.Checkpoint(ctx, &ateletpb.CheckpointRequest{
		Atespace:     "team-a",
		ActorName:    "sleep-1",
		ActorUid:     testActorUID,
		Snapshot:     snap,
		UploadConfig: &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE},
	}); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	// Restoring the actor unschedules the background detach.
	if _, err := h.Restore(ctx, &ateletpb.RestoreRequest{ActorUid: testActorUID}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	clk.Step(defaultDetachDelay)
	h.snapshots.wg.Wait()
	if got := len(reporter.reports()); got != 0 {
		t.Fatalf("got %d detach reports while running, want 0", got)
	}

	// Checkpointing again schedules a fresh detach at defaultDetachDelay.
	if _, err := h.Checkpoint(ctx, &ateletpb.CheckpointRequest{
		Atespace:     "team-a",
		ActorName:    "sleep-1",
		ActorUid:     testActorUID,
		Snapshot:     snap,
		UploadConfig: &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE},
	}); err != nil {
		t.Fatalf("second Checkpoint: %v", err)
	}
	clk.Step(defaultDetachDelay)
	h.snapshots.wg.Wait()

	got := reporter.reports()
	if len(got) != 1 {
		t.Fatalf("got %d reports, want 1", len(got))
	}
	wantReport := &ateapi.ReportActorSnapshotStateRequest{
		Actor:    &ateapi.ObjectRef{Atespace: "team-a", Name: "sleep-1"},
		ActorUid: testActorUID,
		State: &ateapi.ActorSnapshotState{
			LatestDurable: &ateapi.SnapshotReport{
				Uuid:       "snap-1",
				Generation: 1,
				Fidelity:   ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
				Object:     &ateapi.ObjectSnapshot{SnapshotUri: testSnapshotURI},
			},
		},
	}
	if diff := cmp.Diff(wantReport, got[0], protocmp.Transform(), ignoreAPITimes); diff != "" {
		t.Errorf("ReportActorSnapshotState mismatch (-want +got):\n%s", diff)
	}
	if state := h.snapshots.get(testActorUID); state.GetLatestLocal() != nil || state.GetLatestDurable() != nil {
		t.Errorf("actor snapshot state after background detach = %v, want forgotten", state)
	}
}

// ReportActorSnapshotState retries when ate-api-server returns Aborted while a
// workflow owns the actor.
func TestHerderReportRetriesOnAborted(t *testing.T) {
	clk := &signalingClock{
		FakeClock: clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0)),
		timers:    make(chan time.Duration, 4),
	}
	reporter := newRecordingReporter()
	reporter.errs = []error{status.Error(codes.Aborted, "actor busy")}
	h := &herder{clk: clk, storage: &recordingStorage{}, reporter: reporter}
	t.Cleanup(h.snapshots.close)

	snap := testSnapshot("snap-1", 1, testSnapshotURI)
	if _, err := h.Checkpoint(context.Background(), &ateletpb.CheckpointRequest{
		Atespace:     "team-a",
		ActorName:    "sleep-1",
		ActorUid:     testActorUID,
		Snapshot:     snap,
		UploadConfig: &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED},
	}); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	clk.Step(defaultUploadDelay)
	if got := <-clk.timers; got != reportRetryDelay {
		t.Fatalf("retry timer = %v, want %v", got, reportRetryDelay)
	}
	clk.Step(reportRetryDelay)
	h.snapshots.wg.Wait()

	if got := len(reporter.reports()); got != 2 {
		t.Fatalf("got %d report attempts, want 2", got)
	}
}

// Terminate drops the actor's recorded state and cancels its pending
// background timers.
func TestHerderTerminateForgetsSnapshots(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Unix(1_700_000_000, 0))
	reporter := newRecordingReporter()
	h := &herder{clk: clk, storage: &recordingStorage{}, reporter: reporter}
	t.Cleanup(h.snapshots.close)
	ctx := context.Background()

	snap := testSnapshot("snap-1", 1, testSnapshotURI)
	if _, err := h.Checkpoint(ctx, &ateletpb.CheckpointRequest{
		Atespace:     "team-a",
		ActorName:    "sleep-1",
		ActorUid:     testActorUID,
		Snapshot:     snap,
		UploadConfig: &ateletpb.UploadConfig{Mode: ateletpb.UploadMode_UPLOAD_MODE_DEFERRED},
	}); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if _, err := h.Terminate(ctx, &ateletpb.TerminateRequest{ActorUid: testActorUID}); err != nil {
		t.Fatalf("Terminate: %v", err)
	}
	if got := h.snapshots.get(testActorUID); got.GetLatestLocal() != nil || got.GetLatestDurable() != nil {
		t.Errorf("state after Terminate = %v, want empty", got)
	}

	clk.Step(defaultDetachDelay)
	h.snapshots.wg.Wait()
	if got := len(reporter.reports()); got != 0 {
		t.Errorf("got %d reports after Terminate, want 0", got)
	}
}
