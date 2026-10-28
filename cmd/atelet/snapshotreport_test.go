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
	"testing"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/snapshottracker"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	clocktesting "k8s.io/utils/clock/testing"
)

// fakeReportService is a WorkerServiceClient that records the snapshot state
// reports it receives, and fails them with err.
type fakeReportService struct {
	ateapi.WorkerServiceClient

	reports chan *ateapi.ReportActorSnapshotStateRequest
	err     error
}

func newFakeReportService() *fakeReportService {
	return &fakeReportService{reports: make(chan *ateapi.ReportActorSnapshotStateRequest, 10)}
}

func (s *fakeReportService) ReportActorSnapshotState(_ context.Context, in *ateapi.ReportActorSnapshotStateRequest, _ ...grpc.CallOption) (*ateapi.ReportActorSnapshotStateResponse, error) {
	s.reports <- in
	if s.err != nil {
		return nil, s.err
	}
	return &ateapi.ReportActorSnapshotStateResponse{}, nil
}

// TestTrackerReporterSendsState reports the actor's state as the control
// plane's ActorSnapshotState: each snapshot's fields inline, and a snapshot
// atelet does not hold left unset, so a detached actor reads as a detach.
func TestTrackerReporterSendsState(t *testing.T) {
	commit := timestamppb.New(time.Unix(1_700_000_000, 0))
	upload := timestamppb.New(time.Unix(1_700_000_060, 0))
	due := timestamppb.New(time.Unix(1_700_000_300, 0))
	local := func(name string, generation int32) *ateletpb.SnapshotReport {
		return &ateletpb.SnapshotReport{
			Snapshot:        lifecycleSnapshot(pluginActorUID, name, generation, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
			LocalCommitTime: commit,
		}
	}
	durable := local("s1", 1)
	durable.DurableCompleteTime = upload
	reported := func(name string, generation int32) *ateapi.SnapshotReport {
		return &ateapi.SnapshotReport{
			Uuid:            name,
			Generation:      generation,
			Fidelity:        ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			Object:          &ateapi.ObjectSnapshot{SnapshotUri: "gs://test-bucket/atespaces/ate-demo/actors/" + pluginActorUID + "/snapshots/" + name},
			LocalCommitTime: commit,
		}
	}
	reportedDurable := reported("s1", 1)
	reportedDurable.DurableCompleteTime = upload

	for _, tc := range []struct {
		name  string
		state *ateletpb.ActorSnapshotState
		want  *ateapi.ActorSnapshotState
	}{
		{
			name: "every snapshot",
			state: &ateletpb.ActorSnapshotState{
				LatestLocal:   local("s3", 3),
				Uploading:     local("s2", 2),
				LatestDurable: durable,
				UploadDueTime: due,
			},
			want: &ateapi.ActorSnapshotState{
				LatestLocal:   reported("s3", 3),
				Uploading:     reported("s2", 2),
				LatestDurable: reportedDurable,
				UploadDueTime: due,
			},
		},
		{
			name:  "detached",
			state: &ateletpb.ActorSnapshotState{LatestDurable: durable},
			want:  &ateapi.ActorSnapshotState{LatestDurable: reportedDurable},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workers := newFakeReportService()
			actor := resources.ActorRef{Atespace: "ate-demo", Name: "counter"}
			if err := (&trackerReporter{workers: workers}).ReportActorSnapshotState(t.Context(), pluginActorUID, actor, tc.state); err != nil {
				t.Fatalf("ReportActorSnapshotState: %v", err)
			}
			want := &ateapi.ReportActorSnapshotStateRequest{
				Actor:    &ateapi.ObjectRef{Atespace: "ate-demo", Name: "counter"},
				ActorUid: pluginActorUID,
				State:    tc.want,
			}
			if got := <-workers.reports; !proto.Equal(got, want) {
				t.Errorf("request = %v, want %v", got, want)
			}
		})
	}
}

// TestTrackerReporterReturnsRefusal returns the control plane's error
// unwrapped, so the tracker can tell a refusal from a failure to reach it.
func TestTrackerReporterReturnsRefusal(t *testing.T) {
	workers := newFakeReportService()
	workers.err = status.Error(codes.NotFound, "actor not found")
	state := &ateletpb.ActorSnapshotState{LatestDurable: &ateletpb.SnapshotReport{
		Snapshot: lifecycleSnapshot(pluginActorUID, "s1", 1, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
	}}
	err := (&trackerReporter{workers: workers}).ReportActorSnapshotState(t.Context(), pluginActorUID, resources.ActorRef{Atespace: "ate-demo", Name: "counter"}, state)
	if status.Code(err) != codes.NotFound {
		t.Errorf("ReportActorSnapshotState = %v, want NotFound", err)
	}
}

// TestToAteAPIFidelity maps every atelet fidelity onto the public one of the
// same name, and an unknown one onto UNSPECIFIED.
func TestToAteAPIFidelity(t *testing.T) {
	for value, name := range ateletpb.SnapshotFidelity_name {
		if got := toAteAPIFidelity(ateletpb.SnapshotFidelity(value)); got.String() != name {
			t.Errorf("toAteAPIFidelity(%s) = %s, want the public fidelity of the same name", name, got)
		}
	}
	if got := toAteAPIFidelity(ateletpb.SnapshotFidelity(99)); got != ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED {
		t.Errorf("toAteAPIFidelity(99) = %s, want UNSPECIFIED", got)
	}
}

// nextReport steps clk a second at a time until workers receives a report,
// and returns it. The tracker's queues arm their timers from a clock reading
// taken just before, so a step that lands in between leaves the timer late;
// further steps get past it.
func nextReport(t *testing.T, clk *clocktesting.FakeClock, workers *fakeReportService) *ateapi.ReportActorSnapshotStateRequest {
	t.Helper()
	var got *ateapi.ReportActorSnapshotStateRequest
	waitFor(t, "a snapshot state report", func() bool {
		select {
		case got = <-workers.reports:
			return true
		case <-time.After(10 * time.Millisecond):
			clk.Step(time.Second)
			return false
		}
	})
	return got
}

// TestBackgroundUploadAndDetachAreReported checkpoints an actor locally and
// leaves it suspended: atelet reports the background upload of its snapshot,
// then its background detach, to the control plane, for the actor its
// Checkpoint named.
func TestBackgroundUploadAndDetachAreReported(t *testing.T) {
	a := runPluginActor(t)
	clk := clocktesting.NewFakeClock(time.Now())
	workers := newFakeReportService()
	a.s.snapshots = snapshottracker.New(t.Context(), &trackerStore{s: a.s}, clk,
		snapshottracker.WithUploadDelay(time.Second),
		snapshottracker.WithDetachDelay(time.Hour),
		snapshottracker.WithReporter(&trackerReporter{workers: workers}))
	a.s.snapshots.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := a.s.snapshots.Wait(ctx); err != nil {
			t.Errorf("Wait: %v", err)
		}
	})
	if _, err := a.checkpoint(t, a.snapshot("pause-snap-1", 1), ateletpb.UploadMode_UPLOAD_MODE_DEFERRED); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	wantActor := &ateapi.ObjectRef{Atespace: "ate-demo", Name: "counter"}

	uploaded := nextReport(t, clk, workers)
	if !proto.Equal(uploaded.GetActor(), wantActor) || uploaded.GetActorUid() != pluginActorUID {
		t.Errorf("upload report for actor %v %q, want %v %q", uploaded.GetActor(), uploaded.GetActorUid(), wantActor, pluginActorUID)
	}
	if st := uploaded.GetState(); st.GetLatestDurable().GetUuid() != "pause-snap-1" || st.GetLatestLocal().GetUuid() != "pause-snap-1" {
		t.Errorf("upload report state = %v, want pause-snap-1 durable and still local", st)
	}

	clk.Step(time.Hour)
	detached := nextReport(t, clk, workers)
	if !proto.Equal(detached.GetActor(), wantActor) {
		t.Errorf("detach report for actor %v, want %v", detached.GetActor(), wantActor)
	}
	if st := detached.GetState(); st.GetLatestLocal() != nil || st.GetUploading() != nil || st.GetLatestDurable().GetUuid() != "pause-snap-1" {
		t.Errorf("detach report state = %v, want nothing local or uploading, and pause-snap-1 durable", st)
	}
	if exists(ateletpath.LocalSnapshotDir(pluginActorUID, "pause-snap-1")) {
		t.Error("pause-snap-1 is still on the node after the reported detach")
	}
}
