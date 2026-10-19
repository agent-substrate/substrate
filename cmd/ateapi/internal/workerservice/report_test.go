// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package workerservice

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.ate.dev/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

const reportActorUID = "5f2c1a90-7b34-4e6d-8a11-0c3e9d5b7f42"

func reportRequest() *ateapipb.ReportActorSnapshotStateRequest {
	durable := &ateapipb.SnapshotReport{
		Uuid:            "snap-1",
		Generation:      1,
		Fidelity:        ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		Object:          &ateapipb.ObjectSnapshot{SnapshotUri: "gs://bucket/actors/a/snap-1"},
		LocalCommitTime: timestamppb.Now(),
	}
	return &ateapipb.ReportActorSnapshotStateRequest{
		Actor:    &ateapipb.ObjectRef{Atespace: suspendAtespace, Name: suspendActor},
		ActorUid: reportActorUID,
		State:    &ateapipb.ActorSnapshotState{LatestDurable: durable},
	}
}

// A report reaches the control plane with the node from the caller's
// certificate. The handler reads nothing from the store.
func TestReportActorSnapshotState(t *testing.T) {
	control := &fakeControl{}
	s := New(nil, control, testAteletSPIFFEID, nil, "", nil)

	req := reportRequest()
	if _, err := s.ReportActorSnapshotState(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)), req); err != nil {
		t.Fatalf("ReportActorSnapshotState() failed: %v", err)
	}
	if len(control.reports) != 1 {
		t.Fatalf("got %d applied reports, want 1", len(control.reports))
	}
	got := control.reports[0]
	if want := (resources.ActorRef{Atespace: suspendAtespace, Name: suspendActor}); got.actorRef != want {
		t.Errorf("actorRef = %v, want %v", got.actorRef, want)
	}
	if got.actorUID != reportActorUID || got.node != testNode {
		t.Errorf("applied (uid %q, node %q), want (%q, %q)", got.actorUID, got.node, reportActorUID, testNode)
	}
	if !proto.Equal(got.state, req.GetState()) {
		t.Errorf("applied state = %v, want %v", got.state, req.GetState())
	}
}

// A caller that isn't atelet, or a malformed request, doesn't reach the control
// plane. A refusal from the control plane reaches atelet unchanged.
func TestReportActorSnapshotState_Refused(t *testing.T) {
	onNode := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))
	invalid := reportRequest()
	invalid.State = nil

	tests := []struct {
		name        string
		ctx         context.Context
		req         *ateapipb.ReportActorSnapshotStateRequest
		controlErr  error
		want        codes.Code
		wantApplied bool
	}{
		{name: "unauthenticated", ctx: ateletauthtest.ContextWith(nil), req: reportRequest(), want: codes.Unauthenticated},
		{name: "not atelet", ctx: ateletauthtest.ContextWith(ateletauthtest.CertIn(t, "other-namespace", testNode)), req: reportRequest(), want: codes.PermissionDenied},
		{name: "invalid request", ctx: onNode, req: invalid, want: codes.InvalidArgument},
		{name: "control plane refuses", ctx: onNode, req: reportRequest(), controlErr: apierror.NotFound("Actor not found"), want: codes.NotFound, wantApplied: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			control := &fakeControl{reportErr: tt.controlErr}
			s := New(nil, control, testAteletSPIFFEID, nil, "", nil)
			_, err := s.ReportActorSnapshotState(tt.ctx, tt.req)
			if got := apierror.Code(err); got != tt.want {
				t.Fatalf("ReportActorSnapshotState() error = %v, want code %v", err, tt.want)
			}
			if applied := len(control.reports) > 0; applied != tt.wantApplied {
				t.Errorf("report reached the control plane = %v, want %v", applied, tt.wantApplied)
			}
		})
	}
}
