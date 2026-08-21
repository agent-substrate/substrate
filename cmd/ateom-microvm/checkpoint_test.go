//go:build linux

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
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/checkpointmarker"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/roottest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// testScope stands in for a CheckpointWorkloadRequest's stringified scope.
var testScope = ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL.String()

// newCheckpointTestService builds a minimal AteomService for testing pre-sandbox checkpoint logic.
func newCheckpointTestService() *AteomService {
	return &AteomService{
		lock:           newCancelableMutex(),
		atunnelIngress: &atunnel.Server{},
		atunnelEgress:  &atunnel.Egress{},
		actorLogger:    actorlog.NewActorLogger(io.Discard, false),
		running:        make(map[string]*runningActor),
	}
}

func useTempActorsDir(t *testing.T, actorUID string) string {
	t.Helper()
	orig := ateompath.ActorsDir
	t.Cleanup(func() { ateompath.ActorsDir = orig })
	ateompath.ActorsDir = t.TempDir()

	dir := ateompath.CheckpointStateDir(actorUID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("creating checkpoint dir: %v", err)
	}
	return dir
}

func TestListFilesExcludesCompletionMarker(t *testing.T) {
	const actorUID = "actor-1"
	dir := useTempActorsDir(t, actorUID)

	for _, name := range []string{"snapshot.state", ateompath.CheckpointDoneFileName} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatalf("writing %s: %v", name, err)
		}
	}

	got, err := listFiles(dir)
	if err != nil {
		t.Fatalf("listFiles: %v", err)
	}
	want := []string{"snapshot.state"}
	if !slices.Equal(got, want) {
		t.Errorf("listFiles = %v, want %v", got, want)
	}
}

// If the worker has already been reassigned to a successor actor, replaying
// a completed checkpoint must skip teardown to avoid disrupting the new actor.
func TestCheckpointWorkloadReplaySkipsTeardownForAReassignedAteom(t *testing.T) {
	const actorUID = "actor-1"
	useTempActorsDir(t, actorUID)

	want := []string{"snapshot.mem", "snapshot.state"}
	if err := checkpointmarker.Write(actorUID, ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL.String(), want); err != nil {
		t.Fatalf("checkpointmarker.Write: %v", err)
	}

	successor := &resources.ActorAttribution{
		Ref: resources.ActorRef{Atespace: "ate-demo", Name: "counter-2"},
		UID: "actor-2",
	}
	s := newCheckpointTestService()
	s.activeActor.Store(successor)
	s.guestStats.Store(&guestStatsTarget{actorUID: successor.UID})

	resp, err := s.CheckpointWorkload(context.Background(), &ateompb.CheckpointWorkloadRequest{
		Atespace:  "ate-demo",
		ActorName: "counter-1",
		ActorUid:  actorUID,
		Scope:     ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
	})
	if err != nil {
		t.Fatalf("CheckpointWorkload: %v", err)
	}
	if !slices.Equal(resp.GetSnapshotFiles(), want) {
		t.Errorf("SnapshotFiles = %v, want %v", resp.GetSnapshotFiles(), want)
	}

	if got := s.activeActor.Load(); got != successor {
		t.Errorf("activeActor = %v, want the successor %v left untouched", got, successor)
	}
	if got := s.guestStats.Load(); got == nil || got.actorUID != successor.UID {
		t.Errorf("guestStats = %v, want the successor's target left untouched", got)
	}
}

// A marker from a differently-scoped checkpoint must not be replayed.
func TestCheckpointWorkloadDoesNotReplayADifferentScope(t *testing.T) {
	const actorUID = "actor-1"
	useTempActorsDir(t, actorUID)

	if err := checkpointmarker.Write(actorUID, ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA.String(), []string{"durable-dir.tar"}); err != nil {
		t.Fatalf("checkpointmarker.Write: %v", err)
	}

	s := newCheckpointTestService()
	resp, err := s.CheckpointWorkload(context.Background(), &ateompb.CheckpointWorkloadRequest{
		Atespace:  "ate-demo",
		ActorName: "counter-1",
		ActorUid:  actorUID,
		Scope:     ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
	})
	if err == nil {
		t.Fatalf("CheckpointWorkload succeeded (files=%v), want a failure rather than a replay of the DATA snapshot", resp.GetSnapshotFiles())
	}
	if slices.Contains(resp.GetSnapshotFiles(), "durable-dir.tar") {
		t.Errorf("SnapshotFiles = %v, want the DATA marker's files not replayed", resp.GetSnapshotFiles())
	}
}

// When the ateom is still assigned to the checkpointed actor, replaying a
// completed checkpoint runs teardown to clean up the actor's state.
func TestCheckpointWorkloadReplayCleansUpWhenNotReassigned(t *testing.T) {
	roottest.Require(t, "teardown touches network namespaces and mounts")

	const actorUID = "actor-1"
	useTempActorsDir(t, actorUID)

	want := []string{"snapshot.mem", "snapshot.state"}
	if err := checkpointmarker.Write(actorUID, testScope, want); err != nil {
		t.Fatalf("checkpointmarker.Write: %v", err)
	}

	current := &resources.ActorAttribution{
		Ref: resources.ActorRef{Atespace: "ate-demo", Name: "counter-1"},
		UID: actorUID,
	}
	s := newCheckpointTestService()
	s.activeActor.Store(current)
	s.guestStats.Store(&guestStatsTarget{actorUID: actorUID})
	s.running[actorUID] = &runningActor{}

	resp, err := s.CheckpointWorkload(context.Background(), &ateompb.CheckpointWorkloadRequest{
		Atespace:  "ate-demo",
		ActorName: "counter-1",
		ActorUid:  actorUID,
		Scope:     ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
	})
	if err != nil {
		t.Fatalf("CheckpointWorkload: %v", err)
	}
	if !slices.Equal(resp.GetSnapshotFiles(), want) {
		t.Errorf("SnapshotFiles = %v, want %v", resp.GetSnapshotFiles(), want)
	}

	if got := s.activeActor.Load(); got != nil {
		t.Errorf("activeActor = %v, want nil after replay cleanup", got)
	}
	if got := s.guestStats.Load(); got != nil {
		t.Errorf("guestStats = %v, want nil after replay cleanup", got)
	}
	if _, exists := s.running[actorUID]; exists {
		t.Errorf("running[%q] still exists, want removed after replay cleanup", actorUID)
	}
}

func TestCheckpointWorkloadScopePreconditions(t *testing.T) {
	tests := []struct {
		name     string
		scope    ateompb.SnapshotScope
		spec     *ateompb.WorkloadSpec
		wantCode codes.Code
	}{
		{
			name:     "data scope without volumes fails precondition",
			scope:    ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
			spec:     &ateompb.WorkloadSpec{},
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "unsupported scope returns invalid argument",
			scope:    ateompb.SnapshotScope_SNAPSHOT_SCOPE_UNSPECIFIED,
			wantCode: codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const actorUID = "actor-1"
			useTempActorsDir(t, actorUID)

			s := newCheckpointTestService()
			_, err := s.CheckpointWorkload(context.Background(), &ateompb.CheckpointWorkloadRequest{
				Atespace:  "ate-demo",
				ActorName: "counter-1",
				ActorUid:  actorUID,
				Scope:     tt.scope,
				Spec:      tt.spec,
			})
			if gotCode := status.Code(err); gotCode != tt.wantCode {
				t.Errorf("CheckpointWorkload code = %v, want %v (err: %v)", gotCode, tt.wantCode, err)
			}
		})
	}
}
