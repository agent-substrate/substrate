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
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/checkpointmarker"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/roottest"
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
	}
}

// useTempActorsDir points the shared actor-state root at a temp directory and
// creates the actor's checkpoint dir.
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

func TestListSnapshotFilesExcludesCompletionMarker(t *testing.T) {
	const actorUID = "actor-1"
	dir := useTempActorsDir(t, actorUID)

	for _, name := range []string{"checkpoint.img", "pages.img"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatalf("writing %s: %v", name, err)
		}
	}
	if err := checkpointmarker.Write(actorUID, testScope, []string{"checkpoint.img", "pages.img"}); err != nil {
		t.Fatalf("checkpointmarker.Write: %v", err)
	}

	got, err := listSnapshotFiles(dir)
	if err != nil {
		t.Fatalf("listSnapshotFiles: %v", err)
	}
	want := []string{"checkpoint.img", "pages.img"}
	if !slices.Equal(got, want) {
		t.Errorf("listSnapshotFiles = %v, want %v", got, want)
	}
}

// If the worker has already been reassigned to a successor actor, replaying
// a completed checkpoint must skip teardown to avoid disrupting the new actor.
func TestCheckpointWorkloadReplaySkipsTeardownForAReassignedAteom(t *testing.T) {
	const actorUID = "actor-1"
	useTempActorsDir(t, actorUID)

	want := []string{"checkpoint.img", "pages.img"}
	if err := checkpointmarker.Write(actorUID, testScope, want); err != nil {
		t.Fatalf("checkpointmarker.Write: %v", err)
	}

	// The ateom has moved on: it now runs a different actor.
	successor := &resources.ActorAttribution{
		Ref: resources.ActorRef{Atespace: "ate-demo", Name: "counter-2"},
		UID: "actor-2",
	}
	s := newCheckpointTestService()
	s.activeActor.Store(successor)

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

func TestSandboxNotFound(t *testing.T) {
	tests := []struct {
		name string
		out  string
		want bool
	}{
		{"container absent", `error: loading container: container "pause" does not exist`, true},
		{"control server unresponsive", "error: connecting to control server: connection refused", false},
		{"runsc binary missing", "fork/exec /usr/bin/runsc: no such file or directory", false},
		{"probe timed out", "signal: killed", false},
		{"no output at all", "", false},
		{
			"phrase in log line but not verdict",
			`{"msg":"cgroup path \"/sys/fs/cgroup/runsc\" does not exist, skipping","level":"warning"}
error: connecting to control server: connection refused`,
			false,
		},
		{
			"verdict after log noise",
			`{"msg":"loading container","level":"info"}
error: loading container: container "pause" does not exist`,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sandboxNotFound([]byte(tt.out)); got != tt.want {
				t.Errorf("sandboxNotFound(%q) = %v, want %v", tt.out, got, tt.want)
			}
		})
	}
}

// Retried checkpoints must clear stale checkpoint files before writing new images.
func TestCheckpointWorkloadClearsStaleCheckpointFiles(t *testing.T) {
	const actorUID = "actor-1"
	dir := useTempActorsDir(t, actorUID)

	stale := filepath.Join(dir, "pages.img")
	if err := os.WriteFile(stale, []byte("half-written"), 0o600); err != nil {
		t.Fatalf("writing stale image: %v", err)
	}

	s := newCheckpointTestService()
	if _, err := s.CheckpointWorkload(context.Background(), &ateompb.CheckpointWorkloadRequest{
		Atespace:  "ate-demo",
		ActorName: "counter-1",
		ActorUid:  actorUID,
		Scope:     ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
	}); err == nil {
		t.Fatal("CheckpointWorkload succeeded, want a failure with no runsc to drive")
	}

	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("os.Stat(%q) = %v, want the previous attempt's image to be gone", stale, err)
	}
}

// When the ateom is still assigned to the checkpointed actor, replaying a
// completed checkpoint runs teardown and clears the active session.
func TestCheckpointWorkloadReplayCleansUpWhenNotReassigned(t *testing.T) {
	roottest.Require(t, "teardown touches network namespaces and mounts")

	const actorUID = "actor-1"
	useTempActorsDir(t, actorUID)

	want := []string{"checkpoint.img", "pages.img"}
	if err := checkpointmarker.Write(actorUID, testScope, want); err != nil {
		t.Fatalf("checkpointmarker.Write: %v", err)
	}

	current := &resources.ActorAttribution{
		Ref: resources.ActorRef{Atespace: "ate-demo", Name: "counter-1"},
		UID: actorUID,
	}
	s := newCheckpointTestService()
	s.activeActor.Store(current)
	s.activeSession = &workloadSession{}

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
	if s.activeSession != nil {
		t.Errorf("activeSession = %v, want nil after replay cleanup", s.activeSession)
	}
}

func TestCheckpointWorkloadScopeValidation(t *testing.T) {
	tests := []struct {
		name        string
		scope       ateompb.SnapshotScope
		spec        *ateompb.WorkloadSpec
		errContains string
	}{
		{
			name:        "data scope without durable volumes",
			scope:       ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
			spec:        &ateompb.WorkloadSpec{},
			errContains: "no durable-dir volumes found",
		},
		{
			name:        "unsupported scope",
			scope:       ateompb.SnapshotScope_SNAPSHOT_SCOPE_UNSPECIFIED,
			errContains: "unsupported snapshot scope",
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
			if err == nil || !strings.Contains(err.Error(), tt.errContains) {
				t.Errorf("CheckpointWorkload err = %v, want error containing %q", err, tt.errContains)
			}
		})
	}
}
