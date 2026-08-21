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
	"testing"

	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/checkpointmarker"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// testScope stands in for a CheckpointWorkloadRequest's stringified scope.
var testScope = ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL.String()

// newCheckpointTestService builds the least AteomService CheckpointWorkload can
// be driven with. Only the collaborators it reaches before touching the sandbox
// are supplied, and each is inert: the atunnel servers hold no activation, so
// Deactivate is a no-op, and the logger writes nowhere. Everything past that
// point needs a real runsc, so a checkpoint driven here always fails — which is
// what the tests below want, since they assert on what happens *before* it.
//
// The fields are supplied rather than left zero because CheckpointWorkload
// calls straight into them: s.lock on its first line, then s.atunnelIngress and
// s.actorLogger, all of which panic on a nil pointer.
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
	// The marker is ateom's bookkeeping. Shipping it as snapshot content would
	// put it in the manifest, and a restore would then expect it back.
	want := []string{"checkpoint.img", "pages.img"}
	if !slices.Equal(got, want) {
		t.Errorf("listSnapshotFiles = %v, want %v", got, want)
	}
}

func TestCheckpointWorkloadReplaysCompletedCheckpoint(t *testing.T) {
	const actorUID = "actor-1"
	useTempActorsDir(t, actorUID)

	want := []string{"checkpoint.img", "pages.img"}
	if err := checkpointmarker.Write(actorUID, testScope, want); err != nil {
		t.Fatalf("checkpointmarker.Write: %v", err)
	}

	// No sandbox and no runsc path: reaching the checkpoint itself would fail,
	// so a success here can only be the replay.
	s := newCheckpointTestService()
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
		t.Errorf("SnapshotFiles = %v, want %v (the recorded result, replayed verbatim)", resp.GetSnapshotFiles(), want)
	}
}

// A marker outlives the attempt that wrote it until resetActorDirs clears it,
// so a late retry can arrive after the ateom has been handed to another actor.
// Parts of the post-checkpoint teardown are the ateom's rather than the
// actor's — the interior network, the stats attribution — so running it then
// would cut the network out from under the actor now holding the ateom.
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
	// The recorded result is still owed to the caller: the checkpoint did
	// happen, and only its teardown is someone else's business now.
	if !slices.Equal(resp.GetSnapshotFiles(), want) {
		t.Errorf("SnapshotFiles = %v, want %v (the recorded result, replayed verbatim)", resp.GetSnapshotFiles(), want)
	}

	if got := s.activeActor.Load(); got != successor {
		t.Errorf("activeActor = %v, want the successor %v left untouched", got, successor)
	}
}

// A marker from a differently-scoped checkpoint is not this checkpoint's
// result, so it must not be replayed as one. The request falls through to the
// ordinary path, where the absent sandbox is reported for what it is.
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
	// Falling through to the real checkpoint is the point: with no runsc it
	// fails, where a replay would have returned the DATA file list as though it
	// were this checkpoint's own result.
	if err == nil {
		t.Fatalf("CheckpointWorkload succeeded (files=%v), want a failure rather than a replay of the DATA snapshot", resp.GetSnapshotFiles())
	}
	if slices.Contains(resp.GetSnapshotFiles(), "durable-dir.tar") {
		t.Errorf("SnapshotFiles = %v, want the DATA marker's files not replayed", resp.GetSnapshotFiles())
	}
}

// The classification hangs on this predicate, and the safe direction is
// asymmetric: failing to match leaves the checkpoint error retriable, while a
// wrong match crashes the actor permanently. Only runsc's own report that the
// container is absent counts — not the many ways the probe can fail to reach
// it.
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
		// We capture runsc's whole --alsologtostderr stream, and gVisor logs
		// "does not exist" about things it merely probed on its way to an
		// unrelated failure. Only the verdict line counts: reading a log line as
		// the verdict would crash an actor whose sandbox is alive.
		{
			"phrase in an incidental log line, verdict says otherwise",
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

// A retried checkpoint must not inherit the previous attempt's images: they
// would join the manifest through listSnapshotFiles and reach a restore as
// pages from a checkpoint that never completed.
func TestCheckpointWorkloadClearsStaleCheckpointFiles(t *testing.T) {
	const actorUID = "actor-1"
	dir := useTempActorsDir(t, actorUID)

	stale := filepath.Join(dir, "pages.img")
	if err := os.WriteFile(stale, []byte("half-written"), 0o600); err != nil {
		t.Fatalf("writing stale image: %v", err)
	}

	// No marker and no runsc: the checkpoint itself fails, which is fine — the
	// dir is cleared on the way there, before anything is written.
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
