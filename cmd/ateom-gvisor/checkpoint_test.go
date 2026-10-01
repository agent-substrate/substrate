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
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/actorlock"
	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/ateomtunnel"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// Exercise the RPC with a runsc executable: a successful state command can
// report a stopped container while the sandbox's pause container stays alive.
func TestCheckpointWorkloadRejectsExitedApplication(t *testing.T) {
	for _, code := range []string{"0", "1"} {
		t.Run("exit "+code, func(t *testing.T) {
			s, req, dir := checkpointFixture(t)
			writeCheckpointFixture(t, dir, "sidecar.state", `{"id":"sidecar","status":"stopped"}`)
			writeCheckpointFixture(t, dir, "sidecar.wait", `{"id":"sidecar","exitStatus":`+code+`}`)
			resp, err := s.CheckpointWorkload(t.Context(), req)
			if err == nil || !strings.Contains(err.Error(), `container "sidecar"`) || !strings.Contains(err.Error(), "exit code "+code) {
				t.Fatalf("CheckpointWorkload = %v, %v; want the exited container and its code", resp, err)
			}
			assertCheckpointCommands(t, dir, false, true)
		})
	}
}

func TestCheckpointWorkloadChecksAfterFreeze(t *testing.T) {
	s, req, dir := checkpointFixture(t)
	// The application exits as the sandbox is being frozen.
	writeCheckpointFixture(t, dir, "sidecar.after-pause", `{"id":"sidecar","status":"stopped"}`)
	writeCheckpointFixture(t, dir, "sidecar.wait", `{"id":"sidecar","exitStatus":1}`)
	if _, err := s.CheckpointWorkload(t.Context(), req); err == nil {
		t.Fatal("checkpoint accepted an application that exited before the freeze completed")
	}
	assertCheckpointCommands(t, dir, false, true)
}

func TestCheckpointWorkloadFailsClosed(t *testing.T) {
	for _, tc := range []struct{ name, file, value, want string }{
		{"invalid state", "app.state", "not json", "app"},
		{"wrong container", "app.state", `{"id":"other","status":"running"}`, "app"},
		{"missing status", "app.state", `{"id":"app"}`, "app"},
		{"not started", "app.state", `{"id":"app","status":"created"}`, "created"},
		{"state failure", "state.error", "state unavailable", "state unavailable"},
		{"checkpoint failure", "checkpoint.error", "save failed", "while checkpointing pause"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, req, dir := checkpointFixture(t)
			writeCheckpointFixture(t, dir, tc.file, tc.value)
			if _, err := s.CheckpointWorkload(t.Context(), req); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CheckpointWorkload error = %v, want %q", err, tc.want)
			}
			assertCheckpointCommands(t, dir, tc.file == "checkpoint.error", true)
		})
	}
}

func TestCheckpointWorkloadResumesAfterCanceledInspection(t *testing.T) {
	s, req, dir := checkpointFixture(t)
	writeCheckpointFixture(t, dir, "state.block", "")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		for {
			if _, err := os.Stat(filepath.Join(dir, "inspecting")); err == nil {
				cancel()
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	if _, err := s.CheckpointWorkload(ctx, req); err == nil {
		t.Fatal("checkpoint succeeded after cancellation")
	}
	assertCheckpointCommands(t, dir, false, true)
}

func TestCheckpointWorkloadLiveApplications(t *testing.T) {
	s, req, dir := checkpointFixture(t)
	resp, err := s.CheckpointWorkload(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetSnapshotFiles()) != 1 || resp.GetSnapshotFiles()[0] != "checkpoint.img" {
		t.Fatalf("snapshot files = %v, want checkpoint.img", resp.GetSnapshotFiles())
	}
	assertCheckpointCommands(t, dir, true, true)
}

func TestCheckpointWorkloadUnknownExitStatus(t *testing.T) {
	for _, result := range []string{"not json", `{"id":"app"}`, `{"id":"other","exitStatus":0}`} {
		t.Run(result, func(t *testing.T) {
			s, req, dir := checkpointFixture(t)
			writeCheckpointFixture(t, dir, "app.state", `{"id":"app","status":"stopped"}`)
			writeCheckpointFixture(t, dir, "app.wait", result)
			if _, err := s.CheckpointWorkload(t.Context(), req); err == nil || !strings.Contains(err.Error(), "exit code unknown") {
				t.Fatalf("CheckpointWorkload error = %v, want unknown exit code", err)
			}
			assertCheckpointCommands(t, dir, false, true)
		})
	}
}

func TestCheckpointWorkloadResumeFailure(t *testing.T) {
	s, req, dir := checkpointFixture(t)
	writeCheckpointFixture(t, dir, "app.state", `{"id":"app","status":"stopped"}`)
	writeCheckpointFixture(t, dir, "resume.error", "resume failed")
	if _, err := s.CheckpointWorkload(t.Context(), req); err == nil || !strings.Contains(err.Error(), "exited before checkpoint") || !strings.Contains(err.Error(), "while resuming sandbox") {
		t.Fatalf("CheckpointWorkload error = %v, want both inspection and resume errors", err)
	}
	assertCheckpointCommands(t, dir, false, true)
}

func TestCheckpointWorkloadDataDoesNotRequireLiveApplications(t *testing.T) {
	s, req, dir := checkpointFixture(t)
	req.Scope = ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA
	req.Spec.Containers[0].DurableDirVolumeMounts = []*ateompb.DurableDirVolumeMount{{VolumeName: "data"}}
	if err := os.MkdirAll(req.ActorDirs.DurableDirVolumeMountsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeCheckpointFixture(t, dir, "app.state", `{"id":"app","status":"stopped"}`)
	resp, err := s.CheckpointWorkload(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetSnapshotFiles()) != 1 || resp.GetSnapshotFiles()[0] != durableTarFile {
		t.Fatalf("snapshot files = %v, want durable data", resp.GetSnapshotFiles())
	}
	assertCheckpointCommands(t, dir, false, true)
}

func checkpointFixture(t *testing.T) (*AteomService, *ateompb.CheckpointWorkloadRequest, string) {
	t.Helper()
	dir := t.TempDir()
	writeCheckpointFixture(t, dir, "runsc", `#!/bin/sh
set -eu
dir=$(dirname "$0")
while [ "$1" != "-root" ]; do shift; done
shift 2
printf '%s\n' "$*" >> "$dir/calls"
if [ -f "$dir/$1.error" ]; then cat "$dir/$1.error" >&2; exit 1; fi
case "$1" in
pause)
  for name in app sidecar; do
    if [ -f "$dir/$name.after-pause" ]; then cp "$dir/$name.after-pause" "$dir/$name.state"; fi
  done ;;
state)
  if [ -f "$dir/state.block" ]; then touch "$dir/inspecting"; exec sleep 30; fi
  cat "$dir/$2.state" ;;
wait) cat "$dir/$2.wait" ;;
checkpoint) mkdir -p "$3"; touch "$3/checkpoint.img" ;;
esac
`)
	if err := os.Chmod(filepath.Join(dir, "runsc"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"app", "sidecar", "_pause"} {
		writeCheckpointFixture(t, dir, name+".state", `{"id":"`+name+`","status":"running"}`)
		writeCheckpointFixture(t, dir, name+".wait", `{"id":"`+name+`","exitStatus":0}`)
	}
	dirs := &ateompb.ActorDirs{
		RootDir: dir, OciBundleDir: filepath.Join(dir, "bundles"),
		CheckpointDir: filepath.Join(dir, "checkpoint"), RestoreDir: filepath.Join(dir, "restore"),
		DurableDirVolumeMountsDir: filepath.Join(dir, "durable"),
		SystemInfoVolumeRootsDir:  filepath.Join(dir, "system-info"), VolumesDir: filepath.Join(dir, "volumes"),
	}
	s := &AteomService{
		locks: actorlock.New(), inFlight: actorlock.NewInFlight(),
		actorLogger: actorlog.NewActorLogger(io.Discard, false),
		tunnel:      &ateomtunnel.Tunnel{Ingress: &atunnel.Server{}, Egress: &atunnel.Egress{}},
	}
	req := &ateompb.CheckpointWorkloadRequest{
		ActorUid: "test-uid", ActorDirs: dirs, RunscPath: filepath.Join(dir, "runsc"),
		Scope: ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		Spec:  &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: "app"}, {Name: "sidecar"}}},
	}
	return s, req, dir
}

func writeCheckpointFixture(t *testing.T, dir, name, value string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertCheckpointCommands(t *testing.T, dir string, checkpoint, resume bool) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "calls"))
	if err != nil {
		t.Fatal(err)
	}
	calls := string(b)
	if got := strings.Contains(calls, "checkpoint -image-path"); got != checkpoint {
		t.Errorf("checkpoint called = %v, want %v; commands:\n%s", got, checkpoint, calls)
	}
	if got := strings.Contains(calls, "resume _pause"); got != resume {
		t.Errorf("resume called = %v, want %v; commands:\n%s", got, resume, calls)
	}
}
