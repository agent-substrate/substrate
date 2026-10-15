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
	"github.com/agent-substrate/substrate/internal/actorlock"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimeGoldenMarker(t *testing.T) {
	for _, tc := range []struct {
		name, content      string
		present, want, bad bool
	}{
		{name: "ordinary snapshot"},
		{name: "pause only", content: runtimeGoldenFormat, present: true, want: true},
		{name: "wrong runtime", content: "microvm-before-create-sandbox-v1\n", present: true, bad: true},
		{name: "empty", present: true, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.present {
				if err := os.WriteFile(filepath.Join(dir, runtimeGoldenFile), []byte(tc.content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			got, err := readRuntimeGolden(dir)
			if got != tc.want || (err != nil) != tc.bad {
				t.Fatalf("got %v, %v", got, err)
			}
		})
	}
}

func TestRuntimeGoldenApplicationRestore(t *testing.T) {
	memory := ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	volumes := ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	for _, tc := range []struct {
		name     string
		capture  bool
		fidelity ateworkerpb.SnapshotFidelity
		want     bool
		bad      bool
	}{
		{"own memory restores apps", false, memory, true, false},
		{"own volumes starts apps", false, volumes, false, false},
		{"runtime memory starts apps", true, memory, false, false},
		{"runtime volumes rejected", true, volumes, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := restoreApplicationMemory(tc.fidelity, tc.capture)
			if got != tc.want || (err != nil) != tc.bad {
				t.Fatalf("got %v, %v", got, err)
			}
		})
	}
}

func TestRuntimeGoldenRejectsNormalActorBeforeMutation(t *testing.T) {
	s := &AteWorkerService{}
	_, err := s.RunWorkload(context.Background(), &ateworkerpb.RunWorkloadRequest{Spec: &ateworkerpb.WorkloadSpec{RuntimeGolden: true}})
	if err == nil {
		t.Fatal("normal actor allowed to capture runtime-only snapshot")
	}
}

func TestRuntimeGoldenCheckpointRejectsVolumesBeforeMutation(t *testing.T) {
	originalStaticDir := nodepath.StaticFilesDir
	nodepath.StaticFilesDir = t.TempDir()
	t.Cleanup(func() { nodepath.StaticFilesDir = originalStaticDir })
	if err := os.WriteFile(filepath.Join(nodepath.StaticFilesDir, "runsc"), nil, 0700); err != nil {
		t.Fatal(err)
	}
	s := &AteWorkerService{locks: actorlock.New(), actors: map[string]*hostedActor{}}
	dirs := &ateworkerpb.ActorDirs{
		RootDir: "/node/actors/a", OciBundleDir: "/node/actors/a/bundle",
		CheckpointDir: "/node/actors/a/checkpoint-state", RestoreDir: "/node/actors/a/restore-state",
		DurableDirVolumeMountsDir: "/node/actors/a/durable-dirs", SystemInfoVolumeRootsDir: "/node/actors/a/system-info",
		VolumesDir: "/node/actors/a/volumes",
	}
	// All mutation dependencies are nil; touching networking or runtime would panic.
	_, err := s.CheckpointWorkload(context.Background(), &ateworkerpb.CheckpointWorkloadRequest{
		ActorUid: "a", ActorDirs: dirs, RunscPath: filepath.Join(nodepath.StaticFilesDir, "runsc"),
		Fidelity: ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
		Spec:     &ateworkerpb.WorkloadSpec{RuntimeGolden: true},
	})
	if err == nil || !strings.Contains(err.Error(), "requires MEMORY") {
		t.Fatalf("got %v", err)
	}
}
