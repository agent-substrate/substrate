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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
)

func TestRuntimeGoldenMarker(t *testing.T) {
	for _, tc := range []struct {
		name, content     string
		exists, want, bad bool
	}{
		{name: "ordinary"}, {name: "runtime", content: runtimeGoldenFormat, exists: true, want: true}, {name: "empty", exists: true, bad: true}, {name: "unknown", content: "different", exists: true, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := t.TempDir()
			if tc.exists {
				if err := os.WriteFile(filepath.Join(d, runtimeGoldenFile), []byte(tc.content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			got, err := readRuntimeGolden(d)
			if got != tc.want || (err != nil) != tc.bad {
				t.Fatalf("got %v,%v", got, err)
			}
		})
	}
}

func TestRuntimeGoldenRejectsOrdinaryActorBeforeActivation(t *testing.T) {
	s := &AteWorkerService{}
	_, err := s.RunWorkload(context.Background(), &ateworkerpb.RunWorkloadRequest{Atespace: "user", Spec: &ateworkerpb.WorkloadSpec{RuntimeGolden: true}})
	if err == nil || !strings.Contains(err.Error(), "requires a golden actor") {
		t.Fatalf("got %v", err)
	}
}

func TestRuntimeGoldenCheckpointRejectsBeforeMutation(t *testing.T) {
	for _, fidelity := range []ateworkerpb.SnapshotFidelity{ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS} {
		t.Run(fidelity.String(), func(t *testing.T) {
			root := t.TempDir()
			checkpoint := filepath.Join(root, "checkpoint")
			if err := os.Mkdir(checkpoint, 0700); err != nil {
				t.Fatal(err)
			}
			sentinel := filepath.Join(checkpoint, "existing")
			if err := os.WriteFile(sentinel, []byte("keep"), 0600); err != nil {
				t.Fatal(err)
			}
			ra := &runningActor{phase: guestBooted}
			// No tunnel or CH client: crossing the mutation boundary would panic or
			// fail to reach CH instead of returning the expected precondition error.
			s := &AteWorkerService{locks: actorlock.New(), inFlight: actorlock.NewInFlight(), actors: map[string]*hostedActor{"a": {vm: ra}}}
			dirs := &ateworkerpb.ActorDirs{RootDir: root, OciBundleDir: root, CheckpointDir: checkpoint, RestoreDir: root, DurableDirVolumeMountsDir: root, SystemInfoVolumeRootsDir: root, VolumesDir: root}
			_, err := s.CheckpointWorkload(context.Background(), &ateworkerpb.CheckpointWorkloadRequest{ActorUid: "a", ActorDirs: dirs, Fidelity: fidelity})
			if err == nil || !strings.Contains(err.Error(), "requires MEMORY") {
				t.Fatalf("got %v", err)
			}
			data, err := os.ReadFile(sentinel)
			if err != nil || string(data) != "keep" {
				t.Fatalf("checkpoint changed: %s %v", data, err)
			}
			if s.runningVM("a") != ra || ra.phase != guestBooted {
				t.Fatal("guest state changed")
			}
		})
	}
}
