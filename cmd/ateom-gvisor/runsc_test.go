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
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"

	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateompb"
)

var testActorDirs = &ateompb.ActorDirs{
	RootDir:      "/node/actors/test-actor-123",
	OciBundleDir: "/node/actors/test-actor-123/bundle",
}

func TestKillArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
	}

	got := r.killArgs("my-container", "SIGTERM")
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"kill",
		"my-container",
		"SIGTERM",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("killArgs() = %v, want %v", got, want)
	}
}

func TestWaitArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
	}

	got := r.waitArgs("my-container")
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"wait",
		"my-container",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("waitArgs() = %v, want %v", got, want)
	}
}

func TestPauseArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
	}

	got := r.pauseArgs(ocispec.PauseContainer)
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"pause",
		ocispec.PauseContainer,
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("pauseArgs() = %v, want %v", got, want)
	}
}

func TestResumeArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
	}

	got := r.resumeArgs(ocispec.PauseContainer)
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"resume",
		ocispec.PauseContainer,
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("resumeArgs() = %v, want %v", got, want)
	}
}

func TestRestoreArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
	}
	checkpointDir := "/node/actors/test-actor-123/checkpoints/snap-1"

	got := r.restoreArgs(ocispec.PauseContainer, checkpointDir)
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"--cpu-num-from-quota",
		"restore",
		"-bundle", "/node/actors/test-actor-123/bundle/" + ocispec.PauseContainer,
		"-image-path", checkpointDir,
		"-pid-file", "/node/actors/test-actor-123/pidfiles/" + ocispec.PauseContainer + ".pid",
		"-background",
		"-detach",
		ocispec.PauseContainer,
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("restoreArgs() = %v, want %v", got, want)
	}
}

func TestRestoreWorkloadRejectsEmptyRestoreDir(t *testing.T) {
	s := &AteomService{
		locks:    actorlock.New(),
		inFlight: actorlock.NewInFlight(),
	}

	for _, tc := range []struct {
		name string
		req  *ateompb.RestoreWorkloadRequest
	}{
		{
			name: "nil actor_dirs",
			req:  &ateompb.RestoreWorkloadRequest{ActorUid: "actor-a"},
		},
		{
			name: "empty restore_dir",
			req: &ateompb.RestoreWorkloadRequest{
				ActorUid: "actor-a",
				ActorDirs: &ateompb.ActorDirs{
					RootDir:                   "/node/actors/actor-a",
					OciBundleDir:              "/node/actors/actor-a/bundle",
					CheckpointDir:             "/node/actors/actor-a/checkpoint-state",
					DurableDirVolumeMountsDir: "/node/actors/actor-a/durable-dirs",
					SystemInfoVolumeRootsDir:  "/node/actors/actor-a/system-info",
					VolumesDir:                "/node/actors/actor-a/volumes",
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.RestoreWorkload(context.Background(), tc.req)
			if apierror.Code(err) != codes.InvalidArgument {
				t.Fatalf("RestoreWorkload() code = %v, want %v (err: %v)", apierror.Code(err), codes.InvalidArgument, err)
			}
		})
	}
}
