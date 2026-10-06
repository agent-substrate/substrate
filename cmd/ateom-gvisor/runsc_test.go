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

	"github.com/agent-substrate/substrate/internal/actorlock"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

var testActorDirs = &ateompb.ActorDirs{
	RootDir:      "/node/actors/test-actor-123",
	OciBundleDir: "/node/actors/test-actor-123/bundle",
}

func mustFlagStore(t *testing.T) *flagStore {
	t.Helper()
	store, err := newFlagStore(t.Context(), "kodata/flags.json")
	if err != nil {
		t.Fatalf("newFlagStore(kodata/flags.json) failed: %v", err)
	}
	return store
}

func TestBuildArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
		flags:     mustFlagStore(t),
	}

	for _, tc := range []struct {
		name          string
		command       string
		containerName string
		extraArgs     []string
		want          []string
	}{
		{
			name:          "kill",
			command:       "kill",
			containerName: "",
			extraArgs:     []string{"my-container", "SIGTERM"},
			want: []string{
				"-log-format", "json",
				"--alsologtostderr",
				"-root", "/node/actors/test-actor-123/runsc-state",
				"kill",
				"my-container",
				"SIGTERM",
			},
		},
		{
			name:          "wait",
			command:       "wait",
			containerName: "my-container",
			want: []string{
				"-log-format", "json",
				"--alsologtostderr",
				"-root", "/node/actors/test-actor-123/runsc-state",
				"wait",
				"my-container",
			},
		},
		{
			name:          "pause",
			command:       "pause",
			containerName: ocispec.PauseContainer,
			want: []string{
				"-log-format", "json",
				"--alsologtostderr",
				"-root", "/node/actors/test-actor-123/runsc-state",
				"pause",
				ocispec.PauseContainer,
			},
		},
		{
			name:          "resume",
			command:       "resume",
			containerName: ocispec.PauseContainer,
			want: []string{
				"-log-format", "json",
				"--alsologtostderr",
				"-root", "/node/actors/test-actor-123/runsc-state",
				"resume",
				ocispec.PauseContainer,
			},
		},
		{
			name:          "list",
			command:       "list",
			containerName: "",
			want: []string{
				"-log-format", "json",
				"--alsologtostderr",
				"-root", "/node/actors/test-actor-123/runsc-state",
				"list",
				"-quiet",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := r.buildArgs(tc.command, tc.containerName, tc.extraArgs...)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("buildArgs(%q, %q, %v) = %v, want %v", tc.command, tc.containerName, tc.extraArgs, got, tc.want)
			}
		})
	}
}

func TestRestoreArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
		flags:     mustFlagStore(t),
	}
	checkpointDir := "/node/actors/test-actor-123/checkpoints/snap-1"

	got := r.restoreArgs(ocispec.PauseContainer, checkpointDir)
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"--cpu-num-from-quota",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"restore",
		"-background",
		"-detach",
		"-bundle", "/node/actors/test-actor-123/bundle/" + ocispec.PauseContainer,
		"-image-path", checkpointDir,
		"-pid-file", "/node/actors/test-actor-123/pidfiles/" + ocispec.PauseContainer + ".pid",
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
