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
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/agent-substrate/substrate/internal/actorlock"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// Every RPC rejects a request without ActorDirs before touching any state.
func TestRPCsRejectMissingActorDirs(t *testing.T) {
	s := &AteomService{}
	ctx := context.Background()
	for name, call := range map[string]func() error{
		"RunWorkload": func() error {
			_, err := s.RunWorkload(ctx, &ateompb.RunWorkloadRequest{})
			return err
		},
		"RestoreWorkload": func() error {
			_, err := s.RestoreWorkload(ctx, &ateompb.RestoreWorkloadRequest{})
			return err
		},
		"CheckpointWorkload": func() error {
			_, err := s.CheckpointWorkload(ctx, &ateompb.CheckpointWorkloadRequest{})
			return err
		},
		"TerminateWorkload": func() error {
			_, err := s.TerminateWorkload(ctx, &ateompb.TerminateWorkloadRequest{})
			return err
		},
	} {
		if got := apierror.Code(call()); got != codes.InvalidArgument {
			t.Errorf("%s() code = %v, want %v", name, got, codes.InvalidArgument)
		}
	}
}

// An actor this ateom does not host, as after an ateom restart, terminates
// without runsc: the PIDs in the runsc state it left are not its sandbox's.
// Repeating the call succeeds too.
func TestTerminateWorkloadOfUnhostedActorSkipsRunsc(t *testing.T) {
	tmp := t.TempDir()
	root := filepath.Join(tmp, "actor")
	actorDirs := &ateompb.ActorDirs{
		RootDir:                   root,
		OciBundleDir:              filepath.Join(root, "bundles"),
		CheckpointDir:             filepath.Join(root, "checkpoint"),
		RestoreDir:                filepath.Join(root, "restore"),
		DurableDirVolumeMountsDir: filepath.Join(root, "durable"),
		SystemInfoVolumeRootsDir:  filepath.Join(root, "systeminfo"),
		VolumesDir:                filepath.Join(root, "volumes"),
	}
	// What a dead ateom leaves behind for the actor.
	for _, f := range []string{
		filepath.Join(runscStateDir(actorDirs), "pause.state"),
		filepath.Join(pidFileDir(actorDirs), "pause.pid"),
	} {
		if err := os.MkdirAll(filepath.Dir(f), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(f, []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	invoked := filepath.Join(tmp, "runsc-invoked")
	runscPath := filepath.Join(tmp, "runsc")
	if err := os.WriteFile(runscPath, []byte("#!/bin/sh\ntouch "+invoked+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}

	s := &AteomService{locks: actorlock.New(), actors: map[string]*hostedActor{}}
	req := &ateompb.TerminateWorkloadRequest{
		Atespace:  "default",
		ActorName: "actor",
		ActorUid:  "actor-uid",
		RunscPath: runscPath,
		ActorDirs: actorDirs,
		Spec:      &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: "app"}}},
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := s.TerminateWorkload(context.Background(), req); err != nil {
			t.Fatalf("TerminateWorkload() attempt %d error = %v, want nil", attempt, err)
		}
	}

	if _, err := os.Stat(invoked); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("runsc was invoked (stat err = %v), want no runsc calls", err)
	}
	for _, dir := range []string{runscStateDir(actorDirs), pidFileDir(actorDirs)} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Errorf("ReadDir(%q) error = %v, want an empty dir", dir, err)
			continue
		}
		if len(entries) != 0 {
			t.Errorf("%q holds %d entries, want none", dir, len(entries))
		}
	}
}
