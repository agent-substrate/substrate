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
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"

	"github.com/agent-substrate/substrate/internal/actorlock"
	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateomtunnel"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/roottest"
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

func TestRPCsRejectUntrustedRunscPath(t *testing.T) {
	s := &AteomService{}
	ctx := context.Background()
	dirs := &ateompb.ActorDirs{
		RootDir:                   "/node/actors/actor-a",
		OciBundleDir:              "/node/actors/actor-a/bundle",
		CheckpointDir:             "/node/actors/actor-a/checkpoint-state",
		RestoreDir:                "/node/actors/actor-a/restore",
		DurableDirVolumeMountsDir: "/node/actors/actor-a/durable-dirs",
		SystemInfoVolumeRootsDir:  "/node/actors/actor-a/system-info",
		VolumesDir:                "/node/actors/actor-a/volumes",
	}
	for name, call := range map[string]func() error{
		"RunWorkload": func() error {
			_, err := s.RunWorkload(ctx, &ateompb.RunWorkloadRequest{ActorDirs: dirs, RunscPath: "/bin/sh"})
			return err
		},
		"RestoreWorkload": func() error {
			_, err := s.RestoreWorkload(ctx, &ateompb.RestoreWorkloadRequest{ActorDirs: dirs, RunscPath: "/bin/sh"})
			return err
		},
		"CheckpointWorkload": func() error {
			_, err := s.CheckpointWorkload(ctx, &ateompb.CheckpointWorkloadRequest{ActorDirs: dirs, RunscPath: "/bin/sh"})
			return err
		},
		"TerminateWorkload": func() error {
			_, err := s.TerminateWorkload(ctx, &ateompb.TerminateWorkloadRequest{ActorDirs: dirs, RunscPath: "/bin/sh"})
			return err
		},
	} {
		if got := apierror.Code(call()); got != codes.InvalidArgument {
			t.Errorf("%s() code = %v, want %v", name, got, codes.InvalidArgument)
		}
	}
}

// newTestService returns an AteomService that hosts no actors, with a tunnel
// no actor is active on.
func newTestService(t *testing.T) *AteomService {
	t.Helper()
	egress, err := atunnel.NewEgress(atunnel.TCPOriginalDestination)
	if err != nil {
		t.Fatal(err)
	}
	return &AteomService{
		locks:       actorlock.New(),
		actors:      map[string]*hostedActor{},
		tunnel:      &ateomtunnel.Tunnel{Ingress: &atunnel.Server{}, Egress: egress},
		actorLogger: actorlog.NewActorLogger(io.Discard, false),
	}
}

// newTerminateRequest returns a TerminateWorkload request for an actor
// rooted in a temp dir. Its runsc is a script that creates the returned path.
func newTerminateRequest(t *testing.T) (req *ateompb.TerminateWorkloadRequest, runscInvoked string) {
	t.Helper()
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
	runscInvoked = filepath.Join(tmp, "runsc-invoked")
	origStatic := nodepath.StaticFilesDir
	nodepath.StaticFilesDir = filepath.Join(tmp, "static-files")
	t.Cleanup(func() { nodepath.StaticFilesDir = origStatic })
	if err := os.MkdirAll(nodepath.StaticFilesDir, 0o700); err != nil {
		t.Fatal(err)
	}
	runscPath := filepath.Join(nodepath.StaticFilesDir, "runsc")
	if err := os.WriteFile(runscPath, []byte("#!/bin/sh\ntouch "+runscInvoked+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return &ateompb.TerminateWorkloadRequest{
		Atespace:  "default",
		ActorName: "actor",
		ActorUid:  "actor-uid",
		RunscPath: runscPath,
		ActorDirs: actorDirs,
		Spec:      &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: "app"}}},
	}, runscInvoked
}

// An actor this ateom does not host, as after an ateom restart, terminates
// without runsc: the PIDs in the runsc state it left are not its sandbox's.
// Repeating the call succeeds too.
func TestTerminateWorkloadOfUnhostedActorSkipsRunsc(t *testing.T) {
	req, invoked := newTerminateRequest(t)
	actorDirs := req.GetActorDirs()
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

	s := newTestService(t)
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

// An earlier TerminateWorkload can unhost an actor after failing to unmount
// its bundle rootfs overlays. Retrying it unmounts them, though it skips runsc.
func TestTerminateWorkloadOfUnhostedActorUnmountsBundles(t *testing.T) {
	roottest.Require(t, "mount/unmount")
	req, _ := newTerminateRequest(t)
	target := filepath.Join(req.GetActorDirs().GetOciBundleDir(), "app", "rootfs")
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "marker"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount(src, target, "", unix.MS_BIND, ""); err != nil {
		t.Fatalf("bind mount: %v", err)
	}
	t.Cleanup(func() { _ = unix.Unmount(target, unix.MNT_DETACH) })

	if _, err := newTestService(t).TerminateWorkload(context.Background(), req); err != nil {
		t.Fatalf("TerminateWorkload() error = %v, want nil", err)
	}

	if _, err := os.Stat(filepath.Join(target, "marker")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("%q is still mounted (stat err = %v), want it unmounted", target, err)
	}
}
