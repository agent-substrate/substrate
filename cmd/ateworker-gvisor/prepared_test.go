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
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

func preparedTestRequest(t *testing.T) *ateworkerpb.PrepareSandboxRequest {
	t.Helper()
	old := nodepath.StaticFilesDir
	nodepath.StaticFilesDir = t.TempDir()
	t.Cleanup(func() { nodepath.StaticFilesDir = old })
	binary := filepath.Join(nodepath.StaticFilesDir, "runsc")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	return &ateworkerpb.PrepareSandboxRequest{Workload: &ateworkerpb.RunWorkloadRequest{
		ActorUid: "actor-a", RunscPath: binary, CpuMilli: 1000, MemoryBytes: 1 << 30,
		ActorDirs: &ateworkerpb.ActorDirs{
			RootDir: root, OciBundleDir: filepath.Join(root, "bundle"),
			CheckpointDir: filepath.Join(root, "checkpoint"), RestoreDir: filepath.Join(root, "restore"),
			DurableDirVolumeMountsDir: filepath.Join(root, "durable"),
			SystemInfoVolumeRootsDir:  filepath.Join(root, "system-info"), VolumesDir: filepath.Join(root, "volumes"),
		},
	}}
}

func TestPreparedSandboxRejectsMismatchedRun(t *testing.T) {
	req := preparedTestRequest(t)
	s := NewService(nil, nil, 2, nil)
	s.actors["actor-a"] = &hostedActor{prepared: &preparedSandbox{request: proto.Clone(req).(*ateworkerpb.PrepareSandboxRequest)}}
	if _, err := s.PrepareSandbox(context.Background(), req); err != nil {
		t.Fatalf("idempotent prepare: %v", err)
	}
	run := proto.Clone(req.GetWorkload()).(*ateworkerpb.RunWorkloadRequest)
	run.CpuMilli = 500
	if _, err := s.RunWorkload(context.Background(), run); apierror.Code(err) != codes.FailedPrecondition {
		t.Fatalf("mismatched RunWorkload = %v, want FailedPrecondition", err)
	}
	if s.preparedSandbox("actor-a") == nil {
		t.Fatal("mismatched consumer discarded prepared state")
	}
}

func TestPreparedSandboxRejectsIncompleteCleanup(t *testing.T) {
	req := preparedTestRequest(t)
	s := NewService(nil, nil, 1, nil)
	s.actors["actor-a"] = &hostedActor{prepared: &preparedSandbox{request: req, discarding: true}}
	if _, err := s.PrepareSandbox(context.Background(), req); apierror.Code(err) != codes.FailedPrecondition {
		t.Fatalf("PrepareSandbox = %v, want FailedPrecondition", err)
	}
}

func TestDiscardPreparedSandboxPreservesOtherActors(t *testing.T) {
	req := preparedTestRequest(t)
	s := NewService(nil, nil, 2, nil)
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.actors["actor-a"] = &hostedActor{cancel: cancel, prepared: &preparedSandbox{
		request: req, rcmd: &runsc{actorUID: "actor-a", actorDirs: req.GetWorkload().GetActorDirs()},
	}}
	other := &hostedActor{}
	s.actors["actor-b"] = other
	for range 2 {
		if _, err := s.DiscardPreparedSandbox(context.Background(), &ateworkerpb.DiscardPreparedSandboxRequest{ActorUid: "actor-a"}); err != nil {
			t.Fatal(err)
		}
	}
	if s.lookupActor("actor-a") != nil {
		t.Fatal("discarded actor still hosted")
	}
	if s.lookupActor("actor-b") != other {
		t.Fatal("discard changed another actor")
	}
}

func TestRestoreRejectsPreparedColdRun(t *testing.T) {
	req := preparedTestRequest(t)
	s := NewService(nil, nil, 2, nil)
	prepared := &preparedSandbox{request: req}
	s.actors["actor-a"] = &hostedActor{prepared: prepared}
	w := req.GetWorkload()
	_, err := s.RestoreWorkload(t.Context(), &ateworkerpb.RestoreWorkloadRequest{
		ActorUid: w.GetActorUid(), ActorDirs: w.GetActorDirs(), RunscPath: w.GetRunscPath(),
		CpuMilli: w.GetCpuMilli(), MemoryBytes: w.GetMemoryBytes(),
		Fidelity: ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
	})
	if apierror.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RestoreWorkload = %v, want FailedPrecondition", err)
	}
	if s.preparedSandbox("actor-a") != prepared {
		t.Fatal("restore consumed cold-run preparation")
	}
}
