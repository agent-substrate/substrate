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
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

func testPrepareRequest(t *testing.T) *ateworkerpb.PrepareSandboxRequest {
	t.Helper()
	root := t.TempDir()
	return &ateworkerpb.PrepareSandboxRequest{Workload: &ateworkerpb.RunWorkloadRequest{
		ActorUid: "actor-a", CpuMilli: 1000, MemoryBytes: 1 << 30,
		ActorDirs: &ateworkerpb.ActorDirs{
			RootDir: root, OciBundleDir: filepath.Join(root, "bundle"), CheckpointDir: filepath.Join(root, "checkpoint"),
			RestoreDir: filepath.Join(root, "restore"), DurableDirVolumeMountsDir: filepath.Join(root, "durable"),
			SystemInfoVolumeRootsDir: filepath.Join(root, "system-info"), VolumesDir: filepath.Join(root, "volumes"),
		},
	}}
}

func TestRunWorkloadMismatchDoesNotConsumePreparedSandbox(t *testing.T) {
	req := testPrepareRequest(t)
	prepared := &preparedSandbox{request: req, actor: &runningActor{}}
	s := NewService("pod", "", false, 128, 2, nil, nil)
	s.actors["actor-a"] = &hostedActor{prepared: prepared}
	run := proto.Clone(req.GetWorkload()).(*ateworkerpb.RunWorkloadRequest)
	run.CpuMilli++
	if _, err := s.RunWorkload(t.Context(), run); apierror.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RunWorkload = %v, want FailedPrecondition", err)
	}
	if s.preparedSandbox("actor-a") != prepared {
		t.Fatal("mismatched consumer changed the prepared VM")
	}
}

func TestPrepareSandboxIdempotencyStopsWhenCleanupStarts(t *testing.T) {
	req := testPrepareRequest(t)
	prepared := &preparedSandbox{request: req, actor: &runningActor{}}
	s := NewService("pod", "", false, 128, 2, nil, nil)
	s.actors["actor-a"] = &hostedActor{prepared: prepared}
	if _, err := s.PrepareSandbox(t.Context(), req); err != nil {
		t.Fatalf("idempotent PrepareSandbox: %v", err)
	}
	prepared.discarding = true
	if _, err := s.PrepareSandbox(t.Context(), req); apierror.Code(err) != codes.FailedPrecondition {
		t.Fatalf("PrepareSandbox during cleanup = %v, want FailedPrecondition", err)
	}
}
