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
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/sizing"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testSandboxParams() sandboxBootParams {
	return sandboxBootParams{
		actorUID:       "actor-1",
		assetPaths:     map[string]string{assetKernel: "/kernel", assetImage: "/image"},
		redirectEgress: true,
		size:           sizing.FromLimits(1500, 512*1024*1024),
	}
}

func TestSandboxBootParamsMatchesVMShape(t *testing.T) {
	want := testSandboxParams()
	tests := []struct {
		name  string
		other sandboxBootParams
		match bool
	}{
		{name: "identical", other: testSandboxParams(), match: true},
		{name: "actor", other: sandboxBootParams{actorUID: "actor-2", assetPaths: want.assetPaths, redirectEgress: true, size: want.size}},
		{name: "assets", other: sandboxBootParams{actorUID: want.actorUID, assetPaths: map[string]string{assetKernel: "/other", assetImage: "/image"}, redirectEgress: true, size: want.size}},
		{name: "egress", other: sandboxBootParams{actorUID: want.actorUID, assetPaths: want.assetPaths, size: want.size}},
		{name: "limits", other: sandboxBootParams{actorUID: want.actorUID, assetPaths: want.assetPaths, redirectEgress: true, size: sizing.FromLimits(2000, 512*1024*1024)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := want.matches(tt.other); got != tt.match {
				t.Fatalf("matches() = %v, want %v", got, tt.match)
			}
		})
	}
}

func TestRunWorkloadMismatchDoesNotConsumePreparedSandbox(t *testing.T) {
	prepared := &preparedSandbox{params: testSandboxParams(), actor: &runningActor{}}
	s := &AteomService{lock: newCancelableMutex(), prepared: prepared}

	_, err := s.RunWorkload(t.Context(), &ateompb.RunWorkloadRequest{
		ActorUid:          "different-actor",
		RuntimeAssetPaths: testSandboxParams().assetPaths,
		EgressGateway:     &ateompb.EgressGateway{},
		CpuMilli:          1500,
		MemoryBytes:       512 * 1024 * 1024,
		Spec: &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{
			Name: "app",
			DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{{
				VolumeName: "data",
			}},
		}}},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RunWorkload() error = %v, want FailedPrecondition", err)
	}
	if s.prepared != prepared {
		t.Fatal("mismatched RunWorkload consumed the prepared sandbox")
	}
}

func TestPrepareSandboxIdempotencyStopsWhenCleanupStarts(t *testing.T) {
	params := testSandboxParams()
	prepared := &preparedSandbox{params: params, actor: &runningActor{}}
	s := &AteomService{lock: newCancelableMutex(), prepared: prepared}
	req := &ateompb.PrepareSandboxRequest{
		ActorUid:          params.actorUID,
		RuntimeAssetPaths: params.assetPaths,
		RedirectEgress:    true,
		CpuMilli:          1500,
		MemoryBytes:       512 * 1024 * 1024,
		Spec: &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{
			Name: "app",
			DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{{
				VolumeName: "data",
			}},
		}}},
	}

	if _, err := s.PrepareSandbox(t.Context(), req); err != nil {
		t.Fatalf("idempotent PrepareSandbox() returned %v", err)
	}
	prepared.cleanupStarted = true
	if _, err := s.PrepareSandbox(t.Context(), req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("PrepareSandbox() during cleanup error = %v, want FailedPrecondition", err)
	}
}
