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
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// Every RPC with a request body rejects an empty one before touching any
// state: the generated validators run first in each handler.
func TestRPCsRejectInvalidRequests(t *testing.T) {
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
		"GetWorkloadStats": func() error {
			_, err := s.GetWorkloadStats(ctx, &ateompb.GetWorkloadStatsRequest{})
			return err
		},
	} {
		if got := apierror.Code(call()); got != codes.InvalidArgument {
			t.Errorf("%s() code = %v, want %v", name, got, codes.InvalidArgument)
		}
	}
}

// The requests below describe testActor and pass the generated validators, so
// a test aimed at the asset-path check that runs after them sees that check's
// error rather than an unrelated rejection. Only RunWorkload and
// RestoreWorkload read the asset map, so only those two are built.

func testDirs() *ateompb.ActorDirs {
	return &ateompb.ActorDirs{
		RootDir:                   "/node/actors/actor-a",
		OciBundleDir:              "/node/actors/actor-a/bundle",
		CheckpointDir:             "/node/actors/actor-a/checkpoint-state",
		RestoreDir:                "/node/actors/actor-a/restore-state",
		DurableDirVolumeMountsDir: "/node/actors/actor-a/durable-dirs",
		SystemInfoVolumeRootsDir:  "/node/actors/actor-a/system-info",
		VolumesDir:                "/node/actors/actor-a/volumes",
	}
}

func testSpec() *ateompb.WorkloadSpec {
	return &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{Name: "app"}}}
}

func validRunRequest() *ateompb.RunWorkloadRequest {
	return &ateompb.RunWorkloadRequest{Atespace: testActor.Ref.Atespace, ActorName: testActor.Ref.Name, ActorUid: testActor.UID, Spec: testSpec(), ActorDirs: testDirs()}
}

func validRestoreRequest() *ateompb.RestoreWorkloadRequest {
	return &ateompb.RestoreWorkloadRequest{Atespace: testActor.Ref.Atespace, ActorName: testActor.Ref.Name, ActorUid: testActor.UID, Spec: testSpec(), ActorDirs: testDirs(), Fidelity: ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY}
}
