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

package sandboxprep

import (
	"testing"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"google.golang.org/protobuf/proto"
)

func TestMatchesStartupShape(t *testing.T) {
	prepared := &ateworkerpb.PrepareSandboxRequest{Workload: &ateworkerpb.RunWorkloadRequest{
		ActorUid: "actor", CpuMilli: 1000, MemoryBytes: 1 << 30,
		RuntimeAssetPaths: map[string]string{"kernel": "/kernel"},
		ActorDirs:         &ateworkerpb.ActorDirs{RootDir: "/actor"},
		Spec:              &ateworkerpb.WorkloadSpec{Containers: []*ateworkerpb.Container{{Name: "app"}}},
	}}
	for _, tc := range []struct {
		name   string
		change func(*ateworkerpb.PrepareSandboxRequest)
		want   bool
	}{
		{"resolved image", func(r *ateworkerpb.PrepareSandboxRequest) {
			r.Workload.Spec.Containers[0].ContainerSpec = &ateworkerpb.ContainerSpec{Args: []string{"python"}, Env: []string{"X=1"}}
		}, true},
		{"actor", func(r *ateworkerpb.PrepareSandboxRequest) { r.Workload.ActorUid = "other" }, false},
		{"cpu", func(r *ateworkerpb.PrepareSandboxRequest) { r.Workload.CpuMilli = 2000 }, false},
		{"memory", func(r *ateworkerpb.PrepareSandboxRequest) { r.Workload.MemoryBytes++ }, false},
		{"assets", func(r *ateworkerpb.PrepareSandboxRequest) { r.Workload.RuntimeAssetPaths["kernel"] = "/other" }, false},
		{"directory", func(r *ateworkerpb.PrepareSandboxRequest) { r.Workload.ActorDirs.RootDir = "/other" }, false},
		{"origin", func(r *ateworkerpb.PrepareSandboxRequest) { r.FromCheckpoint = true }, false},
		{"volume", func(r *ateworkerpb.PrepareSandboxRequest) {
			r.Workload.Spec.Containers[0].DurableDirVolumeMounts = []*ateworkerpb.DurableDirVolumeMount{{VolumeName: "data"}}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := proto.Clone(prepared).(*ateworkerpb.PrepareSandboxRequest)
			tc.change(req)
			before := proto.Clone(req)
			if got := Matches(prepared, req); got != tc.want {
				t.Fatalf("Matches = %v, want %v", got, tc.want)
			}
			if !proto.Equal(req, before) {
				t.Fatal("matching mutated the consumer request")
			}
		})
	}
}
