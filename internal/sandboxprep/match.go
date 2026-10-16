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

// Package sandboxprep matches image-independent sandbox startup parameters.
package sandboxprep

import (
	"google.golang.org/protobuf/proto"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

// Matches permits image-derived process configuration to arrive with the consumer
// while requiring its identity, resources, volumes, and runtime assets to match.
func Matches(prepared, requested *ateworkerpb.PrepareSandboxRequest) bool {
	return proto.Equal(shape(prepared), shape(requested))
}

func shape(req *ateworkerpb.PrepareSandboxRequest) *ateworkerpb.PrepareSandboxRequest {
	copy := proto.Clone(req).(*ateworkerpb.PrepareSandboxRequest)
	for _, c := range copy.GetWorkload().GetSpec().GetContainers() {
		c.ContainerSpec = nil
	}
	return copy
}

// FromRestore describes the cold sandbox needed by a volumes-only restore.
// The snapshot files themselves are consumed only by RestoreWorkload.
func FromRestore(req *ateworkerpb.RestoreWorkloadRequest) *ateworkerpb.PrepareSandboxRequest {
	return &ateworkerpb.PrepareSandboxRequest{FromCheckpoint: true, Workload: &ateworkerpb.RunWorkloadRequest{
		Atespace: req.GetAtespace(), ActorName: req.GetActorName(), ActorUid: req.GetActorUid(),
		ActorTemplateAtespace: req.GetActorTemplateAtespace(), ActorTemplateName: req.GetActorTemplateName(),
		RunscPath: req.GetRunscPath(), RuntimeAssetPaths: req.GetRuntimeAssetPaths(), Spec: req.GetSpec(),
		ActorDirs: req.GetActorDirs(), EgressGateway: req.GetEgressGateway(),
		CpuMilli: req.GetCpuMilli(), MemoryBytes: req.GetMemoryBytes(),
	}}
}
