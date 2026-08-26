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

// Package ateomstats holds the pieces both ateom runtimes need to answer
// ateompb.Ateom/GetWorkloadStats. Today that is the attribution an ateom
// retains for the workload it is executing; the per-runtime measurement reads
// live with their runtimes (the cgroup read is only meaningful inside the
// gVisor worker's cgroup namespace, the guest-agent read only over the
// micro-VM's vsock).
package ateomstats

import (
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// attributionSource is the attribution-bearing subset of the ateom requests
// that name the actor they act on.
type attributionSource interface {
	GetAtespace() string
	GetActorName() string
	GetActorUid() string
	GetActorTemplateNamespace() string
	GetActorTemplateName() string
}

var (
	_ attributionSource = (*ateompb.RunWorkloadRequest)(nil)
	_ attributionSource = (*ateompb.RestoreWorkloadRequest)(nil)
	_ attributionSource = (*ateompb.CheckpointWorkloadRequest)(nil)
	_ attributionSource = (*ateompb.TerminateWorkloadRequest)(nil)
)

// ActorAttributionFromRequest extracts the attribution an ateom should retain
// for the workload req starts: nothing later in the run, checkpoint, or restore
// paths carries it, and GetWorkloadStats needs it.
func ActorAttributionFromRequest(req attributionSource) resources.ActorAttribution {
	return resources.ActorAttribution{
		Ref: resources.ActorRef{
			Atespace: req.GetAtespace(),
			Name:     req.GetActorName(),
		},
		UID:               req.GetActorUid(),
		TemplateNamespace: req.GetActorTemplateNamespace(),
		TemplateName:      req.GetActorTemplateName(),
	}
}
