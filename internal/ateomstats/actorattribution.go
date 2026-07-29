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

// ActorAttribution is what a usage sample is attributed to: the actor an ateom
// retains from the RunWorkloadRequest / RestoreWorkloadRequest that started the
// workload it is currently executing.
//
// ateom is otherwise incurious about who it is running: these fields arrive on
// the call that starts the workload and are not needed again by the run,
// checkpoint, or restore paths. GetWorkloadStats needs them, because a usage
// sample is only useful once attributed to an actor and a template, and the
// request carries nothing of its own beyond the actor UID it is asserting.
//
// This is the same tuple actorlog passes around as loose parameters
// (EmitLifecycleLog, StartJSONLogPipe, WrapContainerLogs) and that ateattr
// stamps as ate.actor.* / ate.template.*: an ActorRef plus the two things a ref
// does not carry, the server-assigned uid and the template it was built from.
//
// Unrelated to the credential sense of "actor identity" elsewhere in the repo
// (ateapi's ActorIdentity service, substratex509, ateompath.ActorIdentityDirPath)
// — nothing here is a secret or is presented as proof of anything.
type ActorAttribution struct {
	Ref               resources.ActorRef
	UID               string
	TemplateNamespace string
	TemplateName      string
}

// attributionSource is the attribution-bearing subset of the requests that
// start a workload. Both RunWorkloadRequest and RestoreWorkloadRequest satisfy
// it.
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
)

// ActorAttributionFromRequest extracts the attribution an ateom should retain
// for the workload req starts.
func ActorAttributionFromRequest(req attributionSource) ActorAttribution {
	return ActorAttribution{
		Ref: resources.ActorRef{
			Atespace: req.GetAtespace(),
			Name:     req.GetActorName(),
		},
		UID:               req.GetActorUid(),
		TemplateNamespace: req.GetActorTemplateNamespace(),
		TemplateName:      req.GetActorTemplateName(),
	}
}
