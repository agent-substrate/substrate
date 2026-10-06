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

package controlapi

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/hostruntimepb"
	"google.golang.org/protobuf/proto"
)

type capturingHostRuntime struct {
	endpoint string
	request  *hostruntimepb.ActivateRequest
	response *hostruntimepb.ActivateResponse
}

func (r *capturingHostRuntime) Activate(_ context.Context, endpoint string, req *hostruntimepb.ActivateRequest) (*hostruntimepb.ActivateResponse, error) {
	r.endpoint = endpoint
	r.request = proto.Clone(req).(*hostruntimepb.ActivateRequest)
	return r.response, nil
}

func (*capturingHostRuntime) Terminate(context.Context, string, *hostruntimepb.TerminateRequest) error {
	return nil
}

func TestEnsureMacActivatedDispatchesAndPersistsEndpoint(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "mac-1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "mac-template"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RESUMING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:          &ateapipb.ObjectRef{Name: "mac-worker-1"},
				RuntimeEndpoint: "dns:///mac-worker-1.example:9443",
			},
		},
	})
	tmpl := &ateapipb.ActorTemplate{
		MacVm: &ateapipb.MacVMWorkload{
			Image: "registry.example/mac-base@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			WakeupProbe: &ateapipb.ContainerWakeupProbe{
				HttpGet:        &ateapipb.HTTPGetAction{Path: "/ready", Port: 8123},
				TimeoutSeconds: 120,
			},
		},
		Resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{
			{Name: "cpu", Quantity: "6"},
			{Name: "memory", Quantity: "12Gi"},
		}},
	}
	runtime := &capturingHostRuntime{response: &hostruntimepb.ActivateResponse{
		Endpoint: &hostruntimepb.ActorEndpoint{Host: "192.168.64.17", Port: 8123},
	}}
	w := &ActorWorkflow{store: persistence, hostRuntime: runtime}
	actorRef := resources.ActorRefFromActor(actor)

	if _, err := w.ensureMacActivated(ctx, actorRef, actor, tmpl, resumeSnapshotSource{}); err != nil {
		t.Fatalf("ensureMacActivated: %v", err)
	}
	if runtime.endpoint != "dns:///mac-worker-1.example:9443" {
		t.Errorf("Activate endpoint = %q", runtime.endpoint)
	}
	wantRequest := &hostruntimepb.ActivateRequest{
		ActorUid:    actor.GetMetadata().GetUid(),
		Image:       tmpl.GetMacVm().GetImage(),
		CpuMilli:    6000,
		MemoryBytes: 12 * 1024 * 1024 * 1024,
		ReadinessProbe: &hostruntimepb.HTTPReadinessProbe{
			Port: 8123, Path: "/ready", TimeoutSeconds: 120,
		},
	}
	if !proto.Equal(runtime.request, wantRequest) {
		t.Errorf("Activate request = %v, want %v", runtime.request, wantRequest)
	}
	updated, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	wantEndpoint := &ateapipb.ActorEndpoint{Host: "192.168.64.17", Port: 8123}
	if got := updated.GetStatus().GetWorkerAssignment().GetActorEndpoint(); !proto.Equal(got, wantEndpoint) {
		t.Errorf("persisted endpoint = %v, want %v", got, wantEndpoint)
	}
}

func TestEnsureMacActivatedRejectsInvalidProviderEndpoint(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "mac-1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "mac-template"},
		Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RESUMING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker: &ateapipb.ObjectRef{Name: "mac-worker-1"}, RuntimeEndpoint: "dns:///mac-worker-1.example:9443",
			}},
	})
	runtime := &capturingHostRuntime{response: &hostruntimepb.ActivateResponse{Endpoint: &hostruntimepb.ActorEndpoint{Port: 8123}}}
	w := &ActorWorkflow{store: persistence, hostRuntime: runtime}
	tmpl := &ateapipb.ActorTemplate{MacVm: &ateapipb.MacVMWorkload{Image: "registry.example/mac-base@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}

	if _, err := w.ensureMacActivated(ctx, resources.ActorRefFromActor(actor), actor, tmpl, resumeSnapshotSource{}); err == nil {
		t.Fatal("ensureMacActivated succeeded with an empty provider host")
	}
	updated, err := persistence.GetActor(ctx, resources.ActorRefFromActor(actor))
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if got := updated.GetStatus().GetWorkerAssignment().GetActorEndpoint(); got != nil {
		t.Errorf("persisted endpoint = %v, want nil", got)
	}
}

func TestEnsureVolumesAttachedAllowsExternalWorkerWithoutVolumes(t *testing.T) {
	w := &ActorWorkflow{}
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "mac-1"},
		Status:   &ateapipb.ActorStatus{},
	}
	worker := &ateapipb.Worker{
		Metadata:     &ateapipb.ResourceMetadata{Name: "mac-worker-1"},
		ExternalHost: &ateapipb.ExternalWorkerHost{RuntimeEndpoint: "dns:///mac-worker-1.example:9443"},
	}

	if err := w.ensureVolumesAttached(context.Background(), actor, worker, &ateapipb.ActorTemplate{}); err != nil {
		t.Fatalf("ensureVolumesAttached: %v", err)
	}
}
