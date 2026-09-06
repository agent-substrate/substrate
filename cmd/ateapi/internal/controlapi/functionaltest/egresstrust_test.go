//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package functionaltest

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// Spelled as literals rather than imported: these names are the contract
// with atelet's allowlist and workload readers, so constant drift must fail
// here.
const (
	injectedTrustVolume    = "trust.ate.dev"
	injectedTrustBundle    = "egress-mitm.ate.dev"
	injectedTrustMountPath = "/run/substrate/certs"
)

// TestActorLifecycle_InjectsEgressTrustVolume asserts the injected volume on
// every wire spec across resume, pause, resume-from-paused, and suspend,
// pinning that each RPC flow builds its spec through the injection wrapper.
func TestActorLifecycle_InjectsEgressTrustVolume(t *testing.T) {
	ns := namespaceForTest("ns-egress-trust")
	tc := setupTestWithEgressTrustInjection(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)
	worker := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	const actorName = "actor-egress-trust"
	ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: actorName}
	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: actorName},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		},
	}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// Boot skips the golden snapshot createTemplate seeds; without it the
	// first resume is a Restore and no Run spec is ever recorded.
	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: ref, Boot: true}); err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}
	assertEgressTrustInjected(t, "Run", lockedSpec(tc, func() *ateletpb.WorkloadSpec { return tc.fakeAtelet.RunRequest.GetSpec() }))

	if _, err := tc.client.PauseActor(context.Background(), &ateapipb.PauseActorRequest{Actor: ref}); err != nil {
		t.Fatalf("PauseActor failed: %v", err)
	}
	pause := lockedCheckpoint(tc)
	if got := pause.GetType(); got != ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL {
		t.Errorf("pause checkpoint type = %v, want CHECKPOINT_TYPE_LOCAL", got)
	}
	assertEgressTrustInjected(t, "Checkpoint (pause)", pause.GetSpec())

	// Pause and suspend release the worker after the RPC returns; the next
	// resume needs it free again.
	waitForWorkerAvailable(t, tc, worker)
	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor from paused failed: %v", err)
	}
	assertEgressTrustInjected(t, "Restore", lockedSpec(tc, func() *ateletpb.WorkloadSpec { return tc.fakeAtelet.RestoreRequest.GetSpec() }))

	if _, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{Actor: ref}); err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	// The type assertion is what proves this is the suspend checkpoint and
	// not a stale read of the pause one.
	suspend := lockedCheckpoint(tc)
	if got := suspend.GetType(); got != ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL {
		t.Errorf("suspend checkpoint type = %v, want CHECKPOINT_TYPE_EXTERNAL", got)
	}
	assertEgressTrustInjected(t, "Checkpoint (suspend)", suspend.GetSpec())

	// Terminate: delete only runs from SUSPENDED or CRASHED, and only a
	// still-assigned worker gets the RPC, so resume again and crash the
	// actor in the store with its assignment intact.
	waitForWorkerAvailable(t, tc, worker)
	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor from suspended failed: %v", err)
	}
	actorRef := resources.ActorRef{Atespace: testAtespace, Name: actorName}
	current, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if _, err := tc.persistence.UpdateActor(context.Background(), actorRef, store.PreconditionFrom(current), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
		return nil
	}); err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}
	if _, err := tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{Actor: ref}); err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
	assertEgressTrustInjected(t, "Terminate", lockedSpec(tc, func() *ateletpb.WorkloadSpec { return tc.fakeAtelet.TerminateRequest.GetSpec() }))
}

func lockedSpec(tc *testContext, get func() *ateletpb.WorkloadSpec) *ateletpb.WorkloadSpec {
	tc.fakeAtelet.Lock.Lock()
	defer tc.fakeAtelet.Lock.Unlock()
	return get()
}

func lockedCheckpoint(tc *testContext) *ateletpb.CheckpointRequest {
	tc.fakeAtelet.Lock.Lock()
	defer tc.fakeAtelet.Lock.Unlock()
	return tc.fakeAtelet.CheckpointRequest
}

func assertEgressTrustInjected(t *testing.T, op string, spec *ateletpb.WorkloadSpec) {
	t.Helper()
	if spec == nil {
		t.Fatalf("%s: no workload spec recorded", op)
	}

	found := false
	for _, vol := range spec.GetVolumes() {
		if vol.GetName() != injectedTrustVolume {
			continue
		}
		found = true
		sources := vol.GetSystemInfo().GetDataSources()
		if len(sources) != 1 {
			t.Errorf("%s: volume %q has %d data sources, want 1", op, injectedTrustVolume, len(sources))
			break
		}
		tb := sources[0].GetTrustBundle()
		if tb.GetName() != injectedTrustBundle || tb.GetPath() != injectedTrustBundle+".pem" {
			t.Errorf("%s: volume %q projects {name %q, path %q}, want {name %q, path %q}", op, injectedTrustVolume, tb.GetName(), tb.GetPath(), injectedTrustBundle, injectedTrustBundle+".pem")
		}
	}
	if !found {
		t.Errorf("%s: spec is missing the injected volume %q", op, injectedTrustVolume)
	}

	for _, ctr := range spec.GetContainers() {
		mounted := false
		for _, m := range ctr.GetVolumeMounts() {
			if m.GetName() == injectedTrustVolume && m.GetMountPath() == injectedTrustMountPath {
				mounted = true
			}
		}
		if !mounted {
			t.Errorf("%s: container %q is missing the injected mount %q at %q", op, ctr.GetName(), injectedTrustVolume, injectedTrustMountPath)
		}
	}
}
