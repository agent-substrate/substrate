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

package functionaltest

import (
	"context"
	"testing"

	"go.ate.dev/substrate/internal/volume"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
)

// CreateVolume serves the per-actor template volume the owned-volume case
// below provisions.
func (p *externalVolumePlugin) CreateVolume(_ context.Context, req volume.CreateVolumeRequest) (volume.CreateVolumeResponse, error) {
	return volume.CreateVolumeResponse{VolumeID: "disk/" + req.Name, VolumeContext: req.Parameters}, nil
}

func resetAteletCalls(tc *testContext) {
	tc.fakeAtelet.Lock.Lock()
	defer tc.fakeAtelet.Lock.Unlock()
	tc.fakeAtelet.RunCalled, tc.fakeAtelet.RunRequest = false, nil
	tc.fakeAtelet.RestoreCalled, tc.fakeAtelet.RestoreRequest = false, nil
}

func ateletCalls(tc *testContext) (run, restore bool) {
	tc.fakeAtelet.Lock.Lock()
	defer tc.fakeAtelet.Lock.Unlock()
	return tc.fakeAtelet.RunCalled, tc.fakeAtelet.RestoreCalled
}

func getActor(t *testing.T, tc *testContext, name string) *ateapipb.Actor {
	t.Helper()
	actor, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor(%s) failed: %v", name, err)
	}
	return actor
}

func TestExternalVolumeRef_ColdBootsThenRestoresOwnSnapshot(t *testing.T) {
	ns := namespaceForTest("ns-extvol-coldboot")
	tc, _ := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	createExternalVolume(t, tc, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	tmpl := createTemplateWithVolumes(t, tc, ns,
		[]*ateapipb.Volume{{Name: "work", External: &ateapipb.ExternalVolumeSource{Ref: &ateapipb.ExternalVolumeRef{Name: "shared"}}}},
		[]*ateapipb.VolumeMount{{Name: "work", MountPath: "/mnt/work"}})
	if tmpl.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag() == nil {
		t.Fatal("test template has no golden tag; the case needs one to prove it is bypassed")
	}
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	ctx := context.Background()
	ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "borrower"}

	createBorrowingActor(t, tc, "borrower", nil)
	if snaps := getActor(t, tc, "borrower").GetStatus().GetSnapshots(); len(snaps) != 0 {
		t.Fatalf("new actor carries snapshots %v, want none: it must not seed from the golden", snaps)
	}

	waitForWorkerAvailable(t, tc, workerName)
	resetAteletCalls(tc)
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("first ResumeActor failed: %v", err)
	}
	if run, restore := ateletCalls(tc); !run || restore {
		t.Fatalf("first resume: Run=%v Restore=%v, want a cold boot through Run", run, restore)
	}

	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor:   ref,
		Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	}); err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	own := durableSnapshotURI(getActor(t, tc, "borrower").GetStatus())
	if own == "" || own == goldenSnapshotURI(t) {
		t.Fatalf("snapshot after suspend = %q, want the actor's own", own)
	}

	waitForWorkerAvailable(t, tc, workerName)
	resetAteletCalls(tc)
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("second ResumeActor failed: %v", err)
	}
	if run, restore := ateletCalls(tc); run || !restore {
		t.Fatalf("second resume: Run=%v Restore=%v, want a restore", run, restore)
	}
	if got := tc.fakeAtelet.lastRestoreRequest().GetSnapshot().GetObject().GetSnapshotUri(); got != own {
		t.Errorf("second resume restored %q, want the actor's own snapshot %q", got, own)
	}
}

func TestExternalVolumeRef_RejectsSourceTag(t *testing.T) {
	ns := namespaceForTest("ns-extvol-sourcetag")
	tc, _ := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	createExternalVolume(t, tc, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	tmpl := createTemplateWithVolumes(t, tc, ns,
		[]*ateapipb.Volume{{Name: "work", External: &ateapipb.ExternalVolumeSource{Ref: &ateapipb.ExternalVolumeRef{Name: "shared"}}}},
		[]*ateapipb.VolumeMount{{Name: "work", MountPath: "/mnt/work"}})

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "clone"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		SourceTag:     tmpl.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag(),
	}})
	assertGrpcError(t, err, codes.FailedPrecondition, "Tag cloning does not support ActorTemplates that reference an ExternalVolume")
}

func TestExternalVolumeTemplate_FirstResumeRestoresGolden(t *testing.T) {
	ns := namespaceForTest("ns-extvol-owned-golden")
	tc, _ := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	createTemplateWithVolumes(t, tc, ns,
		[]*ateapipb.Volume{{Name: "work", External: &ateapipb.ExternalVolumeSource{Template: &ateapipb.ExternalVolumeTemplate{StorageClassName: "standard", CapacityBytes: 1 << 30}}}},
		[]*ateapipb.VolumeMount{{Name: "work", MountPath: "/mnt/work"}})
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	ctx := context.Background()
	ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "owner"}

	if _, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "owner"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if got := durableSnapshotURI(getActor(t, tc, "owner").GetStatus()); got != goldenSnapshotURI(t) {
		t.Fatalf("new actor snapshot = %q, want the golden %q", got, goldenSnapshotURI(t))
	}

	waitForWorkerAvailable(t, tc, workerName)
	resetAteletCalls(tc)
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}
	if run, restore := ateletCalls(tc); run || !restore {
		t.Fatalf("first resume: Run=%v Restore=%v, want a restore from the golden", run, restore)
	}
	if got := tc.fakeAtelet.lastRestoreRequest().GetSnapshot().GetObject().GetSnapshotUri(); got != goldenSnapshotURI(t) {
		t.Errorf("first resume restored %q, want the golden %q", got, goldenSnapshotURI(t))
	}
}
