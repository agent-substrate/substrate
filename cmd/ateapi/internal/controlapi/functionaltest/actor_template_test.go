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
	"fmt"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/ateapi/internal/controlapi"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestActorTemplateCRUD(t *testing.T) {
	ns := namespaceForTest("ns-template-crud")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()

	created, err := tc.client.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{
		ActorTemplate: &ateapi.ActorTemplate{
			Metadata:       &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "tmpl-a"},
			Containers:     []*ateapi.Container{{Name: "main", Image: "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}},
			SnapshotConfig: &ateapi.SnapshotConfig{StorageLocation: "gs://my-bucket/snapshots"},
			Resources:      &ateapi.ResourceRequirements{Limits: resources.CPUMemory(1000, 1<<30)},
			// Server-owned status on the request is ignored.
			Status: &ateapi.ActorTemplateStatus{
				GoldenSnapshotStatus: &ateapi.GoldenSnapshotStatus{
					GoldenTag: &ateapi.ObjectRef{Atespace: testAtespace, Name: "golden-tag"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}
	want := &ateapi.ActorTemplate{
		Metadata:   &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "tmpl-a", Version: 1},
		Containers: []*ateapi.Container{{Name: "main", Image: "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}},
		SnapshotConfig: &ateapi.SnapshotConfig{
			StorageLocation:      "gs://my-bucket/snapshots",
			PreferredFidelity:    ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED},
		},
		Resources: &ateapi.ResourceRequirements{Limits: resources.CPUMemory(1000, 1<<30)},
		Status:    &ateapi.ActorTemplateStatus{},
	}
	if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
		t.Errorf("CreateActorTemplate response mismatch (-want +got):\n%s", diff)
	}

	_, err = tc.client.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{
		ActorTemplate: &ateapi.ActorTemplate{
			Metadata:       &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "tmpl-a"},
			Containers:     []*ateapi.Container{{Name: "main", Image: "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}},
			SnapshotConfig: &ateapi.SnapshotConfig{StorageLocation: "gs://my-bucket/snapshots"},
			Resources:      &ateapi.ResourceRequirements{Limits: resources.CPUMemory(1000, 512<<20)},
		},
	})
	assertGrpcError(t, err, codes.AlreadyExists, "ActorTemplate "+testAtespace+"/tmpl-a already exists")

	got, err := tc.client.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Atespace: testAtespace, Name: "tmpl-a"}})
	if err != nil {
		t.Fatalf("GetActorTemplate failed: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetActorTemplate response mismatch (-created +got):\n%s", diff)
	}

	list, err := tc.client.ListActorTemplates(ctx, &ateapi.ListActorTemplatesRequest{})
	if err != nil {
		t.Fatalf("ListActorTemplates failed: %v", err)
	}
	if len(list.GetActorTemplates()) != 1 || list.GetActorTemplates()[0].GetMetadata().GetName() != "tmpl-a" {
		t.Errorf("ListActorTemplates = %v, want [tmpl-a]", list.GetActorTemplates())
	}

	// The atespace filter scopes the listing: a match returns the template, a
	// different atespace returns nothing.
	list, err = tc.client.ListActorTemplates(ctx, &ateapi.ListActorTemplatesRequest{Atespace: testAtespace})
	if err != nil {
		t.Fatalf("ListActorTemplates(atespace) failed: %v", err)
	}
	if len(list.GetActorTemplates()) != 1 {
		t.Errorf("ListActorTemplates(atespace=%s) = %v, want [tmpl-a]", testAtespace, list.GetActorTemplates())
	}
	list, err = tc.client.ListActorTemplates(ctx, &ateapi.ListActorTemplatesRequest{Atespace: "other-atespace"})
	if err != nil {
		t.Fatalf("ListActorTemplates(other atespace) failed: %v", err)
	}
	if len(list.GetActorTemplates()) != 0 {
		t.Errorf("ListActorTemplates(atespace=other-atespace) = %v, want []", list.GetActorTemplates())
	}

	deleted, err := tc.client.DeleteActorTemplate(ctx, &ateapi.DeleteActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Atespace: testAtespace, Name: "tmpl-a"}})
	if err != nil {
		t.Fatalf("DeleteActorTemplate failed: %v", err)
	}
	if diff := cmp.Diff(created, deleted, protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("DeleteActorTemplate response mismatch (-created +deleted):\n%s", diff)
	}
	if deleted.GetMetadata().GetDeleteTime() == nil {
		t.Errorf("the deleted template carries no delete_time")
	}
	_, err = tc.client.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Atespace: testAtespace, Name: "tmpl-a"}})
	assertGrpcError(t, err, codes.NotFound, "ActorTemplate "+testAtespace+"/tmpl-a not found")
	_, err = tc.client.DeleteActorTemplate(ctx, &ateapi.DeleteActorTemplateRequest{ActorTemplate: &ateapi.ObjectRef{Atespace: testAtespace, Name: "tmpl-a"}})
	assertGrpcError(t, err, codes.NotFound, "ActorTemplate "+testAtespace+"/tmpl-a not found")
}

func TestGoldenTagLifecycle(t *testing.T) {
	ns := namespaceForTest("golden-tag")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	createWorkerPool(t, tc, ns, "pool1", map[string]string{poolLabelKey: ns})
	tmpl := createTemplateWithSelector(t, tc, "golden-template", &ateapi.Selector{MatchLabels: map[string]string{poolLabelKey: ns}})
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	templateRef := resources.ActorTemplateRefFromActorTemplate(tmpl)
	// Created before readiness: a later golden tag must not change its source.
	early, err := tc.client.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "early"}, ActorTemplate: templateRef.ToObjectRef(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	controlapi.NewActorTemplateReconciler(tc.persistence, tc.service, 7*time.Second).Start(ctx)
	var goldenRef *ateapi.ObjectRef
	err = wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		current, err := tc.client.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{ActorTemplate: templateRef.ToObjectRef()})
		goldenRef = current.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag()
		return goldenRef != nil, err
	})
	if err != nil {
		t.Fatalf("waiting for golden tag: %v", err)
	}
	golden, err := tc.client.GetTag(ctx, &ateapi.GetTagRequest{Tag: goldenRef})
	if err != nil {
		t.Fatal(err)
	}
	uri := tagSnapshotURI(golden)
	parsed, err := resources.ParseSnapshotURI(uri)
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.OwnedBy(resources.TagSnapshotOwner(testAtespace, golden.GetMetadata().GetUid())) {
		t.Fatalf("golden snapshot %q is not under the golden tag's own prefix", uri)
	}
	assertSnapshotPresent(t, tc, uri)
	_, err = tc.client.GetActor(ctx, &ateapi.GetActorRequest{Actor: goldenRef})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("golden actor was not deleted: %v", err)
	}
	late, err := tc.client.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "late"}, ActorTemplate: templateRef.ToObjectRef(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if durableSnapshotURI(late.GetStatus()) != uri || durableSnapshot(late.GetStatus()).GetActorTemplateUid() != tmpl.GetMetadata().GetUid() {
		t.Fatal("actor did not inherit golden tag snapshot and template UID")
	}
	waitForWorkerAvailable(t, tc, workerName)
	tc.fakeAtelet.Lock.Lock()
	tc.fakeAtelet.RunCalled = false
	tc.fakeAtelet.Lock.Unlock()
	if _, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: resources.ActorRefFromActor(early).ToObjectRef()}); err != nil {
		t.Fatal(err)
	}
	if !tc.fakeAtelet.RunCalled {
		t.Fatal("actor created before golden readiness did not cold boot")
	}
	if _, err := tc.client.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: resources.ActorRefFromActor(early).ToObjectRef()}); err != nil {
		t.Fatal(err)
	}
	waitForWorkerAvailable(t, tc, workerName)
	if _, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: resources.ActorRefFromActor(late).ToObjectRef()}); err != nil {
		t.Fatal(err)
	}
	if !tc.fakeAtelet.RestoreCalled {
		t.Fatal("actor did not restore golden tag")
	}
	if _, err := tc.client.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor:   resources.ActorRefFromActor(late).ToObjectRef(),
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	}); err != nil {
		t.Fatal(err)
	}
	assertSnapshotPresent(t, tc, uri)
	for _, actor := range []*ateapi.Actor{early, late} {
		if _, err := tc.client.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: resources.ActorRefFromActor(actor).ToObjectRef()}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tc.client.DeleteActorTemplate(ctx, &ateapi.DeleteActorTemplateRequest{ActorTemplate: templateRef.ToObjectRef()}); err != nil {
		t.Fatal(err)
	}
	assertSnapshotCollected(t, tc, uri)
	_, err = tc.client.GetTag(ctx, &ateapi.GetTagRequest{Tag: goldenRef})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("golden tag was not deleted: %v", err)
	}
}

func TestActorLifecycleWithoutGoldenSnapshot(t *testing.T) {
	ns := namespaceForTest("no-golden")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	createWorkerPool(t, tc, ns, "pool1", map[string]string{poolLabelKey: ns})
	tmpl, err := tc.client.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{ActorTemplate: &ateapi.ActorTemplate{
		Metadata: &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "no-golden"},
		SnapshotConfig: &ateapi.SnapshotConfig{
			StorageLocation:      testStorageLocation,
			GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED},
		},
		Containers:     []*ateapi.Container{{Name: "main", Image: "main@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", Command: []string{"/main"}}},
		WorkerSelector: &ateapi.Selector{MatchLabels: map[string]string{poolLabelKey: ns}},
		Resources:      &ateapi.ResourceRequirements{Limits: resources.CPUMemory(1000, 512<<20)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if tmpl.GetSnapshotConfig().GetGoldenSnapshotConfig().GetMode() != ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED {
		t.Fatal("CreateActorTemplate did not preserve the golden snapshot opt-out")
	}
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	controlTemplate := createTemplateWithSelector(t, tc, "golden-control", &ateapi.Selector{MatchLabels: map[string]string{poolLabelKey: ns}})
	goldenRef := &ateapi.ObjectRef{Atespace: testAtespace, Name: tmpl.GetMetadata().GetUid()}
	assertNoGolden := func(ctx context.Context) error {
		if _, err := tc.client.GetActor(ctx, &ateapi.GetActorRequest{Actor: goldenRef}); status.Code(err) != codes.NotFound {
			return fmt.Errorf("disabled template's golden actor: got %v, want NotFound", err)
		}
		if _, err := tc.client.GetTag(ctx, &ateapi.GetTagRequest{Tag: goldenRef}); status.Code(err) != codes.NotFound {
			return fmt.Errorf("disabled template's golden tag: got %v, want NotFound", err)
		}
		return nil
	}
	controlapi.NewActorTemplateReconciler(tc.persistence, tc.service, 7*time.Second).Start(ctx)
	// The enabled template proves the same reconciler is making progress.
	err = wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		if err := assertNoGolden(ctx); err != nil {
			return false, err
		}
		current, err := tc.client.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{
			ActorTemplate: resources.ActorTemplateRefFromActorTemplate(controlTemplate).ToObjectRef(),
		})
		return current.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag() != nil, err
	})
	if err != nil {
		t.Fatalf("waiting for enabled template's golden tag: %v", err)
	}
	waitForWorkerAvailable(t, tc, workerName)
	actor, err := tc.client.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "actor"},
		ActorTemplate: resources.ActorTemplateRefFromActorTemplate(tmpl).ToObjectRef(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if durableSnapshot(actor.GetStatus()) != nil {
		t.Fatal("new actor unexpectedly inherited a snapshot")
	}
	ref := resources.ActorRefFromActor(actor).ToObjectRef()
	tc.fakeAtelet.Lock.Lock()
	tc.fakeAtelet.RunCalled, tc.fakeAtelet.RestoreCalled = false, false
	tc.fakeAtelet.Lock.Unlock()
	if _, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatal(err)
	}
	tc.fakeAtelet.Lock.Lock()
	ran, restored := tc.fakeAtelet.RunCalled, tc.fakeAtelet.RestoreCalled
	tc.fakeAtelet.Lock.Unlock()
	if !ran || restored {
		t.Fatal("first resume did not cold boot")
	}
	suspended, err := tc.client.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: ref, Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS}})
	if err != nil {
		t.Fatal(err)
	}
	snapshotURI := durableSnapshotURI(suspended.GetActor().GetStatus())
	if snapshotURI == "" {
		t.Fatal("suspend did not produce the actor's own snapshot")
	}
	assertSnapshotPresent(t, tc, snapshotURI)
	waitForWorkerAvailable(t, tc, workerName)
	if _, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatal(err)
	}
	if got := tc.fakeAtelet.lastRestoreRequest().GetSnapshot().GetObject().GetSnapshotUri(); got != snapshotURI {
		t.Fatalf("restored snapshot = %q, want actor's snapshot %q", got, snapshotURI)
	}
	if err := assertNoGolden(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.client.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: ref}); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.client.DeleteActorTemplate(ctx, &ateapi.DeleteActorTemplateRequest{ActorTemplate: resources.ActorTemplateRefFromActorTemplate(tmpl).ToObjectRef()}); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.client.DeleteActorTemplate(ctx, &ateapi.DeleteActorTemplateRequest{ActorTemplate: resources.ActorTemplateRefFromActorTemplate(controlTemplate).ToObjectRef()}); err != nil {
		t.Fatal(err)
	}
}

func TestListActorTemplates_InvalidPageToken(t *testing.T) {
	ns := namespaceForTest("ns-template-invalid-token")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.ListActorTemplates(context.Background(),
		&ateapi.ListActorTemplatesRequest{Options: &ateapi.ListOptions{PageToken: "%%%"}})
	assertGrpcError(t, err, codes.InvalidArgument, "invalid page_token")
}

func TestDeleteActorTemplate_Preconditions(t *testing.T) {
	ns := namespaceForTest("ns-delete-template-preconditions")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()
	tmpl := createTemplateWithSelector(t, tc, "tmpl1", nil)
	templateRef := resources.ActorTemplateRefFromActorTemplate(tmpl)
	ref := templateRef.ToObjectRef()
	del := func(opts *ateapi.DeleteOptions) error {
		_, err := tc.client.DeleteActorTemplate(ctx, &ateapi.DeleteActorTemplateRequest{ActorTemplate: ref, Options: opts})
		return err
	}

	uid, version := tmpl.GetMetadata().GetUid(), tmpl.GetMetadata().GetVersion()

	assertGrpcError(t, del(&ateapi.DeleteOptions{Version: version + 1}), codes.Aborted, "concurrent update conflict, please retry")
	assertGrpcError(t, del(&ateapi.DeleteOptions{Uid: uid, Version: version + 1}), codes.Aborted, "concurrent update conflict, please retry")
	assertGrpcError(t, del(&ateapi.DeleteOptions{Uid: foreignUID}), codes.Aborted, "ActorTemplate "+templateRef.String()+" does not have uid "+foreignUID)
	assertGrpcError(t, del(&ateapi.DeleteOptions{Uid: foreignUID, Version: version}), codes.Aborted, "ActorTemplate "+templateRef.String()+" does not have uid "+foreignUID)
	if _, err := tc.client.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{ActorTemplate: ref}); err != nil {
		t.Fatalf("a refused delete removed the template: %v", err)
	}

	if err := del(&ateapi.DeleteOptions{Version: version}); err != nil {
		t.Fatalf("DeleteActorTemplate with the matching version: %v", err)
	}
	tmpl = createTemplateWithSelector(t, tc, "tmpl1", nil)
	if err := del(&ateapi.DeleteOptions{Uid: tmpl.GetMetadata().GetUid()}); err != nil {
		t.Fatalf("DeleteActorTemplate with the matching uid: %v", err)
	}
	tmpl = createTemplateWithSelector(t, tc, "tmpl1", nil)
	if err := del(&ateapi.DeleteOptions{Uid: tmpl.GetMetadata().GetUid(), Version: tmpl.GetMetadata().GetVersion()}); err != nil {
		t.Fatalf("DeleteActorTemplate with both guards: %v", err)
	}
	_, err := tc.client.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{ActorTemplate: ref})
	assertGrpcError(t, err, codes.NotFound, "ActorTemplate "+templateRef.String()+" not found")
}

// TestCreateActorTemplate_ExternalVolumesPreview checks that external volumes
// are accepted only when the server enables the gate and the client opts in
// to preview features.
func TestCreateActorTemplate_ExternalVolumesPreview(t *testing.T) {
	ns := namespaceForTest("ns-template-ext-vol-preview")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	for i, tt := range []struct {
		name     string
		gates    []string
		optIn    bool
		wantCode codes.Code
	}{
		{name: "gate off", gates: nil, optIn: true, wantCode: codes.InvalidArgument},
		{name: "no opt-in", gates: []string{"ExternalVolumes"}, optIn: false, wantCode: codes.InvalidArgument},
		{name: "gate on and opt-in", gates: []string{"ExternalVolumes"}, optIn: true, wantCode: codes.OK},
	} {
		t.Run(tt.name, func(t *testing.T) {
			preview.InitForTest(t, tt.gates...)
			ctx := context.Background()
			if tt.optIn {
				ctx = metadata.AppendToOutgoingContext(ctx, preview.Header, "true")
			}
			_, err := tc.client.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{
				ActorTemplate: &ateapi.ActorTemplate{
					Metadata: &ateapi.ResourceMetadata{Atespace: testAtespace, Name: fmt.Sprintf("tmpl-ext-%d", i)},
					Containers: []*ateapi.Container{{
						Name:         "main",
						Image:        "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
						VolumeMounts: []*ateapi.VolumeMount{{Name: "data", MountPath: "/data"}},
					}},
					Volumes: []*ateapi.Volume{{
						Name:     "data",
						External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{StorageClassName: "standard", CapacityBytes: 1 << 30}},
					}},
					Resources:      &ateapi.ResourceRequirements{Limits: resources.CPUMemory(1000, 1<<30)},
					SnapshotConfig: &ateapi.SnapshotConfig{StorageLocation: "gs://my-bucket/snapshots"},
				},
			})
			if status.Code(err) != tt.wantCode {
				t.Fatalf("CreateActorTemplate error = %v, want %v", err, tt.wantCode)
			}
		})
	}
}

// TestCreateActorTemplate_PerTemplateSnapshotLocationPreview pins how
// snapshotConfig.storageLocation combines the PerTemplateSnapshotLocation gate
// with the client's preview opt-in. With the gate off the field is ignored,
// so any client may set it. With the gate on, only a client that opts in may
// set it; a template that leaves it empty uses the server's
// --snapshot-location.
func TestCreateActorTemplate_PerTemplateSnapshotLocationPreview(t *testing.T) {
	ns := namespaceForTest("ns-template-storage-location-preview")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	for i, tt := range []struct {
		name     string
		gates    []string
		optIn    bool
		location string
		wantCode codes.Code
	}{
		{name: "gate off, no opt-in, location set", optIn: false, location: "gs://my-bucket/snapshots", wantCode: codes.OK},
		{name: "gate off, opt-in, location set", optIn: true, location: "gs://my-bucket/snapshots", wantCode: codes.OK},
		{name: "gate on, no opt-in, location set", gates: []string{"PerTemplateSnapshotLocation"}, optIn: false, location: "gs://my-bucket/snapshots", wantCode: codes.InvalidArgument},
		{name: "gate on, no opt-in, no location", gates: []string{"PerTemplateSnapshotLocation"}, optIn: false, wantCode: codes.OK},
		{name: "gate on, opt-in, location set", gates: []string{"PerTemplateSnapshotLocation"}, optIn: true, location: "gs://my-bucket/snapshots", wantCode: codes.OK},
		{name: "gate on, opt-in, no location", gates: []string{"PerTemplateSnapshotLocation"}, optIn: true, wantCode: codes.OK},
	} {
		t.Run(tt.name, func(t *testing.T) {
			preview.InitForTest(t, tt.gates...)
			ctx := context.Background()
			if tt.optIn {
				ctx = metadata.AppendToOutgoingContext(ctx, preview.Header, "true")
			}
			_, err := tc.client.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{
				ActorTemplate: &ateapi.ActorTemplate{
					Metadata: &ateapi.ResourceMetadata{Atespace: testAtespace, Name: fmt.Sprintf("tmpl-loc-%d", i)},
					Containers: []*ateapi.Container{{
						Name:  "main",
						Image: "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
					}},
					Resources:      &ateapi.ResourceRequirements{Limits: resources.CPUMemory(1000, 1<<30)},
					SnapshotConfig: &ateapi.SnapshotConfig{StorageLocation: tt.location},
				},
			})
			if status.Code(err) != tt.wantCode {
				t.Fatalf("CreateActorTemplate error = %v, want %v", err, tt.wantCode)
			}
		})
	}
}

func TestCreateActorTemplate_GoldenEgressPolicyPreview(t *testing.T) {
	tc := setupPreviewTest(t, namespaceForTest("ns-template-golden-egress-preview"))
	t.Cleanup(tc.cleanup)
	base := createTemplateWithSelector(t, tc, "base", nil)

	for i, tt := range []struct {
		name              string
		gates             []string
		optIn             bool
		snapshotsDisabled bool
		wantCode          codes.Code
	}{
		{name: "gate off", optIn: true, wantCode: codes.InvalidArgument},
		{name: "no opt-in", gates: []string{"GoldenEgressPolicy"}, wantCode: codes.InvalidArgument},
		{name: "unrelated gate", gates: []string{"ExternalVolumes"}, optIn: true, wantCode: codes.InvalidArgument},
		{name: "gate on and opt-in", gates: []string{"GoldenEgressPolicy"}, optIn: true, wantCode: codes.OK},
		{name: "all gates and opt-in", gates: []string{"ALL"}, optIn: true, wantCode: codes.OK},
		{name: "golden snapshots disabled", gates: []string{"GoldenEgressPolicy"}, optIn: true, snapshotsDisabled: true, wantCode: codes.InvalidArgument},
	} {
		t.Run(tt.name, func(t *testing.T) {
			preview.InitForTest(t, tt.gates...)
			client := tc.noOptInClient
			if tt.optIn {
				client = tc.client
			}
			tmpl := proto.Clone(base).(*ateapi.ActorTemplate)
			tmpl.Metadata = &ateapi.ResourceMetadata{Atespace: testAtespace, Name: fmt.Sprintf("golden-egress-%d", i)}
			tmpl.Status = nil
			tmpl.GoldenEgressPolicy = &ateapi.EgressPolicyTemplate{Rules: functionalEgressPolicy().Rules}
			if tt.snapshotsDisabled {
				tmpl.SnapshotConfig.GoldenSnapshotConfig = &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED}
			}
			ref := resources.ActorTemplateRefFromActorTemplate(tmpl).ToObjectRef()
			created, err := client.CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: tmpl})
			if tt.wantCode != codes.OK {
				assertGrpcErrorRegex(t, err, tt.wantCode, "golden_egress_policy")
				_, err = client.GetActorTemplate(t.Context(), &ateapi.GetActorTemplateRequest{ActorTemplate: ref})
				assertGrpcErrorRegex(t, err, codes.NotFound, "not found")
				return
			}
			if err != nil {
				t.Fatalf("CreateActorTemplate: %v", err)
			}
			wantPolicy := &ateapi.EgressPolicyTemplate{Rules: []*ateapi.EgressRule{{
				Http: &ateapi.HttpRule{Hostnames: []string{"api.example.com"}, Ports: &ateapi.Ports{Numbers: []int32{80}}},
			}}}
			if diff := cmp.Diff(wantPolicy, created.GetGoldenEgressPolicy(), protocmp.Transform()); diff != "" {
				t.Fatalf("golden policy (-want +got):\n%s", diff)
			}

			// Opting out or disabling the gate must not strand existing templates.
			preview.InitForTest(t)
			got, err := tc.noOptInClient.GetActorTemplate(t.Context(), &ateapi.GetActorTemplateRequest{ActorTemplate: ref})
			if err != nil {
				t.Fatalf("GetActorTemplate with gate disabled: %v", err)
			}
			if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
				t.Fatalf("template changed with gate disabled (-want +got):\n%s", diff)
			}
			if _, err := tc.noOptInClient.DeleteActorTemplate(t.Context(), &ateapi.DeleteActorTemplateRequest{ActorTemplate: ref}); err != nil {
				t.Fatalf("DeleteActorTemplate with gate disabled: %v", err)
			}
		})
	}
}
