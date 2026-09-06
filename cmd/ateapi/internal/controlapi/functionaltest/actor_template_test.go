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
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/controlapi"
	"github.com/agent-substrate/substrate/internal/resources"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestActorTemplateCRUD(t *testing.T) {
	ns := namespaceForTest("ns-template-crud")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()
	ensureDefaultGvisorSandboxConfig(t, tc)

	created, err := tc.client.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata:        &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tmpl-a"},
			Containers:      []*ateapipb.Container{{Name: "main", Image: "example.com/app:v1"}},
			SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: "gs://my-bucket/snapshots"},
			SandboxConfig: &ateapipb.SandboxConfig{
				SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR,
				ConfigName:   "gvisor-default",
			},
			Resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "memory", Quantity: "1Gi"}}},
			// Server-owned status on the request is ignored.
			Status: &ateapipb.ActorTemplateStatus{
				GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
					GoldenTag: &ateapipb.ObjectRef{Atespace: "ate-golden", Name: "golden-tag"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}
	want := &ateapipb.ActorTemplate{
		Metadata:        &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tmpl-a", Version: 1},
		Containers:      []*ateapipb.Container{{Name: "main", Image: "example.com/app:v1"}},
		SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: "gs://my-bucket/snapshots"},
		SandboxConfig: &ateapipb.SandboxConfig{
			SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR,
			ConfigName:   "gvisor-default",
		},
		Resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "memory", Quantity: "1Gi"}}},
		Status:    &ateapipb.ActorTemplateStatus{},
	}
	if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
		t.Errorf("CreateActorTemplate response mismatch (-want +got):\n%s", diff)
	}

	_, err = tc.client.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata:        &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tmpl-a"},
			Containers:      []*ateapipb.Container{{Name: "main", Image: "example.com/app:v1"}},
			SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: "gs://my-bucket/snapshots"},
			SandboxConfig:   &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "gvisor-default"},
		},
	})
	assertGrpcError(t, err, codes.AlreadyExists, "ActorTemplate "+testAtespace+"/tmpl-a already exists")

	got, err := tc.client.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-a"}})
	if err != nil {
		t.Fatalf("GetActorTemplate failed: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetActorTemplate response mismatch (-created +got):\n%s", diff)
	}

	list, err := tc.client.ListActorTemplates(ctx, &ateapipb.ListActorTemplatesRequest{})
	if err != nil {
		t.Fatalf("ListActorTemplates failed: %v", err)
	}
	if len(list.GetActorTemplates()) != 1 || list.GetActorTemplates()[0].GetMetadata().GetName() != "tmpl-a" {
		t.Errorf("ListActorTemplates = %v, want [tmpl-a]", list.GetActorTemplates())
	}

	// The atespace filter scopes the listing: a match returns the template, a
	// different atespace returns nothing.
	list, err = tc.client.ListActorTemplates(ctx, &ateapipb.ListActorTemplatesRequest{Atespace: testAtespace})
	if err != nil {
		t.Fatalf("ListActorTemplates(atespace) failed: %v", err)
	}
	if len(list.GetActorTemplates()) != 1 {
		t.Errorf("ListActorTemplates(atespace=%s) = %v, want [tmpl-a]", testAtespace, list.GetActorTemplates())
	}
	list, err = tc.client.ListActorTemplates(ctx, &ateapipb.ListActorTemplatesRequest{Atespace: "other-atespace"})
	if err != nil {
		t.Fatalf("ListActorTemplates(other atespace) failed: %v", err)
	}
	if len(list.GetActorTemplates()) != 0 {
		t.Errorf("ListActorTemplates(atespace=other-atespace) = %v, want []", list.GetActorTemplates())
	}

	deleted, err := tc.client.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-a"}})
	if err != nil {
		t.Fatalf("DeleteActorTemplate failed: %v", err)
	}
	if diff := cmp.Diff(created, deleted, protocmp.Transform()); diff != "" {
		t.Errorf("DeleteActorTemplate response mismatch (-created +deleted):\n%s", diff)
	}
	_, err = tc.client.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-a"}})
	assertGrpcError(t, err, codes.NotFound, "ActorTemplate "+testAtespace+"/tmpl-a not found")
	_, err = tc.client.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-a"}})
	assertGrpcError(t, err, codes.NotFound, "ActorTemplate "+testAtespace+"/tmpl-a not found")

	// config_name is required: a template must name its SandboxConfig.
	_, err = tc.client.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata:        &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tmpl-unnamed-config"},
			Containers:      []*ateapipb.Container{{Name: "main", Image: "example.com/app:v1"}},
			SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: "gs://my-bucket/snapshots"},
			SandboxConfig:   &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR},
		},
	})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, `sandbox_config\.config_name`)
}

func TestGoldenTagLifecycle(t *testing.T) {
	ns := namespaceForTest("golden-tag")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	createWorkerPool(t, tc, ns, "pool1", map[string]string{poolLabelKey: ns})
	tmpl := createTemplateWithSelector(t, tc, "golden-template", &ateapipb.Selector{MatchLabels: map[string]string{poolLabelKey: ns}})
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	templateRef := resources.ActorTemplateRefFromActorTemplate(tmpl)
	// Created before readiness: a later golden tag must not change its source.
	early, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "early"}, ActorTemplate: templateRef.ToObjectRef(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	controlapi.NewActorTemplateReconciler(tc.persistence, tc.service).Start(ctx)
	var goldenRef *ateapipb.ObjectRef
	err = wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		current, err := tc.client.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: templateRef.ToObjectRef()})
		goldenRef = current.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag()
		return goldenRef != nil, err
	})
	if err != nil {
		t.Fatalf("waiting for golden tag: %v", err)
	}
	golden, err := tc.client.GetTag(ctx, &ateapipb.GetTagRequest{Tag: goldenRef})
	if err != nil {
		t.Fatal(err)
	}
	uri := golden.GetStatus().GetSnapshot().GetSnapshotUri()
	parsed, err := resources.ParseSnapshotURI(uri)
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.OwnedBy(resources.TagSnapshotOwner(resources.GoldenActorAtespace, parsed.Name())) {
		t.Fatal("golden snapshot is not tag-owned")
	}
	assertSnapshotPresent(t, tc, uri)
	_, err = tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: goldenRef})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("golden actor was not deleted: %v", err)
	}
	late, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "late"}, ActorTemplate: templateRef.ToObjectRef(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if late.GetStatus().GetExternalSnapshot().GetSnapshotUri() != uri || late.GetStatus().GetCurrentActorTemplateUid() != tmpl.GetMetadata().GetUid() {
		t.Fatal("actor did not inherit golden tag snapshot and template UID")
	}
	waitForWorkerAvailable(t, tc, workerName)
	tc.fakeAtelet.Lock.Lock()
	tc.fakeAtelet.RunCalled = false
	tc.fakeAtelet.Lock.Unlock()
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: resources.ActorRefFromActor(early).ToObjectRef()}); err != nil {
		t.Fatal(err)
	}
	if !tc.fakeAtelet.RunCalled {
		t.Fatal("actor created before golden readiness did not cold boot")
	}
	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: resources.ActorRefFromActor(early).ToObjectRef()}); err != nil {
		t.Fatal(err)
	}
	waitForWorkerAvailable(t, tc, workerName)
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: resources.ActorRefFromActor(late).ToObjectRef()}); err != nil {
		t.Fatal(err)
	}
	if !tc.fakeAtelet.RestoreCalled {
		t.Fatal("actor did not restore golden tag")
	}
	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: resources.ActorRefFromActor(late).ToObjectRef()}); err != nil {
		t.Fatal(err)
	}
	assertSnapshotPresent(t, tc, uri)
	for _, actor := range []*ateapipb.Actor{early, late} {
		if _, err := tc.client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: resources.ActorRefFromActor(actor).ToObjectRef(), AnyState: true}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tc.client.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: templateRef.ToObjectRef()}); err != nil {
		t.Fatal(err)
	}
	assertSnapshotCollected(t, tc, uri)
	_, err = tc.client.GetTag(ctx, &ateapipb.GetTagRequest{Tag: goldenRef})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("golden tag was not deleted: %v", err)
	}
}
