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
					GoldenSnapshot: &ateapipb.ObjectRef{Atespace: "ate-golden", Name: "sneaky"},
				},
				SandboxAssets: &ateapipb.SandboxAssets{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR},
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
}
