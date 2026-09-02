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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"k8s.io/apimachinery/pkg/util/wait"
)

// TestCreateActor_Success tests the happy path for creating an actor.
// Workflow:
// 1. Creates a mock ActorTemplate in the test atespace.
// 2. Calls CreateActor RPC.
// 3. Verifies that the actor is successfully created and returned in the response with a generated ID.
func TestCreateActor_Success(t *testing.T) {
	ns := namespaceForTest("ns-create-success")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	createResp, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: testAtespace,
			Name:     "id1",
		},
		ActorTemplate:  &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "free"}},
		Status:         &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	want := &ateapipb.Actor{
		Metadata:       &ateapipb.ResourceMetadata{Name: "id1", Atespace: testAtespace, Version: 1},
		ActorTemplate:  &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		Status:         &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
		WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "free"}},
	}

	// The diff below ignores the server-assigned uid/timestamps (non-deterministic),
	// so assert they are populated separately — and that uid is server-generated,
	// not the caller-supplied value.
	md := createResp.GetMetadata()
	if md.GetUid() == "" {
		t.Errorf("CreateActor response missing server-assigned uid")
	}
	if md.GetUid() == "caller-supplied-uid" {
		t.Errorf("CreateActor echoed caller-supplied uid instead of generating one")
	}
	if md.GetCreateTime() == nil {
		t.Errorf("CreateActor response missing create_time")
	}
	if md.GetUpdateTime() == nil {
		t.Errorf("CreateActor response missing update_time")
	}

	if diff := cmp.Diff(want, createResp, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
		t.Errorf("CreateActor response mismatch (-want +got):\n%s", diff)
	}
}

func TestCreateActor_WithExternalVolumes(t *testing.T) {
	ns := namespaceForTest("ns-create-ext-vols")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	volumes := []*ateapipb.Volume{
		{
			Name: "ext-vol-1",
			ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
				StorageClassName: "standard",
				Capacity:         "10Gi",
			},
		},
	}
	mounts := []*ateapipb.VolumeMount{
		{
			Name:      "ext-vol-1",
			MountPath: "/data",
		},
	}
	createTemplateWithVolumes(t, tc, ns, volumes, mounts)

	createResp, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "vol-actor-1"},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		},
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	if len(createResp.GetStatus().GetActorVolumes()) != 1 {
		t.Fatalf("expected 1 volume in CreateActor response, got %d", len(createResp.GetStatus().GetActorVolumes()))
	}
	vol := createResp.GetStatus().GetActorVolumes()[0]
	if vol.GetVolumeName() != "ext-vol-1" {
		t.Errorf("volume name = %q, want %q", vol.GetVolumeName(), "ext-vol-1")
	}
	if vol.GetStatus() != ateapipb.ExternalVolume_STATUS_PENDING {
		t.Errorf("volume status = %v, want %v", vol.GetStatus(), ateapipb.ExternalVolume_STATUS_PENDING)
	}
	if vol.GetStorageVolumeId() != "" {
		t.Errorf("expected empty storageVolumeId before resume, got %q", vol.GetStorageVolumeId())
	}

	// Verify GetActor returns the same external volume state
	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "vol-actor-1"},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if len(getResp.GetStatus().GetActorVolumes()) != 1 {
		t.Fatalf("expected 1 volume in GetActor response, got %d", len(getResp.GetStatus().GetActorVolumes()))
	}
	if getResp.GetStatus().GetActorVolumes()[0].GetStatus() != ateapipb.ExternalVolume_STATUS_PENDING {
		t.Errorf("GetActor status = %v, want %v", getResp.GetStatus().GetActorVolumes()[0].GetStatus(), ateapipb.ExternalVolume_STATUS_PENDING)
	}
}

// TestCreateActor_TemplateNotFound tests that creating an actor with a non-existent template fails with FailedPrecondition.
func TestCreateActor_TemplateNotFound(t *testing.T) {
	ns := namespaceForTest("ns-create-notfound")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "non-existent"},
	}})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("CreateActor with a missing template = %v, want FailedPrecondition (err: %v)", got, err)
	}
}

// TestCreateActor_SubstrateTemplateRef covers creation against a template
// with no golden snapshot yet: the actor names its template with an
// actor_template ObjectRef resolved from the store at create time.
func TestCreateActor_SubstrateTemplateRef(t *testing.T) {
	ns := namespaceForTest("ns-create-sub-ref")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	ctx := context.Background()
	if _, err := tc.client.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata:        &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "sub-tmpl"},
			Containers:      []*ateapipb.Container{{Name: "main", Image: "example.com/app:v1"}},
			SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: "gs://my-bucket/snapshots"},
			SandboxConfig:   &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "gvisor-default"},
		},
	}); err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}

	created, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "ref-actor"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "sub-tmpl"},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	want := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "ref-actor", Version: 1},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "sub-tmpl"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	}
	if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
		t.Errorf("CreateActor response mismatch (-want +got):\n%s", diff)
	}

	// A reference to a template that does not exist fails.
	_, err = tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "ref-actor-2"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "absent"},
	}})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("CreateActor with an absent template ref = %v, want FailedPrecondition (err: %v)", got, err)
	}
}

// TestCreateActor_Duplicate tests that creating an actor with an existing ID fails.
func TestCreateActor_Duplicate(t *testing.T) {
	ns := namespaceForTest("ns-create-dup")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	if err != nil {
		t.Fatalf("first CreateActor failed: %v", err)
	}

	_, err = tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	assertGrpcError(t, err, codes.AlreadyExists, "Actor id1 already exists")
}

// CreateActor is the only lifecycle op with the full identity (incl. version)
// available in the request, so the whole ate.* set should land on its span.
func TestCreateActor_StampsFullSpanIdentity(t *testing.T) {
	ns := namespaceForTest("ns-span-create")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)

	attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
		if _, err := tc.service.CreateActor(ctx, &ateapipb.CreateActorRequest{
			Actor: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
			},
		}); err != nil {
			t.Fatalf("CreateActor: %v", err)
		}
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, testAtespace)
	assertSpanStr(t, attrs, ateattr.ActorNameKey, testActorID)
	assertSpanStr(t, attrs, ateattr.TemplateNameKey, "tmpl1")
	assertSpanStr(t, attrs, ateattr.TemplateAtespaceKey, testAtespace)
	// uid is server-assigned on create, so assert it is present and non-empty
	// rather than a fixed value.
	if v, ok := attrs[ateattr.ActorUIDKey]; !ok || v.Type() != attribute.STRING || v.AsString() == "" {
		t.Errorf("%s = %v, want non-empty server-assigned uid", ateattr.ActorUIDKey, v.String())
	}
	if v, ok := attrs[ateattr.ActorVersionKey]; !ok || v.Type() != attribute.INT64 || v.AsInt64() != 1 {
		t.Errorf("%s = %v, want int64 1", ateattr.ActorVersionKey, v.String())
	}
}

func TestCreateActor_RejectsDifferentTemplateForDataSnapshot(t *testing.T) {
	ns := namespaceForTest("ns-data-snapshot-template")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	tmpl := createTemplate(t, tc, ns)
	createTemplateWithSelector(t, tc, "tmpl2", nil)

	snapshot := storetest.MustCreateActorSnapshot(t, context.Background(), tc.persistence, &ateapipb.ActorSnapshot{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "data-snapshot"},
		Status: &ateapipb.ActorSnapshotStatus{
			SourceActor:      &ateapipb.ObjectRef{Atespace: testAtespace, Name: "source"},
			ActorTemplateUid: tmpl.GetMetadata().GetUid(),
			ContentScope:     ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			SnapshotUri:      "gs://snapshots/snapshots/" + testAtespace + "/data-snapshot",
		},
	})
	if _, err := tc.persistence.CreateActorSnapshotTag(context.Background(), resources.ActorSnapshotRef{Atespace: testAtespace, Name: snapshot.GetMetadata().GetName()}, &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "data-snapshot"},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	}); err != nil {
		t.Fatalf("CreateActorSnapshotTag: %v", err)
	}

	_, err := tc.service.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:          &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "clone"},
			ActorTemplate:     &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl2"},
			SourceSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "data-snapshot"},
		},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CreateActor status = %v, want FailedPrecondition", status.Code(err))
	}
}

func TestCreateActor_RejectsSnapshotWithExternalVolumes(t *testing.T) {
	ns := namespaceForTest("ns-snapshot-external-volume")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	template, err := tc.client.CreateActorTemplate(context.Background(), &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata:        &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tmpl1"},
			SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: "gs://snapshots"},
			SandboxConfig: &ateapipb.SandboxConfig{
				SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR,
				ConfigName:   "gvisor-default",
			},
			Containers: []*ateapipb.Container{{
				Name: "main", Image: "main@sha256:abc", VolumeMounts: []*ateapipb.VolumeMount{{Name: "data", MountPath: "/data"}},
			}},
			Volumes: []*ateapipb.Volume{{
				Name: "data",
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					Capacity: "1Gi", StorageClassName: "standard",
				},
			}},
		},
	})
	if err != nil {
		t.Fatalf("Create ActorTemplate: %v", err)
	}
	snapshot := storetest.MustCreateActorSnapshot(t, context.Background(), tc.persistence, &ateapipb.ActorSnapshot{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "external-volume-snapshot"},
		Status: &ateapipb.ActorSnapshotStatus{
			ActorTemplateUid: template.GetMetadata().GetUid(),
			SnapshotUri:      "gs://snapshots/snapshots/" + testAtespace + "/external-volume-snapshot",
		},
	})
	tagRef := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "external-volume-snapshot"}
	if _, err := tc.persistence.CreateActorSnapshotTag(context.Background(), resources.ActorSnapshotRef{Atespace: testAtespace, Name: snapshot.GetMetadata().GetName()}, &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: tagRef.GetAtespace(), Name: tagRef.GetName()},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	}); err != nil {
		t.Fatalf("CreateActorSnapshotTag: %v", err)
	}

	_, err = tc.service.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:          &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "clone"},
			ActorTemplate:     &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
			SourceSnapshotTag: tagRef,
		},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CreateActor status = %v, want FailedPrecondition", status.Code(err))
	}
}

// TestGetActor_Found tests that an existing actor can be retrieved.
func TestGetActor_Found(t *testing.T) {
	ns := namespaceForTest("ns-get-found")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	name := "id1"

	createResp, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}

	want := createResp

	if diff := cmp.Diff(want, getResp, protocmp.Transform()); diff != "" {
		t.Errorf("GetActor response mismatch (-want +got):\n%s", diff)
	}
}

// TestGetActor_NotFound tests that retrieving a non-existent actor fails.
// Workflow:
// 1. Calls GetActor RPC with a non-existent ID.
// 2. Verifies that it returns an error (NotFound).
func TestGetActor_NotFound(t *testing.T) {
	ns := namespaceForTest("ns-get-notfound")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "non-existent"},
	})
	assertGrpcError(t, err, codes.NotFound, "Actor test-atespace/non-existent not found")
}

// TestListActors tests that all created actors can be listed.
// Workflow:
// 1. Creates a mock ActorTemplate.
// 2. Calls CreateActor twice to create two actors.
// 3. Calls ListActors RPC.
// 4. Verifies that both actors are returned in the list.
func TestListActors(t *testing.T) {
	ns := namespaceForTest("ns-list-actors")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	resp1, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	if err != nil {
		t.Fatalf("CreateActor 1 failed: %v", err)
	}
	resp2, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id2"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	if err != nil {
		t.Fatalf("CreateActor 2 failed: %v", err)
	}

	listResp, err := tc.client.ListActors(context.Background(), &ateapipb.ListActorsRequest{Atespace: testAtespace})
	if err != nil {
		t.Fatalf("ListActors failed: %v", err)
	}

	if len(listResp.Actors) != 2 {
		t.Fatalf("expected 2 actors, got %d", len(listResp.Actors))
	}

	want := []*ateapipb.Actor{
		resp1,
		resp2,
	}

	opts := []cmp.Option{
		protocmp.Transform(),
		cmpopts.SortSlices(func(a, b *ateapipb.Actor) bool {
			return a.GetMetadata().GetName() < b.GetMetadata().GetName()
		}),
	}

	if diff := cmp.Diff(want, listResp.Actors, opts...); diff != "" {
		t.Errorf("ListActors response mismatch (-want +got):\n%s", diff)
	}
}

// TestListActors_ByAtespace verifies create + list are scoped by atespace end to
// end through the RPC surface: an actor created with a given atespace is only
// returned by ListActors(atespace=X) and only fetched by GetActor(atespace=X).
func TestListActors_ByAtespace(t *testing.T) {
	ns := namespaceForTest("ns-list-by-atespace")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)
	createAtespace(t, tc, "team-a")
	createAtespace(t, tc, "team-b")

	create := func(atespace, name string) *ateapipb.Actor {
		resp, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		}})
		if err != nil {
			t.Fatalf("CreateActor(%s, atespace=%q) failed: %v", name, atespace, err)
		}
		return resp
	}
	a1 := create("team-a", "id1")
	a2 := create("team-a", "id2")
	b1 := create("team-b", "id3")

	sortByID := []cmp.Option{
		protocmp.Transform(),
		cmpopts.SortSlices(func(a, b *ateapipb.Actor) bool { return a.GetMetadata().GetName() < b.GetMetadata().GetName() }),
	}

	// List scoped to team-a returns only its actors.
	listA, err := tc.client.ListActors(context.Background(), &ateapipb.ListActorsRequest{Atespace: "team-a"})
	if err != nil {
		t.Fatalf("ListActors(team-a) failed: %v", err)
	}
	if diff := cmp.Diff([]*ateapipb.Actor{a1, a2}, listA.GetActors(), sortByID...); diff != "" {
		t.Errorf("ListActors(team-a) mismatch (-want +got):\n%s", diff)
	}

	// List scoped to team-b returns only its actor.
	listB, err := tc.client.ListActors(context.Background(), &ateapipb.ListActorsRequest{Atespace: "team-b"})
	if err != nil {
		t.Fatalf("ListActors(team-b) failed: %v", err)
	}
	if diff := cmp.Diff([]*ateapipb.Actor{b1}, listB.GetActors(), sortByID...); diff != "" {
		t.Errorf("ListActors(team-b) mismatch (-want +got):\n%s", diff)
	}

	// Get is scoped: the right atespace hits, the empty atespace misses (deny-across by key).
	if _, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "team-a", Name: "id1"}}); err != nil {
		t.Errorf("GetActor(id1, team-a) failed: %v", err)
	}
	_, err = tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"}})
	assertGrpcError(t, err, codes.NotFound, "Actor test-atespace/id1 not found")
}

// TestListActors_AllAtespaces verifies that an empty atespace lists actors across
// all atespaces (the `-A` / admin view), unlike the scoped single-atespace listing.
func TestListActors_AllAtespaces(t *testing.T) {
	ns := namespaceForTest("ns-list-all-atespaces")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)
	createAtespace(t, tc, "team-a")
	createAtespace(t, tc, "team-b")

	create := func(atespace, name string) {
		if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		}}); err != nil {
			t.Fatalf("CreateActor(%s, atespace=%q) failed: %v", name, atespace, err)
		}
	}
	create("team-a", "id1")
	create("team-b", "id2")

	// Empty atespace lists across all atespaces; returned actors carry their atespace.
	resp, err := tc.client.ListActors(context.Background(), &ateapipb.ListActorsRequest{})
	if err != nil {
		t.Fatalf("ListActors(all) failed: %v", err)
	}
	got := map[string]string{}
	for _, a := range resp.GetActors() {
		got[a.GetMetadata().GetName()] = a.GetMetadata().GetAtespace()
	}
	if got["id1"] != "team-a" {
		t.Errorf("ListActors(all): got[id1]=%q, want team-a", got["id1"])
	}
	if got["id2"] != "team-b" {
		t.Errorf("ListActors(all): got[id2]=%q, want team-b", got["id2"])
	}
}

// TestListActors_Pagination tests that ListActors correctly paginates results.
func TestListActors_Pagination(t *testing.T) {
	ns := namespaceForTest("ns-list-actors-pagination")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	var want []*ateapipb.Actor
	for i := 0; i < 5; i++ {
		resp, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: fmt.Sprintf("name%d", i)},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		}})
		if err != nil {
			t.Fatalf("CreateActor %d failed: %v", i, err)
		}
		want = append(want, resp)
	}

	var allActors []*ateapipb.Actor
	pageToken := ""

	for {
		listResp, err := tc.client.ListActors(context.Background(), &ateapipb.ListActorsRequest{
			Atespace:  testAtespace,
			PageSize:  2,
			PageToken: pageToken,
		})
		if err != nil {
			t.Fatalf("ListActors failed: %v", err)
		}

		allActors = append(allActors, listResp.Actors...)
		pageToken = listResp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}

	if len(allActors) != 5 {
		t.Fatalf("expected 5 actors total, got %d", len(allActors))
	}

	opts := []cmp.Option{
		protocmp.Transform(),
		cmpopts.SortSlices(func(a, b *ateapipb.Actor) bool {
			return a.GetMetadata().GetName() < b.GetMetadata().GetName()
		}),
	}

	if diff := cmp.Diff(want, allActors, opts...); diff != "" {
		t.Errorf("ListActors pagination response mismatch (-want +got):\n%s", diff)
	}
}

// TestUpdateActor_Success verifies UpdateActor replaces the actor's
// worker_selector and that the change is durably persisted.
func TestUpdateActor_Success(t *testing.T) {
	ns := namespaceForTest("ns-update-actor")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	toUpdate, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"tier": "free"},
		},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	toUpdate.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}

	updateResp, err := tc.client.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{
		Actor: toUpdate,
	})
	if err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	wantActor := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Name: "id1", Atespace: testAtespace, Version: 2},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"tier": "paid"},
		},
	}
	if diff := cmp.Diff(wantActor, updateResp, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
		t.Errorf("UpdateActor response mismatch (-want +got):\n%s", diff)
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	wantGetResp := wantActor
	if diff := cmp.Diff(wantGetResp, getResp, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
		t.Errorf("GetActor response mismatch after UpdateActor (-want +got):\n%s", diff)
	}
}

// TestUpdateActor verifies a typical RMW UpdateActor flow: a
// client reads an actor, modifies it and send an UpdateActor request.
// Output-only fields it sets are ignored, and the mutable actor_template ref
// can repoint the actor at another template.
func TestUpdateActor(t *testing.T) {
	ns := namespaceForTest("ns-update-replace")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	tmpl := createTemplate(t, tc, ns)

	created, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"tier": "free"},
		},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// Mutable field
	created.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}
	// Output-only: server-owned, so this is ignored rather than applied.
	created.Status = &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING}
	updatedActor, err := tc.client.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{Actor: created})
	if err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	wantActor := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Name: "id1", Atespace: testAtespace, Version: 2},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"tier": "paid"},
		},
	}
	if diff := cmp.Diff(wantActor, updatedActor, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
		t.Errorf("UpdateActor response mismatch (-want +got):\n%s", diff)
	}

	// Mutable template ref: repoint the actor at a second template.
	tmpl2 := proto.Clone(tmpl).(*ateapipb.ActorTemplate)
	tmpl2.Metadata = &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "tmpl2"}
	tmpl2.Status = nil
	if _, err := tc.client.CreateActorTemplate(context.Background(), &ateapipb.CreateActorTemplateRequest{ActorTemplate: tmpl2}); err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}

	updatedActor.ActorTemplate = &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl2"}
	repointed, err := tc.client.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{Actor: updatedActor})
	if err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}
	wantActor.Metadata.Version = 3
	wantActor.ActorTemplate = &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl2"}
	if diff := cmp.Diff(wantActor, repointed, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
		t.Errorf("UpdateActor response mismatch after template repoint (-want +got):\n%s", diff)
	}
}

// TestUpdateActor_Preconditions verifies the required version and uid guards
// carried in the embedded resource's metadata.
func TestUpdateActor_Preconditions(t *testing.T) {
	ns := namespaceForTest("ns-update-preconditions")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	ctx := context.Background()
	createActor := func() *ateapipb.Actor {
		t.Helper()
		actor, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		}})
		if err != nil {
			t.Fatalf("CreateActor failed: %v", err)
		}
		return actor
	}

	update := func(observed *ateapipb.Actor, tier string) (*ateapipb.Actor, error) {
		actor := proto.Clone(observed).(*ateapipb.Actor)
		actor.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": tier}}
		return tc.client.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: actor})
	}
	// Delete and recreate the same atespace/name actor, so the first lifecycle's uid
	// becomes stale.
	staleUID := createActor().GetMetadata().GetUid()
	if _, err := tc.client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: testActorID},
	}); err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}

	created := createActor()
	staleVersion := created.GetMetadata().GetVersion()
	uid := created.GetMetadata().GetUid()
	if uid == staleUID {
		t.Fatalf("recreated actor reused uid %s, want a fresh one", uid)
	}
	// No preconditions
	unguarded := proto.Clone(created).(*ateapipb.Actor)
	unguarded.Metadata.Uid, unguarded.Metadata.Version = "", 0
	_, err := update(unguarded, "blind")
	assertGrpcError(t, err, codes.InvalidArgument, "while updating actor test-atespace/id1: persistence: precondition required: uid")

	// The uid from the deleted lifecycle must be rejected, even though the
	// atespace/name it was observed under still resolves and the version it
	// guards on matches the recreated actor's.
	otherLifecycle := proto.Clone(created).(*ateapipb.Actor)
	otherLifecycle.Metadata.Uid = staleUID
	_, err = update(otherLifecycle, "other-lifecycle")
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")

	// Both guards matching the observed state: the update goes through, and
	// moves the resource past the version observed above.
	first, err := update(created, "free")
	if err != nil {
		t.Fatalf("UpdateActor(matching guards) failed: %v", err)
	}
	currentVersion := first.GetMetadata().GetVersion()
	if currentVersion <= staleVersion {
		t.Fatalf("version = %d, want greater than %d after an update", currentVersion, staleVersion)
	}
	if got := first.GetWorkerSelector().GetMatchLabels()["tier"]; got != "free" {
		t.Errorf("worker_selector[tier] = %q, want free", got)
	}

	// The version observed before that write is now stale: rejected rather than
	// silently overwriting the concurrent change.
	_, err = update(created, "stale")
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")

	// Guarding on the version the last write produced succeeds again.
	updated, err := update(first, "paid")
	if err != nil {
		t.Fatalf("UpdateActor(matching guards) failed: %v", err)
	}
	if got := updated.GetWorkerSelector().GetMatchLabels()["tier"]; got != "paid" {
		t.Errorf("worker_selector[tier] = %q, want paid", got)
	}
	if updated.GetMetadata().GetVersion() <= currentVersion {
		t.Errorf("version = %d, want greater than %d", updated.GetMetadata().GetVersion(), currentVersion)
	}

	// The guard the client just satisfied is now stale in turn.
	_, err = update(first, "free")
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")
}

func TestUpdateActor_NotFound(t *testing.T) {
	ns := namespaceForTest("ns-update-actor-notfound")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{
		Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{
			Atespace: testAtespace,
			Name:     "does-not-exist",
			// Well-formed guards to pass preconditions validation
			Uid:     "9a2b1c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d",
			Version: 1,
		}},
	})
	assertGrpcError(t, err, codes.NotFound, "actor test-atespace/does-not-exist not found")
}

func TestUpdateActor_StampsFullSpanIdentity(t *testing.T) {
	ns := namespaceForTest("ns-span-update")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)

	toUpdate, err := tc.service.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		},
	})
	if err != nil {
		t.Fatalf("seed CreateActor: %v", err)
	}
	toUpdate.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"env": "prod"}}

	attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
		if _, err := tc.service.UpdateActor(ctx, &ateapipb.UpdateActorRequest{
			Actor: toUpdate,
		}); err != nil {
			t.Fatalf("UpdateActor: %v", err)
		}
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, testAtespace)
	assertSpanStr(t, attrs, ateattr.ActorNameKey, testActorID)
	assertSpanStr(t, attrs, ateattr.TemplateNameKey, "tmpl1")
	assertSpanStr(t, attrs, ateattr.TemplateAtespaceKey, testAtespace)
	if v, ok := attrs[ateattr.ActorUIDKey]; !ok || v.Type() != attribute.STRING || v.AsString() == "" {
		t.Errorf("%s = %v, want non-empty server-assigned uid", ateattr.ActorUIDKey, v.String())
	}
	if v, ok := attrs[ateattr.ActorVersionKey]; !ok || v.Type() != attribute.INT64 || v.AsInt64() != 2 {
		t.Errorf("%s = %v, want int64 2 (updated version)", ateattr.ActorVersionKey, v.String())
	}
}

func TestUpdateActor_FailedLookupStampsRefIdentityOnly(t *testing.T) {
	ns := namespaceForTest("ns-span-update-err")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
		if _, err := tc.service.UpdateActor(ctx, &ateapipb.UpdateActorRequest{
			Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{
				Atespace: testAtespace,
				Name:     testActorID,
				// Well-formed guards to pass preconditions validation
				Uid:     "9a2b1c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d",
				Version: 1,
			}},
		}); status.Code(err) != codes.NotFound {
			t.Fatalf("UpdateActor(missing) error = %v, want code NotFound", err)
		}
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, testAtespace)
	assertSpanStr(t, attrs, ateattr.ActorNameKey, testActorID)
	for _, k := range []attribute.Key{ateattr.ActorUIDKey, ateattr.TemplateNameKey, ateattr.TemplateAtespaceKey, ateattr.ActorVersionKey} {
		if _, ok := attrs[k]; ok {
			t.Errorf("unexpected %s on failed-update span", k)
		}
	}
}

func TestDeleteActor_Success(t *testing.T) {
	ns := namespaceForTest("ns-delete-success")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	deleted, err := tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"},
	})
	if err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
	// DeleteActor returns the deleted resource.
	if got := deleted.GetMetadata().GetName(); got != "id1" {
		t.Errorf("deleted actor name = %q, want id1", got)
	}
	if got := deleted.GetMetadata().GetAtespace(); got != testAtespace {
		t.Errorf("deleted actor atespace = %q, want %q", got, testAtespace)
	}

	_, err = tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"},
	})
	assertGrpcError(t, err, codes.NotFound, "Actor test-atespace/id1 not found")
}

func TestDeleteActor_NotSuspended(t *testing.T) {
	ns := namespaceForTest("ns-delete-notsuspended")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	_, err = tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"},
	})
	assertGrpcError(t, err, codes.FailedPrecondition, "Actor test-atespace/id1 is not in a deletable state (state: ACTOR_STATE_RUNNING)")
}

func TestDeleteActor_Crashed(t *testing.T) {
	ns := namespaceForTest("ns-delete-crashed")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	created, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	actorRef := resources.ActorRef{Atespace: testAtespace, Name: "id1"}
	if _, err := tc.persistence.UpdateActor(context.Background(), actorRef, store.PreconditionFrom(created), func(toUpdate *ateapipb.Actor) error {
		toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
		return nil
	}); err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	deleted, err := tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"},
	})
	if err != nil {
		t.Fatalf("DeleteActor of crashed actor failed: %v", err)
	}
	if got := deleted.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_DELETING {
		t.Errorf("deleted actor state = %v, want %v", got, ateapipb.ActorState_ACTOR_STATE_DELETING)
	}

	_, err = tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"},
	})
	assertGrpcError(t, err, codes.NotFound, "Actor test-atespace/id1 not found")
}

func TestDeleteActor_NotFound(t *testing.T) {
	ns := namespaceForTest("ns-delete-notfound")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "non-existent"},
	})
	assertGrpcError(t, err, codes.NotFound, "Actor test-atespace/non-existent not found")
}

// Delete addresses the actor by ref (atespace + id) and does not resolve the
// template/version, so only the ref identity is stamped.
func TestDeleteActor_StampsRefSpanIdentity(t *testing.T) {
	ns := namespaceForTest("ns-span-delete")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)
	if _, err := tc.service.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		},
	}); err != nil {
		t.Fatalf("seed CreateActor: %v", err)
	}

	attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
		if _, err := tc.service.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: testActorID},
		}); err != nil {
			t.Fatalf("DeleteActor: %v", err)
		}
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, testAtespace)
	assertSpanStr(t, attrs, ateattr.ActorNameKey, testActorID)
}

func TestDeleteActor_StateDeleting(t *testing.T) {
	ns := namespaceForTest("ns-delete-deleting")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)

	deletingActor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: testAtespace,
			Name:     "deleting-actor",
		},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}
	if _, err := tc.persistence.CreateActor(context.Background(), deletingActor); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}

	if _, err := tc.service.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "deleting-actor"},
	}); err != nil {
		t.Fatalf("DeleteActor on ACTOR_STATE_DELETING actor failed: %v", err)
	}

	if _, err := tc.persistence.GetActor(context.Background(), resources.ActorRef{Atespace: testAtespace, Name: "deleting-actor"}); err == nil {
		t.Errorf("expected actor to be deleted, but it still exists")
	}
}

func TestDeleteActor_WrongState(t *testing.T) {
	ns := namespaceForTest("ns-delete-wrong-status")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)

	runningActor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: testAtespace,
			Name:     "running-actor",
		},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}
	if _, err := tc.persistence.CreateActor(context.Background(), runningActor); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}

	_, err := tc.service.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "running-actor"},
	})
	if err == nil {
		t.Fatalf("expected DeleteActor on ACTOR_STATE_RUNNING actor to fail, but it succeeded")
	}
}

type failingVolumePlugin struct {
	volume.VolumePluginControlPlane
	deletedIDs []string
}

func (f *failingVolumePlugin) DeleteVolume(ctx context.Context, volumeID string) error {
	f.deletedIDs = append(f.deletedIDs, volumeID)
	return fmt.Errorf("simulated delete error for %s", volumeID)
}

func TestDeleteActor_MultipleVolumeDeletionFailures(t *testing.T) {
	ns := namespaceForTest("ns-delete-multivol-fail")
	plugin := &failingVolumePlugin{}
	tc := setupTestWithVolumePlugins(t, ns, map[string]volume.VolumePluginControlPlane{
		"substrate.io/mock": plugin,
	})
	defer tc.cleanup()
	createTemplate(t, tc, ns)

	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: testAtespace,
			Name:     "multi-vol-actor",
		},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			ActorVolumes: []*ateapipb.ExternalVolume{
				{VolumeName: "vol1", StorageVolumeId: "storage-vol-1", Status: ateapipb.ExternalVolume_STATUS_CREATED, VolumeType: "substrate.io/mock"},
				{VolumeName: "vol2", StorageVolumeId: "storage-vol-2", Status: ateapipb.ExternalVolume_STATUS_CREATED, VolumeType: "substrate.io/mock"},
			},
		},
	}
	if _, err := tc.persistence.CreateActor(context.Background(), actor); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}

	_, err := tc.service.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "multi-vol-actor"},
	})
	if err == nil {
		t.Fatalf("expected DeleteActor to fail when volume deletion fails, but it succeeded")
	}

	wantDeleted := []string{"storage-vol-1", "storage-vol-2"}
	if diff := cmp.Diff(wantDeleted, plugin.deletedIDs); diff != "" {
		t.Errorf("deletedIDs mismatch (-want +got):\n%s", diff)
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "storage-vol-1") || !strings.Contains(errMsg, "storage-vol-2") {
		t.Errorf("expected error message to contain both volume failure details, got: %v", errMsg)
	}
}

func TestCreateActor_AtespaceNotFound(t *testing.T) {
	ns := namespaceForTest("ns-create-actor-no-atespace")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)

	// The template exists, but "missing-as" was never created. The template
	// check fires first, so reaching this error proves the atespace check ran.
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "missing-as", Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	assertGrpcError(t, err, codes.FailedPrecondition, "Atespace missing-as not found")
}

func TestValidation_Actor(t *testing.T) {
	ns := namespaceForTest("ns-validation-actor")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	t.Run("CreateActor", func(t *testing.T) {
		_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "actor: Required value")
	})

	t.Run("GetActor", func(t *testing.T) {
		_, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "actor: Required value")
	})

	t.Run("ResumeActor", func(t *testing.T) {
		_, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "actor: Required value")
	})

	t.Run("PauseActor", func(t *testing.T) {
		_, err := tc.client.PauseActor(context.Background(), &ateapipb.PauseActorRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "actor: Required value")
	})

	t.Run("SuspendActor", func(t *testing.T) {
		_, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "actor: Required value")
	})

	t.Run("UpdateActor", func(t *testing.T) {
		_, err := tc.client.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "actor: Required value")
	})

	t.Run("DeleteActor", func(t *testing.T) {
		_, err := tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "actor: Required value")
	})

	t.Run("ListActors", func(t *testing.T) {
		_, err := tc.client.ListActors(context.Background(), &ateapipb.ListActorsRequest{PageSize: -1})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "page_size: Invalid value")
	})

	t.Run("ListActors invalid token", func(t *testing.T) {
		_, err := tc.client.ListActors(context.Background(), &ateapipb.ListActorsRequest{PageToken: "%%%"})
		assertGrpcError(t, err, codes.InvalidArgument, "invalid page_token")
	})

	t.Run("ListActorSnapshots invalid token", func(t *testing.T) {
		_, err := tc.client.ListActorSnapshots(context.Background(), &ateapipb.ListActorSnapshotsRequest{PageToken: "%%%"})
		assertGrpcError(t, err, codes.InvalidArgument, "invalid page_token")
	})
}

func TestActorLifecycle_WithExternalVolumes(t *testing.T) {
	ns := namespaceForTest("ns-lifecycle-ext-vols")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	volumes := []*ateapipb.Volume{
		{
			Name: "data-vol",
			ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
				StorageClassName: "fast",
				Capacity:         "20Gi",
			},
		},
	}
	mounts := []*ateapipb.VolumeMount{
		{
			Name:      "data-vol",
			MountPath: "/mnt/data",
		},
	}
	createTemplateWithVolumes(t, tc, ns, volumes, mounts)
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	// 1. CreateActor
	createResp, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "actor-vol-lc"},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		},
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if createResp.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("expected initial state ACTOR_STATE_SUSPENDED, got %v", createResp.GetStatus().GetState())
	}
	if len(createResp.GetStatus().GetActorVolumes()) != 1 || createResp.GetStatus().GetActorVolumes()[0].GetStatus() != ateapipb.ExternalVolume_STATUS_PENDING {
		t.Fatalf("expected 1 pending volume after CreateActor, got %v", createResp.GetStatus().GetActorVolumes())
	}

	// 2. ResumeActor
	resumeResp, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor-vol-lc"},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}
	if resumeResp.GetActor().GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("expected state ACTOR_STATE_RUNNING after resume, got %v", resumeResp.GetActor().GetStatus().GetState())
	}
	if len(resumeResp.GetActor().GetStatus().GetActorVolumes()) != 1 || resumeResp.GetActor().GetStatus().GetActorVolumes()[0].GetStatus() != ateapipb.ExternalVolume_STATUS_CREATED {
		t.Fatalf("expected 1 created volume after ResumeActor, got %v", resumeResp.GetActor().GetStatus().GetActorVolumes())
	}
	if resumeResp.GetActor().GetStatus().GetActorVolumes()[0].GetStorageVolumeId() == "" {
		t.Fatalf("expected non-empty storageVolumeId after ResumeActor")
	}

	// 3. PauseActor
	pauseResp, err := tc.client.PauseActor(context.Background(), &ateapipb.PauseActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor-vol-lc"},
	})
	if err != nil {
		t.Fatalf("PauseActor failed: %v", err)
	}
	if pauseResp.GetActor().GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_PAUSED {
		t.Fatalf("expected state ACTOR_STATE_PAUSED after pause, got %v", pauseResp.GetActor().GetStatus().GetState())
	}

	// 4. ResumeActor from paused
	resumeResp2, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor-vol-lc"},
	})
	if err != nil {
		t.Fatalf("ResumeActor from paused failed: %v", err)
	}
	if resumeResp2.GetActor().GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("expected state ACTOR_STATE_RUNNING after second resume, got %v", resumeResp2.GetActor().GetStatus().GetState())
	}

	// 5. SuspendActor
	suspendResp, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor-vol-lc"},
	})
	if err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	if suspendResp.GetActor().GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("expected state ACTOR_STATE_SUSPENDED after suspend, got %v", suspendResp.GetActor().GetStatus().GetState())
	}

	// 6. DeleteActor
	deleteResp, err := tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor-vol-lc"},
	})
	if err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
	if deleteResp.GetMetadata().GetName() != "actor-vol-lc" {
		t.Errorf("deleted actor name = %q, want %q", deleteResp.GetMetadata().GetName(), "actor-vol-lc")
	}

	// Confirm GetActor returns NotFound after deletion
	_, err = tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor-vol-lc"},
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetActor after delete err = %v, want NotFound", err)
	}
}

type partialFailVolumePlugin struct {
	volume.VolumePluginControlPlane
	deleted []string
}

func (f *partialFailVolumePlugin) CreateVolume(ctx context.Context, name, capacity, driverName string, parameters map[string]string) (string, map[string]string, error) {
	if strings.HasSuffix(name, "fail-vol2") {
		return "", nil, fmt.Errorf("simulated volume creation failure")
	}
	return "storage-" + name, parameters, nil
}

func (f *partialFailVolumePlugin) AttachVolume(ctx context.Context, volumeID, node string) error {
	return nil
}

func (f *partialFailVolumePlugin) DetachVolume(ctx context.Context, volumeID, node string) error {
	return nil
}

func (f *partialFailVolumePlugin) DeleteVolume(ctx context.Context, volumeID string) error {
	f.deleted = append(f.deleted, volumeID)
	return nil
}

// TestResumeActor_VolumeCreationFailure tests that when volume provisioning fails during ResumeActor,
// successfully created volumes are saved, the actor remains in ACTOR_STATE_SUSPENDED,
// and that calling DeleteActor on the suspended actor cleans up all partially created volumes.
func TestResumeActor_VolumeCreationFailure(t *testing.T) {
	ns := namespaceForTest("ns-resume-vol-fail")
	plugin := &partialFailVolumePlugin{}
	tc := setupTestWithVolumePlugins(t, ns, map[string]volume.VolumePluginControlPlane{
		"substrate.io/mock": plugin,
	})
	defer tc.cleanup()

	volumes := []*ateapipb.Volume{
		{
			Name: "succ-vol1",
			ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
				StorageClassName: "standard",
				Capacity:         "10Gi",
			},
		},
		{
			Name: "fail-vol2",
			ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
				StorageClassName: "standard",
				Capacity:         "10Gi",
			},
		},
	}
	mounts := []*ateapipb.VolumeMount{
		{Name: "succ-vol1", MountPath: "/mnt/vol1"},
		{Name: "fail-vol2", MountPath: "/mnt/vol2"},
	}
	createTemplateWithVolumes(t, tc, ns, volumes, mounts)

	// Call CreateActor RPC directly
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "fail-actor"},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		},
	})
	if err != nil {
		t.Fatalf("expected CreateActor to succeed, got: %v", err)
	}

	// Call ResumeActor RPC, which should trigger volume provisioning and fail on fail-vol2
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "fail-actor"},
	})
	if err == nil {
		t.Fatalf("expected ResumeActor to fail due to volume creation error, but it succeeded")
	}

	// Verify GetActor returns the actor in ACTOR_STATE_SUSPENDED state
	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "fail-actor"},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if getResp.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("actor state = %v, want %v", getResp.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	}

	actorUID := getResp.GetMetadata().GetUid()
	if actorUID == "" {
		t.Fatalf("expected non-empty UID on actor")
	}

	// Verify that succ-vol1 was updated to CREATED with a storageVolumeId, and fail-vol2 is still PENDING
	if len(getResp.GetStatus().GetActorVolumes()) != 2 {
		t.Fatalf("expected 2 volumes on actor, got %d", len(getResp.GetStatus().GetActorVolumes()))
	}
	volsByName := make(map[string]*ateapipb.ExternalVolume)
	for _, v := range getResp.GetStatus().GetActorVolumes() {
		volsByName[v.GetVolumeName()] = v
	}
	if v1, ok := volsByName["succ-vol1"]; !ok || v1.GetStatus() != ateapipb.ExternalVolume_STATUS_CREATED || v1.GetStorageVolumeId() == "" {
		t.Errorf("succ-vol1 unexpected state: %v", v1)
	}
	if v2, ok := volsByName["fail-vol2"]; !ok || v2.GetStatus() != ateapipb.ExternalVolume_STATUS_PENDING {
		t.Errorf("fail-vol2 unexpected state: %v", v2)
	}

	// Call DeleteActor on the actor in ACTOR_STATE_SUSPENDED
	_, err = tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "fail-actor"},
	})
	if err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}

	// Verify both volumes were deleted (succ-vol1 via storageID, fail-vol2 via fallback actorVolumeID)
	wantDeleted := []string{
		"storage-substrate-" + actorUID + "-succ-vol1",
		"substrate-" + actorUID + "-fail-vol2",
	}
	if diff := cmp.Diff(wantDeleted, plugin.deleted); diff != "" {
		t.Errorf("deleted volume IDs mismatch (-want +got):\n%s", diff)
	}

	// Confirm GetActor returns NotFound after deletion
	_, err = tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "fail-actor"},
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetActor after DeleteActor err = %v, want NotFound", err)
	}
}

type retrySuccessVolumePlugin struct {
	volume.VolumePluginControlPlane
	mu       sync.Mutex
	attempts int
	deleted  []string
}

func (r *retrySuccessVolumePlugin) CreateVolume(ctx context.Context, name, capacity, driverName string, parameters map[string]string) (string, map[string]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if strings.HasSuffix(name, "retry-vol2") {
		r.attempts++
		if r.attempts == 1 {
			return "", nil, fmt.Errorf("simulated temporary volume creation failure")
		}
	}
	return "storage-" + name, parameters, nil
}

func (r *retrySuccessVolumePlugin) AttachVolume(ctx context.Context, volumeID, node string) error {
	return nil
}

func (r *retrySuccessVolumePlugin) DetachVolume(ctx context.Context, volumeID, node string) error {
	return nil
}

func (r *retrySuccessVolumePlugin) DeleteVolume(ctx context.Context, volumeID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleted = append(r.deleted, volumeID)
	return nil
}

// TestResumeActor_VolumeCreationRetrySuccess tests that when volume provisioning fails on the first ResumeActor call,
// a subsequent call to ResumeActor retries provisioning only the pending volumes and succeeds.
func TestResumeActor_VolumeCreationRetrySuccess(t *testing.T) {
	ns := namespaceForTest("ns-resume-vol-retry")
	plugin := &retrySuccessVolumePlugin{}
	tc := setupTestWithVolumePlugins(t, ns, map[string]volume.VolumePluginControlPlane{
		"substrate.io/mock": plugin,
	})
	defer tc.cleanup()

	volumes := []*ateapipb.Volume{
		{
			Name: "succ-vol1",
			ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
				StorageClassName: "standard",
				Capacity:         "10Gi",
			},
		},
		{
			Name: "retry-vol2",
			ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
				StorageClassName: "standard",
				Capacity:         "10Gi",
			},
		},
	}
	retryMounts := []*ateapipb.VolumeMount{
		{Name: "succ-vol1", MountPath: "/mnt/vol1"},
		{Name: "retry-vol2", MountPath: "/mnt/vol2"},
	}
	createTemplateWithVolumes(t, tc, ns, volumes, retryMounts)
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	// Call CreateActor RPC directly
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "retry-actor"},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		},
	})
	if err != nil {
		t.Fatalf("expected CreateActor to succeed, got: %v", err)
	}

	// First call to ResumeActor RPC, which should fail on retry-vol2 (attempt 1)
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "retry-actor"},
	})
	if err == nil {
		t.Fatalf("expected first ResumeActor to fail due to temporary volume creation error, but it succeeded")
	}

	// Verify GetActor returns the actor in ACTOR_STATE_SUSPENDED state with succ-vol1 created and retry-vol2 pending
	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "retry-actor"},
	})
	if err != nil {
		t.Fatalf("GetActor after first resume failed: %v", err)
	}
	if getResp.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("actor state after first resume = %v, want %v", getResp.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	}

	volsByName := make(map[string]*ateapipb.ExternalVolume)
	for _, v := range getResp.GetStatus().GetActorVolumes() {
		volsByName[v.GetVolumeName()] = v
	}
	if v1, ok := volsByName["succ-vol1"]; !ok || v1.GetStatus() != ateapipb.ExternalVolume_STATUS_CREATED || v1.GetStorageVolumeId() == "" {
		t.Errorf("succ-vol1 unexpected state after first resume: %v", v1)
	}
	if v2, ok := volsByName["retry-vol2"]; !ok || v2.GetStatus() != ateapipb.ExternalVolume_STATUS_PENDING {
		t.Errorf("retry-vol2 unexpected state after first resume: %v", v2)
	}

	// Second call to ResumeActor RPC, which should succeed on retry-vol2 (attempt 2)
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "retry-actor"},
	})
	if err != nil {
		t.Fatalf("expected second ResumeActor to succeed, got: %v", err)
	}

	// Verify GetActor returns the actor in ACTOR_STATE_RUNNING state with both volumes CREATED
	getResp, err = tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "retry-actor"},
	})
	if err != nil {
		t.Fatalf("GetActor after second resume failed: %v", err)
	}
	if getResp.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state after second resume = %v, want %v", getResp.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_RUNNING)
	}
	for _, v := range getResp.GetStatus().GetActorVolumes() {
		if v.GetStatus() != ateapipb.ExternalVolume_STATUS_CREATED || v.GetStorageVolumeId() == "" {
			t.Errorf("volume %s unexpected state after second resume: %v", v.GetVolumeName(), v)
		}
	}

	// Clean up by suspending and deleting the actor
	_, err = tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "retry-actor"},
	})
	if err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	_, err = tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "retry-actor"},
	})
	if err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
}

// TestResumeActor tests the full workflow of resuming a suspended actor.
// Workflow:
// 1. Creates a mock ActorTemplate.
// 2. Creates a mock Atelet Pod in 'ate-system' namespace on 'node1'.
// 3. Creates a mock worker Pod in the test namespace on 'node1'.
// 4. Waits for the WorkerPoolSyncer to mirror the worker to the store.
// 5. Creates an actor (starts as SUSPENDED).
// 6. Calls ResumeActor RPC.
// 7. Verifies that the fake Atelet received the Restore call.
// 8. Verifies that the actor state is updated to RUNNING.
func TestResumeActor(t *testing.T) {
	ns := namespaceForTest("ns-resume")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	podUID := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	if !tc.fakeAtelet.RestoreCalled {
		t.Errorf("expected Restore to be called")
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	want := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Name: name, Atespace: testAtespace},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:          &ateapipb.ObjectRef{Name: podUID},
				WorkerNamespace: ns,
				WorkerPool:      "pool1",
				WorkerPod:       "worker-1",
				WorkerPodUid:    podUID,
				WorkerPodIp:     "127.0.0.1",
			},
		},
	}
	if diff := cmp.Diff(want, getResp, protocmp.Transform(), ignoreUID, ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("GetActor response mismatch (-want +got):\n%s", diff)
	}

	// Verify that the worker record also has the assigned actor details
	listWorkersResp, err := tc.client.ListWorkers(context.Background(), &ateapipb.ListWorkersRequest{})
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}
	var actorWorker *ateapipb.Worker
	for _, w := range listWorkersResp.GetWorkers() {
		if w.GetWorkerNamespace() == ns && w.GetWorkerPod() == "worker-1" {
			actorWorker = w
			break
		}
	}
	if actorWorker == nil {
		t.Fatalf("expected worker-1 in namespace %s not found in ListWorkers", ns)
	}

	wantWorker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: podUID},
		WorkerNamespace: ns,
		WorkerPool:      "pool1",
		WorkerPod:       "worker-1",
		WorkerPodUid:    podUID,
		Ip:              "127.0.0.1",
		NodeName:        "node1",
		SandboxClass:    "gvisor",
		Labels:          map[string]string{poolLabelKey: ns},
		Status: &ateapipb.WorkerStatus{
			Assignment: &ateapipb.ActorAssignment{
				ActorTemplateRef: &ateapipb.ObjectRef{
					Atespace: testAtespace,
					Name:     "tmpl1",
				},
				Actor: &ateapipb.ObjectRef{
					Name:     name,
					Atespace: testAtespace,
				},
				ActorUid: getResp.GetMetadata().GetUid(),
			},
			State: ateapipb.WorkerState_WORKER_STATE_ACTIVE,
		},
	}

	if diff := cmp.Diff(wantWorker, actorWorker, protocmp.Transform(), ignoreServerMetadata,
		protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "version")); diff != "" {
		t.Errorf("Worker state mismatch (-want +got):\n%s", diff)
	}
}

func TestResumeActorPassesLiteralEnv(t *testing.T) {
	ns := namespaceForTest("ns-resume-literal-env")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplateWithContainers(t, tc, ns, []*ateapipb.Container{
		{
			Name:    "main",
			Image:   "main@sha256:abc",
			Command: []string{"/main"},
			Env: []*ateapipb.EnvVar{
				{
					Name:  "LITERAL",
					Value: "plain",
				},
			},
		},
	})
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	restoreReq := tc.fakeAtelet.lastRestoreRequest()
	if restoreReq == nil {
		t.Fatalf("expected Restore to be called")
	}
	if len(restoreReq.GetSpec().GetContainers()) != 1 {
		t.Fatalf("expected one container in restore request, got %d", len(restoreReq.GetSpec().GetContainers()))
	}
	gotEnv := map[string]string{}
	for _, env := range restoreReq.GetSpec().GetContainers()[0].GetEnv() {
		gotEnv[env.GetName()] = env.GetValue()
	}
	wantEnv := map[string]string{
		"LITERAL": "plain",
	}
	if diff := cmp.Diff(wantEnv, gotEnv); diff != "" {
		t.Errorf("env mismatch (-want +got):\n%s", diff)
	}
}

// TestResumeActor_NoWorkers tests that resuming an actor fails when no free workers are available.
// Workflow:
// 1. Creates a mock ActorTemplate.
// 2. Creates an actor.
// 3. Calls ResumeActor RPC without creating any workers.
// 4. Verifies that ResumeActor fails with FailedPrecondition status.
// TestResumeActor_NoWorkers tests that resuming an actor fails when no free workers are available.
// Workflow:
// 1. Creates a mock ActorTemplate.
// 2. Creates an actor.
// 3. Calls ResumeActor RPC without creating any workers.
// 4. Verifies that ResumeActor fails with FailedPrecondition status.
func TestResumeActor_NoWorkers(t *testing.T) {
	ns := namespaceForTest("ns-resume-no-workers")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	createResp, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	name := createResp.GetMetadata().GetName()

	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	assertGrpcError(t, err, codes.ResourceExhausted, "no free workers available")
}

// TestResumeActor_MultiPoolSelector exercises the AND-of-two-selectors path
// end to end: a template's WorkerSelector gates two pools, and the actor's
// worker_selector narrows to just one of them.
func TestResumeActor_MultiPoolSelector(t *testing.T) {
	ns := namespaceForTest("ns-multi-pool")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createWorkerPool(t, tc, ns, "pool-a", map[string]string{"group": ns, "tier": "a"})
	createWorkerPool(t, tc, ns, "pool-b", map[string]string{"group": ns, "tier": "b"})
	createTemplateWithSelector(t, tc, "tmpl1", &ateapipb.Selector{
		MatchLabels: map[string]string{"group": ns},
	})

	createWorkerPod(t, tc, ns, "worker-a", "node1", "pool-a")
	createWorkerPod(t, tc, ns, "worker-b", "node1", "pool-b")

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"tier": "b"},
		},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"}})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := getResp.GetStatus().GetWorkerAssignment().GetWorkerPod(); got != "worker-b" {
		t.Errorf("expected actor to be assigned to worker-b (pool-b, matching narrowed selector), got %q", got)
	}
	if got := getResp.GetStatus().GetWorkerAssignment().GetWorkerPool(); got != "pool-b" {
		t.Errorf("expected actor's worker_assignment.worker_pool to be pool-b, got %q", got)
	}
}

// TestResumeActor_RequiresBothSelectorsToMatch proves eligibility is the AND
// of the template's WorkerSelector and the actor's worker_selector, not
// either one alone: a pool matching only the template selector and a pool
// matching only the actor selector must both be rejected, end to end
// through CreateActor/ResumeActor (not just the eligibleWorkerPools unit
// test), while a pool matching both is the one actually used.
func TestResumeActor_RequiresBothSelectorsToMatch(t *testing.T) {
	ns := namespaceForTest("ns-resume-and-selectors")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createWorkerPool(t, tc, ns, "pool-both", map[string]string{"group": ns, "tier": "b"})
	createWorkerPool(t, tc, ns, "pool-template-only", map[string]string{"group": ns, "tier": "a"})
	createWorkerPool(t, tc, ns, "pool-actor-only", map[string]string{"tier": "b"})
	createTemplateWithSelector(t, tc, "tmpl1", &ateapipb.Selector{
		MatchLabels: map[string]string{"group": ns},
	})

	createWorkerPod(t, tc, ns, "worker-both", "node1", "pool-both")
	createWorkerPod(t, tc, ns, "worker-template-only", "node1", "pool-template-only")
	createWorkerPod(t, tc, ns, "worker-actor-only", "node1", "pool-actor-only")

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"tier": "b"},
		},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"}}); err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := getResp.GetStatus().GetWorkerAssignment().GetWorkerPool(); got != "pool-both" {
		t.Errorf("expected actor to be assigned to pool-both (the only pool matching both selectors), got worker_assignment.worker_pool=%q", got)
	}
}

// TestResumeActor_Reentrancy tests the failure recovery and re-entrancy of ResumeActor.
// Workflow:
// 1. Creates a mock ActorTemplate.
// 2. Creates a mock Atelet Pod and a mock Worker Pod.
// 3. Waits for the WorkerPoolSyncer to mirror the worker to store.
// 4. Creates an actor in SUSPENDED state.
// 5. Configures fake Atelet to FAIL on Restore.
// 6. Calls ResumeActor and verifies it fails, but actor state becomes RESUMING.
// 7. Configures fake Atelet to SUCCEED on Restore.
// 8. Calls ResumeActor again and verifies it succeeds and actor state becomes RUNNING.
func TestResumeActor_Reentrancy(t *testing.T) {
	ns := namespaceForTest("ns-resume-reentrancy")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	// Create Worker Pod
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// STEP 1: Make Atelet FAIL on Restore!
	tc.fakeAtelet.FailRestore = fmt.Errorf("mock atelet failure")

	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err == nil {
		t.Fatalf("expected ResumeActor to fail due to atelet error")
	}

	// Verify actor state is RESUMING in the store.
	actor, err := tc.persistence.GetActor(context.Background(), resources.ActorRef{Atespace: testAtespace, Name: name})
	if err != nil {
		t.Fatalf("failed to get actor from store: %v", err)
	}
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RESUMING {
		t.Errorf("expected state RESUMING, got %v", actor.GetStatus().GetState())
	}

	// STEP 2: Make Atelet SUCCEED!
	tc.fakeAtelet.FailRestore = nil
	tc.fakeAtelet.RestoreCalled = false // reset for verification

	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed on retry: %v", err)
	}

	if !tc.fakeAtelet.RestoreCalled {
		t.Errorf("expected Restore to be called on retry")
	}

	// Verify actor state is RUNNING!
	actor, err = tc.persistence.GetActor(context.Background(), resources.ActorRef{Atespace: testAtespace, Name: name})
	if err != nil {
		t.Fatalf("failed to get actor from store: %v", err)
	}
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("expected state RUNNING, got %v", actor.GetStatus().GetState())
	}
}

// The early ref stamp must land on the span even when the op fails, so a failed
// resume is still attributable to who/where.
func TestResumeActor_ErrorStillStampsRefSpanIdentity(t *testing.T) {
	ns := namespaceForTest("ns-span-resume-err")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
		if _, err := tc.service.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "missing"},
		}); err == nil {
			t.Fatal("expected error resuming missing actor")
		}
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, testAtespace)
	assertSpanStr(t, attrs, ateattr.ActorNameKey, "missing")
}

// TestSuspendActor tests the full workflow of suspending a running actor.
// Workflow:
// 1. Creates a mock ActorTemplate.
// 2. Creates a mock Atelet Pod on 'node1'.
// 3. Creates a mock worker Pod on 'node1'.
// 4. Waits for the WorkerPoolSyncer to mirror the worker to the store.
// 5. Creates an actor.
// 6. Calls ResumeActor to transition it to RUNNING.
// 7. Calls SuspendActor RPC.
// 8. Verifies that the fake Atelet received the Suspend call.
func TestSuspendActor(t *testing.T) {
	ns := namespaceForTest("ns-suspend")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	name := "id1"

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// Resume first to make it running
	running, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	// Suspend
	suspended, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	waitForWorkerAvailable(t, tc, workerName)

	if !tc.fakeAtelet.CheckpointCalled {
		t.Errorf("expected atelet Checkpoint to be called")
	}
	ref := suspended.GetActor().GetStatus().GetLatestSnapshot()
	if ref.GetName() == "" {
		t.Fatalf("SuspendActor returned no ActorSnapshot reference: %v", suspended)
	}
	snapshotRef := ref
	snapshot, err := tc.client.GetActorSnapshot(context.Background(), &ateapipb.GetActorSnapshotRequest{ActorSnapshot: snapshotRef})
	if err != nil {
		t.Fatalf("GetActorSnapshot failed: %v", err)
	}
	if got := snapshot.GetStatus().GetSourceActorVersion(); got != running.GetActor().GetMetadata().GetVersion() {
		t.Errorf("snapshot source version = %d, want %d", got, running.GetActor().GetMetadata().GetVersion())
	}
	listed, err := tc.client.ListActorSnapshots(context.Background(), &ateapipb.ListActorSnapshotsRequest{Atespace: testAtespace, PageSize: 1})
	if err != nil || len(listed.GetActorSnapshots()) != 1 {
		t.Fatalf("ListActorSnapshots = (%v, %v), want one", listed, err)
	}
	tagRef := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "before-upgrade"}
	tagged, err := tc.client.CreateActorSnapshotTag(context.Background(), &ateapipb.CreateActorSnapshotTagRequest{
		ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "before-upgrade"},
			Snapshot: snapshotRef,
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		},
	})
	if err != nil || !proto.Equal(tagged.GetSnapshot(), ref) {
		t.Fatalf("CreateActorSnapshotTag = (%v, %v), want tag for snapshot", tagged, err)
	}
	if _, err := tc.client.CreateActorSnapshotTag(context.Background(), &ateapipb.CreateActorSnapshotTagRequest{
		ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "other", Name: "cross-atespace"},
			Snapshot: snapshotRef,
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		},
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("cross-atespace CreateActorSnapshotTag status = %v, want FailedPrecondition", status.Code(err))
	}
	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:          &ateapipb.ResourceMetadata{Atespace: "other", Name: "cross-atespace"},
			ActorTemplate:     &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
			SourceSnapshotTag: tagRef,
		},
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("cross-atespace CreateActor status = %v, want FailedPrecondition", status.Code(err))
	}
	tagged.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED
	updated, err := tc.client.UpdateActorSnapshotTag(context.Background(), &ateapipb.UpdateActorSnapshotTagRequest{
		ActorSnapshotTag: tagged,
	})
	if err != nil || updated.GetScope() != ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED {
		t.Fatalf("UpdateActorSnapshotTag = (%v, %v), want published", updated, err)
	}
	if got, err := tc.client.GetActorSnapshotTag(context.Background(), &ateapipb.GetActorSnapshotTagRequest{ActorSnapshotTag: tagRef}); err != nil || !proto.Equal(got.GetSnapshot(), ref) {
		t.Fatalf("tag after publication = (%v, %v), want same address", got, err)
	}
	createAtespace(t, tc, "other")
	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:          &ateapipb.ResourceMetadata{Atespace: "other", Name: "cross-atespace"},
			ActorTemplate:     &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
			SourceSnapshotTag: tagRef,
		},
	}); err != nil {
		t.Fatalf("CreateActor from published tag failed: %v", err)
	}

	clone, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:          &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "clone"},
			ActorTemplate:     &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
			SourceSnapshotTag: tagRef,
		},
	})
	if err != nil {
		t.Fatalf("CreateActor from snapshot failed: %v", err)
	}
	if !proto.Equal(clone.GetStatus().GetLatestSnapshot(), ref) {
		t.Fatalf("clone latest snapshot = %v, want %v", clone.GetStatus().GetLatestSnapshot(), ref)
	}
	if !proto.Equal(clone.GetSourceSnapshotTag(), tagRef) {
		t.Fatalf("clone source snapshot tag = %v, want %v", clone.GetSourceSnapshotTag(), tagRef)
	}
	if got := clone.GetStatus().GetSourceSnapshot(); !proto.Equal(got.GetSnapshot(), ref) || got.GetSnapshotUid() != snapshot.GetMetadata().GetUid() {
		t.Fatalf("clone source snapshot = %v, want snapshot %v, uid %v", got, ref, snapshot.GetMetadata().GetUid())
	}
	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "clone"}}); err != nil {
		t.Fatalf("ResumeActor clone failed: %v", err)
	}
	if !tc.fakeAtelet.RestoreCalled {
		t.Error("resuming clone did not restore its source ActorSnapshot")
	}
	cloneSuspended, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "clone"}})
	if err != nil {
		t.Fatalf("SuspendActor clone failed: %v", err)
	}
	if cloneSuspended.GetActor().GetStatus().GetLatestSnapshot().GetName() == ref.GetName() {
		t.Fatal("clone suspension reused its source snapshot")
	}
	listed, err = tc.client.ListActorSnapshots(context.Background(), &ateapipb.ListActorSnapshotsRequest{Atespace: testAtespace})
	if err != nil || len(listed.GetActorSnapshots()) != 2 {
		t.Fatalf("ListActorSnapshots after clone suspension = (%v, %v), want two", listed, err)
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	want := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Name: name, Atespace: testAtespace},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	}

	if diff := cmp.Diff(want, getResp,
		protocmp.Transform(),
		ignoreUID,
		ignoreVersion,
		ignoreTimestamps,
		protocmp.IgnoreFields(&ateapipb.ActorStatus{}, "latest_snapshot"),
	); diff != "" {
		t.Errorf("GetActor response mismatch (-want +got):\n%s", diff)
	}
	if _, err := tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}}); err != nil {
		t.Fatalf("DeleteActor source failed: %v", err)
	}
	if _, err := tc.client.GetActorSnapshotTag(context.Background(), &ateapipb.GetActorSnapshotTagRequest{ActorSnapshotTag: tagRef}); err != nil {
		t.Fatalf("source snapshot tag disappeared with source Actor: %v", err)
	}
	if deleted, err := tc.client.DeleteActorSnapshotTag(context.Background(), &ateapipb.DeleteActorSnapshotTagRequest{ActorSnapshotTag: tagRef}); err != nil || deleted.GetMetadata().GetName() != tagRef.GetName() {
		t.Fatalf("DeleteActorSnapshotTag = (%v, %v)", deleted, err)
	}
	if _, err := tc.client.GetActorSnapshotTag(context.Background(), &ateapipb.GetActorSnapshotTagRequest{ActorSnapshotTag: tagRef}); status.Code(err) != codes.NotFound {
		t.Fatalf("deleted tag status = %v, want NotFound", status.Code(err))
	}
	if _, err := tc.client.GetActorSnapshot(context.Background(), &ateapipb.GetActorSnapshotRequest{ActorSnapshot: snapshotRef}); err != nil {
		t.Fatalf("snapshot metadata disappeared after tag deletion: %v", err)
	}
}

// TestPauseActor tests the full workflow of pausing a running actor.
// Workflow:
// 1. Creates a mock ActorTemplate.
// 2. Creates a mock Atelet Pod on 'node1'.
// 3. Creates a mock worker Pod on 'node1'.
// 4. Waits for the WorkerPoolSyncer to mirror the worker to the store.
// 5. Creates an actor.
// 6. Calls ResumeActor to transition it to RUNNING.
// 7. Calls PauseActor RPC.
// 8. Verifies that the fake Atelet received the Pause call.
func TestPauseActor(t *testing.T) {
	ns := namespaceForTest("ns-pause")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// Resume first to make it running
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	// Pause
	_, err = tc.client.PauseActor(context.Background(), &ateapipb.PauseActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("PauseActor failed: %v", err)
	}

	if !tc.fakeAtelet.CheckpointCalled {
		t.Errorf("expected atelet Checkpoint to be called")
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	want := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Name: name, Atespace: testAtespace},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_PAUSED,
			LocalSnapshotInfo: &ateapipb.LocalSnapshotInfo{
				NodeVmsWithLocalSnapshots: []string{"node1"},
				ContentScope:              ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			},
		},
	}

	if diff := cmp.Diff(want, getResp,
		protocmp.Transform(),
		ignoreUID,
		ignoreVersion,
		ignoreTimestamps,
		protocmp.IgnoreFields(&ateapipb.LocalSnapshotInfo{}, "snapshot_name"),
	); diff != "" {
		t.Errorf("GetActor response mismatch (-want +got):\n%s", diff)
	}
	if getResp.GetStatus().GetLocalSnapshotInfo().GetSnapshotName() == "" {
		t.Error("LocalSnapshotInfo.SnapshotName is empty, want the name the pause checkpointed under")
	}
}

// Pause stamps the ref identity before resolving the Actor record, so a failed
// lookup still carries who/where; it must not invent template/version, which are
// known only once the record resolves (and stamped on success).
func TestPauseActor_FailedLookupStampsRefIdentityOnly(t *testing.T) {
	ns := namespaceForTest("ns-span-pause-err")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
		if _, err := tc.service.PauseActor(ctx, &ateapipb.PauseActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: testActorID},
		}); status.Code(err) != codes.NotFound {
			t.Fatalf("PauseActor(missing) error = %v, want code NotFound", err)
		}
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, testAtespace)
	assertSpanStr(t, attrs, ateattr.ActorNameKey, testActorID)
	for _, k := range []attribute.Key{ateattr.ActorUIDKey, ateattr.TemplateNameKey, ateattr.TemplateAtespaceKey, ateattr.ActorVersionKey} {
		if _, ok := attrs[k]; ok {
			t.Errorf("unexpected %s on failed-pause span", k)
		}
	}
}

// TestResumeActor_ReleasesStaleWorkerWhenPoolBecomesIneligible verifies that
// a worker claimed by a failed resume attempt is released back to the free
// pool if, by the next resume attempt, the actor's worker_selector has
// changed such that the worker's pool is no longer eligible. The actor
// itself is crashed rather than transparently migrated to another pool.
// Workflow:
//  1. Creates pool-a (tier=a) and pool-b (tier=b), and an actor narrowed to
//     tier=a.
//  2. Makes the fake atelet fail Run, then resumes: the actor gets assigned
//     to worker-a (the only eligible pool) and the resume fails after the
//     worker is claimed, leaving worker-a's actor assignment set and the actor
//     stuck in RESUMING.
//  3. Updates the actor's selector to tier=b, making pool-a ineligible.
//  4. Resumes again; asserts it fails and the actor is CRASHED, that worker-a
//     has been released (actor assignment cleared) rather than left dangling,
//     and that worker-b remains free (the crashed actor must not claim it).
func TestResumeActor_ReleasesStaleWorkerWhenPoolBecomesIneligible(t *testing.T) {
	ns := namespaceForTest("ns-resume-release-stale")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createWorkerPool(t, tc, ns, "pool-a", map[string]string{"group": ns, "tier": "a"})
	createWorkerPool(t, tc, ns, "pool-b", map[string]string{"group": ns, "tier": "b"})
	createTemplateWithSelector(t, tc, "tmpl1", &ateapipb.Selector{
		MatchLabels: map[string]string{"group": ns},
	})
	createWorkerPod(t, tc, ns, "worker-a", "node1", "pool-a")
	createWorkerPod(t, tc, ns, "worker-b", "node1", "pool-b")

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:       &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate:  &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "a"}},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	tc.fakeAtelet.FailRun = fmt.Errorf("mock atelet failure")
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}})
	if err == nil {
		t.Fatalf("expected first ResumeActor (onto worker-a) to fail")
	}
	tc.fakeAtelet.FailRun = nil

	current, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	current.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "b"}}
	if _, err := tc.client.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{
		Actor: current,
	}); err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}}); err == nil {
		t.Fatalf("expected second ResumeActor to fail: the assigned worker's pool is no longer eligible")
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := getResp.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("expected actor state CRASHED, got %v", got)
	}

	listResp, err := tc.client.ListWorkers(context.Background(), &ateapipb.ListWorkersRequest{})
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}
	for _, w := range listResp.GetWorkers() {
		if w.GetWorkerNamespace() != ns {
			continue
		}
		switch w.GetWorkerPool() {
		case "pool-a":
			if wass := w.GetStatus().GetAssignment(); wass != nil {
				got := "<nil-actor>"
				if wass.Actor != nil {
					got = wass.Actor.Name
				}
				t.Errorf("expected worker-a (now-ineligible pool-a) to be released, got actor name=%q", got)
			}
		case "pool-b":
			if wass := w.GetStatus().GetAssignment(); wass != nil {
				got := "<nil-actor>"
				if wass.Actor != nil {
					got = wass.Actor.Name
				}
				t.Errorf("expected worker-b to stay free (actor crashed, not migrated), got actor name=%q", got)
			}
		}
	}
}

// TestResumeActor_ReleasesDrainingWorkerFromPriorAttempt exercises the reuse-loop
// change in AssignWorkerStep.Execute: a worker still assigned to the actor from a
// previous (failed) attempt that has since entered DRAINING must not be reused —
// it is released and the actor is crashed.
func TestResumeActor_CrashesIfAssignedWorkerIsDraining(t *testing.T) {
	ns := namespaceForTest("ns-resume-release-draining")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	// createTemplate sets up pool1 (labeled pool=<ns>) + tmpl1 (selecting it) with
	// a golden snapshot, so resume drives Restore. Two workers share the pool.
	createTemplate(t, tc, ns)
	createWorkerPod(t, tc, ns, "worker-a", "node1", "pool1")
	createWorkerPod(t, tc, ns, "worker-b", "node1", "pool1")

	id := "id1"
	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: testAtespace,
				Name:     id,
			},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		},
	}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// First resume fails after a worker is assigned, leaving the actor bound to
	// that worker from a prior attempt.
	tc.fakeAtelet.FailRestore = fmt.Errorf("mock atelet failure")
	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: id}}); err == nil {
		t.Fatalf("expected first ResumeActor to fail")
	}
	tc.fakeAtelet.FailRestore = nil

	// Learn which worker got assigned (findFreeWorker shuffles), then mark it
	// DRAINING as the syncer would when its pod enters Terminating.
	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: id}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	assignedPod := getResp.GetStatus().GetWorkerAssignment().GetWorkerPod()
	if assignedPod == "" {
		t.Fatalf("expected actor to be bound to a worker after the failed attempt")
	}

	assigned, err := tc.persistence.GetWorker(context.Background(), getResp.GetStatus().GetWorkerAssignment().GetWorker().GetName())
	if err != nil {
		t.Fatalf("GetWorker(%s) failed: %v", assignedPod, err)
	}
	if _, err := tc.persistence.UpdateWorker(context.Background(), assigned.GetMetadata().GetName(), store.PreconditionFrom(assigned), func(toUpdate *ateapipb.Worker) error {
		toUpdate.Status.State = ateapipb.WorkerState_WORKER_STATE_DRAINING
		return nil
	}); err != nil {
		t.Fatalf("marking worker %s draining failed: %v", assignedPod, err)
	}

	// Wait until the DRAINING state is observable, which also gives the store
	// watch time to propagate it into the scheduler's worker cache.
	if err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		resp, err := tc.client.ListWorkers(ctx, &ateapipb.ListWorkersRequest{})
		if err != nil {
			return false, nil
		}
		for _, w := range resp.GetWorkers() {
			if w.GetWorkerNamespace() == ns && w.GetWorkerPod() == assignedPod {
				return w.GetStatus().GetState() == ateapipb.WorkerState_WORKER_STATE_DRAINING, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatalf("worker %s did not reach DRAINING: %v", assignedPod, err)
	}

	// Second resume must fail and crash the actor because its worker is draining.
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: id}})
	if err == nil {
		t.Fatalf("expected second ResumeActor to fail")
	}
	if status.Code(err) != codes.Aborted || !strings.Contains(err.Error(), "crashed") {
		t.Errorf("expected Aborted/crashed error, got %v", err)
	}

	getResp, err = tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: id}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := getResp.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("expected actor state CRASHED, got %v", got)
	}
	if got := getResp.GetStatus().GetWorkerAssignment().GetWorkerPod(); got != "" {
		t.Errorf("expected actor pod name to be empty, got %q", got)
	}

	// The draining worker must have been released.
	listResp, err := tc.client.ListWorkers(context.Background(), &ateapipb.ListWorkersRequest{})
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}
	for _, w := range listResp.GetWorkers() {
		if w.GetWorkerNamespace() != ns {
			continue
		}
		if w.GetWorkerPod() == assignedPod {
			if w.GetStatus().GetAssignment() != nil {
				t.Errorf("expected draining worker %q to be released, still assigned to %q", assignedPod, w.GetStatus().GetAssignment().GetActor().GetName())
			}
		}
	}
}

// TestUpdateActor_ReassignsPoolAcrossSuspendResume verifies that updating an
// actor's worker_selector moves it onto a different eligible pool not just
// on the next fresh resume, but also across a full suspend/resume cycle of
// an already-running actor.
// Workflow:
//  1. Creates two WorkerPools, pool-a (tier=a) and pool-b (tier=b), both
//     under the template's gating selector.
//  2. Creates an actor narrowed to tier=a and resumes it; asserts it lands on
//     pool-a/worker-a.
//  3. Updates the actor's selector to tier=b while it's still running.
//  4. Suspends then resumes the actor; asserts it now lands on
//     pool-b/worker-b, proving the updated selector — not the one in effect
//     when it was first scheduled — governs the new placement.
func TestUpdateActor_ReassignsPoolAcrossSuspendResume(t *testing.T) {
	ns := namespaceForTest("ns-update-actor-suspend-resume")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createWorkerPool(t, tc, ns, "pool-a", map[string]string{"group": ns, "tier": "a"})
	createWorkerPool(t, tc, ns, "pool-b", map[string]string{"group": ns, "tier": "b"})
	createTemplateWithSelector(t, tc, "tmpl1", &ateapipb.Selector{
		MatchLabels: map[string]string{"group": ns},
	})

	createWorkerPod(t, tc, ns, "worker-a", "node1", "pool-a")
	createWorkerPod(t, tc, ns, "worker-b", "node1", "pool-b")

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"tier": "a"},
		},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}}); err != nil {
		t.Fatalf("first ResumeActor failed: %v", err)
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := getResp.GetStatus().GetWorkerAssignment().GetWorkerPool(); got != "pool-a" {
		t.Fatalf("expected actor to first resume onto pool-a, got worker_assignment.worker_pool=%q", got)
	}
	if got := getResp.GetStatus().GetWorkerAssignment().GetWorkerPod(); got != "worker-a" {
		t.Fatalf("expected actor to first resume onto worker-a, got worker_assignment.worker_pod=%q", got)
	}

	getResp.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "b"}}
	if _, err := tc.client.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{
		Actor: getResp,
	}); err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	if _, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}}); err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}}); err != nil {
		t.Fatalf("second ResumeActor failed: %v", err)
	}

	getResp, err = tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := getResp.GetStatus().GetWorkerAssignment().GetWorkerPool(); got != "pool-b" {
		t.Errorf("expected actor to resume onto pool-b after selector update, got worker_assignment.worker_pool=%q", got)
	}
	if got := getResp.GetStatus().GetWorkerAssignment().GetWorkerPod(); got != "worker-b" {
		t.Errorf("expected actor to resume onto worker-b after selector update, got worker_assignment.worker_pod=%q", got)
	}
	if got := getResp.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("expected actor state RUNNING after second resume, got %v", got)
	}
}

func TestResumeActor_LeaseConflict(t *testing.T) {
	ns := namespaceForTest("ns-resume-conflict")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// Set a delay on the fake Atelet to hold the lease
	tc.fakeAtelet.RestoreDelay = 1 * time.Second

	// Launch Request A in a goroutine
	errChan := make(chan error, 1)
	go func() {
		_, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
		})
		errChan <- err
	}()

	// Sleep a bit to ensure Request A acquired the lease
	time.Sleep(200 * time.Millisecond)

	// Launch Request B (should fail due to lease conflict)
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	assertGrpcError(t, err, codes.Aborted, "another operation is in progress for this actor")

	// Wait for Request A to finish
	if errA := <-errChan; errA != nil {
		t.Fatalf("Request A failed: %v", errA)
	}
}

func TestResumeActor_DanglingWorker(t *testing.T) {
	ns := namespaceForTest("ns-resume-dangling")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	// 1. Create Worker Pod A
	createWorkerPod(t, tc, ns, "worker-a", "node1", "pool1")

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// 2. Configure fake Atelet to FAIL on Restore!
	tc.fakeAtelet.FailRestore = fmt.Errorf("mock atelet failure")

	// 3. Call ResumeActor -> Expect failure
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err == nil {
		t.Fatalf("expected ResumeActor to fail due to atelet error")
	}

	// Verify actor state is RESUMING with worker A assigned
	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	actor := getResp
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RESUMING {
		t.Fatalf("expected state RESUMING, got %v", actor.GetStatus().GetState())
	}
	if actor.GetStatus().GetWorkerAssignment().GetWorkerPod() != "worker-a" {
		t.Fatalf("expected worker-a assigned, got %v", actor.GetStatus().GetWorkerAssignment().GetWorkerPod())
	}

	deleteWorkerPod(t, tc, ns, "worker-a")

	// 6. Create Worker Pod B
	createWorkerPod(t, tc, ns, "worker-b", "node1", "pool1")

	// 7. Configure fake Atelet to SUCCEED on Restore
	tc.fakeAtelet.FailRestore = nil
	tc.fakeAtelet.RestoreCalled = false // reset

	// 8. Call ResumeActor again -> Expect it to fail because it is already CRASHED by background syncer.
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err == nil {
		t.Fatalf("expected ResumeActor to fail because worker is gone")
	}
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "ACTOR_STATE_CRASHED") {
		t.Errorf("expected FailedPrecondition/ACTOR_STATE_CRASHED error, got %v", err)
	}

	// Verify actor state is CRASHED and worker assignment is empty
	actor, err = tc.persistence.GetActor(context.Background(), resources.ActorRef{Atespace: testAtespace, Name: name})
	if err != nil {
		t.Fatalf("failed to get actor from store: %v", err)
	}
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("expected state CRASHED, got %v", actor.GetStatus().GetState())
	}
	if actor.GetStatus().GetWorkerAssignment().GetWorkerPod() != "" {
		t.Errorf("expected worker to be unassigned, got %v", actor.GetStatus().GetWorkerAssignment().GetWorkerPod())
	}
}

func TestSuspendActor_DanglingWorker(t *testing.T) {
	ns := namespaceForTest("ns-sd")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	// 1. Create Worker Pod
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// Resume first to make it running
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	deleteWorkerPod(t, tc, ns, "worker-1")

	// 3. Call SuspendActor -> Expect it to fail because it is already CRASHED by background syncer
	_, err = tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err == nil {
		t.Fatalf("expected SuspendActor to fail because worker is gone")
	}
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "ACTOR_STATE_CRASHED") {
		t.Errorf("expected FailedPrecondition error, got %v", err)
	}

	// 4. Verify it becomes CRASHED in the store.
	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if getResp.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("expected status CRASHED, got %v", getResp.GetStatus())
	}
	if getResp.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("expected worker_assignment to be cleared, got %v", getResp.GetStatus().GetWorkerAssignment())
	}
}

// TestSuspendActor_FromPaused suspends a PAUSED actor end-to-end: instead of
// checkpointing a running workload, ateapi asks the atelet on the node
// holding the pause snapshot to upload it, then finalizes the actor with a
// durable ActorSnapshot and no node pinning left behind.
func TestSuspendActor_FromPaused(t *testing.T) {
	ns := namespaceForTest("ns-suspend-paused")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	}); err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}
	if _, err := tc.client.PauseActor(context.Background(), &ateapipb.PauseActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	}); err != nil {
		t.Fatalf("PauseActor failed: %v", err)
	}
	paused, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	// Drop the pause's Checkpoint call so the suspend's atelet traffic is
	// observable in isolation.
	tc.fakeAtelet.Reset()

	suspended, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}

	if !tc.fakeAtelet.UploadCalled {
		t.Fatal("expected atelet UploadPausedCheckpoint to be called")
	}
	if tc.fakeAtelet.CheckpointCalled {
		t.Error("atelet Checkpoint called for a paused actor; there is no workload to checkpoint")
	}
	upload := tc.fakeAtelet.UploadRequest
	if got, want := upload.GetLocalSnapshotName(), paused.GetStatus().GetLocalSnapshotInfo().GetSnapshotName(); got != want {
		t.Errorf("upload local_snapshot_name = %q, want the pause snapshot %q", got, want)
	}
	if got, want := upload.GetAtespace(), testAtespace; got != want {
		t.Errorf("upload atespace = %q, want %q", got, want)
	}
	if got := upload.GetDesiredScope(); got != ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL {
		t.Errorf("upload desired_scope = %v, want FULL (template default)", got)
	}

	actor := suspended.GetActor()
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED", actor.GetStatus().GetState())
	}
	if actor.GetStatus().GetLocalSnapshotInfo() != nil {
		t.Errorf("LocalSnapshotInfo = %v, want cleared (node pinning must not survive suspend)", actor.GetStatus().GetLocalSnapshotInfo())
	}
	ref := actor.GetStatus().GetLatestSnapshot()
	if ref.GetName() == "" {
		t.Fatalf("SuspendActor returned no ActorSnapshot reference: %v", suspended)
	}
	snapshot, err := tc.client.GetActorSnapshot(context.Background(), &ateapipb.GetActorSnapshotRequest{
		ActorSnapshot: ref,
	})
	if err != nil {
		t.Fatalf("GetActorSnapshot failed: %v", err)
	}
	if got, want := snapshot.GetStatus().GetSnapshotUri(), upload.GetDestinationSnapshotUri(); got != want {
		t.Errorf("snapshot URI = %q, want the upload destination %q", got, want)
	}
	if got := snapshot.GetStatus().GetContentScope(); got != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL {
		t.Errorf("snapshot ContentScope = %v, want FULL", got)
	}
}

// TestSuspendActor_FromPaused_RetryAfterUploadFailure exercises client-driven
// forward recovery on the paused path: a failed upload leaves the actor
// SUSPENDING with its local snapshot record intact, and a retry completes the
// suspend against the same destination.
func TestSuspendActor_FromPaused_RetryAfterUploadFailure(t *testing.T) {
	ns := namespaceForTest("ns-suspend-paused-retry")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	}); err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}
	if _, err := tc.client.PauseActor(context.Background(), &ateapipb.PauseActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	}); err != nil {
		t.Fatalf("PauseActor failed: %v", err)
	}

	tc.fakeAtelet.Reset()
	tc.fakeAtelet.FailUpload = status.Error(codes.Unavailable, "injected upload failure")
	if _, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	}); err == nil {
		t.Fatal("SuspendActor succeeded despite failing upload")
	}

	stuck, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if stuck.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
		t.Fatalf("state after failed upload = %v, want SUSPENDING (retryable)", stuck.GetStatus().GetState())
	}
	if stuck.GetStatus().GetLocalSnapshotInfo() == nil {
		t.Fatal("LocalSnapshotInfo cleared by a failed upload; the retry could never find the snapshot")
	}
	firstDestination := tc.fakeAtelet.UploadRequest.GetDestinationSnapshotUri()

	tc.fakeAtelet.Lock.Lock()
	tc.fakeAtelet.FailUpload = nil
	tc.fakeAtelet.Lock.Unlock()
	retried, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("SuspendActor retry failed: %v", err)
	}
	if got := retried.GetActor().GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state after retry = %v, want SUSPENDED", got)
	}
	if got := tc.fakeAtelet.UploadRequest.GetDestinationSnapshotUri(); got != firstDestination {
		t.Errorf("retry destination = %q, want the original %q (idempotent upload target)", got, firstDestination)
	}
}

// TestResumeActor_RelocatesAfterSuspendFromPaused covers the capacity-recovery
// flow: a PAUSED actor is pinned to the node holding its local snapshot, so it
// cannot resume while that node is full. Suspending it uploads the snapshot and
// drops the node pinning, after which it is scheduled onto a worker on a different
// node.
func TestResumeActor_RelocatesAfterSuspendFromPaused(t *testing.T) {
	ns := namespaceForTest("ns-resume-relocate")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	const pinned, relocated = "actor-pinned", "actor-squatter"
	for _, name := range []string{pinned, relocated} {
		if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		}}); err != nil {
			t.Fatalf("CreateActor(%s) failed: %v", name, err)
		}
	}

	// The actor under test runs on node1's only worker, then pauses — which
	// frees that worker but pins the actor to node1 via LocalSnapshotInfo.
	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: pinned},
	}); err != nil {
		t.Fatalf("ResumeActor(%s) failed: %v", pinned, err)
	}
	if _, err := tc.client.PauseActor(context.Background(), &ateapipb.PauseActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: pinned},
	}); err != nil {
		t.Fatalf("PauseActor(%s) failed: %v", pinned, err)
	}
	paused, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: pinned},
	})
	if err != nil {
		t.Fatalf("GetActor(%s) failed: %v", pinned, err)
	}
	if got := paused.GetStatus().GetLocalSnapshotInfo().GetNodeVmsWithLocalSnapshots(); len(got) != 1 || got[0] != "node1" {
		t.Fatalf("paused actor pinned to %v, want [node1]", got)
	}
	waitForWorkerAvailable(t, tc, workerName)

	// Another actor takes node1's only worker, so the pinned actor's node is full
	// while free capacity exists elsewhere.
	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: relocated},
	}); err != nil {
		t.Fatalf("ResumeActor(%s) failed: %v", relocated, err)
	}
	createWorkerPod(t, tc, ns, "worker-2", "node2", "pool1")
	setupAteletOnNode(t, tc, "atelet-node2", "node2")

	// Capacity exhaustion is ResourceExhausted.
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: pinned},
	})
	assertGrpcError(t, err, codes.ResourceExhausted, "no free workers available")

	suspended, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: pinned},
	})
	if err != nil {
		t.Fatalf("SuspendActor(%s) failed: %v", pinned, err)
	}
	if got := suspended.GetActor().GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("state after suspend = %v, want SUSPENDED", got)
	}
	if got := suspended.GetActor().GetStatus().GetLocalSnapshotInfo(); got != nil {
		t.Fatalf("LocalSnapshotInfo = %v, want cleared so the actor can be scheduled anywhere", got)
	}

	// Resume should succeed now and the actor scheduled on node2.
	resumed, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: pinned},
	})
	if err != nil {
		t.Fatalf("ResumeActor(%s) after suspend failed: %v", pinned, err)
	}
	if got := resumed.GetActor().GetStatus().GetWorkerAssignment().GetWorkerPod(); got != "worker-2" {
		t.Errorf("resumed onto worker %q, want worker-2 (the worker on node2)", got)
	}
	worker, err := tc.persistence.GetWorker(context.Background(), resumed.GetActor().GetStatus().GetWorkerAssignment().GetWorker().GetName())
	if err != nil {
		t.Fatalf("GetWorker(worker-2) failed: %v", err)
	}
	if got := worker.GetNodeName(); got != "node2" {
		t.Errorf("worker-2 node = %q, want node2", got)
	}
}

// TestLifecycleOpPoolAttributesOnSuccess is the regression test for #957: a
// successful suspend and pause must stamp the pool they ran on. Both recorded
// the histogram from a defer that read the finalized record, whose assignment
// the finalize step had already cleared, so the pair landed only on failures.
func TestLifecycleOpPoolAttributesOnSuccess(t *testing.T) {
	tests := []struct {
		name string
		op   string
		// run performs the operation on an actor that is already RUNNING.
		run func(t *testing.T, tc *testContext, actor *ateapipb.ObjectRef)
	}{
		{
			name: "suspend",
			op:   ateattr.OperationSuspend,
			run: func(t *testing.T, tc *testContext, actor *ateapipb.ObjectRef) {
				t.Helper()
				if _, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{Actor: actor}); err != nil {
					t.Fatalf("SuspendActor failed: %v", err)
				}
			},
		},
		{
			name: "pause",
			op:   ateattr.OperationPause,
			run: func(t *testing.T, tc *testContext, actor *ateapipb.ObjectRef) {
				t.Helper()
				if _, err := tc.client.PauseActor(context.Background(), &ateapipb.PauseActorRequest{Actor: actor}); err != nil {
					t.Fatalf("PauseActor failed: %v", err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := namespaceForTest("ns-lifecycle-pool-" + tt.name)
			tc := setupTest(t, ns)
			defer tc.cleanup()

			createTemplate(t, tc, ns)
			createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

			actorRef := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"}
			if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.GetAtespace(), Name: actorRef.GetName()},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
			}}); err != nil {
				t.Fatalf("CreateActor failed: %v", err)
			}
			if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: actorRef}); err != nil {
				t.Fatalf("ResumeActor failed: %v", err)
			}

			tt.run(t, tc, actorRef)

			attrs := lifecycleOpAttributes(t, tc, tt.op)
			if got, ok := attrs.Value(ateattr.WorkerPoolNamespaceKey); !ok || got.AsString() != ns {
				t.Errorf("%s = %q (present: %v), want %q", ateattr.WorkerPoolNamespaceKey, got.AsString(), ok, ns)
			}
			if got, ok := attrs.Value(ateattr.WorkerPoolNameKey); !ok || got.AsString() != "pool1" {
				t.Errorf("%s = %q (present: %v), want %q", ateattr.WorkerPoolNameKey, got.AsString(), ok, "pool1")
			}
			// error.type's absence marks a success, so its presence would mean the
			// datapoint under test is not the happy path.
			if _, ok := attrs.Value(ateattr.ErrorTypeKey); ok {
				t.Errorf("%s is set on the %s datapoint, want the successful operation", ateattr.ErrorTypeKey, tt.op)
			}
		})
	}
}

// lifecycleOpAttributes returns the attribute set of the single
// ate.actor.lifecycle.operation.duration datapoint recorded for op.
func lifecycleOpAttributes(t *testing.T, tc *testContext, op string) attribute.Set {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := tc.metricReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	var got []attribute.Set
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "ate.actor.lifecycle.operation.duration" {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("%s data type = %T, want a float64 histogram", m.Name, m.Data)
			}
			for _, dp := range hist.DataPoints {
				if v, ok := dp.Attributes.Value(ateattr.ActorOperationNameKey); ok && v.AsString() == op {
					got = append(got, dp.Attributes)
				}
			}
		}
	}
	if len(got) != 1 {
		t.Fatalf("datapoints for %s = %d (%v), want exactly one", op, len(got), got)
	}
	return got[0]
}

// TestCreateActor_RejectsUnknownRequestFields checks that a request carrying a
// field this binary has no descriptor for is refused by the server-wide
// interceptor before it reaches the handler.
func TestCreateActor_RejectsUnknownRequestFields(t *testing.T) {
	ns := namespaceForTest("ns-create-unknown-field")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	req := &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}}
	unknown := protowire.AppendTag(nil, 9999, protowire.VarintType)
	req.ProtoReflect().SetUnknown(protowire.AppendVarint(unknown, 42))

	_, err := tc.client.CreateActor(context.Background(), req)
	assertGrpcError(t, err, codes.InvalidArgument, "request: Invalid value: unknown field with protobuf tag 9999")
}
