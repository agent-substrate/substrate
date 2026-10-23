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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/objectstoreplugin/objectstoreplugintest"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
)

// validActorTemplate returns the smallest template that passes create
// validation; mutations tweak it per test case. The snapshot scopes and
// resume policy are set explicitly because TestValidateActorTemplate
// exercises validation directly, without defaulting.
func validActorTemplate(mutations ...func(*ateapi.ActorTemplate)) *ateapi.ActorTemplate {
	template := &ateapi.ActorTemplate{
		Metadata:   &ateapi.ResourceMetadata{Atespace: "ns1", Name: "tmpl-a"},
		Containers: []*ateapi.Container{{Name: "main", Image: "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}},
		SnapshotConfig: &ateapi.SnapshotConfig{
			StorageLocation:   "gs://my-bucket/snapshots",
			PreferredFidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		},
		Resources: &ateapi.ResourceRequirements{Limits: resources.CPUMemory(1000, 512<<20)},
	}
	for _, m := range mutations {
		m(template)
	}
	return template
}

// TestCreateActorTemplate covers the atespace precondition: creation fails
// while the atespace is missing, and succeeds once the atespace exists.
func TestCreateActorTemplate(t *testing.T) {
	persistence := newTestPersistence(t)
	s := &RPCService{impl: newServiceImpl(persistence, nil)}
	ctx := context.Background()
	req := func(atespace, name string) *ateapi.CreateActorTemplateRequest {
		return &ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
			tmpl.Metadata = &ateapi.ResourceMetadata{Atespace: atespace, Name: name}
		})}
	}

	if _, err := s.CreateActorTemplate(ctx, req("ns-missing", "tmpl-a")); apierror.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActorTemplate in missing atespace = %v, want FailedPrecondition", err)
	}

	if _, err := persistence.CreateAtespace(ctx, &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: "ns1"}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	created, err := s.CreateActorTemplate(ctx, req("ns1", "tmpl-a"))
	if err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}
	if created.GetMetadata().GetName() != "tmpl-a" {
		t.Errorf("created name = %q, want tmpl-a", created.GetMetadata().GetName())
	}
}

// TestCreateActorTemplate_RequiresSnapshotLocation pins that create rejects,
// with FailedPrecondition, a template whose actors would have no snapshot
// location: neither an honored storageLocation nor --snapshot-location.
func TestCreateActorTemplate_RequiresSnapshotLocation(t *testing.T) {
	tests := []struct {
		name                    string
		gates                   []string
		optOut                  bool
		clusterSnapshotLocation string
		location                string
		wantCode                codes.Code
	}{{
		name:                    "gate off, cluster location set",
		clusterSnapshotLocation: "gs://cluster-bucket/",
		location:                "gs://my-bucket/snapshots",
		wantCode:                codes.OK,
	}, {
		name:     "gate off, no cluster location",
		location: "gs://my-bucket/snapshots",
		wantCode: codes.FailedPrecondition,
	}, {
		name:     "gate on, location set, no cluster location",
		gates:    []string{"PerTemplateSnapshotLocation"},
		location: "gs://my-bucket/snapshots",
		wantCode: codes.OK,
	}, {
		name:                    "gate on, no location, cluster location set",
		gates:                   []string{"PerTemplateSnapshotLocation"},
		clusterSnapshotLocation: "gs://cluster-bucket/",
		wantCode:                codes.OK,
	}, {
		name:                    "gate on, opted out, no location, cluster location set",
		gates:                   []string{"PerTemplateSnapshotLocation"},
		optOut:                  true,
		clusterSnapshotLocation: "gs://cluster-bucket/",
		wantCode:                codes.OK,
	}, {
		name:     "gate on, no location, no cluster location",
		gates:    []string{"PerTemplateSnapshotLocation"},
		wantCode: codes.FailedPrecondition,
	}}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			preview.InitForTest(t, tt.gates...)
			persistence := newTestPersistence(t)
			impl := newServiceImpl(persistence, nil)
			impl.clusterSnapshotLocation = tt.clusterSnapshotLocation
			s := &RPCService{impl: impl}
			ctx := context.Background()
			if _, err := persistence.CreateAtespace(ctx, &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: "ns1"}}); err != nil {
				t.Fatalf("CreateAtespace failed: %v", err)
			}
			if tt.optOut {
				ctx = preview.ContextWithOptOut(ctx)
			}
			req := &ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
				tmpl.Metadata = &ateapi.ResourceMetadata{Atespace: "ns1", Name: fmt.Sprintf("tmpl-%d", i)}
				tmpl.SnapshotConfig.StorageLocation = tt.location
			})}
			if _, err := s.CreateActorTemplate(ctx, req); apierror.Code(err) != tt.wantCode {
				t.Errorf("CreateActorTemplate error = %v, want code %v", err, tt.wantCode)
			}
		})
	}
}

// TestCreateActorTemplateIgnoresServerOwnedFields pins the create contract:
// status on the request is dropped and new templates start with an empty
// status. The store persists whatever the handler hands it, so the handler is
// the only guard.
func TestCreateActorTemplateIgnoresServerOwnedFields(t *testing.T) {
	persistence := newTestPersistence(t)
	s := &RPCService{impl: newServiceImpl(persistence, nil)}
	ctx := context.Background()

	if _, err := persistence.CreateAtespace(ctx, &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: "ns1"}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}

	in := validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
		tmpl.Metadata.Uid = "11111111-1111-1111-1111-111111111111"
		tmpl.Metadata.Version = 42
		tmpl.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"pool": "default"}}
		tmpl.Containers = []*ateapi.Container{{Name: "main", Image: "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}}
		tmpl.Resources = &ateapi.ResourceRequirements{Limits: resources.CPUMemory(1000, 1<<30)}
		// Server-owned status a client must not be able to set.
		tmpl.Status = &ateapi.ActorTemplateStatus{
			GoldenSnapshotStatus: &ateapi.GoldenSnapshotStatus{
				GoldenTag: &ateapi.ObjectRef{Atespace: "ns1", Name: "golden-tag"},
			},
		}
	})
	created, err := s.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{ActorTemplate: in})
	if err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}

	want := validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
		tmpl.Metadata.Version = 1
		tmpl.SnapshotConfig.GoldenSnapshotConfig = &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED}
		tmpl.WorkerSelector = in.GetWorkerSelector()
		tmpl.Containers = in.GetContainers()
		tmpl.Resources = in.GetResources()
		tmpl.Status = &ateapi.ActorTemplateStatus{}
	})
	if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
		t.Errorf("CreateActorTemplate response mismatch (-want +got):\n%s", diff)
	}
	if got := created.GetMetadata().GetUid(); got == "" || got == in.GetMetadata().GetUid() {
		t.Errorf("created uid = %q, want a fresh server-assigned uid", got)
	}
}

// TestCreateActorTemplateDefaultsOmittedSnapshotConfig pins that
// snapshot_config is optional: create fills an omitted one with every default.
func TestCreateActorTemplateDefaultsOmittedSnapshotConfig(t *testing.T) {
	persistence := newTestPersistence(t)
	impl := newServiceImpl(persistence, nil)
	impl.clusterSnapshotLocation = "gs://cluster-bucket/"
	s := &RPCService{impl: impl}
	ctx := context.Background()
	if _, err := persistence.CreateAtespace(ctx, &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: "ns1"}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}

	created, err := s.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
		tmpl.SnapshotConfig = nil
	})})
	if err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}
	want := &ateapi.SnapshotConfig{
		PreferredFidelity:    ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED},
	}
	if diff := cmp.Diff(want, created.GetSnapshotConfig(), protocmp.Transform()); diff != "" {
		t.Errorf("CreateActorTemplate snapshot_config mismatch (-want +got):\n%s", diff)
	}
}

func TestDeleteActorTemplate(t *testing.T) {
	tests := []struct {
		name         string
		actorDeleted bool
		tagDeleted   bool
		pendingTag   bool
		// failPrefix makes object storage fail cleanup for this resource kind.
		failPrefix            string
		wantActorAfterFailure bool
		staleGuard            bool
	}{
		{name: "golden actor and tag"},
		{name: "golden actor already deleted", actorDeleted: true},
		{name: "golden tag absent", tagDeleted: true},
		{name: "no golden resources", actorDeleted: true, tagDeleted: true},
		{name: "incomplete golden tag", pendingTag: true},
		{name: "actor cleanup failure", failPrefix: "/actors/", wantActorAfterFailure: true},
		{name: "tag cleanup failure", failPrefix: "/tags/"},
		{name: "stale guard refused before cleanup", staleGuard: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			persistence := newTestPersistence(t)
			tmpl := seedSubstrateTemplate(t, ctx, persistence, "tmpl")
			templateRef := resources.ActorTemplateRefFromActorTemplate(tmpl)
			goldenRef := resources.ActorRef{Atespace: "team-a", Name: tmpl.GetMetadata().GetUid()}
			actor := storetest.MustCreateActor(t, ctx, persistence, &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Atespace: goldenRef.Atespace, Name: goldenRef.Name},
				ActorTemplate: templateRef.ToObjectRef(),
				Status:        &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
			})
			workflow, objects := newFinalizeWorkflow(persistence)
			actorURI := mustActorSnapshotURI(t, tmpl, actor, "snapshot")
			objects.PutSnapshot(t, actorURI, "manifest.json")
			actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapi.ActorStatus) {
				s.LastAssignedGeneration = 1
				s.Snapshots = []*ateapi.Snapshot{
					newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, tmpl.GetMetadata().GetUid(), actorURI.Name(), actorURI.String(), ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				}
			})
			var tag *ateapi.Tag
			if tt.pendingTag {
				tag = storetest.MustCreateTag(t, ctx, persistence, newPendingTestTag(t, goldenRef.Name, actor))
			} else {
				var err error
				tag, err = workflow.TagActorSnapshot(ctx, tagToCreate(goldenRef, goldenRef.Name))
				if err != nil {
					t.Fatal(err)
				}
			}
			tagRef := resources.TagRefFromTag(tag)
			tagURI := mustReservedTagSnapshotURI(t, tag)
			objects.PutSnapshot(t, tagURI, "manifest.json")
			svc := &RPCService{impl: newServiceImpl(persistence, nil), actorWorkflow: workflow, objectStorePlugin: objectstoreplugintest.ControlClient(objects)}
			// The handler must clean up an active golden actor.
			mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapi.ActorStatus) {
				s.State = ateapi.ActorState_ACTOR_STATE_RUNNING
			})
			if tt.actorDeleted {
				if _, err := workflow.DeleteActor(ctx, goldenRef, store.DeletePreconditions{}); err != nil {
					t.Fatal(err)
				}
			}
			if tt.tagDeleted {
				if _, err := svc.DeleteTag(ctx, &ateapi.DeleteTagRequest{Tag: tagRef.ToObjectRef()}); err != nil {
					t.Fatal(err)
				}
			}
			if tt.failPrefix != "" {
				objects.OnDelete = func(_, key string) error {
					if strings.Contains(key, tt.failPrefix) {
						return errObjectStore
					}
					return nil
				}
			}
			if tt.staleGuard {
				current, err := persistence.GetActorTemplate(ctx, templateRef)
				if err != nil {
					t.Fatal(err)
				}
				stale := store.DeletePreconditions{UID: current.GetMetadata().GetUid(), Version: current.GetMetadata().GetVersion() + 1}
				if _, err := workflow.DeleteActorTemplate(ctx, templateRef, stale); apierror.Code(err) != codes.Aborted {
					t.Fatalf("DeleteActorTemplate with a stale version = %v, want code Aborted", err)
				}
				if _, err := persistence.GetActor(ctx, goldenRef); err != nil {
					t.Fatalf("golden actor after the refused delete: %v", err)
				}
				if _, err := persistence.GetTag(ctx, tagRef); err != nil {
					t.Fatalf("golden tag after the refused delete: %v", err)
				}
				if current, err := persistence.GetActorTemplate(ctx, templateRef); err != nil || current.GetMetadata().GetDeleteTime() != nil {
					t.Fatalf("template after the refused delete = %v, %v; want it present and not marked", current, err)
				}
			}
			req := &ateapi.DeleteActorTemplateRequest{ActorTemplate: templateRef.ToObjectRef()}
			deleted, err := svc.DeleteActorTemplate(ctx, req)
			if tt.failPrefix != "" {
				if !isObjectStoreErr(err) {
					t.Fatalf("DeleteActorTemplate = %v, want object storage error", err)
				}
				admitted, getErr := persistence.GetActorTemplate(ctx, templateRef)
				if getErr != nil {
					t.Fatalf("template lost after cleanup failure: %v", getErr)
				}
				if admitted.GetMetadata().GetDeleteTime() == nil {
					t.Fatal("a failed delete left delete_time unset")
				}
				if _, err := persistence.GetTag(ctx, tagRef); err != nil {
					t.Fatalf("tag lost after cleanup failure: %v", err)
				}
				_, actorErr := persistence.GetActor(ctx, goldenRef)
				if tt.wantActorAfterFailure && actorErr != nil || !tt.wantActorAfterFailure && !errors.Is(actorErr, store.ErrNotFound) {
					t.Fatalf("GetActor after failure = %v, want present %v", actorErr, tt.wantActorAfterFailure)
				}
				if _, err := svc.DeleteActorTemplate(ctx, req); !isObjectStoreErr(err) {
					t.Fatalf("retried DeleteActorTemplate = %v, want object storage error", err)
				}
				retried, retryErr := persistence.GetActorTemplate(ctx, templateRef)
				if retryErr != nil {
					t.Fatalf("template lost after a retried cleanup failure: %v", retryErr)
				}
				if got, want := retried.GetMetadata().GetVersion(), admitted.GetMetadata().GetVersion(); got != want {
					t.Errorf("a retried delete moved the version to %d, want %d", got, want)
				}
				objects.OnDelete = nil
				deleted, err = svc.DeleteActorTemplate(ctx, req)
			}
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tmpl, deleted, protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
				t.Fatalf("deleted template mismatch (-want +got):\n%s", diff)
			}
			if deleted.GetMetadata().GetDeleteTime() == nil {
				t.Fatal("the deleted template carries no delete_time")
			}
			if _, err := persistence.GetActorTemplate(ctx, templateRef); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("GetActorTemplate after delete = %v, want NotFound", err)
			}
			if _, err := persistence.GetActor(ctx, goldenRef); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("GetActor after delete = %v, want NotFound", err)
			}
			if _, err := persistence.GetTag(ctx, tagRef); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("GetTag after delete = %v, want NotFound", err)
			}
			for _, uri := range []resources.SnapshotURI{actorURI, tagURI} {
				if got := objects.Snapshot(t, uri); len(got) != 0 {
					t.Errorf("snapshot %s still holds %v", uri, got)
				}
			}
			if _, err := svc.DeleteActorTemplate(ctx, req); apierror.Code(err) != codes.NotFound {
				t.Fatalf("delete missing template = %v, want NotFound", err)
			}
		})
	}
}

// seedSubstrateTemplate stores a minimal substrate ActorTemplate in team-a.
func seedSubstrateTemplate(t *testing.T, ctx context.Context, persistence store.Interface, name string) *ateapi.ActorTemplate {
	t.Helper()
	created, err := persistence.CreateActorTemplate(ctx, &ateapi.ActorTemplate{
		Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: name},
		SnapshotConfig: &ateapi.SnapshotConfig{
			StorageLocation: "gs://ate-snapshots/team-a/",
		},
	})
	if err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}
	stored, err := persistence.GetActorTemplate(ctx, resources.ActorTemplateRefFromActorTemplate(created))
	if err != nil {
		t.Fatalf("GetActorTemplate: %v", err)
	}
	return stored
}

// TestResolveActorTemplate verifies the resolver reads the substrate resource
// the actor's actor_template reference names.
func TestResolveActorTemplate(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	stored := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")

	t.Run("ref reads the store", func(t *testing.T) {
		actor := &ateapi.Actor{ActorTemplate: &ateapi.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"}}
		got, err := resolveActorTemplate(ctx, persistence, actor)
		if err != nil {
			t.Fatalf("resolveActorTemplate: %v", err)
		}
		if got.GetMetadata().GetUid() != stored.GetMetadata().GetUid() {
			t.Errorf("template uid = %q, want the stored substrate template %q", got.GetMetadata().GetUid(), stored.GetMetadata().GetUid())
		}
	})

	t.Run("ref to a missing template is FailedPrecondition", func(t *testing.T) {
		actor := &ateapi.Actor{ActorTemplate: &ateapi.ObjectRef{Atespace: "team-a", Name: "absent"}}
		_, err := resolveActorTemplate(ctx, persistence, actor)
		if got := apierror.Code(err); got != codes.FailedPrecondition {
			t.Fatalf("status.Code = %v, want FailedPrecondition (err: %v)", got, err)
		}
	})
}

// TestResolveActorTemplate_NotFound verifies a vanished template and an actor
// naming no template at all surface errActorTemplateNotFound, so callers like
// delete can tolerate them.
func TestResolveActorTemplate_NotFound(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	stored := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")

	tests := []struct {
		name         string
		actor        *ateapi.Actor
		wantNotFound bool
	}{
		{"ref resolves", &ateapi.Actor{ActorTemplate: &ateapi.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"}}, false},
		{"ref to deleted template", &ateapi.Actor{ActorTemplate: &ateapi.ObjectRef{Atespace: "team-a", Name: "gone"}}, true},
		{"no template named at all", &ateapi.Actor{}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveActorTemplate(ctx, persistence, tc.actor)
			if tc.wantNotFound {
				if !errors.Is(err, errActorTemplateNotFound) {
					t.Fatalf("resolveActorTemplate err = %v, want errActorTemplateNotFound", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveActorTemplate: %v", err)
			}
			if got.GetMetadata().GetUid() != stored.GetMetadata().GetUid() {
				t.Errorf("template uid = %q, want %q", got.GetMetadata().GetUid(), stored.GetMetadata().GetUid())
			}
		})
	}
}

// TestUpdateActorTemplateMetadata pins the store's update behavior: the
// server-assigned metadata is recomputed in place, and metadata identity is
// immutable.
func TestUpdateActorTemplateMetadata(t *testing.T) {
	persistence := newTestPersistence(t)
	ctx := context.Background()

	if _, err := persistence.CreateAtespace(ctx, &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: "ns1"}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	created, err := persistence.CreateActorTemplate(ctx, validActorTemplate())
	if err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}
	ref := resources.ActorTemplateRefFromActorTemplate(created)

	// A server-owned status write passes validation and bumps the version.
	updated, err := persistence.UpdateActorTemplate(ctx, ref, store.PreconditionFrom(created), func(tmpl *ateapi.ActorTemplate) error {
		tmpl.Status = &ateapi.ActorTemplateStatus{GoldenSnapshotStatus: &ateapi.GoldenSnapshotStatus{
			GoldenTag: &ateapi.ObjectRef{Atespace: "ns1", Name: "golden-tag"},
		}}
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateActorTemplate failed: %v", err)
	}
	if got, want := updated.GetMetadata().GetVersion(), created.GetMetadata().GetVersion()+1; got != want {
		t.Errorf("updated version = %d, want %d", got, want)
	}

	// A mutation that touches an immutable field is a server bug, rejected by
	// the store.
	for name, mutate := range map[string]func(*ateapi.ActorTemplate) error{
		"atespace": func(tmpl *ateapi.ActorTemplate) error { tmpl.Metadata.Atespace = "ns2"; return nil },
		"name":     func(tmpl *ateapi.ActorTemplate) error { tmpl.Metadata.Name = "tmpl-b"; return nil },
	} {
		if _, err := persistence.UpdateActorTemplate(ctx, ref, store.PreconditionFrom(updated), mutate); err == nil {
			t.Errorf("mutating %s succeeded, want error", name)
		}
	}

	// Server-assigned metadata edits are overwritten, not errors: the store
	// restores them from the stored value.
	reverted, err := persistence.UpdateActorTemplate(ctx, ref, store.PreconditionFrom(updated), func(tmpl *ateapi.ActorTemplate) error {
		tmpl.Metadata.Uid = "1e186271-b829-4085-b2b1-6b665c1a4f42"
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateActorTemplate with uid edit failed: %v", err)
	}
	if got, want := reverted.GetMetadata().GetUid(), created.GetMetadata().GetUid(); got != want {
		t.Errorf("uid after update = %q, want %q", got, want)
	}
}
