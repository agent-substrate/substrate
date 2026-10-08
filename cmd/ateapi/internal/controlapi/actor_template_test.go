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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// validActorTemplate returns the smallest template that passes create
// validation; mutations tweak it per test case. The snapshot scopes and
// resume policy are set explicitly because TestValidateActorTemplate
// exercises validation directly, without defaulting.
func validActorTemplate(mutations ...func(*ateapipb.ActorTemplate)) *ateapipb.ActorTemplate {
	template := &ateapipb.ActorTemplate{
		Metadata:   &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tmpl-a"},
		Containers: []*ateapipb.Container{{Name: "main", Image: "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			StorageLocation: "gs://my-bucket/snapshots",
			OnCommit:        ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
		},
		SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "gvisor-default"},
	}
	for _, m := range mutations {
		m(template)
	}
	return template
}

// gvisorDefaultLister returns a SandboxConfig lister seeded with the
// "gvisor-default" config that validActorTemplate names.
func gvisorDefaultLister(t *testing.T) listersv1alpha1.SandboxConfigLister {
	t.Helper()
	return sandboxConfigListerFor(t, []*atev1alpha1.SandboxConfig{{
		ObjectMeta: metav1.ObjectMeta{Name: "gvisor-default"},
		Spec: atev1alpha1.SandboxConfigSpec{
			SandboxClass:   atev1alpha1.SandboxClassGvisor,
			DefaultVersion: "v1",
			Versions: []atev1alpha1.SandboxVersionConfig{{
				Name:       "v1",
				PauseImage: "registry.k8s.io/pause@sha256:x",
				Assets:     testAssets(),
			}},
		},
	}})
}

// TestCreateActorTemplate_SandboxConfigChecks pins the create-time checks on
// the template's named SandboxConfig: it must exist and match the template's
// class, both FailedPrecondition — they depend on cluster state, and the
// lister may briefly lag a just-created config, so the error is retryable.
func TestCreateActorTemplate_SandboxConfigChecks(t *testing.T) {
	persistence := newTestPersistence(t)
	s := &RPCService{impl: newServiceImpl(persistence, nil), sandboxConfigLister: gvisorDefaultLister(t)}
	ctx := context.Background()
	if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "ns1"}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}

	tests := []struct {
		name     string
		sandbox  *ateapipb.SandboxConfig
		wantCode codes.Code
	}{{
		name:     "named config exists and matches",
		sandbox:  &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "gvisor-default"},
		wantCode: codes.OK,
	}, {
		name:     "empty config_name is rejected",
		sandbox:  &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM},
		wantCode: codes.InvalidArgument,
	}, {
		name:     "named config missing",
		sandbox:  &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "does-not-exist"},
		wantCode: codes.FailedPrecondition,
	}, {
		name:     "named config class mismatch",
		sandbox:  &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM, ConfigName: "gvisor-default"},
		wantCode: codes.FailedPrecondition,
	}}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
				tmpl.Metadata = &ateapipb.ResourceMetadata{Atespace: "ns1", Name: fmt.Sprintf("tmpl-%d", i)}
				tmpl.SandboxConfig = tt.sandbox
			})}
			_, err := s.CreateActorTemplate(ctx, req)
			if apierror.Code(err) != tt.wantCode {
				t.Errorf("CreateActorTemplate error = %v, want code %v", err, tt.wantCode)
			}
		})
	}
}

// TestCreateActorTemplate covers the atespace precondition: creation fails
// while the atespace is missing, and succeeds once the atespace exists.
func TestCreateActorTemplate(t *testing.T) {
	persistence := newTestPersistence(t)
	s := &RPCService{impl: newServiceImpl(persistence, nil), sandboxConfigLister: gvisorDefaultLister(t)}
	ctx := context.Background()
	req := func(atespace, name string) *ateapipb.CreateActorTemplateRequest {
		return &ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Metadata = &ateapipb.ResourceMetadata{Atespace: atespace, Name: name}
		})}
	}

	if _, err := s.CreateActorTemplate(ctx, req("ns-missing", "tmpl-a")); apierror.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActorTemplate in missing atespace = %v, want FailedPrecondition", err)
	}

	if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "ns1"}}); err != nil {
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

// TestCreateActorTemplateIgnoresServerOwnedFields pins the create contract:
// status on the request is dropped and new templates start with an empty
// status. The store persists whatever the handler hands it, so the handler is
// the only guard.
func TestCreateActorTemplateIgnoresServerOwnedFields(t *testing.T) {
	persistence := newTestPersistence(t)
	s := &RPCService{impl: newServiceImpl(persistence, nil), sandboxConfigLister: gvisorDefaultLister(t)}
	ctx := context.Background()

	if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "ns1"}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}

	in := validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
		tmpl.Metadata.Uid = "11111111-1111-1111-1111-111111111111"
		tmpl.Metadata.Version = 42
		tmpl.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"pool": "default"}}
		tmpl.Containers = []*ateapipb.Container{{Name: "main", Image: "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}}
		tmpl.Resources = &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "memory", Quantity: "1Gi"}}}
		// Server-owned status a client must not be able to set.
		tmpl.Status = &ateapipb.ActorTemplateStatus{
			GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
				GoldenTag: &ateapipb.ObjectRef{Atespace: "ate-golden", Name: "golden-tag"},
			},
		}
	})
	created, err := s.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: in})
	if err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}

	want := validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
		tmpl.Metadata.Version = 1
		tmpl.WorkerSelector = in.GetWorkerSelector()
		tmpl.Containers = in.GetContainers()
		tmpl.Resources = in.GetResources()
		tmpl.Status = &ateapipb.ActorTemplateStatus{}
	})
	if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
		t.Errorf("CreateActorTemplate response mismatch (-want +got):\n%s", diff)
	}
	if got := created.GetMetadata().GetUid(); got == "" || got == in.GetMetadata().GetUid() {
		t.Errorf("created uid = %q, want a fresh server-assigned uid", got)
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
			goldenRef := resources.ActorRef{Atespace: resources.GoldenActorAtespace, Name: tmpl.GetMetadata().GetUid()}
			actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: goldenRef.Atespace, Name: goldenRef.Name},
				ActorTemplate: templateRef.ToObjectRef(),
				Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
			})
			workflow, objects := newFinalizeWorkflow(persistence)
			actorURI := mustActorSnapshotURI(t, tmpl, actor, "snapshot")
			objects.PutSnapshot(t, actorURI, "manifest.json")
			actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
				s.ExternalSnapshot = &ateapipb.ExternalSnapshot{
					SnapshotUri:      actorURI.String(),
					ContentScope:     ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
					ActorTemplateUid: tmpl.GetMetadata().GetUid(),
				}
			})
			var tag *ateapipb.Tag
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
			svc := &RPCService{impl: newServiceImpl(persistence, nil), actorWorkflow: workflow, objectStore: objects}
			// The handler must request AnyState to clean up an active golden actor.
			mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
				s.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
			})
			if tt.actorDeleted {
				if _, err := workflow.DeleteActor(ctx, goldenRef, true, store.DeletePreconditions{}); err != nil {
					t.Fatal(err)
				}
			}
			if tt.tagDeleted {
				if _, err := svc.DeleteTag(ctx, &ateapipb.DeleteTagRequest{Tag: tagRef.ToObjectRef()}); err != nil {
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
			}
			req := &ateapipb.DeleteActorTemplateRequest{ActorTemplate: templateRef.ToObjectRef()}
			deleted, err := svc.DeleteActorTemplate(ctx, req)
			if tt.failPrefix != "" {
				if !errors.Is(err, errObjectStore) {
					t.Fatalf("DeleteActorTemplate = %v, want object storage error", err)
				}
				if _, err := persistence.GetActorTemplate(ctx, templateRef); err != nil {
					t.Fatalf("template lost after cleanup failure: %v", err)
				}
				if _, err := persistence.GetTag(ctx, tagRef); err != nil {
					t.Fatalf("tag lost after cleanup failure: %v", err)
				}
				_, actorErr := persistence.GetActor(ctx, goldenRef)
				if tt.wantActorAfterFailure && actorErr != nil || !tt.wantActorAfterFailure && !errors.Is(actorErr, store.ErrNotFound) {
					t.Fatalf("GetActor after failure = %v, want present %v", actorErr, tt.wantActorAfterFailure)
				}
				objects.OnDelete = nil
				deleted, err = svc.DeleteActorTemplate(ctx, req)
			}
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tmpl, deleted, protocmp.Transform()); diff != "" {
				t.Fatalf("deleted template mismatch (-want +got):\n%s", diff)
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
func seedSubstrateTemplate(t *testing.T, ctx context.Context, persistence store.Interface, name string) *ateapipb.ActorTemplate {
	t.Helper()
	created, err := persistence.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: name},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			StorageLocation: "gs://ate-snapshots/team-a/",
		},
		SandboxConfig: &ateapipb.SandboxConfig{
			SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR,
			ConfigName:   "gvisor",
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
		actor := &ateapipb.Actor{ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"}}
		got, err := resolveActorTemplate(ctx, persistence, actor)
		if err != nil {
			t.Fatalf("resolveActorTemplate: %v", err)
		}
		if got.GetMetadata().GetUid() != stored.GetMetadata().GetUid() {
			t.Errorf("template uid = %q, want the stored substrate template %q", got.GetMetadata().GetUid(), stored.GetMetadata().GetUid())
		}
	})

	t.Run("ref to a missing template is FailedPrecondition", func(t *testing.T) {
		actor := &ateapipb.Actor{ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "absent"}}
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
		actor        *ateapipb.Actor
		wantNotFound bool
	}{
		{"ref resolves", &ateapipb.Actor{ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"}}, false},
		{"ref to deleted template", &ateapipb.Actor{ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "gone"}}, true},
		{"no template named at all", &ateapipb.Actor{}, true},
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

	if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "ns1"}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	created, err := persistence.CreateActorTemplate(ctx, validActorTemplate())
	if err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}
	ref := resources.ActorTemplateRefFromActorTemplate(created)

	// A server-owned status write passes validation and bumps the version.
	updated, err := persistence.UpdateActorTemplate(ctx, ref, store.PreconditionFrom(created), func(tmpl *ateapipb.ActorTemplate) error {
		tmpl.Status = &ateapipb.ActorTemplateStatus{GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
			GoldenTag: &ateapipb.ObjectRef{Atespace: "ate-golden", Name: "golden-tag"},
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
	for name, mutate := range map[string]func(*ateapipb.ActorTemplate) error{
		"atespace": func(tmpl *ateapipb.ActorTemplate) error { tmpl.Metadata.Atespace = "ns2"; return nil },
		"name":     func(tmpl *ateapipb.ActorTemplate) error { tmpl.Metadata.Name = "tmpl-b"; return nil },
	} {
		if _, err := persistence.UpdateActorTemplate(ctx, ref, store.PreconditionFrom(updated), mutate); err == nil {
			t.Errorf("mutating %s succeeded, want error", name)
		}
	}

	// Server-assigned metadata edits are overwritten, not errors: the store
	// restores them from the stored value.
	reverted, err := persistence.UpdateActorTemplate(ctx, ref, store.PreconditionFrom(updated), func(tmpl *ateapipb.ActorTemplate) error {
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
