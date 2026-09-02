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
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

// validActorTemplate returns the smallest template that passes create
// validation; mutations tweak it per test case.
func validActorTemplate(mutations ...func(*ateapipb.ActorTemplate)) *ateapipb.ActorTemplate {
	template := &ateapipb.ActorTemplate{
		Metadata:        &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tmpl-a"},
		Containers:      []*ateapipb.Container{{Name: "main", Image: "example.com/app:v1"}},
		SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: "gs://my-bucket/snapshots"},
		SandboxConfig:   &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "gvisor-default"},
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
	req := func(atespace, name string) *ateapipb.CreateActorTemplateRequest {
		return &ateapipb.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
			tmpl.Metadata = &ateapipb.ResourceMetadata{Atespace: atespace, Name: name}
		})}
	}

	if _, err := s.CreateActorTemplate(ctx, req("ns-missing", "tmpl-a")); status.Code(err) != codes.FailedPrecondition {
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
	s := &RPCService{impl: newServiceImpl(persistence, nil)}
	ctx := context.Background()

	if _, err := persistence.CreateAtespace(ctx, &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "ns1"}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}

	in := validActorTemplate(func(tmpl *ateapipb.ActorTemplate) {
		tmpl.Metadata.Uid = "11111111-1111-1111-1111-111111111111"
		tmpl.Metadata.Version = 42
		tmpl.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"pool": "default"}}
		tmpl.Containers = []*ateapipb.Container{{Name: "main", Image: "example.com/app:v1"}}
		tmpl.SnapshotsConfig = &ateapipb.SnapshotsConfig{StorageLocation: "gs://my-bucket/snapshots"}
		tmpl.Resources = &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "memory", Quantity: "1Gi"}}}
		// Server-owned status a client must not be able to set.
		tmpl.Status = &ateapipb.ActorTemplateStatus{
			GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
				GoldenSnapshot: &ateapipb.ObjectRef{Atespace: "ate-golden", Name: "sneaky"},
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
		tmpl.SnapshotsConfig = in.GetSnapshotsConfig()
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

// seedSubstrateTemplate stores a minimal substrate ActorTemplate in team-a.
func seedSubstrateTemplate(t *testing.T, ctx context.Context, persistence store.Interface, name string) *ateapipb.ActorTemplate {
	t.Helper()
	stored, err := persistence.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: name},
		SnapshotsConfig: &ateapipb.SnapshotsConfig{
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
		if got := status.Code(err); got != codes.FailedPrecondition {
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
			GoldenSnapshot: &ateapipb.ObjectRef{Atespace: "ate-golden", Name: "snap-1"},
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

// TestActorTemplateObjectRef pins that snapshot and assignment records get a
// fresh copy of the reference, never the actor's own message.
func TestActorTemplateObjectRef(t *testing.T) {
	if got := actorTemplateObjectRef(&ateapipb.Actor{}); got != nil {
		t.Errorf("actorTemplateObjectRef(no ref) = %v, want nil", got)
	}
	actor := &ateapipb.Actor{ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "tmpl1"}}
	got := actorTemplateObjectRef(actor)
	if got == actor.GetActorTemplate() {
		t.Error("actorTemplateObjectRef aliases the actor's reference")
	}
	if got.GetAtespace() != "team-a" || got.GetName() != "tmpl1" {
		t.Errorf("actorTemplateObjectRef = %v, want team-a/tmpl1", got)
	}
}
