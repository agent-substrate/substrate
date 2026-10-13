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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateActorStatusCrash(t *testing.T) {
	crashPath := field.NewPath("status", "crash")
	withCrash := func(mutate ...func(*ateapipb.ActorCrash)) func(*ateapipb.Actor) {
		return withActorStatus(func(s *ateapipb.ActorStatus) {
			s.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
			s.Crash = &ateapipb.ActorCrash{
				Message:   crashMessageWorkerGone,
				CrashTime: &timestamppb.Timestamp{Seconds: 867},
			}
			for _, m := range mutate {
				m(s.Crash)
			}
		})
	}

	tests := []struct {
		name   string
		oldVal *ateapipb.Actor
		newVal *ateapipb.Actor
		want   field.ErrorList
	}{
		{
			name:   "set crash",
			oldVal: validActor(withActorStatus()),
			newVal: validActor(withCrash()),
		},
		{
			name:   "set empty crash",
			oldVal: validActor(withActorStatus()),
			newVal: validActor(withCrash(func(c *ateapipb.ActorCrash) { *c = ateapipb.ActorCrash{} })),
		},
		{
			name:   "clear crash",
			oldVal: validActor(withCrash()),
			newVal: validActor(withActorStatus()),
		},
		{
			name:   "replace crash",
			oldVal: validActor(withCrash()),
			newVal: validActor(withCrash(func(c *ateapipb.ActorCrash) {
				c.Message = crashMessageWorkerDraining
				c.CrashTime = &timestamppb.Timestamp{Seconds: 5309}
			})),
		},
		{
			name:   "message at max length",
			oldVal: validActor(withActorStatus()),
			newVal: validActor(withCrash(func(c *ateapipb.ActorCrash) { c.Message = strings.Repeat("x", 4096) })),
		},
		{
			name:   "message too long",
			oldVal: validActor(withActorStatus()),
			newVal: validActor(withCrash(func(c *ateapipb.ActorCrash) { c.Message = strings.Repeat("x", 4097) })),
			want:   field.ErrorList{field.TooLong(crashPath.Child("message"), nil, 4096).WithOrigin("maxLength")},
		},
		{
			name:   "truncated crash message fits",
			oldVal: validActor(withActorStatus()),
			newVal: validActor(withCrash(func(c *ateapipb.ActorCrash) {
				c.Message = newActorCrash("pause", strings.Repeat("x", 2*maxCrashMessageBytes)).GetMessage()
			})),
		},
		{
			// Unchanged fields are not revalidated on update, so a crash stored
			// before a limit was tightened does not block later status writes.
			name:   "unchanged overlong crash is not revalidated",
			oldVal: validActor(withCrash(func(c *ateapipb.ActorCrash) { c.Message = strings.Repeat("x", 4097) })),
			newVal: validActor(withCrash(func(c *ateapipb.ActorCrash) { c.Message = strings.Repeat("x", 4097) })),
		},
		{
			name:   "changed overlong crash is revalidated",
			oldVal: validActor(withCrash(func(c *ateapipb.ActorCrash) { c.Message = strings.Repeat("x", 4097) })),
			newVal: validActor(withCrash(func(c *ateapipb.ActorCrash) { c.Message = strings.Repeat("y", 4097) })),
			want:   field.ErrorList{field.TooLong(crashPath.Child("message"), nil, 4096).WithOrigin("maxLength")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, apivalidation.ValidateActorUpdate(context.Background(), nil, tt.newVal, tt.oldVal, true), tt.want)
		})
	}
}

func TestUpdateActor(t *testing.T) {
	const templateNS, templateName = "ns1", "tmpl1"

	tests := []struct {
		name     string
		stored   *ateapipb.Actor
		req      *ateapipb.Actor
		want     *ateapipb.Actor
		wantCode codes.Code
	}{
		{
			name:   "sets a worker_selector the stored actor does not have",
			stored: &ateapipb.Actor{},
			req: &ateapipb.Actor{
				ActorTemplate:  &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
				WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
			},
			want: &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}},
		},
		{
			name:   "overwrites an existing worker_selector",
			stored: &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "free"}}},
			req: &ateapipb.Actor{
				ActorTemplate:  &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
				WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
			},
			want: &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}},
		},
		{
			name:   "an omitted worker_selector is cleared",
			stored: &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "free"}}},
			req: &ateapipb.Actor{
				ActorTemplate: &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
			},
			want: &ateapipb.Actor{},
		},
		{
			name:   "SourceTag immutable field is kept",
			stored: &ateapipb.Actor{SourceTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"}},
			req: &ateapipb.Actor{
				ActorTemplate:  &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
				SourceTag:      &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"},
				WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
			},
			want: &ateapipb.Actor{
				SourceTag:      &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"},
				WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
			},
		},
		{
			name:   "changes to status in the request are ignored",
			stored: &ateapipb.Actor{},
			req: &ateapipb.Actor{
				ActorTemplate: &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
				Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
			},
			want: &ateapipb.Actor{},
		},
		{
			name:   "an omitted immutable field is rejected",
			stored: &ateapipb.Actor{SourceTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"}},
			req: &ateapipb.Actor{
				ActorTemplate: &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
				// Omitted SourceTag
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name:   "an immutable field the request rewrites is rejected",
			stored: &ateapipb.Actor{SourceTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"}},
			req: &ateapipb.Actor{
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "attacker-ns", Name: "attacker-tmpl"},
				SourceTag:     &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag2"},
			},
			wantCode: codes.InvalidArgument,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.stored.Metadata = &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID}
			tt.stored.ActorTemplate = &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName}
			tt.stored.Status = &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			}

			svc, created := rpcServiceWithActor(t, tt.stored)

			tt.req.Metadata = created.GetMetadata()
			updated, err := svc.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{Actor: tt.req})

			if tt.wantCode != codes.OK {
				if code := apierror.Code(err); code != tt.wantCode {
					t.Errorf("UpdateActor error = %v (code %v), want code %v", err, code, tt.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("UpdateActor failed: %v", err)
			}

			tt.want.Metadata = &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID, Version: 2}
			tt.want.ActorTemplate = &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName}
			tt.want.Status = &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			}
			if diff := cmp.Diff(tt.want, updated, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
				t.Errorf("UpdateActor response mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestUpdateActor_RepointTemplate covers the mutable actor_template ref: an
// update may point a suspended actor at a different template (it takes effect
// on the next ResumeActor), but the actor must be suspended, the new ref must
// resolve, and the replacement's sandbox config, volumes, and volume mounts
// must match the old template's.
func TestUpdateActor_RepointTemplate(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	storetest.MustCreateAtespace(t, ctx, persistence, testAtespace)
	// tmpl-a and tmpl-b are volume-compatible; tmpl-c mounts the data volume
	// elsewhere, tmpl-d declares an extra volume, and tmpl-e runs on a
	// different sandbox class.
	dataVolume := &ateapipb.Volume{Name: "data", DurableDir: &ateapipb.DurableDirVolumeSource{}}
	scratchVolume := &ateapipb.Volume{Name: "scratch", DurableDir: &ateapipb.DurableDirVolumeSource{}}
	gvisorConfig := &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "gvisor-default"}
	microvmConfig := &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM, ConfigName: "microvm"}
	templates := map[string]struct {
		mountPath     string
		volumes       []*ateapipb.Volume
		sandboxConfig *ateapipb.SandboxConfig
	}{
		"tmpl-a": {"/data", []*ateapipb.Volume{dataVolume}, gvisorConfig},
		"tmpl-b": {"/data", []*ateapipb.Volume{dataVolume}, gvisorConfig},
		"tmpl-c": {"/mnt/data", []*ateapipb.Volume{dataVolume}, gvisorConfig},
		"tmpl-d": {"/data", []*ateapipb.Volume{dataVolume, scratchVolume}, gvisorConfig},
		"tmpl-e": {"/data", []*ateapipb.Volume{dataVolume}, microvmConfig},
	}
	for name, tmpl := range templates {
		if _, err := persistence.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
			Containers: []*ateapipb.Container{{
				Name:         "main",
				Image:        "example.com/app:v1",
				VolumeMounts: []*ateapipb.VolumeMount{{Name: "data", MountPath: tmpl.mountPath}},
			}},
			Volumes:        tmpl.volumes,
			SnapshotConfig: &ateapipb.SnapshotConfig{StorageLocation: "gs://my-bucket/snapshots"},
			SandboxConfig:  tmpl.sandboxConfig,
		}); err != nil {
			t.Fatalf("creating template %s: %v", name, err)
		}
	}

	created := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-a"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	svc := &RPCService{impl: newServiceImpl(persistence, nil)}

	// Repointing at a template that does not exist is rejected.
	_, err := svc.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      created.GetMetadata(),
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "absent"},
	}})
	if got := apierror.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("UpdateActor to an absent template = %v, want FailedPrecondition (err: %v)", got, err)
	}

	// Repointing at a template with different volume mounts is rejected.
	_, err = svc.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      created.GetMetadata(),
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-c"},
	}})
	if got := apierror.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("UpdateActor to a template with different mounts = %v, want FailedPrecondition (err: %v)", got, err)
	}

	// Repointing at a template with different volumes is rejected.
	_, err = svc.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      created.GetMetadata(),
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-d"},
	}})
	if got := apierror.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("UpdateActor to a template with different volumes = %v, want FailedPrecondition (err: %v)", got, err)
	}

	// Repointing at a template naming a different SandboxConfig is rejected.
	_, err = svc.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      created.GetMetadata(),
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-e"},
	}})
	if got := apierror.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("UpdateActor to a template with a different sandbox config = %v, want FailedPrecondition (err: %v)", got, err)
	}

	// Repointing at an existing template with identical volumes and mounts
	// succeeds.
	updated, err := svc.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      created.GetMetadata(),
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-b"},
	}})
	if err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}
	if got, want := updated.GetActorTemplate().GetName(), "tmpl-b"; got != want {
		t.Errorf("updated actor_template.name = %q, want %q", got, want)
	}

	// When the old template no longer exists there is nothing left to
	// compare the sandbox config or volume layout against, so the repoint
	// only requires the new ref to resolve.
	orphan := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "orphan-actor"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-gone"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	repointed, err := svc.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      orphan.GetMetadata(),
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-e"},
	}})
	if err != nil {
		t.Fatalf("UpdateActor from a deleted template failed: %v", err)
	}
	if got, want := repointed.GetActorTemplate().GetName(), "tmpl-e"; got != want {
		t.Errorf("updated actor_template.name = %q, want %q", got, want)
	}

	// Repointing an actor that is not suspended is rejected, even at a
	// compatible template.
	running := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "running-actor"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-a"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})
	_, err = svc.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      running.GetMetadata(),
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-b"},
	}})
	if got := apierror.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("UpdateActor repointing a running actor = %v, want FailedPrecondition (err: %v)", got, err)
	}

	// An update that keeps the template ref is still allowed while running:
	// the suspended-state gate applies only to repoints.
	kept, err := svc.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: &ateapipb.Actor{
		Metadata:       running.GetMetadata(),
		ActorTemplate:  &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-a"},
		WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
	}})
	if err != nil {
		t.Fatalf("UpdateActor keeping the template on a running actor failed: %v", err)
	}
	if got, want := kept.GetActorTemplate().GetName(), "tmpl-a"; got != want {
		t.Errorf("updated actor_template.name = %q, want %q", got, want)
	}
}

// TestUpdateActor_RepointTemplateStorageLocation covers the storage location
// check on a repoint: an actor that owns an external snapshot may only move to
// a template storing snapshots under the same location, so deleting the actor
// still collects everything it wrote. Stored data the check cannot parse is a
// server fault: the error carries no status, which ServerUnaryInterceptor
// answers with INTERNAL.
func TestUpdateActor_RepointTemplateStorageLocation(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	storetest.MustCreateAtespace(t, ctx, persistence, testAtespace)
	const (
		sameLocation      = "gs://my-bucket/snapshots"
		differentLocation = "gs://other-bucket/snapshots"
	)
	// same-location-slash spells sameLocation differently but resolves to the
	// same prefix. corrupt has no bucket, which template validation rejects, so
	// only a write straight to the store can leave it behind.
	for name, location := range map[string]string{
		"same-location":       sameLocation,
		"same-location-slash": sameLocation + "/",
		"different-location":  differentLocation,
		"corrupt":             "gs:///snapshots",
	} {
		if _, err := persistence.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{
			Metadata:       &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
			SnapshotConfig: &ateapipb.SnapshotConfig{StorageLocation: location},
			SandboxConfig:  &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "gvisor-default"},
		}); err != nil {
			t.Fatalf("creating template %s: %v", name, err)
		}
	}

	snapshotOwnedByActor := func(t *testing.T, actor *ateapipb.Actor) string {
		t.Helper()
		uri, err := resources.NewActorSnapshotURI(sameLocation, testAtespace, actor.GetMetadata().GetUid(), "snap")
		if err != nil {
			t.Fatalf("NewActorSnapshotURI: %v", err)
		}
		return uri.String()
	}
	snapshotOwnedByTag := func(t *testing.T, _ *ateapipb.Actor) string {
		t.Helper()
		uri, err := resources.NewTagSnapshotURI(sameLocation, testAtespace, "0c6e2f4a-8b1d-4e57-a3f9-2d7c5b8e1a60")
		if err != nil {
			t.Fatalf("NewTagSnapshotURI: %v", err)
		}
		return uri.String()
	}
	unparseableSnapshot := func(*testing.T, *ateapipb.Actor) string {
		return sameLocation + "/not-a-snapshot"
	}

	tests := []struct {
		name      string
		template  string
		snapshot  func(*testing.T, *ateapipb.Actor) string
		repointTo string
		wantCode  codes.Code
	}{
		{
			name:      "no external snapshot moves freely",
			template:  "same-location",
			repointTo: "different-location",
			wantCode:  codes.OK,
		},
		{
			name:      "snapshot borrowed from a tag moves freely",
			template:  "same-location",
			snapshot:  snapshotOwnedByTag,
			repointTo: "different-location",
			wantCode:  codes.OK,
		},
		{
			name:      "owned snapshot moves to the same location spelled differently",
			template:  "same-location",
			snapshot:  snapshotOwnedByActor,
			repointTo: "same-location-slash",
			wantCode:  codes.OK,
		},
		{
			name:      "owned snapshot cannot move to another location",
			template:  "same-location",
			snapshot:  snapshotOwnedByActor,
			repointTo: "different-location",
			wantCode:  codes.FailedPrecondition,
		},
		{
			name:      "snapshot location repoint is checked even when old template is gone",
			template:  "gone",
			snapshot:  snapshotOwnedByActor,
			repointTo: "different-location",
			wantCode:  codes.FailedPrecondition,
		},
		{
			name:      "unparseable snapshot",
			template:  "same-location",
			snapshot:  unparseableSnapshot,
			repointTo: "different-location",
			wantCode:  codes.Internal,
		},
		{
			name:      "invalid storage location on the new template",
			template:  "same-location",
			snapshot:  snapshotOwnedByActor,
			repointTo: "corrupt",
			wantCode:  codes.Internal,
		},
	}
	svc := &RPCService{impl: newServiceImpl(persistence, nil)}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: fmt.Sprintf("actor-%d", i)},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: tt.template},
				Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
			})
			if tt.snapshot != nil {
				uri := tt.snapshot(t, actor)
				actor = mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
					s.LastAssignedGeneration = 1
					s.Snapshots = []*ateapipb.Snapshot{
						newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "snapshot", uri, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
					}
				})
			}

			updated, err := svc.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: &ateapipb.Actor{
				Metadata:      actor.GetMetadata(),
				ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: tt.repointTo},
			}})
			if got := apierror.Code(err); got != tt.wantCode {
				t.Fatalf("UpdateActor to %s = %v, want %v (err: %v)", tt.repointTo, got, tt.wantCode, err)
			}
			if err == nil {
				if got := updated.GetActorTemplate().GetName(); got != tt.repointTo {
					t.Errorf("updated actor_template.name = %q, want %q", got, tt.repointTo)
				}
			}
		})
	}
}

// TestValidateTemplateVolumesUnchanged exercises the volumes and
// per-container mount comparison applied when an actor is repointed at a
// replacement template.
func TestValidateTemplateVolumesUnchanged(t *testing.T) {
	dataVolume := &ateapipb.Volume{Name: "data", DurableDir: &ateapipb.DurableDirVolumeSource{}}
	scratchVolume := &ateapipb.Volume{Name: "scratch", DurableDir: &ateapipb.DurableDirVolumeSource{}}
	template := func(volumes []*ateapipb.Volume, containers ...*ateapipb.Container) *ateapipb.ActorTemplate {
		return &ateapipb.ActorTemplate{Volumes: volumes, Containers: containers}
	}
	container := func(name string, mounts ...*ateapipb.VolumeMount) *ateapipb.Container {
		return &ateapipb.Container{Name: name, Image: "example.com/app:v1", VolumeMounts: mounts}
	}
	dataMount := &ateapipb.VolumeMount{Name: "data", MountPath: "/data"}
	scratchMount := &ateapipb.VolumeMount{Name: "scratch", MountPath: "/scratch"}

	oneVolume := []*ateapipb.Volume{dataVolume}
	twoVolumes := []*ateapipb.Volume{dataVolume, scratchVolume}

	tests := []struct {
		name             string
		oldTmpl, newTmpl *ateapipb.ActorTemplate
		wantErr          bool
	}{{
		name:    "identical volumes and mounts",
		oldTmpl: template(oneVolume, container("main", dataMount)),
		newTmpl: template(oneVolume, container("main", dataMount)),
	}, {
		name:    "no volumes or mounts on either side",
		oldTmpl: template(nil, container("main")),
		newTmpl: template(nil, container("other")),
	}, {
		name:    "volume added",
		oldTmpl: template(oneVolume, container("main", dataMount)),
		newTmpl: template(twoVolumes, container("main", dataMount)),
		wantErr: true,
	}, {
		name:    "volume removed",
		oldTmpl: template(twoVolumes, container("main", dataMount)),
		newTmpl: template(oneVolume, container("main", dataMount)),
		wantErr: true,
	}, {
		name:    "volume renamed",
		oldTmpl: template(oneVolume, container("main", dataMount)),
		newTmpl: template([]*ateapipb.Volume{{Name: "data2", DurableDir: &ateapipb.DurableDirVolumeSource{}}}, container("main", dataMount)),
		wantErr: true,
	}, {
		name:    "volume source changed",
		oldTmpl: template(oneVolume, container("main", dataMount)),
		newTmpl: template([]*ateapipb.Volume{{Name: "data", Image: &ateapipb.ImageVolumeSource{Reference: "example.com/data@sha256:0f9c04b7387d13ba9d15ec50355f9ad533fee2e5ad25378753a30671f8f9b938"}}}, container("main", dataMount)),
		wantErr: true,
	}, {
		name:    "volume order changed",
		oldTmpl: template(twoVolumes, container("main", dataMount)),
		newTmpl: template([]*ateapipb.Volume{scratchVolume, dataVolume}, container("main", dataMount)),
		wantErr: true,
	}, {
		name:    "mount path changed",
		oldTmpl: template(oneVolume, container("main", dataMount)),
		newTmpl: template(oneVolume, container("main", &ateapipb.VolumeMount{Name: "data", MountPath: "/mnt/data"})),
		wantErr: true,
	}, {
		name:    "mount added",
		oldTmpl: template(twoVolumes, container("main", dataMount)),
		newTmpl: template(twoVolumes, container("main", dataMount, scratchMount)),
		wantErr: true,
	}, {
		name:    "mount removed",
		oldTmpl: template(oneVolume, container("main", dataMount)),
		newTmpl: template(oneVolume, container("main")),
		wantErr: true,
	}, {
		name:    "mounted container renamed",
		oldTmpl: template(oneVolume, container("main", dataMount)),
		newTmpl: template(oneVolume, container("renamed", dataMount)),
	}, {
		name:    "container added with mounts",
		oldTmpl: template(twoVolumes, container("main", dataMount)),
		newTmpl: template(twoVolumes, container("main", dataMount), container("sidecar", scratchMount)),
	}, {
		name:    "mount order changed",
		oldTmpl: template(twoVolumes, container("main", dataMount, scratchMount)),
		newTmpl: template(twoVolumes, container("main", scratchMount, dataMount)),
		wantErr: true,
	}, {
		name:    "mountless container renamed",
		oldTmpl: template(oneVolume, container("main", dataMount), container("sidecar")),
		newTmpl: template(oneVolume, container("main", dataMount), container("helper")),
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTemplateVolumesUnchanged(tt.oldTmpl, tt.newTmpl)
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Fatalf("validateVolumeMountsUnchanged() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				if got := apierror.Code(err); got != codes.FailedPrecondition {
					t.Errorf("status code = %v, want FailedPrecondition", got)
				}
			}
		})
	}
}

// TestUpdateActor_DeleteRecreateRace checks that an update is not applied
// if an actor was deleted and recreated during the update operation.
func TestUpdateActor_DeleteRecreateRace(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	actorRef := resources.ActorRef{Atespace: testAtespace, Name: testActorID}

	// Actor A: what the client reads, and what its uid precondition names.
	// Freshly created, so it sits at version 1.
	original := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl1"},
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPod: "pod-a"},
		},
	})

	// A concurrent client deletes A and recreates the same atespace/name as a
	// brand new actor B, in the window the handler used to leave open between
	// its own read and the store's WATCH.
	var recreated *ateapipb.Actor
	var err error
	racing := &conflictInjectingStore{
		Interface: persistence,
		inject: func() {
			if _, err := persistence.UpdateActor(ctx, actorRef, store.PreconditionFrom(original), func(toUpdate *ateapipb.Actor) error {
				toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_DELETING
				return nil
			}); err != nil {
				t.Fatalf("racing writer: mark deleting: %v", err)
			}
			if _, err := persistence.DeleteActor(ctx, actorRef, store.DeletePreconditions{}); err != nil {
				t.Fatalf("racing writer: DeleteActor: %v", err)
			}
			recreated, err = persistence.CreateActor(ctx, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl1"},
				Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
			})
			if err != nil {
				t.Fatalf("racing writer: recreate CreateActor: %v", err)
			}
		},
	}
	svc := &RPCService{impl: newServiceImpl(racing, nil)}

	// The client asserts "only update the actor with uid A".
	original.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}
	_, err = svc.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: original})
	if code := apierror.Code(err); code != codes.Aborted {
		t.Errorf("UpdateActor error = %v (code %v), want code Aborted: the actor holding uid %s was deleted mid-update",
			err, code, original.GetMetadata().GetUid())
	}

	stored, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if got, want := stored.GetMetadata().GetUid(), recreated.GetMetadata().GetUid(); got != want {
		t.Fatalf("stored uid = %s, want recreated actor's uid %s", got, want)
	}
	// The stored record must still be actor B as its creator left it. Any of A's
	// state showing up here is the clobber.
	if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("stored state = %v, want %v: recreated actor was overwritten with the deleted actor's state",
			got, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	}
	if got := stored.GetStatus().GetWorkerAssignment(); got != nil {
		t.Errorf("stored worker_assignment = %v, want nil: recreated actor inherited the deleted actor's worker", got)
	}
	if got := stored.GetWorkerSelector(); got != nil {
		t.Errorf("stored worker_selector = %v, want nil: update meant for the deleted actor was applied", got)
	}
}

// TestUpdateActor_ConcurrentDisjointUpdates checks that a concurrent write is
// reported even when it touched a field the update does not. The version guards
// the whole actor, not a single field, so the server cannot know the two
// writes commute: it reports the conflict and leaves reconciling to the client.
func TestUpdateActor_ConcurrentDisjointUpdates(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	actorRef := resources.ActorRef{Atespace: testAtespace, Name: testActorID}

	original := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl1"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})

	// A suspend workflow bumps state (a field that a later update operation will not touch)
	// inside the handler's read-modify-write window.
	racing := &conflictInjectingStore{
		Interface: persistence,
		inject: func() {
			if _, err := persistence.UpdateActor(ctx, actorRef, store.PreconditionFrom(original), func(toUpdate *ateapipb.Actor) error {
				toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDING
				return nil
			}); err != nil {
				t.Fatalf("racing writer: mark suspending: %v", err)
			}
		},
	}
	svc := &RPCService{impl: newServiceImpl(racing, nil)}

	// Update operation is changing the worker_selector field, not the actor's state (like the concurrent op)
	// This update must fail: the racing update bumped the version.
	original.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}
	_, err := svc.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: original})
	if code := apierror.Code(err); code != codes.Aborted {
		t.Errorf("UpdateActor error = %v (code %v), want code Aborted: the guarded version moved under the update", err, code)
	}

	stored, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	// The concurrent writer's field survives; the rejected update wrote nothing.
	if got := stored.GetWorkerSelector(); got != nil {
		t.Errorf("stored worker_selector = %v, want nil: the rejected update was applied anyway", got)
	}
	if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
		t.Errorf("stored state = %v, want %v: the concurrent writer's field must survive", got, ateapipb.ActorState_ACTOR_STATE_SUSPENDING)
	}
}

// validActor returns a minimal Actor which should pass input validation.
func validActor(mods ...func(*ateapipb.Actor)) *ateapipb.Actor {
	a := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl1"},
	}
	for _, m := range mods {
		m(a)
	}
	return a
}

// withActorStatus returns a modifier func (see validActor) which sets the
// actor's status to a valid value.
func withActorStatus(mods ...func(*ateapipb.ActorStatus)) func(*ateapipb.Actor) {
	return func(a *ateapipb.Actor) {
		a.Status = &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		}
		for _, m := range mods {
			m(a.Status)
		}
	}
}

// rpcServiceWithActor seeds one actor in a PostgreSQL-backed store and returns an
// RPCService over it.
func rpcServiceWithActor(t *testing.T, actor *ateapipb.Actor) (*RPCService, *ateapipb.Actor) {
	t.Helper()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	created := storetest.MustCreateActor(t, context.Background(), persistence, actor)
	return &RPCService{impl: newServiceImpl(persistence, nil)}, created
}

func TestCreateActor_GoldenTagDefault(t *testing.T) {
	for _, scenario := range []string{"default", "explicit tag", "own snapshot", "missing", "pending", "wrong template", "data scope"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := t.Context()
			persistence := newTestPersistence(t)
			storetest.MustCreateAtespace(t, ctx, persistence, "team-a")
			storetest.MustCreateAtespace(t, ctx, persistence, resources.GoldenActorAtespace)
			tmpl := seedSubstrateTemplate(t, ctx, persistence, "tmpl")
			ref := &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: "golden"}
			tag := &ateapipb.Tag{
				Metadata:    &ateapipb.ResourceMetadata{Atespace: ref.Atespace, Name: ref.Name},
				SourceActor: ref,
				Scope:       ateapipb.TagScope_TAG_SCOPE_PUBLISHED,
				Status: &ateapipb.TagStatus{
					ActorTemplateUid: tmpl.GetMetadata().GetUid(),
					Snapshot:         newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, tmpl.GetMetadata().GetUid(), someActorUID, "gs://bucket/atespaces/ate-golden/tags/"+someActorUID, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				},
			}
			wantCode := codes.OK
			switch scenario {
			case "missing":
				wantCode = codes.FailedPrecondition
			case "pending":
				tag.Status.Snapshot = nil
				wantCode = codes.FailedPrecondition
			case "wrong template":
				tag.Status.ActorTemplateUid = "other"
				wantCode = codes.FailedPrecondition
			case "data scope":
				tag.Status.Snapshot.Fidelity = ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
				wantCode = codes.FailedPrecondition
			}
			if scenario != "missing" {
				if _, err := persistence.CreateTag(ctx, tag); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := persistence.UpdateActorTemplate(ctx, resources.ActorTemplateRefFromActorTemplate(tmpl), store.PreconditionFrom(tmpl), func(db *ateapipb.ActorTemplate) error {
				db.Status = &ateapipb.ActorTemplateStatus{GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{GoldenTag: ref}}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			actor := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor"}, ActorTemplate: resources.ActorTemplateRefFromActorTemplate(tmpl).ToObjectRef()}
			if scenario == "explicit tag" {
				tag.Metadata.Name = "explicit"
				tag.Status.Snapshot = newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, tmpl.GetMetadata().GetUid(), "explicit", "gs://bucket/atespaces/ate-golden/tags/explicit", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
				if _, err := persistence.CreateTag(ctx, tag); err != nil {
					t.Fatal(err)
				}
				actor.SourceTag = &ateapipb.ObjectRef{Atespace: ref.Atespace, Name: "explicit"}
			}
			svc := &ServiceImpl{store: persistence}
			created, err := svc.CreateActor(ctx, actor)
			if apierror.Code(err) != wantCode {
				t.Fatalf("CreateActor = %v, want %v", err, wantCode)
			}
			if err != nil {
				return
			}
			gotSnap := findLatestDurableSnapshot(created.GetStatus(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
			if gotSnap.GetDurableSnapshot().GetObject().GetSnapshotUri() != tagDurableSnapshotURI(tag, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED) || gotSnap.GetActorTemplateUid() != tmpl.GetMetadata().GetUid() {
				t.Fatalf("incorrect initial status: %v", created.GetStatus())
			}
			if scenario == "own snapshot" {
				uri, err := resources.NewActorSnapshotURI(tmpl.GetSnapshotConfig().GetStorageLocation(), "team-a", created.GetMetadata().GetUid(), "snapshot")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := persistence.UpdateActor(ctx, resources.ActorRefFromActor(created), store.PreconditionFrom(created), func(db *ateapipb.Actor) error {
					db.Status.LastAssignedGeneration = 1
					db.Status.Snapshots = []*ateapipb.Snapshot{
						newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, tmpl.GetMetadata().GetUid(), uri.Name(), uri.String(), ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			workflow := &ActorWorkflow{store: persistence}
			_, _, src, err := workflow.loadActorForResume(ctx, resources.ActorRefFromActor(created))
			if err != nil {
				t.Fatal(err)
			}
			if src.SnapshotURI.IsZero() {
				t.Fatalf("missing snapshot source for %s", scenario)
			}
		})
	}
}

type fakeActorServiceStore struct {
	serviceStore
	actors map[resources.ActorRef]*ateapipb.Actor
}

func (f *fakeActorServiceStore) GetActor(_ context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
	a, ok := f.actors[actorRef]
	if !ok {
		return nil, store.ErrNotFound
	}
	return a, nil
}

func TestMintActorCertificate(t *testing.T) {
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{
			State: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{{}},
			},
		},
	})

	actorUID := "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21"
	created := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID, Uid: actorUID},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl-1"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	}
	fakeStore := &fakeActorServiceStore{
		actors: map[resources.ActorRef]*ateapipb.Actor{
			{Atespace: testAtespace, Name: testActorID}: created,
		},
	}

	ca, err := localca.GenerateCA("1", localca.KeyTypeECDSAP256, 24*time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	caPool := &localca.ConcretePool{
		CAs:              []*localca.CA{ca},
		ActiveForSigning: "1",
	}

	svc := &RPCService{
		impl:          fakeStore,
		actorIDCAPool: caPool,
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatalf("CreateCertificateRequest: %v", err)
	}

	resp, err := svc.MintActorCertificate(ctx, &ateapipb.MintActorCertificateRequest{
		Actor:                     &ateapipb.ObjectRef{Atespace: testAtespace, Name: testActorID},
		ActorUid:                  created.GetMetadata().GetUid(),
		CertificateSigningRequest: csr,
	})
	if err != nil {
		t.Fatalf("MintActorCertificate() failed: %v", err)
	}

	chain := resp.GetActorCertificates()
	if len(chain) == 0 {
		t.Fatal("MintActorCertificate() returned empty chain")
	}

	leaf, err := x509.ParseCertificate(chain[0])
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}
	if !key.PublicKey.Equal(leaf.PublicKey) {
		t.Error("leaf public key does not match CSR key")
	}

	wantURI := fmt.Sprintf("spiffe://substrate-actor.local/actor/%s/%s", testAtespace, testActorID)
	if len(leaf.URIs) != 1 || leaf.URIs[0].String() != wantURI {
		t.Errorf("leaf URIs = %v, want [%s]", leaf.URIs, wantURI)
	}

	identity, err := substratex509.ActorIdentityFromCertificate(leaf)
	if err != nil {
		t.Fatalf("ActorIdentityFromCertificate: %v", err)
	}
	wantIdentity := &substratex509.ActorIdentity{
		Atespace:  testAtespace,
		ActorName: testActorID,
		ActorUid:  created.GetMetadata().GetUid(),
	}
	if diff := cmp.Diff(wantIdentity, identity); diff != "" {
		t.Errorf("ActorIdentity mismatch (-want +got):\n%s", diff)
	}
}
