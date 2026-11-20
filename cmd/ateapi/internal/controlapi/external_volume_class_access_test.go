// Copyright 2026 The Agent Substrate Authors
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
	"slices"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/authz"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volume"
)

// fakeSubjectChecker allows the atespaces in allowed to reference every
// ExternalVolumeClass, denies everything else, and records each check.
type fakeSubjectChecker struct {
	allowed []string
	checks  []string
}

func (f *fakeSubjectChecker) CheckSubject(_ context.Context, subject, relation, object string) error {
	f.checks = append(f.checks, subject+" "+relation+" "+object)
	if relation == authz.RelationCanReference && slices.Contains(f.allowed, subject) {
		return nil
	}
	return apierror.PermissionDenied("%s lacks %s on %s", subject, relation, object)
}

// createEVCTemplate stores a template in atespace that provisions the volume
// "data" from the ExternalVolumeClass "gold".
func createEVCTemplate(t *testing.T, persistence store.Interface, atespace, name string) {
	t.Helper()
	if _, err := persistence.CreateActorTemplate(context.Background(), &ateapi.ActorTemplate{
		Metadata: &ateapi.ResourceMetadata{Atespace: atespace, Name: name},
		Containers: []*ateapi.Container{{
			Name:         "main",
			Image:        "example.com/app:v1",
			VolumeMounts: []*ateapi.VolumeMount{{Name: "data", MountPath: "/data"}},
		}},
		Volumes: []*ateapi.Volume{{
			Name: "data",
			External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
				ExternalVolumeClass: &ateapi.ObjectRef{Name: "gold"},
				CapacityBytes:       1 << 30,
			}},
		}},
		Resources: &ateapi.ResourceRequirements{Limits: resources.CPUMemory(1000, 512<<20)},
	}); err != nil {
		t.Fatalf("creating template %s/%s: %v", atespace, name, err)
	}
}

func createGoldClass(t *testing.T, persistence store.Interface) {
	t.Helper()
	if _, err := persistence.CreateExternalVolumeClass(context.Background(), &ateapi.ExternalVolumeClass{
		Metadata:   &ateapi.ResourceMetadata{Name: "gold"},
		DriverName: "mock",
	}); err != nil {
		t.Fatalf("CreateExternalVolumeClass failed: %v", err)
	}
}

const goldReference = "atespace:actors can_reference external_volume_class:gold"

// CreateActor checks the actor's atespace, which can differ from the
// template's, against every class the template uses.
func TestCreateActor_ExternalVolumeClassAccess(t *testing.T) {
	preview.InitForTest(t, "ExternalVolumes")
	for _, tt := range []struct {
		name     string
		allowed  []string
		wantCode codes.Code
	}{
		{name: "denied", wantCode: codes.PermissionDenied},
		{name: "allowed", allowed: []string{"atespace:actors"}, wantCode: codes.OK},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			persistence := newTestPersistence(t)
			storetest.MustCreateAtespace(t, ctx, persistence, "templates")
			storetest.MustCreateAtespace(t, ctx, persistence, "actors")
			createGoldClass(t, persistence)
			createEVCTemplate(t, persistence, "templates", "tmpl")

			checker := &fakeSubjectChecker{allowed: tt.allowed}
			impl := newServiceImpl(persistence)
			impl.clusterSnapshotLocation = "gs://cluster-bucket/"
			impl.subjectChecker = checker
			svc := &RPCService{impl: impl}

			actor, err := svc.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Atespace: "actors", Name: "alpha"},
				ActorTemplate: &ateapi.ObjectRef{Atespace: "templates", Name: "tmpl"},
			}})
			if got := apierror.Code(err); got != tt.wantCode {
				t.Fatalf("CreateActor code = %v, want %v (err: %v)", got, tt.wantCode, err)
			}
			if !slices.Equal(checker.checks, []string{goldReference}) {
				t.Errorf("checks = %q, want [%q]", checker.checks, goldReference)
			}
			if err == nil {
				if got := actor.GetStatus().GetExternalVolumes().GetVolumes(); len(got) != 1 || got[0].GetVolumeType() != "mock" {
					t.Errorf("actor volumes = %v, want one volume of type mock", got)
				}
			}
		})
	}
}

// Repointing an actor at another template checks the actor's atespace
// against the classes the new template uses.
func TestUpdateActor_RepointExternalVolumeClassAccess(t *testing.T) {
	for _, tt := range []struct {
		name     string
		allowed  []string
		wantCode codes.Code
	}{
		{name: "denied", wantCode: codes.PermissionDenied},
		{name: "allowed", allowed: []string{"atespace:actors"}, wantCode: codes.OK},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			persistence := newTestPersistence(t)
			storetest.MustCreateAtespace(t, ctx, persistence, "actors")
			createGoldClass(t, persistence)
			createEVCTemplate(t, persistence, "actors", "tmpl-a")
			createEVCTemplate(t, persistence, "actors", "tmpl-b")
			created := storetest.MustCreateActor(t, ctx, persistence, &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Atespace: "actors", Name: "alpha"},
				ActorTemplate: &ateapi.ObjectRef{Atespace: "actors", Name: "tmpl-a"},
				Status:        &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED},
			})

			checker := &fakeSubjectChecker{allowed: tt.allowed}
			impl := newServiceImpl(persistence)
			impl.clusterSnapshotLocation = "gs://cluster-bucket/"
			impl.subjectChecker = checker
			svc := &RPCService{impl: impl}

			_, err := svc.UpdateActor(ctx, &ateapi.UpdateActorRequest{Actor: &ateapi.Actor{
				Metadata:      created.GetMetadata(),
				ActorTemplate: &ateapi.ObjectRef{Atespace: "actors", Name: "tmpl-b"},
			}})
			if got := apierror.Code(err); got != tt.wantCode {
				t.Fatalf("UpdateActor code = %v, want %v (err: %v)", got, tt.wantCode, err)
			}
			if !slices.Equal(checker.checks, []string{goldReference}) {
				t.Errorf("checks = %q, want [%q]", checker.checks, goldReference)
			}
		})
	}
}

// First provisioning rechecks the class of each unprovisioned volume, so a
// class deleted and recreated under the same name, which starts with no
// grants, cannot provision for templates that name it.
func TestEnsureVolumesCreated_ExternalVolumeClassAccess(t *testing.T) {
	for _, tt := range []struct {
		name        string
		allowed     []string
		wantCode    codes.Code
		wantCreates int
	}{
		{name: "denied", wantCode: codes.FailedPrecondition},
		{name: "allowed", allowed: []string{"atespace:actors"}, wantCode: codes.OK, wantCreates: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			persistence := newTestPersistence(t)
			storetest.MustCreateAtespace(t, ctx, persistence, "actors")
			createGoldClass(t, persistence)
			createEVCTemplate(t, persistence, "actors", "tmpl")
			tmpl, err := persistence.GetActorTemplate(ctx, resources.ActorTemplateRef{Atespace: "actors", Name: "tmpl"})
			if err != nil {
				t.Fatalf("GetActorTemplate: %v", err)
			}
			actorRef := resources.ActorRef{Atespace: "actors", Name: "alpha"}
			actor := storetest.MustCreateActor(t, ctx, persistence, &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Atespace: "actors", Name: "alpha"},
				ActorTemplate: &ateapi.ObjectRef{Atespace: "actors", Name: "tmpl"},
				Status: &ateapi.ActorStatus{
					State:           ateapi.ActorState_ACTOR_STATE_SUSPENDED,
					ExternalVolumes: &ateapi.ExternalVolumes{Volumes: []*ateapi.ExternalVolumeStatus{{Name: "data", VolumeType: "mock"}}},
				},
			})

			plugin := &createCountingVolumePlugin{}
			w := &ActorWorkflow{
				store:          persistence,
				pluginRegistry: &mockPluginRegistry{plugins: map[string]volume.VolumePluginControlPlane{"mock": plugin}},
				subjectChecker: &fakeSubjectChecker{allowed: tt.allowed},
			}
			_, err = w.ensureVolumesCreated(ctx, actorRef, actor, tmpl)
			if got := apierror.Code(err); got != tt.wantCode {
				t.Fatalf("ensureVolumesCreated code = %v, want %v (err: %v)", got, tt.wantCode, err)
			}
			if err != nil && !strings.Contains(err.Error(), `may not use ExternalVolumeClass "gold"`) {
				t.Errorf("ensureVolumesCreated error = %v, want it to name the class", err)
			}
			if plugin.creates != tt.wantCreates {
				t.Errorf("CreateVolume calls = %d, want %d", plugin.creates, tt.wantCreates)
			}
		})
	}
}
