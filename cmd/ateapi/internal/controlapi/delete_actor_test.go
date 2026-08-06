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
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Delete addresses the actor by ref (atespace + id) and does not resolve the
// template/version, so only the ref identity is stamped.
func TestDeleteActor_StampsRefSpanIdentity(t *testing.T) {
	ns := namespaceForTest("ns-span-delete")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)
	if _, err := tc.service.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
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

func TestValidateDeleteActorRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.DeleteActorRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}},
		nil,
	}, {
		"missing actor",
		&ateapipb.DeleteActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.atespace",
		&ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Name: "id1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "atespace"), "")},
	}, {
		"invalid actor.atespace",
		&ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "NS1", Name: "id1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "")},
	}, {
		"missing actor.name",
		&ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateDeleteActorRequest(tt.req), tt.want)
		})
	}
}

func TestDeleteActor_StatusDeleting(t *testing.T) {
	ns := namespaceForTest("ns-delete-deleting")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)

	deletingActor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: testAtespace,
			Name:     "deleting-actor",
		},
		Status:                 ateapipb.Actor_STATUS_DELETING,
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}
	if _, err := tc.persistence.CreateActor(context.Background(), deletingActor); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}

	if _, err := tc.service.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "deleting-actor"},
	}); err != nil {
		t.Fatalf("DeleteActor on STATUS_DELETING actor failed: %v", err)
	}

	if _, err := tc.persistence.GetActor(context.Background(), resources.ActorRef{Atespace: testAtespace, Name: "deleting-actor"}); err == nil {
		t.Errorf("expected actor to be deleted, but it still exists")
	}
}

func TestDeleteActor_WrongStatus(t *testing.T) {
	ns := namespaceForTest("ns-delete-wrong-status")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)

	runningActor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: testAtespace,
			Name:     "running-actor",
		},
		Status:                 ateapipb.Actor_STATUS_RUNNING,
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}
	if _, err := tc.persistence.CreateActor(context.Background(), runningActor); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}

	_, err := tc.service.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "running-actor"},
	})
	if err == nil {
		t.Fatalf("expected DeleteActor on STATUS_RUNNING actor to fail, but it succeeded")
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
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)

	plugin := &failingVolumePlugin{}
	oldGlobalPlugin := globalVolumePlugin
	globalVolumePlugin = plugin
	defer func() { globalVolumePlugin = oldGlobalPlugin }()

	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: testAtespace,
			Name:     "multi-vol-actor",
		},
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		ActorVolumes: []*ateapipb.ExternalVolume{
			{VolumeName: "vol1", StorageVolumeId: "storage-vol-1", Status: ateapipb.ExternalVolume_STATUS_CREATED},
			{VolumeName: "vol2", StorageVolumeId: "storage-vol-2", Status: ateapipb.ExternalVolume_STATUS_CREATED},
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
