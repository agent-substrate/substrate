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
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/volume"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/wait"
)

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
				Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
				ActorTemplateNamespace: ns,
				ActorTemplateName:      "tmpl1",
			},
		}); err != nil {
			t.Fatalf("CreateActor: %v", err)
		}
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, testAtespace)
	assertSpanStr(t, attrs, ateattr.ActorNameKey, testActorID)
	assertSpanStr(t, attrs, ateattr.TemplateNameKey, "tmpl1")
	assertSpanStr(t, attrs, ateattr.TemplateNamespaceKey, ns)
	// uid is server-assigned on create, so assert it is present and non-empty
	// rather than a fixed value.
	if v, ok := attrs[ateattr.ActorUIDKey]; !ok || v.Type() != attribute.STRING || v.AsString() == "" {
		t.Errorf("%s = %v, want non-empty server-assigned uid", ateattr.ActorUIDKey, v.Emit())
	}
	if v, ok := attrs[ateattr.ActorVersionKey]; !ok || v.Type() != attribute.INT64 || v.AsInt64() != 1 {
		t.Errorf("%s = %v, want int64 1", ateattr.ActorVersionKey, v.Emit())
	}
}

func TestValidateCreateActorRequest(t *testing.T) {
	validActor := func(mutate func(*ateapipb.Actor)) *ateapipb.CreateActorRequest {
		a := &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "id1"},
			ActorTemplateNamespace: "ns1",
			ActorTemplateName:      "tmpl1",
		}
		if mutate != nil {
			mutate(a)
		}
		return &ateapipb.CreateActorRequest{Actor: a}
	}

	tests := []struct {
		name string
		req  *ateapipb.CreateActorRequest
		want field.ErrorList
	}{{
		"valid",
		validActor(nil),
		nil,
	}, {
		"missing actor",
		&ateapipb.CreateActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.metadata.atespace",
		validActor(func(a *ateapipb.Actor) { a.Metadata.Atespace = "" }),
		field.ErrorList{field.Required(field.NewPath("actor", "metadata", "atespace"), "")},
	}, {
		"invalid actor.metadata.atespace",
		validActor(func(a *ateapipb.Actor) { a.Metadata.Atespace = "NS1" }),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "atespace"), "NS1", "")},
	}, {
		"missing actor.metadata.name",
		validActor(func(a *ateapipb.Actor) { a.Metadata.Name = "" }),
		field.ErrorList{field.Required(field.NewPath("actor", "metadata", "name"), "")},
	}, {
		"invalid actor.metadata.name",
		validActor(func(a *ateapipb.Actor) { a.Metadata.Name = "ID1" }),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "name"), "ID1", "")},
	}, {
		"missing actor_template_namespace",
		validActor(func(a *ateapipb.Actor) { a.ActorTemplateNamespace = "" }),
		field.ErrorList{field.Required(field.NewPath("actor", "actor_template_namespace"), "")},
	}, {
		"invalid actor_template_namespace",
		validActor(func(a *ateapipb.Actor) { a.ActorTemplateNamespace = "invalid value" }),
		field.ErrorList{field.Invalid(field.NewPath("actor", "actor_template_namespace"), "invalid value", "")},
	}, {
		"missing actor_template_name",
		validActor(func(a *ateapipb.Actor) { a.ActorTemplateName = "" }),
		field.ErrorList{field.Required(field.NewPath("actor", "actor_template_name"), "")},
	}, {
		"invalid actor_template_name",
		validActor(func(a *ateapipb.Actor) { a.ActorTemplateName = "invalid value" }),
		field.ErrorList{field.Invalid(field.NewPath("actor", "actor_template_name"), "invalid value", "")},
	}, {
		"worker_selector with nil match_labels",
		validActor(func(a *ateapipb.Actor) { a.WorkerSelector = &ateapipb.Selector{} }),
		nil,
	}, {
		"worker_selector with empty match_labels",
		validActor(func(a *ateapipb.Actor) { a.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{}} }),
		nil,
	}, {
		"valid worker_selector",
		validActor(func(a *ateapipb.Actor) {
			a.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "1"}}
		}),
		nil,
	}, {
		"worker_selector with exactly max match_labels",
		validActor(func(a *ateapipb.Actor) { a.WorkerSelector = &ateapipb.Selector{MatchLabels: selectorLabelsOfSize(10)} }),
		nil,
	}, {
		"invalid worker_selector label key",
		validActor(func(a *ateapipb.Actor) {
			a.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"bad key!": "1"}}
		}),
		field.ErrorList{field.Invalid(field.NewPath("actor", "worker_selector", "match_labels").Key("bad key!"), "bad key!", "")},
	}, {
		"invalid worker_selector label value",
		validActor(func(a *ateapipb.Actor) {
			a.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "not valid!"}}
		}),
		field.ErrorList{field.Invalid(field.NewPath("actor", "worker_selector", "match_labels").Key("tier"), "not valid!", "")},
	}, {
		"too many worker_selector.match_labels",
		validActor(func(a *ateapipb.Actor) { a.WorkerSelector = &ateapipb.Selector{MatchLabels: selectorLabelsOfSize(11)} }),
		field.ErrorList{field.TooMany(field.NewPath("actor", "worker_selector", "match_labels"), 11, 10)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateCreateActorRequest(tt.req), tt.want)
		})
	}
}

func TestCreateActor_RejectsDifferentTemplateForDataSnapshot(t *testing.T) {
	ns := namespaceForTest("ns-data-snapshot-template")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)
	createTemplateWithSelector(t, tc, ns, "tmpl2", nil)

	tmpl, err := tc.actorTemplateLister.ActorTemplates(ns).Get("tmpl1")
	if err != nil {
		t.Fatalf("Get source ActorTemplate: %v", err)
	}
	snapshot, err := tc.persistence.CreateActorSnapshot(context.Background(), &ateapipb.ActorSnapshot{
		Metadata:         &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "data-snapshot"},
		SourceActor:      &ateapipb.ObjectRef{Atespace: testAtespace, Name: "source"},
		ActorTemplateUid: string(tmpl.GetUID()),
		ContentScope:     ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
		SnapshotUri:      "gs://snapshots/snapshots/" + testAtespace + "/data-snapshot",
	})
	if err != nil {
		t.Fatalf("CreateActorSnapshot: %v", err)
	}
	if _, err := tc.persistence.CreateActorSnapshotTag(context.Background(), testAtespace, snapshot.GetMetadata().GetName(), &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "data-snapshot"},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	}); err != nil {
		t.Fatalf("CreateActorSnapshotTag: %v", err)
	}

	_, err = tc.service.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "clone"},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl2",
			SourceSnapshot:         &ateapipb.ActorSnapshotSource{Tag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "data-snapshot"}},
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
	template, err := tc.substrateClient.ApiV1alpha1().ActorTemplates(ns).Create(context.Background(), &atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tmpl1", Namespace: ns},
		Spec: atev1alpha1.ActorTemplateSpec{
			SnapshotsConfig: atev1alpha1.SnapshotsConfig{Location: "gs://snapshots"},
			Containers: []atev1alpha1.Container{{
				Name: "main", Image: "main@sha256:abc", VolumeMounts: []atev1alpha1.VolumeMount{{Name: "data", MountPath: "/data"}},
			}},
			Volumes: []atev1alpha1.Volume{{
				Name: "data",
				VolumeSource: atev1alpha1.VolumeSource{ExternalVolumeTemplate: &atev1alpha1.ExternalVolumeTemplate{
					Capacity: resource.MustParse("1Gi"), StorageClassName: "standard",
				}},
			}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create ActorTemplate: %v", err)
	}
	if err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		got, err := tc.actorTemplateLister.ActorTemplates(ns).Get("tmpl1")
		return err == nil && len(got.Spec.Volumes) == 1, nil
	}); err != nil {
		t.Fatalf("wait for ActorTemplate update: %v", err)
	}
	snapshot, err := tc.persistence.CreateActorSnapshot(context.Background(), &ateapipb.ActorSnapshot{
		Metadata:         &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "external-volume-snapshot"},
		ActorTemplateUid: string(template.GetUID()),
		SnapshotUri:      "gs://snapshots/snapshots/" + testAtespace + "/external-volume-snapshot",
	})
	if err != nil {
		t.Fatalf("CreateActorSnapshot: %v", err)
	}
	tagRef := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "external-volume-snapshot"}
	if _, err := tc.persistence.CreateActorSnapshotTag(context.Background(), testAtespace, snapshot.GetMetadata().GetName(), &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: tagRef.GetAtespace(), Name: tagRef.GetName()},
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
	}); err != nil {
		t.Fatalf("CreateActorSnapshotTag: %v", err)
	}

	_, err = tc.service.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "clone"},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
			SourceSnapshot:         &ateapipb.ActorSnapshotSource{Tag: tagRef},
		},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CreateActor status = %v, want FailedPrecondition", status.Code(err))
	}
}

func TestValidateGetActorRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.GetActorRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}},
		nil,
	}, {
		"missing actor",
		&ateapipb.GetActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.atespace",
		&ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Name: "id1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "atespace"), "")},
	}, {
		"invalid actor.atespace",
		&ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "NS1", Name: "id1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "")},
	}, {
		"missing actor.name",
		&ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateGetActorRequest(tt.req), tt.want)
		})
	}
}

func TestValidateListActorsRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.ListActorsRequest
		want field.ErrorList
	}{{
		"valid, atespace scoped",
		&ateapipb.ListActorsRequest{Atespace: "ns1"},
		nil,
	}, {
		// Empty atespace means "all atespaces" (kubectl ate get actors -A).
		"valid, empty atespace means all atespaces",
		&ateapipb.ListActorsRequest{},
		nil,
	}, {
		"invalid atespace",
		&ateapipb.ListActorsRequest{Atespace: "NS1"},
		field.ErrorList{field.Invalid(field.NewPath("atespace"), "NS1", "")},
	}, {
		"valid, positive page_size",
		&ateapipb.ListActorsRequest{Atespace: "ns1", PageSize: 10},
		nil,
	}, {
		"negative page_size",
		&ateapipb.ListActorsRequest{Atespace: "ns1", PageSize: -1},
		field.ErrorList{field.Invalid(field.NewPath("page_size"), int32(-1), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateListActorsRequest(tt.req), tt.want)
		})
	}
}

func TestValidateUpdateActorRequest(t *testing.T) {
	mutableFields := []string{
		"worker_selector",
		"worker_selector.match_labels",
	}

	tests := []struct {
		name string
		req  *ateapipb.UpdateActorRequest
		want field.ErrorList
	}{{
		"valid",
		updateActorReq(),
		nil,
	}, {
		"missing actor",
		&ateapipb.UpdateActorRequest{UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"worker_selector"}}},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.metadata.atespace",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Atespace = "" })),
		field.ErrorList{field.Required(field.NewPath("actor", "metadata", "atespace"), "")},
	}, {
		"invalid actor.metadata.atespace",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Atespace = "NS1" })),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "atespace"), "NS1", "")},
	}, {
		"missing actor.metadata.name",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Name = "" })),
		field.ErrorList{field.Required(field.NewPath("actor", "metadata", "name"), "")},
	}, {
		"invalid actor.metadata.name",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Name = "ID1" })),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "name"), "ID1", "")},
	}, {
		"valid actor.metadata.uid precondition",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) {
			m.Uid = "2a5f8c1e-9b3d-4f7a-8e6c-1d0b4a7f2e93"
		})),
		nil,
	}, {
		"invalid actor.metadata.uid precondition",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Uid = "not-a-uuid" })),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "uid"), "not-a-uuid", "")},
	}, {
		"valid actor.metadata.version precondition",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Version = 7 })),
		nil,
	}, {
		"negative actor.metadata.version precondition",
		updateActorReq(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Version = -1 })),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "version"), int64(-1), "")},
	}, {
		"missing update_mask",
		updateActorReq(func(req *ateapipb.UpdateActorRequest) { req.UpdateMask = nil }),
		field.ErrorList{field.Required(field.NewPath("update_mask"), "")},
	}, {
		"empty update_mask",
		updateActorReq(withMaskPaths()),
		field.ErrorList{field.Required(field.NewPath("update_mask"), "")},
	}, {
		"wildcard update_mask",
		updateActorReq(withMaskPaths("*")),
		field.ErrorList{field.NotSupported(field.NewPath("update_mask"), "*", mutableFields)},
	}, {
		"output-only field in update_mask",
		updateActorReq(withMaskPaths("status")),
		field.ErrorList{field.NotSupported(field.NewPath("update_mask"), "status", mutableFields)},
	}, {
		"immutable field in update_mask",
		updateActorReq(withMaskPaths("metadata.name")),
		field.ErrorList{field.NotSupported(field.NewPath("update_mask"), "metadata.name", mutableFields)},
	}, {
		"leaf path under a whole-mutable field, also separately mutable",
		updateActorReq(withMaskPaths("worker_selector.match_labels")),
		nil,
	}, {
		"nil worker_selector",
		updateActorReq(),
		nil,
	}, {
		"valid worker_selector",
		updateActorReq(withSelector(map[string]string{"tier": "1"})),
		nil,
	}, {
		"invalid worker_selector label key",
		updateActorReq(withSelector(map[string]string{"bad key!": "1"})),
		field.ErrorList{field.Invalid(field.NewPath("actor", "worker_selector", "match_labels").Key("bad key!"), "bad key!", "")},
	}, {
		"invalid worker_selector label value",
		updateActorReq(withSelector(map[string]string{"tier": "not valid!"})),
		field.ErrorList{field.Invalid(field.NewPath("actor", "worker_selector", "match_labels").Key("tier"), "not valid!", "")},
	}, {
		"too many worker_selector.match_labels",
		updateActorReq(withSelector(selectorLabelsOfSize(11))),
		field.ErrorList{field.TooMany(field.NewPath("actor", "worker_selector", "match_labels"), 11, 10)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateUpdateActorRequest(tt.req), tt.want)
		})
	}
}

func TestUpdateActor_FieldMasks(t *testing.T) {
	tests := []struct {
		name      string
		stored    *ateapipb.Actor
		req       *ateapipb.Actor
		maskPaths []string
		want      *ateapipb.Actor
	}{
		{
			name:      "whole mask sets worker_selector from nil",
			stored:    &ateapipb.Actor{},
			req:       &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}},
			maskPaths: []string{"worker_selector"},
			want:      &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}},
		},
		{
			name:      "whole mask clears worker_selector to nil",
			stored:    &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "free"}}},
			req:       &ateapipb.Actor{},
			maskPaths: []string{"worker_selector"},
			want:      &ateapipb.Actor{},
		},
		{
			name:      "leaf mask initializes worker_selector from nil to set match_labels",
			stored:    &ateapipb.Actor{},
			req:       &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}},
			maskPaths: []string{"worker_selector.match_labels"},
			want:      &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}},
		},
		{
			name:      "leaf mask overwrites match_labels, worker_selector already present",
			stored:    &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "free"}}},
			req:       &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}},
			maskPaths: []string{"worker_selector.match_labels"},
			want:      &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}},
		},
		{
			name:      "leaf mask clears match_labels, worker_selector stays present",
			stored:    &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "free"}}},
			req:       &ateapipb.Actor{},
			maskPaths: []string{"worker_selector.match_labels"},
			want:      &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.stored.Metadata = &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID}
			tt.stored.ActorTemplateNamespace = "ns1"
			tt.stored.ActorTemplateName = "tmpl1"
			svc, _ := serviceWithActor(t, tt.stored)

			tt.req.Metadata = &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID}
			updated, err := svc.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{
				Actor:      tt.req,
				UpdateMask: &fieldmaskpb.FieldMask{Paths: tt.maskPaths},
			})
			if err != nil {
				t.Fatalf("UpdateActor failed: %v", err)
			}

			tt.want.Metadata = &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID, Version: 2}
			tt.want.ActorTemplateNamespace = "ns1"
			tt.want.ActorTemplateName = "tmpl1"
			if diff := cmp.Diff(tt.want, updated, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
				t.Errorf("UpdateActor response mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestUpdateActor_StampsFullSpanIdentity(t *testing.T) {
	ns := namespaceForTest("ns-span-update")
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
		if _, err := tc.service.UpdateActor(ctx, &ateapipb.UpdateActorRequest{
			Actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
				WorkerSelector: &ateapipb.Selector{
					MatchLabels: map[string]string{"env": "prod"},
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"worker_selector"}},
		}); err != nil {
			t.Fatalf("UpdateActor: %v", err)
		}
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, testAtespace)
	assertSpanStr(t, attrs, ateattr.ActorNameKey, testActorID)
	assertSpanStr(t, attrs, ateattr.TemplateNameKey, "tmpl1")
	assertSpanStr(t, attrs, ateattr.TemplateNamespaceKey, ns)
	if v, ok := attrs[ateattr.ActorUIDKey]; !ok || v.Type() != attribute.STRING || v.AsString() == "" {
		t.Errorf("%s = %v, want non-empty server-assigned uid", ateattr.ActorUIDKey, v.Emit())
	}
	if v, ok := attrs[ateattr.ActorVersionKey]; !ok || v.Type() != attribute.INT64 || v.AsInt64() != 2 {
		t.Errorf("%s = %v, want int64 2 (updated version)", ateattr.ActorVersionKey, v.Emit())
	}
}

func TestUpdateActor_FailedLookupStampsRefIdentityOnly(t *testing.T) {
	ns := namespaceForTest("ns-span-update-err")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	attrs := recordRootSpanAttrs(t, func(ctx context.Context) {
		if _, err := tc.service.UpdateActor(ctx, &ateapipb.UpdateActorRequest{
			Actor:      &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID}},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"worker_selector"}},
		}); status.Code(err) != codes.NotFound {
			t.Fatalf("UpdateActor(missing) error = %v, want code NotFound", err)
		}
	})

	assertSpanStr(t, attrs, ateattr.AtespaceKey, testAtespace)
	assertSpanStr(t, attrs, ateattr.ActorNameKey, testActorID)
	for _, k := range []attribute.Key{ateattr.ActorUIDKey, ateattr.TemplateNameKey, ateattr.TemplateNamespaceKey, ateattr.ActorVersionKey} {
		if _, ok := attrs[k]; ok {
			t.Errorf("unexpected %s on failed-update span", k)
		}
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
	original, err := persistence.CreateActor(ctx, &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
		ActorTemplateNamespace: "ns1",
		ActorTemplateName:      "tmpl1",
		Status:                 ateapipb.Actor_STATUS_RUNNING,
		WorkerAssignment:       &ateapipb.WorkerAssignment{WorkerPod: "pod-a"},
	})
	if err != nil {
		t.Fatalf("seed CreateActor: %v", err)
	}

	// A concurrent client deletes A and recreates the same atespace/name as a
	// brand new actor B, in the window the handler used to leave open between
	// its own read and the store's WATCH.
	var recreated *ateapipb.Actor
	racing := &conflictInjectingStore{
		Interface: persistence,
		inject: func() {
			if _, err := persistence.UpdateActor(ctx, actorRef, func(toUpdate *ateapipb.Actor) error {
				toUpdate.Status = ateapipb.Actor_STATUS_DELETING
				return nil
			}); err != nil {
				t.Fatalf("racing writer: mark deleting: %v", err)
			}
			if _, err := persistence.DeleteActor(ctx, actorRef); err != nil {
				t.Fatalf("racing writer: DeleteActor: %v", err)
			}
			recreated, err = persistence.CreateActor(ctx, &ateapipb.Actor{
				Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
				ActorTemplateNamespace: "ns1",
				ActorTemplateName:      "tmpl1",
				Status:                 ateapipb.Actor_STATUS_SUSPENDED,
			})
			if err != nil {
				t.Fatalf("racing writer: recreate CreateActor: %v", err)
			}
		},
	}
	svc := &Service{persistence: racing}

	// The client asserts "only update the actor with uid A".
	_, err = svc.UpdateActor(ctx, &ateapipb.UpdateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: testAtespace,
				Name:     testActorID,
				Uid:      original.GetMetadata().GetUid(),
			},
			WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"worker_selector"}},
	})
	if code := status.Code(err); code != codes.Aborted {
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
	if got := stored.GetStatus(); got != ateapipb.Actor_STATUS_SUSPENDED {
		t.Errorf("stored status = %v, want %v: recreated actor was overwritten with the deleted actor's state",
			got, ateapipb.Actor_STATUS_SUSPENDED)
	}
	if got := stored.GetWorkerAssignment(); got != nil {
		t.Errorf("stored worker_assignment = %v, want nil: recreated actor inherited the deleted actor's worker", got)
	}
	if got := stored.GetWorkerSelector(); got != nil {
		t.Errorf("stored worker_selector = %v, want nil: update meant for the deleted actor was applied", got)
	}
}

// TestUpdateActor_ConcurrentDisjointUpdates checks that concurrent write
// to a disjoint field is resolved by the store and both fields survive the update.
func TestUpdateActor_ConcurrentDisjointUpdates(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	actorRef := resources.ActorRef{Atespace: testAtespace, Name: testActorID}

	if _, err := persistence.CreateActor(ctx, &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
		ActorTemplateNamespace: "ns1",
		ActorTemplateName:      "tmpl1",
		Status:                 ateapipb.Actor_STATUS_RUNNING,
	}); err != nil {
		t.Fatalf("seed CreateActor: %v", err)
	}

	// A suspend workflow bumps status (a field that a later update operation will not touch)
	// inside the handler's read-modify-write window.
	racing := &conflictInjectingStore{
		Interface: persistence,
		inject: func() {
			if _, err := persistence.UpdateActor(ctx, actorRef, func(toUpdate *ateapipb.Actor) error {
				toUpdate.Status = ateapipb.Actor_STATUS_SUSPENDING
				return nil
			}); err != nil {
				t.Fatalf("racing writer: mark suspending: %v", err)
			}
		},
	}
	svc := &Service{persistence: racing}

	// Update operation is changing the worker_selector field, not the actor's status (like the concurrent op)
	if _, err := svc.UpdateActor(ctx, &ateapipb.UpdateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:       &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
			WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"worker_selector"}},
	}); err != nil {
		t.Fatalf("UpdateActor error = %v, want success: no version precondition was set, so the conflict is the server's to resolve", err)
	}

	stored, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	// Both worker selector and status updates survive
	if got := stored.GetWorkerSelector().GetMatchLabels()["tier"]; got != "paid" {
		t.Errorf("stored worker_selector[tier] = %q, want %q", got, "paid")
	}
	if got := stored.GetStatus(); got != ateapipb.Actor_STATUS_SUSPENDING {
		t.Errorf("stored status = %v, want %v: the concurrent writer's field must survive", got, ateapipb.Actor_STATUS_SUSPENDING)
	}
}

// updateActorReq builds a minimal valid UpdateActorRequest, then applies the
// given mutations.
func updateActorReq(mutate ...func(*ateapipb.UpdateActorRequest)) *ateapipb.UpdateActorRequest {
	req := &ateapipb.UpdateActorRequest{
		Actor:      &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "id1"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"worker_selector"}},
	}
	for _, m := range mutate {
		m(req)
	}
	return req
}

func withMetadata(mutate func(*ateapipb.ResourceMetadata)) func(*ateapipb.UpdateActorRequest) {
	return func(req *ateapipb.UpdateActorRequest) { mutate(req.GetActor().GetMetadata()) }
}

func withMaskPaths(paths ...string) func(*ateapipb.UpdateActorRequest) {
	return func(req *ateapipb.UpdateActorRequest) { req.UpdateMask = &fieldmaskpb.FieldMask{Paths: paths} }
}

func withSelector(labels map[string]string) func(*ateapipb.UpdateActorRequest) {
	return func(req *ateapipb.UpdateActorRequest) {
		req.GetActor().WorkerSelector = &ateapipb.Selector{MatchLabels: labels}
	}
}

// serviceWithActor seeds one actor in a miniredis-backed store and returns a
// Service over it.
func serviceWithActor(t *testing.T, actor *ateapipb.Actor) (*Service, *ateapipb.Actor) {
	t.Helper()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	created, err := persistence.CreateActor(context.Background(), actor)
	if err != nil {
		t.Fatalf("Failed to CreateActor: %v", err)
	}
	return &Service{persistence: persistence}, created
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
	tc.service.volumePlugins = map[string]volume.VolumePluginControlPlane{
		"substrate.io/mock": plugin,
	}

	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: testAtespace,
			Name:     "multi-vol-actor",
		},
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		ActorVolumes: []*ateapipb.ExternalVolume{
			{VolumeName: "vol1", StorageVolumeId: "storage-vol-1", Status: ateapipb.ExternalVolume_STATUS_CREATED, VolumeType: "substrate.io/mock"},
			{VolumeName: "vol2", StorageVolumeId: "storage-vol-2", Status: ateapipb.ExternalVolume_STATUS_CREATED, VolumeType: "substrate.io/mock"},
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
	for _, k := range []attribute.Key{ateattr.ActorUIDKey, ateattr.TemplateNameKey, ateattr.TemplateNamespaceKey, ateattr.ActorVersionKey} {
		if _, ok := attrs[k]; ok {
			t.Errorf("unexpected %s on failed-pause span", k)
		}
	}
}

func TestValidatePauseActorRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.PauseActorRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}},
		nil,
	}, {
		"missing actor",
		&ateapipb.PauseActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.atespace",
		&ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Name: "id1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "atespace"), "")},
	}, {
		"invalid actor.atespace",
		&ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "NS1", Name: "id1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "")},
	}, {
		"missing actor.name",
		&ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validatePauseActorRequest(tt.req), tt.want)
		})
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

func TestValidateResumeActorRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.ResumeActorRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}},
		nil,
	}, {
		"missing actor",
		&ateapipb.ResumeActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.atespace",
		&ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Name: "id1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "atespace"), "")},
	}, {
		"invalid actor.atespace",
		&ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "NS1", Name: "id1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "")},
	}, {
		"missing actor.name",
		&ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateResumeActorRequest(tt.req), tt.want)
		})
	}
}

func TestValidateSuspendActorRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.SuspendActorRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}},
		nil,
	}, {
		"missing actor",
		&ateapipb.SuspendActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.atespace",
		&ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Name: "id1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "atespace"), "")},
	}, {
		"invalid actor.atespace",
		&ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "NS1", Name: "id1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "")},
	}, {
		"missing actor.name",
		&ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateSuspendActorRequest(tt.req), tt.want)
		})
	}
}
