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
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

// TestActorTemplateFromCRD pins the full CRD-to-proto projection: every field
// the workflows consume must survive the conversion, since both template
// sources share the proto code path during the migration.
func TestActorTemplateFromCRD(t *testing.T) {
	crd := &atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ate-demo-counter-microvm-csi",
			Name:      "counter-microvm-csi",
			UID:       types.UID("9a1b6f9e-6a3f-4a3e-9a51-0c2f3a34d001"),
		},
		Spec: atev1alpha1.ActorTemplateSpec{
			SandboxClass: atev1alpha1.SandboxClassMicroVM,
			WorkerSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"workload": "counter-microvm-csi"},
			},
			Containers: []atev1alpha1.Container{{
				Name:    "counter",
				Image:   "ko://github.com/agent-substrate/substrate/demos/counter",
				Command: []string{"/ko-app/counter"},
				Args:    []string{"--port=80"},
				Env:     []atev1alpha1.EnvVar{{Name: "COUNTER_DATA_DIR", Value: "/home/counter"}},
				Readyz: &atev1alpha1.ContainerReadyz{
					HTTPGet:        &atev1alpha1.HTTPGetAction{Path: "/readyz", Port: 80},
					TimeoutSeconds: 30,
				},
				VolumeMounts: []atev1alpha1.VolumeMount{{Name: "data", MountPath: "/home/counter"}},
				SecurityContext: &atev1alpha1.SecurityContext{
					Capabilities: &atev1alpha1.Capabilities{
						Add:  []atev1alpha1.Capability{"NET_BIND_SERVICE"},
						Drop: []atev1alpha1.Capability{"ALL"},
					},
				},
			}},
			Volumes: []atev1alpha1.Volume{
				{Name: "durable", VolumeSource: atev1alpha1.VolumeSource{DurableDir: &atev1alpha1.DurableDirVolumeSource{}}},
				{Name: "data", VolumeSource: atev1alpha1.VolumeSource{
					ExternalVolumeTemplate: &atev1alpha1.ExternalVolumeTemplate{
						Capacity:         resource.MustParse("1Gi"),
						StorageClassName: "csi-hostpath-sc",
					},
				}},
				{Name: "model", VolumeSource: atev1alpha1.VolumeSource{
					Image: &atev1alpha1.ImageVolumeSource{Reference: "ko://github.com/agent-substrate/substrate/demos/counter"},
				}},
				{Name: "system", VolumeSource: atev1alpha1.VolumeSource{
					SystemInfo: &atev1alpha1.SystemInfoVolumeSource{
						DataSources: []atev1alpha1.SystemInfoDataSource{
							{ActorMetadata: &atev1alpha1.ActorMetadataDataSource{Items: []atev1alpha1.ActorMetadataItem{
								{Field: atev1alpha1.ActorMetadataFieldName, Path: "actor/name"},
								{Field: atev1alpha1.ActorMetadataFieldAtespace, Path: "actor/atespace"},
								{Field: atev1alpha1.ActorMetadataFieldUID, Path: "actor/uid"},
							}}},
							{TrustBundle: &atev1alpha1.TrustBundleDataSource{Name: "egress-mitm.ate.dev", Path: "tls/egress-ca.pem"}},
						},
					},
				}},
			},
			SnapshotsConfig: atev1alpha1.SnapshotsConfig{
				Location: "gs://ate-snapshots/ate-demo-counter-microvm-csi/",
				OnPause:  atev1alpha1.SnapshotScopeFull,
				OnCommit: atev1alpha1.SnapshotScopeData,
				OnResume: atev1alpha1.OnResumeConfig{FromData: atev1alpha1.ResumeSourceGolden},
			},
			Resources: &corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1500m"),
					corev1.ResourceMemory: resource.MustParse("512Mi"),
				},
			},
		},
		Status: atev1alpha1.ActorTemplateStatus{
			Phase:          atev1alpha1.PhaseReady,
			GoldenSnapshot: "2026-01-01t00-00-00z-abc",
		},
	}

	want := &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "ate-demo-counter-microvm-csi",
			Name:     "counter-microvm-csi",
			Uid:      "9a1b6f9e-6a3f-4a3e-9a51-0c2f3a34d001",
		},
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"workload": "counter-microvm-csi"},
		},
		Containers: []*ateapipb.Container{{
			Name:    "counter",
			Image:   "ko://github.com/agent-substrate/substrate/demos/counter",
			Command: []string{"/ko-app/counter"},
			Args:    []string{"--port=80"},
			Env:     []*ateapipb.EnvVar{{Name: "COUNTER_DATA_DIR", Value: "/home/counter"}},
			Readyz: &ateapipb.ContainerReadyz{
				HttpGet:        &ateapipb.HTTPGetAction{Path: "/readyz", Port: 80},
				TimeoutSeconds: 30,
			},
			VolumeMounts: []*ateapipb.VolumeMount{{Name: "data", MountPath: "/home/counter"}},
			SecurityContext: &ateapipb.SecurityContext{
				Capabilities: &ateapipb.Capabilities{Add: []string{"NET_BIND_SERVICE"}, Drop: []string{"ALL"}},
			},
		}},
		Volumes: []*ateapipb.Volume{
			{Name: "durable", Type: "DurableDir", DurableDir: &ateapipb.DurableDirVolumeSource{}},
			{Name: "data", Type: "ExternalVolumeTemplate", ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
				Capacity:         "1Gi",
				StorageClassName: "csi-hostpath-sc",
			}},
			{Name: "model", Type: "Image", Image: &ateapipb.ImageVolumeSource{Reference: "ko://github.com/agent-substrate/substrate/demos/counter"}},
			{Name: "system", Type: "SystemInfo", SystemInfo: &ateapipb.SystemInfoVolumeSource{
				DataSources: []*ateapipb.SystemInfoDataSource{
					{ActorMetadata: &ateapipb.ActorMetadataDataSource{Items: []*ateapipb.ActorMetadataItem{
						{Field: ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME, Path: "actor/name"},
						{Field: ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_ATESPACE, Path: "actor/atespace"},
						{Field: ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_UID, Path: "actor/uid"},
					}}},
					{TrustBundle: &ateapipb.TrustBundleDataSource{Name: "egress-mitm.ate.dev", Path: "tls/egress-ca.pem"}},
				},
			}},
		},
		SnapshotsConfig: &ateapipb.SnapshotsConfig{
			StorageLocation: "gs://ate-snapshots/ate-demo-counter-microvm-csi/",
			OnPause:         ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			OnCommit:        ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			OnResume:        &ateapipb.OnResumeConfig{FromData: ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN},
		},
		SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM},
		Resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{
			{Name: "cpu", Quantity: "1500m"},
			{Name: "memory", Quantity: "512Mi"},
		}},
		Status: &ateapipb.ActorTemplateStatus{
			GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
				GoldenSnapshot: &ateapipb.ObjectRef{
					Atespace: resources.GoldenActorAtespace,
					Name:     "2026-01-01t00-00-00z-abc",
				},
			},
		},
	}

	got := mustTemplateFromCRD(crd)
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("actorTemplateFromCRD mismatch (-want +got):\n%s", diff)
	}
}

// TestActorTemplateFromCRD_Defaults pins the conversion of a minimal CRD:
// unset scopes normalize to the CRD defaults (Full / ColdBoot), and no
// golden snapshot yields an empty status.
func TestActorTemplateFromCRD_Defaults(t *testing.T) {
	got := mustTemplateFromCRD(&atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "tmpl1"},
	})

	want := &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "tmpl1"},
		SnapshotsConfig: &ateapipb.SnapshotsConfig{
			OnPause:  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			OnCommit: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			OnResume: &ateapipb.OnResumeConfig{FromData: ateapipb.ResumeSource_RESUME_SOURCE_COLD_BOOT},
		},
		SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_UNSPECIFIED},
		Status:        &ateapipb.ActorTemplateStatus{},
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("actorTemplateFromCRD mismatch (-want +got):\n%s", diff)
	}

	if _, err := actorTemplateFromCRD(nil); status.Code(err) != codes.Internal {
		t.Errorf("actorTemplateFromCRD(nil) error = %v, want Internal", err)
	}
}

// TestActorTemplateFromCRD_RejectsMatchExpressions pins the equality-only
// selector contract: conversion refuses a template with matchExpressions
// rather than dropping them and scheduling onto a wider pool set.
func TestActorTemplateFromCRD_RejectsMatchExpressions(t *testing.T) {
	_, err := actorTemplateFromCRD(&atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "tmpl1"},
		Spec: atev1alpha1.ActorTemplateSpec{
			WorkerSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: "tier", Operator: metav1.LabelSelectorOpIn, Values: []string{"1"}},
				},
			},
		},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("actorTemplateFromCRD error = %v, want FailedPrecondition", err)
	}
}

// mustTemplateFromCRD converts a fixture that must be convertible; rejection
// paths call actorTemplateFromCRD directly.
func mustTemplateFromCRD(crd *atev1alpha1.ActorTemplate) *ateapipb.ActorTemplate {
	out, err := actorTemplateFromCRD(crd)
	if err != nil {
		panic(err)
	}
	return out
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

// TestResolveActorTemplate_PrefersSubstrateRef verifies the resolver reads
// the substrate resource when the actor carries an actor_template reference,
// and falls back to the converted CRD for legacy actors.
func TestResolveActorTemplate_PrefersSubstrateRef(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)

	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if err := indexer.Add(&atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "crd-tmpl", UID: types.UID("crd-uid-1")},
	}); err != nil {
		t.Fatalf("add template to indexer: %v", err)
	}
	lister := listersv1alpha1.NewActorTemplateLister(indexer)
	stored := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")

	t.Run("ref mode reads the store", func(t *testing.T) {
		actor := &ateapipb.Actor{ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"}}
		got, err := resolveActorTemplate(ctx, persistence, lister, actor)
		if err != nil {
			t.Fatalf("resolveActorTemplate: %v", err)
		}
		if got.GetMetadata().GetUid() != stored.GetMetadata().GetUid() {
			t.Errorf("template uid = %q, want the stored substrate template %q", got.GetMetadata().GetUid(), stored.GetMetadata().GetUid())
		}
	})

	t.Run("ref to a missing template is FailedPrecondition", func(t *testing.T) {
		actor := &ateapipb.Actor{ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "absent"}}
		_, err := resolveActorTemplate(ctx, persistence, lister, actor)
		if got := status.Code(err); got != codes.FailedPrecondition {
			t.Fatalf("status.Code = %v, want FailedPrecondition (err: %v)", got, err)
		}
	})

	t.Run("legacy mode converts the CRD", func(t *testing.T) {
		actor := &ateapipb.Actor{ActorTemplateNamespace: "ns", ActorTemplateName: "crd-tmpl"}
		got, err := resolveActorTemplate(ctx, persistence, lister, actor)
		if err != nil {
			t.Fatalf("resolveActorTemplate: %v", err)
		}
		if got.GetMetadata().GetUid() != "crd-uid-1" {
			t.Errorf("template uid = %q, want the CRD uid", got.GetMetadata().GetUid())
		}
		if got.GetMetadata().GetAtespace() != "ns" || got.GetMetadata().GetName() != "crd-tmpl" {
			t.Errorf("template identity = %s/%s, want ns/crd-tmpl", got.GetMetadata().GetAtespace(), got.GetMetadata().GetName())
		}
	})
}

// TestResolveActorTemplate_NotFound verifies a vanished template (either
// form) and an actor naming no template at all surface
// errActorTemplateNotFound, so callers like delete can tolerate them.
func TestResolveActorTemplate_NotFound(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	lister := listersv1alpha1.NewActorTemplateLister(indexer)
	stored := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")

	tests := []struct {
		name         string
		actor        *ateapipb.Actor
		wantNotFound bool
	}{
		{"ref mode resolves", &ateapipb.Actor{ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "sub-tmpl"}}, false},
		{"ref to deleted template", &ateapipb.Actor{ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "gone"}}, true},
		{"legacy CRD gone", &ateapipb.Actor{ActorTemplateNamespace: "ns", ActorTemplateName: "gone"}, true},
		{"no template named at all", &ateapipb.Actor{}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveActorTemplate(ctx, persistence, lister, tc.actor)
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
