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
	"testing"

	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
