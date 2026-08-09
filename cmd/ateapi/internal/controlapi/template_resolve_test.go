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

	"github.com/google/go-cmp/cmp"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/testing/protocmp"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestCRDTemplateFromVersion(t *testing.T) {
	prod := "prod"
	at := &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Name: "tmpl-a", Uid: "at-uid"},
		Spec: &ateapipb.ActorTemplateSpec{
			WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "1"}},
		},
	}
	atv := &ateapipb.ActorTemplateVersion{
		Metadata:      &ateapipb.ResourceMetadata{Name: "tmpl-a-v1", Uid: "atv-uid"},
		ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl-a"},
		Spec: validTemplateVersionSpec(func(spec *ateapipb.ActorTemplateVersionSpec) {
			spec.Volumes = append(spec.Volumes, &ateapipb.Volume{
				Name: "scratch",
				Source: &ateapipb.Volume_ExternalVolumeTemplate{ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					Capacity: "1Gi", StorageClassName: "standard",
				}},
			})
			spec.SnapshotsConfig.OnPause = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
			spec.SnapshotsConfig.OnCommit = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA
			spec.SnapshotsConfig.OnResume = &ateapipb.OnResumeConfig{FromData: ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN}
		}),
		Status: &ateapipb.ActorTemplateVersionStatus{
			State:           ateapipb.ActorTemplateVersionStatus_STATE_READY,
			GoldenSnapshot:  &ateapipb.ObjectRef{Atespace: "ate-golden", Name: "snap-1"},
			ResolvedSandbox: &ateapipb.SandboxAssets{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM},
		},
	}

	got, err := crdTemplateFromVersion(at, atv)
	if err != nil {
		t.Fatalf("crdTemplateFromVersion failed: %v", err)
	}

	want := &atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tmpl-a-v1", UID: types.UID("atv-uid")},
		Spec: atev1alpha1.ActorTemplateSpec{
			PauseImage:   "pause@sha256:abc",
			SandboxClass: atev1alpha1.SandboxClassMicroVM,
			SnapshotsConfig: atev1alpha1.SnapshotsConfig{
				Location: "gs://bucket/snapshots",
				OnPause:  atev1alpha1.SnapshotScopeData,
				OnCommit: atev1alpha1.SnapshotScopeData,
				OnResume: atev1alpha1.OnResumeConfig{FromData: atev1alpha1.ResumeSourceGolden},
			},
			WorkerSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"tier": "1"}},
			Volumes: []atev1alpha1.Volume{{
				Name:         "data",
				VolumeSource: atev1alpha1.VolumeSource{DurableDir: &atev1alpha1.DurableDirVolumeSource{}},
			}, {
				Name: "scratch",
				VolumeSource: atev1alpha1.VolumeSource{ExternalVolumeTemplate: &atev1alpha1.ExternalVolumeTemplate{
					Capacity: resource.MustParse("1Gi"), StorageClassName: "standard",
				}},
			}},
			Containers: []atev1alpha1.Container{{
				Name:  "main",
				Image: "app@sha256:def",
				Env:   []atev1alpha1.EnvVar{{Name: "MODE", Value: &prod}},
				Readyz: &atev1alpha1.ContainerReadyz{
					HTTPGet:        &atev1alpha1.HTTPGetAction{Path: "/readyz", Port: 8080},
					TimeoutSeconds: 30,
				},
				VolumeMounts: []atev1alpha1.VolumeMount{{Name: "data", MountPath: "/data"}},
			}},
		},
		Status: atev1alpha1.ActorTemplateStatus{GoldenSnapshot: "snap-1"},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("crdTemplateFromVersion mismatch (-want +got):\n%s", diff)
	}
}

func TestCRDTemplateFromVersion_Defaults(t *testing.T) {
	// UNSPECIFIED scopes mean Full, UNSPECIFIED resume source means ColdBoot,
	// UNSPECIFIED sandbox class means gvisor, and no selector stays nil.
	at := &ateapipb.ActorTemplate{Metadata: &ateapipb.ResourceMetadata{Name: "tmpl-a"}}
	atv := &ateapipb.ActorTemplateVersion{
		Metadata:      &ateapipb.ResourceMetadata{Name: "tmpl-a-v1"},
		ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl-a"},
		Spec: &ateapipb.ActorTemplateVersionSpec{
			PauseImage:      "pause@sha256:abc",
			SnapshotsConfig: &ateapipb.SnapshotsConfig{StorageLocation: "gs://bucket/snapshots"},
		},
	}

	got, err := crdTemplateFromVersion(at, atv)
	if err != nil {
		t.Fatalf("crdTemplateFromVersion failed: %v", err)
	}
	if got.Spec.SandboxClass != atev1alpha1.SandboxClassGvisor {
		t.Errorf("SandboxClass = %q, want gvisor", got.Spec.SandboxClass)
	}
	if got.Spec.SnapshotsConfig.OnPause != atev1alpha1.SnapshotScopeFull || got.Spec.SnapshotsConfig.OnCommit != atev1alpha1.SnapshotScopeFull {
		t.Errorf("snapshot scopes = (%q, %q), want (Full, Full)", got.Spec.SnapshotsConfig.OnPause, got.Spec.SnapshotsConfig.OnCommit)
	}
	if got.Spec.SnapshotsConfig.OnResume.FromData != atev1alpha1.ResumeSourceColdBoot {
		t.Errorf("OnResume.FromData = %q, want ColdBoot", got.Spec.SnapshotsConfig.OnResume.FromData)
	}
	if got.Spec.WorkerSelector != nil {
		t.Errorf("WorkerSelector = %v, want nil", got.Spec.WorkerSelector)
	}
	if got.Status.GoldenSnapshot != "" {
		t.Errorf("GoldenSnapshot = %q, want empty", got.Status.GoldenSnapshot)
	}
}

func TestCRDTemplateFromVersion_BadCapacity(t *testing.T) {
	at := &ateapipb.ActorTemplate{Metadata: &ateapipb.ResourceMetadata{Name: "tmpl-a"}}
	atv := &ateapipb.ActorTemplateVersion{
		Metadata:      &ateapipb.ResourceMetadata{Name: "tmpl-a-v1"},
		ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl-a"},
		Spec: validTemplateVersionSpec(func(spec *ateapipb.ActorTemplateVersionSpec) {
			spec.Volumes = []*ateapipb.Volume{{
				Name: "data",
				Source: &ateapipb.Volume_ExternalVolumeTemplate{ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					Capacity: "not-a-quantity", StorageClassName: "standard",
				}},
			}}
		}),
	}
	if _, err := crdTemplateFromVersion(at, atv); err == nil {
		t.Fatal("crdTemplateFromVersion accepted an invalid capacity; want error")
	}
}

func TestAteletSandboxAssetsFromAPI(t *testing.T) {
	in := &ateapipb.SandboxAssets{
		SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM,
		Assets: map[string]*ateapipb.ArchAssets{
			"amd64": {Files: map[string]*ateapipb.AssetFile{
				"runsc": {Url: "https://assets/runsc", Sha256: "abc"},
			}},
		},
	}
	want := &ateletpb.SandboxAssets{
		SandboxClass: "microvm",
		Assets: map[string]*ateletpb.ArchAssets{
			"amd64": {Files: map[string]*ateletpb.AssetFile{
				"runsc": {Url: "https://assets/runsc", Sha256: "abc"},
			}},
		},
	}
	if diff := cmp.Diff(want, ateletSandboxAssetsFromAPI(in), protocmp.Transform()); diff != "" {
		t.Errorf("ateletSandboxAssetsFromAPI mismatch (-want +got):\n%s", diff)
	}
}

func TestTemplateRefForAtelet(t *testing.T) {
	crdActor := &ateapipb.Actor{ActorTemplateNamespace: "ns1", ActorTemplateName: "tmpl1"}
	if ns, name := templateRefForAtelet(crdActor); ns != "ns1" || name != "tmpl1" {
		t.Errorf("templateRefForAtelet(crd) = (%q, %q), want (ns1, tmpl1)", ns, name)
	}
	nativeActor := &ateapipb.Actor{ActorTemplate: "tmpl1", ActorTemplateVersion: "tmpl1-v1"}
	if ns, name := templateRefForAtelet(nativeActor); ns != "" || name != "tmpl1-v1" {
		t.Errorf("templateRefForAtelet(native) = (%q, %q), want (\"\", tmpl1-v1)", ns, name)
	}
}
