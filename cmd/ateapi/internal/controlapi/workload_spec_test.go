//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package controlapi

import (
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestWorkloadSpecFromActorTemplate(t *testing.T) {
	tests := []struct {
		name     string
		template *atev1alpha1.ActorTemplate
		want     *ateletpb.WorkloadSpec
	}{
		{
			name: "converts DurableDir volume and mounts",
			template: &atev1alpha1.ActorTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "tmpl1", Namespace: "agent-ns"},
				Spec: atev1alpha1.ActorTemplateSpec{
					Volumes: []atev1alpha1.Volume{
						{Name: "home", VolumeSource: atev1alpha1.VolumeSource{DurableDir: &atev1alpha1.DurableDirVolumeSource{}}},
					},
					Containers: []atev1alpha1.Container{
						{
							Name:  "main",
							Image: "main",
							VolumeMounts: []atev1alpha1.VolumeMount{
								{Name: "home", MountPath: "/home/user"},
								{Name: "home", MountPath: "/workspace"},
							},
						},
					},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Volumes: []*ateletpb.Volume{
					{
						Name:   "home",
						Type:   ateletpb.VolumeType_VOLUME_TYPE_DURABLE_DIR,
						Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}},
					},
				},
				Containers: []*ateletpb.Container{
					{
						Name:  "main",
						Image: "main",
						VolumeMounts: []*ateletpb.VolumeMount{
							{Name: "home", MountPath: "/home/user"},
							{Name: "home", MountPath: "/workspace"},
						},
					},
				},
			},
		},
		{
			name: "skips non-DurableDir volumes",
			template: &atev1alpha1.ActorTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "tmpl1", Namespace: "agent-ns"},
				Spec: atev1alpha1.ActorTemplateSpec{
					Volumes: []atev1alpha1.Volume{
						{Name: "unsupported", VolumeSource: atev1alpha1.VolumeSource{}},
						{Name: "home", VolumeSource: atev1alpha1.VolumeSource{DurableDir: &atev1alpha1.DurableDirVolumeSource{}}},
					},
					Containers: []atev1alpha1.Container{
						{
							Name:  "main",
							Image: "main",
							VolumeMounts: []atev1alpha1.VolumeMount{
								{Name: "home", MountPath: "/workspace"},
							},
						},
					},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Volumes: []*ateletpb.Volume{
					{
						Name:   "home",
						Type:   ateletpb.VolumeType_VOLUME_TYPE_DURABLE_DIR,
						Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}},
					},
				},
				Containers: []*ateletpb.Container{
					{
						Name:  "main",
						Image: "main",
						VolumeMounts: []*ateletpb.VolumeMount{
							{Name: "home", MountPath: "/workspace"},
						},
					},
				},
			},
		},
		{
			name: "container without volume mounts has none",
			template: &atev1alpha1.ActorTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "tmpl1", Namespace: "agent-ns"},
				Spec: atev1alpha1.ActorTemplateSpec{
					Volumes: []atev1alpha1.Volume{
						{Name: "home", VolumeSource: atev1alpha1.VolumeSource{DurableDir: &atev1alpha1.DurableDirVolumeSource{}}},
					},
					Containers: []atev1alpha1.Container{
						{Name: "main", Image: "main"},
					},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Volumes: []*ateletpb.Volume{
					{
						Name:   "home",
						Type:   ateletpb.VolumeType_VOLUME_TYPE_DURABLE_DIR,
						Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}},
					},
				},
				Containers: []*ateletpb.Container{{Name: "main", Image: "main"}},
			},
		},
		{
			name: "maps literal env",
			template: &atev1alpha1.ActorTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "tmpl1", Namespace: "agent-ns"},
				Spec: atev1alpha1.ActorTemplateSpec{
					Containers: []atev1alpha1.Container{
						{
							Name:  "main",
							Image: "main",
							Env: []atev1alpha1.EnvVar{
								{Name: "LITERAL", Value: "plain"},
								{Name: "EMPTY", Value: ""},
							},
						},
					},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{{
					Name:  "main",
					Image: "main",
					Env: []*ateletpb.EnvEntry{
						{Name: "LITERAL", Value: "plain"},
						{Name: "EMPTY", Value: ""},
					},
				}},
			},
		},
		{
			name: "maps command and args",
			template: &atev1alpha1.ActorTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "tmpl1", Namespace: "agent-ns"},
				Spec: atev1alpha1.ActorTemplateSpec{
					Containers: []atev1alpha1.Container{
						{
							Name:    "main",
							Image:   "main",
							Command: []string{"/entrypoint"},
							Args:    []string{"--foo", "--bar"},
						},
					},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{{
					Name:    "main",
					Image:   "main",
					Command: []string{"/entrypoint"},
					Args:    []string{"--foo", "--bar"},
				}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := workloadSpecFromActorTemplate(tt.template, nil)
			if err != nil {
				t.Fatalf("workloadSpecFromActorTemplate failed: %v", err)
			}
			if diff := cmp.Diff(tt.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("WorkloadSpec mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestWorkloadSpecFromActorTemplatePropagatesReadyz(t *testing.T) {
	got, err := workloadSpecFromActorTemplate(&atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tmpl-readyz", Namespace: "agent-ns"},
		Spec: atev1alpha1.ActorTemplateSpec{
			Containers: []atev1alpha1.Container{
				{
					Name:  "with-probe",
					Image: "main",
					Readyz: &atev1alpha1.ContainerReadyz{
						HTTPGet:        &atev1alpha1.HTTPGetAction{Path: "/health", Port: 8080},
						TimeoutSeconds: 45,
					},
				},
				{
					Name:  "without-probe",
					Image: "side",
				},
			},
		},
	}, nil)
	if err != nil {
		t.Fatalf("workloadSpecFromActorTemplate failed: %v", err)
	}

	want := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{
			{
				Name:  "with-probe",
				Image: "main",
				Readyz: &ateletpb.Readyz{
					HttpGet:        &ateletpb.HTTPGetAction{Path: "/health", Port: 8080},
					TimeoutSeconds: 45,
				},
			},
			{
				Name:  "without-probe",
				Image: "side",
			},
		},
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("WorkloadSpec mismatch (-want +got):\n%s", diff)
	}
}

func TestAppendExternalVolumes(t *testing.T) {
	template := &atev1alpha1.ActorTemplate{
		Spec: atev1alpha1.ActorTemplateSpec{
			Containers: []atev1alpha1.Container{
				{
					Name: "main",
					VolumeMounts: []atev1alpha1.VolumeMount{
						{Name: "vol-1", MountPath: "/mnt/vol1"},
					},
				},
			},
			Volumes: []atev1alpha1.Volume{
				{
					Name: "vol-1",
					VolumeSource: atev1alpha1.VolumeSource{
						ExternalVolumeTemplate: &atev1alpha1.ExternalVolumeTemplate{
							StorageClassName: "pd-standard",
						},
					},
				},
				{
					Name: "vol-2",
					VolumeSource: atev1alpha1.VolumeSource{
						DurableDir: &atev1alpha1.DurableDirVolumeSource{},
					},
				},
				{
					Name: "unmounted-vol",
					VolumeSource: atev1alpha1.VolumeSource{
						ExternalVolumeTemplate: &atev1alpha1.ExternalVolumeTemplate{
							StorageClassName: "pd-standard",
						},
					},
				},
			},
		},
	}

	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "space-abc",
			Name:     "actor-123",
		},
		Status: &ateapipb.ActorStatus{
			ActorVolumes: []*ateapipb.ExternalVolume{
				{
					VolumeName:      "vol-1",
					StorageVolumeId: "vol-gce-pd-123",
					VolumeType:      "pd-standard",
					VolumeContext:   map[string]string{"foo": "bar"},
				},
			},
		},
	}

	workloadSpec := &ateletpb.WorkloadSpec{}
	if err := appendExternalVolumes(workloadSpec, template, actor); err != nil {
		t.Fatalf("appendExternalVolumes unexpected error: %v", err)
	}

	want := &ateletpb.WorkloadSpec{
		Volumes: []*ateletpb.Volume{
			{
				Name: "vol-1",
				Type: ateletpb.VolumeType_VOLUME_TYPE_EXTERNAL,
				Source: &ateletpb.Volume_External{
					External: &ateletpb.ExternalVolumeSource{
						StorageVolumeId: "vol-gce-pd-123",
						VolumeType:      "pd-standard",
						VolumeContext:   map[string]string{"foo": "bar"},
					},
				},
			},
		},
	}

	if diff := cmp.Diff(want, workloadSpec, protocmp.Transform()); diff != "" {
		t.Errorf("appendExternalVolumes mismatch (-want +got):\n%s", diff)
	}

	// Test missing mounted volume returns an error
	missingActor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "space-abc",
			Name:     "actor-123",
		},
		Status: &ateapipb.ActorStatus{ActorVolumes: []*ateapipb.ExternalVolume{}},
	}
	if err := appendExternalVolumes(&ateletpb.WorkloadSpec{}, template, missingActor); err == nil {
		t.Errorf("appendExternalVolumes expected error for missing volume, got nil")
	}
}
