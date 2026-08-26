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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
			name: "converts SystemInfo volume with actorMetadata items",
			template: &atev1alpha1.ActorTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "tmpl1", Namespace: "agent-ns"},
				Spec: atev1alpha1.ActorTemplateSpec{
					Volumes: []atev1alpha1.Volume{
						{
							Name: "system-info",
							VolumeSource: atev1alpha1.VolumeSource{
								SystemInfo: &atev1alpha1.SystemInfoVolumeSource{
									DataSources: []atev1alpha1.SystemInfoDataSource{
										{ActorMetadata: &atev1alpha1.ActorMetadataDataSource{
											Items: []atev1alpha1.ActorMetadataItem{
												{Field: atev1alpha1.ActorMetadataFieldName, Path: "actor-name"},
												{Field: atev1alpha1.ActorMetadataFieldAtespace, Path: "atespace"},
												{Field: atev1alpha1.ActorMetadataFieldUID, Path: "identity/actor-uid"},
											},
										}},
									},
								},
							},
						},
					},
					Containers: []atev1alpha1.Container{
						{
							Name:  "main",
							Image: "main",
							VolumeMounts: []atev1alpha1.VolumeMount{
								{Name: "system-info", MountPath: "/run/ate"},
							},
						},
					},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Volumes: []*ateletpb.Volume{
					{
						Name: "system-info",
						Source: &ateletpb.Volume_SystemInfo{
							SystemInfo: &ateletpb.SystemInfoVolume{
								DataSources: []*ateletpb.SystemInfoDataSource{
									{DataSource: &ateletpb.SystemInfoDataSource_ActorMetadata{
										ActorMetadata: &ateletpb.ActorMetadataDataSource{
											Items: []*ateletpb.ActorMetadataItem{
												{Field: ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME, Path: "actor-name"},
												{Field: ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_ATESPACE, Path: "atespace"},
												{Field: ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_UID, Path: "identity/actor-uid"},
											},
										},
									}},
								},
							},
						},
					},
				},
				Containers: []*ateletpb.Container{
					{
						Name:  "main",
						Image: "main",
						VolumeMounts: []*ateletpb.VolumeMount{
							{Name: "system-info", MountPath: "/run/ate"},
						},
					},
				},
			},
		},
		{
			name: "converts Image volume and mounts",
			template: &atev1alpha1.ActorTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "tmpl1", Namespace: "agent-ns"},
				Spec: atev1alpha1.ActorTemplateSpec{
					Volumes: []atev1alpha1.Volume{
						{Name: "home", VolumeSource: atev1alpha1.VolumeSource{DurableDir: &atev1alpha1.DurableDirVolumeSource{}}},
						{Name: "agent", VolumeSource: atev1alpha1.VolumeSource{Image: &atev1alpha1.ImageVolumeSource{Reference: "example.com/agent@sha256:abc"}}},
					},
					Containers: []atev1alpha1.Container{
						{
							Name:  "main",
							Image: "main",
							VolumeMounts: []atev1alpha1.VolumeMount{
								{Name: "home", MountPath: "/home/user"},
								{Name: "agent", MountPath: "/ate"},
							},
						},
					},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Volumes: []*ateletpb.Volume{
					{
						Name:   "home",
						Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}},
					},
					{
						Name:   "agent",
						Source: &ateletpb.Volume_Image{Image: &ateletpb.ImageVolumeSource{Reference: "example.com/agent@sha256:abc"}},
					},
				},
				Containers: []*ateletpb.Container{
					{
						Name:  "main",
						Image: "main",
						VolumeMounts: []*ateletpb.VolumeMount{
							{Name: "home", MountPath: "/home/user"},
							{Name: "agent", MountPath: "/ate"},
						},
					},
				},
			},
		},
		{
			name: "converts trustBundle data sources carrying the name for node-side resolution",
			template: &atev1alpha1.ActorTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "tmpl1", Namespace: "agent-ns"},
				Spec: atev1alpha1.ActorTemplateSpec{
					Volumes: []atev1alpha1.Volume{
						{
							Name: "system-info",
							VolumeSource: atev1alpha1.VolumeSource{
								SystemInfo: &atev1alpha1.SystemInfoVolumeSource{
									DataSources: []atev1alpha1.SystemInfoDataSource{
										{TrustBundle: &atev1alpha1.TrustBundleDataSource{Name: "egress-trust", Path: "trust/ca.pem"}},
									},
								},
							},
						},
					},
					Containers: []atev1alpha1.Container{
						{
							Name:  "main",
							Image: "main",
							VolumeMounts: []atev1alpha1.VolumeMount{
								{Name: "system-info", MountPath: "/run/substrate/certs"},
							},
						},
					},
				},
			},
			// The NAME crosses the wire; atelet resolves it against its
			// allowlist and ClusterTrustBundle informer at write time.
			want: &ateletpb.WorkloadSpec{
				Volumes: []*ateletpb.Volume{
					{
						Name: "system-info",
						Source: &ateletpb.Volume_SystemInfo{
							SystemInfo: &ateletpb.SystemInfoVolume{
								DataSources: []*ateletpb.SystemInfoDataSource{
									{DataSource: &ateletpb.SystemInfoDataSource_TrustBundle{
										TrustBundle: &ateletpb.TrustBundleDataSource{Name: "egress-trust", Path: "trust/ca.pem"},
									}},
								},
							},
						},
					},
				},
				Containers: []*ateletpb.Container{
					{
						Name:  "main",
						Image: "main",
						VolumeMounts: []*ateletpb.VolumeMount{
							{Name: "system-info", MountPath: "/run/substrate/certs"},
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

func TestWorkloadSpecFromActorTemplatePropagatesSecurityContext(t *testing.T) {
	got, err := workloadSpecFromActorTemplate(&atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tmpl-caps", Namespace: "agent-ns"},
		Spec: atev1alpha1.ActorTemplateSpec{
			Containers: []atev1alpha1.Container{
				{
					Name:  "adjusted",
					Image: "main",
					SecurityContext: &atev1alpha1.SecurityContext{
						Capabilities: &atev1alpha1.Capabilities{
							Add:  []atev1alpha1.Capability{"NET_ADMIN"},
							Drop: []atev1alpha1.Capability{"ALL"},
						},
					},
				},
				{
					Name:  "unset",
					Image: "side",
				},
				{
					// An empty capabilities block asks for no adjustment, so
					// nothing is put on the wire for it.
					Name:            "empty",
					Image:           "third",
					SecurityContext: &atev1alpha1.SecurityContext{Capabilities: &atev1alpha1.Capabilities{}},
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
				Name:  "adjusted",
				Image: "main",
				SecurityContext: &ateletpb.SecurityContext{
					Capabilities: &ateletpb.Capabilities{
						Add:  []string{"NET_ADMIN"},
						Drop: []string{"ALL"},
					},
				},
			},
			{
				Name:  "unset",
				Image: "side",
			},
			{
				Name:  "empty",
				Image: "third",
			},
		},
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("WorkloadSpec mismatch (-want +got):\n%s", diff)
	}
}

func TestToAteletResources(t *testing.T) {
	tests := []struct {
		name string
		in   *atev1alpha1.ContainerResources
		want *ateletpb.ResourceLimits
	}{{
		name: "nil resources",
		in:   nil,
		want: nil,
	}, {
		name: "empty limits",
		in:   &atev1alpha1.ContainerResources{},
		want: nil,
	}, {
		name: "memory only",
		in: &atev1alpha1.ContainerResources{Limits: atev1alpha1.ContainerResourceList{
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		}},
		want: &ateletpb.ResourceLimits{MemoryBytes: 268435456},
	}, {
		name: "cpu only",
		in: &atev1alpha1.ContainerResources{Limits: atev1alpha1.ContainerResourceList{
			corev1.ResourceCPU: resource.MustParse("200m"),
		}},
		want: &ateletpb.ResourceLimits{CpuMillis: 200},
	}, {
		name: "both",
		in: &atev1alpha1.ContainerResources{Limits: atev1alpha1.ContainerResourceList{
			corev1.ResourceMemory: resource.MustParse("1Gi"),
			corev1.ResourceCPU:    resource.MustParse("1"),
		}},
		want: &ateletpb.ResourceLimits{MemoryBytes: 1073741824, CpuMillis: 1000},
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := toAteletResources(tc.in)
			if tc.want == nil {
				if got != nil {
					t.Fatalf("toAteletResources() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("toAteletResources() = nil, want %v", tc.want)
			}
			if got.GetMemoryBytes() != tc.want.GetMemoryBytes() {
				t.Errorf("memory_bytes = %d, want %d", got.GetMemoryBytes(), tc.want.GetMemoryBytes())
			}
			if got.GetCpuMillis() != tc.want.GetCpuMillis() {
				t.Errorf("cpu_millis = %d, want %d", got.GetCpuMillis(), tc.want.GetCpuMillis())
			}
		})
	}
}

// The limits a template declares must reach the workload spec atelet receives.
// Without this, removing the Resources field from the ateletpb.Container
// literal leaves every test in the repository green while limits never leave
// ate-api-server.
func TestWorkloadSpecFromActorTemplatePropagatesResources(t *testing.T) {
	got, err := workloadSpecFromActorTemplate(&atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tmpl-limits", Namespace: "agent-ns"},
		Spec: atev1alpha1.ActorTemplateSpec{
			SandboxClass: atev1alpha1.SandboxClassMicroVM,
			Containers: []atev1alpha1.Container{
				{
					Name:  "limited",
					Image: "main",
					Resources: &atev1alpha1.ContainerResources{
						Limits: atev1alpha1.ContainerResourceList{
							corev1.ResourceMemory: resource.MustParse("256Mi"),
							corev1.ResourceCPU:    resource.MustParse("200m"),
						},
					},
				},
				{Name: "unlimited", Image: "main"},
			},
		},
	}, nil)
	if err != nil {
		t.Fatalf("workloadSpecFromActorTemplate failed: %v", err)
	}
	if len(got.GetContainers()) != 2 {
		t.Fatalf("containers = %d, want 2", len(got.GetContainers()))
	}

	limited := got.GetContainers()[0].GetResources()
	if limited == nil {
		t.Fatal("limited container Resources = nil, want the declared limits")
	}
	if limited.GetMemoryBytes() != 268435456 {
		t.Errorf("memory_bytes = %d, want 268435456", limited.GetMemoryBytes())
	}
	if limited.GetCpuMillis() != 200 {
		t.Errorf("cpu_millis = %d, want 200", limited.GetCpuMillis())
	}
	if r := got.GetContainers()[1].GetResources(); r != nil {
		t.Errorf("unlimited container Resources = %v, want nil", r)
	}
}
