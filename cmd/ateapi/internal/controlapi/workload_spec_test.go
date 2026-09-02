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
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestWorkloadSpecFromActorTemplate(t *testing.T) {
	tests := []struct {
		name     string
		template *ateapipb.ActorTemplate
		want     *ateletpb.WorkloadSpec
	}{
		{
			name: "converts DurableDir volume and mounts",
			template: &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl1"},
				Volumes: []*ateapipb.Volume{
					{Name: "home", DurableDir: &ateapipb.DurableDirVolumeSource{}},
				},
				Containers: []*ateapipb.Container{
					{
						Name:  "main",
						Image: "main",
						VolumeMounts: []*ateapipb.VolumeMount{
							{Name: "home", MountPath: "/home/user"},
							{Name: "home", MountPath: "/workspace"},
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
			template: &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl1"},
				Volumes: []*ateapipb.Volume{
					{
						Name: "system-info",
						SystemInfo: &ateapipb.SystemInfoVolumeSource{
							DataSources: []*ateapipb.SystemInfoDataSource{
								{ActorMetadata: &ateapipb.ActorMetadataDataSource{
									Items: []*ateapipb.ActorMetadataItem{
										{Field: ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME, Path: "actor-name"},
										{Field: ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_ATESPACE, Path: "atespace"},
										{Field: ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_UID, Path: "identity/actor-uid"},
									},
								}},
							},
						},
					},
				},
				Containers: []*ateapipb.Container{
					{
						Name:  "main",
						Image: "main",
						VolumeMounts: []*ateapipb.VolumeMount{
							{Name: "system-info", MountPath: "/run/ate"},
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
			template: &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl1"},
				Volumes: []*ateapipb.Volume{
					{Name: "home", DurableDir: &ateapipb.DurableDirVolumeSource{}},
					{Name: "agent", Image: &ateapipb.ImageVolumeSource{Reference: "example.com/agent@sha256:abc"}},
				},
				Containers: []*ateapipb.Container{
					{
						Name:  "main",
						Image: "main",
						VolumeMounts: []*ateapipb.VolumeMount{
							{Name: "home", MountPath: "/home/user"},
							{Name: "agent", MountPath: "/ate"},
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
			template: &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl1"},
				Volumes: []*ateapipb.Volume{
					{
						Name: "system-info",
						SystemInfo: &ateapipb.SystemInfoVolumeSource{
							DataSources: []*ateapipb.SystemInfoDataSource{
								{TrustBundle: &ateapipb.TrustBundleDataSource{Name: "egress-trust", Path: "trust/ca.pem"}},
							},
						},
					},
				},
				Containers: []*ateapipb.Container{
					{
						Name:  "main",
						Image: "main",
						VolumeMounts: []*ateapipb.VolumeMount{
							{Name: "system-info", MountPath: "/run/substrate/certs"},
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
			template: &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl1"},
				Volumes: []*ateapipb.Volume{
					{Name: "unsupported"},
					{Name: "home", DurableDir: &ateapipb.DurableDirVolumeSource{}},
				},
				Containers: []*ateapipb.Container{
					{
						Name:  "main",
						Image: "main",
						VolumeMounts: []*ateapipb.VolumeMount{
							{Name: "home", MountPath: "/workspace"},
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
			template: &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl1"},
				Volumes: []*ateapipb.Volume{
					{Name: "home", DurableDir: &ateapipb.DurableDirVolumeSource{}},
				},
				Containers: []*ateapipb.Container{
					{Name: "main", Image: "main"},
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
			template: &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl1"},
				Containers: []*ateapipb.Container{
					{
						Name:  "main",
						Image: "main",
						Env: []*ateapipb.EnvVar{
							{Name: "LITERAL", Value: "plain"},
							{Name: "EMPTY", Value: ""},
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
			template: &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl1"},
				Containers: []*ateapipb.Container{
					{
						Name:    "main",
						Image:   "main",
						Command: []string{"/entrypoint"},
						Args:    []string{"--foo", "--bar"},
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
	got, err := workloadSpecFromActorTemplate(&ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl-readyz"},
		Containers: []*ateapipb.Container{
			{
				Name:  "with-probe",
				Image: "main",
				Readyz: &ateapipb.ContainerReadyz{
					HttpGet:        &ateapipb.HTTPGetAction{Path: "/health", Port: 8080},
					TimeoutSeconds: 45,
				},
			},
			{
				Name:  "without-probe",
				Image: "side",
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
	template := &ateapipb.ActorTemplate{
		Containers: []*ateapipb.Container{
			{
				Name: "main",
				VolumeMounts: []*ateapipb.VolumeMount{
					{Name: "vol-1", MountPath: "/mnt/vol1"},
				},
			},
		},
		Volumes: []*ateapipb.Volume{
			{
				Name: "vol-1",
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					StorageClassName: "pd-standard",
				},
			},
			{
				Name:       "vol-2",
				DurableDir: &ateapipb.DurableDirVolumeSource{},
			},
			{
				Name: "unmounted-vol",
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					StorageClassName: "pd-standard",
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
	got, err := workloadSpecFromActorTemplate(&ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl-caps"},
		Containers: []*ateapipb.Container{
			{
				Name:  "adjusted",
				Image: "main",
				SecurityContext: &ateapipb.SecurityContext{
					Capabilities: &ateapipb.Capabilities{
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
				// An empty capabilities block asks for no adjustment, so
				// nothing is put on the wire for it.
				Name:            "empty",
				Image:           "third",
				SecurityContext: &ateapipb.SecurityContext{Capabilities: &ateapipb.Capabilities{}},
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
		name    string
		in      *ateapipb.Resources
		want    *ateletpb.ResourceLimits
		wantErr bool
	}{{
		name: "nil resources",
		in:   nil,
		want: nil,
	}, {
		name: "empty limits",
		in:   &ateapipb.Resources{},
		want: nil,
	}, {
		name: "memory only",
		in:   &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "memory", Quantity: "256Mi"}}},
		want: &ateletpb.ResourceLimits{MemoryBytes: 268435456},
	}, {
		name: "cpu only",
		in:   &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "cpu", Quantity: "200m"}}},
		want: &ateletpb.ResourceLimits{CpuMillis: 200},
	}, {
		name: "both",
		in: &ateapipb.Resources{Limits: []*ateapipb.Limits{
			{Name: "cpu", Quantity: "1"},
			{Name: "memory", Quantity: "1Gi"},
		}},
		want: &ateletpb.ResourceLimits{MemoryBytes: 1073741824, CpuMillis: 1000},
	}, {
		name:    "invalid quantity",
		in:      &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "cpu", Quantity: "banana"}}},
		wantErr: true,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := toAteletResources(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatal("toAteletResources() error = nil, want parse error")
				}
				return
			}
			if err != nil {
				t.Fatalf("toAteletResources() failed: %v", err)
			}
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
	got, err := workloadSpecFromActorTemplate(&ateapipb.ActorTemplate{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl-limits"},
		SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM},
		Containers: []*ateapipb.Container{
			{
				Name:  "limited",
				Image: "main",
				Resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{
					{Name: "cpu", Quantity: "200m"},
					{Name: "memory", Quantity: "256Mi"},
				}},
			},
			{Name: "unlimited", Image: "main"},
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
