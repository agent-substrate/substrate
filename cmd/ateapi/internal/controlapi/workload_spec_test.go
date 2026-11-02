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

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestWorkloadSpecFromActorTemplate(t *testing.T) {
	tests := []struct {
		name     string
		template *ateapi.ActorTemplate
		want     *ateletpb.WorkloadSpec
	}{
		{
			name: "converts DurableDir volume and mounts",
			template: &ateapi.ActorTemplate{
				Metadata: &ateapi.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl1"},
				Volumes: []*ateapi.Volume{
					{Name: "home", DurableDir: &ateapi.DurableDirVolumeSource{}},
				},
				Containers: []*ateapi.Container{
					{
						Name:  "main",
						Image: "main",
						VolumeMounts: []*ateapi.VolumeMount{
							{Name: "home", MountPath: "/home/user"},
							{Name: "home", MountPath: "/workspace"},
						},
					},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Volumes: []*ateletpb.Volume{
					{
						Name:       "home",
						DurableDir: &ateletpb.DurableDirVolumeSource{},
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
			template: &ateapi.ActorTemplate{
				Metadata: &ateapi.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl1"},
				Volumes: []*ateapi.Volume{
					{
						Name: "system-info",
						SystemInfo: &ateapi.SystemInfoVolumeSource{
							DataSources: []*ateapi.SystemInfoDataSource{
								{ActorMetadata: &ateapi.ActorMetadataDataSource{
									Items: []*ateapi.ActorMetadataItem{
										{Field: ateapi.ActorMetadataField_ACTOR_METADATA_FIELD_NAME, Path: "actor-name"},
										{Field: ateapi.ActorMetadataField_ACTOR_METADATA_FIELD_ATESPACE, Path: "atespace"},
										{Field: ateapi.ActorMetadataField_ACTOR_METADATA_FIELD_UID, Path: "identity/actor-uid"},
									},
								}},
							},
						},
					},
				},
				Containers: []*ateapi.Container{
					{
						Name:  "main",
						Image: "main",
						VolumeMounts: []*ateapi.VolumeMount{
							{Name: "system-info", MountPath: "/run/ate"},
						},
					},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Volumes: []*ateletpb.Volume{
					{
						Name: "system-info",
						SystemInfo: &ateletpb.SystemInfoVolumeSource{
							DataSources: []*ateletpb.SystemInfoDataSource{
								{ActorMetadata: &ateletpb.ActorMetadataDataSource{
									Items: []*ateletpb.ActorMetadataItem{
										{Field: ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME, Path: "actor-name"},
										{Field: ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_ATESPACE, Path: "atespace"},
										{Field: ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_UID, Path: "identity/actor-uid"},
									},
								}},
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
			template: &ateapi.ActorTemplate{
				Metadata: &ateapi.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl1"},
				Volumes: []*ateapi.Volume{
					{Name: "home", DurableDir: &ateapi.DurableDirVolumeSource{}},
					{Name: "agent", Image: &ateapi.ImageVolumeSource{Reference: "example.com/agent@sha256:abc"}},
				},
				Containers: []*ateapi.Container{
					{
						Name:  "main",
						Image: "main",
						VolumeMounts: []*ateapi.VolumeMount{
							{Name: "home", MountPath: "/home/user"},
							{Name: "agent", MountPath: "/ate"},
						},
					},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Volumes: []*ateletpb.Volume{
					{
						Name:       "home",
						DurableDir: &ateletpb.DurableDirVolumeSource{},
					},
					{
						Name:  "agent",
						Image: &ateletpb.ImageVolumeSource{Reference: "example.com/agent@sha256:abc"},
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
			template: &ateapi.ActorTemplate{
				Metadata: &ateapi.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl1"},
				Volumes: []*ateapi.Volume{
					{
						Name: "system-info",
						SystemInfo: &ateapi.SystemInfoVolumeSource{
							DataSources: []*ateapi.SystemInfoDataSource{
								{TrustBundle: &ateapi.TrustBundleDataSource{Names: []string{"egress-trust"}, Path: "trust/ca.pem"}},
							},
						},
					},
				},
				Containers: []*ateapi.Container{
					{
						Name:  "main",
						Image: "main",
						VolumeMounts: []*ateapi.VolumeMount{
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
						SystemInfo: &ateletpb.SystemInfoVolumeSource{
							DataSources: []*ateletpb.SystemInfoDataSource{
								{TrustBundle: &ateletpb.TrustBundleDataSource{Names: []string{"egress-trust"}, Path: "trust/ca.pem"}},
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
			template: &ateapi.ActorTemplate{
				Metadata: &ateapi.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl1"},
				Volumes: []*ateapi.Volume{
					{Name: "unsupported"},
					{Name: "home", DurableDir: &ateapi.DurableDirVolumeSource{}},
				},
				Containers: []*ateapi.Container{
					{
						Name:  "main",
						Image: "main",
						VolumeMounts: []*ateapi.VolumeMount{
							{Name: "home", MountPath: "/workspace"},
						},
					},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Volumes: []*ateletpb.Volume{
					{
						Name:       "home",
						DurableDir: &ateletpb.DurableDirVolumeSource{},
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
			template: &ateapi.ActorTemplate{
				Metadata: &ateapi.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl1"},
				Volumes: []*ateapi.Volume{
					{Name: "home", DurableDir: &ateapi.DurableDirVolumeSource{}},
				},
				Containers: []*ateapi.Container{
					{Name: "main", Image: "main"},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Volumes: []*ateletpb.Volume{
					{
						Name:       "home",
						DurableDir: &ateletpb.DurableDirVolumeSource{},
					},
				},
				Containers: []*ateletpb.Container{{Name: "main", Image: "main"}},
			},
		},
		{
			name: "maps literal env",
			template: &ateapi.ActorTemplate{
				Metadata: &ateapi.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl1"},
				Containers: []*ateapi.Container{
					{
						Name:  "main",
						Image: "main",
						Env: []*ateapi.EnvVar{
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
					Env: []*ateletpb.EnvVar{
						{Name: "LITERAL", Value: "plain"},
						{Name: "EMPTY", Value: ""},
					},
				}},
			},
		},
		{
			name: "maps command and args",
			template: &ateapi.ActorTemplate{
				Metadata: &ateapi.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl1"},
				Containers: []*ateapi.Container{
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
			got, err := workloadSpecFromActorTemplate(tt.template, nil, nil)
			if err != nil {
				t.Fatalf("workloadSpecFromActorTemplate failed: %v", err)
			}
			if diff := cmp.Diff(tt.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("WorkloadSpec mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestWorkloadSpecFromActorTemplatePropagatesWakeupProbe(t *testing.T) {
	got, err := workloadSpecFromActorTemplate(&ateapi.ActorTemplate{
		Metadata: &ateapi.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl-wakeup-probe"},
		Containers: []*ateapi.Container{
			{
				Name:  "with-probe",
				Image: "main",
				WakeupProbe: &ateapi.WakeupProbe{
					HttpGet:        &ateapi.HttpGetAction{Path: "/health", Port: 8080},
					TimeoutSeconds: 45,
				},
			},
			{
				Name:  "without-probe",
				Image: "side",
			},
		},
	}, nil, nil)
	if err != nil {
		t.Fatalf("workloadSpecFromActorTemplate failed: %v", err)
	}

	want := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{
			{
				Name:  "with-probe",
				Image: "main",
				WakeupProbe: &ateletpb.WakeupProbe{
					HttpGet:        &ateletpb.HttpGetAction{Path: "/health", Port: 8080},
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
	template := &ateapi.ActorTemplate{
		Containers: []*ateapi.Container{
			{
				Name: "main",
				VolumeMounts: []*ateapi.VolumeMount{
					{Name: "vol-1", MountPath: "/mnt/vol1"},
				},
			},
		},
		Volumes: []*ateapi.Volume{
			{
				Name: "vol-1",
				External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
					StorageClassName: "pd-standard",
				}},
			},
			{
				Name:       "vol-2",
				DurableDir: &ateapi.DurableDirVolumeSource{},
			},
			{
				Name: "unmounted-vol",
				External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
					StorageClassName: "pd-standard",
				}},
			},
		},
	}

	actor := &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{
			Atespace: "space-abc",
			Name:     "actor-123",
		},
		Status: &ateapi.ActorStatus{
			ExternalVolumes: &ateapi.ExternalVolumes{Volumes: []*ateapi.ExternalVolumeStatus{
				{
					Name:            "vol-1",
					StorageVolumeId: "vol-gce-pd-123",
					VolumeType:      "pd-standard",
					VolumeContext:   map[string]string{"foo": "bar"},
				},
			}},
		},
	}

	for _, tt := range []struct {
		name                  string
		volumePublishContexts map[string]map[string]string
		wantPublishCtx        map[string]string
	}{
		{
			name:                  "with attachment metadata",
			volumePublishContexts: map[string]map[string]string{"vol-1": {"devicePath": "/dev/xvdba"}},
			wantPublishCtx:        map[string]string{"devicePath": "/dev/xvdba"},
		},
		{
			name: "without attachment metadata",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			workloadSpec := &ateletpb.WorkloadSpec{}
			if err := appendExternalVolumes(workloadSpec, template, actor, tt.volumePublishContexts); err != nil {
				t.Fatalf("appendExternalVolumes unexpected error: %v", err)
			}

			want := &ateletpb.WorkloadSpec{
				Volumes: []*ateletpb.Volume{
					{
						Name: "vol-1",
						External: &ateletpb.ExternalVolumeSource{
							StorageVolumeId: "vol-gce-pd-123",
							VolumeType:      "pd-standard",
							VolumeContext:   map[string]string{"foo": "bar"},
							PublishContext:  tt.wantPublishCtx,
						},
					},
				},
			}

			if diff := cmp.Diff(want, workloadSpec, protocmp.Transform()); diff != "" {
				t.Errorf("appendExternalVolumes mismatch (-want +got):\n%s", diff)
			}
		})
	}

	// Test missing mounted volume returns an error
	missingActor := &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{
			Atespace: "space-abc",
			Name:     "actor-123",
		},
		Status: &ateapi.ActorStatus{ExternalVolumes: &ateapi.ExternalVolumes{Volumes: []*ateapi.ExternalVolumeStatus{}}},
	}
	if err := appendExternalVolumes(&ateletpb.WorkloadSpec{}, template, missingActor, nil); err == nil {
		t.Errorf("appendExternalVolumes expected error for missing volume, got nil")
	}
}

func TestWorkloadSpecFromActorTemplatePropagatesSecurityContext(t *testing.T) {
	got, err := workloadSpecFromActorTemplate(&ateapi.ActorTemplate{
		Metadata: &ateapi.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl-caps"},
		Containers: []*ateapi.Container{
			{
				Name:  "adjusted",
				Image: "main",
				SecurityContext: &ateapi.SecurityContext{
					Capabilities: &ateapi.Capabilities{
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
				SecurityContext: &ateapi.SecurityContext{Capabilities: &ateapi.Capabilities{}},
			},
		},
	}, nil, nil)
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
		in   *ateapi.ResourceRequirements
		want *ateletpb.ResourceRequirements
	}{{
		name: "nil resources",
		in:   nil,
		want: nil,
	}, {
		name: "empty resources",
		in:   &ateapi.ResourceRequirements{},
		want: nil,
	}, {
		name: "memory only",
		in:   &ateapi.ResourceRequirements{Limits: &ateapi.Resources{MemoryBytes: 256 << 20}},
		want: &ateletpb.ResourceRequirements{Limits: &ateletpb.Resources{MemoryBytes: 268435456}},
	}, {
		name: "cpu only",
		in:   &ateapi.ResourceRequirements{Limits: &ateapi.Resources{CpuMillicores: 200}},
		want: &ateletpb.ResourceRequirements{Limits: &ateletpb.Resources{CpuMillicores: 200}},
	}, {
		name: "both",
		in:   &ateapi.ResourceRequirements{Limits: &ateapi.Resources{CpuMillicores: 1000, MemoryBytes: 1 << 30}},
		want: &ateletpb.ResourceRequirements{Limits: &ateletpb.Resources{MemoryBytes: 1073741824, CpuMillicores: 1000}},
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
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("toAteletResources() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// The limits a template declares must reach the workload spec atelet receives.
// Without this, removing the Resources field from the ateletpb.Container
// literal leaves every test in the repository green while limits never leave
// ate-api-server.
func TestWorkloadSpecFromActorTemplatePropagatesResources(t *testing.T) {
	got, err := workloadSpecFromActorTemplate(&ateapi.ActorTemplate{
		Metadata: &ateapi.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl-limits"},
		Containers: []*ateapi.Container{
			{
				Name:      "limited",
				Image:     "main",
				Resources: &ateapi.ResourceRequirements{Limits: &ateapi.Resources{CpuMillicores: 200, MemoryBytes: 256 << 20}},
			},
			{Name: "unlimited", Image: "main"},
		},
	}, nil, nil)
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
	if got := limited.GetLimits().GetMemoryBytes(); got != 268435456 {
		t.Errorf("memory_bytes = %d, want 268435456", got)
	}
	if got := limited.GetLimits().GetCpuMillicores(); got != 200 {
		t.Errorf("cpu_millicores = %d, want 200", got)
	}
	if r := got.GetContainers()[1].GetResources(); r != nil {
		t.Errorf("unlimited container Resources = %v, want nil", r)
	}
}
