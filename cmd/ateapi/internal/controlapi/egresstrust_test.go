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
	"context"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

// egressTrustVolume is the volume injectEgressTrustVolume appends.
func egressTrustVolume() *ateletpb.Volume {
	return &ateletpb.Volume{
		Name: egressTrustVolumeName,
		Source: &ateletpb.Volume_SystemInfo{
			SystemInfo: &ateletpb.SystemInfoVolume{
				DataSources: []*ateletpb.SystemInfoDataSource{
					{DataSource: &ateletpb.SystemInfoDataSource_TrustBundle{
						TrustBundle: &ateletpb.TrustBundleDataSource{
							Name: egressTrustBundleName,
							Path: egressTrustBundleFile,
						},
					}},
				},
			},
		},
	}
}

func egressTrustMount() *ateletpb.VolumeMount {
	return &ateletpb.VolumeMount{Name: egressTrustVolumeName, MountPath: egressTrustMountPath}
}

func TestInjectEgressTrustVolume(t *testing.T) {
	tests := []struct {
		name string
		spec *ateletpb.WorkloadSpec
		want *ateletpb.WorkloadSpec
	}{
		{
			name: "mounts the bundle into every container",
			spec: &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{
					{Name: "main"},
					{Name: "sidecar", VolumeMounts: []*ateletpb.VolumeMount{{Name: "home", MountPath: "/home/user"}}},
				},
				Volumes: []*ateletpb.Volume{
					{Name: "home", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{
					{Name: "main", VolumeMounts: []*ateletpb.VolumeMount{egressTrustMount()}},
					{Name: "sidecar", VolumeMounts: []*ateletpb.VolumeMount{{Name: "home", MountPath: "/home/user"}, egressTrustMount()}},
				},
				Volumes: []*ateletpb.Volume{
					{Name: "home", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
					egressTrustVolume(),
				},
			},
		},
		{
			name: "a container mounting the reserved path keeps its own mount",
			spec: &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{
					{Name: "main", VolumeMounts: []*ateletpb.VolumeMount{{Name: "trust", MountPath: egressTrustMountPath}}},
					{Name: "sidecar"},
				},
				Volumes: []*ateletpb.Volume{
					{Name: "trust", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{
					{Name: "main", VolumeMounts: []*ateletpb.VolumeMount{{Name: "trust", MountPath: egressTrustMountPath}}},
					{Name: "sidecar", VolumeMounts: []*ateletpb.VolumeMount{egressTrustMount()}},
				},
				Volumes: []*ateletpb.Volume{
					{Name: "trust", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
					egressTrustVolume(),
				},
			},
		},
		{
			name: "no volume at all when every container overrides",
			spec: &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{
					{Name: "main", VolumeMounts: []*ateletpb.VolumeMount{{Name: "trust", MountPath: egressTrustMountPath}}},
				},
				Volumes: []*ateletpb.Volume{
					{Name: "trust", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{
					{Name: "main", VolumeMounts: []*ateletpb.VolumeMount{{Name: "trust", MountPath: egressTrustMountPath}}},
				},
				Volumes: []*ateletpb.Volume{
					{Name: "trust", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
				},
			},
		},
		{
			name: "a mount nested below the reserved path opts the container out",
			spec: &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{
					{Name: "main", VolumeMounts: []*ateletpb.VolumeMount{
						{Name: "extra", MountPath: egressTrustMountPath + "/extra"},
					}},
					{Name: "sidecar"},
				},
				Volumes: []*ateletpb.Volume{
					{Name: "extra", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{
					{Name: "main", VolumeMounts: []*ateletpb.VolumeMount{
						{Name: "extra", MountPath: egressTrustMountPath + "/extra"},
					}},
					{Name: "sidecar", VolumeMounts: []*ateletpb.VolumeMount{egressTrustMount()}},
				},
				Volumes: []*ateletpb.Volume{
					{Name: "extra", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
					egressTrustVolume(),
				},
			},
		},
		{
			name: "a mount above the reserved path does not override",
			spec: &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{
					{Name: "main", VolumeMounts: []*ateletpb.VolumeMount{
						{Name: "run", MountPath: "/run/substrate"},
					}},
				},
				Volumes: []*ateletpb.Volume{
					{Name: "run", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{
					{Name: "main", VolumeMounts: []*ateletpb.VolumeMount{
						{Name: "run", MountPath: "/run/substrate"},
						egressTrustMount(),
					}},
				},
				Volumes: []*ateletpb.Volume{
					{Name: "run", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
					egressTrustVolume(),
				},
			},
		},
		{
			// Guards the prefix boundary: a sibling sharing the reserved
			// path as a string prefix is not below it.
			name: "a sibling path sharing the prefix does not opt out",
			spec: &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{
					{Name: "main", VolumeMounts: []*ateletpb.VolumeMount{
						{Name: "certs-extra", MountPath: egressTrustMountPath + "-extra"},
					}},
				},
				Volumes: []*ateletpb.Volume{
					{Name: "certs-extra", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{
					{Name: "main", VolumeMounts: []*ateletpb.VolumeMount{
						{Name: "certs-extra", MountPath: egressTrustMountPath + "-extra"},
						egressTrustMount(),
					}},
				},
				Volumes: []*ateletpb.Volume{
					{Name: "certs-extra", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
					egressTrustVolume(),
				},
			},
		},
		{
			// An image volume above the path opts out where a durableDir
			// does not: the micro-VM runtime binds image volumes after
			// systemInfo, which would shadow the injected mount.
			name: "an image volume mounted above the reserved path opts the container out",
			spec: &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{
					{Name: "main", VolumeMounts: []*ateletpb.VolumeMount{
						{Name: "rootfs", MountPath: "/run/substrate"},
					}},
				},
				Volumes: []*ateletpb.Volume{
					{Name: "rootfs", Source: &ateletpb.Volume_Image{Image: &ateletpb.ImageVolumeSource{Reference: "img@sha256:abc"}}},
				},
			},
			want: &ateletpb.WorkloadSpec{
				Containers: []*ateletpb.Container{
					{Name: "main", VolumeMounts: []*ateletpb.VolumeMount{
						{Name: "rootfs", MountPath: "/run/substrate"},
					}},
				},
				Volumes: []*ateletpb.Volume{
					{Name: "rootfs", Source: &ateletpb.Volume_Image{Image: &ateletpb.ImageVolumeSource{Reference: "img@sha256:abc"}}},
				},
			},
		},
		{
			name: "no containers, no volume",
			spec: &ateletpb.WorkloadSpec{},
			want: &ateletpb.WorkloadSpec{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := injectEgressTrustVolume(context.Background(), nil, tt.spec); err != nil {
				t.Fatalf("injectEgressTrustVolume() error: %v", err)
			}
			if diff := cmp.Diff(tt.want, tt.spec, protocmp.Transform()); diff != "" {
				t.Errorf("injectEgressTrustVolume() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestInjectEgressTrustVolumeRejectsReservedName(t *testing.T) {
	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{Name: "main"}},
		Volumes: []*ateletpb.Volume{
			{Name: egressTrustVolumeName, Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
		},
	}
	err := injectEgressTrustVolume(context.Background(), nil, spec)
	if err == nil {
		t.Fatalf("injectEgressTrustVolume() = nil, want reserved-name error")
	}
	if !strings.Contains(err.Error(), egressTrustVolumeName) || !strings.Contains(err.Error(), "reserved") {
		t.Errorf("injectEgressTrustVolume() error %q, want it to name %q as reserved", err, egressTrustVolumeName)
	}
}

// TestWorkloadSpecInjectsEgressTrustVolume pins that the ActorWorkflow
// wrapper applies injection if and only if the deployment enables it.
func TestWorkloadSpecInjectsEgressTrustVolume(t *testing.T) {
	template := &ateapipb.ActorTemplate{
		Metadata:   &ateapipb.ResourceMetadata{Atespace: "agent-ns", Name: "tmpl1"},
		Containers: []*ateapipb.Container{{Name: "main", Image: "main"}},
	}

	for _, inject := range []bool{false, true} {
		w := NewActorWorkflow(nil, nil, nil, nil, nil, nil, "", inject, nil, nil)
		got, err := w.workloadSpec(context.Background(), template, nil)
		if err != nil {
			t.Fatalf("workloadSpec(inject=%v) error: %v", inject, err)
		}
		want := &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "main", Image: "main"}}}
		if inject {
			want.Containers[0].VolumeMounts = []*ateletpb.VolumeMount{egressTrustMount()}
			want.Volumes = []*ateletpb.Volume{egressTrustVolume()}
		}
		if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
			t.Errorf("workloadSpec(inject=%v) mismatch (-want +got):\n%s", inject, diff)
		}
	}
}
