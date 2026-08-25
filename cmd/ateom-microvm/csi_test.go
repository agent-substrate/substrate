//go:build linux

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

package main

import (
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/google/go-cmp/cmp"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func TestHasCsiVolumes(t *testing.T) {
	tests := []struct {
		name       string
		containers []*ateompb.Container
		want       bool
	}{
		{
			name:       "empty containers",
			containers: nil,
			want:       false,
		},
		{
			name: "no CSI volumes",
			containers: []*ateompb.Container{
				{
					Name: "c1",
					DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{
						{VolumeName: "data", MountPath: "/data"},
					},
				},
			},
			want: false,
		},
		{
			name: "has CSI volumes",
			containers: []*ateompb.Container{
				{
					Name: "c1",
					CsiVolumeMounts: []*ateompb.VolumeMount{
						{VolumeName: "csi-vol", MountPath: "/csi"},
					},
				},
			},
			want: true,
		},
		{
			name: "multiple containers, one has CSI",
			containers: []*ateompb.Container{
				{
					Name: "c1",
				},
				{
					Name: "c2",
					CsiVolumeMounts: []*ateompb.VolumeMount{
						{VolumeName: "csi-vol", MountPath: "/csi"},
					},
				},
			},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasCsiVolumes(tc.containers); got != tc.want {
				t.Errorf("hasCsiVolumes() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCsiMounts(t *testing.T) {
	mounts := []*ateompb.VolumeMount{
		{VolumeName: "vol1", MountPath: "/mnt/vol1"},
		{VolumeName: "vol2", MountPath: "/mnt/vol2"},
	}

	want := []specs.Mount{
		{
			Destination: "/mnt/vol1",
			Source:      kata.GuestCSIVolumeDir("vol1"),
			Type:        "bind",
			Options:     []string{"rbind", "rw"},
		},
		{
			Destination: "/mnt/vol2",
			Source:      kata.GuestCSIVolumeDir("vol2"),
			Type:        "bind",
			Options:     []string{"rbind", "rw"},
		},
	}

	got := csiMounts(mounts)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("csiMounts() mismatch (-want +got):\n%s", diff)
	}
}
