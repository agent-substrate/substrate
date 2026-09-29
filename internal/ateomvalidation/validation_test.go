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

package ateomvalidation

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// Every request field is gated optional and the message-typed ones are
// opaque, so nothing is rejected yet. These tests hold that line: an empty
// request and a fully populated one both pass, for every RPC. Rules land
// field by field in follow-ups, each with its own negative cases.

func fullSpec() *ateompb.WorkloadSpec {
	return &ateompb.WorkloadSpec{Containers: []*ateompb.Container{{
		Name:                   "main",
		WakeupProbe:            &ateompb.WakeupProbe{HttpGet: &ateompb.HttpGetAction{Path: "/healthz", Port: 8080}, TimeoutSeconds: 30},
		DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{{VolumeName: "data", MountPath: "/data"}},
		CsiVolumeMounts:        []*ateompb.VolumeMount{{VolumeName: "csi", MountPath: "/csi"}},
		SystemInfoVolumeMounts: []*ateompb.SystemInfoVolumeMount{{VolumeName: "sysinfo", MountPath: "/etc/sysinfo"}},
		ImageVolumeMounts:      []*ateompb.ImageVolumeMount{{VolumeName: "img", MountPath: "/img"}},
	}}}
}

func fullActorDirs() *ateompb.ActorDirs {
	return &ateompb.ActorDirs{
		RootDir:                   "/actors/uid",
		OciBundleDir:              "/actors/uid/bundles",
		CheckpointDir:             "/actors/uid/checkpoint",
		RestoreDir:                "/actors/uid/restore",
		DurableDirVolumeMountsDir: "/actors/uid/durable",
		SystemInfoVolumeRootsDir:  "/actors/uid/sysinfo",
		VolumesDir:                "/actors/uid/volumes",
	}
}

func TestValidateRunWorkloadRequest(t *testing.T) {
	for name, req := range map[string]*ateompb.RunWorkloadRequest{
		"empty": {},
		"full": {
			Atespace:              "team-a",
			ActorName:             "actor-1",
			ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
			ActorTemplateAtespace: "team-a",
			ActorTemplateName:     "template-1",
			RunscPath:             "/opt/runsc",
			Spec:                  fullSpec(),
			RuntimeAssetPaths:     map[string]string{"kata-kernel": "/opt/kernel"},
			EgressGateway:         &ateompb.EgressGateway{Address: "gateway:443"},
			CpuMilli:              1500,
			MemoryBytes:           1 << 30,
			ActorDirs:             fullActorDirs(),
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateRunWorkloadRequest(context.Background(), req); err != nil {
				t.Fatalf("ValidateRunWorkloadRequest() = %v, want nil", err)
			}
		})
	}
}

func TestValidateCheckpointWorkloadRequest(t *testing.T) {
	for name, req := range map[string]*ateompb.CheckpointWorkloadRequest{
		"empty": {},
		"full": {
			Atespace:              "team-a",
			ActorName:             "actor-1",
			ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
			ActorTemplateAtespace: "team-a",
			ActorTemplateName:     "template-1",
			RunscPath:             "/opt/runsc",
			Spec:                  fullSpec(),
			SnapshotUri:           "gs://bucket/root/atespaces/team-a/actors/uid/snapshots/1",
			RuntimeAssetPaths:     map[string]string{"kata-kernel": "/opt/kernel"},
			Fidelity:              ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			ActorDirs:             fullActorDirs(),
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateCheckpointWorkloadRequest(context.Background(), req); err != nil {
				t.Fatalf("ValidateCheckpointWorkloadRequest() = %v, want nil", err)
			}
		})
	}
}

func TestValidateRestoreWorkloadRequest(t *testing.T) {
	for name, req := range map[string]*ateompb.RestoreWorkloadRequest{
		"empty": {},
		"full": {
			Atespace:              "team-a",
			ActorName:             "actor-1",
			ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
			ActorTemplateAtespace: "team-a",
			ActorTemplateName:     "template-1",
			RunscPath:             "/opt/runsc",
			Spec:                  fullSpec(),
			SnapshotUri:           "gs://bucket/root/atespaces/team-a/actors/uid/snapshots/1",
			RuntimeAssetPaths:     map[string]string{"kata-kernel": "/opt/kernel"},
			Fidelity:              ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			EgressGateway:         &ateompb.EgressGateway{Address: "gateway:443"},
			CpuMilli:              1500,
			MemoryBytes:           1 << 30,
			ActorDirs:             fullActorDirs(),
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateRestoreWorkloadRequest(context.Background(), req); err != nil {
				t.Fatalf("ValidateRestoreWorkloadRequest() = %v, want nil", err)
			}
		})
	}
}

func TestValidateTerminateWorkloadRequest(t *testing.T) {
	for name, req := range map[string]*ateompb.TerminateWorkloadRequest{
		"empty": {},
		"full": {
			Atespace:              "team-a",
			ActorName:             "actor-1",
			ActorUid:              "01234567-89ab-cdef-0123-456789abcdef",
			ActorTemplateAtespace: "team-a",
			ActorTemplateName:     "template-1",
			RunscPath:             "/opt/runsc",
			Spec:                  fullSpec(),
			ActorDirs:             fullActorDirs(),
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateTerminateWorkloadRequest(context.Background(), req); err != nil {
				t.Fatalf("ValidateTerminateWorkloadRequest() = %v, want nil", err)
			}
		})
	}
}

func TestValidateGetWorkloadStatsRequest(t *testing.T) {
	for name, req := range map[string]*ateompb.GetWorkloadStatsRequest{
		"empty": {},
		"full":  {ActorUid: "01234567-89ab-cdef-0123-456789abcdef"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateGetWorkloadStatsRequest(context.Background(), req); err != nil {
				t.Fatalf("ValidateGetWorkloadStatsRequest() = %v, want nil", err)
			}
		})
	}
}
