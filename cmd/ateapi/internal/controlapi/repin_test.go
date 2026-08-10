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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCheckVolumesUnchanged(t *testing.T) {
	version := func(name string, mutations ...func(*ateapipb.ActorTemplateVersionSpec)) *ateapipb.ActorTemplateVersion {
		return &ateapipb.ActorTemplateVersion{
			Metadata: &ateapipb.ResourceMetadata{Name: name},
			Spec:     validTemplateVersionSpec(mutations...),
		}
	}

	tests := []struct {
		name   string
		next   *ateapipb.ActorTemplateVersion
		wantOK bool
	}{{
		"identical volumes",
		version("v2"),
		true,
	}, {
		"new image, same volumes",
		version("v2", func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Containers[0].Image = "app@sha256:fff"
		}),
		true,
	}, {
		"same mounts under a renamed container",
		version("v2", func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Containers[0].Name = "renamed"
		}),
		true,
	}, {
		"added volume",
		version("v2", func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Volumes = append(s.Volumes, &ateapipb.Volume{
				Name:   "extra",
				Source: &ateapipb.Volume_DurableDir{DurableDir: &ateapipb.DurableDirVolumeSource{}},
			})
		}),
		false,
	}, {
		"removed volume",
		version("v2", func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Volumes = nil
			s.Containers[0].VolumeMounts = nil
		}),
		false,
	}, {
		"changed volume source",
		version("v2", func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Volumes[0].Source = &ateapipb.Volume_ExternalVolumeTemplate{
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{Capacity: "1Gi", StorageClassName: "fast"},
			}
		}),
		false,
	}, {
		"changed mount path",
		version("v2", func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Containers[0].VolumeMounts[0].MountPath = "/data-v2"
		}),
		false,
	}}
	current := version("v1")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkVolumesUnchanged(current, tt.next)
			if tt.wantOK && err != nil {
				t.Fatalf("checkVolumesUnchanged() = %v, want nil", err)
			}
			if !tt.wantOK {
				if status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("checkVolumesUnchanged() = %v, want FailedPrecondition", err)
				}
			}
		})
	}
}
