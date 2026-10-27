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

	"go.ate.dev/substrate/api/ateapi/v1"
)

func TestTemplateHasExternalVolumeRef(t *testing.T) {
	tests := []struct {
		name    string
		volumes []*ateapi.Volume
		want    bool
	}{
		{name: "no volumes"},
		{name: "durable dir only", volumes: []*ateapi.Volume{{Name: "home", DurableDir: &ateapi.DurableDirVolumeSource{}}}},
		{name: "shared volume template", volumes: []*ateapi.Volume{{Name: "owned", External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{StorageClassName: "standard"}}}}},
		{name: "named ref", volumes: []*ateapi.Volume{{Name: "shared", External: &ateapi.ExternalVolumeSource{Ref: &ateapi.ExternalVolumeRef{Name: "ctx"}}}}, want: true},
		{name: "unnamed ref after other volumes", volumes: []*ateapi.Volume{
			{Name: "home", DurableDir: &ateapi.DurableDirVolumeSource{}},
			{Name: "shared", External: &ateapi.ExternalVolumeSource{Ref: &ateapi.ExternalVolumeRef{}}},
		}, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := templateHasExternalVolumeRef(&ateapi.ActorTemplate{Volumes: tc.volumes}); got != tc.want {
				t.Errorf("templateHasExternalVolumeRef() = %v, want %v", got, tc.want)
			}
		})
	}
}
