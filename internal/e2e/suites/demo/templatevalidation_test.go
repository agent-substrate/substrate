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

package demo

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/e2e"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// CreateActorTemplate rejects the malformed templates docs/api-guide.md says
// it rejects. Each row starts from the counter fixture's template.
func TestActorTemplateValidation(t *testing.T) {
	t.Parallel()
	nsObj := e2e.CreateNamespace(t)
	ctx := context.Background()
	clients := e2e.GetClients()

	src := e2e.SubstrateCounterFixture()
	base, err := clients.SubstrateAPI.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{
		ActorTemplate: &ateapi.ObjectRef{Atespace: src.Atespace, Name: src.Name},
	})
	if err != nil {
		t.Fatalf("failed to get ActorTemplate %s/%s (deploy with: %s): %v", src.Atespace, src.Name, src.DeployWith, err)
	}

	tests := []struct {
		name   string
		slug   string
		mutate func(*ateapi.ActorTemplate)
		want   string
	}{
		{
			name: "volume mounted by no container",
			slug: "unmounted-volume",
			mutate: func(at *ateapi.ActorTemplate) {
				at.Volumes = append(at.Volumes, &ateapi.Volume{Name: "orphan", DurableDir: &ateapi.DurableDirVolumeSource{}})
			},
			want: "orphan",
		},
		{
			name: "duplicate container names",
			slug: "duplicate-container",
			mutate: func(at *ateapi.ActorTemplate) {
				at.Containers = append(at.Containers, proto.CloneOf(at.Containers[0]))
			},
			want: "actor_template.containers",
		},
		{
			name:   "no resource limits",
			slug:   "no-limits",
			mutate: func(at *ateapi.ActorTemplate) { at.Resources = nil },
			want:   "actor_template.resources",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			at := proto.CloneOf(base)
			name := "validation-" + tc.slug + "-" + nsObj.Name
			at.Metadata = &ateapi.ResourceMetadata{Atespace: demoAtespace, Name: name}
			at.Status = nil
			// No pool carries this label, so an accepted template cannot
			// start a golden build either way.
			at.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"demo": nsObj.Name}}
			at.SnapshotConfig.GoldenSnapshotConfig = &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED}
			at.GoldenEgressPolicy = nil
			tc.mutate(at)

			_, err := clients.SubstrateAPI.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{ActorTemplate: at})
			if err == nil {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				if _, derr := clients.SubstrateAPI.DeleteActorTemplate(cleanupCtx, &ateapi.DeleteActorTemplateRequest{
					ActorTemplate: &ateapi.ObjectRef{Atespace: demoAtespace, Name: name},
				}); derr != nil {
					t.Logf("failed to delete ActorTemplate %s/%s: %v", demoAtespace, name, derr)
				}
				t.Fatalf("CreateActorTemplate accepted a template with %s", tc.name)
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("CreateActorTemplate with %s = %v, want InvalidArgument", tc.name, err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CreateActorTemplate with %s = %v, want error containing %q", tc.name, err, tc.want)
			}
			t.Logf("rejected: %v", err)
		})
	}
}
