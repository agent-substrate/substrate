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

package router

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestFileATStore(t *testing.T) {
	tmpDir := t.TempDir()

	singleTmpl := `
apiVersion: substrate.storage.ate.dev/v1alpha1
kind: ActorTemplate
metadata:
  name: single-actor
  namespace: default
spec:
  snapshotsConfig:
    location: "gs://test-bucket/snapshots"
  workerSelector:
    matchLabels:
      workload: pool-1
`

	multiTmpl := `
apiVersion: substrate.storage.ate.dev/v1alpha1
kind: ActorTemplate
metadata:
  name: actor-a
  namespace: prod
spec:
  snapshotsConfig:
    location: "gs://prod-bucket/a"
  workerSelector:
    matchLabels:
      workload: heavy-pool
---
apiVersion: substrate.storage.ate.dev/v1alpha1
kind: ActorTemplate
metadata:
  name: actor-b
  namespace: prod
spec:
  snapshotsConfig:
    location: "gs://prod-bucket/b"
  workerSelector:
    matchLabels:
      workload: light-pool
`

	listTmpl := `
apiVersion: v1
kind: List
items:
- apiVersion: substrate.storage.ate.dev/v1alpha1
  kind: ActorTemplate
  metadata:
    name: list-a
    namespace: dev
  spec:
    snapshotsConfig:
      location: "gs://dev-bucket/a"
    workerSelector:
      matchLabels:
        workload: dev-pool
- apiVersion: substrate.storage.ate.dev/v1alpha1
  kind: ActorTemplate
  metadata:
    name: list-b
    namespace: dev
  spec:
    snapshotsConfig:
      location: "gs://dev-bucket/b"
    workerSelector:
      matchLabels:
        workload: dev-pool
`

	tests := []struct {
		name        string
		yamlContent string
		wantCount   int
		wantNames   []string
	}{
		{
			name:        "Single Document",
			yamlContent: singleTmpl,
			wantCount:   1,
			wantNames:   []string{"single-actor"},
		},
		{
			name:        "Multi Document Stream",
			yamlContent: multiTmpl,
			wantCount:   2,
			wantNames:   []string{"actor-a", "actor-b"},
		},
		{
			name:        "List structure",
			yamlContent: listTmpl,
			wantCount:   2,
			wantNames:   []string{"list-a", "list-b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filePath := filepath.Join(tmpDir, tt.name+".yaml")
			if err := os.WriteFile(filePath, []byte(tt.yamlContent), 0644); err != nil {
				t.Fatalf("failed to create test file: %v", err)
			}

			store := newFileATStore(filePath)
			templates, err := store.readyTemplates(context.Background())
			if err != nil {
				t.Fatalf("Templates() error: %v", err)
			}

			if len(templates) != tt.wantCount {
				t.Errorf("got %d items, expected %d", len(templates), tt.wantCount)
			}

			for i, wantName := range tt.wantNames {
				if i >= len(templates) {
					break
				}
				if templates[i].Name != wantName {
					t.Errorf("item %d name = %q, expected %q", i, templates[i].Name, wantName)
				}
			}
		})
	}
}
