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
	"context"
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

func TestApplySnapshotManifest(t *testing.T) {
	spec := &ateletpb.WorkloadSpec{
		PauseImage: "registry.k8s.io/pause:3.10.2",
		Containers: []*ateletpb.Container{{
			Name:    "main",
			Image:   "busybox:latest",
			Command: []string{"sleep", "1"},
			Env: []*ateletpb.EnvEntry{{
				Name:  "A",
				Value: "B",
			}},
		}},
	}
	manifest := &snapshotManifest{
		Version: snapshotManifestVersion,
		Images: []snapshotManifestImage{{
			Name:        "pause",
			OriginalRef: "registry.k8s.io/pause:3.10.2",
			ResolvedRef: "registry.k8s.io/pause@sha256:pause",
		}, {
			Name:        "main",
			OriginalRef: "busybox:latest",
			ResolvedRef: "index.docker.io/library/busybox@sha256:main",
		}},
	}

	got, err := applySnapshotManifest(spec, manifest)
	if err != nil {
		t.Fatalf("applySnapshotManifest: %v", err)
	}
	if got.GetPauseImage() != "registry.k8s.io/pause@sha256:pause" {
		t.Errorf("pause image = %q", got.GetPauseImage())
	}
	if got.GetContainers()[0].GetImage() != "index.docker.io/library/busybox@sha256:main" {
		t.Errorf("container image = %q", got.GetContainers()[0].GetImage())
	}
	if spec.GetContainers()[0].GetImage() != "busybox:latest" {
		t.Errorf("applySnapshotManifest mutated input spec image to %q", spec.GetContainers()[0].GetImage())
	}
	if got.GetContainers()[0].GetCommand()[0] != "sleep" || got.GetContainers()[0].GetEnv()[0].GetValue() != "B" {
		t.Errorf("applySnapshotManifest did not preserve non-image fields: %+v", got.GetContainers()[0])
	}
}

func TestApplySnapshotManifestRequiresEveryImage(t *testing.T) {
	spec := &ateletpb.WorkloadSpec{
		PauseImage: "pause:latest",
		Containers: []*ateletpb.Container{{Name: "main", Image: "busybox:latest"}},
	}
	manifest := &snapshotManifest{
		Version: snapshotManifestVersion,
		Images: []snapshotManifestImage{{
			Name:        "pause",
			OriginalRef: "pause:latest",
			ResolvedRef: "pause@sha256:pause",
		}},
	}

	if _, err := applySnapshotManifest(spec, manifest); err == nil {
		t.Fatalf("applySnapshotManifest succeeded with missing container image")
	}
}

func TestWorkloadSpecImagesPinned(t *testing.T) {
	tests := []struct {
		name string
		spec *ateletpb.WorkloadSpec
		want bool
	}{{
		name: "all pinned",
		spec: &ateletpb.WorkloadSpec{
			PauseImage: "pause@sha256:pause",
			Containers: []*ateletpb.Container{{Name: "main", Image: "busybox@sha256:main"}},
		},
		want: true,
	}, {
		name: "tagged pause",
		spec: &ateletpb.WorkloadSpec{
			PauseImage: "pause:latest",
			Containers: []*ateletpb.Container{{Name: "main", Image: "busybox@sha256:main"}},
		},
		want: false,
	}, {
		name: "tagged container",
		spec: &ateletpb.WorkloadSpec{
			PauseImage: "pause@sha256:pause",
			Containers: []*ateletpb.Container{{Name: "main", Image: "busybox:latest"}},
		},
		want: false,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := workloadSpecImagesPinned(tt.spec); got != tt.want {
				t.Errorf("workloadSpecImagesPinned() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSnapshotManifestRoundTripSortsImages(t *testing.T) {
	manifest := &snapshotManifest{
		Version: snapshotManifestVersion,
		Images: []snapshotManifestImage{{
			Name:        "z",
			OriginalRef: "z:latest",
			ResolvedRef: "z@sha256:z",
		}, {
			Name:        "pause",
			OriginalRef: "pause:latest",
			ResolvedRef: "pause@sha256:pause",
		}, {
			Name:        "a",
			OriginalRef: "a:latest",
			ResolvedRef: "a@sha256:a",
		}},
	}

	manifestBytes, err := marshalSnapshotManifest(manifest)
	if err != nil {
		t.Fatalf("marshalSnapshotManifest: %v", err)
	}
	gotJSON := string(manifestBytes)
	if !(strings.Index(gotJSON, `"name": "a"`) < strings.Index(gotJSON, `"name": "pause"`) &&
		strings.Index(gotJSON, `"name": "pause"`) < strings.Index(gotJSON, `"name": "z"`)) {
		t.Fatalf("manifest images were not sorted by name:\n%s", gotJSON)
	}

	got, err := parseSnapshotManifest(manifestBytes)
	if err != nil {
		t.Fatalf("parseSnapshotManifest: %v", err)
	}
	if len(got.Images) != 3 || got.Images[0].Name != "a" || got.Images[1].Name != "pause" || got.Images[2].Name != "z" {
		t.Fatalf("round-tripped images = %+v", got.Images)
	}
}

func TestParseSnapshotManifestValidation(t *testing.T) {
	tests := []struct {
		name string
		json string
	}{{
		name: "unsupported version",
		json: `{"version":2,"images":[]}`,
	}, {
		name: "empty image name",
		json: `{"version":1,"images":[{"name":"","resolvedRef":"busybox@sha256:abc"}]}`,
	}, {
		name: "empty resolved ref",
		json: `{"version":1,"images":[{"name":"main","resolvedRef":""}]}`,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseSnapshotManifest([]byte(tt.json)); err == nil {
				t.Fatalf("parseSnapshotManifest succeeded")
			}
		})
	}
}

type missingObjectStorage struct{}

func (missingObjectStorage) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	return nil, fs.ErrNotExist
}

func (missingObjectStorage) PutObject(ctx context.Context, bucket, object string, reader io.Reader) error {
	return nil
}

type brokenObjectStorage struct{}

func (brokenObjectStorage) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	return nil, errors.New("permission denied")
}

func (brokenObjectStorage) PutObject(ctx context.Context, bucket, object string, reader io.Reader) error {
	return nil
}

func TestFetchSnapshotManifestNotFound(t *testing.T) {
	manifest, found, err := fetchSnapshotManifest(context.Background(), missingObjectStorage{}, "gs://bucket/snapshot")
	if err != nil {
		t.Fatalf("fetchSnapshotManifest: %v", err)
	}
	if found || manifest != nil {
		t.Fatalf("fetchSnapshotManifest found = %v, manifest = %+v; want missing", found, manifest)
	}
}

func TestFetchSnapshotManifestPropagatesRealErrors(t *testing.T) {
	if _, _, err := fetchSnapshotManifest(context.Background(), brokenObjectStorage{}, "gs://bucket/snapshot"); err == nil {
		t.Fatalf("fetchSnapshotManifest succeeded on non-not-found error")
	}
}
