// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
)

func TestValidateIamFlags(t *testing.T) {
	origGetProject := getProjectFn
	defer func() { getProjectFn = origGetProject }()

	getProjectFn = func(ctx context.Context, name string) (*resourcemanagerpb.Project, error) {
		return &resourcemanagerpb.Project{
			Name:      "projects/123",
			ProjectId: "test-project",
		}, nil
	}

	tooLong := strings.Repeat("b", 57)
	tooLongErr := `bucket name "` + tooLong + `" is too long: the asset bucket "` + tooLong + `-assets" would have 64 characters, and GCS allows 63`

	tests := []struct {
		name           string
		cfg            Config
		bucketBindings bool
		want           string
	}{
		{
			name:           "missing project id",
			cfg:            Config{},
			bucketBindings: true,
			want:           "--project-id is required",
		},
		{
			name:           "missing project id when only project number provided",
			cfg:            Config{ProjectNumber: "123"},
			bucketBindings: true,
			want:           "--project-id is required",
		},
		{
			name:           "only project id provided, bucket missing",
			cfg:            Config{ProjectID: "test-project"},
			bucketBindings: true,
			want:           "--bucket is required for bucket bindings",
		},
		{
			name:           "missing bucket while bucket bindings are requested",
			cfg:            Config{ProjectID: "test-project", ProjectNumber: "123"},
			bucketBindings: true,
			want:           "--bucket is required for bucket bindings",
		},
		{
			name:           "bucket name too long for the asset bucket",
			cfg:            Config{ProjectID: "test-project", BucketName: tooLong},
			bucketBindings: true,
			want:           tooLongErr,
		},
		{
			name:           "bucket not required when bucket bindings are disabled",
			cfg:            Config{ProjectID: "test-project", ProjectNumber: "123"},
			bucketBindings: false,
		},
		{
			name:           "all required flags present with both id and number",
			cfg:            Config{ProjectID: "test-project", ProjectNumber: "123", BucketName: "test-bucket"},
			bucketBindings: true,
		},
		{
			name:           "all required flags present with only project id",
			cfg:            Config{ProjectID: "test-project", BucketName: "test-bucket"},
			bucketBindings: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfgCopy := tt.cfg
			err := validateIamFlags(t.Context(), &cfgCopy, tt.bucketBindings)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("validateIamFlags() = %v; want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateIamFlags() = nil; want %q", tt.want)
			}
			if err.Error() != tt.want {
				t.Errorf("validateIamFlags() = %q; want %q", err.Error(), tt.want)
			}
		})
	}
}

func TestNodeGSAEmail(t *testing.T) {
	tests := []struct {
		name string
		cfg  *Config
		want string
	}{
		{
			name: "default service account name",
			cfg:  &Config{ProjectID: "my-project"},
			want: "substrate-gke-node@my-project.iam.gserviceaccount.com",
		},
		{
			name: "custom service account name",
			cfg:  &Config{ProjectID: "my-project", NodeGSAName: "custom-node-sa"},
			want: "custom-node-sa@my-project.iam.gserviceaccount.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nodeGSAEmail(tt.cfg); got != tt.want {
				t.Errorf("nodeGSAEmail() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseArtifactRegistryRepo(t *testing.T) {
	tests := []struct {
		repo     string
		wantProj string
		wantLoc  string
		wantRepo string
		wantOK   bool
	}{
		{"gcr.io/my-project", "my-project", "us", "gcr.io", true},
		{"eu.gcr.io/my-project/image", "my-project", "europe", "eu.gcr.io", true},
		{"us-central1-docker.pkg.dev/my-project/my-repo", "my-project", "us-central1", "my-repo", true},
		{"us-central1-docker.pkg.dev/my-project", "", "", "", false},
		{"ghcr.io/agent-substrate/substrate", "", "", "", false},
		{"", "", "", "", false},
		{"gcr.io", "", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.repo, func(t *testing.T) {
			p, l, r, ok := parseArtifactRegistryRepo(tt.repo)
			if ok != tt.wantOK || p != tt.wantProj || l != tt.wantLoc || r != tt.wantRepo {
				t.Errorf("parseArtifactRegistryRepo(%q) = (%q, %q, %q, %v), want (%q, %q, %q, %v)",
					tt.repo, p, l, r, ok, tt.wantProj, tt.wantLoc, tt.wantRepo, tt.wantOK)
			}
		})
	}
}

func TestImagePullGrantCommand(t *testing.T) {
	const sa = "node@p.iam.gserviceaccount.com"
	for repo, want := range map[string]string{
		"gcr.io/p/img": "gcloud artifacts repositories add-iam-policy-binding gcr.io --project=p --location=us --member=serviceAccount:" + sa + " --role=roles/artifactregistry.reader",
		"":             "gcloud artifacts repositories add-iam-policy-binding <repo> --project=<project> --location=<location> --member=serviceAccount:" + sa + " --role=roles/artifactregistry.reader",
	} {
		if got := imagePullGrantCommand(repo, sa); got != want {
			t.Errorf("imagePullGrantCommand(%q) = %q, want %q", repo, got, want)
		}
	}
}
