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
