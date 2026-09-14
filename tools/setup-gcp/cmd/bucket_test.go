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

func TestValidateBucketFlags(t *testing.T) {
	origGetProject := getProjectFn
	defer func() { getProjectFn = origGetProject }()

	getProjectFn = func(ctx context.Context, name string) (*resourcemanagerpb.Project, error) {
		return &resourcemanagerpb.Project{
			Name:      "projects/123456",
			ProjectId: "test-project",
		}, nil
	}

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "missing bucket name",
			cfg:  Config{ProjectID: "test-project"},
			want: "--name is required",
		},
		{
			name: "missing project id",
			cfg:  Config{BucketName: "test-bucket"},
			want: "--project-id is required",
		},
		{
			name: "missing project id when only project number provided",
			cfg:  Config{BucketName: "test-bucket", ProjectNumber: "123456"},
			want: "--project-id is required",
		},
		{
			name: "all required flags present with both id and number",
			cfg:  Config{ProjectID: "test-project", ProjectNumber: "123456", BucketName: "test-bucket"},
		},
		{
			name: "all required flags present with only project id",
			cfg:  Config{ProjectID: "test-project", BucketName: "test-bucket"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfgCopy := tt.cfg
			err := validateBucketFlags(t.Context(), &cfgCopy)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("validateBucketFlags() = %v; want nil", err)
				}
				if cfgCopy.ProjectNumber != "123456" {
					t.Errorf("cfgCopy.ProjectNumber = %q; want %q", cfgCopy.ProjectNumber, "123456")
				}
				return
			}
			if err == nil {
				t.Fatalf("validateBucketFlags() = nil; want %q", tt.want)
			}
			if err.Error() != tt.want {
				t.Errorf("validateBucketFlags() = %q; want %q", err.Error(), tt.want)
			}
		})
	}
}
