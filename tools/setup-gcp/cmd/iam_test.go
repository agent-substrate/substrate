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

import "testing"

func TestValidateIamFlags(t *testing.T) {
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
			name:           "missing project number",
			cfg:            Config{ProjectID: "test-project"},
			bucketBindings: true,
			want:           "--project-number is required",
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
			name:           "all required flags present",
			cfg:            Config{ProjectID: "test-project", ProjectNumber: "123", BucketName: "test-bucket"},
			bucketBindings: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateIamFlags(&tt.cfg, tt.bucketBindings)
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
