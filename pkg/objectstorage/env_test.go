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

package objectstorage

import (
	"context"
	"path/filepath"
	"testing"
)

func TestNewFromEnv(t *testing.T) {
	for _, tc := range []struct {
		name          string
		backend       string
		pathStyle     string
		wantS3        bool
		wantPathStyle bool
	}{
		{name: "unset selects GCS", backend: ""},
		{name: "unknown value selects GCS", backend: "gcs"},
		{name: "s3", backend: "s3", wantS3: true},
		{name: "s3 with path style", backend: "s3", pathStyle: "true", wantS3: true, wantPathStyle: true},
		{name: "s3 path style needs exactly true", backend: "s3", pathStyle: "1", wantS3: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// GCS: the emulator host makes storage.NewClient skip credential lookup.
			t.Setenv("STORAGE_EMULATOR_HOST", "127.0.0.1:1")
			// S3: keep the developer's AWS config out of the test.
			missing := filepath.Join(t.TempDir(), "missing")
			t.Setenv("AWS_CONFIG_FILE", missing)
			t.Setenv("AWS_SHARED_CREDENTIALS_FILE", missing)
			t.Setenv("AWS_REGION", "us-east-1")
			t.Setenv("ATE_STORAGE_BACKEND", tc.backend)
			t.Setenv("AWS_S3_USE_PATH_STYLE", tc.pathStyle)

			got, err := NewFromEnv(context.Background())
			if err != nil {
				t.Fatalf("NewFromEnv: %v", err)
			}
			switch c := got.(type) {
			case *s3Client:
				if !tc.wantS3 {
					t.Fatalf("NewFromEnv returned an S3 client, want GCS")
				}
				if got := c.client.Options().UsePathStyle; got != tc.wantPathStyle {
					t.Errorf("UsePathStyle = %v, want %v", got, tc.wantPathStyle)
				}
			case *gcsClient:
				if tc.wantS3 {
					t.Fatalf("NewFromEnv returned a GCS client, want S3")
				}
			default:
				t.Fatalf("NewFromEnv returned %T", got)
			}
		})
	}
}
