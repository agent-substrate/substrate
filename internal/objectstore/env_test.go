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

package objectstore_test

import (
	"os"
	"testing"

	"github.com/agent-substrate/substrate/internal/objectstore"
)

func TestStorageEnvironmentDefaults(t *testing.T) {
	for _, name := range []string{"ATE_STORAGE_BACKEND", "AWS_S3_USE_PATH_STYLE"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	if got := objectstore.BackendEnv.Get(); got != "gcs" {
		t.Errorf("unset storage backend = %q, want gcs", got)
	}
	if objectstore.S3PathStyleEnv.Get() {
		t.Error("unset S3 path-style addressing = true, want false")
	}
}

func TestStorageEnvironmentOverrides(t *testing.T) {
	for _, tc := range []struct {
		name, backend, pathStyle string
		wantPathStyle            bool
	}{
		{name: "empty"},
		{name: "GCS", backend: "gcs", pathStyle: "false"},
		{name: "S3", backend: "s3", pathStyle: "true", wantPathStyle: true},
		{name: "unknown backend and invalid boolean", backend: "unknown", pathStyle: "invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ATE_STORAGE_BACKEND", tc.backend)
			t.Setenv("AWS_S3_USE_PATH_STYLE", tc.pathStyle)
			if got := objectstore.BackendEnv.Get(); got != tc.backend {
				t.Errorf("storage backend = %q, want %q", got, tc.backend)
			}
			if got := objectstore.S3PathStyleEnv.Get(); got != tc.wantPathStyle {
				t.Errorf("S3 path-style addressing = %v, want %v", got, tc.wantPathStyle)
			}
		})
	}
}
