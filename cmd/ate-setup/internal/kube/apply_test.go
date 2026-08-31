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

package kube

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// Teardown walks a fixed list of manifests covering every install shape, so a
// path the running configuration never referenced is expected to be absent.
//
// The zero-value Client has no cluster connection: reaching the delete calls
// would panic, which is the point. A missing path must short-circuit before
// any request rather than fail the surrounding teardown loop.
func TestDeletePathIgnoresMissingPath(t *testing.T) {
	c := &Client{}

	for _, name := range []string{"absent.yaml", "absent-dir"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), name)
			if err := c.DeletePath(context.Background(), path); err != nil {
				t.Errorf("DeletePath(%q) = %v, want nil", path, err)
			}
		})
	}
}

// A path that exists but cannot be parsed is a real problem and must still
// surface, so the ErrNotExist check above does not become a blanket catch.
func TestDeletePathReportsUnparseableManifest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "broken.yaml")
	if err := os.WriteFile(path, []byte("kind: [unterminated\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := (&Client{}).DeletePath(context.Background(), path); err == nil {
		t.Error("DeletePath() = nil, want an error for an unparseable manifest")
	}
}
