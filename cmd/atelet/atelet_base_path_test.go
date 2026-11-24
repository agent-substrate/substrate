// Copyright 2026 The Agent Substrate Authors
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
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareAteletBasePath(t *testing.T) {
	root := t.TempDir()
	// kubelet creates the hostPath 0755.
	existing := filepath.Join(root, "existing")
	if err := os.Mkdir(existing, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{existing, filepath.Join(root, "missing")} {
		if err := prepareAteletBasePath(dir); err != nil {
			t.Fatalf("prepareAteletBasePath(%s): %v", dir, err)
		}
		if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o700 {
			t.Errorf("%s: Stat = (%v, %v), want mode 0700", dir, fi, err)
		}
	}
}
