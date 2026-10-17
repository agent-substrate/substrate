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

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// Chmod, not MkdirAll's mode: the umask narrows a fresh dir.
func TestPrepareDurableDirVolume(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vol")
	if err := prepareDurableDirVolume(path); err != nil {
		t.Fatal(err)
	}
	if got := dirMode(t, path); got != 0o777 {
		t.Errorf("mode = %o, want 777", got)
	}
}

func dirMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Mode().Perm()
}
