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

package mountinfo

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMountedRequiresFilesystemTypeAndMountRoot(t *testing.T) {
	mounted, err := Mounted("/", "apfs")
	if err != nil || !mounted {
		t.Fatalf("Mounted root = %v, %v, want true", mounted, err)
	}
	mounted, err = Mounted("/", "nfs")
	if err != nil || mounted {
		t.Fatalf("Mounted root as NFS = %v, %v, want false", mounted, err)
	}
	mounted, err = Mounted(t.TempDir(), "apfs")
	if err != nil || mounted {
		t.Fatalf("Mounted ordinary directory = %v, %v, want false", mounted, err)
	}
	rootLink := filepath.Join(t.TempDir(), "root")
	if err := os.Symlink("/", rootLink); err != nil {
		t.Fatal(err)
	}
	mounted, err = Mounted(rootLink, "apfs")
	if err != nil || !mounted {
		t.Fatalf("Mounted symlink to root = %v, %v, want true", mounted, err)
	}
}
