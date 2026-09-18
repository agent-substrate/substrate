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

//go:build linux

package sandboxd

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCreateTaskRequestBuildsManagedRootfs(t *testing.T) {
	bundle := t.TempDir()
	if err := os.Mkdir(filepath.Join(bundle, "rootfs"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "rootfs", "counter"), []byte("7\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "config.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	rootfs := filepath.Join(bundle, "rootfs.raw")
	req, err := createTaskRequest(&Task{TaskID: "task", Bundle: bundle, RootfsFile: rootfs, RootfsFSType: "ext4"})
	if err != nil {
		t.Fatal(err)
	}
	if len(req.GetRootfs()) != 1 || req.GetRootfs()[0].GetSource() != rootfs {
		t.Fatalf("rootfs = %+v", req.GetRootfs())
	}
	foundLoop := false
	for _, option := range req.GetRootfs()[0].GetOptions() {
		foundLoop = foundLoop || option == "loop"
	}
	if !foundLoop {
		t.Fatal("managed block rootfs lacks loop option")
	}
	if info, err := os.Stat(rootfs); err != nil || info.Size() != 512<<20 {
		t.Fatalf("rootfs file: info=%v err=%v", info, err)
	}
}

func TestManagedRootfsSizeGrowsWithContent(t *testing.T) {
	root := t.TempDir()
	f, err := os.Create(filepath.Join(root, "large"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(400 << 20); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	size, err := managedRootfsSize(root)
	if err != nil {
		t.Fatal(err)
	}
	if size <= 512<<20 || size%(1<<20) != 0 {
		t.Fatalf("managedRootfsSize() = %d", size)
	}
}
