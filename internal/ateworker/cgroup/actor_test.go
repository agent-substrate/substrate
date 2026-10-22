//go:build linux

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

package cgroup

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCPUMax(t *testing.T) {
	for _, tc := range []struct {
		milli int64
		want  string
	}{
		{0, "max 100000"},
		{500, "50000 100000"},
		{2000, "200000 100000"},
		// Below the kernel's 1ms floor.
		{5, "1000 100000"},
	} {
		if got := cpuMax(tc.milli); got != tc.want {
			t.Errorf("cpuMax(%d) = %q, want %q", tc.milli, got, tc.want)
		}
	}
}

// A leaf left by an earlier incarnation is reused, and its old cap replaced.
func TestOpenActorLeafReusesAndResetsALeftover(t *testing.T) {
	root := t.TempDir()
	// A regular directory stands in for cgroupfs, which provides cpu.max itself.
	if err := os.Mkdir(filepath.Join(root, "uid-a"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "uid-a", "cpu.max"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, milli := range []int64{500, 0} {
		leaf, err := openActorLeaf(root, "uid-a", milli)
		if err != nil {
			t.Fatal(err)
		}
		if err := leaf.Close(); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join(root, "uid-a", "cpu.max"))
		if err != nil {
			t.Fatal(err)
		}
		if want := cpuMax(milli); string(got) != want {
			t.Errorf("cpu.max = %q, want %q", got, want)
		}
	}
	if err := os.Remove(filepath.Join(root, "uid-a", "cpu.max")); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := removeActorLeaf(root, "uid-a"); err != nil {
			t.Fatalf("removeActorLeaf: %v", err)
		}
	}
}

// Without the cpu controller there is no cpu.max; the leaf still isolates.
func TestOpenActorLeafWithoutTheCPUController(t *testing.T) {
	root := t.TempDir()
	leaf, err := openActorLeaf(root, "uid-a", 500)
	if err != nil {
		t.Fatalf("openActorLeaf without cpu.max: %v", err)
	}
	_ = leaf.Close()
	if _, err := os.Stat(filepath.Join(root, "uid-a", "cpu.max")); !os.IsNotExist(err) {
		t.Errorf("cpu.max was created: %v", err)
	}
}

func TestActorLeafRejectsUnsafeNames(t *testing.T) {
	for _, name := range []string{"", ".", "..", "a/b", "../escape", workerLeaf, "nul\x00"} {
		if _, err := openActorLeaf(t.TempDir(), name, 0); err == nil {
			t.Errorf("openActorLeaf(%q) succeeded, want an error", name)
		}
	}
}

func TestLimitLeafGrowth(t *testing.T) {
	root := t.TempDir()
	// A regular directory stands in for cgroupfs, which provides both files itself.
	dir := filepath.Join(root, "uid-a")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.current"), []byte("1000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.high"), []byte("max\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	release, err := limitLeafGrowth(root, "uid-a", 24, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "memory.high")); string(got) != "1024" {
		t.Errorf("memory.high = %q, want %q", got, "1024")
	}
	if _, err := limitLeafGrowth(root, "uid-a", 24, 4096, 0); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "memory.high")); string(got) != "4096" {
		t.Errorf("memory.high with a ceiling = %q, want %q", got, "4096")
	}
	if _, err := limitLeafGrowth(root, "uid-a", 24, 4096, 100); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "memory.high")); string(got) != "4196" {
		t.Errorf("memory.high with a borrow = %q, want %q", got, "4196")
	}
	release()
	if got, _ := os.ReadFile(filepath.Join(dir, "memory.high")); string(got) != "max" {
		t.Errorf("memory.high after release = %q, want %q", got, "max")
	}
	// Releasing after the leaf is gone is a no-op.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	release()
}

// Without the memory controller there is no memory.high to write.
func TestLimitLeafGrowthWithoutTheMemoryController(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "uid-a"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := limitLeafGrowth(root, "uid-a", 24, 0, 0); err == nil {
		t.Error("limitLeafGrowth succeeded without memory.current")
	}
}

func TestPodMemory(t *testing.T) {
	root := t.TempDir()
	for name, content := range map[string]string{"memory.max": "4294967296\n", "memory.current": "1073741824\n"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	limit, current, err := podMemory(root)
	if err != nil || limit != 4<<30 || current != 1<<30 {
		t.Errorf("podMemory() = %d, %d, %v, want %d, %d", limit, current, err, int64(4<<30), int64(1<<30))
	}
	if err := os.WriteFile(filepath.Join(root, "memory.max"), []byte("max\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if limit, _, err := podMemory(root); err != nil || limit != 0 {
		t.Errorf("podMemory() without a limit = %d, %v, want 0", limit, err)
	}
}
