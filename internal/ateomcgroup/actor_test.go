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

package ateomcgroup

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

func TestNilActorLeaf(t *testing.T) {
	var leaf *ActorLeaf
	if leaf.SysProcAttr() != nil {
		t.Error("a nil leaf set SysProcAttr")
	}
	if err := leaf.Close(); err != nil {
		t.Error(err)
	}
}
