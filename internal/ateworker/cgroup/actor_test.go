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

func TestNilActorLeaf(t *testing.T) {
	var leaf *ActorLeaf
	if leaf.SysProcAttr() != nil {
		t.Error("a nil leaf set SysProcAttr")
	}
	if err := leaf.SetCPUWeight(1000); err != nil {
		t.Error(err)
	}
	if err := leaf.Close(); err != nil {
		t.Error(err)
	}
}

func TestActorLeafSetCPUWeight(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "uid-a"), 0o755); err != nil {
		t.Fatal(err)
	}
	weightPath := filepath.Join(root, "uid-a", "cpu.weight")
	if err := os.WriteFile(weightPath, []byte("100"), 0o644); err != nil {
		t.Fatal(err)
	}
	leaf, err := openActorLeaf(root, "uid-a", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer leaf.Close()

	// openActorLeaf starts the leaf at MaxCPUWeight for startup.
	if got, _ := os.ReadFile(weightPath); string(got) != "10000" {
		t.Fatalf("initial cpu.weight = %q, want \"10000\"", got)
	}

	// Steady priority (100), then transition priority (10000).
	for _, tc := range []struct {
		w    int
		want string
	}{
		{DefaultCPUWeight, "100"},
		{MaxCPUWeight, "10000"},
		{DefaultCPUWeight, "100"},
	} {
		if err := leaf.SetCPUWeight(tc.w); err != nil {
			t.Fatalf("SetCPUWeight(%d): %v", tc.w, err)
		}
		if got, _ := os.ReadFile(weightPath); string(got) != tc.want {
			t.Errorf("after SetCPUWeight(%d), cpu.weight = %q, want %q", tc.w, got, tc.want)
		}
	}

	// Out-of-range weights are rejected without touching the file.
	for _, bad := range []int{-1, 0, 10001} {
		if err := leaf.SetCPUWeight(bad); err == nil {
			t.Errorf("SetCPUWeight(%d) succeeded, want an error", bad)
		}
	}

	// SetActorLeafCPUWeightUnder updates the leaf by name without an open handle
	// (used on checkpoint/teardown to raise a running actor's weight).
	if err := SetActorLeafCPUWeightUnder(root, "uid-a", MaxCPUWeight); err != nil {
		t.Fatalf("SetActorLeafCPUWeightUnder(10000): %v", err)
	}
	if got, _ := os.ReadFile(weightPath); string(got) != "10000" {
		t.Errorf("after SetActorLeafCPUWeightUnder(10000), cpu.weight = %q, want \"10000\"", got)
	}
	// Missing leaf is a no-op.
	if err := SetActorLeafCPUWeightUnder(root, "no-such-actor", MaxCPUWeight); err != nil {
		t.Errorf("SetActorLeafCPUWeightUnder on missing leaf: %v", err)
	}
	// Unsafe name is rejected.
	if err := SetActorLeafCPUWeightUnder(root, "../escape", DefaultCPUWeight); err == nil {
		t.Error("SetActorLeafCPUWeightUnder(../escape) succeeded, want an error")
	}

	// Missing cpu.weight (no cpu controller) is a no-op.
	if err := os.Remove(weightPath); err != nil {
		t.Fatal(err)
	}
	if err := leaf.SetCPUWeight(500); err != nil {
		t.Errorf("SetCPUWeight without cpu.weight: %v", err)
	}
}

func BenchmarkActorLeafCPUWeightTransition(b *testing.B) {
	root := b.TempDir()
	if err := os.Mkdir(filepath.Join(root, "uid-bench"), 0o755); err != nil {
		b.Fatal(err)
	}
	weightPath := filepath.Join(root, "uid-bench", "cpu.weight")
	if err := os.WriteFile(weightPath, []byte("100"), 0o644); err != nil {
		b.Fatal(err)
	}
	leaf, err := openActorLeaf(root, "uid-bench", 0)
	if err != nil {
		b.Fatal(err)
	}
	defer leaf.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := leaf.SetCPUWeight(10000); err != nil {
			b.Fatal(err)
		}
		if err := leaf.SetCPUWeight(100); err != nil {
			b.Fatal(err)
		}
	}
}
