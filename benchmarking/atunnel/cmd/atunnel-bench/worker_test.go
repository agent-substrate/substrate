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

package main

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestParseBytes(t *testing.T) {
	for in, want := range map[string]int64{"": 0, "512": 512, "4k": 4 << 10, "200m": 200 << 20, "1G": 1 << 30} {
		got, err := parseBytes(in)
		if err != nil || got != want {
			t.Errorf("parseBytes(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	if _, err := parseBytes("lots"); err == nil {
		t.Error("parseBytes accepted a non-number")
	}
}

// TestCreateCgroup writes the limits into a plain directory standing in for a
// cgroup.
func TestCreateCgroup(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tunnel-1")
	s := &supervisor{cpus: 1.5, memBytes: 1 << 30}
	fd, err := s.createCgroup(dir)
	if err != nil {
		t.Fatal(err)
	}
	_ = syscall.Close(fd)
	for file, want := range map[string]string{"cpu.max": "150000 100000", "memory.max": "1073741824", "memory.swap.max": "0"} {
		got, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil || string(got) != want {
			t.Errorf("%s = %q, %v; want %q", file, got, err, want)
		}
	}

	unlimited := filepath.Join(t.TempDir(), "tunnel-2")
	fd, err = (&supervisor{}).createCgroup(unlimited)
	if err != nil {
		t.Fatal(err)
	}
	_ = syscall.Close(fd)
	if _, err := os.Stat(filepath.Join(unlimited, "cpu.max")); !os.IsNotExist(err) {
		t.Errorf("cpu.max written with no CPU limit: %v", err)
	}
}

func TestOOMKills(t *testing.T) {
	dir := t.TempDir()
	if got := oomKills(dir); got != 0 {
		t.Errorf("oomKills without memory.events = %d, want 0", got)
	}
	events := "low 0\nhigh 0\nmax 12\noom 3\noom_kill 2\noom_group_kill 0\n"
	if err := os.WriteFile(filepath.Join(dir, "memory.events"), []byte(events), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := oomKills(dir); got != 2 {
		t.Errorf("oomKills = %d, want 2", got)
	}
}
