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

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestKillMatching(t *testing.T) {
	cmd := exec.Command("sleep", "31337")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()

	// Start returns when exec closes the child's status pipe, before the
	// kernel has set up the new argv, so /proc/<pid>/cmdline can read empty.
	cmdline := filepath.Join("/proc", strconv.Itoa(cmd.Process.Pid), "cmdline")
	for start := time.Now(); ; time.Sleep(time.Millisecond) {
		if b, _ := os.ReadFile(cmdline); strings.Contains(string(b), "sleep\x0031337") {
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatalf("%s still has no command line", cmdline)
		}
	}

	n, err := killMatching("/proc", "sleep\x0031337", os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("killed %d, want 1", n)
	}
	select {
	case <-exited:
	case <-time.After(10 * time.Second):
		t.Fatal("matching process still running")
	}
}

func TestKillMatchingSkipsSelf(t *testing.T) {
	// the test binary's own command line contains its name
	n, err := killMatching("/proc", os.Args[0], os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("killed %d, want 0", n)
	}
}

func TestKillMatchingCountsAProcessAlreadyGone(t *testing.T) {
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	// a process listed but reaped by the time it is killed
	proc := t.TempDir()
	dir := filepath.Join(proc, strconv.Itoa(cmd.Process.Pid))
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cmdline"), []byte("virtiofsd\x00actor-uid"), 0o600); err != nil {
		t.Fatal(err)
	}
	n, err := killMatching(proc, "actor-uid", os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("killed %d, want 1", n)
	}
}
