//go:build linux

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
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/resources"
)

func writeCgroupFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func readCgroupFile(t *testing.T, dir, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}

func TestSwapMemoryMaxForHigh(t *testing.T) {
	dir := t.TempDir()
	writeCgroupFiles(t, dir, map[string]string{"memory.max": "268435456\n", "memory.high": "max\n"})
	limit, err := swapMemoryMaxForHigh(dir)
	if err != nil || limit != 268435456 {
		t.Fatalf("swapMemoryMaxForHigh() = %d, %v, want 268435456", limit, err)
	}
	if got := readCgroupFile(t, dir, "memory.high"); got != "268435456" {
		t.Errorf("memory.high = %q, want 268435456", got)
	}
	if got := readCgroupFile(t, dir, "memory.max"); got != "max" {
		t.Errorf("memory.max = %q, want max", got)
	}
}

func TestSwapMemoryMaxForHighNoLimit(t *testing.T) {
	for name, files := range map[string]map[string]string{
		"unlimited":            {"memory.max": "max\n", "memory.high": "max\n"},
		"no memory controller": {},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeCgroupFiles(t, dir, files)
			if limit, err := swapMemoryMaxForHigh(dir); err != nil || limit != 0 {
				t.Errorf("swapMemoryMaxForHigh() = %d, %v, want 0", limit, err)
			}
			if _, err := os.Stat(filepath.Join(dir, "memory.high")); len(files) == 0 && err == nil {
				t.Error("memory.high was created")
			}
		})
	}
}

func TestRemoveStaleSandboxLeaf(t *testing.T) {
	root := t.TempDir()
	s := &AteWorkerService{cgroupRoot: root}
	leaf := s.sandboxLeaf("uid-a")
	if err := os.Mkdir(leaf, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := s.removeStaleSandboxLeaf(context.Background(), "uid-a"); err != nil {
		t.Fatalf("removeStaleSandboxLeaf() = %v", err)
	}
	if _, err := os.Stat(leaf); !os.IsNotExist(err) {
		t.Errorf("leftover leaf still there (stat err: %v)", err)
	}
	if err := s.removeStaleSandboxLeaf(context.Background(), "uid-a"); err != nil {
		t.Errorf("removeStaleSandboxLeaf() without a leaf = %v", err)
	}
}

// A sandbox past the overshoot cap is killed once, and a checkpoint pausing
// the watch waits for the kill.
func TestCheckMemoryKillsOncePausesForCheckpoint(t *testing.T) {
	logs := captureWarnings(t)
	root := t.TempDir()
	s := &AteWorkerService{cgroupRoot: root, throttleMemory: true}
	h := &hostedActor{attribution: resources.ActorAttribution{UID: "uid-a"}}
	leaf := s.sandboxLeaf("uid-a")
	if err := os.Mkdir(leaf, 0o755); err != nil {
		t.Fatal(err)
	}
	const limit = 256 << 20
	const current = limit + 32<<20
	writeCgroupFiles(t, leaf, map[string]string{
		"memory.current": strconv.Itoa(current) + "\n",
		"memory.stat":    "anon " + strconv.Itoa(current) + "\n",
		"cgroup.kill":    "",
		"cgroup.events":  "populated 0\nfrozen 0\n",
	})
	h.memory.Resume(limit)

	s.checkMemory(context.Background(), h)
	if !h.memory.Killed() {
		t.Fatal("sandbox over its limit was not killed")
	}
	if got := h.memory.Pause(); got != limit {
		t.Errorf("Pause() = %d, want %d", got, limit)
	}
	if got := readCgroupFile(t, leaf, "cgroup.kill"); got != "1" {
		t.Errorf("cgroup.kill = %q after pausing, want the kill done", got)
	}

	writeCgroupFiles(t, leaf, map[string]string{"cgroup.kill": ""})
	h.memory.Resume(limit)
	s.checkMemory(context.Background(), h)
	h.memory.Pause()
	if got := readCgroupFile(t, leaf, "cgroup.kill"); got != "" {
		t.Errorf("sandbox was killed twice")
	}

	type killLog struct {
		Level            string `json:"level"`
		Msg              string `json:"msg"`
		ActorUID         string `json:"ate.actor.uid"`
		MemoryLimitBytes uint64 `json:"memoryLimitBytes"`
		MemoryUsage      uint64 `json:"ate.stats.memory.usage"`
	}
	var got killLog
	if err := json.Unmarshal(logs.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal log %q: %v", logs.String(), err)
	}
	want := killLog{
		Level:            slog.LevelError.String(),
		Msg:              "Killing a sandbox over its memory limit",
		ActorUID:         "uid-a",
		MemoryLimitBytes: limit,
		MemoryUsage:      current,
	}
	if got != want {
		t.Errorf("kill log = %+v, want %+v", got, want)
	}
}
