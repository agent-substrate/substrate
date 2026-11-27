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
	"bytes"
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"go.ate.dev/substrate/internal/resources"
)

func TestLeafMemoryHigh(t *testing.T) {
	throttled := &AteWorkerService{throttleMemory: true}
	if got, want := throttled.leafMemoryHigh(1<<30), actorBooking(1<<30); got != want {
		t.Errorf("leafMemoryHigh(1GiB) = %d, want the booking %d", got, want)
	}
	if got := throttled.leafMemoryHigh(0); got != 0 {
		t.Errorf("leafMemoryHigh(0) = %d, want 0", got)
	}
	if got := (&AteWorkerService{}).leafMemoryHigh(1 << 30); got != 0 {
		t.Errorf("leafMemoryHigh without PSI = %d, want 0", got)
	}
}

func TestCheckMemoryLogsError(t *testing.T) {
	var logs bytes.Buffer
	// slog.SetDefault also points the log package at the handler, and setting
	// slog back does not undo that, so restore all three.
	prev, prevOut, prevFlags := slog.Default(), log.Writer(), log.Flags()
	t.Cleanup(func() {
		slog.SetDefault(prev)
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	})
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))

	root := t.TempDir()
	s := &AteWorkerService{}
	ra := &runningActor{}
	h := &hostedActor{
		attribution: resources.ActorAttribution{UID: "uid-a"},
		vm:          ra,
	}
	leaf := filepath.Join(root, "uid-a")
	if err := os.Mkdir(leaf, 0o755); err != nil {
		t.Fatal(err)
	}
	const limit = 256 << 20
	const current = limit + 32<<20
	for name, content := range map[string]string{
		"memory.current": strconv.Itoa(current) + "\n",
		"memory.stat":    "anon " + strconv.Itoa(current) + "\n",
		"cgroup.kill":    "",
		"cgroup.events":  "populated 0\nfrozen 0\n",
	} {
		if err := os.WriteFile(filepath.Join(leaf, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ra.memory.Resume(limit)

	s.checkMemory(context.Background(), root, h)
	if !ra.memory.Killed() {
		t.Fatal("microVM over its limit was not killed")
	}
	ra.memory.Pause()

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
		Msg:              "Killing a microVM over its memory limit",
		ActorUID:         "uid-a",
		MemoryLimitBytes: limit,
		MemoryUsage:      current,
	}
	if got != want {
		t.Errorf("kill log = %+v, want %+v", got, want)
	}
}
