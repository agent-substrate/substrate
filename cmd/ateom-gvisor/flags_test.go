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
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestFlagsBuildArgs(t *testing.T) {
	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
		flags:     mustFlagStore(t),
	}

	got := r.buildArgs("start", "app")
	want := []string{
		"-global1",
		"-global2",
		"-start-global",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"start",
		"app",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("buildArgs(start) = %v, want %v", got, want)
	}

	got = r.buildArgs("restore", "app", "-bundle", "/bundle")
	want = []string{
		"-global1",
		"-global2",
		"-restore-global",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"restore",
		"-restore-sub1",
		"-restore-sub2",
		"-bundle", "/bundle",
		"app",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("buildArgs(restore) = %v, want %v", got, want)
	}

	got = r.buildArgs("list", "")
	want = []string{
		"-global1",
		"-global2",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"list",
		"-list-sub",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("buildArgs(list) = %v, want %v", got, want)
	}
}

func TestFlagStoreEmptyPath(t *testing.T) {
	store, err := newFlagStore(t.Context(), "")
	if err != nil {
		t.Fatalf("newFlagStore(ctx, \"\") unexpected error: %v", err)
	}
	if store == nil {
		t.Fatal("newFlagStore(ctx, \"\") returned nil store")
	}

	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
		flags:     store,
	}

	got := r.buildArgs("pause", "contId")
	want := []string{
		"-root", "/node/actors/test-actor-123/runsc-state",
		"pause",
		"contId",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("buildArgs(pause) with empty store = %v, want %v", got, want)
	}
}

func waitForReload(t *testing.T, reloadCh <-chan struct{}) {
	t.Helper()
	select {
	case <-reloadCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for config reload")
	}
}

func TestFlagStoreWatchAndReload(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "flags.json")

	initialJSON := `{
  "globalFlags": ["-global1"],
  "commands": {
    "create": {
      "globalFlags": ["-create-global1"]
    }
  }
}`
	if err := os.WriteFile(cfgPath, []byte(initialJSON), 0o600); err != nil {
		t.Fatalf("failed to write initial config: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	store, err := newFlagStore(ctx, cfgPath)
	if err != nil {
		t.Fatalf("newFlagStore() unexpected error: %v", err)
	}

	r := &runsc{
		path:      "/usr/bin/runsc",
		actorUID:  "test-actor-123",
		actorDirs: testActorDirs,
		flags:     store,
	}

	// Verify initial config loaded from JSON file.
	gotInitial := r.buildArgs("create", "app")
	wantInitial := []string{
		"-global1",
		"-create-global1",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"create",
		"app",
	}
	if !reflect.DeepEqual(gotInitial, wantInitial) {
		t.Fatalf("initial buildArgs(create) = %v, want %v", gotInitial, wantInitial)
	}

	// Set up reload notification channel for deterministic synchronization.
	reloadCh := make(chan struct{}, 16)
	store.onReload = func() {
		select {
		case reloadCh <- struct{}{}:
		default:
		}
	}

	// Update with valid new flags via atomic rename.
	updatedJSON := `{
  "globalFlags": ["-global1", "-global2"],
  "commands": {
    "create": {
      "globalFlags": ["-create-global1", "-create-global2"]
    },
    "delete": {
      "subcommandFlags": ["-delete-sub"]
    }
  }
}`
	tmpPath := filepath.Join(dir, ".flags.json.tmp")
	if err := os.WriteFile(tmpPath, []byte(updatedJSON), 0o600); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}
	if err := os.Rename(tmpPath, cfgPath); err != nil {
		t.Fatalf("failed to rename temp config: %v", err)
	}
	waitForReload(t, reloadCh)

	gotUpdated := r.buildArgs("create", "app")
	wantUpdated := []string{
		"-global1",
		"-global2",
		"-create-global1",
		"-create-global2",
		"-root", "/node/actors/test-actor-123/runsc-state",
		"create",
		"app",
	}
	if !reflect.DeepEqual(gotUpdated, wantUpdated) {
		t.Fatalf("updated buildArgs(create) = %v, want %v", gotUpdated, wantUpdated)
	}

	// Write invalid JSON via atomic rename: watcher must ignore invalid update
	// and preserve the existing valid configuration struct.
	badTmp := filepath.Join(dir, ".bad.tmp")
	if err := os.WriteFile(badTmp, []byte(`{"globalFlags": ["broken"`), 0o600); err != nil {
		t.Fatalf("failed to write bad temp config: %v", err)
	}
	if err := os.Rename(badTmp, cfgPath); err != nil {
		t.Fatalf("failed to rename bad temp config: %v", err)
	}
	waitForReload(t, reloadCh)

	gotAfterBad := r.buildArgs("create", "app")
	if !reflect.DeepEqual(gotAfterBad, wantUpdated) {
		t.Fatalf("after invalid reload buildArgs(create) = %v, want previous valid config %v", gotAfterBad, wantUpdated)
	}
}
