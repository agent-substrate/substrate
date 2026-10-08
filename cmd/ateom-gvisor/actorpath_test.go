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
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/roottest"
)

func TestResetRunscStateAndPidFileDirs(t *testing.T) {
	actorDirs := &ateompb.ActorDirs{RootDir: t.TempDir()}
	if err := os.MkdirAll(filepath.Join(runscStateDir(actorDirs), "stale"), 0o700); err != nil {
		t.Fatal(err)
	}
	// The previous activation's runsc log and sentry panic log go too, so a
	// failure never quotes another activation's lines.
	for _, file := range []string{runscLogPath(actorDirs), panicLogPath(actorDirs)} {
		if err := os.WriteFile(file, []byte("stale\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := resetRunscStateAndPidFileDirs(actorDirs); err != nil {
		t.Fatalf("resetRunscStateAndPidFileDirs() = %v", err)
	}
	for _, dir := range []string{runscStateDir(actorDirs), pidFileDir(actorDirs)} {
		if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
			t.Errorf("ReadDir(%q) = %v, %v; want an empty directory", dir, entries, err)
		}
	}
	for _, file := range []string{runscLogPath(actorDirs), panicLogPath(actorDirs)} {
		if _, err := os.Stat(file); !os.IsNotExist(err) {
			t.Errorf("Stat(%q) err = %v, want not exist", file, err)
		}
	}
	// Reset with neither file present is fine.
	if err := resetRunscStateAndPidFileDirs(actorDirs); err != nil {
		t.Fatalf("second resetRunscStateAndPidFileDirs() = %v", err)
	}
}

// runsc leaves mounts in its state directory; unlinking one fails with EBUSY
// until it is detached.
func TestResetRunscStateAndPidFileDirs_DetachesMounts(t *testing.T) {
	roottest.Require(t, "mount/unmount")
	actorDirs := &ateompb.ActorDirs{RootDir: t.TempDir()}
	if err := os.MkdirAll(runscStateDir(actorDirs), 0o700); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(t.TempDir(), "netns")
	target := filepath.Join(runscStateDir(actorDirs), "null-netns")
	for _, f := range []string{src, target} {
		if err := os.WriteFile(f, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := unix.Mount(src, target, "", unix.MS_BIND, ""); err != nil {
		t.Fatalf("bind mount: %v", err)
	}
	t.Cleanup(func() { _ = unix.Unmount(target, unix.MNT_DETACH) })

	if err := resetRunscStateAndPidFileDirs(actorDirs); err != nil {
		t.Fatalf("resetRunscStateAndPidFileDirs() = %v", err)
	}
	if entries, err := os.ReadDir(runscStateDir(actorDirs)); err != nil || len(entries) != 0 {
		t.Errorf("ReadDir(%q) = %v, %v; want an empty directory", runscStateDir(actorDirs), entries, err)
	}
}
