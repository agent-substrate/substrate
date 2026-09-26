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
	"testing"

	"github.com/agent-substrate/substrate/internal/ocispec"
)

const testActorUID = "0b5f3c1e-7a2d-4e8f-9c61-2d4a8b7e5f10"

type ckptFixture struct {
	root, proc, leaf string
}

func newCkptFixture(t *testing.T, procs map[string]string) ckptFixture {
	t.Helper()
	f := ckptFixture{root: t.TempDir(), proc: t.TempDir()}
	f.leaf = filepath.Join(f.root, ocispec.GVisorCgroupLeaf(testActorUID, ocispec.PauseContainer))
	mustMkdir(t, f.leaf)
	var list string
	for pid, cmdline := range procs {
		list += pid + "\n"
		mustMkdir(t, filepath.Join(f.proc, pid))
		mustWrite(t, filepath.Join(f.proc, pid, "cmdline"), cmdline)
	}
	mustWrite(t, filepath.Join(f.leaf, "cgroup.procs"), list)
	mustWrite(t, filepath.Join(f.leaf, "memory.max"), "805306368\n")
	return f
}

func mustMkdir(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
}

func mustWrite(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

var leafProcs = map[string]string{
	"100": "runsc-gofer\x00--root=/x\x00gofer\x00",
	"101": "runsc-sandbox\x00--root=/x\x00boot\x00",
	"102": "",
	"103": "runsc-gofer\x00--log-format=json\x00",
}

func TestIsolateCheckpointCacheMovesOnlyTheSentry(t *testing.T) {
	f := newCkptFixture(t, leafProcs)

	c, err := isolateCheckpointCache(f.root, f.proc, testActorUID)
	if err != nil {
		t.Fatalf("isolateCheckpointCache: %v", err)
	}
	dir := checkpointCgroupDir(f.root, testActorUID)
	if got := mustRead(t, filepath.Join(dir, "cgroup.procs")); got != "101" {
		t.Errorf("checkpoint cgroup.procs = %q, want the sentry's PID 101", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "memory.max")); !os.IsNotExist(err) {
		t.Errorf("checkpoint memory.max should not be set, stat err = %v", err)
	}

	if err := c.restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got := mustRead(t, filepath.Join(f.leaf, "cgroup.procs")); got != "101" {
		t.Errorf("pause leaf cgroup.procs after restore = %q, want 101", got)
	}
}

func TestIsolateCheckpointCacheWithoutSentry(t *testing.T) {
	f := newCkptFixture(t, map[string]string{"100": "runsc-gofer\x00gofer\x00"})

	if _, err := isolateCheckpointCache(f.root, f.proc, testActorUID); err == nil {
		t.Fatal("isolateCheckpointCache succeeded with no sentry in the leaf")
	}
	if _, err := os.Stat(checkpointCgroupDir(f.root, testActorUID)); !os.IsNotExist(err) {
		t.Errorf("checkpoint cgroup left behind: stat err = %v", err)
	}
}

func TestIsolateCheckpointCacheWithoutLeaf(t *testing.T) {
	if _, err := isolateCheckpointCache(t.TempDir(), t.TempDir(), testActorUID); err == nil {
		t.Fatal("isolateCheckpointCache succeeded with no pause leaf")
	}
}

func TestCheckpointCgroupRestoreAfterLeafGone(t *testing.T) {
	f := newCkptFixture(t, leafProcs)
	c, err := isolateCheckpointCache(f.root, f.proc, testActorUID)
	if err != nil {
		t.Fatalf("isolateCheckpointCache: %v", err)
	}
	if err := os.RemoveAll(f.leaf); err != nil {
		t.Fatal(err)
	}
	if err := c.restore(); err != nil {
		t.Errorf("restore with the leaf gone = %v, want nil", err)
	}
}

func TestCheckpointCgroupNilIsNoop(t *testing.T) {
	var c *checkpointCgroup
	if err := c.restore(); err != nil {
		t.Errorf("nil restore = %v", err)
	}
	if err := c.remove(); err != nil {
		t.Errorf("nil remove = %v", err)
	}
}

func TestRemoveStaleCheckpointCgroups(t *testing.T) {
	root := t.TempDir()
	stale := checkpointCgroupDir(root, testActorUID)
	mustMkdir(t, stale)
	busy := checkpointCgroupDir(root, "busy")
	mustMkdir(t, busy)
	// A non-empty directory stands in for a cgroup that still has processes:
	// rmdir fails on both.
	mustWrite(t, filepath.Join(busy, "cgroup.procs"), "42")
	leaf := filepath.Join(root, ocispec.GVisorCgroupLeaf(testActorUID, ocispec.PauseContainer))
	mustMkdir(t, leaf)

	removeStaleCheckpointCgroups(context.Background(), root)

	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale checkpoint cgroup not removed: stat err = %v", err)
	}
	if _, err := os.Stat(busy); err != nil {
		t.Errorf("busy checkpoint cgroup removed: %v", err)
	}
	if _, err := os.Stat(leaf); err != nil {
		t.Errorf("pause leaf removed: %v", err)
	}
}

func TestDropCheckpointPageCache(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "local-checkpoint", "snap-1")
	mustMkdir(t, sub)
	img := filepath.Join(sub, "pages.img")
	mustWrite(t, img, "checkpoint pages")
	manifest := filepath.Join(sub, "manifest.json")
	mustWrite(t, manifest, "{}")

	targets := findCheckpointImagesInDirs(dir, filepath.Join(dir, "missing"))
	if len(targets) != 1 || targets[0].path != img {
		t.Fatalf("findCheckpointImagesInDirs = %+v, want [%s]", targets, img)
	}
	if !evictCheckpointImages(targets) {
		t.Errorf("evictCheckpointImages() = false for existing inode, want true")
	}
	if got := mustRead(t, img); got != "checkpoint pages" {
		t.Errorf("pages.img content = %q, want %q", got, "checkpoint pages")
	}

	// Replacing the file with a new inode stops eviction for the old target.
	replacement := filepath.Join(sub, "pages.img.new")
	mustWrite(t, replacement, "new checkpoint pages")
	if err := os.Rename(replacement, img); err != nil {
		t.Fatal(err)
	}
	if evictCheckpointImages(targets) {
		t.Errorf("evictCheckpointImages() = true after inode replacement, want false")
	}
}
