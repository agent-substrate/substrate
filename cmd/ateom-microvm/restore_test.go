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
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/actorlock"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// writeSnapshotConfig writes a config.json holding the given fs devices (plus the
// vsock and console entries every snapshot has) into a fresh snapshot dir. The
// serial device is the debug-mode one, present here so the rewrite is exercised
// against a snapshot that has both.
func writeSnapshotConfig(t *testing.T, fsDevices []map[string]any) string {
	t.Helper()
	dir := t.TempDir()
	cfg := map[string]any{
		"vsock":   map[string]any{"cid": 3, "socket": "/run/vc/vm/golden/clh.sock"},
		"console": map[string]any{"mode": "File", "file": "/run/vc/vm/golden/console.log"},
		"serial":  map[string]any{"mode": "File", "file": "/run/vc/vm/golden/serial.log"},
		"fs":      fsDevices,
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshaling config: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), b, 0o600); err != nil {
		t.Fatalf("writing config.json: %v", err)
	}
	return dir
}

// readFsSockets returns the rewritten config's fs sockets keyed by device tag.
func readFsSockets(t *testing.T, dir string) map[string]string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("reading config.json: %v", err)
	}
	var cfg struct {
		Vsock  map[string]any `json:"vsock"`
		Serial map[string]any `json:"serial"`
		Fs     []struct {
			Tag    string `json:"tag"`
			Socket string `json:"socket"`
		} `json:"fs"`
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		t.Fatalf("parsing rewritten config: %v", err)
	}
	got := map[string]string{}
	for _, f := range cfg.Fs {
		got[f.Tag] = f.Socket
	}
	return got
}

func TestRewriteSnapshotSocketPaths(t *testing.T) {
	const id = "actor-uid"

	t.Run("overlay lower only", func(t *testing.T) {
		dir := writeSnapshotConfig(t, []map[string]any{
			{"tag": kata.FsTag, "socket": "/run/vc/vm/golden/virtiofsd.sock"},
		})
		if err := rewriteSnapshotSocketPaths(dir, id); err != nil {
			t.Fatalf("rewriteSnapshotSocketPaths: %v", err)
		}
		if got, want := readFsSockets(t, dir)[kata.FsTag], kata.VirtiofsdSocketPath(id); got != want {
			t.Errorf("%s socket = %q, want %q", kata.FsTag, got, want)
		}
	})

	t.Run("unknown tag is an error", func(t *testing.T) {
		// "ateUpper", "ateDurable", and "ateCSI" are retired multi-share tags:
		// they never appear in snapshots this code produces, and one showing up
		// must fail loudly rather than be silently repointed.
		for _, tag := range []string{"somethingElse", "ateUpper", "ateDurable", "ateCSI"} {
			dir := writeSnapshotConfig(t, []map[string]any{
				{"tag": tag, "socket": "/run/vc/vm/golden/other.sock"},
			})
			if err := rewriteSnapshotSocketPaths(dir, id); err == nil {
				t.Fatalf("rewriteSnapshotSocketPaths accepted fs tag %q, want an error", tag)
			}
		}
	})

	t.Run("vsock and console devices are repointed", func(t *testing.T) {
		dir := writeSnapshotConfig(t, []map[string]any{
			{"tag": kata.FsTag, "socket": "/run/vc/vm/golden/virtiofsd.sock"},
		})
		if err := rewriteSnapshotSocketPaths(dir, id); err != nil {
			t.Fatalf("rewriteSnapshotSocketPaths: %v", err)
		}
		b, err := os.ReadFile(filepath.Join(dir, "config.json"))
		if err != nil {
			t.Fatalf("reading config.json: %v", err)
		}
		var cfg struct {
			Vsock   struct{ Socket string } `json:"vsock"`
			Console struct{ File string }   `json:"console"`
			Serial  struct{ File string }   `json:"serial"`
		}
		if err := json.Unmarshal(b, &cfg); err != nil {
			t.Fatalf("parsing rewritten config: %v", err)
		}
		if want := kata.VsockSocketPath(id); cfg.Vsock.Socket != want {
			t.Errorf("vsock socket = %q, want %q", cfg.Vsock.Socket, want)
		}
		if want := kata.ConsoleLogPath(id); cfg.Console.File != want {
			t.Errorf("console file = %q, want %q", cfg.Console.File, want)
		}
		if want := kata.SerialLogPath(id); cfg.Serial.File != want {
			t.Errorf("serial file = %q, want %q", cfg.Serial.File, want)
		}
	})

	// atelet stages a local checkpoint into the restore dir by hard-linking it, so
	// the config.json rewritten here can share an inode with the actor's cached
	// pause snapshot. Writing in place would rewrite that snapshot's config too.
	t.Run("a hardlinked config is not written through", func(t *testing.T) {
		dir := writeSnapshotConfig(t, []map[string]any{
			{"tag": kata.FsTag, "socket": "/run/vc/vm/golden/virtiofsd.sock"},
		})
		cfgPath := filepath.Join(dir, "config.json")
		before, err := os.ReadFile(cfgPath)
		if err != nil {
			t.Fatalf("reading config.json: %v", err)
		}
		cached := filepath.Join(t.TempDir(), "config.json")
		if err := os.Link(cfgPath, cached); err != nil {
			t.Fatalf("linking config.json: %v", err)
		}

		if err := rewriteSnapshotSocketPaths(dir, id); err != nil {
			t.Fatalf("rewriteSnapshotSocketPaths: %v", err)
		}
		if got, want := readFsSockets(t, dir)[kata.FsTag], kata.VirtiofsdSocketPath(id); got != want {
			t.Errorf("%s socket = %q, want %q", kata.FsTag, got, want)
		}
		got, err := os.ReadFile(cached)
		if err != nil {
			t.Fatalf("reading the linked config.json: %v", err)
		}
		if !bytes.Equal(got, before) {
			t.Errorf("the linked config.json was rewritten: got %q, want the original %q", got, before)
		}
		// Nothing may be left behind for atelet to ship as part of the snapshot.
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if e.Name() != "config.json" {
				t.Errorf("stray file left in the restore dir: %q", e.Name())
			}
		}
	})

	t.Run("unchanged socket paths leave config.json untouched", func(t *testing.T) {
		dir := t.TempDir()
		cfgPath := filepath.Join(dir, "config.json")
		cfg := map[string]any{
			"vsock":   map[string]any{"cid": 3, "socket": kata.VsockSocketPath(id)},
			"console": map[string]any{"mode": "File", "file": kata.ConsoleLogPath(id)},
			"serial":  map[string]any{"mode": "File", "file": kata.SerialLogPath(id)},
			"fs": []map[string]any{
				{"tag": kata.FsTag, "socket": kata.VirtiofsdSocketPath(id)},
			},
		}
		b, err := json.Marshal(cfg)
		if err != nil {
			t.Fatalf("marshaling config: %v", err)
		}
		if err := os.WriteFile(cfgPath, b, 0o600); err != nil {
			t.Fatalf("writing config.json: %v", err)
		}
		fiBefore, err := os.Stat(cfgPath)
		if err != nil {
			t.Fatalf("stat before: %v", err)
		}
		inoBefore := fiBefore.Sys().(*syscall.Stat_t).Ino

		if err := rewriteSnapshotSocketPaths(dir, id); err != nil {
			t.Fatalf("rewriteSnapshotSocketPaths: %v", err)
		}

		fiAfter, err := os.Stat(cfgPath)
		if err != nil {
			t.Fatalf("stat after: %v", err)
		}
		inoAfter := fiAfter.Sys().(*syscall.Stat_t).Ino
		if inoAfter != inoBefore {
			t.Errorf("config.json inode changed (%d -> %d); expected no rewrite when socket paths already match", inoBefore, inoAfter)
		}
	})
}

func TestRestoreWorkloadRejectsEmptyRestoreDir(t *testing.T) {
	s := &AteomService{
		locks:    actorlock.New(),
		inFlight: actorlock.NewInFlight(),
	}

	for _, tc := range []struct {
		name string
		req  *ateompb.RestoreWorkloadRequest
	}{
		{
			name: "nil actor_dirs",
			req:  &ateompb.RestoreWorkloadRequest{ActorUid: "actor-a"},
		},
		{
			name: "empty restore_dir",
			req: &ateompb.RestoreWorkloadRequest{
				ActorUid: "actor-a",
				ActorDirs: &ateompb.ActorDirs{
					RootDir:                   "/node/actors/actor-a",
					OciBundleDir:              "/node/actors/actor-a/bundle",
					CheckpointDir:             "/node/actors/actor-a/checkpoint-state",
					DurableDirVolumeMountsDir: "/node/actors/actor-a/durable-dirs",
					SystemInfoVolumeRootsDir:  "/node/actors/actor-a/system-info",
					VolumesDir:                "/node/actors/actor-a/volumes",
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.RestoreWorkload(context.Background(), tc.req)
			if apierror.Code(err) != codes.InvalidArgument {
				t.Fatalf("RestoreWorkload() code = %v, want %v (err: %v)", apierror.Code(err), codes.InvalidArgument, err)
			}
		})
	}
}

func TestMaybeDropStagedMemoryImage(t *testing.T) {
	for _, tc := range []struct {
		name               string
		memMode            string
		preserveRestoreDir bool
		wantRemoved        bool
	}{
		{
			name:               "eager staging dir removes memory-ranges",
			memMode:            ch.MemRestoreEager,
			preserveRestoreDir: false,
			wantRemoved:        true,
		},
		{
			name:               "eager preserved dir keeps memory-ranges",
			memMode:            ch.MemRestoreEager,
			preserveRestoreDir: true,
			wantRemoved:        false,
		},
		{
			name:               "on-demand staging dir keeps memory-ranges",
			memMode:            ch.MemRestoreOnDemand,
			preserveRestoreDir: false,
			wantRemoved:        false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			memPath := filepath.Join(dir, "memory-ranges")
			if err := os.WriteFile(memPath, []byte("ram"), 0o600); err != nil {
				t.Fatalf("writing memory-ranges: %v", err)
			}

			maybeDropStagedMemoryImage(context.Background(), dir, tc.memMode, tc.preserveRestoreDir)

			_, err := os.Stat(memPath)
			if tc.wantRemoved && !os.IsNotExist(err) {
				t.Errorf("memory-ranges still exists (stat err = %v), want removed", err)
			}
			if !tc.wantRemoved && err != nil {
				t.Errorf("memory-ranges missing (stat err = %v), want preserved", err)
			}
		})
	}
}

func TestMergeOnDemandDeltaPreservesRestoreSource(t *testing.T) {
	const pageSize = 4096
	const fileSize = 2 * pageSize

	basePage0 := bytes.Repeat([]byte{0xAA}, pageSize)
	deltaPage1 := bytes.Repeat([]byte{0xBB}, pageSize)
	wantMerged := append(append([]byte{}, basePage0...), deltaPage1...)

	writePages := func(t *testing.T, path string, off int64, data []byte) {
		t.Helper()
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := f.Truncate(fileSize); err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt(data, off); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("preserveRestoreSource=true keeps single-link base in restoreSourceDir", func(t *testing.T) {
		restoreDir := t.TempDir()
		checkpointDir := t.TempDir()
		basePath := filepath.Join(restoreDir, "memory-ranges")
		deltaPath := filepath.Join(checkpointDir, "memory-ranges")

		writePages(t, basePath, 0, basePage0)
		writePages(t, deltaPath, pageSize, deltaPage1)

		if err := mergeOnDemandDelta(context.Background(), restoreDir, checkpointDir, true); err != nil {
			t.Fatalf("mergeOnDemandDelta: %v", err)
		}

		// The preserved local snapshot's memory-ranges must still exist and hold
		// its original contents.
		gotBase, err := os.ReadFile(basePath)
		if err != nil {
			t.Fatalf("base memory-ranges was removed from preserved restoreDir: %v", err)
		}
		wantBase := append(append([]byte{}, basePage0...), make([]byte, pageSize)...)
		if !bytes.Equal(gotBase, wantBase) {
			t.Errorf("base memory-ranges was mutated in preserved restoreDir")
		}

		gotMerged, err := os.ReadFile(deltaPath)
		if err != nil {
			t.Fatalf("reading merged memory-ranges: %v", err)
		}
		if !bytes.Equal(gotMerged, wantMerged) {
			t.Errorf("merged memory-ranges mismatch")
		}
	})

	t.Run("preserveRestoreSource=false moves expendable base into checkpointDir", func(t *testing.T) {
		restoreDir := t.TempDir()
		checkpointDir := t.TempDir()
		basePath := filepath.Join(restoreDir, "memory-ranges")
		deltaPath := filepath.Join(checkpointDir, "memory-ranges")

		writePages(t, basePath, 0, basePage0)
		writePages(t, deltaPath, pageSize, deltaPage1)

		if err := mergeOnDemandDelta(context.Background(), restoreDir, checkpointDir, false); err != nil {
			t.Fatalf("mergeOnDemandDelta: %v", err)
		}

		if _, err := os.Stat(basePath); !os.IsNotExist(err) {
			t.Errorf("expendable base memory-ranges still in restoreDir (stat err = %v), want moved", err)
		}
		gotMerged, err := os.ReadFile(deltaPath)
		if err != nil {
			t.Fatalf("reading merged memory-ranges: %v", err)
		}
		if !bytes.Equal(gotMerged, wantMerged) {
			t.Errorf("merged memory-ranges mismatch")
		}
	})
}

func TestNewReseedNonce(t *testing.T) {
	a, err := newReseedNonce()
	if err != nil {
		t.Fatalf("newReseedNonce: %v", err)
	}
	if len(a) != 32 {
		t.Fatalf("nonce length = %d, want 32", len(a))
	}
	// The reseed only makes clones of one snapshot diverge if each restore mixes in
	// distinct bytes, so two nonces must never be equal.
	b, err := newReseedNonce()
	if err != nil {
		t.Fatalf("newReseedNonce (second call): %v", err)
	}
	if bytes.Equal(a, b) {
		t.Fatal("two nonces are identical; reseed would not make clones diverge")
	}
}

func TestRPCsRejectUntrustedRuntimeAssetPaths(t *testing.T) {
	s := &AteomService{}
	ctx := context.Background()
	dirs := &ateompb.ActorDirs{
		RootDir:                   "/node/actors/actor-a",
		OciBundleDir:              "/node/actors/actor-a/bundle",
		CheckpointDir:             "/node/actors/actor-a/checkpoint-state",
		RestoreDir:                "/node/actors/actor-a/restore",
		DurableDirVolumeMountsDir: "/node/actors/actor-a/durable-dirs",
		SystemInfoVolumeRootsDir:  "/node/actors/actor-a/system-info",
		VolumesDir:                "/node/actors/actor-a/volumes",
	}
	assets := map[string]string{assetCH: "/bin/sh"}
	for name, call := range map[string]func() error{
		"RunWorkload": func() error {
			_, err := s.RunWorkload(ctx, &ateompb.RunWorkloadRequest{ActorDirs: dirs, RuntimeAssetPaths: assets})
			return err
		},
		"RestoreWorkload": func() error {
			_, err := s.RestoreWorkload(ctx, &ateompb.RestoreWorkloadRequest{ActorDirs: dirs, RuntimeAssetPaths: assets})
			return err
		},
	} {
		if got := apierror.Code(call()); got != codes.InvalidArgument {
			t.Errorf("%s() code = %v, want %v", name, got, codes.InvalidArgument)
		}
	}
}
