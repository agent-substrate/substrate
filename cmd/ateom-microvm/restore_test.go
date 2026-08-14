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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
)

// writeSnapshotConfig writes a config.json holding the given fs devices (plus the
// vsock and serial entries every snapshot has) into a fresh snapshot dir.
func writeSnapshotConfig(t *testing.T, fsDevices []map[string]any) string {
	t.Helper()
	dir := t.TempDir()
	cfg := map[string]any{
		"vsock":  map[string]any{"cid": 3, "socket": "/run/vc/vm/golden/clh.sock"},
		"serial": map[string]any{"mode": "File", "file": "/run/vc/vm/golden/serial.log"},
		"fs":     fsDevices,
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

	t.Run("each share keeps its own socket", func(t *testing.T) {
		// Ordered durable-first to catch a rewrite that assumes the RO lower comes
		// first, which would hand the guest the wrong filesystem.
		dir := writeSnapshotConfig(t, []map[string]any{
			{"tag": kata.DurableFsTag, "socket": "/run/vc/vm/golden/virtiofsd-durable.sock"},
			{"tag": kata.FsTag, "socket": "/run/vc/vm/golden/virtiofsd.sock"},
		})
		if err := rewriteSnapshotSocketPaths(dir, id); err != nil {
			t.Fatalf("rewriteSnapshotSocketPaths: %v", err)
		}
		got := readFsSockets(t, dir)
		for tag, want := range map[string]string{
			kata.FsTag:        kata.VirtiofsdSocketPath(id),
			kata.DurableFsTag: kata.DurableVirtiofsdSocketPath(id),
		} {
			if got[tag] != want {
				t.Errorf("%s socket = %q, want %q", tag, got[tag], want)
			}
		}
		if got[kata.FsTag] == got[kata.DurableFsTag] {
			t.Error("both shares were pointed at the same socket")
		}
	})

	t.Run("unknown tag is an error", func(t *testing.T) {
		dir := writeSnapshotConfig(t, []map[string]any{
			{"tag": "somethingElse", "socket": "/run/vc/vm/golden/other.sock"},
		})
		if err := rewriteSnapshotSocketPaths(dir, id); err == nil {
			t.Fatal("rewriteSnapshotSocketPaths accepted an unknown fs tag, want an error")
		}
	})

	t.Run("vsock and serial are repointed", func(t *testing.T) {
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
			Vsock  struct{ Socket string } `json:"vsock"`
			Serial struct{ File string }   `json:"serial"`
		}
		if err := json.Unmarshal(b, &cfg); err != nil {
			t.Fatalf("parsing rewritten config: %v", err)
		}
		if want := kata.VsockSocketPath(id); cfg.Vsock.Socket != want {
			t.Errorf("vsock socket = %q, want %q", cfg.Vsock.Socket, want)
		}
		if want := filepath.Join(kata.VMDir(id), "serial.log"); cfg.Serial.File != want {
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
}
