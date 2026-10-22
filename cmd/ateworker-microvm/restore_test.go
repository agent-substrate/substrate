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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/kata"
	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
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
		"payload": map[string]any{"kernel": "/var/lib/ate/actors/golden/sandbox-assets/kata-kernel", "cmdline": "console=hvc0"},
		"pmem":    []map[string]any{{"id": guestImagePmemID, "file": "/var/lib/ate/actors/golden/sandbox-assets/kata-image", "discard_writes": true}},
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

// mutateSnapshotConfig rewrites dir's config.json through mutate.
func mutateSnapshotConfig(t *testing.T, dir string, mutate func(cfg map[string]any)) {
	t.Helper()
	cfgPath := filepath.Join(dir, "config.json")
	b, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(b, &cfg); err != nil {
		t.Fatal(err)
	}
	mutate(cfg)
	b, err = json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfgPath, b, 0o600); err != nil {
		t.Fatal(err)
	}
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

func TestRewriteSnapshotPaths(t *testing.T) {
	const id = "actor-uid"
	// The restoring actor's own staged kernel and guest image.
	const (
		kernel = "/var/lib/ate/actors/actor-uid/sandbox-assets/kata-kernel"
		image  = "/var/lib/ate/actors/actor-uid/sandbox-assets/kata-image"
	)

	t.Run("overlay lower only", func(t *testing.T) {
		dir := writeSnapshotConfig(t, []map[string]any{
			{"tag": kata.FsTag, "socket": "/run/vc/vm/golden/virtiofsd.sock"},
		})
		if err := rewriteSnapshotPaths(dir, id, kernel, image); err != nil {
			t.Fatalf("rewriteSnapshotPaths: %v", err)
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
			if err := rewriteSnapshotPaths(dir, id, kernel, image); err == nil {
				t.Fatalf("rewriteSnapshotPaths accepted fs tag %q, want an error", tag)
			}
		}
	})

	// A snapshot config that lacks or mangles the two per-actor boot paths
	// is broken: skipping their rewrite would restore with the source
	// actor's, so each shape is an error.
	for name, mutate := range map[string]func(cfg map[string]any){
		"missing payload":            func(cfg map[string]any) { delete(cfg, "payload") },
		"malformed payload":          func(cfg map[string]any) { cfg["payload"] = "vmlinux" },
		"malformed pmem entry":       func(cfg map[string]any) { cfg["pmem"] = []any{"not a device"} },
		"missing pmem list":          func(cfg map[string]any) { delete(cfg, "pmem") },
		"missing guest image device": func(cfg map[string]any) { cfg["pmem"] = []any{} },
	} {
		t.Run(name+" is an error", func(t *testing.T) {
			dir := writeSnapshotConfig(t, []map[string]any{
				{"tag": kata.FsTag, "socket": "/run/vc/vm/golden/virtiofsd.sock"},
			})
			mutateSnapshotConfig(t, dir, mutate)
			if err := rewriteSnapshotPaths(dir, id, kernel, image); err == nil {
				t.Fatalf("rewriteSnapshotPaths accepted a config with %s, want an error", name)
			}
		})
	}

	t.Run("unknown pmem device is an error", func(t *testing.T) {
		dir := writeSnapshotConfig(t, []map[string]any{
			{"tag": kata.FsTag, "socket": "/run/vc/vm/golden/virtiofsd.sock"},
		})
		cfgPath := filepath.Join(dir, "config.json")
		b, err := os.ReadFile(cfgPath)
		if err != nil {
			t.Fatal(err)
		}
		var cfg map[string]any
		if err := json.Unmarshal(b, &cfg); err != nil {
			t.Fatal(err)
		}
		cfg["pmem"] = []map[string]any{{"id": "scratch-disk", "file": "/somewhere/else"}}
		b, err = json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(cfgPath, b, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := rewriteSnapshotPaths(dir, id, kernel, image); err == nil {
			t.Fatal("rewriteSnapshotPaths accepted a pmem device it does not know, want an error")
		}
	})

	t.Run("vsock, console, kernel and image are repointed", func(t *testing.T) {
		dir := writeSnapshotConfig(t, []map[string]any{
			{"tag": kata.FsTag, "socket": "/run/vc/vm/golden/virtiofsd.sock"},
		})
		if err := rewriteSnapshotPaths(dir, id, kernel, image); err != nil {
			t.Fatalf("rewriteSnapshotPaths: %v", err)
		}
		b, err := os.ReadFile(filepath.Join(dir, "config.json"))
		if err != nil {
			t.Fatalf("reading config.json: %v", err)
		}
		var cfg struct {
			Vsock   struct{ Socket string } `json:"vsock"`
			Console struct{ File string }   `json:"console"`
			Serial  struct{ File string }   `json:"serial"`
			Payload struct{ Kernel string } `json:"payload"`
			Pmem    []struct {
				ID   string `json:"id"`
				File string `json:"file"`
			} `json:"pmem"`
		}
		if err := json.Unmarshal(b, &cfg); err != nil {
			t.Fatalf("parsing rewritten config: %v", err)
		}
		if want := kata.VsockSocketPath(id); cfg.Vsock.Socket != want {
			t.Errorf("vsock socket = %q, want %q", cfg.Vsock.Socket, want)
		}
		// The source actor's staged kernel and image may be gone; the
		// snapshot must boot from this actor's.
		if cfg.Payload.Kernel != kernel {
			t.Errorf("payload kernel = %q, want %q", cfg.Payload.Kernel, kernel)
		}
		if len(cfg.Pmem) != 1 || cfg.Pmem[0].ID != guestImagePmemID || cfg.Pmem[0].File != image {
			t.Errorf("pmem = %+v, want the %q device at %q", cfg.Pmem, guestImagePmemID, image)
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

		if err := rewriteSnapshotPaths(dir, id, kernel, image); err != nil {
			t.Fatalf("rewriteSnapshotPaths: %v", err)
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
			"payload": map[string]any{"kernel": kernel, "cmdline": "console=hvc0"},
			"pmem":    []map[string]any{{"id": guestImagePmemID, "file": image, "discard_writes": true}},
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

		if err := rewriteSnapshotPaths(dir, id, kernel, image); err != nil {
			t.Fatalf("rewriteSnapshotPaths: %v", err)
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
	s := &AteWorkerService{
		locks:    actorlock.New(),
		inFlight: actorlock.NewInFlight(),
	}

	noDirs := validRestoreRequest()
	noDirs.ActorDirs = nil
	noRestoreDir := validRestoreRequest()
	noRestoreDir.ActorDirs.RestoreDir = ""
	for _, tc := range []struct {
		name string
		req  *ateworkerpb.RestoreWorkloadRequest
		want string
	}{
		{name: "nil actor_dirs", req: noDirs, want: "actor_dirs"},
		{name: "empty restore_dir", req: noRestoreDir, want: "restore_dir"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.RestoreWorkload(context.Background(), tc.req)
			if apierror.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("RestoreWorkload() = %v, want InvalidArgument naming %s", err, tc.want)
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

	t.Run("preserveRestoreSource=false merges expendable base in place and links into checkpointDir", func(t *testing.T) {
		restoreDir := t.TempDir()
		checkpointDir := t.TempDir()
		basePath := filepath.Join(restoreDir, "memory-ranges")
		deltaPath := filepath.Join(checkpointDir, "memory-ranges")

		writePages(t, basePath, 0, basePage0)
		writePages(t, deltaPath, pageSize, deltaPage1)

		if err := mergeOnDemandDelta(context.Background(), restoreDir, checkpointDir, false); err != nil {
			t.Fatalf("mergeOnDemandDelta: %v", err)
		}

		bi, err := os.Stat(basePath)
		if err != nil {
			t.Fatalf("stat base memory-ranges in restoreDir: %v", err)
		}
		di, err := os.Stat(deltaPath)
		if err != nil {
			t.Fatalf("stat merged memory-ranges in checkpointDir: %v", err)
		}
		if !os.SameFile(bi, di) {
			t.Errorf("expected basePath and deltaPath to share an inode after in-place merge")
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

type fakeReseeder func(ctx context.Context, data []byte) error

func (f fakeReseeder) ReseedRandomDev(ctx context.Context, data []byte) error {
	return f(ctx, data)
}

func TestReseedGuestCRNG(t *testing.T) {
	t.Run("sends a fresh nonce once, bounded by reseedTimeout", func(t *testing.T) {
		var calls int
		var gotNonce []byte
		var gotDeadline time.Time
		start := time.Now()
		err := reseedGuestCRNG(t.Context(), fakeReseeder(func(ctx context.Context, data []byte) error {
			calls++
			gotNonce = append([]byte(nil), data...)
			gotDeadline, _ = ctx.Deadline()
			return nil
		}))
		if err != nil {
			t.Fatalf("reseedGuestCRNG: %v", err)
		}
		if calls != 1 {
			t.Errorf("calls = %d, want 1", calls)
		}
		if len(gotNonce) != 32 {
			t.Errorf("nonce length = %d, want 32", len(gotNonce))
		}
		if gotDeadline.IsZero() || gotDeadline.Before(start.Add(reseedTimeout)) || gotDeadline.After(time.Now().Add(reseedTimeout)) {
			t.Errorf("call deadline = %v, want about %v after the call started at %v", gotDeadline, reseedTimeout, start)
		}
	})

	t.Run("returns the agent's error without retrying", func(t *testing.T) {
		wantErr := errors.New("agent busy")
		var calls int
		err := reseedGuestCRNG(t.Context(), fakeReseeder(func(context.Context, []byte) error {
			calls++
			return wantErr
		}))
		if !errors.Is(err, wantErr) {
			t.Errorf("reseedGuestCRNG error = %v, want %v", err, wantErr)
		}
		if calls != 1 {
			t.Errorf("calls = %d, want 1", calls)
		}
	})
}

func TestRestoreGuestReadyError(t *testing.T) {
	// Child timeouts in dialAgentRetry and reseedGuestCRNG wrap a real
	// context.DeadlineExceeded.
	cause := fmt.Errorf("agent ReseedRandomDev: %w", context.DeadlineExceeded)

	t.Run("live caller context reports Unavailable even when cause wraps child DeadlineExceeded", func(t *testing.T) {
		err := restoreGuestReadyError(t.Context(), "reseeding guest CRNG", cause)
		if got := apierror.Code(err); got != codes.Unavailable {
			t.Errorf("apierror.Code(err) = %v, want %v (err: %v)", got, codes.Unavailable, err)
		}
		if !errors.Is(err, cause) {
			t.Errorf("err = %v, want it to wrap %v", err, cause)
		}
	})

	t.Run("canceled caller context reports Canceled without child DeadlineExceeded shadowing it", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := restoreGuestReadyError(ctx, "dialing kata-agent after resume", cause)
		if got := apierror.Code(err); got != codes.Canceled {
			t.Errorf("apierror.Code(err) = %v, want %v (err: %v)", got, codes.Canceled, err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want it to wrap context.Canceled", err)
		}
	})

	t.Run("stopped guest reports Internal, not Unavailable", func(t *testing.T) {
		stopped := fmt.Errorf("%w (cloud-hypervisor removed %q): %w", errGuestStopped, "/run/vsock.sock", fs.ErrNotExist)
		err := restoreGuestReadyError(t.Context(), "dialing kata-agent after resume", stopped)
		if got := apierror.Code(err); got != codes.Internal {
			t.Errorf("apierror.Code(err) = %v, want %v (err: %v)", got, codes.Internal, err)
		}
		if !errors.Is(err, errGuestStopped) {
			t.Errorf("err = %v, want it to wrap errGuestStopped", err)
		}
	})
}

func TestRPCsRejectUntrustedRuntimeAssetPaths(t *testing.T) {
	s := &AteWorkerService{}
	ctx := context.Background()
	assets := map[string]string{assetCH: "/bin/sh"}
	for name, call := range map[string]func() error{
		"RunWorkload": func() error {
			req := validRunRequest()
			req.RuntimeAssetPaths = assets
			_, err := s.RunWorkload(ctx, req)
			return err
		},
		"RestoreWorkload": func() error {
			req := validRestoreRequest()
			req.RuntimeAssetPaths = assets
			_, err := s.RestoreWorkload(ctx, req)
			return err
		},
	} {
		err := call()
		if apierror.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "runtime_asset_paths") {
			t.Errorf("%s() = %v, want InvalidArgument naming runtime_asset_paths", name, err)
		}
	}
}

// TestValidateRuntimeAssetPathsRequiresTheActorsOwnDir pins that the root
// the asset paths are checked against is derived from the actor UID, not
// taken from the request.
func TestValidateRuntimeAssetPathsRequiresTheActorsOwnDir(t *testing.T) {
	orig := nodepath.ActorsDir
	nodepath.ActorsDir = t.TempDir()
	t.Cleanup(func() { nodepath.ActorsDir = orig })
	stage := func(t *testing.T, actorUID string) string {
		t.Helper()
		dir := nodepath.ActorSandboxAssetsDir(actorUID)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, assetCH), nil, 0o555); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	own, other := stage(t, "actor-a"), stage(t, "actor-b")
	for _, tc := range []struct {
		name    string
		dir     string
		asset   string
		wantErr bool
	}{
		{"asset in the actor's own dir", own, filepath.Join(own, assetCH), false},
		{"asset outside the actor's dir", own, "/bin/sh", true},
		{"another actor's dir named as the root", other, filepath.Join(other, assetCH), true},
		{"the root named as the root", "/", "/bin/sh", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRuntimeAssetPaths("actor-a", &ateworkerpb.ActorDirs{SandboxAssetsDir: tc.dir}, map[string]string{assetCH: tc.asset})
			if (err != nil) != tc.wantErr {
				t.Errorf("validateRuntimeAssetPaths(actor-a, %q, %q) = %v, wantErr %v", tc.dir, tc.asset, err, tc.wantErr)
			}
		})
	}
}

// TestRestoreMemoryFidelityRequiresKernelAndImage pins that a memory restore
// without the kernel or the guest image among its asset paths fails before
// it prepares or cleans up anything: the snapshot's boot paths are repointed
// at them, so there is nothing to restore with.
func TestRestoreMemoryFidelityRequiresKernelAndImage(t *testing.T) {
	// A zero service cannot prepare egress or clean up sandbox state; the
	// check must come first for this test to return at all.
	s := &AteWorkerService{}
	for name, paths := range map[string]map[string]string{
		"no assets":  nil,
		"no kernel":  {assetImage: "/node/actors/actor-a/sandbox-assets/kata-image"},
		"no image":   {assetKernel: "/node/actors/actor-a/sandbox-assets/kata-kernel"},
		"empty path": {assetKernel: "", assetImage: "/node/actors/actor-a/sandbox-assets/kata-image"},
	} {
		t.Run(name, func(t *testing.T) {
			p := actorBootParams{actorUID: "actor-a", assetPaths: paths}
			err := s.restoreMemoryFidelity(t.Context(), p, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, t.TempDir(), false, time.Now())
			if err == nil || !strings.Contains(err.Error(), assetKernel) || !strings.Contains(err.Error(), assetImage) {
				t.Fatalf("restoreMemoryFidelity with %s = %v, want an error naming %q and %q", name, err, assetKernel, assetImage)
			}
		})
	}
}
