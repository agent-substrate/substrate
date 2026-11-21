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

package ateworker

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

// testBootID is the boot the tests record and reuse checkpoints on.
const testBootID = "boot-1"

func TestRecordAndReuseCheckpoint(t *testing.T) {
	checkpointDir := t.TempDir()
	imgPath := filepath.Join(checkpointDir, "checkpoint.img")
	if err := os.WriteFile(imgPath, []byte("snapshot-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Without a completion marker (e.g. partial files from an interrupted attempt),
	// ReuseCheckpoint returns nil.
	if got := ReuseCheckpoint(checkpointDir, testBootID, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY); got != nil {
		t.Fatalf("ReuseCheckpoint(before RecordCheckpoint) = %v, want nil", got)
	}

	wantResp := &ateworkerpb.CheckpointWorkloadResponse{
		SnapshotFiles: []string{"checkpoint.img"},
	}
	if err := RecordCheckpoint(checkpointDir, testBootID, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, wantResp); err != nil {
		t.Fatalf("RecordCheckpoint: %v", err)
	}

	// Reusing with matching fidelity and intact file succeeds.
	got := ReuseCheckpoint(checkpointDir, testBootID, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
	if got == nil || !slices.Equal(got.GetSnapshotFiles(), wantResp.GetSnapshotFiles()) {
		t.Fatalf("ReuseCheckpoint() = %v, want %v", got, wantResp)
	}

	// A marker recorded before the node rebooted is not reused.
	if got := ReuseCheckpoint(checkpointDir, "boot-2", ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY); got != nil {
		t.Fatalf("ReuseCheckpoint(another boot) = %v, want nil", got)
	}

	// Mismatched fidelity is not reused.
	if got := ReuseCheckpoint(checkpointDir, testBootID, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES); got != nil {
		t.Fatalf("ReuseCheckpoint(different fidelity) = %v, want nil", got)
	}

	// Once the file is gone from checkpoint-state/, the marker is not reused.
	if err := os.Remove(imgPath); err != nil {
		t.Fatal(err)
	}
	if got := ReuseCheckpoint(checkpointDir, testBootID, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY); got != nil {
		t.Fatalf("ReuseCheckpoint(after file removed) = %v, want nil", got)
	}
}

func TestReuseCheckpointDataSnapshotFilesByFidelity(t *testing.T) {
	checkpointDir := t.TempDir()
	fsDir := filepath.Join(checkpointDir, "fs")
	if err := os.MkdirAll(fsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{"checkpoint.img", "fs/fscheckpoint.pb", "fs/multitar.img"} {
		if err := os.WriteFile(filepath.Join(checkpointDir, rel), []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	// MEMORY fidelity with DataSnapshotFiles subset succeeds when all files exist.
	memResp := &ateworkerpb.CheckpointWorkloadResponse{
		SnapshotFiles:     []string{"checkpoint.img", "fs/fscheckpoint.pb", "fs/multitar.img"},
		DataSnapshotFiles: []string{"fs/fscheckpoint.pb", "fs/multitar.img"},
	}
	if err := RecordCheckpoint(checkpointDir, testBootID, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, memResp); err != nil {
		t.Fatalf("RecordCheckpoint(MEMORY): %v", err)
	}
	got := ReuseCheckpoint(checkpointDir, testBootID, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
	if got == nil || !slices.Equal(got.GetSnapshotFiles(), memResp.GetSnapshotFiles()) || !slices.Equal(got.GetDataSnapshotFiles(), memResp.GetDataSnapshotFiles()) {
		t.Fatalf("ReuseCheckpoint(MEMORY) = %v, want %v", got, memResp)
	}

	// MEMORY fidelity rejects DataSnapshotFiles not present in SnapshotFiles.
	badSubset := &ateworkerpb.CheckpointWorkloadResponse{
		SnapshotFiles:     []string{"checkpoint.img"},
		DataSnapshotFiles: []string{"fs/fscheckpoint.pb"},
	}
	if err := RecordCheckpoint(checkpointDir, testBootID, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, badSubset); err != nil {
		t.Fatalf("RecordCheckpoint(badSubset): %v", err)
	}
	if got := ReuseCheckpoint(checkpointDir, testBootID, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY); got != nil {
		t.Fatalf("ReuseCheckpoint(data file not in SnapshotFiles) = %v, want nil", got)
	}

	// VOLUMES fidelity succeeds when SnapshotFiles and DataSnapshotFiles contain
	// the same set of files, even in different order.
	volResp := &ateworkerpb.CheckpointWorkloadResponse{
		SnapshotFiles:     []string{"fs/multitar.img", "fs/fscheckpoint.pb"},
		DataSnapshotFiles: []string{"fs/fscheckpoint.pb", "fs/multitar.img"},
	}
	if err := RecordCheckpoint(checkpointDir, testBootID, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, volResp); err != nil {
		t.Fatalf("RecordCheckpoint(VOLUMES): %v", err)
	}
	got = ReuseCheckpoint(checkpointDir, testBootID, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES)
	if got == nil || !slices.Equal(got.GetSnapshotFiles(), volResp.GetSnapshotFiles()) || !slices.Equal(got.GetDataSnapshotFiles(), volResp.GetDataSnapshotFiles()) {
		t.Fatalf("ReuseCheckpoint(VOLUMES) = %v, want %v", got, volResp)
	}

	// VOLUMES fidelity rejects non-data files in SnapshotFiles.
	if err := RecordCheckpoint(checkpointDir, testBootID, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, memResp); err != nil {
		t.Fatalf("RecordCheckpoint(VOLUMES with non-data files): %v", err)
	}
	if got := ReuseCheckpoint(checkpointDir, testBootID, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES); got != nil {
		t.Fatalf("ReuseCheckpoint(VOLUMES with non-data files) = %v, want nil", got)
	}
}

// TestReuseCheckpointSnapshotNames checks reuse against the snapshot file
// names atelet accepts. A symlink or a name that leaves the checkpoint
// directory must not be returned: atelet rejects that list and the completion
// marker would otherwise keep the same result on retry.
func TestReuseCheckpointSnapshotNames(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(t *testing.T) (dir string, resp *ateworkerpb.CheckpointWorkloadResponse)
		reuse   bool
	}{
		{
			name: "regular file",
			prepare: func(t *testing.T) (string, *ateworkerpb.CheckpointWorkloadResponse) {
				dir := t.TempDir()
				if err := os.WriteFile(filepath.Join(dir, "checkpoint.img"), []byte("snapshot-bytes"), 0o600); err != nil {
					t.Fatal(err)
				}
				return dir, &ateworkerpb.CheckpointWorkloadResponse{SnapshotFiles: []string{"checkpoint.img"}}
			},
			reuse: true,
		},
		{
			name: "fs subdirectory file",
			prepare: func(t *testing.T) (string, *ateworkerpb.CheckpointWorkloadResponse) {
				dir := t.TempDir()
				if err := os.Mkdir(filepath.Join(dir, "fs"), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "fs", "fscheckpoint.pb"), []byte("data"), 0o600); err != nil {
					t.Fatal(err)
				}
				return dir, &ateworkerpb.CheckpointWorkloadResponse{SnapshotFiles: []string{"fs/fscheckpoint.pb"}}
			},
			reuse: true,
		},
		{
			name: "symlink",
			prepare: func(t *testing.T) (string, *ateworkerpb.CheckpointWorkloadResponse) {
				parent := t.TempDir()
				dir := filepath.Join(parent, "checkpoint")
				if err := os.Mkdir(dir, 0o700); err != nil {
					t.Fatal(err)
				}
				target := filepath.Join(parent, "outside")
				if err := os.WriteFile(target, []byte("snapshot-bytes"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, filepath.Join(dir, "checkpoint.img")); err != nil {
					t.Fatal(err)
				}
				return dir, &ateworkerpb.CheckpointWorkloadResponse{SnapshotFiles: []string{"checkpoint.img"}}
			},
		},
		{
			name: "symlink directory",
			prepare: func(t *testing.T) (string, *ateworkerpb.CheckpointWorkloadResponse) {
				parent := t.TempDir()
				outside := filepath.Join(parent, "outside")
				if err := os.Mkdir(outside, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(outside, "esc"), []byte("snapshot-bytes"), 0o600); err != nil {
					t.Fatal(err)
				}
				dir := filepath.Join(parent, "checkpoint")
				if err := os.Mkdir(dir, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, filepath.Join(dir, "fs")); err != nil {
					t.Fatal(err)
				}
				return dir, &ateworkerpb.CheckpointWorkloadResponse{SnapshotFiles: []string{"fs/esc"}}
			},
		},
		{
			name: "escaping name",
			prepare: func(t *testing.T) (string, *ateworkerpb.CheckpointWorkloadResponse) {
				parent := t.TempDir()
				dir := filepath.Join(parent, "checkpoint")
				if err := os.Mkdir(dir, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(parent, "outside"), []byte("snapshot-bytes"), 0o600); err != nil {
					t.Fatal(err)
				}
				return dir, &ateworkerpb.CheckpointWorkloadResponse{SnapshotFiles: []string{"../outside"}}
			},
		},
		{
			name: "nested name",
			prepare: func(t *testing.T) (string, *ateworkerpb.CheckpointWorkloadResponse) {
				dir := t.TempDir()
				if err := os.Mkdir(filepath.Join(dir, "a"), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "a", "b"), []byte("snapshot-bytes"), 0o600); err != nil {
					t.Fatal(err)
				}
				return dir, &ateworkerpb.CheckpointWorkloadResponse{SnapshotFiles: []string{"a/b"}}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, resp := tc.prepare(t)
			if err := RecordCheckpoint(dir, testBootID, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, resp); err != nil {
				t.Fatalf("RecordCheckpoint: %v", err)
			}
			got := ReuseCheckpoint(dir, testBootID, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
			if tc.reuse {
				if got == nil || !slices.Equal(got.GetSnapshotFiles(), resp.GetSnapshotFiles()) || !slices.Equal(got.GetDataSnapshotFiles(), resp.GetDataSnapshotFiles()) {
					t.Fatalf("ReuseCheckpoint() = %v, want %v", got, resp)
				}
				return
			}
			if got != nil {
				t.Fatalf("ReuseCheckpoint() = %v, want nil", got)
			}
		})
	}
}

// TestCheckpointContext checks that a checkpoint outlives its canceled caller
// but keeps the caller's deadline.
func TestCheckpointContext(t *testing.T) {
	deadline := time.Now().Add(time.Hour)
	for _, tc := range []struct {
		name         string
		caller       func(context.Context) (context.Context, context.CancelFunc)
		wantDeadline time.Time
	}{
		{
			name: "caller deadline",
			caller: func(ctx context.Context) (context.Context, context.CancelFunc) {
				return context.WithDeadline(ctx, deadline)
			},
			wantDeadline: deadline,
		},
		{name: "no deadline", caller: context.WithCancel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caller, cancelCaller := tc.caller(t.Context())
			ctx, cancel := CheckpointContext(caller)
			defer cancel()

			cancelCaller()
			if err := ctx.Err(); err != nil {
				t.Errorf("Err() after the caller canceled = %v, want nil", err)
			}
			got, ok := ctx.Deadline()
			if wantOK := !tc.wantDeadline.IsZero(); ok != wantOK || !got.Equal(tc.wantDeadline) {
				t.Errorf("Deadline() = %v, %t, want %v, %t", got, ok, tc.wantDeadline, wantOK)
			}
		})
	}
}
