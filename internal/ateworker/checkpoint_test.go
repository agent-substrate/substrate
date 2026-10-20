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

package ateworker

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

func TestRecordAndReuseCheckpoint(t *testing.T) {
	checkpointDir := t.TempDir()
	imgPath := filepath.Join(checkpointDir, "checkpoint.img")
	if err := os.WriteFile(imgPath, []byte("snapshot-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Without a completion marker (e.g. partial files from an interrupted attempt),
	// ReuseCheckpoint returns nil.
	if got := ReuseCheckpoint(checkpointDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY); got != nil {
		t.Fatalf("ReuseCheckpoint(before RecordCheckpoint) = %v, want nil", got)
	}

	wantResp := &ateworkerpb.CheckpointWorkloadResponse{
		SnapshotFiles: []string{"checkpoint.img"},
	}
	if err := RecordCheckpoint(checkpointDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, wantResp); err != nil {
		t.Fatalf("RecordCheckpoint: %v", err)
	}

	// Reusing with matching fidelity and intact file succeeds.
	got := ReuseCheckpoint(checkpointDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
	if got == nil || !slices.Equal(got.GetSnapshotFiles(), wantResp.GetSnapshotFiles()) {
		t.Fatalf("ReuseCheckpoint() = %v, want %v", got, wantResp)
	}

	// Mismatched fidelity is not reused.
	if got := ReuseCheckpoint(checkpointDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES); got != nil {
		t.Fatalf("ReuseCheckpoint(different fidelity) = %v, want nil", got)
	}

	// Once atelet moves/removes the file from checkpoint-state/, the marker is not reused.
	if err := os.Remove(imgPath); err != nil {
		t.Fatal(err)
	}
	if got := ReuseCheckpoint(checkpointDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY); got != nil {
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
	if err := RecordCheckpoint(checkpointDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, memResp); err != nil {
		t.Fatalf("RecordCheckpoint(MEMORY): %v", err)
	}
	got := ReuseCheckpoint(checkpointDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
	if got == nil || !slices.Equal(got.GetSnapshotFiles(), memResp.GetSnapshotFiles()) || !slices.Equal(got.GetDataSnapshotFiles(), memResp.GetDataSnapshotFiles()) {
		t.Fatalf("ReuseCheckpoint(MEMORY) = %v, want %v", got, memResp)
	}

	// MEMORY fidelity rejects DataSnapshotFiles not present in SnapshotFiles.
	badSubset := &ateworkerpb.CheckpointWorkloadResponse{
		SnapshotFiles:     []string{"checkpoint.img"},
		DataSnapshotFiles: []string{"fs/fscheckpoint.pb"},
	}
	if err := RecordCheckpoint(checkpointDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, badSubset); err != nil {
		t.Fatalf("RecordCheckpoint(badSubset): %v", err)
	}
	if got := ReuseCheckpoint(checkpointDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY); got != nil {
		t.Fatalf("ReuseCheckpoint(data file not in SnapshotFiles) = %v, want nil", got)
	}

	// VOLUMES fidelity succeeds when SnapshotFiles and DataSnapshotFiles contain
	// the same set of files, even in different order.
	volResp := &ateworkerpb.CheckpointWorkloadResponse{
		SnapshotFiles:     []string{"fs/multitar.img", "fs/fscheckpoint.pb"},
		DataSnapshotFiles: []string{"fs/fscheckpoint.pb", "fs/multitar.img"},
	}
	if err := RecordCheckpoint(checkpointDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, volResp); err != nil {
		t.Fatalf("RecordCheckpoint(VOLUMES): %v", err)
	}
	got = ReuseCheckpoint(checkpointDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES)
	if got == nil || !slices.Equal(got.GetSnapshotFiles(), volResp.GetSnapshotFiles()) || !slices.Equal(got.GetDataSnapshotFiles(), volResp.GetDataSnapshotFiles()) {
		t.Fatalf("ReuseCheckpoint(VOLUMES) = %v, want %v", got, volResp)
	}

	// VOLUMES fidelity rejects non-data files in SnapshotFiles.
	if err := RecordCheckpoint(checkpointDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, memResp); err != nil {
		t.Fatalf("RecordCheckpoint(VOLUMES with non-data files): %v", err)
	}
	if got := ReuseCheckpoint(checkpointDir, ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES); got != nil {
		t.Fatalf("ReuseCheckpoint(VOLUMES with non-data files) = %v, want nil", got)
	}
}
