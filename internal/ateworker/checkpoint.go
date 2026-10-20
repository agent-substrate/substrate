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
	"encoding/json"
	"os"
	"path/filepath"
	"slices"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

// checkpointDoneFile is written into checkpointDir after all snapshot files are
// listed and the workload is torn down. It is wiped by os.RemoveAll(checkpointDir)
// on the next checkpoint or atelet's resetActorDirs.
const checkpointDoneFile = ".checkpoint-done.json"

type checkpointRecord struct {
	Fidelity          ateworkerpb.SnapshotFidelity `json:"fidelity"`
	SnapshotFiles     []string                     `json:"snapshot_files,omitempty"`
	DataSnapshotFiles []string                     `json:"data_snapshot_files,omitempty"`
}

// RecordCheckpoint writes the completion marker into checkpointDir so a retry
// after a caller timeout or atelet restart can reuse the finished
// CheckpointWorkloadResponse.
func RecordCheckpoint(checkpointDir string, fidelity ateworkerpb.SnapshotFidelity, resp *ateworkerpb.CheckpointWorkloadResponse) error {
	b, err := json.Marshal(checkpointRecord{
		Fidelity:          fidelity,
		SnapshotFiles:     resp.GetSnapshotFiles(),
		DataSnapshotFiles: resp.GetDataSnapshotFiles(),
	})
	if err != nil {
		return err
	}
	tmp := filepath.Join(checkpointDir, checkpointDoneFile+".tmp")
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(checkpointDir, checkpointDoneFile))
}

// ReuseCheckpoint returns the recorded CheckpointWorkloadResponse from
// checkpointDir if the completion marker exists, its fidelity matches, and all
// listed snapshot files are still present on disk.
func ReuseCheckpoint(checkpointDir string, fidelity ateworkerpb.SnapshotFidelity) *ateworkerpb.CheckpointWorkloadResponse {
	b, err := os.ReadFile(filepath.Join(checkpointDir, checkpointDoneFile))
	if err != nil {
		return nil
	}
	var rec checkpointRecord
	if err := json.Unmarshal(b, &rec); err != nil || rec.Fidelity != fidelity {
		return nil
	}
	// DataSnapshotFiles is the VOLUMES subset of SnapshotFiles: the same file
	// set on VOLUMES, and a strict subset on ROOTFS/MEMORY.
	if fidelity == ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES {
		if len(rec.SnapshotFiles) != len(rec.DataSnapshotFiles) {
			return nil
		}
		for _, f := range rec.SnapshotFiles {
			if !slices.Contains(rec.DataSnapshotFiles, f) {
				return nil
			}
		}
	} else if len(rec.SnapshotFiles) == 0 || len(rec.DataSnapshotFiles) >= len(rec.SnapshotFiles) {
		return nil
	}
	for _, f := range rec.DataSnapshotFiles {
		if !slices.Contains(rec.SnapshotFiles, f) {
			return nil
		}
	}
	for _, f := range rec.SnapshotFiles {
		info, err := os.Stat(filepath.Join(checkpointDir, f))
		if err != nil || !info.Mode().IsRegular() {
			return nil
		}
	}
	return &ateworkerpb.CheckpointWorkloadResponse{
		SnapshotFiles:     rec.SnapshotFiles,
		DataSnapshotFiles: rec.DataSnapshotFiles,
	}
}
