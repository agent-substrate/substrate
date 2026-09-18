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

//go:build linux

package sandboxd

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const kataCheckpointFormatVersion = 10
const KataCheckpointProfile = "ateom-kata-qemu-cr-v1"
const KataCheckpointProfileDragonball = "ateom-kata-dragonball-cr-v1"

type RuntimeCompatibility struct {
	Profile     string
	Fingerprint string
}

type checkpointVMState struct {
	File   string `json:"file"`
	Size   uint64 `json:"size"`
	SHA256 string `json:"sha256"`
}

type checkpointManifest struct {
	FormatVersion uint32              `json:"format_version"`
	Hypervisor    string              `json:"hypervisor"`
	SandboxID     string              `json:"sandbox_id"`
	Tasks         []checkpointTask    `json:"tasks"`
	Rootfs        []checkpointRootfs  `json:"rootfs"`
	BlockVolumes  []json.RawMessage   `json:"block_volumes"`
	NetworkMACs   []json.RawMessage   `json:"network_macs"`
	MemoryHotplug uint64              `json:"memory_hotplug_size"`
	VCPUHotplug   uint32              `json:"vcpu_hotplug_count"`
	VMState       []checkpointVMState `json:"vm_state"`
	Fingerprint   string              `json:"runtime_fingerprint"`
	OperationID   string              `json:"operation_id,omitempty"`
	Template      *json.RawMessage    `json:"template,omitempty"`
}

type checkpointTask struct {
	CheckpointKey string `json:"checkpoint_key"`
	TaskID        string `json:"task_id"`
}

type checkpointRootfs struct {
	CheckpointKey      string `json:"checkpoint_key"`
	File               string `json:"file"`
	Format             string `json:"format"`
	DeviceID           string `json:"device_id"`
	Index              uint64 `json:"index"`
	DriverOption       string `json:"driver_option"`
	VirtPath           string `json:"virt_path"`
	IsDirect           *bool  `json:"is_direct"`
	DiscardUnmap       bool   `json:"discard_unmap"`
	AIO                string `json:"aio"`
	QueueSize          uint32 `json:"queue_size"`
	NumQueues          uint64 `json:"num_queues"`
	LogicalSectorSize  uint32 `json:"logical_sector_size"`
	PhysicalSectorSize uint32 `json:"physical_sector_size"`
	SerialOverride     string `json:"serial_override"`
	Size               uint64 `json:"size"`
	SHA256             string `json:"sha256"`
}

// ValidateCheckpoint reads manifest.json first and validates every file named
// by it. It intentionally understands only the integrity envelope, not Kata's
// VM-state contents.
func ValidateCheckpoint(root string, expectedOperationID ...string) error {
	_, err := validateCheckpoint(root, expectedOperationID...)
	return err
}

func CheckpointCompatibility(root string) (RuntimeCompatibility, error) {
	manifest, err := validateCheckpoint(root)
	if err != nil {
		return RuntimeCompatibility{}, err
	}
	return RuntimeCompatibility{Profile: KataCheckpointProfile, Fingerprint: manifest.Fingerprint}, nil
}

func validateCheckpoint(root string, expectedOperationID ...string) (*checkpointManifest, error) {
	if !filepath.IsAbs(root) {
		return nil, errors.New("checkpoint path must be absolute")
	}
	manifestPath := filepath.Join(root, "manifest.json")
	manifestBytes, err := readRegularFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("read checkpoint manifest: %w", err)
	}
	var manifest checkpointManifest
	dec := json.NewDecoder(strings.NewReader(string(manifestBytes)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode checkpoint manifest: %w", err)
	}
	if manifest.FormatVersion != kataCheckpointFormatVersion || manifest.SandboxID == "" || manifest.Fingerprint == "" {
		return nil, errors.New("checkpoint manifest lacks required identity fields")
	}
	if len(expectedOperationID) > 0 && manifest.OperationID != expectedOperationID[0] {
		return nil, fmt.Errorf("checkpoint operation ID %q does not match request %q", manifest.OperationID, expectedOperationID[0])
	}
	if len(manifest.Tasks) == 0 || len(manifest.Rootfs) != len(manifest.Tasks) {
		return nil, errors.New("checkpoint task/rootfs inventory is empty or mismatched")
	}
	if len(manifest.BlockVolumes) != 0 {
		return nil, errors.New("checkpoint contains unsupported block volumes")
	}
	if len(manifest.VMState) == 0 {
		return nil, errors.New("checkpoint has no VM state artifacts")
	}
	vmStateFiles := map[string]bool{}
	for _, item := range manifest.VMState {
		clean := filepath.Clean(item.File)
		if filepath.Dir(clean) != "vm" || filepath.Base(clean) == "." || vmStateFiles[clean] {
			return nil, fmt.Errorf("invalid or duplicate VM state path %q", item.File)
		}
		path, err := confinedPath(root, item.File)
		if err != nil {
			return nil, err
		}
		if err := validateFile(path, item.Size, item.SHA256); err != nil {
			return nil, fmt.Errorf("validate VM state %q: %w", item.File, err)
		}
		vmStateFiles[clean] = true
	}
	taskKeys, taskIDs := map[string]bool{}, map[string]bool{}
	for _, task := range manifest.Tasks {
		if task.CheckpointKey == "" || !validID(task.TaskID) || taskKeys[task.CheckpointKey] || taskIDs[task.TaskID] {
			return nil, fmt.Errorf("invalid or duplicate checkpoint task %q=%q", task.CheckpointKey, task.TaskID)
		}
		taskKeys[task.CheckpointKey], taskIDs[task.TaskID] = true, true
	}
	seen := map[string]bool{}
	for _, item := range manifest.Rootfs {
		if item.CheckpointKey == "" || !taskKeys[item.CheckpointKey] || seen[item.CheckpointKey] {
			return nil, fmt.Errorf("invalid or duplicate rootfs key %q", item.CheckpointKey)
		}
		if item.Format != "raw" || item.DeviceID == "" || item.DriverOption == "" || item.VirtPath == "" || item.AIO == "" {
			return nil, fmt.Errorf("rootfs %q lacks managed-device identity", item.CheckpointKey)
		}
		seen[item.CheckpointKey] = true
		path, err := confinedPath(root, item.File)
		if err != nil {
			return nil, err
		}
		if err := validateFile(path, item.Size, item.SHA256); err != nil {
			return nil, fmt.Errorf("validate rootfs %q: %w", item.CheckpointKey, err)
		}
	}
	return &manifest, nil
}

func confinedPath(root, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) {
		return "", fmt.Errorf("invalid manifest path %q", relative)
	}
	clean := filepath.Clean(relative)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("manifest path escapes checkpoint: %q", relative)
	}
	return filepath.Join(root, clean), nil
}

func readRegularFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("not a regular file")
	}
	return os.ReadFile(path)
}

func validateFile(path string, size uint64, digest string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || uint64(info.Size()) != size {
		return fmt.Errorf("invalid type or size: got %d, require %d", info.Size(), size)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	got := hex.EncodeToString(h.Sum(nil))
	if len(digest) != sha256.Size*2 || got != strings.ToLower(digest) {
		return fmt.Errorf("sha256 mismatch: got %s, require %s", got, digest)
	}
	return nil
}
