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

// Package ateompath is what the ateoms still derive from the actor UID instead
// of reading from ActorDirs. Each function goes away as the ateoms switch.
package ateompath

import (
	"path/filepath"

	"github.com/agent-substrate/substrate/internal/nodepath"
)

// ActorPath is ActorDirs.root_dir.
func ActorPath(actorUID string) string {
	return filepath.Join(
		nodepath.ActorsDir,
		actorUID,
	)
}

// ActorResolvConfPath is the resolver bind source outside the actor's rootfs.
// ateom-gvisor writes it under ActorDirs.root_dir.
func ActorResolvConfPath(actorUID string) string {
	return filepath.Join(ActorPath(actorUID), "resolv.conf")
}

// RunSCStateDir is runsc --root for the actor's sandbox, under ActorDirs.root_dir.
func RunSCStateDir(actorUID string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"runsc-state",
	)
}

// OCIBundleDir is ActorDirs.oci_bundle_dir.
func OCIBundleDir(actorUID string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"bundles",
	)
}

func OCIBundlePath(actorUID, containerName string) string {
	return filepath.Join(
		OCIBundleDir(actorUID),
		containerName,
	)
}

// ImageVolumeMountPath returns where ateom composes one image volume for a
// container. The path is per-container: containers of one actor may mount the
// same volume, and each needs its own mount point inside its own bundle.
func ImageVolumeMountPath(actorUID, containerName, volumeName string) string {
	return filepath.Join(OCIBundlePath(actorUID, containerName), "volumes", volumeName)
}

func RunscDebugLogDir(actorUID, containerName string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"runsc-debug-logs",
		containerName,
	)
}

// CheckpointStateDir is ActorDirs.checkpoint_dir.
func CheckpointStateDir(actorUID string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"checkpoint-state",
	)
}

// DurableDirVolumeMountsDir is ActorDirs.durable_dir_volume_mounts_dir.
func DurableDirVolumeMountsDir(actorUID string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"durable-dir",
	)
}

// SystemInfoVolumeRootsDir is ActorDirs.system_info_volume_roots_dir.
// Snapshots must capture durable-dir data but never system-info contents,
// which atelet regenerates on every Run/Restore; each sandbox class excludes
// them differently:
//
//   - micro-VM captures by location: its checkpoint tars all of
//     DurableDirVolumeMountsDir (see ateom-microvm's tarDurableVolumes), so
//     system-info roots are excluded by living in this separate directory.
//   - gVisor captures by declaration: durable mounts are registered with
//     the sandbox (mount-hint annotations for FULL checkpoints, the
//     enumerated durable mount paths for DATA fscheckpoints); system-info
//     mounts are plain undeclared binds, never captured regardless of host
//     layout.
//
// The separate directory is therefore critical only for micro-VM.
func SystemInfoVolumeRootsDir(actorUID string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"system-info",
	)
}

// RestoreStateDir is ActorDirs.restore_dir.
//
// We need to use a different path from CheckpointStateDir, because using `runsc
// restore -direct -background` means that runsc starts executing first, then
// demand-pages in parts of the checkpoint file as they are needed.  To know
// when the background reading is finished, we would need to run `runsc wait
// -checkpoint`, which will block until the read is done.  Alternatively, we can
// make sure we write the suspension checkpoint to a different location.  This
// will work properly, with `runsc checkpoint` paging in any data that hasn't
// yet been loaded.
func RestoreStateDir(actorUID string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"restore-state",
	)
}

// PIDFileDir is where runsc writes <container>.pid, under ActorDirs.root_dir.
func PIDFileDir(actorUID string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"pidfiles",
	)
}

func PIDFilePath(actorUID, containerName string) string {
	return filepath.Join(
		PIDFileDir(actorUID),
		containerName+".pid",
	)
}

// VolumesDir is ActorDirs.volumes_dir.
func VolumesDir(actorUID string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"volumes",
	)
}
