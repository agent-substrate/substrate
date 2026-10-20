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

// Package ateletpath is atelet's on-node layout: the per-actor directories
// it passes to worker as ActorDirs, and the directories only atelet uses.
package ateletpath

import (
	"path/filepath"

	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

var (
	// ImageCacheDir is the node-local OCI image layer cache (see
	// internal/imagecache). It lives under BasePath so the cached layer
	// directories are visible at the same path in atelet (which writes them)
	// and in every worker pod (which mounts them as overlay lowerdirs).
	ImageCacheDir = filepath.Join(nodepath.BasePath, "image-cache")

	// SnapshotScratchDir holds short-lived directories for snapshot manifests
	// moved through the object-store plugin. It lives under BasePath, which the
	// node plugin shares with atelet.
	SnapshotScratchDir = filepath.Join(nodepath.BasePath, "snapshot-scratch")

	// SandboxAssetCacheDir is the node-local sandbox asset cache (see
	// cmd/atelet/internal/filecache): runsc releases, micro-VM kernels, VMMs
	// and guest images, content-addressed. It lives under BasePath, on the
	// same mount as ActorsDir, because an actor's assets are hard links from
	// here into its SandboxAssetsDir.
	SandboxAssetCacheDir = filepath.Join(nodepath.BasePath, "sandbox-asset-cache")

	// SandboxAssetScratchDir holds short-lived destination directories for
	// asset fetches that have no actor, such as prewarming a SandboxConfig's
	// default version. Wiped at startup. Under BasePath so the links the
	// cache serves into it are on the cache's mount.
	SandboxAssetScratchDir = filepath.Join(nodepath.BasePath, "sandbox-asset-scratch")

	// SandboxRuntimeIndexFile holds every sandbox runtime a worker on this
	// node has advertised, by the runtime's hash, with the asset record the
	// hash was computed from. atelet rewrites it before a worker's own file
	// can let the worker register a new hash, and loads it at startup, so a
	// runtime registered before a restart resolves the moment atelet serves
	// again.
	SandboxRuntimeIndexFile = filepath.Join(nodepath.BasePath, "sandbox-runtime-index.json")

	// SnapshotCacheDir is the node-local cache of shared snapshot files, the
	// snapshots many actors are created from (see
	// cmd/atelet/internal/filecache). It lives under BasePath because cache
	// hits are served as hard links into the per-actor restore dirs under
	// ActorsDir, and link(2) requires both ends on one mounted filesystem,
	// the same mount, not merely the same disk.
	SnapshotCacheDir = filepath.Join(nodepath.BasePath, "snapshot-cache")
)

func ActorPath(actorUID string) string {
	return filepath.Join(
		nodepath.ActorsDir,
		actorUID,
	)
}

// ActorSandboxAssetsFile is the per-actor file where atelet records the sandbox
// binaries (class + content-addressed asset set, for this node's architecture)
// the actor is currently running. It is written at Run/Restore and read at
// Checkpoint (when the request no longer carries the sandbox config). It lives
// directly under ActorPath — NOT under a subdir wiped by atelet's
// resetActorDirs — so it survives between Run and a later Checkpoint.
func ActorSandboxAssetsFile(actorUID string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"sandbox-assets.json",
	)
}

// ActorSnapshotStateFile is the per-actor file where atelet keeps what it
// tracks of the actor's snapshots: its latest local and durable snapshots, so
// a restarted atelet still knows them. Like ActorSandboxAssetsFile it lives
// directly under ActorPath, outside the subdirs resetActorDirs wipes.
func ActorSnapshotStateFile(actorUID string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"snapshot-state.json",
	)
}

// SandboxAssetsDir holds the actor's sandbox runtime assets, one entry per
// asset name, each a hard link (or a directory of hard links, for a gVisor
// release) from SandboxAssetCacheDir. The worker runs binaries from here
// only, and derives the same path itself to check a request against.
func SandboxAssetsDir(actorUID string) string {
	return nodepath.ActorSandboxAssetsDir(actorUID)
}

// SandboxAssetsStagingDir is where Run stages an activation's assets before
// moving them into SandboxAssetsDir, so a failed fetch leaves whatever a
// live activation has there untouched. Not among the directories
// resetActorDirs empties.
func SandboxAssetsStagingDir(actorUID string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"sandbox-assets.staging",
	)
}

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

func CheckpointStateDir(actorUID string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"checkpoint-state",
	)
}

func LocalCheckpointsDir(actorUID string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"local-checkpoint",
	)
}

// LocalSnapshotDir is the directory holding one named local (pause) snapshot
// of an actor: the checkpoint files plus their manifest.
func LocalSnapshotDir(actorUID, snapshotName string) string {
	return filepath.Join(LocalCheckpointsDir(actorUID), snapshotName)
}

// DurableDirVolumeMountsDir is the directory where individual durable-dir
// volumes are mounted.
func DurableDirVolumeMountsDir(actorUID string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"durable-dir",
	)
}

// DurableDirVolumeMountPoint returns the path where a specific durable-dir volume is mounted on the nodeVM.
func DurableDirVolumeMountPoint(actorUID, volumeName string) string {
	return filepath.Join(
		DurableDirVolumeMountsDir(actorUID),
		volumeName,
	)
}

// SystemInfoVolumeRootsDir is the directory containing the per-volume root
// directories of system-info volumes. Snapshots must capture durable-dir
// data but never system-info contents, which atelet regenerates on every
// Run/Restore; each sandbox class excludes them differently:
//
//   - micro-VM captures by location: its checkpoint tars all of
//     DurableDirVolumeMountsDir (see ateworker-microvm's tarDurableVolumes), so
//     system-info roots are excluded by living in this separate directory.
//   - gVisor captures by declaration: durable mounts are registered with
//     the sandbox (mount-hint annotations for MEMORY checkpoints, the
//     enumerated durable mount paths for VOLUMES fscheckpoints); system-info
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

// SystemInfoVolumeRoot returns the host path of the root directory for a
// specific system-info volume.
func SystemInfoVolumeRoot(actorUID, volumeName string) string {
	return filepath.Join(
		SystemInfoVolumeRootsDir(actorUID),
		volumeName,
	)
}

// RestoreStateDir is the local directory to use to restore an actor from a
// checkpoint downloaded from GCS.
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

func ExternalVolumeMountsDir(actorUID string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"volumes",
	)
}

func ExternalVolumeHostPath(actorUID, volumeName string) string {
	return filepath.Join(
		ExternalVolumeMountsDir(actorUID),
		volumeName,
	)
}

// ActorDirs is the directory set atelet passes to worker for an actor. worker
// takes these from the request rather than deriving them from the actor UID.
func ActorDirs(actorUID string) *ateworkerpb.ActorDirs {
	return &ateworkerpb.ActorDirs{
		RootDir:                   ActorPath(actorUID),
		OciBundleDir:              OCIBundleDir(actorUID),
		CheckpointDir:             CheckpointStateDir(actorUID),
		RestoreDir:                RestoreStateDir(actorUID),
		DurableDirVolumeMountsDir: DurableDirVolumeMountsDir(actorUID),
		SystemInfoVolumeRootsDir:  SystemInfoVolumeRootsDir(actorUID),
		ExternalVolumeMountsDir:   ExternalVolumeMountsDir(actorUID),
		SandboxAssetsDir:          SandboxAssetsDir(actorUID),
	}
}
