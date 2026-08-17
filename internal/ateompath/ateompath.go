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

// Ateom and atelet need to agree on many filesystem paths.  They are defined in this package.
package ateompath

import (
	"path/filepath"
)

const (
	// The base path.  This is both the path of the root shared folder on the
	// host filesystem, and when it is mounted into ateom and atelet containers.
	BasePath = "/var/lib/ateom-gvisor"
)

var (
	// StaticFilesDir holds things like downloaded runsc binaries.
	StaticFilesDir = filepath.Join(BasePath, "static-files")

	// ImageCacheDir is the node-local OCI image layer cache (see
	// internal/imagecache). It lives under BasePath so the cached layer
	// directories are visible at the same path in atelet (which writes them)
	// and in every ateom pod (which mounts them as overlay lowerdirs).
	ImageCacheDir = filepath.Join(BasePath, "image-cache")

	// ActorsDir holds the per-actor state directories (see ActorPath). The
	// image cache's eviction root-set scan reads the bundle overlay specs
	// under it.
	ActorsDir = filepath.Join(BasePath, "actors")

	// CredentialBrokerSocket is the node-local atelet socket used by atunnel
	// to request credentials for the worker's current actor assignment.
	CredentialBrokerSocket = filepath.Join(BasePath, "credential-broker.sock")
)

func RunSCBinaryPath(sha256 string) string {
	return filepath.Join(StaticFilesDir, "runsc-"+sha256)
}

// GVisorReleaseDir is the directory a gVisor release tarball (gvisor.tar.bz2,
// containing runsc plus its gvisor-bin/ helper binaries) is extracted into,
// content-addressed by the tarball's sha256. runsc requires the gvisor-bin/
// subdirectory to sit next to it, so the whole release is kept together under
// one directory rather than as loose files in StaticFilesDir.
func GVisorReleaseDir(sha256 string) string {
	return filepath.Join(StaticFilesDir, "gvisor-"+sha256)
}

// AteletOTLPSocketPath is the node-scoped unix socket atelet serves the OTLP
// relay on (see internal/otlprelay). It is node-scoped rather than per-pod
// because every ateom on the node pushes into the same relay: atelet is a
// DaemonSet, so one socket collapses N per-pod collector connections into one
// per-node connection.
//
// It sits directly under BasePath, which is the host directory already mounted
// at the same path into atelet and into every ateom pod, so no new volume is
// needed for ateom to reach it. Note that BasePath is mounted writable
// (workerpool_apply.go) and shared with CredentialBrokerSocket and the image
// cache, so a worker pod can unlink or replace this socket. Confining
// atelet-owned sockets to a subdirectory mounted read-only would be an
// improvement, but it is a property of the whole BasePath mount rather than of
// this socket — a read-only subdir needs its own volume and mount, and the pod
// keeps CAP_SYS_ADMIN. Tracked separately rather than solved here.
func AteletOTLPSocketPath() string {
	return filepath.Join(
		BasePath,
		"atelet-otlp.sock",
	)
}

func AteomPath(podUID string) string {
	return filepath.Join(
		BasePath,
		"ateoms",
		podUID,
	)
}

func AteomSocketPath(podUID string) string {
	return filepath.Join(
		AteomPath(podUID),
		"ateom.sock",
	)
}

func AteomNetNSName(podUID string) string {
	return "ateom:" + podUID
}

func AteomNetNSPath(podUID string) string {
	return filepath.Join(
		"/run/netns",
		AteomNetNSName(podUID),
	)
}

func ActorPath(actorUID string) string {
	return filepath.Join(
		ActorsDir,
		actorUID,
	)
}

// ActorIdentityDirPath is the host directory atelet populates with the
// actor's identity data (currently the single file "actor-id") and
// bind-mounts read-only into the actor. It is per-actor and regenerated on
// every resume, so (unlike the checkpointed process environment) it reflects
// the correct ID after a restore from the golden snapshot.
func ActorIdentityDirPath(actorUID string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"identity",
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

func RunSCStateDir(actorUID string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"runsc-state",
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

func RunscDebugLogDir(actorUID, containerName string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"runsc-debug-logs",
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

// DurableDirTarFile is the snapshot file holding the tar of a micro-VM
// actor's durable-dir volumes (entries are <volumeName>/... relative to
// DurableDirVolumeMountsDir). Written by ateom-microvm at checkpoint; a DATA
// snapshot consists of this file alone, so atelet uses the name to carve the
// durable data out of a FULL snapshot's file set.
const DurableDirTarFile = "durable-dir.tar"

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

func VolumesDir(actorUID string) string {
	return filepath.Join(
		ActorPath(actorUID),
		"volumes",
	)
}

func VolumeHostPath(actorUID, volumeName string) string {
	return filepath.Join(
		VolumesDir(actorUID),
		volumeName,
	)
}

// StagingDirPrefix returns the prefix directory for staging CSI volumes.
func StagingDirPrefix() string {
	return filepath.Join(BasePath, "staging")
}

// KubeletPluginSocketPath returns the path to the CSI driver socket in kubelet plugins directory.
func KubeletPluginSocketPath(driverName string) string {
	return filepath.Join("/var/lib/kubelet/plugins", driverName, "csi.sock")
}
