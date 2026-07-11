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
)

func RunSCBinaryPath(sha256 string) string {
	return filepath.Join(StaticFilesDir, "runsc-"+sha256)
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

func ActorPath(atespace, actorName string) string {
	return filepath.Join(
		BasePath,
		"actors",
		atespace+":"+actorName,
	)
}

// ActorIdentityDirPath is the host directory atelet populates with the
// actor's identity data (currently the single file "actor-id") and
// bind-mounts read-only into the actor. It is per-actor and regenerated on
// every resume, so (unlike the checkpointed process environment) it reflects
// the correct ID after a restore from the golden snapshot.
func ActorIdentityDirPath(atespace, actorName string) string {
	return filepath.Join(
		ActorPath(atespace, actorName),
		"identity",
	)
}

// ActorSandboxAssetsFile is the per-actor file where atelet records the sandbox
// binaries (class + content-addressed asset set, for this node's architecture)
// the actor is currently running. It is written at Run/Restore and read at
// Checkpoint (when the request no longer carries the sandbox config). It lives
// directly under ActorPath — NOT under a subdir wiped by atelet's
// resetActorDirs — so it survives between Run and a later Checkpoint.
func ActorSandboxAssetsFile(atespace, actorName string) string {
	return filepath.Join(
		ActorPath(atespace, actorName),
		"sandbox-assets.json",
	)
}

func RunSCStateDir(atespace, actorName string) string {
	return filepath.Join(
		ActorPath(atespace, actorName),
		"runsc-state",
	)
}

func OCIBundleDir(atespace, actorName string) string {
	return filepath.Join(
		ActorPath(atespace, actorName),
		"bundles",
	)
}

func OCIBundlePath(atespace, actorName, containerName string) string {
	return filepath.Join(
		OCIBundleDir(atespace, actorName),
		containerName,
	)
}

func RunscDebugLogDir(atespace, actorName, containerName string) string {
	return filepath.Join(
		ActorPath(atespace, actorName),
		"runsc-debug-logs",
		containerName,
	)
}

func CheckpointStateDir(atespace, actorName string) string {
	return filepath.Join(
		ActorPath(atespace, actorName),
		"checkpoint-state",
	)
}

func LocalCheckpointsDir(atespace, actorName string) string {
	return filepath.Join(
		ActorPath(atespace, actorName),
		"local-checkpoint",
	)
}

// DurableDirVolumeMountsDir is the directory where individual durable-dir
// volumes are mounted.
func DurableDirVolumeMountsDir(atespace, actorName string) string {
	return filepath.Join(
		ActorPath(atespace, actorName),
		"durable-dir",
	)
}

// DurableDirVolumeMountPoint returns the path where a specific durable-dir volume is mounted on the nodeVM.
func DurableDirVolumeMountPoint(atespace, actorName, volumeName string) string {
	return filepath.Join(
		DurableDirVolumeMountsDir(atespace, actorName),
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
func RestoreStateDir(atespace, actorName string) string {
	return filepath.Join(
		ActorPath(atespace, actorName),
		"restore-state",
	)
}

func PIDFileDir(atespace, actorName string) string {
	return filepath.Join(
		ActorPath(atespace, actorName),
		"pidfiles",
	)
}

func PIDFilePath(atespace, actorName, containerName string) string {
	return filepath.Join(
		PIDFileDir(atespace, actorName),
		containerName+".pid",
	)
}
