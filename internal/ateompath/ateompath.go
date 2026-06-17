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

func ActorPath(actorTemplateNamespace, actorTemplateName, actorID string) string {
	return filepath.Join(
		BasePath,
		"actors",
		actorTemplateNamespace+":"+actorTemplateName+":"+actorID,
	)
}

// ActorIdentityDirPath is the host directory atelet populates with the
// actor's identity data (currently the single file "actor-id") and
// bind-mounts read-only into the actor. It is per-actor and regenerated on
// every resume, so (unlike the checkpointed process environment) it reflects
// the correct ID after a restore from the golden snapshot.
func ActorIdentityDirPath(actorTemplateNamespace, actorTemplateName, actorID string) string {
	return filepath.Join(
		ActorPath(actorTemplateNamespace, actorTemplateName, actorID),
		"identity",
	)
}

// ActorSandboxAssetsFile is the per-actor file where atelet records the sandbox
// binaries (class + content-addressed asset set, for this node's architecture)
// the actor is currently running. It is written at Run/Restore and read at
// Checkpoint (when the request no longer carries the sandbox config). It lives
// directly under ActorPath — NOT under a subdir wiped by atelet's
// resetActorDirs — so it survives between Run and a later Checkpoint.
func ActorSandboxAssetsFile(actorTemplateNamespace, actorTemplateName, actorID string) string {
	return filepath.Join(
		ActorPath(actorTemplateNamespace, actorTemplateName, actorID),
		"sandbox-assets.json",
	)
}

func RunSCStateDir(actorTemplateNamespace, actorTemplateName, actorID string) string {
	return filepath.Join(
		ActorPath(actorTemplateNamespace, actorTemplateName, actorID),
		"runsc-state",
	)
}

func OCIBundleDir(actorTemplateNamespace, actorTemplateName, actorID string) string {
	return filepath.Join(
		ActorPath(actorTemplateNamespace, actorTemplateName, actorID),
		"bundles",
	)
}

func OCIBundlePath(actorTemplateNamespace, actorTemplateName, actorID, containerName string) string {
	return filepath.Join(
		OCIBundleDir(actorTemplateNamespace, actorTemplateName, actorID),
		containerName,
	)
}

func RunscDebugLogDir(actorTemplateNamespace, actorTemplateName, actorID, containerName string) string {
	return filepath.Join(
		ActorPath(actorTemplateNamespace, actorTemplateName, actorID),
		"runsc-debug-logs",
		containerName,
	)
}

func CheckpointStateDir(actorTemplateNamespace, actorTemplateName, actorID string) string {
	return filepath.Join(
		ActorPath(actorTemplateNamespace, actorTemplateName, actorID),
		"checkpoint-state",
	)
}

func LocalCheckpointsDir(actorTemplateNamespace, actorTemplateName, actorID string) string {
	return filepath.Join(
		ActorPath(actorTemplateNamespace, actorTemplateName, actorID),
		"local-checkpoint",
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
func RestoreStateDir(actorTemplateNamespace, actorTemplateName, actorID string) string {
	return filepath.Join(
		ActorPath(actorTemplateNamespace, actorTemplateName, actorID),
		"restore-state",
	)
}

func PIDFileDir(actorTemplateNamespace, actorTemplateName, actorID string) string {
	return filepath.Join(
		ActorPath(actorTemplateNamespace, actorTemplateName, actorID),
		"pidfiles",
	)
}

func PIDFilePath(actorTemplateNamespace, actorTemplateName, actorID, containerName string) string {
	return filepath.Join(
		PIDFileDir(actorTemplateNamespace, actorTemplateName, actorID),
		containerName+".pid",
	)
}
