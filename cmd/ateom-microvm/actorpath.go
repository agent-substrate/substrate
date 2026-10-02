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

// The paths ateom-microvm derives from the ActorDirs atelet sends. The
// directory under root_dir is ateom-microvm's own: atelet does not know it.

package main

import (
	"path/filepath"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// ociBundlePath is the container's OCI bundle.
func ociBundlePath(actorDirs *ateompb.ActorDirs, containerName string) string {
	return filepath.Join(actorDirs.GetOciBundleDir(), containerName)
}

// rootfsUpperDir is the host directory backing the actor's rootfs overlay
// uppers: one subdirectory per container (see kata.UpperWorkDirs). Local to
// this binary: atelet never touches it, so it is not one of the ActorDirs.
func rootfsUpperDir(actorDirs *ateompb.ActorDirs) string {
	return filepath.Join(actorDirs.GetRootDir(), "rootfs-upper")
}
