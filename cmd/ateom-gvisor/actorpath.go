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

// The paths ateom-gvisor derives from the ActorDirs atelet sends.

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// ociBundlePath is the container's OCI bundle.
func ociBundlePath(actorDirs *ateompb.ActorDirs, containerName string) string {
	return filepath.Join(actorDirs.GetOciBundleDir(), containerName)
}

// runscStateDir is runsc --root for the actor's sandbox.
func runscStateDir(actorDirs *ateompb.ActorDirs) string {
	return filepath.Join(actorDirs.GetRootDir(), "runsc-state")
}

// pidFileDir is where runsc writes <container>.pid.
func pidFileDir(actorDirs *ateompb.ActorDirs) string {
	return filepath.Join(actorDirs.GetRootDir(), "pidfiles")
}

// pidFilePath is where runsc writes the container's pid.
func pidFilePath(actorDirs *ateompb.ActorDirs, containerName string) string {
	return filepath.Join(pidFileDir(actorDirs), containerName+".pid")
}

// resolvConfPath is the resolver bind source outside the actor's rootfs.
func resolvConfPath(actorDirs *ateompb.ActorDirs) string {
	return filepath.Join(actorDirs.GetRootDir(), "resolv.conf")
}

// resetRunscStateAndPidFileDirs empties both directories for a new activation.
func resetRunscStateAndPidFileDirs(actorDirs *ateompb.ActorDirs) error {
	for _, dir := range []string{runscStateDir(actorDirs), pidFileDir(actorDirs)} {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("while clearing %q: %w", dir, err)
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("while creating %q: %w", dir, err)
		}
	}
	return nil
}
