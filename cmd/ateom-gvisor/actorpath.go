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
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/agent-substrate/substrate/internal/imagecache"
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

// runscLogPath is runsc -log for every command run for the actor, and for
// the sentry they start: the file a failed command's error quotes from. It
// is removed with the activation, so it holds only this activation's lines.
func runscLogPath(actorDirs *ateompb.ActorDirs) string {
	return filepath.Join(actorDirs.GetRootDir(), "runsc.log")
}

// panicLogPath is runsc -panic-log: where the sentry writes a panic, so it
// outlives the sentry. One file per sandbox, since the sub-containers share
// the pause container's sentry.
func panicLogPath(actorDirs *ateompb.ActorDirs) string {
	return filepath.Join(actorDirs.GetRootDir(), "sentry-panic.log")
}

// resolvConfPath is the resolver bind source outside the actor's rootfs.
func resolvConfPath(actorDirs *ateompb.ActorDirs) string {
	return filepath.Join(actorDirs.GetRootDir(), "resolv.conf")
}

// resetRunscStateAndPidFileDirs empties both directories for a new activation,
// and removes the previous activation's runsc log and sentry panic log so a
// failure never quotes another activation's lines. runsc can leave mounts
// behind in its state directory (its null-netns), which must be detached in
// this mount namespace before they can be removed.
func resetRunscStateAndPidFileDirs(actorDirs *ateompb.ActorDirs) error {
	for _, file := range []string{runscLogPath(actorDirs), panicLogPath(actorDirs)} {
		if err := os.Remove(file); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("while removing %q: %w", file, err)
		}
	}
	for _, dir := range []string{runscStateDir(actorDirs), pidFileDir(actorDirs)} {
		if err := imagecache.UnmountAllUnder(dir); err != nil {
			return fmt.Errorf("while unmounting under %q: %w", dir, err)
		}
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("while clearing %q: %w", dir, err)
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("while creating %q: %w", dir, err)
		}
	}
	return nil
}
