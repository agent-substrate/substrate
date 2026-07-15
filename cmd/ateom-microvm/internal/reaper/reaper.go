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

// Package reaper centralizes ateom-microvm's child-process reaping.
//
// ateom-microvm spawns long-lived, detached helpers (the cloud-hypervisor VMM,
// virtiofsd) that get reparented to it, so it runs a SIGCHLD reaper (Start) to
// collect them. That reaper calls wait4(-1), which will collect ANY child of the
// process — including one an exec.Cmd is about to wait for itself. When that
// happens the caller's own wait fails with "waitid: no child processes".
//
// Run and RunCombined are for the other kind of subprocess: short, synchronous
// helpers (mount, umount, cp, ...) whose exit status the caller needs. They hold a
// shared lock that is mutually exclusive with the reaper's collection pass, so the
// reaper cannot collect the child before the caller's wait completes. Every
// synchronous subprocess in ateom-microvm MUST go through Run/RunCombined.
//
// Detached daemons that must outlive the call (the VMM, virtiofsd) are deliberately
// NOT run through here: they are started with Cmd.Start and left for the reaper.
//
// gVisor's ateom (cmd/ateom-gvisor) holds the same kind of lock in shared mode around
// every runsc invocation; this package is the micro-VM equivalent, shared across the
// ateom-microvm subpackages that exec (internal/kata, internal/ch).
package reaper

import (
	"os/exec"
	"sync"
)

// lock serializes deliberate fork+exec+wait against the child reaper.
var lock sync.RWMutex

// Run runs cmd with the reap lock held shared so the child reaper can't reap it first.
func Run(cmd *exec.Cmd) error {
	lock.RLock()
	defer lock.RUnlock()
	return cmd.Run()
}

// RunCombined is Run for callers that need cmd.CombinedOutput.
func RunCombined(cmd *exec.Cmd) ([]byte, error) {
	lock.RLock()
	defer lock.RUnlock()
	return cmd.CombinedOutput()
}
