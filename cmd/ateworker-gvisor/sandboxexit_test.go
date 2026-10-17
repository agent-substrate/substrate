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

package main

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

// startFakeSandbox starts a process that runs until killed and records it as
// the sandbox.
func startFakeSandbox(t *testing.T) (*exec.Cmd, *sandboxProcess) {
	t.Helper()
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	actorDirs := &ateworkerpb.ActorDirs{RootDir: t.TempDir()}
	if err := os.MkdirAll(pidFileDir(actorDirs), 0o700); err != nil {
		t.Fatal(err)
	}
	pidFile := pidFilePath(actorDirs, ocispec.PauseContainer)
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := openSandboxProcess(actorDirs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return cmd, p
}

func TestSandboxWaitGoneAfterReap(t *testing.T) {
	cmd, p := startFakeSandbox(t)
	_ = cmd.Process.Kill()
	go func() { _, _ = cmd.Process.Wait() }()
	if err := p.waitGone(5 * time.Second); err != nil {
		t.Fatalf("waitGone = %v, want nil", err)
	}
}

func TestSandboxWaitGoneWaitsForReap(t *testing.T) {
	cmd, p := startFakeSandbox(t)
	// a zombie: killed, but nothing reaps it until cleanup
	_ = cmd.Process.Kill()
	err := p.waitGone(100 * time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "still present") {
		t.Fatalf("waitGone = %v, want a still present error", err)
	}
}

func TestSandboxWaitGoneTimesOutWhileRunning(t *testing.T) {
	_, p := startFakeSandbox(t)
	err := p.waitGone(10 * time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "still present") {
		t.Fatalf("waitGone = %v, want a still present error", err)
	}
}
