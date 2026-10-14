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

	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// startFakeSandbox starts a short-lived process and records it as the sandbox.
func startFakeSandbox(t *testing.T) (*exec.Cmd, *sandboxProcess) {
	t.Helper()
	cmd := exec.Command("sleep", "0.1")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	actorDirs := &ateompb.ActorDirs{RootDir: t.TempDir()}
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
	go func() { _ = cmd.Wait() }()
	if err := p.waitGone(5 * time.Second); err != nil {
		t.Fatalf("waitGone = %v, want nil", err)
	}
}

func TestSandboxWaitGoneWaitsForReap(t *testing.T) {
	cmd, p := startFakeSandbox(t)
	defer func() { _ = cmd.Wait() }()
	// Left a zombie: exited, but nothing reaps it.
	err := p.waitGone(time.Second)
	if err == nil || !strings.Contains(err.Error(), "unreaped") {
		t.Fatalf("waitGone = %v, want an unreaped error", err)
	}
	if !isZombie(p.pid) {
		t.Fatalf("isZombie(%d) = false, want true", p.pid)
	}
}

func TestSandboxWaitGoneTimesOutWhileRunning(t *testing.T) {
	cmd, p := startFakeSandbox(t)
	defer func() { _ = cmd.Wait() }()
	err := p.waitGone(10 * time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "still running") {
		t.Fatalf("waitGone = %v, want a still running error", err)
	}
}
