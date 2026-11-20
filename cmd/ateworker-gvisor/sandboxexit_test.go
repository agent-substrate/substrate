//go:build linux

// Copyright 2026 The Agent Substrate Authors
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
	"context"
	"math"
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
	pidFile := pidFilePath(actorDirs, ocispec.SandboxID)
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

func TestWaitSandboxExitedReturnsFalseWhenExitUnconfirmed(t *testing.T) {
	tests := []struct {
		name    string
		sandbox *sandboxProcess
	}{
		{name: "missing sandbox"},
		// no process can have this descriptor open
		{name: "invalid pidfd", sandbox: &sandboxProcess{pidfd: math.MaxInt32}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if waitSandboxExited(t.Context(), tc.sandbox) {
				t.Fatal("waitSandboxExited = true, want false")
			}
		})
	}
}

func TestSandboxWaitExited(t *testing.T) {
	for _, tc := range []struct {
		name     string
		kill     bool
		reap     bool
		canceled bool
		timeout  time.Duration
		wantErr  string
	}{
		{name: "reaped", kill: true, reap: true, timeout: 5 * time.Second},
		// nothing reaps it until cleanup
		{name: "zombie", kill: true, timeout: 5 * time.Second},
		{name: "running", timeout: 10 * time.Millisecond, wantErr: "still running"},
		{name: "canceled", canceled: true, timeout: 5 * time.Second, wantErr: "context canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd, p := startFakeSandbox(t)
			if tc.kill {
				_ = cmd.Process.Kill()
			}
			if tc.reap {
				_, _ = cmd.Process.Wait()
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			err := p.waitExited(ctx, tc.timeout)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("waitExited = %v, want an error containing %q", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("waitExited = %v, want nil", err)
			}
		})
	}
}

func TestSandboxExited(t *testing.T) {
	cmd, p := startFakeSandbox(t)
	exited, err := p.exited()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-exited:
		t.Fatal("closed while the sandbox runs")
	case <-time.After(100 * time.Millisecond):
	}
	_ = cmd.Process.Kill()
	select {
	case <-exited:
	case <-time.After(10 * time.Second):
		t.Fatal("not closed after the sandbox exited")
	}
	// the watch uses its own copy, so the sandbox's pidfd still works
	if err := p.waitExited(t.Context(), 5*time.Second); err != nil {
		t.Fatalf("waitExited after the watch = %v", err)
	}
}
