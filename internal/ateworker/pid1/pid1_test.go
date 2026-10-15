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

package ateominit

import (
	"os"
	"os/signal"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestSuperviseReturnsExitCode(t *testing.T) {
	if got := supervise("/bin/sh", []string{"sh", "-c", "exit 3"}); got != 3 {
		t.Fatalf("supervise = %d, want 3", got)
	}
}

func TestSuperviseReturnsSignalExitCode(t *testing.T) {
	if got := supervise("/bin/sh", []string{"sh", "-c", "kill -KILL $$"}); got != 128+9 {
		t.Fatalf("supervise = %d, want %d", got, 128+9)
	}
}

func TestSuperviseReapsOrphans(t *testing.T) {
	// The inner sh is orphaned to us and exits while the child still runs.
	if got := supervise("/bin/sh", []string{"sh", "-c", "(sh -c 'exit 0' &); sleep 0.5"}); got != 0 {
		t.Fatalf("supervise = %d, want 0", got)
	}
	var status unix.WaitStatus
	if pid, err := unix.Wait4(-1, &status, unix.WNOHANG, nil); err != unix.ECHILD {
		t.Fatalf("Wait4 = (%d, %v), want ECHILD: a child was left unreaped", pid, err)
	}
}

func TestSuperviseForwardsSignals(t *testing.T) {
	// Keep SIGTERM from killing the test before supervise subscribes.
	ignore := make(chan os.Signal, 1)
	signal.Notify(ignore, unix.SIGTERM)
	defer signal.Stop(ignore)

	done := make(chan int)
	go func() { done <- supervise("/bin/sh", []string{"sh", "-c", "exec sleep 30"}) }()
	// Repeated, since the first may land before supervise subscribes.
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case got := <-done:
			if got != 128+int(unix.SIGTERM) {
				t.Fatalf("supervise = %d, want %d", got, 128+int(unix.SIGTERM))
			}
			return
		case <-tick.C:
			_ = unix.Kill(unix.Getpid(), unix.SIGTERM)
		case <-timeout:
			t.Fatal("SIGTERM was not forwarded to the child")
		}
	}
}
