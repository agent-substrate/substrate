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

package pidfd

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestExited(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Wait()
	done, err := Exited(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("closed while the process runs")
	case <-time.After(100 * time.Millisecond):
	}
	_ = cmd.Process.Kill()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("not closed after the process exited")
	}
}

func TestExitedAlreadyGone(t *testing.T) {
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	done, err := Exited(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("not closed for a reaped process")
	}
}

func TestExitedFD(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Wait()
	fd, err := unix.PidfdOpen(cmd.Process.Pid, 0)
	if err != nil {
		t.Fatal(err)
	}
	done, err := ExitedFD(fd)
	if err != nil {
		t.Fatal(err)
	}
	// the caller's pidfd is its own to close
	unix.Close(fd)
	_ = cmd.Process.Kill()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("not closed after the process exited")
	}
}

// A blocking pidfd cannot join the netpoller. The watch must still wait for
// the exit, not report one straight away.
func TestWatchFallsBackToABlockingPoll(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Wait()
	fd, err := unix.PidfdOpen(cmd.Process.Pid, 0)
	if err != nil {
		t.Fatal(err)
	}
	done, err := watch(fd, make(chan struct{}))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("closed while the process runs")
	case <-time.After(200 * time.Millisecond):
	}
	_ = cmd.Process.Kill()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("not closed after the process exited")
	}
}

// A process that exits while the watcher is starting must still be seen.
func TestExitedRacingExit(t *testing.T) {
	for range 200 {
		cmd := exec.Command("true")
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done, err := Exited(cmd.Process.Pid)
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("exit missed")
		}
		_ = cmd.Wait()
	}
}

// Watching many processes must not cost a thread each.
func TestExitedHoldsNoThreads(t *testing.T) {
	var cmds []*exec.Cmd
	defer func() {
		for _, c := range cmds {
			_ = c.Process.Kill()
			_ = c.Wait()
		}
	}()
	before := threads(t)
	for range 200 {
		c := exec.Command("sleep", "30")
		if err := c.Start(); err != nil {
			t.Fatal(err)
		}
		cmds = append(cmds, c)
		if _, err := Exited(c.Process.Pid); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	if after := threads(t); after-before > 20 {
		t.Errorf("threads grew from %d to %d watching 200 processes", before, after)
	}
}

func threads(t *testing.T) int {
	t.Helper()
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, "Threads:"); ok {
			n, _ := strconv.Atoi(strings.TrimSpace(v))
			return n
		}
	}
	t.Fatal("no Threads line")
	return 0
}
