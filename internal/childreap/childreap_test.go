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

//go:build linux

package childreap

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

// waitReaped fails the test if pid is still in /proc after timeout.
func waitReaped(t *testing.T, pid int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		_, err := os.Stat("/proc/" + strconv.Itoa(pid))
		if errors.Is(err, fs.ErrNotExist) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("child %d was not reaped within %s", pid, timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// An orphan is collected while a tracked subprocess is still running: reaping
// does not wait for a quiet moment.
func TestOrphansAreReapedBesideARunningCommand(t *testing.T) {
	r := New()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go r.Run(ctx)

	var long sync.WaitGroup
	long.Go(func() { _ = r.RunCommand(exec.Command("/bin/sleep", "3")) })
	defer long.Wait()

	orphan := exec.Command("true")
	if err := orphan.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	waitReaped(t, orphan.Process.Pid, 2*time.Second)
}

// A child that died before Run began raised a SIGCHLD nobody heard.
func TestTheFirstPassCollectsEarlierOrphans(t *testing.T) {
	orphan := exec.Command("true")
	if err := orphan.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	r := New()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go r.Run(ctx)
	waitReaped(t, orphan.Process.Pid, 2*time.Second)
}

// Short subprocesses must stay short while a long one is always in flight
// beside them, and no exit status may be taken from a tracked subprocess
// while orphans keep SIGCHLD pending.
func TestShortSubprocessesAreNotPacedByLongOnes(t *testing.T) {
	const (
		shortRunners = 8
		short        = 20 * time.Millisecond
		testFor      = 3 * time.Second
	)

	r := New()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go r.Run(ctx)

	run, stop := context.WithTimeout(ctx, testFor)
	defer stop()
	var wg sync.WaitGroup

	// Keep SIGCHLD pending throughout the test.
	for range 4 {
		wg.Go(func() {
			for run.Err() == nil {
				cmd := exec.Command("true")
				if cmd.Start() != nil {
					return
				}
				_ = cmd.Wait()
			}
		})
	}

	// Keep one long subprocess in flight.
	wg.Go(func() {
		for run.Err() == nil {
			_ = r.RunCommand(exec.Command("/bin/sleep", "0.4"))
		}
	})

	var mu sync.Mutex
	var samples []time.Duration
	for range shortRunners {
		wg.Go(func() {
			for run.Err() == nil {
				started := time.Now()
				err := r.RunCommand(exec.Command("/bin/sleep", "0.02"))
				elapsed := time.Since(started)
				mu.Lock()
				if err != nil {
					t.Errorf("short subprocess: %v", err)
				}
				samples = append(samples, elapsed)
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(samples) < 50 {
		t.Fatalf("only %d short subprocesses completed in %s; too few to judge", len(samples), testFor)
	}
	slices.Sort(samples)
	p90 := samples[len(samples)*90/100]
	if limit := 8 * short; p90 > limit {
		t.Errorf("p90 of %d short subprocesses was %s, want under %s (median %s, max %s)",
			len(samples), p90.Round(time.Millisecond), limit,
			samples[len(samples)/2].Round(time.Millisecond), samples[len(samples)-1].Round(time.Millisecond))
	}
}

func TestCombinedOutputKeepsBothStreams(t *testing.T) {
	out, err := New().CombinedOutput(exec.Command("sh", "-c", "echo out; echo err >&2"))
	if err != nil {
		t.Fatalf("CombinedOutput: %v", err)
	}
	if string(out) != "out\nerr\n" {
		t.Errorf("output = %q, want %q", out, "out\nerr\n")
	}
}

func TestParseStatReadsPastACommandNameWithParentheses(t *testing.T) {
	state, ppid, ok := parseStat([]byte("4242 (a (b) c) Z 17 4242 4242 0 -1 4194560"))
	if !ok || state != 'Z' || ppid != 17 {
		t.Errorf("parseStat = %c %d %v, want Z 17 true", state, ppid, ok)
	}
	if _, _, ok := parseStat([]byte("garbage")); ok {
		t.Error("parseStat accepted a line without fields")
	}
}
