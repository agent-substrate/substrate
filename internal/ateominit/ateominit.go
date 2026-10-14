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

// Package ateominit runs a minimal init as PID 1 so ateom doesn't have to.
//
// PID 1 inherits every orphan (e.g. the runsc sandbox once `runsc create`
// exits) and must reap them with wait4(-1), which races os/exec for the exit
// status of ateom's own subprocesses. The init re-executes the binary as its
// only child, reaps everything, and exits with the child's status.
package ateominit

import (
	"fmt"
	"os"
	"os/signal"

	"golang.org/x/sys/unix"
)

// forwarded are the signals passed through to ateom.
var forwarded = []os.Signal{
	unix.SIGTERM, unix.SIGINT, unix.SIGHUP, unix.SIGQUIT, unix.SIGUSR1, unix.SIGUSR2,
}

// RunIfPID1 runs the init and exits if the process is PID 1, and otherwise
// returns. Call it first thing in main.
func RunIfPID1() {
	if os.Getpid() != 1 {
		return
	}
	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ateominit: finding the executable: %v\n", err)
		os.Exit(1)
	}
	os.Exit(supervise(exe, os.Args))
}

// supervise starts path as a child, reaps until it exits, and returns its exit
// code.
func supervise(path string, argv []string) int {
	// A no-op as PID 1, but it lets tests run this as an ordinary process.
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		fmt.Fprintf(os.Stderr, "ateominit: setting child subreaper: %v\n", err)
		return 1
	}

	// Subscribe before starting the child so no exit goes unnoticed.
	sigs := make(chan os.Signal, 32)
	signal.Notify(sigs, append([]os.Signal{unix.SIGCHLD}, forwarded...)...)
	defer signal.Stop(sigs)

	child, err := os.StartProcess(path, argv, &os.ProcAttr{
		Env:   os.Environ(),
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ateominit: starting %s: %v\n", path, err)
		return 1
	}

	for sig := range sigs {
		if sig != unix.SIGCHLD {
			_ = child.Signal(sig)
		}
		// Reap after every signal: os/signal drops signals when sigs is full,
		// so the SIGCHLD for the child's exit may never arrive.
		if status, exited := reap(child.Pid); exited {
			return exitCode(status)
		}
	}
	panic("unreachable")
}

// reap collects every exited child and reports whether pid was among them.
// SIGCHLD coalesces, so one signal may stand for several exits.
func reap(pid int) (unix.WaitStatus, bool) {
	var childStatus unix.WaitStatus
	childExited := false
	for {
		var status unix.WaitStatus
		got, err := unix.Wait4(-1, &status, unix.WNOHANG, nil)
		if err == unix.EINTR {
			continue
		}
		if err != nil || got <= 0 {
			return childStatus, childExited
		}
		if got == pid {
			childStatus, childExited = status, true
		}
	}
}

// exitCode maps a wait status to a shell-style exit code.
func exitCode(status unix.WaitStatus) int {
	if status.Signaled() {
		return 128 + int(status.Signal())
	}
	return status.ExitStatus()
}
