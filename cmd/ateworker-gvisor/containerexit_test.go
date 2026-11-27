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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/actorlog"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

type fakeExitReader struct {
	status   string
	stateErr error
	code     int32
	waitErr  error
	// hold, if set, blocks cmdState until it is closed. running counts the
	// calls that wait on it.
	hold    chan struct{}
	running atomic.Int32
}

func (f *fakeExitReader) cmdState(context.Context, string) (string, error) {
	if f.hold != nil {
		f.running.Add(1)
		<-f.hold
		f.running.Add(-1)
	}
	return f.status, f.stateErr
}

func (f *fakeExitReader) cmdExitCode(context.Context, string) (int32, error) {
	return f.code, f.waitErr
}

// exitService hosts testActor on a service that logs to w.
func exitService(w io.Writer) (*AteWorkerService, *hostedActor) {
	hosted := &hostedActor{attribution: testActor}
	s := &AteWorkerService{
		locks:       actorlock.New(),
		actors:      map[string]*hostedActor{testActor.UID: hosted},
		actorLogger: actorlog.NewActorLogger(w, false),
		exitChecks:  make(chan struct{}, containerExitChecks),
	}
	return s, hosted
}

// liveSandbox pins a process that runs until the test ends as the actor's
// sandbox.
func liveSandbox(t *testing.T, h *hostedActor) {
	t.Helper()
	cmd := exec.Command("sleep", "600")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	actorDirs := &ateworkerpb.ActorDirs{RootDir: t.TempDir()}
	if err := os.MkdirAll(pidFileDir(actorDirs), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pidFilePath(actorDirs, ocispec.SandboxID), []byte(strconv.Itoa(cmd.Process.Pid)), 0o600); err != nil {
		t.Fatal(err)
	}
	sandbox, err := openSandboxProcess(actorDirs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sandbox.Close)
	h.sandbox = sandbox
}

// deadSandbox pins a process that has exited as the actor's sandbox.
func deadSandbox(t *testing.T, h *hostedActor) {
	t.Helper()
	sandbox, err := openSandboxProcess(exitingSandbox(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sandbox.Close)
	if err := sandbox.waitExited(context.Background(), sandboxExitTimeout); err != nil {
		t.Fatal(err)
	}
	h.sandbox = sandbox
}

func TestContainerOutputEndedLogsExit(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rcmd     *fakeExitReader
		wantCode any // nil means the key is absent
	}{
		{"exit code", &fakeExitReader{status: containerStopped, code: 137}, float64(137)},
		{"clean exit", &fakeExitReader{status: containerStopped}, float64(0)},
		{"no exit code", &fakeExitReader{status: containerStopped, waitErr: errors.New("wait failed")}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			s, hosted := exitService(&buf)
			liveSandbox(t, hosted)
			s.containerOutputEnded(hosted, tc.rcmd, "app")
			var record map[string]any
			if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
				t.Fatalf("log = %q, want one exit record: %v", buf.String(), err)
			}
			if got := record["message"]; got != actorlog.ContainerExitedMessage {
				t.Errorf("message = %v, want %q", got, actorlog.ContainerExitedMessage)
			}
			if got := record[string(ateattr.ActorExitedContainerNameKey)]; got != "app" {
				t.Errorf("container = %v, want app", got)
			}
			if got := record[string(ateattr.ActorExitedContainerExitCodeKey)]; got != tc.wantCode {
				t.Errorf("exit code = %v, want %v", got, tc.wantCode)
			}
		})
	}
}

func TestContainerOutputEndedIgnores(t *testing.T) {
	stopped := &fakeExitReader{status: containerStopped}
	for _, tc := range []struct {
		name  string
		rcmd  *fakeExitReader
		setup func(*testing.T, *AteWorkerService, *hostedActor)
	}{
		{"container closed its output and still runs", &fakeExitReader{status: "running"}, nil},
		{"state unknown", &fakeExitReader{status: containerStopped, stateErr: errors.New("no state")}, nil},
		{"stopped by ateworker", stopped, func(_ *testing.T, _ *AteWorkerService, h *hostedActor) { h.stopping.Store(true) }},
		{"worker shutting down", stopped, func(_ *testing.T, s *AteWorkerService, _ *hostedActor) { s.shuttingDown.Store(true) }},
		{"actor no longer hosted", stopped, func(_ *testing.T, s *AteWorkerService, _ *hostedActor) { delete(s.actors, testActor.UID) }},
		{"actor hosted again", stopped, func(_ *testing.T, s *AteWorkerService, _ *hostedActor) {
			s.actors[testActor.UID] = &hostedActor{attribution: testActor}
		}},
		{"sandbox died", stopped, func(t *testing.T, _ *AteWorkerService, h *hostedActor) { deadSandbox(t, h) }},
		{"wait failed and the sandbox is not pinned", &fakeExitReader{status: containerStopped, waitErr: errors.New("wait failed")}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			s, hosted := exitService(&buf)
			if tc.setup != nil {
				tc.setup(t, s, hosted)
			}
			s.containerOutputEnded(hosted, tc.rcmd, "app")
			if buf.Len() != 0 {
				t.Errorf("log = %q, want no record", buf.String())
			}
		})
	}
}

// recordWriter sends each record the logger writes on a channel.
type recordWriter chan map[string]any

func (w recordWriter) Write(p []byte) (int, error) {
	var record map[string]any
	if err := json.Unmarshal(p, &record); err != nil {
		return 0, err
	}
	w <- record
	return len(p), nil
}

// TestStartContainerLogsOrder pins that the exit record comes after the
// container's last line, including a line with no newline.
func TestStartContainerLogsOrder(t *testing.T) {
	records := make(recordWriter, 2)
	s, hosted := exitService(records)
	pw, err := s.startContainerLogs(hosted, &fakeExitReader{status: containerStopped, code: 1}, "app")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pw.WriteString("fatal: out of cheese"); err != nil {
		t.Fatal(err)
	}
	pw.Close()
	for _, want := range []string{"fatal: out of cheese", actorlog.ContainerExitedMessage} {
		select {
		case record := <-records:
			if got := record["message"]; got != want {
				t.Errorf("record = %v, want %q", got, want)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("no %q record", want)
		}
	}
}

// TestContainerExitChecksAreCapped pins that the worker runs no more than
// containerExitChecks checks at once when the containers of many actors exit
// together.
func TestContainerExitChecksAreCapped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, _ := exitService(io.Discard)
		rcmd := &fakeExitReader{status: "running", hold: make(chan struct{})}
		const actors = 3 * containerExitChecks
		for i := range actors {
			attribution := testActor
			attribution.UID = "actor-" + strconv.Itoa(i)
			hosted := &hostedActor{attribution: attribution}
			s.actors[attribution.UID] = hosted
			go s.containerOutputEnded(hosted, rcmd, "app")
		}
		synctest.Wait()
		if got := rcmd.running.Load(); got != containerExitChecks {
			t.Errorf("checks that run at once = %d, want %d", got, containerExitChecks)
		}
		close(rcmd.hold)
		synctest.Wait()
		if got := len(s.exitChecks); got != 0 {
			t.Errorf("slots held after the checks = %d, want 0", got)
		}
	})
}

// scriptRunsc is a runsc that prints output and exits with code.
func scriptRunsc(t *testing.T, output string, code int) *runsc {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runsc")
	script := "#!/bin/sh\ncat <<'EOF'\n" + output + "\nEOF\nexit " + strconv.Itoa(code) + "\n"
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return &runsc{path: path, actorDirs: testActorDirs, flags: mustFlagStore(t)}
}

func TestCmdState(t *testing.T) {
	got, err := scriptRunsc(t, `{"ociVersion":"1.2.1","id":"app","status":"stopped","pid":0}`, 0).cmdState(context.Background(), "app")
	if err != nil || got != containerStopped {
		t.Errorf("cmdState = %q, %v, want %q", got, err, containerStopped)
	}
	if _, err := scriptRunsc(t, "not json", 0).cmdState(context.Background(), "app"); err == nil {
		t.Error("cmdState took output that is not JSON")
	}
	if _, err := scriptRunsc(t, "", 1).cmdState(context.Background(), "app"); err == nil {
		t.Error("cmdState took a failed command")
	}
}

func TestCmdExitCode(t *testing.T) {
	got, err := scriptRunsc(t, `{"id":"app","exitStatus":137}`, 0).cmdExitCode(context.Background(), "app")
	if err != nil || got != 137 {
		t.Errorf("cmdExitCode = %d, %v, want 137", got, err)
	}
	got, err = scriptRunsc(t, `{"id":"app","exitStatus":0}`, 0).cmdExitCode(context.Background(), "app")
	if err != nil || got != 0 {
		t.Errorf("cmdExitCode = %d, %v, want 0", got, err)
	}
	for name, r := range map[string]*runsc{
		"no exit status": scriptRunsc(t, `{"id":"app"}`, 0),
		"failed command": scriptRunsc(t, "", 1),
	} {
		if _, err := r.cmdExitCode(context.Background(), "app"); err == nil {
			t.Errorf("cmdExitCode took %s", name)
		}
	}
}
