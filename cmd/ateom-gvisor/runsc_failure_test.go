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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// scriptRunsc stands in for the runsc binary: a shell script run with the
// argv runsc would get. The actor's root dir is the script's temp dir. The
// script can append to runsc's log file as $log, the path runsc gets in
// -log, and to its panic log as $panic.
func scriptRunsc(t *testing.T, script string) *runsc {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "runsc")
	prelude := `#!/bin/sh
log=; panic=; prev=
for a in "$@"; do
  [ "$prev" = "-log" ] && log=$a
  [ "$prev" = "-panic-log" ] && panic=$a
  prev=$a
done
`
	if err := os.WriteFile(path, []byte(prelude+script), 0o755); err != nil {
		t.Fatal(err)
	}
	return &runsc{
		path:     path,
		actorUID: "test-actor-123",
		actorDirs: &ateompb.ActorDirs{
			RootDir:      dir,
			OciBundleDir: filepath.Join(dir, "bundle"),
		},
	}
}

// logLine is one runsc JSON log line.
func logLine(level, msg string) string {
	b, _ := json.Marshal(map[string]string{"level": level, "time": "2026-10-08T12:00:00Z", "msg": msg})
	return string(b)
}

func TestEveryCommandLogsToTheActorsRunscLog(t *testing.T) {
	r := &runsc{path: "/usr/bin/runsc", actorUID: "test-actor-123", actorDirs: testActorDirs}
	wantLog := "/node/actors/test-actor-123/runsc.log"
	for name, args := range map[string][]string{
		"create":  r.createArgs(ocispec.PauseContainer, []string{"-extra"}),
		"restore": r.restoreArgs(ocispec.PauseContainer, "/snap"),
		"pause":   r.pauseArgs(ocispec.PauseContainer),
		"resume":  r.resumeArgs(ocispec.PauseContainer),
		"kill":    r.killArgs("app", "SIGTERM"),
		"wait":    r.waitArgs("app"),
	} {
		i := slices.Index(args, "-log")
		verb := slices.Index(args, name)
		if i < 0 || i+1 >= len(args) || args[i+1] != wantLog {
			t.Errorf("%s args %v: want -log %s", name, args, wantLog)
			continue
		}
		// A global flag has to come before the subcommand.
		if verb < 0 || i > verb {
			t.Errorf("%s args %v: -log must precede the %q subcommand", name, args, name)
		}
	}
}

func TestCreateArgsAndRestoreArgsKeepSentryPanics(t *testing.T) {
	r := &runsc{path: "/usr/bin/runsc", actorUID: "test-actor-123", actorDirs: testActorDirs}
	wantPath := "/node/actors/test-actor-123/sentry-panic.log"
	for name, args := range map[string][]string{
		"create":  r.createArgs(ocispec.PauseContainer, []string{"-extra"}),
		"restore": r.restoreArgs(ocispec.PauseContainer, "/snap"),
	} {
		i := slices.Index(args, "-panic-log")
		verb := slices.Index(args, name)
		if i < 0 || i+1 >= len(args) || args[i+1] != wantPath {
			t.Errorf("%s args %v: want -panic-log %s", name, args, wantPath)
			continue
		}
		if verb < 0 || i > verb {
			t.Errorf("%s args %v: -panic-log must precede the %q subcommand", name, args, name)
		}
		if args[len(args)-1] != ocispec.PauseContainer {
			t.Errorf("%s args %v: container name must stay last", name, args)
		}
	}
	if got := r.createArgs("app", []string{"-extra"}); !slices.Contains(got, "-extra") {
		t.Errorf("createArgs dropped additional args: %v", got)
	}
}

func TestRunscFailureQuotesWhatItLogged(t *testing.T) {
	r := scriptRunsc(t, `
echo '`+logLine("info", "Checkpointing container")+`' >> "$log"
echo '`+logLine("warning", `checkpoint failed: cannot checkpoint container "_pause" in state stopped`)+`' >> "$log"
exit 128
`)
	err := r.cmdCheckpoint(context.Background(), ocispec.PauseContainer, "/snap")
	if err == nil {
		t.Fatal("cmdCheckpoint() = nil, want error")
	}
	want := "while running `runsc checkpoint`: exit status 128: " +
		`Checkpointing container; checkpoint failed: cannot checkpoint container "_pause" in state stopped`
	if got := err.Error(); got != want {
		t.Errorf("error = %q\nwant    %q", got, want)
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 128 {
		t.Errorf("error does not wrap the exit status: %v", err)
	}
}

// Only what the failing command logged is quoted, not an earlier command's
// lines or the sentry's from before it; and a command that logged nothing
// adds nothing.
func TestRunscFailureQuotesOnlyTheCommandsLines(t *testing.T) {
	r := scriptRunsc(t, `
case "$*" in
  *" state "*) echo '`+logLine("warning", "container not found")+`' >> "$log"; exit 1 ;;
  *" kill "*) exit 3 ;;
esac
`)
	earlier := logLine("info", "Starting gVisor") + "\n" + logLine("warning", "unsupported syscall") + "\n"
	if err := os.WriteFile(runscLogPath(r.actorDirs), []byte(earlier), 0o644); err != nil {
		t.Fatal(err)
	}

	err := r.cmdKill(context.Background(), "app", "SIGTERM")
	if err == nil || err.Error() != "while running `runsc kill`: exit status 3" {
		t.Errorf("cmdKill() = %v, want the bare failure: it logged nothing", err)
	}
	err = r.cmdState(context.Background(), "app")
	if err == nil || err.Error() != "while running `runsc state`: exit status 1: container not found" {
		t.Errorf("cmdState() = %v, want only the line it logged", err)
	}
}

func TestRunscFailureIsBounded(t *testing.T) {
	r := scriptRunsc(t, `
i=0
while [ $i -lt 400 ]; do
  echo '`+logLine("info", "line LINE: padding padding padding padding padding")+`' | sed "s/LINE/$i/" >> "$log"
  i=$((i+1))
done
echo '`+logLine("warning", "final: boom")+`' >> "$log"
exit 2
`)
	err := r.cmdPause(context.Background(), ocispec.PauseContainer)
	if err == nil {
		t.Fatal("cmdPause() = nil, want error")
	}
	got := err.Error()
	if !strings.HasSuffix(got, "final: boom") {
		t.Errorf("error = %q, want it to end with the last message", got)
	}
	if !strings.Contains(got, "exit status 2: ...line ") {
		t.Errorf("error = %q, want a ... marker followed by whole lines", got)
	}
	if strings.Contains(got, "line 0:") {
		t.Errorf("error = %q, want the start of the log dropped", got)
	}
	if len(got) > runscLogTailBytes+128 {
		t.Errorf("error is %d bytes, want at most %d plus the wrapping", len(got), runscLogTailBytes)
	}
}

func TestRunscFailureKeepsNonJSONLines(t *testing.T) {
	r := scriptRunsc(t, `
echo 'plain text from runsc' >> "$log"
echo '{"level":"warning","time":"t"}' >> "$log"
exit 1
`)
	err := r.cmdResume(context.Background(), ocispec.PauseContainer)
	want := "while running `runsc resume`: exit status 1: plain text from runsc; {\"level\":\"warning\",\"time\":\"t\"}"
	if err == nil || err.Error() != want {
		t.Errorf("cmdResume() = %v\nwant       %s", err, want)
	}
}

func TestRunscSuccessIgnoresLog(t *testing.T) {
	r := scriptRunsc(t, `
echo '`+logLine("warning", "chatter")+`' >> "$log"
echo 'lots of' 
echo 'chatter' >&2
exit 0
`)
	if err := r.cmdResume(context.Background(), ocispec.PauseContainer); err != nil {
		t.Errorf("cmdResume() = %v, want nil", err)
	}
}

// The writer a caller passes is runsc's stdio as it is. For create, start and
// restore it becomes the sandbox's stdio, which outlives the command: a
// writer that os/exec had to copy through a pipe would hold the command's
// wait open for as long as the sandbox runs.
func TestRunscCommandReturnsWhileAChildHoldsItsStdio(t *testing.T) {
	r := scriptRunsc(t, `
sleep 30 &
echo 'started'
exit 0
`)
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer pr.Close()
	defer pw.Close()

	done := make(chan error, 1)
	go func() { done <- r.cmdStart(context.Background(), pw, "app") }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("cmdStart() = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cmdStart() did not return while a child held its stdio open")
	}
	buf := make([]byte, 64)
	n, _ := pr.Read(buf)
	if got := string(buf[:n]); got != "started\n" {
		t.Errorf("writer got %q, want the command's output", got)
	}
}

func TestRunscFailureIncludesSentryPanic(t *testing.T) {
	r := scriptRunsc(t, `
echo '`+logLine("warning", "sandbox is gone")+`' >> "$log"
exit 1
`)
	panicLog := "panic: runtime error: index out of range [3] with length 3\n\ngoroutine 1 [running]:\nmain.main()\n"
	if err := os.WriteFile(panicLogPath(r.actorDirs), []byte(panicLog), 0o644); err != nil {
		t.Fatal(err)
	}
	err := r.cmdKill(context.Background(), "app", "SIGTERM")
	if err == nil {
		t.Fatal("cmdKill() = nil, want error")
	}
	want := "while running `runsc kill`: exit status 1: sandbox is gone; " +
		"sentry panic log: panic: runtime error: index out of range [3] with length 3; goroutine 1 [running]:; main.main()"
	if got := err.Error(); got != want {
		t.Errorf("error = %q\nwant    %q", got, want)
	}
}

// A panic log keeps its head, where the panic message is, not its tail.
func TestSentryPanicKeepsTheHead(t *testing.T) {
	r := scriptRunsc(t, "exit 0\n")
	panicLog := "panic: boom\n" + strings.Repeat("goroutine frame line\n", 400) + "last line\n"
	if err := os.WriteFile(panicLogPath(r.actorDirs), []byte(panicLog), 0o644); err != nil {
		t.Fatal(err)
	}
	got := r.sentryPanic()
	if !strings.HasPrefix(got, "panic: boom; goroutine frame line") {
		t.Errorf("sentryPanic() = %q, want it to start with the panic message", got)
	}
	if !strings.HasSuffix(got, "...") || strings.Contains(got, "last line") {
		t.Errorf("sentryPanic() = %q, want the end cut and marked", got)
	}
	// The bound is on bytes read; joining lines with "; " can add one byte per line.
	if len(got) > 2*runscLogTailBytes {
		t.Errorf("sentryPanic() is %d bytes, want at most %d", len(got), 2*runscLogTailBytes)
	}
}

func TestSentryPanicAbsentOrEmpty(t *testing.T) {
	r := scriptRunsc(t, "exit 0\n")
	if got := r.sentryPanic(); got != "" {
		t.Errorf("sentryPanic() with no file = %q, want empty", got)
	}
	if err := os.WriteFile(panicLogPath(r.actorDirs), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if got := r.sentryPanic(); got != "" {
		t.Errorf("sentryPanic() with an empty file = %q, want empty", got)
	}
}

func TestCmdListParsesIDsAndQuotesFailures(t *testing.T) {
	r := scriptRunsc(t, `
echo 'a b'
echo 'c'
echo 'noise' >&2
`)
	ids, err := r.cmdList(context.Background())
	if err != nil {
		t.Fatalf("cmdList() = %v", err)
	}
	if want := []string{"a", "b", "c"}; !slices.Equal(ids, want) {
		t.Errorf("cmdList() = %v, want %v", ids, want)
	}

	r = scriptRunsc(t, `echo '`+logLine("warning", "list failed: no root dir")+`' >> "$log"
exit 1
`)
	if _, err := r.cmdList(context.Background()); err == nil || !strings.Contains(err.Error(), "exit status 1: list failed: no root dir") {
		t.Errorf("cmdList() = %v, want the quoted failure", err)
	}
}

func TestLogSentryPanic(t *testing.T) {
	attribution := resources.ActorAttribution{
		Ref:              resources.ActorRef{Atespace: "space-1", Name: "act-1"},
		UID:              "uid-1",
		TemplateAtespace: "space-1",
		TemplateName:     "tmpl-1",
	}

	capture := func(t *testing.T) *bytes.Buffer {
		t.Helper()
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
		t.Cleanup(func() { slog.SetDefault(prev) })
		return &buf
	}

	t.Run("no panic log", func(t *testing.T) {
		r := scriptRunsc(t, "exit 0\n")
		buf := capture(t)
		r.logSentryPanic(context.Background(), attribution)
		if buf.Len() != 0 {
			t.Errorf("logged %q, want nothing", buf.String())
		}
	})

	t.Run("empty panic log is removed quietly", func(t *testing.T) {
		r := scriptRunsc(t, "exit 0\n")
		if err := os.WriteFile(panicLogPath(r.actorDirs), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		buf := capture(t)
		r.logSentryPanic(context.Background(), attribution)
		if buf.Len() != 0 {
			t.Errorf("logged %q, want nothing", buf.String())
		}
		if _, err := os.Stat(panicLogPath(r.actorDirs)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("panic log still present (stat err %v), want removed", err)
		}
	})

	t.Run("panic is reported with the actor's identity then removed", func(t *testing.T) {
		r := scriptRunsc(t, "exit 0\n")
		if err := os.WriteFile(panicLogPath(r.actorDirs), []byte("panic: boom\n\ngoroutine 7 [running]:\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		buf := capture(t)
		r.logSentryPanic(context.Background(), attribution)

		var rec map[string]any
		if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
			t.Fatalf("want exactly one JSON record, got %q: %v", buf.String(), err)
		}
		want := map[string]any{
			"level":                 "WARN",
			"msg":                   "Sentry panic log found while terminating the actor",
			"ate.atespace":          "space-1",
			"ate.actor.name":        "act-1",
			"ate.actor.uid":         "uid-1",
			"ate.template.atespace": "space-1",
			"ate.template.name":     "tmpl-1",
			"panic_log":             "panic: boom; goroutine 7 [running]:",
		}
		for k, v := range want {
			if rec[k] != v {
				t.Errorf("record[%q] = %v, want %v", k, rec[k], v)
			}
		}
		if _, err := os.Stat(panicLogPath(r.actorDirs)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("panic log still present (stat err %v), want removed", err)
		}
	})
}
