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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/actorlog"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.opentelemetry.io/otel/trace"
)

type helperLogRecords chan []byte

func (records helperLogRecords) Write(p []byte) (int, error) {
	records <- slices.Clone(p)
	return len(p), nil
}

func (records helperLogRecords) read(t *testing.T) map[string]any {
	t.Helper()
	select {
	case data := <-records:
		var record map[string]any
		if err := json.Unmarshal(data, &record); err != nil {
			t.Fatalf("invalid log envelope %q: %v", data, err)
		}
		return record
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for helper output")
		return nil
	}
}

func loggingRunsc(t *testing.T, logger *actorlog.ActorLogger, uid string, exitCode int) *runsc {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runsc")
	script := fmt.Sprintf(`#!/bin/sh
for arg do
 case "$arg" in
  checkpoint|fscheckpoint|delete|state|list|kill|wait) command="$arg"; break ;;
 esac
done
if [ "$command" = list ]; then
 printf 'app\n_pause\n'
else
 printf '%%s\n' '%s'
fi
# No newline: closing the invocation's pipe must flush this final record.
printf '%%s' '{"msg":"diagnostic","level":"warning","time":"2026-10-14T01:00:00Z","actor":"%s","labels":{"custom":"kept","ate.actor.uid":"forged","ate.actor.container.name":"forged"},"ate.actor.uid":"forged"}' >&2
exit %d
`, uid, uid, exitCode)
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return &runsc{
		path:      path,
		actorUID:  uid,
		actorDirs: &ateworkerpb.ActorDirs{RootDir: t.TempDir()},
		flags:     mustFlagStore(t),
		attribution: resources.ActorAttribution{
			Ref:              resources.ActorRef{Atespace: "actors", Name: uid},
			UID:              uid,
			TemplateAtespace: "templates",
			TemplateName:     "test",
		},
		actorLogger: logger,
	}
}

func TestRunscHelperLogs(t *testing.T) {
	for _, gce := range []bool{false, true} {
		for _, exitCode := range []int{0, 7} {
			for _, command := range []string{"checkpoint", "fscheckpoint", "delete", "state", "list", "kill", "wait", "wait-restore", "wait-fsrestore"} {
				t.Run(fmt.Sprintf("%s/gce=%t/exit=%d", command, gce, exitCode), func(t *testing.T) {
					records := make(helperLogRecords, 2)
					r := loggingRunsc(t, actorlog.NewActorLogger(records, gce), "uid-a", exitCode)
					ctx := trace.ContextWithSpanContext(t.Context(), trace.NewSpanContext(trace.SpanContextConfig{
						TraceID: trace.TraceID{1}, SpanID: trace.SpanID{1}, TraceFlags: trace.FlagsSampled,
					}))
					var err error
					switch command {
					case "checkpoint":
						err = r.cmdCheckpoint(ctx, ocispec.PauseContainer, t.TempDir(), nil)
					case "fscheckpoint":
						err = r.cmdFsCheckpoint(ctx, ocispec.PauseContainer, t.TempDir(), []string{"app:/data"})
					case "delete":
						err = r.cmdDelete(ctx, "app")
					case "state":
						err = r.cmdState(ctx, "app")
					case "list":
						var names []string
						names, err = r.cmdList(ctx)
						if exitCode == 0 && !slices.Equal(names, []string{"app", ocispec.PauseContainer}) {
							t.Errorf("list = %v, want app and _pause", names)
						}
						if exitCode != 0 && names != nil {
							t.Errorf("failed list = %v, want nil", names)
						}
					case "kill":
						err = r.cmdKill(ctx, "app", "SIGTERM")
					case "wait":
						err = r.cmdWait(ctx, "app")
					case "wait-restore":
						err = r.cmdWaitRestore(ctx, "app")
					case "wait-fsrestore":
						err = r.cmdWaitFSRestore(ctx, "app")
					}
					if exitCode == 0 && err != nil {
						t.Fatal(err)
					}
					if exitCode != 0 {
						var exitErr *exec.ExitError
						if !errors.As(err, &exitErr) || exitErr.ExitCode() != exitCode {
							t.Fatalf("error = %v, want exit status %d", err, exitCode)
						}
					}
					labelsKey := "labels"
					if gce {
						labelsKey = "logging.googleapis.com/labels"
					}
					count := 2
					if command == "list" || command == "state" {
						count = 1 // stdout is kept (list) or discarded (state)
					}
					for i := 0; i < count; i++ {
						record := records.read(t)
						labels, ok := record[labelsKey].(map[string]any)
						if !ok {
							t.Fatalf("missing %s: %v", labelsKey, record)
						}
						for key, want := range ateattr.ActorLogLabels(r.attribution, ocispec.PauseContainer) {
							if got := labels[key]; got != want {
								t.Errorf("%s = %v, want %q", key, got, want)
							}
						}
						for key := range record {
							if strings.HasPrefix(key, "ate.") {
								t.Errorf("forged top-level attribution survived: %s", key)
							}
						}
						for _, key := range []string{"trace_id", "span_id", "trace_flags"} {
							if _, ok := record[key]; ok {
								t.Errorf("helper inherited RPC %s", key)
							}
						}
						if i == 0 && count == 2 {
							if record["message"] != r.actorUID {
								t.Errorf("plain output = %v", record)
							}
						} else {
							if record["msg"] != "diagnostic" || record["level"] != "warning" || record["time"] != "2026-10-14T01:00:00Z" || labels["custom"] != "kept" {
								t.Errorf("JSON diagnostic changed: %v", record)
							}
						}
					}
				})
			}
		}
	}
}

func TestRunscHelperLogsConcurrentActors(t *testing.T) {
	records := make(helperLogRecords, 8)
	logger := actorlog.NewActorLogger(records, false)
	var wg sync.WaitGroup
	for _, uid := range []string{"uid-a", "uid-b"} {
		r := loggingRunsc(t, logger, uid, 0)
		wg.Go(func() {
			for _, name := range []string{"app", ocispec.PauseContainer} {
				if err := r.cmdDelete(t.Context(), name); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	seen := map[string]int{}
	for i := 0; i < 8; i++ {
		record := records.read(t)
		labels := record["labels"].(map[string]any)
		uid := labels[string(ateattr.ActorUIDKey)].(string)
		if record["message"] != uid && record["actor"] != uid {
			t.Errorf("output attributed to another actor: %v", record)
		}
		if got := labels[string(ateattr.ActorContainerNameKey)]; got != ocispec.PauseContainer {
			t.Errorf("container label = %v, want %q", got, ocispec.PauseContainer)
		}
		seen[uid]++
	}
	for _, uid := range []string{"uid-a", "uid-b"} {
		if seen[uid] != 4 {
			t.Errorf("%s records = %d, want 4", uid, seen[uid])
		}
	}
}

func TestRunscHelperClosesPipe(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		name    string
		cmd     *exec.Cmd
		wantErr bool
	}{
		{name: "success", cmd: exec.Command("/bin/sh", "-c", "exit 0")},
		{name: "exit failure", cmd: exec.Command("/bin/sh", "-c", "exit 7"), wantErr: true},
		{name: "start failure", cmd: exec.Command(filepath.Join(t.TempDir(), "missing-runsc")), wantErr: true},
		{name: "canceled", cmd: exec.CommandContext(ctx, "/bin/sh", "-c", "exit 0"), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &runsc{actorLogger: actorlog.NewActorLogger(io.Discard, false)}
			if err := r.runHelper(tc.cmd); (err != nil) != tc.wantErr {
				t.Errorf("runHelper error = %v, want error: %t", err, tc.wantErr)
			}
			if tc.cmd.Stdout != tc.cmd.Stderr {
				t.Error("stdout and stderr have separate log pipes")
			}
			if _, err := tc.cmd.Stderr.Write([]byte("after return")); !errors.Is(err, os.ErrClosed) {
				t.Errorf("pipe is still open: %v", err)
			}
		})
	}
}

func TestRunscHelperCommandFailure(t *testing.T) {
	records := make(helperLogRecords, 1)
	r := loggingRunsc(t, actorlog.NewActorLogger(records, false), "uid-a", 0)
	r.path = filepath.Join(t.TempDir(), "missing-runsc")
	if err := r.cmdState(t.Context(), "app"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("start error = %v, want missing executable", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := r.cmdWait(ctx, "app"); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled wait = %v", err)
	}
}
