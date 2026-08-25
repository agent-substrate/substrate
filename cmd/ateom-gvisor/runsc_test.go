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
	"path/filepath"
	"reflect"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateompath"
)

// TestNvproxyGlobalArgs checks that runsc is told to enable nvproxy exactly when the
// worker has a GPU. The flag must be present on sandbox creation so the sentry
// initializes GPU support up front; without it the GPU subcontainer crashes.
func TestNvproxyGlobalArgs(t *testing.T) {
	dir := t.TempDir()
	old := gpuDeviceGlob
	gpuDeviceGlob = filepath.Join(dir, "nvidia[0-9]*")
	defer func() { gpuDeviceGlob = old }()

	if got := nvproxyGlobalArgs(); len(got) != 0 {
		t.Fatalf("no GPU: want no flags, got %v", got)
	}

	if err := os.WriteFile(filepath.Join(dir, "nvidia0"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	got := nvproxyGlobalArgs()
	if len(got) != 1 || got[0] != "--nvproxy" {
		t.Fatalf("GPU: want [--nvproxy], got %v", got)
	}
}

func TestKillArgs(t *testing.T) {
	r := &runsc{
		path:     "/usr/bin/runsc",
		actorUID: "test-actor-123",
	}

	got := r.killArgs("my-container", "SIGTERM")
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir("test-actor-123"),
		"kill",
		"my-container",
		"SIGTERM",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("killArgs() = %v, want %v", got, want)
	}
}

func TestWaitArgs(t *testing.T) {
	r := &runsc{
		path:     "/usr/bin/runsc",
		actorUID: "test-actor-123",
	}

	got := r.waitArgs("my-container")
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir("test-actor-123"),
		"wait",
		"my-container",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("waitArgs() = %v, want %v", got, want)
	}
}

// TestParseWaitExitStatus pins the wire format of `runsc wait`: a single
// {"id", "exitStatus"} JSON object on stdout (exitStatus is 128+signal when
// the process was killed by a signal).
func TestParseWaitExitStatus(t *testing.T) {
	for name, tc := range map[string]struct {
		out     string
		want    int32
		wantErr bool
	}{
		"clean exit":     {out: `{"id":"main","exitStatus":0}`, want: 0},
		"nonzero exit":   {out: `{"id":"main","exitStatus":7}`, want: 7},
		"signaled":       {out: `{"id":"main","exitStatus":137}`, want: 137},
		"trailing space": {out: "{\"id\":\"main\",\"exitStatus\":1}\n", want: 1},
		"garbage":        {out: "not json", wantErr: true},
		"empty":          {out: "", wantErr: true},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := parseWaitExitStatus([]byte(tc.out))
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseWaitExitStatus(%q) error = %v, wantErr %v", tc.out, err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Errorf("parseWaitExitStatus(%q) = %d, want %d", tc.out, got, tc.want)
			}
		})
	}
}

func TestStateArgs(t *testing.T) {
	r := &runsc{
		path:     "/usr/bin/runsc",
		actorUID: "test-actor-123",
	}

	got := r.stateArgs("my-container")
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir("test-actor-123"),
		"state",
		"my-container",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("stateArgs() = %v, want %v", got, want)
	}
}
