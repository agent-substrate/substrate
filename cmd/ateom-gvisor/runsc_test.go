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
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateompath"
)

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

func TestPauseArgs(t *testing.T) {
	r := &runsc{
		path:     "/usr/bin/runsc",
		actorUID: "test-actor-123",
	}

	got := r.pauseArgs("pause")
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir("test-actor-123"),
		"pause",
		"pause",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("pauseArgs() = %v, want %v", got, want)
	}
}

func TestResumeArgs(t *testing.T) {
	r := &runsc{
		path:     "/usr/bin/runsc",
		actorUID: "test-actor-123",
	}

	got := r.resumeArgs("pause")
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir("test-actor-123"),
		"resume",
		"pause",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("resumeArgs() = %v, want %v", got, want)
	}
}

func TestCtxErrorSuffix(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	killed := errors.New("signal: killed")

	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want string
	}{
		{"active", context.Background(), killed, ""},
		{"canceled", canceled, killed, " (context: context canceled)"},
		{"deadline exceeded", expired, killed, " (context: context deadline exceeded)"},
		{"err already carries it", canceled, context.Canceled, ""},
		{"err wraps it", expired, fmt.Errorf("run: %w", context.DeadlineExceeded), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ctxErrorSuffix(tc.ctx, tc.err); got != tc.want {
				t.Errorf("ctxErrorSuffix() = %q, want %q", got, tc.want)
			}
		})
	}
}
